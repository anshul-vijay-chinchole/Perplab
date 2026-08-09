"""The session worker -- one session, one process (spec 2.3, spec 11).

```
python -m perplab.live.worker <userdata-root> <run-id>      # credentials on stdin
```

Runs both of spec 6.1's live modes. A **paper** session is the engine's simulated
transport against a live market feed and touches no credential at all. A **live** session
(spec 13, Phase 8) is the same session with the one substitution the spec permits: after
`live.preflight.configure_account` has brought the exchange into agreement with the
ledger, `ExchangeTransport` replaces the simulator, `UserDataStream` carries the fills
back, and `Reconciler` checks the account against the venue every sixty seconds. Which of
the two runs is decided by `spec.session_kind`, written by the API at start and never
after.

The sibling of `engine.worker`, and separate from it for the same reason that one is
separate from the API: spec 11 says *"no strategy code executes in this process"* of the
server, and spec 2.3 wants a runaway hook to cost one killable process rather than the
platform. A session additionally holds API keys, which is a second and stronger reason for
its own address space.

**Credentials arrive on stdin and nowhere else.** `spec.json` is the existing API-to-worker
channel and it is a file, which spec 11 forbids for keys. `env=` is no better: it is
inherited by every grandchild and readable from the process table. One JSON line on a pipe
that is then closed exists only in the two processes' memory.

**Everything the session recorded is durable before the status says anything.** The tape is
appended continuously and sealed before the run is completed, so a crash at hour forty-seven
loses at most the last unflushed second of observations -- and the shadow backtest, which is
what turns a session into evidence, is rebuilt from the tape rather than from the artefacts.

**Session end is three obligations, not one.** Publishing the artefacts is the obvious one.
The other two used to be missing entirely: spec 7.6 wants an automatic risk halt to arm the
*persistent* kill switch, so that the machine cannot start another session against an account
the risk layer has just declared untrustworthy (`_arm_kill_switch` -- the backstop; the first
write happens at halt time via `_arm_tripped_switch`, hung on the engine's `on_halt` hook, so
a process killed mid-halt still leaves the interlock on disk); and spec 6.7.1 wants the
tape replayed as a shadow backtest with a parity report attached to the run, which is the only
measurement anyone takes of whether the backtester is telling the truth (`_attach_shadow`).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
import traceback
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from perplab.core.account import FeeSchedule
from perplab.core.money import Money, money_to_str, parse_money
from perplab.core.risk import RiskLimits
from perplab.core.types import MarginMode
from perplab.data.manifest import market_root
from perplab.engine import ENGINE_VERSION
from perplab.engine.backtest import AutoFlatten, BacktestConfig
from perplab.engine.fills import fill_model_for_tier, fill_model_from_json
from perplab.engine.latency import latency_from_json
from perplab.engine.runspec import (
    RunSpec,
    platform_commit,
    resolve_brackets,
    resolve_filters,
)
from perplab.engine.tiers import tier_from_name
from perplab.engine.worker import _write_equity, _write_events, _write_json
from perplab.exchange.keys import KeySession
from perplab.exchange.signed import SignedRestClient
from perplab.exchange.userstream import UserDataStream
from perplab.live import PRODUCTION_LIVE_ENABLED
from perplab.live.exchange_transport import ExchangeTransport
from perplab.live.preflight import configure_account
from perplab.live.reconcile import Reconciler
from perplab.live.session import VENUES, PaperSession, SessionConfig
from perplab.live.shadow import create_shadow, write_parity
from perplab.live.stack import LiveStack
from perplab.store.claims import SymbolClaims
from perplab.store.killswitch import KillSwitchStore
from perplab.store.runs import RunStatus, RunStore
from perplab.strategy.loader import load_strategy_class

log = logging.getLogger(__name__)

__all__ = ["execute_session", "main"]

SHADOW_TIMEOUT_S = 3600.0
"""How long the worker waits for its own shadow backtest before giving up on the report.

The replay is a separate process (`RunStore.launch`), and nothing else in the platform is
watching for it to finish -- so the session's worker is what turns a queued shadow into a
parity report. An hour is generous against a replay of a 48-hour tape and finite, because a
worker that waited forever would hold a process open on a run that has already been recorded
as `done`.
"""

SHADOW_POLL_S = 1.0
"""How often the shadow's status is re-read while waiting. `RunStore.get` reaps dead workers
on the way past, so this is also what turns a shadow that died into a `failed` row."""


def execute_session(root: Path, run_id: int, store: RunStore, secrets: dict[str, Any]) -> None:
    """Run one session -- paper or live -- to completion and record everything it produced."""
    store.mark_running(run_id)
    spec = RunSpec.from_storage(store.read_json(run_id, "spec.json"))
    live = spec.session_kind == "live"
    mode_flag = "LIVE" if live else "PAPER"

    if live and spec.endpoint == "production" and not PRODUCTION_LIVE_ENABLED:
        # The API refuses this first, where the operator can read the refusal; this is the
        # second check, in the process that would actually trade, for the reason every
        # other interlock here is doubled -- a check only in the caller is a check another
        # caller can skip. See `perplab.live.PRODUCTION_LIVE_ENABLED` for what unlocks it.
        raise RuntimeError(
            "live trading against the production endpoint is disabled until the Phase 8 "
            "exit criterion has been met on testnet (spec 13): one real order placed, "
            "filled and reconciled to the cent. Start this session on testnet."
        )

    # Spec 7.6: an armed kill switch must block a new session until someone un-arms it
    # explicitly. Checked here rather than in the API because this is the process that would
    # actually trade -- a check in the caller is a check a different caller can skip.
    with KillSwitchStore(root) as switch:
        switch.require_clear()

    # The symbol claim, re-taken here for the same reason the kill switch is re-checked:
    # this is the process that would actually trade, and a check only in the API is a check
    # another caller can skip. Idempotent for a run that already holds its own symbols, so
    # the ordinary path is a no-op; a *different* run holding one of them stops the session
    # before its first order, which is the point -- the exchange has one position per symbol
    # and side, and two sessions filling into it cannot be told apart afterwards.
    with SymbolClaims(root) as claims:
        claims.claim(
            run_id=run_id,
            symbols=spec.symbols,
            leverage=spec.leverage,
            margin_mode=spec.margin_mode,
            hedge_mode=spec.hedge_mode,
            endpoint=spec.endpoint,
            now_ms=int(time.time() * 1000),
            # `active()` and not a windowed listing: this list decides which *other*
            # claims the pruner may destructively release, and a live run that had
            # merely aged past a recency window would lose its symbol to the next
            # session (see `RunStore.active`).
            active_run_ids=[summary.id for summary in store.active()],
        )

    filters, filter_reference = resolve_filters(root, spec.symbols, spec.start_ms)
    brackets, bracket_reference = resolve_brackets(root, spec.symbols, spec.start_ms)

    cls = load_strategy_class(spec.code, filename=f"{spec.strategy_name}.py")
    strategy = cls(spec.params)
    requirements = strategy.declared

    tier = tier_from_name(spec.fill_tier)
    config = BacktestConfig(
        symbols=spec.symbols,
        timeframe=spec.timeframe,
        start_ms=spec.start_ms,
        end_ms=spec.end_ms,
        seed=spec.seed,
        opening_balance=parse_money(spec.opening_balance),
        leverage=spec.leverage,
        hedge_mode=spec.hedge_mode,
        fees=FeeSchedule(
            maker_rate=parse_money(spec.maker_rate),
            taker_rate=parse_money(spec.taker_rate),
            source=spec.fee_source,
        ),
        latency=latency_from_json(spec.latency),
        fill_tier=tier,
        fill_model=(
            fill_model_from_json(dict(spec.fill_model))
            if spec.fill_model.get("tier") == tier.name
            else fill_model_for_tier(tier.name)
        ),
        liquidation_recovery_pct=parse_money(spec.liquidation_recovery_pct),
        # **No wall-clock budget.** `timeout_s` exists so a backtest with a looping hook
        # cannot run forever; a session is *supposed* to run for forty-eight hours, and the
        # budget that bounds it is `SessionConfig.max_runtime_s` measured in wall time
        # rather than the engine's own 900-second default.
        timeout_s=0.0,
        risk=RiskLimits.from_json(spec.risk_limits),
        auto_flatten=AutoFlatten.from_json(spec.auto_flatten),
        kill_switch_flatten=spec.kill_switch_flatten,
    )

    session: PaperSession | None = None

    def build_session(
        opening_balance: Money | None, fees: FeeSchedule | None = None
    ) -> PaperSession:
        """Construct the session, optionally adopting the venue's wallet as the ledger's
        opening balance and the account's measured commission rates as its fee schedule.

        A closure rather than a plain statement because a **live** session cannot know its
        opening balance until the exchange has been asked: spec 6.7.3 compares the ledger's
        wallet against `totalWalletBalance` to the cent, so a ledger opened at the number
        the operator typed would be halted by its own first reconciliation pass against an
        account that was never wrong. `_execute_live` asks, then builds; a paper session
        has no venue wallet and keeps the requested figure.

        `fees` follows the identical logic one field over, and was earned the same way the
        wallet adoption was: the platform's first live round trip halted on reconciliation
        because the session's typed taker rate (0.05%) disagreed with what the venue
        actually charged this account (0.04%) by exactly the fee on one fill. Spec 3.8
        forbids hardcoding rates precisely because they are per-account facts published by
        `GET /fapi/v1/commissionRate` -- so the preflight reads them and the ledger charges
        what the venue charges. A paper session has no account to ask and keeps the
        requested schedule.
        """
        nonlocal session, spec
        cfg = config
        rewrite = False
        if opening_balance is not None:
            cfg = replace(cfg, opening_balance=opening_balance)
            if parse_money(spec.opening_balance) != opening_balance:
                # **The run's record says what was executed, not what was asked for**
                # (spec 12.1) -- and it has to, because the shadow backtest replays from
                # `spec.json`. A shadow opened at the requested figure while the session
                # opened at the venue's wallet would diverge on `min_equity`, every
                # equity-relative limit and any equity-based sizing from the first bar,
                # and the parity report would attribute all of it to the fill model --
                # the one measurement of the backtester's honesty, measuring a config
                # mismatch instead. The requested figure survives in the adoption
                # warning `_execute_live` writes.
                spec = replace(spec, opening_balance=money_to_str(opening_balance))
                rewrite = True
        if fees is not None:
            cfg = replace(cfg, fees=fees)
            if (
                fees.maker_rate != parse_money(spec.maker_rate)
                or fees.taker_rate != parse_money(spec.taker_rate)
                or fees.source != spec.fee_source
            ):
                # Same spec-12.1 rule as the balance above: the shadow must replay with the
                # rates the session actually charged, or the parity report measures a fee
                # model mismatch and calls it fill-model error.
                spec = replace(
                    spec,
                    maker_rate=money_to_str(fees.maker_rate),
                    taker_rate=money_to_str(fees.taker_rate),
                    fee_source=fees.source,
                )
                rewrite = True
        if rewrite:
            _write_json(store.directory(run_id) / "spec.json", spec.to_storage())
        session = PaperSession(
            run_dir=store.directory(run_id),
            strategy=strategy,
            requirements=requirements,
            config=cfg,
            session=SessionConfig(
                run_id=run_id,
                endpoint=spec.endpoint or "testnet",
                mode="live" if live else "paper",
                reorder_window_ms=spec.reorder_buffer_ms or 250,
                max_runtime_s=float(secrets.get("max_runtime_s") or 0.0),
                flatten_on_stop=spec.kill_switch_flatten,
            ),
            filters=filters,
            brackets=brackets,
            flags=(*filter_reference.flags, *bracket_reference.flags),
            heartbeat=lambda: store.heartbeat(run_id),
            checkpoint=lambda s: _publish(store, run_id, s, final=False),
        )
        # **The persistent kill switch is armed at halt time, not only at session end.**
        # `_arm_kill_switch` in the `finally` was the sole writer, so an OOM kill or a
        # `TerminateProcess` between the halt and process exit -- and the halt's own settle
        # phase blocks on the network for exactly that window -- left the on-disk switch
        # un-armed and the next session's `require_clear()` passing against the account
        # the risk layer had just declared untrustworthy (spec 7.6). The engine invokes
        # this at the top of `_perform_halt`, before anything that can block; `flattened`
        # is recorded `False` because at that instant nothing has been closed, and the
        # `finally` backstop upgrades it once the outcome is verified
        # (`KillSwitchStore.arm`'s arm-first-then-catch-up contract).
        built = session
        built.engine.on_halt = lambda: _arm_tripped_switch(root, run_id, built.engine)
        return session

    stop = asyncio.Event()
    _install_signal_handlers(stop)
    try:
        if live:
            asyncio.run(_execute_live(build_session, spec, secrets, run_id, stop))
        else:
            asyncio.run(build_session(None).run(stop))
    finally:
        # A live run that failed before its session was built -- a preflight refusal, a
        # stale credential -- has no tape to seal and no engine to read a kill switch off;
        # the exception carrying the reason is already on its way to `main`, which records
        # it on the run row.
        if session is not None:
            # Sealed on every path. An unsealed tape is how a reader tells a crash from a
            # clean stop, and `TapeSource` refuses one by default -- so failing to seal
            # after an orderly stop would make the shadow backtest, and therefore the
            # parity report, unavailable for a session that completed perfectly.
            try:
                session.seal()
            except Exception:  # pragma: no cover - sealing must not mask the real failure
                pass
            # Deliberately *not* guarded. A halt that could not be recorded on disk is a
            # spec 7.6 interlock that does not exist, and the operator has to be told that
            # rather than discover it when the next session starts.
            _arm_kill_switch(root, run_id, session)

    assert session is not None  # any path that skipped build_session raised out of the try

    _publish(store, run_id, session, final=True)

    result = session.engine.result(session.processed)
    directory = store.directory(run_id)
    _write_events(directory / "events.jsonl", result)
    _write_equity(directory / "equity.parquet", result)
    _write_json(
        directory / "trades.json",
        {"trades": [trade.to_json() for trade in result.trades]},
    )
    # One list, referenced by both artefacts and by the run row, so that a note appended
    # after the shadow has run reaches all three by rewriting them. See `_attach_shadow`.
    warnings: list[str] = list(result.warnings)
    if session.stopped_reason:
        warnings.append(f"the session ended because {session.stopped_reason}.")
    flags = tuple(sorted(set(result.flags) | {mode_flag}))
    metrics_payload: dict[str, Any] = {
        "metrics": result.metrics.to_json(),
        "attribution": result.attribution.to_json(),
        "summary": {**result.summary(), "flags": list(flags), "warnings": warnings},
        "risk_breaches": [b.to_json() for b in result.risk_breaches],
    }
    manifest_payload: dict[str, Any] = {
        "reproducibility": {
            **spec.to_json(),
            "platform_commit": platform_commit(),
            "event_hash": result.event_hash,
            "executed_engine_version": ENGINE_VERSION,
            "fill_model_tier": result.fill_tier,
            "executed_fill_model": config.resolved_fill_model().to_json(),
            "data_start_ms": result.data_start_ms,
        },
        # The tape *is* this run's dataset fingerprint. A lake manifest would describe
        # files the session never read -- the bulk archive does not hold this window yet.
        "tape": session.tape.meta_snapshot(),
        "risk": result.risk_summary,
        "session": {
            "mode": session.session.mode,
            "endpoint": session.session.endpoint,
            "stopped_reason": session.stopped_reason,
            "status_log": session.status_log[-200:],
            "buffer": session.buffer.stats(),
            "feed": session.feed.counts,
            # The live-only record (spec 13, Phase 8): what the preflight applied, what
            # the transport sent and received, how the reconciliation loop fared, and the
            # final pass taken after the book was closed. `None` throughout for a paper
            # session -- absence of the check and the check passing must never look alike.
            "preflight": (
                None
                if session.live_stack is None
                else session.live_stack.preflight.to_json()
            ),
            "transport": (
                None
                if session.live_stack is None
                else session.live_stack.transport.summary()
            ),
            "reconciliation": (
                None
                if session.live_stack is None
                else {
                    **session.live_stack.reconciler.summary(),
                    "final_pass": session.final_reconciliation,
                }
            ),
            "open_at_exchange_after_stop": session.open_at_exchange,
        },
        "reference": {
            "exchangeInfo_used": filter_reference.used,
            "exchangeInfo_in_force": filter_reference.in_force,
            "leverageBracket_used": bracket_reference.used,
            "leverageBracket_in_force": bracket_reference.in_force,
        },
        "dataset": None,
        "flags": list(flags),
        "warnings": warnings,
    }

    def _publish_results() -> None:
        """Write the two artefacts and the run row from the payloads above.

        A closure rather than three statements, because the shadow step may append to
        `warnings` afterwards and the note has to reach all three places -- the run row the
        Runs list shows, `metrics.json`'s summary and `manifest.json` -- or it reaches an
        operator only by accident of which page they opened.
        """
        _write_json(directory / "metrics.json", metrics_payload)
        _write_json(directory / "manifest.json", manifest_payload)
        store.complete(
            run_id,
            event_hash=result.event_hash,
            fill_tier=result.fill_tier,
            tier_reason="live session; the tier is what the venue's streams supported",
            flags=flags,
            warnings=warnings,
            net_pnl=str(result.attribution.net_pnl),
            sharpe=result.metrics.sharpe,
            max_drawdown=result.metrics.max_drawdown,
            round_trips=result.metrics.trades.round_trips,
            fills=result.fills,
            bars=result.bars,
        )

    _publish_results()
    # **A session is a trial; its shadow is not.** This is a genuine evaluation of a
    # parameter combination against real market data, so spec 8.5's `N` moves here and
    # `engine.worker` deliberately skips it for the replay.
    store.record_trial(
        strategy_id=spec.strategy_id,
        params=spec.params,
        sharpe=result.metrics.sharpe,
        run_id=run_id,
    )

    # Spec 6.7.1, and **after** the run is recorded rather than before it: the session's own
    # numbers are complete and valid whether or not the replay succeeds, and a replay of a
    # 48-hour tape takes minutes -- a run left `running` for that long reads as a session
    # still trading, with a stop button that would do nothing.
    note = _attach_shadow(root, store, run_id)
    if note is not None:
        # Recorded, not swallowed. Rewriting the two artefacts and re-completing is what puts
        # it where an operator looks; the second `complete` re-stamps `finished_ms`, which is
        # the price of the run row carrying the note at all.
        warnings.append(note)
        _publish_results()


async def _execute_live(
    build_session: Callable[[Money | None], PaperSession],
    spec: RunSpec,
    secrets: dict[str, Any],
    run_id: int,
    stop: asyncio.Event,
) -> None:
    """Preflight the account, build the exchange stack, and run the session against it.

    The ordering is the contract, and every step of it is load-bearing:

    1. **The credential becomes a `KeySession` before anything else touches it.** The
       worker's copy gets its own idle timer and its own `wipe()`, which the plain strings
       on stdin have neither of. `main` clears the `secrets` dict in its `finally` either
       way.
    2. **Clock drift is measured before the first signed request**, so a machine whose
       clock is wrong shows up as a drift figure rather than as a `-1021` blamed on the
       preflight.
    3. **The account must be a clean slate on this run's symbols.** A pre-existing
       position would sit in the venue's ledger with nothing in ours -- the exact state
       spec 6.7.3 halts on, sixty seconds in, after the account has already been
       reconfigured. A pre-existing working order would fill under a client order id this
       session never issued and arrive as foreign flow. Both are refused *here*, before
       anything about the account has been changed, with the remedy in the message.
    4. **The ledger opens at the venue's wallet, not at the requested balance.** Spec
       6.7.3 compares the two to the cent; see `build_session`.
    5. **`configure_account` runs before the transport exists.** `ExchangeTransport`
       refuses construction without the resulting report -- the guard that closed the
       "`set_leverage` implemented and uncalled" gap -- so a preflight failure fails the
       run before any order path exists at all. A `PreflightError` propagates to `main`,
       which records it on the run row; nothing has traded and the kill switch is left
       alone.
    6. **The stack is attached before `run`**, so the transport is in place before
       `engine.start()` gives strategy code its first chance to submit.
    """
    api_key = str(secrets.get("api_key") or "")
    api_secret = str(secrets.get("api_secret") or "")
    if not api_key or not api_secret:
        raise RuntimeError(
            "this run is a live session and no credential reached the worker on stdin. "
            "The API refuses a live start without a connected exchange key, so an empty "
            "handoff here means the pipe broke rather than that the key was absent -- "
            "reconnect the exchange in the Data & Feed tab and start the session again."
        )
    keys = KeySession(api_key=api_key, api_secret=api_secret)
    try:
        await _execute_live_with(keys, build_session, spec, secrets, run_id, stop)
    finally:
        # On **every** exit, not only after a run. The refusal paths below -- a stale
        # deadline, an unfunded wallet, a dirty symbol, a preflight failure -- all raise
        # between here and `session.run`, and each one then spends the rest of the process
        # formatting a traceback and writing the run row. That is exactly the crash-dump
        # window the in-place zeroing exists to close, and it was open on every path but
        # the happy one.
        keys.wipe()


async def _execute_live_with(
    keys: KeySession,
    build_session: Callable[[Money | None], PaperSession],
    spec: RunSpec,
    secrets: dict[str, Any],
    run_id: int,
    stop: asyncio.Event,
) -> None:
    """The body of `_execute_live`, with the credential's lifetime owned by the caller."""
    expire_ms = secrets.get("keys_expire_ms")
    deadline_ms = None if expire_ms is None else int(expire_ms)
    if deadline_ms is not None and deadline_ms <= int(time.time() * 1000):
        raise RuntimeError(
            "the credential handed to this worker had already passed the deadline the API "
            "computed for it, so the parent's copy has been wiped and this one is stale. "
            "Refusing to trade on it; reconnect the exchange and start again."
        )

    venue = VENUES[spec.endpoint or "testnet"]
    async with SignedRestClient(venue.rest_base, keys) as client:
        # Measures drift as a side effect; the value itself is not needed here. The
        # warning is consumed after the session is built (below), and the reconciler
        # re-measures on its own cadence for the rest of the run (H10) -- a clock that
        # was fine at hour zero says nothing about hour thirty.
        await client.server_time_ms()

        account = await client.account()
        wallet_raw = account.get("totalWalletBalance")
        if wallet_raw in (None, ""):
            raise RuntimeError(
                "the account payload carried no totalWalletBalance, so the ledger has no "
                "opening balance to adopt and reconciliation would have nothing to check "
                "the wallet against. Refusing to trade on an unreadable account."
            )
        wallet = parse_money(str(wallet_raw))
        if wallet <= 0:
            raise RuntimeError(
                f"the account's wallet balance is {money_to_str(wallet)} USDT. A ledger "
                f"opened at zero trips the min-equity floor on its first mark, and there "
                f"is no margin to place an order with -- fund the "
                f"{spec.endpoint or 'testnet'} account first."
            )

        for symbol in spec.symbols:
            rows = await client.position_risk(symbol)
            held = [
                str(row.get("positionAmt", "0") or "0")
                for row in rows
                if parse_money(str(row.get("positionAmt", "0") or "0")) != 0
            ]
            if held:
                raise RuntimeError(
                    f"{symbol}: the account already holds a position (positionAmt "
                    f"{', '.join(held)}). This session's ledger would open flat, and spec "
                    f"6.7.3's first pass would halt the run on a disagreement that was "
                    f"true before it started. Close the position on {symbol} first."
                )
            working = await client.open_orders(symbol)
            if working:
                raise RuntimeError(
                    f"{symbol}: the account already has {len(working)} working order(s). "
                    f"Their fills would arrive under client order ids this session never "
                    f"issued and be reported as foreign flow. Cancel them first."
                )

        report = await configure_account(
            client,
            spec.symbols,
            leverage=spec.leverage,
            margin_mode=MarginMode.parse(spec.margin_mode),
            hedge_mode=spec.hedge_mode,
        )

        # **The account's real commission rates, read rather than trusted** (spec 3.8).
        # The same verify-the-echo principle as leverage above, earned the hard way: the
        # first live round trip halted on reconciliation because the requested taker rate
        # (the platform default, 0.05%) disagreed with what this account is actually
        # charged (0.04%) by exactly one fill's fee. Rates are per-account facts the venue
        # publishes; a typed rate is a guess about someone else's billing. Read per symbol
        # because the endpoint is per symbol -- the schedule adopted is the first symbol's,
        # and a cross-symbol disagreement is warned about below rather than averaged,
        # because an average is a rate nobody is charged.
        commission: dict[str, FeeSchedule] = {}
        for symbol in spec.symbols:
            commission[symbol] = FeeSchedule.from_commission_payload(
                await client.commission_rate(symbol)
            )
        adopted_fees = commission[spec.symbols[0]]

        requested_maker = parse_money(spec.maker_rate)
        requested_taker = parse_money(spec.taker_rate)
        session = build_session(wallet, fees=adopted_fees)
        requested = parse_money(spec.opening_balance)
        if wallet != requested:
            session.engine.warnings.append(
                f"this live session's ledger opened at the venue's wallet balance "
                f"({money_to_str(wallet)} USDT) rather than the requested "
                f"{spec.opening_balance}. A live ledger must start where the account "
                f"stands, or spec 6.7.3's first reconciliation pass would halt the run on "
                f"a difference that meant nothing. Every equity-relative risk limit is "
                f"measured from the adopted figure."
            )
        drift_warning = client.clock_drift_warning
        if drift_warning:
            # Spec 11's warning, surfaced where the operator reads warnings rather than
            # left as a property nothing consumed. The session still starts: drift under
            # recvWindow signs fine today, and the reconciler keeps re-measuring -- but a
            # clock already drifting at hour zero deserves to be on the record the run
            # carries.
            session.engine.warnings.append(drift_warning)
        if (
            adopted_fees.maker_rate != requested_maker
            or adopted_fees.taker_rate != requested_taker
        ):
            session.engine.warnings.append(
                f"this live session's fee schedule was adopted from the account's own "
                f"commission rates (maker {money_to_str(adopted_fees.maker_rate)}, taker "
                f"{money_to_str(adopted_fees.taker_rate)}, via /fapi/v1/commissionRate) "
                f"rather than the requested maker {money_to_str(requested_maker)} / taker "
                f"{money_to_str(requested_taker)}. The ledger must charge what the venue "
                f"charges, or spec 6.7.3's wallet comparison halts the run on the "
                f"difference -- and any backtest run at the requested rates has been "
                f"mis-charging fees by the same margin."
            )
        mismatched = {
            symbol: schedule
            for symbol, schedule in commission.items()
            if schedule.maker_rate != adopted_fees.maker_rate
            or schedule.taker_rate != adopted_fees.taker_rate
        }
        if mismatched:
            detail = ", ".join(
                f"{symbol}: maker {money_to_str(s.maker_rate)} / taker "
                f"{money_to_str(s.taker_rate)}"
                for symbol, s in sorted(mismatched.items())
            )
            session.engine.warnings.append(
                f"the account's commission rates differ across this session's symbols "
                f"({detail}); the ledger charges one schedule (adopted from "
                f"{spec.symbols[0]}), so fees on the listed symbols are modelled at the "
                f"wrong rate and the wallet reconciliation will drift by the difference "
                f"on each of their fills. Split the symbols across sessions to trade "
                f"each at its true rate."
            )
        transport = ExchangeTransport(
            session.engine,
            client,
            run_id=run_id,
            on_event=session.exchange_event,
            preflight=report,
        )
        stream = UserDataStream(
            client,
            transport.on_report,
            session.exchange_event,
            ws_base_url=venue.ws_base,
        )
        # The transport is handed over so each pass can settle unknown order outcomes
        # (C10) and diff the venue's working-order set against the engine's book (H5) --
        # both need the transport's route table to know which ids are this session's.
        reconciler = Reconciler(
            session.engine, client, on_event=session.exchange_event, transport=transport
        )
        # One weight pool for the whole session (H9): the feed's pollers draw against
        # the signed client's own budget, so market-data polling can never spend the
        # allowance an order or a kill-switch cancel needs.
        session.feed.budget = client.budget
        session.attach_live_stack(
            LiveStack(
                keys=keys,
                client=client,
                transport=transport,
                stream=stream,
                reconciler=reconciler,
                preflight=report,
                keys_expire_ms=deadline_ms,
            )
        )
        await session.run(stop)
        # The credential is destroyed by `_execute_live`'s own finally, on this path and
        # every refusal path alike; the manifest reads the stack's summaries afterwards,
        # which never touch the key.


def _arm_tripped_switch(root: Path, run_id: int, engine: Any) -> None:
    """Write the persistent trip the moment the in-memory switch fires (spec 7.6).

    The crash-ordered half of the arming: called from `BacktestEngine._perform_halt`
    before the halt cancels, flattens, or settles -- every one of which can block on the
    network in a live session and is exactly where a process gets killed. The write is a
    single SQLite insert; `flattened=False` is the truth at this instant (nothing has been
    closed yet), and `_arm_kill_switch` below re-arms in the worker's `finally` with the
    verified outcome, which `KillSwitchStore.arm` folds into the same trip rather than
    inventing a second one. No-ops when the switch has not tripped, so a session that ends
    without a halt writes nothing from here.
    """
    switch = engine.risk.kill_switch
    if not switch.tripped:
        return
    with KillSwitchStore(root) as armed:
        armed.arm(
            ts_ms=switch.tripped_at_ms or 0,
            trigger=switch.trigger or "UNKNOWN",
            detail=switch.detail,
            run_id=run_id,
            flattened=False,
        )


def _arm_kill_switch(root: Path, run_id: int, session: PaperSession) -> None:
    """Spec 7.6: an automatic trip has to outlive the process that took it.

    `KillSwitchStore.arm` had exactly one caller in the platform -- `POST /kill`, the red
    button a human presses. Every automatic trigger spec 7 lists tripped only
    `RiskEngine.kill_switch`, which is an object in this worker's memory and dies with it. So
    a session halted by the platform's own risk layer left `require_clear()` passing, and the
    next session could start immediately against the account whose state had just been
    declared untrustworthy. That is precisely the failure `store.killswitch`'s module
    docstring says the module exists to prevent -- it names the triggers one by one -- and
    the store's own `arm` refuses every trigger value except the one a person can produce,
    because nothing else ever called it.

    `flattened` is the *outcome*, read off the account after the halt has cancelled and
    optionally closed, not the request. `require_clear` prints "the account was left flat" to
    the next operator, and a safety statement built from the intention rather than the result
    is the kind of reassurance this platform treats as worse than silence.

    **And the outcome is the venue's, not the ledger's.** A flat ledger only says this
    process believes nothing is open; the statement `require_clear` makes is about the
    *account*, and the two can disagree in exactly the circumstances that trip a kill
    switch -- a missed fill, an unresolved order, a reconciliation mismatch. So `flattened`
    additionally requires that the session's final reconciliation pass actually fetched the
    account and found no mismatch against the flat ledger. "Could not check" records as
    not-flat, because unverified is not flat -- the next operator is told to look, which
    costs a glance when the account was fine and catches the case where it was not.
    """
    switch = session.engine.risk.kill_switch
    if not switch.tripped:
        return
    final = session.final_reconciliation or {}
    venue_agrees = bool(final.get("fetched")) and not final.get("mismatches")
    with KillSwitchStore(root) as armed:
        armed.arm(
            ts_ms=switch.tripped_at_ms or 0,
            trigger=switch.trigger or "UNKNOWN",
            detail=switch.detail,
            run_id=run_id,
            flattened=not session.engine.account.positions and venue_agrees,
        )


def _attach_shadow(root: Path, store: RunStore, run_id: int) -> str | None:
    """Spec 6.7.1's shadow backtest and parity report. Returns what went wrong, if anything.

    > *"On session end, a backtest is automatically re-run over that window with the same
    > strategy version, seed, and params. A parity report is attached to the run."*

    Neither half happened: `live.shadow.create_shadow` and `write_parity` had no caller
    anywhere in the platform, so `GET /runs/{id}/parity` answered 404 for every session ever
    run and its own message -- "one is written when its shadow backtest finishes" -- described
    a condition no code path could produce. The report is the only measurement anyone takes of
    whether the backtester is telling the truth (spec 6.7: *"architecture alone does not
    prevent divergence, it has to be measured"*), so with it absent, spec 6.7.2's divergence
    feedback loop had no input at all.

    **Waiting is the point.** `create_shadow` queues the replay as an ordinary run in its own
    process, and nothing else in the platform watches for it to finish -- so if this worker
    does not wait, the parity report is never written even though both halves exist.

    **A failure here is recorded, never raised.** The session's own results are a complete,
    valid artefact whether or not its shadow could be built, and a session that stopped
    before it observed anything legitimately has no shadow at all (`ShadowError` says so).
    Failing the run over that would destroy good evidence to report a missing comparison.
    """
    try:
        shadow_run_id = create_shadow(root, run_id, store)
    except Exception as exc:  # noqa: BLE001 - the reason is the payload, not the class
        return (
            f"no shadow backtest was built for this session, so it has no parity report "
            f"(spec 6.7.1): {type(exc).__name__}: {exc}"
        )

    deadline = time.monotonic() + SHADOW_TIMEOUT_S
    while True:
        # `get` reaps: a shadow whose worker died without recording anything becomes `failed`
        # here rather than leaving this loop spinning until the timeout.
        status = store.get(shadow_run_id).status
        if status in RunStatus.TERMINAL:
            break
        if time.monotonic() >= deadline:
            return (
                f"shadow backtest run {shadow_run_id} was still {status} after "
                f"{SHADOW_TIMEOUT_S:g}s, so no parity report was written. The shadow is a "
                f"run of its own -- check it, and rebuild the report from it."
            )
        time.sleep(SHADOW_POLL_S)

    if status != RunStatus.DONE:
        return (
            f"shadow backtest run {shadow_run_id} ended {status}, so this session has no "
            f"parity report. Its own results are unaffected -- what is missing is the "
            f"comparison against a backtest of the same window."
        )
    try:
        write_parity(store, run_id, shadow_run_id)
    except Exception as exc:  # noqa: BLE001 - as above
        return (
            f"shadow backtest run {shadow_run_id} finished but its parity report could not "
            f"be built: {type(exc).__name__}: {exc}"
        )
    return None


def _publish(store: RunStore, run_id: int, session: PaperSession, *, final: bool) -> None:
    """Republish the headline figures and the interim trade table.

    `TradeBuilder.snapshot` rather than `finish`: `finish` pops every still-open round-trip
    out of the book, so calling it once a minute would report the position as closed and then
    stop accounting for the rest of it.
    """
    engine = session.engine
    trades = engine.trades.snapshot()
    closed = [t for t in trades if t.exit_ms is not None]
    directory = store.directory(run_id)
    _write_json(directory / "trades.json", {"trades": [t.to_json() for t in trades]})
    _write_json(directory / "monitor.json", session.monitor())
    if final:
        return
    store.checkpoint(
        run_id,
        net_pnl=str(engine.account.equity - engine.config.opening_balance),
        sharpe=None,
        max_drawdown=None,
        round_trips=len(closed),
        fills=engine.counts["fills"],
        bars=engine.counts["bars"],
        fill_tier=engine.tier.name,
        flags=sorted(engine.flags),
        warnings=engine.warnings[:50],
    )


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """Let Ctrl-C and a terminate request end the session in an orderly way.

    Best-effort: the handler only sets the event, and everything that has to happen -- the
    final flush, the seal, the artefacts -- runs on the normal path afterwards. On Windows a
    `terminate()` from the API is `TerminateProcess` and no handler runs at all, which is
    precisely why the API asks through `control.json` first and only kills after a grace
    period.
    """

    def _handle(_signum: int, _frame: Any) -> None:
        stop.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        handler = getattr(signal, name, None)
        if handler is not None:
            try:
                signal.signal(handler, _handle)
            except (ValueError, OSError):  # pragma: no cover - not on the main thread
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m perplab.live.worker",
        description="Execute one paper session. Started by the API server.",
    )
    parser.add_argument("root", type=Path, help="the userdata root")
    parser.add_argument("run_id", type=int)
    args = parser.parse_args(argv)

    secrets: dict[str, Any] = {}
    try:
        line = sys.stdin.readline()
    except (OSError, ValueError):
        line = ""
    if line.strip():
        try:
            secrets = json.loads(line)
        except ValueError:
            secrets = {}

    store = RunStore(args.root)
    try:
        execute_session(args.root, args.run_id, store, secrets)
    except BaseException as exc:  # noqa: BLE001 - a failed session is data, not a crash
        detail = f"{type(exc).__name__}: {exc}\n\n" + "".join(
            traceback.format_exception(exc)
        )
        try:
            store.fail(args.run_id, detail)
        finally:
            print(detail, file=sys.stderr)
        return 1
    finally:
        # The credentials never touched disk and must not outlive the process any longer
        # than they have to. Nothing here can guarantee the interpreter's heap is scrubbed,
        # but dropping the only reference is the part that is in our gift.
        secrets.clear()
        # **Release this run's symbol claims however the session ended.** A claim held by a
        # process that is no longer running blocks every future session on that symbol, and
        # while `SymbolClaims.open_claims` prunes those against the run table, that only
        # happens when somebody next looks. Releasing here frees the symbol in milliseconds
        # rather than at the next read, and it runs on the crash path as well as the clean
        # one -- which is the path that would otherwise leave the claim behind.
        try:
            with SymbolClaims(args.root) as claims:
                claims.release(args.run_id, int(time.time() * 1000))
        except Exception:  # noqa: BLE001 - a stuck claim must not mask the session's own error
            log.exception("could not release symbol claims for run %s", args.run_id)
        store.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    raise SystemExit(main())
