"""How a paper session ends, and the things that used to happen only in a backtest.

`BacktestEngine.run` is the composition `start -> step* -> drain -> finish -> result`, and
spec 6.1 says a paper session runs the *same* engine. It ran three fifths of it: `start`,
`step` and `result`. Most of this file is a consequence of the two missing phases.

- **`drain` is the only dispatcher of `on_stop`.** A strategy that flattens at shutdown ended
  the session still holding its position, with `round_trips` zero and every per-trade ratio
  `None` -- while its own shadow, which goes through `run()`, closed the position, so the
  parity report reported the difference as a fill-model divergence of one fill and -3.66.
- **`finish` is the only caller of `Account.reconcile`**, the ledger's one independent replay
  of its own event log. Every backtest got that check; the mode that trades a live market
  did not.
- **`finish` is the only place `effective_end_ms` moves off the *nominal* end**, which for a
  session `api.routers.sessions` builds is `now + 48h` by default. So a five-minute session
  published `days` 2.0, an hourly grid of forty-seven periods of which forty-four were
  fabricated zeros, and an exposure measured over forty-eight hours it never observed --
  15.6x wrong on one harness and 35.8x on a stored run.
- **`run`'s `except InvariantViolation` arm is spec 7's first auto-trigger.** Driving the
  loop by hand meant an invariant failure left the kill switch un-tripped, no breach
  recorded and no `KILL_SWITCH` event -- and the final drain then ran `on_bar` again and
  booked another fill on a ledger that had just proved itself wrong.

The rest is the other ways a session end or a session's risk layer was silent: spec 7's
disconnect trigger evaluated only on the way back up, a REST poller's failure latching the
socket's downtime clock, an automatic halt that never armed the persistent kill switch, a
shadow backtest nothing ever built, and a cold start that told nobody it was cold.

Every instant here is a number the test writes, and the session module's wall clock is
replaced outright rather than slept on, so what the engine measures is a function of the
inputs the test chose rather than of how long the test took to run.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import pytest

from perplab.core.account import Account
from perplab.core.invariants import InvariantViolation
from perplab.core.money import parse_money, to_scaled
from perplab.core.risk import RiskLimits
from perplab.core.types import Bar, CollectorEventKind
from perplab.engine import ENGINE_VERSION
from perplab.engine.backtest import BacktestConfig
from perplab.engine.clock import Event, EventKind
from perplab.engine.feed import BarStep, MarkBar
from perplab.engine.fills import fill_model_for_tier
from perplab.engine.latency import FixedLatency
from perplab.engine.reorder import live_dataset_id
from perplab.engine.runspec import RunSpec
from perplab.engine.ticks import TradePrint
from perplab.live import session as live_session
from perplab.live import worker as live_worker
from perplab.live.feed import LiveFeed
from perplab.live.session import PaperSession, SessionConfig
from perplab.live.shadow import ShadowError
from perplab.store import db
from perplab.store.killswitch import KillSwitchStore
from perplab.store.runs import RunStatus, RunStore
from perplab.strategy.base import Strategy
from perplab.strategy.context import Context, FillTier
from tests.support import BTCUSDT_PAYLOAD, btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
MINUTE_MS = 60_000
MS_PER_DAY = 86_400_000
WINDOW_MS = 250

START_MS = 1_760_000_040_000
"""The session's `start_ms`: a 1m boundary, so every bar close lands on `+59_999`."""

NOMINAL_END_MS = START_MS + 48 * 60 * MINUTE_MS
"""What `api.routers.sessions` sets `end_ms` to when `max_runtime_s` is left at its default
of 0 -- it reads `now + (max_runtime_s or 48h)`. Every ratio metric a session published was
measured against this instant rather than against what it observed."""

PRICE = "60000"
QTY = "0.01"
"""0.01 at 60 000 is a notional of 600, clear of BTCUSDT's MIN_NOTIONAL of 50, and a step of
0.001 divides it exactly -- so nothing in the filter layer can refuse these orders."""

SUBMIT_LATENCY_MS = 120
"""Fixed, so an order's arrival instant is a number the test wrote rather than a draw."""

MINUTES = 5
"""Minutes of market data every session below is fed. Deliberately far short of the
forty-eight-hour nominal range, because the gap between the two is the defect."""

STOP_MS = START_MS + MINUTES * MINUTE_MS + 2_000
"""The wall clock when the session stops observing: two seconds past the last bar close's
minute. `_finalise` measures the run to `STOP_MS - 1` and seals the tape at `STOP_MS`."""

DISCONNECT_LIMIT_S = 30
"""Spec 7's stated live default for `max_disconnect_seconds`, and what `LiveMonitor.tsx`
hardcodes as the ceiling it draws."""


class _Clock:
    """The wall clock `perplab.live.session` reads, as a value the test sets."""

    def __init__(self, now_ms: int) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


# ------------------------------------------------------------------------------ strategies


class BuyAndHold(Strategy):
    """Buys once on the first bar it is warm for, and never sells."""

    requires = {
        "symbols": [SYMBOL],
        "timeframe": "1m",
        "history": 1,
        "datasets": ["klines"],
    }

    def on_start(self, ctx: Context) -> None:
        self.submitted = False

    def on_bar(self, ctx: Context, bar: Bar) -> None:
        if self.submitted or not ctx.warm:
            return
        self.submitted = True
        ctx.buy(qty=ctx.money(QTY))


class FlattenOnStop(BuyAndHold):
    """The same, plus spec 5.2's shutdown hook -- the documented way to exit at the end."""

    def on_start(self, ctx: Context) -> None:
        super().on_start(ctx)
        self.stopped = False

    def on_stop(self, ctx: Context) -> None:
        self.stopped = True
        held = ctx.position(SYMBOL).qty
        if held != 0:
            ctx.sell(qty=abs(held), reduce_only=True)


class RestingStop(BuyAndHold):
    """Buys, then attaches a stop far enough away that nothing in this file triggers it.

    The stop goes on once the entry has filled, because a protective order takes its side
    from the position it protects and there is nothing to infer it from while flat.
    """

    def on_start(self, ctx: Context) -> None:
        super().on_start(ctx)
        self.protected = False

    def on_bar(self, ctx: Context, bar: Bar) -> None:
        super().on_bar(ctx, bar)
        if self.protected or ctx.position(SYMBOL).qty == 0:
            return
        self.protected = True
        # Half the market price. Every print and mark in these fixtures is `PRICE`, so this
        # order is still resting when the session stops, which is what makes the cancel
        # observable rather than a fill wearing its name.
        ctx.stop_loss(stop_price=ctx.money("30000"), qty=ctx.money(QTY))


class BreakTheLedger(Strategy):
    """Raises the ledger's own `InvariantViolation` from inside a dispatched event.

    Armed for the second warm bar so that a position is already open when it fires, which is
    the case spec 3.10 is about: an accounting engine that has lost track of state while it
    is holding something. It counts its own calls so the test can see whether the hook ran
    again after the failure.
    """

    requires = {
        "symbols": [SYMBOL],
        "timeframe": "1m",
        "history": 1,
        "datasets": ["klines"],
    }

    def on_start(self, ctx: Context) -> None:
        self.warm_bars = 0
        self.orders = 0

    def on_bar(self, ctx: Context, bar: Bar) -> None:
        if not ctx.warm:
            return
        self.warm_bars += 1
        if self.warm_bars == 1:
            self.orders += 1
            ctx.buy(qty=ctx.money(QTY))
            return
        raise InvariantViolation("I1", "the wallet does not reconcile against its events")


# -------------------------------------------------------------------------------- fixtures


def paper_session(
    run_dir: Path,
    strategy: Strategy,
    *,
    tier: FillTier = FillTier.BAR_CLOSE,
    risk: RiskLimits | None = None,
    flatten_on_stop: bool = False,
) -> PaperSession:
    """A session over no lake at all, with the API's own forty-eight-hour nominal range."""
    config = BacktestConfig(
        symbols=(SYMBOL,),
        timeframe="1m",
        start_ms=START_MS,
        end_ms=NOMINAL_END_MS,
        opening_balance=parse_money("10000"),
        leverage=10,
        latency=FixedLatency(submit=SUBMIT_LATENCY_MS, cancel=SUBMIT_LATENCY_MS),
        fill_tier=tier,
        risk=risk if risk is not None else RiskLimits(),
    )
    return PaperSession(
        run_dir=run_dir,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        session=SessionConfig(
            run_id=1,
            endpoint="testnet",
            reorder_window_ms=WINDOW_MS,
            flatten_on_stop=flatten_on_stop,
        ),
        filters={SYMBOL: btcusdt_filters()},
        brackets={SYMBOL: single_bracket_table()},
    )


def bar_close_event(close_ms: int, seq: int) -> Event:
    bar = Bar(
        symbol=SYMBOL,
        open_time=close_ms - MINUTE_MS + 1,
        close_time=close_ms,
        open=to_scaled(PRICE),
        high=to_scaled(PRICE),
        low=to_scaled(PRICE),
        close=to_scaled(PRICE),
        volume=to_scaled("10"),
        quote_volume=to_scaled("600000"),
        trades=100,
    )
    return Event(
        ts_ms=close_ms,
        kind=EventKind.BAR_CLOSE,
        source_seq=seq,
        dataset_id=live_dataset_id("klines", SYMBOL),
        payload=BarStep(close_time=close_ms, bars=(bar,)),
    )


def mark_event(close_ms: int, seq: int) -> Event:
    return Event(
        ts_ms=close_ms,
        kind=EventKind.MARK_PRICE_UPDATE,
        source_seq=seq,
        dataset_id=live_dataset_id("markPrice", SYMBOL),
        payload=MarkBar(
            symbol=SYMBOL,
            close_time=close_ms,
            high=to_scaled(PRICE),
            low=to_scaled(PRICE),
            close=to_scaled(PRICE),
        ),
    )


def trade_event(ts_ms: int, seq: int) -> Event:
    return Event(
        ts_ms=ts_ms,
        kind=EventKind.TRADE,
        source_seq=seq,
        dataset_id=live_dataset_id("aggTrades", SYMBOL),
        payload=TradePrint(
            symbol=SYMBOL,
            ts_ms=ts_ms,
            price_scaled=to_scaled(PRICE),
            qty_scaled=to_scaled("1"),
            is_buyer_maker=False,
            agg_id=seq,
        ),
    )


def feed_minutes(session: PaperSession, clock: _Clock, minutes: int = MINUTES) -> None:
    """`minutes` minutes of a healthy feed, drained as the session's own loop drains it.

    One trade print mid-minute -- so a market order arriving on the bar close has a price to
    fill against -- then a mark bar and the bar close at the minute's last millisecond, then
    one more print three hundred milliseconds later, which is what carries the released
    frontier past the `ORDER_ARRIVAL` scheduled at `close + SUBMIT_LATENCY_MS`.
    """
    for index in range(minutes):
        close_ms = START_MS + (index + 1) * MINUTE_MS - 1
        clock.now_ms = close_ms + 900
        for source in LiveFeed.SLOW_SOURCES:
            session._complete_through(source, close_ms)
        session._offer(trade_event(close_ms - 30_000, 1_000 + index), close_ms - 29_950)
        session._offer(mark_event(close_ms, index), close_ms + 300)
        session._offer(bar_close_event(close_ms, index), close_ms + 900)
        session._offer(trade_event(close_ms + 300, index), close_ms + 950)
        clock.now_ms = close_ms + 1_500
        for source in LiveFeed.SLOW_SOURCES:
            session._complete_through(source, close_ms + 400)
        session._drain_to_engine()


async def _wait_only(stop: asyncio.Event) -> None:
    """The feed a test that only needs the session's own loop gets: no socket, no pollers."""
    await stop.wait()


def run_to_control_stop(session: PaperSession, *, flatten: bool | None = None) -> None:
    """Drive the real `PaperSession.run` to a control-file stop, without a socket.

    The control file is written *before* `run` is awaited, so the loop makes exactly one
    pass -- drain, read the control file, return -- and never reaches its own sleep. Nothing
    here depends on how long the test took to run.
    """
    session.feed.run = _wait_only  # type: ignore[method-assign]
    payload: dict[str, Any] = {"stop": True, "reason": "asked to stop"}
    if flatten is not None:
        payload["flatten"] = flatten
    (session.run_dir / live_session.CONTROL_FILENAME).write_text(
        json.dumps(payload), encoding="utf-8"
    )
    asyncio.run(session.run(asyncio.Event()))


# --------------------------------------------------------------------- the measured window


def test_a_stopped_sessions_metrics_are_measured_to_the_instant_it_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`effective_end_ms` follows what was observed, not the range that was asked for.

    The session is built exactly as the API builds one -- `end_ms = start + 48h`, because
    `StartSessionRequest.max_runtime_s` defaults to 0 and `sessions.py` reads it as
    `now + (max_runtime_s or 48h)`. It then observes five minutes and is stopped.

    Every number below is derived from two instants this test wrote. The observed window is
    `STOP_MS - START_MS` = 302 000 ms, so `days` is that over a day. No whole hour fits
    inside it, so the hourly grid has no periods and there is no Sharpe -- measured against
    the nominal end there were forty-seven periods, forty-four of them zeros manufactured by
    carrying the final equity sample across hours the session never saw, and a Sharpe of
    `sqrt(8760/47)` that is a function of the configured runtime rather than of the data.

    Exposure is the same story with a number that can be checked by hand. The position opens
    on the arrival that follows the first warm bar close, so the first sample that carries it
    is the *second* bar close: it is held for `STOP_MS - (START_MS + 2 * MINUTE_MS - 1)` =
    182 001 of the 302 000 ms observed. Against the nominal end the identical run reports
    99.93%.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    strategy = BuyAndHold()
    session = paper_session(tmp_path, strategy)
    session.engine.start()
    feed_minutes(session, clock)
    assert strategy.submitted, "the strategy must have traded for exposure to mean anything"

    clock.now_ms = STOP_MS
    session._end()
    result = session.engine.result(session.processed)

    observed_ms = STOP_MS - START_MS
    held_from_ms = START_MS + 2 * MINUTE_MS - 1
    assert session.engine.effective_end_ms == STOP_MS
    assert session.engine.config.end_ms == NOMINAL_END_MS, "the request itself is untouched"
    assert result.metrics.days == pytest.approx(observed_ms / MS_PER_DAY)
    assert result.metrics.periods == 0
    assert result.metrics.sharpe is None
    assert result.metrics.exposure == pytest.approx(
        (STOP_MS - held_from_ms) / observed_ms
    )


def test_a_halted_session_is_measured_to_the_halt_and_not_to_the_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The halted case is the engine's own rule, and a session must not override it.

    `max_position_notional` of 100 refuses an order of 600 rather than halting, so the limit
    used here is `max_disconnect_seconds`: the session is told the socket has been down for
    twice its ceiling with a position open, which is spec 7's third auto-trigger and ends the
    run where it stands.

    `BacktestEngine._finalise` already ends a halted run at the halt and shortens
    `effective_end_ms` to it, and that is exactly right -- after a halt nothing is being
    observed. So a halted session must land on the halt instant, not on the wall clock two
    seconds later that a *stopped* session lands on: `effective_end_ms` is the engine clock
    at the halt plus one, and the halt happened on the last event dispatched before it.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    session = paper_session(
        tmp_path,
        BuyAndHold(),
        risk=RiskLimits.from_json({"max_disconnect_seconds": DISCONNECT_LIMIT_S}),
    )
    session.engine.start()
    feed_minutes(session, clock)
    assert session.engine.account.qty(SYMBOL) != 0

    session._on_status(CollectorEventKind.DISCONNECT, "btcusdt@bookTicker,x", "gone", 0)
    down_since = session._market_down_since
    assert down_since is not None
    session._check_disconnect(down_since + 2 * DISCONNECT_LIMIT_S)
    halt_ms = session.engine.runtime.now_ms

    clock.now_ms = STOP_MS
    session._end()

    assert session.engine.halted
    assert session.engine.effective_end_ms == halt_ms + 1
    assert session.engine.effective_end_ms < STOP_MS
    assert session.ended_ms == halt_ms + 1


def test_the_tape_is_sealed_at_the_instant_the_metrics_are_measured_to(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The session and its own shadow must be measured over one window, not two.

    `shadow_spec` takes the shadow's `end_ms` straight from the tape's `ended_ms`, so if the
    seal reads its own wall clock while the metrics are measured somewhere else, the two runs
    report different exposure and Sharpe for the same market and the same fills -- and the
    parity report compares fills and PnL, never metrics, so nothing surfaces the difference.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    session = paper_session(tmp_path, BuyAndHold())
    session.engine.start()
    feed_minutes(session, clock)

    clock.now_ms = STOP_MS
    session._end()
    # A later wall clock, as the worker's own `_now_ms()` would read by the time it seals.
    clock.now_ms = STOP_MS + 5_000
    session.seal()

    meta = json.loads((tmp_path / "tape" / "meta.json").read_text(encoding="utf-8"))
    assert meta["ended_ms"] == session.engine.effective_end_ms == STOP_MS


# ------------------------------------------------------------------------ on_stop and drain


def test_on_stop_runs_at_session_end_and_the_order_it_submits_actually_fills(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A strategy that flattens at shutdown must end flat, with a closed round-trip.

    `BacktestEngine.drain`'s own docstring gives the reason it exists: *"a strategy that
    flattens in `on_stop` is doing something legitimate and the fill has to actually happen,
    or the run reports a position the strategy closed"*. In a session it did not happen at
    all -- the hook never ran, so the position stayed open, `round_trips` was 0 and every
    per-trade ratio was `None`, because `trade_stats` excludes open trades from all of them.

    The counts are derived: one buy on the first warm bar, one reduce-only sell from
    `on_stop`, so exactly two fills and exactly one round-trip.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    strategy = FlattenOnStop()
    session = paper_session(tmp_path, strategy)
    session.engine.start()
    feed_minutes(session, clock)
    assert session.engine.account.qty(SYMBOL) == parse_money(QTY)

    clock.now_ms = STOP_MS
    session._end()
    result = session.engine.result(session.processed)

    assert strategy.stopped, "on_stop must have been dispatched"
    assert session.engine.account.qty(SYMBOL) == parse_money("0")
    assert session.engine.counts["fills"] == 2
    assert result.metrics.trades.round_trips == 1
    assert [trade.exit_ms is not None for trade in result.trades] == [True]


def test_the_ledger_is_reconciled_once_at_session_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`Account.reconcile` is the ledger's one independent check, and a session skipped it.

    It replays the event log from the opening balance and compares the result against live
    state, so a fill mis-booked identically into both the wallet and the totals -- the one
    failure I1 is blind to -- surfaces there and nowhere else. `_finalise` is its only caller
    in the package, and `_finalise` is reached only through `finish()`, which a session never
    called. Spec 3.10 gives I9 the check point "end of run" and says a failed invariant
    aborts the run "in backtest and paper"; paper is named.

    Counted rather than asserted-on-effect, because the check passes on a healthy session:
    the property is that it *ran*, exactly once, and calls are counted from a wrapper this
    test installs so the number cannot come from anywhere else.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    calls: list[int] = []
    original = Account.reconcile

    def counting(self: Account) -> None:
        calls.append(1)
        original(self)

    monkeypatch.setattr(Account, "reconcile", counting)

    session = paper_session(tmp_path, BuyAndHold())
    session.engine.start()
    feed_minutes(session, clock)
    assert calls == [], "nothing reconciles mid-session"

    clock.now_ms = STOP_MS
    session._end()

    assert len(calls) == 1


# ------------------------------------------------------------------- the invariant trigger


def test_an_invariant_failure_records_the_kill_switch_and_dispatches_nothing_further(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec 7's first auto-trigger, in the loop the session drives itself.

    `BacktestEngine.run` catches `InvariantViolation`, records the trip with trigger
    `INVARIANT`, raises `RISK_HALTED`, emits a `KILL_SWITCH` event and re-raises. A session
    never calls `run`, so none of that happened: the exception left the worker as a bare
    traceback with the switch un-tripped and nothing on the run saying the accounting had
    broken.

    The second half matters as much as the first. The old final drain ran regardless, with
    `engine.halted` still false, so `on_bar` ran again and another order was submitted and
    filled *after* the ledger had declared itself wrong -- which is the sentence spec 3.10 is
    quoted for. The strategy here counts its own orders, so one order is the assertion that
    nothing was dispatched after the failure.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    strategy = BreakTheLedger()
    session = paper_session(tmp_path, strategy)

    with pytest.raises(InvariantViolation, match="I1"):
        run_to_control_stop_with_data(session, clock)

    switch = session.engine.risk.kill_switch
    assert switch.tripped
    assert switch.trigger == "INVARIANT"
    assert "RISK_HALTED" in session.engine.flags
    assert [e.kind for e in session.engine.runtime.events if e.kind == "KILL_SWITCH"] == [
        "KILL_SWITCH"
    ]
    assert strategy.orders == 1, "no hook may run after the ledger contradicted itself"
    assert session.ended_ms is None, "an abandoned run is not finalised"


def run_to_control_stop_with_data(session: PaperSession, clock: _Clock) -> None:
    """`run_to_control_stop`, with two minutes of market data offered before the loop starts.

    The events are offered first so the loop's very first drain dispatches all of them; the
    control file then stops it on the same pass. `BreakTheLedger` needs two warm bars, so two
    minutes past the first is the smallest fixture that reaches its failure.
    """
    for index in range(3):
        close_ms = START_MS + (index + 1) * MINUTE_MS - 1
        for source in LiveFeed.SLOW_SOURCES:
            session._complete_through(source, close_ms + 400)
        session._offer(trade_event(close_ms - 30_000, 1_000 + index), close_ms - 29_950)
        session._offer(mark_event(close_ms, index), close_ms + 300)
        session._offer(bar_close_event(close_ms, index), close_ms + 900)
        session._offer(trade_event(close_ms + 300, index), close_ms + 950)
    clock.now_ms = START_MS + 3 * MINUTE_MS + 1_500
    session.engine.start()
    run_to_control_stop(session)


# ------------------------------------------------------------------- the disconnect trigger


def test_the_disconnect_trigger_fires_while_the_socket_is_still_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A socket that never comes back must still halt a session holding a position.

    The downtime used to be measured only on `CONNECT`/`RECONNECT`, which the socket emits
    only when it returns -- so the one outage shape spec 7's third trigger exists for could
    not fire it. Measured against a dead endpoint with a 1 s ceiling and 0.01 BTC open: ten
    seconds down, three `DISCONNECT` entries, zero breaches, kill switch clear, position
    still open; feeding a single `RECONNECT` halted immediately.

    No reconnect is fed here. `_check_disconnect` is handed the instant to evaluate at, so
    the observed downtime is exactly the `2 * DISCONNECT_LIMIT_S` this test chose.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    session = paper_session(
        tmp_path,
        BuyAndHold(),
        risk=RiskLimits.from_json({"max_disconnect_seconds": DISCONNECT_LIMIT_S}),
    )
    session.engine.start()
    feed_minutes(session, clock)
    assert session.engine.account.qty(SYMBOL) != 0

    session._on_status(CollectorEventKind.DISCONNECT, "btcusdt@bookTicker,x", "gone", 0)
    down_since = session._market_down_since
    assert down_since is not None

    # Still inside the ceiling: nothing may fire yet.
    session._check_disconnect(down_since + DISCONNECT_LIMIT_S)
    assert session.engine.risk.breaches == []

    session._check_disconnect(down_since + 2 * DISCONNECT_LIMIT_S)

    breach = session.engine.risk.breaches[-1]
    assert breach.limit == "max_disconnect_seconds"
    assert breach.observed == str(2 * DISCONNECT_LIMIT_S * 1_000)
    assert breach.allowed == str(DISCONNECT_LIMIT_S * 1_000)
    assert session.engine.risk.kill_switch.trigger == "DISCONNECT"


def test_a_failed_rest_poll_is_not_the_sockets_downtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A poller and the socket fail independently and must not share one clock.

    `LiveFeed._report_poll_failure` reports through the same `on_status` callback the socket
    reports through, and only the socket ever emits `CONNECT`/`RECONNECT` -- so one kline
    429 started the socket's downtime clock with nothing able to stop it. Measured: a single
    REST failure, then a genuine 2 000 ms socket outage ninety seconds later, halted a
    healthy session on an observed 92 000 ms against a 30 s ceiling, and the live monitor
    reported the market feed down while frames were arriving.

    A whole hour of poller failure is evaluated here -- twice the ceiling and then some --
    against a position that is open, which is every condition the trigger needs except the
    one that matters.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    session = paper_session(
        tmp_path,
        BuyAndHold(),
        risk=RiskLimits.from_json({"max_disconnect_seconds": DISCONNECT_LIMIT_S}),
    )
    session.engine.start()
    feed_minutes(session, clock)
    assert session.engine.account.qty(SYMBOL) != 0

    session._on_status(
        CollectorEventKind.DISCONNECT, "klines", "poll failed: BinanceRestError: 429", 0
    )

    assert session._market_down_since is None
    assert session.monitor()["connection"]["market"] == "up"
    assert session.monitor()["connection"]["pollers_down"] == ["klines"]
    session._check_disconnect(time.monotonic() + 3_600.0)
    assert session.engine.risk.breaches == []
    assert not session.engine.halted

    # A poll that succeeds is the only recovery signal a poller has.
    session._complete_through("klines", START_MS)
    assert session.monitor()["connection"]["pollers_down"] == []


# ------------------------------------------------------------------------- the stop sequence


def test_a_stop_cancels_the_resting_book_and_leaves_the_position_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec 7.3 item 2 always, item 3 only when asked.

    `CONTROL_FILENAME`'s docstring says the control file exists so that a stopping session
    "gets to cancel its resting orders at the exchange, which is items 1 and 2 of spec 7.3".
    Nothing did: a stopped session ended with its book intact, and `flatten_on_stop` was set
    by the worker and read by nothing at all.

    Cancel-only is the default because spec 7.3 says so -- *"force-closing everything at
    market during a flash crash can be worse than the exposure"* -- so the position must
    still be there afterwards, and only the resting stop must be gone.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    session = paper_session(tmp_path, RestingStop(), tier=FillTier.TRADE_ONLY)
    session.engine.start()
    feed_minutes(session, clock)
    resting = [o for o in session.engine.orders.values() if o.is_open]
    assert len(resting) == 1, "the stop must still be on the book before the session ends"

    clock.now_ms = STOP_MS
    session._end()

    assert [o.status.value for o in session.engine.orders.values() if not o.is_open] == [
        "FILLED",
        "CANCELLED",
    ]
    assert session.engine.account.qty(SYMBOL) == parse_money(QTY)


def test_a_close_all_stop_flattens_the_position(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`POST /kill` with close-all writes `flatten` into the control file, and it is honoured.

    `RunStore.request_stop` writes the key on every stop and the session used to parse the
    file and discard it -- so a close-all kill closed nothing while `KillSwitchStore` went on
    telling the next operator "the account was left flat". The stop is driven through the
    real control file and the real `run` loop here, because the file is the whole path.
    """
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    session = paper_session(tmp_path, BuyAndHold())
    session.engine.start()
    feed_minutes(session, clock)
    assert session.engine.account.qty(SYMBOL) == parse_money(QTY)

    clock.now_ms = STOP_MS
    run_to_control_stop(session, flatten=True)

    assert session.engine.account.qty(SYMBOL) == parse_money("0")
    assert session.engine.counts["fills"] == 2


# -------------------------------------------------------------------------------- warm-up


def test_a_cold_start_raises_a_flag_and_a_warning_naming_the_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A session that cannot trade for hours must not look like one that chose not to.

    Nothing preloads history for a live session, so `ctx.warm` is false until the declared
    bars have elapsed in real time: 200 bars on a 1m timeframe is three hours and twenty
    minutes of a session that reports `completed`, zero orders, zero flags and zero warnings.
    `LakeSource` raises `WARMUP_SHORT` plus an explicit warning for a strictly smaller
    version of the same shortfall, and the same pair is what makes this one legible.

    `BuyAndHold` is the negative case, and it is the reason the flag means anything: it
    declares one bar of history and is warm on the first bar close, exactly as a backtest
    would be, so it loses nothing to starting cold. A badge raised for it too would be on
    for every session ever run.

    The 200 is this test's, and the minutes in the warning are derived from it and from the
    declared timeframe rather than written twice.
    """
    bars = 200

    class SlowToWarm(BuyAndHold):
        requires = {
            "symbols": [SYMBOL],
            "timeframe": "1m",
            "history": bars,
            "datasets": ["klines"],
        }

    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    strategy = SlowToWarm()
    session = paper_session(tmp_path, strategy)
    session.engine.start()

    assert session.engine.warmup_bars == bars
    assert "WARMUP_SHORT" in session.engine.flags
    warning = "".join(w for w in session.engine.warnings if "starts cold" in w)
    assert str(bars) in warning
    assert f"{bars * MINUTE_MS // 60_000} minute(s)" in warning

    feed_minutes(session, clock)
    assert session.engine.context.warm is False
    assert session.monitor()["warmup"] == {
        "bars_required": bars,
        "bars_seen": MINUTES,
        "warm": False,
    }

    one_bar_dir = tmp_path / "one-bar"
    one_bar_dir.mkdir()
    warm_at_once = paper_session(one_bar_dir, BuyAndHold())
    warm_at_once.engine.start()
    assert warm_at_once.engine.warmup_bars == 1
    assert "WARMUP_SHORT" not in warm_at_once.engine.flags
    assert [w for w in warm_at_once.engine.warnings if "starts cold" in w] == []


# --------------------------------------------------------------- the worker's own two jobs


def test_an_automatic_halt_arms_the_persistent_kill_switch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec 7.6's un-arm gate has to apply to the triggers that fire without a human.

    `KillSwitchStore.arm` had exactly one caller in the platform: `POST /kill`, with
    `trigger="MANUAL"` hard-coded. Every automatic trigger tripped only the in-memory
    `RiskEngine.kill_switch`, which dies with the worker -- so a session halted by the
    platform's own risk layer left `require_clear()` passing and the machine free to start
    another session immediately against the account whose state had just been declared
    untrustworthy.

    The halt is fired through the production status path so the trigger recorded on disk is
    one the platform can actually produce, and `flattened` is read off the account rather
    than off the request, because `require_clear` prints it to the next operator.
    """
    db.connect(tmp_path).close()
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    session = paper_session(
        tmp_path,
        BuyAndHold(),
        risk=RiskLimits.from_json({"max_disconnect_seconds": DISCONNECT_LIMIT_S}),
    )
    session.engine.start()
    feed_minutes(session, clock)
    session._on_status(CollectorEventKind.DISCONNECT, "btcusdt@bookTicker,x", "gone", 0)
    down_since = session._market_down_since
    assert down_since is not None
    session._check_disconnect(down_since + 2 * DISCONNECT_LIMIT_S)
    clock.now_ms = STOP_MS
    session._end()
    assert session.engine.halted

    with KillSwitchStore(tmp_path) as switch:
        switch.require_clear()  # clear until the worker records the halt

    live_worker._arm_kill_switch(tmp_path, 1, session)

    with KillSwitchStore(tmp_path) as switch:
        trip = switch.state()
        assert trip is not None
        assert trip.trigger == "DISCONNECT"
        assert trip.run_id == 1
        assert trip.flattened is False, "cancel-only left the position open"
        with pytest.raises(Exception, match="kill switch"):
            switch.require_clear()


def test_a_session_that_ended_clean_leaves_the_kill_switch_clear(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The gate is only worth having if an ordinary stop does not arm it.

    Spec 7.6 asks for an explicit un-arm before a live session may start again, and a switch
    that armed itself after every clean session would be un-armed reflexively -- which is the
    same as not having one.
    """
    db.connect(tmp_path).close()
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    session = paper_session(tmp_path, BuyAndHold())
    session.engine.start()
    feed_minutes(session, clock)
    clock.now_ms = STOP_MS
    session._end()

    live_worker._arm_kill_switch(tmp_path, 1, session)

    with KillSwitchStore(tmp_path) as switch:
        assert switch.state() is None
        switch.require_clear()


def test_the_parity_report_is_written_once_the_shadow_backtest_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec 6.7.1's second half: the report is only attached if someone waits for the replay.

    `create_shadow` queues the shadow as a run of its own, in its own process, and nothing
    else in the platform watches for it -- so the session's worker is what has to turn a
    finished shadow into a parity report. The shadow's status is served here as `queued`
    first and `done` second, which is the property under test: the report is written after
    the replay has finished and not before.
    """
    statuses = iter([RunStatus.QUEUED, RunStatus.DONE])
    written: list[tuple[int, int]] = []

    class _Store:
        """A store that answers `queued` once and `done` once, and nothing else."""

        def get(self, run_id: int) -> Any:
            assert run_id == 77
            return type("Row", (), {"status": next(statuses)})()

    monkeypatch.setattr(live_worker, "create_shadow", lambda root, run_id, store: 77)
    monkeypatch.setattr(
        live_worker,
        "write_parity",
        lambda store, paper, shadow: written.append((paper, shadow)),
    )
    monkeypatch.setattr(live_worker, "SHADOW_POLL_S", 0.0)

    note = live_worker._attach_shadow(tmp_path, _Store(), 5)  # type: ignore[arg-type]

    assert note is None
    assert written == [(5, 77)]


def test_a_shadow_that_cannot_be_built_is_recorded_on_the_run_rather_than_raised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A session's own results are valid whether or not its shadow could be built.

    A session that stopped before it observed anything legitimately has no shadow --
    `shadow_spec` refuses it, because a replay of an empty market would report a parity of
    zero against zero. Failing the run over that would destroy good evidence in order to
    report a missing comparison, and swallowing it would leave the operator reading a run
    with no parity panel and no reason given.
    """

    def refuse(root: Path, run_id: int, store: Any) -> int:
        raise ShadowError("the tape recorded no dispatched events")

    monkeypatch.setattr(live_worker, "create_shadow", refuse)
    note = live_worker._attach_shadow(tmp_path, object(), 5)  # type: ignore[arg-type]

    assert note is not None
    assert "no dispatched events" in note
    assert "6.7.1" in note


# ------------------------------------------------------------------- the whole worker path


BRACKETS = """
[
  {
    "symbol": "BTCUSDT",
    "brackets": [
      {
        "bracket": 1,
        "initialLeverage": 125,
        "notionalCap": 1000000000000,
        "notionalFloor": 0,
        "maintMarginRatio": 0.004,
        "cum": 0
      }
    ]
  }
]
"""
"""Written as text so no Python float ever exists in the fixture: `Decimal(0.004)` is
`0.004000000000000000083...`, and that value multiplies a notional inside every liquidation
solve."""

STRATEGY_SOURCE = '''
from perplab.strategy import Strategy


class Idle(Strategy):
    """Trades nothing. The worker's own end-of-session obligations are what is under test."""

    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1}

    def on_bar(self, ctx, bar):
        return
'''


def _seed_run(root: Path, store: RunStore) -> int:
    """A paper run row with a spec, reference snapshots, and a strategy the loader can load."""
    exchange = root / "reference" / "exchangeInfo"
    exchange.mkdir(parents=True, exist_ok=True)
    (exchange / "2024-01-01.json").write_text(
        json.dumps({"symbols": [BTCUSDT_PAYLOAD]}), encoding="utf-8"
    )
    brackets = root / "reference" / "leverageBracket"
    brackets.mkdir(parents=True, exist_ok=True)
    (brackets / "2024-01-01.json").write_text(BRACKETS, encoding="utf-8")

    connection = db.connect(root)
    with connection:
        strategy_id = int(
            connection.execute(
                "INSERT INTO strategies (name, created_ms, updated_ms) VALUES ('Idle', 0, 0)"
            ).lastrowid
            or 0
        )
        version_id = int(
            connection.execute(
                """
                INSERT INTO strategy_versions
                    (strategy_id, version_no, code, code_sha256, created_ms, valid)
                VALUES (?, 1, ?, 'sha', 0, 1)
                """,
                (strategy_id, STRATEGY_SOURCE),
            ).lastrowid
            or 0
        )
    connection.close()

    spec = RunSpec(
        strategy_id=strategy_id,
        version_id=version_id,
        version_no=1,
        strategy_name="Idle",
        code=STRATEGY_SOURCE,
        class_name="Idle",
        params={},
        symbols=(SYMBOL,),
        timeframe="1m",
        start_ms=START_MS,
        end_ms=NOMINAL_END_MS,
        seed=7,
        opening_balance="10000",
        leverage=10,
        maker_rate="0.0002",
        taker_rate="0.0005",
        fee_source="default",
        latency={"model": "fixed", "submit_ms": SUBMIT_LATENCY_MS, "cancel_ms": 120},
        fill_tier="BAR_CLOSE",
        fill_model=fill_model_for_tier("BAR_CLOSE").to_json(),
        liquidation_recovery_pct="0.005",
        timeout_s=0.0,
        engine_version=ENGINE_VERSION,
        risk_limits={},
        auto_flatten={},
        kill_switch_flatten=False,
        source="",
        endpoint="testnet",
        reorder_buffer_ms=WINDOW_MS,
        session_kind="paper",
    )
    return store.create(
        strategy_id=strategy_id,
        version_id=version_id,
        spec=spec.to_storage(),
        label="paper session",
        mode="paper",
    )


def test_a_session_worker_discharges_both_of_its_end_of_session_obligations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole worker path, ending where spec 6.7.1 and spec 7.6 say it ends.

    `create_shadow` and `write_parity` had no caller anywhere in the platform, so
    `GET /runs/{id}/parity` answered 404 for every session ever run and its own message --
    "one is written when its shadow backtest finishes" -- described a condition no code path
    could produce. `KillSwitchStore.arm` had one caller, the API's manual red button. Both
    are obligations of *session end*, so both are pinned at the only place that owns it.

    The real `execute_session` runs here with the socket replaced by a coroutine that only
    waits and the control file already written, so the session observes nothing and stops on
    its first pass. A session that saw nothing has no shadow, by `shadow_spec`'s own refusal
    -- so the property asserted is the one that holds either way: the run finishes `done`
    with its artefacts intact, and the reason it has no parity report is written on the run
    rather than left to be guessed at. Under the unwired version there is no shadow, no
    report, no note, and nothing consults the persistent switch at all.
    """
    monkeypatch.setattr(live_session, "_now_ms", _Clock(STOP_MS))
    monkeypatch.setattr(LiveFeed, "run", lambda self, stop: _wait_only(stop))
    armed: list[int] = []
    real_arm = live_worker._arm_kill_switch

    def recording(root: Path, rid: int, session: PaperSession) -> None:
        armed.append(rid)
        real_arm(root, rid, session)

    monkeypatch.setattr(live_worker, "_arm_kill_switch", recording)
    store = RunStore(tmp_path)
    try:
        run_id = _seed_run(tmp_path, store)
        store.request_stop(run_id, flatten=False, reason="asked to stop")

        live_worker.execute_session(tmp_path, run_id, store, {})

        summary = store.get(run_id)
        assert summary.status == RunStatus.DONE, summary.error
        notes = [w for w in summary.warnings if "6.7.1" in w]
        assert len(notes) == 1
        assert "no dispatched events" in notes[0]
        metrics = store.read_json(run_id, "metrics.json")
        assert notes[0] in metrics["summary"]["warnings"]
        assert armed == [run_id], "session end must reach the persistent kill switch"
    finally:
        store.close()

    # Nothing halted, so the switch stays clear and the next session may start.
    with KillSwitchStore(tmp_path) as switch:
        assert switch.state() is None


def test_the_engine_learns_the_funding_schedule_from_the_live_feed(tmp_path: Path) -> None:
    """One `markPriceUpdate` frame announcing `T`, and the engine's schedule now holds it.

    A backtest is handed every settlement instant by `LakeSource.prepare` before it starts.
    A live session has no such table and cannot have one: the instants are only knowable from
    `premiumIndex.nextFundingTime` as the session runs. With nothing wiring the observation
    into the engine, `_next_funding_ms` returns `None` for every symbol, `_flatten_reason`
    returns `None`, and `AutoFlatten.before_funding_ms` -- an exit the operator configured by
    name -- never fires, with no error and nothing in the log to say so. The shadow, replaying
    a tape whose `meta.json` *does* carry the schedule, would then honour a platform exit its
    own session had ignored.

    The frame announces `T = START_MS + 8 h`, so that is the instant the engine must report as
    the next settlement for a query made at `START_MS`. Before the frame there is no schedule
    at all, which is what makes the assertion about the frame rather than about a default.
    """
    announced = START_MS + 8 * 60 * 60 * 1000
    session = paper_session(tmp_path, BuyAndHold())
    try:
        assert session.engine._next_funding_ms(SYMBOL, START_MS) is None

        session.feed._on_message(
            "btcusdt@markPrice@1s",
            {
                "e": "markPriceUpdate",
                "E": START_MS,
                "s": SYMBOL,
                "p": PRICE,
                "r": "0.00010000",
                "T": announced,
            },
            START_MS,
        )

        assert session.engine._next_funding_ms(SYMBOL, START_MS) == announced
    finally:
        session.tape.close()
