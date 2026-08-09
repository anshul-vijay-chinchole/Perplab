"""The paper session: one strategy, one wall clock, forty-eight hours (spec 13, Phase 7).

**This is not an engine.** `BacktestEngine` is the engine, and a paper session runs the
same one -- same order lifecycle, same risk checks, same ledger, same trade builder, same
metrics. What lives here is the loop that a backtest gets for free from an exhaustible queue
and a live session cannot: read frames as they arrive, hold them briefly so spec 6.2's total
order can still be applied, dispatch them, and checkpoint the artefacts often enough that a
crash at hour forty-seven is not a lost run.

```
LiveFeed --event,recv_ms--> ReorderBuffer --released, in 6.2 order--> TapeWriter
                                                                 and> Engine.step
```

Three properties this arrangement buys, each of which is the answer to a way Phase 7 could
have quietly given a wrong answer:

**The shadow backtest sees the identical dispatch order.** The tape is written from the
buffer's *output*, not its input, so it records what the engine actually processed. Replaying
it through the same `EventQueue` reproduces the run exactly, and every difference the parity
report then shows is attributable to the fill model -- which is what spec 6.7.2 wants it to
measure.

**A dead stream cannot stall the session.** The reorder watermark is wall clock, never
max-observed-exchange-time. One quiet symbol on a thin testnet book would otherwise freeze
the engine holding an open position.

**Silence is not death.** The store presumes a run with no heartbeat for three minutes is a
corpse, and the backtest worker's only heartbeat is its progress callback -- which is driven
by event volume. An event-driven session on a quiet market goes minutes without events, so
the heartbeat here is on a wall-clock timer instead. Without that, a live session gets marked
`lost`, `lost` is terminal, the UI hides its stop control, and it keeps trading unreachable.

**A session ends the way a backtest ends.** `BacktestEngine.run` is the composition
`start -> step* -> drain -> finish -> result`, and this loop used to run only `start`, `step`
and `result`. Every phase it skipped was load-bearing: `drain` is the sole dispatcher of
`on_stop`, `finish` is the sole caller of `Account.reconcile`, and `finish` is the only place
`effective_end_ms` is ever moved off the *nominal* end -- which for a session the API builds
is forty-eight hours away by default. So a five-minute session published exposure, Sharpe,
volatility and CAGR measured over forty-eight hours it never observed (exposure wrong by
15.6x on one harness and 35.8x on a stored run, a Sharpe of three real hourly returns padded
with forty-four fabricated zeros), a strategy that flattened in `on_stop` reported the
position still open, and the ledger's one independent replay of its own event log never ran
on the mode that trades a live market. `_end` below is that composition, with the one
substitution a wall clock forces -- see `_finalise`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from perplab.core.invariants import InvariantViolation
from perplab.core.types import CollectorEventKind
from perplab.data.rest_poller import REST_DATASETS
from perplab.engine.backtest import BacktestConfig, BacktestEngine
from perplab.store.runs import PROGRESS_EQUITY_NAME
from perplab.engine.clock import Event
from perplab.engine.reorder import ReorderBuffer, SequenceAllocator
from perplab.engine.tape import TapeWriter
from perplab.exchange.userstream import USER_STREAM_LABEL
from perplab.live.feed import PRODUCTION, TESTNET, LiveFeed, Venue
from perplab.live.stack import LiveStack
from perplab.strategy.base import Strategy
from perplab.strategy.params import Requirements

__all__ = [
    "SessionConfig",
    "StopRequest",
    "PaperSession",
    "CONTROL_FILENAME",
    "HEARTBEAT_INTERVAL_S",
    "CHECKPOINT_INTERVAL_S",
    "STEP_INTERVAL_S",
    "DISCONNECT_CHECK_INTERVAL_S",
    "LIVE_REPORT_GRACE_S",
    "LIVE_BOOK_CLEAR_WAIT_S",
    "LIVE_FLATTEN_WAIT_S",
    "LIVE_SHUTDOWN_GRACE_S",
    "POLLED_SOURCES",
    "VENUES",
]

log = logging.getLogger("perplab.live.session")

CONTROL_FILENAME = "control.json"
"""How the API asks a running session to stop.

A file rather than a signal or a socket. `RunStore.cancel` calls `Popen.terminate`, which on
Windows is `TerminateProcess` -- no `finally`, no `atexit`, so a session killed that way
never gets to cancel its resting orders at the exchange, which is items 1 and 2 of spec 7.3.
A file also survives an API restart, and the API is single-process and synchronous by
written policy, so it has nothing to hold a socket open with.
"""

HEARTBEAT_INTERVAL_S = 30.0
"""Wall-clock liveness, independent of event volume. See the module docstring."""

CHECKPOINT_INTERVAL_S = 60.0
"""How often the artefacts are republished. A crash costs at most this much visibility --
never any *data*, because the tape is appended continuously and is what the run is rebuilt
from."""

EQUITY_INTERVAL_S = 5.0
"""How often the in-progress equity curve is republished.

Twelve times more often than `CHECKPOINT_INTERVAL_S`, and deliberately *not* folded into it.
A checkpoint reserialises every trade the session has closed and runs a SQLite UPDATE, so its
cost grows with the session; at 60 s over 48 hours that is already the right trade. This
writes one thinned float array and nothing else, so it can be cheap and frequent instead --
which is what a chart needs to look alive rather than to step once a minute."""

STEP_INTERVAL_S = 0.1
"""How often the buffer is drained into the engine. Well under the 250 ms reorder window, so
the window rather than this interval is what decides an event's latency."""

DISCONNECT_CHECK_INTERVAL_S = 1.0
"""How often spec 7's disconnect trigger is evaluated **while the socket is still down**.

Spec 7's default ceiling is 30 s, so a one-second cadence costs at most one second of
overshoot on the halt instant and thirty evaluations per outage, each of which is an integer
comparison. See `_check_disconnect` for why the evaluation cannot wait for the reconnect.
"""

POLLED_SOURCES: frozenset[str] = frozenset(LiveFeed.SLOW_SOURCES) | REST_DATASETS
"""Dataset names a *poller* reports a failure under, as opposed to the socket's own label.

`LiveFeed._report_poll_failure` and `StreamManager` share one `on_status` callback, and the
only thing that separates them is the name in the `stream` field: a poller names the dataset
it was polling (`klines`, `markPrice`, ...), the socket names the joined stream list it
subscribed to. Composed from the feed's own declaration of its late sources and the
collector's `REST_DATASETS` rather than written out again here, so a poller added to either
list is covered without a second edit -- the failure this guards against is silent.

**Why they must not share one downtime clock.** Both emit `DISCONNECT`; only the socket ever
emits `CONNECT`/`RECONNECT`, and that is what used to clear the latch. So a single kline 429
started the *socket's* downtime clock and nothing turned it off until the next socket
reconnect. Measured: one REST failure at t=0, then a genuine 2 000 ms socket outage ninety
seconds later, halted a healthy session on an observed 92 000 ms against a 30 s ceiling --
46x the real outage -- and `monitor()` reported the market feed down while it was up, for as
long as the socket stayed connected (Binance recycles connections on a 24-hour cycle). The
collector reached the same conclusion for the same reason and keeps two independent
baselines: `_poller_started_s` against `_connected_at_s`.
"""

VENUES: dict[str, Venue] = {"testnet": TESTNET, "production": PRODUCTION}

LIVE_REPORT_GRACE_S = 5.0
"""How long the live end sequence waits for in-flight order reports to land.

Bounded because a report that never comes must not hold a stopping session open forever,
and short because the orders it waits for are market exits from `on_stop`, whose fills
arrive in network time. An order still unresolved after this window is recorded as such
and settled by the final reconciliation pass rather than by more waiting.
"""

LIVE_BOOK_CLEAR_WAIT_S = 10.0
"""How long to wait, after a venue-side cancel-all, for the CANCELED reports to end the
local orders. The venue acknowledged the cancel-all before this wait begins, so what is
being waited for is only the user-data stream's delivery of the outcomes -- anything still
open locally afterwards is swept with a loud note, because the venue has already said the
book is clear."""

LIVE_FLATTEN_WAIT_S = 30.0
"""How long a stop or halt with flatten armed waits for real exits to fill.

A reduce-only market order on a liquid perp fills in well under a second; thirty seconds is
the allowance for a testnet book at its thinnest. A position still open past this window is
reported as exactly that -- open at the exchange, needing attention -- rather than waited
on indefinitely by a session whose operator pressed stop."""

LIVE_SETTLE_POLL_S = 0.25
"""How often a bounded settle wait re-checks its condition. The reports it waits for are
booked inline by the user-data stream's own task, so all this loop does between checks is
yield the event loop and keep the transport drained."""

LIVE_SHUTDOWN_GRACE_S = 10.0
"""How long the live-stack tasks get to end on their own after `live_stop` is set.

The user-data stream's shutdown path closes its listen key at the exchange, and a hard
cancel would skip that close -- leaving a key alive that the next session's create would
then be handed back (Binance returns the active key rather than minting another)."""

_MAX_KIND = 1 << 30
_MAX_SEQ = 1 << 62
"""Upper bounds for the synthetic horizon key built from the release watermark.

The watermark is a timestamp, and the queue is ordered on the full spec-6.2 four-tuple, so
"everything at or before this instant" has to be expressed as a key that sorts after every
real event sharing that millisecond. These are those saturating components; the string
`"\\uffff"` is the fourth.
"""


@dataclass(frozen=True, slots=True)
class SessionConfig:
    """What a paper session needs beyond a `BacktestConfig`."""

    run_id: int
    endpoint: str = "testnet"
    """`testnet` or `production`. Spec 6.1 puts paper on testnet; production is permitted
    here for a data-only session, which never sends an order anywhere."""

    mode: str = "paper"
    """`paper` or `live` -- whether accepted orders reach a real venue.

    Paper runs the engine's simulated transport against the live feed; live runs
    `ExchangeTransport` against the endpoint above, and requires `attach_live_stack` to
    have been called before `run`. The two are the same session in every other respect,
    which is spec 6.1's table holding: the mode changes the order destination and the
    fill source, and nothing else."""

    reorder_window_ms: int = 250
    max_runtime_s: float = 0.0
    """Wall-clock ceiling, or 0 for none. A 48-hour exit criterion needs 172 800."""

    flatten_on_stop: bool = False
    """Whether a stop closes the positions as well as the book. Spec 7.3's default is
    cancel-only, and so is this.

    The default for this session; one stop may override it, because `POST /kill` carries
    close-all as a per-press choice and writes it into `control.json`. See `StopRequest`.
    """

    def venue(self) -> Venue:
        try:
            return VENUES[self.endpoint]
        except KeyError:
            raise ValueError(
                f"unknown endpoint {self.endpoint!r}; expected one of "
                f"{sorted(VENUES)}. A session cannot be started against an endpoint whose "
                "stream capabilities have not been measured -- see perplab.live.feed."
            ) from None


@dataclass(frozen=True, slots=True)
class StopRequest:
    """What the API asked for when it wrote `control.json`.

    `RunStore.request_stop` writes `flatten` on every stop and `POST /kill` sets it from the
    operator's close-all choice. The session used to read `stop` and `reason` and discard it,
    so a close-all kill cancelled nothing and closed nothing while `KillSwitchStore` recorded
    "the account was left flat" -- a safety statement made from the request rather than from
    the outcome.
    """

    reason: str
    flatten: bool


class PaperSession:
    """Drives one strategy against a live market on a wall clock.

    Single use. Build one, `await run(stop)`, then read `engine.result(...)`.
    """

    def __init__(
        self,
        *,
        run_dir: Path | str,
        strategy: Strategy,
        requirements: Requirements,
        config: BacktestConfig,
        session: SessionConfig,
        filters: dict[str, Any],
        brackets: dict[str, Any],
        flags: Sequence[str] = (),
        heartbeat: Callable[[], None] | None = None,
        checkpoint: Callable[[PaperSession], None] | None = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.session = session
        self.config = config
        self._heartbeat = heartbeat
        self._checkpoint = checkpoint
        if session.mode not in ("paper", "live"):
            raise ValueError(
                f"unknown session mode {session.mode!r}; expected 'paper' or 'live'. A "
                "session whose mode is neither would trade somewhere nobody named."
            )

        venue = session.venue()
        self.allocator = SequenceAllocator()
        self.buffer = ReorderBuffer(window_ms=session.reorder_window_ms)
        self.tape = TapeWriter(
            self.run_dir,
            meta={
                "run_id": session.run_id,
                "mode": session.mode,
                "endpoint": venue.name,
                "ws_base": venue.ws_base,
                "rest_base": venue.rest_base,
                "reorder_buffer_ms": session.reorder_window_ms,
                "mark_aggregation_ms": 60_000,
                "depth_downsample_ms": 1_000,
                "fill_tier": config.fill_tier.name,
                "symbols": list(config.symbols),
                "timeframe": config.timeframe,
                "data_start_ms": config.start_ms,
                "started_ms": _now_ms(),
            },
        )
        self.feed = LiveFeed(
            config.symbols,
            venue=venue,
            timeframe_ms=_timeframe_ms(requirements),
            on_event=self._offer,
            on_status=self._on_status,
            allocator=self.allocator,
            on_complete_through=self._complete_through,
            on_funding_time=self._note_funding_time,
        )
        # Sources whose events are discovered after the fact hold the release watermark back
        # rather than being dropped as late. Without this the engine sees no bar closes at
        # all -- measured, not theorised. See `ReorderBuffer.expect`.
        for source, stale_after_ms in LiveFeed.SLOW_SOURCES.items():
            self.buffer.expect(source, stale_after_ms=stale_after_ms)
        # **The same engine a backtest runs.** Its source supplies no streams -- events are
        # pushed onto its queue as they are released -- and its transport starts as the
        # default simulator, so a paper session's fills are priced by exactly the models
        # spec 6.4 defines and booked by exactly the ledger spec 3.3 defines. A live session
        # replaces the transport with `ExchangeTransport` via `attach_live_stack` before
        # `run` -- the one substitution spec 6.1's table permits -- and everything else is
        # this same object either way.
        self.engine = BacktestEngine(
            root=self.run_dir,
            strategy=strategy,
            requirements=requirements,
            config=config,
            filters=filters,
            brackets=brackets,
            flags=(*flags, "LIVE" if session.mode == "live" else "PAPER"),
            source=_PushSource(),
        )
        # **The event-log cap becomes a named stop, never an exception.** `EngineRuntime.
        # emit` raises `EventLogFull` when the cap is hit and no handler is registered --
        # right for a backtest, where nothing is at risk, and catastrophic here: the
        # exception unwinds out of a strategy hook, through the pump, and the end sequence
        # itself emits (the stop's cancels, a halt's KILL_SWITCH), so the teardown would
        # die partway with orders working at a venue and no final reconciliation recorded.
        # The handler asks the pump for an orderly stop; events past the cap are dropped,
        # which is the ceiling's own contract, and the stop reason says the log was capped
        # so a truncated log is never read as a complete one.
        self.engine.runtime.on_log_full = self._note_log_full
        self._log_full = False

        # **Live sessions cannot move leverage from strategy code.** Paper keeps the hook
        # because its ledger *is* the account. Live drops it because the account is
        # Binance's: `preflight.configure_account` sets the venue's leverage once, before
        # the first order, and `ExchangeTransport` refuses to exist unless the ledger agrees
        # with that preflight. Writing the ledger's copy afterwards would leave the platform
        # sizing margin and solving liquidation prices against a number the venue never
        # heard, and the first sign of it would be a real liquidation arriving before the
        # displayed one. `ctx.set_leverage` raises there instead, naming the session form.
        if session.mode == "live":
            self.engine.runtime.leverage_hook = None

        self.status_log: list[dict[str, Any]] = []
        self.stopped_reason: str | None = None
        self.processed = 0
        self.ended_ms: int | None = None
        """The instant the session stopped observing, once `_finalise` has decided it.

        The tape is sealed at the same instant and `shadow_spec` reads it back as the
        shadow's `end_ms`, so the session and its own shadow measure the same window. They
        did not: the shadow used the observed end and the session used the nominal one, and
        the parity report compares fills and PnL, never metrics, so nothing surfaced it.
        """
        self._market_down_since: float | None = None
        self._last_disconnect_check = 0.0
        self._disconnect_halted = False
        self._poller_down: set[str] = set()
        self._mark_down_since: float | None = None
        """When the mark poller last failed, or `None` while it delivers. Only ever set on
        an endpoint where the poller is the *sole* mark source (`feed.polls_marks`) --
        there, a dead poller freezes every number the mark feeds, which is spec 7's
        disconnect state arriving through a poller instead of a socket. See `_on_status`."""
        self._mark_disconnect_halted = False
        self._flatten_on_stop = session.flatten_on_stop
        self._ledger_broken = False
        self._last_status_ms = 0
        self._started = False

        self._live: LiveStack | None = None
        self._user_down_since: float | None = None
        """When the user-data stream last dropped, or `None` while it is up. Tracked apart
        from `_market_down_since` on purpose -- see `exchange_event`."""
        self.final_reconciliation: dict[str, Any] | None = None
        """The last spec 6.7.3 pass, taken after the book was closed and before the ledger
        finalised. `None` for a paper session and for a live one whose pass could not run;
        the manifest records which."""
        self.open_at_exchange: dict[str, Any] | None = None
        """What `GET /fapi/v1/openOrders` still showed per symbol at session end, or `None`
        when the question was not (or could not be) asked. An empty dict is the good answer
        and is distinct from `None`, for `ReconciliationPass.fetched`'s reason."""

    # --------------------------------------------------------------------- live stack

    def attach_live_stack(self, stack: LiveStack) -> None:
        """Give a live session its exchange stack. Must happen before `run`.

        The transport needs the engine it feeds, and the engine is built by `__init__` --
        so the worker constructs the session first, runs the preflight, builds the stack
        around `self.engine`, and hands it here. Assigning `engine.transport` is the one
        substitution spec 6.1's table permits, and doing it before `run` means it is in
        place before `engine.start()` gives strategy code its first chance to submit.
        """
        if self.session.mode != "live":
            raise ValueError(
                f"this session's mode is {self.session.mode!r}; a live stack on a paper "
                "session would send its orders to a real venue the operator never chose."
            )
        if self._live is not None:
            raise ValueError("a live stack is already attached; a session is single use.")
        if self._started:
            raise ValueError(
                "the session has already started; attaching a transport now would switch "
                "venues mid-run."
            )
        self._live = stack
        self.engine.transport = stack.transport

    def exchange_event(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int = 0
    ) -> None:
        """Status sink for the live stack: transport, user-data stream and reconciler.

        Deliberately **not** `_on_status`. That sink feeds spec 7's market-feed disconnect
        trigger, and these sources must never reach it: a user-stream drop or a
        reconciliation timeout says nothing about the market feed, and counting it there
        would halt a session whose market data is perfectly healthy -- the same conflation
        `POLLED_SOURCES` exists to prevent for the REST pollers, arriving through a third
        door. A user-stream outage is tracked on its own clock for the monitor; while it
        lasts, fills are invisible on the fast path, and the reconciler's 60-second pass is
        the designed net underneath (`live.reconcile`'s module docstring is the argument).
        """
        self._note_status(kind, stream, detail, downtime_ms)
        if stream == USER_STREAM_LABEL:
            if kind is CollectorEventKind.DISCONNECT:
                if self._user_down_since is None:
                    self._user_down_since = time.monotonic()
            elif kind in (CollectorEventKind.CONNECT, CollectorEventKind.RECONNECT):
                self._user_down_since = None

    # --------------------------------------------------------------------------- run

    async def run(self, stop: asyncio.Event) -> None:
        """Stream, dispatch and checkpoint until `stop` is set or a limit ends it."""
        if self.session.mode == "live" and self._live is None:
            raise RuntimeError(
                "a live session cannot run without its exchange stack: no preflight was "
                "run, no transport exists, and orders would be accepted into a ledger with "
                "nothing to send them. Call attach_live_stack first."
            )
        self._started = True
        self.engine.start()
        started = time.monotonic()
        deadline = (
            started + self.session.max_runtime_s
            if self.session.max_runtime_s > 0
            else float("inf")
        )
        feed_task = asyncio.create_task(self.feed.run(stop), name="live-feed")
        live = self._live
        live_stop = asyncio.Event()
        live_tasks: list[asyncio.Task[None]] = []
        if live is not None:
            # On their own stop event, not the session's. The end-of-run sequence needs the
            # sender and the report stream *alive after `stop` is set* -- the venue-side
            # cancels, the real exits and their fills all happen inside the window between
            # the two events, and sharing one event would end the order path at the exact
            # moment it is most needed.
            live_tasks = [
                asyncio.create_task(live.transport.run(live_stop), name="live-transport"),
                asyncio.create_task(live.stream.run(live_stop), name="live-userstream"),
                asyncio.create_task(live.reconciler.run(live_stop), name="live-reconcile"),
            ]
        try:
            await self._pump(stop, deadline)
        except InvariantViolation as exc:
            self._note_invariant_failure(exc)
            raise
        finally:
            stop.set()
            feed_task.cancel()
            try:
                await feed_task
            except asyncio.CancelledError:
                pass
            try:
                # **Nothing is dispatched after the ledger has disagreed with itself.** The
                # final drain used to run regardless, with `engine.halted` still false, so an
                # `on_bar` ran again, a second order was submitted and a second fill was
                # booked *after* the invariant failure -- which is the one sentence spec 3.10
                # and spec 7 are quoted for: an accounting engine that has lost track of
                # state must not keep sending orders.
                if not self._ledger_broken:
                    try:
                        if live is not None:
                            await self._end_live()
                        else:
                            self._end()
                    except InvariantViolation as exc:
                        self._note_invariant_failure(exc)
                        raise
            finally:
                # The stack ends on its own event, gently first: the stream's shutdown path
                # closes its listen key at the exchange, and a hard cancel would skip it.
                live_stop.set()
                if live_tasks:
                    _done, pending = await asyncio.wait(
                        live_tasks, timeout=LIVE_SHUTDOWN_GRACE_S
                    )
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*live_tasks, return_exceptions=True)

    async def _pump(self, stop: asyncio.Event, deadline: float) -> None:
        last_beat = 0.0
        last_check = time.monotonic()
        last_equity = 0.0
        while not stop.is_set():
            now = time.monotonic()
            if now >= deadline:
                self.stopped_reason = "the session reached its configured runtime"
                return
            if self.engine.halted:
                self.stopped_reason = "the risk layer halted the run"
                return

            self._drain_to_engine()
            self._check_disconnect(now)

            if self._log_full:
                self.stopped_reason = self.stopped_reason or (
                    "the event log reached its cap, so the session stopped in order "
                    "rather than losing its remaining records to a crash; the log is "
                    "truncated at the cap and says so"
                )
                return

            if self._live is not None and self._live.keys.expired():
                # Spec 11's idle expiry, enforced by the worker's own copy of the credential.
                # Unreachable while the reconciler is signing every sixty seconds -- which is
                # `exchange.keys`' intended reading -- so reaching it means the session has
                # gone twelve hours without one signed request and can no longer prove
                # anything about the account it is trading. Positions are left open
                # deliberately: spec 11 says a forced close on expiry is worse than the
                # exposure, so this stop overrides any flatten preference.
                self._flatten_on_stop = False
                self.stopped_reason = (
                    "the worker's API key session idled past its timeout (spec 11), so this "
                    "session can no longer sign requests. Positions are left open "
                    "deliberately and still need attention."
                )
                return

            if now - last_beat >= HEARTBEAT_INTERVAL_S:
                last_beat = now
                if self._heartbeat is not None:
                    self._heartbeat()
            if now - last_equity >= EQUITY_INTERVAL_S:
                last_equity = now
                self.engine.equity_snapshot().publish(
                    self.run_dir / PROGRESS_EQUITY_NAME
                )
            if now - last_check >= CHECKPOINT_INTERVAL_S:
                last_check = now
                if self._checkpoint is not None:
                    self._checkpoint(self)
            request = self._read_control()
            if request is not None:
                self.stopped_reason = request.reason
                self._flatten_on_stop = request.flatten
                return

            if await _sleep_or_stop(stop, STEP_INTERVAL_S):
                self.stopped_reason = self.stopped_reason or "asked to stop"
                return

    # ------------------------------------------------------------------------ ending

    def _end(self) -> None:
        """Spec 5.2's end of run: `on_stop`, the stop sequence, then the final accounting.

        The same three phases `BacktestEngine.run` composes after its loop, in the same
        order and through the same methods -- what a wall clock changes is only *when* they
        happen and *where* the run is measured to, never what they do (spec 6.1).
        """
        engine = self.engine
        self._drain_to_engine(final=True)
        # **`drain` is the engine's, not a copy.** It is the only dispatcher of `on_stop`,
        # and a strategy that flattens there is doing something legitimate: the closing
        # order has to actually fill, or the run reports a position the strategy closed.
        # It did exactly that -- round_trips 0, one phantom open trade, every per-trade
        # ratio `None`, and a shadow (which runs `drain`) that closed the position and was
        # therefore reported as a fill-model divergence of +1 fill and -3.66 PnL.
        #
        # `halted=True` means "do not process what is left". In a backtest that is remaining
        # market data; here it is the arrivals the session scheduled ahead of the clock, and
        # after a halt the risk layer refuses them anyway.
        self.processed += engine.drain(halted=engine.halted)
        if engine.perform_pending_halt():
            self.stopped_reason = self.stopped_reason or "the risk layer halted the run"
        if not engine.halted:
            # A halt has already run the same sequence, with the operator's flatten choice
            # taken from `kill_switch_flatten`; running it twice would cancel an empty book.
            self._close_book()
        self._finalise()

    def _close_book(self) -> None:
        """Spec 7.3 items 2 and 3 on the stop path: cancel the book, optionally flatten.

        `CONTROL_FILENAME`'s docstring says the file exists so that a stopping session
        "gets to cancel its resting orders at the exchange, which is items 1 and 2 of spec
        7.3". Nothing did: `flatten_on_stop` was set by the worker and read by nothing, the
        control file's `flatten` key was parsed and discarded, and a stopped session ended
        with every resting order still on the book and every position still open -- while
        `KillSwitchStore.require_clear` told the next operator "the account was left flat."

        The engine's own removal and forced-exit paths rather than a second copy of them: an
        order taken off the book without `_remove` would never reach `on_cancel`, never leave
        the trade builder and never appear in the event log, and spec 6.1 puts the order
        lifecycle in exactly one place. These are private to `BacktestEngine` because the
        halt path is their only other caller -- and a stop is the same sequence.
        """
        engine = self.engine
        engine._cancel_all(None, reason="the session stopped")
        if self._flatten_on_stop:
            ts_ms = engine.runtime.now_ms
            # Snapshotted before the loop -- `_force_flatten` deletes what it closes, and in
            # hedge mode a symbol has two entries, so iterating the live dict would skip one.
            for key in engine._open_keys():
                engine._force_flatten(ts_ms, key, reason="stop:flatten")
        # `_drain_order_ends` normally runs at the tail of `_dispatch`, and this runs outside
        # dispatch -- so without it the `on_cancel` notifications are built and thrown away.
        engine._drain_order_ends()

    # -------------------------------------------------------------------- live ending

    async def _end_live(self) -> None:
        """Spec 5.2's end of run against a real venue: the same phases, wire-shaped.

        `_end` is synchronous because against the simulator every outcome is the engine's
        own model -- "cancel" removes, "flatten" books, immediately and locally. Against a
        real exchange three of those sentences are *requests*: an order ends when the venue
        reports it ended, a position closes when its exit fills, and both answers arrive on
        the user-data stream. So this method is the same composition with waits where the
        wire is, and the live-stack tasks are still running throughout -- `run` keeps them
        alive on their own stop event precisely so this sequence has a sender and a report
        stream to work with.

        Every wait is bounded, and what a timeout leaves behind is recorded rather than
        resolved by assumption: an order the venue never answered for stays in
        `transport.unresolved`, a position that did not close stays in the warnings, and
        the final reconciliation pass is the measurement the manifest keeps either way.
        """
        engine = self.engine
        self._drain_to_engine(final=True)
        # `drain` dispatches `on_stop`; a strategy that flattens there submits real orders
        # through the live transport. On the halt path this is where `_perform_halt`'s
        # queued cancels and exits are already waiting to be sent.
        self.processed += engine.drain(halted=engine.halted)
        if engine.perform_pending_halt():
            self.stopped_reason = self.stopped_reason or "the risk layer halted the run"
        await self._transport_flush()
        await self._settle_until(self._no_open_orders, LIVE_REPORT_GRACE_S)
        if engine.halted:
            await self._settle_halt()
        else:
            await self._close_book_live()
        await self._record_final_state()
        self._finalise()

    async def _close_book_live(self) -> None:
        """Spec 7.3 items 1-3 at a real venue: cancel at the exchange, then flatten for real.

        **Venue first, ledger second.** Removing an order locally while the venue still
        works it opens a window where its fill arrives, finds the order terminal, and is
        dropped as a duplicate -- a position at the exchange with nothing in the ledger to
        say so. Cancelling at the venue first closes that window: once the cancel-all is
        acknowledged nothing can fill, the CANCELED reports end the local orders through
        the ordinary path, and the sweep below only touches orders whose reports were lost
        -- with the venue already on record as having cleared the book.
        """
        engine = self.engine
        live = self._live
        assert live is not None  # only reachable from _end_live
        for symbol in dict.fromkeys(self.config.symbols):
            await live.transport.cancel_all_at_exchange(symbol)
        cleared = await self._settle_until(self._no_open_orders, LIVE_BOOK_CLEAR_WAIT_S)
        if not cleared:
            self._note_status(
                CollectorEventKind.STALE,
                "orders",
                f"{sum(1 for o in engine.orders.values() if o.is_open)} order(s) were "
                f"still open locally {LIVE_BOOK_CLEAR_WAIT_S:g}s after the venue accepted "
                f"cancel-all; their CANCELED reports did not arrive. Removing them locally "
                f"-- the venue has already said the book is clear.",
            )
        engine._cancel_all(
            None, reason="the session stopped; the venue book was cleared with cancel-all"
        )
        engine._drain_order_ends()
        if not self._flatten_on_stop:
            return
        ts_ms = engine.runtime.now_ms
        for key in engine._open_keys():
            engine._transport_flatten(ts_ms, key, reason="stop:flatten")
        await self._transport_flush()
        flat = await self._settle_until(
            lambda: not engine.account.positions, LIVE_FLATTEN_WAIT_S
        )
        if not flat:
            still_open = ", ".join(
                f"{p.symbol} {p.position_side.value}"
                for p in engine.account.positions.values()
            )
            engine.warnings.append(
                f"the stop asked to flatten, but {still_open} did not close within "
                f"{LIVE_FLATTEN_WAIT_S:g}s. The position(s) may still be open at the "
                f"exchange -- check the final reconciliation and the venue directly."
            )

    async def _settle_halt(self) -> None:
        """Finish what `_perform_halt` started: it queued, the venue must now answer.

        The engine's halt already sent every open order a cancel through the transport and,
        with flatten armed, a real exit per position -- all queued, all flushed by
        `_end_live` before this runs. What is left is the backstop and the waiting:
        cancel-all catches anything the per-order pass could not name (an order from a
        crashed predecessor, one placed by hand), and the waits give the venue's answers
        time to land before the sweep records what never arrived.
        """
        engine = self.engine
        live = self._live
        assert live is not None  # only reachable from _end_live
        for symbol in dict.fromkeys(self.config.symbols):
            await live.transport.cancel_all_at_exchange(symbol)
        if engine.risk.kill_switch.flatten:
            flat = await self._settle_until(
                lambda: not engine.account.positions, LIVE_FLATTEN_WAIT_S
            )
            if not flat:
                still_open = ", ".join(
                    f"{p.symbol} {p.position_side.value}"
                    for p in engine.account.positions.values()
                )
                engine.warnings.append(
                    f"the halt asked to flatten, but {still_open} did not close within "
                    f"{LIVE_FLATTEN_WAIT_S:g}s. The position(s) may still be open at the "
                    f"exchange -- check the final reconciliation and the venue directly."
                )
        else:
            await self._settle_until(self._no_open_orders, LIVE_BOOK_CLEAR_WAIT_S)
        if not self._no_open_orders():
            engine._cancel_all(
                None, reason="halted; the venue book was cleared with cancel-all"
            )
        # Unconditionally, not only after the sweep: the CANCELED reports that arrived
        # *during* the settle wait ran `_remove` -> `_note_order_end`, and post-halt
        # nothing dispatches again -- so on the prompt path (the good one) the `on_cancel`
        # notifications were built and never delivered. The simulated halt drains for
        # exactly this reason.
        engine._drain_order_ends()

    async def _record_final_state(self) -> None:
        """One last look at the venue, recorded rather than enforced (spec 6.7.3).

        Taken *before* `_finalise` so the closing equity sample includes every fill the
        settle window landed, and recorded whatever it says: a session that ends with the
        exchange disagreeing about the account must publish that disagreement, because it
        is the single most important fact about the run. A mismatch here also arms the
        kill switch through the ordinary path (`observe_reconciliation` -> `request_halt`),
        so the next session cannot start against the disputed account until a person looks.
        """
        engine = self.engine
        live = self._live
        assert live is not None  # only reachable from _end_live
        final = await live.reconciler.check_once()
        self.final_reconciliation = final.to_json()
        if final.mismatches and engine.perform_pending_halt():
            self.stopped_reason = self.stopped_reason or (
                "the final reconciliation pass disagreed with the exchange"
            )
            # The halt's live arm just queued transport cancels and, with flatten armed,
            # real exits -- *after* every settle phase already ran. Left unflushed, the
            # KILL_SWITCH event would name exits that never left the process; flushed but
            # unawaited, their fills would land after `_finalise` took the closing equity
            # sample. So: send, wait bounded, and re-measure -- the post-settle pass is
            # the honest final record, and the mismatch that caused all this is already
            # preserved in the risk breach, the events and the pass recorded above being
            # overwritten only after its verdict was acted on.
            await self._transport_flush()
            if engine.risk.kill_switch.flatten:
                await self._settle_until(
                    lambda: not engine.account.positions, LIVE_FLATTEN_WAIT_S
                )
            else:
                await self._settle_until(self._no_open_orders, LIVE_BOOK_CLEAR_WAIT_S)
            self.final_reconciliation = (await live.reconciler.check_once()).to_json()
        try:
            leftovers: dict[str, Any] = {}
            for symbol in dict.fromkeys(self.config.symbols):
                rows = await live.client.open_orders(symbol)
                if rows:
                    leftovers[symbol] = [
                        {
                            "clientOrderId": str(row.get("clientOrderId", "")),
                            "orderId": row.get("orderId"),
                            "type": row.get("type"),
                            "side": row.get("side"),
                        }
                        for row in rows
                    ]
            self.open_at_exchange = leftovers
            if leftovers:
                engine.warnings.append(
                    f"the exchange still reports working order(s) after the stop sequence: "
                    f"{leftovers}. Cancel them by hand before starting another session."
                )
        except Exception as exc:  # noqa: BLE001 - "could not check" must be recorded as such
            self.open_at_exchange = None
            self._note_status(
                CollectorEventKind.STALE,
                "orders",
                f"could not list open orders at session end: {type(exc).__name__}: {exc}. "
                f"Whether the venue book is clear is unverified.",
            )

    def _no_open_orders(self) -> bool:
        return not any(order.is_open for order in self.engine.orders.values())

    async def _transport_flush(self) -> None:
        """Send everything the transport has queued, without letting a fault end the run."""
        live = self._live
        assert live is not None
        try:
            await live.transport.drain()
        except Exception as exc:  # noqa: BLE001 - the artefacts still have to publish
            self._note_status(
                CollectorEventKind.STALE,
                "orders",
                f"draining queued order requests failed: {type(exc).__name__}: {exc}. "
                f"Requests may be unsent; the final reconciliation is the check.",
            )

    async def _settle_until(self, done: Callable[[], bool], timeout_s: float) -> bool:
        """Wait for the wire to answer, up to `timeout_s`. Returns whether it did.

        The answers arrive through the user-data stream's own task -- `on_report` books
        fills and removals inline -- so this loop only has to yield the event loop and
        keep the transport drained (a report can trigger a follow-up request). Bounded
        always: a venue that never answers must leave a recorded gap, not a hung worker.
        """
        deadline = time.monotonic() + timeout_s
        while True:
            if done():
                return True
            if time.monotonic() >= deadline:
                return done()
            await self._transport_flush()
            await asyncio.sleep(LIVE_SETTLE_POLL_S)

    def _finalise(self) -> None:
        """Final mark-to-market and `Account.reconcile`, at the instant actually observed.

        **A session does not run out of data, it is stopped.** That is the one assumption
        `BacktestEngine._finalise` cannot make here. Its rule for an unhalted run -- finalise
        at `config.end_ms - 1`, because that is where the data ended -- is right for a
        backtest and catastrophic for a session: `api.routers.sessions` builds
        `end_ms = now + (max_runtime_s or 48h)` and `max_runtime_s` defaults to 0, so a
        session stopped after five minutes would sample equity forty-eight hours into a
        future nothing was observed in, and `effective_end_ms` would stay there. Every
        time-normalised metric then describes that window: `days` 2.0 for a three-hour run,
        exposure 6.2% for a strategy in the market 96.8% of the time, and a Sharpe of
        `sqrt(8760/47)` that is a pure function of the configured runtime -- identical for a
        session that gained 0.01% and one that gained 25%.

        So there are three cases, not two. A **halted** run is the case the engine already
        gets right, and `finish()` is called unchanged for it: it ends the run at the halt,
        shortens `effective_end_ms` and reconciles. A **stopped** session ends at the wall
        clock, which is the same instant the tape is sealed at and therefore the same window
        its shadow is measured over. The requested range is untouched either way --
        `config.end_ms` still records what was asked for.
        """
        engine = self.engine
        if engine.halted:
            engine.finish()
            self.ended_ms = engine.effective_end_ms
            return

        # One millisecond before the seal, so `effective_end_ms` (an exclusive bound, as
        # `_finalise` treats it) lands exactly on the tape's `ended_ms`. `max` with the
        # engine clock because `on_stop`'s order arrives at submit + latency, which can be a
        # few milliseconds past the wall clock, and `runtime.advance` refuses to go backwards.
        end_ms = max(engine.runtime.now_ms, _now_ms() - 1)
        engine.effective_end_ms = end_ms + 1
        self.ended_ms = end_ms + 1
        engine.runtime.advance(end_ms)
        # `_sample_equity` is spec 5.2's closing mark-to-market and carries spec 8.2's
        # intrabar band with it; re-deriving that band here would be a second implementation
        # of the drawdown sampling the metrics are computed from, which is the duplication
        # spec 6.1 forbids. Calling the engine's own is the smaller coupling.
        engine._sample_equity(end_ms)
        # The ledger's one independent check: `reconcile` replays the event log from the
        # opening balance and compares it against live state, so a fill mis-booked
        # identically into both the wallet and the totals -- the failure I1 is blind to --
        # surfaces here. Every backtest got it; the mode that trades a live market did not.
        engine.account.reconcile()

    def _note_log_full(self) -> None:
        """`EngineRuntime.on_log_full`: ask the pump for an orderly stop. Idempotent --
        `emit` calls this for every dropped event once the cap is reached, including the
        end sequence's own emits, and the first call already said everything."""
        if self._log_full:
            return
        self._log_full = True
        log.warning(
            "session %s: the event log reached its cap; stopping in order",
            self.session.run_id,
        )

    def _note_invariant_failure(self, exc: InvariantViolation) -> None:
        """Spec 7's first auto-trigger, which lived only inside `BacktestEngine.run`.

        The engine's own `except InvariantViolation` arm records the kill switch, raises
        `RISK_HALTED` and emits `KILL_SWITCH{trigger: INVARIANT}` before re-raising. A
        session drives `start`/`step` itself, so it reached none of that: the exception went
        straight out of the worker as a bare traceback with the switch un-tripped, no breach
        recorded and no `KILL_SWITCH` entry. Spec 7 lists four auto-triggers and makes none
        of them conditional on which loop is driving.

        Recorded and re-raised, never swallowed: returning a result here would publish an
        equity curve the platform has just proved wrong.
        """
        self._ledger_broken = True
        self.stopped_reason = (
            f"the ledger's own arithmetic failed ({exc.invariant}); the run was abandoned "
            f"rather than finalised"
        )
        self.engine.note_invariant_failure(exc)

    def _offer(self, event: Event, recv_ms: int) -> None:
        """Hold one event. Runs on the socket read path, so it does no work worth naming."""
        if not self.buffer.offer(event, recv_ms):
            # Late: its timestamp is already behind the release watermark, so it can no
            # longer be dispatched in order. **Dropped and counted, never forced through.**
            # `EventQueue.push` raises `OrderingViolation` on an event at or before the
            # current key, and that exception is not caught by the engine's run loop -- it
            # would kill a session holding an open position, with no metrics and no
            # artefacts, over one late frame.
            self._note_status(
                CollectorEventKind.STALE,
                event.dataset_id,
                f"late frame at {event.ts_ms} dropped; the reorder window had already "
                f"released past it",
            )

    def _note_funding_time(self, symbol: str, ts_ms: int) -> None:
        """Hand the engine a settlement instant the feed has just observed.

        A backtest is given the whole schedule by `LakeSource.prepare` before it starts. A
        live session has no such table -- `_PushSource` supplies none, and it cannot: the
        instants are only knowable from `premiumIndex.nextFundingTime` as the session runs.
        Without this, `_next_funding_ms` returns `None` for every symbol, `_flatten_reason`
        returns `None`, and `AutoFlatten.before_funding_ms` -- an exit the operator asked for
        by name -- never fires, with no error and nothing in the log to say the guarantee was
        not kept. The shadow, meanwhile, *is* handed a schedule from the tape, so the session
        and its own replay would disagree about a platform exit.
        """
        self.engine.note_funding_time(symbol, ts_ms)

    def _complete_through(self, source: str, ts_ms: int) -> None:
        """A slow source reporting how far it has delivered. Runs on a poller's task.

        Also a poller's only "I am working again" signal -- it emits `DISCONNECT` on a failed
        poll and nothing at all on a successful one -- so this is where its own outage ends.
        """
        self._poller_down.discard(source)
        if source == "markPrice" and self._mark_down_since is not None:
            # The mark clock ends the way the socket clock does: measured once on the way
            # back up, then cleared, so a recovery inside the ceiling still records how
            # long the mark was frozen. See `_check_disconnect` for the down-side check.
            down_since, self._mark_down_since = self._mark_down_since, None
            self._mark_disconnect_halted = False
            self._observe_disconnect(int((time.monotonic() - down_since) * 1000))
        self.buffer.complete_through(source, ts_ms, wall_now_ms=_now_ms())

    def _drain_to_engine(self, *, final: bool = False) -> None:
        """Release what the window has matured, record it, then dispatch it.

        Recording happens **before** dispatch and in released order, so the tape is a record
        of what the engine processed rather than of what arrived. A tape written on arrival
        would replay in a different order than the session ran in, and every difference would
        land in the parity report as if the fill model had caused it.
        """
        if final:
            self.feed.flush()
            records = self.buffer.drain_records()
        else:
            records = self.buffer.release_records(_now_ms())
        for held in records:
            self.tape.append_market(held.event, held.recv_ms)
            try:
                self.engine.queue.push(held.event)
            except Exception as exc:  # noqa: BLE001 - one bad event must not end the run
                self._note_status(
                    CollectorEventKind.STALE,
                    held.event.dataset_id,
                    f"event refused by the queue and skipped: {type(exc).__name__}: {exc}",
                )
                continue

        # **Dispatch only as far as the market data is complete to.** The queue holds two
        # kinds of event: market data that has already happened, and order arrivals the
        # engine scheduled *ahead* of the clock. Draining it unconditionally pops those
        # arrivals too -- so an order submitted at T with 120 ms of latency dragged the
        # clock to T+120 ms, and the next batch of frames, stamped before that, was refused
        # by `EventQueue.push` as out of order. Measured on a real session: one bookTicker
        # frame lost per session to exactly this. Stopping at the released frontier means a
        # scheduled arrival waits until market data genuinely reaches its instant, which is
        # also what makes the latency model mean anything live.
        # **The frontier, not the last released event.** Taking `records[-1].key` meant a
        # drain that released nothing dispatched nothing -- so an `ORDER_ARRIVAL` whose
        # latency had long since elapsed sat in the queue until the next market frame
        # happened to arrive, which on a quiet symbol is a real wait. The watermark is the
        # instant the session is complete to whether or not anything came out at it, which is
        # exactly the bound a scheduled arrival should be measured against.
        watermark = self.buffer.watermark_ms
        horizon = None if watermark is None else (watermark, _MAX_KIND, _MAX_SEQ, "￿")
        while self.engine.queue:
            if not final and (
                horizon is None or (self.engine.queue.next_key or horizon) > horizon
            ):
                break
            if not self.engine.step(self.engine.queue.pop()):
                self.stopped_reason = self.stopped_reason or "the risk layer halted the run"
                break
            self.processed += 1
        # A breach raised from outside dispatch -- a disconnect, a reconciliation mismatch --
        # has no event to ride, and on a quiet market the next one could be minutes away.
        if self.engine.perform_pending_halt():
            self.stopped_reason = self.stopped_reason or "the risk layer halted the run"

    # ------------------------------------------------------------------------ control

    def _read_control(self) -> StopRequest | None:
        """Has the API asked this session to stop, and how? See `CONTROL_FILENAME`."""
        path = self.run_dir / CONTROL_FILENAME
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not payload.get("stop"):
            return None
        flatten = payload.get("flatten")
        return StopRequest(
            reason=str(payload.get("reason") or "stopped from the API"),
            # Absent means "this stop expressed no preference", which is the session's own
            # default rather than cancel-only: an older control file must not silently
            # downgrade a session configured to close out.
            flatten=self._flatten_on_stop if flatten is None else bool(flatten),
        )

    def _on_status(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        """Connection lifecycle from the feed, recorded and fed to the risk layer.

        Spec 7's third auto-trigger is *"WS disconnection exceeding `max_disconnect_seconds`
        (default 30) while a position is open"*. A disconnect while flat is explicitly not a
        breach, so the position is consulted at the moment the downtime is measured rather
        than at the moment the socket dropped.

        **A poller's failure is not the socket's downtime.** See `POLLED_SOURCES`.
        """
        self._note_status(kind, stream, detail, downtime_ms)
        if stream in POLLED_SOURCES:
            # Tracked on its own baseline and never mixed into the socket's. A poller has no
            # recovery event of its own -- `LiveFeed` emits `DISCONNECT` and nothing else --
            # so the signal that it is working again is a completion report from a poll that
            # succeeded, which is `_complete_through`.
            if kind is CollectorEventKind.DISCONNECT:
                self._poller_down.add(stream)
                # **The mark poller is not just another poller when it is the only mark
                # source** (H8). On production every `@markPrice` stream is suppressed, so
                # a dead `MarkPricePoller` freezes `Account.marks` at its last value --
                # and equity, unrealised PnL, every risk notional and the liquidation
                # proximity all keep computing against a price that stopped. That is
                # exactly the state spec 7's disconnect trigger names ("the platform has
                # stopped seeing the mark that liquidation is decided against"), so the
                # mark's outage runs the same clock, checked by `_check_disconnect` while
                # down and settled by `_complete_through` on recovery. When a live
                # `@markPrice` stream exists, the socket's own clock already covers it.
                if (
                    stream == "markPrice"
                    and self.feed.polls_marks
                    and self._mark_down_since is None
                ):
                    self._mark_down_since = time.monotonic()
            else:
                self._poller_down.discard(stream)
            return
        if kind is CollectorEventKind.DISCONNECT:
            if self._market_down_since is None:
                self._market_down_since = time.monotonic()
            return
        if kind in (CollectorEventKind.CONNECT, CollectorEventKind.RECONNECT):
            down_since, self._market_down_since = self._market_down_since, None
            self._disconnect_halted = False
            if down_since is None:
                return
            down_ms = int((time.monotonic() - down_since) * 1000)
            self._observe_disconnect(down_ms)

    def _check_disconnect(self, now: float) -> None:
        """Evaluate spec 7's disconnect trigger **while the socket is still down**.

        It used to be evaluated only on the way back up, because that is when a total
        downtime figure exists -- so the one outage shape the trigger is written for, a
        socket that does not come back, could not fire it. Measured against a dead endpoint
        with a 1 s ceiling and 0.01 BTC open: ten seconds down, three `DISCONNECT` entries,
        zero breaches, kill switch clear, position still open; feeding a single `RECONNECT`
        halted immediately. A session whose process is killed during the outage never halts
        at all, and that is precisely the case spec 7 exists for -- the platform has stopped
        seeing the mark that liquidation is decided against.

        Only ever escalates: `RiskEngine.observe_disconnect` returns nothing while the
        account is flat or inside the ceiling, and once it has breached there is nothing
        further to observe until the socket returns.
        """
        if now - self._last_disconnect_check < DISCONNECT_CHECK_INTERVAL_S:
            return
        self._last_disconnect_check = now
        if self._market_down_since is not None and not self._disconnect_halted:
            self._observe_disconnect(
                int((now - self._market_down_since) * 1000)
            )
        # The mark's own clock (H8): a dead mark poller on an endpoint with no mark
        # stream is the same blindness as a dead socket, measured on its own baseline.
        if self._mark_down_since is not None and not self._mark_disconnect_halted:
            breach_before = self.engine.risk.halted
            self._observe_disconnect(int((now - self._mark_down_since) * 1000))
            if self.engine.risk.halted and not breach_before:
                self._mark_disconnect_halted = True

    def _observe_disconnect(self, down_ms: int) -> None:
        breach = self.engine.risk.observe_disconnect(
            self.engine.runtime.now_ms,
            down_ms,
            # Any side of any symbol. Under hedge mode a symbol whose long is flat can still
            # have an open short, and `qty(symbol)` cannot even be asked there -- it raises
            # rather than pick a leg. A disconnect over a half-hedged book is exactly the
            # case spec 7's trigger is for.
            position_open=any(
                self.engine.account.has_position(s) for s in self.config.symbols
            ),
        )
        if breach is not None:
            self._disconnect_halted = True
            self.engine.request_halt(breach)

    def _note_status(
        self,
        kind: CollectorEventKind,
        stream: str,
        detail: str,
        downtime_ms: int = 0,
    ) -> None:
        entry = {
            "ts_ms": _now_ms(),
            "kind": kind.value,
            "stream": stream,
            "detail": detail[:500],
            "downtime_ms": downtime_ms,
        }
        self.status_log.append(entry)
        # Bounded: forty-eight hours of a flapping endpoint would otherwise be the largest
        # object in the process. The *first* entries are kept, because the first disconnect
        # is the one that explains the incident.
        if len(self.status_log) > 5_000:
            del self.status_log[2_500:3_500]
        self.tape.append_exchange({"kind": "STATUS", **entry})
        log.info("%s %s: %s", kind.value, stream, detail)

    # -------------------------------------------------------------------------- state

    @property
    def live_stack(self) -> LiveStack | None:
        """The exchange stack, or `None` for a paper session. Read by the worker's
        manifest, which records the preflight, the transport counters and the final
        reconciliation pass -- the facts spec 13's Phase 8 exit criterion is judged on."""
        return self._live

    def monitor(self) -> dict[str, Any]:
        """A snapshot for the live monitor (spec 10.3). Cheap enough to poll every second."""
        engine = self.engine
        account = engine.account
        positions = []
        # Deduplicated and ordered, because `config.symbols` is caller-supplied and a repeat
        # would emit two identical rows -- which the monitor table then renders with the
        # same React key. One row per *addressable position*, so a hedge shows its long and
        # its short as the separate positions they are, each with its own entry price and its
        # own liquidation price.
        seen: set[str] = set()
        for symbol in self.config.symbols:
            if symbol in seen:
                continue
            seen.add(symbol)
            for side in engine._sides:
                view = engine.runtime.position_view(symbol, side)
                mark = account.marks.get(symbol)
                liq = view.liquidation_price
                distance = None
                if liq is not None and mark not in (None, 0) and liq != 0:
                    # **A fraction, not a percent.** Every other `_pct` field in this
                    # platform is a fraction (`max_drawdown_pct: 0.15` is fifteen percent)
                    # and the monitor's own contract says so -- but this one shipped
                    # multiplied by 100, so a position 5% from liquidation arrived as `5.0`,
                    # rendered as "500.00%", and never crossed the `< 0.05` threshold that
                    # turns the proximity bar red. The single most dangerous number on the
                    # page had its warning silently disabled.
                    distance = float(abs(mark - liq) / mark)
                positions.append(
                    {
                        "symbol": symbol,
                        "position_side": side.value,
                        "qty": str(view.qty),
                        "entry_price": str(view.entry_price),
                        "mark_price": None if mark is None else str(mark),
                        "unrealized_pnl": str(view.unrealized_pnl),
                        "liquidation_price": None if liq is None else str(liq),
                        "liq_distance_pct": distance,
                        "margin": str(view.margin),
                    }
                )
        # Newest first, from the engine's own event log rather than a second tally: the
        # FILL entries there are the ones the ledger booked, stamped and priced, so the
        # monitor cannot show a fill the accounting does not hold. The log is bounded
        # (`EventLogFull`), and the reversed scan stops at twenty entries.
        recent_fills: list[dict[str, Any]] = []
        for event in reversed(engine.runtime.events):
            if len(recent_fills) >= 20:
                break
            if event.kind != "FILL":
                continue
            payload = event.payload
            recent_fills.append(
                {
                    "ts_ms": payload.get("ts_ms"),
                    "symbol": payload.get("symbol"),
                    "side": payload.get("side"),
                    "qty": payload.get("qty"),
                    "price": payload.get("price"),
                    "fee": payload.get("fee"),
                    "realized_pnl": payload.get("realized"),
                    "maker": payload.get("is_maker"),
                    "order_id": payload.get("order_id"),
                    "tag": payload.get("tag"),
                }
            )

        live = self._live
        return {
            "run_id": self.session.run_id,
            "endpoint": self.session.endpoint,
            "mode": self.session.mode,
            "now_ms": _now_ms(),
            "engine_now_ms": engine.runtime.now_ms,
            "halted": engine.halted,
            "stopped_reason": self.stopped_reason,
            "connection": {
                # The *socket*. A failing REST poller used to set this and only a socket
                # reconnect could clear it, so the monitor showed the market feed down
                # beside a "last frame 0.3 s ago" reading for hours. See `POLLED_SOURCES`.
                "market": "down" if self._market_down_since is not None else "up",
                # The user-data stream, on its own clock -- see `exchange_event`. "n/a" for
                # a paper session, which has no fill stream to be down.
                "user": (
                    "n/a"
                    if live is None
                    else ("down" if self._user_down_since is not None else "up")
                ),
                "last_frame_ms": self.feed.last_frame_ms,
                "pollers_down": sorted(self._poller_down),
            },
            # Present only on a live session: what left for the venue and what came back
            # (spec 10.3), and how the last spec 6.7.3 comparison went. The monitor is the
            # one place an operator can see an unresolved order or a foreign report while
            # there is still time to act on it.
            "transport": None if live is None else live.transport.summary(),
            "reconcile": None if live is None else live.reconciler.summary(),
            # Spec 5.4's warm-up gate, which a live session pays in wall-clock time rather
            # than in history. Reported because a strategy declaring 200 bars on 1m cannot
            # trade for the first three hours and twenty minutes, and "no orders yet" is
            # otherwise indistinguishable from "chose not to trade". See `_PushSource`.
            "warmup": {
                "bars_required": engine.warmup_bars,
                "bars_seen": engine.context.bars_seen,
                "warm": engine.context.warm,
            },
            "account": {
                "wallet": str(account.wallet),
                "equity": str(account.equity),
                "available": str(account.available_balance),
                "used_margin": str(account.allocated_margin),
            },
            "positions": positions,
            "recent_fills": recent_fills,
            # `usage` is spliced in rather than folded into `summary()`: the run record
            # stores that summary forever, and a usage reading is true for one instant.
            # See `RiskEngine.usage`.
            "risk": {**engine.risk.summary(), "usage": engine.risk_usage()},
            "counts": {**engine.counts, **self.feed.counts, "events": self.processed},
            "buffer": self.buffer.stats(),
            "status": self.status_log[-50:],
        }

    def latency_samples(self) -> dict[str, list[int]]:
        """Per-order submit-to-arrival times, for the shadow's empirical model (spec 6.3).

        Read off the engine's own order records rather than measured separately, so the two
        cannot disagree. Under the local fill simulator these are the modelled latencies, and
        the shadow ignores them in favour of replaying the same model with the same seed --
        see `shadow._replay_latency` for why re-deriving a pool from modelled draws would
        perturb the RNG for no gain. They matter when a real exchange transport fills the
        orders, which is the case spec 6.3 reserves the mode for.

        **A live session reads the transport's own pool instead.** The engine's order
        records are polluted for this purpose by `_fire_trigger`, which re-stamps
        `arrival_ts` when a stop fires while `submit_ts` stays at submission -- so a stop
        that rested nine minutes would contribute a 520 000 ms "latency"
        (`shadow._replay_latency` documents the damage). The transport measures
        submit-to-ack on its own monotonic clocks, per request, and nothing re-stamps it.
        """
        if self._live is not None:
            return self._live.transport.latency_samples()
        submits = [
            order.arrival_ts - order.submit_ts
            for order in self.engine.orders.values()
            if order.arrival_ts > order.submit_ts
        ]
        return {"submit": submits, "cancel": []}

    def seal(self) -> None:
        """Close the tape with the counts a shadow replay needs.

        Sealed at the instant `_finalise` measured the run to, not at whatever the wall
        clock reads by the time the worker gets here: `shadow_spec` takes the shadow's
        `end_ms` from this field, and the two runs are only comparable if they cover the
        same window. `_now_ms()` is the fallback for a session that never reached its end --
        a crash -- whose tape is unsealed anyway.
        """
        self.tape.seal(
            ended_ms=self.ended_ms if self.ended_ms is not None else _now_ms(),
            funding_times=self.feed.funding_times(),
            latency_samples=self.latency_samples(),
            counts={
                **self.buffer.stats(),
                **{f"feed_{k}": v for k, v in self.feed.counts.items()},
                "bar_closes": self.feed.counts["bars"],
                "events_dispatched": self.processed,
                "stalled_sources": list(self.buffer.stalled_sources),
                # What the latency samples *are*: modelled draws for a paper session, real
                # submit-to-ack measurements for a live one. `shadow._replay_latency` names
                # this exact distinction as the missing fact that keeps the empirical
                # replay path closed -- recording it is the first half of opening it.
                "latency_samples_kind": (
                    "measured" if self._live is not None else "modelled"
                ),
            },
        )


class _PushSource:
    """A `MarketSource` that supplies nothing: the session pushes events itself.

    A live feed has no stream to register -- there is no iterator over the future -- so this
    hands the engine an empty preparation and the session drives `queue.push` as frames
    mature. `total_bars` is zero because the end is not known in advance, which is what the
    UI reads to show elapsed time instead of a percentage.

    **A session therefore starts cold, and says so.** `engine.start()` still computes
    `warmup = max(indicators.warmup, requires["history"])` and `ctx.warm` still gates on the
    bar count, so a strategy declaring 200 bars on a 1m timeframe cannot place an order for
    the first three hours and twenty minutes of its session -- `Strategy`'s own docstring
    example declares 400, which is six and a half hours. Nothing said so: the run reported
    `completed`, zero orders, zero flags and zero warnings, and the operator's only reading
    was "the strategy chose not to trade". That is the failure `_check_declarations` exists
    to prevent in a backtest, and `LakeSource` raises `WARMUP_SHORT` plus a warning for a
    strictly smaller version of the same condition -- so this raises the same pair, and
    `RunList.tsx`'s existing help text for that badge ("its first trades happen later than
    the range begins") is already the true statement about this case.

    **Why the bars are not preloaded from the lake**, which is the obvious alternative:

    - The lake does not hold them. The bulk archive lags roughly a day and the collector's
      own lake is written on its own schedule, so the bars immediately before a session's
      start -- exactly the ones warm-up needs -- are the ones most likely to be missing. An
      indicator warmed from a hole is worse than one that is honestly cold, because the gate
      would then be open.
    - It would break the shadow. The tape is the shadow's only input, and preloaded bars are
      not on it, so the replay would warm on a different history than the session did and
      the parity report would be measuring warm-up instead of the fill model. Writing them
      onto the tape is worse still: it would record data the session never observed as
      though it had, which is the failure `TapeSource`'s docstring is built to refuse.
    - Nothing is *wrong* meanwhile. `ctx.buy` raises `WarmupViolation` while cold, so no
      order is placed under-warmed and no published number is false. What was missing was
      the operator's ability to know, and that is what a flag and a warning are for.

    Loading warm-up from a *sealed source with a known fingerprint* -- the tape of a previous
    session, or a lake range validated against the manifest -- is a defensible feature. It is
    a different one, it changes what the shadow must replay, and it is not this fix.
    """

    def prepare(self, engine: BacktestEngine) -> Any:
        from perplab.engine.source import Prepared

        warmup = max(engine.warmup_bars, 0)
        flags: tuple[str, ...] = ()
        warnings: tuple[str, ...] = ()
        # **Raised on the bars a backtest would not have spent cold**, which is `warmup - 1`:
        # every run is warm no earlier than its first bar close, so a strategy needing one
        # bar loses nothing to starting cold and does not deserve a badge that would then be
        # on for every session ever run and mean nothing. One needing two loses one, and one
        # needing two hundred loses a hundred and ninety-nine bar closes it could have
        # traded -- and does so inside the measured range.
        if warmup > 1:
            cold_ms = warmup * engine.requirements.timeframe_ms
            warnings = (
                f"this session starts cold: no history is loaded before "
                f"{engine.config.start_ms}, so the {warmup} warm-up bar(s) this strategy "
                f"needs have to elapse in real time. `ctx.warm` is false and every order is "
                f"refused for the first {cold_ms // 60_000} minute(s) of the session, and "
                f"unlike a backtest that window falls inside the range the metrics are "
                f"measured over rather than before it.",
            )
            flags = ("WARMUP_SHORT",)

        return Prepared(
            streams=(),
            data_start_ms=engine.config.start_ms,
            total_bars=0,
            flags=flags,
            warnings=warnings,
        )

    def close(self) -> None:
        """Nothing to release."""


def _timeframe_ms(requirements: Requirements) -> int:
    return requirements.timeframe_ms


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
        return True
    except TimeoutError:
        return False


def _now_ms() -> int:
    return int(time.time() * 1000)
