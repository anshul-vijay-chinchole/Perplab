"""Making a live socket feed satisfy spec 6.2's total ordering.

A backtest reads a table that was sorted before the run started. A live session reads
sockets, and sockets do not deliver in order: two streams are two TCP connections, a
reconnect replays from a different point in the exchange's buffer, and the REST pollers this
deployment depends on (see `data.rest_poller`) sample on their own cadences. Spec 6.2's key
is `(timestamp_ms, kind_priority, source_seq, dataset_id)` and it *admits no ties* -- so the
recorder has to manufacture, from an unordered arrival stream, exactly the ordered stream the
engine already knows how to consume.

Two failures this module exists to prevent, both of which end a live session mid-position.

**Reusing the exchange's own ids.** Binance allocates aggregate trade ids and depth update
ids *per symbol*, so two symbols printing the same id in the same millisecond under one
shared `dataset_id` produce two identical total-order keys, and `EventQueue.pop` raises
`OrderingViolation` on the tie. The rule is `dataset_id = f"{stream}:{symbol}"` -- one
sequence space per stream per symbol -- and it is shared: `ticks.trade_events` and
`ticks.depth_events` spell their lake-replay ids the same way via `live_dataset_id`, so a
tape and a lake replay of the same window are keyed alike. Live goes one step further and
has `source_seq` handed out by `SequenceAllocator` rather than read off the wire: an id we
allocated is dense, monotonic and ours; an id the exchange allocated is a number whose
uniqueness guarantee is scoped to something other than our stream.

**Pushing a frame that arrived too late.** `EventQueue.push` refuses an event at or before
the event being processed, because scheduling into the past is look-ahead. That refusal is a
`RuntimeError`, and a `RuntimeError` in a live session holding an open position is the worst
available outcome. `ReorderBuffer` holds every event for `window_ms` of wall clock so that
ordinary jitter resolves before anything reaches the queue, and whatever is *still* late is
dropped and counted -- never forced in, and never silently ignored, because a dropped frame
is a fact the parity report (spec 6.7.1) needs in order to explain a divergence.

**The release watermark is wall clock, never the highest exchange timestamp seen.** The
max-observed-time watermark is the textbook construction and it is wrong here: a stream that
stops delivering stops advancing the maximum, so nothing ever crosses the release threshold
and the buffer holds everything forever. The session would stop processing mark prices at
precisely the moment its feed broke, which is when a position most needs them.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Any

from perplab.engine.clock import Event, OrderingViolation

__all__ = [
    "DEFAULT_MAX_HELD",
    "DEFAULT_WINDOW_MS",
    "Held",
    "ReorderBuffer",
    "ReorderOverflow",
    "SequenceAllocator",
    "live_dataset_id",
]

DEFAULT_WINDOW_MS = 250
"""How long an event waits before it may be released.

Long enough to absorb the reordering a healthy multi-socket feed produces, short enough that
it is invisible beside spec 6.3's default submit latency of a 120 ms median. It is a delay on
*processing*, not on the recorded timestamps: the tape stores the exchange's `ts_ms`, so a
shadow backtest of the session sees the same instants whatever this is set to.
"""

DEFAULT_MAX_HELD = 100_000
"""Ceiling on events waiting out the window, above which the buffer refuses to hold more."""


def live_dataset_id(stream: str, symbol: str) -> str:
    """The `dataset_id` a live stream records under: `stream:symbol`.

    One sequence space per stream per symbol, which is what makes a recorder-allocated
    `source_seq` sufficient to break every tie within a millisecond. Both halves are refused
    if they are empty or contain a colon, so the id can always be split back into the pair
    that produced it -- a tape whose `dataset_id` cannot be parsed is a tape whose rows cannot
    be attributed to a stream when a parity report has to explain which feed diverged.
    """
    for name, value in (("stream", stream), ("symbol", symbol)):
        if not value:
            raise ValueError(f"a live dataset id needs a non-empty {name}")
        if ":" in value:
            raise ValueError(
                f"a live {name} may not contain ':' (got {value!r}); the colon separates "
                f"the stream from the symbol, and an ambiguous id cannot be split back into "
                f"the feed it came from."
            )
    return f"{stream}:{symbol}"


class SequenceAllocator:
    """Hands out a monotonic `source_seq` within each `dataset_id`.

    Spec 6.2 wants a per-source monotonic index, and in a backtest a row number satisfies it
    because the rows were sorted before the run began. Live, there is no row number, and the
    obvious substitute -- the exchange's own id -- is scoped per symbol rather than per
    stream. Allocating here makes the sequence dense, monotonic and unique inside the one
    scope the total-order key checks it against.

    Sequences start at zero and are never reused within a session. There is no resume path:
    an interrupted session leaves an unsealed tape (see `tape.TapeWriter.seal`) and starts a
    new one rather than appending, so a sequence never has to be recovered from disk.

    Not thread-safe, deliberately. The recorder is an asyncio loop with no await between the
    read and the write below, so a lock would buy nothing and would sit on the hottest path
    in a live session. Call it from one task.
    """

    __slots__ = ("_next",)

    def __init__(self) -> None:
        self._next: dict[str, int] = {}

    def allocate(self, dataset_id: str) -> int:
        """The next sequence number for this dataset."""
        seq = self._next.get(dataset_id, 0)
        self._next[dataset_id] = seq + 1
        return seq

    def allocate_live(self, stream: str, symbol: str) -> tuple[str, int]:
        """The `(dataset_id, source_seq)` pair for one frame off one live stream.

        The two halves of spec 6.2's live rule in one call, because separating them is how a
        recorder ends up allocating against one id and stamping the event with another.
        """
        dataset_id = live_dataset_id(stream, symbol)
        return dataset_id, self.allocate(dataset_id)

    def issued(self, dataset_id: str) -> int:
        """How many sequence numbers this dataset has been given."""
        return self._next.get(dataset_id, 0)

    def datasets(self) -> tuple[str, ...]:
        """Every dataset that has ever been allocated from, in name order."""
        return tuple(sorted(self._next))


@dataclass(frozen=True, slots=True)
class Held:
    """An event waiting out the reorder window, with the instant it reached us.

    `recv_ms` travels with the event because `tape.TapeWriter.append_market` records it and
    the reorder window is the only place that still knows it. Re-reading the clock at release
    time would store the moment the buffer let go of the frame rather than the moment the
    frame arrived, which is the difference between a measurement of feed lag and a
    measurement of `window_ms`.
    """

    event: Event
    recv_ms: int


@dataclass(frozen=True, slots=True)
class _Source:
    """One slow source's watermark, and when it last said anything.

    The two timestamps answer different questions and conflating them is a real defect, not
    a tidiness point: `through_ms` is how far the source has delivered (a market-data
    instant), `reported_wall_ms` is when it last said so (a wall-clock instant). Liveness is
    the second; the release cutoff is the first.
    """

    through_ms: int
    reported_wall_ms: int
    stale_after_ms: int


class ReorderOverflow(RuntimeError):
    """The buffer holds `max_held` events and another was offered.

    Fatal rather than trimmed. A trim would discard market data the session then traded
    without, and nothing in the tape would say which frames were missing -- so the shadow
    backtest would replay a market that never existed and report the difference as a fill
    model problem.
    """


class ReorderBuffer:
    """Holds live events for a wall-clock window, then releases them in total order.

    The contract is narrow on purpose: everything offered is either released exactly once, in
    spec 6.2's key order, or refused at the moment it is offered. Nothing is reordered after
    release and nothing is quietly discarded, so the count of dropped frames is a number the
    session can report rather than a discrepancy someone has to infer later.
    """

    __slots__ = (
        "window_ms",
        "max_held",
        "_heap",
        "_keys",
        "_counter",
        "_watermark_ms",
        "_offered",
        "_released",
        "_late_dropped",
        "_max_lag_ms",
        "_sources",
        "_stalled",
        "_last_wall_ms",
        "_clock_regressions",
        "_max_regression_ms",
    )

    def __init__(
        self, *, window_ms: int = DEFAULT_WINDOW_MS, max_held: int = DEFAULT_MAX_HELD
    ) -> None:
        if window_ms < 0:
            raise ValueError(
                f"window_ms cannot be negative, got {window_ms}; a negative window would "
                f"release events before they arrived."
            )
        if max_held <= 0:
            raise ValueError(f"max_held must be positive, got {max_held}")
        self.window_ms = window_ms
        self.max_held = max_held
        # `_counter` sits between the key and the payload for the reason `EventQueue._offer`
        # gives: heapq falls through to comparing the next tuple element on a tie, and `Held`
        # wraps arbitrary payloads that are not orderable. Ties cannot reach it here -- they
        # are refused in `offer` -- but a heap that would raise `TypeError` from inside the
        # standard library if one ever did is a heap with a worse error message than it needs.
        self._heap: list[tuple[tuple[int, int, int, str], int, Held]] = []
        self._keys: set[tuple[int, int, int, str]] = set()
        self._counter = 0
        self._watermark_ms: int | None = None
        self._offered = 0
        self._released = 0
        self._late_dropped = 0
        self._max_lag_ms = 0
        self._sources: dict[str, _Source] = {}
        """Slow sources holding the watermark back. See `expect`."""
        self._stalled: set[str] = set()
        self._last_wall_ms: int | None = None
        self._clock_regressions = 0
        self._max_regression_ms = 0

    # ------------------------------------------------------------------------ accepting

    def offer(self, event: Event, recv_ms: int) -> bool:
        """Hold an event until the window has elapsed. `False` means it was too late.

        An event at or below the current release watermark can no longer be emitted in
        order: the buffer has already released everything at or before that timestamp, and
        the engine's clock has moved past it. Pushing it anyway would raise
        `OrderingViolation` out of `EventQueue.push` and end a session that may be holding an
        open position, so it is dropped and counted instead.

        The bound is on the timestamp rather than on the full key, and it is inclusive. The
        buffer does not know the engine's current key -- an event at exactly the watermark
        with a later `kind_priority` might in fact still be pushable -- and guessing wrong
        costs a session. Refusing the whole millisecond is one frame of conservatism against
        an exception with no recovery path.
        """
        watermark = self._watermark_ms
        if watermark is not None and event.ts_ms <= watermark:
            self._late_dropped += 1
            return False

        if len(self._heap) >= self.max_held:
            raise ReorderOverflow(
                f"the reorder buffer already holds {self.max_held} events and "
                f"{event.kind.name} at {event.ts_ms} was offered. Either release() has "
                f"stopped being called -- a live loop that is no longer pumping -- or "
                f"window_ms ({self.window_ms}) is far larger than this feed's real "
                f"disorder. Fix the release cadence, or raise max_held deliberately; "
                f"trimming the buffer would drop market data the session then traded on."
            )

        key = event.key
        if key in self._keys:
            # Two events inside one window carrying the same total-order key. Caught here
            # rather than at `EventQueue.pop` because here the message can name the cause:
            # it is what happens when live events are stamped with the exchange's own ids
            # instead of recorder-allocated ones.
            raise OrderingViolation(
                f"two events share the total order key {key} inside the reorder window. "
                f"Spec 6.2 admits no ties. Live events must take dataset_id from "
                f"live_dataset_id(stream, symbol) and source_seq from SequenceAllocator; "
                f"the exchange's own ids are allocated per symbol and collide across them."
            )

        self._counter += 1
        heapq.heappush(self._heap, (key, self._counter, Held(event=event, recv_ms=recv_ms)))
        self._keys.add(key)
        self._offered += 1
        lag = recv_ms - event.ts_ms
        if lag > self._max_lag_ms:
            self._max_lag_ms = lag
        return True

    # ------------------------------------------------------------------------ releasing

    def release(self, wall_now_ms: int) -> list[Event]:
        """Every held event at or before `wall_now_ms - window_ms`, in total order."""
        return [held.event for held in self.release_records(wall_now_ms)]

    def expect(self, source: str, *, stale_after_ms: int) -> None:
        """Declare a source whose events are discovered well after they happened.

        **This is what stops a polled series being dropped as late.** Streamed events arrive
        within tens of milliseconds of their timestamp, so a wall-clock watermark releases
        them correctly. A one-minute kline is a different animal: it closes at T and is not
        published until the next poll, so by the time it is in hand the wall clock -- and
        therefore the watermark -- has moved a second or two past T, and the bar can no
        longer be inserted before the trades already released. Measured on a real testnet
        session: **every** kline and every funding row was dropped, the engine saw zero bar
        closes, and the strategy never traded. Nothing raised; the session simply did
        nothing, which is exactly the class of silent wrong answer this platform is built to
        refuse.

        The fix is a per-source watermark, the standard answer in stream processing. A slow
        source declares how far it is *complete through*, and the buffer never releases past
        the earliest such point. Trades are then held only while a bar boundary is genuinely
        outstanding -- about a second, once a minute -- instead of everything being delayed by
        the slowest source's lag all the time, which is what widening `window_ms` would cost.

        `stale_after_ms` bounds the damage when a source dies. A per-source watermark that
        only ever moved forward on delivery would let one dead poller freeze the entire
        session, which is the failure the wall-clock watermark was chosen to avoid in the
        first place. Past this much silence the source is declared stalled, is excluded from
        the cutoff, and says so.
        """
        if stale_after_ms <= 0:
            raise ValueError(
                f"stale_after_ms must be positive, got {stale_after_ms}; a source that is "
                f"never allowed to fall behind can never be declared dead either, and one "
                f"dead poller would then stall the whole session."
            )
        self._sources.setdefault(
            source, _Source(through_ms=0, reported_wall_ms=0, stale_after_ms=stale_after_ms)
        )

    def complete_through(self, source: str, ts_ms: int, *, wall_now_ms: int) -> None:
        """A slow source reporting it has delivered everything up to `ts_ms`.

        Monotonic per source, for the same reason the global watermark is: a source that
        reported backwards would re-open a stretch the buffer had already released.

        `wall_now_ms` is recorded separately from `ts_ms`, and the distinction is the whole
        correctness of the staleness rule. A one-minute kline source reporting every second
        is *perfectly healthy* while its completion point sits up to sixty seconds behind the
        wall clock -- that lag is the source's cadence, not its sickness. Judging staleness on
        `wall_now - ts_ms` therefore declared every kline and every mark source dead within
        fifteen seconds of the session starting, dropped their events as late, and left the
        engine with one bar close in four minutes. Measured, on a real testnet session.
        """
        current = self._sources.get(source)
        if current is None:
            raise KeyError(
                f"{source!r} was not declared with expect(); an undeclared source cannot "
                f"hold the watermark back and its events would be dropped as late."
            )
        self._sources[source] = _Source(
            through_ms=max(current.through_ms, ts_ms),
            reported_wall_ms=max(current.reported_wall_ms, wall_now_ms),
            stale_after_ms=current.stale_after_ms,
        )
        self._stalled.discard(source)

    def release_records(self, wall_now_ms: int) -> list[Held]:
        """`release`, keeping each event's arrival time for the tape.

        The cutoff is the earliest of the wall-clock watermark and every live slow source's
        completion point -- so nothing is released past an instant some source may still have
        events for.

        The watermark only ever moves forward. A wall clock can step backwards -- an NTP
        correction is the ordinary case, not an exotic one -- and a watermark that followed
        it back would re-admit events the buffer had already released past, which is the
        out-of-order push this whole module exists to make impossible.

        **A backward step therefore stalls dispatch for its own length, and that is now
        counted rather than merely endured.** Holding the watermark is the right behaviour --
        the alternative re-opens a released stretch -- but a session that stops dispatching
        for thirty seconds after a clock correction looks exactly like a session whose feed
        has died, and an operator watching the monitor has no way to tell them apart.
        `clock_regressions` and `max_clock_regression_ms` say which it was.
        """
        cutoff = wall_now_ms - self.window_ms
        if self._last_wall_ms is not None and wall_now_ms < self._last_wall_ms:
            step_back = self._last_wall_ms - wall_now_ms
            self._clock_regressions += 1
            self._max_regression_ms = max(self._max_regression_ms, step_back)
        self._last_wall_ms = max(self._last_wall_ms or wall_now_ms, wall_now_ms)
        for name, source in self._sources.items():
            # **Staleness is time since it last reported, not how far behind it is.** A
            # minute-cadence source is legitimately a minute behind and still healthy; see
            # `complete_through`.
            silent_for = wall_now_ms - source.reported_wall_ms
            if source.reported_wall_ms and silent_for > source.stale_after_ms:
                self._stalled.add(name)
                continue
            if not source.reported_wall_ms:
                # Nothing reported yet. Held back only until the staleness bound, so a poller
                # that never starts cannot freeze the session for ever.
                cutoff = min(cutoff, wall_now_ms - source.stale_after_ms)
                continue
            cutoff = min(cutoff, source.through_ms)

        if self._watermark_ms is None or cutoff > self._watermark_ms:
            self._watermark_ms = cutoff
        return self._pop_through(self._watermark_ms)

    @property
    def clock_regressions(self) -> int:
        """How many times the wall clock was observed to step backwards.

        Non-zero means dispatch paused for as long as the step, which is correct and is not
        a feed outage -- see `release_records`. Reported so the two can be told apart."""
        return self._clock_regressions

    @property
    def max_clock_regression_ms(self) -> int:
        """The largest single backward step, which bounds how long dispatch was paused."""
        return self._max_regression_ms

    @property
    def stalled_sources(self) -> tuple[str, ...]:
        """Slow sources that have fallen past their staleness bound and stopped holding the
        watermark back. A non-empty tuple means events from them are now being dropped."""
        return tuple(sorted(self._stalled))

    def drain(self) -> list[Event]:
        """Release everything still held, whatever the clock says. For session end."""
        return [held.event for held in self.drain_records()]

    def drain_records(self) -> list[Held]:
        """`drain`, keeping each event's arrival time for the tape.

        The window is a bet that a late frame is still coming. At session end there is no
        later frame to wait for, so holding the tail would simply lose it -- and a tape
        missing its last quarter second is a tape whose final fills cannot be reproduced.
        """
        if not self._heap:
            return []
        highest = max(key[0] for key, _, _ in self._heap)
        released = self._pop_through(highest)
        if self._watermark_ms is None or highest > self._watermark_ms:
            self._watermark_ms = highest
        return released

    def _pop_through(self, cutoff_ms: int) -> list[Held]:
        out: list[Held] = []
        # `key[0]` is `ts_ms`, the first component of the total-order key, so the heap's
        # minimum key is also its minimum timestamp.
        while self._heap and self._heap[0][0][0] <= cutoff_ms:
            key, _, held = heapq.heappop(self._heap)
            self._keys.discard(key)
            out.append(held)
        self._released += len(out)
        return out

    # ------------------------------------------------------------------------ reporting

    @property
    def held(self) -> int:
        """Events waiting out the window right now."""
        return len(self._heap)

    @property
    def offered(self) -> int:
        return self._offered

    @property
    def released(self) -> int:
        return self._released

    @property
    def late_dropped(self) -> int:
        """Frames that arrived after their instant had already been released past."""
        return self._late_dropped

    @property
    def watermark_ms(self) -> int | None:
        """The release watermark, or `None` before the first release."""
        return self._watermark_ms

    @property
    def max_lag_ms(self) -> int:
        """The largest `recv_ms - ts_ms` observed, floored at zero.

        This is the observable that says whether `window_ms` is wide enough for the feed the
        session is actually running against. Floored because a negative value means our clock
        reads behind the exchange's, which is a clock-synchronisation fact and not a
        measurement of how long a frame spent in flight.
        """
        return self._max_lag_ms

    def stats(self) -> dict[str, Any]:
        """A summary for the session record and for `tape.TapeWriter.seal`."""
        return {
            "window_ms": self.window_ms,
            "offered": self._offered,
            "released": self._released,
            "late_dropped": self._late_dropped,
            "still_held": len(self._heap),
            "max_lag_ms": self._max_lag_ms,
            "clock_regressions": self._clock_regressions,
            "max_clock_regression_ms": self._max_regression_ms,
            "stalled_sources": sorted(self._stalled),
        }
