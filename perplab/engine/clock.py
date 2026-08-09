"""The deterministic event clock (spec 6.2).

A backtest is only evidence if it reproduces, and the thing that most often stops one
reproducing is not the arithmetic -- it is the order events were processed in. Spec 6.2 is
blunt about why sorting by timestamp is not enough: *"thousands of events share a
millisecond, and Python's sort stability over an arbitrary file read order is not a
guarantee you should rely on across machines."*

So every event carries a **total** order key:

```
(timestamp_ms, kind_priority, source_seq, dataset_id)
```

No two events can tie. `EventQueue` checks that at pop time rather than trusting it --
see `_ORDER_VIOLATION` below -- because a tie is not a cosmetic problem: it means two
events could have come out in either order, and the run's hash is then a function of the
interpreter rather than of the data.

**Three of the nine priorities are load-bearing**, and spec 6.2 names them:

- *Funding (1) before the liquidation check (2)*: a funding payment reduces margin and can
  itself cause the liquidation. Checking first lets a position survive a payment that
  should have killed it.
- *The liquidation check (2) before fill checks (5)*: being liquidated cancels your resting
  orders, so they must not fill in the same instant.
- *Bar close (6) after trades (4)*: the bar's closing trade is part of the bar. Running
  `on_bar` first would hand the strategy a bar that had not finished forming.

All nine kinds are declared here; a run emits the subset its tier and its strategy reach
for. The numbers are never reassigned -- renumbering would silently reorder every stored run,
and a priority table that changes is a reproducibility contract that does not exist. That is
also why `ORDER_ARRIVAL` carries submits, cancels *and* parked-order deadlines rather than a
cancel getting a priority of its own: all three are the same event, an instruction reaching
the matching engine after its latency has elapsed.

**Dynamic scheduling.** Data events are pulled lazily from per-dataset iterators; order
arrivals are *pushed* during the run, at a timestamp the latency model chooses. `push`
refuses an event at or before the event currently being processed, which is where the
no-look-ahead guarantee stops being a convention and becomes a check: an engine that
scheduled a fill into the past would be filling on information it did not have.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

__all__ = [
    "EventKind",
    "Event",
    "EventQueue",
    "OrderingViolation",
    "merge_sorted",
]


class EventKind(IntEnum):
    """Spec 6.2's priority table, verbatim. The integer value *is* the priority.

    `IntEnum` rather than a name-to-priority mapping so the ordering cannot drift from the
    declaration: there is one number per kind and it lives in one place. Comparisons
    between kinds are therefore priority comparisons, which is what every use site wants.
    """

    MARK_PRICE_UPDATE = 0
    """Mark price moves first, so risk state is current before anything reads it."""

    FUNDING_SETTLEMENT = 1
    """Funding debits/credits the wallet -- and the position's own allocation (spec 3.5)."""

    LIQUIDATION_CHECK = 2
    """Evaluated against the updated mark *and* the post-funding wallet."""

    BOOK_UPDATE = 3
    """Depth snapshot applied.

    Only *queued* when a depth-driven indicator is registered, which needs every row in
    order. Otherwise book state is pulled to a horizon the loop sets -- exactly equivalent,
    because a snapshot replaces its predecessor rather than incrementing it, and the
    difference between a two-day `BOOK_TICKER` run taking 47 seconds and taking an hour. See
    `ticks.StateStream`."""

    TRADE = 4
    """A trade printed.

    Real `aggTrades` at the `TRADE_ONLY` tier and above. At `BAR_CLOSE` the only prints that
    can be dated exactly are the bar's open and its close -- Binance's kline `open` *is* the
    first trade of the bar and its `close` is the last -- so those two are emitted and the
    high/low are not, because nothing in a kline says when they happened. The two sources are
    never both emitted: a tick tier would otherwise carry two accounts of the same two
    trades."""

    ORDER_FILL_CHECK = 5
    """Resting limit orders and contract-price triggers evaluated against this
    millisecond's prints. Scheduled only when something is actually resting."""

    BAR_CLOSE = 6
    """Indicators update; `on_bar` is invoked. One event per timestamp carrying every
    symbol's bar for that instant -- see `Event.payload` and `backtest._on_bar_close`."""

    ORDER_SUBMIT = 7
    """The strategy's order enters the latency queue."""

    ORDER_ARRIVAL = 8
    """The order's latency has elapsed and it reaches the matching engine."""


class OrderingViolation(RuntimeError):
    """The event stream is not totally ordered, or an event was scheduled into the past.

    Fatal rather than warned about. Spec 12.1 makes an unreproducible run disqualifying,
    and both of these conditions mean the run's event-log hash is a function of something
    other than its inputs.
    """


_ORDER_VIOLATION = (
    "two events share the total order key {key}. Spec 6.2 requires "
    "(timestamp_ms, kind_priority, source_seq, dataset_id) to admit no ties, because a tie "
    "means these two could have been processed in either order and the run's hash is then "
    "a property of the interpreter rather than of the data. Give each source its own "
    "dataset_id, and each row within a source its own source_seq."
)


@dataclass(frozen=True, slots=True)
class Event:
    """One thing that happens, at one instant, from one source.

    `payload` is a plain object rather than a union of typed events. The queue's only job
    is ordering; giving it opinions about content would mean every new event kind touched
    this module. The engine casts on `kind`, which it already switches on.
    """

    ts_ms: int
    kind: EventKind
    source_seq: int
    """Monotonic *within its own source*, not globally.

    Per-source rather than global because a global counter would depend on the order the
    queue happened to pull from its streams, which is the very thing this key exists to
    pin down. A row index is stable across runs and across machines.
    """
    dataset_id: str
    """Which stream this came from -- a dataset name, or `engine` for scheduled events.

    The last tie-break, and it is reachable: two datasets both emit their row 0 at the same
    millisecond and the same priority whenever a range starts on a boundary.
    """
    payload: Any = None

    @property
    def key(self) -> tuple[int, int, int, str]:
        return (self.ts_ms, int(self.kind), self.source_seq, self.dataset_id)


@dataclass
class EventQueue:
    """A merge of sorted event streams, plus events scheduled while the run is in flight.

    Streams are consumed lazily -- one buffered event per stream -- so a year of 1-minute
    bars across four datasets never materialises in memory. That is not only a size
    argument: a run that has to load everything before it can start cannot report progress,
    and a backtest with no progress is indistinguishable from a hung one.
    """

    _heap: list[tuple[tuple[int, int, int, str], int, Event, int]] = field(
        default_factory=list, init=False, repr=False
    )
    _streams: list[Iterator[Event]] = field(default_factory=list, init=False, repr=False)
    _push_seq: int = field(default=0, init=False, repr=False)
    _last_key: tuple[int, int, int, str] | None = field(default=None, init=False, repr=False)
    _popped: int = field(default=0, init=False, repr=False)

    def add_stream(self, events: Iterable[Event]) -> None:
        """Register a stream. Its events must already be in key order.

        Not sorted here, and not checked exhaustively either -- a stream is a lazy iterator
        over a Parquet scan and buffering it to verify would defeat the point. What *is*
        checked is the merged output, at every pop, which catches an out-of-order stream on
        the first offending row.
        """
        iterator = iter(events)
        index = len(self._streams)
        self._streams.append(iterator)
        self._advance(index)

    def push(self, event: Event) -> None:
        """Schedule an event produced during the run.

        Refuses anything at or before the event currently being processed. A fill scheduled
        into the past is look-ahead in its purest form -- it acts on information the engine
        did not have at that instant -- and an equal key would be an ordering tie.
        """
        if self._last_key is not None and event.key <= self._last_key:
            raise OrderingViolation(
                f"cannot schedule {event.kind.name} at key {event.key}: the run is already "
                f"at {self._last_key}. An event scheduled at or before the current instant "
                "would act on information the engine did not have (spec 6.2)."
            )
        self._offer(event, -1)

    # ------------------------------------------------------------------ iteration

    def __iter__(self) -> Iterator[Event]:
        while self._heap:
            yield self.pop()

    def pop(self) -> Event:
        """The next event in total order, refilling whichever stream it came from.

        The refill happens *before* the event is returned, so a stream is always one event
        ahead of the clock. That ordering matters for `push`: a hook running on this event
        may schedule another, and the queue has to already know what its streams hold next
        or the comparison would be against a stale heap.
        """
        if not self._heap:
            raise IndexError("pop from an exhausted event queue")
        key, _, event, stream = heapq.heappop(self._heap)
        if self._last_key is not None and key <= self._last_key:
            raise OrderingViolation(_ORDER_VIOLATION.format(key=key))
        self._last_key = key
        self._popped += 1
        if stream >= 0:
            self._advance(stream)
        return event

    @property
    def now_key(self) -> tuple[int, int, int, str] | None:
        """The key of the event most recently popped, or `None` before the first."""
        return self._last_key

    @property
    def next_key(self) -> tuple[int, int, int, str] | None:
        """The key of the event `pop` would return next, or `None` when empty.

        A backtest never needs this: its streams are exhaustible, so draining the queue
        completely is the same thing as replaying the range. A live session does, because its
        queue holds two kinds of event with different futures -- market data that has already
        happened, and order arrivals the engine has *scheduled* ahead of the clock. Draining
        unconditionally pops those arrivals too, advancing the clock past market data that
        has not been released yet; the next batch of frames is then older than the clock and
        `push` refuses it. Peeking lets the caller stop at the point its data is complete to.
        """
        return self._heap[0][0] if self._heap else None

    @property
    def popped(self) -> int:
        return self._popped

    def __len__(self) -> int:
        """Events currently buffered -- *not* events remaining.

        Streams are lazy, so this is at most one per stream plus whatever has been
        scheduled. Named `__len__` anyway because `while queue:` is the natural way to
        drive it and truthiness is all that expression needs.
        """
        return len(self._heap)

    # -------------------------------------------------------------------- internals

    def _advance(self, index: int) -> None:
        event = next(self._streams[index], None)
        if event is not None:
            self._offer(event, index)

    def _offer(self, event: Event, stream: int) -> None:
        self._push_seq += 1
        # `_push_seq` sits between the key and the event so that heapq never compares two
        # `Event`s -- payloads are arbitrary objects and most are not orderable, so a tie
        # would raise `TypeError` from inside heapq with no useful message. Ties are caught
        # at pop time instead, where the error can name the key.
        heapq.heappush(self._heap, (event.key, self._push_seq, event, stream))


def merge_sorted(streams: Sequence[Iterable[Event]]) -> Iterator[Event]:
    """Eagerly merge fully-static streams. Convenience for tests, not used by the engine.

    The engine needs `EventQueue` because it schedules order arrivals mid-run; a test that
    only wants "these rows, in the right order" does not, and building a queue for it
    obscures what is being asserted.
    """
    queue = EventQueue()
    for stream in streams:
        queue.add_stream(stream)
    return iter(queue)
