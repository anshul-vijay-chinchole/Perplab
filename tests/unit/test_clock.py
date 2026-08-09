"""The deterministic event clock (spec 6.2).

Ordering is the part of a backtest that is silently wrong. Every test here is about the
guarantee that two runs of the same inputs process the same events in the same sequence --
not about whether the numbers are right, which the accounting tests own.
"""

from __future__ import annotations

import pytest

from perplab.engine.clock import Event, EventKind, EventQueue, OrderingViolation, merge_sorted


def event(ts: int, kind: EventKind, seq: int = 0, dataset: str = "a", payload=None) -> Event:
    return Event(ts_ms=ts, kind=kind, source_seq=seq, dataset_id=dataset, payload=payload)


def test_the_priority_table_matches_spec_6_2_exactly() -> None:
    """The nine kinds and their numbers are a contract, not an implementation detail.

    Renumbering would silently reorder every stored run, so this asserts the values rather
    than the relative order: a change that preserved the order but shifted the numbers would
    still break comparison against runs recorded under the old table.
    """
    assert EventKind.MARK_PRICE_UPDATE == 0
    assert EventKind.FUNDING_SETTLEMENT == 1
    assert EventKind.LIQUIDATION_CHECK == 2
    assert EventKind.BOOK_UPDATE == 3
    assert EventKind.TRADE == 4
    assert EventKind.ORDER_FILL_CHECK == 5
    assert EventKind.BAR_CLOSE == 6
    assert EventKind.ORDER_SUBMIT == 7
    assert EventKind.ORDER_ARRIVAL == 8


def test_funding_settles_before_the_liquidation_check() -> None:
    """Spec 6.2's R5. A funding payment reduces margin and can cause the liquidation.

    Asserted at the queue level as well as in the engine because it is a property of the
    ordering itself: if these two numbers were ever swapped, every downstream test that
    happens not to have a marginal position would still pass.
    """
    assert EventKind.FUNDING_SETTLEMENT < EventKind.LIQUIDATION_CHECK
    assert EventKind.MARK_PRICE_UPDATE < EventKind.FUNDING_SETTLEMENT
    assert EventKind.LIQUIDATION_CHECK < EventKind.ORDER_FILL_CHECK
    assert EventKind.TRADE < EventKind.BAR_CLOSE


def test_events_at_one_millisecond_come_out_in_priority_order() -> None:
    queue = EventQueue()
    queue.add_stream([event(100, EventKind.BAR_CLOSE, 1)])
    queue.add_stream([event(100, EventKind.MARK_PRICE_UPDATE, 1, "b")])
    queue.add_stream([event(100, EventKind.FUNDING_SETTLEMENT, 1, "c")])
    assert [e.kind for e in queue] == [
        EventKind.MARK_PRICE_UPDATE,
        EventKind.FUNDING_SETTLEMENT,
        EventKind.BAR_CLOSE,
    ]


def test_the_dataset_id_breaks_a_tie_that_source_seq_cannot() -> None:
    """Two datasets both emitting their first row at the same instant.

    Not hypothetical: it is what happens whenever a range starts on a partition boundary.
    Without the fourth key component these two would be an ordering tie.
    """
    queue = EventQueue()
    queue.add_stream([event(5, EventKind.TRADE, 0, "zeta")])
    queue.add_stream([event(5, EventKind.TRADE, 0, "alpha")])
    assert [e.dataset_id for e in queue] == ["alpha", "zeta"]


def test_a_genuine_tie_is_fatal_rather_than_arbitrary() -> None:
    """Identical keys mean the run's hash depends on the interpreter, not on the data."""
    queue = EventQueue()
    queue.add_stream([event(5, EventKind.TRADE, 0, "same")])
    queue.add_stream([event(5, EventKind.TRADE, 0, "same")])
    with pytest.raises(OrderingViolation, match="admit no ties"):
        list(queue)


def test_an_out_of_order_stream_is_caught_at_the_first_offending_row() -> None:
    queue = EventQueue()
    queue.add_stream(
        [event(10, EventKind.TRADE, 1), event(5, EventKind.TRADE, 2)]
    )
    with pytest.raises(OrderingViolation):
        list(queue)


def test_scheduling_into_the_past_is_refused() -> None:
    """The structural half of the no-look-ahead guarantee.

    An engine that scheduled a fill before the instant it was processing would be acting on
    information it did not have. Raising makes that a bug report rather than a better number.
    """
    queue = EventQueue()
    queue.add_stream([event(100, EventKind.BAR_CLOSE, 1)])
    queue.pop()
    with pytest.raises(OrderingViolation, match="did not have"):
        queue.push(event(99, EventKind.ORDER_ARRIVAL, 1, "engine"))


def test_scheduling_at_the_current_instant_but_a_later_priority_is_allowed() -> None:
    """This is how the engine queues its own liquidation check after a mark update."""
    queue = EventQueue()
    queue.add_stream([event(100, EventKind.MARK_PRICE_UPDATE, 1)])
    queue.pop()
    queue.push(event(100, EventKind.LIQUIDATION_CHECK, 1, "engine"))
    assert queue.pop().kind is EventKind.LIQUIDATION_CHECK


def test_scheduling_at_the_same_key_is_refused() -> None:
    queue = EventQueue()
    queue.add_stream([event(100, EventKind.LIQUIDATION_CHECK, 1, "engine")])
    queue.pop()
    with pytest.raises(OrderingViolation):
        queue.push(event(100, EventKind.LIQUIDATION_CHECK, 1, "engine"))


def test_unorderable_payloads_never_reach_the_heap_comparison() -> None:
    """Payloads are arbitrary objects; two events sharing a key must not be compared.

    Without the push counter between the key and the event, heapq would fall through to
    comparing payloads and raise `TypeError` from inside the standard library -- a failure
    with no useful message about an ordering problem that does have one.
    """

    class Opaque:
        pass

    queue = EventQueue()
    queue.add_stream([event(1, EventKind.TRADE, 0, "a", Opaque())])
    queue.add_stream([event(1, EventKind.TRADE, 0, "b", Opaque())])
    assert len(list(queue)) == 2


def test_streams_are_consumed_lazily() -> None:
    """A year of bars must never be materialised to start a run."""
    pulled = 0

    def stream():
        nonlocal pulled
        for index in range(1000):
            pulled += 1
            yield event(index, EventKind.TRADE, index)

    queue = EventQueue()
    queue.add_stream(stream())
    assert pulled == 1
    queue.pop()
    assert pulled == 2


def test_merge_sorted_interleaves_two_streams() -> None:
    left = [event(t, EventKind.TRADE, i, "l") for i, t in enumerate((0, 10, 20))]
    right = [event(t, EventKind.TRADE, i, "r") for i, t in enumerate((5, 15, 25))]
    assert [e.ts_ms for e in merge_sorted([left, right])] == [0, 5, 10, 15, 20, 25]


def test_popped_counts_and_now_key_track_the_run() -> None:
    queue = EventQueue()
    queue.add_stream([event(1, EventKind.TRADE, 0), event(2, EventKind.TRADE, 1)])
    assert queue.now_key is None
    queue.pop()
    assert queue.popped == 1
    assert queue.now_key == (1, int(EventKind.TRADE), 0, "a")


def test_popping_an_exhausted_queue_raises() -> None:
    queue = EventQueue()
    with pytest.raises(IndexError):
        queue.pop()
