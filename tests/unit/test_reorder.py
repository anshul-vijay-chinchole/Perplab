"""Live event sequencing and the reorder window (spec 6.2, 6.7).

Every test here is about one of two outcomes that end a live session mid-position: an
ordering tie, and a frame pushed into the past. Both raise `OrderingViolation` out of
`EventQueue` in production, so the properties are pinned here where raising is free.
"""

from __future__ import annotations

import pytest

from perplab.engine.clock import Event, EventKind, EventQueue, OrderingViolation
from perplab.engine.reorder import (
    ReorderBuffer,
    ReorderOverflow,
    SequenceAllocator,
    live_dataset_id,
)


def event(
    ts: int,
    kind: EventKind = EventKind.TRADE,
    seq: int = 0,
    dataset: str = "aggTrades:BTCUSDT",
    payload: object = None,
) -> Event:
    return Event(ts_ms=ts, kind=kind, source_seq=seq, dataset_id=dataset, payload=payload)


# ------------------------------------------------------------------------- the allocator


def test_two_symbols_printing_the_same_agg_id_collide_under_the_backtest_rule() -> None:
    """The failure `SequenceAllocator` exists to prevent, demonstrated end to end.

    `ticks.trade_events` stamps `source_seq = agg_id` under the constant `dataset_id`
    "aggTrades", which is sound for the lake because one sorted query produced every symbol.
    Binance allocates aggregate trade ids per symbol, so BTCUSDT id 90 and ETHUSDT id 90 in
    the same millisecond give the identical key (7, 4, 90, "aggTrades") and the queue refuses
    the tie.
    """
    queue = EventQueue()
    queue.add_stream([event(7, EventKind.TRADE, 90, "aggTrades")])
    queue.add_stream([event(7, EventKind.TRADE, 90, "aggTrades")])
    with pytest.raises(OrderingViolation, match="admit no ties"):
        list(queue)


def test_the_live_rule_separates_two_symbols_that_share_an_exchange_id() -> None:
    """The same two prints, stamped the live way, are two distinct keys.

    `dataset_id` becomes "aggTrades:BTCUSDT" and "aggTrades:ETHUSDT" -- different strings, so
    the fourth component of spec 6.2's key breaks the tie even before the sequence numbers
    do. Both events pop, and the order is the dataset name's, which is deterministic.
    """
    allocator = SequenceAllocator()
    btc_id, btc_seq = allocator.allocate_live("aggTrades", "BTCUSDT")
    eth_id, eth_seq = allocator.allocate_live("aggTrades", "ETHUSDT")

    queue = EventQueue()
    queue.add_stream([event(7, EventKind.TRADE, btc_seq, btc_id)])
    queue.add_stream([event(7, EventKind.TRADE, eth_seq, eth_id)])
    assert [e.dataset_id for e in queue] == ["aggTrades:BTCUSDT", "aggTrades:ETHUSDT"]


def test_the_allocator_hands_out_a_separate_monotonic_sequence_per_dataset() -> None:
    """Three allocations on one dataset are 0, 1, 2; a second dataset restarts at 0.

    Per-dataset rather than global, matching spec 6.2's "monotonic within its own source":
    a global counter would make a stream's sequence depend on what the *other* streams
    happened to deliver, which is the non-determinism the key exists to remove.
    """
    allocator = SequenceAllocator()
    first = [allocator.allocate("bookTicker:BTCUSDT") for _ in range(3)]
    second = allocator.allocate("depth20:BTCUSDT")

    assert first == [0, 1, 2]
    assert second == 0
    assert allocator.issued("bookTicker:BTCUSDT") == 3
    assert allocator.issued("depth20:BTCUSDT") == 1
    assert allocator.issued("aggTrades:BTCUSDT") == 0
    assert allocator.datasets() == ("bookTicker:BTCUSDT", "depth20:BTCUSDT")


def test_a_live_dataset_id_is_the_stream_and_the_symbol_joined_by_a_colon() -> None:
    assert live_dataset_id("aggTrades", "BTCUSDT") == "aggTrades:BTCUSDT"


@pytest.mark.parametrize(
    ("stream", "symbol"),
    [("agg:Trades", "BTCUSDT"), ("aggTrades", "BTC:USDT"), ("", "BTCUSDT"), ("aggTrades", "")],
)
def test_a_dataset_id_that_cannot_be_split_back_apart_is_refused(
    stream: str, symbol: str
) -> None:
    """The colon is the separator, so neither half may contain one.

    An id that cannot be parsed back into `(stream, symbol)` cannot be attributed to a feed,
    and attribution is what a parity report needs in order to say which stream diverged.
    """
    with pytest.raises(ValueError):
        live_dataset_id(stream, symbol)


# ----------------------------------------------------------------------------- the window


def test_events_are_released_in_full_spec_6_2_total_order_key_order() -> None:
    """All four key components decide the order, not the timestamp alone.

    Offered in a deliberately wrong order, the four events at ts 1000 must come out
    MARK_PRICE_UPDATE (kind 0) then the two TRADEs (kind 4) then BAR_CLOSE (kind 6); the two
    trades tie on kind so `source_seq` orders them 1 before 2. The ts-2000 event sorts last on
    the first component regardless of its priority.
    """
    buffer = ReorderBuffer(window_ms=100)
    buffer.offer(event(2000, EventKind.MARK_PRICE_UPDATE, 0, "markPrice:BTCUSDT"), 2000)
    buffer.offer(event(1000, EventKind.BAR_CLOSE, 0, "klines:BTCUSDT"), 1000)
    buffer.offer(event(1000, EventKind.TRADE, 2, "aggTrades:BTCUSDT"), 1000)
    buffer.offer(event(1000, EventKind.MARK_PRICE_UPDATE, 0, "markPrice:BTCUSDT"), 1000)
    buffer.offer(event(1000, EventKind.TRADE, 1, "aggTrades:BTCUSDT"), 1000)

    released = buffer.release(2100)
    assert [(e.ts_ms, int(e.kind), e.source_seq) for e in released] == [
        (1000, 0, 0),
        (1000, 4, 1),
        (1000, 4, 2),
        (1000, 6, 0),
        (2000, 0, 0),
    ]


def test_nothing_is_released_until_the_window_has_elapsed() -> None:
    """An event at ts 1000 with a 250 ms window is released at wall clock 1250, not 1249.

    At wall 1249 the cutoff is 1249 - 250 = 999, and 1000 > 999, so it stays held. At wall
    1250 the cutoff is exactly 1000 and the release bound is inclusive, so it goes.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.offer(event(1000), 1010)

    assert buffer.release(1249) == []
    assert buffer.held == 1
    assert [e.ts_ms for e in buffer.release(1250)] == [1000]
    assert buffer.held == 0


def test_a_late_frame_is_dropped_and_counted_rather_than_pushed() -> None:
    """The watermark has passed 1000, so a frame at or before it can no longer go in order.

    Releasing at wall 1250 with a 250 ms window sets the watermark to 1000. Two frames then
    arrive behind it -- one stamped exactly 1000, which the inclusive bound refuses, and one
    stamped 999, which is unambiguously in the past -- so `late_dropped` goes from 0 to 2.
    Neither is handed to the queue: the last three lines show what the 999 one would have
    cost a session that pushed it anyway.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.offer(event(1000, EventKind.TRADE, 0, "aggTrades:BTCUSDT"), 1010)
    released = buffer.release(1250)
    assert len(released) == 1

    assert buffer.offer(event(1000, EventKind.TRADE, 1, "aggTrades:ETHUSDT"), 1400) is False
    late = event(999, EventKind.TRADE, 1, "aggTrades:ETHUSDT")
    assert buffer.offer(late, 1400) is False
    assert buffer.late_dropped == 2
    assert buffer.held == 0

    queue = EventQueue()
    queue.add_stream(released)
    queue.pop()
    with pytest.raises(OrderingViolation, match="did not have"):
        queue.push(late)


def test_the_watermark_is_wall_clock_so_a_dead_stream_cannot_stall_the_buffer() -> None:
    """One event, then silence. It must still be released.

    A watermark taken from the highest exchange timestamp seen would sit at 1000 forever --
    no later frame ever arrives to advance it -- and the event would never cross its own
    release threshold. The wall clock reaching 5000 releases it regardless, which is what
    keeps a session processing marks through a broken feed.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.offer(event(1000), 1000)
    assert [e.ts_ms for e in buffer.release(5000)] == [1000]
    assert buffer.watermark_ms == 4750


def test_a_wall_clock_that_steps_backwards_does_not_re_admit_late_events() -> None:
    """An NTP correction must not move the watermark back.

    Releasing at wall 2000 with a 250 ms window sets the watermark to 1750. A later call at
    wall 1000 computes a cutoff of 750, which is lower, so the watermark stays at 1750 -- and
    a frame stamped 1500, which the buffer has already released past, is still refused.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.release(2000)
    assert buffer.watermark_ms == 1750

    buffer.release(1000)
    assert buffer.watermark_ms == 1750
    assert buffer.offer(event(1500), 1900) is False
    assert buffer.late_dropped == 1


def test_exceeding_max_held_is_an_error_rather_than_a_silent_trim() -> None:
    """With `max_held=3`, the fourth simultaneous hold raises.

    Trimming would discard market data the session then traded on with nothing in the tape to
    say which frames were missing, and the shadow backtest would report the hole as a fill
    model divergence.
    """
    buffer = ReorderBuffer(window_ms=1000, max_held=3)
    for index in range(3):
        assert buffer.offer(event(2000 + index, EventKind.TRADE, index), 2000) is True
    with pytest.raises(ReorderOverflow, match="max_held"):
        buffer.offer(event(2100, EventKind.TRADE, 99), 2100)


def test_a_duplicate_total_order_key_inside_the_window_is_refused() -> None:
    """Two events with identical keys are the tie spec 6.2 forbids.

    Caught at `offer` rather than at `EventQueue.pop`, so the message can name the cause:
    live events stamped with the exchange's own per-symbol ids instead of allocated ones.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.offer(event(1000, EventKind.TRADE, 5, "aggTrades"), 1000)
    with pytest.raises(OrderingViolation, match="SequenceAllocator"):
        buffer.offer(event(1000, EventKind.TRADE, 5, "aggTrades"), 1000)


def test_a_key_freed_by_release_may_be_offered_again() -> None:
    """The duplicate check is scoped to what is held, not to the whole session.

    Stated because the opposite would be a slow memory leak: a session running for days
    cannot keep every key it has ever seen. A repeat after release is caught by the watermark
    instead, which is the check that matters for ordering.
    """
    buffer = ReorderBuffer(window_ms=0)
    buffer.offer(event(1000, EventKind.TRADE, 5, "aggTrades"), 1000)
    assert len(buffer.release(1000)) == 1
    assert buffer.offer(event(2000, EventKind.TRADE, 5, "aggTrades"), 2000) is True


def test_the_arrival_time_survives_release_so_the_tape_can_record_it() -> None:
    """`TapeWriter.append_market` needs `recv_ms`, and only the buffer still knows it.

    Re-reading the clock at release time would store the moment the window let go of the
    frame, which is a measurement of `window_ms` rather than of feed lag.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.offer(event(1000), 1042)
    records = buffer.release_records(1250)
    assert [(record.event.ts_ms, record.recv_ms) for record in records] == [(1000, 1042)]


def test_draining_at_session_end_releases_everything_still_held() -> None:
    """The last quarter second of a session must not be lost to the window.

    Two events are held and neither has cleared a 250 ms window at wall 1000. `drain` emits
    both, in key order, and moves the watermark to the highest timestamp released so nothing
    can be offered behind them afterwards.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.offer(event(1200, EventKind.TRADE, 2), 1200)
    buffer.offer(event(1100, EventKind.TRADE, 1), 1100)

    assert [e.ts_ms for e in buffer.drain()] == [1100, 1200]
    assert buffer.held == 0
    assert buffer.watermark_ms == 1200
    assert buffer.offer(event(1200, EventKind.TRADE, 3), 1300) is False


def test_draining_an_empty_buffer_leaves_the_watermark_alone() -> None:
    """Session end with nothing held must not invent a watermark of zero.

    A watermark at 0 would be harmless, but one derived from `max()` of an empty sequence
    would raise, and a session shutting down cleanly is not where an exception belongs.
    """
    buffer = ReorderBuffer(window_ms=250)
    assert buffer.drain() == []
    assert buffer.watermark_ms is None


def test_the_maximum_observed_feed_lag_is_reported() -> None:
    """Lags of 120 ms and 50 ms report 120; a frame ahead of our clock reports nothing.

    The third offer has `recv_ms` 10 ms *before* its exchange timestamp, which means our
    clock reads behind Binance's. That is a clock-synchronisation fact, not a flight time, so
    the floor of zero keeps it out of the maximum -- which stays 120.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.offer(event(1000, EventKind.TRADE, 1), 1120)
    buffer.offer(event(1100, EventKind.TRADE, 2), 1150)
    buffer.offer(event(1200, EventKind.TRADE, 3), 1190)
    assert buffer.max_lag_ms == 120


def test_stats_report_what_was_offered_released_and_dropped() -> None:
    """Three offers, one of them late: 2 offered, 2 released, 1 dropped, 0 still held.

    The counts are what `tape.TapeWriter.seal` records, so a parity report can say a session
    dropped frames instead of leaving the divergence unexplained.

    The clock and stall fields are asserted here too, at their quiet values, so that the
    dictionary is pinned as a whole rather than field by field: a counter added and never
    reported is a counter nobody reads.
    """
    buffer = ReorderBuffer(window_ms=250)
    buffer.offer(event(1000, EventKind.TRADE, 1), 1000)
    buffer.offer(event(1100, EventKind.TRADE, 2), 1100)
    buffer.release(1400)
    buffer.offer(event(1100, EventKind.TRADE, 3), 1500)

    assert buffer.stats() == {
        "window_ms": 250,
        "offered": 2,
        "released": 2,
        "late_dropped": 1,
        "still_held": 0,
        "max_lag_ms": 0,
        "clock_regressions": 0,
        "max_clock_regression_ms": 0,
        "stalled_sources": [],
    }


def test_a_negative_window_and_a_zero_ceiling_are_refused() -> None:
    """Both would be silently wrong rather than loudly wrong.

    A negative window releases events before they arrive; a ceiling of zero refuses the first
    offer, so a session would fail at its first frame with an overflow message about a buffer
    that was never able to hold anything.
    """
    with pytest.raises(ValueError, match="window_ms"):
        ReorderBuffer(window_ms=-1)
    with pytest.raises(ValueError, match="max_held"):
        ReorderBuffer(max_held=0)
