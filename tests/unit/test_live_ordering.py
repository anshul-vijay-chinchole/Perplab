"""The live ordering guarantees: per-source watermarks, and the session's drain horizon.

`tests/unit/test_reorder.py` pins the reorder window itself -- the wall-clock watermark, the
late-frame drop, the overflow ceiling, the sequence allocator. This file pins the two
mechanisms layered on top of it, both of which exist because a real testnet session produced
a run that looked healthy and had done nothing.

**Per-source watermarks (`ReorderBuffer.expect` / `complete_through`).** A polled or bucketed
series is discovered well after the instant it describes: a 1m kline closes at T and is not
published until the next poll, a depth snapshot for second N is published when N+1 opens, a
mark bar for minute M when M+1's first sample arrives. Against a wall-clock-only watermark
every one of those is behind the release cutoff by the time it is in hand, so every one was
dropped as late -- zero bar closes reached the engine, the strategy never traded, and nothing
raised. Two further properties are load-bearing and were each wrong once: staleness is time
since a source last *reported*, not how far behind its completion point is; and a repeated,
unchanged completion point is still a liveness signal.

**The drain horizon (`EventQueue.next_key`, `PaperSession._drain_to_engine`).** The engine's
queue holds two kinds of event with different futures -- market data that has already
happened, and order arrivals the engine has scheduled *ahead* of the clock. Draining it
unconditionally pops those arrivals, drags the clock past market data the reorder buffer has
not released yet, and the next batch of frames is then refused by `EventQueue.push` as out of
order.

Every instant here is a number the test writes. The session tests replace the session
module's wall clock outright rather than sleeping, so what the drain does is a function of
inputs the test chose rather than of how long the test took to run.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from perplab.core.money import parse_money, to_scaled
from perplab.core.types import Bar, DepthSnapshot
from perplab.engine.backtest import BacktestConfig
from perplab.engine.clock import Event, EventKind, EventQueue
from perplab.engine.feed import BarStep
from perplab.engine.latency import FixedLatency
from perplab.engine.reorder import ReorderBuffer, live_dataset_id
from perplab.engine.ticks import TopOfBook, TradePrint
from perplab.live import session as live_session
from perplab.live.feed import DEPTH_BUCKET_MS, MARK_BAR_MS, LiveFeed
from perplab.live.session import PaperSession, SessionConfig
from perplab.strategy.base import Strategy
from perplab.strategy.context import Context, FillTier
from tests.support import btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
MINUTE_MS = 60_000
WINDOW_MS = 250
"""The reorder window every buffer here is built with, matching `DEFAULT_WINDOW_MS`."""

KLINE_STALE_MS = LiveFeed.SLOW_SOURCES["klines"]
"""The feed's own staleness bound for the kline poller, so the tests move if it does."""


def market_event(
    ts_ms: int,
    kind: EventKind = EventKind.TRADE,
    seq: int = 0,
    dataset: str = "aggTrades:BTCUSDT",
) -> Event:
    """A payload-free event, for the tests that only care about ordering."""
    return Event(ts_ms=ts_ms, kind=kind, source_seq=seq, dataset_id=dataset)


# ------------------------------------------------------- sources discovered after the fact


def test_a_kline_discovered_after_its_close_is_still_released_before_its_own_trades() -> None:
    """The bug that stopped every bar close reaching the engine, and its fix, side by side.

    The minute [600 000, 659 999] closes at 659 999. Two trades inside it arrive within 20 ms
    of printing. The kline poller runs once a second, so while the minute is still open it
    reports "complete through 600 000" -- the open bar's start -- and only at wall 660 900,
    nine hundred milliseconds after the close, does it hand over the closed bar and advance
    to 660 000.

    At wall 660 400 the wall-clock cutoff is 660 400 - 250 = 660 150, which is already past
    the bar's close time. `expect` is what stops that cutoff being used: the declared source
    pins it at 600 000, both trades stay held, and the bar arriving five hundred milliseconds
    later still slots in *before* them -- released in spec 6.2 order, with BAR_CLOSE (priority
    6) after the TRADEs (priority 4) that formed it.

    The `undeclared` buffer is the same feed without the declaration and is exactly what a
    real session did: it releases the trades at 660 400, and the bar is then refused.
    """
    bar_open_ms = 600_000
    bar_close_ms = bar_open_ms + MINUTE_MS - 1
    poll_inside_minute_ms = bar_close_ms - 199
    poll_after_close_ms = bar_close_ms + 901

    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    buffer.expect("klines", stale_after_ms=KLINE_STALE_MS)
    undeclared = ReorderBuffer(window_ms=WINDOW_MS)

    early_trade = market_event(bar_close_ms - 999, EventKind.TRADE, 1)
    late_trade = market_event(bar_close_ms - 499, EventKind.TRADE, 2)
    for candidate in (buffer, undeclared):
        assert candidate.offer(early_trade, early_trade.ts_ms + 20) is True
        assert candidate.offer(late_trade, late_trade.ts_ms + 20) is True
    buffer.complete_through("klines", bar_open_ms, wall_now_ms=poll_inside_minute_ms)

    # The wall clock is past the window for both trades (660 400 - 250 = 660 150), and past
    # the bar's close time too -- which is the whole problem.
    released_early = buffer.release(bar_close_ms + 401)
    assert released_early == []
    assert [event.ts_ms for event in undeclared.release(bar_close_ms + 401)] == [
        early_trade.ts_ms,
        late_trade.ts_ms,
    ]

    bar = market_event(bar_close_ms, EventKind.BAR_CLOSE, 0, "klines:BTCUSDT")
    assert buffer.offer(bar, poll_after_close_ms) is True
    assert undeclared.offer(bar, poll_after_close_ms) is False
    assert undeclared.late_dropped == 1

    buffer.complete_through("klines", bar_close_ms + 1, wall_now_ms=poll_after_close_ms)
    released = buffer.release(poll_after_close_ms + 100)
    assert [(event.ts_ms, int(event.kind)) for event in released] == [
        (early_trade.ts_ms, int(EventKind.TRADE)),
        (late_trade.ts_ms, int(EventKind.TRADE)),
        (bar_close_ms, int(EventKind.BAR_CLOSE)),
    ]
    assert buffer.late_dropped == 0


@pytest.mark.parametrize(
    ("source", "bucket_ms", "kind"),
    [
        ("depth20", DEPTH_BUCKET_MS, EventKind.BOOK_UPDATE),
        ("markPrice", MARK_BAR_MS, EventKind.MARK_PRICE_UPDATE),
    ],
)
def test_a_bucketed_source_is_late_by_construction_and_is_still_dispatched_in_order(
    source: str, bucket_ms: int, kind: EventKind
) -> None:
    """A bucket's event is published when the *next* bucket opens, so it is always late.

    Depth is downsampled to one second and mark price aggregated to one minute, both to match
    what the lake holds. Neither bucket can be published until a frame from the following one
    proves it ended, so an event stamped at the bucket's last millisecond reaches the buffer
    four hundred milliseconds later -- past a two hundred and fifty millisecond window, every
    single time, on a perfectly healthy feed.

    Every instant below is derived from `bucket_ms`, so the same reasoning is checked at both
    cadences: the bucket under test runs from `10 * bucket_ms` to `11 * bucket_ms - 1`, the
    source reports "complete through `bucket_start - 1`" while it is still filling, and the
    event lands 401 ms after the bucket's end. The `undeclared` buffer -- the same feed with
    no declaration -- has already released past it by then.
    """
    bucket_start = 10 * bucket_ms
    bucket_end = bucket_start + bucket_ms - 1
    reported_wall_ms = bucket_end - 100
    publish_wall_ms = bucket_end + 401

    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    buffer.expect(source, stale_after_ms=LiveFeed.SLOW_SOURCES[source])
    undeclared = ReorderBuffer(window_ms=WINDOW_MS)

    trade = market_event(bucket_end - 500, EventKind.TRADE, 1)
    for candidate in (buffer, undeclared):
        assert candidate.offer(trade, trade.ts_ms + 10) is True
    buffer.complete_through(source, bucket_start - 1, wall_now_ms=reported_wall_ms)

    # 300 ms after the bucket ended: the wall-clock cutoff (bucket_end + 50) is past both the
    # trade and the bucket's own timestamp, and the declared source holds it at
    # bucket_start - 1 instead.
    assert buffer.release(bucket_end + 300) == []
    assert [e.ts_ms for e in undeclared.release(bucket_end + 300)] == [trade.ts_ms]

    bucketed = market_event(bucket_end, kind, 0, f"{source}:{SYMBOL}")
    assert buffer.offer(bucketed, publish_wall_ms) is True
    assert undeclared.offer(bucketed, publish_wall_ms) is False

    buffer.complete_through(source, bucket_end, wall_now_ms=publish_wall_ms)
    assert [e.ts_ms for e in buffer.release(publish_wall_ms + 100)] == [
        trade.ts_ms,
        bucket_end,
    ]


# ------------------------------------------------------------------------------- staleness


def test_a_source_reporting_every_hundred_milliseconds_is_healthy_however_far_behind() -> None:
    """Staleness is time since the source last reported, not how far behind it is.

    The source here reports every 100 ms, and every report says it is complete through
    exactly 60 000 ms ago -- a minute-cadence series behaving perfectly. Two hundred such
    reports span 19 900 ms of wall clock, more than its own 15 000 ms bound, so a rule that
    measured either quantity wrongly would have declared it dead somewhere in here.

    Judged on `wall_now - through_ms` it is 60 000 ms behind from the very first report and
    is condemned immediately; judged on `wall_now - reported_wall_ms` it is never silent for
    more than 100 ms. The difference is visible in what gets released: the event stamped
    10 000 ms *after* the final completion point is still held at the end, which is only true
    if the source is still pinning the cutoff.
    """
    lag_ms = 60_000
    cadence_ms = 100
    reports = 200
    start_wall_ms = 1_000_000
    final_wall_ms = start_wall_ms + (reports - 1) * cadence_ms
    final_through_ms = final_wall_ms - lag_ms

    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    buffer.expect("markPrice", stale_after_ms=LiveFeed.SLOW_SOURCES["markPrice"])
    behind = market_event(start_wall_ms - lag_ms - 10_000, EventKind.TRADE, 1)
    ahead = market_event(final_through_ms + 10_000, EventKind.TRADE, 2)
    assert buffer.offer(behind, behind.ts_ms + 10) is True
    assert buffer.offer(ahead, ahead.ts_ms + 10) is True

    released: list[Event] = []
    for index in range(reports):
        wall_ms = start_wall_ms + index * cadence_ms
        buffer.complete_through("markPrice", wall_ms - lag_ms, wall_now_ms=wall_ms)
        released.extend(buffer.release(wall_ms))

    assert buffer.stalled_sources == ()
    assert [event.ts_ms for event in released] == [behind.ts_ms]
    assert buffer.held == 1
    assert buffer.watermark_ms == final_through_ms


def test_a_source_that_goes_silent_past_its_bound_stops_holding_the_watermark() -> None:
    """Silence, not lag, is what declares a source dead -- and death releases the backlog.

    The source reports once, at wall 100 100, that it is complete through 99 000. A trade
    stamped 100 000 is therefore held: it sits beyond the source's completion point, however
    long the wall clock runs on.

    The bound is 15 000 ms, so silence of exactly 15 000 ms is not yet stalled -- the check is
    strictly greater, and a source that reports on a 15 000 ms cadence must survive its own
    cadence. One millisecond later it is stalled, drops out of the cutoff, and the wall-clock
    watermark (115 101 - 250 = 114 851) releases the trade. That last step is the point: a
    per-source watermark that only ever moved forward on delivery would let one dead poller
    freeze a session holding an open position.
    """
    reported_wall_ms = 100_100
    through_ms = 99_000
    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    buffer.expect("klines", stale_after_ms=KLINE_STALE_MS)

    trade = market_event(100_000, EventKind.TRADE, 1)
    assert buffer.offer(trade, trade.ts_ms + 20) is True
    buffer.complete_through("klines", through_ms, wall_now_ms=reported_wall_ms)

    assert buffer.release(reported_wall_ms + 400) == []
    assert buffer.release(reported_wall_ms + KLINE_STALE_MS) == []
    assert buffer.stalled_sources == ()

    released = buffer.release(reported_wall_ms + KLINE_STALE_MS + 1)
    assert [event.ts_ms for event in released] == [trade.ts_ms]
    assert buffer.stalled_sources == ("klines",)
    assert buffer.watermark_ms == reported_wall_ms + KLINE_STALE_MS + 1 - WINDOW_MS


def test_a_declared_source_that_has_never_reported_still_holds_the_watermark_back() -> None:
    """The startup window: declared, not yet heard from, and not yet dead either.

    Every other slow-source test here reports at least once before asserting anything, which
    left the first fifteen seconds of every session untested -- and that is the stretch a
    session actually starts in. `expect` registers a source with `reported_wall_ms=0`, so it
    is neither live (it has no completion point to contribute) nor stalled (silence is
    measured from a report that has not happened). Holding on the wall clock alone for that
    stretch would release market data the kline poller is still fetching, and the bars would
    then be dropped as late by `offer` -- the session's first minute, silently missing.

    So an undeclared-yet source holds the cutoff at `wall - stale_after_ms`: far enough back
    that a source about to report cannot be overtaken, and bounded, so a poller that never
    starts at all cannot freeze the session for ever. The trade at 100 000 is therefore held
    until wall 115 000 -- `stale_after_ms` after its own timestamp, not `window_ms` after it.
    """
    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    buffer.expect("klines", stale_after_ms=KLINE_STALE_MS)

    trade = market_event(100_000, EventKind.TRADE, 1)
    assert buffer.offer(trade, trade.ts_ms + 20) is True

    # The ordinary wall-clock watermark alone would have released it here, one window later.
    assert buffer.release(trade.ts_ms + WINDOW_MS) == []
    assert buffer.stalled_sources == ()
    assert buffer.release(trade.ts_ms + KLINE_STALE_MS - 1) == []

    released = buffer.release(trade.ts_ms + KLINE_STALE_MS)
    assert [event.ts_ms for event in released] == [trade.ts_ms]
    # Still not *stalled* -- silence is only counted once a source has reported at all, so a
    # source that has never spoken drops out of the cutoff by the bound rather than by death.
    assert buffer.stalled_sources == ()


def test_repeating_an_unchanged_completion_point_still_counts_as_a_sign_of_life() -> None:
    """An unchanged watermark is a liveness signal; suppressing the repeat is silence.

    A minute-cadence kline source advances its completion point once a minute and reports
    once a second, so all but one report in sixty carries the same number. Deduping them made
    the buffer hear nothing for up to a minute, declare the source stalled at fifteen seconds,
    and drop every bar close for the rest of the session.

    The reports below span 100 100 to 116 100 and all carry the same completion point of
    99 000. The release at 116 500 is 400 ms after the last of them and 16 400 ms after the
    first -- so under the repeats the source is healthy with room to spare, and had they been
    suppressed it would be 1 400 ms past its 15 000 ms bound, would have been declared
    stalled, and the trade held behind it would have been released past.
    """
    through_ms = 99_000
    first_report_ms = 100_100
    last_report_ms = 116_100
    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    buffer.expect("klines", stale_after_ms=KLINE_STALE_MS)

    trade = market_event(100_000, EventKind.TRADE, 1)
    assert buffer.offer(trade, trade.ts_ms + 20) is True
    for wall_ms in range(first_report_ms, last_report_ms + 1, 1_000):
        buffer.complete_through("klines", through_ms, wall_now_ms=wall_ms)

    assert last_report_ms - first_report_ms > KLINE_STALE_MS, "the repeats must span the bound"
    assert buffer.release(last_report_ms + 400) == []
    assert buffer.stalled_sources == ()
    assert buffer.held == 1


def test_completing_through_a_source_that_was_never_declared_raises() -> None:
    """An undeclared source must not silently do nothing.

    Silently accepting the report would leave the source holding nothing back, so its own
    events would go on being dropped as late -- which is the original defect wearing the
    fix's clothes, and is exactly as invisible.
    """
    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    with pytest.raises(KeyError, match="expect"):
        buffer.complete_through("klines", 1_000, wall_now_ms=2_000)


def test_a_completion_point_that_moves_backwards_is_ignored() -> None:
    """Per-source watermarks are monotonic, for the reason the global one is.

    A poller re-serving an older page reports 30 000 after having reported 60 000. Honouring
    it would re-open a stretch the buffer had been cleared to release, so the cutoff stays at
    60 000: the trade at 50 000 goes and the one at 70 000 stays. Under a non-monotonic
    implementation the cutoff would fall back to 30 000 and neither would be released.
    """
    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    buffer.expect("klines", stale_after_ms=KLINE_STALE_MS)
    assert buffer.offer(market_event(50_000, EventKind.TRADE, 1), 50_010) is True
    assert buffer.offer(market_event(70_000, EventKind.TRADE, 2), 70_010) is True

    buffer.complete_through("klines", 60_000, wall_now_ms=100_000)
    buffer.complete_through("klines", 30_000, wall_now_ms=100_100)

    released = buffer.release(100_200)
    assert [event.ts_ms for event in released] == [50_000]
    assert buffer.held == 1
    assert buffer.watermark_ms == 60_000


# ------------------------------------------------------------------------ the total order


def test_release_order_is_the_whole_key_even_when_the_arrival_order_disagrees() -> None:
    """All four components decide dispatch order, and `recv_ms` decides none of it.

    Four events share the timestamp 500 000 and are offered in a wall-clock order that is
    the reverse of their dispatch order in two separate ways:

    - the BAR_CLOSE (priority 6) arrives first, at 500 100, and must be dispatched **last**,
      because the bar's closing trades are part of the bar; the MARK_PRICE_UPDATE (priority
      0) arrives last, at 500 900, and must be dispatched first, because risk state has to be
      current before anything reads it;
    - the two TRADEs share a timestamp, a priority *and* a `source_seq` of 7, so only the
      fourth component can separate them -- and "aggTrades:BTCUSDT" sorts before
      "aggTrades:ETHUSDT", regardless of which arrived first.

    That last pair is the live rule doing its job: one sequence space per stream per symbol
    means two symbols may legitimately reuse a sequence number in one millisecond.
    """
    ts_ms = 500_000
    buffer = ReorderBuffer(window_ms=WINDOW_MS)
    arrivals = [
        (market_event(ts_ms, EventKind.BAR_CLOSE, 0, "klines:BTCUSDT"), ts_ms + 100),
        (market_event(ts_ms, EventKind.TRADE, 7, "aggTrades:ETHUSDT"), ts_ms + 200),
        (market_event(ts_ms, EventKind.TRADE, 7, "aggTrades:BTCUSDT"), ts_ms + 300),
        (market_event(ts_ms, EventKind.MARK_PRICE_UPDATE, 0, "markPrice:BTCUSDT"), ts_ms + 900),
    ]
    for event, recv_ms in arrivals:
        assert buffer.offer(event, recv_ms) is True

    released = buffer.release(ts_ms + 900 + WINDOW_MS)
    assert [(int(e.kind), e.dataset_id) for e in released] == [
        (int(EventKind.MARK_PRICE_UPDATE), "markPrice:BTCUSDT"),
        (int(EventKind.TRADE), "aggTrades:BTCUSDT"),
        (int(EventKind.TRADE), "aggTrades:ETHUSDT"),
        (int(EventKind.BAR_CLOSE), "klines:BTCUSDT"),
    ]


def test_next_key_is_the_key_pop_would_return_and_none_when_the_queue_is_empty() -> None:
    """The peek the live drain stops on. It must not consume, and must see pushes.

    A backtest never needs this -- draining to exhaustion is the same thing as replaying the
    range -- so the whole of `next_key`'s contract is what a live session asks of it: what
    would come out next, without taking it, including anything scheduled mid-run.
    """
    queue = EventQueue()
    assert queue.next_key is None

    trade = market_event(1_000, EventKind.TRADE, 0, "aggTrades:BTCUSDT")
    bar = market_event(1_000, EventKind.BAR_CLOSE, 0, "klines:BTCUSDT")
    queue.add_stream([trade, bar])
    assert queue.next_key == trade.key
    assert queue.next_key == trade.key  # peeking twice is still peeking

    arrival = market_event(1_500, EventKind.ORDER_ARRIVAL, 1, "engine")
    queue.push(arrival)
    assert queue.next_key == trade.key

    assert queue.pop() is trade
    assert queue.next_key == bar.key
    assert queue.pop() is bar
    assert queue.next_key == (1_500, int(EventKind.ORDER_ARRIVAL), 1, "engine")
    assert queue.pop() is arrival
    assert queue.next_key is None


# ------------------------------------------------------------------- the session's drain

BAR_OPEN_MS = 1_760_000_040_000
"""A real 1m kline open: an exact multiple of 60 000."""

BAR_CLOSE_MS = BAR_OPEN_MS + MINUTE_MS - 1
SUBMIT_LATENCY_MS = 120
"""Spec 6.3's default submit median, fixed here so the arrival instant is a number the test
wrote rather than a draw from a distribution."""

ORDER_QTY = "0.01"
PRICE = "60000"
"""0.01 at 60 000 is a notional of 600, clear of BTCUSDT's MIN_NOTIONAL of 50, and a step of
0.001 divides the quantity exactly -- so nothing in the filter layer can refuse this order
and turn a drain-order failure into a quantisation one."""


class _Clock:
    """The wall clock `perplab.live.session` reads, as a value the test sets."""

    def __init__(self, now_ms: int) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


class BuyOnFirstBar(Strategy):
    """Buys once, on the first bar close it is warm for.

    The order itself is incidental. What the test needs is the `ORDER_ARRIVAL` the engine
    schedules `SUBMIT_LATENCY_MS` into the future when the order is submitted, because that
    is the event the drain must refuse to pop early.
    """

    requires = {"symbols": [SYMBOL], "timeframe": "1m", "history": 1, "datasets": ["klines"]}

    def on_start(self, ctx: Context) -> None:
        self.submitted = False

    def on_bar(self, ctx: Context, bar: Bar) -> None:
        if self.submitted or not ctx.warm:
            return
        self.submitted = True
        ctx.buy(qty=ctx.money(ORDER_QTY))


def paper_session(
    run_dir: Path, strategy: Strategy, *, fill_tier: FillTier = FillTier.BAR_CLOSE
) -> PaperSession:
    """A session over no lake at all -- `_PushSource` supplies no streams.

    `fill_tier` stays at the engine's own default, because the drain tests care about which
    events are dispatched and not about what they are priced against; a test that needs a
    book to fill from asks for the tier that has one.
    """
    config = BacktestConfig(
        symbols=(SYMBOL,),
        timeframe="1m",
        start_ms=BAR_OPEN_MS - MINUTE_MS,
        end_ms=BAR_OPEN_MS + 60 * MINUTE_MS,
        opening_balance=parse_money("10000"),
        leverage=10,
        latency=FixedLatency(submit=SUBMIT_LATENCY_MS, cancel=SUBMIT_LATENCY_MS),
        fill_tier=fill_tier,
    )
    return PaperSession(
        run_dir=run_dir,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        session=SessionConfig(run_id=1, endpoint="testnet", reorder_window_ms=WINDOW_MS),
        filters={SYMBOL: btcusdt_filters()},
        brackets={SYMBOL: single_bracket_table()},
    )


def report_every_source_complete(session: PaperSession, through_ms: int) -> None:
    """Every slow source reporting the same completion point, as a poll cycle would.

    A declared source that has never reported holds the cutoff back to
    `wall_now - stale_after_ms` -- two minutes, for funding -- so a session test that skipped
    this would be measuring that bound instead of the drain.
    """
    for source in LiveFeed.SLOW_SOURCES:
        session._complete_through(source, through_ms)


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


def bar_close_event(close_time_ms: int, seq: int) -> Event:
    bar = Bar(
        symbol=SYMBOL,
        open_time=close_time_ms - MINUTE_MS + 1,
        close_time=close_time_ms,
        open=to_scaled(PRICE),
        high=to_scaled(PRICE),
        low=to_scaled(PRICE),
        close=to_scaled(PRICE),
        volume=to_scaled("10"),
        quote_volume=to_scaled("600000"),
        trades=100,
    )
    return Event(
        ts_ms=close_time_ms,
        kind=EventKind.BAR_CLOSE,
        source_seq=seq,
        dataset_id=live_dataset_id("klines", SYMBOL),
        payload=BarStep(close_time=close_time_ms, bars=(bar,)),
    )


def depth_event(ts_ms: int, seq: int) -> Event:
    """A `depth20` ladder whose touch is thinner than the order the strategy sends.

    0.005 resting at each of the two best asks, so a 0.01 buy takes both and pays a
    volume-weighted 60 005 rather than the 60 000 on display.
    """
    return Event(
        ts_ms=ts_ms,
        kind=EventKind.BOOK_UPDATE,
        source_seq=seq,
        dataset_id=live_dataset_id("depth20", SYMBOL),
        payload=DepthSnapshot(
            symbol=SYMBOL,
            ts_ms=ts_ms,
            recv_ms=ts_ms + 400,
            last_update_id=390_497_878,
            bid_px=(to_scaled("59999"), to_scaled("59998"), to_scaled("59997")),
            bid_qty=(to_scaled("0.005"), to_scaled("0.005"), to_scaled("0.01")),
            ask_px=(to_scaled("60000"), to_scaled("60010"), to_scaled("60020")),
            ask_qty=(to_scaled("0.005"), to_scaled("0.005"), to_scaled("0.01")),
        ),
    )


def book_ticker_event(ts_ms: int, seq: int) -> Event:
    """A `bookTicker` quote at the ladder's own touch, publishing far more size than it.

    The 0.5 on each side is what a top-of-book row says: everything resting at the best
    price and nothing whatever about the price behind it.
    """
    return Event(
        ts_ms=ts_ms,
        kind=EventKind.BOOK_UPDATE,
        source_seq=seq,
        dataset_id=live_dataset_id("bookTicker", SYMBOL),
        payload=TopOfBook(
            symbol=SYMBOL,
            ts_ms=ts_ms,
            bid_px=to_scaled("59999.9"),
            bid_qty=to_scaled("0.5"),
            ask_px=to_scaled("60000"),
            ask_qty=to_scaled("0.5"),
        ),
    )


def test_the_drain_stops_at_the_released_frontier_so_a_scheduled_arrival_cannot_lead_the_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The queue holds the future as well as the past, and only the past may be dispatched.

    A bar closing at `BAR_CLOSE_MS` makes the strategy submit, and a fixed 120 ms of submit
    latency puts the resulting `ORDER_ARRIVAL` at `BAR_CLOSE_MS + 120` -- an event the engine
    scheduled *ahead* of every frame the feed has delivered. Draining the queue to exhaustion
    pops it, which moves `EventQueue._last_key` to `(BAR_CLOSE_MS + 120, 8, ...)`; the next
    trade off the socket is stamped `BAR_CLOSE_MS + 50`, earlier than that, and `push` then
    refuses it as an event scheduled into the past. Measured on a real session: one bookTicker
    frame lost per session, to nothing worse than an order being in flight.

    So the three assertions that matter are that the clock stops at `BAR_CLOSE_MS` while the
    arrival waits, that the trade at `+50` is accepted and dispatched, and that the arrival is
    not *lost* -- it fires as soon as market data genuinely reaches its instant, which is what
    makes the latency model mean anything live.
    """
    clock = _Clock(BAR_CLOSE_MS + 500)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    strategy = BuyOnFirstBar()
    session = paper_session(tmp_path, strategy)
    session.engine.start()
    arrival_ms = BAR_CLOSE_MS + SUBMIT_LATENCY_MS

    # The poll that discovers the closed bar. Every slow source is complete through the bar's
    # close, so the cutoff is the bar's own timestamp rather than 500 ms of wall clock.
    report_every_source_complete(session, BAR_CLOSE_MS)
    session._offer(trade_event(BAR_OPEN_MS + 30_000, 0), BAR_OPEN_MS + 30_020)
    session._offer(bar_close_event(BAR_CLOSE_MS, 0), clock.now_ms)
    session._drain_to_engine()

    assert strategy.submitted, "the bar close must have reached on_bar"
    assert session.engine.runtime.now_ms == BAR_CLOSE_MS, (
        "the clock must stop at the released frontier, not at the scheduled arrival"
    )
    assert session.processed == 2
    assert len(session.engine.queue) == 1
    assert session.engine.queue.next_key is not None
    assert session.engine.queue.next_key[:2] == (arrival_ms, int(EventKind.ORDER_ARRIVAL))

    # A frame stamped *before* the scheduled arrival, which is the case the horizon exists
    # for: the market has not reached the arrival's instant yet, so nothing may act as if it
    # had.
    late_ms = BAR_CLOSE_MS + 50
    assert late_ms < arrival_ms
    session._offer(trade_event(late_ms, 1), late_ms + 20)
    assert session.buffer.held == 1, "the reorder buffer must not have dropped it as late"

    clock.now_ms = BAR_CLOSE_MS + 900
    report_every_source_complete(session, BAR_CLOSE_MS + 100)
    session._drain_to_engine()

    assert [entry for entry in session.status_log if "refused" in entry["detail"]] == []
    assert session.engine.runtime.now_ms == late_ms
    assert session.engine.counts["ticks"] == 2
    assert session.processed == 3
    assert session.engine.queue.next_key is not None
    assert session.engine.queue.next_key[:2] == (arrival_ms, int(EventKind.ORDER_ARRIVAL))

    # Market data finally reaches past the arrival's instant, and only now does it dispatch.
    later_ms = BAR_CLOSE_MS + 200
    assert later_ms > arrival_ms
    session._offer(trade_event(later_ms, 2), later_ms + 20)
    clock.now_ms = BAR_CLOSE_MS + 1_500
    report_every_source_complete(session, BAR_CLOSE_MS + 1_000)
    session._drain_to_engine()

    assert session.engine.runtime.now_ms == later_ms
    assert session.engine.counts["fills"] == 1
    assert session.engine.account.qty(SYMBOL) == parse_money(ORDER_QTY)
    assert session.processed == 5


def test_a_matured_arrival_dispatches_on_the_frontier_without_waiting_for_a_frame(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other half of the horizon: it is the frontier, not the last released event.

    The test above pins that a scheduled arrival may not *lead* the released market data.
    This one pins that it may not *lag* it either, and the two are one bound read in opposite
    directions -- which is why taking the horizon from `records[-1]` passes the test above and
    is still wrong.

    Here the order rests past its latency on a symbol that goes quiet. The sources keep
    reporting -- they are healthy, they simply have nothing new to deliver, which on a
    real symbol between prints is the ordinary case rather than an edge one -- so the release
    watermark advances past the arrival's instant while `release_records` returns nothing at
    all. The session is complete through that instant, so the arrival is due; the horizon has
    to come from the watermark to know that, because there is no released event to read it
    off. Taken from the last record instead, an empty release yields no horizon, the loop
    breaks at once, and the order sits in the queue until the next frame happens to arrive.

    On a quiet symbol that is a real wait, and it is invisible: the order is not lost, so
    nothing raises and the fill still eventually happens at a price the market moved to in the
    meantime, which is the class of divergence the parity report exists to catch and would
    here report against the fill model.
    """
    clock = _Clock(BAR_CLOSE_MS + 500)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    strategy = BuyOnFirstBar()
    session = paper_session(tmp_path, strategy)
    session.engine.start()
    arrival_ms = BAR_CLOSE_MS + SUBMIT_LATENCY_MS

    report_every_source_complete(session, BAR_CLOSE_MS)
    session._offer(trade_event(BAR_OPEN_MS + 30_000, 0), BAR_OPEN_MS + 30_020)
    session._offer(bar_close_event(BAR_CLOSE_MS, 0), clock.now_ms)
    session._drain_to_engine()

    assert strategy.submitted
    assert session.engine.runtime.now_ms == BAR_CLOSE_MS
    assert session.engine.queue.next_key is not None
    assert session.engine.queue.next_key[:2] == (arrival_ms, int(EventKind.ORDER_ARRIVAL))
    processed_before = session.processed

    # Not one frame offered. The pollers report the session complete past the arrival, the
    # buffer holds nothing, and so this release returns an empty list while still moving the
    # watermark -- which is precisely the state the horizon has to be derived from.
    clock.now_ms = BAR_CLOSE_MS + 1_500
    report_every_source_complete(session, BAR_CLOSE_MS + 300)
    assert session.buffer.held == 0
    session._drain_to_engine()

    assert session.buffer.watermark_ms == BAR_CLOSE_MS + 300 > arrival_ms
    assert session.engine.runtime.now_ms == arrival_ms, (
        "the arrival was due at the frontier and nothing released to carry it"
    )
    assert session.processed == processed_before + 1
    assert len(session.engine.queue) == 0


def test_every_slow_source_the_feed_reports_is_declared_on_the_sessions_buffer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The declaration list and the reporting list live in two modules and must agree.

    `LiveFeed._report_complete` runs on a poller's own task, outside the `try` that turns a
    failed poll into a recorded status -- so a source the buffer never heard of raises
    `KeyError` there and either kills that poller for the rest of the session or, on the
    socket path, is misreported as an unparseable frame. Either way the dataset goes quietly
    offline, which is the failure mode this whole file exists to make loud.

    `bookTicker` is the negative case on purpose: it is streamed rather than polled, arrives
    within tens of milliseconds of its timestamp, and is deliberately not declared.
    """
    monkeypatch.setattr(live_session, "_now_ms", _Clock(BAR_CLOSE_MS))
    session = paper_session(tmp_path, BuyOnFirstBar())

    assert set(LiveFeed.SLOW_SOURCES) == {"klines", "funding", "markPrice", "depth20"}
    for source, stale_after_ms in LiveFeed.SLOW_SOURCES.items():
        assert stale_after_ms > 0
        session._complete_through(source, BAR_CLOSE_MS)

    with pytest.raises(KeyError, match="expect"):
        session._complete_through("bookTicker", BAR_CLOSE_MS)


def test_a_book_ticker_quote_does_not_stand_in_for_the_ladder_a_market_order_walks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two payload shapes ride one priority, and applying either as the other hides a cost.

    Spec 6.2 fixes the nine-kind table, so a live `bookTicker` frame cannot be given a
    priority of its own and rides `BOOK_UPDATE` beside `depth20` -- the payload's type is the
    only thing that tells them apart, and a session receives both, interleaved, within
    milliseconds of each other. A quote is the best bid and the best ask; a ladder is twenty
    levels. Treating the quote as a ladder installs a book exactly one level deep, and spec
    6.4's walk then has nothing left to walk.

    The numbers are the argument. The published ladder rests 0.005 at 60 000 and 0.005 at
    60 010, so the 0.01 buy below consumes two levels and pays 60 005; the quote that arrives
    a second later advertises 0.5 at the same 60 000 touch, so a run that mistook it for a
    book would fill the whole order at 60 000, walk one level, and report a fill 5.05 per
    unit better than the market it was actually taken from -- no slippage at all on a size
    that moved the book two levels, which is precisely the flattering fiction the tier
    exists to refuse.

    So a quote replaces the touch and leaves the ladder standing: `top_of_book` becomes the
    quote, `ladder` is still the snapshot, and the fill is the two-level average.
    """
    clock = _Clock(BAR_CLOSE_MS + 500)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    strategy = BuyOnFirstBar()
    session = paper_session(tmp_path, strategy, fill_tier=FillTier.BOOK_WALK)
    session.engine.start()

    ladder = depth_event(BAR_CLOSE_MS - 2_000, 0)
    quote = book_ticker_event(BAR_CLOSE_MS - 1_000, 0)
    report_every_source_complete(session, BAR_CLOSE_MS)
    session._offer(ladder, ladder.ts_ms + 400)
    session._offer(quote, quote.ts_ms + 20)
    session._offer(bar_close_event(BAR_CLOSE_MS, 0), clock.now_ms)
    session._drain_to_engine()

    assert strategy.submitted, "the bar close must have reached on_bar"
    book = session.engine.market
    assert book.top_of_book(SYMBOL, BAR_CLOSE_MS) is quote.payload
    assert book.ladder(SYMBOL, BAR_CLOSE_MS) is ladder.payload

    # Market data reaches past the arrival's instant, and the order is priced there against
    # whatever the two frames above left in force.
    later_ms = BAR_CLOSE_MS + 200
    session._offer(trade_event(later_ms, 1), later_ms + 20)
    clock.now_ms = BAR_CLOSE_MS + 1_500
    report_every_source_complete(session, BAR_CLOSE_MS + 1_000)
    session._drain_to_engine()

    assert session.engine.counts["fills"] == 1
    assert session.engine.orders["o1"].filled_price == parse_money("60005")
    filled = [entry for entry in session.engine.runtime.events if entry.kind == "FILL"]
    assert [entry.payload["levels_walked"] for entry in filled] == [2]
