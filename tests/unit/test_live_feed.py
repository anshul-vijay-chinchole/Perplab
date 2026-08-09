"""Binance frames in, engine events out (`live.feed`, spec 6.1).

The paper session runs the *same* `BacktestEngine` a backtest runs, so everything this
module produces has to be indistinguishable from a replayed row. That makes the failures
here quiet by nature: a mark bar that is really a point sample, a depth stream three
observations fresher than any lake, an aggressor flag transcribed backwards, a bar step fed
short. None of them raise, all of them change the fill model's answer, and the shadow
backtest of spec 6.7 would report the difference as a fill-model divergence rather than as
the feed bug it is.

Every test below pins one of those, plus the four regressions a real testnet session found:
the first kline page must be seeded whole (not row by row), a repeated completion point is
still a liveness signal, and the bucketed sources report a watermark that stops just short
of the bucket still accumulating.

No test touches a socket or the network. Frames go in through the handlers the socket would
have called; pollers are driven with a scripted `FakeRestClient` and a pre-armed stop event,
so the loops run an exact number of iterations and never wait on a timer.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest

from perplab.core.types import Bar, CollectorEventKind, DepthSnapshot
from perplab.data.collector import DEPTH_BUCKET_MS as LAKE_DEPTH_BUCKET_MS
from perplab.data.query import TIMEFRAMES
from perplab.engine.clock import Event, EventKind
from perplab.engine.feed import BarStep, FundingPoint, MarkBar
from perplab.engine.reorder import ReorderBuffer, ReorderOverflow
from perplab.engine.ticks import TopOfBook, TradePrint
from perplab.live import feed as live_feed
from perplab.live.feed import (
    DEPTH_BUCKET_MS,
    FUNDING_PUBLICATION_GRACE_MS,
    MARK_BAR_MS,
    PRODUCTION,
    TESTNET,
    LiveFeed,
    MarkAggregator,
)
from perplab.live.session import STEP_INTERVAL_S

SYMBOL = "BTCUSDT"
OTHER = "ETHUSDT"

MINUTE = MARK_BAR_MS
SECOND = DEPTH_BUCKET_MS
FUNDING_INTERVAL = 8 * 60 * 60 * 1_000
DRAIN_MS = int(STEP_INTERVAL_S * 1_000)
"""How often `PaperSession` drains the reorder buffer, in milliseconds.

Imported rather than written down because `Dispatched` below is only a model of the session
if it drains at the session's own cadence: a test that released only when a frame happened to
arrive would never move the wall-clock watermark between two frames, which is precisely the
window in which the defects here drop an event."""

T0 = 1_764_000_000_000
"""An instant that is exactly on a minute, a second and an eight-hour funding boundary.

1 764 000 000 000 / 60 000 = 29 400 000 and / 28 800 000 = 61 250, both whole, so every
bucket boundary in this file is `T0 + k * MINUTE` or `T0 + k * SECOND` with no rounding to
reason about. It is also in the past, which matters for `_poll_funding`: that poller clamps
its watermark with `min(now, horizon)`, and a future timestamp would make the clamp read the
real wall clock and the assertion non-deterministic.
"""


def at_scale(whole: int, fraction: str) -> int:
    """The scaled-int64 form of `whole.fraction`, derived here rather than looked up.

    Spec 3.1 stores market data as int64 scaled by 10^8. Calling `money.to_scaled` to build
    the expected value would assert only that the parser agrees with itself, so the scaling
    is spelled out: the whole part is multiplied by 10^8 and the fraction is right-padded to
    eight digits. `at_scale(3, "271")` is 3 * 10^8 + 27 100 000 = 327 100 000.
    """
    if len(fraction) > 8:
        raise ValueError(f"{fraction!r} is finer than the 10^8 storage scale")
    return whole * 10**8 + int(fraction.ljust(8, "0"))


# ----------------------------------------------------------------------------- test doubles


class Capture:
    """Everything the feed hands out, in the order it handed it out.

    Stands in for `PaperSession`, which offers each event to a reorder buffer. Recording
    rather than acting, because every property under test here is about *what* was emitted
    and *when* relative to everything else.
    """

    def __init__(self) -> None:
        self.events: list[tuple[Event, int]] = []
        self.status: list[tuple[CollectorEventKind, str, str, int]] = []
        self.complete: list[tuple[str, int]] = []

    def on_event(self, event: Event, recv_ms: int) -> None:
        self.events.append((event, recv_ms))

    def on_status(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        self.status.append((kind, stream, detail, downtime_ms))

    def on_complete_through(self, source: str, ts_ms: int) -> None:
        self.complete.append((source, ts_ms))

    def payloads(self, kind: EventKind) -> list[Any]:
        return [event.payload for event, _ in self.events if event.kind is kind]

    def datasets(self, kind: EventKind) -> list[str]:
        return [event.dataset_id for event, _ in self.events if event.kind is kind]


class Dispatched(Capture):
    """`Capture` plus the reorder buffer and drain cadence `PaperSession` puts behind it.

    The watermark tests assert what the feed *reports*; these assert what the engine would
    actually have received, which is what a dropped ladder or a dropped settlement costs.
    Wired the way `PaperSession.__init__` wires it -- every `SLOW_SOURCES` entry declared with
    its own staleness bound, each event offered as it arrives, `release` called every
    `DRAIN_MS` -- with one difference: the wall clock is passed in rather than read, so the
    whole trace is derivable from the frame timestamps in the test.

    `advance` reports every source *except* `under_test` complete through the instant it
    reaches. A declared source that has never reported holds the cutoff at its own staleness
    bound (`ReorderBuffer.release_records`), so a test that skipped that would be measuring
    that bound instead of the source it means to measure.
    """

    def __init__(self, *, under_test: str, wall_ms: int, window_ms: int = 250) -> None:
        super().__init__()
        self.under_test = under_test
        self.wall_ms = wall_ms
        self.buffer = ReorderBuffer(window_ms=window_ms)
        for source, stale_after_ms in LiveFeed.SLOW_SOURCES.items():
            self.buffer.expect(source, stale_after_ms=stale_after_ms)
        self.dispatched: list[Event] = []

    def on_event(self, event: Event, recv_ms: int) -> None:
        super().on_event(event, recv_ms)
        self.buffer.offer(event, recv_ms)

    def on_complete_through(self, source: str, ts_ms: int) -> None:
        super().on_complete_through(source, ts_ms)
        self.buffer.complete_through(source, ts_ms, wall_now_ms=self.wall_ms)

    def advance(self, to_ms: int) -> None:
        """Run the session's drain loop up to `to_ms`."""
        while self.wall_ms < to_ms:
            self.wall_ms = min(to_ms, self.wall_ms + DRAIN_MS)
            for source in LiveFeed.SLOW_SOURCES:
                if source != self.under_test:
                    self.buffer.complete_through(
                        source, self.wall_ms, wall_now_ms=self.wall_ms
                    )
            self.dispatched.extend(self.buffer.release(self.wall_ms))

    def marks(self) -> list[tuple[str, int]]:
        """Every mark bar the engine received, as (symbol, close time)."""
        return [
            (event.payload.symbol, event.payload.close_time)
            for event in self.dispatched
            if event.kind is EventKind.MARK_PRICE_UPDATE
        ]


class RefusingCapture(Capture):
    """A sink that raises on every event, the way a full reorder buffer does.

    `ReorderBuffer.offer` raises `ReorderOverflow` once it holds `max_held` events, and
    `PaperSession._offer` does not catch it -- so this is what a poller's emit call really
    looks like when the live loop has stopped releasing.
    """

    def on_event(self, event: Event, recv_ms: int) -> None:
        super().on_event(event, recv_ms)
        raise ReorderOverflow("the reorder buffer already holds 100000 events")


class Clock:
    """A wall clock the test moves by hand, for `live.feed._now_ms`.

    The staleness rules in `_report_complete` and `_funding_horizon` are measured against the
    wall clock rather than against event timestamps -- deliberately, because a minute-cadence
    source is legitimately a minute behind and still healthy -- so pinning them needs a clock
    a test owns.
    """

    def __init__(self, now_ms: int) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


class FakeRestClient:
    """A scripted stand-in for `exchange.rest.PublicRestClient`.

    Each method is a coroutine serving the next scripted response for its symbol, so a poll
    loop runs offline and deterministically. `stop` is set once `stop_after` calls have been
    served, which is what makes `while not stop.is_set()` execute an exact number of
    iterations: `_sleep_or_stop` returns immediately on an already-set event, so nothing here
    waits on a real timer.
    """

    def __init__(
        self,
        stop: asyncio.Event,
        *,
        klines: dict[str, list[list[list[Any]]]] | None = None,
        premium_index: dict[str, list[dict[str, Any]]] | None = None,
        agg_trades: dict[str, list[list[dict[str, Any]]]] | None = None,
        funding_rate: dict[str, list[list[dict[str, Any]]]] | None = None,
        stop_after: int | None = None,
    ) -> None:
        self._stop = stop
        self._scripts: dict[str, dict[str, list[Any]]] = {
            "klines": dict(klines or {}),
            "premium_index": dict(premium_index or {}),
            "agg_trades": dict(agg_trades or {}),
            "funding_rate": dict(funding_rate or {}),
        }
        scripted = sum(
            len(responses)
            for method in self._scripts.values()
            for responses in method.values()
        )
        self.stop_after = scripted if stop_after is None else stop_after
        self.calls: list[tuple[str, str]] = []
        self.from_ids: list[int | None] = []

    async def klines(
        self, symbol: str, *, interval: str = "1m", limit: int = 3
    ) -> list[list[Any]]:
        return self._serve("klines", symbol)

    async def premium_index(self, symbol: str) -> dict[str, Any]:
        served = self._serve("premium_index", symbol)
        return served if isinstance(served, dict) else {}

    async def agg_trades(
        self, symbol: str, *, from_id: int | None = None, limit: int = 1000
    ) -> list[dict[str, Any]]:
        self.from_ids.append(from_id)
        return self._serve("agg_trades", symbol)

    async def funding_rate(self, symbol: str, *, limit: int = 2) -> list[dict[str, Any]]:
        return self._serve("funding_rate", symbol)

    def _serve(self, method: str, symbol: str) -> Any:
        served = sum(1 for name, sym in self.calls if name == method and sym == symbol)
        self.calls.append((method, symbol))
        if len(self.calls) >= self.stop_after:
            self._stop.set()
        script = self._scripts[method].get(symbol) or []
        if not script:
            return []
        # A loop that ticks once more than the test scripted sees the endpoint repeat itself,
        # which is what a real one does between updates. An IndexError here would replace the
        # failure under test with an unrelated one.
        return script[min(served, len(script) - 1)]


def make_feed(
    *symbols: str,
    venue: live_feed.Venue = TESTNET,
    capture: Capture | None = None,
) -> tuple[LiveFeed, Capture]:
    sink = Capture() if capture is None else capture
    feed = LiveFeed(
        symbols or (SYMBOL,),
        venue=venue,
        on_event=sink.on_event,
        on_status=sink.on_status,
        on_complete_through=sink.on_complete_through,
    )
    return feed, sink


# ------------------------------------------------------------------------------ real frames


def book_ticker_frame() -> dict[str, Any]:
    """A real `<symbol>@bookTicker` frame from the USD-M futures stream.

    `T` is the exchange's transaction time and `E` the event push time; they differ by a
    couple of milliseconds on a live socket, which is why the feed prefers `T`.
    """
    return {
        "e": "bookTicker",
        "u": 400_900_217,
        "E": T0 + 123,
        "T": T0 + 121,
        "s": "BTCUSDT",
        "b": "60250.10",
        "B": "3.271",
        "a": "60250.20",
        "A": "1.884",
    }


def depth_frame(
    ts_ms: int, *, update_id: int, best_bid: str = "60250.10", symbol: str = SYMBOL
) -> dict[str, Any]:
    """A real `<symbol>@depth20@100ms` frame. Binance labels the partial book `depthUpdate`.

    `U`/`u`/`pu` are the first, last and previous-stream update ids; only `u` is read, as the
    snapshot's `last_update_id`.
    """
    return {
        "e": "depthUpdate",
        "E": ts_ms + 2,
        "T": ts_ms,
        "s": symbol,
        "U": update_id - 41,
        "u": update_id,
        "pu": update_id - 42,
        "b": [[best_bid, "3.271"], ["60249.90", "12.5"]],
        "a": [["60250.20", "1.884"], ["60250.80", "0.4"]],
    }


def offer_depth(
    feed: LiveFeed, ts_ms: int, *, update_id: int, symbol: str = SYMBOL
) -> None:
    """Hand one `depth20` frame to the feed the way the socket would have.

    Arrival is three milliseconds after the frame's own timestamp, which is what a healthy
    socket looks like and keeps every `recv_ms` in this file derivable from its `ts_ms`.
    """
    feed._on_message(
        f"{symbol.lower()}@depth20@100ms",
        depth_frame(ts_ms, update_id=update_id, symbol=symbol),
        ts_ms + 3,
    )


def agg_trade_frame(
    ts_ms: int, *, agg_id: int, price: str = "60250.15", is_buyer_maker: bool = True
) -> dict[str, Any]:
    """A real `<symbol>@aggTrade` frame. `m` is the aggressor flag (spec 6.4)."""
    return {
        "e": "aggTrade",
        "E": ts_ms + 5,
        "s": "BTCUSDT",
        "a": agg_id,
        "p": price,
        "q": "0.037",
        "f": 1_281_000,
        "l": 1_281_004,
        "T": ts_ms,
        "m": is_buyer_maker,
    }


def mark_frame(
    ts_ms: int, *, price: str, next_funding_ms: int, symbol: str = SYMBOL
) -> dict[str, Any]:
    """A real `<symbol>@markPrice@1s` frame. On this stream `T` is the *next* funding time."""
    return {
        "e": "markPriceUpdate",
        "E": ts_ms,
        "s": symbol,
        "p": price,
        "i": "60249.80000000",
        "P": "60250.00000000",
        "r": "0.00010000",
        "T": next_funding_ms,
    }


def offer_mark(
    feed: LiveFeed,
    ts_ms: int,
    *,
    price: str,
    next_funding_ms: int,
    symbol: str = SYMBOL,
) -> None:
    """Hand one `markPrice@1s` frame to the feed the way the socket would have."""
    feed._on_message(
        f"{symbol.lower()}@markPrice@1s",
        mark_frame(ts_ms, price=price, next_funding_ms=next_funding_ms, symbol=symbol),
        ts_ms + 2,
    )


def kline_row(open_time: int, *, close: str = "60250.40") -> list[Any]:
    """One row of `GET /fapi/v1/klines`, in Binance's positional order.

    Positions the feed reads: 0 open time, 1-4 OHLC, 5 volume, 6 close time, 7 quote volume,
    8 trade count. The trailing three (taker buy base, taker buy quote, and Binance's unused
    "ignore" column) are present because a real row has them and a positional parser must not
    care.
    """
    return [
        open_time,
        "60250.00",
        "60251.50",
        "60249.25",
        close,
        "12.345",
        open_time + MINUTE - 1,
        "743790.12",
        308,
        "6.100",
        "367450.55",
        "0",
    ]


def funding_row(funding_time: int, *, rate: str) -> dict[str, Any]:
    """One row of `GET /fapi/v1/fundingRate`."""
    return {
        "symbol": "BTCUSDT",
        "fundingTime": funding_time,
        "fundingRate": rate,
        "markPrice": "60250.10000000",
    }


def premium_index_payload(
    ts_ms: int, *, price: str, next_funding_ms: int
) -> dict[str, Any]:
    """One `GET /fapi/v1/premiumIndex` payload, the mark source where the stream is silent."""
    return {
        "symbol": "BTCUSDT",
        "markPrice": price,
        "indexPrice": "60249.80000000",
        "estimatedSettlePrice": "60250.00000000",
        "lastFundingRate": "0.00010000",
        "interestRate": "0.00010000",
        "nextFundingTime": next_funding_ms,
        "time": ts_ms,
    }


# ------------------------------------------------------------------------- MarkAggregator


class TestMarkAggregator:
    """One minute of 1 Hz samples becomes one bar with a real traversed range (spec 3.4)."""

    def test_one_minute_of_samples_becomes_one_bar_ending_at_the_minutes_last_millisecond(
        self,
    ) -> None:
        """Scripted series, all inside the minute that opens at T0:

            T0 +     0 ms -> 100
            T0 + 1 000 ms -> 130
            T0 + 2 000 ms ->  90
            T0 + 3 000 ms -> 110

        High and low are the running extremes of those four samples, so 130 and 90, and
        `close` is the last of them, 110 -- not an extreme, and not the next minute's sample.
        The minute opens at T0 and covers 60 000 ms, so its last millisecond, and therefore
        the bar's `close_time`, is T0 + 59 999.

        Nothing is published until a sample from the following minute proves the minute
        ended, which is what makes a partially-formed bar unreachable -- the same guarantee
        spec 6.2 makes about klines.
        """
        aggregator = MarkAggregator(SYMBOL)

        opened = [aggregator.offer(T0 + offset, price) for offset, price in
                  ((0, 100), (1_000, 130), (2_000, 90), (3_000, 110))]
        finished = aggregator.offer(T0 + MINUTE, 105)

        assert opened == [None, None, None, None]
        assert finished == MarkBar(
            symbol=SYMBOL, close_time=T0 + MINUTE - 1, high=130, low=90, close=110
        )

    def test_a_minute_with_no_samples_produces_no_bar_at_all(self) -> None:
        """Series: one sample at T0, silence for two whole minutes, one sample at T0 + 3 min.

        Spec 3.4 is last-observation-carried-forward and forbids inventing a mark, so the two
        silent minutes must read as a gap rather than as flat minutes nobody observed. Two
        bars come out of the four minutes touched: the minute at T0 (close_time
        T0 + 59 999) and, on flush, the minute at T0 + 3 min (close_time T0 + 239 999). The
        close times T0 + 119 999 and T0 + 179 999 must not appear at all -- a filler bar
        there would be a mark price the exchange never published, and the liquidation check
        would probe a range that was never traversed.
        """
        aggregator = MarkAggregator(SYMBOL)

        published = [
            aggregator.offer(T0, 100),
            aggregator.offer(T0 + 3 * MINUTE, 140),
            aggregator.flush(),
        ]

        bars = [bar for bar in published if bar is not None]
        assert [bar.close_time for bar in bars] == [
            T0 + MINUTE - 1,
            T0 + 4 * MINUTE - 1,
        ]

    def test_a_sample_for_an_already_published_minute_is_dropped_not_folded_back_in(
        self,
    ) -> None:
        """Series: 100 at T0, then 105 at T0 + 60 000 which publishes the minute at T0, then
        a stray 999 stamped back inside that minute, then 107 at T0 + 61 000.

        The minute at T0 has been dispatched and the engine has already probed its range, so
        re-opening it would retroactively change a bar the session has acted on. The stray
        sample must therefore leave both bars alone. The minute at T0 stays
        high = low = close = 100, and the minute at T0 + 60 000 -- flushed at the end -- must
        report high 107 / low 105 / close 107 from its own two samples. A 999 anywhere in
        either bar means the out-of-order sample was folded in.
        """
        aggregator = MarkAggregator(SYMBOL)

        aggregator.offer(T0, 100)
        first = aggregator.offer(T0 + MINUTE, 105)
        stray = aggregator.offer(T0 + 30_000, 999)
        aggregator.offer(T0 + MINUTE + 1_000, 107)
        second = aggregator.flush()

        assert stray is None
        assert first == MarkBar(
            symbol=SYMBOL, close_time=T0 + MINUTE - 1, high=100, low=100, close=100
        )
        assert second == MarkBar(
            symbol=SYMBOL, close_time=T0 + 2 * MINUTE - 1, high=107, low=105, close=107
        )

    def test_flush_publishes_the_part_minute_the_session_ended_inside(self) -> None:
        """Series: 100 at T0 and 120 at T0 + 1 000, then the session stops.

        The bar covers two seconds of a sixty-second minute and that is the honest encoding:
        nothing was observed after the session ended, so high 120 / low 100 / close 120 is
        the whole of what was seen. It is still stamped at the minute's own end,
        T0 + 59 999, because that is the key a backtest's row for this minute carries and the
        two have to be the same event.

        A second flush yields nothing -- there is no longer a minute in progress -- and so
        does a flush before any sample, which is what stops session shutdown emitting a bar
        for a symbol that never ticked.
        """
        assert MarkAggregator(SYMBOL).flush() is None

        aggregator = MarkAggregator(SYMBOL)
        aggregator.offer(T0, 100)
        aggregator.offer(T0 + 1_000, 120)

        assert aggregator.flush() == MarkBar(
            symbol=SYMBOL, close_time=T0 + MINUTE - 1, high=120, low=100, close=120
        )
        assert aggregator.flush() is None


# --------------------------------------------------------------------------- frame parsing


class TestFrameParsing:
    """A live frame must become the exact object a backtest replays, field for field."""

    def test_a_book_ticker_frame_becomes_a_top_of_book_with_scaled_int64_fields(
        self,
    ) -> None:
        """The frame quotes bid 60250.10 x 3.271 and ask 60250.20 x 1.884 as decimal strings.

        Spec 3.1 stores market data scaled by 10^8, so those four become 6 025 010 000 000,
        327 100 000, 6 025 020 000 000 and 188 400 000. Parsing through `float` -- the
        obvious shortcut for a string like "3.271" -- would not land on any of them exactly.

        The event is a `BOOK_UPDATE` under `bookTicker:BTCUSDT`: spec 6.2's live rule is one
        sequence space per stream per symbol, because the exchange's own update ids are
        allocated per symbol and would tie across two of them.
        """
        feed, capture = make_feed(SYMBOL)

        feed._on_message("btcusdt@bookTicker", book_ticker_frame(), T0 + 130)

        assert capture.payloads(EventKind.BOOK_UPDATE) == [
            TopOfBook(
                symbol=SYMBOL,
                ts_ms=T0 + 121,
                bid_px=at_scale(60_250, "10"),
                bid_qty=at_scale(3, "271"),
                ask_px=at_scale(60_250, "20"),
                ask_qty=at_scale(1, "884"),
            )
        ]
        assert capture.datasets(EventKind.BOOK_UPDATE) == ["bookTicker:BTCUSDT"]
        assert feed.counts["book_ticker"] == 1

    def test_a_book_ticker_is_stamped_with_transaction_time_and_falls_back_to_event_time(
        self,
    ) -> None:
        """The frame carries `T` = T0 + 121 (the exchange's transaction time) and `E` =
        T0 + 123 (when it was pushed). Spec 6.2 orders on when a thing happened, so the
        event must be stamped T0 + 121.

        Older futures frames omit `T` entirely; those are stamped with `E`, which is the only
        exchange clock they carry. Recording push time when transaction time is available
        would put the event two milliseconds late in the total order and, at the millisecond
        boundaries where it matters, on the wrong side of a bar close.
        """
        feed, capture = make_feed(SYMBOL)
        without_transaction_time = book_ticker_frame()
        del without_transaction_time["T"]

        feed._on_message("btcusdt@bookTicker", book_ticker_frame(), T0 + 130)
        feed._on_message("btcusdt@bookTicker", without_transaction_time, T0 + 130)

        assert [top.ts_ms for top in capture.payloads(EventKind.BOOK_UPDATE)] == [
            T0 + 121,
            T0 + 123,
        ]

    def test_a_depth_update_frame_becomes_a_depth_snapshot_with_scaled_parallel_ladders(
        self,
    ) -> None:
        """The frame's two bid levels are 60250.10 x 3.271 and 60249.90 x 12.5, and its two
        ask levels 60250.20 x 1.884 and 60250.80 x 0.4, best-first.

        `core.types.DepthSnapshot` stores them as parallel best-first tuples at the 10^8
        scale, so the bid prices are (6 025 010 000 000, 6 024 990 000 000) and the ask sizes
        (188 400 000, 40 000 000). Same type the collector writes and `ctx.book()` returns:
        one type end to end, so nothing downstream can tell a live ladder from a replayed one.

        The snapshot is published when the next second opens (see the downsampling tests), so
        the second frame here is the trigger and not itself the subject. `recv_ms` belongs to
        the frame the snapshot was built from, not to the frame that flushed it -- it is the
        feed-lag measurement the tape records.
        """
        feed, capture = make_feed(SYMBOL)

        offer_depth(feed, T0 + 240, update_id=390_497_878)
        offer_depth(feed, T0 + SECOND, update_id=390_498_100)

        assert capture.payloads(EventKind.BOOK_UPDATE) == [
            DepthSnapshot(
                symbol=SYMBOL,
                ts_ms=T0 + 240,
                recv_ms=T0 + 243,
                last_update_id=390_497_878,
                bid_px=(at_scale(60_250, "10"), at_scale(60_249, "90")),
                bid_qty=(at_scale(3, "271"), at_scale(12, "5")),
                ask_px=(at_scale(60_250, "20"), at_scale(60_250, "80")),
                ask_qty=(at_scale(1, "884"), at_scale(0, "4")),
            )
        ]
        assert capture.datasets(EventKind.BOOK_UPDATE) == ["depth20:BTCUSDT"]

    def test_an_agg_trade_frame_becomes_a_trade_print_with_the_aggressor_flag_not_inverted(
        self,
    ) -> None:
        """Two frames, `m` true then `m` false, and the flag must survive unchanged.

        `is_buyer_maker=True` means the *buyer* was the maker, so the trade was
        sell-aggressive and consumed bid-side queue -- which is exactly what
        `TradePrint.consumes_bids` returns. Inverting the flag reverses every
        queue-consumption decision in the fill model (spec 6.4) without raising anything, so
        both directions are asserted: a test that only checked the true case would pass
        against `not bool(data["m"])` if the second frame were absent.

        Price 60250.15 and quantity 0.037 scale to 6 025 015 000 000 and 3 700 000 at 10^8.
        """
        feed, capture = make_feed(SYMBOL)

        feed._on_message(
            "btcusdt@aggTrade",
            agg_trade_frame(T0 + 300, agg_id=5_933_014, is_buyer_maker=True),
            T0 + 306,
        )
        feed._on_message(
            "btcusdt@aggTrade",
            agg_trade_frame(T0 + 400, agg_id=5_933_015, is_buyer_maker=False),
            T0 + 406,
        )

        prints = capture.payloads(EventKind.TRADE)
        assert prints[0] == TradePrint(
            symbol=SYMBOL,
            ts_ms=T0 + 300,
            price_scaled=at_scale(60_250, "15"),
            qty_scaled=at_scale(0, "037"),
            is_buyer_maker=True,
            agg_id=5_933_014,
        )
        assert [(p.is_buyer_maker, p.consumes_bids, p.consumes_asks) for p in prints] == [
            (True, True, False),
            (False, False, True),
        ]
        assert capture.datasets(EventKind.TRADE) == [
            "aggTrades:BTCUSDT",
            "aggTrades:BTCUSDT",
        ]

    def test_a_malformed_frame_is_reported_as_data_rather_than_taking_the_socket_down(
        self,
    ) -> None:
        """A `bookTicker` frame with no `b` field, which is a `KeyError` in the handler.

        `_on_message` runs on the socket read path. An exception escaping it stops us
        draining the socket, Binance's send buffer fills, and the connection is dropped --
        one bad message turned into a gap on every stream. It is reported as a DISCONNECT
        against the stream instead, so the gap it does cause is explainable from the tape
        rather than only from a log nobody kept.
        """
        feed, capture = make_feed(SYMBOL)
        frame = book_ticker_frame()
        del frame["b"]

        feed._on_message("btcusdt@bookTicker", frame, T0 + 130)

        assert capture.events == []
        assert [kind for kind, _, _, _ in capture.status] == [
            CollectorEventKind.DISCONNECT
        ]
        assert "unparseable payload" in capture.status[0][2]


# ------------------------------------------------------------------------ depth downsampling


class TestDepthDownsampling:
    """Live depth arrives at 100 ms; the lake holds 1 s, and the lake is what a backtest
    reads."""

    def test_only_the_last_depth_snapshot_of_each_second_is_emitted(self) -> None:
        """Four frames: T0 + 100, T0 + 300 and T0 + 900 all fall in the second opening at T0,
        and T0 + 1 000 opens the next one.

        Exactly one snapshot is published for the first second and it is the one stamped
        T0 + 900 -- last-observation-carried-forward, matching `collector.DEPTH_BUCKET_MS` and
        therefore matching the row a backtest of this period would read. First-of-bucket would
        be a different series; all four would give the paper session a book three observations
        fresher than any backtest this platform can run, which makes the session
        unrepresentative of the thing it exists to validate.

        The frame at T0 + 1 000 is still accumulating and must not appear.
        """
        feed, capture = make_feed(SYMBOL)

        for offset, update_id in ((100, 1_001), (300, 1_002), (900, 1_003), (SECOND, 1_004)):
            offer_depth(feed, T0 + offset, update_id=update_id)

        snapshots = capture.payloads(EventKind.BOOK_UPDATE)
        assert [(s.ts_ms, s.last_update_id) for s in snapshots] == [(T0 + 900, 1_003)]
        assert feed.counts["depth"] == 1

    def test_the_feeds_depth_bucket_is_the_one_the_lake_stores(self) -> None:
        """Two constants that must not drift apart.

        `live.feed.DEPTH_BUCKET_MS` decides the paper session's book resolution and
        `collector.DEPTH_BUCKET_MS` decides the lake's. If they ever differ, the shadow
        backtest of spec 6.7 reads a different book from the one the session traded and
        reports the difference as a fill-model divergence.
        """
        assert DEPTH_BUCKET_MS == LAKE_DEPTH_BUCKET_MS

    def test_the_feeds_mark_window_is_the_one_a_backtest_reads(self) -> None:
        """`markPriceKlines` is a 1 m series (`engine.feed` loads it at that interval and
        nothing else), so the live aggregation window has to be the same 60 000 ms or the
        equity series would be sampled at a different cadence from every backtest."""
        assert MARK_BAR_MS == TIMEFRAMES["1m"]

    def test_a_depth_bucket_rollover_reports_completion_short_of_the_open_bucket(
        self,
    ) -> None:
        """Frames at T0 + 100 and T0 + 1 000, which opens the next second.

        Depth is stale by construction: the snapshot kept for the second at T0 is only
        published once the second at T0 + 1 000 opens. Opening that bucket proves every
        earlier one has been published and proves nothing about the new one, so the buffer may
        release up to the last millisecond before it -- T0 + 1 000 - 1 -- and no further.
        Reporting the new bucket's own instant instead would race past a snapshot still
        accumulating, and it would then be dropped as late.
        """
        feed, capture = make_feed(SYMBOL)

        offer_depth(feed, T0 + 100, update_id=1)
        offer_depth(feed, T0 + SECOND, update_id=2)

        assert capture.complete == [("depth20", T0 + SECOND - 1)]

    def test_one_symbols_bucket_rollover_does_not_report_past_another_symbols_held_snapshot(
        self,
    ) -> None:
        """Two symbols 800 ms out of phase, over three seconds.

        BTCUSDT's frames land at .100 of each second and ETHUSDT's at .900, which is what a
        depth20 stream measured at 29 frames per 12 s looks like -- the gap between two
        symbols is routinely larger than the 250 ms reorder window. Each symbol's snapshot
        for a second is published only when its *own* next frame opens the following one, so
        when BTCUSDT opens second 2 at T0 + 2 100, ETHUSDT is still holding its snapshot for
        second 1, stamped T0 + 1 900.

        Reporting BTCUSDT's own bucket there -- T0 + 1 999 -- would put that watermark past a
        snapshot that has not been offered yet, and `ReorderBuffer.offer` refuses anything
        stamped at or before the watermark: measured at 42% of one symbol's ladders in a
        two-symbol session and 55% in a three-symbol one. So the point reported stays at
        T0 + 999, the last instant *both* symbols have published through, until ETHUSDT rolls
        over too.

        Four rollovers happen and three reports come out of them. BTCUSDT's first says
        nothing, because ETHUSDT has not spoken yet; ETHUSDT's first carries T0 + 999;
        BTCUSDT's second carries T0 + 999 again, held down by ETHUSDT; ETHUSDT's second
        finally moves it to T0 + 1 999. Reporting the maximum instead gives four entries and
        moves to T0 + 1 999 one frame early -- which is the frame ETHUSDT's snapshot is lost
        to.
        """
        feed, capture = make_feed(SYMBOL, OTHER)

        for second in range(3):
            offer_depth(feed, T0 + second * SECOND + 100, update_id=1 + second)
            offer_depth(
                feed, T0 + second * SECOND + 900, update_id=11 + second, symbol=OTHER
            )

        assert capture.complete == [
            ("depth20", T0 + SECOND - 1),
            ("depth20", T0 + SECOND - 1),
            ("depth20", T0 + 2 * SECOND - 1),
        ]


# ----------------------------------------------------------------------------- mark frames


class TestMarkFrames:
    def test_mark_frames_reach_the_engine_as_one_bar_per_minute_not_as_point_samples(
        self,
    ) -> None:
        """Four `markPriceUpdate` frames: 60250.10 and 60260.00 inside the minute at T0, then
        60240.00 and 60245.00 inside the minute at T0 + 60 000.

        Only one `MARK_PRICE_UPDATE` reaches the sink while the session runs, carrying the
        first minute's traversed range: high 60260.00, low 60250.10, close 60260.00, stamped
        T0 + 59 999. Feeding the four samples through individually would give every bar
        high == low == close, which degenerates spec 3.4's traversed-range liquidation probe
        to a close-only test -- an error always in the paper session's favour, because it
        misses exactly the liquidations the market immediately recovered from.

        `flush` at session end publishes the part-minute in progress, so the second bar shows
        high 60245.00, low 60240.00, close 60245.00 at T0 + 119 999.
        """
        feed, capture = make_feed(SYMBOL)
        settlement = T0 + FUNDING_INTERVAL

        for offset, price in (
            (0, "60250.10"),
            (30_000, "60260.00"),
            (MINUTE, "60240.00"),
            (MINUTE + 30_000, "60245.00"),
        ):
            feed._on_message(
                "btcusdt@markPrice@1s",
                mark_frame(T0 + offset, price=price, next_funding_ms=settlement),
                T0 + offset + 2,
            )
        feed.flush()

        assert capture.payloads(EventKind.MARK_PRICE_UPDATE) == [
            MarkBar(
                symbol=SYMBOL,
                close_time=T0 + MINUTE - 1,
                high=at_scale(60_260, "00"),
                low=at_scale(60_250, "10"),
                close=at_scale(60_260, "00"),
            ),
            MarkBar(
                symbol=SYMBOL,
                close_time=T0 + 2 * MINUTE - 1,
                high=at_scale(60_245, "00"),
                low=at_scale(60_240, "00"),
                close=at_scale(60_245, "00"),
            ),
        ]
        assert capture.datasets(EventKind.MARK_PRICE_UPDATE) == [
            "markPrice:BTCUSDT",
            "markPrice:BTCUSDT",
        ]
        assert feed.counts["marks"] == 4
        assert feed.counts["mark_bars"] == 2

    def test_a_mark_sample_reports_completion_up_to_just_before_its_minute_closes(
        self,
    ) -> None:
        """One sample at T0 + 30 000, which is inside the minute opening at T0.

        Mark bars are stamped at minute ends. Every earlier one has been published, and the
        next one this source can produce is stamped T0 + 59 999 -- so the strongest sound
        claim is the instant just before it, T0 + 59 998. Nothing this source emits can ever
        land at or below that, which is what the completion point has to guarantee.

        Reporting T0 - 1 instead (the minute's *opening*) was also sound and far too weak: it
        does not move for a whole minute, so the buffer's `min()` held every trade and quote
        stamped inside that minute until the minute after it. Market data reached the engine
        in one-minute bursts, and `AutoFlatten` deadlines and the live monitor ran a minute
        behind the market.
        """
        feed, capture = make_feed(SYMBOL)

        feed._on_message(
            "btcusdt@markPrice@1s",
            mark_frame(T0 + 30_000, price="60250.10", next_funding_ms=T0 + FUNDING_INTERVAL),
            T0 + 30_002,
        )

        assert capture.complete == [("markPrice", T0 + 59_998)]

    def test_a_symbol_ticking_into_a_new_minute_does_not_report_past_another_symbols_bar(
        self,
    ) -> None:
        """Two symbols 800 ms out of phase: BTCUSDT at .100 of each second, ETHUSDT at .900.

        A mark bar cannot be emitted until a sample from the *next* minute proves the minute
        ended, so when BTCUSDT's sample at T0 + 60 100 opens minute two, ETHUSDT's bar for
        minute one -- stamped T0 + 59 999 -- has not been built yet. Reporting BTCUSDT's own
        minute there carries `markPrice` to T0 + 119 998, past that bar, and
        `ReorderBuffer.offer` then refuses it: the symbol ends the minute with no mark at
        all, which `Account._valuation_mark` values at its entry price, so its unrealised PnL
        reads exactly zero and `check_liquidations` skips it entirely.

        So the point stays at T0 + 59 998 -- one millisecond below the bar neither symbol has
        published yet -- until ETHUSDT also ticks into minute two. Three reports: BTCUSDT's
        first is withheld while ETHUSDT is silent, then two carrying T0 + 59 998, then the
        move to T0 + 119 998 once both minutes are closed.
        """
        settlement = T0 + FUNDING_INTERVAL
        feed, capture = make_feed(SYMBOL, OTHER)

        for minute in range(2):
            offer_mark(
                feed,
                T0 + minute * MINUTE + 100,
                price="60250.10",
                next_funding_ms=settlement,
            )
            offer_mark(
                feed,
                T0 + minute * MINUTE + 900,
                price="2050.10",
                next_funding_ms=settlement,
                symbol=OTHER,
            )

        assert capture.complete == [
            ("markPrice", T0 + MINUTE - 2),
            ("markPrice", T0 + MINUTE - 2),
            ("markPrice", T0 + 2 * MINUTE - 2),
        ]

    def test_both_symbols_mark_bars_reach_the_engine_when_their_samples_are_out_of_phase(
        self,
    ) -> None:
        """The same two-symbol trace, through the buffer and drain cadence a session runs.

        This is the cost of the watermark above, measured where it lands. Both symbols build
        a bar for the minute at T0, both stamped T0 + 59 999, and both have to be dispatched:
        the equity curve, the liquidation probe and the funding cashflow of *each* symbol are
        priced off its own mark, and a session that drops one reports a flat curve for it
        while the market moves.

        Every drain tick between the two frames matters, which is why the wall clock is
        stepped at the session's own `DRAIN_MS` rather than only when a frame arrives: with a
        per-symbol maximum, BTCUSDT's sample at T0 + 60 100 releases the watermark and the
        drains that follow carry it past T0 + 59 999 well before ETHUSDT's sample at
        T0 + 60 900 arrives to build its bar.

        The two bars share a timestamp and a kind, so spec 6.2's total order breaks the tie on
        `dataset_id`: `markPrice:BTCUSDT` before `markPrice:ETHUSDT`.
        """
        settlement = T0 + FUNDING_INTERVAL
        capture = Dispatched(under_test="markPrice", wall_ms=T0)
        feed, _ = make_feed(SYMBOL, OTHER, capture=capture)

        for minute in range(2):
            for symbol, offset, price in (
                (SYMBOL, 100, "60250.10"),
                (OTHER, 900, "2050.10"),
            ):
                ts_ms = T0 + minute * MINUTE + offset
                capture.advance(ts_ms)
                offer_mark(
                    feed, ts_ms, price=price, next_funding_ms=settlement, symbol=symbol
                )
        capture.advance(T0 + MINUTE + 2 * SECOND)

        assert capture.marks() == [
            (SYMBOL, T0 + MINUTE - 1),
            (OTHER, T0 + MINUTE - 1),
        ]
        assert capture.buffer.late_dropped == 0

    def test_the_next_settlement_is_observed_from_the_frame_rather_than_assumed(
        self,
    ) -> None:
        """`markPriceUpdate.T` is the next funding time, and it is the only live source of it.

        Spec 3.5 rule 3 and R17 both forbid assuming an eight-hour schedule, and without an
        observed value `AutoFlatten.before_funding_ms` -- a platform exit the operator asked
        for -- silently never fires. The frame here says T0 + 28 800 000, so that is what the
        tape's meta must carry.
        """
        feed, _ = make_feed(SYMBOL)
        settlement = T0 + FUNDING_INTERVAL

        feed._on_message(
            "btcusdt@markPrice@1s",
            mark_frame(T0, price="60250.10", next_funding_ms=settlement),
            T0 + 2,
        )

        assert feed.funding_times() == {SYMBOL: [settlement]}


# ------------------------------------------------------------------------------- bar steps


class TestBarAssembly:
    @pytest.mark.asyncio
    async def test_the_whole_first_kline_page_is_seeded_and_none_of_it_is_dispatched(
        self,
    ) -> None:
        """Two polls of a three-row page. Poll 1 returns bars opening at T0, T0 + 1 min and
        T0 + 2 min; poll 2 returns T0 + 1 min, T0 + 2 min and T0 + 3 min. The last row of each
        page is the bar still forming and is dropped as look-ahead.

        Both closed bars in the first page closed before the session existed, so **neither**
        may be dispatched -- their timestamps are minutes behind the engine's clock and the
        first `on_bar` of the session would fire on stale data. Seeding after the first *row*
        instead of after the whole page let the second one through, where the reorder buffer
        dropped it as late: the very first bar of every session was lost, and nothing
        reported it. Exactly one step therefore reaches the engine here, the bar opening at
        T0 + 2 min, which is the only close poll 2 adds.

        Its close time is one millisecond before the next minute opens, T0 + 3 min - 1, and
        the whole positional row mapping is asserted with it: swapping the volume and
        close-time columns is a one-character mistake that produces a plausible bar.
        """
        stop = asyncio.Event()
        client = FakeRestClient(
            stop,
            klines={
                SYMBOL: [
                    [kline_row(T0), kline_row(T0 + MINUTE), kline_row(T0 + 2 * MINUTE)],
                    [
                        kline_row(T0 + MINUTE),
                        kline_row(T0 + 2 * MINUTE),
                        kline_row(T0 + 3 * MINUTE),
                    ],
                ]
            },
        )
        feed, capture = make_feed(SYMBOL)

        await feed._poll_klines(client, stop)

        steps = capture.payloads(EventKind.BAR_CLOSE)
        assert [step.close_time for step in steps] == [T0 + 3 * MINUTE - 1]
        assert steps[0].bars == (
            Bar(
                symbol=SYMBOL,
                open_time=T0 + 2 * MINUTE,
                close_time=T0 + 3 * MINUTE - 1,
                open=at_scale(60_250, "00"),
                high=at_scale(60_251, "50"),
                low=at_scale(60_249, "25"),
                close=at_scale(60_250, "40"),
                volume=at_scale(12, "345"),
                quote_volume=at_scale(743_790, "12"),
                trades=308,
            ),
        )
        assert feed.counts["bars"] == 1

    @pytest.mark.asyncio
    async def test_a_bar_step_waits_until_every_symbol_has_a_bar_for_that_close_time(
        self,
    ) -> None:
        """Three polls over two symbols. Both are seeded on poll 1 with the bar opening at T0.
        On poll 2 only BTCUSDT has published the bar opening at T0 + 1 min; ETHUSDT is still
        serving the same page as before. On poll 3 ETHUSDT catches up.

        Indicators for *all* symbols must advance before *any* `on_bar` runs
        (`feed.BarStep`), so nothing may be emitted on poll 2 with one leg missing: a pairs
        strategy reading the other leg in its first `on_bar` would get a value one bar stale
        and have no way to notice. Exactly one step therefore reaches the engine, on poll 3,
        carrying both bars in the feed's own symbol order -- a per-symbol emitter would have
        produced two events, each with a single bar.
        """
        stop = asyncio.Event()
        client = FakeRestClient(
            stop,
            klines={
                SYMBOL: [
                    [kline_row(T0), kline_row(T0 + MINUTE)],
                    [kline_row(T0 + MINUTE), kline_row(T0 + 2 * MINUTE)],
                    [kline_row(T0 + MINUTE), kline_row(T0 + 2 * MINUTE)],
                ],
                OTHER: [
                    [kline_row(T0), kline_row(T0 + MINUTE)],
                    [kline_row(T0), kline_row(T0 + MINUTE)],
                    [kline_row(T0 + MINUTE), kline_row(T0 + 2 * MINUTE)],
                ],
            },
        )
        feed, capture = make_feed(SYMBOL, OTHER)

        await feed._poll_klines(client, stop)

        steps = capture.payloads(EventKind.BAR_CLOSE)
        assert len(steps) == 1
        assert steps[0].close_time == T0 + 2 * MINUTE - 1
        assert [bar.symbol for bar in steps[0].bars] == [SYMBOL, OTHER]
        assert feed.counts["bars"] == 1

    @pytest.mark.asyncio
    async def test_a_bar_step_that_can_never_complete_is_dropped_rather_than_fed_short(
        self,
    ) -> None:
        """Three polls over two symbols, with ETHUSDT missing the minute at T0 + 1 min
        entirely: on poll 3 it jumps straight to the bar opening at T0 + 2 min, which BTCUSDT
        also publishes.

        The step for T0 + 1 min can now never complete -- ETHUSDT's bar for that minute has
        been superseded and will never be served. It is dropped, not held for ever and not
        emitted with one leg, so the only step the engine sees closes at T0 + 3 min - 1 with
        both symbols present, and nothing is left waiting behind it.
        """
        stop = asyncio.Event()
        client = FakeRestClient(
            stop,
            klines={
                SYMBOL: [
                    [kline_row(T0), kline_row(T0 + MINUTE)],
                    [kline_row(T0 + MINUTE), kline_row(T0 + 2 * MINUTE)],
                    [kline_row(T0 + 2 * MINUTE), kline_row(T0 + 3 * MINUTE)],
                ],
                OTHER: [
                    [kline_row(T0), kline_row(T0 + MINUTE)],
                    [kline_row(T0), kline_row(T0 + MINUTE)],
                    [kline_row(T0 + 2 * MINUTE), kline_row(T0 + 3 * MINUTE)],
                ],
            },
        )
        feed, capture = make_feed(SYMBOL, OTHER)

        await feed._poll_klines(client, stop)

        steps = capture.payloads(EventKind.BAR_CLOSE)
        assert [step.close_time for step in steps] == [T0 + 3 * MINUTE - 1]
        assert [bar.symbol for bar in steps[0].bars] == [SYMBOL, OTHER]
        assert feed._pending_bars == {}


# ------------------------------------------------------------------------------- watermarks


class TestCompletionReporting:
    def test_an_unchanged_completion_point_is_reported_again_rather_than_deduped(
        self,
    ) -> None:
        """Three calls: the same point twice, then a point behind it.

        An unchanged watermark is still a liveness signal. Suppressing the repeat is not an
        optimisation, it is silence: a minute-cadence kline source advances its completion
        point once a minute and polls every second, so deduping meant the buffer heard
        nothing for up to sixty seconds, declared the source stalled at fifteen
        (`SLOW_SOURCES["klines"]`), stopped waiting for it, and dropped every bar close for
        the rest of the session.

        The third call carries T0, which is behind T0 + 59 999. The watermark is monotonic so
        the retained value stands -- but the report still happens, because the source has just
        proved it is alive. All three reports therefore carry T0 + 59 999.
        """
        feed, capture = make_feed(SYMBOL)

        feed._report_complete("klines", SYMBOL, T0 + MINUTE - 1)
        feed._report_complete("klines", SYMBOL, T0 + MINUTE - 1)
        feed._report_complete("klines", SYMBOL, T0)

        assert capture.complete == [
            ("klines", T0 + MINUTE - 1),
            ("klines", T0 + MINUTE - 1),
            ("klines", T0 + MINUTE - 1),
        ]

    def test_a_source_reports_the_minimum_across_its_symbols_rather_than_the_maximum(
        self,
    ) -> None:
        """Two symbols reporting different points, the second one further ahead.

        A source is complete through an instant only when *every* symbol it covers is, so the
        point the buffer is told is BTCUSDT's T0 + 999 -- not ETHUSDT's T0 + 1 999, and not
        the maximum of the two. Reporting the maximum is the whole defect: it carries the
        release watermark past an instant another symbol still owes events for, and
        `ReorderBuffer.offer` then refuses them, because it drops anything stamped at or
        before the watermark.

        The first report is silent on purpose -- see the test below.
        """
        feed, capture = make_feed(SYMBOL, OTHER)

        feed._report_complete("depth20", SYMBOL, T0 + SECOND - 1)
        feed._report_complete("depth20", OTHER, T0 + 2 * SECOND - 1)

        assert capture.complete == [("depth20", T0 + SECOND - 1)]

    def test_a_source_reports_nothing_until_every_symbol_it_covers_has_spoken_once(
        self,
    ) -> None:
        """One symbol of a two-symbol session reporting, twice, while the other stays quiet.

        There is no instant this source is complete through yet: ETHUSDT has delivered
        nothing, so it may still produce an event stamped anywhere. Reporting BTCUSDT's point
        over it would be the same false claim as reporting the maximum, and `ReorderBuffer`
        already has the right answer for a source that has said nothing at all -- hold the
        cutoff at that source's staleness bound, no further -- which is also what stops a
        symbol that never starts from freezing the session.
        """
        feed, capture = make_feed(SYMBOL, OTHER)

        feed._report_complete("markPrice", SYMBOL, T0 + MINUTE - 2)
        feed._report_complete("markPrice", SYMBOL, T0 + 2 * MINUTE - 2)

        assert capture.complete == []

    def test_a_symbol_silent_past_its_sources_bound_leaves_the_minimum_and_is_recorded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Both symbols report, then ETHUSDT goes quiet for one millisecond past the bound.

        The minimum is a new way to freeze a session if it is taken over a dead symbol: the
        cutoff would sit at ETHUSDT's last watermark for ever while BTCUSDT kept reporting,
        so the buffer would never declare `markPrice` stalled and dispatch would stop for the
        rest of the run. Past `SLOW_SOURCES["markPrice"]` -- 15 000 ms, the same bound the
        buffer applies to the source as a whole -- the quiet symbol is dropped from the
        minimum and the report becomes BTCUSDT's own point.

        Dropping it is data loss, so it is recorded: one `STALE` entry naming the dataset,
        `markPrice:ETHUSDT`, and one only. This runs on the socket read path at up to ten
        frames a second, and the status log is also the gap-explanation record, so repeating
        it would bury what it describes. `STALE` rather than `DISCONNECT` because the socket
        is fine -- and because `PaperSession` measures spec 7's disconnect trigger from
        `DISCONNECT` records, so the wrong kind here could halt a session over one quiet
        symbol.
        """
        bound_ms = LiveFeed.SLOW_SOURCES["markPrice"]
        clock = Clock(T0)
        monkeypatch.setattr(live_feed, "_now_ms", clock)
        feed, capture = make_feed(SYMBOL, OTHER)

        feed._report_complete("markPrice", SYMBOL, T0 + MINUTE - 2)
        feed._report_complete("markPrice", OTHER, T0 + MINUTE - 2)
        clock.now_ms = T0 + bound_ms + 1
        feed._report_complete("markPrice", SYMBOL, T0 + 2 * MINUTE - 2)
        feed._report_complete("markPrice", SYMBOL, T0 + 3 * MINUTE - 2)

        assert capture.complete == [
            ("markPrice", T0 + MINUTE - 2),
            ("markPrice", T0 + 2 * MINUTE - 2),
            ("markPrice", T0 + 3 * MINUTE - 2),
        ]
        assert [(kind, stream) for kind, stream, _, _ in capture.status] == [
            (CollectorEventKind.STALE, f"markPrice:{OTHER}")
        ]
        assert f"{bound_ms + 1} ms" in capture.status[0][2]

        # It comes back: the minimum is its point again, and the next stretch of silence is
        # reported afresh rather than suppressed by the first one.
        feed._report_complete("markPrice", OTHER, T0 + 2 * MINUTE - 2)
        clock.now_ms += bound_ms + 1
        feed._report_complete("markPrice", SYMBOL, T0 + 4 * MINUTE - 2)

        assert capture.complete[-2:] == [
            ("markPrice", T0 + 2 * MINUTE - 2),
            ("markPrice", T0 + 4 * MINUTE - 2),
        ]
        assert len(capture.status) == 2

    @pytest.mark.asyncio
    async def test_a_kline_poll_reports_its_completion_point_on_every_tick(self) -> None:
        """Two polls returning the identical page, which is the ordinary case: klines close
        once a minute and this poller runs at 1 Hz, so roughly fifty-nine polls in sixty
        return nothing new.

        The page's last row is the bar in progress, which closes at T0 + 2 min - 1. Klines
        produce events only at minute ends, every earlier one has been published, and the
        next cannot arrive before that open bar closes -- so the completion point is the
        instant just before it, `T0 + 2 * MINUTE - 2`.

        Reporting the open bar's *start* instead was sound and far too weak: it does not move
        for a whole minute, so the buffer held every trade, quote and mark stamped inside
        that minute until the minute after it, and market data reached the engine in
        one-minute bursts.

        Both polls must report, so two identical entries. One entry means the repeat was
        deduped, which is the failure above.
        """
        stop = asyncio.Event()
        page = [kline_row(T0), kline_row(T0 + MINUTE)]
        client = FakeRestClient(stop, klines={SYMBOL: [page, page]})
        feed, capture = make_feed(SYMBOL)

        await feed._poll_klines(client, stop)

        open_bar_close = T0 + 2 * MINUTE - 1
        assert capture.complete == [
            ("klines", open_bar_close - 1),
            ("klines", open_bar_close - 1),
        ]

    @pytest.mark.asyncio
    async def test_every_source_that_reports_a_watermark_is_declared_late_by_construction(
        self,
    ) -> None:
        """All four late sources driven once, and the set of names they report compared with
        `SLOW_SOURCES`.

        A source holds the release watermark back only if the session called
        `ReorderBuffer.expect` for it, and `SLOW_SOURCES` is that list. A source that reports
        a completion point under a name nobody declared is worse than one that reports
        nothing: the buffer has no reason to wait for it, so its events arrive after the
        wall-clock watermark has passed them and are dropped as late -- measured on a real
        testnet session as zero bar closes reaching the engine, with nothing raised anywhere.

        So the two sets must be equal, not merely overlapping.
        """
        feed, capture = make_feed(SYMBOL)

        offer_depth(feed, T0 + 100, update_id=1)
        offer_depth(feed, T0 + SECOND, update_id=2)
        feed._on_message(
            "btcusdt@markPrice@1s",
            mark_frame(T0, price="60250.10", next_funding_ms=T0 + FUNDING_INTERVAL),
            T0 + 2,
        )

        kline_stop = asyncio.Event()
        page = [kline_row(T0), kline_row(T0 + MINUTE)]
        await feed._poll_klines(
            FakeRestClient(kline_stop, klines={SYMBOL: [page]}), kline_stop
        )
        funding_stop = asyncio.Event()
        settled = [funding_row(T0 - FUNDING_INTERVAL, rate="0.00005000")]
        await feed._poll_funding(
            FakeRestClient(funding_stop, funding_rate={SYMBOL: [settled]}), funding_stop
        )

        assert {source for source, _ in capture.complete} == set(LiveFeed.SLOW_SOURCES)


# --------------------------------------------------------------------------------- funding


class LaggingFundingEndpoint:
    """`GET /fapi/v1/fundingRate` with a publication lag, and the mark stream beside it.

    Binance publishes a settlement row some time *after* the boundary it is stamped at, and
    `premiumIndex.nextFundingTime` advances first. Measured at the 10:00:00.000 UTC boundary
    on 2026-08-03: the advance came 638 ms after the settlement, the row itself 1 926 ms
    after it, and the row's visibility then flapped across the endpoint's nodes for another
    ten seconds. This serves exactly that order -- the mark frame advancing `nextFundingTime`
    lands before the poll that first sees the row -- which is the ordinary case rather than a
    contrived one.
    """

    def __init__(
        self,
        feed: LiveFeed,
        stop: asyncio.Event,
        *,
        settlement_ms: int,
        advance_on_call: int,
        publish_on_call: int,
        stop_after: int,
    ) -> None:
        self._feed = feed
        self._stop = stop
        self._settlement_ms = settlement_ms
        self._advance_on_call = advance_on_call
        self._publish_on_call = publish_on_call
        self._stop_after = stop_after
        self.calls = 0

    async def funding_rate(self, symbol: str, *, limit: int = 2) -> list[dict[str, Any]]:
        self.calls += 1
        if self.calls == self._advance_on_call:
            offer_mark(
                self._feed,
                self._settlement_ms + 700,
                price="60250.10",
                next_funding_ms=self._settlement_ms + FUNDING_INTERVAL,
                symbol=symbol,
            )
        rows = [funding_row(self._settlement_ms - FUNDING_INTERVAL, rate="0.00005000")]
        if self.calls >= self._publish_on_call:
            rows.append(funding_row(self._settlement_ms, rate="0.00010000"))
        if self.calls >= self._stop_after:
            self._stop.set()
        return rows


class TestFundingPolling:
    @pytest.mark.asyncio
    async def test_the_watermark_waits_at_a_boundary_whose_row_has_not_been_published(
        self,
    ) -> None:
        """Three polls. The mark stream advances `nextFundingTime` past the settlement at T0
        before poll 2, and `fundingRate` does not serve the row until poll 3.

        `nextFundingTime` is what lets this source claim completeness up to the present: no
        settlement can occur before it. The moment it advances, that argument covers a
        boundary the endpoint has not published a row for yet -- so reporting `now` there is a
        false completeness claim, the release watermark crosses T0, and the
        `FUNDING_SETTLEMENT` stamped T0 that poll 3 emits is refused by
        `ReorderBuffer.offer`, which drops anything stamped at or before the watermark. The
        cashflow is then never booked while `counts['funding']` still reads 1 and
        `FUNDING_UNSETTLED` cannot fire, because the engine never saw the event.

        So every point reported before the settlement is emitted is T0 - 1: complete up to
        the last millisecond before the boundary, and not one past it. Only once the row is
        in hand does the source claim the present again, which is bounded by the next
        announced settlement at T0 + 8 h - 1.
        """
        stop = asyncio.Event()
        feed, capture = make_feed(SYMBOL)
        offer_mark(feed, T0 - SECOND, price="60250.10", next_funding_ms=T0)
        client = LaggingFundingEndpoint(
            feed,
            stop,
            settlement_ms=T0,
            advance_on_call=2,
            publish_on_call=3,
            stop_after=3,
        )

        await feed._poll_funding(client, stop)

        assert [ts for source, ts in capture.complete if source == "funding"] == [
            T0 - 1,
            T0 - 1,
            T0 + FUNDING_INTERVAL - 1,
        ]
        assert capture.payloads(EventKind.FUNDING_SETTLEMENT) == [
            FundingPoint(symbol=SYMBOL, ts_ms=T0, rate=at_scale(0, "00010000"))
        ]

    def test_a_settlement_that_is_never_published_stops_holding_the_watermark_and_says_so(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The boundary at T0 passes, and no row for it ever appears.

        Waiting for ever is not an option and the reason is specific: this source keeps
        *reporting* while it waits, so `ReorderBuffer` never declares it stalled, and the
        release cutoff is the minimum over every live source -- so one settlement the
        endpoint never published would freeze the whole session's dispatch for the rest of
        the run. `FUNDING_PUBLICATION_GRACE_MS` is a wide multiple of the 1 926 ms lag
        measured at a real boundary, and the wait is timed from the moment the advance was
        *noticed* rather than from the settlement instant, so a session cannot inherit a wait
        that expired before it existed.

        At the bound exactly the watermark is still held at T0 - 1; one millisecond past it
        the source claims the present again -- the clock reads T0 + 60 501, which is short of
        the next announced settlement -- and writes one `STALE` record naming the settlement
        it gave up on. Silence there would leave a dropped cashflow with nothing in the tape
        to explain it.
        """
        clock = Clock(T0 - SECOND)
        monkeypatch.setattr(live_feed, "_now_ms", clock)
        feed, capture = make_feed(SYMBOL)
        offer_mark(feed, T0 - SECOND, price="60250.10", next_funding_ms=T0)
        clock.now_ms = T0 + 500
        offer_mark(feed, T0 + 500, price="60250.10", next_funding_ms=T0 + FUNDING_INTERVAL)
        settled_through = T0 - FUNDING_INTERVAL

        assert feed._funding_horizon(SYMBOL, settled_through) == T0 - 1
        clock.now_ms = T0 + 500 + FUNDING_PUBLICATION_GRACE_MS
        assert feed._funding_horizon(SYMBOL, settled_through) == T0 - 1
        assert capture.status == []

        clock.now_ms += 1
        assert feed._funding_horizon(SYMBOL, settled_through) == clock.now_ms
        assert feed._funding_horizon(SYMBOL, settled_through) == clock.now_ms

        assert [(kind, stream) for kind, stream, _, _ in capture.status] == [
            (CollectorEventKind.STALE, f"funding:{SYMBOL}")
        ]
        assert str(T0) in capture.status[0][2]

    @pytest.mark.asyncio
    async def test_a_settlement_from_before_the_session_is_seeded_and_the_next_is_emitted(
        self,
    ) -> None:
        """Two polls. The first sees only the settlement at T0 - 8 h; the second sees that one
        again plus a new one at T0.

        The first already happened, hours before the session existed. Replaying it would
        charge the account a cashflow it was never party to, at a timestamp hours behind the
        engine's clock -- so the first poll only records what the latest settlement *was*, and
        the second can then tell a new one from it. Exactly one `FUNDING_SETTLEMENT` is
        emitted, for T0.

        Spec 3.5 rule 3 forbids assuming an eight-hour schedule, so the emitted point carries
        the timestamp the row said. Its rate is the string "0.00010000" at the 10^8 scale,
        which is 10 000.
        """
        stop = asyncio.Event()
        old = funding_row(T0 - FUNDING_INTERVAL, rate="0.00005000")
        new = funding_row(T0, rate="0.00010000")
        client = FakeRestClient(stop, funding_rate={SYMBOL: [[old], [old, new]]})
        feed, capture = make_feed(SYMBOL)

        await feed._poll_funding(client, stop)

        assert capture.payloads(EventKind.FUNDING_SETTLEMENT) == [
            FundingPoint(symbol=SYMBOL, ts_ms=T0, rate=at_scale(0, "00010000"))
        ]
        assert feed.counts["funding"] == 1


# ----------------------------------------------------------------------------------- venues


def documented_delivery() -> dict[str, tuple[bool, bool]]:
    """The measurement table in `live.feed`'s module docstring, as data.

    Those stream lists are measurements taken on 2026-08-03, not documentation, and the
    module says so. Parsing the table is what makes the assertions below a check that the
    code agrees with what was measured rather than a restatement of the code.
    """
    row = re.compile(r"^\|\s*`([^`]+)`\s*\|([^|]+)\|([^|]+)\|$")
    table: dict[str, tuple[bool, bool]] = {}
    for line in (live_feed.__doc__ or "").splitlines():
        match = row.match(line.strip())
        if match is not None:
            stream, production, testnet = match.groups()
            table[stream] = ("silent" not in production, "silent" not in testnet)
    return table


class TestVenues:
    def test_each_venue_subscribes_to_exactly_the_streams_it_was_measured_to_deliver(
        self,
    ) -> None:
        """The docstring table against `Venue.ws_streams`, row by row.

        Subscribing to a stream an endpoint does not serve is not harmless: the socket carries
        a permanently dead subscription while a poller supplies the same dataset, so the day
        the stream is restored the session records every row twice.

        `@kline_1m` is the one deliberate departure. Testnet serves it and production does
        not, and a feed that preferred the stream would give a session recorded on one venue a
        structurally different bar series from one recorded on the other -- for the single most
        load-bearing series there is. So bars come from REST on both, and neither venue
        subscribes to it.
        """
        table = documented_delivery()

        assert table == {
            "@bookTicker": (True, True),
            "@depth20@100ms": (True, True),
            "@aggTrade": (False, True),
            "@markPrice@1s": (False, True),
            "@kline_1m": (False, True),
        }
        for stream, (on_production, on_testnet) in table.items():
            if stream == "@kline_1m":
                continue
            assert (stream in PRODUCTION.ws_streams) is on_production
            assert (stream in TESTNET.ws_streams) is on_testnet
        assert "@kline_1m" not in PRODUCTION.ws_streams
        assert "@kline_1m" not in TESTNET.ws_streams

    def test_production_polls_trades_and_marks_and_testnet_polls_neither(self) -> None:
        """The polling decision follows directly from the measured stream list.

        Production's socket suppresses `@aggTrade` and `@markPrice@1s`, so both datasets have
        to come from REST there -- and mark price is what liquidation is decided against
        (spec 3.7), so a venue that neither streamed nor polled it would run a session in
        which no position can ever be liquidated. Testnet delivers both, so polling them as
        well would double-record every row.
        """
        assert (PRODUCTION.polls_trades, PRODUCTION.polls_marks) == (True, True)
        assert (TESTNET.polls_trades, TESTNET.polls_marks) == (False, False)

    def test_the_subscription_list_is_every_symbol_crossed_with_the_venues_streams(
        self,
    ) -> None:
        """Two symbols against production's two streams gives four lowercase subscriptions.

        Lowercase because Binance's stream names are, and grouped per symbol because that is
        the order the combined subscription is sent in -- both are what the socket accepts
        rather than choices this module gets to make.
        """
        feed, _ = make_feed(SYMBOL, OTHER, venue=PRODUCTION)

        assert feed.streams == [
            "btcusdt@bookTicker",
            "btcusdt@depth20@100ms",
            "ethusdt@bookTicker",
            "ethusdt@depth20@100ms",
        ]


# ----------------------------------------------------------------------------- mark polling


class TestMarkPolling:
    @pytest.mark.asyncio
    async def test_polled_mark_samples_feed_the_same_aggregator_the_stream_does(self) -> None:
        """Three `premiumIndex` polls where production suppresses `@markPrice@1s`: 60250.10 at
        T0, the *same* payload re-served, and 60260.00 at T0 + 60 000.

        The middle poll is the ordinary case at 1 Hz against a server that updates once a
        second, and re-absorbing it would give last-observation-carried-forward two candidate
        rows for one instant. It is dropped on `time`, so the minute at T0 is built from two
        samples, not three -- and the bar it produces has to be identical in shape to the one
        the streamed path produced above, because the venue a session ran on must not change
        what the engine consumes.

        The bar published when the minute rolls is therefore high 60250.10, low 60250.10,
        close 60250.10 at T0 + 59 999: one observed sample, so the range is a point, which is
        honest rather than degenerate -- nothing else was seen.
        """
        stop = asyncio.Event()
        settlement = T0 + FUNDING_INTERVAL
        first = premium_index_payload(T0, price="60250.10", next_funding_ms=settlement)
        client = FakeRestClient(
            stop,
            premium_index={
                SYMBOL: [
                    first,
                    first,
                    premium_index_payload(
                        T0 + MINUTE, price="60260.00", next_funding_ms=settlement
                    ),
                ]
            },
        )
        feed, capture = make_feed(SYMBOL, venue=PRODUCTION)

        await feed._poll_marks(client, stop)

        assert capture.payloads(EventKind.MARK_PRICE_UPDATE) == [
            MarkBar(
                symbol=SYMBOL,
                close_time=T0 + MINUTE - 1,
                high=at_scale(60_250, "10"),
                low=at_scale(60_250, "10"),
                close=at_scale(60_250, "10"),
            )
        ]
        assert feed.counts["marks"] == 2
        assert feed.funding_times() == {SYMBOL: [settlement]}


# ---------------------------------------------------------------------------- trade polling


class TestTradePolling:
    @pytest.mark.asyncio
    async def test_the_first_trade_poll_anchors_the_cursor_instead_of_replaying_history(
        self,
    ) -> None:
        """Two polls. The first asks for the head with no `fromId` and must emit nothing; the
        second asks from head + 1 and emits what it gets.

        The head record already happened before the session started, and the page behind it
        is unbounded -- replaying it would push hours of trades through the fill model at
        timestamps far behind the engine's clock. Gaplessness comes from the cursor rather
        than from polling quickly: the second call asks from 5 933 015, exactly one past the
        head id 5 933 014, so falling behind costs latency and never data.
        """
        stop = asyncio.Event()
        head = [dict(agg_trade_frame(T0, agg_id=5_933_014))]
        page = [
            dict(agg_trade_frame(T0 + 300, agg_id=5_933_015)),
            dict(agg_trade_frame(T0 + 400, agg_id=5_933_016, is_buyer_maker=False)),
        ]
        client = FakeRestClient(stop, agg_trades={SYMBOL: [head, page]})
        feed, capture = make_feed(SYMBOL, venue=PRODUCTION)

        await feed._poll_trades(client, stop)

        assert client.from_ids == [None, 5_933_015]
        assert [
            (print_.agg_id, print_.ts_ms, print_.is_buyer_maker)
            for print_ in capture.payloads(EventKind.TRADE)
        ] == [(5_933_015, T0 + 300, True), (5_933_016, T0 + 400, False)]


# ------------------------------------------------------------------------ poller resilience


class TestPollerResilience:
    """A poller must outlive anything the sink or the endpoint throws at it.

    Nothing watches a poller task while a session runs -- `LiveFeed.run` only awaits them
    after the socket loop returns, hours later -- so an exception that escapes one ends that
    dataset for the rest of the run with no status entry, no warning and no log line. The
    strategy stops receiving `on_bar`, `monitor()` still reads market='up', and the session
    trades on: the exact shape `SLOW_SOURCES` names as the thing this platform exists to
    refuse. The guard used to cover only the fetch, so everything done with the response --
    the emit into the reorder buffer, and the parse -- was outside it.
    """

    @pytest.mark.asyncio
    async def test_a_sink_that_refuses_a_bar_does_not_kill_the_kline_poller(self) -> None:
        """Two polls, with the second offering a closed bar to a sink that raises.

        `ReorderOverflow` is the reachable case: `ReorderBuffer.offer` raises it once the
        buffer holds `max_held` events, `PaperSession._offer` does not catch it, and the
        buffer fills whenever releases stop -- a wall clock stepping backwards freezes the
        release watermark by design while offers keep arriving. REST klines are the only bar
        source on either venue, so a poller that dies here means a session with no bar closes
        at all for as long as it keeps running.

        The poll loop must therefore return normally, and the failure has to be recorded as
        data: one `STALE` entry naming the dataset and the exception. `STALE` rather than
        `DISCONNECT` because the endpoint answered correctly -- and because `PaperSession`
        measures spec 7's `max_disconnect_seconds` trigger from `DISCONNECT` records, so
        calling a full buffer a socket outage could halt a session holding a position.
        """
        stop = asyncio.Event()
        client = FakeRestClient(
            stop,
            klines={
                SYMBOL: [
                    [kline_row(T0), kline_row(T0 + MINUTE)],
                    [kline_row(T0 + MINUTE), kline_row(T0 + 2 * MINUTE)],
                ]
            },
        )
        feed, capture = make_feed(SYMBOL, capture=RefusingCapture())

        await feed._poll_klines(client, stop)

        assert [(kind, stream) for kind, stream, _, _ in capture.status] == [
            (CollectorEventKind.STALE, "klines")
        ]
        assert "handler raised: ReorderOverflow" in capture.status[0][2]
        assert feed.counts["bars"] == 1, "the bar was assembled; the sink refused it"

    @pytest.mark.asyncio
    async def test_a_malformed_premium_index_payload_does_not_kill_the_mark_poller(
        self,
    ) -> None:
        """Two polls, the second serving `markPrice` as JSON null instead of a string.

        `to_scaled` starts by stripping the text, so a null lands as `AttributeError` -- a
        class the poller's guard never covered, because the parse happened outside it. The
        socket path has guarded exactly this since `StreamManager._deliver` was written, with
        the same wording; the polled path had no equivalent, and mark price is what
        liquidation is decided against on the venue that polls it.

        One sample is absorbed, the malformed one is recorded, and the loop returns.
        """
        stop = asyncio.Event()
        settlement = T0 + FUNDING_INTERVAL
        client = FakeRestClient(
            stop,
            premium_index={
                SYMBOL: [
                    premium_index_payload(T0, price="60250.10", next_funding_ms=settlement),
                    {
                        **premium_index_payload(
                            T0 + MINUTE, price="60260.00", next_funding_ms=settlement
                        ),
                        "markPrice": None,
                    },
                ]
            },
        )
        feed, capture = make_feed(SYMBOL, venue=PRODUCTION)

        await feed._poll_marks(client, stop)

        assert [(kind, stream) for kind, stream, _, _ in capture.status] == [
            (CollectorEventKind.STALE, "markPrice")
        ]
        assert "handler raised: AttributeError" in capture.status[0][2]
        assert feed.counts["marks"] == 1

    @pytest.mark.asyncio
    async def test_a_sink_that_refuses_a_settlement_does_not_kill_the_funding_poller(
        self,
    ) -> None:
        """Two polls: the first seeds the settlement before the session, the second emits a
        new one into a sink that raises.

        Same guard as the kline poller, at the source whose events are the largest non-fill
        cashflow in a run. The settlement is counted -- the feed did produce it -- and the
        refusal is recorded rather than ending the poller, which would leave every later
        settlement of a forty-eight hour session undiscovered as well.
        """
        stop = asyncio.Event()
        old = funding_row(T0 - FUNDING_INTERVAL, rate="0.00005000")
        new = funding_row(T0, rate="0.00010000")
        client = FakeRestClient(stop, funding_rate={SYMBOL: [[old], [old, new]]})
        feed, capture = make_feed(SYMBOL, capture=RefusingCapture())

        await feed._poll_funding(client, stop)

        assert [(kind, stream) for kind, stream, _, _ in capture.status] == [
            (CollectorEventKind.STALE, "funding")
        ]
        assert "handler raised: ReorderOverflow" in capture.status[0][2]
        assert feed.counts["funding"] == 1

    @pytest.mark.asyncio
    async def test_a_poller_that_died_is_recorded_rather_than_raised_out_of_the_shutdown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A poller task that ends with an exception, awaited by `run`'s own cleanup.

        The guards above make this unreachable through the pollers as they stand, and it is
        still the wrong end for a stored exception: `run`'s cleanup awaits the tasks, so
        re-raising there propagates through `PaperSession.run`'s `finally` and skips
        `_drain_to_engine(final=True)` -- the session loses the tape's last events, its
        metrics and its manifest to a poller that died hours earlier. Recorded instead, under
        the task's own name, so the gap in that dataset is explainable from the tape.

        The socket loop returns only once the poller has raised, so the ordering under test is
        the real one: a task that is already done when `run` starts cancelling.
        """
        raised = asyncio.Event()

        async def die(client: Any, stop: asyncio.Event) -> None:
            raised.set()
            raise ReorderOverflow("the reorder buffer already holds 100000 events")

        class SilentStreamManager:
            """A socket that connects to nothing and closes when the poller has failed."""

            def __init__(
                self,
                streams: list[str],
                on_message: Any,
                on_status: Any,
                *,
                base_url: str,
            ) -> None:
                self.streams = streams

            async def run(self, stop: asyncio.Event) -> None:
                await raised.wait()

        class EmptyRestClient:
            """Every endpoint served empty, so the surviving pollers do nothing at all."""

            async def __aenter__(self) -> EmptyRestClient:
                return self

            async def __aexit__(self, *exc: Any) -> bool:
                return False

            async def funding_rate(
                self, symbol: str, *, limit: int = 2
            ) -> list[dict[str, Any]]:
                return []

        monkeypatch.setattr(live_feed, "StreamManager", SilentStreamManager)
        monkeypatch.setattr(
            live_feed, "PublicRestClient", lambda base, budget=None: EmptyRestClient()
        )
        feed, capture = make_feed(SYMBOL)
        monkeypatch.setattr(feed, "_poll_klines", die)

        await feed.run(asyncio.Event())

        assert [(kind, stream) for kind, stream, _, _ in capture.status] == [
            (CollectorEventKind.STALE, "poll-klines")
        ]
        assert "handler raised: ReorderOverflow" in capture.status[0][2]
