"""Tick and depth replay -- the data half of the realism layer (spec 4.2, 6.4).

Phase 4 replayed three series a strategy could hold in memory. Phase 5 adds three it cannot:
a single day of BTCUSDT `bookTicker` is tens of millions of rows, and `aggTrades` is not far
behind. Every loader here is therefore a **generator over `stream_query`**, yielding one
`Event` at a time and holding at most one Arrow batch, and the engine's `EventQueue` merges
them lazily against the bar and mark streams it already had.

| Stream | Dataset | Priority | What it decides |
|---|---|---|---|
| depth | `depth20` | `BOOK_UPDATE` (3) | the ladder a market order walks, and the size resting at a limit level |
| top of book | `bookTicker` | `BOOK_UPDATE` (3) | best bid/ask for the mid-fidelity tiers, and `ctx.spread()` |
| trades | `aggTrades` | `TRADE` (4) | what consumes limit queue, and the print a `TRADE_ONLY` market order fills at |

**Both book streams share priority 3, and that is not a tie.** Spec 6.2's key ends in
`dataset_id`, and `bookTicker` sorts before `depth20`, so a run carrying both applies the
top-of-book update first and the ladder second at the same millisecond. Deterministic, and
the right way round: the ladder is the richer observation and should win.

**Aggressor side is preserved exactly.** `is_buyer_maker=True` means the buyer was the
maker, so the trade was *sell*-aggressive and consumed **bid**-side queue. The entire limit
model reads this one boolean, and inverting it silently reverses every queue decision in the
backtest -- which is why `TradePrint.consumes_bids` exists rather than each call site
re-deriving it from the flag.

**Nothing here fabricates a level.** A `depth20` row carries exactly the levels the exchange
published; if an order is larger than all twenty, that is the depth-exhaustion case
(spec 6.4) and it is priced by a stated penalty and flagged, not by extrapolating a
twenty-first level nobody saw.

**Book rows carry two clocks, and visibility keys on the receive one.** `ts_ms` is the
exchange's stamp -- the instant the observation *holds for* -- and `recv_ms` is when the
platform's collector actually had it in hand. The two differ by transport latency (tens of
milliseconds on a healthy link, unbounded across a reconnect), and a backtest that gates on
`ts_ms` hands the strategy and the fill models a book the platform had not yet received --
a look-ahead that is invisible in replay, always favourable, and would not survive a live
session, where the engine can only apply frames after they arrive. So the book loaders gate
and order on `COALESCE("recv_ms", "ts_ms")` -- exactly the rule `backtest.load_macro`
applies, and the same `COALESCE` fallback for the bulk-archive rows that predate the
collector and have no honest receive time. The snapshot itself keeps its exchange `ts_ms`,
which is what `book.MarketView` ages against: a row is *visible* from when we had it and
*stale* from when the exchange stamped it. The trade tape is deliberately not recv-gated:
prints feed the simulated matching engine as venue-side events -- queue consumption, trigger
hits -- which happen at the venue at exchange time whether or not our collector had seen
them yet, and an order's own path to the venue is already priced by the latency model.
"""

from __future__ import annotations

import logging
from bisect import bisect_right
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from perplab.core.types import DepthSnapshot
from perplab.data.query import partition_predicate, query, stream_query
from perplab.data.schemas import normalise_symbol
from perplab.engine.clock import Event, EventKind

log = logging.getLogger("perplab.engine.ticks")

__all__ = [
    "TRADE_DATASET",
    "BOOK_TICKER_DATASET",
    "DEPTH_DATASET",
    "TradePrint",
    "TopOfBook",
    "StateStream",
    "trade_events",
    "book_ticker_stream",
    "depth_stream",
    "depth_events",
    "first_trade_ms",
]

TRADE_DATASET = "aggTrades"
BOOK_TICKER_DATASET = "bookTicker"
DEPTH_DATASET = "depth20"

_SCALE_F = float(10**8)
"""The lake's scaling as a float, for `TradePrint`'s strategy-facing views only.

A float divide, not `from_scaled`: this is the indicator side of spec 3.1's seam, where
`float` is the declared representation. Nothing that reaches a balance goes through here.
"""


# --------------------------------------------------------------------------------- rows


@dataclass(frozen=True, slots=True)
class TradePrint:
    """One aggregate trade: a price that genuinely traded, and which side was hit.

    This is also what `Strategy.on_tick` receives, so it has two faces on purpose.
    `price_scaled` / `qty_scaled` are the lake's exact integers and are what the queue model
    and the fill models compare against book sizes; `price` / `qty` are `float` views for
    indicator-style strategy code, which spec 3.1 puts firmly on the float side of the seam.
    Naming the integers explicitly is the point -- a strategy reading `trade.price` and
    getting `6_000_000_000_000` would be a footgun, and one reading it and getting a
    `Decimal` would put an exact-arithmetic construction on the hottest path in the engine.

    `recv_ms`, `first_trade_id` and `last_trade_id` are deliberately absent, which is why
    this is not `core.types.AggTrade`. Nothing in the engine reads them, a strategy cannot
    act on them identically in backtest and live, and three unread `int64`s per row across
    eighteen million rows is real money.
    """

    symbol: str
    ts_ms: int
    price_scaled: int
    qty_scaled: int
    is_buyer_maker: bool
    agg_id: int

    @property
    def price(self) -> float:
        return self.price_scaled / _SCALE_F

    @property
    def qty(self) -> float:
        return self.qty_scaled / _SCALE_F

    @property
    def notional(self) -> float:
        return self.price * self.qty

    @property
    def consumes_bids(self) -> bool:
        """Whether this trade ate **bid**-side resting size.

        `is_buyer_maker=True` means the resting order was the buy, so the aggressor sold
        into it. Named rather than inlined because the double negative in "buyer was the
        maker, therefore the trade was sell-aggressive" is exactly the kind of reasoning
        that gets transcribed backwards, and every queue decision in `resting.py` depends
        on it.
        """
        return self.is_buyer_maker

    @property
    def consumes_asks(self) -> bool:
        return not self.is_buyer_maker

    @property
    def is_sell_aggressive(self) -> bool:
        """Reader-facing spelling of the same fact, for strategy code."""
        return self.is_buyer_maker


@dataclass(frozen=True, slots=True)
class TopOfBook:
    """Best bid and ask with their sizes, as published by `<symbol>@bookTicker`.

    Event-driven rather than sampled: Binance emits a row whenever the best bid or ask
    changes, so unlike `depth20` this is not a downsampled view of a faster stream and the
    timestamps are the moments the touch actually moved.
    """

    symbol: str
    ts_ms: int
    bid_px: int
    bid_qty: int
    ask_px: int
    ask_qty: int


"""Depth rows are replayed as `core.types.DepthSnapshot`, the type the collector already
writes and `ctx.book()` already returns -- one type end to end, no conversion at the
strategy boundary.

Trades are *not* replayed as `core.types.AggTrade`, and the asymmetry is deliberate.
`AggTrade` carries `recv_ms`, `first_trade_id` and `last_trade_id`, which nothing in the
engine reads; depth is one row per second and the extra columns cost nothing, while trades
are the highest-volume stream in the system and three unread int64s per row is real money
across a year. `TradePrint` is the lean row, and `consumes_bids` is the one piece of
interpretation it adds.
"""


# ------------------------------------------------------------------------------- loaders


def _symbol_clause(symbols: Sequence[str]) -> tuple[list[str], str]:
    wanted = [normalise_symbol(s) for s in symbols]
    return wanted, ", ".join("?" for _ in wanted)


def trade_events(
    root: Path | str,
    symbols: Sequence[str],
    start_ms: int,
    end_ms: int,
) -> Iterator[Event]:
    """`TRADE` events from `aggTrades`, in `(ts_ms, agg_id)` order.

    Ordered by `agg_id` within a millisecond rather than by nothing in particular, because
    `agg_id` is Binance's own monotonic sequence and is therefore the exchange's account of
    what happened first. It also doubles as `source_seq`: spec 6.2 wants a per-source
    monotonic index, and an id the exchange assigned is stable across re-ingests in a way
    that a row number produced by whatever order DuckDB read the files in is not.

    **Each symbol gets its own `dataset_id`, mirroring `reorder.live_dataset_id`.** Binance
    allocates `agg_id` *per symbol*, so under one shared `"aggTrades"` id two symbols
    printing the same id in the same millisecond produced two identical total-order keys and
    `EventQueue.pop` raised `OrderingViolation` mid-run -- the exact failure
    `engine.reorder`'s docstring dissects for the live path, reproduced in replay. One
    sequence space per stream per symbol is the rule that makes the exchange's id a valid
    `source_seq`, and using the same `stream:symbol` spelling live uses keeps a tape and a
    lake replay of the same window keyed alike.

    Multi-symbol runs interleave correctly because the sort is on `(ts_ms, agg_id, symbol)`
    across all symbols at once -- one query, not one per symbol -- which is exactly the
    total-order key `(ts_ms, priority, source_seq, dataset_id)` with the constant priority
    dropped, so the merged stream reaches the queue already in key order.
    """
    wanted, placeholders = _symbol_clause(symbols)
    dataset_ids = {symbol: f"{TRADE_DATASET}:{symbol}" for symbol in wanted}
    pruning = partition_predicate(TRADE_DATASET, start_ms=start_ms, end_ms=end_ms)
    sql = f"""
        SELECT "symbol", "ts_ms", "price", "qty", "is_buyer_maker", "agg_id"
        FROM "{TRADE_DATASET}"
        WHERE {pruning}
          AND "symbol" IN ({placeholders})
          AND "ts_ms" >= ? AND "ts_ms" < ?
        ORDER BY "ts_ms", "agg_id", "symbol"
    """
    for batch in stream_query(
        root, sql, datasets=(TRADE_DATASET,), params=[*wanted, int(start_ms), int(end_ms)]
    ):
        columns = batch.to_pydict()
        symbols_out = columns["symbol"]
        times = columns["ts_ms"]
        prices = columns["price"]
        quantities = columns["qty"]
        makers = columns["is_buyer_maker"]
        ids = columns["agg_id"]
        for index in range(batch.num_rows):
            yield Event(
                ts_ms=times[index],
                kind=EventKind.TRADE,
                source_seq=ids[index],
                dataset_id=dataset_ids[symbols_out[index]],
                payload=TradePrint(
                    symbol=symbols_out[index],
                    ts_ms=times[index],
                    price_scaled=prices[index],
                    qty_scaled=quantities[index],
                    is_buyer_maker=makers[index],
                    agg_id=ids[index],
                ),
            )


class StateStream:
    """A time-sorted single-symbol row stream, pulled forward on demand.

    **Why the book is not replayed as events.** A day of BTCUSDT `bookTicker` is about 15
    million rows -- 96 million in this lake -- and pushing each one through the event queue
    would build 96 million `Event` objects and 96 million heap operations to answer a
    question that is only ever asked at order arrivals and bar closes. A book is a *state*,
    not a sequence of things that happen: what the engine needs is "the touch in force at
    time T", and that is one row.

    So the rows are scanned by DuckDB in C++, delivered as Arrow batches, and located by
    binary search over the batch's timestamp column. Advancing across a million rows costs
    one `bisect` per batch and one row materialisation -- the other 999 999 rows are never
    touched by Python at all.

    **This is exactly equivalent to dispatching every row at `BOOK_UPDATE` priority**, not an
    approximation of it. Applying every row visible at or before `T` in order and keeping
    the last leaves precisely the state this returns, because each row wholly *replaces* the
    one before -- a book snapshot is not an increment. `backtest._dispatch` advances to `T`
    for events at or after `BOOK_UPDATE`'s priority and to `T - 1` for the three that precede
    it, which reproduces spec 6.2's ordering to the millisecond. The book loaders hand this
    class `visible_ms` -- `COALESCE(recv_ms, ts_ms)`, the module docstring's rule -- as the
    `time_column`, so "at or before `T`" means what the platform *had* by `T`, not what the
    exchange had stamped by then.

    **One stream per symbol.** A merged stream would make "the last row at or before T"
    return one symbol's row while every other symbol's state silently went stale. Splitting
    the query costs one more open connection per symbol and removes the failure entirely.

    **Rows the builder rejects are skipped, and the previous state stands.** `build` returns
    `None` for a row that cannot be trusted -- an empty ladder, or one whose price and
    quantity arrays differ in length -- and the search then walks backwards for the most
    recent usable row rather than reporting no book at all.
    """

    __slots__ = ("_batches", "_build", "_time", "_batch", "_times", "_cursor", "_current")

    _times: list[int] | None

    def __init__(
        self,
        batches: Iterator[pa.RecordBatch],
        build: Callable[[pa.RecordBatch, int], Any | None],
        *,
        time_column: str = "ts_ms",
    ) -> None:
        self._batches = batches
        self._build = build
        self._time = time_column
        self._batch: pa.RecordBatch | None = None
        self._times: Any = None
        self._cursor = 0
        self._current: Any | None = None

    def advance_to(self, ts_ms: int) -> Any | None:
        """The row in force at `ts_ms`: the most recent usable one at or before it.

        Monotonic by construction -- consumed rows are never revisited -- so a call with a
        timestamp earlier than a previous one returns the state already reached rather than
        rewinding. The engine's clock never moves backwards (`EngineRuntime.advance` raises
        if it tries), so that case does not arise; it is stated because a stream that
        silently rewound would be a look-ahead bug wearing a caching bug's clothes.
        """
        while True:
            if self._batch is None:
                batch = next(self._batches, None)
                if batch is None:
                    return self._current
                if batch.num_rows == 0:
                    continue
                self._batch = batch
                # One bulk conversion of a single `int64` column per batch, so `bisect` can
                # work on it. Converting the whole batch would be the per-row cost this
                # class exists to avoid; converting one column of 65 536 integers is a C
                # loop that runs once per batch and is never the bottleneck.
                self._times = batch.column(self._time).to_pylist()
                self._cursor = 0

            times = self._times
            index = bisect_right(times, ts_ms) - 1
            if index >= self._cursor:
                # Backwards from the newest acceptable row: normally one iteration, more
                # only where the loader has rejected rows.
                for candidate in range(index, self._cursor - 1, -1):
                    built = self._build(self._batch, candidate)
                    if built is not None:
                        self._current = built
                        break
                self._cursor = index + 1

            if self._cursor < len(times):
                # The next row is in the future; this batch still has more to give.
                return self._current
            self._batch = None
            self._times = None

    @property
    def current(self) -> Any | None:
        """The most recently materialised row, without advancing."""
        return self._current

    def close(self) -> None:
        """Release the underlying DuckDB connection.

        `stream_query` closes on exhaustion, but a run that aborts part-way leaves the
        generator suspended and its connection open until collection. Closing explicitly is
        what keeps a worker that raised from holding a lake handle for the rest of its life.
        """
        self._batches.close()  # type: ignore[attr-defined]
        self._batch = None
        self._times = None


def book_ticker_stream(
    root: Path | str, symbol: str, start_ms: int, end_ms: int
) -> StateStream:
    """Top-of-book state for one symbol over one range.

    **Visibility keys on `COALESCE("recv_ms", "ts_ms")`, not on the exchange stamp** -- see
    the module docstring. A quote stamped `...199_999` that reached the collector at
    `...200_085` was not knowable at `...200_000`, and gating on `ts_ms` served it 85 ms
    early to every mid, spread and crossing decision in the run. The row itself still
    carries its exchange `ts_ms`, which is what `book.MarketView` bounds staleness against.
    Bulk-archive rows have `recv_ms` null -- there was no local clock to record -- and fall
    back to their own stamp, the least wrong figure available; that residual asymmetry is
    bounded by one transport hop and is stated here rather than hidden. (Partition pruning
    still keys on `ts_ms`'s day, so a row received across a midnight boundary from its own
    stamp can be pruned out of a range starting exactly there -- a sub-second edge accepted
    for the pruning.)

    `update_id` breaks ties within a millisecond of visibility -- it is Binance's own
    order-book update sequence, so it is the exchange's account of which quote came first.

    No lead-in row is fetched, unlike `feed.load_marks`. Top of book is only ever read as
    "the state at this instant", and an order arriving before the first observation is
    refused rather than filled against a quote from outside the run's own range.
    """
    pruning = partition_predicate(BOOK_TICKER_DATASET, start_ms=start_ms, end_ms=end_ms)
    sql = f"""
        SELECT COALESCE("recv_ms", "ts_ms") AS "visible_ms",
               "ts_ms", "bid_px", "bid_qty", "ask_px", "ask_qty"
        FROM "{BOOK_TICKER_DATASET}"
        WHERE {pruning} AND "symbol" = ?
          AND COALESCE("recv_ms", "ts_ms") >= ? AND COALESCE("recv_ms", "ts_ms") < ?
        ORDER BY "visible_ms", "update_id"
    """
    wanted = normalise_symbol(symbol)
    batches = stream_query(
        root,
        sql,
        datasets=(BOOK_TICKER_DATASET,),
        params=[wanted, int(start_ms), int(end_ms)],
    )

    def build(batch: pa.RecordBatch, index: int) -> TopOfBook | None:
        return TopOfBook(
            symbol=wanted,
            ts_ms=batch.column(1)[index].as_py(),
            bid_px=batch.column(2)[index].as_py(),
            bid_qty=batch.column(3)[index].as_py(),
            ask_px=batch.column(4)[index].as_py(),
            ask_qty=batch.column(5)[index].as_py(),
        )

    return StateStream(batches, build, time_column="visible_ms")


def depth_events(
    root: Path | str, symbols: Sequence[str], start_ms: int, end_ms: int
) -> Iterator[Event]:
    """`BOOK_UPDATE` events from `depth20`, for the one case that needs every row.

    The fill models want *the ladder in force at instant T*, which `depth_stream` answers
    with a binary search and no per-row work. A **depth-driven indicator** wants something
    different: every snapshot, in order, because its value is a function of the whole series
    rather than of the latest row. Feeding one from the state stream would hand it a silently
    subsampled series -- whatever rows the engine happened to advance past on its way to an
    order arrival -- and an indicator computed over an arbitrary subsample is not the
    indicator the author registered.

    So a run that registers one pays for real events instead. That is affordable here in a
    way it is not for `bookTicker`: `depth20` is sampled at 1 s, about 45 000 rows a day,
    against `bookTicker`'s fifteen million.

    **Events are stamped and ordered on the visible clock** -- `COALESCE("recv_ms",
    "ts_ms")`, the module docstring's rule -- so a snapshot the collector received 86 ms
    after its exchange stamp is dispatched 86 ms later, exactly when a live session would
    have applied it. The payload keeps the exchange `ts_ms`, so `MarketView` still ages the
    ladder from when it held, not from when it arrived. And each symbol is its own
    `dataset_id` (`depth20:BTCUSDT`), mirroring `reorder.live_dataset_id`, because
    `last_update_id` is allocated per symbol: on a 1 s-sampled stream every symbol lands on
    the same aligned millisecond, so two symbols sharing an id under one dataset produced
    identical total-order keys and killed a multi-symbol run with `OrderingViolation`.
    """
    wanted, placeholders = _symbol_clause(symbols)
    dataset_ids = {symbol: f"{DEPTH_DATASET}:{symbol}" for symbol in wanted}
    pruning = partition_predicate(DEPTH_DATASET, start_ms=start_ms, end_ms=end_ms)
    sql = f"""
        SELECT COALESCE("recv_ms", "ts_ms") AS "visible_ms",
               "symbol", "ts_ms", "recv_ms", "last_update_id",
               "bid_px", "bid_qty", "ask_px", "ask_qty"
        FROM "{DEPTH_DATASET}"
        WHERE {pruning}
          AND "symbol" IN ({placeholders})
          AND COALESCE("recv_ms", "ts_ms") >= ? AND COALESCE("recv_ms", "ts_ms") < ?
        ORDER BY "visible_ms", "last_update_id", "symbol"
    """
    # Counted rather than silently skipped (M31): `feed.load_bars` counts and warns for
    # exactly this class, and a malformed ladder here means the *previous* snapshot stood
    # in for the fill model -- which is a data-quality fact a BOOK_WALK run needs on the
    # record, not something to infer months later from an unexplained slippage number.
    malformed = 0
    for batch in stream_query(
        root, sql, datasets=(DEPTH_DATASET,), params=[*wanted, int(start_ms), int(end_ms)]
    ):
        columns = batch.to_pydict()
        for index in range(batch.num_rows):
            bids, bidq = columns["bid_px"][index], columns["bid_qty"][index]
            asks, askq = columns["ask_px"][index], columns["ask_qty"][index]
            if not bids or not asks or len(bids) != len(bidq) or len(asks) != len(askq):
                malformed += 1
                continue
            yield Event(
                ts_ms=columns["visible_ms"][index],
                kind=EventKind.BOOK_UPDATE,
                source_seq=columns["last_update_id"][index],
                dataset_id=dataset_ids[columns["symbol"][index]],
                payload=DepthSnapshot(
                    symbol=columns["symbol"][index],
                    ts_ms=columns["ts_ms"][index],
                    recv_ms=columns["recv_ms"][index] or 0,
                    last_update_id=columns["last_update_id"][index],
                    bid_px=tuple(bids),
                    bid_qty=tuple(bidq),
                    ask_px=tuple(asks),
                    ask_qty=tuple(askq),
                ),
            )
    if malformed:
        log.warning(
            "depth_events: %d malformed depth row(s) skipped over [%d, %d) -- empty or "
            "ragged ladders; the previous snapshot stood in for each (GAP_SKIPPED)",
            malformed,
            start_ms,
            end_ms,
        )


def depth_stream(root: Path | str, symbol: str, start_ms: int, end_ms: int) -> StateStream:
    """Depth-ladder state for one symbol over one range.

    **Rows with an empty or ragged ladder are rejected, not repaired.** A snapshot with no
    bids is not a market: walking it would exhaust on level zero and charge the whole order
    the depth-exhaustion penalty, which reads on the results page as an illiquid instrument
    rather than as the missing observation it actually is. A row whose price and quantity
    arrays differ in length is worse -- walking it would pair a size with a price it never
    belonged to. Both leave the previous snapshot standing, and `book.MarketView`'s
    staleness bound is what then reports the hole for what it is.

    **Advanced on the visible clock**, `COALESCE("recv_ms", "ts_ms")` -- the module
    docstring's rule, and the closing of a systematic look-ahead: this stream used to bisect
    on `ts_ms`, so a ladder stamped `...199_999` and received at `...200_085` was serving
    `ctx.book()` and the fill models 86 ms before the platform had it, while the macro
    loader one module over documented the recv gate as "the no-look-ahead guarantee". The
    snapshot keeps its exchange `ts_ms` for `MarketView`'s staleness bound; bulk rows with
    no receive clock fall back to their own stamp, as everywhere else.
    """
    pruning = partition_predicate(DEPTH_DATASET, start_ms=start_ms, end_ms=end_ms)
    sql = f"""
        SELECT COALESCE("recv_ms", "ts_ms") AS "visible_ms",
               "ts_ms", "recv_ms", "last_update_id",
               "bid_px", "bid_qty", "ask_px", "ask_qty"
        FROM "{DEPTH_DATASET}"
        WHERE {pruning} AND "symbol" = ?
          AND COALESCE("recv_ms", "ts_ms") >= ? AND COALESCE("recv_ms", "ts_ms") < ?
        ORDER BY "visible_ms", "last_update_id"
    """
    wanted = normalise_symbol(symbol)
    batches = stream_query(
        root, sql, datasets=(DEPTH_DATASET,), params=[wanted, int(start_ms), int(end_ms)]
    )

    def build(batch: pa.RecordBatch, index: int) -> DepthSnapshot | None:
        bids = batch.column(4)[index].as_py()
        bid_qty = batch.column(5)[index].as_py()
        asks = batch.column(6)[index].as_py()
        ask_qty = batch.column(7)[index].as_py()
        if not bids or not asks or len(bids) != len(bid_qty) or len(asks) != len(ask_qty):
            return None
        return DepthSnapshot(
            symbol=wanted,
            ts_ms=batch.column(1)[index].as_py(),
            recv_ms=batch.column(2)[index].as_py() or 0,
            last_update_id=batch.column(3)[index].as_py(),
            bid_px=tuple(bids),
            bid_qty=tuple(bid_qty),
            ask_px=tuple(asks),
            ask_qty=tuple(ask_qty),
        )

    return StateStream(batches, build, time_column="visible_ms")


def first_trade_ms(
    root: Path | str, symbols: Sequence[str], start_ms: int, end_ms: int
) -> int | None:
    """Timestamp of the earliest trade in the range, or `None` if there is none.

    A one-row aggregate, not a stream: the engine uses it only to decide whether the tick
    datasets actually hold anything for the range before it commits to a tier, and reading
    a whole day of ticks to answer a yes/no question would cost more than the run.
    """
    wanted, placeholders = _symbol_clause(symbols)
    pruning = partition_predicate(TRADE_DATASET, start_ms=start_ms, end_ms=end_ms)
    table = query(
        root,
        f"""
        SELECT min("ts_ms") AS first_ms
        FROM "{TRADE_DATASET}"
        WHERE {pruning}
          AND "symbol" IN ({placeholders})
          AND "ts_ms" >= ? AND "ts_ms" < ?
        """,
        datasets=(TRADE_DATASET,),
        params=[*wanted, int(start_ms), int(end_ms)],
    )
    if table.num_rows == 0:
        return None
    value = table.column("first_ms")[0].as_py()
    return None if value is None else int(value)
