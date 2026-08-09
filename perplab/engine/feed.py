"""Turning the Parquet lake into ordered event streams (spec 6.2).

Three streams feed *every* backtest, whatever its tier, and each one exists because the
engine needs a different kind of truth from it. The tick and depth streams the higher tiers
add live in `ticks.py`:

| Stream | Dataset | Carries |
|---|---|---|
| bars | `klines`, aggregated to the strategy's timeframe | what the strategy sees |
| marks | `markPriceKlines`, always 1 m | what risk is measured against |
| funding | `funding` | the cashflows |

**Bars and marks are different series and must not be substituted for each other.** Spec 3.4
is categorical: fills happen at traded prices, risk happens at mark price, and conflating
them is "a classic and expensive bug". `klines` is the traded series and `markPriceKlines`
is the mark series; they are different numbers, and on a wick they can differ by hundreds
of dollars.

**Marks stay at 1 m whatever timeframe the strategy runs.** A strategy on 4 h bars still has
a position that can be liquidated at 03:17, and sampling risk at the strategy's cadence
would mean a position could survive a mark excursion simply because nobody looked. The mark
stream is the finest resolution the bulk archive offers.

**The high and low of a mark bar are carried but never become the mark.** Spec 3.4's rule is
last-observation-carried-forward: the sample at a bar's close time is its close, held flat
until the next. The high and the low are *evidence that the mark traversed that range*
during the bar, which is exactly what the liquidation check needs and nothing else does.
They arrive with the close and are consumed at `LIQUIDATION_CHECK`; see
`backtest._on_liquidation_check` for why they are probed there rather than applied here.

**Warm-up is loaded, not simulated.** A strategy declaring `history: 400` gets 400 real bars
from before the run's start date, so it is warm on the first bar of the range the user asked
for. The alternative -- starting cold at `start_ms` -- silently converts the first N bars of
every backtest into a period the strategy could not trade, and the user's chosen start date
would not be the date trading began.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from perplab.core.types import Bar
from perplab.data.query import (
    TIMEFRAMES,
    derive_timeframe,
    missing_buckets,
    partition_predicate,
    query,
    timeframe_ms,
)
from perplab.data.schemas import normalise_symbol
from perplab.engine.clock import Event, EventKind

__all__ = [
    "MARK_DATASET",
    "BAR_DATASET",
    "FUNDING_DATASET",
    "MarkBar",
    "FundingPoint",
    "BarStep",
    "FeedError",
    "load_bars",
    "load_marks",
    "load_funding",
    "bar_events",
    "mark_events",
    "funding_events",
    "warmup_start_ms",
]

BAR_DATASET = "klines"
MARK_DATASET = "markPriceKlines"
FUNDING_DATASET = "funding"

_MARK_INTERVAL_MS = TIMEFRAMES["1m"]


class FeedError(RuntimeError):
    """The lake cannot supply what the run needs, and no substitute is honest."""


@dataclass(frozen=True, slots=True)
class MarkBar:
    """One minute of mark price: the sample, plus the range it traversed to get there.

    `close` is the *sample* -- spec 3.4's stored observation at `close_time`, carried
    forward until the next one. `high` and `low` are not samples and must never be assigned
    a timestamp: a kline says they happened inside the bar and says nothing about when. They
    exist so the liquidation check can ask "did the mark cross `P_liq` at any point during
    this minute", which is a question about a range and not about an instant.
    """

    symbol: str
    close_time: int
    high: int
    low: int
    close: int


@dataclass(frozen=True, slots=True)
class FundingPoint:
    symbol: str
    ts_ms: int
    rate: int
    """Scaled by 10^8, like every other lake number. A rate of 0.0001 is 10 000 here."""


@dataclass(frozen=True, slots=True)
class BarStep:
    """Every symbol's bar closing at one instant.

    Grouped rather than emitted per symbol because indicators for *all* symbols must
    advance before *any* `on_bar` runs. A pairs strategy reading the other leg's indicator
    in its first `on_bar` would otherwise get a value one bar stale, and which leg was stale
    would depend on the order symbols happened to come out of the query.
    """

    close_time: int
    bars: tuple[Bar, ...]


# --------------------------------------------------------------------------------- bars


def warmup_start_ms(start_ms: int, timeframe: str, warmup_bars: int) -> int:
    """Where data loading begins so the strategy is warm at `start_ms`.

    One extra bar beyond the declared warm-up. The gate flips when `bars_seen >= warmup`,
    and that comparison is made *after* the bar has been counted -- so with exactly
    `warmup` bars fed the strategy becomes warm on the last warm-up bar's own hook, which
    is a bar from before the requested range. The extra bar moves the first tradeable hook
    to the first bar of the range the user actually asked for.
    """
    if warmup_bars < 0:
        raise ValueError(f"warm-up cannot be negative, got {warmup_bars}")
    return start_ms - (warmup_bars + 1) * timeframe_ms(timeframe)


def load_bars(
    root: Path | str,
    symbols: Sequence[str],
    timeframe: str,
    start_ms: int,
    end_ms: int,
) -> tuple[list[BarStep], tuple[str, ...]]:
    """Derived bars for every symbol, grouped by close time, plus any warnings.

    `on_incomplete="flag"` rather than `drop` or `raise`. Dropping an incomplete bucket
    would silently shorten the series and shift every indicator window past the hole;
    raising would make a single missing minute in a year fatal. Flagging keeps the bar --
    with its OHLC intact, only its volume understated -- and returns a warning that reaches
    the run's badge list, which is spec 4.5's "the run says so rather than pretending".
    """
    warnings: list[str] = []
    by_close: dict[int, dict[str, Bar]] = {}
    order = [normalise_symbol(s) for s in symbols]

    for symbol in order:
        table = derive_timeframe(
            root,
            timeframe,
            symbol=symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            dataset=BAR_DATASET,
        )
        if table.num_rows == 0:
            raise FeedError(
                f"no {timeframe} bars for {symbol} in [{start_ms}, {end_ms}). The range "
                f"predates the ingested history, or the lake has never held this symbol."
            )
        columns = {name: table.column(name).to_pylist() for name in table.schema.names}
        incomplete = 0
        for index in range(table.num_rows):
            if not columns["complete"][index]:
                incomplete += 1
            close_time = columns["close_time"][index]
            by_close.setdefault(close_time, {})[symbol] = Bar(
                symbol=symbol,
                open_time=columns["open_time"][index],
                close_time=close_time,
                open=columns["open"][index],
                high=columns["high"][index],
                low=columns["low"][index],
                close=columns["close"][index],
                volume=columns["volume"][index],
                quote_volume=columns["quote_volume"][index],
                trades=columns["count"][index],
            )
        if incomplete:
            warnings.append(
                f"{symbol}: {incomplete} of {table.num_rows} {timeframe} bars were built "
                f"from fewer than the full complement of 1m bars (GAP_SKIPPED). Their OHLC "
                f"is real but their volume is understated."
            )
        # **The gap shape the `complete` flag cannot see** (C9): a bucket with *zero*
        # constituent bars produces no row at all, so it is neither complete nor
        # incomplete -- it is absent, and for one release absence was undetectable. A 1m
        # backtest across a three-hour hole received 07:59 then 11:00 back to back, every
        # indicator window treated them as adjacent, and the run finished with no warning.
        # The calendar knows what the GROUP BY cannot.
        report = missing_buckets(
            root,
            timeframe,
            symbol=symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            dataset=BAR_DATASET,
        )
        if report.missing:
            warnings.append(
                f"{symbol}: {report.describe()} (GAP_SKIPPED). These buckets produced "
                f"no bar at all; on_bar jumps across them and every indicator window "
                f"treats the bars either side as adjacent."
            )

    steps: list[BarStep] = []
    partial = 0
    for close_time in sorted(by_close):
        found = by_close[close_time]
        if len(found) != len(order):
            # A multi-symbol step missing a leg is dropped rather than fed short. Handing
            # `on_bar` a step where one symbol's bar is absent would make a pairs strategy
            # compare this minute against a stale one without any way to notice.
            partial += 1
            continue
        steps.append(
            BarStep(close_time=close_time, bars=tuple(found[s] for s in order))
        )
    if partial:
        warnings.append(
            f"{partial} bar step(s) were dropped because not every symbol had a bar at "
            f"that close time; the strategy never saw a partially-populated step."
        )
    if not steps:
        raise FeedError(
            f"no bar step covers all of {list(order)} in [{start_ms}, {end_ms})"
        )
    return steps, tuple(warnings)


def bar_events(steps: Sequence[BarStep], *, prints: bool = True) -> Iterator[Event]:
    """`TRADE` events for each bar's first and last print, then the `BAR_CLOSE` step.

    Binance's kline `open` *is* the first trade of the bar and its `close` is the last, so
    both are prints with exact timestamps and both are emitted at `EventKind.TRADE`. The
    `high` and `low` are not emitted: nothing in a kline says when they occurred, and a
    fill priced against an undated print is a fill priced against a guess.

    The consequence is the whole reason latency is not zero by default. An order submitted
    at a bar's close arrives after the *next* bar has opened, so it fills at that open --
    a price published after the decision was made. With zero latency it would fill at the
    close print at the same instant, which is the price the strategy just looked at.

    **`prints=False` above the `BAR_CLOSE` tier**, and it is not an optimisation. A run
    replaying `aggTrades` already has every print of the bar, dated exactly; emitting the
    kline's open and close *as well* would interleave two accounts of the same two trades,
    and the kline's would win whenever it sorted later within the millisecond. The engine
    would then price a fill against a bar summary while claiming to be at a tick tier.
    """
    seq = 0
    for step in steps:
        if prints:
            for bar in step.bars:
                seq += 1
                yield Event(
                    ts_ms=bar.open_time,
                    kind=EventKind.TRADE,
                    source_seq=seq,
                    dataset_id=BAR_DATASET,
                    payload=(bar.symbol, bar.open),
                )
            for bar in step.bars:
                seq += 1
                yield Event(
                    ts_ms=bar.close_time,
                    kind=EventKind.TRADE,
                    source_seq=seq,
                    dataset_id=BAR_DATASET,
                    payload=(bar.symbol, bar.close),
                )
        seq += 1
        yield Event(
            ts_ms=step.close_time,
            kind=EventKind.BAR_CLOSE,
            source_seq=seq,
            dataset_id=BAR_DATASET,
            payload=step,
        )


# -------------------------------------------------------------------------------- marks


def load_marks(
    root: Path | str,
    symbols: Sequence[str],
    start_ms: int,
    end_ms: int,
) -> tuple[list[MarkBar], tuple[str, ...]]:
    """1-minute mark bars for every symbol, in `(close_time, symbol)` order.

    Ordered by close time first so the result is already a valid event stream. The
    secondary sort is the symbol name rather than the caller's symbol order: this stream's
    events are ordered by `source_seq`, which is a row index, so a stable and
    self-describing ordering is what keeps the same range producing the same sequence
    whichever order the caller listed its symbols in.

    Raises when a symbol has no mark data at all. Spec 3.4 forbids computing mark price,
    and `Account` refuses to settle funding or liquidate against an unrecorded mark, so a
    run without marks is a run in which nothing can be liquidated -- an equity curve with
    no downside bound, which spec 4.2 calls out as the failure that looks like a good
    result.
    """
    warnings: list[str] = []
    wanted = [normalise_symbol(s) for s in symbols]
    placeholders = ", ".join("?" for _ in wanted)
    # One bar of lead-in, so last-observation-carried-forward has an anchor *at* `start_ms`
    # rather than only from the first in-range close. Without it the earliest mark in a run
    # lands at `start_ms + 59_999`, and anything needing a price in that first minute -- a
    # funding settlement above all, which lands on an 8-hour boundary that `warmup_start_ms`
    # routinely aligns with -- found no mark and was dropped.
    scan_start = start_ms - _MARK_INTERVAL_MS
    # `partition_predicate` prunes by *path* before any file is opened. Filtering on the
    # timestamp column alone leaves DuckDB to open every footer in the lake and consult its
    # statistics: measured at 30 of 2 357 files read with the predicate and all 2 357
    # without, for the same 1 440 rows.
    pruning = partition_predicate(MARK_DATASET, start_ms=scan_start, end_ms=end_ms)
    sql = f"""
        SELECT "symbol", "close_time", "high", "low", "close"
        FROM "{MARK_DATASET}"
        WHERE {pruning}
          AND "symbol" IN ({placeholders})
          AND "open_time" >= ? AND "open_time" < ?
        ORDER BY "close_time", "symbol"
    """
    table = query(
        root,
        sql,
        datasets=(MARK_DATASET,),
        params=[*wanted, int(scan_start), int(end_ms)],
    )
    columns = {name: table.column(name).to_pylist() for name in table.schema.names}

    marks = [
        MarkBar(
            symbol=columns["symbol"][index],
            close_time=columns["close_time"][index],
            high=columns["high"][index],
            low=columns["low"][index],
            close=columns["close"][index],
        )
        for index in range(table.num_rows)
    ]

    seen = {mark.symbol for mark in marks}
    missing = [symbol for symbol in wanted if symbol not in seen]
    if missing:
        raise FeedError(
            f"no mark price for {missing} in [{start_ms}, {end_ms}). Spec 3.4 forbids "
            f"deriving mark price from trades, and a run whose positions cannot be "
            f"liquidated reports an equity curve with no downside bound."
        )

    # Counted against the *requested* range, not the widened scan: the lead-in bar is an
    # anchor for LOCF, not data the run asked for, and charging it to the expectation would
    # report a one-sample shortfall on every complete lake. The filter below is what makes
    # the comment true (M24): `marks` still holds the lead-in bar (it closes at
    # `start_ms - 1`), and counting it cancelled exactly one missing in-range sample --
    # a range short by one mark bar produced no warning and held the mark flat across the
    # hole with liquidation unchecked.
    expected = (end_ms - start_ms) // _MARK_INTERVAL_MS
    counts = Counter(mark.symbol for mark in marks if mark.close_time >= start_ms)
    for symbol in wanted:
        found = counts[symbol]
        if expected and found < expected:
            warnings.append(
                f"{symbol}: {expected - found} of {expected} expected 1m mark samples are "
                f"missing (GAP_SKIPPED). Mark price is held flat across each hole "
                f"(spec 3.4 LOCF), so liquidation is checked less often there."
            )
    return marks, tuple(warnings)


def mark_events(marks: Sequence[MarkBar]) -> Iterator[Event]:
    """One `MARK_PRICE_UPDATE` per mark bar, carrying the close and the traversed range."""
    for seq, mark in enumerate(marks):
        yield Event(
            ts_ms=mark.close_time,
            kind=EventKind.MARK_PRICE_UPDATE,
            source_seq=seq,
            dataset_id=MARK_DATASET,
            payload=mark,
        )


# ------------------------------------------------------------------------------ funding


def load_funding(
    root: Path | str,
    symbols: Sequence[str],
    start_ms: int,
    end_ms: int,
) -> tuple[list[FundingPoint], tuple[str, ...]]:
    """Realised funding settlements from the historical record.

    Spec 3.5 rule 2: use the actual rates and the actual timestamps. Rule 3: do not assume
    an 8-hour schedule -- Binance runs different intervals on different symbols and has
    changed the interval on existing symbols (R17). Both rules reduce to "read the rows",
    which is what this does; the settlement cadence is whatever the data says it was.

    An empty result is a warning rather than an error. Funding is a real cashflow and its
    absence overstates a short's PnL and understates a long's, but a range with no funding
    rows is a legible state -- an un-backfilled dataset -- and refusing to run would block
    a backtest over a range whose bars are perfectly good.
    """
    warnings: list[str] = []
    wanted = [normalise_symbol(s) for s in symbols]
    placeholders = ", ".join("?" for _ in wanted)
    sql = f"""
        SELECT "symbol", "calc_time", "funding_rate"
        FROM "{FUNDING_DATASET}"
        WHERE "symbol" IN ({placeholders})
          AND "calc_time" >= ? AND "calc_time" < ?
        ORDER BY "calc_time", "symbol"
    """
    table = query(
        root,
        sql,
        datasets=(FUNDING_DATASET,),
        params=[*wanted, int(start_ms), int(end_ms)],
    )
    columns = {name: table.column(name).to_pylist() for name in table.schema.names}
    points = [
        FundingPoint(
            symbol=columns["symbol"][index],
            ts_ms=columns["calc_time"][index],
            rate=columns["funding_rate"][index],
        )
        for index in range(table.num_rows)
    ]

    for symbol in wanted:
        if not any(point.symbol == symbol for point in points):
            warnings.append(
                f"{symbol}: no funding settlements in the range (FUNDING_MISSING). A perp "
                f"backtest without funding overstates shorts and understates longs; "
                f"backfill the funding dataset before trusting the attribution."
            )
    return points, tuple(warnings)


def funding_events(points: Sequence[FundingPoint]) -> Iterator[Event]:
    for seq, point in enumerate(points):
        yield Event(
            ts_ms=point.ts_ms,
            kind=EventKind.FUNDING_SETTLEMENT,
            source_seq=seq,
            dataset_id=FUNDING_DATASET,
            payload=point,
        )
