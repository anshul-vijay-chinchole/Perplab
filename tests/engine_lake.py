"""A tiny, exactly-known lake for engine tests.

The Phase 1 fixture (`tests/integration/sample_lake.py`) publishes real archives and runs
them through the ingest pipeline, because what it is testing *is* that pipeline. Engine
tests want the opposite: a lake whose every bar was chosen by the test, written as directly
as possible, so that a failure means the engine is wrong rather than the ingest.

So this writes Parquet through the platform's own writer -- which keeps the partition layout
and the scaled-int64 convention honest -- from price paths the caller supplies as plain
integers. A test that needs the mark to dip to exactly 40 000 on minute 812 says so, and
gets it.

**Klines and mark klines are written separately and are allowed to differ.** That is the
whole point: spec 3.4 says fills happen at traded prices and risk happens at mark price, and
a fixture that used one series for both could not tell a liquidation checked against the
wrong series from one checked against the right one.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path

from perplab.data.schemas import SCHEMAS
from perplab.data.writer import ParquetBufferedWriter

__all__ = [
    "MS_PER_MINUTE",
    "MS_PER_DAY",
    "Ohlc",
    "write_klines",
    "write_marks",
    "write_funding",
    "write_trades",
    "write_book_ticker",
    "write_depth",
    "build_lake",
    "flat_path",
    "ramp_path",
    "wave_path",
    "scaled",
]

MS_PER_MINUTE = 60_000
MS_PER_DAY = 86_400_000

_SCALE = 10**8


class Ohlc(tuple):
    """`(open, high, low, close)` in scaled int64. A tuple subclass purely for the name."""

    __slots__ = ()

    def __new__(cls, open_: int, high: int, low: int, close: int) -> "Ohlc":
        if not (low <= open_ <= high and low <= close <= high):
            raise ValueError(
                f"OHLC is inconsistent: open {open_} / high {high} / low {low} / close "
                f"{close}. A fixture whose bars are impossible would hide a real failure."
            )
        return super().__new__(cls, (open_, high, low, close))


def flat_path(price: float) -> Callable[[int], Ohlc]:
    """Every bar identical. Isolates whatever the test is varying from price movement."""
    scaled = int(round(price * _SCALE))
    return lambda index: Ohlc(scaled, scaled, scaled, scaled)


def ramp_path(start: float, step: float) -> Callable[[int], Ohlc]:
    """A straight line, one `step` per bar, with the high and low hugging it.

    Deterministic and monotonic, so a test can state the price at bar *n* in one line of
    arithmetic instead of reading it back out of the fixture.
    """

    def build(index: int) -> Ohlc:
        open_ = int(round((start + step * index) * _SCALE))
        close = int(round((start + step * (index + 1)) * _SCALE))
        return Ohlc(open_, max(open_, close), min(open_, close), close)

    return build


def wave_path(base: float, amplitude: float, period_bars: int) -> Callable[[int], Ohlc]:
    """A triangular wave in exact integer arithmetic.

    Triangular rather than sinusoidal because every value is an exact multiple of a
    fraction of the amplitude, so the series is bit-identical on any machine -- `math.sin`
    is not guaranteed to be. A wave is what a crossover strategy needs: a monotonic ramp
    never crosses its own moving average, so a test built on one cannot tell a working
    crossover from a strategy that ignores its indicator entirely.
    """
    if period_bars < 2:
        raise ValueError(f"a wave needs at least two bars per period, got {period_bars}")
    scaled_base = int(round(base * _SCALE))
    scaled_amp = int(round(amplitude * _SCALE))
    half = period_bars // 2

    def value(index: int) -> int:
        phase = index % period_bars
        if phase <= half:
            return scaled_base + scaled_amp * phase // half
        return scaled_base + scaled_amp * (period_bars - phase) // (period_bars - half)

    def build(index: int) -> Ohlc:
        open_ = value(index)
        close = value(index + 1)
        return Ohlc(open_, max(open_, close), min(open_, close), close)

    return build


def _write_bars(
    market_root: Path,
    dataset: str,
    symbol: str,
    start_ms: int,
    minutes: int,
    path: Callable[[int], Ohlc],
) -> None:
    writer = ParquetBufferedWriter(
        market_root, dataset, SCHEMAS[dataset], symbol=symbol, max_rows=10**9
    )
    for index in range(minutes):
        open_time = start_ms + index * MS_PER_MINUTE
        open_, high, low, close = path(index)
        writer.append(
            {
                "open_time": open_time,
                "close_time": open_time + MS_PER_MINUTE - 1,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 100 * _SCALE,
                "quote_volume": 100 * close,
                "count": 500,
                "taker_buy_volume": 50 * _SCALE,
                "taker_buy_quote_volume": 50 * close,
            }
        )
    writer.flush()


def write_klines(
    market_root: Path,
    symbol: str,
    start_ms: int,
    minutes: int,
    path: Callable[[int], Ohlc],
) -> None:
    _write_bars(market_root, "klines", symbol, start_ms, minutes, path)


def write_marks(
    market_root: Path,
    symbol: str,
    start_ms: int,
    minutes: int,
    path: Callable[[int], Ohlc],
) -> None:
    _write_bars(market_root, "markPriceKlines", symbol, start_ms, minutes, path)


def write_funding(
    market_root: Path,
    symbol: str,
    settlements: Sequence[tuple[int, float]],
    *,
    interval_hours: int = 8,
) -> None:
    """`settlements` is `(calc_time_ms, rate)`. An empty sequence writes nothing at all,
    which is a legitimate fixture: a range with no settlement in it."""
    if not settlements:
        return
    writer = ParquetBufferedWriter(
        market_root, "funding", SCHEMAS["funding"], symbol=symbol, max_rows=10**9
    )
    for ts, rate in settlements:
        writer.append(
            {
                "calc_time": ts,
                "funding_interval_hours": interval_hours,
                "funding_rate": int(round(rate * _SCALE)),
            }
        )
    writer.flush()


# ---------------------------------------------------------------------------- tick data
#
# The three tick datasets are written **verbatim from what the test lists**, with no path
# generators. Bars can be described by a shape -- a ramp, a wave -- because a strategy reads
# hundreds of them and only their shape matters. A golden scenario reads three or four ticks
# and every one of them is load-bearing: the size resting at a level, which side an
# aggressor hit, whether a print landed at the limit or one tick through it. A generator
# would put those between the test and the number it is asserting.


def scaled(value: float) -> int:
    """A price or quantity in the lake's 10^8 scaling."""
    return int(round(value * _SCALE))


def write_trades(
    market_root: Path,
    symbol: str,
    trades: Sequence[tuple[int, float, float, bool]],
) -> None:
    """`aggTrades` rows as `(ts_ms, price, qty, is_buyer_maker)`.

    `is_buyer_maker=True` means the buyer was the maker, so the trade was **sell-aggressive**
    and consumed bid-side queue. Written out in full at each call site in the scenarios
    rather than hidden behind a helper called `sell()`, because the whole queue model turns
    on this flag and a test that abbreviates it can be read as asserting the opposite of what
    it asserts.
    """
    if not trades:
        return
    writer = ParquetBufferedWriter(
        market_root, "aggTrades", SCHEMAS["aggTrades"], symbol=symbol, max_rows=10**9
    )
    for index, (ts, price, qty, buyer_maker) in enumerate(trades):
        writer.append(
            {
                "ts_ms": ts,
                "recv_ms": ts,
                "agg_id": index + 1,
                "price": scaled(price),
                "qty": scaled(qty),
                "first_trade_id": index + 1,
                "last_trade_id": index + 1,
                "is_buyer_maker": buyer_maker,
            }
        )
    writer.flush()


def write_book_ticker(
    market_root: Path,
    symbol: str,
    quotes: Sequence[tuple[int, float, float, float, float]],
    *,
    recv_offset_ms: int = 0,
) -> None:
    """`bookTicker` rows as `(ts_ms, bid_px, bid_qty, ask_px, ask_qty)`.

    `recv_offset_ms` is the transport lag the row records: `recv_ms = ts_ms + offset`. The
    default of zero collapses the two clocks, which every legacy golden scenario was
    hand-derived against; a test **about** the visibility gate must pass a positive offset,
    because a fixture whose clocks are equal cannot tell recv-gating from ts-gating at all.
    """
    if not quotes:
        return
    writer = ParquetBufferedWriter(
        market_root, "bookTicker", SCHEMAS["bookTicker"], symbol=symbol, max_rows=10**9
    )
    for index, (ts, bid_px, bid_qty, ask_px, ask_qty) in enumerate(quotes):
        writer.append(
            {
                "ts_ms": ts,
                "recv_ms": ts + recv_offset_ms,
                "update_id": index + 1,
                "bid_px": scaled(bid_px),
                "bid_qty": scaled(bid_qty),
                "ask_px": scaled(ask_px),
                "ask_qty": scaled(ask_qty),
            }
        )
    writer.flush()


def write_depth(
    market_root: Path,
    symbol: str,
    snapshots: Sequence[tuple[int, Sequence[tuple[float, float]], Sequence[tuple[float, float]]]],
    *,
    recv_offset_ms: int = 0,
) -> None:
    """`depth20` rows as `(ts_ms, bids, asks)`, each side a best-first `(price, size)` list.

    `recv_offset_ms` as on `write_book_ticker`: rows are visible at `ts + offset` and aged
    from `ts`, and a gating test needs the two clocks apart to be a test.
    """
    if not snapshots:
        return
    writer = ParquetBufferedWriter(
        market_root, "depth20", SCHEMAS["depth20"], symbol=symbol, max_rows=10**9
    )
    for index, (ts, bids, asks) in enumerate(snapshots):
        writer.append(
            {
                "ts_ms": ts,
                "recv_ms": ts + recv_offset_ms,
                "last_update_id": index + 1,
                "bid_px": [scaled(p) for p, _ in bids],
                "bid_qty": [scaled(q) for _, q in bids],
                "ask_px": [scaled(p) for p, _ in asks],
                "ask_qty": [scaled(q) for _, q in asks],
            }
        )
    writer.flush()


def build_lake(
    market_root: Path,
    *,
    symbol: str = "BTCUSDT",
    start_ms: int = 1_709_251_200_000,
    minutes: int = 24 * 60,
    trade_path: Callable[[int], Ohlc] | None = None,
    mark_path: Callable[[int], Ohlc] | None = None,
    funding: Sequence[tuple[int, float]] = (),
    ticks: Sequence[tuple[int, float, float, bool]] = (),
    quotes: Sequence[tuple[int, float, float, float, float]] = (),
    depth: Sequence[
        tuple[int, Sequence[tuple[float, float]], Sequence[tuple[float, float]]]
    ] = (),
) -> None:
    """Write a complete engine-ready lake.

    `mark_path` defaults to `trade_path`, which is the right default for a test that does
    not care about the distinction -- and stating the default here rather than silently
    reusing one series means a test that *does* care has to say so.

    The three tick arguments default to empty, and an empty tick dataset is what makes a
    range resolve to `BAR_CLOSE`. That is the fixture equivalent of spec 4.2's coverage rule
    and it is how the degradation scenarios are built: the same bars, the same strategy, and
    a lake that simply does not hold what the run asked for.
    """
    trades = trade_path or ramp_path(40_000.0, 1.0)
    marks = mark_path or trades
    write_klines(market_root, symbol, start_ms, minutes, trades)
    write_marks(market_root, symbol, start_ms, minutes, marks)
    write_funding(market_root, symbol, funding)
    write_trades(market_root, symbol, ticks)
    write_book_ticker(market_root, symbol, quotes)
    write_depth(market_root, symbol, depth)
