"""Deterministic synthetic market data for the validator's smoke run (spec 5.5 step 5).

Not a market simulator and not trying to be. Its job is to be *shaped* like market data --
positive prices, `high >= max(open, close)`, advancing timestamps, non-zero volume, a
mixture of up and down bars so crossovers actually occur -- so that running a strategy over
it exercises the code paths a real run would, and does so in under a second.

**Everything derives from an explicit seed, and only from `random.Random`.** Not
`random.gauss` (whose internal caching has changed across CPython versions), not
`numpy.random`, not the module-level `random` functions (which share global state with
anything else in the process). The determinism probe compares two runs in two *separate*
interpreters with deliberately different `PYTHONHASHSEED` values, so anything here that
varied between processes would report every strategy as nondeterministic.
"""

from __future__ import annotations

import random

from perplab.core.money import SCALE
from perplab.core.types import Bar, DepthSnapshot
from perplab.engine.ticks import TradePrint

__all__ = [
    "SYNTHETIC_START_MS",
    "SYNTHETIC_SYMBOL",
    "SYNTHETIC_START_PRICE",
    "synthetic_bars",
    "synthetic_trades",
    "synthetic_depth",
    "synthetic_funding",
]

SYNTHETIC_START_MS = 1_704_067_200_000
"""2024-01-01T00:00:00Z. A round UTC day boundary, so session-anchored indicators such as
VWAP see a clean first session rather than starting mid-session."""

SYNTHETIC_SYMBOL = "BTCUSDT"
SYNTHETIC_START_PRICE = 30_000.0

_MAX_STEP = 0.004
"""Per-bar move, +/-0.4%. Large enough that a 12/26 EMA pair actually crosses within a few
hundred bars -- a flatter series would let a crossover strategy pass the smoke run without
its entry path ever executing."""


def _scale(value: float) -> int:
    return int(round(value * SCALE))


def _symbol_salt(symbol: str) -> int:
    """A per-symbol seed offset that is stable across processes.

    Not `hash(symbol)`: Python salts string hashing with `PYTHONHASHSEED`, and the
    determinism probe runs the two smoke passes with *different* values of it. Using the
    builtin here would generate different synthetic data in the two passes and report every
    multi-symbol strategy as nondeterministic.
    """
    return sum((index + 1) * ord(char) for index, char in enumerate(symbol)) & 0xFFFF


def synthetic_bars(
    count: int,
    *,
    symbol: str = SYNTHETIC_SYMBOL,
    timeframe_ms: int = 900_000,
    start_ms: int = SYNTHETIC_START_MS,
    seed: int = 20260803,
    start_price: float = SYNTHETIC_START_PRICE,
) -> list[Bar]:
    """A random walk of closed bars, byte-identical for a given `(count, seed, ...)`.

    The walk is generated forward from `start_price`, and each bar's `open` is the previous
    bar's `close`. Gapless by construction: a synthetic gap would exercise the engine's
    gap handling during *validation*, which is not what validation is for.
    """
    if count < 1:
        raise ValueError(f"count must be >= 1, got {count}")
    if timeframe_ms <= 0:
        raise ValueError(f"timeframe_ms must be positive, got {timeframe_ms}")

    rng = random.Random(seed)
    bars: list[Bar] = []
    price = start_price
    for index in range(count):
        open_price = price
        close_price = open_price * (1.0 + (rng.random() - 0.5) * 2.0 * _MAX_STEP)
        top = max(open_price, close_price) * (1.0 + rng.random() * 0.002)
        bottom = min(open_price, close_price) * (1.0 - rng.random() * 0.002)
        volume = 10.0 + rng.random() * 90.0

        open_time = start_ms + index * timeframe_ms
        scaled_volume = _scale(volume)
        scaled_close = _scale(close_price)
        bars.append(
            Bar(
                symbol=symbol,
                open_time=open_time,
                # Binance's close time is the last millisecond *inside* the interval, not
                # the next interval's open. Off by one here and every bar would appear to
                # overlap its successor, which is precisely the boundary condition the
                # no-look-ahead guarantee turns on (spec 3.1).
                close_time=open_time + timeframe_ms - 1,
                open=_scale(open_price),
                high=_scale(top),
                low=_scale(bottom),
                close=scaled_close,
                volume=scaled_volume,
                quote_volume=scaled_volume * scaled_close // SCALE,
                trades=1 + int(volume),
            )
        )
        price = close_price
    return bars


def synthetic_trades(
    bar: Bar, *, per_bar: int = 4, seed: int = 20260803
) -> list[TradePrint]:
    """Aggregate trades inside one bar, for strategies that implement `on_tick`.

    Seeded from the bar's open time as well as `seed`, so the trades for bar `i` do not
    depend on how many bars were generated before it. That independence is what lets the
    look-ahead truncation test compare a 500-bar run against a 400-bar one: the shared
    prefix has to be identical, and a stream whose values depended on the total count would
    not be.

    **`TradePrint`, because that is what the engine's `on_tick` receives.** These built
    `core.types.AggTrade` until the Phase 5 review, and the two types disagree about what
    `price` and `qty` *mean* -- `AggTrade` stores the lake's scaled integers under those
    names, `TradePrint` puts floats there and the integers under `price_scaled`/`qty_scaled`.
    The validator therefore rejected strategies written against the documented engine
    contract (`AttributeError: 'AggTrade' object has no attribute 'consumes_bids'`) and
    passed strategies whose `trade.price` threshold was wrong by a factor of 10^8 at run
    time -- a green validation followed by a run that means something else, which is the
    exact failure spec 5.5 exists to close.
    """
    rng = random.Random(seed ^ bar.open_time)
    span = max(bar.close_time - bar.open_time, 1)
    low = bar.low / SCALE
    high = bar.high / SCALE
    trades: list[TradePrint] = []
    for index in range(per_bar):
        price = low + (high - low) * rng.random()
        qty = 0.01 + rng.random() * 0.5
        ts = bar.open_time + (span * (index + 1)) // (per_bar + 1)
        trades.append(
            TradePrint(
                symbol=bar.symbol,
                ts_ms=ts,
                price_scaled=_scale(price),
                qty_scaled=_scale(qty),
                is_buyer_maker=rng.random() < 0.5,
                agg_id=bar.open_time // 1000 * 100 + index,
            )
        )
    return trades


def synthetic_depth(bar: Bar, *, levels: int = 20, seed: int = 20260803) -> DepthSnapshot:
    """A 20-level book centred on the bar's close, for depth-driven indicators.

    Seeded from the symbol as well as the time. Without the symbol, every instrument in a
    multi-symbol run got byte-identical bid and ask sizes -- only the prices differed --
    so `BookImbalance` produced the *same series* for all of them and a pairs strategy
    comparing two books saw one.
    """
    rng = random.Random(seed ^ (bar.open_time + 1) ^ _symbol_salt(bar.symbol))
    mid = bar.close / SCALE
    tick = max(mid * 0.00001, 0.1)
    bid_px: list[int] = []
    bid_qty: list[int] = []
    ask_px: list[int] = []
    ask_qty: list[int] = []
    for level in range(levels):
        bid_px.append(_scale(mid - tick * (level + 1)))
        ask_px.append(_scale(mid + tick * (level + 1)))
        bid_qty.append(_scale(0.5 + rng.random() * 5.0))
        ask_qty.append(_scale(0.5 + rng.random() * 5.0))
    return DepthSnapshot(
        symbol=bar.symbol,
        ts_ms=bar.close_time,
        recv_ms=bar.close_time,
        last_update_id=bar.open_time,
        bid_px=tuple(bid_px),
        bid_qty=tuple(bid_qty),
        ask_px=tuple(ask_px),
        ask_qty=tuple(ask_qty),
    )


def synthetic_funding(bar: Bar, *, seed: int = 20260803) -> str:
    """A funding rate for a settlement landing inside this bar, as a decimal string.

    Centred on zero and bounded by +/-0.05%, which is Binance's cap for most USD-M perps.
    Sign varies, so a strategy branching on funding direction sees both branches.

    Returned as text, not as a float, because it has two consumers on opposite sides of the
    seam: `FundingMean` wants a float and the settlement cashflow wants an exact `Money`.
    Handing out a float would force the accounting side to construct a `Decimal` from
    binary floating point -- the one conversion `core.money` exists to prevent.
    """
    rng = random.Random(seed ^ (bar.open_time + 2))
    return f"{(rng.random() - 0.5) * 0.001:.8f}"
