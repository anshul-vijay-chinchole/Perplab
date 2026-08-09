"""Portfolio analysis of a multi-symbol run (spec 9.5).

The engine already does the hard half of spec 9.5 by construction: one event loop across
all symbols, one shared account, one shared risk budget, and under isolated margin one
shared wallet that a liquidation on any symbol drains for all the others. This module is
the *reporting* half -- the per-symbol decomposition of what that shared account did:

- **Correlation matrix** of per-symbol PnL changes on the metric grid.
- **Rolling correlation** per pair, because spec 9.5's warning is precise: "correlations
  converge to 1 in crashes, which is exactly when it matters -- a static matrix hides
  this". The static matrix is still shown; the rolling series is what stops it lying.
- **Symbol-selection bias warning** -- if every symbol in the run is a currently-liquid
  major, the results page says so. Robustness claims require at least one symbol that
  went through a genuinely bad period.

Correlations are over per-period PnL *changes*, not PnL levels. Cumulative series are
near-monotonic whenever a strategy is profitable, and two rising lines correlate at ~1.0
regardless of whether their day-to-day fortunes are related -- the level correlation
answers "did both make money", which nobody asked.
"""

from __future__ import annotations

from bisect import bisect_right
from typing import Any, Mapping, Sequence

from perplab.analytics.metrics import grid_step
from perplab.lab.stats import pearson

__all__ = ["LIQUID_MAJORS", "portfolio_report"]

LIQUID_MAJORS = frozenset(
    {
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "LTCUSDT", "AVAXUSDT", "LINKUSDT",
    }
)
"""Symbols that are, as of this writing, top-of-book liquid USD-M majors.

A judgement, recorded as a constant so it is visible and editable rather than implied. A
portfolio drawn only from this set has never traded through a delisting scare, a depeg or
a liquidity collapse, and spec 9.5 requires the results page to say so in as many words.
"""

DEFAULT_ROLLING_WINDOW = 30
"""Grid periods per rolling-correlation window: a month of daily periods."""


def _resample_locf(
    times: Sequence[int], values: Sequence[float], boundaries: Sequence[int]
) -> list[float]:
    """Last observation at or before each boundary; 0.0 before the first sample.

    The same LOCF rule `build_grid` applies to equity. Zero before the first sample is
    exact here rather than invented: a PnL series is zero before anything has traded.
    """
    out: list[float] = []
    for boundary in boundaries:
        index = bisect_right(times, boundary) - 1
        out.append(values[index] if index >= 0 else 0.0)
    return out


def portfolio_report(
    *,
    times: Sequence[int],
    symbol_pnl: Mapping[str, Sequence[float]],
    start_ms: int,
    end_ms: int,
    round_trips: Mapping[str, int] | None = None,
    rolling_window: int = DEFAULT_ROLLING_WINDOW,
) -> dict[str, Any]:
    """The spec 9.5 report over one run's per-symbol PnL series.

    `times` and each `symbol_pnl` series share indices (the engine samples them
    together); `round_trips` carries how many round-trips each symbol closed, so a
    symbol whose flat series means "never traded" is labelled as such rather than
    entering the matrix as a row of `None`s with no explanation.
    """
    if len(symbol_pnl) < 2:
        raise ValueError(
            "a portfolio report needs at least two symbols; a single-symbol run's "
            "PnL curve is its equity curve, already on the run page"
        )
    for symbol, series in symbol_pnl.items():
        if len(series) != len(times):
            raise ValueError(
                f"{symbol} has {len(series)} samples for {len(times)} timestamps"
            )
    if rolling_window < 3:
        raise ValueError(f"rolling_window must be at least 3, got {rolling_window}")

    label, periods_per_year, step = grid_step(start_ms, end_ms)
    first = -(-start_ms // step) * step
    last = (end_ms // step) * step
    boundaries = list(range(first, last + 1, step))
    if len(boundaries) < 3:
        raise ValueError(
            "the range holds fewer than three grid boundaries; per-symbol correlations "
            "need at least two aligned periods"
        )

    symbols = sorted(symbol_pnl)
    sampled = {
        symbol: _resample_locf(times, symbol_pnl[symbol], boundaries)
        for symbol in symbols
    }
    deltas = {
        symbol: [
            series[i] - series[i - 1] for i in range(1, len(series))
        ]
        for symbol, series in sampled.items()
    }

    matrix: list[list[float | None]] = []
    for a in symbols:
        row: list[float | None] = []
        for b in symbols:
            row.append(1.0 if a == b else pearson(deltas[a], deltas[b]))
        matrix.append(row)

    period_count = len(boundaries) - 1
    rolling: list[dict[str, Any]] = []
    for i, a in enumerate(symbols):
        for b in symbols[i + 1 :]:
            series: list[float | None] = []
            for end in range(rolling_window, period_count + 1):
                series.append(
                    pearson(
                        deltas[a][end - rolling_window : end],
                        deltas[b][end - rolling_window : end],
                    )
                )
            rolling.append(
                {
                    "pair": [a, b],
                    "ts": boundaries[rolling_window:],
                    "correlation": series,
                }
            )

    trips = dict(round_trips or {})
    per_symbol = {
        symbol: {
            # **The series' own last value, not the last grid boundary.** `sampled` is
            # resampled onto whole grid steps, so a run ending 12 hours past its last
            # daily boundary had those 12 hours silently dropped from this figure -- and
            # this is the column a reader reconciles against the run's ledger PnL.
            # `build_grid` makes the same distinction: remainders leave the *return*
            # series and stay in the level series.
            "final_pnl": symbol_pnl[symbol][-1] if symbol_pnl[symbol] else 0.0,
            "final_pnl_at_grid_close": sampled[symbol][-1],
            "round_trips": trips.get(symbol),
            "traded": bool(trips.get(symbol)) or any(v != 0.0 for v in symbol_pnl[symbol]),
        }
        for symbol in symbols
    }

    warnings: list[str] = []
    if all(symbol in LIQUID_MAJORS for symbol in symbols):
        warnings.append(
            "every symbol in this run is a currently-liquid major. None of them has "
            "traded through a delisting scare, a depeg or a liquidity collapse, so this "
            "portfolio's robustness is untested against exactly the conditions that "
            "break portfolios (spec 9.5)."
        )
    untraded = [symbol for symbol, row in per_symbol.items() if not row["traded"]]
    if untraded:
        warnings.append(
            f"{', '.join(untraded)} never traded in this run; their correlation "
            f"entries are undefined rather than zero"
        )

    return {
        "symbols": symbols,
        "grid": label,
        "periods": period_count,
        "rolling_window": rolling_window,
        "correlation": {"symbols": symbols, "matrix": matrix},
        "rolling_correlation": rolling,
        "per_symbol": per_symbol,
        "warnings": warnings,
        "notes": [
            (
                "correlations are over per-period PnL changes on the metric grid, not "
                "over cumulative PnL levels -- two profitable symbols' levels correlate "
                "near 1.0 whether or not their daily fortunes are related"
            ),
            (
                "under isolated margin the positions' margins are siloed but the wallet "
                "is shared: a liquidation on one symbol reduces the equity available to "
                "every other. The engine models this; these curves include it."
            ),
        ],
    }
