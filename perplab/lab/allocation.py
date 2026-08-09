"""Allocation modes for portfolio backtests (spec 9.5).

Four ways to split a notional budget across symbols. All four are pure arithmetic over
inputs the caller supplies, and that is a design decision rather than a shortcut: an
inverse-volatility weight is only causal if the volatilities were computed causally, and
the only party who can guarantee that is the caller holding the indicator (a strategy's
`ctx.indicators.realised_volatility`, or the run form sampling trailing vol from data
*before* the range). This module never reads a price, so it cannot leak one.

**Binding into a run happens through strategy parameters, not behind the strategy's
back.** The platform does not scale a strategy's orders silently -- a sizing the author
did not write producing fills the author cannot explain is exactly the "quietly wrong
answer" rule spec 1.4 forbids. The portfolio template passes the chosen mode's weights in
as params, and the strategy sizes with them in its own code, visible in its own source.
"""

from __future__ import annotations

from typing import Mapping, Sequence

__all__ = [
    "equal_notional",
    "inverse_volatility",
    "fixed_fractional",
    "custom_weights",
]


def _check_symbols(symbols: Sequence[str]) -> list[str]:
    if not symbols:
        raise ValueError("no symbols to allocate across")
    if len(set(symbols)) != len(symbols):
        raise ValueError(f"duplicate symbols in {list(symbols)}")
    return list(symbols)


def _check_budget(total_notional: float) -> float:
    if total_notional <= 0:
        raise ValueError(f"total_notional must be positive, got {total_notional}")
    return float(total_notional)


def equal_notional(
    symbols: Sequence[str], total_notional: float
) -> dict[str, float]:
    """The budget split evenly. The baseline every other mode is compared against."""
    names = _check_symbols(symbols)
    budget = _check_budget(total_notional)
    share = budget / len(names)
    return {symbol: share for symbol in names}


def inverse_volatility(
    volatilities: Mapping[str, float], total_notional: float
) -> dict[str, float]:
    """Weights proportional to `1 / vol`: every symbol contributes similar risk.

    Volatilities must be positive and must have been computed causally by the caller --
    trailing realised vol as of the allocation instant, never over the range about to be
    traded. A zero or negative vol is refused rather than clamped: a symbol whose
    volatility estimate is zero would swallow the entire budget, and an estimate that
    broken means the input window is wrong, not that the symbol is riskless.
    """
    if not volatilities:
        raise ValueError("no volatilities to weight by")
    budget = _check_budget(total_notional)
    for symbol, vol in volatilities.items():
        if vol <= 0:
            raise ValueError(
                f"{symbol} has non-positive volatility {vol}; an inverse-volatility "
                f"weight would be infinite. Check the estimation window."
            )
    inverse = {symbol: 1.0 / vol for symbol, vol in volatilities.items()}
    total = sum(inverse.values())
    return {symbol: budget * value / total for symbol, value in inverse.items()}


def fixed_fractional(
    symbols: Sequence[str], equity: float, fraction: float
) -> dict[str, float]:
    """Each symbol gets `equity * fraction` of notional.

    The one mode whose total scales with symbol count -- ten symbols at 10% is 100% of
    equity in aggregate notional. That is the mode's own meaning (per-position risk
    budget, not a portfolio budget), so it is documented rather than silently
    renormalised; the risk layer's `total_notional_pct` is the guard that catches an
    aggregate the account cannot carry.
    """
    names = _check_symbols(symbols)
    if equity <= 0:
        raise ValueError(f"equity must be positive, got {equity}")
    if not 0 < fraction <= 1:
        raise ValueError(f"fraction must be in (0, 1], got {fraction}")
    per_symbol = float(equity) * float(fraction)
    return {symbol: per_symbol for symbol in names}


def custom_weights(
    weights: Mapping[str, float], total_notional: float
) -> dict[str, float]:
    """The caller's own weights, normalised to sum to one before applying the budget.

    Negative weights are refused: a short *allocation* is not a short *position* -- the
    strategy decides direction, the allocation decides size -- and a negative budget
    share has no meaning the fill path could honour.
    """
    if not weights:
        raise ValueError("no weights given")
    budget = _check_budget(total_notional)
    for symbol, weight in weights.items():
        if weight < 0:
            raise ValueError(
                f"{symbol} has negative weight {weight}; direction belongs to the "
                f"strategy, not the allocation"
            )
    total = sum(weights.values())
    if total <= 0:
        raise ValueError("weights sum to zero; nothing would be allocated")
    return {symbol: budget * weight / total for symbol, weight in weights.items()}
