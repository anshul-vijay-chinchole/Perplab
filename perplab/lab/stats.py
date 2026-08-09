"""Small statistics shared by the Lab tools.

One implementation of each, because two Lab panels disagreeing about what a Sharpe is
would be the platform quietly giving two answers to one question -- the exact failure
spec 1.4 exists to prevent. The formulas match `analytics.metrics` (`ddof=1`, zero
risk-free, `sqrt(A)` annualisation); they are reimplemented here rather than imported
because the metrics module's versions carry a risk-free de-annualisation the Lab's
resampled series deliberately do not use, and threading a zero through a private helper
is a coupling with no payoff.
"""

from __future__ import annotations

import math
from typing import Sequence

__all__ = ["pearson", "sharpe_ratio"]


def pearson(a: Sequence[float], b: Sequence[float]) -> float | None:
    """Pearson correlation, `None` when undefined.

    `None` -- not zero -- for mismatched lengths, fewer than two points, or a
    zero-variance side: a flat series correlates with nothing, and `0.0` would claim
    independence as a finding.
    """
    if len(a) != len(b) or len(a) < 2:
        return None
    n = float(len(a))
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    var_a = sum((x - mean_a) ** 2 for x in a)
    var_b = sum((x - mean_b) ** 2 for x in b)
    if var_a <= 0 or var_b <= 0:
        return None
    cov = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    value = cov / math.sqrt(var_a * var_b)
    return value if math.isfinite(value) else None


def sharpe_ratio(returns: Sequence[float], periods_per_year: int) -> float | None:
    """`mean / stdev(ddof=1) * sqrt(A)`, `None` when undefined (spec 8.2 conventions).

    `None` rather than zero or infinity for the degenerate cases -- fewer than two
    returns, or zero dispersion -- for the same reason `analytics.metrics` refuses them:
    every substitute value sorts somewhere, and an undefined statistic must not rank.
    """
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    if variance <= 0:
        return None
    value = mean / math.sqrt(variance) * math.sqrt(periods_per_year)
    return value if math.isfinite(value) else None
