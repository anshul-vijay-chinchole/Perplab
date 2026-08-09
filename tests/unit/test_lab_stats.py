"""The Lab's shared statistics, against hand-computed values.

Born from a mutation survivor: every test that touched `sharpe_ratio` used the function
itself as its own oracle, so switching its variance to the population form (`ddof=0`) --
which flatters every small sample -- broke nothing. A statistic's test must state the
number, not re-derive it.
"""

from __future__ import annotations

import math

import pytest

from perplab.lab.stats import pearson, sharpe_ratio


def test_sharpe_uses_the_sample_variance_to_a_stated_number() -> None:
    """Returns (0.01, 0.02, 0.03): mean 0.02, *sample* variance 1e-4 (divide by n-1 = 2),
    so Sharpe = 0.02/0.01 * sqrt(A) = 2*sqrt(A). The population form would divide by 3
    and report 2.449*sqrt(A) -- always the flattering direction."""
    assert sharpe_ratio([0.01, 0.02, 0.03], 365) == pytest.approx(2.0 * math.sqrt(365))


def test_sharpe_undefined_cases_are_none() -> None:
    assert sharpe_ratio([0.01], 365) is None
    assert sharpe_ratio([0.01, 0.01], 365) is None  # zero dispersion


def test_pearson_to_a_stated_number() -> None:
    # (1,2), (2,4), (3,5): cov = 3, var_x = 2, var_y = 4.666..., r = 3/sqrt(9.333...)
    assert pearson([1.0, 2.0, 3.0], [2.0, 4.0, 5.0]) == pytest.approx(
        3.0 / math.sqrt(2.0 * (14.0 / 3.0))
    )
    assert pearson([1.0, 2.0], [3.0, 3.0]) is None  # zero variance refuses
    assert pearson([1.0], [1.0]) is None
    assert pearson([1.0, 2.0], [1.0]) is None
