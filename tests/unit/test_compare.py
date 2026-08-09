"""Multi-run comparison: alignment, refusals, correlations, the naive blend (spec 9.6)."""

from __future__ import annotations

import pytest

from perplab.lab.compare import ComparisonError, compare_runs

HOUR = 3_600_000
BASE = 1_709_251_200_000


def _entry(run_id: int, equity: list[float], *, times: list[int] | None = None, **overrides):
    boundaries = times if times is not None else [BASE + i * HOUR for i in range(len(equity))]
    entry = {
        "run_id": run_id,
        "label": f"run {run_id}",
        "start_ms": BASE,
        "end_ms": BASE + 12 * HOUR,
        "grid_times": boundaries,
        "grid_equity": equity,
        "grid_returns": [
            # Guarded like the module itself: a ruined test fixture (equity holding
            # zeros) must not crash the helper on a field `compare_runs` never reads.
            equity[i] / equity[i - 1] - 1.0 if equity[i - 1] > 0 else None
            for i in range(1, len(equity))
        ],
        "metrics": {"sharpe": 1.0},
    }
    entry.update(overrides)
    return entry


def test_mismatched_ranges_are_refused_naming_both_runs() -> None:
    """Spec 9.6: block the comparison rather than produce a misleading overlay."""
    with pytest.raises(ComparisonError, match="run 2 covers"):
        compare_runs(
            [
                _entry(1, [100.0, 101.0, 102.0, 103.0]),
                _entry(2, [100.0, 101.0, 102.0, 103.0], end_ms=BASE + 13 * HOUR),
            ]
        )


def test_fewer_than_two_runs_is_refused() -> None:
    with pytest.raises(ComparisonError, match="at least two"):
        compare_runs([_entry(1, [100.0, 101.0, 102.0])])


def test_perfectly_opposite_runs_correlate_at_minus_one() -> None:
    """Anti-correlation is a claim about *returns*, so the anti-path is built from the
    negated returns -- `C / equity` is not it (1/(1+r) - 1 is only approximately -r)."""
    up = [100.0, 101.0, 100.0, 102.0, 100.0]
    up_returns = [up[i] / up[i - 1] - 1.0 for i in range(1, len(up))]
    down = [100.0]
    for r in up_returns:
        down.append(down[-1] * (1.0 - r))
    result = compare_runs([_entry(1, up), _entry(2, down)])
    matrix = result["correlation"]["matrix"]
    assert matrix[0][0] == 1.0
    assert matrix[0][1] == pytest.approx(-1.0, abs=1e-6)
    assert matrix[1][0] == pytest.approx(-1.0, abs=1e-6)


def test_a_flat_run_correlates_with_nothing_rather_than_zero() -> None:
    """Zero variance has no correlation; 0.0 would claim independence as a finding."""
    result = compare_runs(
        [_entry(1, [100.0, 101.0, 100.0, 102.0]), _entry(2, [50.0] * 4)]
    )
    assert result["correlation"]["matrix"][0][1] is None


def test_a_ruined_run_correlates_over_its_defined_returns_only() -> None:
    """A return over a non-positive prior equity is undefined -- there is no balance for
    a percentage to be a percentage of -- and must be excluded pairwise, not written 0.0.

    Run 1 is ruined at the third boundary: equity [100, 50, 0, 0, 0], returns
    [-0.5, -1.0, ?, ?]. Run 2 lives the whole range: [100, 120, 108, 129.6, 116.64],
    returns [0.2, -0.1, 0.2, -0.1] exactly. Over the two boundaries where both are
    defined, the points (-0.5, 0.2) and (-1.0, -0.1) are two distinct points, and two
    points are always perfectly collinear -- both series fall together, so r = +1.0.

    The buggy version substituted 0.0 for run 1's dead tail and correlated
    x = [-0.5, -1, 0, 0] with y = [0.2, -0.1, 0.2, -0.1]: means -3/8 and 1/20,
    covariance 3/40, variances 11/16 and 9/100, r = (3/40)/(3*sqrt(11)/40) =
    1/sqrt(11) ~= 0.3015 -- a fabricated number claiming the ruined run kept trading
    flat. The overlay, by contrast, keeps every boundary: drawing the dead run at zero
    is a true statement about equity.
    """
    ruined = _entry(1, [100.0, 50.0, 0.0, 0.0, 0.0])
    alive = _entry(2, [100.0, 120.0, 108.0, 129.6, 116.64])
    result = compare_runs([ruined, alive])
    assert result["correlation"]["matrix"][0][1] == pytest.approx(1.0)
    assert result["correlation"]["matrix"][1][0] == pytest.approx(1.0)
    # The basis travels with the coefficient: 2 defined pairs, not the grid's 4 -- a
    # 1.0 earned over two boundaries must not read like one earned over the range.
    assert result["correlation"]["defined_returns"][0][1] == 2
    assert result["correlation"]["defined_returns"][1][1] == 4
    # The equity overlay is untouched by the exclusion.
    assert result["curves"][0]["equity_normalised"] == pytest.approx(
        [1.0, 0.5, 0.0, 0.0, 0.0]
    )


def test_a_run_ruined_at_the_first_step_correlates_with_nothing() -> None:
    """One defined return is not evidence of co-movement, and `pearson` refuses
    single points -- but only if the dead tail reaches it as *absent*, not as 0.0.

    Run 1: equity [100, 0, 0, 0, 0], returns [-1.0, ?, ?, ?]. Only one boundary pair
    is defined against run 2, so the correlation must be None. The buggy version
    correlated x = [-1, 0, 0, 0] with y = [0.2, -0.1, 0.2, -0.1]: means -1/4 and 1/20,
    covariance -3/20, variances 3/4 and 9/100, r = (-3/20)/(3*sqrt(3)/20) =
    -1/sqrt(3) ~= -0.577 -- a confident anti-correlation manufactured entirely from
    padding.
    """
    ruined = _entry(1, [100.0, 0.0, 0.0, 0.0, 0.0])
    alive = _entry(2, [100.0, 120.0, 108.0, 129.6, 116.64])
    result = compare_runs([ruined, alive])
    assert result["correlation"]["matrix"][0][1] is None
    assert result["correlation"]["defined_returns"][0][1] == 1


def test_curves_are_normalised_and_the_blend_is_their_mean() -> None:
    result = compare_runs(
        [_entry(1, [100.0, 110.0, 120.0]), _entry(2, [200.0, 200.0, 260.0])]
    )
    first, second = (c["equity_normalised"] for c in result["curves"])
    assert first == pytest.approx([1.0, 1.1, 1.2])
    assert second == pytest.approx([1.0, 1.0, 1.3])
    assert result["combined"]["equity_normalised"] == pytest.approx([1.0, 1.05, 1.25])
    assert "no rebalancing" in result["combined"]["definition"]


def test_alignment_takes_the_intersection_of_boundaries() -> None:
    """A run whose series starts one boundary late still compares, on the shared part."""
    times_full = [BASE + i * HOUR for i in range(5)]
    late = _entry(2, [100.0, 101.0, 102.0, 103.0], times=times_full[1:])
    full = _entry(1, [100.0, 101.0, 102.0, 103.0, 104.0], times=times_full)
    result = compare_runs([full, late])
    assert result["boundaries"] == times_full[1:]
    assert len(result["curves"][0]["equity_normalised"]) == 4


def test_too_few_shared_boundaries_is_refused() -> None:
    with pytest.raises(ComparisonError, match="fewer than three"):
        compare_runs(
            [
                _entry(1, [100.0, 101.0], times=[BASE, BASE + HOUR]),
                _entry(2, [100.0, 101.0], times=[BASE, BASE + HOUR]),
            ]
        )
