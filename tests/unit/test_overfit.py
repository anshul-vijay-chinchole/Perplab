"""Overfitting diagnostics: fits, plateau aggregation, sensitivity surface (spec 9.4)."""

from __future__ import annotations

import pytest

from perplab.lab.overfit import _fit, diagnostics
from perplab.lab.sweep import SweepResult
from perplab.lab.walkforward import (
    Fold,
    FoldRecord,
    WalkForwardConfig,
    WalkForwardResult,
)

MS_PER_HOUR = 3_600_000


def _grid_result(index: int, sharpe: float | None, *, ok: bool = True) -> SweepResult:
    return SweepResult(index=index, params={"p": index + 1}, ok=ok, sharpe=sharpe)


def _record(
    index: int,
    *,
    grid_sharpes: list[float | None],
    objective: list[float | None],
    chosen: int | None,
    is_sharpe: float | None = None,
    oos_sharpe: float | None = None,
    oos_annualised: float | None = None,
    plateau: float | None = None,
    unprofitable: bool = False,
) -> FoldRecord:
    fold = Fold(
        index=index,
        is_start_ms=0,
        is_end_ms=(index + 1) * MS_PER_HOUR,
        oos_start_ms=(index + 1) * MS_PER_HOUR,
        oos_end_ms=(index + 2) * MS_PER_HOUR,
    )
    oos = None
    if oos_sharpe is not None or oos_annualised is not None:
        oos = {
            "sharpe": oos_sharpe,
            "annualised_return": oos_annualised,
            "total_return": None,
            "net_pnl": "0",
            "max_drawdown": None,
            "round_trips": 0,
            "fills": 0,
            "flags": [],
            "event_hash": None,
            "equity_ms": [],
            "equity": [],
        }
    return FoldRecord(
        fold=fold,
        grid=tuple(_grid_result(i, s) for i, s in enumerate(grid_sharpes)),
        objective=tuple(objective),
        chosen_index=chosen,
        chosen_params={"p": chosen + 1} if chosen is not None else None,
        is_sharpe=is_sharpe,
        is_total_return=None,
        is_annualised=None,
        is_net_pnl=None,
        is_max_drawdown=None,
        is_round_trips=None,
        oos=oos,
        is_halted=False,
        is_halt_limit=None,
        wfe=None,
        plateau=plateau,
        neighbours_defined=0,
        neighbours_total=0,
        neighbourhood_unprofitable=unprofitable,
        error=None,
    )


def _result(records: list[FoldRecord], grid: dict | None = None) -> WalkForwardResult:
    return WalkForwardResult(
        config=WalkForwardConfig(
            is_ms=MS_PER_HOUR,
            oos_ms=MS_PER_HOUR,
            grid=grid if grid is not None else {"p": [1, 2, 3]},
        ),
        folds=tuple(records),
        stitched_ms=(),
        stitched_equity=(),
        stitched_pnl=(),
        stitched_fold_index=(),
        fold_scales=(),
        truncated_at_fold=None,
        opening_balance=100_000.0,
        oos_days=0.0,
        wfe_aggregate=None,
        wfe_aggregate_definition="",
        wfe_median=None,
        stitched_total_return=None,
        stitched_annualised=None,
        stability={},
        uncovered_ms=0,
        warnings=(),
    )


# -------------------------------------------------------------------------------- fit


def test_fit_recovers_an_exact_line() -> None:
    fit = _fit([(0.0, 1.0), (1.0, 3.0), (2.0, 5.0)])
    assert fit is not None
    assert fit["slope"] == pytest.approx(2.0)
    assert fit["intercept"] == pytest.approx(1.0)
    assert fit["correlation"] == pytest.approx(1.0)


def test_fit_refuses_degenerate_inputs() -> None:
    assert _fit([]) is None
    assert _fit([(1.0, 2.0)]) is None
    # x never moves: no slope is defensible.
    assert _fit([(1.0, 2.0), (1.0, 3.0)]) is None
    # y never moves: the line is flat and real, but correlation has no answer.
    flat = _fit([(0.0, 2.0), (1.0, 2.0)])
    assert flat is not None
    assert flat["slope"] == pytest.approx(0.0)
    assert flat["correlation"] is None


# --------------------------------------------------------------------------- scatter


def test_is_vs_oos_scatter_fits_over_folds_with_both_defined() -> None:
    records = [
        _record(0, grid_sharpes=[1.0, 2.0, 3.0], objective=[1.0, 2.0, 3.0], chosen=2,
                is_sharpe=2.0, oos_sharpe=1.0, oos_annualised=0.1),
        _record(1, grid_sharpes=[1.0, 2.0, 3.0], objective=[1.0, 2.0, 3.0], chosen=2,
                is_sharpe=4.0, oos_sharpe=2.0, oos_annualised=0.2),
        # A fold whose OOS Sharpe is undefined contributes a scatter row but not a
        # fitted point.
        _record(2, grid_sharpes=[1.0, 2.0, 3.0], objective=[1.0, 2.0, 3.0], chosen=2,
                is_sharpe=3.0, oos_sharpe=None, oos_annualised=0.3),
    ]
    out = diagnostics(_result(records))
    scatter = out["is_vs_oos"]
    assert len(scatter["points"]) == 3
    assert scatter["fit"]["points"] == 2
    assert scatter["fit"]["slope"] == pytest.approx(0.5)


def test_decay_slope_is_negative_when_oos_performance_falls_by_fold() -> None:
    records = [
        _record(i, grid_sharpes=[1.0], objective=[1.0], chosen=0,
                is_sharpe=1.0, oos_sharpe=3.0 - i, oos_annualised=0.3 - 0.1 * i)
        for i in range(4)
    ]
    out = diagnostics(_result(records, grid={"p": [1]}))
    assert out["decay"]["sharpe_fit"]["slope"] == pytest.approx(-1.0)
    assert out["decay"]["annualised_return_fit"]["slope"] == pytest.approx(-0.1)


# --------------------------------------------------------------------------- plateau


def test_plateau_aggregates_by_median_and_counts_unprofitable_neighbourhoods() -> None:
    records = [
        _record(0, grid_sharpes=[1.0], objective=[1.0], chosen=0, plateau=1.1,
                oos_sharpe=1.0),
        _record(1, grid_sharpes=[1.0], objective=[1.0], chosen=0, plateau=4.0,
                oos_sharpe=1.0),
        _record(2, grid_sharpes=[1.0], objective=[1.0], chosen=0, plateau=None,
                unprofitable=True, oos_sharpe=1.0),
    ]
    out = diagnostics(_result(records, grid={"p": [1]}))
    plateau = out["plateau"]
    assert plateau["per_fold"] == [1.1, 4.0, None]
    assert plateau["median"] == pytest.approx(2.55)
    assert plateau["neighbourhood_unprofitable_folds"] == 1


# ----------------------------------------------------------------------- sensitivity


def test_sensitivity_averages_each_point_across_folds_and_counts_choices() -> None:
    records = [
        _record(0, grid_sharpes=[0.5, 1.0, None], objective=[0.5, 1.0, None], chosen=1),
        _record(1, grid_sharpes=[1.5, 2.0, 1.0], objective=[1.5, 2.0, 1.0], chosen=1),
    ]
    out = diagnostics(_result(records))
    surface = out["sensitivity"]
    assert surface["render"] == "parallel_coordinates"  # one axis
    points = surface["points"]
    assert points[0]["mean_objective"] == pytest.approx(1.0)
    assert points[0]["folds_defined"] == 2
    assert points[2]["mean_objective"] == pytest.approx(1.0)
    assert points[2]["folds_defined"] == 1  # undefined in fold 0
    assert points[1]["times_chosen"] == 2
    assert points[1]["params"] == {"p": 2}


def test_two_parameter_grids_render_as_a_heatmap() -> None:
    grid = {"fast": [5, 10], "slow": [50, 100]}
    records = [
        _record(0, grid_sharpes=[1.0] * 4, objective=[1.0] * 4, chosen=0),
    ]
    out = diagnostics(_result(records, grid=grid))
    assert out["sensitivity"]["render"] == "heatmap"
    assert out["sensitivity"]["axes"] == {"fast": [5, 10], "slow": [50, 100]}


# --------------------------------------------------------------------------- trials


def test_trials_context_is_carried_verbatim() -> None:
    records = [_record(0, grid_sharpes=[1.0], objective=[1.0], chosen=0, oos_sharpe=1.0)]
    trials = {"combinations": 500, "evaluations": 700, "selection_bias_sd": 3.5}
    out = diagnostics(_result(records, grid={"p": [1]}), trials)
    assert out["trials"] == trials
    assert diagnostics(_result(records, grid={"p": [1]}))["trials"] is None
