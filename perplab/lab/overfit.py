"""Overfitting diagnostics over a walk-forward's own data (spec 9.4).

Everything here is arithmetic on numbers the walk-forward already computed -- fold grids,
chosen points, IS and OOS figures -- so the diagnostics are produced *with* every
walk-forward rather than as a separate job someone has to remember to run. An overfitting
check that is optional is an overfitting check that is skipped on exactly the runs that
need it.

The four views, and what each is for:

- **Parameter sensitivity surface** -- the grid's objective at every point, aggregated
  across folds. A robust optimum shows as a broad warm region; an isolated hot pixel is
  noise. Two parameters render as a heatmap, more as parallel coordinates; this module
  ships the numbers and the axes, and the UI decides how to draw them.
- **Plateau score** -- `chosen / mean(neighbourhood)`, per fold and aggregated by median.
  Spec 9.4 calls it the most actionable overfitting signal available, "displayed as a
  single prominent number". Near 1.0 is a plateau; much greater is a spike.
- **IS vs OOS scatter with fitted slope** -- one point per fold. A slope near zero means
  in-sample performance carries no information about out-of-sample performance: the
  optimisation is doing nothing, however good the IS numbers look.
- **Performance decay** -- the OOS metric against fold index, with a fitted trend. A
  downward slope means the edge is decaying or was never there.

**The trials count is context for all of it** (spec 8.5): the fitted slopes and scores
above are themselves statistics that were selected over `N` parameter combinations, and
the reader is owed that `N`. The worker supplies it from the store; this module only
carries it -- computing it here would be a second implementation of the counter.
"""

from __future__ import annotations

import math
from statistics import median
from typing import Any, Mapping, Sequence

from perplab.lab.walkforward import GridShape, WalkForwardResult

__all__ = ["diagnostics"]


def _fit(points: Sequence[tuple[float, float]]) -> dict[str, Any] | None:
    """Ordinary least squares over `(x, y)` pairs: slope, intercept, correlation.

    `None` with fewer than two points or zero variance in `x` -- a slope fitted through
    one point, or through folds whose x never moved, is a number with no content. The
    correlation is `None` (not the slope) when `y` has zero variance: the line exists,
    but "how tightly do the points hug it" has no answer when they are all the same
    height.
    """
    if len(points) < 2:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    n = float(len(points))
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x <= 0:
        return None
    cov = sum((x - mean_x) * (y - mean_y) for x, y in points)
    slope = cov / var_x
    var_y = sum((y - mean_y) ** 2 for y in ys)
    correlation = (
        cov / math.sqrt(var_x * var_y) if var_y > 0 else None
    )
    return {
        "slope": slope,
        "intercept": mean_y - slope * mean_x,
        "correlation": correlation,
        "points": len(points),
    }


def _sensitivity(result: WalkForwardResult) -> dict[str, Any]:
    """The grid surface, aggregated across folds.

    Per point: the mean of the objective and of the raw Sharpe over the folds where each
    was defined, with the count of contributing folds beside it -- a point defined in one
    fold out of eight is not the same evidence as one defined in all eight, and averaging
    without saying so would flatten that difference away.
    """
    shape = GridShape.from_grid(result.config.grid)
    objective_sums = [0.0] * shape.size
    objective_counts = [0] * shape.size
    sharpe_sums = [0.0] * shape.size
    sharpe_counts = [0] * shape.size
    chosen_counts = [0] * shape.size

    for record in result.folds:
        for index in range(shape.size):
            value = record.objective[index] if index < len(record.objective) else None
            if value is not None:
                objective_sums[index] += value
                objective_counts[index] += 1
            sharpe = record.grid[index].sharpe if index < len(record.grid) else None
            if sharpe is not None and record.grid[index].ok:
                sharpe_sums[index] += sharpe
                sharpe_counts[index] += 1
        if record.chosen_index is not None:
            chosen_counts[record.chosen_index] += 1

    points = []
    for index in range(shape.size):
        position = shape.position(index)
        points.append(
            {
                "index": index,
                "params": {
                    name: shape.values[axis][position[axis]]
                    for axis, name in enumerate(shape.names)
                },
                "mean_objective": (
                    objective_sums[index] / objective_counts[index]
                    if objective_counts[index]
                    else None
                ),
                "mean_sharpe": (
                    sharpe_sums[index] / sharpe_counts[index]
                    if sharpe_counts[index]
                    else None
                ),
                "folds_defined": objective_counts[index],
                "times_chosen": chosen_counts[index],
            }
        )
    return {
        "axes": {
            name: list(values) for name, values in zip(shape.names, shape.values)
        },
        "render": "heatmap" if len(shape.names) == 2 else "parallel_coordinates",
        "points": points,
    }


def diagnostics(
    result: WalkForwardResult, trials: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """The spec 9.4 set, from a finished walk-forward.

    `trials` is the store's spec 8.5 record for the strategy (combinations, evaluations,
    selection bias), passed through verbatim so every rendering of these diagnostics has
    the multiple-testing context beside it.
    """
    evaluated = [r for r in result.folds if r.oos is not None]

    scatter_points = [
        (r.is_sharpe, r.oos["sharpe"])
        for r in evaluated
        if r.is_sharpe is not None and r.oos["sharpe"] is not None
    ]
    decay_sharpe = [
        (float(r.fold.index), r.oos["sharpe"])
        for r in evaluated
        if r.oos["sharpe"] is not None
    ]
    decay_return = [
        (float(r.fold.index), r.oos["annualised_return"])
        for r in evaluated
        if r.oos["annualised_return"] is not None
    ]

    plateaus = [r.plateau for r in result.folds if r.plateau is not None]
    unprofitable = sum(1 for r in result.folds if r.neighbourhood_unprofitable)

    return {
        "plateau": {
            "per_fold": [r.plateau for r in result.folds],
            "median": float(median(plateaus)) if plateaus else None,
            "neighbourhood_unprofitable_folds": unprofitable,
            "reading": (
                "near 1.0 is a robust plateau; much greater than 1.0 is an isolated "
                "spike, which is almost certainly noise (spec 9.4)"
            ),
        },
        "is_vs_oos": {
            "points": [
                {"fold": r.fold.index, "is_sharpe": r.is_sharpe, "oos_sharpe": r.oos["sharpe"]}
                for r in evaluated
            ],
            "fit": _fit(scatter_points),
            "reading": (
                "slope near 0 means in-sample performance carries no information about "
                "out-of-sample performance -- the optimisation is doing nothing"
            ),
        },
        "decay": {
            "sharpe_by_fold": [
                {"fold": r.fold.index, "oos_sharpe": r.oos["sharpe"]} for r in evaluated
            ],
            "sharpe_fit": _fit(decay_sharpe),
            "annualised_return_fit": _fit(decay_return),
            "reading": (
                "a downward trend across folds means the edge is decaying or was "
                "never there"
            ),
        },
        "sensitivity": _sensitivity(result),
        "trials": dict(trials) if trials is not None else None,
    }
