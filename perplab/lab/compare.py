"""Multi-run comparison (spec 9.6): aligned curves, correlations, a naive blend.

**Only runs sharing a range are comparable, and the check is a refusal, not a warning.**
Spec 9.6: "the UI blocks mismatched comparisons rather than producing a misleading
overlay." Two runs over different ranges can both be normalised to 1.0 and drawn on one
chart, and the chart will look meaningful -- that is exactly the overlay this module
exists to refuse. Sharing a range also pins the metric grid: the grid's step is a
function of the range alone (`build_grid`), so aligned runs land on identical boundaries
and "correlation of daily returns" is a correlation of *the same days*.

**The naive combined curve is exactly what its name says**, and its definition travels in
the payload: equal capital split across the runs at the range start, each held with no
rebalancing -- the mean of the normalised equity curves. It answers "what if I had simply
run all of these at once", not "what is the optimal blend"; portfolio construction is
spec 9.5's job, not a chart's.

Correlations are Pearson over the shared grid returns, `None` when either series has
zero variance -- a flat run correlates with nothing, and 0.0 would claim independence
where there is no evidence of anything.

**A return over a non-positive prior equity is undefined, not 0.0.** A run that reached
zero has no balance for a percentage to be a percentage *of*; fabricating a flat 0.0
there claimed the dead run was trading flat, and -- because 0.0 is a value Pearson
happily consumes -- diluted every correlation involving a ruined run toward zero. Each
pair now correlates over the boundaries where *both* runs' returns are defined (pairwise
deletion, so a third run's ruin cannot shrink an unrelated pair's basis, and two runs
correlate identically whatever else is in the comparison set), and the per-pair basis
travels in the payload as `defined_returns`, because a coefficient over 2 boundaries
beside one over 200 must not read as equally earned. The equity overlay keeps every
boundary: drawing a ruined curve at zero is a true statement about equity; correlating
its fabricated returns was not a true statement about anything.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from perplab.lab.stats import pearson

__all__ = ["ComparisonError", "compare_runs"]


class ComparisonError(ValueError):
    """The runs cannot be compared, and the message says which and why."""


def compare_runs(runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Compare completed runs. Each entry carries what the API layer read from disk:

    ```
    {"run_id", "label", "start_ms", "end_ms",
     "grid_times": [...], "grid_equity": [...], "grid_returns": [...],
     "metrics": {...}}
    ```

    `grid_*` come from `build_grid` over the run's stored equity series -- the same
    resampling its own metrics used, so the table and the chart agree with the run pages
    they link back to.
    """
    if len(runs) < 2:
        raise ComparisonError("a comparison needs at least two runs")

    first = runs[0]
    for run in runs[1:]:
        if (run["start_ms"], run["end_ms"]) != (first["start_ms"], first["end_ms"]):
            raise ComparisonError(
                f"run {run['run_id']} covers [{run['start_ms']}, {run['end_ms']}) but "
                f"run {first['run_id']} covers [{first['start_ms']}, "
                f"{first['end_ms']}). Runs over different ranges cannot share a chart "
                f"without the overlay being misleading (spec 9.6); backtest them over "
                f"one range to compare them."
            )

    # The shared boundaries: identical ranges give identical grids, but a run whose
    # equity series starts late (a warm-up quirk) can miss the first boundary, so the
    # intersection is taken and reported rather than assumed.
    common = set(runs[0]["grid_times"])
    for run in runs[1:]:
        common &= set(run["grid_times"])
    boundaries = sorted(common)
    if len(boundaries) < 3:
        raise ComparisonError(
            "the runs share fewer than three grid boundaries, so there are not enough "
            "aligned returns to correlate or overlay"
        )

    aligned_equity: list[list[float]] = []
    aligned_returns: list[list[float | None]] = []
    for run in runs:
        index = {ts: i for i, ts in enumerate(run["grid_times"])}
        equity = [run["grid_equity"][index[ts]] for ts in boundaries]
        opening = equity[0]
        if opening <= 0:
            raise ComparisonError(
                f"run {run['run_id']} opens the shared range at {opening}; a "
                f"non-positive base cannot be normalised"
            )
        aligned_equity.append([value / opening for value in equity])
        # `None`, not 0.0, over a non-positive base: the step *into* ruin is a real
        # return (at or below -100%), but the steps after it describe a balance that no
        # longer exists, and a fabricated 0.0 is a value `pearson` cannot refuse -- the
        # module docstring's own argument against claiming independence, violated one
        # screen below where it was stated.
        aligned_returns.append(
            [
                equity[i] / equity[i - 1] - 1.0 if equity[i - 1] > 0 else None
                for i in range(1, len(equity))
            ]
        )

    ids = [run["run_id"] for run in runs]
    correlation: list[list[float | None]] = []
    defined_counts: list[list[int]] = []
    for i in range(len(runs)):
        row: list[float | None] = []
        counts: list[int] = []
        for j in range(len(runs)):
            # Pairwise-complete: each cell uses the boundaries where both runs still had
            # an account. `pearson` then applies its own refusals (fewer than two points,
            # zero variance) to what genuinely remains rather than to padding.
            pairs = [
                (a, b)
                for a, b in zip(aligned_returns[i], aligned_returns[j])
                if a is not None and b is not None
            ]
            counts.append(len(pairs))
            row.append(
                1.0
                if i == j
                else pearson([a for a, _ in pairs], [b for _, b in pairs])
            )
        correlation.append(row)
        defined_counts.append(counts)

    combined = [
        sum(curve[i] for curve in aligned_equity) / len(aligned_equity)
        for i in range(len(boundaries))
    ]

    return {
        "run_ids": ids,
        "start_ms": first["start_ms"],
        "end_ms": first["end_ms"],
        "boundaries": boundaries,
        "curves": [
            {
                "run_id": run["run_id"],
                "label": run.get("label", ""),
                "equity_normalised": curve,
                "metrics": run.get("metrics"),
            }
            for run, curve in zip(runs, aligned_equity)
        ],
        "correlation": {
            "run_ids": ids,
            "matrix": correlation,
            # Per cell: how many aligned returns the coefficient was computed over --
            # the boundaries where both runs' returns were defined. Anything short of
            # `len(boundaries) - 1` means a run was ruined inside the range, and a
            # reader weighing a coefficient is owed the size of its evidence.
            "defined_returns": defined_counts,
        },
        "combined": {
            "equity_normalised": combined,
            "definition": (
                "equal capital split across the runs at the range start, held with no "
                "rebalancing -- the mean of the normalised equity curves. An "
                "illustration, not an optimised portfolio (spec 9.5 is where "
                "construction lives)."
            ),
        },
    }
