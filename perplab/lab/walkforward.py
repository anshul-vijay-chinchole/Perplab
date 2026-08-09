"""Walk-forward analysis (spec 9.1): optimise in sample, evaluate out of sample, repeat.

The headline output is the **stitched out-of-sample equity curve** -- spec 9.1 calls it
"the only number worth quoting", because every point on it was earned on data the
optimisation had not seen. Everything else here exists to qualify that curve: the per-fold
IS/OOS table says where it came from, WFE says how much of the in-sample promise survived
contact with new data, and the parameter-stability record says whether there was a stable
optimum to find at all.

## The default objective is not `max(Sharpe)`

Spec 9.1: the default is **neighbourhood-median Sharpe** -- for each grid point, the median
Sharpe over the point and its immediate grid neighbours (one step along one axis). A spike
with poor neighbours loses to a plateau with decent ones, which is precisely the
anti-overfitting property wanted, and it costs nothing because the grid is already computed.
`max(Sharpe)` remains selectable and carries an `(overfit-prone)` label wherever it is
shown (R15).

## Warm-up is realistic, not leakage

Each fold's runs are ordinary engine runs, and the engine already feeds
`requires["history"]` bars before the range in warm-up mode -- indicators update, orders
are blocked. Spec 9.1 is explicit that this is not look-ahead: live trading *would* have
that history. Leakage would be choosing parameters using OOS data, and the fold structure
is what prevents that -- parameters are chosen from the IS sweep alone, before the OOS
window is ever run.

## Two stitched curves, because sizing decides which one is true

Stitching fold curves multiplicatively -- scale each fold to start where the last ended --
assumes the strategy compounds: trading fold 3 with the balance fold 2 left behind. A
strategy sizing a **fixed notional** produces the same PnL whatever the balance, and for it
the additive curve (cumulative PnL over the opening balance) is the honest one. The
platform cannot know which sizing a strategy's own code implements, so **both** are
computed and stored, the compounded one as the headline and the caveat carried in the
artefact -- the same honesty rule spec 9.2 imposes on the Monte Carlo panel.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass, field, replace
from pathlib import Path
from statistics import median
from typing import Any, Callable, Mapping, Sequence

from perplab.analytics.metrics import MS_PER_DAY, annualised_return
from perplab.engine.runspec import RunSpec
from perplab.lab.sweep import (
    SweepPoint,
    SweepResult,
    execute_point,
    expand_grid,
    points_from_spec,
    sweep,
)

__all__ = [
    "OBJECTIVES",
    "WalkForwardConfig",
    "Fold",
    "GridShape",
    "FoldRecord",
    "WalkForwardResult",
    "build_folds",
    "objective_values",
    "choose_index",
    "plateau_score",
    "run_walkforward",
]


OBJECTIVES: dict[str, str] = {
    "nbhd_median_sharpe": "neighbourhood-median Sharpe",
    "max_sharpe": "max Sharpe (overfit-prone)",
}
"""Selectable optimisation objectives, keyed by wire name.

The label travels into the artefact so every rendering of the result says what was
optimised -- spec 9.1 requires `max(Sharpe)` to be *labelled* overfit-prone, and a label
applied in one UI component is a label that is missing from the CSV export.
"""


# ------------------------------------------------------------------------------- folds


@dataclass(frozen=True, slots=True)
class Fold:
    """One IS/OOS pair. Half-open windows, `[start, end)`, like every engine range."""

    index: int
    is_start_ms: int
    is_end_ms: int
    oos_start_ms: int
    oos_end_ms: int

    def to_json(self) -> dict[str, int]:
        return {
            "index": self.index,
            "is_start_ms": self.is_start_ms,
            "is_end_ms": self.is_end_ms,
            "oos_start_ms": self.oos_start_ms,
            "oos_end_ms": self.oos_end_ms,
        }


def build_folds(
    start_ms: int,
    end_ms: int,
    *,
    is_ms: int,
    oos_ms: int,
    step_ms: int | None = None,
    mode: str = "anchored",
) -> tuple[tuple[Fold, ...], int]:
    """Cut `[start_ms, end_ms)` into IS/OOS folds. Returns `(folds, uncovered_ms)`.

    **Whole OOS windows only.** A final stub window would put a 3-day fold beside 30-day
    ones in the same table, and every per-fold statistic would be computed over samples too
    few to mean anything. The stub is not silently dropped either: its length comes back as
    `uncovered_ms` and the artefact states it, because "the last eleven days were
    never evaluated" is a fact the reader of a stitched curve is owed.

    `anchored` grows the IS window from `start_ms` (expanding); `rolling` keeps it at
    `is_ms` (fixed length, sliding). The default step equals the OOS length, which makes
    consecutive OOS windows contiguous and the stitched curve gap-free; a smaller step
    overlaps them, a larger one leaves holes, and both are legitimate experiments --
    `run_walkforward` refuses only the overlap, because stitching overlapping windows
    would count the same days twice in one curve.
    """
    if mode not in ("anchored", "rolling"):
        raise ValueError(f"unknown walk-forward mode {mode!r}: use 'anchored' or 'rolling'")
    if is_ms <= 0 or oos_ms <= 0:
        raise ValueError(
            f"window lengths must be positive: is_ms={is_ms}, oos_ms={oos_ms}"
        )
    step = oos_ms if step_ms is None else step_ms
    if step <= 0:
        raise ValueError(f"step_ms must be positive, got {step}")
    if end_ms - start_ms < is_ms + oos_ms:
        raise ValueError(
            f"the range is {end_ms - start_ms} ms long but one fold needs "
            f"{is_ms + oos_ms} ms (IS {is_ms} + OOS {oos_ms}). Shorten the windows or "
            f"widen the range."
        )

    folds: list[Fold] = []
    oos_start = start_ms + is_ms
    while oos_start + oos_ms <= end_ms:
        index = len(folds)
        is_start = start_ms if mode == "anchored" else oos_start - is_ms
        folds.append(
            Fold(
                index=index,
                is_start_ms=is_start,
                is_end_ms=oos_start,
                oos_start_ms=oos_start,
                oos_end_ms=oos_start + oos_ms,
            )
        )
        oos_start += step
    # **Every millisecond of the range no OOS window covers**, not just the tail. A step
    # wider than the OOS window leaves holes *between* folds -- legitimate, but the
    # docstring's own argument ("the last eleven days were never evaluated is a fact the
    # reader of a stitched curve is owed") applies to an eleven-day hole in the middle
    # exactly as much, and reporting only the tail understated it silently.
    covered = sum(fold.oos_end_ms - fold.oos_start_ms for fold in folds)
    first_oos = folds[0].oos_start_ms if folds else end_ms
    uncovered = (end_ms - start_ms) - covered - (first_oos - start_ms)
    return tuple(folds), max(0, uncovered)


# -------------------------------------------------------------------------- the grid


@dataclass(frozen=True)
class GridShape:
    """The parameter grid as geometry: axes, strides, and who neighbours whom.

    Mirrors `expand_grid`'s ordering exactly -- names sorted, values in the order given,
    last axis varying fastest. The agreement is not asserted at runtime (it is a
    property of two pure functions, checked once in
    `test_grid_shape_indexing_agrees_with_expand_grid` rather than on every call), and
    it is load-bearing: every neighbourhood computed here is a set of unrelated
    parameter sets if index arithmetic and grid expansion disagree about what an index
    means.
    """

    names: tuple[str, ...]
    values: tuple[tuple[Any, ...], ...]
    strides: tuple[int, ...]
    size: int

    @classmethod
    def from_grid(cls, grid: Mapping[str, Sequence[Any]]) -> "GridShape":
        names = tuple(sorted(grid))
        values = tuple(tuple(grid[name]) for name in names)
        for name, axis in zip(names, values):
            if not axis:
                raise ValueError(f"parameter {name!r} has no values to sweep over")
        strides: list[int] = []
        running = 1
        for axis in reversed(values):
            strides.append(running)
            running *= len(axis)
        return cls(
            names=names,
            values=values,
            strides=tuple(reversed(strides)),
            size=running,
        )

    def position(self, index: int) -> tuple[int, ...]:
        return tuple(
            (index // stride) % len(axis)
            for stride, axis in zip(self.strides, self.values)
        )

    def neighbours(self, index: int) -> tuple[int, ...]:
        """Indices one step along exactly one axis -- the spec 9.1 neighbourhood.

        The von Neumann neighbourhood, not the Moore one: "immediate grid neighbours"
        along each parameter axis. Diagonal points differ in two parameters at once, which
        is two steps of experiment, not one.
        """
        found: list[int] = []
        position = self.position(index)
        for axis_index, (stride, axis) in enumerate(zip(self.strides, self.values)):
            at = position[axis_index]
            if at > 0:
                found.append(index - stride)
            if at + 1 < len(axis):
                found.append(index + stride)
        return tuple(found)


def objective_values(
    results: Sequence[SweepResult],
    shape: GridShape,
    objective: str,
) -> list[float | None]:
    """The objective at every grid point, `None` where the point is ineligible.

    A point is ineligible when its own Sharpe is undefined -- it failed, produced fewer
    than two grid returns, or blew up -- because selecting a point whose quality cannot be
    stated would be optimisation by coin toss. Undefined *neighbours* are different: they
    are excluded from the median rather than poisoning it, and the count of defined
    neighbours is recorded per fold so a choice made in a thin neighbourhood is visible.
    """
    if objective not in OBJECTIVES:
        raise ValueError(
            f"unknown objective {objective!r}: use one of {sorted(OBJECTIVES)}"
        )
    if len(results) != shape.size:
        raise ValueError(
            f"the grid has {shape.size} points but {len(results)} results were given"
        )

    sharpes = [r.sharpe if r.ok else None for r in results]
    if objective == "max_sharpe":
        return list(sharpes)

    values: list[float | None] = []
    for index, own in enumerate(sharpes):
        if own is None:
            values.append(None)
            continue
        candidates = [own] + [
            sharpes[n] for n in shape.neighbours(index) if sharpes[n] is not None
        ]
        values.append(float(median(candidates)))
    return values


def choose_index(values: Sequence[float | None]) -> int | None:
    """The best-scoring point, ties broken by lowest index.

    Deterministic on purpose: spec 12.1's reproducibility argument applies to the Lab as
    much as to the engine, and a tie broken by dict order would make the same walk-forward
    choose different parameters on different interpreters.
    """
    best: int | None = None
    for index, value in enumerate(values):
        if value is None or not math.isfinite(value):
            continue
        if best is None or value > values[best]:
            best = index
    return best


def plateau_score(
    results: Sequence[SweepResult], shape: GridShape, chosen: int
) -> float | None:
    """`chosen Sharpe / mean(neighbour Sharpes)` -- spec 9.4's single most actionable number.

    Near 1.0 means the chosen point sits on a plateau; much greater than 1.0 means an
    isolated spike, which is almost certainly noise. Neighbours only, excluding the point
    itself -- including it would pull every score toward 1.0 by construction, flattering
    exactly the spikes the number exists to expose.

    `None` when it cannot be honestly computed: no neighbours (a single-point or
    single-axis-edge grid... an edge point still has one), no neighbour with a defined
    Sharpe, or a neighbourhood mean at or below zero. That last case is not a computation
    detail: a positive point surrounded by unprofitable neighbours is the *strongest*
    overfit signal there is, and a negative ratio would bury it in a column meant to hover
    around 1.0. The caller records `neighbourhood_unprofitable` instead, which says it in
    words.
    """
    own = results[chosen].sharpe if results[chosen].ok else None
    if own is None:
        return None
    neighbour_sharpes = [
        results[n].sharpe
        for n in shape.neighbours(chosen)
        if results[n].ok and results[n].sharpe is not None
    ]
    if not neighbour_sharpes:
        return None
    mean = sum(neighbour_sharpes) / len(neighbour_sharpes)
    if mean <= 0:
        return None
    return own / mean


# ------------------------------------------------------------------------ configuration


@dataclass(frozen=True, slots=True)
class WalkForwardConfig:
    """What the user chose on the Lab form (spec 9.1's config list)."""

    is_ms: int
    oos_ms: int
    grid: Mapping[str, Sequence[Any]]
    step_ms: int | None = None
    mode: str = "anchored"
    objective: str = "nbhd_median_sharpe"
    max_workers: int | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "is_ms": self.is_ms,
            "oos_ms": self.oos_ms,
            "step_ms": self.step_ms,
            "mode": self.mode,
            "objective": self.objective,
            "objective_label": OBJECTIVES.get(self.objective, self.objective),
            "grid": {name: list(values) for name, values in self.grid.items()},
        }

    MIN_WINDOW_MS = 60_000
    """One minute -- the shortest bar the platform stores. A window below it cannot hold
    a single observation, so it is a typo rather than an experiment."""

    MAX_WORKERS = 32
    """Ceiling on the sweep pool. `sweep()` spawns `min(max_workers, len(points))`
    processes, so an unbounded value in a submitted config was a way to ask the server
    for five hundred Python interpreters."""

    MAX_FOLDS = 512
    """Ceiling on fold count, checked from the config's own arithmetic before any fold is
    built. `is_ms=1, oos_ms=1` over a year passes every per-field check and then asks
    `build_folds` for ~3e10 `Fold` objects -- an out-of-memory kill that no cancel can
    reach, from a request that returned 201."""

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> "WalkForwardConfig":
        """Parse **and validate**, so a bad config is a 400 rather than a dead job.

        The router's contract is that a mistake comes back naming the field while the
        user is looking at the form. That only holds if the checks live here: everything
        below used to be enforced inside `run_walkforward` or `build_folds`, which run in
        the worker -- so an overlapping step returned 201, created a row, launched a
        process, and died on its first line.
        """
        grid = obj.get("grid", {})
        if not isinstance(grid, Mapping):
            raise ValueError("walk-forward grid must be an object of parameter: values")

        is_ms = int(obj["is_ms"])
        oos_ms = int(obj["oos_ms"])
        step_ms = None if obj.get("step_ms") is None else int(obj["step_ms"])
        mode = str(obj.get("mode", "anchored"))
        objective = str(obj.get("objective", "nbhd_median_sharpe"))
        max_workers = (
            None if obj.get("max_workers") is None else int(obj["max_workers"])
        )

        if is_ms < cls.MIN_WINDOW_MS or oos_ms < cls.MIN_WINDOW_MS:
            raise ValueError(
                f"in-sample and out-of-sample windows must each be at least "
                f"{cls.MIN_WINDOW_MS} ms (one bar); got is_ms={is_ms}, oos_ms={oos_ms}"
            )
        if step_ms is not None and step_ms < oos_ms:
            raise ValueError(
                f"step_ms ({step_ms}) is shorter than oos_ms ({oos_ms}), which would "
                "overlap consecutive out-of-sample windows and count the same days twice "
                "in one stitched curve. Use step_ms >= oos_ms, or omit it."
            )
        if mode not in ("anchored", "rolling"):
            raise ValueError(f"unknown walk-forward mode {mode!r}: use 'anchored' or 'rolling'")
        if objective not in OBJECTIVES:
            raise ValueError(
                f"unknown objective {objective!r}: use one of {sorted(OBJECTIVES)}"
            )
        if max_workers is not None and not 1 <= max_workers <= cls.MAX_WORKERS:
            raise ValueError(
                f"max_workers must be between 1 and {cls.MAX_WORKERS}, got {max_workers}"
            )
        shape = GridShape.from_grid({str(k): list(v) for k, v in grid.items()})
        if shape.size > 4096:
            raise ValueError(
                f"the grid expands to {shape.size} points; that is a sweep per fold and "
                "an enormous multiple-testing N (spec 8.5). Narrow it."
            )
        return cls(
            is_ms=is_ms,
            oos_ms=oos_ms,
            step_ms=step_ms,
            mode=mode,
            objective=objective,
            grid={str(k): list(v) for k, v in grid.items()},
            max_workers=max_workers,
        )

    def check_against(self, start_ms: int, end_ms: int) -> None:
        """Refuse a config that this run's range cannot support. Called at submit time.

        Fold count is derived arithmetically rather than by building the folds, because
        building them is exactly the allocation this guard exists to prevent.
        """
        span = end_ms - start_ms
        if span < self.is_ms + self.oos_ms:
            raise ValueError(
                f"this run covers {span} ms but one fold needs {self.is_ms + self.oos_ms} "
                f"ms (IS {self.is_ms} + OOS {self.oos_ms}). Shorten the windows, or pick "
                f"a longer run."
            )
        step = self.oos_ms if self.step_ms is None else self.step_ms
        folds = (span - self.is_ms - self.oos_ms) // step + 1
        if folds > self.MAX_FOLDS:
            raise ValueError(
                f"these windows produce {folds} folds over this run's range, past the "
                f"{self.MAX_FOLDS} cap. Each fold is a full grid sweep plus an "
                f"out-of-sample run; widen the windows or the step."
            )


# ----------------------------------------------------------------------------- records


@dataclass(frozen=True, slots=True)
class FoldRecord:
    """One row of the per-fold IS vs OOS table, plus the grid it was chosen from."""

    fold: Fold
    grid: tuple[SweepResult, ...]
    objective: tuple[float | None, ...]
    chosen_index: int | None
    chosen_params: Mapping[str, Any] | None
    is_sharpe: float | None
    is_total_return: float | None
    is_annualised: float | None
    is_net_pnl: str | None
    is_max_drawdown: float | None
    is_round_trips: int | None
    oos: Mapping[str, Any] | None
    """The OOS evaluation's summary, `None` when the fold produced no evaluation."""
    is_halted: bool
    """Whether the *chosen* in-sample evaluation risk-halted before its window ended.

    Load-bearing honesty, discovered the expensive way: a strategy that trips a risk
    limit days into every anchored window makes all folds optimise over the same short
    stub while the fold table reads like full windows -- and the stitched curve gets
    quoted by someone who never learns the optimiser saw two days of data. The fact was
    always in `grid[chosen].halted`; a fact three clicks deep is a fact nobody has."""
    is_halt_limit: str | None
    wfe: float | None
    plateau: float | None
    neighbours_defined: int
    neighbours_total: int
    neighbourhood_unprofitable: bool
    error: str | None

    def to_json(self) -> dict[str, Any]:
        return {
            **self.fold.to_json(),
            "grid": [r.to_json() for r in self.grid],
            "objective": list(self.objective),
            "chosen_index": self.chosen_index,
            "chosen_params": None
            if self.chosen_params is None
            else dict(self.chosen_params),
            "is": {
                "sharpe": self.is_sharpe,
                "total_return": self.is_total_return,
                "annualised_return": self.is_annualised,
                "net_pnl": self.is_net_pnl,
                "max_drawdown": self.is_max_drawdown,
                "round_trips": self.is_round_trips,
            },
            "oos": None if self.oos is None else dict(self.oos),
            "is_halted": self.is_halted,
            "is_halt_limit": self.is_halt_limit,
            "wfe": self.wfe,
            "plateau_score": self.plateau,
            "neighbours_defined": self.neighbours_defined,
            "neighbours_total": self.neighbours_total,
            "neighbourhood_unprofitable": self.neighbourhood_unprofitable,
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class WalkForwardResult:
    """Everything a walk-forward produced. The arrays go to Parquet, the rest to JSON."""

    config: WalkForwardConfig
    folds: tuple[FoldRecord, ...]
    stitched_ms: tuple[int, ...]
    stitched_equity: tuple[float, ...]
    """Compounded: each fold scaled to open where the previous one closed."""
    stitched_pnl: tuple[float, ...]
    """Additive: opening balance plus cumulative fold PnL. See the module docstring."""
    stitched_fold_index: tuple[int, ...]
    """Which fold each stitched sample came from, so fold boundaries survive the join."""
    fold_scales: tuple[float, ...]
    """Per stitched fold: the factor its equity was multiplied by. 1.0 for the first."""
    truncated_at_fold: int | None
    opening_balance: float
    oos_days: float
    wfe_aggregate: float | None
    wfe_aggregate_definition: str
    wfe_median: float | None
    stitched_total_return: float | None
    stitched_annualised: float | None
    stability: Mapping[str, Any]
    uncovered_ms: int
    warnings: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        """The artefact body, minus the stitched arrays (those live in Parquet)."""
        return {
            "config": self.config.to_json(),
            "folds": [f.to_json() for f in self.folds],
            "fold_scales": list(self.fold_scales),
            "truncated_at_fold": self.truncated_at_fold,
            "opening_balance": self.opening_balance,
            "oos_days": self.oos_days,
            "wfe_aggregate": self.wfe_aggregate,
            "wfe_aggregate_definition": self.wfe_aggregate_definition,
            "wfe_median": self.wfe_median,
            "stitched_total_return": self.stitched_total_return,
            "stitched_annualised": self.stitched_annualised,
            "stitched_samples": len(self.stitched_ms),
            "stability": dict(self.stability),
            "uncovered_ms": self.uncovered_ms,
            "warnings": list(self.warnings),
        }


# --------------------------------------------------------------------------- execution


_WFE_AGGREGATE_DEFINITION = (
    "annualised return of the stitched OOS curve, divided by the mean of the per-fold "
    "annualised IS returns of the chosen parameter sets"
)


def run_walkforward(
    root: Path | str,
    base_spec: RunSpec,
    config: WalkForwardConfig,
    *,
    progress: Callable[[int, int], None] | None = None,
    on_trial: Callable[[Mapping[str, Any], float | None], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> WalkForwardResult:
    """Run the full walk-forward: per fold, sweep the IS grid, pick, evaluate OOS.

    `base_spec` supplies everything spec 12.1 counts as an input except the range and the
    parameters, exactly as a sweep does -- so two folds differ in nothing but their
    windows, and two walk-forwards over the same run differ in nothing but their config.

    `on_trial` fires once per completed IS grid point and once per OOS evaluation, with
    the full parameter set and the Sharpe it produced. Spec 8.5 counts walk-forward
    optimisation among the sources of trials, and the caller (the Lab worker) is the one
    holding a database handle.

    `should_stop` is polled between engine runs. Cancellation is cooperative: the sweep's
    process pool cannot be safely killed mid-point from outside, so a stop request takes
    effect at the next point boundary and the partial work is discarded.
    """
    # Re-checked here as well as in `WalkForwardConfig.from_json`, because a config
    # constructed directly in Python never passes through that classmethod -- and this
    # is the check that stops a stitched curve double-counting days.
    if config.step_ms is not None and config.step_ms < config.oos_ms:
        raise ValueError(
            f"step_ms ({config.step_ms}) is shorter than oos_ms ({config.oos_ms}), which "
            "would overlap consecutive OOS windows and count the same days twice in one "
            "stitched curve. Use step_ms >= oos_ms, or omit it for contiguous folds."
        )
    folds, uncovered = build_folds(
        base_spec.start_ms,
        base_spec.end_ms,
        is_ms=config.is_ms,
        oos_ms=config.oos_ms,
        step_ms=config.step_ms,
        mode=config.mode,
    )
    shape = GridShape.from_grid(config.grid)
    # Validated here rather than first failing inside fold 3's sweep.
    if config.objective not in OBJECTIVES:
        raise ValueError(
            f"unknown objective {config.objective!r}: use one of {sorted(OBJECTIVES)}"
        )

    total_steps = len(folds) * (shape.size + 1)
    done = 0

    def _tick() -> None:
        if progress is not None:
            progress(done, total_steps)

    warnings: list[str] = []
    records: list[FoldRecord] = []
    _tick()

    for fold in folds:
        if should_stop is not None and should_stop():
            raise KeyboardInterrupt("walk-forward cancelled")

        is_spec = replace(base_spec, start_ms=fold.is_start_ms, end_ms=fold.is_end_ms)
        points = points_from_spec(is_spec, config.grid)

        def _point_done(result: SweepResult) -> None:
            nonlocal done
            done += 1
            _tick()
            if on_trial is not None:
                on_trial(dict(result.params), result.sharpe)

        results = tuple(
            sweep(root, points, max_workers=config.max_workers, on_result=_point_done)
        )
        values = tuple(objective_values(results, shape, config.objective))
        chosen = choose_index(values)

        if chosen is None:
            failures = sorted({r.error for r in results if r.error})
            records.append(
                _empty_fold(
                    fold,
                    results,
                    values,
                    error=(
                        "no grid point produced a defined Sharpe on this IS window"
                        + (f" (errors: {'; '.join(failures[:3])})" if failures else "")
                    ),
                )
            )
            warnings.append(
                f"fold {fold.index}: no parameters could be chosen, so its OOS window "
                f"was never evaluated and the stitched curve skips it"
            )
            done += 1
            _tick()
            continue

        chosen_result = results[chosen]
        neighbours = shape.neighbours(chosen)
        defined = [
            n for n in neighbours if results[n].ok and results[n].sharpe is not None
        ]
        neighbour_mean = (
            sum(results[n].sharpe for n in defined) / len(defined) if defined else None
        )
        unprofitable = neighbour_mean is not None and neighbour_mean <= 0

        if should_stop is not None and should_stop():
            raise KeyboardInterrupt("walk-forward cancelled")

        oos_spec = replace(
            base_spec,
            start_ms=fold.oos_start_ms,
            end_ms=fold.oos_end_ms,
            params=dict(chosen_result.params),
        )
        oos_point = points_from_spec(oos_spec, {})[0]
        try:
            oos = _run_oos(str(root), oos_point)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:  # noqa: BLE001 - a failed fold is data
            oos = None
            error = f"OOS evaluation failed: {type(exc).__name__}: {exc}"
            warnings.append(f"fold {fold.index}: {error}")
        else:
            error = None
            if on_trial is not None:
                on_trial(dict(chosen_result.params), oos["sharpe"])
        done += 1
        _tick()

        if chosen_result.halted:
            warnings.append(
                f"fold {fold.index}: the chosen in-sample evaluation risk-halted on "
                f"{chosen_result.halt_limit}, so the optimisation saw only the window "
                f"before the halt -- not the {_days(fold.is_end_ms - fold.is_start_ms)} "
                f"the fold declares"
            )
        if oos is not None and oos.get("halted"):
            warnings.append(
                f"fold {fold.index}: the out-of-sample evaluation risk-halted, so its "
                f"figures cover only the window before the halt"
            )

        is_days = (fold.is_end_ms - fold.is_start_ms) / MS_PER_DAY
        is_annualised = annualised_return(chosen_result.total_return, is_days)
        oos_annualised = None if oos is None else oos["annualised_return"]
        records.append(
            FoldRecord(
                fold=fold,
                grid=results,
                objective=values,
                chosen_index=chosen,
                chosen_params=dict(chosen_result.params),
                is_sharpe=chosen_result.sharpe,
                is_total_return=chosen_result.total_return,
                is_annualised=is_annualised,
                is_net_pnl=chosen_result.net_pnl,
                is_max_drawdown=chosen_result.max_drawdown,
                is_round_trips=chosen_result.round_trips,
                oos=oos,
                is_halted=chosen_result.halted,
                is_halt_limit=chosen_result.halt_limit,
                wfe=_wfe(oos_annualised, is_annualised),
                plateau=plateau_score(results, shape, chosen),
                neighbours_defined=len(defined),
                neighbours_total=len(neighbours),
                neighbourhood_unprofitable=unprofitable,
                error=error,
            )
        )

    halted_folds = [r for r in records if r.is_halted]
    if halted_folds and len(halted_folds) == sum(1 for r in records if r.chosen_index is not None):
        warnings.append(
            "every fold's chosen in-sample evaluation risk-halted. Under an anchored "
            "start, halts land near the same early date, so the folds optimised over "
            "near-identical stubs rather than their declared windows -- this "
            "walk-forward says more about the risk limits than about the parameters. "
            "Consider re-running the base backtest with the risk layer relaxed for "
            "research."
        )

    stitched = _stitch(records, float(base_spec.opening_balance), warnings)
    stability = _stability(shape, records)

    # **The fold set is whatever the stitched curve actually contains**, which is not the
    # same as "folds with an OOS evaluation": `_stitch` also drops a fold whose window
    # held no samples, and stops entirely at a bankruptcy. Deriving the denominator from
    # `stitched.fold_index` rather than from the records means the ratio cannot compare a
    # two-fold numerator against a four-fold mean -- which it did, and which the comment
    # here previously claimed it did not.
    stitched_folds = set(stitched.fold_index)
    oos_days = sum(
        (r.fold.oos_end_ms - r.fold.oos_start_ms) / MS_PER_DAY
        for r in records
        if r.fold.index in stitched_folds
    )
    stitched_total = (
        stitched.equity[-1] / stitched.equity[0] - 1.0
        if len(stitched.equity) >= 2 and stitched.equity[0] > 0
        else None
    )
    stitched_annualised = (
        annualised_return(stitched_total, oos_days) if oos_days > 0 else None
    )

    is_annualiseds = [
        r.is_annualised
        for r in records
        if r.fold.index in stitched_folds and r.is_annualised is not None
    ]
    wfe_aggregate = None
    if stitched_annualised is not None and is_annualiseds:
        mean_is = sum(is_annualiseds) / len(is_annualiseds)
        if mean_is > 0:
            wfe_aggregate = stitched_annualised / mean_is
    wfes = [r.wfe for r in records if r.wfe is not None]
    wfe_median = float(median(wfes)) if wfes else None

    evaluated = sum(1 for r in records if r.oos is not None)
    if len(stitched_folds) < evaluated:
        warnings.append(
            f"{evaluated - len(stitched_folds)} evaluated fold(s) are absent from the "
            f"stitched curve, so every figure derived from it -- total return, "
            f"annualised, WFE aggregate -- covers {len(stitched_folds)} fold(s), not "
            f"{evaluated}"
        )

    return WalkForwardResult(
        config=config,
        folds=tuple(records),
        stitched_ms=tuple(stitched.times),
        stitched_equity=tuple(stitched.equity),
        stitched_pnl=tuple(stitched.pnl),
        stitched_fold_index=tuple(stitched.fold_index),
        fold_scales=tuple(stitched.scales),
        truncated_at_fold=stitched.truncated_at_fold,
        opening_balance=float(base_spec.opening_balance),
        oos_days=oos_days,
        wfe_aggregate=wfe_aggregate,
        wfe_aggregate_definition=_WFE_AGGREGATE_DEFINITION,
        wfe_median=wfe_median,
        stitched_total_return=stitched_total,
        stitched_annualised=stitched_annualised,
        stability=stability,
        uncovered_ms=uncovered,
        warnings=tuple(warnings),
    )


def _days(ms: int) -> str:
    return f"{ms / MS_PER_DAY:g} days"


def _wfe(oos_annualised: float | None, is_annualised: float | None) -> float | None:
    """Annualised OOS return over annualised IS return (spec 9.1).

    `None` when the IS return is not positive: a ratio over a negative denominator flips
    sign -- an OOS loss over an IS loss reads as *positive* efficiency -- and spec 9.1's
    "well below ~0.5 means fitting noise" reading only makes sense against an IS gain.
    A fold whose optimiser chose a losing parameter set is reported through its own
    columns, not through a ratio that would misread it.
    """
    if oos_annualised is None or is_annualised is None or is_annualised <= 0:
        return None
    return oos_annualised / is_annualised


def _empty_fold(
    fold: Fold,
    results: tuple[SweepResult, ...],
    values: tuple[float | None, ...],
    *,
    error: str,
) -> FoldRecord:
    return FoldRecord(
        fold=fold,
        grid=results,
        objective=values,
        chosen_index=None,
        chosen_params=None,
        is_sharpe=None,
        is_total_return=None,
        is_annualised=None,
        is_net_pnl=None,
        is_max_drawdown=None,
        is_round_trips=None,
        oos=None,
        is_halted=False,
        is_halt_limit=None,
        wfe=None,
        plateau=None,
        neighbours_defined=0,
        neighbours_total=0,
        neighbourhood_unprofitable=False,
        error=error,
    )


# ------------------------------------------------------------------- the OOS evaluation


def _run_oos(root: str, point: SweepPoint) -> dict[str, Any]:
    """One out-of-sample evaluation: a full engine run, windowed to the OOS range.

    Uses `execute_point` -- the same assembly the IS grid ran through -- so an OOS number
    is comparable with the IS number that chose it. The equity series is sliced to the
    window with the same last-observation-carried-forward convention `compute_metrics`
    uses, because the engine's series starts in warm-up and can end a moment after the
    range (a final-bar order genuinely fills late), and neither belongs in a curve that
    claims to be out-of-sample.
    """
    result = execute_point(root, point)

    times = result.equity_ms
    lo = bisect_right(times, point.start_ms) - 1
    if lo < 0:
        lo = 0
    hi = bisect_right(times, point.end_ms) - 1
    if hi < lo:
        hi = lo
    window_ms = list(times[lo : hi + 1])
    window_equity = list(result.equity[lo : hi + 1])

    days = (point.end_ms - point.start_ms) / MS_PER_DAY
    metrics = result.metrics
    return {
        "start_ms": point.start_ms,
        "end_ms": point.end_ms,
        "params": dict(point.params),
        "sharpe": metrics.sharpe,
        "sortino": metrics.sortino,
        "total_return": metrics.total_return,
        "annualised_return": annualised_return(metrics.total_return, days),
        "net_pnl": str(result.attribution.net_pnl),
        "max_drawdown": metrics.max_drawdown,
        "round_trips": metrics.trades.round_trips,
        "fills": result.fills,
        "halted": result.halt_reason is not None,
        "flags": list(result.flags),
        "event_hash": result.event_hash,
        "equity_ms": window_ms,
        "equity": window_equity,
    }


# ----------------------------------------------------------------------------- stitching


@dataclass
class _Stitched:
    times: list[int] = field(default_factory=list)
    equity: list[float] = field(default_factory=list)
    pnl: list[float] = field(default_factory=list)
    fold_index: list[int] = field(default_factory=list)
    scales: list[float] = field(default_factory=list)
    truncated_at_fold: int | None = None


def _stitch(
    records: Sequence[FoldRecord], opening_balance: float, warnings: list[str]
) -> _Stitched:
    """Join the fold OOS curves into one. See the module docstring for the two variants.

    Boundary discipline: each fold's first sample is clamped to its window start (the
    slice carries the equity *in force* at the boundary, whose own timestamp may precede
    it), and samples that do not advance the clock past the previous stitched sample are
    dropped. The result is strictly increasing in time, which is what lets a reader --
    and `build_grid` -- treat it as one series.

    A fold that opens at or below zero equity ends the compounded curve: there is no
    balance left to scale the next fold by. Later folds' additive PnL is *also* dropped
    rather than stitched past a bankruptcy, and the truncation is recorded -- a curve
    that quietly kept trading through zero is the exact fiction spec 8.2's truncation
    rule exists to prevent.
    """
    out = _Stitched()
    running = opening_balance
    cumulative_pnl = 0.0

    for record in records:
        if record.oos is None:
            continue
        times: Sequence[int] = record.oos["equity_ms"]
        equity: Sequence[float] = record.oos["equity"]
        if not times:
            warnings.append(
                f"fold {record.fold.index}: the OOS window held no equity samples, so "
                f"the stitched curve skips it"
            )
            continue
        opening = equity[0]
        if opening <= 0 or running <= 0:
            out.truncated_at_fold = record.fold.index
            warnings.append(
                f"fold {record.fold.index}: equity opened at {opening:.2f} with "
                f"{running:.2f} carried forward -- the stitched curve is truncated here, "
                f"because there is no balance left to compound"
            )
            break
        scale = running / opening
        stitched_before = len(out.times)
        for ts, value in zip(times, equity):
            stamped = max(ts, record.fold.oos_start_ms)
            if out.times and stamped <= out.times[-1]:
                continue
            out.times.append(stamped)
            out.equity.append(value * scale)
            out.pnl.append(opening_balance + cumulative_pnl + (value - opening))
            out.fold_index.append(record.fold.index)
        if len(out.times) == stitched_before:
            # A window can hold samples and still stitch none of them: when the engine's
            # series never advanced past the window start, `_run_oos` returns a single
            # carried-forward observation whose clamped timestamp collides with the
            # previous fold's close, and the clock check above drops it. Appending the
            # scale anyway (which this branch used to do) desynchronised `fold_scales`
            # from `set(fold_index)` -- the fold set every downstream denominator and
            # the artefact's own "per stitched fold" docstring are keyed on -- so a
            # reader pairing scales with stitched folds misattributed every scale after
            # the invisible one. The fold is skipped whole instead, and said out loud
            # like the empty-window case: `running` and `cumulative_pnl` stay put too,
            # because a curve must not carry PnL from a fold it does not contain.
            warnings.append(
                f"fold {record.fold.index}: none of the OOS window's samples advanced "
                f"the stitched clock, so the stitched curve skips it"
            )
            continue
        out.scales.append(scale)
        running = out.equity[-1]
        cumulative_pnl += equity[-1] - opening
    return out


# ----------------------------------------------------------------------------- stability


def _stability(shape: GridShape, records: Sequence[FoldRecord]) -> dict[str, Any]:
    """Chosen parameter values across folds, per axis (spec 9.1's stability plot).

    Values that jump around every fold mean there is no stable optimum to find -- the
    optimisation is chasing noise. Alongside the raw series, each axis carries the chosen
    value's *position* on the axis, because "jumped from 10 to 200" on a log-spaced axis
    is one step, and a plot of raw values would read it as a leap.
    """
    axes: dict[str, Any] = {}
    for axis_index, name in enumerate(shape.names):
        chosen_values: list[Any] = []
        chosen_positions: list[int | None] = []
        for record in records:
            if record.chosen_index is None:
                chosen_values.append(None)
                chosen_positions.append(None)
            else:
                position = shape.position(record.chosen_index)[axis_index]
                chosen_values.append(shape.values[axis_index][position])
                chosen_positions.append(position)
        distinct = len({v for v in chosen_values if v is not None})
        axes[name] = {
            "values": list(shape.values[axis_index]),
            "chosen": chosen_values,
            "chosen_position": chosen_positions,
            "distinct": distinct,
        }
    return {"axes": axes, "folds": len(records)}
