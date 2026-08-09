"""Parameter sweeps, run in parallel across processes.

A sweep is N backtests over one strategy with N parameter sets. There is no statistics here
and deliberately so: the sweep reports what each point returned and stops. Picking the best
one is spec 8.5's problem, not this module's, and a sweep that quietly ranked its own
results would be doing the selection nobody had accounted for.

## Why processes, and why not threads

Backtests are pure CPU in Python bytecode, so threads would serialise on the GIL and a
"parallel" sweep would take exactly as long as a serial one while looking faster in the
code. `ProcessPoolExecutor` gives real parallelism, at the cost of every argument having to
survive pickling -- which is why a worker is handed a `SweepPoint` of plain data and builds
its own engine, rather than being handed an engine.

The default worker count is `cpu_count() - 1`, floored at one. Leaving a core free is not
politeness: the parent process is serialising results and the machine is very likely also
running the collector, whose whole purpose is to not miss a message.

**The caller must live in an importable, `__main__`-guarded module.** Windows and macOS
spawn workers by re-importing the calling module, so a REPL, a `python -` script or a
notebook cell cannot supply one and every worker dies before it runs a line. `sweep()`
turns the resulting `BrokenProcessPool` into a message that says so; `max_workers=1` runs
in this process and is always available.

## What is *not* parallelised, and why it matters

Each worker opens its own DuckDB connection and reads the same Parquet files. That is fine
-- the lake is read-only during a sweep -- but it means memory scales with worker count,
and a `BOOK_WALK` sweep over a year of depth on eight workers will use eight times the
memory one run uses. `max_workers` is therefore a real knob rather than a formality.

**Determinism is per point, not across the pool.** Each point runs with the seed it was
given, so re-running a sweep reproduces every point exactly (spec 12.1) -- but the *order*
results arrive in depends on how long each took. Results are sorted back into grid order
before they are returned, so the output is a function of the inputs and nothing else.
"""

from __future__ import annotations

import itertools
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from perplab.core.account import FeeSchedule
from perplab.core.money import money_to_str, parse_money
from perplab.core.risk import RiskLimits
from perplab.engine.backtest import AutoFlatten, BacktestConfig, BacktestEngine
from perplab.engine.fills import fill_model_for_tier
from perplab.engine.latency import latency_from_json
from perplab.engine.runspec import RunSpec, resolve_brackets, resolve_filters
from perplab.engine.tiers import resolve_tier, tier_from_name
from perplab.data.gaps import detect_gaps
from perplab.data.manifest import market_root
from perplab.engine.worker import TIER_DATASETS
from perplab.strategy.loader import load_strategy_class

__all__ = [
    "SweepPoint",
    "SweepResult",
    "expand_grid",
    "execute_point",
    "points_from_spec",
    "run_point",
    "sweep",
    "default_workers",
]


def default_workers() -> int:
    """One fewer than the machine has, floored at one.

    The spare core is for the parent process and for the collector, which is very likely
    running on the same machine and whose 72-hour guarantee is worth more than the last
    10% of sweep throughput.
    """
    return max(1, (os.cpu_count() or 2) - 1)


def expand_grid(grid: Mapping[str, Sequence[Any]]) -> list[dict[str, Any]]:
    """Cartesian product of a parameter grid, in a stable order.

    Keys are sorted and values keep the order they were given, so the same grid always
    expands to the same list. A sweep whose point order depended on dict iteration would
    produce a different *trial sequence* between Python versions, and spec 8.5 counts
    trials.
    """
    if not grid:
        return [{}]
    names = sorted(grid)
    for name in names:
        if not grid[name]:
            raise ValueError(f"parameter {name!r} has no values to sweep over")
    return [
        dict(zip(names, combination))
        for combination in itertools.product(*(grid[name] for name in names))
    ]


@dataclass(frozen=True, slots=True)
class SweepPoint:
    """One backtest in a sweep: the spec, minus the code, plus this point's parameters.

    Carries the strategy source rather than a strategy id, for the same reason `RunSpec`
    does: a sweep that re-read the library between points could silently evaluate two
    different versions of the same strategy and report them as one grid.
    """

    index: int
    params: Mapping[str, Any]
    code: str
    class_name: str | None
    symbols: tuple[str, ...]
    timeframe: str
    start_ms: int
    end_ms: int
    seed: int
    opening_balance: str
    leverage: int
    maker_rate: str
    taker_rate: str
    fee_source: str
    latency: Mapping[str, Any]
    fill_tier: str
    liquidation_recovery_pct: str
    timeout_s: float
    risk_limits: Mapping[str, Any] = field(default_factory=dict)
    auto_flatten: Mapping[str, Any] = field(default_factory=dict)
    kill_switch_flatten: bool = False
    hedge_mode: bool = False
    """Carried so a sweep over a hedge-mode strategy runs the mode it was written for.

    Defaulted and last, so every existing construction of this dataclass is unchanged. A
    sweep that silently ran a hedge strategy one-way would refuse every side-routed order
    and report a grid of strategies that never traded."""


@dataclass(frozen=True, slots=True)
class SweepResult:
    """What one point produced, or why it did not produce anything.

    A point that raised is **kept**, with its error, rather than dropped. A sweep that
    silently discarded failures would report a grid with holes in it as a complete grid,
    and the holes are usually the interesting part -- a parameter set that liquidates, or
    one whose sizing the exchange refuses, is a finding rather than a gap.
    """

    index: int
    params: Mapping[str, Any]
    ok: bool
    fill_tier: str | None = None
    net_pnl: str | None = None
    sharpe: float | None = None
    max_drawdown: float | None = None
    round_trips: int | None = None
    fills: int = 0
    risk_rejects: int = 0
    halted: bool = False
    halt_limit: str | None = None
    flags: tuple[str, ...] = ()
    event_hash: str | None = None
    wall_s: float = 0.0
    error: str | None = None
    total_return: float | None = None
    """`E_end / E_start - 1` over the point's own range.

    Carried for the walk-forward's WFE, whose numerator and denominator are *annualised
    returns* (spec 9.1) -- a figure the Sharpe column cannot stand in for, because two
    points with equal Sharpe and different volatility earned different amounts.
    """

    def to_json(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "params": dict(self.params),
            "ok": self.ok,
            "fill_tier": self.fill_tier,
            "net_pnl": self.net_pnl,
            "sharpe": self.sharpe,
            "max_drawdown": self.max_drawdown,
            "round_trips": self.round_trips,
            "fills": self.fills,
            "risk_rejects": self.risk_rejects,
            "halted": self.halted,
            "halt_limit": self.halt_limit,
            "flags": list(self.flags),
            "event_hash": self.event_hash,
            "wall_s": self.wall_s,
            "error": self.error,
            "total_return": self.total_return,
        }


def execute_point(root: str, point: SweepPoint) -> Any:
    """Assemble the engine for one point and run it. Raises on failure.

    The one place a sweep point becomes an engine, shared by `run_point` and by the
    walk-forward's out-of-sample runner. Two assemblies would be two chances for the
    resolution order -- gaps, then tier, then config -- to drift apart, and a grid whose
    points were built one way and evaluated another is not a parameter surface (spec 6.1's
    argument, applied to the Lab).

    Returns the engine's `BacktestResult`. Must stay picklable-friendly in its *arguments*
    only; the result is consumed in whichever process ran it.
    """
    base = Path(root)
    # `class_name` is carried on the point for the record, not passed here: the
    # loader finds the single `Strategy` subclass in the module, and a sweep that
    # re-derived it would be a second implementation of that rule.
    strategy_class = load_strategy_class(point.code)
    strategy = strategy_class(dict(point.params))
    requirements = strategy.declared

    filters, _ = resolve_filters(base, point.symbols, point.start_ms)
    brackets, _ = resolve_brackets(base, point.symbols, point.start_ms)
    # **The same gap report the worker uses.** Without it, `resolve_tier` judges
    # coverage at partition granularity only, and a range with a genuine hole in its
    # tick data resolves to `TRADE_ONLY` here while a single run of the identical spec
    # executes at `BAR_CLOSE` -- with no degradation flag on the sweep to say so. The
    # tier changes the answer (the two produce different event hashes), so a grid
    # resolved without it is not comparable with anything.
    gaps: list[Any] = []
    for symbol in point.symbols:
        gaps.extend(
            detect_gaps(
                market_root(base),
                symbol,
                point.start_ms,
                point.end_ms,
                datasets=TIER_DATASETS,
            ).gaps
        )
    resolution = resolve_tier(
        base,
        point.symbols,
        point.start_ms,
        point.end_ms,
        requested=tier_from_name(point.fill_tier),
        gaps=gaps,
    )
    config = BacktestConfig(
        symbols=point.symbols,
        timeframe=point.timeframe,
        start_ms=point.start_ms,
        end_ms=point.end_ms,
        seed=point.seed,
        opening_balance=parse_money(point.opening_balance),
        leverage=point.leverage,
        hedge_mode=point.hedge_mode,
        fees=FeeSchedule(
            maker_rate=parse_money(point.maker_rate),
            taker_rate=parse_money(point.taker_rate),
            source=point.fee_source,
        ),
        latency=latency_from_json(dict(point.latency)),
        fill_tier=resolution.tier,
        fill_model=fill_model_for_tier(resolution.tier.name),
        liquidation_recovery_pct=parse_money(point.liquidation_recovery_pct),
        timeout_s=point.timeout_s,
        risk=RiskLimits.from_json(point.risk_limits),
        auto_flatten=AutoFlatten.from_json(point.auto_flatten),
        kill_switch_flatten=point.kill_switch_flatten,
    )
    engine = BacktestEngine(
        root=market_root(base),
        strategy=strategy,
        requirements=requirements,
        config=config,
        filters=filters,
        brackets=brackets,
        flags=resolution.flags,
    )
    return engine.run()


def run_point(root: str, point: SweepPoint) -> SweepResult:
    """Execute one sweep point. Runs in a worker process; must stay picklable.

    A module-level function rather than a closure or a method, because
    `ProcessPoolExecutor` pickles the callable by qualified name and neither of those
    survives it.
    """
    import time

    started = time.perf_counter()
    try:
        result = execute_point(root, point)
        return SweepResult(
            index=point.index,
            params=dict(point.params),
            ok=True,
            fill_tier=result.fill_tier,
            net_pnl=money_to_str(result.attribution.net_pnl),
            sharpe=result.metrics.sharpe,
            max_drawdown=result.metrics.max_drawdown,
            round_trips=result.metrics.trades.round_trips,
            fills=result.fills,
            risk_rejects=result.risk_rejects,
            halted=result.halt_reason is not None,
            halt_limit=None if result.halt_reason is None else result.halt_reason.limit,
            flags=result.flags,
            event_hash=result.event_hash,
            wall_s=time.perf_counter() - started,
            total_return=result.metrics.total_return,
        )
    except (KeyboardInterrupt, SystemExit):
        # **Not data.** A failed point is a finding; an interrupt is an instruction. Catching
        # it turned Ctrl-C into a grid cell reading `"error": "KeyboardInterrupt: "` and let
        # the sweep run every remaining point, which is the opposite of what was asked.
        raise
    except BaseException as exc:  # noqa: BLE001 - a failed point is data, not control flow
        return SweepResult(
            index=point.index,
            params=dict(point.params),
            ok=False,
            error=f"{type(exc).__name__}: {exc}",
            wall_s=time.perf_counter() - started,
        )


def points_from_spec(
    spec: RunSpec, grid: Mapping[str, Sequence[Any]]
) -> list[SweepPoint]:
    """Expand a grid against a base `RunSpec`, so a sweep varies only the parameters.

    Everything a run's answer depends on except the parameters comes from the spec, which
    means a sweep cannot accidentally compare points that also differ in fee schedule,
    latency model or fill tier. Those would be a different experiment wearing a sweep's
    name.
    """
    combinations = expand_grid(grid)
    return [
        SweepPoint(
            index=index,
            params={**dict(spec.params), **combination},
            code=spec.code,
            class_name=spec.class_name,
            symbols=tuple(spec.symbols),
            timeframe=spec.timeframe,
            start_ms=spec.start_ms,
            end_ms=spec.end_ms,
            seed=spec.seed,
            opening_balance=spec.opening_balance,
            leverage=spec.leverage,
            hedge_mode=spec.hedge_mode,
            maker_rate=spec.maker_rate,
            taker_rate=spec.taker_rate,
            fee_source=spec.fee_source,
            latency=dict(spec.latency),
            fill_tier=spec.fill_tier,
            liquidation_recovery_pct=spec.liquidation_recovery_pct,
            timeout_s=spec.timeout_s,
            risk_limits=dict(spec.risk_limits),
            auto_flatten=dict(spec.auto_flatten),
            kill_switch_flatten=spec.kill_switch_flatten,
        )
        for index, combination in enumerate(combinations)
    ]


def sweep(
    root: Path | str,
    points: Sequence[SweepPoint],
    *,
    max_workers: int | None = None,
    on_result: Callable[[SweepResult], None] | None = None,
) -> list[SweepResult]:
    """Run every point, in parallel, and return the results in grid order.

    `on_result` fires as each point finishes, in completion order, for progress reporting.
    The returned list is sorted by `index` regardless, so the value of this function is a
    function of its arguments and not of scheduling -- which is what makes a sweep
    comparable with the sweep somebody ran last week.

    A single point runs in this process rather than paying for a pool. `run_point` still
    reduces the exception to `type: message` -- the traceback is not carried across, in
    either mode -- but the failure happens on this stack, so a debugger or a `-X faulthandler`
    sees it.
    """
    if not points:
        return []
    root_text = str(root)
    workers = max_workers if max_workers is not None else default_workers()
    if workers < 1:
        raise ValueError(f"max_workers must be at least 1, got {workers}")

    if workers == 1 or len(points) == 1:
        results = []
        for point in points:
            result = run_point(root_text, point)
            if on_result is not None:
                on_result(result)
            results.append(result)
        return sorted(results, key=lambda r: r.index)

    results: list[SweepResult] = []
    try:
        with ProcessPoolExecutor(max_workers=min(workers, len(points))) as pool:
            futures = {pool.submit(run_point, root_text, p): p for p in points}
            for future in as_completed(futures):
                result = future.result()
                if on_result is not None:
                    on_result(result)
                results.append(result)
    except BrokenProcessPool as exc:
        # **Almost always the caller's module, not the sweep.** Windows spawns workers by
        # re-importing `__main__`, so a caller that is a stdin script, a REPL, or a module
        # without an `if __name__ == "__main__":` guard cannot be re-imported and every
        # worker dies before it runs a line. The raw `BrokenProcessPool` says none of that.
        raise RuntimeError(
            "the sweep's worker processes died before running. On Windows and macOS "
            "`ProcessPoolExecutor` starts workers by re-importing the calling module, so "
            "`sweep()` must be called from a module guarded by `if __name__ == "
            '"__main__":` -- not from a REPL, a `python -` script, or a notebook cell. '
            f"Pass `max_workers=1` to run in this process instead. ({exc})"
        ) from exc
    return sorted(results, key=lambda r: r.index)
