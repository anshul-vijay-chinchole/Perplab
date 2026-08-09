"""The Lab job worker -- one tool invocation, one process (spec 2.3 applied to spec 9).

```
python -m perplab.lab.worker <userdata-root> <job-id>
```

A process for the same two reasons backtests get one: a walk-forward runs strategy code,
which the API server never does (spec 11), and a grid sweep with a runaway point has to
be killable without taking the platform down (spec 2.3). Monte Carlo and regime analysis
run no strategy code, but they run in the same worker anyway -- one lifecycle, one reap
path, one thing that can be wrong.

**This module is the assembly seam.** The tools themselves (`walkforward`, `montecarlo`,
`regimes`) are pure: they take prepared series and return structures. Everything that
touches disk -- the run's spec and artefacts, the lake, the trials counter -- happens
here, so a tool's arithmetic is testable without a run directory and this file's job is
only to fetch honestly. The one rule of that fetching: what could not be fetched is
reported as absent (`None`, a flag, a refusal), never substituted.

Artefacts land in the job directory before the row says done, in `RunStore`'s discipline:

- `result.json` -- the tool's full output, under a key named after the tool
- `stitched.parquet` -- walk-forward only: the stitched OOS curve, both variants
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from perplab.analytics.metrics import ReturnGrid, build_grid
from perplab.core.money import SCALE
from perplab.core.types import Bar
from perplab.data.manifest import market_root
from perplab.data.query import derive_timeframe, partition_predicate, query
from perplab.engine.runspec import RunSpec
from perplab.lab.montecarlo import MonteCarloConfig, MonteCarloInputs, run_montecarlo
from perplab.lab.overfit import diagnostics
from perplab.lab.regimes import RegimeConfig, RegimeInputs, run_regimes
from perplab.lab.walkforward import (
    GridShape,
    WalkForwardConfig,
    WalkForwardResult,
    run_walkforward,
)
from perplab.store.lab import LabStore
from perplab.store.runs import RunStatus, RunStore

__all__ = ["execute_job", "main"]


class JobRefused(ValueError):
    """The job cannot run against this source run, and the message says why."""


def execute_job(root: Path, job_id: int, labs: LabStore, runs: RunStore) -> None:
    labs.mark_running(job_id)
    job = labs.read_json(job_id, "job.json")
    tool = str(job["tool"])
    run_id = int(job["run_id"])
    config = job.get("config") or {}

    summary = runs.get(run_id)
    if summary.status != RunStatus.DONE:
        raise JobRefused(
            f"every Lab tool operates on a *completed* run (spec 9), and run {run_id} "
            f"is {summary.status}"
        )
    spec = RunSpec.from_storage(runs.read_json(run_id, "spec.json"))

    if tool == "walkforward":
        _walkforward(root, job_id, labs, runs, spec, config)
    elif tool == "montecarlo":
        _montecarlo(job_id, labs, runs, run_id, spec, config)
    elif tool == "regimes":
        _regimes(root, job_id, labs, runs, run_id, spec, config)
    else:
        raise JobRefused(f"unknown Lab tool {tool!r}")


# ------------------------------------------------------------------------ walk-forward


def _walkforward(
    root: Path,
    job_id: int,
    labs: LabStore,
    runs: RunStore,
    spec: RunSpec,
    config_json: dict[str, Any],
) -> None:
    if spec.source or spec.session_kind:
        raise JobRefused(
            "a walk-forward re-runs the engine over the lake with new windows and "
            "parameters, and this run was a session (or a replay of one): its data came "
            "from a tape covering only its own window. Run a walk-forward on a lake "
            "backtest instead."
        )
    config = WalkForwardConfig.from_json(config_json)

    def _stop() -> bool:
        return labs.stop_requested(job_id)

    def _trial(params: dict[str, Any], sharpe: float | None) -> None:
        runs.record_trial(
            strategy_id=spec.strategy_id, params=params, sharpe=sharpe, run_id=None
        )

    try:
        result = run_walkforward(
            root,
            spec,
            config,
            progress=lambda done, total: labs.progress(job_id, done, total),
            on_trial=_trial,
            should_stop=_stop,
        )
    except KeyboardInterrupt:
        labs.fail(job_id, "cancelled by the user", status=RunStatus.CANCELLED)
        return

    overfit = diagnostics(result, runs.trials(spec.strategy_id))
    _write_stitched(labs.artefact(job_id, "stitched.parquet"), result)
    _write_json(
        labs.artefact(job_id, "result.json"),
        {
            "tool": "walkforward",
            "strategy_id": spec.strategy_id,
            "walkforward": result.to_json(),
            "overfit": overfit,
        },
    )
    labs.complete(
        job_id,
        summary={
            "folds": len(result.folds),
            "grid_points": GridShape.from_grid(config.grid).size,
            "objective": config.objective,
            "wfe_aggregate": result.wfe_aggregate,
            "wfe_median": result.wfe_median,
            "stitched_total_return": result.stitched_total_return,
            "stitched_annualised": result.stitched_annualised,
            "truncated_at_fold": result.truncated_at_fold,
            "uncovered_ms": result.uncovered_ms,
            "warnings": len(result.warnings),
        },
    )


def _write_stitched(path: Path, result: WalkForwardResult) -> None:
    table = pa.table(
        {
            "ts_ms": pa.array(result.stitched_ms, type=pa.int64()),
            "equity": pa.array(result.stitched_equity, type=pa.float64()),
            "pnl": pa.array(result.stitched_pnl, type=pa.float64()),
            "fold": pa.array(result.stitched_fold_index, type=pa.int32()),
        }
    )
    tmp = path.parent / f".{path.name}.tmp"
    pq.write_table(table, tmp, compression="zstd", compression_level=3)
    tmp.replace(path)


# ------------------------------------------------------------------------ monte carlo


def _run_grid(runs: RunStore, run_id: int, spec: RunSpec) -> ReturnGrid:
    """The run's metric grid, rebuilt from its stored equity series.

    `build_grid` with the run's own range reproduces exactly the grid `compute_metrics`
    used, because it *is* the same function over the same series -- the resampling
    conventions (aligned UTC boundaries, LOCF, whole steps only) travel with it.
    """
    path = runs.artefact(run_id, "equity.parquet")
    if not path.exists():
        raise JobRefused(f"run {run_id} has no stored equity series to resample")
    table = pq.read_table(path)
    return build_grid(
        table.column("ts_ms").to_pylist(),
        table.column("equity").to_pylist(),
        start_ms=spec.start_ms,
        end_ms=spec.end_ms,
    )


def _closed_trades(runs: RunStore, run_id: int) -> list[tuple[int, float]]:
    """`(entry_ms, net_pnl)` for closed round-trips, in entry order.

    Open trades are excluded: an unrealised result is not a draw from the trade
    distribution (spec 8.1's argument, applied to resampling). `net_pnl` is parsed from
    the exact decimal string the artefact stores; float is the right target because
    everything downstream is distribution arithmetic, not accounting.
    """
    payload = runs.read_json(run_id, "trades.json")
    closed = [
        (int(t["entry_ms"]), float(t["net_pnl"]))
        for t in payload.get("trades", [])
        if t.get("exit_ms") is not None
    ]
    closed.sort(key=lambda pair: pair[0])
    return closed


def _drawdown_limit(spec: RunSpec) -> float | None:
    value = spec.risk_limits.get("max_drawdown_pct") if spec.risk_limits else None
    return None if value is None else float(value)


def _montecarlo(
    job_id: int,
    labs: LabStore,
    runs: RunStore,
    run_id: int,
    spec: RunSpec,
    config_json: dict[str, Any],
) -> None:
    config = MonteCarloConfig.from_json(config_json)
    grid = _run_grid(runs, run_id, spec)
    inputs = MonteCarloInputs(
        trade_pnls=tuple(pnl for _, pnl in _closed_trades(runs, run_id)),
        grid_returns=grid.returns,
        periods_per_year=grid.periods_per_year,
        grid_label=grid.label,
        opening_balance=float(spec.opening_balance),
        max_drawdown_limit=_drawdown_limit(spec),
    )
    labs.progress(job_id, 0, 1)
    result = run_montecarlo(inputs, config)
    _write_json(
        labs.artefact(job_id, "result.json"),
        {"tool": "montecarlo", "montecarlo": result},
    )
    labs.complete(
        job_id,
        summary={
            "iterations": config.iterations,
            "methods": list(config.methods),
            "trades": len(inputs.trade_pnls),
            "grid_returns": len(inputs.grid_returns),
        },
    )


# ---------------------------------------------------------------------------- regimes


def _daily_bars(
    root: Path, symbol: str, start_ms: int, end_ms: int
) -> tuple[Bar, ...]:
    """Completed daily bars over `[start_ms, end_ms)`, derived from stored 1 m bars.

    `on_incomplete="drop"`: these bars feed indicators, and a partial daily bar is a lie
    about the day (the `derive_timeframe` docstring's own words). Dropping is visible
    downstream as an `unclassified` stretch, which is the honest rendering of a gap.
    """
    table = derive_timeframe(
        market_root(root),
        "1d",
        symbol=symbol,
        start_ms=start_ms,
        end_ms=end_ms,
        on_incomplete="drop",
    )
    rows = zip(
        table.column("open_time").to_pylist(),
        table.column("close_time").to_pylist(),
        table.column("open").to_pylist(),
        table.column("high").to_pylist(),
        table.column("low").to_pylist(),
        table.column("close").to_pylist(),
        table.column("volume").to_pylist(),
        table.column("quote_volume").to_pylist(),
        table.column("count").to_pylist(),
    )
    return tuple(
        Bar(
            symbol=symbol,
            open_time=open_time,
            close_time=close_time,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            quote_volume=quote_volume,
            trades=count,
        )
        for open_time, close_time, open_, high, low, close, volume, quote_volume, count in rows
    )


def _funding_rates(
    root: Path, symbol: str, start_ms: int, end_ms: int
) -> tuple[tuple[int, float], ...]:
    predicate = partition_predicate("funding", symbol=symbol)
    table = query(
        market_root(root),
        f'SELECT "calc_time", "funding_rate" FROM "funding" '
        f"WHERE {predicate} AND \"calc_time\" >= {int(start_ms)} "
        f'AND "calc_time" < {int(end_ms)} ORDER BY "calc_time"',
        datasets=("funding",),
    )
    return tuple(
        (int(ts), rate / SCALE)
        for ts, rate in zip(
            table.column("calc_time").to_pylist(),
            table.column("funding_rate").to_pylist(),
        )
    )


def _liquidations(
    root: Path, symbol: str, start_ms: int, end_ms: int
) -> tuple[tuple[int, float], ...] | None:
    """`(ts_ms, notional)` over the range, or `None` when the dataset has no files.

    The distinction is the cascade dimension's honesty: an empty range in a dataset that
    exists means "no liquidations happened" -- quiet as an observation. A dataset with no
    files at all means "nobody was listening", and labelling that quiet would be a claim
    about a market nobody watched.
    """
    directory = market_root(root) / "liquidations"
    if not directory.is_dir() or not any(directory.rglob("*.parquet")):
        return None
    predicate = partition_predicate(
        "liquidations", symbol=symbol, start_ms=start_ms, end_ms=end_ms
    )
    table = query(
        market_root(root),
        f'SELECT "ts_ms", "qty", "price", "avg_price" FROM "liquidations" '
        f"WHERE {predicate} AND \"ts_ms\" >= {int(start_ms)} "
        f'AND "ts_ms" < {int(end_ms)} ORDER BY "ts_ms"',
        datasets=("liquidations",),
    )
    out: list[tuple[int, float]] = []
    for ts, qty, price, avg_price in zip(
        table.column("ts_ms").to_pylist(),
        table.column("qty").to_pylist(),
        table.column("price").to_pylist(),
        table.column("avg_price").to_pylist(),
    ):
        # The average fill price is what the forced order actually traded at; the order
        # price is the fallback for rows where the stream carried no average.
        reference = avg_price if avg_price else price
        out.append((int(ts), (qty / SCALE) * (reference / SCALE)))
    return tuple(out)


def _regimes(
    root: Path,
    job_id: int,
    labs: LabStore,
    runs: RunStore,
    run_id: int,
    spec: RunSpec,
    config_json: dict[str, Any],
) -> None:
    config = RegimeConfig.from_json(config_json)
    grid = _run_grid(runs, run_id, spec)
    if not grid.returns:
        raise JobRefused(
            "the run's range is shorter than one metric grid step, so there are no "
            "periods to label"
        )
    symbol = spec.symbols[0]

    ms_per_day = 86_400_000
    # Enough history before the range for every indicator to be defined by the run's
    # first period: the vol window plus its expanding-quantile minimum, or the ADX
    # warm-up, whichever reaches further. A couple of spare days cover boundary
    # truncation; genuinely absent history surfaces as `unclassified`, not as an error.
    context_days = (
        max(
            config.vol_window_days + config.vol_min_history,
            2 * config.adx_period,
        )
        + 3
    )
    bars_start = spec.start_ms - context_days * ms_per_day
    funding_start = spec.start_ms - config.funding_trailing_ms - ms_per_day
    cascade_start = spec.start_ms - config.cascade_cluster_ms - config.cascade_tag_ms

    labs.progress(job_id, 0, 1)
    inputs = RegimeInputs(
        grid_times=grid.times,
        grid_returns=grid.returns,
        periods_per_year=grid.periods_per_year,
        grid_label=grid.label,
        trades=tuple(_closed_trades(runs, run_id)),
        daily_bars=_daily_bars(root, symbol, bars_start, spec.end_ms),
        funding=_funding_rates(root, symbol, funding_start, spec.end_ms),
        liquidations=_liquidations(root, symbol, cascade_start, spec.end_ms),
    )
    result = run_regimes(inputs, config)
    if len(spec.symbols) > 1:
        result["notes"].append(
            f"this run traded {len(spec.symbols)} symbols; regimes are labelled from "
            f"{symbol}'s market data only"
        )
    _write_json(
        labs.artefact(job_id, "result.json"), {"tool": "regimes", "regimes": result}
    )
    labs.complete(
        job_id,
        summary={
            "periods": result["periods"],
            "grid": result["grid"],
            "dimensions": [
                name
                for name, dim in result["dimensions"].items()
                if dim.get("available", True)
            ],
        },
    )


# ------------------------------------------------------------------------------ entry


def _write_json(path: Path, payload: Any) -> None:
    tmp = path.parent / f".{path.name}.tmp"
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    tmp.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m perplab.lab.worker",
        description="Execute one stored Lab job. Started by the API server.",
    )
    parser.add_argument("root", type=Path, help="the userdata root")
    parser.add_argument("job_id", type=int)
    args = parser.parse_args(argv)

    labs = LabStore(args.root)
    runs = RunStore(args.root)
    try:
        execute_job(args.root, args.job_id, labs, runs)
    except BaseException as exc:  # noqa: BLE001 - a failed job is data, not a crash
        detail = f"{type(exc).__name__}: {exc}\n\n" + "".join(
            traceback.format_exception(exc)
        )
        try:
            labs.fail(args.job_id, detail)
        finally:
            print(detail, file=sys.stderr)
        return 1
    finally:
        labs.close()
        runs.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    raise SystemExit(main())
