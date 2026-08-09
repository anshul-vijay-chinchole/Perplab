"""The Lab API (spec 9, 10.3): submit a tool against a completed run, poll, fetch.

Config validation happens **here**, with the tool's own `from_json`, before a row is
created -- the same argument as `start_run`'s: a bad block length or an overlapping step
refused at submit time comes back as a 400 naming the field, while the same mistake
discovered in the worker is a queued job that dies on its first line and reads as a
platform fault.
"""

from __future__ import annotations

from typing import Any

import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from perplab.analytics.metrics import build_grid
from perplab.api.deps import get_lab, get_runs
from perplab.engine.runspec import RunSpec
from perplab.lab.compare import ComparisonError, compare_runs
from perplab.lab.montecarlo import MonteCarloConfig
from perplab.lab.regimes import RegimeConfig
from perplab.lab.walkforward import WalkForwardConfig
from perplab.store.lab import LAB_TOOLS, LabNotFound, LabStore
from perplab.store.runs import RunNotFound, RunStatus, RunStore

router = APIRouter(tags=["lab"])

MAX_STITCHED_POINTS = 4000


MAX_ID = 2**63 - 1
"""SQLite binds an INTEGER as int64 and raises `OverflowError` above it, which escaped as
a 500. FastAPI's `int` is arbitrary-precision, so the bound has to be stated here."""


def _checked_id(value: int, what: str) -> int:
    if not 0 < value <= MAX_ID:
        raise HTTPException(status_code=404, detail=f"no {what} with id {value}")
    return value


class SubmitJobRequest(BaseModel):
    run_id: int = Field(gt=0, le=MAX_ID)
    tool: str
    config: dict[str, Any] = Field(default_factory=dict)
    label: str = ""


_VALIDATORS = {
    "walkforward": WalkForwardConfig.from_json,
    "montecarlo": MonteCarloConfig.from_json,
    "regimes": RegimeConfig.from_json,
}


@router.post("/lab/jobs", status_code=201)
def submit_job(
    request: SubmitJobRequest,
    lab: LabStore = Depends(get_lab),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    if request.tool not in LAB_TOOLS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown Lab tool {request.tool!r}: use one of {list(LAB_TOOLS)}",
        )
    summary = runs.get(request.run_id)  # 404s via the RunNotFound handler
    if summary.status != RunStatus.DONE:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"every Lab tool operates on a completed run (spec 9); run "
                f"{request.run_id} is {summary.status}"
            ),
        )
    try:
        _VALIDATORS[request.tool](request.config)
    except (ValueError, KeyError, TypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None

    if request.tool == "walkforward":
        # Refused at submit rather than in the worker, so the message reaches the form.
        spec = RunSpec.from_storage(runs.read_json(request.run_id, "spec.json"))
        if spec.source or spec.session_kind:
            raise HTTPException(
                status_code=400,
                detail=(
                    "a walk-forward re-runs the engine over the lake, and this run was "
                    "a session or a replay of one -- its data came from a tape covering "
                    "only its own window. Pick a lake backtest."
                ),
            )
        config = WalkForwardConfig.from_json(request.config)
        try:
            # Fold count is a function of the config *and* the run's range, so it can
            # only be checked once both are known -- here, not in `from_json`.
            config.check_against(summary.start_ms or 0, summary.end_ms or 0)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None

    job_id = lab.create(
        run_id=request.run_id,
        tool=request.tool,
        config=request.config,
        label=request.label,
    )
    lab.launch(job_id)
    return {"job": lab.get(job_id).to_json()}


@router.get("/lab/jobs")
def list_jobs(
    run_id: int | None = None,
    job_status: str | None = Query(None, alias="status"),
    limit: int = Query(200, ge=1, le=1000),
    lab: LabStore = Depends(get_lab),
) -> dict[str, Any]:
    if run_id is not None:
        _checked_id(run_id, 'run')
    items = lab.list(run_id=run_id, status=job_status, limit=limit)
    return {"jobs": [item.to_json() for item in items]}


@router.get("/lab/jobs/{job_id}")
def get_job(job_id: int, lab: LabStore = Depends(get_lab)) -> dict[str, Any]:
    return {"job": lab.get(_checked_id(job_id, 'Lab job')).to_json()}


@router.get("/lab/jobs/{job_id}/result")
def get_result(job_id: int, lab: LabStore = Depends(get_lab)) -> dict[str, Any]:
    job = lab.get(_checked_id(job_id, 'Lab job'))
    payload = lab.read_json(job_id, "result.json")
    payload["job"] = job.to_json()
    return payload


@router.get("/lab/jobs/{job_id}/stitched")
def get_stitched(
    job_id: int,
    points: int = Query(2000, ge=100, le=MAX_STITCHED_POINTS),
    lab: LabStore = Depends(get_lab),
) -> dict[str, Any]:
    """The stitched OOS curve, downsampled by extremes like the runs equity endpoint.

    Both variants ship together: the compounded curve is the headline and the additive
    PnL curve is the fixed-notional reading, and a client that has to make two requests
    to compare them will not compare them.
    """
    job = lab.get(_checked_id(job_id, "Lab job"))
    path = lab.artefact(job_id, "stitched.parquet")
    if not path.exists():
        raise LabNotFound(f"Lab job {job_id} has no stitched curve")
    if job.status != RunStatus.DONE:
        # A walk-forward that died between writing this file and marking the row leaves
        # a real Parquet file describing a partial answer. Serving it unqualified is how
        # a fragment gets quoted as a result.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Lab job {job_id} is {job.status}; its stitched curve is whatever had "
                f"been written when it stopped, not a finished walk-forward"
            ),
        )
    from perplab.api.routers.runs import _downsample_extremes

    table = pq.read_table(path)
    times = table.column("ts_ms").to_pylist()
    equity = table.column("equity").to_pylist()
    pnl = table.column("pnl").to_pylist()
    folds = table.column("fold").to_pylist()
    keep = _downsample_extremes(times, equity, points // 2)
    return {
        "ts": [times[i] for i in keep],
        "equity": [equity[i] for i in keep],
        "pnl": [pnl[i] for i in keep],
        "fold": [folds[i] for i in keep],
        "samples": len(times),
        "returned": len(keep),
    }


@router.get("/runs/{run_id}/portfolio")
def get_portfolio(
    run_id: int,
    window: int = Query(30, ge=3, le=365),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    """The spec 9.5 report for a multi-symbol run. Synchronous, like compare."""
    from perplab.lab.portfolio import portfolio_report

    summary = runs.get(_checked_id(run_id, "run"))
    path = runs.artefact(run_id, "per_symbol.parquet")
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                f"run {run_id} has no per-symbol series. Single-symbol runs do not "
                f"record one -- their PnL curve is the equity curve on the run page -- "
                f"and runs from before Phase 9 predate the artefact."
            ),
        )
    table = pq.read_table(path)
    times = table.column("ts_ms").to_pylist()
    symbol_pnl = {
        name: table.column(name).to_pylist()
        for name in table.schema.names
        if name != "ts_ms"
    }
    trips: dict[str, int] = {}
    for trade in runs.read_json(run_id, "trades.json").get("trades", []):
        if trade.get("exit_ms") is not None:
            trips[trade["symbol"]] = trips.get(trade["symbol"], 0) + 1
    try:
        report = portfolio_report(
            times=times,
            symbol_pnl=symbol_pnl,
            start_ms=summary.start_ms or 0,
            end_ms=summary.end_ms or 1,
            round_trips=trips,
            rolling_window=window,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    report["run_id"] = run_id
    return report


@router.get("/lab/compare")
def compare(
    runs_csv: str = Query(..., alias="runs", max_length=200),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    """Spec 9.6. Synchronous -- a comparison is a read, not a job.

    Mismatched ranges come back 409: the check is `compare_runs`'s own, and the refusal
    text names the runs so the user knows which one to re-run rather than which chart
    not to trust.
    """
    try:
        ids = [int(part) for part in runs_csv.split(",") if part.strip()]
    except ValueError:
        raise HTTPException(
            status_code=400, detail="runs must be a comma-separated list of run ids"
        ) from None
    if not 2 <= len(ids) <= 8:
        raise HTTPException(
            status_code=400, detail=f"compare 2 to 8 runs at once, got {len(ids)}"
        )
    if len(set(ids)) != len(ids):
        raise HTTPException(status_code=400, detail="the same run is listed twice")
    # Bounded before any of them reaches the store: `int(part)` above happily parses a
    # 200-digit number, and the `OverflowError` SQLite then raises is not a `ValueError`,
    # so the try/except around the parse never saw it and it surfaced as a 500.
    for run_id in ids:
        _checked_id(run_id, "run")

    entries = []
    for run_id in ids:
        summary = runs.get(run_id)
        if summary.status != RunStatus.DONE:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"run {run_id} is {summary.status}; only completed runs compare",
            )
        path = runs.artefact(run_id, "equity.parquet")
        if not path.exists():
            raise RunNotFound(f"run {run_id} has no stored equity series")
        table = pq.read_table(path)
        grid = build_grid(
            table.column("ts_ms").to_pylist(),
            table.column("equity").to_pylist(),
            start_ms=summary.start_ms or 0,
            end_ms=summary.end_ms or 1,
        )
        metrics = runs.read_json(run_id, "metrics.json").get("metrics")
        entries.append(
            {
                "run_id": run_id,
                "label": f"{summary.strategy_name} v{summary.version_no}"
                + (f" · {summary.label}" if summary.label else ""),
                "start_ms": summary.start_ms,
                "end_ms": summary.end_ms,
                "grid_times": list(grid.times),
                "grid_equity": list(grid.equity),
                "grid_returns": list(grid.returns),
                "metrics": metrics,
            }
        )
    try:
        return compare_runs(entries)
    except ComparisonError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from None


@router.post("/lab/jobs/{job_id}/cancel")
def cancel_job(job_id: int, lab: LabStore = Depends(get_lab)) -> dict[str, Any]:
    return {"job": lab.cancel(_checked_id(job_id, 'Lab job')).to_json()}


@router.delete("/lab/jobs/{job_id}")
def delete_job(job_id: int, lab: LabStore = Depends(get_lab)) -> dict[str, Any]:
    try:
        lab.delete(_checked_id(job_id, 'Lab job'))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED, detail=str(exc)
        ) from None
    return {"deleted": job_id}
