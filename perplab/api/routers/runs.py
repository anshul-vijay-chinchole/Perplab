"""Run endpoints -- the Runs tab's server half (spec 10.3).

Three things here are shaped by what a results page actually has to do, rather than by what
is easiest to serve.

**Starting a run returns immediately.** `POST /runs` writes the row, spawns the worker and
comes back; the client polls. A synchronous endpoint would hold a request open for the
length of a backtest, and the one thing worse than waiting is a browser that has already
timed out on a run which is still going.

**The equity series is downsampled by extremes, not by stride.** A year of 1-minute
mark-to-market samples is close to a million points and no screen has a million pixels, so
something has to go. Taking every *n*-th sample would delete precisely the spikes that
matter -- spec 8.2 measures drawdown on every tick for exactly that reason, and a chart that
smooths the trough away contradicts the number printed beside it. Each bucket therefore
contributes its minimum *and* its maximum, in timestamp order, so the rendered curve touches
every extreme the metrics were computed from.

**Money is served as strings.** Every exact figure -- net PnL, fees, funding, per-trade
results -- crosses as decimal text, because JSON numbers are IEEE doubles and a balance that
round-trips through one is no longer the balance the ledger computed. Ratios and metrics are
genuine floats and are sent as numbers.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field

from perplab.api.deps import get_library, get_root, get_runs
from perplab.data.query import TIMEFRAMES, derive_timeframe, query
from perplab.data.schemas import market_root, normalise_symbol
from perplab.data.manifest import CoverageError
from perplab.core.risk import RiskLimits
from perplab.core.types import MarginMode
from perplab.engine import ENGINE_VERSION
from perplab.engine.backtest import AutoFlatten
from perplab.engine.fills import DEFAULT_SLIPPAGE_BPS, fill_model_from_json
from perplab.engine.latency import DEFAULT_CANCEL_MS, DEFAULT_SUBMIT_MS, latency_from_json
from perplab.engine.runspec import RunSpec
from perplab.engine.tiers import TIER_CAPABILITIES, resolve_tier, tier_from_name
from perplab.store.runs import (
    PROGRESS_EQUITY_NAME,
    RunNotFound,
    RunStatus,
    RunStore,
)
from perplab.strategy.library import StrategyLibrary

router = APIRouter(tags=["runs"])

MAX_EQUITY_POINTS = 4000
"""Ceiling on the points an equity response carries.

Two per bucket, so this is 2 000 buckets -- more than the horizontal pixels any laptop gives
a chart, which is the point at which more data stops being more information.
"""


# --------------------------------------------------------------------------- schemas


class StartRunRequest(BaseModel):
    """Everything a run needs that is not already in the strategy version.

    Every default here is recorded in the run manifest as a *default*, not silently
    inherited. Spec 12.1 makes the parameter set part of a run's identity, and a field that
    took its value from a constant nobody wrote down would make two runs look identical
    while having been configured differently.
    """

    strategy_id: int
    version_no: int | None = None
    """Defaults to the head version. Pinned in the spec either way, so a later save does
    not change what a stored run says it executed."""

    label: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    symbols: list[str] | None = None
    timeframe: str | None = None
    start_ms: int
    end_ms: int
    seed: int = 0
    opening_balance: str = "10000"
    leverage: int = 1
    hedge_mode: bool = False
    """Spec 3.3 extended: hold a long **and** a short position per symbol.

    Off by default. In hedge mode every order must name a `position_side`, and the exchange
    account has to be in the same mode -- `live.preflight.configure_account` refuses a
    session whose ledger and account disagree, in either direction."""
    margin_mode: str = "ISOLATED"
    """Spec 3.7. Isolated is the default because it is the only mode the ledger prices;
    `CROSSED` is refused rather than quietly computed as isolated. See `MarginMode`."""
    maker_rate: str = "0.0002"
    taker_rate: str = "0.0005"
    fee_source: str = "binance-usdm-standard"
    latency_model: str = "lognormal"
    """Spec 6.3: *"`fixed` (deterministic, use for golden tests), `lognormal` (default,
    realistic)"*.

    `fixed` was the right Phase 4 default and `latency.py` says why: at the `BAR_CLOSE` tier
    every latency from 1 ms to just under one bar produces exactly the same fill, so sampling
    a distribution added a random draw to the reproducibility surface and bought nothing. It
    is Phase 5 now -- fills are evaluated against individual trades, and two samples 200 ms
    apart genuinely differ -- so the spec's own default applies again. A golden test asks for
    `fixed` explicitly.
    """

    latency_submit_ms: int = DEFAULT_SUBMIT_MS
    """The fixed latency, or the lognormal median."""

    latency_p99_ms: int = 600
    fill_tier: str = "BOOK_TICKER"
    """The tier this run *asks* for. What it gets is resolved from the lake by the worker.

    `BOOK_TICKER` rather than the best tier that exists, and rather than the worst. Spec 4.2
    calls it "the default for most backtests"; `BOOK_WALK` is only available from the day the
    collector started, so defaulting to it would degrade almost every run and make the
    degradation badge meaningless through familiarity. `BAR_CLOSE` is spec 4.2's "explicit
    opt-in only". A request for more than the data supports is not an error -- it resolves
    down and says so.
    """

    slippage_bps: str = str(DEFAULT_SLIPPAGE_BPS)
    """`BAR_CLOSE` only. The other tiers derive their cost from the book or the tape."""

    impact_k_bps: str = "10"
    """`BOOK_TICKER`'s `k` in spec 6.4's `k x sqrt(notional / 1min notional)`."""

    depth_exhaustion_pct: str = "0.001"
    """`BOOK_WALK`'s penalty beyond the deepest published level."""

    trade_spread_bps: str = "1.0"
    """`TRADE_ONLY`'s "fixed conservative spread assumption"."""

    liquidation_recovery_pct: str = "0"
    timeout_s: float = 900.0

    # ------------------------------------------------------------------- risk (spec 7)

    risk_enabled: bool = True
    """**Spec 7's defaults apply unless the caller turns them off, and this is the one
    place that is true.**

    The engine's own default is no limits, because a default that silently halted a Phase 4
    run would change an answer nobody asked to change. But a *person* starting a run through
    this API is choosing, and choosing nothing should get them the table spec 7 wrote rather
    than an unbounded account. Setting this to `false` is the explicit opt-out, and the run
    is flagged `RISK_UNBOUNDED` when it happens."""

    max_position_notional: str | None = None
    max_leverage: str | None = "5"
    max_daily_loss_pct: str | None = "0.02"
    max_drawdown_pct: str | None = "0.15"
    max_open_orders: int | None = 10
    max_orders_per_minute: int | None = 30
    max_consecutive_losses: int | None = None
    halt_on_liquidation: bool = True
    min_equity_pct: str | None = "0.50"
    max_consecutive_rejections: int | None = 5

    kill_switch_flatten: bool = False
    """Spec 7.3: cancel-only by default. Arming this makes a halt close positions at
    market, which during a flash crash can be worse than the exposure."""

    max_hold_ms: int | None = None
    before_funding_ms: int | None = None
    """Platform-enforced exits. Off unless asked for -- a platform that flattens positions
    nobody asked it to flatten is not reporting the strategy."""

    def risk_json(self) -> dict[str, Any]:
        if not self.risk_enabled:
            return RiskLimits.unlimited().to_json()
        return {
            "max_position_notional": self.max_position_notional,
            "max_leverage": self.max_leverage,
            "max_daily_loss_pct": self.max_daily_loss_pct,
            "max_drawdown_pct": self.max_drawdown_pct,
            "max_open_orders": self.max_open_orders,
            "max_orders_per_minute": self.max_orders_per_minute,
            "max_consecutive_losses": self.max_consecutive_losses,
            "halt_on_liquidation": self.halt_on_liquidation,
            "min_equity_pct": self.min_equity_pct,
            "max_consecutive_rejections": self.max_consecutive_rejections,
        }

    def auto_flatten_json(self) -> dict[str, Any]:
        return {
            "max_hold_ms": self.max_hold_ms,
            "before_funding_ms": self.before_funding_ms,
        }

    def fill_model_json(self) -> dict[str, Any]:
        """The requested tier's model parameters, and only that tier's.

        One tier's knobs, not all four. Storing every parameter on every run would put three
        numbers in each manifest that had no effect on it, and spec 12.1's contract is that
        everything recorded as an input *is* one.
        """
        if self.fill_tier == "BAR_CLOSE":
            return {"tier": "BAR_CLOSE", "slippage_bps": self.slippage_bps}
        if self.fill_tier == "TRADE_ONLY":
            return {"tier": "TRADE_ONLY", "spread_bps": self.trade_spread_bps}
        if self.fill_tier == "BOOK_WALK":
            return {"tier": "BOOK_WALK", "depth_exhaustion_pct": self.depth_exhaustion_pct}
        return {"tier": "BOOK_TICKER", "impact_k_bps": self.impact_k_bps}

    def latency_json(self) -> dict[str, Any]:
        if self.latency_model == "lognormal":
            return {
                "model": "lognormal",
                "median_ms": self.latency_submit_ms,
                "p99_ms": self.latency_p99_ms,
                "cancel_median_ms": DEFAULT_CANCEL_MS,
            }
        return {
            "model": "fixed",
            "submit_ms": self.latency_submit_ms,
            "cancel_ms": DEFAULT_CANCEL_MS,
        }


class ArchiveRunRequest(BaseModel):
    archived: bool = True


# ---------------------------------------------------------------------------- routes


@router.post("/runs", status_code=201)
def start_run(
    request: StartRunRequest,
    library: StrategyLibrary = Depends(get_library),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    """Validate the request against the strategy, then queue and launch a worker."""
    strategy = library.get(request.strategy_id)
    version = (
        strategy.head
        if request.version_no is None
        else library.version(request.strategy_id, request.version_no)
    )
    if version is None:
        raise HTTPException(status_code=404, detail="that strategy has no saved version")
    if not version.valid:
        # Spec 5.6 stores an invalid version deliberately, so work is never lost. Running
        # one is a different question: its diagnostics travelled with it, and a run whose
        # strategy failed validation would either crash on the first bar or -- worse --
        # complete with numbers produced by code the validator had already refused.
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=(
                f"version {version.version_no} did not pass validation, so it cannot be "
                f"backtested. Fix the diagnostics in the editor and save again."
            ),
        )

    requires = version.requires or {}
    symbols = tuple(request.symbols or requires.get("symbols") or ())
    timeframe = request.timeframe or requires.get("timeframe") or ""
    if not symbols:
        raise HTTPException(status_code=400, detail="the run has no symbols")
    if timeframe not in TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown timeframe {timeframe!r}; known: {', '.join(TIMEFRAMES)}",
        )
    if request.end_ms <= request.start_ms:
        raise HTTPException(
            status_code=400,
            detail="the range is empty; ranges are half-open [start_ms, end_ms)",
        )
    if request.leverage < 1:
        raise HTTPException(status_code=400, detail="leverage must be at least 1")
    try:
        margin_mode = MarginMode.parse(request.margin_mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    # SQLite stores an INTEGER as int64 and raises `OverflowError` above it -- an
    # uncaught 500 for a value the request could simply refuse. Bounded here, where the
    # message can name the field.
    if not (-(2**63) <= request.seed < 2**63):
        raise HTTPException(status_code=400, detail="seed must fit in a signed 64-bit int")
    try:
        normalised = tuple(normalise_symbol(s) for s in symbols)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        latency_from_json(request.latency_json())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        RiskLimits.from_json(request.risk_json())
        AutoFlatten.from_json(request.auto_flatten_json())
    except ValueError as exc:
        # A limit written as a percent, or a misspelled field. Refused here so the caller
        # gets the offending value back rather than a queued run that dies on its first
        # line -- and, worse, a limit that would silently never have fired.
        raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        tier_from_name(request.fill_tier)
        fill_model_from_json(request.fill_model_json())
    except ValueError as exc:
        # Refused here rather than in the worker. A bad tier name or a negative impact
        # coefficient would otherwise produce a queued run that fails on its first line,
        # which reads as a platform fault rather than as a rejected request.
        raise HTTPException(status_code=400, detail=str(exc)) from None

    # Parameters are bound here, against the version's own declarations, so an unknown or
    # out-of-range value is a 400 with the offending key rather than a worker that starts
    # and dies. `bind_params` is the same code the strategy constructor uses.
    params = _bind_params(version, request.params)

    spec = RunSpec(
        strategy_id=strategy.id,
        version_id=version.id,
        version_no=version.version_no,
        strategy_name=strategy.name,
        code=version.code or "",
        class_name=version.class_name,
        params=params,
        symbols=normalised,
        timeframe=timeframe,
        start_ms=request.start_ms,
        end_ms=request.end_ms,
        seed=request.seed,
        opening_balance=request.opening_balance,
        leverage=request.leverage,
        margin_mode=margin_mode.value,
        hedge_mode=request.hedge_mode,
        maker_rate=request.maker_rate,
        taker_rate=request.taker_rate,
        fee_source=request.fee_source,
        latency=request.latency_json(),
        fill_tier=request.fill_tier,
        fill_model=request.fill_model_json(),
        liquidation_recovery_pct=request.liquidation_recovery_pct,
        timeout_s=request.timeout_s,
        engine_version=ENGINE_VERSION,
        risk_limits=request.risk_json(),
        auto_flatten=request.auto_flatten_json(),
        kill_switch_flatten=request.kill_switch_flatten,
    )
    run_id = runs.create(
        strategy_id=strategy.id,
        version_id=version.id,
        spec=spec.to_storage(),
        label=request.label,
    )
    runs.launch(run_id)
    return {"run": runs.get(run_id).to_json()}


@router.get("/runs")
def list_runs(
    strategy_id: int | None = None,
    run_status: str | None = Query(None, alias="status"),
    archived: bool = False,
    limit: int = Query(200, ge=1, le=1000),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    items = runs.list(
        strategy_id=strategy_id,
        status=run_status,
        include_archived=archived,
        limit=limit,
    )
    return {"runs": [item.to_json() for item in items]}


@router.get("/runs/{run_id}")
def get_run(run_id: int, runs: RunStore = Depends(get_runs)) -> dict[str, Any]:
    summary = runs.get(run_id)
    payload: dict[str, Any] = {"run": summary.to_json()}
    if summary.status == RunStatus.DONE:
        stored = runs.read_json(run_id, "metrics.json")
        payload["metrics"] = stored["metrics"]
        payload["attribution"] = stored["attribution"]
        payload["summary"] = stored["summary"]
        # `.get`, not `[...]`: runs completed before Phase 6 have no breach list, and a
        # KeyError on an old run would make the results page unopenable for it.
        payload["risk_breaches"] = stored.get("risk_breaches", [])
        payload["manifest"] = runs.read_json(run_id, "manifest.json")
    payload["spec"] = _spec_without_code(runs.read_json(run_id, "spec.json"))
    return payload


@router.get("/runs/{run_id}/trades")
def get_trades(run_id: int, runs: RunStore = Depends(get_runs)) -> dict[str, Any]:
    return runs.read_json(run_id, "trades.json")


_FORMULA_LEADS = ("=", "+", "-", "@", "\t", "\r")
"""Leading characters a spreadsheet interprets as a formula (OWASP CSV-injection set)."""

_PLAIN_NUMBER = re.compile(r"^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$")
"""A bare signed decimal -- the shape every money string in a trades table takes."""


def _csv_safe(value: Any) -> Any:
    """Neutralise spreadsheet formula injection in one exported cell (OWASP).

    The trades table carries user-controlled text -- a strategy can be named
    `=HYPERLINK(...)`, and `tag` is free-form -- and Excel executes a cell that begins
    with `=`, `+`, `-`, `@`, a tab or a carriage return *on open*, quoting or not. The
    OWASP remedy is a leading apostrophe, which Excel displays as text and drops from the
    value. It is applied only where the cell could be a formula: a string that parses as a
    plain signed number is left alone, because the platform's exact money strings
    (`"-30.05000000"`) begin with `-` by design and an apostrophe would break every
    numeric column for the sake of a character that cannot start a formula call.
    """
    if not isinstance(value, str) or not value:
        return value
    if not value.startswith(_FORMULA_LEADS):
        return value
    if _PLAIN_NUMBER.match(value):
        return value
    return f"'{value}"


@router.get("/runs/{run_id}/trades.csv")
def export_trades(run_id: int, runs: RunStore = Depends(get_runs)) -> Response:
    """Spec 10.3's CSV export.

    Written with `csv.writer` rather than by joining commas, because a strategy tag or a
    close reason containing a comma would otherwise shift every column after it -- silently,
    and only in some rows. Every cell additionally passes `_csv_safe`, because this file's
    stated purpose is to be opened in a spreadsheet and a `tag` of `=HYPERLINK(...)`
    executes there. (The client-side exporter in `frontend/lib/export.ts` builds its own
    CSVs and needs the same guard; it is not served by this route.)
    """
    trades = runs.read_json(run_id, "trades.json")["trades"]
    buffer = io.StringIO()
    if trades:
        writer = csv.DictWriter(buffer, fieldnames=list(trades[0]))
        writer.writeheader()
        writer.writerows(
            [{key: _csv_safe(value) for key, value in row.items()} for row in trades]
        )
    return Response(
        content=buffer.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="run-{run_id}-trades.csv"'
        },
    )


@router.get("/runs/{run_id}/equity")
def get_equity(
    run_id: int,
    points: int = Query(MAX_EQUITY_POINTS, ge=100, le=MAX_EQUITY_POINTS),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    """The mark-to-market series and its drawdown, downsampled by extremes.

    Drawdown is computed on the **full** series before anything is dropped. Computing it on
    the downsampled points would produce a shaded area that disagrees with the `max_drawdown`
    printed above it, and the number, not the picture, would be the one that was right.
    """
    path = runs.artefact(run_id, "equity.parquet")
    partial = False
    if path.exists():
        table = pq.read_table(path)
        times = table.column("ts_ms").to_pylist()
        equity = table.column("equity").to_pylist()
        # The same band `analytics.metrics.drawdown_stats` uses, so the shaded panel and the
        # "Max drawdown" card are one computation rather than two that nearly agree. Absent
        # on runs written before the band existed, where the close series is the honest
        # fallback.
        names = set(table.schema.names)
        low = table.column("equity_low").to_pylist() if "equity_low" in names else equity
        high = table.column("equity_high").to_pylist() if "equity_high" in names else equity
        samples = len(times)
    else:
        # **The run is still going.** The worker republishes a thinned copy of the curve on
        # its progress cadence so the chart can fill in as the run proceeds, instead of
        # showing nothing for an hour and everything at the end. Checked second, so a
        # finished run is always served from its exact artefact even if a stale preview
        # survived a crash.
        preview = _read_progress_equity(runs.artefact(run_id, PROGRESS_EQUITY_NAME))
        if preview is None:
            raise RunNotFound(f"run {run_id} has no equity series")
        times, equity, low, high, samples = preview
        partial = True

    # `None`, not `0.0`, where the running peak is non-positive: a percentage fall from a
    # peak of nothing is undefined, and rendering it as zero drew a flat-zero shaded panel
    # under an account that was at or below zero -- contradicting the Max-drawdown card
    # beside it, which comes from `analytics.metrics.drawdown_stats` and *skips* such
    # samples. Undefined is None everywhere else on this platform (the em-dash rule), and
    # the chart treats a null as a gap rather than as a value.
    drawdown: list[float | None] = []
    peak = max(equity[0], high[0]) if equity else 0.0
    for index, value in enumerate(equity):
        if high[index] > peak:
            peak = high[index]
        drawdown.append((low[index] / peak - 1.0) if peak > 0 else None)

    keep = _downsample_extremes(times, equity, points // 2, anchors=drawdown)
    return {
        "ts": [times[i] for i in keep],
        "equity": [equity[i] for i in keep],
        "drawdown": [drawdown[i] for i in keep],
        "samples": samples,
        "returned": len(keep),
        # **The caller has to be able to tell these apart.** A partial curve's drawdown is
        # measured against the highest peak *so far*, so it can only deepen as the run
        # continues; presenting it as the run's drawdown would be a number that quietly
        # changes meaning when the run ends.
        "partial": partial,
    }


def _read_progress_equity(
    path: Path,
) -> tuple[list[int], list[float], list[float], list[float], int] | None:
    """The in-progress curve, or `None` if there is not a readable one.

    Every failure returns `None` rather than raising: this file is rewritten every few
    seconds by another process, and the reader can lose the race with the writer's
    `os.replace` on Windows, where the open can fail outright while the swap is in flight.
    The caller's fallback -- "this run has no equity series yet" -- is already the right
    answer for a run whose first checkpoint has not landed, so a missed read costs one poll
    rather than an error page.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    try:
        times = [int(v) for v in payload["ts"]]
        equity = [float(v) for v in payload["equity"]]
        low = [float(v) for v in payload["low"]]
        high = [float(v) for v in payload["high"]]
        samples = int(payload.get("samples", len(times)))
    except (KeyError, TypeError, ValueError):
        return None
    # A short column would silently misalign the drawdown loop against the equity it is
    # measuring, so a torn write is refused rather than padded.
    if not (len(times) == len(equity) == len(low) == len(high)):
        return None
    if not times:
        return None
    return times, equity, low, high, samples


@router.get("/runs/{run_id}/price")
def get_price(
    run_id: int,
    points: int = Query(2000, ge=100, le=MAX_EQUITY_POINTS),
    runs: RunStore = Depends(get_runs),
    root: Path = Depends(get_root),
) -> dict[str, Any]:
    """The price series the run traded, for the trade-marker overlay (spec 10.3).

    Read from the lake rather than stored with the run. The bars are already on disk and
    identical for every run over the same range, so copying them into each run directory
    would multiply a year of data by the number of times it was backtested.
    """
    summary = runs.get(run_id)
    if not summary.symbols or summary.start_ms is None or summary.end_ms is None:
        raise HTTPException(status_code=404, detail="this run has no range recorded")
    symbol = summary.symbols[0]
    table = derive_timeframe(
        market_root(root),
        summary.timeframe,
        symbol=symbol,
        start_ms=summary.start_ms,
        end_ms=summary.end_ms,
    )
    times = table.column("close_time").to_pylist()
    closes = [value / 1e8 for value in table.column("close").to_pylist()]
    keep = _downsample_extremes(times, closes, points // 2)
    return {
        "symbol": symbol,
        "timeframe": summary.timeframe,
        "ts": [times[i] for i in keep],
        "close": [closes[i] for i in keep],
        "bars": len(times),
    }


@router.get("/runs/{run_id}/events")
def get_events(
    run_id: int,
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=2000),
    kind: str | None = None,
    q: str | None = Query(None, max_length=200),
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    events, total = runs.read_events(
        run_id, offset=offset, limit=limit, kind=kind, search=q
    )
    return {"events": events, "total": total, "offset": offset, "limit": limit}


@router.get("/runs/{run_id}/manifest")
def get_manifest(run_id: int, runs: RunStore = Depends(get_runs)) -> dict[str, Any]:
    return runs.read_json(run_id, "manifest.json")


@router.post("/runs/{run_id}/cancel")
def cancel_run(run_id: int, runs: RunStore = Depends(get_runs)) -> dict[str, Any]:
    return {"run": runs.cancel(run_id).to_json()}


@router.post("/runs/{run_id}/archive")
def archive_run(
    run_id: int,
    request: ArchiveRunRequest,
    runs: RunStore = Depends(get_runs),
) -> dict[str, Any]:
    return {"run": runs.archive(run_id, archived=request.archived).to_json()}


@router.delete("/runs/{run_id}")
def delete_run(run_id: int, runs: RunStore = Depends(get_runs)) -> dict[str, Any]:
    try:
        runs.delete(run_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED, detail=str(exc)
        ) from None
    return {"deleted": run_id}


@router.get("/strategies/{strategy_id}/trials")
def get_trials(
    strategy_id: int, runs: RunStore = Depends(get_runs)
) -> dict[str, Any]:
    """Spec 8.5's multiple-testing counter."""
    return runs.trials(strategy_id)


DAY_MS = 86_400_000
_EPOCH_DAY = date(1970, 1, 1)


def _partition_days(market: Path, dataset: str, symbol: str) -> list[int]:
    """Epoch-day numbers that hold data, read from `date=` partition names.

    For the tick datasets, which are the ones with holes worth seeing. Directory names are
    the whole answer here and no row is touched: `SELECT DISTINCT "date"` over `aggTrades`
    costs 4.2 s against 3.38 billion rows and returns the identical 2,410 days this returns
    instantly. Checked against that query on the real lake for all three tick datasets --
    2,410, 12 and 5 -- so this is a shortcut, not an approximation.

    A partition holding no parquet is not counted. An empty directory is what a crash between
    `mkdir` and the publishing `os.replace` leaves behind, and counting it would draw a day
    of coverage that contains nothing -- the exact class of claim this endpoint exists to
    stop making.
    """
    directory = market / dataset / f"symbol={symbol}"
    if not directory.is_dir():
        return []

    days: list[int] = []
    with os.scandir(directory) as entries:
        partitions = [e for e in entries if e.is_dir() and e.name.startswith("date=")]
    for partition in partitions:
        try:
            day = date.fromisoformat(partition.name[len("date=") :])
        except ValueError:
            continue  # not a partition this dataset wrote; not evidence of anything
        with os.scandir(partition.path) as files:
            if not any(f.name.endswith(".parquet") for f in files):
                continue
        days.append((day - _EPOCH_DAY).days)
    return sorted(set(days))


def _bar_days(market: Path, dataset: str, symbol: str, column: str) -> list[int]:
    """Epoch-day numbers that hold bars, from the bars themselves.

    The bar datasets are partitioned by month, so their directory names cannot answer this at
    the day resolution the holes actually have -- `markPriceKlines` is missing 41 days that
    every month it is missing them from still has a directory for. They are small enough
    (3.4 M rows) that asking the rows directly costs 0.42 s, so they are asked.
    """
    table = query(
        market,
        f'SELECT DISTINCT "{column}" // {DAY_MS} AS day FROM "{dataset}" WHERE "symbol" = ?',
        datasets=(dataset,),
        params=[symbol],
    )
    return sorted(int(row["day"]) for row in table.to_pylist() if row["day"] is not None)


def _segments(days: list[int]) -> list[dict[str, int]]:
    """Consecutive days merged into half-open `[start_ms, end_ms)` runs.

    Merged rather than sent a day at a time because the wire and the renderer both scale with
    the number of runs, not the number of days: six years of unbroken `aggTrades` is one
    segment, and `bookTicker`'s twelve days across twenty-nine months are two.
    """
    runs: list[list[int]] = []
    for day in days:
        if runs and day == runs[-1][1] + 1:
            runs[-1][1] = day
        else:
            runs.append([day, day])
    return [
        {"start_ms": first * DAY_MS, "end_ms": (last + 1) * DAY_MS} for first, last in runs
    ]


def _with_coverage(entry: dict[str, Any], days: list[int]) -> dict[str, Any]:
    """Attach what is actually covered between the bounds, not just the bounds.

    `start_ms` and `end_ms` are the first and last row, and on their own they invite exactly
    one reading: that everything between them is there. For `bookTicker` that reading is
    wrong by two orders of magnitude -- twelve days of data inside an 865-day span, drawn
    until now as one solid block. `days_covered` against `days_spanned` is the number that
    makes the difference impossible to miss, and `segments` is where the holes actually are
    so the bar can draw them instead of painting over them.
    """
    entry["segments"] = _segments(days)
    entry["days_covered"] = len(days)
    entry["days_spanned"] = 0 if not days else days[-1] - days[0] + 1
    return entry


@router.get("/coverage")
def get_coverage(
    symbol: str = Query("BTCUSDT"),
    root: Path = Depends(get_root),
) -> dict[str, Any]:
    """The range the lake can actually *run*, so the form cannot offer a date that fails.

    The **intersection** of `klines` and `markPriceKlines`, not the klines range alone.
    Spec 3.4 makes the mark a separate series that may not be derived, and `load_marks`
    refuses a range it does not cover -- so advertising the klines range invited exactly the
    failure this endpoint exists to prevent. On the real lake the mark series is absent for
    34 days inside the advertised klines window.

    Interior holes are reported rather than folded into the bounds: shrinking the range to
    exclude them would hide six years of usable history behind one outage, and a range that
    spans a hole is legitimate as long as the run is willing to carry a `GAP_SKIPPED` flag.
    """
    try:
        wanted = normalise_symbol(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None

    market = market_root(root)
    bounds: dict[str, dict[str, Any]] = {}
    for dataset in ("klines", "markPriceKlines"):
        table = query(
            market,
            f"""
            SELECT min("open_time") AS lo, max("close_time") AS hi, count(*) AS bars
            FROM "{dataset}" WHERE "symbol" = ?
            """,
            datasets=(dataset,),
            params=[wanted],
        )
        row = table.to_pylist()[0] if table.num_rows else {}
        bounds[dataset] = _with_coverage(
            {
                "start_ms": row.get("lo"),
                "end_ms": None if row.get("hi") is None else int(row["hi"]) + 1,
                "bars": row.get("bars", 0),
            },
            _bar_days(market, dataset, wanted, "open_time"),
        )

    starts = [b["start_ms"] for b in bounds.values() if b["start_ms"] is not None]
    ends = [b["end_ms"] for b in bounds.values() if b["end_ms"] is not None]
    complete = len(starts) == len(bounds) and len(ends) == len(bounds)

    # The tick datasets are reported but deliberately kept *out* of the runnable bounds.
    # A run needs bars and marks; it does not need `depth20`, which exists only from the day
    # the collector started. Intersecting them in would shrink six years of runnable history
    # to two days and present a fidelity ceiling as a data floor -- the exact inversion spec
    # 4.2 decision 3 is written to prevent. They are here so the form can say which tiers a
    # chosen range can reach, which is a different question from whether it can run at all.
    for dataset, column in (
        ("aggTrades", "ts_ms"),
        ("bookTicker", "ts_ms"),
        ("depth20", "ts_ms"),
    ):
        table = query(
            market,
            f"""
            SELECT min("{column}") AS lo, max("{column}") AS hi, count(*) AS rows
            FROM "{dataset}" WHERE "symbol" = ?
            """,
            datasets=(dataset,),
            params=[wanted],
        )
        row = table.to_pylist()[0] if table.num_rows else {}
        bounds[dataset] = _with_coverage(
            {
                "start_ms": row.get("lo"),
                "end_ms": None if row.get("hi") is None else int(row["hi"]) + 1,
                "rows": row.get("rows", 0),
            },
            _partition_days(market, dataset, wanted),
        )

    return {
        "symbol": wanted,
        "start_ms": max(starts) if complete else None,
        "end_ms": min(ends) if complete else None,
        "bars": bounds["klines"]["bars"],
        "datasets": bounds,
    }


@router.get("/tiers")
def preview_tier(
    symbol: str = Query("BTCUSDT"),
    start_ms: int = Query(...),
    end_ms: int = Query(...),
    requested: str = Query("BOOK_TICKER"),
    root: Path = Depends(get_root),
) -> dict[str, Any]:
    """What tier this exact range would actually execute at, before anything is queued.

    Spec 4.2 requires a downgrade to be visible on the results page. Showing it there is
    necessary and late: the user has already waited for the run. This runs the same
    `resolve_tier` the worker will run, over the same range, so the New Backtest dialog can
    say "this range only supports TRADE_ONLY -- limit orders will be refused" *before* the
    strategy is submitted rather than after it has produced a curve with no fills in it.

    Gaps are not scanned here. Row-level gap detection over a year of tick data would make
    this endpoint slower than the backtest, and the preview's job is to be right about
    coverage; the worker's pass is the one whose verdict binds. When they differ the run's
    own badge is authoritative, and it can only ever be lower -- a gap demotes, never
    promotes.
    """
    try:
        wanted = normalise_symbol(symbol)
        want = tier_from_name(requested)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if end_ms <= start_ms:
        raise HTTPException(
            status_code=400,
            detail="the range is empty; ranges are half-open [start_ms, end_ms)",
        )
    try:
        resolution = resolve_tier(root, [wanted], start_ms, end_ms, requested=want)
    except CoverageError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {
        **resolution.to_json(),
        "preview": True,
        "all_capabilities": TIER_CAPABILITIES,
    }


# -------------------------------------------------------------------------- internals


def _bind_params(version: Any, overrides: dict[str, Any]) -> dict[str, Any]:
    """Resolve the run's parameters against the version's declared specs.

    Bound here, in the request, so a typo in a parameter name is a 400 naming the key rather
    than a worker that starts, fails on construction, and leaves a failed run to explain.

    **Types survive the round trip.** Stringifying everything -- which is what this did --
    turned a declared `bool` into `"True"`, and `params._coerce_override` rightly refuses
    that, so *every* backtest of a strategy with a boolean parameter was accepted with a 201
    and then failed in the worker with `expected true or false, got 'True'`. Bools and ints
    are JSON-native and stay native; decimals stay exact *strings*, which is the whole reason
    `strategy.params` takes them that way (a JSON number would already be a float by the time
    it arrived).
    """
    from perplab.strategy.params import ParamError, bind_params, parse_param_specs

    declared = version.params or []
    specs = parse_param_specs(
        {
            spec["name"]: {k: v for k, v in spec.items() if k != "name"}
            for spec in declared
        }
    )
    try:
        bound = bind_params(specs, overrides)
    except ParamError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None

    resolved: dict[str, Any] = {}
    for spec in specs:
        value = getattr(bound, spec.name)
        resolved[spec.name] = value if isinstance(value, (bool, int)) else str(value)
    return resolved


def _spec_without_code(spec: dict[str, Any]) -> dict[str, Any]:
    """The run spec minus the source.

    The hash identifies the code and the strategy version holds it; sending the whole file
    with every poll of a running backtest would multiply a 4 kB response by the polling
    interval for no benefit.
    """
    return {k: v for k, v in spec.items() if k != "code"}


def _downsample_extremes(
    times: list[int],
    values: list[float],
    buckets: int,
    anchors: list[float | None] | None = None,
) -> list[int]:
    """Indices to keep: each bucket's minimum and maximum, plus the points that must survive.

    Preserving both extremes is what keeps a rendered curve honest. Stride sampling drops
    whichever of the two happened between the points it kept, so a sharp drawdown and its
    recovery can vanish entirely -- and the deeper and faster the excursion, the more likely
    it is to be the thing that disappears.

    Three anchors are forced in regardless of which bucket they land in, because each is a
    number printed elsewhere on the page and a chart that omits it disagrees with its own
    caption:

    - **the first sample**, which is the run's opening equity and the baseline the chart
      draws its dashed reference line at. Only the last was pinned before, so on any range
      where bucket 0 outgrows the flat warm-up prefix the baseline was a value the account
      never opened at -- 31 of 50 simulated year-long runs, one of them off by 532;
    - **the last sample**, the closing equity;
    - **the global minimum of `anchors`**, which is the drawdown trough. It is an extreme of
      `equity / running peak`, not of equity, so bucketing by equity alone can drop it and
      leave the shaded panel reading shallower than the "Max drawdown" card beside it.
    """
    if not values:
        return []
    if len(values) <= buckets * 2 or buckets <= 0:
        return list(range(len(values)))

    forced = {0, len(values) - 1}
    if anchors is not None and len(anchors) == len(values):
        # An undefined anchor (a drawdown with no positive peak, served as null) can never
        # be the trough that must survive downsampling, so it ranks above everything.
        defined = [i for i in range(len(anchors)) if anchors[i] is not None]
        if defined:
            forced.add(min(defined, key=lambda i: anchors[i]))

    keep: set[int] = set(forced)
    size = len(values) / buckets
    for bucket in range(buckets):
        lo = int(bucket * size)
        hi = min(len(values), int((bucket + 1) * size))
        if hi <= lo:
            continue
        keep.add(min(range(lo, hi), key=lambda i: values[i]))
        keep.add(max(range(lo, hi), key=lambda i: values[i]))
    return sorted(keep)
