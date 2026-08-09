"""The data-refresh API -- the "bring my data up to date" button's server half.

Two surfaces, and they answer different questions on purpose.

**`/api/data/update` and `/api/data/updates`** are the button and its job list. Starting a
refresh returns immediately and the client polls, for `runs.start_run`'s reason: a top-up of
`aggTrades` is minutes of transfer and the one thing worse than waiting is a browser that has
already timed out on a download which is still running. `dry_run` answers "what would this
do" without starting anything, because the same button on an empty lake plans 190 GB across
2400 requests and nobody should discover that from a progress bar.

**`/api/data/collector`** is the status card for the thing that cannot be re-downloaded. Its
whole design constraint is stated in `_collector_json` and is worth reading before changing
any field name here: it must never claim, or imply by a percentage, that the Phase 1b exit
criterion is met, because that criterion is a conjunction of three things and this endpoint
can observe one of them.

Refusals are 409 rather than 400 throughout. A held lock, an unhealthy collector and a full
disk are all *states of the machine*, not faults in the request -- the same request will
succeed later, unchanged, and a 400 would tell the operator to go and fix their input.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

from perplab.api.deps import get_root
from perplab.data import refresh
from perplab.data.ingest_bulk import format_bytes
from perplab.data.schemas import market_root, normalise_symbol
from perplab.store.ingests import IngestNotFound, IngestStore

router = APIRouter(tags=["data"])

MAX_ID = 2**63 - 1
"""SQLite binds an INTEGER as int64 and raises `OverflowError` above it, which escapes as a
500. FastAPI's `int` is arbitrary-precision, so the bound has to be stated here."""

COLLECTOR_STATE_FILE = "collector_state.json"
"""The collector's self-report, at the lake root. Spelled out rather than imported because
`refresh.collector_state` folds "absent" and "unparseable" into one `None`, and this endpoint
has to tell them apart -- absent is ambiguous by design, unparseable is a fault."""

EVENT_PARTITIONS_SCANNED = 7
"""How many day partitions of `collectorEvents` the status card reads.

A bound, not a window anyone chose for its meaning. The card is polled, so it must not grow
a scan as the lake grows; seven days is comfortably longer than any run this collector has
completed. When the newest CONNECT or RESTART is older than this the answer is `None` plus a
caveat saying so, never a guess -- the whole point of the field is that it is evidence.

**Seven partitions is not seven files.** The collector flushes on a size-or-time boundary and
writes roughly 1,400 part-files a day, so this bound is about ten thousand files, not the
"handful" an earlier version of this docstring claimed. That mistaken figure is why the read
below was written to open them one at a time, and why the card then took seconds to answer a
question about the last ten of them. The bound is still worth having -- it is what keeps the
worst case finite as the lake grows -- but the read has to be sized for the real count.
"""

_EVENT_COLUMNS = ("ts_ms", "kind", "stream", "detail")
"""The only columns the card reads. Named once so the fast path and the fallback below cannot
drift into projecting different columns and answering differently."""


def get_ingests(request: Request) -> IngestStore:
    """The process-wide refresh-job store.

    One instance per server process, for `deps.get_runs`'s reason: it holds the `Popen`
    handles for the workers *this* process launched, and that is the only way a worker that
    died without recording a result is ever noticed. Declared here rather than in `deps`
    because nothing outside this router needs it.
    """
    ingests: IngestStore | None = getattr(request.app.state, "ingests", None)
    if ingests is None:  # pragma: no cover - only if the app was built by hand
        raise HTTPException(status_code=500, detail="refresh job store not initialised")
    return ingests


# --------------------------------------------------------------------------- schemas


class UpdateRequest(BaseModel):
    kind: str
    """`candles` or `trades` -- see `refresh.REFRESH_KINDS` for why there are two."""

    symbol: str
    dry_run: bool = False
    confirm: bool = False
    """Required when the plan exceeds `refresh.MAX_UNCONFIRMED_ARCHIVES` or
    `MAX_UNCONFIRMED_BYTES`. Defaults to false so that a client which has never heard of
    confirmation cannot start a 190 GB backfill by omitting a field."""


# ---------------------------------------------------------------------------- routes


@router.post("/data/update")
def start_update(
    request: UpdateRequest,
    root: Path = Depends(get_root),
    ingests: IngestStore = Depends(get_ingests),
) -> dict[str, Any]:
    """Plan a refresh, and unless `dry_run` is set, start one.

    The order of the checks is the interesting part:

    **Environment first, intent second.** `require_collector_idle` runs before anything else
    touches the network, and it gates the dry run too -- planning is not free, it probes the
    publication frontier of every dataset over HTTP, and the moment the collector is fighting
    to re-establish its sockets is the moment those probes cost data nobody can get back.
    The disk check follows for the same reason it exists at all: the headroom is reserved for
    the collector, not for this.

    **Starting-only checks are skipped for a dry run.** A held lock or a job already in
    flight is an answer to "may I start", not to "what would this do"; refusing to *describe*
    a refresh because another one is running would leave the operator unable to find out why.

    **Confirmation is checked before the row is written, never after.** A plan needing
    confirmation must start *nothing* -- not a queued job the caller then has to cancel.
    """
    try:
        symbol = normalise_symbol(request.symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if request.kind not in refresh.REFRESH_KINDS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"unknown refresh kind {request.kind!r}; expected one of "
                f"{sorted(refresh.REFRESH_KINDS)}"
            ),
        )

    market = market_root(root)
    try:
        refresh.require_collector_idle(market)
    except refresh.CollectorBusy as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from None

    try:
        plan = refresh.plan_refresh(market, symbol, request.kind)
    except KeyError as exc:
        # `bulk_dataset` raises **KeyError** for an unregistered dataset name, and nothing
        # in `app._install_error_handlers` maps it -- so it surfaced as a 500 for what is a
        # naming mistake with a perfectly good message inside it. `str(KeyError)` is the
        # repr of the argument, hence `exc.args[0]`.
        detail = exc.args[0] if exc.args else str(exc)
        raise HTTPException(status_code=400, detail=str(detail)) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None

    try:
        refresh.require_disk_headroom(market, plan)
    except refresh.InsufficientDisk as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from None

    if request.dry_run:
        return {
            "plan": plan.to_json(),
            "conflicts_predicted": _predicted_conflicts(market, symbol, plan),
        }

    if plan.needs_confirmation and not request.confirm:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"this refresh plans {plan.archives} archive(s)"
                + (
                    ", of unknown total size"
                    if plan.estimated_bytes is None
                    else f", about {format_bytes(plan.estimated_bytes)}"
                )
                + ". That is far more than a top-up, so it needs explicit confirmation: "
                "run it as a dry run first and re-send with confirm=true if the numbers "
                "are what you expect. Full-history backfills are a CLI operation on purpose."
            ),
        )

    running = ingests.active()
    if running:
        job = running[0]
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"refresh job {job.id} ({job.kind} of {job.symbol}) is already {job.status}. "
                f"Only one refresh runs at a time, so the two cannot race for the same "
                f"archives or crowd the collector's network. Cancel it, or wait."
            ),
        )
    _probe_lock(market, request.kind, symbol)

    job_id = ingests.create(kind=request.kind, symbol=symbol)
    ingests.launch(job_id)
    return {"job": ingests.get(job_id).to_json()}


@router.get("/data/updates")
def list_updates(
    limit: int = Query(20, ge=1, le=200),
    ingests: IngestStore = Depends(get_ingests),
) -> dict[str, Any]:
    return {"jobs": [job.to_json() for job in ingests.list(limit=limit)]}


@router.get("/data/updates/{job_id}")
def get_update(
    job_id: int, ingests: IngestStore = Depends(get_ingests)
) -> dict[str, Any]:
    return {"job": _job(ingests, job_id).to_json()}


@router.post("/data/updates/{job_id}/cancel")
def cancel_update(
    job_id: int, ingests: IngestStore = Depends(get_ingests)
) -> dict[str, Any]:
    _job(ingests, job_id)
    try:
        return {"job": ingests.cancel(job_id).to_json()}
    except IngestNotFound as exc:  # pragma: no cover - deleted between the two calls
        raise HTTPException(status_code=404, detail=str(exc)) from None


@router.get("/data/collector")
def get_collector(root: Path = Depends(get_root)) -> dict[str, Any]:
    return {"collector": _collector_json(market_root(root))}


# -------------------------------------------------------------------------- internals


def _job(ingests: IngestStore, job_id: int) -> Any:
    """Fetch a job, mapping both "no such id" and "not a plausible id" to 404.

    `IngestNotFound` is caught here rather than registered as an app-wide handler because
    `app.py`'s error handlers are shared by every router, and adding one there would make a
    store-specific lookup failure part of the whole application's contract.
    """
    if not 0 < job_id <= MAX_ID:
        raise HTTPException(status_code=404, detail=f"no refresh job with id {job_id}")
    try:
        return ingests.get(job_id)
    except IngestNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None


def _probe_lock(market: Path, kind: str, symbol: str) -> None:
    """Refuse now if another process holds the lake's refresh lock.

    **Taken and immediately released, rather than held across the launch.** The lock belongs
    to the worker: it heartbeats it on its own timer and releases it in a `finally`, and a
    lock acquired in this process would have neither -- if the launch then failed, the lake
    would stay locked until the stale timeout with nothing alive to release it.

    That leaves a window between the release here and the worker's own acquire, and the
    window is deliberately not closed. What it can lose is a race against a *different*
    process starting a refresh in the same few milliseconds, and the consequence is that the
    worker's own `refresh_lock` raises and the job records `RefreshLocked` verbatim -- which
    is the correct outcome, reached one step later. Within this process the check above for
    an already-active job is what prevents the ordinary double-click.
    """
    try:
        handle = refresh.refresh_lock(market, kind=kind, symbol=symbol)
    except refresh.RefreshLocked as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from None
    handle.release()


def _predicted_conflicts(market: Path, symbol: str, plan: refresh.RefreshPlan) -> list[str]:
    """Which planned periods the live collector already owns, as `"<dataset> <period>"`.

    Said *before* anything is fetched rather than discovered one `PartitionConflict` at a
    time while a progress bar runs. Qualified with the dataset because a dry run spans up to
    four of them and a bare list of dates could not be attributed to any.

    An empty list is the ordinary answer and means only that no *planned* period collides --
    it is not a claim that the lake has no conflicts, because periods already ingested are
    not in the plan at all.
    """
    predicted: list[str] = []
    for dataset_plan in plan.datasets:
        if not dataset_plan.to_fetch:
            continue
        try:
            owned = refresh.collector_owned_periods(
                market, dataset_plan.dataset, symbol, dataset_plan.to_fetch
            )
        except (KeyError, OSError):  # pragma: no cover - unreadable lake directory
            continue
        predicted.extend(f"{dataset_plan.dataset} {period}" for period in owned)
    return predicted


# ------------------------------------------------------------------- collector status


def _collector_json(market: Path) -> dict[str, Any]:
    """What can be *observed* about the live collector right now, and nothing more.

    **This card must never claim the Phase 1b exit criterion is met, or show a percentage
    toward it.** That criterion (PERPLAB_SPEC.md section 13, docs/PHASE_SIGNOFF.md) is a
    conjunction of three things: 72 hours of wall clock, a `detect_gaps` run across all five
    rules returning zero unexplained gaps, and a manual power-setting precondition that this
    process has no way to verify. Elapsed time is one conjunct out of three, and it is the
    only one anything here can see. A progress bar built from it would read as "72 % of the
    way to signed off" while the other two conjuncts were unevaluated -- which is precisely
    how a criterion designed to be hard to meet gets declared met. There is deliberately no
    field named for a criterion, a percentage or an uptime.

    Three consequences, each of which is a field decision rather than a caveat:

    **`run_started_ms` is when the current *run* started, not how long the collector has been
    up.** It comes from the newest CONNECT or RESTART on `stream='collector'`, which is the
    record every process start writes. Nothing in the lake proves the process stayed alive
    between that record and now -- a machine that slept would leave exactly the same trail --
    so the honest label is "current run started" and `run_elapsed_ms` is the distance from it,
    never "uptime".

    **`datasets_recording` comes from the heartbeat's own counter keys.** Not from
    `SCHEMAS`, not from `REST_DATASETS`, not from any constant in today's source: the process
    that is running may predate a poller that exists in this checkout, and listing what
    *should* be recording as what *is* would invent a dataset out of a code change. The
    keys the running collector puts in its own heartbeat detail are the only evidence.

    **`state_file_present=false` is ambiguous and is reported as ambiguous.** The file is
    deleted on a *clean* shutdown, so its absence means either "not running" or "was stopped
    tidily" -- and never "running". It goes in `caveats` rather than being rendered as a
    stopped light.
    """
    now = int(time.time() * 1000)
    caveats: list[str] = []

    state_path = market / COLLECTOR_STATE_FILE
    present = state_path.is_file()
    state = refresh.collector_state(market)
    if present and state is None:
        caveats.append(
            f"{COLLECTOR_STATE_FILE} exists but could not be parsed, so the pid and "
            f"heartbeat below are unknown rather than absent"
        )
    if not present:
        caveats.append(
            f"there is no {COLLECTOR_STATE_FILE}. That is ambiguous: the collector deletes "
            f"it on a clean shutdown, so this means either 'not running' or 'was stopped "
            f"tidily'. It never means 'running'."
        )

    pid = state.get("pid") if isinstance(state, dict) else None
    beat = state.get("last_heartbeat_ms") if isinstance(state, dict) else None
    pid = pid if isinstance(pid, int) else None
    beat = beat if isinstance(beat, int) else None

    events = _recent_collector_events(market, caveats)
    started, restarts = _run_start(events, caveats)
    recording = _datasets_recording(events, caveats)

    return {
        "state_file_present": present,
        "pid": pid,
        "last_heartbeat_ms": beat,
        # `None`, not zero, when there is no heartbeat to age. A zero here would render as
        # "beating right now", which is the opposite of what an absent heartbeat means.
        "heartbeat_age_ms": None if beat is None else now - beat,
        "run_started_ms": started,
        "run_elapsed_ms": None if started is None else now - started,
        "restarts_since_run_start": restarts,
        "datasets_recording": recording,
        "caveats": caveats,
    }


def _recent_collector_events(
    market: Path, caveats: list[str]
) -> list[tuple[int, str, str, str]]:
    """`(ts_ms, kind, stream, detail)` from the newest `EVENT_PARTITIONS_SCANNED` days.

    Newest partitions only, and read directly rather than through `data.query`: this endpoint
    is polled, and a DuckDB view over the whole `collectorEvents` glob would re-open every
    day the collector has ever run to answer a question about the last one.

    **One read over all the files, not one read per file.** The per-file loop this replaced
    cost about 3.1 s against a five-day lake of 4,590 part-files, against 0.65 s for the same
    files and the same 27,690 rows read in one call -- the difference is per-file open
    overhead, paid 4,590 times, on an endpoint the UI polls every 15 s. At the seven-partition
    bound that loop was heading for several seconds a poll in the steady state and far worse
    under any disk contention, which is what made the card sit on its loading skeleton.

    **The fallback is not decoration.** Reading files individually is what let a single
    unreadable file cost one file rather than the whole card, and that property is worth
    keeping: a bulk read raises on the first bad or schema-divergent file and would take every
    event down with it. So the fast path is tried first and a failure -- for any reason, since
    the reason does not change the remedy -- drops to the loop, which isolates the bad file
    and names it in a caveat. Both paths project `_EVENT_COLUMNS` and return the same tuples,
    so which one ran is invisible in the answer and is deliberately not reported: it is a fact
    about this process, not about the data, and these caveats are read as being about data.
    """
    directory = market / "collectorEvents"
    if not directory.is_dir():
        caveats.append(
            "the collectorEvents dataset does not exist in this lake, so nothing can be "
            "said about when the current run started or what it is recording"
        )
        return []

    partitions = sorted(
        (p for p in directory.iterdir() if p.is_dir() and p.name.startswith("date=")),
        key=lambda p: p.name,
        reverse=True,
    )[:EVENT_PARTITIONS_SCANNED]
    files = [
        file for partition in partitions for file in sorted(partition.glob("*.parquet"))
    ]
    if not files:
        return []

    rows = _read_events_together(files)
    if rows is None:
        rows = _read_events_separately(files, caveats)
    rows.sort(key=lambda row: row[0])
    return rows


def _read_events_together(files: list[Path]) -> list[tuple[int, str, str, str]] | None:
    """Every file in one read, or `None` if that is not possible for any reason.

    `None` rather than an exception because the caller's response to every failure is the
    same -- fall back to the loop that can attribute it to a file -- and because a partial
    result here would be worse than no result: the caller cannot tell which files a
    half-finished bulk read covered, and a card built on a silent subset of the events is
    exactly the kind of quiet wrong answer the rest of this endpoint is built to refuse.
    """
    import pyarrow.parquet as pq

    try:
        table = pq.read_table([str(file) for file in files], columns=list(_EVENT_COLUMNS))
    except Exception:  # noqa: BLE001 - the reason does not change the remedy
        return None
    return _rows_of(table)


def _read_events_separately(
    files: list[Path], caveats: list[str]
) -> list[tuple[int, str, str, str]]:
    """File by file, so one unreadable file costs one file and is named."""
    import pyarrow.parquet as pq

    rows: list[tuple[int, str, str, str]] = []
    for file in files:
        try:
            table = pq.read_table(file, columns=list(_EVENT_COLUMNS))
        except Exception as exc:  # noqa: BLE001 - an unreadable day is a finding
            caveats.append(
                f"{file.parent.name}/{file.name} could not be read "
                f"({type(exc).__name__}), so events in it are not counted below"
            )
            continue
        rows.extend(_rows_of(table))
    return rows


def _rows_of(table: Any) -> list[tuple[int, str, str, str]]:
    """`_EVENT_COLUMNS` as tuples. Shared so the two read paths cannot coerce differently."""
    return [
        (int(ts), str(kind), str(stream), str(detail or ""))
        for ts, kind, stream, detail in zip(
            table.column("ts_ms").to_pylist(),
            table.column("kind").to_pylist(),
            table.column("stream").to_pylist(),
            table.column("detail").to_pylist(),
        )
    ]


def _run_start(
    events: list[tuple[int, str, str, str]], caveats: list[str]
) -> tuple[int | None, int | None]:
    """`(run_started_ms, restarts_since_run_start)` from the collector's own records.

    `run_started_ms` is the newest CONNECT or RESTART on `stream='collector'`: the collector
    writes exactly one of the two every time the process starts (`_emit_startup_event`), so
    the newest is when the process now running began.

    `restarts_since_run_start` counts RESTART records from the newest **CONNECT** onward. A
    CONNECT is a cold start -- no state file, meaning the previous run shut down cleanly or
    there was no previous run -- so this is "how many times has the collector crashed and
    recovered during this recording campaign", which is the number an operator wants. When no
    CONNECT appears in the scanned window the count is `None`, not zero: the campaign began
    before the window and its crashes cannot be counted from here.
    """
    collector = [row for row in events if row[2] == "collector"]
    starts = [row for row in collector if row[1] in ("CONNECT", "RESTART")]
    if not starts:
        caveats.append(
            f"no CONNECT or RESTART record in the last {EVENT_PARTITIONS_SCANNED} day "
            f"partitions of collectorEvents, so when the current run started is unknown "
            f"rather than recent"
        )
        return None, None

    started = starts[-1][0]
    cold = [row for row in collector if row[1] == "CONNECT"]
    if not cold:
        caveats.append(
            "no cold-start CONNECT in the scanned window, so restarts during this "
            "recording campaign cannot be counted"
        )
        return started, None
    since = cold[-1][0]
    return started, sum(
        1 for row in collector if row[1] == "RESTART" and row[0] >= since
    )


def _datasets_recording(
    events: list[tuple[int, str, str, str]], caveats: list[str]
) -> list[str]:
    """Dataset names from the newest HEARTBEAT's own counter keys.

    The collector writes its heartbeat detail as `"<dataset>=<count>, ..."` over whatever
    writers *that process* was constructed with. Parsing those keys is the only way to
    describe a running process rather than this checkout's source constants.

    Only keys with a non-zero count are listed. A key at zero means the collector subscribed
    and nothing has ever arrived on it, which is the opposite of recording; it is named in a
    caveat instead, because it is a fact worth seeing and a fact worth not mislabelling.
    Counts are cumulative since the run started, so a dataset that stopped an hour ago still
    appears here -- said plainly rather than letting the list read as "live right now".
    """
    beats = [row for row in events if row[1] == "HEARTBEAT" and row[2] == "collector"]
    if not beats:
        caveats.append(
            "no HEARTBEAT record in the scanned window, so which datasets the collector "
            "is recording is unknown rather than none"
        )
        return []

    recording: list[str] = []
    silent: list[str] = []
    for part in beats[-1][3].split(","):
        key, separator, value = part.partition("=")
        key = key.strip()
        if not separator or not key:
            continue
        try:
            count = int(value.strip())
        except ValueError:
            # A counter that does not parse is not evidence of anything. Listed as unknown
            # rather than assumed to be either recording or silent.
            caveats.append(
                f"the newest heartbeat's counter for {key!r} is not a number, so whether "
                f"it is recording is unknown"
            )
            continue
        (recording if count > 0 else silent).append(key)

    if silent:
        caveats.append(
            f"the newest heartbeat carries a zero counter for {', '.join(sorted(silent))}: "
            f"subscribed, but nothing has arrived since this run started"
        )
    caveats.append(
        "these counters are cumulative since the run started, so a dataset listed here "
        "has recorded something during this run -- not necessarily in the last minute"
    )
    return sorted(recording)
