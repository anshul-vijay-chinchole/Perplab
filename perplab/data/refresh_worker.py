"""The refresh worker -- one "bring my data up to date" click, one process.

```
python -m perplab.data.refresh_worker <userdata-root> <job-id>
```

A process rather than a thread in the API server, for the reason `store.runs` gives and one
of its own: a refresh is minutes to hours of network transfer, and a transfer that cannot be
ended without ending the platform is a transfer nobody will start while the collector is
recording something irreplaceable.

**This module is the assembly seam and nothing else.** `data.refresh` decides *what* a
refresh means -- where the range starts, where it ends, which days the collector owns, what a
refusal costs, what will never arrive. `data.ingest_bulk` does the fetching. This file's only
job is to run those two in the right order, keep the row honest while it does, and turn the
result into a verdict a person can act on. Every decision it does not make is deliberate; see
`refresh.py`'s module docstring for the four ways a naive version of this is wrong.

Four things here exist because of a specific failure:

**The heartbeat runs on a wall clock, not on progress.** `ingest_archive` retries a transient
fetch failure with backoff, and a single archive can therefore spend over five minutes inside
one call emitting no progress event at all. A heartbeat driven by progress callbacks would go
silent for longer than `STALE_HEARTBEAT_MS`, and `IngestStore._reap` would mark a job `lost`
-- and its lock abandoned -- while it was still downloading. The same timer heartbeats the
lock handle, so the two clocks cannot drift apart.

**Cancellation is polled on that same thread.** The download threads are inside
`ingest_range`, which takes a `threading.Event`; nothing else in this process is free to
notice a `control.json` appearing. Polling from the timer means a cancel is seen within one
tick whether the job is downloading, retrying, or waiting on a lock.

**Conflicts are measured, not just counted.** A declined day is reported by `ingest_range` as
`CONFLICT` with no statement of what the refusal left out. `refresh.measure_conflict_cost`
reads that one partition and answers it. On 2026-08-02 the unmeasured version of this
reported "1 declined" and eleven hours of the day were missing.

**Permanently unfetchable periods are recorded.** 619 archives on this lake fail identically
on every attempt -- 56 `markPriceKlines` days Binance never published (F7) and 563 `metrics`
days whose rows the parser refuses (F8). Without `refresh.record_barren` every click
re-downloads all of them and prints 619 lines nobody can act on, which is how the one line
that mattered got ignored.
"""

from __future__ import annotations

import argparse
import sys
import threading
import traceback
from pathlib import Path
from typing import Any

from perplab.data import refresh
from perplab.data.ingest_bulk import FileOutcome, IngestStatus, ingest_range
from perplab.data.schemas import market_root, normalise_symbol
from perplab.store.ingests import IngestStore
from perplab.store.runs import RunStatus

__all__ = ["HEARTBEAT_INTERVAL_S", "execute_job", "main"]


HEARTBEAT_INTERVAL_S = 15.0
"""Seconds between heartbeat/cancel-poll ticks.

Twelve times inside `store.runs.STALE_HEARTBEAT_MS` (180 s), so a job is declared `lost`
only after eleven consecutive ticks have failed to happen -- which is a stopped process, not
a busy one. It is also well inside `refresh.LOCK_STALE_MS` (the same 180 s), so the lock this
worker holds is refreshed long before another refresh could conclude it was abandoned.

The cost is one small `UPDATE` and one 120-byte file write per tick. A shorter interval would
buy a faster cancel and pay for it in SQLite write contention with the API server's polling,
against a cancel latency that is already dominated by the archive in flight.
"""

_PERMANENT_FAILURE_PREFIX = "MalformedArchive"
"""The one failure class that will never succeed on a retry.

A `MalformedArchive` is raised *after* the bytes have arrived and matched the publisher's own
checksum: the archive is exactly what Binance published, and this platform refuses to parse
it -- a value with more than eight decimal places (finding F8), or a zip whose member is not
the single CSV the registry expects. Downloading the same bytes again produces the same
refusal, forever.

Every other failure is deliberately excluded. A `ChecksumMismatch` means the bytes on the
wire were *not* what was published, which is exactly the transient corruption a retry fixes;
a timeout or a 5xx is the network. Recording either as permanent would write a claim of
"never" over a fault that clears itself, and the record is designed to be trusted for months.
"""


class _Counter:
    """Archives finished, shared between the download threads and the heartbeat thread.

    A plain int would be read by the timer while `ingest_range`'s workers increment it. The
    GIL happens to make that safe for `+= 1` on CPython today; relying on it would make the
    correctness of a progress bar a property of the interpreter's implementation, and the
    lock costs a microsecond per archive.
    """

    def __init__(self, total: int) -> None:
        self._lock = threading.Lock()
        self.done = 0
        self.total = total

    def finished(self) -> None:
        with self._lock:
            self.done += 1

    def read(self) -> tuple[int, int]:
        with self._lock:
            return self.done, self.total


class _ProgressCounter:
    """`ingest_bulk.Progress`, reduced to the one event this worker needs.

    Only `end` does anything. The byte-level callbacks fire per chunk -- thousands of times
    per archive -- and writing the row from them would turn a download into a SQLite write
    storm for a number the UI redraws once a second anyway.
    """

    def __init__(self, counter: _Counter) -> None:
        self._counter = counter

    def plan(self, dataset: str, symbol: str, periods: int) -> None:
        return None

    def note(self, message: str) -> None:
        return None

    def begin(self, dataset: str, symbol: str, period: str, url: str) -> None:
        return None

    def bytes_read(self, count: int, total: int | None) -> None:
        return None

    def end(self, outcome: FileOutcome) -> None:
        self._counter.finished()


class _Pulse:
    """The wall-clock heartbeat, and the only thing that watches for a cancel.

    A thread that waits on an `Event` rather than a chain of `threading.Timer` objects: a
    `Timer` is a fresh thread per tick and its cancellation is racy against a tick already in
    flight, which for a job that runs for hours means hundreds of threads and one that
    reliably outlives the job it belongs to. One thread, one flag, one join.

    Nothing in here may raise into the job. A heartbeat that fails is a heartbeat that will
    be retried in fifteen seconds; a heartbeat that kills its own thread turns a transient
    disk error into a `lost` job whose download is still running and whose lock is still held.
    """

    def __init__(
        self,
        store: IngestStore,
        job_id: int,
        handle: Any,
        counter: _Counter,
        stop: threading.Event,
    ) -> None:
        self._store = store
        self._job_id = job_id
        self._handle = handle
        self._counter = counter
        self._stop = stop
        self._finished = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name=f"perplab-refresh-pulse-{job_id}", daemon=True
        )

    def __enter__(self) -> _Pulse:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._finished.set()
        self._thread.join(timeout=HEARTBEAT_INTERVAL_S + 5.0)

    def tick(self) -> None:
        """One beat. Public so a test can drive it without waiting fifteen seconds."""
        done, total = self._counter.read()
        try:
            self._store.progress(self._job_id, done, total)
        except Exception:  # noqa: BLE001 - see the class docstring
            pass
        try:
            self._handle.heartbeat()
        except Exception:  # noqa: BLE001
            pass
        try:
            if self._store.stop_requested(self._job_id):
                self._stop.set()
        except Exception:  # noqa: BLE001
            pass

    def _loop(self) -> None:
        # Beat once immediately. The first archive can take minutes, and a job whose row
        # still reads `progress_total = 0` for the first fifteen seconds looks stuck at
        # exactly the moment the operator is watching hardest.
        self.tick()
        while not self._finished.wait(HEARTBEAT_INTERVAL_S):
            self.tick()


REFUSALS = (refresh.RefreshLocked, refresh.CollectorBusy, refresh.InsufficientDisk)
"""Preconditions that said no. Their messages are the whole answer and are stored verbatim.

Recorded without a traceback, unlike every other failure. Each of these three is written to
tell an operator what to do -- the disk refusal names the two figures it compared, the
collector refusal names the heartbeat age and says to try again once it recovers -- and
prefixing that with a Python stack turns a sentence someone can act on into something that
reads as a crash. A genuine crash keeps its traceback, because there the stack *is* the
information.
"""


def execute_job(root: Path, job_id: int, store: IngestStore) -> None:
    """Run one refresh job to a terminal status. Never raises for data.

    The lock is taken *before* `mark_running`, so a job that cannot get it never shows a
    started time it did not have. It is released in a `finally` that wraps everything below
    it, including the preconditions -- a `CollectorBusy` refusal that left the lake locked
    would block every subsequent refresh for three minutes over a check that took a
    millisecond.
    """
    root = Path(root)
    market = market_root(root)
    job = store.get(job_id)
    kind = job.kind
    symbol = normalise_symbol(job.symbol)

    handle = refresh.refresh_lock(market, kind=kind, symbol=symbol)
    try:
        store.mark_running(job_id)
        stop = threading.Event()
        # **Read once, synchronously, before the pulse exists.** A cancel that landed while
        # the job was queued -- or while it was waiting on the lock -- must be seen before
        # the first archive is requested, and the heartbeat thread's first tick races the
        # first download. `ingest_range` already treats an already-set event as "do not
        # start", so setting it here is the same unwind path rather than a second one.
        if store.stop_requested(job_id):
            stop.set()
        counter = _Counter(0)
        with _Pulse(store, job_id, handle, counter, stop):
            _run(market, job_id, store, kind, symbol, counter, stop)
    finally:
        handle.release()


def _run(
    market: Path,
    job_id: int,
    store: IngestStore,
    kind: str,
    symbol: str,
    counter: _Counter,
    stop: threading.Event,
) -> None:
    # Re-checked here rather than trusted from the API's own preflight. Minutes can pass
    # between the click and the worker reaching this line -- the queue, process start, the
    # lock -- and the collector's health is exactly the kind of fact that changes inside
    # that window. The check is one file read.
    refresh.require_collector_idle(market)
    plan = refresh.plan_refresh(market, symbol, kind)
    refresh.require_disk_headroom(market, plan)

    counter.total = plan.archives
    notes: list[str] = list(plan.notes)
    conflicts: list[refresh.ConflictCost] = []
    datasets: list[dict[str, Any]] = []
    totals = {status: 0 for status in IngestStatus}
    rows = 0
    downloaded = 0

    for dataset_plan in plan.datasets:
        periods = dataset_plan.to_fetch
        if dataset_plan.note:
            notes.append(f"{dataset_plan.dataset}: {dataset_plan.note}")
        if not periods:
            # **Skipped entirely, not requested as an empty range.** `plan_refresh` has
            # already subtracted everything with a receipt and everything recorded barren,
            # so an empty list means there is genuinely nothing to do. Handing the
            # dataset's full published span to `ingest_range` anyway would re-probe every
            # period the barren record exists to stop -- which is where the 619 doomed
            # archives come back from.
            continue
        if stop.is_set():
            break

        # The span of what remains, not the dataset's whole history. Both bounds come from
        # the trimmed plan, so periods the barren record removed fall outside the request
        # unless a genuine gap happens to straddle them -- and a straddled barren period is
        # re-probed once, then recorded again, rather than on every click forever.
        report = ingest_range(
            market,
            symbol,
            dataset_plan.dataset,
            periods[0],
            periods[-1],
            concurrency=refresh.REFRESH_CONCURRENCY,
            progress=_ProgressCounter(counter),
            stop=stop,
        )
        notes.extend(f"{dataset_plan.dataset}: {w}" for w in report.warnings)
        rows += report.rows
        downloaded += report.bytes_downloaded
        for status in IngestStatus:
            totals[status] += report.count(status)

        for outcome in report.outcomes:
            if outcome.status is IngestStatus.CONFLICT:
                conflicts.append(
                    refresh.measure_conflict_cost(
                        market, outcome.dataset, symbol, outcome.period
                    )
                )
        _record_unfetchable(market, symbol, dataset_plan.dataset, report.outcomes, notes)

        datasets.append(
            {
                "dataset": dataset_plan.dataset,
                "written": report.count(IngestStatus.WRITTEN),
                "declined": report.count(IngestStatus.CONFLICT),
                "missing": report.count(IngestStatus.MISSING),
                # `report.failures`, not `count(FAILED)`: it folds in `UNAVAILABLE`, which
                # is a dataset with no bulk source at all (finding F2). Counting that as
                # zero failures would let a dataset that fetched *nothing* report clean.
                "failed": len(report.failures),
            }
        )

    failed = sum(int(d["failed"]) for d in datasets)
    summary = {
        "verdict": _verdict(failed, conflicts),
        "written": totals[IngestStatus.WRITTEN],
        "skipped": totals[IngestStatus.SKIPPED],
        "declined": totals[IngestStatus.CONFLICT],
        "missing": totals[IngestStatus.MISSING],
        "failed": failed,
        "rows": rows,
        "bytes_downloaded": downloaded,
        "notes": notes,
        "conflicts": [
            {
                "dataset": cost.dataset,
                "period": cost.period,
                "collector_rows": cost.collector_rows,
                "missing_ms": cost.missing_ms,
                "note": cost.note,
            }
            for cost in conflicts
        ],
        "datasets": datasets,
    }

    if stop.is_set():
        # **Cancelled, with the summary kept.** The partitions written before the stop are
        # complete and durable, and reporting a cancel as "nothing happened" would send the
        # operator back to re-plan work that is already done.
        store.fail(
            job_id,
            "cancelled by the user. Everything written before the stop is complete and "
            "durable; re-running the refresh resumes from there rather than repeating it.",
            status=RunStatus.CANCELLED,
            summary=summary,
        )
        return

    done, total = counter.read()
    store.progress(job_id, done, total)
    store.complete(job_id, summary=summary)


def _verdict(failed: int, conflicts: list[refresh.ConflictCost]) -> str:
    """`"clean"` only when nothing failed and no refusal demonstrably cost anything.

    **A conflict whose `missing_ms` is `None` is not clean.** `None` there means the
    partition could not be read at all, so what the refusal left out is *unknown* -- and an
    unknown rendered as green is the exact shape of the 2026-08-02 failure, where "we
    declined" was displayed as "we are finished" and eleven hours went missing for a day.
    Two words, and only one of them may be said without evidence.
    """
    if failed:
        return "attention"
    if any(cost.missing_ms is None or cost.missing_ms > 0 for cost in conflicts):
        return "attention"
    return "clean"


def _record_unfetchable(
    market: Path,
    symbol: str,
    dataset: str,
    outcomes: list[FileOutcome],
    notes: list[str],
) -> None:
    """Persist the periods this attempt proved cannot be fetched. See `refresh.BARREN_NAME`.

    Two classes, and both are already documented findings on this lake:

    `MISSING` is a 404 *inside* the dataset's published coverage window -- `ingest_range`
    has already distinguished it from `UNPUBLISHED`, which is the documented edge of the
    history and is not recorded here. The 56 `markPriceKlines` days of finding F7 are this.

    `FAILED` with a `MalformedArchive` is an archive that arrived intact and was refused on
    its contents. The 563 `metrics` days of finding F8 are this.

    Nothing else qualifies, and the omissions are the point: a timeout, a 5xx or a checksum
    mismatch all clear on their own, and writing "never" over one of them would suppress a
    day that upstream is perfectly willing to serve tomorrow.
    """
    barren: dict[str, str] = {}
    for outcome in outcomes:
        if outcome.status is IngestStatus.MISSING:
            barren[outcome.period] = (
                "404 inside the dataset's published coverage window -- the archive was "
                "never published for this period"
            )
        elif outcome.status is IngestStatus.FAILED and str(
            outcome.error or ""
        ).startswith(_PERMANENT_FAILURE_PREFIX):
            barren[outcome.period] = (
                f"the archive downloaded and matched its published checksum, and this "
                f"platform refuses its contents: {outcome.error}"
            )
    if not barren:
        return
    refresh.record_barren(market, dataset, symbol, barren)
    notes.append(
        f"{dataset}: {len(barren)} period(s) recorded as unfetchable upstream and will be "
        f"skipped by future refreshes. Delete {refresh.BARREN_NAME} under the lake's "
        f"_ingest directory to re-probe them."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m perplab.data.refresh_worker",
        description="Execute one stored data-refresh job. Started by the API server.",
    )
    parser.add_argument("root", type=Path, help="the userdata root")
    parser.add_argument("job_id", type=int)
    args = parser.parse_args(argv)

    store = IngestStore(args.root)
    try:
        execute_job(args.root, args.job_id, store)
    except BaseException as exc:  # noqa: BLE001 - a refused refresh is data, not a crash
        if isinstance(exc, REFUSALS):
            detail = str(exc)
        else:
            detail = f"{type(exc).__name__}: {exc}\n\n" + "".join(
                traceback.format_exception(exc)
            )
        try:
            store.fail(args.job_id, detail)
        finally:
            print(detail, file=sys.stderr)
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    raise SystemExit(main())
