"""Refresh job persistence -- the row, the control file, and the worker lifecycle.

The shape is `store.lab`'s, which is in turn `store.runs`': a SQLite row for "what refreshes
exist and how did they go", a directory under `userdata/ingests/<job_id>/` for the two files
a worker needs to be told and to be stopped, and a `_reap` that closes the books on workers
that died without saying so. Where the reasoning is identical to `LabStore`'s, that
docstring is the reference rather than a copy here -- one place for an argument, or the
copies drift.

Three differences worth stating:

**Cancellation is cooperative, and here that is not a preference.** A refresh worker holds
the lake-wide refresh lock (`data.refresh.refresh_lock`) and releases it in a `finally`.
`terminate()` skips `finally` on Windows -- the process dies between the `os.open` that
took the lock and the `unlink` that gives it back -- so a hard kill would leave a lock file
with a frozen heartbeat that blocks every subsequent refresh for `LOCK_STALE_MS`. Worse, it
would strand a half-downloaded temporary and, mid-`os.replace`, could land a partition file
whose receipt was never written. The worker polls `stop_requested` on its heartbeat thread
and unwinds through `ingest_range`'s own stop event, which is the path Ctrl+C already takes.

**A refresh job never sees a credential.** It talks to `data.binance.vision`, which is
public and unauthenticated; there is nothing to hand it over stdin and `launch` opens no
pipe for one.

**The heartbeat does not come from progress.** That is the worker's business rather than
this store's, but it decides what `heartbeat_ms` means in these rows and therefore how
`_reap` reads them: a single `aggTrades` archive can retry for over five minutes with no
progress event at all, so a progress-driven heartbeat would have `_reap` declaring a job
`lost` while it was still downloading. See `data.refresh_worker`.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from perplab.store import db
from perplab.store.runs import (
    STALE_HEARTBEAT_MS,
    RunStatus,
    _now_ms,
    _StderrTail,
    _write_json,
)

__all__ = [
    "INGESTS_SUBDIR",
    "IngestJob",
    "IngestNotFound",
    "IngestStore",
]

INGESTS_SUBDIR = "ingests"
"""`userdata/ingests/` -- beside `userdata/runs/` and `userdata/lab/`.

Deliberately *not* under the lake root's `_ingest/`, which holds the receipt ledger, the
barren record and the refresh lock. Those are facts about the **data**: they are read by
the CLI, they must survive a database that is deleted and rebuilt, and they mean the same
thing to a process that has never opened SQLite. A job directory is a fact about one
*attempt* by this platform, keyed by a row id that only exists in the database. Mixing the
two would put a directory named `7` next to `unfetchable.json` and make "delete the job
history" a decision about the lake.
"""


class IngestNotFound(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class IngestJob:
    """One row of the refresh jobs table."""

    id: int
    kind: str
    symbol: str
    status: str
    created_ms: int
    started_ms: int | None
    finished_ms: int | None
    progress_done: int
    progress_total: int
    summary: Mapping[str, Any] | None
    error: str | None
    cancel_requested_ms: int | None

    def to_json(self) -> dict[str, Any]:
        """The wire shape. `cancel_requested` is a **bool**, not the stamp.

        The client's only question is whether the Cancel button has already been pressed --
        it renders a disabled button and a "stopping..." label from it. Sending the raw
        millisecond stamp would invite a UI to subtract it from `Date.now()` and display an
        elapsed time that means nothing: cancellation lands at the next archive boundary,
        so the interval between the request and the stop is a property of whatever download
        was in flight, not a countdown to anything.
        """
        return {
            "id": self.id,
            "kind": self.kind,
            "symbol": self.symbol,
            "status": self.status,
            "created_ms": self.created_ms,
            "started_ms": self.started_ms,
            "finished_ms": self.finished_ms,
            "progress_done": self.progress_done,
            "progress_total": self.progress_total,
            "summary": None if self.summary is None else dict(self.summary),
            "error": self.error,
            "cancel_requested": self.cancel_requested_ms is not None,
        }


def _row_to_job(row: Any) -> IngestJob:
    summary = row["summary_json"]
    return IngestJob(
        id=int(row["id"]),
        kind=str(row["kind"]),
        symbol=str(row["symbol"]),
        status=str(row["status"]),
        created_ms=int(row["created_ms"]),
        started_ms=row["started_ms"],
        finished_ms=row["finished_ms"],
        progress_done=int(row["progress_done"] or 0),
        progress_total=int(row["progress_total"] or 0),
        summary=None if summary is None else json.loads(summary),
        error=row["error"],
        cancel_requested_ms=row["cancel_requested_ms"],
    )


class IngestStore:
    """Refresh jobs, their processes and their control files. One instance per API process.

    The same handle-ownership argument as `RunStore` (`api.deps.get_runs`): this object
    holds the `Popen` handles for the workers *this* process launched, and that is the only
    way a worker that died without recording a result is ever noticed. A store built per
    request would have no handles, and every crashed refresh would sit in the UI claiming to
    be running until its heartbeat expired three minutes later.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self._connection = db.connect(self.root)
        self._processes: dict[int, subprocess.Popen[bytes]] = {}
        self._stderr: dict[int, _StderrTail] = {}
        # **One lock, guarding both the connection and the handle maps** -- audit finding
        # M25, and the reason every read below takes it too. `db.connect` passes
        # `check_same_thread=False` on the premise that "the connection is guarded by a
        # lock at the library level"; FastAPI runs every `def` route in a thread pool, so
        # without this two concurrent polls of `GET /api/data/updates` share one connection
        # and `with self._connection:` is not an isolated transaction -- one thread's commit
        # publishes the other's half-written state and its rollback then undoes nothing. The
        # same race hits `_reap`, where two threads deleting the same exited handle turn a
        # routine poll into a 500. A refresh is polled once a second by an open tab, which
        # is precisely the traffic pattern that finds this.
        self._lock = threading.RLock()

    def close(self) -> None:
        self._connection.close()

    # ------------------------------------------------------------------------ paths

    def directory(self, job_id: int) -> Path:
        return self.root / INGESTS_SUBDIR / str(job_id)

    def artefact(self, job_id: int, name: str) -> Path:
        return self.directory(job_id) / name

    # ----------------------------------------------------------------------- create

    def create(self, *, kind: str, symbol: str) -> int:
        """Write the row and `job.json`. Does not start anything.

        `job.json` duplicates two columns the worker could have read from SQLite, and that
        is the point: the worker opens the database anyway, but a job whose row is
        unreadable for any reason still has to be able to say *what it was asked to do* in
        its own failure record. The same argument `LabStore.create` makes.
        """
        now = _now_ms()
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO ingest_jobs
                    (kind, symbol, status, created_ms, heartbeat_ms)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    kind,
                    symbol,
                    RunStatus.QUEUED,
                    now,
                    # Stamped at insert for the same reason `RunStore.create` stamps it:
                    # the stale sweep reads `coalesce(heartbeat_ms, created_ms)` and a row
                    # born without one would be dead on arrival.
                    now,
                ),
            )
        job_id = int(cursor.lastrowid)
        directory = self.directory(job_id)
        directory.mkdir(parents=True, exist_ok=True)
        _write_json(
            directory / "job.json",
            {"job_id": job_id, "kind": kind, "symbol": symbol, "created_ms": now},
        )
        return job_id

    def launch(self, job_id: int) -> None:
        creationflags = 0
        if sys.platform == "win32":  # pragma: no branch - single-platform deployment
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        process = subprocess.Popen(
            [sys.executable, "-m", "perplab.data.refresh_worker", str(self.root),
             str(job_id)],
            cwd=str(Path(__file__).resolve().parents[2]),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            creationflags=creationflags,
        )
        # Registered under the lock **before** the DB write: `_reap` iterates and deletes
        # from these dicts while holding it, and an unlocked insert from launch is exactly
        # the shared-state mutation the lock's docstring promises cannot happen (M25).
        # Order matters as well as locking -- a reader that saw the `pid` column populated
        # while `self._processes` was still empty would conclude this process has no handle
        # to a worker it had in fact just started.
        with self._lock:
            if process.stderr is not None:
                self._stderr[job_id] = _StderrTail(process.stderr)
            self._processes[job_id] = process
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE ingest_jobs SET pid = ?, heartbeat_ms = ? WHERE id = ?",
                (process.pid, _now_ms(), job_id),
            )

    # ----------------------------------------------------------------------- worker

    def mark_running(self, job_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE ingest_jobs SET status = ?, started_ms = ?, heartbeat_ms = ? "
                "WHERE id = ?",
                (RunStatus.RUNNING, _now_ms(), _now_ms(), job_id),
            )

    def progress(self, job_id: int, done: int, total: int) -> None:
        """Record archives-completed-of-planned, and stamp the heartbeat.

        This is also the *only* thing that stamps `heartbeat_ms` while a job runs, which is
        why `refresh_worker` calls it from a wall-clock timer rather than from the ingest's
        progress callbacks. Both counters are re-sent every tick even when neither has
        moved: an unchanged number written on time is the evidence that the worker is alive,
        and a store method that skipped the write when nothing changed would turn a slow
        archive into a `lost` job.
        """
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE ingest_jobs SET progress_done = ?, progress_total = ?,
                    heartbeat_ms = ? WHERE id = ?
                """,
                (done, total, _now_ms(), job_id),
            )

    def complete(self, job_id: int, *, summary: Mapping[str, Any]) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE ingest_jobs SET status = ?, finished_ms = ?, heartbeat_ms = ?,
                    summary_json = ?, error = NULL WHERE id = ?
                """,
                (
                    RunStatus.DONE,
                    _now_ms(),
                    _now_ms(),
                    json.dumps(dict(summary)),
                    job_id,
                ),
            )

    def fail(
        self,
        job_id: int,
        error: str,
        *,
        status: str = RunStatus.FAILED,
        summary: Mapping[str, Any] | None = None,
    ) -> None:
        """End the job badly, optionally keeping what it managed to do first.

        `summary` is accepted here and not only on `complete` because a cancelled refresh
        has genuinely written partitions, and they are durable: every published file is one
        `os.replace` and every receipt is written last. Discarding the tally on the way out
        would leave the operator unable to tell a cancel that downloaded nothing from one
        that downloaded six days -- and the second is a very different starting point for
        the next attempt.
        """
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE ingest_jobs SET status = ?, finished_ms = ?, heartbeat_ms = ?,
                    error = ?, summary_json = coalesce(?, summary_json) WHERE id = ?
                """,
                (
                    status,
                    _now_ms(),
                    _now_ms(),
                    error[:4000],
                    None if summary is None else json.dumps(dict(summary)),
                    job_id,
                ),
            )

    def stop_requested(self, job_id: int) -> bool:
        """Whether `cancel` has asked this job to stop. Polled by the worker.

        A file rather than a database read, matching `LabStore.stop_requested`, and here
        with an extra reason: the worker polls this every few seconds from its heartbeat
        thread while up to two download threads run, and a `stat` plus a 40-byte read costs
        nothing, where a SQLite read would contend with the same connection's heartbeat
        writes for the duration of a multi-hour backfill.
        """
        path = self.directory(job_id) / "control.json"
        if not path.exists():
            return False
        try:
            return bool(json.loads(path.read_text(encoding="utf-8")).get("stop"))
        except (OSError, json.JSONDecodeError):  # pragma: no cover - torn read
            # Written atomically, so a malformed file means something other than the store
            # wrote it; treat as no instruction rather than guessing one.
            return False

    # ------------------------------------------------------------------------- reads

    def get(self, job_id: int) -> IngestJob:
        self._reap()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM ingest_jobs WHERE id = ?", (job_id,)
            ).fetchone()
        if row is None:
            raise IngestNotFound(f"no refresh job with id {job_id}")
        return _row_to_job(row)

    def list(self, *, status: str | None = None, limit: int = 50) -> list[IngestJob]:
        self._reap()
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(int(limit))
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM ingest_jobs{where} "
                f"ORDER BY created_ms DESC, id DESC LIMIT ?",
                params,
            ).fetchall()
        return [_row_to_job(row) for row in rows]

    def active(self) -> list[IngestJob]:
        """Jobs that have not finished -- what a second refresh request collides with.

        Separate from `list(status=...)` because "not finished" is two statuses and the
        caller asking this question must not have to know which. It is asked on the write
        path of `POST /api/data/update`, where getting the set wrong by one status means
        either refusing a refresh for no reason or starting a second one beside a running
        download.
        """
        self._reap()
        placeholders = ", ".join("?" for _ in RunStatus.TERMINAL)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM ingest_jobs WHERE status NOT IN ({placeholders}) "
                f"ORDER BY created_ms DESC, id DESC",
                list(RunStatus.TERMINAL),
            ).fetchall()
        return [_row_to_job(row) for row in rows]

    # --------------------------------------------------------------------- lifecycle

    def cancel(self, job_id: int) -> IngestJob:
        """Ask the worker to stop at its next archive boundary.

        See the module docstring for why this is a file and not `terminate()`. The delay is
        bounded by one archive: `ingest_range`'s stop event is checked per megabyte inside a
        transfer as well as between periods, so even a 40 MB `aggTrades` day unwinds in
        seconds rather than at the end of the download.
        """
        job = self.get(job_id)
        if job.status in RunStatus.TERMINAL:
            return job
        _write_json(
            self.directory(job_id) / "control.json",
            {"stop": True, "requested_ms": _now_ms()},
        )
        # Stamped on the row as well as in the file, so the UI has something to render
        # immediately. A button that produces no visible change gets clicked again -- and a
        # second click here does not merely duplicate work, it makes the operator believe
        # the first one failed while a download they asked to stop is still running.
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE ingest_jobs SET cancel_requested_ms = ? WHERE id = ? "
                "AND cancel_requested_ms IS NULL",
                (_now_ms(), job_id),
            )
        return self.get(job_id)

    def delete(self, job_id: int) -> None:
        """Remove the row and the job directory.

        Refused while the worker may still be running, for `LabStore.delete`'s reason: the
        directory holds `control.json`, and deleting it out from under a live worker removes
        the only channel through which that worker can be told to stop.
        """
        job = self.get(job_id)
        if job.status not in RunStatus.DELETABLE | {RunStatus.LOST}:
            raise ValueError(
                f"refresh job {job_id} is {job.status}; cancel it and wait for the worker "
                f"to stop before deleting, or it will keep downloading with no row and no "
                f"way to be stopped"
            )
        directory = self.directory(job_id)
        if directory.is_dir():
            shutil.rmtree(directory)
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM ingest_jobs WHERE id = ?", (job_id,))

    # --------------------------------------------------------------------- internals

    def _reap(self) -> None:
        """`RunStore._reap`'s reasoning, applied to refresh workers. See that docstring.

        Held under the lock end to end. Two threads reaping the same exited worker would
        otherwise race on `del self._processes[job_id]`, turning a routine poll of
        `GET /api/data/updates` into a `KeyError` 500 -- the delete below is unreachable by
        a second thread, and `pop(..., None)` would have hidden the race rather than fixed
        the shared-state problem underneath it.

        The stale sweep matters more here than anywhere else on the platform, because a
        refresh worker that dies without saying so leaves the lake-wide refresh lock behind
        it. The row going `lost` and the lock ageing out are two independent clocks on the
        same event (`STALE_HEARTBEAT_MS` and `refresh.LOCK_STALE_MS`, both three minutes),
        so the UI stops spinning at about the moment a new refresh becomes possible.
        """
        with self._lock:
            for job_id, process in list(self._processes.items()):
                code = process.poll()
                if code is None:
                    continue
                del self._processes[job_id]
                tail = self._stderr.pop(job_id, None)
                row = self._connection.execute(
                    "SELECT status FROM ingest_jobs WHERE id = ?", (job_id,)
                ).fetchone()
                if row is None or str(row["status"]) in RunStatus.TERMINAL:
                    continue
                stderr = tail.read() if tail is not None else b""
                detail = stderr.decode("utf-8", "replace").strip()[-2000:]
                self.fail(
                    job_id,
                    f"the refresh worker exited with code {code} without recording a "
                    f"result. Nothing written before it died is lost -- every published "
                    f"partition is one atomic replace and its receipt is written last -- "
                    f"so re-running the refresh resumes rather than repeats"
                    + (f":\n{detail}" if detail else ""),
                )

            cutoff = _now_ms() - STALE_HEARTBEAT_MS
            stale = self._connection.execute(
                """
                SELECT id FROM ingest_jobs
                WHERE status IN (?, ?) AND coalesce(heartbeat_ms, created_ms) < ?
                """,
                (RunStatus.QUEUED, RunStatus.RUNNING, cutoff),
            ).fetchall()
            self._fail_stale(stale)

    def _fail_stale(self, stale: list[Any]) -> None:
        for row in stale:
            job_id = int(row["id"])
            if job_id in self._processes:
                continue
            self.fail(
                job_id,
                f"no heartbeat for over {STALE_HEARTBEAT_MS // 1000}s and no handle to "
                f"this worker, so its fate is unknown rather than known-bad. It may still "
                f"be downloading: check for a lock under the lake's _ingest directory "
                f"before starting another refresh.",
                status=RunStatus.LOST,
            )
