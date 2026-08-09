"""Lab job persistence -- the row, the artefacts, and the worker lifecycle (spec 9).

The shape deliberately mirrors `store.runs`: a SQLite row for "what jobs exist and how
did they go", a directory under `userdata/lab/<job_id>/` for "what exactly was produced",
artefacts written before the status says done, and a `_reap` that closes the books on
workers that died without saying so. Where the reasoning is identical to `RunStore`'s,
the docstring there is the reference rather than a copy here -- one place for an argument,
or the copies drift.

Two differences worth stating:

**Cancellation is cooperative, not `terminate()`.** A Lab worker nests a
`ProcessPoolExecutor` for the walk-forward's grid sweeps, and killing the parent on
Windows strands the pool's children mid-point -- they hold the lake open and burn CPU on
an answer nobody will read. `cancel` therefore writes the job's `control.json` and the
worker checks it between engine runs; the request takes effect at the next point
boundary. There is nothing here a hard kill protects (no exchange orders, no positions),
so slower-but-clean wins.

**A Lab job never sees a credential.** It reads the lake and the run directory, both on
disk already; there is nothing to hand it over stdin and `launch` opens no pipe.
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

__all__ = ["LAB_SUBDIR", "LAB_TOOLS", "LabJob", "LabNotFound", "LabStore"]

LAB_SUBDIR = "lab"

LAB_TOOLS = ("walkforward", "montecarlo", "regimes")
"""The runnable tools. Overfitting diagnostics (spec 9.4) ride inside every walk-forward
artefact rather than being a job of their own -- an overfitting check that is optional is
one that is skipped on exactly the runs that need it. Comparison (spec 9.6) is a read,
not a job: it computes in one request and produces nothing worth persisting."""


class LabNotFound(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class LabJob:
    """One row of the Lab jobs table."""

    id: int
    run_id: int
    tool: str
    status: str
    label: str
    config: Mapping[str, Any]
    created_ms: int
    started_ms: int | None
    finished_ms: int | None
    progress_done: int
    progress_total: int
    summary: Mapping[str, Any] | None
    error: str | None

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "tool": self.tool,
            "status": self.status,
            "label": self.label,
            "config": dict(self.config),
            "created_ms": self.created_ms,
            "started_ms": self.started_ms,
            "finished_ms": self.finished_ms,
            "progress_done": self.progress_done,
            "progress_total": self.progress_total,
            "summary": None if self.summary is None else dict(self.summary),
            "error": self.error,
        }


def _row_to_job(row: Any) -> LabJob:
    summary = row["summary_json"]
    return LabJob(
        id=int(row["id"]),
        run_id=int(row["run_id"]),
        tool=str(row["tool"]),
        status=str(row["status"]),
        label=str(row["label"] or ""),
        config=json.loads(row["config_json"] or "{}"),
        created_ms=int(row["created_ms"]),
        started_ms=row["started_ms"],
        finished_ms=row["finished_ms"],
        progress_done=int(row["progress_done"] or 0),
        progress_total=int(row["progress_total"] or 0),
        summary=None if summary is None else json.loads(summary),
        error=row["error"],
    )


class LabStore:
    """Lab jobs, their processes and their artefacts. One instance per API process --
    the same handle-ownership argument as `RunStore` (`deps.get_runs`)."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self._connection = db.connect(self.root)
        self._processes: dict[int, subprocess.Popen[bytes]] = {}
        self._stderr: dict[int, _StderrTail] = {}
        # **One lock, guarding both the connection and the handle maps.** `db.connect`
        # passes `check_same_thread=False` on the stated premise that "the connection is
        # guarded by a lock at the library level" -- true of `StrategyLibrary` and, until
        # this line, not of this store. FastAPI runs every `def` route in a thread pool,
        # so two concurrent requests shared one connection: `with self._lock, self._connection:` is
        # not an isolated transaction, so one thread's commit published the other's
        # half-written state and its rollback then undid nothing. The same race hit
        # `_reap`, where two threads deleting the same exited handle turned a routine
        # `GET /lab/jobs` into a 500.
        self._lock = threading.RLock()

    def close(self) -> None:
        self._connection.close()

    # ------------------------------------------------------------------------ paths

    def directory(self, job_id: int) -> Path:
        return self.root / LAB_SUBDIR / str(job_id)

    def artefact(self, job_id: int, name: str) -> Path:
        return self.directory(job_id) / name

    # ----------------------------------------------------------------------- create

    def create(
        self,
        *,
        run_id: int,
        tool: str,
        config: Mapping[str, Any],
        label: str = "",
    ) -> int:
        """Write the row and `job.json`. Does not start anything."""
        if tool not in LAB_TOOLS:
            raise ValueError(f"unknown Lab tool {tool!r}: use one of {list(LAB_TOOLS)}")
        now = _now_ms()
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO lab_jobs
                    (run_id, tool, status, label, config_json, created_ms, heartbeat_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    tool,
                    RunStatus.QUEUED,
                    label,
                    json.dumps(dict(config)),
                    now,
                    # Stamped at insert for the same reason `RunStore.create` stamps it:
                    # the stale sweep reads `coalesce(heartbeat_ms, created_ms)` and a
                    # row born without one would be dead on arrival.
                    now,
                ),
            )
        job_id = int(cursor.lastrowid)
        directory = self.directory(job_id)
        directory.mkdir(parents=True, exist_ok=True)
        _write_json(
            directory / "job.json",
            {"job_id": job_id, "run_id": run_id, "tool": tool, "label": label,
             "config": dict(config)},
        )
        return job_id

    def launch(self, job_id: int) -> None:
        creationflags = 0
        if sys.platform == "win32":  # pragma: no branch - single-platform deployment
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        process = subprocess.Popen(
            [sys.executable, "-m", "perplab.lab.worker", str(self.root), str(job_id)],
            cwd=str(Path(__file__).resolve().parents[2]),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            creationflags=creationflags,
        )
        # Registered under the lock: `_reap` iterates and deletes from these dicts while
        # holding it, and an unlocked insert from launch is exactly the shared-state
        # mutation the lock's docstring promises cannot happen (M25).
        with self._lock:
            if process.stderr is not None:
                self._stderr[job_id] = _StderrTail(process.stderr)
            self._processes[job_id] = process
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE lab_jobs SET pid = ?, heartbeat_ms = ? WHERE id = ?",
                (process.pid, _now_ms(), job_id),
            )

    # ----------------------------------------------------------------------- worker

    def mark_running(self, job_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE lab_jobs SET status = ?, started_ms = ?, heartbeat_ms = ? "
                "WHERE id = ?",
                (RunStatus.RUNNING, _now_ms(), _now_ms(), job_id),
            )

    def progress(self, job_id: int, done: int, total: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE lab_jobs SET progress_done = ?, progress_total = ?,
                    heartbeat_ms = ? WHERE id = ?
                """,
                (done, total, _now_ms(), job_id),
            )

    def complete(self, job_id: int, *, summary: Mapping[str, Any]) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE lab_jobs SET status = ?, finished_ms = ?, heartbeat_ms = ?,
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

    def fail(self, job_id: int, error: str, *, status: str = RunStatus.FAILED) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE lab_jobs SET status = ?, finished_ms = ?, heartbeat_ms = ?,
                    error = ? WHERE id = ?
                """,
                (status, _now_ms(), _now_ms(), error[:4000], job_id),
            )

    def stop_requested(self, job_id: int) -> bool:
        """Whether `cancel` has asked this job to stop. Polled by the worker."""
        path = self.directory(job_id) / "control.json"
        if not path.exists():
            return False
        try:
            return bool(json.loads(path.read_text(encoding="utf-8")).get("stop"))
        except (OSError, json.JSONDecodeError):  # pragma: no cover - torn read
            # Written atomically, so a malformed file means something other than the
            # store wrote it; treat as no instruction rather than guessing one.
            return False

    # ------------------------------------------------------------------------- reads

    def get(self, job_id: int) -> LabJob:
        self._reap()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM lab_jobs WHERE id = ?", (job_id,)
            ).fetchone()
        if row is None:
            raise LabNotFound(f"no Lab job with id {job_id}")
        return _row_to_job(row)

    def list(
        self,
        *,
        run_id: int | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[LabJob]:
        self._reap()
        clauses: list[str] = []
        params: list[Any] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(int(limit))
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM lab_jobs{where} ORDER BY created_ms DESC, id DESC LIMIT ?",
                params,
            ).fetchall()
        return [_row_to_job(row) for row in rows]

    def read_json(self, job_id: int, name: str) -> Any:
        path = self.artefact(job_id, name)
        if not path.exists():
            raise LabNotFound(
                f"Lab job {job_id} has no {name}; it may not have finished, or its "
                f"directory was removed"
            )
        return json.loads(path.read_text(encoding="utf-8"))

    # --------------------------------------------------------------------- lifecycle

    def cancel(self, job_id: int) -> LabJob:
        """Ask the worker to stop at its next **fold** boundary (walk-forward only).

        See the module docstring for why this is a file and not `terminate()`, and for
        which tools honour it -- Monte Carlo and regime analysis do not poll, so cancel
        marks their row and lets the process finish.
        """
        job = self.get(job_id)
        if job.status in RunStatus.TERMINAL:
            return job
        _write_json(
            self.directory(job_id) / "control.json",
            {"stop": True, "requested_ms": _now_ms()},
        )
        # Stamped on the row as well as in the file, so the UI has something to render
        # immediately. A button that produces no visible change gets clicked again.
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE lab_jobs SET cancel_requested_ms = ? WHERE id = ? "
                "AND cancel_requested_ms IS NULL",
                (_now_ms(), job_id),
            )
        return self.get(job_id)

    LAB_DELETABLE = frozenset(RunStatus.DELETABLE | {RunStatus.LOST})
    """Statuses a Lab job may be deleted from -- `RunStore.DELETABLE` **plus `lost`**.

    A run refuses deletion while `lost` because its directory may still be being written
    by a worker this process cannot see, and losing a run's artefacts is losing the only
    record of an execution. Neither argument survives here, and applying the run's rule
    to Lab jobs produced a trap with no way out: a `lost` job could not be deleted
    ("cancel it first"), could not be cancelled (`cancel` returns early for terminal
    statuses), and blocked deleting its source run ("delete those Lab jobs first") --
    three refusals pointing at each other, reachable by nothing worse than restarting the
    API server while a walk-forward ran.

    A Lab job is also cheap to lose in a way a run is not: it is derived from a run that
    still exists, so re-running it reproduces it exactly.
    """

    def delete(self, job_id: int) -> None:
        job = self.get(job_id)
        if job.status not in self.LAB_DELETABLE:
            raise ValueError(
                f"Lab job {job_id} is {job.status}; cancel it and wait for the worker "
                f"to stop before deleting, or it will keep writing into a directory "
                f"that no longer has a row"
            )
        directory = self.directory(job_id)
        if directory.is_dir():
            shutil.rmtree(directory)
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM lab_jobs WHERE id = ?", (job_id,))

    # --------------------------------------------------------------------- internals

    def _reap(self) -> None:
        """`RunStore._reap`'s reasoning, applied to Lab workers. See that docstring.

        Held under the lock end to end. Two threads reaping the same exited worker used
        to race on `del self._processes[job_id]`, turning a routine `GET /lab/jobs` into
        a `KeyError` 500 -- the pop below is now unreachable by a second thread, and
        `pop(..., None)` would have hidden the race rather than fixed the shared-state
        problem underneath it.
        """
        with self._lock:
            for job_id, process in list(self._processes.items()):
                code = process.poll()
                if code is None:
                    continue
                del self._processes[job_id]
                tail = self._stderr.pop(job_id, None)
                row = self._connection.execute(
                    "SELECT status FROM lab_jobs WHERE id = ?", (job_id,)
                ).fetchone()
                if row is None or str(row["status"]) in RunStatus.TERMINAL:
                    continue
                stderr = tail.read() if tail is not None else b""
                detail = stderr.decode("utf-8", "replace").strip()[-2000:]
                self.fail(
                    job_id,
                    f"the Lab worker exited with code {code} without recording a result"
                    + (f":\n{detail}" if detail else ""),
                )

            cutoff = _now_ms() - STALE_HEARTBEAT_MS
            stale = self._connection.execute(
                """
                SELECT id FROM lab_jobs
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
                f"this worker, so its fate is unknown rather than known-bad. Check "
                f"before deleting: any artefacts are still in this job's directory.",
                status=RunStatus.LOST,
            )
