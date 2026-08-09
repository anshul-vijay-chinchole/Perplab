"""Run persistence -- the metadata row, the artefacts, and the job worker that fills them.

**Runs execute in their own process.** Spec 2.3 puts backtests in a "job runner pool -- N
worker processes... isolated so an infinite loop in strategy code kills one worker, not the
platform", and spec 11 forbids the API server from executing strategy code at all. So
`launch` spawns `python -m perplab.engine.worker`, and everything the two processes need to
say to each other goes through SQLite and the run directory. Nothing about a strategy can
hang the server, and killing a run is killing a process rather than hoping a thread notices
a flag.

**Two storage layers, split by what the question is.** The SQLite row answers "what runs
exist and how did they do" -- it is what the Runs table sorts and filters, so it carries the
denormalised headline figures. The run directory answers "what exactly happened", and is
where the event log, the equity series, the trades and the manifest live. Putting the event
log in SQLite would make the metadata database grow by hundreds of megabytes per run; putting
the metrics only on disk would make listing fifty runs open fifty files.

**Artefacts are written before the row is marked done.** A run whose status says `done` and
whose directory is half-written is worse than one that says `failed`, because the first is
believed. The status transition is the last thing the worker does.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

from perplab.store import db

__all__ = [
    "RUNS_SUBDIR",
    "RunStatus",
    "RunNotFound",
    "RunSummary",
    "RunStore",
    "PROGRESS_EQUITY_NAME",
    "STALE_HEARTBEAT_MS",
]

RUNS_SUBDIR = "runs"
"""`userdata/runs/` -- spec 2.4's own name for it."""

PROGRESS_EQUITY_NAME = "progress_equity.json"
"""Artefact holding the equity curve of a run that is still going.

Named here rather than in either worker because three modules have to agree on it: the
backtest worker and the session worker write it, and the runs router reads it. It is
deliberately *not* `equity.parquet` -- that file's existence is what every reader uses to
mean "this run finished", and overloading it would break that test. Deleted when the run
finishes, since the finished artefact supersedes it."""

STALE_HEARTBEAT_MS = 180_000
"""How long a `running` row may go without a heartbeat before it is presumed dead.

Three minutes, chosen against the slowest thing a worker does between heartbeats: the
initial lake scan, which on a six-year BTCUSDT lake takes seconds, not minutes. Long enough
that a slow load is never mistaken for a corpse; short enough that a machine that lost power
mid-run does not leave a row claiming to be running tomorrow.
"""


STDERR_TAIL_BYTES = 64 * 1024
"""How much of a worker's stderr is kept in memory for its failure record.

Generous against what the record can hold -- `_reap` keeps the last 2000 characters and the
row truncates at 4000 -- but a ceiling all the same, because a strategy stuck in a
`traceback.print_exc()` loop produces megabytes and the point of draining the pipe is to
keep the worker running, not to archive its noise.
"""

_STDERR_BLOCK_BYTES = 64 * 1024
"""How much the drain asks for per read. Only an upper bound: `read1` returns whatever the
pipe already holds, so a worker that writes one line at a time is not waited on."""


class _StderrTail:
    """Drain one worker's stderr as it is produced, keeping only the tail.

    **A pipe nobody reads is a pipe that stops the writer.** An anonymous pipe's OS buffer
    is 4 KB on Windows; once a child has filled it the child blocks inside `write` and can
    never reach its own exit -- so `poll()` never returns a code, the read in `_reap` that
    would have released it never happens, and the worker is wedged for good. Reading at
    reap time is precisely the deadlock, not a fix for it.

    That was not a theoretical path. Three REST pollers logging one warning a second during
    an outage (~128 bytes a record, and the session worker has no logging handlers, so
    `logging.lastResort` sends them to stderr) fill 4 KB in about eleven seconds. The
    session then stopped observing the market, stopped heartbeating and stopped reading
    `control.json` -- so the stop button and the kill switch both reported success and did
    nothing, over a position the run still held.

    A thread per worker rather than one loop over all of them: selecting over pipes is not
    portable to Windows, and the alternative -- redirecting stderr to a file in the run
    directory -- would put whatever a worker prints on disk, which is the one thing spec 11
    forbids for a process that has been handed a credential.
    """

    def __init__(self, stream: IO[bytes]) -> None:
        self._blocks: deque[bytes] = deque()
        self._size = 0
        self._lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._pump, args=(stream,), name="perplab-worker-stderr", daemon=True
        )
        self._thread.start()

    def _pump(self, stream: IO[bytes]) -> None:
        read = getattr(stream, "read1", stream.read)
        try:
            while True:
                block = read(_STDERR_BLOCK_BYTES)
                if not block:
                    return
                with self._lock:
                    self._blocks.append(block)
                    self._size += len(block)
                    while (
                        len(self._blocks) > 1
                        and self._size - len(self._blocks[0]) >= STDERR_TAIL_BYTES
                    ):
                        self._size -= len(self._blocks.popleft())
        except (OSError, ValueError):  # pragma: no cover - the pipe was closed under us
            return
        finally:
            try:
                stream.close()
            except (OSError, ValueError):  # pragma: no cover
                pass

    def read(self, *, timeout: float = 2.0) -> bytes:
        """The tail, after waiting briefly for the drain to reach end-of-file.

        Called once the child has exited, when the drain is at most one block behind.
        Returning without the join would drop exactly the traceback the failure record
        exists to carry; the timeout is there because a wait that can hang is not an
        improvement on a pipe that can hang.
        """
        self._thread.join(timeout)
        with self._lock:
            return b"".join(self._blocks)[-STDERR_TAIL_BYTES:]


class RunStatus:
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"

    LOST = "lost"
    """The worker stopped checking in and this process has no handle to ask.

    A distinct status rather than `failed`, because the two claim different things and only
    one of them is known. `failed` says the run finished badly; `lost` says nobody can tell,
    and it is what an orphan after an API restart genuinely is.

    The distinction is load-bearing rather than cosmetic. Marking such a run `failed` made it
    terminal, and terminal unlocked `delete`, whose guard exists precisely to stop a run
    being removed while its worker is alive -- so a live worker's directory was deleted out
    from under it and its work destroyed. `lost` polls as finished so the UI stops spinning,
    and refuses deletion until someone has established what actually happened.
    """

    TERMINAL = frozenset({DONE, FAILED, CANCELLED, LOST})
    DELETABLE = frozenset({DONE, FAILED, CANCELLED})


class RunNotFound(LookupError):
    pass


def _now_ms() -> int:
    return int(time.time() * 1000)


def params_sha(params: Mapping[str, Any]) -> str:
    """Stable hash of a parameter combination, for the spec 8.5 trials counter.

    Sorted keys and a canonical separator, so `{"fast": 12, "slow": 26}` and
    `{"slow": 26, "fast": 12}` are one combination rather than two. Values are rendered with
    `str` because a param is already an int, a bool or an exact decimal *string* by the time
    it gets here (`strategy.params`), and `repr` on a `Decimal` would fold the same number
    written two ways into two different trials.
    """
    payload = {str(k): str(v) for k, v in sorted(params.items())}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class RunSummary:
    """One row of the Runs table (spec 10.3)."""

    id: int
    strategy_id: int
    strategy_name: str
    version_id: int
    version_no: int
    mode: str
    status: str
    label: str
    symbols: tuple[str, ...]
    timeframe: str
    start_ms: int | None
    end_ms: int | None
    seed: int | None
    engine_version: int | None
    fill_tier: str | None
    requested_fill_tier: str | None
    tier_reason: str | None
    """Spec 4.2's "never let a fill-model downgrade happen invisibly", in two columns.

    `fill_tier` alone says what the run executed at; it cannot say whether that was the
    choice or the ceiling. These carry the difference into the Runs table so the list can
    show a degradation badge without opening every run's manifest.
    """
    created_ms: int
    started_ms: int | None
    finished_ms: int | None
    archived_ms: int | None
    event_hash: str | None
    flags: tuple[str, ...]
    warnings: tuple[str, ...]
    error: str | None
    progress_bars: int
    progress_total: int
    net_pnl: str | None
    sharpe: float | None
    max_drawdown: float | None
    round_trips: int | None
    fills: int | None

    @property
    def archived(self) -> bool:
        return self.archived_ms is not None

    @property
    def tier_degraded(self) -> bool:
        """Whether the run executed below the tier it asked for.

        Derived rather than stored so it cannot disagree with the two columns it is derived
        from. `False` while either is unknown -- a queued run has not resolved a tier yet,
        and a pre-Phase-5 row never recorded a request.
        """
        if self.fill_tier is None or self.requested_fill_tier is None:
            return False
        return self.fill_tier != self.requested_fill_tier

    @property
    def duration_ms(self) -> int | None:
        if self.started_ms is None or self.finished_ms is None:
            return None
        return self.finished_ms - self.started_ms

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "version_id": self.version_id,
            "version_no": self.version_no,
            "mode": self.mode,
            "status": self.status,
            "label": self.label,
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "seed": self.seed,
            "engine_version": self.engine_version,
            "fill_tier": self.fill_tier,
            "requested_fill_tier": self.requested_fill_tier,
            "tier_reason": self.tier_reason,
            "tier_degraded": self.tier_degraded,
            "created_ms": self.created_ms,
            "started_ms": self.started_ms,
            "finished_ms": self.finished_ms,
            "archived_ms": self.archived_ms,
            "archived": self.archived,
            "duration_ms": self.duration_ms,
            "event_hash": self.event_hash,
            "flags": list(self.flags),
            "warnings": list(self.warnings),
            "error": self.error,
            "progress_bars": self.progress_bars,
            "progress_total": self.progress_total,
            "net_pnl": self.net_pnl,
            "sharpe": self.sharpe,
            "max_drawdown": self.max_drawdown,
            "round_trips": self.round_trips,
            "fills": self.fills,
        }


def _row_to_summary(row: Any) -> RunSummary:
    return RunSummary(
        id=int(row["id"]),
        strategy_id=int(row["strategy_id"]),
        strategy_name=str(row["strategy_name"] or ""),
        version_id=int(row["version_id"]),
        version_no=int(row["version_no"] or 0),
        mode=str(row["mode"]),
        status=str(row["status"]),
        label=str(row["label"] or ""),
        symbols=tuple(json.loads(row["symbols_json"] or "[]")),
        timeframe=str(row["timeframe"] or ""),
        start_ms=row["start_ms"],
        end_ms=row["end_ms"],
        seed=row["seed"],
        engine_version=row["engine_version"],
        fill_tier=row["fill_tier"],
        requested_fill_tier=row["requested_fill_tier"],
        tier_reason=row["tier_reason"],
        created_ms=int(row["created_ms"]),
        started_ms=row["started_ms"],
        finished_ms=row["finished_ms"],
        archived_ms=row["archived_ms"],
        event_hash=row["event_hash"],
        flags=tuple(json.loads(row["flags_json"] or "[]")),
        warnings=tuple(json.loads(row["warnings_json"] or "[]")),
        error=row["error"],
        progress_bars=int(row["progress_bars"] or 0),
        progress_total=int(row["progress_total"] or 0),
        net_pnl=row["net_pnl"],
        sharpe=row["sharpe"],
        max_drawdown=row["max_drawdown"],
        round_trips=row["round_trips"],
        fills=row["fills"],
    )


_SELECT = """
SELECT r.*, s.name AS strategy_name, v.version_no AS version_no
FROM runs r
JOIN strategies s ON s.id = r.strategy_id
JOIN strategy_versions v ON v.id = r.version_id
"""


class RunStore:
    """Runs, their processes and their artefacts. One instance per API process."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self._connection = db.connect(self.root)
        # The Runs table's landing query -- `list()` with no filters -- asks for "newest
        # unarchived first": `WHERE archived_ms IS NULL ORDER BY created_ms DESC, id DESC`.
        # Neither of the schema's indexes covers that shape (`runs_by_strategy` leads on a
        # column the default query does not constrain; `runs_by_status` leads on `status`),
        # so the query behind every render of the Runs page was a full table scan followed
        # by a temp B-tree sort -- over a table that only ever grows, because deleting a
        # run is a deliberate manual act. This index serves the filter and the sort
        # together: `IS NULL` is an equality probe on the first column, a reverse scan of
        # the second yields `created_ms DESC`, and `id DESC` comes free because `id`
        # aliases the rowid and index entries with equal keys are stored in rowid order.
        # Writes barely pay for it: the once-a-second heartbeat/progress UPDATEs touch
        # neither indexed column, and SQLite leaves an index alone when an UPDATE does not
        # change its columns.
        #
        # Created here on open rather than by a numbered migration in `store.db`,
        # deliberately. An index changes no data shape, `IF NOT EXISTS` makes the statement
        # idempotent across the four processes that open this store, and not bumping the
        # schema version means a database this build has opened still opens under the
        # previous build -- migrations are forward-only, so a v8 whose only content is a
        # read optimisation would refuse the database to a downgrade for nothing. The cost
        # is one table scan to build the index, paid once per existing database.
        #
        # What this deliberately does NOT fix: the growth itself. Nothing prunes `runs`,
        # `strategy_trials`, `kill_switch_trips`, or the per-run `events.jsonl` files.
        # Most of that is by design rather than by omission -- `strategy_trials` must never
        # shrink or spec 8.5's multiple-testing `N` understates the selection bias it
        # exists to report; `kill_switch_trips` is an incident log whose history is the
        # point; `events.jsonl` lives and dies with its run's directory -- but `runs`
        # retention genuinely is unbuilt: it remains the operator's `delete`, and an
        # automatic policy needs a design of its own (what age, which statuses, what about
        # runs that Lab jobs link to permanently). This index keeps the landing query
        # proportional to the LIMIT regardless of how far that history grows.
        with self._connection:
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS runs_recency"
                " ON runs(archived_ms, created_ms)"
            )
        self._processes: dict[int, subprocess.Popen[bytes]] = {}
        self._stderr: dict[int, _StderrTail] = {}
        self._lock = threading.RLock()
        """Serialises this store's connection and its two process dicts.

        `db.connect` passes `check_same_thread=False` "because FastAPI serves requests from a
        thread pool and the connection is guarded by a lock at the library level" -- true of
        `StrategyLibrary` and `LabStore`, and until this line **not** true here. Every route
        in `api.routers.runs` and `api.routers.sessions` is a plain `def`, so FastAPI runs
        each in its own worker thread, and `with self._connection:` from two of them is not
        two isolated transactions: one thread's commit publishes the other's half-written
        state.

        Two concrete failures this closes, both reachable the moment a second session is
        started while the first is running:

        - `create()` does its INSERT inside `with self._connection` and reads
          `cursor.lastrowid` *after* it. Two concurrent starts could hand both callers the
          same run id, and the second launch would then write its `spec.json` into the first
          run's directory.
        - `_reap()` deletes from `_processes` and `_stderr`, and it runs on every `get` and
          every `list` -- which the Live Monitor polls once a second per open session. Two
          polls landing together on the same just-exited worker turned a routine
          `GET /sessions/{id}/monitor` into a `KeyError` and a 500. `LabStore` documents
          having fixed exactly this.

        An `RLock` rather than a `Lock` because `_reap` is called from inside methods that
        already hold it.

        **Reads hold it too, not only writes.** On a shared connection a transaction is
        only isolated because every touch of the connection happens under this lock:
        `with self._connection:` opens the transaction at its first write and commits on
        exit, and a read from another thread that lands in between executes *inside* that
        open transaction -- it observes `delete()`'s scrub of
        `strategy_trials.best_run_id` before the row itself goes, or whichever half of any
        multi-statement write happens to be done. A dirty read never raises; it answers
        wrongly and moves on, which is why the read side of this discipline is pinned by
        `test_every_database_touch_holds_the_stores_lock` rather than left to review.
        """

    def close(self) -> None:
        self._connection.close()

    # ------------------------------------------------------------------------ paths

    def directory(self, run_id: int) -> Path:
        return self.root / RUNS_SUBDIR / str(run_id)

    def artefact(self, run_id: int, name: str) -> Path:
        return self.directory(run_id) / name

    # ----------------------------------------------------------------------- create

    def create(
        self,
        *,
        strategy_id: int,
        version_id: int,
        spec: Mapping[str, Any],
        label: str = "",
        mode: str = "backtest",
    ) -> int:
        """Write the row and the spec file. Does not start anything."""
        now = _now_ms()
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO runs (
                    strategy_id, version_id, mode, status, created_ms, heartbeat_ms, label,
                    symbols_json, timeframe, start_ms, end_ms, seed, engine_version,
                    params_sha, requested_fill_tier, progress_total
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    strategy_id,
                    version_id,
                    mode,
                    RunStatus.QUEUED,
                    now,
                    # Stamped at insert, not left null. `_reap` presumes a `queued` or
                    # `running` row with no recent heartbeat is dead, and a row created
                    # without one is dead on arrival -- which broke `create` used on its own,
                    # and left a race in `create`-then-`launch` that would have surfaced as
                    # runs failing at random under load.
                    now,
                    label,
                    json.dumps(list(spec["symbols"])),
                    spec["timeframe"],
                    int(spec["start_ms"]),
                    int(spec["end_ms"]),
                    int(spec["seed"]),
                    int(spec["engine_version"]),
                    params_sha(spec.get("params", {})),
                    # Recorded at creation, before the worker has resolved anything. A run
                    # that fails during resolution still says what it was asked for, which
                    # is the difference between "this range has no depth coverage" and a
                    # row with two empty tier columns.
                    spec.get("fill_tier"),
                ),
            )
            # Read inside the lock, which is **tidiness rather than a fix**, and the
            # distinction is worth recording because the obvious-sounding hazard here is not
            # real. `sqlite3.Cursor.lastrowid` is stamped on the cursor at `execute` time,
            # not read back from the connection when the attribute is touched -- three
            # INSERTs on one connection yield three cursors reporting 1, 2 and 3 whatever
            # order they are read in. So two threads sharing this connection cannot be
            # handed the same id by interleaving here, and a mutation moving this line back
            # outside the block is *equivalent*: it survives every test, correctly.
            #
            # It stays inside because the value is only meaningful if the transaction
            # commits, and having the read adjacent to the write it describes is one fewer
            # thing to reason about. `test_concurrent_run_creation_hands_out_distinct_ids`
            # pins the property that actually matters -- eight simultaneous creates produce
            # eight run ids and eight directories -- rather than this line's placement.
            run_id = int(cursor.lastrowid)
        directory = self.directory(run_id)
        directory.mkdir(parents=True, exist_ok=True)
        _write_json(directory / "spec.json", spec)
        return run_id

    def launch(self, run_id: int) -> None:
        """Spawn the worker for a queued run.

        `sys.executable -m perplab.engine.worker` rather than a thread. Spec 2.3's isolation
        argument is the reason and spec 11's "never runs strategy code" is the rule: a
        strategy with an accidental `while True` has to be killable, and a thread is not.
        """
        creationflags = 0
        if sys.platform == "win32":  # pragma: no branch - single-platform deployment
            # Without this a console window flashes up for every backtest, which on a
            # platform whose whole point is unattended runs is both startling and, on a
            # laptop, a way to steal focus mid-typing.
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        process = subprocess.Popen(
            [sys.executable, "-m", "perplab.engine.worker", str(self.root), str(run_id)],
            cwd=str(Path(__file__).resolve().parents[2]),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            creationflags=creationflags,
        )
        self._track(run_id, process)

    def launch_session(self, run_id: int, *, secrets: Mapping[str, Any] | None = None) -> None:
        """Spawn a paper session worker, handing it credentials **over stdin**.

        Spec 11 forbids API keys reaching disk, and the only existing API-to-worker channel
        is `spec.json`, which is a file. The environment is no better: `env=` is inherited by
        every grandchild and is readable from the process table on most systems. A pipe that
        is written once and closed is neither -- it exists only in the two processes' memory,
        and closing it is what tells the child there is nothing more coming.

        A session with no credentials is the ordinary case rather than a fallback: a paper
        session runs the engine's simulated transport against a live market feed and signs
        nothing, so nothing in `perplab.live.worker` consumes a key today. The caller decides
        whether to send one, and this method only carries what it is given.
        """
        creationflags = 0
        if sys.platform == "win32":  # pragma: no branch - single-platform deployment
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        process = subprocess.Popen(
            [sys.executable, "-m", "perplab.live.worker", str(self.root), str(run_id)],
            cwd=str(Path(__file__).resolve().parents[2]),
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            creationflags=creationflags,
        )
        # Tracked **before** the credential is written, unlike the order this had. The child
        # never closes its own stdin, so a write that fails means it is already dead -- and
        # the failure propagated out of here past the handle and past the pid update, so the
        # caller got a 500 reading `OSError: [Errno 22] Invalid argument`, the `Popen` was
        # dropped on the floor, and the row sat at `queued` for three minutes before the
        # stale sweep could call it `lost`. Tracked first, `_reap` reads the child's exit
        # code and its stderr and records what actually went wrong.
        self._track(run_id, process)
        if process.stdin is not None:
            try:
                process.stdin.write(
                    (json.dumps(dict(secrets or {})) + "\n").encode("utf-8")
                )
                process.stdin.flush()
            except OSError:
                # A broken pipe here is the child having exited before it read; there is
                # nobody left to hand anything to. Swallowed rather than raised because the
                # diagnosis the operator needs is the child's own stderr, which `_reap` will
                # attach to the row, and not this end's errno.
                pass
            finally:
                # Closed unconditionally, and its own failure ignored for the same reason:
                # `close()` re-raises a pending flush. The child blocks on a single
                # `readline`, so an un-closed pipe on any error path would leave the session
                # waiting for credentials that are never coming, looking alive and idle.
                try:
                    process.stdin.close()
                except OSError:
                    pass

    def _track(self, run_id: int, process: subprocess.Popen[bytes]) -> None:
        """Register a spawned worker: its handle, its stderr drain, and its pid on the row.

        One place for both launchers, because they differed and the difference was the
        defect fixed above. The stderr drain starts here rather than at reap time -- see
        `_StderrTail` for the deadlock that reading-at-exit produces.
        """
        with self._lock:
            if process.stderr is not None:
                self._stderr[run_id] = _StderrTail(process.stderr)
            self._processes[run_id] = process
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE runs SET pid = ?, heartbeat_ms = ? WHERE id = ?",
                (process.pid, _now_ms(), run_id),
            )

    def link_shadow(self, shadow_run_id: int, paper_run_id: int) -> None:
        """Record that one run is the replay of another (spec 6.7.1)."""
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE runs SET shadow_of_run_id = ? WHERE id = ?",
                (paper_run_id, shadow_run_id),
            )

    def request_stop(self, run_id: int, *, flatten: bool, reason: str) -> None:
        """Ask a running session to stop itself, via its control file.

        Not `terminate()`. On Windows that is `TerminateProcess`: no `finally`, no `atexit`,
        so a session killed that way never gets to cancel its resting orders at the exchange
        -- which is items 1 and 2 of spec 7.3, the two things the kill switch most has to do.
        The file is written atomically so a session polling it can never read half a command.
        """
        path = self.directory(run_id) / "control.json"
        payload = {"stop": True, "flatten": bool(flatten), "reason": reason,
                   "requested_ms": _now_ms()}
        _write_json(path, payload)

    # ----------------------------------------------------------------------- worker

    def mark_running(self, run_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE runs SET status = ?, started_ms = ?, heartbeat_ms = ? WHERE id = ?",
                (RunStatus.RUNNING, _now_ms(), _now_ms(), run_id),
            )

    def heartbeat(self, run_id: int) -> None:
        """Say "still alive" without claiming any progress.

        A backtest's only heartbeat is `progress`, which is driven by event volume. That is
        fine for a replay, which always has more events; it is wrong for a live session,
        which on a quiet market can legitimately go minutes without one. `_reap` presumes a
        row silent for `STALE_HEARTBEAT_MS` is dead and marks it `lost` -- and `lost` is
        terminal, so `cancel` returns early and the UI hides the stop control while the
        session carries on trading, unreachable.
        """
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE runs SET heartbeat_ms = ? WHERE id = ?", (_now_ms(), run_id)
            )

    def checkpoint(
        self,
        run_id: int,
        *,
        net_pnl: str,
        sharpe: float | None,
        max_drawdown: float | None,
        round_trips: int,
        fills: int,
        bars: int,
        fill_tier: str | None = None,
        flags: Sequence[str] = (),
        warnings: Sequence[str] = (),
    ) -> None:
        """Publish a running session's headline figures without ending it.

        `complete` is the same write plus the terminal status, and a live session cannot use
        it: the Runs table would show the session as finished from its first minute. This
        deliberately never touches `status` or `finished_ms`.
        """
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE runs SET heartbeat_ms = ?, net_pnl = ?, sharpe = ?,
                    max_drawdown = ?, round_trips = ?, fills = ?, progress_bars = ?,
                    fill_tier = coalesce(?, fill_tier), flags_json = ?, warnings_json = ?
                WHERE id = ? AND status = ?
                """,
                (
                    _now_ms(),
                    net_pnl,
                    sharpe,
                    max_drawdown,
                    round_trips,
                    fills,
                    bars,
                    fill_tier,
                    json.dumps(list(flags)),
                    json.dumps(list(warnings)),
                    run_id,
                    RunStatus.RUNNING,
                ),
            )

    def progress(self, run_id: int, bars: int, total: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE runs SET progress_bars = ?, progress_total = ?, heartbeat_ms = ?
                WHERE id = ?
                """,
                (bars, total, _now_ms(), run_id),
            )

    def complete(
        self,
        run_id: int,
        *,
        event_hash: str,
        fill_tier: str,
        tier_reason: str | None,
        flags: Sequence[str],
        warnings: Sequence[str],
        net_pnl: str,
        sharpe: float | None,
        max_drawdown: float | None,
        round_trips: int,
        fills: int,
        bars: int,
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE runs SET status = ?, finished_ms = ?, heartbeat_ms = ?,
                    event_hash = ?, fill_tier = ?, tier_reason = ?, flags_json = ?,
                    warnings_json = ?,
                    net_pnl = ?, sharpe = ?, max_drawdown = ?, round_trips = ?,
                    fills = ?, progress_bars = ?, progress_total = ?, error = NULL
                WHERE id = ?
                """,
                (
                    RunStatus.DONE,
                    _now_ms(),
                    _now_ms(),
                    event_hash,
                    fill_tier,
                    tier_reason,
                    json.dumps(list(flags)),
                    json.dumps(list(warnings)),
                    net_pnl,
                    sharpe,
                    max_drawdown,
                    round_trips,
                    fills,
                    bars,
                    bars,
                    run_id,
                ),
            )

    def fail(self, run_id: int, error: str, *, status: str = RunStatus.FAILED) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE runs SET status = ?, finished_ms = ?, heartbeat_ms = ?, error = ?
                WHERE id = ?
                """,
                (status, _now_ms(), _now_ms(), error[:4000], run_id),
            )

    def record_trial(
        self,
        *,
        strategy_id: int,
        params: Mapping[str, Any],
        sharpe: float | None,
        run_id: int | None,
    ) -> None:
        """Bump the spec 8.5 counter for this parameter combination.

        `run_id` is `None` for evaluations with no run row -- a walk-forward's grid
        points execute in a pool and never touch the runs table. If such an evaluation
        becomes the combination's best, `best_run_id` goes NULL with it: pointing the
        "best run" link at an older, inferior run would be a wrong answer wearing a
        working link.

        Recorded on *completion*, not on submission. A run that failed to start evaluated
        nothing, and counting it would inflate `N` in the multiple-testing correction -- the
        one number this counter exists to keep honest.
        """
        sha = params_sha(params)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO strategy_trials
                    (strategy_id, params_sha, params_json, evaluations, best_sharpe, best_run_id)
                VALUES (?, ?, ?, 1, ?, ?)
                ON CONFLICT(strategy_id, params_sha) DO UPDATE SET
                    evaluations = evaluations + 1,
                    best_sharpe = CASE
                        WHEN excluded.best_sharpe IS NULL THEN best_sharpe
                        WHEN best_sharpe IS NULL OR excluded.best_sharpe > best_sharpe
                            THEN excluded.best_sharpe
                        ELSE best_sharpe END,
                    best_run_id = CASE
                        WHEN excluded.best_sharpe IS NOT NULL
                             AND (best_sharpe IS NULL OR excluded.best_sharpe > best_sharpe)
                            THEN excluded.best_run_id
                        ELSE best_run_id END
                """,
                (
                    strategy_id,
                    sha,
                    json.dumps({str(k): str(v) for k, v in sorted(params.items())}),
                    sharpe,
                    run_id,
                ),
            )

    def trials(self, strategy_id: int) -> dict[str, Any]:
        """Spec 8.5's counter, beside the best Sharpe it produced.

        `combinations` is the statistically meaningful `N`: the bias in a maximum over `N`
        draws is about `sqrt(2 ln N)` standard deviations, and re-running one combination is
        not a new draw. `evaluations` is reported too because it answers the different
        question of how much compute a strategy has consumed.
        """
        # Under the lock like every other touch of the connection: an unlocked read here
        # could land inside a concurrent `record_trial` transaction and report a counter
        # mid-upsert. See `_lock`'s docstring.
        with self._lock:
            row = self._connection.execute(
                """
                SELECT count(*) AS combinations,
                       coalesce(sum(evaluations), 0) AS evaluations,
                       max(best_sharpe) AS best_sharpe
                FROM strategy_trials WHERE strategy_id = ?
                """,
                (strategy_id,),
            ).fetchone()
        combinations = int(row["combinations"] or 0)
        return {
            "combinations": combinations,
            "evaluations": int(row["evaluations"] or 0),
            "best_sharpe": row["best_sharpe"],
            # The expected upward bias, in standard deviations, of the best result from N
            # independent trials under the null. Reported rather than left as an exercise:
            # spec 8.5's whole argument is that "after 500 grid combinations, a Sharpe of
            # 1.8 is unremarkable noise", and a bare count does not make that visible.
            "selection_bias_sd": _selection_bias(combinations),
        }

    # ------------------------------------------------------------------------- reads
    #
    # Every read takes the lock the writers take -- see `_lock`'s docstring for why an
    # unlocked read on a shared connection is a dirty read, not merely a stale one. The
    # SQL readers call `_reap_locked` inside the same hold rather than `_reap` before it,
    # so "reap, then read" is one critical section: the summary handed back reflects the
    # reap that just ran, with no room for another thread's transaction in between.
    # `read_json`/`read_events`/`iter_events` are exempt because they touch only the run
    # directory, never the connection -- and holding the lock while streaming a
    # million-line event log would stall every write for the duration, which is the
    # opposite trade to the one the lock exists to make.

    def get(self, run_id: int) -> RunSummary:
        with self._lock:
            self._reap_locked()
            row = self._connection.execute(
                _SELECT + " WHERE r.id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise RunNotFound(f"no run with id {run_id}")
        return _row_to_summary(row)

    def list(
        self,
        *,
        strategy_id: int | None = None,
        status: str | None = None,
        include_archived: bool = False,
        limit: int = 200,
    ) -> list[RunSummary]:
        clauses: list[str] = []
        params: list[Any] = []
        if strategy_id is not None:
            clauses.append("r.strategy_id = ?")
            params.append(strategy_id)
        if status is not None:
            clauses.append("r.status = ?")
            params.append(status)
        if not include_archived:
            clauses.append("r.archived_ms IS NULL")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(int(limit))
        # The default shape of this query -- archived filter only, ordered by recency --
        # is served by `runs_recency`; see the index's rationale in `__init__`.
        with self._lock:
            self._reap_locked()
            rows = self._connection.execute(
                _SELECT + where + " ORDER BY r.created_ms DESC, r.id DESC LIMIT ?", params
            ).fetchall()
        return [_row_to_summary(row) for row in rows]

    def active(self) -> list[RunSummary]:
        """Every run not yet in a terminal state -- **no window, no archive filter**.

        This method exists because `list(limit=500)` was quietly load-bearing in seven
        safety paths: the symbol-claim pruner, the kill switch's stop sweep, disconnect,
        the idle-key expiry sweep and the startup orphan scan all walked "the newest 500
        runs" as a stand-in for "every run that could still be trading". A 48-hour live
        session pushed past position 500 by one parameter sweep then fell out of all of
        them at once -- its symbol claim was destructively pruned as stale (so a second
        session could trade the same symbol into one merged venue position), and the kill
        switch's sweep skipped it. A safety sweep must enumerate what is *alive*, and
        alive is a property of status, not of recency.

        No limit, deliberately: the set of non-terminal runs is bounded by how many
        workers a machine can host, not by history, so the scan is small forever. Archived
        rows are included for the same reason -- archiving is a display choice, and a row
        that is hidden from the Runs list can still own a process with a position.
        """
        placeholders = ", ".join("?" for _ in RunStatus.TERMINAL)
        with self._lock:
            self._reap_locked()
            rows = self._connection.execute(
                _SELECT + f" WHERE r.status NOT IN ({placeholders})"
                " ORDER BY r.created_ms DESC, r.id DESC",
                list(RunStatus.TERMINAL),
            ).fetchall()
        return [_row_to_summary(row) for row in rows]

    def read_json(self, run_id: int, name: str) -> Any:
        path = self.artefact(run_id, name)
        if not path.exists():
            raise RunNotFound(
                f"run {run_id} has no {name}; it may not have finished, or its directory "
                f"was removed"
            )
        return json.loads(path.read_text(encoding="utf-8"))

    def read_events(
        self,
        run_id: int,
        *,
        offset: int = 0,
        limit: int = 500,
        kind: str | None = None,
        search: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """A page of the event log, plus the total after filtering.

        Streamed line by line rather than parsed whole. A year-long run's log can hold a
        million entries; loading it to serve fifty of them would make the viewer's first
        page the slowest request the server ever answers.

        `search` is a case-insensitive substring match against the **raw line**, which is
        deliberately coarser than matching parsed fields: it finds an order id in a payload,
        a symbol, a reason string, or a number, without the caller having to know which key
        holds it. Filtering the rendered page instead would search fifty rows out of a
        million and report "no matches" for something plainly in the log.
        """
        path = self.artefact(run_id, "events.jsonl")
        if not path.exists():
            raise RunNotFound(f"run {run_id} has no event log")
        needle = search.lower() if search else None
        page: list[dict[str, Any]] = []
        total = 0
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                if needle is not None and needle not in line.lower():
                    continue
                entry = None
                if kind is not None:
                    # Cheap pre-filter on the raw text before paying for a JSON parse; the
                    # engine writes `kind` from a fixed vocabulary so a false *negative* is
                    # impossible. A false positive is possible -- a strategy could log a
                    # field whose value is that literal -- so the survivors are parsed and
                    # re-checked, and the parse is not wasted because the page needs it.
                    if f'"kind":"{kind}"' not in line:
                        continue
                    entry = json.loads(line)
                    if entry.get("kind") != kind:
                        continue
                if offset <= total < offset + limit:
                    page.append(entry if entry is not None else json.loads(line))
                total += 1
        return page, total

    def iter_events(self, run_id: int) -> Iterator[dict[str, Any]]:
        path = self.artefact(run_id, "events.jsonl")
        if not path.exists():
            raise RunNotFound(f"run {run_id} has no event log")
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)

    # ---------------------------------------------------------------------- lifecycle

    def cancel(self, run_id: int) -> RunSummary:
        summary = self.get(run_id)
        if summary.status in RunStatus.TERMINAL:
            return summary
        with self._lock:
            process = self._processes.pop(run_id, None)
            self._stderr.pop(run_id, None)
        if process is not None and process.poll() is None:
            process.terminate()
            self.fail(run_id, "cancelled by the user", status=RunStatus.CANCELLED)
            return self.get(run_id)

        # No handle. The recorded pid is **not** identity: process ids are recycled, and on
        # a machine that has been up for weeks the id stored against yesterday's run may
        # belong to anything. Signalling it was demonstrated to kill an unrelated program.
        # The row is marked cancelled and the truth is stated instead.
        self.fail(
            run_id,
            "cancelled by the user, but this server did not launch the worker and cannot "
            "safely signal it: the recorded process id may since have been reused by an "
            "unrelated program. If a worker is still running, stop it by hand.",
            status=RunStatus.CANCELLED,
        )
        return self.get(run_id)

    def archive(self, run_id: int, *, archived: bool = True) -> RunSummary:
        summary = self.get(run_id)
        # The same guard `delete` has, for the same reason with a sharper edge: archiving
        # hides the row from every default listing, and a *running* session hidden from
        # the operator is a process with a live position that no view shows and no glance
        # can find. The safety sweeps enumerate by status (`active()`) so they would still
        # reach it -- but the human watching the platform would not, and spec 10's whole
        # premise is that the operator can see what is trading. Un-archiving is always
        # allowed; there is no state in which revealing a row is dangerous.
        if archived and summary.status not in RunStatus.TERMINAL:
            raise ValueError(
                f"run {run_id} is {summary.status}; a run can be archived only once it "
                f"has finished. Archiving a running session would hide a live position "
                f"from every list the operator looks at -- stop it first."
            )
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE runs SET archived_ms = ? WHERE id = ?",
                (_now_ms() if archived else None, run_id),
            )
        return self.get(run_id)

    def delete(self, run_id: int) -> None:
        """Remove the row and the artefacts.

        The directory goes with the row. Leaving it would accumulate gigabytes of event logs
        belonging to runs nothing references, and `userdata/runs/` is not somewhere anyone
        thinks to look.
        """
        summary = self.get(run_id)
        if summary.status not in RunStatus.DELETABLE:
            raise ValueError(
                f"run {run_id} is {summary.status}; cancel it before deleting, or its "
                f"worker will keep writing into a directory that no longer has a row"
            )
        # Spec 9: Lab artefacts are "linked back to the source run permanently", and a
        # link to a deleted run is not a link. The guard is here rather than a foreign
        # key so the refusal can say what to do about it; deleting the jobs first is an
        # explicit act, which is the only way analysis should ever be destroyed.
        #
        # Read under the lock like every touch of the connection (`_lock`'s docstring).
        # Not under the *same* hold as the DELETE below, deliberately: that would pin the
        # lock across `rmtree` of a directory that can run to gigabytes, stalling every
        # once-a-second monitor poll for the duration. The check-then-act window that
        # leaves is against a Lab job created *after* this count, which is the operator
        # racing their own delete button -- not the silent cross-thread corruption the
        # lock exists to prevent.
        with self._lock:
            linked = self._connection.execute(
                "SELECT count(*) AS n FROM lab_jobs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if linked is not None and int(linked["n"]):
            raise ValueError(
                f"run {run_id} has {int(linked['n'])} Lab artefact(s) linked to it "
                f"(walk-forwards, Monte Carlo, regime analyses). Delete those Lab jobs "
                f"first if you really mean to destroy the analysis with the run."
            )
        # **Artefacts first, row last.** The other order left a failure removing the
        # directory with no row referencing it -- exactly the orphaned-gigabytes state the
        # deletion exists to prevent, arrived at by the deletion itself. `rmtree` also
        # handles a nested directory, which `unlink` could not and which raised a 500.
        directory = self.directory(run_id)
        if directory.is_dir():
            shutil.rmtree(directory)
        with self._lock, self._connection:
            # `strategy_trials.best_run_id` is declared without a foreign key, so nothing
            # clears it for us and the spec 8.5 panel's "best run" link pointed at a run that
            # no longer existed. Cleared in the same transaction as the row, so the two
            # cannot disagree even if the delete fails half-way.
            self._connection.execute(
                "UPDATE strategy_trials SET best_run_id = NULL WHERE best_run_id = ?",
                (run_id,),
            )
            self._connection.execute("DELETE FROM runs WHERE id = ?", (run_id,))

    # --------------------------------------------------------------------- internals

    def _reap(self) -> None:
        """Close the books on workers that died without saying so.

        Two ways that happens, and they are not equally knowable.

        A worker **this process spawned** can be polled directly: an exit code with no
        terminal status means it died between its last write and its exit, and `failed` is a
        fact. A worker **orphaned by an API restart** cannot be polled at all -- there is no
        handle, and the recorded pid is not identity, because pids are recycled. Silence is
        then the only evidence, and silence supports `lost`, not `failed`.

        The distinction used to be collapsed into `failed`, which unlocked `delete` and
        destroyed the directory of a worker that was still running. `lost` reads as finished
        so the UI stops spinning, and refuses deletion.

        **Held under the store's lock.** This runs on every `get` and every `list`, which the
        Live Monitor polls once a second per open session, and it mutates two dicts. Two
        polls landing together on the same just-exited worker had one thread `del` an entry
        the other was about to `del` -- a `KeyError` out of a routine GET, presented as a
        500. `pop` with a default rather than `del` for the same reason, so the race is
        closed twice: by the lock, and by an operation that cannot fail if the lock is ever
        removed.

        The SQL readers no longer come through this wrapper: they call `_reap_locked`
        inside their own hold, so the reap and the read they are about to do form one
        critical section. This entry point remains for any caller that is not already
        holding the lock.
        """
        with self._lock:
            self._reap_locked()

    def _reap_locked(self) -> None:
        for run_id, process in list(self._processes.items()):
            code = process.poll()
            if code is None:
                continue
            self._processes.pop(run_id, None)
            # Popped whichever way this goes, so the drain's buffer is not kept alive by a
            # run nobody is going to ask about again.
            tail = self._stderr.pop(run_id, None)
            row = self._connection.execute(
                "SELECT status FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if row is None or str(row["status"]) in RunStatus.TERMINAL:
                continue
            stderr = tail.read() if tail is not None else b""
            detail = stderr.decode("utf-8", "replace").strip()[-2000:]
            self.fail(
                run_id,
                f"the run worker exited with code {code} without recording a result"
                + (f":\n{detail}" if detail else ""),
            )

        cutoff = _now_ms() - STALE_HEARTBEAT_MS
        stale = self._connection.execute(
            """
            SELECT id FROM runs
            WHERE status IN (?, ?) AND coalesce(heartbeat_ms, created_ms) < ?
            """,
            (RunStatus.QUEUED, RunStatus.RUNNING, cutoff),
        ).fetchall()
        for row in stale:
            run_id = int(row["id"])
            with self._lock:
                tracked = run_id in self._processes
            if tracked:
                continue
            self.fail(
                run_id,
                f"no heartbeat for over {STALE_HEARTBEAT_MS // 1000}s and no handle to "
                f"this worker, so its fate is unknown rather than known-bad. The machine "
                f"may have slept, been powered off, or run out of memory -- or the worker "
                f"may still be running under an API server that has since restarted. Check "
                f"before deleting: the artefacts are still in this run's directory.",
                status=RunStatus.LOST,
            )


def _selection_bias(trials: int) -> float | None:
    """`sqrt(2 ln N)` -- spec 8.5's rule of thumb, in standard deviations.

    `None` below two trials, where the expression is zero or undefined and reporting a
    number would imply a correction that does not apply.
    """
    if trials < 2:
        return None
    import math

    return math.sqrt(2.0 * math.log(trials))


def _write_json(path: Path, payload: Any) -> None:
    """Serialise and publish atomically.

    Same `.tmp` + `os.replace` discipline as the Parquet writer and the dataset manifest. A
    half-written `metrics.json` parses far enough to look authoritative, and the reader has
    no way to tell it apart from a run that genuinely produced those numbers.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.tmp"
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)
