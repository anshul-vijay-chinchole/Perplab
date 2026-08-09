"""Run persistence: the metadata row, the trials counter, and the artefacts.

The worker subprocess is exercised end-to-end in `tests/integration/test_run_worker.py`.
What is here is everything that has to be right *around* it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from perplab.store import db
from perplab.store import runs as runs_module
from perplab.store.runs import (
    STALE_HEARTBEAT_MS,
    RunNotFound,
    RunStatus,
    RunStore,
    params_sha,
)

SPEC = {
    "symbols": ["BTCUSDT"],
    "timeframe": "15m",
    "start_ms": 1_000_000,
    "end_ms": 2_000_000,
    "seed": 7,
    "engine_version": 4,
    "params": {"fast": 12, "slow": 26},
}


def seed_strategy(root: Path) -> tuple[int, int]:
    """A strategy and a version row, so the runs table's foreign keys resolve."""
    connection = db.connect(root)
    with connection:
        strategy = connection.execute(
            "INSERT INTO strategies (name, created_ms, updated_ms) VALUES ('S', 0, 0)"
        ).lastrowid
        version = connection.execute(
            """
            INSERT INTO strategy_versions
                (strategy_id, version_no, code, code_sha256, created_ms, valid)
            VALUES (?, 1, 'x', 'abc', 0, 1)
            """,
            (strategy,),
        ).lastrowid
    connection.close()
    return int(strategy), int(version)


@pytest.fixture()
def store(tmp_path: Path):
    strategy_id, version_id = seed_strategy(tmp_path)
    handle = RunStore(tmp_path)
    yield handle, strategy_id, version_id
    handle.close()


# ------------------------------------------------------------------------- migration


def test_a_v1_database_migrates_in_place(tmp_path: Path) -> None:
    """Phase 3 shipped schema v1 and real databases exist on disk carrying it.

    The migration adds columns by `ALTER`, so a database created by v1 and one created
    fresh must end up with the same shape -- otherwise a query written against one silently
    fails against the other.
    """
    path = tmp_path / "perplab.db"
    import sqlite3

    legacy = sqlite3.connect(path)
    legacy.executescript(db.SCHEMA)
    legacy.execute("INSERT INTO schema_version (version) VALUES (1)")
    legacy.commit()
    legacy.close()

    connection = db.connect(tmp_path)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
    for name, _ in (*db.RUN_COLUMNS_V2, *db.RUN_COLUMNS_V3):
        assert name in columns
    assert (
        connection.execute("SELECT version FROM schema_version").fetchone()[0]
        == db.SCHEMA_VERSION
    )
    connection.close()


def test_the_migration_is_idempotent(tmp_path: Path) -> None:
    """A crash between the ALTER and the version bump must not leave a database that
    neither migrates nor works."""
    db.connect(tmp_path).close()
    connection = db.connect(tmp_path)
    connection.execute("UPDATE schema_version SET version = 1")
    connection.commit()
    connection.close()
    db.connect(tmp_path).close()  # must not raise


def test_a_newer_schema_is_refused_rather_than_partially_read(tmp_path: Path) -> None:
    db.connect(tmp_path).close()
    connection = db.connect(tmp_path)
    connection.execute("UPDATE schema_version SET version = 99")
    connection.commit()
    connection.close()
    with pytest.raises(db.SchemaTooNew):
        db.connect(tmp_path)


# ---------------------------------------------------------------------------- runs


def test_create_writes_the_row_and_the_spec_file(store) -> None:
    handle, strategy_id, version_id = store
    run_id = handle.create(
        strategy_id=strategy_id, version_id=version_id, spec=SPEC, label="first"
    )
    summary = handle.get(run_id)
    assert summary.status == RunStatus.QUEUED
    assert summary.label == "first"
    assert summary.symbols == ("BTCUSDT",)
    assert summary.seed == 7
    stored = json.loads(handle.artefact(run_id, "spec.json").read_text())
    assert stored["params"] == {"fast": 12, "slow": 26}


def test_listing_hides_archived_runs_unless_asked(store) -> None:
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    # Finished first: archive refuses a run that could still be trading (see below).
    handle.fail(run_id, "done for the purposes of this test")
    handle.archive(run_id)
    assert handle.list() == []
    assert len(handle.list(include_archived=True)) == 1
    handle.archive(run_id, archived=False)
    assert len(handle.list()) == 1


def test_a_run_that_is_still_going_cannot_be_archived(store) -> None:
    """C5's second route: archiving hid a *running* session from every default listing.

    A hidden row is a live position no view shows -- the operator's glance says nothing
    is trading while a worker holds the symbol. Un-archiving is always allowed; there is
    no state in which revealing a row is dangerous.
    """
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.mark_running(run_id)
    with pytest.raises(ValueError, match="archived only once it has finished"):
        handle.archive(run_id)
    # The refusal is about hiding, not about the flag: clearing it is always legal.
    handle.archive(run_id, archived=False)


def test_active_enumerates_by_status_with_no_window_and_no_archive_filter(store) -> None:
    """C5's first route: every safety sweep now walks `active()`, so `active()` must be
    a property of status alone. The one live run here is older than 500 newer terminal
    rows and would have fallen out of `list(limit=500)` -- the exact shape that let a
    symbol claim be pruned out from under a running session.
    """
    handle, strategy_id, version_id = store
    old_live = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.mark_running(old_live)
    for _ in range(500):
        newer = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
        handle.fail(newer, "sweep filler")

    assert old_live not in [s.id for s in handle.list(limit=500)], (
        "the filler must actually push the live run past the window, or this test "
        "proves nothing"
    )
    assert [s.id for s in handle.active()] == [old_live]


def test_a_run_that_is_still_going_cannot_be_deleted(store) -> None:
    """Its worker would keep writing into a directory that no longer has a row."""
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.mark_running(run_id)
    with pytest.raises(ValueError, match="cancel it before deleting"):
        handle.delete(run_id)


def test_delete_removes_the_artefacts_as_well_as_the_row(store) -> None:
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    directory = handle.directory(run_id)
    assert directory.is_dir()
    handle.fail(run_id, "nope")
    handle.delete(run_id)
    assert not directory.exists()
    with pytest.raises(RunNotFound):
        handle.get(run_id)


def test_a_worker_that_vanishes_is_marked_lost_rather_than_failed(store) -> None:
    """A hard kill never lets the worker record its own failure, so the row outlives the
    process. Silence supports `lost` -- nobody can tell -- and not `failed`, which would
    claim knowledge this process does not have."""
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.mark_running(run_id)
    with handle._connection:
        handle._connection.execute(
            "UPDATE runs SET heartbeat_ms = ? WHERE id = ?",
            (0, run_id),
        )
    summary = handle.get(run_id)  # triggers `_reap`
    assert summary.status == RunStatus.LOST
    assert "heartbeat" in (summary.error or "")
    assert str(STALE_HEARTBEAT_MS // 1000) in (summary.error or "")
    # And it stops the UI spinning without unlocking destruction.
    assert summary.status in RunStatus.TERMINAL
    assert summary.status not in RunStatus.DELETABLE


def test_a_lost_run_cannot_be_deleted(store) -> None:
    """`lost` means the worker may still be running. Deleting its directory destroys work
    that is in progress -- which is exactly what `failed` used to permit."""
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.mark_running(run_id)
    with handle._connection:
        handle._connection.execute(
            "UPDATE runs SET heartbeat_ms = 0 WHERE id = ?", (run_id,)
        )
    assert handle.get(run_id).status == RunStatus.LOST
    with pytest.raises(ValueError, match="cancel it before deleting"):
        handle.delete(run_id)
    assert handle.artefact(run_id, "spec.json").exists()


def test_cancelling_a_run_this_process_did_not_launch_signals_nothing(store) -> None:
    """The recorded pid is not identity: ids are recycled, and signalling one was shown to
    kill an unrelated program. The row is cancelled and the limitation is stated."""
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.mark_running(run_id)
    with handle._connection:
        # A pid this store never launched -- our own, which must survive the call.
        handle._connection.execute(
            "UPDATE runs SET pid = ? WHERE id = ?", (os.getpid(), run_id)
        )
    summary = handle.cancel(run_id)
    assert summary.status == RunStatus.CANCELLED
    assert "did not launch" in (summary.error or "")
    assert os.getpid()  # still here


def test_a_fresh_run_is_not_reaped_for_want_of_a_heartbeat(store) -> None:
    """A run is `queued` between `create` and `launch`, and that gap is legitimate.

    The heartbeat is stamped at insert for exactly this: a null one read as "no heartbeat
    ever" made every created-but-not-yet-launched run fail the instant it was read back.
    """
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    assert handle.get(run_id).status == RunStatus.QUEUED
    handle.mark_running(run_id)
    assert handle.get(run_id).status == RunStatus.RUNNING


def test_a_row_with_no_heartbeat_at_all_is_judged_by_when_it_was_created(store) -> None:
    """Belt and braces for a row that predates the heartbeat column, or one written by a
    build that did not stamp it. Presuming it dead outright would fail a run that had only
    just started; judging it by `created_ms` is the honest fallback."""
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    with handle._connection:
        handle._connection.execute(
            "UPDATE runs SET heartbeat_ms = NULL WHERE id = ?", (run_id,)
        )
    assert handle.get(run_id).status == RunStatus.QUEUED


def test_progress_moves_and_completion_records_the_headline_figures(store) -> None:
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.mark_running(run_id)
    handle.progress(run_id, 500, 2000)
    assert handle.get(run_id).progress_bars == 500

    handle.complete(
        run_id,
        event_hash="deadbeef",
        fill_tier="BAR_CLOSE",
        tier_reason=None,
        flags=["LOW_FIDELITY"],
        warnings=["careful"],
        net_pnl="-12.50",
        sharpe=-0.4,
        max_drawdown=-0.2,
        round_trips=3,
        fills=6,
        bars=2000,
    )
    summary = handle.get(run_id)
    assert summary.status == RunStatus.DONE
    assert summary.event_hash == "deadbeef"
    assert summary.net_pnl == "-12.50"
    assert summary.flags == ("LOW_FIDELITY",)
    # Progress is snapped to complete, so a finished run never renders at 97%.
    assert summary.progress_bars == summary.progress_total == 2000


# -------------------------------------------------------------------------- trials


def test_the_trials_counter_counts_combinations_not_evaluations(store) -> None:
    """Spec 8.5's `N` is the number of *distinct* parameter combinations tried.

    Re-running one combination is not a new draw from the null, so counting evaluations
    would inflate `N` and overstate the selection-bias correction -- the one number this
    counter exists to keep honest.
    """
    handle, strategy_id, _ = store
    for _ in range(3):
        handle.record_trial(strategy_id=strategy_id, params={"fast": 12}, sharpe=1.0, run_id=1)
    handle.record_trial(strategy_id=strategy_id, params={"fast": 20}, sharpe=2.0, run_id=2)

    trials = handle.trials(strategy_id)
    assert trials["combinations"] == 2
    assert trials["evaluations"] == 4
    assert trials["best_sharpe"] == 2.0


def test_the_trials_counter_ignores_key_order(store) -> None:
    handle, strategy_id, _ = store
    handle.record_trial(strategy_id=strategy_id, params={"a": 1, "b": 2}, sharpe=1.0, run_id=1)
    handle.record_trial(strategy_id=strategy_id, params={"b": 2, "a": 1}, sharpe=1.0, run_id=2)
    assert handle.trials(strategy_id)["combinations"] == 1
    assert params_sha({"a": 1, "b": 2}) == params_sha({"b": 2, "a": 1})


def test_a_worse_result_does_not_overwrite_the_best(store) -> None:
    handle, strategy_id, _ = store
    handle.record_trial(strategy_id=strategy_id, params={"a": 1}, sharpe=2.0, run_id=1)
    handle.record_trial(strategy_id=strategy_id, params={"a": 1}, sharpe=0.5, run_id=2)
    assert handle.trials(strategy_id)["best_sharpe"] == 2.0


def test_a_run_with_no_sharpe_does_not_erase_the_best(store) -> None:
    """A run too short for a standard deviation reports `None`, and `None` is not worse."""
    handle, strategy_id, _ = store
    handle.record_trial(strategy_id=strategy_id, params={"a": 1}, sharpe=2.0, run_id=1)
    handle.record_trial(strategy_id=strategy_id, params={"a": 1}, sharpe=None, run_id=2)
    assert handle.trials(strategy_id)["best_sharpe"] == 2.0


def test_the_selection_bias_is_reported_alongside_the_count(store) -> None:
    """Spec 8.5: after 500 combinations, a Sharpe of 1.8 is unremarkable noise. A bare count
    does not make that visible; sqrt(2 ln N) does."""
    import math

    handle, strategy_id, _ = store
    assert handle.trials(strategy_id)["selection_bias_sd"] is None
    for index in range(500):
        handle.record_trial(
            strategy_id=strategy_id, params={"a": index}, sharpe=0.1, run_id=index
        )
    trials = handle.trials(strategy_id)
    assert trials["combinations"] == 500
    assert trials["selection_bias_sd"] == pytest.approx(math.sqrt(2 * math.log(500)))
    assert trials["selection_bias_sd"] == pytest.approx(3.526, abs=1e-3)


# --------------------------------------------------------------------------- events


def write_events(store: RunStore, run_id: int, entries: list[dict]) -> None:
    path = store.artefact(run_id, "events.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(entry, separators=(",", ":")) + "\n" for entry in entries),
        encoding="utf-8",
    )


def test_event_paging_filters_and_searches_the_whole_file(store) -> None:
    """Filtering the rendered page instead would search fifty rows out of a million and
    report 'no matches' for something plainly in the log."""
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    write_events(
        handle,
        run_id,
        [
            {"seq": i, "ts_ms": i, "kind": "FILL" if i % 2 else "RECORD", "payload": {"i": i}}
            for i in range(500)
        ],
    )

    page, total = handle.read_events(run_id, offset=0, limit=10)
    assert total == 500 and len(page) == 10 and page[0]["seq"] == 0

    page, total = handle.read_events(run_id, offset=0, limit=10, kind="FILL")
    assert total == 250
    assert all(entry["kind"] == "FILL" for entry in page)

    page, total = handle.read_events(run_id, offset=0, limit=10, search='"i":499')
    assert total == 1 and page[0]["seq"] == 499

    page, total = handle.read_events(run_id, offset=495, limit=10)
    assert total == 500 and len(page) == 5


def test_a_payload_that_merely_looks_like_a_kind_is_not_counted(store) -> None:
    """The raw-text prefilter is a fast path, not the answer.

    The false positive has to be a **nested key**, not a string value: JSON escapes the
    quotes inside a value, so `{"message": '"kind":"FILL"'}` never produces the raw substring
    and cannot exercise the re-check at all. A strategy writing
    `ctx.log.info("x", kind="FILL")` produces `"fields":{"kind":"FILL"}` unescaped, which
    does -- and that is the shape this fixture uses.
    """
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    write_events(
        handle,
        run_id,
        [
            {
                "seq": 1,
                "ts_ms": 1,
                "kind": "LOG",
                "payload": {"level": "INFO", "message": "x", "fields": {"kind": "FILL"}},
            },
            {"seq": 2, "ts_ms": 2, "kind": "FILL", "payload": {}},
        ],
    )
    raw = handle.artefact(run_id, "events.jsonl").read_text(encoding="utf-8")
    assert raw.count('"kind":"FILL"') == 2, "the fixture must actually fool the prefilter"

    page, total = handle.read_events(run_id, kind="FILL")
    assert total == 1
    assert page[0]["seq"] == 2


def test_reading_a_missing_artefact_names_the_run(store) -> None:
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    with pytest.raises(RunNotFound, match="metrics.json"):
        handle.read_json(run_id, "metrics.json")


# -------------------------------------------------------------------- worker pipes

FLOOD_BYTES = 64 * 1024
"""Sixteen times the 4096-byte buffer Windows gives an anonymous pipe.

The buffer size is what matters: past it, a `stderr=PIPE` that nobody is reading blocks the
writing process inside `write()`, and a process blocked there can never exit. Sixteen times
over rather than just past it, so the test is not measuring a boundary.
"""

STDERR_END_MARKER = "END-OF-WORKER-STDERR-4172"
"""Written last, so an assertion for it proves the drain kept up to the end of the stream
rather than catching the first block and stopping."""

FLOOD_CHILD = (
    "import sys\n"
    f"sys.stderr.write('E' * {FLOOD_BYTES})\n"
    f"sys.stderr.write('\\n{STDERR_END_MARKER}\\n')\n"
    "sys.stderr.flush()\n"
    "raise SystemExit(3)\n"
)

DEAD_CHILD_MARKER = "the worker could not start"
DEAD_CHILD = (
    "import sys\n"
    f"sys.stderr.write('{DEAD_CHILD_MARKER}\\n')\n"
    "sys.stderr.flush()\n"
    "raise SystemExit(4)\n"
)

CHILD_EXIT_TIMEOUT_S = 20.0
"""How long a child that writes 64 KB and exits is given. It needs milliseconds; this is the
bound past which "the pipe wedged it" is the only remaining explanation."""


def scripted_child(monkeypatch: pytest.MonkeyPatch, code: str, *, wait: bool = False) -> None:
    """Swap the worker's argv for `code`, keeping every other `Popen` argument.

    The pipes, the working directory and the Windows creation flags are the launcher's own,
    because they are what the deadlock is made of -- only the program is this test's.

    With `wait=True` the child is reaped before the launcher gets its handle back, which is
    the "the worker was already gone" case: a state `launch_session` cannot produce on its
    own, since a subprocess pipe's read end survives exactly as long as the process does.
    """
    real_popen = subprocess.Popen

    def fake_popen(args, **kwargs):  # type: ignore[no-untyped-def]
        process = real_popen([sys.executable, "-c", code], **kwargs)
        if wait:
            process.wait(timeout=CHILD_EXIT_TIMEOUT_S)
        return process

    monkeypatch.setattr(runs_module.subprocess, "Popen", fake_popen)


@pytest.mark.slow
def test_a_backtest_worker_that_floods_stderr_still_exits_and_is_recorded(
    store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pipe nobody reads is a pipe that stops the writer.

    `_reap` used to be the only reader of `process.stderr`, and it read only after
    `poll()` returned an exit code -- which a child blocked inside `write()` can never
    produce. The child in this test writes past the pipe buffer and then exits with 3; if
    nothing is draining, `wait` never returns and the run is wedged with the row still
    claiming to be alive.

    The exit code and the end-of-stream marker are both this test's own, so a pass means the
    worker really ran to completion and its whole stderr survived to the failure record.
    """
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    scripted_child(monkeypatch, FLOOD_CHILD)

    handle.launch(run_id)
    process = handle._processes[run_id]
    try:
        assert process.wait(timeout=CHILD_EXIT_TIMEOUT_S) == 3
    finally:
        if process.poll() is None:  # pragma: no cover - only on the wedged path
            process.kill()

    summary = handle.get(run_id)  # triggers `_reap`
    assert summary.status == RunStatus.FAILED
    assert "code 3" in (summary.error or "")
    assert STDERR_END_MARKER in (summary.error or "")


@pytest.mark.slow
def test_a_session_worker_that_floods_stderr_still_exits_and_is_recorded(
    store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same deadlock on the session launcher, where it is the more expensive one.

    A wedged session is blocked inside `write()`, so it cannot poll `control.json`, cannot
    heartbeat and cannot cancel its resting orders: the stop button and the kill switch both
    write a control file nobody will ever read and report success. Reaching it takes no
    strategy misbehaviour at all -- three REST pollers logging one warning a second during
    an outage fill a 4 KB pipe in about eleven seconds.
    """
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    scripted_child(monkeypatch, "import sys\nsys.stdin.readline()\n" + FLOOD_CHILD)

    handle.launch_session(run_id, secrets={"max_runtime_s": 0.0})
    process = handle._processes[run_id]
    try:
        assert process.wait(timeout=CHILD_EXIT_TIMEOUT_S) == 3
    finally:
        if process.poll() is None:  # pragma: no cover - only on the wedged path
            process.kill()

    summary = handle.get(run_id)
    assert summary.status == RunStatus.FAILED
    assert STDERR_END_MARKER in (summary.error or "")


@pytest.mark.slow
def test_launching_a_session_whose_worker_is_already_gone_records_why(
    store, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dead child is a diagnosis, not an exception on the way out of the store.

    Writing the credential to a child that has already exited raises `OSError: [Errno 22]`,
    and it used to propagate past the line that keeps the handle and past the `UPDATE runs
    SET pid` -- so the caller got a 500 quoting an errno, the `Popen` was dropped on the
    floor, and the row sat at `queued` until the stale sweep could call it `lost` three
    minutes later. What the operator needs is the worker's own stderr, and the row now
    carries it.
    """
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    scripted_child(monkeypatch, DEAD_CHILD, wait=True)

    handle.launch_session(run_id, secrets={"max_runtime_s": 0.0})

    assert run_id in handle._processes, "the dead worker was not tracked"
    row = handle._connection.execute(
        "SELECT pid FROM runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert row["pid"] == handle._processes[run_id].pid

    summary = handle.get(run_id)  # triggers `_reap`
    assert summary.status == RunStatus.FAILED
    assert "code 4" in (summary.error or "")
    assert DEAD_CHILD_MARKER in (summary.error or "")


# ------------------------------------------------------ shared-connection discipline


class ConnectionProbe:
    """The store's real connection, wrapped so every touch records whether the store's
    lock was held at that moment.

    Why instrumentation rather than racing threads: the defect class under test is a
    dirty read. Every FastAPI worker thread shares this store's one sqlite3 connection,
    and a read that runs while another thread is inside `with self._connection:`
    executes inside that open transaction and observes its half-written state. Nothing
    raises -- the read simply answers wrongly -- so a race-it-and-see test would pass on
    almost every schedule and flake on the rest. The invariant that *prevents* the class
    is deterministic and directly checkable: no statement reaches the connection without
    the lock. `RLock._is_owned()` is the same primitive `threading.Condition` leans on
    to enforce "you must hold the lock to wait", asked here as "did the store hold its
    own lock when it touched its own connection".
    """

    def __init__(self, connection, lock) -> None:
        self._real = connection
        self._lock = lock
        self.calls: list[tuple[str, tuple]] = []
        self.unlocked: list[str] = []

    def _note(self, what: str, parameters: tuple) -> None:
        self.calls.append((what, parameters))
        if not self._lock._is_owned():
            self.unlocked.append(what)

    def execute(self, sql: str, parameters=()):
        self._note(" ".join(sql.split()), tuple(parameters))
        return self._real.execute(sql, parameters)

    def __enter__(self):
        self._note("<with connection: transaction>", ())
        return self._real.__enter__()

    def __exit__(self, *exc):
        return self._real.__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._real, name)


def probe_store(handle: RunStore) -> ConnectionProbe:
    probe = ConnectionProbe(handle._connection, handle._lock)
    handle._connection = probe  # type: ignore[assignment]
    return probe


def test_every_database_touch_holds_the_stores_lock(store) -> None:
    """M25: the writers all took the lock; the readers shared the connection bare.

    `get`, `list`, `active`, `trials` and `delete`'s Lab-link guard each ran
    `self._connection.execute` without the lock, so a Live Monitor poll landing while
    another thread sat inside a write transaction read that transaction's half-written
    state -- status updated, artefact pointer not yet, believed either way. This walks
    every public method that touches the database (the launchers are exercised by the
    pipe tests above; their SQL runs through `_track`, which was already disciplined)
    and asserts the invariant directly. See `ConnectionProbe` for why this form and not
    a racing test.
    """
    handle, strategy_id, version_id = store
    probe = probe_store(handle)

    first = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.mark_running(first)
    handle.heartbeat(first)
    handle.progress(first, 10, 100)
    handle.checkpoint(
        first, net_pnl="1.0", sharpe=0.5, max_drawdown=-0.1, round_trips=1, fills=2,
        bars=10,
    )
    handle.complete(
        first, event_hash="ff", fill_tier="BAR_CLOSE", tier_reason=None, flags=[],
        warnings=[], net_pnl="1.0", sharpe=0.5, max_drawdown=-0.1, round_trips=1,
        fills=2, bars=100,
    )
    handle.get(first)
    handle.list()
    handle.list(
        strategy_id=strategy_id, status=RunStatus.DONE, include_archived=True, limit=10
    )
    handle.active()
    handle.record_trial(
        strategy_id=strategy_id, params={"fast": 9}, sharpe=0.5, run_id=first
    )
    handle.trials(strategy_id)
    handle.archive(first)
    handle.archive(first, archived=False)

    second = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.link_shadow(second, first)
    handle.cancel(second)  # the no-handle path: two reads around an unconditional fail
    handle.delete(second)
    handle.delete(first)  # the Lab-link guard read plus the two-statement transaction

    assert len(probe.calls) > 20, "the probe must actually be in the store's path"
    assert probe.unlocked == []


# ---------------------------------------------------------------- the landing query


def test_the_default_listing_is_served_by_an_index_not_a_scan_and_sort(store) -> None:
    """L4: `list()`'s default shape had no covering index.

    `WHERE archived_ms IS NULL ORDER BY created_ms DESC, id DESC` matched neither
    `runs_by_strategy` nor `runs_by_status`, so the plan was `SCAN` plus `USE TEMP
    B-TREE FOR ORDER BY` -- a full pass and a full sort of a table nothing prunes, on
    the query the Runs page issues every time it renders. EXPLAIN QUERY PLAN is the
    failing-before/passing-after form for an index: the plan is a property of the
    schema, not the row count, so the assertion holds at two rows exactly as at a
    hundred thousand. The SQL is captured from a real `list()` call rather than
    duplicated here, so the plan being asserted is the plan being served.
    """
    handle, strategy_id, version_id = store
    run_id = handle.create(strategy_id=strategy_id, version_id=version_id, spec=SPEC)
    handle.fail(run_id, "plan fodder")

    probe = probe_store(handle)
    assert len(handle.list()) == 1
    sql, params = next(
        (sql, params) for sql, params in probe.calls if "ORDER BY" in sql
    )
    plan = "; ".join(
        str(row["detail"])
        for row in handle._connection.execute("EXPLAIN QUERY PLAN " + sql, params)
    )
    assert "runs_recency" in plan, plan
    assert "TEMP B-TREE" not in plan, plan


def test_the_recency_index_is_retrofitted_onto_an_existing_database(tmp_path: Path) -> None:
    """Databases created by earlier builds exist on disk without the index, and the
    schema version deliberately does not carry it: an index is not a shape change, so
    `RunStore` creates it with IF NOT EXISTS at open rather than by a v8 migration
    that would refuse the database to the previous build over a read optimisation.
    """
    db.connect(tmp_path).close()  # the database as an earlier build leaves it

    def run_indexes(connection) -> set[str]:
        return {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'runs'"
            )
        }

    plain = db.connect(tmp_path)
    try:
        assert "runs_recency" not in run_indexes(plain), (
            "db.connect must not create the index itself, or this test no longer "
            "exercises the retrofit path and the CREATE in RunStore.__init__ is "
            "redundant -- move one of them"
        )
    finally:
        plain.close()

    handle = RunStore(tmp_path)
    try:
        assert "runs_recency" in run_indexes(handle._connection)
    finally:
        handle.close()
