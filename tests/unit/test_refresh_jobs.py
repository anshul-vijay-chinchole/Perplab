"""Refresh jobs: the store, the worker's assembly seam, and the data API.

Nothing here touches the network or writes a real archive. `ingest_range` is replaced by a
function returning a hand-built `IngestReport`, and `plan_refresh` by a hand-built
`RefreshPlan` -- the two seams `refresh_worker` and `routers.data` are assembled from. That
is deliberate rather than merely fast: the behaviours under test are the *guards* (what is
refused, what is measured, what a verdict may claim), and a test that downloaded a real
archive to reach them would be testing Binance's availability instead.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from perplab.api.app import create_app
from perplab.data import refresh, refresh_worker
from perplab.data.ingest_bulk import FileOutcome, IngestPlan, IngestReport, IngestStatus
from perplab.data.refresh import ConflictCost, DatasetRefresh, RefreshPlan
from perplab.data.schemas import COLLECTOR_EVENTS, market_root
from perplab.store import db
from perplab.store.ingests import IngestNotFound, IngestStore
from perplab.store.runs import RunStatus

SYMBOL = "BTCUSDT"


# ------------------------------------------------------------------------- fixtures


@pytest.fixture()
def store(tmp_path: Path):
    market_root(tmp_path).mkdir(parents=True, exist_ok=True)
    handle = IngestStore(tmp_path)
    yield handle
    handle.close()


def _plan(archives: int = 1, dataset: str = "klines", kind: str = "candles") -> RefreshPlan:
    """A `RefreshPlan` with `archives` periods in one dataset, built without HTTP."""
    periods = tuple(f"2026-07-{day + 1:02d}" for day in range(archives))
    inner = IngestPlan(
        symbol=SYMBOL,
        dataset=dataset,
        periods=periods,
        already=(),
        outside=(),
        available=True,
        caveat=None,
    )
    plan = RefreshPlan(kind=kind, symbol=SYMBOL)
    plan.datasets.append(
        DatasetRefresh(dataset, inner, periods[0] if periods else None,
                       periods[-1] if periods else None, None)
    )
    return plan


def _report(*outcomes: FileOutcome, dataset: str = "klines") -> IngestReport:
    report = IngestReport(symbol=SYMBOL, dataset=dataset, root=Path("."))
    report.outcomes = list(outcomes)
    report.periods_requested = len(report.outcomes)
    return report


def _outcome(status: IngestStatus, period: str, **kwargs: Any) -> FileOutcome:
    return FileOutcome(
        symbol=SYMBOL, dataset=kwargs.pop("dataset", "klines"), period=period,
        status=status, **kwargs
    )


def _patch_seams(
    monkeypatch: pytest.MonkeyPatch,
    *,
    plan: RefreshPlan,
    report: IngestReport,
    cost: ConflictCost | None = None,
) -> list[tuple[str, str, str]]:
    """Replace the worker's two seams. Returns the `ingest_range` calls it made."""
    calls: list[tuple[str, str, str]] = []

    def fake_range(root, symbol, dataset, start, end, **kwargs):
        calls.append((dataset, start, end))
        return report

    monkeypatch.setattr(refresh, "plan_refresh", lambda *a, **k: plan)
    monkeypatch.setattr(refresh_worker, "ingest_range", fake_range)
    if cost is not None:
        monkeypatch.setattr(refresh, "measure_conflict_cost", lambda *a, **k: cost)
    return calls


# ---------------------------------------------------------------------------- store


def test_the_schema_carries_the_ingest_jobs_table(tmp_path: Path) -> None:
    connection = db.connect(tmp_path)
    version = connection.execute("SELECT version FROM schema_version").fetchone()
    columns = {
        str(row["name"]) for row in connection.execute("PRAGMA table_info(ingest_jobs)")
    }
    connection.close()
    assert int(version["version"]) == db.SCHEMA_VERSION >= 8
    assert "cancel_requested_ms" in columns
    assert "heartbeat_ms" in columns


def test_job_lifecycle_from_queued_to_done(store: IngestStore) -> None:
    job_id = store.create(kind="candles", symbol=SYMBOL)
    job = store.get(job_id)
    assert job.status == RunStatus.QUEUED
    assert job.kind == "candles"
    assert job.to_json()["cancel_requested"] is False
    assert json.loads(
        store.artefact(job_id, "job.json").read_text(encoding="utf-8")
    )["symbol"] == SYMBOL

    store.mark_running(job_id)
    assert store.get(job_id).status == RunStatus.RUNNING
    store.progress(job_id, 2, 5)
    assert store.get(job_id).progress_done == 2

    store.complete(job_id, summary={"verdict": "clean"})
    done = store.get(job_id)
    assert done.status == RunStatus.DONE
    assert done.summary == {"verdict": "clean"}
    assert done.finished_ms is not None
    assert [j.id for j in store.list()] == [job_id]
    assert store.active() == []

    with pytest.raises(IngestNotFound):
        store.get(job_id + 999)


def test_a_failure_may_keep_the_summary_of_what_it_managed_to_do(store) -> None:
    """A cancelled refresh has written durable partitions; discarding the tally would
    send the operator back to re-plan work that is already on disk."""
    job_id = store.create(kind="trades", symbol=SYMBOL)
    store.fail(job_id, "stopped", status=RunStatus.CANCELLED, summary={"written": 6})
    job = store.get(job_id)
    assert job.status == RunStatus.CANCELLED
    assert job.summary == {"written": 6}


def test_every_read_holds_the_lock(store: IngestStore) -> None:
    """Audit finding M25: FastAPI serves `def` routes from a thread pool, so an unlocked
    read shares one SQLite connection between two concurrent polls."""

    class _Watcher:
        def __init__(self, connection: Any, lock: Any) -> None:
            self._connection = connection
            self._lock = lock
            self.held: list[bool] = []

        def execute(self, *args: Any, **kwargs: Any) -> Any:
            self.held.append(bool(self._lock._is_owned()))
            return self._connection.execute(*args, **kwargs)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._connection, name)

        def __enter__(self) -> Any:
            return self._connection.__enter__()

        def __exit__(self, *exc: object) -> Any:
            return self._connection.__exit__(*exc)

    job_id = store.create(kind="candles", symbol=SYMBOL)
    watcher = _Watcher(store._connection, store._lock)
    store._connection = watcher  # type: ignore[assignment]

    store.get(job_id)
    store.list()
    store.active()
    assert watcher.held, "no statement ran, so the test proved nothing"
    assert all(watcher.held), "a read executed SQL without holding the store lock"


def test_a_silent_job_with_no_handle_is_lost_not_failed(store, tmp_path: Path) -> None:
    job_id = store.create(kind="candles", symbol=SYMBOL)
    connection = db.connect(tmp_path)
    with connection:
        connection.execute(
            "UPDATE ingest_jobs SET heartbeat_ms = 0, created_ms = 0 WHERE id = ?",
            (job_id,),
        )
    connection.close()
    job = store.get(job_id)
    assert job.status == RunStatus.LOST
    assert "fate is unknown" in (job.error or "")


def test_cancel_writes_the_control_file_the_worker_polls(store) -> None:
    job_id = store.create(kind="trades", symbol=SYMBOL)
    assert store.stop_requested(job_id) is False
    job = store.cancel(job_id)
    assert store.stop_requested(job_id) is True
    assert job.to_json()["cancel_requested"] is True


# --------------------------------------------------------------------------- worker


def test_a_refresh_runs_the_planned_span_and_records_a_clean_verdict(
    store, tmp_path: Path, monkeypatch
) -> None:
    plan = _plan(archives=3)
    report = _report(
        _outcome(IngestStatus.WRITTEN, "2026-07-01", rows=1440, archive_bytes=350_000),
        _outcome(IngestStatus.SKIPPED, "2026-07-02"),
        _outcome(IngestStatus.WRITTEN, "2026-07-03", rows=1440, archive_bytes=350_000),
    )
    calls = _patch_seams(monkeypatch, plan=plan, report=report)

    job_id = store.create(kind="candles", symbol=SYMBOL)
    refresh_worker.execute_job(tmp_path, job_id, store)

    job = store.get(job_id)
    assert job.status == RunStatus.DONE, job.error
    assert job.summary is not None
    assert job.summary["verdict"] == "clean"
    assert job.summary["written"] == 2
    assert job.summary["skipped"] == 1
    assert job.summary["rows"] == 2880
    assert job.summary["datasets"] == [
        {"dataset": "klines", "written": 2, "declined": 0, "missing": 0, "failed": 0}
    ]
    # The span comes from the *trimmed* plan, which is what keeps periods the barren
    # record removed out of the request.
    assert calls == [("klines", "2026-07-01", "2026-07-03")]
    # The lock is released whatever happened.
    assert not (market_root(tmp_path) / "_ingest" / refresh.LOCK_NAME).exists()


def test_a_conflict_with_a_measured_hole_is_never_clean(
    store, tmp_path: Path, monkeypatch
) -> None:
    """The 2026-08-02 failure: the archive was declined, eleven hours were missing, and
    the surface above reported "we declined" as "we are finished"."""
    cost = ConflictCost(
        period="2026-07-01",
        dataset="aggTrades",
        collector_rows=900_000,
        first_ms=1,
        last_ms=2,
        missing_ms=11 * 3600 * 1000,
        note="eleven hours of it exists in the bulk archive and not in your lake",
    )
    _patch_seams(
        monkeypatch,
        plan=_plan(archives=1, dataset="aggTrades", kind="trades"),
        report=_report(
            _outcome(IngestStatus.CONFLICT, "2026-07-01", dataset="aggTrades"),
            dataset="aggTrades",
        ),
        cost=cost,
    )

    job_id = store.create(kind="trades", symbol=SYMBOL)
    refresh_worker.execute_job(tmp_path, job_id, store)

    summary = store.get(job_id).summary
    assert summary is not None
    assert summary["verdict"] == "attention"
    assert summary["declined"] == 1
    assert summary["conflicts"] == [
        {
            "dataset": "aggTrades",
            "period": "2026-07-01",
            "collector_rows": 900_000,
            "missing_ms": 11 * 3600 * 1000,
            "note": cost.note,
        }
    ]


def test_a_conflict_that_cost_nothing_is_clean(store, tmp_path: Path, monkeypatch) -> None:
    _patch_seams(
        monkeypatch,
        plan=_plan(archives=1, dataset="aggTrades", kind="trades"),
        report=_report(
            _outcome(IngestStatus.CONFLICT, "2026-07-01", dataset="aggTrades"),
            dataset="aggTrades",
        ),
        cost=ConflictCost(
            period="2026-07-01",
            dataset="aggTrades",
            collector_rows=900_000,
            first_ms=0,
            last_ms=86_399_999,
            missing_ms=0,
            note="the collector's rows span the whole day",
        ),
    )
    job_id = store.create(kind="trades", symbol=SYMBOL)
    refresh_worker.execute_job(tmp_path, job_id, store)
    assert store.get(job_id).summary["verdict"] == "clean"


def test_an_unreadable_conflict_partition_is_not_clean(
    store, tmp_path: Path, monkeypatch
) -> None:
    """`missing_ms is None` means the cost is *unknown*, which must never render green."""
    _patch_seams(
        monkeypatch,
        plan=_plan(archives=1, dataset="aggTrades", kind="trades"),
        report=_report(
            _outcome(IngestStatus.CONFLICT, "2026-07-01", dataset="aggTrades"),
            dataset="aggTrades",
        ),
        cost=ConflictCost(
            period="2026-07-01", dataset="aggTrades", collector_rows=None,
            first_ms=None, last_ms=None, missing_ms=None,
            note="the partition could not be read",
        ),
    )
    job_id = store.create(kind="trades", symbol=SYMBOL)
    refresh_worker.execute_job(tmp_path, job_id, store)
    assert store.get(job_id).summary["verdict"] == "attention"


def test_permanently_unfetchable_periods_are_recorded_and_transient_ones_are_not(
    store, tmp_path: Path, monkeypatch
) -> None:
    """F7 (404 inside coverage) and F8 (a parse refusal) are recorded; a checksum
    mismatch and a timeout are not -- both clear on their own."""
    _patch_seams(
        monkeypatch,
        plan=_plan(archives=4, dataset="metrics"),
        report=_report(
            _outcome(IngestStatus.MISSING, "2026-07-01", dataset="metrics"),
            _outcome(
                IngestStatus.FAILED, "2026-07-02", dataset="metrics",
                error="MalformedArchive: line 2: ValueError: metrics.sum_open_interest: "
                      "more than 8 decimal places",
            ),
            _outcome(
                IngestStatus.FAILED, "2026-07-03", dataset="metrics",
                error="ChecksumMismatch: published abc, downloaded def",
            ),
            _outcome(
                IngestStatus.FAILED, "2026-07-04", dataset="metrics",
                error="TransientFetchError: read timeout",
            ),
            dataset="metrics",
        ),
    )
    job_id = store.create(kind="candles", symbol=SYMBOL)
    refresh_worker.execute_job(tmp_path, job_id, store)

    recorded = refresh.read_barren(market_root(tmp_path), "metrics", SYMBOL)
    assert sorted(recorded) == ["2026-07-01", "2026-07-02"]
    assert store.get(job_id).summary["verdict"] == "attention"


def test_a_cancelled_refresh_lands_as_cancelled_with_its_tally(
    store, tmp_path: Path, monkeypatch
) -> None:
    _patch_seams(monkeypatch, plan=_plan(archives=2), report=_report())
    job_id = store.create(kind="candles", symbol=SYMBOL)
    store.cancel(job_id)
    refresh_worker.execute_job(tmp_path, job_id, store)

    job = store.get(job_id)
    assert job.status == RunStatus.CANCELLED
    assert "cancelled by the user" in (job.error or "")
    assert job.summary is not None and job.summary["written"] == 0
    assert not (market_root(tmp_path) / "_ingest" / refresh.LOCK_NAME).exists()


def test_an_unhealthy_collector_fails_the_job_with_its_own_message(
    store, tmp_path: Path, monkeypatch
) -> None:
    """Re-checked in the worker, not only at the API: minutes pass between the click and
    this line, and the collector's health is exactly what changes inside that window."""
    _stale_collector(tmp_path)
    _patch_seams(monkeypatch, plan=_plan(), report=_report())
    job_id = store.create(kind="candles", symbol=SYMBOL)
    assert refresh_worker.main([str(tmp_path), str(job_id)]) == 1
    job = store.get(job_id)
    assert job.status == RunStatus.FAILED
    assert "last heartbeat" in (job.error or "")
    # A refusal message is stored verbatim -- no traceback in front of the sentence the
    # operator is meant to act on.
    assert "Traceback" not in (job.error or "")


def test_the_heartbeat_is_a_wall_clock_tick_not_a_progress_event(
    store, tmp_path: Path
) -> None:
    """A single archive can retry for over five minutes emitting no progress event; a
    progress-driven heartbeat would have `_reap` calling that job lost mid-download."""
    job_id = store.create(kind="candles", symbol=SYMBOL)
    store.mark_running(job_id)
    handle = refresh.refresh_lock(market_root(tmp_path), kind="candles", symbol=SYMBOL)
    try:
        connection = db.connect(tmp_path)
        with connection:
            connection.execute(
                "UPDATE ingest_jobs SET heartbeat_ms = 1 WHERE id = ?", (job_id,)
            )
        connection.close()

        counter = refresh_worker._Counter(4)
        stop = threading.Event()
        pulse = refresh_worker._Pulse(store, job_id, handle, counter, stop)
        pulse.tick()  # no progress has been reported; the beat happens anyway

        row = store._connection.execute(
            "SELECT heartbeat_ms FROM ingest_jobs WHERE id = ?", (job_id,)
        ).fetchone()
        assert int(row["heartbeat_ms"]) > 1
        assert store.get(job_id).progress_total == 4

        # The same tick is what notices a cancel, on the one thread that is free to look.
        store.cancel(job_id)
        pulse.tick()
        assert stop.is_set()
    finally:
        handle.release()


# ------------------------------------------------------------------------------ API


def _stale_collector(root: Path) -> None:
    market = market_root(root)
    market.mkdir(parents=True, exist_ok=True)
    (market / "collector_state.json").write_text(
        json.dumps(
            {"pid": 4242, "symbol": SYMBOL,
             "last_heartbeat_ms": int(time.time() * 1000) - 600_000}
        ),
        encoding="utf-8",
    )


@pytest.fixture()
def client(tmp_path: Path, monkeypatch):
    """The API with `launch` patched out -- the submission surface, not the worker."""
    market_root(tmp_path).mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(IngestStore, "launch", lambda self, job_id: None)
    monkeypatch.setattr(refresh, "plan_refresh", lambda *a, **k: _plan(archives=2))
    app = create_app(tmp_path)
    with TestClient(app) as handle:
        yield handle, tmp_path


def test_a_dry_run_describes_the_plan_and_starts_nothing(client) -> None:
    handle, root = client
    response = handle.post(
        "/api/data/update",
        json={"kind": "candles", "symbol": "btcusdt", "dry_run": True},
    )
    assert response.status_code == 200, response.json()
    payload = response.json()
    assert payload["plan"]["archives"] == 2
    assert payload["plan"]["needs_confirmation"] is False
    assert payload["conflicts_predicted"] == []
    assert handle.get("/api/data/updates").json()["jobs"] == []


def test_a_bad_symbol_or_kind_is_a_400(client) -> None:
    handle, _ = client
    bad_symbol = handle.post(
        "/api/data/update", json={"kind": "candles", "symbol": "BTC/USDT"}
    )
    assert bad_symbol.status_code == 400
    bad_kind = handle.post(
        "/api/data/update", json={"kind": "orderbooks", "symbol": SYMBOL}
    )
    assert bad_kind.status_code == 400
    assert "unknown refresh kind" in bad_kind.json()["detail"]


def test_a_second_concurrent_refresh_is_a_409(client) -> None:
    handle, _ = client
    first = handle.post("/api/data/update", json={"kind": "candles", "symbol": SYMBOL})
    assert first.status_code == 200, first.json()
    job = first.json()["job"]
    assert job["status"] == "queued"

    second = handle.post("/api/data/update", json={"kind": "trades", "symbol": SYMBOL})
    assert second.status_code == 409
    assert "already" in second.json()["detail"]
    assert len(handle.get("/api/data/updates").json()["jobs"]) == 1


def test_a_lock_held_by_another_process_is_a_409(client) -> None:
    handle, root = client
    held = refresh.refresh_lock(market_root(root), kind="candles", symbol=SYMBOL)
    try:
        response = handle.post(
            "/api/data/update", json={"kind": "candles", "symbol": SYMBOL}
        )
        assert response.status_code == 409
        assert "still holds the lake" in response.json()["detail"]
        assert handle.get("/api/data/updates").json()["jobs"] == []
    finally:
        held.release()


def test_an_unhealthy_collector_is_a_409(client) -> None:
    handle, root = client
    _stale_collector(root)
    response = handle.post(
        "/api/data/update", json={"kind": "candles", "symbol": SYMBOL}
    )
    assert response.status_code == 409
    assert "heartbeat" in response.json()["detail"]
    assert handle.get("/api/data/updates").json()["jobs"] == []


def test_a_plan_needing_confirmation_is_a_409_and_starts_nothing(
    client, monkeypatch
) -> None:
    handle, _ = client
    monkeypatch.setattr(refresh, "plan_refresh", lambda *a, **k: _plan(archives=30))

    refused = handle.post("/api/data/update", json={"kind": "candles", "symbol": SYMBOL})
    assert refused.status_code == 409
    assert "confirm=true" in refused.json()["detail"]
    assert handle.get("/api/data/updates").json()["jobs"] == []

    accepted = handle.post(
        "/api/data/update",
        json={"kind": "candles", "symbol": SYMBOL, "confirm": True},
    )
    assert accepted.status_code == 200, accepted.json()
    assert len(handle.get("/api/data/updates").json()["jobs"]) == 1


def test_a_job_can_be_polled_and_cancelled_over_http(client) -> None:
    handle, _ = client
    job_id = handle.post(
        "/api/data/update", json={"kind": "candles", "symbol": SYMBOL}
    ).json()["job"]["id"]

    fetched = handle.get(f"/api/data/updates/{job_id}").json()["job"]
    assert set(fetched) == {
        "id", "kind", "symbol", "status", "created_ms", "started_ms", "finished_ms",
        "progress_done", "progress_total", "summary", "error", "cancel_requested",
    }

    cancelled = handle.post(f"/api/data/updates/{job_id}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["job"]["cancel_requested"] is True

    assert handle.get("/api/data/updates/9999").status_code == 404
    assert handle.get(f"/api/data/updates/{2**70}").status_code == 404


# ------------------------------------------------------------------- collector card


def _write_events(root: Path, rows: list[dict[str, Any]], date: str) -> None:
    partition = market_root(root) / "collectorEvents" / f"date={date}"
    partition.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=COLLECTOR_EVENTS)
    pq.write_table(table, partition / "part-1-0.parquet")


def _event(ts: int, kind: str, detail: str = "", stream: str = "collector") -> dict:
    return {"ts_ms": ts, "kind": kind, "stream": stream, "detail": detail,
            "downtime_ms": 0}


def test_the_collector_card_reports_what_it_can_observe(client) -> None:
    handle, root = client
    now = int(time.time() * 1000)
    _write_events(
        root,
        [
            _event(now - 400_000, "CONNECT", "cold start"),
            _event(now - 300_000, "RESTART", "previous run ended without clean shutdown"),
            _event(now - 60_000, "HEARTBEAT", "aggTrades=12, bookTicker=44, depth20=0"),
            _event(now - 30_000, "STALE", "depth20: no data", stream="depth20"),
        ],
        "2026-08-05",
    )
    (market_root(root) / "collector_state.json").write_text(
        json.dumps({"pid": 77, "symbol": SYMBOL, "last_heartbeat_ms": now - 5_000}),
        encoding="utf-8",
    )

    card = handle.get("/api/data/collector").json()["collector"]
    assert card["state_file_present"] is True
    assert card["pid"] == 77
    assert card["last_heartbeat_ms"] == now - 5_000
    assert 0 <= card["heartbeat_age_ms"] < 60_000
    # The newest CONNECT/RESTART, not the oldest, and not "uptime".
    assert card["run_started_ms"] == now - 300_000
    assert card["run_elapsed_ms"] >= 300_000
    assert card["restarts_since_run_start"] == 1
    # From the heartbeat's own counter keys. `depth20` sits at zero, so it is not claimed
    # to be recording -- and it is named in a caveat rather than silently dropped.
    assert card["datasets_recording"] == ["aggTrades", "bookTicker"]
    assert any("depth20" in c for c in card["caveats"])


def test_the_collector_card_never_claims_the_phase_1b_criterion(client) -> None:
    """PERPLAB_SPEC.md section 13's criterion is a conjunction of three things and this
    endpoint can observe one. A percentage built from elapsed time alone would read as
    progress toward a sign-off whose other two conjuncts are unevaluated."""
    handle, root = client
    now = int(time.time() * 1000)
    _write_events(root, [_event(now - 400_000, "CONNECT", "cold start")], "2026-08-05")

    payload = handle.get("/api/data/collector").json()
    forbidden = ("criterion", "percent", "pct", "uptime", "complete", "ready", "progress")

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                assert not any(
                    word in key.lower() for word in forbidden
                ), f"collector card exposes a field named {key!r}"
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    card = payload["collector"]
    assert card["restarts_since_run_start"] == 0
    assert card["datasets_recording"] == []
    assert any("HEARTBEAT" in c for c in card["caveats"])


def test_an_absent_state_file_is_ambiguous_not_stopped(client) -> None:
    """The file is deleted on a *clean* shutdown, so absence means either 'not running'
    or 'was stopped tidily' -- never 'running', and never a red light."""
    handle, _ = client
    card = handle.get("/api/data/collector").json()["collector"]
    assert card["state_file_present"] is False
    assert card["pid"] is None
    assert card["last_heartbeat_ms"] is None
    assert card["heartbeat_age_ms"] is None
    assert card["run_started_ms"] is None
    assert card["run_elapsed_ms"] is None
    assert card["restarts_since_run_start"] is None
    assert any("ambiguous" in c for c in card["caveats"])


def _write_event_files(root: Path, date: str, rows: list[dict[str, Any]]) -> Path:
    """One part-file per row, as the collector itself writes: many small files per day."""
    partition = market_root(root) / "collectorEvents" / f"date={date}"
    partition.mkdir(parents=True, exist_ok=True)
    for index, row in enumerate(rows):
        table = pa.Table.from_pylist([row], schema=COLLECTOR_EVENTS)
        pq.write_table(table, partition / f"part-{row['ts_ms']}-{index:06d}.parquet")
    return partition


def test_the_collector_card_reads_events_split_across_many_files(client) -> None:
    """The collector writes ~1,400 part-files a day, so the card's answer must not depend on
    the events happening to share a file. This is the shape the endpoint actually meets."""
    handle, root = client
    now = int(time.time() * 1000)
    _write_event_files(
        root,
        "2026-08-04",
        [_event(now - 800_000, "CONNECT", "cold start")]
        + [_event(now - 700_000 + i * 1_000, "HEARTBEAT", "aggTrades=1") for i in range(40)],
    )
    _write_event_files(
        root,
        "2026-08-05",
        [_event(now - 300_000, "RESTART", "unclean shutdown")]
        + [
            _event(now - 200_000 + i * 1_000, "HEARTBEAT", f"aggTrades={i + 2}, depth20=9")
            for i in range(40)
        ],
    )

    card = handle.get("/api/data/collector").json()["collector"]
    # The CONNECT is in the older partition and the RESTART in the newer one: finding the
    # newest start at all proves both partitions and every file in them were read.
    assert card["run_started_ms"] == now - 300_000
    assert card["restarts_since_run_start"] == 1
    # From the newest heartbeat of the 80 written, which is the last file of the last day.
    assert card["datasets_recording"] == ["aggTrades", "depth20"]
    assert not any("could not be read" in c for c in card["caveats"])


def test_one_unreadable_file_costs_one_file_and_is_named(client) -> None:
    """The bulk read raises on a corrupt file and would take every event with it. The
    fallback exists so the card degrades by one file instead of going blank, and says which
    file it lost rather than quietly answering from a subset."""
    handle, root = client
    now = int(time.time() * 1000)
    partition = _write_event_files(
        root,
        "2026-08-05",
        [
            _event(now - 400_000, "CONNECT", "cold start"),
            _event(now - 300_000, "RESTART", "unclean shutdown"),
            _event(now - 60_000, "HEARTBEAT", "aggTrades=12, bookTicker=44"),
        ],
    )
    (partition / "part-9999999999999-000099.parquet").write_bytes(b"not a parquet file")

    card = handle.get("/api/data/collector").json()["collector"]
    # Every readable event still counted -- the corrupt file did not blank the card.
    assert card["run_started_ms"] == now - 300_000
    assert card["restarts_since_run_start"] == 1
    assert card["datasets_recording"] == ["aggTrades", "bookTicker"]
    # And the loss is named, with the file that caused it.
    assert any(
        "could not be read" in c and "part-9999999999999-000099.parquet" in c
        for c in card["caveats"]
    ), card["caveats"]


# -------------------------------------------------------------------------- startup


def test_a_restart_reconciles_an_orphaned_refresh_as_lost(tmp_path: Path, monkeypatch) -> None:
    market_root(tmp_path).mkdir(parents=True, exist_ok=True)
    seed = IngestStore(tmp_path)
    job_id = seed.create(kind="candles", symbol=SYMBOL)
    seed.mark_running(job_id)
    seed.close()

    app = create_app(tmp_path)
    with TestClient(app) as handle:
        job = handle.get(f"/api/data/updates/{job_id}").json()["job"]
    assert job["status"] == RunStatus.LOST
    assert "no handle to its worker" in job["error"]
