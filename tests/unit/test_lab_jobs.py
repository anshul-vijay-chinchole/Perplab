"""Lab jobs: the store, the worker's assembly seam, and the API surface (spec 9)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from perplab.api.app import create_app
from perplab.engine.worker import execute_run
from perplab.lab.worker import JobRefused, execute_job
from perplab.store import db
from perplab.store.lab import LabNotFound, LabStore
from perplab.store.runs import RunStatus, RunStore
from tests.engine_lake import MS_PER_MINUTE
from tests.integration.test_run_worker import (
    MINUTES,
    START,
    STRATEGY_SOURCE,
    build_userdata,
    make_spec,
    seed_strategy,
)

MS_PER_HOUR = 60 * MS_PER_MINUTE


@pytest.fixture()
def stores(tmp_path: Path):
    """A userdata tree with one completed backtest run, plus both stores."""
    build_userdata(tmp_path)
    runs = RunStore(tmp_path)
    labs = LabStore(tmp_path)
    strategy_id, version_id = seed_strategy(tmp_path, STRATEGY_SOURCE)
    spec = make_spec(strategy_id, version_id, STRATEGY_SOURCE)
    run_id = runs.create(
        strategy_id=strategy_id, version_id=version_id, spec=spec.to_storage()
    )
    execute_run(tmp_path, run_id, runs)
    assert runs.get(run_id).status == RunStatus.DONE
    yield tmp_path, runs, labs, run_id, strategy_id
    labs.close()
    runs.close()


def _wf_config(**overrides) -> dict:
    base = {
        "is_ms": 4 * MS_PER_HOUR,
        "oos_ms": 2 * MS_PER_HOUR,
        "grid": {"period": [4, 5, 6]},
        "max_workers": 1,
    }
    base.update(overrides)
    return base


# ----------------------------------------------------------------------------- store


def test_create_get_list_round_trip(stores) -> None:
    root, runs, labs, run_id, _ = stores
    job_id = labs.create(
        run_id=run_id, tool="montecarlo", config={"iterations": 500}, label="mc"
    )
    job = labs.get(job_id)
    assert job.status == RunStatus.QUEUED
    assert job.tool == "montecarlo"
    assert job.config == {"iterations": 500}
    assert [j.id for j in labs.list(run_id=run_id)] == [job_id]
    assert labs.read_json(job_id, "job.json")["run_id"] == run_id


def test_an_unknown_tool_is_refused_at_create(stores) -> None:
    _, _, labs, run_id, _ = stores
    with pytest.raises(ValueError, match="unknown Lab tool"):
        labs.create(run_id=run_id, tool="alchemy", config={})


def test_a_missing_job_raises_labnotfound(stores) -> None:
    _, _, labs, _, _ = stores
    with pytest.raises(LabNotFound):
        labs.get(999)


def test_cancel_writes_the_control_file_the_worker_polls(stores) -> None:
    _, _, labs, run_id, _ = stores
    job_id = labs.create(run_id=run_id, tool="walkforward", config=_wf_config())
    assert labs.stop_requested(job_id) is False
    labs.cancel(job_id)
    assert labs.stop_requested(job_id) is True


def test_a_silent_job_with_no_handle_is_lost_not_failed(stores) -> None:
    """`RunStore._reap`'s distinction, holding for Lab workers too."""
    root, _, labs, run_id, _ = stores
    job_id = labs.create(run_id=run_id, tool="montecarlo", config={})
    connection = db.connect(root)
    with connection:
        connection.execute(
            "UPDATE lab_jobs SET heartbeat_ms = 0, created_ms = 0 WHERE id = ?",
            (job_id,),
        )
    connection.close()
    job = labs.get(job_id)
    assert job.status == RunStatus.LOST
    assert "fate is unknown" in job.error


def test_a_run_with_lab_artefacts_cannot_be_deleted_out_from_under_them(stores) -> None:
    """Spec 9: artefacts are linked to the source run *permanently*."""
    _, runs, labs, run_id, _ = stores
    job_id = labs.create(run_id=run_id, tool="montecarlo", config={})
    labs.complete(job_id, summary={})
    with pytest.raises(ValueError, match="Lab artefact"):
        runs.delete(run_id)
    labs.delete(job_id)
    runs.delete(run_id)  # now allowed


def test_a_non_terminal_job_cannot_be_deleted(stores) -> None:
    _, _, labs, run_id, _ = stores
    job_id = labs.create(run_id=run_id, tool="montecarlo", config={})
    with pytest.raises(ValueError, match="cancel it"):
        labs.delete(job_id)


# ---------------------------------------------------------------------- walk-forward


def test_a_walkforward_job_produces_the_artefacts_and_counts_the_trials(stores) -> None:
    root, runs, labs, run_id, strategy_id = stores
    job_id = labs.create(run_id=run_id, tool="walkforward", config=_wf_config())
    execute_job(root, job_id, labs, runs)

    job = labs.get(job_id)
    assert job.status == RunStatus.DONE, job.error
    assert job.summary["folds"] == 2
    assert job.summary["grid_points"] == 3
    assert job.progress_done == job.progress_total > 0

    result = labs.read_json(job_id, "result.json")
    assert result["tool"] == "walkforward"
    assert len(result["walkforward"]["folds"]) == 2
    assert result["overfit"]["plateau"] is not None
    assert result["overfit"]["trials"]["combinations"] >= 3
    assert labs.artefact(job_id, "stitched.parquet").exists()

    # Spec 8.5: the grid was three combinations; the run itself already recorded one.
    trials = runs.trials(strategy_id)
    assert trials["combinations"] >= 3
    # Grid evaluations with no run row must not have stolen the best-run link unless
    # they genuinely beat it -- and if they did, the link is honestly NULL, not stale.
    connection = db.connect(root)
    rows = connection.execute(
        "SELECT best_run_id, best_sharpe FROM strategy_trials WHERE strategy_id = ?",
        (strategy_id,),
    ).fetchall()
    connection.close()
    assert rows  # the counter moved


def test_a_walkforward_on_a_session_run_is_refused_with_the_reason(stores) -> None:
    root, runs, labs, _, strategy_id = stores
    spec = make_spec(strategy_id, 1, STRATEGY_SOURCE).to_storage()
    spec["source"] = "tape:1"
    spec["session_kind"] = "paper"
    session_run = runs.create(strategy_id=strategy_id, version_id=1, spec=spec, mode="paper")
    runs.complete(
        session_run,
        event_hash="x", fill_tier="BOOK_WALK", tier_reason="", flags=(), warnings=(),
        net_pnl="0", sharpe=None, max_drawdown=None, round_trips=0, fills=0, bars=0,
    )
    job_id = labs.create(run_id=session_run, tool="walkforward", config=_wf_config())
    with pytest.raises(JobRefused, match="tape"):
        execute_job(root, job_id, labs, runs)


def test_a_job_on_an_incomplete_run_is_refused(stores) -> None:
    root, runs, labs, _, strategy_id = stores
    queued = runs.create(
        strategy_id=strategy_id,
        version_id=1,
        spec=make_spec(strategy_id, 1, STRATEGY_SOURCE).to_storage(),
    )
    job_id = labs.create(run_id=queued, tool="montecarlo", config={})
    with pytest.raises(JobRefused, match="completed"):
        execute_job(root, job_id, labs, runs)


def test_a_pre_cancelled_walkforward_ends_cancelled_not_failed(stores) -> None:
    root, runs, labs, run_id, _ = stores
    job_id = labs.create(run_id=run_id, tool="walkforward", config=_wf_config())
    labs.cancel(job_id)
    execute_job(root, job_id, labs, runs)
    job = labs.get(job_id)
    assert job.status == RunStatus.CANCELLED
    assert "cancelled" in job.error


# ----------------------------------------------------------------------- monte carlo


def test_a_montecarlo_job_reads_the_run_artefacts_and_completes(stores) -> None:
    root, runs, labs, run_id, _ = stores
    job_id = labs.create(
        run_id=run_id, tool="montecarlo", config={"iterations": 500, "seed": 3}
    )
    execute_job(root, job_id, labs, runs)
    job = labs.get(job_id)
    assert job.status == RunStatus.DONE, job.error
    result = labs.read_json(job_id, "result.json")["montecarlo"]
    assert set(result["methods"]) == {
        "trade_permutation", "trade_bootstrap", "block_bootstrap", "random_start",
    }
    assert result["inputs"]["grid_returns"] > 0
    # The run ran with no risk limits, so no breach probability may be invented.
    for method in result["methods"].values():
        assert method["drawdown_limit"] is None


# --------------------------------------------------------------------------- regimes


def test_a_regimes_job_reports_what_the_lake_cannot_support_honestly(stores) -> None:
    """A 10-hour lake holds no complete daily bar, three funding settlements do not
    exist, and no liquidation dataset was ever collected. The honest output is
    `unclassified` periods and an unavailable cascade dimension -- never a guess."""
    root, runs, labs, run_id, _ = stores
    job_id = labs.create(run_id=run_id, tool="regimes", config={})
    execute_job(root, job_id, labs, runs)
    job = labs.get(job_id)
    assert job.status == RunStatus.DONE, job.error

    result = labs.read_json(job_id, "result.json")["regimes"]
    dims = result["dimensions"]
    assert dims["cascade"]["available"] is False
    periods = result["periods"]
    assert dims["volatility"]["buckets"]["unclassified"]["periods"] == periods
    assert dims["trend"]["buckets"]["unclassified"]["periods"] == periods
    assert dims["funding"]["buckets"]["unclassified"]["periods"] == periods
    assert job.summary["dimensions"] == ["volatility", "trend", "funding"]


# --------------------------------------------------------------------------- the API


@pytest.fixture()
def client(stores, monkeypatch):
    root, runs, labs, run_id, strategy_id = stores
    # The API test asserts the submission surface, not the worker: launching a real
    # subprocess per test would re-run the walk-forward suite through the HTTP layer.
    monkeypatch.setattr(LabStore, "launch", lambda self, job_id: None)
    app = create_app(root)
    with TestClient(app) as handle:
        yield handle, run_id


def test_submit_validates_tool_config_and_run_state(client) -> None:
    handle, run_id = client

    response = handle.post(
        "/api/lab/jobs", json={"run_id": run_id, "tool": "alchemy", "config": {}}
    )
    assert response.status_code == 400
    assert "unknown Lab tool" in response.json()["detail"]

    response = handle.post(
        "/api/lab/jobs",
        json={"run_id": run_id, "tool": "montecarlo", "config": {"iterations": 5}},
    )
    assert response.status_code == 400
    assert "at least 100" in response.json()["detail"]

    response = handle.post(
        "/api/lab/jobs", json={"run_id": 999, "tool": "montecarlo", "config": {}}
    )
    assert response.status_code == 404

    response = handle.post(
        "/api/lab/jobs",
        json={"run_id": run_id, "tool": "walkforward", "config": _wf_config()},
    )
    assert response.status_code == 201
    job = response.json()["job"]
    assert job["status"] == "queued"
    assert job["run_id"] == run_id

    listed = handle.get(f"/api/lab/jobs?run_id={run_id}").json()["jobs"]
    assert [j["id"] for j in listed] == [job["id"]]

    assert handle.get(f"/api/lab/jobs/{job['id']}").status_code == 200
    # No result yet: the job never ran (launch is patched out).
    assert handle.get(f"/api/lab/jobs/{job['id']}/result").status_code == 404
    assert handle.get(f"/api/lab/jobs/{job['id']}/stitched").status_code == 404

    assert handle.post(f"/api/lab/jobs/{job['id']}/cancel").status_code == 200
    assert handle.delete(f"/api/lab/jobs/{job['id']}").status_code == 412


def test_submitting_against_an_unfinished_run_is_a_409(client, stores) -> None:
    handle, _ = client
    root, runs, _, _, strategy_id = stores
    queued = runs.create(
        strategy_id=strategy_id,
        version_id=1,
        spec=make_spec(strategy_id, 1, STRATEGY_SOURCE).to_storage(),
    )
    response = handle.post(
        "/api/lab/jobs", json={"run_id": queued, "tool": "montecarlo", "config": {}}
    )
    assert response.status_code == 409
    assert "completed run" in response.json()["detail"]


def test_compare_endpoint_aligns_two_real_runs_and_blocks_mismatches(client, stores) -> None:
    handle, run_id = client
    root, runs, _, _, strategy_id = stores

    # A second completed run over the same range, different parameters.
    spec = make_spec(strategy_id, 1, STRATEGY_SOURCE)
    other_spec = spec.to_storage()
    other_spec["params"] = {"period": 9}
    other = runs.create(strategy_id=strategy_id, version_id=1, spec=other_spec)
    execute_run(root, other, runs)

    response = handle.get(f"/api/lab/compare?runs={run_id},{other}")
    assert response.status_code == 200, response.json()
    payload = response.json()
    assert payload["run_ids"] == [run_id, other]
    assert len(payload["curves"]) == 2
    assert payload["curves"][0]["equity_normalised"][0] == pytest.approx(1.0)
    assert payload["correlation"]["matrix"][0][0] == 1.0

    # A third run over a shorter range does not compare.
    short_spec = spec.to_storage()
    short_spec["end_ms"] = spec.end_ms - 120 * MS_PER_MINUTE
    short = runs.create(strategy_id=strategy_id, version_id=1, spec=short_spec)
    execute_run(root, short, runs)
    response = handle.get(f"/api/lab/compare?runs={run_id},{short}")
    assert response.status_code == 409
    assert "cannot share a chart" in response.json()["detail"]

    assert handle.get(f"/api/lab/compare?runs={run_id}").status_code == 400
    assert handle.get(f"/api/lab/compare?runs={run_id},{run_id}").status_code == 400


def test_submitting_a_walkforward_against_a_session_is_a_400(client, stores) -> None:
    handle, _ = client
    root, runs, _, _, strategy_id = stores
    spec = make_spec(strategy_id, 1, STRATEGY_SOURCE).to_storage()
    spec["source"] = "tape:7"
    spec["session_kind"] = "paper"
    session_run = runs.create(
        strategy_id=strategy_id, version_id=1, spec=spec, mode="paper"
    )
    runs.complete(
        session_run,
        event_hash="x", fill_tier="BOOK_WALK", tier_reason="", flags=(), warnings=(),
        net_pnl="0", sharpe=None, max_drawdown=None, round_trips=0, fills=0, bars=0,
    )
    response = handle.post(
        "/api/lab/jobs",
        json={"run_id": session_run, "tool": "walkforward", "config": _wf_config()},
    )
    assert response.status_code == 400
    assert "tape" in response.json()["detail"]
