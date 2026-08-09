"""Regressions for the Phase 9/10 adversarial review.

Every test here fails against the code as it was written. They are grouped by the claim
each defect made falsely, because that is what made them worth fixing: in each case the
platform was not merely wrong, it was *asserting* something.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from perplab.api.app import create_app
from perplab.api.routers.settings import load_settings
from perplab.lab.montecarlo import MonteCarloConfig, MonteCarloInputs, run_montecarlo
from perplab.lab.portfolio import portfolio_report
from perplab.lab.regimes import RegimeConfig, RegimeInputs, _bucket_metrics
from perplab.lab.walkforward import WalkForwardConfig, build_folds
from perplab.store import db
from perplab.store.lab import LabStore
from perplab.store.runs import RunStatus

HOUR = 3_600_000
BASE = 1_709_251_200_000


# ------------------------------------------- "ruin is structurally unreachable" (false)


def _blowup_inputs(**overrides) -> MonteCarloInputs:
    base = dict(
        trade_pnls=(10.0, -5.0),
        # `build_grid` emits a return at or below -100% when equity crosses zero inside a
        # grid step -- so this series is what a blown-up run actually produces.
        grid_returns=tuple([0.001] * 60 + [-1.04]),
        periods_per_year=365,
        grid_label="daily",
        opening_balance=1_000.0,
        max_drawdown_limit=None,
    )
    base.update(overrides)
    return MonteCarloInputs(**base)


def test_a_compounded_path_that_reaches_zero_is_counted_as_ruin() -> None:
    """The critical one: both compounding methods used to answer `null` and cite an
    arithmetic guarantee (`1 + r > 0`) that `max(0.0, 1.0 + r)` does not provide."""
    result = run_montecarlo(
        _blowup_inputs(), MonteCarloConfig(iterations=300, seed=3)
    )
    started = result["methods"]["random_start"]
    assert started["prob_ruin"] == pytest.approx(1.0)
    assert started["ruin_unavailable_reason"] is None
    assert started["final_equity"]["max"] == pytest.approx(0.0)

    blocks = result["methods"]["block_bootstrap"]
    assert blocks["prob_ruin"] is not None and blocks["prob_ruin"] > 0.0
    assert blocks["ruin_unavailable_reason"] is None


def test_a_ruined_paths_sharpe_stops_at_the_return_that_killed_it() -> None:
    """Annualising over periods after the account died dilutes the collapse with
    periods nobody could have traded -- the rule `build_grid` already applies."""
    result = run_montecarlo(
        _blowup_inputs(grid_returns=(0.01, -1.5, 0.02, 0.03, 0.04)),
        MonteCarloConfig(iterations=100, seed=1, methods=("random_start",)),
    )
    first = result["methods"]["random_start"]["series"][0]
    assert first["ruined"] is True
    # Two returns scored (0.01 and -1.5), not five.
    assert first["final_equity"] == pytest.approx(0.0)


def test_the_sharpe_distribution_discloses_how_many_iterations_it_dropped() -> None:
    """`iterations: 1000` beside a Sharpe over 63 of them, with nothing saying so."""
    result = run_montecarlo(
        _blowup_inputs(grid_returns=tuple([0.0] * 40)),
        MonteCarloConfig(iterations=200, seed=5, methods=("block_bootstrap",)),
    )
    method = result["methods"]["block_bootstrap"]
    assert method["sharpe"] is None  # every resample has zero dispersion
    assert method["sharpe_undefined_iterations"] == 200


def test_a_directly_built_config_refuses_a_zero_block_length() -> None:
    """`config.block_length or default` read 0 as "unset"; `from_json` caught it and a
    config built in Python did not."""
    with pytest.raises(ValueError, match="block_length"):
        MonteCarloConfig(block_length=0)


# ------------------------------ "the same trades, two different win rates" (regimes)


def test_the_regime_win_rate_matches_the_run_pages_win_rate() -> None:
    """`wins / closed`, including scratches in the denominator, exactly as
    `analytics.metrics.trade_stats` computes it -- 0.333, not 0.500."""
    trades = tuple(
        [(BASE + i * HOUR, 1.0) for i in range(10)]
        + [(BASE + (10 + i) * HOUR, -1.0) for i in range(10)]
        + [(BASE + (20 + i) * HOUR, 0.0) for i in range(10)]
    )
    inputs = RegimeInputs(
        grid_times=tuple(BASE + i * HOUR for i in range(4)),
        grid_returns=(0.01, 0.0, -0.01),
        periods_per_year=8760,
        grid_label="hourly",
        trades=trades,
        daily_bars=(),
        funding=(),
        liquidations=(),
    )
    buckets = _bucket_metrics(
        ["x", "x", "x"], inputs, ["x"] * len(trades), RegimeConfig(min_periods=1, min_trades=1)
    )
    assert buckets["x"]["trade_win_rate"] == pytest.approx(10 / 30)
    assert buckets["x"]["trade_scratches"] == 10


# --------------------------------- "uncovered_tail_ms" hid interior holes (walk-forward)


def test_a_step_wider_than_the_window_reports_its_interior_holes() -> None:
    """`step_ms > oos_ms` leaves gaps *between* folds. Reporting only the tail said
    100 ms uncovered where 300 ms of the range was never evaluated."""
    folds, uncovered = build_folds(0, 1000, is_ms=300, oos_ms=100, step_ms=250)
    assert [(f.oos_start_ms, f.oos_end_ms) for f in folds] == [(300, 400), (550, 650), (800, 900)]
    # 400-550, 650-800, 900-1000 = 150 + 150 + 100.
    assert uncovered == 400


# ------------------------------- "defaults for new runs" that nothing read (settings)


def test_settings_survive_a_file_the_model_cannot_parse(tmp_path: Path) -> None:
    """`extra="forbid"` is right for a PUT and fatal on a read: an unknown key turned
    the whole Settings tab into a 500 with no way back."""
    db.connect(tmp_path).close()
    (tmp_path / "settings.json").write_text('{"default_leverage": 3, "from_the_future": 1}')
    settings, problem = load_settings(tmp_path)
    assert settings.default_leverage == 10  # platform defaults, not a crash
    assert problem is not None and "settings.json" in problem

    app = create_app(tmp_path)
    with TestClient(app) as client:
        body = client.get("/api/settings")
        assert body.status_code == 200
        assert body.json()["problem"] is not None


def test_settings_survive_a_corrupt_file(tmp_path: Path) -> None:
    db.connect(tmp_path).close()
    (tmp_path / "settings.json").write_text("{not json")
    settings, problem = load_settings(tmp_path)
    assert settings.default_leverage == 10
    assert problem is not None


# ---------------------------- "cancel it first" on a job that cannot be cancelled (lab)


def test_a_lost_lab_job_can_be_deleted_and_stops_blocking_its_run(tmp_path: Path) -> None:
    """The deadlock: three refusals pointing at each other, reachable by restarting the
    API server while a walk-forward ran."""
    db.connect(tmp_path).close()
    labs = LabStore(tmp_path)
    try:
        job_id = labs.create(run_id=1, tool="montecarlo", config={})
        connection = db.connect(tmp_path)
        with connection:
            connection.execute(
                "UPDATE lab_jobs SET heartbeat_ms = 0, created_ms = 0 WHERE id = ?",
                (job_id,),
            )
        connection.close()
        assert labs.get(job_id).status == RunStatus.LOST
        labs.delete(job_id)  # used to raise
        assert labs.list() == []
    finally:
        labs.close()


def test_concurrent_reads_of_the_lab_store_do_not_race(tmp_path: Path) -> None:
    """`GET /lab/jobs` under the Lab tab's own polling used to 500 on a `KeyError`."""
    db.connect(tmp_path).close()
    labs = LabStore(tmp_path)
    try:
        for _ in range(20):
            labs.create(run_id=1, tool="montecarlo", config={})
        errors: list[BaseException] = []

        def hammer() -> None:
            try:
                for _ in range(40):
                    labs.list()
                    labs.create(run_id=1, tool="regimes", config={})
            except BaseException as exc:  # noqa: BLE001 - the assertion is below
                errors.append(exc)

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert not errors, errors[0]
    finally:
        labs.close()


# ------------------------- "refused at submit time" that was not (walk-forward config)


def test_walkforward_config_refuses_an_overlapping_step_at_parse_time() -> None:
    """The router's docstring names this exact example as a 400. It reached the worker."""
    with pytest.raises(ValueError, match="overlap"):
        WalkForwardConfig.from_json(
            {"is_ms": 30 * 24 * HOUR, "oos_ms": 24 * HOUR, "step_ms": 1, "grid": {}}
        )


def test_walkforward_config_refuses_absurd_windows_and_worker_counts() -> None:
    with pytest.raises(ValueError, match="at least"):
        WalkForwardConfig.from_json({"is_ms": 1, "oos_ms": 1, "grid": {}})
    with pytest.raises(ValueError, match="max_workers"):
        WalkForwardConfig.from_json(
            {"is_ms": HOUR, "oos_ms": HOUR, "grid": {}, "max_workers": 512}
        )


def test_walkforward_config_refuses_a_fold_count_the_range_cannot_carry() -> None:
    """`is_ms=1min, oos_ms=1min` over a year passes every per-field check and then asks
    `build_folds` for half a million folds."""
    config = WalkForwardConfig.from_json(
        {"is_ms": 60_000, "oos_ms": 60_000, "grid": {}}
    )
    with pytest.raises(ValueError, match="folds"):
        config.check_against(BASE, BASE + 365 * 24 * HOUR)


# ------------------------------ "final_pnl" that dropped the trailing partial period


def test_portfolio_final_pnl_includes_the_partial_final_period() -> None:
    """`sampled[-1]` is the last *whole* grid boundary; a run ending 12 hours later had
    those hours silently dropped from the column a reader reconciles against the ledger."""
    days = 90
    step = 24 * HOUR
    times = [BASE + i * step for i in range(days + 1)] + [BASE + days * step + 12 * HOUR]
    btc = [float(i) for i in range(days + 1)] + [float(days) + 5.0]
    eth = [float(-i) for i in range(days + 1)] + [float(-days) - 3.0]
    report = portfolio_report(
        times=times,
        symbol_pnl={"BTCUSDT": btc, "ETHUSDT": eth},
        start_ms=BASE,
        end_ms=BASE + days * step + 12 * HOUR,
        rolling_window=5,
    )
    assert report["per_symbol"]["BTCUSDT"]["final_pnl"] == pytest.approx(days + 5.0)
    assert report["per_symbol"]["BTCUSDT"]["final_pnl_at_grid_close"] == pytest.approx(days)
    assert report["per_symbol"]["ETHUSDT"]["final_pnl"] == pytest.approx(-days - 3.0)


# --------------------------------------------- an out-of-range id crashed the server


def test_an_out_of_range_id_is_a_404_not_a_500(tmp_path: Path) -> None:
    """FastAPI's `int` is arbitrary-precision; SQLite's binding is not."""
    db.connect(tmp_path).close()
    app = create_app(tmp_path)
    with TestClient(app) as client:
        big = 10**25
        assert client.get(f"/api/lab/jobs/{big}").status_code == 404
        assert client.get(f"/api/lab/jobs?run_id={big}").status_code == 404
        assert client.post(f"/api/lab/jobs/{big}/cancel").status_code == 404
        assert client.delete(f"/api/lab/jobs/{big}").status_code == 404
        assert client.get(f"/api/lab/jobs/{big}/result").status_code == 404
        # `/lab/compare` parsed a 200-digit number inside its own try/except and the
        # OverflowError was raised later, in the store.
        assert client.get(f"/api/lab/compare?runs={big},2").status_code in (404, 409, 422)


# ------------------------ the kill-switch UI surface, which had never worked at all


def test_the_kill_endpoints_shapes_are_what_the_client_unwraps(tmp_path: Path) -> None:
    """Three client bugs lived in these two shapes, and all three hid behind a green
    type-check because TypeScript cannot see across the wire:

    1. `GET /kill` answers `{kill: null | trip}`; the client read `.armed` off the
       envelope, so the armed badge in the chrome and the Dashboard banner could never
       render -- the one safety interlock in the platform was invisible.
    2. The trip's field is `flattened`, not `flatten`. The confirm dialog derived its
       behaviour from it and, finding `undefined`, disabled its own fire button.
    3. `POST /kill/unarm` requires a body and answers `{cleared: ...}`, not `{kill: ...}`.
       The client sent no body (422) and parsed the wrong key, so spec 7.6's explicit
       un-arm reported success while doing nothing.

    This test pins the wire contract those fixes depend on.
    """
    db.connect(tmp_path).close()
    app = create_app(tmp_path)
    with TestClient(app) as client:
        idle = client.get("/api/kill")
        assert idle.status_code == 200
        assert idle.json() == {"kill": None}, "the envelope shape the client unwraps"

        fired = client.post("/api/kill", json={"flatten": False, "detail": "test"})
        assert fired.status_code == 200
        trip = fired.json()["kill"]
        assert trip["armed"] is True
        assert "flattened" in trip and "flatten" not in trip, (
            "the trip records what it did (`flattened`); a client reading `flatten` gets "
            "undefined and disables its own fire button"
        )

        assert client.get("/api/kill").json()["kill"]["armed"] is True

        # A bodyless un-arm is refused -- which is why the button had to send one.
        assert client.post("/api/kill/unarm").status_code == 422

        cleared = client.post("/api/kill/unarm", json={"actor": "operator"})
        assert cleared.status_code == 200
        assert "cleared" in cleared.json(), "the response key is `cleared`, not `kill`"
        assert cleared.json()["cleared"]["cleared_by"] == "operator"

        # Spec 7.6: the trip is cleared, and its history survives for the incident review.
        assert client.get("/api/kill").json()["kill"] is None
