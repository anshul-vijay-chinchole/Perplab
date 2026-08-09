"""The Runs API surface.

Field names are asserted here rather than in the browser, for the reason `frontend/src/api.ts`
gives: the two are hand-kept in sync, so a rename on the server has to break a Python test
instead of a React runtime.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from perplab.api.app import create_app
from perplab.api.routers.runs import (
    DAY_MS,
    _downsample_extremes,
    _partition_days,
    _segments,
)
from perplab.data.schemas import BOOK_TICKER, market_root
from perplab.engine.backtest import EquityProgress, _thin_extremes
from perplab.store import db
from perplab.store.runs import PROGRESS_EQUITY_NAME, RunStore
from tests.integration.test_run_worker import (
    MINUTES,
    START,
    STRATEGY_SOURCE,
    build_userdata,
)
from tests.engine_lake import MS_PER_MINUTE


@pytest.fixture()
def client(tmp_path: Path):
    build_userdata(tmp_path)
    app = create_app(tmp_path)
    with TestClient(app) as handle:
        yield handle, tmp_path


def add_strategy(root: Path, *, valid: bool = True, source: str = STRATEGY_SOURCE) -> int:
    connection = db.connect(root)
    with connection:
        strategy_id = connection.execute(
            "INSERT INTO strategies (name, created_ms, updated_ms) VALUES ('Wobble', 0, 0)"
        ).lastrowid
        version_id = connection.execute(
            """
            INSERT INTO strategy_versions
                (strategy_id, version_no, code, code_sha256, created_ms, valid,
                 params_json, requires_json)
            VALUES (?, 1, ?, 'sha', 0, ?, ?, ?)
            """,
            (
                strategy_id,
                source,
                1 if valid else 0,
                json.dumps(
                    [{"name": "period", "type": "int", "default": 5, "min": 2, "max": 50}]
                ),
                json.dumps(
                    {
                        "symbols": ["BTCUSDT"],
                        "timeframe": "1m",
                        "history": 50,
                        "datasets": ["klines"],
                    }
                ),
            ),
        ).lastrowid
        connection.execute(
            "UPDATE strategies SET head_version_id = ? WHERE id = ?",
            (version_id, strategy_id),
        )
    connection.close()
    return int(strategy_id)


def start_body(strategy_id: int, **overrides) -> dict:
    body = {
        "strategy_id": strategy_id,
        "start_ms": START + 120 * MS_PER_MINUTE,
        "end_ms": START + MINUTES * MS_PER_MINUTE,
        "seed": 3,
        "leverage": 10,
    }
    body.update(overrides)
    return body


# --------------------------------------------------------------------------- refusals


def test_an_invalid_version_cannot_be_backtested(client) -> None:
    """Spec 5.6 stores an invalid version so work is never lost. Running one is a different
    question: its numbers would come from code the validator had already refused."""
    handle, root = client
    strategy_id = add_strategy(root, valid=False)
    response = handle.post("/api/runs", json=start_body(strategy_id))
    assert response.status_code == 412
    assert "did not pass validation" in response.json()["detail"]


def test_an_empty_range_is_refused(client) -> None:
    handle, root = client
    strategy_id = add_strategy(root)
    response = handle.post(
        "/api/runs", json=start_body(strategy_id, start_ms=START, end_ms=START)
    )
    assert response.status_code == 400
    assert "half-open" in response.json()["detail"]


def test_an_unknown_timeframe_is_refused(client) -> None:
    handle, root = client
    strategy_id = add_strategy(root)
    response = handle.post("/api/runs", json=start_body(strategy_id, timeframe="7m"))
    assert response.status_code == 400
    assert "unknown timeframe" in response.json()["detail"]


def test_an_unknown_parameter_is_a_400_naming_the_key(client) -> None:
    """Bound in the request, against the version's own declarations, so a typo does not
    become a worker that starts and dies."""
    handle, root = client
    strategy_id = add_strategy(root)
    response = handle.post(
        "/api/runs", json=start_body(strategy_id, params={"perod": 5})
    )
    assert response.status_code == 400
    assert "perod" in response.json()["detail"]


def test_a_parameter_outside_its_declared_range_is_refused(client) -> None:
    handle, root = client
    strategy_id = add_strategy(root)
    response = handle.post(
        "/api/runs", json=start_body(strategy_id, params={"period": 9999})
    )
    assert response.status_code == 400


def test_a_zero_leverage_run_is_refused(client) -> None:
    handle, root = client
    strategy_id = add_strategy(root)
    response = handle.post("/api/runs", json=start_body(strategy_id, leverage=0))
    assert response.status_code == 400


def test_an_unknown_run_is_a_404(client) -> None:
    handle, _ = client
    assert handle.get("/api/runs/999").status_code == 404


# ------------------------------------------------------------------------- happy path


@pytest.mark.slow
def test_a_run_starts_completes_and_serves_every_artefact(client) -> None:
    handle, root = client
    strategy_id = add_strategy(root)

    created = handle.post("/api/runs", json=start_body(strategy_id))
    assert created.status_code == 201
    run_id = created.json()["run"]["id"]
    assert created.json()["run"]["status"] in ("queued", "running")

    store = handle.app.state.runs
    import time

    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        if store.get(run_id).status in ("done", "failed", "cancelled"):
            break
        time.sleep(0.2)

    detail = handle.get(f"/api/runs/{run_id}").json()
    assert detail["run"]["status"] == "done", detail["run"]["error"]
    assert detail["metrics"]["periods_per_year"] in (365, 8760)
    assert detail["attribution"]["net_pnl"]
    assert "reproducibility" in detail["manifest"]
    # The source never crosses the wire on a poll: the hash identifies it and the strategy
    # version holds it.
    assert "code" not in detail["spec"]
    assert detail["spec"]["code_sha256"]

    equity = handle.get(f"/api/runs/{run_id}/equity?points=200").json()
    assert len(equity["ts"]) == len(equity["equity"]) == len(equity["drawdown"])
    assert equity["returned"] <= 201
    assert equity["samples"] >= equity["returned"]
    assert max(equity["drawdown"]) <= 0.0
    # The shaded panel and the metric card must be the same computation, not two that
    # nearly agree: the served series carries the deepest drawdown exactly.
    assert min(equity["drawdown"]) == pytest.approx(detail["metrics"]["max_drawdown"])
    assert equity["equity"][0] == pytest.approx(10_000.0)

    price = handle.get(f"/api/runs/{run_id}/price?points=200").json()
    assert price["symbol"] == "BTCUSDT" and len(price["ts"]) == len(price["close"])

    trades = handle.get(f"/api/runs/{run_id}/trades").json()["trades"]
    assert trades
    for field in ("mae", "mfe", "mae_price", "mfe_price", "net_pnl", "close_reason"):
        assert field in trades[0]

    csv_response = handle.get(f"/api/runs/{run_id}/trades.csv")
    assert csv_response.status_code == 200
    assert "attachment" in csv_response.headers["content-disposition"]
    assert csv_response.text.splitlines()[0].startswith("index,")

    events = handle.get(f"/api/runs/{run_id}/events?limit=5&kind=FILL").json()
    assert events["total"] >= 1
    assert all(e["kind"] == "FILL" for e in events["events"])

    listed = handle.get("/api/runs").json()["runs"]
    assert [item["id"] for item in listed] == [run_id]

    trials = handle.get(f"/api/strategies/{strategy_id}/trials").json()
    assert trials["combinations"] == 1

    assert handle.post(f"/api/runs/{run_id}/archive", json={"archived": True}).status_code == 200
    assert handle.get("/api/runs").json()["runs"] == []
    assert handle.delete(f"/api/runs/{run_id}").status_code == 200
    assert handle.get(f"/api/runs/{run_id}").status_code == 404


def test_coverage_reports_the_range_the_lake_holds(client) -> None:
    """So the run form cannot offer a date the worker would then refuse."""
    handle, _ = client
    coverage = handle.get("/api/coverage?symbol=btcusdt").json()
    assert coverage["symbol"] == "BTCUSDT"
    assert coverage["start_ms"] == START
    assert coverage["end_ms"] == START + MINUTES * MS_PER_MINUTE
    assert coverage["bars"] == MINUTES


# ------------------------------------------------------------- coverage has holes in it


def test_segments_merge_consecutive_days_and_break_on_a_missing_one() -> None:
    """The unit under the bar. A run of days is one block; a missing day splits it, and the
    end is exclusive so a single day is exactly 86,400,000 ms wide rather than zero."""
    assert _segments([]) == []
    assert _segments([0]) == [{"start_ms": 0, "end_ms": DAY_MS}]
    assert _segments([0, 1, 2]) == [{"start_ms": 0, "end_ms": 3 * DAY_MS}]
    assert _segments([0, 1, 3]) == [
        {"start_ms": 0, "end_ms": 2 * DAY_MS},
        {"start_ms": 3 * DAY_MS, "end_ms": 4 * DAY_MS},
    ]


def test_partition_days_ignores_a_partition_holding_no_parquet(tmp_path: Path) -> None:
    """An empty `date=` directory is what a crash between `mkdir` and the publishing
    `os.replace` leaves behind. Counting it would draw a day of coverage over nothing."""
    market = tmp_path / "market"
    base = market / "bookTicker" / "symbol=BTCUSDT"
    (base / "date=2024-03-24").mkdir(parents=True)
    (base / "date=2024-03-24" / "data.parquet").write_bytes(b"")
    (base / "date=2024-03-25").mkdir()  # published nothing
    (base / "date=not-a-date").mkdir()  # not a period at all

    days = _partition_days(market, "bookTicker", "BTCUSDT")
    assert days == [(date(2024, 3, 24) - date(1970, 1, 1)).days]
    assert _segments(days) == [
        {"start_ms": days[0] * DAY_MS, "end_ms": (days[0] + 1) * DAY_MS}
    ]


def test_coverage_does_not_paint_over_an_interior_hole(client) -> None:
    """The defect this exists to prevent: `bookTicker` held 12 days inside an 865-day span
    and the panel drew one solid block from the first row to the last, which reads as two
    and a half years of continuous tick history. Bounds alone cannot say otherwise, so the
    covered days and the segments between them are reported alongside."""
    handle, root = client
    base = market_root(root) / "bookTicker" / "symbol=BTCUSDT"
    for day in (date(2024, 3, 24), date(2024, 3, 25), date(2026, 8, 1)):
        partition = base / f"date={day.isoformat()}"
        partition.mkdir(parents=True, exist_ok=True)
        noon = ((day - date(1970, 1, 1)).days * DAY_MS) + DAY_MS // 2
        pq.write_table(
            pa.table(
                {name: [noon if name in ("ts_ms", "recv_ms") else 1] for name in BOOK_TICKER.names},
                schema=BOOK_TICKER,
            ),
            partition / "data.parquet",
        )

    entry = handle.get("/api/coverage?symbol=BTCUSDT").json()["datasets"]["bookTicker"]
    # Two days together, then a hole of well over two years, then one day.
    assert entry["days_covered"] == 3
    assert entry["days_spanned"] == (date(2026, 8, 1) - date(2024, 3, 24)).days + 1
    assert len(entry["segments"]) == 2
    first, second = entry["segments"]
    assert second["start_ms"] > first["end_ms"], "the hole must survive to the wire"
    assert (first["end_ms"] - first["start_ms"]) == 2 * DAY_MS


def test_every_dataset_carries_the_coverage_fields(client) -> None:
    """`frontend/src/api.ts` types these three as always present rather than optional, so a
    dataset that omitted them would be a wire-shape mismatch TypeScript cannot see."""
    handle, _ = client
    datasets = handle.get("/api/coverage?symbol=BTCUSDT").json()["datasets"]
    assert datasets, "no datasets reported at all"
    for name, entry in datasets.items():
        assert isinstance(entry["segments"], list), name
        assert isinstance(entry["days_covered"], int), name
        assert isinstance(entry["days_spanned"], int), name
        assert entry["days_covered"] <= entry["days_spanned"], name


# ------------------------------------------------------------------------ downsampling


def test_downsampling_keeps_both_extremes_of_every_bucket() -> None:
    """Stride sampling drops whichever extreme fell between the points it kept, so a sharp
    drawdown and its recovery can vanish entirely -- and the deeper and faster the
    excursion, the more likely it is to be the one that disappears."""
    values = [100.0] * 100
    values[37] = 10.0  # a spike down
    values[63] = 500.0  # and one up
    keep = _downsample_extremes(list(range(100)), values, 5)
    kept = {values[i] for i in keep}
    assert 10.0 in kept
    assert 500.0 in kept
    assert len(keep) <= 12


def test_downsampling_always_keeps_the_final_sample() -> None:
    """The last point is the run's closing equity. A chart that ended somewhere else would
    disagree with every figure printed beside it."""
    values = [float(i) for i in range(1000)]
    keep = _downsample_extremes(list(range(1000)), values, 10)
    assert keep[-1] == 999


def test_a_short_series_is_returned_whole() -> None:
    assert _downsample_extremes([0, 1, 2], [1.0, 2.0, 3.0], 100) == [0, 1, 2]


def test_an_empty_series_downsamples_to_nothing() -> None:
    assert _downsample_extremes([], [], 10) == []


# --------------------------------------------------------- the in-progress equity preview


def _seed_run_row(root: Path, strategy_id: int) -> int:
    """A run row with a directory but no finished artefacts — what a live run looks like."""
    store = RunStore(root)
    run_id = store.create(
        strategy_id=strategy_id,
        version_id=1,
        spec={
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "start_ms": START,
            "end_ms": START + MINUTES * MS_PER_MINUTE,
            "seed": 3,
            "engine_version": 1,
        },
    )
    store.directory(run_id).mkdir(parents=True, exist_ok=True)
    return run_id


class TestPartialEquity:
    """A run publishes its curve while it runs, and says that is what it is.

    Before this, `equity.parquet` was written in one block after `engine.run()` returned, so
    `/equity` 404'd for the whole life of a run and the panel was empty until the last event
    was dispatched. On a session that is 48 hours of nothing.
    """

    def test_a_running_run_serves_the_preview_and_flags_it_partial(
        self, client, tmp_path: Path
    ) -> None:
        handle, root = client
        strategy_id = add_strategy(root)
        run_id = _seed_run_row(root, strategy_id)

        EquityProgress(
            ts=(1_000, 2_000, 3_000),
            equity=(10_000.0, 10_500.0, 10_200.0),
            low=(10_000.0, 10_400.0, 10_100.0),
            high=(10_000.0, 10_600.0, 10_500.0),
            samples=1_500,
            bars=1_500,
        ).publish(RunStore(root).artefact(run_id, PROGRESS_EQUITY_NAME))

        payload = handle.get(f"/api/runs/{run_id}/equity").json()
        assert payload["partial"] is True
        assert payload["equity"] == [10_000.0, 10_500.0, 10_200.0]
        # `samples` reports the *full* series, not the thinned one, so the caption cannot
        # be read as "the run has done 3 bars".
        assert payload["samples"] == 1_500
        assert payload["returned"] == 3

    def test_the_preview_drawdown_is_measured_against_the_peak_so_far(
        self, client
    ) -> None:
        handle, root = client
        run_id = _seed_run_row(root, add_strategy(root))
        EquityProgress(
            ts=(1_000, 2_000),
            equity=(10_000.0, 9_000.0),
            low=(10_000.0, 9_000.0),
            high=(10_000.0, 10_000.0),
            samples=2,
            bars=2,
        ).publish(RunStore(root).artefact(run_id, PROGRESS_EQUITY_NAME))

        payload = handle.get(f"/api/runs/{run_id}/equity").json()
        assert payload["drawdown"][0] == pytest.approx(0.0)
        assert payload["drawdown"][1] == pytest.approx(-0.1)

    def test_a_run_with_no_checkpoint_yet_is_still_a_404(self, client) -> None:
        """Expected for the first seconds of a run, and the frontend polls through it."""
        handle, root = client
        run_id = _seed_run_row(root, add_strategy(root))
        assert handle.get(f"/api/runs/{run_id}/equity").status_code == 404

    def test_a_torn_write_is_refused_rather_than_served_misaligned(self, client) -> None:
        """The file is rewritten every few seconds by another process. Columns of unequal
        length would silently misalign drawdown against the equity it measures."""
        handle, root = client
        run_id = _seed_run_row(root, add_strategy(root))
        RunStore(root).artefact(run_id, PROGRESS_EQUITY_NAME).write_text(
            json.dumps({"ts": [1, 2, 3], "equity": [1.0], "low": [1.0], "high": [1.0]}),
            encoding="utf-8",
        )
        assert handle.get(f"/api/runs/{run_id}/equity").status_code == 404

    def test_unparseable_json_is_refused(self, client) -> None:
        handle, root = client
        run_id = _seed_run_row(root, add_strategy(root))
        RunStore(root).artefact(run_id, PROGRESS_EQUITY_NAME).write_text(
            "{not json", encoding="utf-8"
        )
        assert handle.get(f"/api/runs/{run_id}/equity").status_code == 404


class TestThinExtremes:
    def test_a_spike_survives_thinning(self) -> None:
        """Stride sampling steps straight past the part of an equity curve worth seeing."""
        values = [0.0] * 400
        values[137] = 99.0
        values[288] = -50.0
        keep = _thin_extremes(values, 20)
        assert 137 in keep and 288 in keep
        assert keep[0] == 0 and keep[-1] == 399
        assert list(keep) == sorted(set(keep))

    def test_a_short_series_is_returned_whole(self) -> None:
        assert _thin_extremes([1.0, 2.0, 3.0], 10) == (0, 1, 2)

    def test_an_empty_series_is_empty(self) -> None:
        assert _thin_extremes([], 10) == ()
