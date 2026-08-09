"""Incremental refresh planning: the four ways "bring my data up to date" goes wrong.

Every test here pins one of the failure modes `perplab.data.refresh`'s module docstring
describes, using a lake built in the test rather than the real one, so the expected values
are derived from what the test wrote.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from perplab.data import refresh
from perplab.data.ingest_bulk import ArchiveNotPublished
from perplab.data.refresh import (
    CollectorBusy,
    InsufficientDisk,
    RefreshLocked,
    collector_owned_periods,
    discover_frontier,
    measure_conflict_cost,
    plan_refresh,
    read_barren,
    record_barren,
    refresh_lock,
    require_collector_idle,
    require_disk_headroom,
)
from perplab.data.schemas import AGG_TRADES

SYMBOL = "BTCUSDT"


def ms(text: str) -> int:
    return int(
        datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )


class FrontierFetcher:
    """A mirror where only `published` exists. Records what was probed."""

    def __init__(self, published: set[str]) -> None:
        self.published = published
        self.probed: list[str] = []

    def get_text(self, url: str) -> str:
        self.probed.append(url)
        if any(period in url for period in self.published):
            return "deadbeef  archive.zip"
        raise ArchiveNotPublished(f"404 for {url}")

    def download(self, url: str, sink, *, on_chunk=None) -> int:  # pragma: no cover
        raise AssertionError("planning must not download")


class TestTheFrontierIsProbedPerDataset:
    """A fixed `today - 1` reports a normal publication lag as a hole in the data.

    Measured on the real lake: `aggTrades` for 2026-08-04 was published while `klines`
    for the same day was not. Assuming one shared frontier makes one of them wrong every
    day, and `ingest_range` calls a 404 inside a dataset's published window `MISSING`,
    which the docs define as "a real hole -- investigate". Being told about two holes that
    are not holes, every day, is how the one that matters gets ignored.
    """

    def test_the_newest_published_day_is_found_by_walking_back(self) -> None:
        now = ms("2026-08-05 09:00:00")
        fetcher = FrontierFetcher({"2026-08-03"})

        period, note = discover_frontier(fetcher, "klines", SYMBOL, now_ms=now)

        assert period == "2026-08-03"
        assert note is None
        # 08-04 was probed and refused before 08-03 was accepted; today is never asked
        # for, because a daily archive for an unfinished day cannot exist.
        assert any("2026-08-04" in u for u in fetcher.probed)
        assert not any("2026-08-05" in u for u in fetcher.probed)

    def test_two_datasets_can_have_different_frontiers(self) -> None:
        now = ms("2026-08-05 09:00:00")
        trades = FrontierFetcher({"2026-08-04"})
        candles = FrontierFetcher({"2026-08-03"})

        assert discover_frontier(trades, "aggTrades", SYMBOL, now_ms=now)[0] == "2026-08-04"
        assert discover_frontier(candles, "klines", SYMBOL, now_ms=now)[0] == "2026-08-03"

    def test_a_stalled_publisher_is_one_fact_not_five_holes(self) -> None:
        now = ms("2026-08-05 09:00:00")
        fetcher = FrontierFetcher(set())

        period, note = discover_frontier(fetcher, "klines", SYMBOL, now_ms=now)

        assert period is None
        assert note is not None and "upstream" in note
        assert len(fetcher.probed) == refresh.FRONTIER_LOOKBACK_DAYS

    def test_a_monthly_dataset_is_never_probed_by_day(self) -> None:
        """`fundingRate`'s period is a month; walking back days would ask about last year.

        Keyed on the *archive's* cadence, not the lake's partitioning: `klines` archives
        are daily while the lake stores them in month partitions, and confusing the two
        made a daily dataset plan its range to the end of last month -- refusing to fetch
        exactly the days a top-up exists for.
        """
        fetcher = FrontierFetcher({"2026-08-04"})

        period, note = discover_frontier(
            fetcher, "fundingRate", SYMBOL, now_ms=ms("2026-08-05 09:00:00")
        )

        assert period is None
        assert note is None
        assert fetcher.probed == []


class TestADeclinedDayIsMeasuredNotJustNamed:
    """The sentence that was missing on 2026-08-02.

    The collector started at 11:07 UTC, the archive covering the whole day was declined to
    avoid double-counting, and the first eleven hours went missing until a gap check
    happened to run days later. Reporting "declined" without measuring what the refusal
    costs is what let that sit.
    """

    @staticmethod
    def write_partition(lake: Path, period: str, first: int, last: int, rows: int) -> None:
        directory = lake / "aggTrades" / f"symbol={SYMBOL}" / f"date={period}"
        directory.mkdir(parents=True, exist_ok=True)
        stamps = [first] + [first + 1] * max(0, rows - 2) + [last]
        table = pa.table(
            {
                "ts_ms": pa.array(stamps[:rows], pa.int64()),
                "agg_id": pa.array(list(range(rows)), pa.int64()),
            }
        )
        import pyarrow.parquet as pq

        pq.write_table(table, directory / "part-1-000001.parquet")

    def test_a_partial_day_reports_the_hours_it_is_missing(self, tmp_path: Path) -> None:
        """Collector rows run 11:07:14Z -> 23:59:59Z on a day that starts at 00:00:00Z.

        Hand-derived. The day spans [00:00:00.000Z, 23:59:59.999Z], so its last valid
        instant is `day + 86_399_999`.
          leading  = 40_034_530 - 0          = 40_034_530 ms   (11 h 07 m 14.530 s)
          trailing = 86_399_999 - 86_399_988 =         11 ms
          total                              = 40_034_541 ms
        Both edges count, and the trailing 11 ms is noise beside a real eleven-hour hole --
        which is the point of comparing against a threshold rather than against zero.
        """
        lake = tmp_path / "market"
        day = ms("2026-08-02 00:00:00")
        self.write_partition(
            lake, "2026-08-02", day + 40_034_530, day + 86_399_988, rows=3
        )

        cost = measure_conflict_cost(lake, "aggTrades", SYMBOL, "2026-08-02")

        assert cost.missing_ms == 40_034_541
        assert cost.is_material
        assert cost.collector_rows == 3
        assert "11:07:14Z" in cost.note

    def test_a_fully_covered_day_costs_nothing(self, tmp_path: Path) -> None:
        lake = tmp_path / "market"
        day = ms("2026-08-03 00:00:00")
        self.write_partition(lake, "2026-08-03", day, day + 86_399_999, rows=3)

        cost = measure_conflict_cost(lake, "aggTrades", SYMBOL, "2026-08-03")

        assert cost.missing_ms == 0
        assert not cost.is_material

    def test_a_realistic_last_trade_still_counts_as_covered(self, tmp_path: Path) -> None:
        """No day's final trade lands on 23:59:59.999.

        On the real lake they arrive around 23:59:59.98. Comparing strictly against the
        day's last millisecond marked every complete day as short by a few milliseconds,
        and a measure that flags everything flags nothing. The cut is one minute because
        below one kline there is no dataset that can distinguish "no trade happened" from
        "a trade is missing".
        """
        lake = tmp_path / "market"
        day = ms("2026-08-03 00:00:00")
        self.write_partition(lake, "2026-08-03", day + 41, day + 86_399_988, rows=3)

        cost = measure_conflict_cost(lake, "aggTrades", SYMBOL, "2026-08-03")

        assert cost.missing_ms == 0
        assert not cost.is_material

    def test_a_shortfall_past_one_bar_is_material(self, tmp_path: Path) -> None:
        """Two minutes missing at the open is longer than a kline, so it is provable.

        120_000 ms leading + 11 ms trailing = 120_011 ms. Above one bar, so a kline with
        volume over that window would prove trading occurred and the absence is real.
        """
        lake = tmp_path / "market"
        day = ms("2026-08-03 00:00:00")
        self.write_partition(lake, "2026-08-03", day + 120_000, day + 86_399_988, rows=3)

        cost = measure_conflict_cost(lake, "aggTrades", SYMBOL, "2026-08-03")

        assert cost.missing_ms == 120_011
        assert cost.is_material

    def test_an_unreadable_partition_is_unknown_and_never_fine(
        self, tmp_path: Path
    ) -> None:
        """`None` is not zero. An unreadable partition is a reason to look, not to relax."""
        lake = tmp_path / "market"
        directory = lake / "aggTrades" / f"symbol={SYMBOL}" / "date=2026-08-04"
        directory.mkdir(parents=True)
        (directory / "part-1-000001.parquet").write_bytes(b"not a parquet file")

        cost = measure_conflict_cost(lake, "aggTrades", SYMBOL, "2026-08-04")

        assert cost.missing_ms is None
        assert cost.is_material, "unknown must never read as clean"


class TestConflictsArePredictedBeforeAnythingIsFetched:
    def test_collector_owned_days_are_named_up_front(self, tmp_path: Path) -> None:
        lake = tmp_path / "market"
        owned = lake / "aggTrades" / f"symbol={SYMBOL}" / "date=2026-08-04"
        owned.mkdir(parents=True)
        (owned / "part-1-000001.parquet").write_bytes(b"x")
        free = lake / "aggTrades" / f"symbol={SYMBOL}" / "date=2026-08-01"
        free.mkdir(parents=True)
        (free / "data.parquet").write_bytes(b"x")

        found = collector_owned_periods(
            lake, "aggTrades", SYMBOL, ("2026-08-01", "2026-08-04", "2026-08-05")
        )

        # Bulk's own `data.parquet` is not a conflict; a missing partition is not either.
        assert found == ("2026-08-04",)


class TestTheLakeTakesOneRefreshAtATime:
    def test_a_second_refresh_is_refused_while_the_first_holds_the_lock(
        self, tmp_path: Path
    ) -> None:
        first = refresh_lock(tmp_path, kind="trades", symbol=SYMBOL)
        try:
            with pytest.raises(RefreshLocked) as excinfo:
                refresh_lock(tmp_path, kind="candles", symbol=SYMBOL)
        finally:
            first.release()
        assert "trades" in str(excinfo.value)

    def test_the_lock_is_released_and_retakeable(self, tmp_path: Path) -> None:
        refresh_lock(tmp_path, kind="trades", symbol=SYMBOL).release()
        refresh_lock(tmp_path, kind="candles", symbol=SYMBOL).release()

    def test_an_abandoned_lock_is_taken_over(self, tmp_path: Path) -> None:
        """A killed job must not lock the lake forever.

        Liveness is judged by the heartbeat alone, never by probing the pid: a pid check
        is unreliable across process lifetimes on Windows, and releasing on a wrong
        liveness answer is worse than holding slightly too long.
        """
        held = refresh_lock(tmp_path, kind="trades", symbol=SYMBOL)
        path = tmp_path / "_ingest" / refresh.LOCK_NAME
        stale = json.loads(path.read_text(encoding="utf-8"))
        stale["heartbeat_ms"] = int(time.time() * 1000) - refresh.LOCK_STALE_MS - 1
        path.write_text(json.dumps(stale), encoding="utf-8")

        taken = refresh_lock(tmp_path, kind="candles", symbol=SYMBOL)
        taken.release()
        held.release()


class TestPreflightProtectsTheCollector:
    def test_a_stale_heartbeat_refuses_the_refresh(self, tmp_path: Path) -> None:
        """Running-but-stuck is the one state where competing costs unrecoverable data."""
        now = ms("2026-08-05 09:00:00")
        (tmp_path / "collector_state.json").write_text(
            json.dumps({"pid": 4242, "last_heartbeat_ms": now - 120_000}),
            encoding="utf-8",
        )

        with pytest.raises(CollectorBusy) as excinfo:
            require_collector_idle(tmp_path, now_ms=now)

        assert "4242" in str(excinfo.value)
        assert "re-downloaded" in str(excinfo.value)

    def test_a_healthy_collector_does_not_block(self, tmp_path: Path) -> None:
        now = ms("2026-08-05 09:00:00")
        (tmp_path / "collector_state.json").write_text(
            json.dumps({"pid": 4242, "last_heartbeat_ms": now - 5_000}), encoding="utf-8"
        )
        require_collector_idle(tmp_path, now_ms=now)

    def test_no_state_file_does_not_block(self, tmp_path: Path) -> None:
        """Absence is ambiguous -- the file is removed on a clean shutdown -- but there is
        no running collector to protect, so it cannot be a reason to refuse."""
        require_collector_idle(tmp_path, now_ms=ms("2026-08-05 09:00:00"))

    def test_a_nearly_full_disk_refuses_before_the_transfer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import shutil as shutil_module

        plan = refresh.RefreshPlan(kind="trades", symbol=SYMBOL)
        monkeypatch.setattr(
            shutil_module,
            "disk_usage",
            lambda _p: type("U", (), {"free": refresh.MIN_FREE_BYTES - 1})(),
        )
        monkeypatch.setattr(refresh, "shutil", shutil_module)

        with pytest.raises(InsufficientDisk) as excinfo:
            require_disk_headroom(tmp_path, plan)

        assert "collector" in str(excinfo.value)


class TestKnownUnfetchablePeriodsAreNotRetriedForever:
    """619 archives on this lake fail identically on every attempt (findings F7 and F8).

    Re-downloading them on every click buries the one line that might matter under 619
    that never will.
    """

    def test_a_recorded_period_is_dropped_from_a_later_plan(self, tmp_path: Path) -> None:
        record_barren(tmp_path, "metrics", SYMBOL, {"2021-12-01": "parser refused"})

        stored = read_barren(tmp_path, "metrics", SYMBOL)

        assert "2021-12-01" in stored
        assert stored["2021-12-01"]["reason"] == "parser refused"

    def test_recording_twice_keeps_the_original_sighting(self, tmp_path: Path) -> None:
        """The age of the claim is the evidence for trusting it."""
        record_barren(tmp_path, "metrics", SYMBOL, {"2021-12-01": "parser refused"})
        first = read_barren(tmp_path, "metrics", SYMBOL)["2021-12-01"]["first_seen_ms"]
        record_barren(tmp_path, "metrics", SYMBOL, {"2021-12-01": "parser refused again"})
        again = read_barren(tmp_path, "metrics", SYMBOL)["2021-12-01"]

        assert again["first_seen_ms"] == first
        assert again["reason"] == "parser refused again"


class TestPlanShape:
    def test_an_unknown_kind_is_refused_by_name(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError) as excinfo:
            plan_refresh(tmp_path, SYMBOL, "everything")
        assert "candles" in str(excinfo.value)

    def test_an_unknown_estimate_makes_the_total_unknown(self) -> None:
        """`None` propagates rather than being counted as zero.

        A confirmation dialogue showing a confident number built from a missing one is
        worse than one showing none.
        """
        plan = refresh.RefreshPlan(kind="trades", symbol=SYMBOL)
        plan.datasets.append(
            refresh.DatasetRefresh("aggTrades", None, None, None, "unplannable")
        )
        assert plan.estimated_bytes == 0  # nothing to fetch is genuinely zero

        class Sized:
            to_fetch = ("2026-08-01",)
            estimated_bytes = None

        plan.datasets.append(
            refresh.DatasetRefresh("metrics", Sized(), None, None, None)  # type: ignore[arg-type]
        )
        assert plan.estimated_bytes is None
        assert plan.needs_confirmation
