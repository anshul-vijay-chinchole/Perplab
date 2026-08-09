"""Tests for the dataset manifest (spec 4.6).

The hash tests carry the weight here. A fingerprint that moves when nothing moved is worse
than no fingerprint: it teaches whoever sees the warning to click past it, and the next
warning -- the real one -- gets clicked past too. So the stability tests (recompute with no
changes, path normalisation) matter at least as much as the sensitivity tests (size, mtime).

The fill-model tests use the real finding F1 dates rather than round numbers, because the
boundary is the thing that is easy to get wrong by one day and impossible to notice: an
off-by-one there silently promotes a run to `BOOK_TICKER` on a day the exchange never
published a book.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from perplab.data.gaps import Gap, GapKind, detect_gaps
from perplab.data.manifest import (
    MANIFEST_DATASETS,
    MANIFEST_VERSION,
    CoverageError,
    DatasetEntry,
    FileRecord,
    Manifest,
    ManifestError,
    build_manifest,
    dataset_covers,
    derive_fill_model_tier,
    diff_manifests,
    fingerprint,
    fingerprint_payload,
    lake_relative_path,
    market_root,
    read_manifest,
    resolve_reference,
    scan_dataset,
    unexplained_gap_ms,
    write_manifest,
)
from perplab.data.schemas import SCHEMAS, layout_for, partition_key
from perplab.data.writer import ParquetBufferedWriter

_MS_PER_DAY = 86_400_000


# --------------------------------------------------------------------------------------
# Fixture lake
# --------------------------------------------------------------------------------------


def _days_from_civil(year: int, month: int, day: int) -> int:
    """Days since the Unix epoch, by integer calendar arithmetic.

    The test suite needs to name real dates (finding F1's boundaries) without going through
    a naive `datetime`, whose `.timestamp()` would apply this machine's local zone and shift
    every fixture partition by hours. Guarded by `test_calendar_helper_agrees_with_schemas`.
    """
    y = year - (1 if month <= 2 else 0)
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    mp = month - 3 if month > 2 else month + 9
    doy = (153 * mp + 2) // 5 + day - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146_097 + doe - 719_468


def _ms(year: int, month: int, day: int, hour: int = 12) -> int:
    return (_days_from_civil(year, month, day) * 86_400 + hour * 3_600) * 1_000


def _midnights(start_ms: int, end_ms: int) -> list[int]:
    """Every UTC midnight in an inclusive range."""
    day = start_ms - start_ms % _MS_PER_DAY
    last = end_ms - end_ms % _MS_PER_DAY
    out: list[int] = []
    while day <= last:
        out.append(day)
        day += _MS_PER_DAY
    return out


def _row(schema: pa.Schema, time_column: str, ts_ms: int) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for field in schema:
        if pa.types.is_list(field.type):
            row[field.name] = [1, 2, 3]
        elif pa.types.is_boolean(field.type):
            row[field.name] = True
        elif pa.types.is_string(field.type):
            row[field.name] = "x"
        else:
            row[field.name] = 1
    row[time_column] = ts_ms
    return row


def fill(
    root: Path,
    dataset: str,
    symbol: str,
    timestamps: list[int],
    *,
    rows_each: int = 1,
) -> None:
    """Write real Parquet into the lake, through the real writer.

    Deliberately not hand-built paths: the manifest has to look exactly where the writer
    writes, and a fixture that hardcodes the layout would keep passing after a layout change
    that broke the manifest in production.
    """
    schema = SCHEMAS[dataset]
    time_column = layout_for(dataset).time_column
    writer = ParquetBufferedWriter(market_root(root), dataset, schema, symbol=symbol)
    for ts in timestamps:
        for i in range(rows_each):
            writer.append(_row(schema, time_column, ts + i))
    writer.flush()


def fill_days(
    root: Path,
    dataset: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    rows_each: int = 1,
) -> None:
    fill(root, dataset, symbol, _midnights(start_ms, end_ms), rows_each=rows_each)


def snapshot(root: Path, kind: str, date: str) -> Path:
    """A dated reference snapshot, named the way `reference.py` names them."""
    directory = root / "reference" / kind
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{date}.json"
    path.write_text(json.dumps({"serverTime": 0}), encoding="utf-8")
    return path


def test_calendar_helper_agrees_with_schemas() -> None:
    """If the test's own calendar is wrong, every date-boundary assertion below is a lie."""
    for year, month, day in [
        (1970, 1, 1),
        (2023, 5, 16),
        (2024, 2, 29),
        (2024, 3, 30),
        (2026, 8, 1),
    ]:
        assert partition_key(_ms(year, month, day)) == f"{year:04d}-{month:02d}-{day:02d}"


# --------------------------------------------------------------------------------------
# Path normalisation
# --------------------------------------------------------------------------------------


class TestLakeRelativePath:
    def test_is_relative_to_the_root(self, tmp_path: Path) -> None:
        path = tmp_path / "market" / "aggTrades" / "symbol=BTCUSDT" / "x.parquet"
        assert lake_relative_path(tmp_path, path) == (
            "market/aggTrades/symbol=BTCUSDT/x.parquet"
        )

    def test_uses_forward_slashes_on_every_platform(self, tmp_path: Path) -> None:
        """The same lake on a shared drive must hash identically from Windows and Linux."""
        path = tmp_path.joinpath("market", "klines", "symbol=BTCUSDT", "x.parquet")
        relative = lake_relative_path(tmp_path, path)
        assert "\\" not in relative
        assert relative.count("/") == 3

    def test_does_not_leak_the_machine(self, tmp_path: Path) -> None:
        relative = lake_relative_path(tmp_path, tmp_path / "market" / "x.parquet")
        assert str(tmp_path) not in relative
        assert not Path(relative).is_absolute()

    def test_outside_the_root_raises(self, tmp_path: Path) -> None:
        """Falling back to an absolute path would poison the hash with a machine name."""
        with pytest.raises(ManifestError, match="not inside the userdata root"):
            lake_relative_path(tmp_path / "lake", tmp_path / "elsewhere" / "x.parquet")


# --------------------------------------------------------------------------------------
# The fingerprint itself
# --------------------------------------------------------------------------------------


class TestFingerprint:
    def test_serialisation_is_exactly_this(self) -> None:
        """Pins the byte format. Reformatting it invalidates every stored manifest."""
        records = [
            FileRecord("market/a/x.parquet", 10, 111),
            FileRecord("market/a/y.parquet", 20, 222),
        ]
        assert fingerprint_payload(records) == (
            b"market/a/x.parquet\x0010\x00111\n" b"market/a/y.parquet\x0020\x00222\n"
        )

    def test_input_order_does_not_matter(self) -> None:
        """Spec 4.6 hashes a *sorted* list; directory iteration order must not leak in."""
        records = [
            FileRecord("market/a/y.parquet", 20, 222),
            FileRecord("market/a/x.parquet", 10, 111),
            FileRecord("market/a/z.parquet", 30, 333),
        ]
        assert fingerprint(records) == fingerprint(sorted(records, reverse=True))

    def test_hash_is_stable_for_identical_input(self) -> None:
        records = [FileRecord("market/a/x.parquet", 10, 111)]
        assert fingerprint(records) == fingerprint(list(records))

    def test_size_change_moves_the_hash(self) -> None:
        before = [FileRecord("market/a/x.parquet", 10, 111)]
        after = [FileRecord("market/a/x.parquet", 11, 111)]
        assert fingerprint(before) != fingerprint(after)

    def test_mtime_change_moves_the_hash(self) -> None:
        before = [FileRecord("market/a/x.parquet", 10, 111)]
        after = [FileRecord("market/a/x.parquet", 10, 112)]
        assert fingerprint(before) != fingerprint(after)

    def test_path_change_moves_the_hash(self) -> None:
        before = [FileRecord("market/a/x.parquet", 10, 111)]
        after = [FileRecord("market/a/z.parquet", 10, 111)]
        assert fingerprint(before) != fingerprint(after)

    def test_fields_cannot_be_confused_across_the_separator(self) -> None:
        """`a` + size 1 must not serialise the same as `a\\x001` + something else."""
        assert fingerprint([FileRecord("a", 1, 2)]) != fingerprint(
            [FileRecord("a\x001", 2, 0)]
        )

    def test_empty_dataset_hashes_the_empty_string(self) -> None:
        assert fingerprint([]) == (
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )


# --------------------------------------------------------------------------------------
# Scanning
# --------------------------------------------------------------------------------------


class TestScanDataset:
    def test_records_relative_forward_slashed_paths(self, tmp_path: Path) -> None:
        fill_days(tmp_path, "aggTrades", "BTCUSDT", _ms(2024, 1, 1), _ms(2024, 1, 1))
        records = scan_dataset(
            tmp_path, "aggTrades", ["BTCUSDT"], _ms(2024, 1, 1), _ms(2024, 1, 2)
        )
        assert len(records) == 1
        assert records[0].path.startswith(
            "market/aggTrades/symbol=BTCUSDT/date=2024-01-01/"
        )
        assert records[0].size > 0

    def test_ignores_partitions_outside_the_range(self, tmp_path: Path) -> None:
        fill_days(tmp_path, "aggTrades", "BTCUSDT", _ms(2024, 1, 1), _ms(2024, 1, 5))
        records = scan_dataset(
            tmp_path, "aggTrades", ["BTCUSDT"], _ms(2024, 1, 2), _ms(2024, 1, 3)
        )
        assert len(records) == 2

    def test_ignores_other_symbols(self, tmp_path: Path) -> None:
        fill_days(tmp_path, "aggTrades", "BTCUSDT", _ms(2024, 1, 1), _ms(2024, 1, 1))
        fill_days(tmp_path, "aggTrades", "ETHUSDT", _ms(2024, 1, 1), _ms(2024, 1, 1))
        records = scan_dataset(
            tmp_path, "aggTrades", ["BTCUSDT"], _ms(2024, 1, 1), _ms(2024, 1, 2)
        )
        assert len(records) == 1
        assert "symbol=BTCUSDT" in records[0].path

    def test_skips_unpublished_and_foreign_files(self, tmp_path: Path) -> None:
        """A half-written `.tmp` would record a size that is obsolete before it is stored."""
        fill_days(tmp_path, "aggTrades", "BTCUSDT", _ms(2024, 1, 1), _ms(2024, 1, 1))
        partition = (
            market_root(tmp_path) / "aggTrades" / "symbol=BTCUSDT" / "date=2024-01-01"
        )
        (partition / ".part-999.parquet.tmp").write_bytes(b"partial")
        (partition / "notes.txt").write_text("not data", encoding="utf-8")

        records = scan_dataset(
            tmp_path, "aggTrades", ["BTCUSDT"], _ms(2024, 1, 1), _ms(2024, 1, 2)
        )
        assert len(records) == 1
        assert records[0].path.endswith(".parquet")

    def test_funding_has_no_time_component(self, tmp_path: Path) -> None:
        """Granularity `none` resolves to the whole symbol; that must not crash or miss."""
        fill(tmp_path, "funding", "BTCUSDT", [_ms(2021, 6, 1)])
        records = scan_dataset(
            tmp_path, "funding", ["BTCUSDT"], _ms(2024, 1, 1), _ms(2024, 1, 2)
        )
        assert len(records) == 1
        assert records[0].path.startswith("market/funding/symbol=BTCUSDT/")


# --------------------------------------------------------------------------------------
# Building
# --------------------------------------------------------------------------------------


def _basic_lake(tmp_path: Path) -> tuple[int, int]:
    """aggTrades and klines over 2024-01-01..2024-01-03, one symbol."""
    start, end = _ms(2024, 1, 1), _ms(2024, 1, 3)
    fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end, rows_each=5)
    fill_days(tmp_path, "klines", "BTCUSDT", start, end, rows_each=2)
    return start, end


class TestBuildManifest:
    def test_shape_matches_spec_4_6(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        obj = build_manifest(tmp_path, ["BTCUSDT"], start, end).to_json()

        assert obj["symbols"] == ["BTCUSDT"]
        assert obj["range"] == {"start_ms": start, "end_ms": end}
        assert set(obj["datasets"]) == {"aggTrades", "klines_1m"}
        assert set(obj["datasets"]["aggTrades"]) == {"files", "rows", "sha256"}
        assert set(obj["reference"]) == {
            "exchangeInfo_snapshot",
            "leverageBracket_snapshot",
        }
        assert obj["gaps"] == []
        assert obj["fill_model_tier"] == "TRADE_ONLY"
        assert obj["manifest_version"] == MANIFEST_VERSION

    def test_counts_files_and_rows(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end)

        # Three daily partitions, five rows each.
        assert manifest.datasets["aggTrades"].files == 3
        assert manifest.datasets["aggTrades"].rows == 15
        # Klines partition monthly, so three days of bars land in one file.
        assert manifest.datasets["klines_1m"].files == 1
        assert manifest.datasets["klines_1m"].rows == 6

    def test_klines_key_carries_its_interval(self, tmp_path: Path) -> None:
        """Spec 4.6's own example is `klines_1m`; the interval is part of the identity."""
        start, end = _basic_lake(tmp_path)
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        assert "klines_1m" in manifest.datasets
        assert "klines" not in manifest.datasets

    def test_absent_datasets_are_omitted_not_zeroed(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        assert "depth20" not in manifest.datasets
        assert "bookTicker" not in manifest.datasets

    def test_collector_events_never_appear(self, tmp_path: Path) -> None:
        """It has no symbol level and grows every minute; it would break every diff."""
        assert "collectorEvents" not in MANIFEST_DATASETS

    def test_recomputation_with_no_changes_is_identical(self, tmp_path: Path) -> None:
        """The property the whole mechanism rests on."""
        start, end = _basic_lake(tmp_path)
        first = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        second = build_manifest(tmp_path, ["BTCUSDT"], start, end)

        assert first == second
        assert first.to_json() == second.to_json()
        assert not diff_manifests(first, second).changed

    def test_touching_a_file_moves_the_dataset_hash(self, tmp_path: Path) -> None:
        """A re-ingest that happens to produce identical bytes still has to be visible."""
        start, end = _basic_lake(tmp_path)
        before = build_manifest(tmp_path, ["BTCUSDT"], start, end)

        target = next(
            (market_root(tmp_path) / "aggTrades").rglob("*.parquet")
        )
        stat = target.stat()
        os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

        after = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        assert after.datasets["aggTrades"].sha256 != before.datasets["aggTrades"].sha256
        assert after.datasets["aggTrades"].files == before.datasets["aggTrades"].files
        assert after.datasets["aggTrades"].rows == before.datasets["aggTrades"].rows
        # An untouched dataset must not move with it.
        assert after.datasets["klines_1m"] == before.datasets["klines_1m"]

    def test_rewriting_a_partition_larger_moves_files_rows_and_hash(
        self, tmp_path: Path
    ) -> None:
        start, end = _basic_lake(tmp_path)
        before = build_manifest(tmp_path, ["BTCUSDT"], start, end)

        fill_days(tmp_path, "aggTrades", "BTCUSDT", _ms(2024, 1, 2), _ms(2024, 1, 2))

        after = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        assert after.datasets["aggTrades"].files == 4
        assert after.datasets["aggTrades"].rows == 16
        assert after.datasets["aggTrades"].sha256 != before.datasets["aggTrades"].sha256

    def test_hash_does_not_depend_on_where_the_lake_lives(self, tmp_path: Path) -> None:
        """Two copies of the same lake in different directories must agree.

        This is the whole point of recording relative paths. If it fails, every user who
        moves their lake gets told their data changed.
        """
        import shutil

        one = tmp_path / "one"
        two = tmp_path / "a-much-longer-directory-name"
        one.mkdir()
        start, end = _basic_lake(one)
        shutil.copytree(one, two)
        for source in one.rglob("*.parquet"):
            stat = source.stat()
            mirror = two / source.relative_to(one)
            os.utime(mirror, ns=(stat.st_atime_ns, stat.st_mtime_ns))

        assert (
            build_manifest(one, ["BTCUSDT"], start, end).datasets["aggTrades"].sha256
            == build_manifest(two, ["BTCUSDT"], start, end).datasets["aggTrades"].sha256
        )

    def test_gaps_are_recorded_verbatim(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        gaps = [{"dataset": "aggTrades", "symbol": "BTCUSDT", "start_ms": 1, "end_ms": 2}]
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end, gaps=gaps)
        assert list(manifest.gaps) == gaps

    def test_unserialisable_gaps_fail_at_build_time(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        with pytest.raises(ManifestError, match="JSON-serialisable"):
            build_manifest(tmp_path, ["BTCUSDT"], start, end, gaps=[{"x": object()}])


class TestGapsFromTheGapDetector:
    """The seam between `gaps.py` and this module, which did not previously connect.

    `Gap` is a frozen dataclass, so `build_manifest(..., gaps=detect_gaps(...).gaps)`
    raised "gap report is not JSON-serialisable". Both modules were written in parallel and
    each was individually right; the only thing missing was an agreed shape.
    """

    def _gap(self, dataset: str, start_ms: int, end_ms: int, **kwargs: Any) -> Gap:
        return Gap(
            dataset=dataset,
            symbol="BTCUSDT",
            start_ms=start_ms,
            end_ms=end_ms,
            kind=GapKind.TICK_SILENCE,
            detail="silence",
            **kwargs,
        )

    def test_gap_objects_are_accepted_and_stored_as_json(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        gap = self._gap("aggTrades", start, start + 1000)
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end, gaps=[gap])
        assert manifest.gaps[0]["dataset"] == "aggTrades"
        assert manifest.gaps[0]["kind"] == "TICK_SILENCE"
        assert json.loads(json.dumps(list(manifest.gaps))) == list(manifest.gaps)

    def test_a_whole_report_can_be_passed_straight_in(self, tmp_path: Path) -> None:
        """A caller has a `GapReport`, not a `Gap` tuple; taking either avoids one more
        thing to get wrong, and passing the report by mistake used to store one opaque
        object rather than raising."""
        start, end = _basic_lake(tmp_path)
        report = detect_gaps(
            market_root(tmp_path), "BTCUSDT", start, end, datasets=("klines",)
        )
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end, gaps=report)
        assert len(manifest.gaps) == len(report.gaps)

    def test_an_unexplained_gap_demotes_the_tier(self, tmp_path: Path) -> None:
        """Coverage is judged per partition, so a day with one part-file counts as covered
        even if the recorder was down for nine hours of it. The gap report is the only
        thing that knows better."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)

        assert derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end) == "BOOK_WALK"

        holed = self._gap("depth20", start + 1, start + 3_600_000)
        assert (
            derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end, gaps=[holed])
            == "TRADE_ONLY"
        )

    def test_an_explained_gap_does_not_demote(self, tmp_path: Path) -> None:
        """An explained outage is still missing data and still recorded, but its cause is
        known. Re-pricing every fill in the run off the collector's uptime log would make
        the tier depend on the wrong thing."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)

        explained = self._gap(
            "depth20", start + 1, start + 3_600_000, explanation="RESTART at 03:00"
        )
        assert (
            derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end, gaps=[explained])
            == "BOOK_WALK"
        )

    def test_a_gap_outside_the_range_does_not_demote(self, tmp_path: Path) -> None:
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)

        # Ends exactly at the range start: it removed nothing from the range.
        before = self._gap("depth20", start - 3_600_000, start)
        assert (
            derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end, gaps=[before])
            == "BOOK_WALK"
        )

    def test_a_gap_for_another_symbol_does_not_demote(self, tmp_path: Path) -> None:
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)

        elsewhere = Gap(
            dataset="depth20",
            symbol="ETHUSDT",
            start_ms=start + 1,
            end_ms=start + 3_600_000,
            kind=GapKind.TICK_SILENCE,
            detail="silence",
        )
        assert (
            derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end, gaps=[elsewhere])
            == "BOOK_WALK"
        )

    def test_a_symbolless_gap_counts_for_every_symbol(self, tmp_path: Path) -> None:
        """`collectorEvents` describes the process. If it was not running, it was not
        running for all of them at once."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)

        outage = Gap(
            dataset="depth20",
            symbol=None,
            start_ms=start + 1,
            end_ms=start + 3_600_000,
            kind=GapKind.COLLECTOR_OUTAGE,
            detail="down",
        )
        assert (
            derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end, gaps=[outage])
            == "TRADE_ONLY"
        )

    def test_the_demotion_is_flagged_on_the_manifest(self, tmp_path: Path) -> None:
        """Spec 4.2: a fill model downgrade must never happen invisibly. The flag is what
        separates "never ingested" from "ingested and holed" -- same tier, different fix."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)
        fill_days(tmp_path, "klines", "BTCUSDT", start, end)

        clean = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        assert clean.fill_model_tier == "BOOK_WALK"
        assert "FILL_TIER_LIMITED_BY_GAPS" not in clean.flags

        holed = build_manifest(
            tmp_path,
            ["BTCUSDT"],
            start,
            end,
            gaps=[self._gap("depth20", start + 1, start + 3_600_000)],
        )
        assert holed.fill_model_tier == "TRADE_ONLY"
        assert "FILL_TIER_LIMITED_BY_GAPS" in holed.flags

    def test_a_holed_kline_range_still_gets_a_tier(self, tmp_path: Path) -> None:
        """Gaps demote *between* tiers and do not reach the floor: there is nothing below
        `BAR_CLOSE` to demote to, and refusing to name one would block the very manifest
        that records the holes."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "klines", "BTCUSDT", start, end)
        manifest = build_manifest(
            tmp_path,
            ["BTCUSDT"],
            start,
            end,
            gaps=[self._gap("klines", start + 1, start + 3_600_000)],
        )
        assert manifest.fill_model_tier == "BAR_CLOSE"
        assert "LOW_FIDELITY" in manifest.flags

    def test_an_unrecognised_gap_shape_is_recorded_but_ignored(self, tmp_path: Path) -> None:
        """`gaps` is a free-form spec 4.6 field and a reloaded manifest may carry a shape
        this build does not know. Refusing to build would block the recomputation the diff
        exists to perform."""
        start, end = _basic_lake(tmp_path)
        manifest = build_manifest(
            tmp_path, ["BTCUSDT"], start, end, gaps=[{"legacy": "shape"}, "a string"]
        )
        assert list(manifest.gaps) == [{"legacy": "shape"}, "a string"]

    def test_a_corrupt_file_raises_rather_than_counting_zero(self, tmp_path: Path) -> None:
        """A truncated Parquet reads as *short*, not broken; a silent 0 hides real loss."""
        start, end = _basic_lake(tmp_path)
        partition = (
            market_root(tmp_path) / "aggTrades" / "symbol=BTCUSDT" / "date=2024-01-01"
        )
        (partition / "part-truncated.parquet").write_bytes(b"not parquet at all")
        with pytest.raises(ManifestError, match="truncated or"):
            build_manifest(tmp_path, ["BTCUSDT"], start, end)

    def test_no_symbols_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="at least one"):
            build_manifest(tmp_path, [], 0, 1)

    def test_inverted_range_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="precedes start"):
            build_manifest(tmp_path, ["BTCUSDT"], _ms(2024, 1, 2), _ms(2024, 1, 1))

    def test_symbol_that_would_escape_its_partition_raises(self, tmp_path: Path) -> None:
        """Refused by the one shared `schemas.normalise_symbol`, not by a local check.

        There were three symbol conventions before integration -- this module took symbols
        verbatim, `query` uppercased them, `ingest_bulk` uppercased them somewhere else --
        so a lake written as `symbol=BTCUSDT` could be manifested as `symbol=btcusdt` and
        silently found empty.
        """
        with pytest.raises(ValueError, match="implausible symbol"):
            build_manifest(tmp_path, ["BTC/USDT"], _ms(2024, 1, 1), _ms(2024, 1, 1))

    def test_a_lowercase_symbol_finds_the_partition_the_writer_used(
        self, tmp_path: Path
    ) -> None:
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 2)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)
        fill_days(tmp_path, "klines", "BTCUSDT", start, end)

        manifest = build_manifest(tmp_path, ["btcusdt"], start, end)

        assert manifest.symbols == ("BTCUSDT",)
        assert manifest.datasets["aggTrades"].files > 0

    def test_empty_lake_refuses_to_pretend(self, tmp_path: Path) -> None:
        """`BAR_CLOSE` over no data would produce a flat curve that looks like a result."""
        with pytest.raises(CoverageError, match="no fill model is supportable"):
            build_manifest(tmp_path, ["BTCUSDT"], _ms(2024, 1, 1), _ms(2024, 1, 2))


# --------------------------------------------------------------------------------------
# Fill model tier
# --------------------------------------------------------------------------------------


class TestFillModelTier:
    def test_depth20_full_coverage_is_book_walk(self, tmp_path: Path) -> None:
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)
        assert derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end) == "BOOK_WALK"

    def test_one_missing_depth_day_loses_book_walk(self, tmp_path: Path) -> None:
        """Coverage is read off the disk, not assumed from the dataset existing at all."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill(tmp_path, "depth20", "BTCUSDT", [_ms(2026, 8, 1), _ms(2026, 8, 3)])
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)
        assert derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end) == "TRADE_ONLY"

    def test_second_symbol_without_depth_loses_book_walk(self, tmp_path: Path) -> None:
        """A tier granted on one symbol's coverage would book-walk a symbol with no book."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 2)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "ETHUSDT", start, end)
        assert (
            derive_fill_model_tier(tmp_path, ["BTCUSDT", "ETHUSDT"], start, end)
            == "TRADE_ONLY"
        )

    def test_empty_partition_directory_is_not_coverage(self, tmp_path: Path) -> None:
        """A directory left behind by a failed ingest holds no data and must not count."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 2)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)
        for path in (
            market_root(tmp_path) / "depth20" / "symbol=BTCUSDT" / "date=2026-08-02"
        ).glob("*.parquet"):
            path.unlink()
        assert not dataset_covers(tmp_path, "depth20", ["BTCUSDT"], start, end)
        assert derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end) == "TRADE_ONLY"

    def test_book_ticker_plus_agg_trades(self, tmp_path: Path) -> None:
        start, end = _ms(2023, 6, 1), _ms(2023, 6, 3)
        fill_days(tmp_path, "bookTicker", "BTCUSDT", start, end)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)
        assert derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end) == "BOOK_TICKER"

    def test_book_ticker_without_agg_trades_is_not_enough(self, tmp_path: Path) -> None:
        """Spec 4.2's tier requires both; trades are what actually cross the touch."""
        start, end = _ms(2023, 6, 1), _ms(2023, 6, 2)
        fill_days(tmp_path, "bookTicker", "BTCUSDT", start, end)
        fill_days(tmp_path, "klines", "BTCUSDT", start, end)
        assert derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end) == "BAR_CLOSE"

    def test_agg_trades_only_is_trade_only(self, tmp_path: Path) -> None:
        start, end = _ms(2021, 3, 1), _ms(2021, 3, 2)
        fill_days(tmp_path, "aggTrades", "BTCUSDT", start, end)
        assert derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end) == "TRADE_ONLY"

    def test_klines_only_is_bar_close_and_flagged(self, tmp_path: Path) -> None:
        start, end = _ms(2021, 3, 1), _ms(2021, 3, 2)
        fill_days(tmp_path, "klines", "BTCUSDT", start, end)
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        assert manifest.fill_model_tier == "BAR_CLOSE"
        assert "LOW_FIDELITY" in manifest.flags

    def test_nothing_at_all_raises(self, tmp_path: Path) -> None:
        with pytest.raises(CoverageError):
            derive_fill_model_tier(
                tmp_path, ["BTCUSDT"], _ms(2021, 3, 1), _ms(2021, 3, 2)
            )


class TestCoarsePartitionCoverage:
    """Finding M23: klines partition by *month*, so file presence answered "is there a
    file somewhere in this month" while the check's callers asked a day-level question.
    A single day's file asserted a thirty-day range; `tiers._uncovered_bars` fixed this
    for the engine and the manifest kept the weaker check."""

    def test_one_day_of_klines_does_not_cover_a_month(self, tmp_path: Path) -> None:
        start, end = _ms(2021, 3, 1), _ms(2021, 3, 30)
        # One row, one day, one file -- inside the month partition the whole range maps to.
        fill(tmp_path, "klines", "BTCUSDT", [_ms(2021, 3, 1)])
        assert not dataset_covers(tmp_path, "klines", ["BTCUSDT"], start, end)
        with pytest.raises(CoverageError):
            derive_fill_model_tier(tmp_path, ["BTCUSDT"], start, end)

    def test_a_row_on_every_day_does_cover(self, tmp_path: Path) -> None:
        start, end = _ms(2021, 3, 1), _ms(2021, 3, 30)
        fill_days(tmp_path, "klines", "BTCUSDT", start, end)
        assert dataset_covers(tmp_path, "klines", ["BTCUSDT"], start, end)

    def test_a_missing_interior_day_is_not_covered(self, tmp_path: Path) -> None:
        """Bounds alone would pass this lake; the check is per day, like the
        date-partitioned datasets get from their directory tree for free."""
        start, end = _ms(2021, 3, 1), _ms(2021, 3, 5)
        timestamps = [
            ts
            for ts in _midnights(start, end)
            if ts != _ms(2021, 3, 3, hour=0)
        ]
        fill(tmp_path, "klines", "BTCUSDT", timestamps)
        assert not dataset_covers(tmp_path, "klines", ["BTCUSDT"], start, end)

    def test_date_partitioned_datasets_are_unchanged(self, tmp_path: Path) -> None:
        """`depth20` already answers at day level through its directory layout; the
        row-level refinement must not double-charge it."""
        start, end = _ms(2026, 8, 1), _ms(2026, 8, 3)
        fill_days(tmp_path, "depth20", "BTCUSDT", start, end)
        assert dataset_covers(tmp_path, "depth20", ["BTCUSDT"], start, end)


class TestFindingF1Window:
    """Bulk `bookTicker` exists only for 2023-05-16 .. 2024-03-30 (finding F1).

    Both edges are tested at one-day resolution. An off-by-one here would hand a run
    `BOOK_TICKER` fills on a day for which no book was ever published, and nothing
    downstream would contradict it.
    """

    def _lake_at_start(self, tmp_path: Path) -> None:
        fill_days(tmp_path, "aggTrades", "BTCUSDT", _ms(2023, 5, 13), _ms(2023, 5, 20))
        fill_days(tmp_path, "bookTicker", "BTCUSDT", _ms(2023, 5, 16), _ms(2023, 5, 20))

    def _lake_at_end(self, tmp_path: Path) -> None:
        fill_days(tmp_path, "aggTrades", "BTCUSDT", _ms(2024, 3, 26), _ms(2024, 4, 2))
        fill_days(tmp_path, "bookTicker", "BTCUSDT", _ms(2024, 3, 26), _ms(2024, 3, 30))

    def test_inside_the_window_is_book_ticker(self, tmp_path: Path) -> None:
        self._lake_at_start(tmp_path)
        tier = derive_fill_model_tier(
            tmp_path, ["BTCUSDT"], _ms(2023, 5, 16), _ms(2023, 5, 20)
        )
        assert tier == "BOOK_TICKER"

    def test_one_day_before_the_window_degrades(self, tmp_path: Path) -> None:
        self._lake_at_start(tmp_path)
        tier = derive_fill_model_tier(
            tmp_path, ["BTCUSDT"], _ms(2023, 5, 15), _ms(2023, 5, 20)
        )
        assert tier == "TRADE_ONLY"

    def test_the_last_published_day_still_qualifies(self, tmp_path: Path) -> None:
        self._lake_at_end(tmp_path)
        tier = derive_fill_model_tier(
            tmp_path, ["BTCUSDT"], _ms(2024, 3, 26), _ms(2024, 3, 30)
        )
        assert tier == "BOOK_TICKER"

    def test_one_day_after_the_window_degrades(self, tmp_path: Path) -> None:
        self._lake_at_end(tmp_path)
        tier = derive_fill_model_tier(
            tmp_path, ["BTCUSDT"], _ms(2024, 3, 26), _ms(2024, 3, 31)
        )
        assert tier == "TRADE_ONLY"

    def test_a_range_ending_at_midnight_does_not_reach_into_that_day(
        self, tmp_path: Path
    ) -> None:
        """Ranges are half-open, so midnight on the 31st asks for nothing on the 31st.

        This is the convention `gaps.py` filters on and `query.partition_predicate` emits.
        The manifest previously read the end bound as inclusive, which made it demand a
        partition for a day the rest of the pipeline had already excluded -- so every range
        ending on a midnight boundary, which is every range the CLI produces, degraded the
        tier against a lake that was in fact complete.
        """
        self._lake_at_end(tmp_path)
        midnight_after = _ms(2024, 3, 31, hour=0)
        assert (
            derive_fill_model_tier(tmp_path, ["BTCUSDT"], _ms(2024, 3, 26), midnight_after)
            == "BOOK_TICKER"
        )

    def test_one_millisecond_into_the_next_day_does_need_it(self, tmp_path: Path) -> None:
        """The other side of the same boundary: [start, end) including any of the 31st."""
        self._lake_at_end(tmp_path)
        assert (
            derive_fill_model_tier(
                tmp_path, ["BTCUSDT"], _ms(2024, 3, 26), _ms(2024, 3, 31, hour=0) + 1
            )
            == "TRADE_ONLY"
        )

    def test_an_empty_range_is_refused_rather_than_vacuously_covered(
        self, tmp_path: Path
    ) -> None:
        """A zero-length range expects no partition, so every check would pass trivially."""
        self._lake_at_end(tmp_path)
        with pytest.raises(ValueError, match="empty range"):
            derive_fill_model_tier(
                tmp_path, ["BTCUSDT"], _ms(2024, 3, 26), _ms(2024, 3, 26)
            )


# --------------------------------------------------------------------------------------
# Reference snapshots
# --------------------------------------------------------------------------------------


class TestReference:
    def test_resolves_the_snapshot_in_force_not_the_newest(self, tmp_path: Path) -> None:
        """Spec 3.2: a backtest over 2023 must use the 2023 filters."""
        snapshot(tmp_path, "exchangeInfo", "2023-01-05")
        snapshot(tmp_path, "exchangeInfo", "2026-08-01")
        resolved, _, flags = resolve_reference(
            tmp_path, _ms(2023, 3, 1), _ms(2023, 4, 1)
        )
        assert resolved == "2023-01-05"
        assert "FILTERS_APPROXIMATE" not in flags

    def test_no_old_enough_snapshot_is_flagged_not_substituted(
        self, tmp_path: Path
    ) -> None:
        """Today's situation: snapshotting began 2026-08-01, so history has none."""
        snapshot(tmp_path, "exchangeInfo", "2026-08-01")
        resolved, _, flags = resolve_reference(
            tmp_path, _ms(2024, 1, 1), _ms(2024, 6, 1)
        )
        assert resolved is None
        assert "FILTERS_APPROXIMATE" in flags

    def test_flag_reaches_the_manifest(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        snapshot(tmp_path, "exchangeInfo", "2026-08-01")
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        assert manifest.exchange_info_snapshot is None
        assert "FILTERS_APPROXIMATE" in manifest.flags
        assert manifest.to_json()["reference"]["exchangeInfo_snapshot"] is None

    def test_a_lake_with_no_reference_directory_at_all(self, tmp_path: Path) -> None:
        resolved, brackets, flags = resolve_reference(
            tmp_path, _ms(2024, 1, 1), _ms(2024, 1, 2)
        )
        assert resolved is None and brackets is None
        assert "FILTERS_APPROXIMATE" in flags
        assert "BRACKETS_APPROXIMATE" in flags

    def test_brackets_missing_is_its_own_flag(self, tmp_path: Path) -> None:
        """Finding F3: `leverageBracket` needs API keys, so none exist yet."""
        snapshot(tmp_path, "exchangeInfo", "2023-01-05")
        _, brackets, flags = resolve_reference(tmp_path, _ms(2023, 3, 1), _ms(2023, 4, 1))
        assert brackets is None
        assert "BRACKETS_APPROXIMATE" in flags
        assert "FILTERS_APPROXIMATE" not in flags

    def test_filters_changing_mid_range_is_visible(self, tmp_path: Path) -> None:
        """A run that validated every order against the opening snapshot modelled rules
        that stopped being true half way through."""
        snapshot(tmp_path, "exchangeInfo", "2023-01-05")
        snapshot(tmp_path, "exchangeInfo", "2023-06-01")
        resolved, _, flags = resolve_reference(
            tmp_path, _ms(2023, 3, 1), _ms(2023, 9, 1)
        )
        assert resolved == "2023-01-05"
        assert "FILTERS_CHANGED_MID_RANGE" in flags

    def test_no_mid_range_flag_when_the_snapshot_held(self, tmp_path: Path) -> None:
        snapshot(tmp_path, "exchangeInfo", "2023-01-05")
        snapshot(tmp_path, "exchangeInfo", "2023-06-01")
        _, _, flags = resolve_reference(tmp_path, _ms(2023, 2, 1), _ms(2023, 3, 1))
        assert "FILTERS_CHANGED_MID_RANGE" not in flags


# --------------------------------------------------------------------------------------
# Persistence
# --------------------------------------------------------------------------------------


class TestPersistence:
    def test_round_trip(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        manifest = build_manifest(
            tmp_path, ["BTCUSDT"], start, end, gaps=[{"symbol": "BTCUSDT", "ms": 5}]
        )
        path = write_manifest(manifest, tmp_path / "runs" / "r1" / "manifest.json")
        assert read_manifest(path) == manifest

    def test_write_is_atomic(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        target = tmp_path / "runs" / "r1" / "manifest.json"
        write_manifest(manifest, target)
        assert not list(target.parent.glob("*.tmp"))
        assert not list(target.parent.glob(".*"))
        assert json.loads(target.read_text(encoding="utf-8"))["symbols"] == ["BTCUSDT"]

    def test_rewriting_produces_identical_bytes(self, tmp_path: Path) -> None:
        start, end = _basic_lake(tmp_path)
        manifest = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        first = write_manifest(manifest, tmp_path / "a.json").read_bytes()
        second = write_manifest(manifest, tmp_path / "b.json").read_bytes()
        assert first == second

    def test_unknown_version_is_refused(self, tmp_path: Path) -> None:
        """Reading it under the wrong assumptions would report shape drift as data drift."""
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps({"manifest_version": 999}), encoding="utf-8")
        with pytest.raises(ManifestError, match="cannot be read by this build"):
            read_manifest(path)

    def test_missing_version_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps({"symbols": ["BTCUSDT"]}), encoding="utf-8")
        with pytest.raises(ManifestError):
            read_manifest(path)


# --------------------------------------------------------------------------------------
# Diff
# --------------------------------------------------------------------------------------


def _manifest(**overrides: Any) -> Manifest:
    base: dict[str, Any] = {
        "symbols": ("BTCUSDT",),
        "start_ms": _ms(2024, 1, 1, hour=0),
        "end_ms": _ms(2024, 1, 3, hour=0),
        "datasets": {
            "aggTrades": DatasetEntry(files=366, rows=891_203_847, sha256="9c11" + "0" * 60),
            "klines_1m": DatasetEntry(files=24, rows=527_040, sha256="a3f2" + "0" * 60),
        },
        "exchange_info_snapshot": "2024-01-02",
        "leverage_bracket_snapshot": None,
        "gaps": (),
        "fill_model_tier": "TRADE_ONLY",
        "flags": ("BRACKETS_APPROXIMATE",),
    }
    base.update(overrides)
    return Manifest(**base)


class TestDiff:
    def test_identical_manifests_report_unchanged(self) -> None:
        diff = diff_manifests(_manifest(), _manifest())
        assert not diff.changed
        assert "unchanged" in diff.report()

    def test_file_and_row_movement_is_named_precisely(self) -> None:
        after = _manifest(
            datasets={
                "aggTrades": DatasetEntry(
                    files=367, rows=891_303_847, sha256="44be" + "0" * 60
                ),
                "klines_1m": DatasetEntry(files=24, rows=527_040, sha256="a3f2" + "0" * 60),
            }
        )
        report = diff_manifests(_manifest(), after).report()
        assert "datasets.aggTrades" in report
        assert "files 366 -> 367 (+1)" in report
        assert "rows 891203847 -> 891303847 (+100000)" in report
        # The dataset that did not move must not be mentioned.
        assert "klines_1m" not in report

    def test_rewrite_with_identical_shape_is_called_out(self) -> None:
        """Same files, same rows, different hash -- the case a boolean cannot explain."""
        after = _manifest(
            datasets={
                "aggTrades": DatasetEntry(
                    files=366, rows=891_203_847, sha256="ffff" + "0" * 60
                ),
                "klines_1m": DatasetEntry(files=24, rows=527_040, sha256="a3f2" + "0" * 60),
            }
        )
        report = diff_manifests(_manifest(), after).report()
        assert "same 366 files and 891203847 rows" in report
        assert "sizes or mtimes moved" in report

    def test_added_and_removed_datasets(self) -> None:
        after = _manifest(
            datasets={
                "aggTrades": _manifest().datasets["aggTrades"],
                "bookTicker": DatasetEntry(files=3, rows=90, sha256="dd" + "0" * 62),
            }
        )
        report = diff_manifests(_manifest(), after).report()
        assert "datasets.bookTicker: added (3 files, 90 rows)" in report
        assert "datasets.klines_1m: gone (was 24 files, 527040 rows)" in report

    def test_tier_change_leads_the_report(self) -> None:
        diff = diff_manifests(_manifest(), _manifest(fill_model_tier="BAR_CLOSE"))
        assert diff.changes[0].startswith("fill_model_tier: TRADE_ONLY -> BAR_CLOSE")

    def test_range_change_names_both_forms(self) -> None:
        report = diff_manifests(
            _manifest(), _manifest(start_ms=_ms(2024, 1, 2, hour=0))
        ).report()
        assert "range.start_ms" in report
        assert "2024-01-01 -> 2024-01-02" in report

    def test_symbol_changes(self) -> None:
        report = diff_manifests(
            _manifest(), _manifest(symbols=("BTCUSDT", "ETHUSDT"))
        ).report()
        assert "symbols: added ETHUSDT" in report

        reverse = diff_manifests(_manifest(symbols=("BTCUSDT", "ETHUSDT")), _manifest())
        assert "symbols: removed ETHUSDT" in reverse.report()

    def test_reference_change(self) -> None:
        report = diff_manifests(
            _manifest(), _manifest(exchange_info_snapshot=None)
        ).report()
        assert "reference.exchangeInfo_snapshot: 2024-01-02 -> none" in report

    def test_gap_count_change(self) -> None:
        report = diff_manifests(
            _manifest(), _manifest(gaps=({"a": 1}, {"a": 2}, {"a": 3}))
        ).report()
        assert "gaps: 0 -> 3" in report

    def test_same_number_of_different_gaps(self) -> None:
        report = diff_manifests(
            _manifest(gaps=({"a": 1},)), _manifest(gaps=({"a": 2},))
        ).report()
        assert "not the same gaps" in report

    def test_flag_changes(self) -> None:
        report = diff_manifests(
            _manifest(), _manifest(flags=("BRACKETS_APPROXIMATE", "FILTERS_APPROXIMATE"))
        ).report()
        assert "flags: added FILTERS_APPROXIMATE" in report

    def test_end_to_end_backfill_is_explained(self, tmp_path: Path) -> None:
        """The two-second answer, over a real lake: one more day of trades arrived."""
        start, end = _basic_lake(tmp_path)
        saved = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        write_manifest(saved, tmp_path / "manifest.json")

        fill(tmp_path, "aggTrades", "BTCUSDT", [_ms(2024, 1, 2, hour=18)], rows_each=7)

        recomputed = build_manifest(tmp_path, ["BTCUSDT"], start, end)
        diff = diff_manifests(read_manifest(tmp_path / "manifest.json"), recomputed)

        assert diff.changed
        assert "files 3 -> 4 (+1)" in diff.report()
        assert "rows 15 -> 22 (+7)" in diff.report()


# --------------------------------------------------------------------------------------
# Gap magnitude
# --------------------------------------------------------------------------------------


class TestUnexplainedGapMs:
    """How much of a range a dataset is missing, which is what the fill tier is judged on.

    This answered a boolean until runs 23 and 24 were demoted from `TRADE_ONLY` to
    `BAR_CLOSE` over a nineteen-minute Binance halt inside a 365-day range. A dataset absent
    for the whole range and one absent for 0.004% of it are not the same fact, and the
    boolean could not tell them apart.
    """

    HOUR = 3_600_000
    START = 1_709_251_200_000

    def _gap(self, dataset, lo, hi, symbol="BTCUSDT", **extra):
        return {
            "dataset": dataset,
            "symbol": symbol,
            "start_ms": self.START + lo,
            "end_ms": self.START + hi,
            "explained": False,
            **extra,
        }

    def test_a_gap_is_measured_in_milliseconds_of_the_range(self) -> None:
        gaps = [self._gap("aggTrades", 600_000, 900_000)]
        missing = unexplained_gap_ms(gaps, ["BTCUSDT"], self.START, self.START + self.HOUR)
        assert missing == {"aggTrades": 300_000}

    def test_a_gap_is_clipped_to_the_range_rather_than_counted_whole(self) -> None:
        """A gap running past the end of the range did not remove anything past the end."""
        gaps = [self._gap("aggTrades", -self.HOUR, 600_000)]
        missing = unexplained_gap_ms(gaps, ["BTCUSDT"], self.START, self.START + self.HOUR)
        assert missing == {"aggTrades": 600_000}

    def test_overlapping_gaps_are_merged_not_summed(self) -> None:
        """Two symbols down over one hour is one missing hour, not two.

        Summing would let a two-symbol run blow any budget twice as fast as a one-symbol run
        over identically holed data, and the budget would then measure the symbol count.
        """
        gaps = [
            self._gap("aggTrades", 600_000, 1_200_000, symbol="BTCUSDT"),
            self._gap("aggTrades", 900_000, 1_500_000, symbol="ETHUSDT"),
        ]
        missing = unexplained_gap_ms(
            gaps, ["BTCUSDT", "ETHUSDT"], self.START, self.START + self.HOUR
        )
        assert missing == {"aggTrades": 900_000}

    def test_disjoint_gaps_in_one_dataset_do_add_up(self) -> None:
        gaps = [
            self._gap("aggTrades", 0, 60_000),
            self._gap("aggTrades", 600_000, 660_000),
        ]
        missing = unexplained_gap_ms(gaps, ["BTCUSDT"], self.START, self.START + self.HOUR)
        assert missing == {"aggTrades": 120_000}

    def test_an_explained_gap_contributes_nothing(self) -> None:
        gaps = [self._gap("aggTrades", 0, 600_000, explained=True)]
        assert unexplained_gap_ms(gaps, ["BTCUSDT"], self.START, self.START + self.HOUR) == {}

    def test_another_symbols_gap_contributes_nothing(self) -> None:
        gaps = [self._gap("aggTrades", 0, 600_000, symbol="ETHUSDT")]
        assert unexplained_gap_ms(gaps, ["BTCUSDT"], self.START, self.START + self.HOUR) == {}

    def test_a_gap_with_no_symbol_counts_for_every_symbol(self) -> None:
        """`collectorEvents` describes the process, not an instrument: nothing was recorded,
        for all of them at once."""
        gaps = [self._gap("aggTrades", 0, 600_000, symbol=None)]
        missing = unexplained_gap_ms(gaps, ["BTCUSDT"], self.START, self.START + self.HOUR)
        assert missing == {"aggTrades": 600_000}

    def test_a_gap_entirely_outside_the_range_contributes_nothing(self) -> None:
        gaps = [self._gap("aggTrades", -self.HOUR, -1)]
        assert unexplained_gap_ms(gaps, ["BTCUSDT"], self.START, self.START + self.HOUR) == {}

    def test_unrecognised_entries_are_ignored_rather_than_raised_on(self) -> None:
        """`gaps` is spec 4.6 free-form, and a reloaded manifest may carry an older shape."""
        gaps = [object(), {"dataset": "aggTrades"}, self._gap("aggTrades", 0, 60_000)]
        missing = unexplained_gap_ms(gaps, ["BTCUSDT"], self.START, self.START + self.HOUR)
        assert missing == {"aggTrades": 60_000}
