"""Tests for the Parquet writer and partitioning.

The date-partitioning tests carry more weight than they look: an off-by-one-day
partition boundary is invisible until a backtest straddles midnight, at which point it
presents as missing data rather than as a bug in this file.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from perplab.data.schemas import AGG_TRADES, SCHEMAS, partition_key
from perplab.data.writer import ParquetBufferedWriter


class TestPartitionKey:
    @pytest.mark.parametrize(
        ("ts_ms", "expected"),
        [
            (0, "1970-01-01"),
            (86_399_999, "1970-01-01"),
            (86_400_000, "1970-01-02"),
            (1_704_067_200_000, "2024-01-01"),
            # Leap day -- the classic civil-calendar off-by-one.
            (1_709_164_800_000, "2024-02-29"),
            (1_709_251_199_999, "2024-02-29"),
            (1_709_251_200_000, "2024-03-01"),
            (1_785_542_400_000, "2026-08-01"),
        ],
    )
    def test_utc_dates(self, ts_ms: int, expected: str) -> None:
        assert partition_key(ts_ms) == expected

    def test_is_timezone_independent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Must not shift under a local timezone.

        This is why the implementation uses integer arithmetic rather than `datetime`:
        there is no code path a TZ environment variable could influence.
        """
        monkeypatch.setenv("TZ", "Pacific/Auckland")
        assert partition_key(1_704_067_200_000) == "2024-01-01"


def _trade(ts_ms: int, agg_id: int = 1) -> dict[str, object]:
    return {
        "ts_ms": ts_ms,
        "recv_ms": ts_ms + 5,
        "agg_id": agg_id,
        "price": 5_000_010_000_000,
        "qty": 1_400_000,
        "first_trade_id": agg_id,
        "last_trade_id": agg_id,
        "is_buyer_maker": True,
    }


class TestWriter:
    def test_round_trip(self, tmp_path: Path) -> None:
        with ParquetBufferedWriter(
            tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT"
        ) as w:
            for i in range(10):
                w.append(_trade(1_704_067_200_000 + i, agg_id=i))

        files = list(tmp_path.rglob("*.parquet"))
        assert len(files) == 1
        table = pa.parquet.read_table(files[0])
        assert table.num_rows == 10
        assert table.schema.equals(AGG_TRADES)
        # The aggressor flag must survive storage intact -- the limit-fill model is built
        # entirely on it (spec 6.4).
        assert table.column("is_buyer_maker").to_pylist() == [True] * 10

    def test_hive_partition_layout(self, tmp_path: Path) -> None:
        with ParquetBufferedWriter(
            tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT"
        ) as w:
            w.append(_trade(1_704_067_200_000))

        expected = tmp_path / "aggTrades" / "symbol=BTCUSDT" / "date=2024-01-01"
        assert expected.is_dir()
        assert list(expected.glob("*.parquet"))

    def test_buffer_spanning_midnight_splits(self, tmp_path: Path) -> None:
        """A flush straddling midnight must file each row under its own UTC date."""
        with ParquetBufferedWriter(
            tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT"
        ) as w:
            w.append(_trade(1_704_067_199_999, agg_id=1))  # 2023-12-31
            w.append(_trade(1_704_067_200_000, agg_id=2))  # 2024-01-01

        dirs = sorted(p.name for p in (tmp_path / "aggTrades" / "symbol=BTCUSDT").iterdir())
        assert dirs == ["date=2023-12-31", "date=2024-01-01"]

    def test_no_tmp_files_survive(self, tmp_path: Path) -> None:
        """A reader must never encounter a partially written file."""
        with ParquetBufferedWriter(
            tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT"
        ) as w:
            for i in range(100):
                w.append(_trade(1_704_067_200_000 + i, agg_id=i))

        assert not list(tmp_path.rglob("*.tmp"))
        assert not list(tmp_path.rglob(".*"))

    def test_flush_threshold_by_rows(self, tmp_path: Path) -> None:
        w = ParquetBufferedWriter(
            tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT", max_rows=5
        )
        for i in range(4):
            w.append(_trade(1_704_067_200_000 + i, agg_id=i))
        w.maybe_flush()
        assert w.files_written == 0, "must not flush below the row threshold"

        w.append(_trade(1_704_067_200_005, agg_id=5))
        w.maybe_flush()
        assert w.files_written == 1
        assert w.rows_written == 5

    def test_flush_is_idempotent_when_empty(self, tmp_path: Path) -> None:
        w = ParquetBufferedWriter(tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT")
        w.flush()
        w.flush()
        assert w.files_written == 0

    def test_rows_retained_when_write_fails(self, tmp_path: Path) -> None:
        """A transient disk error must not silently drop buffered rows."""
        w = ParquetBufferedWriter(tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT")
        w.append(_trade(1_704_067_200_000))

        original = w._write_partition

        def boom(*_a: object, **_k: object) -> None:
            raise OSError("disk full")

        w._write_partition = boom  # type: ignore[method-assign]
        with pytest.raises(OSError):
            w.flush()

        w._write_partition = original  # type: ignore[method-assign]
        w.flush()
        assert w.rows_written == 1, "row must be retried, not dropped"

    def test_multiple_part_files_read_as_one_partition(self, tmp_path: Path) -> None:
        """Part-files must be transparent to DuckDB's Hive reader.

        This is the justification for deviating from spec 4.3's single `data.parquet`:
        nothing downstream can tell the difference.
        """
        w = ParquetBufferedWriter(tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT")
        for batch in range(3):
            for i in range(10):
                w.append(_trade(1_704_067_200_000 + batch * 100 + i, agg_id=i))
            w.flush()

        assert w.files_written == 3
        glob = (tmp_path / "aggTrades" / "**" / "*.parquet").as_posix()
        (count,) = duckdb.sql(
            f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning := true)"
        ).fetchone()
        assert count == 30


class TestDurabilityAndIdentity:
    """Finding H25: the two halves of "the published file is the file that was written".

    fsync-before-rename is what makes the atomic publish mean anything after a power cut,
    and the pid in the part stem is what stops two processes flushing the shared
    `collectorEvents` partition in the same millisecond from silently destroying one
    another's file via the very `os.replace` that makes publishing atomic.
    """

    def test_part_stems_carry_the_pid(self, tmp_path: Path) -> None:
        with ParquetBufferedWriter(
            tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT"
        ) as w:
            w.append(_trade(1_704_067_200_000))

        (path,) = tmp_path.rglob("*.parquet")
        stem_parts = path.stem.split("-")
        assert stem_parts[0] == "part"
        assert stem_parts[2] == str(os.getpid()), (
            "the stem must carry this process's pid so two processes sharing a "
            "partition cannot collide on part-<ms>-<seq>"
        )
        assert stem_parts[3] == "000001"

    def test_two_writers_in_one_millisecond_cannot_collide(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same frozen clock, same seq, distinct files -- the pre-fix collision setup.

        The second process is simulated by patching `os.getpid` for one writer; freezing
        `time.time` makes the millisecond identical, which before the fix produced the
        same stem twice and let the second `os.replace` overwrite the first file.
        """
        import perplab.data.writer as writer_module

        frozen = 1_704_067_200.0
        monkeypatch.setattr(writer_module.time, "time", lambda: frozen)

        a = ParquetBufferedWriter(tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT")
        b = ParquetBufferedWriter(tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT")
        a.append(_trade(1_704_067_200_000, agg_id=1))
        a.flush()

        real_pid = os.getpid()
        monkeypatch.setattr(writer_module.os, "getpid", lambda: real_pid + 1)
        b.append(_trade(1_704_067_200_001, agg_id=2))
        b.flush()

        files = list(tmp_path.rglob("*.parquet"))
        assert len(files) == 2, "the second flush must not replace the first file"

    def test_the_tmp_file_is_fsynced_before_publish(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`os.replace` orders the name, not the bytes; without the fsync a power cut
        could publish a name pointing at pages the OS never flushed -- exactly the
        truncated-footer corruption the module docstring says it closed."""
        import perplab.data.writer as writer_module

        synced: list[int] = []
        replaced: list[str] = []
        real_fsync = os.fsync
        real_replace = os.replace

        def spy_fsync(fd: int) -> None:
            synced.append(fd)
            real_fsync(fd)

        def spy_replace(src: object, dst: object) -> None:
            assert synced, "os.replace ran before any fsync -- durability after visibility"
            replaced.append(str(dst))
            real_replace(src, dst)  # type: ignore[arg-type]

        monkeypatch.setattr(writer_module.os, "fsync", spy_fsync)
        monkeypatch.setattr(writer_module.os, "replace", spy_replace)

        w = ParquetBufferedWriter(tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT")
        w.append(_trade(1_704_067_200_000))
        w.flush()

        assert len(synced) >= 1
        assert len(replaced) == 1


class TestSymbolValidation:
    """Finding M29: `symbol` becomes a literal path component, so it must be refused,
    not escaped, when it carries a separator -- `../../..` walked out of the lake root."""

    @pytest.mark.parametrize("bad", ["BTC/USDT", "../../..", "BTC USDT", "sym='x'"])
    def test_a_path_shaped_symbol_is_refused_at_construction(
        self, tmp_path: Path, bad: str
    ) -> None:
        with pytest.raises(ValueError, match="implausible symbol"):
            ParquetBufferedWriter(tmp_path, "aggTrades", AGG_TRADES, symbol=bad)
        assert list(tmp_path.rglob("*")) == [], "nothing may be created for a bad symbol"

    def test_a_lowercase_symbol_lands_in_the_canonical_partition(
        self, tmp_path: Path
    ) -> None:
        with ParquetBufferedWriter(
            tmp_path, "aggTrades", AGG_TRADES, symbol="btcusdt"
        ) as w:
            w.append(_trade(1_704_067_200_000))
        assert (tmp_path / "aggTrades" / "symbol=BTCUSDT").is_dir()


def test_every_dataset_schema_is_writable(tmp_path: Path) -> None:
    """Guards against a schema that Arrow accepts but cannot actually serialise."""
    for name, schema in SCHEMAS.items():
        w = ParquetBufferedWriter(tmp_path, name, schema)
        row: dict[str, object] = {}
        for field in schema:
            if pa.types.is_list(field.type):
                row[field.name] = [1, 2, 3]
            elif pa.types.is_boolean(field.type):
                row[field.name] = True
            elif pa.types.is_string(field.type):
                row[field.name] = "x"
            else:
                row[field.name] = 1_704_067_200_000
        w.append(row)
        w.flush()
        assert w.files_written == 1, f"{name} failed to write"
