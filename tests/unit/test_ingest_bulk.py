"""Tests for the bulk ingest pipeline: download, verify, parse, write, resume.

Every archive here is built in-process from CSV lines that were observed on
data.binance.vision on 2026-08-01 -- the same sample rows `tests/unit/test_bulk_layout.py`
uses -- zipped, and served through `LocalDirectoryFetcher`. No network, and no stubbing of
the stage that matters: the checksum comparison runs against a real sha256 of real zip
bytes, so a test that says "corruption is rejected" is testing the production code path
rather than a mock's opinion of it.

The one thing deliberately not exercised here is `HttpFetcher`. It is the only part that
needs data.binance.vision to be reachable, and a unit test that reaches the network is a
test that fails for reasons unrelated to this module.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import threading
import time
import zipfile
from dataclasses import replace as dataclass_replace
from datetime import date
from pathlib import Path
from typing import Any, BinaryIO, Callable
from unittest import mock

import httpx
import pyarrow.parquet as pq
import pytest

from perplab.core.money import to_scaled
from perplab.data.bulk_layout import BULK_DATASETS, BulkDataset, bulk_dataset
from perplab.data.ingest_bulk import (
    PHASE1_DATASETS,
    PUBLISHED_COVERAGE,
    TYPICAL_BYTES_PER_PERIOD,
    AddressBanned,
    ArchiveNotPublished,
    BulkIngestError,
    ChecksumMismatch,
    DatasetUnavailable,
    FileOutcome,
    HttpFetcher,
    IngestStatus,
    LocalDirectoryFetcher,
    MalformedArchive,
    MalformedChecksum,
    TransientFetchError,
    fill_days_from_monthly,
    ingest_archive,
    ingest_range,
    is_ingested,
    parse_checksum_file,
    periods_for,
    plan_range,
    receipt_path,
    sweep_stale_downloads,
)
from perplab.data.schemas import SCHEMAS
from perplab.data.writer import ParquetBufferedWriter

# --------------------------------------------------------------------------------------
# Real sample lines, verified 2026-08-01 against BTCUSDT archives.
# --------------------------------------------------------------------------------------

SYMBOL = "BTCUSDT"

KLINE_HEADER = (
    "open_time,open,high,low,close,volume,close_time,quote_volume,count,"
    "taker_buy_volume,taker_buy_quote_volume,ignore"
)
KLINE_ROW = (
    "1784073600000,65014.60,65031.80,64996.80,65011.50,327.207,1784073659999,"
    "21273110.50730,5998,222.863,14489221.32700,0"
)
"""First data row of `BTCUSDT-1m-2026-07-15.zip`; `open_time` is 2026-07-15T00:00:00Z."""

KLINE_DATE = "2026-07-15"


def kline_row_at(minute: int) -> str:
    """The real row shifted forward by whole minutes, to build a plausible day.

    Only the two timestamps move. Every price, volume and count stays exactly as
    published, so nothing in this file is a made-up market value.
    """
    fields = KLINE_ROW.split(",")
    fields[0] = str(int(fields[0]) + minute * 60_000)
    fields[6] = str(int(fields[6]) + minute * 60_000)
    return ",".join(fields)


AGG_TRADE_HEADER = (
    "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker"
)
AGG_TRADE_ROW = "3383271130,65014.6,0.03,7900100566,7900100566,1784073600069,true"
AGG_TRADE_DATE = "2026-07-15"

BOOK_TICKER_HEADER = (
    "update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,"
    "transaction_time,event_time"
)
BOOK_TICKER_ROW = (
    "2948552298577,25115.90000000,13.09700000,25116.00000000,1.23000000,"
    "1686787200009,1686787200015"
)
BOOK_TICKER_DATE = "2023-06-15"
"""Inside the 320-day window of finding F1."""


def book_ticker_row_at(index: int, *, update_id: str | None = None) -> str:
    """The real row shifted forward by whole milliseconds, to build a plausible burst.

    `update_id` is overridable so a test can plant a value the exchange would never send.
    `bulk_layout._to_int` returns an unbounded Python int, so an id past int64 survives
    parsing and fails later, at Arrow conversion -- which is the only trigger that reaches
    the batch loop without stubbing anything.
    """
    fields = BOOK_TICKER_ROW.split(",")
    fields[0] = update_id if update_id is not None else str(int(fields[0]) + index)
    fields[5] = str(int(fields[5]) + index)
    fields[6] = str(int(fields[6]) + index)
    return ",".join(fields)

FUNDING_HEADER = "calc_time,funding_interval_hours,last_funding_rate"
FUNDING_ROW = "1780272000001,8,0.00005703"
FUNDING_MONTH = "2026-06"

METRICS_HEADER = (
    "create_time,symbol,sum_open_interest,sum_open_interest_value,"
    "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
    "count_long_short_ratio,sum_taker_long_short_vol_ratio"
)
METRICS_ROW = (
    "2026-07-15 00:00:00,BTCUSDT,105550.9850000000000000,6858675634.7062540000000000,"
    "1.28623339,1.47112400,1.19972635,1.55827200"
)
METRICS_DATE = "2026-07-15"


# --------------------------------------------------------------------------------------
# A local mirror of the bucket
# --------------------------------------------------------------------------------------


def zip_bytes(member: str, lines: list[str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, "\n".join(lines) + "\n")
    return buffer.getvalue()


def publish(
    mirror: Path,
    dataset: str,
    period: str,
    lines: list[str],
    *,
    symbol: str = SYMBOL,
    digest: str | None = None,
    checksum: bool = True,
    payload: bytes | None = None,
    checksum_name: str | None = None,
) -> Path:
    """Place one archive and its `.CHECKSUM` sibling in the local mirror.

    `digest`, `payload` and `checksum_name` exist so a test can publish an archive whose
    checksum is wrong, whose bytes are corrupt, or whose checksum names another file --
    the three ways this can go wrong in the wild.
    """
    bulk = bulk_dataset(dataset)
    directory = mirror.joinpath(*bulk.path_prefix(symbol).rstrip("/").split("/"))
    directory.mkdir(parents=True, exist_ok=True)

    stem = bulk.file_stem(symbol, period)
    data = payload if payload is not None else zip_bytes(f"{stem}.csv", lines)
    archive = directory / f"{stem}.zip"
    archive.write_bytes(data)

    if checksum:
        name = checksum_name if checksum_name is not None else archive.name
        published = digest if digest is not None else hashlib.sha256(data).hexdigest()
        (directory / f"{stem}.zip.CHECKSUM").write_text(
            f"{published}  {name}\n", encoding="utf-8"
        )
    return archive


class CountingFetcher:
    """Wraps a fetcher and records every URL, so "did not re-download" is provable."""

    def __init__(self, inner: LocalDirectoryFetcher) -> None:
        self.inner = inner
        self.text_urls: list[str] = []
        self.download_urls: list[str] = []

    def get_text(self, url: str) -> str:
        self.text_urls.append(url)
        return self.inner.get_text(url)

    def download(
        self,
        url: str,
        sink: BinaryIO,
        *,
        on_chunk: Callable[[int, int | None], None] | None = None,
    ) -> int:
        self.download_urls.append(url)
        return self.inner.download(url, sink, on_chunk=on_chunk)


@pytest.fixture
def mirror(tmp_path: Path) -> Path:
    path = tmp_path / "mirror"
    path.mkdir()
    return path


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    return tmp_path / "market"


@pytest.fixture
def fetcher(mirror: Path) -> CountingFetcher:
    return CountingFetcher(LocalDirectoryFetcher(mirror))


def partition_dir(lake: Path, dataset: str) -> Path:
    return lake / dataset / f"symbol={SYMBOL}"


def parquet_files(lake: Path) -> list[Path]:
    return sorted(p for p in lake.rglob("*.parquet") if not p.name.startswith("."))


# --------------------------------------------------------------------------------------


class TestChecksumFile:
    """The `.CHECKSUM` sibling is the only thing standing between us and a corrupt lake."""

    def test_parses_the_sha256sum_format(self) -> None:
        digest = "a" * 64
        assert parse_checksum_file(f"{digest}  X.zip\n", "X.zip") == digest

    def test_accepts_the_binary_mode_star(self) -> None:
        digest = "b" * 64
        assert parse_checksum_file(f"{digest} *X.zip\n", "X.zip") == digest

    def test_rejects_a_digest_that_is_not_hex(self) -> None:
        with pytest.raises(MalformedChecksum, match="hex sha256"):
            parse_checksum_file(f"{'z' * 64}  X.zip", "X.zip")

    def test_rejects_a_short_digest(self) -> None:
        with pytest.raises(MalformedChecksum, match="hex sha256"):
            parse_checksum_file("abc  X.zip", "X.zip")

    def test_rejects_a_checksum_naming_a_different_archive(self) -> None:
        """A stale mirror object would otherwise be verified against the wrong file."""
        with pytest.raises(MalformedChecksum, match="but the archive requested"):
            parse_checksum_file(f"{'c' * 64}  OTHER.zip", "X.zip")

    def test_rejects_a_multi_line_file(self) -> None:
        with pytest.raises(MalformedChecksum, match="exactly one line"):
            parse_checksum_file(f"{'d' * 64}  X.zip\n{'e' * 64}  Y.zip\n", "X.zip")


class TestChecksumVerification:
    def test_a_matching_checksum_ingests_the_archive(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])

        outcome = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert outcome.status is IngestStatus.WRITTEN
        assert outcome.rows == 1
        assert len(parquet_files(lake)) == 1

    def test_a_mismatched_checksum_raises_and_writes_nothing(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """The whole point of spec 4.5's verify stage.

        A corrupt archive that still inflates yields plausible rows, so the failure has
        to happen before anything is parsed -- not after, with a cleanup.
        """
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW], digest="f" * 64)

        with pytest.raises(ChecksumMismatch) as excinfo:
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert excinfo.value.expected == "f" * 64
        assert parquet_files(lake) == []
        assert not (lake / "klines").exists()
        assert not receipt_path(lake, "klines", SYMBOL, KLINE_DATE).exists()

    def test_a_mismatch_leaves_no_scratch_file_behind(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW], digest="0" * 64)
        with pytest.raises(ChecksumMismatch):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert list((lake / "_ingest" / "tmp").glob("*")) == []

    def test_corrupted_bytes_are_caught_even_though_the_zip_would_open(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Flip a byte in the CSV payload but publish the *original* digest.

        This is the failure mode spec 4.5 names: the archive still inflates, so nothing
        downstream would complain. Only the checksum knows.
        """
        good = zip_bytes(f"{SYMBOL}-1m-{KLINE_DATE}.csv", [KLINE_ROW])
        corrupt = zip_bytes(f"{SYMBOL}-1m-{KLINE_DATE}.csv", [kline_row_at(7)])
        publish(
            mirror,
            "klines",
            KLINE_DATE,
            [],
            payload=corrupt,
            digest=hashlib.sha256(good).hexdigest(),
        )

        with pytest.raises(ChecksumMismatch):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        assert parquet_files(lake) == []

    def test_an_archive_without_a_checksum_is_never_downloaded(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Verification is structural: no digest, no parse, and no wasted 240 MB."""
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW], checksum=False)

        with pytest.raises(ArchiveNotPublished):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert fetcher.download_urls == []
        assert parquet_files(lake) == []

    def test_the_checksum_is_fetched_before_the_archive(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert fetcher.text_urls[0].endswith(".zip.CHECKSUM")
        assert len(fetcher.download_urls) == 1


class TestHeaderSniffing:
    """Header rows are conditional per file; both wrong answers lose data silently."""

    def test_header_and_headerless_archives_yield_the_same_rows(
        self, tmp_path: Path, mirror: Path
    ) -> None:
        rows = [kline_row_at(i) for i in range(5)]

        with_header = tmp_path / "with"
        without_header = tmp_path / "without"
        publish(mirror, "klines", KLINE_DATE, [KLINE_HEADER, *rows])
        headered = ingest_archive(
            with_header,
            SYMBOL,
            "klines",
            KLINE_DATE,
            fetcher=LocalDirectoryFetcher(mirror),
        )

        publish(mirror, "klines", KLINE_DATE, rows)
        bare = ingest_archive(
            without_header,
            SYMBOL,
            "klines",
            KLINE_DATE,
            fetcher=LocalDirectoryFetcher(mirror),
        )

        assert headered.rows == bare.rows == 5
        left = pq.read_table(parquet_files(with_header)[0])
        right = pq.read_table(parquet_files(without_header)[0])
        assert left.to_pydict() == right.to_pydict()

    def test_a_pre_2023_archive_keeps_its_first_row(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """The specific silent loss: assuming a header drops one bar per file."""
        rows = [kline_row_at(i) for i in range(3)]
        publish(mirror, "klines", KLINE_DATE, rows)

        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        table = pq.read_table(parquet_files(lake)[0])
        assert table.column("open_time").to_pylist() == [
            int(row.split(",")[0]) for row in rows
        ]

    def test_a_trailing_blank_line_is_counted_not_parsed(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW, ""])
        outcome = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        assert outcome.rows == 1
        assert outcome.blank_lines == 1


class TestScaling:
    """No float ever touches these values, and the schema stays int64."""

    def test_values_land_as_scaled_int64(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_HEADER, KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        table = pq.read_table(parquet_files(lake)[0])
        assert str(table.schema.field("open").type) == "int64"
        assert table.column("open")[0].as_py() == to_scaled("65014.60")
        assert table.column("close_time")[0].as_py() == 1784073659999
        # A raw trade count, deliberately unscaled.
        assert table.column("count")[0].as_py() == 5998

    def test_bulk_agg_trades_carry_a_null_recv_ms(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Bulk archives were never received by us; a zero would fake the latency."""
        publish(mirror, "aggTrades", AGG_TRADE_DATE, [AGG_TRADE_HEADER, AGG_TRADE_ROW])
        ingest_archive(lake, SYMBOL, "aggTrades", AGG_TRADE_DATE, fetcher=fetcher)

        table = pq.read_table(parquet_files(lake)[0])
        assert table.column("recv_ms").to_pylist() == [None]
        assert table.column("is_buyer_maker").to_pylist() == [True]

    def test_a_blank_metrics_cell_is_written_null(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        blank = METRICS_ROW.split(",")
        blank[4] = ""
        publish(mirror, "metrics", METRICS_DATE, [METRICS_HEADER, ",".join(blank)])

        ingest_archive(lake, SYMBOL, "metrics", METRICS_DATE, fetcher=fetcher)

        table = pq.read_table(parquet_files(lake)[0])
        assert table.column("count_toptrader_long_short_ratio").to_pylist() == [None]
        assert table.column("sum_open_interest").to_pylist() == [
            to_scaled("105550.9850000000000000")
        ]


class TestStreamingLargeArchives:
    """A bookTicker day is roughly 240 MB zipped; it can never all be resident."""

    def test_rows_are_batched_into_row_groups_without_losing_any(
        self,
        lake: Path,
        mirror: Path,
        fetcher: CountingFetcher,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Same code path as a real day, with the thresholds shrunk so it is quick.

        Exercising this matters because the batching is where rows could silently go
        missing: a tail batch left unwritten, or a partition written before the last
        concat, would both produce a valid Parquet file with fewer rows than the archive
        -- which no schema check and no checksum would catch.
        """
        monkeypatch.setattr("perplab.data.ingest_bulk._ROWS_PER_ARROW_BATCH", 10)
        monkeypatch.setattr("perplab.data.ingest_bulk._ROWS_PER_ROW_GROUP", 30)

        rows = [kline_row_at(i) for i in range(250)]
        publish(mirror, "klines", KLINE_DATE, [KLINE_HEADER, *rows])

        outcome = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert outcome.rows == 250
        parquet = pq.ParquetFile(parquet_files(lake)[0])
        assert parquet.metadata.num_rows == 250
        assert parquet.metadata.num_row_groups == 9  # 8 full groups of 30 plus a tail
        table = parquet.read()
        assert table.column("open_time").to_pylist() == [
            1784073600000 + i * 60_000 for i in range(250)
        ]

    def test_a_tail_shorter_than_one_batch_still_lands(
        self,
        lake: Path,
        mirror: Path,
        fetcher: CountingFetcher,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("perplab.data.ingest_bulk._ROWS_PER_ARROW_BATCH", 10)
        monkeypatch.setattr("perplab.data.ingest_bulk._ROWS_PER_ROW_GROUP", 30)

        publish(mirror, "klines", KLINE_DATE, [kline_row_at(i) for i in range(7)])
        outcome = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert outcome.rows == 7
        assert pq.read_table(parquet_files(lake)[0]).num_rows == 7


class TestPartitionPaths:
    """Spec 4.3's layout, exactly, for each shape of dataset."""

    def test_daily_dataset_writes_one_data_parquet_per_date(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "aggTrades", AGG_TRADE_DATE, [AGG_TRADE_HEADER, AGG_TRADE_ROW])
        outcome = ingest_archive(
            lake, SYMBOL, "aggTrades", AGG_TRADE_DATE, fetcher=fetcher
        )

        expected = (
            partition_dir(lake, "aggTrades") / f"date={AGG_TRADE_DATE}" / "data.parquet"
        )
        assert expected.is_file()
        assert outcome.parquet == expected.relative_to(lake).as_posix()

    def test_klines_land_under_interval_year_and_month(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        expected = (
            partition_dir(lake, "klines")
            / "interval=1m"
            / "year=2026"
            / "month=07"
            / f"{KLINE_DATE}.parquet"
        )
        assert expected.is_file()

    def test_a_month_partition_accumulates_one_file_per_day(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """The contrast with `data.parquet`: rewriting the month per day is quadratic."""
        for day in ("2026-07-15", "2026-07-16"):
            offset = 0 if day == "2026-07-15" else 1440
            publish(mirror, "klines", day, [kline_row_at(offset)])
            ingest_archive(lake, SYMBOL, "klines", day, fetcher=fetcher)

        month = partition_dir(lake, "klines") / "interval=1m" / "year=2026" / "month=07"
        assert sorted(p.name for p in month.glob("*.parquet")) == [
            "2026-07-15.parquet",
            "2026-07-16.parquet",
        ]

    def test_metrics_land_under_year(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "metrics", METRICS_DATE, [METRICS_HEADER, METRICS_ROW])
        ingest_archive(lake, SYMBOL, "metrics", METRICS_DATE, fetcher=fetcher)

        assert (
            partition_dir(lake, "metrics") / "year=2026" / f"{METRICS_DATE}.parquet"
        ).is_file()

    def test_funding_lands_directly_under_symbol(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """`fundingRate` archives feed the lake dataset `funding`, which has no time
        component at all -- a whole symbol's settlements are one partition."""
        publish(mirror, "fundingRate", FUNDING_MONTH, [FUNDING_HEADER, FUNDING_ROW])
        outcome = ingest_archive(
            lake, SYMBOL, "fundingRate", FUNDING_MONTH, fetcher=fetcher
        )

        assert (partition_dir(lake, "funding") / f"{FUNDING_MONTH}.parquet").is_file()
        assert outcome.dataset == "fundingRate"

    def test_book_ticker_lands_under_date(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(
            mirror, "bookTicker", BOOK_TICKER_DATE, [BOOK_TICKER_HEADER, BOOK_TICKER_ROW]
        )
        ingest_archive(lake, SYMBOL, "bookTicker", BOOK_TICKER_DATE, fetcher=fetcher)

        assert (
            partition_dir(lake, "bookTicker")
            / f"date={BOOK_TICKER_DATE}"
            / "data.parquet"
        ).is_file()


class TestCollectorPartitionCollision:
    """Two producers, one partition directory.

    `aggTrades` and `bookTicker` are shared between the live collector and the bulk
    archives (see `schemas.SCHEMAS`), they resolve to the same `symbol=/date=` directory,
    and `cli.py` hands both producers the same lake root. The collector publishes
    `part-<epoch_ms>-<seq>.parquet` and bulk publishes `data.parquet`, so the `os.replace`
    that makes a re-ingest idempotent overwrites nothing: both files survive, and the
    `**/*.parquet` glob every reader uses counts every overlapping row twice.

    Little downstream can see it. `detect_tick_gaps` now counts exchange-id collisions in
    `Coverage.duplicate_rows` (finding H26), but that is a post-hoc symptom in one report;
    duplicated rows still *shorten* inter-record intervals rather than opening gaps, and
    the manifest sees a two-file partition, which is what a legitimate multi-part
    collector partition also looks like. Refusing before the write remains the guard.
    """

    @staticmethod
    def agg_trade(ts_ms: int, agg_id: int) -> dict[str, Any]:
        return {
            "ts_ms": ts_ms,
            "recv_ms": ts_ms + 1,
            "agg_id": agg_id,
            "price": to_scaled("65014.6"),
            "qty": to_scaled("0.03"),
            "first_trade_id": agg_id,
            "last_trade_id": agg_id,
            "is_buyer_maker": True,
        }

    @classmethod
    def collector_wrote(cls, lake: Path, ts_ms: int = 1784073600069) -> Path:
        """One collector part-file for the date `AGG_TRADE_DATE`, via the real writer.

        Built with `ParquetBufferedWriter` rather than a touched file so the name is
        whatever the collector actually produces; a guard keyed to a name the collector
        does not use would pass a hand-written fixture and fail in production.
        """
        with ParquetBufferedWriter(
            lake, "aggTrades", SCHEMAS["aggTrades"], symbol=SYMBOL
        ) as writer:
            writer.append(cls.agg_trade(ts_ms, 3383271130))
        return next(
            (lake / "aggTrades" / f"symbol={SYMBOL}" / f"date={AGG_TRADE_DATE}").glob(
                "part-*.parquet"
            )
        )

    def test_it_refuses_a_partition_the_collector_already_filled(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        part = self.collector_wrote(lake)
        publish(mirror, "aggTrades", AGG_TRADE_DATE, [AGG_TRADE_HEADER, AGG_TRADE_ROW])

        with pytest.raises(BulkIngestError) as excinfo:
            ingest_archive(lake, SYMBOL, "aggTrades", AGG_TRADE_DATE, fetcher=fetcher)

        assert type(excinfo.value).__name__ == "PartitionConflict"
        assert part.name in str(excinfo.value)
        # Nothing published, so the lake still holds exactly what the collector recorded.
        assert parquet_files(lake) == [part]
        assert not is_ingested(lake, "aggTrades", SYMBOL, AGG_TRADE_DATE)

    def test_the_refusal_costs_no_network(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """A bookTicker day is roughly 240 MB. Discovering the collision after the
        transfer would make a 320-day backfill spend 77 GB to learn it cannot land."""
        self.collector_wrote(lake)
        publish(mirror, "aggTrades", AGG_TRADE_DATE, [AGG_TRADE_HEADER, AGG_TRADE_ROW])

        with pytest.raises(BulkIngestError):
            ingest_archive(lake, SYMBOL, "aggTrades", AGG_TRADE_DATE, fetcher=fetcher)

        assert fetcher.text_urls == []
        assert fetcher.download_urls == []

    def test_a_collector_flush_during_the_download_is_caught_before_publish(
        self, lake: Path, mirror: Path
    ) -> None:
        """The collector is *live*. A pre-download check alone leaves the window open."""
        publish(mirror, "aggTrades", AGG_TRADE_DATE, [AGG_TRADE_HEADER, AGG_TRADE_ROW])

        class FlushingFetcher(CountingFetcher):
            def download(self, url, sink, *, on_chunk=None):  # type: ignore[no-untyped-def]
                written = super().download(url, sink, on_chunk=on_chunk)
                TestCollectorPartitionCollision.collector_wrote(lake)
                return written

        with pytest.raises(BulkIngestError) as excinfo:
            ingest_archive(
                lake,
                SYMBOL,
                "aggTrades",
                AGG_TRADE_DATE,
                fetcher=FlushingFetcher(LocalDirectoryFetcher(mirror)),
            )

        assert type(excinfo.value).__name__ == "PartitionConflict"
        partition = lake / "aggTrades" / f"symbol={SYMBOL}" / f"date={AGG_TRADE_DATE}"
        assert not (partition / "data.parquet").exists()
        assert list(partition.glob(".*")) == []  # the working file was cleaned up

    def test_the_range_report_records_the_conflict_rather_than_the_rows(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """`ingest_range` prints "written 1, 1 rows" for a day it silently doubled.

        The conflict is reported as `CONFLICT`, never `FAILED`: nothing failed, the
        ingester declined. It still keeps `exit_code` non-zero, because the requested
        range was not satisfied -- a declined period is a period the caller does not have.
        """
        self.collector_wrote(lake)
        publish(mirror, "aggTrades", AGG_TRADE_DATE, [AGG_TRADE_HEADER, AGG_TRADE_ROW])

        report = ingest_range(
            lake, SYMBOL, "aggTrades", AGG_TRADE_DATE, AGG_TRADE_DATE, fetcher=fetcher
        )

        assert report.exit_code != 0
        assert report.count(IngestStatus.WRITTEN) == 0
        assert report.count(IngestStatus.CONFLICT) == 1
        assert report.failures == []  # declined is not failed
        assert [o.period for o in report.conflicts] == [AGG_TRADE_DATE]
        # The status carries the kind, so the message is free to be about the situation:
        # which partition, who owns it, and what the operator can do about it.
        error = report.outcomes[0].error or ""
        assert "collector part-file" in error
        assert AGG_TRADE_DATE in error

    def test_bulks_own_files_are_not_a_conflict(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """The guard must key on the collector's naming, not on "the directory is not
        empty". A re-ingest replaces `data.parquet` in place, and a month partition
        legitimately accumulates one `<period>.parquet` per day."""
        publish(mirror, "aggTrades", AGG_TRADE_DATE, [AGG_TRADE_HEADER, AGG_TRADE_ROW])
        ingest_archive(lake, SYMBOL, "aggTrades", AGG_TRADE_DATE, fetcher=fetcher)

        again = ingest_archive(
            lake, SYMBOL, "aggTrades", AGG_TRADE_DATE, fetcher=fetcher, force=True
        )
        assert again.status is IngestStatus.WRITTEN
        assert len(parquet_files(lake)) == 1

    def test_force_does_not_licence_writing_over_the_collector(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """`--force` means "ignore the receipt", not "the collector's rows are mine"."""
        self.collector_wrote(lake)
        publish(mirror, "aggTrades", AGG_TRADE_DATE, [AGG_TRADE_HEADER, AGG_TRADE_ROW])

        with pytest.raises(BulkIngestError):
            ingest_archive(
                lake, SYMBOL, "aggTrades", AGG_TRADE_DATE, fetcher=fetcher, force=True
            )

    def test_metrics_backfill_coexists_with_live_collection(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Finding M22: `metrics` is year-partitioned, so one hour of live open-interest
        collection used to block the whole year's archive backfill -- with advice
        ("narrow --start/--end") that cannot be satisfied when the conflicting partition
        is the entire year. The refusal protected nothing: the live poller deliberately
        records unsnapped instants precisely so a backfill *densifies* the series rather
        than duplicating it, so here both producers' files must survive side by side.
        """
        with ParquetBufferedWriter(
            lake, "metrics", SCHEMAS["metrics"], symbol=SYMBOL
        ) as writer:
            writer.append(
                {
                    # A live sample, off the archive's five-minute grid by design.
                    "create_time": 1784073600000 + 17_345,
                    "sum_open_interest": to_scaled("105551.001"),
                    "sum_open_interest_value": None,
                    "count_toptrader_long_short_ratio": None,
                    "sum_toptrader_long_short_ratio": None,
                    "count_long_short_ratio": None,
                    "sum_taker_long_short_vol_ratio": None,
                }
            )
        part = next(
            (lake / "metrics" / f"symbol={SYMBOL}" / "year=2026").glob("part-*.parquet")
        )

        publish(mirror, "metrics", METRICS_DATE, [METRICS_HEADER, METRICS_ROW])
        outcome = ingest_archive(lake, SYMBOL, "metrics", METRICS_DATE, fetcher=fetcher)

        assert outcome.status is IngestStatus.WRITTEN
        published = partition_dir(lake, "metrics") / "year=2026" / f"{METRICS_DATE}.parquet"
        assert published.is_file()
        assert part.is_file(), "the collector's file must survive the backfill"


class TestArchiveHandleLifetime:
    """The zip is opened inside a generator; the loop that drains it can raise.

    `_iter_archive_rows` holds the `ZipFile` open across every `yield`, so an exception
    raised by the *consumer* -- a full disk in `write_table`, an out-of-int64 id reaching
    `from_pylist`, Ctrl+C -- leaves the generator suspended and reachable from the
    traceback. On Windows the still-open handle makes `ingest_archive`'s
    `finally: temp_zip.unlink()` fail with WinError 32, which replaces the real exception,
    orphans a quarter-gigabyte `.zip.part`, and downgrades a KeyboardInterrupt to an
    ordinary FAILED outcome that the run then carries on past.
    """

    @staticmethod
    def publish_a_bad_batch(mirror: Path) -> None:
        """Five bookTicker rows whose third carries an id past int64.

        With `_ROWS_PER_ARROW_BATCH` at 2 the failure lands on the second conversion,
        inside the loop, with the fifth row still unyielded -- so the generator is
        genuinely suspended rather than exhausted. Nothing is stubbed: `_to_int` has no
        range check, so the value reaches Arrow exactly as a malformed archive's would.
        """
        rows = [book_ticker_row_at(i) for i in range(5)]
        rows[2] = book_ticker_row_at(2, update_id=str(2**70))
        publish(mirror, "bookTicker", BOOK_TICKER_DATE, [BOOK_TICKER_HEADER, *rows])

    def test_the_real_error_escapes_and_no_download_is_orphaned(
        self,
        lake: Path,
        mirror: Path,
        fetcher: CountingFetcher,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("perplab.data.ingest_bulk._ROWS_PER_ARROW_BATCH", 2)
        self.publish_a_bad_batch(mirror)

        with pytest.raises(OverflowError):
            ingest_archive(lake, SYMBOL, "bookTicker", BOOK_TICKER_DATE, fetcher=fetcher)

        assert list((lake / "_ingest" / "tmp").glob("*.zip.part")) == []

    def test_the_archive_is_closed_before_the_temporary_is_removed(
        self,
        lake: Path,
        mirror: Path,
        fetcher: CountingFetcher,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The platform-independent half: WinError 32 is a symptom, the open handle is
        the defect, and a POSIX box would unlink the file out from under it instead."""
        self.publish_a_bad_batch(mirror)
        monkeypatch.setattr("perplab.data.ingest_bulk._ROWS_PER_ARROW_BATCH", 2)

        opened: list[zipfile.ZipFile] = []

        class Recording(zipfile.ZipFile):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                opened.append(self)

        monkeypatch.setattr(zipfile, "ZipFile", Recording)

        # Bound to a local on purpose: the traceback keeps the frames -- and therefore the
        # suspended generator -- alive, which is exactly the condition being tested.
        with pytest.raises(BaseException) as excinfo:
            ingest_archive(lake, SYMBOL, "bookTicker", BOOK_TICKER_DATE, fetcher=fetcher)

        assert excinfo.value is not None
        assert opened, "the archive was never opened; the test proves nothing"
        assert all(archive.fp is None for archive in opened)

    def test_a_range_reports_the_parse_failure_not_a_cleanup_failure(
        self,
        lake: Path,
        mirror: Path,
        fetcher: CountingFetcher,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """What the operator reads at the end of a six-hour run."""
        monkeypatch.setattr("perplab.data.ingest_bulk._ROWS_PER_ARROW_BATCH", 2)
        self.publish_a_bad_batch(mirror)

        report = ingest_range(
            lake,
            SYMBOL,
            "bookTicker",
            BOOK_TICKER_DATE,
            BOOK_TICKER_DATE,
            fetcher=fetcher,
            concurrency=1,
        )

        outcome = report.outcomes[0]
        assert outcome.status is IngestStatus.FAILED
        assert (outcome.error or "").startswith("OverflowError")
        assert list((lake / "_ingest" / "tmp").glob("*.zip.part")) == []


class TestResumability:
    def test_a_completed_partition_is_skipped_without_refetching(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        downloads = len(fetcher.download_urls)

        again = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert again.status is IngestStatus.SKIPPED
        assert len(fetcher.download_urls) == downloads
        assert fetcher.text_urls[-1].endswith(".zip.CHECKSUM")  # from the first pass only

    def test_force_re_ingests_a_completed_partition(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        again = ingest_archive(
            lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher, force=True
        )
        assert again.status is IngestStatus.WRITTEN
        assert len(parquet_files(lake)) == 1  # replaced in place, not duplicated

    def test_the_receipt_records_what_was_verified(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        archive = publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        payload = json.loads(
            receipt_path(lake, "klines", SYMBOL, KLINE_DATE).read_text(encoding="utf-8")
        )
        assert payload["sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
        assert payload["rows"] == 1
        assert payload["ts_min"] == payload["ts_max"] == 1784073600000

    def test_receipts_live_outside_the_dataset_tree(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """A receipt inside the lake would change spec 4.6's per-dataset file hash."""
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert list((lake / "klines").rglob("*.json")) == []
        assert receipt_path(lake, "klines", SYMBOL, KLINE_DATE).is_file()


class TestHalfWrittenPartitions:
    """A partition that is not finished must never read as finished."""

    def test_a_parquet_without_a_receipt_is_not_complete(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """The crash window between publishing the data and publishing the receipt."""
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        receipt_path(lake, "klines", SYMBOL, KLINE_DATE).unlink()

        assert not is_ingested(lake, "klines", SYMBOL, KLINE_DATE)
        again = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        assert again.status is IngestStatus.WRITTEN

    def test_a_receipt_whose_parquet_was_deleted_is_not_complete(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Spec 4.4's retention job deletes partitions; the ledger must notice."""
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        parquet_files(lake)[0].unlink()

        assert not is_ingested(lake, "klines", SYMBOL, KLINE_DATE)

    def test_only_a_tmp_file_is_not_complete(self, lake: Path) -> None:
        directory = (
            partition_dir(lake, "klines") / "interval=1m" / "year=2026" / "month=07"
        )
        directory.mkdir(parents=True)
        (directory / f".{KLINE_DATE}.parquet.tmp").write_bytes(b"half a parquet")

        assert not is_ingested(lake, "klines", SYMBOL, KLINE_DATE)
        assert parquet_files(lake) == []

    def test_a_corrupt_receipt_is_treated_as_absent(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        receipt_path(lake, "klines", SYMBOL, KLINE_DATE).write_text("{ not json")

        assert not is_ingested(lake, "klines", SYMBOL, KLINE_DATE)

    def test_a_receipt_from_another_format_version_is_ignored(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        path = receipt_path(lake, "klines", SYMBOL, KLINE_DATE)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["version"] = 999
        path.write_text(json.dumps(payload), encoding="utf-8")

        assert not is_ingested(lake, "klines", SYMBOL, KLINE_DATE)


class TestReceiptedFileIntegrity:
    """Finding M26: `is_ingested` accepted *any* file at the receipted path, so a
    truncated or zero-byte partition read as ingested forever and the plan stage skipped
    the one re-download that would have healed it. The receipt already recorded rows and
    bounds; nothing consulted them."""

    def _ingested(self, lake: Path, mirror: Path, fetcher: CountingFetcher) -> Path:
        publish(mirror, "klines", KLINE_DATE, [kline_row_at(0), kline_row_at(1)])
        outcome = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        assert outcome.status is IngestStatus.WRITTEN
        return parquet_files(lake)[0]

    def test_an_intact_partition_still_reads_as_ingested(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        self._ingested(lake, mirror, fetcher)
        assert is_ingested(lake, "klines", SYMBOL, KLINE_DATE)

    def test_the_receipt_records_the_published_size(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        parquet = self._ingested(lake, mirror, fetcher)
        payload = json.loads(
            receipt_path(lake, "klines", SYMBOL, KLINE_DATE).read_text(encoding="utf-8")
        )
        assert payload["parquet_bytes"] == parquet.stat().st_size

    def test_a_truncated_partition_is_not_ingested(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        parquet = self._ingested(lake, mirror, fetcher)
        parquet.write_bytes(parquet.read_bytes()[:-16])

        assert not is_ingested(lake, "klines", SYMBOL, KLINE_DATE)
        # And the plan-level consequence: the re-ingest actually runs.
        again = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        assert again.status is IngestStatus.WRITTEN
        assert is_ingested(lake, "klines", SYMBOL, KLINE_DATE)

    def test_a_zero_byte_partition_is_not_ingested(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        parquet = self._ingested(lake, mirror, fetcher)
        parquet.write_bytes(b"")
        assert not is_ingested(lake, "klines", SYMBOL, KLINE_DATE)

    def test_a_legacy_receipt_falls_back_to_the_footer_row_count(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Receipts written before `parquet_bytes` existed stay valid -- a version bump
        would force a re-download of every archive ever ingested -- and are checked
        against the row count they do record, read from the footer alone."""
        parquet = self._ingested(lake, mirror, fetcher)
        path = receipt_path(lake, "klines", SYMBOL, KLINE_DATE)
        payload = json.loads(path.read_text(encoding="utf-8"))
        del payload["parquet_bytes"]
        path.write_text(json.dumps(payload), encoding="utf-8")

        assert is_ingested(lake, "klines", SYMBOL, KLINE_DATE)

        parquet.write_bytes(b"not parquet at all")
        assert not is_ingested(lake, "klines", SYMBOL, KLINE_DATE)

    def test_a_failed_parse_leaves_the_partition_empty(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """A row that fails halfway through must not leave a truncated Parquet visible."""
        publish(
            mirror,
            "klines",
            KLINE_DATE,
            [kline_row_at(0), kline_row_at(1).replace("65014.60", "??")],
        )

        with pytest.raises(MalformedArchive, match="line 2"):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        directory = (
            partition_dir(lake, "klines") / "interval=1m" / "year=2026" / "month=07"
        )
        assert list(directory.iterdir()) == []
        assert not receipt_path(lake, "klines", SYMBOL, KLINE_DATE).exists()

    def test_a_decimal_conversion_error_still_names_the_line(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """`money.to_scaled` sends anything containing `e` through `Decimal`.

        `"not-a-price"` ends in one, so it takes that branch, and raw `Decimal` would
        raise `decimal.InvalidOperation` -- an `ArithmeticError`, not the `ValueError`
        every other malformed field produces. Uncaught it escapes as a bare
        `[<class 'ConversionSyntax'>]` with no archive, no column and no line number,
        which is what a 2400-file backfill would report at hour six.

        `money.to_scaled` normalises it to `ValueError` at the seam, so the ordinary
        chain works: `bulk_layout._to_scaled` adds the column, this module adds the
        archive and the line. All three are asserted here, because each was separately
        absent at some point and any one missing makes the other two much less useful.
        """
        publish(
            mirror,
            "klines",
            KLINE_DATE,
            [kline_row_at(0), kline_row_at(1).replace("65014.60", "not-a-price")],
        )

        with pytest.raises(
            MalformedArchive,
            match=r"BTCUSDT-1m-2026-07-15\.csv line 2: ValueError: klines\.open: ",
        ):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        assert parquet_files(lake) == []

    def test_sweep_removes_only_old_scratch_files(self, lake: Path) -> None:
        scratch = lake / "_ingest" / "tmp"
        scratch.mkdir(parents=True)
        fresh = scratch / "fresh.zip.part"
        stale = scratch / "stale.zip.part"
        fresh.write_bytes(b"x")
        stale.write_bytes(b"x")
        old = time.time() - 200_000
        os.utime(stale, (old, old))

        assert sweep_stale_downloads(lake) == 1
        assert fresh.exists() and not stale.exists()


class TestArchiveIntegrity:
    def test_an_archive_holding_the_wrong_member_is_refused(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        payload = zip_bytes("something-else.csv", [KLINE_ROW])
        publish(mirror, "klines", KLINE_DATE, [], payload=payload)

        with pytest.raises(MalformedArchive, match="exactly one member"):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

    def test_an_empty_archive_is_refused(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        payload = zip_bytes(f"{SYMBOL}-1m-{KLINE_DATE}.csv", [""])
        publish(mirror, "klines", KLINE_DATE, [], payload=payload)

        with pytest.raises(MalformedArchive, match="empty"):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

    def test_rows_from_another_partition_are_refused(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """A mislabelled archive would file real rows where no query will look."""
        # 2026-07-15's archive, but carrying a bar from August.
        august = kline_row_at(17 * 1440)
        publish(mirror, "klines", KLINE_DATE, [kline_row_at(0), august])

        with pytest.raises(MalformedArchive, match="resolves to partition"):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)
        assert parquet_files(lake) == []

    def test_a_metrics_archive_for_the_wrong_symbol_is_refused(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        row = METRICS_ROW.replace("BTCUSDT", "ETHUSDT")
        publish(mirror, "metrics", METRICS_DATE, [METRICS_HEADER, row])

        with pytest.raises(MalformedArchive, match="ETHUSDT"):
            ingest_archive(lake, SYMBOL, "metrics", METRICS_DATE, fetcher=fetcher)


class TestPeriodEnumeration:
    def test_daily_periods_are_inclusive(self) -> None:
        assert periods_for("klines", "2026-07-14", "2026-07-16") == [
            "2026-07-14",
            "2026-07-15",
            "2026-07-16",
        ]

    def test_daily_periods_cross_a_month_boundary(self) -> None:
        assert periods_for("klines", "2026-02-27", "2026-03-01") == [
            "2026-02-27",
            "2026-02-28",
            "2026-03-01",
        ]

    def test_monthly_periods_cross_a_year_boundary(self) -> None:
        assert periods_for("fundingRate", "2025-11", "2026-02") == [
            "2025-11",
            "2025-12",
            "2026-01",
            "2026-02",
        ]

    def test_a_monthly_dataset_accepts_full_dates(self) -> None:
        assert periods_for("fundingRate", "2026-06-14", "2026-07-02") == [
            "2026-06",
            "2026-07",
        ]

    def test_a_daily_dataset_refuses_a_bare_month(self) -> None:
        """Expanding it would turn a typo into a month-long download."""
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            periods_for("klines", "2026-07", "2026-07-15")

    def test_a_reversed_range_is_refused(self) -> None:
        with pytest.raises(ValueError, match="precedes start"):
            periods_for("klines", "2026-07-16", "2026-07-14")


class TestIngestRange:
    def test_a_range_writes_every_day_and_reports_them_sorted(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        for index, day in enumerate(("2026-07-15", "2026-07-16", "2026-07-17")):
            publish(mirror, "klines", day, [kline_row_at(index * 1440)])

        report = ingest_range(
            lake, SYMBOL, "klines", "2026-07-15", "2026-07-17", fetcher=fetcher
        )

        assert report.exit_code == 0
        assert [o.period for o in report.outcomes] == [
            "2026-07-15",
            "2026-07-16",
            "2026-07-17",
        ]
        assert report.count(IngestStatus.WRITTEN) == 3
        assert report.rows == 3

    def test_a_second_run_skips_everything(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        for index, day in enumerate(("2026-07-15", "2026-07-16")):
            publish(mirror, "klines", day, [kline_row_at(index * 1440)])
        ingest_range(lake, SYMBOL, "klines", "2026-07-15", "2026-07-16", fetcher=fetcher)
        downloads = len(fetcher.download_urls)

        report = ingest_range(
            lake, SYMBOL, "klines", "2026-07-15", "2026-07-16", fetcher=fetcher
        )

        assert report.count(IngestStatus.SKIPPED) == 2
        assert len(fetcher.download_urls) == downloads

    def test_a_partial_run_resumes_where_it_stopped(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        days = ("2026-07-15", "2026-07-16", "2026-07-17")
        for index, day in enumerate(days):
            publish(mirror, "klines", day, [kline_row_at(index * 1440)])
        ingest_range(lake, SYMBOL, "klines", days[0], days[0], fetcher=fetcher)

        report = ingest_range(lake, SYMBOL, "klines", days[0], days[-1], fetcher=fetcher)

        assert report.count(IngestStatus.SKIPPED) == 1
        assert report.count(IngestStatus.WRITTEN) == 2
        assert len(parquet_files(lake)) == 3

    def test_one_bad_day_does_not_abort_the_range(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", "2026-07-15", [kline_row_at(0)])
        publish(mirror, "klines", "2026-07-16", [kline_row_at(1440)], digest="a" * 64)
        publish(mirror, "klines", "2026-07-17", [kline_row_at(2880)])

        report = ingest_range(
            lake, SYMBOL, "klines", "2026-07-15", "2026-07-17", fetcher=fetcher
        )

        assert report.count(IngestStatus.WRITTEN) == 2
        assert report.count(IngestStatus.FAILED) == 1
        assert report.exit_code == 1
        assert "ChecksumMismatch" in (report.failures[0].error or "")
        assert len(parquet_files(lake)) == 2

    def test_a_missing_day_inside_coverage_is_reported_not_failed(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", "2026-07-15", [kline_row_at(0)])

        report = ingest_range(
            lake, SYMBOL, "klines", "2026-07-15", "2026-07-16", fetcher=fetcher
        )

        assert report.count(IngestStatus.MISSING) == 1
        assert report.count(IngestStatus.FAILED) == 0
        assert report.exit_code == 0

    def test_the_manifest_hook_sees_written_archives_only(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        seen: list[str] = []
        ingest_range(
            lake,
            SYMBOL,
            "klines",
            KLINE_DATE,
            KLINE_DATE,
            fetcher=fetcher,
            manifest_hook=lambda outcome: seen.append(outcome.period),
        )
        ingest_range(
            lake,
            SYMBOL,
            "klines",
            KLINE_DATE,
            KLINE_DATE,
            fetcher=fetcher,
            manifest_hook=lambda outcome: seen.append(outcome.period),
        )

        assert seen == [KLINE_DATE]

    def test_a_failing_manifest_hook_warns_but_keeps_the_data(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])

        def explode(outcome: Any) -> None:
            raise RuntimeError("manifest is on fire")

        report = ingest_range(
            lake,
            SYMBOL,
            "klines",
            KLINE_DATE,
            KLINE_DATE,
            fetcher=fetcher,
            manifest_hook=explode,
        )

        assert report.count(IngestStatus.WRITTEN) == 1
        assert any("manifest is on fire" in w for w in report.warnings)
        assert len(parquet_files(lake)) == 1

    def test_concurrent_and_serial_runs_agree(
        self, tmp_path: Path, mirror: Path
    ) -> None:
        days = [f"2026-07-{day:02d}" for day in range(15, 23)]
        for index, day in enumerate(days):
            publish(mirror, "klines", day, [kline_row_at(index * 1440)])

        serial_root, parallel_root = tmp_path / "serial", tmp_path / "parallel"
        serial = ingest_range(
            serial_root,
            SYMBOL,
            "klines",
            days[0],
            days[-1],
            fetcher=LocalDirectoryFetcher(mirror),
            concurrency=1,
        )
        parallel = ingest_range(
            parallel_root,
            SYMBOL,
            "klines",
            days[0],
            days[-1],
            fetcher=LocalDirectoryFetcher(mirror),
            concurrency=4,
        )

        assert [(o.period, o.status, o.rows) for o in serial.outcomes] == [
            (o.period, o.status, o.rows) for o in parallel.outcomes
        ]
        assert [p.name for p in parquet_files(serial_root)] == [
            p.name for p in parquet_files(parallel_root)
        ]

    def test_an_empty_archive_is_written_and_flagged(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """"Published but empty" and "never published" are different facts."""
        publish(mirror, "metrics", METRICS_DATE, [METRICS_HEADER])

        report = ingest_range(
            lake, SYMBOL, "metrics", METRICS_DATE, METRICS_DATE, fetcher=fetcher
        )

        assert report.count(IngestStatus.WRITTEN) == 1
        assert report.rows == 0
        assert any("no data rows" in w for w in report.warnings)
        assert pq.read_table(parquet_files(lake)[0]).num_rows == 0


class TestUnavailableDatasets:
    def test_liquidation_snapshot_is_reported_not_skipped(self, lake: Path) -> None:
        """Finding F2. A silent omission looks identical to having forgotten it."""
        report = ingest_range(
            lake, SYMBOL, "liquidationSnapshot", "2026-07-15", "2026-07-16"
        )

        assert report.count(IngestStatus.UNAVAILABLE) == 1
        assert report.exit_code == 1
        assert any("F2" in w for w in report.warnings)
        assert "forceOrder" in (report.outcomes[0].error or "")
        assert not lake.joinpath("liquidations").exists()

    def test_ingesting_one_unavailable_archive_raises(
        self, lake: Path, fetcher: CountingFetcher
    ) -> None:
        with pytest.raises(DatasetUnavailable, match="F2"):
            ingest_archive(
                lake, SYMBOL, "liquidationSnapshot", "2026-07-15", fetcher=fetcher
            )


class TestCoverageWarnings:
    def test_book_ticker_outside_the_f1_window_warns_clearly(
        self, lake: Path, fetcher: CountingFetcher
    ) -> None:
        report = ingest_range(
            lake, SYMBOL, "bookTicker", "2025-01-01", "2025-01-03", fetcher=fetcher
        )

        joined = " ".join(report.warnings)
        assert "2024-03-30" in joined
        assert "2023-05-16" in joined
        assert "DATA_AVAILABILITY" in joined
        # Not failures -- the days were never published, and calling them failures would
        # send someone hunting for a network fault.
        assert report.count(IngestStatus.UNPUBLISHED) == 3
        assert report.count(IngestStatus.FAILED) == 0

    def test_the_registry_caveat_is_surfaced_verbatim(
        self, lake: Path, fetcher: CountingFetcher
    ) -> None:
        caveat = bulk_dataset("bookTicker").caveat
        assert caveat is not None
        report = ingest_range(
            lake, SYMBOL, "bookTicker", "2025-01-01", "2025-01-01", fetcher=fetcher
        )
        assert any(caveat in w for w in report.warnings)

    def test_inside_the_window_there_is_no_coverage_warning(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(
            mirror, "bookTicker", BOOK_TICKER_DATE, [BOOK_TICKER_HEADER, BOOK_TICKER_ROW]
        )
        report = ingest_range(
            lake,
            SYMBOL,
            "bookTicker",
            BOOK_TICKER_DATE,
            BOOK_TICKER_DATE,
            fetcher=fetcher,
        )

        assert report.count(IngestStatus.WRITTEN) == 1
        assert not any("outside" in w for w in report.warnings)

    def test_klines_before_the_first_published_day_warn(
        self, lake: Path, fetcher: CountingFetcher
    ) -> None:
        report = ingest_range(
            lake, SYMBOL, "klines", "2019-12-29", "2019-12-30", fetcher=fetcher
        )
        assert any("2019-12-31" in w for w in report.warnings)
        assert report.count(IngestStatus.UNPUBLISHED) == 2


class TestReportRendering:
    def test_render_names_the_failures(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", "2026-07-15", [kline_row_at(0)])
        publish(mirror, "klines", "2026-07-16", [kline_row_at(1440)], digest="b" * 64)

        report = ingest_range(
            lake, SYMBOL, "klines", "2026-07-15", "2026-07-16", fetcher=fetcher
        )
        text = report.render()

        assert "FAILED 2026-07-16" in text
        assert "written 1" in text
        assert "failed 1" in text

    def test_an_interrupted_report_exits_non_zero(self, lake: Path) -> None:
        """Partial success is still a range the caller does not have."""
        from perplab.data.ingest_bulk import IngestReport

        report = IngestReport(symbol=SYMBOL, dataset="klines", root=lake)
        report.periods_requested = 10
        report.interrupted = True
        assert report.exit_code == 1
        assert report.not_attempted == 10
        assert "INTERRUPTED" in report.render()


class TestProgress:
    """A multi-hour backfill is only debuggable through what it printed."""

    def test_it_reports_files_bytes_rate_eta_and_running_counts(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        from perplab.data.ingest_bulk import TextProgress

        publish(mirror, "klines", "2026-07-15", [kline_row_at(0)])
        publish(mirror, "klines", "2026-07-16", [kline_row_at(1440)], digest="c" * 64)
        stream = io.StringIO()

        ingest_range(
            lake,
            SYMBOL,
            "klines",
            "2026-07-15",
            "2026-07-16",
            fetcher=fetcher,
            concurrency=1,
            progress=TextProgress(stream, heartbeat_s=0.0),
        )
        text = stream.getvalue()

        assert "ingest klines BTCUSDT: 2 periods" in text
        assert "2026-07-15" in text and "2026-07-16" in text
        assert "written" in text and "failed" in text
        assert "/s" in text  # rate
        assert "ETA " in text
        assert "[    1/2    ]" in text  # a running file counter, not just a total

    def test_it_survives_concurrent_workers(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        from perplab.data.ingest_bulk import TextProgress

        days = [f"2026-07-{d:02d}" for d in range(15, 21)]
        for index, day in enumerate(days):
            publish(mirror, "klines", day, [kline_row_at(index * 1440)])
        stream = io.StringIO()

        report = ingest_range(
            lake,
            SYMBOL,
            "klines",
            days[0],
            days[-1],
            fetcher=fetcher,
            concurrency=4,
            progress=TextProgress(stream, heartbeat_s=0.0),
        )

        assert report.exit_code == 0
        assert stream.getvalue().count("written") >= len(days)

    def test_one_reporter_across_datasets_starts_each_leg_from_zero(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """`cmd_ingest` builds one `TextProgress` and loops the six PHASE1_DATASETS.

        The counter, the running tallies and the ETA are the only instrument an operator
        has over an unattended overnight run. Carrying `_done` across a `plan()` makes it
        outgrow `_total`, and `remaining = max(_total - _done, 0)` is then 0 for every
        line of every dataset after the first -- so the 320-period bookTicker leg reports
        `ETA 00:00:00` for its entire multi-hour duration.
        """
        from perplab.data.ingest_bulk import TextProgress

        days = ("2026-07-15", "2026-07-16")
        for index, day in enumerate(days):
            publish(mirror, "klines", day, [kline_row_at(index * 1440)])
            publish(mirror, "markPriceKlines", day, [kline_row_at(index * 1440)])

        stream = io.StringIO()
        progress = TextProgress(stream, heartbeat_s=0.0)
        for dataset in ("klines", "markPriceKlines"):
            ingest_range(
                lake,
                SYMBOL,
                dataset,
                days[0],
                days[-1],
                fetcher=fetcher,
                concurrency=1,
                progress=progress,
            )

        second_leg = [
            line
            for line in stream.getvalue().splitlines()
            if line.startswith("[") and "markPriceKlines" in line
        ]
        assert [line.split("]")[0] + "]" for line in second_leg] == [
            "[    1/2    ]",
            "[    2/2    ]",
        ]
        # The status tallies are per-leg too, or the second dataset opens on `writ 3`.
        assert "writ 1" in second_leg[0]
        assert "writ 2" in second_leg[1]


class TestCancellation:
    def test_an_unaccounted_period_is_not_success(self, lake: Path) -> None:
        """A worker that dies of something no `except Exception` catches leaves a hole."""
        from perplab.data.ingest_bulk import IngestReport

        report = IngestReport(symbol=SYMBOL, dataset="klines", root=lake)
        report.periods_requested = 3
        report.outcomes.append(
            FileOutcome(
                symbol=SYMBOL,
                dataset="klines",
                period="2026-07-15",
                status=IngestStatus.WRITTEN,
            )
        )
        assert report.not_attempted == 2
        assert not report.ok
        assert report.exit_code == 1

    def test_ctrl_c_mid_range_stops_cleanly_and_keeps_what_landed(
        self, lake: Path, mirror: Path
    ) -> None:
        days = ("2026-07-15", "2026-07-16", "2026-07-17")
        for index, day in enumerate(days):
            publish(mirror, "klines", day, [kline_row_at(index * 1440)])

        class InterruptingFetcher(CountingFetcher):
            def download(self, url, sink, *, on_chunk=None):  # type: ignore[no-untyped-def]
                if "2026-07-16" in url:
                    raise KeyboardInterrupt
                return super().download(url, sink, on_chunk=on_chunk)

        report = ingest_range(
            lake,
            SYMBOL,
            "klines",
            days[0],
            days[-1],
            fetcher=InterruptingFetcher(LocalDirectoryFetcher(mirror)),
            concurrency=1,
        )

        assert report.interrupted
        assert report.exit_code == 1
        assert report.not_attempted == 2
        # The day that did land is complete, verified and resumable.
        assert is_ingested(lake, "klines", SYMBOL, days[0])
        assert len(parquet_files(lake)) == 1
        assert list((lake / "_ingest" / "tmp").glob("*.zip.part")) == []

    def test_a_set_stop_flag_aborts_before_any_write(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Ctrl+C unwinds workers rather than letting a 240 MB transfer finish."""
        from perplab.data.ingest_bulk import _Interrupted

        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        stop = threading.Event()
        stop.set()

        with pytest.raises(_Interrupted):
            ingest_archive(
                lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher, stop=stop
            )
        assert parquet_files(lake) == []


class TestHttpFetcher:
    """The real HTTP path, driven through `httpx.MockTransport` rather than the network.

    A unit test that reaches data.binance.vision fails for reasons that have nothing to
    do with this code. A mock transport still exercises the genuine `httpx` client, the
    streaming read, and the status classification -- everything except the socket.
    """

    @staticmethod
    def client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_it_streams_and_reports_the_content_length(self) -> None:
        body = b"x" * 4096

        with HttpFetcher(
            client=self.client(lambda request: httpx.Response(200, content=body))
        ) as fetcher:
            sink = io.BytesIO()
            seen: list[tuple[int, int | None]] = []
            written = fetcher.download(
                "https://example/a.zip", sink, on_chunk=lambda n, t: seen.append((n, t))
            )

        assert written == len(body)
        assert sink.getvalue() == body
        assert seen and seen[0][1] == len(body)

    def test_404_is_not_published_rather_than_a_failure(self) -> None:
        with HttpFetcher(
            client=self.client(lambda request: httpx.Response(404))
        ) as fetcher:
            with pytest.raises(ArchiveNotPublished):
                fetcher.get_text("https://example/a.zip.CHECKSUM")

    @pytest.mark.parametrize("status", [429, 500, 503])
    def test_rate_limits_and_server_errors_are_transient(self, status: int) -> None:
        with HttpFetcher(
            client=self.client(lambda request: httpx.Response(status, text="nope"))
        ) as fetcher:
            with pytest.raises(TransientFetchError):
                fetcher.get_text("https://example/a")

    def test_a_ban_is_not_transient_and_names_the_collector(self) -> None:
        """418 is an IP-level block, and retrying it extends the block.

        Split from 429 deliberately. 429 means "slow down" and a backoff answers it; 418
        means the address is already blocked, so five retries per archive across a range
        is how a short block becomes a long one. The message has to name the live
        collector's REST pollers because they share the address: a ban earned by a
        re-runnable backfill silently starves a recording that cannot be re-run.
        """
        with HttpFetcher(
            client=self.client(lambda request: httpx.Response(418, text="banned"))
        ) as fetcher:
            with pytest.raises(AddressBanned) as excinfo:
                fetcher.get_text("https://example/a")
        assert not isinstance(excinfo.value, TransientFetchError)
        assert "collector" in str(excinfo.value)

    def test_a_client_error_is_not_retried(self) -> None:
        """Retrying a 403 burns time and cannot ever succeed (cf. exchange/rest.py)."""
        with HttpFetcher(
            client=self.client(lambda request: httpx.Response(403, text="forbidden"))
        ) as fetcher:
            with pytest.raises(BulkIngestError) as excinfo:
                fetcher.get_text("https://example/a")
        assert not isinstance(excinfo.value, TransientFetchError)

    def test_a_retry_restarts_the_hash_rather_than_appending(
        self, lake: Path
    ) -> None:
        """The reason retries live above the fetcher and not inside it.

        A retry that resumed into the same hasher would fold two partial transfers into
        one digest, and the mismatch would be reported as a corrupt archive -- sending
        whoever reads the report looking for a fault at Binance that is not there.
        """
        stem = f"{SYMBOL}-1m-{KLINE_DATE}"
        payload = zip_bytes(f"{stem}.csv", [KLINE_ROW])
        digest = hashlib.sha256(payload).hexdigest()
        attempts: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url.endswith(".CHECKSUM"):
                return httpx.Response(200, text=f"{digest}  {stem}.zip\n")
            attempts.append(url)
            if len(attempts) == 1:
                # Half the archive, then a server error on the next attempt's worth.
                return httpx.Response(503, text="try again")
            return httpx.Response(200, content=payload)

        with HttpFetcher(client=self.client(handler)) as fetcher:
            outcome = ingest_archive(
                lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher, max_attempts=3
            )

        assert outcome.status is IngestStatus.WRITTEN
        assert len(attempts) == 2
        assert outcome.rows == 1


class TestRegistryAlignment:
    """Guards against this module drifting from the foundation it depends on."""

    @pytest.mark.parametrize(
        "dataset",
        [
            "klines",
            "markPriceKlines",
            "aggTrades",
            "bookTicker",
            "fundingRate",
            "metrics",
            "bookDepth",
        ],
    )
    def test_every_available_dataset_can_resolve_a_partition(self, dataset: str) -> None:
        entry: BulkDataset = bulk_dataset(dataset)
        assert entry.available
        assert entry.target_dataset is not None
        assert entry.target_schema is not None
        period = "2026-07" if entry.cadence == "monthly" else "2026-07-15"
        assert entry.archive_url(SYMBOL, period).endswith(".zip")
        assert entry.checksum_url(SYMBOL, period).endswith(".zip.CHECKSUM")


class TestPlanRange:
    """The dry-run planner, which is the only thing standing between an operator and 77 GB.

    Every assertion here is about the plan being computed from the *local* ledger: it must
    not touch the network, because a warning that costs most of what it is warning about is
    a warning people learn to skip.
    """

    def test_counts_the_periods_it_would_fetch(self, lake: Path) -> None:
        plan = plan_range(lake, SYMBOL, "klines", "2026-07-01", "2026-07-03")
        assert plan.periods == ("2026-07-01", "2026-07-02", "2026-07-03")
        assert plan.to_fetch == plan.periods
        assert plan.already == ()

    def test_makes_no_network_calls(self, lake: Path, fetcher: CountingFetcher) -> None:
        plan_range(lake, SYMBOL, "bookTicker", "2023-06-01", "2023-06-30")
        assert fetcher.text_urls == []
        assert fetcher.download_urls == []

    def test_already_ingested_periods_are_reported_not_silently_dropped(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """"Nothing to do" and "this range was never requested" look identical otherwise."""
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        plan = plan_range(lake, SYMBOL, "klines", KLINE_DATE, "2026-07-16")
        assert plan.already == (KLINE_DATE,)
        assert plan.to_fetch == ("2026-07-16",)

    def test_force_plans_the_refetch_it_would_cause(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_ROW])
        ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        plan = plan_range(lake, SYMBOL, "klines", KLINE_DATE, KLINE_DATE, force=True)
        assert plan.already == ()
        assert plan.to_fetch == (KLINE_DATE,)

    def test_periods_outside_published_coverage_are_named(self, lake: Path) -> None:
        """Finding F1: bookTicker stops on 2024-03-30, and a run of bare 404s reads as
        "the exchange had no data then"."""
        plan = plan_range(lake, SYMBOL, "bookTicker", "2024-03-29", "2024-04-02")
        assert plan.outside == ("2024-03-31", "2024-04-01", "2024-04-02")
        assert plan.to_fetch == ("2024-03-29", "2024-03-30")
        assert "2024-03-30" in plan.render()

    def test_the_estimate_conveys_scale(self, lake: Path) -> None:
        """The whole point: bookTicker's published window is tens of gigabytes."""
        plan = plan_range(lake, SYMBOL, "bookTicker", "2023-05-16", "2024-03-30")
        assert plan.estimated_bytes is not None
        assert plan.estimated_bytes > 50 * 1024**3

    def test_an_unmeasured_dataset_estimates_None_not_zero(self, lake: Path) -> None:
        """A fabricated zero would answer the question this type exists to ask."""
        plan = plan_range(lake, SYMBOL, "klines", "2026-07-01", "2026-07-02")
        assert plan.estimated_bytes is not None

        original = TYPICAL_BYTES_PER_PERIOD.pop("klines")
        try:
            unknown = plan_range(lake, SYMBOL, "klines", "2026-07-01", "2026-07-02")
            assert unknown.estimated_bytes is None
            assert "unknown size" in unknown.render()
        finally:
            TYPICAL_BYTES_PER_PERIOD["klines"] = original

    def test_an_unavailable_dataset_plans_nothing_and_says_why(self, lake: Path) -> None:
        plan = plan_range(lake, SYMBOL, "liquidationSnapshot", "2026-07-01", "2026-07-02")
        assert plan.available is False
        assert plan.to_fetch == ()
        assert "UNAVAILABLE" in plan.render()
        assert "F2" in plan.render()

    def test_the_lake_dataset_name_is_accepted(self, lake: Path) -> None:
        assert (
            plan_range(lake, SYMBOL, "funding", "2026-06", "2026-07").dataset
            == "fundingRate"
        )

    def test_a_lowercase_symbol_is_normalised(self, lake: Path) -> None:
        assert plan_range(lake, "btcusdt", "klines", "2026-07-01", "2026-07-01").symbol == (
            SYMBOL
        )


class TestCoverageTableDoesNotDrift:
    """`PUBLISHED_COVERAGE` and the registry caveats state the same dates twice.

    The ingest author flagged the duplication and settled for a comment, because the third
    copy lives in `docs/DATA_AVAILABILITY.md` and asserting against that would mean parsing
    prose. The two *code* copies can be tied together though, and finding F1's window is
    exactly the number a reader would trust from whichever they happened to open.
    """

    def test_the_bookticker_window_matches_its_caveat(self) -> None:
        first, last = PUBLISHED_COVERAGE["bookTicker"]
        caveat = bulk_dataset("bookTicker").caveat or ""
        assert first in caveat
        assert last is not None and last in caveat

    def test_every_covered_dataset_is_a_real_one(self) -> None:
        for name in PUBLISHED_COVERAGE:
            assert bulk_dataset(name).name == name

    def test_every_available_dataset_has_a_coverage_window(self) -> None:
        """A dataset with no entry silently gets no out-of-window warning, so a request
        outside its history comes back as bare 404s that read as an absence of market."""
        for name, entry in BULK_DATASETS.items():
            if entry.available:
                assert name in PUBLISHED_COVERAGE

    def test_the_bounds_are_in_the_datasets_own_period_format(self) -> None:
        """Bounds are compared lexicographically, which is only exact if the format
        matches the cadence."""
        for name, (first, last) in PUBLISHED_COVERAGE.items():
            width = 10 if bulk_dataset(name).cadence == "daily" else 7
            assert len(first) == width
            assert last is None or len(last) == width


class TestPhase1DatasetSet:
    def test_every_default_dataset_is_actually_fetchable(self) -> None:
        """A default that always exits non-zero teaches an operator to ignore exit codes."""
        for name in PHASE1_DATASETS:
            assert bulk_dataset(name).available

    def test_liquidations_are_excluded_but_still_registered(self) -> None:
        """Spec 13 lists liquidations in Phase 1; finding F2 says they cannot be fetched.
        Absent *and* unmentioned would be indistinguishable from forgotten, which is how
        the spec came to claim they were available."""
        assert "liquidationSnapshot" not in PHASE1_DATASETS
        assert bulk_dataset("liquidationSnapshot").caveat is not None

    def test_klines_come_first(self) -> None:
        """The tick rules cross-check against klines (spec 4.5 rule 2), so a run
        interrupted halfway still leaves a lake whose gap report can be computed."""
        assert PHASE1_DATASETS[0] == "klines"


class TestConcurrentIngestsDoNotShareAWorkingFile:
    """Two ingests of the same period must not write the same temporary path.

    The published Parquet is one `os.replace` and the receipt is written after it, which
    is what makes a re-ingest idempotent. That reasoning holds only while each writer owns
    its own working file. When the temporary name was derived from the output name alone,
    two processes -- two clicks of a UI button, a scheduled run beside a manual one --
    opened the *same* `.data.parquet.tmp`, and one `os.replace` fired against bytes the
    other was still appending. The surviving receipt then described the other run's file,
    `parquet_bytes` disagreed, and `is_ingested` answered False for that period forever:
    it re-downloaded on every pass and never settled.
    """

    def test_a_foreign_working_file_is_untouched_and_the_ingest_still_settles(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """A working file belonging to another pid is neither read, written nor removed.

        Stands in for "another ingest is mid-transfer": if this run can complete and settle
        while that file sits in the same directory, the two names are genuinely disjoint.
        """
        publish(mirror, "klines", KLINE_DATE, [KLINE_HEADER, KLINE_ROW])
        partition = lake / "klines" / f"symbol={SYMBOL}" / f"date={KLINE_DATE}"
        partition.mkdir(parents=True, exist_ok=True)
        foreign = partition / ".data.parquet.999999.tmp"
        foreign.write_bytes(b"another process is mid-write")

        outcome = ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert outcome.status is IngestStatus.WRITTEN
        assert foreign.read_bytes() == b"another process is mid-write"
        assert is_ingested(lake, "klines", SYMBOL, KLINE_DATE)

    def test_the_working_file_carries_the_writing_process(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Pins the naming itself, so the disjointness above cannot regress silently."""
        publish(mirror, "klines", KLINE_DATE, [KLINE_HEADER, KLINE_ROW])
        seen: list[str] = []
        real_replace = os.replace

        def spy(src: Any, dst: Any) -> None:
            seen.append(Path(src).name)
            real_replace(src, dst)

        with mock.patch("perplab.data.ingest_bulk.os.replace", spy):
            ingest_archive(lake, SYMBOL, "klines", KLINE_DATE, fetcher=fetcher)

        assert seen, "the archive was never published"
        assert str(os.getpid()) in seen[0]
        assert seen[0].startswith(".") and seen[0].endswith(".tmp")


class TestABanEndsTheRunRatherThanRetryingIt:
    """418 is address-level and shared with the collector; retrying extends it."""

    def test_the_first_ban_stops_the_remaining_periods(
        self, lake: Path, mirror: Path
    ) -> None:
        """Every later period would meet the same block, and each attempt prolongs it.

        Contrast the generic handler, whose rule is "one bad day must not end the run" --
        correct for a corrupt archive, wrong for a condition that belongs to the address
        rather than to the day.
        """
        for date in ("2026-07-15", "2026-07-16", "2026-07-17"):
            publish(mirror, "klines", date, [KLINE_HEADER, KLINE_ROW])

        class Banning:
            def __init__(self) -> None:
                self.calls = 0

            def get_text(self, url: str) -> str:
                self.calls += 1
                raise AddressBanned(f"HTTP 418 for {url}: collector affected too")

            def download(self, url: str, sink: Any, *, on_chunk: Any = None) -> int:
                raise AssertionError("a banned run must not transfer anything")

        banning = Banning()
        report = ingest_range(
            lake, SYMBOL, "klines", "2026-07-15", "2026-07-17",
            fetcher=banning, concurrency=1,
        )

        assert report.exit_code != 0
        assert report.count(IngestStatus.WRITTEN) == 0
        # One period met the ban; the rest were abandoned rather than each earning
        # their own five retries against an address that is already blocked.
        assert banning.calls == 1
        assert report.not_attempted == 2


class TestAnExternalStopCancelsWithoutAConsole:
    """A background job has no way to raise KeyboardInterrupt in the worker."""

    def test_an_already_set_event_transfers_nothing(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        publish(mirror, "klines", KLINE_DATE, [KLINE_HEADER, KLINE_ROW])
        stop = threading.Event()
        stop.set()

        report = ingest_range(
            lake, SYMBOL, "klines", KLINE_DATE, KLINE_DATE,
            fetcher=fetcher, concurrency=1, stop=stop,
        )

        assert fetcher.download_urls == []
        assert report.count(IngestStatus.WRITTEN) == 0
        assert report.exit_code != 0, "an unfinished range is not a satisfied one"


class TestFillDaysFromMonthly:
    """Days whose daily archive 404s but whose monthly archive holds them (finding F7).

    F7 concluded Binance never published 56 `markPriceKlines` days because
    `daily/markPriceKlines/.../BTCUSDT-1m-2021-01-18.zip` is a 404. The monthly archive for
    the same month is a 200 carrying all 31 days. A 404 locates a key, not a fact.
    """

    MINUTE_MS = 60_000
    DAY_MS = 86_400_000

    def _day_lines(self, day_start_ms: int, minutes: int = 3) -> list[str]:
        """`markPriceKlines` CSV rows. Volume columns are always 0 -- data, not a gap."""
        lines = []
        for index in range(minutes):
            open_ms = day_start_ms + index * self.MINUTE_MS
            price = f"{35000 + index}.12345678"
            lines.append(
                f"{open_ms},{price},{price},{price},{price},0,"
                f"{open_ms + self.MINUTE_MS - 1},0,60,0,0,0"
            )
        return lines

    def _publish_monthly(
        self, mirror: Path, month: str, day_starts: dict[str, int]
    ) -> None:
        monthly = dataclass_replace(bulk_dataset("markPriceKlines"), cadence="monthly")
        directory = mirror.joinpath(*monthly.path_prefix(SYMBOL).rstrip("/").split("/"))
        directory.mkdir(parents=True, exist_ok=True)
        stem = monthly.file_stem(SYMBOL, month)
        lines: list[str] = []
        for start in day_starts.values():
            lines.extend(self._day_lines(start))
        data = zip_bytes(f"{stem}.csv", lines)
        (directory / f"{stem}.zip").write_bytes(data)
        (directory / f"{stem}.zip.CHECKSUM").write_text(
            f"{hashlib.sha256(data).hexdigest()}  {stem}.zip\n", encoding="utf-8"
        )

    def _starts(self, *days: str) -> dict[str, int]:
        epoch = date(1970, 1, 1)
        return {d: (date.fromisoformat(d) - epoch).days * self.DAY_MS for d in days}

    def test_writes_one_file_per_day_rather_than_one_for_the_month(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Publishing the month as a single file would sit beside the per-day files already
        in the partition, and every reader globbing it would count the overlap twice."""
        days = self._starts("2021-01-18", "2021-01-19", "2021-01-20")
        self._publish_monthly(mirror, "2021-01", days)

        written = fill_days_from_monthly(
            lake, SYMBOL, "markPriceKlines", "2021-01", list(days), fetcher=fetcher
        )

        assert [o.status for o in written] == [IngestStatus.WRITTEN] * 3
        assert all(o.rows == 3 for o in written)
        partition = lake / "markPriceKlines" / f"symbol={SYMBOL}" / "year=2021" / "month=01"
        assert sorted(p.name for p in partition.glob("*.parquet")) == [
            "2021-01-18.parquet",
            "2021-01-19.parquet",
            "2021-01-20.parquet",
        ]
        # One monthly archive fetched, not one request per day.
        assert len(fetcher.download_urls) == 1
        assert "monthly" in fetcher.download_urls[0]

    def test_never_overwrites_a_day_the_lake_already_holds(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """The monthly archive is a superset of the whole month, so 'fill everything in it'
        would rewrite days that were already settled from their own daily archive."""
        days = self._starts("2021-01-18", "2021-01-19")
        self._publish_monthly(mirror, "2021-01", days)
        partition = lake / "markPriceKlines" / f"symbol={SYMBOL}" / "year=2021" / "month=01"
        partition.mkdir(parents=True)
        settled = partition / "2021-01-18.parquet"
        settled.write_bytes(b"already here, written by the daily path")

        written = fill_days_from_monthly(
            lake, SYMBOL, "markPriceKlines", "2021-01", list(days), fetcher=fetcher
        )

        assert [o.period for o in written] == ["2021-01-19"]
        assert settled.read_bytes() == b"already here, written by the daily path"

    def test_asking_only_for_days_already_held_fetches_nothing(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """The existence check runs before the download, so a repeat run is free."""
        self._publish_monthly(mirror, "2021-01", self._starts("2021-01-18"))
        partition = lake / "markPriceKlines" / f"symbol={SYMBOL}" / "year=2021" / "month=01"
        partition.mkdir(parents=True)
        (partition / "2021-01-18.parquet").write_bytes(b"held")

        assert (
            fill_days_from_monthly(
                lake, SYMBOL, "markPriceKlines", "2021-01", ["2021-01-18"], fetcher=fetcher
            )
            == []
        )
        assert fetcher.download_urls == []
        assert fetcher.text_urls == []

    def test_the_receipt_names_the_archive_the_rows_actually_came_from(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Without the provenance fields the receipt for 2021-01-18 carries a sha256
        belonging to no file at that day's URL, which 404s -- so anyone verifying it later
        finds nothing to verify against."""
        self._publish_monthly(mirror, "2021-01", self._starts("2021-01-18"))

        fill_days_from_monthly(
            lake, SYMBOL, "markPriceKlines", "2021-01", ["2021-01-18"], fetcher=fetcher
        )

        receipt = json.loads(
            receipt_path(lake, "markPriceKlines", SYMBOL, "2021-01-18").read_text("utf-8")
        )
        assert receipt["period"] == "2021-01-18"
        assert receipt["source_period"] == "2021-01"
        assert receipt["source_cadence"] == "monthly"
        # And the day now counts as ingested, so a later top-up does not re-probe its 404.
        assert is_ingested(lake, "markPriceKlines", SYMBOL, "2021-01-18")

    def test_a_day_the_monthly_archive_does_not_carry_is_reported_not_invented(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """The whole point is to stop guessing about absent days, so an absence here is a
        MISSING outcome rather than an empty file that reads as a covered day."""
        self._publish_monthly(mirror, "2021-01", self._starts("2021-01-18"))

        written = fill_days_from_monthly(
            lake,
            SYMBOL,
            "markPriceKlines",
            "2021-01",
            ["2021-01-18", "2021-01-19"],
            fetcher=fetcher,
        )

        by_period = {o.period: o for o in written}
        assert by_period["2021-01-18"].status == IngestStatus.WRITTEN
        assert by_period["2021-01-19"].status == IngestStatus.MISSING
        partition = lake / "markPriceKlines" / f"symbol={SYMBOL}" / "year=2021" / "month=01"
        assert not (partition / "2021-01-19.parquet").exists()

    def test_a_day_outside_the_named_month_is_refused(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Silently ignoring it would report success for a day nothing had fetched."""
        self._publish_monthly(mirror, "2021-01", self._starts("2021-01-18"))
        with pytest.raises(ValueError, match="not a day of"):
            fill_days_from_monthly(
                lake, SYMBOL, "markPriceKlines", "2021-01", ["2021-02-01"], fetcher=fetcher
            )

    def test_a_corrupt_monthly_archive_writes_nothing(
        self, lake: Path, mirror: Path, fetcher: CountingFetcher
    ) -> None:
        """Checksum first, and a mismatch has to leave the lake exactly as it was."""
        days = self._starts("2021-01-18")
        self._publish_monthly(mirror, "2021-01", days)
        monthly = dataclass_replace(bulk_dataset("markPriceKlines"), cadence="monthly")
        directory = mirror.joinpath(*monthly.path_prefix(SYMBOL).rstrip("/").split("/"))
        stem = monthly.file_stem(SYMBOL, "2021-01")
        (directory / f"{stem}.zip").write_bytes(b"corrupted")

        with pytest.raises(ChecksumMismatch):
            fill_days_from_monthly(
                lake, SYMBOL, "markPriceKlines", "2021-01", list(days), fetcher=fetcher
            )
        partition = lake / "markPriceKlines" / f"symbol={SYMBOL}" / "year=2021" / "month=01"
        assert list(partition.glob("*.parquet")) == []
