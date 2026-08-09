"""Tests for the DuckDB view layer and timeframe derivation.

Two of these carry more weight than the rest.

The pruning tests assert on the *query plan* -- how many Parquet files DuckDB will open --
rather than on the result. A query that reads every file in the lake returns exactly the
same rows as one that reads two, so a result-only test would pass while Phase 1's "<2 s"
exit criterion quietly stopped holding as the lake grew.

The timeframe tests assert exact close times. A derived 4 h bar that closes at
`start + 4h` instead of `start + 4h - 1` is wrong by one millisecond, produces identical
OHLCV, and lets a strategy see the bar one tick before it closed. It breaks the
no-look-ahead guarantee (spec 6.2) only at timeframe boundaries, which is a defect that
survives every eyeball test of an equity curve.

The fixture lake is written with the real `ParquetBufferedWriter` rather than hand-built
directories, so a partition-path change in the writer surfaces here as a failing query
instead of an empty result.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from perplab.core.money import SCALE
from perplab.data import query as q
from perplab.data.schemas import (
    COLLECTOR_EVENTS,
    DEPTH20,
    FUNDING,
    KLINES,
    METRICS,
    SCHEMAS,
)
from perplab.data.writer import ParquetBufferedWriter

# 2024-01-01T00:00:00Z. Every timestamp below is an offset from this, so a reader can
# check any assertion by adding minutes rather than decoding an epoch.
ANCHOR = 1_704_067_200_000
MINUTE = 60_000
HOUR = 3_600_000
DAY = 86_400_000

_PX_BASE = 100 * SCALE
_PX_STEP = SCALE // 4
"""0.25, chosen because it is exact in binary floating point: the unscaled-view tests can
then compare `== 100.25` rather than `approx(100.25)`, and a genuine scaling error cannot
hide inside a tolerance."""


def _bar(open_time: int, index: int, *, volume: int = SCALE, count: int = 3) -> dict[str, int]:
    """A synthetic 1 m bar whose OHLC is a known function of `index`.

    Monotonically rising, so the aggregate of bars `i0 .. i0+n-1` has a closed form: open
    at `i0`, close at `i0+n`, high at `i0+n+1`, low at `i0-1` (in units of `_PX_STEP`).
    The tests state those expectations directly instead of recomputing them with the same
    aggregation they are meant to check.
    """
    o = _PX_BASE + index * _PX_STEP
    return {
        "open_time": open_time,
        # Binance's own convention, verified: a 1 m bar opening at 1784073600000 closes at
        # 1784073659999. Stored, never derived.
        "close_time": open_time + 59_999,
        "open": o,
        "high": o + 2 * _PX_STEP,
        "low": o - _PX_STEP,
        "close": o + _PX_STEP,
        "volume": volume,
        "quote_volume": 10 * volume,
        "count": count,
        "taker_buy_volume": volume // 2,
        "taker_buy_quote_volume": 5 * volume,
    }


def _write_klines(
    root: Path, symbol: str, start_ms: int, n: int, *, first_index: int = 0
) -> None:
    with ParquetBufferedWriter(root, "klines", KLINES, symbol=symbol) as w:
        for i in range(n):
            w.append(_bar(start_ms + i * MINUTE, first_index + i))


@pytest.fixture(scope="session")
def lake(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A lake exercising every partition layout, plus datasets that were never ingested.

    `liquidations`, `bookTicker`, `markPrice`, `markPriceKlines` and `bookDepth` are
    deliberately absent. That is the normal state of a real lake -- `bookTicker` stopped
    being published in 2024 (finding F1) and the collector-only datasets only accumulate
    forward -- and the connection has to survive it.
    """
    root = tmp_path_factory.mktemp("lake")

    # klines: symbol / interval / year / month. A full UTC day, then two later months so
    # that month pruning has something to prune, then a second symbol.
    _write_klines(root, "BTCUSDT", ANCHOR, 1440)
    _write_klines(root, "BTCUSDT", ANCHOR + 31 * DAY, 10, first_index=5000)
    _write_klines(root, "BTCUSDT", ANCHOR + 60 * DAY, 10, first_index=6000)
    _write_klines(root, "ETHUSDT", ANCHOR, 60, first_index=9000)

    # aggTrades: symbol / date. Five consecutive days for each of two symbols.
    for symbol in ("BTCUSDT", "ETHUSDT"):
        with ParquetBufferedWriter(root, "aggTrades", SCHEMAS["aggTrades"], symbol=symbol) as w:
            for day in range(5):
                for i in range(3):
                    w.append(
                        {
                            "ts_ms": ANCHOR + day * DAY + i,
                            "recv_ms": None,  # bulk archives were never received by us
                            "agg_id": day * 10 + i,
                            "price": _PX_BASE,
                            "qty": SCALE // 2,
                            "first_trade_id": day * 10 + i,
                            "last_trade_id": day * 10 + i,
                            "is_buyer_maker": bool(i % 2),
                        }
                    )
                w.flush()

    # funding: symbol only, no time partition at all.
    with ParquetBufferedWriter(root, "funding", FUNDING, symbol="BTCUSDT") as w:
        for k in range(3):
            w.append(
                {
                    "calc_time": ANCHOR + k * 8 * HOUR,
                    "funding_interval_hours": 8,
                    # Signed: negative funding means shorts pay longs and is common.
                    "funding_rate": 5_703 if k % 2 else -5_703,
                }
            )

    # metrics: symbol / year, with the documented NULL for a blank archive cell.
    with ParquetBufferedWriter(root, "metrics", METRICS, symbol="BTCUSDT") as w:
        for k in range(4):
            w.append(
                {
                    "create_time": ANCHOR + k * 300_000,
                    "sum_open_interest": 105_550 * SCALE,
                    "sum_open_interest_value": 6_858_675_634 * SCALE,
                    "count_toptrader_long_short_ratio": None if k == 2 else SCALE + SCALE // 4,
                    "sum_toptrader_long_short_ratio": SCALE,
                    "count_long_short_ratio": SCALE,
                    "sum_taker_long_short_vol_ratio": SCALE,
                }
            )

    # depth20: list-valued scaled columns, for the unscaling path that needs a lambda.
    with ParquetBufferedWriter(root, "depth20", DEPTH20, symbol="BTCUSDT") as w:
        w.append(
            {
                "ts_ms": ANCHOR,
                "recv_ms": ANCHOR + 5,
                "last_update_id": 42,
                "bid_px": [_PX_BASE, _PX_BASE - _PX_STEP],
                "bid_qty": [SCALE, 2 * SCALE],
                "ask_px": [_PX_BASE + _PX_STEP, _PX_BASE + 2 * _PX_STEP],
                "ask_qty": [SCALE, 2 * SCALE],
            }
        )

    # collectorEvents: the one dataset with no symbol= level.
    with ParquetBufferedWriter(root, "collectorEvents", COLLECTOR_EVENTS) as w:
        w.append(
            {
                "ts_ms": ANCHOR,
                "kind": "RESTART",
                "stream": "depth20",
                "detail": "supervisor restart",
                "downtime_ms": 1_500,
            }
        )

    return root


def _fetchall(root: Path, sql: str, **kwargs: object) -> list[tuple[object, ...]]:
    connection = q.connect(root, **kwargs)  # type: ignore[arg-type]
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


# ---------------------------------------------------------------------------------------


class TestColumnClassification:
    """Every numeric lake column must be declared scaled or raw, with no overlap."""

    def test_no_column_is_both(self) -> None:
        assert not (q.SCALED_COLUMNS & q.RAW_INT_COLUMNS)

    def test_every_numeric_column_is_classified(self) -> None:
        """The import-time guard, re-asserted so its failure names the column."""
        unclassified = {
            field.name
            for schema in SCHEMAS.values()
            for field in schema
            if q._int_like(field.type)
            and field.name not in q.SCALED_COLUMNS
            and field.name not in q.RAW_INT_COLUMNS
        }
        assert not unclassified

    def test_trade_count_is_not_scaled(self) -> None:
        """`count` and `count_long_short_ratio` differ by a suffix and by everything else."""
        assert "count" in q.RAW_INT_COLUMNS
        assert "count_long_short_ratio" in q.SCALED_COLUMNS

    def test_timestamps_are_never_scaled(self) -> None:
        for name in ("ts_ms", "recv_ms", "open_time", "close_time", "calc_time", "create_time"):
            assert name in q.RAW_INT_COLUMNS


class TestHiveColumns:
    @pytest.mark.parametrize(
        ("dataset", "expected"),
        [
            ("klines", ("symbol", "interval", "year", "month")),
            ("markPriceKlines", ("symbol", "year", "month")),
            ("metrics", ("symbol", "year")),
            ("funding", ("symbol",)),
            ("aggTrades", ("symbol", "date")),
            ("depth20", ("symbol", "date")),
            # No instrument: this dataset records the collector process itself.
            ("collectorEvents", ("date",)),
        ],
    )
    def test_path_order(self, dataset: str, expected: tuple[str, ...]) -> None:
        assert q.hive_columns(dataset) == expected

    def test_unknown_dataset_raises(self) -> None:
        with pytest.raises(KeyError):
            q.hive_columns("nope")


class TestViews:
    """One view per dataset, over each of the four partition layouts."""

    def test_every_dataset_has_a_view(self, lake: Path) -> None:
        connection = q.connect(lake)
        try:
            names = {r[0] for r in connection.execute("SHOW TABLES").fetchall()}
        finally:
            connection.close()
        for dataset in q.LAKE_DATASETS:
            assert dataset in names
            assert dataset + q.UNSCALED_SUFFIX in names

    def test_month_partitioned_klines(self, lake: Path) -> None:
        rows = _fetchall(
            lake,
            'SELECT "symbol", "interval", "year", "month", count(*) FROM "klines" '
            'GROUP BY 1, 2, 3, 4 ORDER BY 1, 3, 4',
            datasets=("klines",),
        )
        assert rows == [
            ("BTCUSDT", "1m", "2024", "01", 1440),
            ("BTCUSDT", "1m", "2024", "02", 10),
            ("BTCUSDT", "1m", "2024", "03", 10),
            ("ETHUSDT", "1m", "2024", "01", 60),
        ]

    def test_date_partitioned_agg_trades(self, lake: Path) -> None:
        rows = _fetchall(
            lake,
            'SELECT "date", count(*) FROM "aggTrades" WHERE "symbol" = \'BTCUSDT\' '
            'GROUP BY 1 ORDER BY 1',
            datasets=("aggTrades",),
        )
        assert rows == [(f"2024-01-0{d + 1}", 3) for d in range(5)]

    def test_unpartitioned_funding(self, lake: Path) -> None:
        """`funding` has no time partition; the glob must still reach one level down."""
        rows = _fetchall(
            lake,
            'SELECT "symbol", count(*), min("funding_rate") FROM "funding" GROUP BY 1',
            datasets=("funding",),
        )
        assert rows == [("BTCUSDT", 3, -5_703)]

    def test_year_partitioned_metrics_preserves_nulls(self, lake: Path) -> None:
        """The blank-cell policy is NULL, and NULL has to survive the view unchanged.

        A view that coalesced it to 0 would turn "no top-trader accounts on that side"
        into "the ratio was zero", which is a different and real observation.
        """
        rows = _fetchall(
            lake,
            'SELECT "year", count(*), count("count_toptrader_long_short_ratio") '
            'FROM "metrics" GROUP BY 1',
            datasets=("metrics",),
        )
        assert rows == [("2024", 4, 3)]

    def test_collector_events_has_no_symbol_column(self, lake: Path) -> None:
        names = [
            r[0]
            for r in _fetchall(
                lake, 'DESCRIBE "collectorEvents"', datasets=("collectorEvents",)
            )
        ]
        assert "symbol" not in names
        assert names[-1] == "date"

    def test_column_order_is_schema_then_path(self, lake: Path) -> None:
        described = [
            r[0] for r in _fetchall(lake, 'DESCRIBE "klines"', datasets=("klines",))
        ]
        assert described == [f.name for f in KLINES] + [
            "symbol",
            "interval",
            "year",
            "month",
        ]

    def test_partition_keys_are_varchar(self, lake: Path) -> None:
        """Pinned, not autodetected.

        Left to DuckDB, `year=2024` becomes BIGINT while `month=03` stays VARCHAR (the
        leading zero defeats the numeric sniff), so the two halves of one partition path
        would need different literal syntax in every predicate.
        """
        types = {
            r[0]: r[1] for r in _fetchall(lake, 'DESCRIBE "klines"', datasets=("klines",))
        }
        for key in ("symbol", "interval", "year", "month"):
            assert types[key] == "VARCHAR"
        assert types["open_time"] == "BIGINT"

    def test_scaled_columns_stay_int64(self, lake: Path) -> None:
        """The primary view is the integer one; nothing here silently becomes a float."""
        table = q.query(
            lake, 'SELECT * FROM "klines" LIMIT 1', datasets=("klines",)
        )
        for field in KLINES:
            assert table.schema.field(field.name).type == pa.int64()

    def test_unknown_dataset_is_refused(self, lake: Path) -> None:
        with pytest.raises(KeyError):
            q.connect(lake, datasets=("nope",))


class TestEmptyDatasets:
    """A never-ingested dataset must be queryable and empty, not an exception."""

    def test_missing_directory_yields_empty_view(self, lake: Path) -> None:
        assert not (lake / "liquidations").exists()
        rows = _fetchall(
            lake, 'SELECT count(*) FROM "liquidations"', datasets=("liquidations",)
        )
        assert rows == [(0,)]

    def test_empty_view_has_the_full_column_list(self, lake: Path) -> None:
        described = [
            r[0]
            for r in _fetchall(
                lake, 'DESCRIBE "bookTicker"', datasets=("bookTicker",)
            )
        ]
        assert described == [f.name for f in SCHEMAS["bookTicker"]] + ["symbol", "date"]

    def test_empty_and_populated_views_agree_on_types(
        self, lake: Path, tmp_path: Path
    ) -> None:
        """The whole point: a query written against a full lake runs against an empty one.

        If the fabricated types drifted from the real ones, the failure would appear only
        on whichever machine had not ingested that dataset yet.
        """
        populated = _fetchall(lake, 'DESCRIBE "klines"', datasets=("klines",))
        empty = _fetchall(tmp_path, 'DESCRIBE "klines"', datasets=("klines",))
        assert populated == empty

    def test_empty_view_accepts_the_same_predicates(self, tmp_path: Path) -> None:
        rows = _fetchall(
            tmp_path,
            'SELECT count(*) FROM "klines" WHERE "symbol" = \'BTCUSDT\' '
            'AND "year" = \'2024\' AND "open_time" >= 0',
            datasets=("klines",),
        )
        assert rows == [(0,)]

    def test_directory_present_but_no_parquet(self, tmp_path: Path) -> None:
        """Mid-flush, or after a failed ingest, a partition directory can exist empty."""
        (tmp_path / "klines" / "symbol=BTCUSDT" / "interval=1m" / "year=2024" / "month=01").mkdir(
            parents=True
        )
        assert _fetchall(tmp_path, 'SELECT count(*) FROM "klines"', datasets=("klines",)) == [
            (0,)
        ]

    def test_in_flight_tmp_file_is_invisible(self, tmp_path: Path) -> None:
        """The writer's `.<stem>.parquet.tmp` must not be picked up by the glob."""
        directory = (
            tmp_path / "klines" / "symbol=BTCUSDT" / "interval=1m" / "year=2024" / "month=01"
        )
        directory.mkdir(parents=True)
        (directory / ".part-1.parquet.tmp").write_bytes(b"not a parquet file")
        assert _fetchall(tmp_path, 'SELECT count(*) FROM "klines"', datasets=("klines",)) == [
            (0,)
        ]

    def test_entirely_empty_lake_connects(self, tmp_path: Path) -> None:
        """Every view builds on a lake with nothing in it at all."""
        connection = q.connect(tmp_path)
        try:
            for dataset in q.LAKE_DATASETS:
                got = connection.execute(f'SELECT count(*) FROM "{dataset}"').fetchone()
                assert got == (0,), dataset
        finally:
            connection.close()

    def test_one_missing_dataset_does_not_break_the_others(self, lake: Path) -> None:
        """The failure mode this exists to prevent: an unqueryable lake."""
        connection = q.connect(lake)
        try:
            assert connection.execute('SELECT count(*) FROM "liquidations"').fetchone() == (0,)
            assert connection.execute('SELECT count(*) FROM "klines"').fetchone() == (1520,)
        finally:
            connection.close()


class TestUnscaledViews:
    """Division by 10^8 for humans. Inspection and plotting only -- never accounting."""

    def test_prices_are_divided(self, lake: Path) -> None:
        rows = _fetchall(
            lake,
            'SELECT "open", "close" FROM "klines_unscaled" '
            'WHERE "symbol" = \'BTCUSDT\' AND "open_time" = ' + str(ANCHOR),
            datasets=("klines",),
        )
        assert rows == [(100.0, 100.25)]

    def test_output_is_double(self, lake: Path) -> None:
        table = q.query(
            lake, 'SELECT * FROM "klines_unscaled" LIMIT 1', datasets=("klines",)
        )
        assert table.schema.field("close").type == pa.float64()

    def test_raw_columns_are_not_divided(self, lake: Path) -> None:
        """`open_time` divided by 1e8 is a plausible-looking number and a total lie."""
        rows = _fetchall(
            lake,
            'SELECT "open_time", "count" FROM "klines_unscaled" '
            'WHERE "symbol" = \'BTCUSDT\' AND "open_time" = ' + str(ANCHOR),
            datasets=("klines",),
        )
        assert rows == [(ANCHOR, 3)]

    def test_raw_view_is_untouched(self, lake: Path) -> None:
        """The integer view stays primary; the unscaled one is a companion, not a swap."""
        rows = _fetchall(
            lake,
            'SELECT "open" FROM "klines" WHERE "symbol" = \'BTCUSDT\' '
            'AND "open_time" = ' + str(ANCHOR),
            datasets=("klines",),
        )
        assert rows == [(_PX_BASE,)]

    def test_list_columns_are_unscaled_elementwise(self, lake: Path) -> None:
        rows = _fetchall(
            lake, 'SELECT "bid_px", "bid_qty" FROM "depth20_unscaled"', datasets=("depth20",)
        )
        assert rows == [([100.0, 99.75], [1.0, 2.0])]

    def test_signed_rates_keep_their_sign(self, lake: Path) -> None:
        """Negative funding is common; nothing may clamp it."""
        rows = _fetchall(
            lake,
            'SELECT min("funding_rate") FROM "funding_unscaled"',
            datasets=("funding",),
        )
        assert rows == [(-0.00005703,)]

    def test_null_survives_unscaling(self, lake: Path) -> None:
        rows = _fetchall(
            lake,
            'SELECT count(*) - count("count_toptrader_long_short_ratio") '
            'FROM "metrics_unscaled"',
            datasets=("metrics",),
        )
        assert rows == [(1,)]

    def test_empty_unscaled_view_is_double_typed(self, tmp_path: Path) -> None:
        types = {
            r[0]: r[1]
            for r in _fetchall(tmp_path, 'DESCRIBE "klines_unscaled"', datasets=("klines",))
        }
        assert types["close"] == "DOUBLE"
        assert types["open_time"] == "BIGINT"

    def test_unscaled_views_can_be_switched_off(self, lake: Path) -> None:
        connection = q.connect(lake, datasets=("klines",), include_unscaled=False)
        try:
            names = {r[0] for r in connection.execute("SHOW TABLES").fetchall()}
        finally:
            connection.close()
        assert names == {"klines"}


class TestPartitionPredicate:
    def test_symbol_only(self) -> None:
        assert q.partition_predicate("klines", symbol="btcusdt") == "\"symbol\" = 'BTCUSDT'"

    def test_date_range_is_inclusive_of_the_last_partition(self) -> None:
        got = q.partition_predicate(
            "aggTrades", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + 2 * DAY
        )
        # end_ms is exclusive, so 2024-01-03 must not appear.
        assert got == (
            "\"symbol\" = 'BTCUSDT' AND \"date\" >= '2024-01-01' AND \"date\" <= '2024-01-02'"
        )

    def test_month_range_within_one_year_is_tight(self) -> None:
        got = q.partition_predicate(
            "klines", start_ms=ANCHOR, end_ms=ANCHOR + 61 * DAY
        )
        assert got == "\"year\" = '2024' AND \"month\" >= '01' AND \"month\" <= '03'"

    def test_month_range_across_years_falls_back_to_years(self) -> None:
        """A disjunction would be exact and would prune nothing; see the docstring."""
        got = q.partition_predicate(
            "klines", start_ms=ANCHOR, end_ms=ANCHOR + 400 * DAY
        )
        assert got == "\"year\" >= '2024' AND \"year\" <= '2025'"

    def test_unpartitioned_dataset_constrains_only_the_symbol(self) -> None:
        got = q.partition_predicate(
            "funding", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + DAY
        )
        assert got == "\"symbol\" = 'BTCUSDT'"

    def test_no_constraints_is_true(self) -> None:
        assert q.partition_predicate("klines") == "TRUE"

    def test_symbolless_dataset_refuses_a_symbol(self) -> None:
        with pytest.raises(ValueError, match="not partitioned by symbol"):
            q.partition_predicate("collectorEvents", symbol="BTCUSDT")

    def test_reversed_range_raises(self) -> None:
        with pytest.raises(ValueError, match="empty range"):
            q.partition_predicate("klines", start_ms=ANCHOR, end_ms=ANCHOR - 1)

    @pytest.mark.parametrize("symbol", ["", "BTC-USDT", "BTC USDT", "'; DROP VIEW klines--"])
    def test_implausible_symbols_are_refused(self, symbol: str) -> None:
        """Refused rather than escaped: such a symbol matches no partition path anyway."""
        with pytest.raises(ValueError, match="implausible symbol"):
            q.partition_predicate("klines", symbol=symbol)


class TestPruning:
    """Assertions on the plan, not the result -- an unpruned query returns the same rows."""

    def test_symbol_predicate_prunes_files(self, lake: Path) -> None:
        report = q.pruning_report(
            lake,
            'SELECT sum("volume") FROM "klines" WHERE "symbol" = \'BTCUSDT\'',
            datasets=("klines",),
        )
        assert len(report) == 1
        scan = report[0]
        assert scan.prunes
        assert scan.files_total == 4, "BTC x 3 months + ETH x 1 month"
        assert scan.files_scanned == 3
        assert scan.file_filters is not None and "symbol" in scan.file_filters

    def test_month_predicate_prunes_further(self, lake: Path) -> None:
        predicate = q.partition_predicate(
            "klines", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + DAY
        )
        report = q.pruning_report(
            lake,
            f'SELECT sum("volume") FROM "klines" WHERE {predicate}',
            datasets=("klines",),
        )
        assert report[0].files_scanned == 1
        assert report[0].files_total == 4

    def test_date_range_prunes(self, lake: Path) -> None:
        predicate = q.partition_predicate(
            "aggTrades", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + 2 * DAY
        )
        report = q.pruning_report(
            lake,
            f'SELECT sum("qty") FROM "aggTrades" WHERE {predicate}',
            datasets=("aggTrades",),
        )
        assert report[0].files_scanned == 2
        assert report[0].files_total == 10, "5 days x 2 symbols"

    def test_unfiltered_query_reports_no_pruning(self, lake: Path) -> None:
        """The negative control: without a file filter, nothing is pruned and we say so.

        `files_scanned is None` rather than a fabricated number -- reporting 0 here would
        read as perfect pruning, which is the opposite of the truth.
        """
        report = q.pruning_report(
            lake, 'SELECT sum("volume") FROM "klines"', datasets=("klines",)
        )
        assert not report[0].prunes
        assert report[0].files_scanned is None

    def test_derived_timeframe_query_prunes(self, lake: Path) -> None:
        """The aggregation carries its own partition predicate, or it is not fast."""
        sql = q.timeframe_sql("1h", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + DAY)
        report = q.pruning_report(lake, sql, datasets=("klines",))
        assert report[0].files_scanned == 1
        assert report[0].files_total == 4


class TestBucketArithmetic:
    """Pure integer boundary maths. Everything below depends on these being exact."""

    def test_binance_1m_convention(self) -> None:
        """The verified real bar: open 1784073600000 closes at 1784073659999."""
        assert q.bucket_start_ms(1_784_073_600_000, MINUTE) == 1_784_073_600_000
        assert q.bucket_close_ms(1_784_073_600_000, MINUTE) == 1_784_073_659_999

    @pytest.mark.parametrize("timeframe", list(q.TIMEFRAMES))
    def test_close_is_one_ms_before_the_next_open(self, timeframe: str) -> None:
        tf = q.timeframe_ms(timeframe)
        start = q.bucket_start_ms(ANCHOR + 7 * MINUTE + 1, tf)
        assert q.bucket_close_ms(ANCHOR + 7 * MINUTE + 1, tf) == start + tf - 1
        assert q.bucket_start_ms(start + tf, tf) == start + tf

    @pytest.mark.parametrize("timeframe", list(q.TIMEFRAMES))
    def test_buckets_align_to_utc_midnight(self, timeframe: str) -> None:
        tf = q.timeframe_ms(timeframe)
        assert DAY % tf == 0
        assert q.bucket_start_ms(ANCHOR, tf) == ANCHOR, "midnight opens a bucket"

    def test_last_ms_of_a_day_belongs_to_that_day(self) -> None:
        assert q.bucket_start_ms(ANCHOR + DAY - 1, DAY) == ANCHOR
        assert q.bucket_start_ms(ANCHOR + DAY, DAY) == ANCHOR + DAY

    def test_negative_epoch_floors_rather_than_truncating(self) -> None:
        """Not reachable from lake data, and correct anyway.

        A bucket function that only works for the timestamps someone happened to test is
        a landmine; truncation towards zero would put -1 ms in the bucket *after* the one
        it belongs to.
        """
        assert q.bucket_start_ms(-1, MINUTE) == -MINUTE
        assert q.bucket_close_ms(-1, MINUTE) == -1

    def test_unknown_timeframe_raises(self) -> None:
        with pytest.raises(KeyError, match="unknown timeframe"):
            q.timeframe_ms("3m")

    def test_weekly_is_absent_on_purpose(self) -> None:
        """Epoch day zero was a Thursday; a weekly bucket needs an explicit anchor."""
        assert "1w" not in q.TIMEFRAMES

    def test_sql_bucket_matches_python(self, lake: Path) -> None:
        """DuckDB's `%` truncates and Python's floors; the SQL spells the difference out.

        Asserted against each other rather than assumed, because the two only disagree
        for inputs no fixture naturally contains.
        """
        connection = q.connect(lake, datasets=("klines",))
        try:
            for tf in q.TIMEFRAMES.values():
                for ts in (0, 1, ANCHOR, ANCHOR + DAY - 1, ANCHOR + 7 * HOUR + 13, -1, -tf - 1):
                    (got,) = connection.execute(
                        f"SELECT {ts} - (({ts} % {tf}) + {tf}) % {tf}"
                    ).fetchone()
                    assert got == q.bucket_start_ms(ts, tf), (ts, tf)
        finally:
            connection.close()


class TestTimeframeDerivation:
    """Spec 4.3: store 1 m, derive the rest, and get the close times exactly right."""

    def _bars(self, lake: Path, timeframe: str, **kwargs: object) -> pa.Table:
        return q.derive_timeframe(
            lake, timeframe, symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + DAY, **kwargs
        )  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("timeframe", "expected_bars"),
        [("5m", 288), ("15m", 96), ("1h", 24), ("4h", 6), ("1d", 1)],
    )
    def test_bar_counts_over_a_full_utc_day(
        self, lake: Path, timeframe: str, expected_bars: int
    ) -> None:
        table = self._bars(lake, timeframe)
        assert table.num_rows == expected_bars
        assert all(table.column("complete").to_pylist())

    @pytest.mark.parametrize("timeframe", ["5m", "15m", "1h", "4h", "1d"])
    def test_close_time_is_exact(self, lake: Path, timeframe: str) -> None:
        """`open + tf - 1`, for every bar, at every timeframe.

        One millisecond late here and a strategy sees the bar before it closed; spec 6.2
        breaks at timeframe boundaries only, which no equity curve will show you.
        """
        tf = q.timeframe_ms(timeframe)
        table = self._bars(lake, timeframe)
        opens = table.column("open_time").to_pylist()
        closes = table.column("close_time").to_pylist()
        assert closes == [o + tf - 1 for o in opens]
        assert opens == [ANCHOR + i * tf for i in range(len(opens))]

    def test_close_time_is_computed_not_taken_from_the_last_bar(
        self, tmp_path: Path
    ) -> None:
        """An hour missing its final minute still closes at :59:59.999.

        `max(close_time)` over the constituent bars would report :58:59.999, and a
        strategy gated on that close would act a minute early. The bar is flagged
        incomplete -- but its stated close time is still the truth about the period.
        """
        _write_klines(tmp_path, "BTCUSDT", ANCHOR, 59)
        table = q.derive_timeframe(tmp_path, "1h", symbol="BTCUSDT")
        assert table.column("close_time").to_pylist() == [ANCHOR + HOUR - 1]
        assert table.column("bar_count").to_pylist() == [59]
        assert table.column("complete").to_pylist() == [False]

    def test_ohlcv_aggregation(self, lake: Path) -> None:
        """Open from the first minute, close from the last, high/low the extremes.

        The synthetic bars rise monotonically, so every expectation here has a closed
        form and is stated directly rather than recomputed by the same aggregation under
        test.
        """
        table = q.derive_timeframe(
            lake, "1h", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + HOUR
        )
        row = {name: table.column(name)[0].as_py() for name in table.column_names}
        assert row["open"] == _PX_BASE
        assert row["close"] == _PX_BASE + 60 * _PX_STEP
        assert row["high"] == _PX_BASE + 61 * _PX_STEP
        assert row["low"] == _PX_BASE - _PX_STEP
        assert row["volume"] == 60 * SCALE
        assert row["quote_volume"] == 600 * SCALE
        assert row["count"] == 180
        assert row["bar_count"] == 60
        assert row["complete"] is True

    def test_output_stays_scaled_int64(self, lake: Path) -> None:
        """No float, and no decimal128 either.

        DuckDB widens `sum(BIGINT)` to HUGEINT, which reaches Arrow as decimal128(38, 0)
        -- the one type `schemas.py` rules out of the lake, arriving through the back door
        of a query result.
        """
        table = self._bars(lake, "4h")
        for name in ("open", "high", "low", "close", "volume", "quote_volume", "count"):
            assert table.schema.field(name).type == pa.int64(), name

    def test_bars_are_never_merged_across_symbols(self, lake: Path) -> None:
        sql = q.timeframe_sql("1h", start_ms=ANCHOR, end_ms=ANCHOR + HOUR)
        table = q.query(lake, sql, datasets=("klines",))
        by_symbol = dict(
            zip(table.column("symbol").to_pylist(), table.column("bar_count").to_pylist())
        )
        assert by_symbol == {"BTCUSDT": 60, "ETHUSDT": 60}

    def test_1m_aggregation_is_the_identity(self, lake: Path) -> None:
        """Deriving 1 m from 1 m must reproduce the stored bar, close time included."""
        table = q.derive_timeframe(
            lake, "1m", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + MINUTE
        )
        stored = q.query(
            lake,
            'SELECT * FROM "klines" WHERE "symbol" = \'BTCUSDT\' '
            f'AND "open_time" = {ANCHOR}',
            datasets=("klines",),
        )
        for name in ("open_time", "close_time", "open", "high", "low", "close", "volume"):
            assert table.column(name)[0].as_py() == stored.column(name)[0].as_py(), name

    def test_non_kline_dataset_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not kline-shaped"):
            q.timeframe_sql("1h", dataset="aggTrades")


class TestAgainstRealBinanceBars:
    """Boundary arithmetic pinned to observed archive values, not to our own convention.

    `1784073600000` is the `open_time` of a real BTCUSDT 1 m kline (verified 2026-08-01
    against data.binance.vision), whose published `close_time` is `1784073659999`. It is
    also an exact UTC midnight -- `partition_key` puts it on 2026-07-15, which is the same
    instant the metrics archive of that day labels `"2026-07-15 00:00:00"`. Two
    independently verified datasets agreeing on the boundary is what makes it a fact here
    rather than an assumption.

    Binance's own 5 m/1 h/4 h/1 d archives were not available to diff against offline, so
    what is asserted is the convention those archives follow -- close is the last
    millisecond inside the bar -- applied at every derived timeframe.
    """

    REAL_OPEN = 1_784_073_600_000
    REAL_CLOSE = 1_784_073_659_999

    def test_the_published_1m_close_time_is_reproduced(self) -> None:
        assert q.bucket_close_ms(self.REAL_OPEN, MINUTE) == self.REAL_CLOSE

    @pytest.mark.parametrize("timeframe", ["5m", "15m", "1h", "4h", "1d"])
    def test_the_real_bar_opens_every_timeframe(self, timeframe: str) -> None:
        """This instant is a UTC midnight, so it opens a bucket at every timeframe."""
        tf = q.timeframe_ms(timeframe)
        assert q.bucket_start_ms(self.REAL_OPEN, tf) == self.REAL_OPEN
        assert q.bucket_close_ms(self.REAL_OPEN, tf) == self.REAL_OPEN + tf - 1

    def test_derived_bars_from_real_timestamps(self, tmp_path: Path) -> None:
        """A day of 1 m bars stamped from the real anchor, aggregated to 1 h and 1 d."""
        _write_klines(tmp_path, "BTCUSDT", self.REAL_OPEN, 1440)

        hourly = q.derive_timeframe(tmp_path, "1h", symbol="BTCUSDT")
        assert hourly.num_rows == 24
        assert hourly.column("open_time")[0].as_py() == self.REAL_OPEN
        assert hourly.column("close_time")[0].as_py() == self.REAL_OPEN + HOUR - 1
        assert all(hourly.column("complete").to_pylist())

        daily = q.derive_timeframe(tmp_path, "1d", symbol="BTCUSDT")
        assert daily.column("close_time").to_pylist() == [self.REAL_OPEN + DAY - 1]
        assert daily.column("bar_count").to_pylist() == [1440]

    def test_partitions_follow_the_same_calendar(self, tmp_path: Path) -> None:
        """The bar lands in the month its UTC date says, not the one a local clock says."""
        _write_klines(tmp_path, "BTCUSDT", self.REAL_OPEN, 1)
        rows = _fetchall(
            tmp_path, 'SELECT "year", "month" FROM "klines"', datasets=("klines",)
        )
        assert rows == [("2026", "07")]


class TestUtcMidnightStraddle:
    """A bucket must never span midnight, and midnight must never split one that shouldn't."""

    @pytest.fixture
    def straddle(self, tmp_path: Path) -> Path:
        # 22:00 on 2024-01-01 through 02:00 on 2024-01-02, continuous across the boundary.
        _write_klines(tmp_path, "BTCUSDT", ANCHOR + 22 * HOUR, 240)
        return tmp_path

    def test_four_hour_buckets_break_at_midnight(self, straddle: Path) -> None:
        """20:00-23:59 and 00:00-03:59 are different bars, whatever the range asked for.

        A naive "bucket from the first row" aggregation would produce a single 22:00-02:00
        bar here, which is not a 4 h bar of any exchange's and would silently disagree
        with every chart it is compared against.
        """
        table = q.derive_timeframe(straddle, "4h", symbol="BTCUSDT")
        opens = table.column("open_time").to_pylist()
        assert opens == [ANCHOR + 20 * HOUR, ANCHOR + DAY]
        assert table.column("close_time").to_pylist() == [o + 4 * HOUR - 1 for o in opens]

    def test_four_hour_bucket_membership(self, straddle: Path) -> None:
        """Each minute lands in the bucket its own UTC clock time says, not its neighbour's."""
        table = q.derive_timeframe(straddle, "4h", symbol="BTCUSDT")
        counts = dict(
            zip(table.column("open_time").to_pylist(), table.column("bar_count").to_pylist())
        )
        assert counts == {
            # 22:00-23:59 -- the back half of the 20:00 bucket, so incomplete.
            ANCHOR + 20 * HOUR: 120,
            # 00:00-01:59 -- the front half of the next day's first bucket.
            ANCHOR + DAY: 120,
        }
        assert table.column("complete").to_pylist() == [False, False]

    def test_daily_buckets_split_at_midnight(self, straddle: Path) -> None:
        table = q.derive_timeframe(straddle, "1d", symbol="BTCUSDT")
        rows = list(
            zip(
                table.column("open_time").to_pylist(),
                table.column("close_time").to_pylist(),
                table.column("bar_count").to_pylist(),
                table.column("complete").to_pylist(),
            )
        )
        assert rows == [
            (ANCHOR, ANCHOR + DAY - 1, 120, False),
            (ANCHOR + DAY, ANCHOR + 2 * DAY - 1, 120, False),
        ]

    def test_last_minute_of_the_day_is_not_in_the_next_day(self, straddle: Path) -> None:
        table = q.derive_timeframe(
            straddle,
            "1d",
            symbol="BTCUSDT",
            start_ms=ANCHOR + DAY - MINUTE,
            end_ms=ANCHOR + DAY,
        )
        assert table.column("open_time").to_pylist() == [ANCHOR]
        assert table.column("bar_count").to_pylist() == [1]


class TestMissingBars:
    """A gap must never produce a plausible higher-timeframe bar without saying so."""

    @pytest.fixture
    def gapped(self, tmp_path: Path) -> Path:
        """Two hours of 1 m bars with minute 30 of the first hour never written."""
        with ParquetBufferedWriter(tmp_path, "klines", KLINES, symbol="BTCUSDT") as w:
            for i in range(120):
                if i == 30:
                    continue
                w.append(_bar(ANCHOR + i * MINUTE, i))
        return tmp_path

    def test_incomplete_bucket_is_flagged(self, gapped: Path) -> None:
        table = q.derive_timeframe(gapped, "1h", symbol="BTCUSDT")
        rows = dict(
            zip(table.column("open_time").to_pylist(), table.column("bar_count").to_pylist())
        )
        assert rows == {ANCHOR: 59, ANCHOR + HOUR: 60}
        assert table.column("complete").to_pylist() == [False, True]

    def test_volume_of_the_gapped_bar_is_understated_but_labelled(
        self, gapped: Path
    ) -> None:
        """The point of `bar_count`.

        The gapped hour's OHLC is entirely plausible and its volume is merely low, so
        nothing about the row itself reveals the missing minute. Only the count does.
        """
        table = q.derive_timeframe(gapped, "1h", symbol="BTCUSDT")
        volumes = table.column("volume").to_pylist()
        assert volumes == [59 * SCALE, 60 * SCALE]

    def test_drop_policy_removes_it(self, gapped: Path) -> None:
        table = q.derive_timeframe(
            gapped, "1h", symbol="BTCUSDT", on_incomplete="drop"
        )
        assert table.column("open_time").to_pylist() == [ANCHOR + HOUR]

    def test_drop_policy_survives_a_range_with_no_complete_bucket(
        self, tmp_path: Path
    ) -> None:
        """Ten minutes of 1 m bars hold no complete daily bucket at all.

        The result is an empty table with the schema intact -- pyarrow infers `null` as
        the type of an empty index list, and `take` had no (string, null) kernel, so
        this crashed instead of returning the nothing it means. The Lab's regime worker
        hits exactly this on a short run over a young lake.
        """
        with ParquetBufferedWriter(tmp_path, "klines", KLINES, symbol="BTCUSDT") as w:
            for i in range(10):
                w.append(_bar(ANCHOR + i * MINUTE, i))
        table = q.derive_timeframe(
            tmp_path, "1d", symbol="BTCUSDT", on_incomplete="drop"
        )
        assert table.num_rows == 0
        assert "open_time" in table.schema.names

    def test_raise_policy_names_the_bucket(self, gapped: Path) -> None:
        with pytest.raises(ValueError, match=r"1 incomplete 1h bucket"):
            q.derive_timeframe(gapped, "1h", symbol="BTCUSDT", on_incomplete="raise")

    def test_raise_policy_reports_the_shortfall(self, gapped: Path) -> None:
        with pytest.raises(ValueError, match=r"59/60 1m bars"):
            q.derive_timeframe(gapped, "1h", symbol="BTCUSDT", on_incomplete="raise")

    def test_duplicate_bars_are_collapsed_not_summed(self, tmp_path: Path) -> None:
        """A partition written twice must not double the derived volume (finding M20).

        The pre-fix behaviour summed both copies -- a duplicated hour's volume read 120
        when 60 was traded -- while the loader's incomplete-bucket warning told the user
        volume was *understated*. Deduplication keeps the derived bar equal to the one a
        clean lake produces, and `duplicate_rows` says loudly that the lake needs a
        compaction; the gap report's `Coverage.duplicate_rows` reports the same fact.
        """
        _write_klines(tmp_path, "BTCUSDT", ANCHOR, 60)
        _write_klines(tmp_path, "BTCUSDT", ANCHOR, 60)
        table = q.derive_timeframe(tmp_path, "1h", symbol="BTCUSDT")
        assert table.column("bar_count").to_pylist() == [60]
        assert table.column("complete").to_pylist() == [True]
        assert table.column("volume").to_pylist() == [60 * SCALE]
        assert table.column("duplicate_rows").to_pylist() == [60]

    def test_dedup_is_deterministic_whatever_the_file_order(self, tmp_path: Path) -> None:
        """Two conflicting copies of one bar must resolve by content, not by ingest order.

        Spec 12.1: the same lake contents give the same answer however the files landed.
        The surviving copy is the lexicographic minimum over the value columns, so the
        assertion below knows which one wins without consulting any file order.
        """
        with ParquetBufferedWriter(tmp_path, "klines", KLINES, symbol="BTCUSDT") as w:
            w.append(_bar(ANCHOR, 0))
        with ParquetBufferedWriter(tmp_path, "klines", KLINES, symbol="BTCUSDT") as w:
            w.append(_bar(ANCHOR, 7))  # same open_time, higher OHLC -- loses the sort

        table = q.derive_timeframe(tmp_path, "1m", symbol="BTCUSDT")
        assert table.num_rows == 1
        assert table.column("open").to_pylist() == [_PX_BASE]  # index 0's open, not 7's
        assert table.column("duplicate_rows").to_pylist() == [1]

    def test_a_clean_lake_reports_zero_duplicates(self, gapped: Path) -> None:
        table = q.derive_timeframe(gapped, "1h", symbol="BTCUSDT")
        assert table.column("duplicate_rows").to_pylist() == [0, 0]

    def test_a_whole_missing_bucket_is_absent_not_zero_filled(self, tmp_path: Path) -> None:
        """Nothing here invents a bar. Spec 4.5 forbids interpolating across a gap.

        An absent bucket is a gap for the gap report to name; a zero-filled one is a
        price that never traded.
        """
        _write_klines(tmp_path, "BTCUSDT", ANCHOR, 60)
        _write_klines(tmp_path, "BTCUSDT", ANCHOR + 2 * HOUR, 60, first_index=120)
        table = q.derive_timeframe(tmp_path, "1h", symbol="BTCUSDT")
        assert table.column("open_time").to_pylist() == [ANCHOR, ANCHOR + 2 * HOUR]

    def test_unknown_policy_is_refused(self, gapped: Path) -> None:
        with pytest.raises(ValueError, match="unknown on_incomplete policy"):
            q.derive_timeframe(gapped, "1h", symbol="BTCUSDT", on_incomplete="fill")


class TestMissingBuckets:
    """Finding C9: `complete` cannot flag a bucket that produced no row at all.

    At `timeframe='1m'` the flag was degenerately true for every stored bar and false for
    none, so a 1 m backtest over a lake missing three hours loaded 44 460 bars instead of
    44 640 and finished silently. These tests pin the two channels that now see it: the
    calendar comparison (`missing_buckets`) and the `raise` policy.
    """

    @pytest.fixture
    def holed(self, tmp_path: Path) -> Path:
        """Three hours of 1 m bars with the entire second hour never written."""
        _write_klines(tmp_path, "BTCUSDT", ANCHOR, 60)
        _write_klines(tmp_path, "BTCUSDT", ANCHOR + 2 * HOUR, 60, first_index=120)
        return tmp_path

    def test_missing_buckets_reports_count_and_ranges(self, holed: Path) -> None:
        report = q.missing_buckets(
            holed,
            "1m",
            symbol="BTCUSDT",
            start_ms=ANCHOR,
            end_ms=ANCHOR + 3 * HOUR,
        )
        assert report.expected == 180
        assert report.present == 120
        assert report.missing == 60
        assert report.ranges == ((ANCHOR + HOUR, ANCHOR + 2 * HOUR),)
        assert "60 of 180 1m bucket(s) absent" in report.describe()

    def test_a_complete_range_reports_nothing_missing(self, holed: Path) -> None:
        report = q.missing_buckets(
            holed, "1m", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + HOUR
        )
        assert report.missing == 0
        assert report.ranges == ()

    def test_leading_and_trailing_absence_count_too(self, holed: Path) -> None:
        """The calendar grid comes from the range, not from the observed data, so a lake
        that simply starts late or ends early cannot shrink the expectation."""
        report = q.missing_buckets(
            holed,
            "1m",
            symbol="BTCUSDT",
            start_ms=ANCHOR - HOUR,
            end_ms=ANCHOR + 4 * HOUR,
        )
        assert report.missing == 180
        assert report.ranges == (
            (ANCHOR - HOUR, ANCHOR),
            (ANCHOR + HOUR, ANCHOR + 2 * HOUR),
            (ANCHOR + 3 * HOUR, ANCHOR + 4 * HOUR),
        )

    def test_coarser_timeframes_use_their_own_grid(self, holed: Path) -> None:
        report = q.missing_buckets(
            holed,
            "1h",
            symbol="BTCUSDT",
            start_ms=ANCHOR,
            end_ms=ANCHOR + 3 * HOUR,
        )
        assert report.expected == 3
        assert report.missing == 1
        assert report.ranges == ((ANCHOR + HOUR, ANCHOR + 2 * HOUR),)

    def test_raise_policy_catches_a_wholly_missing_bucket(self, holed: Path) -> None:
        """Before the fix this returned two plausible hourly bars and no error."""
        with pytest.raises(ValueError, match="wholly absent"):
            q.derive_timeframe(
                holed,
                "1h",
                symbol="BTCUSDT",
                start_ms=ANCHOR,
                end_ms=ANCHOR + 3 * HOUR,
                on_incomplete="raise",
            )

    def test_raise_policy_catches_it_at_1m_where_complete_never_fires(
        self, holed: Path
    ) -> None:
        """The audit's exact degenerate case: expected_bars == 1, `complete` always true."""
        with pytest.raises(ValueError, match="60 1m bucket\\(s\\) wholly absent"):
            q.derive_timeframe(
                holed,
                "1m",
                symbol="BTCUSDT",
                start_ms=ANCHOR,
                end_ms=ANCHOR + 3 * HOUR,
                on_incomplete="raise",
            )

    def test_raise_policy_refuses_an_entirely_empty_range(self, holed: Path) -> None:
        with pytest.raises(ValueError, match="wholly absent"):
            q.derive_timeframe(
                holed,
                "1m",
                symbol="BTCUSDT",
                start_ms=ANCHOR + 10 * DAY,
                end_ms=ANCHOR + 10 * DAY + HOUR,
                on_incomplete="raise",
            )

    def test_raise_policy_still_passes_a_complete_range(self, holed: Path) -> None:
        table = q.derive_timeframe(
            holed,
            "1m",
            symbol="BTCUSDT",
            start_ms=ANCHOR,
            end_ms=ANCHOR + HOUR,
            on_incomplete="raise",
        )
        assert table.num_rows == 60

    def test_without_bounds_interior_holes_are_still_caught(self, holed: Path) -> None:
        """No calendar was stated, so only the span between observed buckets is judged --
        which still contains the hole."""
        with pytest.raises(ValueError, match="wholly absent"):
            q.derive_timeframe(holed, "1h", symbol="BTCUSDT", on_incomplete="raise")

    def test_flag_mode_is_unchanged_and_documentedly_blind(self, holed: Path) -> None:
        """`flag` cannot represent an absent bucket -- the loader must ask the calendar.

        Pinned so nobody "fixes" flag mode by fabricating placeholder rows: a made-up
        bar would be a price that never traded (spec 4.5).
        """
        table = q.derive_timeframe(
            holed, "1h", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + 3 * HOUR
        )
        assert table.column("open_time").to_pylist() == [ANCHOR, ANCHOR + 2 * HOUR]
        assert table.column("complete").to_pylist() == [True, True]

    def test_missing_buckets_refuses_an_empty_range(self, holed: Path) -> None:
        with pytest.raises(ValueError, match="empty range"):
            q.missing_buckets(
                holed, "1m", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR
            )

    def test_missing_buckets_refuses_a_non_kline_dataset(self, holed: Path) -> None:
        with pytest.raises(ValueError, match="not kline-shaped"):
            q.missing_buckets(
                holed,
                "1m",
                symbol="BTCUSDT",
                start_ms=ANCHOR,
                end_ms=ANCHOR + HOUR,
                dataset="aggTrades",
            )


class TestConnections:
    """Fresh per query, because a shared one cascades."""

    def test_each_connection_is_independent(self, lake: Path) -> None:
        a = q.connect(lake, datasets=("klines",))
        b = q.connect(lake, datasets=("klines",))
        try:
            a.execute("CREATE TABLE scratch(x INT)")
            with pytest.raises(Exception):
                b.execute("SELECT * FROM scratch")
        finally:
            a.close()
            b.close()

    def test_a_failed_query_does_not_poison_the_next(self, lake: Path) -> None:
        """The TransactionException cascade this project has already paid for once.

        On a shared connection, one failed statement leaves the implicit transaction
        aborted and every later query fails with a message pointing at the wrong place.
        """
        with pytest.raises(Exception):
            q.query(lake, 'SELECT * FROM "no_such_view"', datasets=("klines",))
        table = q.query(
            lake, 'SELECT count(*) FROM "klines"', datasets=("klines",)
        )
        assert table.column(0)[0].as_py() == 1520

    def test_a_half_built_connection_is_never_returned(self, lake: Path) -> None:
        """One bad dataset name fails the whole `connect`, rather than yielding a
        connection whose views look complete until a query hits the missing one."""
        with pytest.raises(KeyError):
            q.connect(lake, datasets=("klines", "nope"))

    def test_query_returns_a_detached_table(self, lake: Path) -> None:
        """Arrow, not a relation: a relation would die with the connection on the way out."""
        table = q.query(lake, 'SELECT count(*) AS n FROM "klines"', datasets=("klines",))
        assert isinstance(table, pa.Table)
        assert table.column("n")[0].as_py() == 1520

    def test_parameters_are_supported(self, lake: Path) -> None:
        table = q.query(
            lake,
            'SELECT count(*) FROM "klines" WHERE "symbol" = ?',
            datasets=("klines",),
            params=["ETHUSDT"],
        )
        assert table.column(0)[0].as_py() == 60


class TestTiming:
    """The <2 s exit criterion, measured rather than asserted."""

    def test_timing_is_split_at_the_useful_point(self, lake: Path) -> None:
        table, timing = q.timed_query(
            lake, 'SELECT count(*) FROM "klines"', datasets=("klines",)
        )
        assert table.num_rows == 1
        assert timing.rows == 1
        assert timing.connect_s >= 0
        assert timing.execute_s >= 0
        assert timing.total_s == pytest.approx(timing.connect_s + timing.execute_s)

    def test_a_symbol_range_query_is_well_inside_the_budget(self, lake: Path) -> None:
        """Spec 13, Phase 1: any symbol/range queryable in <2 s.

        A 1440-bar fixture cannot prove this for a 100 GB lake -- what it proves is that
        the view layer itself adds no fixed cost worth worrying about, and the pruning
        tests above are what make the claim scale.
        """
        sql = q.timeframe_sql("4h", symbol="BTCUSDT", start_ms=ANCHOR, end_ms=ANCHOR + DAY)
        _, timing = q.timed_query(lake, sql, datasets=("klines",))
        assert timing.within(2.0), timing

    def test_within_is_strict(self) -> None:
        timing = q.QueryTiming(rows=1, connect_s=1.0, execute_s=1.5)
        assert timing.total_s == 2.5
        assert not timing.within(2.0)
        assert timing.within(3.0)


class TestLakeRoot:
    def test_default_layout(self) -> None:
        assert q.market_root("userdata") == Path("userdata") / "market"

    def test_the_lake_root_has_exactly_one_definition(self) -> None:
        """Three modules each defined this, under three names, one level apart.

        `manifest.market_root` and this module's former `lake_root` were the same
        function; `gaps` and `ingest_bulk` reimplemented the convention as prose in their
        docstrings. A module reaching one level too high finds no files and reports an
        empty lake rather than an error, so the definition belongs in exactly one place.
        """
        from perplab.data import manifest, schemas

        assert q.market_root is schemas.market_root
        assert manifest.market_root is schemas.market_root
        assert q.MARKET_SUBDIR == schemas.MARKET_SUBDIR == "market"

    def test_datasets_come_from_the_schema_registry(self) -> None:
        """One list, so a new dataset cannot be registered and left without a view."""
        assert set(q.LAKE_DATASETS) == set(SCHEMAS)


class TestSpillIsAbsoluteAndInsideTheLake:
    """DuckDB's default spill path is relative, which made large queries cwd-dependent.

    `temp_directory` defaults to the literal `.tmp`, resolved against the *process*
    working directory. A gap check over a multi-billion-row `aggTrades` sorts the whole
    range, exceeds memory, spills -- and died with `IOException: Cannot open file
    ".tmp/..."` because the directory was neither present nor creatable where that
    process happened to be running. The API server, the CLI and every worker start from
    different directories, so whether a query could finish depended on who launched it.
    """

    def test_the_spill_directory_is_absolute_and_under_the_lake(
        self, tmp_path: Path
    ) -> None:
        connection = duckdb.connect(":memory:")
        try:
            spill = q.configure_spill(connection, tmp_path)
            configured = connection.execute(
                "SELECT current_setting('temp_directory')"
            ).fetchone()[0]
        finally:
            connection.close()

        assert spill.is_absolute()
        assert spill.is_dir()
        assert spill.parent == tmp_path
        assert spill.name.startswith("_"), "must stay out of every dataset glob"
        assert Path(configured) == spill

    def test_a_query_survives_a_working_directory_it_cannot_write_to(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The regression itself: the same query, run from an unwritable cwd.

        Spilling is not forced here -- the point is that the *configured* path no longer
        resolves against the cwd, so the query's fate no longer depends on it.
        """
        lake = tmp_path / "market"
        unwritable = tmp_path / "elsewhere"
        unwritable.mkdir()
        monkeypatch.chdir(unwritable)

        connection = q.connect(lake, datasets=("klines",), include_unscaled=False)
        try:
            configured = Path(
                connection.execute("SELECT current_setting('temp_directory')").fetchone()[0]
            )
        finally:
            connection.close()

        assert configured.is_absolute()
        assert configured.parent == lake
        assert not (unwritable / ".tmp").exists()
