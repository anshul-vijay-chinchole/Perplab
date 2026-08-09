"""Tests for the bulk-archive registry, header sniffing, and row parsers.

Every fixture below is a line or a value observed on data.binance.vision on 2026-08-01,
not one written to match the code. That rule is not fussiness: two of the spec's stated
assumptions about this exact data turned out to be wrong against the real archives
(findings F1 and F2), and a hand-written fixture would have reproduced the assumption
instead of the reality -- see the docstring at the top of `tests/unit/test_filters.py`.

Where a captured sample is incomplete, the test says so in its own docstring and says
exactly which fields are real. Nothing is quietly invented.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from perplab.data.bulk_layout import (
    BULK_DATASETS,
    bulk_dataset,
    datetime_str_to_ms,
    is_header_line,
    parse_agg_trade_row,
    parse_book_ticker_row,
    parse_funding_row,
    parse_kline_row,
    parse_metrics_row,
)
from perplab.data.schemas import SCHEMAS, partition_components
from perplab.data.writer import ParquetBufferedWriter

# --------------------------------------------------------------------------------------
# Real sample lines, verified 2026-08-01 against BTCUSDT archives.
# --------------------------------------------------------------------------------------

KLINE_ROW_2026_07_15 = (
    "1784073600000,65014.60,65031.80,64996.80,65011.50,327.207,1784073659999,"
    "21273110.50730,5998,222.863,14489221.32700,0"
)
"""First data row of `BTCUSDT-1m-2026-07-15.zip`. `open_time` is 2026-07-15T00:00:00Z."""

KLINE_HEADER = (
    "open_time,open,high,low,close,volume,close_time,quote_volume,count,"
    "taker_buy_volume,taker_buy_quote_volume,ignore"
)
"""First line of a 2023-or-later klines archive, e.g. `BTCUSDT-1m-2023-06-15.zip`."""

KLINE_2022_06_15_PREFIX = "1655251200000,22122.70"
"""Verified opening fields of `BTCUSDT-1m-2022-06-15.zip` -- a pre-header-era archive.

Only these two fields were captured. `KLINE_2022_06_15_LINE` below splices them onto the
tail of the fully-captured 2026 row purely to reach the archive's 12-column arity; the
fields the sniff actually discriminates on are the real ones, and column order and count
are stable across every era checked.
"""

KLINE_2022_06_15_LINE = ",".join(
    [*KLINE_2022_06_15_PREFIX.split(","), *KLINE_ROW_2026_07_15.split(",")[2:]]
)

AGG_TRADE_ROW = "3383271130,65014.6,0.03,7900100566,7900100566,1784073600069,true"
"""Real aggTrades row, 2026-07-15. Pre-2023 archives open with a row of this same shape;
the header/no-header split is by era, but the *shape* of a data row never changed."""

AGG_TRADE_HEADER = (
    "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker"
)
"""First line of a 2023-or-later aggTrades archive."""

BOOK_TICKER_ROW = (
    "2948552298577,25115.90000000,13.09700000,25116.00000000,1.23000000,"
    "1686787200009,1686787200015"
)
"""Real bookTicker row, 2023-06-15 -- inside the 320-day window of finding F1."""

BOOK_TICKER_HEADER = (
    "update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,"
    "transaction_time,event_time"
)

FUNDING_ROW = "1780272000001,8,0.00005703"
"""Real fundingRate row. `funding_interval_hours` is present here and nowhere else."""

FUNDING_HEADER = "calc_time,funding_interval_hours,last_funding_rate"

METRICS_ROW = (
    "2026-07-15 00:00:00,BTCUSDT,105550.9850000000000000,6858675634.7062540000000000,"
    "1.28623339,1.47112400,1.19972635,1.55827200"
)
"""Real metrics row. Note the string timestamp and the 16 decimal places of padding."""

METRICS_HEADER = (
    "create_time,symbol,sum_open_interest,sum_open_interest_value,"
    "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
    "count_long_short_ratio,sum_taker_long_short_vol_ratio"
)

PREMIUM_INDEX_VALUE = "-0.00018479"
"""The one premiumIndexKlines value captured. Signed, which is the whole point of it."""


def _fields(line: str) -> list[str]:
    return line.split(",")


class TestHeaderSniffing:
    """The single biggest trap in this dataset family, so it gets the most tests.

    Both misclassifications are silent. Assuming a header drops the first data row of
    every pre-2023 archive; assuming no header feeds column names to `int()`.
    """

    def test_klines_pre_2023_first_line_is_data(self) -> None:
        """2022-06-15 klines open with `1655251200000,22122.70,...` -- no header."""
        assert is_header_line(KLINE_2022_06_15_LINE, bulk_dataset("klines").column_names) is False

    def test_klines_post_2023_first_line_is_a_header(self) -> None:
        """From ~2023 the same dataset ships column names on line 1."""
        assert is_header_line(KLINE_HEADER, bulk_dataset("klines").column_names) is True

    def test_klines_both_eras_disagree(self) -> None:
        """The two eras of the *same dataset* must sniff differently.

        Stated as one assertion because the failure mode is a "simplification" that
        returns a constant -- which passes either test above on its own.
        """
        klines = bulk_dataset("klines")
        assert klines.is_header(KLINE_HEADER)
        assert not klines.is_header(KLINE_2022_06_15_LINE)
        assert not klines.is_header(KLINE_ROW_2026_07_15)

    def test_agg_trades_both_eras_disagree(self) -> None:
        agg = bulk_dataset("aggTrades")
        assert agg.is_header(AGG_TRADE_HEADER)
        assert not agg.is_header(AGG_TRADE_ROW)

    def test_funding_and_metrics_are_headered_in_every_era(self) -> None:
        """These two carry a header back to 2020-01, unlike klines and aggTrades.

        Their data rows must still sniff as data. `metrics` is the case that rules out
        the naive "does field 0 parse as a number" test: its first data field is the
        string `2026-07-15 00:00:00`, so the numeric test would call this a header.
        """
        assert bulk_dataset("fundingRate").is_header(FUNDING_HEADER)
        assert not bulk_dataset("fundingRate").is_header(FUNDING_ROW)
        assert bulk_dataset("metrics").is_header(METRICS_HEADER)
        assert not bulk_dataset("metrics").is_header(METRICS_ROW)

    def test_book_ticker_header_and_row(self) -> None:
        book = bulk_dataset("bookTicker")
        assert book.is_header(BOOK_TICKER_HEADER)
        assert not book.is_header(BOOK_TICKER_ROW)

    def test_header_sniff_is_case_insensitive(self) -> None:
        assert is_header_line(KLINE_HEADER.upper(), bulk_dataset("klines").column_names)

    def test_wrong_field_count_raises(self) -> None:
        """Arity has been stable across every era; a change means the layout moved."""
        with pytest.raises(ValueError, match="12 CSV fields"):
            is_header_line("1784073600000,65014.60", bulk_dataset("klines").column_names)

    def test_partial_header_match_raises(self) -> None:
        """Half a header is neither shape, and guessing would misalign every column."""
        drifted = KLINE_HEADER.replace("quote_volume", "quoteVolume").replace(
            "taker_buy_volume", "takerBuyVolume"
        )
        with pytest.raises(ValueError, match="neither a header nor a data row"):
            is_header_line(drifted, bulk_dataset("klines").column_names)

    def test_unobserved_columns_refuse_to_sniff(self) -> None:
        """`liquidationSnapshot` has no verified column list, so it cannot be sniffed."""
        with pytest.raises(ValueError, match="never been observed"):
            bulk_dataset("liquidationSnapshot").is_header("anything,at,all")


class TestKlineParser:
    def test_real_row(self) -> None:
        row = parse_kline_row(_fields(KLINE_ROW_2026_07_15))
        assert row == {
            "open_time": 1_784_073_600_000,
            "close_time": 1_784_073_659_999,
            "open": 6_501_460_000_000,
            "high": 6_503_180_000_000,
            "low": 6_499_680_000_000,
            "close": 6_501_150_000_000,
            "volume": 32_720_700_000,
            "quote_volume": 2_127_311_050_730_000,
            "count": 5_998,
            "taker_buy_volume": 22_286_300_000,
            "taker_buy_quote_volume": 1_448_922_132_700_000,
        }

    def test_matches_the_target_schema_exactly(self) -> None:
        """Field set must equal the schema's, or the writer fails at flush time."""
        row = parse_kline_row(_fields(KLINE_ROW_2026_07_15))
        assert set(row) == {f.name for f in SCHEMAS["klines"]}

    def test_ignore_column_is_dropped(self) -> None:
        assert "ignore" not in parse_kline_row(_fields(KLINE_ROW_2026_07_15))

    def test_close_time_is_stored_not_derived(self) -> None:
        """Spec 3.1 requires a distinct close_time, and Binance's is 1 ms short of the
        next open. Deriving it as `open_time + 60_000` would be off by exactly that
        millisecond, which is the width of the window a look-ahead test checks."""
        row = parse_kline_row(_fields(KLINE_ROW_2026_07_15))
        assert row["close_time"] == row["open_time"] + 59_999

    def test_negative_premium_index_value(self) -> None:
        """`premiumIndexKlines` quotes signed premia -- the parser must not clamp.

        Only the value `-0.00018479` was captured, so it is placed in the four price
        columns of a kline-shaped row whose remaining columns are the all-zero volume
        shape those index archives carry. The assertion is about the sign surviving.
        """
        v = PREMIUM_INDEX_VALUE
        line = f"1784073600000,{v},{v},{v},{v},0,1784073659999,0,0,0,0,0"
        row = parse_kline_row(_fields(line), dataset="premiumIndexKlines")
        assert row["open"] == row["high"] == row["low"] == row["close"] == -18_479

    def test_mark_price_zero_volume_is_data(self) -> None:
        """Mark-price bars carry 0 volume by construction; 0 must parse, not raise."""
        line = "1784073600000,65014.60,65031.80,64996.80,65011.50,0,1784073659999,0,0,0,0,0"
        row = parse_kline_row(_fields(line), dataset="markPriceKlines")
        assert row["volume"] == 0
        assert row["quote_volume"] == 0
        assert row["count"] == 0

    def test_header_row_fed_to_the_parser_raises(self) -> None:
        """Belt and braces for a caller who forgets to sniff: `int('open_time')` must
        fail loudly rather than coerce to anything."""
        with pytest.raises(ValueError, match="open_time"):
            parse_kline_row(_fields(KLINE_HEADER))

    def test_short_row_raises(self) -> None:
        with pytest.raises(ValueError, match="expected 12 fields"):
            parse_kline_row(_fields(KLINE_ROW_2026_07_15)[:-1])

    def test_float_is_never_used(self) -> None:
        """`float('0.07')` is not 0.07; the scaled path must be exact.

        0.1 + 0.2 in binary floating point is the canonical demonstration, and a volume
        column summed over a year of bars accumulates exactly that error.
        """
        line = "1784073600000,0.1,0.2,0.07,0.3,0.1,1784073659999,0.2,1,0.1,0.2,0"
        row = parse_kline_row(_fields(line))
        assert row["low"] == 7_000_000
        assert row["open"] + row["high"] == 30_000_000


class TestAggTradeParser:
    def test_real_row(self) -> None:
        assert parse_agg_trade_row(_fields(AGG_TRADE_ROW)) == {
            "ts_ms": 1_784_073_600_069,
            "recv_ms": None,
            "agg_id": 3_383_271_130,
            "price": 6_501_460_000_000,
            "qty": 3_000_000,
            "first_trade_id": 7_900_100_566,
            "last_trade_id": 7_900_100_566,
            "is_buyer_maker": True,
        }

    def test_recv_ms_is_null_not_a_copy_of_ts(self) -> None:
        """Bulk archives were never received by us, so there is no receive clock.

        Copying `ts_ms` in would assert zero transport latency across six years and
        poison any later measurement of the collector's real latency.
        """
        assert parse_agg_trade_row(_fields(AGG_TRADE_ROW))["recv_ms"] is None

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("true", True),
            ("false", False),
            ("TRUE", True),
            ("False", False),
            ("1", True),
            ("0", False),
        ],
    )
    def test_is_buyer_maker_accepted_forms(self, text: str, expected: bool) -> None:
        line = AGG_TRADE_ROW.rsplit(",", 1)[0] + "," + text
        assert parse_agg_trade_row(_fields(line))["is_buyer_maker"] is expected

    @pytest.mark.parametrize("text", ["", "maybe", "2", "t", "yes", "null"])
    def test_unrecognised_is_buyer_maker_raises(self, text: str) -> None:
        """No default, ever. This flag decides which side of the book a trade hit, and
        defaulting it to False would relabel every ambiguous trade as buy-aggressive and
        bias every backtested fill in one direction."""
        line = AGG_TRADE_ROW.rsplit(",", 1)[0] + "," + text
        with pytest.raises(ValueError, match="unrecognised boolean"):
            parse_agg_trade_row(_fields(line))

    def test_matches_the_target_schema_exactly(self) -> None:
        row = parse_agg_trade_row(_fields(AGG_TRADE_ROW))
        assert set(row) == {f.name for f in SCHEMAS["aggTrades"]}


class TestBookTickerParser:
    def test_real_row(self) -> None:
        assert parse_book_ticker_row(_fields(BOOK_TICKER_ROW)) == {
            "ts_ms": 1_686_787_200_009,
            "recv_ms": None,
            "update_id": 2_948_552_298_577,
            "bid_px": 2_511_590_000_000,
            "bid_qty": 1_309_700_000,
            "ask_px": 2_511_600_000_000,
            "ask_qty": 123_000_000,
        }

    def test_ts_is_transaction_time_not_event_time(self) -> None:
        """The collector stores the stream's `T`; bulk must use the same clock or the
        two eras sit 6 ms apart for no reason a query could explain."""
        row = parse_book_ticker_row(_fields(BOOK_TICKER_ROW))
        assert row["ts_ms"] == 1_686_787_200_009  # transaction_time, not ...015

    def test_coverage_caveat_is_carried_in_the_registry(self) -> None:
        """Finding F1 must be reportable from code, not from someone's memory."""
        caveat = bulk_dataset("bookTicker").caveat
        assert caveat is not None
        assert "2024-03-30" in caveat and "F1" in caveat


class TestFundingParser:
    def test_real_row(self) -> None:
        assert parse_funding_row(_fields(FUNDING_ROW)) == {
            "calc_time": 1_780_272_000_001,
            "funding_interval_hours": 8,
            "funding_rate": 5_703,
        }

    def test_interval_hours_is_raw_not_scaled(self) -> None:
        """It is a count of hours. Scaling it would make `8` read as 8e8 to anyone who
        divided by SCALE the way they do for every other column."""
        assert parse_funding_row(_fields(FUNDING_ROW))["funding_interval_hours"] == 8

    def test_negative_rate(self) -> None:
        """Negative funding (shorts pay longs) is routine, not an error."""
        row = parse_funding_row(_fields("1780272000001,8,-0.00005703"))
        assert row["funding_rate"] == -5_703

    def test_is_monthly_only(self) -> None:
        """There is no daily fundingRate path; asking for one must fail here rather
        than as an unexplained 404 from the archive server."""
        funding = bulk_dataset("fundingRate")
        assert funding.cadence == "monthly"
        assert funding.file_stem("BTCUSDT", "2026-07") == "BTCUSDT-fundingRate-2026-07"
        with pytest.raises(ValueError, match="monthly"):
            funding.file_stem("BTCUSDT", "2026-07-15")


class TestMetricsParser:
    def test_real_row(self) -> None:
        assert parse_metrics_row(_fields(METRICS_ROW)) == {
            "create_time": 1_784_073_600_000,
            "sum_open_interest": 10_555_098_500_000,
            "sum_open_interest_value": 685_867_563_470_625_400,
            "count_toptrader_long_short_ratio": 128_623_339,
            "sum_toptrader_long_short_ratio": 147_112_400,
            "count_long_short_ratio": 119_972_635,
            "sum_taker_long_short_vol_ratio": 155_827_200,
        }

    def test_sixteen_decimal_places_of_padding_scale_exactly(self) -> None:
        """`to_scaled` strips trailing zeros before declaring a precision overflow, so
        `6858675634.7062540000000000` is representable. Verified rather than assumed --
        if the stripping ever changed, every metrics ingest would start raising.
        """
        row = parse_metrics_row(_fields(METRICS_ROW))
        assert row["sum_open_interest_value"] == 685_867_563_470_625_400
        assert row["sum_open_interest_value"] < 2**63 - 1

    def test_genuine_precision_overflow_still_raises(self) -> None:
        """Stripping padding must not become "round whatever does not fit"."""
        line = METRICS_ROW.replace("105550.9850000000000000", "105550.9850000000000001")
        with pytest.raises(ValueError, match="sum_open_interest"):
            parse_metrics_row(_fields(line))

    def test_symbol_column_is_dropped_from_the_row(self) -> None:
        row = parse_metrics_row(_fields(METRICS_ROW))
        assert "symbol" not in row
        assert set(row) == {f.name for f in SCHEMAS["metrics"]}

    def test_symbol_column_is_checked_when_asked(self) -> None:
        """The dropped column earns its keep as an integrity check: an archive filed
        under the wrong symbol is only ever catchable at this point."""
        assert parse_metrics_row(_fields(METRICS_ROW), expect_symbol="BTCUSDT")
        with pytest.raises(ValueError, match="ETHUSDT"):
            parse_metrics_row(_fields(METRICS_ROW), expect_symbol="ETHUSDT")

    def test_blank_value_becomes_null_not_zero(self) -> None:
        """Policy: a blank ratio is NULL. Illiquid symbols publish rows where a ratio
        column is empty because there was nothing to compute a ratio from. Zero is a
        different and real observation, and writing it would skew every mean taken over
        the column by however many blanks it swallowed.
        """
        fields = _fields(METRICS_ROW)
        fields[4] = ""  # count_toptrader_long_short_ratio, as seen on thin symbols
        row = parse_metrics_row(fields)
        assert row["count_toptrader_long_short_ratio"] is None
        # The rest of the row is still ingested -- one blank cell must not cost a day.
        assert row["sum_open_interest"] == 10_555_098_500_000
        assert row["create_time"] == 1_784_073_600_000

    def test_blank_timestamp_raises(self) -> None:
        """A row with no time cannot be ordered, partitioned or joined; it is not
        salvageable the way a blank ratio is."""
        fields = _fields(METRICS_ROW)
        fields[0] = ""
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            parse_metrics_row(fields)

    def test_null_survives_a_parquet_round_trip(self, tmp_path: Path) -> None:
        """The policy is only real if Arrow and Parquet preserve it."""
        fields = _fields(METRICS_ROW)
        fields[4] = ""
        with ParquetBufferedWriter(tmp_path, "metrics", SCHEMAS["metrics"], symbol="BTCUSDT") as w:
            w.append(parse_metrics_row(fields))
        (file,) = tmp_path.rglob("*.parquet")
        table = pa.parquet.read_table(file)
        assert table.column("count_toptrader_long_short_ratio").to_pylist() == [None]


class TestDatetimeConversion:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("1970-01-01 00:00:00", 0),
            ("2024-01-01 00:00:00", 1_704_067_200_000),
            ("2023-12-31 23:59:59", 1_704_067_199_000),
            # Leap day, the classic civil-calendar off-by-one.
            ("2024-02-29 00:00:00", 1_709_164_800_000),
            ("2024-03-01 00:00:00", 1_709_251_200_000),
            ("2026-07-15 00:00:00", 1_784_073_600_000),
            ("2026-08-01 00:00:00", 1_785_542_400_000),
        ],
    )
    def test_utc_conversion(self, text: str, expected: int) -> None:
        assert datetime_str_to_ms(text) == expected

    def test_midnight_boundary_is_exact(self) -> None:
        """One millisecond of drift here moves a whole 5-minute row into the previous
        partition, which presents as a gap at the start of the day and a duplicate at
        the end of the one before."""
        assert (
            datetime_str_to_ms("2024-01-01 00:00:00")
            - datetime_str_to_ms("2023-12-31 23:59:59")
            == 1_000
        )

    def test_agrees_with_the_epoch_ms_archives(self) -> None:
        """The metrics row and the klines row below are the same UTC midnight, read out
        of two archives that timestamp in different formats. If the string conversion
        and the exchange's own epoch disagree, a join across the two datasets silently
        misaligns."""
        assert datetime_str_to_ms("2026-07-15 00:00:00") == parse_kline_row(
            _fields(KLINE_ROW_2026_07_15)
        )["open_time"]

    def test_is_timezone_independent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No `datetime`, so no TZ environment variable can reach it."""
        monkeypatch.setenv("TZ", "Pacific/Auckland")
        assert datetime_str_to_ms("2024-01-01 00:00:00") == 1_704_067_200_000

    @pytest.mark.parametrize(
        "text",
        [
            "2026-02-31 00:00:00",  # not a real date
            "2026-13-01 00:00:00",  # month 13
            "2026-07-15 24:00:00",  # hour 24
            "2026-07-15T00:00:00",  # ISO 'T' separator
            "2026-07-15 00:00",  # seconds missing
            "1784073600000",  # epoch ms in the string column
            "",
        ],
    )
    def test_malformed_input_raises(self, text: str) -> None:
        with pytest.raises(ValueError):
            datetime_str_to_ms(text)


class TestPartitionLayout:
    @pytest.mark.parametrize(
        ("dataset", "expected"),
        [
            # Spec 4.3, verbatim.
            ("klines", ("interval=1m", "year=2025", "month=03")),
            ("markPriceKlines", ("year=2025", "month=03")),
            ("funding", ()),
            ("metrics", ("year=2025",)),
            ("aggTrades", ("date=2025-03-14",)),
            ("bookTicker", ("date=2025-03-14",)),
            ("depth20", ("date=2025-03-14",)),
            ("liquidations", ("date=2025-03-14",)),
        ],
    )
    def test_spec_4_3_paths(self, dataset: str, expected: tuple[str, ...]) -> None:
        # 2025-03-14T00:00:00Z, the date used in spec 4.3's own examples.
        assert partition_components(dataset, 1_741_910_400_000) == expected

    def test_collector_datasets_are_unchanged(self) -> None:
        """The on-disk collector data already uses `date=YYYY-MM-DD`. Changing any of
        these would orphan every partition written so far."""
        for dataset in ("depth20", "aggTrades", "bookTicker", "markPrice", "liquidations", "collectorEvents"):
            assert partition_components(dataset, 1_785_542_400_000) == ("date=2026-08-01",)

    def test_month_partition_uses_the_same_calendar_as_the_date(self) -> None:
        """Leap day: `year=2024/month=02` must agree with `date=2024-02-29`."""
        assert partition_components("klines", 1_709_164_800_000) == (
            "interval=1m",
            "year=2024",
            "month=02",
        )

    def test_unregistered_dataset_raises(self) -> None:
        """Defaulting an unknown dataset to daily would write real rows to a path no
        reader looks in, and the loss would present as an empty query, not an error."""
        with pytest.raises(KeyError, match="no partition layout registered"):
            partition_components("klines_5m", 1_741_910_400_000)

    def test_writer_produces_the_spec_path(self, tmp_path: Path) -> None:
        """The layout is only real if the writer actually consults it."""
        row = parse_kline_row(_fields(KLINE_ROW_2026_07_15))
        with ParquetBufferedWriter(tmp_path, "klines", SCHEMAS["klines"], symbol="BTCUSDT") as w:
            w.append(row)

        expected = (
            tmp_path / "klines" / "symbol=BTCUSDT" / "interval=1m" / "year=2026" / "month=07"
        )
        assert list(expected.glob("*.parquet"))

    def test_writer_groups_by_the_datasets_own_time_column(self, tmp_path: Path) -> None:
        """Klines partition on `open_time`. A writer that assumed `ts_ms` would raise;
        one that fell back to the first int64 column would file bars by whatever that
        happened to be."""
        base = parse_kline_row(_fields(KLINE_ROW_2026_07_15))
        # 2026-12-15T00:00:00Z -- same symbol, same interval, five months later.
        december = dict(base, open_time=1_797_292_800_000, close_time=1_797_292_859_999)
        with ParquetBufferedWriter(tmp_path, "klines", SCHEMAS["klines"], symbol="BTCUSDT") as w:
            w.append(base)
            w.append(december)

        months = sorted(
            p.relative_to(tmp_path).as_posix()
            for p in (tmp_path / "klines" / "symbol=BTCUSDT" / "interval=1m").rglob("month=*")
        )
        assert months == [
            "klines/symbol=BTCUSDT/interval=1m/year=2026/month=07",
            "klines/symbol=BTCUSDT/interval=1m/year=2026/month=12",
        ]

    def test_funding_writes_one_partition_per_symbol(self, tmp_path: Path) -> None:
        """Spec 4.3: `funding/symbol=BTCUSDT/data.parquet`, no time component."""
        with ParquetBufferedWriter(tmp_path, "funding", SCHEMAS["funding"], symbol="BTCUSDT") as w:
            w.append(parse_funding_row(_fields(FUNDING_ROW)))
            w.append(parse_funding_row(_fields("1780300800001,8,-0.00001234")))

        symbol_dir = tmp_path / "funding" / "symbol=BTCUSDT"
        assert len(list(symbol_dir.glob("*.parquet"))) == 1
        assert not [p for p in symbol_dir.iterdir() if p.is_dir()]


class TestRegistry:
    def test_every_ingestible_dataset_has_a_registered_schema(self) -> None:
        for name, entry in BULK_DATASETS.items():
            if entry.target_dataset is None:
                continue
            assert entry.target_dataset in SCHEMAS, name
            assert entry.target_schema is SCHEMAS[entry.target_dataset], name
            assert entry.parser is not None, name

    def test_parsers_produce_exactly_their_schemas_fields(self) -> None:
        """Guards the seam the writer depends on: a parser that omits or invents a field
        fails at flush, mid-backfill, after the download has already been paid for."""
        samples = {
            "klines": KLINE_ROW_2026_07_15,
            "markPriceKlines": KLINE_ROW_2026_07_15,
            "aggTrades": AGG_TRADE_ROW,
            "bookTicker": BOOK_TICKER_ROW,
            "fundingRate": FUNDING_ROW,
            "metrics": METRICS_ROW,
        }
        for name, line in samples.items():
            entry = bulk_dataset(name)
            assert entry.parser is not None
            row = entry.parser(_fields(line))
            assert entry.target_schema is not None
            assert set(row) == {f.name for f in entry.target_schema}, name

    def test_registry_columns_match_the_parsed_arity(self) -> None:
        """The declared column list and what the parser indexes must not drift."""
        assert len(bulk_dataset("klines").column_names) == 12
        assert len(bulk_dataset("aggTrades").column_names) == 7
        assert len(bulk_dataset("bookTicker").column_names) == 7
        assert len(bulk_dataset("fundingRate").column_names) == 3
        assert len(bulk_dataset("metrics").column_names) == 8
        assert len(bulk_dataset("bookDepth").column_names) == 4

    def test_urls(self) -> None:
        klines = bulk_dataset("klines")
        assert klines.archive_url("btcusdt", "2026-07-15") == (
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/"
            "BTCUSDT-1m-2026-07-15.zip"
        )
        assert klines.checksum_url("BTCUSDT", "2026-07-15").endswith(".zip.CHECKSUM")
        assert klines.member_name("BTCUSDT", "2026-07-15") == "BTCUSDT-1m-2026-07-15.csv"

        funding = bulk_dataset("fundingRate")
        assert funding.archive_url("BTCUSDT", "2026-07") == (
            "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/"
            "BTCUSDT-fundingRate-2026-07.zip"
        )
        assert bulk_dataset("aggTrades").archive_url("BTCUSDT", "2026-07-15").endswith(
            "daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-07-15.zip"
        )

    def test_liquidation_snapshot_is_present_and_marked_unavailable(self) -> None:
        """Finding F2. It must be *reported* as unavailable by design, not omitted --
        an ingest that silently skips it looks identical to one that forgot it.
        """
        entry = bulk_dataset("liquidationSnapshot")
        assert entry.available is False
        assert entry.parser is None
        assert entry.target_dataset is None
        assert entry.caveat is not None and "F2" in entry.caveat

    def test_only_liquidation_snapshot_is_unavailable(self) -> None:
        unavailable = [n for n, e in BULK_DATASETS.items() if not e.available]
        assert unavailable == ["liquidationSnapshot"]

    def test_unknown_dataset_raises(self) -> None:
        with pytest.raises(KeyError, match="unknown bulk dataset"):
            bulk_dataset("trades")

    def test_the_lake_name_resolves_to_the_archive(self) -> None:
        """`funding` and `fundingRate` are the same dataset under two vocabularies.

        The archives are named for what Binance publishes and the lake for what it stores,
        and both names are right -- one monthly archive is not one lake partition, which is
        why `FileOutcome.dataset` records the source name so a failure traces to a URL.
        What was not right was that the one-word difference made `ingest_range(
        dataset="funding")` a `KeyError` while every other module in the project spells it
        exactly that way.
        """
        assert bulk_dataset("funding") is bulk_dataset("fundingRate")
        assert bulk_dataset("funding").name == "fundingRate"

    def test_every_lake_name_in_the_registry_resolves(self) -> None:
        """Derived from the registry, so a future divergent name cannot be forgotten."""
        for entry in BULK_DATASETS.values():
            if entry.target_dataset is not None:
                assert bulk_dataset(entry.target_dataset) is entry

    def test_the_unknown_dataset_message_lists_both_vocabularies(self) -> None:
        with pytest.raises(KeyError, match="funding.*fundingRate"):
            bulk_dataset("nope")
