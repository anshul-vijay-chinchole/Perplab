"""What data.binance.vision actually publishes, and how to turn a CSV row into a lake row.

This module is deliberately pure: a registry of facts plus a set of functions from
`list[str]` to `dict`. No HTTP, no zipfile, no filesystem. The download layer stays dumb
-- fetch, verify the checksum, unzip, hand lines over -- and every decision that could be
wrong about the *data* lives here, where it is testable against real sample rows with no
network in the loop. Every column set below was verified against BTCUSDT archives on
2026-08-01 rather than read from the spec, because the spec has already been wrong twice
about bulk availability (findings F1 and F2).

**The header trap.** Header rows are conditional and vary by dataset *and by era*. See
`is_header_line`; it is the single most important function in this file and the reason it
is not a one-line constant.

**Scaling.** Every price, quantity, rate and ratio is parsed with
`perplab.core.money.to_scaled` from the exchange's own decimal string. Never `float`.
The archives quote some values with sixteen decimal places of trailing zeros
(`6858675634.7062540000000000`), which `to_scaled` strips before deciding a value has
overflowed 8 decimal places -- so they scale exactly, and a value that genuinely carries
more than 8 significant decimals raises instead of being silently rounded.

**Timestamps.** `metrics` and `bookDepth` timestamp their rows with a UTC string,
`"YYYY-MM-DD HH:MM:SS"`, not epoch milliseconds. `datetime_str_to_ms` converts them with
integer calendar arithmetic; parsing them with `datetime.strptime` would produce a naive
object whose `.timestamp()` silently applies the machine's local zone, shifting an entire
partition by hours on any developer machine that is not on UTC.

**One caveat carried in the registry rather than in someone's memory.** `bookTicker` is
published only for 2023-05-16 .. 2024-03-30 (finding F1) and `liquidationSnapshot` is not
published at all (finding F2). Phase 1 must *report* those, not skip them quietly, so both
appear here with an explicit coverage note rather than being absent.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from perplab.core.money import to_scaled
from perplab.data.schemas import (
    AGG_TRADES,
    BOOK_DEPTH,
    BOOK_TICKER,
    FUNDING,
    KLINES,
    LIQUIDATIONS,
    MARK_PRICE_KLINES,
    METRICS,
    partition_key,
)

__all__ = [
    "BASE_URL",
    "CsvColumn",
    "BulkDataset",
    "BULK_DATASETS",
    "bulk_dataset",
    "is_header_line",
    "datetime_str_to_ms",
    "parse_kline_row",
    "parse_mark_price_kline_row",
    "parse_agg_trade_row",
    "parse_book_ticker_row",
    "parse_funding_row",
    "parse_metrics_row",
    "parse_book_depth_row",
]

BASE_URL = "https://data.binance.vision/data/futures/um"
"""USD-M perpetuals. COIN-M lives under `/data/futures/cm` and is out of scope (spec 1.2);
the two are not interchangeable -- COIN-M quantities are contracts, not coins."""

_MS_PER_DAY = 86_400_000


# --------------------------------------------------------------------------------------
# Header sniffing
# --------------------------------------------------------------------------------------


def is_header_line(line: str, columns: Sequence[str]) -> bool:
    """Decide whether a CSV's first line is a header. Both wrong answers lose data.

    Binance changed its mind about headers around 2023 and did not backfill, so within a
    single dataset the answer differs by file:

    - `klines` and `aggTrades` before ~2023 have **no** header; from ~2023 they do.
    - `fundingRate` and `metrics` have a header in **every** era, back to 2020-01.

    A parser hardcoded to "there is a header" silently drops the first data row of every
    pre-2023 archive -- one bar or one trade per file, thousands of files, no error, and a
    gap detector looking for missing *minutes* will not notice a missing first *row*.
    A parser hardcoded to "there is no header" calls `int("open_time")` and either crashes
    or, if someone later wraps it in a `try`, coerces the header into a junk row that
    sorts to the front of the partition. Neither failure announces itself, which is why
    this is sniffed per file rather than configured per dataset.

    The test is a comparison against the dataset's known column names, not "does field 0
    parse as a number". The numeric test looks obvious and is wrong here: `metrics` and
    `bookDepth` open every data row with a datetime string, so the numeric test calls
    their data rows headers too. It gives the right answer for those datasets today only
    because they happen to be always-headered -- a coincidence, not a reason.

    Raises rather than guessing if the line does not cleanly match either shape: a
    partially-matching header means the archive layout changed under us, and continuing
    would write mismatched columns into the lake (spec 1.4).
    """
    if not columns:
        raise ValueError(
            "cannot sniff a header without a verified column list; this dataset's "
            "columns have never been observed"
        )

    fields = [f.strip().strip('"') for f in line.strip().split(",")]
    if len(fields) != len(columns):
        raise ValueError(
            f"expected {len(columns)} CSV fields, got {len(fields)}: {line[:200]!r}. "
            f"Column count has been stable across every era checked, so a different "
            f"count means the archive layout changed -- refusing to parse it."
        )

    matches = sum(1 for got, want in zip(fields, columns) if got.casefold() == want.casefold())
    if matches == len(columns):
        return True
    if matches:
        raise ValueError(
            f"first line matches {matches} of {len(columns)} expected column names, "
            f"which is neither a header nor a data row: {line[:200]!r}. "
            f"Expected {list(columns)}."
        )
    return False


# --------------------------------------------------------------------------------------
# Field-level conversions
# --------------------------------------------------------------------------------------


def _to_int(text: str, column: str, dataset: str) -> int:
    """Parse a raw integer field, rejecting anything that is not exactly one.

    `int()` already rejects `"1.0"` and `"1e3"`, which is the behaviour we want: a
    timestamp or trade id that has grown a decimal point means the field is not what we
    think it is, and truncating it would be a guess.
    """
    s = text.strip()
    if not s:
        raise ValueError(f"{dataset}.{column}: empty integer field")
    try:
        return int(s)
    except ValueError:
        raise ValueError(f"{dataset}.{column}: expected an integer, got {text!r}") from None


def _to_scaled(text: str, column: str, dataset: str) -> int:
    """Parse a decimal string into a scaled int64, with the column named in any failure.

    `to_scaled` raises informatively about the *value*; ingest failures need to name the
    *column* too, or a run over a year of archives reports "more than 8 decimal places"
    with no clue which of eleven columns produced it.
    """
    try:
        return to_scaled(text)
    except ValueError as exc:
        raise ValueError(f"{dataset}.{column}: {exc}") from None


def _to_optional_scaled(text: str, column: str, dataset: str) -> int | None:
    """As `_to_scaled`, but an empty cell becomes `None` rather than a value.

    This is the documented policy for `metrics` (see `parse_metrics_row`). It applies only
    where the registry marks a column optional; everywhere else an empty cell raises.
    """
    if not text.strip():
        return None
    return _to_scaled(text, column, dataset)


_TRUE = frozenset({"true", "1"})
_FALSE = frozenset({"false", "0"})


def _to_bool(text: str, column: str, dataset: str) -> bool:
    """Parse `is_buyer_maker`. Unrecognised input raises; there is no default.

    The archives write lowercase `true`/`false`, but the REST and WebSocket forms of the
    same field are JSON booleans and at least one historical era of tooling wrote `1`/`0`,
    so all three are accepted. What is *not* accepted is anything else: this flag decides
    which side of the book a trade consumed, and the entire limit-fill model (spec 6.4)
    reads it. Defaulting an unknown value to `False` would relabel every ambiguous trade
    as buy-aggressive and bias every backtested fill in one direction, invisibly.
    """
    s = text.strip().casefold()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    raise ValueError(
        f"{dataset}.{column}: unrecognised boolean {text!r}; refusing to default it, "
        f"the aggressor flag drives the fill model"
    )


def datetime_str_to_ms(text: str) -> int:
    """Convert `"YYYY-MM-DD HH:MM:SS"` UTC to epoch milliseconds, by integer arithmetic.

    `metrics` and `bookDepth` are the only archives that timestamp with a string. The
    obvious `datetime.strptime(...).timestamp()` is wrong twice over: the parsed object is
    naive, so `.timestamp()` interprets it in the machine's local zone, and even
    `.replace(tzinfo=utc)` reintroduces a timezone database into a path that has no need
    of one. A whole partition landing a day early on a developer laptop in Auckland is
    exactly the failure spec 3.1 forbids naive datetimes to prevent.

    The result is round-tripped through `partition_key` before being returned, which
    rejects impossible dates such as `2026-02-31` without needing a calendar table here.
    """
    s = text.strip()
    if (
        len(s) != 19
        or s[4] != "-"
        or s[7] != "-"
        or s[10] != " "
        or s[13] != ":"
        or s[16] != ":"
    ):
        raise ValueError(f"expected 'YYYY-MM-DD HH:MM:SS' UTC, got {text!r}")

    parts = (s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19])
    if not all(p.isdigit() for p in parts):
        raise ValueError(f"expected 'YYYY-MM-DD HH:MM:SS' UTC, got {text!r}")
    year, month, day, hour, minute, second = (int(p) for p in parts)

    if not 1 <= month <= 12 or not 1 <= day <= 31:
        raise ValueError(f"impossible calendar date in {text!r}")
    if hour > 23 or minute > 59 or second > 59:
        raise ValueError(f"impossible clock time in {text!r}")

    # Days-from-civil, Howard Hinnant's algorithm -- the exact inverse of the
    # civil-from-days used by `schemas.partition_key`, so the two directions cannot drift.
    y = year - (1 if month <= 2 else 0)
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (month + (-3 if month > 2 else 9)) + 2) // 5 + day - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    days = era * 146_097 + doe - 719_468

    ms = days * _MS_PER_DAY + (hour * 3600 + minute * 60 + second) * 1000
    if partition_key(ms) != s[:10]:
        raise ValueError(f"impossible calendar date in {text!r}")
    return ms


# --------------------------------------------------------------------------------------
# Row parsers -- pure, one per dataset
# --------------------------------------------------------------------------------------


def _check_arity(fields: Sequence[str], columns: Sequence[str], dataset: str) -> None:
    """Reject a row whose field count is not the verified one.

    A short row is usually a stray newline inside the archive and a long row is usually a
    column that was added upstream. Padding or truncating either would put values in the
    wrong columns, which is worse than a failed ingest because it produces plausible
    numbers.
    """
    if len(fields) != len(columns):
        raise ValueError(
            f"{dataset}: expected {len(columns)} fields {list(columns)}, "
            f"got {len(fields)}: {list(fields)[:14]!r}"
        )


_KLINE_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
)


def parse_kline_row(fields: Sequence[str], *, dataset: str = "klines") -> dict[str, Any]:
    """Parse one kline CSV row into a `KLINES` row.

    Serves every kline-shaped archive: `klines`, `markPriceKlines`, `indexPriceKlines` and
    `premiumIndexKlines` all publish the same twelve columns in the same order. Two
    consequences the implementation must respect:

    - values may be **negative**. `premiumIndexKlines` quotes signed premia such as
      `-0.00018479`, so no min/max clamp and no unsigned assumption belongs here.
    - `markPriceKlines` volume columns are always 0. That is data, not a gap -- there is
      no volume concept for a computed price -- so nothing here treats 0 as missing.

    Binance's twelfth column, `ignore`, is read for arity checking and then discarded; see
    the comment on `schemas._KLINE_FIELDS`.
    """
    _check_arity(fields, _KLINE_COLUMNS, dataset)
    return {
        "open_time": _to_int(fields[0], "open_time", dataset),
        "close_time": _to_int(fields[6], "close_time", dataset),
        "open": _to_scaled(fields[1], "open", dataset),
        "high": _to_scaled(fields[2], "high", dataset),
        "low": _to_scaled(fields[3], "low", dataset),
        "close": _to_scaled(fields[4], "close", dataset),
        "volume": _to_scaled(fields[5], "volume", dataset),
        "quote_volume": _to_scaled(fields[7], "quote_volume", dataset),
        "count": _to_int(fields[8], "count", dataset),
        "taker_buy_volume": _to_scaled(fields[9], "taker_buy_volume", dataset),
        "taker_buy_quote_volume": _to_scaled(
            fields[10], "taker_buy_quote_volume", dataset
        ),
    }


def parse_mark_price_kline_row(fields: Sequence[str]) -> dict[str, Any]:
    """`parse_kline_row` with the dataset name fixed, for error messages."""
    return parse_kline_row(fields, dataset="markPriceKlines")


_AGG_TRADE_COLUMNS = (
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
)


def parse_agg_trade_row(fields: Sequence[str]) -> dict[str, Any]:
    """Parse one aggTrades CSV row into an `AGG_TRADES` row.

    `recv_ms` is `None`, not a copy of `ts_ms`. The collector records it as the local
    clock reading when a message arrived, and the archives were never received by us at
    all. Copying the exchange timestamp into it would assert zero transport latency for
    six years of history, and any later analysis of collector latency would be quietly
    averaged against a wall of fabricated zeros. A null says "not observed", which is the
    truth and which every consumer can test for.
    """
    _check_arity(fields, _AGG_TRADE_COLUMNS, "aggTrades")
    return {
        "ts_ms": _to_int(fields[5], "transact_time", "aggTrades"),
        "recv_ms": None,
        "agg_id": _to_int(fields[0], "agg_trade_id", "aggTrades"),
        "price": _to_scaled(fields[1], "price", "aggTrades"),
        "qty": _to_scaled(fields[2], "quantity", "aggTrades"),
        "first_trade_id": _to_int(fields[3], "first_trade_id", "aggTrades"),
        "last_trade_id": _to_int(fields[4], "last_trade_id", "aggTrades"),
        "is_buyer_maker": _to_bool(fields[6], "is_buyer_maker", "aggTrades"),
    }


_BOOK_TICKER_COLUMNS = (
    "update_id",
    "best_bid_price",
    "best_bid_qty",
    "best_ask_price",
    "best_ask_qty",
    "transaction_time",
    "event_time",
)


def parse_book_ticker_row(fields: Sequence[str]) -> dict[str, Any]:
    """Parse one bookTicker CSV row into a `BOOK_TICKER` row.

    `transaction_time` becomes `ts_ms`, matching the collector, which stores the stream's
    `T` (transaction time) and falls back to `E` only when `T` is absent. Using
    `event_time` here instead would put the bulk era on a different clock from the live
    era by the exchange's own internal publish delay, and spec 6.1 exists to stop exactly
    that kind of backtest/live asymmetry.

    `event_time` is therefore dropped: there is no collector-side counterpart, and adding
    a column that only half the history can populate makes every query special-case the
    join date. `recv_ms` is null for the same reason as in `parse_agg_trade_row`.

    Coverage is 2023-05-16 .. 2024-03-30 only (finding F1). This parser is correct for
    those 320 days and there is nothing else to point it at.
    """
    _check_arity(fields, _BOOK_TICKER_COLUMNS, "bookTicker")
    return {
        "ts_ms": _to_int(fields[5], "transaction_time", "bookTicker"),
        "recv_ms": None,
        "update_id": _to_int(fields[0], "update_id", "bookTicker"),
        "bid_px": _to_scaled(fields[1], "best_bid_price", "bookTicker"),
        "bid_qty": _to_scaled(fields[2], "best_bid_qty", "bookTicker"),
        "ask_px": _to_scaled(fields[3], "best_ask_price", "bookTicker"),
        "ask_qty": _to_scaled(fields[4], "best_ask_qty", "bookTicker"),
    }


_FUNDING_COLUMNS = ("calc_time", "funding_interval_hours", "last_funding_rate")


def parse_funding_row(fields: Sequence[str]) -> dict[str, Any]:
    """Parse one fundingRate CSV row into a `FUNDING` row.

    `funding_interval_hours` is stored as a raw integer count of hours, not scaled. It is
    the reason this archive is authoritative: `exchangeInfo` carries no
    `fundingIntervalHours` for any USD-M symbol (verified 2026-08-01,
    `tests/unit/test_filters.py::test_funding_interval_absent_from_exchange_info`), and
    spec 3.5 rule 3 forbids assuming 8 hours. Gap detection reads it too -- spec 4.5 calls
    a funding gap `> 1.5x fundingIntervalHours`, which is meaningless without the real
    value.

    `last_funding_rate` is signed. Negative funding means shorts pay longs, and it is
    common; nothing here may assume otherwise.
    """
    _check_arity(fields, _FUNDING_COLUMNS, "fundingRate")
    return {
        "calc_time": _to_int(fields[0], "calc_time", "fundingRate"),
        "funding_interval_hours": _to_int(
            fields[1], "funding_interval_hours", "fundingRate"
        ),
        "funding_rate": _to_scaled(fields[2], "last_funding_rate", "fundingRate"),
    }


_METRICS_COLUMNS = (
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)


def parse_metrics_row(
    fields: Sequence[str], *, expect_symbol: str | None = None
) -> dict[str, Any]:
    """Parse one metrics CSV row into a `METRICS` row.

    **Empty-cell policy: a blank value column becomes `NULL`, never 0, and never a
    dropped row.** Illiquid symbols publish rows with some ratio columns empty -- there
    were no top-trader accounts on that side in that five-minute window, so the exchange
    has no ratio to report. The three candidate policies fail differently:

    - writing `0` claims the ratio *was* zero, which is a real and different observation;
      any mean or z-score over the column is then wrong by however many blanks it ate.
    - dropping the row throws away the open-interest values that were present, and turns
      a partially-reported window into a gap that the gap detector will then report.
    - failing the ingest costs a whole day of a symbol over one blank cell in one window.

    A null is the only option that says what actually happened. It survives Parquet, it
    propagates through DuckDB aggregates as a skip rather than a zero, and a strategy that
    cannot tolerate one is forced to handle it explicitly.

    `create_time` is exempt: a row with no timestamp cannot be ordered, partitioned, or
    joined, so a blank there raises. So is `symbol`, which is checked rather than stored
    -- passing `expect_symbol` turns the column the schema drops into an assertion that
    the archive is for the instrument its path claimed.
    """
    _check_arity(fields, _METRICS_COLUMNS, "metrics")

    symbol = fields[1].strip()
    if expect_symbol is not None and symbol.upper() != expect_symbol.upper():
        raise ValueError(
            f"metrics: archive row is for {symbol!r} but was read as {expect_symbol!r}; "
            f"the symbol column is dropped on write, so this mismatch is only ever "
            f"catchable here"
        )

    return {
        "create_time": datetime_str_to_ms(fields[0]),
        "sum_open_interest": _to_optional_scaled(fields[2], "sum_open_interest", "metrics"),
        "sum_open_interest_value": _to_optional_scaled(
            fields[3], "sum_open_interest_value", "metrics"
        ),
        "count_toptrader_long_short_ratio": _to_optional_scaled(
            fields[4], "count_toptrader_long_short_ratio", "metrics"
        ),
        "sum_toptrader_long_short_ratio": _to_optional_scaled(
            fields[5], "sum_toptrader_long_short_ratio", "metrics"
        ),
        "count_long_short_ratio": _to_optional_scaled(
            fields[6], "count_long_short_ratio", "metrics"
        ),
        "sum_taker_long_short_vol_ratio": _to_optional_scaled(
            fields[7], "sum_taker_long_short_vol_ratio", "metrics"
        ),
    }


_BOOK_DEPTH_COLUMNS = ("timestamp", "percentage", "depth", "notional")


def parse_book_depth_row(fields: Sequence[str]) -> dict[str, Any]:
    """Parse one bookDepth CSV row into a `BOOK_DEPTH` row.

    Banded depth, not a ladder: each row is "how much sits within N percent of mid", so it
    is a liquidity *feature* and can never support book walking (spec 4.2 / R1). The
    percentage is signed in principle -- bid-side bands are published as negative offsets
    on some venues -- so it is scaled like any other decimal rather than read as a count.

    Caveat for whoever ingests this first: the column names and order here were verified
    on 2026-08-01, but no sample *row* was captured, so the per-column types are inferred
    from the names. Spot-check one real archive against these conversions before trusting
    a bulk backfill -- inferring types from column names is precisely how this project
    previously encoded an assumption instead of a fact.
    """
    _check_arity(fields, _BOOK_DEPTH_COLUMNS, "bookDepth")
    return {
        "ts_ms": datetime_str_to_ms(fields[0]),
        "percentage": _to_scaled(fields[1], "percentage", "bookDepth"),
        "depth": _to_scaled(fields[2], "depth", "bookDepth"),
        "notional": _to_scaled(fields[3], "notional", "bookDepth"),
    }


# --------------------------------------------------------------------------------------
# The registry
# --------------------------------------------------------------------------------------

_SCALED = "scaled"
_INT = "int"
_BOOL = "bool"
_DATETIME = "datetime"
_SYMBOL = "symbol"
_IGNORED = "ignored"


@dataclass(frozen=True, slots=True)
class CsvColumn:
    """One CSV column: its exact published name and how its text must be read.

    Name and kind live together so they cannot desync. A parallel list of names and a
    parallel list of types is the shape that silently shifts by one when a column is
    inserted, and a shifted type list reads prices as trade ids without complaining.
    """

    name: str
    kind: str
    """`scaled` (decimal string -> scaled int64), `int` (raw count/id/epoch-ms),
    `bool`, `datetime` (UTC "YYYY-MM-DD HH:MM:SS" string), `symbol` (checked, not
    stored), `ignored` (read for arity only)."""


@dataclass(frozen=True, slots=True)
class BulkDataset:
    """Everything needed to locate, sniff and parse one bulk dataset.

    Deliberately holds no client, no session and no path on disk: it describes what the
    exchange publishes. The downloader composes URLs from it, the parser reads rows with
    it, the availability report prints its caveat -- and none of them need to agree on
    anything beyond this object.
    """

    name: str
    """Directory name on data.binance.vision. Also the middle token of the file name for
    every dataset except the kline-shaped ones, which use the interval instead."""
    cadence: str
    """`daily` or `monthly`. Not cosmetic: it decides both the URL path segment and the
    period format, and asking for the wrong one produces a 404 rather than an error that
    explains itself."""
    columns: tuple[CsvColumn, ...]
    target_dataset: str | None
    """Lake dataset name (a key of `schemas.SCHEMAS`), or `None` when nothing is
    ingestible."""
    target_schema: pa.Schema | None
    parser: Callable[..., dict[str, Any]] | None
    interval: str | None = None
    available: bool = True
    caveat: str | None = None
    """A published coverage limitation, carried here so that the ingest path can report
    it rather than a human having to remember it. `None` means no known caveat, not
    "unchecked"."""

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def is_header(self, line: str) -> bool:
        """Per-file header sniff. See `is_header_line` for why this is not a constant."""
        return is_header_line(line, self.column_names)

    def _period_ok(self, period: str) -> bool:
        if self.cadence == "daily":
            return len(period) == 10 and period[4] == "-" and period[7] == "-"
        return len(period) == 7 and period[4] == "-"

    def file_stem(self, symbol: str, period: str) -> str:
        """Archive stem, e.g. `BTCUSDT-1m-2026-07-15` or `BTCUSDT-fundingRate-2026-07`.

        The period format is checked against the cadence rather than trusted. A daily
        period handed to a monthly dataset yields a URL that 404s, and a 404 from an
        archive server is indistinguishable from "that day was never published" -- which
        is precisely the distinction the gap report exists to make.
        """
        if not self._period_ok(period):
            want = "YYYY-MM-DD" if self.cadence == "daily" else "YYYY-MM"
            raise ValueError(
                f"{self.name} is {self.cadence}; expected a {want} period, got {period!r}"
            )
        token = self.interval or self.name
        return f"{symbol.upper()}-{token}-{period}"

    def path_prefix(self, symbol: str) -> str:
        """S3/HTTP key prefix for one symbol, with a trailing slash."""
        parts = [self.cadence, self.name, symbol.upper()]
        if self.interval is not None:
            parts.append(self.interval)
        return "/".join(parts) + "/"

    def archive_url(self, symbol: str, period: str) -> str:
        return f"{BASE_URL}/{self.path_prefix(symbol)}{self.file_stem(symbol, period)}.zip"

    def checksum_url(self, symbol: str, period: str) -> str:
        """Sibling `.zip.CHECKSUM`, holding `"<sha256>  <filename>"` (sha256sum format).

        Spec 4.5 requires verifying it. Silently corrupted archives are a real failure
        mode and a corrupt zip that still inflates yields plausible-looking rows.
        """
        return self.archive_url(symbol, period) + ".CHECKSUM"

    def member_name(self, symbol: str, period: str) -> str:
        """The single CSV inside the archive; it shares the stem."""
        return f"{self.file_stem(symbol, period)}.csv"


BULK_DATASETS: dict[str, BulkDataset] = {
    "klines": BulkDataset(
        name="klines",
        cadence="daily",
        interval="1m",
        columns=(
            CsvColumn("open_time", _INT),
            CsvColumn("open", _SCALED),
            CsvColumn("high", _SCALED),
            CsvColumn("low", _SCALED),
            CsvColumn("close", _SCALED),
            CsvColumn("volume", _SCALED),
            CsvColumn("close_time", _INT),
            CsvColumn("quote_volume", _SCALED),
            CsvColumn("count", _INT),
            CsvColumn("taker_buy_volume", _SCALED),
            CsvColumn("taker_buy_quote_volume", _SCALED),
            CsvColumn("ignore", _IGNORED),
        ),
        target_dataset="klines",
        target_schema=KLINES,
        parser=parse_kline_row,
    ),
    "markPriceKlines": BulkDataset(
        name="markPriceKlines",
        cadence="daily",
        interval="1m",
        columns=(
            CsvColumn("open_time", _INT),
            CsvColumn("open", _SCALED),
            CsvColumn("high", _SCALED),
            CsvColumn("low", _SCALED),
            CsvColumn("close", _SCALED),
            CsvColumn("volume", _SCALED),
            CsvColumn("close_time", _INT),
            CsvColumn("quote_volume", _SCALED),
            CsvColumn("count", _INT),
            CsvColumn("taker_buy_volume", _SCALED),
            CsvColumn("taker_buy_quote_volume", _SCALED),
            CsvColumn("ignore", _IGNORED),
        ),
        target_dataset="markPriceKlines",
        target_schema=MARK_PRICE_KLINES,
        parser=parse_mark_price_kline_row,
        caveat=(
            "volume, quote_volume and both taker columns are always 0 -- a mark price "
            "has no volume concept. Zero here is data, not a gap."
        ),
    ),
    "aggTrades": BulkDataset(
        name="aggTrades",
        cadence="daily",
        columns=(
            CsvColumn("agg_trade_id", _INT),
            CsvColumn("price", _SCALED),
            CsvColumn("quantity", _SCALED),
            CsvColumn("first_trade_id", _INT),
            CsvColumn("last_trade_id", _INT),
            CsvColumn("transact_time", _INT),
            CsvColumn("is_buyer_maker", _BOOL),
        ),
        target_dataset="aggTrades",
        target_schema=AGG_TRADES,
        parser=parse_agg_trade_row,
    ),
    "bookTicker": BulkDataset(
        name="bookTicker",
        cadence="daily",
        columns=(
            CsvColumn("update_id", _INT),
            CsvColumn("best_bid_price", _SCALED),
            CsvColumn("best_bid_qty", _SCALED),
            CsvColumn("best_ask_price", _SCALED),
            CsvColumn("best_ask_qty", _SCALED),
            CsvColumn("transaction_time", _INT),
            CsvColumn("event_time", _INT),
        ),
        target_dataset="bookTicker",
        target_schema=BOOK_TICKER,
        parser=parse_book_ticker_row,
        caveat=(
            "Published only 2023-05-16 .. 2024-03-30, then discontinued -- see "
            "docs/DATA_AVAILABILITY.md finding F1. Spec 4.2 calls this 'full history' "
            "and makes it the primary fill-realism input; it is 320 days out of a "
            "6.5-year instrument history, so TRADE_ONLY is the real default tier. "
            "Also roughly 240 MB zipped per day for BTCUSDT -- size the disk first."
        ),
    ),
    "fundingRate": BulkDataset(
        name="fundingRate",
        cadence="monthly",
        columns=(
            CsvColumn("calc_time", _INT),
            CsvColumn("funding_interval_hours", _INT),
            CsvColumn("last_funding_rate", _SCALED),
        ),
        target_dataset="funding",
        target_schema=FUNDING,
        parser=parse_funding_row,
        caveat=(
            "Monthly only -- there is no daily path. Authoritative source for "
            "fundingIntervalHours (spec 3.5 / R17): exchangeInfo does not carry it."
        ),
    ),
    "metrics": BulkDataset(
        name="metrics",
        cadence="daily",
        columns=(
            CsvColumn("create_time", _DATETIME),
            CsvColumn("symbol", _SYMBOL),
            CsvColumn("sum_open_interest", _SCALED),
            CsvColumn("sum_open_interest_value", _SCALED),
            CsvColumn("count_toptrader_long_short_ratio", _SCALED),
            CsvColumn("sum_toptrader_long_short_ratio", _SCALED),
            CsvColumn("count_long_short_ratio", _SCALED),
            CsvColumn("sum_taker_long_short_vol_ratio", _SCALED),
        ),
        target_dataset="metrics",
        target_schema=METRICS,
        parser=parse_metrics_row,
        caveat=(
            "5-minute cadence from 2020-09-01. create_time is a UTC string, not epoch "
            "ms. Value columns may be blank on illiquid symbols and are written NULL, "
            "never 0 -- see parse_metrics_row."
        ),
    ),
    "bookDepth": BulkDataset(
        name="bookDepth",
        cadence="daily",
        columns=(
            CsvColumn("timestamp", _DATETIME),
            CsvColumn("percentage", _SCALED),
            CsvColumn("depth", _SCALED),
            CsvColumn("notional", _SCALED),
        ),
        target_dataset="bookDepth",
        target_schema=BOOK_DEPTH,
        parser=parse_book_depth_row,
        caveat=(
            "Percentage-banded depth, not a raw ladder (spec 4.2 / R1) -- a feature "
            "input, never a book-walking input. Column names verified 2026-08-01 but no "
            "sample row was captured; spot-check the types on first ingest."
        ),
    ),
    "liquidationSnapshot": BulkDataset(
        name="liquidationSnapshot",
        cadence="daily",
        # Never observed. Inventing a plausible column list would be indistinguishable
        # from a verified one to every later reader, which is the exact habit that
        # produced findings F1 and F2 -- so this stays empty, and `is_header` on it
        # raises rather than pretending.
        columns=(),
        target_dataset=None,
        target_schema=LIQUIDATIONS,
        parser=None,
        available=False,
        caveat=(
            "404 at both the daily and monthly paths, re-verified 2026-08-01 -- see "
            "docs/DATA_AVAILABILITY.md finding F2. Report this as unavailable by "
            "design; do not skip it silently. The live !forceOrder@arr collector stream "
            "is the only source, and it is throttled to one order per symbol per "
            "second, so counts from it are a lower bound."
        ),
    ),
}
"""Every bulk dataset Phase 1 knows about, including the two that cannot be ingested.

The unavailable ones are present on purpose. An ingest run that simply omits
`liquidationSnapshot` looks identical to one that forgot about it, and a coverage report
that never mentions `bookTicker` gives no hint that its history stops in 2024.
"""


_LAKE_ALIASES: dict[str, str] = {
    bulk.target_dataset: name
    for name, bulk in BULK_DATASETS.items()
    if bulk.target_dataset is not None and bulk.target_dataset != name
}
"""Lake dataset name -> published archive name, for the one pair that differs.

Today that is `funding` -> `fundingRate` and nothing else. The project speaks two dataset
vocabularies and both are right: the archives are named by what Binance publishes
(`fundingRate`, monthly files of settlements) and the lake is named by what it stores
(`funding`). They are not the same thing -- one monthly archive is not one lake partition --
which is why `FileOutcome.dataset` records the source name, so a failure traces back to a
URL.

What was not right was that a single-word difference made `ingest_range(dataset="funding")`
a `KeyError` while every other module used exactly that spelling. Resolving both here means
one lookup is total over the vocabulary the rest of the codebase already speaks, rather
than each caller remembering which of two names this particular registry wants. Built from
`BULK_DATASETS` rather than written out, so a dataset whose lake name diverges later cannot
be forgotten.
"""


def bulk_dataset(name: str) -> BulkDataset:
    """Look up a bulk dataset by archive name or lake name, failing loudly on a typo.

    Both spellings resolve to the same object -- see `_LAKE_ALIASES` -- and `.name` on the
    result is always the archive name, so URLs, receipts and reports stay in one vocabulary
    however the caller asked.
    """
    try:
        return BULK_DATASETS[_LAKE_ALIASES.get(name, name)]
    except KeyError:
        known = sorted(set(BULK_DATASETS) | set(_LAKE_ALIASES))
        raise KeyError(
            f"unknown bulk dataset {name!r}; known: {', '.join(known)}"
        ) from None
