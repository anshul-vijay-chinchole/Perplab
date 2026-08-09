"""Arrow schemas for the Parquet lake.

Every numeric market-data column is `int64` holding a value scaled by 10^8
(`perplab.core.money`). No column is ever `float64` or `decimal128`:

- `float64` cannot represent 0.07 exactly, and the error compounds through aggregation.
- `decimal128` round-trips exactly but is slow to scan and awkward in Polars, which is
  precisely the throughput cost the seam exists to avoid.

Timestamps are `int64` epoch milliseconds UTC, not Arrow timestamp types. Arrow
timestamps carry a timezone that different readers interpret differently; an integer
carries no ambiguity to disagree about (spec 3.1).

The module covers two families of dataset that happen to share a lake:

- the **collector** schemas, fed by the live WebSocket streams, which carry a `recv_ms`
  alongside the exchange's own `ts_ms` so that transport latency stays measurable;
- the **bulk** schemas, fed by the daily and monthly archives on data.binance.vision,
  which have no local receive clock at all.

They are kept in one file because a single `SCHEMAS` registry is what makes the writer,
the manifest, and the query layer agree on what a dataset *is*. They differ in how they
partition, which is why `PARTITION_BY_DATE` -- true when only the collector existed -- has
been replaced by the explicit `PARTITION_LAYOUT` mapping below.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

__all__ = [
    "MARKET_SUBDIR",
    "market_root",
    "normalise_symbol",
    "SYMBOLLESS_DATASETS",
    "DEPTH20",
    "AGG_TRADES",
    "BOOK_TICKER",
    "MARK_PRICE",
    "LIQUIDATIONS",
    "COLLECTOR_EVENTS",
    "KLINES",
    "MARK_PRICE_KLINES",
    "FUNDING",
    "METRICS",
    "BOOK_DEPTH",
    "SCHEMAS",
    "PartitionLayout",
    "PARTITION_LAYOUT",
    "layout_for",
    "partition_key",
    "partition_components",
]

_I64 = pa.int64()
_LIST_I64 = pa.list_(pa.int64())


# --------------------------------------------------------------------------------------
# Where the lake lives, and how a symbol is spelled inside it
# --------------------------------------------------------------------------------------

MARKET_SUBDIR = "market"
"""Market data lives at `<userdata>/market/`, reference snapshots at
`<userdata>/reference/` (spec 4.3), and `cli.py` derives both from one `--root`.

Two directory levels, therefore two words, and they are not interchangeable:

- **userdata root** -- the `--root` directory, holding `market/` and `reference/`. Only
  the manifest needs it, because only the manifest records both kinds of input.
- **lake root** (`market_root(userdata)`) -- `<userdata>/market`, the directory the
  writer, the bulk ingester, the gap detector and the query layer are all given.

The distinction is one level of directory and is invisible until it is wrong, at which
point every glob silently matches nothing: a reader handed the userdata root looks for
`<userdata>/klines/...` and finds an empty lake rather than an error. Both names are
defined here, once, so no module can invent its own answer -- there were three before
this was consolidated, under three different names.
"""

SYMBOLLESS_DATASETS = frozenset({"collectorEvents", "macroGlobal", "macroFx"})
"""Datasets written without a `symbol=` path component.

The collector's event stream records the *process* -- heartbeats, reconnects, stale-stream
warnings -- not an instrument. An outage in it affects every symbol at once, so scoping it
to one would understate it.

Declared here beside `PARTITION_LAYOUT` because it is part of the same fact: it says which
level the path starts at, exactly as the layout says what comes below. Three modules
previously each carried their own copy under a different name, which is one edit away from
a query projecting a `symbol` column the path does not contain.
"""


def market_root(userdata: Path | str) -> Path:
    """The Parquet lake root, `<userdata>/market`."""
    return Path(userdata) / MARKET_SUBDIR


def normalise_symbol(symbol: str) -> str:
    """Canonical spelling of a symbol as it appears in a `symbol=` partition value.

    Uppercased, because the writer files `symbol=BTCUSDT` and a lookup for
    `symbol=btcusdt` matches no directory on a case-sensitive filesystem -- and, worse,
    matches on Windows, so the bug only appears in production. Uppercasing at every
    boundary means one spelling reaches the path, whatever the operator typed.

    Refused rather than escaped if it is not a plausible Binance symbol. Binance USD-M
    symbols are uppercase alphanumerics (`BTCUSDT`, `1000PEPEUSDT`); anything carrying a
    separator, a quote or a `=` is a typo or an injection attempt, and both are better
    refused here than quoted downstream. A symbol needing escapes could never have matched
    a partition path anyway, so accepting it could only ever return zero rows and read as
    missing data (spec 1.4).
    """
    normalised = symbol.strip().upper()
    if not normalised or not all(c.isalnum() or c == "_" for c in normalised):
        raise ValueError(
            f"implausible symbol {symbol!r}; expected uppercase alphanumerics such as "
            f"'BTCUSDT'. It would not resolve to the directory the writer used."
        )
    return normalised

DEPTH20 = pa.schema(
    [
        ("ts_ms", _I64),
        ("recv_ms", _I64),
        ("last_update_id", _I64),
        ("bid_px", _LIST_I64),
        ("bid_qty", _LIST_I64),
        ("ask_px", _LIST_I64),
        ("ask_qty", _LIST_I64),
    ]
)

AGG_TRADES = pa.schema(
    [
        ("ts_ms", _I64),
        ("recv_ms", _I64),
        ("agg_id", _I64),
        ("price", _I64),
        ("qty", _I64),
        ("first_trade_id", _I64),
        ("last_trade_id", _I64),
        # The aggressor flag. True => buyer was maker => the trade was sell-aggressive
        # and consumed bid-side queue. The entire limit-fill model (spec 6.4) reads this.
        ("is_buyer_maker", pa.bool_()),
    ]
)

BOOK_TICKER = pa.schema(
    [
        ("ts_ms", _I64),
        ("recv_ms", _I64),
        ("update_id", _I64),
        ("bid_px", _I64),
        ("bid_qty", _I64),
        ("ask_px", _I64),
        ("ask_qty", _I64),
    ]
)

MARK_PRICE = pa.schema(
    [
        ("ts_ms", _I64),
        ("recv_ms", _I64),
        ("mark_price", _I64),
        ("index_price", _I64),
        ("estimated_settle_price", _I64),
        ("last_funding_rate", _I64),
        ("next_funding_ms", _I64),
    ]
)

LIQUIDATIONS = pa.schema(
    [
        ("ts_ms", _I64),
        ("recv_ms", _I64),
        ("side", pa.string()),
        ("order_type", pa.string()),
        ("time_in_force", pa.string()),
        ("qty", _I64),
        ("price", _I64),
        ("avg_price", _I64),
        ("status", pa.string()),
        ("last_filled_qty", _I64),
        ("filled_accum_qty", _I64),
        ("trade_ms", _I64),
    ]
)

COLLECTOR_EVENTS = pa.schema(
    [
        ("ts_ms", _I64),
        ("kind", pa.string()),
        ("stream", pa.string()),
        ("detail", pa.string()),
        ("downtime_ms", _I64),
    ]
)

MACRO_USD_UNIT = 1
"""Macro USD aggregates are stored in **whole dollars**, unscaled.

Not a shortcut -- the 10^8 price scale is unusable here and the alternatives are worse.
Total crypto market cap is ~2.3e12 USD; at 10^8 that is 2.3e20, twenty-five times past
int64, and `to_scaled` refuses it outright (correctly). A private intermediate scale, say
cents, would then have to be excluded from `query.SCALED_COLUMNS` -- whose whole contract
is "divide by 10^8" -- and any reader who forgot would be wrong by a factor of a million.

Whole dollars make the column mean exactly what its name says, need no division in any
view, and give up precision that never existed: CoinGecko publishes this as a JSON float,
so the digits below about the twelfth significant figure are binary noise, not data.
"""

MACRO_GLOBAL = pa.schema(
    [
        # The **source's** own timestamp (CoinGecko `updated_at`), not poll time. Two polls
        # of an unchanged snapshot must collapse to one observation, and only the source's
        # clock can say whether the snapshot changed.
        ("ts_ms", _I64),
        ("recv_ms", _I64),
        # A fraction scaled by 10^8, matching the platform's `_pct` convention
        # (`max_drawdown_pct: 0.15` is fifteen percent), not the 56.43 the API returns.
        ("btc_dominance", _I64),
        ("eth_dominance", _I64),
        # Whole USD, not scaled -- see MACRO_USD_UNIT.
        ("total_market_cap_usd", _I64),
        ("total_volume_usd", _I64),
        ("active_cryptocurrencies", _I64),
        ("markets", _I64),
    ]
)
"""CoinGecko's `/global` snapshot: BTC dominance and total market cap (Phase 11)."""

MACRO_FX = pa.schema(
    [
        ("ts_ms", _I64),
        ("recv_ms", _I64),
        ("series", pa.string()),
        # Scaled by 10^8 like a price: DXY is ~100, so there is no range problem here.
        ("value", _I64),
        # Which provider served this row. Recorded per row rather than assumed, because
        # the DXY provider is replaceable and two providers' quotes for the same instant
        # can differ -- a series silently stitched from two sources is a series nobody
        # can reconcile later.
        ("source", pa.string()),
    ]
)
"""A macro/FX level series, one row per observation (Phase 11). `DXY` today."""

_KLINE_FIELDS = [
    # Spec 3.1: a bar is keyed by its open time but has a *distinct* close time, and a
    # strategy may only see the bar at or after that close time. Both are stored rather
    # than one being derived from the other plus an interval, because the derivation
    # quietly encodes an assumption about the interval that the no-look-ahead guarantee
    # then depends on. Storing what the exchange published removes the assumption.
    ("open_time", _I64),
    ("close_time", _I64),
    ("open", _I64),
    ("high", _I64),
    ("low", _I64),
    ("close", _I64),
    ("volume", _I64),
    ("quote_volume", _I64),
    # A trade count, not money: stored raw, *not* scaled by 10^8. Scaling a count would
    # make it look like every other column and invite a reader to divide by SCALE.
    ("count", _I64),
    ("taker_buy_volume", _I64),
    ("taker_buy_quote_volume", _I64),
    # Binance's twelfth column, `ignore`, is deliberately absent. It is 0 in every row of
    # every era checked, so storing it buys nothing and costs a column in every scan --
    # and a column named `ignore` on disk is an invitation for someone to eventually
    # decide it means something.
]

KLINES = pa.schema(_KLINE_FIELDS)

MARK_PRICE_KLINES = pa.schema(_KLINE_FIELDS)
"""Identical shape to `KLINES`; the archives have identical columns.

`volume`, `quote_volume` and both taker columns are always 0 here -- a mark price is a
computed index, so there is nothing to trade and no volume concept. They are kept rather
than dropped so that one kline parser and one set of query helpers serve both datasets;
a divergent schema would fork every consumer to save four always-zero int64s.
"""

FUNDING = pa.schema(
    [
        ("calc_time", _I64),
        # Hours, raw, not scaled. Present because this archive is the *only* place it can
        # be read: `exchangeInfo` carries no `fundingIntervalHours` for any of the 851
        # USD-M symbols (verified 2026-08-01). Spec 3.5 rule 3 forbids hardcoding 8, and
        # R17 exists because someone did. Gap detection (spec 4.5) needs the per-symbol
        # interval to know what "a missing settlement" even means.
        ("funding_interval_hours", _I64),
        ("funding_rate", _I64),
    ]
)

METRICS = pa.schema(
    [
        # Epoch ms, converted from the archive's "YYYY-MM-DD HH:MM:SS" UTC string at
        # parse time. Nothing downstream should ever see the string form.
        ("create_time", _I64),
        ("sum_open_interest", _I64),
        ("sum_open_interest_value", _I64),
        ("count_toptrader_long_short_ratio", _I64),
        ("sum_toptrader_long_short_ratio", _I64),
        ("count_long_short_ratio", _I64),
        ("sum_taker_long_short_vol_ratio", _I64),
        # The archive's per-row `symbol` column is dropped: it is constant within a file
        # and is already the Hive partition key, so storing it repeats the same string
        # several hundred thousand times per symbol-year to answer a question the path
        # already answers. It is not discarded blindly -- the parser checks it against
        # the symbol the caller asked for, which turns a redundant column into an
        # integrity check that a mislabelled archive would fail.
    ]
)

BOOK_DEPTH = pa.schema(
    [
        ("ts_ms", _I64),
        # Percentage band from mid, not a price level. This dataset is banded depth, not
        # a raw ladder (spec 4.2 / R1), so it is a feature input and can never support
        # book walking however much it looks like a book.
        ("percentage", _I64),
        ("depth", _I64),
        ("notional", _I64),
    ]
)
"""**Column types inferred from column *names*; no sample row has ever been captured.**

The archive's column names were verified against the S3 listing on 2026-08-01, but the
per-column conversions (`bulk_layout.parse_book_depth_row`) rest on what the names
suggest, which is precisely the habit that produced findings F1 and F2. Until a real
archive has been ingested and spot-checked, treat every reader of this schema as
provisional -- `gaps.DATASET_RULES` declines to give it a gap rule for exactly this
reason, and whoever performs the first ingest should verify one file by hand before
trusting a bulk backfill (finding L7).
"""

SCHEMAS: dict[str, pa.Schema] = {
    # Collector datasets.
    "depth20": DEPTH20,
    "aggTrades": AGG_TRADES,
    "bookTicker": BOOK_TICKER,
    "markPrice": MARK_PRICE,
    "liquidations": LIQUIDATIONS,
    "collectorEvents": COLLECTOR_EVENTS,
    # Phase 11 macro sources. Symbolless: they describe the market as a whole and the
    # dollar, not an instrument, and filing them under `symbol=BTCUSDT` would claim a
    # relationship the data does not have.
    "macroGlobal": MACRO_GLOBAL,
    "macroFx": MACRO_FX,
    # Bulk datasets. `aggTrades` and `bookTicker` are shared with the collector: the bulk
    # archives fill the history behind the collector's start, into the same schema, with
    # `recv_ms` left null because there was no local clock to record.
    "klines": KLINES,
    "markPriceKlines": MARK_PRICE_KLINES,
    "funding": FUNDING,
    "metrics": METRICS,
    "bookDepth": BOOK_DEPTH,
}


@dataclass(frozen=True, slots=True)
class PartitionLayout:
    """How one dataset lays out its Hive partitions beneath `symbol=`.

    `time_column` is named explicitly rather than assumed to be `ts_ms`, because the bulk
    schemas keep the exchange's own field names (`open_time`, `calc_time`, `create_time`)
    and renaming them to a house convention would make the on-disk column set disagree
    with the archive it came from -- the first thing anyone checks when a value looks
    wrong.
    """

    time_column: str
    granularity: str
    """One of `date`, `month`, `year`, `none`. `none` means a single partition per symbol,
    which is right only for datasets small enough that a whole symbol's history is one
    reasonable file -- funding, at a few thousand rows per symbol per decade."""
    interval: str | None = None
    """Bar interval, for kline-shaped datasets. Becomes an `interval=1m` path component
    (spec 4.3) so that storing a second interval later cannot collide with the first."""


PARTITION_LAYOUT: dict[str, PartitionLayout] = {
    # Collector datasets: date, every one of them, unchanged. A live collector appends
    # continuously, so daily partitions bound how much data one corrupted or truncated
    # partition can cost. Coarser partitions would put a month at risk per bad flush.
    "depth20": PartitionLayout("ts_ms", "date"),
    "aggTrades": PartitionLayout("ts_ms", "date"),
    "bookTicker": PartitionLayout("ts_ms", "date"),
    "markPrice": PartitionLayout("ts_ms", "date"),
    "liquidations": PartitionLayout("ts_ms", "date"),
    "collectorEvents": PartitionLayout("ts_ms", "date"),
    # Bulk datasets: spec 4.3. These arrive a whole day or month at a time and are
    # re-downloadable from the exchange, so the argument for small blast-radius
    # partitions does not apply; partition count and query pruning do.
    "klines": PartitionLayout("open_time", "month", interval="1m"),
    "markPriceKlines": PartitionLayout("open_time", "month"),
    "funding": PartitionLayout("calc_time", "none"),
    "metrics": PartitionLayout("create_time", "year"),
    # Not in spec 4.3's layout table. Daily, matching how it is published, so a re-ingest
    # of one archive rewrites exactly one partition.
    "bookDepth": PartitionLayout("ts_ms", "date"),
    # Macro (Phase 11): month, not date. An hourly series is 720 rows a month -- daily
    # partitions would produce 30 files of 24 rows each, where the argument for small
    # blast-radius partitions (a continuously-appending high-rate stream) does not apply
    # and the file-count cost does.
    "macroGlobal": PartitionLayout("ts_ms", "month"),
    "macroFx": PartitionLayout("ts_ms", "month"),
}
"""Per-dataset partition layout, consulted by both the collector and bulk writers.

This replaces `PARTITION_BY_DATE`, which asserted that *every* dataset partitions by
date. That was true while the collector was the only producer and became false the moment
bulk klines arrived. A frozenset of names cannot express "year/month, under an interval",
and a set that is silently wrong for half its members is worse than no set at all.

Note that collector `markPrice` (the 1 s stream) and bulk `markPriceKlines` (1 m bars)
are separate datasets with separate layouts, despite spec 4.3 listing a single
`markPrice/`. They carry different columns at different cadences, and merging them would
mean a query could not tell a recorded stream sample from an aggregated bar.
"""


def layout_for(dataset: str) -> PartitionLayout:
    """Look up a dataset's layout, refusing to invent one (spec 1.4).

    An unregistered dataset name is a typo or a dataset someone forgot to declare. Either
    way, defaulting it to daily partitions would write real data to a path no reader
    looks in, and the loss would surface as an empty query result rather than an error.
    """
    try:
        return PARTITION_LAYOUT[dataset]
    except KeyError:
        raise KeyError(
            f"no partition layout registered for dataset {dataset!r}; "
            f"add it to PARTITION_LAYOUT rather than defaulting it. "
            f"Known: {', '.join(sorted(PARTITION_LAYOUT))}"
        ) from None


def _civil_from_days(days: int) -> tuple[int, int, int]:
    """Proleptic Gregorian `(year, month, day)` for a count of days since the epoch.

    Howard Hinnant's civil-from-days, shifted to a 0000-03-01 internal epoch. Integer
    arithmetic throughout, so there is no code path a local timezone could influence.
    """
    z = days + 719_468
    era = (z if z >= 0 else z - 146_096) // 146_097
    doe = z - era * 146_097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146_096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    if m <= 2:
        y += 1
    return y, m, d


def partition_key(ts_ms: int) -> str:
    """UTC date string for a timestamp, e.g. `2026-08-01`.

    Computed by integer arithmetic on the epoch rather than via `datetime`, so there is
    no path by which a local timezone can shift a partition boundary. Off-by-one-day
    partitioning is exactly the kind of bug that stays invisible until a backtest
    straddles midnight.
    """
    y, m, d = _civil_from_days(ts_ms // 86_400_000)
    return f"{y:04d}-{m:02d}-{d:02d}"


def partition_components(dataset: str, ts_ms: int) -> tuple[str, ...]:
    """Hive path components below `symbol=`, for one dataset and one timestamp.

    Returned as a tuple of already-formatted `key=value` segments rather than a joined
    string so that callers build paths with their own separator and nothing has to guess
    whether a leading or trailing slash was included. An empty tuple is a legitimate
    answer -- funding stores one partition per symbol and has no time component at all.

    The same integer calendar as `partition_key` produces the year and month, so a bar
    can never land in a month its date string disagrees with.
    """
    layout = layout_for(dataset)
    prefix: tuple[str, ...] = ()
    if layout.interval is not None:
        prefix = (f"interval={layout.interval}",)

    if layout.granularity == "date":
        return prefix + (f"date={partition_key(ts_ms)}",)
    if layout.granularity == "month":
        y, m, _ = _civil_from_days(ts_ms // 86_400_000)
        return prefix + (f"year={y:04d}", f"month={m:02d}")
    if layout.granularity == "year":
        y, _, _ = _civil_from_days(ts_ms // 86_400_000)
        return prefix + (f"year={y:04d}",)
    if layout.granularity == "none":
        return prefix

    raise ValueError(
        f"dataset {dataset!r} declares unknown partition granularity "
        f"{layout.granularity!r}"
    )
