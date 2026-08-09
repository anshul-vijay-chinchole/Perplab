"""DuckDB views over the Parquet lake, and timeframe derivation on top of them.

This is the read side of Phase 1, whose exit criterion is "any symbol/range queryable in
<2 s" (spec 13). Nothing here copies, caches or rewrites data: every view is a `SELECT`
over `read_parquet` with `hive_partitioning`, so a query that constrains `symbol` and the
time partition never opens the files it does not need. `timed_query` exists so that the
"<2 s" is measured rather than believed, and `pruning_report` so that a query which
misses the budget can be diagnosed as "read the whole lake" rather than guessed at.

**Three decisions this module makes, and why.**

*Views are declared column by column, not `SELECT *`.* DuckDB appends Hive partition
columns to the scan in alphabetical order, which is neither the path order nor the schema
order, and which nothing promises to keep stable. An explicit projection pins the shape of
`SELECT * FROM klines` to (schema fields in schema order, then partition keys in path
order) for the populated and the empty case alike -- and the empty case is not
hypothetical, see below.

*Partition keys are forced to `VARCHAR` via `hive_types`.* Left to autodetect, DuckDB
reads `year=2024` as `BIGINT` and `month=03` as `VARCHAR` (the leading zero defeats the
numeric sniff) in the same view, and `date=2024-01-01` as `DATE`. A predicate that works
against one dataset then fails against another, and the fabricated empty view cannot
possibly guess the same types. Every partition value is a string on disk; keeping it a
string keeps one declaration authoritative. ISO-8601 `date`, zero-padded `year` and
`month` all sort chronologically as strings, which is the entire reason those formats
exist, so range predicates lose nothing.

*A missing or empty dataset directory yields an empty view, not an exception.* `klines`
is ingested from the archives, `depth20` only accumulates forward from the day the
collector starts, and `bookTicker` stops in 2024 (finding F1). A lake where some datasets
have never been written is the normal state, not a broken one, and a query layer that
raises `IOException: No files found` for the whole connection because one dataset is
absent makes the lake unqueryable for the datasets that *are* there. The empty view
carries the full column list and the correct types, so a query returns zero rows rather
than a different error -- and zero rows is the honest answer to "what klines do you have".
This is not a permissive default in the sense spec 1.4 forbids: nothing is guessed and no
value is invented. `SELECT count(*)` returning 0 for a dataset that was never ingested is
the truth, and the gap report is what distinguishes "never ingested" from "ingested and
empty".

**Scaling.** Storage is scaled int64 (`perplab.core.money`) and the primary views keep it
that way. The `_unscaled` views divide by 10^8 into `DOUBLE` for humans and charts. See
`UNSCALED_SUFFIX` for the warning that goes with them; the short version is that a
`DOUBLE` out of this module must never reach the accounting layer.

**Connections.** `connect` returns a *new* in-memory database every call and never touches
`duckdb.default_connection`. A shared connection has already cost this project an
afternoon: one failed statement leaves the implicit transaction aborted, and every
subsequent query on that connection fails with `TransactionException: Current transaction
is aborted` until someone thinks to `ROLLBACK`. The symptom is a cascade of failures that
all point at the wrong query. Views are metadata over files, so rebuilding them per query
costs a directory glob, not a data read -- and `connect(datasets=...)` narrows even that.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from perplab.core.money import SCALE
from perplab.data.schemas import (
    MARKET_SUBDIR,
    SCHEMAS,
    SYMBOLLESS_DATASETS,
    layout_for,
    market_root,
    normalise_symbol,
    partition_key,
)

__all__ = [
    "MARKET_SUBDIR",
    "market_root",
    "LAKE_DATASETS",
    "UNSCALED_SUFFIX",
    "SCALED_COLUMNS",
    "RAW_INT_COLUMNS",
    "TIMEFRAMES",
    "KLINE_SHAPED",
    "timeframe_ms",
    "bucket_start_ms",
    "bucket_close_ms",
    "hive_columns",
    "ViewColumn",
    "view_columns",
    "dataset_source_sql",
    "connect",
    "query",
    "stream_query",
    "STREAM_BATCH_ROWS",
    "timed_query",
    "QueryTiming",
    "pruning_report",
    "ScanPruning",
    "partition_predicate",
    "timeframe_sql",
    "derive_timeframe",
    "MissingBuckets",
    "missing_buckets",
]

# `MARKET_SUBDIR`, `market_root`, `SYMBOLLESS_DATASETS` and `normalise_symbol` are defined
# in `schemas.py` and re-exported here rather than redefined. All four were duplicated
# across this module, `manifest.py` and `gaps.py` under three different names, which is one
# edit away from two modules disagreeing about which directory the lake is in -- a
# disagreement that produces empty results rather than an error.

LAKE_DATASETS: tuple[str, ...] = tuple(SCHEMAS)
"""Every dataset the lake can hold, collector-written and bulk-written alike.

Taken from `SCHEMAS` rather than listed again here. A second list would be a second place
to forget a dataset, and a dataset with no view is invisible to every query -- which looks
exactly like a dataset with no data.
"""

UNSCALED_SUFFIX = "_unscaled"
"""Suffix of the human-readable companion view, e.g. `klines_unscaled`.

**These views emit `DOUBLE` and are for inspection, plotting and eyeballing only. Their
output must never reach the accounting layer.** Spec 3.1 puts money in `Decimal` and
`perplab.core.money` is the single seam where the two representations meet; a `DOUBLE`
that arrives at a balance mutation breaks the exact-equality invariants of spec 3.10,
which explicitly forbid an epsilon, so the failure presents as an unfixable reconciliation
mismatch rather than a clean error. `1234.56` printed on a chart is worth the division;
`1234.56` added to a ledger is not.

The raw int64 views keep the plain dataset name and are the primary interface. Anything
that computes -- backtests, fills, funding, the derived timeframes below -- reads those.
"""

_SCALE_LITERAL = f"{SCALE}.0"

RAW_INT_COLUMNS = frozenset(
    {
        # Epoch milliseconds. Dividing one of these by 10^8 yields a number that looks
        # like a plausible price, which is exactly why the classification is explicit.
        "ts_ms",
        "recv_ms",
        "open_time",
        "close_time",
        "calc_time",
        "create_time",
        "next_funding_ms",
        "trade_ms",
        # Identifiers and sequence numbers.
        "agg_id",
        "update_id",
        "last_update_id",
        "first_trade_id",
        "last_trade_id",
        # Genuine counts and durations.
        "count",
        "funding_interval_hours",
        "downtime_ms",
        # Macro counts (Phase 11).
        "active_cryptocurrencies",
        "markets",
        # Macro USD aggregates, in **whole dollars** rather than scaled by 10^8, because
        # 2.3e12 USD at that scale is twenty-five times past int64 (see
        # `schemas.MACRO_USD_UNIT`). Raw is the truthful classification: there is nothing
        # to divide, and listing them as scaled would understate a trillion-dollar market
        # cap by a factor of 10^8.
        "total_market_cap_usd",
        "total_volume_usd",
    }
)
"""Integer columns that are *not* scaled and must never be divided.

Note `count` (a kline's trade count) sitting beside `count_long_short_ratio` (a scaled
ratio) below. The names differ by a suffix and the treatments differ completely, which is
why this is a list of decisions rather than a heuristic over column names.
"""

SCALED_COLUMNS = frozenset(
    {
        "price",
        "qty",
        "bid_px",
        "bid_qty",
        "ask_px",
        "ask_qty",
        "mark_price",
        "index_price",
        "estimated_settle_price",
        "last_funding_rate",
        "avg_price",
        "last_filled_qty",
        "filled_accum_qty",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_volume",
        "taker_buy_quote_volume",
        "funding_rate",
        "sum_open_interest",
        "sum_open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
        "percentage",
        "depth",
        "notional",
        # Macro (Phase 11). `btc_dominance`/`eth_dominance` are fractions, and `value` is
        # a level series (DXY ~100) -- all small enough for the 10^8 scale.
        "btc_dominance",
        "eth_dominance",
        "value",
    }
)
"""Integer columns holding a value scaled by 10^8, which the `_unscaled` views divide.

Names are global across datasets rather than per dataset, because they already are: a
`price` is a price in every schema that has one. The totality check below is what keeps
this honest -- a column added to any schema and classified in neither set fails at import,
so nobody can add one and find out later that a chart has been off by eight orders of
magnitude.
"""


def _int_like(field_type: pa.DataType) -> bool:
    """True for `int64` and `list<int64>` -- the two shapes a scaled value takes."""
    if pa.types.is_int64(field_type):
        return True
    return pa.types.is_list(field_type) and pa.types.is_int64(field_type.value_type)


def _check_column_classification() -> None:
    """Refuse to import with an unclassified numeric column (spec 1.4).

    The failure this prevents is silent and permanent-looking: a new scaled column that
    nobody declared would be left undivided in the `_unscaled` views, so a chart would
    plot 6.5e12 where it meant 65000 and the obvious conclusion would be "the ingest is
    broken". Raising here costs one line in this file whenever a schema grows, and the
    error names the column.
    """
    both = RAW_INT_COLUMNS & SCALED_COLUMNS
    if both:
        raise RuntimeError(
            f"columns classified as both raw and scaled: {sorted(both)}"
        )

    unclassified: set[str] = set()
    for schema in SCHEMAS.values():
        for field in schema:
            if not _int_like(field.type):
                continue
            if field.name not in RAW_INT_COLUMNS and field.name not in SCALED_COLUMNS:
                unclassified.add(field.name)

    if unclassified:
        raise RuntimeError(
            f"numeric lake columns classified as neither raw nor scaled: "
            f"{sorted(unclassified)}. Add each to RAW_INT_COLUMNS or SCALED_COLUMNS in "
            f"perplab/data/query.py -- the unscaled views cannot guess."
        )


_check_column_classification()


TIMEFRAMES: dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}
"""Derivable timeframes and their length in milliseconds (spec 4.3).

Only 1 m bars are stored; everything coarser is aggregated at query time so that the two
representations cannot drift apart. Every entry divides a day exactly, which is what makes
the buckets land on UTC boundaries: the epoch begins at 1970-01-01T00:00:00Z, so
`ts - ts % tf` is a UTC-aligned boundary for any `tf` that divides 86 400 000. The
constraint is enforced at import rather than trusted.

`1w` is deliberately absent. Epoch day zero was a Thursday, so an epoch-aligned weekly
bucket would open on Thursday; a weekly bar needs an explicit anchor day, and inventing
one here would produce bars that silently disagree with every chart the user compares
them against.
"""

_MS_PER_DAY = 86_400_000

for _name, _ms in TIMEFRAMES.items():
    if _ms <= 0 or _MS_PER_DAY % _ms:
        raise RuntimeError(
            f"timeframe {_name!r} ({_ms} ms) does not divide a UTC day; its buckets "
            f"would drift off midnight and spec 4.3 requires exact UTC boundaries"
        )

KLINE_SHAPED = frozenset({"klines", "markPriceKlines"})
"""Datasets carrying the twelve-column bar shape, and therefore aggregatable by timeframe.

`markPriceKlines` is included on purpose even though its volume columns are always zero:
that is data, not a gap (there is no volume concept for a computed price), and the sums
over it are correctly zero.
"""


def timeframe_ms(timeframe: str) -> int:
    """Length of a timeframe in milliseconds, refusing to parse an unknown one."""
    try:
        return TIMEFRAMES[timeframe]
    except KeyError:
        raise KeyError(
            f"unknown timeframe {timeframe!r}; known: {', '.join(TIMEFRAMES)}"
        ) from None


def bucket_start_ms(ts_ms: int, tf_ms: int) -> int:
    """Open time of the bucket containing `ts_ms`, by integer arithmetic on the epoch.

    Python's `%` floors, so this is correct for negative epochs too. The SQL side has to
    say it the long way round because DuckDB's `%` truncates towards zero; the two are
    tested against each other rather than assumed to agree.
    """
    if tf_ms <= 0:
        raise ValueError(f"timeframe length must be positive, got {tf_ms}")
    return ts_ms - ts_ms % tf_ms


def bucket_close_ms(ts_ms: int, tf_ms: int) -> int:
    """Close time of that bucket: `start + tf - 1` ms, matching Binance exactly.

    The minus one is the whole point. Binance's own 1 m bar opening at 1784073600000
    closes at 1784073659999, not at 1784073660000 -- the close time is the last
    millisecond *inside* the bar, not the first millisecond of the next one. A derived 4 h
    bar that closed at `start + 4h` would be visible to a strategy one millisecond before
    it actually closed, and the no-look-ahead guarantee (spec 6.2) would break at every
    timeframe boundary while remaining perfectly intact everywhere else -- which is the
    hardest kind of bug to see in a backtest curve.
    """
    return bucket_start_ms(ts_ms, tf_ms) + tf_ms - 1


# ---------------------------------------------------------------------------------------
# SQL construction
# ---------------------------------------------------------------------------------------


def _quote_ident(name: str) -> str:
    """Double-quote an identifier. Not optional here.

    Two lake column names collide with DuckDB type keywords -- the `interval` partition
    key and the `date` partition key -- and one collides with a function name (`count`).
    Quoting some identifiers and not others is how one of those gets missed, so everything
    generated by this module is quoted.
    """
    return '"' + name.replace('"', '""') + '"'


def _sql_string(text: str) -> str:
    """Single-quote a string literal, escaping embedded quotes."""
    return "'" + text.replace("'", "''") + "'"


def hive_columns(dataset: str) -> tuple[str, ...]:
    """Partition keys for a dataset, in *path* order.

    Path order, not the alphabetical order DuckDB happens to project them in, because it
    is the order a human reads off the directory tree when checking where a row landed.
    """
    layout = layout_for(dataset)
    columns: list[str] = []
    if dataset not in SYMBOLLESS_DATASETS:
        columns.append("symbol")
    if layout.interval is not None:
        columns.append("interval")
    if layout.granularity == "date":
        columns.append("date")
    elif layout.granularity == "month":
        columns.extend(("year", "month"))
    elif layout.granularity == "year":
        columns.append("year")
    elif layout.granularity != "none":
        raise ValueError(
            f"dataset {dataset!r} declares unknown partition granularity "
            f"{layout.granularity!r}"
        )
    return tuple(columns)


@dataclass(frozen=True, slots=True)
class ViewColumn:
    """One column of a generated view: its name, its SQL, and the type that SQL produces.

    The type travels with the expression so that the empty-lake view can be fabricated
    from the same declaration that builds the populated one. Two separate lists -- one of
    expressions, one of casts -- is the shape that drifts apart the first time a column is
    inserted in the middle.
    """

    name: str
    expression: str
    duck_type: str


def view_columns(dataset: str, *, unscaled: bool = False) -> tuple[ViewColumn, ...]:
    """Columns of a dataset's view: schema fields, then partition keys in path order."""
    try:
        schema = SCHEMAS[dataset]
    except KeyError:
        raise KeyError(
            f"unknown dataset {dataset!r}; known: {', '.join(sorted(SCHEMAS))}"
        ) from None

    columns: list[ViewColumn] = []
    for field in schema:
        ident = _quote_ident(field.name)
        scaled = field.name in SCALED_COLUMNS

        if pa.types.is_int64(field.type):
            if unscaled and scaled:
                expression, duck_type = f"{ident} / {_SCALE_LITERAL}", "DOUBLE"
            else:
                expression, duck_type = ident, "BIGINT"
        elif pa.types.is_list(field.type) and pa.types.is_int64(field.type.value_type):
            if unscaled and scaled:
                expression = f"list_transform({ident}, x -> x / {_SCALE_LITERAL})"
                duck_type = "DOUBLE[]"
            else:
                expression, duck_type = ident, "BIGINT[]"
        elif pa.types.is_boolean(field.type):
            expression, duck_type = ident, "BOOLEAN"
        elif pa.types.is_string(field.type):
            expression, duck_type = ident, "VARCHAR"
        else:
            # Reached only if a schema grows a type this module has never seen. Guessing
            # a DuckDB type for it would produce a view that binds and then misreads the
            # column.
            raise TypeError(
                f"{dataset}.{field.name}: no DuckDB mapping for Arrow type "
                f"{field.type}; add one to perplab.data.query.view_columns"
            )

        columns.append(ViewColumn(field.name, expression, duck_type))

    # Partition keys are VARCHAR by construction; see the module docstring.
    for key in hive_columns(dataset):
        columns.append(ViewColumn(key, _quote_ident(key), "VARCHAR"))

    return tuple(columns)


def _dataset_dir(root: Path | str, dataset: str) -> Path:
    return Path(root) / dataset


def _dataset_glob(root: Path | str, dataset: str) -> str:
    """`<root>/<dataset>/**/*.parquet`, POSIX-separated for DuckDB.

    `**` matches zero or more directory levels in DuckDB, which matters for `funding`:
    it partitions by symbol only, so its files sit one level down while every other
    dataset's sit three or four. One glob covers both.

    The writer's in-flight files are named `.<stem>.parquet.tmp`, which this pattern does
    not match -- a reader can never see a partially written file even mid-flush.
    """
    return (_dataset_dir(root, dataset) / "**" / "*.parquet").as_posix()


def _has_parquet(root: Path | str, dataset: str) -> bool:
    """Whether any published Parquet file exists for a dataset.

    Cheaper than it looks: `rglob` is a generator and this stops at the first hit. It is
    needed because `read_parquet` over a glob that matches nothing raises `IOException`
    rather than returning an empty result, and one never-ingested dataset must not take
    the whole connection down with it.
    """
    directory = _dataset_dir(root, dataset)
    if not directory.is_dir():
        return False
    return next(directory.rglob("*.parquet"), None) is not None


def dataset_source_sql(
    root: Path | str, dataset: str, *, unscaled: bool = False
) -> str:
    """The `SELECT` behind one dataset's view, populated or empty.

    Both branches project the same column names in the same order with the same types, so
    a query written against a lake that has the dataset works unchanged against one that
    does not -- it returns no rows instead of a different error.
    """
    columns = view_columns(dataset, unscaled=unscaled)

    if not _has_parquet(root, dataset):
        projection = ",\n       ".join(
            f"CAST(NULL AS {c.duck_type}) AS {_quote_ident(c.name)}" for c in columns
        )
        return f"SELECT {projection}\nWHERE false"

    projection = ",\n       ".join(
        c.expression
        if c.expression == _quote_ident(c.name)
        else f"{c.expression} AS {_quote_ident(c.name)}"
        for c in columns
    )
    hive_types = ", ".join(f"'{key}': VARCHAR" for key in hive_columns(dataset))
    source = (
        f"read_parquet({_sql_string(_dataset_glob(root, dataset))}, "
        f"hive_partitioning := true"
    )
    if hive_types:
        source += f", hive_types := {{{hive_types}}}"
    source += ")"
    return f"SELECT {projection}\nFROM {source}"


def partition_predicate(
    dataset: str,
    *,
    symbol: str | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> str:
    """A predicate over partition keys only, so DuckDB can prune files before reading any.

    Filtering on the timestamp column alone leaves DuckDB to open every file and consult
    its statistics. Filtering on the partition keys lets it discard files by *path*, which
    is the difference the "<2 s" exit criterion is made of: one month of a symbol is one
    directory out of tens of thousands.

    `end_ms` is exclusive, so the last partition needed is the one holding `end_ms - 1`.
    Half-open ranges are the only convention under which two adjacent queries neither
    double-count nor skip the boundary row.

    One deliberate weakness, in the month-partitioned case spanning more than one year:
    the exact predicate would be a disjunction (`year > lo OR (year = lo AND month >=
    ...)`), and DuckDB only pushes conjunctions down to file filters, so writing it would
    prune nothing while looking like it did. Instead the year bounds alone are emitted --
    a real, if coarser, pruning -- and the timestamp filter trims the remainder from
    Parquet statistics. A single-year range gets the tight `year = ... AND month BETWEEN
    ...` form.
    """
    layout = layout_for(dataset)
    terms: list[str] = []

    if symbol is not None:
        if dataset in SYMBOLLESS_DATASETS:
            raise ValueError(
                f"dataset {dataset!r} is not partitioned by symbol; it records the "
                f"collector process, not an instrument"
            )
        terms.append(f'"symbol" = {_sql_string(normalise_symbol(symbol))}')

    if start_ms is not None and end_ms is not None and end_ms < start_ms:
        raise ValueError(
            f"empty range: end_ms {end_ms} precedes start_ms {start_ms}. Ranges are "
            f"half-open [start, end), so this can only be a caller error"
        )

    # The last instant actually requested. `end_ms` itself belongs to the next partition
    # whenever it lands on a boundary, and including it would read a file for no rows.
    last_ms = None if end_ms is None else end_ms - 1

    if layout.granularity == "date":
        if start_ms is not None:
            terms.append(f'"date" >= {_sql_string(partition_key(start_ms))}')
        if last_ms is not None:
            terms.append(f'"date" <= {_sql_string(partition_key(last_ms))}')
    elif layout.granularity == "year":
        if start_ms is not None:
            terms.append(f'"year" >= {_sql_string(partition_key(start_ms)[:4])}')
        if last_ms is not None:
            terms.append(f'"year" <= {_sql_string(partition_key(last_ms)[:4])}')
    elif layout.granularity == "month":
        lo = None if start_ms is None else partition_key(start_ms)
        hi = None if last_ms is None else partition_key(last_ms)
        if lo is not None and hi is not None and lo[:4] == hi[:4]:
            terms.append(f'"year" = {_sql_string(lo[:4])}')
            terms.append(f'"month" >= {_sql_string(lo[5:7])}')
            terms.append(f'"month" <= {_sql_string(hi[5:7])}')
        else:
            if lo is not None:
                terms.append(f'"year" >= {_sql_string(lo[:4])}')
            if hi is not None:
                terms.append(f'"year" <= {_sql_string(hi[:4])}')
    # granularity "none" (funding) has no time partition to constrain.

    return " AND ".join(terms) if terms else "TRUE"


# ---------------------------------------------------------------------------------------
# Connections and execution
# ---------------------------------------------------------------------------------------


def _resolve_datasets(datasets: Iterable[str] | None) -> tuple[str, ...]:
    if datasets is None:
        return LAKE_DATASETS
    resolved = tuple(datasets)
    unknown = [d for d in resolved if d not in SCHEMAS]
    if unknown:
        raise KeyError(
            f"unknown dataset(s) {unknown}; known: {', '.join(sorted(SCHEMAS))}"
        )
    return resolved


SPILL_SUBDIR = "_duckdb_tmp"
"""Where DuckDB spills a query too large for memory. Absolute, and inside the lake.

DuckDB's default `temp_directory` is the literal relative path `.tmp`, resolved against
the *process* working directory. That default is why a gap check over a 3.4-billion-row
`aggTrades` -- whose `lag(ts_ms) OVER (ORDER BY ts_ms)` sorts the whole range -- died with
`IOException: Cannot open file ".tmp/duckdb_temp_storage-*.tmp"` rather than answering: the
API server, the CLI and every worker run from different directories, and in at least one of
them `.tmp` is neither present nor creatable. A relative spill path makes the success of a
query depend on where the process happened to be started, which is not a property anyone
reasons about when reading a query.

The leading underscore keeps it out of `<root>/*/symbol=*` and every `**/*.parquet` glob,
so a spill file can never be mistaken for data -- the same convention `_ingest` uses.

**It shares a volume with the lake the collector is writing to**, which is the reason
`max_temp_directory_size` is set alongside it: an unbounded spill during a full-history
scan would fill the disk, and the process that loses is the collector, whose depth data
cannot be re-downloaded. Bounding the spill turns that into a failed query, which is
recoverable, instead of a permanent hole in the recording.
"""

MAX_SPILL_BYTES = 32 * 1024**3
"""Ceiling on spill. Large enough for a full-history sort, small enough to leave room."""


def configure_spill(
    connection: duckdb.DuckDBPyConnection, root: Path | str
) -> Path:
    """Point a connection's spill at an absolute directory inside the lake.

    Called for every connection this package opens. See `SPILL_SUBDIR` for why the default
    is unusable and why the size is capped.
    """
    spill = Path(root) / SPILL_SUBDIR
    spill.mkdir(parents=True, exist_ok=True)
    connection.execute(f"SET temp_directory = '{spill.as_posix()}'")
    connection.execute(f"SET max_temp_directory_size = '{MAX_SPILL_BYTES}B'")
    return spill


def connect(
    root: Path | str,
    *,
    datasets: Iterable[str] | None = None,
    include_unscaled: bool = True,
) -> duckdb.DuckDBPyConnection:
    """A fresh in-memory DuckDB with one view per dataset. The caller closes it.

    Fresh every time, and never `duckdb.default_connection`. DuckDB runs each statement in
    an implicit transaction; when one fails, that transaction is left aborted and every
    later statement on the same connection raises `TransactionException: Current
    transaction is aborted` until something issues a `ROLLBACK`. On a shared connection
    that turns one bad query into a cascade of failures whose messages all point away from
    the actual cause. A connection per query cannot cascade, and the cost is a filesystem
    glob rather than a data read.

    `datasets` narrows which views are built. `CREATE VIEW` binds its `SELECT`, so
    declaring a view lists that dataset's files; on a lake with tens of thousands of files
    across eleven datasets, restricting to the two a query touches is the difference
    between a fast connection and a slow one. `timed_query` reports connect and execute
    time separately so this is visible rather than suspected.

    `include_unscaled` builds the `DOUBLE`-typed companion views. See `UNSCALED_SUFFIX`:
    inspection and plotting only.
    """
    connection = duckdb.connect(":memory:")
    try:
        configure_spill(connection, root)
        for dataset in _resolve_datasets(datasets):
            connection.execute(
                f"CREATE VIEW {_quote_ident(dataset)} AS "
                f"{dataset_source_sql(root, dataset)}"
            )
            if include_unscaled:
                connection.execute(
                    f"CREATE VIEW {_quote_ident(dataset + UNSCALED_SUFFIX)} AS "
                    f"{dataset_source_sql(root, dataset, unscaled=True)}"
                )
    except Exception:
        # A half-built connection is worse than none: its views look complete until a
        # query hits the one that was never created.
        connection.close()
        raise
    return connection


def _fetch_arrow(cursor: duckdb.DuckDBPyConnection) -> pa.Table:
    """Materialise a result as Arrow, across the rename in the DuckDB Python API.

    `fetch_arrow_table` was renamed `to_arrow_table` and the old name now emits a
    `DeprecationWarning`. `pyproject.toml` turns DeprecationWarnings into errors on
    purpose -- it is what keeps the "no `decimal` outside the seam" guard from being
    quietly skippable -- so the deprecated spelling is not merely untidy here, it fails
    the suite. `duckdb>=1.0` is the declared floor and the old name still works there,
    hence the fallback rather than a hard bump.
    """
    fetch = getattr(cursor, "to_arrow_table", None)
    if fetch is None:
        fetch = cursor.fetch_arrow_table
    return fetch()


def _arrow_reader(cursor: duckdb.DuckDBPyConnection, batch_rows: int) -> Any:
    """A streaming Arrow reader, across the rename in the DuckDB Python API.

    The same situation as `_fetch_arrow`: `fetch_record_batch` was renamed
    `to_arrow_reader` and the old name now emits a `DeprecationWarning`. `pyproject.toml`
    turns those into errors -- which is what keeps the "no `decimal` outside the seam" guard
    from being quietly skippable -- and this one is raised from *inside* a C call, so it
    surfaces as `SystemError: <built-in function __import__> returned a result with an
    exception set` rather than as anything resembling a deprecation. Preferring the new name
    is not tidiness; the old one fails the suite in a way that takes twenty minutes to
    recognise.
    """
    fetch = getattr(cursor, "to_arrow_reader", None)
    if fetch is None:
        fetch = cursor.fetch_record_batch
    return fetch(batch_rows)


def query(
    root: Path | str,
    sql: str,
    *,
    datasets: Iterable[str] | None = None,
    params: Sequence[Any] | None = None,
) -> pa.Table:
    """Run one query on its own connection and return a detached Arrow table.

    Arrow rather than a DuckDB relation because the relation would be invalidated the
    moment this function closes the connection, and returning something that dies on the
    way out of the function is a trap. Arrow also keeps the int64 columns int64, so the
    scaled representation survives the trip out of the query layer intact.
    """
    connection = connect(root, datasets=datasets)
    try:
        cursor = connection.execute(sql, params) if params is not None else connection.execute(sql)
        return _fetch_arrow(cursor)
    finally:
        connection.close()


STREAM_BATCH_ROWS = 65_536
"""Rows per batch in `stream_query`.

Large enough that the per-batch Python overhead disappears against the row loop that
consumes it, small enough that one batch of the widest tick schema is a few megabytes
rather than a working set. It is not a correctness parameter -- any value produces the same
rows in the same order -- so it is a module constant rather than a run input.
"""


def stream_query(
    root: Path | str,
    sql: str,
    *,
    datasets: Iterable[str] | None = None,
    params: Sequence[Any] | None = None,
    batch_rows: int = STREAM_BATCH_ROWS,
) -> Iterator[pa.RecordBatch]:
    """Run one query and yield its result in batches, never materialising the whole thing.

    `query` returns an Arrow table, which is the right shape for a kline range -- a year of
    1-minute bars is half a million rows and a few megabytes. It is the wrong shape for the
    tick datasets: a single day of BTCUSDT `bookTicker` is tens of millions of rows, and a
    year is measured in tens of gigabytes. A backtest reads those strictly in timestamp
    order and never looks back, so they can be consumed a batch at a time and discarded.

    **The connection outlives this call and is closed by the generator.** That is unusual
    enough to be worth stating: the `RecordBatchReader` is a cursor into DuckDB, so closing
    the connection eagerly -- as `query` does -- would invalidate it. The `try/finally`
    around the yield loop closes it on exhaustion, on `GeneratorExit` (the caller stopped
    early, which the engine does whenever a run aborts), and on any exception raised
    downstream. Callers that abandon a stream without closing it leak a connection until
    the generator is collected, which is why the engine's feeds are always driven to
    exhaustion or closed explicitly.
    """
    connection = connect(root, datasets=datasets)
    try:
        cursor = (
            connection.execute(sql, params) if params is not None else connection.execute(sql)
        )
        for batch in _arrow_reader(cursor, batch_rows):
            yield batch
    finally:
        connection.close()


@dataclass(frozen=True, slots=True)
class QueryTiming:
    """Where the wall clock went, split at the point that actually matters.

    Phase 1's exit criterion is "any symbol/range queryable in <2 s" (spec 13), and a
    breach is one of two entirely different problems: view construction globbing a lake
    with too many small files, or the scan itself reading partitions it should have
    pruned. One total would leave a reader guessing; these two numbers say which, and
    `pruning_report` confirms the second.
    """

    rows: int
    connect_s: float
    execute_s: float

    @property
    def total_s(self) -> float:
        return self.connect_s + self.execute_s

    def within(self, budget_s: float) -> bool:
        return self.total_s < budget_s


def timed_query(
    root: Path | str,
    sql: str,
    *,
    datasets: Iterable[str] | None = None,
    params: Sequence[Any] | None = None,
) -> tuple[pa.Table, QueryTiming]:
    """`query`, plus a measurement of how long it took.

    `perf_counter` rather than `time.time`: a wall clock that a time sync can step
    backwards is not a stopwatch, and a negative elapsed time reported against a 2 s
    budget would be read as a pass.
    """
    t0 = time.perf_counter()
    connection = connect(root, datasets=datasets)
    t1 = time.perf_counter()
    try:
        cursor = connection.execute(sql, params) if params is not None else connection.execute(sql)
        table = _fetch_arrow(cursor)
        t2 = time.perf_counter()
    finally:
        connection.close()
    return table, QueryTiming(
        rows=table.num_rows, connect_s=t1 - t0, execute_s=t2 - t1
    )


@dataclass(frozen=True, slots=True)
class ScanPruning:
    """What one Parquet scan in a query plan will actually open.

    `files_scanned is None` means DuckDB reported no file filter for that scan -- every
    file will be read. That is a legitimate plan for an unfiltered aggregate and a bug for
    a symbol/range query, and only the caller knows which.
    """

    file_filters: str | None
    files_scanned: int | None
    files_total: int | None

    @property
    def prunes(self) -> bool:
        return (
            self.files_scanned is not None
            and self.files_total is not None
            and self.files_scanned < self.files_total
        )


def pruning_report(
    root: Path | str,
    sql: str,
    *,
    datasets: Iterable[str] | None = None,
) -> tuple[ScanPruning, ...]:
    """Read the query plan and report, per Parquet scan, how many files survive pruning.

    `EXPLAIN` does not execute the query, so this is cheap enough to run against a
    production-sized lake when a query misses its budget. It answers the one question that
    matters there -- "is this reading the whole lake?" -- with a number rather than an
    opinion.

    Parsed from `EXPLAIN (FORMAT json)` rather than the box-drawing text plan, which is a
    rendering and changes freely between releases. The JSON keys can move too, hence
    `None` rather than a fabricated zero when they are absent: a report that invents
    "0 files scanned" would read as perfect pruning.
    """
    connection = connect(root, datasets=datasets)
    try:
        row = connection.execute(f"EXPLAIN (FORMAT json) {sql}").fetchone()
    finally:
        connection.close()

    if row is None:
        raise RuntimeError("EXPLAIN returned no plan")

    scans: list[ScanPruning] = []

    def walk(nodes: list[dict[str, Any]]) -> None:
        for node in nodes:
            info = node.get("extra_info") or {}
            if node.get("name") in ("READ_PARQUET", "TABLE_SCAN") and (
                info.get("Function") == "READ_PARQUET"
            ):
                scanned = total = None
                raw = info.get("Scanning Files")
                if isinstance(raw, str) and "/" in raw:
                    got, _, want = raw.partition("/")
                    scanned, total = int(got), int(want)
                scans.append(
                    ScanPruning(
                        file_filters=info.get("File Filters"),
                        files_scanned=scanned,
                        files_total=total,
                    )
                )
            walk(node.get("children") or [])

    walk(json.loads(row[1]))
    return tuple(scans)


# ---------------------------------------------------------------------------------------
# Timeframe derivation (spec 4.3)
# ---------------------------------------------------------------------------------------


def timeframe_sql(
    timeframe: str,
    *,
    symbol: str | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
    dataset: str = "klines",
    view: str | None = None,
) -> str:
    """SQL aggregating stored 1 m bars into `timeframe` bars, on exact UTC boundaries.

    Spec 4.3 stores 1 m klines only and derives everything coarser here, so the two can
    never disagree. Its one rule is that the aggregation respect UTC boundaries and
    produce exact close times, because the no-look-ahead guarantee (spec 6.2) is stated in
    terms of a bar's close.

    **Boundaries.** `open_time - ((open_time % tf) + tf) % tf` is floor division written
    out. The doubled modulo is not superstition: DuckDB's `%` truncates towards zero, so
    the obvious `open_time - open_time % tf` rounds the wrong way for a negative epoch and
    would put such a bar in the following bucket. All lake data is post-2019 and positive,
    but a bucket function that is only correct for the inputs someone happened to test is
    a landmine, and the correct form costs one extra modulo.

    **Close times are computed, never taken from the data.** `bucket + tf - 1`, matching
    Binance (a 1 m bar opening at 1784073600000 closes at 1784073659999). The tempting
    alternative, `max(close_time)` over the constituent bars, is wrong precisely when it
    matters: if the final 1 m bar of an hour is missing, `max(close_time)` reports the
    hour as closing at :58:59.999, and a strategy gated on that close would act on the
    hourly bar a minute before the hour was over. That is look-ahead manufactured by the
    query layer.

    **Missing bars are counted, not hidden.** `bar_count` is how many distinct 1 m bars
    the bucket actually contained and `complete` is whether that equals `tf / 60000`. A
    4 h bar built from 239 minutes is not obviously wrong -- its OHLC is plausible and its
    volume is merely low -- so nothing but the count reveals it.

    **Duplicate bars are collapsed to one row, deterministically, and counted.** A
    re-ingest that wrote a partition twice used to flow both copies into the sums, so a
    duplicated bar's volume was *doubled* -- while the loader's warning, written for the
    missing-bar case, told the user volume was understated. Both statements about the same
    row, both wrong. Now exactly one copy of each `(symbol, open_time)` survives, chosen
    by ordering on every value column rather than on physical file order, so the same lake
    contents produce the same bars whatever order the files were written in (the spec 12.1
    determinism rule; ties between byte-identical copies cannot differ). The collapsed
    copies are not hidden: `duplicate_rows` carries how many extra rows each bucket
    contained, and the gap report's `Coverage.duplicate_rows` reports the same condition
    at 1 m resolution -- a partition written twice is a lake defect worth fixing even
    though the derived bars are now correct despite it.

    **A bucket with *no* constituent bars produces no row at all.** `GROUP BY` cannot
    aggregate rows that do not exist, so nothing in this result -- neither `complete` nor
    the row's absence being flagged anywhere -- reveals a wholly-missing bucket, and at
    `timeframe='1m'` (`expected_bars == 1`) `complete` is degenerately true for every row
    that exists. Use `missing_buckets` to compare the result against the calendar grid;
    `derive_timeframe(on_incomplete="raise")` does so automatically.

    **The aggregate reads the raw scaled int64 view, and its output is scaled int64.**
    Summing `DOUBLE` volumes would reintroduce exactly the representation error the
    scaled-integer seam exists to prevent. Unscale afterwards if a human is going to look
    at it. The sums are cast back to `BIGINT` explicitly because DuckDB widens
    `sum(BIGINT)` to `HUGEINT`, which arrives in Arrow as `decimal128(38, 0)` -- the exact
    type `schemas.py` rules out of the lake, and it would arrive through the back door of
    a query result. The cast raises on overflow rather than truncating, which is the right
    failure: a volume sum that will not fit in int64 cannot be stored either.

    `end_ms` is exclusive. A range whose bounds do not land on bucket boundaries yields
    partial first and last buckets, correctly flagged `complete = false` rather than
    silently widened -- quietly extending a caller's range is how a backtest ends up
    reading data from outside the window it declared.
    """
    if dataset not in KLINE_SHAPED:
        raise ValueError(
            f"dataset {dataset!r} is not kline-shaped; timeframe aggregation sums OHLCV "
            f"columns and only {sorted(KLINE_SHAPED)} have them"
        )

    tf_ms = timeframe_ms(timeframe)
    expected_bars = tf_ms // TIMEFRAMES["1m"]
    source = _quote_ident(view if view is not None else dataset)

    where = [partition_predicate(dataset, symbol=symbol, start_ms=start_ms, end_ms=end_ms)]
    if start_ms is not None:
        where.append(f'"open_time" >= {int(start_ms)}')
    if end_ms is not None:
        where.append(f'"open_time" < {int(end_ms)}')
    predicate = " AND ".join(where)

    bucket = f'"open_time" - (("open_time" % {tf_ms}) + {tf_ms}) % {tf_ms}'

    # The dedup window orders on every value column, not on anything positional: physical
    # file order is the one thing two scans of the same lake are allowed to disagree on,
    # and a tie-break that consulted it would let ingest sequence change a backtest.
    dedup_order = ", ".join(
        _quote_ident(name)
        for name in (
            "close_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        )
    )

    return f"""\
SELECT
    "symbol",
    "bucket" AS "open_time",
    "bucket" + {tf_ms} - 1 AS "close_time",
    arg_min("open", "open_time") AS "open",
    max("high") AS "high",
    min("low") AS "low",
    arg_max("close", "open_time") AS "close",
    CAST(sum("volume") AS BIGINT) AS "volume",
    CAST(sum("quote_volume") AS BIGINT) AS "quote_volume",
    CAST(sum("count") AS BIGINT) AS "count",
    CAST(sum("taker_buy_volume") AS BIGINT) AS "taker_buy_volume",
    CAST(sum("taker_buy_quote_volume") AS BIGINT) AS "taker_buy_quote_volume",
    count(*) AS "bar_count",
    count(*) = {expected_bars} AS "complete",
    CAST(sum("copies" - 1) AS BIGINT) AS "duplicate_rows"
FROM (
    SELECT *, {bucket} AS "bucket"
    FROM (
        SELECT *,
               count(*) OVER (PARTITION BY "symbol", "open_time") AS "copies",
               row_number() OVER (
                   PARTITION BY "symbol", "open_time" ORDER BY {dedup_order}
               ) AS "copy_rank"
        FROM {source}
        WHERE {predicate}
    )
    WHERE "copy_rank" = 1
)
GROUP BY "symbol", "bucket"
ORDER BY "symbol", "bucket\""""


def _absent_bucket_ranges(
    present: Sequence[int],
    tf_ms: int,
    first_bucket: int,
    last_bucket: int,
) -> tuple[tuple[int, int], ...]:
    """Contiguous runs of grid buckets with no row, as `[start_open, end_open)` ranges.

    `present` must be sorted ascending and lie within `[first_bucket, last_bucket]`; both
    bounds are bucket *open* times on the `tf_ms` grid. Walking the present list against
    the arithmetic grid keeps this O(rows + ranges) rather than materialising the whole
    grid -- a year of expected 1 m buckets is half a million integers nobody needs.
    """
    ranges: list[tuple[int, int]] = []
    previous = first_bucket - tf_ms
    for open_ms in present:
        if open_ms - previous > tf_ms:
            ranges.append((previous + tf_ms, open_ms))
        previous = open_ms
    if previous < last_bucket:
        ranges.append((previous + tf_ms, last_bucket + tf_ms))
    return tuple(ranges)


@dataclass(frozen=True, slots=True)
class MissingBuckets:
    """Which calendar buckets of a range produced no derived bar at all (finding C9).

    `timeframe_sql` aggregates the rows that exist, so a bucket with zero constituent
    bars simply has no output row: `complete` cannot flag it (there is no row to carry
    the flag) and at `timeframe='1m'` `complete` is vacuously true for every stored bar.
    A 1 m backtest over a lake missing three hours therefore loaded 44 460 bars instead
    of 44 640 and finished without a word. This type is the calendar's side of the story:
    the buckets `[start_ms, end_ms)` *should* contain, minus the ones the lake produced.

    `ranges` are `[start_open_ms, end_open_ms)` half-open spans of consecutive missing
    bucket open times, so `end - start` over `tf_ms` is exactly the count in that run and
    two adjacent ranges can never touch. `expected` counts every grid bucket overlapping
    the range, including ones the range only partially covers -- a partially-covered
    bucket still has at least one 1 m slot inside the range, so a complete lake gives it
    a (flagged-incomplete) row and only genuine absence lands here.
    """

    timeframe: str
    tf_ms: int
    start_ms: int
    end_ms: int
    expected: int
    present: int
    ranges: tuple[tuple[int, int], ...]

    @property
    def missing(self) -> int:
        return self.expected - self.present

    def describe(self, limit: int = 5) -> str:
        """The missing ranges as text, for warnings and refusals."""
        shown = ", ".join(
            f"[{start} .. {end}) = {(end - start) // self.tf_ms} bucket(s)"
            for start, end in self.ranges[:limit]
        )
        more = f" (and {len(self.ranges) - limit} more range(s))" if len(self.ranges) > limit else ""
        return f"{self.missing} of {self.expected} {self.timeframe} bucket(s) absent: {shown}{more}"


def missing_buckets(
    root: Path | str,
    timeframe: str,
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    dataset: str = "klines",
) -> MissingBuckets:
    """Compare the derivable buckets of a range against the calendar grid (finding C9).

    This is the check `timeframe_sql` structurally cannot make: its `GROUP BY` only sees
    buckets that have at least one stored 1 m bar, so `complete` catches a *partially*
    missing bucket and says nothing about a wholly missing one. The expected grid here
    comes from arithmetic on `(start_ms, end_ms, timeframe)` alone -- the same epoch-anchored
    buckets `bucket_start_ms` produces -- so no amount of missing data can shrink it.

    One symbol at a time, deliberately. Presence is per symbol, and a multi-symbol answer
    would have to say *which* symbol each absence belongs to -- at which point it is this
    call in a loop, less clearly.

    The scan reads one `DISTINCT` over the bucket expression with the partition predicate
    applied, so the cost is a pruned column scan, not a second full derivation.
    """
    if end_ms <= start_ms:
        raise ValueError(
            f"empty range: end_ms {end_ms} precedes start_ms {start_ms}. Ranges are "
            f"half-open [start, end), so this can only be a caller error"
        )

    tf_ms = timeframe_ms(timeframe)
    if dataset not in KLINE_SHAPED:
        raise ValueError(
            f"dataset {dataset!r} is not kline-shaped; bucket coverage is defined over "
            f"the 1 m bar grid and only {sorted(KLINE_SHAPED)} store it"
        )

    bucket = f'"open_time" - (("open_time" % {tf_ms}) + {tf_ms}) % {tf_ms}'
    predicate = " AND ".join(
        [
            partition_predicate(dataset, symbol=symbol, start_ms=start_ms, end_ms=end_ms),
            f'"open_time" >= {int(start_ms)}',
            f'"open_time" < {int(end_ms)}',
        ]
    )
    sql = (
        f'SELECT DISTINCT {bucket} AS "bucket" FROM {_quote_ident(dataset)} '
        f'WHERE {predicate} ORDER BY "bucket"'
    )
    present = query(root, sql, datasets=(dataset,)).column("bucket").to_pylist()

    first_bucket = bucket_start_ms(start_ms, tf_ms)
    last_bucket = bucket_start_ms(end_ms - 1, tf_ms)
    expected = (last_bucket - first_bucket) // tf_ms + 1

    return MissingBuckets(
        timeframe=timeframe,
        tf_ms=tf_ms,
        start_ms=start_ms,
        end_ms=end_ms,
        expected=expected,
        present=len(present),
        ranges=_absent_bucket_ranges(present, tf_ms, first_bucket, last_bucket),
    )


def derive_timeframe(
    root: Path | str,
    timeframe: str,
    *,
    symbol: str | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
    dataset: str = "klines",
    on_incomplete: str = "flag",
) -> pa.Table:
    """Execute `timeframe_sql` against the lake and return the derived bars.

    `on_incomplete` decides what happens to a bucket that did not contain its full
    complement of 1 m bars. It is spelled out rather than defaulted quietly, in the spirit
    of spec 4.5's gap policy:

    - `flag` (default) -- return every bucket with `bar_count` and `complete` set. This is
      not a silent default: the row says what it is, and a partial *last* bucket is the
      normal, correct result of a range that ends mid-bucket.
    - `drop` -- return only complete buckets. Right for indicator input, where a partial
      bar is a lie about the period; wrong if you needed to know the gap was there.
    - `raise` -- refuse, naming the incomplete buckets **and the wholly absent ones**.
      Right for a backtest under `STRICT`, which must not run over a gapped range at all.
      Absent buckets need their own check because the aggregate cannot emit a row for
      rows that do not exist (finding C9): with both `start_ms` and `end_ms` given the
      expected grid comes from the calendar, so leading and trailing absences are caught
      too; without bounds only the span between the first and last observed bucket can be
      judged, which is stated here rather than silently narrowed.

    `flag` and `drop` cannot represent an absent bucket -- there is no row to flag or to
    drop -- so a caller on those paths that needs to *see* absence must compare against
    the calendar with `missing_buckets`. That is not this function quietly declining: a
    fabricated placeholder row would be a bar that never existed, which spec 4.5 forbids.

    There is deliberately no mode that fills or interpolates a missing bucket. Spec 4.5:
    interpolating across a gap manufactures prices that never traded.
    """
    if on_incomplete not in ("flag", "drop", "raise"):
        raise ValueError(
            f"unknown on_incomplete policy {on_incomplete!r}; expected "
            f"'flag', 'drop' or 'raise'"
        )

    sql = timeframe_sql(
        timeframe,
        symbol=symbol,
        start_ms=start_ms,
        end_ms=end_ms,
        dataset=dataset,
    )
    table = query(root, sql, datasets=(dataset,))

    if on_incomplete == "flag":
        return table

    complete = table.column("complete").to_pylist()
    if on_incomplete == "drop":
        keep = [i for i, ok in enumerate(complete) if ok]
        # Typed indices, not a bare list: pyarrow infers `null` as the type of an empty
        # list, and `take` has no (string, null) kernel -- so a range in which *no*
        # bucket was complete crashed instead of returning the empty table it means.
        return table.take(pa.array(keep, type=pa.int64()))

    # Wholly absent buckets first: they are the failure `complete` cannot carry, because
    # the aggregate has no row to carry it on (finding C9). Judged per symbol -- absence
    # is a per-symbol fact -- against the calendar where the caller stated one, and
    # against the observed first/last bucket where it did not.
    tf_ms = timeframe_ms(timeframe)
    opens_by_symbol: dict[str, list[int]] = {}
    for sym, open_ms in zip(
        table.column("symbol").to_pylist(), table.column("open_time").to_pylist()
    ):
        opens_by_symbol.setdefault(sym, []).append(open_ms)
    if not opens_by_symbol and start_ms is not None and end_ms is not None:
        first_bucket = bucket_start_ms(start_ms, tf_ms)
        last_bucket = bucket_start_ms(end_ms - 1, tf_ms)
        raise ValueError(
            f"every {timeframe} bucket in [{start_ms}, {end_ms}) is wholly absent "
            f"({(last_bucket - first_bucket) // tf_ms + 1} expected, 0 derivable). "
            f"The lake holds no 1m bars for this range"
            + (f" for {symbol}" if symbol is not None else "")
            + "; run the gap report to see why."
        )
    for sym in sorted(opens_by_symbol):
        opens = sorted(opens_by_symbol[sym])
        first_bucket = (
            bucket_start_ms(start_ms, tf_ms) if start_ms is not None else opens[0]
        )
        last_bucket = (
            bucket_start_ms(end_ms - 1, tf_ms) if end_ms is not None else opens[-1]
        )
        absent = _absent_bucket_ranges(opens, tf_ms, first_bucket, last_bucket)
        if absent:
            count = sum((end - start) // tf_ms for start, end in absent)
            shown = ", ".join(
                f"[{start} .. {end})" for start, end in absent[:5]
            )
            more = f" (and {len(absent) - 5} more range(s))" if len(absent) > 5 else ""
            raise ValueError(
                f"{sym}: {count} {timeframe} bucket(s) wholly absent from the range: "
                f"{shown}{more}. These buckets have no stored 1m bars at all, so no "
                f"derived row exists to flag; run the gap report to see why the lake "
                f"is missing them."
            )

    bad = [
        (sym, open_ms, bars)
        for sym, open_ms, bars, ok in zip(
            table.column("symbol").to_pylist(),
            table.column("open_time").to_pylist(),
            table.column("bar_count").to_pylist(),
            complete,
        )
        if not ok
    ]
    if bad:
        expected = timeframe_ms(timeframe) // TIMEFRAMES["1m"]
        shown = ", ".join(
            f"{sym} open_time={ms} had {bars}/{expected} 1m bars" for sym, ms, bars in bad[:5]
        )
        more = f" (and {len(bad) - 5} more)" if len(bad) > 5 else ""
        raise ValueError(
            f"{len(bad)} incomplete {timeframe} bucket(s): {shown}{more}. "
            f"A bucket short of its 1m bars has understated volume and possibly the "
            f"wrong high, low or close; run the gap report before trusting it."
        )
    return table
