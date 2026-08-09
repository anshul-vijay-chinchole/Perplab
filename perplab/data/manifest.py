"""Dataset manifests -- a record of exactly what data a run saw (spec 4.6).

A backtest is only evidence if you can say what it was run against. Six months later the
lake has been backfilled, compacted, re-downloaded after a corrupt archive, and extended
by the collector; the same strategy over the same date range now prints a different curve.
Spec 4.6 is blunt about the standard: *"why doesn't this backtest match the one I ran last
month" should be a two-second answer, not an investigation*. The manifest is that answer.
It is written next to a run, recomputed when the run is repeated, and diffed.

**What the fingerprint is, and why it is not the file contents.** The `sha256` per dataset
is taken over the sorted list of `(file_path, file_size, file_mtime_ns)`, exactly as spec
4.6 prescribes. Hashing several hundred gigabytes of Parquet on every run would make the
check expensive enough that people turn it off, and a check that is turned off detects
nothing. Stat metadata is cheap and catches every realistic mutation: a re-ingest rewrites
the file (mtime moves), a compaction merges part-files (the path set changes), a truncated
download shortens it (size moves). It does not catch a byte flipped in place with the size
and mtime restored, which is a scenario that does not occur outside deliberate tampering,
and Binance's own `.CHECKSUM` verification at ingest (spec 4.5) is where content integrity
belongs anyway.

**Two properties the fingerprint must have or it is worse than nothing.** A hash that
changes when nothing changed trains you to ignore the warning, which is the failure mode
this whole mechanism exists to prevent. So:

- *The serialisation is explicit.* One record per line, `path\\0size\\0mtime_ns\\n`, UTF-8,
  sorted by path. NUL cannot appear in a path on any filesystem we support, so no value can
  smuggle a separator and shift the field boundaries. Nothing here depends on `repr`, on
  `json` key ordering, or on the iteration order of a dict -- all three are things that
  have historically changed between Python releases and would silently invalidate every
  stored manifest.
- *Paths are recorded relative to the userdata root, with forward slashes.* An absolute
  path embeds the machine (`C:/Users/anshul/...` vs `/home/anshul/...`), so an
  absolute-path hash reports "your data changed" when all that happened is that the lake
  moved or a colleague ran the same range. `os.sep` embeds the platform, so the same lake
  on a shared drive would hash differently from Windows and from Linux. Neither difference
  is a data difference, and both would produce exactly the false alarm described above.

**This module is the one that takes the *userdata* root, not the lake root.** Every other
reader -- the writer, the bulk ingester, the gap detector, the query layer -- is handed
`<userdata>/market` and calls it `root`. The manifest is handed `<userdata>` itself,
because it is the only component that records both market data and the `reference/`
snapshots a run validated its orders against, and one base is what keeps every recorded
path relative to a single thing. The parameter is named `userdata` rather than `root` so
the difference is visible at every call site: passing `<userdata>/market` here does not
raise, it silently finds no reference snapshots and flags every manifest
`FILTERS_APPROXIMATE`. `schemas.market_root` is the only place the extra level is applied.

**`fill_model_tier` is derived here, never passed in.** Spec 4.2's central rule is that a
fill-model downgrade must never happen invisibly. A tier supplied by the caller is an
assumption; a tier derived from the partitions actually present on disk is an observation.
Finding F1 makes this concrete: bulk `bookTicker` exists only for 2023-05-16 .. 2024-03-30,
so `TRADE_ONLY` -- not `BOOK_TICKER` -- is the honest answer for most historical ranges,
and a manifest that inherited the spec's optimistic assumption would say otherwise.

**Gaps are detected by `gaps.py` and consumed here, in that direction only.** Gap detection
reads row-level timestamps; the manifest records the verdict, so a manifest can be
recomputed for comparison without re-running detection over a lake that may no longer hold
the data. Consuming them is not merely storage: a partition that exists but has a hole in
it cannot support the fill model its presence implies, so an unexplained gap in a tier's
input demotes the tier and raises `FILL_TIER_LIMITED_BY_GAPS`. Without that, coverage is
judged at partition granularity -- day-level for the tick datasets -- and `BOOK_WALK` would
be granted over a day the collector was down for nine hours of.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from perplab.data.gaps import Gap, GapReport
from perplab.data.query import partition_predicate, query
from perplab.data.reference import latest_snapshot
from perplab.data.schemas import (
    MARKET_SUBDIR,
    SCHEMAS,
    SYMBOLLESS_DATASETS,
    layout_for,
    market_root,
    normalise_symbol,
    partition_components,
    partition_key,
)

__all__ = [
    "MANIFEST_VERSION",
    "MARKET_SUBDIR",
    "MANIFEST_DATASETS",
    "FILL_MODEL_TIERS",
    "ManifestError",
    "CoverageError",
    "FileRecord",
    "DatasetEntry",
    "Manifest",
    "ManifestDiff",
    "lake_relative_path",
    "market_root",
    "fingerprint_payload",
    "fingerprint",
    "scan_dataset",
    "dataset_covers",
    "derive_fill_model_tier",
    "unexplained_gap_ms",
    "TICK_GAP_TOLERANCE",
    "resolve_reference",
    "build_manifest",
    "write_manifest",
    "read_manifest",
    "diff_manifests",
]

MANIFEST_VERSION = 1
"""Bumped when the on-disk shape changes. `read_manifest` refuses anything else rather
than best-effort parsing it: a manifest read under the wrong assumptions would compare
unequal for reasons that have nothing to do with the data, and the resulting "your lake
changed" warning would send someone looking for a problem that does not exist."""

_MS_PER_DAY = 86_400_000

MANIFEST_DATASETS: tuple[str, ...] = tuple(
    sorted(set(SCHEMAS) - SYMBOLLESS_DATASETS)
)
"""Derived from `SCHEMAS` rather than listed, so a dataset added to the lake is manifested
by default. The alternative -- an explicit list -- fails silently in the one direction that
matters: new data would be read by queries but absent from the record of what was read.

`SYMBOLLESS_DATASETS` (today just `collectorEvents`) is subtracted because those datasets
carry no `symbol=` level and so cannot be scoped to a run's symbols. That exclusion earns
its keep twice over for the collector's log: it grows every sixty seconds whether or not
any market data changed, so including it would make every recomputed manifest differ from
every saved one -- precisely the false positive that teaches a user to ignore the diff. Its
content still matters, since it is what turns a gap into an *explained* gap (spec 4.5), but
that is evidence *about* a gap and reaches the manifest through `gaps`, not as data a run
consumed."""

FILL_MODEL_TIERS: tuple[str, ...] = ("BOOK_WALK", "BOOK_TICKER", "TRADE_ONLY", "BAR_CLOSE")
"""Spec 4.2, descending fidelity. Order is load-bearing: `derive_fill_model_tier` returns
the first tier whose inputs are actually on disk."""


class ManifestError(RuntimeError):
    """Something about the lake prevents an honest manifest from being written."""


class CoverageError(ManifestError):
    """No fill model at all is supportable over the requested range.

    Distinct from a *downgrade*: a downgrade is recorded and the run proceeds, whereas this
    means the lake holds nothing usable for these symbols and dates. Returning `BAR_CLOSE`
    here would be a lie -- spec 4.2 lists `klines` as that tier's requirement -- and the run
    would proceed against no data at all, producing a flat equity curve that looks like a
    strategy result rather than an empty lake.
    """


# --------------------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------------------


def lake_relative_path(userdata: Path | str, path: Path | str) -> str:
    """Path of `path` relative to the userdata root, forward-slashed.

    Both halves of that sentence are there to keep the fingerprint from moving when the
    data has not: the relative part removes the machine, the forward slashes remove the
    platform. See the module docstring.

    A path outside the root raises rather than falling back to the absolute form. Silently
    absolutising one record would poison the whole dataset hash with a machine-specific
    string, and the resulting mismatch would be indistinguishable from real data drift --
    the single most expensive kind of false alarm this module can produce.
    """
    root_resolved = Path(userdata).resolve()
    resolved = Path(path).resolve()
    try:
        relative = resolved.relative_to(root_resolved)
    except ValueError:
        raise ManifestError(
            f"{resolved} is not inside the userdata root {root_resolved}; refusing to "
            f"record a machine-specific absolute path in a manifest"
        ) from None
    return relative.as_posix()


# --------------------------------------------------------------------------------------
# Fingerprint
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, order=True)
class FileRecord:
    """One row of the `(file_path, file_size, file_mtime_ns)` list spec 4.6 hashes.

    `order=True` so sorting is by path first and is defined by the dataclass rather than by
    whatever key a caller happens to pass -- the sort order is part of the hash definition,
    not an implementation detail of one call site.
    """

    path: str
    """Lake-relative, forward slashes. Never absolute; see `lake_relative_path`."""
    size: int
    mtime_ns: int


def fingerprint_payload(records: Iterable[FileRecord]) -> bytes:
    """The exact bytes that get hashed.

    Exposed rather than inlined so that a test can assert the serialisation itself. If this
    format ever changes, every stored manifest silently stops matching a recomputation of
    identical data, so the format needs to be pinned by a test that fails loudly when
    someone reformats it -- not merely by a hash whose value nobody can eyeball.

    `\\0` separates the fields because it is the one byte that cannot occur in a path on
    NTFS or on any POSIX filesystem, so no filename can forge a field boundary. The
    trailing newline per record means a path ending in whitespace cannot be confused with
    the start of the next record either.
    """
    return b"".join(
        f"{record.path}\0{record.size}\0{record.mtime_ns}\n".encode("utf-8")
        for record in sorted(records)
    )


def fingerprint(records: Iterable[FileRecord]) -> str:
    """SHA-256 of `fingerprint_payload`, hex.

    An empty record list hashes the empty string, which is a real and useful answer: a
    dataset that is absent is not the same as one that is present and changed, and the
    difference shows up in the diff as an added or removed dataset rather than as a hash
    that happens to collide with nothing.
    """
    return hashlib.sha256(fingerprint_payload(records)).hexdigest()


# --------------------------------------------------------------------------------------
# Scanning the lake
# --------------------------------------------------------------------------------------


def _expected_partitions(dataset: str, start_ms: int, end_ms: int) -> tuple[tuple[str, ...], ...]:
    """Hive components below `symbol=` that could hold data for this range.

    Built by asking `schemas.partition_components` about each UTC day in the range rather
    than by parsing directory names. That keeps the reader and the writer using one
    definition of where a row lives: if the layout changes, the manifest follows it
    automatically, whereas a hand-rolled path builder here would keep looking in the old
    place and report a lake that had gone empty.

    **The range is half-open, `[start_ms, end_ms)`**, so the last partition needed is the
    one holding `end_ms - 1`. A range ending at exactly midnight does *not* reach into that
    day, because none of it was requested.

    That convention is not this function's preference; it is the one `gaps.py` filters on
    (`ts >= start AND ts < end`) and the one `query.partition_predicate` emits, and it is
    the only convention under which two adjacent ranges neither double-count nor skip the
    boundary. This module previously treated the end as inclusive, which was defensible in
    isolation and wrong in company: handing the same `(start_ms, end_ms)` to the manifest
    and to gap detection made the manifest demand a partition for a day the gap report had
    already decided was outside the range, so every range ending on a midnight boundary --
    which is every range the CLI produces -- failed coverage against a lake that was
    complete.
    """
    if end_ms <= start_ms:
        raise ValueError(
            f"empty range: end {end_ms} precedes start {start_ms} or equals it. Ranges "
            f"are half-open [start_ms, end_ms), so an empty one covers no partition at "
            f"all while a vacuous coverage check would report every dataset complete"
        )

    seen: dict[tuple[str, ...], None] = {}
    day = start_ms - start_ms % _MS_PER_DAY
    last = (end_ms - 1) - (end_ms - 1) % _MS_PER_DAY
    while day <= last:
        seen[partition_components(dataset, day)] = None
        day += _MS_PER_DAY
    return tuple(seen)


def _partition_files(directory: Path) -> list[Path]:
    """Published Parquet files in one partition directory, sorted.

    Only `*.parquet`, and never a name starting with a dot. The writer publishes atomically
    via `.<stem>.parquet.tmp` -> `os.replace`, so a dotted name is by definition a file that
    is still being written. Hashing one would record a size and mtime that are already
    obsolete by the time the manifest is stored, producing a mismatch on the very next
    recomputation.
    """
    if not directory.is_dir():
        return []
    return sorted(
        p
        for p in directory.iterdir()
        if p.is_file() and p.suffix == ".parquet" and not p.name.startswith(".")
    )


def scan_dataset(
    userdata: Path | str,
    dataset: str,
    symbols: Sequence[str],
    start_ms: int,
    end_ms: int,
) -> tuple[FileRecord, ...]:
    """Every published file of one dataset that overlaps the range, for these symbols.

    Selection is at partition granularity, which is as fine as the layout allows: a
    date-partitioned dataset resolves to the day, `funding` -- one partition per symbol,
    with no time component at all -- resolves to the whole symbol. That is stated here
    rather than hidden, because it means a manifest naming a one-month funding range still
    fingerprints the symbol's entire funding history, and a diff will therefore flag a
    2021 funding backfill against a 2024 run. Over-reporting a change is the safe
    direction; the alternative is a run whose inputs moved without the manifest noticing.
    """
    lake = Path(userdata)
    market = market_root(lake)
    records: list[FileRecord] = []
    for symbol in sorted({normalise_symbol(s) for s in symbols}):
        base = market / dataset / f"symbol={symbol}"
        for components in _expected_partitions(dataset, start_ms, end_ms):
            directory = base.joinpath(*components)
            for path in _partition_files(directory):
                stat = path.stat()
                records.append(
                    FileRecord(
                        path=lake_relative_path(lake, path),
                        size=stat.st_size,
                        mtime_ns=stat.st_mtime_ns,
                    )
                )
    return tuple(sorted(records))


def _row_count(path: Path) -> int:
    """Rows in one Parquet file, from the footer only.

    `read_metadata` reads the footer, not the data, so counting a dataset costs one seek
    per file rather than a full scan. A file whose footer cannot be read is raised on
    rather than counted as zero: a truncated Parquet reads as *short* rather than broken in
    DuckDB (see `writer.py`), so a silent zero here would turn real data loss into a row
    count that merely looks a bit low.
    """
    try:
        return int(pq.read_metadata(path).num_rows)
    except Exception as exc:
        raise ManifestError(
            f"cannot read the Parquet footer of {path}: {exc}. The file is truncated or "
            f"not Parquet; re-ingest that partition rather than manifesting it"
        ) from exc


def _dataset_key(dataset: str) -> str:
    """Manifest key for a dataset, e.g. `klines_1m`.

    Spec 4.6's own example writes `klines_1m`, and it is right to: the bar interval is part
    of the dataset's identity, and the lake is laid out to allow a second interval beside
    the first (`interval=` in the path, spec 4.3). The suffix is taken from the registered
    partition layout rather than hardcoded, so a dataset stored at another interval cannot
    end up sharing a manifest key with `1m` and silently averaging two things together.
    """
    interval = layout_for(dataset).interval
    return f"{dataset}_{interval}" if interval is not None else dataset


# --------------------------------------------------------------------------------------
# Fill model tier
# --------------------------------------------------------------------------------------


def _covers_every_day(
    market: Path,
    dataset: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> bool:
    """Whether every UTC day the range overlaps has at least one row (finding M23).

    The refinement behind `dataset_covers` for coarsely partitioned datasets. Partition
    presence for a month-partitioned dataset answers "is there a file somewhere in this
    month", which is thirty times weaker than the day-level question the tier check
    actually asks -- a single `2026-08-01` kline file made a thirty-day manifest assert
    coverage that did not exist. `tiers.py` fixed exactly this for the engine
    (`_uncovered_bars`, judging by row timestamps instead of file presence); this is the
    same judgment held at day granularity, so the manifest's answer for a
    month-partitioned dataset is exactly as strong as the one it already gives for a
    date-partitioned one.

    One partition-pruned `count(DISTINCT day)` aggregate per symbol. The timestamp
    column is scanned, but only inside the pruned range, and only for datasets whose
    layout is coarser than a day -- the date-partitioned ones already get this answer
    from the directory tree for free.
    """
    day_lo = start_ms - start_ms % _MS_PER_DAY
    day_hi = (end_ms - 1) - (end_ms - 1) % _MS_PER_DAY + _MS_PER_DAY
    expected_days = (day_hi - day_lo) // _MS_PER_DAY

    column = f'"{layout_for(dataset).time_column}"'
    pruning = partition_predicate(dataset, symbol=symbol, start_ms=day_lo, end_ms=day_hi)
    sql = (
        f"SELECT count(DISTINCT {column} // {_MS_PER_DAY}) AS days "
        f'FROM "{dataset}" WHERE {pruning} '
        f"AND {column} >= {int(day_lo)} AND {column} < {int(day_hi)}"
    )
    table = query(market, sql, datasets=(dataset,))
    found = table.column("days").to_pylist()[0] or 0
    return int(found) >= expected_days


def dataset_covers(
    userdata: Path | str,
    dataset: str,
    symbols: Sequence[str],
    start_ms: int,
    end_ms: int,
) -> bool:
    """Does this dataset cover every expected partition -- at day granularity or better?

    Every symbol and every partition, not any: a fill model that needs `depth20` needs it
    for the whole run, and a tier granted on the strength of one symbol's coverage would
    silently apply book walking to a symbol that has no book.

    The file-presence check is partition-level, which for the date-partitioned tick
    datasets means day-level already. For datasets partitioned by month or year it is
    not, and file presence alone let one day's file assert a whole month (finding M23) --
    so those layouts get a second, row-level check that every UTC day in the range holds
    at least one row (`_covers_every_day`, mirroring `tiers._uncovered_bars`). Day
    granularity is the deliberate floor in both directions: a day holding one part-file
    counts as covered even if the collector was down for nine hours of it, because
    sub-day holes are gap detection's business (spec 4.5), they arrive on the manifest
    through `gaps`, and re-deriving them here would mean re-answering a question the gap
    report already answers.
    """
    lake = Path(userdata)
    market = market_root(lake)
    coarse = layout_for(dataset).granularity in ("month", "year")
    for symbol in sorted({normalise_symbol(s) for s in symbols}):
        base = market / dataset / f"symbol={symbol}"
        for components in _expected_partitions(dataset, start_ms, end_ms):
            if not _partition_files(base.joinpath(*components)):
                return False
        if coarse and not _covers_every_day(market, dataset, symbol, start_ms, end_ms):
            return False
    return True


def unexplained_gap_ms(
    gaps: Sequence[Any], symbols: Sequence[str], start_ms: int, end_ms: int
) -> dict[str, int]:
    """How much of the range each dataset is *missing*, in ms, from unexplained gaps only.

    This used to answer a boolean -- "does this dataset have a gap anywhere in the range"
    -- and the boolean was the bug. A dataset absent for the whole range and a dataset
    absent for nineteen minutes of a year both came back `True`, and both were struck out
    of the upper fill tiers. See `derive_fill_model_tier` for why that is the wrong trade.

    Only unexplained gaps count. An explained gap is still missing data and still belongs
    on the manifest, but its cause is known and recorded, and treating a clean SHUTDOWN as
    grounds for silently re-pricing every fill in the run would make the tier depend on the
    collector's uptime log rather than on what the run can actually model.

    A gap whose `symbol` is null -- `collectorEvents`, which describes the process rather
    than an instrument -- counts for every symbol. That is what it means: the recorder was
    not running, for all of them at once.

    **Overlapping gaps are merged, not summed.** Two symbols down over the same hour is one
    missing hour of range, and adding them would let a two-symbol run exceed any budget
    twice as fast as a one-symbol run over identically holed data.

    Entries that are neither `Gap` nor a mapping carrying the fields are ignored rather
    than raised on. `gaps` is a spec 4.6 free-form field and a caller may legitimately pass
    a reloaded manifest's list back in; refusing to build a manifest because an old one
    stored a shape this build does not recognise would block the recomputation that the
    diff exists to perform.
    """
    wanted = {normalise_symbol(s) for s in symbols}
    spans: dict[str, list[tuple[int, int]]] = {}
    for gap in gaps:
        if isinstance(gap, Gap):
            entry: Mapping[str, Any] = gap.to_json()
        elif isinstance(gap, Mapping):
            entry = gap
        else:
            continue

        dataset = entry.get("dataset")
        start = entry.get("start_ms")
        end = entry.get("end_ms")
        if not isinstance(dataset, str) or not isinstance(start, int) or not isinstance(end, int):
            continue
        if entry.get("explained") or entry.get("explanation"):
            continue

        symbol = entry.get("symbol")
        if isinstance(symbol, str) and normalise_symbol(symbol) not in wanted:
            continue
        # Clipped to the range, then half-open on both sides: a gap ending exactly at
        # `start_ms` removed nothing from the range, and one opening at `end_ms` is after it.
        lo, hi = max(start, start_ms), min(end, end_ms)
        if lo < hi:
            spans.setdefault(dataset, []).append((lo, hi))

    missing: dict[str, int] = {}
    for dataset, intervals in spans.items():
        total, cursor = 0, start_ms
        for lo, hi in sorted(intervals):
            lo = max(lo, cursor)
            if hi > lo:
                total += hi - lo
                cursor = hi
        missing[dataset] = total
    return missing


TICK_GAP_TOLERANCE = 0.01
"""Fraction of a range that unexplained gaps may cover before a tick dataset stops counting
as an input to the upper fill tiers.

A threshold rather than a boolean, because the boolean made the platform's own worst
mistake: it degraded a whole run to protect a sliver of it. A single nineteen-minute venue
halt inside a 365-day range -- one where the klines themselves show eighteen consecutive
zero-volume bars, so nothing traded to be recorded -- struck `aggTrades` out entirely and
sent the run to `BAR_CLOSE`. That re-prices **every** fill across the whole year at the
lowest fidelity in order to avoid mispricing 0.004% of it, which is strictly worse
accounting than the thing it was avoiding.

The rule scales with the range on purpose. Nineteen minutes is noise in a year and material
in a day, and the same 1% budget says so without a second constant. The motivating case for
the original strictness still demotes: a collector down for nine hours of a one-day range is
37% of it.

**Tolerated is not hidden.** `tiers.resolve_tier` flags what it let through, names the
dataset and the duration, and every gap remains on the manifest and in the gap report.
"""


def derive_fill_model_tier(
    userdata: Path | str,
    symbols: Sequence[str],
    start_ms: int,
    end_ms: int,
    *,
    gaps: Sequence[Any] = (),
) -> str:
    """The best fill model the data on disk can actually support (spec 4.2).

    Derived, never accepted from a caller. Spec 4.2's rule is that a downgrade is recorded
    and never happens invisibly, and the only way to keep that promise is to read the lake:
    a tier passed in is a statement of what someone hoped was there.

    Expect `TRADE_ONLY` for most historical ranges. Spec 4.2 calls `BOOK_TICKER` "the
    default for most backtests", but finding F1 established that bulk `bookTicker` covers
    only 2023-05-16 .. 2024-03-30 -- 320 days of a six-and-a-half-year instrument history.
    `BOOK_WALK` is available only from the day the collector started, because L2 depth
    exists nowhere else (spec 4.2, decision 1).

    `gaps` is the gap report for the same range (a `GapReport`, its `Gap` tuple, or the
    JSON form). A dataset whose unexplained gaps cover more than `TICK_GAP_TOLERANCE` of
    the range does not count as an input to the three upper tiers, however complete its
    partition list looks. Presence is judged at partition granularity -- day-level for the
    tick datasets -- so without this a single part-file would win `BOOK_WALK` for a day the
    collector was down for nine hours of, and the run would walk a book that stops moving
    mid-afternoon. Defaults to empty, so a caller with no gap report gets the coverage-only
    answer rather than a silently optimistic one it cannot tell apart.

    **The budget is a fraction, not a boolean, and that distinction is load-bearing.** The
    boolean form demoted a 365-day `TRADE_ONLY` request to `BAR_CLOSE` over a single
    nineteen-minute Binance halt, which made every fill in the run worse in order to avoid
    getting a handful of them wrong. `TICK_GAP_TOLERANCE` documents the trade; what it lets
    through is flagged by `tiers.resolve_tier`, never swallowed.

    **Gaps demote between tiers; they do not reach the floor.** `BAR_CLOSE` is decided on
    presence alone, because there is no tier below it to demote to and because a holed
    kline range is already the gap policy's decision to make (spec 4.5: `STRICT` refuses,
    `HALT_TRADING` models the outage). Refusing to name a tier for it would block the
    manifest that records the holes, which is the artefact someone needs in order to act on
    them.

    Raises rather than returning `BAR_CLOSE` when not even `klines` covers the range; see
    `CoverageError`. That check stays gap-free on purpose -- `CoverageError` means the lake
    holds nothing for these symbols and dates, which is a statement about presence, and
    conflating it with completeness would report an ingested-but-holed range as an empty
    one.
    """
    missing_ms = unexplained_gap_ms(gaps, symbols, start_ms, end_ms)
    budget_ms = max(0, end_ms - start_ms) * TICK_GAP_TOLERANCE

    def usable(dataset: str) -> bool:
        if missing_ms.get(dataset, 0) > budget_ms:
            return False
        return dataset_covers(userdata, dataset, symbols, start_ms, end_ms)

    if usable("depth20"):
        return "BOOK_WALK"

    trades = usable("aggTrades")
    if trades and usable("bookTicker"):
        return "BOOK_TICKER"
    if trades:
        return "TRADE_ONLY"
    if dataset_covers(userdata, "klines", symbols, start_ms, end_ms):
        return "BAR_CLOSE"

    raise CoverageError(
        f"no fill model is supportable for {sorted(set(symbols))} over "
        f"{partition_key(start_ms)}..{partition_key(end_ms)}: none of depth20, aggTrades, "
        f"bookTicker or klines has a published file in every partition of the range. "
        f"Ingest the range before running against it"
    )


# --------------------------------------------------------------------------------------
# Reference snapshots
# --------------------------------------------------------------------------------------


def resolve_reference(
    userdata: Path | str, start_ms: int, end_ms: int
) -> tuple[str | None, str | None, tuple[str, ...]]:
    """Snapshot ids in force at the *start* of the range, plus any flags that raises.

    Spec 3.2: a backtest over 2023 data must use the 2023 snapshot, not today's. So the
    lookup is `on_or_before` the range's first day, and it returns `None` -- never today's
    file -- when nothing that old exists. Snapshotting only began on 2026-08-01, so `None`
    is the answer for every historical range today; that is exactly why it has to be
    visible. `FILTERS_APPROXIMATE` on the manifest is spec 3.2's own name for it.

    A second, quieter failure is also flagged. If a *newer* snapshot exists inside the
    range then the exchange's filters changed part-way through the run, and a run that
    validated every order against the opening snapshot modelled a rule set that stopped
    being true mid-backtest. Detected by asking for the newest snapshot on or before the
    range's last day and seeing whether it is the same file.

    Brackets are handled identically and are absent today for a different reason:
    `GET /fapi/v1/leverageBracket` is a signed endpoint (finding F3), so nothing has been
    snapshotted yet. `BRACKETS_APPROXIMATE` says so rather than letting the margin engine
    assume current brackets applied in 2023.
    """
    lake = Path(userdata)
    start_date = partition_key(start_ms)
    # `end_ms` is exclusive, so the last day actually in the range is the one holding
    # `end_ms - 1`. Using `end_ms` itself would consult a snapshot dated the morning after
    # a range that stopped at midnight, and report FILTERS_CHANGED_MID_RANGE for a change
    # that happened after the run finished.
    end_date = partition_key(max(end_ms - 1, start_ms))

    resolved: list[str | None] = []
    flags: list[str] = []
    for kind, missing_flag, changed_flag in (
        ("exchangeInfo", "FILTERS_APPROXIMATE", "FILTERS_CHANGED_MID_RANGE"),
        ("leverageBracket", "BRACKETS_APPROXIMATE", "BRACKETS_CHANGED_MID_RANGE"),
    ):
        at_start = latest_snapshot(lake, kind, on_or_before=start_date)
        if at_start is None:
            resolved.append(None)
            flags.append(missing_flag)
            continue
        resolved.append(at_start.stem)
        at_end = latest_snapshot(lake, kind, on_or_before=end_date)
        if at_end is not None and at_end.stem != at_start.stem:
            flags.append(changed_flag)

    return resolved[0], resolved[1], tuple(sorted(flags))


# --------------------------------------------------------------------------------------
# The manifest
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DatasetEntry:
    """`{"files": 24, "rows": 527040, "sha256": "a3f2..."}` from spec 4.6.

    `files` and `rows` are carried alongside the hash because the hash alone answers "did
    it change" and nothing else. A diff that can say *366 files became 367 and rows grew by
    ninety thousand* points straight at an extra day of backfill; a diff that can only say
    the hashes differ starts an investigation.
    """

    files: int
    rows: int
    sha256: str

    def to_json(self) -> dict[str, Any]:
        return {"files": self.files, "rows": self.rows, "sha256": self.sha256}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> DatasetEntry:
        return cls(
            files=int(obj["files"]), rows=int(obj["rows"]), sha256=str(obj["sha256"])
        )


@dataclass(frozen=True, slots=True)
class Manifest:
    """Spec 4.6's document, in memory.

    Two fields go beyond the spec's example and both earn their place. `manifest_version`
    is written so a future shape change cannot be misread as a data change. `flags` carries
    the conditions the spec requires to be *visible* but gives no home in the example --
    `FILTERS_APPROXIMATE` (spec 3.2) and `LOW_FIDELITY` (spec 4.2) -- and a flag that has
    nowhere to live is a flag that ends up nowhere.
    """

    symbols: tuple[str, ...]
    start_ms: int
    end_ms: int
    datasets: dict[str, DatasetEntry]
    exchange_info_snapshot: str | None
    leverage_bracket_snapshot: str | None
    gaps: tuple[Any, ...]
    fill_model_tier: str
    flags: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "manifest_version": MANIFEST_VERSION,
            "symbols": list(self.symbols),
            "range": {"start_ms": self.start_ms, "end_ms": self.end_ms},
            "datasets": {
                key: self.datasets[key].to_json() for key in sorted(self.datasets)
            },
            "reference": {
                "exchangeInfo_snapshot": self.exchange_info_snapshot,
                "leverageBracket_snapshot": self.leverage_bracket_snapshot,
            },
            "gaps": list(self.gaps),
            "fill_model_tier": self.fill_model_tier,
            "flags": list(self.flags),
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Manifest:
        version = obj.get("manifest_version")
        if version != MANIFEST_VERSION:
            raise ManifestError(
                f"manifest version {version!r} cannot be read by this build "
                f"(expected {MANIFEST_VERSION}); comparing it against a freshly computed "
                f"manifest would report shape differences as data differences"
            )
        reference = obj.get("reference", {})
        return cls(
            symbols=tuple(obj["symbols"]),
            start_ms=int(obj["range"]["start_ms"]),
            end_ms=int(obj["range"]["end_ms"]),
            datasets={
                key: DatasetEntry.from_json(value)
                for key, value in obj["datasets"].items()
            },
            exchange_info_snapshot=reference.get("exchangeInfo_snapshot"),
            leverage_bracket_snapshot=reference.get("leverageBracket_snapshot"),
            gaps=tuple(obj.get("gaps", ())),
            fill_model_tier=str(obj["fill_model_tier"]),
            flags=tuple(obj.get("flags", ())),
        )


def _as_gap_sequence(gaps: GapReport | Sequence[Any] | None) -> Sequence[Any]:
    """Accept a whole `GapReport`, its `Gap` tuple, or an already-JSON list.

    Taking the report itself is the case that matters, because it is what the caller has:
    `detect_gaps` returns a `GapReport`, and requiring `report.gaps` at every call site is
    one more thing to get wrong -- passing the report by mistake would previously have
    stored a single opaque object rather than raising.
    """
    if gaps is None:
        return ()
    if isinstance(gaps, GapReport):
        return gaps.gaps
    return list(gaps)


def _normalise_gaps(gaps: GapReport | Sequence[Any] | None) -> tuple[Any, ...]:
    """Round-trip the gap report through JSON at build time, not at write time.

    A gap report that cannot be serialised should fail while the caller still has the
    context to fix it, not thirty seconds later when the manifest is written. The round
    trip also canonicalises the value -- tuples become lists, integer keys become strings --
    so that a manifest compared against its own reloaded copy compares equal instead of
    differing on container types nobody chose deliberately.

    `Gap` is a frozen dataclass and `json.dumps` cannot serialise one, so it is converted
    through `Gap.to_json` first. Before that conversion existed the two modules could not
    in fact be wired together: handing `detect_gaps(...).gaps` straight to `build_manifest`
    raised "gap report is not JSON-serialisable", which reads as a caller error and is
    really a missing seam.
    """
    entries = [
        gap.to_json() if isinstance(gap, Gap) else gap for gap in _as_gap_sequence(gaps)
    ]
    try:
        return tuple(json.loads(json.dumps(entries)))
    except (TypeError, ValueError) as exc:
        raise ManifestError(f"gap report is not JSON-serialisable: {exc}") from exc


def build_manifest(
    userdata: Path | str,
    symbols: Sequence[str],
    start_ms: int,
    end_ms: int,
    *,
    gaps: GapReport | Sequence[Any] | None = None,
) -> Manifest:
    """Compute a manifest from what is on disk right now.

    Datasets with no file in the range are omitted rather than recorded as zero. An absent
    dataset and an empty one are different states, and the diff distinguishes them: an
    omission shows up as a dataset appearing or disappearing, which is a far clearer
    account of a backfill than a row count moving from 0.

    Row counts are whole-file totals for every partition that overlaps the range, so a
    range starting mid-day counts that day's file entirely. Trimming to the millisecond
    would mean scanning the timestamp column of every tick file -- minutes of I/O for a
    number whose purpose is to make a diff legible.

    `gaps` may be a whole `GapReport`, its `Gap` tuple, or a plain JSON list from a saved
    manifest. It is recorded, and it also *constrains the fill model tier*: see
    `derive_fill_model_tier`. When a gap is what demotes the tier, `FILL_TIER_LIMITED_BY_
    GAPS` is flagged, so the manifest distinguishes "the data was never ingested" from
    "the data is there but holed" -- two states with the same tier and completely different
    fixes.
    """
    lake = Path(userdata)
    unique = sorted({normalise_symbol(s) for s in symbols})
    if not unique:
        raise ValueError("a manifest over no symbols records nothing; pass at least one")
    if end_ms <= start_ms:
        raise ValueError(
            f"empty range: end {end_ms} precedes start {start_ms} or equals it. Ranges "
            f"are half-open [start_ms, end_ms), matching gaps.py and query.py"
        )

    recorded_gaps = _normalise_gaps(gaps)

    datasets: dict[str, DatasetEntry] = {}
    for dataset in MANIFEST_DATASETS:
        records = scan_dataset(lake, dataset, unique, start_ms, end_ms)
        if not records:
            continue
        rows = sum(_row_count(lake / record.path) for record in records)
        datasets[_dataset_key(dataset)] = DatasetEntry(
            files=len(records), rows=rows, sha256=fingerprint(records)
        )

    tier = derive_fill_model_tier(lake, unique, start_ms, end_ms, gaps=recorded_gaps)
    exchange_info, brackets, flags = resolve_reference(lake, start_ms, end_ms)
    if tier == "BAR_CLOSE":
        # Spec 4.2: the bar-close tier is flagged LOW_FIDELITY on the results page. The
        # badge has to be driven by something, and this is the only place that knows.
        flags = tuple(sorted(set(flags) | {"LOW_FIDELITY"}))
    if recorded_gaps:
        # Computed a second time without the gaps. That is a handful of extra `stat` calls
        # and it buys the one distinction the tier alone cannot make: whether the range is
        # thin because nothing was ingested, or because what was ingested has holes. The
        # first is fixed by a backfill, the second by finding out why the recorder stopped.
        ungapped = derive_fill_model_tier(lake, unique, start_ms, end_ms)
        if ungapped != tier:
            flags = tuple(
                sorted(set(flags) | {"FILL_TIER_LIMITED_BY_GAPS"})
            )

    return Manifest(
        symbols=tuple(unique),
        start_ms=start_ms,
        end_ms=end_ms,
        datasets=datasets,
        exchange_info_snapshot=exchange_info,
        leverage_bracket_snapshot=brackets,
        # `recorded_gaps`, not a second `_normalise_gaps(gaps)` call: the value was
        # already normalised above (finding L25), and round-tripping the report through
        # JSON twice per build was pure duplicated work that also left two spellings for
        # a future edit to make disagree.
        gaps=recorded_gaps,
        fill_model_tier=tier,
        flags=flags,
    )


def write_manifest(manifest: Manifest, path: Path | str) -> Path:
    """Serialise to JSON and publish atomically.

    Same `.tmp` + `os.replace` discipline as the Parquet writer, for the same reason: a
    manifest half-written by an interrupted run is worse than a missing one, because it
    parses far enough to look authoritative. Indented output because this file is read by
    people at least as often as by code -- it is the artefact someone opens when a run
    stops reproducing.
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp = destination.parent / f".{destination.name}.tmp"
    tmp.write_text(
        json.dumps(manifest.to_json(), indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    os.replace(tmp, destination)
    return destination


def read_manifest(path: Path | str) -> Manifest:
    return Manifest.from_json(json.loads(Path(path).read_text(encoding="utf-8")))


# --------------------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ManifestDiff:
    """The answer to "why doesn't this match the run from last month" (spec 4.6).

    A boolean would technically satisfy "warns loudly if it differs" and would be useless:
    it says an investigation is needed without starting one. Every line here names the
    thing that moved and by how much.
    """

    changes: tuple[str, ...]

    @property
    def changed(self) -> bool:
        return bool(self.changes)

    def report(self) -> str:
        if not self.changes:
            return "dataset manifest unchanged: this run saw exactly the data the saved run saw."
        lines = ["dataset manifest DIFFERS from the saved one:"]
        lines.extend(f"  - {change}" for change in self.changes)
        return "\n".join(lines)


def _short(sha: str) -> str:
    return sha[:12]


def _delta(old: int, new: int) -> str:
    return f"{old} -> {new} ({new - old:+d})"


def diff_manifests(old: Manifest, new: Manifest) -> ManifestDiff:
    """Compare a saved manifest against a freshly computed one.

    Lines are ordered by how much each difference can move a result, so the first line is
    usually the explanation. A changed `fill_model_tier` re-prices every fill in the run
    and comes first; changed dataset contents come next; the range and symbol list come
    after those because a caller who changed them already knows. Reference snapshots and
    gaps come last -- they change *whether* a run is trustworthy more often than they
    change its numbers.
    """
    changes: list[str] = []

    if old.fill_model_tier != new.fill_model_tier:
        changes.append(
            f"fill_model_tier: {old.fill_model_tier} -> {new.fill_model_tier} "
            f"(fills are modelled differently; the numbers will not match)"
        )

    for key in sorted(set(old.datasets) | set(new.datasets)):
        before = old.datasets.get(key)
        after = new.datasets.get(key)
        if before is None and after is not None:
            changes.append(
                f"datasets.{key}: added ({after.files} files, {after.rows} rows)"
            )
        elif before is not None and after is None:
            changes.append(
                f"datasets.{key}: gone (was {before.files} files, {before.rows} rows)"
            )
        elif before is not None and after is not None and before != after:
            if before.files == after.files and before.rows == after.rows:
                # The interesting case: the same number of files holding the same number
                # of rows, with different sizes or mtimes. A re-ingest of identical data,
                # or a rewrite that changed the bytes without changing the shape.
                changes.append(
                    f"datasets.{key}: same {after.files} files and {after.rows} rows, but "
                    f"file sizes or mtimes moved (sha256 {_short(before.sha256)} -> "
                    f"{_short(after.sha256)}); the data was rewritten"
                )
            else:
                changes.append(
                    f"datasets.{key}: files {_delta(before.files, after.files)}, "
                    f"rows {_delta(before.rows, after.rows)} "
                    f"(sha256 {_short(before.sha256)} -> {_short(after.sha256)})"
                )

    if old.start_ms != new.start_ms:
        changes.append(
            f"range.start_ms: {old.start_ms} -> {new.start_ms} "
            f"({partition_key(old.start_ms)} -> {partition_key(new.start_ms)})"
        )
    if old.end_ms != new.end_ms:
        changes.append(
            f"range.end_ms: {old.end_ms} -> {new.end_ms} "
            f"({partition_key(old.end_ms)} -> {partition_key(new.end_ms)})"
        )

    added_symbols = sorted(set(new.symbols) - set(old.symbols))
    removed_symbols = sorted(set(old.symbols) - set(new.symbols))
    if added_symbols:
        changes.append(f"symbols: added {', '.join(added_symbols)}")
    if removed_symbols:
        changes.append(f"symbols: removed {', '.join(removed_symbols)}")

    for label, before_ref, after_ref in (
        ("exchangeInfo_snapshot", old.exchange_info_snapshot, new.exchange_info_snapshot),
        (
            "leverageBracket_snapshot",
            old.leverage_bracket_snapshot,
            new.leverage_bracket_snapshot,
        ),
    ):
        if before_ref != after_ref:
            changes.append(
                f"reference.{label}: {before_ref or 'none'} -> {after_ref or 'none'} "
                f"(orders were validated against different exchange rules)"
            )

    if len(old.gaps) != len(new.gaps):
        changes.append(f"gaps: {len(old.gaps)} -> {len(new.gaps)}")
    elif json.dumps(list(old.gaps), sort_keys=True) != json.dumps(
        list(new.gaps), sort_keys=True
    ):
        changes.append(
            f"gaps: still {len(new.gaps)}, but they are not the same gaps"
        )

    added_flags = sorted(set(new.flags) - set(old.flags))
    removed_flags = sorted(set(old.flags) - set(new.flags))
    if added_flags:
        changes.append(f"flags: added {', '.join(added_flags)}")
    if removed_flags:
        changes.append(f"flags: cleared {', '.join(removed_flags)}")

    return ManifestDiff(changes=tuple(changes))
