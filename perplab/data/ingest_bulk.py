"""Bulk ingestion from data.binance.vision: download, verify, parse, write, record.

Spec 4.5 names the stages and their order:

    bulk zip download -> checksum verify -> parse -> normalise types -> write Parquet
    -> update manifest

This module is that pipeline. It owns the network, the filesystem and the concurrency;
every decision about what the *data* means already lives in `perplab.data.bulk_layout`
and is reused here rather than restated. Nothing below parses a CSV field, sniffs a
header, or scales a decimal.

**Checksum verification is structural, not a step that can be skipped.** The `.CHECKSUM`
sibling is fetched *before* the archive, and the digest it carries is the only thing that
can unlock parsing: there is no code path from bytes on disk to rows in Parquet that does
not pass through a comparison against it. That ordering also means a missing checksum is
detected for the price of a 300-byte request rather than a 240 MB one. Spec 4.5 calls
silently corrupted archives "a real failure mode", and a corrupt zip that still inflates
is the nasty case -- it yields plausible numbers, lands in the lake, and is
indistinguishable from good data six months later. A mismatch raises before a single row
is written.

**Resumability is a receipt per archive, written last.** An ingest of 2400 daily files
will be interrupted -- by Ctrl+C, by a laptop sleeping, by a full disk. Completion is
recorded in `<root>/_ingest/<dataset>/symbol=<SYM>/<period>.json`, published atomically
*after* the Parquet file it describes has itself been published atomically. The ordering
is the whole point:

- crash mid-download  -> only a `.zip.part` in the scratch directory; nothing in the lake;
- crash mid-write     -> only a dot-prefixed `.tmp` in the partition; no receipt;
- crash after write, before receipt -> a complete Parquet file and no receipt, so the
  archive is fetched again and the same deterministic path is atomically replaced.

A half-written partition therefore cannot read as done in any interleaving, and the
recovery action is always the same idempotent re-ingest. The receipt also records the
archive's sha256, its row count and its timestamp bounds, which is what makes a later
"is this partition the one I ingested?" question answerable without re-downloading.

The ledger lives in a `_ingest` tree *beside* the dataset directories rather than inside
them, because the manifest (spec 4.6) hashes the sorted list of files under each dataset;
a JSON receipt filed next to the Parquet would change that hash and make every ingest
look like a data change.

**One file per archive, not per flush -- the contrast with the collector.**
`ParquetBufferedWriter` emits `part-<epoch_ms>-<seq>.parquet` because a live collector
cannot hold a day in memory and cannot know when a day is finished. Bulk has neither
problem: an archive is a closed, complete unit, so it is written as exactly one Parquet
file with a deterministic name, published with one `os.replace`. Where a partition is
filled by a single archive (the date-partitioned datasets) that file is spec 4.3's
`data.parquet` exactly. Where a partition spans many archives -- daily klines into a
month, daily metrics into a year -- the file is named after its source period instead.
Rewriting a whole month on the arrival of each of its days would turn a 31-file ingest
into 496 file-writes, and would mean a crash could damage days that were already safe.
Hive globbing reads a multi-file partition identically either way.

**Failures are collected, not fatal.** One bad day out of 2400 must not abort the run, so
`ingest_archive` raises and `ingest_range` catches, records, and carries on -- reporting
everything at the end with a non-zero exit code. The one distinction worth making is
between an archive that failed and an archive that was never published: a 404 outside a
dataset's documented coverage window is an expected absence (see `PUBLISHED_COVERAGE`),
while a 404 inside it is a genuine hole that gap detection needs to hear about. Neither
is silently swallowed.

`liquidationSnapshot` is reported as unavailable by design, with a pointer to finding F2,
and produces a non-zero exit. Returning success for a request that cannot be satisfied
would let a batch script conclude it had the data.
"""

from __future__ import annotations

import contextlib
import csv
import hashlib
import io
import json
import os
import queue
import sys
import tempfile
import threading
import time
import zipfile
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field, replace as dataclass_replace
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, BinaryIO, Protocol, TextIO

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from perplab.data.bulk_layout import BASE_URL, BulkDataset, bulk_dataset, datetime_str_to_ms
from perplab.data.schemas import (
    layout_for,
    normalise_symbol,
    partition_components,
    partition_key,
)

__all__ = [
    "AddressBanned",
    "BulkIngestError",
    "ChecksumMismatch",
    "MalformedChecksum",
    "MalformedArchive",
    "PartitionConflict",
    "ArchiveNotPublished",
    "DatasetUnavailable",
    "TransientFetchError",
    "IngestStatus",
    "FileOutcome",
    "IngestReport",
    "ByteSink",
    "Fetcher",
    "HttpFetcher",
    "LocalDirectoryFetcher",
    "Progress",
    "TextProgress",
    "LEDGER_DIRNAME",
    "DEFAULT_CONCURRENCY",
    "PHASE1_DATASETS",
    "PUBLISHED_COVERAGE",
    "TYPICAL_BYTES_PER_PERIOD",
    "IngestPlan",
    "format_bytes",
    "parse_checksum_file",
    "periods_for",
    "plan_range",
    "receipt_path",
    "is_ingested",
    "fill_days_from_monthly",
    "ingest_archive",
    "ingest_range",
    "sweep_stale_downloads",
]


LEDGER_DIRNAME = "_ingest"
"""Sibling of the dataset directories under the lake root. Leading underscore so that a
`<root>/*/symbol=*` glob cannot pick it up, and so a human reading the tree can see it is
bookkeeping rather than data."""

RECEIPT_VERSION = 1
"""Bumped when the on-disk shape of a partition changes. A receipt written by an older
version is ignored rather than trusted, which forces a re-ingest instead of leaving a
partition that claims to be current but was written by different rules."""

DEFAULT_CONCURRENCY = 4
"""Downloads are network-bound, so a handful of parallel transfers is most of the win.
Kept small deliberately: data.binance.vision is a free public mirror, and a backfill that
hammers it is both rude and likely to be throttled into being slower than four."""

DEFAULT_MAX_ATTEMPTS = 5
_BACKOFF_BASE = 2.0
_DOWNLOAD_CHUNK = 1 << 20

_COMPRESSION = "zstd"
_COMPRESSION_LEVEL = 3
"""Spec 4.3, matching `ParquetBufferedWriter` -- the lake must not contain two
compression regimes, or a query's scan cost depends on which producer wrote the file."""

_ROWS_PER_ARROW_BATCH = 100_000
"""How many Python dicts are held before conversion to Arrow. This is the real memory
knob: a bookTicker day is roughly 240 MB zipped and tens of millions of rows, so the rows
can never all be resident. Arrow's columnar form costs about 8 bytes per value, so the
accumulated batches below are cheap by comparison."""

_ROWS_PER_ROW_GROUP = 1_000_000
"""Target row-group size (spec 4.3 asks for roughly 128 MB). At eight to eleven int64
columns this is 64-88 MB uncompressed, which is the right order; sizing it by row count
rather than bytes keeps the writer from having to measure its own output."""


PUBLISHED_COVERAGE: dict[str, tuple[str, str | None]] = {
    # Verified 2026-08-01 against the S3 listing; see docs/DATA_AVAILABILITY.md. `None`
    # as the end bound means "still being published as of that date", not "unbounded" --
    # the whole reason this table exists is that one of these stopped without notice.
    "klines": ("2019-12-31", None),
    "markPriceKlines": ("2019-12-23", None),
    "aggTrades": ("2019-12-31", None),
    "bookDepth": ("2023-01-01", None),
    "metrics": ("2020-09-01", None),
    "fundingRate": ("2020-01", None),
    "bookTicker": ("2023-05-16", "2024-03-30"),
}
"""Published coverage per bulk dataset, in that dataset's own period format.

Bounds are compared lexicographically, which is exact for `YYYY-MM-DD` and `YYYY-MM`.
They exist so that a request outside the window produces a *warning naming the window*
rather than a run of bare 404s that a caller could read as "the exchange had no data
then". Finding F1 is precisely that misreading waiting to happen: `bookTicker` looks like
full history in spec 4.2 and is 320 days.

These dates are duplicated from `docs/DATA_AVAILABILITY.md` and can drift from it.
Whoever re-runs `scripts/verify_bulk_availability.py` must update both; asserting one
against the other would mean parsing prose.
"""

PHASE1_DATASETS: tuple[str, ...] = (
    "klines",
    "markPriceKlines",
    "aggTrades",
    "bookTicker",
    "fundingRate",
    "metrics",
)
"""The datasets a Phase 1 backfill fetches, in the order it fetches them.

Spec 13 names Phase 1's scope as "klines, aggTrades, bookTicker, funding, metrics,
liquidations". Two departures, both deliberate:

- `liquidations` is absent because it cannot be fetched. It is not published in bulk at
  either path (finding F2), so including it here would make every default run exit
  non-zero and teach an operator to ignore the exit code -- the one signal that says
  whether the backfill worked. It is *reported* instead: `bulk_layout.BULK_DATASETS` keeps
  `liquidationSnapshot` with its caveat precisely so the CLI can print it every time
  rather than silently omitting the dataset.
- `markPriceKlines` is added. It is a kline-shaped archive of the same size and cost as
  `klines`, and `gaps.DATASET_RULES` gives it a real rule -- so leaving it out of the
  default backfill would make the default gap report open with "all bars absent" for a
  dataset nobody was told to fetch.

Cheapest first is not the order. This is publication order by usefulness: `klines` is what
the tick rules cross-check against (spec 4.5 rule 2), so fetching it first means a run
interrupted halfway still leaves a lake whose gap report can be computed.
"""

TYPICAL_BYTES_PER_PERIOD: dict[str, int] = {
    # Order-of-magnitude compressed sizes for BTCUSDT, the largest USD-M symbol, measured
    # 2026-08-01. Deliberately round: this exists to answer "is this ten megabytes or
    # eighty gigabytes" before a backfill starts, and a false precision would invite
    # someone to plan disk against it.
    "klines": 350_000,
    "markPriceKlines": 300_000,
    "aggTrades": 80_000_000,
    "bookTicker": 240_000_000,
    "fundingRate": 4_000,
    "metrics": 60_000,
    "bookDepth": 2_000_000,
}
"""Rough compressed archive size per period, for the dry-run estimate only.

An estimate rather than a `HEAD` per file because the point is to warn *before* 2400
requests, and because the number that matters is the order of magnitude: `bookTicker`
across its whole published window is roughly 77 GB, which is the difference between a
backfill and a disk-full incident at hour nine. Nothing consumes this except the plan
report, and `IngestReport.bytes_downloaded` reports what was actually transferred.

A symbol smaller than BTCUSDT will come in well under, which is the safe direction for a
warning. An unknown dataset has no entry and its estimate is reported as unknown rather
than as zero -- a fabricated zero would read as "this costs nothing".
"""


# --------------------------------------------------------------------------------------
# Failure modes
# --------------------------------------------------------------------------------------


class BulkIngestError(RuntimeError):
    """Base for every ingest failure. Nothing here is recoverable by guessing."""


class TransientFetchError(BulkIngestError):
    """A network condition worth retrying: connection error, 5xx, 429.

    Separate from the rest so the retry loop cannot accidentally retry a 404 or a
    checksum mismatch, neither of which will ever succeed on a second attempt.
    """


class AddressBanned(BulkIngestError):
    """HTTP 418: Binance has blocked this IP. Deliberately *not* retryable.

    418 and 429 arrive from the same infrastructure and were handled as one condition,
    which is right per-request and wrong per-campaign. 429 says "you asked too fast, wait";
    418 says "you ignored that, and the address is now blocked". Backing off and retrying
    a ban -- five attempts per archive, across every archive in a range -- extends it.

    It is separated here because of what else lives on this address. The block is
    IP-level, so it applies to the *collector's* REST pollers too (`data.rest_poller`
    supplies `markPrice`, `aggTrades` and open interest), and those feed a recording that
    cannot be re-downloaded later. A backfill is always re-runnable; the minutes of live
    data lost to a ban it earned are not. So the first 418 aborts the run rather than
    grinding through `max_attempts`, and says plainly what else it affects.
    """


class ArchiveNotPublished(BulkIngestError):
    """The archive (or its checksum) is absent at the documented path.

    Not the same as a failure. The gap report exists to distinguish "we could not fetch
    this" from "this was never published", and collapsing the two would make a
    discontinued dataset look like a flaky network.
    """


class MalformedChecksum(BulkIngestError):
    """The `.CHECKSUM` sibling is not a single `<sha256>  <filename>` line."""


class ChecksumMismatch(BulkIngestError):
    """The downloaded bytes do not hash to the published digest.

    Raised before anything is written. The archive is left undecoded: a zip that fails
    its checksum may still inflate, and rows recovered from it would be indistinguishable
    from good data once they are in the lake.
    """

    def __init__(self, url: str, expected: str, actual: str, size: int) -> None:
        super().__init__(
            f"checksum mismatch for {url}: published {expected}, downloaded {actual} "
            f"({size} bytes). Refusing to parse -- a corrupt archive that still inflates "
            f"produces plausible rows, which is worse than no rows."
        )
        self.url = url
        self.expected = expected
        self.actual = actual
        self.size = size


class MalformedArchive(BulkIngestError):
    """The zip's contents are not the single CSV the registry says they should be."""


class PartitionConflict(BulkIngestError):
    """The target partition already holds files written by the live collector.

    `aggTrades` and `bookTicker` have two producers (`schemas.SCHEMAS`), one lake root
    (`cli.py` passes `market_root(--root)` to both `collect` and `ingest`), and one
    partition path. What they do not share is a filename: the collector publishes
    `part-<epoch_ms>-<seq>.parquet` and bulk publishes `data.parquet`, so the `os.replace`
    that makes a re-ingest idempotent overwrites *nothing* here. Both files survive, and
    the `<root>/<dataset>/**/*.parquet` glob that `query.py`, `gaps.py` and `manifest.py`
    all use reads every overlapping row twice.

    Nothing downstream can notice. `Coverage.duplicate_rows` is computed only by
    `detect_kline_gaps`, whose `open_time` is a unique key; the tick rule measures silence
    between records and duplicates only shorten intervals, so the gap report comes back
    clean. The manifest sees a two-file partition, which is also what a healthy multi-part
    collector day looks like. The doubling is therefore silent, durable, and invisible to
    every reporting surface -- which is why this is refused rather than warned about.

    Raised before the download, so a 320-day bookTicker backfill does not spend 77 GB
    discovering it. Re-checked immediately before the publish, because the collector is
    *live*: it can flush into the partition during the minutes the transfer takes, and a
    pre-flight check alone would leave exactly that window open.
    """


class DatasetUnavailable(BulkIngestError):
    """The dataset is not published in bulk at all (finding F2)."""


class _Interrupted(Exception):
    """Internal: Ctrl+C reached a worker. Never escapes `ingest_range`."""


# --------------------------------------------------------------------------------------
# Results
# --------------------------------------------------------------------------------------


class IngestStatus(StrEnum):
    """What happened to one (symbol, dataset, period).

    `MISSING` and `UNPUBLISHED` are both 404s and are deliberately different: the first
    is a hole inside a window where data is expected and is worth investigating, the
    second is the documented edge of a dataset's history and is not.

    `CONFLICT` and `FAILED` are separated on the same principle. A `PartitionConflict` is
    not a failure to fetch -- the archive is there and the transfer would have worked; the
    ingester *declined* to publish into a partition the live collector owns, because both
    files would survive and every overlapping row would then be counted twice by the
    `**/*.parquet` glob every reader uses. Folding that into `FAILED` reads as "retry
    later", and retrying is exactly what does not help: the conflict is permanent for as
    long as the collector owns the day. The distinction exists so a caller can say
    "declined, and here is what that leaves missing" rather than "failed".
    """

    WRITTEN = "written"
    SKIPPED = "skipped"
    UNPUBLISHED = "unpublished"
    MISSING = "missing"
    CONFLICT = "conflict"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class FileOutcome:
    """The result of one archive, whether or not it produced any bytes."""

    symbol: str
    dataset: str
    """Binance's own dataset name (`fundingRate`), not the lake's (`funding`). The two
    differ for exactly one dataset, and naming the source here is what lets a failure be
    traced back to a URL."""
    period: str
    status: IngestStatus
    rows: int = 0
    archive_bytes: int = 0
    parquet: str | None = None
    """Lake-relative POSIX path of the file written, or None."""
    ts_min: int | None = None
    ts_max: int | None = None
    blank_lines: int = 0
    error: str | None = None


@dataclass(slots=True)
class IngestReport:
    """Everything one `ingest_range` call did, in a form a CLI or an API can render.

    Outcomes are sorted by period regardless of the order threads finished in, so two
    runs over the same range produce byte-identical reports (spec 1.4, determinism).
    """

    symbol: str
    dataset: str
    root: Path
    outcomes: list[FileOutcome] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    interrupted: bool = False
    periods_requested: int = 0
    elapsed_s: float = 0.0

    def count(self, status: IngestStatus) -> int:
        return sum(1 for o in self.outcomes if o.status is status)

    @property
    def rows(self) -> int:
        return sum(o.rows for o in self.outcomes)

    @property
    def bytes_downloaded(self) -> int:
        return sum(o.archive_bytes for o in self.outcomes)

    @property
    def not_attempted(self) -> int:
        return max(0, self.periods_requested - len(self.outcomes))

    @property
    def failures(self) -> list[FileOutcome]:
        return [
            o
            for o in self.outcomes
            if o.status in (IngestStatus.FAILED, IngestStatus.UNAVAILABLE)
        ]

    @property
    def conflicts(self) -> list[FileOutcome]:
        """Periods declined because the live collector owns the partition.

        Separate from `failures` because they are a different instruction to the reader --
        nothing is wrong with the archive or the transfer, and retrying changes nothing.
        Deciding whether a given conflict actually *cost* anything needs a query against
        that partition (does the collector's data cover the whole day, or start at noon?),
        which is a lake read this report deliberately does not perform.
        """
        return [o for o in self.outcomes if o.status is IngestStatus.CONFLICT]

    @property
    def ok(self) -> bool:
        """Every requested period was accounted for, and every one of them landed.

        `not_attempted` is part of the test on purpose. Outcomes are appended by worker
        threads, so a worker dying of something no `except Exception` would catch -- a
        `MemoryError`, an interpreter shutdown -- would leave its periods with no outcome
        at all. Counting them makes that shortfall a failure rather than a silently
        shorter report, which is the difference between noticing a hole and backtesting
        over one.

        **Conflicts count against `ok` even though they are not failures.** The question
        this property answers is "was the requested range satisfied", and a declined period
        was not written, so it was not. A caller that can measure the conflict -- and can
        therefore say the collector's own rows already cover the day -- is free to overrule
        this on better evidence; a caller that cannot must not read green. Last night a
        conflict-driven refusal left an eleven-hour hole precisely because the surface
        above it treated "we declined" as "we are finished".
        """
        return (
            not self.failures
            and not self.conflicts
            and not self.interrupted
            and self.not_attempted == 0
        )

    @property
    def exit_code(self) -> int:
        """Zero only if the whole requested range was satisfied.

        An interruption counts as failure. The data already written is sound and the run
        can be resumed, but a caller that treats a partial backfill as complete will
        backtest over a range it does not have.
        """
        return 0 if self.ok else 1

    def render(self) -> str:
        lines: list[str] = []
        lines.append(
            f"{self.dataset} {self.symbol}: {len(self.outcomes)}/{self.periods_requested} "
            f"periods in {_format_clock(self.elapsed_s)}"
        )
        counts = ", ".join(
            f"{status.value} {self.count(status)}"
            for status in IngestStatus
            if self.count(status)
        )
        lines.append(f"  {counts or 'nothing attempted'}")
        lines.append(
            f"  {self.rows:,} rows, {format_bytes(self.bytes_downloaded)} downloaded"
        )
        if self.interrupted:
            lines.append(f"  INTERRUPTED -- {self.not_attempted} periods not attempted")
        for warning in self.warnings:
            lines.append(f"  warning: {warning}")
        for outcome in self.failures:
            lines.append(f"  FAILED {outcome.period}: {outcome.error}")
        for outcome in self.conflicts:
            lines.append(f"  DECLINED {outcome.period}: {outcome.error}")
        missing = [o.period for o in self.outcomes if o.status is IngestStatus.MISSING]
        if missing:
            lines.append(
                f"  not published inside the expected window ({len(missing)}): "
                f"{', '.join(missing[:10])}{' ...' if len(missing) > 10 else ''}"
            )
        return "\n".join(lines)


# --------------------------------------------------------------------------------------
# Progress
# --------------------------------------------------------------------------------------


class Progress(Protocol):
    """Sink for progress events. Every method may be called from a worker thread."""

    def plan(self, dataset: str, symbol: str, periods: int) -> None: ...

    def note(self, message: str) -> None: ...

    def begin(self, dataset: str, symbol: str, period: str, url: str) -> None: ...

    def bytes_read(self, count: int, total: int | None) -> None: ...

    def end(self, outcome: FileOutcome) -> None: ...


def format_bytes(count: float) -> str:
    """Render a byte count for a human, at one decimal place.

    Public because the CLI's dry-run has the same job and a second implementation
    beside this one would eventually disagree with it about what a gigabyte is --
    which, in a warning whose entire purpose is to convey scale, is the one detail
    that must not wobble. Rounds at 1024, matching what a disk-space dialog shows.
    """
    for unit in ("B", "KB", "MB", "GB"):
        if count < 1024 or unit == "GB":
            return f"{count:.1f} {unit}" if unit != "B" else f"{int(count)} B"
        count /= 1024
    return f"{count:.1f} GB"


def _format_clock(seconds: float) -> str:
    """Render a duration in seconds as `HH:MM:SS`, for elapsed times and ETAs.

    Deliberately not `gaps.format_duration`, which renders a *gap* as `1h 02m 03s`.
    The two look like the same helper written twice and are not: one is a clock a
    reader watches tick during a six-hour backfill, the other is a magnitude a reader
    ranks gaps by. Named for the clock so the difference is visible at the call site
    rather than discovered by diffing two output formats.
    """
    if seconds < 0 or seconds != seconds:  # NaN guard; an ETA is a guess, not a promise
        return "--:--:--"
    total = int(seconds)
    return f"{total // 3600:02d}:{total // 60 % 60:02d}:{total % 60:02d}"


class TextProgress:
    """Line-oriented progress for a run that may last hours.

    Written for a scrollback buffer rather than a terminal cursor: one line per completed
    archive plus a periodic heartbeat while a large one is in flight. Redrawing a status
    bar in place looks better and is useless afterwards -- when a 2400-file backfill
    fails at hour six, the question is always "which files, and was it slowing down", and
    only a log answers that.

    Thread-safe. With four concurrent downloads the per-chunk byte counts interleave, so
    the heartbeat reports the aggregate rate and what is currently in flight rather than
    pretending to track one file.
    """

    def __init__(self, stream: TextIO | None = None, *, heartbeat_s: float = 5.0) -> None:
        self._stream = stream if stream is not None else sys.stderr
        self._heartbeat_s = heartbeat_s
        self._lock = threading.Lock()
        self._t0 = time.monotonic()
        self._last_beat = self._t0
        self._total = 0
        self._done = 0
        self._bytes = 0
        self._counts: dict[IngestStatus, int] = {s: 0 for s in IngestStatus}
        self._inflight: dict[str, int] = {}

    def _write(self, line: str) -> None:
        self._stream.write(line + "\n")
        self._stream.flush()

    def plan(self, dataset: str, symbol: str, periods: int) -> None:
        """Begin a leg. Every counter below is scoped to the leg, never to the process.

        `cmd_ingest` builds one reporter and loops the six `PHASE1_DATASETS`, so this is
        called once per dataset on the same object. Resetting `_total` and `_t0` while
        carrying `_done` forward made the two disagree about what they were counting:
        `_done` outgrew `_total` from the second dataset onward, printing `[    4/3    ]`,
        and `remaining = max(_total - _done, 0)` is then 0 for every subsequent line -- so
        the 320-period bookTicker leg reported `ETA 00:00:00` for its whole multi-hour
        duration. `_bytes` had the same shape in the other direction: run-cumulative bytes
        divided by seconds since *this* leg started overstates throughput by one to two
        orders of magnitude at each boundary. Those figures and the running tallies are the
        only instrument an operator has over an unattended overnight run, so they answer
        for one dataset at a time rather than for a mixture of two scopes.

        `_inflight` is cleared for the same reason and is safe to clear here: a leg's
        downloads are all started and joined inside its own `ingest_range`, so nothing of
        this dataset's is in flight yet, and an entry surviving an interrupted previous leg
        would otherwise inflate "N in flight" for the rest of the run.
        """
        with self._lock:
            self._total = periods
            self._done = 0
            self._bytes = 0
            self._counts = {s: 0 for s in IngestStatus}
            self._inflight.clear()
            self._t0 = time.monotonic()
            self._last_beat = self._t0
        self._write(f"ingest {dataset} {symbol}: {periods} periods")

    def note(self, message: str) -> None:
        self._write(f"  note: {message}")

    def begin(self, dataset: str, symbol: str, period: str, url: str) -> None:
        with self._lock:
            self._inflight[period] = 0

    def bytes_read(self, count: int, total: int | None) -> None:
        with self._lock:
            self._bytes += count
            now = time.monotonic()
            if now - self._last_beat < self._heartbeat_s:
                return
            self._last_beat = now
            elapsed = max(now - self._t0, 1e-9)
            line = (
                f"    ... {len(self._inflight)} in flight, "
                f"{format_bytes(self._bytes)} at "
                f"{format_bytes(self._bytes / elapsed)}/s"
            )
        self._write(line)

    def end(self, outcome: FileOutcome) -> None:
        with self._lock:
            self._inflight.pop(outcome.period, None)
            self._done += 1
            self._counts[outcome.status] += 1
            now = time.monotonic()
            elapsed = max(now - self._t0, 1e-9)
            remaining = max(self._total - self._done, 0)
            eta = (elapsed / self._done) * remaining if self._done else 0.0
            rate = self._bytes / elapsed
            counts = " ".join(
                f"{s.value[:4]} {self._counts[s]}" for s in IngestStatus if self._counts[s]
            )
            line = (
                f"[{self._done:>5}/{self._total:<5}] {outcome.dataset} {outcome.symbol} "
                f"{outcome.period}  {outcome.status.value:<11} "
                f"{outcome.rows:>10,} rows  {format_bytes(outcome.archive_bytes):>9}  "
                f"| {counts}  {format_bytes(rate)}/s  ETA {_format_clock(eta)}"
            )
            if outcome.error:
                line += f"\n              {outcome.error}"
        self._write(line)


class _NullProgress:
    """Default sink. A library call should not write to anyone's terminal."""

    def plan(self, dataset: str, symbol: str, periods: int) -> None: ...

    def note(self, message: str) -> None: ...

    def begin(self, dataset: str, symbol: str, period: str, url: str) -> None: ...

    def bytes_read(self, count: int, total: int | None) -> None: ...

    def end(self, outcome: FileOutcome) -> None: ...


# --------------------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------------------


class ByteSink(Protocol):
    """Whatever a fetcher pours bytes into. Only `write` is ever called.

    Narrower than `BinaryIO` on purpose: the sink a download actually receives is
    `_HashingSink`, which is not a file at all. Declaring the wide type here would make
    every implementation claim seek and tell support it does not have, and the first
    fetcher to rely on that claim would break the hashing.
    """

    def write(self, data: bytes, /) -> int: ...


class Fetcher(Protocol):
    """The only seam through which this module touches a network.

    Deliberately single-shot: neither method retries. Retrying a stream after bytes have
    already reached the hasher would fold two partial downloads into one digest, and the
    resulting mismatch would look like archive corruption rather than a retry bug. The
    retry loop therefore lives in `_download_and_hash`, which can reset both the temp
    file and the hasher together.
    """

    def get_text(self, url: str) -> str: ...

    def download(
        self,
        url: str,
        sink: ByteSink,
        *,
        on_chunk: Callable[[int, int | None], None] | None = None,
    ) -> int: ...


class _HashingSink:
    """Write-through wrapper that hashes on the way past.

    Hashing during the stream rather than by re-reading the finished file matters at
    240 MB per bookTicker day: a second full pass over the file is a second disk read for
    no information that was not already in flight.
    """

    __slots__ = ("_fh", "digest", "size")

    def __init__(self, fh: BinaryIO) -> None:
        self._fh = fh
        self.digest = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> int:  # noqa: D102 - BinaryIO protocol
        self.digest.update(data)
        self.size += len(data)
        return self._fh.write(data)


class HttpFetcher:
    """`httpx` fetcher for data.binance.vision.

    Retry classification mirrors `perplab.exchange.rest`: network errors, 5xx and the
    rate-limit statuses are transient, everything else is the request's own fault and
    retrying it only wastes time. 404 is singled out because for this publisher it is
    information -- see `ArchiveNotPublished`.
    """

    def __init__(
        self,
        *,
        timeout: float = 60.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "perplab/0.1"},
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> HttpFetcher:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @staticmethod
    def _classify(url: str, status: int, body: str) -> None:
        if status == 404:
            raise ArchiveNotPublished(f"404 for {url}")
        if status == 418:
            raise AddressBanned(
                f"HTTP 418 for {url}: this IP is blocked by Binance. Retrying extends the "
                f"block, so the run stops here. The block is address-wide, so the live "
                f"collector's REST pollers (markPrice, aggTrades, open interest) are "
                f"affected too until it lifts: {body[:200]}"
            )
        if status == 429 or status >= 500:
            raise TransientFetchError(f"HTTP {status} for {url}: {body[:200]}")
        raise BulkIngestError(f"HTTP {status} for {url}: {body[:200]}")

    def get_text(self, url: str) -> str:
        try:
            response = self._client.get(url)
        except httpx.HTTPError as exc:
            raise TransientFetchError(f"{type(exc).__name__} for {url}: {exc}") from exc
        if response.status_code != 200:
            self._classify(url, response.status_code, response.text)
        return response.text

    def download(
        self,
        url: str,
        sink: ByteSink,
        *,
        on_chunk: Callable[[int, int | None], None] | None = None,
    ) -> int:
        written = 0
        try:
            with self._client.stream("GET", url) as response:
                if response.status_code != 200:
                    response.read()
                    self._classify(url, response.status_code, response.text)
                header = response.headers.get("content-length")
                total = int(header) if header and header.isdigit() else None
                for chunk in response.iter_bytes(_DOWNLOAD_CHUNK):
                    sink.write(chunk)
                    written += len(chunk)
                    if on_chunk is not None:
                        on_chunk(len(chunk), total)
        except httpx.HTTPError as exc:
            raise TransientFetchError(f"{type(exc).__name__} for {url}: {exc}") from exc
        return written


class LocalDirectoryFetcher:
    """Serve archives from a local mirror of the bucket instead of the network.

    Two real uses. A backfill of several hundred gigabytes is often done once with
    `aws s3 sync` and then re-ingested several times as the parsers are corrected, and
    re-downloading each time is hours of someone else's bandwidth. And it lets the ingest
    pipeline be tested end to end -- including the checksum comparison, which is the part
    that must never be stubbed -- with no network in the loop.

    The directory mirrors the URL layout below `BASE_URL`, e.g.
    `<dir>/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-07-15.zip`.
    """

    def __init__(self, directory: Path | str, *, base_url: str = BASE_URL) -> None:
        self._dir = Path(directory)
        self._base = base_url.rstrip("/") + "/"

    def _resolve(self, url: str) -> Path:
        if not url.startswith(self._base):
            raise BulkIngestError(f"{url!r} is not below {self._base!r}")
        return self._dir.joinpath(*url[len(self._base) :].split("/"))

    def get_text(self, url: str) -> str:
        path = self._resolve(url)
        if not path.is_file():
            raise ArchiveNotPublished(f"absent from the local mirror: {path}")
        return path.read_text(encoding="utf-8")

    def download(
        self,
        url: str,
        sink: ByteSink,
        *,
        on_chunk: Callable[[int, int | None], None] | None = None,
    ) -> int:
        path = self._resolve(url)
        if not path.is_file():
            raise ArchiveNotPublished(f"absent from the local mirror: {path}")
        total = path.stat().st_size
        written = 0
        with path.open("rb") as source:
            while chunk := source.read(_DOWNLOAD_CHUNK):
                sink.write(chunk)
                written += len(chunk)
                if on_chunk is not None:
                    on_chunk(len(chunk), total)
        return written


# --------------------------------------------------------------------------------------
# Checksums
# --------------------------------------------------------------------------------------

_HEX = frozenset("0123456789abcdef")


def parse_checksum_file(text: str, expected_name: str) -> str:
    """Read Binance's `.CHECKSUM` sibling: one `"<sha256>  <filename>"` line.

    The file name is checked, not just the digest. Both are needed: the digest catches a
    corrupted transfer, and the name catches the rarer but nastier case of a checksum
    belonging to a *different* archive -- a mirror that served a stale object, or a URL
    built with the wrong period. A digest that simply mismatches would be reported as
    corruption, sending whoever reads the report looking for a network fault that is not
    there.

    A leading `*` on the name is accepted because that is how `sha256sum` marks binary
    mode; anything else about the line is refused rather than pattern-matched around.
    """
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) != 1:
        raise MalformedChecksum(
            f"expected exactly one line in the CHECKSUM for {expected_name}, "
            f"got {len(lines)}: {text[:200]!r}"
        )

    parts = lines[0].split()
    if len(parts) != 2:
        raise MalformedChecksum(
            f"expected '<sha256>  <filename>' for {expected_name}, got {lines[0][:200]!r}"
        )

    digest, name = parts[0].strip().lower(), parts[1].lstrip("*")
    if len(digest) != 64 or not set(digest) <= _HEX:
        raise MalformedChecksum(
            f"{expected_name}: {digest!r} is not a 64-character hex sha256"
        )
    if name != expected_name:
        raise MalformedChecksum(
            f"CHECKSUM names {name!r} but the archive requested was {expected_name!r}; "
            f"verifying against it would prove the wrong file intact"
        )
    return digest


# --------------------------------------------------------------------------------------
# The completion ledger
# --------------------------------------------------------------------------------------


def ledger_dir(root: Path, dataset: str, symbol: str) -> Path:
    """Receipt directory, keyed by the *source* dataset name.

    Keyed by Binance's name rather than the lake's because a receipt records that one
    archive was consumed, and `fundingRate` (monthly archives) and `funding` (the lake
    dataset) are not in one-to-one correspondence.
    """
    return Path(root) / LEDGER_DIRNAME / dataset / f"symbol={normalise_symbol(symbol)}"


def receipt_path(root: Path, dataset: str, symbol: str, period: str) -> Path:
    return ledger_dir(root, dataset, symbol) / f"{period}.json"


def _read_receipt(path: Path) -> dict[str, Any] | None:
    """Load a receipt, returning None for anything that is not a current, valid one.

    Every rejection here costs a re-download and nothing else, because ingesting an
    archive twice replaces the same deterministic path atomically. That asymmetry is why
    this leans towards "not done": treating a doubtful receipt as complete risks a
    permanent hole, treating a good one as doubtful risks a few minutes.
    """
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("version") != RECEIPT_VERSION:
        return None
    return payload


def is_ingested(root: Path, dataset: str, symbol: str, period: str) -> bool:
    """Has this archive already been ingested completely -- and is it still intact?

    True only if a current receipt exists *and* the Parquet file it names is still on
    disk *and* the file still looks like the one the receipt was written for. The
    presence half is not paranoia: the retention job in spec 4.4 deletes partitions, and
    a receipt outliving its data would make the deleted range invisible to a re-ingest --
    a gap that no gap detector could explain, because nothing recorded that the data had
    ever gone.

    The intactness half closes finding M26: this check used to accept *any* file at the
    receipted path, so a partition truncated after ingest -- a copy interrupted, a disk
    filling mid-write, an editor mishap -- read as ingested forever, and the plan stage
    skipped the one re-download that would have fixed it. The verification tiers are a
    deliberate trade-off, documented here because each tier buys different assurance at
    different cost:

    - **byte size against `parquet_bytes`** (one `stat`, effectively free) whenever the
      receipt records it. Truncation, the zero-byte file, and partial rewrites all move
      the size, and those are the corruptions that actually occur outside deliberate
      tampering.
    - **row count from the Parquet footer against `rows`** (one footer read, a seek and a
      few kilobytes) for receipts that predate `parquet_bytes` -- the fields those
      receipts already carry are what they can be checked against, and an unreadable
      footer fails the check rather than passing it.
    - **no content hash, ever, here.** The receipt's `sha256` is the *ZIP archive's*
      digest, so it cannot verify the Parquet at all, and re-hashing gigabytes per
      `plan_range` call would make the check expensive enough to turn off -- the fate the
      manifest module documents for exactly this idea. Byte-level content integrity
      belongs to the checksum verification at ingest time, which already happened.

    Every rejection costs one re-download and nothing else, so the lean stays towards
    "not done", as `_read_receipt` documents.
    """
    payload = _read_receipt(receipt_path(root, dataset, symbol, period))
    if payload is None:
        return False
    relative = payload.get("parquet")
    if not isinstance(relative, str):
        return False
    parquet = Path(root) / relative
    if not parquet.is_file():
        return False

    recorded_bytes = payload.get("parquet_bytes")
    if isinstance(recorded_bytes, int) and not isinstance(recorded_bytes, bool):
        return parquet.stat().st_size == recorded_bytes

    recorded_rows = payload.get("rows")
    if isinstance(recorded_rows, int) and not isinstance(recorded_rows, bool):
        try:
            return int(pq.read_metadata(parquet).num_rows) == recorded_rows
        except Exception:
            # A footer that cannot be read is a truncated or non-Parquet file wearing a
            # receipted name; "not ingested" forces the idempotent re-ingest that heals it.
            return False
    return True


def _atomic_write_text(path: Path, text: str) -> None:
    """Publish a small file with the writer's `.tmp` + `os.replace` discipline.

    `fsync` before the rename, unlike the collector's part-files: this file is the
    durability marker for the Parquet beside it, and a receipt that survives a power cut
    while its data does not is exactly the "reads as done but is not" state the ledger
    exists to make impossible.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.tmp"
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


# --------------------------------------------------------------------------------------
# Period enumeration
# --------------------------------------------------------------------------------------

_MS_PER_DAY = 86_400_000


def _daily_periods(start: str, end: str) -> list[str]:
    first = datetime_str_to_ms(f"{start} 00:00:00")
    last = datetime_str_to_ms(f"{end} 00:00:00")
    if last < first:
        raise ValueError(f"end {end!r} precedes start {start!r}")
    return [partition_key(ms) for ms in range(first, last + 1, _MS_PER_DAY)]


def _month_index(period: str) -> int:
    """Months since year 0, so month arithmetic never touches a calendar library."""
    if len(period) < 7 or period[4] != "-":
        raise ValueError(f"expected a YYYY-MM or YYYY-MM-DD period, got {period!r}")
    year, month = period[:4], period[5:7]
    if not year.isdigit() or not month.isdigit():
        raise ValueError(f"expected a YYYY-MM or YYYY-MM-DD period, got {period!r}")
    if not 1 <= int(month) <= 12:
        raise ValueError(f"impossible month in {period!r}")
    return int(year) * 12 + int(month) - 1


def _monthly_periods(start: str, end: str) -> list[str]:
    first, last = _month_index(start), _month_index(end)
    if last < first:
        raise ValueError(f"end {end!r} precedes start {start!r}")
    return [f"{i // 12:04d}-{i % 12 + 1:02d}" for i in range(first, last + 1)]


def periods_for(dataset: str, start: str, end: str) -> list[str]:
    """Every archive period a request covers, in the dataset's own cadence.

    Both bounds are inclusive, and a monthly dataset accepts either `YYYY-MM` or a full
    date (truncated to its month) so that a caller can pass one range to several datasets
    without knowing which of them is monthly. A daily dataset refuses a bare `YYYY-MM`:
    silently expanding it to the whole month would turn a typo into a 31-day download.
    """
    bulk = bulk_dataset(dataset)
    if bulk.cadence == "monthly":
        return _monthly_periods(start, end)
    for bound, label in ((start, "start"), (end, "end")):
        if len(bound) != 10:
            raise ValueError(
                f"{dataset} is daily; {label} must be a YYYY-MM-DD date, got {bound!r}"
            )
    return _daily_periods(start, end)


def _period_start_ms(bulk: BulkDataset, period: str) -> int:
    """Epoch ms of the first instant a period can contain, for partition resolution."""
    if bulk.cadence == "daily":
        return datetime_str_to_ms(f"{period} 00:00:00")
    return datetime_str_to_ms(f"{period}-01 00:00:00")


def _outside_coverage(bulk: BulkDataset, period: str) -> bool:
    window = PUBLISHED_COVERAGE.get(bulk.name)
    if window is None:
        return False
    first, last = window
    return period < first or (last is not None and period > last)


def _coverage_warning(bulk: BulkDataset, periods: Sequence[str]) -> str | None:
    window = PUBLISHED_COVERAGE.get(bulk.name)
    if window is None:
        return None
    outside = [p for p in periods if _outside_coverage(bulk, p)]
    if not outside:
        return None
    first, last = window
    message = (
        f"{len(outside)} of {len(periods)} requested periods ({outside[0]} .. "
        f"{outside[-1]}) fall outside {bulk.name}'s published coverage "
        f"{first} .. {last or 'current'} (verified 2026-08-01, "
        f"docs/DATA_AVAILABILITY.md). They will 404; that is the edge of the published "
        f"history, not a fetch failure and not an absence of market activity."
    )
    if bulk.caveat:
        message += f" Registry caveat: {bulk.caveat}"
    return message


# --------------------------------------------------------------------------------------
# Planning a run before committing to it
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IngestPlan:
    """What a range *would* fetch, computed without touching the network.

    Exists because this pipeline can be asked, in one plausible command, to pull seventy-
    seven gigabytes across two and a half thousand requests. An operator should learn that
    at second zero rather than at hour nine when the disk fills -- and by then the cost is
    not only the wasted transfer but a half-backfilled lake whose gap report is
    indistinguishable from a genuinely incomplete history.

    Every field is derived from the receipt ledger and the coverage table, so a plan is
    cheap and can be recomputed as often as anyone likes.
    """

    symbol: str
    dataset: str
    """Binance's archive name, matching `FileOutcome.dataset`."""
    periods: tuple[str, ...]
    already: tuple[str, ...]
    """Periods with a current receipt whose Parquet is still on disk; they would be
    skipped. Reported rather than subtracted silently, because "nothing to do" and "this
    range was never requested" look identical otherwise."""
    outside: tuple[str, ...]
    """Periods outside the dataset's published window. They will 404 by publication, not
    by failure -- see `PUBLISHED_COVERAGE`."""
    available: bool
    caveat: str | None

    @property
    def to_fetch(self) -> tuple[str, ...]:
        """Periods that would actually be downloaded.

        Excludes both the already-ingested and the outside-coverage periods. The latter
        cost one 300-byte checksum request each rather than nothing, but counting them as
        downloads would inflate the estimate with bytes that will never arrive.
        """
        skip = set(self.already) | set(self.outside)
        return tuple(p for p in self.periods if p not in skip)

    @property
    def estimated_bytes(self) -> int | None:
        """Rough compressed transfer size, or `None` when the dataset has no estimate.

        `None`, never 0. A dataset whose size nobody has measured is unknown, and printing
        a zero would answer the question this whole type exists to ask.
        """
        per_period = TYPICAL_BYTES_PER_PERIOD.get(self.dataset)
        if per_period is None:
            return None
        return per_period * len(self.to_fetch)

    def render(self) -> str:
        """One block per dataset, leading with the number the operator has to decide on.

        Rendered here rather than in the CLI, beside `IngestReport.render`, so the plan and
        the result of carrying it out are formatted by the same module and can be read
        against each other. A dry-run whose layout differs from the run it predicts is a
        dry-run people stop reading.
        """
        if not self.available:
            return (
                f"{self.dataset} {self.symbol}: UNAVAILABLE -- not published in bulk\n"
                f"  {self.caveat or 'see docs/DATA_AVAILABILITY.md'}"
            )

        estimate = self.estimated_bytes
        size = "unknown size" if estimate is None else f"~{format_bytes(estimate)}"
        lines = [
            f"{self.dataset} {self.symbol}: {len(self.to_fetch)} of "
            f"{len(self.periods)} periods to fetch, {size}"
        ]
        if self.already:
            lines.append(
                f"  {len(self.already)} already ingested (receipt + Parquet on disk); "
                f"--force would re-fetch them"
            )
        if self.outside:
            window = PUBLISHED_COVERAGE.get(self.dataset)
            bounds = (
                f"{window[0]} .. {window[1] or 'current'}" if window else "the published window"
            )
            lines.append(
                f"  {len(self.outside)} outside published coverage {bounds} "
                f"({self.outside[0]} .. {self.outside[-1]}); these will 404 by "
                f"publication, not by failure"
            )
        if self.to_fetch:
            head = ", ".join(self.to_fetch[:5])
            tail = " ..." if len(self.to_fetch) > 5 else ""
            lines.append(f"  first: {head}{tail}")
        if self.caveat:
            lines.append(f"  caveat: {self.caveat}")
        return "\n".join(lines)


def plan_range(
    root: Path | str,
    symbol: str,
    dataset: str,
    start: str,
    end: str,
    *,
    force: bool = False,
) -> IngestPlan:
    """Work out what `ingest_range` would do, without doing any of it.

    Reads the local ledger only -- no HTTP, not even a `HEAD`. Asking the server for 2400
    content lengths to warn about 2400 downloads would be most of the cost of the thing it
    is warning about, and the answer needed here is an order of magnitude rather than a
    byte count.

    `force` mirrors `ingest_range`: with it set, nothing counts as already done, so the
    plan shows the full re-fetch the flag would cause.
    """
    root = Path(root)
    symbol = normalise_symbol(symbol)
    bulk = bulk_dataset(dataset)

    if not bulk.available:
        return IngestPlan(
            symbol=symbol,
            dataset=bulk.name,
            periods=(),
            already=(),
            outside=(),
            available=False,
            caveat=bulk.caveat,
        )

    periods = tuple(periods_for(bulk.name, start, end))
    already = (
        ()
        if force
        else tuple(p for p in periods if is_ingested(root, bulk.name, symbol, p))
    )
    outside = tuple(p for p in periods if _outside_coverage(bulk, p))
    return IngestPlan(
        symbol=symbol,
        dataset=bulk.name,
        periods=periods,
        already=already,
        outside=outside,
        available=True,
        caveat=bulk.caveat,
    )


# --------------------------------------------------------------------------------------
# Parse and write one archive
# --------------------------------------------------------------------------------------


@dataclass(slots=True)
class _ArchiveStats:
    """Out-of-band counters for a streaming parse, which cannot return them."""

    rows: int = 0
    blank_lines: int = 0
    had_header: bool = False
    ts_min: int | None = None
    ts_max: int | None = None


def _iter_archive_rows(
    zip_path: Path,
    bulk: BulkDataset,
    symbol: str,
    period: str,
    time_column: str,
    stats: _ArchiveStats,
) -> Iterator[dict[str, Any]]:
    """Yield lake rows from the single CSV inside one archive.

    The header sniff happens on the real first line of this file and is not cached:
    Binance added headers around 2023 without backfilling, so the answer differs between
    two files of the same dataset (see `bulk_layout.is_header_line`). Getting it wrong in
    either direction is silent -- a dropped first data row, or a header coerced into a
    junk row that sorts to the front of the partition.

    Blank lines are tolerated but counted rather than ignored. Every archive ends with a
    trailing newline, which is a file-format artifact and not a missing row; more than
    that is worth seeing in the report, so the count is carried out rather than dropped.
    """
    parser = bulk.parser
    if parser is None:  # unreachable via ingest_archive, which checks `available` first
        raise DatasetUnavailable(f"{bulk.name} has no parser")

    expected_member = bulk.member_name(symbol, period)
    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        if names != [expected_member]:
            raise MalformedArchive(
                f"expected exactly one member {expected_member!r} in "
                f"{zip_path.name}, found {names!r}"
            )

        with archive.open(expected_member) as raw:
            handle = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            first = handle.readline()
            if not first.strip():
                raise MalformedArchive(f"{expected_member} is empty")

            stats.had_header = bulk.is_header(first)
            reader = csv.reader(handle)
            line_no = 1

            def convert(fields: list[str], line: int) -> dict[str, Any]:
                try:
                    # `metrics` is the one parser that takes the symbol: its per-row
                    # `symbol` column is dropped on write, so this call is the only place
                    # a mislabelled archive can ever be caught.
                    if bulk.name == "metrics":
                        return parser(fields, expect_symbol=symbol)
                    return parser(fields)
                except ValueError as exc:
                    # `ValueError` alone is sufficient because `money.to_scaled` and
                    # `money.decimal_to_scaled` guarantee it -- they normalise the
                    # `decimal.InvalidOperation` and `OverflowError` that the `Decimal`
                    # branch would otherwise raise, both of which are `ArithmeticError`
                    # and would escape this handler with no archive and no line number
                    # attached. That guarantee is stated in their docstrings and pinned by
                    # `tests/unit/test_money.py`; widening the catch here instead would
                    # leave the same hole open for every other caller of the seam.
                    raise MalformedArchive(
                        f"{expected_member} line {line}: "
                        f"{type(exc).__name__}: {exc}"
                    ) from None

            if not stats.had_header:
                yield _record(convert(next(csv.reader([first])), 1), time_column, stats)

            for fields in reader:
                line_no += 1
                if not fields or all(not f.strip() for f in fields):
                    stats.blank_lines += 1
                    continue
                yield _record(convert(fields, line_no), time_column, stats)


def _record(row: dict[str, Any], time_column: str, stats: _ArchiveStats) -> dict[str, Any]:
    """Update the running row count and timestamp bounds, then pass the row through.

    Tracking min and max rather than checking every row's partition is what makes the
    containment check below both exact and cheap: partitions are contiguous time
    intervals, so if the earliest and latest rows land in the same partition then so does
    everything between them, whatever order the file happened to be in.
    """
    ts = row[time_column]
    stats.rows += 1
    if stats.ts_min is None or ts < stats.ts_min:
        stats.ts_min = ts
    if stats.ts_max is None or ts > stats.ts_max:
        stats.ts_max = ts
    return row


def _output_name(bulk: BulkDataset, granularity: str, period: str) -> str:
    """`data.parquet` when one archive fills the partition, else the period's own name.

    Spec 4.3 shows `data.parquet`, and for the date-partitioned datasets a daily archive
    is exactly one partition, so that is what gets written. Klines, markPriceKlines and
    metrics partition more coarsely than they publish -- a month or a year built from
    daily files -- and there the only ways to keep a single file are to rewrite the whole
    partition on every archive, which is quadratic and puts already-safe days at risk on
    each crash, or to hold the partition open across the run, which loses resumability.
    Naming the file after its source period keeps one atomic publish per archive and
    keeps the file traceable back to the URL and checksum it came from.
    """
    one_archive_per_partition = (bulk.cadence == "daily" and granularity == "date") or (
        bulk.cadence == "monthly" and granularity == "month"
    )
    return "data.parquet" if one_archive_per_partition else f"{period}.parquet"


COLLECTOR_FILE_GLOB = "part-*.parquet"
"""How a `ParquetBufferedWriter` file is recognised from outside the collector.

Duplicated from `writer.ParquetBufferedWriter._write_partition` rather than imported,
because importing the writer here would tie the bulk path to the live collector's module
for the sake of one string. The pattern is deliberately narrow: it must match what the
collector publishes and nothing this module publishes, or the guard below would either
miss the collision it exists for or refuse the legitimate `<period>.parquet` siblings a
month partition accumulates. `writer.py` names its own working file `.part-*.parquet.tmp`,
which the leading dot keeps out of this glob exactly as it keeps it out of every reader's.
"""


def _partition_dir(
    root: Path, bulk: BulkDataset, symbol: str, period: str
) -> tuple[Path, tuple[str, ...]]:
    """Where one archive's rows belong, and the components that path is built from.

    Shared by the pre-flight conflict check and the writer so the two cannot disagree
    about which directory is at stake -- a guard that inspects a different path from the
    one that is written is worse than no guard, because it reports safety it did not
    verify.
    """
    target = bulk.target_dataset
    if target is None:  # guarded by `available` in ingest_archive
        raise DatasetUnavailable(f"{bulk.name} has no lake dataset")

    components = partition_components(target, _period_start_ms(bulk, period))
    directory = Path(root) / target / f"symbol={normalise_symbol(symbol)}"
    for component in components:
        directory = directory / component
    return directory, components


_COLLECTOR_COEXISTENT_DATASETS = frozenset({"metrics"})
"""Datasets where bulk and collector rows in one partition are complementary, not
duplicates (finding M22).

`metrics` is year-partitioned, so one hour of live open-interest collection put a
part-file in `year=2026` and the overlap guard then blocked the *entire year's* archive
backfill -- with advice ("narrow --start/--end") that is unsatisfiable when the
conflicting partition is the whole year. And the refusal protected nothing: the live
poller deliberately records the endpoint's own unsnapped timestamps precisely so that an
archive backfill *densifies* the series rather than colliding with it (see
`OpenInterestPoller` -- snapping them to the archive's grid was a defect, removed under
spec 12.1, because tied instants made results depend on file order). Two producers, two
sets of instants, one continuous series: the exact coexistence the tick-dataset guard
exists to prevent is the designed behaviour here.

`aggTrades` and `bookTicker` stay guarded: their two producers record the *same events*,
so overlap there really does read every row twice."""


def _refuse_collector_overlap(
    directory: Path, bulk: BulkDataset, symbol: str, period: str
) -> None:
    """Raise if the collector has already filed rows in this partition (spec 1.4).

    Refused rather than merged, deduplicated or overwritten. Merging would mean the bulk
    path silently rewriting rows another producer published, deduplication needs a
    compaction pass this repo does not yet have, and overwriting would discard the
    collector's `recv_ms` -- the only record of transport latency, which the archives
    cannot supply. Every one of those is a guess about which producer is right; refusing
    is the answer that leaves the operator holding both sets of facts.

    Datasets in `_COLLECTOR_COEXISTENT_DATASETS` are exempt: there the two producers
    record different instants by design and coexistence is the intended, gap-checked
    state of the partition -- see that constant for the full argument (finding M22).
    """
    if bulk.target_dataset in _COLLECTOR_COEXISTENT_DATASETS:
        return
    existing = sorted(p.name for p in directory.glob(COLLECTOR_FILE_GLOB))
    if not existing:
        return
    shown = ", ".join(existing[:3]) + (" ..." if len(existing) > 3 else "")
    raise PartitionConflict(
        f"{bulk.name} {symbol} {period}: partition {directory} already holds "
        f"{len(existing)} collector part-file(s) ({shown}). Bulk publishes "
        f"'data.parquet' here, so nothing would be overwritten and both would survive -- "
        f"every overlapping row read twice by the '**/*.parquet' glob that query, gaps "
        f"and manifest all use, with no duplicate count anywhere to reveal it. Bulk "
        f"archives are meant to fill the history *behind* the collector's start: narrow "
        f"--start/--end to end before {period}, or move the collector's files out of that "
        f"partition first."
    )


def _write_archive(
    zip_path: Path,
    root: Path,
    bulk: BulkDataset,
    symbol: str,
    period: str,
) -> tuple[Path, _ArchiveStats]:
    """Parse an archive and publish it as one Parquet file. Returns (path, stats).

    Rows are converted to Arrow every `_ROWS_PER_ARROW_BATCH` and grouped into row groups
    of roughly `_ROWS_PER_ROW_GROUP`, so a 240 MB bookTicker day never has more than a
    fraction of itself resident as Python objects. `pq.ParquetWriter` keeps that inside
    one output file, which a table-at-a-time approach could not.

    Nothing is visible to a reader until the final `os.replace`: the working file is both
    dot-prefixed and `.tmp`-suffixed, so a crash leaves something that no Parquet glob and
    no human will mistake for data.

    **The working file carries this process's pid**, for the same reason `writer.py`'s
    part-files do. Two ingests of the same period -- two clicks of a UI button, a CLI run
    beside a scheduled one -- derive an identical `name` from `_output_name`, so a shared
    `.{name}.tmp` means one process's `os.replace` fires against a file the other is still
    writing. The published Parquet then belongs to one run while the receipt written after
    it describes the other, `parquet_bytes` disagrees, and `is_ingested` returns False for
    that period *forever*: it re-downloads on every pass and never settles. A pid in the
    name makes the concurrent case merely wasteful -- two full downloads, one winner --
    instead of leaving a permanently unsettleable day behind.
    """
    target = bulk.target_dataset
    schema = bulk.target_schema
    if target is None or schema is None:  # guarded by `available` in ingest_archive
        raise DatasetUnavailable(f"{bulk.name} has no lake dataset")

    layout = layout_for(target)
    directory, expected = _partition_dir(root, bulk, symbol, period)
    directory.mkdir(parents=True, exist_ok=True)

    name = _output_name(bulk, layout.granularity, period)
    final = directory / name
    tmp = directory / f".{name}.{os.getpid()}.tmp"

    stats = _ArchiveStats()
    pending: list[pa.Table] = []
    pending_rows = 0

    try:
        with tmp.open("wb") as handle:
            with pq.ParquetWriter(
                handle,
                schema,
                compression=_COMPRESSION,
                compression_level=_COMPRESSION_LEVEL,
            ) as writer:
                batch: list[dict[str, Any]] = []
                # `closing`, not a bare `for`. `_iter_archive_rows` holds the `ZipFile`
                # open across every yield, so an exception raised by *this* loop body --
                # ENOSPC inside `write_table`, an out-of-int64 id reaching `from_pylist`,
                # a MemoryError, Ctrl+C -- leaves the generator suspended and reachable
                # from the propagating traceback, with the archive still open. On Windows
                # that makes `ingest_archive`'s `finally: temp_zip.unlink()` fail with
                # WinError 32, and the consequences are all silent: the real exception is
                # demoted to `__context__` and a cleanup error is reported in its place, a
                # quarter-gigabyte `.zip.part` is orphaned on the disk that may have just
                # filled, and a KeyboardInterrupt arrives at `run_one` as a PermissionError
                # -- an ordinary `Exception`, so it is recorded as one failed day and the
                # run carries on through the rest of the range the operator asked to stop.
                with contextlib.closing(
                    _iter_archive_rows(
                        zip_path, bulk, symbol, period, layout.time_column, stats
                    )
                ) as rows:
                    for row in rows:
                        batch.append(row)
                        if len(batch) >= _ROWS_PER_ARROW_BATCH:
                            pending.append(pa.Table.from_pylist(batch, schema=schema))
                            pending_rows += len(batch)
                            batch = []
                            if pending_rows >= _ROWS_PER_ROW_GROUP:
                                writer.write_table(pa.concat_tables(pending))
                                pending, pending_rows = [], 0
                if batch:
                    pending.append(pa.Table.from_pylist(batch, schema=schema))
                    pending_rows += len(batch)
                if pending:
                    writer.write_table(pa.concat_tables(pending))

            handle.flush()
            os.fsync(handle.fileno())

        _check_partition_containment(target, expected, stats, bulk, period)
        # Re-checked here, not only before the download: the collector is live and may
        # have flushed into this partition during the transfer, which on a bookTicker day
        # is minutes wide. This is the check that actually guards the publish; the
        # pre-flight one in `ingest_archive` exists to save the bytes.
        _refuse_collector_overlap(directory, bulk, symbol, period)
        os.replace(tmp, final)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    return final, stats


def _check_partition_containment(
    target: str,
    expected: tuple[str, ...],
    stats: _ArchiveStats,
    bulk: BulkDataset,
    period: str,
) -> None:
    """Refuse an archive whose rows do not belong to the partition its path claims.

    An archive is named for a period and is filed under the partition that period
    resolves to. If its earliest or latest row resolves elsewhere, either the archive is
    mislabelled upstream or the URL was built for the wrong period -- and writing it
    anyway would file real rows under a date no query for them will ever look at, which
    surfaces as an unexplained gap rather than an error (spec 1.4).

    Not observed firing against any real archive. It is cheap insurance against the class
    of mistake that the `metrics` symbol check catches in the other dimension.
    """
    if stats.ts_min is None or stats.ts_max is None:
        return
    for label, ts in (("earliest", stats.ts_min), ("latest", stats.ts_max)):
        actual = partition_components(target, ts)
        if actual != expected:
            raise MalformedArchive(
                f"{bulk.name} {period}: its {label} row (ts_ms={ts}, "
                f"{partition_key(ts)}) resolves to partition {actual or '<root>'}, "
                f"but the archive's period resolves to {expected or '<root>'}. "
                f"Filing it would hide the rows from every query for that date."
            )


# --------------------------------------------------------------------------------------
# One archive, end to end
# --------------------------------------------------------------------------------------


def _sleep_interruptibly(seconds: float, stop: threading.Event) -> None:
    """Back off without becoming unresponsive to Ctrl+C.

    A worker asleep for eight seconds inside `time.sleep` keeps the whole run alive that
    much longer after the user has asked it to stop, which reads as a hang.
    """
    if stop.wait(seconds):
        raise _Interrupted()


def _download_and_hash(
    fetcher: Fetcher,
    url: str,
    destination: Path,
    *,
    max_attempts: int,
    stop: threading.Event,
    progress: Progress,
) -> tuple[int, str]:
    """Stream an archive to disk, hashing as it goes. Returns (bytes, sha256 hex).

    Never held in memory: bookTicker reaches roughly 240 MB zipped per day and aggTrades
    80 MB, and four of those in flight at once would be most of a laptop's RAM for no
    benefit -- the bytes are going to a file regardless.

    Each retry truncates the file and starts a fresh hasher. Resuming a partial transfer
    would be faster and is deliberately not done: a resumed stream that silently
    misaligns produces a digest mismatch indistinguishable from real corruption, and the
    one thing this function must never do is make the checksum ambiguous.
    """
    last: Exception | None = None
    for attempt in range(max_attempts):
        if stop.is_set():
            raise _Interrupted()
        try:
            with destination.open("wb") as handle:
                sink = _HashingSink(handle)

                def on_chunk(count: int, total: int | None) -> None:
                    if stop.is_set():
                        raise _Interrupted()
                    progress.bytes_read(count, total)

                fetcher.download(url, sink, on_chunk=on_chunk)
                handle.flush()
                os.fsync(handle.fileno())
            return sink.size, sink.digest.hexdigest()
        except TransientFetchError as exc:
            last = exc
            if attempt < max_attempts - 1:
                progress.note(f"retrying {url} after {exc}")
                _sleep_interruptibly(_BACKOFF_BASE**attempt, stop)

    raise TransientFetchError(f"exhausted {max_attempts} attempts for {url}: {last}")


def _fetch_checksum(
    fetcher: Fetcher,
    url: str,
    expected_name: str,
    *,
    max_attempts: int,
    stop: threading.Event,
    progress: Progress,
) -> str:
    last: Exception | None = None
    for attempt in range(max_attempts):
        if stop.is_set():
            raise _Interrupted()
        try:
            return parse_checksum_file(fetcher.get_text(url), expected_name)
        except TransientFetchError as exc:
            last = exc
            if attempt < max_attempts - 1:
                progress.note(f"retrying {url} after {exc}")
                _sleep_interruptibly(_BACKOFF_BASE**attempt, stop)
    raise TransientFetchError(f"exhausted {max_attempts} attempts for {url}: {last}")


def fill_days_from_monthly(
    root: Path | str,
    symbol: str,
    dataset: str,
    month: str,
    days: Sequence[str],
    *,
    fetcher: Fetcher,
    progress: Progress | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    stop: threading.Event | None = None,
) -> list[FileOutcome]:
    """Fill days whose *daily* archive 404s from the *monthly* archive that contains them.

    **Finding F7 said Binance never published 56 `markPriceKlines` days. It published all
    but six of them -- monthly.** F7 probed `daily/markPriceKlines/...` for 2,413 days, got
    56 404s, and concluded the days do not exist. They do: `monthly/.../BTCUSDT-1m-2021-01
    .zip` is 200 and holds 44,640 rows, which is 31 x 1440 -- every day of January 2021 at
    full length, including the fourteen the daily endpoint refuses. A 404 means "not at this
    key", and only a probe of the other key can turn that into "not published".

    Days are written **individually and only where the lake has none**, which is what makes
    this additive rather than a rewrite. `markPriceKlines` partitions by month but names each
    file after the day it came from (`_output_name`), so January 2021 is seventeen files and
    the fourteen missing ones slot in beside them untouched. Publishing the monthly archive
    as one file instead would put 31 days of rows next to 17 days of the same rows in one
    partition, and every reader globbing the directory would double-count the overlap -- the
    `PartitionConflict` failure, self-inflicted.

    The receipt records the period it fills and, in `source_period` / `source_cadence`, the
    archive it actually came from. Without those a receipt for 2021-01-18 would carry a
    sha256 belonging to no file at that day's URL, and the next person to verify it would
    find a 404 and a hash that matches nothing. They are optional fields, added the way
    `parquet_bytes` was and for the same reason: old receipts stay valid.
    """
    root = Path(root)
    reporter: Progress = progress if progress is not None else _NullProgress()
    stop = stop if stop is not None else threading.Event()
    symbol = normalise_symbol(symbol)

    daily = bulk_dataset(dataset)
    if daily.cadence != "daily":
        raise DatasetUnavailable(
            f"{daily.name} is already {daily.cadence}; this fills daily holes from monthly"
        )
    schema = daily.target_schema
    target = daily.target_dataset
    if target is None or schema is None:
        raise DatasetUnavailable(f"{daily.name} has no lake dataset")
    monthly = dataclass_replace(daily, cadence="monthly")

    wanted = sorted(set(days))
    for day in wanted:
        if not day.startswith(f"{month}-"):
            raise ValueError(f"{day!r} is not a day of {month!r}")
    # Never overwrite: a day already on disk is not a hole, and the archive this reads is a
    # superset of the whole month, so "fill everything in it" would rewrite settled days.
    outstanding = [
        day
        for day in wanted
        if not (_partition_dir(root, daily, symbol, day)[0] / _output_name(
            daily, layout_for(target).granularity, day
        )).exists()
    ]
    if not outstanding:
        return []

    archive_url = monthly.archive_url(symbol, month)
    reporter.begin(monthly.name, symbol, month, archive_url)
    digest = _fetch_checksum(
        fetcher,
        monthly.checksum_url(symbol, month),
        f"{monthly.file_stem(symbol, month)}.zip",
        max_attempts=max_attempts,
        stop=stop,
        progress=reporter,
    )

    scratch = root / LEDGER_DIRNAME / "tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    handle_fd, handle_name = tempfile.mkstemp(
        dir=scratch, prefix=f"{monthly.file_stem(symbol, month)}-", suffix=".zip.part"
    )
    os.close(handle_fd)
    temp_zip = Path(handle_name)

    time_column = layout_for(target).time_column
    try:
        size, actual = _download_and_hash(
            fetcher, archive_url, temp_zip, max_attempts=max_attempts, stop=stop,
            progress=reporter,
        )
        if actual != digest:
            raise ChecksumMismatch(archive_url, digest, actual, size)

        # Bucketed in memory: a month of 1m klines is 44,640 rows, three orders of
        # magnitude below the tick archives `_write_archive` streams for.
        stats = _ArchiveStats()
        buckets: dict[str, list[dict[str, Any]]] = {day: [] for day in outstanding}
        with contextlib.closing(
            _iter_archive_rows(temp_zip, monthly, symbol, month, time_column, stats)
        ) as rows:
            for row in rows:
                day = _epoch_ms_to_day(int(row[time_column]))
                bucket = buckets.get(day)
                if bucket is not None:
                    bucket.append(row)
    finally:
        temp_zip.unlink(missing_ok=True)

    written: list[FileOutcome] = []
    for day in outstanding:
        rows = buckets[day]
        if not rows:
            # The monthly archive does not carry it either. Reported, never invented.
            written.append(
                FileOutcome(
                    symbol=symbol, dataset=daily.name, period=day,
                    status=IngestStatus.MISSING,
                    error=f"{archive_url} holds no rows for {day}",
                )
            )
            continue
        written.append(
            _publish_day(
                root, daily, symbol, day, rows, schema, target,
                source_period=month, digest=digest, archive_bytes=size,
                time_column=time_column,
            )
        )
    return written


def _epoch_ms_to_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _publish_day(
    root: Path,
    bulk: BulkDataset,
    symbol: str,
    day: str,
    rows: list[dict[str, Any]],
    schema: pa.Schema,
    target: str,
    *,
    source_period: str,
    digest: str,
    archive_bytes: int,
    time_column: str,
) -> FileOutcome:
    """One day's rows published atomically, with the receipt that says where they came from."""
    directory, _ = _partition_dir(root, bulk, symbol, day)
    directory.mkdir(parents=True, exist_ok=True)
    _refuse_collector_overlap(directory, bulk, symbol, day)

    rows.sort(key=lambda row: int(row[time_column]))
    name = _output_name(bulk, layout_for(target).granularity, day)
    final = directory / name
    tmp = directory / f".{name}.{os.getpid()}.tmp"
    table = pa.Table.from_pylist(rows, schema=schema)
    try:
        pq.write_table(
            table, tmp, compression=_COMPRESSION, compression_level=_COMPRESSION_LEVEL
        )
        os.replace(tmp, final)
    finally:
        tmp.unlink(missing_ok=True)

    stamps = [int(row[time_column]) for row in rows]
    _atomic_write_text(
        receipt_path(root, bulk.name, symbol, day),
        json.dumps(
            {
                "version": RECEIPT_VERSION,
                "dataset": bulk.name,
                "target_dataset": target,
                "symbol": symbol,
                "period": day,
                "sha256": digest,
                "archive_bytes": archive_bytes,
                "rows": len(rows),
                "blank_lines": 0,
                "had_header": False,
                "ts_min": min(stamps),
                "ts_max": max(stamps),
                "parquet": final.relative_to(root).as_posix(),
                "parquet_bytes": final.stat().st_size,
                # Provenance. `sha256` and `archive_bytes` describe the monthly archive
                # above, which is the only file these rows ever lived in -- the day's own
                # URL 404s, so a reader checking the hash against it would find nothing.
                "source_period": source_period,
                "source_cadence": "monthly",
                "written_ms": int(time.time() * 1000),
            },
            indent=2,
            sort_keys=True,
        ),
    )
    return FileOutcome(
        symbol=symbol,
        dataset=bulk.name,
        period=day,
        status=IngestStatus.WRITTEN,
        rows=len(rows),
        archive_bytes=archive_bytes,
        parquet=final.relative_to(root).as_posix(),
        ts_min=min(stamps),
        ts_max=max(stamps),
    )


def ingest_archive(
    root: Path | str,
    symbol: str,
    dataset: str,
    period: str,
    *,
    fetcher: Fetcher,
    force: bool = False,
    progress: Progress | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    stop: threading.Event | None = None,
) -> FileOutcome:
    """Run the full spec 4.5 pipeline for one (symbol, dataset, period).

    Raises on any failure -- checksum mismatch, malformed archive, unparseable row -- and
    writes nothing when it does. `ingest_range` is what turns a raised failure into a
    recorded one; a single-archive call is expected to be loud.

    The checksum is fetched *first*. It is three hundred bytes against an archive that
    may be a quarter of a gigabyte, so a day that was never published costs nothing to
    discover, and more importantly there is then no ordering in which parsing could
    happen before verification.
    """
    root = Path(root)
    reporter: Progress = progress if progress is not None else _NullProgress()
    stop = stop if stop is not None else threading.Event()
    symbol = normalise_symbol(symbol)

    bulk = bulk_dataset(dataset)
    if not bulk.available:
        raise DatasetUnavailable(
            f"{bulk.name} is not published in bulk. {bulk.caveat or ''}".strip()
        )

    if not force and is_ingested(root, bulk.name, symbol, period):
        return FileOutcome(
            symbol=symbol,
            dataset=bulk.name,
            period=period,
            status=IngestStatus.SKIPPED,
            parquet=_receipt_parquet(root, bulk.name, symbol, period),
        )

    # Before a single byte. `force` does not relax it: the flag means "ignore the receipt",
    # not "the collector's rows are mine to bury", and the two files could not overwrite
    # each other in any case. `is_ingested` cannot stand in for this -- it reads the
    # `_ingest` receipt ledger, which knows only what *this* pipeline wrote and is blind
    # to everything the collector publishes.
    partition, _ = _partition_dir(root, bulk, symbol, period)
    _refuse_collector_overlap(partition, bulk, symbol, period)

    archive_url = bulk.archive_url(symbol, period)
    reporter.begin(bulk.name, symbol, period, archive_url)

    digest = _fetch_checksum(
        fetcher,
        bulk.checksum_url(symbol, period),
        f"{bulk.file_stem(symbol, period)}.zip",
        max_attempts=max_attempts,
        stop=stop,
        progress=reporter,
    )

    scratch = root / LEDGER_DIRNAME / "tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    handle_fd, handle_name = tempfile.mkstemp(
        dir=scratch, prefix=f"{bulk.file_stem(symbol, period)}-", suffix=".zip.part"
    )
    os.close(handle_fd)
    temp_zip = Path(handle_name)

    try:
        size, actual = _download_and_hash(
            fetcher,
            archive_url,
            temp_zip,
            max_attempts=max_attempts,
            stop=stop,
            progress=reporter,
        )
        if actual != digest:
            raise ChecksumMismatch(archive_url, digest, actual, size)

        parquet, stats = _write_archive(temp_zip, root, bulk, symbol, period)
    finally:
        temp_zip.unlink(missing_ok=True)

    relative = parquet.relative_to(root).as_posix()
    _atomic_write_text(
        receipt_path(root, bulk.name, symbol, period),
        json.dumps(
            {
                "version": RECEIPT_VERSION,
                "dataset": bulk.name,
                "target_dataset": bulk.target_dataset,
                "symbol": symbol,
                "period": period,
                "sha256": digest,
                "archive_bytes": size,
                "rows": stats.rows,
                "blank_lines": stats.blank_lines,
                "had_header": stats.had_header,
                "ts_min": stats.ts_min,
                "ts_max": stats.ts_max,
                "parquet": relative,
                # The published file's byte size, so `is_ingested` can tell a truncated
                # partition from the one this receipt was written for (finding M26).
                # An optional field rather than a RECEIPT_VERSION bump: old receipts
                # stay valid (a bump would force a full re-download of every archive
                # ever ingested) and fall back to the footer row-count check.
                "parquet_bytes": parquet.stat().st_size,
                "written_ms": int(time.time() * 1000),
            },
            indent=2,
            sort_keys=True,
        ),
    )

    return FileOutcome(
        symbol=symbol,
        dataset=bulk.name,
        period=period,
        status=IngestStatus.WRITTEN,
        rows=stats.rows,
        archive_bytes=size,
        parquet=relative,
        ts_min=stats.ts_min,
        ts_max=stats.ts_max,
        blank_lines=stats.blank_lines,
    )


def _receipt_parquet(root: Path, dataset: str, symbol: str, period: str) -> str | None:
    payload = _read_receipt(receipt_path(root, dataset, symbol, period))
    value = payload.get("parquet") if payload else None
    return value if isinstance(value, str) else None


# --------------------------------------------------------------------------------------
# A whole range
# --------------------------------------------------------------------------------------

ManifestHook = Callable[[FileOutcome], None]
"""Called once per newly written archive, after its receipt has landed.

A hook rather than a direct import of `manifest.py` because the manifest is spec 4.6's
concern and is versioned against runs, not against ingests; wiring the two together here
would make every backfill depend on the run-metadata layer being present and agreeing.
A hook that raises is reported as a warning and does not fail the archive -- the Parquet
and its receipt are already durable, and a manifest can be recomputed from the lake.
"""


def ingest_range(
    root: Path | str,
    symbol: str,
    dataset: str,
    start: str,
    end: str,
    *,
    fetcher: Fetcher | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    force: bool = False,
    progress: Progress | None = None,
    manifest_hook: ManifestHook | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    stop: threading.Event | None = None,
) -> IngestReport:
    """Ingest every archive between `start` and `end` inclusive. Never raises for data.

    `root` is the lake root, the same directory `ParquetBufferedWriter` is given -- e.g.
    `userdata/market`.

    Interruption is cooperative: Ctrl+C sets a stop flag that in-flight downloads check
    per chunk, so workers unwind promptly instead of finishing a quarter-gigabyte
    transfer nobody is waiting for. Nothing can be corrupted by stopping at any point --
    every published file is one `os.replace` and every unfinished one is a dot-prefixed
    temporary -- so the report simply comes back marked interrupted, with a non-zero exit
    code and the untried periods counted.

    `stop` lets a caller that is not a terminal do the same thing. A `KeyboardInterrupt`
    only exists for the process that owns a console; a run launched as a background job has
    no way to deliver one -- on Windows a child started without its own process group
    cannot even be sent `CTRL_BREAK_EVENT` -- so without this parameter the only way to end
    a job was to kill the process, which strands a partial transfer and tells the operator
    nothing. Passing an `Event` here makes cancellation the same cooperative unwind Ctrl+C
    already gets, checked per megabyte inside the transfer and between periods; the
    resulting report is marked interrupted, which is the honest verdict for a range that
    was not finished.
    """
    root = Path(root)
    reporter: Progress = progress if progress is not None else _NullProgress()
    symbol = normalise_symbol(symbol)
    bulk = bulk_dataset(dataset)
    report = IngestReport(symbol=symbol, dataset=bulk.name, root=root)
    started = time.monotonic()

    if not bulk.available:
        # Reported, never skipped. An ingest run that quietly omits this dataset looks
        # exactly like one that forgot it existed, which is how finding F2 stayed
        # unnoticed in the spec.
        report.periods_requested = 1
        report.warnings.append(
            f"{bulk.name} is unavailable by design -- see docs/DATA_AVAILABILITY.md "
            f"finding F2. {bulk.caveat or ''}".strip()
        )
        report.outcomes.append(
            FileOutcome(
                symbol=symbol,
                dataset=bulk.name,
                period=f"{start}..{end}",
                status=IngestStatus.UNAVAILABLE,
                error=(
                    "not published in bulk at either the daily or the monthly path "
                    "(finding F2); the live !forceOrder@arr collector stream is the "
                    "only source"
                ),
            )
        )
        report.elapsed_s = time.monotonic() - started
        return report

    periods = periods_for(bulk.name, start, end)
    report.periods_requested = len(periods)
    if bulk.caveat:
        report.warnings.append(f"{bulk.name}: {bulk.caveat}")
    coverage = _coverage_warning(bulk, periods)
    if coverage:
        report.warnings.append(coverage)

    reporter.plan(bulk.name, symbol, len(periods))
    for warning in report.warnings:
        reporter.note(warning)

    owned_fetcher = fetcher is None
    active: Fetcher = fetcher if fetcher is not None else HttpFetcher()

    pending: queue.SimpleQueue[str] = queue.SimpleQueue()
    for period in periods:
        pending.put(period)

    # The caller's event when there is one, so an external cancel and a Ctrl+C converge on
    # the same flag and the same unwind path rather than on two mechanisms that would need
    # testing separately. An already-set event means "do not start", which is what a cancel
    # racing the launch should do.
    stop = stop if stop is not None else threading.Event()
    lock = threading.Lock()

    def run_one(period: str) -> FileOutcome:
        try:
            outcome = ingest_archive(
                root,
                symbol,
                bulk.name,
                period,
                fetcher=active,
                force=force,
                progress=reporter,
                max_attempts=max_attempts,
                stop=stop,
            )
        except ArchiveNotPublished as exc:
            status = (
                IngestStatus.UNPUBLISHED
                if _outside_coverage(bulk, period)
                else IngestStatus.MISSING
            )
            return FileOutcome(
                symbol=symbol,
                dataset=bulk.name,
                period=period,
                status=status,
                error=str(exc),
            )
        except PartitionConflict as exc:
            # **Declined, not failed** (see `IngestStatus.CONFLICT`). Caught above the
            # generic handler so the two never merge: a conflict is a standing property of
            # a partition the collector owns, and reporting it as a failure invites a retry
            # that cannot succeed while telling the reader nothing about what is actually
            # missing from the day.
            return FileOutcome(
                symbol=symbol,
                dataset=bulk.name,
                period=period,
                status=IngestStatus.CONFLICT,
                error=str(exc),
            )
        except AddressBanned as exc:
            # **Ends the run, unlike every other error here.** The generic handler below
            # is built on "one bad day must not end the run", which holds for a corrupt
            # archive or a dropped connection because the next period is unaffected. A
            # ban is a property of the address, not of the period: every remaining request
            # would meet it, each one prolonging the block that is simultaneously starving
            # the live collector's pollers. Stopping is the only action that helps.
            stop.set()
            return FileOutcome(
                symbol=symbol,
                dataset=bulk.name,
                period=period,
                status=IngestStatus.FAILED,
                error=f"{type(exc).__name__}: {exc}",
            )
        except _Interrupted:
            raise
        except Exception as exc:  # noqa: BLE001 - one bad day must not end the run
            return FileOutcome(
                symbol=symbol,
                dataset=bulk.name,
                period=period,
                status=IngestStatus.FAILED,
                error=f"{type(exc).__name__}: {exc}",
            )

        if manifest_hook is not None and outcome.status is IngestStatus.WRITTEN:
            try:
                manifest_hook(outcome)
            except Exception as exc:  # noqa: BLE001 - the data is already durable
                with lock:
                    report.warnings.append(
                        f"manifest hook failed for {period}: {type(exc).__name__}: {exc}"
                    )
        return outcome

    def worker() -> None:
        while not stop.is_set():
            try:
                period = pending.get_nowait()
            except queue.Empty:
                return
            try:
                outcome = run_one(period)
            except _Interrupted:
                return
            with lock:
                report.outcomes.append(outcome)
            reporter.end(outcome)

    threads: list[threading.Thread] = []
    try:
        if concurrency <= 1 or len(periods) <= 1:
            # Run inline so that Ctrl+C lands exactly where the work is, and so a
            # single-period call has no thread to reason about at all.
            worker()
        else:
            threads = [
                threading.Thread(target=worker, name=f"ingest-{i}", daemon=True)
                for i in range(min(concurrency, len(periods)))
            ]
            for thread in threads:
                thread.start()
            # Joined in short slices rather than one blocking join so that Ctrl+C is
            # delivered to this thread promptly; a bare join swallows it until the
            # longest download finishes, which on a bookTicker day is minutes of
            # apparent hang after the user has asked to stop.
            while True:
                alive = [t for t in threads if t.is_alive()]
                if not alive:
                    break
                alive[0].join(timeout=0.2)
    except KeyboardInterrupt:
        stop.set()
        report.interrupted = True
        # Only this call's own threads, so a second ingest running in the same process
        # is neither waited on nor interfered with.
        for thread in threads:
            thread.join(timeout=30.0)
        reporter.note("interrupted; already-written partitions are complete and durable")
    finally:
        if owned_fetcher and isinstance(active, HttpFetcher):
            active.close()

    report.outcomes.sort(key=lambda o: o.period)
    report.elapsed_s = time.monotonic() - started

    empty = [o.period for o in report.outcomes if o.status is IngestStatus.WRITTEN and not o.rows]
    if empty:
        report.warnings.append(
            f"{len(empty)} archive(s) contained no data rows ({', '.join(empty[:5])}"
            f"{' ...' if len(empty) > 5 else ''}); written as empty partitions rather "
            f"than dropped, so the distinction between 'published but empty' and 'never "
            f"published' survives into the gap report"
        )
    return report


def sweep_stale_downloads(root: Path | str, *, older_than_s: float = 86_400.0) -> int:
    """Delete abandoned `.zip.part` scratch files. Returns how many went.

    `ingest_archive` removes its own temporary in a `finally`, so this only matters after
    a hard kill or a power cut. The age threshold is what makes it safe to run while
    another ingest is in progress: a file being written right now is seconds old, not a
    day, so a concurrent backfill cannot have its download deleted underneath it.
    """
    scratch = Path(root) / LEDGER_DIRNAME / "tmp"
    if not scratch.is_dir():
        return 0
    cutoff = time.time() - older_than_s
    removed = 0
    for path in scratch.glob("*.zip.part"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError:
            # A file that vanished under us was already someone else's problem to clean.
            continue
    return removed
