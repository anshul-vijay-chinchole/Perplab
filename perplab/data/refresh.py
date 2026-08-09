"""Incremental top-up of the bulk datasets, safe to trigger from a button.

`ingest_bulk` answers "fetch this explicit range". This module answers the question a
person actually has -- *"bring my data up to date"* -- and it exists because the naive
translation between the two is wrong in four separate ways, each of which has already
cost this lake something.

**The end of the range is not today, and it is not the same for every dataset.** Binance
publishes a daily archive after the day closes, and the lag differs per dataset: measured
on 2026-08-05, `aggTrades` for 08-04 was published while `klines` for 08-04 was not. A
fixed `today - 1` therefore asks for files that do not exist yet, and `ingest_range`
classifies a 404 inside a dataset's published window as `MISSING` -- which `docs/INGESTION.md`
documents as *"a real hole -- investigate"*. Clicking a button every day and being told
about two holes that are not holes is how an operator learns to ignore the one that is.
So the frontier is *probed*, per dataset, and everything past it is simply not requested.

**The start of the range cannot come from the data.** `max(ts_ms)` looks like "how far we
have got" and is not: on this lake it reads through *today* for `aggTrades`, because the
collector's own rows are in the same glob, while three days have no bulk archive at all;
and it hides 56 interior holes in `markPriceKlines` that no amount of appending will fill.
Completeness is a per-period property, and the only thing that knows it is the receipt
ledger -- `is_ingested`, which checks the receipt version *and* that the Parquet is still
on disk at the recorded size. So the plan is computed over the dataset's whole published
history every time, which costs about a second and is the only answer that is true.

**The last few days belong to the collector, and declining them quietly is how holes are
born.** `aggTrades` has two producers writing into one partition directory under two
filenames, so publishing bulk into a day the collector owns leaves both files in place and
every overlapping trade is read twice by the `**/*.parquet` glob that every reader uses --
silently, durably, and invisibly to every report. `ingest_archive` refuses, correctly. But
a refusal that is merely reported leaves the operator to notice: on 2026-08-02 the
collector started at 11:07 UTC, the archive covering the whole day was declined, and the
first eleven hours went missing until a gap check happened to run. So this module does not
stop at declining. For every declined day it *measures* what the refusal costs -- reading
that one partition and comparing its span against the day's -- and reports the shortfall in
milliseconds. A conflict that costs nothing is a normal state; a conflict that costs eleven
hours is the thing you needed to be told.

**It runs beside something irreplaceable.** The collector records `depth20` and
`bookTicker`, which cannot be re-downloaded at any price -- bulk `bookTicker` ended in
March 2024 and L2 depth was never published at all. Everything this module fetches can be
fetched again tomorrow. That asymmetry decides every trade-off here: the concurrency is
lower than the CLI's, the run refuses to start while the collector is mid-reconnect, and
an address-level ban aborts rather than retrying, because the ban applies to the
collector's REST pollers too.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from perplab.data.bulk_layout import bulk_dataset
from perplab.data.ingest_bulk import (
    COLLECTOR_FILE_GLOB,
    PUBLISHED_COVERAGE,
    ArchiveNotPublished,
    Fetcher,
    HttpFetcher,
    IngestPlan,
    plan_range,
)
from perplab.data.schemas import normalise_symbol, partition_key

__all__ = [
    "BARREN_NAME",
    "REFRESH_KINDS",
    "REFRESH_CONCURRENCY",
    "MIN_FREE_BYTES",
    "MAX_UNCONFIRMED_ARCHIVES",
    "MAX_UNCONFIRMED_BYTES",
    "CollectorBusy",
    "ConflictCost",
    "DatasetRefresh",
    "InsufficientDisk",
    "RefreshLocked",
    "RefreshPlan",
    "barren_path",
    "collector_owned_periods",
    "collector_state",
    "discover_frontier",
    "measure_conflict_cost",
    "plan_refresh",
    "read_barren",
    "record_barren",
    "refresh_lock",
    "require_collector_idle",
    "require_disk_headroom",
]

# --------------------------------------------------------------------------------------
# What each button maintains
# --------------------------------------------------------------------------------------

REFRESH_KINDS: dict[str, tuple[str, ...]] = {
    "candles": ("klines", "markPriceKlines", "metrics", "fundingRate"),
    "trades": ("aggTrades",),
}
"""The datasets behind each button, by Binance archive name.

Two groups rather than one, because the costs differ by three orders of magnitude: a day
of `klines` is ~100 KB and a day of `aggTrades` is 7-40 MB. Bundling them would make the
cheap, frequently-needed top-up wait behind the expensive one every time.

`metrics` and `fundingRate` ride with the candles deliberately. They are not what anyone
means by "candles", but they are the two datasets that went stale unnoticed -- open
interest stopped on 2026-07-31 with nothing to top it up, and funding publishes monthly so
it goes a month stale in silence. A button that maintained only what its label named would
have left exactly the gap that motivated building it. `metrics` is also the one dataset
here that the collector coexists with rather than conflicts over, so it is always safe.

Nothing else is refreshable from bulk: `bookTicker` stopped being published in March 2024
and `depth20` never was. Their coverage grows only while the collector runs, which is why
the status card reports them and no button offers to fetch them.
"""

REFRESH_CONCURRENCY = 2
"""Parallel downloads for a refresh. Deliberately below `DEFAULT_CONCURRENCY`.

The CLI's four is chosen for an operator watching a 2400-file backfill who can see a
problem and press Ctrl+C. This runs unattended, beside a live collector, and six workers
have already been observed exhausting DNS resolution badly enough to knock that collector
off its websockets for two minutes -- two minutes of order book that cannot be recovered,
spent to save seconds on a download that could be repeated any time.

It buys almost nothing anyway: a routine top-up is one to three archives, and `ingest_range`
already runs inline without threads when there is a single period.
"""

MIN_FREE_BYTES = 20 * 1024**3
"""Disk that must remain after the estimated transfer. Matches `cli.cmd_preflight`.

The floor exists for the collector, not for the ingest. An ingest that hits `ENOSPC` fails
one archive and says so; the collector's flush fails silently into a memory buffer and then
starts losing depth once the buffer caps. The same 20 GB is roughly a fortnight of
collector throughput.
"""

MAX_UNCONFIRMED_ARCHIVES = 14
MAX_UNCONFIRMED_BYTES = 5 * 1024**3
"""Above either, a refresh needs explicit confirmation rather than a click.

A top-up is a handful of archives. A lake with nothing in it plans the entire published
history -- around 190 GB of `aggTrades` across 2400 requests -- from the same button. Two
weeks is comfortably above any routine top-up and far below anything that should start
without someone reading a number first. Full history is a CLI operation on purpose.
"""

FRONTIER_LOOKBACK_DAYS = 5
"""How far back to walk looking for the newest published archive.

Bounded so a genuine publication outage is reported as one stalled dataset rather than as
a growing list of missing days. Each probe is a 300-byte checksum request.
"""


# --------------------------------------------------------------------------------------
# Failure modes the caller must be able to distinguish
# --------------------------------------------------------------------------------------


class RefreshLocked(RuntimeError):
    """Another refresh holds the lake lock. Carries who and since when."""


class CollectorBusy(RuntimeError):
    """The collector is mid-reconnect or its heartbeat has stopped.

    Refused rather than queued. The window where the collector is fighting to re-establish
    its sockets is precisely the window where competing for DNS and bandwidth costs
    unrecoverable data, and a refresh is never urgent enough to spend that.
    """


class InsufficientDisk(RuntimeError):
    """The estimated transfer would leave less than `MIN_FREE_BYTES` free."""


# --------------------------------------------------------------------------------------
# Publication frontier
# --------------------------------------------------------------------------------------


def _utc_today(now_ms: int | None = None) -> str:
    """Today's UTC date as a partition key.

    UTC, never local. `partition_key` is integer arithmetic over epoch milliseconds, so it
    cannot drift into the host's timezone -- which on a UTC+5:30 machine is a different
    calendar day for eighteen and a half hours out of every twenty-four, and would ask for
    tomorrow's archive most of the working day.
    """
    return partition_key(int(time.time() * 1000) if now_ms is None else now_ms)


def discover_frontier(
    fetcher: Fetcher,
    dataset: str,
    symbol: str,
    *,
    now_ms: int | None = None,
    lookback: int = FRONTIER_LOOKBACK_DAYS,
) -> tuple[str | None, str | None]:
    """The newest published period for one dataset. Returns `(period, note)`.

    Probed rather than assumed, because the lag is per-dataset and changes: on 2026-08-05
    `aggTrades` for 08-04 was published and `klines` for 08-04 was not. Assuming a shared
    frontier makes one of them wrong every day, and being wrong here means reporting a
    normal publication lag as a hole in the data.

    Only the `.CHECKSUM` sibling is fetched -- 300 bytes against the archive's megabytes --
    so a probe costs nothing worth optimising.

    `(None, note)` means nothing in the lookback window is published. That is reported as
    its own condition, never as N missing days: a stalled publisher is one fact about the
    upstream, and rendering it as a list of holes would bury it.

    Monthly datasets are exempt: their period is a month, the current month is never
    published until it ends, and walking back through months would ask about last year.
    """
    bulk = bulk_dataset(dataset)
    if bulk.target_dataset is None:
        return None, f"{bulk.name} has no lake dataset"

    if bulk.cadence == "monthly":
        # **The archive's cadence, not the lake's partitioning.** `klines` archives are
        # daily while the lake stores them in month partitions, so asking `layout_for`
        # here called a daily dataset monthly and planned its range to the end of last
        # month -- quietly refusing to fetch the very days a top-up exists to fetch.
        # `bulk.cadence` is the property that decides what a "period" means to the
        # publisher, which is the question a frontier probe is asking.
        return None, None

    today = _utc_today(now_ms)
    today_ms = int(time.time() * 1000) if now_ms is None else now_ms
    day = 86_400_000
    for back in range(1, lookback + 1):
        period = partition_key(today_ms - back * day)
        try:
            fetcher.get_text(bulk.checksum_url(symbol, period))
        except ArchiveNotPublished:
            continue
        except Exception:
            # A probe that fails for any other reason is not evidence of absence. Treat
            # the frontier as unknown rather than inventing one; the caller degrades to
            # "nothing to do", which is safe.
            return None, f"{bulk.name}: could not probe the publication frontier"
        return period, None
    return None, (
        f"{bulk.name}: nothing published in the last {lookback} days (newest checked "
        f"{partition_key(today_ms - day)}, today is {today} UTC). This is upstream, not "
        f"a hole in the lake."
    )


# --------------------------------------------------------------------------------------
# What a declined day actually costs
# --------------------------------------------------------------------------------------


MATERIAL_SHORTFALL_MS = 60_000
"""Edge shortfall below which a declined day is treated as fully covered.

Not a fudge factor -- a limit on what the available data can distinguish. A day's last
trade is never at exactly 23:59:59.999; on this lake they land around 23:59:59.98, so a
strict comparison against the day's final millisecond reports every complete day as
missing a few milliseconds, and a measure that flags everything flags nothing.

One minute is the principled cut because the finest independent evidence in the lake is
the 1-minute kline: below one bar there is no dataset that can tell "no trade happened
here" apart from "a trade is missing here", so claiming a shortfall would be asserting
something unverifiable. Above it, a kline with volume proves trading occurred and the
absence is real -- which is exactly the cross-check `gaps.detect_tick_gaps` performs.

The leading edge is where this matters and it is never within the tolerance: a collector
that started at 11:07 leaves eleven hours, not milliseconds.
"""


@dataclass(frozen=True, slots=True)
class ConflictCost:
    """What is missing from a day the ingester declined to publish into.

    `missing_ms is None` means the partition could not be read, which is *not* the same as
    zero and must never be rendered as "fine": an unreadable partition is a reason to look,
    not a reason to relax.
    """

    period: str
    dataset: str
    collector_rows: int | None
    first_ms: int | None
    last_ms: int | None
    missing_ms: int | None
    note: str

    @property
    def is_material(self) -> bool:
        """True when the refusal demonstrably left data out, or cannot be shown not to."""
        return self.missing_ms is None or self.missing_ms > 0


def measure_conflict_cost(
    root: Path | str, dataset: str, symbol: str, period: str
) -> ConflictCost:
    """Read one partition and say how much of its day the collector's rows do not cover.

    Scoped to a single partition directory rather than the dataset glob: this runs while a
    job is in progress, and a query over three billion rows to answer a question about one
    day would be the slowest part of the whole refresh by orders of magnitude.

    The number this produces is the sentence that was missing on 2026-08-02 -- *"the
    partition holds 11:07Z onward, the day begins at 00:00Z, so eleven hours of it exist in
    the archive and not in your lake"*. Reporting the refusal without it is what let that
    day sit incomplete.
    """
    import duckdb

    root = Path(root)
    symbol = normalise_symbol(symbol)
    bulk = bulk_dataset(dataset)
    target = bulk.target_dataset or bulk.name
    partition = root / target / f"symbol={symbol}" / f"date={period}"

    if not partition.is_dir():
        return ConflictCost(
            period=period,
            dataset=bulk.name,
            collector_rows=None,
            first_ms=None,
            last_ms=None,
            missing_ms=None,
            note="the partition does not exist, yet publishing into it was declined",
        )

    glob = (partition / "*.parquet").as_posix()
    connection = duckdb.connect(":memory:")
    try:
        rows, lo, hi = connection.execute(
            f"SELECT count(*), min(ts_ms), max(ts_ms) FROM read_parquet('{glob}')"
        ).fetchone()
    except Exception as exc:  # noqa: BLE001 - unreadable is a finding, not a crash
        return ConflictCost(
            period=period,
            dataset=bulk.name,
            collector_rows=None,
            first_ms=None,
            last_ms=None,
            missing_ms=None,
            note=f"the partition could not be read ({type(exc).__name__}), so what the "
            f"refusal costs is unknown",
        )
    finally:
        connection.close()

    if not rows or lo is None or hi is None:
        return ConflictCost(
            period=period,
            dataset=bulk.name,
            collector_rows=0,
            first_ms=None,
            last_ms=None,
            missing_ms=None,
            note="the partition holds no rows, yet publishing into it was declined",
        )

    day_start = _period_start_ms(period)
    day_end = day_start + 86_400_000
    # Only the ends are measurable this way. A hole in the middle of the day would not
    # show up here, which is what the gap detector is for -- said plainly rather than
    # letting a zero read as "this day is complete".
    missing = max(0, lo - day_start) + max(0, (day_end - 1) - hi)
    if missing < MATERIAL_SHORTFALL_MS:
        note = (
            f"the collector's rows span {_iso(lo)} to {_iso(hi)}, covering the day to "
            f"within {_duration(missing)}, so declining the archive cost nothing "
            f"measurable at its edges (an interior hole would still be a gap-report "
            f"finding)"
        )
        missing = 0
    else:
        note = (
            f"the partition holds {_iso(lo)} to {_iso(hi)}; the day runs {_iso(day_start)} "
            f"to {_iso(day_end - 1)}. {_duration(missing)} of it exists in the bulk "
            f"archive and not in your lake."
        )
    return ConflictCost(
        period=period,
        dataset=bulk.name,
        collector_rows=int(rows),
        first_ms=int(lo),
        last_ms=int(hi),
        missing_ms=int(missing),
        note=note,
    )


def _last_day_of_previous_month(now_ms: int) -> str:
    """The newest date that belongs to a month Binance has finished publishing."""
    from datetime import datetime, timezone

    now = datetime.fromtimestamp(now_ms / 1000, timezone.utc)
    first_of_this_month = now.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    last_of_previous = first_of_this_month.timestamp() * 1000 - 86_400_000
    return partition_key(int(last_of_previous))


def _period_start_ms(period: str) -> int:
    from datetime import datetime, timezone

    return int(
        datetime.strptime(period, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )


def _iso(ms: int) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%SZ")


def _duration(ms: int) -> str:
    seconds, _ = divmod(int(ms), 1000)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


# --------------------------------------------------------------------------------------
# The plan
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DatasetRefresh:
    """One dataset's share of a refresh."""

    dataset: str
    plan: IngestPlan | None
    start: str | None
    end: str | None
    note: str | None

    @property
    def to_fetch(self) -> tuple[str, ...]:
        return () if self.plan is None else self.plan.to_fetch

    @property
    def estimated_bytes(self) -> int | None:
        return None if self.plan is None else self.plan.estimated_bytes


@dataclass(slots=True)
class RefreshPlan:
    """Everything a refresh would fetch, and what it deliberately would not."""

    kind: str
    symbol: str
    datasets: list[DatasetRefresh] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def archives(self) -> int:
        return sum(len(d.to_fetch) for d in self.datasets)

    @property
    def estimated_bytes(self) -> int | None:
        """Total transfer estimate, or `None` if any contributing dataset has none.

        `None` propagates rather than being treated as zero. A dataset whose size nobody
        has measured makes the total unknown, and a confirmation dialogue that showed a
        confident number built from a missing one would be worse than showing none.
        """
        parts = [d.estimated_bytes for d in self.datasets if d.to_fetch]
        if not parts:
            return 0
        if any(p is None for p in parts):
            return None
        return sum(p for p in parts if p is not None)

    @property
    def needs_confirmation(self) -> bool:
        estimate = self.estimated_bytes
        if self.archives > MAX_UNCONFIRMED_ARCHIVES:
            return True
        if estimate is None:
            return self.archives > 0
        return estimate > MAX_UNCONFIRMED_BYTES

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "symbol": self.symbol,
            "archives": self.archives,
            "estimated_bytes": self.estimated_bytes,
            "needs_confirmation": self.needs_confirmation,
            "notes": list(self.notes),
            "datasets": [
                {
                    "dataset": d.dataset,
                    "start": d.start,
                    "end": d.end,
                    "archives": len(d.to_fetch),
                    "periods": list(d.to_fetch[:20]),
                    "estimated_bytes": d.estimated_bytes,
                    "note": d.note,
                }
                for d in self.datasets
            ],
        }


def plan_refresh(
    root: Path | str,
    symbol: str,
    kind: str,
    *,
    fetcher: Fetcher | None = None,
    now_ms: int | None = None,
) -> RefreshPlan:
    """Work out what "bring `kind` up to date" means right now.

    The range runs from the dataset's published floor -- not from `max(ts_ms)` -- to the
    probed frontier. Starting at the floor every time is what makes interior holes fixable:
    `plan_range` subtracts everything the receipt ledger says is already done, so the cost
    is a ledger scan (about a second across 2400 periods) and the benefit is that a day
    missed two years ago is still in the plan.
    """
    root = Path(root)
    symbol = normalise_symbol(symbol)
    if kind not in REFRESH_KINDS:
        raise ValueError(
            f"unknown refresh kind {kind!r}; expected one of {sorted(REFRESH_KINDS)}"
        )

    owned = fetcher is None
    active: Fetcher = fetcher if fetcher is not None else HttpFetcher()
    plan = RefreshPlan(kind=kind, symbol=symbol)
    try:
        for dataset in REFRESH_KINDS[kind]:
            floor = PUBLISHED_COVERAGE.get(dataset, (None, None))[0]
            if floor is None:
                plan.datasets.append(
                    DatasetRefresh(dataset, None, None, None, "no published coverage")
                )
                continue

            frontier, note = discover_frontier(
                active, dataset, symbol, now_ms=now_ms
            )
            if frontier is None:
                if bulk_dataset(dataset).cadence == "monthly":
                    # **The last day of the previous month, not yesterday.** A monthly
                    # archive is published once the month closes, so asking up to yesterday
                    # includes the current month and produces a period that cannot exist
                    # yet -- reported as `MISSING`, which reads as a hole in the data
                    # rather than as the calendar working normally. `fundingRate` is the
                    # only dataset here that is monthly, and it would have raised a false
                    # alarm on every click for all but one day of each month.
                    end = _last_day_of_previous_month(
                        int(time.time() * 1000) if now_ms is None else now_ms
                    )
                    plan.datasets.append(
                        _dataset_plan(root, symbol, dataset, floor, end, note)
                    )
                    continue
                if note:
                    plan.notes.append(note)
                plan.datasets.append(
                    DatasetRefresh(dataset, None, None, None, note)
                )
                continue

            plan.datasets.append(
                _dataset_plan(root, symbol, dataset, floor, frontier, note)
            )
    finally:
        if owned and hasattr(active, "close"):
            active.close()  # type: ignore[attr-defined]
    return plan


def _dataset_plan(
    root: Path, symbol: str, dataset: str, start: str, end: str, note: str | None
) -> DatasetRefresh:
    try:
        plan = plan_range(root, symbol, dataset, start, end)
    except Exception as exc:  # noqa: BLE001 - one dataset must not sink the plan
        return DatasetRefresh(
            dataset, None, start, end, f"could not be planned: {type(exc).__name__}: {exc}"
        )

    barren = read_barren(root, dataset, symbol)
    if not barren:
        return DatasetRefresh(dataset, plan, start, end, note)

    remaining = tuple(p for p in plan.to_fetch if p not in barren)
    dropped = len(plan.to_fetch) - len(remaining)
    if dropped:
        extra = (
            f"{dropped} period(s) skipped as known-unfetchable upstream (recorded after a "
            f"previous attempt; delete {BARREN_NAME} under _ingest to re-probe)"
        )
        note = f"{note}. {extra}" if note else extra
    trimmed = IngestPlan(
        symbol=plan.symbol,
        dataset=plan.dataset,
        periods=remaining,
        already=plan.already,
        outside=plan.outside,
        available=plan.available,
        caveat=plan.caveat,
    )
    return DatasetRefresh(dataset, trimmed, start, end, note)


# --------------------------------------------------------------------------------------
# Periods that will never arrive
# --------------------------------------------------------------------------------------

BARREN_NAME = "unfetchable.json"
"""Periods a previous attempt proved cannot be ingested, so they are not re-attempted.

Not every absent period is a transient one. Two classes are permanent and both are already
documented findings on this lake: 6 `markPriceKlines` days published at no cadence (F7, as
corrected), and 563 `metrics` days whose rows the parser refuses because a value does not
survive `to_scaled` (F8). Together they are 569 archives that fail identically every time.

**A daily 404 is not on its own grounds for this record.** F7 originally counted 56
`markPriceKlines` days here; fifty of them were published monthly all along, and recording
them as barren would have taught every future top-up to stop looking for data that exists.
`fill_days_from_monthly` is what recovers that case. What belongs here is a period proved
absent at *every* cadence the dataset is published at, or one that fails deterministically
on parse -- not one endpoint's silence.

Without this record a top-up re-downloads all of them every time it runs -- minutes of
transfer and 619 red lines, none of which anyone can act on, in front of the one line that
might matter. That is how a report becomes noise, and a report nobody reads is how the
eleven-hour hole of 2026-08-02 survived.

Deliberately *not* the receipt ledger. A receipt means "this period is in the lake";
`is_ingested` would then have to distinguish "done" from "cannot be done", and every
consumer of the ledger would inherit that distinction. This is a separate file, holding a
separate claim, that only the refresh planner reads.

The record keeps `first_seen_ms` and the reason so it can be audited, and re-probing is one
file deletion away -- because "never" is a strong claim to persist, and upstream can always
publish something it previously did not.
"""


def barren_path(root: Path | str, dataset: str, symbol: str) -> Path:
    bulk = bulk_dataset(dataset)
    return (
        Path(root)
        / "_ingest"
        / bulk.name
        / f"symbol={normalise_symbol(symbol)}"
        / BARREN_NAME
    )


def read_barren(root: Path | str, dataset: str, symbol: str) -> dict[str, Any]:
    try:
        loaded = json.loads(barren_path(root, dataset, symbol).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def record_barren(
    root: Path | str, dataset: str, symbol: str, periods: dict[str, str]
) -> None:
    """Remember that `periods` (period -> reason) could not be fetched.

    Merges rather than replaces, and never overwrites an existing `first_seen_ms`: the age
    of the claim is the evidence for trusting it.
    """
    if not periods:
        return
    path = barren_path(root, dataset, symbol)
    existing = read_barren(root, dataset, symbol)
    now = int(time.time() * 1000)
    for period, reason in periods.items():
        entry = existing.get(period)
        if isinstance(entry, dict):
            entry["last_seen_ms"] = now
            entry["reason"] = reason
        else:
            existing[period] = {
                "first_seen_ms": now,
                "last_seen_ms": now,
                "reason": reason,
            }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(existing, indent=1, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


# --------------------------------------------------------------------------------------
# Preflight
# --------------------------------------------------------------------------------------


def collector_state(root: Path | str) -> dict[str, Any] | None:
    """The live collector's self-report, or `None` when there is no state file.

    Absence is genuinely ambiguous and must be treated as such by callers: the file is
    removed on a *clean* shutdown, so "no file" means either "not running" or "was stopped
    tidily". It never means "running".
    """
    path = Path(root) / "collector_state.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def require_collector_idle(
    root: Path | str, *, stale_after_ms: int = 60_000, now_ms: int | None = None
) -> None:
    """Refuse the refresh if the collector is running but not healthy.

    Three states, and only the middle one blocks. No state file: nothing to protect, go
    ahead. A fresh heartbeat: the collector is coping, and a two-worker download beside it
    is what the concurrency cap is sized for. A *stale* heartbeat while the file still
    exists: the collector is alive and stuck -- mid-reconnect, or failing to flush -- and
    that is the one moment when competing for the network costs data nobody can get back.
    """
    state = collector_state(root)
    if state is None:
        return
    beat = state.get("last_heartbeat_ms")
    if not isinstance(beat, int):
        return
    now = int(time.time() * 1000) if now_ms is None else now_ms
    age = now - beat
    if age > stale_after_ms:
        raise CollectorBusy(
            f"the collector's last heartbeat is {age / 1000:.0f}s old (pid "
            f"{state.get('pid')}), so it is running but not keeping up -- most likely "
            f"reconnecting. A refresh started now would compete for the network it needs, "
            f"and its depth and book data cannot be re-downloaded later. Try again once "
            f"it recovers."
        )


def require_disk_headroom(root: Path | str, plan: RefreshPlan) -> None:
    """Refuse if the transfer would leave less than `MIN_FREE_BYTES`.

    The estimate is coarse -- `TYPICAL_BYTES_PER_PERIOD` is right to an order of magnitude,
    not to a byte -- so this is a floor with margin, not an accounting exercise. An unknown
    estimate is treated as unknown: the check still runs against free space alone, because
    refusing to start on a nearly-full disk is right whether or not the size is known.
    """
    free = shutil.disk_usage(Path(root)).free
    estimate = plan.estimated_bytes or 0
    if free - estimate < MIN_FREE_BYTES:
        raise InsufficientDisk(
            f"{free / 1024**3:.1f} GB free and the transfer is estimated at "
            f"{estimate / 1024**3:.1f} GB, which would leave less than "
            f"{MIN_FREE_BYTES / 1024**3:.0f} GB. That headroom is reserved for the "
            f"collector: an ingest that runs out of disk fails one archive and says so, "
            f"while the collector's flush fails into a memory buffer and then starts "
            f"losing depth."
        )


# --------------------------------------------------------------------------------------
# The lock
# --------------------------------------------------------------------------------------

LOCK_NAME = "refresh.lock"
LOCK_STALE_MS = 180_000
"""How old a lock's heartbeat may be before it is treated as abandoned.

Matches `store.runs.STALE_HEARTBEAT_MS`. Liveness is judged by the heartbeat alone rather
than by probing the pid: a pid check is unreliable across users and process lifetimes on
Windows, and a lock that releases on a *wrong* liveness answer is worse than one that
holds slightly too long.
"""


class _LockHandle:
    def __init__(self, path: Path, payload: dict[str, Any]) -> None:
        self._path = path
        self._payload = payload

    def heartbeat(self) -> None:
        """Refresh the lock's clock. Called on the job's own timer, not on progress."""
        self._payload["heartbeat_ms"] = int(time.time() * 1000)
        try:
            self._path.write_text(json.dumps(self._payload), encoding="utf-8")
        except OSError:
            # A failed heartbeat must not kill a running ingest. The lock ages out and
            # another refresh may eventually start; duplicate work is wasteful, and the
            # partition guards are what prevent it from being harmful.
            pass

    def release(self) -> None:
        try:
            self._path.unlink()
        except OSError:
            pass


def refresh_lock(root: Path | str, *, kind: str, symbol: str) -> _LockHandle:
    """Take the lake-wide refresh lock, or raise `RefreshLocked`.

    **Lake-wide, not per dataset.** Two refreshes of different datasets do not corrupt each
    other's partitions, but they share one network, one DNS resolver and one address whose
    rate limit is enforced against the machine -- and the thing being protected from all
    three is the collector. One at a time is also what makes the trailing-edge reasoning
    tractable: two concurrent runs could each see a different publication frontier.

    Cross-process, because the CLI and the API server are different processes and both can
    ingest. `O_EXCL` is the primitive; a lock whose heartbeat has aged past `LOCK_STALE_MS`
    is taken over, which is what recovers from a killed job.
    """
    root = Path(root)
    directory = root / "_ingest"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / LOCK_NAME
    now = int(time.time() * 1000)
    payload = {
        "pid": os.getpid(),
        "kind": kind,
        "symbol": symbol,
        "started_ms": now,
        "heartbeat_ms": now,
    }

    try:
        handle = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        held = _read_lock(path)
        age = now - int(held.get("heartbeat_ms", 0) or 0)
        if age <= LOCK_STALE_MS:
            raise RefreshLocked(
                f"a {held.get('kind', 'refresh')} of {held.get('symbol', '?')} started "
                f"{(now - int(held.get('started_ms', now) or now)) / 1000:.0f}s ago "
                f"(pid {held.get('pid')}) still holds the lake. Only one refresh runs at "
                f"a time, so the two cannot race for the same archives or crowd the "
                f"collector's network."
            ) from None
        # Abandoned: the previous holder stopped checking in. Taking it over is safe --
        # every published file is one atomic replace and every receipt is written last,
        # so the worst case is repeating work that was already done.
        path.write_text(json.dumps(payload), encoding="utf-8")
        return _LockHandle(path, payload)

    with os.fdopen(handle, "w", encoding="utf-8") as stream:
        json.dump(payload, stream)
    return _LockHandle(path, payload)


def _read_lock(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def collector_owned_periods(
    root: Path | str, dataset: str, symbol: str, periods: tuple[str, ...]
) -> tuple[str, ...]:
    """Which of `periods` the live collector already owns files in.

    Lets a plan be *described* honestly before anything is fetched -- "three of these will
    be declined, and here is what that leaves out" -- instead of discovering it one
    `PartitionConflict` at a time while the operator watches a progress bar.
    """
    root = Path(root)
    symbol = normalise_symbol(symbol)
    bulk = bulk_dataset(dataset)
    target = bulk.target_dataset
    if target is None:
        return ()
    owned: list[str] = []
    for period in periods:
        partition = root / target / f"symbol={symbol}" / f"date={period}"
        if partition.is_dir() and any(partition.glob(COLLECTOR_FILE_GLOB)):
            owned.append(period)
    return tuple(owned)
