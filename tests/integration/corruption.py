"""The deliberately-corrupted-sample drill (spec 13, Phase 1 exit criterion).

Phase 1 exits on two claims. One is a stopwatch reading. The other is that the gap report
is *accurate on a deliberately corrupted sample*, and accuracy there is two-sided: a gap
the report misses ships a hole into a backtest, and a gap the report invents trains an
operator to stop reading it, after which the next real hole ships anyway. So every check
below compares the report against the injected damage in both directions and fails on
either -- a missed gap and a phantom gap are equally disqualifying.

**Six kinds of damage, chosen because they fail differently, not to make six of them.**

- a whole day's partition removed -- the coarsest loss, and the one a partition-level
  coverage check (the manifest's) would catch. Everything below is invisible to it.
- a contiguous block removed from the middle of a day -- the partition is present and its
  file is valid, so only a row-level rule sees this.
- exactly one bar removed -- the minimum-size gap, and the boundary the interior rule's
  `> step` comparison turns on. One off-by-one and this is silently clean.
- one funding settlement removed -- a different rule with a different threshold, read from
  the data rather than assumed (R17).
- a zero-volume bar left intact -- the named trap in spec 4.5. This one must produce
  *nothing*, and it is the only injection whose success is measured by silence.
- a Parquet file truncated -- corruption rather than absence. Included because
  `writer.py`'s whole atomicity argument rests on a claim about what a truncated file
  does, and a claim of that weight should be exercised rather than believed.

**The real lake is never written to.** Every injection operates on its own copy, and the
drill fingerprints the source before and after by the same `(path, size, mtime_ns)` rule
the manifest uses (spec 4.6), so "we left it alone" is a checked assertion rather than a
promise about code nobody re-read.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from perplab.data.gaps import Gap, GapKind, detect_gaps, format_duration, format_ms
from perplab.data.manifest import FileRecord, fingerprint, lake_relative_path
from perplab.data.schemas import layout_for, normalise_symbol, partition_components

__all__ = [
    "MS_PER_MINUTE",
    "MS_PER_DAY",
    "DRILL_DATASETS",
    "ExpectedGap",
    "Injection",
    "DrillPlan",
    "DrillOutcome",
    "DrillReport",
    "CORRUPTIONS",
    "copy_range",
    "source_fingerprint",
    "run_drill",
]

MS_PER_MINUTE = 60_000
MS_PER_DAY = 86_400_000
_COMPRESSION = "zstd"
_COMPRESSION_LEVEL = 3
"""Matching `writer.py` and `ingest_bulk.py`. A rewritten partition that came back under a
different codec would change the file's size and therefore the manifest fingerprint for a
reason that has nothing to do with the rows in it."""

DRILL_DATASETS: tuple[str, ...] = ("klines", "funding")
"""The two datasets the drill damages, and therefore the two it asks the report about.

Narrowed deliberately. `detect_gaps` over every registered dataset reports the
collector-only ones as wholly absent on a bulk-only lake, which is correct and is noise
here: a drill that has to subtract a fixed set of expected-absent gaps before comparing is
a drill whose comparison can be quietly wrong. These two carry the three rules that a bulk
backfill can actually violate.
"""


# --------------------------------------------------------------------------------------
# What a gap is, for comparison purposes
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExpectedGap:
    """A gap reduced to the four things the drill asserts on.

    Deliberately *not* the whole `Gap`. `detail` is prose and `explanation` is always
    `None` for a bulk dataset, so comparing them would make the drill fail on a reworded
    message -- and a test that fails on wording is a test people edit until it passes.
    What must be exact is which dataset, which rule, and the two boundaries: the duration
    an operator reads first is `end_ms - start_ms`, so getting the boundaries right *is*
    getting the duration right.
    """

    dataset: str
    kind: GapKind
    start_ms: int
    end_ms: int

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms

    @property
    def sort_key(self) -> tuple[int, str, str, int]:
        """Report order -- `detect_gaps`'s own (start, dataset, end), with the rule between.

        Spelled out rather than taken from `order=True` on the dataclass, which would sort
        by `dataset` first and, worse, would compare two `GapKind` members directly the
        first time an injection produced two rules' gaps in one dataset. `Enum` defines no
        `<`, so that comparison is a `TypeError` -- a harness crash in place of a report,
        arriving only for the mixed case nobody wrote a fixture for yet.
        """
        return (self.start_ms, self.dataset, self.kind.value, self.end_ms)

    @classmethod
    def of(cls, gap: Gap) -> ExpectedGap:
        return cls(
            dataset=gap.dataset, kind=gap.kind, start_ms=gap.start_ms, end_ms=gap.end_ms
        )

    def render(self) -> str:
        return (
            f"{self.dataset}/{self.kind.value} {format_ms(self.start_ms)} .. "
            f"{format_ms(self.end_ms)} ({format_duration(self.duration_ms)})"
        )


def _order(gap: ExpectedGap) -> tuple[int, str, str, int]:
    """Sort key, as a module function so every `sorted()` here uses the same one."""
    return gap.sort_key


@dataclass(frozen=True, slots=True)
class Injection:
    """One act of damage and the exact report it must produce.

    `expected` being empty is a meaningful value, not a default nobody filled in: the
    zero-volume injection asserts that a specific change to the data produces *no* gap,
    which is the false-positive half of the criterion and the half a careless drill omits.
    """

    name: str
    what: str
    expected: tuple[ExpectedGap, ...] = ()
    expect_error: str | None = None
    """Substring required in the exception message, for damage that should stop the report
    rather than appear in it. `None` means the report must be produced."""


@dataclass(frozen=True, slots=True)
class DrillPlan:
    """Which lake, which symbol, which range, and which minute each injection targets.

    The targets are fields rather than recomputed inside each corruption so that the
    fixture, the injection and the assertion all read the same numbers. A drill that
    derives "the middle day" twice is a drill that can damage one day and assert about
    another.
    """

    symbol: str
    start_ms: int
    end_ms: int
    datasets: tuple[str, ...] = DRILL_DATASETS
    step_ms: int = MS_PER_MINUTE

    @property
    def days(self) -> tuple[int, ...]:
        first = self.start_ms - self.start_ms % MS_PER_DAY
        last = (self.end_ms - 1) - (self.end_ms - 1) % MS_PER_DAY
        return tuple(range(first, last + 1, MS_PER_DAY))

    @property
    def whole_day_ms(self) -> int:
        """The day removed wholesale. Interior, so the gap has a bar either side of it and
        the leading/trailing branches of the kline rule are not what is being tested."""
        return self.days[len(self.days) // 2]

    @property
    def block_start_ms(self) -> int:
        """Start of the mid-day hole: 10:00 on the first day, well away from midnight."""
        return self.days[0] + 600 * MS_PER_MINUTE

    @property
    def block_bars(self) -> int:
        return 17

    @property
    def single_bar_ms(self) -> int:
        """The one-bar hole, on a different day from the block so neither can mask the
        other if a future change makes the injections cumulative."""
        return self.days[-1] + 1_000 * MS_PER_MINUTE

    @property
    def zero_volume_target_ms(self) -> int:
        return self.days[1] + 300 * MS_PER_MINUTE

    @property
    def truncate_day_ms(self) -> int:
        return self.days[len(self.days) // 2]


# --------------------------------------------------------------------------------------
# Copying a range out of a lake, without touching it
# --------------------------------------------------------------------------------------


def _expected_partitions(dataset: str, start_ms: int, end_ms: int) -> tuple[tuple[str, ...], ...]:
    """Hive components below `symbol=` that can hold rows for `[start_ms, end_ms)`.

    Asked of `schemas.partition_components` per UTC day rather than derived from directory
    names, for the reason `manifest.py` gives about its own copy of this walk: the layout
    is one decision, and a reader that rebuilds the path itself keeps looking in the old
    place after the layout moves. Duplicated here rather than imported because the
    manifest's version is private and this file is a test harness, not a second consumer
    of the lake.
    """
    seen: dict[tuple[str, ...], None] = {}
    day = start_ms - start_ms % MS_PER_DAY
    last = (end_ms - 1) - (end_ms - 1) % MS_PER_DAY
    while day <= last:
        seen[partition_components(dataset, day)] = None
        day += MS_PER_DAY
    return tuple(seen)


def _published_files(directory: Path) -> list[Path]:
    """Parquet a reader can see: no dot-prefixed names, which are writes still in flight."""
    if not directory.is_dir():
        return []
    return sorted(
        p
        for p in directory.iterdir()
        if p.is_file() and p.suffix == ".parquet" and not p.name.startswith(".")
    )


def copy_range(
    source_market: Path,
    destination_market: Path,
    symbol: str,
    datasets: Sequence[str],
    start_ms: int,
    end_ms: int,
) -> int:
    """Copy exactly the partitions covering a range into a scratch lake. Returns the file
    count.

    Partition-pruned rather than a `copytree` of the dataset. Six and a half years of
    BTCUSDT klines is 2400 files, and copying all of them once per injection would make
    the drill slow enough to be run rarely, which for a correctness harness is the same as
    not having one.
    """
    symbol = normalise_symbol(symbol)
    copied = 0
    for dataset in datasets:
        source_base = Path(source_market) / dataset / f"symbol={symbol}"
        target_base = Path(destination_market) / dataset / f"symbol={symbol}"
        for components in _expected_partitions(dataset, start_ms, end_ms):
            source_dir = source_base.joinpath(*components)
            files = _published_files(source_dir)
            if not files:
                continue
            target_dir = target_base.joinpath(*components)
            target_dir.mkdir(parents=True, exist_ok=True)
            for path in files:
                shutil.copy2(path, target_dir / path.name)
                copied += 1
    return copied


def source_fingerprint(
    source_market: Path, symbol: str, datasets: Sequence[str], start_ms: int, end_ms: int
) -> str:
    """`(path, size, mtime_ns)` hash of the source range, by spec 4.6's own rule.

    Taken before and after the drill. The drill only ever reads the source, so this is
    checking a property of the code rather than of the disk -- which is exactly when a
    check is worth having, because "it only reads" is an assertion about every line of a
    file somebody will later edit.
    """
    symbol = normalise_symbol(symbol)
    userdata = Path(source_market).parent
    records: list[FileRecord] = []
    for dataset in datasets:
        base = Path(source_market) / dataset / f"symbol={symbol}"
        for components in _expected_partitions(dataset, start_ms, end_ms):
            for path in _published_files(base.joinpath(*components)):
                stat = path.stat()
                records.append(
                    FileRecord(
                        path=lake_relative_path(userdata, path),
                        size=stat.st_size,
                        mtime_ns=stat.st_mtime_ns,
                    )
                )
    return fingerprint(records)


# --------------------------------------------------------------------------------------
# Locating and rewriting one partition file
# --------------------------------------------------------------------------------------


def _dataset_files(lake: Path, dataset: str, symbol: str) -> list[Path]:
    base = lake / dataset / f"symbol={normalise_symbol(symbol)}"
    return sorted(p for p in base.rglob("*.parquet") if not p.name.startswith("."))


def _file_holding(lake: Path, dataset: str, symbol: str, ts_ms: int) -> Path:
    """The single published file whose rows bracket `ts_ms`.

    Refuses an ambiguous answer rather than taking the first match. Two files covering one
    instant means the partition was written twice -- which is a real condition the ingest
    path refuses (`PartitionConflict`) and which would make an injection damage half the
    rows it meant to, producing a gap report that disagrees with the drill's expectation
    for a reason that has nothing to do with the detector.
    """
    column = layout_for(dataset).time_column
    matches: list[Path] = []
    for path in _dataset_files(lake, dataset, symbol):
        stamps = pq.read_table(path, columns=[column]).column(column)
        if not len(stamps):
            continue
        lo = min(stamps.to_pylist())
        hi = max(stamps.to_pylist())
        if lo <= ts_ms <= hi:
            matches.append(path)
    if len(matches) != 1:
        raise AssertionError(
            f"expected exactly one {dataset} file covering {format_ms(ts_ms)} for "
            f"{symbol}, found {[p.name for p in matches]}"
        )
    return matches[0]


def _rewrite(path: Path, table: pa.Table) -> None:
    """Replace a partition file's contents atomically, as every writer in this repo does.

    `.tmp` + `os.replace` even in a throwaway scratch lake. A harness that publishes a
    final path directly would be the one place in the project where a reader can observe a
    half-written file, and the drill's own flakiness would then be indistinguishable from
    a detector bug.
    """
    tmp = path.parent / f".{path.name}.tmp"
    pq.write_table(
        table, tmp, compression=_COMPRESSION, compression_level=_COMPRESSION_LEVEL
    )
    os.replace(tmp, path)


def _drop_where(path: Path, column: str, doomed: Callable[[int], bool]) -> int:
    """Rewrite a file without the rows whose timestamp satisfies `doomed`. Returns the
    count removed."""
    table = pq.read_table(path)
    stamps = table.column(column).to_pylist()
    keep = [not doomed(int(ts)) for ts in stamps]
    removed = len(keep) - sum(keep)
    if not removed:
        raise AssertionError(
            f"{path.name}: nothing matched the injection predicate on {column}; the "
            f"drill would then assert about damage it never did"
        )
    _rewrite(path, table.filter(pa.array(keep, pa.bool_())))
    return removed


# --------------------------------------------------------------------------------------
# The injections
# --------------------------------------------------------------------------------------


def _delete_whole_day(lake: Path, plan: DrillPlan) -> Injection:
    day = plan.whole_day_ms
    path = _file_holding(lake, "klines", plan.symbol, day + MS_PER_MINUTE)
    path.unlink()
    return Injection(
        name="missing-day",
        what=f"deleted the klines file for {format_ms(day)[:10]} ({path.name})",
        expected=(
            ExpectedGap("klines", GapKind.MISSING_BARS, day, day + MS_PER_DAY),
        ),
    )


def _delete_mid_day_block(lake: Path, plan: DrillPlan) -> Injection:
    start = plan.block_start_ms
    end = start + plan.block_bars * plan.step_ms
    path = _file_holding(lake, "klines", plan.symbol, start)
    removed = _drop_where(
        path, "open_time", lambda ts: start <= ts < end
    )
    return Injection(
        name="mid-day-hole",
        what=f"removed {removed} consecutive bars from inside {path.name}",
        expected=(ExpectedGap("klines", GapKind.MISSING_BARS, start, end),),
    )


def _delete_single_bar(lake: Path, plan: DrillPlan) -> Injection:
    target = plan.single_bar_ms
    path = _file_holding(lake, "klines", plan.symbol, target)
    _drop_where(path, "open_time", lambda ts: ts == target)
    return Injection(
        name="single-bar",
        what=f"removed exactly one bar, {format_ms(target)}, from {path.name}",
        expected=(
            ExpectedGap("klines", GapKind.MISSING_BARS, target, target + plan.step_ms),
        ),
    )


def _remove_funding_settlement(lake: Path, plan: DrillPlan) -> Injection:
    path = _file_holding(lake, "funding", plan.symbol, plan.start_ms + MS_PER_DAY)
    table = pq.read_table(path)
    stamps = [int(t) for t in table.column("calc_time").to_pylist()]
    inside = sorted(t for t in stamps if plan.start_ms <= t < plan.end_ms)
    if len(inside) < 3:
        raise AssertionError(
            f"funding for {plan.symbol} has only {len(inside)} settlement(s) in range; "
            f"the drill needs one with a neighbour on each side"
        )
    target = inside[1]
    before, after = inside[0], inside[2]
    _drop_where(path, "calc_time", lambda ts: ts == target)
    return Injection(
        name="missed-settlement",
        what=f"removed the funding settlement at {format_ms(target)}",
        expected=(
            ExpectedGap("funding", GapKind.MISSED_SETTLEMENT, before, after),
        ),
    )


def _zero_a_bar(lake: Path, plan: DrillPlan) -> Injection:
    """Set one bar's volume to zero and leave the bar itself in place.

    The trap spec 4.5 names by hand: Binance publishes zero-volume klines through an
    illiquid minute rather than omitting them, so a presence test that consults `volume`
    reports a quiet market as an outage. This injection must produce *nothing*, and it is
    the only one whose whole assertion is that the report stayed silent.
    """
    target = plan.zero_volume_target_ms
    path = _file_holding(lake, "klines", plan.symbol, target)
    table = pq.read_table(path)
    stamps = [int(t) for t in table.column("open_time").to_pylist()]
    if target not in stamps:
        raise AssertionError(f"{path.name} has no bar opening at {format_ms(target)}")
    index = stamps.index(target)

    columns = {name: table.column(name).to_pylist() for name in table.schema.names}
    for name in (
        "volume",
        "quote_volume",
        "count",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    ):
        columns[name][index] = 0
    _rewrite(path, pa.table(columns, schema=table.schema))
    return Injection(
        name="zero-volume-bar",
        what=(
            f"zeroed the volume of the bar at {format_ms(target)}, leaving the bar "
            f"present -- must NOT be reported"
        ),
        expected=(),
    )


def _truncate_file(lake: Path, plan: DrillPlan) -> Injection:
    """Chop a third off the end of one Parquet file, footer and all.

    `writer.py` justifies its atomic publish with the claim that DuckDB reads a truncated
    file as *short* rather than broken -- a silent loss. Measured on duckdb 1.x that is not
    what happens: the footer's magic bytes are gone, so the scan raises and the whole
    dataset becomes unqueryable. The conclusion is unchanged and the reasoning is not, so
    the behaviour is pinned here rather than assumed: loud is the answer this drill
    requires, and a future release that made it quiet again would have to break this test
    to do it.
    """
    day = plan.truncate_day_ms
    path = _file_holding(lake, "klines", plan.symbol, day + MS_PER_MINUTE)
    data = path.read_bytes()
    if len(data) < 64:
        raise AssertionError(f"{path.name} is too small to truncate meaningfully")
    tmp = path.parent / f".{path.name}.tmp"
    tmp.write_bytes(data[: len(data) * 2 // 3])
    os.replace(tmp, path)
    return Injection(
        name="truncated-file",
        what=f"truncated {path.name} to two thirds of its bytes ({len(data)} -> {len(data) * 2 // 3})",
        expect_error="magic bytes",
    )


def _pristine(lake: Path, plan: DrillPlan) -> Injection:
    """The control. Nothing is damaged and the report must be empty.

    First in the list because every other outcome is meaningless without it: a drill whose
    source range already had a gap would report that gap as a phantom against every
    injection, and the resulting six failures would all point at the detector.
    """
    return Injection(
        name="control",
        what="pristine copy, nothing damaged",
        expected=(),
    )


CORRUPTIONS: tuple[Callable[[Path, DrillPlan], Injection], ...] = (
    _pristine,
    _delete_whole_day,
    _delete_mid_day_block,
    _delete_single_bar,
    _remove_funding_settlement,
    _zero_a_bar,
    _truncate_file,
)


# --------------------------------------------------------------------------------------
# Running the drill
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DrillOutcome:
    injection: Injection
    observed: tuple[ExpectedGap, ...]
    error: str | None

    @property
    def missed(self) -> tuple[ExpectedGap, ...]:
        return tuple(
            sorted(set(self.injection.expected) - set(self.observed), key=_order)
        )

    @property
    def phantom(self) -> tuple[ExpectedGap, ...]:
        return tuple(
            sorted(set(self.observed) - set(self.injection.expected), key=_order)
        )

    @property
    def ok(self) -> bool:
        if self.injection.expect_error is not None:
            return self.error is not None and self.injection.expect_error in self.error
        return self.error is None and not self.missed and not self.phantom

    def render(self) -> str:
        lines = [
            f"  [{'PASS' if self.ok else 'FAIL'}] {self.injection.name}: "
            f"{self.injection.what}"
        ]
        if self.injection.expect_error is not None:
            lines.append(
                f"        expected a loud failure containing "
                f"{self.injection.expect_error!r}; got "
                f"{self.error if self.error else 'a report, with no error at all'}"
            )
            return "\n".join(lines)
        if self.error is not None:
            lines.append(f"        report could not be produced: {self.error}")
            return "\n".join(lines)
        lines.append(
            f"        expected {len(self.injection.expected)} gap(s), reported "
            f"{len(self.observed)}"
        )
        for gap in self.observed:
            mark = "phantom" if gap in self.phantom else "matched"
            lines.append(f"          {mark:<8} {gap.render()}")
        for gap in self.missed:
            lines.append(f"          MISSED   {gap.render()}")
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class DrillReport:
    plan: DrillPlan
    source: Path
    files_copied: int
    fingerprint_before: str
    fingerprint_after: str
    outcomes: tuple[DrillOutcome, ...]

    @property
    def source_untouched(self) -> bool:
        return self.fingerprint_before == self.fingerprint_after

    @property
    def ok(self) -> bool:
        return self.source_untouched and all(o.ok for o in self.outcomes)

    def render(self) -> str:
        lines = [
            f"Corruption drill -- {self.plan.symbol}  "
            f"{format_ms(self.plan.start_ms)} .. {format_ms(self.plan.end_ms)}",
            f"  source {self.source}  ({self.files_copied} file(s) per copy, "
            f"datasets {', '.join(self.plan.datasets)})",
            "",
        ]
        lines.extend(outcome.render() for outcome in self.outcomes)
        lines.append("")
        lines.append(
            f"  source fingerprint {self.fingerprint_before[:12]} -> "
            f"{self.fingerprint_after[:12]}  "
            f"{'UNCHANGED' if self.source_untouched else 'THE REAL LAKE WAS MODIFIED'}"
        )
        passed = sum(1 for o in self.outcomes if o.ok)
        lines.append(
            f"  {passed}/{len(self.outcomes)} injections behaved exactly as specified"
        )
        return "\n".join(lines)


def run_drill(
    source_market: Path,
    workspace: Path,
    plan: DrillPlan,
    *,
    corruptions: Sequence[Callable[[Path, DrillPlan], Injection]] = CORRUPTIONS,
) -> DrillReport:
    """Copy, damage, detect, compare -- once per injection, each on its own lake.

    One scratch lake per injection rather than one cumulative lake. Damage that
    accumulates would let a later injection's expectation be satisfied by an earlier
    injection's hole, and the drill would still pass with a rule switched off.
    """
    source_market = Path(source_market)
    workspace = Path(workspace)
    before = source_fingerprint(
        source_market, plan.symbol, plan.datasets, plan.start_ms, plan.end_ms
    )

    copied = 0
    outcomes: list[DrillOutcome] = []
    for corruption in corruptions:
        scratch = workspace / corruption.__name__.lstrip("_") / "market"
        shutil.rmtree(scratch.parent, ignore_errors=True)
        copied = copy_range(
            source_market,
            scratch,
            plan.symbol,
            plan.datasets,
            plan.start_ms,
            plan.end_ms,
        )
        if not copied:
            raise AssertionError(
                f"no files copied from {source_market} for {plan.symbol} over "
                f"{format_ms(plan.start_ms)} .. {format_ms(plan.end_ms)}; the drill would "
                f"otherwise 'pass' against an empty lake"
            )
        injection = corruption(scratch, plan)

        error: str | None = None
        observed: tuple[ExpectedGap, ...] = ()
        try:
            report = detect_gaps(
                scratch,
                plan.symbol,
                plan.start_ms,
                plan.end_ms,
                datasets=plan.datasets,
            )
            observed = tuple(
                sorted((ExpectedGap.of(g) for g in report.gaps), key=_order)
            )
        except Exception as exc:  # noqa: BLE001 - the drill records failures, it is one
            error = f"{type(exc).__name__}: {exc}"
        outcomes.append(DrillOutcome(injection=injection, observed=observed, error=error))

    after = source_fingerprint(
        source_market, plan.symbol, plan.datasets, plan.start_ms, plan.end_ms
    )
    return DrillReport(
        plan=plan,
        source=source_market,
        files_copied=copied,
        fingerprint_before=before,
        fingerprint_after=after,
        outcomes=tuple(outcomes),
    )
