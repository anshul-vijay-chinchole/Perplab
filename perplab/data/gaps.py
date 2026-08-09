"""Gap detection over the Parquet lake (spec 4.5).

A gap is a stretch of time the lake claims to cover and does not. The distinction that
makes this module worth writing carefully is that **absence of records is not evidence of
absence of market**: Binance emits zero-volume klines through illiquid periods rather than
omitting them, and a collector that recorded nothing for ten minutes looks, on disk,
exactly like ten minutes in which nobody traded. Conflating the two in either direction is
a specific, named failure (review finding R21), and both directions are expensive:

- reading a zero-volume bar as a gap floods the report with false positives until the
  report is ignored, which is how a real gap gets shipped into a backtest;
- reading a missing bar as "quiet" hides a hole, and a backtest over a hole silently
  prices the strategy on data that never existed.

So every rule below asks for positive evidence rather than inferring from silence.

**The four rules, exactly as spec 4.5 states them.**

1. *Klines* -- expected bar count for the interval versus actual. Presence is judged on the
   bar's `open_time` alone; `volume` is never consulted, so a zero-volume bar counts as
   data by construction rather than by remembering to special-case it.
2. *Tick datasets* (`aggTrades`, `bookTicker`, `depth20`) -- an inter-record interval over
   a threshold (60 s by default) **during a period where klines show non-zero volume**.
   The kline cross-check is the whole point: without it, a genuinely quiet market is
   reported as a collector dropout. It also means tick gap detection *depends* on klines
   being ingested, and when they are not this module says so rather than guessing.
3. *Funding* -- consecutive settlements more than `1.5 x fundingIntervalHours` apart, with
   the interval read from the ingested data. Not hardcoded to 8: Binance runs different
   intervals on different symbols and has changed the interval on existing ones, and
   `exchangeInfo` does not carry the field at all, so the archive is the only source
   (spec 3.5, review finding R17).
4. *Collector datasets* -- the collector writes a heartbeat every 10 s whether or not
   anything happened, so a hole in the event stream is unambiguous evidence the process
   was not running. A gap that a CONNECT / RECONNECT / DISCONNECT / RESTART / STALE /
   SHUTDOWN record accounts for is **explained**; one with no such record is a real
   failure. That distinction is what makes the Phase 1b exit criterion ("zero *unexplained*
   gaps") a decidable question instead of a judgement call.

A fifth rule postdates spec 4.5: *metrics* -- inter-sample spacing over three times its
five-minute cadence, the same threshold the collector's own staleness alarm uses. Added by
the data-lake audit (finding L7) once the dataset gained a live producer; see
`detect_metrics_gaps` for why no kline cross-check applies to it.

**Interpolation is never offered, and there is deliberately no code path for it.** Filling
a gap manufactures prices that never traded, and a manufactured price is indistinguishable
from a real one once written. The three things a run may do about a gap are `STRICT`,
`HALT_TRADING` and `SKIP` (`GapPolicy`); inventing data is not among them and no argument,
flag or keyword here enables it.

**Why DuckDB and not pyarrow.** Every rule is a `lag()` over one ordered timestamp column
spread across many part-files. DuckDB streams that through a windowing operator and spills
to disk if it must; pyarrow would need the whole column resident, and a year of BTCUSDT
`aggTrades` is tens of gigabytes of `ts_ms` alone. Hive partition pruning also makes the
range filter close to free -- which is why `_LakeReader.source` declares the Hive layout
and applies `query.partition_predicate` rather than handing DuckDB a bare glob: without
the predicate every gap check opened every file of the symbol's history to consult its
footer statistics, and a sweep calling `detect_gaps` per grid point performed thousands of
full-lake scans. The coupling to `perplab.data.query` is limited to that one pure
predicate builder (plus the shared `hive_columns` declaration); this module still builds
its own `read_parquet` expressions and accepts an optional connection, so a gap report can
run against a lake mid-ingest and distinguish "no rows" from "no dataset".
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from perplab.core.types import CollectorEventKind
from perplab.data.query import configure_spill, hive_columns, partition_predicate
from perplab.data.schemas import (
    SYMBOLLESS_DATASETS,
    layout_for,
    normalise_symbol,
    partition_key,
)

__all__ = [
    "GapPolicy",
    "GapKind",
    "GapRule",
    "Gap",
    "Coverage",
    "Unevaluated",
    "GapReport",
    "CollectorEventRecord",
    "GapDetectionError",
    "KlinesUnavailable",
    "DATASET_RULES",
    "DEFAULT_TICK_THRESHOLD_MS",
    "HEARTBEAT_INTERVAL_MS",
    "DEFAULT_EVENT_SILENCE_MS",
    "DEFAULT_EXPLANATION_TOLERANCE_MS",
    "MAX_UNCLOSED_EXPLANATION_MS",
    "METRICS_CADENCE_MS",
    "METRICS_SILENCE_MS",
    "EXPLAINING_KINDS",
    "interval_to_ms",
    "format_ms",
    "format_duration",
    "parse_gap_policy",
    "load_collector_events",
    "explain_gaps",
    "detect_kline_gaps",
    "detect_tick_gaps",
    "detect_funding_gaps",
    "detect_metrics_gaps",
    "detect_collector_event_gaps",
    "detect_gaps",
    "render_report",
]

_MS_PER_DAY = 86_400_000
_MS_PER_HOUR = 3_600_000

DEFAULT_TICK_THRESHOLD_MS = 60_000
"""Spec 4.5's default for tick datasets. Well above any healthy stream's cadence -- the
quietest of the three (`aggTrades`) publishes on every trade -- so this catches a stream
that has stopped, not one that is slow."""

HEARTBEAT_INTERVAL_MS = 10_000
"""Mirrors `perplab.data.collector.HEARTBEAT_INTERVAL_S` (spec 4.5).

Duplicated as a constant rather than imported because importing the collector drags in
`asyncio` and `websockets` for a number, and gap detection runs in batch contexts that
have no business starting an event loop. `tests/unit/test_gaps.py` asserts the two agree,
so the duplication cannot drift silently."""

DEFAULT_EVENT_SILENCE_MS = 3 * HEARTBEAT_INTERVAL_MS
"""How long the collector's event stream may go quiet before the process is presumed dead.

One missed beat is jitter: the heartbeat loop flushes Parquet between beats, and a slow
flush on a busy disk pushes the next beat late. Three consecutive misses is not jitter.
Setting this at exactly one interval would report a gap every time a flush ran long, which
trains an operator to ignore the report -- the failure mode that matters most here, since
the report's only job is to be believed."""

DEFAULT_EXPLANATION_TOLERANCE_MS = HEARTBEAT_INTERVAL_MS
"""Slack when matching a gap against the lifecycle record that explains it.

A DISCONNECT is stamped when the socket error surfaces, while the gap begins at the last
message that got through; the two are never the same millisecond. One heartbeat interval
is the resolution the event stream actually offers, so it is the honest tolerance. It is
deliberately not larger: widen this and every gap acquires an explanation, which converts
the Phase 1b exit criterion into a tautology."""

EXPLAINING_KINDS = frozenset(
    {
        CollectorEventKind.CONNECT,
        CollectorEventKind.RECONNECT,
        CollectorEventKind.DISCONNECT,
        CollectorEventKind.RESTART,
        CollectorEventKind.STALE,
        CollectorEventKind.SHUTDOWN,
        CollectorEventKind.UNAVAILABLE,
    }
)
"""Kinds that account for missing data. `HEARTBEAT` is excluded on purpose: a heartbeat is
evidence the collector was *alive*, so one adjacent to a gap makes that gap more alarming,
not less.

`UNAVAILABLE` is included because a dataset with no source cannot produce rows, and
reporting that permanently as an unexplained failure would leave the Phase 1b criterion
impossible to satisfy for a reason no amount of correct collector code could fix. It is
narrow by construction: the collector writes it only for datasets it knows it cannot
source, once per run, naming the dataset -- so it explains that dataset's silence and
nothing else's."""

_ONGOING_KINDS = frozenset(
    {
        CollectorEventKind.DISCONNECT,
        CollectorEventKind.STALE,
        CollectorEventKind.SHUTDOWN,
        CollectorEventKind.UNAVAILABLE,
        CollectorEventKind.CONNECT,
    }
)
"""Records that open an outage rather than measure one.

A DISCONNECT means "down from here"; a STALE means "this stream has stopped delivering".
Neither says for how long -- the outage runs until something *closes* it, which is what
`_RECOVERY_KINDS` do. A SHUTDOWN, an UNAVAILABLE and a CONNECT are never closed at all: the
process stopped, the source does not exist, or the collector did not yet exist.

RESTART and RECONNECT are absent because they are the opposite: they arrive *after* the
outage and state its measured length, so their window is exactly what they say it is."""

_RECOVERY_KINDS = frozenset(
    {CollectorEventKind.RECONNECT, CollectorEventKind.CONNECT, CollectorEventKind.RESTART}
)
"""Records that close an outage that an earlier record opened."""

_CAPPED_UNCLOSED_KINDS = frozenset(
    {CollectorEventKind.DISCONNECT, CollectorEventKind.STALE}
)
"""The transient openers, whose closure is *expected*: a live collector reconnects and a
dead one writes a RESTART, so one of these left unpaired forever is an anomaly rather
than an ongoing state, and its explaining power is bounded by
`MAX_UNCLOSED_EXPLANATION_MS`. The other ongoing kinds state open-ended facts and stay
unbounded -- see that constant's docstring."""

_DOWNTIME_KINDS = frozenset(
    {CollectorEventKind.RESTART, CollectorEventKind.RECONNECT}
)
"""Kinds written *after* the outage they account for, and carrying its measured length.

Everything else in the event stream is contemporaneous with what it describes. These two
are not: they are written when the collector comes back, so the record that explains the
last gap in a range can sit well outside it."""

RECOVERY_LOOKAHEAD_MS = 7 * 24 * 60 * 60 * 1000
"""How far past a range to look for a record that explains its trailing gap.

Seven days. The bound exists at all only to keep the query finite -- a lifetime scan of
`collectorEvents` on a multi-year lake is not free -- and it is generous because the cost of
being wrong is asymmetric. Too short reports a real, explained outage as an unexplained
one, which is the failure this whole module exists to prevent; too long reads a few extra
lifecycle rows. A machine that is off for more than a week has a gap nobody needed a report
to notice."""

MAX_UNCLOSED_EXPLANATION_MS = 24 * 60 * 60 * 1000
"""How long an unpaired DISCONNECT or STALE may keep explaining gaps (finding H23).

Twenty-four hours, and the number is Binance's, not ours: the exchange closes every
WebSocket connection after 24 h regardless of health, so a live collector writes a closing
CONNECT/RECONNECT at least daily, a dead one writes a RESTART with its measured downtime
at the next start, and the REST pollers pair every DISCONNECT with a RECONNECT the moment
a call succeeds. A transient opener that nothing has closed within one full connection
lifetime is therefore a record whose outage nothing ever measured -- most often an
incident record that was never an outage at all -- and before this bound existed, one such
record explained every gap after it forever: a single unparseable frame at 03:00 filed a
six-hour tick silence as EXPLAINED, which is precisely the "every gap acquires an
explanation" tautology the tolerance note warns against.

SHUTDOWN, UNAVAILABLE and CONNECT stay unbounded on purpose. They state open-ended facts
-- the process was stopped, the source does not exist, the collector did not yet exist --
whose whole meaning is "until further notice", and capping them would report a
deliberately stopped collector as an unexplained failure one day after the operator
stopped it."""

METRICS_CADENCE_MS = 300_000
"""The `metrics` sampling interval: five minutes, in both of the dataset's producers.
The daily archive publishes on a five-minute grid and `OpenInterestPoller` polls on the
same cadence precisely so the live period continues the historical series."""

METRICS_SILENCE_MS = 3 * METRICS_CADENCE_MS
"""How far apart consecutive `metrics` samples may sit before the stretch is a gap.

Not a number invented here: it is `collector.MAX_SILENCE_S['metrics']` (900 s), the
threshold the collector itself already uses to declare its open-interest poller STALE --
so the detector and the alarm that explains its findings agree by construction, and
`tests/unit/test_gaps.py` asserts they keep agreeing. Three cadences for the same reason
the heartbeat rule uses three intervals: one late sample is jitter (the archive's grid
and the poller's unsnapped clock both wander), three in a row is a stopped source."""

_MACRO_SERVICE_STREAMS = frozenset({"macroGlobal", "macroFx"})
"""Streams written by `MacroService`, which is a separate process from the market
collector (finding H24). Mirrors `macro.MACRO_DATASETS`; duplicated rather than imported
for the same reason as `HEARTBEAT_INTERVAL_MS` -- importing `macro` drags `httpx` into a
batch report -- and `tests/unit/test_gaps.py` asserts the two agree so the copy cannot
drift silently."""

_FUNDING_TOLERANCE_NUM = 3
_FUNDING_TOLERANCE_DEN = 2
"""Spec 4.5's `1.5 x fundingIntervalHours`, as a ratio rather than a float literal. An
interval in whole hours times 3_600_000 is always even, so `* 3 // 2` is exact -- and a
threshold computed in floating point is a threshold that can disagree with itself across
machines at the boundary, which is precisely where a gap detector is judged."""


class GapDetectionError(Exception):
    """A gap report could not be produced, and producing a partial one would mislead."""


class KlinesUnavailable(GapDetectionError):
    """Tick gap detection was asked for without the klines it must cross-check against.

    Spec 4.5 defines a tick gap only relative to kline volume. Without klines the rule has
    no meaning, and the two ways of proceeding anyway are both wrong: report every silence
    and a quiet weekend becomes a hundred false outages, or report none and a real dropout
    is filed as a calm market. Neither is a gap report, so this raises instead (spec 1.4).
    """


class GapPolicy(Enum):
    """What a backtest run does about a gapped range (spec 4.5).

    Consumed by the engine in Phase 4; defined here so the vocabulary is fixed before the
    code that acts on it exists, and so nothing invents a fourth option.

    The choice is made in run config and is **never defaulted silently**. `STRICT` is the
    value a UI should preselect, not a fallback this module applies on the caller's behalf
    -- `parse_gap_policy` raises on a missing or unknown value rather than choosing one,
    because a run that silently picked its own gap policy is a run whose results mean
    something different from what the operator believes.

    There is no `INTERPOLATE`. Interpolating across a gap manufactures prices that never
    traded, and once written those prices are indistinguishable from observed ones.
    """

    STRICT = "STRICT"
    """Refuse to run. The report must show which gaps and how long -- see `render_report`,
    which exists to make that answerable at a glance rather than by reading a list."""

    HALT_TRADING = "HALT_TRADING"
    """Run, treating each gap as "no execution possible": no fills, no new orders, existing
    positions held and marked at the last known price. A realistic simulation of an
    outage rather than a pretence that one did not happen."""

    SKIP = "SKIP"
    """Jump over the gap. Requires typed confirmation and permanently flags the run
    `GAP_SKIPPED`, because a skipped gap changes what the equity curve means and that fact
    must survive into every later view of the run."""

    @property
    def refuses_to_run(self) -> bool:
        return self is GapPolicy.STRICT

    @property
    def requires_typed_confirmation(self) -> bool:
        return self is GapPolicy.SKIP

    @property
    def run_flag(self) -> str | None:
        """The badge this policy stamps on the run, if any (spec 8 header badges)."""
        return "GAP_SKIPPED" if self is GapPolicy.SKIP else None


def parse_gap_policy(value: str | None) -> GapPolicy:
    """Resolve a configured gap policy, refusing to pick one on the caller's behalf."""
    if value is None or not str(value).strip():
        raise ValueError(
            "gap policy must be stated explicitly in run config; it is never defaulted "
            f"silently (spec 4.5). Choose one of: "
            f"{', '.join(p.value for p in GapPolicy)}"
        )
    text = str(value).strip().upper()
    try:
        return GapPolicy(text)
    except ValueError:
        raise ValueError(
            f"unknown gap policy {value!r}; known: "
            f"{', '.join(p.value for p in GapPolicy)}. Interpolating across a gap is not "
            f"an option and never will be -- it manufactures prices that never traded."
        ) from None


class GapKind(Enum):
    """What kind of evidence produced a gap. One per rule in spec 4.5."""

    MISSING_BARS = "MISSING_BARS"
    """Kline open times absent from the interval grid. `start_ms` is the first absent
    bar's open time and `end_ms` the next present one, so `duration_ms` is exactly the
    span of missing bars."""

    TICK_SILENCE = "TICK_SILENCE"
    """An inter-record interval over the threshold with kline volume inside it.
    `start_ms` and `end_ms` are the records either side, so `duration_ms` is the
    inter-record interval the rule tests."""

    MISSED_SETTLEMENT = "MISSED_SETTLEMENT"
    """Consecutive funding settlements further apart than `1.5 x` the interval in force.
    `duration_ms` is the observed inter-settlement interval."""

    METRIC_SILENCE = "METRIC_SILENCE"
    """Consecutive `metrics` samples further apart than `METRICS_SILENCE_MS` on a
    dataset that samples every five minutes from both of its producers. `start_ms` and
    `end_ms` are the samples either side (or the range edge), so `duration_ms` is the
    inter-sample interval the rule tests. No kline cross-check applies: open interest is
    published whether or not anyone trades, so a quiet market cannot excuse a silent
    series the way it excuses silent trades."""

    COLLECTOR_OUTAGE = "COLLECTOR_OUTAGE"
    """A hole in the collector's own event stream: no heartbeat, no lifecycle record,
    nothing. `duration_ms` is the silence between the bracketing events."""


class GapRule(Enum):
    """Which rule governs a dataset. The first four are spec 4.5's; `METRICS` was added
    by the data-lake audit (finding L7) once `metrics` gained a second, live producer
    whose staleness threshold gave the rule a reviewed number to borrow."""

    KLINES = "klines"
    TICK = "tick"
    FUNDING = "funding"
    METRICS = "metrics"
    COLLECTOR_EVENTS = "collectorEvents"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class DatasetRule:
    rule: GapRule
    note: str
    """Why this rule, or -- for `NONE` -- why no rule exists. Carried next to the rule
    rather than in a parallel table so the two cannot desync, and so an unevaluated
    dataset can explain itself in the report instead of merely being absent from it."""


DATASET_RULES: dict[str, DatasetRule] = {
    "klines": DatasetRule(
        GapRule.KLINES,
        "expected bar count for the interval vs actual; zero-volume bars are data",
    ),
    "markPriceKlines": DatasetRule(
        GapRule.KLINES,
        "same 1 m grid as klines. Its volume columns are always 0 because a mark price "
        "has no volume concept -- that is data, and presence is judged on open_time "
        "alone, so nothing here can mistake it for a gap",
    ),
    "aggTrades": DatasetRule(GapRule.TICK, "inter-trade silence, cross-checked on klines"),
    "bookTicker": DatasetRule(
        GapRule.TICK,
        "inter-update silence, cross-checked on klines. Bulk coverage is only "
        "2023-05-16 .. 2024-03-30 (finding F1), so a range outside that window is "
        "absent by publication, not by failure",
    ),
    "depth20": DatasetRule(GapRule.TICK, "collector-only; 1 s cadence, cross-checked on klines"),
    "funding": DatasetRule(
        GapRule.FUNDING,
        "1.5 x funding_interval_hours, read from the data rather than assumed (R17)",
    ),
    "collectorEvents": DatasetRule(
        GapRule.COLLECTOR_EVENTS, "heartbeat every 10 s; a hole means the process was down"
    ),
    "macroGlobal": DatasetRule(
        GapRule.NONE,
        "CoinGecko publishes on its own schedule and a poll that finds an unchanged "
        "snapshot writes no row, so a quiet stretch is indistinguishable from an outage "
        "by row spacing alone -- the two are separated by the DISCONNECT/RECONNECT "
        "events the macro pollers write into collectorEvents, which is the signal that "
        "actually applies. `ctx.macro()` additionally hands every reading's age to the "
        "strategy, so staleness is judged where it matters rather than inferred here",
    ),
    "macroFx": DatasetRule(
        GapRule.NONE,
        "DXY does not print at weekends or exchange holidays, so an absence of rows is "
        "usually the market being shut rather than a gap -- a spacing rule would report "
        "a failure every Saturday and train the reader to ignore it. Outages are visible "
        "through collectorEvents, and `ctx.macro()` carries the reading's age so a "
        "strategy can refuse a stale dollar on its own terms",
    ),
    "markPrice": DatasetRule(
        GapRule.NONE,
        "the 1 s mark price stream updates whether or not anyone trades, so the kline "
        "volume cross-check would suppress real gaps during quiet periods rather than "
        "confirm them. Its coverage is reported through collectorEvents, which is the "
        "signal that actually applies to it",
    ),
    "liquidations": DatasetRule(
        GapRule.NONE,
        "forced orders are genuinely sparse -- hours pass without one on a calm day -- so "
        "no silence threshold distinguishes an outage from a quiet market. This is the "
        "same reason the collector omits it from MAX_SILENCE_S",
    ),
    "metrics": DatasetRule(
        GapRule.METRICS,
        "5 min cadence from both producers (daily archive and OpenInterestPoller); "
        "samples further apart than 3x that -- the collector's own MAX_SILENCE_S "
        "threshold, not a number invented here -- are a gap. No kline cross-check: open "
        "interest publishes whether or not anyone trades",
    ),
    "bookDepth": DatasetRule(
        GapRule.NONE,
        "no rule can be responsible here: this dataset's column types were inferred from "
        "the archive's column *names* on 2026-08-01 and no sample row has ever been "
        "captured (see bulk_layout.parse_book_depth_row), so even its cadence is an "
        "assumption -- a silence threshold over unverified data would be a threshold "
        "over a guess. It is also a banded liquidity feature (spec 4.2/R1), not an "
        "execution input, so a hole degrades a signal rather than a fill. Revisit after "
        "the first real archive is ingested and spot-checked",
    ),
}
"""Every lake dataset, including the four with no rule.

The rule-less ones are listed rather than omitted for the same reason `bulk_layout` keeps
`liquidationSnapshot`: a report that never mentions a dataset is indistinguishable from
one that forgot it, and "we cannot check this, here is why" is information the operator
needs before trusting a clean report.
"""


@dataclass(frozen=True, slots=True)
class Gap:
    """One stretch of time the lake does not cover.

    `start_ms` and `end_ms` bracket the gap in epoch milliseconds UTC and `duration_ms` is
    their difference; what the two endpoints *are* depends on the rule and is documented on
    each `GapKind`, because a bar grid and an inter-record silence are measured from
    genuinely different things and papering over that would make one of them wrong.

    `duration_ms` is a property rather than a stored field so it cannot disagree with the
    endpoints. A gap whose stated length contradicts its own timestamps is worse than no
    gap report at all -- it is the number an operator reads first.

    `explanation` is `None` until something accounts for the gap. For collector datasets
    that is a lifecycle record from the event stream (`explain_gaps`); for bulk datasets
    there is nothing to account for a gap and `None` is the permanent, correct answer.
    """

    dataset: str
    symbol: str | None
    """`None` for `collectorEvents`, which describes the process rather than an
    instrument -- an outage there affects every symbol at once and attributing it to the
    one being reported on would understate it."""
    start_ms: int
    end_ms: int
    kind: GapKind
    detail: str
    explanation: str | None = None

    def __post_init__(self) -> None:
        if self.end_ms <= self.start_ms:
            raise ValueError(
                f"gap must span forward in time: {self.start_ms} .. {self.end_ms} "
                f"({self.dataset})"
            )

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms

    @property
    def explained(self) -> bool:
        return self.explanation is not None

    def with_explanation(self, explanation: str) -> Gap:
        return replace(self, explanation=explanation)

    def to_json(self) -> dict[str, Any]:
        """The shape a gap takes inside a dataset manifest (spec 4.6's `gaps` field).

        Defined here rather than in `manifest.py` because this module owns what a gap *is*.
        The manifest's job is to record the verdict, and a serialisation living on the
        recording side would have to be updated from a distance every time a field moved.

        `duration_ms` is written out even though it is derivable, because the manifest is
        read by people at least as often as by code and "how long" is the first question
        asked of a gap. `explained` likewise: it is the Phase 1b exit criterion, and making
        a reader deduce it from `explanation is not null` is how it gets deduced wrongly.

        Everything here is a JSON primitive, so a manifest round-trips to an equal value.
        Enums are written as their `.value`; a `repr` would embed the Python class name and
        make every stored manifest depend on this module's identity.
        """
        return {
            "dataset": self.dataset,
            "symbol": self.symbol,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "duration_ms": self.duration_ms,
            "kind": self.kind.value,
            "detail": self.detail,
            "explained": self.explained,
            "explanation": self.explanation,
        }


@dataclass(frozen=True, slots=True)
class Coverage:
    """What was actually found for one dataset over the reported range.

    Present so a clean report can be distinguished from an empty one. "No gaps" over a
    dataset with zero rows is not good news, and a gap list alone cannot tell the
    difference.
    """

    dataset: str
    symbol: str | None
    rows: int
    first_ms: int | None
    last_ms: int | None
    expected: int | None = None
    """Expected record count where the rule defines one (kline bars). `None` where it does
    not -- there is no expected number of trades in a minute."""
    zero_volume_bars: int | None = None
    """Klines only. Reported explicitly because these are the rows most likely to be
    mistaken for gaps (R21); seeing the count is how a reader confirms they were counted
    as data."""
    duplicate_rows: int = 0
    """Rows sharing their identity with another -- `open_time` for klines, `calc_time`
    for funding, `create_time` for metrics, and the per-dataset column in
    `_TICK_IDENTITY_COLUMNS` for the tick datasets (exchange ids where they exist, so a
    busy millisecond is never miscounted as a duplicate). Gap detection deduplicates
    before comparing against the grid -- a bar ingested twice is not a missing bar -- but
    the count is surfaced because it means a partition was written more than once, or a
    poller restart re-wrote an observation it had already flushed (finding H26), and the
    lake deserves a compaction either way."""


@dataclass(frozen=True, slots=True)
class Unevaluated:
    """A dataset that was not checked, and the reason.

    Silence about a dataset reads as a clean bill of health. This type exists so that
    "could not check" never renders as "nothing wrong" (spec 1.4).
    """

    dataset: str
    symbol: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class GapReport:
    symbol: str
    start_ms: int
    end_ms: int
    gaps: tuple[Gap, ...]
    coverage: tuple[Coverage, ...]
    unevaluated: tuple[Unevaluated, ...]

    @property
    def unexplained(self) -> tuple[Gap, ...]:
        """The Phase 1b exit criterion is that this is empty."""
        return tuple(g for g in self.gaps if not g.explained)

    @property
    def explained(self) -> tuple[Gap, ...]:
        return tuple(g for g in self.gaps if g.explained)

    @property
    def total_gap_ms(self) -> int:
        """Summed durations. Deliberately not deduplicated across datasets: two datasets
        down for the same hour is two hours of missing data, not one, because each has to
        be refetched separately."""
        return sum(g.duration_ms for g in self.gaps)

    def to_json(self) -> list[dict[str, Any]]:
        """The gap list a manifest stores, in report order.

        A list rather than an object because spec 4.6's `gaps` field is an array, and
        because the coverage and unevaluated sections are evidence *about* this run rather
        than part of the record of what data it saw. `datasets` in the manifest already
        says which files were read; repeating coverage there would give two answers to one
        question.

        The ordering is `detect_gaps`'s -- by start time, then dataset -- and it is stable,
        so two manifests over an unchanged lake compare equal rather than differing on the
        order threads happened to finish in.
        """
        return [gap.to_json() for gap in self.gaps]


@dataclass(frozen=True, slots=True)
class CollectorEventRecord:
    """One row of `collectorEvents`, typed.

    `kind` is kept as the enum rather than the raw string so an unknown kind fails at load
    time. A record the detector does not understand is a record it cannot use to explain a
    gap, and silently ignoring it would turn a real explanation into a phantom failure.
    """

    ts_ms: int
    kind: CollectorEventKind
    stream: str
    detail: str
    downtime_ms: int

    @property
    def covers_start_ms(self) -> int:
        """Earliest instant this record accounts for.

        `downtime_ms` is measured backwards: the collector stamps a RESTART with how long
        the previous run was dead, and a STALE with how long the stream had been silent.
        So the window a record explains begins that far before its own timestamp, not at
        it. Reading `downtime_ms` forwards would leave every crash gap unexplained --
        the record is written *after* the outage it describes.
        """
        return self.ts_ms - max(0, self.downtime_ms)


# --------------------------------------------------------------------------------------
# Formatting -- integer calendar arithmetic only
# --------------------------------------------------------------------------------------


def format_ms(ts_ms: int) -> str:
    """Render epoch ms as `2024-01-01T03:05:00.000Z`.

    Built on `schemas.partition_key` plus integer remainder arithmetic rather than
    `datetime`. A report that prints a timestamp an hour off because the machine is not on
    UTC is a report that sends someone hunting a gap that does not exist, and this is the
    one place in the module where a human reads a number and acts on it.
    """
    ms_of_day = ts_ms % _MS_PER_DAY
    hours, rest = divmod(ms_of_day, _MS_PER_HOUR)
    minutes, rest = divmod(rest, 60_000)
    seconds, millis = divmod(rest, 1000)
    return (
        f"{partition_key(ts_ms)}T{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}Z"
    )


def format_duration(ms: int) -> str:
    """Render a duration so "how long" is answerable without arithmetic (spec 4.5 STRICT).

    Sub-second gaps keep their milliseconds because at tick cadence that is the whole
    quantity; longer ones lead with the largest non-zero unit so an operator can rank
    gaps by eye.
    """
    if ms < 0:
        raise ValueError(f"duration must be non-negative, got {ms}")
    if ms < 1000:
        return f"{ms}ms"

    seconds, millis = divmod(ms, 1000)
    days, seconds = divmod(seconds, 86_400)
    hours, seconds = divmod(seconds, 3_600)
    minutes, seconds = divmod(seconds, 60)

    tail = f"{seconds:02d}.{millis:03d}s" if millis else f"{seconds:02d}s"
    if days:
        return f"{days}d {hours:02d}h {minutes:02d}m {tail}"
    if hours:
        return f"{hours}h {minutes:02d}m {tail}"
    if minutes:
        return f"{minutes}m {tail}"
    return f"{seconds}.{millis:03d}s" if millis else f"{seconds}s"


_INTERVAL_UNITS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


def interval_to_ms(interval: str) -> int:
    """Convert a Binance interval string (`1m`, `4h`, `1d`) to milliseconds.

    Refuses anything it does not recognise rather than falling back to a minute. The
    interval is the denominator of the entire kline rule: get it wrong and every bar in
    the range is reported either missing or misaligned, which looks like catastrophic data
    loss and is really a typo.
    """
    text = interval.strip()
    for unit in ("ms", "d", "h", "m", "s"):
        if text.endswith(unit) and len(text) > len(unit):
            count = text[: -len(unit)]
            if count.isdigit() and int(count) > 0:
                return int(count) * _INTERVAL_UNITS[unit]
            break
    raise ValueError(
        f"unrecognised interval {interval!r}; expected a positive count followed by one "
        f"of ms/s/m/h/d, e.g. '1m'"
    )


# --------------------------------------------------------------------------------------
# Lake access
# --------------------------------------------------------------------------------------


class _LakeReader:
    """Minimal DuckDB access to one Parquet lake root.

    Deliberately not built on `perplab.data.query`'s views. Gap detection has to run
    against a lake mid-ingest, over datasets that may not exist yet, and it needs to
    distinguish "no rows" from "no dataset" -- a view abstracts that difference away.
    Keeping the coupling at zero also means the two modules can change independently,
    which matters while both are being written.
    """

    def __init__(
        self, root: Path, connection: duckdb.DuckDBPyConnection | None = None
    ) -> None:
        self.root = Path(root)
        if connection is not None:
            self._con = connection
        else:
            # Spill to an absolute directory inside the lake, never DuckDB's relative
            # `.tmp` default: the tick rule sorts the whole requested range, and over a
            # multi-billion-row `aggTrades` that exceeds memory and spills. See
            # `query.SPILL_SUBDIR` for why the default made this crash depend on the
            # caller's working directory rather than on the query.
            self._con = duckdb.connect()
            configure_spill(self._con, self.root)

    def partition_root(self, dataset: str, symbol: str | None) -> Path:
        base = self.root / dataset
        if dataset in SYMBOLLESS_DATASETS or symbol is None:
            return base
        return base / f"symbol={normalise_symbol(symbol)}"

    def has_data(self, dataset: str, symbol: str | None) -> bool:
        """True if any published part-file exists.

        Checked on the filesystem rather than by catching DuckDB's IO error, because an
        empty glob and a corrupt file raise the same way and the two need different
        answers. In-flight `.tmp` files are invisible to this glob by construction -- the
        writer gives them a leading dot and a `.tmp` suffix.
        """
        root = self.partition_root(dataset, symbol)
        if not root.is_dir():
            return False
        return any(root.rglob("*.parquet"))

    def source(
        self,
        dataset: str,
        symbol: str | None,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> str:
        """A `read_parquet(...)` expression for use inside a query, partition-pruned.

        `hive_partitioning` plus `query.partition_predicate` is what lets DuckDB discard
        files by *path* before opening any of them. Without it, a rule's `WHERE ts_ms`
        filter still gave the right rows -- but only after reading the footer of every
        file the symbol has ever written, so each gap check was a full-history scan and
        `sweep.execute_point`, which runs one per grid point, performed thousands of them.

        The partition key types are pinned to `VARCHAR` via `hive_types`, borrowing
        `query.py`'s declaration, for `query.py`'s reason: left to autodetect, DuckDB
        types `year=2024` as `BIGINT` and `date=...` as `DATE`, and the predicate's string
        comparisons would bind differently per dataset. The symbol constraint is *not* in
        the predicate -- `partition_root` already scoped the glob to one `symbol=`
        directory, which prunes at least as hard.
        """
        glob = (self.partition_root(dataset, symbol) / "**" / "*.parquet").as_posix()
        # `**` matches zero or more directories in DuckDB, so one expression serves both
        # `funding/symbol=X/*.parquet` (granularity "none") and the nested layouts.
        hive_types = ", ".join(f"'{key}': VARCHAR" for key in hive_columns(dataset))
        expression = (
            f"""read_parquet('{glob.replace("'", "''")}', hive_partitioning := true"""
            + (f", hive_types := {{{hive_types}}}" if hive_types else "")
            + ")"
        )
        pruning = partition_predicate(dataset, start_ms=start_ms, end_ms=end_ms)
        if pruning == "TRUE":
            return expression
        return f"(SELECT * FROM {expression} WHERE {pruning})"

    def query(self, sql: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
        return self._con.execute(sql, params or []).fetchall()

    def register(self, name: str, table: pa.Table) -> None:
        self._con.register(name, table)

    def unregister(self, name: str) -> None:
        self._con.unregister(name)


# --------------------------------------------------------------------------------------
# Rule 1 -- klines
# --------------------------------------------------------------------------------------


def detect_kline_gaps(
    root: Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    dataset: str = "klines",
    interval: str | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[tuple[Gap, ...], Coverage]:
    """Spec 4.5 rule 1: expected bar count for the interval versus actual.

    **A zero-volume bar is data; an absent bar is a gap.** This function never reads the
    `volume` column, so there is no code path in which a quiet minute can be mistaken for
    a missing one -- the distinction is structural rather than a condition someone has to
    remember to write. `Coverage.zero_volume_bars` counts them anyway, so a reader can see
    that they were found and counted as present.

    The same property makes this correct for `markPriceKlines`, whose volume columns are
    always 0 by construction (there is nothing to trade in a computed index). A
    volume-based presence test would report every mark-price bar in history as missing.

    The interval defaults to the one in `schemas.PARTITION_LAYOUT`, so the grid used to
    judge coverage is the same string that names the `interval=` partition directory and
    the two cannot drift apart.

    Bars that are not on the interval grid raise rather than being reported as gaps: an
    off-grid open time means the range was ingested at a different interval, or the file
    is not what its path claims, and every count derived from a broken grid would be
    fiction (spec 1.4).
    """
    if end_ms <= start_ms:
        raise ValueError(f"range must span forward in time: {start_ms} .. {end_ms}")

    # Canonicalised once, here, so the symbol that goes into the `symbol=` glob is the same
    # string that comes back on every `Gap` and `Coverage`. A report naming `btcusdt` while
    # having read `symbol=BTCUSDT` is a report nobody can grep.
    symbol = normalise_symbol(symbol)
    step = interval_to_ms(interval or layout_for(dataset).interval or "1m")
    lake = _LakeReader(root, connection)

    first_expected = -(-start_ms // step) * step
    # A bar opening at `o` does not exist until it *closes*, at `o + step`. Expecting every
    # bar whose open time falls inside the range -- `((end_ms - 1) // step) * step` -- means
    # expecting the bar that is still forming, which can never be present. For a completed
    # historical range the two agree exactly, because `end_ms` lands on a step boundary; the
    # difference only appears when the range ends part-way through a bar, which is precisely
    # what `cmd_gaps` produces after clamping the end to the present. There it reported one
    # missing bar, every run, for ever, and exited non-zero on a lake with nothing wrong
    # with it -- teaching the operator to ignore the one command that says whether the
    # collector is working.
    last_expected = ((end_ms - step) // step) * step
    expected = 0 if last_expected < first_expected else (last_expected - first_expected) // step + 1

    if not lake.has_data(dataset, symbol):
        coverage = Coverage(
            dataset=dataset,
            symbol=symbol,
            rows=0,
            first_ms=None,
            last_ms=None,
            expected=expected,
            zero_volume_bars=0,
        )
        if expected == 0:
            return (), coverage
        return (
            Gap(
                dataset=dataset,
                symbol=symbol,
                start_ms=first_expected,
                end_ms=last_expected + step,
                kind=GapKind.MISSING_BARS,
                detail=f"all {expected} expected bars absent; dataset not present in lake",
            ),
        ), coverage

    src = lake.source(dataset, symbol, start_ms=start_ms, end_ms=end_ms)
    (rows, distinct, lo, hi, zero_volume, off_grid) = lake.query(
        f"SELECT count(*), count(DISTINCT open_time), min(open_time), max(open_time), "
        f"       count(*) FILTER (WHERE volume = 0), "
        f"       count(*) FILTER (WHERE open_time % ? <> 0) "
        f"FROM {src} WHERE open_time >= ? AND open_time < ?",
        [step, start_ms, end_ms],
    )[0]

    if off_grid:
        raise GapDetectionError(
            f"{dataset}/{symbol}: {off_grid} bar(s) in {format_ms(start_ms)} .. "
            f"{format_ms(end_ms)} have an open_time that is not a multiple of the "
            f"{step} ms interval. The range was ingested at a different interval, or "
            f"these files are not what their path claims -- every count derived from a "
            f"broken grid would be fiction, so no gap report is produced for it."
        )

    coverage = Coverage(
        dataset=dataset,
        symbol=symbol,
        rows=int(distinct or 0),
        first_ms=lo,
        last_ms=hi,
        expected=expected,
        zero_volume_bars=int(zero_volume or 0),
        duplicate_rows=int((rows or 0) - (distinct or 0)),
    )

    if not distinct:
        if expected == 0:
            return (), coverage
        return (
            Gap(
                dataset=dataset,
                symbol=symbol,
                start_ms=first_expected,
                end_ms=last_expected + step,
                kind=GapKind.MISSING_BARS,
                detail=f"all {expected} expected bars absent",
            ),
        ), coverage

    gaps: list[Gap] = []
    if lo > first_expected:
        gaps.append(
            Gap(
                dataset=dataset,
                symbol=symbol,
                start_ms=first_expected,
                end_ms=lo,
                kind=GapKind.MISSING_BARS,
                detail=f"{(lo - first_expected) // step} bar(s) absent before first bar",
            )
        )

    interior = lake.query(
        f"WITH bars AS ("
        f"  SELECT DISTINCT open_time FROM {src} WHERE open_time >= ? AND open_time < ?"
        f"), adjacent AS ("
        f"  SELECT open_time, lag(open_time) OVER (ORDER BY open_time) AS prev_open "
        f"  FROM bars"
        f") "
        f"SELECT prev_open, open_time FROM adjacent "
        f"WHERE prev_open IS NOT NULL AND open_time - prev_open > ? ORDER BY prev_open",
        [start_ms, end_ms, step],
    )
    # Every open_time was confirmed to be on the grid above, so every span between two of
    # them is a whole number of bars and this division is exact by construction.
    for prev_open, open_time in interior:
        gaps.append(
            Gap(
                dataset=dataset,
                symbol=symbol,
                start_ms=prev_open + step,
                end_ms=open_time,
                kind=GapKind.MISSING_BARS,
                detail=f"{(open_time - prev_open) // step - 1} bar(s) absent",
            )
        )

    if hi < last_expected:
        gaps.append(
            Gap(
                dataset=dataset,
                symbol=symbol,
                start_ms=hi + step,
                end_ms=last_expected + step,
                kind=GapKind.MISSING_BARS,
                detail=f"{(last_expected - hi) // step} bar(s) absent after last bar",
            )
        )

    return tuple(gaps), coverage


# --------------------------------------------------------------------------------------
# Rule 2 -- tick datasets, cross-checked against kline volume
# --------------------------------------------------------------------------------------


_TICK_IDENTITY_COLUMNS: dict[str, str] = {
    # What makes one tick row *the same observation* as another, per dataset -- the
    # column `Coverage.duplicate_rows` counts collisions on. It is deliberately not
    # `ts_ms` everywhere: two aggTrades in one millisecond are two trades on a busy
    # symbol, while two rows sharing one `agg_id` are one trade written twice. bookTicker
    # carries the exchange's own update sequence; depth20 is downsampled to one snapshot
    # per second by construction (`collector.DEPTH_BUCKET_MS`), so a repeated `ts_ms`
    # there really is a duplicate. Datasets absent from this table fall back to `ts_ms`.
    "aggTrades": "agg_id",
    "bookTicker": "update_id",
    "depth20": "ts_ms",
}


@dataclass(frozen=True, slots=True)
class _Silence:
    """A candidate tick gap, before the kline cross-check has ruled on it."""

    start_ms: int
    end_ms: int
    evidence_after_ms: int
    """A bar counts as evidence only if its `open_time` is strictly greater than this.

    The bar *containing* the last record before the silence has volume from that very
    record, so counting it would make every silence self-justifying. Excluding it leaves
    exactly the bars wholly inside the silence -- the ones whose volume can only have come
    from trades we failed to record.

    Where the silence starts at the range boundary rather than at a record there is
    nothing to exclude, so this is `start_ms - 1` and the bar opening exactly at the range
    start counts."""
    evidence_before_ms: int
    """Mirror for the far end: a bar counts only if its stored `close_time` is strictly
    less than this.

    No adjustment is needed at the range end. `close_time < end_ms` already means the bar
    finishes inside a half-open range, and the bar containing the record that ends a
    silence always closes at or after that record."""


def detect_tick_gaps(
    root: Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    dataset: str,
    threshold_ms: int = DEFAULT_TICK_THRESHOLD_MS,
    kline_dataset: str = "klines",
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[tuple[Gap, ...], Coverage]:
    """Spec 4.5 rule 2: silence over the threshold *while klines show non-zero volume*.

    The cross-check is the rule, not a refinement of it. A tick dataset going quiet proves
    nothing on its own -- most of an illiquid symbol's history is legitimately silent for
    minutes at a time, and reporting that as a dropout produces a gap list nobody reads.
    What proves a dropout is a bar that traded *entirely inside* the silence: the exchange
    recorded trades in a window where we recorded nothing.

    "Entirely inside" is exact and deliberately conservative. The bars containing the
    records either side of the silence carry volume from those very records, so they are
    excluded; only bars whose whole span falls in the hole count. The cost is that a
    silence just over the threshold, straddling two bars, contains no whole bar and is not
    flagged. The benefit is that a flagged gap is always backed by trades we can point at,
    which is what makes the report worth acting on.

    Three outcomes per silence, kept distinct because collapsing any two of them is the
    bug this rule exists to prevent:

    - bars inside, some with volume -> a gap;
    - bars inside, all zero-volume -> not a gap, the market was quiet;
    - no bars inside at all -> the cross-check could not run. Reported as a gap with that
      stated in the detail, because a hole in both datasets at once is not evidence of
      health and suppressing it would be the guess this module refuses to make.

    Raises `KlinesUnavailable` if the kline dataset is missing altogether. The caller is
    expected to record that rather than treat the dataset as clean; `detect_gaps` does.
    """
    if end_ms <= start_ms:
        raise ValueError(f"range must span forward in time: {start_ms} .. {end_ms}")
    if threshold_ms <= 0:
        raise ValueError(f"threshold must be positive, got {threshold_ms}")

    symbol = normalise_symbol(symbol)
    lake = _LakeReader(root, connection)

    if not lake.has_data(kline_dataset, symbol):
        raise KlinesUnavailable(
            f"{dataset}/{symbol}: no {kline_dataset} in the lake, so spec 4.5's tick rule "
            f"cannot be applied -- it is defined only relative to kline volume. Ingest "
            f"{kline_dataset} for this range first; reporting every silence or none of "
            f"them would both be guesses."
        )

    if not lake.has_data(dataset, symbol):
        # The registry note is repeated into the detail here because this is the one gap a
        # reader is most likely to misread as a collector failure. `bookTicker` outside
        # 2023-05-16 .. 2024-03-30 is absent because Binance stopped publishing it
        # (finding F1), not because anything broke, and the report has to say so.
        return (
            (
                Gap(
                    dataset=dataset,
                    symbol=symbol,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    kind=GapKind.TICK_SILENCE,
                    detail=(
                        "dataset not present in lake for this range -- "
                        f"{DATASET_RULES[dataset].note}"
                    ),
                ),
            ),
            Coverage(dataset=dataset, symbol=symbol, rows=0, first_ms=None, last_ms=None),
        )

    src = lake.source(dataset, symbol, start_ms=start_ms, end_ms=end_ms)
    identity = _TICK_IDENTITY_COLUMNS.get(dataset, "ts_ms")
    (rows, distinct, lo, hi) = lake.query(
        f"SELECT count(*), count(DISTINCT {identity}), min(ts_ms), max(ts_ms) FROM {src} "
        f"WHERE ts_ms >= ? AND ts_ms < ?",
        [start_ms, end_ms],
    )[0]
    coverage = Coverage(
        dataset=dataset,
        symbol=symbol,
        rows=int(rows or 0),
        first_ms=lo,
        last_ms=hi,
        duplicate_rows=int((rows or 0) - (distinct or 0)),
    )

    candidates: list[_Silence] = []
    if not rows:
        candidates.append(
            _Silence(start_ms, end_ms, evidence_after_ms=start_ms - 1, evidence_before_ms=end_ms)
        )
    else:
        if lo - start_ms > threshold_ms:
            candidates.append(
                _Silence(start_ms, lo, evidence_after_ms=start_ms - 1, evidence_before_ms=lo)
            )
        for prev_ms, ts_ms in lake.query(
            f"WITH ticks AS ("
            f"  SELECT ts_ms FROM {src} WHERE ts_ms >= ? AND ts_ms < ?"
            f"), adjacent AS ("
            f"  SELECT ts_ms, lag(ts_ms) OVER (ORDER BY ts_ms) AS prev_ms FROM ticks"
            f") "
            f"SELECT prev_ms, ts_ms FROM adjacent "
            f"WHERE prev_ms IS NOT NULL AND ts_ms - prev_ms > ? ORDER BY prev_ms",
            [start_ms, end_ms, threshold_ms],
        ):
            candidates.append(
                _Silence(prev_ms, ts_ms, evidence_after_ms=prev_ms, evidence_before_ms=ts_ms)
            )
        if end_ms - hi > threshold_ms:
            candidates.append(
                _Silence(hi, end_ms, evidence_after_ms=hi, evidence_before_ms=end_ms)
            )

    if not candidates:
        return (), coverage

    return tuple(_apply_kline_cross_check(lake, dataset, symbol, kline_dataset, candidates)), coverage


def _apply_kline_cross_check(
    lake: _LakeReader,
    dataset: str,
    symbol: str,
    kline_dataset: str,
    candidates: list[_Silence],
) -> list[Gap]:
    """Ask the klines, once, whether each candidate silence covers a period that traded.

    The candidates go into DuckDB as a registered Arrow table and are range-joined against
    the klines, rather than looping one query per candidate or pulling the bar index into
    Python. A badly broken day can produce thousands of candidates and a year of 1 m bars
    is half a million rows; either naive shape turns a report into a coffee break.
    """
    table = pa.table(
        {
            "idx": pa.array(range(len(candidates)), pa.int64()),
            "lo": pa.array([c.evidence_after_ms for c in candidates], pa.int64()),
            "hi": pa.array([c.evidence_before_ms for c in candidates], pa.int64()),
        }
    )
    # Every bar the join can use lies inside the span of the candidate windows, so the
    # kline scan is pruned to it rather than reading the symbol's whole bar history.
    scan_start = min(c.evidence_after_ms for c in candidates)
    scan_end = max(c.evidence_before_ms for c in candidates)
    name = f"_gaps_candidates_{id(candidates):x}"
    lake.register(name, table)
    try:
        counted = lake.query(
            f"SELECT c.idx, count(k.open_time) AS bars, "
            f"       count(k.open_time) FILTER (WHERE k.volume <> 0) AS traded, "
            f'       sum(k."count") FILTER (WHERE k.volume <> 0) AS trades '
            f"FROM {name} c "
            f"LEFT JOIN {lake.source(kline_dataset, symbol, start_ms=scan_start, end_ms=scan_end)} k "
            f"  ON k.open_time > c.lo AND k.close_time < c.hi "
            f"GROUP BY c.idx"
        )
    finally:
        lake.unregister(name)

    gaps: list[Gap] = []
    for idx, bars, traded, trades in sorted(counted):
        candidate = candidates[int(idx)]
        if traded:
            gaps.append(
                Gap(
                    dataset=dataset,
                    symbol=symbol,
                    start_ms=candidate.start_ms,
                    end_ms=candidate.end_ms,
                    kind=GapKind.TICK_SILENCE,
                    detail=(
                        f"no records for {format_duration(candidate.end_ms - candidate.start_ms)} "
                        f"while {traded} kline bar(s) inside the window traded "
                        f"({int(trades or 0)} trades)"
                    ),
                )
            )
        elif not bars:
            gaps.append(
                Gap(
                    dataset=dataset,
                    symbol=symbol,
                    start_ms=candidate.start_ms,
                    end_ms=candidate.end_ms,
                    kind=GapKind.TICK_SILENCE,
                    detail=(
                        "no records, and no kline bars cover the window either, so the "
                        "volume cross-check could not be applied -- reported on the "
                        "silence alone rather than assumed benign"
                    ),
                )
            )
        # else: bars present, none traded. A quiet market, which is data, not a gap --
        # this is the case the cross-check exists to protect, so it produces nothing.
    return gaps


# --------------------------------------------------------------------------------------
# Rule 3 -- funding
# --------------------------------------------------------------------------------------


def detect_funding_gaps(
    root: Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    dataset: str = "funding",
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[tuple[Gap, ...], Coverage]:
    """Spec 4.5 rule 3: settlements more than `1.5 x fundingIntervalHours` apart.

    **The interval is read from the data, never assumed.** Hardcoding 8 hours is review
    finding R17: Binance runs 4-hourly funding on some symbols, has moved existing symbols
    between schedules, and `exchangeInfo` carries no `fundingIntervalHours` at all for any
    of the 851 USD-M symbols -- so the monthly `fundingRate` archive is the only place the
    number exists, and it is stored per settlement precisely so this rule can read it.

    Where two adjacent settlements declare *different* intervals the schedule changed
    between them, and the archive does not say which side of the change the elapsed period
    belongs to. The longer of the two is used, so a documented schedule change does not
    manufacture a gap out of our own uncertainty. Where they agree -- every pair that is
    not a schedule change -- this is exactly `1.5 x` the stated interval.

    The settlement immediately before the range is loaded as an anchor. Without it a gap
    that opens at the start of the range is invisible, because the rule compares pairs and
    the first in-range settlement has nothing to pair with.
    """
    if end_ms <= start_ms:
        raise ValueError(f"range must span forward in time: {start_ms} .. {end_ms}")

    symbol = normalise_symbol(symbol)
    lake = _LakeReader(root, connection)
    if not lake.has_data(dataset, symbol):
        raise GapDetectionError(
            f"{dataset}/{symbol}: no funding data in the lake. The interval that defines "
            f"a funding gap is carried by the data itself (R17), so with no rows there is "
            f"no threshold to apply and no gap can be either found or ruled out."
        )

    src = lake.source(dataset, symbol)
    rows = lake.query(
        f"SELECT calc_time, funding_interval_hours FROM {src} "
        f"WHERE calc_time < ? ORDER BY calc_time",
        [end_ms],
    )
    in_range = [r for r in rows if r[0] >= start_ms]
    anchored = ([rows[len(rows) - len(in_range) - 1]] if len(in_range) < len(rows) else []) + in_range

    coverage = Coverage(
        dataset=dataset,
        symbol=symbol,
        rows=len(in_range),
        first_ms=in_range[0][0] if in_range else None,
        last_ms=in_range[-1][0] if in_range else None,
        duplicate_rows=len(in_range) - len({r[0] for r in in_range}),
    )

    if not anchored:
        raise GapDetectionError(
            f"{dataset}/{symbol}: no settlement at or before {format_ms(end_ms)}, so the "
            f"funding interval for this range is unknown and cannot be assumed (R17)."
        )

    for calc_time, hours in anchored:
        if hours is None or hours <= 0:
            raise GapDetectionError(
                f"{dataset}/{symbol}: settlement at {format_ms(calc_time)} declares a "
                f"funding interval of {hours!r} hours. A non-positive interval makes the "
                f"threshold zero and every pair a gap; the archive column is wrong or the "
                f"row was written by something that guessed."
            )

    gaps: list[Gap] = []
    for (prev_time, prev_hours), (calc_time, hours) in zip(anchored, anchored[1:]):
        # The longer of the two intervals: see the docstring. When they agree -- which is
        # every pair that is not a schedule change -- this reduces to 1.5 x the interval.
        governing = max(int(prev_hours), int(hours))
        limit = governing * _MS_PER_HOUR * _FUNDING_TOLERANCE_NUM // _FUNDING_TOLERANCE_DEN
        elapsed = calc_time - prev_time
        if elapsed > limit:
            gaps.append(
                Gap(
                    dataset=dataset,
                    symbol=symbol,
                    start_ms=prev_time,
                    end_ms=calc_time,
                    kind=GapKind.MISSED_SETTLEMENT,
                    detail=(
                        f"{format_duration(elapsed)} between settlements, over the "
                        f"{format_duration(limit)} allowed by a {governing}h interval "
                        f"read from the data"
                    ),
                )
            )

    last_time, last_hours = anchored[-1]
    trailing_limit = (
        int(last_hours) * _MS_PER_HOUR * _FUNDING_TOLERANCE_NUM // _FUNDING_TOLERANCE_DEN
    )
    if end_ms - last_time > trailing_limit:
        gaps.append(
            Gap(
                dataset=dataset,
                symbol=symbol,
                # Clamped to the range. The anchor is often the last settlement *before*
                # the range -- that is what makes the trailing check work at all when no
                # settlement falls inside it -- but reporting a gap that starts hours
                # before the window the user asked about overstates the duration, inflates
                # `total_gap_ms`, and lets a gap outside the range satisfy the overlap test
                # that decides which datasets count as gapped.
                start_ms=max(last_time, start_ms),
                end_ms=end_ms,
                kind=GapKind.MISSED_SETTLEMENT,
                detail=(
                    f"no settlement in the {format_duration(end_ms - last_time)} since "
                    f"{format_ms(last_time)}, over the {format_duration(trailing_limit)} "
                    f"allowed by a {int(last_hours)}h interval read from the data"
                ),
            )
        )

    return tuple(gaps), coverage


# --------------------------------------------------------------------------------------
# Rule 5 -- metrics (audit finding L7; threshold borrowed from the collector)
# --------------------------------------------------------------------------------------


def detect_metrics_gaps(
    root: Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    dataset: str = "metrics",
    threshold_ms: int = METRICS_SILENCE_MS,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[tuple[Gap, ...], Coverage]:
    """Inter-sample silence over `METRICS_SILENCE_MS` on the five-minute metrics series.

    This rule postdates spec 4.5 (audit finding L7) and exists because `metrics` stopped
    being archive-only: `ctx.oi()` reads it, `OpenInterestPoller` extends it live, and a
    dataset a strategy consumes with no gap rule meant an eight-hour hole in open
    interest degraded signals with nothing anywhere saying so. The threshold is not a new
    judgment call -- it is the collector's own `MAX_SILENCE_S['metrics']`, the number a
    reviewer already accepted as "this source has stopped", which is what keeps the
    original objection to a rule here ("an unreviewed threshold beside four specified
    ones") from applying to this one.

    **No kline cross-check, unlike the tick rule, and that is the point of a separate
    rule.** Open interest is a position count: Binance publishes a fresh sample every
    five minutes whether or not anyone trades, so a quiet market cannot excuse a silent
    series -- while running the tick rule's volume cross-check here would do exactly
    that, suppressing real outages over every calm stretch.

    Zero rows in a range where the dataset otherwise exists is one whole-range gap, not
    an error: the series demonstrably runs on a grid, so absence over the range is the
    thing this rule reports. A dataset absent from the lake entirely still raises --
    "never ingested" is a statement about the lake, not about the source, and
    `detect_gaps` records it as unevaluated exactly as it does for funding.

    Duplicate `create_time` values are deduplicated before spacing is judged and counted
    in `Coverage.duplicate_rows` (finding H26): a restarted poller re-writing one sample
    must be visible as a duplicate, not mistaken for coverage.
    """
    if end_ms <= start_ms:
        raise ValueError(f"range must span forward in time: {start_ms} .. {end_ms}")
    if threshold_ms <= 0:
        raise ValueError(f"threshold must be positive, got {threshold_ms}")

    symbol = normalise_symbol(symbol)
    lake = _LakeReader(root, connection)
    if not lake.has_data(dataset, symbol):
        raise GapDetectionError(
            f"{dataset}/{symbol}: no metrics data in the lake for this symbol. Ingest "
            f"the archive (or run the collector) before asking whether the series has "
            f"holes; an empty lake is a coverage fact, not a source outage."
        )

    src = lake.source(dataset, symbol, start_ms=start_ms, end_ms=end_ms)
    (rows, distinct, lo, hi) = lake.query(
        f"SELECT count(*), count(DISTINCT create_time), min(create_time), "
        f"max(create_time) FROM {src} WHERE create_time >= ? AND create_time < ?",
        [start_ms, end_ms],
    )[0]
    coverage = Coverage(
        dataset=dataset,
        symbol=symbol,
        rows=int(rows or 0),
        first_ms=lo,
        last_ms=hi,
        duplicate_rows=int((rows or 0) - (distinct or 0)),
    )

    gaps: list[Gap] = []
    if not rows:
        return (
            (
                Gap(
                    dataset=dataset,
                    symbol=symbol,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    kind=GapKind.METRIC_SILENCE,
                    detail=(
                        f"no samples at all in a range that should hold one every "
                        f"{format_duration(METRICS_CADENCE_MS)}"
                    ),
                ),
            ),
            coverage,
        )

    def silence(gap_start: int, gap_end: int) -> Gap:
        return Gap(
            dataset=dataset,
            symbol=symbol,
            start_ms=gap_start,
            end_ms=gap_end,
            kind=GapKind.METRIC_SILENCE,
            detail=(
                f"{format_duration(gap_end - gap_start)} between samples, over the "
                f"{format_duration(threshold_ms)} allowed on a "
                f"{format_duration(METRICS_CADENCE_MS)} cadence"
            ),
        )

    if lo - start_ms > threshold_ms:
        gaps.append(silence(start_ms, lo))
    for prev_ms, ts_ms in lake.query(
        f"WITH samples AS ("
        f"  SELECT DISTINCT create_time FROM {src} "
        f"  WHERE create_time >= ? AND create_time < ?"
        f"), adjacent AS ("
        f"  SELECT create_time, lag(create_time) OVER (ORDER BY create_time) AS prev_ms "
        f"  FROM samples"
        f") "
        f"SELECT prev_ms, create_time FROM adjacent "
        f"WHERE prev_ms IS NOT NULL AND create_time - prev_ms > ? ORDER BY prev_ms",
        [start_ms, end_ms, threshold_ms],
    ):
        gaps.append(silence(prev_ms, ts_ms))
    if end_ms - hi > threshold_ms:
        gaps.append(silence(hi, end_ms))

    return tuple(gaps), coverage


# --------------------------------------------------------------------------------------
# Rule 4 -- the collector event stream
# --------------------------------------------------------------------------------------

_WS_STREAM_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("@depth20@100ms", "depth20"),
    ("@bookTicker", "bookTicker"),
    ("@aggTrade", "aggTrades"),
    ("@markPrice@1s", "markPrice"),
)
"""WebSocket stream suffix to lake dataset, mirroring `Collector._on_message`.

Duplicated rather than imported for the same reason as `HEARTBEAT_INTERVAL_MS`; the test
suite asserts every dataset the collector records is reachable through this table, so a
new stream cannot quietly become unexplainable.
"""

_PROCESS_WIDE_STREAM = "collector"
"""The `stream` value on process-level records (RESTART, SHUTDOWN, HEARTBEAT). These
account for a gap in any collector dataset, because the process being dead stops all of
them at once."""

_COLLECTOR_DATASETS = frozenset(
    {
        "depth20",
        "bookTicker",
        "aggTrades",
        "markPrice",
        "liquidations",
        "metrics",
        "collectorEvents",
    }
)
"""Datasets the collector produces, and therefore the only ones its event stream can speak
for. A missing month of bulk klines is not explained by the collector having restarted --
it is explained by a download that never ran, which is a different problem with a different
fix, and letting a RESTART absolve it would hide the second behind the first.

`aggTrades`, `bookTicker` and `metrics` are here because the collector writes them too
(`OpenInterestPoller` fills `metrics` live, stamping its DISCONNECT/RECONNECT pairs with
the dataset name). Over a bulk-only range there are simply no events to match, so nothing
is explained away."""


def _stream_covers_dataset(stream: str, dataset: str) -> bool:
    """Whether a record's `stream` field speaks for `dataset`.

    Three shapes appear on disk. `collector` is process-wide. A STALE record names the lake
    dataset directly. Connection records carry the comma-joined subscription list written
    by `StreamManager`, which covers every stream on that socket -- a disconnect takes all
    of them down together, so any one of them matching is enough.
    """
    for token in stream.split(","):
        token = token.strip()
        if not token:
            continue
        if token == _PROCESS_WIDE_STREAM or token == dataset:
            return True
        if token == "!forceOrder@arr" and dataset == "liquidations":
            return True
        for suffix, mapped in _WS_STREAM_SUFFIXES:
            if token.endswith(suffix) and mapped == dataset:
                return True
    return False


def load_collector_events(
    root: Path,
    start_ms: int,
    end_ms: int,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[CollectorEventRecord, ...]:
    """Read the event stream over a range, widened to catch the records that bracket it.

    The window is extended by one `DEFAULT_EVENT_SILENCE_MS` at each end because the record
    that explains a gap at the edge of the range routinely falls outside it: a collector
    that died before the range started writes its RESTART after the range starts, and a
    DISCONNECT immediately before the range end is explained by a RECONNECT just after.
    Clipping strictly to the range would make edge gaps look unexplained, which is the one
    error this whole mechanism exists to avoid.

    An unrecognised `kind` raises. A record the detector cannot interpret is a record it
    cannot use, and dropping it silently would convert a real explanation into a phantom
    failure.
    """
    lake = _LakeReader(root, connection)
    if not lake.has_data("collectorEvents", None):
        return ()

    # Two windows, because two kinds of record sit at different distances from the gap they
    # explain.
    #
    # Ordinary records (heartbeats, CONNECT, STALE) are contemporaneous, so a
    # `DEFAULT_EVENT_SILENCE_MS` cushion at each end is enough.
    #
    # A **downtime-carrying** record is not. A RESTART is written when the collector comes
    # *back*, and it accounts for the outage that preceded it — so the record that explains
    # a gap at the end of the range can be written arbitrarily later. Widening only by 30 s
    # meant an overnight crash reported UNEXPLAINED on every `--end yesterday` run, which is
    # the exact query the Phase 1b exit criterion is checked with: the explanation existed,
    # matched perfectly, and was simply never loaded.
    #
    # The lookahead is a separate, narrow query rather than a wider main one so the cost is
    # a handful of lifecycle rows instead of a week of 10-second heartbeats.
    main_source = lake.source(
        "collectorEvents",
        None,
        start_ms=start_ms - DEFAULT_EVENT_SILENCE_MS,
        end_ms=end_ms + DEFAULT_EVENT_SILENCE_MS,
    )
    rows = list(
        lake.query(
            f"SELECT ts_ms, kind, stream, detail, downtime_ms FROM {main_source} "
            f"WHERE ts_ms >= ? AND ts_ms < ? ORDER BY ts_ms",
            [start_ms - DEFAULT_EVENT_SILENCE_MS, end_ms + DEFAULT_EVENT_SILENCE_MS],
        )
    )
    recovery_kinds = ", ".join(f"'{kind.value}'" for kind in _DOWNTIME_KINDS)
    lookahead_source = lake.source(
        "collectorEvents",
        None,
        start_ms=end_ms + DEFAULT_EVENT_SILENCE_MS,
        end_ms=end_ms + RECOVERY_LOOKAHEAD_MS,
    )
    rows.extend(
        lake.query(
            f"SELECT ts_ms, kind, stream, detail, downtime_ms FROM {lookahead_source} "
            f"WHERE ts_ms >= ? AND ts_ms < ? AND kind IN ({recovery_kinds}) "
            "ORDER BY ts_ms",
            [end_ms + DEFAULT_EVENT_SILENCE_MS, end_ms + RECOVERY_LOOKAHEAD_MS],
        )
    )

    events: list[CollectorEventRecord] = []
    for ts_ms, kind, stream, detail, downtime_ms in rows:
        try:
            parsed = CollectorEventKind(kind)
        except ValueError:
            raise GapDetectionError(
                f"collectorEvents at {format_ms(int(ts_ms))} has unknown kind {kind!r}; "
                f"known: {', '.join(k.value for k in CollectorEventKind)}. Gap detection "
                f"refuses to ignore a record it cannot interpret -- it may be the one "
                f"that explains an outage."
            ) from None
        events.append(
            CollectorEventRecord(
                ts_ms=int(ts_ms),
                kind=parsed,
                stream=stream or "",
                detail=detail or "",
                downtime_ms=int(downtime_ms or 0),
            )
        )
    return tuple(events)


def _is_macro_service_event(event: CollectorEventRecord) -> bool:
    """Whether a record was written by `MacroService` rather than the market collector.

    Judged on the `stream` field, which is the only identity the shared `collectorEvents`
    dataset carries -- the macro pollers stamp their dataset name (`macroGlobal`,
    `macroFx`) on every record they write, including the DISCONNECT/RECONNECT pairs their
    HTTP failures produce, so the field separates the two producers for every row ever
    written and no schema change is needed to keep reading old ones.
    """
    tokens = {token.strip() for token in event.stream.split(",") if token.strip()}
    return bool(tokens) and tokens <= _MACRO_SERVICE_STREAMS


def detect_collector_event_gaps(
    root: Path,
    start_ms: int,
    end_ms: int,
    *,
    events: tuple[CollectorEventRecord, ...] | None = None,
    max_silence_ms: int = DEFAULT_EVENT_SILENCE_MS,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> tuple[tuple[Gap, ...], Coverage]:
    """Spec 4.5 rule 4: holes in the collector's own heartbeat.

    The heartbeat is what makes an outage *unambiguous*. Market data going quiet is
    consistent with a quiet market; the collector writing nothing for a minute is
    consistent with nothing except the collector not running, because it writes a record
    every 10 s whether or not anything happened.

    Every *market-collector* record counts as liveness, not only `HEARTBEAT` -- a
    DISCONNECT proves the process was alive enough to notice the disconnection. Whether
    the resulting gap is *explained* is a separate question answered by `explain_gaps`,
    and keeping the two apart is deliberate: a SHUTDOWN followed by a CONNECT an hour
    later is a real gap in coverage that happens to have a known cause, and flattening it
    into "not a gap" would hide an hour of missing data behind a tidy log.

    Records written by `MacroService` are excluded from the liveness judgment (finding
    H24). The macro service is a separate process writing into the same `collectorEvents`
    partition, so counting its rows as heartbeats let a dead market collector hide behind
    a live macro poller: the collector dies at 02:00, MacroService keeps emitting a
    DISCONNECT per failed CoinGecko poll, and an eight-hour blackout of every market
    dataset produced no COLLECTOR_OUTAGE at all. Macro rows still *explain* macro
    datasets' quiet stretches through `explain_gaps`; they are simply not evidence that
    the market recorder was running, because they are not produced by it.
    """
    if end_ms <= start_ms:
        raise ValueError(f"range must span forward in time: {start_ms} .. {end_ms}")
    if max_silence_ms <= 0:
        raise ValueError(f"max_silence_ms must be positive, got {max_silence_ms}")

    if events is None:
        events = load_collector_events(root, start_ms, end_ms, connection=connection)

    in_range = [
        e
        for e in events
        if start_ms <= e.ts_ms < end_ms and not _is_macro_service_event(e)
    ]
    coverage = Coverage(
        dataset="collectorEvents",
        symbol=None,
        rows=len(in_range),
        first_ms=in_range[0].ts_ms if in_range else None,
        last_ms=in_range[-1].ts_ms if in_range else None,
    )

    if not in_range:
        return (
            (
                Gap(
                    dataset="collectorEvents",
                    symbol=None,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    kind=GapKind.COLLECTOR_OUTAGE,
                    detail="no collector records at all over the range",
                ),
            ),
            coverage,
        )

    gaps: list[Gap] = []
    if in_range[0].ts_ms - start_ms > max_silence_ms:
        gaps.append(
            Gap(
                dataset="collectorEvents",
                symbol=None,
                start_ms=start_ms,
                end_ms=in_range[0].ts_ms,
                kind=GapKind.COLLECTOR_OUTAGE,
                detail="no collector records before the first one in range",
            )
        )
    for previous, current in zip(in_range, in_range[1:]):
        silence = current.ts_ms - previous.ts_ms
        if silence > max_silence_ms:
            gaps.append(
                Gap(
                    dataset="collectorEvents",
                    symbol=None,
                    start_ms=previous.ts_ms,
                    end_ms=current.ts_ms,
                    kind=GapKind.COLLECTOR_OUTAGE,
                    detail=(
                        f"{format_duration(silence)} with no record, over the "
                        f"{format_duration(max_silence_ms)} a live collector may go quiet"
                    ),
                )
            )
    if end_ms - in_range[-1].ts_ms > max_silence_ms:
        gaps.append(
            Gap(
                dataset="collectorEvents",
                symbol=None,
                start_ms=in_range[-1].ts_ms,
                end_ms=end_ms,
                kind=GapKind.COLLECTOR_OUTAGE,
                detail="no collector records after the last one in range",
            )
        )

    return tuple(gaps), coverage


def _stream_identity(stream: str) -> frozenset[str]:
    """The set of things a record's `stream` field speaks for, in canonical names.

    Three producers write three spellings of the same identity, and exact string equality
    between them never holds (finding H23): the collector's malformed-frame records carry
    a single raw WebSocket stream (`btcusdt@bookTicker`), `StreamManager`'s lifecycle
    records carry the comma-joined subscription list, and the REST/staleness paths carry
    the lake dataset name. Each comma-separated token is therefore normalised -- WebSocket
    suffixes through the same `_WS_STREAM_SUFFIXES` table `_stream_covers_dataset` uses,
    `!forceOrder@arr` to `liquidations`, `collector` kept as the process-wide marker --
    and anything unrecognised is kept verbatim, so two unknown-but-equal spellings still
    pair with each other while never pairing with anything else.
    """
    tokens: set[str] = set()
    for raw in stream.split(","):
        token = raw.strip()
        if not token:
            continue
        if token == "!forceOrder@arr":
            tokens.add("liquidations")
            continue
        for suffix, mapped in _WS_STREAM_SUFFIXES:
            if token.endswith(suffix):
                tokens.add(mapped)
                break
        else:
            tokens.add(token)
    return frozenset(tokens)


def _closes(opener: frozenset[str], closer: frozenset[str]) -> bool:
    """Whether a recovery on `closer` ends an outage opened on `opener`.

    Intersection, not equality (finding H23). A socket carrying four streams drops all of
    them at once, so a RECONNECT stamped with the full subscription list closes a
    DISCONNECT stamped with any one of them -- under exact string matching those never
    paired, and every single-stream DISCONNECT stayed open forever, explaining everything
    after it. A process-wide recovery (`collector`: a RESTART or a cold-start CONNECT)
    closes any opener, because the process coming back supersedes whatever state the
    previous run's records described; the reverse is not true -- a single stream
    recovering says nothing about a SHUTDOWN of the whole process.
    """
    return _PROCESS_WIDE_STREAM in closer or bool(opener & closer)


def _closing_times(
    events: Sequence[CollectorEventRecord],
) -> dict[int, int | None]:
    """For each outage-opening record, the timestamp of the recovery that closes it.

    Keyed by `id()` of the record, which is safe because the mapping is built and consumed
    inside one `explain_gaps` call over one list.
    """
    closers: list[tuple[int, frozenset[str]]] = sorted(
        (event.ts_ms, _stream_identity(event.stream))
        for event in events
        if event.kind in _RECOVERY_KINDS
    )

    out: dict[int, int | None] = {}
    for event in events:
        if event.kind not in _ONGOING_KINDS:
            continue
        opened = _stream_identity(event.stream)
        out[id(event)] = next(
            (
                ts
                for ts, closer in closers
                if ts > event.ts_ms and _closes(opened, closer)
            ),
            None,
        )
    return out


def _accounts_for(
    event: CollectorEventRecord,
    gap: Gap,
    tolerance_ms: int,
    closes_at: dict[int, int | None],
) -> bool:
    """Is the stretch this record accounts for long enough to cover the gap?

    Overlap alone was the whole test, and it made an instantaneous record explain a gap of
    any length. A REST poller writes a DISCONNECT when one HTTP request fails and a
    RECONNECT when the next succeeds twenty seconds later — and under pure overlap that
    twenty-second incident was recorded as accounting for a twenty-four-hour outage. That is
    the "widen it and every gap acquires an explanation" tautology this module's tolerance
    note warns about, arriving by a different route, and it is what let the aggregate-trade
    loss described in `rest_poller.fetch` be filed as explained.

    So each record is held to the stretch it actually speaks for:

    - **RESTART / RECONNECT** state a measured downtime: `[ts - downtime_ms, ts]`.
    - **DISCONNECT / STALE** open an outage that runs until the next recovery whose stream
      set covers the opener's (`_closes`) — `[ts, next_recovery]`. One that nothing has
      closed speaks for at most `MAX_UNCLOSED_EXPLANATION_MS` (finding H23): the exchange
      forcibly recycles every connection daily, so a live collector closes its transient
      openers at least that often, and an opener still unpaired after a full connection
      lifetime is an incident record, not a measured outage. Before the bound, one
      unparseable frame's DISCONNECT explained a gap of unbounded length.
    - **SHUTDOWN / UNAVAILABLE / CONNECT** are never closed and never capped: the process
      was stopped, the source does not exist, or the collector did not yet exist — all
      open-ended by meaning, not by accident of a missing closer.

    The `tolerance_ms` cushion is the same one used everywhere else and for the same reason:
    a failure is stamped when it surfaces, not when the last message got through.
    """
    if event.kind in _ONGOING_KINDS:
        closing = closes_at.get(id(event))
        if closing is None:
            if event.kind in _CAPPED_UNCLOSED_KINDS:
                window = MAX_UNCLOSED_EXPLANATION_MS + 2 * tolerance_ms
            else:
                return True
        else:
            window = closing - event.covers_start_ms + 2 * tolerance_ms
    else:
        window = event.ts_ms - event.covers_start_ms + 2 * tolerance_ms
    return window >= gap.end_ms - gap.start_ms


def explain_gaps(
    gaps: tuple[Gap, ...],
    events: tuple[CollectorEventRecord, ...],
    *,
    tolerance_ms: int = DEFAULT_EXPLANATION_TOLERANCE_MS,
) -> tuple[Gap, ...]:
    """Attach the lifecycle record that accounts for each gap, where one exists.

    This is the function the Phase 1b exit criterion rests on. A gap with a matching
    CONNECT / RECONNECT / DISCONNECT / RESTART / STALE / SHUTDOWN record is explained; one
    without is a real failure, and the run failed.

    A record matches when the window it accounts for overlaps the gap and its `stream`
    speaks for the gap's dataset. That window is `[ts - downtime_ms, ts]` widened by
    `tolerance_ms` at both ends: `downtime_ms` is measured backwards (a RESTART is written
    after the outage it reports, a STALE after the silence it reports), and the tolerance
    covers the gap between the last message that got through and the moment the failure
    was noticed. The tolerance is one heartbeat interval and should stay there -- widen it
    and every gap acquires an explanation, at which point "zero unexplained gaps" stops
    meaning anything.

    Gaps in non-collector datasets are returned untouched. Nothing in the event stream
    speaks for a bulk archive: if a month of klines is missing, the collector's health has
    no bearing on it and pretending otherwise would explain away a download that never ran.
    """
    if tolerance_ms < 0:
        raise ValueError(f"tolerance must be non-negative, got {tolerance_ms}")

    explaining = [e for e in events if e.kind in EXPLAINING_KINDS]
    closes_at = _closing_times(events)
    out: list[Gap] = []
    for gap in gaps:
        if gap.dataset not in _COLLECTOR_DATASETS:
            out.append(gap)
            continue
        match: CollectorEventRecord | None = None
        for event in explaining:
            if not _stream_covers_dataset(event.stream, gap.dataset):
                continue
            if not _accounts_for(event, gap, tolerance_ms, closes_at):
                continue
            if (
                event.covers_start_ms - tolerance_ms <= gap.end_ms
                and event.ts_ms + tolerance_ms >= gap.start_ms
            ):
                match = event
                break
        if match is None:
            out.append(gap)
            continue
        downtime = (
            f", downtime {format_duration(match.downtime_ms)}" if match.downtime_ms else ""
        )
        out.append(
            gap.with_explanation(
                f"{match.kind.value} on {match.stream.split(',')[0] or '?'} at "
                f"{format_ms(match.ts_ms)}{downtime}: {match.detail}"
            )
        )
    return tuple(out)


# --------------------------------------------------------------------------------------
# The whole report
# --------------------------------------------------------------------------------------


def detect_gaps(
    root: Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    datasets: tuple[str, ...] | None = None,
    tick_threshold_ms: int = DEFAULT_TICK_THRESHOLD_MS,
    max_event_silence_ms: int = DEFAULT_EVENT_SILENCE_MS,
    explanation_tolerance_ms: int = DEFAULT_EXPLANATION_TOLERANCE_MS,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> GapReport:
    """Run every applicable rule over one symbol and range.

    Datasets with no rule in spec 4.5 are reported as `Unevaluated` with the reason rather
    than omitted, and so is a tick dataset whose kline cross-check could not run. An empty
    gap list is only good news if the report also says what was checked, and a reader has
    no way to tell "clean" from "skipped" unless the report distinguishes them.
    """
    if end_ms <= start_ms:
        raise ValueError(f"range must span forward in time: {start_ms} .. {end_ms}")

    owned = connection is None
    lake_connection = connection if connection is not None else duckdb.connect()
    try:
        return _detect_gaps(
            root,
            normalise_symbol(symbol),
            start_ms,
            end_ms,
            datasets=datasets,
            tick_threshold_ms=tick_threshold_ms,
            max_event_silence_ms=max_event_silence_ms,
            explanation_tolerance_ms=explanation_tolerance_ms,
            connection=lake_connection,
        )
    finally:
        # Closed only if we opened it. A connection handed in by the caller may back a
        # long-lived query session, and closing someone else's handle mid-session is a
        # far more confusing failure than leaking one.
        if owned:
            lake_connection.close()


def _detect_gaps(
    root: Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    *,
    datasets: tuple[str, ...] | None,
    tick_threshold_ms: int,
    max_event_silence_ms: int,
    explanation_tolerance_ms: int,
    connection: duckdb.DuckDBPyConnection,
) -> GapReport:
    """The body of `detect_gaps`, split out only so the connection it may have opened is
    closed by a `finally` that cannot be confused with a rule's own error handling."""
    lake_connection = connection
    names = datasets if datasets is not None else tuple(DATASET_RULES)

    events = load_collector_events(
        root, start_ms, end_ms, connection=lake_connection
    )

    gaps: list[Gap] = []
    coverages: list[Coverage] = []
    unevaluated: list[Unevaluated] = []

    for dataset in names:
        try:
            entry = DATASET_RULES[dataset]
        except KeyError:
            raise KeyError(
                f"no gap rule registered for dataset {dataset!r}; add it to DATASET_RULES "
                f"-- including as GapRule.NONE with a reason -- rather than letting it be "
                f"silently unchecked. Known: {', '.join(sorted(DATASET_RULES))}"
            ) from None

        if entry.rule is GapRule.NONE:
            unevaluated.append(Unevaluated(dataset, symbol, entry.note))
            continue

        try:
            if entry.rule is GapRule.KLINES:
                found, coverage = detect_kline_gaps(
                    root,
                    symbol,
                    start_ms,
                    end_ms,
                    dataset=dataset,
                    connection=lake_connection,
                )
            elif entry.rule is GapRule.TICK:
                found, coverage = detect_tick_gaps(
                    root,
                    symbol,
                    start_ms,
                    end_ms,
                    dataset=dataset,
                    threshold_ms=tick_threshold_ms,
                    connection=lake_connection,
                )
            elif entry.rule is GapRule.FUNDING:
                found, coverage = detect_funding_gaps(
                    root, symbol, start_ms, end_ms, dataset=dataset, connection=lake_connection
                )
            elif entry.rule is GapRule.METRICS:
                found, coverage = detect_metrics_gaps(
                    root, symbol, start_ms, end_ms, dataset=dataset, connection=lake_connection
                )
            else:
                found, coverage = detect_collector_event_gaps(
                    root,
                    start_ms,
                    end_ms,
                    events=events,
                    max_silence_ms=max_event_silence_ms,
                    connection=lake_connection,
                )
        except GapDetectionError as exc:
            # Recorded, never swallowed. The rule could not run, which is a different
            # statement from "the dataset is clean" and the report keeps them apart.
            unevaluated.append(Unevaluated(dataset, symbol, str(exc)))
            continue

        gaps.extend(found)
        coverages.append(coverage)

    explained = explain_gaps(
        tuple(gaps), events, tolerance_ms=explanation_tolerance_ms
    )
    ordered = tuple(sorted(explained, key=lambda g: (g.start_ms, g.dataset, g.end_ms)))

    return GapReport(
        symbol=symbol,
        start_ms=start_ms,
        end_ms=end_ms,
        gaps=ordered,
        coverage=tuple(coverages),
        unevaluated=tuple(unevaluated),
    )


def render_report(report: GapReport) -> str:
    """Render a report a human can act on.

    Written for `GapPolicy.STRICT`, which refuses to run and must "show which gaps and how
    long". The headline is therefore the count and the total duration, the per-gap lines
    lead with their duration, and explained gaps are marked but not hidden -- an explained
    outage is still an hour of data the backtest will not have.

    Coverage and unevaluated sections are always printed, including when there are no gaps.
    A gap list is the wrong thing to read to find out that a dataset was never ingested.
    """
    lines: list[str] = [
        f"Gap report -- {report.symbol}  "
        f"{format_ms(report.start_ms)} .. {format_ms(report.end_ms)}"
    ]

    unexplained = report.unexplained
    if report.gaps:
        lines.append(
            f"  {len(report.gaps)} gap(s), {len(unexplained)} unexplained, "
            f"{format_duration(report.total_gap_ms)} missing in total"
        )
    else:
        lines.append("  no gaps found")

    by_dataset: dict[str, list[Gap]] = {}
    for gap in report.gaps:
        by_dataset.setdefault(gap.dataset, []).append(gap)

    # Worst dataset first, measured by how much time is missing from it. "Which gaps and
    # how long" is the question STRICT mode has to answer, and answering it in ingest
    # order makes the reader do the sorting.
    for dataset, found in sorted(
        by_dataset.items(), key=lambda kv: (-sum(g.duration_ms for g in kv[1]), kv[0])
    ):
        lines.append("")
        lines.append(f"  {dataset}  ({len(found)} gap(s))")
        for gap in found:
            lines.append(
                f"    {format_duration(gap.duration_ms):>16}  "
                f"{format_ms(gap.start_ms)} .. {format_ms(gap.end_ms)}  "
                f"{gap.kind.value}"
            )
            lines.append(f"        {gap.detail}")
            lines.append(
                f"        EXPLAINED BY {gap.explanation}"
                if gap.explained
                else "        UNEXPLAINED"
            )

    if report.coverage:
        lines.append("")
        lines.append("  coverage")
        for cov in report.coverage:
            span = (
                f"{format_ms(cov.first_ms)} .. {format_ms(cov.last_ms)}"
                if cov.first_ms is not None and cov.last_ms is not None
                else "no rows in range"
            )
            counts = f"{cov.rows} rows"
            if cov.expected is not None:
                counts = f"{cov.rows}/{cov.expected} bars"
            lines.append(f"    {cov.dataset:<18} {counts:<22} {span}")
            if cov.zero_volume_bars:
                lines.append(
                    f"        {cov.zero_volume_bars} zero-volume bar(s) -- data, not gaps"
                )
            if cov.duplicate_rows:
                lines.append(
                    f"        {cov.duplicate_rows} duplicate timestamp(s) -- a partition "
                    f"was written more than once"
                )

    if report.unevaluated:
        lines.append("")
        lines.append("  not evaluated")
        for item in report.unevaluated:
            lines.append(f"    {item.dataset:<18} {item.reason}")

    return "\n".join(lines)
