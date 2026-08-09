"""Phase 1's exit criterion, as a test: the gap report on a deliberately corrupted sample.

Spec 13 asks for accuracy, and these assertions are what "accurate" is allowed to mean
here: for each injected fault the report must name exactly the gaps that were injected --
the same count, the same boundaries and therefore the same durations -- and nothing else.
A missed gap and a phantom gap both fail, and the zero-volume case fails if the report says
anything at all.

The sample is built by running the real bulk pipeline (checksum verification, per-file
header sniff, parse, atomic publish) over generated archives, so the lake the drill damages
is the lake a network backfill produces rather than a Parquet file written by the test.
See `sample_lake.py`.

`scripts/gap_detection_drill.py` runs the identical drill against the real ingested
BTCUSDT lake. Sharing the harness rather than the assertions is deliberate: this file
guarantees the drill is exercised on every `pytest` run without a six-year backfill on
disk, and the script proves the same code says the same thing about real exchange data.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from perplab.data.gaps import GapKind, detect_gaps, detect_kline_gaps
from tests.integration.corruption import (
    MS_PER_DAY,
    MS_PER_MINUTE,
    CORRUPTIONS,
    DrillPlan,
    DrillReport,
    ExpectedGap,
    run_drill,
)
from tests.integration.corruption import DrillOutcome
from tests.integration.sample_lake import SampleSpec, build_sample_lake

_DAYS = 5

_NAMES: dict[str, str] = {
    "pristine": "control",
    "delete_whole_day": "missing-day",
    "delete_mid_day_block": "mid-day-hole",
    "delete_single_bar": "single-bar",
    "remove_funding_settlement": "missed-settlement",
    "zero_a_bar": "zero-volume-bar",
    "truncate_file": "truncated-file",
}
"""Corruption function name -> the injection name it reports under.

Two vocabularies on purpose: the functions are named for what they do to the disk and the
injections for what the report should say, and a test id that reads `truncate_file` while
the failure output says `truncated-file` is one lookup away from confusing whoever is
diagnosing it. Mapped explicitly rather than derived so that renaming either side fails
here rather than silently parametrising over nothing.
"""


@pytest.fixture(scope="module")
def sample(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, SampleSpec]:
    """A known-good five-day lake, ingested for real from generated archives.

    Module-scoped: building it runs the full ingest pipeline over six archives, and every
    test below reads it without writing to it -- the drill copies before it damages.
    """
    base = tmp_path_factory.mktemp("sample")
    market = base / "userdata" / "market"
    spec = build_sample_lake(market, base / "mirror", days=_DAYS)
    return market, spec


@pytest.fixture(scope="module")
def drill(
    sample: tuple[Path, SampleSpec], tmp_path_factory: pytest.TempPathFactory
) -> DrillReport:
    """The whole drill, run once; each test below inspects one injection's outcome.

    Run once rather than per test because the injections are independent by construction
    (a fresh copy each) and re-running the ingest for every assertion would buy nothing.
    """
    market, spec = sample
    plan = DrillPlan(symbol=spec.symbol, start_ms=spec.start_ms, end_ms=spec.end_ms)
    return run_drill(market, tmp_path_factory.mktemp("drill"), plan)


def _outcome(report: DrillReport, name: str) -> DrillOutcome:
    """The one outcome an assertion is about, or a failure naming what was actually run.

    A lookup that returned `None` for a missing name would turn a renamed injection into
    an `AttributeError` three lines later, which reads as a harness bug rather than as the
    drill having silently stopped running that case.
    """
    for outcome in report.outcomes:
        if outcome.injection.name == name:
            return outcome
    raise AssertionError(
        f"no injection named {name!r} in the drill; ran "
        f"{[o.injection.name for o in report.outcomes]}"
    )


class TestKnownGoodSample:
    """The control. Every other assertion is meaningless if this one does not hold."""

    def test_pristine_sample_reports_no_gaps(
        self, sample: tuple[Path, SampleSpec]
    ) -> None:
        market, spec = sample
        report = detect_gaps(
            market,
            spec.symbol,
            spec.start_ms,
            spec.end_ms,
            datasets=("klines", "funding"),
        )
        assert report.gaps == ()

    def test_every_expected_bar_is_present(
        self, sample: tuple[Path, SampleSpec]
    ) -> None:
        market, spec = sample
        _, coverage = detect_kline_gaps(market, spec.symbol, spec.start_ms, spec.end_ms)
        assert coverage.rows == _DAYS * MS_PER_DAY // MS_PER_MINUTE
        assert coverage.rows == coverage.expected
        assert coverage.duplicate_rows == 0

    def test_zero_volume_bar_is_counted_as_data_not_as_a_gap(
        self, sample: tuple[Path, SampleSpec]
    ) -> None:
        """The archive carries a genuinely zero-volume bar, as Binance publishes them.

        Asserted on the coverage counter as well as on the empty gap list, because the two
        say different things: an empty gap list could also mean the bar was never read, and
        the counter proves it was found and classified as data (R21).
        """
        market, spec = sample
        gaps, coverage = detect_kline_gaps(
            market, spec.symbol, spec.start_ms, spec.end_ms
        )
        assert coverage.zero_volume_bars == 1
        assert gaps == ()

    def test_header_sniff_survived_both_eras(
        self, sample: tuple[Path, SampleSpec]
    ) -> None:
        """Half the generated archives carry a header and half do not.

        A parser that assumed "always headered" would drop the first bar of every
        headerless day, which surfaces here as a one-bar gap at midnight rather than as an
        error. Asserting on the first bar of each day names that failure directly, so a
        regression does not have to be diagnosed from a bar count.
        """
        market, spec = sample
        gaps, coverage = detect_kline_gaps(
            market, spec.symbol, spec.start_ms, spec.end_ms
        )
        assert gaps == ()
        assert coverage.first_ms == spec.days[0]
        assert coverage.last_ms == spec.days[-1] + MS_PER_DAY - MS_PER_MINUTE


class TestCorruptionDrill:
    """One test per injected fault, each asserting the exact report it must produce."""

    @pytest.mark.parametrize(
        "name", [c.__name__.lstrip("_") for c in CORRUPTIONS], ids=lambda n: n
    )
    def test_injection_produces_exactly_the_specified_report(
        self, drill: DrillReport, name: str
    ) -> None:
        outcome = _outcome(drill, _NAMES[name])
        assert outcome.ok, "\n" + outcome.render()

    def test_missing_day_is_one_gap_of_exactly_one_day(
        self, drill: DrillReport, sample: tuple[Path, SampleSpec]
    ) -> None:
        _, spec = sample
        outcome = _outcome(drill, "missing-day")
        day = DrillPlan(
            symbol=spec.symbol, start_ms=spec.start_ms, end_ms=spec.end_ms
        ).whole_day_ms
        assert outcome.observed == (
            ExpectedGap("klines", GapKind.MISSING_BARS, day, day + MS_PER_DAY),
        )
        assert outcome.observed[0].duration_ms == MS_PER_DAY

    def test_mid_day_hole_is_bounded_by_the_surviving_bars(
        self, drill: DrillReport
    ) -> None:
        """The gap opens at the first absent bar and closes at the next present one.

        Not at the last surviving bar and not at the last absent one: `duration_ms` is what
        an operator reads, and either off-by-one would misstate it by a whole bar while
        still looking like a correctly detected hole.
        """
        outcome = _outcome(drill, "mid-day-hole")
        assert len(outcome.observed) == 1
        gap = outcome.observed[0]
        assert gap.duration_ms == 17 * MS_PER_MINUTE
        assert gap.start_ms % MS_PER_MINUTE == 0

    def test_single_missing_bar_is_the_minimum_size_gap(
        self, drill: DrillReport
    ) -> None:
        """The boundary the interior rule's `> step` comparison turns on.

        One bar is the smallest hole the kline grid can express. A rule written with `>=`
        would report every adjacent pair of bars as a gap; one written to require two
        missing bars would report nothing here. Both are plausible mistakes and both are
        silent, so this case is asserted on its own rather than folded into the block test.
        """
        outcome = _outcome(drill, "single-bar")
        assert len(outcome.observed) == 1
        assert outcome.observed[0].duration_ms == MS_PER_MINUTE

    def test_missed_settlement_spans_the_surviving_neighbours(
        self, drill: DrillReport, sample: tuple[Path, SampleSpec]
    ) -> None:
        _, spec = sample
        outcome = _outcome(drill, "missed-settlement")
        assert len(outcome.observed) == 1
        gap = outcome.observed[0]
        assert gap.dataset == "funding"
        assert gap.kind is GapKind.MISSED_SETTLEMENT
        # Two intervals elapsed where one was due, which is what puts it over the
        # 1.5x threshold read from the data.
        assert gap.duration_ms == 2 * spec.funding_interval_hours * 3_600_000

    def test_zero_volume_bar_produces_no_gap_at_all(self, drill: DrillReport) -> None:
        """The false-positive half of the criterion.

        A detector that consults `volume` to decide presence passes every other test in
        this class and fails only this one, which is why it is asserted as an emptiness
        rather than inferred from the others passing.
        """
        outcome = _outcome(drill, "zero-volume-bar")
        assert outcome.observed == ()
        assert outcome.error is None

    def test_truncated_file_fails_loudly_rather_than_reading_short(
        self, drill: DrillReport
    ) -> None:
        """A corrupt file must stop the report, not shorten it.

        This is the one injection whose correct outcome is an exception. A truncated
        Parquet that read as merely *short* would present as a clean report over fewer
        bars, which is the failure mode `writer.py`'s atomic publish exists to prevent and
        the only one in this drill that a gap list cannot express.
        """
        outcome = _outcome(drill, "truncated-file")
        assert outcome.error is not None
        assert "magic bytes" in outcome.error
        assert outcome.observed == ()


class TestTheDrillItself:
    """Properties of the harness. A drill that damaged the source, or that ran against an
    empty lake, would pass every test above while proving nothing."""

    def test_source_lake_is_left_byte_identical(self, drill: DrillReport) -> None:
        assert drill.fingerprint_before == drill.fingerprint_after
        assert drill.source_untouched

    def test_every_injection_copied_real_files(self, drill: DrillReport) -> None:
        # Five daily kline files plus one monthly funding file.
        assert drill.files_copied == _DAYS + 1

    def test_report_renders_without_raising(self, drill: DrillReport) -> None:
        rendered = drill.render()
        assert "Corruption drill" in rendered
        for outcome in drill.outcomes:
            assert outcome.injection.name in rendered

    def test_whole_drill_passes(self, drill: DrillReport) -> None:
        assert drill.ok, "\n" + drill.render()

    def test_every_corruption_is_named_in_the_id_table(self) -> None:
        """`_NAMES` is the only thing keeping the parametrised ids honest.

        A corruption added to `CORRUPTIONS` without an entry here would raise a `KeyError`
        inside one parametrised case, which is a legible failure; one *removed* from
        `CORRUPTIONS` while its entry stays would leave a mapping nobody notices is dead.
        Both directions are checked so the table cannot drift in either.
        """
        assert {c.__name__.lstrip("_") for c in CORRUPTIONS} == set(_NAMES)
