"""Tests for gap detection (spec 4.5).

Phase 1's exit criterion is "gap report accurate on a deliberately corrupted sample", so
accuracy is the deliverable and these tests are the thing that establishes it. Each one
builds a small Parquet lake in `tmp_path` through the real `ParquetBufferedWriter`, so the
detector reads the same on-disk shape the ingest path writes -- partition layout, part-file
splitting and all. Nothing is stubbed between the fixture and the query.

Two failure modes get disproportionate coverage because they are the ones that produce a
*plausible* wrong answer rather than an obvious one:

- a zero-volume kline read as a gap, or a missing bar read as a quiet market (R21). Both
  render identically in a summary count and only diverge when someone acts on it;
- a tick silence flagged during a genuinely quiet market. That is the false positive that
  makes an operator stop reading the report, at which point the next real gap ships.

Every threshold is tested at its boundary and one millisecond either side. A gap detector
that is right in the middle of the range and wrong at the edge is a gap detector that
disagrees with itself about the interesting cases.
"""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import pytest

from perplab.core.money import to_scaled
from perplab.core.types import CollectorEventKind
from perplab.data import gaps
from perplab.data.gaps import (
    DATASET_RULES,
    DEFAULT_EXPLANATION_TOLERANCE_MS,
    EXPLAINING_KINDS,
    CollectorEventRecord,
    Gap,
    GapDetectionError,
    GapKind,
    GapPolicy,
    GapRule,
    KlinesUnavailable,
    detect_collector_event_gaps,
    detect_funding_gaps,
    detect_gaps,
    detect_kline_gaps,
    detect_metrics_gaps,
    detect_tick_gaps,
    explain_gaps,
    format_duration,
    format_ms,
    interval_to_ms,
    load_collector_events,
    parse_gap_policy,
    render_report,
)
from perplab.data.schemas import SCHEMAS
from perplab.data.writer import ParquetBufferedWriter

SYMBOL = "BTCUSDT"
MIN_MS = 60_000
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
T0 = 1_704_067_200_000
"""2024-01-01T00:00:00.000Z -- an exact boundary for every interval used below, so a bar
grid anchored at the epoch and one anchored at the range agree and no test accidentally
passes on an alignment coincidence."""

_PRICE = to_scaled("42000.10")
_VOLUME = to_scaled("327.207")
"""Scaled int64, as everything numeric in the lake is. The rules only ever test volume
against zero, but the fixtures use a real scaled value so nothing here quietly depends on
volume being a small integer."""


# --------------------------------------------------------------------------------------
# Fixture builders
# --------------------------------------------------------------------------------------


def _write(root: Path, dataset: str, rows: list[dict[str, Any]], *, symbol: str | None = SYMBOL) -> None:
    with ParquetBufferedWriter(root, dataset, SCHEMAS[dataset], symbol=symbol) as writer:
        for row in rows:
            writer.append(row)


def _bar(open_time: int, *, volume: int = _VOLUME, count: int = 5998) -> dict[str, Any]:
    """One kline row, with Binance's `close_time == open_time + interval - 1`.

    The stored close time matters: the tick cross-check asks whether a bar closes *inside*
    a silence, and deriving it as `open + interval` would move the answer by the exact
    millisecond the containment test turns on.
    """
    return {
        "open_time": open_time,
        "close_time": open_time + MIN_MS - 1,
        "open": _PRICE,
        "high": _PRICE,
        "low": _PRICE,
        "close": _PRICE,
        "volume": volume,
        "quote_volume": volume,
        "count": count if volume else 0,
        "taker_buy_volume": volume,
        "taker_buy_quote_volume": volume,
    }


def _write_klines(
    root: Path,
    minutes: list[int],
    *,
    zero_volume: frozenset[int] = frozenset(),
    dataset: str = "klines",
    base_ms: int = T0,
) -> None:
    _write(
        root,
        dataset,
        [
            _bar(base_ms + m * MIN_MS, volume=0 if m in zero_volume else _VOLUME)
            for m in minutes
        ],
    )


def _trade(ts_ms: int, agg_id: int = 1) -> dict[str, Any]:
    return {
        "ts_ms": ts_ms,
        # Null, as bulk archives have no local receive clock. The detector must never read
        # it; a fabricated copy of ts_ms here would hide that if it ever did.
        "recv_ms": None,
        "agg_id": agg_id,
        "price": _PRICE,
        "qty": _VOLUME,
        "first_trade_id": agg_id,
        "last_trade_id": agg_id,
        "is_buyer_maker": True,
    }


def _funding_row(calc_time: int, hours: int, rate: str = "0.00005703") -> dict[str, Any]:
    return {
        "calc_time": calc_time,
        "funding_interval_hours": hours,
        "funding_rate": to_scaled(rate),
    }


def _metrics_row(create_time: int) -> dict[str, Any]:
    return {
        "create_time": create_time,
        "sum_open_interest": to_scaled("105550.985"),
        "sum_open_interest_value": to_scaled("6858675634.70625"),
        "count_toptrader_long_short_ratio": to_scaled("1.28623339"),
        "sum_toptrader_long_short_ratio": to_scaled("1.471124"),
        "count_long_short_ratio": to_scaled("1.19972635"),
        "sum_taker_long_short_vol_ratio": to_scaled("1.558272"),
    }


def _event(
    ts_ms: int,
    kind: CollectorEventKind = CollectorEventKind.HEARTBEAT,
    *,
    stream: str = "collector",
    detail: str = "",
    downtime_ms: int = 0,
) -> dict[str, Any]:
    return {
        "ts_ms": ts_ms,
        "kind": kind.value,
        "stream": stream,
        "detail": detail,
        "downtime_ms": downtime_ms,
    }


def _record(
    ts_ms: int,
    kind: CollectorEventKind,
    *,
    stream: str = "collector",
    downtime_ms: int = 0,
) -> CollectorEventRecord:
    return CollectorEventRecord(
        ts_ms=ts_ms, kind=kind, stream=stream, detail="", downtime_ms=downtime_ms
    )


# --------------------------------------------------------------------------------------
# Policy
# --------------------------------------------------------------------------------------


class TestGapPolicy:
    def test_exactly_three_modes(self) -> None:
        """Spec 4.5 lists three, and a fourth would have to be specified, not invented."""
        assert [p.value for p in GapPolicy] == ["STRICT", "HALT_TRADING", "SKIP"]

    def test_interpolation_is_not_offered(self) -> None:
        """Interpolation manufactures prices that never traded; there is no code path.

        Asserted structurally rather than trusted to the docstring: once a helper called
        `fill_gap` exists, someone will call it, and an interpolated price is
        indistinguishable from an observed one the moment it is written.
        """
        assert not any(
            token in name.lower()
            for name in dir(gaps)
            for token in ("interpolat", "impute", "fill_gap", "backfill")
        )

    def test_strict_refuses_to_run(self) -> None:
        assert GapPolicy.STRICT.refuses_to_run
        assert not GapPolicy.HALT_TRADING.refuses_to_run
        assert not GapPolicy.SKIP.refuses_to_run

    def test_skip_is_the_only_mode_needing_confirmation_and_a_badge(self) -> None:
        assert GapPolicy.SKIP.requires_typed_confirmation
        assert GapPolicy.SKIP.run_flag == "GAP_SKIPPED"
        assert GapPolicy.STRICT.run_flag is None
        assert GapPolicy.HALT_TRADING.run_flag is None
        assert not GapPolicy.HALT_TRADING.requires_typed_confirmation

    @pytest.mark.parametrize("text", ["STRICT", "strict", "  Skip  ", "halt_trading"])
    def test_parse_accepts_configured_values(self, text: str) -> None:
        assert parse_gap_policy(text) in set(GapPolicy)

    @pytest.mark.parametrize("bad", [None, "", "   "])
    def test_parse_refuses_to_default(self, bad: str | None) -> None:
        """Spec 4.5: the mode is chosen in run config and never defaulted silently."""
        with pytest.raises(ValueError, match="never defaulted"):
            parse_gap_policy(bad)

    def test_parse_rejects_unknown(self) -> None:
        with pytest.raises(ValueError, match="unknown gap policy"):
            parse_gap_policy("INTERPOLATE")


# --------------------------------------------------------------------------------------
# Formatting helpers
# --------------------------------------------------------------------------------------


class TestFormatting:
    @pytest.mark.parametrize(
        ("ts_ms", "expected"),
        [
            (0, "1970-01-01T00:00:00.000Z"),
            (T0, "2024-01-01T00:00:00.000Z"),
            (T0 + 3 * HOUR_MS + 5 * MIN_MS, "2024-01-01T03:05:00.000Z"),
            (T0 - 1, "2023-12-31T23:59:59.999Z"),
            # Leap day, the classic civil-calendar off-by-one.
            (1_709_164_800_000, "2024-02-29T00:00:00.000Z"),
        ],
    )
    def test_format_ms(self, ts_ms: int, expected: str) -> None:
        assert format_ms(ts_ms) == expected

    def test_format_ms_is_timezone_independent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A report that prints a timestamp an hour off sends someone hunting a phantom."""
        monkeypatch.setenv("TZ", "Pacific/Auckland")
        assert format_ms(T0) == "2024-01-01T00:00:00.000Z"

    @pytest.mark.parametrize(
        ("ms", "expected"),
        [
            (0, "0ms"),
            (999, "999ms"),
            (1_000, "1s"),
            (1_500, "1.500s"),
            (60_000, "1m 00s"),
            (60_001, "1m 00.001s"),
            (3_600_000, "1h 00m 00s"),
            (86_400_000, "1d 00h 00m 00s"),
            (90_061_000, "1d 01h 01m 01s"),
        ],
    )
    def test_format_duration(self, ms: int, expected: str) -> None:
        assert format_duration(ms) == expected

    def test_format_duration_rejects_negative(self) -> None:
        with pytest.raises(ValueError):
            format_duration(-1)

    @pytest.mark.parametrize(
        ("text", "expected"),
        [("1m", 60_000), ("5m", 300_000), ("4h", 14_400_000), ("1d", 86_400_000), ("1s", 1_000)],
    )
    def test_interval_to_ms(self, text: str, expected: int) -> None:
        assert interval_to_ms(text) == expected

    @pytest.mark.parametrize("bad", ["", "m", "0m", "1w", "1", "-1m", "1.5m"])
    def test_interval_to_ms_refuses_to_guess(self, bad: str) -> None:
        """The interval is the denominator of the whole kline rule; a fallback to 1m would
        report an entire range as missing and look like catastrophic data loss."""
        with pytest.raises(ValueError):
            interval_to_ms(bad)


# --------------------------------------------------------------------------------------
# The Gap value type
# --------------------------------------------------------------------------------------


class TestGap:
    def test_duration_is_derived(self) -> None:
        """Stored separately it could contradict its own endpoints, and it is the number
        an operator reads first."""
        gap = Gap("klines", SYMBOL, T0, T0 + 3 * MIN_MS, GapKind.MISSING_BARS, "x")
        assert gap.duration_ms == 3 * MIN_MS

    def test_rejects_a_gap_that_runs_backwards(self) -> None:
        with pytest.raises(ValueError, match="forward in time"):
            Gap("klines", SYMBOL, T0, T0, GapKind.MISSING_BARS, "x")

    def test_explanation_round_trip(self) -> None:
        gap = Gap("aggTrades", SYMBOL, T0, T0 + MIN_MS, GapKind.TICK_SILENCE, "x")
        assert not gap.explained
        explained = gap.with_explanation("RESTART")
        assert explained.explained
        assert explained.explanation == "RESTART"
        assert not gap.explained, "the original must be untouched"

    def test_is_frozen(self) -> None:
        """A gap that a caller can edit is a gap report that can disagree with the lake."""
        gap = Gap("klines", SYMBOL, T0, T0 + MIN_MS, GapKind.MISSING_BARS, "x")
        with pytest.raises(FrozenInstanceError):
            gap.start_ms = 0  # type: ignore[misc]


# --------------------------------------------------------------------------------------
# Rule 1 -- klines
# --------------------------------------------------------------------------------------


class TestKlineGaps:
    def test_zero_volume_bar_is_data_not_a_gap(self, tmp_path: Path) -> None:
        """R21, the named trap. Binance emits zero-volume bars through illiquid periods
        rather than omitting them, so an hour of them is a complete hour of data."""
        _write_klines(tmp_path, list(range(60)), zero_volume=frozenset(range(10, 40)))

        found, coverage = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS)

        assert found == ()
        assert coverage.rows == 60
        assert coverage.expected == 60
        assert coverage.zero_volume_bars == 30

    def test_absent_bar_is_a_gap(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, [m for m in range(60) if m != 10])

        found, coverage = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS)

        assert len(found) == 1
        assert found[0].start_ms == T0 + 10 * MIN_MS
        assert found[0].end_ms == T0 + 11 * MIN_MS
        assert found[0].duration_ms == MIN_MS
        assert found[0].kind is GapKind.MISSING_BARS
        assert coverage.rows == 59
        assert coverage.expected == 60

    def test_a_zero_volume_bar_and_a_missing_bar_are_reported_differently(
        self, tmp_path: Path
    ) -> None:
        """The whole of R21 in one lake: minute 20 traded nothing, minute 40 is absent.

        A detector that keyed presence on volume would report both, or neither. Exactly one
        is a gap, and it is the one with no row.
        """
        _write_klines(
            tmp_path, [m for m in range(60) if m != 40], zero_volume=frozenset({20})
        )

        found, coverage = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS)

        assert [g.start_ms for g in found] == [T0 + 40 * MIN_MS]
        assert coverage.zero_volume_bars == 1

    def test_adjacent_bars_at_exactly_one_interval_are_not_a_gap(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, [0, 1])
        found, _ = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 2 * MIN_MS)
        assert found == ()

    def test_two_intervals_apart_is_exactly_one_missing_bar(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, [0, 2])
        found, _ = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 3 * MIN_MS)
        assert len(found) == 1
        assert found[0].duration_ms == MIN_MS
        assert "1 bar(s) absent" in found[0].detail

    def test_run_of_missing_bars_counted_exactly(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, [m for m in range(60) if not 10 <= m <= 19])
        found, _ = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS)
        assert len(found) == 1
        assert found[0].duration_ms == 10 * MIN_MS
        assert "10 bar(s) absent" in found[0].detail

    def test_leading_gap_relative_to_the_requested_range(self, tmp_path: Path) -> None:
        """A range that starts before the data is a gap, not a shorter range.

        Only a pairwise scan would miss this: the first bar present has nothing before it
        to be compared against.
        """
        _write_klines(tmp_path, list(range(5, 60)))
        found, _ = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS)
        assert len(found) == 1
        assert (found[0].start_ms, found[0].end_ms) == (T0, T0 + 5 * MIN_MS)

    def test_trailing_gap_relative_to_the_requested_range(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, list(range(55)))
        found, _ = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS)
        assert len(found) == 1
        assert (found[0].start_ms, found[0].end_ms) == (T0 + 55 * MIN_MS, T0 + 60 * MIN_MS)

    def test_empty_range_within_data_reports_the_whole_range(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, list(range(10)))
        found, coverage = detect_kline_gaps(
            tmp_path, SYMBOL, T0 + 20 * MIN_MS, T0 + 30 * MIN_MS
        )
        assert len(found) == 1
        assert found[0].duration_ms == 10 * MIN_MS
        assert coverage.rows == 0
        assert coverage.expected == 10

    def test_absent_dataset_reports_the_whole_range(self, tmp_path: Path) -> None:
        found, coverage = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS)
        assert len(found) == 1
        assert found[0].duration_ms == 60 * MIN_MS
        assert "not present in lake" in found[0].detail
        assert coverage.rows == 0

    def test_mark_price_klines_always_zero_volume_are_not_gaps(self, tmp_path: Path) -> None:
        """A mark price is a computed index: its volume columns are 0 in every row ever
        published. Presence keyed on volume would report all of history as missing."""
        _write_klines(
            tmp_path,
            list(range(60)),
            zero_volume=frozenset(range(60)),
            dataset="markPriceKlines",
        )
        found, coverage = detect_kline_gaps(
            tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS, dataset="markPriceKlines"
        )
        assert found == ()
        assert coverage.zero_volume_bars == 60

    def test_off_grid_bars_raise_rather_than_being_reported_as_gaps(
        self, tmp_path: Path
    ) -> None:
        """An open time off the interval grid means the range was ingested at a different
        interval. Every count derived from a broken grid would be fiction (spec 1.4)."""
        _write(tmp_path, "klines", [_bar(T0), _bar(T0 + 30_000), _bar(T0 + MIN_MS)])
        with pytest.raises(GapDetectionError, match="not a multiple"):
            detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 3 * MIN_MS)

    def test_duplicate_bars_are_counted_not_reported_as_gaps(self, tmp_path: Path) -> None:
        """A partition written twice is a bookkeeping problem, not a missing bar."""
        _write(tmp_path, "klines", [_bar(T0), _bar(T0), _bar(T0 + MIN_MS)])
        found, coverage = detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 2 * MIN_MS)
        assert found == ()
        assert coverage.rows == 2
        assert coverage.duplicate_rows == 1

    def test_interval_comes_from_the_partition_layout(self, tmp_path: Path) -> None:
        """The grid used to judge coverage is the same string that names the `interval=`
        directory, so the two cannot drift apart."""
        _write_klines(tmp_path, [0, 2])
        assert detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 3 * MIN_MS)[0][0].duration_ms == MIN_MS
        # Asking for a coarser grid over 1 m data must fail loudly rather than silently
        # rebase the expected count.
        with pytest.raises(GapDetectionError):
            detect_kline_gaps(tmp_path, SYMBOL, T0, T0 + 3 * MIN_MS, interval="5m")

    def test_backwards_range_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="forward in time"):
            detect_kline_gaps(tmp_path, SYMBOL, T0 + MIN_MS, T0)


# --------------------------------------------------------------------------------------
# Rule 2 -- tick datasets cross-checked against kline volume
# --------------------------------------------------------------------------------------


class TestTickGaps:
    """The cross-check is the rule, so the two halves of it are tested against the same
    tick data with only the kline volumes changed. Anything that flags one and not the
    other is reading the ticks; anything that flags both is not reading the klines."""

    @staticmethod
    def _ticks_with_silence(root: Path, silent: range) -> None:
        _write(
            root,
            "aggTrades",
            [
                _trade(T0 + m * MIN_MS + 500, agg_id=m)
                for m in range(60)
                if m not in silent
            ],
        )

    def test_silence_during_traded_klines_is_flagged(self, tmp_path: Path) -> None:
        self._ticks_with_silence(tmp_path, range(12, 19))
        _write_klines(tmp_path, list(range(60)))

        found, coverage = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS, dataset="aggTrades"
        )

        assert len(found) == 1
        assert found[0].kind is GapKind.TICK_SILENCE
        assert found[0].start_ms == T0 + 11 * MIN_MS + 500
        assert found[0].end_ms == T0 + 19 * MIN_MS + 500
        assert "7 kline bar(s) inside the window traded" in found[0].detail
        assert coverage.rows == 53

    def test_the_identical_silence_during_zero_volume_klines_is_not_flagged(
        self, tmp_path: Path
    ) -> None:
        """Byte-identical tick data to the test above. A quiet market is data.

        Without the cross-check this is the false positive that fills the report with
        noise until the report stops being read, which is how the next real gap ships.
        """
        self._ticks_with_silence(tmp_path, range(12, 19))
        _write_klines(tmp_path, list(range(60)), zero_volume=frozenset(range(60)))

        found, _ = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS, dataset="aggTrades"
        )

        assert found == ()

    def test_partial_quiet_still_flags_when_any_contained_bar_traded(
        self, tmp_path: Path
    ) -> None:
        """One traded bar inside the silence is enough: those trades happened and we did
        not record them."""
        self._ticks_with_silence(tmp_path, range(12, 19))
        _write_klines(
            tmp_path,
            list(range(60)),
            zero_volume=frozenset(m for m in range(60) if m != 15),
        )
        found, _ = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS, dataset="aggTrades"
        )
        assert len(found) == 1
        assert "1 kline bar(s) inside the window traded" in found[0].detail

    def test_threshold_boundary(self, tmp_path: Path) -> None:
        """Exactly at the threshold is not a gap; one millisecond over is.

        A five-minute threshold is used rather than the 60 s default so that the boundary
        being tested is the threshold itself and not the containment rule -- a silence of
        exactly 60 s never contains a whole 1 m bar, so the default would conflate the two.
        """
        threshold = 5 * MIN_MS
        _write_klines(tmp_path, list(range(6)))
        _write(tmp_path, "aggTrades", [_trade(T0, 1), _trade(T0 + threshold, 2)])

        found, _ = detect_tick_gaps(
            tmp_path,
            SYMBOL,
            T0,
            T0 + threshold + 1,
            dataset="aggTrades",
            threshold_ms=threshold,
        )
        assert found == (), "an interval equal to the threshold does not exceed it"

    def test_one_millisecond_over_the_threshold_is_a_gap(self, tmp_path: Path) -> None:
        threshold = 5 * MIN_MS
        _write_klines(tmp_path, list(range(6)))
        _write(tmp_path, "aggTrades", [_trade(T0, 1), _trade(T0 + threshold + 1, 2)])

        found, _ = detect_tick_gaps(
            tmp_path,
            SYMBOL,
            T0,
            T0 + threshold + 2,
            dataset="aggTrades",
            threshold_ms=threshold,
        )
        assert len(found) == 1
        assert found[0].duration_ms == threshold + 1

    def test_only_bars_wholly_inside_the_silence_count_as_evidence(
        self, tmp_path: Path
    ) -> None:
        """The bars containing the records either side carry volume from those very
        records, so counting them would make every silence self-justifying.

        Ticks sit exactly on bar boundaries at minutes 0 and 2. Bars 0 and 2 traded
        heavily; bar 1 -- the only one wholly inside the silence -- did not. Nothing is
        missing here, and a detector that counted the endpoint bars would say otherwise.
        """
        _write(tmp_path, "aggTrades", [_trade(T0, 1), _trade(T0 + 2 * MIN_MS, 2)])
        _write_klines(tmp_path, [0, 1, 2], zero_volume=frozenset({1}))

        found, _ = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 3 * MIN_MS, dataset="aggTrades"
        )
        assert found == ()

    def test_the_wholly_contained_bar_is_what_flags_it(self, tmp_path: Path) -> None:
        """The mirror of the test above: same ticks, same bars, only bar 1's volume moved.

        Bars 0 and 2 are now silent and bar 1 traded, which is the one arrangement that
        proves trades happened while we recorded nothing.
        """
        _write(tmp_path, "aggTrades", [_trade(T0, 1), _trade(T0 + 2 * MIN_MS, 2)])
        _write_klines(tmp_path, [0, 1, 2], zero_volume=frozenset({0, 2}))

        found, _ = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 3 * MIN_MS, dataset="aggTrades"
        )
        assert len(found) == 1
        assert found[0].start_ms == T0
        assert found[0].end_ms == T0 + 2 * MIN_MS

    def test_leading_silence_at_the_range_edge(self, tmp_path: Path) -> None:
        """No record precedes the range start, so the bar opening exactly at it counts."""
        _write_klines(tmp_path, list(range(10)))
        _write(tmp_path, "aggTrades", [_trade(T0 + 5 * MIN_MS, 1)])

        found, _ = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 6 * MIN_MS, dataset="aggTrades"
        )
        assert len(found) == 1
        assert (found[0].start_ms, found[0].end_ms) == (T0, T0 + 5 * MIN_MS)

    def test_trailing_silence_at_the_range_edge(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, list(range(10)))
        _write(tmp_path, "aggTrades", [_trade(T0, 1)])

        found, _ = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 6 * MIN_MS, dataset="aggTrades"
        )
        assert len(found) == 1
        assert (found[0].start_ms, found[0].end_ms) == (T0, T0 + 6 * MIN_MS)

    def test_trailing_silence_over_a_quiet_tail_is_not_flagged(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, list(range(10)), zero_volume=frozenset(range(1, 10)))
        _write(tmp_path, "aggTrades", [_trade(T0, 1)])

        found, _ = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 6 * MIN_MS, dataset="aggTrades"
        )
        assert found == ()

    def test_absent_tick_dataset_reports_the_whole_range(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, list(range(10)))
        found, coverage = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 10 * MIN_MS, dataset="bookTicker"
        )
        assert len(found) == 1
        assert found[0].duration_ms == 10 * MIN_MS
        assert coverage.rows == 0

    def test_absent_klines_raise_rather_than_guessing(self, tmp_path: Path) -> None:
        """Spec 4.5 defines a tick gap only relative to kline volume. Reporting every
        silence and reporting none of them are both guesses, so neither is offered."""
        self._ticks_with_silence(tmp_path, range(12, 19))
        with pytest.raises(KlinesUnavailable, match="klines"):
            detect_tick_gaps(tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS, dataset="aggTrades")

    def test_a_window_no_klines_cover_is_reported_with_that_stated(
        self, tmp_path: Path
    ) -> None:
        """Both datasets silent at once is not evidence of health.

        The klines exist for the range as a whole -- so the rule can run -- but not inside
        this particular window, which means the cross-check has nothing to say. The gap is
        reported anyway, with the inconclusiveness spelled out rather than resolved in
        either direction.
        """
        _write_klines(tmp_path, [0, 1, 2, 3, 4] + list(range(26, 60)))
        _write(
            tmp_path,
            "aggTrades",
            [_trade(T0 + m * MIN_MS + 500, agg_id=m) for m in [4, 26]],
        )

        found, _ = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS, dataset="aggTrades"
        )
        silence = [g for g in found if g.start_ms == T0 + 4 * MIN_MS + 500]
        assert len(silence) == 1
        assert "cross-check could not be applied" in silence[0].detail

    def test_rejects_a_non_positive_threshold(self, tmp_path: Path) -> None:
        _write_klines(tmp_path, [0])
        with pytest.raises(ValueError, match="threshold"):
            detect_tick_gaps(
                tmp_path, SYMBOL, T0, T0 + MIN_MS, dataset="aggTrades", threshold_ms=0
            )

    def test_duplicate_trades_are_counted_by_exchange_id(self, tmp_path: Path) -> None:
        """Finding H26: tick coverage never populated `duplicate_rows`, so a partition
        holding the same trades twice -- the bulk/collector overlap, or a re-flushed
        buffer -- was invisible everywhere. Identity is `agg_id`, not `ts_ms`."""
        _write_klines(tmp_path, [0, 1])
        _write(
            tmp_path,
            "aggTrades",
            [_trade(T0 + 100, agg_id=1), _trade(T0 + 100, agg_id=1), _trade(T0 + 200, agg_id=2)],
        )
        _, coverage = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 2 * MIN_MS, dataset="aggTrades"
        )
        assert coverage.rows == 3
        assert coverage.duplicate_rows == 1

    def test_two_trades_in_one_millisecond_are_not_duplicates(self, tmp_path: Path) -> None:
        """A busy millisecond is data; only a repeated exchange id is a re-write."""
        _write_klines(tmp_path, [0, 1])
        _write(
            tmp_path,
            "aggTrades",
            [_trade(T0 + 100, agg_id=1), _trade(T0 + 100, agg_id=2)],
        )
        _, coverage = detect_tick_gaps(
            tmp_path, SYMBOL, T0, T0 + 2 * MIN_MS, dataset="aggTrades"
        )
        assert coverage.duplicate_rows == 0

    def test_duplicate_funding_settlements_are_counted(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0, 8), _funding_row(T0, 8), _funding_row(T0 + 8 * HOUR_MS, 8)],
        )
        _, coverage = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 8 * HOUR_MS + 1)
        assert coverage.duplicate_rows == 1


class TestPartitionPruning:
    """Finding M21: `_LakeReader.source` handed DuckDB a bare glob, so every rule's scan
    opened the symbol's whole history to consult footer statistics -- and a sweep calling
    `detect_gaps` per grid point performed thousands of full-lake scans."""

    def test_the_source_expression_filters_by_partition_path(self, tmp_path: Path) -> None:
        """Rows outside the requested partitions must be excluded by the *path* filter
        alone -- no timestamp column in the query -- proving the predicate is in the scan
        expression rather than left to the caller's WHERE clause."""
        _write_klines(tmp_path, [0])  # lands in year=2024/month=01
        _write_klines(tmp_path, [0], base_ms=T0 + 40 * DAY_MS)  # year=2024/month=02

        lake = gaps._LakeReader(tmp_path)
        pruned = lake.source("klines", SYMBOL, start_ms=T0, end_ms=T0 + DAY_MS)
        (count,) = lake.query(f"SELECT count(*) FROM {pruned}")[0]
        assert count == 1, "the February file must be excluded by path"

        unpruned = lake.source("klines", SYMBOL)
        (count_all,) = lake.query(f"SELECT count(*) FROM {unpruned}")[0]
        assert count_all == 2

    def test_the_expression_declares_hive_partitioning(self, tmp_path: Path) -> None:
        """Path pruning only exists if DuckDB knows the layout is Hive; a bare glob has
        no partition columns to filter on."""
        _write_klines(tmp_path, [0])
        lake = gaps._LakeReader(tmp_path)
        expression = lake.source("klines", SYMBOL, start_ms=T0, end_ms=T0 + DAY_MS)
        assert "hive_partitioning := true" in expression
        assert '"year"' in expression and '"month"' in expression


# --------------------------------------------------------------------------------------
# Rule 3 -- funding
# --------------------------------------------------------------------------------------


class TestFundingGaps:
    def test_regular_eight_hour_schedule_has_no_gaps(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0 + i * 8 * HOUR_MS, 8) for i in range(4)],
        )
        found, coverage = detect_funding_gaps(
            tmp_path, SYMBOL, T0, T0 + 24 * HOUR_MS + 1
        )
        assert found == ()
        assert coverage.rows == 4

    def test_missed_eight_hour_settlement_is_a_gap(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0, 8), _funding_row(T0 + 16 * HOUR_MS, 8)],
        )
        found, _ = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 16 * HOUR_MS + 1)
        assert len(found) == 1
        assert found[0].kind is GapKind.MISSED_SETTLEMENT
        assert found[0].duration_ms == 16 * HOUR_MS

    def test_four_hour_symbol_gap_that_a_hardcoded_eight_would_miss(
        self, tmp_path: Path
    ) -> None:
        """Review finding R17, made concrete.

        This symbol settles every four hours, so its limit is six. The missed settlement
        leaves eight hours between rows: over six, under the twelve a hardcoded eight-hour
        assumption would allow. A detector that assumed 8 reports this range as clean.
        """
        _write(
            tmp_path,
            "funding",
            [
                _funding_row(T0, 4),
                _funding_row(T0 + 4 * HOUR_MS, 4),
                _funding_row(T0 + 12 * HOUR_MS, 4),
                _funding_row(T0 + 16 * HOUR_MS, 4),
            ],
        )
        found, _ = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 16 * HOUR_MS + 1)

        assert len(found) == 1
        assert found[0].start_ms == T0 + 4 * HOUR_MS
        assert found[0].duration_ms == 8 * HOUR_MS
        assert "4h interval read from the data" in found[0].detail

    def test_four_hour_symbol_on_schedule_has_no_gaps(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0 + i * 4 * HOUR_MS, 4) for i in range(6)],
        )
        found, _ = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 20 * HOUR_MS + 1)
        assert found == ()

    @pytest.mark.parametrize(
        ("hours", "elapsed_ms", "expect_gap"),
        [
            (8, 12 * HOUR_MS, False),
            (8, 12 * HOUR_MS + 1, True),
            (4, 6 * HOUR_MS, False),
            (4, 6 * HOUR_MS + 1, True),
            (1, HOUR_MS + HOUR_MS // 2, False),
            (1, HOUR_MS + HOUR_MS // 2 + 1, True),
        ],
    )
    def test_one_point_five_multiplier_boundary(
        self, tmp_path: Path, hours: int, elapsed_ms: int, expect_gap: bool
    ) -> None:
        """Exactly `1.5 x` the interval does not exceed it; one millisecond more does.

        Computed as `hours * 3_600_000 * 3 // 2` rather than in floating point, so the
        boundary is the same integer on every machine.
        """
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0, hours), _funding_row(T0 + elapsed_ms, hours)],
        )
        found, _ = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + elapsed_ms + 1)
        assert bool(found) is expect_gap

    def test_schedule_change_does_not_manufacture_a_gap(self, tmp_path: Path) -> None:
        """Binance has moved live symbols between schedules. The archive does not say
        which side of the change the elapsed period belongs to, so the longer interval
        governs -- a documented schedule change is not a missing settlement.

        Reading the *later* row's four hours here would allow only six and report this
        entirely normal eight-hour period as a gap.
        """
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0, 8), _funding_row(T0 + 8 * HOUR_MS, 4)],
        )
        found, _ = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 8 * HOUR_MS + 1)
        assert found == ()

    def test_schedule_change_still_catches_a_real_gap(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0, 8), _funding_row(T0 + 13 * HOUR_MS, 4)],
        )
        found, _ = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 13 * HOUR_MS + 1)
        assert len(found) == 1

    def test_settlement_before_the_range_anchors_a_leading_gap(self, tmp_path: Path) -> None:
        """Without the anchor the first in-range settlement has nothing to pair with and a
        gap that opens at the range start is invisible."""
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0 - 8 * HOUR_MS, 8), _funding_row(T0 + 16 * HOUR_MS, 8)],
        )
        found, coverage = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 16 * HOUR_MS + 1)

        assert len(found) == 1
        assert found[0].start_ms == T0 - 8 * HOUR_MS
        assert found[0].duration_ms == 24 * HOUR_MS
        assert coverage.rows == 1, "only the in-range settlement counts as coverage"

    def test_trailing_gap_after_the_last_settlement(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0, 8), _funding_row(T0 + 8 * HOUR_MS, 8)],
        )
        found, _ = detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 24 * HOUR_MS)
        assert len(found) == 1
        assert found[0].start_ms == T0 + 8 * HOUR_MS
        assert found[0].end_ms == T0 + 24 * HOUR_MS

    def test_range_entirely_after_the_last_settlement(self, tmp_path: Path) -> None:
        _write(tmp_path, "funding", [_funding_row(T0, 8)])
        found, coverage = detect_funding_gaps(
            tmp_path, SYMBOL, T0 + 24 * HOUR_MS, T0 + 48 * HOUR_MS
        )
        assert len(found) == 1
        assert coverage.rows == 0

    @pytest.mark.parametrize("hours", [0, -8])
    def test_non_positive_interval_raises(self, tmp_path: Path, hours: int) -> None:
        """A zero interval makes the threshold zero and every pair a gap. That is a
        corrupt column, not a finding."""
        _write(
            tmp_path,
            "funding",
            [_funding_row(T0, hours), _funding_row(T0 + 8 * HOUR_MS, hours)],
        )
        with pytest.raises(GapDetectionError, match="funding interval"):
            detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 8 * HOUR_MS + 1)

    def test_absent_funding_data_raises(self, tmp_path: Path) -> None:
        """The interval that defines a funding gap lives in the data, so with no rows
        there is no threshold and nothing can be either found or ruled out."""
        with pytest.raises(GapDetectionError, match="no funding data"):
            detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 24 * HOUR_MS)

    def test_range_entirely_before_any_settlement_raises(self, tmp_path: Path) -> None:
        _write(tmp_path, "funding", [_funding_row(T0 + 48 * HOUR_MS, 8)])
        with pytest.raises(GapDetectionError, match="cannot be assumed"):
            detect_funding_gaps(tmp_path, SYMBOL, T0, T0 + 24 * HOUR_MS)


# --------------------------------------------------------------------------------------
# Rule 5 -- metrics (audit finding L7)
# --------------------------------------------------------------------------------------

FIVE_MIN_MS = 300_000


class TestMetricsGaps:
    """The five-minute open-interest series, judged by the collector's own threshold."""

    @staticmethod
    def _samples(root: Path, offsets: list[int]) -> None:
        _write(root, "metrics", [_metrics_row(T0 + o) for o in offsets])

    def test_a_regular_series_has_no_gaps(self, tmp_path: Path) -> None:
        self._samples(tmp_path, [i * FIVE_MIN_MS for i in range(12)])
        found, coverage = detect_metrics_gaps(tmp_path, SYMBOL, T0, T0 + HOUR_MS)
        assert found == ()
        assert coverage.rows == 12
        assert coverage.duplicate_rows == 0

    def test_a_dead_hour_is_a_gap(self, tmp_path: Path) -> None:
        """The finding's scenario at small scale: the poller dies, the series stops, and
        before this rule existed nothing anywhere reported it."""
        offsets = [i * FIVE_MIN_MS for i in range(24) if not 6 <= i <= 17]
        self._samples(tmp_path, offsets)
        found, _ = detect_metrics_gaps(tmp_path, SYMBOL, T0, T0 + 2 * HOUR_MS)
        assert len(found) == 1
        assert found[0].kind is GapKind.METRIC_SILENCE
        assert found[0].start_ms == T0 + 5 * FIVE_MIN_MS
        assert found[0].end_ms == T0 + 18 * FIVE_MIN_MS

    def test_threshold_boundary(self, tmp_path: Path) -> None:
        """Exactly three cadences apart is not a gap; one millisecond more is."""
        self._samples(tmp_path, [0, 3 * FIVE_MIN_MS])
        found, _ = detect_metrics_gaps(tmp_path, SYMBOL, T0, T0 + 3 * FIVE_MIN_MS + 1)
        assert found == ()

        self._samples(tmp_path, [4 * FIVE_MIN_MS, 7 * FIVE_MIN_MS + 1])
        found, _ = detect_metrics_gaps(
            tmp_path, SYMBOL, T0 + 4 * FIVE_MIN_MS, T0 + 7 * FIVE_MIN_MS + 2
        )
        assert len(found) == 1
        assert found[0].duration_ms == 3 * FIVE_MIN_MS + 1

    def test_unsnapped_live_timestamps_are_not_gaps(self, tmp_path: Path) -> None:
        """The live poller records the endpoint's own instants, off the archive grid by
        design; ordinary jitter must not read as silence."""
        self._samples(tmp_path, [0, FIVE_MIN_MS + 17_345, 2 * FIVE_MIN_MS + 3_001])
        found, _ = detect_metrics_gaps(tmp_path, SYMBOL, T0, T0 + 3 * FIVE_MIN_MS)
        assert found == ()

    def test_leading_and_trailing_silence_at_the_range_edges(self, tmp_path: Path) -> None:
        self._samples(tmp_path, [4 * FIVE_MIN_MS, 5 * FIVE_MIN_MS])
        found, _ = detect_metrics_gaps(tmp_path, SYMBOL, T0, T0 + 10 * FIVE_MIN_MS)
        assert [(g.start_ms, g.end_ms) for g in found] == [
            (T0, T0 + 4 * FIVE_MIN_MS),
            (T0 + 5 * FIVE_MIN_MS, T0 + 10 * FIVE_MIN_MS),
        ]

    def test_no_rows_in_range_is_one_whole_range_gap(self, tmp_path: Path) -> None:
        self._samples(tmp_path, [0])
        found, coverage = detect_metrics_gaps(
            tmp_path, SYMBOL, T0 + DAY_MS, T0 + DAY_MS + HOUR_MS
        )
        assert len(found) == 1
        assert found[0].duration_ms == HOUR_MS
        assert coverage.rows == 0

    def test_an_absent_dataset_raises_rather_than_reporting_an_outage(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(GapDetectionError, match="no metrics data"):
            detect_metrics_gaps(tmp_path, SYMBOL, T0, T0 + HOUR_MS)

    def test_the_absent_dataset_lands_in_unevaluated_not_clean(self, tmp_path: Path) -> None:
        report = detect_gaps(tmp_path, SYMBOL, T0, T0 + HOUR_MS, datasets=("metrics",))
        assert report.gaps == ()
        assert [u.dataset for u in report.unevaluated] == ["metrics"]

    def test_duplicate_samples_are_counted_not_read_as_coverage(
        self, tmp_path: Path
    ) -> None:
        """Finding H26's tie, seen from the detector's side: a restarted poller that
        re-recorded one instant must show up as a duplicate, and the duplicated instant
        must not double-count towards spacing."""
        self._samples(tmp_path, [0, 0, FIVE_MIN_MS])
        found, coverage = detect_metrics_gaps(
            tmp_path, SYMBOL, T0, T0 + FIVE_MIN_MS + 1
        )
        assert found == ()
        assert coverage.duplicate_rows == 1

    def test_a_metrics_gap_with_a_recorded_poller_outage_is_explained(
        self, tmp_path: Path
    ) -> None:
        """End to end through `detect_gaps`: the OI poller's DISCONNECT/RECONNECT pair,
        stamped with the dataset name, accounts for the hole it caused."""
        offsets = [i * FIVE_MIN_MS for i in range(24) if not 6 <= i <= 17]
        self._samples(tmp_path, offsets)
        _write(
            tmp_path,
            "collectorEvents",
            [
                _event(
                    T0 + 5 * FIVE_MIN_MS + 1_000,
                    CollectorEventKind.DISCONNECT,
                    stream="metrics",
                    detail="poll failed (1 consecutive)",
                ),
                _event(
                    T0 + 18 * FIVE_MIN_MS,
                    CollectorEventKind.RECONNECT,
                    stream="metrics",
                    detail="recovered",
                    downtime_ms=13 * FIVE_MIN_MS,
                ),
            ],
            symbol=None,
        )
        report = detect_gaps(tmp_path, SYMBOL, T0, T0 + 2 * HOUR_MS, datasets=("metrics",))
        assert len(report.gaps) == 1
        assert report.gaps[0].explained
        assert report.unexplained == ()


# --------------------------------------------------------------------------------------
# Rule 4 -- the collector event stream
# --------------------------------------------------------------------------------------


class TestCollectorEventGaps:
    def test_steady_heartbeats_have_no_gaps(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "collectorEvents",
            [_event(T0 + i * 10_000) for i in range(30)],
            symbol=None,
        )
        found, coverage = detect_collector_event_gaps(tmp_path, T0, T0 + 300_000)
        assert found == ()
        assert coverage.rows == 30
        assert coverage.symbol is None

    def test_silence_boundary(self, tmp_path: Path) -> None:
        """Three missed beats is the limit. One missed beat is a slow Parquet flush, and
        reporting that would train an operator to ignore the whole report."""
        _write(
            tmp_path,
            "collectorEvents",
            [_event(T0), _event(T0 + 30_000), _event(T0 + 40_000)],
            symbol=None,
        )
        found, _ = detect_collector_event_gaps(tmp_path, T0, T0 + 50_000)
        assert found == ()

    def test_one_millisecond_over_the_silence_limit_is_a_gap(self, tmp_path: Path) -> None:
        _write(
            tmp_path,
            "collectorEvents",
            [_event(T0), _event(T0 + 30_001), _event(T0 + 40_000)],
            symbol=None,
        )
        found, _ = detect_collector_event_gaps(tmp_path, T0, T0 + 50_000)
        assert len(found) == 1
        assert found[0].duration_ms == 30_001
        assert found[0].kind is GapKind.COLLECTOR_OUTAGE
        assert found[0].symbol is None

    def test_no_records_at_all_reports_the_whole_range(self, tmp_path: Path) -> None:
        found, coverage = detect_collector_event_gaps(tmp_path, T0, T0 + HOUR_MS)
        assert len(found) == 1
        assert found[0].duration_ms == HOUR_MS
        assert coverage.rows == 0

    def test_gap_matched_to_a_restart_is_explained(self, tmp_path: Path) -> None:
        """The collector leaves a state file behind when it dies and reports the downtime
        on the next start. That record is what turns a hole into an accounted-for one."""
        _write(
            tmp_path,
            "collectorEvents",
            [
                *[_event(T0 + i * 10_000) for i in range(11)],
                _event(
                    T0 + 400_000,
                    CollectorEventKind.RESTART,
                    detail="previous run ended without clean shutdown (pid 4242)",
                    downtime_ms=300_000,
                ),
                *[_event(T0 + 400_000 + i * 10_000) for i in range(1, 6)],
            ],
            symbol=None,
        )
        report = detect_gaps(
            tmp_path, SYMBOL, T0, T0 + 450_000, datasets=("collectorEvents",)
        )
        assert len(report.gaps) == 1
        assert report.gaps[0].explained
        assert "RESTART" in (report.gaps[0].explanation or "")
        assert report.unexplained == ()

    def test_the_same_gap_without_a_restart_is_a_real_failure(self, tmp_path: Path) -> None:
        """Identical timing, identical hole. Only the lifecycle record is gone, and that
        is precisely the difference the Phase 1b exit criterion turns on."""
        _write(
            tmp_path,
            "collectorEvents",
            [
                *[_event(T0 + i * 10_000) for i in range(11)],
                *[_event(T0 + 400_000 + i * 10_000) for i in range(6)],
            ],
            symbol=None,
        )
        report = detect_gaps(
            tmp_path, SYMBOL, T0, T0 + 450_000, datasets=("collectorEvents",)
        )
        assert len(report.gaps) == 1
        assert not report.gaps[0].explained
        assert len(report.unexplained) == 1

    def test_heartbeat_never_explains_a_gap(self, tmp_path: Path) -> None:
        """A heartbeat is evidence the collector was alive, which makes an adjacent gap
        more alarming, not less."""
        assert CollectorEventKind.HEARTBEAT not in EXPLAINING_KINDS

    def test_unknown_event_kind_raises(self, tmp_path: Path) -> None:
        """A record the detector cannot interpret may be the one that explains an outage;
        dropping it silently turns a real explanation into a phantom failure."""
        _write(
            tmp_path,
            "collectorEvents",
            [{"ts_ms": T0, "kind": "WOBBLE", "stream": "collector", "detail": "", "downtime_ms": 0}],
            symbol=None,
        )
        with pytest.raises(GapDetectionError, match="unknown kind"):
            load_collector_events(tmp_path, T0, T0 + 60_000)

    def test_events_are_loaded_from_beyond_the_range_edges(self, tmp_path: Path) -> None:
        """The record that explains an edge gap routinely falls outside the range: a
        collector that died before the range started writes its RESTART inside it."""
        _write(
            tmp_path,
            "collectorEvents",
            [_event(T0 - 20_000, CollectorEventKind.SHUTDOWN), _event(T0 + 5_000)],
            symbol=None,
        )
        events = load_collector_events(tmp_path, T0, T0 + 60_000)
        assert [e.ts_ms for e in events] == [T0 - 20_000, T0 + 5_000]

    def test_missing_event_dataset_loads_as_empty(self, tmp_path: Path) -> None:
        assert load_collector_events(tmp_path, T0, T0 + 60_000) == ()

    def test_macro_service_events_are_not_collector_liveness(self, tmp_path: Path) -> None:
        """Finding H24: `perplab macro` writes into the same collectorEvents partition.

        The market collector here dies at +100 s while the macro service keeps writing a
        record every 10 s to the end of the range. Before the fix those macro rows
        satisfied the liveness rule and an outage of every market dataset produced no
        COLLECTOR_OUTAGE at all; the macro process's uptime said nothing about the
        recorder's.
        """
        _write(
            tmp_path,
            "collectorEvents",
            [
                *[_event(T0 + ts) for ts in range(0, 110_000, 10_000)],
                *[
                    _event(
                        T0 + ts,
                        CollectorEventKind.DISCONNECT,
                        stream="macroGlobal",
                        detail="poll failed",
                    )
                    for ts in range(110_000, 600_000, 10_000)
                ],
            ],
            symbol=None,
        )
        found, coverage = detect_collector_event_gaps(tmp_path, T0, T0 + 600_000)
        assert len(found) == 1
        assert found[0].start_ms == T0 + 100_000
        assert found[0].end_ms == T0 + 600_000
        assert coverage.rows == 11, "coverage counts market-collector records only"

    def test_market_collector_records_still_count_as_liveness(self, tmp_path: Path) -> None:
        """The mirror control: the same cadence of records from the market collector's
        own streams is liveness, so nothing is reported."""
        _write(
            tmp_path,
            "collectorEvents",
            [
                *[_event(T0 + ts) for ts in range(0, 110_000, 10_000)],
                *[
                    _event(
                        T0 + ts,
                        CollectorEventKind.DISCONNECT,
                        stream="btcusdt@bookTicker,btcusdt@depth20@100ms",
                        detail="retry",
                    )
                    for ts in range(110_000, 600_000, 10_000)
                ],
            ],
            symbol=None,
        )
        found, _ = detect_collector_event_gaps(tmp_path, T0, T0 + 600_000)
        assert found == ()


class TestExplanationMatching:
    """`explain_gaps` as a pure function, so the matching window can be tested at its
    boundary without the tick timestamps and event timestamps having to be co-arranged."""

    GAP = Gap("aggTrades", SYMBOL, T0 + 10 * MIN_MS, T0 + 20 * MIN_MS, GapKind.TICK_SILENCE, "x")

    def test_disconnect_inside_the_gap_explains_it(self) -> None:
        events = (
            _record(T0 + 12 * MIN_MS, CollectorEventKind.DISCONNECT, stream="btcusdt@aggTrade"),
        )
        assert explain_gaps((self.GAP,), events)[0].explained

    def test_tolerance_boundary_before_the_gap(self) -> None:
        """A DISCONNECT is stamped when the socket error surfaces, never at the same
        millisecond as the last message that got through. One heartbeat interval of slack
        is the resolution the event stream actually offers."""
        tol = DEFAULT_EXPLANATION_TOLERANCE_MS
        at_limit = (_record(self.GAP.start_ms - tol, CollectorEventKind.DISCONNECT),)
        past_limit = (_record(self.GAP.start_ms - tol - 1, CollectorEventKind.DISCONNECT),)
        assert explain_gaps((self.GAP,), at_limit)[0].explained
        assert not explain_gaps((self.GAP,), past_limit)[0].explained

    def test_tolerance_boundary_after_the_gap(self) -> None:
        """A RECONNECT accounts for `[ts - downtime_ms, ts]`, so it stops explaining the gap
        once that window has moved entirely past it."""
        tol = DEFAULT_EXPLANATION_TOLERANCE_MS
        span = self.GAP.end_ms - self.GAP.start_ms
        at_limit = (
            _record(self.GAP.end_ms + tol, CollectorEventKind.RECONNECT, downtime_ms=span),
        )
        past_limit = (
            _record(
                self.GAP.end_ms + span + tol + 1,
                CollectorEventKind.RECONNECT,
                downtime_ms=span,
            ),
        )
        assert explain_gaps((self.GAP,), at_limit)[0].explained
        assert not explain_gaps((self.GAP,), past_limit)[0].explained

    def test_a_recovery_does_not_explain_more_downtime_than_it_measured(self) -> None:
        """The REST pollers made this urgent.

        A poller writes a DISCONNECT when one HTTP request fails and a RECONNECT with the
        measured downtime when the next succeeds — and it passes `downtime_ms = 0` on the
        way in. Matching on overlap alone, that twenty-second incident accounted for an
        outage of any length: a full day of missing aggregate trades was filed as explained
        by one request that failed and recovered. It is the mechanism by which genuinely
        lost data reads as accounted-for, which is the one thing this module exists to
        prevent.
        """
        tiny = _record(self.GAP.start_ms + 1_000, CollectorEventKind.RECONNECT, downtime_ms=0)
        assert not explain_gaps((self.GAP,), (tiny,))[0].explained

        honest = _record(
            self.GAP.end_ms,
            CollectorEventKind.RECONNECT,
            downtime_ms=self.GAP.end_ms - self.GAP.start_ms,
        )
        assert explain_gaps((self.GAP,), (honest,))[0].explained

    def test_a_disconnect_stops_explaining_once_it_is_closed(self) -> None:
        """An open DISCONNECT means "down from here", and legitimately covers everything
        after it. The same DISCONNECT with a RECONNECT twenty seconds later covers twenty
        seconds — the outage it opened is over, and a longer gap needs its own explanation.
        """
        opened = _record(self.GAP.start_ms, CollectorEventKind.DISCONNECT, stream="aggTrades")
        assert explain_gaps((self.GAP,), (opened,))[0].explained

        closed = (
            opened,
            _record(
                self.GAP.start_ms + 20_000,
                CollectorEventKind.RECONNECT,
                stream="aggTrades",
                downtime_ms=20_000,
            ),
        )
        assert not explain_gaps((self.GAP,), closed)[0].explained

    def test_downtime_is_measured_backwards_from_the_record(self) -> None:
        """A RESTART is written *after* the outage it describes. Reading `downtime_ms`
        forwards leaves every crash gap unexplained."""
        far_after = self.GAP.end_ms + 10 * MIN_MS
        without_downtime = (_record(far_after, CollectorEventKind.RESTART),)
        with_downtime = (
            _record(far_after, CollectorEventKind.RESTART, downtime_ms=20 * MIN_MS),
        )
        assert not explain_gaps((self.GAP,), without_downtime)[0].explained
        assert explain_gaps((self.GAP,), with_downtime)[0].explained

    def test_stale_explains_only_its_own_dataset(self) -> None:
        """A STALE record names one lake dataset. The collector is alive and every other
        stream is still flowing, so it accounts for that dataset's silence and no other."""
        stale = (
            _record(
                T0 + 15 * MIN_MS,
                CollectorEventKind.STALE,
                stream="depth20",
                downtime_ms=120_000,
            ),
        )
        depth_gap = Gap(
            "depth20", SYMBOL, T0 + 10 * MIN_MS, T0 + 20 * MIN_MS, GapKind.TICK_SILENCE, "x"
        )
        assert explain_gaps((depth_gap,), stale)[0].explained
        assert not explain_gaps((self.GAP,), stale)[0].explained

    def test_connection_records_cover_every_stream_on_the_socket(self) -> None:
        """`StreamManager` stamps the comma-joined subscription list. One socket dropping
        takes all of them down together."""
        joined = "btcusdt@depth20@100ms,btcusdt@bookTicker,btcusdt@aggTrade,btcusdt@markPrice@1s"
        events = (_record(T0 + 12 * MIN_MS, CollectorEventKind.DISCONNECT, stream=joined),)
        for dataset in ("depth20", "bookTicker", "aggTrades", "markPrice"):
            gap = Gap(
                dataset, SYMBOL, T0 + 10 * MIN_MS, T0 + 20 * MIN_MS, GapKind.TICK_SILENCE, "x"
            )
            assert explain_gaps((gap,), events)[0].explained, dataset

    def test_bulk_datasets_are_never_explained_by_the_collector(self) -> None:
        """A missing month of klines is explained by a download that never ran, not by the
        collector having restarted. Letting a RESTART absolve it hides the real problem.

        `metrics` is deliberately absent from this list since the collector grew an
        open-interest poller: it is now dual-fed exactly like `aggTrades`, and over a
        bulk-only range there are simply no events to match.
        """
        events = (
            _record(T0 + 12 * MIN_MS, CollectorEventKind.RESTART, downtime_ms=HOUR_MS),
        )
        for dataset in ("klines", "markPriceKlines", "funding", "bookDepth"):
            gap = Gap(
                dataset, SYMBOL, T0 + 10 * MIN_MS, T0 + 20 * MIN_MS, GapKind.MISSING_BARS, "x"
            )
            assert not explain_gaps((gap,), events)[0].explained, dataset

    def test_metrics_gaps_are_explainable_by_the_collector(self) -> None:
        """`OpenInterestPoller` writes `metrics` live and stamps its DISCONNECT records
        with the dataset name, so a metrics hole during a recorded poller outage must be
        accounted for the same way an aggTrades hole is."""
        gap = Gap(
            "metrics", SYMBOL, T0 + 10 * MIN_MS, T0 + 20 * MIN_MS, GapKind.METRIC_SILENCE, "x"
        )
        events = (
            _record(T0 + 10 * MIN_MS, CollectorEventKind.DISCONNECT, stream="metrics"),
        )
        assert explain_gaps((gap,), events)[0].explained

    def test_a_single_stream_disconnect_pairs_with_the_socket_reconnect(self) -> None:
        """Finding H23's broken pairing, from the closing side.

        The collector stamps some DISCONNECTs with a single raw stream name while
        `StreamManager` stamps every CONNECT/RECONNECT with the comma-joined subscription
        list. Under exact string equality those never paired, so the DISCONNECT stayed
        open forever and explained everything after it. Normalised to stream sets, the
        RECONNECT twenty seconds later closes it -- and a ten-minute gap then needs its
        own explanation, exactly as an explicitly same-stream pair already behaved.
        """
        joined = "btcusdt@depth20@100ms,btcusdt@bookTicker"
        events = (
            _record(
                self.GAP.start_ms, CollectorEventKind.DISCONNECT, stream="btcusdt@aggTrade"
            ),
            _record(
                self.GAP.start_ms + 20_000,
                CollectorEventKind.RECONNECT,
                stream="btcusdt@aggTrade," + joined,
                downtime_ms=20_000,
            ),
        )
        assert not explain_gaps((self.GAP,), events)[0].explained

        # Control: without the closer the same DISCONNECT still explains the gap.
        assert explain_gaps((self.GAP,), events[:1])[0].explained

    def test_a_process_restart_closes_a_stream_disconnect(self) -> None:
        """A RESTART is process-wide: the previous run's open outages end at the moment
        the new process starts writing its own records."""
        events = (
            _record(self.GAP.start_ms, CollectorEventKind.DISCONNECT, stream="btcusdt@aggTrade"),
            _record(
                self.GAP.start_ms + 20_000,
                CollectorEventKind.RESTART,
                stream="collector",
                downtime_ms=15_000,
            ),
        )
        assert not explain_gaps((self.GAP,), events)[0].explained

    def test_an_unclosed_disconnect_explains_at_most_one_connection_lifetime(self) -> None:
        """Finding H23's bound. One stray DISCONNECT that nothing ever closed used to
        explain a gap of unbounded length; it now speaks for at most the 24 h after which
        Binance itself forces a reconnect cycle."""
        day = 24 * 60 * 60 * 1000
        long_gap = Gap(
            "aggTrades", SYMBOL, T0, T0 + day + HOUR_MS, GapKind.TICK_SILENCE, "x"
        )
        opener = (_record(T0, CollectorEventKind.DISCONNECT, stream="aggTrades"),)
        assert not explain_gaps((long_gap,), opener)[0].explained

        within_bound = Gap(
            "aggTrades", SYMBOL, T0, T0 + day - HOUR_MS, GapKind.TICK_SILENCE, "x"
        )
        assert explain_gaps((within_bound,), opener)[0].explained

    def test_a_shutdown_stays_unbounded(self) -> None:
        """A SHUTDOWN states an open-ended fact -- the operator stopped the collector --
        and must keep explaining however long the stop lasts, unlike a transient
        DISCONNECT (see MAX_UNCLOSED_EXPLANATION_MS)."""
        week = 7 * 24 * 60 * 60 * 1000
        long_gap = Gap("aggTrades", SYMBOL, T0, T0 + week, GapKind.TICK_SILENCE, "x")
        events = (_record(T0, CollectorEventKind.SHUTDOWN, stream="collector"),)
        assert explain_gaps((long_gap,), events)[0].explained

    def test_negative_tolerance_rejected(self) -> None:
        with pytest.raises(ValueError):
            explain_gaps((self.GAP,), (), tolerance_ms=-1)


class TestStreamMappingDoesNotDrift:
    def test_heartbeat_interval_matches_the_collector(self) -> None:
        """Duplicated as a constant to keep `asyncio` and `websockets` out of a batch
        report; this is what stops the duplicate drifting."""
        from perplab.data.collector import HEARTBEAT_INTERVAL_S

        assert gaps.HEARTBEAT_INTERVAL_MS == int(HEARTBEAT_INTERVAL_S * 1000)

    def test_metrics_threshold_matches_the_collectors_staleness_alarm(self) -> None:
        """The metrics rule's whole licence to exist is that its threshold is the
        collector's own reviewed number, not a new one invented in this module."""
        from perplab.data.collector import MAX_SILENCE_S

        assert gaps.METRICS_SILENCE_MS == int(MAX_SILENCE_S["metrics"] * 1000)

    def test_macro_stream_list_matches_the_macro_service(self) -> None:
        """Duplicated to keep `httpx` out of a batch report; this stops the drift."""
        from perplab.data.macro import MACRO_DATASETS

        assert gaps._MACRO_SERVICE_STREAMS == MACRO_DATASETS

    def test_every_stream_the_collector_subscribes_to_maps_to_a_dataset(
        self, tmp_path: Path
    ) -> None:
        """A new stream that no record can be attributed to is a dataset whose outages are
        permanently unexplainable, which would be discovered only after a 72-hour run."""
        from perplab.data.collector import Collector

        collector = Collector(SYMBOL, tmp_path)
        datasets = ("depth20", "bookTicker", "aggTrades", "markPrice", "liquidations")
        for stream in collector.streams:
            matched = [d for d in datasets if gaps._stream_covers_dataset(stream, d)]
            assert len(matched) == 1, f"{stream!r} mapped to {matched}"

    def test_every_lake_dataset_has_a_rule_or_a_reason(self) -> None:
        """A report that never mentions a dataset is indistinguishable from one that forgot
        it, so every schema must be either checkable or explicitly not."""
        assert set(DATASET_RULES) == set(SCHEMAS)
        for name, entry in DATASET_RULES.items():
            assert entry.note, name
            if entry.rule is GapRule.NONE:
                assert len(entry.note) > 40, f"{name} must say why it cannot be checked"


# --------------------------------------------------------------------------------------
# The whole report
# --------------------------------------------------------------------------------------


def _corrupted_lake(tmp_path: Path) -> Path:
    """A lake with one of each deliberate defect, per the Phase 1 exit criterion.

    - klines: minute 30 deleted; minutes 10-14 present but zero-volume (data, not a gap)
    - aggTrades: silent for minutes 40-49, over a stretch that traded
    - funding: two settlements missing from a four-hourly schedule, on the far side of the
      range start so the anchor row is what makes them visible
    - collectorEvents: one outage explained by a RESTART, one not
    """
    _write_klines(
        tmp_path, [m for m in range(60) if m != 30], zero_volume=frozenset(range(10, 15))
    )
    _write(
        tmp_path,
        "aggTrades",
        [_trade(T0 + m * MIN_MS + 500, agg_id=m) for m in range(60) if not 40 <= m <= 49],
    )
    _write(
        tmp_path,
        "funding",
        [_funding_row(T0 - 12 * HOUR_MS, 4), _funding_row(T0 + 30 * MIN_MS, 4)],
    )
    _write(
        tmp_path,
        "collectorEvents",
        [
            # Alive, then dead from 100 s to 400 s -- a crash, reported by the RESTART the
            # next process writes. Alive again to 590 s, then dead to 900 s with nothing
            # to account for it. Heartbeats run to the end of the range from there, so the
            # trailing edge is clean and only the two intended holes are in the report.
            *[_event(T0 + ts) for ts in range(0, 110_000, 10_000)],
            _event(
                T0 + 400_000,
                CollectorEventKind.RESTART,
                detail="previous run ended without clean shutdown (pid 7)",
                downtime_ms=300_000,
            ),
            *[_event(T0 + ts) for ts in range(410_000, 600_000, 10_000)],
            *[_event(T0 + ts) for ts in range(900_000, 60 * MIN_MS, 10_000)],
        ],
        symbol=None,
    )
    return tmp_path


class TestReport:
    DATASETS = ("klines", "aggTrades", "funding", "collectorEvents")

    def test_end_to_end_on_a_deliberately_corrupted_sample(self, tmp_path: Path) -> None:
        report = detect_gaps(
            _corrupted_lake(tmp_path),
            SYMBOL,
            T0,
            T0 + 60 * MIN_MS,
            datasets=self.DATASETS,
        )
        by_dataset = {d: [g for g in report.gaps if g.dataset == d] for d in self.DATASETS}

        assert [g.start_ms for g in by_dataset["klines"]] == [T0 + 30 * MIN_MS]
        assert by_dataset["klines"][0].duration_ms == MIN_MS

        assert len(by_dataset["aggTrades"]) == 1
        assert by_dataset["aggTrades"][0].start_ms == T0 + 39 * MIN_MS + 500

        assert len(by_dataset["funding"]) == 1
        assert by_dataset["funding"][0].duration_ms == 12 * HOUR_MS + 30 * MIN_MS

        outages = by_dataset["collectorEvents"]
        assert len(outages) == 2
        assert [g.explained for g in outages] == [True, False]

    def test_zero_volume_bars_never_reach_the_gap_list(self, tmp_path: Path) -> None:
        report = detect_gaps(
            _corrupted_lake(tmp_path), SYMBOL, T0, T0 + 60 * MIN_MS, datasets=("klines",)
        )
        kline_coverage = next(c for c in report.coverage if c.dataset == "klines")
        assert kline_coverage.zero_volume_bars == 5
        assert len(report.gaps) == 1

    def test_report_totals(self, tmp_path: Path) -> None:
        report = detect_gaps(
            _corrupted_lake(tmp_path), SYMBOL, T0, T0 + 60 * MIN_MS, datasets=self.DATASETS
        )
        assert len(report.explained) + len(report.unexplained) == len(report.gaps)
        assert report.total_gap_ms == sum(g.duration_ms for g in report.gaps)

    def test_gaps_are_ordered_by_time(self, tmp_path: Path) -> None:
        report = detect_gaps(
            _corrupted_lake(tmp_path), SYMBOL, T0, T0 + 60 * MIN_MS, datasets=self.DATASETS
        )
        assert list(report.gaps) == sorted(report.gaps, key=lambda g: g.start_ms)

    def test_rule_less_datasets_are_reported_as_unevaluated(self, tmp_path: Path) -> None:
        """Silence about a dataset reads as a clean bill of health, so "cannot check" is
        printed rather than omitted."""
        report = detect_gaps(
            _corrupted_lake(tmp_path),
            SYMBOL,
            T0,
            T0 + 60 * MIN_MS,
            datasets=("metrics", "liquidations", "markPrice", "bookDepth"),
        )
        assert report.gaps == ()
        assert {u.dataset for u in report.unevaluated} == {
            "metrics",
            "liquidations",
            "markPrice",
            "bookDepth",
        }

    def test_missing_klines_make_tick_datasets_unevaluated_not_clean(
        self, tmp_path: Path
    ) -> None:
        _write(tmp_path, "aggTrades", [_trade(T0, 1), _trade(T0 + 30 * MIN_MS, 2)])
        report = detect_gaps(
            tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS, datasets=("aggTrades",)
        )
        assert report.gaps == ()
        assert len(report.unevaluated) == 1
        assert "klines" in report.unevaluated[0].reason

    def test_unregistered_dataset_raises(self, tmp_path: Path) -> None:
        with pytest.raises(KeyError, match="no gap rule registered"):
            detect_gaps(tmp_path, SYMBOL, T0, T0 + MIN_MS, datasets=("nonsuch",))

    def test_backwards_range_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="forward in time"):
            detect_gaps(tmp_path, SYMBOL, T0 + MIN_MS, T0)


class TestSymbolNormalisation:
    """The lake spells symbols one way; every entry point has to agree with it.

    The writer files `symbol=BTCUSDT`. A detector that globbed `symbol=btcusdt` verbatim
    would find nothing on a case-sensitive filesystem and everything on Windows, so the bug
    would only appear in production. `schemas.normalise_symbol` is the one place that
    decides, and these assert this module goes through it on the way in *and* on the way
    out -- a report naming `btcusdt` while having read `symbol=BTCUSDT` is a report nobody
    can grep.
    """

    def test_a_lowercase_symbol_reads_the_partition_the_writer_wrote(
        self, tmp_path: Path
    ) -> None:
        _write(tmp_path, "klines", [_bar(T0 + i * MIN_MS) for i in range(10)])
        found, coverage = detect_kline_gaps(tmp_path, "btcusdt", T0, T0 + 10 * MIN_MS)
        assert found == ()
        assert coverage.rows == 10

    def test_the_report_carries_the_canonical_spelling(self, tmp_path: Path) -> None:
        _write(tmp_path, "klines", [_bar(T0)])
        report = detect_gaps(tmp_path, "btcusdt", T0, T0 + 3 * MIN_MS, datasets=("klines",))
        assert report.symbol == SYMBOL
        assert {g.symbol for g in report.gaps} == {SYMBOL}

    def test_an_implausible_symbol_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="implausible symbol"):
            detect_gaps(tmp_path, "BTC/USDT", T0, T0 + MIN_MS, datasets=("klines",))


class TestJsonSerialisation:
    """The seam `manifest.py` consumes (spec 4.6's `gaps` field).

    `Gap` is a frozen dataclass, so before this existed `json.dumps` could not touch one
    and handing `detect_gaps(...).gaps` straight to `build_manifest` raised "gap report is
    not JSON-serialisable" -- which reads as a caller error and was really a missing seam
    between two modules written in parallel.
    """

    def test_a_gap_round_trips_through_json(self) -> None:
        gap = Gap(
            dataset="klines",
            symbol=SYMBOL,
            start_ms=T0,
            end_ms=T0 + MIN_MS,
            kind=GapKind.MISSING_BARS,
            detail="1 bar(s) absent",
        )
        payload = gap.to_json()
        assert json.loads(json.dumps(payload)) == payload
        assert payload["kind"] == "MISSING_BARS"
        assert payload["duration_ms"] == MIN_MS
        assert payload["explained"] is False

    def test_the_enum_is_written_as_its_value_not_its_repr(self) -> None:
        """A `repr` would embed this module's class name in every stored manifest."""
        gap = Gap(
            dataset="collectorEvents",
            symbol=None,
            start_ms=T0,
            end_ms=T0 + MIN_MS,
            kind=GapKind.COLLECTOR_OUTAGE,
            detail="down",
        )
        assert "GapKind" not in json.dumps(gap.to_json())

    def test_an_explanation_survives(self) -> None:
        gap = Gap(
            dataset="depth20",
            symbol=SYMBOL,
            start_ms=T0,
            end_ms=T0 + MIN_MS,
            kind=GapKind.TICK_SILENCE,
            detail="quiet",
        ).with_explanation("RESTART at 12:00")
        payload = gap.to_json()
        assert payload["explained"] is True
        assert payload["explanation"] == "RESTART at 12:00"

    def test_the_report_serialises_in_report_order(self, tmp_path: Path) -> None:
        report = detect_gaps(
            _corrupted_lake(tmp_path),
            SYMBOL,
            T0,
            T0 + 60 * MIN_MS,
            datasets=TestReport.DATASETS,
        )
        payload = report.to_json()
        assert [g["start_ms"] for g in payload] == [g.start_ms for g in report.gaps]
        assert json.loads(json.dumps(payload)) == payload


class TestRenderReport:
    def test_answers_which_gaps_and_how_long(self, tmp_path: Path) -> None:
        """The STRICT-mode requirement, verbatim: refuse to run, show which gaps and how
        long. Both answers have to be legible without arithmetic."""
        report = detect_gaps(
            _corrupted_lake(tmp_path),
            SYMBOL,
            T0,
            T0 + 60 * MIN_MS,
            datasets=TestReport.DATASETS,
        )
        text = render_report(report)

        assert SYMBOL in text
        assert "2024-01-01T00:00:00.000Z" in text
        assert "unexplained" in text
        assert "UNEXPLAINED" in text
        assert "EXPLAINED BY" in text
        for dataset in TestReport.DATASETS:
            assert dataset in text
        assert format_duration(report.total_gap_ms) in text

    def test_shows_zero_volume_bars_as_data(self, tmp_path: Path) -> None:
        report = detect_gaps(
            _corrupted_lake(tmp_path), SYMBOL, T0, T0 + 60 * MIN_MS, datasets=("klines",)
        )
        assert "zero-volume bar(s) -- data, not gaps" in render_report(report)

    def test_a_clean_report_still_states_what_was_checked(self, tmp_path: Path) -> None:
        """An empty gap list is only good news if the report says what was looked at."""
        _write_klines(tmp_path, list(range(60)))
        report = detect_gaps(
            tmp_path, SYMBOL, T0, T0 + 60 * MIN_MS, datasets=("klines", "metrics")
        )
        text = render_report(report)
        assert "no gaps found" in text
        assert "coverage" in text
        assert "not evaluated" in text
        assert "60/60 bars" in text

    def test_duplicate_partitions_are_surfaced(self, tmp_path: Path) -> None:
        _write(tmp_path, "klines", [_bar(T0), _bar(T0), _bar(T0 + MIN_MS)])
        report = detect_gaps(
            tmp_path, SYMBOL, T0, T0 + 2 * MIN_MS, datasets=("klines",)
        )
        assert "duplicate timestamp(s)" in render_report(report)
