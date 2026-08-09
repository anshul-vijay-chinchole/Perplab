"""Indicator arithmetic, checked against numbers computed by hand.

Hand-computed rather than compared against a reference library, for the reason spec 12
gives about golden tests generally: a test that agrees with TA-Lib proves the two agree,
not that either is right, and it inherits every convention TA-Lib chose without ever
stating one. Each expectation below is derived from the definition in the docstring of the
indicator under test, on a series short enough to check by inspection.

The truncation test at the bottom is the one that matters most (spec 12.3). Every other
test here would still pass if an indicator read one bar into the future.
"""

from __future__ import annotations

import math

import pytest

from perplab.core.money import SCALE
from perplab.core.types import Bar, DepthSnapshot
from perplab.engine.ticks import TradePrint
from perplab.strategy.indicators import (
    ADX,
    ATR,
    CVD,
    EMA,
    MACD,
    OBV,
    RSI,
    SMA,
    VWAP,
    WMA,
    BookImbalance,
    Bollinger,
    DerivedSeries,
    Donchian,
    FundingMean,
    Indicator,
    IndicatorSet,
    OIDelta,
    RealisedVolatility,
)

TF_MS = 900_000
START = 1_704_067_200_000


def scaled(value: float) -> int:
    return int(round(value * SCALE))


def bar(
    index: int,
    close: float,
    *,
    high: float | None = None,
    low: float | None = None,
    open_: float | None = None,
    volume: float = 10.0,
    symbol: str = "BTCUSDT",
    timeframe_ms: int = TF_MS,
) -> Bar:
    opened = close if open_ is None else open_
    top = max(opened, close) if high is None else high
    bottom = min(opened, close) if low is None else low
    open_time = START + index * timeframe_ms
    return Bar(
        symbol=symbol,
        open_time=open_time,
        close_time=open_time + timeframe_ms - 1,
        open=scaled(opened),
        high=scaled(top),
        low=scaled(bottom),
        close=scaled(close),
        volume=scaled(volume),
        quote_volume=scaled(volume * close),
        trades=7,
    )


def feed(indicator: Indicator, closes: list[float]) -> None:
    for index, close in enumerate(closes):
        indicator.update(bar(index, close))


# ------------------------------------------------------------------- moving averages


class TestMovingAverages:
    def test_sma_is_the_mean_of_the_window(self) -> None:
        sma = SMA(3)
        feed(sma, [1.0, 2.0, 3.0, 4.0])
        assert sma.value == pytest.approx(3.0)  # (2+3+4)/3
        assert sma.prev == pytest.approx(2.0)  # (1+2+3)/3

    def test_sma_has_no_value_until_the_window_is_full(self) -> None:
        sma = SMA(5)
        feed(sma, [1.0, 2.0, 3.0, 4.0])
        assert sma.value is None
        assert not sma.ready
        assert sma.warmup == 5

    def test_sma_of_a_constant_series_stays_that_constant(self) -> None:
        sma = SMA(20)
        feed(sma, [100.0] * 5_000)
        assert sma.value == 100.0

    def test_the_rolling_sum_keeps_no_residue_from_values_that_left_the_window(self) -> None:
        """What the periodic exact re-sum is actually for.

        A purely incremental rolling sum (`s += new - old`) never forgets: rounding error
        from values that have long since left the window stays in `s` forever. A constant
        series cannot show this — there `new - old` is exactly zero — so the test above
        passes with or without the fix, which is how the re-sum survived a mutation that
        deleted it.

        The shape that shows it is a magnitude collapse. Volume on a thin symbol does go
        to zero, and `source="volume"` is offered, so this is not hypothetical. Without the
        re-sum the mean of a window of twenty genuine zeros comes back as -1.0e-09.

        The volumes carry real fractional bits deliberately. An earlier version of this
        test used `1e9 * (0.3 + i % 7 / 10)`, whose scaled values are exactly representable
        as doubles — so every `s += x - out` cancelled precisely and the test passed with or
        without the fix. It took a mutation to notice that the numbers, not the logic, were
        doing the work.
        """
        sma = SMA(20, source="volume")
        for index in range(500):
            sma.update(
                bar(index, close=100.0, volume=12345.6789 * (1 + (index % 11) * 0.37))
            )
        for index in range(500, 560):
            sma.update(bar(index, close=100.0, volume=0.0))
        assert sma.value == 0.0

    def test_ema_is_seeded_with_the_sma_then_recurses(self) -> None:
        # alpha = 2/(3+1) = 0.5. Seed = mean(1,2,3) = 2. Then 2 + 0.5*(4-2) = 3.
        ema = EMA(3)
        feed(ema, [1.0, 2.0, 3.0, 4.0])
        assert ema.value == pytest.approx(3.0)
        assert ema.prev == pytest.approx(2.0)
        assert ema.alpha == pytest.approx(0.5)

    def test_ema_converges_to_a_constant_series(self) -> None:
        ema = EMA(10)
        feed(ema, [50.0] * 200)
        assert ema.value == pytest.approx(50.0, abs=1e-9)

    def test_wma_weights_the_most_recent_bar_heaviest(self) -> None:
        # (1*1 + 2*2 + 3*3) / (1+2+3) = 14/6
        wma = WMA(3)
        feed(wma, [1.0, 2.0, 3.0])
        assert wma.value == pytest.approx(14.0 / 6.0)

    def test_wma_differs_from_sma_on_a_trend(self) -> None:
        closes = [1.0, 2.0, 3.0, 4.0, 5.0]
        wma, sma = WMA(5), SMA(5)
        feed(wma, closes)
        feed(sma, closes)
        assert wma.value > sma.value

    def test_price_source_selects_the_right_field(self) -> None:
        b = bar(0, close=10.0, open_=8.0, high=12.0, low=6.0)
        for source, expected in (
            ("close", 10.0),
            ("open", 8.0),
            ("high", 12.0),
            ("low", 6.0),
            ("hl2", 9.0),
            ("hlc3", (12.0 + 6.0 + 10.0) / 3),
            ("ohlc4", (8.0 + 12.0 + 6.0 + 10.0) / 4),
        ):
            sma = SMA(1, source=source)
            sma.update(b)
            assert sma.value == pytest.approx(expected), source

    def test_an_unknown_source_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown price source"):
            SMA(3, source="typical")

    @pytest.mark.parametrize("period", [0, -1])
    def test_a_non_positive_period_is_refused(self, period: int) -> None:
        with pytest.raises(ValueError, match="must be >="):
            SMA(period)

    def test_a_bool_period_is_refused(self) -> None:
        """`SMA(True)` would otherwise become a 1-period average of everything."""
        with pytest.raises(TypeError):
            SMA(True)  # type: ignore[arg-type]


# ------------------------------------------------------------------------ crossings


class TestCrossings:
    def _pair(self, fast_values: list[float], slow_values: list[float]) -> tuple[SMA, SMA]:
        fast, slow = SMA(1), SMA(1)
        for index, (f, s) in enumerate(zip(fast_values, slow_values)):
            fast.update(bar(index, f))
            slow.update(bar(index, s))
        return fast, slow

    def test_cross_fires_on_the_edge_only(self) -> None:
        """The `actionSeq` rule from spec 5.4: an edge, never a level.

        Level-derived crossing is the single most common cause of a strategy entering two
        hundred times where it meant to enter once, and it is invisible in a backtest
        summary -- the equity curve just looks wrong.
        """
        fast, slow = self._pair([1.0], [2.0])
        assert not fast.crossed_above(slow)  # no prev yet

        fast.update(bar(1, 3.0))
        slow.update(bar(1, 2.0))
        assert fast.crossed_above(slow)  # 1<=2 and 3>2

        fast.update(bar(2, 4.0))
        slow.update(bar(2, 2.0))
        assert not fast.crossed_above(slow)  # still above, but no longer an edge

    def test_touching_then_rising_counts_as_a_cross(self) -> None:
        """`prev == other.prev` then above. Requiring strict inequality on both sides
        would drop every cross that pauses exactly on the line for one bar."""
        fast, slow = self._pair([2.0, 3.0], [2.0, 2.0])
        assert fast.crossed_above(slow)

    def test_crossed_below_is_the_mirror(self) -> None:
        fast, slow = self._pair([3.0, 1.0], [2.0, 2.0])
        assert fast.crossed_below(slow)
        assert not fast.crossed_above(slow)

    def test_crossing_a_plain_number_works(self) -> None:
        rsi = SMA(1)
        rsi.update(bar(0, 65.0))
        rsi.update(bar(1, 75.0))
        assert rsi.crossed_above(70)
        assert not rsi.crossed_below(70)

    def test_an_unready_series_never_reports_a_cross(self) -> None:
        fast, slow = SMA(10), SMA(1)
        for index in range(3):
            fast.update(bar(index, 1.0 + index))
            slow.update(bar(index, 0.0))
        assert not fast.crossed_above(slow)
        assert not fast.crossed_below(slow)

    def test_crossing_a_non_number_is_a_type_error(self) -> None:
        sma = SMA(1)
        feed(sma, [1.0, 2.0])
        with pytest.raises(TypeError, match="another indicator or a number"):
            sma.crossed_above("70")  # type: ignore[arg-type]


# ----------------------------------------------------------------------- oscillators


class TestRSI:
    def test_matches_a_hand_computed_wilder_value(self) -> None:
        # Two periods of pure gain, then a loss. period=2.
        # closes 10, 11, 12 -> gains 1, 1 -> avg_gain = 1, avg_loss = 0 -> RSI 100.
        rsi = RSI(2)
        feed(rsi, [10.0, 11.0, 12.0])
        assert rsi.value == 100.0
        # Next close 10: loss 2. avg_gain = (1*1 + 0)/2 = 0.5,
        #                        avg_loss = (0*1 + 2)/2 = 1.0  -> RS 0.5 -> 100 - 100/1.5
        rsi.update(bar(3, 10.0))
        assert rsi.value == pytest.approx(100.0 - 100.0 / 1.5)

    def test_a_perfectly_flat_series_is_neutral_not_overbought(self) -> None:
        """Both averages zero. The usual `100` reads as maximum overbought on a series
        that has not moved at all, and a strategy thresholding at 70 would fire on a dead
        market."""
        rsi = RSI(3)
        feed(rsi, [100.0] * 10)
        assert rsi.value == 50.0

    def test_only_losses_bottoms_out(self) -> None:
        rsi = RSI(3)
        feed(rsi, [10.0, 9.0, 8.0, 7.0])
        assert rsi.value == pytest.approx(0.0)

    def test_warmup_accounts_for_the_extra_price(self) -> None:
        """`period` changes need `period + 1` prices; an off-by-one here would let the
        first RSI value be computed from one change fewer than declared."""
        rsi = RSI(14)
        assert rsi.warmup == 15
        feed(rsi, [100.0 + i for i in range(14)])
        assert rsi.value is None
        rsi.update(bar(14, 114.0))
        assert rsi.value is not None

    def test_a_fall_then_a_flat_window_converges_to_neutral_not_zero(self) -> None:
        """The M40 case: Wilder's recursion never forgets a fall.

        After the falling seed, `avg_gain` is exactly 0 and *stays* exactly 0 through any
        flat stretch (0 * (n-1)/n + 0 is 0), while `avg_loss` only decays geometrically and
        never reaches zero in float for thousands of bars. `rs = 0 / avg_loss = 0`, so the
        textbook formula prints exactly 0.0 -- maximum oversold -- on every bar of a market
        that is not moving at all, and `if rsi.value < 30: buy` fires on each one. Once the
        whole lookback window is flat (`period` consecutive zero changes) the documented
        neutral reading applies.
        """
        rsi = RSI(3)
        # Seed: closes 10, 9, 8, 7 -> three losses of 1. avg_gain = 0, avg_loss = 1 -> RSI 0.
        feed(rsi, [10.0, 9.0, 8.0, 7.0])
        assert rsi.value == 0.0
        # Two flat bars: the window still contains a real loss, so 0 remains the honest
        # reading. avg_loss decays 1 -> 2/3 -> 4/9; avg_gain stays 0.
        rsi.update(bar(4, 7.0))
        assert rsi.value == 0.0
        rsi.update(bar(5, 7.0))
        assert rsi.value == 0.0
        # Third flat bar: all `period` changes in the window are now zero -> neutral.
        rsi.update(bar(6, 7.0))
        assert rsi.value == 50.0
        rsi.update(bar(7, 7.0))
        assert rsi.value == 50.0
        # A real move resumes the ordinary Wilder computation from the memory it kept:
        # after four flat bars avg_loss = 1 * (2/3)^4 = 16/81; the +1 gain makes
        # avg_gain = (0*2 + 1)/3 = 1/3 = 27/81 and avg_loss = (16/81 * 2 + 0)/3 = 32/243.
        # rs = (27/81)/(32/243) = 81/32 -> RSI = 100 - 100/(1 + 81/32) = 8100/113.
        rsi.update(bar(8, 8.0))
        assert rsi.value == pytest.approx(8100.0 / 113.0)


class TestMACD:
    def test_line_is_the_difference_of_the_two_emas(self) -> None:
        closes = [100.0 + math.sin(i / 3.0) * 5 for i in range(60)]
        macd = MACD(3, 6, 4)
        fast, slow = EMA(3), EMA(6)
        feed(macd, closes)
        feed(fast, closes)
        feed(slow, closes)
        assert macd.value == pytest.approx(fast.value - slow.value)

    def test_histogram_is_line_minus_signal(self) -> None:
        macd = MACD(3, 6, 4)
        feed(macd, [100.0 + i for i in range(40)])
        assert macd.histogram.value == pytest.approx(macd.value - macd.signal.value)

    def test_warmup_covers_the_signal_line_too(self) -> None:
        """`warmup` must gate on the *slowest* thing the indicator exposes. Gating on the
        MACD line alone would let a strategy compare against an undefined signal."""
        macd = MACD(12, 26, 9)
        assert macd.warmup == 26 + 9 - 1
        feed(macd, [100.0 + i * 0.5 for i in range(macd.warmup - 1)])
        assert macd.signal.value is None
        macd.update(bar(macd.warmup - 1, 200.0))
        assert macd.signal.value is not None

    def test_ready_waits_for_the_signal_line_not_just_the_macd_line(self) -> None:
        """M32: `.ready` must keep the same promise `.warmup` does.

        The line leads the signal by `signal - 1` bars. Between them, `ready` used to be
        True while `.signal.value` was None, so `macd.value - macd.signal.value` -- the
        first thing every MACD strategy computes -- was a TypeError on an indicator that
        claimed readiness, and `IndicatorSet.all_ready` repeated the claim for the set.
        """
        macd = MACD(3, 6, 4)
        feed(macd, [100.0 + i for i in range(6)])  # slow EMA full: the line exists
        assert macd.value is not None
        assert macd.signal.value is None
        assert not macd.ready  # the reading the strategy wants does not exist yet
        # warmup = 6 + 4 - 1 = 9: three more bars complete the signal seed.
        feed_from = 6
        for index, close in enumerate([106.0, 107.0, 108.0]):
            macd.update(bar(feed_from + index, close))
        assert macd.signal.value is not None
        assert macd.ready

    def test_a_set_with_a_ready_line_but_no_signal_does_not_report_all_ready(self) -> None:
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        macd = indicators.macd(3, 6, 4)
        for index in range(6):
            indicators.on_bar(bar(index, 100.0 + index))
        assert macd.value is not None and macd.signal.value is None
        assert not indicators.all_ready
        assert macd in indicators.not_ready()

    def test_a_fast_period_at_or_above_slow_is_refused(self) -> None:
        """Reversed, the histogram's sign inverts and every signal reads backwards --
        a strategy that still runs and is exactly wrong."""
        with pytest.raises(ValueError, match="must be shorter"):
            MACD(26, 12, 9)
        with pytest.raises(ValueError, match="must be shorter"):
            MACD(12, 12, 9)


# ------------------------------------------------------------------------ volatility


class TestATR:
    def test_bar_zero_has_no_true_range_and_the_first_value_lands_at_index_period(
        self,
    ) -> None:
        """TA-Lib's convention, adopted in the Phase 6 audit (H15).

        Bar 0 donates only its close: with no previous close there is no gap term, and a
        fabricated `TR[0] = high - low` (the TradingView convention this class used to
        follow) both emitted a bar early and overstated the seed exactly where strategies
        start trading -- here, bar 0's fabricated TR of 4 against real TRs of 3 and 2
        turned the first reading into 3.5 where TA-Lib says nothing yet.
        """
        atr = ATR(2)
        atr.update(bar(0, close=10.0, high=12.0, low=8.0))  # no prev close: no TR
        atr.update(bar(1, close=11.0, high=13.0, low=11.0))  # TR1 = max(2, 3, 1) = 3
        assert atr.value is None  # TA-Lib: no value until `period` real TRs exist
        atr.update(bar(2, close=11.0, high=12.0, low=10.0))  # TR2 = max(2, 1, 1) = 2
        # Seed = mean(TR1, TR2) = (3 + 2) / 2 = 2.5, first value at index `period`.
        assert atr.value == pytest.approx(2.5)
        assert atr.warmup == 3  # period + 1: two TRs need three bars

    def test_gap_up_uses_the_previous_close(self) -> None:
        """`|high - prev_close|` is the term that makes a gap count. Dropping it would
        report a violent overnight gap as a quiet 1-point range."""
        atr = ATR(2)
        atr.update(bar(0, close=10.0, high=10.0, low=10.0))  # no TR (no prev close)
        atr.update(bar(1, close=20.0, high=20.5, low=19.5))  # TR1 = max(1, 10.5, 9.5)
        atr.update(bar(2, close=20.0, high=21.0, low=20.0))  # TR2 = max(1, 1, 0) = 1
        # (10.5 + 1) / 2 = 5.75
        assert atr.value == pytest.approx(5.75)

    def test_wilder_smoothing_after_the_seed(self) -> None:
        atr = ATR(2)
        atr.update(bar(0, close=10.0, high=12.0, low=8.0))  # dropped: no prev close
        atr.update(bar(1, close=11.0, high=13.0, low=11.0))  # TR 3
        atr.update(bar(2, close=11.0, high=12.0, low=10.0))  # TR 2 -> seed ATR 2.5
        atr.update(bar(3, close=12.0, high=12.0, low=11.0))  # TR max(1, 1, 0) = 1
        # Wilder: (2.5 * (2 - 1) + 1) / 2 = 1.75
        assert atr.value == pytest.approx(1.75)

    def test_atr_and_adx_smooth_the_same_true_range_series(self) -> None:
        """The two consumers of `_TrueRangeMixin` must agree on what TR[0] is.

        ATR used to keep a fabricated bar-0 true range that ADX always discarded, so one
        mixin fed two different series. The seeds are both plain means over the same
        window, so equality here is exact: ATR(2)'s seed is mean(TR1, TR2) and ADX's
        smoothed TR after its seed is sum(TR1, TR2) -- the same numbers or the mixin has
        forked again.
        """
        bars = [
            bar(0, close=10.0, high=11.0, low=9.0),
            bar(1, close=12.0, high=13.0, low=10.0),  # TR1 = max(3, 3, 0) = 3
            bar(2, close=11.0, high=12.5, low=10.5),  # TR2 = max(2, 0.5, 1.5) = 2
        ]
        atr = ATR(2)
        adx = ADX(2)
        for b in bars:
            atr.update(b)
            adx.update(b)
        # ATR seed = (3 + 2) / 2 = 2.5; ADX's running TR sum after its seed = 3 + 2 = 5.
        assert atr.value == pytest.approx(2.5)
        assert adx._sum_tr == pytest.approx(5.0)


class TestADX:
    def test_an_inside_bar_is_not_directional(self) -> None:
        """Both moves negative, so neither side registers."""
        adx = ADX(2)
        adx.update(bar(0, close=10.0, high=11.0, low=9.0))
        adx.update(bar(1, close=10.0, high=10.5, low=9.5))  # inside: no DM either way
        adx.update(bar(2, close=10.0, high=10.4, low=9.6))
        assert adx.plus_di.value == 0.0
        assert adx.minus_di.value == 0.0

    def test_an_outside_bar_counts_only_the_larger_move(self) -> None:
        """Wilder's directional movement is *exclusive*, and this is the only case that
        shows it.

        An outside bar has a higher high **and** a lower low, so `up_move` and `down_move`
        are both positive and only the larger counts. The inside-bar case above cannot
        distinguish that rule from a naive `max(move, 0)` — there both moves are negative
        and both implementations return zero — which is exactly how this survived a
        mutation that removed the exclusivity. Counting both sides makes +DI and -DI rise
        together on a volatile bar, dragging DX toward zero and reporting a strong trend as
        chop.
        """
        adx = ADX(2)
        adx.update(bar(0, close=10.0, high=10.0, low=9.0))
        # high +2.0, low -1.0: an outside bar, up-move larger.
        adx.update(bar(1, close=10.0, high=12.0, low=8.0))
        adx.update(bar(2, close=10.0, high=12.0, low=8.0))
        assert adx.plus_di.value > 0.0
        assert adx.minus_di.value == 0.0

    def test_equal_opposing_moves_cancel(self) -> None:
        """`>` not `>=` on both comparisons: a bar that extended equally in both
        directions has no direction, and awarding it to whichever branch is written first
        would put a systematic bias in every ADX reading."""
        adx = ADX(2)
        adx.update(bar(0, close=10.0, high=10.0, low=9.0))
        adx.update(bar(1, close=10.0, high=11.0, low=8.0))  # up +1.0, down +1.0
        adx.update(bar(2, close=10.0, high=11.0, low=8.0))
        assert adx.plus_di.value == 0.0
        assert adx.minus_di.value == 0.0

    def test_a_pure_uptrend_puts_all_directional_weight_on_plus_di(self) -> None:
        adx = ADX(2)
        for index in range(8):
            price = 10.0 + index
            adx.update(bar(index, close=price, high=price + 0.5, low=price - 0.5))
        assert adx.plus_di.value > 0
        assert adx.minus_di.value == 0.0
        assert adx.value == pytest.approx(100.0)

    def test_warmup_is_two_periods(self) -> None:
        adx = ADX(14)
        assert adx.warmup == 28
        for index in range(27):
            price = 10.0 + index * 0.1
            adx.update(bar(index, close=price, high=price + 0.2, low=price - 0.2))
        assert adx.value is None
        adx.update(bar(27, close=13.0, high=13.2, low=12.8))
        assert adx.value is not None

    def test_a_motionless_series_does_not_divide_by_zero(self) -> None:
        """Impossible in a real market, reachable on synthetic data during validation --
        and a ZeroDivisionError there would fail the strategy for the harness's reason."""
        adx = ADX(3)
        for index in range(20):
            adx.update(bar(index, close=10.0, high=10.0, low=10.0))
        assert adx.value == 0.0


class TestBollinger:
    def test_bands_use_population_deviation(self) -> None:
        # closes 2, 4, 4, 4, 5, 5, 7, 9 -> mean 5, population sd 2 (the textbook set)
        bb = Bollinger(8, 2.0)
        feed(bb, [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        assert bb.value == pytest.approx(5.0)
        assert bb.upper.value == pytest.approx(9.0)
        assert bb.lower.value == pytest.approx(1.0)

    def test_zero_variance_collapses_the_bands_without_dividing_by_zero(self) -> None:
        bb = Bollinger(5)
        feed(bb, [7.0] * 10)
        assert bb.upper.value == pytest.approx(7.0)
        assert bb.lower.value == pytest.approx(7.0)
        assert bb.percent_b.value == 0.5
        assert bb.bandwidth.value == pytest.approx(0.0)

    def test_variance_stays_non_negative_on_a_large_offset_series(self) -> None:
        """The reason the variance is two-pass. An incremental sum-of-squares against a
        mean of 60,000 and a spread of 0.01 cancels catastrophically and can return a
        negative variance, whose square root is a crash."""
        bb = Bollinger(20)
        feed(bb, [60_000.0 + (i % 3) * 0.01 for i in range(200)])
        assert bb.upper.value >= bb.value >= bb.lower.value

    def test_percent_b_places_the_price_across_the_bands(self) -> None:
        bb = Bollinger(8, 2.0)
        feed(bb, [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
        # last close 9, lower 1, upper 9 -> 1.0
        assert bb.percent_b.value == pytest.approx(1.0)

    def test_bandwidth_and_percent_b_are_series_with_an_edge_form(self) -> None:
        """L13: the two readings a Bollinger strategy most wants to *cross*-test.

        As computed properties they had no `.prev`, so a squeeze entry had to be written as
        the latching level comparison the module docstring forbids. As derived series the
        edge form works, and the values are checkable by hand.
        """
        bb = Bollinger(2, 2.0)
        feed(bb, [10.0, 10.0, 10.0])
        # Two full flat windows so far: bandwidth 0 on each, percent_b 0.5 on each.
        assert bb.bandwidth.value == pytest.approx(0.0)
        assert bb.bandwidth.prev == pytest.approx(0.0)
        bb.update(bar(3, 14.0))
        # Window (10, 14): mean 12, population sd = sqrt(((10-12)^2 + (14-12)^2)/2) = 2.
        # upper = 12 + 4 = 16, lower = 12 - 4 = 8.
        # bandwidth = (16 - 8) / 12 = 2/3; percent_b = (14 - 8) / 8 = 0.75.
        assert bb.bandwidth.value == pytest.approx(2.0 / 3.0)
        assert bb.percent_b.value == pytest.approx(0.75)
        # The edge: prev 0.0 <= 0.5 and value 2/3 > 0.5 -- fires exactly once.
        assert bb.bandwidth.crossed_above(0.5)
        assert bb.percent_b.prev == pytest.approx(0.5)


class TestDonchian:
    def test_channel_spans_the_window(self) -> None:
        d = Donchian(3)
        d.update(bar(0, close=10.0, high=11.0, low=9.0))
        d.update(bar(1, close=10.0, high=15.0, low=8.0))
        d.update(bar(2, close=10.0, high=12.0, low=7.0))
        assert d.upper.value == pytest.approx(15.0)
        assert d.lower.value == pytest.approx(7.0)
        assert d.value == pytest.approx(11.0)

    def test_the_current_bar_is_included(self) -> None:
        """Stated as a test because the breakout variant excludes it, the two differ by
        one bar, and that difference silently doubles or halves a trade count. A breakout
        strategy compares against `.upper.prev`, which says what it means."""
        d = Donchian(2)
        d.update(bar(0, close=10.0, high=10.0, low=10.0))
        d.update(bar(1, close=20.0, high=20.0, low=20.0))
        assert d.upper.value == pytest.approx(20.0)
        assert d.upper.prev is None

    def test_warmup_covers_the_documented_breakout_idiom(self) -> None:
        """M38: `warmup` must make `.upper.prev` -- the comparison this class's own
        docstring prescribes -- defined at the first warm bar.

        At `period` bars the channel is full but `.upper.prev` is still None, so a
        strategy gated on `ctx.warm` met `close > d.upper.prev` as a TypeError on its
        first tradeable bar. Warm-up gates on the slowest thing the indicator tells you
        to read (the MACD-signal rule), so it is `period + 1`.
        """
        d = Donchian(2)
        assert d.warmup == 3
        d.update(bar(0, close=10.0, high=11.0, low=9.0))
        d.update(bar(1, close=12.0, high=13.0, low=10.0))
        # Channel full, but the idiom's operand is missing: not yet warm.
        assert d.upper.value is not None and d.upper.prev is None
        d.update(bar(2, close=12.0, high=12.5, low=11.0))
        # At `warmup` updates the documented comparison is defined: prev spans bars 0-1.
        assert d.upper.prev == pytest.approx(13.0)
        assert d.upper.value == pytest.approx(13.0)  # max(high[1], high[2])


class TestRealisedVolatility:
    def test_annualises_the_root_mean_square_return(self) -> None:
        rv = RealisedVolatility(2, bar_ms=TF_MS)
        feed(rv, [100.0, 101.0, 100.0])
        r1 = math.log(101.0 / 100.0)
        r2 = math.log(100.0 / 101.0)
        expected = math.sqrt((r1 * r1 + r2 * r2) / 2 * rv.periods_per_year)
        assert rv.value == pytest.approx(expected)

    def test_uses_a_365_day_year(self) -> None:
        """Perps trade continuously. A 252-day equities calendar would understate
        annualised volatility by about 20% for no reason but habit."""
        rv = RealisedVolatility(2, bar_ms=86_400_000)
        assert rv.periods_per_year == pytest.approx(365.0)

    def test_a_flat_series_has_zero_volatility(self) -> None:
        rv = RealisedVolatility(5)
        feed(rv, [100.0] * 20)
        assert rv.value == pytest.approx(0.0)

    def test_a_zero_volume_bar_is_skipped_honestly_not_raised(self) -> None:
        """M37: zero is a value `source="volume"` legitimately produces.

        Raising killed the whole run on data the source explicitly admits, and the raise
        left `_prev` already advanced. The honest handling: no log return exists across a
        zero, so the bar contributes nothing and the window restarts -- a window that
        silently skipped a bar would span more bars than it claims.
        """
        rv = RealisedVolatility(2, bar_ms=TF_MS, source="volume")
        volumes = [10.0, 12.0, 0.0, 15.0, 9.0, 18.0]
        for index, volume in enumerate(volumes):
            rv.update(bar(index, 100.0, volume=volume))  # never raises
        # The zero cleared the window: only the returns *after* the gap count.
        # r1 = ln(9/15), r2 = ln(18/9); value = sqrt((r1^2 + r2^2)/2 * periods_per_year).
        r1 = math.log(9.0 / 15.0)
        r2 = math.log(18.0 / 9.0)
        expected = math.sqrt((r1 * r1 + r2 * r2) / 2 * rv.periods_per_year)
        assert rv.value == pytest.approx(expected)
        # And the pre-gap return ln(12/10) is not in the window: with it, the first push
        # would have happened one bar earlier, at the 15 -> 9 bar.
        assert len(rv.series(10)) == 1

    def test_an_impossible_observation_is_refused_before_prev_advances(self) -> None:
        """The refusal must not corrupt state: `_prev` used to advance first, so a caught
        error left the series primed to compute its next return against the rejected
        observation."""
        rv = RealisedVolatility(2, bar_ms=TF_MS)
        rv.update(bar(0, 100.0))
        with pytest.raises(ValueError, match="positive prices"):
            rv.update(bar(1, 0.0))  # a zero *price* remains impossible data
        # State is exactly as before the bad bar: the next good bar's return is against
        # 100, giving ln(101/100) and then ln(100/101) -- a hand-checkable value.
        rv.update(bar(2, 101.0))
        rv.update(bar(3, 100.0))
        r1 = math.log(101.0 / 100.0)
        r2 = math.log(100.0 / 101.0)
        expected = math.sqrt((r1 * r1 + r2 * r2) / 2 * rv.periods_per_year)
        assert rv.value == pytest.approx(expected)


# ---------------------------------------------------------------------------- flow


class TestFlow:
    def test_obv_signs_volume_by_direction_and_ignores_an_unchanged_close(self) -> None:
        obv = OBV()
        obv.update(bar(0, close=10.0, volume=5.0))
        obv.update(bar(1, close=11.0, volume=3.0))  # up   -> +3
        obv.update(bar(2, close=11.0, volume=8.0))  # flat -> +0
        obv.update(bar(3, close=10.0, volume=2.0))  # down -> -2
        assert obv.value == pytest.approx(1.0)

    def test_cvd_reads_the_aggressor_flag_the_right_way_round(self) -> None:
        """`is_buyer_maker=True` means the buyer was the *maker*, so the trade was
        sell-aggressive. Inverted, CVD is the exact negative of the truth and still looks
        like a plausible line."""
        cvd = CVD()
        cvd.update(_trade(qty=2.0, is_buyer_maker=False))  # buy aggressor  -> +2
        assert cvd.value == pytest.approx(2.0)
        cvd.update(_trade(qty=3.0, is_buyer_maker=True))  # sell aggressor -> -3
        assert cvd.value == pytest.approx(-1.0)

    def test_book_imbalance_is_bounded_and_signed(self) -> None:
        imbalance = BookImbalance(2)
        imbalance.update(_depth(bids=[3.0, 1.0], asks=[1.0, 1.0]))
        assert imbalance.value == pytest.approx((4.0 - 2.0) / 6.0)

    def test_an_empty_book_does_not_divide_by_zero(self) -> None:
        imbalance = BookImbalance(2)
        imbalance.update(_depth(bids=[0.0, 0.0], asks=[0.0, 0.0]))
        assert imbalance.value == 0.0

    def test_funding_mean_averages_the_window(self) -> None:
        mean = FundingMean(3)
        for rate in (0.0001, 0.0002, 0.0003):
            mean.update(rate)
        assert mean.value == pytest.approx(0.0002)

    def test_oi_delta_spans_the_declared_number_of_observations(self) -> None:
        delta = OIDelta(2)
        for value in (100.0, 110.0, 130.0):
            delta.update(value)
        assert delta.value == pytest.approx(30.0)
        assert delta.warmup == 3


class TestVWAP:
    def test_weights_by_volume(self) -> None:
        vwap = VWAP(source="close")
        vwap.update(bar(0, close=100.0, volume=1.0))
        vwap.update(bar(1, close=200.0, volume=3.0))
        assert vwap.value == pytest.approx((100.0 + 600.0) / 4.0)

    def test_resets_at_the_session_boundary(self) -> None:
        """Anchored, not rolling. A VWAP that never reset would be a volume-weighted
        moving average wearing the name."""
        vwap = VWAP(session_ms=2 * TF_MS, source="close")
        vwap.update(bar(0, close=100.0, volume=1.0))
        vwap.update(bar(1, close=200.0, volume=1.0))
        assert vwap.value == pytest.approx(150.0)
        vwap.update(bar(2, close=300.0, volume=1.0))  # new session
        assert vwap.value == pytest.approx(300.0)

    def test_a_zero_volume_session_falls_back_to_the_price(self) -> None:
        vwap = VWAP(source="close")
        vwap.update(bar(0, close=100.0, volume=0.0))
        assert vwap.value == pytest.approx(100.0)


def _trade(
    *, qty: float, is_buyer_maker: bool, agg_id: int = 1, symbol: str = "BTCUSDT"
) -> TradePrint:
    """A `TradePrint`, which is what `IndicatorSet.on_trade` is handed by both the engine
    and the validator's smoke run. It used to be a `core.types.AggTrade`, and the two
    disagree about whether `qty` is a scaled integer or a float -- which is how `CVD` came
    to divide by 10^8 twice with a green test."""
    return TradePrint(
        symbol=symbol,
        ts_ms=START,
        price_scaled=scaled(100.0),
        qty_scaled=scaled(qty),
        is_buyer_maker=is_buyer_maker,
        agg_id=agg_id,
    )


def _depth(*, bids: list[float], asks: list[float]) -> DepthSnapshot:
    return DepthSnapshot(
        symbol="BTCUSDT",
        ts_ms=START,
        recv_ms=START,
        last_update_id=1,
        bid_px=tuple(scaled(100.0 - i) for i in range(len(bids))),
        bid_qty=tuple(scaled(q) for q in bids),
        ask_px=tuple(scaled(101.0 + i) for i in range(len(asks))),
        ask_qty=tuple(scaled(q) for q in asks),
    )


# ------------------------------------------------------------------------- registry


class TestIndicatorSet:
    def test_warmup_is_the_max_over_bar_indicators(self) -> None:
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        indicators.ema(12)
        indicators.ema(200)
        indicators.atr(14)
        assert indicators.warmup == 200

    def test_non_bar_indicators_do_not_contribute_to_bar_warmup(self) -> None:
        """"Three funding settlements" has no honest conversion into 15-minute bars --
        the interval is a property of the symbol and has changed historically (spec 3.5).
        Pretending otherwise would produce a warm-up wrong by an unknown amount."""
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        indicators.funding_mean(3)
        indicators.cvd()
        assert indicators.warmup == 0
        assert not indicators.all_ready

    def test_indicators_are_driven_per_symbol(self) -> None:
        indicators = IndicatorSet(
            primary_symbol="BTCUSDT", symbols=("BTCUSDT", "ETHUSDT"), bar_ms=TF_MS
        )
        btc = indicators.sma(2)
        eth = indicators.sma(2, symbol="ETHUSDT")
        for index in range(3):
            indicators.on_bar(bar(index, 100.0 + index))
            indicators.on_bar(bar(index, 10.0 + index, symbol="ETHUSDT"))
        assert btc.value == pytest.approx(101.5)
        assert eth.value == pytest.approx(11.5)

    def test_an_unknown_symbol_is_refused(self) -> None:
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        with pytest.raises(ValueError, match="not in this run"):
            indicators.sma(5, symbol="ETHUSDT")

    def test_a_repeated_bar_is_refused(self) -> None:
        """A bar delivered twice advances every recursive indicator an extra step,
        shifting the whole series against the price permanently and invisibly."""
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        indicators.ema(3)
        indicators.on_bar(bar(5, 100.0))
        with pytest.raises(ValueError, match="does not advance"):
            indicators.on_bar(bar(5, 101.0))
        with pytest.raises(ValueError, match="does not advance"):
            indicators.on_bar(bar(4, 101.0))

    def test_a_replayed_trade_is_dropped_not_double_counted(self) -> None:
        """M33: `on_bar` had a replay guard; the non-bar dispatchers had none.

        CVD is a running total, so one redelivered print -- a reconnect re-sending its
        buffer, a retried poll -- shifted it permanently: the same 2.0-qty trade twice
        read 4.0. `agg_id` is the exchange's own per-symbol monotonic identity and is
        already on the payload, so a print at or below the last seen one is a replay.
        """
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        cvd = indicators.cvd()
        first = _trade(qty=2.0, is_buyer_maker=False, agg_id=10)
        indicators.on_trade(first)
        indicators.on_trade(first)  # replayed verbatim
        assert cvd.value == pytest.approx(2.0)
        assert cvd.updates == 1
        # An *older* id after a newer one is the same failure delivered out of order.
        indicators.on_trade(_trade(qty=5.0, is_buyer_maker=False, agg_id=9))
        assert cvd.value == pytest.approx(2.0)
        # The next genuine print advances normally: +3 sell-aggressive -> 2 - 3.
        indicators.on_trade(_trade(qty=3.0, is_buyer_maker=True, agg_id=11))
        assert cvd.value == pytest.approx(-1.0)

    def test_trade_replay_identity_is_per_symbol(self) -> None:
        """`agg_id` sequences from different symbols overlap freely; a shared high-water
        mark would silently drop most of one symbol's prints in a pairs run."""
        indicators = IndicatorSet(
            primary_symbol="BTCUSDT", symbols=("BTCUSDT", "ETHUSDT"), bar_ms=TF_MS
        )
        btc = indicators.cvd()
        eth = indicators.cvd(symbol="ETHUSDT")
        indicators.on_trade(_trade(qty=2.0, is_buyer_maker=False, agg_id=100))
        indicators.on_trade(
            _trade(qty=1.0, is_buyer_maker=False, agg_id=5, symbol="ETHUSDT")
        )
        assert btc.value == pytest.approx(2.0)
        assert eth.value == pytest.approx(1.0)

    def test_a_replayed_depth_snapshot_is_dropped(self) -> None:
        """The depth mirror of the trade guard, keyed on `last_update_id`: a depth-driven
        indicator's value is a function of the whole series it was fed, so a redelivered
        snapshot is a fabricated extra observation in every window from then on."""
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        imbalance = indicators.book_imbalance(2)
        snapshot = _depth(bids=[3.0, 1.0], asks=[1.0, 1.0])
        indicators.on_depth(snapshot)
        indicators.on_depth(snapshot)
        assert imbalance.updates == 1
        assert imbalance.value == pytest.approx((4.0 - 2.0) / 6.0)

    def test_registering_after_the_first_bar_is_refused(self) -> None:
        """An indicator created mid-run has seen less history than the warm-up gate
        assumes, so it emits unwarmed values that look warm."""
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        indicators.ema(3)
        indicators.freeze()
        with pytest.raises(RuntimeError, match="on_start"):
            indicators.ema(5)

    def test_a_derived_series_cannot_be_registered(self) -> None:
        """Registering `macd.signal` would advance it twice per bar -- once by the set and
        once by its parent -- shifting it a bar against the line it is compared to."""
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
        macd = indicators.macd(3, 6, 4)
        with pytest.raises(TypeError, match="driven by its parent"):
            indicators.register(macd.signal)

    def test_a_derived_series_cannot_be_updated_directly(self) -> None:
        series = DerivedSeries(5)
        with pytest.raises(TypeError, match="driven by its parent"):
            series.update(None)

    def test_realised_volatility_inherits_the_run_timeframe(self) -> None:
        """Annualisation depends on the bar length. An indicator built with the default
        1-minute assumption inside a daily run would overstate vol by ~38x."""
        indicators = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=86_400_000)
        rv = indicators.realised_volatility(20)
        assert rv.periods_per_year == pytest.approx(365.0)

    def test_series_returns_oldest_first_and_never_pads(self) -> None:
        sma = SMA(1)
        feed(sma, [1.0, 2.0, 3.0])
        assert sma.series(2) == (2.0, 3.0)
        assert sma.series(10) == (1.0, 2.0, 3.0)

    def test_series_is_capped_at_the_retention_bound(self) -> None:
        """L8: retention is `history`, and `series(n)` cannot resurrect what was never
        kept. The cap is silent by design -- "everything retained" is a legitimate answer
        -- but it must be the *newest* window, not an arbitrary one."""
        sma = SMA(1, history=8)
        feed(sma, [float(i) for i in range(20)])
        capped = sma.series(1_000)
        assert len(capped) == 8
        # The retained tail is the last 8 of the 20 values pushed: 12.0 .. 19.0.
        assert capped == tuple(float(i) for i in range(12, 20))
        # And a small ask still slices from the newest end.
        assert sma.series(3) == (17.0, 18.0, 19.0)


# -------------------------------------------------------------- the look-ahead test


def _build_all(indicators: IndicatorSet) -> list[Indicator]:
    """One of every bar-driven indicator, at settings small enough to warm up quickly."""
    return [
        indicators.sma(5),
        indicators.ema(8),
        indicators.wma(6),
        indicators.rsi(7),
        indicators.macd(4, 9, 3),
        indicators.atr(5),
        indicators.adx(5),
        indicators.bollinger(10, 2.0),
        indicators.donchian(7),
        indicators.vwap(4 * TF_MS),
        indicators.realised_volatility(6),
        indicators.obv(),
    ]


def _walk(count: int) -> list[Bar]:
    """A deterministic, non-monotonic price series with real highs and lows."""
    bars: list[Bar] = []
    price = 100.0
    for index in range(count):
        price *= 1.0 + math.sin(index * 1.7) * 0.01 + math.cos(index * 0.31) * 0.004
        opened = price * (1 - math.sin(index) * 0.001)
        bars.append(
            bar(
                index,
                close=price,
                open_=opened,
                high=max(price, opened) * 1.002,
                low=min(price, opened) * 0.998,
                volume=10.0 + (index % 7),
            )
        )
    return bars


def test_no_indicator_peeks_forward() -> None:
    """The definitive look-ahead test of spec 12.3, applied to the whole library.

    ```
    full     = run(data[0:N])
    truncate = run(data[0:N-100])
    assert truncate.values == full.values[: len(truncate.values)]
    ```

    If any indicator reads even one bar ahead -- a centred window, a lookahead-by-one in a
    seed, an off-by-one in a Wilder recurrence -- the shared prefix diverges. This is the
    highest-value test in the suite precisely because every other test in this file would
    still pass with the bug present: they check a value against a definition, and a
    forward-reading implementation can satisfy a definition on a fixed series.
    """
    bars = _walk(400)

    full_set = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
    full = _build_all(full_set)
    for b in bars:
        full_set.on_bar(b)

    short_set = IndicatorSet(primary_symbol="BTCUSDT", bar_ms=TF_MS)
    short = _build_all(short_set)
    for b in bars[:300]:
        short_set.on_bar(b)

    assert any(i.ready for i in short), "the truncated run produced no values to compare"

    for truncated, complete in zip(short, full):
        prefix = truncated.series(10_000)
        whole = complete.series(10_000)
        assert prefix, f"{type(truncated).__name__} produced nothing"
        # Exact equality, not approx. Both runs perform the identical sequence of float
        # operations over the shared prefix, so the results are bit-identical or the
        # indicator is doing something that depends on data it should not have seen.
        assert whole[: len(prefix)] == prefix, (
            f"{type(truncated).__name__} diverged: seeing 100 extra bars changed values "
            "it had already produced, which means it reads forward"
        )


def test_the_truncation_test_catches_a_centred_window() -> None:
    """The negative control. A test that cannot fail proves nothing.

    `_Centred` is a three-bar centred moving average, written the way one has to be written
    incrementally: emit a provisional value for the current bar, then *correct the previous
    bar's value* once the following bar arrives. That retroactive edit is the whole of the
    look-ahead -- the series ends up self-consistent and every value is "correct" against
    the centred definition, which is why code review does not catch it.

    Run to 400 bars it is indistinguishable from an honest indicator. Truncated at 300, its
    last value is the uncorrected placeholder while the full run's is the corrected mean,
    and the prefix comparison finds it.
    """

    class _Centred(Indicator):
        feed = "bar"

        def __init__(self) -> None:
            super().__init__()
            self._closes: list[float] = []

        @property
        def warmup(self) -> int:
            return 3

        def update(self, b: Bar) -> None:
            self._updates += 1
            self._closes.append(b.close / SCALE)
            self._push(self._closes[-1])
            if len(self._closes) >= 3:
                corrected = list(self._values)
                corrected[-2] = sum(self._closes[-3:]) / 3.0
                self._values.clear()
                self._values.extend(corrected)

    bars = _walk(50)
    complete = _Centred()
    for b in bars:
        complete.update(b)
    truncated = _Centred()
    for b in bars[:30]:
        truncated.update(b)

    prefix = truncated.series(10_000)
    whole = complete.series(10_000)
    assert len(prefix) == 30
    # Everything except the final value agrees — which is exactly why this bug survives
    # spot checks. The prefix comparison is what refuses it.
    assert whole[: len(prefix) - 1] == prefix[:-1]
    assert whole[: len(prefix)] != prefix
