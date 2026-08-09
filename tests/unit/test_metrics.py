"""Spec 8.2's formulas, against numbers worked out by hand.

Every expected value below is derived in a comment from the series in the test, not read
back out of the implementation. A metric test that computes its expectation the same way the
code does proves only that the code is self-consistent -- which it would be if the
annualisation factor were 252 and the whole point is that it must be 365.
"""

from __future__ import annotations

import math

import pytest

from perplab.analytics.metrics import (
    DAILY_PERIODS,
    HOURLY_PERIODS,
    MS_PER_DAY,
    MS_PER_HOUR,
    build_grid,
    compute_metrics,
    drawdown_stats,
    trade_stats,
)


class FakeTrade:
    """The shape `trade_stats` reads, without dragging the whole `Trade` in.

    A stand-in rather than the real dataclass so a change to `Trade`'s constructor cannot
    silently change what these statistics are computed over.
    """

    def __init__(
        self,
        net: float,
        *,
        duration: int | None = 3600_000,
        reason: str = "signal",
        legs: int = 2,
        exit_ms: int | None = 3600_000,
    ):
        self.net_pnl = net
        self.duration_ms = duration
        self.close_reason = reason
        self.legs = legs
        # `Trade.exit_ms` is `None` exactly while open; the window rule reads it, so the
        # stand-in has to carry it or it is a shape `trade_stats` no longer accepts.
        self.exit_ms = None if reason == "open" else exit_ms

    @property
    def is_open(self) -> bool:
        return self.close_reason == "open"


# The series every formula test below uses: four hourly samples, +10% / -10% / +10%.
#
#   returns  = [110/100-1, 99/110-1, 108.9/99-1] = [+0.1, -0.1, +0.1]
#   mean     = 0.1/3                = 1/30
#   var(n-1) = (2*(1/15)^2 + (2/15)^2)/2 = 0.0133... = 1/75
#   sd       = 1/sqrt(75)
TS = [0, MS_PER_HOUR, 2 * MS_PER_HOUR, 3 * MS_PER_HOUR]
EQUITY = [100.0, 110.0, 99.0, 108.9]
MEAN = 1 / 30
SD = math.sqrt(1 / 75)


def test_the_grid_is_hourly_under_sixty_days_and_daily_above() -> None:
    """Spec 8.1 rule 2. A 30-day backtest gives 30 daily returns, and a Sharpe over 30
    observations has a standard error of roughly 0.19 before annualisation."""
    short = build_grid(TS, EQUITY, start_ms=0, end_ms=3 * MS_PER_HOUR)
    assert short.label == "hourly"
    assert short.periods_per_year == HOURLY_PERIODS == 8760

    long_ts = [i * MS_PER_DAY for i in range(100)]
    long_eq = [100.0 + i for i in range(100)]
    long = build_grid(long_ts, long_eq, start_ms=0, end_ms=99 * MS_PER_DAY)
    assert long.label == "daily"
    assert long.periods_per_year == DAILY_PERIODS == 365


def test_annualisation_is_365_not_252() -> None:
    """Spec 8.1 rule 1. Using the equities convention overstates Sharpe by ~20%."""
    assert DAILY_PERIODS == 365
    assert HOURLY_PERIODS == 24 * 365


def test_sharpe_matches_the_hand_computed_value() -> None:
    metrics = compute_metrics(
        times=TS,
        equity=EQUITY,
        positions_open=[True, True, False, False],
        trades=[],
        start_ms=0,
        end_ms=3 * MS_PER_HOUR,
        traded_notional=0.0,
    )
    expected = MEAN / SD * math.sqrt(HOURLY_PERIODS)
    assert metrics.sharpe == pytest.approx(expected, rel=1e-12)
    assert metrics.sharpe == pytest.approx(27.0185, abs=1e-4)


def test_sortino_divides_by_every_period_not_only_the_losing_ones() -> None:
    """Spec 8.2 says so explicitly, and the difference is not cosmetic.

    Dividing by the count of downside periods would make a strategy look better the rarer
    its losses are, independently of their size -- the opposite of what the ratio is for.
    Here one of three periods is negative, so the two conventions differ by sqrt(3).
    """
    metrics = compute_metrics(
        times=TS,
        equity=EQUITY,
        positions_open=[False] * 4,
        trades=[],
        start_ms=0,
        end_ms=3 * MS_PER_HOUR,
        traded_notional=0.0,
    )
    downside = math.sqrt((0.1**2) / 3)
    expected = MEAN / downside * math.sqrt(HOURLY_PERIODS)
    assert metrics.sortino == pytest.approx(expected, rel=1e-12)

    wrong = MEAN / math.sqrt((0.1**2) / 1) * math.sqrt(HOURLY_PERIODS)
    assert metrics.sortino != pytest.approx(wrong)


def test_drawdown_is_measured_on_every_tick_and_beats_the_grid_figure() -> None:
    """Spec 8.2's note. A grid-close series misses an excursion that recovered inside it.

    The intraperiod trough here is between two hourly boundaries, so the grid never sees it
    and reports a drawdown of zero for a run that fell 20%.
    """
    ticks = [0, MS_PER_HOUR // 2, MS_PER_HOUR, 2 * MS_PER_HOUR]
    equity = [100.0, 80.0, 100.0, 100.0]
    stats = drawdown_stats(ticks, equity)
    assert stats.max_drawdown == pytest.approx(-0.2)
    assert stats.max_drawdown_ms == MS_PER_HOUR // 2

    metrics = compute_metrics(
        times=ticks,
        equity=equity,
        positions_open=[True] * 4,
        trades=[],
        start_ms=0,
        end_ms=2 * MS_PER_HOUR,
        traded_notional=0.0,
    )
    assert metrics.max_drawdown == pytest.approx(-0.2)
    assert metrics.max_drawdown_grid == pytest.approx(0.0)


def test_the_running_peak_never_looks_forward() -> None:
    """A later high must not deepen an earlier drawdown that never happened."""
    stats = drawdown_stats([0, 1, 2], [100.0, 90.0, 1000.0])
    assert stats.max_drawdown == pytest.approx(-0.1)


def test_ulcer_index_is_the_rms_of_the_drawdown_series() -> None:
    stats = drawdown_stats(TS, EQUITY)
    # peaks: 100, 110, 110, 110 -> drawdowns 0, 0, -0.1, -0.01
    expected = math.sqrt((0 + 0 + 0.01 + 0.0001) / 4)
    assert stats.ulcer_index == pytest.approx(expected)
    assert stats.ulcer_index == pytest.approx(0.05024937810560445)


def test_cagr_uses_365_and_reports_minus_one_hundred_percent_when_wiped_out() -> None:
    metrics = compute_metrics(
        times=[0, 365 * MS_PER_DAY],
        equity=[100.0, 200.0],
        positions_open=[False, False],
        trades=[],
        start_ms=0,
        end_ms=365 * MS_PER_DAY,
        traded_notional=0.0,
    )
    assert metrics.cagr == pytest.approx(1.0)

    wiped = compute_metrics(
        times=[0, 365 * MS_PER_DAY],
        equity=[100.0, 0.0],
        positions_open=[False, False],
        trades=[],
        start_ms=0,
        end_ms=365 * MS_PER_DAY,
        traded_notional=0.0,
    )
    assert wiped.cagr == -1.0
    # And Calmar follows CAGR rather than raising on the same series.
    assert wiped.calmar == pytest.approx(-1.0)


def test_exposure_is_weighted_by_wall_time_not_by_sample_count() -> None:
    """Uneven sampling is the normal case: mark bars are dense in a busy market.

    Two of four samples hold a position, but each covers a different span, so counting
    samples would give 50% where the honest answer is 90%.
    """
    times = [0, 9 * MS_PER_HOUR, 10 * MS_PER_HOUR, 10 * MS_PER_HOUR + 1]
    metrics = compute_metrics(
        times=times,
        equity=[100.0] * 4,
        positions_open=[True, False, False, False],
        trades=[],
        start_ms=0,
        end_ms=10 * MS_PER_HOUR,
        traded_notional=0.0,
    )
    assert metrics.exposure == pytest.approx(0.9)


def test_turnover_is_notional_over_mean_equity() -> None:
    metrics = compute_metrics(
        times=[0, MS_PER_HOUR],
        equity=[100.0, 300.0],
        positions_open=[True, False],
        trades=[],
        start_ms=0,
        end_ms=MS_PER_HOUR,
        traded_notional=400.0,
    )
    assert metrics.turnover == pytest.approx(400.0 / 200.0)


def test_a_short_profitable_range_does_not_crash_on_the_annualisation() -> None:
    """`3 ** (365/(1/24))` is `3 ** 8760`, which is outside float, not merely large.

    Python raises `OverflowError` there, which is an `ArithmeticError` and so escapes the
    `math.isfinite` guard. Before this was handled, a one-hour backtest that happened to go
    well killed the entire metrics computation -- so the better the run, the more likely it
    was to produce nothing at all. `None` is the honest report; `total_return` still works.
    """
    metrics = compute_metrics(
        times=[0, MS_PER_HOUR],
        equity=[100.0, 300.0],
        positions_open=[True, False],
        trades=[],
        start_ms=0,
        end_ms=MS_PER_HOUR,
        traded_notional=0.0,
    )
    assert metrics.cagr is None
    assert metrics.calmar is None
    assert metrics.total_return == pytest.approx(2.0)


def test_undefined_metrics_are_none_and_never_zero_or_nan() -> None:
    """A Sharpe over one return, a Calmar with no drawdown, a profit factor with no loss.

    Each substitute lies in a way that survives a sort: 0.0 reads as flat, infinity ranks an
    untested strategy first, and NaN compares false against itself.
    """
    metrics = compute_metrics(
        times=[0, MS_PER_HOUR],
        equity=[100.0, 100.0],
        positions_open=[False, False],
        trades=[],
        start_ms=0,
        end_ms=MS_PER_HOUR,
        traded_notional=0.0,
    )
    assert metrics.sharpe is None  # one return
    assert metrics.sortino is None  # no downside deviation
    assert metrics.calmar is None  # no drawdown
    assert metrics.trades.profit_factor is None
    assert metrics.trades.win_rate is None


def test_the_return_series_stops_when_equity_reaches_zero() -> None:
    """A return needs a positive denominator. An account at zero stops producing returns."""
    grid = build_grid(
        [0, MS_PER_HOUR, 2 * MS_PER_HOUR, 3 * MS_PER_HOUR],
        [100.0, 0.0, 0.0, 0.0],
        start_ms=0,
        end_ms=3 * MS_PER_HOUR,
    )
    assert grid.returns == (-1.0,)
    assert grid.truncated_at_ms == MS_PER_HOUR


def test_a_flat_series_has_no_drawdown_and_no_ratio() -> None:
    stats = drawdown_stats([0, 1, 2], [50.0, 50.0, 50.0])
    assert stats.max_drawdown == 0.0
    assert stats.ulcer_index == 0.0


# --------------------------------------------------------------------------- trade stats


def test_open_trades_are_excluded_from_every_ratio() -> None:
    """Folding an unrealised result into a win rate is how a losing strategy reports well:
    the losers are closed and the winner is 'still running'."""
    trades = [FakeTrade(-10.0), FakeTrade(-20.0), FakeTrade(1000.0, reason="open")]
    stats = trade_stats(trades)
    assert stats.round_trips == 2
    assert stats.open_trades == 1
    assert stats.win_rate == 0.0
    assert stats.expectancy == pytest.approx(-15.0)


def test_scratches_are_counted_separately_from_wins_and_losses() -> None:
    stats = trade_stats([FakeTrade(5.0), FakeTrade(0.0), FakeTrade(-5.0)])
    assert (stats.wins, stats.losses, stats.scratches) == (1, 1, 1)
    assert stats.win_rate == pytest.approx(1 / 3)


def test_profit_factor_and_payoff_ratio() -> None:
    stats = trade_stats([FakeTrade(30.0), FakeTrade(10.0), FakeTrade(-20.0)])
    assert stats.profit_factor == pytest.approx(40 / 20)
    assert stats.avg_win == pytest.approx(20.0)
    assert stats.avg_loss == pytest.approx(-20.0)
    assert stats.payoff_ratio == pytest.approx(1.0)
    assert stats.largest_win == 30.0
    assert stats.largest_loss == -20.0


def test_per_trade_sharpe_is_reported_separately_and_is_not_annualised() -> None:
    """Spec 8.1 rule 2: a different, non-comparable statistic, labelled as such."""
    stats = trade_stats([FakeTrade(10.0), FakeTrade(-10.0), FakeTrade(10.0)])
    mean = 10 / 3
    var = ((10 - mean) ** 2 * 2 + (-10 - mean) ** 2) / 2
    assert stats.per_trade_sharpe == pytest.approx(mean / math.sqrt(var))
    # No annualisation factor anywhere in it.
    assert abs(stats.per_trade_sharpe) < 1.0


def test_liquidated_trades_are_counted() -> None:
    stats = trade_stats([FakeTrade(-100.0, reason="liquidation"), FakeTrade(5.0)])
    assert stats.liquidated == 1


def test_an_empty_run_produces_no_statistics_rather_than_zeros() -> None:
    stats = trade_stats([])
    assert stats.round_trips == 0
    assert stats.win_rate is None
    assert stats.expectancy is None
    assert stats.per_trade_sharpe is None


def test_a_mismatched_series_is_refused() -> None:
    with pytest.raises(ValueError, match="values for"):
        build_grid([0, 1], [1.0], start_ms=0, end_ms=2)
