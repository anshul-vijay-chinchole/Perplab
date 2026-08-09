"""Performance metrics (spec 8.2), with spec 8.1's conventions applied rather than assumed.

Spec 8.1 names three definitional choices *"that are routinely got wrong and that silently
inflate results"*. All three are load-bearing here:

1. **The annualisation factor is 365, not 252.** Crypto trades every day. Using the equities
   convention overstates Sharpe by `sqrt(365/252)` -- about 20%, applied uniformly, which is
   large enough to move a strategy from "not worth trading" to "promising" and invisible
   unless you go looking for it.
2. **Returns are on a fixed time grid, not per trade.** Daily UTC boundaries by default,
   hourly for ranges under 60 days. A per-trade "Sharpe" is a different statistic that is
   not comparable with anything published; it is computed here too, under its own name, so
   nobody has to reach for the wrong one to get it.
3. **Drawdown is measured on every mark-to-market tick, not on grid closes.** Grid-close
   drawdown understates the real figure, sometimes badly -- an intraday collapse and
   recovery is invisible at daily resolution. Both are reported, labelled, so comparisons
   with tools that only do the grid version remain possible.

**Everything here is `float`, deliberately.** Spec 3.1 puts money in `Decimal` and analytics
in `float64`, and these are ratios of ratios: a Sharpe carried to fifty significant digits
would be false precision over a sample of a few hundred returns. The exact money figures --
net PnL, fees, funding, the attribution identity -- come from the ledger and never pass
through this module.

**Undefined is `None`, never `NaN` and never a placeholder.** A Sharpe over one return, a
Calmar with no drawdown, a profit factor with no losing trade: each is genuinely undefined,
and every substitute lies. `0.0` reads as "flat", `inf` breaks JSON, and `NaN` compares
false against itself and quietly poisons any sort the results page performs.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

__all__ = [
    "DAILY_PERIODS",
    "HOURLY_PERIODS",
    "HOURLY_GRID_MAX_DAYS",
    "MS_PER_DAY",
    "MS_PER_HOUR",
    "ReturnGrid",
    "DrawdownStats",
    "TradeStats",
    "Metrics",
    "build_grid",
    "grid_step",
    "drawdown_stats",
    "trade_stats",
    "compute_metrics",
    "annualised_return",
]

MS_PER_HOUR = 3_600_000
MS_PER_DAY = 86_400_000

DAILY_PERIODS = 365
"""Spec 8.1. Not 252 -- see the module docstring."""

HOURLY_PERIODS = 24 * DAILY_PERIODS  # 8760

HOURLY_GRID_MAX_DAYS = 60
"""Spec 8.1: hourly returns for backtests shorter than 60 days.

Below that a daily grid leaves too few points for a standard deviation to mean anything --
a 30-day backtest gives 30 returns, and a Sharpe over 30 observations has a standard error
of roughly 0.19 before annualisation.
"""


def _finite(value: float) -> float | None:
    """`None` for anything that is not a real number. See the module docstring."""
    return value if math.isfinite(value) else None


@dataclass(frozen=True, slots=True)
class ReturnGrid:
    """Equity resampled onto fixed UTC boundaries, and the returns between them."""

    label: str
    periods_per_year: int
    times: tuple[int, ...]
    equity: tuple[float, ...]
    returns: tuple[float, ...]
    truncated_at_ms: int | None = None
    """Set when the series was cut short because equity reached zero or below.

    A return needs a positive denominator. An account that reaches zero has not produced a
    -100% return followed by undefined ones; it has stopped producing returns, and
    continuing the series past that point would divide by a number that means the account
    no longer exists. The cut is reported so the results page can say the run blew up
    rather than showing a Sharpe computed over the part before it did.
    """

    @property
    def periods(self) -> int:
        return len(self.returns)


@dataclass(frozen=True, slots=True)
class DrawdownStats:
    """Drawdown measured on every mark-to-market tick (spec 8.2's note)."""

    max_drawdown: float | None
    """Most negative value of `E_t / cummax(E)_t - 1`. Negative or zero, never positive."""
    max_drawdown_ms: int | None
    peak_equity: float | None
    trough_equity: float | None
    ulcer_index: float | None
    """`sqrt(mean(DD_t^2))` over the same ticks, as a fraction (0.05 is a 5% ulcer)."""


@dataclass(frozen=True, slots=True)
class TradeStats:
    """Round-trip statistics (spec 8.1 rule 3: a trade is flat -> flat)."""

    round_trips: int
    legs: int
    wins: int
    losses: int
    scratches: int
    """Round-trips whose net PnL was exactly zero.

    Counted separately rather than folded into losses. Folding them in understates win rate
    on a strategy that scratches often, and calling them wins overstates it; they are
    neither, and a strategy with many of them is telling you something about its exits.
    """
    win_rate: float | None
    profit_factor: float | None
    expectancy: float | None
    payoff_ratio: float | None
    avg_win: float | None
    avg_loss: float | None
    largest_win: float | None
    largest_loss: float | None
    avg_duration_ms: float | None
    per_trade_sharpe: float | None
    """Mean/stdev of per-trade PnL. **Not annualised and not comparable to `sharpe`.**

    Spec 8.1: per-trade Sharpe "is a different, non-comparable statistic and is reported
    separately and labelled as such". It has no time dimension, so a strategy trading once a
    month and one trading hourly produce numbers on the same scale that mean nothing alike.
    """
    open_trades: int
    liquidated: int


@dataclass(frozen=True, slots=True)
class Metrics:
    """The spec 8.2 set, plus the counters the results page needs to caveat them."""

    grid: str
    periods: int
    periods_per_year: int
    sharpe: float | None
    sortino: float | None
    cagr: float | None
    calmar: float | None
    max_drawdown: float | None
    max_drawdown_grid: float | None
    max_drawdown_ms: int | None
    ulcer_index: float | None
    exposure: float | None
    turnover: float | None
    volatility: float | None
    """Annualised standard deviation of grid returns. Not in spec 8.2's list, and reported
    because every ratio above is a quotient with it hiding in the denominator: a Sharpe of
    0.4 on 20% vol and one on 200% vol are different claims about a strategy."""
    total_return: float | None
    days: float
    trades: TradeStats
    truncated_at_ms: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "grid": self.grid,
            "periods": self.periods,
            "periods_per_year": self.periods_per_year,
            "sharpe": self.sharpe,
            "sortino": self.sortino,
            "cagr": self.cagr,
            "calmar": self.calmar,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_grid": self.max_drawdown_grid,
            "max_drawdown_ms": self.max_drawdown_ms,
            "ulcer_index": self.ulcer_index,
            "exposure": self.exposure,
            "turnover": self.turnover,
            "volatility": self.volatility,
            "total_return": self.total_return,
            "days": self.days,
            "truncated_at_ms": self.truncated_at_ms,
            "trades": {
                "round_trips": self.trades.round_trips,
                "legs": self.trades.legs,
                "wins": self.trades.wins,
                "losses": self.trades.losses,
                "scratches": self.trades.scratches,
                "win_rate": self.trades.win_rate,
                "profit_factor": self.trades.profit_factor,
                "expectancy": self.trades.expectancy,
                "payoff_ratio": self.trades.payoff_ratio,
                "avg_win": self.trades.avg_win,
                "avg_loss": self.trades.avg_loss,
                "largest_win": self.trades.largest_win,
                "largest_loss": self.trades.largest_loss,
                "avg_duration_ms": self.trades.avg_duration_ms,
                "per_trade_sharpe": self.trades.per_trade_sharpe,
                "open_trades": self.trades.open_trades,
                "liquidated": self.trades.liquidated,
            },
        }


# --------------------------------------------------------------------------- the grid


def grid_step(start_ms: int, end_ms: int) -> tuple[str, int, int]:
    """`(label, periods_per_year, step_ms)` for a range -- spec 8.1's grid rule.

    A function of the range and nothing else, which is what lets two series over the same
    range land on identical boundaries. Public because the Lab's portfolio report samples
    per-symbol PnL on this grid; a second copy of the 60-day threshold would be the place
    the run page and the portfolio page start disagreeing about what "daily" means.
    """
    if end_ms <= start_ms:
        raise ValueError(f"empty range: end {end_ms} does not follow start {start_ms}")
    span_days = (end_ms - start_ms) / MS_PER_DAY
    if span_days < HOURLY_GRID_MAX_DAYS:
        return "hourly", HOURLY_PERIODS, MS_PER_HOUR
    return "daily", DAILY_PERIODS, MS_PER_DAY


def build_grid(
    times: Sequence[int],
    equity: Sequence[float],
    *,
    start_ms: int,
    end_ms: int,
) -> ReturnGrid:
    """Resample the mark-to-market series onto **aligned** UTC boundaries.

    **Last observation at or before each boundary**, which is the only sampling rule that
    does not invent data: it is the same LOCF convention spec 3.4 applies to mark price, and
    interpolating between two equity samples would report a balance the account never had.

    **Only whole grid steps become periods.** Spec 8.1's rule is a *fixed* time grid, and a
    range that does not start and end on a boundary has a stub at each end. Including them
    was the bug: a 60-second head remainder became one grid period and was annualised by the
    same `sqrt(A)` as a full day, so a 5% move in that minute produced a Sharpe of 2.43 on a
    series that is otherwise flat. The remainders are therefore excluded from the *return*
    series -- they remain in `equity`, so `total_return`, `CAGR` and every drawdown figure
    still see them, which is where a partial final day belongs.

    A range shorter than one grid step yields no returns at all, and the ratios that need
    them report `None` rather than a number computed from a single stub.
    """
    if len(times) != len(equity):
        raise ValueError(
            f"the equity series has {len(equity)} values for {len(times)} timestamps"
        )
    label, periods_per_year, step = grid_step(start_ms, end_ms)

    if not times:
        return ReturnGrid(label, periods_per_year, (), (), ())

    # Aligned boundaries only: the first at or after `start_ms`, the last at or before
    # `end_ms`. Integer arithmetic on the epoch, so every boundary is an exact UTC instant.
    first = -(-start_ms // step) * step
    last = (end_ms // step) * step

    sampled_times: list[int] = []
    sampled_equity: list[float] = []
    boundary = first
    while boundary <= last:
        index = bisect_right(times, boundary) - 1
        if index >= 0:
            sampled_times.append(boundary)
            sampled_equity.append(equity[index])
        boundary += step

    returns: list[float] = []
    truncated: int | None = None
    for index in range(1, len(sampled_equity)):
        previous = sampled_equity[index - 1]
        if previous <= 0:
            truncated = sampled_times[index - 1]
            del sampled_times[index:]
            del sampled_equity[index:]
            break
        returns.append(sampled_equity[index] / previous - 1.0)

    return ReturnGrid(
        label=label,
        periods_per_year=periods_per_year,
        times=tuple(sampled_times),
        equity=tuple(sampled_equity),
        returns=tuple(returns),
        truncated_at_ms=truncated,
    )


# ------------------------------------------------------------------------- drawdown


def drawdown_stats(
    times: Sequence[int],
    equity: Sequence[float],
    low: Sequence[float] | None = None,
    high: Sequence[float] | None = None,
) -> DrawdownStats:
    """Running-peak drawdown over every sample, plus the ulcer index.

    The peak is tracked forward from the first sample -- never reset, and never taken as the
    maximum of the whole series, which would let a later peak retroactively deepen an
    earlier drawdown that never happened.

    **`low` and `high` are the intrabar band, not extra samples.** Spec 8.2 requires
    drawdown on every mark-to-market tick and says grid closes understate it; a 1-minute
    close series understates it too, because the mark provably traversed a range inside each
    bar. So the peak is raised by `high` and the trough scored at `low`, both within the same
    sample. That is deliberately the pessimistic reading of an ordering the data cannot
    resolve -- the crest and the trough both happened, and taking the worse pairing is the
    direction spec 1.4 requires.

    Feeding the extremes as *ordered points* instead, which is what this used to do upstream,
    is wrong in two ways at once: the first one written is scored against a peak the second
    has not yet raised, which makes longs and shorts report different drawdowns on identical
    price paths; and moving every symbol to its own extreme simultaneously fabricates a joint
    state, which for a market-neutral book cancels and reports no drawdown at all.

    A non-positive peak makes the ratio meaningless (an account starting at zero has no
    percentage to fall), so those samples are skipped rather than contributing an infinity.
    """
    if not equity:
        return DrawdownStats(None, None, None, None, None)
    lows = low if low is not None and len(low) == len(equity) else equity
    highs = high if high is not None and len(high) == len(equity) else equity

    peak = max(equity[0], highs[0])
    worst = 0.0
    worst_ms: int | None = None
    trough = equity[0]
    square_sum = 0.0
    counted = 0

    for index, value in enumerate(equity):
        crest = highs[index]
        if crest > peak:
            peak = crest
        if peak <= 0:
            continue
        drawdown = lows[index] / peak - 1.0
        counted += 1
        square_sum += drawdown * drawdown
        if drawdown < worst:
            worst = drawdown
            worst_ms = times[index]
            trough = lows[index]

    if counted == 0:
        return DrawdownStats(None, None, None, None, None)

    return DrawdownStats(
        max_drawdown=_finite(worst),
        max_drawdown_ms=worst_ms,
        peak_equity=_finite(peak),
        trough_equity=_finite(trough),
        ulcer_index=_finite(math.sqrt(square_sum / counted)),
    )


# ---------------------------------------------------------------------------- trades


def trade_stats(
    trades: Sequence[Any],
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> TradeStats:
    """Round-trip statistics from `analytics.trades.Trade` records.

    Open trades are excluded from every ratio and counted separately. A position still open
    when the run ended has an unrealised result, and folding an unrealised number into a win
    rate is how a losing strategy reports a good one: the losers are closed and the winner
    is "still running".

    **Windowed like everything else, when a window is given -- and `compute_metrics`
    always gives one.** The window rule is stated once, in `compute_metrics`'s docstring:
    every figure describes `[start_ms, end_ms]` and nothing else. This function was the
    exception -- the one un-windowed figure on the results page -- so `drain()`'s
    legitimate post-`end_ms` fills and a halted run's truncated range still moved
    `win_rate`, `profit_factor` and `expectancy`. The windowed reading of a round-trip:

    - closed inside the window -- a member of every ratio;
    - closed *after* `end_ms` -- **open as of the window's end**, which is what the
      figures claim to describe, so it counts in `open_trades` and in no ratio;
    - closed *before* `start_ms` -- outside the run's own account of itself entirely
      (unreachable from the engine, whose warm-up gate forbids trading before
      `start_ms`; reachable from a caller windowing a wider table) and excluded.

    `legs` counts the trades the window can see (closed-in-window plus open), because it
    is the denominator of "how much did one round trip cost to build" and must cover the
    same population as the ratios beside it.
    """
    in_window = [
        t
        for t in trades
        if t.is_open
        or t.exit_ms is None
        or start_ms is None
        or t.exit_ms >= start_ms
    ]
    closed = [
        t
        for t in in_window
        if not t.is_open
        and t.exit_ms is not None
        and (end_ms is None or t.exit_ms <= end_ms)
    ]
    open_trades = len(in_window) - len(closed)
    pnls = [float(t.net_pnl) for t in closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    scratches = len(pnls) - len(wins) - len(losses)
    durations = [t.duration_ms for t in closed if t.duration_ms is not None]

    gross_win = sum(wins)
    gross_loss = abs(sum(losses))

    return TradeStats(
        round_trips=len(closed),
        legs=sum(t.legs for t in in_window),
        wins=len(wins),
        losses=len(losses),
        scratches=scratches,
        win_rate=(len(wins) / len(closed)) if closed else None,
        # `None` rather than infinity when nothing lost. A strategy with no losing trade
        # has an undefined profit factor, and rendering it as infinity in a sortable
        # column puts an untested strategy at the top of the list.
        profit_factor=(gross_win / gross_loss) if gross_loss > 0 else None,
        expectancy=(sum(pnls) / len(pnls)) if pnls else None,
        payoff_ratio=(
            (gross_win / len(wins)) / (gross_loss / len(losses))
            if wins and losses
            else None
        ),
        avg_win=(gross_win / len(wins)) if wins else None,
        avg_loss=(-gross_loss / len(losses)) if losses else None,
        largest_win=max(wins) if wins else None,
        largest_loss=min(losses) if losses else None,
        avg_duration_ms=(sum(durations) / len(durations)) if durations else None,
        per_trade_sharpe=_sample_ratio(pnls),
        open_trades=open_trades,
        liquidated=sum(1 for t in closed if t.close_reason == "liquidation"),
    )


def _sample_ratio(values: Sequence[float]) -> float | None:
    """`mean / stdev(ddof=1)`, or `None` when fewer than two samples or zero dispersion."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    if variance <= 0:
        return None
    return _finite(mean / math.sqrt(variance))


# ------------------------------------------------------------------------ everything


def compute_metrics(
    *,
    times: Sequence[int],
    equity: Sequence[float],
    positions_open: Sequence[bool],
    trades: Sequence[Any],
    start_ms: int,
    end_ms: int,
    traded_notional: float,
    equity_low: Sequence[float] | None = None,
    equity_high: Sequence[float] | None = None,
    risk_free_annual: float = 0.0,
    mar_annual: float = 0.0,
) -> Metrics:
    """The full spec 8.2 set over one run's mark-to-market series.

    **Every figure is computed over `[start_ms, end_ms]` and nothing else.** The series
    handed in is longer at both ends: it begins in the warm-up window, where the strategy
    cannot trade at all, and it can end after `end_ms` because an order submitted on the
    final bar arrives a moment later and genuinely fills. Neither belongs in a *return*
    measured over the range the user asked for, and mixing them was a real error rather than
    a tidiness point -- exposure divided by `end_ms - times[0]` charged the whole warm-up to
    the denominator, understating a 60-day run with a 400-day warm-up by 7.7x.

    **The window rule, stated once and applied everywhere:** the returns grid, the
    drawdown band, exposure, the trade ratios (`trade_stats`, which treats a round trip
    closed after `end_ms` as open-at-window-end) and both halves of `turnover` --
    `traded_notional` must arrive already windowed, which the engine does at accumulation
    since only it sees per-fill timestamps. A figure on this page that covered a different
    period from the one beside it would invite exactly the cross-figure arithmetic that
    cannot work.

    The window is taken by last-observation-carried-forward at each end, so the opening
    figure is the equity in force *at* `start_ms` even when no sample lands exactly there.

    `positions_open[i]` says whether any position was open at `times[i]`; exposure is the
    fraction of *wall time* it was true, weighted by the interval each sample covers rather
    than by sample count. Unweighted counting would let a dense cluster of samples during a
    volatile hour outvote a quiet week.

    `equity_low`/`equity_high` are the intrabar band -- see `drawdown_stats`.

    `risk_free_annual` and `mar_annual` default to zero, matching spec 8.2's own defaults.
    They are de-annualised geometrically -- `(1 + rf)^(1/A) - 1` -- because compounding a
    linear approximation over 8 760 hourly periods is not the same number.
    """
    lo, hi = _window(times, start_ms, end_ms)
    if lo is None or hi is None:
        window_times: Sequence[int] = ()
        window_equity: Sequence[float] = ()
        window_open: Sequence[bool] = ()
        window_low: Sequence[float] | None = None
        window_high: Sequence[float] | None = None
    else:
        window_times = times[lo : hi + 1]
        window_equity = equity[lo : hi + 1]
        window_open = positions_open[lo : hi + 1]
        window_low = None if equity_low is None else equity_low[lo : hi + 1]
        window_high = None if equity_high is None else equity_high[lo : hi + 1]

    grid = build_grid(window_times, window_equity, start_ms=start_ms, end_ms=end_ms)
    drawdown = drawdown_stats(window_times, window_equity, window_low, window_high)
    # Windowed like every other figure -- see `trade_stats` for what the window means for
    # a round trip. This call and `traded_notional` (whose window the engine applies at
    # accumulation, because only it sees per-fill timestamps) are what keep `turnover`
    # a ratio of two figures measured over the same period.
    stats = trade_stats(trades, start_ms=start_ms, end_ms=end_ms)

    periods_per_year = grid.periods_per_year
    rf_period = (1.0 + risk_free_annual) ** (1.0 / periods_per_year) - 1.0
    mar_period = (1.0 + mar_annual) ** (1.0 / periods_per_year) - 1.0

    sharpe = _sharpe(grid.returns, rf_period, periods_per_year)
    sortino = _sortino(grid.returns, mar_period, periods_per_year)
    volatility = _volatility(grid.returns, periods_per_year)

    days = (end_ms - start_ms) / MS_PER_DAY
    opening = window_equity[0] if window_equity else None
    closing = window_equity[-1] if window_equity else None
    total_return = (
        closing / opening - 1.0 if opening is not None and opening > 0 else None
    )
    cagr = _cagr(opening, closing, days)
    calmar = (
        cagr / abs(drawdown.max_drawdown)
        if cagr is not None
        and drawdown.max_drawdown is not None
        and drawdown.max_drawdown < 0
        else None
    )

    grid_drawdown = drawdown_stats(grid.times, grid.equity).max_drawdown
    mean_equity = (
        (sum(window_equity) / len(window_equity)) if window_equity else 0.0
    )

    return Metrics(
        grid=grid.label,
        periods=grid.periods,
        periods_per_year=periods_per_year,
        sharpe=sharpe,
        sortino=sortino,
        cagr=cagr,
        calmar=calmar,
        max_drawdown=drawdown.max_drawdown,
        max_drawdown_grid=grid_drawdown,
        max_drawdown_ms=drawdown.max_drawdown_ms,
        ulcer_index=drawdown.ulcer_index,
        exposure=_exposure(window_times, window_open, start_ms, end_ms),
        turnover=(traded_notional / mean_equity) if mean_equity > 0 else None,
        volatility=volatility,
        total_return=total_return,
        days=days,
        trades=stats,
        truncated_at_ms=grid.truncated_at_ms,
    )


def _window(
    times: Sequence[int], start_ms: int, end_ms: int
) -> tuple[int | None, int | None]:
    """Indices bounding `[start_ms, end_ms]`, carrying the last observation into each end.

    `lo` is the last sample at or before `start_ms` -- the equity in force when the range
    opened -- falling back to the first sample when the series starts later. `hi` is the last
    sample at or before `end_ms`, so a fill that landed after the range is excluded from
    every figure measured over it.
    """
    if not times:
        return None, None
    lo = bisect_right(times, start_ms) - 1
    if lo < 0:
        lo = 0
    hi = bisect_right(times, end_ms) - 1
    if hi < lo:
        hi = lo
    return lo, hi


def _sharpe(
    returns: Sequence[float], rf_period: float, periods_per_year: int
) -> float | None:
    """`(mean(r) - rf) / stdev(r, ddof=1) * sqrt(A)` (spec 8.2).

    `ddof=1` because these are a *sample* of the strategy's return distribution, not its
    population. With 365 daily returns the difference is under 0.2%, but with 30 it is 1.7%
    and always in the flattering direction.
    """
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    if variance <= 0:
        return None
    return _finite(
        (mean - rf_period) / math.sqrt(variance) * math.sqrt(periods_per_year)
    )


def _sortino(
    returns: Sequence[float], mar_period: float, periods_per_year: int
) -> float | None:
    """`(mean(r) - MAR) / DD * sqrt(A)`, `DD = sqrt(sum(min(r - MAR, 0)^2) / N)`.

    **Divided by `N`, every period, not by the count of losing ones.** Spec 8.2 says so
    explicitly, and the distinction is not cosmetic: dividing by the number of downside
    periods only would make a strategy that is rarely negative look better the rarer its
    losses are, independently of how large they are, which is the opposite of what the ratio
    is for.
    """
    if not returns:
        return None
    mean = sum(returns) / len(returns)
    downside = sum(min(r - mar_period, 0.0) ** 2 for r in returns) / len(returns)
    if downside <= 0:
        # No period fell below the MAR. The ratio is genuinely undefined -- there is no
        # downside deviation to divide by -- and a large placeholder would rank a strategy
        # with three lucky periods above one with a real record.
        return None
    return _finite(
        (mean - mar_period) / math.sqrt(downside) * math.sqrt(periods_per_year)
    )


def _volatility(returns: Sequence[float], periods_per_year: int) -> float | None:
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    if variance < 0:  # pragma: no cover - a sum of squares cannot be negative
        return None
    return _finite(math.sqrt(variance) * math.sqrt(periods_per_year))


def _cagr(opening: float | None, closing: float | None, days: float) -> float | None:
    """`(E_T / E_0)^(365/days) - 1`, and `-100%` when the account ended at or below zero.

    Spec 8.2 states the `E_T <= 0` case directly. Without it the expression raises or
    returns a complex root, and a wiped-out account is exactly the run whose metrics
    someone most needs to read.

    **The exponent can overflow, and it does so on a plausible run.** Annualising is
    `ratio ** (365/days)`, and for a range far shorter than a year that exponent is enormous:
    a one-hour backtest that tripled gives `3 ** 8760`, which is not merely large but outside
    float entirely. Python raises `OverflowError` there -- an `ArithmeticError`, so the
    `_finite` guard below never sees it -- and the whole metrics computation dies on a run
    that went unusually *well*.

    `None` is the honest report. The figure is not "zero" and not "infinite"; it is an
    extrapolation of an hour across a year, which no float can hold and no reader should act
    on. `total_return` is unaffected and is the meaningful number for such a range.
    """
    if opening is None or closing is None or opening <= 0 or days <= 0:
        return None
    if closing <= 0:
        return -1.0
    try:
        return _finite((closing / opening) ** (DAILY_PERIODS / days) - 1.0)
    except OverflowError:
        return None


def annualised_return(total_return: float | None, days: float) -> float | None:
    """`(1 + r)^(365/days) - 1`, with every guard `_cagr` has.

    The walk-forward's WFE (spec 9.1) is a ratio of *annualised* returns, and its windows
    are normal metric windows: short enough to overflow the exponent on a good month and
    capable of ending at or below zero. One implementation of the annualisation, one set of
    guards -- a second copy in the Lab would be the place they drift apart.
    """
    if total_return is None:
        return None
    return _cagr(1.0, 1.0 + total_return, days)


def _exposure(
    times: Sequence[int],
    positions_open: Sequence[bool],
    start_ms: int,
    end_ms: int,
) -> float | None:
    """Fraction of the **run's** wall time with a position open (spec 8.2).

    The denominator is `end_ms - start_ms`, not `end_ms - times[0]`. The series begins in the
    warm-up window, where the strategy is gated out of trading entirely, so charging that
    time to the denominator understates exposure by an amount that scales with the declared
    history: a 60-day run behind a 400-day warm-up reported 13% for a position held every
    minute of the range.

    Each sample covers the interval until the next one, clipped to the range at both ends, so
    a sample carried in from before `start_ms` contributes only the part inside it. Weighting
    by interval rather than by sample count matters as soon as sampling is uneven, which it
    is: mark bars are dense in a busy market and absent across a gap.
    """
    if len(times) != len(positions_open) or not times:
        return None
    total = end_ms - start_ms
    if total <= 0:
        return None
    held = 0
    for index in range(len(times)):
        nxt = times[index + 1] if index + 1 < len(times) else end_ms
        if not positions_open[index]:
            continue
        lo = max(times[index], start_ms)
        hi = min(nxt, end_ms)
        held += max(0, hi - lo)
    return held / total
