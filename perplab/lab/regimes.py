"""Regime analysis of a completed run (spec 9.3).

**Regimes must be defined causally or the analysis is itself look-ahead.** Spec 9.3 is
blunt about the standard mistake: bucketing by quantiles computed over the whole sample
uses future information to label the past -- a "high-volatility" label assigned to March
because of what volatility did in September. Everything here is built so the label a
period wears was computable *at that period's open*:

- **Volatility** -- trailing realised vol from *daily* closes, labelled low/normal/high by
  **expanding-window tertiles** (at time `t`, boundaries from observations up to `t` only)
  or by fixed absolute thresholds set in advance. Both are spec 9.3's own two options; the
  whole-sample quantile is not offered at all.
- **Trend vs range** -- ADX(14) on the daily bar against a fixed threshold of 25.
- **Funding** -- trailing mean of realised settlements over a time window, against a fixed
  dead-band around zero. Perp-specific and genuinely informative: many strategies only
  work in one funding environment, and the settlement series is the honest input (a
  predicted rate is a forecast that changes until the settlement millisecond).
- **Cascade** -- periods within a tag window after a liquidation cluster whose summed
  notional crossed a threshold. The tag extends *forward* from the cluster only, which is
  what makes it causal: "we are inside the aftermath of a cascade" is knowable live,
  "a cascade is about to happen" is not.

The indicator arithmetic is **the strategy library's own** -- `ADX`, `RealisedVolatility`
from `perplab.strategy.indicators` -- not a re-derivation. A Lab that smoothed Wilder's
DX differently from the strategies it analyses would file the same market under two
regimes depending on who asked.

**Labels step, they do not interpolate.** Each dimension yields a step series of
`(effective_ms, label)` change points; a period is labelled by the last change at or
before its open (LOCF, the spec 3.4 convention). Before a dimension's first defined value
-- the vol window still filling, ADX still seeding -- periods are labelled `unclassified`
and *counted*: a run whose first month cannot be classified says so, rather than silently
folding that month into whichever bucket came first.

**A regime with 4 observations is not evidence** (spec 9.3). Every bucket carries `thin`,
set when it has fewer than the configured minimum of periods or of trades, and the UI
greys those statistics rather than presenting a meaningless Sharpe. The numbers are still
there -- greyed is not hidden -- because "too few to mean anything" is itself a finding.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from perplab.core.types import Bar
from perplab.lab.stats import sharpe_ratio
from perplab.strategy.indicators import ADX, RealisedVolatility

__all__ = [
    "RegimeConfig",
    "RegimeInputs",
    "run_regimes",
]

MS_PER_MINUTE = 60_000
MS_PER_DAY = 86_400_000

UNCLASSIFIED = "unclassified"


@dataclass(frozen=True, slots=True)
class RegimeConfig:
    """Thresholds and windows, all set *before* the data is read (spec 9.3)."""

    vol_mode: str = "expanding_tertiles"
    """`expanding_tertiles` or `fixed` -- spec 9.3's two causal options."""
    vol_low: float | None = None
    """`fixed` mode: annualised vol at or below this is `low`."""
    vol_high: float | None = None
    """`fixed` mode: annualised vol at or above this is `high`."""
    vol_window_days: int = 30
    vol_min_history: int = 10
    """Expanding mode: observations before the tertiles are trusted at all. Boundaries
    estimated from three points would label everything with conviction and no basis."""
    adx_period: int = 14
    adx_threshold: float = 25.0
    funding_trailing_ms: int = 7 * MS_PER_DAY
    funding_threshold: float = 0.00005
    """Dead band around zero for the trailing mean, per funding interval. Half the
    0.0001 baseline rate: a mean inside the band is `neutral`, beyond it signed."""
    funding_min_settlements: int = 3
    cascade_cluster_ms: int = 5 * MS_PER_MINUTE
    cascade_notional: float = 1_000_000.0
    """Summed liquidation notional within the cluster window that declares a cascade.
    Configurable because it is symbol-scale-dependent, and because Binance's forceOrder
    stream is rate-capped (spec 15's data honesty applies: the stream understates)."""
    cascade_tag_ms: int = 30 * MS_PER_MINUTE
    min_periods: int = 30
    min_trades: int = 10

    def __post_init__(self) -> None:
        if self.vol_mode not in ("expanding_tertiles", "fixed"):
            raise ValueError(
                f"unknown vol_mode {self.vol_mode!r}: use 'expanding_tertiles' or 'fixed'"
            )
        if self.vol_mode == "fixed":
            if self.vol_low is None or self.vol_high is None:
                raise ValueError(
                    "fixed vol_mode needs vol_low and vol_high thresholds set in advance"
                )
            if not 0 < self.vol_low < self.vol_high:
                raise ValueError(
                    f"need 0 < vol_low < vol_high, got {self.vol_low} and {self.vol_high}"
                )
        for name in (
            "vol_window_days",
            "vol_min_history",
            "adx_period",
            "funding_trailing_ms",
            "funding_min_settlements",
            "cascade_cluster_ms",
            "cascade_tag_ms",
            "min_periods",
            "min_trades",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive, got {getattr(self, name)}")
        if self.funding_threshold < 0 or self.cascade_notional <= 0:
            raise ValueError("funding_threshold must be >= 0 and cascade_notional > 0")
        if self.adx_threshold <= 0 or self.adx_threshold >= 100:
            raise ValueError(f"adx_threshold must be in (0, 100), got {self.adx_threshold}")

    def to_json(self) -> dict[str, Any]:
        return {
            "vol_mode": self.vol_mode,
            "vol_low": self.vol_low,
            "vol_high": self.vol_high,
            "vol_window_days": self.vol_window_days,
            "vol_min_history": self.vol_min_history,
            "adx_period": self.adx_period,
            "adx_threshold": self.adx_threshold,
            "funding_trailing_ms": self.funding_trailing_ms,
            "funding_threshold": self.funding_threshold,
            "funding_min_settlements": self.funding_min_settlements,
            "cascade_cluster_ms": self.cascade_cluster_ms,
            "cascade_notional": self.cascade_notional,
            "cascade_tag_ms": self.cascade_tag_ms,
            "min_periods": self.min_periods,
            "min_trades": self.min_trades,
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> "RegimeConfig":
        defaults = cls()
        kwargs: dict[str, Any] = {}
        for name in (
            "vol_mode",
            "vol_low",
            "vol_high",
            "vol_window_days",
            "vol_min_history",
            "adx_period",
            "adx_threshold",
            "funding_trailing_ms",
            "funding_threshold",
            "funding_min_settlements",
            "cascade_cluster_ms",
            "cascade_notional",
            "cascade_tag_ms",
            "min_periods",
            "min_trades",
        ):
            if name in obj and obj[name] is not None:
                default = getattr(defaults, name)
                if isinstance(default, str):
                    kwargs[name] = str(obj[name])
                elif isinstance(default, float) or name in ("vol_low", "vol_high"):
                    kwargs[name] = float(obj[name])
                else:
                    kwargs[name] = int(obj[name])
        return cls(**kwargs)


@dataclass(frozen=True, slots=True)
class RegimeInputs:
    """Everything the labelling reads, assembled by the Lab worker.

    `daily_bars` must extend *before* the run's range by at least the vol window plus the
    ADX warm-up, or the early periods come back `unclassified` -- which is the honest
    result when the history genuinely is not there, and the wrong one when it is and was
    not fetched. The worker owns that responsibility; this module labels what it is given.
    """

    grid_times: tuple[int, ...]
    """Grid boundaries, `N + 1` of them for `N` returns (from `build_grid`)."""
    grid_returns: tuple[float, ...]
    periods_per_year: int
    grid_label: str
    trades: tuple[tuple[int, float], ...]
    """Closed round-trips as `(entry_ms, net_pnl)`. Open trades excluded upstream."""
    daily_bars: tuple[Bar, ...]
    """Completed daily bars, ascending, context window included."""
    funding: tuple[tuple[int, float], ...]
    """Realised settlements as `(settlement_ms, rate)`, ascending."""
    liquidations: tuple[tuple[int, float], ...] | None
    """`(ts_ms, notional)` ascending, or `None` when the dataset is unavailable --
    which makes the cascade dimension report itself unavailable rather than 'quiet'."""


# ------------------------------------------------------------------------ step series


class _StepSeries:
    """`(effective_ms, label)` change points, looked up by last-at-or-before (LOCF).

    `default` is what a query before the first change point gets. For the indicator
    dimensions that is `unclassified` -- before the window fills, the label genuinely
    could not be known. The cascade dimension sets `quiet`: with the dataset present,
    the absence of clusters *is* the observation, not a gap in one.
    """

    def __init__(self, *, default: str = UNCLASSIFIED) -> None:
        self._times: list[int] = []
        self._labels: list[str] = []
        self._default = default

    def push(self, effective_ms: int, label: str) -> None:
        if self._times and effective_ms < self._times[-1]:
            raise ValueError(
                f"step series must be pushed in time order: {effective_ms} after "
                f"{self._times[-1]}"
            )
        if self._labels and self._labels[-1] == label:
            return  # a step that does not change the label is not a change point
        # Same timestamp, new label: replace, last writer wins (one observation per
        # instant is the input contract; this guards a same-ms revision).
        if self._times and effective_ms == self._times[-1]:
            self._labels[-1] = label
            return
        self._times.append(effective_ms)
        self._labels.append(label)

    def at(self, ts_ms: int) -> str:
        index = bisect_right(self._times, ts_ms) - 1
        return self._labels[index] if index >= 0 else self._default

    def to_json(self) -> list[dict[str, Any]]:
        return [
            {"ms": ms, "label": label}
            for ms, label in zip(self._times, self._labels)
        ]


# ------------------------------------------------------------------- the dimensions


def _volatility_series(inputs: RegimeInputs, config: RegimeConfig) -> _StepSeries:
    """Trailing realised vol per completed daily bar, labelled causally.

    The indicator is the strategy library's `RealisedVolatility` over daily closes. Each
    value becomes *effective at the bar's close time* -- the first instant it is
    observable -- and labels every period opening from then until the next daily close.

    Expanding tertiles include the newest observation in their own boundary set. That is
    causal (nothing later than `t` is read at `t`) and it is the standard expanding-window
    construction; excluding the point would not make it more causal, only lag it.
    """
    series = _StepSeries()
    indicator = RealisedVolatility(config.vol_window_days, bar_ms=MS_PER_DAY)
    observed: list[float] = []
    for bar in inputs.daily_bars:
        indicator.update(bar)
        value = indicator.value
        if value is None:
            continue
        observed.append(value)
        if config.vol_mode == "fixed":
            if value <= config.vol_low:  # type: ignore[operator]
                label = "low"
            elif value >= config.vol_high:  # type: ignore[operator]
                label = "high"
            else:
                label = "normal"
        else:
            if len(observed) < config.vol_min_history:
                continue  # boundaries from a handful of points are conviction without basis
            ordered = sorted(observed)
            low_bound = _quantile(ordered, 1.0 / 3.0)
            high_bound = _quantile(ordered, 2.0 / 3.0)
            if value <= low_bound:
                label = "low"
            elif value >= high_bound:
                label = "high"
            else:
                label = "normal"
        series.push(bar.close_time, label)
    return series


def _quantile(ordered: Sequence[float], q: float) -> float:
    """Linear interpolation between order statistics, matching the Monte Carlo panel."""
    if not ordered:
        raise ValueError("no values")
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * q
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _trend_series(inputs: RegimeInputs, config: RegimeConfig) -> _StepSeries:
    """ADX(14) on the daily bar against a fixed threshold (spec 9.3's definition)."""
    series = _StepSeries()
    indicator = ADX(config.adx_period)
    for bar in inputs.daily_bars:
        indicator.update(bar)
        value = indicator.value
        if value is None:
            continue
        series.push(
            bar.close_time,
            "trend" if value >= config.adx_threshold else "range",
        )
    return series


def _funding_series(inputs: RegimeInputs, config: RegimeConfig) -> _StepSeries:
    """Trailing time-window mean of realised settlements against a fixed dead band.

    A *time* window rather than a count of settlements, because Binance varies the
    funding interval per symbol and has changed it historically (spec 3.5, R17) -- "the
    last 21 settlements" is a week on one symbol and three days on another.
    """
    series = _StepSeries()
    window: list[tuple[int, float]] = []
    for ts_ms, rate in inputs.funding:
        window.append((ts_ms, float(rate)))
        cutoff = ts_ms - config.funding_trailing_ms
        while window and window[0][0] <= cutoff:
            window.pop(0)
        if len(window) < config.funding_min_settlements:
            continue
        mean = sum(r for _, r in window) / len(window)
        if mean > config.funding_threshold:
            label = "positive"
        elif mean < -config.funding_threshold:
            label = "negative"
        else:
            label = "neutral"
        series.push(ts_ms, label)
    return series


def _cascade_series(inputs: RegimeInputs, config: RegimeConfig) -> _StepSeries | None:
    """Tag windows after liquidation clusters. `None` when the dataset is unavailable.

    A cluster is a rolling sum of liquidation notional within `cascade_cluster_ms`
    reaching `cascade_notional`; from that instant the market is `cascade` until
    `cascade_tag_ms` later (extended while the cluster keeps firing). Everything is
    derived from liquidations at or before the labelling instant -- the tag reaches
    forward from a cluster already seen, never backward from one still to come.
    """
    if inputs.liquidations is None:
        return None
    series = _StepSeries(default="quiet")
    window: list[tuple[int, float]] = []
    tagged_until: int | None = None
    for ts_ms, notional in inputs.liquidations:
        window.append((ts_ms, float(notional)))
        cutoff = ts_ms - config.cascade_cluster_ms
        while window and window[0][0] <= cutoff:
            window.pop(0)
        total = sum(n for _, n in window)
        if total >= config.cascade_notional:
            if tagged_until is not None and tagged_until < ts_ms:
                # The previous tag lapsed before this cluster: close it first, at its
                # own expiry, or the quiet stretch between the two would be tagged.
                series.push(tagged_until, "quiet")
            series.push(ts_ms, "cascade")
            tagged_until = ts_ms + config.cascade_tag_ms
    if tagged_until is not None:
        series.push(tagged_until, "quiet")
    return series


# ------------------------------------------------------------------------- bucketing


def _bucket_metrics(
    labels: Sequence[str],
    inputs: RegimeInputs,
    trade_labels: Sequence[str],
    config: RegimeConfig,
) -> dict[str, Any]:
    """The spec 8.2-style figures per label, plus counts and the `thin` flag."""
    by_label: dict[str, list[float]] = {}
    for label, value in zip(labels, inputs.grid_returns):
        by_label.setdefault(label, []).append(value)
    trades_by_label: dict[str, list[float]] = {}
    for label, (_, pnl) in zip(trade_labels, inputs.trades):
        trades_by_label.setdefault(label, []).append(pnl)

    buckets: dict[str, Any] = {}
    for label in sorted(set(by_label) | set(trades_by_label)):
        returns = by_label.get(label, [])
        pnls = trades_by_label.get(label, [])
        compounded = 1.0
        floored = False
        for r in returns:
            factor = 1.0 + r
            if factor <= 0.0:
                compounded = 0.0
                floored = True
                break
            compounded *= factor
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        scratches = len(pnls) - wins - losses
        mean = sum(returns) / len(returns) if returns else None
        buckets[label] = {
            "periods": len(returns),
            "period_share": len(returns) / len(inputs.grid_returns)
            if inputs.grid_returns
            else 0.0,
            "mean_return": mean,
            "total_return": (compounded - 1.0) if returns else None,
            "total_return_floored": floored,
            "sharpe_conditional": sharpe_ratio(returns, inputs.periods_per_year),
            "best_period": max(returns) if returns else None,
            "worst_period": min(returns) if returns else None,
            "trades": len(pnls),
            "trade_net_pnl": sum(pnls) if pnls else None,
            # **`wins / closed`, exactly as `analytics.metrics.trade_stats` computes it.**
            # This used to divide by `wins + losses`, dropping scratches, which made the
            # regime table and the run page report different win rates for the same
            # trades -- 0.50 against 0.33 on a set of ten wins, ten losses and ten
            # scratches. `metrics` argues the convention (a scratch is neither a win nor
            # a loss, and folding it either way distorts the rate); this follows it
            # rather than taking a third position in silence.
            "trade_win_rate": (wins / len(pnls)) if pnls else None,
            "trade_scratches": scratches,
            "trade_expectancy": (sum(pnls) / len(pnls)) if pnls else None,
            "thin": len(returns) < config.min_periods or len(pnls) < config.min_trades,
        }
    return buckets


def _dimension(
    name: str,
    series: _StepSeries,
    inputs: RegimeInputs,
    config: RegimeConfig,
    extra: Mapping[str, Any],
) -> dict[str, Any]:
    period_labels = [series.at(open_ms) for open_ms in inputs.grid_times[:-1]]
    trade_labels = [series.at(entry_ms) for entry_ms, _ in inputs.trades]
    return {
        "name": name,
        "available": True,
        "buckets": _bucket_metrics(period_labels, inputs, trade_labels, config),
        "changes": series.to_json(),
        **dict(extra),
    }


def run_regimes(inputs: RegimeInputs, config: RegimeConfig) -> dict[str, Any]:
    """Label every grid period on each dimension and bucket the run's figures by label.

    A period `(t_i, t_i+1]` is labelled by each dimension's state at `t_i` -- its open,
    the last instant at which the label was knowable without reading the period being
    labelled. Trades are labelled by their entry time on the same series, so "win rate in
    high vol" means volatility as it stood when the trade was entered.

    The conditional Sharpe per bucket is a statistic over *non-contiguous* periods,
    annualised with the grid's own factor. It answers "how did the strategy's periods in
    this regime perform", not "what would trading only this regime have returned" --
    entering and exiting a regime costs execution the concatenation cannot see. The panel
    label states this.
    """
    if len(inputs.grid_times) != len(inputs.grid_returns) + 1:
        raise ValueError(
            f"{len(inputs.grid_returns)} returns need {len(inputs.grid_returns) + 1} "
            f"boundaries, got {len(inputs.grid_times)}"
        )
    if not inputs.grid_returns:
        raise ValueError(
            "the run has no grid returns to bucket; regime analysis needs a range of at "
            "least one full grid period"
        )

    dimensions: dict[str, Any] = {
        "volatility": _dimension(
            "volatility",
            _volatility_series(inputs, config),
            inputs,
            config,
            {
                "mode": config.vol_mode,
                "window_days": config.vol_window_days,
                "thresholds": (
                    {"low": config.vol_low, "high": config.vol_high}
                    if config.vol_mode == "fixed"
                    else "expanding tertiles, boundaries from data up to each point only"
                ),
            },
        ),
        "trend": _dimension(
            "trend",
            _trend_series(inputs, config),
            inputs,
            config,
            {"adx_period": config.adx_period, "adx_threshold": config.adx_threshold},
        ),
        "funding": _dimension(
            "funding",
            _funding_series(inputs, config),
            inputs,
            config,
            {
                "trailing_ms": config.funding_trailing_ms,
                "threshold": config.funding_threshold,
            },
        ),
    }
    cascade = _cascade_series(inputs, config)
    if cascade is None:
        dimensions["cascade"] = {
            "name": "cascade",
            "available": False,
            "reason": (
                "no liquidation data is available for this range, so cascade periods "
                "cannot be identified -- 'quiet' would be a claim, not an observation"
            ),
            "buckets": {},
            "changes": [],
        }
    else:
        dimensions["cascade"] = _dimension(
            "cascade",
            cascade,
            inputs,
            config,
            {
                "cluster_ms": config.cascade_cluster_ms,
                "notional_threshold": config.cascade_notional,
                "tag_ms": config.cascade_tag_ms,
            },
        )

    return {
        "config": config.to_json(),
        "grid": inputs.grid_label,
        "periods": len(inputs.grid_returns),
        "periods_per_year": inputs.periods_per_year,
        "trades": len(inputs.trades),
        "dimensions": dimensions,
        "notes": [
            (
                "per-bucket Sharpe is conditional: a statistic over the run's periods "
                "in that regime, annualised on the grid factor. It is not the return of "
                "trading only that regime -- regime entries and exits cost execution "
                "this concatenation cannot see."
            ),
            (
                "buckets flagged thin have fewer than "
                f"{config.min_periods} periods or {config.min_trades} trades; their "
                "statistics are shown greyed because they are not evidence (spec 9.3)."
            ),
        ],
    }
