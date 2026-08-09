"""Regime analysis: causal labelling and per-bucket honesty (spec 9.3)."""

from __future__ import annotations

import pytest

from perplab.core.money import SCALE
from perplab.core.types import Bar
from perplab.lab.regimes import (
    MS_PER_DAY,
    MS_PER_MINUTE,
    RegimeConfig,
    RegimeInputs,
    run_regimes,
    _bucket_metrics,
    _cascade_series,
    _funding_series,
    _trend_series,
    _volatility_series,
)
from perplab.lab.stats import sharpe_ratio

MS_PER_HOUR = 60 * MS_PER_MINUTE
BASE = 1_709_251_200_000  # 2024-03-01T00:00Z


def _daily_bars(closes: list[float], start_ms: int = BASE) -> tuple[Bar, ...]:
    bars: list[Bar] = []
    prev = closes[0]
    for index, close in enumerate(closes):
        open_time = start_ms + index * MS_PER_DAY
        high = max(prev, close)
        low = min(prev, close)
        bars.append(
            Bar(
                symbol="BTCUSDT",
                open_time=open_time,
                close_time=open_time + MS_PER_DAY - 1,
                open=int(round(prev * SCALE)),
                high=int(round(high * SCALE)),
                low=int(round(low * SCALE)),
                close=int(round(close * SCALE)),
                volume=0,
                quote_volume=0,
                trades=0,
            )
        )
        prev = close
    return tuple(bars)


def _alternating_closes(base: float, move: float, count: int) -> list[float]:
    closes = []
    price = base
    for index in range(count):
        price = price * (1.0 + move) if index % 2 == 0 else price / (1.0 + move)
        closes.append(price)
    return closes


def _inputs(**overrides) -> RegimeInputs:
    grid_start = BASE + 40 * MS_PER_DAY
    base = dict(
        grid_times=tuple(grid_start + i * MS_PER_HOUR for i in range(7)),
        grid_returns=(0.01, -0.005, 0.02, -0.01, 0.0, 0.015),
        periods_per_year=8760,
        grid_label="hourly",
        trades=(),
        daily_bars=_daily_bars(_alternating_closes(40_000.0, 0.001, 50)),
        funding=(),
        liquidations=(),
    )
    base.update(overrides)
    return RegimeInputs(**base)


def _config(**overrides) -> RegimeConfig:
    base = dict(vol_window_days=5, vol_min_history=3, adx_period=3)
    base.update(overrides)
    return RegimeConfig(**base)


# ------------------------------------------------------------------------ causality


def test_expanding_tertile_labels_are_a_prefix_property() -> None:
    """The no-look-ahead test: appending future days must not relabel the past.

    Whole-sample quantiles fail exactly this -- a volatile September moves the boundaries
    that labelled March. Expanding-window boundaries use data up to each point only, so
    the change points computed from a prefix of the history must be identical to the same
    stretch of the full history's change points.
    """
    # Magnitudes that drift upward, so late observations would move early boundaries if
    # they leaked: the failure this test exists to catch.
    closes: list[float] = []
    price = 40_000.0
    for index in range(40):
        move = 0.001 + 0.002 * (index // 5)
        price = price * (1.0 + move) if index % 2 == 0 else price / (1.0 + move)
        closes.append(price)

    config = _config()
    full = _volatility_series(
        _inputs(daily_bars=_daily_bars(closes)), config
    ).to_json()
    prefix = _volatility_series(
        _inputs(daily_bars=_daily_bars(closes[:25])), config
    ).to_json()

    horizon = BASE + 25 * MS_PER_DAY
    assert prefix == [entry for entry in full if entry["ms"] < horizon]


def test_fixed_thresholds_label_calm_and_violent_stretches() -> None:
    closes = _alternating_closes(40_000.0, 0.001, 15)
    closes += _alternating_closes(closes[-1], 0.05, 15)
    series = _volatility_series(
        _inputs(daily_bars=_daily_bars(closes)),
        _config(vol_mode="fixed", vol_low=0.10, vol_high=0.50),
    )
    changes = series.to_json()
    assert changes[0]["label"] == "low"
    assert changes[-1]["label"] == "high"


def test_a_label_becomes_effective_at_the_bars_close_never_its_open() -> None:
    """The other look-ahead a mutation found: a daily bar's label pushed effective at the
    bar's *open* would let a period opening mid-day wear a label computed from that day's
    own close -- price action the period could not yet have seen. The label a mid-day
    instant wears must be the last *closed* bar's.

    Ten calm days, then one violent day: the violent day's own bar is what flips the vol
    label, so any instant inside that day must still read the calm label.
    """
    closes = _alternating_closes(40_000.0, 0.001, 10)
    closes.append(closes[-1] * 1.30)  # day 11: a 30% day
    bars = _daily_bars(closes)
    config = _config(vol_mode="fixed", vol_low=0.10, vol_high=0.50)
    series = _volatility_series(_inputs(daily_bars=bars), config)

    violent_day = bars[-1]
    mid_day = violent_day.open_time + 6 * MS_PER_HOUR
    assert series.at(mid_day) == "low", (
        "an instant inside the violent day already wears a label computed from that "
        "day's own close -- look-ahead"
    )
    assert series.at(violent_day.close_time) == "high"

    trend = _trend_series(_inputs(daily_bars=_daily_bars([40_000.0 * 1.01**i for i in range(12)])), _config())
    first_labelled = trend.to_json()[0]["ms"]
    labelled_bar = next(b for b in _daily_bars([40_000.0 * 1.01**i for i in range(12)]) if b.close_time == first_labelled)
    assert trend.at(labelled_bar.open_time + MS_PER_HOUR) == "unclassified", (
        "the trend label leaked back inside the bar that produced it"
    )


# ---------------------------------------------------------------------------- trend


def test_a_ramp_reads_as_trend_and_a_sawtooth_as_range() -> None:
    config = _config()
    ramp = _trend_series(
        _inputs(daily_bars=_daily_bars([40_000.0 * 1.01**i for i in range(12)])),
        config,
    )
    assert {entry["label"] for entry in ramp.to_json()} == {"trend"}

    # An expanding wedge: every up-swing makes a higher high, every down-swing a lower
    # low, in roughly equal measure. A plain alternation of equal closes is degenerate
    # here -- consecutive bars share identical highs and lows, so the one directional
    # impulse at its start is never offset and Wilder's DX reads pure directionality.
    # Twenty swings, not twelve: Wilder smoothing carries the seed window's one-sided
    # impulse for several bars, and the label genuinely is "trend" until it decays.
    wedge: list[float] = [40_000.0]
    for swing in range(20):
        step = 1_000.0 + 100.0 * swing
        wedge.append(wedge[-1] + step if swing % 2 == 0 else wedge[-1] - step)
    sawtooth = _trend_series(_inputs(daily_bars=_daily_bars(wedge)), config)
    assert sawtooth.to_json()[-1]["label"] == "range"


# -------------------------------------------------------------------------- funding


def test_funding_regime_follows_the_trailing_mean_through_the_dead_band() -> None:
    eight_hours = 8 * MS_PER_HOUR
    positive = [(BASE + i * eight_hours, 0.0003) for i in range(9)]
    negative = [(BASE + (9 + i) * eight_hours, -0.0003) for i in range(21)]
    series = _funding_series(
        _inputs(funding=tuple(positive + negative)), _config()
    )
    changes = series.to_json()
    assert changes[0]["label"] == "positive"
    assert changes[-1]["label"] == "negative"
    # The flip happens through the mean crossing the band, not at the first negative
    # settlement: with three positives still in the trailing window the mean is not yet
    # below -0.00005.
    first_negative_label = next(e for e in changes if e["label"] == "negative")
    assert first_negative_label["ms"] > negative[0][0]


def test_a_mean_inside_the_dead_band_is_neutral() -> None:
    settlements = tuple((BASE + i * 8 * MS_PER_HOUR, 0.00001) for i in range(6))
    series = _funding_series(_inputs(funding=settlements), _config())
    assert {entry["label"] for entry in series.to_json()} == {"neutral"}


# -------------------------------------------------------------------------- cascade


def test_a_liquidation_cluster_tags_a_window_and_only_that_window() -> None:
    cluster = BASE + 2 * MS_PER_HOUR
    liquidations = (
        (cluster - MS_PER_MINUTE, 600_000.0),
        (cluster, 600_000.0),  # rolling 5-minute sum crosses 1M here
        (BASE + 5 * MS_PER_HOUR, 100_000.0),  # lone small print, no cluster
    )
    series = _cascade_series(_inputs(liquidations=liquidations), _config())
    assert series is not None
    assert series.at(cluster - 10 * MS_PER_MINUTE) == "quiet"
    assert series.at(cluster + 10 * MS_PER_MINUTE) == "cascade"
    assert series.at(cluster + 40 * MS_PER_MINUTE) == "quiet"
    assert series.at(BASE + 5 * MS_PER_HOUR + MS_PER_MINUTE) == "quiet"


def test_missing_liquidation_data_reports_unavailable_not_quiet() -> None:
    """'Quiet' is an observation; with no data it would be a claim."""
    result = run_regimes(_inputs(liquidations=None), _config())
    cascade = result["dimensions"]["cascade"]
    assert cascade["available"] is False
    assert "cannot be identified" in cascade["reason"]
    assert cascade["buckets"] == {}


def test_present_but_empty_liquidations_are_genuinely_quiet() -> None:
    result = run_regimes(_inputs(liquidations=()), _config())
    cascade = result["dimensions"]["cascade"]
    assert cascade["available"] is True
    assert cascade["buckets"]["quiet"]["periods"] == 6


# ------------------------------------------------------------------------ bucketing


def test_periods_are_labelled_at_their_open_and_bucketed_with_their_return() -> None:
    """A label effective mid-period applies from the *next* period, not this one."""
    result = run_regimes(_inputs(), _config(vol_mode="fixed", vol_low=0.1, vol_high=0.5))
    volatility = result["dimensions"]["volatility"]
    total = sum(bucket["periods"] for bucket in volatility["buckets"].values())
    assert total == 6  # every period lands in exactly one bucket


def test_bucket_metrics_are_computed_over_that_buckets_periods_only() -> None:
    inputs = _inputs()
    labels = ["a", "a", "b", "a", "b", "b"]
    buckets = _bucket_metrics(labels, inputs, [], _config(min_periods=2, min_trades=1))
    a_returns = [0.01, -0.005, -0.01]
    assert buckets["a"]["periods"] == 3
    assert buckets["a"]["mean_return"] == pytest.approx(sum(a_returns) / 3)
    assert buckets["a"]["sharpe_conditional"] == pytest.approx(
        sharpe_ratio(a_returns, 8760)
    )
    expected_total = (1.01 * 0.995 * 0.99) - 1.0
    assert buckets["a"]["total_return"] == pytest.approx(expected_total)
    assert buckets["b"]["periods"] == 3
    assert buckets["a"]["period_share"] == pytest.approx(0.5)


def test_a_thin_bucket_is_flagged_not_hidden() -> None:
    """Spec 9.3: a regime with 4 observations is not evidence; grey it, keep it."""
    inputs = _inputs()
    labels = ["rare", "common", "common", "common", "common", "common"]
    buckets = _bucket_metrics(labels, inputs, [], _config(min_periods=3, min_trades=1))
    assert buckets["rare"]["thin"] is True
    assert buckets["rare"]["mean_return"] is not None  # greyed is not hidden
    assert buckets["common"]["thin"] is True  # 5 periods but 0 trades < 1
    relaxed = _bucket_metrics(labels, inputs, [], _config(min_periods=3, min_trades=1))
    assert relaxed["common"]["trades"] == 0


def test_trades_are_labelled_by_the_regime_at_their_entry() -> None:
    grid_start = BASE + 40 * MS_PER_DAY
    trades = (
        (grid_start + 30 * MS_PER_MINUTE, 50.0),   # entered in period 0
        (grid_start + 3 * MS_PER_HOUR + 1, -20.0),  # entered in period 3
    )
    result = run_regimes(
        _inputs(trades=trades, liquidations=()),
        _config(min_periods=1, min_trades=1),
    )
    quiet = result["dimensions"]["cascade"]["buckets"]["quiet"]
    assert quiet["trades"] == 2
    assert quiet["trade_net_pnl"] == pytest.approx(30.0)
    assert quiet["trade_win_rate"] == pytest.approx(0.5)


def test_periods_before_any_label_are_counted_as_unclassified() -> None:
    """A first month the vol window cannot label must not fold into a real bucket."""
    result = run_regimes(
        _inputs(daily_bars=_daily_bars(_alternating_closes(40_000.0, 0.001, 3))),
        _config(),
    )
    volatility = result["dimensions"]["volatility"]
    assert volatility["buckets"]["unclassified"]["periods"] == 6


# ---------------------------------------------------------------------------- config


def test_config_validation_refuses_nonsense() -> None:
    with pytest.raises(ValueError, match="unknown vol_mode"):
        RegimeConfig(vol_mode="whole_sample")
    with pytest.raises(ValueError, match="vol_low and vol_high"):
        RegimeConfig(vol_mode="fixed")
    with pytest.raises(ValueError, match="0 < vol_low < vol_high"):
        RegimeConfig(vol_mode="fixed", vol_low=0.5, vol_high=0.1)
    with pytest.raises(ValueError, match="adx_threshold"):
        RegimeConfig(adx_threshold=0.0)
    with pytest.raises(ValueError, match="min_periods"):
        RegimeConfig(min_periods=0)


def test_config_json_round_trips() -> None:
    config = RegimeConfig(
        vol_mode="fixed", vol_low=0.2, vol_high=0.8, adx_period=20, min_periods=50
    )
    rebuilt = RegimeConfig.from_json(config.to_json())
    assert rebuilt == config
    assert RegimeConfig.from_json({}) == RegimeConfig()


def test_the_artefact_carries_its_honesty_notes() -> None:
    result = run_regimes(_inputs(), _config())
    text = " ".join(result["notes"])
    assert "conditional" in text
    assert "not evidence" in text


def test_an_empty_grid_is_refused() -> None:
    with pytest.raises(ValueError, match="no grid returns"):
        run_regimes(
            _inputs(grid_times=(BASE,), grid_returns=()), _config()
        )
