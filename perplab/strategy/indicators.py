"""Causal indicator library (spec 5.4).

Every indicator here obeys one rule, and the library is designed so that breaking it takes
deliberate effort rather than a moment's inattention:

> The value at bar `i` uses data through bar `i` only, and updates only on a closed bar.

Three structural consequences, all from spec 5.4:

**No centred windows.** There is no `center=True` anywhere and there never will be. A
centred moving average is the most elegant look-ahead bug in existence -- it looks like
smoothing, it backtests beautifully, and it is reading the future. Not offering it is the
only reliable defence.

**Cross detection is an edge, not a level.** `crossed_above` compares the `(prev, current)`
pair against the other series' `(prev, current)` pair. Deriving it from a level comparison
(`a > b`) latches: the signal fires on every bar the condition holds rather than on the bar
it became true, and a strategy that meant to enter once enters two hundred times.

**Warm-up is derived, not declared.** Each indicator knows how many updates it needs before
its value means anything, and `IndicatorSet.warmup` is the maximum over the set. That number
is cross-checked against `requires["history"]` at validation time, so a 200-period EMA
cannot quietly produce signals from twelve bars.

**Arithmetic is float64, deliberately.** Spec 3.1 puts indicators on the float side of the
seam: they feed comparisons and thresholds, never a balance. Market data arrives as scaled
int64 and is converted once, here, at the boundary.
"""

from __future__ import annotations

import math
from collections import deque
from itertools import islice
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Sequence

from perplab.core.money import SCALE

if TYPE_CHECKING:  # pragma: no cover
    from perplab.core.types import Bar, DepthSnapshot
    from perplab.engine.ticks import TradePrint

__all__ = [
    "DEFAULT_HISTORY",
    "SOURCES",
    "Indicator",
    "DerivedSeries",
    "SMA",
    "EMA",
    "WMA",
    "RSI",
    "MACD",
    "ATR",
    "ADX",
    "Bollinger",
    "Donchian",
    "VWAP",
    "RealisedVolatility",
    "OBV",
    "CVD",
    "BookImbalance",
    "FundingMean",
    "OIDelta",
    "IndicatorSet",
    "SealedIndicators",
]

DEFAULT_HISTORY = 4096
"""How many past values each indicator retains for `.series(n)`.

Bounded rather than unbounded: a 1-minute backtest over six years is three million bars,
and an unbounded history per indicator turns a handful of indicators into gigabytes for
values nobody reads. 4096 is far past any plausible `.series(n)` call and costs 32 KB per
indicator.

**This bound is also a hard cap on `.series(n)`.** `series(1_000_000)` returns at most
this many values -- the older ones are gone, not lazily loaded -- and it does so without
raising, because "everything retained" is a legitimate request. A consumer that genuinely
needs a longer tail must construct its indicator with a larger `history=`.
"""

MS_PER_YEAR = 365 * 24 * 60 * 60 * 1000
"""Annualisation base for realised volatility.

365 rather than 252 trading days: perpetual futures trade continuously, with no weekends
and no holidays. Using an equities calendar here would understate annualised vol by ~20%
for no reason other than habit.
"""

SOURCES = ("open", "high", "low", "close", "hl2", "hlc3", "ohlc4", "volume")


def _scaled_to_float(value: int) -> float:
    """Cross the storage seam into indicator arithmetic.

    The one conversion point. `SCALE` is a power of ten and both operands are exact
    integers, so this is a single correctly-rounded division rather than a chain of them.
    """
    return value / SCALE


def _source_value(bar: Bar, source: str) -> float:
    if source == "close":
        return _scaled_to_float(bar.close)
    if source == "open":
        return _scaled_to_float(bar.open)
    if source == "high":
        return _scaled_to_float(bar.high)
    if source == "low":
        return _scaled_to_float(bar.low)
    if source == "volume":
        return _scaled_to_float(bar.volume)
    if source == "hl2":
        return (_scaled_to_float(bar.high) + _scaled_to_float(bar.low)) / 2.0
    if source == "hlc3":
        return (
            _scaled_to_float(bar.high)
            + _scaled_to_float(bar.low)
            + _scaled_to_float(bar.close)
        ) / 3.0
    if source == "ohlc4":
        return (
            _scaled_to_float(bar.open)
            + _scaled_to_float(bar.high)
            + _scaled_to_float(bar.low)
            + _scaled_to_float(bar.close)
        ) / 4.0
    raise ValueError(f"unknown price source {source!r}; expected one of {list(SOURCES)}")


def _check_period(period: int, *, name: str, minimum: int = 1) -> int:
    if isinstance(period, bool) or not isinstance(period, int):
        raise TypeError(f"{name} must be an int, got {type(period).__name__}")
    if period < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {period}")
    return period


# ------------------------------------------------------------------------- base


class Indicator:
    """Base class: a causal time series with edge-detecting comparisons.

    Subclasses push a value with `_push` exactly once per update of their own feed, and
    only once the value is defined. Pushing `None` placeholders was rejected deliberately:
    `prev` would then sometimes mean "the previous bar" and sometimes "the previous bar
    that happened to have a value", and every `crossed_*` result would silently depend on
    which.
    """

    feed: ClassVar[str] = "bar"
    """Which stream drives this indicator: bar, trade, depth, funding or oi.

    Only `bar` indicators contribute to the engine's warm-up gate, because warm-up is
    counted in bars and there is no honest conversion from "20 funding settlements" to a
    number of 15-minute bars. Non-bar indicators expose `.ready` for the strategy to check.
    """

    def __init__(self, *, history: int = DEFAULT_HISTORY) -> None:
        self._values: deque[float] = deque(maxlen=history)
        self._updates = 0

    # -------------------------------------------------------------- introspection

    @property
    def warmup(self) -> int:
        """Updates of this indicator's own feed before it and its derived series are ready.

        Deliberately covers derived series too: `MACD.warmup` accounts for the signal line,
        not just the MACD line, so gating on it cannot leave a strategy comparing against
        an undefined signal.

        **What "ready" promises -- and what it does not.** Warm-up guarantees `.value` (on
        the indicator and everything it derives), not `.prev`: at the first warm bar most
        series have exactly one reading, so `.prev` is `None` and `crossed_*` answers a
        defined `False` rather than raising or guessing (see `crossed_above`). The edge a
        crossing strategy waits for can therefore fire no earlier than one bar after
        warm-up, which is the honest reading -- an edge needs two points. The exception is
        a class whose *documented idiom is* `.prev` (`Donchian`, whose breakout comparison
        reads `.upper.prev`): there the extra bar is folded into that class's own `warmup`,
        so the documented comparison is defined at the first warm bar rather than a
        `TypeError` on `None`.
        """
        raise NotImplementedError

    @property
    def ready(self) -> bool:
        return bool(self._values)

    @property
    def value(self) -> float | None:
        return self._values[-1] if self._values else None

    @property
    def prev(self) -> float | None:
        return self._values[-2] if len(self._values) >= 2 else None

    @property
    def updates(self) -> int:
        """How many times this indicator's feed has driven it. Diagnostics only."""
        return self._updates

    def series(self, n: int) -> tuple[float, ...]:
        """The last `n` values, oldest first.

        Returns fewer than `n` if fewer exist, rather than padding or raising. A strategy
        asking for 50 values during warm-up wants "everything so far"; padding with zeros
        would feed a fabricated flat series into whatever consumes it.

        **Retention is capped at the `history` this indicator was constructed with**
        (`DEFAULT_HISTORY` = 4096 unless overridden), and the cap is silent by design: once
        a run is longer than the retention window, "everything so far" and "everything
        retained" are different numbers and this returns the latter. A caller that needs a
        longer tail must ask for it up front via `history=`; asking `series()` for it after
        the fact cannot resurrect values that were never kept.

        Cost is `O(n)`, not `O(history)`: only the requested tail is copied. The previous
        implementation materialised all 4096 retained values to hand back three, on a call
        sitting inside per-bar strategy code.
        """
        _check_period(n, name="n")
        if n >= len(self._values):
            return tuple(self._values)
        # `reversed` walks a deque from its right end at O(1) a step, so taking `n` values
        # from the reversed view touches exactly the tail being returned. The final
        # `[::-1]` restores oldest-first order.
        return tuple(islice(reversed(self._values), n))[::-1]

    def _push(self, value: float) -> None:
        self._values.append(value)

    # ------------------------------------------------------------------ crossings

    def _other_pair(self, other: Any) -> tuple[float, float] | None:
        """The `(prev, value)` pair to cross against, or `None` while it is undefined.

        **Two indicators may only be crossed when the same feed drives both.** A cross is
        a statement about one instant: "at this update, A moved past B". CVD advances on
        every trade print and an SMA once per closed bar, so their `(prev, value)` pairs
        describe different moments -- the CVD pair spans microseconds while the SMA pair
        spans fifteen minutes -- and an edge computed across them is a comparison between
        two instants the market never shared. It fires, it looks like a signal, and it is
        fiction. The `feed` attribute stating which stream drives each series sits right
        on the class, so the mismatch is refused by name rather than silently compared.
        A plain number has no cadence -- a level is the same level at every instant -- so
        numeric thresholds are unaffected. Derived series carry their parent's feed
        (see `DerivedSeries`), which is what keeps `macd.crossed_above(macd.signal)`,
        the documented idiom, working.
        """
        if isinstance(other, Indicator):
            if other.feed != self.feed:
                raise TypeError(
                    f"crossed_above/crossed_below cannot compare {type(self).__name__} "
                    f"(driven by the {self.feed!r} feed) with {type(other).__name__} "
                    f"(driven by the {other.feed!r} feed): the two series advance at "
                    "different cadences, so their (prev, value) pairs describe "
                    "different instants and the crossing is undefined. Compare each "
                    "against a numeric level instead, or derive both signals from the "
                    "same feed."
                )
            prev, now = other.prev, other.value
            return None if prev is None or now is None else (prev, now)
        if isinstance(other, bool) or not isinstance(other, (int, float)):
            raise TypeError(
                "crossed_above/crossed_below take another indicator or a number, got "
                f"{type(other).__name__}"
            )
        level = float(other)
        return (level, level)

    def crossed_above(self, other: Indicator | float | int) -> bool:
        """True on the single update where this series crossed from at-or-below to above.

        An edge, never a level (spec 5.4 rule 3). `False` whenever either side lacks a
        previous value, because "was it below before?" has no answer yet and guessing
        would fire a spurious signal on the first ready bar of every run.
        """
        pair = self._other_pair(other)
        if pair is None or self.prev is None or self.value is None:
            return False
        other_prev, other_now = pair
        return self.prev <= other_prev and self.value > other_now

    def crossed_below(self, other: Indicator | float | int) -> bool:
        """True on the single update where this series crossed from at-or-above to below."""
        pair = self._other_pair(other)
        if pair is None or self.prev is None or self.value is None:
            return False
        other_prev, other_now = pair
        return self.prev >= other_prev and self.value < other_now

    def __repr__(self) -> str:
        value = "unset" if self.value is None else f"{self.value:.8g}"
        return f"{type(self).__name__}(value={value}, updates={self._updates})"


class DerivedSeries(Indicator):
    """A series produced by another indicator, never driven directly.

    MACD's signal line, ADX's directional indicators, Bollinger's bands. They are full
    `Indicator`s so `macd.crossed_above(macd.signal)` works, but they are *not* registered
    with the `IndicatorSet` -- registering them would update them twice per bar, once by
    the set and once by their parent, which shifts every value by a bar.

    **`feed` is the parent's feed, stated explicitly.** A derived series advances exactly
    when its parent does, so its cadence *is* the parent's -- and `crossed_*` now refuses
    to compare series on different feeds (see `_other_pair`), so a derived series whose
    `feed` merely fell back to the class default would refuse to cross its own non-bar
    parent while happily crossing unrelated bar series: the documented idiom broken and
    the fictional comparison allowed, both at once. Every parent in this module is
    bar-driven today, which is why the inherited `"bar"` happened to be right; passing it
    explicitly is what keeps that a fact rather than a coincidence.
    """

    def __init__(
        self,
        warmup: int,
        *,
        history: int = DEFAULT_HISTORY,
        feed: str | None = None,
    ) -> None:
        super().__init__(history=history)
        self._warmup = warmup
        if feed is not None:
            # An instance attribute deliberately shadowing the ClassVar: the feed is a
            # property of the parent, known only at construction time.
            self.feed = feed  # type: ignore[misc]

    @property
    def warmup(self) -> int:
        return self._warmup

    def update(self, *_args: Any, **_kwargs: Any) -> None:
        raise TypeError(
            f"{type(self).__name__} is driven by its parent indicator and must not be "
            "updated directly; updating it here would advance it twice per bar"
        )


class _BarIndicator(Indicator):
    """A bar-driven indicator reading one price source."""

    feed = "bar"

    def __init__(self, *, source: str = "close", history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        if source not in SOURCES:
            raise ValueError(f"unknown price source {source!r}; expected one of {list(SOURCES)}")
        self.source = source

    def update(self, bar: Bar) -> None:
        self._updates += 1
        self._on_bar(bar, _source_value(bar, self.source))

    def _on_bar(self, bar: Bar, x: float) -> None:
        raise NotImplementedError


# ------------------------------------------------------------------- moving averages


class SMA(_BarIndicator):
    """Simple moving average over `period` bars."""

    def __init__(self, period: int, *, source: str = "close", history: int = DEFAULT_HISTORY) -> None:
        super().__init__(source=source, history=history)
        self.period = _check_period(period, name="period")
        self._window: deque[float] = deque(maxlen=self.period)
        self._sum = 0.0
        self._since_resync = 0

    @property
    def warmup(self) -> int:
        return self.period

    def _on_bar(self, bar: Bar, x: float) -> None:
        # Rolling sum rather than re-summing the window each bar. The window can be 400
        # deep over three million bars, and re-summing is O(n) per bar; the incremental
        # form is O(1) and performs an identical operation sequence on any prefix of the
        # data, which is what the truncation test in spec 12.3 actually checks.
        outgoing = self._window[0] if len(self._window) == self.period else 0.0
        self._window.append(x)
        self._sum += x - outgoing
        self._since_resync += 1
        # ...and re-summed exactly once every `period` bars, which costs O(n)/n = O(1)
        # amortised and caps the accumulated rounding error at `period` steps instead of
        # letting it grow with the length of the run. Three million incremental steps
        # against a 60000-magnitude price would otherwise drift by ~1e-5 in absolute terms
        # -- harmless for a comparison, but it makes the SMA of a constant series stop
        # being that constant, which is the kind of thing that fails a test for reasons
        # nobody can find. The counter advances from the first bar in every run, so a
        # truncated run resyncs on exactly the same bars.
        if self._since_resync >= self.period:
            self._sum = math.fsum(self._window)
            self._since_resync = 0
        if len(self._window) == self.period:
            self._push(self._sum / self.period)


class EMA(_BarIndicator):
    """Exponential moving average, `alpha = 2 / (period + 1)`.

    Seeded with the simple average of the first `period` values rather than with the first
    value alone. Both are causal; the SMA seed converges to the steady state far faster,
    so the first hundred bars of a 200-period EMA are not dominated by an arbitrary
    starting point. It is also the convention TA-Lib and TradingView use, which matters
    when a result is compared against a chart.
    """

    def __init__(self, period: int, *, source: str = "close", history: int = DEFAULT_HISTORY) -> None:
        super().__init__(source=source, history=history)
        self.period = _check_period(period, name="period")
        self.alpha = 2.0 / (self.period + 1.0)
        self._seed: list[float] = []
        self._current: float | None = None

    @property
    def warmup(self) -> int:
        return self.period

    def _on_bar(self, bar: Bar, x: float) -> None:
        if self._current is None:
            self._seed.append(x)
            if len(self._seed) < self.period:
                return
            self._current = math.fsum(self._seed) / self.period
            self._seed = []
        else:
            self._current += self.alpha * (x - self._current)
        self._push(self._current)


class WMA(_BarIndicator):
    """Linearly weighted moving average: weight `i` on the `i`-th most recent bar."""

    def __init__(self, period: int, *, source: str = "close", history: int = DEFAULT_HISTORY) -> None:
        super().__init__(source=source, history=history)
        self.period = _check_period(period, name="period")
        self._window: deque[float] = deque(maxlen=self.period)
        self._weight_sum = self.period * (self.period + 1) / 2.0

    @property
    def warmup(self) -> int:
        return self.period

    def _on_bar(self, bar: Bar, x: float) -> None:
        self._window.append(x)
        if len(self._window) < self.period:
            return
        total = math.fsum(
            value * (index + 1) for index, value in enumerate(self._window)
        )
        self._push(total / self._weight_sum)


# ------------------------------------------------------------------------ oscillators


class RSI(_BarIndicator):
    """Wilder's relative strength index over `period` bars.

    Wilder smoothing (`alpha = 1/period`), seeded with the simple average of the first
    `period` gains and losses -- the original 1978 definition, and what every charting
    package reproduces.

    **One stated departure: a window that has not moved reads 50, not an extreme.** That
    covers two cases, and the second is the one that matters. When both smoothed averages
    are zero (a flat tape from the seed onward) the usual `100` reads as maximum overbought
    on a market that has done nothing. And when the tape goes flat *after* a fall, Wilder's
    recursion never forgets: `avg_gain` is exactly zero and stays exactly zero through any
    number of flat bars, while `avg_loss` only decays geometrically -- so the textbook
    formula prints exactly `0.0` for thousands of dead bars and `if rsi.value < 30: buy`
    fires on every one of them. The neutral rule therefore keys on the *window*, not only
    on the averages: once the last `period` price changes are all zero, the reading is 50.
    The smoothed averages keep updating underneath, so the first real move resumes the
    ordinary Wilder computation from the memory it always had. (TA-Lib prints 0 through a
    dead stretch like this; matching it there would mean endorsing "maximally oversold" as
    the description of a market that is not moving at all.)
    """

    def __init__(self, period: int = 14, *, source: str = "close", history: int = DEFAULT_HISTORY) -> None:
        super().__init__(source=source, history=history)
        self.period = _check_period(period, name="period", minimum=2)
        self._prev_price: float | None = None
        self._gains: list[float] = []
        self._losses: list[float] = []
        self._avg_gain: float | None = None
        self._avg_loss: float | None = None
        self._flat_streak = 0
        """Consecutive zero changes, so a whole-window-flat tape can be recognised."""

    @property
    def warmup(self) -> int:
        # `period` price *changes* need `period + 1` prices.
        return self.period + 1

    def _on_bar(self, bar: Bar, x: float) -> None:
        if self._prev_price is None:
            self._prev_price = x
            return

        change = x - self._prev_price
        self._prev_price = x
        gain = change if change > 0.0 else 0.0
        loss = -change if change < 0.0 else 0.0
        self._flat_streak = self._flat_streak + 1 if change == 0.0 else 0

        if self._avg_gain is None:
            self._gains.append(gain)
            self._losses.append(loss)
            if len(self._gains) < self.period:
                return
            self._avg_gain = math.fsum(self._gains) / self.period
            self._avg_loss = math.fsum(self._losses) / self.period
            self._gains = []
            self._losses = []
        else:
            n = float(self.period)
            self._avg_gain = (self._avg_gain * (n - 1.0) + gain) / n
            self._avg_loss = (self._avg_loss * (n - 1.0) + loss) / n  # type: ignore[operator]

        if self._flat_streak >= self.period:
            # Every change in the lookback window is zero: the market has conveyed no
            # directional information for a full period, whatever the decaying smoothed
            # averages still remember from before it went quiet. See the class docstring --
            # without this, a fall followed by a long flat stretch reads exactly 0.0
            # (avg_gain is pinned at zero while avg_loss merely decays) and an oversold
            # threshold fires on every dead bar.
            self._push(50.0)
        else:
            self._push(self._rsi(self._avg_gain, self._avg_loss))  # type: ignore[arg-type]

    @staticmethod
    def _rsi(avg_gain: float, avg_loss: float) -> float:
        """RSI from the two smoothed averages, including both degenerate cases.

        `avg_loss == 0` with any gain is unambiguously 100. Both averages zero means no
        remembered movement in either direction, and the usual `100` is then actively
        misleading: it reads as maximum overbought on a series that has not moved at all.
        50 -- no directional information -- is what the number is supposed to convey. (The
        window-flat case is handled a level up, in `_on_bar`, because it must fire even
        while `avg_loss` is still decaying toward zero rather than exactly there.)
        """
        if avg_loss == 0.0:
            return 100.0 if avg_gain > 0.0 else 50.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))


class MACD(_BarIndicator):
    """Moving average convergence/divergence.

    `.value` is the MACD line, so `macd.crossed_above(macd.signal)` reads the way it does
    on a chart. `.signal` and `.histogram` are derived series driven from here.
    """

    def __init__(
        self,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        *,
        source: str = "close",
        history: int = DEFAULT_HISTORY,
    ) -> None:
        super().__init__(source=source, history=history)
        self.fast_period = _check_period(fast, name="fast")
        self.slow_period = _check_period(slow, name="slow")
        self.signal_period = _check_period(signal, name="signal")
        if self.fast_period >= self.slow_period:
            raise ValueError(
                f"fast period ({self.fast_period}) must be shorter than slow "
                f"({self.slow_period}); reversed, the histogram's sign is inverted and "
                "every signal reads backwards"
            )
        self._fast = EMA(self.fast_period, source=source, history=history)
        self._slow = EMA(self.slow_period, source=source, history=history)
        self.signal = DerivedSeries(self.warmup, history=history, feed=self.feed)
        self.histogram = DerivedSeries(self.warmup, history=history, feed=self.feed)
        self._signal_alpha = 2.0 / (self.signal_period + 1.0)
        self._signal_seed: list[float] = []
        self._signal_value: float | None = None

    @property
    def warmup(self) -> int:
        # The MACD line is defined once the slow EMA is (`slow` bars); the signal line
        # then needs `signal` MACD values, the first of which is that same bar.
        return self.slow_period + self.signal_period - 1

    @property
    def ready(self) -> bool:
        """True only once the **signal line** has a value, not merely the MACD line.

        The line leads the signal by `signal - 1` bars, and `macd.value - macd.signal.value`
        is the first expression every MACD strategy writes. The base-class answer -- "I have
        pushed a value, so I am ready" -- was true of the line and false of the indicator:
        from bar `slow` to bar `slow + signal - 1` it handed that expression a `None` (a
        `TypeError` at the first bar the strategy believed it could trade), and because
        `IndicatorSet.all_ready` aggregates this property, a whole set reported ready while
        its slowest member could not be read. `warmup` already covers the signal line;
        `ready` now keeps the same promise. The signal implies the line: the signal's first
        value is pushed on a bar the line also pushed.
        """
        return self.signal.ready

    def _on_bar(self, bar: Bar, x: float) -> None:
        self._fast.update(bar)
        self._slow.update(bar)
        if self._fast.value is None or self._slow.value is None:
            return

        line = self._fast.value - self._slow.value
        self._push(line)

        if self._signal_value is None:
            self._signal_seed.append(line)
            if len(self._signal_seed) < self.signal_period:
                return
            self._signal_value = math.fsum(self._signal_seed) / self.signal_period
            self._signal_seed = []
        else:
            self._signal_value += self._signal_alpha * (line - self._signal_value)

        self.signal._push(self._signal_value)
        self.histogram._push(line - self._signal_value)


# ------------------------------------------------------------------------ volatility


class _TrueRangeMixin:
    """Shared true-range state for ATR and ADX.

    The first bar has no previous close, so it has **no true range**: `_true_range`
    returns `None` there and both consumers drop the bar, which is what TA-Lib does. This
    mixin used to fabricate `high - low` for bar 0 instead -- defensible on its own
    (TradingView's `ta.atr` does exactly that), but it was only ever consumed by ATR while
    ADX independently discarded bar 0, so the two consumers of one shared mixin smoothed
    *different* series. Worse, the fabricated term made ATR emit a bar early and carried a
    gap-less range into the seed at precisely the bars where strategies start trading; see
    `ATR` for the numbers. One convention, stated once, consumed identically by both.
    """

    _prev_close: float | None

    def _true_range(self, high: float, low: float) -> float | None:
        if self._prev_close is None:
            return None
        return max(
            high - low,
            abs(high - self._prev_close),
            abs(low - self._prev_close),
        )


class ATR(Indicator, _TrueRangeMixin):
    """Average true range, Wilder-smoothed over `period` bars.

    **TA-Lib's convention: bar 0 contributes no true range.** The seed is the simple mean
    of `TR[1..period]` and the first value lands at bar index `period` -- which is why
    `warmup` is `period + 1` updates, exactly as RSI needs `period + 1` prices for
    `period` changes. Until the Phase 6 audit this class followed TradingView instead,
    seeding a bar early on a fabricated `TR[0] = high - low`: on a series opening with a
    wide bar, ATR(14)'s first reading overstated TA-Lib's by 2.26x at the first bar a
    strategy could trade, decaying with a half-life of ~9.4 bars -- a stop sized off it sat
    more than twice as far away as the author's chart said, at exactly the bars where
    trading starts. It also made ATR and ADX smooth different series (ADX always dropped
    bar 0), and the platform's stated references are the TA-Lib-verified batch
    implementations, so the TradingView behaviour was retired along with the docstring
    that misattributed it.
    """

    feed = "bar"

    def __init__(self, period: int = 14, *, history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        self.period = _check_period(period, name="period", minimum=2)
        self._prev_close = None
        self._seed: list[float] = []
        self._current: float | None = None

    @property
    def warmup(self) -> int:
        # `period` true ranges need `period + 1` bars: bar 0 only donates its close.
        return self.period + 1

    def update(self, bar: Bar) -> None:
        self._updates += 1
        high = _scaled_to_float(bar.high)
        low = _scaled_to_float(bar.low)
        close = _scaled_to_float(bar.close)
        tr = self._true_range(high, low)
        self._prev_close = close
        if tr is None:
            # Bar 0: no previous close, no true range (TA-Lib drops it; so does ADX).
            return

        if self._current is None:
            self._seed.append(tr)
            if len(self._seed) < self.period:
                return
            self._current = math.fsum(self._seed) / self.period
            self._seed = []
        else:
            n = float(self.period)
            self._current = (self._current * (n - 1.0) + tr) / n
        self._push(self._current)


class ADX(Indicator, _TrueRangeMixin):
    """Average directional index, with `.plus_di` and `.minus_di` as derived series.

    Wilder's running-sum smoothing (`S -= S/n; S += x`) on +DM, -DM and TR, then a Wilder
    average of DX. The directional movements are *exclusive*: a bar whose up-move and
    down-move are equal contributes neither, which is what stops an inside bar from
    registering as directional.
    """

    feed = "bar"

    def __init__(self, period: int = 14, *, history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        self.period = _check_period(period, name="period", minimum=2)
        self._prev_close = None
        self._prev_high: float | None = None
        self._prev_low: float | None = None
        self.plus_di = DerivedSeries(self.period + 1, history=history, feed=self.feed)
        self.minus_di = DerivedSeries(self.period + 1, history=history, feed=self.feed)
        self._seed_plus: list[float] = []
        self._seed_minus: list[float] = []
        self._seed_tr: list[float] = []
        self._sum_plus: float | None = None
        self._sum_minus: float | None = None
        self._sum_tr: float | None = None
        self._dx_seed: list[float] = []
        self._adx: float | None = None

    @property
    def warmup(self) -> int:
        # `period` directional-movement readings (first available on bar 2) give the first
        # DX; `period` DX readings give the first ADX.
        return 2 * self.period

    def update(self, bar: Bar) -> None:
        self._updates += 1
        high = _scaled_to_float(bar.high)
        low = _scaled_to_float(bar.low)
        close = _scaled_to_float(bar.close)

        if self._prev_high is None or self._prev_low is None:
            # Bar 0 donates its levels and nothing else: no directional movement without a
            # previous bar, and no true range without a previous close -- the same
            # convention ATR now follows, so the two smooth the same TR series.
            self._prev_high, self._prev_low, self._prev_close = high, low, close
            return

        tr = self._true_range(high, low)
        assert tr is not None  # _prev_close was set on bar 0, before any TR is consumed
        up_move = high - self._prev_high
        down_move = self._prev_low - low
        plus_dm = up_move if (up_move > down_move and up_move > 0.0) else 0.0
        minus_dm = down_move if (down_move > up_move and down_move > 0.0) else 0.0
        self._prev_high, self._prev_low, self._prev_close = high, low, close

        n = float(self.period)
        if self._sum_tr is None:
            self._seed_plus.append(plus_dm)
            self._seed_minus.append(minus_dm)
            self._seed_tr.append(tr)
            if len(self._seed_tr) < self.period:
                return
            self._sum_plus = math.fsum(self._seed_plus)
            self._sum_minus = math.fsum(self._seed_minus)
            self._sum_tr = math.fsum(self._seed_tr)
            self._seed_plus, self._seed_minus, self._seed_tr = [], [], []
        else:
            self._sum_plus = self._sum_plus - self._sum_plus / n + plus_dm  # type: ignore[operator]
            self._sum_minus = self._sum_minus - self._sum_minus / n + minus_dm  # type: ignore[operator]
            self._sum_tr = self._sum_tr - self._sum_tr / n + tr

        # A zero smoothed true range means every bar in the window had an identical
        # high, low and close. Real markets do not, but a synthetic smoke-run series can,
        # and dividing there would end a validation run in a ZeroDivisionError that has
        # nothing to do with the strategy under test.
        if self._sum_tr == 0.0:
            plus_di = minus_di = 0.0
        else:
            plus_di = 100.0 * self._sum_plus / self._sum_tr
            minus_di = 100.0 * self._sum_minus / self._sum_tr
        self.plus_di._push(plus_di)
        self.minus_di._push(minus_di)

        di_sum = plus_di + minus_di
        dx = 0.0 if di_sum == 0.0 else 100.0 * abs(plus_di - minus_di) / di_sum

        if self._adx is None:
            self._dx_seed.append(dx)
            if len(self._dx_seed) < self.period:
                return
            self._adx = math.fsum(self._dx_seed) / self.period
            self._dx_seed = []
        else:
            self._adx = (self._adx * (n - 1.0) + dx) / n
        self._push(self._adx)


class Bollinger(_BarIndicator):
    """Bollinger bands. `.value` is the middle band; `.upper`/`.lower` are derived.

    Standard deviation is the *population* form (divide by `n`), matching Bollinger's own
    definition and every charting package. The window is the whole population of interest,
    not a sample drawn from a larger one, so the Bessel correction would be answering a
    question nobody asked -- and would put the bands ~2.6% wider at `n = 20` than the chart
    the author is looking at.

    `.bandwidth` and `.percent_b` are derived series like the bands themselves, so they
    have `.prev`, `.series()` and the `crossed_*` edge forms: a squeeze entry is
    `bb.bandwidth.crossed_below(threshold)` and reads as one. They were plain computed
    properties until the Phase 6 audit -- current value only, no history -- which left the
    two readings a Bollinger strategy most wants to cross-test as the only ones on the
    class with no edge form, forcing exactly the hand-rolled `a > b` level comparison the
    module docstring warns latches. Read them as `bb.bandwidth.value` / `bb.percent_b.value`.
    """

    def __init__(
        self,
        period: int = 20,
        deviations: float = 2.0,
        *,
        source: str = "close",
        history: int = DEFAULT_HISTORY,
    ) -> None:
        super().__init__(source=source, history=history)
        self.period = _check_period(period, name="period", minimum=2)
        if isinstance(deviations, bool) or not isinstance(deviations, (int, float)):
            raise TypeError("deviations must be a number")
        if deviations <= 0:
            raise ValueError(f"deviations must be positive, got {deviations}")
        self.deviations = float(deviations)
        self._window: deque[float] = deque(maxlen=self.period)
        self.upper = DerivedSeries(self.period, history=history, feed=self.feed)
        self.lower = DerivedSeries(self.period, history=history, feed=self.feed)
        self.bandwidth = DerivedSeries(self.period, history=history, feed=self.feed)
        """`(upper - lower) / middle`, per bar, with history and `.prev`.

        One stated hole: a bar whose middle band is exactly zero has no defined bandwidth
        and pushes nothing (a `None` placeholder would corrupt what `.prev` means -- see
        `Indicator`). Unreachable from a price source, where every value is positive;
        possible only for a hand-picked source such as a spread. `.prev` there means "the
        previous *defined* reading"."""
        self.percent_b = DerivedSeries(self.period, history=history, feed=self.feed)
        """Where the latest source value sits across the bands: 0 at lower, 1 at upper.

        Zero variance puts the price simultaneously at both bands; 0.5 is the only answer
        that does not arbitrarily claim an extreme, so the series stays gapless."""

    @property
    def warmup(self) -> int:
        return self.period

    def _on_bar(self, bar: Bar, x: float) -> None:
        self._window.append(x)
        if len(self._window) < self.period:
            return
        window = tuple(self._window)
        mean = math.fsum(window) / self.period
        # Two-pass variance, recomputed from the window rather than carried incrementally.
        # The incremental sum-of-squares form suffers catastrophic cancellation when the
        # mean is large relative to the spread -- which is precisely a price series, where
        # the mean is 60000 and the deviation is 50 -- and can return a negative variance.
        variance = math.fsum((v - mean) ** 2 for v in window) / self.period
        sd = math.sqrt(variance) if variance > 0.0 else 0.0
        upper = mean + self.deviations * sd
        lower = mean - self.deviations * sd
        self._push(mean)
        self.upper._push(upper)
        self.lower._push(lower)
        if mean != 0.0:
            self.bandwidth._push((upper - lower) / mean)
        span = upper - lower
        self.percent_b._push(0.5 if span == 0.0 else (x - lower) / span)


class Donchian(Indicator):
    """Donchian channel over `period` bars, **including the current bar**.

    The breakout variant excludes the current bar so that "close above the channel" is
    possible at all. Both are legitimate and they differ by one bar, which is exactly the
    kind of difference that silently doubles or halves a backtest's trade count. Only the
    inclusive form is offered; a breakout strategy compares against `.upper.prev`, which
    says what it means.

    **Because `.upper.prev` is the documented idiom, `warmup` covers it**: `period + 1`
    updates, not `period`. The channel itself is full one bar sooner, but at that bar
    `.upper.prev` is still `None`, and a strategy gated on `ctx.warm` would meet a
    `TypeError` on its first tradeable bar from the exact comparison this docstring tells
    it to write. Same reasoning as `MACD.warmup` covering the signal line: warm-up gates
    on the slowest thing the indicator tells you to read (see `Indicator.warmup`).
    """

    feed = "bar"

    def __init__(self, period: int = 20, *, history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        self.period = _check_period(period, name="period", minimum=2)
        self._highs: deque[float] = deque(maxlen=self.period)
        self._lows: deque[float] = deque(maxlen=self.period)
        self.upper = DerivedSeries(self.period + 1, history=history, feed=self.feed)
        self.lower = DerivedSeries(self.period + 1, history=history, feed=self.feed)

    @property
    def warmup(self) -> int:
        # One past the window: the documented breakout comparison reads `.upper.prev`.
        return self.period + 1

    def update(self, bar: Bar) -> None:
        self._updates += 1
        self._highs.append(_scaled_to_float(bar.high))
        self._lows.append(_scaled_to_float(bar.low))
        if len(self._highs) < self.period:
            return
        top = max(self._highs)
        bottom = min(self._lows)
        self.upper._push(top)
        self.lower._push(bottom)
        self._push((top + bottom) / 2.0)


class VWAP(Indicator):
    """Session-anchored volume-weighted average price (spec 5.4).

    Anchored, never rolling: VWAP's meaning comes from the anchor, and a "rolling VWAP" is
    just a volume-weighted moving average wearing the name. The session resets at each UTC
    boundary of `session`, derived from the bar's *open* time so a bar belongs to the
    session it started in.
    """

    feed = "bar"

    def __init__(
        self,
        session_ms: int = 86_400_000,
        *,
        source: str = "hlc3",
        history: int = DEFAULT_HISTORY,
    ) -> None:
        super().__init__(history=history)
        if isinstance(session_ms, bool) or not isinstance(session_ms, int) or session_ms <= 0:
            raise ValueError(f"session_ms must be a positive int, got {session_ms!r}")
        if source not in SOURCES:
            raise ValueError(f"unknown price source {source!r}")
        self.session_ms = session_ms
        self.source = source
        self._session: int | None = None
        self._pv = 0.0
        self._volume = 0.0

    @property
    def warmup(self) -> int:
        return 1

    def update(self, bar: Bar) -> None:
        self._updates += 1
        session = bar.open_time // self.session_ms
        if session != self._session:
            self._session = session
            self._pv = 0.0
            self._volume = 0.0

        price = _source_value(bar, self.source)
        volume = _scaled_to_float(bar.volume)
        self._pv += price * volume
        self._volume += volume
        # A session that has only seen zero-volume bars has no volume-weighted price. The
        # unweighted price is the limit as volume goes to zero and keeps the series
        # continuous; returning None would put a hole in it that every consumer -- crosses,
        # `.series()`, the chart -- would have to special-case.
        self._push(self._pv / self._volume if self._volume > 0.0 else price)


class RealisedVolatility(Indicator):
    """Annualised realised volatility from log returns over `period` bars.

    The zero-drift estimator, `sqrt(mean(r^2) * periods_per_year)`. Subtracting an
    estimated mean is the wrong move at these window sizes: over 20 bars the drift estimate
    is almost entirely noise, and removing it adds more variance to the volatility estimate
    than the drift it removes. This is also what "realised volatility" means in the
    literature, as distinct from the sample standard deviation of returns.

    **A zero observation with `source="volume"` is data, not an error.** Volume on a thin
    symbol does go to zero, and a zero has no log return in either direction -- so the bar
    is skipped and the return window is *cleared*, not left to straddle the gap: a window
    of `period` returns that silently skipped a bar would span more bars than it claims,
    which is the very corner this class refuses elsewhere. `.value` freezes at its last
    reading until `period` consecutive well-defined returns exist again, so readiness after
    such a gap arrives later than `warmup` alone promises -- a property of the data, not of
    the indicator. A zero *price*, or a negative anything, remains impossible in USD-M
    market data and is refused loudly -- and refused **before** `_prev` advances, so a
    caller that catches the error does not also inherit a corrupted return series.
    """

    feed = "bar"

    def __init__(
        self,
        period: int = 20,
        *,
        bar_ms: int = 60_000,
        source: str = "close",
        history: int = DEFAULT_HISTORY,
    ) -> None:
        super().__init__(history=history)
        self.period = _check_period(period, name="period", minimum=2)
        if isinstance(bar_ms, bool) or not isinstance(bar_ms, int) or bar_ms <= 0:
            raise ValueError(f"bar_ms must be a positive int, got {bar_ms!r}")
        if source not in SOURCES:
            raise ValueError(f"unknown price source {source!r}")
        self.source = source
        self.bar_ms = bar_ms
        self.periods_per_year = MS_PER_YEAR / bar_ms
        self._returns: deque[float] = deque(maxlen=self.period)
        self._prev: float | None = None

    @property
    def warmup(self) -> int:
        return self.period + 1

    def update(self, bar: Bar) -> None:
        self._updates += 1
        price = _source_value(bar, self.source)
        if price < 0.0 or (price == 0.0 and self.source != "volume"):
            # Impossible in USD-M market data: prices are strictly positive and volume is
            # non-negative. Refused before `_prev` moves -- this used to advance `_prev`
            # first, so the exception left the series primed to compute its next return
            # against the very observation it had just rejected.
            raise ValueError(
                f"realised volatility needs positive prices, got {self._prev} -> {price}"
            )
        prev = self._prev
        self._prev = price
        if prev is None:
            return
        if price == 0.0 or prev == 0.0:
            # A legitimate zero volume (see the class docstring): no log return exists
            # across this bar in either direction. Clearing rather than skipping keeps the
            # window's claim -- `period` *consecutive* returns -- true; the next value is
            # computed once the tape provides that many again.
            self._returns.clear()
            return
        self._returns.append(math.log(price / prev))
        if len(self._returns) < self.period:
            return
        mean_square = math.fsum(r * r for r in self._returns) / self.period
        self._push(math.sqrt(mean_square * self.periods_per_year))


# --------------------------------------------------------------------------- flow


class OBV(Indicator):
    """On-balance volume: cumulative volume signed by the close-to-close direction.

    An unchanged close adds nothing. Assigning it to either side -- as some
    implementations do -- makes the indicator drift in a direction the price did not move,
    which on a thin series is most of the signal.
    """

    feed = "bar"

    def __init__(self, *, history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        self._prev_close: float | None = None
        self._total = 0.0

    @property
    def warmup(self) -> int:
        return 2

    def update(self, bar: Bar) -> None:
        self._updates += 1
        close = _scaled_to_float(bar.close)
        volume = _scaled_to_float(bar.volume)
        if self._prev_close is not None:
            if close > self._prev_close:
                self._total += volume
            elif close < self._prev_close:
                self._total -= volume
            self._push(self._total)
        self._prev_close = close


class CVD(Indicator):
    """Cumulative volume delta from aggregate trades (spec 5.4).

    `is_buyer_maker` is the aggressor flag and its sense is the whole indicator: `True`
    means the *buyer* was the resting maker, so the trade was sell-aggressive and counts
    negative. Inverting it does not break anything visibly -- it produces a plausible line
    that is the exact negative of the truth.
    """

    feed = "trade"

    def __init__(self, *, history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        self._total = 0.0

    @property
    def warmup(self) -> int:
        return 1

    def update(self, trade: TradePrint) -> None:
        self._updates += 1
        # `qty_scaled`, not `qty`. `TradePrint` exposes both: the lake's exact integer and a
        # float view for indicator code. `_scaled_to_float` expects the integer, so reading
        # the float view divided by 10^8 a second time -- every volume in this series was a
        # hundred-millionth of its true size, on a line whose whole purpose is its slope.
        qty = _scaled_to_float(trade.qty_scaled)
        self._total += -qty if trade.is_buyer_maker else qty
        self._push(self._total)


class BookImbalance(Indicator):
    """Top-of-book depth imbalance in `[-1, +1]`, from a depth snapshot.

    `+1` is all bid, `-1` is all ask. Requires depth data, so it is only meaningful at the
    `BOOK_WALK` fill tier (spec 4.2); at lower tiers no depth snapshots are delivered and
    the indicator simply never becomes ready, rather than reporting a fabricated zero.
    """

    feed = "depth"

    def __init__(self, levels: int = 5, *, history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        self.levels = _check_period(levels, name="levels")

    @property
    def warmup(self) -> int:
        return 1

    def update(self, snapshot: DepthSnapshot) -> None:
        self._updates += 1
        bids = math.fsum(_scaled_to_float(q) for q in snapshot.bid_qty[: self.levels])
        asks = math.fsum(_scaled_to_float(q) for q in snapshot.ask_qty[: self.levels])
        total = bids + asks
        self._push(0.0 if total == 0.0 else (bids - asks) / total)


class FundingMean(Indicator):
    """Mean of the last `period` realised funding rates (spec 3.5).

    Fed from settlements, not from the predicted rate: the predicted rate is a forecast
    that changes until the settlement millisecond, and averaging forecasts alongside
    realised values mixes two different quantities.
    """

    feed = "funding"

    def __init__(self, period: int = 3, *, history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        self.period = _check_period(period, name="period")
        self._window: deque[float] = deque(maxlen=self.period)

    @property
    def warmup(self) -> int:
        return self.period

    def update(self, rate: float) -> None:
        self._updates += 1
        self._window.append(float(rate))
        if len(self._window) < self.period:
            return
        self._push(math.fsum(self._window) / self.period)


class OIDelta(Indicator):
    """Change in open interest over `period` observations."""

    feed = "oi"

    def __init__(self, period: int = 1, *, history: int = DEFAULT_HISTORY) -> None:
        super().__init__(history=history)
        self.period = _check_period(period, name="period")
        self._window: deque[float] = deque(maxlen=self.period + 1)

    @property
    def warmup(self) -> int:
        return self.period + 1

    def update(self, open_interest: float) -> None:
        self._updates += 1
        self._window.append(float(open_interest))
        if len(self._window) < self.period + 1:
            return
        self._push(self._window[-1] - self._window[0])


# ------------------------------------------------------------------------- registry


_FEEDS = ("bar", "trade", "depth", "funding", "oi")


class IndicatorSet:
    """`ctx.indicators` — constructs, registers and drives every indicator in a run.

    Registration happens at construction, so `ctx.indicators.ema(200)` inside `on_start`
    both returns the indicator and wires it up. That single call is why the engine can
    derive the warm-up length without the author declaring anything (spec 5.4 rule 4).

    Indicators are keyed by symbol. A multi-symbol strategy calling `ema(20,
    symbol="ETHUSDT")` gets one driven by ETHUSDT bars only; without the key, every symbol
    in the run would feed the same series and the result would be an average of unrelated
    instruments.
    """

    def __init__(
        self,
        *,
        primary_symbol: str,
        symbols: Sequence[str] | None = None,
        bar_ms: int = 60_000,
    ) -> None:
        self.primary_symbol = primary_symbol
        self.symbols = tuple(symbols) if symbols else (primary_symbol,)
        if primary_symbol not in self.symbols:
            raise ValueError(
                f"primary symbol {primary_symbol!r} is not among {list(self.symbols)}"
            )
        self.bar_ms = bar_ms
        self._registry: dict[tuple[str, str], list[Indicator]] = {}
        self._last_bar_open: dict[str, int] = {}
        self._last_trade_id: dict[str, int] = {}
        self._last_depth_id: dict[str, int] = {}
        self._frozen = False

    # ------------------------------------------------------------------ lifecycle

    def freeze(self) -> None:
        """Forbid further registration once the run's first bar has been dispatched.

        An indicator created mid-run has a shorter history than every other series in the
        set, so its warm-up has already been passed by the engine's gate and it starts
        emitting values that are not warm. Constructing indicators in `on_start` is the
        documented lifecycle (spec 5.2); this makes the alternative fail loudly instead of
        producing a subtly unwarmed signal.
        """
        self._frozen = True

    def register(self, indicator: Indicator, *, symbol: str | None = None) -> Indicator:
        """Attach an indicator to a symbol's feed. Returns it, for chaining."""
        if isinstance(indicator, DerivedSeries):
            raise TypeError(
                f"{type(indicator).__name__} is driven by its parent indicator and must "
                "not be registered; doing so would advance it twice per bar"
            )
        if self._frozen:
            raise RuntimeError(
                "indicators must be created in on_start, before the first bar. One "
                "created later has seen less history than the run's warm-up gate assumes, "
                "so it would emit unwarmed values that look warm (spec 5.2)."
            )
        target = symbol or self.primary_symbol
        if target not in self.symbols:
            raise ValueError(
                f"symbol {target!r} is not in this run; declared symbols are "
                f"{list(self.symbols)}"
            )
        if indicator.feed not in _FEEDS:
            raise ValueError(f"unknown indicator feed {indicator.feed!r}")
        self._registry.setdefault((target, indicator.feed), []).append(indicator)
        return indicator

    def all(self) -> tuple[Indicator, ...]:
        return tuple(
            indicator for group in self._registry.values() for indicator in group
        )

    @property
    def warmup(self) -> int:
        """Bars of warm-up the registered set needs (spec 5.4 rule 4).

        Bar-driven indicators only. A funding-mean over three settlements is not
        convertible into a number of 15-minute bars -- the funding interval is a property
        of the symbol and has changed historically (spec 3.5) -- so pretending otherwise
        would produce a warm-up figure that is wrong by an unknown amount. Non-bar
        indicators expose `.ready` instead, and `all_ready` aggregates it.
        """
        bar_indicators = [
            indicator
            for (_, feed), group in self._registry.items()
            if feed == "bar"
            for indicator in group
        ]
        return max((i.warmup for i in bar_indicators), default=0)

    @property
    def all_ready(self) -> bool:
        """True when *every* registered indicator, bar-driven or not, has a value."""
        return all(indicator.ready for indicator in self.all())

    def not_ready(self) -> tuple[Indicator, ...]:
        return tuple(i for i in self.all() if not i.ready)

    # -------------------------------------------------------------------- dispatch

    def on_bar(self, bar: Bar) -> None:
        """Drive every bar indicator for this bar's symbol. Closed bars only."""
        last = self._last_bar_open.get(bar.symbol)
        if last is not None and bar.open_time <= last:
            # A repeated or out-of-order bar would advance every recursive indicator an
            # extra step, permanently shifting the series against the price. Loud, because
            # the shift is invisible downstream.
            raise ValueError(
                f"{bar.symbol} bar at {bar.open_time} does not advance past {last}; "
                "indicators must be driven by closed bars in order (spec 6.2)"
            )
        self._last_bar_open[bar.symbol] = bar.open_time
        for indicator in self._registry.get((bar.symbol, "bar"), ()):
            indicator.update(bar)

    def on_trade(self, trade: TradePrint) -> None:
        """Drive trade indicators, once per distinct print.

        Guarded by `agg_id` the way `on_bar` is guarded by `open_time`, and for the same
        reason: a print delivered twice advances every cumulative series twice, and CVD's
        entire output is a running total, so one redelivered trade shifts it *permanently*.
        `agg_id` is Binance's own per-symbol monotonic sequence, so a print at or below the
        last one seen is a replay -- a reconnect re-sending its buffer, a retried poll --
        and is dropped. Dropped silently rather than raised, unlike the bar guard: bars are
        the engine's own clock and a duplicate there is an engine bug worth halting for,
        while duplicate prints are an ordinary property of the transports that carry them.
        Keyed per symbol, because `agg_id` sequences from different symbols overlap freely.
        """
        last = self._last_trade_id.get(trade.symbol)
        if last is not None and trade.agg_id <= last:
            return
        self._last_trade_id[trade.symbol] = trade.agg_id
        for indicator in self._registry.get((trade.symbol, "trade"), ()):
            indicator.update(trade)

    def on_depth(self, snapshot: DepthSnapshot) -> None:
        """Drive depth indicators, once per distinct snapshot.

        Same replay guard as `on_trade`, keyed on the exchange's own `last_update_id`. A
        depth-driven indicator's value is a function of the whole series it was fed, so a
        redelivered snapshot is one fabricated extra observation in every window from then
        on. Funding and open-interest dispatch below carry no such guard, stated rather
        than implied: their payloads are bare floats with no event identity, and two equal
        consecutive rates are routinely the truth rather than a replay.
        """
        last = self._last_depth_id.get(snapshot.symbol)
        if last is not None and snapshot.last_update_id <= last:
            return
        self._last_depth_id[snapshot.symbol] = snapshot.last_update_id
        for indicator in self._registry.get((snapshot.symbol, "depth"), ()):
            indicator.update(snapshot)

    def on_funding(self, symbol: str, rate: float) -> None:
        for indicator in self._registry.get((symbol, "funding"), ()):
            indicator.update(rate)

    def on_open_interest(self, symbol: str, value: float) -> None:
        for indicator in self._registry.get((symbol, "oi"), ()):
            indicator.update(value)

    # ------------------------------------------------------------------ factories

    def ema(self, period: int, *, source: str = "close", symbol: str | None = None) -> EMA:
        return self.register(EMA(period, source=source), symbol=symbol)  # type: ignore[return-value]

    def sma(self, period: int, *, source: str = "close", symbol: str | None = None) -> SMA:
        return self.register(SMA(period, source=source), symbol=symbol)  # type: ignore[return-value]

    def wma(self, period: int, *, source: str = "close", symbol: str | None = None) -> WMA:
        return self.register(WMA(period, source=source), symbol=symbol)  # type: ignore[return-value]

    def rsi(self, period: int = 14, *, source: str = "close", symbol: str | None = None) -> RSI:
        return self.register(RSI(period, source=source), symbol=symbol)  # type: ignore[return-value]

    def macd(
        self,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        *,
        source: str = "close",
        symbol: str | None = None,
    ) -> MACD:
        return self.register(MACD(fast, slow, signal, source=source), symbol=symbol)  # type: ignore[return-value]

    def atr(self, period: int = 14, *, symbol: str | None = None) -> ATR:
        return self.register(ATR(period), symbol=symbol)  # type: ignore[return-value]

    def adx(self, period: int = 14, *, symbol: str | None = None) -> ADX:
        return self.register(ADX(period), symbol=symbol)  # type: ignore[return-value]

    def bollinger(
        self,
        period: int = 20,
        deviations: float = 2.0,
        *,
        source: str = "close",
        symbol: str | None = None,
    ) -> Bollinger:
        return self.register(Bollinger(period, deviations, source=source), symbol=symbol)  # type: ignore[return-value]

    def donchian(self, period: int = 20, *, symbol: str | None = None) -> Donchian:
        return self.register(Donchian(period), symbol=symbol)  # type: ignore[return-value]

    def vwap(
        self,
        session_ms: int = 86_400_000,
        *,
        source: str = "hlc3",
        symbol: str | None = None,
    ) -> VWAP:
        return self.register(VWAP(session_ms, source=source), symbol=symbol)  # type: ignore[return-value]

    def realised_volatility(
        self, period: int = 20, *, source: str = "close", symbol: str | None = None
    ) -> RealisedVolatility:
        return self.register(
            RealisedVolatility(period, bar_ms=self.bar_ms, source=source), symbol=symbol
        )  # type: ignore[return-value]

    def obv(self, *, symbol: str | None = None) -> OBV:
        return self.register(OBV(), symbol=symbol)  # type: ignore[return-value]

    def cvd(self, *, symbol: str | None = None) -> CVD:
        return self.register(CVD(), symbol=symbol)  # type: ignore[return-value]

    def book_imbalance(self, levels: int = 5, *, symbol: str | None = None) -> BookImbalance:
        return self.register(BookImbalance(levels), symbol=symbol)  # type: ignore[return-value]

    def funding_mean(self, period: int = 3, *, symbol: str | None = None) -> FundingMean:
        return self.register(FundingMean(period), symbol=symbol)  # type: ignore[return-value]

    def oi_delta(self, period: int = 1, *, symbol: str | None = None) -> OIDelta:
        return self.register(OIDelta(period), symbol=symbol)  # type: ignore[return-value]


_ENGINE_ONLY = frozenset(
    {"freeze", "on_bar", "on_trade", "on_depth", "on_funding", "on_open_interest"}
)
"""`IndicatorSet`'s dispatch surface -- the methods `SealedIndicators` refuses to proxy."""


class SealedIndicators:
    """What `ctx.indicators` actually is: registration and reads, never dispatch.

    `IndicatorSet.freeze` closes *registration* after the first bar (spec 5.2), but the
    dispatch surface stayed open through the context: `ctx.indicators.on_bar(bar)` from a
    hook advanced every registered series off a bar the market never printed, permanently
    shifting each one against the price with no error anywhere. The block cannot live on
    `IndicatorSet` itself, because dispatching after freeze is precisely the engine's job
    -- so the engine keeps the set it constructed and drives it directly, while the
    context wraps its copy in this facade: every read and every registration factory
    passes through untouched, and the six dispatch methods raise by name.

    Same doctrine as the rest of the context's sealing (spec 5.3): **not a security
    boundary.** An indicator the strategy already holds still has its underscore-private
    `_push`, and reaching the real set through this wrapper's own private slot is one
    deliberate line. This exists so the *accidental* spelling fails loudly at the line
    that did it, instead of producing a subtly wrong series downstream.
    """

    __slots__ = ("_set",)

    def __init__(self, indicator_set: IndicatorSet) -> None:
        object.__setattr__(self, "_set", indicator_set)

    def __getattr__(self, name: str) -> Any:
        if name in _ENGINE_ONLY:
            raise AttributeError(
                f"ctx.indicators.{name} is engine-only. The run feeds indicators from "
                "the market data it replays; a strategy pushing its own events would "
                "advance every registered series off data the market never printed. "
                "Register indicators in on_start and read their values -- the engine "
                "drives them (spec 5.2/5.4)."
            )
        if name.startswith("__") and name.endswith("__"):
            # Never proxy dunders: copy/pickle/inspect must see this wrapper for what it
            # is, and a dunder probe during construction must not recurse through _set.
            raise AttributeError(name)
        return getattr(self._set, name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(
            f"ctx.indicators.{name} cannot be assigned: the indicator registry is "
            "engine-owned. Strategy state belongs on the strategy object (self.*)."
        )

    def __repr__(self) -> str:
        return f"SealedIndicators({self._set!r})"


def warmup_of(indicators: Iterable[Indicator]) -> int:
    """Warm-up in bars for a loose collection of indicators. Bar-driven only."""
    return max((i.warmup for i in indicators if i.feed == "bar"), default=0)
