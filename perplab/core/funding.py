"""Funding settlement (spec 3.5).

Perpetual futures have no expiry, so the funding payment is the mechanism that tethers the
contract to spot. It is also, for a meaningful class of strategy, the entire edge -- which
is why spec 8.4 insists PnL attribution keep it in its own column rather than folding it
into price PnL.

```
funding_cashflow = -Q * Pm(t) * F(t)
```

The sign convention falls out of that one minus: a long (`Q > 0`) with a positive rate
pays, a short receives, and a negative rate reverses both. `funding_cashflow` is the only
place that sign is expressed, deliberately -- it is easy to get backwards, and a strategy
built on funding capture that has the sign inverted looks brilliant in backtest.

**Three rules from spec 3.5 shape this module more than the formula does:**

1. *Discrete, never amortised.* A position open at the settlement millisecond pays in full;
   one closed a second earlier pays nothing. Smearing funding across the interval -- the
   obvious "smoother" implementation -- destroys every strategy that trades the settlement
   itself, and does so while still producing plausible aggregate numbers.
2. *Actual historical rates and timestamps.* Never the premium-index formula, never an
   assumed schedule.
3. *Never hardcode eight hours.* Binance runs different intervals on different symbols and
   has changed the interval on symbols already trading (R17). `exchangeInfo` did not carry
   `fundingIntervalHours` at all when it was checked on 2026-08-01, so in practice the
   interval has to come from the settlement record -- which is what `FundingSchedule` reads
   and `interval_segments` reports on.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, localcontext

from perplab.core.money import ACCOUNTING_CONTEXT, from_scaled

__all__ = [
    "FundingSettlement",
    "FundingSchedule",
    "IntervalSegment",
    "funding_cashflow",
    "interval_segments",
]

_MS_PER_HOUR = 3_600_000


@dataclass(frozen=True, slots=True)
class FundingSettlement:
    """One realised settlement, on the accounting side of the seam.

    `perplab.core.types.FundingRate` is the storage-side counterpart carrying scaled
    int64. This is its `Decimal` twin, and `from_record` is the crossing point. They are
    kept as two types rather than one because the whole value of the seam is that the
    compiler-visible type tells you which side of it you are on.
    """

    ts_ms: int
    rate: Decimal
    mark_price: Decimal

    @classmethod
    def from_record(cls, symbol_record: object) -> FundingSettlement:
        """Build from a scaled-int `types.FundingRate` read out of the lake."""
        return cls(
            ts_ms=int(getattr(symbol_record, "funding_ms")),
            rate=from_scaled(int(getattr(symbol_record, "funding_rate"))),
            mark_price=from_scaled(int(getattr(symbol_record, "mark_price"))),
        )


def funding_cashflow(qty: Decimal, mark_price: Decimal, rate: Decimal) -> Decimal:
    """`-Q * Pm * F` -- signed, added to the wallet as-is (spec 3.5).

    Positive means the account receives. The spec's own sign checks, reproduced as a
    reminder of which way round this goes:

    - long 1 @ mark 50 000, `F = +0.0001` -> `-5.00`, the long pays
    - short 1 (`Q = -1`), same rate       -> `+5.00`, the short receives
    - long, `F = -0.0001`                 -> `+5.00`, the long receives

    A flat position pays and receives nothing, which is the formula's own answer at
    `Q = 0` rather than a special case.
    """
    if mark_price < 0:
        raise ValueError(f"mark price must be non-negative, got {mark_price}")
    with localcontext(ACCOUNTING_CONTEXT):
        return -qty * mark_price * rate


@dataclass(frozen=True, slots=True)
class IntervalSegment:
    """A stretch of history over which the funding interval was constant."""

    start_ms: int
    end_ms: int
    interval_ms: int
    settlements: int

    @property
    def interval_hours(self) -> Decimal:
        return Decimal(self.interval_ms) / Decimal(_MS_PER_HOUR)


def interval_segments(times_ms: list[int], *, tolerance_ms: int = 60_000) -> list[IntervalSegment]:
    """Derive the funding interval(s) actually used, from settlement timestamps.

    Returns one segment per constant-interval stretch. More than one segment means the
    exchange changed the schedule mid-history, which spec 3.5 rule 3 says to expect and
    which a hardcoded eight hours would silently misprice on both sides of the change.

    `tolerance_ms` absorbs the ordinary jitter in settlement times -- Binance's settlements
    land within a few seconds of the nominal boundary, not on it exactly -- without
    absorbing a genuine schedule change, the smallest of which (8h to 4h) is four hours
    wide. Missing settlements are *not* absorbed: a doubled gap reads as a new segment,
    which is the honest outcome, because a hole in the record and a schedule change are
    genuinely indistinguishable from timestamps alone and the loud answer is the safe one.
    """
    if len(times_ms) < 2:
        return []

    ordered = sorted(times_ms)
    gaps = [b - a for a, b in zip(ordered, ordered[1:])]

    segments: list[IntervalSegment] = []
    seg_start_index = 0
    # Seed with the modal gap of the leading run rather than the first gap alone: a single
    # anomalous first gap (a late first settlement after listing, which does happen) would
    # otherwise define the whole segment's nominal interval.
    current = gaps[0]

    def flush(start_i: int, end_i: int, interval: int) -> None:
        window = gaps[start_i:end_i]
        if not window:
            return
        modal = Counter(window).most_common(1)[0][0] if window else interval
        segments.append(
            IntervalSegment(
                start_ms=ordered[start_i],
                end_ms=ordered[end_i],
                interval_ms=modal,
                settlements=end_i - start_i + 1,
            )
        )

    for i, gap in enumerate(gaps):
        if abs(gap - current) > tolerance_ms:
            flush(seg_start_index, i, current)
            seg_start_index = i
            current = gap
    flush(seg_start_index, len(gaps), current)
    return segments


@dataclass(frozen=True, slots=True)
class FundingSchedule:
    """The historical settlement record for one symbol, queried by time window.

    Built from what actually settled, never from an assumed cadence. `due` is the method
    the event loop uses to find the settlements inside a step; everything else is
    diagnostics.
    """

    symbol: str
    settlements: tuple[FundingSettlement, ...]

    def __post_init__(self) -> None:
        times = [s.ts_ms for s in self.settlements]
        if times != sorted(times):
            raise ValueError(f"{self.symbol}: settlements must be in timestamp order")
        if len(set(times)) != len(times):
            raise ValueError(f"{self.symbol}: duplicate settlement timestamps")

    def due(self, start_ms: int, end_ms: int) -> tuple[FundingSettlement, ...]:
        """Settlements in the half-open window `[start_ms, end_ms)`.

        Half-open, and documented as such, because the alternative double-charges. An
        event loop that steps in windows and asks for `[a, b]` then `[b, c]` settles
        anything landing exactly on `b` twice -- and settlement timestamps are round
        numbers, so landing exactly on a step boundary is the common case rather than the
        rare one.
        """
        if end_ms < start_ms:
            raise ValueError(f"window end {end_ms} precedes start {start_ms}")
        return tuple(s for s in self.settlements if start_ms <= s.ts_ms < end_ms)

    def segments(self) -> list[IntervalSegment]:
        return interval_segments([s.ts_ms for s in self.settlements])

    @property
    def interval_ms(self) -> int | None:
        """The single interval in force across the whole record, or None if it changed.

        `None` is a deliberate refusal rather than a best guess. A caller that wants one
        number for a symbol whose schedule changed is about to misprice one side of the
        change, and should be looking at `segments()` instead.
        """
        segs = self.segments()
        if len(segs) != 1:
            return None
        return segs[0].interval_ms
