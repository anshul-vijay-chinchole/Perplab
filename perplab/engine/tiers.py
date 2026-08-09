"""Choosing a run's fill tier, and saying so when it is not the one that was asked for.

Spec 4.2, decision 3, is the whole of this module:

> L2 book-walking backtests are scoped to the collector's coverage window. The Runs tab must
> surface this explicitly: if a run's date range extends before depth coverage begins, the
> fill model silently degrades from `BOOK_WALK` to `BOOK_TICKER`. That degradation is
> recorded in run metadata and shown as a badge on the results page. **Never let a
> fill-model downgrade happen invisibly.**

The rule that makes that promise keepable is that the tier is **derived from the lake, never
accepted from a caller**. A tier passed in is a statement of what someone hoped was there;
`manifest.derive_fill_model_tier` reads which partitions actually hold published files, and
takes the gap report into account so that a day with one part-file does not win a fidelity
the data cannot support.

**What "requested" means, and why it is still an input.** A run may ask for a *lower* tier
than the data supports -- `BAR_CLOSE` is spec 4.2's "explicit opt-in only", and it is the
right choice when you want a fast sweep and know the results are indicative. So the resolved
tier is `min(requested, available)`, and the two cases are reported differently: asking for
less than you have is a choice (`TIER_BELOW_DATA`), getting less than you asked for is a
degradation (`TIER_DEGRADED`). Collapsing them would make a deliberate low-fidelity sweep
look like a data problem, and a data problem look like a preference.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from perplab.data.manifest import (
    TICK_GAP_TOLERANCE,
    CoverageError,
    derive_fill_model_tier,
    unexplained_gap_ms,
)
from perplab.data.gaps import format_duration
from perplab.data.query import market_root, query
from perplab.data.schemas import normalise_symbol
from perplab.strategy.context import FillTier

__all__ = [
    "TierResolution",
    "resolve_tier",
    "tier_from_name",
    "TIER_CAPABILITIES",
    "TIER_INPUTS",
]


TIER_INPUTS: dict[str, tuple[str, ...]] = {
    "BOOK_WALK": ("depth20",),
    "BOOK_TICKER": ("aggTrades", "bookTicker"),
    "TRADE_ONLY": ("aggTrades",),
    "BAR_CLOSE": (),
}
"""Which tick datasets each tier prices fills from.

Used to report tolerated gaps against the tier that actually consumed the dataset. A hole in
`bookTicker` is worth saying nothing about on a `TRADE_ONLY` run -- that run never read it,
and a warning about data it did not use is noise that trains the reader to skip the badge
that matters. `BAR_CLOSE` is empty because it prices from klines, whose holes are the gap
policy's business (spec 4.5), not the tier's.
"""


def tier_from_name(name: str) -> FillTier:
    """Parse a tier name, refusing an unknown one rather than defaulting it.

    Defaulting would re-price every fill in a run while leaving its stored identity
    unchanged, which is exactly what spec 12.1's reproducibility contract exists to make
    impossible.
    """
    try:
        return FillTier[name]
    except KeyError:
        raise ValueError(
            f"unknown fill tier {name!r}; expected one of "
            f"{[t.name for t in FillTier]}"
        ) from None


TIER_CAPABILITIES: dict[str, dict[str, bool]] = {
    "BAR_CLOSE": {"market": True, "limit": False, "trigger": False, "book": False},
    "TRADE_ONLY": {"market": True, "limit": False, "trigger": True, "book": False},
    "BOOK_TICKER": {"market": True, "limit": True, "trigger": True, "book": False},
    "BOOK_WALK": {"market": True, "limit": True, "trigger": True, "book": True},
}
"""What each tier can execute, as data rather than as four `if` statements.

Served to the frontend so the New Backtest dialog can say *why* a tier is a downgrade in the
terms the user cares about -- "limit orders unavailable" rather than "BOOK_TICKER" -- and
consumed by nothing in the engine, which enforces the same matrix through
`backtest.LIMIT_TIERS` and `backtest.TRIGGER_TIERS`. Two representations of one fact, and
`test_tiers.py` asserts they agree.
"""


_BAR_DATASETS: tuple[tuple[str, str, str], ...] = (
    ("klines", "open_time", "close_time"),
    ("markPriceKlines", "open_time", "close_time"),
)
"""The two datasets every run needs, whatever its tier, and the columns that bound them."""


def _uncovered_bars(
    userdata: Path | str, symbols: Sequence[str], start_ms: int, end_ms: int
) -> list[str]:
    """Which of `klines` / `markPriceKlines` fail to span `[start_ms, end_ms)`, by row bounds.

    Row bounds rather than `dataset_covers`, because both are partitioned by month: presence
    answers "is there a file somewhere in this month", and a range starting the day after the
    last published bar sits inside a covered month. One `min`/`max` aggregate per dataset per
    symbol, against a partition-pruned scan.
    """
    root = market_root(userdata)
    uncovered: list[str] = []
    for dataset, lo_column, hi_column in _BAR_DATASETS:
        for symbol in {normalise_symbol(s) for s in symbols}:
            table = query(
                root,
                f"""
                SELECT min("{lo_column}") AS lo, max("{hi_column}") AS hi
                FROM "{dataset}" WHERE "symbol" = ?
                """,
                datasets=(dataset,),
                params=[symbol],
            )
            row = table.to_pylist()[0] if table.num_rows else {}
            lo, hi = row.get("lo"), row.get("hi")
            if lo is None or hi is None or lo > start_ms or hi + 1 < end_ms:
                uncovered.append(dataset)
                break
    return uncovered


@dataclass(frozen=True, slots=True)
class TierResolution:
    """The tier a run will execute at, what it asked for, and why they differ."""

    tier: FillTier
    """What the run executes at: `min(requested, available)`."""

    requested: FillTier
    available: FillTier
    """The best tier the lake can support over this exact range."""

    flags: tuple[str, ...]
    reason: str | None
    """One sentence for the results page, or `None` when the run got what it asked for."""

    @property
    def degraded(self) -> bool:
        return self.tier < self.requested

    def to_json(self) -> dict[str, Any]:
        return {
            "tier": self.tier.name,
            "requested": self.requested.name,
            "available": self.available.name,
            "degraded": self.degraded,
            "reason": self.reason,
            "capabilities": TIER_CAPABILITIES[self.tier.name],
        }


def resolve_tier(
    userdata: Path | str,
    symbols: Sequence[str],
    start_ms: int,
    end_ms: int,
    *,
    requested: FillTier,
    gaps: Sequence[Any] = (),
) -> TierResolution:
    """Decide the run's tier from the lake, and record the difference from the request.

    `gaps` is the gap report for the same range. It matters here for the same reason it
    matters inside `derive_fill_model_tier`: presence is judged at partition granularity --
    day-level for the tick datasets -- so without it a day the collector was down for nine
    hours of would win `BOOK_WALK` on the strength of one part-file, and the run would walk a
    book that stops moving mid-afternoon.

    **A range the lake does not fully cover resolves to `BAR_CLOSE`, it does not fail.**
    `derive_fill_model_tier` demands a published file in *every* partition of the range and
    raises otherwise, which is the right rule for a manifest and the wrong one for a
    decision the run has to make in order to start. A range extending a few days past the
    end of the lake is a legible state the engine already handles -- it reads what is there
    and flags the shortfall -- and refusing here would discard a run over a presence check
    before it had read a single bar. If the lake genuinely holds nothing, `feed.load_bars`
    says so, at the point where the message can name the dataset and the range.
    """
    flags: list[str] = []
    reason: str | None = None

    # **Bars and marks come first, whatever the tick datasets hold.** `derive_fill_model_tier`
    # answers "what fill fidelity is supportable", and answers it from the tick datasets
    # alone -- so on this lake it happily returned `BOOK_WALK` for 2026-08-02, a day the
    # collector has depth for and the bulk kline archive has not published yet. Every tier
    # needs bars to drive the strategy and marks to price risk (spec 3.4 forbids deriving
    # one from the other), so a range missing either is not a `BOOK_WALK` range with a
    # detail wrong -- it is a range no run can execute over. Saying so here is what lets the
    # New Backtest dialog warn before the worker starts rather than after it fails.
    #
    # **Checked by row bounds, not by `dataset_covers`.** Both kline datasets partition by
    # *month*, so partition presence answers "is there a file somewhere in August" -- which
    # was `True` for 2026-08-02 on the strength of the single 2026-08-01 file, and the guard
    # let through exactly the case it was added for. Worse, a range half inside coverage
    # (2026-08-01 to 08-03) passed and then ran to completion on half its bars with no flag
    # at all. Bounds are exact and cost one aggregate per dataset.
    missing = _uncovered_bars(userdata, symbols, start_ms, end_ms)
    if missing:
        return TierResolution(
            tier=FillTier.BAR_CLOSE,
            requested=requested,
            available=FillTier.BAR_CLOSE,
            flags=("COVERAGE_INCOMPLETE", "LOW_FIDELITY")
            + (("TIER_DEGRADED",) if requested > FillTier.BAR_CLOSE else ()),
            reason=(
                f"{' and '.join(sorted(set(missing)))} do not span this range for "
                f"{sorted(set(symbols))}, so no run can execute over it whatever the tick "
                f"datasets hold. Ingest the range first."
                if len(set(missing)) > 1
                else f"{missing[0]} does not span this range for {sorted(set(symbols))}, so "
                f"no run can execute over it whatever the tick datasets hold. Ingest the "
                f"range first."
            ),
        )

    try:
        available = tier_from_name(
            derive_fill_model_tier(userdata, symbols, start_ms, end_ms, gaps=gaps)
        )
    except CoverageError as exc:  # pragma: no cover - klines are checked above
        return TierResolution(
            tier=FillTier.BAR_CLOSE,
            requested=requested,
            available=FillTier.BAR_CLOSE,
            flags=("COVERAGE_INCOMPLETE", "LOW_FIDELITY")
            + (("TIER_DEGRADED",) if requested > FillTier.BAR_CLOSE else ()),
            reason=(
                f"no tier is fully supportable over this range, so it ran at BAR_CLOSE on "
                f"whatever the lake holds: {exc}"
            ),
        )

    tier = available if available < requested else requested

    if tier < requested:
        flags.append("TIER_DEGRADED")
        lost = sorted(
            capability
            for capability, present in TIER_CAPABILITIES[requested.name].items()
            if present and not TIER_CAPABILITIES[tier.name][capability]
        )
        reason = (
            f"asked for {requested.name} but the lake only supports {available.name} over "
            f"this range, so the run executed at {tier.name}"
        )
        if lost:
            reason += f". Lost: {', '.join(lost)}"
        reason += (
            ". L2 depth exists only from the moment the collector started recording "
            "(spec 4.2), so a range reaching further back cannot walk the book."
        )
    elif available > tier:
        flags.append("TIER_BELOW_DATA")
        reason = (
            f"this run executed at {tier.name} by request; the lake could have supported "
            f"{available.name} over the same range"
        )

    if tier is FillTier.BAR_CLOSE:
        flags.append("LOW_FIDELITY")

    # **What the tolerance let through is stated, not swallowed.** `TICK_GAP_TOLERANCE`
    # exists so a nineteen-minute venue halt does not re-price a whole year at BAR_CLOSE,
    # and the price of that judgement is that some fills really were modelled over a
    # dataset with holes in it. Spec 4.2's rule is that fidelity is never quietly less than
    # it appears, so the run says which dataset, for how long, and over what share of its
    # range -- and every gap remains in the gap report and on the manifest regardless.
    tolerated = sorted(
        (dataset, missing)
        for dataset, missing in unexplained_gap_ms(gaps, symbols, start_ms, end_ms).items()
        if missing > 0 and dataset in TIER_INPUTS[tier.name]
    )
    if tolerated:
        flags.append("TICK_GAPS_TOLERATED")
        span = max(1, end_ms - start_ms)
        detail = ", ".join(
            f"{dataset} for {format_duration(missing)} ({missing / span:.3%} of the range)"
            for dataset, missing in tolerated
        )
        note = (
            f"{tier.name} was kept even though the lake is missing {detail}. Under "
            f"{TICK_GAP_TOLERANCE:.0%} of the range is holed, so demoting every bar to "
            f"BAR_CLOSE would model the whole run worse to protect that fraction of it. "
            f"Fills inside the gap were priced without those records; the gap report lists "
            f"each one."
        )
        reason = f"{reason} {note}" if reason else note

    return TierResolution(
        tier=tier,
        requested=requested,
        available=available,
        flags=tuple(flags),
        reason=reason,
    )
