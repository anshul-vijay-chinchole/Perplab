"""Fill-tier resolution and degradation (spec 4.2 decision 3).

> Never let a fill-model downgrade happen invisibly.

That sentence is the second half of Phase 5's exit criterion, and it has three parts that
fail independently: the tier has to be *derived* from the lake rather than accepted from a
caller, the difference between what was asked for and what was possible has to be *recorded*,
and the two have to be distinguishable from a deliberate low-fidelity run. A resolution that
silently returned the requested tier would pass any test that only checked the happy path.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from perplab.engine.backtest import LIMIT_TIERS, TRIGGER_TIERS
from perplab.engine.tiers import (
    TIER_CAPABILITIES,
    resolve_tier,
    tier_from_name,
)
from perplab.strategy.context import FillTier
from tests.engine_lake import MS_PER_MINUTE, build_lake, flat_path

START = 1_709_251_200_000  # 2024-03-01T00:00:00Z
END = START + 60 * MS_PER_MINUTE


def _lake(root: Path, **datasets) -> Path:
    """A one-hour lake carrying exactly the tick datasets named."""
    build_lake(
        root / "market",
        start_ms=START,
        minutes=60,
        trade_path=flat_path(40_000.0),
        **datasets,
    )
    return root


def _ticks(count: int = 3600):
    return [(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(count)]


def _quotes(count: int = 3600):
    return [(START + s * 1000, 39_999.9, 5.0, 40_000.1, 5.0) for s in range(count)]


def _depth(count: int = 3600):
    return [
        (START + s * 1000, [(39_999.9, 5.0)], [(40_000.1, 5.0)]) for s in range(count)
    ]


# --------------------------------------------------------------------------- resolution


def test_a_run_gets_the_tier_it_asked_for_when_the_data_supports_it(tmp_path: Path) -> None:
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes(), depth=_depth())
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_WALK
    )
    assert resolution.tier is FillTier.BOOK_WALK
    assert not resolution.degraded
    assert resolution.flags == ()
    assert resolution.reason is None


def test_asking_for_more_than_the_lake_holds_degrades_and_names_what_was_lost(
    tmp_path: Path,
) -> None:
    """The badge has to say more than "BOOK_TICKER".

    A user who asked for `BOOK_WALK` and got `BOOK_TICKER` needs to know two things: that it
    happened, and what stopped working. "Lost: book" is the sentence that turns a tier name
    into a fact about their strategy -- `ctx.book()` will raise for the whole run.
    """
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes())
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_WALK
    )
    assert resolution.tier is FillTier.BOOK_TICKER
    assert resolution.available is FillTier.BOOK_TICKER
    assert resolution.degraded
    assert "TIER_DEGRADED" in resolution.flags
    assert "Lost: book" in (resolution.reason or "")
    assert "collector started recording" in (resolution.reason or "")


def test_a_range_with_only_trades_falls_all_the_way_to_trade_only(tmp_path: Path) -> None:
    """And that costs limit orders, which the reason has to say."""
    root = _lake(tmp_path, ticks=_ticks())
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_WALK
    )
    assert resolution.tier is FillTier.TRADE_ONLY
    assert "limit" in (resolution.reason or "")
    assert "book" in (resolution.reason or "")


def test_a_range_with_no_tick_data_at_all_falls_to_bar_close(tmp_path: Path) -> None:
    root = _lake(tmp_path)
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_TICKER
    )
    assert resolution.tier is FillTier.BAR_CLOSE
    assert "LOW_FIDELITY" in resolution.flags
    assert "TIER_DEGRADED" in resolution.flags


def test_asking_for_less_than_the_data_supports_is_a_choice_not_a_degradation(
    tmp_path: Path,
) -> None:
    """Spec 4.2 calls `BAR_CLOSE` "explicit opt-in only" -- it is a legitimate request.

    Collapsing this into `TIER_DEGRADED` would make a deliberate fast sweep look like a data
    problem, and after a few of those the degradation badge stops meaning anything. So it
    gets its own flag and its own sentence.
    """
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes(), depth=_depth())
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BAR_CLOSE
    )
    assert resolution.tier is FillTier.BAR_CLOSE
    assert not resolution.degraded
    assert "TIER_BELOW_DATA" in resolution.flags
    assert "TIER_DEGRADED" not in resolution.flags
    assert "could have supported BOOK_WALK" in (resolution.reason or "")


def test_a_range_the_lake_does_not_cover_resolves_rather_than_raising(
    tmp_path: Path,
) -> None:
    """A presence check must not discard a run before it has read a bar.

    `derive_fill_model_tier` demands a published file in *every* partition, which is right
    for a manifest and wrong for a decision the run has to make in order to start. A range
    extending past the end of the lake is a state the engine already handles; if the lake
    genuinely holds nothing, `feed.load_bars` says so where the message can name the dataset.
    """
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes())
    far_future = END + 400 * 86_400_000
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, far_future, requested=FillTier.BOOK_TICKER
    )
    assert resolution.tier is FillTier.BAR_CLOSE
    assert "COVERAGE_INCOMPLETE" in resolution.flags
    assert "TIER_DEGRADED" in resolution.flags


def _gap(dataset: str, start_ms: int, end_ms: int, symbol: str = "BTCUSDT") -> dict:
    return {
        "dataset": dataset,
        "symbol": symbol,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "explained": False,
    }


def test_a_gap_inside_the_range_demotes_the_tier(tmp_path: Path) -> None:
    """Presence is judged per partition, so one part-file must not win a day's fidelity.

    Without the gap report a day the collector was down for nine hours of would resolve to
    `BOOK_WALK` on the strength of a single file, and the run would walk a book that stops
    moving mid-afternoon. Twenty minutes of a one-hour range is a third of it, well over
    `TICK_GAP_TOLERANCE`, so this stays a demotion after that budget was introduced.
    """
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes(), depth=_depth())
    gap = _gap("depth20", START + 10 * MS_PER_MINUTE, START + 30 * MS_PER_MINUTE)
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_WALK, gaps=[gap]
    )
    assert resolution.tier is FillTier.BOOK_TICKER
    assert resolution.degraded


# --------------------------------------------------------------- the tolerance and its cost


def test_a_gap_far_under_the_tolerance_keeps_the_tier_and_says_it_did(
    tmp_path: Path,
) -> None:
    """The bug this budget exists to fix, in miniature.

    Runs 23 and 24 asked for `TRADE_ONLY` over a year of BTCUSDT and executed at
    `BAR_CLOSE`, because on 2025-08-29 Binance halted for nineteen minutes -- eighteen
    consecutive zero-volume kline bars, then a 9-14k trades/minute burst on resumption --
    and one aggregated trade in that window never reached the archive. Nineteen minutes is
    0.004% of a year. The old rule struck `aggTrades` out on the boolean and re-priced
    **every** fill across the whole year at the lowest fidelity in the platform to avoid
    mispricing that sliver, which is worse accounting than the thing it was avoiding. Both
    runs then died outright, because the strategy declared `requires['datasets']`.

    Keeping the tier is only defensible if the run says what it kept it over, so the flag
    and the reason are asserted here rather than left to the results page.
    """
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes(), depth=_depth())
    # 20 s of a 60 min range: 0.56%, inside the 1% budget.
    gap = _gap("depth20", START + 10 * MS_PER_MINUTE, START + 10 * MS_PER_MINUTE + 20_000)
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_WALK, gaps=[gap]
    )
    assert resolution.tier is FillTier.BOOK_WALK
    assert not resolution.degraded
    assert "TICK_GAPS_TOLERATED" in resolution.flags
    assert resolution.reason is not None
    assert "depth20" in resolution.reason
    assert "20s" in resolution.reason


def test_the_tolerance_is_a_fraction_of_the_range_not_a_fixed_duration(
    tmp_path: Path,
) -> None:
    """The same hole is noise in a long range and material in a short one.

    Asserted as a pair over one lake so the two cannot drift apart: a 60 s gap is 1.7% of an
    hour and demotes, and nothing about the gap changed except the range measured against.
    """
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes(), depth=_depth())
    gap = _gap("depth20", START + 10 * MS_PER_MINUTE, START + 11 * MS_PER_MINUTE)

    hour = resolve_tier(root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_WALK, gaps=[gap])
    assert hour.tier is FillTier.BOOK_TICKER, "1.7% of the range must not be tolerated"
    assert "TICK_GAPS_TOLERATED" not in hour.flags


def test_a_tolerated_gap_in_a_dataset_the_tier_never_read_is_not_reported(
    tmp_path: Path,
) -> None:
    """A `TRADE_ONLY` run never opens `bookTicker`, so a hole in it is not that run's news.

    Warning about data a run did not use is how a badge becomes noise, and a badge that is
    usually noise is one nobody reads on the day it matters.
    """
    root = _lake(tmp_path, ticks=_ticks())
    gap = _gap("bookTicker", START + 10 * MS_PER_MINUTE, START + 10 * MS_PER_MINUTE + 20_000)
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.TRADE_ONLY, gaps=[gap]
    )
    assert resolution.tier is FillTier.TRADE_ONLY
    assert "TICK_GAPS_TOLERATED" not in resolution.flags
    assert resolution.reason is None


def test_an_explained_gap_is_not_counted_against_the_budget(tmp_path: Path) -> None:
    """A recorded clean shutdown is missing data with a known cause, and the tier has never
    depended on the collector's uptime log. The budget must not quietly change that."""
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes(), depth=_depth())
    gap = _gap("depth20", START + 10 * MS_PER_MINUTE, START + 30 * MS_PER_MINUTE)
    gap["explained"] = True
    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_WALK, gaps=[gap]
    )
    assert resolution.tier is FillTier.BOOK_WALK
    assert resolution.flags == ()


def test_a_range_with_depth_but_no_bars_is_not_a_book_walk_range(tmp_path: Path) -> None:
    """Found on the real lake: the collector runs ahead of the bulk kline archive.

    `derive_fill_model_tier` answers "what fill fidelity is supportable" from the *tick*
    datasets alone, so on 2026-08-02 -- a day the collector has depth for and the archive has
    not published klines for -- it returned `BOOK_WALK`. Every tier needs bars to drive the
    strategy and marks to price risk, so that range is not a `BOOK_WALK` range with a detail
    wrong; it is a range no run can execute over. The `/tiers` preview would otherwise
    advertise the best tier in the platform for a range that fails on its first query.
    """
    root = tmp_path
    build_lake(
        root / "market",
        start_ms=START,
        minutes=60,
        trade_path=flat_path(40_000.0),
        ticks=_ticks(),
        quotes=_quotes(),
        depth=_depth(),
    )
    # A day beyond every bar in the fixture, but inside the tick partitions' own day.
    later = START + 200 * 86_400_000
    resolution = resolve_tier(
        root, ["BTCUSDT"], later, later + 86_400_000, requested=FillTier.BOOK_WALK
    )
    assert resolution.tier is FillTier.BAR_CLOSE
    assert "COVERAGE_INCOMPLETE" in resolution.flags
    assert "klines" in (resolution.reason or "")
    assert "do not span this range" in (resolution.reason or "")


def test_a_range_half_outside_the_bars_is_refused_rather_than_run_on_half_the_data(
    tmp_path: Path,
) -> None:
    """The month-granular version of the guard let this through, and it is the worse case.

    Partition presence answers "is there a file somewhere in this month". A range whose
    second half has no bars therefore passed, the run completed on half its data, and nothing
    flagged it -- `load_bars` only raises when it finds *nothing*. Row bounds are exact.
    """
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes(), depth=_depth())
    # The fixture writes 60 minutes; ask for 120, so the second hour has no bars at all.
    resolution = resolve_tier(
        root,
        ["BTCUSDT"],
        START,
        START + 120 * MS_PER_MINUTE,
        requested=FillTier.BOOK_WALK,
    )
    assert resolution.tier is FillTier.BAR_CLOSE
    assert "COVERAGE_INCOMPLETE" in resolution.flags


def test_a_range_inside_the_bars_is_not_refused(tmp_path: Path) -> None:
    """The control. An exact-fit range and a strict subset both have to pass."""
    root = _lake(tmp_path, ticks=_ticks(), quotes=_quotes(), depth=_depth())
    for lo, hi in ((START, END), (START + 5 * MS_PER_MINUTE, END - 5 * MS_PER_MINUTE)):
        resolution = resolve_tier(
            root, ["BTCUSDT"], lo, hi, requested=FillTier.BOOK_WALK
        )
        assert resolution.tier is FillTier.BOOK_WALK, (lo, hi)
        assert "COVERAGE_INCOMPLETE" not in resolution.flags


def test_a_range_with_bars_but_no_marks_is_refused_too(tmp_path: Path) -> None:
    """Spec 3.4 forbids deriving a mark from trades, so a run without marks cannot price risk.

    `feed.load_marks` says so at load time; saying it here as well is what lets the dialog
    warn before the worker starts.
    """
    from perplab.data.schemas import SCHEMAS
    from perplab.data.writer import ParquetBufferedWriter
    from tests.engine_lake import write_klines

    root = tmp_path
    write_klines(root / "market", "BTCUSDT", START, 60, flat_path(40_000.0))
    # An empty markPriceKlines dataset: the directory exists, no partition covers the range.
    ParquetBufferedWriter(
        root / "market",
        "markPriceKlines",
        SCHEMAS["markPriceKlines"],
        symbol="BTCUSDT",
        max_rows=10**9,
    ).flush()

    resolution = resolve_tier(
        root, ["BTCUSDT"], START, END, requested=FillTier.BOOK_TICKER
    )
    assert resolution.tier is FillTier.BAR_CLOSE
    assert "markPriceKlines" in (resolution.reason or "")


def test_an_unknown_tier_name_is_refused_rather_than_defaulted() -> None:
    """Defaulting would re-price every fill while leaving the run's identity unchanged."""
    with pytest.raises(ValueError, match="unknown fill tier"):
        tier_from_name("BOOK_WALKING")


# ------------------------------------------------------------------------- capabilities


def test_the_published_capability_matrix_matches_what_the_engine_enforces() -> None:
    """Two representations of one fact, so they are asserted equal rather than trusted.

    `TIER_CAPABILITIES` is served to the frontend so the New Backtest dialog can say "limit
    orders unavailable" instead of "BOOK_TICKER". The engine enforces the same matrix through
    `LIMIT_TIERS` and `TRIGGER_TIERS`. A UI that advertised a capability the engine refuses
    would turn a clear refusal into a run that fails on its first order.
    """
    for name, capabilities in TIER_CAPABILITIES.items():
        tier = tier_from_name(name)
        assert capabilities["limit"] == (tier in LIMIT_TIERS), name
        assert capabilities["trigger"] == (tier in TRIGGER_TIERS), name
        assert capabilities["book"] == (tier is FillTier.BOOK_WALK), name
        assert capabilities["market"] is True, name


def test_every_tier_has_a_capability_entry() -> None:
    assert set(TIER_CAPABILITIES) == {t.name for t in FillTier}


def test_capabilities_are_monotonic_in_fidelity() -> None:
    """A higher tier can never do less than a lower one.

    Not a tautology about this table -- it is the property that makes `min(requested,
    available)` a sound way to resolve a tier at all. If some capability existed only at a
    middle tier, resolving downward could lose it while resolving upward gained something
    else, and "degraded" would stop being a single ordered idea.
    """
    ordered = sorted(FillTier, key=lambda t: t.value)
    for lower, higher in zip(ordered, ordered[1:]):
        low = TIER_CAPABILITIES[lower.name]
        high = TIER_CAPABILITIES[higher.name]
        for capability, present in low.items():
            if present:
                assert high[capability], f"{higher.name} lost {capability}"
