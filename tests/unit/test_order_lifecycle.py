"""Amendments, the `on_cancel` hook, and platform-enforced flattening.

Three capabilities that a strategy using resting orders cannot do without, and each of them
has an obvious wrong implementation that a test asserting only "it happened" would accept:

- **Amend** that keeps queue position through a reprice. Queue position is the most valuable
  thing a maker strategy owns, and a model that hands it back for free makes every quoting
  strategy look better than it is.
- **`on_cancel`** that fires only for cancels the strategy asked for. A post-only order that
  silently fails to enter is spec 6.5's named hazard, and a hook that stays quiet for it
  leaves a requoting strategy waiting forever.
- **Auto-flatten** whose hold clock restarts on every increment. A strategy that scales into
  a winner would then never reach any deadline, which is the opposite of what a
  maximum-hold guarantee is for.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.money import parse_money
from perplab.core.risk import RiskLimits
from perplab.engine.backtest import (
    AutoFlatten,
    BacktestConfig,
    BacktestEngine,
    UnsupportedOrder,
)
from perplab.engine.latency import FixedLatency
from perplab.strategy.base import Strategy
from perplab.strategy.context import FillTier
from tests.engine_lake import MS_PER_MINUTE, build_lake, flat_path
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000
BAR1 = START + MS_PER_MINUTE - 1
BAR2 = START + 2 * MS_PER_MINUTE - 1
BAR3 = START + 3 * MS_PER_MINUTE - 1
LATENCY = 10


def money(text: str) -> Decimal:
    return parse_money(text)


class Base(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines", "bookTicker"],
    }


def build(
    root: Path,
    strategy: Strategy,
    *,
    tier: FillTier = FillTier.BOOK_TICKER,
    minutes: int = 6,
    auto_flatten: AutoFlatten | None = None,
    latency: FixedLatency | None = None,
    funding=(),
) -> BacktestEngine:
    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
        seed=1,
        opening_balance=money("1000000"),
        leverage=10,
        latency=latency or FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=tier,
        risk=RiskLimits.unlimited(),
        auto_flatten=auto_flatten or AutoFlatten(),
    )
    return BacktestEngine(
        root=root,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
    )


def run(root: Path, strategy: Strategy, **kwargs):
    return build(root, strategy, **kwargs).run()


def kinds(result, kind: str) -> list[dict]:
    return [dict(e.payload) for e in result.events if e.kind == kind]


def market_lake(root: Path, *, minutes: int = 6, bid=39_999.9, ask=40_000.1, size=10.0):
    seconds = minutes * 60
    build_lake(
        root,
        start_ms=START,
        minutes=minutes,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(seconds)],
        quotes=[(START + s * 1000, bid, size, ask, size) for s in range(seconds)],
    )


# ================================================================== amend: queue priority


def test_shrinking_an_order_keeps_its_place_in_the_queue(tmp_path: Path) -> None:
    """A strict decrease in size does not cost priority, which is the exchange's rule.

    The order rests at 39 999.9 behind 10 BTC of published size, so `queue_ahead` is 10.
    Amending 1.0 down to 0.5 leaves it at 10 -- the orders ahead did not move because we
    asked for less.
    """
    market_lake(tmp_path)

    class ShrinkOnce(Base):
        def on_start(self, ctx) -> None:
            self.oid = None
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            if bar.close_time == BAR1:
                self.oid = ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("39999.9"), tif="GTC"
                )
            elif bar.close_time == BAR2 and not self.done:
                self.done = True
                ctx.modify(self.oid, qty=money("0.5"))

    result = run(tmp_path, ShrinkOnce())
    modified = kinds(result, "MODIFIED")
    assert len(modified) == 1
    assert modified[0]["queue_priority"] == "kept"
    assert modified[0]["remaining"] == "0.50000000"


def test_repricing_an_order_sends_it_to_the_back(tmp_path: Path) -> None:
    """Moving the price is cancel-and-replace as far as the matching engine cares.

    This is the whole reason an amend is modelled separately from a cancel plus a new
    order: a strategy that chases the market with its quote must not keep its queue
    position while doing it. `queue_ahead` is reset to unobserved, so the next book
    observation re-measures it against everything now resting at the new level.
    """
    market_lake(tmp_path)

    class RepriceOnce(Base):
        def on_start(self, ctx) -> None:
            self.oid = None
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            if bar.close_time == BAR1:
                self.oid = ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("39999.0"), tif="GTC"
                )
            elif bar.close_time == BAR2 and not self.done:
                self.done = True
                ctx.modify(self.oid, price=money("39999.5"))

    result = run(tmp_path, RepriceOnce())
    modified = kinds(result, "MODIFIED")
    assert len(modified) == 1
    assert modified[0]["queue_priority"] == "lost"
    assert modified[0]["price"] == "39999.50000000"


def test_growing_an_order_also_sends_it_to_the_back(tmp_path: Path) -> None:
    """Same price, more size: the increase queues behind everyone already there."""
    market_lake(tmp_path)

    class GrowOnce(Base):
        def on_start(self, ctx) -> None:
            self.oid = None
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            if bar.close_time == BAR1:
                self.oid = ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("39999.9"), tif="GTC"
                )
            elif bar.close_time == BAR2 and not self.done:
                self.done = True
                ctx.modify(self.oid, qty=money("2"))

    result = run(tmp_path, GrowOnce())
    assert kinds(result, "MODIFIED")[0]["queue_priority"] == "lost"


def test_an_amendment_takes_latency_like_any_other_instruction(tmp_path: Path) -> None:
    """Spec 6.3's R19, applied to amendments.

    The `MODIFY` event is stamped when the strategy decided, and carries the millisecond
    the amendment will land -- one cancel-latency later. A model that applied the change
    instantly would give a strategy a quote that is never stale, which no market maker has.
    """
    market_lake(tmp_path)

    class RepriceOnce(Base):
        def on_start(self, ctx) -> None:
            self.oid = None
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            if bar.close_time == BAR1:
                self.oid = ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("39999.0"), tif="GTC"
                )
            elif bar.close_time == BAR2 and not self.done:
                self.done = True
                ctx.modify(self.oid, price=money("39998.0"))

    result = run(tmp_path, RepriceOnce())
    sent = [e for e in result.events if e.kind == "MODIFY"][0]
    applied = [e for e in result.events if e.kind == "MODIFIED"][0]
    assert sent.ts_ms == BAR2
    assert sent.payload["arrival_ms"] == BAR2 + LATENCY
    assert applied.ts_ms == BAR2 + LATENCY


def test_only_limit_orders_can_be_amended(tmp_path: Path) -> None:
    """The exchange has no amend endpoint for a stop, and neither does this."""
    market_lake(tmp_path)

    class AmendAStop(Base):
        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == BAR1:
                ctx.buy(qty=money("1"))
            elif ctx.warm and bar.close_time == BAR2:
                oid = ctx.stop_loss(stop_price=money("39000"), qty=money("1"))
                ctx.modify(oid, price=money("38000"))

    with pytest.raises(UnsupportedOrder, match="only LIMIT orders"):
        run(tmp_path, AmendAStop())


def test_amending_below_what_has_already_filled_is_refused(tmp_path: Path) -> None:
    """`qty` is the new *total*, so a total at or below the filled amount is meaningless."""
    market_lake(tmp_path)

    class Impossible(Base):
        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == BAR1:
                oid = ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("39999.9"), tif="GTC"
                )
                ctx.modify(oid, qty=money("0"))

    with pytest.raises(ValueError, match="positive quantity"):
        run(tmp_path, Impossible())


# ============================================================================ on_cancel


def test_on_cancel_fires_for_a_cancel_the_strategy_asked_for(tmp_path: Path) -> None:
    market_lake(tmp_path)

    class QuoteAndPull(Base):
        def on_start(self, ctx) -> None:
            self.oid = None
            self.ends: list = []

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            if bar.close_time == BAR1:
                self.oid = ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("39000"), tif="GTC"
                )
            elif bar.close_time == BAR2:
                ctx.cancel(self.oid)

        def on_cancel(self, ctx, event) -> None:
            self.ends.append(event)

    strategy = QuoteAndPull()
    run(tmp_path, strategy)
    assert len(strategy.ends) == 1
    end = strategy.ends[0]
    assert end.status == "CANCELLED"
    assert end.order_id == strategy.oid
    assert end.remaining_qty == money("1")
    assert end.partially_filled is False


def test_on_cancel_fires_when_a_post_only_order_silently_fails_to_enter(
    tmp_path: Path,
) -> None:
    """Spec 6.5's named hazard, and the reason `on_cancel` is not called `on_user_cancel`.

    A `GTX` buy priced above the ask would take liquidity, so the exchange refuses it
    outright. Nothing the strategy did caused it and nothing else reports it. A requoting
    strategy that only listened for its own cancels would wait forever for an order that
    never existed.
    """
    market_lake(tmp_path)

    class PostOnlyThatCrosses(Base):
        def on_start(self, ctx) -> None:
            self.ends: list = []

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == BAR1:
                ctx.buy(
                    qty=money("1"),
                    type="LIMIT",
                    price=money("40001"),  # above the 40 000.10 ask: would cross
                    tif="GTX",
                )

        def on_cancel(self, ctx, event) -> None:
            self.ends.append(event)

    strategy = PostOnlyThatCrosses()
    run(tmp_path, strategy)
    assert len(strategy.ends) == 1
    assert strategy.ends[0].status == "EXPIRED"
    assert "post-only" in strategy.ends[0].reason


def test_on_cancel_reports_the_part_that_did_fill(tmp_path: Path) -> None:
    """An `IOC` that fills half and expires the rest reports the half.

    A strategy that assumed a cancelled order traded nothing would misreport its own
    position, which is the kind of error that only shows up as an unexplained inventory.
    """
    market_lake(tmp_path, size=0.4)

    class PartialIOC(Base):
        def on_start(self, ctx) -> None:
            self.ends: list = []

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == BAR1:
                ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("40001"), tif="IOC"
                )

        def on_cancel(self, ctx, event) -> None:
            self.ends.append(event)

    strategy = PartialIOC()
    result = run(tmp_path, strategy)
    assert result.fills == 1
    assert len(strategy.ends) == 1
    end = strategy.ends[0]
    assert end.status == "EXPIRED"
    assert end.partially_filled is True
    assert end.filled_qty == money("0.4")
    assert end.remaining_qty == money("0.6")


def test_on_cancel_runs_between_events_not_inside_one(tmp_path: Path) -> None:
    """A hook that requotes from `on_cancel` must not recurse or corrupt the book.

    The strategy cancels and immediately places a replacement from inside the hook. Both
    the cancel and its replacement have to land, and the run has to finish.
    """
    market_lake(tmp_path)

    class Requoter(Base):
        def on_start(self, ctx) -> None:
            self.placed = 0
            self.ends = 0

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == BAR1:
                self.placed += 1
                self.oid = ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("39000"), tif="GTC"
                )
            elif ctx.warm and bar.close_time == BAR2:
                ctx.cancel(self.oid)

        def on_cancel(self, ctx, event) -> None:
            self.ends += 1
            if self.placed < 2:
                self.placed += 1
                ctx.buy(qty=money("1"), type="LIMIT", price=money("38999"), tif="GTC")

    strategy = Requoter()
    result = run(tmp_path, strategy)
    assert strategy.ends == 1
    assert strategy.placed == 2
    assert len(result.events) > 0


def test_a_strategy_without_on_cancel_costs_nothing(tmp_path: Path) -> None:
    """No hook, no queue. The notification is not built at all when nobody listens."""
    market_lake(tmp_path)

    class Silent(Base):
        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == BAR1:
                oid = ctx.buy(
                    qty=money("1"), type="LIMIT", price=money("39000"), tif="GTC"
                )
                ctx.cancel(oid)

    result = run(tmp_path, Silent())
    assert len(kinds(result, "CANCEL")) == 1


# ========================================================================= auto-flatten


def test_max_hold_closes_a_position_that_has_been_open_too_long(tmp_path: Path) -> None:
    """Three minutes of hold allowed; the position opened on the first warm bar.

    The exit is a real market order -- `AUTO_FLATTEN` names it, and it appears in the fill
    count -- so the run pays the spread and the taker fee for it, exactly as a strategy's
    own exit would.
    """
    market_lake(tmp_path, minutes=10)

    class HoldForever(Base):
        def on_start(self, ctx) -> None:
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done:
                self.done = True
                ctx.buy(qty=money("1"))

    result = run(
        tmp_path,
        HoldForever(),
        minutes=10,
        auto_flatten=AutoFlatten(max_hold_ms=3 * MS_PER_MINUTE),
    )
    flattens = kinds(result, "AUTO_FLATTEN")
    assert len(flattens) == 1
    assert flattens[0]["reason"].startswith("max_hold:")
    assert result.auto_flattens == 1
    assert "AUTO_FLATTENED" in result.flags
    closed = [t for t in result.trades if t.close_reason != "open"]
    assert len(closed) == 1


def test_the_hold_clock_runs_from_the_open_not_from_the_last_increment(
    tmp_path: Path,
) -> None:
    """A strategy that scales in every minute must still hit a three-minute deadline.

    Resetting the clock on each increment is the tempting implementation and it makes the
    guarantee vacuous: this strategy would hold forever under a limit that says three
    minutes.
    """
    market_lake(tmp_path, minutes=10)

    class ScaleIn(Base):
        def on_bar(self, ctx, bar) -> None:
            if ctx.warm:
                ctx.buy(qty=money("0.1"))

    result = run(
        tmp_path,
        ScaleIn(),
        minutes=10,
        auto_flatten=AutoFlatten(max_hold_ms=3 * MS_PER_MINUTE),
    )
    assert result.auto_flattens >= 1


def test_a_flip_restarts_the_hold_clock(tmp_path: Path) -> None:
    """Long to short in one fill closes one round-trip and opens another.

    The exposure the deadline bounds is the new one, so the clock restarts. Treating a flip
    as a continuation would flatten a position seconds after it opened.
    """
    market_lake(tmp_path, minutes=12)

    class FlipOnce(Base):
        def on_start(self, ctx) -> None:
            self.stage = 0

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            self.stage += 1
            if self.stage == 1:
                ctx.buy(qty=money("1"))
            elif self.stage == 3:
                ctx.sell(qty=money("2"))  # long 1 -> short 1

    result = run(
        tmp_path,
        FlipOnce(),
        minutes=12,
        auto_flatten=AutoFlatten(max_hold_ms=5 * MS_PER_MINUTE),
    )
    flattens = kinds(result, "AUTO_FLATTEN")
    assert len(flattens) == 1
    flatten_ms = [e.ts_ms for e in result.events if e.kind == "AUTO_FLATTEN"][0]
    # The flip happened on warm bar 3. Five minutes from *there*, not from bar 1.
    assert flatten_ms >= START + 7 * MS_PER_MINUTE


def test_flatten_before_funding_uses_the_schedule_the_run_loaded(tmp_path: Path) -> None:
    """Settlements are read from the funding series, not from a hard-coded 8-hour grid.

    Real intervals vary by symbol and have changed historically (spec 3.5, R17), so a
    platform guarantee built on the nominal grid would flatten against a timetable the data
    does not have -- and would do it silently.
    """
    settlement = START + 8 * MS_PER_MINUTE
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=12,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(720)],
        quotes=[
            (START + s * 1000, 39_999.9, 10.0, 40_000.1, 10.0) for s in range(720)
        ],
        funding=[(settlement, 0.0001)],
    )

    class HoldThrough(Base):
        def on_start(self, ctx) -> None:
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done:
                self.done = True
                ctx.buy(qty=money("1"))

    result = run(
        tmp_path,
        HoldThrough(),
        minutes=12,
        auto_flatten=AutoFlatten(before_funding_ms=2 * MS_PER_MINUTE),
    )
    flattens = [e for e in result.events if e.kind == "AUTO_FLATTEN"]
    assert len(flattens) == 1
    assert flattens[0].payload["reason"] == f"before_funding:{settlement}"
    # Flat before the settlement, so nothing is paid.
    assert flattens[0].ts_ms <= settlement
    assert result.attribution.funding_pnl == Decimal(0)


def test_auto_flatten_is_off_unless_asked_for(tmp_path: Path) -> None:
    """A platform that flattens positions nobody asked it to is not reporting the strategy."""
    market_lake(tmp_path, minutes=10)

    class HoldForever(Base):
        def on_start(self, ctx) -> None:
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done:
                self.done = True
                ctx.buy(qty=money("1"))

    result = run(tmp_path, HoldForever(), minutes=10)
    assert result.auto_flattens == 0
    assert kinds(result, "AUTO_FLATTEN") == []
    assert "AUTO_FLATTENED" not in result.flags


def test_only_one_flatten_is_sent_while_the_first_is_in_flight(tmp_path: Path) -> None:
    """The deadline is still past on the next mark; a second exit must not be queued.

    **The latency has to exceed the mark cadence for this to test anything.** At the usual
    10 ms the exit fills long before the next mark, so by then there is no position left to
    flatten and one order is sent whether or not the latch exists -- the fixture cannot tell.
    At 90 s the exit is genuinely still in flight when the next mark arrives, which is the
    only state in which a second one could be queued.

    Two reduce-only exits for the same position would have the second clamped to zero and
    cancelled: harmless, and it reads in the log as the platform not knowing what it had
    already done.
    """
    market_lake(tmp_path, minutes=12)

    class HoldForever(Base):
        def on_start(self, ctx) -> None:
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done:
                self.done = True
                ctx.buy(qty=money("1"))

    result = run(
        tmp_path,
        HoldForever(),
        minutes=12,
        latency=FixedLatency(submit=90_000, cancel=90_000),
        auto_flatten=AutoFlatten(max_hold_ms=2 * MS_PER_MINUTE),
    )
    flattens = kinds(result, "AUTO_FLATTEN")
    assert len(flattens) == 1, f"one exit, not {len(flattens)}"


def test_auto_flatten_rejects_a_non_positive_deadline() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        AutoFlatten(max_hold_ms=0)
    with pytest.raises(ValueError, match="must be positive"):
        AutoFlatten(before_funding_ms=-1)


def test_on_cancel_is_not_fired_for_the_platforms_own_orders(tmp_path: Path) -> None:
    """A strategy is told about its own orders, and only its own.

    An auto-flatten and a halt's forced exit are issued by the engine. Reporting their
    cancellation through `on_cancel` would tell the strategy about a decision it did not
    make -- and a strategy that requotes from `on_cancel`, which is what the hook is for,
    would requote against one.
    """
    market_lake(tmp_path, minutes=10)

    class Listener(Base):
        def on_start(self, ctx) -> None:
            self.done = False
            self.ends: list = []

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done:
                self.done = True
                ctx.buy(qty=money("1"))

        def on_cancel(self, ctx, event) -> None:
            self.ends.append(event)

    strategy = Listener()
    result = run(
        tmp_path,
        strategy,
        minutes=10,
        auto_flatten=AutoFlatten(max_hold_ms=3 * MS_PER_MINUTE),
    )
    assert result.auto_flattens == 1, "the platform did close the position"
    assert strategy.ends == [], "and said nothing to the strategy about it"


def test_a_flatten_that_fills_stops_being_a_platform_order(tmp_path: Path) -> None:
    """The platform forgets its own exit once the exit has done its job.

    Every deadline that fires records the order it sent -- in `_flatten_orders`, so a
    failure can unlatch the symbol, and in `_platform_orders`, so its refusal is not
    reported to the strategy as one of theirs. A flatten that *works* leaves the book
    through the fill path rather than through `_remove` or `_reject`, so `_release_flatten`
    -- the only other place that forgets one -- never sees it, and dropping the entries
    there is easy to leave out: over the few hours a backtest usually covers nothing reads
    them again and every assertion still passes.

    It is a 48-hour session with a before-funding deadline that pays for it. Three
    deadlines fire in this run and all three exits fill; the tables are empty at the end,
    not three deep. A run with three settlements an hour is three thousand entries by
    Sunday, each of them still answering "yes, the platform sent that" about an order that
    closed a day ago.
    """
    market_lake(tmp_path, minutes=14)

    class ReEnter(Base):
        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and ctx.position("BTCUSDT").qty == 0:
                ctx.buy(qty=money("1"))

    engine = build(
        tmp_path,
        ReEnter(),
        minutes=14,
        auto_flatten=AutoFlatten(max_hold_ms=2 * MS_PER_MINUTE),
    )
    result = engine.run()

    flatten_ids = [f["order_id"] for f in kinds(result, "AUTO_FLATTEN")]
    filled_ids = {
        f["order_id"] for f in kinds(result, "FILL") if f["remaining"] == "0.00000000"
    }
    assert len(flatten_ids) == 3
    assert set(flatten_ids) <= filled_ids, "every exit filled in full"
    assert engine._flatten_orders == {}, "a filled exit is still latched to its symbol"
    assert engine._platform_orders == set(), "a closed exit is still a live platform order"
