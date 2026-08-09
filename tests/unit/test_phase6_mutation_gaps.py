"""Tests that exist because a deliberate defect survived the suite.

Phase 6's first mutation pass broke the code forty-eight ways and the suite noticed
thirty-one. The seventeen survivors are not seventeen unrelated oversights -- they are one
pattern, the same one Phase 4 and Phase 5 found, and it is worth naming because it is
invisible from inside a green run:

**A test asserted the right thing about a fixture that could not tell two answers apart.**

- A flat 40 000 tape cannot distinguish "the mark" from "the far touch", so a limit priced
  at the wrong one survived. The fixtures here quote a spread.
- A test that asserts the *label* in an event log does not test the *behaviour* the label
  describes: `queue_priority: "lost"` is computed from the same expression as the reset, so
  a mutation that stopped resetting the queue kept the label and passed. The test here
  measures the fill instead.
- A drawdown scored on a series whose intrabar band is empty cannot tell a peak taken from
  the trough from one taken from the crest. These fixtures give the band a width.
- A smoke run whose synthetic ladder happens to sit at the mark cannot tell a fill priced
  by the model from one priced at the mark. This one prices against a wide book.

Every test below is written to make one specific wrong implementation produce a different
number, and the mutation tag it closes is named in its docstring.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.money import parse_money
from perplab.core.risk import (
    MS_PER_DAY,
    RiskEngine,
    RiskLimits,
    WorkingExposure,
)
from perplab.engine.backtest import AutoFlatten, BacktestConfig, BacktestEngine
from perplab.engine.latency import FixedLatency
from perplab.strategy.base import Strategy
from perplab.strategy.context import FillTier
from tests.engine_lake import MS_PER_MINUTE, build_lake, flat_path, ramp_path
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000
BAR1 = START + MS_PER_MINUTE - 1
BAR2 = START + 2 * MS_PER_MINUTE - 1
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


def limits(**overrides) -> RiskLimits:
    """`unlimited()` plus exactly the fields a test cares about.

    Building from `unlimited` rather than from the defaults means a test that means to
    exercise one limit cannot be halted by another one it never mentioned -- which is how
    two fixtures in the first pass ended up testing liquidation instead.
    """
    return RiskLimits(
        max_position_notional=overrides.get("max_position_notional"),
        max_leverage=overrides.get("max_leverage"),
        max_daily_loss_pct=overrides.get("max_daily_loss_pct"),
        max_drawdown_pct=overrides.get("max_drawdown_pct"),
        max_open_orders=overrides.get("max_open_orders"),
        max_orders_per_minute=overrides.get("max_orders_per_minute"),
        max_consecutive_losses=overrides.get("max_consecutive_losses"),
        halt_on_liquidation=overrides.get("halt_on_liquidation", False),
        min_equity_pct=overrides.get("min_equity_pct"),
        max_consecutive_rejections=overrides.get("max_consecutive_rejections"),
    )


def engine_for(root: Path, strategy, *, risk=None, tier=FillTier.BOOK_TICKER, minutes=6,
               balance="1000000", leverage=10, auto_flatten=None):
    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
        seed=1,
        opening_balance=money(balance),
        leverage=leverage,
        latency=FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=tier,
        risk=risk or RiskLimits.unlimited(),
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


def run(root: Path, strategy, **kwargs):
    return engine_for(root, strategy, **kwargs).run()


def kinds(result, kind: str) -> list[dict]:
    return [dict(e.payload) for e in result.events if e.kind == kind]


def wide_book(root: Path, *, minutes: int = 6, bid=39_000.0, ask=41_000.0, size=100.0):
    """A book whose touches are a long way from the mark.

    Deliberately absurd as a market and exactly right as a fixture: it is the only way a
    test can tell a risk limit priced at the mark from one priced at the far touch. A flat
    40 000 book makes the two numerically identical, which is why the mutation survived.
    """
    seconds = minutes * 60
    build_lake(
        root,
        start_ms=START,
        minutes=minutes,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(seconds)],
        quotes=[(START + s * 1000, bid, size, ask, size) for s in range(seconds)],
    )


# ================================================== M07: the rate window's exact boundary


def test_a_submission_exactly_sixty_seconds_later_does_not_count_the_first() -> None:
    """M07. The window is `(ts - 60_000, ts]`, and the boundary decides a steady-state rate.

    Two submissions under a ceiling of two, then a third exactly 60 000 ms after the first.
    Inclusive, the first has aged out and the third is admitted. Exclusive -- `< cutoff`
    rather than `<= cutoff` -- it is still in the window and the third is refused.
    """
    engine = RiskEngine(limits=limits(max_orders_per_minute=2), starting_equity=money("100000"))
    common = dict(
        symbol="BTCUSDT",
        side="BUY",
        qty=money("0.001"),
        price=money("40000"),
        reduce_only=False,
        position_qty=money("0"),
        working=WorkingExposure.zero(),
        equity=money("100000"),
        open_orders=0,
    )
    assert engine.check_order(ts_ms=1_000, **common) is None
    assert engine.check_order(ts_ms=2_000, **common) is None
    assert engine.check_order(ts_ms=3_000, **common) is not None, "the window is full"
    # Exactly 60 000 ms after the first: it has aged out, so there is room again.
    assert engine.check_order(ts_ms=61_000, **common) is None


# ========================================= M14/M15/M17: the drawdown peak and the band


def test_the_drawdown_limit_scores_against_a_peak_that_rose(tmp_path: Path) -> None:
    """M14, M15. A run that gained before it fell.

    Equity rises to 120 000 from a 100 000 start, then falls to 105 000. That is a 12.5%
    drawdown from the peak and a *gain* of 5% on the run's start, so a limit measured from
    the start never fires and a peak that never rises never fires either. Only the correct
    one breaches a 10% ceiling.
    """
    engine = RiskEngine(
        limits=limits(max_drawdown_pct=Decimal("0.10")), starting_equity=money("100000")
    )
    assert engine.observe_equity(1_000, money("100000")) is None
    assert engine.observe_equity(2_000, money("120000")) is None
    assert engine.peak_equity == money("120000")
    breach = engine.observe_equity(3_000, money("105000"))
    assert breach is not None, "12.5% below the peak, and 5% above the start"
    assert breach.observed == "0.12500000"


def test_the_risk_layer_sees_the_intrabar_trough_not_the_close(tmp_path: Path) -> None:
    """M17. A mark bar whose low is far below its close.

    The bar traverses 40 000 -> 30 000 -> 40 000 with a 1 BTC long on 100 000 of equity.
    Scored on the close, equity never moves and no limit fires. Scored on the trough it is
    a 10% fall, which breaches an 8% ceiling. Spec 8.2's own argument: a close-only series
    understates drawdown, and a limit that reads one is a limit a strategy can trade
    through.
    """
    marks = _dip_path(dip_at=3, low=30_000.0, base=40_000.0)
    build_lake(tmp_path, start_ms=START, minutes=8, trade_path=flat_path(40_000.0), mark_path=marks)

    class BuyOnce(Strategy):
        requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 0, "datasets": ["klines"]}

        def on_start(self, ctx) -> None:
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done:
                self.done = True
                ctx.buy(qty=money("1"))

    result = run(
        tmp_path,
        BuyOnce(),
        tier=FillTier.BAR_CLOSE,
        minutes=8,
        balance="100000",
        leverage=1,
        risk=limits(max_drawdown_pct=Decimal("0.08")),
    )
    assert result.halt_reason is not None
    assert result.halt_reason.limit == "max_drawdown"


def _dip_path(*, dip_at: int, low: float, base: float):
    """A mark series that is flat except for one bar with a deep low and a flat close.

    The whole fixture: a bar whose *close* says nothing happened and whose *low* says the
    account was 10 000 down inside it. A series without that shape cannot tell a drawdown
    scored on the close from one scored on the band.
    """
    from tests.engine_lake import Ohlc, scaled

    def path(index: int) -> Ohlc:
        if index == dip_at:
            return Ohlc(scaled(base), scaled(base), scaled(low), scaled(base))
        return Ohlc(scaled(base), scaled(base), scaled(base), scaled(base))

    return path


# ============================================= M16: the equity floor is inclusive


def test_equity_exactly_at_the_floor_breaches() -> None:
    """M16. `<=`, not `<`. Half of 10 000 is 5 000, and 5 000 is not above the floor."""
    engine = RiskEngine(
        limits=limits(min_equity_pct=Decimal("0.50")), starting_equity=money("10000")
    )
    assert engine.observe_equity(1_000, money("5000.00000001")) is None
    breach = engine.observe_equity(2_000, money("5000"))
    assert breach is not None
    assert breach.limit == "min_equity"


# ================================== M18/M19: what extends and what breaks a losing streak


def test_a_flat_trade_neither_extends_nor_breaks_the_streak() -> None:
    """M18. loss, flat, loss under a limit of 3 must not halt -- the streak is 2.

    Counting the flat one makes it 3 and halts. The previous fixture used a limit of 2 and
    could not tell the two apart, because both readings reach 2.
    """
    engine = RiskEngine(limits=limits(max_consecutive_losses=3), starting_equity=money("10000"))
    assert engine.observe_trade_closed(1_000, money("-10")) is None
    assert engine.observe_trade_closed(2_000, money("0")) is None
    assert engine.observe_trade_closed(3_000, money("-10")) is None, "two losses, not three"
    assert engine.observe_trade_closed(4_000, money("-10")) is not None


def test_a_winning_trade_breaks_the_streak() -> None:
    """M19. loss, loss, win, loss, loss under a limit of 3 must not halt."""
    engine = RiskEngine(limits=limits(max_consecutive_losses=3), starting_equity=money("10000"))
    for pnl in ("-10", "-10"):
        assert engine.observe_trade_closed(1_000, money(pnl)) is None
    assert engine.observe_trade_closed(2_000, money("5")) is None
    for pnl in ("-10", "-10"):
        assert engine.observe_trade_closed(3_000, money(pnl)) is None
    assert engine.observe_trade_closed(4_000, money("-10")) is not None


# ============================== M21: the rejection streak resets on a successful order


def test_a_successful_order_breaks_the_rejection_streak(tmp_path: Path) -> None:
    """M21. Two refusals, one order that lands, then two more refusals: no halt at 3.

    Without the reset the fourth refusal is the third in a row and the kill switch trips.
    The previous suite only ever rejected consecutively, so a counter that never reset
    behaved identically.
    """
    wide_book(tmp_path, minutes=10)

    class Alternating(Base):
        """Asks for an illegal size on odd bars and a legal one on even bars."""

        def on_start(self, ctx) -> None:
            self.n = 0

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            self.n += 1
            # 2 000 BTC breaches MARKET_LOT_SIZE maxQty 120; 0.01 is fine.
            ctx.buy(qty=money("2000") if self.n % 3 else money("0.01"))

    result = run(
        tmp_path,
        Alternating(),
        minutes=10,
        risk=limits(max_consecutive_rejections=3),
    )
    assert result.rejects >= 4, "several refusals, but never three in a row"
    assert result.halt_reason is None


# ============================ M32/M33: what a risk limit prices, and at what quantity


def test_a_size_limit_prices_at_the_mark_not_at_the_reference(tmp_path: Path) -> None:
    """M32. An **asymmetric** book, because a symmetric one cannot tell the two apart.

    The tape -- and therefore the mark -- is 40 000. The book is 39 500 / 41 900, so the
    reference price a fill is measured against, which at a book tier is the *mid*, is
    40 700. One BTC projects 40 000 at the mark and 40 700 at the reference, and a ceiling
    of 40 350 admits it on the correct reading and refuses it on the wrong one.

    Two coincidences had to be avoided, and the first attempt hit both. A *symmetric* book
    has a mid equal to the mark, so the two readings agree and the mutation survives a test
    written to kill it. And a book far enough from the mark to be obviously asymmetric --
    39 000 / 43 000 -- puts the ask outside `PERCENT_PRICE`'s 5% ceiling, so the fill is
    refused by a filter and the test measures nothing. The spread here is wide enough to
    separate the numbers and narrow enough to trade.
    """
    wide_book(tmp_path, bid=39_500.0, ask=41_900.0)

    class BuyOne(Base):
        def on_start(self, ctx) -> None:
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done:
                self.done = True
                ctx.buy(qty=money("1"))

    result = run(
        tmp_path,
        BuyOne(),
        risk=limits(max_position_notional=money("40350")),
    )
    assert result.risk_rejects == 0, "40 000 at the mark is inside a 40 350 ceiling"
    assert result.fills == 1
    # And the reference really was the other number, so the two agreeing is a fact about
    # the code rather than about the fixture.
    order = [e for e in result.events if e.kind == "ORDER"][0]
    assert money(order.payload["reference_price"]) == money("40700")


def test_a_size_limit_uses_the_quantised_quantity(tmp_path: Path) -> None:
    """M33. A request of 1.0009 against a 0.001 step quantises to 1.000.

    At a 40 000 mark that is 40 000 against a 40 020 ceiling: admitted. Unquantised it is
    40 036 and refused. The engine can only ever fill the quantised amount, so checking the
    request means refusing orders for exposure that cannot exist.
    """
    wide_book(tmp_path)

    class BuyOdd(Base):
        def on_start(self, ctx) -> None:
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done:
                self.done = True
                ctx.buy(qty=money("1.0009"))

    result = run(
        tmp_path,
        BuyOdd(),
        risk=limits(max_position_notional=money("40020")),
    )
    assert result.risk_rejects == 0
    assert result.fills == 1


# ================================ M34: an amendment's queue position, measured not labelled


def test_an_amended_order_actually_goes_to_the_back_of_the_queue(tmp_path: Path) -> None:
    """M34. The previous test asserted the *label*, which the mutation left intact.

    `queue_priority: "lost"` is derived from the same expression as the reset, so a mutation
    that stopped resetting `queue_ahead` still printed "lost" and passed. This measures the
    consequence instead.

    The order rests at the 39 999.9 bid behind 1 000 BTC of published size. Prints land at
    that level at 1 BTC a second, so by the time the amendment goes out about sixty of them
    have been consumed and `queue_ahead` has decremented to roughly 940. Growing the order
    forfeits priority, so the level is re-measured against everything resting *now* -- the
    full 1 000. An engine that kept the priority reports the decremented figure, and the two
    are far enough apart that no rounding can confuse them.
    """
    seconds = 6 * 60
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=6,
        trade_path=flat_path(40_000.0),
        # Prints at the bid, so a resting buy has its queue consumed from the front.
        ticks=[(START + s * 1000, 39_999.9, 1.0, True) for s in range(seconds)],
        quotes=[(START + s * 1000, 39_999.9, 1000.0, 40_000.1, 1000.0) for s in range(seconds)],
    )

    class GrowThenWait(Base):
        def on_start(self, ctx) -> None:
            self.oid = None
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            if bar.close_time == BAR1:
                # 0.01, not 0.001: 0.001 BTC at 40 000 is worth 40 against a 50
                # `MIN_NOTIONAL` and never reaches the book at all.
                self.oid = ctx.buy(
                    qty=money("0.01"), type="LIMIT", price=money("39999.9"), tif="GTC"
                )
            elif bar.close_time == BAR2 and not self.done:
                self.done = True
                ctx.modify(self.oid, qty=money("0.02"))

    result = run(tmp_path, GrowThenWait())
    assert kinds(result, "MODIFIED")[0]["queue_priority"] == "lost"
    working = kinds(result, "ORDER_WORKING")
    assert len(working) == 2, "the original placement, then the re-placement"
    first = money(working[0]["queue_ahead"])
    second = money(working[1]["queue_ahead"])
    assert first == money("1000")
    assert second == money("1000"), "re-measured, not carried over from the decremented queue"
    # And the queue really had decremented in between, so the two figures being equal is a
    # fact about the reset rather than about nothing having happened.
    order = result.events
    consumed = [e for e in order if e.kind == "FILL"]
    assert not consumed, "0.01 behind 1 000 BTC never reaches the front in six minutes"


# ============================== M41: a settlement stamped exactly now is already paid


def test_a_settlement_at_this_instant_is_not_the_next_one(tmp_path: Path) -> None:
    """M41. Funding settles at priority 1; this check runs at priority 2.

    So by the time `_next_funding_ms` is asked, a settlement stamped `ts_ms` has already
    been charged. Treating it as upcoming made the engine flatten against a payment it had
    already made -- and, with a single settlement in the range, flatten every bar after it.
    """
    settlement = START + 4 * MS_PER_MINUTE
    seconds = 10 * 60
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=10,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(seconds)],
        quotes=[(START + s * 1000, 39_999.9, 10.0, 40_000.1, 10.0) for s in range(seconds)],
        funding=[(settlement, 0.0001)],
    )

    class BuyAfterFunding(Base):
        """Opens *after* the only settlement, so no deadline can be pending."""

        def on_start(self, ctx) -> None:
            self.done = False

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.done and bar.close_time > settlement:
                self.done = True
                ctx.buy(qty=money("1"))

    result = run(
        tmp_path,
        BuyAfterFunding(),
        minutes=10,
        auto_flatten=AutoFlatten(before_funding_ms=2 * MS_PER_MINUTE),
    )
    assert kinds(result, "AUTO_FLATTEN") == [], "the only settlement is in the past"
    assert result.auto_flattens == 0


# ================================= M42: one flatten in flight, and a refused one retried


def test_a_refused_flatten_unlatches_and_is_not_counted(tmp_path: Path) -> None:
    """M42, and the review finding behind it.

    `MARKET_LOT_SIZE` caps a single market order at 120 BTC. A position of 200 is legal to
    *accumulate* -- two orders of 100 -- and illegal to exit in one order, so the platform's
    own flatten is refused at arrival every time.

    That combination is the whole fixture. The latch must clear, or the deadline is never
    retried; and the run must not report `auto_flattens: 1`, or it is claiming a guarantee
    it did not keep.
    """
    wide_book(tmp_path, minutes=12, bid=39_999.9, ask=40_000.1, size=1000.0)

    class BigPosition(Base):
        def on_start(self, ctx) -> None:
            self.n = 0

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and self.n < 2:
                self.n += 1
                ctx.buy(qty=money("100"))

    result = run(
        tmp_path,
        BigPosition(),
        minutes=12,
        balance="100000000",
        leverage=20,
        auto_flatten=AutoFlatten(max_hold_ms=3 * MS_PER_MINUTE),
    )
    failures = kinds(result, "AUTO_FLATTEN_FAILED")
    assert failures, "the exit was refused and the run says so"
    assert "MARKET_LOT_SIZE" in failures[0]["reason"]
    assert result.auto_flattens == 0, "not counted as a flatten that happened"
    assert "AUTO_FLATTEN_UNMET" in result.flags
    # Retried on later marks rather than latched forever.
    assert len(kinds(result, "AUTO_FLATTEN")) > 1


# ================================ M43/M44: the smoke run is the engine's arithmetic


def test_the_smoke_run_prices_through_the_fill_model_not_at_the_mark() -> None:
    """M43. A synthetic ladder whose touch is far from the mark.

    `synthetic_depth` builds a book around the bar, so a `BOOK_WALK` buy takes the ask and
    not the mark. Asserting they differ is the only way to tell a fill priced by the model
    from one priced at the mark, and every previous fixture had them coincide.
    """
    from perplab.core.types import DepthSnapshot
    from perplab.strategy.context import Context, OrderIntent, OrderType
    from perplab.strategy.dryrun import DryRunRuntime

    runtime = DryRunRuntime(symbols=("BTCUSDT",))
    runtime.advance(1_000)
    runtime.set_mark("BTCUSDT", money("40000"))
    runtime.set_depth(
        DepthSnapshot(
            symbol="BTCUSDT",
            ts_ms=1_000,
            recv_ms=1_000,
            last_update_id=1,
            bid_px=(3_900_000_000_000,),
            bid_qty=(10_00000000,),
            ask_px=(4_100_000_000_000,),
            ask_qty=(10_00000000,),
        )
    )
    runtime.submit(
        OrderIntent(symbol="BTCUSDT", side="BUY", qty=money("1"), type=OrderType.MARKET)
    )
    # The clock must move before the fill lands: the smoke runtime defers market fills
    # exactly as the engine's latency queue does (H13).
    runtime.advance(1_001)
    fill = runtime.drain_fills()[0]
    assert fill.price == money("41000"), "the ask, not the 40 000 mark"
    assert not any(e.kind == "NO_QUOTE_FILL" for e in runtime.events)


def test_an_unfundable_smoke_order_is_rejected_rather_than_crashing() -> None:
    """M44. The ledger refuses; validation must report it, not die of it.

    A strategy that sizes beyond its account should see the rejection and be able to handle
    it, in validation exactly as in live. Letting `InsufficientMargin` propagate failed the
    whole smoke run with a platform exception carrying the strategy's line number.
    """
    from perplab.strategy.context import OrderIntent, OrderType
    from perplab.strategy.dryrun import DryRunRuntime

    runtime = DryRunRuntime(symbols=("BTCUSDT",), start_balance="100")
    runtime.advance(1_000)
    runtime.set_mark("BTCUSDT", money("40000"))
    runtime.submit(
        OrderIntent(symbol="BTCUSDT", side="BUY", qty=money("1000"), type=OrderType.MARKET)
    )
    runtime.advance(1_001)  # deferred fills (H13): the refusal lands with the clock
    assert runtime.drain_fills() == []
    ends = runtime.drain_ends()
    assert len(ends) == 1 and ends[0].status == "REJECTED"
    assert any(e.kind == "REJECT" for e in runtime.events)


# ===================================================== M46: sweep results in grid order


def test_sweep_results_come_back_in_grid_order_whatever_order_they_ran(
    tmp_path: Path,
) -> None:
    """M46. The previous test only ever ran one worker, where arrival order *is* grid order.

    With a pool, points finish in whatever order they finish. Sorting is what makes the
    returned list a function of the inputs, and the only fixture that can tell is one where
    the completion order is not the submission order.
    """
    from perplab.lab.sweep import points_from_spec, sweep

    root = _sweep_lake(tmp_path)
    points = points_from_spec(_sweep_spec(), {"size": [1.0, 2.0, 3.0]})
    # **Handed to `sweep` out of order.** The serial path iterates the sequence it is
    # given, so an implementation that returns `results` unsorted returns them in *this*
    # order -- and the previous test passed them in index order, where the two are
    # indistinguishable.
    shuffled = [points[2], points[0], points[1]]
    results = sweep(root, shuffled, max_workers=1)
    assert [r.index for r in results] == [0, 1, 2]
    assert [r.params["size"] for r in results] == [1.0, 2.0, 3.0]

    from perplab.lab import sweep as lab_sweep_module

    assert hasattr(lab_sweep_module, "run_point"), (
        "perplab.lab must not shadow its own submodule with the sweep() function"
    )


def _sweep_spec():
    from perplab.engine.runspec import RunSpec

    return RunSpec(
        strategy_id=1,
        version_id=1,
        version_no=1,
        strategy_name="Buyer",
        code=_SWEEP_CODE,
        class_name="Buyer",
        params={"size": 1.0},
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 6 * MS_PER_MINUTE,
        seed=7,
        opening_balance="100000",
        leverage=5,
        maker_rate="0.0002",
        taker_rate="0.0005",
        fee_source="test",
        latency={"model": "fixed", "submit_ms": 10, "cancel_ms": 10},
        fill_tier="BAR_CLOSE",
        fill_model={"tier": "BAR_CLOSE", "slippage_bps": "1.0"},
        liquidation_recovery_pct="0",
        timeout_s=60.0,
        engine_version=1,
    )


def _sweep_lake(root: Path) -> Path:
    import json

    from tests.support import BTCUSDT_PAYLOAD

    build_lake(root / "market", start_ms=START, minutes=6, trade_path=flat_path(40_000.0))
    payloads = {
        "exchangeInfo": {"symbols": [BTCUSDT_PAYLOAD]},
        "leverageBracket": [
            {
                "symbol": "BTCUSDT",
                "brackets": [
                    {
                        "bracket": 1,
                        "initialLeverage": 125,
                        "notionalCap": 50_000,
                        "notionalFloor": 0,
                        "maintMarginRatio": 0.004,
                        "cum": 0.0,
                    },
                    {
                        "bracket": 2,
                        "initialLeverage": 100,
                        "notionalCap": 10_000_000,
                        "notionalFloor": 50_000,
                        "maintMarginRatio": 0.005,
                        "cum": 50.0,
                    },
                ],
            }
        ],
    }
    for kind, payload in payloads.items():
        directory = root / "reference" / kind
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "2024-01-01.json").write_text(json.dumps(payload), encoding="utf-8")
    return root


_SWEEP_CODE = '''
from perplab.strategy import Strategy

class Buyer(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 0, "datasets": ["klines"]}
    params = {"size": {"type": "float", "default": 1.0, "min": 0.001, "max": 100.0}}

    def on_start(self, ctx):
        self.done = False

    def on_bar(self, ctx, bar):
        if ctx.warm and not self.done:
            self.done = True
            ctx.buy(qty=ctx.money(self.p.size))
'''


# ============================================ M48: open interest is not written twice


def test_the_open_interest_poller_drops_a_re_served_sample() -> None:
    """M48. The endpoint re-serves the same instant when polled inside its update window.

    Writing it twice gives a last-observation-carried-forward read two candidate rows for
    one timestamp, and which one wins depends on physical file order.
    """
    import asyncio

    from perplab.data.rest_poller import OpenInterestPoller

    class FakeClient:
        def __init__(self) -> None:
            self.payloads = [
                {"openInterest": "81000.0", "time": 1_700_000_000_000},
                {"openInterest": "81000.0", "time": 1_700_000_000_000},
                {"openInterest": "81200.0", "time": 1_700_000_300_000},
            ]
            self.calls = 0

        async def open_interest(self, symbol: str):
            payload = self.payloads[self.calls]
            self.calls += 1
            return payload

    client = FakeClient()
    poller = OpenInterestPoller(
        "BTCUSDT", client, on_rows=lambda *a: None, on_event=lambda *a: None
    )
    rows = [asyncio.run(poller.fetch()) for _ in range(3)]
    assert [len(r) for r in rows] == [1, 0, 1], "the repeat is dropped"
    assert [r[0]["create_time"] for r in rows if r] == [
        1_700_000_000_000,
        1_700_000_300_000,
    ]
