"""Regression tests for the defects the Phase 5 review found.

Four independent reviews, each required to demonstrate a finding numerically before
reporting it. One test per confirmed defect, named for the *behaviour*, each reproducing the
exact case the review used and each failing on the code as it was.

The pattern that produced most of these is worth naming: **a comment asserting a property the
code did not have.** Two of the four were docstrings claiming a choice was "pessimistic" or
"the only reading that cannot manufacture a fill", sitting directly above the opposite. A
comment is not a test, and a comment that is wrong is worse than none -- it is the reason
nobody looked again.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.money import from_scaled, parse_money
from perplab.core.types import DepthSnapshot, Side
from perplab.engine.backtest import BacktestConfig, BacktestEngine
from perplab.engine.book import MarketView
from perplab.engine.executor_base import Order, OrderStatus
from perplab.engine.latency import FixedLatency
from perplab.engine.resting import RestingBook
from perplab.engine.ticks import TopOfBook, TradePrint
from perplab.exchange.filters import validate_order
from perplab.strategy.base import Strategy
from perplab.strategy.context import (
    FillTier,
    OrderIntent,
    OrderType,
    TimeInForce,
    WorkingType,
)
from tests.engine_lake import MS_PER_MINUTE, Ohlc, build_lake, flat_path, scaled
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000  # 2024-03-01T00:00:00Z
BAR1 = START + MS_PER_MINUTE - 1
BAR2 = START + 2 * MS_PER_MINUTE - 1


def _order(
    order_id: str,
    side: str,
    *,
    kind: OrderType = OrderType.LIMIT,
    price: str | None = None,
    callback: str | None = None,
    stop: str | None = None,
    qty: str = "1",
) -> Order:
    intent = OrderIntent(
        symbol="BTCUSDT",
        side=side,
        qty=parse_money(qty),
        type=kind,
        price=None if price is None else parse_money(price),
        stop_price=None if stop is None else parse_money(stop),
        callback_rate=None if callback is None else parse_money(callback),
        tif=TimeInForce.GTC,
        working_type=WorkingType.MARK_PRICE,
        reduce_only=kind is not OrderType.LIMIT,
    )
    return Order(
        id=order_id,
        intent=intent,
        submit_ts=0,
        arrival_ts=0,
        reference_price=parse_money("100"),
        status=OrderStatus.WORKING,
        remaining=scaled(float(qty)),
        limit_scaled=0 if price is None else scaled(float(price)),
        trigger_price=None if stop is None else parse_money(stop),
    )


# ---------------------------------------------------------------- R1: trailing symmetry


def test_a_trailing_stop_fires_on_the_same_bar_whichever_side_it_protects() -> None:
    """The ratchet and the test have to be two passes, not one interleaved loop.

    One bar, `high=101 / low=99 / close=99.5`, and two trailing stops with the same 1%
    callback. Taking the extreme over the whole range puts **both** trigger levels at 99.99:

        SELL (protects a long)  extreme = max(range) = 101  ->  101 x 0.99 = 99.99
        BUY  (protects a short) extreme = min(range) =  99  ->   99 x 1.01 = 99.99

    and the bar reaches 99.99 from both directions, so both must fire. Interleaving the
    ratchet with the test made the answer depend on the order the caller listed the prices
    in: with `[high, low, close]` the SELL ratcheted on the high and fired on the low, while
    the BUY was tested against the high *before* its extreme had come down, and never fired
    at all. Same bar, same rate, identical levels, one exit.
    """
    market = MarketView()
    book = RestingBook(market=market)
    sell = _order("o-sell", "SELL", kind=OrderType.TRAILING_STOP_MARKET, callback="0.01")
    buy = _order("o-buy", "BUY", kind=OrderType.TRAILING_STOP_MARKET, callback="0.01")
    book.add(sell)
    book.add(buy)

    prices = [parse_money("101"), parse_money("99"), parse_money("99.5")]
    fired = {t.order_id for t in book.observe_price("BTCUSDT", prices, WorkingType.MARK_PRICE, ordered=False)}

    assert fired == {"o-sell", "o-buy"}
    assert sell.trail_extreme == parse_money("101")
    assert buy.trail_extreme == parse_money("99")
    assert sell.trigger_price == parse_money("99.99")
    assert buy.trigger_price == parse_money("99.99")


def test_a_trailing_stop_still_does_not_fire_when_the_range_misses_its_level() -> None:
    """The control. Two passes must not turn the ratchet into an unconditional trigger."""
    market = MarketView()
    book = RestingBook(market=market)
    sell = _order("o-sell", "SELL", kind=OrderType.TRAILING_STOP_MARKET, callback="0.01")
    book.add(sell)
    # Rises to 101 and closes at 100.5. Level is 101 x 0.99 = 99.99; the low never reaches it.
    prices = [parse_money("101"), parse_money("100.4"), parse_money("100.5")]
    assert not list(book.observe_price("BTCUSDT", prices, WorkingType.MARK_PRICE, ordered=False))
    assert sell.trail_extreme == parse_money("101")


def test_the_mirrored_short_stops_out_on_the_same_mark_bar_as_the_long(tmp_path: Path) -> None:
    """The same defect, end to end through the engine rather than the book in isolation.

    A long with a trailing stop and a short with the mirrored trailing stop, over mark bars
    that move against each in turn. Both must exit; before the fix the short exited a whole
    bar later, at a different price, or not at all.
    """

    def mark(index: int) -> Ohlc:
        if index == 3:
            # A wide bar: 39 600 … 40 300, closing back at the middle.
            return Ohlc(scaled(40_000), scaled(40_300), scaled(39_600), scaled(39_900))
        return Ohlc(scaled(40_000), scaled(40_000), scaled(40_000), scaled(40_000))

    exits: dict[str, int | None] = {}
    for side in ("long", "short"):

        class Trailer(Strategy):
            requires = {
                "symbols": ["BTCUSDT"],
                "timeframe": "1m",
                "history": 0,
                "datasets": ["klines"],
            }

            def __init__(self, direction: str) -> None:
                super().__init__({})
                self.direction = direction

            def on_start(self, ctx) -> None:
                self.armed = False

            def on_bar(self, ctx, bar) -> None:
                if not ctx.warm:
                    return
                if bar.close_time == BAR1:
                    if self.direction == "long":
                        ctx.buy(qty=ctx.money("0.01"))
                    else:
                        ctx.sell(qty=ctx.money("0.01"))
                elif bar.close_time == BAR2 and not self.armed and not ctx.position().is_flat:
                    self.armed = True
                    ctx.trailing_stop(callback_rate=ctx.money("0.01"))

        lake = tmp_path / side / "market"
        build_lake(
            lake,
            start_ms=START,
            minutes=6,
            trade_path=flat_path(40_000.0),
            mark_path=mark,
            ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(360)],
            quotes=[
                (START + s * 1000, 39_999.9, 5.0, 40_000.1, 5.0) for s in range(360)
            ],
        )
        strategy = Trailer(side)
        config = BacktestConfig(
            symbols=("BTCUSDT",),
            timeframe="1m",
            start_ms=START,
            end_ms=START + 6 * MS_PER_MINUTE,
            seed=1,
            opening_balance=parse_money("1000000"),
            leverage=10,
            latency=FixedLatency(submit=10, cancel=10),
            fill_tier=FillTier.BOOK_TICKER,
        )
        result = BacktestEngine(
            root=lake,
            strategy=strategy,
            requirements=strategy.declared,
            config=config,
            filters={"BTCUSDT": btcusdt_filters()},
            brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
        ).run()
        triggers = [e for e in result.events if e.kind == "TRIGGER"]
        exits[side] = triggers[0].ts_ms if triggers else None

    assert exits["long"] is not None, "the long's trailing stop never fired"
    assert exits["short"] is not None, "the short's trailing stop never fired"
    assert exits["long"] == exits["short"], (
        f"the two sides exited on different bars: long at {exits['long']}, "
        f"short at {exits['short']} -- the same defect, end to end"
    )


# ------------------------------------------------------------- R2: the unobserved queue


def test_an_unobserved_level_never_fills_on_an_at_level_print() -> None:
    """`queue_ahead = None` must stay `None`, not collapse to the front of the queue.

    Setting it to zero made *no information* strictly better than any non-trivial
    observation. Against two sell-aggressive prints of 2 and 5 BTC at our level, a buy limit
    for 5 filled:

        queue never observed  ->  5 BTC   (zero ahead: the second print is all ours)
        observed queue = 2    ->  5 BTC
        observed queue = 5    ->  2 BTC
        observed queue = 1000 ->  0 BTC

    The run was rewarded for having less data, which is the shape of every backtest lie spec
    6.4 warns about. Reachable whenever the book goes dark and the tape does not: past the
    staleness bound both `ladder()` and `top_of_book()` return `None`, so every resting order
    on that symbol has an unobserved level while trades keep arriving.
    """
    market = MarketView()
    book = RestingBook(market=market)
    order = _order("o1", "BUY", price="100", qty="5")
    book.add(order)
    assert order.queue_ahead is None

    prints = [
        TradePrint("BTCUSDT", 1, scaled(100), scaled(2), True, 1),
        TradePrint("BTCUSDT", 2, scaled(100), scaled(5), True, 2),
    ]
    filled = [fill for trade in prints for fill in book.on_trade(trade)]

    assert filled == [], "an unobserved queue produced a maker fill"
    assert order.queue_ahead is None, "the bound was invented rather than left unknown"
    assert order.remaining == scaled(5)


def test_an_unobserved_level_still_fills_when_the_market_trades_through_it() -> None:
    """The order is refused, not stranded.

    A print *below* our bid says the level was swept whatever was resting on it, so the
    through-trade rule still applies and is still bounded by the aggressor's own size.
    """
    market = MarketView()
    book = RestingBook(market=market)
    order = _order("o1", "BUY", price="100", qty="5")
    book.add(order)

    through = TradePrint("BTCUSDT", 1, scaled(99.9), scaled(2), True, 1)
    filled = list(book.on_trade(through))

    assert len(filled) == 1
    assert filled[0].qty == parse_money("2")
    assert filled[0].price == parse_money("100")


def test_observing_the_level_is_what_makes_it_fillable() -> None:
    """And once the level *is* observed, the ordering of outcomes is monotone in the queue."""
    outcomes = {}
    for queue in (2, 5, 1000):
        market = MarketView()
        market.apply_top(
            TopOfBook("BTCUSDT", 0, scaled(100), scaled(queue), scaled(101), scaled(10))
        )
        book = RestingBook(market=market)
        order = _order("o1", "BUY", price="100", qty="5")
        book.add(order)
        book.observe_book("BTCUSDT", 0)
        prints = [
            TradePrint("BTCUSDT", 1, scaled(100), scaled(2), True, 1),
            TradePrint("BTCUSDT", 2, scaled(100), scaled(5), True, 2),
        ]
        filled = [fill for trade in prints for fill in book.on_trade(trade)]
        outcomes[queue] = sum((f.qty for f in filled), parse_money("0"))

    assert outcomes[2] == parse_money("5")
    assert outcomes[5] == parse_money("2")
    assert outcomes[1000] == parse_money("0")


# ------------------------------------------------------ R3: per-increment filter checks


def test_a_partial_increment_is_not_held_to_an_order_s_size_floors() -> None:
    """`filters.py` said this in prose since Phase 2; now it is enforced.

    *"a partial fill of 0.001 BTC against a larger order is completely legitimate even
    though its notional is far below `minNotional`, so enforcing that filter at fill time
    would reject the correct behaviour of the fill model spec 6.5 describes."*

    The price grid and the lot step still apply to an increment, because those are properties
    of any execution the exchange could have printed.
    """
    filters = btcusdt_filters()
    tiny_qty = scaled(0.001)
    price = scaled(39_990)

    whole = validate_order(filters, qty=tiny_qty, price=price)
    assert not whole and "MIN_NOTIONAL" in whole.reason

    increment = validate_order(filters, qty=tiny_qty, price=price, partial=True)
    assert increment, increment.reason

    # Still refused on the price grid, which is not a size floor.
    off_tick = validate_order(filters, qty=tiny_qty, price=price + 1, partial=True)
    assert not off_tick and "PRICE_FILTER" in off_tick.reason
    # And still refused off the lot step.
    off_step = validate_order(filters, qty=tiny_qty + 1, price=price, partial=True)
    assert not off_step and "quantity" in off_step.reason


def test_a_small_sweep_fills_a_resting_order_instead_of_deleting_it(tmp_path: Path) -> None:
    """The defect end to end: a 0.001 BTC sweep took a live 1 BTC bid off the book.

    A 0.001 BTC increment against a bid at 39 990 is worth 39.99 USDT, below BTCUSDT's
    `MIN_NOTIONAL` of 50 — so the whole-order check rejected it, `_reject` removed the order,
    and the run reported `ORDERS_REJECTED` for something the strategy never did. The exchange
    would have filled the increment and kept the remaining 0.999 working.
    """

    class RestOnce(Strategy):
        requires = {
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "history": 0,
            "datasets": ["klines"],
        }

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == BAR1:
                ctx.buy(qty=ctx.money("1"), type="LIMIT", price=ctx.money("39990"))

    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=[
            *[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(60)],
            # A single sweep of exactly one lot step at our resting price.
            (START + 90_000, 39_990.0, 0.001, True),
        ],
        quotes=[
            *[(START + s * 1000, 39_999.9, 5.0, 40_000.1, 5.0) for s in range(60)],
            # The level becomes the touch and is empty, so we are at the front of the queue.
            *[(START + 60_000 + s * 1000, 39_990.0, 0.0, 40_000.1, 5.0) for s in range(120)],
        ],
    )
    strategy = RestOnce({})
    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 5 * MS_PER_MINUTE,
        seed=1,
        opening_balance=parse_money("1000000"),
        leverage=10,
        latency=FixedLatency(submit=10, cancel=10),
        fill_tier=FillTier.BOOK_TICKER,
    )
    result = BacktestEngine(
        root=lake,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
    ).run()

    fills = [dict(e.payload) for e in result.events if e.kind == "FILL"]
    assert result.rejects == 0, "the sweep rejected an order the exchange would have kept"
    assert "ORDERS_REJECTED" not in result.flags
    assert len(fills) == 1
    assert parse_money(fills[0]["qty"]) == parse_money("0.001")
    assert parse_money(fills[0]["remaining"]) == parse_money("0.999")


# -------------------------------------------------------------- R4: the ladder mirror


def test_a_ladder_with_a_zero_priced_side_is_refused_like_a_quote_with_one() -> None:
    """The two entry points into `MarketView` must use one predicate, and did not.

    `apply_top` refused a non-positive price; `apply_ladder`'s mirror checked only for a
    crossed quote. A zero best bid would therefore be mirrored into the top of book, and
    `_reference_price` takes the **mid** of that — halving it, and reporting ~20 000 USDT of
    fabricated slippage per unit on every subsequent fill.
    """
    market = MarketView()
    market.apply_ladder(
        DepthSnapshot(
            symbol="BTCUSDT",
            ts_ms=0,
            recv_ms=0,
            last_update_id=1,
            bid_px=(0,),
            bid_qty=(scaled(1),),
            ask_px=(scaled(40_000),),
            ask_qty=(scaled(1),),
        )
    )
    assert market.top_of_book("BTCUSDT", 0) is None
    assert market.ladder("BTCUSDT", 0) is None

    market.apply_top(TopOfBook("BTCUSDT", 0, 0, scaled(1), scaled(40_000), scaled(1)))
    assert market.top_of_book("BTCUSDT", 0) is None


def test_a_sane_ladder_still_mirrors_into_the_top_of_book() -> None:
    """The control: the guard must not refuse a healthy ladder.

    The mirror is what lets `ctx.spread()` answer on a `BOOK_WALK` run whose range has depth
    but no `bookTicker` coverage.
    """
    market = MarketView()
    market.apply_ladder(
        DepthSnapshot(
            symbol="BTCUSDT",
            ts_ms=0,
            recv_ms=0,
            last_update_id=1,
            bid_px=(scaled(39_999),),
            bid_qty=(scaled(2),),
            ask_px=(scaled(40_001),),
            ask_qty=(scaled(3),),
        )
    )
    top = market.top_of_book("BTCUSDT", 0)
    assert top is not None
    assert from_scaled(top.bid_px) == parse_money("39999")
    assert from_scaled(top.ask_qty) == parse_money("3")


# --------------------------------------------- R6: cancels are always delivered


class QuoteAndPull(Strategy):
    """Rests a passive bid and cancels it in the same hook -- inside one latency window."""

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and bar.close_time == BAR1:
            order_id = ctx.buy(
                qty=ctx.money("1"), type="LIMIT", price=ctx.money("39900")
            )
            ctx.cancel(order_id)


def _engine(lake: Path, strategy: Strategy, *, minutes: int = 5, tier=FillTier.BOOK_TICKER):
    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
        seed=1,
        opening_balance=parse_money("1000000"),
        leverage=10,
        latency=FixedLatency(submit=10, cancel=10),
        fill_tier=tier,
    )
    return BacktestEngine(
        root=lake,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
    )


def test_a_cancel_issued_before_the_order_lands_still_cancels_it(tmp_path: Path) -> None:
    """A cancel that cannot *beat* the order there still reaches the exchange after it.

    The rule this replaces -- drop the cancel whenever it would arrive at or after a still
    in-flight order -- is right for a market order, which fills at arrival, and wrong for
    everything that rests. A GTC limit is perfectly cancellable one millisecond after it
    lands, and discarding the instruction meant a strategy that quoted and pulled inside one
    latency window could never pull: the order stayed on the book and filled a minute later,
    leaving the run holding a position the strategy had explicitly cancelled.

    Losing the *race* is still modelled -- `_on_arrival` no-ops on an order that is no longer
    open, which is R19 exactly. What is no longer modelled is a cancel that never arrives.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=[
            *[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(60)],
            # A sweep through the resting bid, a full minute after it was cancelled.
            (START + 120_000, 39_899.0, 5.0, True),
        ],
        quotes=[(START + s * 1000, 39_999.9, 5.0, 40_000.1, 5.0) for s in range(300)],
    )
    result = _engine(lake, QuoteAndPull({})).run()

    assert not [e for e in result.events if e.kind == "FILL"], (
        "an order the strategy cancelled still filled"
    )
    assert len([e for e in result.events if e.kind == "CANCEL"]) == 1
    assert result.final_equity == result.opening_balance
    # The diagnostic survives as a note rather than as a decision.
    assert [e for e in result.events if e.kind == "CANCEL_TOO_LATE"]


# ------------------------------------------ R7: no order is left permanently open


class OverClose(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def on_bar(self, ctx, bar) -> None:
        if not ctx.warm:
            return
        if bar.close_time == BAR1:
            ctx.buy(qty=ctx.money("1"))
        elif bar.close_time == BAR2:
            ctx.sell(qty=ctx.money("3"), reduce_only=True)


class OffStep(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and bar.close_time == BAR1:
            ctx.buy(qty=ctx.money("1.0005"))


def _plain_lake(tmp_path: Path, *, minutes: int = 5) -> Path:
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=minutes,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(minutes * 60)],
        quotes=[
            (START + s * 1000, 39_999.9, 5.0, 40_000.1, 5.0) for s in range(minutes * 60)
        ],
    )
    return lake


def test_a_market_order_clamped_at_arrival_does_not_stay_open_forever(
    tmp_path: Path,
) -> None:
    """A reduce-only sell for more than the position leaves a residue with nowhere to go.

    `_clamp` trims the fill to what the position can absorb, but the order's own `remaining`
    kept the untrimmed figure -- so a market order that had done everything it could sat open
    for the rest of the run, was counted as a partial fill, and stayed in `ctx.open_orders()`.
    A market order is not on the resting book; nothing was ever going to revisit it.
    """
    engine = _engine(_plain_lake(tmp_path), OverClose({}))
    result = engine.run()

    assert not engine._open_order_ids(None), "an order was left open at the end of the run"
    assert all(not order.is_open for order in engine.orders.values())
    expiries = [dict(e.payload) for e in result.events if e.kind == "EXPIRE"]
    assert expiries and "carries no remainder" in expiries[0]["reason"]


def test_a_quantity_off_the_lot_step_cannot_leave_an_unfillable_residue(
    tmp_path: Path,
) -> None:
    """`remaining` is quantised at submission, so it can actually reach zero.

    1.0005 against a 0.001 step fills 1.000 and leaves 0.0005 -- a quantity no fill path can
    ever take, because every one of them floors to the step first.
    """
    engine = _engine(_plain_lake(tmp_path), OffStep({}))
    result = engine.run()

    assert not engine._open_order_ids(None)
    assert result.partial_fills == 0, "a fully-worked order was counted as a partial fill"
    assert engine.account.qty("BTCUSDT") == parse_money("1.000")


# ------------------------------------ R8: taker increments are increments too


class CrossThin(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and bar.close_time == BAR1:
            ctx.buy(qty=ctx.money("1"), type="LIMIT", price=ctx.money("40000.10"))


def test_the_crossing_part_of_a_limit_into_a_thin_touch_is_not_rejected(
    tmp_path: Path,
) -> None:
    """`partial` has to mean "an increment", not "a passive increment".

    A GTC buy limit for 1 BTC crossing an ask that holds only 0.001 takes that 0.001 as a
    *taker* and rests the remaining 0.999. Keying the relaxation on `is_maker` fixed the
    queue path and left this one, so the increment failed `MIN_NOTIONAL` at 40.00 against 50
    and the whole order was rejected -- with the run blaming the strategy's sizing for what
    was a thin book.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(60)],
        quotes=[(START + s * 1000, 39_999.9, 5.0, 40_000.1, 0.001) for s in range(300)],
        depth=[
            (START + s * 1000, [(39_999.9, 5.0)], [(40_000.10, 0.001), (40_000.20, 10.0)])
            for s in range(300)
        ],
    )
    result = _engine(lake, CrossThin({}), tier=FillTier.BOOK_WALK).run()

    fills = [dict(e.payload) for e in result.events if e.kind == "FILL"]
    assert result.rejects == 0, "a thin touch rejected the whole order"
    assert len(fills) == 1
    assert parse_money(fills[0]["qty"]) == parse_money("0.001")
    assert fills[0]["is_maker"] is False
    working = [dict(e.payload) for e in result.events if e.kind == "ORDER_WORKING"]
    assert working and parse_money(working[0]["remaining"]) == parse_money("0.999")


# ------------------------- R9: the trade tape is ordered, a mark bar's range is not


def _armed_trail() -> tuple[RestingBook, Order]:
    book = RestingBook(market=MarketView())
    order = _order("o1", "SELL", kind=OrderType.TRAILING_STOP_MARKET, callback="0.01")
    RestingBook.advance_trail(order, parse_money("40000"))
    book.add(order)
    return book, order


def test_a_contract_price_trailing_stop_does_not_ratchet_on_a_later_print() -> None:
    """Two passes over an *ordered* tape is within-millisecond look-ahead.

    A SELL trailing stop armed at 40 000 with a 1% callback sits at 39 600. Two prints share
    one millisecond: 39 700, then 41 000. Ratcheting across the whole batch before testing
    any of it moves the level to 40 590 and then fires the stop on the 39 700 print -- a
    price that preceded the one that created the level it supposedly broke.
    """
    book, order = _armed_trail()
    prices = [parse_money("39700"), parse_money("41000")]
    fired = list(
        book.observe_price("BTCUSDT", prices, WorkingType.MARK_PRICE, ordered=True)
    )
    assert not fired, "the stop fired on a print that preceded its own trigger level"
    assert order.trail_extreme == parse_money("41000")
    assert order.trigger_price == parse_money("40590.00")


def test_the_reversed_tape_gives_the_opposite_answer() -> None:
    """The other half of the same claim: order *must* matter on an ordered sequence.

    41 000 first ratchets the level to 40 590, and the 39 700 print that follows breaks it.
    Same two prices, opposite outcome -- which is what a causal model looks like. The tell
    that the old code was wrong was that reversing them changed nothing.
    """
    book, _ = _armed_trail()
    prices = [parse_money("41000"), parse_money("39700")]
    fired = list(
        book.observe_price("BTCUSDT", prices, WorkingType.MARK_PRICE, ordered=True)
    )
    assert [t.price for t in fired] == [parse_money("39700")]


# ------------------------------- R10: one condition, one terminal state


class StopThenClose(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def on_start(self, ctx) -> None:
        self.armed = False

    def on_bar(self, ctx, bar) -> None:
        if not ctx.warm:
            return
        if bar.close_time == BAR1:
            ctx.buy(qty=ctx.money("0.01"))
        elif bar.close_time == BAR2 and not self.armed:
            self.armed = True
            ctx.stop_loss(stop_price=ctx.money("37000"))
            ctx.close()


def test_a_stop_outliving_its_position_is_cancelled_not_rejected(tmp_path: Path) -> None:
    """An ordinary bracket must not be reported as a sizing mistake.

    A stop attached to a position the strategy later closed by hand is the most common shape
    of leftover order there is. Retiring it as `REJECTED` raised `ORDERS_REJECTED` and a
    run-level warning blaming "the filter or margin that refused them" -- a different and
    untrue story, and one that contradicts `OrderStatus.REJECTED`'s own contract. The
    identical condition was already `CANCELLED` on the other code path, so two terminal
    states existed for one fact and one of them was wrong.
    """

    def mark(index: int) -> Ohlc:
        if index >= 3:
            return Ohlc(scaled(36_000), scaled(36_000), scaled(36_000), scaled(36_000))
        return Ohlc(scaled(40_000), scaled(40_000), scaled(40_000), scaled(40_000))

    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=6,
        trade_path=flat_path(40_000.0),
        mark_path=mark,
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(360)],
        quotes=[(START + s * 1000, 39_999.9, 5.0, 40_000.1, 5.0) for s in range(360)],
    )
    result = _engine(lake, StopThenClose({}), minutes=6).run()

    assert result.rejects == 0
    assert "ORDERS_REJECTED" not in result.flags
    cancels = [dict(e.payload) for e in result.events if e.kind == "CANCEL"]
    assert any("already flat" in c.get("reason", "") for c in cancels)
