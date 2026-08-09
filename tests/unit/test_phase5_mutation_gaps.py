"""Tests that exist because a mutation survived.

Forty-four deliberate defects were put into the Phase 5 engine and the suite was run against
each. **Ten survived**, and a survivor is not a success -- it means the model could be wrong
in that exact way and nothing would notice.

Every one of the ten had the same shape, and it is the same shape Phase 4's mutation pass
found: **a fixture that cannot tell two answers apart.** A book quoted at a flat 40 000
cannot distinguish "the row in force at T" from "the row after it". A tape and a kline series
both at 40 000 cannot distinguish one account of the market from two. A `MARKET_LOT_SIZE`
whose numbers equal `LOT_SIZE`'s cannot distinguish which filter was applied. The assertions
were right; the data made them unfalsifiable.

So each test here constructs the coincidence the original fixture lacked -- a book that
*moves*, a tape that *disagrees* with its bars, a filter set whose two halves *differ* -- and
then asserts the same thing the original test meant to.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.money import parse_money
from perplab.engine.backtest import BacktestConfig, BacktestEngine
from perplab.engine.latency import FixedLatency
from perplab.strategy.base import Strategy
from perplab.strategy.context import (
    FillTier,
    OrderIntent,
    OrderType,
    TimeInForce,
)
from tests.engine_lake import MS_PER_MINUTE, build_lake, flat_path, ramp_path
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000  # 2024-03-01T00:00:00Z
BAR1 = START + MS_PER_MINUTE - 1
BAR2 = START + 2 * MS_PER_MINUTE - 1
LATENCY = 10


def _run(
    lake: Path,
    strategy: Strategy,
    *,
    tier: FillTier,
    minutes: int = 5,
    balance: str = "1000000",
    leverage: int = 10,
    fill_model=None,
):
    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
        seed=1,
        opening_balance=parse_money(balance),
        leverage=leverage,
        latency=FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=tier,
        fill_model=fill_model,
    )
    return BacktestEngine(
        root=lake,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
    ).run()


def _fills(result) -> list[dict]:
    return [dict(e.payload) for e in result.events if e.kind == "FILL"]


class BuyOnBar1(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and bar.close_time == BAR1:
            ctx.buy(qty=ctx.money("1"))


# ============================================== M11: the staleness bound on the ladder


def test_a_ladder_older_than_the_staleness_bound_cannot_price_a_fill(
    tmp_path: Path,
) -> None:
    """The bound was unfalsifiable because every fixture's depth ran the whole range.

    Depth here stops after the first minute; the order arrives in the fourth. A ladder held
    flat for three minutes is not a book, and walking it would price a fill against liquidity
    that has had three minutes to disappear. The real `BOOK_WALK` run over collector data hit
    this for real -- 5 of 13 orders refused, all of them submitted before the depth stream
    started.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=6,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(360)],
        quotes=[(START + s * 1000, 39_999.9, 5.0, 40_000.1, 5.0) for s in range(360)],
        # Depth for the first minute only.
        depth=[
            (START + s * 1000, [(39_999.9, 10.0)], [(40_000.1, 10.0)]) for s in range(60)
        ],
    )

    class BuyLate(Strategy):
        requires = {
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "history": 0,
            "datasets": ["klines"],
        }

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == START + 4 * MS_PER_MINUTE - 1:
                ctx.buy(qty=ctx.money("1"))

    result = _run(lake, BuyLate({}), tier=FillTier.BOOK_WALK, minutes=6)

    assert not _fills(result)
    assert result.rejects == 1
    reason = [dict(e.payload) for e in result.events if e.kind == "REJECT"][0]["reason"]
    assert "no depth snapshot is in force" in reason


def test_a_fresh_ladder_still_prices_the_same_fill(tmp_path: Path) -> None:
    """The control: the bound must refuse staleness, not depth."""
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=6,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(360)],
        quotes=[(START + s * 1000, 39_999.9, 5.0, 40_000.1, 5.0) for s in range(360)],
        depth=[
            (START + s * 1000, [(39_999.9, 10.0)], [(40_000.1, 10.0)]) for s in range(360)
        ],
    )

    class BuyLate(Strategy):
        requires = {
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "history": 0,
            "datasets": ["klines"],
        }

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == START + 4 * MS_PER_MINUTE - 1:
                ctx.buy(qty=ctx.money("1"))

    result = _run(lake, BuyLate({}), tier=FillTier.BOOK_WALK, minutes=6)
    assert len(_fills(result)) == 1
    assert result.rejects == 0


# ====================================== M22: the state stream takes the row *at* T


def test_a_fill_takes_the_quote_in_force_not_the_next_one(tmp_path: Path) -> None:
    """A flat book cannot distinguish `bisect_right(...) - 1` from `bisect_right(...)`.

    The quote steps by 1 every second here, so the two answers differ on every fill. The
    order is submitted at bar 1's close (`…:59.999`) and arrives 10 ms later, at
    `START + 60 009`; the newest quote at or before that is second **60**'s, published at
    `START + 60 000` -- ask `40 000.10 + 60 = 40 060.10`. Taking the *next* row would fill at
    40 061.10 against a quote that had not been published when the order arrived, which is
    look-ahead of exactly one book update.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(300)],
        quotes=[
            (START + s * 1000, 39_999.9 + s, 5.0, 40_000.1 + s, 5.0) for s in range(300)
        ],
    )
    from perplab.engine.fills import BookTickerFillModel

    result = _run(
        lake,
        BuyOnBar1({}),
        tier=FillTier.BOOK_TICKER,
        fill_model=BookTickerFillModel(impact_k_bps=parse_money("0")),
    )

    booked = _fills(result)
    assert len(booked) == 1
    assert parse_money(booked[0]["price"]) == parse_money("40060.10")


# ============================= M24: the reference price is the mid where a book exists


def test_the_slippage_reference_is_the_mid_not_the_last_print(tmp_path: Path) -> None:
    """A book centred on the tape cannot distinguish the mid from the last print.

    So the book is deliberately wide and off-centre: bid 39 000 / ask 41 000, mid **40 000**,
    while the tape prints at 40 500. A buy of 1 fills at the ask, 41 000.

        against the mid        41 000 - 40 000 =  1 000   <- what spec 8.4 should report
        against the last print 41 000 - 40 500 =    500

    The mid is the only side-neutral candidate: a sell in the same book would show 1 000 of
    slippage against the mid and 1 500 against the print. Measuring against the print makes
    the reported figure carry the trade-versus-book basis, whose sign follows the side.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_500.0),
        ticks=[(START + s * 1000, 40_500.0, 1.0, s % 2 == 0) for s in range(300)],
        quotes=[(START + s * 1000, 39_000.0, 5.0, 41_000.0, 5.0) for s in range(300)],
    )
    from perplab.engine.fills import BookTickerFillModel

    result = _run(
        lake,
        BuyOnBar1({}),
        tier=FillTier.BOOK_TICKER,
        fill_model=BookTickerFillModel(impact_k_bps=parse_money("0")),
    )

    booked = _fills(result)
    assert len(booked) == 1
    assert parse_money(booked[0]["reference_price"]) == parse_money("40000")
    assert parse_money(booked[0]["slippage"]) == parse_money("1000")


# ================================ M28: a tick tier carries one account of the market


def test_a_tick_tier_does_not_also_emit_the_kline_s_own_prints(tmp_path: Path) -> None:
    """Bars at 40 500 and a tape at 40 000, so the two accounts cannot be confused.

    Every fixture until now had them agree, which made the extra events invisible. Emitting
    the kline's open and close as `TRADE` events alongside the tape puts two accounts of the
    same market on one queue, and the kline's wins whenever it sorts later within the
    millisecond -- so the engine would price against a bar summary while claiming to be at a
    tick tier. The reference price is where it shows: 40 000 from the tape, 40 500 from the
    bar close the order was submitted at.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        # 40 500 against a 40 000 tape: far enough apart to tell the two accounts apart,
        # close enough to stay inside `PERCENT_PRICE`'s +/-5% band around the mark. A wider
        # gap makes every fill fail that filter and the test measures nothing.
        trade_path=flat_path(40_500.0),
        mark_path=flat_path(40_500.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(300)],
    )
    result = _run(lake, BuyOnBar1({}), tier=FillTier.TRADE_ONLY)

    booked = _fills(result)
    assert len(booked) == 1
    assert parse_money(booked[0]["reference_price"]) == parse_money("40000"), (
        "the reference came from the kline series, not the trade tape"
    )
    assert result.ticks == 300


# ================================= M17: the book horizon respects priority 3


def test_a_hook_below_book_update_priority_cannot_see_that_instant_s_quote(
    tmp_path: Path,
) -> None:
    """`on_funding` runs at priority 1; a `BOOK_UPDATE` at the same millisecond is priority 3.

    A flat book made the `ts_ms` / `ts_ms - 1` split unfalsifiable. Here the quote *jumps*
    exactly at the funding settlement -- 40 000 before, 50 000 at it -- so a hook that reads
    `ctx.spread()` sees which side of spec 6.2's ordering it is on. Priority 1 precedes
    priority 3, so it must see the old quote.
    """
    settlement = START + 3 * MS_PER_MINUTE
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=6,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(360)],
        quotes=[
            *[(START + s * 1000, 39_999.0, 5.0, 40_001.0, 5.0) for s in range(180)],
            # The jump lands exactly on the settlement millisecond.
            (settlement, 49_999.0, 5.0, 50_001.0, 5.0),
            *[(settlement + s * 1000, 49_999.0, 5.0, 50_001.0, 5.0) for s in range(1, 180)],
        ],
        funding=[(settlement, 0.0001)],
    )

    class WatchFunding(Strategy):
        requires = {
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "history": 0,
            "datasets": ["klines"],
        }

        def on_start(self, ctx) -> None:
            self.seen: list = []

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and bar.close_time == BAR1:
                ctx.buy(qty=ctx.money("0.01"))

        def on_funding(self, ctx, event) -> None:
            spread = ctx.spread()
            self.seen.append(None if spread is None else spread.mid)

    strategy = WatchFunding({})
    _run(lake, strategy, tier=FillTier.BOOK_TICKER, minutes=6)

    assert strategy.seen, "the funding hook never ran"
    assert strategy.seen[0] == parse_money("40000"), (
        f"a priority-1 hook saw the priority-3 quote from its own millisecond: "
        f"{strategy.seen[0]}"
    )


# ===================== M19: a triggered stop is validated as a *market* order


def test_a_triggered_stop_is_held_to_market_lot_size(tmp_path: Path) -> None:
    """`MARKET_LOT_SIZE` caps BTCUSDT at 120 where `LOT_SIZE` allows 1000.

    Every previous fixture used a quantity under both, so the two filters gave the same
    answer and nothing could tell which had been applied. A 500 BTC position accumulated in
    pieces is legal; exiting it in one market order is not, and that is precisely what
    `MARKET_LOT_SIZE` means. A stop attached to the whole position therefore has to be
    refused on trigger -- and a backtest that filled it would model an exit the exchange
    would not have accepted, at the worst possible moment.
    """
    lake = tmp_path / "market"

    def mark(index: int):
        from tests.engine_lake import Ohlc, scaled

        # 39 400 is 1.5% below entry: past the 39 500 stop, and comfortably above the
        # liquidation price of a 20x position (roughly 4.6% down at a 0.4% maintenance
        # rate). A deeper drop liquidates first -- correct behaviour, since spec 6.2 puts
        # the liquidation check at priority 2 and it cancels the symbol's resting orders --
        # and the test would then be measuring liquidation rather than the lot filter.
        if index >= 4:
            return Ohlc(scaled(39_400), scaled(39_400), scaled(39_400), scaled(39_400))
        return Ohlc(scaled(40_000), scaled(40_000), scaled(40_000), scaled(40_000))

    build_lake(
        lake,
        start_ms=START,
        minutes=7,
        trade_path=flat_path(40_000.0),
        mark_path=mark,
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(420)],
        quotes=[(START + s * 1000, 39_999.9, 500.0, 40_000.1, 500.0) for s in range(420)],
    )

    class BigThenStop(Strategy):
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
                # Five market orders of 100, each inside MARKET_LOT_SIZE's cap of 120.
                for _ in range(5):
                    ctx.buy(qty=ctx.money("100"))
            elif bar.close_time == BAR2 and not self.armed and not ctx.position().is_flat:
                self.armed = True
                ctx.stop_loss(stop_price=ctx.money("39500"))

    result = _run(
        lake,
        BigThenStop({}),
        tier=FillTier.BOOK_TICKER,
        minutes=7,
        balance="50000000",
        leverage=20,
    )

    triggers = [e for e in result.events if e.kind == "TRIGGER"]
    assert len(triggers) == 1, "the stop never fired"
    rejects = [dict(e.payload) for e in result.events if e.kind == "REJECT"]
    assert rejects, "a 500 BTC market exit was accepted"
    assert "MARKET_LOT_SIZE" in rejects[0]["reason"]


# ======================== M36 / M37: indicators are actually driven


def test_a_trade_driven_indicator_becomes_ready_and_carries_the_right_units(
    tmp_path: Path,
) -> None:
    """`CVD` was registered, declared in spec 5.4, and never fed a single trade.

    Nothing asserted it, so the engine could stop calling `indicators.on_trade` entirely and
    the suite stayed green. The units matter as much as the wiring: `TradePrint` exposes both
    a scaled integer and a float view, and reading the float one through `_scaled_to_float`
    divided by 10^8 twice.

    120 prints of 0.5, alternating aggressor: 60 buy-aggressive (+) and 60 sell-aggressive
    (-), starting with `s % 2 == 0` → `is_buyer_maker=True` → sell-aggressive. So the running
    total ends at exactly 0, and the *magnitude* along the way is 0.5 -- not 5e-9.
    """

    class UsesCvd(Strategy):
        requires = {
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "history": 0,
            "datasets": ["aggTrades"],
        }

        def on_start(self, ctx) -> None:
            self.cvd = ctx.indicators.cvd()

        def on_bar(self, ctx, bar) -> None:
            pass

    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=3,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 0.5, s % 2 == 0) for s in range(120)],
    )
    strategy = UsesCvd({})
    result = _run(lake, strategy, tier=FillTier.TRADE_ONLY, minutes=3)

    assert strategy.cvd.ready, "a trade-driven indicator was never fed"
    assert strategy.cvd.updates == 120
    assert strategy.cvd.value == pytest.approx(0.0, abs=1e-9)
    # The units check: every step of the series is +/-0.5, not +/-5e-9.
    assert max(abs(v) for v in strategy.cvd.series(120)) == pytest.approx(0.5)
    assert result.ticks == 120


def test_a_depth_driven_indicator_sees_every_snapshot(tmp_path: Path) -> None:
    """`BookImbalance` is spec 4.2's headline `BOOK_WALK` capability and was fed nothing.

    Depth is *pulled* as state for the fill models -- only the row in force at each instant
    the engine happens to ask is materialised -- so an indicator served from that path would
    see an arbitrary subsample rather than the series its author registered. A registered
    depth indicator therefore moves the whole dataset onto the event queue, and this asserts
    the count: 180 snapshots in, 180 updates out.
    """

    class UsesImbalance(Strategy):
        requires = {
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "history": 0,
            "datasets": ["depth20"],
        }

        def on_start(self, ctx) -> None:
            self.imbalance = ctx.indicators.book_imbalance(1)

        def on_bar(self, ctx, bar) -> None:
            pass

    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=3,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(180)],
        quotes=[(START + s * 1000, 39_999.9, 3.0, 40_000.1, 1.0) for s in range(180)],
        depth=[
            (START + s * 1000, [(39_999.9, 3.0)], [(40_000.1, 1.0)]) for s in range(180)
        ],
    )
    strategy = UsesImbalance({})
    _run(lake, strategy, tier=FillTier.BOOK_WALK, minutes=3)

    assert strategy.imbalance.ready, "a depth-driven indicator was never fed"
    assert strategy.imbalance.updates == 180
    # 3 bid against 1 ask: (3 - 1) / 4.
    assert strategy.imbalance.value == pytest.approx(0.5)


# ======================== M39 / M40: the intent defends its own invariants


def test_post_only_is_refused_on_a_market_order() -> None:
    """Binance rejects it outright; filling it as a taker is a live/backtest divergence.

    Nothing constructed one, so the guard could be deleted without a test noticing. It is
    the same hazard spec 6.5 names for post-only generally -- "how orders silently fail to
    enter" -- arriving from the other direction.
    """
    for tif in ("GTX", "FOK"):
        with pytest.raises(ValueError, match="resting-order time in force"):
            OrderIntent(
                symbol="BTCUSDT",
                side="BUY",
                qty=parse_money("1"),
                type=OrderType.MARKET,
                tif=TimeInForce(tif),
            )
    # Still accepted where it means something.
    OrderIntent(
        symbol="BTCUSDT",
        side="BUY",
        qty=parse_money("1"),
        type=OrderType.LIMIT,
        price=parse_money("40000"),
        tif=TimeInForce.GTX,
    )


def test_the_callback_rate_bound_is_defended_at_the_intent_not_only_at_ctx() -> None:
    """`Runtime.submit` takes an `OrderIntent` directly, so `ctx` is not the only door.

    Binance's own `callbackRate` is in percent: passing their `1.0` produces a trigger price
    of exactly zero and a trailing stop that can never fire -- silently, on a strategy that
    then looks like one that simply was never stopped out.
    """
    for bad in ("1.0", "5", "0", "-0.01"):
        with pytest.raises(ValueError, match="callback_rate is a fraction"):
            OrderIntent(
                symbol="BTCUSDT",
                side="SELL",
                qty=parse_money("1"),
                type=OrderType.TRAILING_STOP_MARKET,
                callback_rate=parse_money(bad),
            )
    OrderIntent(
        symbol="BTCUSDT",
        side="SELL",
        qty=parse_money("1"),
        type=OrderType.TRAILING_STOP_MARKET,
        callback_rate=parse_money("0.01"),
    )


def test_a_zero_quantity_intent_is_refused_at_the_type() -> None:
    """The third invariant `dryrun` already defended at the `Runtime` seam."""
    with pytest.raises(ValueError, match="positive quantity"):
        OrderIntent(symbol="BTCUSDT", side="BUY", qty=parse_money("0"))
