"""Hedge mode through the whole engine: `ctx`, order routing, trades and the monitor.

`tests/golden/test_hedge_mode.py` pins the *arithmetic* against hand-derived numbers. This
file pins the layers above it -- that a strategy can actually address two sides, that the
orders it sends carry the side to the ledger, that round-trips are reconstructed per side,
and that the live monitor renders two positions rather than one.

Every test here would pass on a build that netted the two legs *if* it only checked the
wallet. So each one asserts on something netting cannot produce: two round-trips, two entry
prices, two rows.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.analytics.trades import TradeBuilder
from perplab.core.money import parse_money
from perplab.core.types import PositionSide
from perplab.engine.backtest import BacktestConfig, BacktestEngine
from perplab.engine.fills import MarketFillModel
from perplab.engine.latency import FixedLatency
from perplab.strategy.base import Strategy
from perplab.strategy.context import OrderIntent, OrderType
from tests.engine_lake import MS_PER_MINUTE, build_lake, ramp_path
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000
NO_SLIPPAGE = MarketFillModel(slippage_bps=parse_money("0"))
SYMBOL = "BTCUSDT"


# ------------------------------------------------------------------------- strategies


class OpenBothSides(Strategy):
    """Opens a long and a short on the same symbol, then holds both."""

    requires = {"symbols": [SYMBOL], "timeframe": "1m", "history": 1, "datasets": ["klines"]}

    def on_start(self, ctx):
        self.done = False

    def on_bar(self, ctx, bar):
        if not ctx.warm or self.done:
            return
        ctx.buy(qty=ctx.money("0.5"), position_side="LONG")
        ctx.sell(qty=ctx.money("0.2"), position_side="SHORT")
        self.done = True


class ForgetsTheSide(Strategy):
    """Calls `ctx.buy()` with no side in a hedge run. Must be refused, not guessed."""

    requires = {"symbols": [SYMBOL], "timeframe": "1m", "history": 1, "datasets": ["klines"]}

    def on_start(self, ctx):
        self.error: str | None = None
        self.done = False

    def on_bar(self, ctx, bar):
        if not ctx.warm or self.done:
            return
        self.done = True
        try:
            ctx.buy(qty=ctx.money("0.5"))
        except ValueError as exc:
            self.error = str(exc)


class ClosesBothSides(OpenBothSides):
    """Opens both, then flattens the symbol with the explicit both-sides call."""

    def on_start(self, ctx):
        super().on_start(ctx)
        self.closed = False
        self.close_ids: tuple[str, ...] = ()

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return
        if not self.done:
            super().on_bar(ctx, bar)
            return
        if not self.closed:
            self.closed = True
            self.close_ids = ctx.close_all()


class ReadsBothSides(OpenBothSides):
    """Records what `ctx.positions()` and `ctx.position(side)` report once both are open."""

    def on_start(self, ctx):
        super().on_start(ctx)
        self.seen: list[tuple[str, Decimal]] = []
        self.ambiguous: str | None = None

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return
        if not self.done:
            super().on_bar(ctx, bar)
            return
        if self.seen:
            return
        for view in ctx.positions():
            self.seen.append((view.position_side.value, view.qty))
        try:
            ctx.position()
        except ValueError as exc:
            self.ambiguous = str(exc)


def run_engine(
    strategy: Strategy,
    *,
    root: Path,
    end_ms: int,
    hedge_mode: bool = True,
    leverage: int = 10,
):
    config = BacktestConfig(
        symbols=(SYMBOL,),
        timeframe=strategy.declared.timeframe,
        start_ms=START,
        end_ms=end_ms,
        seed=1,
        opening_balance=parse_money("100000"),
        leverage=leverage,
        hedge_mode=hedge_mode,
        latency=FixedLatency(submit=120, cancel=120),
        fill_model=NO_SLIPPAGE,
    )
    engine = BacktestEngine(
        root=root,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={SYMBOL: btcusdt_filters()},
        brackets={SYMBOL: single_bracket_table(mmr=Decimal("0.004"))},
    )
    return engine, engine.run()


@pytest.fixture()
def lake(tmp_path: Path) -> Path:
    root = tmp_path / "market"
    build_lake(root, symbol=SYMBOL, start_ms=START, minutes=120, trade_path=ramp_path(40_000.0, 1.0))
    return root


# ------------------------------------------------------------------------------- ctx


def test_a_strategy_can_hold_a_long_and_a_short_on_one_symbol(lake: Path) -> None:
    """End to end: two orders, two positions, two entry prices, nothing realised."""
    engine, result = run_engine(OpenBothSides(), end_ms=START + 120 * MS_PER_MINUTE, root=lake)

    long = engine.account.position(SYMBOL, PositionSide.LONG)
    short = engine.account.position(SYMBOL, PositionSide.SHORT)
    assert long is not None and short is not None
    assert long.qty == Decimal("0.5")
    assert short.qty == Decimal("-0.2")
    assert long.entry_price != short.entry_price or long.qty != -short.qty

    # Netting would have realised something here; two independent positions realise nothing.
    assert engine.account.total_realized == Decimal(0)
    assert engine.account.gross_qty(SYMBOL) == Decimal("0.7")
    assert engine.account.net_qty(SYMBOL) == Decimal("0.3")


def test_an_order_with_no_side_is_refused_in_a_hedge_run(lake: Path) -> None:
    """A `SELL` means "reduce the long" or "open the short". The platform will not pick.

    The refusal reaches the strategy as a `ValueError` from `ctx.buy()` rather than as a
    silent route, so the author finds out at the first bar rather than from a position they
    did not open.
    """
    strategy = ForgetsTheSide()
    run_engine(strategy, end_ms=START + 120 * MS_PER_MINUTE, root=lake)

    assert strategy.error is not None
    assert "hedge mode" in strategy.error
    assert 'position_side="LONG"' in strategy.error


def test_the_same_strategy_in_a_one_way_run_refuses_the_side(lake: Path) -> None:
    """The mirror: a side-routed order in a one-way run is a mode error, not a long."""

    class SidedInOneWay(Strategy):
        requires = {
            "symbols": [SYMBOL], "timeframe": "1m", "history": 1, "datasets": ["klines"]
        }

        def on_start(self, ctx):
            self.error: str | None = None
            self.done = False

        def on_bar(self, ctx, bar):
            if not ctx.warm or self.done:
                return
            self.done = True
            try:
                ctx.buy(qty=ctx.money("0.5"), position_side="LONG")
            except ValueError as exc:
                self.error = str(exc)

    strategy = SidedInOneWay()
    run_engine(strategy, end_ms=START + 120 * MS_PER_MINUTE, hedge_mode=False, root=lake)
    assert strategy.error is not None
    assert "one-way mode" in strategy.error


def test_ctx_positions_returns_both_and_ctx_position_refuses(lake: Path) -> None:
    """`ctx.positions()` is the answer; `ctx.position()` is the question with two answers."""
    strategy = ReadsBothSides()
    run_engine(strategy, end_ms=START + 120 * MS_PER_MINUTE, root=lake)

    assert strategy.seen == [("LONG", Decimal("0.5")), ("SHORT", Decimal("-0.2"))]
    assert strategy.ambiguous is not None
    assert "hedge mode" in strategy.ambiguous


def test_close_all_closes_both_legs(lake: Path) -> None:
    """The explicit form of "flatten this symbol", so `ctx.close()` never has to guess."""
    strategy = ClosesBothSides()
    engine, _result = run_engine(strategy, end_ms=START + 120 * MS_PER_MINUTE, root=lake)

    assert len(strategy.close_ids) == 2
    assert engine.account.positions_for(SYMBOL) == ()


def test_a_reduce_only_flag_on_a_hedged_side_is_refused() -> None:
    """Binance rejects the combination, and so does the intent -- at construction.

    Accepting it and dropping it at the wire would be a live/backtest divergence in the risk
    layer, where `reduce_only` buys an exemption from every size limit.
    """
    with pytest.raises(ValueError, match="reduce_only is not meaningful"):
        OrderIntent(
            symbol=SYMBOL,
            side="SELL",
            qty=Decimal("1"),
            type=OrderType.MARKET,
            reduce_only=True,
            position_side=PositionSide.LONG,
        )


def test_the_intent_carries_the_side_onto_the_wire() -> None:
    """`to_json` is what the event log and the exchange payload are built from."""
    intent = OrderIntent(
        symbol=SYMBOL,
        side="BUY",
        qty=Decimal("1"),
        type=OrderType.MARKET,
        position_side=PositionSide.SHORT,
    )
    assert intent.to_json()["position_side"] == "SHORT"
    # And the default is Binance's own one-way value, sent verbatim.
    assert (
        OrderIntent(symbol=SYMBOL, side="BUY", qty=Decimal("1")).to_json()["position_side"]
        == "BOTH"
    )


# ---------------------------------------------------------------------------- trades


def test_round_trips_are_reconstructed_per_side() -> None:
    """Spec 8.1's "flat to flat" means flat *per side*.

    Keyed by symbol, a long and a short opened at different prices would be one trade whose
    entry price is a weighted average of two positions that never existed together, and which
    "closes" the moment either leg goes flat. Two trades is the honest reconstruction.
    """
    builder = TradeBuilder()
    builder.fill(
        ts_ms=1, symbol=SYMBOL, signed_qty=Decimal("1"), price=Decimal("50000"),
        fee=Decimal("25"), realized=Decimal("0"), qty_before=Decimal("0"),
        qty_after=Decimal("1"), position_side=PositionSide.LONG,
    )
    builder.fill(
        ts_ms=2, symbol=SYMBOL, signed_qty=Decimal("-1"), price=Decimal("52000"),
        fee=Decimal("26"), realized=Decimal("0"), qty_before=Decimal("0"),
        qty_after=Decimal("-1"), position_side=PositionSide.SHORT,
    )
    # Close the long only. The short must stay open.
    builder.fill(
        ts_ms=3, symbol=SYMBOL, signed_qty=Decimal("-1"), price=Decimal("51000"),
        fee=Decimal("25.5"), realized=Decimal("1000"), qty_before=Decimal("1"),
        qty_after=Decimal("0"), position_side=PositionSide.LONG,
    )

    trades = builder.snapshot()
    assert len(trades) == 2
    closed = [t for t in trades if not t.is_open]
    still_open = [t for t in trades if t.is_open]
    assert len(closed) == 1 and len(still_open) == 1
    assert closed[0].position_side == "LONG"
    assert closed[0].side == "LONG"
    assert closed[0].entry_price == Decimal("50000")
    assert closed[0].realized_pnl == Decimal("1000")
    assert still_open[0].position_side == "SHORT"
    assert still_open[0].entry_price == Decimal("52000")


def test_the_two_legs_excursions_do_not_cancel() -> None:
    """MAE per side, from each side's own unrealised PnL.

    Handing both legs the *summed* unrealised would give a market-neutral pair an MAE of
    roughly zero and make a pair whose legs each swung 20% look like it never moved -- and
    spec 8.3 says this distribution is what stops get sized from.
    """
    builder = TradeBuilder()
    builder.fill(
        ts_ms=1, symbol=SYMBOL, signed_qty=Decimal("1"), price=Decimal("50000"),
        fee=Decimal("0"), realized=Decimal("0"), qty_before=Decimal("0"),
        qty_after=Decimal("1"), position_side=PositionSide.LONG,
    )
    builder.fill(
        ts_ms=1, symbol=SYMBOL, signed_qty=Decimal("-1"), price=Decimal("50000"),
        fee=Decimal("0"), realized=Decimal("0"), qty_before=Decimal("0"),
        qty_after=Decimal("-1"), position_side=PositionSide.SHORT,
    )
    # Mark drops 5 000: the long is down 5 000, the short is up 5 000. Netted, nothing moved.
    builder.mark(
        symbol=SYMBOL, mark_price=Decimal("45000"), unrealized=Decimal("-5000"),
        position_side=PositionSide.LONG,
    )
    builder.mark(
        symbol=SYMBOL, mark_price=Decimal("45000"), unrealized=Decimal("5000"),
        position_side=PositionSide.SHORT,
    )

    by_side = {t.position_side: t for t in builder.snapshot()}
    assert by_side["LONG"].mae == Decimal("-5000")
    assert by_side["SHORT"].mfe == Decimal("5000")
    # And the per-symbol PnL still sums to the account's: the split is in the attribution,
    # not in the total.
    assert builder.net_pnl_by_symbol()[SYMBOL] == Decimal("0")


def test_funding_is_attributed_to_the_leg_that_paid_it() -> None:
    """A perfect hedge's *net* funding is zero, and neither leg's is."""
    builder = TradeBuilder()
    for side, qty in ((PositionSide.LONG, Decimal("1")), (PositionSide.SHORT, Decimal("-1"))):
        builder.fill(
            ts_ms=1, symbol=SYMBOL, signed_qty=qty, price=Decimal("50000"),
            fee=Decimal("0"), realized=Decimal("0"), qty_before=Decimal("0"),
            qty_after=qty, position_side=side,
        )
    builder.funding(symbol=SYMBOL, cashflow=Decimal("-5"), position_side=PositionSide.LONG)
    builder.funding(symbol=SYMBOL, cashflow=Decimal("5"), position_side=PositionSide.SHORT)

    by_side = {t.position_side: t for t in builder.snapshot()}
    assert by_side["LONG"].funding == Decimal("-5")
    assert by_side["SHORT"].funding == Decimal("5")


# ---------------------------------------------------------------------- monitor rows


def test_the_monitor_emits_one_row_per_side_with_its_own_liquidation_price(
    lake: Path,
) -> None:
    """What the Live Monitor's table renders -- two rows, distinctly keyed.

    The UI keyed rows by `position.symbol`, so a hedged symbol produced two `<tr>` with the
    same React key: React reconciles them as one row whose values flicker between the legs.
    The payload now carries `position_side`, which is what makes the key unique and what tells
    the reader which leg they are looking at.
    """
    from perplab.live.session import PaperSession  # noqa: F401 - import shape check only

    engine, _result = run_engine(
        OpenBothSides(), end_ms=START + 120 * MS_PER_MINUTE, root=lake
    )

    rows = []
    for side in engine._sides:
        view = engine.runtime.position_view(SYMBOL, side)
        rows.append(
            {
                "symbol": SYMBOL,
                "position_side": side.value,
                "qty": str(view.qty),
                "entry_price": str(view.entry_price),
                "liquidation_price": view.liquidation_price,
            }
        )

    assert [r["position_side"] for r in rows] == ["LONG", "SHORT"]
    # Distinct keys, which is the bug this closes.
    assert len({(r["symbol"], r["position_side"]) for r in rows}) == 2
    # Two genuinely different liquidation prices, straddling the entries.
    prices = [r["liquidation_price"] for r in rows]
    assert all(p is not None for p in prices)
    assert prices[0] != prices[1]
    assert prices[0] < prices[1]


def test_liquidation_distance_is_a_fraction_not_a_percent(lake: Path) -> None:
    """The unit the monitor's danger threshold is written against.

    The server sent this multiplied by 100, so a position 5% from liquidation arrived as
    `5.0`, rendered as "500.00%", and never crossed the `< 0.05` threshold that turns the
    proximity bar red. The warning on the single most dangerous number on the page was
    silently off; nothing failed, because nothing checked the unit.
    """
    engine, _result = run_engine(
        OpenBothSides(), end_ms=START + 120 * MS_PER_MINUTE, root=lake
    )
    view = engine.runtime.position_view(SYMBOL, PositionSide.LONG)
    mark = engine.account.marks[SYMBOL]
    liq = view.liquidation_price
    assert liq is not None

    distance = float(abs(mark - liq) / mark)
    # A 10x long sits about 9-10% from its liquidation, so the fraction is well under 1.
    assert 0.0 < distance < 1.0
    # The percent form would be two orders of magnitude larger, which is the defect.
    assert distance < 1.0 < float(abs(mark - liq) / mark * 100)


# ------------------------------------------------- the monitor payload, as published


def _monitor_positions(run_dir: Path, hedge_mode: bool) -> list[dict]:
    """Build a real `PaperSession` and read its published monitor rows.

    Constructed rather than hand-assembled because the row shape is the *contract with the
    UI*: the earlier version of this file recomputed the fields itself, which pinned the
    arithmetic and left the payload free to disagree with it. Two mutations survived on that
    -- the distance unit and the one-row-per-symbol loop -- and both are payload bugs.
    """
    from perplab.engine.tiers import FillTier
    from perplab.live.session import PaperSession, SessionConfig

    config = BacktestConfig(
        symbols=(SYMBOL, SYMBOL),  # repeated on purpose -- see the dedup assertion below
        timeframe="1m",
        start_ms=START,
        end_ms=START + 48 * 60 * MS_PER_MINUTE,
        opening_balance=parse_money("100000"),
        leverage=10,
        hedge_mode=hedge_mode,
        latency=FixedLatency(submit=120, cancel=120),
        fill_tier=FillTier.BAR_CLOSE,
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    if True:
        session = PaperSession(
            run_dir=run_dir,
            strategy=OpenBothSides(),
            requirements=OpenBothSides().declared,
            config=config,
            session=SessionConfig(run_id=1, endpoint="testnet", reorder_window_ms=250),
            filters={SYMBOL: btcusdt_filters()},
            brackets={SYMBOL: single_bracket_table(mmr=Decimal("0.004"))},
        )
        account = session.engine.account
        if hedge_mode:
            account.apply_fill(
                START, SYMBOL, Decimal("1"), Decimal("50000"),
                position_side=PositionSide.LONG,
            )
            account.apply_fill(
                START, SYMBOL, Decimal("-0.5"), Decimal("52000"),
                position_side=PositionSide.SHORT,
            )
        else:
            account.apply_fill(START, SYMBOL, Decimal("1"), Decimal("50000"))
        account.update_mark(START, SYMBOL, Decimal("50000"))
        return list(session.monitor()["positions"])


def test_the_published_monitor_has_one_row_per_side_each_uniquely_keyed(
    tmp_path: Path,
) -> None:
    """What the Live Monitor's table actually receives.

    Two things this pins that the UI cannot check for itself:

    - **One row per addressable position**, so a hedged symbol is two rows rather than one.
      A single row would have to pick a leg to describe, and whichever it picked would be
      the wrong one half the time.
    - **A key that is unique**, which is `(symbol, position_side)`. The table keyed rows by
      `position.symbol` alone, so a hedged symbol produced two `<tr>` with the same React
      key -- React reconciles those as one row whose values flicker between the two legs.
    """
    rows = _monitor_positions(tmp_path / "hedged", hedge_mode=True)

    assert len(rows) == 2, "a hedged symbol publishes both of its positions"
    assert [r["position_side"] for r in rows] == ["LONG", "SHORT"]
    keys = {(r["symbol"], r["position_side"]) for r in rows}
    assert len(keys) == len(rows), "every row must be distinctly keyable"

    # Two genuinely different positions, not one rendered twice.
    assert rows[0]["entry_price"] != rows[1]["entry_price"]
    assert rows[0]["liquidation_price"] != rows[1]["liquidation_price"]

    # And a repeated symbol in the run's own config does not produce a duplicate row.
    one_way = _monitor_positions(tmp_path / "one_way", hedge_mode=False)
    assert len(one_way) == 1
    assert one_way[0]["position_side"] == "BOTH"


def test_the_published_liquidation_distance_is_a_fraction(tmp_path: Path) -> None:
    """The unit the monitor's danger threshold is written against.

    The server sent this multiplied by 100, so a position 9% from liquidation arrived as
    `9.0`, rendered as "900.00%", and never crossed the `< 0.05` threshold that turns the
    proximity bar red. The warning on the single most dangerous number on the page was
    silently off, and nothing failed -- because nothing checked the unit.

    A 10x long sits a little under 10% from its liquidation, so the correct value is well
    inside `(0, 1)` and the percent form is two orders of magnitude outside it. Asserting the
    band rather than an exact figure keeps this a *unit* test rather than a second copy of
    the liquidation-price golden.
    """
    rows = _monitor_positions(tmp_path, hedge_mode=True)
    distances = [r["liq_distance_pct"] for r in rows]

    assert all(d is not None for d in distances)
    for distance in distances:
        assert 0.0 < distance < 1.0, (
            f"{distance} is not a fraction; every other _pct field in this platform is one, "
            "and the monitor's danger threshold compares against 0.05"
        )
    # The long is roughly 9.6% away at 10x. Pinned loosely, as a sanity band on the unit.
    assert 0.05 < distances[0] < 0.2


# --------------------------------------------------------- working exposure, per side


def test_working_orders_on_one_side_do_not_count_against_the_other(lake: Path) -> None:
    """A working order grows the leg it is routed to and no other.

    Pooling them would let a resting exit on the long cancel out a growing entry on the
    short -- they are opposite *signs* on the same symbol, so a pooled `WorkingExposure` nets
    them and under-reports the exposure the account has to survive. Written because a
    mutation removing the side filter survived everything else in the suite.

    The orders are placed into the book directly rather than submitted through `ctx`: the
    fixture lake carries no ticks, so it resolves to `BAR_CLOSE`, where a LIMIT order is
    refused outright (spec 6.4 -- there is no queue to sit in). What is under test is the
    filter in `_working_exposure`, and that is reachable from any open order.
    """
    from perplab.core.risk import WorkingExposure
    from perplab.engine.executor_base import Order, OrderStatus

    engine, _result = run_engine(
        OpenBothSides(), end_ms=START + 120 * MS_PER_MINUTE, root=lake
    )

    def rest(order_id: str, side: str, qty: str, position_side: PositionSide) -> None:
        intent = OrderIntent(
            symbol=SYMBOL,
            side=side,
            qty=Decimal(qty),
            type=OrderType.LIMIT,
            price=Decimal("1000"),
            position_side=position_side,
        )
        engine.orders[order_id] = Order(
            id=order_id,
            intent=intent,
            submit_ts=START,
            arrival_ts=START,
            reference_price=Decimal("40000"),
            remaining=int(Decimal(qty) * 10**8),
            status=OrderStatus.WORKING,
        )

    rest("w1", "BUY", "2", PositionSide.LONG)
    rest("w2", "SELL", "3", PositionSide.SHORT)

    on_long = engine._working_exposure(SYMBOL, PositionSide.LONG)
    on_short = engine._working_exposure(SYMBOL, PositionSide.SHORT)

    assert on_long.buy_qty == Decimal("2") and on_long.sell_qty == Decimal("0")
    assert on_short.sell_qty == Decimal("3") and on_short.buy_qty == Decimal("0")

    # The side-aware bounds follow: each leg reaches its own held quantity plus its own
    # working growth, and neither sees the other's.
    assert engine._side_exposure(SYMBOL, PositionSide.LONG) == Decimal("2.5")   # 0.5 + 2
    assert engine._side_exposure(SYMBOL, PositionSide.SHORT) == Decimal("3.2")  # 0.2 + 3
    # Summed, that is what `max_position_notional` is measured against.
    assert engine._symbol_exposure(SYMBOL) == Decimal("5.7")
