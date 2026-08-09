"""The Phase 4 backtest engine, against lakes whose every bar the test chose.

The scenarios here are the ones where being *approximately* right is worthless: which print
a fill takes, whether funding settles before the liquidation check, whether an intra-bar mark
excursion is seen at all, and whether a truncated run is a prefix of the full one.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.money import parse_money
from perplab.engine.backtest import (
    PROGRESS_EVERY,
    BacktestConfig,
    BacktestEngine,
    RunAborted,
    UnsupportedOrder,
)
from perplab.engine.fills import MarketFillModel
from perplab.engine.latency import FixedLatency
from perplab.strategy.base import Strategy
from tests.engine_lake import MS_PER_MINUTE, Ohlc, build_lake, flat_path, ramp_path
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000  # 2024-03-01T00:00:00Z
NO_SLIPPAGE = MarketFillModel(slippage_bps=parse_money("0"))


# ------------------------------------------------------------------------- strategies


class BuyOnce(Strategy):
    """Buys one unit on the first tradeable bar and holds. The simplest possible fill test."""

    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1, "datasets": ["klines"]}

    def on_start(self, ctx):
        self.done = False
        self.entry_bar_close = None

    def on_bar(self, ctx, bar):
        if not ctx.warm or self.done:
            return
        self.entry_bar_close = bar.close_time
        ctx.buy(qty=ctx.money("1"))
        self.done = True


class HalfThenFullClose(BuyOnce):
    """Buys, then in one later bar submits a partial close *and* a full close.

    Both orders are sized at submission and arrive in order, so the second one asks to close
    a position the first has already halved. It exists to exercise the reduce-only clamp at
    *arrival*, which is the only place the exchange's own clamp can be modelled.
    """

    def on_start(self, ctx):
        super().on_start(ctx)
        self.closed = False

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return
        if not self.done:
            self.entry_bar_close = bar.close_time
            ctx.buy(qty=ctx.money("1"))
            self.done = True
        elif not self.closed and not ctx.position().is_flat:
            self.closed = True
            ctx.close(qty=ctx.money("0.5"))
            ctx.close()


class Flipper(Strategy):
    """Alternates long and flat every five bars -- enough fills to make a prefix test bite."""

    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 3, "datasets": ["klines"]}

    def on_start(self, ctx):
        self.count = 0

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return
        self.count += 1
        if self.count % 10 == 0:
            ctx.buy(qty=ctx.money("0.1"))
        elif self.count % 10 == 5 and not ctx.position().is_flat:
            ctx.close()
        ctx.record("count", self.count)


class LimitOrderStrategy(BuyOnce):
    def on_bar(self, ctx, bar):
        if ctx.warm and not self.done:
            self.done = True
            ctx.buy(qty=ctx.money("1"), type="LIMIT", price=ctx.money("1"))


class TickStrategy(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1, "datasets": ["klines"]}

    def on_tick(self, ctx, trade):
        pass

    def on_bar(self, ctx, bar):
        pass


# ---------------------------------------------------------------------------- harness


def run(
    root: Path,
    strategy: Strategy,
    *,
    start_ms: int = START,
    end_ms: int,
    leverage: int = 10,
    balance: str = "10000",
    latency: int = 120,
    fill_model: MarketFillModel | None = None,
    seed: int = 1,
    mmr: str = "0.004",
):
    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe=strategy.declared.timeframe,
        start_ms=start_ms,
        end_ms=end_ms,
        seed=seed,
        opening_balance=parse_money(balance),
        leverage=leverage,
        latency=FixedLatency(submit=latency, cancel=latency),
        fill_model=fill_model or NO_SLIPPAGE,
    )
    engine = BacktestEngine(
        root=root,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal(mmr))},
    )
    return engine.run()


def fills(result) -> list[dict]:
    return [dict(e.payload) for e in result.events if e.kind == "FILL"]


# ------------------------------------------------------------------------------ fills


def gapped_path(index: int) -> Ohlc:
    """A path where each bar's close is **not** the next bar's open.

    `ramp_path` makes them equal by construction, which is exactly the fixture that cannot
    tell the two candidate fill rules apart: filling at the signal bar's close and filling at
    the next bar's open produce the same number, and a test built on it passes whichever the
    engine does. Here bar *n* opens at `40000 + 100n` and closes 50 above that, so the two
    rules differ by 50 on every trade.
    """
    open_ = int((40_000 + 100 * index) * 10**8)
    close = int((40_050 + 100 * index) * 10**8)
    return Ohlc(open_, close, open_, close)


def test_a_market_order_fills_at_the_next_bar_open_not_the_signal_bar_close(tmp_path: Path) -> None:
    """The causal rule: the fill takes the most recent print *at or before* arrival.

    A signal fires at a bar's close; with any non-zero latency the order lands after the next
    bar has opened, so the fill takes that open -- a price published after the decision. Taking
    the close the strategy just looked at is a fill decided on information it did not have.
    """
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=120, trade_path=gapped_path)
    strategy = BuyOnce()
    result = run(lake, strategy, end_ms=START + 120 * MS_PER_MINUTE)

    executed = fills(result)
    assert len(executed) == 1
    signal_bar = (strategy.entry_bar_close - START) // MS_PER_MINUTE
    next_open = Decimal(40_000 + 100 * (signal_bar + 1))
    signal_close = Decimal(40_050 + 100 * signal_bar)
    assert next_open != signal_close, "the fixture must distinguish the two rules"
    assert Decimal(executed[0]["print_price"]) == next_open


def test_zero_latency_fills_at_the_price_that_triggered_the_order_and_says_so(tmp_path: Path) -> None:
    """Spec 6.3 calls this systematically optimistic; the run is flagged rather than refused.

    On a gapped path the difference is visible as well as flagged: with zero latency the fill
    takes the *signal bar's close*, which is the print the strategy had already seen.
    """
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=120, trade_path=gapped_path)
    strategy = BuyOnce()
    result = run(lake, strategy, end_ms=START + 120 * MS_PER_MINUTE, latency=0)

    signal_bar = (strategy.entry_bar_close - START) // MS_PER_MINUTE
    assert Decimal(fills(result)[0]["print_price"]) == Decimal(40_050 + 100 * signal_bar)
    assert "ZERO_LATENCY" in result.flags
    assert any("upper bound" in w for w in result.warnings)


def test_a_reduce_only_order_is_clamped_to_the_position_it_finds_at_arrival(tmp_path: Path) -> None:
    """Both closes are sized at submission; the second asks to close what the first halved.

    In live the exchange clamps against whatever is there when the order lands. Clamping at
    submission instead would let a reduce-only order *flip* a position that shrank underneath
    it -- opening a short out of a request to close a long.
    """
    lake = tmp_path / "market"
    minutes = 120
    build_lake(lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0))
    result = run(lake, HalfThenFullClose(), end_ms=START + minutes * MS_PER_MINUTE)

    assert result.fills == 3
    assert result.rejects == 0
    # Flat, not short. Without the clamp the third fill is -1 against a held 0.5.
    assert result.attribution.unrealized_pnl == Decimal("0")
    assert result.trades[-1].close_reason == "signal"
    assert not any(trade.side == "SHORT" for trade in result.trades)
    assert Decimal(fills(result)[-1]["qty"]) == Decimal("0.5")


def test_slippage_is_charged_against_the_trader_and_reaches_the_attribution(tmp_path: Path) -> None:
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=120, trade_path=flat_path(40_000.0))
    result = run(
        lake,
        BuyOnce(),
        end_ms=START + 120 * MS_PER_MINUTE,
        fill_model=MarketFillModel(slippage_bps=parse_money("10")),
    )
    # 40 000 x 1.001 = 40 040, a whole number of ticks.
    assert Decimal(fills(result)[0]["price"]) == Decimal("40040.00")
    assert result.attribution.slippage_cost == Decimal("40")
    assert result.attribution.slippage_abs == Decimal("40")


def test_the_trade_series_and_the_mark_series_are_not_interchangeable(tmp_path: Path) -> None:
    """Spec 3.4: fills happen at traded prices, risk happens at mark price.

    The fixture makes them differ by 1 000, so an engine that filled at the mark -- or valued
    the position at the traded price -- produces a number this test can see.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=120,
        trade_path=flat_path(40_000.0),
        mark_path=flat_path(41_000.0),
    )
    result = run(lake, BuyOnce(), end_ms=START + 120 * MS_PER_MINUTE)
    assert Decimal(fills(result)[0]["price"]) == Decimal("40000")
    # One unit bought at 40 000 and marked at 41 000 is 1 000 of unrealised profit.
    assert result.attribution.unrealized_pnl == Decimal("1000")


# --------------------------------------------------------------------------- ordering


def test_funding_settles_before_the_liquidation_check(tmp_path: Path) -> None:
    """Spec 6.2's R5, demonstrated numerically rather than asserted about the priority table.

    Long 1 at 40 000 on 10x: isolated margin 4 000, so with MMR 0.004 the liquidation price
    solves `4000 + (P - 40000) = 0.004P` -> `P = 36 144.58`. The mark sits at 36 400, above
    it, and the position survives.

    A funding payment of 500 is charged against that same allocation (spec 3.5 / R5), which
    moves the price to `(36000 + 500)/0.996 = 36 646.59` -- now *above* the mark. The position
    must be taken on the very next liquidation check. An engine that checked liquidation
    inside the mark handler, before funding, would let it live.
    """
    lake = tmp_path / "market"
    minutes = 240
    drop = 60

    def path(index: int) -> Ohlc:
        high = int(40_000 * 10**8)
        low = int(36_400 * 10**8)
        if index < drop:
            return Ohlc(high, high, high, high)
        if index == drop:
            return Ohlc(high, high, low, low)
        return Ohlc(low, low, low, low)

    # 500 / 36 400, at the storage seam's eight decimals.
    rate = 0.01373626
    build_lake(
        lake,
        start_ms=START,
        minutes=minutes,
        trade_path=path,
        mark_path=path,
        funding=[(START + 120 * MS_PER_MINUTE, rate)],
    )
    result = run(lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE)
    assert result.liquidations == 1
    assert "LIQUIDATED" in result.flags

    # The control: the same lake with no settlement in it. Nothing else changes.
    control_lake = tmp_path / "control"
    build_lake(
        control_lake, start_ms=START, minutes=minutes, trade_path=path, mark_path=path
    )
    control = run(control_lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE)
    assert control.liquidations == 0


def test_funding_sharing_a_millisecond_with_a_mark_still_settles_before_the_check(
    tmp_path: Path,
) -> None:
    """The same rule, in the one case the ordinary data shape cannot produce.

    Real funding settles at `HH:00:00.000` and a 1-minute mark bar closes at `:59.999`, so
    the two never share a timestamp and the R5 test above passes whether the liquidation
    check is *scheduled* at priority 2 or run inline inside the mark handler. Here they are
    deliberately put on the same millisecond, and the difference becomes decisive:

    - Scheduled (correct): mark applied, funding settled, *then* the check probes the bar's
      low against the post-funding liquidation price.
    - Inline: the check consumes the bar's traversed range **before** funding is applied, so
      the low is tested against a liquidation price that is still too far away, and the check
      funding then schedules finds an empty range and sees only the close.

    The mark bar is a spike that recovers: low 36 400, close back at 40 000. Before funding
    `P_liq` is 36 144.58, so the low survives. Funding of 500 against the position's own
    allocation moves it to `(36000 + 500)/0.996 = 36 646.59`, above the low -- so a correct
    engine liquidates and an inline one does not.
    """
    lake = tmp_path / "market"
    minutes = 240
    spike = 120
    top = int(40_000 * 10**8)
    dip = int(36_400 * 10**8)

    def path(index: int) -> Ohlc:
        if index == spike:
            return Ohlc(top, top, dip, top)
        return Ohlc(top, top, top, top)

    settlement = START + (spike + 1) * MS_PER_MINUTE - 1  # the spike bar's close_time
    build_lake(
        lake,
        start_ms=START,
        minutes=minutes,
        trade_path=flat_path(40_000.0),
        mark_path=path,
        # 500 / 40 000: the mark at settlement is the close the mark handler just applied.
        funding=[(settlement, 0.0125)],
    )
    result = run(lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE)
    assert result.liquidations == 1

    control_lake = tmp_path / "control"
    build_lake(
        control_lake,
        start_ms=START,
        minutes=minutes,
        trade_path=flat_path(40_000.0),
        mark_path=path,
    )
    assert run(control_lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE).liquidations == 0


def test_an_intra_bar_mark_excursion_liquidates_even_when_the_close_recovers(tmp_path: Path) -> None:
    """A minute of mark data says the price *reached* the low. Checking only the close misses
    every liquidation the market immediately recovered from -- which is most of them, and
    exactly the ones an equity curve most needs to show."""
    lake = tmp_path / "market"
    minutes = 180
    spike = 120
    top = int(40_000 * 10**8)
    below_liq = int(36_000 * 10**8)  # under the 36 144.58 liquidation price

    def path(index: int) -> Ohlc:
        if index == spike:
            return Ohlc(top, top, below_liq, top)
        return Ohlc(top, top, top, top)

    build_lake(lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0), mark_path=path)
    result = run(lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE)
    assert result.liquidations == 1

    # The control: the same spike, one tick above the liquidation price.
    safe_lake = tmp_path / "safe"
    just_above = int(36_200 * 10**8)

    def safe(index: int) -> Ohlc:
        if index == spike:
            return Ohlc(top, top, just_above, top)
        return Ohlc(top, top, top, top)

    build_lake(safe_lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0), mark_path=safe)
    assert run(safe_lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE).liquidations == 0


def test_a_trades_excursion_sees_the_intrabar_range_not_only_the_close(tmp_path: Path) -> None:
    """Spec 8.3: the MAE distribution is what stops get sized from.

    A stop is hit by the minute's low, not by where the minute happened to finish, so an
    excursion measured on closes alone systematically understates it. The band is safe to
    use here -- unlike in the equity curve -- because MAE and MFE are per trade and
    therefore per symbol: there is no joint state to fabricate.
    """
    lake = tmp_path / "market"
    minutes = 120
    mid = int(40_000 * 10**8)
    dip = int(39_000 * 10**8)
    peak = int(41_000 * 10**8)

    def marks(index: int) -> Ohlc:
        # One minute that traverses 1 000 either way and closes exactly where it opened.
        return Ohlc(mid, peak, dip, mid) if index == 60 else Ohlc(mid, mid, mid, mid)

    build_lake(lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0), mark_path=marks)
    result = run(lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE)

    trade = result.trades[0]
    # One unit held, so the price excursion is 1 000 each way -- and the running total the
    # excursion is measured on is net of the entry commission already paid, which is why
    # both figures sit exactly one fee away from the round number.
    assert trade.mae == -Decimal("1000") - trade.fees
    assert trade.mfe == Decimal("1000") - trade.fees
    assert trade.mae_price == Decimal("39000")
    assert trade.mfe_price == Decimal("41000")
    # The close alone would have shown no excursion at all.
    assert trade.entry_price == Decimal("40000")


def test_the_mark_settles_back_on_the_close_after_probing_the_extremes(tmp_path: Path) -> None:
    """The extremes are evidence, not observations. Leaving one in place would settle the
    next funding payment against a price that was never the mark at any timestamp."""
    lake = tmp_path / "market"
    top = int(41_000 * 10**8)
    mid = int(40_000 * 10**8)
    bottom = int(39_000 * 10**8)
    build_lake(
        lake,
        start_ms=START,
        minutes=120,
        trade_path=flat_path(40_000.0),
        mark_path=lambda i: Ohlc(mid, top, bottom, mid),
    )
    result = run(lake, BuyOnce(), end_ms=START + 120 * MS_PER_MINUTE)
    # Bought one unit at 40 000 and marked at the close of 40 000: no unrealised PnL. Had the
    # high or the low been left standing, this would be +/-1 000.
    assert result.attribution.unrealized_pnl == Decimal("0")


# -------------------------------------------------------------------------- refusals


def test_limit_orders_are_refused_with_a_reason_rather_than_approximated(tmp_path: Path) -> None:
    """Spec 6.4: touching a limit price is not a fill, and a half-modelled queue is fiction.

    Phase 5 implements the queue model, so this is no longer "not yet" -- it is "not at this
    tier". A `BAR_CLOSE` run has no resting size for an order to sit behind, so the rule that
    makes a limit backtest honest is unenforceable and the order is refused rather than
    filled on a touch. `test_golden_scenarios.py` is where it fills, against a book.
    """
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=60)
    with pytest.raises(UnsupportedOrder, match="BAR_CLOSE tier has none"):
        run(lake, LimitOrderStrategy(), end_ms=START + 60 * MS_PER_MINUTE)


def test_a_tick_driven_strategy_is_refused_before_the_lake_is_read(tmp_path: Path) -> None:
    """Running it would call the hook zero times and report a strategy that never traded as
    one that chose not to."""
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=60)
    with pytest.raises(UnsupportedOrder, match="on_tick"):
        run(lake, TickStrategy(), end_ms=START + 60 * MS_PER_MINUTE)


def test_a_symbol_outside_the_declaration_is_refused(tmp_path: Path) -> None:
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=60)
    strategy = BuyOnce()
    config = BacktestConfig(
        symbols=("ETHUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 60 * MS_PER_MINUTE,
    )
    engine = BacktestEngine(
        root=lake,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"ETHUSDT": btcusdt_filters()},
        brackets={"ETHUSDT": single_bracket_table()},
    )
    with pytest.raises(RunAborted, match="declares"):
        engine.run()


def test_an_order_the_account_cannot_fund_is_rejected_not_crashed(tmp_path: Path) -> None:
    """A rejection is data about the strategy's sizing, and it has to survive into the log."""
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=120, trade_path=flat_path(40_000.0))
    result = run(lake, BuyOnce(), end_ms=START + 120 * MS_PER_MINUTE, leverage=1, balance="100")
    assert result.fills == 0
    assert result.rejects == 1
    assert "ORDERS_REJECTED" in result.flags
    reject = next(e for e in result.events if e.kind == "REJECT")
    assert "available" in reject.payload["reason"]


# ------------------------------------------------------------------------- guarantees


def test_no_order_is_placed_before_the_requested_start_date(tmp_path: Path) -> None:
    """The bar count opens the warm-up gate; the timestamp keeps it shut until the range the
    user asked for. Without the second condition a strategy declaring no history would trade
    on a bar from before its own start date."""
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=300, trade_path=ramp_path(40_000.0, 1.0))
    trading_start = START + 100 * MS_PER_MINUTE
    strategy = BuyOnce()
    result = run(
        lake,
        strategy,
        start_ms=trading_start,
        end_ms=START + 300 * MS_PER_MINUTE,
    )
    assert strategy.entry_bar_close >= trading_start
    assert all(e.ts_ms >= trading_start for e in result.events if e.kind == "ORDER")


def test_the_truncated_run_is_a_prefix_of_the_full_one(tmp_path: Path) -> None:
    """The look-ahead test of spec 12.3, on the engine rather than on the indicators.

    Compared by *timestamp* rather than by index. An order in flight at the truncation
    boundary fills against different data in the two runs -- the full run has the next bar's
    open and the truncated one does not -- and that is data availability, not look-ahead. Every
    event at or before the last instant the truncated run could see must be identical.
    """
    lake = tmp_path / "market"
    minutes = 400
    build_lake(lake, start_ms=START, minutes=minutes, trade_path=ramp_path(40_000.0, 3.0))

    full = run(lake, Flipper(), end_ms=START + minutes * MS_PER_MINUTE)
    cut_end = START + (minutes - 100) * MS_PER_MINUTE
    truncated = run(lake, Flipper(), end_ms=cut_end)

    cut = cut_end - 1
    before = [e for e in full.events if e.ts_ms <= cut]
    after = [e for e in truncated.events if e.ts_ms <= cut]
    assert len(after) > 20, "the fixture must produce enough events for this to mean anything"
    assert [(e.kind, e.ts_ms, e.payload) for e in before] == [
        (e.kind, e.ts_ms, e.payload) for e in after
    ]


def test_two_runs_of_the_same_inputs_produce_the_same_event_hash(tmp_path: Path) -> None:
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=300, trade_path=ramp_path(40_000.0, 2.0))
    first = run(lake, Flipper(), end_ms=START + 300 * MS_PER_MINUTE)
    second = run(lake, Flipper(), end_ms=START + 300 * MS_PER_MINUTE)
    assert first.event_hash == second.event_hash
    assert first.fills > 0


def test_a_different_seed_leaves_a_deterministic_strategy_unchanged(tmp_path: Path) -> None:
    """The seed feeds `ctx.rng` and the latency stream. A strategy that draws no randomness
    and a fixed latency model must be unaffected -- otherwise the seed is leaking into
    something it should not touch."""
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=300, trade_path=ramp_path(40_000.0, 2.0))
    a = run(lake, Flipper(), end_ms=START + 300 * MS_PER_MINUTE, seed=1)
    b = run(lake, Flipper(), end_ms=START + 300 * MS_PER_MINUTE, seed=999)
    assert a.event_hash == b.event_hash


def test_an_open_position_is_marked_to_market_and_never_force_closed(tmp_path: Path) -> None:
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=120, trade_path=flat_path(40_000.0), mark_path=flat_path(42_000.0))
    result = run(lake, BuyOnce(), end_ms=START + 120 * MS_PER_MINUTE)
    assert result.fills == 1
    assert result.trades[-1].is_open
    assert result.attribution.unrealized_pnl == Decimal("2000")
    assert result.attribution.realized_pnl == Decimal("0")


def test_a_wrong_slippage_total_is_caught_even_though_the_identity_still_closes(
    tmp_path: Path,
) -> None:
    """Spec 8.4's identity cannot see the slippage term, so something else has to.

    `price_pnl` is defined as the ledger's figure *plus* the signed slippage, so the term
    enters the identity with `+1` and leaves with `-1`. Corrupting the accumulator therefore
    leaves `build_attribution` perfectly happy while the reported price leg moves by the
    whole amount — the difference between "this strategy has an edge" and "this strategy is
    execution noise". The independent reconstruction from the recorded fill prices is what
    catches it.
    """
    lake = tmp_path / "market"
    minutes = 200
    build_lake(lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0))

    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
        leverage=10,
        latency=FixedLatency(120, 120),
        fill_model=MarketFillModel(slippage_bps=parse_money("10")),
    )
    engine = BacktestEngine(
        root=lake,
        strategy=BuyOnce(),
        requirements=BuyOnce().declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table()},
    )
    # Corrupt the accumulator the moment the engine finishes trading, before it reports.
    real_result = engine._result

    def poisoned(processed: int, wall: float):
        engine.slippage_cost = parse_money("0")
        engine.slippage_abs = parse_money("0")
        return real_result(processed, wall)

    engine._result = poisoned  # type: ignore[method-assign]
    with pytest.raises(RunAborted, match="not a measurement"):
        engine.run()


def test_the_ledger_reconciles_against_its_own_event_log(tmp_path: Path) -> None:
    """`Account.reconcile` replays the log from the opening balance. It shares no accumulator
    with the live path, so a fill mis-booked identically into both surfaces here."""
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=400, trade_path=ramp_path(40_000.0, 3.0),
               funding=[(START + i * 60 * MS_PER_MINUTE, 0.0001) for i in range(1, 6)])
    result = run(lake, Flipper(), end_ms=START + 400 * MS_PER_MINUTE)
    assert result.fills > 0
    # `run()` returning at all means `_finalise` reconciled; assert the identity it proves.
    assert result.final_equity == result.opening_balance + result.attribution.net_pnl


class BuyOnFinalBar(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1, "datasets": ["klines"]}

    target: int = 0
    """Set on the instance before the run. A class attribute rather than something `on_start`
    initialises, because `on_start` runs *after* construction and would overwrite it."""

    def on_start(self, ctx):
        self.submitted = None

    def on_bar(self, ctx, bar):
        if ctx.warm and bar.close_time == self.target:
            self.submitted = bar.close_time
            ctx.buy(qty=ctx.money("1"))


def test_an_order_submitted_on_the_last_bar_still_fills(tmp_path: Path) -> None:
    """Its arrival is past `end_ms`, and it fills anyway -- at the last print, which is known.

    The order was legitimately placed and would have filled in reality; cancelling it would
    model a protection that does not exist. Nothing downstream mishandles the resulting
    sample past the range end: the series stays sorted, the ledger still reconciles, and the
    return grid -- whose boundaries stop at `end_ms` -- simply does not reach it.
    """
    lake = tmp_path / "market"
    minutes = 60
    end_ms = START + minutes * MS_PER_MINUTE
    build_lake(lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0))

    strategy = BuyOnFinalBar()
    strategy.target = end_ms - 1
    result = run(lake, strategy, end_ms=end_ms)

    assert strategy.submitted == end_ms - 1
    assert result.fills == 1 and result.rejects == 0
    assert result.equity_ms[-1] > end_ms
    assert list(result.equity_ms) == sorted(result.equity_ms)
    # Opened after the range closed, so it was held for none of it.
    assert result.metrics.exposure == 0.0
    fill = fills(result)[0]
    assert Decimal(fill["price"]) == Decimal("40000")


def test_the_equity_series_carries_the_whole_run(tmp_path: Path) -> None:
    lake = tmp_path / "market"
    minutes = 200
    build_lake(lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0))
    result = run(lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE)
    assert len(result.equity) == len(result.equity_ms) == len(result.position_open)
    assert result.equity_ms == tuple(sorted(result.equity_ms))
    assert result.equity[0] == float(result.opening_balance)
    assert any(result.position_open)


def test_a_run_with_no_timeout_budget_gets_an_infinite_deadline_not_an_expired_one(
    tmp_path: Path,
) -> None:
    """Spec 2.3's wall-clock budget belongs to a backtest worker, and `timeout_s <= 0` is how
    a run says it has none -- a paper session asked to hold a book for forty-eight hours
    passes zero, and must not inherit the fifteen-minute ceiling a backtest defaults to.

    The value is asserted rather than only the outcome because the failure is timing-shaped
    and would otherwise hide. Read as plain arithmetic, `timeout_s = 0` makes the deadline the
    start instant itself, which every subsequent `perf_counter` reading is already past: the
    first `_checkpoint`, `PROGRESS_EVERY` events into the loop, raises `RunAborted` over a
    budget nobody set. A run short enough to finish before its own first checkpoint would
    survive that arithmetic, so the fixture is deliberately long enough to reach one.
    """
    lake = tmp_path / "market"
    minutes = 600
    build_lake(lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0))

    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
        leverage=10,
        latency=FixedLatency(120, 120),
        fill_model=NO_SLIPPAGE,
        timeout_s=0.0,
    )
    engine = BacktestEngine(
        root=lake,
        strategy=BuyOnce(),
        requirements=BuyOnce().declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table()},
    )
    result = engine.run()

    assert engine._deadline == float("inf")
    # A deadline nothing ever compares against would prove nothing, so the run must have
    # passed at least one checkpoint on its way to here.
    assert result.engine_events > PROGRESS_EVERY
    assert result.fills == 1
