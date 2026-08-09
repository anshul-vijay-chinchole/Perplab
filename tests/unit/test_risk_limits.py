"""Phase 6's exit criterion: *every limit demonstrably halts a misbehaving strategy*.

Spec 13 words it as one sentence, and the sentence has two halves that are easy to conflate.
A limit that *rejects* has to refuse the order and let the run continue; a limit that
*halts* has to stop the run and close out. Spec 7's table says which is which, and a test
suite that only checked "something happened" would pass on an engine that had them backwards
-- which would be the worse failure of the two, because a strategy whose orders are silently
rejected looks like a strategy that decided not to trade.

**Each limit gets a strategy written to breach exactly it.** Not one strategy driven by a
flag: the point is that the misbehaviour is visible in the test body, next to the number the
limit is set to, so a reader can check the arithmetic without running anything. Where a
breach depends on a price path, the path is written into the lake by the test.

**The two failure modes named in the brief get their own sections.** The
order-of-operations bug -- a size limit evaluated against the position as it stands rather
than the position the order would produce -- and the daily-loss reset boundary are both
places where a limit can look implemented and do nothing, so both are tested for the
specific arithmetic that distinguishes a working limit from a decorative one.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.money import parse_money
from perplab.core.risk import (
    MS_PER_DAY,
    KillSwitch,
    RiskAction,
    RiskEngine,
    RiskLimits,
    WorkingExposure,
    projected_exposure,
)
from perplab.engine.backtest import BacktestConfig, BacktestEngine
from perplab.engine.latency import FixedLatency
from perplab.strategy.base import Strategy
from perplab.strategy.context import FillTier
from tests.engine_lake import MS_PER_MINUTE, build_lake, flat_path, ramp_path
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000  # 2024-03-01T00:00:00Z, exactly a UTC midnight
LATENCY = 10


def money(text: str) -> Decimal:
    return parse_money(text)


# ------------------------------------------------------------------------- strategies


class Base(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }


class BuyEveryBar(Base):
    """Adds `qty` on every warm bar, forever. Breaches any size or rate limit eventually."""

    def __init__(self, qty: str = "1", bars: int | None = None) -> None:
        super().__init__({})
        self._qty = money(qty)
        self._bars = bars
        self.submitted = 0

    def on_bar(self, ctx, bar) -> None:
        if not ctx.warm:
            return
        if self._bars is not None and self.submitted >= self._bars:
            return
        self.submitted += 1
        ctx.buy(qty=self._qty)


class BuyOnce(Base):
    """One position, held. The price path is what breaches the account limits."""

    def __init__(self, qty: str = "1") -> None:
        super().__init__({})
        self._qty = money(qty)
        self._done = False

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and not self._done:
            self._done = True
            ctx.buy(qty=self._qty)


class BurstInOneBar(Base):
    """Submits `count` separate orders inside a single bar.

    The order-of-operations case: each order on its own is well inside the limit, and the
    account they would jointly produce is not.
    """

    def __init__(self, count: int, qty: str = "1") -> None:
        super().__init__({})
        self._count = count
        self._qty = money(qty)
        self.ids: list[str] = []

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and not self.ids:
            for _ in range(self._count):
                self.ids.append(ctx.buy(qty=self._qty))


class RestManyLimits(Base):
    """Rests `count` far-from-market limit orders, to breach `max_open_orders`."""

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines", "bookTicker"],
    }

    def __init__(self, count: int) -> None:
        super().__init__({})
        self._count = count
        self.ids: list[str] = []

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and not self.ids:
            for index in range(self._count):
                self.ids.append(
                    ctx.buy(
                        qty=money("0.01"),
                        type="LIMIT",
                        # Just under the 39 999.9 bid, so each one rests rather than
                        # crossing -- and inside `PERCENT_PRICE`, which a 30 000 bid
                        # against a 40 000 market is not.
                        price=money("39990") - money(str(index)),
                    )
                )


class RoundTripEveryBar(Base):
    """Opens and closes on alternate bars, so every round-trip is a closed trade.

    Against a falling price path each one loses, which is what `max_consecutive_losses`
    counts.
    """

    def __init__(self) -> None:
        super().__init__({})
        self._long = False

    def on_bar(self, ctx, bar) -> None:
        if not ctx.warm:
            return
        if self._long:
            ctx.close()
            self._long = False
        else:
            ctx.buy(qty=money("1"))
            self._long = True


# ---------------------------------------------------------------------------- harness


def run(
    root: Path,
    strategy: Strategy,
    *,
    risk: RiskLimits,
    tier: FillTier = FillTier.BAR_CLOSE,
    minutes: int = 10,
    balance: str = "100000",
    leverage: int = 1,
    end_ms: int | None = None,
    kill_switch_flatten: bool = False,
    auto_flatten=None,
):
    from perplab.engine.backtest import AutoFlatten

    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=END if (END := end_ms) else START + minutes * MS_PER_MINUTE,
        seed=1,
        opening_balance=money(balance),
        leverage=leverage,
        latency=FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=tier,
        risk=risk,
        kill_switch_flatten=kill_switch_flatten,
        auto_flatten=auto_flatten or AutoFlatten(),
    )
    engine = BacktestEngine(
        root=root,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
    )
    return engine.run()


def breaches(result, limit: str) -> list:
    return [b for b in result.risk_breaches if b.limit == limit]


def kinds(result, kind: str) -> list[dict]:
    return [dict(e.payload) for e in result.events if e.kind == kind]


# ===================================================== REJECT limits: the order is refused


def test_max_position_notional_refuses_the_order_that_would_breach_it(tmp_path: Path) -> None:
    """A flat 40 000 tape and a 50 000 notional ceiling.

    Each order is 1 BTC = 40 000, so the first is inside the limit and the second -- which
    would produce a 2 BTC position worth 80 000 -- is not. The run continues: this is a
    `REJECT`, so the strategy keeps its 1 BTC and keeps being told no.
    """
    build_lake(tmp_path, start_ms=START, minutes=10, trade_path=flat_path(40_000.0))
    strategy = BuyEveryBar(qty="1")
    result = run(
        tmp_path,
        strategy,
        risk=RiskLimits(
            max_position_notional=money("50000"),
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )

    assert result.halt_reason is None, "a notional breach rejects, it does not halt"
    assert result.fills == 1, "exactly one order got through"
    refusals = breaches(result, "max_position_notional")
    assert len(refusals) >= 1
    assert all(b.action is RiskAction.REJECT for b in refusals)
    # 2 BTC x 40 000 is the projection that was refused, not the 1 BTC actually held.
    assert refusals[0].observed == "80000.00000000"
    assert refusals[0].allowed == "50000.00000000"
    assert "RISK_REJECTED" in result.flags


def test_the_size_limit_counts_orders_still_in_flight(tmp_path: Path) -> None:
    """The order-of-operations bug, in the form that survives the obvious fix.

    Five 1 BTC orders are submitted inside one bar, before any of them has arrived. A limit
    checked against *the position* passes all five -- the position is flat when each is
    written -- and the account ends up holding 5 BTC under a 2 BTC ceiling.

    With a 100 000 ceiling against a 40 000 tape, exactly two orders fit (80 000) and the
    third projects 120 000 -- two in flight plus itself.

    All three refusals report **120 000**, not a climbing 120/160/200. A rejected order is
    not in flight: it will never reach the book, so counting it against the fourth order
    would refuse that one for exposure that does not exist. This is the distinction between
    "orders submitted" and "orders that can still fill", and only the second one is what a
    size limit is about.
    """
    build_lake(tmp_path, start_ms=START, minutes=10, trade_path=flat_path(40_000.0))
    strategy = BurstInOneBar(count=5, qty="1")
    result = run(
        tmp_path,
        strategy,
        risk=RiskLimits(
            max_position_notional=money("100000"),
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )

    refusals = breaches(result, "max_position_notional")
    assert [b.observed for b in refusals] == ["120000.00000000"] * 3
    assert result.fills == 2
    assert result.risk_rejects == 3


def test_a_reduce_only_order_is_never_refused_for_size(tmp_path: Path) -> None:
    """Spec 7's limits bound *exposure*, and a reduce-only order can only lower it.

    1 BTC is bought at 40 000 under a 45 000 ceiling, which admits it. The ramp adds 1 000
    a minute, so by the eleventh warm bar the mark is 51 000 and the position is worth more
    than the ceiling allows -- without the strategy having done anything. From there:

    - a new *opening* order is refused: 2 BTC x 51 000 = 102 000 against a 45 000 ceiling;
    - the *reduce-only* exit still goes through.

    Refusing the exit would trap the account above a limit, holding the very exposure the
    limit exists to prevent. That is the failure this test exists to prevent, and it is a
    tempting one to write: the exit is, arithmetically, an order on a symbol whose position
    already breaches the ceiling.
    """
    build_lake(
        tmp_path, start_ms=START, minutes=12, trade_path=ramp_path(40_000.0, 1_000.0)
    )

    class OpenRiseThenClose(Base):
        def on_start(self, ctx) -> None:
            self.stage = 0
            self.blocked = None

        def on_bar(self, ctx, bar) -> None:
            if not ctx.warm:
                return
            self.stage += 1
            if self.stage == 1:
                ctx.buy(qty=money("1"))
            elif self.stage == 11:
                # Mark is 51 000; this projects 102 000 against a 45 000 ceiling.
                self.blocked = ctx.buy(qty=money("1"))
                ctx.close()

    strategy = OpenRiseThenClose()
    result = run(
        tmp_path,
        strategy,
        minutes=12,
        risk=RiskLimits(
            max_position_notional=money("45000"),
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    refusals = breaches(result, "max_position_notional")
    assert len(refusals) == 1, "the opening order was refused"
    assert refusals[0].observed == "102000.00000000"
    assert result.fills == 2, "the entry filled, and so did the reduce-only exit"
    closed = [t for t in result.trades if t.close_reason != "open"]
    assert len(closed) == 1


def test_max_leverage_is_checked_against_equity_not_against_wallet(tmp_path: Path) -> None:
    """5x against 100 000 of equity permits 500 000 of notional and no more.

    At 40 000, that is 12.5 BTC. An order for 13 projects 520 000, which is 5.2x.
    """
    build_lake(tmp_path, start_ms=START, minutes=10, trade_path=flat_path(40_000.0))
    result = run(
        tmp_path,
        BuyEveryBar(qty="13", bars=1),
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=Decimal(5),
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    refusals = breaches(result, "max_leverage")
    assert len(refusals) == 1
    assert refusals[0].observed == "5.20000000"
    assert result.fills == 0


def test_max_open_orders_counts_what_is_live_not_what_was_ever_sent(tmp_path: Path) -> None:
    """Three resting limits allowed, and the fourth refused while they are still live."""
    quotes = [(START + s * 1000, 39_999.9, 10.0, 40_000.1, 10.0) for s in range(600)]
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=10,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(600)],
        quotes=quotes,
    )
    result = run(
        tmp_path,
        RestManyLimits(count=6),
        tier=FillTier.BOOK_TICKER,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=3,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    refusals = breaches(result, "max_open_orders")
    assert len(refusals) == 3, "six asked for, three admitted, three refused"
    # The count the fourth order *would* have produced. Reporting the pre-order 3 against a
    # limit of 3 read as a value that did not exceed the limit it was refused for.
    assert refusals[0].observed == "4"


def test_max_orders_per_minute_is_a_rolling_window_not_a_calendar_minute(
    tmp_path: Path,
) -> None:
    """Ten submissions inside one bar against a ceiling of four.

    All ten are stamped at the same millisecond, so the window holds every earlier one.
    Four are admitted and six refused -- and the refusals count *accepted* submissions only,
    which is why the sixth refusal still reports 5 rather than climbing to 10.
    """
    build_lake(tmp_path, start_ms=START, minutes=10, trade_path=flat_path(40_000.0))
    result = run(
        tmp_path,
        BurstInOneBar(count=10, qty="0.01"),
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=4,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    refusals = breaches(result, "max_orders_per_minute")
    assert len(refusals) == 6
    assert {b.observed for b in refusals} == {"5"}
    assert result.fills == 4


# =========================================================== HALT limits: the run stops


def test_max_drawdown_halts_on_mark_to_market_not_on_realised_pnl(tmp_path: Path) -> None:
    """Spec 7: *"a strategy sitting in a 40% unrealised loss is in a 40% drawdown"*.

    1 BTC bought at 40 000 against 100 000 of equity. A 10% limit breaches when equity
    falls to 90 000, which is a 10 000 loss on the position -- a mark of 30 000. The
    strategy never closes, so realised PnL stays at zero throughout: an implementation that
    scored closed trades would report no drawdown at all and run to the end.
    """
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=20,
        trade_path=ramp_path(40_000.0, -1_000.0),
    )
    result = run(
        tmp_path,
        BuyOnce(qty="1"),
        minutes=20,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=Decimal("0.10"),
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    assert result.halt_reason is not None
    assert result.halt_reason.limit == "max_drawdown"
    assert result.halt_reason.action is RiskAction.HALT
    assert "RISK_HALTED" in result.flags
    assert len(kinds(result, "KILL_SWITCH")) == 1
    # Realised PnL is zero: the trade was never closed by the strategy.
    assert all(t.close_reason == "open" for t in result.trades)


def test_min_equity_halts_and_the_kill_switch_records_the_trigger(tmp_path: Path) -> None:
    """A 50% floor on 100 000 breaches at 50 000, which a 2 BTC long reaches at 15 000."""
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=40,
        trade_path=ramp_path(40_000.0, -1_000.0),
    )
    result = run(
        tmp_path,
        BuyOnce(qty="2"),
        minutes=40,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=Decimal("0.50"),
            max_consecutive_rejections=None,
        ),
    )
    assert result.halt_reason is not None
    assert result.halt_reason.limit == "min_equity"
    assert result.risk_summary["kill_switch"]["trigger"] == "min_equity"
    assert result.risk_summary["kill_switch"]["tripped_at_ms"] is not None


def test_halt_on_liquidation_stops_immediately(tmp_path: Path) -> None:
    """20x leverage against a 0.4% maintenance rate liquidates on a ~5% adverse move."""
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=30,
        trade_path=ramp_path(40_000.0, -500.0),
    )
    result = run(
        tmp_path,
        BuyOnce(qty="45"),
        minutes=30,
        balance="100000",
        leverage=20,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            halt_on_liquidation=True,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    assert result.liquidations >= 1
    assert result.halt_reason is not None
    assert result.halt_reason.limit == "halt_on_liquidation"
    assert result.risk_summary["kill_switch"]["trigger"] == "LIQUIDATION"


def test_max_consecutive_losses_counts_closed_round_trips(tmp_path: Path) -> None:
    """A falling tape and a strategy that buys and sells on alternate bars.

    Every round-trip loses, so the third close breaches a limit of three. The count is of
    *closed* trades: an open position at a loss does not extend the streak.
    """
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=20,
        trade_path=ramp_path(40_000.0, -100.0),
    )
    result = run(
        tmp_path,
        RoundTripEveryBar(),
        minutes=20,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            max_consecutive_losses=3,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    assert result.halt_reason is not None
    assert result.halt_reason.limit == "max_consecutive_losses"
    assert result.halt_reason.observed == "3"
    closed = [t for t in result.trades if t.close_reason != "open"]
    assert len(closed) == 3
    assert all(t.net_pnl < 0 for t in closed)


def test_repeated_rejections_trip_the_kill_switch(tmp_path: Path) -> None:
    """Spec 7's auto-trigger, driven by the exchange refusing rather than the risk layer.

    A 20 BTC order against a `MARKET_LOT_SIZE` the fixture caps below it is refused by the
    filter at arrival, every time. Three in a row trips the switch -- and the trigger is
    recorded as the rejection limit, not as one of the size limits, which are all off.
    """
    build_lake(tmp_path, start_ms=START, minutes=10, trade_path=flat_path(40_000.0))
    result = run(
        tmp_path,
        BuyEveryBar(qty="2000"),
        balance="100000000",
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=3,
        ),
    )
    assert result.rejects >= 3
    assert result.halt_reason is not None
    assert result.halt_reason.limit == "max_consecutive_rejections"


def test_a_risk_rejection_does_not_count_towards_the_rejection_auto_trigger(
    tmp_path: Path,
) -> None:
    """The trigger must not fire on the risk layer's own verdicts.

    Otherwise a strategy that repeatedly asks for too much size trips the kill switch
    instead of simply being told no, which is the outcome `REJECT` exists to prevent. Ten
    refusals against a limit of three, and the run finishes.
    """
    build_lake(tmp_path, start_ms=START, minutes=15, trade_path=flat_path(40_000.0))
    result = run(
        tmp_path,
        BuyEveryBar(qty="1"),
        minutes=15,
        risk=RiskLimits(
            max_position_notional=money("100"),
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=3,
        ),
    )
    assert result.risk_rejects >= 4
    assert result.rejects == 0
    assert result.halt_reason is None


# ================================================================ the daily-loss boundary


def test_the_daily_loss_baseline_rolls_at_utc_midnight(tmp_path: Path) -> None:
    """The boundary case, checked on the unit rather than through a two-day backtest.

    Equity of 10 000 at the run's start, down to 9 900 by the end of day one. A 2% limit is
    200, so the day-one loss of 100 does not breach. The instant the clock reaches the next
    UTC midnight the baseline becomes 9 900, and a further loss of 200 -- to 9 700 -- does
    breach, even though the *run* is now 300 down. Measuring from the run's start instead
    would have breached at 9 800, half a day early.

    `MS_PER_DAY` is the boundary itself, and a sample stamped exactly on it belongs to the
    new day: `ts // MS_PER_DAY` changes at `00:00:00.000`, not at `00:00:00.001`.
    """
    engine = RiskEngine(
        limits=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=Decimal("0.02"),
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
        starting_equity=money("10000"),
    )
    day0 = 5 * MS_PER_DAY

    assert engine.observe_equity(day0, money("10000")) is None
    assert engine.observe_equity(day0 + 1000, money("9900")) is None, "100 < 200"
    assert engine.day_key == day0 // MS_PER_DAY

    # The first sample of the new day. The baseline becomes the **last sample of the old
    # day** -- 9 900 -- carried forward, not the 9 850 observed here. Everything between
    # the two samples happened on the new day's watch.
    assert engine.observe_equity(day0 + MS_PER_DAY, money("9850")) is None
    assert engine.day_open_equity == money("9900")
    assert engine.observe_equity(day0 + MS_PER_DAY + 1, money("9750")) is None, "150 < 200"

    breach = engine.observe_equity(day0 + MS_PER_DAY + 2, money("9700"))
    assert breach is not None
    assert breach.limit == "max_daily_loss"
    assert breach.observed == "200.00000000"
    assert breach.allowed == "200.00000000", "2% of the run's start, not of the day's"


def test_the_daily_threshold_is_a_fixed_amount_from_the_runs_own_start() -> None:
    """A losing day does not tighten the next day's leash.

    After a day that ends at 5 000 from a 10 000 start, the limit is still 200 -- 2% of
    10 000. Read as "2% of today's equity" it would be 100, and the strategy would be
    halted by an arithmetic reading rather than by a decision anyone made.
    """
    engine = RiskEngine(
        limits=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=Decimal("0.02"),
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
        starting_equity=money("10000"),
    )
    day0 = 5 * MS_PER_DAY
    # Fifty days losing 100 each -- every one of them inside the 200 limit, and the account
    # halves. A single 5 000 step would breach on the first day and the test would never
    # reach the question it is asking.
    equity = 10_000
    engine.observe_equity(day0, money("10000"))
    for day in range(1, 51):
        equity -= 100
        engine.observe_equity(day0 + day * MS_PER_DAY, money(str(equity)))
    assert equity == 5_000
    assert engine.halted is False

    # Equity is 5 000; the limit is still 2% of 10 000.
    assert engine.max_daily_loss == money("200")
    next_day = day0 + 51 * MS_PER_DAY
    assert engine.observe_equity(next_day, money("5000")) is None
    assert engine.observe_equity(next_day + 1, money("4850")) is None, "150 < 200"
    assert engine.observe_equity(next_day + 2, money("4800")) is not None


def test_a_day_with_no_samples_is_skipped_rather_than_interpolated() -> None:
    """A data hole spanning two days rolls once, to the day the next sample lands in.

    The alternative -- advancing one day per gap -- would invent baselines for days the
    account has no observation of, and the invented one would be whatever the last sample
    before the hole happened to be.
    """
    engine = RiskEngine(
        limits=RiskLimits.unlimited(),
        starting_equity=money("10000"),
    )
    day0 = 5 * MS_PER_DAY
    engine.observe_equity(day0, money("10000"))
    engine.observe_equity(day0 + 3 * MS_PER_DAY, money("8000"))
    assert engine.day_key == (day0 + 3 * MS_PER_DAY) // MS_PER_DAY
    # The baseline is the last thing actually observed, which is the 10 000 from day zero.
    # Using the 8 000 seen here would hand the skipped days' losses to nobody.
    assert engine.day_open_equity == money("10000")


# ======================================================== projection and exposure bounds


def test_projected_exposure_bounds_the_path_not_only_the_endpoint() -> None:
    """Long 10, a working buy of 5, and a new sell of 20.

    Netted, that is |10 + 5 - 20| = 5. But if the buy fills first the account holds 15
    before the sell lands, and 15 is the number the margin has to survive. The bound is
    `max(|position + buys|, |position - sells|)` = max(15, 10) = 15.
    """
    working = WorkingExposure(buy_qty=Decimal(5), sell_qty=Decimal(20))
    assert projected_exposure(Decimal(10), working) == Decimal(15)


def test_a_short_projects_the_same_bound_mirrored() -> None:
    working = WorkingExposure(buy_qty=Decimal(20), sell_qty=Decimal(5))
    assert projected_exposure(Decimal(-10), working) == Decimal(15)


def test_reduce_only_orders_do_not_offset_projected_exposure() -> None:
    """A stop-loss under a long must not license more size.

    `working_exposure` drops reduce-only orders rather than netting them, so a long of 10
    with a 10 reduce-only sell resting still projects 10 -- not zero.
    """
    from perplab.core.risk import working_exposure

    exposure = working_exposure(
        [("SELL", Decimal(10), True), ("BUY", Decimal(2), False)]
    )
    assert exposure.sell_qty == Decimal(0)
    assert exposure.buy_qty == Decimal(2)
    assert projected_exposure(Decimal(10), exposure) == Decimal(12)


# ============================================================== kill switch and halting


def test_the_kill_switch_defaults_to_cancel_only(tmp_path: Path) -> None:
    """Spec 7.3: force-closing at market during a crash can be worse than the exposure.

    So a halt cancels the book and leaves the position, and the `KILL_SWITCH` event says
    which positions are still open. Anyone reading it can see the exposure was not closed.
    """
    build_lake(
        tmp_path, start_ms=START, minutes=20, trade_path=ramp_path(40_000.0, -1_000.0)
    )
    result = run(
        tmp_path,
        BuyOnce(qty="1"),
        minutes=20,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=Decimal("0.10"),
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    event = kinds(result, "KILL_SWITCH")[0]
    assert event["flatten"] is False
    assert event["flattened"] == []
    assert event["open_after"] == ["BTCUSDT"]


def test_arming_flatten_closes_the_position_and_says_so(tmp_path: Path) -> None:
    """The opt-in behaviour, and the exit is a real market order with fees.

    Not a mark-price adjustment: the `FORCE_FLATTEN` order goes through the ordinary fill
    path, so the run's own fill count includes it.
    """
    build_lake(
        tmp_path, start_ms=START, minutes=20, trade_path=ramp_path(40_000.0, -1_000.0)
    )
    result = run(
        tmp_path,
        BuyOnce(qty="1"),
        minutes=20,
        kill_switch_flatten=True,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=Decimal("0.10"),
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    event = kinds(result, "KILL_SWITCH")[0]
    assert event["flatten"] is True
    assert event["flattened"] == ["BTCUSDT"]
    assert event["open_after"] == []
    assert len(kinds(result, "FORCE_FLATTEN")) == 1
    closed = [t for t in result.trades if t.close_reason != "open"]
    assert len(closed) == 1


def test_a_halted_run_refuses_further_orders(tmp_path: Path) -> None:
    """`on_stop` still runs, and anything it submits is refused with the halt as the reason.

    A strategy that flattens in `on_stop` is doing something legitimate, so the hook is not
    skipped -- but the account is in the state the operator said to stop at, and letting
    the order through would be the halt failing to halt.
    """
    build_lake(
        tmp_path, start_ms=START, minutes=20, trade_path=ramp_path(40_000.0, -1_000.0)
    )

    class FlattensOnStop(Base):
        def on_start(self, ctx) -> None:
            self.opened = False
            self.stop_order = None

        def on_bar(self, ctx, bar) -> None:
            if ctx.warm and not self.opened:
                self.opened = True
                ctx.buy(qty=money("1"))

        def on_stop(self, ctx) -> None:
            if not ctx.position().is_flat:
                self.stop_order = ctx.close()

    strategy = FlattensOnStop()
    result = run(
        tmp_path,
        strategy,
        minutes=20,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=Decimal("0.10"),
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    assert result.halt_reason is not None
    assert strategy.stop_order is not None, "on_stop ran"
    refusals = breaches(result, "halted")
    assert len(refusals) == 1
    assert refusals[0].action is RiskAction.REJECT


def test_the_kill_switch_requires_an_explicit_unarm() -> None:
    """Spec 7.6. A tripped switch refuses to let a session start until somebody clears it."""
    from perplab.core.risk import KillSwitchArmed

    switch = KillSwitch()
    switch.require_clear()  # clean: no exception
    assert switch.trip(1_000, "INVARIANT", "I1 failed") is True
    assert switch.trip(2_000, "LIQUIDATION", "later") is False, "first trigger wins"
    assert switch.trigger == "INVARIANT"
    with pytest.raises(KillSwitchArmed):
        switch.require_clear()
    switch.unarm()
    switch.require_clear()


def test_an_invariant_failure_trips_the_switch_and_refuses_to_report(
    tmp_path: Path, monkeypatch
) -> None:
    """Spec 7's first auto-trigger, and the reason it re-raises rather than halting quietly.

    If the ledger's own arithmetic disagreed with itself, every number the run computed came
    from state that has just been proved untrustworthy. Publishing a `BacktestResult` would
    publish an equity curve the platform knows is wrong.
    """
    from perplab.core.invariants import InvariantViolation

    build_lake(tmp_path, start_ms=START, minutes=10, trade_path=flat_path(40_000.0))

    class Boom(Base):
        def on_bar(self, ctx, bar) -> None:
            if ctx.warm:
                raise InvariantViolation("I1", "wallet conservation failed")

    strategy = Boom()
    from perplab.engine.backtest import AutoFlatten

    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 10 * MS_PER_MINUTE,
        seed=1,
        opening_balance=money("100000"),
        latency=FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=FillTier.BAR_CLOSE,
        risk=RiskLimits.unlimited(),
        auto_flatten=AutoFlatten(),
    )
    engine = BacktestEngine(
        root=tmp_path,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
    )
    with pytest.raises(InvariantViolation):
        engine.run()
    assert engine.risk.kill_switch.tripped
    assert engine.risk.kill_switch.trigger == "INVARIANT"
    assert any(e.kind == "KILL_SWITCH" for e in engine.runtime.events)


# =========================================================================== defaults


def test_the_engines_default_is_no_limits_and_it_says_so(tmp_path: Path) -> None:
    """A run that nobody set limits on must not silently acquire spec 7's table.

    Every Phase 4 and Phase 5 run stored an empty `risk_limits`, and reading that back as
    the defaults would rewrite their history: a run that never rejected an order would be
    reported as one that ran under a 5x cap and happened never to reach it.
    """
    assert RiskLimits.from_json({}) == RiskLimits.unlimited()
    assert RiskLimits.from_json(None) == RiskLimits.unlimited()
    assert RiskLimits.unlimited().unbounded_exposure is True
    assert RiskLimits().unbounded_exposure is False, "the 5x default bounds it"

    build_lake(tmp_path, start_ms=START, minutes=10, trade_path=flat_path(40_000.0))
    result = run(tmp_path, BuyEveryBar(qty="1"), risk=RiskLimits.unlimited())
    assert "RISK_UNBOUNDED" in result.flags
    assert result.risk_rejects == 0


def test_a_misspelled_limit_is_refused_rather_than_ignored() -> None:
    """A limit nobody validates is a limit that silently does nothing."""
    with pytest.raises(ValueError, match="unknown risk limit"):
        RiskLimits.from_json({"max_postion_notional": "1000"})


def test_a_percentage_written_as_a_percent_is_refused() -> None:
    """`2` meaning 2% would be read as 200% and never fire."""
    with pytest.raises(ValueError, match="fraction"):
        RiskLimits(max_daily_loss_pct=Decimal(2))


def test_a_loss_that_straddles_midnight_is_not_lost_between_two_days() -> None:
    """The reason the baseline is carried forward rather than re-read.

    Equity samples land at bar closes, so a UTC day's first observation is a whole bar
    *after* midnight. Baselining on it made the bar that straddles the boundary belong to
    neither day: 99 976 at 23:59:59.999, 95 976 at 00:00:59.999, a 4 000 loss against a
    2 000 limit, and no halt. Carried forward, the new day opens at 99 976 and the very
    first sample of the day breaches.
    """
    engine = RiskEngine(
        limits=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=Decimal("0.02"),
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
        starting_equity=money("100000"),
    )
    midnight = 5 * MS_PER_DAY
    assert engine.observe_equity(midnight - 1, money("99976")) is None
    breach = engine.observe_equity(midnight + 59_999, money("95976"))
    assert breach is not None
    assert breach.limit == "max_daily_loss"
    assert breach.observed == "4000.00000000"
    assert breach.allowed == "2000.00000000"


def test_an_out_of_order_sample_cannot_reset_the_day() -> None:
    """The day advances; it never goes back.

    A late or replayed sample stamped yesterday used to re-base today's baseline to
    yesterday's equity, and today's accumulated loss vanished. Unreachable through the
    backtest's ordered queue, and ordinary in live -- which is the mode spec 7's first
    sentence says this module exists to share.
    """
    engine = RiskEngine(limits=RiskLimits.unlimited(), starting_equity=money("10000"))
    day5 = 5 * MS_PER_DAY
    engine.observe_equity(day5, money("10000"))
    engine.observe_equity(day5 + MS_PER_DAY, money("10000"))
    engine.observe_equity(day5 + MS_PER_DAY + 1000, money("9900"))
    engine.observe_equity(day5 + 5000, money("9000"))  # a late day-5 sample

    assert engine.day_key == (day5 + MS_PER_DAY) // MS_PER_DAY
    assert engine.day_open_equity == money("10000")


def test_the_drawdown_peak_rises_on_the_crest_not_the_trough() -> None:
    """Drawdown is the fall from the best point to the worst, within and across samples.

    A sample covers an interval and carries both. Feeding the trough to the peak as well
    understated every drawdown by exactly the intrabar range: here a band of 90 000..120 000
    followed by a fall to 100 000 is a 16.67% drawdown from the 120 000 crest, and is 0%
    if the peak only ever saw the 90 000 trough.
    """
    engine = RiskEngine(
        limits=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=Decimal("0.15"),
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
        starting_equity=money("100000"),
    )
    # The crest is 120 000 and the trough of the same sample is 110 000 -- an 8.33% fall,
    # inside the limit. The peak must still rise to 120 000.
    assert engine.observe_equity(1_000, money("110000"), high=money("120000")) is None
    assert engine.peak_equity == money("120000")
    # 100 000 is 16.67% below that crest, and 0% below the 110 000 trough. An engine whose
    # peak only ever saw troughs reports no drawdown at all here.
    breach = engine.observe_equity(2_000, money("100000"), high=money("100000"))
    assert breach is not None
    assert breach.limit == "max_drawdown"
    assert breach.allowed == "0.15000000"


def test_a_reduce_only_exit_survives_the_count_limits() -> None:
    """`max_open_orders` and the rate guard must not refuse the orders that reduce risk.

    Four resting quotes under a ceiling of four, then a reduce-only exit. Refusing it
    would leave the account holding the exposure the ceiling exists to bound -- and the
    platform's own `AutoFlatten` reaches the same wall.
    """
    engine = RiskEngine(
        limits=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=4,
            max_orders_per_minute=2,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
        starting_equity=money("100000"),
    )
    common = dict(
        ts_ms=1_000,
        symbol="BTCUSDT",
        price=money("40000"),
        position_qty=money("1"),
        working=WorkingExposure.zero(),
        equity=money("100000"),
    )
    assert engine.check_order(side="BUY", qty=money("0.01"), reduce_only=False, open_orders=0, **common) is None
    assert engine.check_order(side="BUY", qty=money("0.01"), reduce_only=False, open_orders=1, **common) is None
    # Both count limits are now against it.
    blocked = engine.check_order(
        side="BUY", qty=money("0.01"), reduce_only=False, open_orders=4, **common
    )
    assert blocked is not None
    # And the exit still goes through.
    assert engine.check_order(
        side="SELL", qty=money("1"), reduce_only=True, open_orders=4, **common
    ) is None


def test_the_open_order_breach_reports_the_count_it_would_produce() -> None:
    """`observed 3, limit 3` read as a value that did not exceed the limit it broke."""
    engine = RiskEngine(
        limits=RiskLimits.from_json({"max_open_orders": 3}),
        starting_equity=money("100000"),
    )
    breach = engine.check_order(
        ts_ms=1_000,
        symbol="BTCUSDT",
        side="BUY",
        qty=money("0.01"),
        price=money("40000"),
        reduce_only=False,
        position_qty=money("0"),
        working=WorkingExposure.zero(),
        equity=money("100000"),
        open_orders=3,
    )
    assert breach is not None
    assert breach.observed == "4"
    assert breach.allowed == "3"


def test_a_second_breach_after_a_halt_is_not_recorded_twice() -> None:
    """A halt is one event even when several things break at once.

    Two symbols liquidating inside one check used to append two breaches, so the results
    page listed a cascade of symptoms as though each had stopped something.
    """
    engine = RiskEngine(
        limits=RiskLimits.from_json({"halt_on_liquidation": True}),
        starting_equity=money("100000"),
    )
    first = engine.observe_liquidation(1_000, "BTCUSDT")
    second = engine.observe_liquidation(1_000, "ETHUSDT")
    assert first is not None and second is not None
    assert len(engine.breaches) == 1
    assert engine.halt_breach is first
    assert engine.kill_switch.trigger == "LIQUIDATION"


def test_any_limit_and_unbounded_exposure_are_independent_facts() -> None:
    """A rate-limited run had a risk layer *and* no ceiling on size. Both, not either."""
    rate_only = RiskLimits.from_json({"max_orders_per_minute": 2})
    assert rate_only.any_limit is True
    assert rate_only.unbounded_exposure is True
    assert RiskLimits.unlimited().any_limit is False
    assert RiskLimits().any_limit is True
    assert RiskLimits().unbounded_exposure is False


def test_a_halted_runs_metrics_cover_the_period_it_observed(tmp_path: Path) -> None:
    """Not the range it was asked for. This is the review's most consequential finding.

    A 1 BTC long into a falling ramp halts on a 10% drawdown a few minutes into a range
    that runs for hours. `_finalise` used to advance to the end of the *requested* range and
    take a last equity sample there, carrying the halt's equity flat across a window nothing
    was observed in — and every ratio metric is computed on a grid over that range. All the
    grid points after the halt then carried the same number, so `volatility` came out at
    exactly 0.0 and `sharpe` at `None` for a run that had just lost 10% in minutes.

    Here the range is 600 minutes and the halt is inside the first 20, so `days` must be a
    small fraction of the range rather than its whole length.
    """
    build_lake(
        tmp_path,
        start_ms=START,
        minutes=600,
        trade_path=ramp_path(40_000.0, -1_000.0),
    )
    result = run(
        tmp_path,
        BuyOnce(qty="1"),
        minutes=600,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=Decimal("0.10"),
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    assert result.halt_reason is not None
    halt_ms = result.halt_reason.ts_ms
    # The whole range is 600 minutes; the halt is inside the first 20.
    assert halt_ms - START < 20 * MS_PER_MINUTE
    assert result.equity_ms[-1] <= halt_ms
    assert result.metrics.days < 0.05, f"600 minutes is 0.42 days; got {result.metrics.days}"

    # **Undefined, not fabricated.** Nineteen minutes does not contain a whole hour, so an
    # hourly grid has no periods and the ratios are honestly `None`. The bug produced the
    # opposite: nine periods over the full range, eight of them carrying the halt equity
    # forward unchanged, so `volatility` was exactly 0.0 and `sharpe` was a number computed
    # from a series the run never observed.
    assert result.metrics.periods == 0
    assert result.metrics.volatility is None
    assert result.metrics.sharpe is None


def test_a_flatten_the_exchange_refuses_is_not_charged_to_the_strategy(
    tmp_path: Path,
) -> None:
    """The platform's own order is not the strategy's.

    A 200 BTC position is legal to accumulate in two orders and illegal to exit in one
    (`MARKET_LOT_SIZE` caps a market order at 120). With the kill switch armed, the halt's
    flatten is refused — and booking that refusal into `rejects` raised `ORDERS_REJECTED`
    and produced a warning telling the reader that an order of *theirs* had been refused
    "at arrival; see the REJECT entries for the filter or margin that refused them".
    """
    build_lake(
        tmp_path, start_ms=START, minutes=40, trade_path=ramp_path(40_000.0, -200.0)
    )

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
        minutes=40,
        # 200 BTC at 40 000 is 8M of notional. At 1x that is 8M of isolated margin against
        # 10M of equity, so a 2% drawdown is 200 000 -- a 1 000 fall per BTC, which the ramp
        # reaches in five minutes -- and liquidation is 39 840 away, which it never does.
        # At 20x the position liquidated at 37 870 and halted on `halt_on_liquidation`
        # before the drawdown limit could fire, testing something else entirely.
        balance="10000000",
        leverage=1,
        kill_switch_flatten=True,
        risk=RiskLimits(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=Decimal("0.02"),
            max_open_orders=None,
            max_orders_per_minute=None,
            min_equity_pct=None,
            max_consecutive_rejections=None,
        ),
    )
    assert result.halt_reason is not None
    failures = kinds(result, "FORCE_FLATTEN_FAILED")
    assert failures, "the exit was refused, and the run records it as the platform's"
    assert result.rejects == 0, "not booked against the strategy"
    assert "ORDERS_REJECTED" not in result.flags
    assert kinds(result, "KILL_SWITCH")[0]["open_after"] == ["BTCUSDT"]
    assert any("could not close the position" in w for w in result.warnings)


def test_a_spec_written_today_is_refused_by_a_reader_that_predates_the_risk_layer() -> None:
    """`SPEC_VERSION` had to move, and the direction that matters is forwards.

    A version-2 file replays correctly here -- the three risk fields default to empty, which
    is exactly what those runs had. Leaving the number at 2 meant a *new* file also declared
    2, so a Phase 5 reader would accept it without complaint and silently drop the limits:
    a run executed under a 5x cap with a 15% drawdown halt replayed as unconstrained.
    Refusing an unknown version is this field's entire job.

    Asserted against `SPEC_VERSION` itself rather than against a literal. This test pinned
    `== 3`, so Phase 7's bump to 4 -- made for the same reason, one version later, to stop a
    paper session's `source: "tape:41"` being dropped by an older reader and replayed out of
    the lake -- failed it. What the test is *for* is that the number moves whenever the
    stored shape changes and that an unknown version is refused; hard-coding the number
    turned it into an alarm that fires on the correct action.
    """
    import json

    from perplab.engine.runspec import SPEC_VERSION, RunSpec

    assert SPEC_VERSION >= 3

    spec = RunSpec(
        strategy_id=1,
        version_id=1,
        version_no=1,
        strategy_name="X",
        code="pass",
        class_name=None,
        params={},
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + MS_PER_MINUTE,
        seed=0,
        opening_balance="10000",
        leverage=1,
        maker_rate="0.0002",
        taker_rate="0.0005",
        fee_source="test",
        latency={"model": "fixed", "submit_ms": 10, "cancel_ms": 10},
        fill_tier="BAR_CLOSE",
        fill_model={"tier": "BAR_CLOSE", "slippage_bps": "1.0"},
        liquidation_recovery_pct="0",
        timeout_s=60.0,
        engine_version=1,
        risk_limits={"max_leverage": "5"},
        auto_flatten={"max_hold_ms": 3_600_000},
        kill_switch_flatten=True,
    )
    stored = json.loads(json.dumps(spec.to_storage()))
    assert stored["spec_version"] == SPEC_VERSION

    # A round trip preserves every field, explicit `None`s included.
    back = RunSpec.from_storage(stored)
    assert back.risk_limits == {"max_leverage": "5"}
    assert back.auto_flatten == {"max_hold_ms": 3_600_000}
    assert back.kill_switch_flatten is True

    # A version-2 file — one written before the risk layer existed — reads as a run with
    # no limits, which is what it was.
    old = {k: v for k, v in stored.items() if k not in
           ("risk_limits", "auto_flatten", "kill_switch_flatten")}
    old["spec_version"] = 2
    upgraded = RunSpec.from_storage(old)
    assert RiskLimits.from_json(upgraded.risk_limits) == RiskLimits.unlimited()
    assert upgraded.auto_flatten == {}
    assert upgraded.kill_switch_flatten is False

    with pytest.raises(ValueError, match="cannot be read by this build"):
        RunSpec.from_storage({**stored, "spec_version": 99})
