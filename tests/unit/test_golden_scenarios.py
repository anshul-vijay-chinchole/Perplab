"""Phase 5's golden scenarios -- the realism layer, one hand-computed number at a time.

Spec 13's exit criterion for this phase is *"All golden scenarios reproduce; fill tier
correctly degrades and is surfaced in the UI."* This file is the first half.

**What makes a scenario golden.** Every expected value here is derived in the test's own
docstring from prices the test wrote into the lake, in arithmetic a reader can check without
running anything. That is a deliberately harder standard than "assert whatever the engine
printed": a test that records the current answer passes forever, including after the answer
becomes wrong. A scenario whose expected number was computed independently fails the moment
the model changes, which is the only way a fill model stays honest across a rewrite.

**Every scenario is a market microstructure claim, not a code path.** "A trade at the limit
price does not fill an order behind a queue" is a statement about how exchanges work; the
test exists because getting it wrong is the specific error spec 6.4 calls *"the single most
common way limit strategies look profitable and are not"*. The ones that only exercise a
branch live in `test_backtest.py`.

**Latency is fixed and small throughout.** Golden tests want a deterministic arrival
timestamp, which spec 6.3 names as `fixed` mode's purpose. The lognormal default is the
right one for a real run and the wrong one here -- an assertion on which print a fill took
would be a statement about an RNG.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.core.money import parse_money
from perplab.engine.backtest import (
    BacktestConfig,
    BacktestEngine,
    UnsupportedOrder,
)
from perplab.engine.fills import (
    BookTickerFillModel,
    BookWalkFillModel,
    TradeOnlyFillModel,
)
from perplab.engine.latency import FixedLatency
from perplab.strategy.base import Strategy
from perplab.strategy.context import FillTier
from tests.engine_lake import MS_PER_MINUTE, build_lake, flat_path
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000  # 2024-03-01T00:00:00Z
BAR1 = START + MS_PER_MINUTE - 1  # close of the first minute
BAR2 = START + 2 * MS_PER_MINUTE - 1
LATENCY = 10
"""Milliseconds. Small enough that a scripted tick one second later is unambiguously after
arrival, large enough that a fill can never take the print that triggered it."""


# ------------------------------------------------------------------------- strategies


class Scripted(Strategy):
    """Runs one caller-supplied action on the bar whose close matches `at`.

    A single parameterised strategy rather than twenty near-identical ones. What each
    scenario varies is the *order*, and putting that in the test body next to the expected
    number is what lets the arithmetic be checked against the thing that produced it.
    """

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def __init__(self, action, *, at: int = BAR1) -> None:
        super().__init__({})
        self._action = action
        self._at = at
        self.fills: list = []

    def on_start(self, ctx) -> None:
        self.order_id = None

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and bar.close_time == self._at:
            self.order_id = self._action(ctx)

    def on_fill(self, ctx, fill) -> None:
        self.fills.append(fill)


class TickCounter(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["aggTrades"],
    }

    def on_start(self, ctx) -> None:
        self.seen: list = []

    def on_tick(self, ctx, trade) -> None:
        self.seen.append(trade)

    def on_bar(self, ctx, bar) -> None:
        pass


# ---------------------------------------------------------------------------- harness


def run(
    root: Path,
    strategy: Strategy,
    *,
    tier: FillTier,
    minutes: int = 5,
    fill_model=None,
    balance: str = "1000000",
    leverage: int = 10,
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
    engine = BacktestEngine(
        root=root,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
    )
    return engine.run()


def fills(result) -> list[dict]:
    return [dict(e.payload) for e in result.events if e.kind == "FILL"]


def money(text: str):
    """Compare an event-log figure as an exact quantity, not as a string.

    Every monetary value crosses the event log as decimal text at the storage seam's eight
    places, so `"40000.20"` and `"40000.20000000"` are the same number written two ways. A
    test asserting on the *spelling* would be asserting on `money_to_str`, and would fail on
    a change that moved no money at all.
    """
    return parse_money(text)


def kinds(result, kind: str) -> list[dict]:
    return [dict(e.payload) for e in result.events if e.kind == kind]


def steady_ticks(count: int = 240, price: float = 40_000.0, qty: float = 1.0):
    """One print per second across the first four minutes, alternating aggressor side.

    Present in almost every scenario for one reason: spec 6.4's `BOOK_TICKER` impact term
    divides by the last minute's traded notional, and a lake with no trades at all makes that
    denominator zero. A scenario about depth walking should not also be a scenario about an
    empty tape, so the tape is stocked and the scenario that *is* about an empty tape builds
    its own.
    """
    return [
        (START + second * 1000, price, qty, second % 2 == 0) for second in range(count)
    ]


def flat_book(count: int = 240, bid: float = 39_999.9, ask: float = 40_000.1, size: float = 10.0):
    return [(START + second * 1000, bid, size, ask, size) for second in range(count)]


# ============================================================== market orders: BOOK_WALK


def test_a_market_buy_walks_the_ladder_and_pays_the_weighted_average(tmp_path: Path) -> None:
    """Spec 6.4's walk, arithmetic in full.

    The ladder in force at arrival offers 1 @ 40 000.10 and 5 @ 40 000.20 on the ask. A buy
    of 3 takes all of level one and 2 of level two:

        cost = 1 x 40 000.10 + 2 x 40 000.20 = 120 000.50
        avg  = 120 000.50 / 3 = 40 000.1666...
        tick = 0.10, rounded **against** the buyer -> 40 000.20

    Two levels walked, nothing exhausted. Had the model taken the touch for the whole order
    it would have paid 40 000.10, which is a tenth of a tick cheaper per unit and always in
    the trader's favour -- the shape of error that compounds.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=flat_book(),
        depth=[
            (
                START + second * 1000,
                [(39_999.90, 10.0), (39_999.80, 10.0)],
                [(40_000.10, 1.0), (40_000.20, 5.0)],
            )
            for second in range(240)
        ],
    )
    strategy = Scripted(lambda ctx: ctx.buy(qty=ctx.money("3")))
    result = run(lake, strategy, tier=FillTier.BOOK_WALK)

    booked = fills(result)
    assert len(booked) == 1
    assert money(booked[0]["price"]) == money("40000.20")
    assert booked[0]["levels_walked"] == 2
    assert "exhausted_qty" not in booked[0]
    assert result.depth_exhausted == 0


def test_an_order_larger_than_the_book_pays_the_exhaustion_penalty_and_says_so(
    tmp_path: Path,
) -> None:
    """Spec 6.4: the remainder is priced at `worst_level x (1 + 0.1%)`, and it is *flagged*.

    Ladder: 1 @ 40 000.10, 2 @ 40 000.20. A buy of 5 consumes both levels and is 2 short.

        visible = 1 x 40 000.10 + 2 x 40 000.20 = 120 000.50
        penalty = 40 000.20 x 1.001 = 40 040.2002
        excess  = 2 x 40 040.2002 = 80 080.4004
        avg     = (120 000.50 + 80 080.4004) / 5 = 40 016.18008
        tick    -> 40 016.20

    The flag is half the point. Spec 6.4: *"If a strategy routinely triggers this warning, the
    position sizing is unrealistic for the instrument, and the results page says so rather
    than quietly filling at a fantasy price."*
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=flat_book(),
        depth=[
            (
                START + second * 1000,
                [(39_999.90, 10.0)],
                [(40_000.10, 1.0), (40_000.20, 2.0)],
            )
            for second in range(240)
        ],
    )
    strategy = Scripted(lambda ctx: ctx.buy(qty=ctx.money("5")))
    result = run(lake, strategy, tier=FillTier.BOOK_WALK)

    booked = fills(result)
    assert money(booked[0]["price"]) == money("40016.20")
    assert money(booked[0]["exhausted_qty"]) == money("2.00000000")
    assert result.depth_exhausted == 1
    assert "DEPTH_EXHAUSTED" in result.flags
    assert any("exceeded all published depth" in w for w in result.warnings)


def test_a_market_sell_walks_the_bid_side_and_the_penalty_points_down(tmp_path: Path) -> None:
    """The mirror image, and it must not be a copy of the buy path with a sign flipped.

    Bids: 2 @ 39 999.90, 1 @ 39 999.80. A sell of 5 takes 3 and is 2 short.

        visible = 2 x 39 999.90 + 1 x 39 999.80 = 119 999.60
        penalty = 39 999.80 x 0.999 = 39 959.8002
        excess  = 2 x 39 959.8002 = 79 919.6004
        avg     = (119 999.60 + 79 919.6004) / 5 = 39 983.84008
        tick, rounded against the *seller* (down) -> 39 983.80
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=flat_book(),
        depth=[
            (
                START + second * 1000,
                [(39_999.90, 2.0), (39_999.80, 1.0)],
                [(40_000.10, 10.0)],
            )
            for second in range(240)
        ],
    )
    strategy = Scripted(lambda ctx: ctx.sell(qty=ctx.money("5")))
    result = run(lake, strategy, tier=FillTier.BOOK_WALK)
    assert money(fills(result)[0]["price"]) == money("39983.80")


# ============================================================ market orders: BOOK_TICKER


def test_the_impact_term_is_k_times_the_square_root_of_relative_size(tmp_path: Path) -> None:
    """Spec 6.4: `impact_bps = k x sqrt(order_notional / recent_1min_notional_volume)`.

    The tape prints 1 unit at 40 000 every second, so the trailing minute holds 60 prints and

        recent  = 60 x 40 000 = 2 400 000
        order   = 3 x 40 000.10 (the ask) = 120 000.30
        ratio   = 120 000.30 / 2 400 000 = 0.0500001250
        sqrt    = 0.2236069873...
        impact  = 10 x that = 2.236069873... bps
        fill    = 40 000.10 x (1 + 0.00022360698...) = 40 000.10 + 8.9444 = 40 009.0444
        tick, rounded against the buyer -> 40 009.10

    The order arrives at bar 1's close + 10 ms, so the window is the 60 prints in that
    minute -- which is why the scenario asserts the reported `impact_bps` too: a window that
    silently held a different number of trades would still produce a plausible price.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=flat_book(),
    )
    strategy = Scripted(lambda ctx: ctx.buy(qty=ctx.money("3")))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    booked = fills(result)
    assert money(booked[0]["price"]) == money("40009.10")
    assert booked[0]["impact_bps"].startswith("2.2360")


def test_a_minute_with_no_trades_refuses_the_fill_rather_than_pricing_it_at_the_touch(
    tmp_path: Path,
) -> None:
    """The impact term's denominator is zero and every way of filling that hole is a fiction.

    Zero impact is optimistic, a constant is indefensible, and the visible touch size answers
    a different question. So the order is rejected and the reason names the formula. Spec
    4.5's `HALT_TRADING` is the same idea at run granularity: during an outage there are no
    fills, not cheap ones.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        # One print at the very start of the run, then silence. By bar 1's close it is 60 s
        # old and has left the window.
        ticks=[(START, 40_000.0, 1.0, True)],
        quotes=flat_book(),
    )
    strategy = Scripted(lambda ctx: ctx.buy(qty=ctx.money("1")))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    assert not fills(result)
    assert result.rejects == 1
    assert "zero denominator" in kinds(result, "REJECT")[0]["reason"]


# ============================================================= market orders: TRADE_ONLY


def test_a_trade_only_market_order_fills_at_the_next_print_not_the_last(
    tmp_path: Path,
) -> None:
    """Spec 6.4: *"fill at the next trade price after arrival"*, and the timestamp proves it.

    The tape prints 40 000 through the first minute, then 41 000 at 00:01:05. The order is
    submitted at bar 1's close (00:00:59.999) and arrives 10 ms later, before any 41 000
    print exists. It waits.

        fill print = 41 000 at 00:01:05
        spread     = 1 bp against the buyer -> 41 000 x 1.0001 = 41 004.10
        fill ts    = 00:01:05, **not** the arrival timestamp

    Filling at the last print instead would have paid 40 000 -- a thousand better, taken from
    a price that had already stopped being available.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=[
            *[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(60)],
            (START + 65_000, 41_000.0, 1.0, False),
        ],
    )
    strategy = Scripted(lambda ctx: ctx.buy(qty=ctx.money("1")))
    result = run(
        lake,
        strategy,
        tier=FillTier.TRADE_ONLY,
        fill_model=TradeOnlyFillModel(spread_bps=parse_money("1.0")),
    )

    booked = fills(result)
    assert len(booked) == 1
    assert money(booked[0]["price"]) == money("41004.10")
    assert booked[0]["ts_ms"] == START + 65_000


def test_a_market_order_with_no_print_within_the_deadline_expires(tmp_path: Path) -> None:
    """A minute of silence is an outage, not a slow fill.

    An order that finally executed an hour later against a market that had moved without it
    would be a worse artefact than one that plainly did not fill, so it expires with the
    reason attached rather than waiting for whatever prints next.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(60)],
    )
    strategy = Scripted(lambda ctx: ctx.buy(qty=ctx.money("1")))
    result = run(lake, strategy, tier=FillTier.TRADE_ONLY)

    assert not fills(result)
    expiries = kinds(result, "EXPIRE")
    assert expiries and "nothing to execute against" in expiries[0]["reason"]


def test_on_tick_receives_every_print_in_order(tmp_path: Path) -> None:
    """The hook exists at `TRADE_ONLY` and above, and it sees the tape, not a summary."""
    lake = tmp_path / "market"
    ticks = [(START + s * 1000, 40_000.0 + s, 0.5, s % 3 == 0) for s in range(120)]
    build_lake(
        lake, start_ms=START, minutes=3, trade_path=flat_path(40_000.0), ticks=ticks
    )
    strategy = TickCounter({})
    result = run(lake, strategy, tier=FillTier.TRADE_ONLY, minutes=3)

    assert len(strategy.seen) == len(ticks)
    assert [t.ts_ms for t in strategy.seen] == [t[0] for t in ticks]
    assert strategy.seen[7].price == pytest.approx(40_007.0)
    assert strategy.seen[0].is_sell_aggressive is True
    assert result.ticks == len(ticks)


# ================================================================= limit orders: queue


def _limit_lake(
    tmp_path: Path,
    *,
    queue_size: float,
    prints,
    minutes: int = 5,
) -> Path:
    """A lake whose bid at 39 999.00 holds `queue_size` and whose tape is `prints`.

    The book is deliberately *away* from the limit price at the moment the order arrives --
    the touch is 39 999.90 -- so the order rests rather than crossing, and every fill it gets
    afterwards has to come through the queue model.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=minutes,
        trade_path=flat_path(40_000.0),
        ticks=[*steady_ticks(60), *prints],
        quotes=[
            *[(START + s * 1000, 39_999.90, 5.0, 40_000.10, 5.0) for s in range(60)],
            # From 00:01:00 the market is *at* our level, so its size is observable.
            *[
                (START + 60_000 + s * 1000, 39_999.00, queue_size, 40_000.10, 5.0)
                for s in range(120)
            ],
        ],
    )
    return lake


def _buy_limit(price: str, qty: str, **kwargs):
    return lambda ctx: ctx.buy(
        qty=ctx.money(qty), type="LIMIT", price=ctx.money(price), **kwargs
    )


def test_a_trade_at_the_limit_price_behind_a_queue_is_not_a_fill(tmp_path: Path) -> None:
    """Spec 6.4's headline rule: *"Touching a limit price is not a fill."*

    Our buy limit rests at 39 999.00 behind 4 units of visible queue. One sell-aggressive
    print of 3 units lands at exactly that price. It consumes 3 of the 4 ahead of us and
    leaves nothing over, so we get nothing -- which is what would have happened on the
    exchange, and is the difference between a limit backtest that is roughly honest and one
    that is fiction.
    """
    lake = _limit_lake(
        tmp_path,
        queue_size=4.0,
        prints=[(START + 90_000, 39_999.00, 3.0, True)],
    )
    strategy = Scripted(_buy_limit("39999.00", "1"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    assert not fills(result)
    assert result.orders == 1


def test_the_queue_is_consumed_and_the_next_print_fills_us_at_our_own_price(
    tmp_path: Path,
) -> None:
    """The same order, one print later: `Q_ahead` reaches zero and the overflow is ours.

    Queue ahead: 4. First print 3 units sell-aggressive at 39 999.00 -> 1 left ahead.
    Second print 2.5 units at the same price -> 1 consumes the rest of the queue, 1.5
    remains, and our order takes 1 of it at **39 999.00**, our limit, as a *maker*.

    The fill price is the limit, not the print price -- they happen to be equal here, which
    is what makes the maker flag the thing worth asserting.
    """
    lake = _limit_lake(
        tmp_path,
        queue_size=4.0,
        prints=[
            (START + 90_000, 39_999.00, 3.0, True),
            (START + 91_000, 39_999.00, 2.5, True),
        ],
    )
    strategy = Scripted(_buy_limit("39999.00", "1"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    booked = fills(result)
    assert len(booked) == 1
    assert money(booked[0]["price"]) == money("39999.00")
    assert money(booked[0]["qty"]) == money("1.00000000")
    assert booked[0]["is_maker"] is True
    assert result.maker_fills == 1


def test_a_partial_fill_emits_on_fill_per_increment(tmp_path: Path) -> None:
    """Spec 6.5: *"not batched at the end"*.

    Queue ahead 0 (the level is empty when we join). Three sell-aggressive prints of 1 unit
    each land at our price against a 2.5-unit order:

        print 1 -> 1.0 filled, 1.5 remaining
        print 2 -> 1.0 filled, 0.5 remaining
        print 3 -> 0.5 filled, order complete

    Three `on_fill` calls, three ledger updates, and the position's entry price is the
    weighted average of three fills at the same price rather than one fill of 2.5.
    """
    lake = _limit_lake(
        tmp_path,
        queue_size=0.0,
        prints=[
            (START + 90_000, 39_999.00, 1.0, True),
            (START + 91_000, 39_999.00, 1.0, True),
            (START + 92_000, 39_999.00, 1.0, True),
        ],
    )
    strategy = Scripted(_buy_limit("39999.00", "2.5"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    booked = fills(result)
    assert [money(f["qty"]) for f in booked] == [
        money("1"),
        money("1"),
        money("0.5"),
    ]
    assert len(strategy.fills) == 3
    assert result.partial_fills == 2
    assert money(booked[-1]["remaining"]) == money("0.00000000")


def test_a_buy_aggressive_print_at_our_bid_does_not_touch_us(tmp_path: Path) -> None:
    """Aggressor side is the whole model, and inverting it reverses every queue decision.

    `is_buyer_maker=False` means the *buyer* was the aggressor, so the print consumed
    **ask**-side queue. A resting buy at that price is on the other side of the book and
    cannot have been filled by it, however equal the prices look.
    """
    lake = _limit_lake(
        tmp_path,
        queue_size=0.0,
        prints=[(START + 90_000, 39_999.00, 5.0, False)],
    )
    strategy = Scripted(_buy_limit("39999.00", "1"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)
    assert not fills(result)


def test_a_print_through_our_level_fills_us_bounded_by_the_aggressor_size(
    tmp_path: Path,
) -> None:
    """Spec 6.4 says a through-trade fills "fully"; the bound is a deliberate deviation.

    A 0.4-unit sell printing one tick *below* our 3-unit bid cannot, on any coherent reading,
    have filled 3 units of ours -- had our 3 units really been resting there, a 0.4-unit
    aggressor would have been absorbed at our price and never printed below it. So the
    through-print clears the queue ahead and fills us up to **its own size**.

        queue ahead      : 2 (never consumed -- no print landed at our level)
        print            : 0.4 @ 39 998.90, sell-aggressive, one tick through
        queue ahead      -> 0
        fill             : min(0.4, 3) = 0.4 at **39 999.00**, our limit

    The deviation only bites where the literal rule is incoherent, and it bites
    conservatively: we fill less, never more.
    """
    lake = _limit_lake(
        tmp_path,
        queue_size=2.0,
        prints=[(START + 90_000, 39_998.90, 0.4, True)],
    )
    strategy = Scripted(_buy_limit("39999.00", "3"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    booked = fills(result)
    assert len(booked) == 1
    assert money(booked[0]["qty"]) == money("0.40000000")
    assert money(booked[0]["price"]) == money("39999.00")


def test_the_queue_bound_only_ever_tightens(tmp_path: Path) -> None:
    """`Q_ahead = min(Q_ahead, observed)` -- valid because observed is an upper bound.

    The published size at our level counts *everyone* there; the orders ahead of us are a
    subset, so the observation can never be smaller than the truth. Taking the minimum
    therefore captures cancellations ahead of us -- which no trade-consumption rule can see --
    without ever claiming a better position than the data permits.

    Here the level is observed at 5 units and then at 1, with no print in between. Only the
    tighter bound survives, so a single 1.2-unit print fills us: 1 clears the queue, 0.2 is
    ours. Had the bound stayed at 5, nothing would have filled.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=[*steady_ticks(60), (START + 120_000, 39_999.00, 1.2, True)],
        quotes=[
            *[(START + s * 1000, 39_999.90, 5.0, 40_000.10, 5.0) for s in range(60)],
            *[(START + 60_000 + s * 1000, 39_999.00, 5.0, 40_000.10, 5.0) for s in range(30)],
            # Everything ahead of us cancels.
            *[(START + 90_000 + s * 1000, 39_999.00, 1.0, 40_000.10, 5.0) for s in range(60)],
        ],
    )
    strategy = Scripted(_buy_limit("39999.00", "1"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    booked = fills(result)
    assert len(booked) == 1
    assert money(booked[0]["qty"]) == money("0.20000000")


# ============================================================== limit orders: crossing


def test_a_marketable_limit_takes_the_touch_and_rests_the_remainder(tmp_path: Path) -> None:
    """A limit through the touch is a taker order for as much as the touch will give.

    Ask 40 000.10 with 2 units resting; our buy limit at 40 000.50 wants 5. Two fill
    immediately at 40 000.10 as a *taker* -- not at the limit, and not with an impact charge,
    because a limit order cannot pay more than its limit and nothing beyond the touch is
    visible at this tier. The remaining 3 rest at 40 000.50.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        # The tape stops before the order arrives. Not incidental: a buy limit at 40 000.50
        # left resting while sell-aggressive prints keep landing at 40 000 is *through* its
        # level, so the remainder would fill correctly and this scenario would be measuring
        # two rules at once.
        ticks=steady_ticks(59),
        quotes=[(START + s * 1000, 39_999.90, 5.0, 40_000.10, 2.0) for s in range(240)],
    )
    strategy = Scripted(_buy_limit("40000.50", "5"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    booked = fills(result)
    assert len(booked) == 1
    assert money(booked[0]["qty"]) == money("2")
    assert money(booked[0]["price"]) == money("40000.10")
    assert booked[0]["is_maker"] is False
    working = kinds(result, "ORDER_WORKING")
    assert money(working[0]["remaining"]) == money("3")
    assert money(working[0]["price"]) == money("40000.50")


def test_post_only_is_expired_rather_than_crossed(tmp_path: Path) -> None:
    """Spec 6.5: post-only is how maker fees are guaranteed *and* how orders silently fail.

    So it leaves as `EXPIRED` with the reason attached, not as a cancel the strategy asked
    for and not as a taker fill it explicitly refused.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=flat_book(),
    )
    strategy = Scripted(_buy_limit("40000.50", "1", tif="GTX"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    assert not fills(result)
    expiries = kinds(result, "EXPIRE")
    assert expiries and "post-only" in expiries[0]["reason"]


def test_fill_or_kill_leaves_no_partial_behind(tmp_path: Path) -> None:
    """A `FOK` that had already half-filled when it decided to kill would be neither.

    The touch offers 2 and the order wants 5, so the check happens *before* any fill is
    booked and the order leaves whole.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=[(START + s * 1000, 39_999.90, 5.0, 40_000.10, 2.0) for s in range(240)],
    )
    strategy = Scripted(_buy_limit("40000.50", "5", tif="FOK"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    assert not fills(result)
    expiries = kinds(result, "EXPIRE")
    assert expiries and "fill-or-kill" in expiries[0]["reason"]


def test_immediate_or_cancel_takes_what_is_there_and_expires_the_rest(tmp_path: Path) -> None:
    """The complement of `FOK`: 2 of 5 fill, 3 leave, and nothing rests."""
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=[(START + s * 1000, 39_999.90, 5.0, 40_000.10, 2.0) for s in range(240)],
    )
    strategy = Scripted(_buy_limit("40000.50", "5", tif="IOC"))
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

    booked = fills(result)
    assert len(booked) == 1 and money(booked[0]["qty"]) == money("2.00000000")
    expiries = kinds(result, "EXPIRE")
    assert expiries and "immediate-or-cancel" in expiries[0]["reason"]


def test_a_book_walk_limit_sweeps_several_levels_up_to_its_price(tmp_path: Path) -> None:
    """The fidelity difference between the two book tiers, made visible.

    Asks: 1 @ 40 000.10, 1 @ 40 000.20, 5 @ 40 000.60. A buy limit at 40 000.50 for 5 takes
    the first two levels and stops -- the third is above its limit.

        cost = 1 x 40 000.10 + 1 x 40 000.20 = 80 000.30
        avg  = 40 000.15, tick against the buyer -> 40 000.20
        rest = 3 units at 40 000.50

    At `BOOK_TICKER` the same order would have taken only the touch's 1 unit, because that is
    all that tier can see. Same order, same book, less fill -- which is what a fidelity gap
    should look like.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        # Stops before the order arrives, for the reason given in the BOOK_TICKER twin: a
        # resting buy at 40 000.50 is above a 40 000 tape and would legitimately fill from it.
        ticks=steady_ticks(59),
        quotes=flat_book(),
        depth=[
            (
                START + second * 1000,
                [(39_999.90, 10.0)],
                [(40_000.10, 1.0), (40_000.20, 1.0), (40_000.60, 5.0)],
            )
            for second in range(240)
        ],
    )
    strategy = Scripted(_buy_limit("40000.50", "5"))
    result = run(lake, strategy, tier=FillTier.BOOK_WALK)

    booked = fills(result)
    assert len(booked) == 1
    assert money(booked[0]["qty"]) == money("2.00000000")
    assert money(booked[0]["price"]) == money("40000.20")
    assert money(kinds(result, "ORDER_WORKING")[0]["remaining"]) == money("3.00000000")


# ==================================================================== triggers: stops


class EntryThenProtective(Strategy):
    """Buys on bar 1 and attaches a protective order on bar 2, once the position exists."""

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def __init__(self, attach) -> None:
        super().__init__({})
        self._attach = attach

    def on_start(self, ctx) -> None:
        self.attached = False

    def on_bar(self, ctx, bar) -> None:
        if not ctx.warm:
            return
        if bar.close_time == BAR1:
            ctx.buy(qty=ctx.money("1"))
        elif bar.close_time == BAR2 and not self.attached and not ctx.position().is_flat:
            self.attached = True
            self._attach(ctx)


def _stop_lake(tmp_path: Path, mark_path, *, minutes: int = 6) -> Path:
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=minutes,
        trade_path=flat_path(40_000.0),
        mark_path=mark_path,
        ticks=[
            (START + s * 1000, 40_000.0, 1.0, s % 2 == 0)
            for s in range(minutes * 60)
        ],
        quotes=[
            (START + s * 1000, 39_999.90, 5.0, 40_000.10, 5.0)
            for s in range(minutes * 60)
        ],
    )
    return lake


def test_a_mark_triggered_stop_fires_inside_the_bar_range_and_fills_as_a_market_order(
    tmp_path: Path,
) -> None:
    """Spec 6.4: on trigger it *becomes a market order* and pays the market-order path.

    The stop sits at 39 500. Mark bar 4 traverses [39 000, 40 000] -- so the mark reached the
    stop somewhere inside that minute, even though the bar closed back at 40 000 and a
    close-only check would have missed it entirely.

    The trigger is stamped at the bar's `close_time` because a kline says the mark reached a
    level *somewhere inside* the minute and says nothing about when. Late is the pessimistic
    direction for a stop -- the fill lands further from the trigger -- so the uncertainty is
    spent against the strategy rather than for it.

    The fill is then a market sell at the bid, 39 999.90 -- nowhere near the 39 500 stop
    level, which is exactly spec 6.4's point that *"a stop is not a guaranteed price"*. The
    impact term is switched off here so the fill price is the touch exactly; what this
    scenario is about is *when* the stop fired and *that* it took the market path, and
    leaving `k` at its default would fold a second piece of arithmetic into the same
    assertion.
    """
    from tests.engine_lake import Ohlc, scaled

    def mark(index: int) -> Ohlc:
        if index == 3:
            return Ohlc(scaled(40_000), scaled(40_000), scaled(39_000), scaled(40_000))
        return Ohlc(scaled(40_000), scaled(40_000), scaled(40_000), scaled(40_000))

    lake = _stop_lake(tmp_path, mark)
    strategy = EntryThenProtective(
        lambda ctx: ctx.stop_loss(stop_price=ctx.money("39500"))
    )
    result = run(
        lake,
        strategy,
        tier=FillTier.BOOK_TICKER,
        minutes=6,
        fill_model=BookTickerFillModel(impact_k_bps=parse_money("0")),
    )

    triggers = kinds(result, "TRIGGER")
    assert len(triggers) == 1
    assert triggers[0]["type"] == "STOP_MARKET"
    # Fired at the close of mark bar index 3, which is the fourth minute.
    assert result.events[0].ts_ms <= START + 4 * MS_PER_MINUTE
    sells = [f for f in fills(result) if f["side"] == "SELL"]
    assert len(sells) == 1
    assert money(sells[0]["price"]) == money("39999.90")
    assert sells[0]["reduce_only"] is True


def test_a_take_profit_triggers_in_the_opposite_direction_from_a_stop(
    tmp_path: Path,
) -> None:
    """The four type/side combinations are easy to transcribe backwards.

    A take-profit protecting a long fires when the price rises *through* its level; a stop
    protecting the same long fires when it falls through. Wired as a stop, this take-profit
    would never fire on a rising mark -- and the strategy would look like one that simply
    held.

    Mark bar 4 traverses [40 000, 41 000]; the take-profit sits at 40 500.
    """
    from tests.engine_lake import Ohlc, scaled

    def mark(index: int) -> Ohlc:
        if index == 3:
            return Ohlc(scaled(40_000), scaled(41_000), scaled(40_000), scaled(40_000))
        return Ohlc(scaled(40_000), scaled(40_000), scaled(40_000), scaled(40_000))

    lake = _stop_lake(tmp_path, mark)
    strategy = EntryThenProtective(
        lambda ctx: ctx.take_profit(stop_price=ctx.money("40500"))
    )
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER, minutes=6)

    triggers = kinds(result, "TRIGGER")
    assert len(triggers) == 1
    assert triggers[0]["type"] == "TAKE_PROFIT_MARKET"
    assert len([f for f in fills(result) if f["side"] == "SELL"]) == 1


def test_a_take_profit_does_not_fire_when_the_mark_never_reaches_it(tmp_path: Path) -> None:
    """The control for the scenario above: the same wiring, a mark that stays put."""
    lake = _stop_lake(tmp_path, flat_path(40_000.0))
    strategy = EntryThenProtective(
        lambda ctx: ctx.take_profit(stop_price=ctx.money("40500"))
    )
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER, minutes=6)
    assert not kinds(result, "TRIGGER")


def test_a_trailing_stop_ratchets_with_the_mark_and_fires_on_the_retracement(
    tmp_path: Path,
) -> None:
    """Spec 6.4: track the extreme, trigger at `callback_rate` retracement from it.

    A 1% callback on a long. The mark climbs to 42 000 on bar 4 and falls back to 41 000 on
    bar 5:

        extreme after bar 4 = 42 000
        trigger level       = 42 000 x (1 - 0.01) = 41 580
        bar 5 low           = 41 000  ->  fires

    Had the extreme not ratcheted -- had the level stayed anchored at the 40 000 entry -- the
    trigger would have sat at 39 600 and a 41 000 mark would have sailed past it. The
    ratchet is the whole feature, and a trailing stop that does not trail is a stop.
    """
    from tests.engine_lake import Ohlc, scaled

    def mark(index: int) -> Ohlc:
        if index == 3:
            return Ohlc(scaled(40_000), scaled(42_000), scaled(40_000), scaled(42_000))
        if index == 4:
            return Ohlc(scaled(42_000), scaled(42_000), scaled(41_000), scaled(41_000))
        return Ohlc(scaled(40_000), scaled(40_000), scaled(40_000), scaled(40_000))

    lake = _stop_lake(tmp_path, mark, minutes=7)
    strategy = EntryThenProtective(
        lambda ctx: ctx.trailing_stop(callback_rate=ctx.money("0.01"))
    )
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER, minutes=7)

    triggers = kinds(result, "TRIGGER")
    assert len(triggers) == 1
    assert triggers[0]["type"] == "TRAILING_STOP_MARKET"
    assert len([f for f in fills(result) if f["side"] == "SELL"]) == 1


def test_a_contract_price_stop_fires_on_the_trade_tape_not_the_mark(tmp_path: Path) -> None:
    """`workingType=CONTRACT_PRICE` watches prints; the mark can stay perfectly still.

    The mark never leaves 40 000, so a mark-triggered stop at 39 500 would never fire. The
    tape dips to 39 400 for one print. A contract-price stop sees it and fires on that exact
    print rather than at the end of a minute.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=6,
        trade_path=flat_path(40_000.0),
        mark_path=flat_path(40_000.0),
        ticks=[
            *[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(200)],
            (START + 200_000, 39_400.0, 1.0, True),
            *[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(201, 360)],
        ],
        quotes=[
            (START + s * 1000, 39_999.90, 5.0, 40_000.10, 5.0) for s in range(360)
        ],
    )
    strategy = EntryThenProtective(
        lambda ctx: ctx.stop_loss(
            stop_price=ctx.money("39500"), working_type="CONTRACT_PRICE"
        )
    )
    result = run(lake, strategy, tier=FillTier.BOOK_TICKER, minutes=6)

    triggers = kinds(result, "TRIGGER")
    assert len(triggers) == 1
    assert money(triggers[0]["trigger_price"]) == money("39400.00000000")
    assert len([f for f in fills(result) if f["side"] == "SELL"]) == 1


def test_the_slippage_reference_is_restamped_at_the_trigger(tmp_path: Path) -> None:
    """For a protective order the decision is the moment it fired, not the moment it was set.

    Leaving the original reference -- the price when the stop was attached, 40 000 -- would
    book the entire 600-point move from entry to trigger as *execution slippage*, and the
    results page would report a strategy destroyed by its broker rather than by its stop.

    The stop fires on a 39 400 print and the sell fills at the bid, 39 399.90, so the honest
    figure is a tenth of a point on one unit. Impact is switched off so that tenth is the
    only thing left in the number.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=6,
        trade_path=flat_path(40_000.0),
        mark_path=flat_path(40_000.0),
        ticks=[
            *[(START + s * 1000, 40_000.0, 1.0, s % 2 == 0) for s in range(200)],
            (START + 200_000, 39_400.0, 1.0, True),
            *[(START + s * 1000, 39_400.0, 1.0, s % 2 == 0) for s in range(201, 360)],
        ],
        quotes=[
            *[(START + s * 1000, 39_999.90, 5.0, 40_000.10, 5.0) for s in range(200)],
            *[(START + s * 1000, 39_399.90, 5.0, 39_400.10, 5.0) for s in range(200, 360)],
        ],
    )
    strategy = EntryThenProtective(
        lambda ctx: ctx.stop_loss(
            stop_price=ctx.money("39500"), working_type="CONTRACT_PRICE"
        )
    )
    result = run(
        lake,
        strategy,
        tier=FillTier.BOOK_TICKER,
        minutes=6,
        fill_model=BookTickerFillModel(impact_k_bps=parse_money("0")),
    )

    sells = [f for f in fills(result) if f["side"] == "SELL"]
    assert len(sells) == 1
    assert money(sells[0]["reference_price"]) == money("39400")
    # The stop filled at the bid, one tick under the trigger: 10 points of slippage on
    # one unit, not 600.
    assert money(sells[0]["slippage"]) == money("0.10000000")


# ==================================================================== tier capabilities


def test_a_limit_order_is_refused_at_trade_only_with_the_reason(tmp_path: Path) -> None:
    """No book means no resting size, and no resting size means no enforceable queue rule."""
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
    )
    strategy = Scripted(_buy_limit("39999.00", "1"))
    with pytest.raises(UnsupportedOrder, match="TRADE_ONLY tier has none"):
        run(lake, strategy, tier=FillTier.TRADE_ONLY)


def test_a_stop_is_refused_at_bar_close_with_the_reason(tmp_path: Path) -> None:
    """A stop filling at the next bar's open is a delayed market order wearing a stop's name."""
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=6, trade_path=flat_path(40_000.0))
    strategy = EntryThenProtective(
        lambda ctx: ctx.stop_loss(stop_price=ctx.money("39500"))
    )
    with pytest.raises(UnsupportedOrder, match="BAR_CLOSE tier has only bar prints"):
        run(lake, strategy, tier=FillTier.BAR_CLOSE, minutes=6)


def test_a_maker_fill_pays_the_maker_rate_and_a_taker_fill_the_taker_rate(
    tmp_path: Path,
) -> None:
    """The distinction only exists once orders can rest, which is why it is a Phase 5 test.

    Maker 2 bps, taker 5 bps, both fills of 1 unit at 39 999.00 and 40 000.10 respectively:

        maker fee = 1 x 39 999.00 x 0.0002 = 7.99980000
        taker fee = 1 x 40 000.10 x 0.0005 = 20.00005000
    """
    from perplab.core.account import FeeSchedule

    lake = _limit_lake(
        tmp_path,
        queue_size=0.0,
        prints=[(START + 90_000, 39_999.00, 1.0, True)],
    )

    class BothSides(Strategy):
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
                ctx.buy(qty=ctx.money("1"), type="LIMIT", price=ctx.money("39999.00"))
            elif bar.close_time == BAR2:
                ctx.buy(qty=ctx.money("1"))

    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 5 * MS_PER_MINUTE,
        seed=1,
        opening_balance=parse_money("1000000"),
        leverage=10,
        fees=FeeSchedule(
            maker_rate=parse_money("0.0002"),
            taker_rate=parse_money("0.0005"),
            source="test",
        ),
        latency=FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=FillTier.BOOK_TICKER,
        fill_model=BookTickerFillModel(impact_k_bps=parse_money("0")),
    )
    both = BothSides({})
    engine = BacktestEngine(
        root=lake,
        strategy=both,
        requirements=both.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
    )
    result = engine.run()

    booked = fills(result)
    maker = [f for f in booked if f["is_maker"]]
    taker = [f for f in booked if not f["is_maker"]]
    assert len(maker) == 1 and len(taker) == 1
    assert money(maker[0]["fee"]) == money("7.99980000")
    assert money(taker[0]["fee"]) == money("20.00005000")


def test_a_degraded_run_executes_at_the_resolved_tier_and_carries_the_flag(
    tmp_path: Path,
) -> None:
    """End to end: ask for `BOOK_WALK` over a range with no depth, get `BOOK_TICKER`.

    Spec 4.2 decision 3 is a promise about the whole pipeline, not about one function, so the
    scenario runs the resolution the worker runs and then feeds its verdict into an engine
    that actually executes. The run must *work* -- limit orders still fill, because
    `BOOK_TICKER` still has a book -- and must be labelled.
    """
    from perplab.engine.tiers import resolve_tier

    lake_root = tmp_path
    build_lake(
        lake_root / "market",
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=flat_book(),
    )
    resolution = resolve_tier(
        lake_root,
        ["BTCUSDT"],
        START,
        START + 5 * MS_PER_MINUTE,
        requested=FillTier.BOOK_WALK,
    )
    assert resolution.tier is FillTier.BOOK_TICKER
    assert resolution.degraded

    strategy = Scripted(lambda ctx: ctx.buy(qty=ctx.money("1")))
    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 5 * MS_PER_MINUTE,
        seed=1,
        opening_balance=parse_money("1000000"),
        leverage=10,
        latency=FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=resolution.tier,
    )
    engine = BacktestEngine(
        root=lake_root / "market",
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table(mmr=Decimal("0.004"))},
        flags=resolution.flags,
    )
    result = engine.run()

    assert result.fill_tier == "BOOK_TICKER"
    assert "TIER_DEGRADED" in result.flags
    assert len(fills(result)) == 1


def test_a_degraded_run_refuses_the_orders_the_lost_tier_would_have_allowed(
    tmp_path: Path,
) -> None:
    """The other half of a degradation: what stops working stops working *loudly*.

    A strategy placing limit orders over a range with only `aggTrades` is refused with the
    tier named, rather than running to completion having placed none. A run reporting zero
    fills is indistinguishable from a strategy that found no signal, which is the failure
    spec 4.2's rule exists to prevent.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
    )
    with pytest.raises(UnsupportedOrder) as caught:
        run(lake, Scripted(_buy_limit("39999.00", "1")), tier=FillTier.TRADE_ONLY)
    assert "bookTicker or depth20 coverage" in str(caught.value)


def test_two_identical_runs_over_tick_data_produce_the_same_event_hash(
    tmp_path: Path,
) -> None:
    """Spec 12.1's invariant, at the tier where there is most to get wrong.

    `BAR_CLOSE` has a few thousand events and one price series. `BOOK_TICKER` merges a trade
    tape with a lazily-pulled book, schedules fill checks from inside the loop, and consumes a
    queue whose bound depends on the order observations arrived in. Every one of those is a
    place where an answer could become a function of dictionary ordering or of how many rows
    a batch happened to hold.
    """
    lake = _limit_lake(
        tmp_path,
        queue_size=1.0,
        prints=[
            (START + 90_000, 39_999.00, 1.0, True),
            (START + 91_000, 39_999.00, 1.0, True),
            (START + 92_000, 39_998.90, 0.5, True),
        ],
    )
    hashes = {
        run(lake, Scripted(_buy_limit("39999.00", "2")), tier=FillTier.BOOK_TICKER).event_hash
        for _ in range(3)
    }
    assert len(hashes) == 1


def test_a_book_walk_model_never_charges_impact_and_a_book_ticker_model_never_walks(
    tmp_path: Path,
) -> None:
    """The two book tiers price the same order differently, and the difference is the data.

    Same ladder, same order of 3. `BOOK_WALK` sees three levels and walks two of them;
    `BOOK_TICKER` sees only the touch and charges spec 6.4's impact term for the rest. Both
    are more expensive than the touch alone, which is the property that matters -- but they
    are more expensive for different, stated reasons.
    """
    depth = [
        (
            START + second * 1000,
            [(39_999.90, 10.0)],
            [(40_000.10, 1.0), (40_000.20, 5.0)],
        )
        for second in range(240)
    ]
    walk_lake = tmp_path / "walk" / "market"
    build_lake(
        walk_lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=flat_book(),
        depth=depth,
    )
    ticker_lake = tmp_path / "ticker" / "market"
    build_lake(
        ticker_lake,
        start_ms=START,
        minutes=5,
        trade_path=flat_path(40_000.0),
        ticks=steady_ticks(),
        quotes=flat_book(),
    )

    walked = run(
        walk_lake,
        Scripted(lambda ctx: ctx.buy(qty=ctx.money("3"))),
        tier=FillTier.BOOK_WALK,
        fill_model=BookWalkFillModel(),
    )
    quoted = run(
        ticker_lake,
        Scripted(lambda ctx: ctx.buy(qty=ctx.money("3"))),
        tier=FillTier.BOOK_TICKER,
        fill_model=BookTickerFillModel(),
    )

    walk_fill = fills(walked)[0]
    ticker_fill = fills(quoted)[0]
    assert walk_fill["levels_walked"] == 2
    assert "impact_bps" not in walk_fill
    assert ticker_fill["levels_walked"] == 1
    assert float(ticker_fill["impact_bps"]) > 0
    assert float(walk_fill["price"]) > 40_000.10
    assert float(ticker_fill["price"]) > 40_000.10
