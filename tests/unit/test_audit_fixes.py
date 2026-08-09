"""The final audit batch, one hand-derived number at a time.

Each class below pins one audit finding. The standard is the golden-scenario one: every
expected value is derived in the test's own docstring from inputs the test wrote, so the
test fails the moment the model changes rather than recording whatever the engine printed.
The findings covered here, by their audit ids:

- **H11** -- book visibility gates on the receive clock, not the exchange stamp.
- **H12** -- `ctx` is sealed and its runtime is not casually reachable.
- **H14** -- protective orders name a position side in hedge mode.
- **H21** -- a marketable-limit remainder resting through the touch fills as taker.
- **H22** -- post-only cannot be adjudicated against a book that is not there.
- **M5**  -- per-symbol dataset ids in the tick replay, mirroring `live_dataset_id`.
- **M6**  -- `last_print` reads are staleness-bounded like every other market read.
- **M9**  -- trade statistics observe the same `[start_ms, end_ms]` window as everything.
- **M10** -- `BacktestConfig.to_json` writes `hedge_mode` like its docstring promises.
- **M11** -- a withheld funding settlement says so on the strategy-visible view.
- **M15** -- the persistent kill switch is armed at halt time, crash-ordered first.
- **L1**  -- the slippage cross-check refuses to compare against a capped event log.
- **L3**  -- duplicate shadow fill identities surface instead of vanishing.
- **L9**  -- the context and the indicator set must agree on the default symbol.
- **L10** -- cancel paths work during warm-up; `modify` stays gated, on purpose.
- **L11** -- `SpreadView.mid` is exact and `mid_price` is the order-safe spelling.
- **L12** -- `ctx.money` refuses floats whose integer part already carries residue.
- **L16** -- the CSV export neutralises spreadsheet formula injection.
- **M34** -- `ctx.record` refuses the non-finite floats `ctx.money` always refused.
- **M35** -- emitted payloads are snapshots; mutating a logged dict cannot edit the log.
- **M36** -- crossings between indicators on different feeds are refused by name.
"""

from __future__ import annotations

import random
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from perplab.analytics.metrics import trade_stats
from perplab.analytics.parity import FillRecord, _compare_fills
from perplab.api.routers.runs import _csv_safe, _downsample_extremes
from perplab.core.account import FeeSchedule
from perplab.core.money import SCALE, parse_money
from perplab.core.risk import KillSwitch, RiskAction, RiskBreach
from perplab.core.types import Bar, PositionSide
from perplab.engine.backtest import AutoFlatten, BacktestConfig, BacktestEngine
from perplab.engine.clock import EventQueue
from perplab.engine.feed import FundingPoint
from perplab.engine.latency import FixedLatency
from perplab.engine.ticks import (
    TradePrint,
    book_ticker_stream,
    depth_events,
    depth_stream,
    trade_events,
)
from perplab.live.worker import _arm_tripped_switch
from perplab.store.killswitch import KillSwitchArmed, KillSwitchStore
from perplab.strategy.base import Strategy
from perplab.strategy.context import (
    Context,
    FillTier,
    OrderIntent,
    OrderType,
    PositionView,
    SpreadView,
    WarmupViolation,
)
from perplab.strategy.dryrun import DryRunRuntime, event_hash
from perplab.strategy.indicators import CVD, MACD, SMA, DerivedSeries, IndicatorSet
from tests.engine_lake import (
    MS_PER_MINUTE,
    build_lake,
    flat_path,
    write_book_ticker,
    write_depth,
    write_klines,
    write_marks,
    write_trades,
)
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000  # 2024-03-01T00:00:00Z
BAR1 = START + MS_PER_MINUTE - 1
BAR2 = START + 2 * MS_PER_MINUTE - 1
LATENCY = 10
SYMBOL = "BTCUSDT"


def money(text: str) -> Decimal:
    return parse_money(text)


# --------------------------------------------------------------------------- harnesses


class Scripted(Strategy):
    """Runs one caller-supplied action on the bar whose close matches `at`."""

    requires = {
        "symbols": [SYMBOL],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }

    def __init__(self, action, *, at: int = BAR1) -> None:
        super().__init__({})
        self._action = action
        self._at = at

    def on_start(self, ctx) -> None:
        self.order_id = None

    def on_bar(self, ctx, bar) -> None:
        if ctx.warm and bar.close_time == self._at:
            self.order_id = self._action(ctx)


def run(
    root: Path,
    strategy: Strategy,
    *,
    tier: FillTier,
    minutes: int = 5,
    fees: FeeSchedule | None = None,
    auto_flatten: AutoFlatten | None = None,
):
    config = BacktestConfig(
        symbols=(SYMBOL,),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
        seed=1,
        opening_balance=money("1000000"),
        leverage=10,
        latency=FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=tier,
        **({} if fees is None else {"fees": fees}),
        **({} if auto_flatten is None else {"auto_flatten": auto_flatten}),
    )
    engine = BacktestEngine(
        root=root,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={SYMBOL: btcusdt_filters()},
        brackets={SYMBOL: single_bracket_table(mmr=Decimal("0.004"))},
    )
    return engine.run()


def kinds(result, kind: str) -> list[dict]:
    return [dict(e.payload) for e in result.events if e.kind == kind]


def steady_ticks(count: int = 240, price: float = 40_000.0, qty: float = 1.0):
    """One print per second, sell-aggressive on even seconds (`is_buyer_maker=True`)."""
    return [
        (START + second * 1000, price, qty, second % 2 == 0) for second in range(count)
    ]


def flat_book(count: int = 240, bid: float = 39_999.9, ask: float = 40_000.1, size: float = 0.5):
    return [(START + second * 1000, bid, size, ask, size) for second in range(count)]


class NoSource:
    """A `MarketSource` supplying nothing, for engines whose methods are driven directly."""

    def prepare(self, engine: BacktestEngine) -> Any:
        from perplab.engine.source import Prepared

        return Prepared(streams=(), data_start_ms=engine.config.start_ms, total_bars=0)

    def close(self) -> None:
        """Nothing to release."""


class Quiet(Strategy):
    requires = {
        "symbols": [SYMBOL],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines"],
    }


def bare_engine(tmp_path: Path) -> BacktestEngine:
    """An engine with a mark and a clock but no lake, for driving internals directly."""
    strategy = Quiet()
    config = BacktestConfig(
        symbols=(SYMBOL,),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 60_000,
        seed=1,
        opening_balance=money("1000000"),
        leverage=10,
        latency=FixedLatency(submit=LATENCY, cancel=LATENCY),
        fill_tier=FillTier.BAR_CLOSE,
    )
    engine = BacktestEngine(
        root=tmp_path,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={SYMBOL: btcusdt_filters()},
        brackets={SYMBOL: single_bracket_table(mmr=Decimal("0.004"))},
        source=NoSource(),
    )
    engine.runtime.advance(START)
    engine.account.update_mark(START, SYMBOL, money("40000"))
    return engine


# ================================================= H11: visibility keys on the receive clock


class TestH11BookVisibilityGatesOnReceiveClock:
    def test_a_depth_row_is_invisible_until_the_platform_had_it(self, tmp_path: Path) -> None:
        """One row: stamped `START+1000`, received 500 ms later.

        Gated on `ts_ms` the ladder answered at `START+1000` -- half a second before the
        collector had it, which is the systematic look-ahead the audit measured at 86 ms on
        the real feed. Gated on `COALESCE(recv_ms, ts_ms)` it appears at exactly
        `START+1500`, and the snapshot still carries its exchange stamp for the staleness
        bound (visible from recv, aged from ts -- the project's two-clock rule).
        """
        write_depth(
            tmp_path,
            SYMBOL,
            [(START + 1_000, [(39_999.9, 1.0)], [(40_000.1, 1.0)])],
            recv_offset_ms=500,
        )
        stream = depth_stream(tmp_path, SYMBOL, START, START + 10_000)
        assert stream.advance_to(START + 1_000) is None
        assert stream.advance_to(START + 1_499) is None
        snapshot = stream.advance_to(START + 1_500)
        assert snapshot is not None
        assert snapshot.ts_ms == START + 1_000
        assert snapshot.recv_ms == START + 1_500
        stream.close()

    def test_a_book_ticker_row_is_invisible_until_the_platform_had_it(
        self, tmp_path: Path
    ) -> None:
        """The same rule for the touch: the schema carries `recv_ms` (null on bulk rows,
        hence the COALESCE), and the quote a strategy's mid comes from must not exist
        before the platform received it."""
        write_book_ticker(
            tmp_path,
            SYMBOL,
            [(START + 1_000, 39_999.9, 1.0, 40_000.1, 1.0)],
            recv_offset_ms=500,
        )
        stream = book_ticker_stream(tmp_path, SYMBOL, START, START + 10_000)
        assert stream.advance_to(START + 1_499) is None
        top = stream.advance_to(START + 1_500)
        assert top is not None and top.ts_ms == START + 1_000
        stream.close()

    def test_depth_events_dispatch_at_the_visible_instant_with_the_exchange_stamp(
        self, tmp_path: Path
    ) -> None:
        """The event replay (depth-driven indicators) orders on the visible clock too:
        the `Event` rides at `recv`, the payload keeps `ts`, so `MarketView` ages the
        ladder from when it held rather than from when it arrived."""
        write_depth(
            tmp_path,
            SYMBOL,
            [(START + 1_000, [(39_999.9, 1.0)], [(40_000.1, 1.0)])],
            recv_offset_ms=500,
        )
        events = list(depth_events(tmp_path, [SYMBOL], START, START + 10_000))
        assert len(events) == 1
        assert events[0].ts_ms == START + 1_500
        assert events[0].payload.ts_ms == START + 1_000
        assert events[0].dataset_id == f"depth20:{SYMBOL}"


# =========================================== M5: per-symbol dataset ids in the tick replay


class TestM5PerSymbolDatasetIds:
    def test_two_symbols_sharing_an_agg_id_in_one_millisecond_do_not_tie(
        self, tmp_path: Path
    ) -> None:
        """Binance allocates `agg_id` per symbol, so both fixtures carry `agg_id=1` at the
        same millisecond. Under one shared `"aggTrades"` dataset id that is two identical
        total-order keys and `EventQueue.pop` raises `OrderingViolation` mid-run; under
        `aggTrades:<symbol>` -- `reorder.live_dataset_id`'s rule -- the keys differ in
        their last component and both events dispatch.
        """
        write_trades(tmp_path, "BTCUSDT", [(START, 40_000.0, 1.0, True)])
        write_trades(tmp_path, "ETHUSDT", [(START, 2_000.0, 1.0, True)])
        queue = EventQueue()
        queue.add_stream(
            trade_events(tmp_path, ["BTCUSDT", "ETHUSDT"], START - 1_000, START + 1_000)
        )
        popped = [queue.pop() for _ in range(2)]
        assert {e.dataset_id for e in popped} == {"aggTrades:BTCUSDT", "aggTrades:ETHUSDT"}
        assert all(e.source_seq == 1 for e in popped)

    def test_two_symbols_sharing_a_depth_update_id_do_not_tie(self, tmp_path: Path) -> None:
        """The acute case: depth20 is 1 s-sampled, so every symbol lands on the same
        aligned millisecond, and the fixture writer numbers `last_update_id` from 1 per
        symbol exactly as the venue does."""
        ladder = ([(39_999.9, 1.0)], [(40_000.1, 1.0)])
        write_depth(tmp_path, "BTCUSDT", [(START, *ladder)])
        write_depth(tmp_path, "ETHUSDT", [(START, *ladder)])
        queue = EventQueue()
        queue.add_stream(depth_events(tmp_path, ["BTCUSDT", "ETHUSDT"], START - 1, START + 1))
        popped = [queue.pop() for _ in range(2)]
        assert {e.dataset_id for e in popped} == {"depth20:BTCUSDT", "depth20:ETHUSDT"}


# =========================== H21: a remainder resting through the touch is taker flow


class TestH21CrossedRemainderIsTakerFlow:
    FEES = FeeSchedule(
        maker_rate=parse_money("0.0002"), taker_rate=parse_money("0.0005"), source="audit"
    )

    def test_the_remainder_of_a_marketable_limit_through_the_touch_pays_taker(
        self, tmp_path: Path
    ) -> None:
        """A buy limit of 2 @ 40 010 against a 0.5 ask at 40 000.1.

        The visible touch fills 0.5 as taker; the 1.5 remainder rests at 40 010 -- a price
        *through* the ask, where the venue would have kept taking liquidity the model
        cannot see. Every subsequent print that fills it (sell-aggressive prints at 40 000,
        via the through branch) must therefore be charged as taker flow:

            fill 1: 0.5 x 40 000.1 x 0.0005 = 10.000025   (the cross)
            fill 2: 1.0 x 40 010   x 0.0005 = 20.005      (was maker: 8.002)
            fill 3: 0.5 x 40 010   x 0.0005 = 10.0025     (was maker: 4.001)

        Before the fix, fills 2 and 3 arrived `is_maker=True` -- a systematic ~3 bps
        understatement on the majority of every marketable limit larger than the touch.
        """
        lake = tmp_path / "market"
        build_lake(
            lake,
            trade_path=flat_path(40_000.0),
            ticks=steady_ticks(),
            quotes=flat_book(),
        )
        strategy = Scripted(
            lambda ctx: ctx.buy(qty=money("2"), type=OrderType.LIMIT, price=money("40010"))
        )
        result = run(lake, strategy, tier=FillTier.BOOK_TICKER, fees=self.FEES)

        fills = kinds(result, "FILL")
        assert [f["is_maker"] for f in fills] == [False, False, False]
        assert [money(f["fee"]) for f in fills] == [
            money("10.000025"),
            money("20.005"),
            money("10.0025"),
        ]
        assert result.maker_fills == 0

    def test_a_remainder_resting_exactly_at_the_touch_still_earns_maker(
        self, tmp_path: Path
    ) -> None:
        """The control: a buy limit at exactly the ask, 40 000.1.

        The published touch size is consumed in full and the remainder becomes the new
        best bid *at that price* -- it genuinely rests, so through-prints at 40 000 fill
        it as maker at the maker rate:

            fill 2: 1.0 x 40 000.1 x 0.0002 = 8.00002
            fill 3: 0.5 x 40 000.1 x 0.0002 = 4.00001

        This is the boundary that keeps the H21 fix from over-charging honest passive
        remainders: only a price *strictly* through the touch is taker flow.
        """
        lake = tmp_path / "market"
        build_lake(
            lake,
            trade_path=flat_path(40_000.0),
            ticks=steady_ticks(),
            quotes=flat_book(),
        )
        strategy = Scripted(
            lambda ctx: ctx.buy(qty=money("2"), type=OrderType.LIMIT, price=money("40000.1"))
        )
        result = run(lake, strategy, tier=FillTier.BOOK_TICKER, fees=self.FEES)

        fills = kinds(result, "FILL")
        assert [f["is_maker"] for f in fills] == [False, True, True]
        assert [money(f["fee"]) for f in fills] == [
            money("10.000025"),
            money("8.00002"),
            money("4.00001"),
        ]
        assert result.maker_fills == 2


# ======================= H22: post-only cannot be adjudicated against an absent book


class TestH22PostOnlyDuringABookOutage:
    def test_a_crossing_gtx_order_is_refused_when_no_book_is_in_force(
        self, tmp_path: Path
    ) -> None:
        """Quotes cover only the first minute; the GTX order arrives in the third.

        At arrival (`BAR2 + 10 ms`) the youngest quote is 61 s old -- past
        `MAX_QUOTE_STALENESS_MS`, so `MarketView` reports no book at all. `cross_book`
        then returns zero quantity, and the old `available.qty > 0` rejection test read
        that outage as "does not cross": a plainly-crossing post-only order rested inside
        the spread and later collected maker fills through H21's branch -- the failure
        direction that flatters. With no observation the question has no answer, so the
        order expires loudly instead of resting on silence.
        """
        lake = tmp_path / "market"
        build_lake(
            lake,
            trade_path=flat_path(40_000.0),
            ticks=steady_ticks(),
            quotes=flat_book(count=60),
        )
        strategy = Scripted(
            lambda ctx: ctx.buy(
                qty=money("1"), type=OrderType.LIMIT, price=money("40010"), tif="GTX"
            ),
            at=BAR2,
        )
        result = run(lake, strategy, tier=FillTier.BOOK_TICKER)

        assert kinds(result, "FILL") == []
        expires = kinds(result, "EXPIRE")
        assert len(expires) == 1
        assert "cannot be adjudicated" in expires[0]["reason"]


# ============================== M6: last_print reads are bounded like every other read


class TestM6StalePrintsRefuseToPriceFills:
    def test_an_auto_flatten_inside_a_kline_hole_is_refused_not_filled_at_the_pre_hole_price(
        self, tmp_path: Path
    ) -> None:
        """Klines cover minutes 0-2, then nothing until minute 60; marks cover everything.

        The strategy buys at the first bar close (fills at the bar-close print 40 000
        plus the 1 bp BAR_CLOSE offset: 40 004.0, at `START+60 009`). `max_hold_ms` of
        ten minutes then falls due inside the hole. Every retry that arrives there finds
        the last print older than the BAR_CLOSE bound (one timeframe + 60 s = 120 s;
        the print is 9+ minutes old) and is *refused* -- before the fix each would have
        filled at the pre-hole price. The exit that finally lands is the retry submitted
        at the minute-59 mark close, which arrives at `START+3 600 009`, ten
        milliseconds after minute 60's opening print revives the tape:

            exit price = 40 000 x (1 - 1/10 000) = 39 996.0 (sell, floor to 0.1)

        So the position is held through the hole -- reported, via `AUTO_FLATTEN_UNMET` --
        and closes at the first honest price, not at a fiction from before the gap.
        """
        lake = tmp_path / "market"
        write_klines(lake, SYMBOL, START, 3, flat_path(40_000.0))
        write_klines(lake, SYMBOL, START + 60 * MS_PER_MINUTE, 5, flat_path(40_000.0))
        write_marks(lake, SYMBOL, START, 65, flat_path(40_000.0))

        strategy = Scripted(lambda ctx: ctx.buy(qty=money("1")))
        result = run(
            lake,
            strategy,
            tier=FillTier.BAR_CLOSE,
            minutes=65,
            auto_flatten=AutoFlatten(max_hold_ms=10 * MS_PER_MINUTE),
        )

        fills = kinds(result, "FILL")
        assert len(fills) == 2, "one entry, one exit -- and nothing inside the hole"
        assert money(fills[0]["price"]) == money("40004")
        assert fills[1]["ts_ms"] == START + 60 * MS_PER_MINUTE + LATENCY - 1
        assert money(fills[1]["price"]) == money("39996")

        refusals = kinds(result, "PLATFORM_ORDER_REJECTED")
        assert refusals, "the in-hole retries must be refused, loudly"
        assert all("no trade print" in r["reason"] for r in refusals)
        assert "AUTO_FLATTEN_UNMET" in result.flags


# =============================================== M9: trade statistics observe the window


class TestM9TradeStatsAreWindowed:
    def test_a_trade_closed_after_the_window_counts_as_open_not_as_a_loss(self) -> None:
        """Three trades: +10 closed at 100 (in-window), -5 closed at 250 (a drain fill,
        past `end_ms=200`), and one genuinely open.

        The windowed reading: one round trip, one win, `win_rate=1.0`, expectancy +10,
        and *two* open trades -- the still-open one and the one that was still open as of
        the window's end. The un-windowed call (no bounds) keeps the whole-table reading,
        which is what a caller windowing nothing gets.
        """
        closed_in = SimpleNamespace(
            is_open=False, exit_ms=100, net_pnl=10.0, duration_ms=50,
            close_reason="signal", legs=2,
        )
        closed_after = SimpleNamespace(
            is_open=False, exit_ms=250, net_pnl=-5.0, duration_ms=50,
            close_reason="signal", legs=2,
        )
        still_open = SimpleNamespace(
            is_open=True, exit_ms=None, net_pnl=1000.0, duration_ms=None,
            close_reason="open", legs=1,
        )
        trades = [closed_in, closed_after, still_open]

        windowed = trade_stats(trades, start_ms=0, end_ms=200)
        assert windowed.round_trips == 1
        assert windowed.wins == 1 and windowed.losses == 0
        assert windowed.win_rate == 1.0
        assert windowed.expectancy == pytest.approx(10.0)
        assert windowed.open_trades == 2

        whole = trade_stats(trades)
        assert whole.round_trips == 2
        assert whole.open_trades == 1


# ==================================== M10: the manifest records the position mode


class TestM10HedgeModeReachesTheManifest:
    def test_two_runs_differing_only_in_hedge_mode_produce_different_manifests(self) -> None:
        one_way = BacktestConfig(
            symbols=(SYMBOL,), timeframe="1m", start_ms=START, end_ms=START + 1
        )
        hedged = BacktestConfig(
            symbols=(SYMBOL,), timeframe="1m", start_ms=START, end_ms=START + 1,
            hedge_mode=True,
        )
        assert one_way.to_json()["hedge_mode"] is False
        assert hedged.to_json()["hedge_mode"] is True
        assert one_way.to_json() != hedged.to_json()


# =========================== M11: a withheld settlement says so where strategies read


class TestM11WithheldFundingIsVisible:
    def test_funding_with_no_mark_flags_the_view_and_a_booked_one_clears_it(
        self, tmp_path: Path
    ) -> None:
        """The settlement rate is real market data either way; what differs is whether
        the ledger booked the cashflow. With no mark, spec 3.4 withholds it -- and the
        strategy-visible view must say so, or a carry strategy trades a payment its own
        run's `funding_pnl` never received."""
        engine = bare_engine(tmp_path)
        del engine.account.marks[SYMBOL]

        engine._on_funding(START, FundingPoint(symbol=SYMBOL, ts_ms=START, rate=10_000))
        view = engine.runtime.funding_view(SYMBOL)
        assert view.last_rate == money("0.0001")
        assert view.last_withheld is True
        assert engine.counts["unsettled_funding"] == 1
        assert "FUNDING_UNSETTLED" in engine.flags

        engine.runtime.advance(START + 1)
        engine.account.update_mark(START + 1, SYMBOL, money("40000"))
        engine._on_funding(START + 1, FundingPoint(symbol=SYMBOL, ts_ms=START + 1, rate=10_000))
        assert engine.runtime.funding_view(SYMBOL).last_withheld is False


# ================= M15: the persistent kill switch is armed before the halt can block


class TestM15KillSwitchArmsAtHaltTime:
    def test_on_halt_runs_before_the_halt_touches_the_book(self, tmp_path: Path) -> None:
        """The callback is the crash-ordering point: at the moment it runs, the resting
        order the halt is about to cancel must still be open. A callback invoked after
        the cancels would re-create the window it exists to close -- armed on disk only
        once the network-bound half of the halt had already survived."""
        engine = bare_engine(tmp_path)
        order_id = engine._submit(OrderIntent(symbol=SYMBOL, side="BUY", qty=money("1")))
        seen: list[bool] = []
        engine.on_halt = lambda: seen.append(engine.orders[order_id].is_open)

        engine.request_halt(
            RiskBreach(
                limit="max_drawdown_pct", action=RiskAction.HALT, ts_ms=START,
                observed="0.20", allowed="0.15", detail="drawdown limit",
            )
        )
        assert engine.perform_pending_halt() is True
        assert seen == [True], "on_halt must fire exactly once, before the cancels"
        assert not engine.orders[order_id].is_open

    def test_arm_tripped_switch_writes_the_trip_the_next_session_must_see(
        self, tmp_path: Path
    ) -> None:
        """The worker half: an in-memory trip becomes a SQLite row, `flattened=False`
        because nothing has been closed at halt time, and `require_clear()` then refuses
        the next session -- which is spec 7.6's whole point, and what an OOM kill after
        the halt used to erase."""
        switch = KillSwitch()
        switch.trip(123_456, "max_drawdown_pct", "dd 20% > 15%")
        stub = SimpleNamespace(risk=SimpleNamespace(kill_switch=switch))

        _arm_tripped_switch(tmp_path, 7, stub)

        with KillSwitchStore(tmp_path) as store:
            trip = store.state()
            assert trip is not None
            assert (trip.armed_ms, trip.trigger, trip.run_id) == (123_456, "max_drawdown_pct", 7)
            assert trip.flattened is False
            with pytest.raises(KillSwitchArmed):
                store.require_clear()

    def test_an_untripped_switch_writes_nothing(self, tmp_path: Path) -> None:
        stub = SimpleNamespace(risk=SimpleNamespace(kill_switch=KillSwitch()))
        _arm_tripped_switch(tmp_path, 7, stub)
        with KillSwitchStore(tmp_path) as store:
            assert store.state() is None


# =================== L1: the slippage cross-check refuses a capped event log


class TestL1CappedLogSkipsTheCrossCheck:
    def test_a_capped_log_is_a_documented_skip_not_an_abort(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With the cap patched to 3 and three non-FILL events logged, the rebuilt figure
        (0) disagrees with the accumulator (1) -- but the two cover different populations
        once the log has dropped events, so the check must stand down and say so rather
        than abort blaming the accumulator for the log's ceiling."""
        engine = bare_engine(tmp_path)
        monkeypatch.setattr("perplab.engine.backtest.MAX_EVENTS", 3)
        for _ in range(3):
            engine.runtime.emit("LOG", {"level": "INFO", "message": "x", "fields": {}})
        engine.slippage_cost = money("1")

        engine._check_slippage_accumulator()  # RunAborted before the fix
        assert any("cross-check was skipped" in w for w in engine.warnings)

    def test_an_uncapped_mismatch_still_aborts(self, tmp_path: Path) -> None:
        from perplab.engine.backtest import RunAborted

        engine = bare_engine(tmp_path)
        engine.slippage_cost = money("1")
        with pytest.raises(RunAborted, match="slippage accumulator disagrees"):
            engine._check_slippage_accumulator()


# =========================== L3: duplicate shadow identities surface, never vanish


class TestL3DuplicateShadowIdentities:
    @staticmethod
    def record(seq: int, *, order_id: str = "o1", fill_no: int = 0, price: str = "40000"):
        return FillRecord(
            ts_ms=START + seq, seq=seq, symbol=SYMBOL, side="BUY",
            qty=parse_money("1"), price=parse_money(price), order_id=order_id,
            fill_no=fill_no, tag=None,
        )

    def test_every_shadow_fill_lands_in_exactly_one_population(self) -> None:
        """Two shadow records claiming identity `(o1, 0)`, one paper record.

        The first occurrence keeps the identity and matches; the duplicate cannot be
        paired -- which of the two the paper fill matches is exactly what is ambiguous --
        so it goes to `shadow_only`. The invariant the fix restores:
        `matched + len(shadow_only) == shadow_count`, so the average bps is computed over
        the population the counts beside it claim.
        """
        paper = [self.record(1)]
        shadow = [self.record(1), self.record(2, price="40010")]
        outcome = _compare_fills(iter(paper), iter(shadow))
        assert outcome.shadow_count == 2
        assert outcome.matched == 1
        assert len(outcome.shadow_only) == 1
        assert outcome.matched + len(outcome.shadow_only) == outcome.shadow_count


# ======================================= H12: ctx is sealed, its runtime out of reach


class TestH12ContextIsSealed:
    @staticmethod
    def build(*, warm: bool = True):
        runtime = DryRunRuntime(symbols=(SYMBOL,))
        runtime.advance(START)
        runtime.set_mark(SYMBOL, parse_money("60000"))
        context = Context(
            _runtime=runtime,
            symbols=(SYMBOL,),
            timeframe="15m",
            indicators=IndicatorSet(primary_symbol=SYMBOL, bar_ms=900_000),
            rng=random.Random(1),
        )
        context._set_warm(warm)
        return context, runtime

    def test_flipping_the_warm_gate_fails_loudly(self) -> None:
        """`ctx.buy` during warm-up raises `WarmupViolation` -- and `ctx._warm = True`
        used to walk straight around it. The engine's own mutators still work."""
        ctx, _ = self.build(warm=False)
        with pytest.raises(AttributeError, match="sealed"):
            ctx._warm = True
        assert ctx.warm is False
        ctx._set_warm(True)
        assert ctx.warm is True

    def test_any_attribute_write_after_construction_is_refused(self) -> None:
        ctx, _ = self.build()
        with pytest.raises(AttributeError, match="sealed"):
            ctx.symbols = ("ETHUSDT",)
        with pytest.raises(AttributeError, match="sealed"):
            ctx.scratch = {}

    def test_the_runtime_is_not_reachable_from_the_context(self) -> None:
        """`ctx._runtime.submit(...)` bypassed the warm gate and `ctx._runtime.account`
        was the live ledger. The attribute no longer exists on the instance at all, the
        read raises with the public surface named, and neither `vars` nor `dir` offers a
        runtime-shaped handle to stumble on."""
        ctx, _ = self.build()
        with pytest.raises(AttributeError, match="engine-private"):
            ctx._runtime
        assert not hasattr(ctx, "_runtime")
        assert "_runtime" not in vars(ctx)
        assert all("runtime" not in name.lower() for name in dir(ctx))

    def test_the_public_surface_still_works_through_the_hidden_runtime(self) -> None:
        ctx, runtime = self.build()
        assert ctx.now == START
        assert ctx.mark() == parse_money("60000")
        order_id = ctx.buy(qty=parse_money("0.5"))
        assert order_id is not None
        assert any(e.kind == "ORDER" for e in runtime.events)


class TestH12dIndicatorDispatchIsEngineOnly:
    """`ctx.indicators` delegates registration and reads but refuses dispatch.

    `freeze()` closed *registration* after the first bar, but the dispatch surface stayed
    open through the context: `ctx.indicators.on_bar(bar)` from a hook advanced every
    registered series off a bar the market never printed, permanently shifting each one
    against the price. The context now carries a `SealedIndicators` facade while the
    engine keeps and drives the set it constructed, so the accidental spelling fails
    loudly at the line that did it and the engine's own dispatch is untouched.
    """

    DISPATCH = ("freeze", "on_bar", "on_trade", "on_depth", "on_funding", "on_open_interest")

    @staticmethod
    def build() -> tuple[Context, IndicatorSet]:
        real = IndicatorSet(primary_symbol=SYMBOL, bar_ms=900_000)
        context = Context(
            _runtime=DryRunRuntime(symbols=(SYMBOL,)),
            symbols=(SYMBOL,),
            timeframe="15m",
            indicators=real,
            rng=random.Random(1),
        )
        return context, real

    def test_registration_and_reads_pass_through(self) -> None:
        ctx, real = self.build()
        sma = ctx.indicators.sma(2)
        assert sma in real.all()
        assert ctx.indicators.warmup == sma.warmup
        assert ctx.indicators.primary_symbol == SYMBOL

    def test_dispatch_through_the_context_is_refused(self) -> None:
        ctx, _ = self.build()
        for name in self.DISPATCH:
            with pytest.raises(AttributeError, match="engine-only"):
                getattr(ctx.indicators, name)

    def test_the_engine_still_drives_the_set_it_kept(self) -> None:
        """SMA(2) over closes 100 and 102 is (100 + 102) / 2 = 101, driven post-freeze
        through the engine's own reference while `ctx.indicators` reads the result."""
        ctx, real = self.build()
        sma = ctx.indicators.sma(2)
        real.freeze()
        for index, close in enumerate((100, 102)):
            real.on_bar(
                Bar(
                    symbol=SYMBOL,
                    open_time=START + index * 900_000,
                    close_time=START + (index + 1) * 900_000 - 1,
                    open=close * SCALE,
                    high=close * SCALE,
                    low=close * SCALE,
                    close=close * SCALE,
                    volume=SCALE,
                    quote_volume=close * SCALE,
                    trades=1,
                )
            )
        assert sma.value == 101.0
        assert ctx.indicators.all_ready is True

    def test_the_facade_refuses_writes(self) -> None:
        ctx, _ = self.build()
        with pytest.raises(AttributeError, match="engine-owned"):
            ctx.indicators.scratch = 1


# ====================== H14: protective orders name their leg in hedge mode


class HedgeRuntime:
    """The smallest `Runtime` that is a hedge account: +2 long, -1 short, and a recorder."""

    def __init__(self) -> None:
        self.intents: list[OrderIntent] = []
        self.hedge_mode = True
        self.fill_tier = FillTier.BOOK_TICKER
        self.now_ms = START

    def position_view(self, symbol: str, position_side: PositionSide = PositionSide.BOTH):
        qty = {
            PositionSide.LONG: parse_money("2"),
            PositionSide.SHORT: parse_money("-1"),
        }.get(position_side, parse_money("0"))
        return PositionView(
            symbol=symbol, qty=qty, entry_price=parse_money("40000"),
            unrealized_pnl=parse_money("0"), liquidation_price=None,
            margin=parse_money("0"), position_side=position_side,
        )

    def submit(self, intent: OrderIntent) -> str:
        self.intents.append(intent)
        return f"o{len(self.intents)}"

    def emit(self, kind: str, payload) -> None:  # pragma: no cover - unused here
        pass


class TestH14HedgeProtectiveOrders:
    @staticmethod
    def build() -> tuple[Context, HedgeRuntime]:
        runtime = HedgeRuntime()
        ctx = Context(
            _runtime=runtime,
            symbols=(SYMBOL,),
            timeframe="1m",
            indicators=IndicatorSet(primary_symbol=SYMBOL, bar_ms=60_000),
            rng=random.Random(1),
        )
        ctx._set_warm(True)
        return ctx, runtime

    def test_a_stop_loss_protects_the_named_leg_and_only_that_leg(self) -> None:
        """The long is 2, the short is -1. A stop on the LONG side is a SELL for the
        long's own size, routed to LONG, with `reduce_only` omitted (a hedged side
        refuses the flag; a sell routed to LONG can only reduce it)."""
        ctx, runtime = self.build()
        order_id = ctx.stop_loss(stop_price=parse_money("39000"), position_side="LONG")
        assert order_id == "o1"
        intent = runtime.intents[0]
        assert intent.type is OrderType.STOP_MARKET
        assert intent.side == "SELL"
        assert intent.qty == parse_money("2")
        assert intent.position_side is PositionSide.LONG
        assert intent.reduce_only is False

    def test_a_trailing_stop_on_the_short_is_a_buy_for_the_short_size(self) -> None:
        ctx, runtime = self.build()
        ctx.trailing_stop(callback_rate=parse_money("0.01"), position_side="SHORT")
        intent = runtime.intents[0]
        assert intent.type is OrderType.TRAILING_STOP_MARKET
        assert intent.side == "BUY"
        assert intent.qty == parse_money("1")
        assert intent.position_side is PositionSide.SHORT

    def test_a_take_profit_without_a_side_is_refused_in_hedge_mode(self) -> None:
        """Before the fix the three helpers simply did not accept the parameter, which
        made this refusal a dead end: hedge mode had no way to attach a protective order
        at all. The refusal itself is correct -- which leg is protected is not guessable."""
        ctx, _ = self.build()
        with pytest.raises(ValueError, match="hedge mode"):
            ctx.take_profit(stop_price=parse_money("41000"))


# ===================== L9: the default-symbol resolvers must agree at construction


class TestL9DefaultSymbolAgreement:
    def test_a_primary_symbol_disagreement_is_refused(self) -> None:
        """`symbols=("ETHUSDT","BTCUSDT")` with `primary_symbol="BTCUSDT"`: `ctx.buy()`
        would trade ETH while `ctx.indicators.ema(20)` fed off BTC -- one instrument
        traded off the other's indicator, with no error anywhere. Refused at the only
        moment both fields are in one place."""
        runtime = DryRunRuntime(symbols=("ETHUSDT", "BTCUSDT"))
        with pytest.raises(ValueError, match="primary symbol"):
            Context(
                _runtime=runtime,
                symbols=("ETHUSDT", "BTCUSDT"),
                timeframe="1m",
                indicators=IndicatorSet(
                    primary_symbol="BTCUSDT", symbols=("ETHUSDT", "BTCUSDT"), bar_ms=60_000
                ),
                rng=random.Random(1),
            )


# ================== L10: cancel paths work during warm-up; modify stays gated


class TestL10WarmupGateAsymmetry:
    def test_cancel_paths_are_usable_during_warmup_and_modify_is_not(self) -> None:
        """A cancel can only remove risk -- a live session that adopts working orders
        must be able to pull them before warm-up completes. An amendment re-prices a
        working order off an unfilled window, which is exactly what the gate forbids."""
        ctx, _ = TestH12ContextIsSealed.build(warm=False)
        ctx.cancel("o1")
        ctx.cancel_all()
        assert ctx.open_orders() == ()
        with pytest.raises(WarmupViolation):
            ctx.modify("o1", price=parse_money("40000"))


# ================== L11: the exact mid and the order-safe mid are different numbers


class TestL11MidVsMidPrice:
    def test_the_exact_mid_is_off_grid_and_mid_price_lands_on_it(self) -> None:
        """bid 60 000.01 / ask 60 000.02 on a 0.01 tick: the true midpoint is
        60 000.015, which no order may carry. `mid_price` rounds to the nearest tick,
        ties away from the bid: (60 000.015 + 0.005) // 0.01 -> 60 000.02."""
        spread = SpreadView(bid=parse_money("60000.01"), ask=parse_money("60000.02"))
        assert spread.mid == parse_money("60000.015")
        assert spread.mid_price(parse_money("0.01")) == parse_money("60000.02")

    def test_a_mid_already_on_the_grid_is_returned_unchanged(self) -> None:
        spread = SpreadView(bid=parse_money("60000.00"), ask=parse_money("60000.02"))
        assert spread.mid_price(parse_money("0.01")) == parse_money("60000.01")

    def test_a_nonpositive_tick_is_refused(self) -> None:
        spread = SpreadView(bid=parse_money("1"), ask=parse_money("2"))
        with pytest.raises(ValueError, match="tick_size"):
            spread.mid_price(parse_money("0"))


# ============ L12: floats past 2^53 carry integer-part residue and are refused


class TestL12MoneyRefusesResidueBeyond2To53:
    def test_the_largest_exact_float_integer_still_converts(self) -> None:
        ctx, _ = TestH12ContextIsSealed.build()
        exact = float(2**53 - 1)  # 9 007 199 254 740 991, exactly representable
        assert ctx.money(exact) == parse_money("9007199254740991")

    def test_a_float_at_2_to_53_is_refused(self) -> None:
        """At 2^53 float spacing reaches 1: `float(2**53 + 1) == float(2**53)`, so the
        eight-decimal formatting would faithfully preserve a number the author never
        wrote. The refusal names the remedy (a decimal string)."""
        ctx, _ = TestH12ContextIsSealed.build()
        with pytest.raises(ValueError, match="2\\^53"):
            ctx.money(float(2**53))
        with pytest.raises(ValueError, match="2\\^53"):
            ctx.money(-float(2**54))

    def test_ints_of_any_size_stay_exact_and_accepted(self) -> None:
        ctx, _ = TestH12ContextIsSealed.build()
        assert ctx.money(2**60) == parse_money(str(2**60))


# ======================== L16: the CSV export neutralises formula injection


class TestL16CsvFormulaInjection:
    @pytest.mark.parametrize(
        "cell",
        [
            "=HYPERLINK(\"http://evil\",\"pnl\")",
            "+2+cmd|' /C calc'!A0",
            "@SUM(1+9)*cmd",
            "-2+3+cmd|' /C calc'!A0",
            "\t=1+2",
            "\r=1+2",
        ],
    )
    def test_a_formula_shaped_cell_is_prefixed_with_an_apostrophe(self, cell: str) -> None:
        assert _csv_safe(cell) == f"'{cell}"

    @pytest.mark.parametrize("cell", ["-30.05000000", "+1.5", "-0.001", "40000"])
    def test_exact_money_strings_are_left_alone(self, cell: str) -> None:
        """The platform's money crosses as signed decimal text; a bare number cannot
        start a formula call, and an apostrophe would break every numeric column."""
        assert _csv_safe(cell) == cell

    def test_non_strings_and_ordinary_text_pass_through(self) -> None:
        assert _csv_safe(12) == 12
        assert _csv_safe(None) is None
        assert _csv_safe("momentum entry") == "momentum entry"


# ============ L15's companion: the downsampler tolerates undefined drawdown anchors


class TestL15UndefinedDrawdownAnchors:
    def test_none_anchors_never_become_the_forced_trough(self) -> None:
        """The equity endpoint now serves `None` where the running peak is non-positive
        (undefined is None, the em-dash rule); the extreme-preserving downsampler must
        rank those above every defined drawdown rather than crash comparing them."""
        times = list(range(6))
        values = [100.0, 90.0, 80.0, 95.0, 60.0, 100.0]
        anchors = [None, -0.1, -0.2, None, -0.4, 0.0]
        keep = _downsample_extremes(times, values, 2, anchors=anchors)
        assert 4 in keep, "the defined trough must survive"

    def test_all_none_anchors_do_not_crash(self) -> None:
        times = list(range(6))
        values = [100.0, 90.0, 80.0, 95.0, 60.0, 100.0]
        keep = _downsample_extremes(times, values, 2, anchors=[None] * 6)
        assert keep[0] == 0 and keep[-1] == 5


# ============== M34: record() refuses the non-finite values money() refuses


class TestM34RecordRefusesNonFinite:
    def test_nan_and_the_infinities_are_refused_with_the_remedy_named(self) -> None:
        """`ctx.money(float("nan"))` has always raised; `ctx.record("edge", float("nan"))`
        eight lines below it did not, and the NaN entered the hashed event log -- where
        `json.dumps` serialises it as `NaN`, a token that is not JSON at all, so the JSONL
        log stops parsing in any strict reader; and where `nan != nan` makes the stored
        event unequal even to itself. All three non-finite floats are refused before
        anything is emitted, in `ctx.money`'s voice, with the remedy in the message."""
        ctx, runtime = TestH12ContextIsSealed.build()
        for bad in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ValueError, match="cannot represent"):
                ctx.record("edge", bad)
        assert runtime.events == []  # the refusals left no trace in the log

    def test_a_finite_value_still_lands_in_the_event_log(self) -> None:
        """`record("edge", 3)` stores `float(3) = 3.0` under the declared name: the
        finiteness guard must not tighten what an ordinary number is."""
        ctx, runtime = TestH12ContextIsSealed.build()
        ctx.record("edge", 3)
        event = runtime.events[-1]
        assert event.kind == "RECORD"
        assert event.payload == {"name": "edge", "value": 3.0}


# ==== M35: emitted payloads are snapshots; the log cannot be edited after the fact


class TestM35EmittedPayloadsAreSnapshots:
    def test_mutating_a_logged_nested_dict_does_not_rewrite_the_stored_event(self) -> None:
        """`fields={"detail": {"depth": [1, 2]}}` is logged; the strategy then appends 3
        and adds a key to its own dict. The stored event must still read exactly
        `{"depth": [1, 2]}` and the log's SHA-256 must be the one computed at emit time.

        Before the fix both stored-event paths (`DryRunRuntime.emit`,
        `EngineRuntime.emit`) copied only the payload's top level, so the nested dict
        stayed live inside the hashed log: this test's two final asserts both failed,
        with the stored event reading `{"depth": [1, 2, 3], "late": "edit"}` -- the run
        manifest's hash then described a run that never happened (spec 12.1)."""
        ctx, runtime = TestH12ContextIsSealed.build()
        detail = {"depth": [1, 2]}
        ctx.log.info("entry", detail=detail)
        frozen = event_hash(runtime.events)

        detail["depth"].append(3)
        detail["late"] = "edit"

        event = runtime.events[-1]
        assert event.payload["fields"]["detail"] == {"depth": [1, 2]}
        assert event_hash(runtime.events) == frozen

    def test_the_snapshot_guards_the_emit_boundary_itself(self) -> None:
        """`record()` builds its payload fresh from immutables, so `ctx.log.*` was the
        only strategy-reachable vector -- but the guarantee belongs to `_emit`, the one
        choke point every context emission passes through. A payload emitted there with
        a nested list stores `[1.0]`, and the later append never reaches the log."""
        ctx, runtime = TestH12ContextIsSealed.build()
        payload = {"name": "edge", "values": [1.0]}
        ctx._emit("RECORD", payload)
        payload["values"].append(2.0)
        assert runtime.events[-1].payload == {"name": "edge", "values": [1.0]}


# ========== M36: crossings between indicators on different feeds are refused


def _m36_bar(index: int, close: float) -> Bar:
    scaled = int(round(close * SCALE))
    open_time = START + index * MS_PER_MINUTE
    return Bar(
        symbol=SYMBOL,
        open_time=open_time,
        close_time=open_time + MS_PER_MINUTE - 1,
        open=scaled,
        high=scaled,
        low=scaled,
        close=scaled,
        volume=10 * SCALE,
        quote_volume=int(round(10 * close)) * SCALE,
        trades=1,
    )


class TestM36CrossFeedCrossingsAreRefused:
    @staticmethod
    def _cvd_vs_sma() -> tuple[CVD, SMA]:
        """CVD (trade feed) at `(prev=1.0, value=3.0)`; SMA(1) (bar feed) at `(2.0, 2.0)`.

        Hand-derived: two buy-aggressor prints (`is_buyer_maker=False`) of qty 1.0 then
        2.0 push CVD running totals `+1.0 = 1.0` and `+2.0 = 3.0`; two bars closing at
        2.0 push SMA(1) values 2.0 and 2.0. The pairs are chosen so the old code's edge
        test `1.0 <= 2.0 and 3.0 > 2.0` reported a cross -- one that happened at no
        single instant, since the CVD pair spans two trade prints and the SMA pair two
        closed 1-minute bars.
        """
        cvd = CVD()
        for agg_id, qty in enumerate((1.0, 2.0), start=1):
            cvd.update(
                TradePrint(
                    symbol=SYMBOL,
                    ts_ms=START + agg_id,
                    price_scaled=60_000 * SCALE,
                    qty_scaled=int(qty * SCALE),
                    is_buyer_maker=False,
                    agg_id=agg_id,
                )
            )
        sma = SMA(1)
        for index in range(2):
            sma.update(_m36_bar(index, 2.0))
        assert (cvd.prev, cvd.value) == (1.0, 3.0)
        assert (sma.prev, sma.value) == (2.0, 2.0)
        return cvd, sma

    def test_a_trade_series_refuses_to_cross_a_bar_series(self) -> None:
        """Exactly the state where the old code answered `True` (see `_cvd_vs_sma`) now
        raises, naming both feeds -- and the mirror direction refuses identically, before
        any value comparison, so the misuse fails loudly even while unready."""
        cvd, sma = self._cvd_vs_sma()
        with pytest.raises(TypeError, match="'trade'.*'bar'"):
            cvd.crossed_above(sma)
        with pytest.raises(TypeError, match="'trade'.*'bar'"):
            cvd.crossed_below(sma)
        with pytest.raises(TypeError, match="'bar'.*'trade'"):
            sma.crossed_above(cvd)

    def test_numeric_levels_are_unaffected(self) -> None:
        """A level has no cadence: `cvd.crossed_above(2.0)` with the pair `(1.0, 3.0)`
        is `1.0 <= 2.0 and 3.0 > 2.0` -> True, exactly as before."""
        cvd, _ = self._cvd_vs_sma()
        assert cvd.crossed_above(2.0)
        assert not cvd.crossed_below(2.0)

    def test_the_macd_signal_idiom_keeps_working(self) -> None:
        """`macd.crossed_above(macd.signal)` is the documented idiom and must survive.

        MACD(fast=1, slow=2, signal=2) on closes 10, 10, 10, 20, hand-derived:
        - fast EMA(1): alpha=1, so it is the close: 10, 10, 10, 20.
        - slow EMA(2): seeded at bar 2 with (10+10)/2 = 10; bar 3: 10 + (2/3)(10-10) = 10;
          bar 4: 10 + (2/3)(20-10) = 50/3.
        - MACD line (fast - slow): 0 (bar 2), 0 (bar 3), 20 - 50/3 = 10/3 (bar 4).
        - signal EMA(2) over the line: seeded at bar 3 with (0+0)/2 = 0;
          bar 4: 0 + (2/3)(10/3 - 0) = 20/9.
        Edge at bar 4: line (0, 10/3) vs signal (0, 20/9): 0 <= 0 and 10/3 > 20/9 -> True.
        """
        macd = MACD(1, 2, 2)
        for index, close in enumerate((10.0, 10.0, 10.0, 20.0)):
            macd.update(_m36_bar(index, close))
        assert macd.value == pytest.approx(10.0 / 3.0)
        assert macd.signal.value == pytest.approx(20.0 / 9.0)
        assert macd.signal.feed == "bar"
        assert macd.crossed_above(macd.signal)

    def test_a_derived_series_carries_its_parents_feed(self) -> None:
        """A derived series advances exactly when its parent does, so its cadence *is*
        the parent's: it must refuse a foreign feed and stay comparable with its own.
        The explicit `feed=` plumb-through is what makes this hold for any future
        non-bar parent rather than by coincidence of the class default `"bar"`."""
        derived = DerivedSeries(1, feed="trade")
        assert derived.feed == "trade"
        cvd, sma = self._cvd_vs_sma()
        with pytest.raises(TypeError, match="'trade'.*'bar'"):
            derived.crossed_above(sma)
        # Same feed: the comparison is defined; an empty series just has no edge yet.
        assert derived.crossed_above(cvd) is False
