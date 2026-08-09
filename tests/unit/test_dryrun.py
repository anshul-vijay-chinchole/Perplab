"""The smoke-run harness and the synthetic feed (spec 5.5 step 5).

Two things are being pinned here. First, that the harness *drives* what it claims to drive
— a hook that is never called cannot catch anything, and a smoke run that silently skips
`on_fill` would pass every strategy whose bug lives there. Second, that the event log is a
function of the strategy's decisions and nothing else, because the determinism probe
compares that log across two interpreters.
"""

from __future__ import annotations

import pytest

from perplab.core.money import SCALE, parse_money
from perplab.strategy.base import Strategy
from perplab.strategy.context import DataUnavailable
from perplab.strategy.dryrun import (
    MAX_SMOKE_BARS,
    SMOKE_BARS,
    SMOKE_TAIL_BARS,
    DryRunRuntime,
    event_hash,
    smoke_run,
)
from perplab.strategy.params import parse_requirements
from perplab.strategy.synthetic import synthetic_bars, synthetic_depth, synthetic_trades

REQUIRES = {"symbols": ["BTCUSDT"], "timeframe": "15m", "history": 5}


def run(cls: type[Strategy], requires: dict | None = None):
    requirements = parse_requirements(requires or getattr(cls, "requires", REQUIRES))
    return smoke_run(cls(), requirements, filename="s.py")


class TestSyntheticData:
    def test_bars_are_well_formed(self) -> None:
        bars = synthetic_bars(200)
        for previous, current in zip(bars, bars[1:]):
            assert current.open_time == previous.open_time + 900_000
            assert current.close_time == current.open_time + 900_000 - 1
            assert current.open == previous.close
        for b in bars:
            assert b.high >= max(b.open, b.close)
            assert b.low <= min(b.open, b.close)
            assert b.low > 0
            assert b.volume > 0

    def test_close_time_is_inside_the_interval(self) -> None:
        """Binance's close time is the last millisecond *inside* the bar. One off and
        every bar overlaps its successor -- the exact boundary the no-look-ahead guarantee
        turns on (spec 3.1)."""
        first, second = synthetic_bars(2)
        assert first.close_time < second.open_time

    def test_the_same_seed_gives_the_same_bars(self) -> None:
        assert synthetic_bars(50, seed=7) == synthetic_bars(50, seed=7)
        assert synthetic_bars(50, seed=7) != synthetic_bars(50, seed=8)

    def test_a_prefix_does_not_depend_on_the_total_count(self) -> None:
        """The look-ahead truncation test compares a 500-bar run against a 400-bar one. A
        generator whose values depended on the total would make that comparison meaningless
        before any indicator was involved."""
        assert synthetic_bars(400)[:100] == synthetic_bars(100)

    def test_the_walk_actually_moves_in_both_directions(self) -> None:
        """A monotone series lets a crossover strategy pass the smoke run without its entry
        path ever executing."""
        closes = [b.close for b in synthetic_bars(300)]
        assert any(b < a for a, b in zip(closes, closes[1:]))
        assert any(b > a for a, b in zip(closes, closes[1:]))

    def test_trades_for_one_bar_do_not_depend_on_earlier_bars(self) -> None:
        bars = synthetic_bars(10)
        assert synthetic_trades(bars[5]) == synthetic_trades(bars[5])
        assert synthetic_trades(bars[5]) != synthetic_trades(bars[6])

    def test_trade_prices_stay_inside_the_bar(self) -> None:
        b = synthetic_bars(1)[0]
        for trade in synthetic_trades(b):
            # `price_scaled`, because a `TradePrint`'s `price` is the float view and the
            # bar's `low`/`high` are the lake's scaled integers.
            assert b.low <= trade.price_scaled <= b.high
            assert b.open_time <= trade.ts_ms <= b.close_time

    def test_depth_is_ordered_best_first_and_does_not_cross(self) -> None:
        b = synthetic_bars(1)[0]
        book = synthetic_depth(b)
        assert book.bid_px[0] < book.ask_px[0]
        assert list(book.bid_px) == sorted(book.bid_px, reverse=True)
        assert list(book.ask_px) == sorted(book.ask_px)


class TestHookDispatch:
    def test_every_hook_a_strategy_implements_is_driven(self) -> None:
        """A hook the harness never calls cannot catch anything, and its absence is
        invisible -- the strategy simply validates clean."""

        class AllHooks(Strategy):
            requires = {
                "symbols": ["BTCUSDT"],
                "timeframe": "15m",
                "history": 2,
                "datasets": ["klines", "aggTrades", "funding"],
            }

            def on_start(self, ctx):
                self.seen = set()
                self.seen.add("on_start")

            def on_bar(self, ctx, bar):
                self.seen.add("on_bar")
                if ctx.warm and ctx.position().is_flat:
                    ctx.buy(qty=ctx.money("0.01"))

            def on_tick(self, ctx, trade):
                self.seen.add("on_tick")

            def on_fill(self, ctx, fill):
                self.seen.add("on_fill")

            def on_funding(self, ctx, event):
                self.seen.add("on_funding")

            def on_stop(self, ctx):
                self.seen.add("on_stop")

        strategy = AllHooks()
        result = smoke_run(strategy, parse_requirements(AllHooks.requires), filename="s.py")
        assert result.ok, result.error
        assert strategy.seen == {
            "on_start",
            "on_bar",
            "on_tick",
            "on_fill",
            "on_funding",
            "on_stop",
        }

    def test_a_fill_arrives_as_a_separate_event_not_inside_the_order_call(self) -> None:
        """Dispatching a fill inside `submit` would make `on_fill` re-entrant with the hook
        that placed the order. A strategy that buys in `on_fill` -- perfectly legal in a
        real engine, where fills are separate events -- would recurse until the stack died,
        in the validator."""

        class BuysOnFill(Strategy):
            requires = REQUIRES

            def on_start(self, ctx):
                self.fills = 0

            def on_bar(self, ctx, bar):
                if ctx.warm and ctx.position().is_flat:
                    ctx.buy(qty=ctx.money("0.001"))

            def on_fill(self, ctx, fill):
                self.fills += 1
                if self.fills < 3:
                    ctx.buy(qty=ctx.money("0.001"))

        strategy = BuysOnFill()
        result = smoke_run(strategy, parse_requirements(REQUIRES), filename="s.py")
        assert result.ok, result.error
        assert strategy.fills >= 3

    def test_a_multi_symbol_strategy_gets_a_bar_per_symbol(self) -> None:
        class Pairs(Strategy):
            requires = {
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "timeframe": "1h",
                "history": 2,
            }

            def on_start(self, ctx):
                self.counts: dict[str, int] = {}
                self.btc = ctx.indicators.sma(3)
                self.eth = ctx.indicators.sma(3, symbol="ETHUSDT")

            def on_bar(self, ctx, bar):
                self.counts[bar.symbol] = self.counts.get(bar.symbol, 0) + 1

        strategy = Pairs()
        result = smoke_run(strategy, parse_requirements(Pairs.requires), filename="s.py")
        assert result.ok, result.error
        assert strategy.counts["BTCUSDT"] == strategy.counts["ETHUSDT"] == result.bars
        # Distinct walks, so a pairs strategy does not see two identical series.
        assert strategy.btc.value != strategy.eth.value

    def test_bars_are_counted_once_per_step_not_once_per_symbol(self) -> None:
        """Counting each symbol would reach the warm-up gate N times too early, and a
        two-symbol strategy would start trading at half its declared history."""

        class Watcher(Strategy):
            requires = {"symbols": ["BTCUSDT", "ETHUSDT"], "timeframe": "1h", "history": 10}

            def on_start(self, ctx):
                self.warm_at: int | None = None

            def on_bar(self, ctx, bar):
                if ctx.warm and self.warm_at is None:
                    self.warm_at = ctx.bars_seen

        strategy = Watcher()
        smoke_run(strategy, parse_requirements(Watcher.requires), filename="s.py")
        assert strategy.warm_at == 10


class TestSubmitLatency:
    """H13: the validator must give same-hook reads the engine's answer.

    The engine queues every submission behind latency (`backtest._submit`), and
    `Context._protective` / `Context.close` read the position synchronously to infer a
    side. When the smoke runtime filled inside `submit`, the canonical entry+stop pattern
    validated green and failed the backtest; these pin the deferred behaviour end to end.
    """

    def test_entry_plus_stop_in_one_hook_fails_validation_with_the_engines_guidance(
        self,
    ) -> None:
        """`ctx.buy(); ctx.stop_loss(...)` in one hook must fail here the way it fails
        the engine, message and all -- a green validation followed by a red first bar is
        the exact failure spec 5.5 exists to close."""

        class EntryAndStop(Strategy):
            requires = REQUIRES

            def on_bar(self, ctx, bar):
                if ctx.warm and ctx.position().is_flat:
                    ctx.buy(qty=ctx.money("0.01"))
                    ctx.stop_loss(stop_price=ctx.money("1000"))

        result = run(EntryAndStop)
        assert not result.ok
        assert result.hook == "on_bar"
        assert "protects an open position" in result.error
        assert "is flat" in result.error

    def test_buy_then_close_in_one_hook_is_a_no_op_that_leaves_the_position_open(
        self,
    ) -> None:
        """The `ctx.close()` variant of the same bug was worse than an error: a full round
        trip in validation, a silent no-op in the backtest. Now both harnesses agree --
        `close()` returns None against the not-yet-filled entry and the position the entry
        opens survives."""

        class BuyThenClose(Strategy):
            requires = REQUIRES

            def on_start(self, ctx):
                self.close_result: object = "unset"
                self.left_open = False

            def on_bar(self, ctx, bar):
                if not ctx.warm:
                    return
                if self.close_result == "unset" and ctx.position().is_flat:
                    ctx.buy(qty=ctx.money("0.01"))
                    self.close_result = ctx.close()
                elif self.close_result is None and not ctx.position().is_flat:
                    self.left_open = True

        strategy = BuyThenClose()
        result = smoke_run(strategy, parse_requirements(REQUIRES), filename="s.py")
        assert result.ok, result.error
        assert strategy.close_result is None
        assert strategy.left_open

    def test_a_market_fill_lands_on_the_next_bar_not_inside_the_hook(self) -> None:
        class OneShot(Strategy):
            requires = REQUIRES

            def on_start(self, ctx):
                self.bought_at: int | None = None
                self.visible_at: int | None = None

            def on_bar(self, ctx, bar):
                if not ctx.warm:
                    return
                if self.bought_at is None:
                    ctx.buy(qty=ctx.money("0.01"))
                    self.bought_at = ctx.bars_seen
                    # Same hook, same instant: the engine would not show a position yet,
                    # so neither may the validator. Raising here fails the smoke run,
                    # which is the assertion.
                    if not ctx.position().is_flat:
                        raise AssertionError("a submission was visible inside its own hook")
                elif self.visible_at is None and not ctx.position().is_flat:
                    self.visible_at = ctx.bars_seen

        strategy = OneShot()
        result = smoke_run(strategy, parse_requirements(REQUIRES), filename="s.py")
        assert result.ok, result.error
        assert strategy.bought_at is not None and strategy.visible_at is not None
        assert strategy.visible_at == strategy.bought_at + 1

    def test_funding_predicted_rate_is_none_exactly_as_the_engine_reports_it(
        self,
    ) -> None:
        """M39: the engine builds every `FundingView` with `predicted_rate=None` --
        historical data holds settlements, not the forecasts that preceded them. The smoke
        run used to fabricate the realised rate there, so
        `ctx.funding().predicted_rate > x` validated green and raised TypeError at the
        first real settlement. Validation must not be more generous than the run."""

        class ReadsFunding(Strategy):
            requires = {
                "symbols": ["BTCUSDT"],
                "timeframe": "15m",
                "history": 2,
                "datasets": ["klines", "funding"],
            }

            def on_start(self, ctx):
                self.samples: list[tuple[object, object]] = []

            def on_funding(self, ctx, event):
                view = ctx.funding()
                self.samples.append((view.last_rate, view.predicted_rate))

        strategy = ReadsFunding()
        result = smoke_run(
            strategy, parse_requirements(ReadsFunding.requires), filename="s.py"
        )
        assert result.ok, result.error
        assert strategy.samples, "the synthetic feed never settled funding"
        for last_rate, predicted_rate in strategy.samples:
            assert last_rate is not None  # the settlement itself is real data
            assert predicted_rate is None  # the forecast is not, in either harness


class TestWarmupAndBars:
    def test_bar_count_covers_warmup_plus_a_tradeable_tail(self) -> None:
        """A strategy declaring `history: 4000` would never leave warm-up inside 500 bars,
        so the run would exercise the plumbing and none of the trading logic -- and pass."""

        class Slow(Strategy):
            requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 900}

            def on_bar(self, ctx, bar):
                pass

        result = run(Slow)
        assert result.bars == 900 + SMOKE_TAIL_BARS

    def test_the_bar_count_is_capped(self) -> None:
        class Absurd(Strategy):
            requires = {"symbols": ["BTCUSDT"], "timeframe": "1h", "history": 10_000_000}

            def on_bar(self, ctx, bar):
                pass

        assert run(Absurd).bars == MAX_SMOKE_BARS

    def test_the_default_is_the_specs_five_hundred(self) -> None:
        class Quick(Strategy):
            requires = REQUIRES

            def on_bar(self, ctx, bar):
                pass

        assert run(Quick).bars == SMOKE_BARS

    def test_a_failure_is_returned_as_data_not_raised(self) -> None:
        """A strategy that crashes is the expected outcome of validation. Letting it
        propagate would put a platform traceback in front of the author instead of a gutter
        marker (spec 5.5)."""

        class Boom(Strategy):
            requires = REQUIRES

            def on_bar(self, ctx, bar):
                raise ZeroDivisionError("nope")

        result = run(Boom)
        assert not result.ok
        assert result.hook == "on_bar"
        assert "ZeroDivisionError" in result.error

    def test_a_failure_in_on_start_is_attributed_to_on_start(self) -> None:
        class BadStart(Strategy):
            requires = REQUIRES

            def on_start(self, ctx):
                raise ValueError("bad setup")

            def on_bar(self, ctx, bar):
                pass

        result = run(BadStart)
        assert result.hook == "on_start"


class TestEventLog:
    def test_the_hash_depends_on_the_strategys_decisions(self) -> None:
        class Buyer(Strategy):
            requires = REQUIRES

            def on_bar(self, ctx, bar):
                if ctx.warm and ctx.position().is_flat:
                    ctx.buy(qty=ctx.money("0.01"))

        class Quiet(Strategy):
            requires = REQUIRES

            def on_bar(self, ctx, bar):
                pass

        assert run(Buyer).hash != run(Quiet).hash
        assert run(Buyer).hash == run(Buyer).hash

    def test_a_set_of_log_fields_is_canonicalised_by_content_not_iteration_order(
        self,
    ) -> None:
        """A `set` inside a *log field* is not a decision, and hashing its iteration order
        would report the strategy nondeterministic for storing one.

        Asserted on `_canonical` directly rather than by running the strategy twice. Set
        iteration order is fixed for the life of an interpreter, so two runs in one process
        agree whether or not the sorting exists — which is precisely how the sorting
        survived a mutation that deleted it. The sorted output is the observable behaviour.
        """
        from perplab.strategy.dryrun import _canonical

        # Eight elements, not four. A four-element set has a one-in-24 chance of iterating
        # in sorted order under whatever hash seed the test process happens to get, and
        # this test failed to kill a mutation exactly that way. At eight it is one in
        # 40,320, and the assertion is about the sorting rather than about the luck.
        tags = ["hotel", "golf", "foxtrot", "echo", "delta", "charlie", "bravo", "alpha"]
        assert _canonical(set(tags)) == sorted(tags)
        assert _canonical(frozenset(tags)) == sorted(tags)

        # A list keeps its order: order *is* content there, and sorting it would erase a
        # decision the strategy made.
        assert _canonical(["delta", "alpha"]) == ["delta", "alpha"]

    def test_a_strategy_logging_a_set_still_hashes_stably(self) -> None:
        class Logger(Strategy):
            requires = REQUIRES

            def on_bar(self, ctx, bar):
                if ctx.warm:
                    ctx.log.info("tags", tags={"b", "a", "c"})

        assert run(Logger).hash == run(Logger).hash

    def test_an_unserialisable_log_field_becomes_its_type_name(self) -> None:
        """`repr()` embeds a memory address, which would make two identical runs disagree.
        Raising would turn a stray log call into a failure about logging."""
        runtime = DryRunRuntime(symbols=("BTCUSDT",))
        runtime.emit("LOG", {"level": "INFO", "message": "x", "fields": {"obj": object()}})
        assert event_hash(runtime.events) == event_hash(runtime.events)

    def test_the_empty_log_still_hashes(self) -> None:
        assert len(event_hash([])) == 64


class TestDryRunRuntime:
    def _runtime(self) -> DryRunRuntime:
        runtime = DryRunRuntime(symbols=("BTCUSDT",))
        runtime.advance(1_000)
        runtime.set_mark("BTCUSDT", parse_money("100"))
        return runtime

    def test_the_clock_cannot_move_backwards(self) -> None:
        runtime = self._runtime()
        with pytest.raises(ValueError, match="cannot move backwards"):
            runtime.advance(500)

    def test_a_reduce_only_order_cannot_flip_the_position(self) -> None:
        """In live the exchange clamps. A smoke run that let it flip would let "close
        twice" quietly open the opposite side."""
        from perplab.strategy.context import OrderIntent

        runtime = self._runtime()
        runtime.submit(OrderIntent(symbol="BTCUSDT", side="BUY", qty=parse_money("1")))
        runtime.advance(1_100)  # the entry lands at the next clock step
        runtime.submit(
            OrderIntent(
                symbol="BTCUSDT", side="SELL", qty=parse_money("5"), reduce_only=True
            )
        )
        runtime.advance(1_200)
        assert runtime.position_view("BTCUSDT").qty == 0

    def test_a_reduce_only_order_on_a_flat_position_does_nothing(self) -> None:
        from perplab.strategy.context import OrderIntent

        runtime = self._runtime()
        runtime.submit(
            OrderIntent(
                symbol="BTCUSDT", side="SELL", qty=parse_money("1"), reduce_only=True
            )
        )
        runtime.advance(1_100)  # arrival: the clamp is applied where the engine applies it
        assert runtime.position_view("BTCUSDT").qty == 0
        # Cancelled and reported, matching `backtest._clamp`. Vanishing silently left a
        # strategy that requotes from `on_cancel` waiting in validation for an event
        # the engine does send.
        assert [event.kind for event in runtime.events] == ["ORDER", "CANCEL"]
        assert runtime.drain_ends()[0].status == "CANCELLED"

    def test_flipping_through_zero_restarts_the_entry_at_the_new_price(self) -> None:
        """Spec 3.3 case C: the residual is a fresh position, not an average across the
        flip."""
        from perplab.strategy.context import OrderIntent

        runtime = self._runtime()
        runtime.submit(OrderIntent(symbol="BTCUSDT", side="BUY", qty=parse_money("1")))
        runtime.advance(1_100)  # entry fills at the 100 mark
        runtime.set_mark("BTCUSDT", parse_money("120"))
        runtime.submit(OrderIntent(symbol="BTCUSDT", side="SELL", qty=parse_money("3")))
        runtime.advance(1_200)  # the flip fills at the 120 mark in force at arrival
        position = runtime.position_view("BTCUSDT")
        assert position.qty == parse_money("-2")
        assert position.entry_price == parse_money("120")

    def test_unrealised_pnl_has_the_right_sign_on_both_sides(self) -> None:
        from perplab.strategy.context import OrderIntent

        runtime = self._runtime()
        runtime.submit(OrderIntent(symbol="BTCUSDT", side="SELL", qty=parse_money("1")))
        runtime.advance(1_100)  # short entry lands at the 100 mark
        runtime.set_mark("BTCUSDT", parse_money("90"))
        assert runtime.position_view("BTCUSDT").unrealized_pnl == parse_money("10")
        runtime.set_mark("BTCUSDT", parse_money("110"))
        assert runtime.position_view("BTCUSDT").unrealized_pnl == parse_money("-10")

    def test_resting_orders_are_recorded_and_never_filled(self) -> None:
        """Triggering a stop here would need the mark path, the queue model and the latency
        model -- all of Phase 5 -- and a half-modelled stop firing at the wrong moment
        teaches the author something false about their strategy."""
        from perplab.strategy.context import OrderIntent, OrderType

        runtime = self._runtime()
        order_id = runtime.submit(
            OrderIntent(
                symbol="BTCUSDT",
                side="SELL",
                qty=parse_money("1"),
                type=OrderType.STOP_MARKET,
                stop_price=parse_money("90"),
            )
        )
        assert order_id in runtime.open_order_ids(None)
        assert runtime.drain_fills() == []
        # Not a queued market order either: the clock advancing must not "trigger" it.
        runtime.advance(2_000)
        assert runtime.drain_fills() == []
        assert order_id in runtime.open_order_ids(None)

    def test_a_market_order_is_not_a_position_within_the_same_instant(self) -> None:
        """H13, at the runtime seam: the engine books fills behind latency, so a hook that
        submits and immediately reads `position_view` sees flat. The old synchronous fill
        made `ctx.buy(); ctx.stop_loss(...)` validate green and die on the first bar of
        the backtest -- validation was more permissive than the run about *when* a fill
        exists."""
        from perplab.strategy.context import OrderIntent

        runtime = self._runtime()
        order_id = runtime.submit(
            OrderIntent(symbol="BTCUSDT", side="BUY", qty=parse_money("1"))
        )
        # Same instant: no position, no fill event, but the order is visibly in flight.
        assert runtime.position_view("BTCUSDT").qty == 0
        assert runtime.drain_fills() == []
        assert order_id in runtime.open_order_ids(None)
        # Advancing to the *same* millisecond is the same instant: still nothing.
        runtime.advance(1_000)
        assert runtime.position_view("BTCUSDT").qty == 0
        # A strict advance is the arrival: the fill lands, stamped with the arrival time.
        runtime.advance(1_001)
        assert runtime.position_view("BTCUSDT").qty == parse_money("1")
        fills = runtime.drain_fills()
        assert len(fills) == 1
        assert fills[0].ts_ms == 1_001
        assert order_id not in runtime.open_order_ids(None)

    def test_the_mark_is_required_before_a_fill(self) -> None:
        from perplab.strategy.context import OrderIntent

        runtime = DryRunRuntime(symbols=("BTCUSDT",))
        with pytest.raises(RuntimeError, match="first mark price"):
            runtime.submit(OrderIntent(symbol="BTCUSDT", side="BUY", qty=parse_money("1")))

    def test_the_spread_comes_from_the_book(self) -> None:
        runtime = self._runtime()
        book = synthetic_depth(synthetic_bars(1)[0])
        runtime.set_depth(book)
        spread = runtime.spread("BTCUSDT")
        assert spread is not None
        assert spread.ask > spread.bid
        assert spread.bid == parse_money(str(book.bid_px[0] / SCALE))


class TestMacroInTheSmokeRun:
    """`DryRunRuntime.macro` did not exist, and nothing caught it.

    `Runtime` is a structural Protocol with no enforcement, so the omission raised nothing
    until a strategy called it -- at which point `sandbox` reported
    `AttributeError: 'DryRunRuntime' object has no attribute 'macro'` against the strategy's
    own line number. Since `api/routers/runs.py` refuses to start a run from an invalid
    version, that made every macro strategy unrunnable through the editor.
    """

    def test_an_undeclared_macro_read_raises_as_it_does_in_a_run(self) -> None:
        runtime = DryRunRuntime(symbols=("BTCUSDT",))
        with pytest.raises(DataUnavailable, match="macroGlobal"):
            runtime.macro("btc_dominance")

    def test_a_declared_macro_reads_none_rather_than_a_fabricated_value(self) -> None:
        """The harness has no macro store. Answering with a plausible number is how a
        strategy validates green and behaves differently on its first real bar -- the same
        mistake the fabricated `predicted_rate` made."""
        runtime = DryRunRuntime(symbols=("BTCUSDT",), macro_declared=True)
        assert runtime.macro("btc_dominance") is None

    def test_a_macro_strategy_validates(self) -> None:
        """End to end through `smoke_run`, which is what actually regressed."""
        from perplab.strategy.params import parse_requirements

        class MacroStrategy(Strategy):
            requires = {
                "symbols": ["BTCUSDT"],
                "timeframe": "1h",
                "history": 0,
                "datasets": ["klines", "macroGlobal"],
            }

            def on_bar(self, ctx, bar) -> None:  # type: ignore[no-untyped-def]
                reading = ctx.macro("btc_dominance")
                if reading is not None and reading.value > 60:
                    ctx.log.info("dominant")

        result = smoke_run(MacroStrategy(), parse_requirements(MacroStrategy.requires))
        assert result.ok, result.error
