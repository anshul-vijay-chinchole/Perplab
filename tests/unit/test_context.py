"""The Context API and position sizing (spec 5.3).

Exercised against `DryRunRuntime`, the same runtime the validator's smoke run uses. That
is deliberate: the runtime is the one implementation of the `Runtime` protocol that exists
before Phase 4, and testing the context against a bespoke double would leave the real one
untested.
"""

from __future__ import annotations

import random
from decimal import Decimal

import pytest

from perplab.core.money import parse_money
from perplab.core.sizing import max_qty_for_margin, size_by_notional, size_by_stop
from perplab.strategy.context import (
    Context,
    DataUnavailable,
    FillTier,
    OrderType,
    WarmupViolation,
)
from perplab.strategy.dryrun import DryRunRuntime
from perplab.strategy.indicators import IndicatorSet

STEP = Decimal("0.001")


def build(*, warm: bool = True, tier: FillTier = FillTier.BOOK_WALK) -> tuple[Context, DryRunRuntime]:
    runtime = DryRunRuntime(symbols=("BTCUSDT",), fill_tier=tier)
    runtime.advance(1_704_067_200_000)
    runtime.set_mark("BTCUSDT", parse_money("60000"))
    context = Context(
        _runtime=runtime,
        symbols=("BTCUSDT",),
        timeframe="15m",
        indicators=IndicatorSet(primary_symbol="BTCUSDT", bar_ms=900_000),
        rng=random.Random(1),
    )
    context._set_warm(warm)
    return context, runtime


class TestSizing:
    def test_size_by_stop_risks_the_requested_fraction(self) -> None:
        # 10,000 equity, 1% risk, 500 wide stop -> 100/500 = 0.2 BTC
        qty = size_by_stop(
            entry=Decimal("60000"),
            stop=Decimal("59500"),
            risk_fraction=Decimal("0.01"),
            equity=Decimal("10000"),
            step_size=STEP,
        )
        assert qty == Decimal("0.200")

    def test_size_is_floored_to_the_step_never_raised(self) -> None:
        """Rounding a computed size *up* to the step turns a 1% risk into 1.7% on a small
        account, and the smaller the account the worse the overshoot."""
        qty = size_by_stop(
            entry=Decimal("60000"),
            stop=Decimal("59000"),
            risk_fraction=Decimal("0.01"),
            equity=Decimal("170"),
            step_size=STEP,
        )
        assert qty == Decimal("0.001")  # exact would be 0.0017

    def test_a_stop_at_the_entry_is_refused(self) -> None:
        """Risk per unit is zero, so the solution is an infinite position. Returning a huge
        number or zero would both answer a question nobody asked."""
        with pytest.raises(ValueError, match="stop equals entry"):
            size_by_stop(
                entry=Decimal("60000"),
                stop=Decimal("60000"),
                risk_fraction=Decimal("0.01"),
                equity=Decimal("10000"),
                step_size=STEP,
            )

    def test_side_does_not_change_the_size(self) -> None:
        """A stop above the entry is a short. The distance is what sizes the trade."""
        long_qty = size_by_stop(
            entry=Decimal("60000"), stop=Decimal("59500"), risk_fraction=Decimal("0.01"),
            equity=Decimal("10000"), step_size=STEP,
        )
        short_qty = size_by_stop(
            entry=Decimal("60000"), stop=Decimal("60500"), risk_fraction=Decimal("0.01"),
            equity=Decimal("10000"), step_size=STEP,
        )
        assert long_qty == short_qty

    def test_a_wiped_out_account_sizes_to_zero_rather_than_negative(self) -> None:
        qty = size_by_stop(
            entry=Decimal("60000"), stop=Decimal("59500"), risk_fraction=Decimal("0.01"),
            equity=Decimal("-5"), step_size=STEP,
        )
        assert qty == 0

    @pytest.mark.parametrize("fraction", ["0", "-0.01", "1.5"])
    def test_an_impossible_risk_fraction_is_refused(self, fraction: str) -> None:
        with pytest.raises(ValueError, match="risk_fraction"):
            size_by_stop(
                entry=Decimal("60000"), stop=Decimal("59500"),
                risk_fraction=Decimal(fraction), equity=Decimal("10000"), step_size=STEP,
            )

    def test_size_by_notional_divides_by_price(self) -> None:
        qty = size_by_notional(
            notional=Decimal("6000"), price=Decimal("60000"), step_size=STEP
        )
        assert qty == Decimal("0.100")

    def test_margin_bound_scales_with_leverage(self) -> None:
        at_1x = max_qty_for_margin(
            available=Decimal("6000"), price=Decimal("60000"), leverage=1, step_size=STEP
        )
        at_5x = max_qty_for_margin(
            available=Decimal("6000"), price=Decimal("60000"), leverage=5, step_size=STEP
        )
        assert at_1x == Decimal("0.100")
        assert at_5x == Decimal("0.500")


class TestWarmupGate:
    def test_orders_are_blocked_during_warmup(self) -> None:
        """Enforced by the context, not by asking the strategy to check `ctx.warm`. A
        guarantee that depends on the author remembering is not a guarantee (spec 5.2)."""
        context, _ = build(warm=False)
        with pytest.raises(WarmupViolation, match="blocked during warm-up"):
            context.buy(qty=parse_money("0.01"))
        with pytest.raises(WarmupViolation):
            context.sell(qty=parse_money("0.01"))
        with pytest.raises(WarmupViolation):
            context.close()

    def test_reads_are_allowed_during_warmup(self) -> None:
        context, _ = build(warm=False)
        assert context.mark() == parse_money("60000")
        assert context.position().is_flat
        assert context.account.equity == parse_money("10000")


class TestOrders:
    def test_a_market_buy_fills_and_updates_the_position(self) -> None:
        context, runtime = build()
        context.buy(qty=parse_money("0.5"))
        # The clock must move before the fill lands (H13): the smoke runtime now defers
        # market fills exactly as the engine's latency queue does, so a fill visible in
        # the same instant it was submitted would be the validation-only physics the
        # audit caught. Every advance below this line exists for the same reason.
        runtime.advance(runtime.now_ms + 1)
        position = context.position()
        assert position.qty == parse_money("0.5")
        assert position.entry_price == parse_money("60000")
        # `NO_QUOTE_FILL` because this fixture sets no ladder: the smoke runtime prices
        # through the engine's own fill model and falls back to the mark when there is
        # nothing to price against, which it records rather than hides.
        assert [event.kind for event in runtime.events] == [
            "ORDER",
            "NO_QUOTE_FILL",
            "FILL",
        ]

    def test_adding_averages_the_entry_price(self) -> None:
        """Spec 3.3 case A: volume-weighted entry."""
        context, runtime = build()
        context.buy(qty=parse_money("1"))
        # The first fill must land before the mark moves, or the second buy averages
        # against a position that does not exist yet.
        runtime.advance(runtime.now_ms + 1)
        runtime.set_mark("BTCUSDT", parse_money("62000"))
        context.buy(qty=parse_money("1"))
        runtime.advance(runtime.now_ms + 1)
        assert context.position().entry_price == parse_money("61000")

    def test_close_clamps_the_submitted_quantity_not_just_the_fill(self) -> None:
        """The clamp has to be on the *intent*, not only on the fill.

        Asserting the resulting position is not enough: the runtime clamps reduce-only
        fills as well, so a `close()` that submitted `qty=10` against a 0.5 position would
        still leave the position flat and the test would pass. What differs is the ORDER
        record — and the event log is the reproducibility artefact of spec 12.1, so an
        order for twenty times the position is a lie recorded in the one place that is
        supposed to be true.
        """
        context, runtime = build()
        context.buy(qty=parse_money("0.5"))
        runtime.advance(runtime.now_ms + 1)
        context.close(qty=parse_money("10"))
        runtime.advance(runtime.now_ms + 1)
        assert context.position().qty == 0
        closing = [
            event for event in runtime.events
            if event.kind == "ORDER" and event.payload["reduce_only"]
        ]
        assert len(closing) == 1
        assert closing[0].payload["qty"] == "0.5"

    def test_closing_a_flat_position_is_a_no_op_not_an_error(self) -> None:
        context, runtime = build()
        assert context.close() is None
        assert runtime.events == []

    def test_a_zero_quantity_order_is_refused_with_a_useful_reason(self) -> None:
        """A zero size almost always means `ctx.risk` floored to the step size."""
        context, _ = build()
        with pytest.raises(ValueError, match="positive quantity"):
            context.buy(qty=parse_money("0"))

    def test_a_limit_order_needs_a_price_and_a_market_order_refuses_one(self) -> None:
        context, _ = build()
        with pytest.raises(ValueError, match="LIMIT order needs a price"):
            context.buy(qty=parse_money("1"), type="LIMIT")
        with pytest.raises(ValueError, match="does not take a price"):
            context.buy(qty=parse_money("1"), price=parse_money("59000"))

    def test_a_protective_order_needs_a_position_to_infer_its_side(self) -> None:
        """With no position there is nothing to infer the side from, and guessing places a
        reduce-only order the exchange rejects in live while the backtest holds it."""
        context, _ = build()
        with pytest.raises(ValueError, match="is flat"):
            context.stop_loss(stop_price=parse_money("59000"))

    def test_a_stop_on_a_long_sells(self) -> None:
        context, runtime = build()
        context.buy(qty=parse_money("1"))
        runtime.advance(runtime.now_ms + 1)
        order_id = context.stop_loss(stop_price=parse_money("59000"))
        assert order_id in runtime.open_order_ids(None)
        stop = runtime._resting[order_id]
        assert stop.side == "SELL"
        assert stop.reduce_only
        assert stop.type is OrderType.STOP_MARKET

    def test_a_trailing_stop_needs_a_positive_callback(self) -> None:
        context, _ = build()
        context.buy(qty=parse_money("1"))
        with pytest.raises(ValueError, match="callback_rate must be positive"):
            context.trailing_stop(callback_rate=parse_money("0"))

    def test_resting_orders_can_be_cancelled(self) -> None:
        context, runtime = build()
        context.buy(qty=parse_money("1"))
        runtime.advance(runtime.now_ms + 1)
        order_id = context.take_profit(stop_price=parse_money("70000"))
        context.cancel(order_id)
        assert runtime.open_order_ids(None) == ()

    def test_an_unknown_symbol_names_the_declared_ones(self) -> None:
        context, _ = build()
        with pytest.raises(ValueError, match="not in this run"):
            context.mark("ETHUSDT")

    def test_an_unknown_time_in_force_is_refused(self) -> None:
        context, _ = build()
        with pytest.raises(ValueError, match="unknown time in force"):
            context.buy(qty=parse_money("1"), tif="DAY")


class TestReads:
    def test_book_raises_below_the_book_walk_tier_even_when_depth_exists(self) -> None:
        """The *tier* must be what refuses, not the absence of data.

        With no snapshot loaded, `ctx.book()` raises either way and the test cannot tell
        which branch did it — which is how a mutation removing the tier check survived.
        Depth is loaded here, so the only thing left to refuse is the tier.

        Returning `None` instead would send a strategy down a branch it never intended, and
        a run that silently degraded to BOOK_TICKER (spec 4.2 does that automatically when
        the range predates collector coverage) would produce results that look like a
        strategy rather than like a misconfiguration.
        """
        from perplab.strategy.synthetic import synthetic_bars, synthetic_depth

        context, runtime = build(tier=FillTier.BOOK_TICKER)
        runtime.set_depth(synthetic_depth(synthetic_bars(1)[0]))
        assert runtime.depth("BTCUSDT") is not None
        with pytest.raises(DataUnavailable, match="BOOK_WALK"):
            context.book()

        warm, top_runtime = build(tier=FillTier.BOOK_WALK)
        top_runtime.set_depth(synthetic_depth(synthetic_bars(1)[0]))
        assert warm.book() is not None

    def test_money_crosses_the_seam_explicitly(self) -> None:
        context, _ = build()
        assert context.money("0.01") == parse_money("0.01")
        assert context.money(3) == parse_money("3")
        # A float is quantised to the storage seam's eight decimals rather than carrying
        # seventeen digits of binary residue into the ledger.
        assert context.money(1234.56789012345) == parse_money("1234.56789012")

    def test_money_refuses_a_bool_and_a_nan(self) -> None:
        context, _ = build()
        with pytest.raises(TypeError):
            context.money(True)
        with pytest.raises(ValueError):
            context.money(float("nan"))

    def test_record_takes_numbers_only(self) -> None:
        """An arbitrary object would be serialised into the hashed event log, and a `repr`
        carrying a memory address makes two identical runs disagree."""
        context, _ = build()
        context.record("edge", 1.5)
        with pytest.raises(TypeError, match="takes a number"):
            context.record("edge", object())  # type: ignore[arg-type]

    def test_log_fields_reach_the_event_log(self) -> None:
        context, runtime = build()
        context.log.warn("thin book", levels=3)
        event = runtime.events[-1]
        assert event.kind == "LOG"
        assert event.payload["level"] == "WARN"
        assert event.payload["fields"] == {"levels": 3}

    def test_fill_tiers_are_ordered(self) -> None:
        assert FillTier.BAR_CLOSE < FillTier.BOOK_TICKER < FillTier.BOOK_WALK
        assert FillTier.BOOK_WALK >= FillTier.BOOK_WALK


class TestRuntimeProtocolConformance:
    """The one guard that would have caught the `macro` gap before it shipped.

    `Runtime` is a bare `typing.Protocol`: not `@runtime_checkable`, not an ABC, with no
    registry and no type checker configured in `pyproject.toml`. Adding a method to it and
    forgetting an implementation produces no error at import, none at construction, and an
    `AttributeError` on the first call -- which `dryrun` then attributes to the *strategy's*
    line number. `DryRunRuntime` was missing `macro` on exactly those terms, which made
    every macro-declaring strategy fail validation and, since a run cannot start from an
    invalid version, put Phase 11 out of reach through the editor.
    """

    def test_every_runtime_implements_the_whole_protocol(self) -> None:
        from perplab.engine.executor_base import EngineRuntime
        from perplab.strategy.context import Runtime

        required = set(Runtime.__protocol_attrs__)
        assert required, "the protocol introspection itself broke; this test proves nothing"
        for implementation in (DryRunRuntime, EngineRuntime):
            missing = sorted(required - set(dir(implementation)))
            assert not missing, f"{implementation.__name__} is missing {missing}"


class TestLeverage:
    def test_the_run_starts_at_the_leverage_it_was_given(self) -> None:
        context, runtime = build()
        assert context.leverage() == runtime.account.leverage("BTCUSDT")

    def test_a_strategy_can_override_the_configured_leverage(self) -> None:
        """The point of the feature: the form's number is a default, not a ceiling."""
        context, runtime = build()
        context.set_leverage(3)
        assert context.leverage() == 3
        assert runtime.account.leverage("BTCUSDT") == 3

    def test_leverage_is_refused_while_a_position_is_open(self) -> None:
        """`Account.set_leverage` declines to model Binance's mid-position margin
        re-resolution, so an open position keeps the leverage it opened at."""
        context, runtime = build()
        context.set_leverage(5)
        context.buy(qty=parse_money("0.01"))
        # A market order is in flight until the clock moves: submitting is not holding.
        runtime.advance(runtime.now_ms + 60_000)
        assert not context.position().is_flat

        with pytest.raises(ValueError, match="while a position is open"):
            context.set_leverage(10)
        # And the refusal left the number alone rather than half-applying it.
        assert context.leverage() == 5

    def test_a_leverage_above_any_venue_bracket_is_refused_at_validation(self) -> None:
        """The smoke run holds no bracket table, so without this the ledger's own bracket
        check is skipped and 500x validates green then dies when the run loads brackets."""
        context, _ = build()
        with pytest.raises(ValueError, match="125"):
            context.set_leverage(500)

    def test_zero_and_negative_leverage_are_refused(self) -> None:
        context, _ = build()
        for bad in (0, -1):
            with pytest.raises(ValueError):
                context.set_leverage(bad)

    def test_a_bool_is_not_a_leverage(self) -> None:
        """`True` is an `int` in Python and would silently mean 1x."""
        context, _ = build()
        with pytest.raises(TypeError):
            context.set_leverage(True)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            context.set_leverage(2.5)  # type: ignore[arg-type]

    def test_an_undeclared_symbol_is_refused(self) -> None:
        context, _ = build()
        with pytest.raises(ValueError, match="not in this run"):
            context.set_leverage(5, symbol="ETHUSDT")

    def test_setting_leverage_is_allowed_during_warm_up(self) -> None:
        """It takes no position and reads no indicator, and `on_start` -- before any bar --
        is the most useful place to call it."""
        context, _ = build(warm=False)
        context.set_leverage(4)
        assert context.leverage() == 4


class TestLeverageIsRefusedInLiveMode:
    """Leverage is state at the venue, and this platform will not let the two disagree.

    `preflight.configure_account` sets Binance's leverage once, before the first order, and
    `ExchangeTransport.__init__` refuses to construct unless the ledger agrees with the
    preflight it is handed. Nothing re-runs either. Writing the ledger's copy mid-session
    would leave the platform sizing margin and solving liquidation prices against a number
    Binance never heard -- and the first symptom is a real liquidation arriving before the
    displayed one.
    """

    def _engine_runtime(self, *, leverage_hook: object) -> object:
        from perplab.core.account import Account, FeeSchedule
        from perplab.engine.executor_base import EngineRuntime

        account = Account(
            opening_balance=parse_money("10000"),
            fees=FeeSchedule.all_taker(parse_money("0.0005"), "test"),
            require_brackets=False,
        )
        account.set_leverage("BTCUSDT", 5)
        return EngineRuntime(
            account=account,
            symbols=("BTCUSDT",),
            filters={},
            tier=FillTier.BOOK_WALK,
            submit_hook=lambda intent: "1",
            cancel_hook=lambda order_id: None,
            cancel_all_hook=lambda symbol: None,
            open_orders_hook=lambda symbol: (),
            leverage_hook=leverage_hook,  # type: ignore[arg-type]
        )

    def test_a_live_session_refuses_and_names_the_session_form(self) -> None:
        runtime = self._engine_runtime(leverage_hook=None)
        with pytest.raises(NotImplementedError, match="session form"):
            runtime.set_leverage("BTCUSDT", 10)  # type: ignore[attr-defined]
        # Refused means unchanged, not partially applied.
        assert runtime.leverage("BTCUSDT") == 5  # type: ignore[attr-defined]

    def test_paper_and_backtest_apply_it(self) -> None:
        applied: list[tuple[str, int]] = []
        runtime = self._engine_runtime(
            leverage_hook=lambda symbol, lev: applied.append((symbol, lev))
        )
        runtime.set_leverage("BTCUSDT", 10)  # type: ignore[attr-defined]
        assert applied == [("BTCUSDT", 10)]
