"""The `BAR_CLOSE` market fill model and the latency that decides which print it uses."""

from __future__ import annotations

import math
import random
from decimal import Decimal

import pytest

from perplab.core.money import parse_money, quantize_price
from perplab.core.types import Side
from perplab.engine.fills import (
    DEFAULT_SLIPPAGE_BPS,
    MarketInputs,
    MarketFillModel,
    quantize_fill_price,
)
from perplab.engine.latency import (
    FixedLatency,
    LognormalLatency,
    latency_from_json,
)

TICK = parse_money("0.10")


def bar_close_quote(
    model: MarketFillModel,
    *,
    side: Side,
    print_price: str,
    reference_price: str | None = None,
):
    """Quote a `BAR_CLOSE` fill of one unit.

    Phase 5 gave every tier's model one `quote(MarketInputs)` signature, so the engine does
    not need a four-way branch to know which arguments its own fill model wants. These tests
    predate that and only ever varied the print and the reference, so the bundle is built
    here rather than at nine call sites.
    """
    return model.quote(
        MarketInputs(
            side=side,
            qty=parse_money("1"),
            tick_size=TICK,
            reference_price=parse_money(reference_price or print_price),
            print_price=parse_money(print_price),
        )
    )


def test_a_fill_price_rounds_against_the_trader_not_with_them() -> None:
    """The direction is the opposite of `money.quantize_price`, deliberately.

    `quantize_price` rounds an *order* price so a buy limit sits lower and is less likely to
    fill -- understating execution, the safe error. An *execution* price is the other way
    round: a buy pays the higher tick. Reusing the order-price helper here would hand the
    trader a fraction of a tick on every fill, one-directionally, forever.
    """
    raw = parse_money("100.04")
    assert quantize_fill_price(raw, TICK, Side.BUY) == parse_money("100.10")
    assert quantize_fill_price(raw, TICK, Side.SELL) == parse_money("100.00")
    # And the order-price helper genuinely does the opposite, so the two cannot be swapped
    # without this test noticing.
    assert quantize_price(raw, TICK, Side.BUY) == parse_money("100.00")
    assert quantize_price(raw, TICK, Side.SELL) == parse_money("100.10")


def test_an_exact_multiple_of_the_tick_is_left_alone() -> None:
    for side in (Side.BUY, Side.SELL):
        assert quantize_fill_price(parse_money("60000.10"), TICK, side) == parse_money("60000.10")


def test_a_price_below_one_tick_is_refused_rather_than_rounded_to_zero() -> None:
    with pytest.raises(ValueError, match="cannot have traded"):
        quantize_fill_price(parse_money("0.04"), TICK, Side.SELL)


def test_slippage_is_adverse_on_both_sides() -> None:
    model = MarketFillModel(slippage_bps=parse_money("10"))  # 10 bps = 0.1%
    buy = bar_close_quote(model, side=Side.BUY, print_price="50000")
    sell = bar_close_quote(model, side=Side.SELL, print_price="50000")
    # 50 000 x 1.001 = 50 050 exactly; 50 000 x 0.999 = 49 950 exactly.
    assert buy.price == parse_money("50050.00")
    assert sell.price == parse_money("49950.00")
    assert buy.slippage_per_unit == parse_money("50")
    assert sell.slippage_per_unit == parse_money("50")


def test_slippage_is_measured_against_the_signal_price_not_the_print() -> None:
    """Spec 8.4 measures execution against the price the decision was made at.

    So the reported figure covers the latency drift as well as the modelled offset, which is
    the whole reason a zero-latency backtest looks better than it is.
    """
    model = MarketFillModel(slippage_bps=parse_money("0"))
    quote = bar_close_quote(
        model, side=Side.BUY, print_price="50100", reference_price="50000"
    )
    assert quote.price == parse_money("50100")
    assert quote.slippage_per_unit == parse_money("100")


def test_favourable_drift_produces_a_negative_signed_cost() -> None:
    """Signed, not absolute -- see `analytics.attribution` for why the identity needs it."""
    model = MarketFillModel(slippage_bps=parse_money("0"))
    quote = bar_close_quote(
        model, side=Side.BUY, print_price="49900", reference_price="50000"
    )
    assert quote.slippage_per_unit == parse_money("-100")


def test_zero_slippage_still_quantises_the_print() -> None:
    model = MarketFillModel(slippage_bps=parse_money("0"))
    quote = bar_close_quote(model, side=Side.BUY, print_price="50000.04")
    assert quote.price == parse_money("50000.10")


def test_a_negative_slippage_assumption_is_refused() -> None:
    with pytest.raises(ValueError, match="systematically better"):
        MarketFillModel(slippage_bps=parse_money("-1"))


def test_the_default_slippage_is_pessimistic_relative_to_the_real_spread() -> None:
    """One basis point on BTCUSDT is over a hundred times its half-spread.

    Pinned so that a future "let's make the default realistic" edit has to argue with the
    reasoning in the module docstring: the number prices the *timing* uncertainty this tier
    cannot resolve, not the spread.
    """
    assert DEFAULT_SLIPPAGE_BPS == parse_money("1.0")
    half_spread_bps = (Decimal("0.05") / Decimal("60000")) * Decimal(10_000)
    assert DEFAULT_SLIPPAGE_BPS > half_spread_bps * 100


# ------------------------------------------------------------------------------ latency


def test_fixed_latency_is_constant_and_flags_zero() -> None:
    rng = random.Random(0)
    model = FixedLatency(submit=120, cancel=90)
    assert {model.submit_ms(rng) for _ in range(50)} == {120}
    assert model.cancel_ms(rng) == 90
    assert not model.is_zero
    assert FixedLatency(submit=0).is_zero


def test_negative_latency_is_refused() -> None:
    with pytest.raises(ValueError, match="not a latency model"):
        FixedLatency(submit=-1)


def test_lognormal_hits_its_declared_median_and_p99() -> None:
    """Parameterised by percentiles, so the parameters are checkable against a ping.

    Sampled rather than asserted algebraically because the algebra is what is under test:
    `mu = ln(median)` and `sigma = (ln(p99) - mu)/z99` are only right if the resulting draws
    actually land where they claim.
    """
    model = LognormalLatency(median=120, p99=600)
    rng = random.Random(20260803)
    samples = sorted(model.submit_ms(rng) for _ in range(20000))
    median = samples[len(samples) // 2]
    p99 = samples[int(len(samples) * 0.99)]
    assert 115 <= median <= 125
    assert 550 <= p99 <= 660


def test_lognormal_never_returns_zero() -> None:
    """A zero sample would fill at the price that triggered the order.

    Reachable in any tail however unlikely, which would make it a rare and
    irreproducible-looking anomaly rather than an honest model.
    """
    model = LognormalLatency(median=2, p99=3)
    rng = random.Random(1)
    assert min(model.submit_ms(rng) for _ in range(5000)) >= 1


def test_lognormal_sigma_is_derived_from_the_two_percentiles() -> None:
    model = LognormalLatency(median=120, p99=600)
    expected = (math.log(600) - math.log(120)) / 2.3263478740408408
    assert model.sigma == pytest.approx(expected)


def test_a_tail_below_the_centre_is_refused() -> None:
    with pytest.raises(ValueError, match="not a distribution"):
        LognormalLatency(median=600, p99=120)


def test_latency_round_trips_through_its_manifest_form() -> None:
    for model in (FixedLatency(200, 300), LognormalLatency(90, 400, 150)):
        rebuilt = latency_from_json(model.to_json())
        assert rebuilt.to_json() == model.to_json()


def test_an_unknown_latency_model_is_refused_rather_than_defaulted() -> None:
    """Substituting a default would re-price every fill while keeping the run's identity."""
    with pytest.raises(ValueError, match="re-price every fill"):
        latency_from_json({"model": "empirical"})
