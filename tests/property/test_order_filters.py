"""Random order sizes must respect every spec 3.2 filter (spec 12.2).

This is the second property test spec 12.2 names, and it was missing: the filter set was
parsed out of `exchangeInfo` and then consulted by nothing outside the parser. Spec 3.2 is
categorical about why that matters -- "orders that violate these are rejected by Binance in
live and must be rejected identically in backtest" -- so a filter nobody enforces is a
backtest that fills orders the exchange would have refused.

The fixture is the real 2026-08-01 BTCUSDT snapshot: `tickSize` 0.10, `stepSize` 0.001,
`minQty` 0.001, `maxQty` 1000, market `maxQty` 120, `minNotional` 50. Two of those are
values the spec guessed wrong about (§3.2 calls minNotional "commonly 5"), which is exactly
why the test uses the snapshot rather than invented numbers.
"""

from __future__ import annotations

from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from perplab.core.money import SCALE, to_scaled
from perplab.exchange.filters import validate_order
from tests.support import btcusdt_filters

FILTERS = btcusdt_filters()

# Deliberately wider than anything valid, so the generator produces rejects as well as
# accepts. A strategy that only ever generated valid orders would prove nothing: the
# property under test is that acceptance implies compliance, and it is vacuous without
# non-compliant candidates.
QTYS = st.integers(min_value=0, max_value=2000 * SCALE)
PRICES = st.integers(min_value=0, max_value=200_000 * SCALE)


@given(qty=QTYS, price=PRICES, is_market=st.booleans())
@settings(max_examples=400, deadline=None)
def test_every_accepted_order_satisfies_every_filter(
    qty: int, price: int, is_market: bool
) -> None:
    """Acceptance implies compliance -- checked against the filter values directly.

    Re-deriving each bound here rather than calling the same helper is the point. If
    `validate_order` and this test shared a code path, the test would only prove the
    function agrees with itself.
    """
    result = validate_order(FILTERS, qty=qty, price=price, is_market=is_market)
    if not result.ok:
        return

    step = FILTERS.market_step_size if is_market else FILTERS.step_size
    min_qty = FILTERS.market_min_qty if is_market else FILTERS.min_qty
    max_qty = FILTERS.market_max_qty if is_market else FILTERS.max_qty

    assert qty > 0 and price > 0
    assert qty % step == 0
    assert min_qty <= qty <= max_qty
    assert price % FILTERS.tick_size == 0
    assert FILTERS.min_price <= price <= FILTERS.max_price
    assert (qty * price) // SCALE >= FILTERS.min_notional


@given(qty=QTYS, price=PRICES)
@settings(max_examples=300, deadline=None)
def test_market_orders_are_never_more_permissive_than_limit_orders(
    qty: int, price: int
) -> None:
    """BTCUSDT caps market orders at 120 BTC and limit orders at 1000.

    Collapsing the two filters -- the obvious simplification, since they carry the same
    field names -- would let a backtest fill an 800 BTC market order the exchange rejects
    outright. Stated as a property because the asymmetry is the whole reason
    `MARKET_LOT_SIZE` exists as a separate filter.
    """
    assert FILTERS.market_max_qty < FILTERS.max_qty
    if validate_order(FILTERS, qty=qty, price=price, is_market=True).ok:
        assert validate_order(FILTERS, qty=qty, price=price, is_market=False).ok


@given(
    mark=st.integers(min_value=20_000 * SCALE, max_value=100_000 * SCALE),
    offset_bps=st.integers(min_value=-1500, max_value=1500),
)
@settings(max_examples=300, deadline=None)
def test_percent_price_band_is_enforced_when_a_mark_is_supplied(
    mark: int, offset_bps: int
) -> None:
    """PERCENT_PRICE: BTCUSDT rejects limit orders outside [0.95, 1.05] x mark.

    The band is evaluated only when a mark price is passed. Without one the filter is
    skipped rather than guessed at -- inventing a reference price to validate against is
    the same class of fiction as inventing a tick size.
    """
    price = (mark * (10_000 + offset_bps)) // 10_000
    price -= price % FILTERS.tick_size  # land it on a tick so PRICE_FILTER is not the cause
    if price <= 0:
        return

    result = validate_order(FILTERS, qty=to_scaled("1"), price=price, mark_price=mark)
    assert FILTERS.multiplier_up is not None and FILTERS.multiplier_down is not None
    inside = (
        (mark * FILTERS.multiplier_down) // SCALE
        <= price
        <= (mark * FILTERS.multiplier_up) // SCALE
    )
    if not inside:
        assert not result.ok
        assert "PERCENT_PRICE" in result.reason


class TestKnownRejections:
    """The specific orders spec 3.2 names, as fixed cases rather than generated ones."""

    def test_the_spec_s_own_example_of_fiction(self) -> None:
        """"A backtest that lets you buy 0.13847362 BTC when stepSize = 0.001 is fiction.\""""
        result = validate_order(
            FILTERS, qty=to_scaled("0.13847362"), price=to_scaled("50000")
        )
        assert not result.ok
        assert "LOT_SIZE" in result.reason

    def test_below_min_notional(self) -> None:
        """0.001 BTC at 20 000 is a notional of 20, against a published minimum of 50.

        This one was accepted before `validate_order` existed -- the filter was parsed and
        then read by nothing.
        """
        result = validate_order(FILTERS, qty=to_scaled("0.001"), price=to_scaled("20000"))
        assert not result.ok
        assert "MIN_NOTIONAL" in result.reason

    def test_above_max_qty(self) -> None:
        result = validate_order(FILTERS, qty=to_scaled("5000"), price=to_scaled("50000"))
        assert not result.ok
        assert "maxQty" in result.reason

    def test_market_order_above_its_own_lower_cap(self) -> None:
        """500 BTC is a valid limit order and an invalid market order."""
        qty, price = to_scaled("500"), to_scaled("50000")
        assert validate_order(FILTERS, qty=qty, price=price, is_market=False).ok
        rejected = validate_order(FILTERS, qty=qty, price=price, is_market=True)
        assert not rejected.ok
        assert "MARKET_LOT_SIZE" in rejected.reason

    def test_off_tick_price(self) -> None:
        result = validate_order(FILTERS, qty=to_scaled("1"), price=to_scaled("50000.05"))
        assert not result.ok
        assert "PRICE_FILTER" in result.reason

    def test_a_well_formed_order_is_accepted(self) -> None:
        assert validate_order(FILTERS, qty=to_scaled("0.01"), price=to_scaled("50000.10"))

    def test_the_verdict_is_falsy_when_rejected(self) -> None:
        """`if not validate_order(...)` has to work, or callers will forget `.ok`."""
        assert not validate_order(FILTERS, qty=0, price=to_scaled("50000"))


def test_minimum_notional_boundary_is_inclusive() -> None:
    """Exactly `minNotional` is accepted; one satoshi of quote below it is not.

    Boundary asserted explicitly because `<` and `<=` are indistinguishable to a
    generator that never lands on the boundary, and this one is reachable: 0.001 BTC at
    50 000 is exactly 50.
    """
    assert validate_order(FILTERS, qty=to_scaled("0.001"), price=to_scaled("50000")).ok
    assert not validate_order(
        FILTERS, qty=to_scaled("0.001"), price=to_scaled("49999.90")
    ).ok
    assert FILTERS.min_notional == to_scaled("50")
    assert Decimal(FILTERS.min_notional) / SCALE == Decimal(50)
