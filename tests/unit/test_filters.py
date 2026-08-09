"""Tests for exchangeInfo filter parsing (spec 3.2 / R2).

The fixture below is the real BTCUSDT entry from the 2026-08-01 snapshot, trimmed. Using
real values rather than invented ones is deliberate: two of the spec's stated assumptions
turned out to be wrong against the actual payload, and a hand-written fixture would have
reproduced the assumptions instead of the reality.
"""

from __future__ import annotations

import pytest

from perplab.core.money import scaled_to_str
from perplab.exchange.filters import (
    assert_supported_quote,
    parse_exchange_info,
    parse_symbol,
)
from tests.support import BTCUSDT_PAYLOAD

BTCUSDT = {
    "symbol": "BTCUSDT",
    "status": "TRADING",
    "contractType": "PERPETUAL",
    "pricePrecision": 2,
    "quantityPrecision": 3,
    "onboardDate": 1569398400000,
    "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "556.80", "maxPrice": "4529764"},
        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
        {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "120"},
        {"filterType": "MIN_NOTIONAL", "notional": "50"},
        {
            "filterType": "PERCENT_PRICE",
            "multiplierUp": "1.0500",
            "multiplierDown": "0.9500",
            "multiplierDecimal": "4",
        },
        {"filterType": "MAX_NUM_ORDERS", "limit": 200},
        # No MAX_NUM_ALGO_ORDERS -- Binance does not publish it for this symbol.
    ],
}


class TestParseSymbol:
    def test_core_values(self) -> None:
        f = parse_symbol(BTCUSDT)
        assert f.symbol == "BTCUSDT"
        assert f.is_trading
        assert scaled_to_str(f.tick_size) == "0.1"
        assert scaled_to_str(f.step_size) == "0.001"
        assert (f.price_precision, f.quantity_precision) == (2, 3)

    def test_min_notional_is_not_five(self) -> None:
        """Spec 3.2 calls minNotional "commonly 5 USDT -- read it, don't hardcode".

        BTCUSDT is 50. Hardcoding the common value would reject every order below 50
        USDT notional, or worse, accept ones the exchange rejects.
        """
        assert scaled_to_str(parse_symbol(BTCUSDT).min_notional) == "50"

    def test_market_lot_size_is_stricter_than_lot_size(self) -> None:
        """Market orders cap at 120 BTC where limit orders cap at 1000.

        Collapsing the two filters would let a backtest fill an 800 BTC market order that
        the exchange would have rejected outright.
        """
        f = parse_symbol(BTCUSDT)
        assert f.market_max_qty < f.max_qty
        assert scaled_to_str(f.market_max_qty) == "120"

    def test_absent_limit_is_none_not_zero(self) -> None:
        """The distinction is load-bearing, not cosmetic.

        A validator written as `if open_orders >= limit: reject` treats a missing limit
        of 0 as "no orders permitted" and rejects everything on the symbol.
        """
        f = parse_symbol(BTCUSDT)
        assert f.max_num_orders == 200
        assert f.max_num_algo_orders is None

    def test_funding_interval_absent_from_exchange_info(self) -> None:
        """Spec 3.5 / R17 says to read `fundingIntervalHours` per symbol.

        Verified 2026-08-01: the field is not in the exchangeInfo payload for any of the
        851 symbols. 0 signals "not published"; the interval must be derived from the
        historical settlement record instead, which is the fallback spec 3.5 also names.
        """
        assert parse_symbol(BTCUSDT).funding_interval_hours == 0

    def test_percent_price_parsed(self) -> None:
        f = parse_symbol(BTCUSDT)
        assert scaled_to_str(f.multiplier_up) == "1.05"
        assert scaled_to_str(f.multiplier_down) == "0.95"


class TestMissingFilters:
    def test_missing_required_filter_raises(self) -> None:
        """Spec 1.4: fail loudly. A permissive default is worse than an error.

        Defaulting stepSize to zero would let the backtester accept any quantity, which
        is precisely the fiction R2 was raised about.
        """
        broken = {**BTCUSDT, "filters": [f for f in BTCUSDT["filters"] if f["filterType"] != "LOT_SIZE"]}
        with pytest.raises(ValueError, match="LOT_SIZE.stepSize"):
            parse_symbol(broken)

    def test_market_lot_size_falls_back_to_lot_size(self) -> None:
        """Conservative fallback: it never widens what a market order may do."""
        no_market = {
            **BTCUSDT,
            "filters": [f for f in BTCUSDT["filters"] if f["filterType"] != "MARKET_LOT_SIZE"],
        }
        f = parse_symbol(no_market)
        assert f.market_max_qty == f.max_qty


class TestParseExchangeInfo:
    def test_skips_malformed_entries(self) -> None:
        """One bad symbol out of 851 must not block recording BTCUSDT."""
        payload = {"symbols": [BTCUSDT, {"symbol": "BROKEN", "filters": []}]}
        parsed = parse_exchange_info(payload)
        assert set(parsed) == {"BTCUSDT"}

    def test_empty_payload(self) -> None:
        assert parse_exchange_info({}) == {}


# ------------------------------------------------------- the settlement-currency guard


def test_a_usdt_symbol_passes_the_quote_guard() -> None:
    filters = parse_symbol({**BTCUSDT_PAYLOAD, "quoteAsset": "USDT", "marginAsset": "USDT"})
    assert filters.quote_asset == "USDT"
    assert_supported_quote(filters)  # does not raise


def test_a_usdc_margined_symbol_is_refused() -> None:
    """**The USD-M venue is not a USDT venue.**

    `BTCUSDC` trades on the same `fapi` endpoint, with the same payload shape, and nothing
    in its name marks it out. The ledger is one `wallet: Decimal` with no asset dimension,
    so this symbol's PnL would be added to USDT balances as though the currencies were
    interchangeable -- and no invariant would fire, because there is no field for the
    mismatch to disagree with.
    """
    payload = {**BTCUSDT_PAYLOAD, "symbol": "BTCUSDC", "quoteAsset": "USDC", "marginAsset": "USDC"}
    with pytest.raises(ValueError, match="settles in USDC"):
        assert_supported_quote(parse_symbol(payload))


def test_a_symbol_collateralised_in_another_asset_is_refused() -> None:
    """Quote and margin asset are read separately because they answer different questions,
    and multi-assets mode is the setting that makes them diverge."""
    payload = {**BTCUSDT_PAYLOAD, "quoteAsset": "USDT", "marginAsset": "BNFCR"}
    with pytest.raises(ValueError, match="posts margin in BNFCR"):
        assert_supported_quote(parse_symbol(payload))


def test_a_snapshot_predating_the_field_is_accepted_rather_than_refused() -> None:
    """A deliberate asymmetry: refuse what is provably wrong, do not claim to have checked
    what the snapshot could not say.

    Every exchangeInfo snapshot taken before `quoteAsset` was parsed lacks it, and refusing
    those would make the platform's own history unusable to guard against a risk it does
    not carry -- it has only ever been pointed at USDT pairs.
    """
    payload = {k: v for k, v in BTCUSDT_PAYLOAD.items() if k not in ("quoteAsset", "marginAsset")}
    filters = parse_symbol(payload)
    assert filters.quote_asset == ""
    assert_supported_quote(filters)  # does not raise
