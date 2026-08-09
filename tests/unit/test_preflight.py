"""The exchange preflight: make Binance agree with the ledger, or refuse to trade.

The defect these tests exist for was an *absence*, not a wrong answer:
`SignedRestClient.set_leverage` was implemented, documented and tested, and had no callers
anywhere in the platform. A run configured at 5x would have traded an account still set to
Binance's default 20x, and every margin and liquidation figure the platform displayed would
have been computed from the 5x it believed in. Nothing would have raised.

So the emphasis here is on the refusals. A preflight that only proves the happy path is the
same shape of guarantee as the one that was missing.
"""

from __future__ import annotations

from typing import Any

import pytest

from perplab.core.types import MarginMode
from perplab.exchange.rest import BinanceRestError
from perplab.live.preflight import (
    MARGIN_TYPE_POSITION_OPEN,
    MARGIN_TYPE_UNCHANGED,
    PreflightError,
    configure_account,
)

SYMBOL = "BTCUSDT"


class FakeClient:
    """Records what was sent and answers with whatever the test wants.

    Deliberately not a mock of `SignedRestClient`: the three methods used here are the
    whole of the preflight's contact with the exchange, and a fake that implements exactly
    those three cannot drift into testing something the preflight does not do.
    """

    def __init__(
        self,
        *,
        hedge: Any = False,
        margin_error: int | None = None,
        leverage_echo: Any = "same",
        max_notional: str | None = "1000000",
    ) -> None:
        self.hedge = hedge
        self.margin_error = margin_error
        self.leverage_echo = leverage_echo
        self.max_notional = max_notional
        self.calls: list[tuple[str, Any]] = []

    async def position_mode(self) -> dict[str, Any]:
        self.calls.append(("position_mode", None))
        return {"dualSidePosition": self.hedge}

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        self.calls.append(("margin", (symbol, margin_type)))
        if self.margin_error is not None:
            raise BinanceRestError(400, self.margin_error, "from the test")
        return {"code": 200, "msg": "success"}

    async def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        self.calls.append(("leverage", (symbol, leverage)))
        echoed = leverage if self.leverage_echo == "same" else self.leverage_echo
        return {
            "symbol": symbol,
            "leverage": echoed,
            "maxNotionalValue": self.max_notional,
        }


# --------------------------------------------------------------- the configuration path


@pytest.mark.asyncio
async def test_it_sets_margin_type_and_leverage_on_every_symbol() -> None:
    client = FakeClient()
    report = await configure_account(
        client, ["BTCUSDT", "ETHUSDT"], leverage=5, margin_mode=MarginMode.ISOLATED
    )

    assert ("margin", ("BTCUSDT", "ISOLATED")) in client.calls
    assert ("leverage", ("ETHUSDT", 5)) in client.calls
    assert [c.symbol for c in report.symbols] == ["BTCUSDT", "ETHUSDT"]
    assert all(c.leverage == 5 for c in report.symbols)
    assert all(c.margin_mode is MarginMode.ISOLATED for c in report.symbols)


@pytest.mark.asyncio
async def test_margin_type_is_set_before_leverage() -> None:
    """The call that fails on an open position goes first.

    Ordering is the only thing that decides how much of the account is left modified when
    a symbol is refused. Leverage-then-margin would change the leverage of a symbol the
    very next call is about to reject.
    """
    client = FakeClient()
    await configure_account(client, [SYMBOL], leverage=5)

    kinds = [kind for kind, _ in client.calls]
    assert kinds == ["position_mode", "margin", "leverage"]


@pytest.mark.asyncio
async def test_an_already_isolated_symbol_is_success_not_failure() -> None:
    """`-4046 No need to change margin type` is the idempotent success case.

    Binance reports "it already is what you asked for" as an *error*. Treating it as one
    would make a correctly configured account the only kind that cannot trade.
    """
    client = FakeClient(margin_error=MARGIN_TYPE_UNCHANGED)
    report = await configure_account(client, [SYMBOL], leverage=5)

    assert report.symbols[0].margin_type_changed is False
    assert ("leverage", (SYMBOL, 5)) in client.calls, (
        "a -4046 must not stop the leverage call that follows it"
    )


# ------------------------------------------------------------------------ the refusals


@pytest.mark.asyncio
async def test_cross_margin_is_refused_before_anything_is_sent() -> None:
    """The ledger solves the isolated closed form; under cross there is not one.

    Refused *before* the first request, so a rejected configuration leaves the account
    exactly as it was found.
    """
    client = FakeClient()
    with pytest.raises(PreflightError, match="not implemented"):
        await configure_account(client, [SYMBOL], leverage=5, margin_mode=MarginMode.CROSSED)

    assert client.calls == [], "a refused margin mode must not touch the account"


@pytest.mark.asyncio
async def test_a_hedge_mode_account_is_refused() -> None:
    """One-way ledger, two-sided account: there is no single position to reconcile.

    `reconcile._one_position` already catches this, but only on the first reconciliation
    pass -- up to a minute after the first order. Here it costs nothing and happens before
    any exposure exists.
    """
    client = FakeClient(hedge=True)
    with pytest.raises(PreflightError, match="hedge mode"):
        await configure_account(client, [SYMBOL], leverage=5)

    assert [kind for kind, _ in client.calls] == ["position_mode"], (
        "the hedge check must run before any symbol is configured"
    )


@pytest.mark.asyncio
async def test_the_string_form_of_dual_side_position_is_understood() -> None:
    """Binance has served this field as a JSON boolean and as the string "true"."""
    client = FakeClient(hedge="true")
    with pytest.raises(PreflightError, match="hedge mode"):
        await configure_account(client, [SYMBOL], leverage=5)


@pytest.mark.asyncio
async def test_an_open_position_blocks_the_margin_type_change() -> None:
    """`-4048`. The position was opened under the account's previous configuration."""
    client = FakeClient(margin_error=MARGIN_TYPE_POSITION_OPEN)
    with pytest.raises(PreflightError, match="already has an open position"):
        await configure_account(client, [SYMBOL], leverage=5)


@pytest.mark.asyncio
async def test_an_unrecognised_binance_error_propagates_untouched() -> None:
    """Only -4046 and -4048 are interpreted; guessing at the rest absorbs real failures."""
    client = FakeClient(margin_error=-1021)
    with pytest.raises(BinanceRestError):
        await configure_account(client, [SYMBOL], leverage=5)


# --------------------------------------------------- the leverage echo is what is checked


@pytest.mark.asyncio
async def test_a_leverage_the_exchange_did_not_apply_is_refused() -> None:
    """**A 200 is not confirmation.**

    Binance bounds leverage by the symbol's bracket table. A request for 20x that comes
    back as 10x is a successful HTTP call and a silently wrong account: the ledger would
    size and liquidation-price every position at 20x against a venue using 10x. The real
    liquidation then arrives before the displayed one, which is the direction that costs
    money.
    """
    client = FakeClient(leverage_echo=10)
    with pytest.raises(PreflightError, match="applied 10x"):
        await configure_account(client, [SYMBOL], leverage=20)


@pytest.mark.asyncio
async def test_a_missing_leverage_echo_is_refused_rather_than_assumed() -> None:
    client = FakeClient(leverage_echo=None)
    with pytest.raises(PreflightError, match="no usable leverage field"):
        await configure_account(client, [SYMBOL], leverage=5)


@pytest.mark.asyncio
async def test_the_bracket_ceiling_is_carried_into_the_report() -> None:
    """`maxNotionalValue` is the venue's own bound, worth being able to read afterwards."""
    client = FakeClient(max_notional="250000")
    report = await configure_account(client, [SYMBOL], leverage=5)
    assert report.symbols[0].max_notional == "250000"
    assert report.to_json()["symbols"][0]["max_notional"] == "250000"


@pytest.mark.asyncio
async def test_an_empty_symbol_list_is_refused() -> None:
    """Nothing was verified, so nothing may be traded."""
    client = FakeClient()
    with pytest.raises(PreflightError, match="no symbols"):
        await configure_account(client, [], leverage=5)
