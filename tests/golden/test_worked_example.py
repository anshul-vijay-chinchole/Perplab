"""Spec 3.9's end-to-end worked example, reproduced to the exact Decimal.

This is Phase 2's exit criterion (spec 13): "3.9 worked example reproduces exactly; all
property tests green." The spec hands over every intermediate number, so nothing here is
computed by the implementation and then blessed -- each assertion is transcribed from the
document.

**Setup.** BTCUSDT, isolated, 10x leverage, taker fee 0.05%, `stepSize = 0.001`,
`tickSize = 0.10`, `MMR = 0.004`, `MA = 0`, opening wallet 10 000 USDT.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.core.account import Account, AccountEventKind, FeeSchedule
from perplab.core.margin import bankruptcy_price
from tests.support import btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
TAKER = Decimal("0.0005")

T0, T1, T2, T3, T4, T5 = (1_700_000_000_000 + n * 60_000 for n in range(6))


@pytest.fixture()
def account() -> Account:
    account = Account(
        opening_balance=Decimal("10000.00"),
        fees=FeeSchedule.all_taker(TAKER, source="spec-3.9"),
        brackets={SYMBOL: single_bracket_table()},
        filters={SYMBOL: btcusdt_filters()},
    )
    account.set_leverage(SYMBOL, 10)
    return account


def test_t0_open_long(account: Account) -> None:
    """```
    notional = 0.1 x 50 000   = 5 000.00
    IM       = 5 000 / 10     =   500.00
    fee      = 5 000 x 0.0005 =     2.50
    W        = 10 000 - 2.50  = 9 997.50
    Q = +0.1 , Pe = 50 000.00
    P_liq = (500 - 5 000) / (0.0004 - 0.1) = -4 500 / -0.0996 = 45 180.72
    ```
    """
    result = account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"))

    position = account.position(SYMBOL)
    assert position is not None
    assert position.entry_notional == Decimal("5000.00")
    assert position.isolated_margin == Decimal("500.00")
    assert result.fee == Decimal("2.50")
    assert account.wallet == Decimal("9997.50")
    assert position.qty == Decimal("0.1")
    assert position.entry_price == Decimal("50000.00")

    account.update_mark(T0, SYMBOL, Decimal("50000.00"))
    p_liq = account.liquidation_price(SYMBOL)
    assert p_liq is not None
    assert p_liq.quantize(Decimal("0.01")) == Decimal("45180.72")


def test_t1_mark_to_market(account: Account) -> None:
    """```
    uPnL = 0.1 x (51 000 - 50 000) = +100.00
    E    = 9 997.50 + 100.00       = 10 097.50
    ```
    """
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"))
    account.update_mark(T1, SYMBOL, Decimal("51000.00"))

    assert account.unrealized_pnl == Decimal("100.00")
    assert account.equity == Decimal("10097.50")


def test_full_sequence(account: Account) -> None:
    """Every step in order, with the spec's reconciliation at the end.

    Running the whole sequence in one test rather than six is the point of it: the
    reconciliation at the bottom only means something if the same `Account` carried the
    state the whole way, and a per-step test with a fresh fixture cannot catch an error
    that only shows up as accumulated drift.
    """
    # t0 -- open long 0.1 BTC @ 50 000 (taker)
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"))
    assert account.wallet == Decimal("9997.50")

    # t1 -- mark 51 000
    account.update_mark(T1, SYMBOL, Decimal("51000.00"))
    assert account.equity == Decimal("10097.50")

    # t2 -- funding settlement, F = +0.0001, mark 51 000. The long pays.
    cashflow = account.apply_funding(T2, SYMBOL, Decimal("0.0001"), Decimal("51000.00"))
    assert cashflow == Decimal("-0.51")
    assert account.wallet == Decimal("9996.99")
    assert account.equity == Decimal("10096.99")

    # t3 -- add 0.1 BTC @ 52 000 (taker, Case A)
    account.apply_fill(T3, SYMBOL, Decimal("0.1"), Decimal("52000.00"))
    position = account.position(SYMBOL)
    assert position is not None
    assert position.entry_price == Decimal("51000.00")
    assert position.qty == Decimal("0.2")
    assert account.wallet == Decimal("9994.39")

    # t4 -- close 0.15 BTC @ 53 000 (taker, Case B). Entry price must not move.
    result = account.apply_fill(T4, SYMBOL, Decimal("-0.15"), Decimal("53000.00"))
    assert result.realized == Decimal("300.00")
    assert result.fee == Decimal("3.975")
    assert account.wallet == Decimal("10290.415")
    remaining = account.position(SYMBOL)
    assert remaining is not None
    assert remaining.qty == Decimal("0.05")
    assert remaining.entry_price == Decimal("51000.00")

    # t5 -- mark 53 000, final mark-to-market
    account.update_mark(T5, SYMBOL, Decimal("53000.00"))
    assert account.unrealized_pnl == Decimal("100.00")
    assert account.equity == Decimal("10390.415")

    # Reconciliation (spec 3.10)
    assert account.total_realized == Decimal("300.000")
    assert account.total_fees == Decimal("9.075")  # 2.50 + 2.60 + 3.975
    assert account.total_funding == Decimal("-0.510")
    assert (
        account.opening_balance
        + account.total_realized
        - account.total_fees
        + account.total_funding
    ) == account.wallet
    account.reconcile()


def test_reconciliation_totals_are_exact_not_approximate(account: Account) -> None:
    """The spec's own closing check, asserted as identity rather than equality-to-2dp.

    `10 290.415` has three decimal places because the t4 fee does: `0.15 x 53 000 x 0.0005`
    is 3.975, and the spec carries it rather than rounding to the cent. Quantising fees to
    two places -- an easy and superficially sensible thing to do -- breaks this line, and
    the drift it introduces is exactly what spec 3.10 forbids absorbing with an epsilon.
    """
    for ts, qty, price in (
        (T0, Decimal("0.1"), Decimal("50000.00")),
        (T3, Decimal("0.1"), Decimal("52000.00")),
        (T4, Decimal("-0.15"), Decimal("53000.00")),
    ):
        account.apply_fill(ts, SYMBOL, qty, price)
    account.apply_funding(T4, SYMBOL, Decimal("0.0001"), Decimal("51000.00"))

    assert account.total_fees == Decimal("9.075")
    assert account.total_fees != Decimal("9.08")


def test_attribution_sums_to_net_pnl(account: Account) -> None:
    """Spec 8.4's decomposition must add up exactly (invariant I9)."""
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"))
    account.update_mark(T1, SYMBOL, Decimal("51000.00"))
    account.apply_funding(T2, SYMBOL, Decimal("0.0001"), Decimal("51000.00"))
    account.apply_fill(T3, SYMBOL, Decimal("0.1"), Decimal("52000.00"))
    account.apply_fill(T4, SYMBOL, Decimal("-0.15"), Decimal("53000.00"))
    account.update_mark(T5, SYMBOL, Decimal("53000.00"))

    parts = account.attribution()
    assert parts["price_pnl"] == Decimal("400.00")  # 300 realised + 100 unrealised
    assert parts["funding_pnl"] == Decimal("-0.51")
    assert parts["fees"] == Decimal("9.075")
    assert parts["net_pnl"] == Decimal("390.415")
    assert parts["price_pnl"] + parts["funding_pnl"] - parts["fees"] == parts["net_pnl"]


def test_event_log_records_every_mutation(account: Account) -> None:
    """Three fills and one funding settlement, and nothing else.

    Mark updates are absent because `log_marks` is off by default. The count is asserted
    rather than merely the contents: an event log that quietly gains entries stops being a
    reproducibility hash (spec 12.1) and becomes a source of spurious diffs between runs
    that were in fact identical.
    """
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"))
    account.update_mark(T1, SYMBOL, Decimal("51000.00"))
    account.apply_funding(T2, SYMBOL, Decimal("0.0001"), Decimal("51000.00"))
    account.apply_fill(T3, SYMBOL, Decimal("0.1"), Decimal("52000.00"))
    account.apply_fill(T4, SYMBOL, Decimal("-0.15"), Decimal("53000.00"))

    kinds = [e.kind for e in account.events]
    assert kinds == [
        AccountEventKind.FILL,
        AccountEventKind.FUNDING,
        AccountEventKind.FILL,
        AccountEventKind.FILL,
    ]
    assert [e.wallet for e in account.events] == [
        Decimal("9997.50"),
        Decimal("9996.99"),
        Decimal("9994.39"),
        Decimal("10290.415"),
    ]


def test_bankruptcy_price_brackets_the_liquidation_price(account: Account) -> None:
    """Spec 3.7's sanity invariant (I7): for a long, `P_bank < P_liq < Pe`.

    `P_bank = 50 000 - 500/0.1 = 45 000` and `P_liq = 45 180.72`, so
    `45 000 < 45 180.72 < 50 000`. The spec calls this out as the check that catches a
    wrongly-resolved bracket, which otherwise produces a perfectly plausible number.
    """
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"))
    account.update_mark(T0, SYMBOL, Decimal("50000.00"))

    position = account.position(SYMBOL)
    assert position is not None
    p_bank = bankruptcy_price(position.qty, position.entry_price, position.isolated_margin)
    p_liq = account.liquidation_price(SYMBOL)

    assert p_bank == Decimal("45000")
    assert p_liq is not None
    assert p_bank < p_liq < position.entry_price
