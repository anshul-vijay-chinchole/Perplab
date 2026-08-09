"""Liquidation PnL booking, fee quantisation, and margin-solvency refusals.

These pin the accounting-audit corrections to `Account` (spec 3.7, 3.8, 6.6):

- A liquidation books the spec 6.6 model -- close at the liquidating mark, then
  confiscate the margin balance *remaining* after that close -- so the position's price
  PnL is realised rather than evaporated, and the clearance penalty is attributed to its
  own column, never sign-flipped (findings C8 and M1).
- Fees are quantised to the money seam's 8 decimal places at booking, and a live
  execution report's actual commission can be booked verbatim in place of the schedule's
  estimate (finding M2, and the spec 6.7.3 reconciliation feature).
- `remove_margin` refuses a withdrawal that would leave the position liquidatable at the
  recorded mark, mirroring `add_margin`'s refusal of the overdrawn state (finding M3).
- A reduce whose fee and realised loss the wallet cannot fund is refused with
  `InsufficientMargin` instead of aborting the run through I5 (finding L2).

Every golden figure is hand-derived in a comment or docstring, as the golden suites do.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.core.account import (
    Account,
    AccountEventKind,
    FeeSchedule,
    InsufficientMargin,
)
from tests.support import btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
ETH = "ETHUSDT"
T = 1_700_000_000_000


def make_account(
    balance: str = "20000", taker: str = "0", **kwargs: object
) -> Account:
    account = Account(
        opening_balance=Decimal(balance),
        fees=FeeSchedule.all_taker(Decimal(taker), source="audit-fixture"),
        brackets={SYMBOL: single_bracket_table()},
        filters={SYMBOL: btcusdt_filters()},
        **kwargs,  # type: ignore[arg-type]
    )
    account.set_leverage(SYMBOL, 10)
    return account


# ------------------------------------------------------------------- C8: the price leg


class TestLiquidationBooksThePriceLeg:
    """A liquidation is a close plus a confiscation, and the close's PnL is real money.

    The revision under audit booked the whole outcome as `-reserved_margin`. The floor
    in `reserved_margin` is correct for the question it answers (what is carved out of
    the wallet) and wrong for this one: once funding overdraws the signed allocation,
    the floored figure is zero, the booked loss is zero, and the position's unrealised
    PnL leaves the books with no ledger entry -- equity and uPnL drop together, so I1
    and I9 both stay green around a wrong total.
    """

    def test_overdrawn_allocation_in_profit_realises_the_price_leg(self) -> None:
        """The audit's worked case, end to end.

        ```
        open 1 BTC @ 50 000, 10x            -> allocation 5 000
        mark 60 000, funding F = 0.1        -> cashflow -6 000, wallet 14 000,
                                               isolated margin -1 000
        mark 51 000                         -> margin balance -1 000 + 1 000 = 0
                                               < MM 204 -> liquidated
        price leg = 1 x (51 000 - 50 000)   = +1 000
        remaining = -1 000 + 1 000          =      0   -> nothing to confiscate
        realised  = 0 - (-1 000)            = +1 000
        wallet    = 14 000 + 1 000          = 15 000
        ```

        True round trip: -6 000 funding + 1 000 price = -5 000. The audited revision
        reported -6 000 -- the wallet stopped at 14 000 and `total_realized` at zero,
        a scratch on the books for a trip that made 1 000 back.
        """
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("60000.0"))
        account.apply_funding(T + 1, SYMBOL, Decimal("0.1"))
        assert account.wallet == Decimal("14000.00")

        account.update_mark(T + 2, SYMBOL, Decimal("51000.0"))
        (result,) = account.check_liquidations(T + 2)

        assert result.event.realized == Decimal("1000.00")
        assert account.total_realized == Decimal("1000.00")
        assert account.wallet == Decimal("15000.00")
        # Nothing remained to confiscate, so the penalty column carries nothing and the
        # price column carries the whole leg.
        parts = account.attribution()
        assert parts["liquidation_cost"] == Decimal("0")
        assert parts["price_pnl"] == Decimal("1000.00")
        assert parts["net_pnl"] == Decimal("-5000.00")
        account.reconcile()

    def test_remaining_margin_is_confiscated_and_attributed_as_clearance(self) -> None:
        """Overdrawn, in profit, and with a remainder for the clearance to consume.

        ```
        open 1 BTC @ 50 000, 100x            -> allocation 500
        funding F = 0.011 at 50 000          -> -550, isolated margin -50
        mark 50 100                          -> P_liq solves above the mark; trigger
        price leg = 1 x (50 100 - 50 000)    = +100
        remaining = -50 + 100                =  +50   -> confiscated in full
        realised  = 0 - (-50)                =  +50
        ```

        The split matters as much as the total: +100 of price edge and -50 of clearance
        is a different statement about the strategy than +50 of price edge.
        """
        account = Account(
            opening_balance=Decimal("10000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 100)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("50000.0"))
        account.apply_funding(T + 1, SYMBOL, Decimal("0.011"))
        account.update_mark(T + 2, SYMBOL, Decimal("50100.0"))

        (result,) = account.check_liquidations(T + 2)

        assert result.event.realized == Decimal("50.00")
        assert account.wallet == Decimal("9500.00")  # 10 000 - 550 + 50
        parts = account.attribution()
        assert parts["price_pnl"] == Decimal("100.00")
        assert parts["liquidation_cost"] == Decimal("-50.00")
        assert parts["net_pnl"] == Decimal("-500.00")
        account.reconcile()


# --------------------------------------------------------- M1: the attribution split


class TestGapThroughAttribution:
    """`liquidation_cost` is a penalty. A penalty that reports positive is an artifact."""

    def test_a_gap_through_bankruptcy_cannot_flip_the_penalty_positive(self) -> None:
        """The audit's example: 1 BTC @ 50 000 on 10x, probed at a bar low of 40 000.

        ```
        price leg  = 1 x (40 000 - 50 000) = -10 000
        remaining  = 5 000 - 10 000        =  -5 000  -> past bankruptcy; nothing left
        realised   = 0 - 5 000             =  -5 000  (the isolated-margin cap)
        ```

        The audited revision derived the memo as `loss - price_leg` = -5 000 + 10 000 =
        **+5 000** of "liquidation cost" beside a -10 000 price PnL -- on an account
        that lost exactly 5 000. Both columns were wrong in opposite directions and I9
        closed regardless, because the columns are memos over one total. The honest
        split under isolated margin: the price move consumed the whole allocation (the
        excess lands on the exchange's insurance fund, which is not an account
        cashflow), and the clearance found nothing left to consume.
        """
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("40000.0"))

        (result,) = account.check_liquidations(T + 1)

        assert result.event.realized == Decimal("-5000.00")
        assert account.wallet == Decimal("15000.00")
        parts = account.attribution()
        assert parts["liquidation_cost"] == Decimal("0")
        assert parts["price_pnl"] == Decimal("-5000.00")
        assert parts["net_pnl"] == Decimal("-5000.00")
        account.reconcile()

    def test_a_normal_liquidation_charges_the_confiscated_remainder(self) -> None:
        """Caught just inside the trigger, the remainder is real and the penalty shows it.

        ```
        P_liq     = (5 000 - 50 000) / (0.004 - 1) = 45 180.72...
        mark      = 45 100  (inside the trigger, above the 45 000 bankruptcy)
        price leg = 1 x (45 100 - 50 000)  = -4 900
        remaining = 5 000 - 4 900          =    100   -> confiscated
        realised  = -5 000
        ```
        """
        account = make_account(balance="6000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("45100.0"))

        (result,) = account.check_liquidations(T + 1)

        assert result.event.realized == Decimal("-5000.00")
        parts = account.attribution()
        assert parts["liquidation_cost"] == Decimal("-100.00")
        assert parts["price_pnl"] == Decimal("-4900.00")
        account.reconcile()


# ------------------------------------------------------------- M2: fee quantisation


class TestFeeQuantisation:
    def test_the_scheduled_fee_is_booked_at_eight_decimal_places(self) -> None:
        """`|f| * Pf * rate` carries more places than any venue charges.

        A 0.001 BTC fill at 41 234.10 with a 0.00045 commission rate:

        ```
        raw fee = 0.001 x 41 234.1 x 0.00045 = 0.018555345    (9 decimal places)
        booked  =                              0.01855534     (half-even at 8 dp:
                                                               the dropped digit is
                                                               exactly 5 and the kept
                                                               4 is already even)
        ```

        Binance reports `commission` as an 8-decimal string, so the unrounded figure is
        a number the venue never charged; carried into `total_fees` it guarantees the
        wallet can never reconcile to the cent (spec 6.7.3).
        """
        account = make_account(taker="0.00045")
        result = account.apply_fill(T, SYMBOL, Decimal("0.001"), Decimal("41234.1"))

        assert result.fee == Decimal("0.01855534")
        assert result.fee.as_tuple().exponent >= -8
        assert account.total_fees == Decimal("0.01855534")
        assert account.wallet == Decimal("19999.98144466")
        account.reconcile()


class TestVenueCommissionOverride:
    """`apply_fill(fee=...)`: the venue's actual commission replaces the schedule's model."""

    def test_the_exact_commission_is_booked_instead_of_the_schedule(self) -> None:
        account = make_account(taker="0.0005")  # schedule would charge 25.00
        result = account.apply_fill(
            T, SYMBOL, Decimal("1"), Decimal("50000.0"), fee=Decimal("19.99")
        )

        assert result.fee == Decimal("19.99")
        assert account.total_fees == Decimal("19.99")
        assert account.wallet == Decimal("19980.01")  # 20 000 - 19.99
        assert account.events[-1].kind is AccountEventKind.FILL
        assert account.events[-1].fee == Decimal("19.99")
        account.reconcile()

    def test_the_override_is_quantised_like_any_ledger_amount(self) -> None:
        # 0.123456789 carries 9 places. Half-even at 8: the dropped digit is 9 > 5, so
        # the kept 8 rounds up -> 0.12345679.
        account = make_account(taker="0.0005")
        result = account.apply_fill(
            T, SYMBOL, Decimal("1"), Decimal("50000.0"), fee=Decimal("0.123456789")
        )
        assert result.fee == Decimal("0.12345679")

    def test_a_zero_commission_is_a_legal_override(self) -> None:
        """Zero is a value the venue really reports (fee promotions, BNB burns)."""
        account = make_account(taker="0.0005")
        result = account.apply_fill(
            T, SYMBOL, Decimal("1"), Decimal("50000.0"), fee=Decimal(0)
        )
        assert result.fee == Decimal(0)
        assert account.wallet == Decimal("20000")

    def test_a_negative_commission_is_refused(self) -> None:
        """Maker rebates are not modelled, for the same reason `FeeSchedule` refuses a
        negative rate: spec 3.1 books fees positive and subtracted."""
        account = make_account(taker="0.0005")
        with pytest.raises(ValueError, match="negative"):
            account.apply_fill(
                T, SYMBOL, Decimal("1"), Decimal("50000.0"), fee=Decimal("-0.01")
            )

    def test_none_means_the_schedule_exactly_as_before(self) -> None:
        account = make_account(taker="0.0005")
        result = account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        # 1 x 50 000 x 0.0005 = 25, the scheduled taker charge.
        assert result.fee == Decimal("25.00")


# ------------------------------------------------- M3: remove_margin solvency check


class TestRemoveMarginSolvency:
    def _defended_position(self) -> Account:
        """1 BTC @ 50 000 on 10x, defended with 4 000 of added margin, marked at 44 000.

        With 9 000 of isolated margin `P_liq = (9 000 - 50 000) / -0.996 = 41 164.66`,
        so the position survives a 44 000 mark only *because* of the added margin:
        stripped back to 5 000 the trigger sits at 45 180.72, above the mark.
        """
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("50000.0"))
        account.add_margin(T + 1, SYMBOL, Decimal("4000"))
        account.update_mark(T + 2, SYMBOL, Decimal("44000.0"))
        assert account.check_liquidations(T + 2) == []
        return account

    def test_a_removal_the_next_check_would_punish_is_refused(self) -> None:
        """The audit's scenario: withdraw 4 000 into spendable balance one event before
        the allocation it defended is destroyed. Binance rejects the transfer itself --
        withdrawable isolated margin is capped at what keeps the position above
        maintenance -- so permitting it modelled a free defence the exchange does not
        offer."""
        account = self._defended_position()
        available_before = account.available_balance

        with pytest.raises(InsufficientMargin, match="next liquidation check"):
            account.remove_margin(T + 3, SYMBOL, Decimal("4000"))

        # Refused means untouched: the margin is still defending the position and the
        # next check still finds nothing to take.
        position = account.position(SYMBOL)
        assert position is not None
        assert position.extra_margin == Decimal("4000")
        assert account.available_balance == available_before
        assert account.check_liquidations(T + 4) == []

    def test_a_safe_removal_still_succeeds(self) -> None:
        """The mirror must keep working: at a mark the base allocation can defend, the
        added margin is genuinely free to leave."""
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("50000.0"))
        account.add_margin(T + 1, SYMBOL, Decimal("4000"))

        position = account.remove_margin(T + 2, SYMBOL, Decimal("4000"))
        assert position.extra_margin == Decimal(0)
        assert position.isolated_margin == Decimal("5000")

    def test_an_unpriceable_symbol_skips_the_check(self) -> None:
        """No mark recorded means no trigger to protect -- the same state
        `check_liquidations` skips. Refusing on a guessed mark would invent the margin
        model spec 3.4/3.6 forbid."""
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.add_margin(T + 1, SYMBOL, Decimal("4000"))
        # No update_mark has run; removal falls back to the pre-audit behaviour.
        position = account.remove_margin(T + 2, SYMBOL, Decimal("4000"))
        assert position.extra_margin == Decimal(0)


# ------------------------------------------------------- L2: the clean reduce refusal


class TestUnfundableReduceIsRefusedNotAborted:
    def test_insufficient_margin_not_invariant_violation(self) -> None:
        """A reduce whose released margin nets away the fee can still overdraw the wallet.

        Setup drains the wallet below the BTC reservation without any liquidation:

        ```
        open 1 BTC @ 50 000, 10x   -> reserve 5 000, fee 25        wallet 5 975.00
        open 3 ETH @ 3 000, 10x    -> reserve   900, fee  4.50     wallet 5 970.50
        ETH funding F = 0.22       -> -1 980                       wallet 3 990.50
        ```

        The ETH allocation is overdrawn (900 - 1 980 < 0), so its reservation floors at
        zero while the wallet keeps the full charge: available = 3 990.50 - 5 000 < 0.
        Now sell the BTC at 45 500 (a trade print; the mark never followed):

        ```
        realised = 1 x (45 500 - 50 000) = -4 500      fee = 45 500 x 0.0005 = 22.75
        required = -5 000 + 22.75 + 4 500 = -477.25    <= 0 -> the margin question passes
        wallet after would be 3 990.50 - 4 522.75 = -532.25
        ```

        The audited revision returned early on `required <= 0` and booked the fill; the
        negative wallet then surfaced as an I5 `InvariantViolation` -- a run-aborting
        claim that the ledger is broken, about a ledger that is fine and an order that
        was merely unaffordable. The clean refusal is `InsufficientMargin`, same as
        every other unfundable fill.
        """
        account = Account(
            opening_balance=Decimal("6000"),
            fees=FeeSchedule.all_taker(Decimal("0.0005")),
            brackets={
                SYMBOL: single_bracket_table(),
                ETH: single_bracket_table(ETH),
            },
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.set_leverage(ETH, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("50000.0"))
        account.update_mark(T + 1, ETH, Decimal("3000.0"))
        account.apply_fill(T + 1, ETH, Decimal("3"), Decimal("3000.0"))
        account.apply_funding(T + 2, ETH, Decimal("0.22"))
        assert account.wallet == Decimal("3990.50")
        assert account.available_balance < 0

        with pytest.raises(InsufficientMargin, match="cannot fund"):
            account.apply_fill(T + 3, SYMBOL, Decimal("-1"), Decimal("45500.0"))

        # Refused means unchanged: the long is intact, nothing realised, no fee taken,
        # and the run is still alive to decide what to do about it.
        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("1")
        assert account.wallet == Decimal("3990.50")
        account.reconcile()
