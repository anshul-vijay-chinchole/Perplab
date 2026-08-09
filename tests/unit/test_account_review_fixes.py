"""Regressions from the Phase 2 adversarial review.

Every test here pins a defect that was **live** in the first working version of the
accounting core — the suite was green, the §3.9 worked example reproduced exactly, and all
of these were wrong anyway. That is the useful thing about them: each one is a case the
golden tests could not have caught, because the golden tests assert the numbers the spec
supplies and none of these appear in it.

The precision one (`TestInvariantPrecision`) was found by the property suite rather than by
a reviewer, which is the division of labour spec 12.2 intends.
"""

from __future__ import annotations

from decimal import Decimal, localcontext

import pytest

from perplab.core.account import Account, FeeSchedule, InsufficientMargin
from perplab.core.invariants import InvariantViolation, check_wallet_conservation
from perplab.core.money import ACCOUNTING_CONTEXT
from perplab.core.types import PositionSide
from tests.support import btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
OTHER = "ETHUSDT"
T = 1_700_000_000_000


def make_account(balance: str = "100000", leverage: int = 10, **kwargs: object) -> Account:
    account = Account(
        opening_balance=Decimal(balance),
        fees=FeeSchedule.all_taker(Decimal(0), source="review-fixture"),
        brackets={SYMBOL: single_bracket_table(), OTHER: single_bracket_table(OTHER)},
        filters={SYMBOL: btcusdt_filters()},
        **kwargs,  # type: ignore[arg-type]
    )
    account.set_leverage(SYMBOL, leverage)
    account.set_leverage(OTHER, leverage)
    return account


class TestInvariantPrecision:
    """The invariant checks must evaluate at the ledger's precision, not the ambient one."""

    def test_a_fifty_digit_ledger_satisfies_i1(self) -> None:
        """The bug: `invariants.py` did its arithmetic in the default 28-digit context.

        The ledger runs at 50 significant digits (`money.ACCOUNTING_CONTEXT`), so re-adding
        the same four terms outside that context rounded the *expected* value and produced
        a mismatch around 1e-23 — on a wallet that was entirely correct. At the point of
        failure that is indistinguishable from the float leak I1 exists to catch, and spec
        3.10 explicitly forbids adding the epsilon that would have "fixed" it.

        The operand below is a non-terminating division, which is what a liquidation loss
        on a position at 3x or 7x leverage actually looks like.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            realized = Decimal(-2119) / Decimal(107)
            wallet = Decimal(1000000) + realized

        assert len(realized.as_tuple().digits) > 28  # would round in the default context
        check_wallet_conservation(
            wallet, Decimal(1000000), realized, Decimal(0), Decimal(0)
        )

    def test_margin_allocations_are_quantised_to_the_money_seam(self) -> None:
        """`initial_margin` is a division; unquantised it carries 50 digits into the wallet.

        7x on a 50 000 notional does not divide evenly. The allocation is money, and money
        in this system has a quantum of 1e-8.
        """
        account = make_account(leverage=7)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        position = account.position(SYMBOL)

        assert position is not None
        assert position.base_margin == Decimal("7142.85714286")
        assert -position.base_margin.as_tuple().exponent <= 8


class TestAllocationScalesWithAPartialClose:
    """A reduce releases a proportional share of the allocation — all of it, not part."""

    def test_funding_and_added_margin_both_scale(self) -> None:
        """The bug: `base_margin` shrank with the position and the stored parts did not.

        Closing 90% of a long left the remaining 10% carrying 100% of the funding the full
        position had paid and 100% of its added margin. On a long-held, repeatedly-trimmed
        position that drags the liquidation price toward the mark by an order of magnitude
        more than it should — de-risking would trigger the liquidation it was meant to
        avoid.

        ```
        1 BTC @ 50 000, 10x  -> base 5 000
        add 1 000 margin      -> extra 1 000
        funding -500          -> allocation 5 500
        close 0.9            -> every component x 0.1: base 500, extra 100, funding -50
        ```
        """
        account = make_account("100000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        account.add_margin(T + 1, SYMBOL, Decimal("1000"))
        account.apply_funding(T + 2, SYMBOL, Decimal("0.0002"))  # -1 x 50 000 x 0.0002

        before = account.position(SYMBOL)
        assert before is not None
        assert before.funding_paid == Decimal("-10.00")
        assert before.isolated_margin == Decimal("5990.00")

        account.apply_fill(T + 3, SYMBOL, Decimal("-0.9"), Decimal("50000.0"))

        after = account.position(SYMBOL)
        assert after is not None
        assert after.qty == Decimal("0.1")
        assert after.base_margin == Decimal("500.00")
        assert after.extra_margin == Decimal("100.00")
        assert after.funding_paid == Decimal("-1.00")
        assert after.isolated_margin == Decimal("599.00")

    def test_the_liquidation_price_moves_proportionally_not_disproportionately(self) -> None:
        """The consequence, stated as the thing a trader would actually notice.

        Trimming a position must not move its liquidation price *relative to the entry*.
        The allocation and the size shrink together, so the ratio — and therefore the
        distance to liquidation — is unchanged.
        """
        account = make_account("100000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        account.apply_funding(T + 1, SYMBOL, Decimal("0.0002"))
        before = account.liquidation_price(SYMBOL)

        account.apply_fill(T + 2, SYMBOL, Decimal("-0.9"), Decimal("50000.0"))
        after = account.liquidation_price(SYMBOL)

        assert before is not None and after is not None
        assert after == before

    def test_a_flip_releases_added_margin_too(self) -> None:
        """The residual is a new position; the old one's whole allocation is released.

        Funding was already reset on a flip; added margin was not, which left the new
        short defended by margin the closed long had posted.
        """
        account = make_account("100000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        account.add_margin(T + 1, SYMBOL, Decimal("1000"))
        account.apply_fill(T + 2, SYMBOL, Decimal("-1.5"), Decimal("50000.0"))

        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("-0.5")
        assert position.extra_margin == Decimal(0)
        assert position.funding_paid == Decimal(0)
        assert position.isolated_margin == position.base_margin


class TestI5IsNotLatchedOff:
    """I5's liquidation exemption is scoped to the mutation, not to the run."""

    def test_an_overdraw_after_an_unrelated_liquidation_still_raises(self) -> None:
        """The bug: the exemption was `self.liquidations > 0`.

        Once any position had ever been liquidated, a negative wallet was accepted for the
        rest of the run — disabling the check permanently, at exactly the point in a run
        where the account is least able to absorb an unnoticed error.

        The funding rate below is deliberately absurd. I5 is a check on the ledger, not on
        market plausibility, and the smallest realistic charge that overdraws a six-figure
        wallet would need a position too large to open.
        """
        account = make_account("20000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, OTHER, Decimal("100"), Decimal("300.0"))
        account.update_mark(T + 2, SYMBOL, Decimal("40000.0"))
        account.update_mark(T + 2, OTHER, Decimal("300.0"))

        assert len(account.check_liquidations(T + 2)) == 1
        assert account.liquidations == 1
        assert account.wallet == Decimal("15000")

        with pytest.raises(InvariantViolation, match="I5"):
            account.apply_funding(T + 3, OTHER, Decimal("0.6"))

    def test_a_wallet_left_negative_by_a_liquidation_does_not_re_raise(self) -> None:
        """The other direction, and why the exemption cannot simply be per-call.

        A liquidation can legitimately leave the wallet negative. Re-raising on every
        subsequent mutation would bury the one event that explains it under a stream of
        identical failures, so an already-negative wallet stays exempt. What I5 catches is
        the *transition* — a solvent wallet going negative with nothing to account for it.

        **The first version of this test did not test that.** It left the wallet at exactly
        `0.00` — so `_wallet_was_negative` was never set — and then called `update_mark`,
        which never reaches `_check_wallet` at all. Replacing the latch assignment with a
        constant `False` kept all 887 tests green. Mutation testing found it; the fix is to
        drive the wallet *strictly* below zero and follow up with a call that actually
        mutates it.

        ```
        BTC 1 @ 50 000 at 10x (margin 5 000) and ETH 50 @ 3 000 at 10x (margin 15 000)
        funding on BTC at 0.2 with mark 60 000  -> -12 000, wallet 8 000
        ETH marked down to 2 710                -> liquidated, -15 000, wallet -7 000
        ```
        """
        account = make_account("20000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, OTHER, Decimal("50"), Decimal("3000.0"))
        account.update_mark(T + 2, SYMBOL, Decimal("60000.0"))
        account.apply_funding(T + 2, SYMBOL, Decimal("0.2"))
        assert account.wallet == Decimal("8000.00")

        account.update_mark(T + 3, OTHER, Decimal("2710.0"))
        assert len(account.check_liquidations(T + 3)) == 1
        assert account.wallet < 0  # strictly, so the latch is genuinely set

        # Does not raise: the wallet was already negative and the liquidation is on the
        # log. `apply_funding` is used rather than `update_mark` because only a wallet
        # mutation reaches `_check_wallet` -- which is precisely what the original version
        # of this test got wrong.
        account.apply_funding(T + 4, SYMBOL, Decimal("0.0001"))
        assert account.wallet < Decimal("-7000")


class TestI8CoversLiquidations:
    def test_a_backwards_liquidation_sweep_is_refused(self) -> None:
        """`check_liquidations` did not call `_touch`, so LIQUIDATION events skipped I8.

        Every other mutator enforced monotonic timestamps. A liquidation could therefore be
        appended to the event log out of order, which breaks the reproducibility contract
        in spec 12.1 — the log hash stops being a function of the inputs.
        """
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 10, SYMBOL, Decimal("50000.0"))

        with pytest.raises(InvariantViolation, match="I8"):
            account.check_liquidations(T + 9)


class TestReconcileReplaysTheLog:
    """`reconcile` re-derives state from the event log instead of restating I1."""

    def test_it_catches_state_that_the_log_does_not_explain(self) -> None:
        """The bug: I9 was wired so that it could not fail.

        `reconcile` passed `self.equity` — defined as `wallet + unrealized_pnl` — alongside
        the same `unrealized_pnl`, so the term cancelled on both sides and the check reduced
        exactly to I1, which had already been asserted on every mutation. It was a check
        that always passed.

        The replay is genuinely independent: it shares no accumulator with the live path.
        Here the wallet is corrupted directly, which no accumulator-based check would see
        because every accumulator still agrees with itself.
        """
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, SYMBOL, Decimal("-1"), Decimal("51000.0"))
        account.reconcile()

        account.wallet += Decimal("0.01")
        with pytest.raises(InvariantViolation, match="event log does not reproduce"):
            account.reconcile()

    def test_it_catches_a_position_the_log_does_not_account_for(self) -> None:
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.reconcile()

        account.events.pop()
        with pytest.raises(InvariantViolation, match="position"):
            account.reconcile()


class TestOverdrawnAllocationIsNotSpendable:
    """`allocated_margin` floors each position at zero. Nothing pinned that before.

    The mutation: swap `reserved_margin` for the signed `isolated_margin` in
    `Account.allocated_margin`. All 887 tests stayed green, even though the docstring on
    `reserved_margin` calls the distinction load-bearing — a negative allocation is money
    the wallet has *already* paid out through funding, and subtracting a negative would
    hand it back as spendable balance a second time.
    """

    def test_a_negative_allocation_is_not_credited_back_as_available_balance(self) -> None:
        """```
        BTC 1 @ 50 000 at 10x -> allocation 5 000   |   ETH 20 @ 3 000 at 10x -> 6 000
        mark BTC 60 000, funding 0.1 -> -6 000: wallet 14 000, BTC allocation -1 000
        available = 14 000 - (0 + 6 000) = 8 000        <- floored
                    14 000 - (-1 000 + 6 000) = 9 000   <- the mutant, spending it twice
        ```
        """
        account = make_account("20000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, OTHER, Decimal("20"), Decimal("3000.0"))
        account.update_mark(T + 2, SYMBOL, Decimal("60000.0"))
        account.apply_funding(T + 2, SYMBOL, Decimal("0.1"))

        position = account.position(SYMBOL)
        assert position is not None
        assert position.isolated_margin == Decimal("-1000.00000000")
        assert position.reserved_margin == Decimal(0)

        assert account.wallet == Decimal("14000.00")
        assert account.allocated_margin == Decimal("6000.00000000")
        assert account.available_balance == Decimal("8000.00000000")

    def test_the_overdraft_cannot_be_re_spent_on_another_position(self) -> None:
        """The consequence: a fill sized against the mutant's balance must be refused."""
        account = make_account("20000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, OTHER, Decimal("20"), Decimal("3000.0"))
        account.update_mark(T + 2, SYMBOL, Decimal("60000.0"))
        account.apply_funding(T + 2, SYMBOL, Decimal("0.1"))

        # 28.4 ETH at 3 000 is 8 520 of margin at 10x -- inside the mutant's 9 000, outside
        # the real 8 000.
        with pytest.raises(InsufficientMargin, match="8000"):
            account.apply_fill(T + 3, OTHER, Decimal("28.4"), Decimal("3000.0"))


class TestQuantisationIsDeterministicallyPinned:
    def test_a_reduce_by_a_non_terminating_fraction_stays_on_the_money_seam(self) -> None:
        """`Position.scaled` divides, and the result reaches the wallet via a liquidation.

        Closing two thirds of a position makes `abs(new_qty)/abs(held)` non-terminating, so
        an unquantised `isolated_margin` carries fifty digits into `_liquidate`'s realised
        loss and re-opens the ~1e-23 I1 failure that `TestInvariantPrecision` exists to
        prevent.

        The property suite does catch this — about half the time, depending on whether
        Hypothesis happens to generate the shape. A guard that is only probabilistically
        tested is not tested; this is its deterministic twin.
        """
        account = make_account("100000", leverage=7)
        account.apply_fill(T, SYMBOL, Decimal("3"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        account.apply_funding(T + 1, SYMBOL, Decimal("0.0001"))
        account.apply_fill(T + 2, SYMBOL, Decimal("-2"), Decimal("50000.0"))

        position = account.position(SYMBOL)
        assert position is not None
        assert -position.isolated_margin.as_tuple().exponent <= 8
        assert -position.reserved_margin.as_tuple().exponent <= 8

        account.update_mark(T + 3, SYMBOL, Decimal("40000.0"))
        account.check_liquidations(T + 3)
        account.reconcile()


class TestInvariantCallSitesAreReachable:
    """Each of I2/I3/I4 was deletable from `Account` with the suite green.

    The free functions were well covered; what was not covered was `Account` actually
    calling them. Corrupting state behind the account's back and asserting that the *next*
    operation raises is the shape that distinguishes "the check exists" from "the check
    runs" — the same shape `TestReconcileReplaysTheLog` already uses.
    """

    def test_i3_fires_from_apply_fill(self) -> None:
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account._signed_fills[(SYMBOL, PositionSide.BOTH)] += Decimal("1")

        with pytest.raises(InvariantViolation, match="I3"):
            account.apply_fill(T + 1, SYMBOL, Decimal("1"), Decimal("50000.0"))

    def test_i4_fires_from_apply_fill(self) -> None:
        """A flat position holding a stale entry price feeds I2 a phantom uPnL next tick."""
        from dataclasses import replace as dc_replace

        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        position = account.position(SYMBOL)
        assert position is not None
        # Desynchronise the accumulator so the close leaves a position the ledger thinks
        # is flat while `_signed_fills` disagrees -- I4's precondition.
        account.positions[(SYMBOL, PositionSide.BOTH)] = dc_replace(
            position, qty=Decimal("2")
        )

        with pytest.raises(InvariantViolation, match="I3|I4"):
            account.apply_fill(T + 1, SYMBOL, Decimal("-2"), Decimal("50000.0"))

    def test_i2_fires_from_update_mark(self) -> None:
        """Corrupt the wallet, and the next mark update must not accept the equity."""
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("50000.0"))
        account.total_fees += Decimal("1")  # breaks I1, which update_mark does not check

        with pytest.raises(InvariantViolation, match="I1|I2"):
            account.apply_fill(T + 2, SYMBOL, Decimal("0.001"), Decimal("50000.0"))


class TestExhaustedMarginIsNotAnAutomaticLiquidation:
    def test_the_trigger_inequality_still_governs(self) -> None:
        """Spec 3.7's condition carries unrealised PnL; a short-circuit on margin does not.

        Covered end-to-end in `tests/golden/test_funding.py`; asserted here as the direct
        statement, because it was a *critical* finding: the short-circuit destroyed solvent,
        profitable positions and booked the loss as though the exchange had liquidated them.
        """
        account = make_account("20000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("60000.0"))
        account.apply_funding(T + 1, SYMBOL, Decimal("0.1"))  # -6 000 against a 5 000 allocation

        position = account.position(SYMBOL)
        assert position is not None
        assert position.isolated_margin < 0
        assert account.check_liquidations(T + 2) == []
        assert account.unrealized_pnl == Decimal("10000")
