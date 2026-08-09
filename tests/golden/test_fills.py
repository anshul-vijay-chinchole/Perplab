"""Fill application golden cases (spec 3.3, 12.2).

Spec 3.3 calls fill application "the single most bug-prone function in the codebase" and
supplies four sign checks for it. All four are here verbatim, plus the partial-fill
sequence spec 12.2 asks for and the entry-price behaviour that spec 3.3 marks *critical*.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.core.account import Account, FeeSchedule, InsufficientMargin
from perplab.core.invariants import InvariantViolation
from tests.support import btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
T = 1_700_000_000_000


@pytest.fixture()
def account() -> Account:
    """No fees, so realised PnL is visible unmixed with commission.

    Spec 3.3's checks are stated without fees; adding them here would mean every assertion
    carried a `- 26.00` that tests the fee model rather than the case logic, which
    `test_worked_example` already covers end to end.
    """
    account = Account(
        opening_balance=Decimal("20000"),
        fees=FeeSchedule.all_taker(Decimal(0), source="zero-fee-fixture"),
        brackets={SYMBOL: single_bracket_table()},
        filters={SYMBOL: btcusdt_filters()},
    )
    account.set_leverage(SYMBOL, 10)
    return account


class TestUnrealisedPnLSigns:
    """Spec 3.3's two uPnL checks. Both come out positive; only one is obvious."""

    def test_long_in_profit(self, account: Account) -> None:
        """long 1 @ 50 000, mark 51 000 -> `1 x 1000 = +1000`"""
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("51000.0"))
        assert account.unrealized_pnl == Decimal("1000")

    def test_short_in_profit(self, account: Account) -> None:
        """short 1 (`Q = -1`) @ 50 000, mark 49 000 -> `-1 x (-1000) = +1000`

        The double negative is the whole reason the spec writes this one out. A short is
        profitable when the price falls, and the formula gets there by multiplying two
        negatives -- so an implementation that "fixes" the sign by taking an absolute value
        somewhere reports a loss on every winning short.
        """
        account.apply_fill(T, SYMBOL, Decimal("-1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("49000.0"))
        assert account.unrealized_pnl == Decimal("1000")


class TestCaseAOpenOrIncrease:
    def test_entry_price_is_the_weighted_average(self, account: Account) -> None:
        """`Pe' = (|Q|*Pe + |f|*Pf) / (|Q| + |f|)`.

        0.1 @ 50 000 then 0.3 @ 54 000 -> `(5 000 + 16 200) / 0.4 = 53 000`. Weighted, not
        the midpoint of 52 000 -- an average that ignores size understates the cost basis
        of exactly the positions that were scaled into, which is most of them.
        """
        account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))
        account.apply_fill(T + 1, SYMBOL, Decimal("0.3"), Decimal("54000.0"))

        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("0.4")
        assert position.entry_price == Decimal("53000")
        assert account.total_realized == Decimal(0)

    def test_increasing_a_short_averages_the_same_way(self, account: Account) -> None:
        account.apply_fill(T, SYMBOL, Decimal("-1"), Decimal("50000.0"))
        account.apply_fill(T + 1, SYMBOL, Decimal("-1"), Decimal("52000.0"))

        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("-2")
        assert position.entry_price == Decimal("51000")


class TestCaseBReduce:
    def test_long_reduce(self, account: Account) -> None:
        """`Q=+1, Pe=50 000`, sell `0.5 @ 52 000` -> `realized = +1 x 0.5 x 2000 = +1000`"""
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        result = account.apply_fill(T + 1, SYMBOL, Decimal("-0.5"), Decimal("52000.0"))

        assert result.realized == Decimal("1000")
        assert account.qty(SYMBOL) == Decimal("0.5")

    def test_short_reduce(self, account: Account) -> None:
        """`Q=-1, Pe=50 000`, buy `0.5 @ 48 000` -> `realized = -1 x 0.5 x (-2000) = +1000`"""
        account.apply_fill(T, SYMBOL, Decimal("-1"), Decimal("50000.0"))
        result = account.apply_fill(T + 1, SYMBOL, Decimal("0.5"), Decimal("48000.0"))

        assert result.realized == Decimal("1000")
        assert account.qty(SYMBOL) == Decimal("-0.5")

    def test_entry_price_survives_the_reduce(self, account: Account) -> None:
        """Spec 3.3 marks this critical, and it is the one that quietly breaks everything.

        Recomputing entry on a partial close moves the cost basis of the remaining
        position, so the *next* close measures PnL from a price that was never paid. The
        error nets out only if the position is closed in one go -- meaning it is invisible
        in every simple test and present in every scaled-out trade.
        """
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, SYMBOL, Decimal("-0.5"), Decimal("52000.0"))

        position = account.position(SYMBOL)
        assert position is not None
        assert position.entry_price == Decimal("50000.0")

    def test_full_close_clears_the_entry_price(self, account: Account) -> None:
        """Invariant I4: `Q == 0` if and only if `Pe is None`."""
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        result = account.apply_fill(T + 1, SYMBOL, Decimal("-1"), Decimal("52000.0"))

        assert result.closed
        assert account.position(SYMBOL) is None
        assert account.qty(SYMBOL) == Decimal(0)
        assert account.total_realized == Decimal("2000")


class TestCaseCFlip:
    def test_spec_flip(self, account: Account) -> None:
        """`Q=+1, Pe=50 000`, sell `1.5 @ 52 000` -> `realized=+2000, Q'=-0.5, Pe'=52 000`

        **The spec's stated check for this case is wrong; its formula is right.** Spec 3.3
        writes `realized = sign(Q) * |Q| * (Pf - Pe)` and then checks it as "+1000". The
        formula gives `+1 x 1 x (52 000 - 50 000) = +2000`, and 2 000 is the economically
        unambiguous answer: the flip closes the *entire* 1 BTC long from 50 000 to 52 000
        before opening the residual short. The "+1000" is Case B's answer -- that check
        sells only 0.5 -- carried into Case C by transcription.

        Implemented to the formula. See `docs/ACCOUNTING_NOTES.md` A1. Coding to the stated
        check instead would have halved realised PnL on every flip in the platform, and
        because both numbers are round and plausible it would have survived review.

        Three things change at once here and each has its own failure: the realised amount
        must cover only the closed portion (not all 1.5), the residual must carry the
        *opposite* sign, and its entry price must be the fill price rather than the old
        entry. Getting the first two right and the third wrong leaves the new short holding
        the long's cost basis.
        """
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        result = account.apply_fill(T + 1, SYMBOL, Decimal("-1.5"), Decimal("52000.0"))

        assert result.realized == Decimal("2000")
        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("-0.5")
        assert position.entry_price == Decimal("52000.0")

    def test_flip_realises_exactly_the_closed_portion(self, account: Account) -> None:
        """The flip must realise what closing the position outright would have realised.

        This is the check that settles the spec's ambiguity without appealing to the
        formula at all: selling 1.5 is selling 1.0 and then selling 0.5 more, so the
        realised PnL of the flip has to equal the realised PnL of the plain close. Any
        other answer means the residual short was credited with profit it has not made yet.
        """
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        flip = account.apply_fill(T + 1, SYMBOL, Decimal("-1.5"), Decimal("52000.0"))

        plain = Account(
            opening_balance=Decimal("20000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        plain.set_leverage(SYMBOL, 10)
        plain.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        close = plain.apply_fill(T + 1, SYMBOL, Decimal("-1"), Decimal("52000.0"))

        assert flip.realized == close.realized == Decimal("2000")

    def test_flip_from_short_to_long(self, account: Account) -> None:
        """The mirror: `Q=-1 @ 50 000`, buy `1.5 @ 48 000` -> `realized = +2000, Q' = +0.5`."""
        account.apply_fill(T, SYMBOL, Decimal("-1"), Decimal("50000.0"))
        result = account.apply_fill(T + 1, SYMBOL, Decimal("1.5"), Decimal("48000.0"))

        assert result.realized == Decimal("2000")
        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("0.5")
        assert position.entry_price == Decimal("48000.0")

    def test_flip_leaves_position_equal_to_summed_fills(self, account: Account) -> None:
        """Invariant I3, at the one point it is most likely to fail.

        A flip crosses through zero, which makes it tempting to compute the residual from
        `|f| - |Q|` and then attach a sign to it -- and to attach the wrong one.
        """
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, SYMBOL, Decimal("-1.5"), Decimal("52000.0"))
        assert account.qty(SYMBOL) == Decimal("1") + Decimal("-1.5")


class TestPartialFillSequence:
    def test_scaling_out_in_three_pieces(self, account: Account) -> None:
        """Spec 12.2's partial fill sequence.

        Open 1 @ 50 000, then sell 0.3 @ 51 000, 0.3 @ 52 000, 0.4 @ 49 000:

        ```
        0.3 x (51 000 - 50 000) = +300
        0.3 x (52 000 - 50 000) = +600
        0.4 x (49 000 - 50 000) = -400
                                  ----
                                  +500
        ```

        Every leg is measured from the *original* 50 000 entry. If the entry price drifted
        on the first two reduces, the third would be measured from something else and the
        total would not be 500 -- which is the whole reason the sequence is a golden test
        rather than three separate ones.
        """
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        legs = [
            (Decimal("-0.3"), Decimal("51000.0"), Decimal("300")),
            (Decimal("-0.3"), Decimal("52000.0"), Decimal("600")),
            (Decimal("-0.4"), Decimal("49000.0"), Decimal("-400")),
        ]
        for index, (qty, price, expected) in enumerate(legs, start=1):
            result = account.apply_fill(T + index, SYMBOL, qty, price)
            assert result.realized == expected

        assert account.position(SYMBOL) is None
        assert account.total_realized == Decimal("500")
        assert account.wallet == Decimal("20500")
        account.reconcile()


class TestFilterEnforcement:
    def test_off_tick_fill_price_is_refused(self, account: Account) -> None:
        """Invariant I6. A fill at 50 000.05 on a 0.10 tick is a price that never printed.

        Spec 3.2's rule stated from the other direction: a backtest that lets you buy
        0.13847362 BTC at a step of 0.001 is fiction, and so is one that fills you at half
        a tick.
        """
        with pytest.raises(InvariantViolation, match="I6"):
            account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.05"))

    def test_off_step_quantity_is_refused(self, account: Account) -> None:
        with pytest.raises(InvariantViolation, match="I6"):
            account.apply_fill(T, SYMBOL, Decimal("0.13847362"), Decimal("50000.0"))

    def test_filters_absent_means_unchecked_not_invented(self) -> None:
        """Spec 3.2's `FILTERS_APPROXIMATE` case: no snapshot, no enforcement.

        The run is supposed to be *flagged*, not silently held to today's limits. Inventing
        a tick size here would make a 2021 backtest reject orders that 2021 accepted.
        """
        unfiltered = Account(
            opening_balance=Decimal("20000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
        )
        unfiltered.apply_fill(T, SYMBOL, Decimal("0.13847362"), Decimal("50000.05"))
        assert unfiltered.qty(SYMBOL) == Decimal("0.13847362")


class TestMarginEnforcement:
    def test_a_fill_beyond_available_margin_is_refused(self, account: Account) -> None:
        """20 000 of wallet at 10x funds 200 000 of notional, and not a tick more.

        Without this the account trades at unbounded leverage the moment a strategy sizes
        in notional rather than in margin -- which is how position sizing is usually
        written, and which turns every backtest into a fantasy about a margin call that
        never came.
        """
        with pytest.raises(InsufficientMargin, match="available"):
            account.apply_fill(T, SYMBOL, Decimal("5"), Decimal("50000.0"))

    def test_a_reduce_never_fails_the_margin_check(self, account: Account) -> None:
        """Closing frees margin; it cannot require more. Not obvious from the code path.

        A naive check on the post-fill allocation alone would reject a close whenever the
        account was already fully committed -- trapping the strategy in the position at
        exactly the moment it most needs out.
        """
        account.apply_fill(T, SYMBOL, Decimal("4"), Decimal("50000.0"))
        assert account.available_balance == Decimal(0)

        account.apply_fill(T + 1, SYMBOL, Decimal("-4"), Decimal("50000.0"))
        assert account.position(SYMBOL) is None
