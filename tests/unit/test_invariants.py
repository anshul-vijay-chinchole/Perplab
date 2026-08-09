"""Each spec 3.10 invariant, in both directions (spec 12.2).

A conservation check is only worth having if it fires. Every test here comes in a pair: a
state that must pass and a state that must raise, because a check that never raises and a
check that is never called are indistinguishable from the outside -- and the second is the
more likely of the two after a refactor.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.core.invariants import (
    InvariantViolation,
    check_entry_price_presence,
    check_equity,
    check_liquidation_ordering,
    check_monotonic_timestamps,
    check_pnl_decomposition,
    check_position_sum,
    check_tick_and_step,
    check_wallet_conservation,
    check_wallet_non_negative,
)


class TestI1WalletConservation:
    def test_holds(self) -> None:
        check_wallet_conservation(
            Decimal("10290.415"),
            Decimal("10000"),
            Decimal("300"),
            Decimal("9.075"),
            Decimal("-0.51"),
        )

    def test_an_unbooked_adjustment_is_caught(self) -> None:
        """One cent of wallet that no accumulator explains.

        This is the shape every "small correction" takes -- a rebate, a rounding fix-up, a
        balance nudge -- and it is caught on the very next mutation rather than at the end
        of a 40 000-bar run where the only symptom is a final number nobody can attribute.
        """
        with pytest.raises(InvariantViolation, match="I1"):
            check_wallet_conservation(
                Decimal("10000.01"), Decimal("10000"), Decimal(0), Decimal(0), Decimal(0)
            )

    def test_fees_are_subtracted_not_added(self) -> None:
        """A sign flip on fees passes every casual reading and fails here."""
        with pytest.raises(InvariantViolation, match="I1"):
            check_wallet_conservation(
                Decimal("10010"), Decimal("10000"), Decimal(0), Decimal("10"), Decimal(0)
            )


class TestI2Equity:
    def test_flat_account_equity_is_the_wallet(self) -> None:
        check_equity(Decimal("10000"), Decimal("10000"), [])

    def test_open_position(self) -> None:
        check_equity(
            Decimal("10100"),
            Decimal("10000"),
            [(Decimal("0.1"), Decimal("51000"), Decimal("50000"))],
        )

    def test_sums_across_symbols(self) -> None:
        """Stated as a sum so spec 9.5's portfolio backtesting does not reopen the ledger."""
        check_equity(
            Decimal("10150"),
            Decimal("10000"),
            [
                (Decimal("0.1"), Decimal("51000"), Decimal("50000")),
                (Decimal("-1"), Decimal("2950"), Decimal("3000")),
            ],
        )

    def test_stale_unrealised_pnl_is_caught(self) -> None:
        with pytest.raises(InvariantViolation, match="I2"):
            check_equity(
                Decimal("10200"),
                Decimal("10000"),
                [(Decimal("0.1"), Decimal("51000"), Decimal("50000"))],
            )

    def test_a_flat_position_must_not_be_carried(self) -> None:
        with pytest.raises(InvariantViolation, match="must not be carried"):
            check_equity(
                Decimal("10000"),
                Decimal("10000"),
                [(Decimal(0), Decimal("51000"), Decimal("50000"))],
            )

    def test_open_position_without_an_entry_price_is_caught(self) -> None:
        with pytest.raises(InvariantViolation, match="no entry price"):
            check_equity(
                Decimal("10000"), Decimal("10000"), [(Decimal("0.1"), Decimal("51000"), None)]
            )


class TestI3PositionSum:
    def test_holds(self) -> None:
        check_position_sum(Decimal("-0.5"), Decimal("1") + Decimal("-1.5"))

    def test_a_flip_with_the_wrong_sign_is_caught(self) -> None:
        """`|f| - |Q|` gives 0.5; the answer is -0.5. The magnitude is right and useless."""
        with pytest.raises(InvariantViolation, match="I3"):
            check_position_sum(Decimal("0.5"), Decimal("-0.5"))


class TestI4EntryPricePresence:
    def test_both_legal_states(self) -> None:
        check_entry_price_presence(Decimal(0), None)
        check_entry_price_presence(Decimal("0.1"), Decimal("50000"))

    def test_stale_entry_on_a_flat_position(self) -> None:
        with pytest.raises(InvariantViolation, match="I4"):
            check_entry_price_presence(Decimal(0), Decimal("50000"))

    def test_missing_entry_on_an_open_position(self) -> None:
        with pytest.raises(InvariantViolation, match="I4"):
            check_entry_price_presence(Decimal("0.1"), None)


class TestI5WalletNonNegative:
    def test_positive_wallet_always_passes(self) -> None:
        check_wallet_non_negative(Decimal("0.01"), liquidated=False)
        check_wallet_non_negative(Decimal(0), liquidated=False)

    def test_negative_wallet_without_a_liquidation_is_caught(self) -> None:
        with pytest.raises(InvariantViolation, match="I5"):
            check_wallet_non_negative(Decimal("-0.01"), liquidated=False)

    def test_negative_wallet_after_a_liquidation_is_permitted(self) -> None:
        """Not a loophole: several positions liquidating can legitimately overdraw.

        What must never happen is going negative *silently* -- that means fees or funding
        were charged against money that was not there.
        """
        check_wallet_non_negative(Decimal("-100"), liquidated=True)


class TestI6TickAndStep:
    def test_holds(self) -> None:
        check_tick_and_step(
            Decimal("50000.10"), Decimal("0.001"), Decimal("0.10"), Decimal("0.001")
        )

    def test_off_tick_price(self) -> None:
        with pytest.raises(InvariantViolation, match="tick size"):
            check_tick_and_step(
                Decimal("50000.05"), Decimal("0.001"), Decimal("0.10"), Decimal("0.001")
            )

    def test_off_step_quantity(self) -> None:
        with pytest.raises(InvariantViolation, match="step size"):
            check_tick_and_step(
                Decimal("50000.10"), Decimal("0.0015"), Decimal("0.10"), Decimal("0.001")
            )

    def test_needs_no_epsilon(self) -> None:
        """`%` on `Decimal` is exact, which is the point of scaled-integer storage.

        `0.30000001 % 0.1` is not zero and is not nearly zero either -- there is no
        tolerance to tune, and if one were ever needed it would mean a float had leaked in.
        """
        with pytest.raises(InvariantViolation):
            check_tick_and_step(
                Decimal("0.30000001"), Decimal("0.001"), Decimal("0.1"), Decimal("0.001")
            )

    def test_non_positive_increments_are_a_programming_error(self) -> None:
        with pytest.raises(ValueError):
            check_tick_and_step(Decimal(1), Decimal(1), Decimal(0), Decimal("0.001"))


class TestI7LiquidationOrdering:
    def test_long_ordering(self) -> None:
        check_liquidation_ordering(
            Decimal(1), Decimal(50000), Decimal("45180.72"), Decimal(45000)
        )

    def test_short_ordering(self) -> None:
        check_liquidation_ordering(
            Decimal(-1), Decimal(50000), Decimal("54780.88"), Decimal(55000)
        )

    def test_long_with_liquidation_above_entry_is_caught(self) -> None:
        with pytest.raises(InvariantViolation, match="bracket resolution is wrong"):
            check_liquidation_ordering(
                Decimal(1), Decimal(50000), Decimal(51000), Decimal(45000)
            )

    def test_short_with_liquidation_below_entry_is_caught(self) -> None:
        with pytest.raises(InvariantViolation, match="I7"):
            check_liquidation_ordering(
                Decimal(-1), Decimal(50000), Decimal(49000), Decimal(55000)
            )

    def test_unreachable_liquidation_price_is_skipped(self) -> None:
        check_liquidation_ordering(
            Decimal(1), Decimal(50000), Decimal("-1000"), Decimal("-5000")
        )

    def test_flat_position_has_no_ordering(self) -> None:
        with pytest.raises(InvariantViolation, match="flat position"):
            check_liquidation_ordering(
                Decimal(0), Decimal(50000), Decimal(45000), Decimal(44000)
            )


class TestI8MonotonicTimestamps:
    def test_forward_and_equal_both_pass(self) -> None:
        """Non-decreasing, not increasing: spec 6.2 orders same-millisecond events by kind."""
        check_monotonic_timestamps(100, 101)
        check_monotonic_timestamps(100, 100)

    def test_backwards_is_caught(self) -> None:
        with pytest.raises(InvariantViolation, match="I8"):
            check_monotonic_timestamps(100, 99)


class TestI9PnlDecomposition:
    def test_holds(self) -> None:
        check_pnl_decomposition(
            Decimal("10390.415"),
            Decimal("10000"),
            Decimal("300"),
            Decimal("9.075"),
            Decimal("-0.51"),
            Decimal("100"),
        )

    def test_a_cancelling_pair_that_survives_i1_and_i2_is_caught(self) -> None:
        with pytest.raises(InvariantViolation, match="I9"):
            check_pnl_decomposition(
                Decimal("10391"),
                Decimal("10000"),
                Decimal("300"),
                Decimal("9.075"),
                Decimal("-0.51"),
                Decimal("100"),
            )


def test_violation_carries_the_invariant_id() -> None:
    """So a handler can route on it -- the live kill switch cares which one failed."""
    with pytest.raises(InvariantViolation) as caught:
        check_monotonic_timestamps(100, 99)
    assert caught.value.invariant == "I8"


def test_violations_are_raised_not_asserted() -> None:
    """`python -O` strips `assert`. An accounting guarantee must not be optimisable away.

    This is why `invariants.py` calls `_fail` rather than writing `assert`: the checks are
    part of the engine's behaviour, not a debug aid, and spec 3.10 has them aborting
    backtests and tripping the live kill switch.
    """
    import ast
    from pathlib import Path

    source = Path(__file__).resolve().parents[2] / "perplab" / "core" / "invariants.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    assert not [n for n in ast.walk(tree) if isinstance(n, ast.Assert)]
