"""Liquidation golden cases (spec 3.6, 3.7, 12.2).

Covers the spec's two worked checks (a 10x long and a 10x short), the trigger condition
against the mark series, the total-margin-loss model, and the bracket-boundary crossing
that spec 3.6's fixed-point iteration exists to handle.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.core.account import Account, AccountEventKind, FeeSchedule
from perplab.core.invariants import InvariantViolation
from perplab.core.margin import (
    BracketConvergenceError,
    BracketTable,
    LeverageBracket,
    bankruptcy_price,
    liquidation_price,
)
from tests.support import (
    SPEC_MMR,
    btcusdt_filters,
    graduated_bracket_table,
    single_bracket_table,
)

SYMBOL = "BTCUSDT"
TAKER = Decimal("0.0005")
T = 1_700_000_000_000


def cents(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def make_account(balance: str = "6000", **kwargs: object) -> Account:
    account = Account(
        opening_balance=Decimal(balance),
        fees=FeeSchedule.all_taker(TAKER, source="spec-3.7"),
        brackets={SYMBOL: single_bracket_table()},
        filters={SYMBOL: btcusdt_filters()},
        **kwargs,  # type: ignore[arg-type]
    )
    account.set_leverage(SYMBOL, 10)
    return account


# ----------------------------------------------------------------- spec 3.7 formula


class TestSpecWorkedChecks:
    """Spec 3.7's two worked checks, at the level of the formula rather than the account."""

    def test_ten_x_long(self) -> None:
        """`Q = +1, Pe = 50 000, W = 5 000, MMR = 0.004, MA = 0`

        ```
        P_liq = (5 000 - 50 000 + 0) / (1 x 0.004 - 1) = -45 000 / -0.996 = 45 180.72
        ```
        """
        solution = liquidation_price(
            qty=Decimal(1),
            entry_price=Decimal(50000),
            margin=Decimal(5000),
            mark_price=Decimal(50000),
            table=single_bracket_table(),
        )
        assert cents(solution.price) == Decimal("45180.72")
        assert solution.iterations == 1

    def test_ten_x_short(self) -> None:
        """`Q = -1` and otherwise identical.

        ```
        P_liq = (5 000 + 50 000 + 0) / (1 x 0.004 + 1) = 55 000 / 1.004 = 54 780.88
        ```
        """
        solution = liquidation_price(
            qty=Decimal(-1),
            entry_price=Decimal(50000),
            margin=Decimal(5000),
            mark_price=Decimal(50000),
            table=single_bracket_table(),
        )
        assert cents(solution.price) == Decimal("54780.88")

    def test_bankruptcy_prices(self) -> None:
        """`P_bank = Pe - W/Q`: 45 000 for the long, 55 000 for the short."""
        assert bankruptcy_price(Decimal(1), Decimal(50000), Decimal(5000)) == Decimal(45000)
        assert bankruptcy_price(Decimal(-1), Decimal(50000), Decimal(5000)) == Decimal(55000)

    @pytest.mark.parametrize("qty", [Decimal(1), Decimal(-1)])
    def test_i7_ordering_holds(self, qty: Decimal) -> None:
        """`P_bank < P_liq < Pe` long, `Pe < P_liq < P_bank` short (spec 3.10 I7).

        The liquidation price sits strictly *inside* the bankruptcy price because
        liquidation fires while maintenance margin is still intact -- that gap is what the
        exchange's liquidation engine has to work with. If the two ever coincide or
        invert, the maintenance rate came from the wrong bracket.
        """
        entry, margin = Decimal(50000), Decimal(5000)
        p_liq = liquidation_price(
            qty=qty,
            entry_price=entry,
            margin=margin,
            mark_price=entry,
            table=single_bracket_table(),
        ).price
        p_bank = bankruptcy_price(qty, entry, margin)

        if qty > 0:
            assert p_bank < p_liq < entry
        else:
            assert entry < p_liq < p_bank


# --------------------------------------------------------------- bracket resolution


class TestBracketCrossing:
    """Spec 3.6's circularity: the bracket depends on the price we are solving for."""

    def test_short_liquidation_crosses_into_the_next_tier(self) -> None:
        """The same short resolves differently once the tiers are real.

        Pass 1, bracket 1 (`MMR = 0.004, MA = 0`) at the 50 000 entry notional:
        `55 000 / 1.004 = 54 780.88`. But 1 BTC at 54 780.88 is a 54 780.88 notional, which
        is *above* tier 1's 50 000 cap, so the answer contradicts the assumption it was
        computed under.

        Pass 2, bracket 2 (`MMR = 0.005`, `MA = 50 000 x (0.005 - 0.004) = 50`):
        `(5 000 + 50 000 + 50) / 1.005 = 55 050 / 1.005 = 54 776.12`, whose notional is
        still inside tier 2. Fixed point reached.

        The difference is only 4.76 USDT, and that is the point: an implementation that
        skipped the iteration would be wrong by an amount far too small to notice and in
        the optimistic direction -- it reports the liquidation as further away than it is.
        """
        solution = liquidation_price(
            qty=Decimal(-1),
            entry_price=Decimal(50000),
            margin=Decimal(5000),
            mark_price=Decimal(50000),
            table=graduated_bracket_table(),
        )
        assert cents(solution.price) == Decimal("54776.12")
        assert solution.bracket.bracket == 2
        assert solution.iterations == 2

    def test_maintenance_margin_is_continuous_across_the_boundary(self) -> None:
        """That is what `cum` is for, and why it cannot be dropped.

        At the 50 000 boundary both tiers must charge 200 USDT of maintenance margin:
        `50 000 x 0.004 - 0 == 50 000 x 0.005 - 50`. Without the deduction, crossing a tier
        would step the requirement up discontinuously and manufacture a liquidation out of
        one extra dollar of notional.
        """
        from perplab.core.margin import maintenance_margin

        table = graduated_bracket_table()
        boundary = Decimal(50000)
        below = maintenance_margin(boundary, table.resolve(boundary))
        above = maintenance_margin(boundary + Decimal(1), table.resolve(boundary + Decimal(1)))

        assert below == Decimal(200)
        assert above - below == Decimal("0.005")  # one dollar of notional at tier 2's rate

    def test_non_convergence_raises_rather_than_guessing(self) -> None:
        """Spec 3.6/R6: "if it does not converge, raise -- do not return a guess."

        Constructed to oscillate, using a tier whose maintenance amount was *not* derived
        from continuity -- which is exactly what a malformed or truncated snapshot looks
        like. For the short below:

        ```
        under tier 1 (MMR 0.004, MA 0):  55 000 / 1.004 = 54 780.88  -> notional in tier 2
        under tier 2 (MMR 0.2,   MA 0):  55 000 / 1.200 = 45 833.33  -> notional in tier 1
        ```

        Each tier's answer implies the other tier, forever. Both are internally consistent
        and there is no basis for preferring either, so returning one would be picking a
        liquidation price by coin flip. A real published table cannot do this because the
        `cum` continuity prevents it, which is why the guard has to be tested against a
        synthetic one -- the alternative is shipping an untested branch that fires only
        when the reference data is already broken.
        """
        pathological = BracketTable(
            symbol=SYMBOL,
            brackets=(
                LeverageBracket(
                    bracket=1,
                    max_leverage=125,
                    notional_floor=Decimal(0),
                    notional_cap=Decimal(50000),
                    mmr=Decimal("0.004"),
                    maintenance_amount=Decimal(0),
                ),
                LeverageBracket(
                    bracket=2,
                    max_leverage=100,
                    notional_floor=Decimal(50000),
                    notional_cap=Decimal(10) ** 9,
                    mmr=Decimal("0.2"),
                    maintenance_amount=Decimal(0),
                ),
            ),
        )
        with pytest.raises(BracketConvergenceError, match="did not converge"):
            liquidation_price(
                qty=Decimal(-1),
                entry_price=Decimal(50000),
                margin=Decimal(5000),
                mark_price=Decimal(50000),
                table=pathological,
            )


# ------------------------------------------------------------------ account-level


class TestLiquidationOfALong:
    def test_triggers_when_mark_reaches_the_liquidation_price(self) -> None:
        """Long liquidates at `Pm <= P_liq`, and loses the entire isolated margin.

        Opening 1 BTC at 50 000 on 10x allocates 5 000 of isolated margin and costs 25 in
        taker fees, so `W = 5 975`. The liquidation destroys the 5 000, leaving 975.
        """
        account = make_account("6000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        assert account.wallet == Decimal("5975.00")

        account.update_mark(T + 1, SYMBOL, Decimal("46000.0"))
        assert account.check_liquidations(T + 1) == []

        account.update_mark(T + 2, SYMBOL, Decimal("45000.0"))
        results = account.check_liquidations(T + 2)

        assert len(results) == 1
        assert results[0].margin_lost == Decimal("5000.00")
        assert results[0].recovered == Decimal(0)
        assert account.wallet == Decimal("975.00")
        assert account.position(SYMBOL) is None
        assert account.liquidations == 1
        assert account.events[-1].kind is AccountEventKind.LIQUIDATION

    def test_a_trade_wick_below_liquidation_does_not_liquidate(self) -> None:
        """Spec 3.4: risk is evaluated on mark price, never on last traded price.

        A fill printed at 44 000 while the mark holds at 46 000 is a trade that happened,
        not a liquidation that happened. Conflating the two liquidates positions that
        survived, which is the more expensive direction of this classic bug -- it deletes
        the strategy's best trades, not its worst.
        """
        account = make_account("6000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("46000.0"))

        account.apply_fill(T + 2, SYMBOL, Decimal("0.001"), Decimal("44000.0"))
        assert account.check_liquidations(T + 3) == []
        assert account.position(SYMBOL) is not None


class TestLiquidationOfAShort:
    def test_triggers_upward(self) -> None:
        """Short liquidates at `Pm >= P_liq`, at 54 780.88 for the spec's parameters."""
        account = make_account("6000")
        account.apply_fill(T, SYMBOL, Decimal("-1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("54000.0"))
        assert account.check_liquidations(T + 1) == []

        account.update_mark(T + 2, SYMBOL, Decimal("54800.0"))
        results = account.check_liquidations(T + 2)

        assert len(results) == 1
        assert cents(results[0].trigger_price) == Decimal("54780.88")
        assert account.position(SYMBOL) is None


class TestLiquidationModel:
    def test_recovery_knob_returns_a_fraction_of_remaining_margin(self) -> None:
        """Spec 3.7's `liquidation_recovery_pct`, for sensitivity analysis only.

        The default of zero is the realistic model for isolated margin. This knob exists so
        a run can ask "how much does that assumption matter", not so it can be turned up
        until the blow-ups stop.

        The fraction applies to the margin balance **remaining after the close** -- spec
        6.6 writes the model as `W -= remaining isolated margin x (1 -
        liquidation_recovery_pct)` -- not to the original allocation. An earlier
        revision recovered `0.25 x 5 000 = 1 250` here regardless of where the mark was
        caught, which refunds price losses the market already took; at a mark of 45 000
        (the bankruptcy price exactly) it manufactured 1 250 out of a position with
        nothing left in it, and at 100% recovery it modelled every liquidation as a
        scratch at the entry price. Marked at 45 100 -- inside the trigger at 45 180.72,
        above bankruptcy at 45 000 -- the corrected arithmetic is:

        ```
        price leg = 1 x (45 100 - 50 000)  = -4 900
        remaining = 5 000 - 4 900          =    100
        recovered = 0.25 x 100             =     25
        lost      = 5 000 - 25             =  4 975
        wallet    = 5 975 - 4 975          =  1 000
        ```
        """
        account = make_account("6000", liquidation_recovery_pct=Decimal("0.25"))
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("45100.0"))

        (result,) = account.check_liquidations(T + 1)
        assert result.recovered == Decimal("25.00")
        assert result.margin_lost == Decimal("4975.00")
        assert account.wallet == Decimal("1000.00")  # 5975 - 4975
        account.reconcile()

    def test_no_separate_commission_is_charged(self) -> None:
        """The clearance fee is inside the total-margin-loss model, not on top of it.

        Spec 3.7 models the whole margin as consumed *because* the fee and the adverse fill
        consume it. Charging a taker commission as well would double-count, and on a
        position whose margin is the whole wallet it would drive the balance negative --
        which invariant I5 would then have to excuse rather than catch.
        """
        account = make_account("6000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        fees_before = account.total_fees
        account.update_mark(T + 1, SYMBOL, Decimal("45000.0"))
        account.check_liquidations(T + 1)

        assert account.total_fees == fees_before
        assert account.wallet >= 0

    def test_conservation_survives_a_liquidation(self) -> None:
        """I1 and I9 must still hold once the margin has been destroyed.

        Booking the loss anywhere other than realised PnL -- as a balance adjustment, say --
        passes every eyeball test and fails this one immediately.
        """
        account = make_account("6000")
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("45000.0"))
        account.check_liquidations(T + 1)

        assert account.total_realized == Decimal("-5000.00")
        assert (
            account.opening_balance
            + account.total_realized
            - account.total_fees
            + account.total_funding
        ) == account.wallet
        account.reconcile()


class TestUnreachableLiquidation:
    def test_unleveraged_long_has_no_reachable_liquidation_price(self) -> None:
        """At 1x the solved price is negative -- the mark would have to go below zero.

        Returned as `None` rather than clamped to zero. Zero is a price, and a trigger
        check written against it would be comparing the mark to a number that means
        "never" as though it meant "at zero".
        """
        account = Account(
            opening_balance=Decimal("60000"),
            fees=FeeSchedule.all_taker(TAKER),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))

        assert account.liquidation_price(SYMBOL) is None
        assert account.check_liquidations(T + 1) == []

    def test_i7_is_vacuous_but_not_violated_when_unreachable(self) -> None:
        solution = liquidation_price(
            qty=Decimal(1),
            entry_price=Decimal(50000),
            margin=Decimal(60000),
            mark_price=Decimal(50000),
            table=single_bracket_table(),
        )
        assert not solution.reachable
        # Must not raise: the ordering check skips a non-positive liquidation price.
        from perplab.core.invariants import check_liquidation_ordering

        check_liquidation_ordering(
            Decimal(1),
            Decimal(50000),
            solution.price,
            bankruptcy_price(Decimal(1), Decimal(50000), Decimal(60000)),
        )


class TestTriggerBoundary:
    """Spec 3.7 states the trigger as `Pm <= P_liq` / `Pm >= P_liq`. Inclusive.

    No test placed the mark exactly on the solved price, so both inequalities could be
    tightened to strict with the whole suite green. The boundary is reachable in practice:
    the fixture below solves to exactly 40 000.00, which is a multiple of the 0.10 tick and
    therefore a price the exchange can actually print.
    """

    def _at_forty_thousand(self, side: Decimal) -> Account:
        """`MMR = 0.5` (a real top-tier rate), 2x, plus added margin -> `P_liq` lands flat.

        long:  (30 000 - 50 000) / (0.5 - 1)  = -20 000 / -0.5 = 40 000
        short: (30 000 + 50 000) / (0.5 + 1)  =  80 000 / 1.5   = 53 333.33...
        """
        account = Account(
            opening_balance=Decimal("60000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table(mmr=Decimal("0.5"), max_leverage=2)},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 2)
        account.apply_fill(T, SYMBOL, side, Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        account.add_margin(T + 1, SYMBOL, Decimal("5000"))
        return account

    def test_a_long_liquidates_at_exactly_the_liquidation_price(self) -> None:
        account = self._at_forty_thousand(Decimal("1"))
        assert account.liquidation_price(SYMBOL) == Decimal("40000")

        account.update_mark(T + 2, SYMBOL, Decimal("40000.0"))
        assert len(account.check_liquidations(T + 2)) == 1

    def test_a_long_survives_one_tick_above(self) -> None:
        account = self._at_forty_thousand(Decimal("1"))
        account.update_mark(T + 2, SYMBOL, Decimal("40000.1"))
        assert account.check_liquidations(T + 2) == []

    def test_a_short_liquidates_at_exactly_its_liquidation_price(self) -> None:
        account = self._at_forty_thousand(Decimal("-1"))
        p_liq = account.liquidation_price(SYMBOL)
        assert p_liq is not None

        account.update_mark(T + 2, SYMBOL, p_liq)
        assert len(account.check_liquidations(T + 2)) == 1


class TestMultiTierResolutionThroughTheLedger:
    """The bracket fixed point was only ever reached by calling the free function.

    `graduated_bracket_table` never went through `Account` at all, so nothing proved the
    ledger passes the right `margin` into a solve that changes tier — which is the whole
    point of spec 3.6's iteration, and the one place a wrong allocation would be invisible
    (it produces a plausible price under a plausible bracket).
    """

    def test_the_account_resolves_the_tier_the_free_function_does(self) -> None:
        account = Account(
            opening_balance=Decimal("20000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: graduated_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("-1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))

        solution = account.liquidation_solution(SYMBOL)
        assert solution is not None
        assert cents(solution.price) == Decimal("54776.12")
        assert solution.bracket.bracket == 2
        assert solution.iterations == 2  # the fixed point actually had to iterate

    def test_it_triggers_at_the_re_resolved_price_not_the_first_pass_one(self) -> None:
        """54 776.12, not 54 780.88. The difference is 4.76 USDT and it is the whole point.

        A mark between the two prices liquidates under the correct multi-tier solve and
        survives under a single-pass one. That is a 4.76-wide window in which an
        implementation that skipped the iteration reports the position as alive.
        """
        account = Account(
            opening_balance=Decimal("20000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: graduated_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("-1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("54778.0"))

        assert len(account.check_liquidations(T + 1)) == 1


class TestBracketMisresolutionIsCaught:
    def test_wrong_mmr_trips_i7(self) -> None:
        """The diagnostic spec 3.10 promises: a bad bracket shows up as an I7 failure.

        A maintenance rate of 0.5 on a 10x position puts `P_liq` above the entry price --
        the position would be liquidated the instant it opened. That is arithmetically
        consistent and completely wrong, and I7 is the only thing between it and a
        backtest full of instant liquidations nobody can explain.
        """
        from perplab.core.invariants import check_liquidation_ordering

        table = single_bracket_table(mmr=Decimal("0.5"))
        solution = liquidation_price(
            qty=Decimal(1),
            entry_price=Decimal(50000),
            margin=Decimal(5000),
            mark_price=Decimal(50000),
            table=table,
        )
        with pytest.raises(InvariantViolation, match="I7"):
            check_liquidation_ordering(
                Decimal(1),
                Decimal(50000),
                solution.price,
                bankruptcy_price(Decimal(1), Decimal(50000), Decimal(5000)),
            )

    def test_spec_mmr_is_the_one_used_by_the_worked_examples(self) -> None:
        assert single_bracket_table().resolve(Decimal(50000)).mmr == SPEC_MMR

    def test_a_truncated_maintenance_amount_trips_i7_through_the_ledger(self) -> None:
        """The production shape spec 3.10 names, driven through `Account` rather than by hand.

        The I7 call inside `_solve_liquidation` could be deleted with all 887 tests green:
        every existing I7 test called the free function directly, so nothing proved the
        *ledger's* solve was guarded at all.

        A snapshot whose `cum` column is wrong is the realistic way this happens. With
        `MA = 5 000` on a tier whose charge is only 200, the maintenance requirement clamps
        to zero and the solve returns 40 160.64 — *below* the 45 000 bankruptcy price, so
        the position would supposedly be liquidated after it was already bankrupt. Spec
        3.10 says exactly what that means: "if this invariant ever fails, the bracket
        resolution is wrong."
        """
        broken = BracketTable(
            symbol=SYMBOL,
            brackets=(
                LeverageBracket(
                    bracket=1,
                    max_leverage=125,
                    notional_floor=Decimal(0),
                    notional_cap=Decimal(10) ** 12,
                    mmr=Decimal("0.004"),
                    maintenance_amount=Decimal(5000),  # truncated/garbled `cum`
                ),
            ),
        )
        account = Account(
            opening_balance=Decimal("20000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: broken},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))

        with pytest.raises(InvariantViolation, match="bracket resolution is wrong"):
            account.liquidation_price(SYMBOL)
