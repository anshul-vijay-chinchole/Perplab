"""Account behaviour outside the golden scenarios: refusals, margin moves, multi-symbol.

The golden tests assert the numbers. These assert the edges -- what the ledger refuses to
do, and what it does when asked something the spec's worked examples never ask.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.core.account import (
    DEFAULT_LEVERAGE,
    Account,
    AccountEventKind,
    FeeSchedule,
    InsufficientMargin,
    Position,
)
from perplab.core.invariants import InvariantViolation
from tests.support import btcusdt_filters, graduated_bracket_table, single_bracket_table

SYMBOL = "BTCUSDT"
OTHER = "ETHUSDT"
T = 1_700_000_000_000


def make_account(balance: str = "100000", **kwargs: object) -> Account:
    return Account(
        opening_balance=Decimal(balance),
        fees=FeeSchedule.all_taker(Decimal(0), source="unit"),
        brackets={SYMBOL: single_bracket_table(), OTHER: single_bracket_table(OTHER)},
        filters={SYMBOL: btcusdt_filters()},
        **kwargs,  # type: ignore[arg-type]
    )


class TestFeeSchedule:
    def test_rate_depends_on_liquidity_role(self) -> None:
        schedule = FeeSchedule(maker_rate=Decimal("0.0002"), taker_rate=Decimal("0.0005"))
        assert schedule.rate(is_maker=True) == Decimal("0.0002")
        assert schedule.rate(is_maker=False) == Decimal("0.0005")

    def test_all_taker_is_the_conservative_default(self) -> None:
        """Spec 3.8's recommended starting point until the classifier is validated."""
        schedule = FeeSchedule.all_taker(Decimal("0.0005"))
        assert schedule.rate(is_maker=True) == schedule.rate(is_maker=False)

    def test_negative_rates_are_refused(self) -> None:
        """Maker rebates exist on high VIP tiers and are deliberately not modelled.

        Spec 3.1 says fees are positive and subtracted. A negative rate would flow through
        as a credit that invariant I1 books as a fee, which is arithmetically consistent
        and semantically nonsense.
        """
        with pytest.raises(ValueError, match="negative"):
            FeeSchedule(maker_rate=Decimal("-0.0001"), taker_rate=Decimal("0.0005"))

    def test_rate_of_one_or_more_is_refused(self) -> None:
        with pytest.raises(ValueError, match="fraction of notional"):
            FeeSchedule(maker_rate=Decimal("0.0002"), taker_rate=Decimal(1))

    def test_parses_the_commission_rate_endpoint(self) -> None:
        schedule = FeeSchedule.from_commission_payload(
            {
                "symbol": "BTCUSDT",
                "makerCommissionRate": "0.000200",
                "takerCommissionRate": "0.000400",
            }
        )
        assert schedule.taker_rate == Decimal("0.000400")
        assert schedule.source == "commissionRate:BTCUSDT"


class TestPositionType:
    def test_a_flat_position_cannot_be_constructed(self) -> None:
        """I4 as a type constraint rather than a check that has to be remembered."""
        with pytest.raises(ValueError, match="flat position has no representation"):
            Position(symbol=SYMBOL, qty=Decimal(0), entry_price=Decimal(50000), leverage=1)

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"entry_price": Decimal(0)}, "must be positive"),
            ({"leverage": 0}, "at least 1"),
            ({"extra_margin": Decimal(-1)}, "negative"),
        ],
    )
    def test_malformed_positions_are_refused(self, kwargs: dict, message: str) -> None:
        base = {
            "symbol": SYMBOL,
            "qty": Decimal("0.1"),
            "entry_price": Decimal(50000),
            "leverage": 10,
        }
        with pytest.raises(ValueError, match=message):
            Position(**{**base, **kwargs})

    def test_side_and_notional(self) -> None:
        from perplab.core.types import Side

        long = Position(SYMBOL, Decimal("0.1"), Decimal(50000), 10)
        short = Position(SYMBOL, Decimal("-0.1"), Decimal(50000), 10)
        assert long.side is Side.BUY and short.side is Side.SELL
        assert long.entry_notional == short.entry_notional == Decimal(5000)
        assert long.base_margin == Decimal(500)


class TestLeverage:
    def test_default_is_one_not_twenty(self) -> None:
        """Binance defaults new accounts to 20x. An unset parameter should be the safest
        available value, not the most dangerous one."""
        assert make_account().leverage(SYMBOL) == DEFAULT_LEVERAGE == 1

    def test_cannot_change_while_a_position_is_open(self) -> None:
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))
        with pytest.raises(ValueError, match="while a position is open"):
            account.set_leverage(SYMBOL, 20)

    def test_cannot_exceed_the_highest_published_bracket(self) -> None:
        account = Account(
            opening_balance=Decimal("100000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: graduated_bracket_table()},
        )
        with pytest.raises(ValueError, match="exceeds the highest bracket"):
            account.set_leverage(SYMBOL, 200)

    def test_a_fill_beyond_its_notional_tier_s_leverage_is_refused(self) -> None:
        """125x is available on a small position and not on a large one (spec 3.6).

        The account allows the leverage to be *set* -- the top bracket permits it -- and
        refuses the fill whose notional lands in a tier that does not. That is the order
        Binance enforces it in, and it is the only order that works: at the time leverage
        is set there is no notional to resolve a bracket against.
        """
        account = Account(
            opening_balance=Decimal("100000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: graduated_bracket_table()},
        )
        account.set_leverage(SYMBOL, 125)
        with pytest.raises(InsufficientMargin, match="exceeds the 100x maximum"):
            account.apply_fill(T, SYMBOL, Decimal("2"), Decimal("50000.0"))

    def test_position_keeps_the_leverage_it_opened_at(self) -> None:
        account = make_account()
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))
        account.apply_fill(T + 1, SYMBOL, Decimal("0.1"), Decimal("52000.0"))
        position = account.position(SYMBOL)
        assert position is not None and position.leverage == 10


class TestMarginMoves:
    def test_added_margin_pushes_the_liquidation_price_away(self) -> None:
        account = make_account()
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        before = account.liquidation_price(SYMBOL)

        account.add_margin(T + 1, SYMBOL, Decimal("1000"))
        after = account.liquidation_price(SYMBOL)

        assert before is not None and after is not None
        assert after < before
        assert account.events[-1].kind is AccountEventKind.MARGIN_ADDED

    def test_added_margin_is_carved_out_of_the_available_balance(self) -> None:
        account = make_account("10000")
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        assert account.available_balance == Decimal("5000")

        account.add_margin(T + 1, SYMBOL, Decimal("1000"))
        assert account.available_balance == Decimal("4000")

    def test_cannot_add_more_than_is_available(self) -> None:
        account = make_account("10000")
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        with pytest.raises(InsufficientMargin, match="exceeds available balance"):
            account.add_margin(T + 1, SYMBOL, Decimal("6000"))

    def test_only_added_margin_can_be_removed(self) -> None:
        """The initial requirement is locked while the position is open.

        Releasing it would raise the effective leverage above what the bracket permits,
        silently -- the position would still report the leverage it opened at.
        """
        account = make_account("10000")
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.add_margin(T + 1, SYMBOL, Decimal("1000"))

        account.remove_margin(T + 2, SYMBOL, Decimal("400"))
        position = account.position(SYMBOL)
        assert position is not None and position.extra_margin == Decimal("600")

        with pytest.raises(InsufficientMargin, match="initial requirement is locked"):
            account.remove_margin(T + 3, SYMBOL, Decimal("601"))

    def test_margin_moves_on_a_flat_symbol_are_refused(self) -> None:
        account = make_account()
        with pytest.raises(LookupError, match="no open position"):
            account.add_margin(T, SYMBOL, Decimal("100"))
        with pytest.raises(LookupError, match="no open position"):
            account.remove_margin(T, SYMBOL, Decimal("100"))

    @pytest.mark.parametrize("amount", [Decimal(0), Decimal(-1)])
    def test_non_positive_margin_moves_are_refused(self, amount: Decimal) -> None:
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))
        with pytest.raises(ValueError, match="must be positive"):
            account.add_margin(T + 1, SYMBOL, amount)


class TestRefusals:
    def test_zero_quantity_fill(self) -> None:
        with pytest.raises(ValueError, match="zero quantity"):
            make_account().apply_fill(T, SYMBOL, Decimal(0), Decimal("50000.0"))

    def test_non_positive_fill_price(self) -> None:
        with pytest.raises(ValueError, match="fill price"):
            make_account().apply_fill(T, SYMBOL, Decimal("0.1"), Decimal(0))

    def test_non_positive_mark(self) -> None:
        with pytest.raises(ValueError, match="mark price"):
            make_account().update_mark(T, SYMBOL, Decimal(0))

    def test_funding_without_a_mark_is_refused(self) -> None:
        """A cashflow settled against a guessed mark is real money moved on a guess."""
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))
        with pytest.raises(LookupError, match="no mark price recorded"):
            account.apply_funding(T + 1, SYMBOL, Decimal("0.0001"))

    def test_opening_a_position_with_no_bracket_table_is_refused(self) -> None:
        """The silent failure this closes is the dangerous kind: it looks like a good run.

        `check_liquidations` skips symbols it cannot price, and must -- it runs on every
        event, and raising there would make one missing snapshot fatal to a whole
        multi-symbol run. But a position that is silently never liquidation-checked
        produces an equity curve with no downside bound at all. Refusing when the position
        is *opened* puts the failure where it can be diagnosed.
        """
        account = Account(
            opening_balance=Decimal("100000"), fees=FeeSchedule.all_taker(Decimal(0))
        )
        with pytest.raises(LookupError, match="no leverage bracket table"):
            account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))

    def test_liquidation_price_without_a_bracket_table_is_refused(self) -> None:
        """Spec 3.6 forbids hardcoding the table, so there is no fallback to fall back to."""
        account = Account(
            opening_balance=Decimal("100000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            require_brackets=False,
        )
        account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        with pytest.raises(LookupError, match="no leverage bracket table"):
            account.liquidation_price(SYMBOL)

    def test_liquidation_sweep_skips_symbols_it_cannot_price(self) -> None:
        """Explicitly opted out of margin modelling: the sweep must not raise.

        Skipping and refusing are different answers to different questions. Asking for one
        position's liquidation price with no table is a programming error. Sweeping all
        positions on a run that declared `require_brackets=False` is the mode working as
        intended.
        """
        account = Account(
            opening_balance=Decimal("100000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            require_brackets=False,
        )
        account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        assert account.check_liquidations(T) == []

    def test_events_going_backwards_in_time_are_refused(self) -> None:
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("0.1"), Decimal("50000.0"))
        with pytest.raises(InvariantViolation, match="I8"):
            account.apply_fill(T - 1, SYMBOL, Decimal("0.1"), Decimal("50000.0"))

    def test_negative_opening_balance(self) -> None:
        with pytest.raises(ValueError, match="negative"):
            Account(opening_balance=Decimal(-1), fees=FeeSchedule.all_taker(Decimal(0)))

    @pytest.mark.parametrize("pct", [Decimal("-0.1"), Decimal("1.1")])
    def test_recovery_pct_must_be_a_fraction(self, pct: Decimal) -> None:
        with pytest.raises(ValueError, match="fraction"):
            Account(
                opening_balance=Decimal(1),
                fees=FeeSchedule.all_taker(Decimal(0)),
                liquidation_recovery_pct=pct,
            )


class TestMultiSymbol:
    def test_equity_sums_across_positions(self) -> None:
        account = make_account()
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, OTHER, Decimal("-10"), Decimal("3000.0"))
        account.update_mark(T + 2, SYMBOL, Decimal("51000.0"))
        account.update_mark(T + 2, OTHER, Decimal("2900.0"))

        # +1000 on the long, +1000 on the short.
        assert account.unrealized_pnl == Decimal("2000")
        assert account.equity == Decimal("102000")
        account.reconcile()

    def test_margin_is_allocated_per_position(self) -> None:
        account = make_account("10000")
        account.set_leverage(SYMBOL, 10)
        account.set_leverage(OTHER, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, OTHER, Decimal("10"), Decimal("300.0"))

        assert account.allocated_margin == Decimal("5300")
        assert account.available_balance == Decimal("4700")

    def test_one_liquidation_does_not_touch_the_other_position(self) -> None:
        """The whole reason spec 3.7 makes v1 isolated-only.

        Under cross margin, one bad position can take the account down with it. Under
        isolated, its loss is bounded by its own allocation, and this asserts the boundary
        actually holds rather than being merely intended.
        """
        account = make_account("20000")
        account.set_leverage(SYMBOL, 10)
        account.set_leverage(OTHER, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.apply_fill(T + 1, OTHER, Decimal("10"), Decimal("300.0"))
        account.update_mark(T + 2, SYMBOL, Decimal("40000.0"))
        account.update_mark(T + 2, OTHER, Decimal("300.0"))

        results = account.check_liquidations(T + 2)
        assert [r.symbol for r in results] == [SYMBOL]
        assert account.position(OTHER) is not None
        assert account.wallet == Decimal("15000")
        account.reconcile()


class TestObservability:
    def test_mark_events_are_off_by_default(self) -> None:
        account = make_account()
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        assert account.events == []

    def test_mark_events_can_be_turned_on(self) -> None:
        account = make_account(log_marks=True)
        account.update_mark(T, SYMBOL, Decimal("50000.0"))
        assert [e.kind for e in account.events] == [AccountEventKind.MARK]

    def test_strict_off_disables_the_invariants(self) -> None:
        """For property-test shrinking and profiling. Not for production, ever.

        Asserted so the flag is known to work: a `strict=False` that silently still checked
        would make shrinking unbearably slow, and one that silently never checked when
        `True` would be far worse.
        """
        account = make_account(strict=False)
        account.apply_fill(T, SYMBOL, Decimal("0.13847362"), Decimal("50000.05"))
        account.apply_fill(T - 5000, SYMBOL, Decimal("0.001"), Decimal("50000.0"))
        assert account.qty(SYMBOL) == Decimal("0.13947362")

    def test_attribution_decomposes_exactly(self) -> None:
        account = make_account()
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(T, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(T + 1, SYMBOL, Decimal("51000.0"))
        account.apply_funding(T + 2, SYMBOL, Decimal("0.0001"))

        parts = account.attribution()
        assert parts["unrealized_pnl"] == Decimal("1000")
        assert parts["funding_pnl"] == Decimal("-5.1")
        assert (
            parts["price_pnl"] + parts["funding_pnl"] - parts["fees"] == parts["net_pnl"]
        )
