"""Funding golden cases (spec 3.5, 12.2).

Spec 12.2 asks specifically for "funding at exactly the settlement millisecond", and that
is the case worth having: funding is a discrete event, the settlement instant is a single
millisecond, and every off-by-one around it is worth real money to a strategy that trades
the settlement.
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
from perplab.core.invariants import InvariantViolation
from perplab.core.funding import (
    FundingSchedule,
    FundingSettlement,
    funding_cashflow,
    interval_segments,
)
from tests.support import btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
SETTLE_MS = 1_700_000_000_000
HOUR_MS = 3_600_000


@pytest.fixture()
def account() -> Account:
    account = Account(
        opening_balance=Decimal("20000"),
        fees=FeeSchedule.all_taker(Decimal(0), source="zero-fee-fixture"),
        brackets={SYMBOL: single_bracket_table()},
        filters={SYMBOL: btcusdt_filters()},
    )
    account.set_leverage(SYMBOL, 10)
    return account


class TestSignConvention:
    """Spec 3.5's three sign checks. Getting these backwards inverts the whole edge."""

    def test_long_pays_a_positive_rate(self) -> None:
        """long 1 BTC, mark 50 000, `F = +0.0001` -> `-5.00`"""
        assert funding_cashflow(Decimal(1), Decimal(50000), Decimal("0.0001")) == Decimal("-5.00")

    def test_short_receives_a_positive_rate(self) -> None:
        """short 1 BTC (`Q = -1`), same -> `+5.00`"""
        assert funding_cashflow(Decimal(-1), Decimal(50000), Decimal("0.0001")) == Decimal("5.00")

    def test_long_receives_a_negative_rate(self) -> None:
        """long, `F = -0.0001` -> `+5.00`"""
        assert funding_cashflow(Decimal(1), Decimal(50000), Decimal("-0.0001")) == Decimal("5.00")

    def test_flat_position_settles_nothing(self) -> None:
        assert funding_cashflow(Decimal(0), Decimal(50000), Decimal("0.0001")) == Decimal(0)


class TestSettlementInstant:
    """Spec 3.5 rule 1: discrete, never amortised."""

    def test_a_position_open_at_the_settlement_millisecond_pays_in_full(
        self, account: Account
    ) -> None:
        """Opened on the settlement millisecond itself. Full payment, not a prorated one.

        There is no partial credit for having held the position for zero milliseconds. Any
        implementation that scales the payment by time held is amortising, which spec 3.5
        rules out in its first sentence about funding.
        """
        account.apply_fill(SETTLE_MS, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(SETTLE_MS, SYMBOL, Decimal("50000.0"))
        cashflow = account.apply_funding(SETTLE_MS, SYMBOL, Decimal("0.0001"))

        assert cashflow == Decimal("-5.00")
        assert account.wallet == Decimal("19995.00")

    def test_a_position_closed_one_millisecond_earlier_pays_nothing(
        self, account: Account
    ) -> None:
        """Spec 3.5: "A position closed one second before pays nothing."

        Asserted at one *millisecond* rather than one second, because that is the
        resolution the engine actually runs at and the interesting failure lives in the
        last millisecond, not the last second.
        """
        account.apply_fill(SETTLE_MS - 100, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(SETTLE_MS - 100, SYMBOL, Decimal("50000.0"))
        account.apply_fill(SETTLE_MS - 1, SYMBOL, Decimal("-1"), Decimal("50000.0"))

        cashflow = account.apply_funding(SETTLE_MS, SYMBOL, Decimal("0.0001"))

        assert cashflow == Decimal(0)
        assert account.total_funding == Decimal(0)
        assert not [e for e in account.events if e.kind is AccountEventKind.FUNDING]

    def test_settlement_uses_the_mark_at_the_instant_not_the_last_sample(
        self, account: Account
    ) -> None:
        """The settlement mark is an input, not something to be inferred.

        Spec 3.5 says funding is charged on `Pm(t)` -- the mark *at* the settlement
        instant. Carrying the last sample forward is correct between samples (spec 3.4's
        LOCF rule) but wrong here when the settlement mark is known, and the difference is
        the entire margin of a funding-capture strategy.
        """
        account.apply_fill(SETTLE_MS - 1000, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(SETTLE_MS - 1000, SYMBOL, Decimal("50000.0"))

        carried = funding_cashflow(Decimal(1), Decimal("50000.0"), Decimal("0.0001"))
        actual = account.apply_funding(
            SETTLE_MS, SYMBOL, Decimal("0.0001"), Decimal("51000.0")
        )

        assert carried == Decimal("-5.00")
        assert actual == Decimal("-5.10")

def _near_liquidation() -> Account:
    """1 BTC long @ 50 000 on 10x, marked at 45 300 -- alive, but barely.

    `P_liq` is 45 180.72 at an allocation of 5 000, so 45 300 clears it by about 119 USDT
    of margin. That gap is what the funding payment below has to close.
    """
    account = Account(
        opening_balance=Decimal("6000"),
        fees=FeeSchedule.all_taker(Decimal(0)),
        brackets={SYMBOL: single_bracket_table()},
        filters={SYMBOL: btcusdt_filters()},
    )
    account.set_leverage(SYMBOL, 10)
    account.apply_fill(SETTLE_MS, SYMBOL, Decimal("1"), Decimal("50000.0"))
    account.update_mark(SETTLE_MS + 1, SYMBOL, Decimal("45300.0"))
    return account


class TestFundingBeforeLiquidationCheck:
    """Spec 6.2/R5, the ordering that has to be demonstrable to be worth having.

    R5 was a review finding: "Event ordering did not specify that funding must precede the
    liquidation check. Positions would survive funding payments that should have killed
    them." A test that cannot tell the two orderings apart does not test R5 -- it tests
    that both calls run without raising.
    """

    def test_the_position_survives_at_this_mark_before_funding(self) -> None:
        account = _near_liquidation()
        assert account.check_liquidations(SETTLE_MS + 1) == []
        assert account.position(SYMBOL) is not None

    def test_funding_first_kills_it(self) -> None:
        """```
        cashflow = -1 x 45 300 x 0.003          = -135.90
        margin   = 5 000 - 135.90               = 4 864.10
        P_liq    = (4 864.10 - 50 000) / -0.996 = 45 317.17  >  45 300  -> liquidated
        ```

        The payment does not move the mark and does not move the entry. It moves the
        margin defending the position, which moves `P_liq` up past the mark that was
        already there.
        """
        account = _near_liquidation()
        cashflow = account.apply_funding(SETTLE_MS + 1, SYMBOL, Decimal("0.003"))
        assert cashflow == Decimal("-135.90")

        position = account.position(SYMBOL)
        assert position is not None
        assert position.isolated_margin == Decimal("4864.10")

        results = account.check_liquidations(SETTLE_MS + 1)
        assert len(results) == 1
        assert account.position(SYMBOL) is None
        assert account.total_realized == Decimal("-4864.10")
        assert account.wallet == Decimal("1000.00")
        account.reconcile()

    def test_checking_first_lets_it_survive_the_payment(self) -> None:
        """The bug R5 describes, reproduced deliberately by inverting the order.

        Same account, same rate, same millisecond -- and the position lives, because the
        check consulted a margin figure that the payment was about to invalidate. Spec 6.2
        fixes `kind_priority` 1 (funding) ahead of 2 (liquidation check) for exactly this,
        and the engine, not this class, is what enforces it.
        """
        account = _near_liquidation()
        assert account.check_liquidations(SETTLE_MS + 1) == []
        account.apply_funding(SETTLE_MS + 1, SYMBOL, Decimal("0.003"))

        assert account.position(SYMBOL) is not None  # survived, wrongly
        # ...and the very next check catches it, which is why the damage is bounded to one
        # event rather than to the rest of the run.
        assert len(account.check_liquidations(SETTLE_MS + 2)) == 1

    def test_an_exhausted_allocation_does_not_by_itself_liquidate(self) -> None:
        """A position can outlive its own margin, and spec 3.7 says so.

        The trigger is `W + Q*(Pm - Pe) <= q*Pm*MMR - MA`. That inequality carries
        unrealised PnL, so a long whose allocation has been drained to *negative* by
        funding is still solvent while it is far enough in profit -- its margin balance is
        the profit.

        ```
        open 1 BTC @ 50 000, 10x        -> allocation 5 000
        mark 60 000                     -> uPnL +10 000
        funding F = 0.1 at that mark    -> -6 000, allocation now -1 000
        margin balance = -1 000 + 10 000 = 9 000   vs   MM = 60 000 x 0.004 = 240
        ```

        9 000 > 240, so it lives. An earlier revision short-circuited on "allocation <= 0"
        and liquidated here, destroying a position 10 000 USDT in profit that the exchange
        would never have touched. `P_liq` solves to 51 204.82 -- correctly *above* the
        entry price, which is the formula's own way of saying only a profitable mark keeps
        this alive.
        """
        account = Account(
            opening_balance=Decimal("20000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(SETTLE_MS, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(SETTLE_MS + 1, SYMBOL, Decimal("60000.0"))
        account.apply_funding(SETTLE_MS + 1, SYMBOL, Decimal("0.1"))

        position = account.position(SYMBOL)
        assert position is not None
        assert position.isolated_margin == Decimal("-1000.00")
        assert position.reserved_margin == Decimal(0)

        p_liq = account.liquidation_price(SYMBOL)
        assert p_liq is not None
        assert p_liq.quantize(Decimal("0.01")) == Decimal("51204.82")
        assert p_liq > Decimal("50000")

        assert account.check_liquidations(SETTLE_MS + 1) == []
        account.reconcile()

    def test_an_exhausted_allocation_liquidates_once_the_profit_goes(self) -> None:
        """Same position, marked back down through the solved price.

        At 51 000 the margin balance is `-1 000 + 1 000 = 0`, below the 204 maintenance
        requirement, so it goes. What the liquidation books is spec 6.6's close plus
        confiscation, and here the confiscation finds nothing to take:

        ```
        price leg = 1 x (51 000 - 50 000)  = +1 000
        remaining = -1 000 + 1 000         =      0    -> nothing to confiscate
        realised  = 0 - (-1 000)           = +1 000
        wallet    = 14 000 + 1 000         = 15 000
        ```

        The +1 000 credit is not a bonus for being liquidated. It is the position's
        price PnL, realised by the close the liquidation engine performs, which the
        confiscation cannot reach because the allocation it would have consumed is
        already overdrawn -- that money left the wallet when the funding settled, and
        charging it again would double-count the funding. An earlier revision booked
        the outcome as `-reserved_margin` (zero here) instead: the position was
        deleted, the +1 000 of unrealised profit evaporated with no ledger entry, and
        the round trip reported -6 000 where the true result is `-6 000 funding
        + 1 000 price = -5 000`. This test pinned that wrong wallet (14 000.00) for a
        revision; the figures below are the corrected ones.
        """
        account = Account(
            opening_balance=Decimal("20000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(SETTLE_MS, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(SETTLE_MS + 1, SYMBOL, Decimal("60000.0"))
        account.apply_funding(SETTLE_MS + 1, SYMBOL, Decimal("0.1"))
        account.update_mark(SETTLE_MS + 2, SYMBOL, Decimal("51000.0"))

        results = account.check_liquidations(SETTLE_MS + 2)
        assert len(results) == 1
        # Signed allocation less recovery: -1 000 - 0. Negative means the wallet was
        # credited -- the price leg the confiscation could not reach.
        assert results[0].margin_lost == Decimal("-1000.00")
        assert results[0].event.realized == Decimal("1000.00")
        assert account.total_realized == Decimal("1000.00")
        # I1 by hand: 20 000 opening + 1 000 realised - 0 fees + (-6 000) funding.
        assert account.wallet == Decimal("15000.00")
        account.reconcile()

    def test_funding_larger_than_the_wallet_trips_i5(self) -> None:
        """A payment the account cannot fund is caught, not absorbed.

        Spec 3.10's I5 permits a negative wallet only when a liquidation has been emitted.
        A funding charge that outruns the whole balance with no liquidation on the log
        means the position should have been closed several settlements ago -- a sizing
        failure upstream, surfaced here rather than left to accumulate.
        """
        account = _near_liquidation()
        with pytest.raises(InvariantViolation, match="I5"):
            account.apply_funding(SETTLE_MS + 1, SYMBOL, Decimal("0.2"))

    def test_a_flip_does_not_inherit_the_old_position_s_funding(self) -> None:
        """The residual after a flip is a new position, and starts with a clean allocation.

        Carrying the long's accumulated funding into the short would move the short's
        liquidation price for a reason that stopped existing the moment the long closed.

        Capitalised well above `_near_liquidation`'s 6 000 on purpose. On that fixture the
        flip below is genuinely unfundable — realising −4 700 leaves a wallet of 1 164
        against a residual short needing 2 265 of initial margin — and it only used to
        succeed because `_require_margin` ignored the realised leg. The property this test
        is named for is about funding inheritance, so it should not also be smuggling in an
        account that cannot afford the trade; the refusal itself is pinned separately below.
        """
        account = Account(
            opening_balance=Decimal("20000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(SETTLE_MS, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(SETTLE_MS + 1, SYMBOL, Decimal("45300.0"))
        account.apply_funding(SETTLE_MS + 1, SYMBOL, Decimal("0.003"))
        account.apply_fill(SETTLE_MS + 2, SYMBOL, Decimal("-1.5"), Decimal("45300.0"))

        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("-0.5")
        assert position.funding_paid == Decimal(0)
        assert position.isolated_margin == position.base_margin

    def test_a_losing_flip_the_wallet_cannot_fund_is_refused(self) -> None:
        """The realised leg of a flip is part of what the fill costs (spec 3.3 case C).

        A flip is the only fill that both realises a PnL and needs new margin: an add
        realises nothing, and a reduce releases margin so it can never fail the check. That
        made the missing `realized` term invisible everywhere except here.

        Hand-computed. Wallet after funding is 5 864.10 against a long 1 BTC entered at
        50 000 and marked at 45 300. Selling 1.5 realises 1 x (45 300 - 50 000) = -4 700,
        leaving 1 164.10, and the residual short 0.5 at 45 300 needs 22 650 / 10 = 2 265 of
        initial margin. 2 265 > 1 164.10, so the account cannot fund it.

        Accepted, the account finished with a **negative available balance** and a position
        whose liquidation price sat far further from the mark than the wallet could support.
        Nothing downstream caught it: spec 3.10 states no invariant about available balance,
        and I5 only fires if the wallet itself goes negative — which it does not, because
        the loss is real and affordable. It is the new position that is not.
        """
        account = _near_liquidation()
        account.apply_funding(SETTLE_MS + 1, SYMBOL, Decimal("0.003"))
        assert account.wallet == Decimal("5864.1000")

        with pytest.raises(InsufficientMargin, match="realized"):
            account.apply_fill(SETTLE_MS + 2, SYMBOL, Decimal("-1.5"), Decimal("45300.0"))

        # Refused means unchanged: still the original long, nothing realised, no fee taken.
        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("1")
        assert account.wallet == Decimal("5864.1000")
        assert account.available_balance >= 0

    def test_a_profitable_flip_may_spend_what_it_just_realised(self) -> None:
        """The mirror, and equally load-bearing.

        Subtracting `realized` has to work in both directions. A flip out of a *winning*
        position puts the gain in the wallet before the new side's margin is posted, so a
        fill that looks unaffordable against the pre-fill balance is affordable in fact.
        Only subtracting losses would be a one-sided rule that quietly refuses good trades.

        Long 1 BTC at 50 000 on 10x from a 6 000 wallet: margin 5 000, available 1 000. Mark
        rises to 60 000. Selling 2 realises +10 000 and leaves a short 1 at 60 000 needing
        6 000 of margin — far more than the 1 000 that was available beforehand, and easily
        covered by the 16 000 wallet the realised gain produces.
        """
        account = Account(
            opening_balance=Decimal("6000"),
            fees=FeeSchedule.all_taker(Decimal(0)),
            brackets={SYMBOL: single_bracket_table()},
            filters={SYMBOL: btcusdt_filters()},
        )
        account.set_leverage(SYMBOL, 10)
        account.apply_fill(SETTLE_MS, SYMBOL, Decimal("1"), Decimal("50000.0"))
        account.update_mark(SETTLE_MS + 1, SYMBOL, Decimal("60000.0"))
        assert account.available_balance == Decimal("1000")

        account.apply_fill(SETTLE_MS + 2, SYMBOL, Decimal("-2"), Decimal("60000.0"))

        position = account.position(SYMBOL)
        assert position is not None
        assert position.qty == Decimal("-1")
        assert account.wallet == Decimal("16000.0")
        assert position.isolated_margin == Decimal("6000")
        assert account.available_balance >= 0


class TestSchedule:
    """Settlement times come from the record, never from an assumed cadence (rule 3)."""

    def test_due_window_is_half_open(self) -> None:
        """`[start, end)`. A settlement on the boundary belongs to exactly one window.

        Settlement timestamps are round numbers, so landing exactly on an event-loop step
        boundary is the common case. A closed interval double-charges every one of them.
        """
        times = [SETTLE_MS + n * 8 * HOUR_MS for n in range(3)]
        schedule = FundingSchedule(
            symbol=SYMBOL,
            settlements=tuple(
                FundingSettlement(ts_ms=t, rate=Decimal("0.0001"), mark_price=Decimal(50000))
                for t in times
            ),
        )

        first = schedule.due(times[0], times[1])
        second = schedule.due(times[1], times[2])

        assert [s.ts_ms for s in first] == [times[0]]
        assert [s.ts_ms for s in second] == [times[1]]
        assert len(schedule.due(times[0], times[2] + 1)) == 3

    def test_interval_is_derived_not_assumed(self) -> None:
        """Eight hours, read off a record that happens to use eight hours."""
        times = [SETTLE_MS + n * 8 * HOUR_MS for n in range(10)]
        assert interval_segments(times)[0].interval_ms == 8 * HOUR_MS

    def test_a_mid_history_interval_change_is_reported_as_two_segments(self) -> None:
        """R17: Binance has changed the funding interval on symbols already trading.

        A single hardcoded interval misprices one side of the change, and a single
        *derived* interval does the same thing while looking more principled. Two segments
        is the honest answer, and `interval_ms` returns `None` rather than picking one.
        """
        eight_hourly = [SETTLE_MS + n * 8 * HOUR_MS for n in range(6)]
        start = eight_hourly[-1]
        four_hourly = [start + n * 4 * HOUR_MS for n in range(1, 7)]

        schedule = FundingSchedule(
            symbol=SYMBOL,
            settlements=tuple(
                FundingSettlement(ts_ms=t, rate=Decimal("0.0001"), mark_price=Decimal(50000))
                for t in eight_hourly + four_hourly
            ),
        )
        segments = schedule.segments()

        assert [s.interval_ms for s in segments] == [8 * HOUR_MS, 4 * HOUR_MS]
        assert [s.interval_hours for s in segments] == [Decimal(8), Decimal(4)]
        assert schedule.interval_ms is None

    def test_jitter_does_not_split_a_segment(self) -> None:
        """Settlements land within seconds of the boundary, not on it.

        A tolerance that is too tight turns ordinary jitter into a phantom schedule change;
        one that is too loose swallows the real 8h-to-4h change. A minute is comfortably
        between the two.
        """
        times = [SETTLE_MS + n * 8 * HOUR_MS + (n % 3) * 1200 for n in range(8)]
        segments = interval_segments(times)
        assert len(segments) == 1

    def test_out_of_order_settlements_are_refused(self) -> None:
        with pytest.raises(ValueError, match="timestamp order"):
            FundingSchedule(
                symbol=SYMBOL,
                settlements=(
                    FundingSettlement(SETTLE_MS + 1, Decimal("0.0001"), Decimal(50000)),
                    FundingSettlement(SETTLE_MS, Decimal("0.0001"), Decimal(50000)),
                ),
            )
