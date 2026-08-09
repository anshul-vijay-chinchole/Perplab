"""A hand-derived worked example for **hedge mode**: two positions, one symbol.

Spec 3.9's worked example is inherently one-way -- it opens a long, adds to it, reduces it,
and never holds two positions at once -- so none of its numbers exercise the thing hedge mode
changes. Every figure below is therefore derived from the spec's *formulas* rather than
transcribed from its worked example, and the derivation is written out in each docstring so a
failing assertion can be checked against the algebra rather than against the implementation
that produced it.

**The long leg is deliberately identical to spec 3.9's own position** -- 0.1 BTC at 50 000 on
10x -- so its liquidation price must come out at exactly the 45 180.72 the spec publishes.
That is the anchor: it proves the hedge machinery does not perturb a position the spec has
already pinned. Everything else is new.

**Setup.** BTCUSDT, isolated, **hedge mode**, 10x leverage, taker fee 0.05% (0.0005),
`stepSize = 0.001`, `tickSize = 0.10`, `MMR = 0.004`, `MA = 0`, opening wallet 10 000 USDT.
The same constants spec 3.9 uses, so the two examples are directly comparable.

The sequence:

| t | event |
|---|---|
| t0 | open LONG 0.1 @ 50 000 |
| t1 | open SHORT 0.05 @ 52 000 -- **the same symbol, not a reduce** |
| t2 | mark 51 000: both legs in profit at once |
| t3 | funding +0.0001: the long pays, the short receives |
| t4 | close the SHORT at 50 000; the LONG is untouched |
| t5 | mark 50 000, final |
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from perplab.core.account import Account, FeeSchedule, HedgeFlipRefused
from perplab.core.invariants import InvariantViolation, check_position_sum
from perplab.core.margin import bankruptcy_price
from perplab.core.risk import (
    RiskEngine,
    RiskLimits,
    WorkingExposure,
    gross_projected_exposure,
    projected_exposure,
)
from perplab.core.types import PositionSide
from tests.support import btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
TAKER = Decimal("0.0005")
LONG = PositionSide.LONG
SHORT = PositionSide.SHORT

T0, T1, T2, T3, T4, T5 = (1_700_000_000_000 + n * 60_000 for n in range(6))


def _account(*, hedge: bool = True) -> Account:
    account = Account(
        opening_balance=Decimal("10000.00"),
        fees=FeeSchedule.all_taker(TAKER, source="hedge-golden"),
        brackets={SYMBOL: single_bracket_table()},
        filters={SYMBOL: btcusdt_filters()},
        hedge_mode=hedge,
    )
    account.set_leverage(SYMBOL, 10)
    return account


@pytest.fixture()
def account() -> Account:
    return _account()


def _open_both(account: Account) -> None:
    """t0 and t1: the long and the short, in that order."""
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"), position_side=LONG)
    account.apply_fill(T1, SYMBOL, Decimal("-0.05"), Decimal("52000.00"), position_side=SHORT)


# ------------------------------------------------------------------------------- t0, t1


def test_t0_the_long_leg_reproduces_spec_3_9_exactly(account: Account) -> None:
    """The anchor. Identical inputs to spec 3.9's t0, so identical outputs.

    ```
    notional = 0.1 x 50 000   = 5 000.00
    IM_long  = 5 000 / 10     =   500.00
    fee      = 5 000 x 0.0005 =     2.50
    W        = 10 000 - 2.50  = 9 997.50
    P_liq    = (500 - 0.1x50 000 + 0) / (0.1x0.004 - 0.1)
             = -4 500 / -0.0996 = 45 180.72
    P_bank   = 50 000 - 500/0.1 = 45 000.00
    ```

    If this figure ever moves, hedge mode has changed the arithmetic of a position the spec
    already fixed -- which is a defect regardless of what the hedged cases do.
    """
    result = account.apply_fill(
        T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"), position_side=LONG
    )

    position = account.position(SYMBOL, LONG)
    assert position is not None
    assert position.position_side is LONG
    assert position.entry_notional == Decimal("5000.00")
    assert position.isolated_margin == Decimal("500.00")
    assert result.fee == Decimal("2.50")
    assert result.position_side is LONG
    assert account.wallet == Decimal("9997.50")

    account.update_mark(T0, SYMBOL, Decimal("50000.00"))
    p_liq = account.liquidation_price(SYMBOL, LONG)
    assert p_liq is not None
    assert p_liq.quantize(Decimal("0.01")) == Decimal("45180.72")
    assert bankruptcy_price(
        position.qty, position.entry_price, position.isolated_margin
    ) == Decimal("45000.00")


def test_t1_the_short_is_a_second_position_not_a_reduce(account: Account) -> None:
    """The whole point of hedge mode, in one assertion.

    ```
    notional  = 0.05 x 52 000   = 2 600.00
    IM_short  = 2 600 / 10      =   260.00
    fee       = 2 600 x 0.0005  =     1.30
    W         = 9 997.50 - 1.30 = 9 996.20
    realized  = 0.00              <- nothing was closed
    ```

    In one-way mode this fill is spec 3.3's **case B**: a sell of 0.05 against a long of 0.1
    reduces it, realises `+1 x 0.05 x (52 000 - 50 000) = +100.00`, and leaves one position
    of 0.05 carrying 250.00 of margin. Here it opens a second position, realises nothing, and
    the account carries 760.00 across two allocations. The contrast is asserted directly in
    `test_the_same_two_fills_mean_different_things_in_each_mode`.
    """
    _open_both(account)

    short = account.position(SYMBOL, SHORT)
    long = account.position(SYMBOL, LONG)
    assert short is not None and long is not None

    assert short.qty == Decimal("-0.05")
    assert short.entry_price == Decimal("52000.00")
    assert short.isolated_margin == Decimal("260.00")

    # The long is bit-for-bit what it was before the short existed.
    assert long.qty == Decimal("0.1")
    assert long.entry_price == Decimal("50000.00")
    assert long.isolated_margin == Decimal("500.00")

    assert account.total_realized == Decimal("0")
    assert account.wallet == Decimal("9996.20")
    assert account.allocated_margin == Decimal("760.00")
    assert account.available_balance == Decimal("9236.20")


def test_the_same_two_fills_mean_different_things_in_each_mode() -> None:
    """Side by side: the identical fills, one-way against hedge.

    | | one-way | hedge |
    |---|---|---|
    | realised | +100.00 | 0.00 |
    | wallet | 10 096.20 | 9 996.20 |
    | positions | 1 | 2 |
    | allocated margin | 250.00 | 760.00 |

    A build that quietly netted the two legs would produce the left column while claiming to
    be in the right mode, and every figure in it is individually plausible.
    """
    one_way = _account(hedge=False)
    one_way.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"))
    one_way.apply_fill(T1, SYMBOL, Decimal("-0.05"), Decimal("52000.00"))

    hedged = _account()
    _open_both(hedged)

    assert one_way.total_realized == Decimal("100.00")
    assert one_way.wallet == Decimal("10096.20")
    assert len(one_way.positions) == 1
    assert one_way.allocated_margin == Decimal("250.00")

    assert hedged.total_realized == Decimal("0")
    assert hedged.wallet == Decimal("9996.20")
    assert len(hedged.positions) == 2
    assert hedged.allocated_margin == Decimal("760.00")


def test_two_liquidation_prices_one_symbol(account: Account) -> None:
    """One symbol, two solves, straddling the mark.

    ```
    LONG :  P_liq = (500 - 0.1x50 000) / (0.1x0.004 - 0.1)
                  = -4 500 / -0.0996        = 45 180.72
            P_bank = 50 000 - 500/0.1       = 45 000.00
    SHORT:  P_liq = (260 + 0.05x52 000) / (0.05x0.004 + 0.05)
                  = 2 860 / 0.0502          = 56 972.11
            P_bank = 52 000 - 260/(-0.05)   = 57 200.00
    ```

    Spec 3.10's I7 holds on each leg separately -- `P_bank < P_liq < Pe` for the long and
    `Pe < P_liq < P_bank` for the short -- which is what makes "solve per side" more than a
    bookkeeping choice: there is no single price at which this symbol liquidates, and any
    build that reports one is reporting a number that does not exist.
    """
    _open_both(account)
    account.update_mark(T2, SYMBOL, Decimal("51000.00"))

    long_liq = account.liquidation_price(SYMBOL, LONG)
    short_liq = account.liquidation_price(SYMBOL, SHORT)
    assert long_liq is not None and short_liq is not None
    assert long_liq.quantize(Decimal("0.01")) == Decimal("45180.72")
    assert short_liq.quantize(Decimal("0.01")) == Decimal("56972.11")

    # I7, per leg, against the bankruptcy prices derived above.
    assert long_liq < Decimal("50000.00")
    assert bankruptcy_price(Decimal("0.1"), Decimal("50000"), Decimal("500")) < long_liq
    assert short_liq > Decimal("52000.00")
    assert short_liq < bankruptcy_price(Decimal("-0.05"), Decimal("52000"), Decimal("260"))

    # And they straddle the mark: one below, one above. A netted position cannot.
    assert long_liq < Decimal("51000.00") < short_liq


# ----------------------------------------------------------------------------------- t2


def test_t2_both_legs_are_in_profit_at_the_same_mark(account: Account) -> None:
    """```
    uPnL_long  = +0.10 x (51 000 - 50 000) = +100.00
    uPnL_short = -0.05 x (51 000 - 52 000) =  +50.00
    uPnL       =                             +150.00
    E          = 9 996.20 + 150.00         = 10 146.20
    ```

    Both legs profitable at once is the state netting cannot express: the net position is
    +0.05, and there is no single entry price at which +0.05 is worth +150 at a mark of
    51 000. The gain is locked in by the 2 000 spread between the two entries.
    """
    _open_both(account)
    account.update_mark(T2, SYMBOL, Decimal("51000.00"))

    long = account.position(SYMBOL, LONG)
    short = account.position(SYMBOL, SHORT)
    assert long is not None and short is not None
    assert long.unrealized_pnl(Decimal("51000.00")) == Decimal("100.00")
    assert short.unrealized_pnl(Decimal("51000.00")) == Decimal("50.00")

    assert account.unrealized_pnl == Decimal("150.00")
    assert account.equity == Decimal("10146.20")

    assert account.gross_qty(SYMBOL) == Decimal("0.15")
    assert account.net_qty(SYMBOL) == Decimal("0.05")


# ----------------------------------------------------------------------------------- t3


def test_t3_funding_moves_the_two_legs_in_opposite_directions(account: Account) -> None:
    """The sharpest test in the file.

    ```
    F = +0.0001, mark 51 000, cashflow = -Q x Pm x F

    long :  -(+0.10) x 51 000 x 0.0001 = -0.510   (pays)
    short:  -(-0.05) x 51 000 x 0.0001 = +0.255   (receives)
    total :                              -0.255
    W = 9 996.20 - 0.255 = 9 995.945

    IM_long  = 500 + (-0.510) = 499.490
    IM_short = 260 + (+0.255) = 260.255

    P_liq_long  = (499.490 - 5 000) / (0.0004 - 0.1)
                = -4 500.510 / -0.0996  = 45 185.84   (moved UP  -- closer to the mark)
    P_liq_short = (260.255 + 2 600) / (0.0502)
                = 2 860.255 / 0.0502    = 56 977.19   (moved UP  -- further from the mark)
    ```

    **One settlement, two opposite effects on survivability.** The long's allocation shrinks
    and its liquidation price climbs 5.12 toward the mark; the short's allocation grows and
    its liquidation price climbs 5.08 away from it. A build that settled funding once on the
    net position, or charged both legs the same sign, produces neither number -- and a build
    that charged funding only to the wallet leaves both liquidation prices unmoved, which is
    the defect `Position.funding_paid` exists to prevent (spec 6.2's R5).
    """
    _open_both(account)
    account.update_mark(T2, SYMBOL, Decimal("51000.00"))

    settled = account.apply_funding_by_side(T3, SYMBOL, Decimal("0.0001"))
    assert settled == {LONG: Decimal("-0.51"), SHORT: Decimal("0.255")}
    assert account.total_funding == Decimal("-0.255")
    assert account.wallet == Decimal("9995.945")

    long = account.position(SYMBOL, LONG)
    short = account.position(SYMBOL, SHORT)
    assert long is not None and short is not None
    assert long.isolated_margin == Decimal("499.49")
    assert short.isolated_margin == Decimal("260.255")

    long_liq = account.liquidation_price(SYMBOL, LONG)
    short_liq = account.liquidation_price(SYMBOL, SHORT)
    assert long_liq is not None and short_liq is not None
    assert long_liq.quantize(Decimal("0.01")) == Decimal("45185.84")
    assert short_liq.quantize(Decimal("0.01")) == Decimal("56977.19")

    # The direction of travel, stated as the claim rather than as two numbers: the payer got
    # closer to liquidation and the receiver got further from it.
    assert long_liq > Decimal("45180.72")
    assert short_liq > Decimal("56972.11")


def test_a_perfect_hedge_pays_exactly_zero_net_funding() -> None:
    """Equal and opposite legs net to zero -- to the last digit, with no epsilon.

    The economic claim a carry strategy lives on. Settling one side only, or settling the
    net quantity once, leaks funding on every settlement; over a month of eight-hourly
    payments that is the difference between a market-neutral book that works and one that
    bleeds.
    """
    account = _account()
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"), position_side=LONG)
    account.apply_fill(T1, SYMBOL, Decimal("-0.1"), Decimal("52000.00"), position_side=SHORT)
    account.update_mark(T2, SYMBOL, Decimal("51000.00"))

    before = account.wallet
    settled = account.apply_funding_by_side(T3, SYMBOL, Decimal("0.0001"))

    assert settled[LONG] == -settled[SHORT]
    assert sum(settled.values(), Decimal(0)) == Decimal(0)
    assert account.wallet == before
    assert account.total_funding == Decimal(0)


# ------------------------------------------------------------------------------- t4, t5


def test_t4_closing_the_short_leaves_the_long_untouched(account: Account) -> None:
    """```
    buy 0.05 @ 50 000 on the SHORT side (taker) -- spec 3.3 case B on that leg
    realized = sign(-0.05) x 0.05 x (50 000 - 52 000)
             = (-1) x 0.05 x (-2 000)          = +100.00
    fee      = 0.05 x 50 000 x 0.0005          =   1.25
    W        = 9 995.945 + 100.00 - 1.25       = 10 094.695
    ```

    **Independence, stated as an assertion.** The long's quantity, entry price, funding
    history and liquidation price are all exactly what they were before this fill. A netting
    implementation cannot satisfy this: closing 0.05 of a netted +0.05 position would flatten
    the account entirely.
    """
    _open_both(account)
    account.update_mark(T2, SYMBOL, Decimal("51000.00"))
    account.apply_funding_by_side(T3, SYMBOL, Decimal("0.0001"))

    long_before = account.position(SYMBOL, LONG)
    assert long_before is not None

    result = account.apply_fill(
        T4, SYMBOL, Decimal("0.05"), Decimal("50000.00"), position_side=SHORT
    )

    assert result.realized == Decimal("100.00")
    assert result.fee == Decimal("1.25")
    assert result.closed is True
    assert result.position_side is SHORT
    assert account.wallet == Decimal("10094.695")

    assert account.position(SYMBOL, SHORT) is None
    long_after = account.position(SYMBOL, LONG)
    assert long_after == long_before


def test_t5_the_full_sequence_reconciles(account: Account) -> None:
    """```
    Sigma realized = +100.000
    Sigma fees     =    5.050   (2.50 + 1.30 + 1.25)
    Sigma funding  =   -0.255
    W = 10 000 + 100.000 - 5.050 - 0.255 = 10 094.695  <- matches
    E = W + uPnL = 10 094.695 + 0.00     = 10 094.695
    ```

    `reconcile()` replays the event log independently of the running accumulators, keyed
    **per `(symbol, side)`** -- which is what makes I9 meaningful here. Keyed by symbol, the
    long's `+0.1` and the short's `-0.05, +0.05` would be summed into `+0.1` and compared
    against the long alone, and would pass by coincidence; a mis-booked pair of fills that
    happened to cancel would pass with it.
    """
    _open_both(account)
    account.update_mark(T2, SYMBOL, Decimal("51000.00"))
    account.apply_funding_by_side(T3, SYMBOL, Decimal("0.0001"))
    account.apply_fill(T4, SYMBOL, Decimal("0.05"), Decimal("50000.00"), position_side=SHORT)
    account.update_mark(T5, SYMBOL, Decimal("50000.00"))

    assert account.total_realized == Decimal("100.00")
    assert account.total_fees == Decimal("5.05")
    assert account.total_funding == Decimal("-0.255")
    assert account.wallet == Decimal("10094.695")
    assert account.unrealized_pnl == Decimal("0.00")
    assert account.equity == Decimal("10094.695")

    account.reconcile()


# ------------------------------------------------------------------------- I3 per side


def test_i3_holds_per_side_and_would_fail_per_symbol(account: Account) -> None:
    """The invariant restatement, demonstrated rather than asserted in prose.

    After t0 and t1 the accumulators are:

    ```
    (BTCUSDT, LONG)  : +0.10   position +0.10   <- I3 holds
    (BTCUSDT, SHORT) : -0.05   position -0.05   <- I3 holds
    per symbol       : +0.05   position ...?    <- equals NEITHER leg
    ```

    The per-symbol sum of `+0.05` matches neither `+0.10` nor `-0.05`, so an I3 stated per
    symbol would fire on this perfectly correct account. That is why the restatement is a
    restatement and not a relaxation -- and it is checked here by calling the invariant
    directly with the per-symbol figure and requiring it to raise.
    """
    _open_both(account)

    assert account._signed_fills[(SYMBOL, LONG)] == Decimal("0.1")
    assert account._signed_fills[(SYMBOL, SHORT)] == Decimal("-0.05")

    # Per side: the invariant the ledger actually asserts, and it holds.
    check_position_sum(account.qty(SYMBOL, LONG), account._signed_fills[(SYMBOL, LONG)])
    check_position_sum(account.qty(SYMBOL, SHORT), account._signed_fills[(SYMBOL, SHORT)])

    # Per symbol: what I3 would have been without the restatement, and it is false.
    per_symbol = sum(account._signed_fills.values(), Decimal(0))
    assert per_symbol == Decimal("0.05")
    with pytest.raises(InvariantViolation, match="I3"):
        check_position_sum(account.qty(SYMBOL, LONG), per_symbol)


# --------------------------------------------------------------------- case C refusal


def test_a_sell_beyond_the_long_side_is_refused_not_flipped(account: Account) -> None:
    """Spec 3.3's case C does not exist in hedge mode.

    A sell of 0.3 against a long of 0.1 would, in one-way mode, realise the long in full and
    open a short of 0.2 at the fill price. Here it names the LONG side, and the long side
    cannot go negative -- so the order is refused, exactly as Binance refuses a `SELL` with
    `positionSide=LONG` that exceeds the position.

    Refused rather than truncated to 0.1, because "sell 0.3" might have meant "close it" or
    "close it and go short 0.2", and silently filling the smaller reading reports a strategy
    doing something it did not ask for.
    """
    _open_both(account)
    wallet_before = account.wallet

    with pytest.raises(HedgeFlipRefused, match="through zero"):
        account.apply_fill(
            T2, SYMBOL, Decimal("-0.3"), Decimal("51000.00"), position_side=LONG
        )

    # Nothing moved. The refusal is a rejection, not a partial application.
    assert account.wallet == wallet_before
    assert account.qty(SYMBOL, LONG) == Decimal("0.1")
    assert account.qty(SYMBOL, SHORT) == Decimal("-0.05")


def test_a_side_cannot_be_opened_in_the_wrong_direction(account: Account) -> None:
    """A sell with nothing on the LONG side is a misrouted order, not a short."""
    with pytest.raises(HedgeFlipRefused, match="cannot open the LONG side"):
        account.apply_fill(
            T0, SYMBOL, Decimal("-0.1"), Decimal("50000.00"), position_side=LONG
        )


def test_the_ledger_refuses_to_guess_which_position_is_meant(account: Account) -> None:
    """`account.position(symbol)` has two answers in hedge mode, so it has none."""
    _open_both(account)
    with pytest.raises(ValueError, match="names neither of them"):
        account.position(SYMBOL)
    with pytest.raises(ValueError, match="hedge mode"):
        account.apply_fill(T2, SYMBOL, Decimal("0.01"), Decimal("51000.00"))


def test_a_one_way_account_refuses_a_hedged_side() -> None:
    """The mirror. A `LONG` fill on a one-way ledger is a mode error, not a long."""
    account = _account(hedge=False)
    with pytest.raises(ValueError, match="one-way mode"):
        account.apply_fill(
            T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"), position_side=LONG
        )


# ------------------------------------------------------------------- leverage is shared


def test_the_second_leg_opens_at_the_first_legs_leverage() -> None:
    """Leverage is per **symbol** at the exchange, so the two legs cannot differ.

    `POST /fapi/v1/leverage` takes a symbol and no `positionSide`. If the short leg opened at
    `DEFAULT_LEVERAGE` because its own slot was empty, its initial margin would be ten times
    what the account posts and its liquidation price would be solved against a number the
    venue is not using -- on the leg that opened *later*, which is the one nobody re-checks.

    ```
    IM_short at 10x = 2 600 / 10 =   260.00     <- correct
    IM_short at  1x = 2 600 / 1  = 2 600.00     <- what a per-side default would post
    ```
    """
    account = _account()
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"), position_side=LONG)
    account.apply_fill(T1, SYMBOL, Decimal("-0.05"), Decimal("52000.00"), position_side=SHORT)

    short = account.position(SYMBOL, SHORT)
    assert short is not None
    assert short.leverage == 10
    assert short.isolated_margin == Decimal("260.00")
    assert account.leverage(SYMBOL) == 10


def test_leverage_cannot_change_while_either_leg_is_open() -> None:
    """Refused on the *other* side too, because the change would re-price both."""
    account = _account()
    account.apply_fill(T0, SYMBOL, Decimal("-0.05"), Decimal("52000.00"), position_side=SHORT)
    with pytest.raises(ValueError, match="cannot change leverage"):
        account.set_leverage(SYMBOL, 5)


# ------------------------------------------------------------------- exposure, summed


def test_exposure_is_the_sum_of_both_sides_not_the_net() -> None:
    """The agreed `max_position_notional` rule, with the number netting would have given.

    ```
    long 0.10 + short 0.05
      gross = 0.10 + 0.05 = 0.15   x 51 000 = 7 650.00   <- the rule
      net   = 0.10 - 0.05 = 0.05   x 51 000 = 2 550.00   <- the trap
    ```

    Under a 5 000 ceiling the gross reading refuses and the netted one passes. Netting is the
    dangerous reading because a market-neutral book stops being neutral the moment one leg is
    liquidated, and the survivor is a naked position the limit never saw.
    """
    long_side = (LONG, Decimal("0.1"), WorkingExposure.zero())
    short_side = (SHORT, Decimal("-0.05"), WorkingExposure.zero())

    assert gross_projected_exposure([long_side, short_side]) == Decimal("0.15")

    # What a netted implementation would have produced, for contrast.
    netted = projected_exposure(Decimal("0.05"), WorkingExposure.zero())
    assert netted == Decimal("0.05")


def test_a_hedge_side_bound_ignores_orders_that_can_only_shrink_it() -> None:
    """A working sell on the LONG side cannot make the long a short, so it adds nothing.

    One-way's bound has to consider both directions because the position can cross zero. A
    hedge side cannot, so `max(|q + buys|, |q - sells|)` would report a long of 0.1 with a
    resting sell of 0.5 as an exposure of 0.4 -- a *short* of 0.4 that hedge mode makes
    unreachable -- and a size limit would refuse the exit the strategy needs most.
    """
    from perplab.core.risk import side_projected_exposure

    working = WorkingExposure(buy_qty=Decimal("0.02"), sell_qty=Decimal("0.5"))
    assert side_projected_exposure(LONG, Decimal("0.1"), working) == Decimal("0.12")
    # One-way, same numbers: the sell-heavy path dominates and the answer is different.
    assert projected_exposure(Decimal("0.1"), working) == Decimal("0.4")


def test_the_risk_layer_refuses_on_the_summed_exposure() -> None:
    """End to end through `RiskEngine.check_order`, at the notional derived above.

    An order growing the short is measured against a ceiling the long is already consuming.
    Without `other_side_exposure` a hedged account would get twice the ceiling a one-way
    account gets under the same configured number.
    """
    engine = RiskEngine(
        limits=RiskLimits.unlimited().__class__(
            max_position_notional=Decimal("5000"),
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            max_consecutive_losses=None,
            halt_on_liquidation=False,
            min_equity_pct=None,
            max_consecutive_rejections=None,
            max_disconnect_seconds=None,
        ),
        starting_equity=Decimal("10000"),
    )

    breach = engine.check_order(
        ts_ms=T2,
        symbol=SYMBOL,
        side="SELL",
        qty=Decimal("0.05"),
        price=Decimal("51000"),
        reduce_only=False,
        position_qty=Decimal("0"),
        working=WorkingExposure.zero(),
        equity=Decimal("10000"),
        open_orders=0,
        position_side=SHORT,
        # The long leg, already open at 0.1.
        other_side_exposure=Decimal("0.1"),
    )

    assert breach is not None
    assert breach.limit == "max_position_notional"
    # 0.15 x 51 000 = 7 650, against a ceiling of 5 000.
    assert breach.observed.startswith("7650")
    assert "both sides summed" in breach.detail

    # The same order with no other leg is 0.05 x 51 000 = 2 550 and passes.
    assert (
        engine.check_order(
            ts_ms=T2,
            symbol=SYMBOL,
            side="SELL",
            qty=Decimal("0.05"),
            price=Decimal("51000"),
            reduce_only=False,
            position_qty=Decimal("0"),
            working=WorkingExposure.zero(),
            equity=Decimal("10000"),
            open_orders=0,
            position_side=SHORT,
            other_side_exposure=Decimal("0"),
        )
        is None
    )


# --------------------------------------------------------------- independent liquidation


def test_one_leg_can_liquidate_while_the_other_survives() -> None:
    """Two allocations, two triggers, and only one of them fires.

    The long is opened at 50 000 on 10x, so it liquidates at 45 180.72. The short is opened
    at 52 000 with a much larger allocation relative to its size, so at a mark of 45 000 it
    is deeply *profitable* and nowhere near its own trigger of 56 972.11.

    A netted account has one position and one trigger, so it either liquidates everything or
    nothing. Here the mark crossing 45 180.72 destroys the long's 500.00 allocation and
    leaves the short open and trading -- which is the real behaviour of a hedged account and
    the reason `check_liquidations` iterates per side.
    """
    account = _account()
    _open_both(account)
    account.update_mark(T2, SYMBOL, Decimal("51000.00"))

    results = account.check_liquidations(T3)
    assert results == []

    account.update_mark(T3, SYMBOL, Decimal("45000.00"))
    results = account.check_liquidations(T4)

    assert len(results) == 1
    assert results[0].position_side is LONG
    assert results[0].symbol == SYMBOL
    assert results[0].margin_lost == Decimal("500.00")

    assert account.position(SYMBOL, LONG) is None
    short = account.position(SYMBOL, SHORT)
    assert short is not None
    assert short.qty == Decimal("-0.05")
    assert short.unrealized_pnl(Decimal("45000.00")) == Decimal("350.00")

    account.reconcile()


# ------------------------------------------------------- closing mutation survivors


def test_both_legs_can_liquidate_on_the_same_mark() -> None:
    """`check_liquidations` must return **every** triggered position, not the first.

    Written because a mutation that added `break` after the first liquidation survived the
    rest of this file: the earlier scenario has a profitable short when the long goes, so
    only one leg ever triggers and a first-match loop passes.

    The construction puts both triggers inside one price band:

    ```
    LONG  1 @ 50 000, 10x:  W = 5 000
      P_liq = (5 000 - 50 000) / (0.004 - 1)  = 45 180.72   triggers at Pm <= this
    SHORT 1 @ 40 000, 10x:  W = 4 000
      P_liq = (4 000 + 40 000) / (0.004 + 1)  = 43 824.70   triggers at Pm >= this
    ```

    Any mark in `[43 824.70, 45 180.72]` crosses both. A break after the first would leave
    the second position **open in the ledger after being destroyed at the exchange**, which
    is the worst available outcome: the account reports exposure it does not have and margin
    it has already lost.

    **This pair has no safe mark at all**, and that is worth naming rather than engineering
    around. The short's trigger sits *below* the long's, so below 43 824.70 the long is gone,
    above 45 180.72 the short is gone, and in between both are. Netting would report this
    position as flat -- long 1, short 1, net zero, "no risk" -- while it is in fact an account
    that cannot survive any price whatsoever. It is the sharpest possible illustration of why
    `gross_qty` is the exposure rule.
    """
    account = _account()
    account.apply_fill(T0, SYMBOL, Decimal("1"), Decimal("50000.00"), position_side=LONG)
    account.apply_fill(T1, SYMBOL, Decimal("-1"), Decimal("40000.00"), position_side=SHORT)
    # Netted, this is flat. It is not flat.
    assert account.net_qty(SYMBOL) == Decimal("0")
    assert account.gross_qty(SYMBOL) == Decimal("2")

    account.update_mark(T2, SYMBOL, Decimal("44000.00"))
    long_liq = account.liquidation_price(SYMBOL, LONG)
    short_liq = account.liquidation_price(SYMBOL, SHORT)
    assert long_liq is not None and short_liq is not None
    assert short_liq.quantize(Decimal("0.01")) == Decimal("43824.70")
    assert long_liq.quantize(Decimal("0.01")) == Decimal("45180.72")
    assert short_liq < Decimal("44000") < long_liq

    results = account.check_liquidations(T3)

    assert len(results) == 2, "both legs crossed; both must be reported"
    assert {r.position_side for r in results} == {LONG, SHORT}
    assert account.positions_for(SYMBOL) == ()


def test_the_second_leg_takes_the_symbols_open_leverage_not_the_stored_default() -> None:
    """`_leverage_for` reads the *other side's* leverage, and this is what proves it does.

    Written because a mutation removing that lookup survived: in the ordinary sequence
    `leverages[symbol]` and the open leg's leverage are equal by construction, so falling
    back to the stored default gives the same answer and nothing distinguishes them.

    Here they are forced apart. `set_leverage` refuses to move while a position is open --
    that is the guard that keeps them equal -- so the drift is written directly, which is
    exactly the state the guard exists to prevent and therefore exactly the state worth
    asserting the fallback would get wrong.

    ```
    IM_short at 10x (the open leg's) = 2 600 / 10 =   260.00
    IM_short at  1x (the stale default) = 2 600   = 2 600.00
    ```
    """
    account = _account()
    account.apply_fill(T0, SYMBOL, Decimal("0.1"), Decimal("50000.00"), position_side=LONG)

    # The stored default now disagrees with the open position. Nothing in the public API can
    # produce this -- `set_leverage` refuses while a position is open -- which is why it is
    # written rather than driven.
    account.leverages[SYMBOL] = 1

    account.apply_fill(T1, SYMBOL, Decimal("-0.05"), Decimal("52000.00"), position_side=SHORT)
    short = account.position(SYMBOL, SHORT)
    assert short is not None
    assert short.leverage == 10, "the second leg must match the symbol's open leverage"
    assert short.isolated_margin == Decimal("260.00")
    assert account.leverage(SYMBOL) == 10
