"""Random fill/funding/mark sequences must leave every spec 3.10 invariant standing.

Spec 12.2 asks for exactly this, and it is the half of the Phase 2 exit criterion the
golden tests cannot cover: golden tests fix the cases somebody thought of, and the fill
application in spec 3.3 has three cases whose *interactions* over a long sequence are where
the bugs actually live -- a flip immediately after a partial reduce, a funding settlement
landing between two increases, a liquidation on a position that has been scaled twice.

The oracle is the invariant set, not an expected value. There is nothing to compare a
random sequence's PnL against; what there is, is a set of statements that must be true of
the resulting state no matter which sequence produced it.

**The shadow tally matters as much as the invariants.** `Account` checks its own
accumulators against its own wallet, which catches an error in one but not a matching pair.
These tests re-derive the totals from the returned `FillResult`s and the event log -- an
independent path -- and compare. An account that mis-books a fill *and* mis-accumulates it
identically passes I1 and fails here.
"""

from __future__ import annotations

from decimal import Decimal
from enum import Enum

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from perplab.core.account import Account, AccountEventKind, FeeSchedule, InsufficientMargin
from perplab.core.invariants import (
    check_entry_price_presence,
    check_equity,
    check_pnl_decomposition,
    check_position_sum,
    check_wallet_conservation,
    check_wallet_non_negative,
)
from tests.support import btcusdt_filters, single_bracket_table

SYMBOL = "BTCUSDT"
TAKER = Decimal("0.0005")
T0 = 1_700_000_000_000


class Op(Enum):
    FILL = "FILL"
    FUNDING = "FUNDING"
    MARK = "MARK"


# `places` fixes the decimal places exactly, so every generated price is a multiple of the
# 0.10 tick and every quantity a multiple of the 0.001 step -- invariant I6 is satisfied by
# construction rather than by filtering, which keeps Hypothesis from spending its budget
# generating orders the account will reject.
PRICES = st.decimals(min_value=Decimal("20000"), max_value=Decimal("80000"), places=1)
QTYS = st.decimals(min_value=Decimal("0.001"), max_value=Decimal("0.400"), places=3)
# Binance caps funding at +/-0.75% on most symbols. Generating beyond that would mostly
# produce I5 failures from payments no account could fund, which is a sizing question
# rather than an accounting one.
RATES = st.decimals(min_value=Decimal("-0.0075"), max_value=Decimal("0.0075"), places=6)

OPS = st.one_of(
    st.tuples(st.just(Op.FILL), QTYS, PRICES, st.booleans()),
    st.tuples(st.just(Op.FUNDING), RATES, PRICES, st.booleans()),
    st.tuples(st.just(Op.MARK), PRICES, PRICES, st.booleans()),
)

# Leverage has to be generated, not fixed. An earlier revision of this suite left every
# account at the 1x default, and 1x makes `initial_margin` (notional / leverage) an exact
# division -- which meant the entire non-terminating-division path, and with it the
# margin-exhaustion branch and the precision behaviour those produce, was never reached by
# any property test. 3 and 7 are deliberate: neither divides a round notional evenly.
LEVERAGES = st.sampled_from([1, 3, 7, 10, 20])


def make_account(leverage: int = 1, **kwargs: object) -> Account:
    account = Account(
        opening_balance=Decimal("1000000"),
        fees=FeeSchedule(maker_rate=Decimal("0.0002"), taker_rate=TAKER, source="property"),
        brackets={SYMBOL: single_bracket_table()},
        filters={SYMBOL: btcusdt_filters()},
        **kwargs,  # type: ignore[arg-type]
    )
    account.set_leverage(SYMBOL, leverage)
    return account


def _drive(account: Account, program: list[tuple[Op, Decimal, Decimal, bool]]) -> None:
    """Apply the generated program, tolerating only the rejections that are by design.

    `InsufficientMargin` is a legitimate outcome -- the account refusing to fund a fill is
    the behaviour under test elsewhere -- so it is skipped rather than swallowed globally.
    Nothing else is caught: an `InvariantViolation` escaping here is the failure this
    module exists to find.
    """
    for index, (op, a, b, flag) in enumerate(program):
        ts = T0 + index

        if op is Op.MARK:
            account.update_mark(ts, SYMBOL, a)
            account.check_liquidations(ts)
            continue

        if op is Op.FUNDING:
            if SYMBOL in account.marks:
                account.apply_funding(ts, SYMBOL, a)
                account.check_liquidations(ts)
            continue

        signed = a if flag else -a
        try:
            account.apply_fill(ts, SYMBOL, signed, b, is_maker=not flag)
        except InsufficientMargin:
            continue


@given(program=st.lists(OPS, min_size=1, max_size=60), leverage=LEVERAGES)
@settings(max_examples=250, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_invariants_hold_over_random_sequences(
    program: list[tuple[Op, Decimal, Decimal, bool]], leverage: int
) -> None:
    """Every spec 3.10 invariant, re-checked from outside the account after the run.

    `Account` already asserts these on every mutation. Re-asserting them here from the
    outside is not redundant: it proves the checks are reachable with the state the
    account actually ends in, and it would catch a `strict` flag that silently stopped
    being consulted.
    """
    account = make_account(leverage)
    _drive(account, program)

    check_wallet_conservation(
        account.wallet,
        account.opening_balance,
        account.total_realized,
        account.total_fees,
        account.total_funding,
    )
    check_wallet_non_negative(account.wallet, account.liquidations > 0)

    position = account.position(SYMBOL)
    qty = position.qty if position else Decimal(0)
    entry = position.entry_price if position else None
    check_entry_price_presence(qty, entry)

    # The oracle has to reach the mark by a *different* route than the account does, or
    # this is an identity. `Account.equity` sums over `_valuation_mark`, which is
    # `marks.get(symbol, entry_price)`; an oracle written the same way compares the account
    # with itself and passes no matter what the account does. Reading `marks` directly and
    # skipping symbols with no sample is the same answer computed differently -- a position
    # valued at its entry price contributes zero either way.
    expected_equity = account.wallet
    for (symbol, _side), position in account.positions.items():
        if symbol in account.marks:
            expected_equity += position.qty * (account.marks[symbol] - position.entry_price)
    assert account.equity == expected_equity

    check_equity(
        account.equity,
        account.wallet,
        [(p.qty, account.marks[key[0]], p.entry_price)
         for key, p in account.positions.items() if key[0] in account.marks],
    )
    check_pnl_decomposition(
        account.equity,
        account.opening_balance,
        account.total_realized,
        account.total_fees,
        account.total_funding,
        account.unrealized_pnl,
    )
    account.reconcile()


@given(program=st.lists(OPS, min_size=1, max_size=60), leverage=LEVERAGES)
@settings(max_examples=250, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_accumulators_match_an_independent_tally_of_the_event_log(
    program: list[tuple[Op, Decimal, Decimal, bool]], leverage: int
) -> None:
    """I1's terms, re-derived from the log rather than from the account's own counters.

    The account's I1 check compares its wallet against its own three accumulators. If a
    fill were booked into the wallet and into the accumulator with the same wrong number,
    I1 would pass. Summing the event log is a second, independent path to the same totals,
    and it does not share that failure mode.
    """
    account = make_account(leverage)
    _drive(account, program)

    realized = sum((e.realized for e in account.events), Decimal(0))
    fees = sum((e.fee for e in account.events), Decimal(0))
    funding = sum((e.funding for e in account.events), Decimal(0))

    assert realized == account.total_realized
    assert fees == account.total_fees
    assert funding == account.total_funding
    assert account.opening_balance + realized - fees + funding == account.wallet


@given(program=st.lists(OPS, min_size=1, max_size=60), leverage=LEVERAGES)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_position_always_equals_the_sum_of_signed_fills(
    program: list[tuple[Op, Decimal, Decimal, bool]], leverage: int
) -> None:
    """I3, with liquidation's synthetic close counted in.

    A liquidation removes the position without a fill, so the naive "sum the FILL events"
    tally diverges from the position the moment one fires. It has to be counted, and it is
    counted as the signed quantity it closed -- which is also how the account does it, so
    the two agreeing is what says the liquidation path did not quietly leak position.
    """
    account = make_account(leverage)
    _drive(account, program)

    signed = Decimal(0)
    for event in account.events:
        if event.kind in (AccountEventKind.FILL, AccountEventKind.LIQUIDATION):
            signed += event.qty

    check_position_sum(account.qty(SYMBOL), signed)


@given(program=st.lists(OPS, min_size=1, max_size=40), leverage=LEVERAGES)
@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_event_timestamps_never_go_backwards(
    program: list[tuple[Op, Decimal, Decimal, bool]], leverage: int
) -> None:
    """I8. Non-decreasing, not strictly increasing -- several events share a millisecond."""
    account = make_account(leverage)
    _drive(account, program)

    stamps = [e.ts_ms for e in account.events]
    assert stamps == sorted(stamps)


@given(
    qty=QTYS,
    price=PRICES,
    is_maker=st.booleans(),
)
@settings(max_examples=200, deadline=None)
def test_fee_is_always_positive_and_subtracted(
    qty: Decimal, price: Decimal, is_maker: bool
) -> None:
    """Spec 3.1: "Fees are always positive numbers, always subtracted from wallet balance."

    Stated as a property because the sign of a fee is the kind of thing that gets inverted
    once, in one branch, and then costs a strategy its entire measured edge in the
    direction that makes it look profitable.
    """
    account = make_account()
    before = account.wallet
    result = account.apply_fill(T0, SYMBOL, qty, price, is_maker=is_maker)

    assert result.fee > 0
    assert account.wallet == before - result.fee
    assert result.fee == qty * price * (Decimal("0.0002") if is_maker else TAKER)


@given(qty=QTYS, entry=PRICES, mark=PRICES)
@settings(max_examples=200, deadline=None)
def test_long_and_short_unrealised_pnl_are_exact_mirrors(
    qty: Decimal, entry: Decimal, mark: Decimal
) -> None:
    """A long and a short of the same size at the same prices must offset exactly.

    `Q*(Pm - Pe)` and `-Q*(Pm - Pe)` sum to zero. Any asymmetry -- an `abs()` in the wrong
    place, a side-dependent branch -- shows up here and nowhere in a single-sided test.
    """
    long_account, short_account = make_account(), make_account()
    long_account.apply_fill(T0, SYMBOL, qty, entry)
    short_account.apply_fill(T0, SYMBOL, -qty, entry)
    long_account.update_mark(T0 + 1, SYMBOL, mark)
    short_account.update_mark(T0 + 1, SYMBOL, mark)

    assert long_account.unrealized_pnl + short_account.unrealized_pnl == Decimal(0)


@given(qty=QTYS, entry=PRICES, exit_price=PRICES)
@settings(max_examples=200, deadline=None)
def test_a_round_trip_realises_the_price_difference_exactly(
    qty: Decimal, entry: Decimal, exit_price: Decimal
) -> None:
    """Open and close the same size: realised PnL is `qty * (exit - entry)`, no residue.

    The strongest single statement about the fill logic that does not depend on which case
    handled it, and it holds for both directions.
    """
    account = make_account(enforce_margin=False)
    account.apply_fill(T0, SYMBOL, qty, entry)
    result = account.apply_fill(T0 + 1, SYMBOL, -qty, exit_price)

    assert result.realized == qty * (exit_price - entry)
    assert account.position(SYMBOL) is None
    assert account.qty(SYMBOL) == Decimal(0)


@given(
    first=QTYS,
    second=QTYS,
    price_a=PRICES,
    price_b=PRICES,
)
@settings(max_examples=200, deadline=None)
def test_entry_price_stays_between_the_two_fill_prices(
    first: Decimal, second: Decimal, price_a: Decimal, price_b: Decimal
) -> None:
    """A weighted average of two prices lies between them. Bounds the Case A arithmetic.

    Weak as a statement and strong as a guard: it catches a transposed numerator and
    denominator, a sum where a product belonged, and the quantisation in
    `money.quantize_entry_price` rounding outside the interval it is averaging over.
    """
    assume(price_a != price_b)
    account = make_account(enforce_margin=False)
    account.apply_fill(T0, SYMBOL, first, price_a)
    account.apply_fill(T0 + 1, SYMBOL, second, price_b)

    position = account.position(SYMBOL)
    assert position is not None
    assert min(price_a, price_b) <= position.entry_price <= max(price_a, price_b)
