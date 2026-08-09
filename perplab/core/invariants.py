"""The spec 3.10 conservation invariants, as runtime assertions.

These are not tests. Spec 3.10 is explicit that they "run as assertions inside the engine,
not just as tests", and the reason is a matter of *when* a bug is found rather than
whether: an accounting error at bar 400 of a 40 000-bar run produces a plausible equity
curve, and the only symptom is a final number that is wrong by an amount nobody can
attribute. Checked on every mutation, the same error stops the run at bar 400 with the
event that caused it still in hand.

**No tolerances.** Spec 3.10 closes with the rule that makes the rest of it work: "exact
equality with `Decimal`. If you find yourself adding an epsilon, a float has leaked into
the accounting layer -- fix that instead." Every comparison here is `==`. The precision
that makes that survivable is set by `money.ACCOUNTING_CONTEXT`; the pressure to add an
epsilon is the signal that something upstream stopped being exact.

**Escalation is the caller's job.** These raise; they do not decide what happens next.
Spec 3.10 wants a backtest to abort with the event log dumped and a *live* session to trip
the kill switch immediately -- an accounting engine that has lost track of state must not
keep sending orders (spec 7). Encoding that difference here would put the live kill switch
behind an import in the arithmetic layer.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal, localcontext

from perplab.core.money import ACCOUNTING_CONTEXT

__all__ = [
    "InvariantViolation",
    "check_wallet_conservation",
    "check_equity",
    "check_position_sum",
    "check_entry_price_presence",
    "check_wallet_non_negative",
    "check_tick_and_step",
    "check_liquidation_ordering",
    "check_monotonic_timestamps",
    "check_pnl_decomposition",
]


class InvariantViolation(AssertionError):
    """A conservation invariant failed (spec 3.10).

    Subclasses `AssertionError` so that it reads as what it is at the point of failure,
    but it is raised unconditionally rather than via the `assert` statement: `python -O`
    strips `assert`, and an accounting guarantee that evaporates under an optimisation
    flag is not a guarantee. The `invariant` attribute carries the identifier (`"I1"`) so
    a handler can route on it without parsing the message.
    """

    def __init__(self, invariant: str, message: str) -> None:
        super().__init__(f"{invariant}: {message}")
        self.invariant = invariant
        self.message = message


def _fail(invariant: str, message: str) -> None:
    raise InvariantViolation(invariant, message)


def check_wallet_conservation(
    wallet: Decimal,
    opening_balance: Decimal,
    realized: Decimal,
    fees: Decimal,
    funding: Decimal,
) -> None:
    """I1 -- `W == W0 + sum(realized) - sum(fees) + sum(funding)`. Every wallet mutation.

    The broadest of the nine: it says the wallet only ever moves for one of exactly three
    reasons. Any code path that adjusts a balance without booking it into one of the three
    accumulators -- a "correction", a rebate, a rounding fix-up -- is caught here on the
    very next mutation, which is precisely the class of change that otherwise accumulates
    unnoticed.

    Fees are stored positive and subtracted (spec 3.1); funding is stored signed and added.

    **Evaluated inside `ACCOUNTING_CONTEXT`, like every other check in this module.** A
    check computed at a different precision than the state it checks is not a check. The
    ledger runs at 50 significant digits and `decimal`'s default context carries 28, so
    re-adding the same four terms out here silently rounded the expected value and left a
    difference around 1e-23 -- which is indistinguishable, at the point of failure, from
    the float leak this invariant exists to catch. It cost a property-test run to find and
    would have cost far more to diagnose from a backtest.
    """
    with localcontext(ACCOUNTING_CONTEXT):
        expected = opening_balance + realized - fees + funding
    if wallet != expected:
        _fail(
            "I1",
            f"wallet {wallet} != opening {opening_balance} + realized {realized} "
            f"- fees {fees} + funding {funding} = {expected} "
            f"(difference {wallet - expected})",
        )


def check_equity(
    equity: Decimal,
    wallet: Decimal,
    positions: Sequence[tuple[Decimal, Decimal, Decimal | None]],
) -> None:
    """I2 -- `E == W + Q*(Pm - Pe)`, summed over open positions. Every mark price update.

    `positions` is a sequence of `(qty, mark_price, entry_price)`. Spec 3.10 states I2 for
    a single position because spec 3 describes a single-symbol account; stated as a sum it
    is the same invariant and it survives spec 9.5's portfolio backtesting without the
    accounting layer having to be reopened.

    An empty sequence means flat, and equity is then the wallet exactly. That is not a
    special case bolted on -- it is what the sum evaluates to -- but it is worth naming,
    because the tempting alternative of keeping a zero-quantity position with a stale
    entry price is precisely what I4 forbids.
    """
    with localcontext(ACCOUNTING_CONTEXT):
        total = wallet
        for qty, mark_price, entry_price in positions:
            if qty == 0:
                _fail("I2", "a flat position must not be carried in the position set")
            if entry_price is None:
                _fail("I2", f"position {qty} is open but has no entry price")
                continue  # unreachable; narrows the type below
            total += qty * (mark_price - entry_price)

    if equity != total:
        _fail(
            "I2",
            f"equity {equity} != wallet {wallet} + sum(Q*(Pm - Pe)) = {total} "
            f"(difference {equity - total})",
        )


def check_position_sum(qty: Decimal, signed_fills: Decimal) -> None:
    """I3 -- `Q == sum(signed fills)`, **per position**. Every fill.

    Catches the case-handling in spec 3.3's fill application getting the arithmetic right
    for PnL while getting the resulting position wrong -- most plausibly on a flip, where
    the quantity crosses through zero and it is tempting to compute the residual from
    `|f| - |Q|` and then attach the wrong sign to it.

    **"Per position", not "per symbol", and the distinction is what makes I3 survive hedge
    mode.** Spec 3.10 states it as `Q == Sigma(signed fills)` because spec 3 describes a
    one-way account, where a symbol *is* a position and the two phrasings are the same
    sentence. They come apart the moment a symbol can hold a long and a short at once: a buy
    routed to the short side reduces it, so it enters that side's accumulator negative and
    does not touch the long side at all, and the sum of a symbol's fills then equals neither
    leg's quantity. An I3 stated per symbol would fire on a *correct* hedge account -- and an
    invariant that fires on correct code does not survive contact with a real run, because
    the first thing anybody does is switch it off.

    The check itself is unchanged, deliberately. `Account._check_after_fill` passes the
    quantity and the accumulator for the one `(symbol, side)` the fill moved, so the claim
    being asserted is still exactly "this position holds what was filled into it" -- the
    restatement is in what "this position" addresses, not in what is being proved. Keeping
    the arithmetic identical is also what lets the one-way case remain literally the spec's
    own invariant rather than a generalisation of it.
    """
    if qty != signed_fills:
        _fail(
            "I3",
            f"position {qty} != sum of signed fills {signed_fills} "
            f"(difference {qty - signed_fills})",
        )


def check_entry_price_presence(qty: Decimal, entry_price: Decimal | None) -> None:
    """I4 -- `Q == 0` if and only if `Pe is None`. Every fill.

    Both directions matter. A leftover entry price on a flat position feeds I2 a phantom
    unrealised PnL; a missing one on an open position means the next reduce computes
    realised PnL against nothing at all.
    """
    if qty == 0 and entry_price is not None:
        _fail("I4", f"position is flat but entry price is {entry_price}, expected None")
    if qty != 0 and entry_price is None:
        _fail("I4", f"position is {qty} but entry price is None")


def check_wallet_non_negative(wallet: Decimal, liquidated: bool) -> None:
    """I5 -- `W >= 0` unless a liquidation event was emitted. Every wallet mutation.

    The exemption is not a loophole: under the spec 3.7 model a liquidation destroys the
    entire isolated margin allocated to the position, and with several positions open that
    can legitimately drive the wallet to zero. What it must never do is go negative
    *silently* -- a negative wallet with no liquidation on the log means fees or funding
    were charged against money that was not there, which is a sizing bug upstream.
    """
    if wallet < 0 and not liquidated:
        _fail("I5", f"wallet {wallet} is negative with no liquidation event emitted")


def check_tick_and_step(
    price: Decimal,
    qty: Decimal,
    tick_size: Decimal,
    step_size: Decimal,
) -> None:
    """I6 -- prices are exact multiples of `tickSize`, quantities of `stepSize`.

    Spec 3.10 places this at order submission; it is applied to fills as well, because a
    fill at an off-tick price means the fill model invented a price the exchange could not
    have printed. That is the same fiction spec 3.2 rules out, arriving from the other
    direction.

    `%` on `Decimal` is exact, so this needs no tolerance -- which is the point of storing
    market data as scaled integers in the first place.
    """
    if tick_size <= 0 or step_size <= 0:
        raise ValueError(f"tick {tick_size} and step {step_size} must both be positive")
    if price % tick_size != 0:
        _fail("I6", f"price {price} is not a multiple of tick size {tick_size}")
    if qty % step_size != 0:
        _fail("I6", f"quantity {qty} is not a multiple of step size {step_size}")


def check_liquidation_ordering(
    qty: Decimal,
    entry_price: Decimal,
    liquidation_price: Decimal,
    bankruptcy_price: Decimal,
) -> None:
    """I7 -- long: `P_bank < P_liq < Pe`; short: `Pe < P_liq < P_bank`.

    Spec 3.10 names the diagnosis outright: "if this invariant ever fails, the bracket
    resolution is wrong". It is the cheapest available check on the fixed-point solve in
    `margin.liquidation_price`, because the correct answer to a wrongly-resolved bracket
    is still a perfectly plausible-looking price.

    Skipped when the liquidation price is non-positive. That is an unleveraged position
    whose liquidation price sits below zero -- unreachable, correctly so, and the ordering
    is vacuous there.
    """
    if liquidation_price <= 0:
        return

    if qty > 0:
        ordered = bankruptcy_price < liquidation_price < entry_price
        shape = f"{bankruptcy_price} < {liquidation_price} < {entry_price}"
    elif qty < 0:
        ordered = entry_price < liquidation_price < bankruptcy_price
        shape = f"{entry_price} < {liquidation_price} < {bankruptcy_price}"
    else:
        _fail("I7", "liquidation ordering is undefined for a flat position")
        return

    if not ordered:
        _fail(
            "I7",
            f"expected {'P_bank < P_liq < Pe' if qty > 0 else 'Pe < P_liq < P_bank'} "
            f"for position {qty}, got {shape}; the bracket resolution is wrong",
        )


def check_monotonic_timestamps(previous_ms: int, ts_ms: int) -> None:
    """I8 -- event log timestamps are monotonically non-decreasing. Every event append.

    Non-decreasing, not increasing: a mark update, a funding settlement and a fill can all
    land on the same millisecond, and spec 6.2 orders those by `kind_priority` rather than
    by clock. What this catches is an event arriving *before* one already logged, which
    means the event loop's total ordering has been bypassed -- and once that happens the
    run is no longer reproducible, which spec 12.1 treats as disqualifying.
    """
    if ts_ms < previous_ms:
        _fail(
            "I8",
            f"event at {ts_ms} precedes the previous event at {previous_ms} "
            f"(went backwards by {previous_ms - ts_ms} ms)",
        )


def check_pnl_decomposition(
    equity: Decimal,
    opening_balance: Decimal,
    realized: Decimal,
    fees: Decimal,
    funding: Decimal,
    unrealized: Decimal,
) -> None:
    """I9 -- realised, fees, funding and open unrealised PnL account for total PnL. End of run.

    Spec 3.10 phrases this over reconstructed round-trips ("sum of per-trade PnL + open
    position uPnL == total PnL"). The round-trip reconstruction is spec 8.3 and belongs to
    the analytics layer; the identity it rests on is this one, and it can be asserted the
    moment the ledger exists rather than waiting for the layer above it.

    It is close to I1 + I2 composed, and that is the useful part: run at the end, over
    accumulators rather than a single step, it catches an error that cancelled itself
    within any individual mutation.
    """
    with localcontext(ACCOUNTING_CONTEXT):
        expected = opening_balance + realized - fees + funding + unrealized
    if equity != expected:
        _fail(
            "I9",
            f"equity {equity} != opening {opening_balance} + realized {realized} "
            f"- fees {fees} + funding {funding} + unrealized {unrealized} = {expected} "
            f"(difference {equity - expected})",
        )
