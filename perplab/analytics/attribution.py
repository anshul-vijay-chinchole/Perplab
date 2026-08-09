"""PnL attribution (spec 8.4) -- where the money actually came from.

```
net_pnl = price_pnl + funding_pnl - fees - slippage_cost
```

Spec 8.4 calls this *"the fastest way to spot a strategy that 'works' only because the fee
model was too generous, or one whose entire edge is funding capture"*, and requires the
components to sum **exactly** to the total (invariant I9). This module computes them and
then checks that sum with no tolerance, because a decomposition that only roughly adds up
is a decomposition you cannot draw a conclusion from.

**Two departures from the literal text, both to make the identity true rather than
approximately true.**

*`slippage_cost` is signed.* Spec 8.4 defines it as `sum |fill - reference| x qty` and also
requires the four terms to sum exactly to net PnL. Those cannot both hold: when execution
happens to be *better* than the reference -- price drifting your way during the latency
window -- an absolute value charges a cost that was actually a gain, and the identity is off
by twice it. The signed form (positive when execution was worse) makes the arithmetic exact;
the absolute figure is reported alongside as `slippage_abs`, so the "how far did fills land
from the decision price" question still has its answer.

*`liquidation_cost` is its own column.* Carried forward from Phase 2, where the reason is
set out in `Account.attribution`: a liquidation forfeits the whole isolated allocation, which
mixes the price move the position suffered with the clearance penalty on top of it. Folding
the penalty into `price_pnl` charges it to the strategy's price edge, and the most visible
case -- a position liquidated *while in profit* because funding drained its margin -- then
reports a price leg of zero for a position whose price leg was positive.

`price_pnl` here is therefore the **counterfactual at reference prices**: what the strategy
would have made had every order filled at the mark it saw when it decided. That is the
number spec 8.4 means, and separating it from execution is the whole point of the split.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Any

from perplab.core.money import ACCOUNTING_CONTEXT, Money, money_to_str

__all__ = ["Attribution", "AttributionMismatch", "build_attribution"]


class AttributionMismatch(RuntimeError):
    """The components do not sum to net PnL (spec 8.4, invariant I9).

    Fatal, and with no epsilon offered. Spec 3.10 states the conservation invariants
    without tolerance and adds that reaching for one means a float has leaked into the
    ledger; the same reasoning applies here, one layer up.

    **What this check does and does not cover.** It verifies that the *ledger's* four
    components close. It does **not** verify `slippage_cost`, and the algebra is the reason:
    `price_pnl` is defined as the ledger's figure *plus* the signed slippage, so the term
    enters the sum with `+1` and leaves it with `-1` and cancels exactly. Whatever number the
    engine's accumulator holds -- including zero, including one that missed half the fills --
    this identity still closes.

    That matters more than it sounds, because `price_pnl` is the headline "is this a real
    price edge or an execution artefact" number and it is computed *solely* from that
    accumulator. On the Phase 4 exit run the ledger's realised PnL is -1 071 and the reported
    price leg is +428; the entire sign change comes from adding 1 499 of slippage back. A
    silently wrong accumulator would turn a losing strategy into one with an edge, and this
    check would pass.

    The accumulator is therefore verified separately, against the recorded fill and reference
    *prices* rather than against the recorded slippage -- see
    `engine.backtest.BacktestEngine._check_slippage_accumulator`.
    """


@dataclass(frozen=True, slots=True)
class Attribution:
    """The spec 8.4 split, in exact `Decimal`."""

    price_pnl: Money
    """Realised plus unrealised, at the reference prices the strategy decided on."""
    funding_pnl: Money
    """Signed: negative is paid out, positive received (spec 3.5)."""
    fees: Money
    """Always positive, always subtracted (spec 3.1)."""
    slippage_cost: Money
    """Signed cost of execution against the reference. Positive means worse."""
    slippage_abs: Money
    """`sum |fill - reference| x qty` -- spec 8.4's literal formula, reported as written."""
    liquidation_cost: Money
    """Clearance penalty inside realised PnL, negative. Zero for a run with no liquidation."""
    net_pnl: Money
    realized_pnl: Money
    unrealized_pnl: Money

    def to_json(self) -> dict[str, Any]:
        return {
            "price_pnl": money_to_str(self.price_pnl),
            "funding_pnl": money_to_str(self.funding_pnl),
            "fees": money_to_str(self.fees),
            "slippage_cost": money_to_str(self.slippage_cost),
            "slippage_abs": money_to_str(self.slippage_abs),
            "liquidation_cost": money_to_str(self.liquidation_cost),
            "net_pnl": money_to_str(self.net_pnl),
            "realized_pnl": money_to_str(self.realized_pnl),
            "unrealized_pnl": money_to_str(self.unrealized_pnl),
        }

    def as_float(self) -> dict[str, float]:
        """For charting only. The exact figures are the `Decimal`s above."""
        return {
            "price_pnl": float(self.price_pnl),
            "funding_pnl": float(self.funding_pnl),
            "fees": float(self.fees),
            "slippage_cost": float(self.slippage_cost),
            "liquidation_cost": float(self.liquidation_cost),
            "net_pnl": float(self.net_pnl),
        }


def build_attribution(
    ledger: dict[str, Decimal],
    *,
    slippage_cost: Decimal,
    slippage_abs: Decimal,
) -> Attribution:
    """Assemble the split from `Account.attribution()` and the engine's slippage totals.

    The ledger's `price_pnl` is measured at the prices that actually filled; adding the
    signed slippage back recovers the reference-price figure spec 8.4 means.

    The identity asserted below covers the ledger and **not** the slippage term, which
    cancels out of it algebraically -- see `AttributionMismatch`. It is still worth
    asserting: it is genuinely independent of `Account`'s own accumulators, because it is
    rebuilt from the returned mapping rather than from the running state, so a ledger that
    reported four numbers which do not add up fails here even though every one of spec
    3.10's per-mutation invariants passed.
    """
    with localcontext(ACCOUNTING_CONTEXT):
        price_at_reference = ledger["price_pnl"] + slippage_cost
        net = ledger["net_pnl"]
        rebuilt = (
            price_at_reference
            + ledger["funding_pnl"]
            - ledger["fees"]
            - slippage_cost
            + ledger["liquidation_cost"]
        )
        if rebuilt != net:
            raise AttributionMismatch(
                f"spec 8.4 decomposition does not close: price {price_at_reference} + "
                f"funding {ledger['funding_pnl']} - fees {ledger['fees']} - slippage "
                f"{slippage_cost} + liquidation {ledger['liquidation_cost']} = {rebuilt}, "
                f"but net PnL is {net} (off by {rebuilt - net})"
            )

    return Attribution(
        price_pnl=price_at_reference,
        funding_pnl=ledger["funding_pnl"],
        fees=ledger["fees"],
        slippage_cost=slippage_cost,
        slippage_abs=slippage_abs,
        liquidation_cost=ledger["liquidation_cost"],
        net_pnl=net,
        realized_pnl=ledger["realized_pnl"],
        unrealized_pnl=ledger["unrealized_pnl"],
    )
