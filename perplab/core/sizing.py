"""Position sizing — `ctx.risk` arithmetic (spec 5.3).

This lives in `core/` rather than in `strategy/` because of what it actually is. Every
function here takes balances and prices and returns an order quantity, all in `Decimal`,
finishing on `quantize_qty`. That is ledger arithmetic wearing a strategy-facing name, and
putting it behind the accounting seam (spec 3.1) keeps `strategy/context.py` free of
`Decimal` entirely -- the context becomes a facade that passes values through rather than
a second place where money math happens.

**Every result rounds down.** `quantize_qty` floors to `stepSize`, so a size that would
have been 0.0017 BTC becomes 0.001, never 0.002. Rounding a computed size *up* to the step
is how a strategy asking to risk 1% ends up risking 1.7%, and it compounds: the smaller
the account, the coarser the step relative to the intended size, and the worse the
overshoot. A size of zero is a legitimate answer -- it means the account cannot express
this trade at this step size -- and the caller is expected to treat it as "do not trade"
rather than as a failure.
"""

from __future__ import annotations

from decimal import Decimal, localcontext

from perplab.core.money import ACCOUNTING_CONTEXT, quantize_qty

__all__ = [
    "size_by_stop",
    "size_by_notional",
    "max_qty_for_margin",
]


def _require_positive(value: Decimal, name: str) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")


def size_by_stop(
    *,
    entry: Decimal,
    stop: Decimal,
    risk_fraction: Decimal,
    equity: Decimal,
    step_size: Decimal,
) -> Decimal:
    """Quantity such that a move from `entry` to `stop` loses `risk_fraction` of equity.

    ```
    qty = (equity * risk_fraction) / |entry - stop|      then floored to stepSize
    ```

    Deliberately *not* leverage-aware, and that is the point of the function: it sizes by
    the distance to the invalidation level, so the risk taken is the same whether the stop
    is 0.5% away or 5% away. Sizing by notional and then placing a stop wherever it looks
    nice is the same trade with an unknown risk attached.

    The loss this models is the price move alone. Fees on both legs and any funding paid
    while the position is open are additional, so a 1% risk fraction loses slightly more
    than 1% in practice. Building a fee estimate in here would need a round-trip
    assumption the caller has not made yet; the honest treatment is to state it.
    """
    with localcontext(ACCOUNTING_CONTEXT):
        _require_positive(step_size, "step_size")
        if risk_fraction <= 0 or risk_fraction > 1:
            raise ValueError(
                f"risk_fraction must be in (0, 1], got {risk_fraction}"
            )
        distance = abs(entry - stop)
        if distance == 0:
            # Not a division-by-zero guard for its own sake: a stop at the entry price
            # means "risk this fraction of equity over a zero price move", whose solution
            # is an infinite position. Returning a huge number, or zero, would both be
            # answers to a question that was not asked.
            raise ValueError(
                "stop equals entry: the risk per unit is zero, so no quantity satisfies "
                "the requested risk. Place the stop at the level that invalidates the "
                "trade."
            )
        if equity <= 0:
            return Decimal(0)
        return quantize_qty(equity * risk_fraction / distance, step_size)


def size_by_notional(
    *, notional: Decimal, price: Decimal, step_size: Decimal
) -> Decimal:
    """Quantity worth `notional` at `price`, floored to `stepSize`."""
    with localcontext(ACCOUNTING_CONTEXT):
        _require_positive(price, "price")
        _require_positive(step_size, "step_size")
        if notional < 0:
            raise ValueError(f"notional must be non-negative, got {notional}")
        return quantize_qty(notional / price, step_size)


def max_qty_for_margin(
    *,
    available: Decimal,
    price: Decimal,
    leverage: int,
    step_size: Decimal,
) -> Decimal:
    """Largest quantity the free balance can post initial margin for, at `leverage`.

    The margin bound only. Exchange filters (`maxQty`, `PERCENT_PRICE`) and the per-run
    risk limits of spec 7 constrain it further, and both are applied by their owners --
    `exchange.filters.validate_order` and the risk layer -- rather than approximated here.
    `Context.risk.max_allowed` composes them; this function is the margin term of that
    composition, and on its own it is an upper bound rather than a permission.
    """
    with localcontext(ACCOUNTING_CONTEXT):
        _require_positive(price, "price")
        _require_positive(step_size, "step_size")
        if isinstance(leverage, bool) or not isinstance(leverage, int) or leverage < 1:
            raise ValueError(f"leverage must be an int >= 1, got {leverage!r}")
        if available <= 0:
            return Decimal(0)
        return quantize_qty(available * leverage / price, step_size)
