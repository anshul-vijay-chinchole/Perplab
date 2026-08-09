"""Where an accepted order goes -- the second and last thing spec 6.1 lets a mode change.

Spec 6.1's table has two rows that differ between backtest, paper and live once the data
source is accounted for: *order destination* ("fill simulator" against "testnet REST" or
"production REST") and *fill source* ("modelled from stored data" against a user-data
stream). Both are the same seam seen from either end, and it is this one.

Everything else about an order -- its identity, its quantisation, the filter validation, the
risk verdict, the ledger entry, the trade it becomes, the slippage it books, the risk
feedback it feeds -- happens in the engine and is shared. A transport may only decide *where
the order goes* and *how its outcome comes back*.

```
Engine._submit(intent)
    -> quantise, emit ORDER, risk check          [shared]
    -> transport.place(order)                    [SEAM]
         SimulatedTransport: schedule an ORDER_ARRIVAL; the fill models take it
         ExchangeTransport:  sign a POST; the user-data stream reports the outcome
    -> Engine._book_fill(...) / _remove(...) / _reject(...)   [shared]
```

The narrowness is the point. A transport that reached further in would be re-implementing
part of the ledger, which is exactly the duplication spec 14-I8 records as the defect this
architecture exists to prevent.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from perplab.core.money import Money

if TYPE_CHECKING:  # pragma: no cover - import cycle; the engine imports this module
    from perplab.engine.backtest import BacktestEngine
    from perplab.engine.executor_base import Order

__all__ = ["OrderTransport", "SimulatedTransport"]


class OrderTransport(Protocol):
    """Delivers accepted orders to wherever they execute.

    Every method is called only after the engine has validated, quantised and risk-checked
    the order, so an implementation never has to repeat any of that -- and must not.
    """

    simulated: bool
    """Whether fills are produced by the engine's own models rather than a real venue.

    The halt path is the one consumer, and the distinction it needs is exact: against the
    simulator, "cancel everything and close out" is something the engine can simply *do* to
    its own book, immediately and locally, because the book is its own model. Against a real
    exchange, the same sentence is a set of requests whose outcomes come back on the wire --
    an order is cancelled when the venue says so, a position is closed when its exit fills.
    A halt that removed orders locally while the venue still worked them, or booked an exit
    at a locally computed price while the venue still held the position, would leave the
    ledger describing an account that does not exist at the worst possible moment.
    """

    def place(self, order: Order) -> None:
        """Send a newly accepted order."""

    def cancel(self, order: Order, *, reason: str) -> None:
        """Ask for an order to be cancelled.

        Spec 6.3 and R19: a cancel carries its own latency and does **not** protect against
        a fill inside that window. A transport must therefore never mark the order cancelled
        itself -- it requests, and the outcome arrives the same way a fill does.
        """

    def modify(self, order: Order, price: Money | None, qty: Money | None) -> None:
        """Amend a resting order in place, preserving queue priority where the venue does."""


class SimulatedTransport:
    """Routes orders into the engine's own matching model -- what every backtest uses.

    Holds no state. The simulated matching engine already lives on the engine (the latency
    queue, the resting book, the fill models), and moving it here would mean moving the
    thirty instance attributes it reads with it. What this class contributes is the *name* of
    the seam: with it in place, a live transport is an alternative rather than a branch, and
    `_on_arrival` is simply never reached in a mode that does not schedule arrivals.
    """

    simulated = True

    def __init__(self, engine: BacktestEngine) -> None:
        self._engine = engine

    def place(self, order: Order) -> None:
        self._engine._place_simulated(order)

    def cancel(self, order: Order, *, reason: str) -> None:
        self._engine._schedule_cancel(order, reason=reason)

    def modify(self, order: Order, price: Money | None, qty: Money | None) -> None:
        self._engine._schedule_modify(order, price, qty)
