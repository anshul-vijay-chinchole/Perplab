"""PerpLab — code-first algorithmic trading platform for Binance USD-M perpetual futures.

See PERPLAB_SPEC.md for the authoritative specification. Section references in
docstrings and comments throughout this package point at that document.

The names re-exported here are the ones **strategy code** imports (spec 5.1). They are
deliberately few: a strategy needs the base class and the types its hooks receive, and
everything else reaches it through `ctx`. Two are aliases, because the name a strategy
author wants is not the name the internals want:

- `Position` is `strategy.context.PositionView` — a read-only snapshot, not the ledger's
  mutable `core.account.Position`. A strategy holding the real one could edit the account.
- `Order` is `strategy.context.OrderIntent` — the *request*, which is all a strategy ever
  sees. The engine's order object has an exchange identity and a state machine that only
  exists after submission.

`TradePrint` is here rather than `core.types.AggTrade` because it is what `on_tick`
actually receives. `AggTrade` used to be exported under the banner "the types its hooks
receive" and no hook received one — so an author who typed their `on_tick` against the
exported name was annotating a type the engine never sends, and had no way to import the
one it does.
"""

from __future__ import annotations

from perplab.core.types import Bar, DepthSnapshot, FundingRate, Side
from perplab.engine.ticks import TradePrint
from perplab.strategy.base import Strategy
from perplab.strategy.context import (
    AccountView,
    Context,
    Fill,
    FillTier,
    FundingEvent,
    OrderIntent,
    OrderType,
    PositionView,
    SpreadView,
    TimeInForce,
    WorkingType,
)

Order = OrderIntent
Position = PositionView

__version__ = "0.1.0"

__all__ = [
    "Strategy",
    "Context",
    "Bar",
    "TradePrint",
    "DepthSnapshot",
    "FundingRate",
    "Side",
    "Fill",
    "FundingEvent",
    "Order",
    "OrderIntent",
    "Position",
    "PositionView",
    "AccountView",
    "SpreadView",
    "OrderType",
    "TimeInForce",
    "WorkingType",
    "FillTier",
    "__version__",
]
