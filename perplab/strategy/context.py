"""`ctx` — everything a strategy can see and do (spec 5.3).

`Context` is a facade. It holds no market state and executes no orders; it validates, then
delegates to a `Runtime` the engine supplies. Three things follow from that, and all three
are the reason it is built this way:

**One API, three engines.** Backtest, paper and live all implement `Runtime`, so strategy
code cannot tell which one it is running under (spec 6.1). A context that reached into a
backtest data structure directly would need a second implementation for live, and the two
would drift.

**Warm-up blocking lives here, not in the strategy.** `ctx.buy` raises during warm-up
(spec 5.2). Enforcing it in the one place every order passes through means a strategy that
forgets `if not ctx.warm: return` gets an error instead of a position taken on a
three-bar EMA that thought it was two hundred.

**No clock, no randomness, no I/O.** `ctx.now` is the engine's event clock and `ctx.rng`
is the run's seeded RNG. `time.time()` and an unseeded `random` are both look-ahead and
reproducibility failures, and the validator rejects them (spec 5.5) -- but the reason they
are rejectable at all is that this object already provides the honest versions.

**`Decimal` is not imported here.** Every monetary value on this surface was produced by
the accounting layer and is passed through untouched; the arithmetic that needs `Decimal`
lives in `core.sizing` behind the seam (spec 3.1).
"""

from __future__ import annotations

import random
import weakref
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Mapping, Protocol, Sequence

from perplab.core.money import Money, money_to_str, parse_money
from perplab.core.sizing import max_qty_for_margin, size_by_notional, size_by_stop
from perplab.core.types import PositionSide
from perplab.strategy.indicators import IndicatorSet, SealedIndicators

if TYPE_CHECKING:  # pragma: no cover
    from perplab.core.types import DepthSnapshot

__all__ = [
    "FillTier",
    "OrderType",
    "TimeInForce",
    "WorkingType",
    "OrderIntent",
    "Fill",
    "FundingEvent",
    "PositionView",
    "AccountView",
    "FundingView",
    "SpreadView",
    "MacroView",
    "StrategyEvent",
    "Runtime",
    "RiskAPI",
    "LogAPI",
    "Context",
    "UnsupportedOrder",
    "WarmupViolation",
    "DataUnavailable",
]


class FillTier(Enum):
    """Fidelity of the fill model for this run (spec 4.2).

    Ordered by fidelity, and comparable, so `tier < FillTier.BOOK_WALK` is a legal
    question. `ctx.book` uses exactly that comparison: depth is only real at the top tier,
    and a run whose range predates the collector's coverage degrades to `BOOK_TICKER`
    silently unless something refuses (spec 4.2 note 3).
    """

    BAR_CLOSE = 0
    TRADE_ONLY = 1
    BOOK_TICKER = 2
    BOOK_WALK = 3

    def __lt__(self, other: object) -> bool:
        if isinstance(other, FillTier):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, FillTier):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, FillTier):
            return self.value > other.value
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, FillTier):
            return self.value >= other.value
        return NotImplemented


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"


class TimeInForce(Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTX = "GTX"
    """Post-only. Cancelled if it would cross -- how maker fees are guaranteed, and also
    how orders silently fail to enter (spec 6.5)."""


_LIMIT_ONLY_TIF = frozenset({TimeInForce.FOK, TimeInForce.GTX})
"""Times in force that only mean something for an order that can rest.

`GTX` is post-only -- there is nothing to post a market order to. `FOK` asks the book to fill
a stated quantity outright, which is a question about a limit price. Binance rejects both on
a market order; so does `OrderIntent`.
"""


class WorkingType(Enum):
    """Which price a stop or take-profit trigger is measured against (spec 6.4).

    `MARK_PRICE` is the default because it is Binance's default, and the difference is not
    cosmetic: a stop on contract price can be fired by a single wick on one venue, which is
    the mechanic behind most "my stop was hunted" complaints. Mark price is an index and is
    far harder to push.

    The trade-off runs the other way on timing. Mark price is stored as 1-minute bars, so a
    mark-triggered stop fires at the *close* of the minute whose range contained its level --
    up to a minute late, and late is the pessimistic direction for both a stop and a
    take-profit. A contract-price trigger is evaluated against the trade tape and fires on
    the exact print. Fidelity of the trigger source against fidelity of the trigger instant;
    the default follows the exchange.
    """

    MARK_PRICE = "MARK_PRICE"
    CONTRACT_PRICE = "CONTRACT_PRICE"


class UnsupportedOrder(RuntimeError):
    """This execution mode cannot honour the order as written.

    Lives here rather than in `engine.backtest` because every mode raises it and the smoke
    run cannot import the engine -- `backtest` imports `dryrun.event_hash`, so the arrow
    only points one way. Two exception types for one condition is one of them being wrong,
    and the one a strategy catches would have been the wrong one in the other mode.
    """


class WarmupViolation(RuntimeError):
    """Raised when a strategy tries to trade before warm-up completes (spec 5.2)."""


class DataUnavailable(RuntimeError):
    """Raised when a strategy asks for data this run's fill tier does not carry."""


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """What a strategy asked for, before the engine has decided anything about it.

    Distinct from an `Order` (which has an exchange identity and a state machine) on
    purpose: this is the *request*, and it is what gets written to the event log. A run's
    determinism is checked over intents, so it stays checkable even when the fills differ
    because the fill model changed.
    """

    symbol: str
    side: str
    qty: Money
    type: OrderType = OrderType.MARKET
    price: Money | None = None
    stop_price: Money | None = None
    callback_rate: Money | None = None
    """Trailing-stop retracement as a **fraction**: 0.01 is a 1% callback.

    Binance's own `callbackRate` field is in percent, so 0.01 here is their 1.0. Stated
    because a rate read as percent when it means fraction places the stop a hundred times
    further away than the author intended -- a strategy that then never stops out and looks
    wonderful right up until it does.
    """
    tif: TimeInForce = TimeInForce.GTC
    working_type: WorkingType = WorkingType.MARK_PRICE
    """Trigger source for stop/take-profit/trailing orders. Ignored by other types."""
    reduce_only: bool = False
    position_side: PositionSide = PositionSide.BOTH
    """Which of the symbol's positions this order acts on (hedge mode).

    `BOTH` in one-way mode -- Binance's own value, sent verbatim, so nothing translates
    between the intent and the wire.

    **In hedge mode this is not derivable and is therefore required.** A `SELL` means
    "reduce the long" or "open/increase the short" depending on this field alone, and those
    two orders leave the account in states that differ by the entire position. Binance
    treats it the same way -- `positionSide` is mandatory on a hedge-mode order and a
    mismatch is rejected outright (`-4061`) rather than interpreted.

    `reduce_only` is meaningless alongside a hedged side and is refused with one: on the
    long side a sell is *already* reduce-only by construction, and Binance rejects the
    combination rather than ignoring it.
    """
    client_id: str | None = None
    tag: str | None = None

    def __post_init__(self) -> None:
        """Defend the invariants at the *type*, not only at the method that builds it.

        `Context._order` already checks these, and `Runtime.submit` takes an `OrderIntent`
        directly -- so a future engine, a test, or the live executor can construct one
        without passing through `ctx`. `dryrun` defends `qty <= 0` at that seam for exactly
        this reason; these two are held to the same standard.

        The `callback_rate` bound is the one that matters. It is a **fraction**, and Binance's
        own field is in percent: passing their `1.0` produces a trigger price of *zero* and a
        trailing stop that can never fire, silently, on a strategy that then looks like one
        that was simply never stopped out.
        """
        if self.qty <= 0:
            raise ValueError(f"an order needs a positive quantity, got {self.qty}")
        if self.callback_rate is not None and not (0 < self.callback_rate < 1):
            raise ValueError(
                f"callback_rate is a fraction and must be in (0, 1); got "
                f"{self.callback_rate}. Binance's own field is in percent, so their 1.0 is "
                f"0.01 here."
            )
        if self.position_side.is_hedged and self.reduce_only:
            # Binance rejects `reduceOnly` in hedge mode outright, and it is right to: on
            # the LONG side a sell can only reduce, so the flag is either redundant or a
            # statement about a side this order is not on. Accepting it here and dropping
            # it at the wire would be a live/backtest divergence in the risk layer, where
            # `reduce_only` buys an exemption from every size limit (`RiskEngine.check_order`).
            raise ValueError(
                f"reduce_only is not meaningful on the {self.position_side.value} side of a "
                "hedge: a sell against the long side already only reduces it, and the "
                "exchange rejects the combination rather than ignoring it. Drop "
                "reduce_only, or route the order to the side you meant to reduce."
            )
        if self.type is not OrderType.LIMIT and self.tif in _LIMIT_ONLY_TIF:
            # Binance rejects a post-only or fill-or-kill *market* order outright. Accepting
            # one here and filling it as a taker is a live/backtest divergence in exactly the
            # mechanism spec 6.5 names as "how orders silently fail to enter".
            raise ValueError(
                f"{self.tif.value} is a resting-order time in force and this is a "
                f"{self.type.value} order. The exchange would reject it rather than fill it; "
                f"use GTC or IOC."
            )

    def to_json(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "qty": money_to_str(self.qty),
            "type": self.type.value,
            "price": None if self.price is None else money_to_str(self.price),
            "stop_price": None if self.stop_price is None else money_to_str(self.stop_price),
            "callback_rate": (
                None if self.callback_rate is None else money_to_str(self.callback_rate)
            ),
            "tif": self.tif.value,
            "working_type": self.working_type.value,
            "reduce_only": self.reduce_only,
            "position_side": self.position_side.value,
            "client_id": self.client_id,
            "tag": self.tag,
        }


@dataclass(frozen=True, slots=True)
class Fill:
    """One execution against an order, handed to `on_fill` (spec 5.1).

    Emitted **per increment**, not once per order. A limit order that fills in four pieces
    calls `on_fill` four times, because that is what happens in live and a strategy that
    assumes one-order-one-fill is wrong there (spec 6.5).
    """

    order_id: str
    symbol: str
    side: str
    qty: Money
    price: Money
    ts_ms: int
    is_maker: bool
    reduce_only: bool = False
    tag: str | None = None
    position_side: PositionSide = PositionSide.BOTH
    """Which position the fill landed on -- carried through from the order's intent.

    A strategy handling `on_fill` in hedge mode has to know which of its two positions just
    moved, and `side` does not say: a `SELL` is both "the long shrank" and "the short grew"
    depending on this."""

    def to_json(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "qty": money_to_str(self.qty),
            "price": money_to_str(self.price),
            "ts_ms": self.ts_ms,
            "is_maker": self.is_maker,
            "reduce_only": self.reduce_only,
            "position_side": self.position_side.value,
            "tag": self.tag,
        }


@dataclass(frozen=True, slots=True)
class OrderEnd:
    """An order left the book without filling in full, handed to `on_cancel`.

    **Fires for all three terminal states, not only for cancels the strategy asked for**,
    and `status` is what tells them apart:

    - `CANCELLED` -- the strategy's own cancel arrived, or the engine pulled it (a
      liquidation clearing the symbol's book, a halt).
    - `EXPIRED` -- the order retired itself: an `IOC` remainder, a `FOK` that could not
      fill in full, a post-only that would have crossed, a `TRADE_ONLY` market order that
      waited past its deadline.
    - `REJECTED` -- an exchange filter, the ledger's margin check, or the risk layer
      refused it.

    Firing on one and not the others would be the trap: a strategy that quotes both sides
    and waits for `on_cancel` before requoting would hang forever the first time a
    post-only order silently failed to enter, which is spec 6.5's named hazard. What the
    strategy needs to know is *the order is gone and it did not fill*, and that is one fact
    with three causes.

    `filled_qty` is what did go through before it left. A partially filled limit order that
    is then cancelled reports the part that traded, because a strategy that assumes a
    cancelled order traded nothing will misreport its own position.
    """

    order_id: str
    symbol: str
    side: str
    status: str
    reason: str
    ts_ms: int
    filled_qty: Money
    remaining_qty: Money
    tag: str | None = None

    @property
    def partially_filled(self) -> bool:
        return self.filled_qty > 0

    def to_json(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "status": self.status,
            "reason": self.reason,
            "ts_ms": self.ts_ms,
            "filled_qty": money_to_str(self.filled_qty),
            "remaining_qty": money_to_str(self.remaining_qty),
            "tag": self.tag,
        }


@dataclass(frozen=True, slots=True)
class FundingEvent:
    """A funding settlement affecting an open position, handed to `on_funding` (spec 3.5).

    `payment` is signed from this account's perspective: negative is paid out, positive is
    received. The sign convention is stated because getting it backwards produces a
    strategy that farms funding in exactly the wrong direction and still looks profitable
    on a rising market.
    """

    symbol: str
    ts_ms: int
    rate: Money
    mark_price: Money
    payment: Money

    def to_json(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "ts_ms": self.ts_ms,
            "rate": money_to_str(self.rate),
            "mark_price": money_to_str(self.mark_price),
            "payment": money_to_str(self.payment),
        }


@dataclass(frozen=True, slots=True)
class PositionView:
    """Read-only view of a position (spec 5.3).

    A frozen copy rather than the live `core.account.Position`, so that ordinary strategy
    code -- attribute writes, held references -- cannot move the ledger by accident. It is
    a guard against mistakes, not a security boundary: in-process Python has none (the
    sandbox docs say so in as many words), and a strategy determined to reach the engine's
    internals is a strategy the validator and code review exist to catch, not this
    dataclass. `Context` seals its own attributes and keeps its runtime out of casual
    reach for the same reason and with the same limits.

    `liquidation_price` is `None` when the position is flat or when no bracket table is
    loaded -- never a sentinel number, because a sentinel would be compared against the
    mark and quietly satisfy a distance check.
    """

    symbol: str
    qty: Money
    entry_price: Money
    unrealized_pnl: Money
    liquidation_price: Money | None
    margin: Money
    position_side: PositionSide = PositionSide.BOTH
    """Which position this view describes.

    Defaulted and last so every one-way construction is unchanged. In hedge mode the two
    views of one symbol are genuinely different positions -- different entry price,
    different unrealised PnL, different liquidation price -- and this is what says which
    one is in hand."""

    @property
    def is_flat(self) -> bool:
        return self.qty == 0

    @property
    def is_long(self) -> bool:
        """Whether this view is facing long, from the sign of the quantity.

        Reads the quantity rather than `position_side` deliberately, and the two agree
        wherever both are meaningful: the LONG side of a hedge only ever holds a positive
        quantity (`Position.__post_init__` refuses otherwise), and a flat side is neither
        long nor short. Keying off `position_side` instead would make a *flat* long side
        report `is_long`, and `if ctx.position(...).is_long: close()` would then submit an
        order against nothing.
        """
        return self.qty > 0

    @property
    def is_short(self) -> bool:
        return self.qty < 0


@dataclass(frozen=True, slots=True)
class AccountView:
    wallet_balance: Money
    equity: Money
    available: Money
    used_margin: Money


@dataclass(frozen=True, slots=True)
class FundingView:
    last_rate: Money | None
    next_settlement_ms: int | None
    predicted_rate: Money | None
    last_withheld: bool = False
    """Whether the settlement behind `last_rate` was **not booked** into this ledger.

    Spec 3.4 forbids inferring a mark, so a settlement arriving on a symbol with no mark
    price withholds its cashflow: the wallet did not move and `funding_pnl` omits it, while
    `last_rate` is still published because the venue genuinely settled at that rate. A
    carry strategy reading `last_rate` as money its own ledger received must check this
    first -- without it, the rate and the cashflow were indistinguishable, and the strategy
    traded a payment the run's own attribution reports never happened. The run-level
    `FUNDING_UNSETTLED` flag and warning say the same thing once; this says it per
    settlement, where the trading decision is made.
    """


@dataclass(frozen=True, slots=True)
class SpreadView:
    """Best bid and ask, with the size resting at each.

    The sizes are carried because `bookTicker` publishes them and a strategy sizing against
    visible liquidity has a real use for them -- and because the same field exists on the
    live stream, so a strategy reading it in backtest reads the same thing in production.
    They default to `None` for the smoke-run runtime, which derives a spread from a depth
    fixture and has no independent notion of touch size.
    """

    bid: Money
    ask: Money
    bid_qty: Money | None = None
    ask_qty: Money | None = None

    @property
    def mid(self) -> Money:
        """The exact midpoint -- which is **usually not a price the exchange accepts**.

        `(bid + ask) / 2` lands off the tick grid whenever the spread is an odd number of
        ticks: 60 000.01 / 60 000.02 mids to 60 000.015 on a 0.01 tick. Feeding that to
        `ctx.buy(price=spread.mid)` produces an order the venue's `PRICE_FILTER` refuses --
        in a live session the engine's own wire validation (`_check_wire_filters`, C6)
        catches it locally before the POST, so the mistake is a named rejection rather
        than a `-1111` feeding the kill switch's streak, but it is still a rejected order.
        This property stays exact because the true midpoint is a *measurement* (basis
        arithmetic, spread-fraction signals) and quantising it here would lie to every
        reader that never sends it to an exchange. For an order price, use `mid_price`.
        """
        return (self.bid + self.ask) / 2

    def mid_price(self, tick_size: Money) -> Money:
        """The midpoint quantised to the tick grid, for use as an order price.

        Nearest tick, ties away from the bid: the only representable prices are whole
        ticks, the true mid sits at most half a tick from the result, and picking a fixed
        side for the exact-half case keeps two runs of the same book byte-identical. A
        caller who wants a *directional* rounding -- a buy that must not cross, say --
        should quantise `mid` with `core.money.quantize_price`, which takes the side.
        `ctx.tick_size()` supplies the grid.
        """
        if tick_size <= 0:
            raise ValueError(f"tick_size must be positive, got {tick_size}")
        mid = (self.bid + self.ask) / 2
        steps = int((mid + tick_size / 2) / tick_size)
        return steps * tick_size

    @property
    def absolute(self) -> Money:
        return self.ask - self.bid


@dataclass(frozen=True, slots=True)
class MacroView:
    """One macro/cross-asset reading, with the age that makes it judgeable (Phase 11).

    `value` is a `float`, not a `Money`: these are feature inputs -- a dominance fraction,
    a dollar-index level, a market cap -- and spec 3.1 puts feature inputs on the float
    side of the seam. Nothing here ever reaches the ledger, and typing it as `Money` would
    invite exactly that.
    """

    series: str
    value: float
    ts_ms: int
    """The **source's** timestamp for this reading, not when the platform stored it."""
    age_ms: int
    """How stale the reading is at `ctx.now`, measured from when it **published**.

    Never zero in practice, and that is correct rather than a rounding artefact: a reading
    only becomes visible once the platform has *received* it, and the provider's stamp is
    minutes older than that by the time it arrives. First sight of a fresh CoinGecko
    reading is therefore an age of a few minutes, and of a DXY quote around ten.

    Carried rather than left for the strategy to compute, because computing it needs both
    `ctx.now` and the reading's own timestamp, and a strategy that got that subtraction
    backwards would silently trade on a stale dollar through every weekend.
    """

    @property
    def age_hours(self) -> float:
        return self.age_ms / 3_600_000


@dataclass(frozen=True, slots=True)
class StrategyEvent:
    """One entry in the run's event log.

    The event log is what spec 12.1's reproducibility invariant hashes, so its contents are
    a contract rather than a debugging aid. `seq` is included because two events can share
    a millisecond and the hash must not depend on dict ordering or on how fast the machine
    ran.
    """

    seq: int
    ts_ms: int
    kind: str
    payload: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "ts_ms": self.ts_ms,
            "kind": self.kind,
            "payload": dict(self.payload),
        }


class Runtime(Protocol):
    """What an execution engine must provide to back a `Context` (spec 6.1).

    Backtest, paper and live all satisfy this. Everything is a plain query or a submission;
    nothing here lets the context reach into engine internals, which is what keeps the
    three implementations substitutable.
    """

    @property
    def now_ms(self) -> int: ...

    @property
    def fill_tier(self) -> FillTier: ...

    @property
    def hedge_mode(self) -> bool: ...

    def position_view(
        self, symbol: str, position_side: PositionSide = PositionSide.BOTH
    ) -> PositionView: ...

    def account_view(self) -> AccountView: ...

    def mark_price(self, symbol: str) -> Money: ...

    def funding_view(self, symbol: str) -> FundingView: ...

    def open_interest(self, symbol: str) -> float | None: ...

    def macro(self, name: str) -> "MacroView | None": ...

    def depth(self, symbol: str) -> DepthSnapshot | None: ...

    def spread(self, symbol: str) -> SpreadView | None: ...

    def step_size(self, symbol: str) -> Money: ...

    def leverage(self, symbol: str) -> int: ...

    def set_leverage(self, symbol: str, leverage: int) -> None: ...

    def submit(self, intent: OrderIntent) -> str: ...

    def cancel(self, order_id: str) -> None: ...

    def modify(self, order_id: str, price: Money | None, qty: Money | None) -> None: ...

    def cancel_all(self, symbol: str | None) -> None: ...

    def open_order_ids(self, symbol: str | None) -> Sequence[str]: ...

    def emit(self, kind: str, payload: Mapping[str, Any]) -> None: ...


# ---------------------------------------------------------------------------- risk


@dataclass(frozen=True, slots=True)
class RiskAPI:
    """`ctx.risk` — position sizing (spec 5.3)."""

    _ctx: Context

    def size_by_stop(
        self,
        entry: Money,
        stop: Money,
        risk_fraction: Money,
        *,
        symbol: str | None = None,
    ) -> Money:
        """Size so that being stopped out costs `risk_fraction` of current equity."""
        target = self._ctx._resolve_symbol(symbol)
        return size_by_stop(
            entry=entry,
            stop=stop,
            risk_fraction=risk_fraction,
            equity=self._ctx.account.equity,
            step_size=_runtime_of(self._ctx).step_size(target),
        )

    def size_by_notional(self, notional: Money, *, symbol: str | None = None) -> Money:
        """Size to a target notional at the current mark price."""
        target = self._ctx._resolve_symbol(symbol)
        return size_by_notional(
            notional=notional,
            price=self._ctx.mark(target),
            step_size=_runtime_of(self._ctx).step_size(target),
        )

    def max_allowed(self, symbol: str | None = None) -> Money:
        """The largest quantity this account can currently open at `symbol`.

        Bounded by free margin and the symbol's step size. The per-run risk limits of spec
        7 are *not* applied -- the risk layer is Phase 6 and does not exist yet -- so this
        is an upper bound that will tighten. It is deliberately not documented as "the
        maximum allowed": a number that later gets smaller is safe to have believed; one
        that later gets larger would have permitted trades the limits forbid.
        """
        target = self._ctx._resolve_symbol(symbol)
        return max_qty_for_margin(
            available=self._ctx.account.available,
            price=self._ctx.mark(target),
            leverage=_runtime_of(self._ctx).leverage(target),
            step_size=_runtime_of(self._ctx).step_size(target),
        )


@dataclass(frozen=True, slots=True)
class LogAPI:
    """`ctx.log` — structured logging into the run event log and the Feed tab (spec 5.3).

    Structured rather than formatted: `ctx.log.info("entry", price=p, reason="cross")`
    keeps the fields queryable in the event-log viewer, where a pre-formatted string is
    only greppable. The message and its fields both enter the reproducibility hash, so a
    log line that differs between two runs of the same seed is itself a determinism
    failure worth catching.
    """

    _ctx: Context

    def _write(self, level: str, message: str, fields: Mapping[str, Any]) -> None:
        self._ctx._emit("LOG", {"level": level, "message": message, "fields": dict(fields)})

    def info(self, message: str, **fields: Any) -> None:
        self._write("INFO", message, fields)

    def warn(self, message: str, **fields: Any) -> None:
        self._write("WARN", message, fields)

    def error(self, message: str, **fields: Any) -> None:
        self._write("ERROR", message, fields)


# ------------------------------------------------------------------------- context


_RUNTIMES: "weakref.WeakKeyDictionary[Context, Runtime]" = weakref.WeakKeyDictionary()
"""Each context's runtime, held **outside** the instance.

Why a side table instead of an attribute: `ctx` is handed to arbitrary strategy code, and
`ctx._runtime` was an unguarded escape hatch -- `ctx._runtime.submit(...)` bypassed the
warm-up gate, `ctx._runtime.account` was the live ledger, and the engine itself hung off
`submit_hook.__self__` with every eagerly-loaded future observation in reach. In-process
Python cannot be a hard boundary (the sandbox docs say so), so this is not one; it is the
pragmatic version: the runtime does not appear in `dir(ctx)` or `vars(ctx)` at all, a read
of `ctx._runtime` raises with a message naming the public surface, and reaching it now
requires an import of this module's privates -- which no honest strategy does by accident
and `strategy.scan` can grep for. Weak keys, so a context's entry dies with it.
"""


def _runtime_of(ctx: "Context") -> Runtime:
    """The engine-facing accessor for a context's runtime. See `_RUNTIMES`."""
    return _RUNTIMES[ctx]


def _snapshot(value: Any) -> Any:
    """Detach an emitted payload from the strategy's own objects, at emit time.

    The event log is the reproducibility artefact of spec 12.1: it is hashed, written to
    disk, and offered as the description of what the run did. Both runtimes that store
    events (`DryRunRuntime.emit`, `EngineRuntime.emit`) copy only the *top level* of the
    payload -- `dict(payload)` -- so a nested dict a strategy passed to `ctx.log.info`
    stayed live inside the stored event. Mutating it afterwards rewrote history: the
    stored event changed, and the hash the manifest publishes then described a run that
    never happened. The snapshot has to happen at emit time, here, because "what was true
    when the strategy said it" is only knowable at the moment it was said.

    **Containers are copied deeply; leaves are kept by reference, and that split is the
    point.** The hash serialises payloads through `dryrun._canonical`, which renders
    containers by value (dicts and lists element-wise, sets sorted) but reduces every
    unrecognised object to its type name and every `Money` to its decimal string -- and
    `Money` is an immutable `Decimal` while a type name cannot change under mutation, so
    copying leaves would buy nothing. What it would *cost* is correctness of the copy
    itself: `copy.deepcopy` on an arbitrary logged object (an open file, a lock, the
    strategy instance) raises, and the full JSON round-trip destroys exactly the values
    the canonicaliser has rules for -- `json.dumps` has never heard of `Money`. So the
    JSON-shaped skeleton is rebuilt and everything else rides along by reference, which
    protects everything the hash can see. One stated residue: the JSONL disk write
    (`json.dumps(..., default=str)`) stringifies unrecognised leaves at *write* time, so
    a mutable non-container object's disk rendering can still trail its mutations --
    that rendering was already lossy and sits outside the hash, and refusing such values
    outright would turn a stray `ctx.log.info("x", obj=self)` into a run-ending error,
    the trade `_canonical` already declined.

    Tuples come back as tuples and sets as sets rather than flattened to lists: the
    canonicaliser treats them identically either way, but the stored payload is also what
    tests and the viewer compare against, and a copy that changes types is a copy that
    lies about what was logged.
    """
    if isinstance(value, Mapping):
        return {key: _snapshot(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_snapshot(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_snapshot(item) for item in value)
    if isinstance(value, (set, frozenset)):
        # Elements are hashable, hence not the containers rebuilt above; copying the set
        # itself is what stops a later `.add()` from editing the log.
        return set(value) if isinstance(value, set) else value
    return value


@dataclass(eq=False)
class Context:
    """The object every strategy hook receives.

    Constructed by the engine, once per run. Strategies never build one.

    **Sealed after construction.** `ctx.buy` during warm-up raises `WarmupViolation`, and
    for one release `ctx._warm = True` quietly did not: `Context` was a plain dataclass, so
    a strategy could flip the gate, restamp any field, or walk `ctx._runtime` into the live
    ledger. `__setattr__` now refuses every write once `__post_init__` finishes -- the
    engine's own mutators go through `object.__setattr__` -- and the runtime lives in a
    module-private side table (`_RUNTIMES`) rather than on the instance. Neither is a
    security boundary; both make the accidental version fail loudly at the line that did it.
    """

    _runtime: Runtime = field(repr=False)
    symbols: tuple[str, ...]
    timeframe: str
    indicators: IndicatorSet
    rng: random.Random
    _warm: bool = field(default=False, repr=False)
    _bars_seen: int = field(default=0, repr=False)
    _warmup_bars: int | None = field(default=None, repr=False)
    """The engine's warm-up gate in bars, for diagnostics. Set once warm-up is known."""

    def __post_init__(self) -> None:
        if not self.symbols:
            raise ValueError("a run needs at least one symbol")
        # **The two default-symbol resolvers must agree (L9).** `ctx.buy()` with no symbol
        # trades `symbols[0]`; `ctx.indicators.ema(20)` with no symbol feeds
        # `indicators.primary_symbol`. The two are set by two independent constructor
        # arguments, and `symbols=("ETHUSDT", "BTCUSDT")` against
        # `primary_symbol="BTCUSDT"` would trade ETH off a BTC EMA with no error anywhere.
        if self.indicators.primary_symbol != self.symbols[0]:
            raise ValueError(
                f"the context's default symbol {self.symbols[0]!r} and the indicator "
                f"set's primary symbol {self.indicators.primary_symbol!r} disagree. "
                "ctx.buy() with no symbol would trade one instrument off the other's "
                "indicators; construct both from the same symbol order."
            )
        unknown = sorted(set(self.indicators.symbols) - set(self.symbols))
        if unknown:
            raise ValueError(
                f"the indicator set is keyed over {unknown}, which this run does not "
                f"trade; declared symbols are {list(self.symbols)}."
            )
        # The strategy-facing handle delegates registration and reads but refuses
        # dispatch (H12): the engine keeps the set it constructed and drives that one
        # directly, so `ctx.indicators.on_bar(...)` from a hook fails loudly instead of
        # advancing every series off a bar the market never printed. See
        # `SealedIndicators` for why the block cannot live on `IndicatorSet` itself.
        self.indicators = SealedIndicators(self.indicators)
        _RUNTIMES[self] = self._runtime
        del self.__dict__["_runtime"]
        self.risk = RiskAPI(self)
        self.log = LogAPI(self)
        object.__setattr__(self, "_Context__sealed", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if self.__dict__.get("_Context__sealed"):
            raise AttributeError(
                f"ctx.{name} cannot be assigned: Context is sealed after construction "
                f"(spec 5.3). Strategy state belongs on the strategy object (self.*); the "
                f"engine's own fields move only through its private mutators. If this is "
                f"engine code, use the _set_* methods."
            )
        object.__setattr__(self, name, value)

    def __getattr__(self, name: str) -> Any:
        # Only reached for attributes that do not exist -- which `_runtime` deliberately
        # no longer does once construction completes (see `_RUNTIMES`).
        if name == "_runtime":
            raise AttributeError(
                "ctx._runtime is engine-private and is not reachable from a Context. "
                "Everything a strategy may do is on ctx's public surface -- orders via "
                "ctx.buy/sell/close, market state via ctx.mark/spread/book, account state "
                "via ctx.position/ctx.account (spec 5.3)."
            )
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    # ------------------------------------------------------------------ engine side

    def _set_warm(self, warm: bool) -> None:
        """Engine-only. Flips the warm-up gate once enough bars have been fed."""
        object.__setattr__(self, "_warm", warm)

    def _set_warmup_bars(self, bars: int) -> None:
        """Engine-only. Records the gate's size so a violation can name it."""
        object.__setattr__(self, "_warmup_bars", bars)

    def _note_bar(self) -> None:
        object.__setattr__(self, "_bars_seen", self._bars_seen + 1)

    def _emit(self, kind: str, payload: Mapping[str, Any]) -> None:
        # Snapshotted here, at the strategy boundary, not in the runtimes: every payload
        # that can contain strategy-owned mutable objects (LOG fields, RECORD values)
        # passes through this method, and the runtimes' own `dict(payload)` only copies
        # the top level. See `_snapshot` for why a live nested dict in the stored event
        # falsifies the run's reproducibility hash.
        _runtime_of(self).emit(kind, _snapshot(payload))

    def _resolve_symbol(self, symbol: str | None) -> str:
        if symbol is None:
            return self.symbols[0]
        if symbol not in self.symbols:
            raise ValueError(
                f"{symbol!r} is not in this run; declared symbols are "
                f"{list(self.symbols)}. Add it to requires['symbols'] so the engine can "
                "check its data coverage before the run starts."
            )
        return symbol

    def _require_warm(self, action: str) -> None:
        if not self._warm:
            # The denominator is the *engine's* gate, `max(indicator warm-up,
            # requires["history"])`, not the indicator set's alone. Reporting the latter
            # produced "1 of 0 bars fed" for a strategy declaring `history: 50` and no
            # indicators -- telling the author they need zero bars while refusing their
            # order, and blaming indicators that do not exist.
            needed = self._warmup_bars if self._warmup_bars is not None else self.indicators.warmup
            because = (
                "Indicators are still filling"
                if self.indicators.warmup >= needed and needed > 0
                else "requires['history'] asks for that many bars"
            )
            raise WarmupViolation(
                f"{action} is blocked during warm-up: {self._bars_seen} of {needed} bars "
                f"fed. {because}, so any signal now is computed from a shorter window than "
                "the strategy declared (spec 5.2). Guard the hook with "
                "`if not ctx.warm: return`."
            )

    # -------------------------------------------------------------------- read-only

    @property
    def now(self) -> int:
        """The engine's clock, epoch ms UTC. Never wall clock (spec 5.3)."""
        return _runtime_of(self).now_ms

    @property
    def warm(self) -> bool:
        return self._warm

    @property
    def bars_seen(self) -> int:
        return self._bars_seen

    @property
    def fill_tier(self) -> FillTier:
        return _runtime_of(self).fill_tier

    @property
    def account(self) -> AccountView:
        return _runtime_of(self).account_view()

    @property
    def hedge_mode(self) -> bool:
        """Whether this run holds a long **and** a short position per symbol.

        A strategy that wants to work in both modes branches on this. One that only makes
        sense in one of them should say so in `requires` rather than discover it at the
        first order.
        """
        return _runtime_of(self).hedge_mode

    def position(
        self,
        symbol: str | None = None,
        position_side: PositionSide | str | None = None,
    ) -> PositionView:
        """The position on `symbol`, or -- in hedge mode -- the named side of it.

        ```python
        if ctx.hedge_mode:
            long = ctx.position(position_side="LONG")
            short = ctx.position(position_side="SHORT")
        else:
            pos = ctx.position()
        ```

        **Refused rather than guessed in hedge mode when no side is given.** A symbol holds
        two positions there, with two entry prices and two liquidation prices, and returning
        either of them for `ctx.position()` would make a strategy written for one-way mode
        run silently against half its book. `ctx.positions()` returns both.
        """
        target = self._resolve_symbol(symbol)
        return _runtime_of(self).position_view(target, self._resolve_side(target, position_side))

    def positions(self, symbol: str | None = None) -> tuple[PositionView, ...]:
        """Every side of `symbol`, longs first. One view in one-way mode, two in hedge mode.

        Always returns a view per addressable side, including flat ones, so a strategy can
        iterate without a length check and `is_flat` means what it says.
        """
        target = self._resolve_symbol(symbol)
        sides = (
            (PositionSide.LONG, PositionSide.SHORT)
            if _runtime_of(self).hedge_mode
            else (PositionSide.BOTH,)
        )
        return tuple(_runtime_of(self).position_view(target, side) for side in sides)

    def _resolve_side(
        self, symbol: str, position_side: PositionSide | str | None
    ) -> PositionSide:
        """Validate a strategy-supplied side against this run's position mode.

        Accepts the string spellings so a strategy can write `"LONG"` without importing an
        enum -- the same courtesy `tif` and `working_type` already get -- and rejects an
        unknown one by name rather than letting it fall through as `BOTH`, which would route
        the order to a position that does not exist in hedge mode.
        """
        if isinstance(position_side, str):
            try:
                position_side = PositionSide(position_side.strip().upper())
            except ValueError:
                raise ValueError(
                    f"unknown position side {position_side!r}; expected one of "
                    f"{[s.value for s in PositionSide]}"
                ) from None
        if _runtime_of(self).hedge_mode:
            if position_side is None or position_side is PositionSide.BOTH:
                raise ValueError(
                    f"{symbol}: this run is in hedge mode, so it holds a LONG and a SHORT "
                    "position on this symbol at once. Say which one: "
                    'position_side="LONG" or "SHORT". ctx.positions() returns both.'
                )
            return position_side
        if position_side is not None and position_side is not PositionSide.BOTH:
            raise ValueError(
                f"{symbol}: this run is in one-way mode, where a symbol has a single "
                f"position; {position_side.value} has no meaning here. Start the run with "
                "hedge mode on to address sides separately."
            )
        return PositionSide.BOTH

    def mark(self, symbol: str | None = None) -> Money:
        return _runtime_of(self).mark_price(self._resolve_symbol(symbol))

    def funding(self, symbol: str | None = None) -> FundingView:
        return _runtime_of(self).funding_view(self._resolve_symbol(symbol))

    def oi(self, symbol: str | None = None) -> float | None:
        return _runtime_of(self).open_interest(self._resolve_symbol(symbol))

    def leverage(self, symbol: str | None = None) -> int:
        """This symbol's current leverage.

        The run starts at the number chosen on the form, and returns whatever
        `ctx.set_leverage` last applied after that. **Per symbol, with no `position_side`
        argument by design** -- `POST /fapi/v1/leverage` has no such field, so a hedge's two
        legs cannot hold different leverages at the exchange and must not appear to here.

        While a position is open this reports the leverage that position *opened* at, which
        is the one its margin and liquidation price are computed from -- not a pending
        default that would only apply to the next one.
        """
        return _runtime_of(self).leverage(self._resolve_symbol(symbol))

    def set_leverage(self, leverage: int, symbol: str | None = None) -> None:
        """Set the leverage the symbol's **next** position opens at (spec 3.2).

        This is how a strategy takes leverage out of the form's hands. Leave it alone and
        the run uses the number chosen when it was started; call this and the strategy's
        number wins from that point on. Typically once in `on_start`:

        ```python
        def on_start(self, ctx):
            ctx.set_leverage(3)
        ```

        ...or per regime, which is the case the form cannot express:

        ```python
        def on_bar(self, ctx, bar):
            if ctx.position().is_flat:
                ctx.set_leverage(2 if self.atr.value > self.calm else 5)
        ```

        **Refused while a position is open on that symbol -- either side, in hedge mode.**
        Binance permits it, but the margin re-resolution has edge cases the ledger declines
        to model on guesswork (see `Account.set_leverage`), so the guard is real rather than
        conservative: an open position keeps the leverage it opened at, and `ctx.leverage()`
        keeps reporting that. Check `ctx.position().is_flat` first, or call it in `on_start`
        before anything can be open.

        **Refused outright in live mode**, where leverage is state at the venue rather than
        in this ledger and is applied once, before the first order. The error names the
        session form. Backtest and paper apply it exactly.

        Also refused below 1x and above the symbol's highest bracket -- the same ceilings the
        exchange enforces, checked here so the refusal arrives before an order does.

        Not counted as a warm-up violation: it takes no position and reads no indicator, so
        `on_start` -- where it is most useful -- is a legal place to call it.
        """
        if isinstance(leverage, bool) or not isinstance(leverage, int):
            # `True` is an `int` in Python and would silently mean 1x. A strategy that
            # passed a bool meant something else entirely.
            raise TypeError(
                f"ctx.set_leverage() takes a whole number of times, got "
                f"{type(leverage).__name__}. Binance's leverage is an integer multiplier."
            )
        target = self._resolve_symbol(symbol)
        try:
            _runtime_of(self).set_leverage(target, leverage)
        except ValueError as exc:
            # `Account.set_leverage`'s messages already name the symbol and the reason; the
            # only thing missing is which call the author has to go and look at.
            raise ValueError(f"ctx.set_leverage(): {exc}") from exc

    def book(self, symbol: str | None = None) -> DepthSnapshot:
        """The latest depth snapshot. Raises below the `BOOK_WALK` tier (spec 5.3).

        Raising rather than returning `None` is deliberate. A run whose range predates
        depth coverage degrades to `BOOK_TICKER` automatically (spec 4.2), and a strategy
        written against the book would then take a `None` branch it never intended and
        produce results that look like a strategy rather than like a misconfiguration.
        """
        target = self._resolve_symbol(symbol)
        tier = _runtime_of(self).fill_tier
        if tier < FillTier.BOOK_WALK:
            raise DataUnavailable(
                f"ctx.book() needs the BOOK_WALK fill tier; this run is at {tier.name}. "
                "Depth history exists only from the moment the collector started "
                "recording (spec 4.2), so a range extending before that cannot walk the "
                "book. Use ctx.spread() or shorten the range."
            )
        snapshot = _runtime_of(self).depth(target)
        if snapshot is None:
            raise DataUnavailable(
                f"no depth snapshot for {target} at {self.now}; the run is at BOOK_WALK "
                "but this instant has no coverage"
            )
        return snapshot

    def spread(self, symbol: str | None = None) -> SpreadView | None:
        return _runtime_of(self).spread(self._resolve_symbol(symbol))

    def macro(self, name: str) -> MacroView | None:
        """A macro/cross-asset series as of *now*, or `None` if nothing has published yet.

        ```python
        dxy = ctx.macro("dxy")
        if dxy is not None and dxy.age_ms < 6 * 3600_000:
            ...  # act on a reading no older than six hours
        ```

        **Optional by design (Phase 11).** Macro data is a signal input, not a dependency:
        a strategy that reads it must still run when the lake holds none, so this returns
        `None` rather than aborting. The absence is not silent — the run carries a
        `MACRO_MISSING` flag and a warning naming the series, so nobody reads a flat
        result as "the signal said nothing" when the truth is "there was no signal".

        **`age_ms` is the point of returning a view rather than a float.** DXY does not
        print at the weekend and CoinGecko's snapshot can stall; last-observation-carried-
        forward is the only honest read of a level series (spec 3.4's convention), but
        LOCF without an age silently presents Friday's dollar as Sunday's. The age is the
        caller's to judge, which is why it is handed over rather than thresholded here.

        Declaring the dataset in `requires["datasets"]` is what makes the engine load it;
        calling this without declaring raises rather than returning `None`, because a
        `None` that means "you forgot to ask for it" is indistinguishable from one that
        means "the market published nothing".
        """
        if not isinstance(name, str) or not name:
            raise ValueError("ctx.macro() needs a series name, e.g. 'dxy'")
        return _runtime_of(self).macro(name)

    def money(self, value: float | int | str) -> Money:
        """Cross an indicator value into an exact quantity usable as a price or size.

        Indicators are `float64` and orders are `Decimal` (spec 3.1), so this crossing has
        to happen somewhere. Naming it means it happens once, visibly, instead of being
        improvised in every strategy that wants to place a stop `2 * ATR` below the mark.

        A `float` argument is quantised to eight decimals -- the storage seam's precision --
        and that is a **one-way, lossy** step by design. The float was already inexact; what
        this refuses to do is carry seventeen digits of binary-floating-point residue into a
        ledger whose invariants are stated without an epsilon (spec 3.10). A `str` is parsed
        exactly and is the right form for a literal.

        **Floats beyond 2^53 are refused outright.** Quantising the fraction only helps
        below the point where the *integer part* is exact; past 2^53 a float's spacing
        exceeds 1, so `123456789012345678.0` arrives as `...680` and the eight-decimal
        format faithfully preserves a number the author never typed -- binary residue in
        the digits the ledger trusts most. No price or size on this venue is within nine
        orders of magnitude of the limit, so any such value is an upstream unit error, and
        an error message beats a ledger entry. An `int` of any size stays exact and is
        accepted; a genuinely huge quantity belongs in a string.
        """
        if isinstance(value, str):
            return parse_money(value)
        if isinstance(value, bool):
            raise TypeError("ctx.money() takes a number or a decimal string, not a bool")
        if isinstance(value, int):
            return parse_money(str(value))
        if isinstance(value, float):
            if value != value or value in (float("inf"), float("-inf")):
                raise ValueError(f"ctx.money() cannot represent {value}")
            if value >= 9007199254740992.0 or value <= -9007199254740992.0:
                # 2^53: the largest magnitude at which every integer is representable.
                raise ValueError(
                    f"ctx.money() refuses floats at or beyond 2^53 ({value!r}): the "
                    "integer part already carries binary rounding, so the exact-looking "
                    "result would be a number the author never wrote. Pass the value as a "
                    "decimal string if it is genuinely this large."
                )
            return parse_money(f"{value:.8f}")
        raise TypeError(
            f"ctx.money() takes a number or a decimal string, got {type(value).__name__}"
        )

    def record(self, name: str, value: float | int) -> None:
        """Record a custom time series, charted on the results page (spec 5.3).

        Numbers only. An arbitrary object here would have to be serialised into the event
        log and hashed for the reproducibility check, and a value whose `repr` includes a
        memory address makes two identical runs disagree.

        **Finite numbers only, same as `ctx.money` just above.** NaN and the infinities
        are floats, so the type check alone let them into the one log spec 12.1 requires
        two identical runs to agree on -- where they are poison three ways: `json.dumps`
        writes them as `NaN`/`Infinity`, tokens that are not JSON at all, so the JSONL
        event log stops parsing in any strict reader including the run viewer;
        `nan != nan`, so a stored event stops even equalling itself and every equality
        over the log silently fails; and a NaN on a chart is a hole that looks like
        missing data rather than like the bug that produced it.
        """
        if not isinstance(name, str) or not name:
            raise ValueError("record() needs a non-empty series name")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(
                f"record({name!r}) takes a number, got {type(value).__name__}; series are "
                "charted and hashed into the run's event log"
            )
        if isinstance(value, float) and (
            value != value or value in (float("inf"), float("-inf"))
        ):
            raise ValueError(
                f"record({name!r}) cannot represent {value}: series are charted and "
                "hashed into the run's event log, and NaN/Infinity serialise to tokens "
                "that are not valid JSON. Skip the call on bars where the series has no "
                "finite value, or record a finite sentinel you chose deliberately."
            )
        self._emit("RECORD", {"name": name, "value": float(value)})

    # ---------------------------------------------------------------------- orders

    def _order(
        self,
        *,
        symbol: str | None,
        side: str,
        qty: Money,
        type: OrderType,
        price: Money | None = None,
        stop_price: Money | None = None,
        callback_rate: Money | None = None,
        tif: TimeInForce | str = TimeInForce.GTC,
        working_type: WorkingType | str = WorkingType.MARK_PRICE,
        reduce_only: bool = False,
        position_side: PositionSide | str | None = None,
        client_id: str | None = None,
        tag: str | None = None,
        action: str,
    ) -> str:
        self._require_warm(action)
        target = self._resolve_symbol(symbol)
        routed = self._resolve_side(target, position_side)
        if isinstance(tif, str):
            try:
                tif = TimeInForce(tif)
            except ValueError:
                raise ValueError(
                    f"unknown time in force {tif!r}; expected one of "
                    f"{[t.value for t in TimeInForce]}"
                ) from None
        if isinstance(working_type, str):
            try:
                working_type = WorkingType(working_type)
            except ValueError:
                raise ValueError(
                    f"unknown working type {working_type!r}; expected one of "
                    f"{[w.value for w in WorkingType]}"
                ) from None
        if qty <= 0:
            raise ValueError(
                f"{action} needs a positive quantity, got {qty}. A size of zero usually "
                "means ctx.risk sizing floored to the step size — check it before "
                "submitting rather than sending an empty order."
            )
        if type is OrderType.LIMIT and price is None:
            raise ValueError("a LIMIT order needs a price")
        if type is not OrderType.LIMIT and price is not None:
            raise ValueError(f"a {type.value} order does not take a price")
        if type in (OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET) and stop_price is None:
            raise ValueError(f"a {type.value} order needs a stop_price")
        if type is OrderType.TRAILING_STOP_MARKET and callback_rate is None:
            raise ValueError("a TRAILING_STOP_MARKET order needs a callback_rate")
        if stop_price is not None and stop_price <= 0:
            raise ValueError(f"stop_price must be positive, got {stop_price}")
        if price is not None and price <= 0:
            raise ValueError(f"price must be positive, got {price}")
        if callback_rate is not None and not (0 < callback_rate < 1):
            # A *fraction*, not a percent. Catching this here rather than letting a
            # `callback_rate=1.0` through is the difference between an error and a trailing
            # stop parked at zero -- which never fires, so the strategy looks like one that
            # simply never got stopped out.
            raise ValueError(
                f"callback_rate is a fraction and must be in (0, 1); got {callback_rate}. "
                "Binance's own field is in percent, so their 1.0 is 0.01 here."
            )
        intent = OrderIntent(
            symbol=target,
            side=side,
            qty=qty,
            type=type,
            price=price,
            stop_price=stop_price,
            callback_rate=callback_rate,
            tif=tif,
            working_type=working_type,
            reduce_only=reduce_only,
            position_side=routed,
            client_id=client_id,
            tag=tag,
        )
        return _runtime_of(self).submit(intent)

    def buy(
        self,
        symbol: str | None = None,
        *,
        qty: Money,
        type: OrderType | str = OrderType.MARKET,
        price: Money | None = None,
        tif: TimeInForce | str = TimeInForce.GTC,
        reduce_only: bool = False,
        position_side: PositionSide | str | None = None,
        client_id: str | None = None,
        tag: str | None = None,
    ) -> str:
        """Buy. In hedge mode, `position_side` is required and decides what a buy *means*.

        `position_side="LONG"` opens or increases the long. `position_side="SHORT"` reduces
        the short. Those are different orders with the same side and quantity, which is why
        nothing here picks one for you.
        """
        return self._order(
            symbol=symbol,
            side="BUY",
            qty=qty,
            type=_as_order_type(type),
            price=price,
            tif=tif,
            reduce_only=reduce_only,
            position_side=position_side,
            client_id=client_id,
            tag=tag,
            action="ctx.buy()",
        )

    def sell(
        self,
        symbol: str | None = None,
        *,
        qty: Money,
        type: OrderType | str = OrderType.MARKET,
        price: Money | None = None,
        tif: TimeInForce | str = TimeInForce.GTC,
        reduce_only: bool = False,
        position_side: PositionSide | str | None = None,
        client_id: str | None = None,
        tag: str | None = None,
    ) -> str:
        """Sell. In hedge mode, `position_side` is required and decides what a sell *means*.

        `position_side="SHORT"` opens or increases the short. `position_side="LONG"` reduces
        the long, and cannot exceed it -- the ledger and the exchange both refuse a sell
        that would drive the long side through zero rather than flipping it.
        """
        return self._order(
            symbol=symbol,
            side="SELL",
            qty=qty,
            type=_as_order_type(type),
            price=price,
            tif=tif,
            reduce_only=reduce_only,
            position_side=position_side,
            client_id=client_id,
            tag=tag,
            action="ctx.sell()",
        )

    def close(
        self,
        symbol: str | None = None,
        qty: Money | None = None,
        *,
        position_side: PositionSide | str | None = None,
        tag: str | None = None,
    ) -> str | None:
        """Reduce-only market order against the current position (spec 5.3).

        Returns `None` when the position is already flat, rather than submitting a
        zero-quantity order. Closing a flat position is a no-op the strategy plainly meant,
        not an error worth halting a run for.

        **In hedge mode, `position_side` says which leg to close** and is required. There is
        no reading of `ctx.close()` on a hedged symbol that is not a guess: closing "the"
        position could mean the long, the short, or both, and the three leave the account
        holding entirely different risk. `ctx.close_all()` is the explicit form of "both".
        """
        self._require_warm("ctx.close()")
        target = self._resolve_symbol(symbol)
        routed = self._resolve_side(target, position_side)
        position = _runtime_of(self).position_view(target, routed)
        if position.qty == 0:
            return None
        # Reduce-only is not enough on its own: submitting more than the position holds
        # would be clamped by the exchange in live and would need clamping in backtest
        # too, and the two clamps are exactly the sort of thing that ends up differing.
        # Clamping here means both paths see the same quantity.
        size = abs(position.qty) if qty is None else min(abs(qty), abs(position.qty))
        if size <= 0:
            return None
        return self._order(
            symbol=target,
            side="SELL" if position.qty > 0 else "BUY",
            qty=size,
            type=OrderType.MARKET,
            # A hedged side rejects `reduce_only` (see `OrderIntent.__post_init__`), and
            # does not need it: a sell routed to the LONG side can only reduce it.
            reduce_only=not routed.is_hedged,
            position_side=routed,
            tag=tag,
            action="ctx.close()",
        )

    def close_all(
        self, symbol: str | None = None, *, tag: str | None = None
    ) -> tuple[str, ...]:
        """Close every open side of `symbol`. One order in one-way mode, up to two in hedge.

        The explicit form of "flatten this symbol", so that `ctx.close()` never has to guess
        which leg was meant. Returns the ids of the orders actually submitted, so an empty
        tuple means the symbol was already flat on every side.
        """
        target = self._resolve_symbol(symbol)
        sides: tuple[PositionSide | None, ...] = (
            (PositionSide.LONG, PositionSide.SHORT)
            if _runtime_of(self).hedge_mode
            else (None,)
        )
        submitted = [self.close(target, position_side=side, tag=tag) for side in sides]
        return tuple(order_id for order_id in submitted if order_id is not None)

    def stop_loss(
        self,
        symbol: str | None = None,
        *,
        stop_price: Money,
        qty: Money | None = None,
        working_type: WorkingType | str = WorkingType.MARK_PRICE,
        position_side: PositionSide | str | None = None,
        tag: str | None = None,
    ) -> str | None:
        """Reduce-only stop, triggered on **mark price** by default (spec 6.4).

        On trigger it becomes a market order and takes the market-order path -- latency,
        slippage and all. A stop is not a guaranteed price, and a backtest that filled one
        at its trigger level would understate exactly the cost the stop exists to bound.

        **In hedge mode, `position_side` says which leg this protects and is required** --
        the same rule as `ctx.close()`, and for the same reason: a symbol holds two
        positions there, a stop on the long is a SELL and a stop on the short is a BUY,
        and nothing about a stop price says which was meant. Without the parameter the
        three protective helpers were unusable in hedge mode at all: side resolution
        raised before any order existed.
        """
        return self._protective(
            symbol=symbol,
            stop_price=stop_price,
            qty=qty,
            type=OrderType.STOP_MARKET,
            working_type=working_type,
            position_side=position_side,
            tag=tag,
            action="ctx.stop_loss()",
        )

    def take_profit(
        self,
        symbol: str | None = None,
        *,
        stop_price: Money,
        qty: Money | None = None,
        working_type: WorkingType | str = WorkingType.MARK_PRICE,
        position_side: PositionSide | str | None = None,
        tag: str | None = None,
    ) -> str | None:
        """`ctx.stop_loss`'s mirror. In hedge mode `position_side` names the protected leg."""
        return self._protective(
            symbol=symbol,
            stop_price=stop_price,
            qty=qty,
            type=OrderType.TAKE_PROFIT_MARKET,
            working_type=working_type,
            position_side=position_side,
            tag=tag,
            action="ctx.take_profit()",
        )

    def trailing_stop(
        self,
        symbol: str | None = None,
        *,
        callback_rate: Money,
        qty: Money | None = None,
        working_type: WorkingType | str = WorkingType.MARK_PRICE,
        position_side: PositionSide | str | None = None,
        tag: str | None = None,
    ) -> str | None:
        """Trailing stop at `callback_rate` retracement from the extreme (spec 6.4).

        `callback_rate` is a **fraction**: 0.01 is a 1% callback, which is Binance's own
        `callbackRate=1.0`. In hedge mode `position_side` names the protected leg, as on
        `ctx.stop_loss`.
        """
        if callback_rate <= 0:
            raise ValueError(f"callback_rate must be positive, got {callback_rate}")
        return self._protective(
            symbol=symbol,
            stop_price=None,
            callback_rate=callback_rate,
            qty=qty,
            type=OrderType.TRAILING_STOP_MARKET,
            working_type=working_type,
            position_side=position_side,
            tag=tag,
            action="ctx.trailing_stop()",
        )

    def _protective(
        self,
        *,
        symbol: str | None,
        type: OrderType,
        stop_price: Money | None = None,
        callback_rate: Money | None = None,
        qty: Money | None = None,
        working_type: WorkingType | str = WorkingType.MARK_PRICE,
        position_side: PositionSide | str | None = None,
        tag: str | None = None,
        action: str,
    ) -> str | None:
        self._require_warm(action)
        target = self._resolve_symbol(symbol)
        routed = self._resolve_side(target, position_side)
        position = _runtime_of(self).position_view(target, routed)
        if position.qty == 0:
            # A protective order needs a side, and the side comes from the position it
            # protects. With no position there is nothing to infer it from, and guessing
            # would place a reduce-only order the exchange rejects in live while the
            # backtest quietly held it forever.
            raise ValueError(
                f"{action} protects an open position and {target}"
                f"{'' if not routed.is_hedged else ' ' + routed.value} is flat. Submit the "
                "entry and let its fill land -- an order submitted in this hook is still "
                "in flight -- then attach the stop."
            )
        size = abs(position.qty) if qty is None else min(abs(qty), abs(position.qty))
        if size <= 0:
            raise ValueError(f"{action} needs a positive quantity, got {qty}")
        return self._order(
            symbol=target,
            side="SELL" if position.qty > 0 else "BUY",
            qty=size,
            type=type,
            stop_price=stop_price,
            callback_rate=callback_rate,
            working_type=working_type,
            reduce_only=not routed.is_hedged,
            position_side=routed,
            tag=tag,
            action=action,
        )

    def cancel(self, order_id: str) -> None:
        """Cancel a working order. **Deliberately not warm-gated**, unlike every entry.

        The warm-up gate exists so no *position-taking decision* is made off a shorter
        window than the strategy declared (spec 5.2). A cancel takes no position -- it can
        only remove risk -- and there is a real case where one must work before warm-up
        completes: a live session that adopts working orders left by reconciliation has to
        be able to pull them the moment `on_start` sees them, warm or not.
        `ctx.cancel_all()` and `ctx.open_orders()` are ungated on the same argument;
        `ctx.modify()` is gated, and its docstring says why the asymmetry is the point.
        """
        _runtime_of(self).cancel(order_id)

    def modify(
        self,
        order_id: str,
        *,
        price: Money | None = None,
        qty: Money | None = None,
    ) -> None:
        """Amend a resting limit order's price and/or total size.

        `qty` is the order's **new total**, not the extra amount and not the new remainder --
        the same field Binance's own endpoint takes. An order for 1.0 BTC that has filled
        0.4 and is amended to `qty=0.6` has 0.2 left to work.

        Two things this deliberately does not hide:

        - **It takes latency.** The amendment is an instruction crossing the wire, so a fill
          that lands inside its flight time fills at the *old* price and the old size. A
          strategy that reprices on every tick does not get a quote that is never stale.
        - **It usually costs queue position.** Moving the price or increasing the size sends
          the order to the back, which is what the exchange does. Only a strict *decrease* in
          size keeps the place in the queue.

        Cancel-and-resubmit remains available and is not equivalent: it always loses queue
        position, and it opens a window in which the strategy has no order in the market at
        all. Which of the two is right is a strategy decision, and it can only be made if the
        platform models the difference.

        **Warm-gated, although `ctx.cancel` is not, and the asymmetry is deliberate.** A
        cancel can only remove risk. An amendment re-*prices* or re-*sizes* a working
        order -- it is an order-shaped decision, taken off whatever window the indicators
        currently hold -- and during warm-up that window is shorter than the strategy
        declared, which is exactly what the gate exists to forbid (spec 5.2). A strategy
        that needs a not-yet-warm order gone reshapes it the ungated way: cancel it.
        """
        if price is None and qty is None:
            raise ValueError(
                "ctx.modify needs price=, qty=, or both. Calling it with neither is "
                "probably a typo for ctx.cancel()."
            )
        if price is not None and price <= 0:
            raise ValueError(f"price must be positive, got {price}")
        if qty is not None and qty <= 0:
            raise ValueError(
                f"modify needs a positive quantity, got {qty}. To remove the order "
                "entirely, use ctx.cancel()."
            )
        self._require_warm("modify")
        _runtime_of(self).modify(order_id, price, qty)

    def cancel_all(self, symbol: str | None = None) -> None:
        """Cancel every working order, or one symbol's. Ungated -- see `ctx.cancel`."""
        _runtime_of(self).cancel_all(None if symbol is None else self._resolve_symbol(symbol))

    def open_orders(self, symbol: str | None = None) -> tuple[str, ...]:
        """Ids of every open order. A read, so warm-up has nothing to protect here."""
        return tuple(
            _runtime_of(self).open_order_ids(
                None if symbol is None else self._resolve_symbol(symbol)
            )
        )

    def tick_size(self, symbol: str | None = None) -> Money:
        """The symbol's price grid, for quantising an order price (spec 3.2).

        The companion to `SpreadView.mid_price`: a strategy that derives a price -- a
        midpoint, an ATR offset -- has to land it on the venue's tick or the order is
        refused by `PRICE_FILTER`, and until this existed the grid was not readable from
        strategy code at all. Raises `DataUnavailable` on a runtime that carries no
        filters (the smoke run's synthetic market), rather than inventing a grid.
        """
        target = self._resolve_symbol(symbol)
        runtime = _runtime_of(self)
        getter = getattr(runtime, "tick_size", None)
        if getter is None:
            raise DataUnavailable(
                "this execution mode carries no exchange filters, so the tick grid is "
                "unknown here. Quantise against a stated tick in the strategy's params if "
                "the smoke run needs one."
            )
        return getter(target)


def _as_order_type(value: OrderType | str) -> OrderType:
    if isinstance(value, OrderType):
        return value
    try:
        return OrderType(value)
    except ValueError:
        raise ValueError(
            f"unknown order type {value!r}; expected one of "
            f"{[t.value for t in OrderType]}"
        ) from None
