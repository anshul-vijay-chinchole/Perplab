"""What every execution mode shares (spec 6.1).

Spec 6.1's rule: *"if a piece of logic could live in the shared core, it must."* Most of that
core already exists -- `core.account` is the ledger for all three modes, `core.margin` the
bracket arithmetic, `core.funding` the settlement formula. What is left over, and lives
here, is the part that sits between a strategy's `ctx.buy()` and the ledger's `apply_fill`:
the order's identity, its state machine, and the read-only account surface the strategy sees.

**`EngineRuntime` is the only implementation of `strategy.context.Runtime` that a real
engine uses.** Backtest, paper and live differ in where fills come from and what drives the
clock; they do not differ in what `ctx.position()` means. Giving each mode its own runtime
is exactly the duplication spec 14-I8 was about -- the two copies agree on the day they are
written and disagree by the time either is interesting.

**Cancels have latency, and that is modelled rather than assumed away.** Spec 6.3: *"A
cancel issued at T does not protect you from a fill at T + 50 ms if cancel latency is
120 ms."* So a cancel is **scheduled**, not applied: it takes its own latency to reach the
exchange, and anything that fills the order in the meantime fills it. At the `BAR_CLOSE`
tier a strategy could never win that race at all -- nothing rested, and nothing happened
between submission and arrival -- so the modelling was correct and invisible. With resting
orders and a tick tape it is neither: an order can be filled by a print that lands inside the
cancel's own flight time, which is R19's whole point and a real source of backtest optimism
wherever it is skipped. `backtest._cancel` is where it happens.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Sequence

from perplab.core.account import Account
from perplab.core.money import Money, from_scaled
from perplab.core.types import DepthSnapshot, PositionSide
from perplab.exchange.filters import SymbolFilters
from perplab.strategy.context import (
    AccountView,
    DataUnavailable,
    FillTier,
    FundingView,
    MacroView,
    OrderIntent,
    PositionView,
    SpreadView,
    StrategyEvent,
)

__all__ = [
    "OrderStatus",
    "Order",
    "EventLogFull",
    "EngineRuntime",
    "MAX_EVENTS",
]

MAX_EVENTS = 2_000_000
"""Ceiling on the strategy event log.

Not a performance guard -- it is a guard against an artefact nobody can use. The log is
hashed for spec 12.1, written to disk, and rendered in a virtualised viewer; a strategy
calling `ctx.record()` twice per bar over a year of 1-minute data produces about a million
entries, and one that logs inside a tick loop would produce hundreds of millions. The run
stops with a named reason rather than filling the disk and being killed by the OS, which is
the same failure with no explanation attached.
"""


class EventLogFull(RuntimeError):
    """The strategy emitted more events than a run can meaningfully carry."""


class OrderStatus(Enum):
    PENDING = "PENDING"
    """Submitted; the latency window has not elapsed and the exchange has not seen it."""

    WORKING = "WORKING"
    """Live at the matching engine: a resting limit, an armed stop, or a market order
    waiting for the next print at the `TRADE_ONLY` tier.

    Distinct from `PENDING` because the two differ in exactly the way spec 6.3's cancel race
    is about. A `PENDING` order can still be beaten by a cancel; a `WORKING` one is already
    there, and cancelling it takes another latency window during which it can fill.
    Partially-filled orders stay `WORKING` -- `filled_qty` says how much has gone through.
    """

    FILLED = "FILLED"
    CANCELLED = "CANCELLED"

    EXPIRED = "EXPIRED"
    """Removed by its own time-in-force rather than by anyone's decision.

    An `IOC` remainder, a `FOK` that could not fill in full, a post-only (`GTX`) order that
    would have crossed, or a `TRADE_ONLY` market order that waited past the deadline without
    a print. Separated from `CANCELLED` because a post-only order silently failing to enter
    is spec 6.5's named hazard -- *"it is also how orders silently fail to enter"* -- and
    counting it as a cancel the strategy asked for is precisely how it stays silent.
    """

    REJECTED = "REJECTED"
    """Refused at arrival -- by an exchange filter, or by the ledger for want of margin.

    Distinct from `CANCELLED` because the two mean opposite things about the strategy: a
    cancel is something it asked for, a rejection is something it did not anticipate. A
    run with rejections is a run whose sizing is wrong, and collapsing them would hide that.
    """


_OPEN_STATUSES = frozenset({OrderStatus.PENDING, OrderStatus.WORKING})


@dataclass
class Order:
    """One order, from submission to resolution.

    Mutable, unlike almost everything else in this codebase, because an order genuinely has
    a lifecycle and the alternative -- replacing it in a dict on every transition -- makes
    the `PENDING` list and the order itself two things that can disagree.

    **All quantities that the resting book works with are scaled int64, not `Decimal`.** The
    queue model compares our remaining size against trade sizes and book sizes, all of which
    arrive from the lake as scaled integers, and converting each of them to `Decimal` on
    every tick would put the accounting seam on the hottest path in the engine (spec 3.1).
    The conversion happens once, where a fill crosses into the ledger.
    """

    id: str
    intent: OrderIntent
    submit_ts: int
    arrival_ts: int
    reference_price: Money
    """The price at *signal* time.

    Spec 8.4 measures slippage against the price when the decision was made, not against
    the print the fill was taken from, so this has to be captured at submission and carried
    to the fill. The difference between the two is the latency drift, which is a real
    execution cost and the reason a zero-latency backtest looks better than it is.

    **Re-stamped when a stop triggers.** For a protective order the decision is not the
    moment the stop was attached -- which may be days earlier -- but the moment it fired, so
    the reference becomes the price prevailing at the trigger. Leaving the original would
    report the whole move from entry to stop as execution slippage.
    """
    status: OrderStatus = OrderStatus.PENDING
    reason: str = ""
    filled_qty: Money | None = None
    filled_price: Money | None = None
    """Volume-weighted across every increment, for the order-level record. Individual
    increments are reported as their own `FILL` events."""

    # --------------------------------------------------------------- resting-book state
    remaining: int = 0
    """Unfilled quantity, scaled. Set when the order reaches the exchange."""

    filled_scaled: int = 0
    """Filled quantity, scaled -- the integer twin of `filled_qty`, kept so the queue model
    never has to cross the seam to ask how much is left."""

    filled_cost: Money | None = None
    """Running `sum(qty x price)` over increments, for the volume-weighted `filled_price`."""

    limit_scaled: int = 0
    """The resting price, scaled. Zero for orders that never rest."""

    queue_ahead: int | None = None
    """Resting size believed to be ahead of us at `limit_scaled`, scaled. `None` until the
    level is first observed. See `resting.RestingBook` for why it only ever decreases."""

    triggered: bool = False
    """Whether a stop/take-profit/trailing order has fired and become a market order."""

    trail_extreme: Money | None = None
    """Best price seen since the trailing stop was armed. See `resting.RestingBook`."""

    trigger_price: Money | None = None
    """The resolved trigger level. For a trailing stop this moves with `trail_extreme`."""

    @property
    def is_open(self) -> bool:
        return self.status in _OPEN_STATUSES

    @property
    def is_resting(self) -> bool:
        """Live at the exchange -- as opposed to still crossing the wire."""
        return self.status is OrderStatus.WORKING


@dataclass
class EngineRuntime:
    """The `Runtime` a real engine gives to `Context`.

    Everything read-only is derived from `Account` on demand rather than cached. A cached
    account view is a view that can be stale by exactly one event, and the events most
    likely to invalidate it -- a fill, a funding settlement, a liquidation -- are the ones a
    strategy is most likely to be reacting to when it reads.
    """

    account: Account
    symbols: tuple[str, ...]
    filters: dict[str, SymbolFilters]
    tier: FillTier
    submit_hook: Callable[[OrderIntent], str]
    cancel_hook: Callable[[str], None]
    cancel_all_hook: Callable[[str | None], None]
    open_orders_hook: Callable[[str | None], Sequence[str]]
    modify_hook: Callable[[str, Money | None, Money | None], None] | None = None
    """Amend a resting limit order. `None` where the mode has no amend endpoint.

    Optional rather than required because `EngineRuntime` is constructed by three modes and
    a keyword-only field with no default would break every existing call site to add a
    capability two of them do not have yet. `modify` raises rather than silently doing
    nothing when it is absent -- a strategy whose reprice quietly no-ops is a strategy
    quoting a stale price it believes it moved."""

    leverage_hook: Callable[[str, int], None] | None = None
    """Apply a strategy-requested leverage change, or `None` where the mode cannot.

    `None` in **live** mode, and that refusal is the feature rather than a gap. Leverage is
    account state at the venue -- `POST /fapi/v1/leverage` takes a symbol and nothing else --
    so moving it mid-session means an authenticated call that has to succeed, be echoed back,
    and be re-checked against `ExchangeTransport`'s preflight before the next order sizes
    itself against it. None of that exists: `_apply_leverage` runs once at session start and
    `_check_preflight` runs once at construction. Letting a strategy write the ledger's
    leverage without moving the venue's would mean the platform computing margin and
    liquidation prices at one number while Binance held another -- and the first symptom
    would be a real liquidation arriving before the displayed one.

    Backtest and paper have no venue to disagree with, so there the hook is the ledger's own
    `Account.set_leverage` and the change is exact."""

    market: Any = None
    """The engine's `book.MarketView`, or `None` at a tier with no book.

    Typed loosely on purpose: `executor_base` is the module every mode shares, and importing
    the backtest's market view here would make the live engine depend on the replay layer to
    satisfy a type hint. What every mode has is *something* answering `ladder`/`top_of_book`;
    what differs is where it got them.
    """

    sync_hook: Callable[[], None] | None = None
    """Pull the book state up to the current event before reading it.

    Book streams are advanced lazily (`ticks.StateStream`), so a strategy calling
    `ctx.spread()` from inside a hook has to trigger the same advance the fill path would.
    Without this the strategy would see a quote from whenever the engine last happened to
    need one, which is a different number from the one its own order will fill against.
    """

    on_log_full: Callable[[], None] | None = None
    """What to do when the event log reaches `MAX_EVENTS`. `None` raises `EventLogFull`.

    A backtest wants the exception: nothing is at risk and a run whose log is unusable is
    worth nothing. A live session wants a stop it can perform in order, because the same
    exception raised from inside a strategy hook leaves a real position open with no halt.
    See `emit`."""

    now_ms_value: int = 0
    events: list[StrategyEvent] = field(default_factory=list)
    funding_views: dict[str, FundingView] = field(default_factory=dict)
    open_interest_values: dict[str, float] = field(default_factory=dict)
    macro_values: dict[str, tuple[float, int]] = field(default_factory=dict)
    """Series name -> `(value, source timestamp)` for every macro reading consumed so far.

    Written only by the engine's causal consumption step, so what is in here is exactly
    what the platform had *received* at or before the current bar's close -- not merely
    what the provider had published by then, which is earlier and would be a look-ahead.
    The timestamp in the tuple is still the provider's, because that is what makes
    `MacroView.age_ms` honest (Phase 11)."""
    macro_declared: bool = False
    """Whether the strategy declared a macro dataset. Decides whether `ctx.macro()` answers
    `None` (declared, nothing published) or raises (never declared) -- two very different
    facts that a bare `None` would collapse into one."""
    _seq: int = field(default=0, init=False, repr=False)

    # ------------------------------------------------------------------ engine driving

    def advance(self, ts_ms: int) -> None:
        if ts_ms < self.now_ms_value:
            raise ValueError(
                f"the engine clock cannot move backwards: {ts_ms} < {self.now_ms_value}"
            )
        self.now_ms_value = ts_ms

    # --------------------------------------------------------------- Runtime surface

    @property
    def now_ms(self) -> int:
        return self.now_ms_value

    @property
    def fill_tier(self) -> FillTier:
        return self.tier

    @property
    def hedge_mode(self) -> bool:
        return self.account.hedge_mode

    def position_view(
        self, symbol: str, position_side: PositionSide = PositionSide.BOTH
    ) -> PositionView:
        position = self.account.position(symbol, position_side)
        if position is None:
            zero = Money(0)
            return PositionView(
                symbol=symbol,
                qty=zero,
                entry_price=zero,
                unrealized_pnl=zero,
                liquidation_price=None,
                margin=zero,
                position_side=position_side,
            )
        mark = self.account.marks.get(symbol, position.entry_price)
        return PositionView(
            symbol=symbol,
            qty=position.qty,
            entry_price=position.entry_price,
            unrealized_pnl=position.unrealized_pnl(mark),
            # `None` when no bracket table is loaded or the price is unreachable, never a
            # sentinel: a sentinel would be compared against the mark and quietly satisfy
            # a distance check the strategy meant as a safety rail.
            liquidation_price=(
                self.account.liquidation_price(symbol, position_side)
                if symbol in self.account.brackets
                else None
            ),
            margin=position.reserved_margin,
            position_side=position_side,
        )

    def account_view(self) -> AccountView:
        return AccountView(
            wallet_balance=self.account.wallet,
            equity=self.account.equity,
            available=self.account.available_balance,
            used_margin=self.account.allocated_margin,
        )

    def mark_price(self, symbol: str) -> Money:
        try:
            return self.account.marks[symbol]
        except KeyError:
            raise RuntimeError(
                f"no mark price for {symbol} at {self.now_ms_value}; the first mark sample "
                "of the run has not arrived yet"
            ) from None

    def funding_view(self, symbol: str) -> FundingView:
        return self.funding_views.get(symbol, FundingView(None, None, None))

    def open_interest(self, symbol: str) -> float | None:
        return self.open_interest_values.get(symbol)

    def macro(self, name: str) -> MacroView | None:
        """The latest macro reading at or before now, with its age (Phase 11).

        Raises when the strategy never declared a macro dataset, rather than returning
        `None`: "you did not ask for this" and "the market published nothing" are
        different answers, and a strategy silently taking the `None` branch forever is
        the failure `Context.book`'s docstring describes in the depth case.
        """
        if not self.macro_declared:
            raise DataUnavailable(
                f"ctx.macro({name!r}) needs a macro dataset declared: add 'macroGlobal' "
                "(BTC dominance, total market cap) or 'macroFx' (DXY) to "
                "requires['datasets'] so the engine loads it before the run."
            )
        found = self.macro_values.get(name)
        if found is None:
            return None
        value, ts_ms = found
        return MacroView(
            series=name,
            value=value,
            ts_ms=ts_ms,
            age_ms=max(0, self.now_ms_value - ts_ms),
        )

    def depth(self, symbol: str) -> DepthSnapshot | None:
        """The ladder in force right now. `Context.book` raises below `BOOK_WALK`."""
        if self.market is None:
            return None
        self._sync()
        return self.market.ladder(symbol, self.now_ms_value)

    def spread(self, symbol: str) -> SpreadView | None:
        """Best bid/ask with their sizes, or `None` where the tier carries no book.

        `None` at `BAR_CLOSE` and `TRADE_ONLY`. A kline carries no bid or ask and neither
        does a trade print, and there is no defensible way to infer one: the obvious guess
        -- a fixed fraction of the price -- would be a constant the strategy could not
        distinguish from a measurement, which is the difference between a model and a
        fiction (spec 1.4).
        """
        if self.market is None:
            return None
        self._sync()
        top = self.market.top_of_book(symbol, self.now_ms_value)
        if top is None:
            return None
        return SpreadView(
            bid=from_scaled(top.bid_px),
            ask=from_scaled(top.ask_px),
            bid_qty=from_scaled(top.bid_qty),
            ask_qty=from_scaled(top.ask_qty),
        )

    def _sync(self) -> None:
        if self.sync_hook is not None:
            self.sync_hook()

    def step_size(self, symbol: str) -> Money:
        return from_scaled(self._filters(symbol).step_size)

    def tick_size(self, symbol: str) -> Money:
        return from_scaled(self._filters(symbol).tick_size)

    def leverage(self, symbol: str) -> int:
        return self.account.leverage(symbol)

    def set_leverage(self, symbol: str, leverage: int) -> None:
        if self.leverage_hook is None:
            raise NotImplementedError(
                f"this session cannot change leverage while it runs, so {symbol} stays at "
                f"{self.account.leverage(symbol)}x. Leverage is account state at the "
                "exchange and is applied once, before the first order; changing the "
                "ledger's copy alone would price margin and liquidation against a number "
                "the venue does not hold. Set it on the session form instead."
            )
        self.leverage_hook(symbol, leverage)

    def submit(self, intent: OrderIntent) -> str:
        return self.submit_hook(intent)

    def cancel(self, order_id: str) -> None:
        self.cancel_hook(order_id)

    def modify(self, order_id: str, price: Money | None, qty: Money | None) -> None:
        if self.modify_hook is None:
            raise NotImplementedError(
                "this execution mode has no order-amend endpoint. Cancel the order and "
                "submit a new one -- which always loses queue position, and leaves a gap "
                "with nothing in the market, so the two are not interchangeable."
            )
        self.modify_hook(order_id, price, qty)

    def cancel_all(self, symbol: str | None) -> None:
        self.cancel_all_hook(symbol)

    def open_order_ids(self, symbol: str | None) -> Sequence[str]:
        return self.open_orders_hook(symbol)

    def emit(self, kind: str, payload: Mapping[str, Any]) -> None:
        if len(self.events) >= MAX_EVENTS:
            if self.on_log_full is not None:
                # **A live session is asked to stop; a backtest is stopped.** Raising here is
                # right for a replay -- there is no position at risk and the run is worth
                # nothing once its log is unusable. In a session the same exception unwinds
                # out of a strategy hook, through the dispatch loop, and out of the worker,
                # leaving an open position on the exchange with no halt, no flatten and no
                # `KILL_SWITCH` record. So a live driver registers a handler, and the log
                # cap becomes a named stop rather than a crash.
                #
                # The event is still dropped, and that is deliberate: appending past the cap
                # would make the ceiling a suggestion. The stop reason says the log was
                # capped, so a truncated log is never read as a complete one.
                self.on_log_full()
                return
            raise EventLogFull(
                f"the strategy emitted more than {MAX_EVENTS:,} events. The log is hashed "
                "for reproducibility, written to disk and rendered in the run viewer, and "
                "none of those are usable at this size. Reduce ctx.record()/ctx.log() "
                "frequency, or shorten the range."
            )
        self._seq += 1
        self.events.append(
            StrategyEvent(
                seq=self._seq,
                ts_ms=self.now_ms_value,
                kind=kind,
                payload=dict(payload),
            )
        )

    # -------------------------------------------------------------------- internals

    def _filters(self, symbol: str) -> SymbolFilters:
        try:
            return self.filters[symbol]
        except KeyError:
            raise LookupError(
                f"no exchange filters loaded for {symbol}; order quantisation would have "
                "to be guessed at (spec 3.2)"
            ) from None
