"""The resting order book: limit queue, time-in-force, and triggers (spec 6.4, 6.5).

Spec 6.4 is blunt about why this module is the difference between a useful backtester and a
misleading one:

> Touching a limit price is not a fill. Requiring price to trade *through* the level -- or to
> consume the queue ahead -- is the difference between a limit-order backtest that is roughly
> honest and one that is pure fiction. This is the single most common way limit strategies
> look profitable and are not.

**The queue-position proxy, and why `min` is the operation that makes it sound.** Without L3
data our true position in the queue is unknowable, so it is tracked as an *upper bound* on
the size ahead of us:

- When we join level `L`, everything resting there is ahead of us: `Q_ahead = size(L)`.
- Every passive-side trade at `L` consumes the front of the queue: `Q_ahead -= trade_size`,
  and whatever is left of that trade fills us.
- Every fresh observation of `L` refines the bound: `Q_ahead = min(Q_ahead, size(L))`.

That last line is the load-bearing one and it is exactly right rather than merely
convenient. The published size at `L` is *everyone* at `L`; the orders ahead of us are a
subset of them, so the observation is a valid upper bound on `Q_ahead` at every instant.
Taking the minimum therefore never claims a better queue position than the data permits --
it cannot be optimistic -- while still capturing cancellations ahead of us, which no
trade-consumption rule can see. It also handles the price leaving `L` and coming back: our
time priority survived, so the smaller bound is kept rather than reset to a rebuilt queue.

**Where this deviates from spec 6.4, and why.** The spec says a trade *through* our level
fills us "fully". Implemented literally, a 0.5 BTC print one tick below our 100 BTC bid
would hand us a 100 BTC maker fill -- a counterfactual that contradicts itself, because had
our 100 BTC actually been resting there the 0.5 BTC aggressor would have been absorbed at
our price and never printed below it. So a through-trade instead clears `Q_ahead` to zero and
fills us up to *its own size*. In the ordinary case the two rules agree: Binance aggregates
`aggTrades` by price, so a sweep large enough to print through `L` emits a row **at** `L`
first, and that row has already filled us. The deviation only bites where the literal rule
is incoherent, and it bites conservatively -- we fill less, never more.

**What this module does not do.** It returns *instructions*, never ledger mutations. A fill
that has been decided still has to pass the margin check, and an order book that booked its
own fills would have two places that decide whether a position exists.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field

from perplab.core.money import Money, from_scaled
from perplab.engine.book import MarketView
from perplab.engine.executor_base import Order, OrderStatus
from perplab.engine.ticks import TradePrint
from perplab.strategy.context import OrderType, WorkingType

__all__ = [
    "MakerFill",
    "Trigger",
    "Removal",
    "RestingBook",
    "TRIGGER_TYPES",
    "trigger_hit",
]

TRIGGER_TYPES = frozenset(
    {
        OrderType.STOP_MARKET,
        OrderType.TAKE_PROFIT_MARKET,
        OrderType.TRAILING_STOP_MARKET,
    }
)


@dataclass(frozen=True, slots=True)
class MakerFill:
    """One increment of a resting order, decided by the queue model.

    Per increment, not per order. Spec 6.5: *"Partial fill emits `on_fill` per increment, and
    the position/entry-price update runs per increment (Case A/B/C, spec 3.3) -- not batched
    at the end. Strategies that assume 'one order, one fill' are wrong in live and must be
    wrong in backtest too."*
    """

    order_id: str
    qty: Money
    price: Money
    is_maker: bool = True


@dataclass(frozen=True, slots=True)
class Trigger:
    """A stop/take-profit/trailing order that has fired and must become a market order."""

    order_id: str
    price: Money
    """The price that fired it, and the new slippage reference (see `Order.reference_price`)."""


@dataclass(frozen=True, slots=True)
class Removal:
    """An order leaving the book without a fill, and the status that says why."""

    order_id: str
    status: OrderStatus
    reason: str


def trigger_hit(order: Order, price: Money) -> bool:
    """Whether `price` fires this order's trigger, given its type and side.

    The four combinations are worth writing out because they are easy to transcribe
    backwards, and a take-profit wired as a stop exits every winning trade at the worst
    moment while still producing a plausible-looking equity curve:

    | Type | Side | Fires when |
    |---|---|---|
    | `STOP_MARKET` | SELL (protects a long) | `price <= stop` |
    | `STOP_MARKET` | BUY (protects a short) | `price >= stop` |
    | `TAKE_PROFIT_MARKET` | SELL (protects a long) | `price >= stop` |
    | `TAKE_PROFIT_MARKET` | BUY (protects a short) | `price <= stop` |

    A trailing stop is a `STOP_MARKET` whose level moves; `RestingBook.observe_price` keeps
    `trigger_price` current and this function then reads it like any other stop.
    """
    level = order.trigger_price
    if level is None:
        return False
    selling = order.intent.side == "SELL"
    if order.intent.type is OrderType.TAKE_PROFIT_MARKET:
        return price >= level if selling else price <= level
    # STOP_MARKET and TRAILING_STOP_MARKET both protect against adverse movement.
    return price <= level if selling else price >= level


@dataclass
class RestingBook:
    """Every order live at the simulated matching engine, indexed by symbol.

    Holds no ledger state and no market state -- it is given a `MarketView` to read and
    returns instructions for the engine to book. That split is what lets the queue model be
    tested against hand-built ladders without an account, a lake or a clock.
    """

    market: MarketView
    _by_symbol: dict[str, list[Order]] = field(default_factory=dict, init=False, repr=False)
    _crossed: set[str] = field(default_factory=set, init=False, repr=False)
    """Ids of orders resting at a price **through the far touch they just crossed**.

    The H21 case: a marketable limit larger than the visible liquidity fills what the model
    can see and rests the remainder at its own limit -- which, at `BOOK_TICKER`, is a price
    *through* the published ask (for a buy). At the venue that remainder would have kept
    taking liquidity the model cannot see, so any fill it collects while still priced
    through the book is taker flow, and charging it the maker rate understated costs by the
    maker/taker gap on the majority of every marketable limit larger than the touch. While
    an id is in this set, `on_trade` emits its fills with `is_maker=False`; the flag clears
    the first time a fresh book observation shows the order no longer crossing (the market
    moved past it), from which point it genuinely rests and the ordinary queue model -- and
    the maker rate -- apply. Membership is engine-declared (`note_crossed`), because only
    the placement path knows what the book looked like when the remainder rested.
    """

    # ------------------------------------------------------------------------- lifecycle

    def add(self, order: Order) -> None:
        """Put an order on the book. It must already be `WORKING` with `remaining` set."""
        self._by_symbol.setdefault(order.intent.symbol, []).append(order)

    def note_crossed(self, order: Order) -> None:
        """Mark a resting remainder as priced through the touch it crossed. See `_crossed`."""
        self._crossed.add(order.id)

    def rests_through_book(self, order: Order) -> bool:
        """Whether this order's fills are currently taker flow. See `_crossed`."""
        return order.id in self._crossed

    def discard(self, order: Order) -> None:
        """Remove an order that is no longer live, whatever ended it."""
        self._crossed.discard(order.id)
        orders = self._by_symbol.get(order.intent.symbol)
        if not orders:
            return
        try:
            orders.remove(order)
        except ValueError:  # pragma: no cover - discarding twice is harmless
            return
        if not orders:
            self._by_symbol.pop(order.intent.symbol, None)

    def orders_for(self, symbol: str) -> Sequence[Order]:
        """Live orders on one symbol, in submission order.

        Submission order rather than an arbitrary dict order, because two orders resting at
        the same level share the incoming volume and *which one fills first* has to be a
        property of the run rather than of the interpreter. First submitted, first filled --
        the same rule the exchange applies.
        """
        return tuple(self._by_symbol.get(symbol, ()))

    def symbols(self) -> Iterable[str]:
        return tuple(self._by_symbol)

    # ---------------------------------------------------------------------- observations

    def observe_book(self, symbol: str, now_ms: int) -> None:
        """Refine every resting order's queue bound from the latest book observation.

        Only ever tightens; see the module docstring for why `min` is the sound operation
        rather than a heuristic. Orders whose level is not visible in the observation are
        left alone -- an unobserved level is not an empty one, and treating it as empty
        would put us at the front of a queue we have never seen.
        """
        for order in self._by_symbol.get(symbol, ()):
            if order.limit_scaled <= 0 or order.triggered:
                continue
            if order.id in self._crossed and not self._still_crossing(symbol, order, now_ms):
                # The market has moved past the order's price: a fresh observation puts the
                # far touch strictly beyond it, so from this instant the order genuinely
                # rests and its fills go back to the maker model. See `_crossed`.
                self._crossed.discard(order.id)
            observed = self._resting_size(symbol, order, now_ms)
            if observed is None:
                continue
            if order.queue_ahead is None or observed < order.queue_ahead:
                order.queue_ahead = observed

    def _still_crossing(self, symbol: str, order: Order, now_ms: int) -> bool:
        """Whether the far touch is still at or through this order's limit.

        `True` on no data as well: absence of a book is not evidence the market moved past
        us, and clearing the taker flag on silence would hand back the maker rate during
        exactly the outage windows H22 is about. The far side is read from the ladder's
        best level when one is in force, else the top of book -- the same preference order
        `_resting_size` uses for the near side.
        """
        buying = order.intent.side == "BUY"
        level = order.limit_scaled
        far: int | None = None
        ladder = self.market.ladder(symbol, now_ms)
        if ladder is not None:
            far = ladder.ask_px[0] if buying else ladder.bid_px[0]
        else:
            top = self.market.top_of_book(symbol, now_ms)
            if top is not None:
                far = top.ask_px if buying else top.bid_px
        if far is None or far <= 0:
            return True
        return far <= level if buying else far >= level

    def _resting_size(self, symbol: str, order: Order, now_ms: int) -> int | None:
        """Published size at this order's level, or `None` if the level is not visible.

        The ladder is consulted first because it can see levels behind the touch, which is
        the whole reason `BOOK_WALK` gives a better queue estimate than `BOOK_TICKER`. The
        touch is the fallback and it can only answer for one price per side.
        """
        buying = order.intent.side == "BUY"
        level = order.limit_scaled

        ladder = self.market.ladder(symbol, now_ms)
        if ladder is not None:
            prices = ladder.bid_px if buying else ladder.ask_px
            sizes = ladder.bid_qty if buying else ladder.ask_qty
            for index in range(len(prices)):
                if prices[index] == level:
                    return sizes[index]
            # Inside the published range but absent from it means the level genuinely holds
            # nothing: the ladder is contiguous over the prices it covers. Outside the range
            # -- deeper than level 20 -- it says nothing at all.
            if prices and (
                (buying and level > prices[-1]) or (not buying and level < prices[-1])
            ):
                return 0
            return None

        top = self.market.top_of_book(symbol, now_ms)
        if top is None:
            return None
        if buying and top.bid_px == level:
            return top.bid_qty
        if not buying and top.ask_px == level:
            return top.ask_qty
        return None

    # --------------------------------------------------------------------------- trading

    def on_trade(self, trade: TradePrint) -> Iterator[MakerFill]:
        """Apply one print to every resting limit order on that symbol.

        Yields at most one increment per order per trade. Orders are visited in submission
        order and each consumes from the *same* print in turn, so two resting orders at the
        same level share the incoming volume the way the exchange would rather than both
        being filled from it.
        """
        orders = self._by_symbol.get(trade.symbol)
        if not orders:
            return
        available = trade.qty_scaled
        for order in tuple(orders):
            if available <= 0:
                return
            if order.triggered or order.limit_scaled <= 0 or order.remaining <= 0:
                continue
            if order.intent.type is not OrderType.LIMIT:
                continue

            buying = order.intent.side == "BUY"
            level = order.limit_scaled
            # A print on the same side of the book as our resting order is what consumes
            # it: a sell-aggressive trade eats bid-side queue, a buy-aggressive one eats
            # ask-side queue. A trade on the other side cannot touch us however close it is.
            if buying and not trade.consumes_bids:
                continue
            if not buying and not trade.consumes_asks:
                continue

            through = (
                trade.price_scaled < level if buying else trade.price_scaled > level
            )
            if not through and trade.price_scaled != level:
                continue

            if through:
                # Everything at our level was swept to get here. See the module docstring
                # for why the fill is still bounded by the aggressor's own size.
                order.queue_ahead = 0
                fillable = available
            else:
                ahead = order.queue_ahead
                if ahead is None:
                    # **Never observed the level, so it never fills on an at-level print.**
                    # This branch used to set `queue_ahead = 0` and call that pessimistic. It
                    # is the exact opposite: zero means *front of the queue*, so one print
                    # went by and the next one filled us in full. An unobserved level then
                    # beat every observed one -- a buy limit behind an unknown queue filled
                    # 5 BTC where a measured queue of 5 filled 2 and a measured queue of
                    # 1 000 filled nothing. The run was rewarded for having less data.
                    #
                    # Leaving it `None` is the only reading that cannot manufacture a fill
                    # out of a queue nobody saw. The order is not stranded: a trade *through*
                    # the level still fills it, bounded by the aggressor's own size, and the
                    # moment the level becomes the touch `observe_book` measures it. What
                    # stays refused is the case where the book has gone dark and the tape has
                    # not -- which is spec 4.5's outage, and during an outage there are no
                    # fills, not free ones.
                    continue
                eaten = ahead if ahead < available else available
                order.queue_ahead = ahead - eaten
                fillable = available - eaten

            if fillable <= 0:
                continue
            take = fillable if fillable < order.remaining else order.remaining
            if take <= 0:
                continue
            available -= take
            # An order still priced through the touch it crossed at placement is collecting
            # taker flow, whatever branch filled it: at the venue that quantity would have
            # been taken from liquidity the model cannot see, not earned in a queue. The
            # price stays the limit, which for a buy is at or above anything the venue
            # would actually have charged -- conservative, like the flag itself.
            yield MakerFill(
                order_id=order.id,
                qty=from_scaled(take),
                price=from_scaled(level),
                is_maker=order.id not in self._crossed,
            )

    def observe_price(
        self,
        symbol: str,
        prices: Sequence[Money],
        working: WorkingType,
        *,
        ordered: bool,
    ) -> Iterator[Trigger]:
        """Advance trailing extremes and fire any trigger these prices reach.

        **`ordered` says what the sequence is, and it changes the answer.** Two callers pass
        two genuinely different things:

        *A mark bar's range* (`ordered=False`) is a *set* of prices the mark is known to have
        reached, with no information about when. A trailing stop must therefore ratchet
        against all of them before any is tested against it. Interleaving the two passes
        makes the result depend on the order the caller happened to list them in, which is
        not a fact about the market: with `[high, low, close]`, a long-protecting SELL
        ratchets on the high and fires on the low, while the mirrored short-protecting BUY --
        whose favourable extreme is the *low* -- is tested against the high before it has
        ratcheted, and does not fire at all. Same bar, same rate, identical trigger levels,
        one exit.

        *The trade tape* (`ordered=True`) is a sequence, keyed by `agg_id`, which spec 6.2
        calls the exchange's own account of what happened first. Ratcheting across the whole
        batch there is **within-millisecond look-ahead**: a print at 39 700 fired a stop
        sitting at 39 600 only because a *later* print at 41 000 had already moved the level
        to 40 590. The tell was that reversing the two prints changed nothing -- a causal
        model cannot be indifferent to the order of its own inputs.

        Only orders whose `working_type` matches are considered, so a mark-triggered stop is
        untouched by the trade tape and a contract-price stop is untouched by the mark.
        """
        for order in tuple(self._by_symbol.get(symbol, ())):
            if order.triggered or order.intent.type not in TRIGGER_TYPES:
                continue
            if order.intent.working_type is not working:
                continue
            trailing = order.intent.type is OrderType.TRAILING_STOP_MARKET
            if trailing and not ordered:
                for price in prices:
                    if price > 0:
                        self.advance_trail(order, price)
            for price in prices:
                if price <= 0:
                    continue
                if trailing and ordered:
                    self.advance_trail(order, price)
                if trigger_hit(order, price):
                    order.triggered = True
                    yield Trigger(order_id=order.id, price=price)
                    break

    @staticmethod
    def advance_trail(order: Order, price: Money) -> None:
        """Ratchet a trailing stop's extreme and recompute its level (spec 6.4).

        *"track the extreme (highest mark since entry for a long) and trigger at
        `callback_rate` retracement. Update on every mark price event, not on bar close."*

        `callback_rate` is a **fraction** here -- 0.01 is a 1% callback. Binance's own field
        is in percent, so 0.01 here is their 1.0; the units are stated on `ctx.trailing_stop`
        and validated there, because a rate read as percent when it means fraction is a stop
        a hundred times further away than the author intended.
        """
        rate = order.intent.callback_rate
        if rate is None:  # pragma: no cover - validated at submission
            return
        selling = order.intent.side == "SELL"
        extreme = order.trail_extreme
        if extreme is None or (price > extreme if selling else price < extreme):
            order.trail_extreme = price
            extreme = price
        order.trigger_price = extreme * (1 - rate) if selling else extreme * (1 + rate)

    # ----------------------------------------------------------------------- maintenance

    def expire_stale(self, now_ms: int, deadline_ms: int) -> Iterator[Removal]:
        """Retire market orders that have waited past their deadline for a print.

        Only the `TRADE_ONLY` tier parks a market order: it fills at the *next* trade, and a
        range with no trades for a minute is an outage rather than a slow fill. Expiring
        beats waiting, because an order that finally executes an hour later against a market
        that moved without it is a worse lie than one that plainly did not fill.

        **This sweep is the second of two mechanisms, and it is not dead code.** The primary
        one is a scheduled `expire` instruction at `arrival + deadline`; this runs at each
        liquidation check. Which wins is arithmetic: the sweep needs a mark-bar close or a
        funding settlement at or before `arrival + deadline`, and mark bars close at
        `…:59.999` while an order submitted at a bar close arrives at `…:59.999 + latency`.
        At any ordinary latency the scheduled event gets there first. At `FixedLatency(0)` --
        a supported configuration, flagged `ZERO_LATENCY` -- arrival *is* `…:59.999`, the two
        land on the same millisecond, and the liquidation check's priority 2 beats the
        instruction's priority 8. Both paths go through an `is_open` guard, so whichever
        loses is a no-op rather than a double removal.
        """
        for order in tuple(self._all()):
            if order.intent.type is not OrderType.MARKET:
                continue
            if now_ms - order.arrival_ts < deadline_ms:
                continue
            yield Removal(
                order_id=order.id,
                status=OrderStatus.EXPIRED,
                reason=(
                    f"no trade printed within {deadline_ms} ms of arrival, so this market "
                    "order had nothing to execute against (spec 4.5 HALT_TRADING)"
                ),
            )

    def _all(self) -> Iterator[Order]:
        for orders in self._by_symbol.values():
            yield from orders

    def open_ids(self, symbol: str | None = None) -> tuple[str, ...]:
        if symbol is None:
            return tuple(order.id for order in self._all())
        return tuple(order.id for order in self._by_symbol.get(symbol, ()))
