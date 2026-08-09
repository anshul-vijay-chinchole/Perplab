"""Fill models, one per fidelity tier (spec 4.2, 6.4).

Spec 4.2's four tiers are four different answers to one question -- *what price would this
order actually have got* -- and each is the best answer its data can support:

| Tier | Input | Market order fills at |
|---|---|---|
| `BOOK_WALK` | `depth20` ladder | the volume-weighted walk down the ladder, plus a penalty beyond level 20 |
| `BOOK_TICKER` | `bookTicker` + `aggTrades` | the far touch, plus `k x sqrt(notional / 1 min notional)` |
| `TRADE_ONLY` | `aggTrades` | the **next** print at or after arrival, plus a stated spread |
| `BAR_CLOSE` | `klines` | the **most recent** print at or before arrival, plus a stated offset |

**The two trade-based tiers point in opposite directions in time, and that is not an
inconsistency.** At `TRADE_ONLY` the prints are milliseconds apart, so waiting for the next
one models what a market order really does -- it executes against the next liquidity event.
At `BAR_CLOSE` the only datable prints are the bar open and the bar close, up to a whole
timeframe apart, and waiting for the next would model a market order taking a minute to
fill. Each tier uses the choice its resolution makes true.

**Rounding is always against the trader.** `quantize_fill_price` rounds a buy up and a sell
down -- deliberately the opposite of `money.quantize_price`, which rounds an *order* price
in the direction that understates fill probability. Reusing that function here would hand
back a fraction of a tick on every fill in the run: small, one-directional and compounding,
which is the worst shape a modelling error can take.

**Nothing here invents a price.** A model whose input is missing or stale raises `NoQuote`,
which the engine turns into a named rejection rather than a fill at a fabricated level. Spec
4.5's `HALT_TRADING` is the same idea at run granularity: during an outage there are no
fills, not cheap ones.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, localcontext
from typing import Protocol

from perplab.core.money import ACCOUNTING_CONTEXT, SCALE, Money, from_scaled, parse_money
from perplab.core.types import DepthSnapshot, Side
from perplab.engine.ticks import TopOfBook

__all__ = [
    "DEFAULT_SLIPPAGE_BPS",
    "DEFAULT_TRADE_SPREAD_BPS",
    "DEFAULT_IMPACT_K_BPS",
    "DEFAULT_DEPTH_EXHAUSTION_PCT",
    "BPS",
    "NoQuote",
    "FillQuote",
    "MarketInputs",
    "CrossResult",
    "cross_book",
    "MarketFillModel",
    "BarCloseFillModel",
    "TradeOnlyFillModel",
    "BookTickerFillModel",
    "BookWalkFillModel",
    "fill_model_for_tier",
    "fill_model_from_json",
    "quantize_fill_price",
]

BPS = Decimal(10_000)

_NOTIONAL_SCALE = Decimal(SCALE) * Decimal(SCALE)
"""`price x qty` in lake scaling carries `10^16`. See `book.MarketView.recent_notional`."""

DEFAULT_SLIPPAGE_BPS = parse_money("1.0")
"""Adverse offset applied to every market fill at the `BAR_CLOSE` tier, in basis points.

Deliberately far worse than BTCUSDT's real half-spread -- 0.1 tick on a 60 000 price is
about 0.008 bps. It is not standing in for the spread. It stands in for the *timing
uncertainty this tier cannot resolve*: the fill is priced at a print that may be up to a bar
old, and 1 bp is a conservative charge for not knowing where inside that bar the order
landed. A stated assumption, recorded in the manifest and shown on the results page.
"""

DEFAULT_TRADE_SPREAD_BPS = parse_money("1.0")
"""Spec 6.4's *"fixed conservative spread assumption"* for the `TRADE_ONLY` tier.

A print says a trade happened at that price; it does not say which side of the book it was
on. Our buy executes at the ask, and the print may have been a sell-aggressive one at the
bid -- so the honest charge is a full spread's worth of uncertainty rather than nothing.

`aggTrades` does carry `is_buyer_maker`, so the print's aggressor side *is* known, and it is
deliberately not used to discount the offset on same-side prints. A same-side print is not
evidence our order got that price: the queue that produced it may have been consumed by the
very trade we are looking at. Using it would make the model cheaper exactly where it is
least justified.

Same magnitude as `DEFAULT_SLIPPAGE_BPS` on purpose. What separates `TRADE_ONLY` from
`BAR_CLOSE` is *timing* -- a print milliseconds away rather than up to a bar -- not the size
of the offset, and giving them different numbers would suggest otherwise.
"""

DEFAULT_IMPACT_K_BPS = parse_money("10")
"""Spec 6.4's `k`, in basis points: `impact_bps = k x sqrt(notional / 1 min notional)`.

*"`k` is calibrated once from your own paper-trading fills (compare realised slippage to the
model) -- until then, default `k = 10` bps, chosen to be pessimistic."* There are no paper
sessions until Phase 7, so 10 stands, and it is recorded per run so a recalibration does not
silently re-price old results.

Note what `k` absorbs: the whole cost of crossing, not just the part beyond the touch.
`bookTicker` publishes the size resting at the touch, and it is deliberately *not* used to
zero the impact term for orders that would fit inside it -- the spec's `k` is an all-in
figure and discounting it by visible size would double-count the calibration.
"""

DEFAULT_DEPTH_EXHAUSTION_PCT = parse_money("0.001")
"""Spec 6.4's `depth_exhaustion_penalty`: 0.10% beyond the worst visible level.

*"deliberately pessimistic. If a strategy routinely triggers this warning, the position
sizing is unrealistic for the instrument, and the results page says so rather than quietly
filling at a fantasy price."*
"""


class NoQuote(RuntimeError):
    """This model cannot price this order from the data available at this instant.

    Carried as an exception rather than a `None` return so the reason travels with the
    refusal. The engine puts `str(exc)` straight into the rejection, which is how a run that
    placed twenty orders and filled none says *why* instead of reporting a strategy that
    changed its mind.
    """


def quantize_fill_price(price: Money, tick_size: Money, side: Side) -> Money:
    """Round a fill price to the symbol's tick, **against** the trader.

    Deliberately the opposite direction from `money.quantize_price`, and the difference is
    worth stating because the two look interchangeable and are not.

    `quantize_price` rounds an *order* price: a buy limit rounds *down*, because a limit
    placed lower is less likely to fill, and understating fill probability is the safe
    error. This function rounds an *execution* price, where the same principle points the
    other way: a buy that executes pays the higher tick and a sell receives the lower one.
    """
    if tick_size <= 0:
        raise ValueError(f"tick_size must be positive, got {tick_size}")
    if price <= 0:
        raise ValueError(f"fill price must be positive, got {price}")
    rounding = ROUND_CEILING if side is Side.BUY else ROUND_FLOOR
    with localcontext(ACCOUNTING_CONTEXT):
        steps = (price / tick_size).to_integral_value(rounding=rounding)
        quantised = steps * tick_size
    if quantised <= 0:
        # Only reachable for a sell whose price is below one tick, which the exchange
        # could not have printed in the first place. Refusing beats booking a fill at zero,
        # which `Account.apply_fill` would reject anyway -- with a message about the ledger
        # rather than about the fill model that produced it.
        raise ValueError(
            f"fill price {price} rounds to {quantised} at tick {tick_size}; a price below "
            "one tick cannot have traded"
        )
    return quantised


@dataclass(frozen=True, slots=True)
class FillQuote:
    """What a market order actually costs, and what it would have cost without slippage.

    Both numbers are kept because spec 8.4's attribution needs the difference: `price_pnl`
    is reported at reference prices and `slippage_cost` is what execution took off it. A
    model that returned only the fill price would leave the results page unable to say
    whether a strategy is unprofitable or merely badly executed.
    """

    price: Money
    """The fill price, tick-quantised against the trader. A volume-weighted average when
    the order walked more than one level."""

    print_price: Money
    """The market observation the fill was derived from, before slippage or impact.

    The last print at `BAR_CLOSE`, the filling print at `TRADE_ONLY`, the far touch at
    `BOOK_TICKER` and `BOOK_WALK`.
    """

    slippage_per_unit: Money
    """Signed cost per unit: positive when execution was worse than the reference.

    Signed, not absolute. Spec 8.4 writes the formula with `|.|` *and* requires the four
    components to sum exactly to net PnL, and those two cannot both hold when execution
    happens to be favourable. The signed form is what makes the identity exact; the
    absolute figure is reported alongside it under its own name.
    """

    exhausted_qty: Money = Decimal(0)
    """Quantity filled beyond the deepest visible level (`BOOK_WALK` only).

    Non-zero means the order was larger than the published book, and spec 6.4 requires that
    to reach the results page: *"If a strategy routinely triggers this warning, the position
    sizing is unrealistic for the instrument."*
    """

    levels_walked: int = 0
    """How many ladder levels the order consumed. 1 for a fill that never left the touch."""

    impact_bps: Money = Decimal(0)
    """The modelled impact charge, in basis points (`BOOK_TICKER` only). Reported so a run
    can show whether its costs came from the spread or from its own size."""


@dataclass(frozen=True, slots=True)
class MarketInputs:
    """Everything any tier's market-order model might need, gathered once by the engine.

    One bundle rather than four call signatures. The engine already has to know which tier
    it is running; making it also know which arguments that tier wants would put a four-way
    branch on the fill path, and a four-way branch is four places for the reference price or
    the tick size to be passed differently.
    """

    side: Side
    qty: Money
    tick_size: Money
    reference_price: Money
    """The price at *signal* time -- what spec 8.4 measures slippage against."""
    print_price: Money | None = None
    """The print this fill is priced from, where the tier uses one."""
    top: TopOfBook | None = None
    ladder: DepthSnapshot | None = None
    recent_notional: int = 0
    """Traded notional over the last minute, in `10^16` scaling. See `book.MarketView`."""


class FillModel(Protocol):
    """What the engine needs from a market-order model."""

    @property
    def tier(self) -> str: ...

    def quote(self, inputs: MarketInputs) -> FillQuote: ...

    def to_json(self) -> dict[str, str]: ...


def _signed(side: Side) -> Decimal:
    return Decimal(1) if side is Side.BUY else Decimal(-1)


def _finish(
    *,
    side: Side,
    raw_price: Money,
    print_price: Money,
    reference: Money,
    tick_size: Money,
    exhausted_qty: Money = Decimal(0),
    levels_walked: int = 0,
    impact_bps: Money = Decimal(0),
) -> FillQuote:
    """Quantise against the trader and compute the signed slippage, once, for every tier.

    Shared so the sign convention and the rounding direction cannot drift between models.
    They did not drift; the point is that with four models there are four opportunities, and
    a sign error on one side only is precisely the defect that makes a long-biased strategy
    report favourable execution on data where a short-biased one reports adverse.
    """
    with localcontext(ACCOUNTING_CONTEXT):
        price = quantize_fill_price(raw_price, tick_size, side)
        slippage = _signed(side) * (price - reference)
    return FillQuote(
        price=price,
        print_price=print_price,
        slippage_per_unit=slippage,
        exhausted_qty=exhausted_qty,
        levels_walked=levels_walked,
        impact_bps=impact_bps,
    )


# ------------------------------------------------------------------------------ BAR_CLOSE


@dataclass(frozen=True, slots=True)
class BarCloseFillModel:
    """The `BAR_CLOSE` model: the most recent print at or before arrival, plus a fixed offset.

    A 1-minute kline records four prices and dates two of them: the `open` is the bar's
    first trade and the `close` is its last. The `high` and `low` certainly happened, but
    nothing in the row says *when*, so neither can price a fill without inventing a
    timestamp. That leaves exactly one rule, and it is causal by construction -- the price
    used was published before the order arrived.

    **No partial fills, and that is a property of the tier rather than an omission.** Spec
    6.5's partial-fill rules need a size dimension -- resting depth, or a trade tape to
    consume -- and a kline has neither. What *is* enforced is `MARKET_LOT_SIZE`: an order
    the exchange would have rejected outright is rejected here too.
    """

    slippage_bps: Money = DEFAULT_SLIPPAGE_BPS

    def __post_init__(self) -> None:
        _check_bps(self.slippage_bps, "slippage_bps")

    @property
    def tier(self) -> str:
        return "BAR_CLOSE"

    def quote(self, inputs: MarketInputs) -> FillQuote:
        print_price = inputs.print_price
        if print_price is None or print_price <= 0:
            raise NoQuote(
                "no trade print has been published for this symbol yet, so there is no "
                "price to fill against"
            )
        with localcontext(ACCOUNTING_CONTEXT):
            raw = print_price * (1 + _signed(inputs.side) * self.slippage_bps / BPS)
        return _finish(
            side=inputs.side,
            raw_price=raw,
            print_price=print_price,
            reference=inputs.reference_price,
            tick_size=inputs.tick_size,
            levels_walked=1,
        )

    def to_json(self) -> dict[str, str]:
        return {"tier": "BAR_CLOSE", "slippage_bps": str(self.slippage_bps)}


MarketFillModel = BarCloseFillModel
"""The Phase 4 name for this model, kept as an alias.

Phase 4 had one fill model, so "the market fill model" was an adequate name for it. With
four tiers it is not, and the class is now named after the tier it implements. The alias
stays because the old name is constructed at a couple of dozen call sites across the engine,
the worker and the tests, all of them meaning exactly this model -- renaming them would be
churn with no reader on the other end.
"""


# ----------------------------------------------------------------------------- TRADE_ONLY


@dataclass(frozen=True, slots=True)
class TradeOnlyFillModel:
    """The `TRADE_ONLY` model: the next print at or after arrival, plus a stated spread.

    The order does not fill at arrival. It waits, exactly as a market order waits for the
    next liquidity event, and the fill is stamped with the *print's* timestamp rather than
    the arrival timestamp -- so a market order into a quiet minute shows the delay it
    actually suffered instead of pretending to have filled instantly at a price from
    before it was sent.
    """

    spread_bps: Money = DEFAULT_TRADE_SPREAD_BPS

    def __post_init__(self) -> None:
        _check_bps(self.spread_bps, "spread_bps")

    @property
    def tier(self) -> str:
        return "TRADE_ONLY"

    def quote(self, inputs: MarketInputs) -> FillQuote:
        print_price = inputs.print_price
        if print_price is None or print_price <= 0:
            raise NoQuote(
                "no trade has printed since this order arrived, so there is nothing to "
                "fill it against"
            )
        with localcontext(ACCOUNTING_CONTEXT):
            raw = print_price * (1 + _signed(inputs.side) * self.spread_bps / BPS)
        return _finish(
            side=inputs.side,
            raw_price=raw,
            print_price=print_price,
            reference=inputs.reference_price,
            tick_size=inputs.tick_size,
            levels_walked=1,
        )

    def to_json(self) -> dict[str, str]:
        return {"tier": "TRADE_ONLY", "spread_bps": str(self.spread_bps)}


# ---------------------------------------------------------------------------- BOOK_TICKER


@dataclass(frozen=True, slots=True)
class BookTickerFillModel:
    """The `BOOK_TICKER` model: the far touch, plus spec 6.4's square-root impact term.

    ```
    impact_bps = k x sqrt(order_notional / recent_1min_notional_volume)
    fill       = touch x (1 +/- impact_bps / 10 000)
    ```

    Square-root impact is the standard empirical form and degrades gracefully: an order a
    tenth the size of the minute's volume costs `k/3` bps, one matching it costs `k`, one
    ten times it costs `3k`. It is unbounded above, and deliberately not capped -- a cap
    would make the model cheapest exactly where it is least trustworthy.

    **A minute with no trades is refused, not priced.** The denominator would be zero, and
    every way of filling that hole is an invention: zero impact is optimistic, a constant is
    a number nobody can defend, and the visible touch size answers a different question. The
    engine rejects the order with that reason, which is spec 4.5's `HALT_TRADING` behaviour
    at the resolution the data supports.
    """

    impact_k_bps: Money = DEFAULT_IMPACT_K_BPS

    def __post_init__(self) -> None:
        if self.impact_k_bps < 0:
            raise ValueError(
                f"impact_k_bps {self.impact_k_bps} is negative, which would model execution "
                "that improves with size"
            )
        _check_bps(self.impact_k_bps, "impact_k_bps")

    @property
    def tier(self) -> str:
        return "BOOK_TICKER"

    def quote(self, inputs: MarketInputs) -> FillQuote:
        top = inputs.top
        if top is None:
            raise NoQuote(
                "no top-of-book observation is in force at this instant; bookTicker has "
                "either not started or has been silent past the staleness bound"
            )
        touch = from_scaled(top.ask_px if inputs.side is Side.BUY else top.bid_px)
        if touch <= 0:
            raise NoQuote("the observed touch price is not positive")

        with localcontext(ACCOUNTING_CONTEXT):
            notional = inputs.qty * touch
            recent = Decimal(inputs.recent_notional) / _NOTIONAL_SCALE
            if recent <= 0:
                raise NoQuote(
                    "no trades printed in the last minute, so spec 6.4's impact term "
                    "(k x sqrt(notional / 1min notional)) has a zero denominator. Filling "
                    "at the touch with no impact would be an assumption, not a measurement"
                )
            impact_bps = self.impact_k_bps * (notional / recent).sqrt()
            raw = touch * (1 + _signed(inputs.side) * impact_bps / BPS)

        return _finish(
            side=inputs.side,
            raw_price=raw,
            print_price=touch,
            reference=inputs.reference_price,
            tick_size=inputs.tick_size,
            levels_walked=1,
            impact_bps=impact_bps,
        )

    def to_json(self) -> dict[str, str]:
        return {"tier": "BOOK_TICKER", "impact_k_bps": str(self.impact_k_bps)}


# ------------------------------------------------------------------------------ BOOK_WALK


@dataclass(frozen=True, slots=True)
class BookWalkFillModel:
    """The `BOOK_WALK` model: spec 6.4's ladder walk, verbatim.

    ```
    remaining = qty ; cost = 0
    for (price, size) in book_side_levels:          # best first
        take = min(remaining, size)
        cost += take x price
        remaining -= take
        if remaining == 0: break
    if remaining > 0:                                # exhausted 20 levels
        penalty_price = worst_level_price x (1 +/- depth_exhaustion_penalty)
        cost += remaining x penalty_price
    avg_fill = cost / qty
    ```

    **One fill, at the average, not one per level.** Spec 6.4 ends on `avg_fill = cost/qty`
    and spec 6.5 scopes per-increment `on_fill` to *limit* orders. A real exchange does emit
    a fill per level, and modelling that here would add a hook call per level to every
    market order in the run without changing a single number the ledger records, because
    `apply_fill` is linear in quantity at a constant price and the levels are simultaneous.

    **The ladder is not depleted by our own fills.** Two orders in the same second both walk
    the full published book. The alternative -- subtracting our fills and holding the
    depleted ladder until the next 1 s snapshot -- would overstate our impact by up to a
    second in a book that refills in milliseconds. Aggregate impact is what the
    `BOOK_TICKER` tier's `k` models; this tier prices the instantaneous walk.
    """

    depth_exhaustion_pct: Money = DEFAULT_DEPTH_EXHAUSTION_PCT

    def __post_init__(self) -> None:
        if self.depth_exhaustion_pct < 0:
            raise ValueError(
                f"depth_exhaustion_pct {self.depth_exhaustion_pct} is negative, which would "
                "reward an order for exceeding the visible book"
            )
        if self.depth_exhaustion_pct >= 1:
            raise ValueError(
                f"depth_exhaustion_pct {self.depth_exhaustion_pct} is 100% or more; for a "
                "sell that is a fill at or below zero"
            )

    @property
    def tier(self) -> str:
        return "BOOK_WALK"

    def quote(self, inputs: MarketInputs) -> FillQuote:
        ladder = inputs.ladder
        if ladder is None:
            raise NoQuote(
                "no depth snapshot is in force at this instant; depth20 has either not "
                "started or has been silent past the staleness bound"
            )
        buying = inputs.side is Side.BUY
        prices = ladder.ask_px if buying else ladder.bid_px
        sizes = ladder.ask_qty if buying else ladder.bid_qty
        if not prices:
            raise NoQuote(
                f"the depth snapshot has no {'ask' if buying else 'bid'} levels to walk"
            )

        with localcontext(ACCOUNTING_CONTEXT):
            remaining = inputs.qty
            cost = Decimal(0)
            levels = 0
            worst = Decimal(0)
            for index in range(len(prices)):
                size = from_scaled(sizes[index])
                if size <= 0:
                    continue
                price = from_scaled(prices[index])
                worst = price
                levels += 1
                take = size if size < remaining else remaining
                cost += take * price
                remaining -= take
                if remaining <= 0:
                    break

            if worst <= 0:
                raise NoQuote("every level of the depth snapshot has zero size")

            exhausted = remaining if remaining > 0 else Decimal(0)
            if exhausted > 0:
                sign = Decimal(1) if buying else Decimal(-1)
                cost += exhausted * worst * (1 + sign * self.depth_exhaustion_pct)
            average = cost / inputs.qty
            touch = from_scaled(prices[0])

        return _finish(
            side=inputs.side,
            raw_price=average,
            print_price=touch,
            reference=inputs.reference_price,
            tick_size=inputs.tick_size,
            exhausted_qty=exhausted,
            levels_walked=levels,
        )

    def to_json(self) -> dict[str, str]:
        return {"tier": "BOOK_WALK", "depth_exhaustion_pct": str(self.depth_exhaustion_pct)}


# ------------------------------------------------------------------ marketable limits


@dataclass(frozen=True, slots=True)
class CrossResult:
    """What a limit order can take from the book *right now*, at or better than its price."""

    qty: Money
    price: Money
    """Volume-weighted average of the levels taken, tick-quantised against the trader.
    Meaningless when `qty` is zero."""

    levels: int


def cross_book(
    *,
    side: Side,
    qty: Money,
    limit_price: Money,
    tick_size: Money,
    top: TopOfBook | None,
    ladder: DepthSnapshot | None,
) -> CrossResult:
    """Liquidity available to a limit order at or better than `limit_price`, right now.

    This is the taker half of a limit order, and it is deliberately *not* the market-order
    model. Two differences, both structural:

    **No impact term, and no exhaustion penalty.** Both of those price the cost of going
    *past* the liquidity you can see. A limit order cannot go past its own limit: whatever
    is not available at an acceptable price does not fill, it rests. Charging `k x sqrt(.)`
    on top would push the fill above the limit, which is the one price a limit order is
    guaranteed never to pay.

    **The ladder is used when present, the touch otherwise.** At `BOOK_WALK` the order can
    sweep several levels up to its limit; at `BOOK_TICKER` only the touch size is published,
    so that is all it can take and the rest rests. That difference *is* the fidelity gap
    between the two tiers for limit orders, and it shows up as a smaller immediate fill
    rather than as a different price.

    Returns `qty=0` when nothing crosses, which is the ordinary case for a passive order.
    """
    buying = side is Side.BUY
    with localcontext(ACCOUNTING_CONTEXT):
        if ladder is not None:
            prices = ladder.ask_px if buying else ladder.bid_px
            sizes = ladder.ask_qty if buying else ladder.bid_qty
        elif top is not None:
            prices = (top.ask_px,) if buying else (top.bid_px,)
            sizes = (top.ask_qty,) if buying else (top.bid_qty,)
        else:
            return CrossResult(qty=Decimal(0), price=Decimal(0), levels=0)

        remaining = qty
        cost = Decimal(0)
        taken = Decimal(0)
        levels = 0
        for index in range(len(prices)):
            price = from_scaled(prices[index])
            if price <= 0:
                continue
            # Strictly worse than the limit stops the walk: levels are best-first, so
            # nothing beyond this one can be acceptable either.
            if (buying and price > limit_price) or (not buying and price < limit_price):
                break
            size = from_scaled(sizes[index])
            if size <= 0:
                continue
            take = size if size < remaining else remaining
            cost += take * price
            taken += take
            remaining -= take
            levels += 1
            if remaining <= 0:
                break

        if taken <= 0:
            return CrossResult(qty=Decimal(0), price=Decimal(0), levels=0)
        average = cost / taken

    return CrossResult(
        qty=taken, price=quantize_fill_price(average, tick_size, side), levels=levels
    )


# ------------------------------------------------------------------------------- registry


def _check_bps(value: Money, name: str) -> None:
    if value < 0:
        raise ValueError(
            f"{name} {value} is negative, which would model execution that is "
            "systematically better than the market"
        )
    if value > BPS:
        raise ValueError(
            f"{name} {value} is over 100%; that is not an execution assumption, it is a typo"
        )


_MODELS: dict[str, type] = {
    "BAR_CLOSE": BarCloseFillModel,
    "TRADE_ONLY": TradeOnlyFillModel,
    "BOOK_TICKER": BookTickerFillModel,
    "BOOK_WALK": BookWalkFillModel,
}


def fill_model_for_tier(tier: str) -> FillModel:
    """The default model for a tier, refusing to invent one for an unknown name."""
    try:
        return _MODELS[tier]()  # type: ignore[return-value]
    except KeyError:
        raise ValueError(
            f"unknown fill tier {tier!r}; this build knows {sorted(_MODELS)}"
        ) from None


def fill_model_from_json(payload: dict[str, str] | None) -> FillModel:
    """Rebuild a model from a stored run spec.

    Refuses an unknown tier rather than falling back to a default, for the same reason
    `latency_from_json` does: a spec naming a model this build cannot construct describes a
    run whose fills were priced by rules that are not present, and substituting a default
    produces a *different* run wearing the original's identity (spec 12.1).
    """
    if payload is None:
        return BarCloseFillModel()
    tier = payload.get("tier")
    if tier == "BAR_CLOSE":
        return BarCloseFillModel(slippage_bps=parse_money(str(payload.get("slippage_bps", "1.0"))))
    if tier == "TRADE_ONLY":
        return TradeOnlyFillModel(spread_bps=parse_money(str(payload.get("spread_bps", "1.0"))))
    if tier == "BOOK_TICKER":
        return BookTickerFillModel(
            impact_k_bps=parse_money(str(payload.get("impact_k_bps", "10")))
        )
    if tier == "BOOK_WALK":
        return BookWalkFillModel(
            depth_exhaustion_pct=parse_money(str(payload.get("depth_exhaustion_pct", "0.001")))
        )
    raise ValueError(
        f"unknown fill model tier {tier!r}; this build knows {sorted(_MODELS)}. "
        "Substituting a default would re-price every fill in the run while leaving its "
        "identity unchanged (spec 12.1)."
    )
