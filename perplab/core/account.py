"""The accounting engine (spec 3.3, 3.8) -- the crown jewels.

Everything the platform reports about a strategy is derived from this file. A backtest is
an opinion about the future; the ledger is a statement about arithmetic, and it is the only
part of the system where being approximately right is worthless.

**What lives here.** Wallet balance, positions, entry prices, fill application, fees, the
funding hand-off, the liquidation model, and the isolated-margin bookkeeping that ties them
together. Bracket arithmetic is `margin.py`, the funding formula is `funding.py`, and the
conservation checks are `invariants.py`; this module is what calls them in the right order.

**Three things it deliberately does not do.**

*It does not decide when.* Spec 6.2 fixes a total event ordering -- mark price, then
funding, then the liquidation check, then fills -- and two of those placements are
load-bearing (funding can itself cause the liquidation, and a liquidation must cancel
resting orders before they fill). Encoding that sequence inside `Account` would mean the
backtest and live engines each got to disagree with it. `update_mark`, `apply_funding`,
`check_liquidations` and `apply_fill` are therefore separate calls the engine sequences.

*It does not compute mark price.* Spec 3.4: mark price is ingested from Binance and held
flat between samples, never interpolated and never recomputed from trades.

*It does not decide what a failed invariant means.* It raises. Spec 3.10 wants a backtest
to abort and a live session to trip the kill switch, and that is the caller's distinction.

**Isolated margin, and the number that goes into `P_liq`.** Spec 3.7 derives the
liquidation price with `W` as "the isolated margin allocated to this position (initial
margin plus any added margin), not the whole wallet". Substituting the wallet is the single
easiest way to produce a liquidation price that never triggers -- it asserts that every
dollar in the account is defending this one position, which under isolated margin is
exactly what is not true. `Position.isolated_margin` is that number, and it is what
`liquidation_price` receives.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal, localcontext
from enum import Enum
from typing import Any

from perplab.core import invariants
from perplab.core.funding import funding_cashflow
from perplab.core.margin import (
    BracketTable,
    LiquidationSolution,
    bankruptcy_price,
    initial_margin,
    liquidation_price,
    maintenance_margin,
)
from perplab.core.money import (
    ACCOUNTING_CONTEXT,
    from_scaled,
    quantize_entry_price,
    quantize_money,
)
from perplab.core.types import PositionSide, Side
from perplab.exchange.filters import SymbolFilters

__all__ = [
    "DEFAULT_LEVERAGE",
    "FeeSchedule",
    "Position",
    "AccountEventKind",
    "AccountEvent",
    "FillResult",
    "LiquidationResult",
    "InsufficientMargin",
    "HedgeFlipRefused",
    "PositionKey",
    "position_label",
    "Account",
]

PositionKey = tuple[str, PositionSide]
"""How a position is addressed: symbol plus the side it lives on.

A tuple rather than a formatted string so that neither half can be lost to a parse, and so
that a caller holding a `PositionSide` never has to stringify it to look a position up.
In one-way mode the side is always `PositionSide.BOTH`, so the key is a symbol with a
constant attached and every one-way call site reads the same as it did before.
"""


def position_label(symbol: str, position_side: PositionSide = PositionSide.BOTH) -> str:
    """How a position is named in an event, a warning or a UI row.

    `"BTCUSDT"` in one-way mode and `"BTCUSDT:LONG"` in hedge mode. The side is omitted
    rather than rendered as `:BOTH` so that every event a one-way run writes is byte-identical
    to the ones it wrote before this change -- spec 12.1 compares two runs of the same inputs,
    and a label that gained a suffix would make every historical run incomparable with a
    rerun of itself for a reason that has nothing to do with the strategy.
    """
    return symbol if position_side is PositionSide.BOTH else f"{symbol}:{position_side.value}"

DEFAULT_LEVERAGE = 1
"""Leverage when a symbol's has not been set explicitly.

1x, not the 20x Binance defaults new accounts to. An unset parameter should produce the
least dangerous behaviour available, and a strategy that meant to use leverage will say
so. A default that silently multiplies risk by twenty is the kind of thing that is only
discovered by losing money."""


def _sign(value: Decimal) -> int:
    return (value > 0) - (value < 0)


_SIDE_ORDER = {PositionSide.BOTH: 0, PositionSide.LONG: 1, PositionSide.SHORT: 2}
"""A total order on sides, so anything that iterates positions is reproducible.

Dict insertion order would do it for a single run and would differ between two runs that
opened the same two positions in a different sequence. Spec 12.1 requires a run to be
comparable with itself, and a monitor table whose two rows swap places between refreshes
is the visible half of the same problem."""


# ------------------------------------------------------------------------------- fees


@dataclass(frozen=True, slots=True)
class FeeSchedule:
    """Maker and taker commission rates, as fractions of notional (spec 3.8).

    Spec 3.8 forbids hardcoding these -- they vary by VIP tier and BNB discount and are
    published per account by `GET /fapi/v1/commissionRate`. `source` records where the
    numbers came from so a run's metadata can say whether its fees were measured or
    assumed, which is the difference between a backtest result and a guess about one.

    Rates are stored positive. Spec 3.1: fees are always positive numbers and always
    subtracted, so a negative rate here would mean a rebate, which the maker-rebate tiers
    do exist for but nothing downstream is built to handle. Rejected rather than silently
    inverted.
    """

    maker_rate: Decimal
    taker_rate: Decimal
    source: str = "explicit"

    def __post_init__(self) -> None:
        for label, rate in (("maker", self.maker_rate), ("taker", self.taker_rate)):
            if rate < 0:
                raise ValueError(
                    f"{label} rate {rate} is negative; fees are positive and subtracted "
                    "(spec 3.1). Maker rebates are not modelled."
                )
            if rate >= 1:
                raise ValueError(f"{label} rate {rate} is not a fraction of notional")

    def rate(self, *, is_maker: bool) -> Decimal:
        return self.maker_rate if is_maker else self.taker_rate

    @classmethod
    def all_taker(cls, rate: Decimal, source: str = "explicit") -> FeeSchedule:
        """Charge the taker rate on every fill.

        Spec 3.8's recommended starting point: "Defaulting everything to taker is a safe
        conservative fallback and is the recommended initial setting until the maker/taker
        classifier is validated against real testnet fills." Erring this way costs a
        backtest performance it might have had; erring the other way invents performance
        it never could have had.
        """
        return cls(maker_rate=rate, taker_rate=rate, source=source)

    @classmethod
    def from_commission_payload(cls, payload: dict[str, Any]) -> FeeSchedule:
        """Parse `GET /fapi/v1/commissionRate`, which returns decimal *strings*.

        Unlike `leverageBracket` (see `margin.brackets_from_payload`), this endpoint uses
        the futures API's usual string encoding, so `Decimal(str(...))` is exact. The
        `str()` is still there so that a caller who pre-parsed the JSON with the default
        float handling produces a wrong-but-loud number rather than a `TypeError` three
        frames away.
        """
        return cls(
            maker_rate=Decimal(str(payload["makerCommissionRate"])),
            taker_rate=Decimal(str(payload["takerCommissionRate"])),
            source=f"commissionRate:{payload.get('symbol', '')}",
        )


# --------------------------------------------------------------------------- position


@dataclass(frozen=True, slots=True)
class Position:
    """One symbol's open position under isolated margin.

    Frozen, and replaced wholesale on every mutation. A position that can be edited in
    place is a position that can be edited by something that had no business editing it,
    and the resulting state is unattributable to any event in the log.

    A `Position` with `qty == 0` must never exist -- invariant I4 ties a flat position to a
    `None` entry price, and `Account` deletes the entry instead. The constructor enforces
    it so the illegal state cannot be built at all.
    """

    symbol: str
    qty: Decimal
    entry_price: Decimal
    leverage: int
    position_side: PositionSide = PositionSide.BOTH
    """Which of the symbol's positions this is. `BOTH` in one-way mode -- the default, and
    what every run before hedge mode existed was.

    **Stored rather than derived.** `side` below reads the sign of the quantity, which is
    the right answer for a one-way position and the wrong question for a hedged one: the
    short side of a hedge is the short side regardless, and a fill that would drive it
    through zero is refused rather than flipping it. Keeping this as a field is what makes
    "which position does this fill belong to" answerable before the arithmetic runs.
    """
    extra_margin: Decimal = Decimal(0)
    """Margin added to this position beyond the initial requirement (spec 3.7's "plus any
    added margin"). It raises the liquidation distance without changing the position, and
    it is the one lever a strategy has against a liquidation it can see coming."""
    funding_paid: Decimal = Decimal(0)
    """Cumulative funding settled against *this position's* isolated margin, signed.

    **This field is what makes spec 6.2's R5 true.** R5 states that funding must be settled
    before the liquidation check because "a funding payment reduces margin balance and can
    itself cause liquidation". Under spec 3.7's margin model that is only true if funding
    reaches the isolated allocation: `P_liq` is a function of the margin allocated to the
    position, and spec 3.3 books funding against the *wallet*. Charge it to the wallet
    alone and no funding payment can ever move a liquidation price -- R5 becomes a rule
    about an ordering that cannot matter, and a position one payment away from liquidation
    survives indefinitely.

    Charging it here as well as to the wallet is also what Binance does: on an isolated
    position, funding settles against that position's own margin. Fees and realised PnL
    deliberately do *not* follow suit -- spec 3.9 computes `P_liq = 45 180.72` from an
    initial margin of exactly 500.00 on a position that had already paid a 2.50 fee, so the
    worked example pins fees to the wallet only. See `docs/ACCOUNTING_NOTES.md` A2.
    """

    def __post_init__(self) -> None:
        if self.qty == 0:
            raise ValueError(
                f"{self.symbol}: a flat position has no representation; delete it (I4)"
            )
        if self.entry_price <= 0:
            raise ValueError(f"{self.symbol}: entry price {self.entry_price} must be positive")
        if self.leverage < 1:
            raise ValueError(f"{self.symbol}: leverage {self.leverage} must be at least 1")
        if self.extra_margin < 0:
            raise ValueError(f"{self.symbol}: extra margin {self.extra_margin} is negative")
        if self.position_side is PositionSide.LONG and self.qty < 0:
            raise ValueError(
                f"{self.symbol}: the LONG side of a hedge cannot hold {self.qty}; a sell "
                "beyond it closes the position rather than flipping it"
            )
        if self.position_side is PositionSide.SHORT and self.qty > 0:
            raise ValueError(
                f"{self.symbol}: the SHORT side of a hedge cannot hold {self.qty}; a buy "
                "beyond it closes the position rather than flipping it"
            )

    @property
    def key(self) -> tuple[str, PositionSide]:
        """How `Account.positions` addresses this position."""
        return (self.symbol, self.position_side)

    @property
    def abs_qty(self) -> Decimal:
        return abs(self.qty)

    @property
    def side(self) -> Side:
        """The direction this position is *currently* facing, from its quantity.

        Distinct from `position_side`, and the difference is the whole of hedge mode. This
        is an observation about the quantity; `position_side` is the slot the position
        lives in. They agree in one-way mode, where the slot is `BOTH` and the sign is the
        only thing that says which way the position faces.
        """
        return Side.BUY if self.qty > 0 else Side.SELL

    @property
    def entry_notional(self) -> Decimal:
        """`|Q| * Pe` -- notional at entry, which is what initial margin is posted against.

        Not notional at the current mark. Initial margin is posted once and does not float
        with price; only maintenance margin does (spec 3.6).
        """
        with localcontext(ACCOUNTING_CONTEXT):
            return self.abs_qty * self.entry_price

    @property
    def base_margin(self) -> Decimal:
        return initial_margin(self.entry_notional, self.leverage)

    @property
    def isolated_margin(self) -> Decimal:
        """The `W` in spec 3.7's `P_liq` formula: initial margin, plus added margin, less
        funding already settled against this position.

        **Not floored at zero, deliberately.** An earlier revision clamped it and treated
        an exhausted allocation as an immediate liquidation, which is wrong: spec 3.7's
        trigger is `W + Q*(Pm - Pe) <= q*Pm*MMR - MA`, and that inequality carries
        unrealised PnL. A long that has paid away its whole allocation but is 10% in profit
        is *solvent* -- its margin balance is the profit -- and liquidating it destroys a
        winning position for a reason the exchange would never have applied.

        The formula handles a zero or negative `W` without special-casing; it simply
        returns a liquidation price above the entry (for a long), which is the correct
        statement that only a profitable mark keeps the position alive. What must be
        skipped in that state is invariant I7, whose ordering only holds while the position
        is solvent -- see `Account._solve_liquidation`.

        Quantised to the money seam because this is the figure a liquidation books as
        realised PnL, and `scaled()` reaches it through a division. See
        `money.quantize_money`.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            return quantize_money(self.base_margin + self.extra_margin + self.funding_paid)

    @property
    def reserved_margin(self) -> Decimal:
        """Isolated margin as it counts against the wallet, floored at zero.

        Distinct from `isolated_margin` because the two answer different questions. The
        liquidation formula wants the signed allocation, including an overdrawn one. The
        available-balance calculation wants what is *carved out of the wallet*, and a
        negative allocation does not hand spending power back -- that money is already gone
        from the wallet, and subtracting a negative would credit it twice.
        """
        margin = self.isolated_margin
        return margin if margin > 0 else Decimal(0)

    def scaled(self, factor: Decimal) -> Position:
        """Return this position's margin components scaled by `factor`, for a partial close.

        A reduce releases a proportional share of the allocation, so every component of it
        has to scale together. `base_margin` already does -- it is `|Q|*Pe/L` and the entry
        price is unchanged by a reduce -- but `extra_margin` and `funding_paid` are stored
        amounts and do not.

        Leaving them unscaled was the bug: closing 90% of a long left the remaining 10%
        carrying 100% of the funding the full position had paid, which strips its margin by
        an order of magnitude and drags the liquidation price toward the mark. On a
        long-held, repeatedly-trimmed position that is the difference between surviving and
        being liquidated at a price the exchange would never have triggered.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            return replace(
                self,
                extra_margin=self.extra_margin * factor,
                funding_paid=self.funding_paid * factor,
            )

    def unrealized_pnl(self, mark_price: Decimal) -> Decimal:
        """`uPnL = Q * (Pm - Pe)` (spec 3.3), against mark price -- never last traded price."""
        with localcontext(ACCOUNTING_CONTEXT):
            return self.qty * (mark_price - self.entry_price)


# ------------------------------------------------------------------------------ events


class AccountEventKind(Enum):
    FILL = "FILL"
    FUNDING = "FUNDING"
    LIQUIDATION = "LIQUIDATION"
    MARGIN_ADDED = "MARGIN_ADDED"
    MARGIN_REMOVED = "MARGIN_REMOVED"
    MARK = "MARK"
    """Only logged when `Account(log_marks=True)`. Off by default: a run over a year of
    1-second marks would append 31 million entries that carry no information the market
    data and the run manifest do not already pin down, and an event log nobody can read is
    an event log nobody reads."""


@dataclass(frozen=True, slots=True)
class AccountEvent:
    """One entry in the ledger's event log.

    Every field that moved is recorded alongside the state that resulted, so the log
    reconstructs the account at any point without replaying it. Spec 12.1 hashes this log
    to prove two runs of the same inputs are the same run, so it must contain everything
    that varies and nothing that varies for other reasons -- no wall-clock times, no object
    identities, no iteration-order-dependent text.
    """

    ts_ms: int
    kind: AccountEventKind
    symbol: str
    position_side: PositionSide = PositionSide.BOTH
    """Which of the symbol's positions this event moved.

    **`reconcile()` cannot do its job without this field.** The end-of-run replay rebuilds
    each position from the log and compares it with live state; keyed by symbol alone, a
    hedge account's long and short fills would be summed into one number that matches
    neither position, and I9 would fail on a correct account -- or, worse, a genuinely
    mis-booked pair of fills would cancel out and pass. `BOTH` on every one-way event, which
    is what every event written before hedge mode existed meant.
    """
    qty: Decimal = Decimal(0)
    """Signed fill quantity, or the signed quantity closed by a liquidation."""
    price: Decimal = Decimal(0)
    realized: Decimal = Decimal(0)
    fee: Decimal = Decimal(0)
    funding: Decimal = Decimal(0)
    wallet: Decimal = Decimal(0)
    position: Decimal = Decimal(0)
    entry_price: Decimal | None = None
    detail: str = ""


@dataclass(frozen=True, slots=True)
class FillResult:
    realized: Decimal
    fee: Decimal
    position: Position | None
    event: AccountEvent
    position_side: PositionSide = PositionSide.BOTH
    """Which side the fill was routed to.

    Carried on the result as well as on the event because `position` is `None` when the
    fill closed the side out, and that is exactly the case a caller most needs the side for:
    "which of the two round-trips just ended" has no other answer once the position it
    refers to has been deleted.
    """

    @property
    def closed(self) -> bool:
        return self.position is None


@dataclass(frozen=True, slots=True)
class LiquidationResult:
    symbol: str
    trigger_price: Decimal
    """The solved `P_liq`, not the mark price that crossed it."""
    mark_price: Decimal
    margin_lost: Decimal
    """What the liquidation cost the wallet: the signed isolated allocation, less any
    recovery. Negative exactly when funding had overdrawn the allocation past what the
    close could confiscate -- the wallet is *credited* the price leg the clearance could
    not reach, because the overdraft already left the wallet when the funding settled."""
    recovered: Decimal
    """The fraction of the margin balance *remaining after the close* returned by
    `liquidation_recovery_pct` (spec 3.7: "a fraction of remaining margin"). Zero at the
    default, and zero regardless of the knob when the mark reached bankruptcy -- there
    was nothing left to return."""
    closed_qty: Decimal
    event: AccountEvent
    position_side: PositionSide = PositionSide.BOTH
    """Which side was liquidated.

    In hedge mode one leg can go while the other survives -- that is the point of the two
    allocations being separate -- so a result naming only the symbol would report the wrong
    position as destroyed half the time. Last in the field order, and defaulted, so that
    every existing positional construction still means what it did."""


class InsufficientMargin(ValueError):
    """The account cannot fund the margin or fee this fill requires.

    A `ValueError` rather than an invariant violation: nothing is broken. The order should
    never have been accepted, which is a risk-layer question (spec 7), and the ledger's
    job is to refuse to pretend the money was there.
    """


class HedgeFlipRefused(ValueError):
    """A hedge-mode fill would have driven one side of a position through zero.

    Spec 3.3's case C -- the flip -- is a one-way-mode event. In hedge mode the two sides
    are separate positions, and a sell that exceeds the long side closes the long; it does
    not open a short. Binance refuses the same order for the same reason: a `SELL` carrying
    `positionSide=LONG` is a closing order by construction.

    Refused rather than truncated to the closing quantity, because the two readings of the
    strategy's intent differ by real exposure. "Sell 3" against a long of 1 might have meant
    "close it" or might have meant "close it and go short 2", and filling the smaller of
    those silently would report a strategy that did something it did not ask for. A
    `ValueError` rather than an `InvariantViolation` for the same reason as
    `InsufficientMargin`: nothing in the ledger is broken, the order was never legal.
    """


# ----------------------------------------------------------------------------- account


@dataclass
class Account:
    """The ledger. One wallet, isolated margin, one or two positions per symbol.

    Spec 3.7 explains why v1 is isolated-only: under cross margin a symbol's liquidation
    price depends on the unrealised PnL of every other open position, so there is no closed
    form, and one bad strategy can liquidate every other position in the account. Isolated
    is both simpler and safer, and cross is a v2 item.

    ## Position mode

    Positions are keyed by `(symbol, PositionSide)`, which is Binance's own addressing, and
    `hedge_mode` decides which side values are legal:

    - **One-way** (`hedge_mode=False`, the default and what every run before this change
      was): exactly one position per symbol, keyed `BOTH`. A fill that crosses zero flips
      it -- spec 3.3's case C.
    - **Hedge** (`hedge_mode=True`): a `LONG` and a `SHORT` position on the same symbol,
      **independent in every number that matters**. Each carries its own entry price, so
      each has its own unrealised PnL; each posts its own isolated margin, so each has its
      own liquidation price; each pays or receives funding on its own quantity. They are
      never netted. A perfectly hedged long-and-short pays exactly zero net funding and has
      exactly zero net unrealised PnL, and it gets there by two positions cancelling rather
      than by one position being flat -- which is the economically real answer, because
      either leg can be liquidated on its own while the other survives.

    **A fill's meaning depends on the side it was routed to, so the side is never inferred.**
    In hedge mode a sell is either "reduce the long" or "increase the short", and nothing
    about the fill itself says which. `apply_fill` therefore *requires* an explicit
    `position_side` in hedge mode and refuses to guess -- guessing is the entire bug class
    this addressing exists to eliminate. Binance takes the same position: `positionSide` is
    a required field on a hedge-mode order.

    **Spec 3.3's case C does not exist in hedge mode.** A sell of 3 against a long of 1
    closes the long and stops; it does not open a short of 2. That is the exchange's own
    behaviour -- a `SELL` with `positionSide=LONG` is a closing order and cannot exceed the
    position -- and it is refused here with `HedgeFlipRefused` rather than accommodated,
    because the alternative silently opens exposure on a side the strategy did not name.

    ## What the invariants say about two positions

    - **I1, I5** are wallet identities and are indifferent to how many positions exist.
    - **I2** was already stated as a *sum* over open positions, deliberately, so it needed
      no change: two positions on one symbol are two terms in the same sum.
    - **I3** is asserted per `(symbol, side)` rather than per symbol, and that is a
      restatement rather than a relaxation. Per symbol it would be *false* in hedge mode --
      a buy either grows the long or shrinks the short, so the net of a symbol's fills
      equals neither side's quantity -- and a per-symbol I3 would fail on a correct account.
      Per side it is exactly the original claim: this position holds what was filled into
      it.
    - **I4, I6, I7, I8** are per position or per event and carry over unchanged. I7 in
      particular is solved per side, which is the point: the long leg and the short leg of a
      hedge have different liquidation prices, and one can trigger while the other does not.

    The invariants of spec 3.10 run on every mutation. `strict=False` disables them, which
    is intended for property-test shrinking and profiling and for nothing else -- with them
    off, this class will happily produce a wrong answer quietly, which is the entire
    failure mode they exist to prevent.
    """

    opening_balance: Decimal
    fees: FeeSchedule
    brackets: dict[str, BracketTable] = field(default_factory=dict)
    filters: dict[str, SymbolFilters] = field(default_factory=dict)
    liquidation_recovery_pct: Decimal = Decimal(0)
    """Spec 3.7's sensitivity knob: the fraction of the margin balance *remaining after
    the liquidation's close* that is returned (spec 6.6: `W -= remaining isolated margin
    x (1 - liquidation_recovery_pct)`). Default 0: the clearance consumes the whole
    remainder, so a liquidation destroys the entire isolated margin allocated to the
    position. 1 models a clean close with the remainder returned -- optimistic in the one
    scenario where optimism is most expensive, which is why this is a sensitivity dial
    and not a default. The fraction applies to the *remainder*, not the original
    allocation: an earlier revision recovered a fraction of the whole allocation, which
    at 1.0 modelled a liquidation as a scratch at the entry price -- refunding the price
    loss itself, which no exchange does."""
    enforce_margin: bool = True
    require_brackets: bool = True
    """Refuse to open a position on a symbol with no bracket table.

    `check_liquidations` skips symbols it cannot price, because it runs on every event and
    raising there would make one missing snapshot fatal to a whole multi-symbol run. But
    skipping silently is worse than either: the run completes, never liquidates that
    symbol, and reports an equity curve with no downside bound -- exactly the fiction spec
    1.4 rules out, and one that looks like a *good* result.

    Refusing at the point the position is opened puts the failure where it is diagnosable.
    Set `False` for the deliberate no-margin-modelling mode, which is what the fill-case
    tests use."""
    hedge_mode: bool = False
    """Whether this symbol's positions are addressed `LONG`/`SHORT` rather than `BOTH`.

    An account property, not a per-symbol one, because that is what it is at the exchange:
    `GET /fapi/v1/positionSide/dual` returns one `dualSidePosition` flag for the whole
    account and Binance refuses to change it while any position is open or any order is
    working. Modelling it per symbol would let a run be internally consistent and still
    disagree with the only account it can trade against.
    """
    strict: bool = True
    log_marks: bool = False

    wallet: Decimal = field(init=False)
    positions: dict[PositionKey, Position] = field(init=False, default_factory=dict)
    """Open positions, keyed `(symbol, side)`. See `PositionKey`.

    Never holds a flat position: I4 ties `Q == 0` to no entry price, and the entry is
    deleted rather than zeroed. In hedge mode a symbol may hold two entries, one entry, or
    none, and "one" is not a special case -- a hedge that has closed its short is a long.
    """
    marks: dict[str, Decimal] = field(init=False, default_factory=dict)
    leverages: dict[str, int] = field(init=False, default_factory=dict)
    """Leverage per **symbol**, not per side.

    Binance's own scope: `POST /fapi/v1/leverage` takes a symbol and no `positionSide`, so
    in hedge mode the long and short legs of one symbol necessarily share a leverage. Keying
    this per side would let PerpLab hold a configuration the exchange cannot represent, and
    the first reconciliation would find a margin requirement neither side agreed with."""
    events: list[AccountEvent] = field(init=False, default_factory=list)

    total_realized: Decimal = field(init=False, default=Decimal(0))
    total_fees: Decimal = field(init=False, default=Decimal(0))
    total_funding: Decimal = field(init=False, default=Decimal(0))
    total_liquidation_cost: Decimal = field(init=False, default=Decimal(0))
    """The clearance penalty inside `total_realized`, for spec 8.4 attribution only.

    A *memo* field, not a separate ledger: every amount counted here is already in
    `total_realized`, so none of the spec 3.10 invariants read it and none of them need to.
    It exists so `attribution()` can stop charging a liquidation's penalty to the
    strategy's price edge -- see that method."""
    liquidations: int = field(init=False, default=0)

    _signed_fills: dict[PositionKey, Decimal] = field(init=False, default_factory=dict)
    """Cumulative signed fill quantity per `(symbol, side)` -- the right-hand side of I3.

    Keyed per side rather than per symbol, and that is the whole of what I3 needed for hedge
    mode. Per symbol the sum is meaningless there: a buy routed to the short side *reduces*
    the short, so it enters this accumulator negative on `(symbol, SHORT)` and does not
    touch `(symbol, LONG)` at all. Adding both sides' fills together would produce a number
    equal to neither position."""
    _last_ts_ms: int = field(init=False, default=-1)
    _wallet_was_negative: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        if self.opening_balance < 0:
            raise ValueError(f"opening balance {self.opening_balance} is negative")
        if not (0 <= self.liquidation_recovery_pct <= 1):
            raise ValueError(
                f"liquidation_recovery_pct {self.liquidation_recovery_pct} "
                "must be a fraction between 0 and 1"
            )
        self.wallet = self.opening_balance

    # ------------------------------------------------------------------ derived state

    @property
    def unrealized_pnl(self) -> Decimal:
        """Summed across open positions, each against its own last mark (spec 3.4 LOCF).

        In hedge mode the long and short legs of one symbol are two terms here, each
        measured from **its own entry price**. That is the difference between hedge
        accounting and netting, and it is not cosmetic: a long opened at 50 000 and a short
        opened at 52 000 have a combined unrealised PnL of +2 000 per unit at any mark
        whatsoever, and netting them to a flat position of zero would report that profit as
        nothing at all.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            total = Decimal(0)
            for position in self.positions.values():
                total += position.unrealized_pnl(
                    self._valuation_mark(position.symbol, position)
                )
            return total

    @property
    def equity(self) -> Decimal:
        """`E = W + uPnL` (spec 3.3). Margin balance -- what the account is actually worth."""
        with localcontext(ACCOUNTING_CONTEXT):
            return self.wallet + self.unrealized_pnl

    @property
    def allocated_margin(self) -> Decimal:
        with localcontext(ACCOUNTING_CONTEXT):
            return sum((p.reserved_margin for p in self.positions.values()), Decimal(0))

    @property
    def available_balance(self) -> Decimal:
        """Wallet less margin already carved out to isolated positions.

        Unrealised PnL is deliberately absent from this figure. Under isolated margin a
        position's gains and losses stay inside its own allocation until it is closed, so
        counting an unrealised profit as spendable would let a strategy pyramid on money
        that a single adverse tick can erase -- and counting an unrealised loss would
        double-charge it, since the allocation is already reserved.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            return self.wallet - self.allocated_margin

    # ------------------------------------------------------------------- addressing

    def _resolve_side(self, symbol: str, position_side: PositionSide | None) -> PositionSide:
        """Turn an optional side into the one this account can address, or refuse.

        The refusal is the interesting half. In hedge mode `account.position("BTCUSDT")`
        has **two** answers, and every way of picking one is wrong in a way that is hard to
        see afterwards: returning `BOTH` finds nothing and reads as flat, returning the only
        open side works right up until the other side opens, and returning the larger one
        makes the answer depend on the market. So the question is refused instead, and the
        caller has to say which position it means -- which it always knows, because it either
        came from an order that carried a side or from a loop over `positions_for`.

        One-way mode accepts `None` and `BOTH` interchangeably and refuses `LONG`/`SHORT`,
        which is the mirror image: a one-way account has no side to route to.
        """
        if self.hedge_mode:
            if position_side is None or position_side is PositionSide.BOTH:
                raise ValueError(
                    f"{symbol}: this account is in hedge mode, so it can hold a LONG and a "
                    "SHORT position on this symbol at once and 'the position' names neither "
                    "of them. Pass position_side=PositionSide.LONG or .SHORT, or use "
                    "positions_for(symbol) to see both."
                )
            return position_side
        if position_side is not None and position_side is not PositionSide.BOTH:
            raise ValueError(
                f"{symbol}: this account is in one-way mode, where a symbol has a single "
                f"position keyed BOTH; {position_side.value} has no meaning here. Construct "
                "the Account with hedge_mode=True to address sides separately."
            )
        return PositionSide.BOTH

    def position(self, symbol: str, position_side: PositionSide | None = None) -> Position | None:
        return self.positions.get((symbol, self._resolve_side(symbol, position_side)))

    def positions_for(self, symbol: str) -> tuple[Position, ...]:
        """Every open position on `symbol`, longs before shorts.

        Zero, one or two entries. Ordered rather than left to dict insertion order because
        this feeds the monitor payload and the run record, and a table whose rows reorder
        between two refreshes of the same unchanged account is a table nobody trusts.
        """
        found = [p for (sym, _), p in self.positions.items() if sym == symbol]
        found.sort(key=lambda p: _SIDE_ORDER[p.position_side])
        return tuple(found)

    def symbols(self) -> tuple[str, ...]:
        """Symbols with at least one open position, in a stable order."""
        return tuple(sorted({sym for sym, _ in self.positions}))

    def qty(self, symbol: str, position_side: PositionSide | None = None) -> Decimal:
        position = self.position(symbol, position_side)
        return position.qty if position else Decimal(0)

    def gross_qty(self, symbol: str) -> Decimal:
        """`|Q_long| + |Q_short|` -- the quantity this symbol has *at risk*.

        The agreed reading of exposure under hedge mode, and it is the conservative one on
        purpose. Both sides post their own isolated margin and either can be liquidated on
        its own, so a long 1 against a short 1 is two positions that can each be destroyed
        rather than one position of zero. Netting them would report a symbol that cannot
        lose money and let a strategy pile on unbounded size behind a hedge that only looks
        flat.

        Identical to `abs(qty(symbol))` in one-way mode, where there is one term.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            return sum(
                (p.abs_qty for p in self.positions_for(symbol)), Decimal(0)
            )

    def net_qty(self, symbol: str) -> Decimal:
        """`Q_long + Q_short` -- the directional quantity, signed.

        Reported alongside `gross_qty` rather than instead of it, because the two answer
        different questions and the pair is what makes a hedge legible: a gross of 2 with a
        net of 0 is a market-neutral pair, and a gross of 2 with a net of 2 is a doubled
        long. **Never used for margin, exposure or liquidation** -- see `gross_qty`.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            return sum((p.qty for p in self.positions_for(symbol)), Decimal(0))

    def has_position(self, symbol: str) -> bool:
        """Whether any side of `symbol` is open. Safe to ask in either mode."""
        return any(sym == symbol for sym, _ in self.positions)

    def _mark(self, symbol: str) -> Decimal:
        """The recorded mark, or a refusal. Used wherever a wrong mark costs money.

        Funding and liquidation both go through here. Spec 3.4 is categorical that mark
        price is ingested rather than derived, and settling a real cashflow or destroying
        a position against a guessed one is exactly the fiction it rules out.
        """
        try:
            return self.marks[symbol]
        except KeyError:
            raise LookupError(
                f"no mark price recorded for {symbol}; call update_mark() before "
                "settling funding or liquidation-checking the position (spec 3.4 -- mark "
                "price is ingested, never inferred from fills)"
            ) from None

    def _valuation_mark(self, symbol: str, position: Position) -> Decimal:
        """The recorded mark, falling back to the entry price for *valuation only*.

        Between the first fill and the first mark sample there is no mark to value
        against, and the honest answer for unrealised PnL in that window is zero -- which
        is what valuing at entry produces. The alternative, raising, makes `equity`
        unusable in a state the engine legitimately passes through, and pushes every
        caller into writing the same fallback less carefully.

        This is not a back door around spec 3.4. Nothing that costs money uses it: funding
        goes through `_mark`, and `check_liquidations` skips symbols with no recorded mark
        outright. A position is never liquidated, and a cashflow never settled, against an
        entry price standing in for a mark. Once the first real sample arrives the fallback
        can never be consulted again for that symbol.
        """
        return self.marks.get(symbol, position.entry_price)

    # ---------------------------------------------------------------------- leverage

    def set_leverage(self, symbol: str, leverage: int) -> None:
        """Set the leverage a new position on `symbol` will open at.

        Refused while a position is open. Binance does permit changing leverage on an open
        position, but the resulting margin adjustment has edge cases (it can be rejected
        for insufficient margin, and it silently re-resolves the bracket) that would be
        modelled here on guesswork rather than on observed behaviour. Spec 1.4 prefers a
        loud refusal to a plausible invention; `add_margin` covers the case that actually
        matters, which is wanting more room before a liquidation.
        """
        if leverage < 1:
            raise ValueError(f"leverage {leverage} must be at least 1")
        if self.has_position(symbol):
            # Either side, in hedge mode. Leverage is per symbol at the exchange, so
            # changing it while the *short* leg is open would silently re-price the long
            # leg's margin requirement too -- the case that would be missed by checking
            # only the side being asked about.
            raise ValueError(
                f"{symbol}: cannot change leverage while a position is open; "
                "close it or use add_margin()"
            )
        table = self.brackets.get(symbol)
        if table is not None:
            top = max(b.max_leverage for b in table.brackets)
            if leverage > top:
                raise ValueError(
                    f"{symbol}: leverage {leverage} exceeds the highest bracket's "
                    f"maximum of {top}"
                )
        self.leverages[symbol] = leverage

    def leverage(self, symbol: str) -> int:
        """This symbol's leverage: an open position's, or the configured default.

        Per symbol in both modes, and there is no `position_side` parameter by design --
        `POST /fapi/v1/leverage` has no such field, so a hedge's two legs cannot have
        different leverages at the exchange and must not appear to have them here. See
        `leverages`.
        """
        return self._leverage_for(symbol, None)

    # -------------------------------------------------------------------- mark price

    def update_mark(self, ts_ms: int, symbol: str, price: Decimal) -> None:
        """Record a mark price sample (spec 3.4).

        Does **not** run the liquidation check. Spec 6.2 puts funding between the mark
        update and the liquidation check precisely so a funding payment can cause the
        liquidation it would otherwise have escaped; folding the check in here would
        reinstate that bug in the one place it is hardest to see.
        """
        if price <= 0:
            raise ValueError(f"{symbol}: mark price {price} must be positive")
        self._touch(ts_ms)
        self.marks[symbol] = price

        if self.strict:
            invariants.check_equity(self.equity, self.wallet, self._position_view())

        if self.log_marks:
            # One record per open side. In hedge mode the long and the short are marked by
            # the same sample and are two different positions with two different unrealised
            # PnLs, so a single row would have to pick one of them to describe. A symbol
            # that is flat still logs the mark, with no position attached, because the
            # sample happened.
            open_sides = self.positions_for(symbol)
            for position in open_sides or (None,):
                self._append(
                    AccountEvent(
                        ts_ms=ts_ms,
                        kind=AccountEventKind.MARK,
                        symbol=symbol,
                        position_side=(
                            position.position_side if position else PositionSide.BOTH
                        ),
                        price=price,
                        wallet=self.wallet,
                        position=position.qty if position else Decimal(0),
                        entry_price=position.entry_price if position else None,
                    )
                )

    # ------------------------------------------------------------------------- fills

    def apply_fill(
        self,
        ts_ms: int,
        symbol: str,
        qty: Decimal,
        price: Decimal,
        *,
        position_side: PositionSide | None = None,
        is_maker: bool = False,
        fee: Decimal | None = None,
        detail: str = "",
    ) -> FillResult:
        """Apply a fill of signed quantity `qty` at `price` (spec 3.3).

        Spec 3.3 calls this "the single most bug-prone function in the codebase" and the
        three cases are transcribed from it directly:

        - **A, open or increase** (`Q == 0` or same sign): entry price becomes the
          quantity-weighted average; nothing is realised.
        - **B, reduce** (`|f| <= |Q|`, opposite sign): `realized = sign(Q) * |f| * (Pf - Pe)`
          and **the entry price does not change**. That last part is marked critical in the
          spec and it is worth saying why: recomputing entry on a partial close would move
          the cost basis of the remaining position, so the PnL of the *next* close would be
          measured from a price that was never paid, and the error would net out only if
          the position were closed all at once.
        - **C, flip** (`|f| > |Q|`, opposite sign): the old position realises in full and
          the residual opens fresh at the fill price.

        In every case `fee = |f| * Pf * rate` and `W' = W + realized - fee`.

        ## Fees are booked at the money seam's 8 decimal places

        The scheduled fee is quantised with `quantize_money` before it touches the
        ledger. `|f| * Pf * rate` is exact `Decimal` arithmetic, and that is the problem:
        a 6-decimal commission rate against a 3-decimal quantity and a 1-decimal price is
        already a 10-decimal product, and the venue charges nothing of the kind --
        Binance reports `commission` as an 8-decimal string, like every other amount it
        settles. Booked unrounded, the sub-1e-8 residue is invisible to every invariant
        (both sides of I1 carry the same figure) and lands in `total_fees` and the
        wallet, where it guarantees the ledger can never again agree with the venue's own
        balance to the cent -- which is the standard spec 6.7.3's live reconciliation
        holds it to, and the Phase 8 exit criterion is written in exactly those words.

        ## The `fee` override: the venue's actual commission

        `fee`, when given, is the commission the venue *actually charged* for this fill
        -- parsed off a live execution report -- and it is booked in place of the
        schedule's estimate, quantised identically and flowing into `total_fees`, the
        wallet and the event exactly as a scheduled fee would. The venue's charge is
        ground truth; the schedule is a model of it. VIP tier changes, the BNB discount
        and promotional rates all move the real rate under the model's feet, and spec
        6.7.3 compares the wallet against the exchange every 60 seconds and trips the
        kill switch on a mismatch -- so booking a modelled fee while holding the real one
        would manufacture precisely the divergence that check exists to catch. A negative
        commission (the maker-rebate tiers) is refused for the same reason `FeeSchedule`
        refuses a negative rate: spec 3.1 books fees as positive amounts, always
        subtracted, and nothing downstream models a rebate. When `fee` is None, the
        scheduled path runs unchanged.

        ## Hedge mode

        `position_side` says which of the symbol's two positions this fill belongs to, and in
        hedge mode it is **required** -- see `_resolve_side` for why nothing here is willing
        to infer it. Once routed, the three cases above apply to that side alone and against
        its own entry price, which is the whole of what makes the two positions independent.

        Two things change on the routed side, and both are refusals rather than arithmetic:

        - **Case C cannot happen.** A sell exceeding the long side would drive it negative,
          which is a short in one-way clothing. `HedgeFlipRefused`.
        - **A side can only be opened in its own direction.** A sell with no long position
          open is not "short the long side"; it is an order for the short side that named
          the wrong one, and the exchange rejects it identically.
        """
        if qty == 0:
            raise ValueError(f"{symbol}: a fill of zero quantity is not a fill")
        if price <= 0:
            raise ValueError(f"{symbol}: fill price {price} must be positive")
        if fee is not None and fee < 0:
            raise ValueError(
                f"{symbol}: venue commission {fee} is negative; fees are positive and "
                "subtracted (spec 3.1). Maker rebates are not modelled."
            )

        side = self._resolve_side(symbol, position_side)
        key = (symbol, side)

        self._touch(ts_ms)
        self._check_tick_and_step(symbol, price, abs(qty))

        existing = self.positions.get(key)
        if existing is None and self.require_brackets and symbol not in self.brackets:
            raise LookupError(
                f"{symbol}: no leverage bracket table loaded, so this position could "
                "never be liquidated and the run would report an unbounded downside. "
                "Load a snapshot, or pass require_brackets=False to model without margin."
            )
        # Leverage is a symbol-level setting at the exchange, so the *other* side's leverage
        # is authoritative for a side opening now. Reading only `existing` would let a hedge
        # open its short at the account default while its long ran at 10x.
        leverage = self._leverage_for(symbol, existing)

        with localcontext(ACCOUNTING_CONTEXT):
            charged = quantize_money(
                fee
                if fee is not None
                else abs(qty) * price * self.fees.rate(is_maker=is_maker)
            )

            if existing is None:
                self._require_opening_direction(symbol, side, qty)
                new_qty = qty
                new_entry: Decimal | None = price
                realized = Decimal(0)
                extra = Decimal(0)
                carried_funding = Decimal(0)
            else:
                held, entry = existing.qty, existing.entry_price
                extra = existing.extra_margin
                carried_funding = existing.funding_paid

                if _sign(qty) == _sign(held):
                    # Case A -- increase. VWAP over the combined position. The allocation
                    # carries over whole: it is the same position, grown.
                    new_qty = held + qty
                    new_entry = quantize_entry_price(
                        (abs(held) * entry + abs(qty) * price) / (abs(held) + abs(qty))
                    )
                    realized = Decimal(0)
                elif abs(qty) <= abs(held):
                    # Case B -- reduce. Entry price is untouched, deliberately.
                    realized = Decimal(_sign(held)) * abs(qty) * (price - entry)
                    new_qty = held + qty
                    new_entry = entry if new_qty != 0 else None
                    # The allocation shrinks with the position. `base_margin` follows the
                    # quantity on its own; the two stored components have to be scaled by
                    # the same fraction or the residual inherits the whole position's
                    # funding history and added margin. See `Position.scaled`.
                    if new_qty != 0:
                        shrunk = existing.scaled(abs(new_qty) / abs(held))
                        extra, carried_funding = shrunk.extra_margin, shrunk.funding_paid
                elif side.is_hedged:
                    raise HedgeFlipRefused(
                        f"{symbol} {side.value}: a fill of {qty} against a position of "
                        f"{held} would drive this side through zero. In hedge mode that is "
                        f"not a flip -- the opposite side is a separate position with its "
                        f"own entry price and its own liquidation price, and this order "
                        f"names neither of them. Close {abs(held)} on {side.value}, and "
                        f"send the remaining {abs(qty) - abs(held)} to "
                        f"{PositionSide.SHORT.value if side is PositionSide.LONG else PositionSide.LONG.value} "
                        f"if opening the other side was the intent."
                    )
                else:
                    # Case C -- flip. The old position realises in full and its allocation
                    # is released; the residual is a *new* position on the other side,
                    # opened at this fill's price with a clean allocation. Carrying the old
                    # long's funding history into the new short would move a liquidation
                    # price for a reason that stopped existing.
                    realized = Decimal(_sign(held)) * abs(held) * (price - entry)
                    new_qty = held + qty
                    new_entry = price
                    extra = Decimal(0)
                    carried_funding = Decimal(0)

            updated = (
                None
                if new_qty == 0
                else Position(
                    symbol=symbol,
                    qty=new_qty,
                    entry_price=new_entry,  # type: ignore[arg-type]
                    leverage=leverage,
                    position_side=side,
                    extra_margin=extra,
                    funding_paid=carried_funding,
                )
            )

            self._require_margin(symbol, existing, updated, charged, realized)

            self.wallet += realized - charged
            self.total_realized += realized
            self.total_fees += charged
            self._signed_fills[key] = self._signed_fills.get(key, Decimal(0)) + qty

        if updated is None:
            self.positions.pop(key, None)
        else:
            self.positions[key] = updated

        event = AccountEvent(
            ts_ms=ts_ms,
            kind=AccountEventKind.FILL,
            symbol=symbol,
            position_side=side,
            qty=qty,
            price=price,
            realized=realized,
            fee=charged,
            wallet=self.wallet,
            position=new_qty,
            entry_price=new_entry,
            detail=detail or ("maker" if is_maker else "taker"),
        )
        self._append(event)
        self._check_after_fill(key)

        return FillResult(
            realized=realized,
            fee=charged,
            position=updated,
            event=event,
            position_side=side,
        )

    def _leverage_for(self, symbol: str, existing: Position | None) -> int:
        """The leverage a position on `symbol` opens or continues at.

        The *other* side's leverage counts. `POST /fapi/v1/leverage` takes a symbol and no
        `positionSide`, so a hedge's two legs necessarily share one leverage at the
        exchange; letting the second leg open at `DEFAULT_LEVERAGE` because its own slot was
        empty would post the wrong initial margin and solve a liquidation price the exchange
        does not agree with -- on the leg that opened *later*, which is the one nobody
        re-checks.
        """
        if existing is not None:
            return existing.leverage
        for other in self.positions_for(symbol):
            return other.leverage
        return self.leverages.get(symbol, DEFAULT_LEVERAGE)

    def _require_opening_direction(
        self, symbol: str, side: PositionSide, qty: Decimal
    ) -> None:
        """A hedge side may only be opened by a fill in its own direction.

        `Position.__post_init__` would catch the resulting negative `LONG` anyway, but it
        would report it as an impossible position rather than as a misrouted order, and the
        message an operator reads decides whether they look at the ledger or at the strategy.
        """
        wanted = side.opening_sign
        if wanted and _sign(qty) != wanted:
            opposite = (
                PositionSide.SHORT if side is PositionSide.LONG else PositionSide.LONG
            )
            raise HedgeFlipRefused(
                f"{symbol} {side.value}: a fill of {qty} cannot open the {side.value} side, "
                f"which only grows on a {'buy' if wanted > 0 else 'sell'}. Nothing is open "
                f"on {side.value} to reduce. Route this to {opposite.value} if the intent "
                f"was to open the other side."
            )

    # ----------------------------------------------------------------------- funding

    def apply_funding(
        self,
        ts_ms: int,
        symbol: str,
        rate: Decimal,
        mark_price: Decimal | None = None,
    ) -> Decimal:
        """Settle funding and return the **total** signed cashflow across every open side.

        The sum is what the wallet moved by, so this stays the right answer for any caller
        that cares about the balance. A caller that has to attribute the payment to a
        particular round-trip needs `apply_funding_by_side`, because a perfectly hedged
        symbol's total is zero and attributing zero to both legs would erase a payment each
        of them really made.
        """
        return sum(
            self.apply_funding_by_side(ts_ms, symbol, rate, mark_price).values(),
            Decimal(0),
        )

    def apply_funding_by_side(
        self,
        ts_ms: int,
        symbol: str,
        rate: Decimal,
        mark_price: Decimal | None = None,
    ) -> dict[PositionSide, Decimal]:
        """Settle funding on the open position(s) (spec 3.5). Returns the cashflow per side.

        `mark_price` defaults to the last recorded mark, which is what spec 3.4's
        last-observation-carried-forward rule prescribes between samples. Passing it
        explicitly is for the case the spec cares most about: the settlement mark is the
        one published *at* the settlement instant, and a strategy that trades around
        funding lives or dies on that being the right number rather than a nearby one.

        A flat position settles nothing and, importantly, does not log an event -- spec
        3.5's first rule is that funding is discrete, so a zero cashflow is not an event
        that happened, it is an event that did not.

        **Both sides settle in hedge mode, separately, and the total is returned.** Binance
        charges funding per position side on that side's own quantity, so a long 1 and a
        short 1 at the same mark pay and receive the same amount and net to exactly zero.
        Settling one side only -- or settling the net quantity once -- would make a
        market-neutral hedge leak funding it never paid, and over a month of eight-hourly
        settlements that is the difference between a carry strategy that works and one that
        does not. Each side is charged against **its own** isolated margin, which is what
        lets funding push one leg toward liquidation while the other leg is unaffected.
        """
        self._touch(ts_ms)

        open_sides = self.positions_for(symbol)
        if not open_sides:
            return {}

        mark = mark_price if mark_price is not None else self._mark(symbol)
        settled: dict[PositionSide, Decimal] = {}

        for position in open_sides:
            cashflow = funding_cashflow(position.qty, mark, rate)

            with localcontext(ACCOUNTING_CONTEXT):
                self.wallet += cashflow
                self.total_funding += cashflow
                settled[position.position_side] = cashflow
                # Charged against the position's own margin as well as the wallet. This is
                # what gives spec 6.2's R5 ordering something to protect: without it no
                # funding payment can move a liquidation price and the rule is inert. See
                # `Position.funding_paid`.
                self.positions[position.key] = replace(
                    position, funding_paid=position.funding_paid + cashflow
                )

            self._append(
                AccountEvent(
                    ts_ms=ts_ms,
                    kind=AccountEventKind.FUNDING,
                    symbol=symbol,
                    position_side=position.position_side,
                    price=mark,
                    funding=cashflow,
                    wallet=self.wallet,
                    position=position.qty,
                    entry_price=position.entry_price,
                    detail=f"rate={rate}",
                )
            )

        self._check_wallet()
        return settled

    # ------------------------------------------------------------------- liquidation

    def liquidation_price(
        self, symbol: str, position_side: PositionSide | None = None
    ) -> Decimal | None:
        """Solve `P_liq` for the open position, or `None` if flat or unreachable.

        Requires a bracket table for the symbol. There is no fallback and no default MMR:
        spec 3.6 says never hardcode the table, and a made-up maintenance rate produces a
        liquidation price that is wrong by exactly the amount nobody would notice.

        **Solved per side.** The long and short legs of a hedge post separate isolated
        margin against separate entry prices, so they have genuinely different liquidation
        prices -- typically one above the mark and one below it -- and either can trigger
        while the other does not. There is no such thing as "the symbol's liquidation
        price" in hedge mode, which is why this refuses a missing side rather than picking.
        """
        solution = self.liquidation_solution(symbol, position_side)
        if solution is None:
            return None
        return solution.price if solution.reachable else None

    def liquidation_solution(
        self, symbol: str, position_side: PositionSide | None = None
    ) -> LiquidationSolution | None:
        """The full spec 3.6 solve -- price, resolved bracket, and iteration count.

        Exposed alongside `liquidation_price` because the price alone cannot be
        distinguished from one computed under the wrong tier, and the iteration count is
        the only evidence the fixed point actually had to run. A caller checking that a
        multi-tier table was resolved rather than assumed needs `iterations`, not a number
        that looks plausible either way.
        """
        position = self.position(symbol, position_side)
        if position is None:
            return None
        return self._solve_liquidation(symbol, position)

    def check_liquidations(self, ts_ms: int) -> list[LiquidationResult]:
        """Liquidate every position whose mark has crossed its liquidation price (spec 3.7).

        Long positions trigger at `Pm <= P_liq`, shorts at `Pm >= P_liq`, both evaluated
        against the **mark price series** -- not last traded price. Spec 3.4 is blunt about
        the distinction: fills happen at traded prices, risk happens at mark price, and
        conflating them is a classic and expensive bug. A wick on the trade tape that the
        mark never followed did not liquidate anybody.

        Symbols with no bracket table are skipped rather than guessed at, and symbols with
        no mark yet are skipped rather than valued at their entry price.

        **Each side is checked independently, and both can trigger on the same mark.** That
        is not a corner case to guard against -- it is what happens when a hedge's margin has
        been drained by funding on both legs, and reporting only the first one would leave a
        destroyed position quietly open in the ledger. The iteration order is
        `_SIDE_ORDER`-stable so a run that liquidates two legs at once liquidates them in
        the same order every time.
        """
        self._touch(ts_ms)
        results: list[LiquidationResult] = []

        for key in sorted(self.positions, key=lambda k: (k[0], _SIDE_ORDER[k[1]])):
            symbol = key[0]
            position = self.positions.get(key)
            if position is None or symbol not in self.brackets or symbol not in self.marks:
                continue

            mark = self.marks[symbol]
            solution = self._solve_liquidation(symbol, position)
            if not solution.reachable:
                continue

            triggered = (
                mark <= solution.price if position.qty > 0 else mark >= solution.price
            )
            if triggered:
                results.append(self._liquidate(ts_ms, position, solution.price, mark))

        return results

    def _solve_liquidation(self, symbol: str, position: Position) -> LiquidationSolution:
        table = self.brackets.get(symbol)
        if table is None:
            raise LookupError(
                f"{symbol}: no leverage bracket table loaded; margin cannot be resolved "
                "and spec 3.6 forbids hardcoding one"
            )

        margin = position.isolated_margin
        solution = liquidation_price(
            qty=position.qty,
            entry_price=position.entry_price,
            margin=margin,
            mark_price=self._mark(symbol),
            table=table,
        )

        # I7 is checked only while the position is solvent, and that restriction comes
        # straight out of the algebra rather than being a convenience. For a long,
        # `P_liq < Pe` holds if and only if `W > q*Pe*MMR - MA` -- the allocation exceeds
        # the maintenance requirement at the entry notional. A position whose funding has
        # eaten its margin fails that test legitimately: its liquidation price *is* above
        # its entry, which is the correct statement that only an unrealised profit is
        # keeping it alive. Asserting I7 there would report "the bracket resolution is
        # wrong" about a bracket resolution that is right.
        if self.strict and margin > maintenance_margin(
            position.entry_notional, solution.bracket
        ):
            invariants.check_liquidation_ordering(
                position.qty,
                position.entry_price,
                solution.price,
                bankruptcy_price(position.qty, position.entry_price, margin),
            )
        return solution

    def _liquidate(
        self,
        ts_ms: int,
        position: Position,
        trigger_price: Decimal,
        mark: Decimal,
    ) -> LiquidationResult:
        """Close the position at the mark and forfeit its remaining margin (spec 3.7, 6.6).

        Spec 6.6's model is a close followed by a confiscation: the position realises its
        price PnL, and the margin balance *remaining after that close* is destroyed --
        `W -= remaining isolated margin x (1 - liquidation_recovery_pct)`. Both legs are
        booked here as one realised figure, so the wallet moves by exactly
        `recovered - isolated_margin`: at the default zero recovery a liquidation costs
        the account precisely its allocation, which is spec 3.7's "the entire isolated
        margin allocated to that position is lost". The close is priced at the observed
        mark that triggered rather than at spec 6.6's literal `P_liq`, for spec 3.4's
        reason -- the mark is an observation and the solved price is a model output --
        and the deviation errs pessimistic: the remaining balance the recovery fraction
        sees is measured at the worst price the mark series actually printed.

        **The price leg is booked, not evaporated.** An earlier revision booked the whole
        outcome as `-reserved_margin` and never realised the position's price PnL. For a
        solvent position the two agree -- the price move plus the remaining margin *is*
        the allocation -- but `reserved_margin` floors at zero, and funding routinely
        drives the signed allocation negative (see `Position.isolated_margin`). A
        position in that state liquidated while in profit booked a realised PnL of zero,
        was deleted, and its unrealised profit left the books with no ledger entry at
        all. Equity and unrealised PnL dropped together, so I1 and I9 both stayed green
        while the run's reported PnL was wrong by the whole profit -- unbounded, and
        always overstating the loss. The credit an overdrawn position receives here is
        not a bonus for being liquidated: it is the price leg the confiscation could not
        reach, and the funding that overdrew the allocation already left the wallet when
        it settled.

        **The account never loses more than its allocation.** On a mark that gapped
        through the trigger to beyond the bankruptcy price, the remaining balance is
        negative and there is nothing left to confiscate; the shortfall lands on the
        exchange's insurance fund, which spec 3.7 deliberately does not model as an
        account cashflow. The price leg booked into `attribution()`'s price column is
        capped at the allocation accordingly.

        **No separate commission is charged.** Spec 3.8 classifies a liquidation as a
        taker event, and spec 3.7 models the outcome as the loss of the remaining margin
        precisely *because* "the clearance fee plus adverse fill consume the remaining
        margin" -- the fee is already inside the number. Charging one on top would
        double-count it and, worse, could push the wallet negative in a way invariant I5
        would then have to excuse.

        `total_liquidation_cost` memos the clearance penalty alone -- the confiscated
        remainder, `-(remaining - recovered)`, never positive -- so spec 8.4's
        attribution can keep the penalty out of the strategy's price edge. See
        `attribution`.
        """
        symbol = position.symbol
        key = position.key
        with localcontext(ACCOUNTING_CONTEXT):
            # The *signed* allocation. Flooring it here was the bug this docstring
            # describes: the floor belongs to `reserved_margin`'s question (what is
            # carved out of the wallet), not to this one (what does the account settle).
            margin = position.isolated_margin
            # The close the liquidation engine performs. Quantised because the mark
            # arrives exact but the product joins the wallet, and the seam books at 8dp.
            price_leg = quantize_money(
                position.qty * (mark - position.entry_price)
            )
            # Margin balance left after the close: for a solvent position, roughly the
            # maintenance margin the trigger fired early to preserve. This is what the
            # clearance consumes and what the recovery knob returns a fraction of --
            # spec 6.6 says "remaining isolated margin", not "original allocation", and
            # recovering from the original allocation would refund price losses the
            # market already took.
            remaining = margin + price_leg
            confiscated = remaining if remaining > 0 else Decimal(0)
            recovered = quantize_money(confiscated * self.liquidation_recovery_pct)
            penalty = -(confiscated - recovered)

            # == price_leg + penalty whenever the remainder was non-negative; == -margin
            # (the insurance-fund cap) when the mark gapped past bankruptcy.
            loss = recovered - margin
            self.total_liquidation_cost += penalty

            self.wallet += loss
            self.total_realized += loss
            self._signed_fills[key] = (
                self._signed_fills.get(key, Decimal(0)) - position.qty
            )

        del self.positions[key]
        self.liquidations += 1

        event = AccountEvent(
            ts_ms=ts_ms,
            kind=AccountEventKind.LIQUIDATION,
            symbol=symbol,
            position_side=position.position_side,
            qty=-position.qty,
            price=trigger_price,
            realized=loss,
            wallet=self.wallet,
            position=Decimal(0),
            entry_price=None,
            detail=(
                f"mark={mark} margin_lost={margin - recovered} "
                f"price_leg={price_leg} clearance={confiscated - recovered}"
            ),
        )
        self._append(event)
        self._check_after_fill(key, liquidating=True)

        return LiquidationResult(
            symbol=symbol,
            trigger_price=trigger_price,
            mark_price=mark,
            margin_lost=margin - recovered,
            recovered=recovered,
            closed_qty=-position.qty,
            event=event,
            position_side=position.position_side,
        )

    # ----------------------------------------------------------------------- margin

    def add_margin(
        self,
        ts_ms: int,
        symbol: str,
        amount: Decimal,
        position_side: PositionSide | None = None,
    ) -> Position:
        """Move `amount` from available balance into a position's isolated margin.

        Pushes the liquidation price further away without changing the position, which is
        the only defensive action available to a strategy that can see one coming and does
        not want to reduce.

        Per side in hedge mode, because the allocation being defended is per side. Adding
        margin to the long leg does nothing whatsoever for the short leg's liquidation
        price, and a helper that spread an amount across both would be defending the leg
        that was not in trouble with half the money meant for the one that was.
        """
        if amount <= 0:
            raise ValueError(f"{symbol}: margin to add must be positive, got {amount}")
        position = self.position(symbol, position_side)
        if position is None:
            raise LookupError(f"{symbol}: no open position to add margin to")
        if self.enforce_margin and amount > self.available_balance:
            raise InsufficientMargin(
                f"{symbol}: adding {amount} margin exceeds available balance "
                f"{self.available_balance}"
            )

        if self.enforce_margin and position.isolated_margin < 0:
            # Adding margin to an *overdrawn* allocation would be free, and that has to be
            # refused rather than accounted around. `Position.reserved_margin` floors at
            # zero -- correctly, since a position cannot reserve negative money -- so while
            # `isolated_margin` is negative, which funding does routinely by draining the
            # allocation past its base, `extra_margin` rises, the liquidation price moves
            # further away, and `available_balance` does not change at all. The same money
            # would be simultaneously spendable and pledged, and liquidation defence up to
            # `|isolated_margin|` would cost nothing.
            #
            # Refusing loses no real capability: a position whose allocation is negative
            # already satisfies spec 3.7's trigger, so the next `check_liquidations` takes
            # it regardless of what was added. The state is transient by construction.
            raise InsufficientMargin(
                f"{symbol}: this position's allocation is overdrawn by "
                f"{-position.isolated_margin} — funding has drained it past its base "
                "margin — so it is already liquidatable and margin added to it would not "
                "be charged for. Reduce or close the position instead."
            )

        self._touch(ts_ms)
        with localcontext(ACCOUNTING_CONTEXT):
            updated = replace(position, extra_margin=position.extra_margin + amount)
        self.positions[updated.key] = updated
        self._append(
            AccountEvent(
                ts_ms=ts_ms,
                kind=AccountEventKind.MARGIN_ADDED,
                symbol=symbol,
                position_side=updated.position_side,
                price=amount,
                wallet=self.wallet,
                position=updated.qty,
                entry_price=updated.entry_price,
                detail=f"isolated_margin={updated.isolated_margin}",
            )
        )
        return updated

    def remove_margin(
        self,
        ts_ms: int,
        symbol: str,
        amount: Decimal,
        position_side: PositionSide | None = None,
    ) -> Position:
        """Return added margin to the available balance.

        Only margin added via `add_margin` can be removed. The initial requirement is not
        withdrawable while the position is open -- it is what makes the leverage what it
        says it is, and releasing it would silently raise the effective leverage above the
        bracket's maximum.

        Refused when the withdrawal would leave the position liquidatable at the current
        mark -- the mirror of `add_margin`'s overdrawn-allocation refusal, and for the
        mirrored reason. `add_margin` refuses to accept money into a state the next
        liquidation check destroys; without this check the same strategy could pull added
        margin *out* into such a state: spec 3.7's trigger, re-solved with the smaller
        allocation, is already true at the recorded mark, so the "freed" amount lands in
        spendable balance exactly one event before the allocation it was freed from is
        destroyed. Binance rejects the transfer itself -- withdrawable isolated margin is
        capped at what keeps the position above maintenance -- so a run that allowed it
        would model a defence the exchange does not offer: see the liquidation coming,
        withdraw the margin, and let the clearance eat money the wallet already spent
        elsewhere. Skipped when the symbol has no bracket table or no recorded mark,
        which are exactly the states `check_liquidations` cannot price either -- there is
        no trigger to protect, and refusing on a guessed one would be inventing the
        margin model spec 3.6 forbids.
        """
        if amount <= 0:
            raise ValueError(f"{symbol}: margin to remove must be positive, got {amount}")
        position = self.position(symbol, position_side)
        if position is None:
            raise LookupError(f"{symbol}: no open position to remove margin from")
        if amount > position.extra_margin:
            raise InsufficientMargin(
                f"{symbol}: only {position.extra_margin} of added margin can be removed, "
                f"not {amount}; the initial requirement is locked while the position is open"
            )

        table = self.brackets.get(symbol)
        mark = self.marks.get(symbol)
        if self.enforce_margin and table is not None and mark is not None:
            with localcontext(ACCOUNTING_CONTEXT):
                shrunk = quantize_money(position.isolated_margin - amount)
            solution = liquidation_price(
                qty=position.qty,
                entry_price=position.entry_price,
                margin=shrunk,
                mark_price=mark,
                table=table,
            )
            triggered = solution.reachable and (
                mark <= solution.price if position.qty > 0 else mark >= solution.price
            )
            if triggered:
                raise InsufficientMargin(
                    f"{symbol}: removing {amount} margin would move the liquidation "
                    f"price to {solution.price} against a mark of {mark}, so the very "
                    "next liquidation check would destroy the position. Binance rejects "
                    "the transfer for the same reason: withdrawable isolated margin is "
                    "capped at what keeps the position above maintenance."
                )

        self._touch(ts_ms)
        with localcontext(ACCOUNTING_CONTEXT):
            updated = replace(position, extra_margin=position.extra_margin - amount)
        self.positions[updated.key] = updated
        self._append(
            AccountEvent(
                ts_ms=ts_ms,
                kind=AccountEventKind.MARGIN_REMOVED,
                symbol=symbol,
                position_side=updated.position_side,
                price=amount,
                wallet=self.wallet,
                position=updated.qty,
                entry_price=updated.entry_price,
                detail=f"isolated_margin={updated.isolated_margin}",
            )
        )
        return updated

    # ------------------------------------------------------------------ housekeeping

    def _require_margin(
        self,
        symbol: str,
        before: Position | None,
        after: Position | None,
        fee: Decimal,
        realized: Decimal = Decimal(0),
    ) -> None:
        """Refuse a fill the account cannot fund.

        The requirement is the increase in allocated margin, plus the fee, **less whatever
        the fill realises**:

        ```
        required = (after_margin - before_margin) + fee - realized
        ```

        `realized` was missing, and the omission hid in the one case where it matters. A
        Case A add realises nothing. A Case B reduce releases margin, so `required` is
        already negative and the early return fires before the term could matter. Only a
        **Case C flip** both realises a PnL and needs new margin — and a flip out of a
        *losing* position is exactly where the realised leg is a debit the wallet has to
        cover before the new side's margin can be found.

        Concretely: long 1 BTC at 50 000 on 10x, wallet 9 975, available 4 975. Mark drops
        to 46 000 — not yet liquidatable — and the strategy sells 3. That realises −4 000,
        pays 69 in fees, and opens a short 2 whose initial margin is 9 200. The account
        needs 9 200 + 69 + 4 000 − 5 000 = 8 269 against 4 975 available, so the fill must
        be refused. Without the `realized` term the requirement came out as 4 269, the fill
        was accepted, and the account finished with an **available balance of −3 294** and a
        residual position whose liquidation price sat 59% further from the mark than the
        wallet could actually fund. Nothing caught it downstream: spec 3.10 states no
        invariant about available balance, and I5 only fires if the *wallet* itself goes
        negative — which it does not, because the loss is real and affordable; it is the
        new position that is not.

        A profitable flip is the mirror and is equally correct to allow: the gain is in the
        wallet by the time the new margin is posted, so subtracting a positive `realized`
        lets a fill through that would otherwise have been refused for money it just made.

        The early return below is a *margin* judgement, not a solvency one, and the two
        came apart on a large reduce. The released allocation nets against the fee and
        the realised loss, so `required` goes negative and the check used to return
        before asking whether the wallet could actually pay -- but the loss and the fee
        are real cashflows against the wallet, while the released margin is not a
        cashflow at all: it was never carved out of the wallet, only reserved against
        it. An account whose available balance had already gone negative (funding drains
        the wallet in full while the paying position's `reserved_margin` floors at zero)
        could therefore book a reduce whose net cash leg overdrew the wallet, and the
        failure surfaced one line later as an I5 `InvariantViolation` aborting the run
        -- when the true situation is an order that should simply be *refused*: nothing
        in the ledger is broken. The wallet check is scoped to a non-negative wallet
        because a wallet already negative after a liquidation is I5's own documented
        exemption, and refusing every subsequent fee-bearing close would strand the
        account unable to reduce the very positions that put it there.
        """
        if not self.enforce_margin:
            return

        # The tier check runs first and unconditionally. It used to sit after the early
        # return below, so any fill that *released* margin skipped it entirely -- the check
        # was conditional on an unrelated funding calculation.
        #
        # No sequence reachable through the public API was found that exploits that: a
        # position's leverage is fixed at open, and every fill that pushes its notional into
        # a stricter tier also raises the margin requirement, so `required` comes out
        # positive and the check runs anyway. The reordering is therefore defensive rather
        # than a fix for a reproduced failure -- but a tier rule that silently depends on
        # the sign of a margin delta is one refactor away from being genuinely unreachable,
        # and it costs nothing to make it unconditional.
        if after is not None:
            table = self.brackets.get(symbol)
            if table is not None:
                allowed = table.max_leverage_for(after.entry_notional)
                if after.leverage > allowed:
                    raise InsufficientMargin(
                        f"{symbol}: leverage {after.leverage} exceeds the {allowed}x "
                        f"maximum for a notional of {after.entry_notional} "
                        f"(bracket {table.resolve(after.entry_notional).bracket})"
                    )

        with localcontext(ACCOUNTING_CONTEXT):
            if self.wallet >= 0 and self.wallet + realized - fee < 0:
                raise InsufficientMargin(
                    f"{symbol}: this fill realises {realized} and charges a fee of "
                    f"{fee}, a net cash leg the wallet ({self.wallet}) cannot fund; "
                    "accepting it would overdraw the account with no liquidation to "
                    "answer for it (I5)"
                )
            before_margin = before.reserved_margin if before else Decimal(0)
            after_margin = after.reserved_margin if after else Decimal(0)
            required = (after_margin - before_margin) + fee - realized
            if required <= 0:
                return
            if required > self.available_balance:
                raise InsufficientMargin(
                    f"{symbol}: fill needs {required} (margin "
                    f"{after_margin - before_margin} + fee {fee}"
                    + (f" - realized {realized}" if realized else "")
                    + f") but only {self.available_balance} is available"
                )

    def _check_tick_and_step(self, symbol: str, price: Decimal, qty: Decimal) -> None:
        """Invariant I6, when a filter snapshot is loaded for the symbol.

        Silently skipped when it is not. That is not a soft spot in the guarantee -- it is
        spec 3.2's `FILTERS_APPROXIMATE` condition, and the run that lacks a snapshot is
        supposed to be flagged in its metadata rather than have this layer invent limits.
        """
        if not self.strict:
            return
        filters = self.filters.get(symbol)
        if filters is None:
            return
        invariants.check_tick_and_step(
            price, qty, from_scaled(filters.tick_size), from_scaled(filters.step_size)
        )

    def _position_view(self) -> list[tuple[Decimal, Decimal, Decimal | None]]:
        """The `(qty, mark, entry)` triples invariant I2 is stated over.

        Uses `_valuation_mark` for the same reason `unrealized_pnl` does, and it has to be
        the *same* reason: if the invariant valued a markless position differently from the
        property it checks, I2 would fail on a perfectly correct account for the window
        between the first fill and the first mark.
        """
        return [
            (p.qty, self._valuation_mark(p.symbol, p), p.entry_price)
            for p in self.positions.values()
        ]

    def _check_wallet(self, *, liquidating: bool = False) -> None:
        """I1 and I5 after a wallet mutation.

        I5's exemption is scoped to the mutation, not to the run. Passing
        `self.liquidations > 0` -- which is what this did originally -- turns the first
        liquidation into a permanent licence: every later fee or funding charge could
        overdraw the account and I5 would wave it through for the rest of the run,
        precisely when the account is least able to afford an unnoticed error.

        What I5 is actually for is catching the *transition*: a mutation that takes a
        solvent wallet negative without a liquidation to account for it. A wallet that was
        already negative stays exempt, because a liquidation can legitimately leave it
        there and re-raising on every subsequent mutation would bury the original event.
        """
        if not self.strict:
            return
        invariants.check_wallet_conservation(
            self.wallet,
            self.opening_balance,
            self.total_realized,
            self.total_fees,
            self.total_funding,
        )
        invariants.check_wallet_non_negative(
            self.wallet, liquidating or self._wallet_was_negative
        )
        self._wallet_was_negative = self.wallet < 0

    def _check_after_fill(self, key: PositionKey, *, liquidating: bool = False) -> None:
        """I1, I3, I4, I5 and I2 after a fill or a liquidation, **for the side that moved**.

        I3 is the one that had to be restated for hedge mode, and the restatement is in the
        key rather than in the check: `check_position_sum` still asserts that a position
        equals the sum of the fills booked into it, and what changed is that "it" now means
        one side rather than one symbol. Per symbol the claim would be false on a correct
        hedge account -- a buy on the short side reduces it, so a symbol's fills sum to
        neither leg's quantity -- and an invariant that fires on correct code is worse than
        no invariant, because it gets switched off.
        """
        if not self.strict:
            return
        self._check_wallet(liquidating=liquidating)
        position = self.positions.get(key)
        qty = position.qty if position else Decimal(0)
        entry = position.entry_price if position else None
        invariants.check_position_sum(qty, self._signed_fills.get(key, Decimal(0)))
        invariants.check_entry_price_presence(qty, entry)
        if self.marks:
            invariants.check_equity(self.equity, self.wallet, self._position_view())

    def _touch(self, ts_ms: int) -> None:
        """Invariant I8, applied to every mutation rather than only to logged ones.

        Mark updates are usually not logged (`log_marks`), and they are the highest-volume
        event in the system -- so checking ordering only at append time would leave the
        ordering of the very events most likely to arrive out of order unchecked.
        """
        if self.strict:
            invariants.check_monotonic_timestamps(self._last_ts_ms, ts_ms)
        self._last_ts_ms = max(self._last_ts_ms, ts_ms)

    def _append(self, event: AccountEvent) -> None:
        self.events.append(event)

    # ---------------------------------------------------------------------- closing

    def reconcile(self) -> None:
        """Re-derive the whole ledger from the event log and check it against live state.

        **I9 alone is not enough, and the reason is worth stating.** Spec 3.10 phrases I9
        over reconstructed round-trips -- "sum of per-trade PnL + open-position uPnL ==
        total PnL" -- and that reconstruction is spec 8.3, which belongs to the analytics
        layer. Substituting the accumulator form, as an earlier revision did, produces a
        check that *cannot fail*: `equity` is defined as `wallet + unrealized_pnl`, so
        feeding it and the same `unrealized_pnl` into `check_pnl_decomposition` cancels the
        term on both sides and reduces the whole thing to I1, which has already been
        asserted on every mutation.

        So the end-of-run check is a **replay** instead. The event log is walked from the
        opening balance, re-accumulating the wallet, the totals and the position from the
        recorded per-event amounts, and the result is compared with the account's own
        running state. That is genuinely independent: it shares no accumulator with the
        live path, so a fill mis-booked identically into both the wallet and the totals --
        the one failure I1 is blind to -- shows up here as a divergence between the log and
        the state.

        I9's algebraic form is then applied on top, over the replayed figures.
        """
        wallet = self.opening_balance
        realized = fees = funding = Decimal(0)
        positions: dict[PositionKey, Decimal] = {}

        with localcontext(ACCOUNTING_CONTEXT):
            for event in self.events:
                if event.kind is AccountEventKind.MARK:
                    continue
                realized += event.realized
                fees += event.fee
                funding += event.funding
                wallet += event.realized - event.fee + event.funding
                if event.kind in (AccountEventKind.FILL, AccountEventKind.LIQUIDATION):
                    # Keyed per side, matching `_signed_fills`. A symbol-keyed replay would
                    # add a hedge's long and short fills together and compare the total
                    # against one leg -- failing on a correct account, and, in the case
                    # where two mis-booked fills happened to cancel, passing on a broken one.
                    key = (event.symbol, event.position_side)
                    positions[key] = positions.get(key, Decimal(0)) + event.qty

        divergences = []
        if wallet != self.wallet:
            divergences.append(f"wallet: log {wallet} != state {self.wallet}")
        if realized != self.total_realized:
            divergences.append(f"realized: log {realized} != state {self.total_realized}")
        if fees != self.total_fees:
            divergences.append(f"fees: log {fees} != state {self.total_fees}")
        if funding != self.total_funding:
            divergences.append(f"funding: log {funding} != state {self.total_funding}")
        # The union, not just the symbols the log mentions. Iterating over the replayed
        # dict alone misses the case that matters most -- a position held in state that
        # *no* event accounts for -- because a symbol absent from the log is also absent
        # from the loop.
        for key in sorted(
            set(positions) | set(self.positions), key=lambda k: (k[0], _SIDE_ORDER[k[1]])
        ):
            replayed = positions.get(key, Decimal(0))
            held = self.positions[key].qty if key in self.positions else Decimal(0)
            if replayed != held:
                symbol, side = key
                label = symbol if side is PositionSide.BOTH else f"{symbol} {side.value}"
                divergences.append(f"{label} position: log {replayed} != state {held}")

        if divergences:
            raise invariants.InvariantViolation(
                "I9", "event log does not reproduce account state -- " + "; ".join(divergences)
            )

        invariants.check_pnl_decomposition(
            self.equity,
            self.opening_balance,
            realized,
            fees,
            funding,
            self.unrealized_pnl,
        )

    def attribution(self) -> dict[str, Decimal]:
        """The spec 8.4 PnL decomposition, less slippage (which the engine measures).

        `price_pnl` is realised plus unrealised; funding and fees stand alone. Keeping
        funding in its own column is the fastest way to spot a strategy whose entire edge
        is funding capture -- worth knowing, because that is a different and more fragile
        edge than a price edge.

        **`liquidation_cost` is broken out, and the reason is worth reading.** A
        liquidation's realised figure carries two economically different things -- the
        price move the position suffered to the liquidating mark, and the clearance
        penalty that consumed the margin remaining after it -- and folding the penalty
        into `price_pnl` misattributes it to the strategy's price edge.
        `total_liquidation_cost` therefore memos the penalty alone, `-(remaining margin
        - recovered)`, and the figure is **never positive**: an earlier revision derived
        it as `loss - price_leg` with the loss capped at the allocation and the price
        leg uncapped, so any mark that gapped through the trigger flipped the column
        positive -- a 10x long probed at a bar low of 40 000 reported +5 000 of
        "liquidation cost" beside a -10 000 price PnL on an account that lost 5 000, and
        I9 still closed because the columns are memos over one total. The price column
        now carries the price leg capped at the allocation (the isolated-margin promise:
        losses past bankruptcy land on the insurance fund, not the account), and the
        penalty column carries the confiscated remainder, which on such a gap is
        honestly zero -- the price move left nothing for the clearance to consume.

        The most visible case: a position can be liquidated while *in profit*. Funding
        drains the allocation below the maintenance requirement, the mark then moves in the
        position's favour, and spec 3.7's trigger still fires. With the penalty inside
        `price_pnl`, that run reports a price leg of zero for a position whose price leg was
        positive, and the funding column carries the whole story. Separating them keeps each
        column meaning what its name says; `net_pnl` is unchanged either way, and
        `price_pnl + funding_pnl - fees + liquidation_cost == net_pnl` still holds.
        """
        with localcontext(ACCOUNTING_CONTEXT):
            unrealized = self.unrealized_pnl
            return {
                "price_pnl": self.total_realized + unrealized - self.total_liquidation_cost,
                "realized_pnl": self.total_realized,
                "unrealized_pnl": unrealized,
                "funding_pnl": self.total_funding,
                "fees": self.total_fees,
                "liquidation_cost": self.total_liquidation_cost,
                "net_pnl": self.equity - self.opening_balance,
            }
