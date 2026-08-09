"""The risk layer and kill switch (spec 7).

Spec 7's first sentence is the design: *"Risk checks run in the **shared core**, so a limit
that stops a backtest stops live trading identically."* So this module knows nothing about
backtests, event queues or exchanges. It is given numbers and returns a verdict, and the
three execution modes each call it from their own submit path. A risk layer implemented in
the backtester and re-implemented in the live engine is the same duplication spec 14-I8 was
about, except that here the two copies disagreeing means the paper run was safe and the
live one was not.

**Two kinds of limit, and they are not interchangeable.** Spec 7's table has an *action*
column with two values, and collapsing them would be wrong in both directions:

- `REJECT` refuses one order. The run continues, the strategy keeps trading, and the
  refusal is a fact about that order. A strategy that asks for too much size gets less.
- `HALT` stops the run and closes out. The account itself is in a state the operator said
  was unacceptable, and no subsequent order can improve it.

**Rejections are never silent.** Every refusal returns a `RiskBreach` carrying the observed
value, the allowed value and the limit's name, and the caller writes it to the event log.
A risk layer that quietly drops orders produces a strategy whose backtest shows it doing
something it never did -- the equity curve of a strategy that was mostly blocked looks like
the equity curve of a strategy that mostly declined to trade, and only the log tells them
apart.

## The order-of-operations rule

A pre-submission check has exactly one hard part, and it is not the arithmetic. It is
deciding *which* state to check against.

Checking `max_position_notional` against the position as it stands **now** is the classic
form of the bug: the order under consideration is precisely the thing that would breach the
limit, so a check that ignores it passes every order right up to the one that ruins the
account, and then passes that one too. The check has to run against the position the order
would *produce*.

That is still not sufficient. A strategy that submits ten orders inside one bar has ten
orders in flight before any of them fills, and each one, measured against the position plus
itself, is comfortably inside the limit. `projected_exposure` therefore accounts for every
order already working or pending, and it does so as a **bound over fill orderings** rather
than as a net:

    upper = position + every buy that could still fill
    lower = position - every sell that could still fill
    exposure = max(|upper|, |lower|)

The net would understate it. A long of 10 with a working buy of 5 and a new sell of 20 nets
to |10 + 5 - 20| = 5, while the path where the buy fills first reaches 15 -- and 15 is the
number the account has to survive. Reduce-only orders contribute nothing to either side:
they can only shrink a position, so counting them would let a strategy add exposure on the
strength of a stop-loss that has not fired and may never fire.

## The daily boundary

`max_daily_loss` needs a definition of "day", and the two obvious choices give different
answers on the same run.

The day is the **UTC calendar day**, taken from the event timestamp: `ts_ms // 86_400_000`.
A timestamp of exactly `00:00:00.000` belongs to the day it opens, not the one it closes,
which is why this is a floor division on the timestamp itself and not on `ts_ms - 1`.

**No equity sample ever lands on the boundary, and that is the part that matters.** Samples
arrive at bar closes -- `23:59:59.999`, then `00:00:59.999` -- so the first observation of a
new day is up to a whole bar late. Baselining the day on it makes everything inside that bar
belong to neither day, which is how a 4 000 loss went unnoticed under a 2 000 limit. The
baseline is therefore the **last observation of the previous day, carried forward**, which
is spec 3.4's LOCF rule applied to the same question every other series here answers with it.

The loss is measured from that baseline, and the threshold is a fixed amount derived from the
**run's** starting equity. Spec 7's default reads "2% of starting equity", which is a
description of the default *value*, not of a quantity that follows the account around. The
alternative -- 2% of whatever equity the day opened with -- tightens the leash every time the
strategy loses and loosens it every time it wins, which is a behaviour somebody should have
to ask for rather than inherit from an ambiguous table cell.

## What "exactly at the limit" means

The two kinds of limit answer it differently, on purpose:

- A **size** limit is a ceiling on a *value*, so a value equal to it is still inside:
  `notional > max_position_notional`, `leverage > max_leverage`, and -- the same shape --
  `|ours - theirs| > tolerance` for a reconciliation and
  `down_ms > max_disconnect_seconds * 1000` for a disconnect, whose spec wording is
  "exceeding" in as many words.
- A **count** or a **loss** limit is reached rather than exceeded, so equality breaches:
  `open_orders >= max_open_orders`, `loss >= max_daily_loss`, `equity <= min_equity`,
  `drawdown >= max_drawdown_pct`, `streak >= max_consecutive_losses`.

Stated here because it is not self-evident from any single call site, and because it has
already drifted once -- `min_equity_pct`'s own docstring described a strictness the code did
not have.

## The two live-only auto-triggers

Spec 7 lists four auto-triggers. Two of them -- an invariant failure and a rejection storm
-- are things a backtest can produce, so they were built with the rest of the layer. The
other two exist only where there is a socket and a counterparty:

- **A disconnect while a position is open.** Not a disconnect: spec 7 qualifies it, and the
  qualifier is the trigger rather than a detail of it. A socket down over a flat account
  costs data; a socket down over an open position means the platform has stopped seeing the
  mark that liquidation is decided against (spec 3.7) and stopped receiving the fills that
  say what it holds.
- **A reconciliation mismatch** (spec 6.7.3). Unconditional, like the invariant trigger, and
  for the same reason: a disagreement with the exchange means PerpLab's idea of the account
  is wrong, and every other limit here is evaluated against that idea.

Both are recorded with an explicit `trigger` -- `DISCONNECT`, `RECONCILIATION` -- rather
than with the breached limit's name, because spec 7.5's "timestamp and trigger source" is
asking what kind of thing went wrong, and "max_disconnect_seconds" answers a different
question from "the exchange and we disagree about the position".

## Bounded state, because a live session does not end

A backtest stops when the data does. A paper session runs for the weekend, and every
accumulator in here is then a leak: `breaches` grows one entry per refusal, so a strategy
fighting a filter at 30 rejections a minute for 48 hours produces 86 400 records, and
`rejections` grows one *key* per distinct reason string -- and an exchange message like
`"insufficient margin: need 1234.56789"` is a fresh string every time. `MAX_BREACHES` and
`normalise_rejection_reason` below are the two ceilings; both report what they discarded
rather than discarding it quietly.
"""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from perplab.core.money import Money, accounting, money_to_str, parse_money, quantize_money
from perplab.core.types import PositionSide

__all__ = [
    "MS_PER_DAY",
    "MAX_BREACHES",
    "MAX_REJECTION_REASONS",
    "OTHER_REJECTION_REASON",
    "RiskAction",
    "RiskLimits",
    "RiskBreach",
    "RiskEngine",
    "KillSwitch",
    "KillSwitchArmed",
    "WorkingExposure",
    "normalise_rejection_reason",
    "projected_exposure",
    "side_projected_exposure",
    "gross_projected_exposure",
]

MS_PER_DAY = 86_400_000
MS_PER_MINUTE = 60_000
MS_PER_SECOND = 1_000

MAX_BREACHES = 1_000
"""How many breach records one engine keeps, first-come.

The cap keeps the **first** breaches and drops the rest, which is the opposite of what a
ring buffer would do and is the point: the first refusal is the one that explains the run,
and the ten-thousandth is a consequence of nobody having stopped it. `summary()` reports
`breaches_dropped` so a truncated list is never read as a complete one.

Nothing that stopped the run is lost to the cap. There is at most one halt per engine and
it is held separately, on `halt_breach` and in `summary()["halt_reason"]`.
"""

MAX_REJECTION_REASONS = 64
"""Distinct rejection reasons tallied before the rest are pooled.

At most this many keys plus `OTHER_REJECTION_REASON`. Sixty-four is well above the number
of *kinds* of refusal that exist -- Binance's filter list is a dozen long and the ledger
adds a handful -- so reaching it means the normalisation below has met text it could not
fold, and pooling the tail is how that stays visible instead of unbounded.
"""

OTHER_REJECTION_REASON = "<other>"
"""Where reasons past `MAX_REJECTION_REASONS` are pooled.

Lower-case, and `normalise_rejection_reason` upper-cases everything it returns, so no real
reason can ever land in this bucket by collision. A pool a genuine reason could fall into
would report a count that is the sum of two different things.
"""

_REASON_NUMBER = re.compile(r"\d[\d,._]*")
_REASON_SPACE = re.compile(r"\s+")
_REASON_MAX_CHARS = 60


def normalise_rejection_reason(reason: str) -> str:
    """Fold an exchange rejection message into a bounded tally key.

    `RiskEngine.rejections` counts refusals per reason, and a counter keyed by text the
    exchange wrote is a counter with no ceiling on its size. Filter names are bounded --
    `PRICE_FILTER`, `MARKET_LOT_SIZE`, twenty of them at most -- but free text is not:
    `"insufficient margin: need 1234.56789"` is a new key on every order, so a session that
    spends a weekend out of margin ends with one entry per rejection and a table nobody can
    read, in a process that is not going to restart and free it.

    The rule, applied in this order:

    1. Keep only what precedes the first colon. The convention in both Binance's errors and
       this codebase's own is `kind: this instance`, and the kind is the part worth counting.
    2. Replace each run of digits (with any `,._` inside it) by `#`, which catches the ids,
       quantities and prices that appear without a colon in front of them.
    3. Collapse whitespace, upper-case, and truncate to sixty characters.

    The **raw** message is still what the breach's `detail` carries. This is the key of a
    counter, not a substitute for the text an operator has to read.
    """
    head = reason.split(":", 1)[0]
    head = _REASON_NUMBER.sub("#", head)
    head = _REASON_SPACE.sub(" ", head).strip().upper()
    return head[:_REASON_MAX_CHARS] if head else "UNKNOWN"


def _fmt(value: Money) -> str:
    """Render a limit or an observation at the storage scale.

    Quantised first, because these strings go into the event log, the run record and the
    results page, and the raw values come out of `accounting()` at twenty places. Two runs
    reporting `200` and `200.00000000` for the same limit would be two runs a reader had to
    squint at to see were identical.
    """
    return money_to_str(quantize_money(value))


class RiskAction(Enum):
    """What a breach does. Spec 7's action column, and nothing else."""

    REJECT = "REJECT"
    """Refuse this order. The run continues."""

    HALT = "HALT"
    """Stop the run and close out. The account state is the problem, not the order."""


@dataclass(frozen=True, slots=True)
class RiskBreach:
    """One limit, exceeded, with the numbers that decided it.

    `observed` and `allowed` are strings rather than `Decimal` because a breach travels
    straight into the event log, the run record and the results page, and every one of
    those wants the exact decimal text rather than a float that has already lost digits.
    """

    limit: str
    action: RiskAction
    ts_ms: int
    observed: str
    allowed: str
    detail: str
    symbol: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "limit": self.limit,
            "action": self.action.value,
            "ts_ms": self.ts_ms,
            "observed": self.observed,
            "allowed": self.allowed,
            "detail": self.detail,
            "symbol": self.symbol,
        }

    @property
    def message(self) -> str:
        return f"{self.limit}: {self.detail} (observed {self.observed}, limit {self.allowed})"


def _opt_money(value: Any) -> Money | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return parse_money(str(value))


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"a count limit cannot be negative, got {parsed}")
    return parsed


@dataclass(frozen=True, slots=True)
class RiskLimits:
    """Spec 7's per-run limits.

    Every field is optional and `None` means *unlimited*, which is a value somebody chose
    rather than a value that leaked through. `unbounded_exposure` reports the one
    combination that leaves position size with no ceiling at all, so the caller can flag
    the run instead of discovering it in the equity curve.

    Percentages are fractions -- `0.02`, not `2` -- for the same reason `callback_rate` is:
    a field that accepts both reads a 2 as 200% and produces a limit that never fires, and
    a limit that never fires is indistinguishable from a strategy that stayed within it.
    """

    max_position_notional: Money | None = None
    """Absolute quote-currency ceiling on |projected exposure| x price, per symbol."""

    max_leverage: Money | None = Decimal(5)
    """Projected notional across all symbols, divided by equity."""

    max_daily_loss_pct: Money | None = Decimal("0.02")
    """Fraction of the run's *starting* equity. Measured from each UTC day's open."""

    max_drawdown_pct: Money | None = Decimal("0.15")
    """Fraction below peak mark-to-market equity (spec 7: *not* closed-trade PnL)."""

    max_open_orders: int | None = 10
    max_orders_per_minute: int | None = 30
    max_consecutive_losses: int | None = None
    halt_on_liquidation: bool = True

    min_equity_pct: Money | None = Decimal("0.50")
    """Fraction of the run's starting equity **at or below** which the run halts.

    Inclusive, like every other HALT limit here and unlike the two REJECT size limits.
    The rule is worth stating because it has already drifted once: a *count* or a *loss* of
    exactly N means N has been reached, so it breaches; a *size* of exactly N is still
    within a ceiling of N, so it passes."""

    max_consecutive_rejections: int | None = 5
    """Spec 7's auto-trigger: *"repeated order rejections from the exchange (default 5
    consecutive)"*. Counts exchange-side and ledger-side rejections -- a filter refusal or
    an insufficient-margin refusal -- and not risk-layer rejections, which are this
    module's own verdicts and would make the trigger fire on itself."""

    max_disconnect_seconds: int | None = None
    """Spec 7's auto-trigger: a WS disconnect this long *while a position is open*.

    **Spec 7 says the default is 30 and this field defaults to `None`, deliberately.** The
    trigger needs a socket, and a backtest replays a file: there is nothing that can
    disconnect, so a 30 here would appear in every backtest's stored limit set as a limit
    the run was subject to and never reached. That is the same false history
    `from_json({})` exists to refuse. The session builder that has a socket sets it to 30
    explicitly, where somebody can see the number.
    """

    @property
    def unbounded_exposure(self) -> bool:
        """Neither a notional ceiling nor a leverage ceiling: nothing bounds position size."""
        return self.max_position_notional is None and self.max_leverage is None

    @property
    def any_limit(self) -> bool:
        """Whether any limit at all is in force.

        Distinct from `unbounded_exposure`, and conflating them was a defect: a run whose
        only limit was `max_orders_per_minute` is bounded in one way and unbounded in
        another, and reporting it as having had no risk layer put "nothing was refused
        because nothing could be" on a results page directly above a table of twenty
        refusals.
        """
        return self != RiskLimits.unlimited()

    @classmethod
    def unlimited(cls) -> RiskLimits:
        """Every limit off.

        Exists so that a caller who wants no risk layer says so explicitly. The tests use
        it, and so does any run whose whole purpose is to observe what an unconstrained
        strategy does.
        """
        return cls(
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            max_consecutive_losses=None,
            halt_on_liquidation=False,
            min_equity_pct=None,
            max_consecutive_rejections=None,
            max_disconnect_seconds=None,
        )

    def __post_init__(self) -> None:
        for name in ("max_position_notional",):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when set, got {value}")
        if self.max_leverage is not None and self.max_leverage <= 0:
            raise ValueError(f"max_leverage must be positive when set, got {self.max_leverage}")
        for name in ("max_daily_loss_pct", "max_drawdown_pct", "min_equity_pct"):
            value = getattr(self, name)
            if value is None:
                continue
            if not (0 < value <= 1):
                raise ValueError(
                    f"{name} is a fraction and must be in (0, 1]; got {value}. A limit "
                    "written as a percent -- 2 rather than 0.02 -- would never fire."
                )
        for name in (
            "max_open_orders",
            "max_orders_per_minute",
            "max_consecutive_losses",
            "max_consecutive_rejections",
            # A zero here would halt the run on the first millisecond of the first
            # reconnect, which is a limit nobody would write down on purpose.
            "max_disconnect_seconds",
        ):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when set, got {value}")

    def to_json(self) -> dict[str, Any]:
        def money(value: Money | None) -> str | None:
            return None if value is None else _fmt(value)

        return {
            "max_position_notional": money(self.max_position_notional),
            "max_leverage": money(self.max_leverage),
            "max_daily_loss_pct": money(self.max_daily_loss_pct),
            "max_drawdown_pct": money(self.max_drawdown_pct),
            "max_open_orders": self.max_open_orders,
            "max_orders_per_minute": self.max_orders_per_minute,
            "max_consecutive_losses": self.max_consecutive_losses,
            "halt_on_liquidation": self.halt_on_liquidation,
            "min_equity_pct": money(self.min_equity_pct),
            "max_consecutive_rejections": self.max_consecutive_rejections,
            "max_disconnect_seconds": self.max_disconnect_seconds,
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any] | None) -> RiskLimits:
        """Read a stored limit set.

        An empty mapping is **not** the default limits -- it is what Phase 4 and Phase 5
        runs stored, and those ran with no risk layer at all. Reading them back as the
        defaults would rewrite history: a run that never rejected an order would be
        reported as one that ran under a 5x leverage cap and happened never to hit it.

        The same rule governs a limit added later. A stored set from before Phase 7 has no
        `max_disconnect_seconds` key, and it reads as `None`, because that run was not
        subject to a disconnect trigger and no amount of re-reading makes it so.
        """
        if not obj:
            return cls.unlimited()
        known = set(cls.__dataclass_fields__)
        unknown = sorted(set(obj) - known)
        if unknown:
            raise ValueError(
                f"unknown risk limit(s) {unknown}. A misspelled limit silently does "
                "nothing, which is the failure this refuses to have."
            )
        return cls(
            max_position_notional=_opt_money(obj.get("max_position_notional")),
            max_leverage=_opt_money(obj.get("max_leverage")),
            max_daily_loss_pct=_opt_money(obj.get("max_daily_loss_pct")),
            max_drawdown_pct=_opt_money(obj.get("max_drawdown_pct")),
            max_open_orders=_opt_int(obj.get("max_open_orders")),
            max_orders_per_minute=_opt_int(obj.get("max_orders_per_minute")),
            max_consecutive_losses=_opt_int(obj.get("max_consecutive_losses")),
            halt_on_liquidation=bool(obj.get("halt_on_liquidation", True)),
            min_equity_pct=_opt_money(obj.get("min_equity_pct")),
            max_consecutive_rejections=_opt_int(obj.get("max_consecutive_rejections")),
            max_disconnect_seconds=_opt_int(obj.get("max_disconnect_seconds")),
        )


@dataclass(frozen=True, slots=True)
class WorkingExposure:
    """Quantity that could still reach the book, split by direction.

    Split rather than netted because the bound that matters is over fill *orderings*, and
    a net loses exactly the information needed to compute it. See the module docstring.
    """

    buy_qty: Money
    sell_qty: Money

    @classmethod
    def zero(cls) -> WorkingExposure:
        return cls(Decimal(0), Decimal(0))

    def plus(self, side: str, qty: Money) -> WorkingExposure:
        if side == "BUY":
            return WorkingExposure(self.buy_qty + qty, self.sell_qty)
        return WorkingExposure(self.buy_qty, self.sell_qty + qty)


def projected_exposure(position_qty: Money, working: WorkingExposure) -> Money:
    """The largest |position| reachable from here, over every order of fills.

    Not `|position + buys - sells|`. That is the exposure at the *end* of a sequence, and
    the account has to survive every point along it.

    One-way form: the position can cross zero, so both the buy-heavy and the sell-heavy
    path have to be evaluated and the worse taken. See `side_projected_exposure` for the
    hedge-mode form, where a side cannot cross zero and the bound collapses to one term.
    """
    with accounting():
        upper = position_qty + working.buy_qty
        lower = position_qty - working.sell_qty
        return max(abs(upper), abs(lower))


def side_projected_exposure(
    position_side: PositionSide, position_qty: Money, working: WorkingExposure
) -> Money:
    """The largest |quantity| **one position side** can reach, over every ordering of fills.

    `BOTH` delegates to `projected_exposure` and is the one-way bound unchanged.

    `LONG` and `SHORT` are one term rather than two, and the missing term is a fact about
    hedge mode rather than a simplification. A hedge side cannot cross zero -- a sell beyond
    the long side is refused by the ledger (`account.HedgeFlipRefused`) and by the exchange
    -- so the shrinking direction can only ever *reduce* this side's exposure and never
    contributes to the maximum. The long side's worst case is therefore "every buy fills and
    no sell does", full stop.

    Taking `max(|q + buys|, |q - sells|)` here instead would be wrong in the direction that
    matters: a long of 1 with a working sell of 5 would report an exposure of 4 -- a *short*
    of 4 that hedge mode makes unreachable -- and a `max_position_notional` sized for the
    real risk would refuse an order that was never dangerous. Erring toward refusal sounds
    safe, but a size limit that refuses exits a strategy needs is how an account ends a bad
    day still holding the position.
    """
    if position_side is PositionSide.BOTH:
        return projected_exposure(position_qty, working)
    with accounting():
        growth = working.buy_qty if position_side is PositionSide.LONG else working.sell_qty
        return abs(position_qty) + growth


def gross_projected_exposure(
    sides: Iterable[tuple[PositionSide, Money, WorkingExposure]],
) -> Money:
    """**Sum of both sides**, each bounded over its own fill orderings -- the hedge rule.

    The agreed reading of `max_position_notional` in hedge mode, and the conservative one.
    Both legs of a hedge post their own isolated margin against their own entry price, and
    either can be liquidated while the other survives; the quantity a bad tick can destroy
    is therefore `|Q_long| + |Q_short|`, not the difference between them.

    Netting would be the alternative and it is the trap. A long 10 against a short 10 nets
    to zero, so a netted limit imposes no ceiling at all on a market-neutral book -- and a
    market-neutral book is exactly the thing that stops being neutral when one leg is
    liquidated, at which point the survivor is a naked 10 the limit never saw. Spec 7's
    ceiling is on what the account has to survive, and both legs are that.

    Degenerates correctly: one `BOTH` entry reproduces `projected_exposure` exactly, so the
    one-way path is not a special case of this but literally the same arithmetic.
    """
    with accounting():
        return sum(
            (side_projected_exposure(side, qty, working) for side, qty, working in sides),
            Decimal(0),
        )


class KillSwitchArmed(RuntimeError):
    """A live session was started while the kill switch was tripped (spec 7.6)."""


@dataclass
class KillSwitch:
    """Spec 7's kill switch: the mechanism, shared by every mode.

    **What this class is, and what it deliberately is not.** Spec 7 lists six things the
    kill switch does. Three of them -- stopping strategy processes, cancelling orders at
    the exchange, wiping the key session -- are actions on a live session, and there is no
    live session until Phase 7. The other three -- recording the trip with its timestamp
    and trigger, choosing between cancel-only and flatten, and refusing to start again
    until somebody un-arms it -- are state, and state is what a backtest can exercise now.

    So this holds the state and names the actions as callbacks the live engine will
    supply. The alternative was to write the live half now against an exchange client that
    does not exist, which would produce code that has never run and a phase sign-off that
    means less than it says.

    **Cancel-only is the default**, per spec 7.3: force-closing everything at market during
    a flash crash can be worse than the exposure. `flatten` is opt-in and the caller is
    expected to say which behaviour is armed wherever the button is rendered.
    """

    flatten: bool = False
    tripped_at_ms: int | None = None
    trigger: str | None = None
    detail: str = ""

    @property
    def tripped(self) -> bool:
        return self.tripped_at_ms is not None

    def trip(self, ts_ms: int, trigger: str, detail: str = "") -> bool:
        """Record the trip. Returns `False` if it was already tripped.

        First trigger wins. A cascade -- an invariant failure that causes a liquidation
        that causes a rejection storm -- should report the thing that started it, not the
        last symptom to arrive.
        """
        if self.tripped:
            return False
        self.tripped_at_ms = ts_ms
        self.trigger = trigger
        self.detail = detail
        return True

    def require_clear(self) -> None:
        """Spec 7.6: an explicit un-arm is required before a live session may start."""
        if self.tripped:
            raise KillSwitchArmed(
                f"the kill switch tripped at {self.tripped_at_ms} ({self.trigger}: "
                f"{self.detail}). Un-arm it explicitly before starting a session."
            )

    def unarm(self) -> None:
        self.tripped_at_ms = None
        self.trigger = None
        self.detail = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "flatten": self.flatten,
            "tripped_at_ms": self.tripped_at_ms,
            "trigger": self.trigger,
            "detail": self.detail,
        }


@dataclass
class RiskEngine:
    """Spec 7's limits, evaluated against numbers the caller supplies.

    Holds the state a limit needs across events -- the peak, the day's opening equity, the
    submission timestamps in the last minute, the losing streak -- and nothing else. It
    does not reach into an `Account`, because the live engine's account is a different
    object reconciled against a different source, and a risk layer that reads one of them
    directly is a risk layer that exists in one mode.
    """

    limits: RiskLimits
    starting_equity: Money
    kill_switch: KillSwitch = field(default_factory=KillSwitch)

    peak_equity: Money = field(init=False)
    day_key: int | None = field(default=None, init=False)
    day_open_equity: Money = field(init=False)
    halted: bool = field(default=False, init=False)
    halt_breach: RiskBreach | None = field(default=None, init=False)
    breaches: list[RiskBreach] = field(default_factory=list, init=False)
    breaches_dropped: int = field(default=0, init=False)
    """Breaches past `MAX_BREACHES`, counted rather than kept. See `_record`."""
    rejections: dict[str, int] = field(default_factory=dict, init=False)
    """Refusals per *normalised* reason -- see `normalise_rejection_reason`."""

    _submissions: deque[int] = field(default_factory=deque, init=False, repr=False)
    _last_equity: Money | None = field(default=None, init=False, repr=False)
    """The latest equity observation *by timestamp*, which becomes the next day's baseline."""
    _last_equity_ms: int | None = field(default=None, init=False, repr=False)
    """When `_last_equity` was stamped, so a replayed sample cannot overwrite it."""
    _consecutive_losses: int = field(default=0, init=False, repr=False)
    _consecutive_rejections: int = field(default=0, init=False, repr=False)
    _rejected: int = field(default=0, init=False, repr=False)
    """Refusals this engine issued, counted rather than derived from `breaches`, which is
    capped. See `summary`."""

    def __post_init__(self) -> None:
        if self.starting_equity <= 0:
            raise ValueError(
                f"starting equity must be positive, got {self.starting_equity}. Every "
                "percentage limit is a fraction of it, and a fraction of zero is a limit "
                "that fires on the first event of the run."
            )
        self.peak_equity = self.starting_equity
        self.day_open_equity = self.starting_equity

    # ------------------------------------------------------------------ derived limits

    @property
    def max_daily_loss(self) -> Money | None:
        if self.limits.max_daily_loss_pct is None:
            return None
        with accounting():
            return self.starting_equity * self.limits.max_daily_loss_pct

    @property
    def min_equity(self) -> Money | None:
        if self.limits.min_equity_pct is None:
            return None
        with accounting():
            return self.starting_equity * self.limits.min_equity_pct

    # -------------------------------------------------------------------- pre-submission

    def check_order(
        self,
        *,
        ts_ms: int,
        symbol: str,
        side: str,
        qty: Money,
        price: Money,
        reduce_only: bool,
        position_qty: Money,
        working: WorkingExposure,
        equity: Money,
        open_orders: int,
        other_notional: Money = Decimal(0),
        position_side: PositionSide = PositionSide.BOTH,
        other_side_exposure: Money = Decimal(0),
    ) -> RiskBreach | None:
        """Spec 7's pre-submission checks. `None` means the order may proceed.

        `working` is the exposure already in flight on **this symbol and this position
        side**, excluding this order and excluding reduce-only orders. `other_notional` is
        the projected notional of every *other* symbol, which only `max_leverage` needs --
        leverage is an account property, and checking it per symbol would let two symbols at
        4x each report a compliant 4x account running at 8x.

        `position_side` and `other_side_exposure` are the hedge-mode pair. The first decides
        how this side's own bound is computed (see `side_projected_exposure`); the second is
        the quantity already reachable on the **other** leg of the same symbol, bounded over
        its own working orders, and it is *added* rather than netted. That is the agreed
        `max_position_notional` rule -- see `gross_projected_exposure` for why netting a
        hedge is the dangerous reading. Both default to the one-way values, so a one-way
        caller passes neither and gets the arithmetic it always had.

        Order of evaluation is the cheapest-to-most-specific, and it is stable: a run that
        breaches two limits at once reports the same one every time, so two runs of the
        same strategy do not disagree about why an order was refused.
        """
        if self.halted:
            return self._record(
                RiskBreach(
                    limit="halted",
                    action=RiskAction.REJECT,
                    ts_ms=ts_ms,
                    observed="halted",
                    allowed="running",
                    detail="the run has halted; no further orders are accepted",
                    symbol=symbol,
                )
            )

        # **The reduce-only exemption comes first, before every other rejection.**
        #
        # A reduce-only order cannot increase exposure. It can only shrink a position, so no
        # limit on *size* can be the reason to refuse it -- and neither can a limit on
        # *count*. That was the bug: `max_open_orders` and `max_orders_per_minute` sat above
        # this guard, so a strategy with four resting quotes under a ceiling of four had its
        # own `ctx.close()` refused and finished the run still holding the position. The
        # platform's own `AutoFlatten` exit hit the same wall. An account that has breached
        # a limit is precisely the account whose exits have to work.
        #
        # The submission is still **counted** against the rate window. It is a real API
        # call, it consumes real exchange budget, and the runaway-loop guard would be blind
        # to a strategy looping on exits if it were not.
        if reduce_only:
            self._note_submission(ts_ms)
            return None

        breach = self._check_rate(ts_ms=ts_ms, symbol=symbol)
        if breach is not None:
            return breach

        if self.limits.max_open_orders is not None and open_orders >= self.limits.max_open_orders:
            return self._record(
                RiskBreach(
                    limit="max_open_orders",
                    action=RiskAction.REJECT,
                    ts_ms=ts_ms,
                    # The count this order *would produce*, matching every other limit here.
                    # Reporting the pre-order count printed "observed 3, limit 3", which
                    # reads as a value that does not exceed the limit it was refused for.
                    observed=str(open_orders + 1),
                    allowed=str(self.limits.max_open_orders),
                    detail="this order would exceed the open-order ceiling",
                    symbol=symbol,
                )
            )

        with accounting():
            projected = (
                side_projected_exposure(
                    position_side, position_qty, working.plus(side, qty)
                )
                + other_side_exposure
            )
            notional = projected * price

        if (
            self.limits.max_position_notional is not None
            and notional > self.limits.max_position_notional
        ):
            return self._record(
                RiskBreach(
                    limit="max_position_notional",
                    action=RiskAction.REJECT,
                    ts_ms=ts_ms,
                    observed=_fmt(notional),
                    allowed=_fmt(self.limits.max_position_notional),
                    detail=(
                        "the position this order could produce, counting everything "
                        "already working, exceeds the notional ceiling"
                        + (
                            f" (both sides summed: {_fmt(other_side_exposure)} already "
                            f"reachable on the other leg)"
                            if other_side_exposure
                            else ""
                        )
                    ),
                    symbol=symbol,
                )
            )

        if self.limits.max_leverage is not None:
            if equity <= 0:
                return self._record(
                    RiskBreach(
                        limit="max_leverage",
                        action=RiskAction.REJECT,
                        ts_ms=ts_ms,
                        observed="inf",
                        allowed=_fmt(self.limits.max_leverage),
                        detail=(
                            "equity is not positive, so any position at all is infinite "
                            "leverage"
                        ),
                        symbol=symbol,
                    )
                )
            with accounting():
                gross = notional + other_notional
                leverage = gross / equity
            if leverage > self.limits.max_leverage:
                return self._record(
                    RiskBreach(
                        limit="max_leverage",
                        action=RiskAction.REJECT,
                        ts_ms=ts_ms,
                        observed=_fmt(leverage),
                        allowed=_fmt(self.limits.max_leverage),
                        detail=(
                            "projected gross notional across all symbols, over equity, "
                            "exceeds the leverage ceiling"
                        ),
                        symbol=symbol,
                    )
                )

        self._note_submission(ts_ms)
        return None

    def _check_rate(self, *, ts_ms: int, symbol: str) -> RiskBreach | None:
        """Spec 7's runaway-loop guard: at most N submissions in any rolling minute.

        The window is `(ts - 60_000, ts]`, so a submission exactly 60 000 ms after an
        earlier one does not count it -- the earlier one has aged out. The boundary is
        worth a millisecond of care because it decides the steady-state rate of any
        strategy whose cadence divides the minute: at one submission every 2 000 ms, the
        inclusive window allows 30 a minute and the exclusive one allows 29.
        """
        limit = self.limits.max_orders_per_minute
        if limit is None:
            return None
        cutoff = ts_ms - MS_PER_MINUTE
        while self._submissions and self._submissions[0] <= cutoff:
            self._submissions.popleft()
        if len(self._submissions) >= limit:
            return self._record(
                RiskBreach(
                    limit="max_orders_per_minute",
                    action=RiskAction.REJECT,
                    ts_ms=ts_ms,
                    observed=str(len(self._submissions) + 1),
                    allowed=str(limit),
                    detail=(
                        "submission rate over the last rolling minute exceeds the ceiling; "
                        "this is the runaway-loop guard, not a trading limit"
                    ),
                    symbol=symbol,
                )
            )
        return None

    def _note_submission(self, ts_ms: int) -> None:
        """Record an *accepted* submission against the rate window.

        Rejected orders are deliberately not counted. A strategy stuck in a loop is
        rate-limited by the check above regardless, and counting refusals would mean a
        run that hit a size limit thirty times then found a legal order would have that
        legal order refused for a reason that has nothing to do with rate.
        """
        self._submissions.append(ts_ms)

    # ------------------------------------------------------------------- account state

    def observe_equity(
        self, ts_ms: int, equity: Money, *, high: Money | None = None
    ) -> RiskBreach | None:
        """Mark-to-market checks. Call on every equity sample, not on trade closes.

        Spec 7: *"Drawdown is measured on mark-to-market equity, not closed-trade PnL. A
        strategy sitting in a 40% unrealised loss is in a 40% drawdown regardless of
        whether it has 'realised' anything."*

        **`equity` is the trough of the sample's band and `high` is its crest, and using one
        for both is a real error rather than a refinement.** A sample covers an interval, and
        within it the account reached a best point and a worst point. Drawdown is the fall
        from the *best* to the *worst*, so the peak has to rise on `high` while the trough is
        scored on `equity`. Feeding the trough to both understates every drawdown by exactly
        the intrabar range: a run whose reported `max_drawdown` was 19.00% against a 15%
        ceiling never halted, because the risk layer's own peak had never seen the 119 976
        the equity curve did. `high` defaults to `equity` for a caller with no band, which is
        the degenerate case and not the common one.
        """
        self._roll_day(ts_ms, equity)
        crest = equity if high is None else max(high, equity)
        if crest > self.peak_equity:
            self.peak_equity = crest
        if self.halted:
            return None

        min_equity = self.min_equity
        if min_equity is not None and equity <= min_equity:
            return self._halt(
                RiskBreach(
                    limit="min_equity",
                    action=RiskAction.HALT,
                    ts_ms=ts_ms,
                    observed=_fmt(equity),
                    allowed=_fmt(min_equity),
                    detail="mark-to-market equity fell to the floor",
                )
            )

        max_daily = self.max_daily_loss
        if max_daily is not None:
            with accounting():
                loss = self.day_open_equity - equity
            if loss >= max_daily:
                return self._halt(
                    RiskBreach(
                        limit="max_daily_loss",
                        action=RiskAction.HALT,
                        ts_ms=ts_ms,
                        observed=_fmt(loss),
                        allowed=_fmt(max_daily),
                        detail=(
                            f"loss since this UTC day opened at "
                            f"{_fmt(self.day_open_equity)}"
                        ),
                    )
                )

        if self.limits.max_drawdown_pct is not None and self.peak_equity > 0:
            with accounting():
                drawdown = (self.peak_equity - equity) / self.peak_equity
            if drawdown >= self.limits.max_drawdown_pct:
                return self._halt(
                    RiskBreach(
                        limit="max_drawdown",
                        action=RiskAction.HALT,
                        ts_ms=ts_ms,
                        observed=_fmt(drawdown),
                        allowed=_fmt(self.limits.max_drawdown_pct),
                        detail=(
                            f"mark-to-market drawdown from a peak of "
                            f"{_fmt(self.peak_equity)}"
                        ),
                    )
                )
        return None

    def _roll_day(self, ts_ms: int, equity: Money) -> None:
        """Re-baseline `day_open_equity` when the UTC day advances.

        **The new day's baseline is the last sample of the old day, carried forward -- not
        the first sample of the new one.** Samples land at bar closes, so the first
        observation of a UTC day arrives up to a whole bar *after* midnight. Baselining on
        it made everything that happened inside that bar belong to neither day: a run that
        closed the last minute of March 1st at 99 976 and opened the first minute of March
        2nd at 95 976 had a 4 000 loss -- twice a 2 000 daily limit -- attributed to nothing
        at all, and did not halt. Carrying the previous observation forward is spec 3.4's
        own LOCF rule, which is how this platform reads every other series between samples.

        **The day only ever moves forward.** An out-of-order or replayed sample used to
        re-base the day to whatever it carried: a late sample stamped yesterday reset
        today's baseline to yesterday's equity, and today's accumulated loss vanished. The
        backtest's queue is ordered so it cannot happen there, but spec 7's first sentence
        makes this the shared core for live trading, where a replayed message is ordinary.

        Days are not required to be consecutive. A run whose data has a hole spanning two
        days rolls once, to the day it lands in, rather than pretending to have observed
        the days it skipped -- and the baseline is still the last thing actually seen.

        **The carried-forward value is the latest sample by timestamp, not the last one to
        arrive.** The day-key guard above stops a stale sample rolling the day *backwards*;
        it says nothing about the equity such a sample leaves behind, and that was assigned
        unconditionally. So a replayed day-5 message arriving during day 6 became the
        baseline day 7 opened at, and a day of trading was then measured against an equity
        the account had held two days earlier -- a `max_daily_loss` that could fire on the
        first sample of a day in which nothing had happened yet, or sleep through one in
        which everything had. Same-day replays did it too: a message stamped 10:00 arriving
        after one stamped 10:05 overwrote the later reading with the earlier.

        Equality wins for the newcomer. Two samples on the same millisecond are ordered by
        arrival and the second is the later of the two.
        """
        key = ts_ms // MS_PER_DAY
        if self.day_key is None:
            self.day_key = key
            self.day_open_equity = equity
        elif key > self.day_key:
            self.day_key = key
            self.day_open_equity = (
                equity if self._last_equity is None else self._last_equity
            )
        if self._last_equity_ms is None or ts_ms >= self._last_equity_ms:
            self._last_equity_ms = ts_ms
            self._last_equity = equity

    def observe_trade_closed(self, ts_ms: int, pnl: Money) -> RiskBreach | None:
        """Spec 7's `max_consecutive_losses`, counted on closed trades.

        A trade closing exactly flat neither extends nor resets the streak. It is not a
        loss, and treating it as a win would let a strategy that alternates loss/flat/loss
        run forever under a limit of 2.
        """
        if self.limits.max_consecutive_losses is None:
            return None
        if pnl > 0:
            self._consecutive_losses = 0
            return None
        if pnl == 0:
            return None
        self._consecutive_losses += 1
        if self._consecutive_losses >= self.limits.max_consecutive_losses:
            return self._halt(
                RiskBreach(
                    limit="max_consecutive_losses",
                    action=RiskAction.HALT,
                    ts_ms=ts_ms,
                    observed=str(self._consecutive_losses),
                    allowed=str(self.limits.max_consecutive_losses),
                    detail="closed trades have lost this many times in a row",
                )
            )
        return None

    def observe_liquidation(self, ts_ms: int, symbol: str) -> RiskBreach | None:
        """Spec 7: `halt_on_liquidation`, default true, *"Stop immediately"*."""
        if not self.limits.halt_on_liquidation:
            return None
        return self._halt(
            RiskBreach(
                limit="halt_on_liquidation",
                action=RiskAction.HALT,
                ts_ms=ts_ms,
                observed="1",
                allowed="0",
                detail="a position was liquidated",
                symbol=symbol,
            ),
            trigger="LIQUIDATION",
        )

    def observe_rejection(self, ts_ms: int, reason: str) -> RiskBreach | None:
        """Spec 7's auto-trigger: repeated rejections from the exchange.

        Counts refusals that came from *outside* the risk layer -- an exchange filter, or
        the ledger declining for margin. Counting the risk layer's own rejections would
        make the trigger fire on itself: a strategy repeatedly asking for too much size
        would trip the kill switch rather than simply being told no, which is the
        behaviour `REJECT` exists to avoid.

        The per-reason tally is keyed by the *normalised* reason and the breach reports the
        raw one, which is not a contradiction: a counter needs a bounded key and an operator
        needs the exchange's own words, and one string cannot be both.
        """
        self._tally_rejection(reason)
        if self.limits.max_consecutive_rejections is None:
            return None
        self._consecutive_rejections += 1
        if self._consecutive_rejections >= self.limits.max_consecutive_rejections:
            return self._halt(
                RiskBreach(
                    limit="max_consecutive_rejections",
                    action=RiskAction.HALT,
                    ts_ms=ts_ms,
                    observed=str(self._consecutive_rejections),
                    allowed=str(self.limits.max_consecutive_rejections),
                    detail=f"consecutive order rejections; the last was {reason!r}",
                )
            )
        return None

    def _tally_rejection(self, reason: str) -> None:
        """Count one refusal under a bounded key.

        Past `MAX_REJECTION_REASONS` distinct keys the tail is pooled rather than dropped.
        Dropping would make the counts stop summing to the number of rejections, and a
        breakdown that does not add up to the total is worse than a coarse one.
        """
        key = normalise_rejection_reason(reason)
        if key not in self.rejections and len(self.rejections) >= MAX_REJECTION_REASONS:
            key = OTHER_REJECTION_REASON
        self.rejections[key] = self.rejections.get(key, 0) + 1

    def observe_acceptance(self) -> None:
        """An order reached the exchange, so the rejection streak is over."""
        self._consecutive_rejections = 0

    def observe_disconnect(
        self, ts_ms: int, down_ms: int, position_open: bool
    ) -> RiskBreach | None:
        """Spec 7's live-only auto-trigger: a socket down too long over an open position.

        Spec 7: *"WS disconnection exceeding `max_disconnect_seconds` (default 30) while a
        position is open"*. Both halves of that sentence are conditions. A disconnect over a
        flat account is a data problem, and halting on one would train an operator to switch
        the trigger off; a disconnect over an open position means the platform has stopped
        seeing the mark that liquidation is decided against and stopped receiving the fills
        that say what it holds, which is the situation nobody can trade their way out of.

        `down_ms` is how long the socket has been down as of `ts_ms`, measured by the caller.
        This module owns no clock, for the reason spec 7's first sentence gives: the same
        code has to reach the same verdict in a backtest, where "now" is an event timestamp.
        """
        if down_ms < 0:
            raise ValueError(
                f"downtime cannot be negative, got {down_ms} ms. Two timestamps were "
                "subtracted the wrong way round, and the trigger would silently never fire."
            )
        limit = self.limits.max_disconnect_seconds
        if limit is None or not position_open:
            return None
        allowed_ms = limit * MS_PER_SECOND
        if down_ms <= allowed_ms:
            return None
        return self._halt(
            RiskBreach(
                limit="max_disconnect_seconds",
                action=RiskAction.HALT,
                ts_ms=ts_ms,
                # Both sides in milliseconds. The limit is written in seconds and the
                # measurement arrives in milliseconds, and reporting each in its own unit
                # would put "observed 31000, limit 30" in the event log.
                observed=str(down_ms),
                allowed=str(allowed_ms),
                detail=(
                    f"the market-data socket was down for {down_ms} ms with a position "
                    f"open; the ceiling is {limit} s"
                ),
            ),
            trigger="DISCONNECT",
        )

    def observe_reconciliation(
        self, ts_ms: int, field: str, ours: Money, theirs: Money, tolerance: Money
    ) -> RiskBreach | None:
        """Spec 6.7.3's live-vs-exchange comparison, one field at a time.

        Spec 6.7 names the five things compared every 60 seconds -- wallet balance, position
        size, entry price, unrealised PnL, liquidation price -- and says *"any mismatch
        beyond `tickSize`/`stepSize` tolerance triggers the kill switch"*. So this has no
        limit to switch it off, for the same reason `observe_invariant_failure` has none: a
        disagreement with the exchange means PerpLab's idea of the account is wrong, and
        every other limit here is being evaluated against that idea. Continuing would be
        trading on a position size the platform only believes it has.

        One field per call, because the tolerance is a property of the field -- `stepSize`
        for a quantity, `tickSize` for a price -- and a single call taking all five would
        have to guess which applied to which.

        `tolerance` is a ceiling on the disagreement, so a difference exactly equal to it
        passes. That is the module docstring's rule for size limits and it is the right one
        here: one tick of rounding is precisely the disagreement the tolerance admits.
        """
        if tolerance < 0:
            raise ValueError(
                f"reconciliation tolerance cannot be negative, got {tolerance}. A negative "
                "tolerance halts on an exact match, which reads as the check working."
            )
        with accounting():
            delta = abs(ours - theirs)
        if delta <= tolerance:
            return None
        return self._halt(
            RiskBreach(
                limit="reconciliation",
                action=RiskAction.HALT,
                ts_ms=ts_ms,
                observed=_fmt(delta),
                allowed=_fmt(tolerance),
                detail=(
                    f"{field} disagrees with the exchange: ours {_fmt(ours)}, "
                    f"theirs {_fmt(theirs)}"
                ),
            ),
            trigger="RECONCILIATION",
        )

    def observe_verification_outage(
        self, ts_ms: int, blind_ms: int, limit_ms: int
    ) -> RiskBreach | None:
        """Halt when the account has gone unverifiably long without a reconciliation pass.

        Spec 6.7.3's loop is the platform's licence to keep trading: every sixty seconds
        the exchange confirms the ledger describes the real account. One failed pass says
        nothing (a timeout is weather), and `Reconciler._note_fetch_failure` deliberately
        never halts on one -- but *no* ceiling meant a revoked key or a forty-minute venue
        outage left the strategy submitting orders against a ledger nobody had verified,
        with the one number describing the exposure (`blind_for_ms`) rendered in a UI
        banner and compared against nothing. Past the ceiling, unverified is
        indistinguishable from wrong, and the platform's rule for that state is to stop.
        """
        if self.halted or blind_ms < limit_ms:
            return None
        return self._halt(
            RiskBreach(
                limit="reconciliation_blind",
                action=RiskAction.HALT,
                ts_ms=ts_ms,
                observed=str(blind_ms),
                allowed=str(limit_ms),
                detail=(
                    "the account has gone this many milliseconds without a successful "
                    "reconciliation pass; the ledger is unverified and trading on it "
                    "would be trading on an assumption"
                ),
            ),
            trigger="RECONCILIATION",
        )

    def observe_invariant_failure(self, ts_ms: int, invariant: str, message: str) -> RiskBreach:
        """Spec 7's first auto-trigger: a conservation invariant failed (spec 3.10).

        Unconditional. There is no limit to switch this off, because an invariant failure
        means the ledger's own arithmetic disagreed with itself -- at which point every
        other limit is being evaluated against numbers that cannot be trusted, and
        continuing would produce a result whose only honest description is "unknown".
        """
        return self._halt(
            RiskBreach(
                limit="invariant",
                action=RiskAction.HALT,
                ts_ms=ts_ms,
                observed=invariant,
                allowed="none",
                detail=f"conservation invariant {invariant} failed: {message}",
            ),
            trigger="INVARIANT",
        )

    # -------------------------------------------------------------------- book-keeping

    def _halt(self, breach: RiskBreach, *, trigger: str | None = None) -> RiskBreach:
        """Record the first halt and ignore the rest.

        A halt is a single event even when several things breach at once -- two symbols
        liquidating inside one check, or a rejection storm following an invariant failure.
        Appending each of them made `breach_count` and the results-page table report a
        cascade of symptoms as though they were independent decisions, when only the first
        one stopped anything.
        """
        if self.halted:
            return breach
        self._record(breach)
        self.halted = True
        self.halt_breach = breach
        self.kill_switch.trip(breach.ts_ms, trigger or breach.limit, breach.message)
        return breach

    def _record(self, breach: RiskBreach) -> RiskBreach:
        """Keep the breach if there is room for it, and count it if there is not.

        The breach is returned either way -- the caller logs it and acts on it, and whether
        this engine still has room to remember it is not the caller's business. Only the
        run-level record is capped. See `MAX_BREACHES` for why the *first* ones are the ones
        worth keeping.
        """
        if breach.action is RiskAction.REJECT:
            self._rejected += 1
        if len(self.breaches) < MAX_BREACHES:
            self.breaches.append(breach)
        else:
            self.breaches_dropped += 1
        return breach

    def usage(
        self,
        *,
        ts_ms: int,
        equity: Money,
        notional: Money,
        max_symbol_notional: Money,
        open_orders: int,
    ) -> list[dict[str, Any]]:
        """How much of each limit this run has spent, for the live monitor (spec 10.3).

        **Deliberately not part of `summary()`.** A usage reading is true at one instant, and
        `summary()` is what the run record stores forever: a stored "62% of the daily loss
        limit" would describe the millisecond the run ended and be read as a fact about the
        run. This is polled, shown, and never persisted.

        **Every reading is measured the same way the check that fires on it measures.** The
        panel exists so an operator can see a limit coming, and a bar that fills on a
        different arithmetic from the refusal is worse than no bar -- it would read 80% at
        the moment the order was refused, or 100% while orders kept going through. So daily
        loss is scored from `day_open_equity` and not from the start of the run, drawdown
        from `peak_equity` and not from the opening balance, and the rate window is the same
        half-open `(ts - 60_000, ts]` `_check_rate` uses.

        **The rate window is counted, not pruned.** `_check_rate` pops aged-out submissions
        because it is about to decide; a reporting call that mutated the same deque would
        make polling the monitor part of the trading logic. Nothing here writes.

        `min_equity` is reported as *budget consumed*, not as a level: it fires when equity
        falls to the floor, so the fraction is how far equity has travelled from the run's
        starting point toward that floor. Drawn any other way, the most dangerous limit on
        the page would sit at 100% on a healthy run and empty as the run died.

        Limits with no numeric reading are omitted rather than shown at zero.
        `halt_on_liquidation` is a switch, not a budget, and `max_disconnect_seconds` is
        measured against a socket the monitor already draws in its connection line -- a
        second, staler copy here could disagree with it.
        """
        rows: list[dict[str, Any]] = []

        def add(limit: str, used: Money, allowed: Money, *, integral: bool = False) -> None:
            # `allowed` is positive for every limit that reaches here -- `__post_init__`
            # refuses a non-positive one -- so the division is safe. Clamped at zero
            # because a *gain* is not negative usage of a loss limit; it is none.
            # `integral` marks the count limits: "2 of 10 orders" is a count, and printing
            # it at the money scale ("2.00000000") would dress a tally as an amount.
            with accounting():
                fraction = used / allowed if allowed > 0 else Decimal(0)
            clamped = max(used, Decimal(0))
            rows.append(
                {
                    "limit": limit,
                    "used": str(int(clamped)) if integral else _fmt(clamped),
                    "allowed": str(int(allowed)) if integral else _fmt(allowed),
                    "fraction": max(0.0, float(fraction)),
                }
            )

        if self.limits.max_position_notional is not None:
            add("max_position_notional", max_symbol_notional, self.limits.max_position_notional)

        if self.limits.max_leverage is not None and equity > 0:
            with accounting():
                leverage = notional / equity
            add("max_leverage", leverage, self.limits.max_leverage)

        max_daily = self.max_daily_loss
        if max_daily is not None:
            with accounting():
                loss = self.day_open_equity - equity
            add("max_daily_loss", loss, max_daily)

        if self.limits.max_drawdown_pct is not None and self.peak_equity > 0:
            with accounting():
                drawdown = (self.peak_equity - equity) / self.peak_equity
            add("max_drawdown", drawdown, self.limits.max_drawdown_pct)

        min_equity = self.min_equity
        if min_equity is not None:
            # The distance from the start down to the floor is the budget; how much of it
            # equity has already spent is the reading. A run that starts at its own floor
            # has no budget at all, and reports fully spent rather than dividing by zero.
            with accounting():
                budget = self.starting_equity - min_equity
                spent = self.starting_equity - equity
            if budget > 0:
                add("min_equity", spent, budget)
            else:
                rows.append(
                    {
                        "limit": "min_equity",
                        "used": _fmt(equity),
                        "allowed": _fmt(min_equity),
                        "fraction": 1.0,
                    }
                )

        if self.limits.max_open_orders is not None:
            add(
                "max_open_orders",
                Decimal(open_orders),
                Decimal(self.limits.max_open_orders),
                integral=True,
            )

        if self.limits.max_orders_per_minute is not None:
            cutoff = ts_ms - MS_PER_MINUTE
            recent = sum(1 for stamp in self._submissions if stamp > cutoff)
            add(
                "max_orders_per_minute",
                Decimal(recent),
                Decimal(self.limits.max_orders_per_minute),
                integral=True,
            )

        if self.limits.max_consecutive_losses is not None:
            add(
                "max_consecutive_losses",
                Decimal(self._consecutive_losses),
                Decimal(self.limits.max_consecutive_losses),
                integral=True,
            )

        if self.limits.max_consecutive_rejections is not None:
            add(
                "max_consecutive_rejections",
                Decimal(self._consecutive_rejections),
                Decimal(self.limits.max_consecutive_rejections),
                integral=True,
            )

        return rows

    def summary(self) -> dict[str, Any]:
        """Everything the run record, the results page and the Feed read about risk.

        The two counts are of *breaches that happened*, not of breaches still in the list.
        Deriving them from `breaches` was correct until the list acquired a ceiling, at
        which point a 48-hour session would have reported exactly 1 000 rejections however
        many it had -- a number that looks like a measurement.
        """
        return {
            "limits": self.limits.to_json(),
            "halted": self.halted,
            "halt_reason": None if self.halt_breach is None else self.halt_breach.to_json(),
            "breach_count": len(self.breaches) + self.breaches_dropped,
            # Reported even when zero. A field that appears only once it fires makes its
            # absence ambiguous between "nothing was dropped" and "this build did not count".
            "breaches_dropped": self.breaches_dropped,
            "rejected_orders": self._rejected,
            "peak_equity": _fmt(self.peak_equity),
            "kill_switch": self.kill_switch.to_json(),
        }


def working_exposure(
    orders: Iterable[tuple[str, Money, bool]],
) -> WorkingExposure:
    """Fold `(side, remaining_qty, reduce_only)` triples into a `WorkingExposure`.

    Reduce-only orders are dropped rather than netted. See the module docstring: counting a
    stop-loss as negative exposure lets a strategy add risk on the strength of an order
    that has not fired.
    """
    exposure = WorkingExposure.zero()
    for side, qty, reduce_only in orders:
        if reduce_only or qty <= 0:
            continue
        exposure = exposure.plus(side, qty)
    return exposure


def limits_from_mapping(obj: Mapping[str, Any] | None) -> RiskLimits:
    """Alias kept for call sites that read better this way."""
    return RiskLimits.from_json(obj)


def breaches_to_json(breaches: Sequence[RiskBreach]) -> list[dict[str, Any]]:
    return [b.to_json() for b in breaches]
