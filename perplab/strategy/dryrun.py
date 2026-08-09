"""The smoke-run harness behind spec 5.5 step 5 — and *only* that.

**This is not the backtest engine.** It has no latency model, no funding cashflow, no
margin brackets and no liquidation, and resting orders never fill. Phase 4 builds the
engine; this exists to answer one question five hundred bars at a time:

> Does this code run at all?

That question catches an enormous share of real failures -- an unbound name in a branch
that only executes after a crossover, a `None` where a value was assumed, an indicator read
before it was ready, a hook with the wrong signature -- and it catches them in a second
rather than twenty minutes into a backtest.

**What it deliberately does not do is produce numbers anyone could mistake for results.**
Positions are tracked only so that `ctx.close()` and `ctx.stop_loss()` have something to
act on; without that, every strategy that attaches a stop after entering would fail
validation for a reason that has nothing to do with the strategy. The PnL it computes is
never surfaced.

**Where it does compute something, it computes it with the engine's own code.** Market
orders are priced by `engine.fills.BookWalkFillModel` against the synthetic ladder, and the
resulting fill is booked by `core.account.Account` -- the same model and the same ledger the
backtester uses. This module used to carry its own copy of both: fills landed exactly at the
mark with no spread and no fees, and the spec 3.3 entry-price cases were re-implemented here
by hand. Two implementations of the same arithmetic agree on the day they are written, and
the smoke run is precisely where a disagreement would be least visible -- nobody reads its
numbers, so nobody would notice them drifting from the engine's. Sharing the code makes the
question moot rather than answering it.

What stays deliberately different is the *fixture*, not the arithmetic: a synthetic ladder,
a one-step stand-in for latency (market orders land at the next clock advance, never inside
the hook that submitted them -- see `DryRunRuntime.submit`), and no resting fills. Those are
properties of a 500-bar validation harness, and each of them is stated rather than hidden.

The event log it accumulates *is* load-bearing, though: it is what the determinism probe
(step 6) hashes, and the hash is compared across two separate interpreters started with
different `PYTHONHASHSEED` values. That is what turns "compare two runs" into a probe that
actually finds set-iteration-order dependence rather than confirming that the same process
does the same thing twice.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, replace
from typing import Any, Mapping, Sequence

from perplab.core.account import Account, FeeSchedule, InsufficientMargin
from perplab.core.money import (
    Money,
    from_scaled,
    money_to_str,
    parse_money,
    quantize_qty,
)
from perplab.core.types import DepthSnapshot, PositionSide, Side
from perplab.engine.fills import MarketInputs, NoQuote, fill_model_for_tier
from perplab.strategy.base import Strategy
from perplab.strategy.context import (
    AccountView,
    Context,
    DataUnavailable,
    FillTier,
    Fill,
    FundingEvent,
    FundingView,
    MacroView,
    OrderEnd,
    OrderIntent,
    OrderType,
    PositionView,
    SpreadView,
    StrategyEvent,
    UnsupportedOrder,
)
from perplab.strategy.indicators import IndicatorSet
from perplab.strategy.params import Requirements
from perplab.strategy.synthetic import (
    synthetic_bars,
    synthetic_depth,
    synthetic_funding,
    synthetic_trades,
)

__all__ = [
    "SMOKE_BARS",
    "MAX_SMOKE_BARS",
    "SMOKE_SEED",
    "DryRunRuntime",
    "SmokeResult",
    "smoke_run",
    "event_hash",
]

SMOKE_BARS = 500
"""Spec 5.5: "run 500 synthetic bars"."""

MAX_SMOKE_BARS = 5_000
"""Ceiling once warm-up is added on.

A strategy declaring `history: 4000` would otherwise never leave warm-up inside 500 bars,
so the smoke run would exercise the indicator plumbing and none of the trading logic -- it
would pass, and prove nothing. Bars are extended to cover warm-up plus a tradeable tail,
and capped here so a declaration of `history: 10_000_000` cannot turn a 10-second timeout
into the thing that fails.
"""

SMOKE_TAIL_BARS = 200
"""Bars past warm-up, so entry and exit logic actually runs."""

SMOKE_SEED = 20260803
"""Seed for both the synthetic data and `ctx.rng`. Fixed, so a validation result is
reproducible and a diagnostic can be re-created from the code alone."""

FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000
"""Settlement cadence for the synthetic feed. Real intervals vary by symbol and have
changed historically (spec 3.5, R17); the smoke run only needs the hook to fire."""

_START_BALANCE = "10000"
_SMOKE_LEVERAGE = 125

_VENUE_MAX_LEVERAGE = 125
"""The highest leverage any Binance USDⓈ-M symbol offers, at its smallest bracket.

Numerically the same as `_SMOKE_LEVERAGE` today and kept separate because the two are
different claims: one is what this fixture starts at, the other is a fact about the venue.
Used by `DryRunRuntime.set_leverage` to refuse the impossible without pretending to know a
particular symbol's actual ceiling -- the smoke run holds no bracket table, so 20x on a
symbol whose top tier is 10x still validates green and is refused when the run loads real
brackets. Catching the impossible is not the same as catching the wrong, and only the first
is available here."""
_TAKER_RATE = "0.0005"
"""Spec 3.8's standard USD-M taker rate, the same default `BacktestConfig` carries."""
_STEP_SIZE = "0.001"
_TICK_SIZE = "0.10"


def _canonical(value: Any) -> Any:
    """Reduce an arbitrary logged value to something JSON-stable.

    User log fields are arbitrary, and two of the obvious fallbacks are wrong here.
    `repr()` embeds memory addresses for most objects, so two identical runs would hash
    differently and every strategy would be reported nondeterministic. Raising would turn a
    stray `ctx.log.info("x", obj=self)` into a validation failure about the logging call
    rather than about the strategy. The type name is deterministic, obviously lossy, and
    says what happened.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, Money):
        return money_to_str(value)
    if isinstance(value, Mapping):
        return {str(k): _canonical(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_canonical(v) for v in value]
        if isinstance(value, (set, frozenset)):
            # A set has no order, so hashing its iteration order would be hashing the
            # interpreter's hash seed. Sorting by the canonical rendering makes the hash a
            # function of the *contents*, which is what the strategy actually decided.
            items.sort(key=lambda v: json.dumps(v, sort_keys=True, default=str))
        return items
    return f"<{type(value).__name__}>"


def event_hash(events: Sequence[StrategyEvent]) -> str:
    """SHA-256 over the canonical event log — the reproducibility invariant of spec 12.1."""
    payload = [
        {
            "seq": event.seq,
            "ts_ms": event.ts_ms,
            "kind": event.kind,
            "payload": _canonical(dict(event.payload)),
        }
        for event in events
    ]
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class DryRunRuntime:
    """A `Runtime` that fills market orders at the next clock advance and records everything.

    Satisfies the protocol in `strategy.context` structurally, which is the point: if the
    protocol ever grows a method, this stops satisfying it and the validator breaks loudly
    rather than the smoke run silently testing less than it used to.

    **A market order is never a position inside the hook that submitted it.** The engine
    queues every submission behind modelled latency (`backtest._submit` schedules the
    arrival at `now + latency`), so `ctx.position()` read in the same hook still says flat
    -- and `Context._protective` and `Context.close` both read the position synchronously
    to infer a side. This runtime used to fill inside `submit()` instead, and the canonical
    entry+stop pattern (`ctx.buy(...); ctx.stop_loss(...)` in one hook) validated green,
    then died on the first bar of the backtest; `ctx.buy(); ctx.close()` was worse -- a
    full round trip in validation, a silent no-op leaving the position open in the run.
    Validation must not be more permissive than the engine about *when* a fill exists, so
    submissions queue in `submit` and land in `advance` -- the smallest latency model that
    gives same-hook reads the engine's answer. What is deliberately not modelled is the
    latency *distribution*: one clock step is not a claim about milliseconds, and the fill
    still prices off the engine's own model when it lands.

    **The smoke run reports `BOOK_WALK`, and is deliberately more permissive than any real
    one.** That is now a statement about *capability*, not about arithmetic: fills are
    priced by the real `BOOK_WALK` model against a synthetic ladder, so the numbers are the
    engine's. What stays permissive is what the tier *allows* -- `ctx.book()` always
    succeeds here, so a strategy that calls it without declaring `depth20` validates green
    and raises `DataUnavailable` on the first bar of a run at a lower tier. That asymmetry is the right way round -- validation exists to answer
    "does this code execute", and refusing a strategy because *today's* lake has no depth
    would make the answer depend on the lake rather than on the code -- but it is an
    asymmetry, and it is the one place a green validation does not imply a runnable backtest.
    `BacktestEngine._check_declarations` catches the declared case before the run starts;
    the undeclared case is caught by `ctx.book()` itself, with the tier named.
    """

    def __init__(
        self,
        *,
        symbols: Sequence[str],
        fill_tier: FillTier = FillTier.BOOK_WALK,
        start_balance: str = _START_BALANCE,
        macro_declared: bool = False,
    ) -> None:
        self.symbols = tuple(symbols)
        self._fill_tier = fill_tier
        self.macro_declared = macro_declared
        """Whether the strategy declared a macro dataset -- passed in from `requires`.

        Mirrors `EngineRuntime.macro_declared` so `ctx.macro()` gives the same *kind* of
        answer here as in a run: raise when it was never declared, `None` when it was and
        nothing has been published. The value itself is always `None` here, because the
        smoke run has no macro store and inventing a plausible reading is exactly how a
        strategy validates green and dies on the first real bar."""
        self._now_ms = 0
        self._zero = parse_money("0")
        self._step = parse_money(_STEP_SIZE)
        self._tick = parse_money(_TICK_SIZE)
        # The engine's ledger, not a copy of it. `require_brackets=False` because the smoke
        # run models no liquidation and has no bracket snapshot to model one from -- which
        # is a stated property of the harness, and the one place it is allowed to differ.
        self.account = Account(
            opening_balance=parse_money(start_balance),
            fees=FeeSchedule.all_taker(parse_money(_TAKER_RATE), "smoke-run-default"),
            require_brackets=False,
        )
        for symbol in self.symbols:
            # **Not the engine's 1x default.** The ledger now enforces margin, and a smoke
            # run that refused a 0.5 BTC order against a 10 000 synthetic balance would fail
            # validation for a property of the *fixture* rather than of the code. 125x is
            # BTCUSDT's own top bracket, so it is a real number rather than an arbitrary
            # large one, and it is reported honestly by `ctx.leverage()` -- which is also
            # what a strategy calling `ctx.set_leverage()` overwrites from here.
            self.account.set_leverage(symbol, _SMOKE_LEVERAGE)
        self._model = fill_model_for_tier(fill_tier.name)
        self._depth: dict[str, DepthSnapshot] = {}
        self._funding: dict[str, FundingView] = {
            symbol: FundingView(None, None, None) for symbol in self.symbols
        }
        self._open_interest: dict[str, float] = {}
        self.events: list[StrategyEvent] = []
        self._seq = 0
        self._order_seq = 0
        self._resting: dict[str, OrderIntent] = {}
        self._pending_markets: list[tuple[str, OrderIntent]] = []
        self._pending_fills: list[Fill] = []
        self._pending_ends: list[OrderEnd] = []

    # --------------------------------------------------------------- engine driving

    def advance(self, ts_ms: int) -> None:
        """Move the clock forward; in-flight market orders land on a strict advance.

        Strict, because "the same millisecond" is the same hook invocation as far as this
        harness can tell, and the whole point of queueing (see the class docstring) is that
        a submission is not a position until time has passed. Fills are booked here, at
        arrival, against the marks and ladder then in force -- which is the engine's shape:
        the reference price is captured at submission, but the execution is priced at
        arrival. FIFO, so an entry submitted before a reduce-only exit in the same hook
        still books before it.
        """
        if ts_ms < self._now_ms:
            raise ValueError(
                f"the smoke-run clock cannot move backwards: {ts_ms} < {self._now_ms}"
            )
        advanced = ts_ms > self._now_ms
        self._now_ms = ts_ms
        if advanced and self._pending_markets:
            pending, self._pending_markets = self._pending_markets, []
            for order_id, intent in pending:
                self._fill_market(order_id, intent)

    def set_mark(self, symbol: str, price: Money) -> None:
        self.account.update_mark(self._now_ms, symbol, price)

    def set_depth(self, snapshot: DepthSnapshot) -> None:
        self._depth[snapshot.symbol] = snapshot

    def set_funding(self, symbol: str, view: FundingView) -> None:
        self._funding[symbol] = view

    def set_open_interest(self, symbol: str, value: float) -> None:
        self._open_interest[symbol] = value

    def drain_ends(self) -> list[OrderEnd]:
        """Hand back `on_cancel` notifications since the last drain.

        Queued for the same reason fills are: dispatching from inside `cancel()` would make
        `on_cancel` re-entrant with the hook that issued the cancel, and a strategy that
        re-quotes in `on_cancel` -- which is the entire point of the hook -- would recurse
        until the stack died, in the validator, for a pattern the engine handles fine.
        """
        ends, self._pending_ends = self._pending_ends, []
        return ends

    def drain_fills(self) -> list[Fill]:
        """Hand back fills produced since the last drain.

        Queued rather than dispatched inside `submit`, because dispatching there would make
        `on_fill` re-entrant with the hook that placed the order. A strategy that buys in
        `on_fill` would then recurse until the stack died, in the validator, for a pattern
        that is perfectly legal in a real engine where fills arrive as separate events
        (spec 6.2).
        """
        fills, self._pending_fills = self._pending_fills, []
        return fills

    # ---------------------------------------------------------------- Runtime surface

    @property
    def now_ms(self) -> int:
        return self._now_ms

    @property
    def fill_tier(self) -> FillTier:
        return self._fill_tier

    @property
    def hedge_mode(self) -> bool:
        """Always one-way.

        The smoke run exists to execute a strategy's code once, not to reproduce a run's
        account configuration, and a hedged harness would refuse every `ctx.buy()` that did
        not name a side -- turning validation of a one-way strategy into a wall of errors
        about a mode it never asked for. A hedge-mode strategy's side-routed orders still
        validate here: `PositionSide.LONG` on a one-way runtime is refused loudly by
        `Context._resolve_side`, which is the diagnostic the author needs.
        """
        return False

    def position_view(
        self, symbol: str, position_side: PositionSide = PositionSide.BOTH
    ) -> PositionView:
        """Read straight off the ledger, so it cannot disagree with `account_view`.

        `liquidation_price` is `None` and stays `None`: no bracket table is loaded, and a
        sentinel here would be compared against the mark by a strategy's safety rail and
        quietly satisfy it.
        """
        position = self.account.position(symbol, position_side)
        if position is None:
            return PositionView(
                symbol=symbol,
                qty=self._zero,
                entry_price=self._zero,
                unrealized_pnl=self._zero,
                liquidation_price=None,
                margin=self._zero,
                position_side=position_side,
            )
        mark = self.account.marks.get(symbol, position.entry_price)
        return PositionView(
            symbol=symbol,
            qty=position.qty,
            entry_price=position.entry_price,
            unrealized_pnl=position.unrealized_pnl(mark),
            liquidation_price=None,
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
        mark = self.account.marks.get(symbol)
        if mark is None:
            raise RuntimeError(
                f"no mark price for {symbol} yet; the first bar has not closed"
            )
        return mark

    def funding_view(self, symbol: str) -> FundingView:
        return self._funding[symbol]

    def open_interest(self, symbol: str) -> float | None:
        return self._open_interest.get(symbol)

    def macro(self, name: str) -> MacroView | None:
        """Always `None` when declared; raises when it was not (mirrors `EngineRuntime`).

        **This method's absence was a live bug.** `Runtime` is a structural `Protocol` with
        no runtime enforcement, so omitting it here raised nothing at import and nothing at
        construction -- it surfaced as `AttributeError: 'DryRunRuntime' object has no
        attribute 'macro'` attributed to the *strategy's* line number, which made every
        macro-using strategy fail validation and, since a run cannot start from an invalid
        version, made Phase 11 unreachable through the editor. `test_context.py`'s
        conformance test now enumerates the protocol so the next omission fails loudly here
        instead of quietly there.

        `None` rather than a synthetic reading: spec 1.4 and this module's own history --
        the same-hook fill, the fabricated `predicted_rate` -- say a fixture that answers
        more generously than the run is worse than one that answers less. A strategy has to
        handle "nothing published yet" anyway, because a real run begins that way.
        """
        if not self.macro_declared:
            raise DataUnavailable(
                f"ctx.macro({name!r}) needs a macro dataset declared: add 'macroGlobal' "
                "(BTC dominance, total market cap) or 'macroFx' (DXY) to "
                "requires['datasets'] so the engine loads it before the run."
            )
        return None

    def depth(self, symbol: str) -> DepthSnapshot | None:
        return self._depth.get(symbol)

    def spread(self, symbol: str) -> SpreadView | None:
        snapshot = self._depth.get(symbol)
        if snapshot is None or not snapshot.bid_px or not snapshot.ask_px:
            return None
        return SpreadView(
            bid=from_scaled(snapshot.bid_px[0]), ask=from_scaled(snapshot.ask_px[0])
        )

    def step_size(self, symbol: str) -> Money:
        return self._step

    def leverage(self, symbol: str) -> int:
        return self.account.leverage(symbol)

    def set_leverage(self, symbol: str, leverage: int) -> None:
        """The ledger's own setter, with the ledger's own refusals.

        Deliberately *not* faked into always succeeding. `Account.set_leverage` refuses
        below 1x, above the symbol's top bracket, and -- the one that matters -- while any
        side of the symbol is open, and a strategy that raises leverage mid-position has to
        meet that refusal here rather than on the first bar of a real run. The smoke run's
        synthetic strategy does open positions, so this is genuinely exercised.

        The exception is the live-mode refusal, which cannot be reproduced here: validation
        has no session mode, and failing every strategy that calls this just in case it is
        later run live would refuse working backtest code. `EngineRuntime.set_leverage`
        raises there instead, and the docstring on `ctx.set_leverage` says so.
        """
        if leverage > _VENUE_MAX_LEVERAGE:
            # The ledger's own bracket check cannot fire here: this account is built with
            # `require_brackets=False` and holds no table, so `set_leverage` skips it and a
            # strategy asking for 500x validated green and died at run start. The venue-wide
            # ceiling is the part that is knowable without a table.
            raise ValueError(
                f"{symbol}: leverage {leverage} exceeds {_VENUE_MAX_LEVERAGE}x, which is "
                "the most any Binance USDⓈ-M symbol offers. The run will also check this "
                "symbol's own bracket, which is lower for all but the smallest positions."
            )
        self.account.set_leverage(symbol, leverage)

    def submit(self, intent: OrderIntent) -> str:
        self._order_seq += 1
        order_id = f"smoke-{self._order_seq}"
        self.emit("ORDER", {"order_id": order_id, **intent.to_json()})
        if intent.type is OrderType.MARKET:
            # Refused *now*, not at arrival: the engine refuses a submission with no
            # reference price at `_submit` time (`RunAborted`), and deferring this check
            # would re-attribute a fixture problem to whatever hook the clock next
            # advanced under.
            if self.account.marks.get(intent.symbol) is None:
                raise RuntimeError(
                    f"cannot fill {intent.symbol} before its first mark price exists"
                )
            # Queued, not filled: the fill lands at the next clock advance, mirroring the
            # engine's latency queue closely enough that a same-hook `ctx.position()` read
            # gives the engine's answer (see the class docstring). A cancel racing an
            # in-flight market order loses, as it does in the engine -- `cancel()` only
            # reaches resting orders, and the queued fill still lands.
            self._pending_markets.append((order_id, intent))
        else:
            # Resting orders are recorded and never filled, and that stays right now that
            # the engine models them properly. This is a 500-bar smoke run over synthetic
            # data whose job is "does this code execute", not "what would it earn": a queue
            # model here would need a synthetic book, and a stop firing at a moment invented
            # by the fixture would teach the author something false about their strategy.
            self._resting[order_id] = intent
        return order_id

    def cancel(self, order_id: str) -> None:
        intent = self._resting.pop(order_id, None)
        self.emit("CANCEL", {"order_id": order_id})
        if intent is not None:
            self._note_end(order_id, intent, "cancelled by strategy")

    def cancel_all(self, symbol: str | None) -> None:
        for order_id, intent in list(self._resting.items()):
            if symbol is None or intent.symbol == symbol:
                del self._resting[order_id]
                self._note_end(order_id, intent, "cancelled by strategy")
        self.emit("CANCEL_ALL", {"symbol": symbol})

    def modify(self, order_id: str, price: Money | None, qty: Money | None) -> None:
        """Amend a resting order, so `on_cancel`-and-requote code paths get executed.

        No queue model and no latency here -- the amendment simply lands. What the smoke run
        is checking is that the call signature is right and the surrounding branch runs; the
        engine is where the priority rule and the in-flight race are modelled, and pretending
        to model them against a synthetic ladder would teach the author something false.
        """
        intent = self._resting.get(order_id)
        if intent is None:
            # The engine no-ops on an order that is gone -- it lost the race, which is R19
            # and not a strategy error. Raising here made validation red for a strategy the
            # backtester accepts, which is the wrong way round for a harness whose question
            # is "does this code run".
            return
        if intent.type is not OrderType.LIMIT:
            raise UnsupportedOrder(
                f"only LIMIT orders can be amended; {order_id} is a {intent.type.value}. "
                "Cancel it and submit a new one."
            )
        self._resting[order_id] = replace(
            intent,
            price=intent.price if price is None else price,
            qty=intent.qty if qty is None else qty,
        )
        self.emit(
            "MODIFY",
            {
                "order_id": order_id,
                "price": None if price is None else money_to_str(price),
                "qty": None if qty is None else money_to_str(qty),
            },
        )

    def _note_end(self, order_id: str, intent: OrderIntent, reason: str) -> None:
        self._pending_ends.append(
            OrderEnd(
                order_id=order_id,
                symbol=intent.symbol,
                side=intent.side,
                status="CANCELLED",
                reason=reason,
                ts_ms=self._now_ms,
                filled_qty=self._zero,
                remaining_qty=intent.qty,
                tag=intent.tag,
            )
        )

    def open_order_ids(self, symbol: str | None) -> Sequence[str]:
        """Resting orders plus market orders still in flight.

        The engine counts every order that is `is_open`, and a market order between
        submission and arrival is exactly that -- a strategy polling `ctx.open_orders()`
        right after `ctx.buy()` sees one entry in both harnesses.
        """
        return tuple(
            order_id
            for order_id, intent in (
                list(self._resting.items()) + self._pending_markets
            )
            if symbol is None or intent.symbol == symbol
        )

    def emit(self, kind: str, payload: Mapping[str, Any]) -> None:
        self._seq += 1
        self.events.append(
            StrategyEvent(seq=self._seq, ts_ms=self._now_ms, kind=kind, payload=dict(payload))
        )

    # ---------------------------------------------------------------------- filling

    def _fill_market(self, order_id: str, intent: OrderIntent) -> None:
        """Price the order with the engine's fill model, then book it in the engine's ledger.

        Runs from `advance`, one clock step after submission -- never from inside `submit`,
        so the hook that placed the order cannot observe its own fill (see the class
        docstring). Every line of arithmetic below this point belongs to `engine.fills` or
        `core.account`. What is local is the *fixture*: the synthetic ladder, a reference
        price equal to the mark then in force, and a single clock step standing in for the
        engine's sampled latency. Those are the harness's stated simplifications; the
        entry-price cases of spec 3.3, the fee schedule and the far-touch walk are not
        re-derived here.
        """
        mark = self.account.marks.get(intent.symbol)
        if mark is None:
            raise RuntimeError(
                f"cannot fill {intent.symbol} before its first mark price exists"
            )
        qty = quantize_qty(intent.qty, self._step)
        if qty <= 0:
            # `Context._order` already refuses a non-positive quantity, so nothing a
            # strategy writes reaches here -- but `submit` is the `Runtime` protocol surface
            # and a future engine calls it directly. Unguarded, this produced a
            # `decimal.DivisionUndefined` from inside the runtime, which `smoke_run` would
            # have attributed to the strategy and reported with the strategy's line number:
            # a platform crash presented as a strategy bug.
            return

        side = Side.BUY if intent.side == "BUY" else Side.SELL
        if intent.reduce_only:
            # Clamp so a reduce-only order can never flip the position, exactly as the
            # exchange does. A smoke run that let it flip would let a strategy bug --
            # closing twice, say -- silently open the opposite side.
            held = self.account.qty(intent.symbol)
            if held == 0 or (held > 0) == (side is Side.BUY):
                # Cancelled and *reported*, matching `backtest._clamp`. Returning silently
                # left a strategy that requotes from `on_cancel` waiting forever in the
                # smoke run for an event the engine does send.
                reason = "reduce-only: the position it would have reduced is already flat"
                self.emit("CANCEL", {"order_id": order_id, "reason": reason})
                self._note_end(order_id, intent, reason)
                return
            if abs(held) < qty:
                qty = quantize_qty(abs(held), self._step)
                if qty <= 0:
                    return

        snapshot = self._depth.get(intent.symbol)
        try:
            quote = self._model.quote(
                MarketInputs(
                    side=side,
                    qty=qty,
                    tick_size=self._tick,
                    reference_price=mark,
                    print_price=mark,
                    top=None,
                    ladder=snapshot,
                    recent_notional=None,
                )
            )
        except NoQuote as exc:
            # No ladder for this symbol yet. The harness fills at the mark rather than
            # refusing, because a validation failure here would be about the fixture --
            # `smoke_run` primes a ladder on the first bar close, and a market order from
            # `on_tick` can precede it. The engine `_reject`s instead, so this is a real
            # divergence and it is recorded rather than left for someone to find.
            self.emit("NO_QUOTE_FILL", {"order_id": order_id, "reason": str(exc)})
            price = mark
        else:
            price = quote.price

        signed = qty if side is Side.BUY else -qty
        try:
            self.account.apply_fill(
                self._now_ms, intent.symbol, signed, price, is_maker=False
            )
        except InsufficientMargin as exc:
            # **Reported, not raised.** This is what the exchange does, and a strategy that
            # sizes beyond its account should see the rejection and be able to handle it --
            # in validation as in live. Letting it propagate would fail the whole smoke run
            # with a platform exception carrying the strategy's line number, which is the
            # one thing `smoke_run` exists to avoid.
            self.emit("REJECT", {"order_id": order_id, "reason": str(exc)})
            self._pending_ends.append(
                OrderEnd(
                    order_id=order_id,
                    symbol=intent.symbol,
                    side=intent.side,
                    status="REJECTED",
                    reason=str(exc),
                    ts_ms=self._now_ms,
                    filled_qty=self._zero,
                    remaining_qty=qty,
                    tag=intent.tag,
                )
            )
            return

        fill = Fill(
            order_id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            qty=qty,
            price=price,
            ts_ms=self._now_ms,
            is_maker=False,
            reduce_only=intent.reduce_only,
            tag=intent.tag,
        )
        self._pending_fills.append(fill)
        self.emit("FILL", fill.to_json())


@dataclass(frozen=True, slots=True)
class SmokeResult:
    """Outcome of one smoke run."""

    ok: bool
    bars: int
    warmup: int
    indicator_warmup: int
    events: int
    hash: str
    orders: int
    error: str | None = None
    traceback: str | None = None
    error_line: int | None = None
    hook: str | None = None
    reached_warm: bool = False
    """Did the run ever leave warm-up?

    `False` means the smoke run exercised the indicator plumbing and none of the trading
    logic, because `MAX_SMOKE_BARS` caps the bar count while the warm-up gate does not. A
    strategy declaring `history: 6000` therefore ran 5 000 bars and was never warm, placed
    no orders, passed, and had its event log -- containing nothing the strategy decided --
    compared against itself by the determinism probe. Reported so the validator can say so
    rather than let `ok=True` stand for "nothing was tested"."""


def smoke_run(
    strategy: Strategy,
    requirements: Requirements,
    *,
    bars: int = SMOKE_BARS,
    seed: int = SMOKE_SEED,
    filename: str = "<strategy>",
) -> SmokeResult:
    """Run `strategy` over synthetic data. Never raises for a strategy-side failure.

    A strategy that crashes is the *expected* outcome of validation, not an exception the
    caller has to handle, so the traceback is captured, reduced to the line inside the
    strategy file, and returned as data. Letting it propagate would put a platform
    traceback in front of the author instead of a gutter marker (spec 5.5).
    """
    import traceback as _traceback

    symbols = requirements.symbols
    timeframe_ms = requirements.timeframe_ms
    runtime = DryRunRuntime(
        symbols=symbols,
        # The same set `BacktestEngine.load_macro` gates on, spelled out rather than matched
        # by prefix: validation deciding "declared" differently from the engine is how a
        # strategy validates green against one rule and runs against another.
        macro_declared=bool(set(requirements.datasets) & {"macroGlobal", "macroFx"}),
    )
    indicators = IndicatorSet(
        primary_symbol=symbols[0], symbols=symbols, bar_ms=timeframe_ms
    )
    context = Context(
        _runtime=runtime,
        symbols=symbols,
        timeframe=requirements.timeframe,
        indicators=indicators,
        rng=random.Random(seed),
    )

    hooks = type(strategy).implemented_hooks()
    current_hook = "on_start"

    def _fail(exc: BaseException) -> SmokeResult:
        tb = _traceback.TracebackException.from_exception(exc)
        line = None
        for frame in tb.stack:
            if frame.filename == filename:
                line = frame.lineno
        return SmokeResult(
            ok=False,
            bars=context.bars_seen,
            warmup=0,
            indicator_warmup=indicators.warmup,
            events=len(runtime.events),
            hash=event_hash(runtime.events),
            orders=sum(1 for e in runtime.events if e.kind == "ORDER"),
            error=f"{type(exc).__name__}: {exc}",
            traceback="".join(tb.format()),
            error_line=line,
            hook=current_hook,
        )

    try:
        strategy.on_start(context)
    except BaseException as exc:  # noqa: BLE001 - strategy failure is data, not control flow
        return _fail(exc)

    indicators.freeze()
    indicator_warmup = indicators.warmup
    warmup = max(indicator_warmup, requirements.history)
    total_bars = min(max(bars, warmup + SMOKE_TAIL_BARS), MAX_SMOKE_BARS)
    context._set_warmup_bars(warmup)

    wants_ticks = "on_tick" in hooks or any(i.feed == "trade" for i in indicators.all())

    def _dispatch_fills() -> None:
        """Deliver queued fills and order-ends, in that order.

        Fills first, because that is the order the engine produces them in: an order fills
        or it ends, and a strategy that cancels the rest of a bracket from `on_fill` should
        see its own cancel afterwards rather than before. The loop repeats because either
        hook may place or cancel more orders, and the queues are drained rather than
        snapshotted.
        """
        while True:
            fills = runtime.drain_fills()
            ends = runtime.drain_ends()
            if not fills and not ends:
                return
            for fill in fills:
                if "on_fill" in hooks:
                    strategy.on_fill(context, fill)
            for end in ends:
                if "on_cancel" in hooks:
                    strategy.on_cancel(context, end)

    try:
        # Each symbol gets its own walk, seeded from its name, so a pairs strategy sees two
        # series that are not identical. Bars are aligned on the same grid because the
        # engine's total ordering (spec 6.2) resolves same-millisecond events by dataset,
        # and aligned bars are the case that ordering has to get right.
        series_by_symbol = {
            symbol: synthetic_bars(
                total_bars,
                symbol=symbol,
                timeframe_ms=timeframe_ms,
                seed=seed ^ _symbol_seed(symbol),
            )
            for symbol in symbols
        }
        for symbol, series in series_by_symbol.items():
            # Prime every mark before any hook can ask for one, so a strategy reading
            # ctx.mark("ETHUSDT") on the first bar does not fail for want of data a real
            # engine would already have had.
            runtime.set_mark(symbol, from_scaled(series[0].close))

        primary = symbols[0]
        next_funding_ms = series_by_symbol[primary][0].open_time + FUNDING_INTERVAL_MS

        for index in range(total_bars):
            step = [(symbol, series_by_symbol[symbol][index]) for symbol in symbols]
            primary_bar = step[0][1]

            if wants_ticks:
                current_hook = "on_tick"
                for trade in synthetic_trades(primary_bar, seed=seed):
                    runtime.advance(trade.ts_ms)
                    # `price_scaled`, not `price`. `TradePrint` exposes both faces --
                    # a float in price units and the lake's exact integer -- and
                    # `from_scaled(trade.price)` divided an already-scaled float by
                    # 10^8, so every mark during the tick loop was 0.0003 instead of
                    # 30 000. The engine reads `price_scaled` here; so does this now.
                    runtime.set_mark(primary, from_scaled(trade.price_scaled))
                    indicators.on_trade(trade)
                    if "on_tick" in hooks:
                        strategy.on_tick(context, trade)
                        _dispatch_fills()

            runtime.advance(primary_bar.close_time)
            # Fills that just landed (orders from the previous bar's hooks, or from this
            # bar's tick loop when the strategy has no `on_tick`) are dispatched before
            # `on_bar`, matching the engine's ordering: an arrival precedes the next bar
            # close, so `on_fill` runs before the `on_bar` that follows it.
            _dispatch_fills()
            for symbol, bar in step:
                runtime.set_mark(symbol, from_scaled(bar.close))
                snapshot = synthetic_depth(bar, seed=seed)
                runtime.set_depth(snapshot)
                indicators.on_depth(snapshot)
            runtime.set_open_interest(primary, 1000.0 + index)
            indicators.on_open_interest(primary, 1000.0 + index)

            if primary_bar.close_time >= next_funding_ms:
                current_hook = "on_funding"
                rate_text = synthetic_funding(primary_bar, seed=seed)
                rate = parse_money(rate_text)
                mark = from_scaled(primary_bar.close)
                indicators.on_funding(primary, float(rate_text))
                runtime.set_funding(
                    primary,
                    FundingView(
                        last_rate=rate,
                        next_settlement_ms=next_funding_ms + FUNDING_INTERVAL_MS,
                        # `None`, exactly as the engine reports it (`backtest` builds every
                        # `FundingView` with `predicted_rate=None` -- historical data holds
                        # settlements, not the forecasts that preceded them). This used to
                        # fabricate the realised rate as the "prediction", so
                        # `ctx.funding().predicted_rate > x` validated green and raised
                        # TypeError at the first real settlement. Validation must never be
                        # more generous than the run: a field the run cannot supply has to
                        # be absent here too, so the strategy meets its None where the
                        # answer is cheap.
                        predicted_rate=None,
                    ),
                )
                if "on_funding" in hooks:
                    position = runtime.position_view(primary)
                    strategy.on_funding(
                        context,
                        FundingEvent(
                            symbol=primary,
                            ts_ms=next_funding_ms,
                            rate=rate,
                            mark_price=mark,
                            # Signed from this account: a long pays when the rate is
                            # positive (spec 3.5).
                            payment=-(position.qty * mark * rate),
                        ),
                    )
                    _dispatch_fills()
                next_funding_ms += FUNDING_INTERVAL_MS

            # Indicators for every symbol advance before any `on_bar` runs. All bars in a
            # step close at the same instant, so advancing one symbol's indicators after
            # another symbol's hook had already read them would make the reading depend on
            # symbol ordering.
            for _, bar in step:
                indicators.on_bar(bar)
            context._note_bar()
            context._set_warm(context.bars_seen >= warmup)

            current_hook = "on_bar"
            if "on_bar" in hooks:
                for _, bar in step:
                    strategy.on_bar(context, bar)
                    _dispatch_fills()

        current_hook = "on_stop"
        if "on_stop" in hooks:
            strategy.on_stop(context)
            _dispatch_fills()
    except BaseException as exc:  # noqa: BLE001
        return _fail(exc)

    return SmokeResult(
        ok=True,
        bars=context.bars_seen,
        warmup=warmup,
        indicator_warmup=indicator_warmup,
        events=len(runtime.events),
        hash=event_hash(runtime.events),
        orders=sum(1 for e in runtime.events if e.kind == "ORDER"),
        reached_warm=context.warm,
    )


def _symbol_seed(symbol: str) -> int:
    """A stable per-symbol offset.

    Python's `hash(str)` is salted by `PYTHONHASHSEED` and therefore differs between
    interpreters -- which is exactly what the determinism probe varies. Using it here would
    generate different synthetic data in the two probe runs and report every multi-symbol
    strategy as nondeterministic. This sum is stable across processes and machines.
    """
    return sum((index + 1) * ord(ch) for index, ch in enumerate(symbol)) & 0xFFFF
