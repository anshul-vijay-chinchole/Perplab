"""Where a testnet or live order actually goes: signed REST out, user-data stream back.

Spec 6.1's table permits a mode to change exactly two things once the data source is
accounted for -- the *order destination* and the *fill source* -- and `engine.transport`
names that seam. `SimulatedTransport` is the backtest half of it. This module is the other
half: `place` signs a `POST /fapi/v1/order`, and the outcome arrives later on the user-data
stream as an `ExchangeReport`, which `on_report` routes into the engine's own `_book_fill`,
`_remove` and `_reject`.

**This code has never sent an order to Binance.** Doing so needs API credentials, and none
are available in this environment. Everything below is exercised against a fake signed
client and hand-written reports (`tests/unit/test_exchange_transport.py`), which proves the
routing, the identifiers and the failure handling are what this module claims -- and proves
nothing whatever about how Binance actually answers. Spec 13's Phase 8 exit criterion is a
real round trip reconciled to the cent, and until that has happened every sentence here
describes an intention rather than an observation. Places where the exchange's behaviour was
inferred from its documentation rather than measured are called out where they occur.

**What this module refuses to do.**

*It does not price a fill, size an order, validate a filter or touch the ledger.* All of
that is the engine's shared code (spec 6.1: *"if a piece of logic could live in the shared
core, it must"*), and a transport that reached in would be the duplication spec 14-I8
records as the defect this architecture exists to prevent. The engine quantises the quantity
at submission, runs the risk verdict, and owns every state transition; this module carries
bytes and hands back what came off the wire.

*It does not decide that an order was rejected.* Only two things can: an exchange refusal
with a code attached, and the engine's own ledger. In particular an `OrderOutcomeUnknown` --
a POST that was sent and not answered -- is **not** a rejection. The order may be working,
may have filled, may never have arrived, and the one reading that turns an unknown into a
loss is "it failed": the engine's ledger would go flat while the exchange holds a position,
and the next thing to notice would be a liquidation. Unknowns are recorded, surfaced, and
left for spec 6.7.3's reconciliation to settle.

*It does not mark an order cancelled.* Spec 6.3 and R19: a cancel is a race it can lose, so
`cancel` sends the DELETE and the outcome comes back the same way a fill does.

**The client order id is the platform's, not the strategy's.** Every order is submitted as
`pl{run_id}-{order_seq}`, built here from the engine's own order sequence. Binance restricts
`newClientOrderId` to 36 characters of `^[.A-Za-z0-9:/_-]+$` and nothing in this codebase
validates a strategy-supplied id against that, so letting one through would convert a typo
in a strategy into a `-1100` from the exchange -- which arrives on the rejection path, feeds
`RiskEngine.observe_rejection`, and can trip spec 7's kill switch on what is really a
formatting mistake. The strategy's own `tag` is carried on the route instead, where it
labels fills without ever reaching the wire.

**Requests are queued and sent in order by `run`.** `OrderTransport` is a synchronous
protocol called from inside a strategy hook, and the signed client is async, so `place`
enqueues and the worker sends. Serialising rather than firing concurrent tasks is
deliberate: `RateBudget` counts the exchange's own order-count headers, and a burst of
concurrent POSTs spends that budget faster than the headers can report it -- which is how a
rate-limit warning becomes the 418 IP ban that takes an account offline with positions open.
A session that never drives `run` (or `drain`) would otherwise queue orders silently
forever, so the queue has a hard ceiling and says what is wrong when it reaches it.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from perplab.core.money import (
    Money,
    accounting,
    decimal_to_scaled,
    from_scaled,
    money_to_str,
    parse_money,
)
from perplab.core.types import CollectorEventKind
from perplab.engine.executor_base import Order, OrderStatus
from perplab.exchange.rest import BinanceRestError
from perplab.exchange.signed import (
    ORDER_DOES_NOT_EXIST_CODE,
    UNKNOWN_ORDER_CODE,
    OrderOutcomeUnknown,
    RateBudgetExceeded,
)
from perplab.exchange.userstream import ExchangeReport, ReportKind
from perplab.live.preflight import PreflightReport
from perplab.strategy.context import OrderType, UnsupportedOrder

if TYPE_CHECKING:  # pragma: no cover - import cycle; the engine imports engine.transport
    from perplab.engine.backtest import BacktestEngine
    from perplab.exchange.signed import SignedRestClient

__all__ = [
    "CLIENT_ORDER_ID_PATTERN",
    "CLIENT_ORDER_ID_PREFIX",
    "DRAIN_INTERVAL_S",
    "EXCHANGE_CLOSE_PREFIXES",
    "MAX_CLIENT_ORDER_ID_LEN",
    "MAX_LATENCY_SAMPLES",
    "MAX_TRACKED_FOREIGN_IDS",
    "MAX_UNSENT_REQUESTS",
    "TRANSPORT_LABEL",
    "ExchangeTransport",
    "OrderRoute",
    "TransportStalled",
    "client_order_id",
]

EventSink = Callable[[CollectorEventKind, str, str, int], None]

TRANSPORT_LABEL = "orders"
"""Stream label on every event this module writes.

Deliberately not a dataset name, for the reason `userstream.USER_STREAM_LABEL` gives: spec
4.5's gap detector matches records to datasets by this field, and a label naming a real
dataset would hand that dataset an alibi for a gap it did not earn.
"""

CLIENT_ORDER_ID_PREFIX = "pl"
MAX_CLIENT_ORDER_ID_LEN = 36
CLIENT_ORDER_ID_PATTERN = re.compile(r"^[.A-Za-z0-9:/_-]{1,36}$")
"""Binance's own restriction on `newClientOrderId`, as published.

Read from the documentation rather than measured against the exchange, which is the same
caveat the module docstring carries. It is applied to the ids this module *mints*, so the
check is a guard against a run id or a sequence number that could not have been intended --
not a filter for strategy input, which never reaches this field at all.
"""

EXCHANGE_CLOSE_PREFIXES = ("autoclose-", "adl_autoclose")
"""Client order ids the exchange mints for itself.

A liquidation or an ADL close arrives on the user-data stream as an ordinary
`ORDER_TRADE_UPDATE` carrying an id no session issued. Counting those as "somebody else is
trading this account" would be wrong in a way that matters: the account was not touched by a
third party, it was force-closed, and the two need different responses from an operator.
"""

DRAIN_INTERVAL_S = 0.01
"""How often `run` looks for queued requests.

Well under any latency the network can produce, so the queue contributes nothing measurable
to the submit-to-ack figure the empirical latency model is built from (spec 6.3).
"""

MAX_UNSENT_REQUESTS = 64
"""Queued requests past which `place`/`cancel` refuse rather than queue another.

The failure this exists for is a session that constructed a transport and never scheduled
`run`. Orders would then be accepted by the engine, entered in its ledger as working, and
never sent -- a run that believes it is in the market and is not, which is the worst
available outcome and one that nothing else in the system would notice. Sixty-four is far
above any legitimate burst (spec 7's `max_orders_per_minute` defaults to 30) and far below
a number that could be mistaken for normal.
"""

MAX_LATENCY_SAMPLES = 10_000
"""Ceiling on retained latency measurements, keeping the most recent.

Most recent rather than first, which is the opposite of `RiskEngine`'s breach cap and for
the opposite reason: a breach list is read to find out what started an incident, and a
latency pool is read to model the network *as it is now*. Over 48 hours the link's
behaviour drifts, so the oldest samples are the least representative of the next order.
"""

MAX_TRACKED_FOREIGN_IDS = 50
"""How many distinct unrecognised client order ids are kept as examples.

The count is unbounded and exact; the examples are capped, because the pathological case
here is an account being traded by another process at its own rate for two days.
"""

ROUTE_RETENTION_S = 900.0
"""How long a **terminal** route is kept before it drops to the eviction tally.

`_routes` used to grow one entry per order for the life of the session -- an `Order`, a
trade-id set, two dict slots -- and a 48-hour session quoting every few seconds carries
tens of thousands of them, every one walked by `reconcile_open_orders` each pass and by
`summary()` on every monitor poll. The retention rule (`_evict_expired`): a route is
evictable only when its order is terminal, its outcome is known, and no reconciliation
sweep still wants it -- **an open or unresolved route is never evicted, at any age** --
and it then survives a further fifteen minutes, which is generous against every late
path that can still name it (a duplicate stream frame arrives in seconds; an absence is
escalated after two 60 s reconciliation passes). A report for an evicted id is still
recognisably ours by its `pl{run_id}-` prefix, so it books as a duplicate rather than
being miscounted as foreign flow.
"""

MAX_ROUTES_BEFORE_SWEEP = 4_096
"""Route-table size at which `place` runs the eviction sweep itself.

The sweep ordinarily rides the reconciler's minute cadence; this is the bound for a
session whose reconciler has stalled while its strategy keeps quoting, so the table's
size cannot silently become a function of how long the reconciler has been away.
"""


class TransportStalled(RuntimeError):
    """Requests are queuing and nothing is sending them. See `MAX_UNSENT_REQUESTS`."""


def client_order_id(run_id: int, order_seq: int) -> str:
    """The platform's identity for one order: `pl{run_id}-{order_seq}`.

    Both components are the platform's own integers, so the result is unique within a run
    and traceable to it afterwards -- which is what makes `GET /fapi/v1/openOrders` and
    `GET /fapi/v1/userTrades` able to answer "did this order exist?" after a timeout, and
    what makes a repeat idempotent at the exchange rather than merely unlikely to be needed
    (see `exchange.signed`).

    Refuses rather than truncates. A truncated id is a *valid* id for a different order, so
    the failure would be two orders sharing one handle at exactly the moment reconciliation
    needs to tell them apart.
    """
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id < 0:
        raise ValueError(
            f"run_id must be a non-negative int, got {run_id!r}. It becomes part of the "
            "client order id the exchange stores, and an id built from an arbitrary object "
            "is one reconciliation cannot look an order up by."
        )
    if not isinstance(order_seq, int) or isinstance(order_seq, bool) or order_seq < 0:
        raise ValueError(f"order_seq must be a non-negative int, got {order_seq!r}")
    candidate = f"{CLIENT_ORDER_ID_PREFIX}{run_id}-{order_seq}"
    if not CLIENT_ORDER_ID_PATTERN.match(candidate):
        raise ValueError(
            f"{candidate!r} is not a client order id Binance would accept: at most "
            f"{MAX_CLIENT_ORDER_ID_LEN} characters of [.A-Za-z0-9:/_-]. Refusing to "
            "truncate it, because a truncated id is a valid id for a different order."
        )
    return candidate


def engine_order_seq(engine_order_id: str) -> int:
    """Recover the engine's own counter from its order id.

    `BacktestEngine._submit` names orders `o{order_seq}`, and reusing that number rather
    than counting separately is what keeps `pl7-12` and engine order `o12` obviously the
    same order in a run log. A separate counter would drift the first time an order was
    refused by the risk layer before reaching a transport, and the drift would be silent.

    Refuses on anything else, because a guessed sequence would produce a plausible client
    order id belonging to no order.
    """
    if engine_order_id.startswith("o") and engine_order_id[1:].isdigit():
        return int(engine_order_id[1:])
    raise ValueError(
        f"cannot read an order sequence out of engine order id {engine_order_id!r}; "
        "expected the 'o{n}' form BacktestEngine._submit mints. The client order id has to "
        "be derivable from it, so this refuses rather than inventing a number."
    )


@dataclass(slots=True)
class OrderRoute:
    """One order's exchange identity, and the clocks kept against it.

    Mutable for the same reason `Order` is: this tracks a lifecycle, and a frozen record
    replaced in a dict on every transition makes the dict and the record two things that can
    disagree.
    """

    client_order_id: str
    order: Order
    tag: str | None
    """The strategy's own label, carried beside the id rather than inside it.

    See the module docstring: a strategy-supplied string in `newClientOrderId` is an
    unvalidated string on the wire, and its rejection would feed spec 7's kill switch.
    """

    submitted_at: float
    """`time.monotonic()` when `place` queued the POST -- the start of the submit latency."""

    acked_at: float | None = None
    exchange_order_id: int | None = None
    cancel_sent_at: float | None = None
    cancel_acked_at: float | None = None
    outcome_unknown: str = ""
    """Why this order's outcome is unknown, or empty. Never cleared by a guess: it is
    cleared only when the exchange itself reports the order."""

    booked_scaled: int = 0
    """Cumulative filled quantity already applied to the ledger, scaled.

    Compared against the exchange's own `z` on every trade report, which is what makes a
    redelivered frame detectable. Without it a repeat of one `ORDER_TRADE_UPDATE` would book
    the same fill twice, and the ledger would carry a position the account does not hold.
    """

    booked_trade_ids: set[int] = field(default_factory=set)
    """Trade ids (`t`) already booked on this order -- the duplicate guard that works even
    when `z` does not (M13). Bounded by the number of executions one order can have."""

    retired_at: float | None = None
    """`time.monotonic()` when an eviction sweep first found this route terminal.

    Stamped lazily by `_evict_expired` rather than at the transition, because the order's
    status is mutated by the engine, which does not know routes exist; the retention clock
    therefore starts at the first sweep after retirement, which only ever errs longer."""


@dataclass(slots=True)
class _Request:
    """One thing to send, queued in the order the strategy asked for it."""

    kind: str
    order: Order
    reason: str = ""
    issued_at: float = 0.0


class ExchangeTransport:
    """`engine.transport.OrderTransport` against Binance USD-M.

    Single use, one per session, and wired in four steps:

    ```
    engine = BacktestEngine(...)                       # builds a SimulatedTransport
    report = await configure_account(client, symbols, leverage=..., margin_mode=...)
    engine.transport = ExchangeTransport(engine, client, run_id=..., on_event=...,
                                         preflight=report)
    stream = UserDataStream(client, engine.transport.on_report, on_event, ...)
    await asyncio.gather(engine.transport.run(stop), stream.run(stop), ...)
    ```

    The transport is assigned after construction because it needs the engine it feeds, which
    is the same ordering `BacktestEngine.__init__` uses to build the simulated one. Nothing
    is sent until `run` (or `drain`) is driven; see `MAX_UNSENT_REQUESTS` for what happens if
    it never is.

    **`preflight` is required, and it is checked rather than stored.** This class is the
    only thing in the platform that sends an order to a real venue, which makes it the only
    place where "the exchange agrees with the ledger" can be enforced instead of hoped for.
    `_check_preflight` refuses construction unless every symbol the engine can trade was
    configured, at the leverage the ledger is using. Passing the report is therefore not a
    formality -- a caller who skips `live.preflight.configure_account` has nothing to pass,
    and a caller who runs it against the wrong symbols or the wrong leverage is told so
    here rather than by a liquidation price that does not match Binance's.
    """

    simulated = False
    """Fills come from the venue, not from the engine's models. The engine's halt path reads
    this to know that cancels and exits are *requests* whose outcomes arrive on the wire,
    never facts it may book locally. See `engine.transport.OrderTransport.simulated`."""

    def __init__(
        self,
        engine: BacktestEngine,
        client: SignedRestClient,
        *,
        run_id: int,
        on_event: EventSink,
        preflight: PreflightReport,
    ) -> None:
        self._engine = engine
        self._client = client
        self._run_id = run_id
        self._on_event = on_event
        self._preflight = preflight
        self._check_preflight(engine, preflight)

        self._queue: deque[_Request] = deque()
        self._drain_lock = asyncio.Lock()
        """Serialises `drain` passes. The FIFO promise is only as good as there being
        exactly one popper on the deque at a time -- see `drain` for the halt-sequence
        interleaving that made two."""
        self._routes: dict[str, OrderRoute] = {}
        """By client order id -- the key every inbound report arrives with. Bounded by
        `ROUTE_RETENTION_S`'s eviction rule; open and unresolved routes are never evicted."""
        self._by_order_id: dict[str, OrderRoute] = {}
        """By engine order id, so `cancel` can find the route without re-deriving the id.
        Evicted in lockstep with `_routes`."""
        self._cid_prefix = f"{CLIENT_ORDER_ID_PREFIX}{run_id}-"
        """What every id this session mints starts with -- how a report for an *evicted*
        route is still recognised as ours rather than counted as foreign flow."""
        self._unknown_cids: set[str] = set()
        """Ids whose outcome is unknown right now, maintained so `unresolved` -- read on
        every monitor poll via `summary()` -- is O(unknown) rather than a scan of every
        route the session ever created."""
        self.routes_evicted = 0
        """Terminal routes dropped by the retention rule. The tombstone count: the ids
        themselves are gone, and the prefix check is what keeps their late reports from
        reading as foreign."""

        self._submit_latencies: deque[int] = deque(maxlen=MAX_LATENCY_SAMPLES)
        self._cancel_latencies: deque[int] = deque(maxlen=MAX_LATENCY_SAMPLES)
        self._last_booked_ts_ms = 0
        self._foreign_ids: list[str] = []
        self._foreign_report_at = 1
        self._closure_report_at = 1

        self.placed = 0
        self.cancels_sent = 0
        self.acks = 0
        self.reports = 0
        self.fills_booked = 0
        self.rejections = 0
        self.unknown_outcomes = 0
        """POSTs that were sent and not answered. Not failures -- see the module docstring."""
        self.unknown_resolved = 0
        """Unknown outcomes later settled by `resolve_unknown_outcomes` -- the count that
        says the mechanism built for them actually ran, as distinct from the label
        `unresolved` quietly meaning `permanent`."""
        self.open_order_mismatches = 0
        """Disagreements `reconcile_open_orders` found between the venue's working set and
        the engine's. Non-zero means the account holds or lacks an order this session's
        book disagrees about -- the state that is invisible to the five account-quantity
        checks until it fills."""
        self._venue_orphans_reported: set[str] = set()
        self._absence_reported: set[str] = set()
        self._open_absent_last_pass: set[str] = set()
        self._absent_unresolved: set[str] = set()
        """Orders open in the engine's book that the venue stopped listing for two
        passes -- queued for the same `GET /fapi/v1/order` resolution the unknown
        outcomes get. This is the H3 case reaching the C10 machinery: a fill that
        happened during a user-stream reconnect gap is never redelivered by Binance, so
        the engine's order rests locally while the venue's is done; the order query
        answers with the snapshot and the fill books through `on_report`."""
        self.foreign_reports = 0
        """Reports for a client order id this session did not issue.

        Non-zero means something other than this session is trading the account, which is
        exactly the state spec 6.7.3's reconciliation exists to catch. Counted separately
        from `exchange_closures`, which is the exchange closing us rather than a third party.
        """
        self.exchange_closures = 0
        self.duplicate_reports = 0
        self.dropped_frames = 0
        """Trade reports whose cumulative quantity implies a frame we never saw.

        The user-data socket carries no sequence number (`exchange.userstream`), so this is
        the only place a dropped execution report is detectable at all.
        """
        self.ignored_reports = 0
        self.cancel_failures = 0

    # -------------------------------------------------------------------- the preflight

    @staticmethod
    def _check_preflight(engine: BacktestEngine, report: PreflightReport) -> None:
        """Refuse to exist unless the exchange was configured for what the ledger believes.

        Three failures, and none is detectable later without a position to compare against:

        - **A symbol the engine can trade that the preflight never configured.** Its
          leverage and margin type are then whatever the account was last set to by hand.
        - **A leverage mismatch.** The ledger sizes positions and solves `P_liq` from
          `Account.leverage(symbol)`. If the exchange is on a different number, every
          margin figure the platform reports is a statement about an account that does not
          exist -- and it is wrong in the direction that matters, because the real
          liquidation arrives before the displayed one.
        - **A position-mode mismatch.** The preflight records what `dualSidePosition` said;
          the ledger records what this run is. If they differ, every order carries -- or
          omits -- a `positionSide` the account will not take.

        Raising here rather than warning is deliberate. The alternative is a session that
        starts, trades, and reports plausible numbers against a misconfigured account,
        which is precisely the class of quiet wrongness the invariants exist to prevent.
        """
        if report.hedge_mode != engine.account.hedge_mode:
            raise ValueError(
                f"the exchange preflight found the account in "
                f"{'hedge' if report.hedge_mode else 'one-way'} mode and this run's ledger "
                f"is in {'hedge' if engine.account.hedge_mode else 'one-way'} mode. Every "
                f"order would name a position side the account cannot accept, and nothing "
                f"downstream can tell the difference between that and an order that simply "
                f"did not fill."
            )
        configured = {c.symbol: c for c in report.symbols}
        missing = [s for s in engine.config.symbols if s not in configured]
        if missing:
            raise ValueError(
                f"the exchange preflight did not configure {sorted(missing)}, but the "
                f"engine is able to trade {sorted(engine.config.symbols)}. Those symbols "
                f"would trade at whatever leverage and margin type the account was last "
                f"set to. Run live.preflight.configure_account over every symbol in the "
                f"run before constructing the transport."
            )
        for symbol in engine.config.symbols:
            ledger = engine.account.leverage(symbol)
            applied = configured[symbol].leverage
            if ledger != applied:
                raise ValueError(
                    f"{symbol}: the ledger is computing margin at {ledger}x and the "
                    f"exchange was configured at {applied}x. Every position on this symbol "
                    f"would be sized and liquidation-priced against a leverage the venue "
                    f"is not using."
                )

    # ------------------------------------------------------------------ OrderTransport

    def place(self, order: Order) -> None:
        """Queue a signed `POST /fapi/v1/order` for an order the engine has accepted.

        The client order id is minted here, synchronously, so that it exists before the
        request does. That ordering is what makes an unanswered POST recoverable: the id is
        already on the route and in the log when the timeout happens, so reconciliation has
        something to look the order up by (`exchange.signed.OrderOutcomeUnknown`).
        """
        # Checked before the route is minted, so a refusal leaves no half-registered order
        # behind for a later report to match against.
        self._check_room()
        if len(self._routes) >= MAX_ROUTES_BEFORE_SWEEP:
            self._evict_expired(time.monotonic())
        cid = client_order_id(self._run_id, engine_order_seq(order.id))
        if cid in self._routes:
            raise ValueError(
                f"client order id {cid!r} has already been used in this run. Two orders "
                "sharing one handle would make an inbound fill report ambiguous, so this "
                "refuses rather than overwriting the earlier route."
            )
        route = OrderRoute(
            client_order_id=cid,
            order=order,
            tag=order.intent.tag,
            submitted_at=time.monotonic(),
        )
        self._routes[cid] = route
        self._by_order_id[order.id] = route
        self._enqueue(_Request("place", order, issued_at=route.submitted_at))

    def cancel(self, order: Order, *, reason: str) -> None:
        """Queue a signed `DELETE /fapi/v1/order`.

        **This does not mark the order cancelled**, and nothing here may. Spec 6.3 and R19:
        a cancel carries its own latency and does not protect against a fill inside that
        window, so the outcome arrives on the user-data stream the same way a fill does. A
        transport that set the status itself would turn a race the strategy can lose into a
        veto it always wins, which is precisely the backtest optimism spec 6.3 names.
        """
        route = self._by_order_id.get(order.id)
        if route is None:
            # The order was never sent, so there is nothing at the exchange to cancel. This
            # is reachable and ordinary: an order refused by the risk layer never reaches a
            # transport, and a strategy may still call `ctx.cancel` on the id it was given.
            return
        self._check_room()
        route.cancel_sent_at = time.monotonic()
        self._enqueue(_Request("cancel", order, reason=reason, issued_at=route.cancel_sent_at))

    def modify(self, order: Order, price: Money | None, qty: Money | None) -> None:
        """Refused. Amending a live order is not implemented, and saying so is the point.

        Binance does publish `PUT /fapi/v1/order`, so this is not an exchange limitation. It
        is a seam limitation, and it is the one spec 6.1 draws. An amendment that lands
        changes an order's working size, its price and its queue position, and the rules for
        all three live in `BacktestEngine._apply_modify` -- including the two refusals that
        make an amend honest: an amendment that overtook the order it amends, and one whose
        new size is already behind the fills. A transport cannot reach any of that without
        re-implementing it on the wrong side of the seam, and an amend that silently left
        `order.remaining` describing the pre-amendment size would misprice every subsequent
        fill on that order.

        The other candidate -- quietly sending a cancel and a fresh order -- is worse, and
        `EngineRuntime.modify` already says why: cancel-and-replace *always* loses queue
        position, so a strategy whose reprice became one would be quoting from the back of
        the book while believing it had held its place. Queue position is the most valuable
        thing a maker strategy owns.

        So this raises, loudly, at the call site, with the alternative named. A strategy that
        needs to reprice in a live session should cancel and re-submit explicitly, and pay
        the queue position where it can see itself paying it.
        """
        raise UnsupportedOrder(
            f"{order.id}: amending a working order is not implemented for a live or testnet "
            "session. The engine's amend rules -- queue priority, the overtaken-amendment "
            "refusal, the already-filled-past-the-new-size refusal -- are ledger-side, and "
            "a transport that reproduced them would be a second copy of them. Cancel the "
            "order and submit a new one, accepting that this loses queue position; that is "
            "what the exchange would make you do for anything but a LIMIT order anyway."
        )

    # --------------------------------------------------------------------- the sender

    async def run(self, stop: asyncio.Event) -> None:
        """Send queued requests, in order, until `stop`.

        The session owns this task. Requests go out one at a time on purpose -- see the
        module docstring on `RateBudget` -- and a failure on one request never stops the
        loop, because a transport that died would leave the engine accepting orders that
        nothing sends. Every failure is instead routed to the order it belongs to.
        """
        while not stop.is_set():
            await self.drain()
            if await _sleep_unless_stopped(stop, DRAIN_INTERVAL_S):
                return

    async def drain(self) -> int:
        """Send everything queued right now. Returns how many requests went out.

        Separate from `run` so that a caller can flush deterministically -- which is what
        the tests do, and what a shutdown path wants before it stops caring about the loop.

        **One drainer at a time, enforced with a lock rather than assumed.** The FIFO
        promise this module's docstring makes -- *"requests are queued and sent in
        order"* -- held only while `run` was the sole caller. The end sequence broke
        that: the session keeps `run` alive through the settle phases (venue cancels and
        real exits need a live wire) while `_settle_until` calls this method directly,
        so two coroutines ran `while queue: send(popleft())` over one deque. A halt that
        queued `place(exit)` then `cancel(resting)` could have the cancel popped and
        sent by one drainer while the other was still awaiting the place's POST -- the
        cancel lands first, its -2011 reads as "already gone", and then the place lands
        and rests at the venue *believed cancelled*. The lock covers the whole pass, so
        an overlapping call waits and then finds whatever the first pass left.
        """
        sent = 0
        async with self._drain_lock:
            while self._queue:
                await self._send(self._queue.popleft())
                sent += 1
        return sent

    async def cancel_all_at_exchange(self, symbol: str) -> bool:
        """`DELETE /fapi/v1/allOpenOrders` -- step 2 of spec 7's kill switch.

        Not part of `OrderTransport`, and deliberately not driven from the engine's halt
        path: `BacktestEngine._cancel_all` removes orders from its *own* book without going
        through a transport when the removal is engine-initiated, which is right for a
        simulated venue and leaves the real ones working. The session's halt path calls this.

        Returns whether the request was accepted. `cancelled_or_already_gone` is not used
        because this is the bulk endpoint and has no per-order "unknown order" answer.
        """
        try:
            await self._client.cancel_all(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a halt must report, never raise
            self._event(
                CollectorEventKind.STALE,
                f"cancel-all on {symbol} failed during a halt: {type(exc).__name__}: {exc}. "
                "Orders may still be working at the exchange -- cancel them by hand.",
            )
            return False
        return True

    # ------------------------------------------------------- reconciliation assistance

    async def resolve_unknown_outcomes(self) -> int:
        """Ask the venue what became of every POST it never answered (C10).

        `signed.py` promises that an `OrderOutcomeUnknown` is "settled by reconciliation,
        never by assumption" -- and for one release nothing settled it: the reconciler
        compared five account quantities, none of which move for a resting limit, so an
        orphan produced by a read timeout could work at the venue for the whole session
        with `unresolved` rendered in the UI and read by no code. This is the settling.
        `GET /fapi/v1/order` distinguishes the three possible truths exactly: -2013 means
        the request never landed (the engine's order is retired, loudly -- there is
        nothing at the venue to double-fill against); a payload means it landed, and the
        order's current state is replayed through `on_report`, the same path a stream
        frame takes, so an ack, a fill or a cancel books identically to one that arrived
        on the socket. Any other error keeps the id unknown for the next pass -- unknown
        is a fact, and only an answer from the venue may clear it.

        Called by the reconciler each pass, which bounds how long "unknown" can last at
        one reconciliation interval.
        """
        resolved = 0
        # The union is the H3 case joining the C10 one: an *absent* order (open here,
        # unlisted at the venue for two passes) is most often a fill that happened during
        # a user-stream reconnect gap -- Binance does not redeliver ORDER_TRADE_UPDATE
        # from before a reconnect, so waiting for the frame means waiting forever while
        # the engine believes it is flat and sizes a second entry against exposure it
        # already has. The order query answers both questions the same way.
        pending = sorted(set(self.unresolved) | self._absent_unresolved)
        self._absent_unresolved.clear()
        for cid in pending:
            route = self._routes.get(cid)
            if route is None:  # pragma: no cover - both sets are derived from _routes
                continue
            symbol = route.order.intent.symbol
            try:
                payload = await self._client.get_order(symbol, client_order_id=cid)
            except asyncio.CancelledError:
                raise
            except BinanceRestError as exc:
                if exc.code in (UNKNOWN_ORDER_CODE, ORDER_DOES_NOT_EXIST_CODE):
                    route.outcome_unknown = ""
                    self._unknown_cids.discard(cid)
                    self.unknown_resolved += 1
                    resolved += 1
                    if route.order.is_open:
                        self._engine._remove(
                            route.order,
                            OrderStatus.EXPIRED,
                            "resolved by reconciliation: the venue has no record of "
                            "this order, so the request that timed out never landed",
                        )
                    self._event(
                        CollectorEventKind.RECONNECT,
                        f"{cid}: unknown outcome resolved -- the venue has no record; "
                        f"the request never landed and the order was retired",
                    )
                continue
            except Exception:  # noqa: BLE001 - transient; the next pass retries
                continue

            report = _report_from_rest_order(payload)
            if report is None:
                self._event(
                    CollectorEventKind.STALE,
                    f"{cid}: the venue answered the order query with a payload this "
                    f"module cannot read; the outcome stays unknown",
                )
                continue
            self.unknown_resolved += 1
            resolved += 1
            self._event(
                CollectorEventKind.RECONNECT,
                f"{cid}: unknown outcome resolved by query -- the venue reports "
                f"status {report.status!r} with "
                f"{money_to_str(from_scaled(report.cum_filled_qty))} filled",
            )
            self.on_report(report)
            # A REST snapshot is one report standing for a whole history: an order that
            # part-filled and was then cancelled needs both halves booked, and `_apply`
            # dispatches on exactly one of them (the fill, because `reason == "TRADE"`
            # takes precedence). The terminal half is applied here.
            if (
                report.status in ("CANCELED", "EXPIRED", "REJECTED")
                and route.order.is_open
            ):
                self._engine._remove(
                    route.order,
                    OrderStatus.CANCELLED
                    if report.status == "CANCELED"
                    else OrderStatus.EXPIRED,
                    f"resolved by reconciliation: the venue reports this order "
                    f"{report.status}",
                )
        return resolved

    def reconcile_open_orders(
        self, venue: Mapping[str, Sequence[Mapping[str, Any]]]
    ) -> None:
        """Compare the venue's working-order set against the engine's book (H5).

        The five account-quantity checks cannot see a *resting* order: it moves no
        balance and no position until it fills, which is precisely when discovering it
        stops being cheap. So each pass also diffs `GET /fapi/v1/openOrders` against the
        engine's open orders, in both directions:

        - **A venue order this session never issued** is foreign flow -- another process,
          a hand-placed order, a crashed predecessor -- reported once per id rather than
          once per pass, because the operator who has read it does not need it again.
        - **A venue order the engine believes terminal** is the C11 shape surviving by
          some other route: the book the strategy reasons over disagrees with the book
          the venue will fill from.
        - **An engine order the venue does not hold** alarms only on the second
          consecutive pass it is absent: a fill or cancel ack can legitimately be in
          flight during one, and a one-pass grace costs sixty seconds of silence while a
          zero-pass grace cries wolf on every fast fill.

        Reported and counted rather than halted on: the honest cause is not knowable
        from here (a missed frame, a foreign trader, a race), and spec 6.7.3's halt is
        reserved for quantities whose disagreement is unambiguous. The count is on the
        monitor; an operator watching a live session sees it move.
        """
        self._evict_expired(time.monotonic())
        venue_cids: set[str] = set()
        for symbol, rows in venue.items():
            for row in rows:
                cid = str(row.get("clientOrderId", "") or "")
                if not cid:
                    continue
                venue_cids.add(cid)
                route = self._routes.get(cid)
                if route is None:
                    if cid not in self._venue_orphans_reported:
                        self._venue_orphans_reported.add(cid)
                        self.open_order_mismatches += 1
                        if cid.startswith(self._cid_prefix):
                            # Ours, evicted: the engine finished with this order at least
                            # a retention window ago and the venue still lists it working
                            # -- the C11 shape, reported in its own words rather than as
                            # a third party's order.
                            self._event(
                                CollectorEventKind.STALE,
                                f"{symbol}: the venue reports order {cid} working, but "
                                f"this session finished with it more than "
                                f"{ROUTE_RETENTION_S:g}s ago. Its fill would arrive "
                                f"against an order the ledger has closed. Cancel it at "
                                f"the exchange.",
                            )
                        else:
                            self._event(
                                CollectorEventKind.STALE,
                                f"{symbol}: the venue holds a working order this session "
                                f"never issued (clientOrderId {cid!r}) -- something else "
                                f"is trading this account, or a predecessor session left "
                                f"it behind. Its fill will arrive as foreign flow.",
                            )
                    continue
                if not route.order.is_open and not route.outcome_unknown:
                    if cid not in self._venue_orphans_reported:
                        self._venue_orphans_reported.add(cid)
                        self.open_order_mismatches += 1
                        self._event(
                            CollectorEventKind.STALE,
                            f"{symbol}: the engine's book has order {cid} as "
                            f"{route.order.status.value} but the venue reports it "
                            f"working -- its fill would arrive against an order the "
                            f"ledger has finished with. Cancel it at the exchange.",
                        )

        absent = {
            cid
            for cid, route in self._routes.items()
            if route.order.is_open
            and route.acked_at is not None
            and not route.outcome_unknown
            and cid not in venue_cids
        }
        for cid in (absent & self._open_absent_last_pass) - self._absence_reported:
            self._absence_reported.add(cid)
            self.open_order_mismatches += 1
            # Queued for the order query on the next resolution sweep, not merely
            # reported: the missing report is usually a fill from a reconnect gap, and
            # the query can book it (H3) where a warning could only describe it.
            self._absent_unresolved.add(cid)
            route = self._routes[cid]
            self._event(
                CollectorEventKind.STALE,
                f"{route.order.intent.symbol}: order {cid} is open in the engine's "
                f"book but the venue has not listed it for two passes -- a fill or "
                f"cancel report was likely missed (a user-data reconnect does not "
                f"redeliver). Queued for direct resolution by order query.",
            )
        self._open_absent_last_pass = absent
        # **The once-per-id report guards are pruned to the condition they guard.** Both
        # sets grew one entry per distinct id for the life of the session -- unbounded
        # under a foreign process trading the account for two days. An orphan entry only
        # matters while the venue still lists the order, and an absence entry only while
        # the order is still absent; once the condition clears, the entry's job is done,
        # and a recurrence is a *new* occurrence that deserves a new report.
        self._venue_orphans_reported &= venue_cids
        self._absence_reported &= absent

    # ---------------------------------------------------------------------- inbound

    def on_report(self, report: ExchangeReport) -> None:
        """One `ExchangeReport` from the user-data stream, routed by client order id.

        Runs on the socket read path (`UserDataStream` calls its sink inline), so it does no
        I/O and never awaits. Everything it does is a dictionary lookup and a call into the
        engine's own booking code.

        **Every report is accounted for.** One that names an id this session did not issue is
        counted and reported rather than dropped: it means the account is being traded by
        something else -- another process, a hand-placed order in the Binance app, or the
        exchange itself closing a position -- and each of those makes PerpLab's ledger a
        description of an account that no longer exists. That is the exact state spec 6.7.3's
        reconciliation exists to catch, and a silent drop here would remove the fastest
        evidence of it.
        """
        if report.kind is not ReportKind.ORDER:
            # Account, config, margin-call and listen-key events are the session's business,
            # not an order's. Counted so that "the transport saw nothing" and "the transport
            # saw nothing it owned" stay distinguishable.
            self.ignored_reports += 1
            return

        route = self._routes.get(report.client_order_id)
        if route is None:
            if report.client_order_id.startswith(self._cid_prefix):
                # One of ours, but its route was evicted -- terminal and quiet for longer
                # than the retention window, so anything arriving now is a redelivered
                # frame for an order the ledger finished with. Counted where a late frame
                # for a *retained* terminal route is counted, never as foreign flow.
                self.duplicate_reports += 1
                return
            self._note_foreign_report(report)
            return

        self.reports += 1
        route.outcome_unknown = ""
        self._unknown_cids.discard(report.client_order_id)
        self._apply(route, report)

    def _apply(self, route: OrderRoute, report: ExchangeReport) -> None:
        """Dispatch one order report onto the engine's own lifecycle methods."""
        order = route.order

        if report.reason == "TRADE" and report.last_filled_qty > 0:
            self._book(route, report)
            return

        status = report.status
        if status == "NEW":
            self._note_ack(route, exchange_order_id=report.order_id)
            return
        if status == "CANCELED":
            self._note_cancel_ack(route)
            if order.is_open:
                self._engine._remove(
                    order, OrderStatus.CANCELLED, "cancelled at the exchange"
                )
            return
        if status == "EXPIRED":
            if order.is_open:
                # `EXPIRED`, not `CANCELLED`, and the difference is spec 6.5's named hazard:
                # a post-only order that would have crossed is retired by the exchange, and
                # counting it as a cancel the strategy asked for is how it stays silent.
                self._engine._remove(
                    order,
                    OrderStatus.EXPIRED,
                    f"expired at the exchange ({report.reason or 'no execution type'}); "
                    f"time in force was {order.intent.tif.value}",
                )
            return
        if status in ("REJECTED", "EXPIRED_IN_MATCH"):
            if order.is_open:
                self._refuse(order, f"rejected at the exchange ({report.reason or status})")
            return

        # NEW_INSURANCE and NEW_ADL name an exchange-initiated close, and they ordinarily
        # arrive under an `autoclose-` id that never reaches this method. Reaching it under
        # one of *our* ids would mean the exchange is force-closing an order we submitted,
        # which is worth an event rather than a silent branch.
        self._event(
            CollectorEventKind.STALE,
            f"{route.client_order_id}: unhandled order status {status!r} "
            f"(execution type {report.reason!r}); nothing was booked",
        )

    def _book(self, route: OrderRoute, report: ExchangeReport) -> None:
        """Route one execution into `BacktestEngine._book_fill` -- the shared ledger path.

        **Duplicate and dropped frames are both detectable from `z`.** The exchange sends the
        cumulative filled quantity on every execution report, so comparing it against what
        has already been booked distinguishes a redelivered frame (cumulative has not moved)
        from a missed one (cumulative jumped by more than this report's increment). Neither
        is hypothetical on a socket with no sequence number.

        A missed frame is booked at *this* report's price, because that is the only price
        available, and reported loudly. It is not silently smoothed: the quantity is right,
        so spec 6.7.3's position-size check will pass and the entry-price check is the one
        that will fire -- which is the correct outcome, since the entry price genuinely is
        uncertain until the exchange is asked.
        """
        order = route.order
        if not order.is_open:
            # The order is already terminal in the engine's book -- a late duplicate, or a
            # report that arrived after a cancel was booked. Booking against it would apply
            # a fill to an order the ledger has finished with.
            self.duplicate_reports += 1
            return

        cumulative = report.cum_filled_qty
        if cumulative and cumulative <= route.booked_scaled:
            self.duplicate_reports += 1
            return
        # **The trade id is the second lock on the same door** (M13). The cumulative
        # check above is disabled exactly when `z` is absent or zero -- and this module
        # has never seen a real Binance response, so "z is always present" is an
        # assumption, not a fact. Every TRADE execution carries its own trade id (`t`),
        # ids are unique per symbol, and a redelivered frame redelivers the same one;
        # with both guards a duplicate must defeat two independent fields to book twice.
        trade_id = _report_trade_id(report)
        if trade_id is not None:
            if trade_id in route.booked_trade_ids:
                self.duplicate_reports += 1
                return
            route.booked_trade_ids.add(trade_id)

        increment = report.last_filled_qty
        if cumulative:
            expected = cumulative - route.booked_scaled
            if expected != increment:
                self.dropped_frames += 1
                self._event(
                    CollectorEventKind.STALE,
                    f"{route.client_order_id}: the exchange reports "
                    f"{money_to_str(from_scaled(cumulative))} filled cumulatively but this "
                    f"report's increment is {money_to_str(from_scaled(increment))} against "
                    f"{money_to_str(from_scaled(route.booked_scaled))} already booked -- an "
                    "execution report was dropped. Booking the difference at this report's "
                    "price; the entry price is uncertain until reconciliation checks it.",
                )
                increment = expected
            route.booked_scaled = cumulative
        else:
            route.booked_scaled += increment

        # **The venue's own commission, booked verbatim when it is bookable** (spec 3.8 /
        # H6). The report says what this execution actually cost; the fee schedule is a
        # model of it, and the platform's first live round trip halted on exactly the
        # difference. Three conditions gate the override, each with a reason: the asset
        # must be USDT (a BNB-discounted fee is real but not in the ledger's currency --
        # the model stands in and the wallet check will measure the discount), the
        # commission must be non-zero (zero is also `parse_user_frame`'s default for an
        # absent field, and a *synthesized* snapshot report genuinely has none), and the
        # increment must be the report's own (a dropped-frame difference is being booked
        # at a substitute price, so charging it this report's exact fee would pair a
        # modelled quantity with a measured cost).
        venue_fee: Money | None = None
        if (
            report.commission > 0
            and report.commission_asset == "USDT"
            and increment == report.last_filled_qty
        ):
            venue_fee = from_scaled(report.commission)

        ts_ms = self._fill_ts(report.ts_ms)
        fills_before = self._engine.counts["fills"]
        self._engine._book_fill(
            ts_ms,
            order,
            from_scaled(increment),
            from_scaled(report.last_filled_price),
            is_maker=report.is_maker,
            fee=venue_fee,
        )
        if self._engine.counts["fills"] == fills_before:
            # The ledger refused to book a fill the venue says happened -- a price off the
            # tick grid (a misread report), or a margin model disagreeing with the venue
            # that granted the execution. Whichever it is, the venue's account has moved
            # and ours has not, which is the exact state spec 6.7.3 exists to catch --
            # said here, loudly, at the moment it opens, rather than discovered as a
            # position mismatch a minute later with nothing to explain it.
            self._event(
                CollectorEventKind.STALE,
                f"{route.client_order_id}: the venue reports "
                f"{money_to_str(from_scaled(increment))} filled at "
                f"{money_to_str(from_scaled(report.last_filled_price))} and the ledger "
                f"refused to book it. The account at the exchange has moved and the "
                f"ledger has not; reconciliation will halt on the difference.",
            )
            self._engine.warnings.append(
                f"a venue execution on {route.client_order_id} could not be booked into "
                f"the ledger; the run's numbers from this point describe an account that "
                f"is no longer the one being held. See the orders feed for the report."
            )
            return
        self.fills_booked += 1

    def _note_foreign_report(self, report: ExchangeReport) -> None:
        """An execution report for an id this session did not issue."""
        cid = report.client_order_id
        if cid.startswith(EXCHANGE_CLOSE_PREFIXES):
            self.exchange_closures += 1
            if self.exchange_closures >= self._closure_report_at:
                self._closure_report_at *= 10
                self._event(
                    CollectorEventKind.STALE,
                    f"{self.exchange_closures} exchange-initiated close report(s); most "
                    f"recent {cid!r} on {report.symbol} status {report.status!r}. The "
                    "exchange liquidated or ADL-closed a position -- PerpLab did not send "
                    "this order and its ledger does not know about the close.",
                )
            return

        self.foreign_reports += 1
        if len(self._foreign_ids) < MAX_TRACKED_FOREIGN_IDS and cid not in self._foreign_ids:
            self._foreign_ids.append(cid)
        if self.foreign_reports < self._foreign_report_at:
            return
        # The 1, 10, 100 escalation `ws._note_unrecognised` uses, for its reason: a foreign
        # order flow arrives at whatever rate its own source produces, and an event per
        # occurrence buries the fault inside its own description.
        self._foreign_report_at *= 10
        self._event(
            CollectorEventKind.STALE,
            f"{self.foreign_reports} execution report(s) for client order id(s) this "
            f"session never issued; most recent {cid!r} on {report.symbol} status "
            f"{report.status!r}. Something other than this session is trading the account, "
            "so PerpLab's ledger describes a position that is not the one being held. "
            "Spec 6.7.3's reconciliation is the check that will stop the session.",
        )

    # ---------------------------------------------------------------------- sending

    def _check_room(self) -> None:
        if len(self._queue) >= MAX_UNSENT_REQUESTS:
            raise TransportStalled(
                f"{len(self._queue)} order requests are queued and unsent, at the "
                f"{MAX_UNSENT_REQUESTS} ceiling. Either the session never scheduled "
                "ExchangeTransport.run, or the signed client is not returning. Refusing to "
                "queue more: the engine's ledger already counts these orders as working, "
                "and a run that believes it is in the market and is not is the worst state "
                "this transport can be in."
            )

    def _enqueue(self, request: _Request) -> None:
        self._queue.append(request)

    async def _send(self, request: _Request) -> None:
        if request.kind == "place":
            await self._send_place(request)
        else:
            await self._send_cancel(request)

    async def _send_place(self, request: _Request) -> None:
        order = request.order
        route = self._by_order_id.get(order.id)
        if route is None:  # pragma: no cover - place() minted the route moments ago
            # Reachable only if the retention sweep evicted the route while the request
            # sat in a stalled queue, which requires the order to have been terminal for
            # the whole retention window -- a request nothing should send anyway.
            return
        try:
            params = _order_params(order, route.client_order_id)
        except ValueError as exc:
            # The intent cannot be expressed as a Binance order at all -- a LIMIT with no
            # price, a trailing stop with no callback rate. Refused locally rather than sent
            # for the exchange to refuse, because the exchange's answer would be a `-1102`
            # naming a parameter the strategy author never wrote.
            self._refuse(order, f"unsendable order: {exc}")
            return

        self.placed += 1
        try:
            payload = await self._client.new_order(**params)
        except asyncio.CancelledError:
            # **Marked unknown before the cancellation propagates** (M14). A shutdown that
            # cancels this task mid-POST is the one class of unknown outcome that used to
            # escape the mechanism built for it: the request may be on the wire, the task
            # dies before any answer, and nothing recorded that the order might exist.
            # The end sequence reads `unresolved` to decide what the final reconciliation
            # must settle, and an unrecorded maybe-order is invisible to it.
            self._note_unknown(
                route,
                "the request was cancelled mid-flight at shutdown; it may have reached "
                "the exchange",
            )
            raise
        except OrderOutcomeUnknown as exc:
            self._note_unknown(route, str(exc))
            return
        except RateBudgetExceeded as exc:
            # Nothing was sent, so this is an ordinary refusal and the order does not exist.
            # It does feed `observe_rejection`, which is correct: a session hitting the
            # exchange's own order-count ceiling is submitting faster than spec 7's
            # `max_orders_per_minute` should have allowed, and repeating it is how an IP ban
            # happens.
            self._refuse(order, f"RATE_BUDGET: {exc}")
            return
        except BinanceRestError as exc:
            self._refuse(order, _rejection_reason(exc))
            return
        except Exception as exc:  # noqa: BLE001 - see below
            # Anything unanticipated on an order path is an *unknown*, never a rejection.
            # `SignedRestClient` already converts every ambiguous transport failure into
            # `OrderOutcomeUnknown`, so reaching here means something outside its model went
            # wrong -- and the safe reading of "I do not know what this exception means" on a
            # POST that may have arrived is exactly the reading `OrderOutcomeUnknown` has.
            self._note_unknown(route, f"{type(exc).__name__}: {exc}")
            return

        self._note_ack(route, exchange_order_id=_payload_order_id(payload))

    async def _send_cancel(self, request: _Request) -> None:
        order = request.order
        route = self._by_order_id.get(order.id)
        if route is None:  # pragma: no cover - cancel() checks the route before queueing
            # As in `_send_place`: only a retention eviction during a stalled queue can
            # get here, and an order terminal that long has nothing left to cancel.
            return
        self.cancels_sent += 1
        try:
            await self._client.cancel_order(
                order.intent.symbol, client_order_id=route.client_order_id
            )
        except asyncio.CancelledError:
            raise
        except BinanceRestError as exc:
            if exc.code == UNKNOWN_ORDER_CODE:
                # The expected answer to a cancel that already worked, or to one for an
                # order that filled first. Not a failure -- `exchange.signed` documents this
                # -- and not a state change either: whatever ended the order reported itself
                # on the user-data stream.
                self._note_cancel_ack(route)
                return
            self.cancel_failures += 1
            self._event(
                CollectorEventKind.STALE,
                f"cancel of {route.client_order_id} ({request.reason}) failed: "
                f"{_rejection_reason(exc)}. The order may still be working at the exchange.",
            )
            return
        except Exception as exc:  # noqa: BLE001 - a cancel failure must not end the session
            self.cancel_failures += 1
            self._event(
                CollectorEventKind.STALE,
                f"cancel of {route.client_order_id} did not complete: "
                f"{type(exc).__name__}: {exc}. The order may still be working.",
            )
            return

        # **Acknowledged, not cancelled.** See `cancel`.
        self._note_cancel_ack(route)

    # --------------------------------------------------------------------- outcomes

    def _note_ack(self, route: OrderRoute, *, exchange_order_id: int | None) -> None:
        """The exchange has the order. Records the latency and clears the streak.

        Idempotent, and it has to be: the POST response and the stream's `NEW` report say the
        same thing and either can arrive first. A second ack must not restate the latency,
        because by then the order was already known to be there. Whichever arrives first is
        what the latency sample measures, which is the honest definition of "the order
        reached the matching engine" (spec 6.3) from this side of the wire -- the two paths
        differ by the socket's own delivery time and neither is more authoritative.
        """
        if route.acked_at is None:
            route.acked_at = time.monotonic()
            observed_ms = _elapsed_ms(route.submitted_at, route.acked_at)
            self._submit_latencies.append(observed_ms)
            self.acks += 1
            # **Written back onto the order, replacing the modelled value.**
            # `PaperSession.latency_samples` derives the empirical pool for spec 6.3 from
            # `arrival_ts - submit_ts` on the engine's own order records, so a transport that
            # kept its measurements to itself would leave the shadow replaying a lognormal
            # while real numbers sat one object away. `_submit` stamped `arrival_ts` from
            # `config.latency`, which is a *model* and means nothing once a real exchange is
            # answering -- keeping the larger of the two would report the model whenever the
            # network beat it, which is precisely the case the model is worst at.
            route.order.arrival_ts = route.order.submit_ts + observed_ms
        if exchange_order_id is not None:
            route.exchange_order_id = exchange_order_id
        if route.order.status is OrderStatus.PENDING:
            # Guarded, because the user-data stream can beat the POST response: a market
            # order can be filled and booked before its own HTTP answer arrives, and
            # promoting a FILLED order back to WORKING would put a finished order back in
            # `ctx.open_orders()` forever.
            route.order.status = OrderStatus.WORKING
        if route.order.status is not OrderStatus.REJECTED:
            # Spec 7's rejection streak is over: an order reached the exchange without being
            # refused. Read from the order's own status rather than from which path got here,
            # which is the rule `BacktestEngine._on_arrival` applies at the same point in the
            # lifecycle and for the same reason -- a streak counter that misses one way of
            # being refused is a kill switch that does not fire.
            self._engine.risk.observe_acceptance()

    def _note_cancel_ack(self, route: OrderRoute) -> None:
        if route.cancel_sent_at is None or route.cancel_acked_at is not None:
            return
        route.cancel_acked_at = time.monotonic()
        self._cancel_latencies.append(
            _elapsed_ms(route.cancel_sent_at, route.cancel_acked_at)
        )

    def _note_unknown(self, route: OrderRoute, detail: str) -> None:
        """A POST that was sent and not answered. Recorded, surfaced, and left alone.

        The order's status is **not** touched. It stays `PENDING`, which is the truthful
        encoding -- the engine has not been told the order is working and has not been told
        it is dead -- and it stays that way until the exchange itself says otherwise, on the
        user-data stream or through spec 6.7.3's reconciliation pass. Marking it rejected
        here would flatten the engine's ledger while the exchange held a position, and the
        next thing to notice would be the liquidation.
        """
        route.outcome_unknown = detail
        self._unknown_cids.add(route.client_order_id)
        self.unknown_outcomes += 1
        self._event(
            CollectorEventKind.STALE,
            f"{route.client_order_id} on {route.order.intent.symbol}: the order request "
            f"reached the exchange and its outcome is unknown ({detail}). It has NOT been "
            "treated as a rejection and must not be resubmitted. The engine's ledger does "
            "not count it; reconciliation (spec 6.7.3) is what will settle whether it "
            "exists.",
        )

    def _refuse(self, order: Order, reason: str) -> None:
        """Hand a genuine refusal to the engine's own rejection path.

        `_reject` is the shared code: it counts the rejection, raises `ORDERS_REJECTED`,
        notifies `on_cancel`, writes the `REJECT` event and feeds `observe_rejection`. None
        of that is reproduced here, which is the whole point of the seam.
        """
        self.rejections += 1
        self._engine._reject(order, reason)

    # ----------------------------------------------------------------------- reading

    def latency_samples(self) -> dict[str, list[int]]:
        """Observed submit-to-ack and cancel-to-ack times, in milliseconds (spec 6.3).

        The shape `PaperSession.latency_samples` returns, so a session can publish either
        without its consumers caring which. These are *measurements*: the empirical latency
        model exists precisely so that a backtest can be re-run against the network the
        session actually saw rather than against a lognormal somebody picked.
        """
        return {
            "submit": list(self._submit_latencies),
            "cancel": list(self._cancel_latencies),
        }

    @property
    def unresolved(self) -> dict[str, str]:
        """Client order ids whose outcome is still unknown, and why.

        Read by an operator and by anything deciding whether it is safe to stop: an order in
        here may be working at the exchange with nothing in the ledger to say so.

        Built from `_unknown_cids` rather than a scan of every route, because this sits
        behind `summary()` on the monitor's poll cadence and the route table covers the
        whole retention window -- O(unknown), and unknown is almost always zero.
        """
        found: dict[str, str] = {}
        for cid in self._unknown_cids:
            route = self._routes.get(cid)
            if route is not None and route.outcome_unknown:
                found[cid] = route.outcome_unknown
        return found

    @property
    def foreign_client_order_ids(self) -> tuple[str, ...]:
        """Examples of ids this session did not issue. Capped; `foreign_reports` is not."""
        return tuple(self._foreign_ids)

    def route_for(self, client_order_id_value: str) -> OrderRoute | None:
        return self._routes.get(client_order_id_value)

    def summary(self) -> dict[str, Any]:
        """A snapshot for the live monitor (spec 10.3)."""
        return {
            "placed": self.placed,
            "acks": self.acks,
            "cancels_sent": self.cancels_sent,
            "reports": self.reports,
            "fills_booked": self.fills_booked,
            "rejections": self.rejections,
            "unknown_outcomes": self.unknown_outcomes,
            "unknown_resolved": self.unknown_resolved,
            "open_order_mismatches": self.open_order_mismatches,
            "unresolved": sorted(self.unresolved),
            "foreign_reports": self.foreign_reports,
            "foreign_client_order_ids": list(self._foreign_ids),
            "exchange_closures": self.exchange_closures,
            "duplicate_reports": self.duplicate_reports,
            "dropped_frames": self.dropped_frames,
            "cancel_failures": self.cancel_failures,
            "queued": len(self._queue),
            "routes": len(self._routes),
            "routes_evicted": self.routes_evicted,
        }

    # --------------------------------------------------------------------- internals

    def _evict_expired(self, now: float) -> None:
        """Drop routes that have been terminal, known and unwanted for the retention window.

        The retention rule, in full (see `ROUTE_RETENTION_S` for why it exists):

        - the order is **terminal** -- an open order's route is its only link to the wire;
        - its outcome is **known** -- an unresolved route is what `resolve_unknown_outcomes`
          reads, and evicting one would turn "unknown" into "forgotten";
        - no absence sweep has it **queued** -- `_absent_unresolved` names routes the next
          resolution pass must query;
        - and it has been all three for `ROUTE_RETENTION_S`, stamped lazily at the first
          sweep that found it terminal.

        Runs on the reconciler's cadence (`reconcile_open_orders`) and, as a backstop, from
        `place` once the table passes `MAX_ROUTES_BEFORE_SWEEP`.
        """
        expired: list[str] = []
        for cid, route in self._routes.items():
            if route.order.is_open or route.outcome_unknown or cid in self._absent_unresolved:
                route.retired_at = None
                continue
            if route.retired_at is None:
                route.retired_at = now
                continue
            if now - route.retired_at >= ROUTE_RETENTION_S:
                expired.append(cid)
        for cid in expired:
            route = self._routes.pop(cid)
            self._by_order_id.pop(route.order.id, None)
            self._open_absent_last_pass.discard(cid)
            self.routes_evicted += 1

    def _fill_ts(self, report_ts_ms: int) -> int:
        """A booking timestamp that cannot break the ledger's single timeline -- either way.

        Invariant I8 refuses a ledger mutation stamped before one already applied, and it
        raises `InvariantViolation`, which ends a run. A live session has two clocks
        feeding one ledger: market data arriving through a 250 ms reorder window plus
        poller delivery lag, and execution reports arriving straight off the socket. The
        report's own exchange time is therefore *routinely ahead of the market events
        still to be dispatched* -- a fill at E=12:01:00.800 books, and the 12:01:00.000
        mark bar arrives a second later, and `Account._touch` sees time go backwards by
        800 ms on a perfectly healthy session.

        So the stamp is the **engine's own frontier**: the latest instant the ledger has
        already been advanced to, never the report's time. Every future dispatched event
        is at or after this instant by the queue's own ordering, so nothing that follows
        can violate I8; `_last_booked_ts_ms` keeps successive fills monotonic between
        market events. The cost is honest and bounded -- a fill's ledger stamp can lag its
        exchange time by up to the reorder window -- and the alternative was a session
        abandoned mid-position by its own bookkeeping, with the kill switch blaming an
        INVARIANT that the account never breached. (The first version of this method kept
        a genuinely-later report's own timestamp, reasoning that forward clamping was the
        honest direction; it was exactly backwards, because the frontier had not caught up
        yet.)
        """
        ts_ms = max(self._engine.runtime.now_ms, self._last_booked_ts_ms)
        self._last_booked_ts_ms = ts_ms
        return ts_ms

    def _event(self, kind: CollectorEventKind, detail: str) -> None:
        self._on_event(kind, TRANSPORT_LABEL, detail, 0)


def _order_params(order: Order, cid: str) -> dict[str, Any]:
    """The exact query parameters for one `POST /fapi/v1/order`.

    Quantities and prices are handed over as `Decimal`; `SignedRestClient._encode_value`
    renders them with `money_to_str` and *refuses* a float outright, which is the guard that
    keeps `0.07` from becoming 0.07000000000000001 on the wire.

    The quantity is `order.remaining`, which the engine already quantised to `stepSize` at
    submission. Prices are sent exactly as the strategy wrote them and are **not** quantised
    here: the engine validates them instead -- `_check_wire_filters` runs the same
    `validate_order` the simulated arrival runs, *before* `transport.place` is ever called,
    so an off-tick price is rejected locally and never reaches this function. Quantising
    here would make a live session accept an order its own backtest refused, which is a
    parity break in the direction that hides a bug. (For one release no filter check ran on
    the live path at all and this docstring asserted otherwise; the check now exists where
    the claim says it does.)
    """
    intent = order.intent
    params: dict[str, Any] = {
        "symbol": intent.symbol,
        "side": intent.side,
        "type": intent.type.value,
        "quantity": from_scaled(order.remaining),
        "newClientOrderId": cid,
    }
    if intent.position_side.is_hedged:
        # **Mandatory on a hedge-mode order, and it was never sent.** `ctx` documents the
        # field as "sent verbatim... a mismatch is rejected outright (-4061)" -- and this
        # function omitted it, so a hedge-mode session had literally every order refused
        # -4061, the rejection streak tripped the kill switch, and the halt blamed the
        # strategy. Which side of the dual position this order belongs to is the whole
        # identity of a hedge order: a SELL routed to the LONG side is that leg's exit,
        # and the same SELL with no side is a request to open a short.
        #
        # Omitted in one-way mode, deliberately: the account default is BOTH, and the
        # engine refuses a hedged `position_side` on a one-way account long before here
        # (`_resolve_side`), so sending it would add a parameter that can only be refused
        # for reasons unrelated to the order -- the same argument as `reduceOnly` below.
        params["positionSide"] = intent.position_side.value
    if intent.reduce_only:
        # Sent only when true. Binance defaults it to false, and an explicit `reduceOnly` is
        # one more parameter to be refused for a reason that has nothing to do with the
        # order -- it is rejected outright in hedge mode, for instance. It cannot collide
        # with `positionSide` above: `OrderIntent.__post_init__` refuses the combination,
        # because at the exchange the two are mutually exclusive and on a hedge leg the
        # flag is redundant (a SELL routed to the LONG side can only reduce it).
        params["reduceOnly"] = True

    if intent.type is OrderType.LIMIT:
        if intent.price is None:
            raise ValueError("a LIMIT order needs a price")
        params["price"] = intent.price
        params["timeInForce"] = intent.tif.value
    elif intent.type is OrderType.TRAILING_STOP_MARKET:
        if intent.callback_rate is None:
            raise ValueError("a TRAILING_STOP_MARKET order needs a callback_rate")
        with accounting():
            # `OrderIntent.callback_rate` is a **fraction** and Binance's `callbackRate` is a
            # percent -- its own docstring says so in as many words. Sending the fraction
            # unconverted places the stop a hundred times closer than the author asked for,
            # which is a strategy that stops out instantly and looks like a bad strategy.
            params["callbackRate"] = intent.callback_rate * 100
        params["workingType"] = intent.working_type.value
        if intent.stop_price is not None:
            params["activationPrice"] = intent.stop_price
    elif intent.type in (OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET):
        if intent.stop_price is None:
            raise ValueError(f"a {intent.type.value} order needs a stop_price")
        params["stopPrice"] = intent.stop_price
        params["workingType"] = intent.working_type.value

    return params


def _report_trade_id(report: ExchangeReport) -> int | None:
    """The execution's own trade id (`o.t`), or `None` when the frame carries none.

    Read from `raw` rather than promoted to a field on `ExchangeReport`: it is consumed
    by exactly one guard, and a synthesized snapshot report (which stands for a whole
    history, not one execution) correctly has none.
    """
    order = report.raw.get("o") if isinstance(report.raw, Mapping) else None
    if not isinstance(order, Mapping):
        return None
    raw = order.get("t")
    try:
        value = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _report_from_rest_order(payload: Any) -> ExchangeReport | None:
    """An `ExchangeReport` from `GET /fapi/v1/order`'s REST shape, or `None`.

    The REST payload is a *snapshot* where a stream frame is an *increment*: it carries
    `executedQty` cumulatively and no per-execution quantity. Encoding the whole executed
    quantity as one report is exactly what `_book`'s cumulative bookkeeping was built for
    -- it compares `z` against what is already booked and books only the difference, so a
    resolution that races a stream frame cannot double-book, and one that replaces a
    dropped frame books the missing quantity at `avgPrice` with the dropped-frame warning
    it deserves (the true per-fill prices are unknowable from a snapshot, and the entry
    price is honestly uncertain until the next reconciliation pass checks it).

    `is_maker` is `False` because a snapshot does not say: taker is the conservative
    reading (the higher fee), and the fee model is superseded by the venue's own
    commission wherever a real execution report carried one. Returns `None` for a payload
    missing the fields the report cannot exist without -- the caller keeps the outcome
    unknown, which beats booking a guess.
    """
    if not isinstance(payload, dict):
        return None
    try:
        symbol = str(payload["symbol"])
        cid = str(payload["clientOrderId"])
        status = str(payload["status"])
    except KeyError:
        return None
    executed = decimal_to_scaled(parse_money(str(payload.get("executedQty", "0") or "0")))
    avg = decimal_to_scaled(parse_money(str(payload.get("avgPrice", "0") or "0")))
    raw_order_id = payload.get("orderId")
    return ExchangeReport(
        kind=ReportKind.ORDER,
        ts_ms=int(payload.get("updateTime") or payload.get("time") or 0),
        symbol=symbol,
        order_id=None if raw_order_id is None else int(raw_order_id),
        client_order_id=cid,
        status=status,
        last_filled_qty=executed,
        last_filled_price=avg,
        cum_filled_qty=executed,
        avg_price=avg,
        is_maker=False,
        # "TRADE" routes `_apply` to `_book` exactly when there is quantity to book;
        # with nothing executed the status alone (NEW -> ack, CANCELED -> remove)
        # carries the resolution.
        reason="TRADE" if executed > 0 else "RESOLVED",
        raw=payload,
    )


def _rejection_reason(exc: BinanceRestError) -> str:
    """The exchange's own words for a refusal, kept in its own vocabulary.

    Spec 6.7's parity report compares a live rejection against the backtest's, and
    `filters.OrderCheck` already writes rejections in the exchange's filter names for that
    reason. Keeping the code alongside the message is what lets `normalise_rejection_reason`
    fold five hundred "insufficient margin: need N" messages into one tally key while the
    breach an operator reads still carries the number.
    """
    code = "" if exc.code is None else f" ({exc.code})"
    return f"exchange rejected the order{code}: {exc}"


def _payload_order_id(payload: Any) -> int | None:
    """`orderId` out of a `POST /fapi/v1/order` response, or `None`.

    Optional rather than required. The response shape is Binance's and this module has never
    seen a real one (see the module docstring), so a missing or unparseable field costs the
    convenience of an exchange-side id and must not cost the ack -- the ack is what clears
    spec 7's rejection streak and what the latency measurement hangs on.
    """
    if not isinstance(payload, dict):
        return None
    raw = payload.get("orderId")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _elapsed_ms(start: float, end: float) -> int:
    """Monotonic seconds to whole milliseconds, floored at zero.

    `time.monotonic()` cannot go backwards, so the floor is a guard against a caller passing
    the two the wrong way round rather than against the clock. A negative latency sample
    would poison the empirical distribution silently.
    """
    return max(0, int((end - start) * 1000))


async def _sleep_unless_stopped(stop: asyncio.Event, delay: float) -> bool:
    """Wait `delay` seconds. Returns True if `stop` was set instead of the wait elapsing."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
    except TimeoutError:
        return False
    return True
