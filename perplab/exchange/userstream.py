"""The USD-M user-data stream: the only push channel that says a fill happened (spec 6.7).

Spec 6.7 makes this load-bearing rather than convenient. Live-to-exchange reconciliation
runs every 60 s and **triggers the kill switch** on any mismatch, so the 60 s between
passes is exactly the window in which PerpLab's ledger has to already know about a fill.
This stream is what closes it: an `ORDER_TRADE_UPDATE` lands within milliseconds of the
match, and the REST pass becomes a check on a ledger that is already right rather than the
mechanism by which it becomes right.

It is not a substitute for that check and nothing here pretends otherwise. This socket
carries no sequence number, so a dropped frame is invisible -- which is why spec 6.7 keeps
the REST pass and why `SignedRestClient.user_trades` walks a trade id cursor. What lives
here is the fast path, and the plumbing that keeps it alive.

**Three ways this feed dies quietly, and what is done about each.**

1. *The listen key expires.* Binance kills a key 60 minutes after creation unless it is
   extended, and when it does **the socket stays open and simply stops delivering**. There
   is no close frame, no error and no reconnect, so `StreamManager` sees a healthy
   connection forever and every fill after that moment is lost with nothing anywhere
   saying so. That is why `listenKeyExpired` is surfaced as its own report kind and as a
   STALE event -- `CollectorEventKind.STALE` is defined as precisely this state -- rather
   than as a disconnect, and why `run()` tears the session down and re-keys instead of
   waiting to be told.
2. *The keepalive loop dies.* A `PUT` every 30 minutes is the only thing standing between
   a live session and case 1, so an unhandled exception in that loop is a fill feed that
   stops working an hour later, at a moment nothing connects to the exception. It is
   guarded exactly the way `RestPoller._guarded` guards a poll: every failure becomes a
   recorded event and the loop keeps its schedule.
3. *A frame we cannot parse.* `StreamManager` puts no handler around `on_message`, so an
   exception raised while parsing escapes the read loop, is caught by the reconnect arm and
   is reported as a disconnect -- one unfamiliar frame shape would become a permanent
   reconnect loop against a socket that is working perfectly. Bad frames are counted and
   reported on the escalating schedule `ws._note_unrecognised` uses, for the same reason.

**No credential is formatted anywhere in this module.** The api secret never reaches this
path at all -- the three listen-key endpoints are USER_STREAM and are authorised by the key
header alone, so nothing here ever signs. The listen key itself is a bearer token for the
account's own order flow, so it is scrubbed out of every event message written here, and
out of `raw` if the exchange echoes it back. There is deliberately no logger in this file:
spec 11 names "log line" as one of the six places a credential must never appear, and the
event sink already carries everything an operator needs.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from perplab.core.money import to_scaled
from perplab.core.types import CollectorEventKind, Side
from perplab.exchange.signed import SignedRestClient
from perplab.exchange.ws import PRODUCTION_WS, StreamManager

__all__ = [
    "KEEPALIVE_INTERVAL_S",
    "KEEPALIVE_RETRY_S",
    "LISTEN_KEY_TTL_S",
    "USER_STREAM_LABEL",
    "ExchangeReport",
    "ReportKind",
    "UnhandledUserEvent",
    "UserDataStream",
    "parse_user_frame",
]

USER_STREAM_LABEL = "userData"
"""Stream label on every event this module writes.

Not a dataset name: nothing in spec 4.1 is fed from here, and `gaps._stream_covers_dataset`
matches records to datasets by this field. A label naming a real dataset would hand that
dataset an alibi for a gap it did not earn.
"""

LISTEN_KEY_TTL_S = 60 * 60
"""Binance expires a listen key 60 minutes after it was last extended."""

KEEPALIVE_INTERVAL_S = 30 * 60
"""Half the key's life, and the halving is the point.

The first `PUT` lands with 30 minutes of the hour still on the key, so a keepalive that
starts failing has that whole margin to recover in -- roughly thirty attempts at
`KEEPALIVE_RETRY_S` apart. An interval of, say, 55 minutes would meet the documented
requirement and leave a single failed request as the difference between a live fill feed
and a dead one.
"""

KEEPALIVE_RETRY_S = 60.0
"""Retry spacing after a failed keepalive.

Deliberately not exponential. The ordinary backoff argument -- do not hammer an endpoint
that is struggling -- is outranked here by a hard deadline: the key dies at a known time
and every skipped attempt is one fewer chance to save it. One request a minute is
negligible weight (`PUT /fapi/v1/listenKey` costs 1) against a 30-minute margin.
"""

_BACKOFF_INITIAL_S = 1.0
_BACKOFF_CAP_S = 60.0

_SHUTDOWN_GRACE_S = 5.0
"""How long a session waits for its own tasks after asking them to stop.

`websockets.connect` sits in a 20 s open timeout, and a kill switch (spec 7) that waits
20 s for a socket it is trying to abandon is not a kill switch. Past the grace period the
tasks are cancelled.
"""

_MALFORMED = "malformed"
_UNHANDLED = "unhandled"

ReportSink = Callable[["ExchangeReport"], None]
EventSink = Callable[[CollectorEventKind, str, str, int], None]


class UnhandledUserEvent(LookupError):
    """The frame is well-formed and carries an event name this module does not translate.

    Distinct from a malformed frame, and the distinction is worth a class. Binance adds
    events to this stream without notice (`TRADE_LITE`, `STRATEGY_UPDATE`,
    `CONDITIONAL_ORDER_TRIGGER_REJECT`), so an unknown name means "newer than us" and is
    ordinary; a `KeyError` on `o.L` means a field we depend on has moved, which is not.
    Counting them together would let the second hide inside the first.
    """


class ReportKind(Enum):
    """The user-data events PerpLab acts on.

    The values are the exchange's own event names, so dispatch is `ReportKind(payload["e"])`
    and an unknown name raises rather than falling through to a default branch. There is no
    catch-all member on purpose: a report whose kind is "something else" would be delivered
    to a caller that has no way to act on it, which is worse than being counted as
    unhandled and left visible in the event log.
    """

    ORDER = "ORDER_TRADE_UPDATE"
    ACCOUNT = "ACCOUNT_UPDATE"
    ACCOUNT_CONFIG = "ACCOUNT_CONFIG_UPDATE"
    MARGIN_CALL = "MARGIN_CALL"
    LISTEN_KEY_EXPIRED = "listenKeyExpired"


@dataclass(frozen=True, slots=True)
class ExchangeReport:
    """One thing the exchange told us about our own account.

    Frozen for the reason every record in `core.types` is frozen: this is the input to the
    ledger, and a report that can be edited after delivery is a fill whose price can be
    rewritten by whoever reads it second.

    **Quantities and prices are scaled int64** (`core.money`), not float and not `Decimal`.
    Scaled integers are what the rest of the market-data path speaks, and the accounting
    layer crosses the seam with `from_scaled` when it applies the fill -- which keeps the
    exact-equality invariants of spec 3.10 reachable. `commission` and `realized_pnl` are
    money and are scaled here for the same reason: they arrive as decimal strings, and
    `float("0.07")` is not 0.07.

    Only `kind`, `ts_ms` and `raw` are meaningful on every kind. The order fields are zero
    or empty on an account event, which is the truthful encoding -- an `ACCOUNT_UPDATE`
    reports no order, as opposed to reporting one of size zero -- and callers switch on
    `kind` rather than sniffing for populated fields.
    """

    kind: ReportKind
    ts_ms: int
    """Exchange event time (`E`), which is what the engine orders on (spec 6.2).

    The order object's own transaction time (`o.T`) differs by a millisecond or two and
    stays in `raw`; `E` is used because it is the one timestamp present on every kind, and
    a `ts_ms` that meant different clocks on different events would be worse than useless
    to anything sorting a session's reports.
    """

    symbol: str = ""
    order_id: int | None = None
    client_order_id: str = ""
    """The engine's own handle on the order (`o.c`), and the key reconciliation looks up.

    Note that the exchange mints this itself for liquidation and ADL fills -- those arrive
    with an `autoclose-` or `adl_autoclose` prefix and match no order PerpLab submitted,
    which is how spec 7's `halt_on_liquidation` learns it has happened.
    """

    status: str = ""
    """Order status (`o.X`): NEW, PARTIALLY_FILLED, FILLED, CANCELED, EXPIRED, NEW_INSURANCE,
    NEW_ADL. Distinct from `reason`, which carries the execution type (`o.x`)."""

    side: Side | None = None
    last_filled_qty: int = 0
    last_filled_price: int = 0
    cum_filled_qty: int = 0
    avg_price: int = 0
    commission: int = 0
    commission_asset: str = ""
    is_maker: bool = False
    """Whether *this* execution was the maker side (`o.m`).

    Not the aggressor flag of a public trade: `AggTrade.is_buyer_maker` says who the maker
    was in the market, this says whether we were. Inverting it turns a maker rebate into a
    taker fee in the parity report (spec 6.7) and the discrepancy is small enough per fill
    to look like model error rather than a sign flip.
    """

    reduce_only: bool = False
    realized_pnl: int = 0
    reason: str = ""
    """Why this report exists, in the exchange's own vocabulary for its kind.

    Execution type (`o.x`: TRADE, CALCULATED, EXPIRED, AMENDMENT...) on an order report,
    the event reason type (`a.m`: ORDER, FUNDING_FEE, MARGIN_TRANSFER...) on an account
    update, and which setting moved on a config update. Kept as one field rather than one
    per kind because every consumer switches on `kind` first anyway.
    """

    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)
    """The frame as received, minus any listen key the exchange echoed back.

    Out of `repr` because an `ORDER_TRADE_UPDATE` is 600 characters of JSON and these
    reports are written into the Feed one line each.
    """


def parse_user_frame(payload: Mapping[str, Any]) -> ExchangeReport:
    """Translate one raw user-data frame into an `ExchangeReport`.

    Raises `UnhandledUserEvent` for an event name we do not translate, and `KeyError` /
    `ValueError` for a frame whose shape has moved. Both are refusals rather than
    best-effort reads: a report assembled around a missing field would be a fill with a
    plausible wrong price, and the ledger has no way to notice that.
    """
    name = str(payload.get("e") or "")
    try:
        kind = ReportKind(name)
    except ValueError:
        raise UnhandledUserEvent(
            f"no translation for user-data event {name!r}"
        ) from None
    return _PARSERS[kind](payload)


def _parse_order(payload: Mapping[str, Any]) -> ExchangeReport:
    order = payload["o"]
    return ExchangeReport(
        kind=ReportKind.ORDER,
        ts_ms=int(payload["E"]),
        symbol=str(order["s"]),
        order_id=int(order["i"]),
        client_order_id=str(order["c"]),
        status=str(order["X"]),
        side=Side(str(order["S"])),
        last_filled_qty=_scaled(order, "l"),
        last_filled_price=_scaled(order, "L"),
        cum_filled_qty=_scaled(order, "z"),
        avg_price=_scaled(order, "ap"),
        commission=_scaled(order, "n"),
        commission_asset=str(order.get("N") or ""),
        is_maker=bool(order["m"]),
        reduce_only=bool(order["R"]),
        realized_pnl=_scaled(order, "rp"),
        reason=str(order["x"]),
        raw=payload,
    )


def _parse_account(payload: Mapping[str, Any]) -> ExchangeReport:
    """`ACCOUNT_UPDATE` -- balances and positions changed, here is when and why.

    The `B` (balance) and `P` (position) arrays stay in `raw` rather than being flattened
    into fields. Spec 6.7 reconciles against `GET /fapi/v2/account`, which is authoritative;
    a second account snapshot carrying the same field names from a weaker source is the
    kind of thing that reads fine until the two disagree and nobody can say which one the
    ledger was built from. This report says *when to look*, and the REST pass says *what is
    true*.
    """
    account = payload["a"]
    return ExchangeReport(
        kind=ReportKind.ACCOUNT,
        ts_ms=int(payload["E"]),
        reason=str(account["m"]),
        raw=payload,
    )


def _parse_account_config(payload: Mapping[str, Any]) -> ExchangeReport:
    """`ACCOUNT_CONFIG_UPDATE` -- leverage or multi-assets mode changed under us.

    Worth its own kind because it can arrive without PerpLab having asked for anything: a
    leverage change made in the Binance app during a live session invalidates every margin
    and liquidation figure the engine computed (spec 3.6), and the first symptom otherwise
    is spec 6.7's reconciliation firing the kill switch on a `P_liq` mismatch nobody can
    explain.
    """
    config = payload.get("ac")
    if config is not None:
        return ExchangeReport(
            kind=ReportKind.ACCOUNT_CONFIG,
            ts_ms=int(payload["E"]),
            symbol=str(config["s"]),
            reason="LEVERAGE",
            raw=payload,
        )
    if "ai" in payload:
        return ExchangeReport(
            kind=ReportKind.ACCOUNT_CONFIG,
            ts_ms=int(payload["E"]),
            reason="MULTI_ASSETS_MODE",
            raw=payload,
        )
    raise KeyError(
        "ACCOUNT_CONFIG_UPDATE carried neither 'ac' (leverage) nor 'ai' (multi-assets "
        "mode); refusing to report a configuration change without saying what changed"
    )


def _parse_margin_call(payload: Mapping[str, Any]) -> ExchangeReport:
    """`MARGIN_CALL` -- maintenance margin is short on one or more positions.

    `symbol` is left empty even when the frame names exactly one position. The event is
    about the account, the `p` array can carry several symbols, and a report that sometimes
    names a symbol and sometimes does not is a field every consumer has to special-case.
    The positions, with their mark prices and maintenance margins, are in `raw`.
    """
    return ExchangeReport(
        kind=ReportKind.MARGIN_CALL,
        ts_ms=int(payload["E"]),
        raw=payload,
    )


def _parse_listen_key_expired(payload: Mapping[str, Any]) -> ExchangeReport:
    """`listenKeyExpired` -- this socket will never deliver anything again.

    `E` is read defensively here and nowhere else in this module. Everywhere else a missing
    field is a refusal, because a report built around a guess is worse than no report. This
    one event inverts that: refusing it would count a malformed frame and move on, leaving
    the session attached to a socket that has already stopped delivering, which is the
    exact failure the event exists to prevent. A report with `ts_ms=0` still re-keys the
    stream.
    """
    # Spot echoes the expired key back in this frame and USD-M may follow. It is a bearer
    # token for the account's order flow and `raw` is the field most likely to end up in a
    # run record, so it is dropped once here rather than at every consumer.
    raw = {k: v for k, v in payload.items() if k != "listenKey"}
    return ExchangeReport(
        kind=ReportKind.LISTEN_KEY_EXPIRED,
        ts_ms=int(payload.get("E") or 0),
        reason="listenKeyExpired",
        raw=raw,
    )


_PARSERS: dict[ReportKind, Callable[[Mapping[str, Any]], ExchangeReport]] = {
    ReportKind.ORDER: _parse_order,
    ReportKind.ACCOUNT: _parse_account,
    ReportKind.ACCOUNT_CONFIG: _parse_account_config,
    ReportKind.MARGIN_CALL: _parse_margin_call,
    ReportKind.LISTEN_KEY_EXPIRED: _parse_listen_key_expired,
}


def _scaled(source: Mapping[str, Any], key: str) -> int:
    """One Binance decimal string as a scaled int64.

    A *missing* key raises, because every numeric field read through here is documented as
    always present on its event and its absence means the payload shape has moved. A JSON
    `null` returns zero, because that is what the exchange sends for a commission on an
    event that was not a trade -- "not charged" rather than "unknown".
    """
    value = source[key]
    if value is None:
        return 0
    return to_scaled(str(value))


class UserDataStream:
    """Holds the user-data stream open: create the key, keep it alive, re-key on expiry.

    The socket is a raw-mode `StreamManager` on `/ws/<listenKey>`, which is the mode that
    exists for exactly this stream -- the combined-stream demultiplexer drops any frame
    without a `stream` wrapper, and every frame here is bare.

    `on_report` runs inline on the socket read path and, like `StreamManager.on_message`,
    **must not block**: a disk flush or a network call here stops us draining the socket
    until Binance's send buffer fills and it drops the connection. Parsing failures are
    caught; an exception raised by `on_report` itself is the caller's bug and is not
    swallowed.
    """

    def __init__(
        self,
        rest: SignedRestClient,
        on_report: ReportSink,
        on_event: EventSink,
        *,
        ws_base_url: str = PRODUCTION_WS,
        keepalive_interval_s: float = KEEPALIVE_INTERVAL_S,
        keepalive_retry_s: float = KEEPALIVE_RETRY_S,
    ) -> None:
        if not 0 < keepalive_interval_s < LISTEN_KEY_TTL_S:
            raise ValueError(
                f"keepalive_interval_s must be in (0, {LISTEN_KEY_TTL_S}), got "
                f"{keepalive_interval_s}. An interval at or past the key's own life "
                "guarantees the stream goes silent once an hour, and the silence is the "
                "kind that leaves a healthy-looking socket behind it."
            )
        self._rest = rest
        self._on_report = on_report
        self._on_event = on_event
        self._ws_base_url = ws_base_url
        self._keepalive_interval_s = float(keepalive_interval_s)
        self._keepalive_retry_s = float(keepalive_retry_s)

        self._listen_key: str | None = None
        self._renew = asyncio.Event()
        self._failures = 0
        self._failing_since_ms: int | None = None
        self._bad_frames = {_MALFORMED: 0, _UNHANDLED: 0}
        self._bad_report_at = {_MALFORMED: 1, _UNHANDLED: 1}

        self.reports = 0
        self.keepalives = 0
        self.listen_key_expiries = 0

    # ------------------------------------------------------------------------ counters

    @property
    def malformed_frames(self) -> int:
        """Frames whose shape we could not read. Non-zero always means something moved."""
        return self._bad_frames[_MALFORMED]

    @property
    def unhandled_frames(self) -> int:
        """Frames carrying an event name this module does not translate.

        Non-zero is not automatically wrong -- Binance adds events to this stream without
        notice -- but it is always worth a look, which is why it is a number rather than a
        `continue`.
        """
        return self._bad_frames[_UNHANDLED]

    # ----------------------------------------------------------------------- lifecycle

    async def run(self, stop: asyncio.Event) -> None:
        """Hold the stream open until `stop`, re-keying whenever the listen key expires.

        The outer loop is what makes a `listenKeyExpired` recoverable at all. The key is
        embedded in the socket's own URL, so a new key means a new connection -- there is
        nothing to renew in place, and a `StreamManager` told to reconnect would reconnect
        to the dead key forever.
        """
        backoff = _BACKOFF_INITIAL_S
        while not stop.is_set():
            listen_key = await self._create_key()
            if listen_key is None:
                if await _sleep_unless_stopped(stop, backoff):
                    return
                backoff = min(backoff * 2, _BACKOFF_CAP_S)
                continue
            backoff = _BACKOFF_INITIAL_S

            try:
                await self._session(listen_key, stop)
            finally:
                # Closed before the replacement is created, never after. `POST
                # /fapi/v1/listenKey` returns the *currently active* key rather than
                # minting a second one, so a close issued after a re-create would close
                # the key the new socket had just connected with.
                await self._guarded(self._rest.listen_key_close, "listen key close")
                self._listen_key = None

    async def _create_key(self) -> str | None:
        """`POST /fapi/v1/listenKey`, or `None` and a recorded failure.

        No CONNECT event is written here. `StreamManager` writes one when the socket
        actually opens, and a second CONNECT for the key would leave the event stream with
        a connect that never disconnects -- which is exactly the pairing spec 4.5's gap
        detector reads to decide whether a hole in the data is explained.
        """
        key: str | None = None

        async def create() -> None:
            nonlocal key
            key = await self._rest.listen_key_create()

        if not await self._guarded(create, "listen key create"):
            return None
        self._listen_key = key
        return key

    async def _session(self, listen_key: str, stop: asyncio.Event) -> None:
        """One socket on one listen key, ending when `stop` is set or the key expires."""
        self._renew = asyncio.Event()
        inner = asyncio.Event()
        manager = StreamManager(
            (),
            self._on_frame,
            self._on_event,
            base_url=self._ws_base_url,
            raw_path=listen_key,
            label=USER_STREAM_LABEL,
        )
        socket = asyncio.create_task(manager.run(inner))
        keeper = asyncio.create_task(self._keepalive_loop(inner))
        stop_wait = asyncio.create_task(stop.wait())
        renew_wait = asyncio.create_task(self._renew.wait())

        try:
            # The socket task is in the wait set alongside the two events, so a manager
            # that ends for any reason ends the session and earns a fresh key. Without it,
            # a `run()` that exited -- it absorbs its own connection failures, so only an
            # exception it could not absorb gets it there -- would leave this parked on two
            # events that nothing is ever going to set, and the session would sit on a dead
            # feed with no error anywhere. Silence is the one failure mode this module is
            # built to refuse.
            await asyncio.wait(
                {stop_wait, renew_wait, socket}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            inner.set()
            stop_wait.cancel()
            renew_wait.cancel()
            _, pending = await asyncio.wait({socket, keeper}, timeout=_SHUTDOWN_GRACE_S)
            for task in pending:
                task.cancel()
            await asyncio.gather(
                socket, keeper, stop_wait, renew_wait, return_exceptions=True
            )

    async def _keepalive_loop(self, stop: asyncio.Event) -> None:
        """`PUT /fapi/v1/listenKey` on a schedule, and never die of a failure.

        The schedule is driven from a monotonic deadline rather than `sleep(interval)`
        after each call, for the reason `RestPoller.run` gives: request latency added to
        every cycle accumulates, and here it accumulates against a hard expiry.
        """
        next_at = time.monotonic() + self._keepalive_interval_s
        while not stop.is_set():
            if await _sleep_unless_stopped(
                stop, max(0.0, next_at - time.monotonic())
            ):
                return
            if await self._guarded(self._rest.listen_key_keepalive, "keepalive"):
                self.keepalives += 1
                next_at = time.monotonic() + self._keepalive_interval_s
            else:
                next_at = time.monotonic() + self._keepalive_retry_s

    async def _guarded(self, fn: Callable[[], Awaitable[Any]], phase: str) -> bool:
        """Run one listen-key call, converting any failure into a recorded event.

        Catches broadly and deliberately, exactly as `RestPoller._guarded` does. A keepalive
        loop that dies on an unexpected exception takes the fill feed offline an hour later,
        and by then nothing in the session connects the silence to the exception.

        One failure counter covers create, keepalive and close, because they are one
        dependency: if the listen-key endpoint is unreachable, all three are, and a
        recovery message naming only one of them would describe less than what recovered.
        """
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - see docstring
            self._failures += 1
            if self._failing_since_ms is None:
                self._failing_since_ms = _now_ms()
            self._on_event(
                CollectorEventKind.DISCONNECT,
                USER_STREAM_LABEL,
                self._scrub(
                    f"{phase} failed ({self._failures} consecutive): "
                    f"{type(exc).__name__}: {exc}"
                ),
                0,
            )
            return False

        if self._failures:
            downtime = _now_ms() - (self._failing_since_ms or _now_ms())
            self._on_event(
                CollectorEventKind.RECONNECT,
                USER_STREAM_LABEL,
                f"listen key control plane recovered after {self._failures} "
                "consecutive failure(s)",
                downtime,
            )
            self._failures = 0
            self._failing_since_ms = None
        return True

    # --------------------------------------------------------------------------- frames

    def _on_frame(self, name: str, payload: dict[str, Any], _recv_ms: int) -> None:
        """Translate one frame and hand it to the caller.

        The receive stamp is ignored on purpose. It belongs to the collector's datasets,
        where it measures exchange latency; an execution report is ordered by the exchange's
        own `E` (spec 6.2), and carrying a second clock on the record would invite somebody
        to sort by it.
        """
        try:
            report = parse_user_frame(payload)
        except UnhandledUserEvent as exc:
            self._note_bad_frame(_UNHANDLED, str(exc))
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a bad frame must not close the socket
            self._note_bad_frame(_MALFORMED, f"{name}: {type(exc).__name__}: {exc}")
            return

        self.reports += 1
        self._on_report(report)

        if report.kind is ReportKind.LISTEN_KEY_EXPIRED:
            self.listen_key_expiries += 1
            # STALE, not DISCONNECT. `CollectorEventKind.STALE` is defined as this exact
            # state -- a subscribed stream stopped delivering while the connection stayed
            # up -- and calling it a disconnect would be wrong twice over: the gap detector
            # would treat the silence as explained by a drop that never happened (spec
            # 4.5), and spec 7's disconnect auto-trigger would be counting downtime on a
            # socket that is still connected and will simply never speak again.
            self._on_event(
                CollectorEventKind.STALE,
                USER_STREAM_LABEL,
                "listen key expired; the socket is still open and will deliver nothing "
                "further. Re-keying.",
                0,
            )
            self._renew.set()

    def _note_bad_frame(self, category: str, sample: str) -> None:
        """Count a frame we did not deliver, and report at 1, 10, 100, ...

        The escalation is `ws._note_unrecognised`'s, for its reason: a frame shape we do
        not understand arrives at the rate the ones we do understand arrive at, so an event
        per occurrence buries the fault inside its own description -- and this event stream
        is also the gap-detection input (spec 4.5), so flooding it is not cosmetic.
        """
        self._bad_frames[category] += 1
        seen = self._bad_frames[category]
        if seen < self._bad_report_at[category]:
            return
        self._bad_report_at[category] *= 10
        self._on_event(
            CollectorEventKind.STALE,
            USER_STREAM_LABEL,
            self._scrub(
                f"{seen} {category} user-data frame(s); most recent: {sample[:200]}"
            ),
            0,
        )

    def _scrub(self, text: str) -> str:
        """Remove the live listen key from anything about to become an event message.

        `StreamManager` scrubs its own events -- its `raw_path` *is* the key -- and this
        covers the ones written here, where an `httpx` error can carry a URL and an
        exchange error message can quote the key back at us.
        """
        key = self._listen_key
        if not key:
            return text
        return text.replace(key, "<listen-key>")


async def _sleep_unless_stopped(stop: asyncio.Event, delay: float) -> bool:
    """Wait `delay` seconds. Returns True if `stop` was set instead of the wait elapsing."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
    except TimeoutError:
        return False
    return True


def _now_ms() -> int:
    """Wall-clock epoch milliseconds, for the downtime figure an operator reads."""
    return int(time.time() * 1000)
