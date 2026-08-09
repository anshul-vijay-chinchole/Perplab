"""Signed USD-M REST access, with a retry loop an order endpoint can survive (spec 11).

`PublicRestClient` retries on network error, on 429/418 and on every 5xx, which is right
for the endpoints it serves: asking twice for a mark price costs a little weight and
nothing else. Applied unchanged to `POST /fapi/v1/order` the same loop is a position
generator. A read timeout means the request *was* sent and the answer was lost, so the
retry opens a second position on top of the first, and the account ends the day with twice
the exposure the strategy asked for and a backtest that says otherwise. That single
difference is why this is a separate client rather than a subclass.

The rule it enforces: **a request may only be repeated when it provably never arrived.**
A connection that was never established (`ConnectError`, `ConnectTimeout`) sent no bytes,
so repeating it is free. Everything else on an order path -- a read timeout, a write
failure, a 5xx -- leaves the outcome unknown, and Binance's own documentation is explicit
that a 5xx on an order must not be read as a failure. Those raise `OrderOutcomeUnknown`
naming the client order id, so the caller reconciles against `openOrders` and `userTrades`
(spec 6.7) instead of guessing. Every order therefore carries a caller-supplied
`newClientOrderId`: it is the handle reconciliation needs, and it makes any repeat
idempotent at the exchange rather than merely unlikely to be needed.

The second thing this client refuses to do is get the IP banned. Binance answers every
request with its own count of the weight we have spent; `RateBudget` records it and stops
sending before the ceiling rather than discovering the limit as a 418 and an IP ban that
outlasts the trading session.

Clock drift is measured and reported, never corrected. Spec 11 wants a Feed warning past
1 s of drift; silently shifting our timestamps to match the server would remove the symptom
and leave a machine whose clock is wrong -- which is a problem for every `recv_ms` the
collector writes, not only for signing.

Nothing here writes a log line. The query string carries a signature and the header carries
the key, so the safest handling of both is a code path in which neither is ever formatted
into a string that outlives the request.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from urllib.parse import urlencode

import httpx

from perplab.core.money import Money, money_to_str, parse_money
from perplab.exchange.keys import KeySession
from perplab.exchange.rest import BinanceRestError, PublicRestClient

__all__ = [
    "DEFAULT_RECV_WINDOW_MS",
    "MAX_RECV_WINDOW_MS",
    "MAX_CLOCK_DRIFT_MS",
    "UNKNOWN_ORDER_CODE",
    "WEIGHT_CEILING",
    "ORDER_COUNT_10S_CEILING",
    "ORDER_COUNT_1M_CEILING",
    "OrderOutcomeUnknown",
    "RateBudget",
    "RateBudgetExceeded",
    "RetryPolicy",
    "SignedRestClient",
]

UNKNOWN_ORDER_CODE = -2011
"""Binance's "Unknown order sent".

Named because it is the *expected* answer to a cancel that already worked -- a retry after a
read timeout finds nothing left to cancel -- and treating it as a failure would report a
successful kill-switch cancel as an error. See `cancelled_or_already_gone`.
"""

ORDER_DOES_NOT_EXIST_CODE = -2013
"""Binance's "Order does not exist", from `GET /fapi/v1/order`.

The *good* answer to an unknown-outcome query: the venue never saw the POST that timed
out, so the engine's order can be retired with nothing at the exchange to double-fill
against. Distinct from `UNKNOWN_ORDER_CODE` (-2011), which is the cancel endpoint's
spelling of a similar fact -- the two arrive from different endpoints and conflating them
has no cost here, so `resolve_unknown_outcomes` accepts either.
"""

DEFAULT_RECV_WINDOW_MS = 5_000
MAX_RECV_WINDOW_MS = 60_000
"""Binance's own cap. A larger window is rejected outright, so it is refused here instead.

The window is how long a signed request stays valid after its `timestamp`. Widening it to
paper over a drifting clock is the tempting fix and the wrong one: it widens the replay
window on every request we send, and it hides the drift that spec 11 wants reported.
"""

MAX_CLOCK_DRIFT_MS = 1_000
"""Spec 11: *"clock drift beyond 1 s raises a Feed warning before it causes failures."*"""

WEIGHT_CEILING = 2_000
ORDER_COUNT_10S_CEILING = 250
ORDER_COUNT_1M_CEILING = 1_000
"""Refusal thresholds, set below Binance's published limits (2400 weight/min, 300 orders
per 10 s, 1200 per minute) rather than at them.

The headroom is the point. The counters we hold are the exchange's own figures as of the
last response, so they are always at least one request out of date, and a ceiling set at
the limit would be crossed by the request that discovers it. Configurable per client
because the published limits are exactly the kind of number spec Appendix B warns changes
without notice -- the authority is the response header, which is why this tracks the
observed value instead of counting locally.
"""

_WEIGHT_WINDOW_MS = 60_000
_ORDER_10S_WINDOW_MS = 10_000
_ORDER_1M_WINDOW_MS = 60_000

_BACKOFF_BASE_S = 2.0

_SIGNATURE_PATTERN = re.compile(r"signature=[0-9a-fA-F]*")


def _redact(text: str) -> str:
    """Strip any signature out of text that is about to become an exception message.

    A signature is not the secret and cannot be reversed into it, but it is a valid
    authorisation for one request until `recvWindow` elapses, and exception messages travel
    into the Feed, the run record and whatever a user pastes into an issue. Removing it
    costs nothing.
    """
    return _SIGNATURE_PATTERN.sub("signature=<redacted>", text)


def _decode_error(response: httpx.Response) -> tuple[int | None, str]:
    """Binance's error envelope, decoded by the public client's own parser.

    Delegated rather than re-implemented: it is the same envelope from the same exchange,
    and a second copy would drift the day Binance adds a field to it. Reaching past the
    underscore is the smaller cost -- `rest.py` belongs to the data phase and widening its
    surface is not this module's business.
    """
    return PublicRestClient._decode_error(response)


class RetryPolicy(Enum):
    """Whether repeating a request is safe, which is a property of the *operation*.

    Not of the verb. `POST /fapi/v1/leverage` sets leverage to 5 whether it is sent once or
    twice, while `POST /fapi/v1/order` opens a position each time -- so the choice is made
    per endpoint and stated at the call site. POST and PUT default to `ORDER`, so a method
    added later without thinking about this is safe by default and a repeat has to be
    argued for rather than inherited.
    """

    IDEMPOTENT = "IDEMPOTENT"
    """Repeating the request cannot change the outcome. Retried like `PublicRestClient`."""

    ORDER = "ORDER"
    """Repeating the request could open a second position. Retried only when it never
    arrived; anything ambiguous is raised as `OrderOutcomeUnknown`."""


class RateBudgetExceeded(RuntimeError):
    """The exchange's own weight counter is at the refusal ceiling, so nothing was sent."""


class OrderOutcomeUnknown(RuntimeError):
    """An order request reached the exchange and the answer did not come back.

    This is not a failure and must not be treated as one. The order may be working, may
    have filled, may have been rejected -- and the one thing that turns an unknown into a
    disaster is resubmitting it. The message names the client order id precisely because
    that is the handle `GET /fapi/v1/openOrders` and `GET /fapi/v1/userTrades` need to find
    out which of the three happened (spec 6.7).
    """

    def __init__(self, *, symbol: str, client_order_id: str, detail: str) -> None:
        super().__init__(
            f"order {client_order_id!r} on {symbol} reached the exchange but its outcome is "
            f"unknown ({detail}). Do NOT resubmit it: reconcile with openOrders and "
            f"userTrades on {symbol} first, and resubmit only if neither knows this client "
            f"order id."
        )
        self.symbol = symbol
        self.client_order_id = client_order_id
        self.detail = detail


@dataclass
class RateBudget:
    """The exchange's own account of what we have spent, and the line we stop at.

    Every response carries `X-MBX-USED-WEIGHT-1M`, and order endpoints add
    `X-MBX-ORDER-COUNT-10S` / `-1M`. Those are authoritative in a way a local counter never
    is: they include the weight of requests that failed, of retries, and of anything else
    sharing this IP. Tracking them is what makes refusal possible before a 429 becomes a
    418 and an IP ban -- which, during a live session, takes the account offline with
    positions open.

    **An observation goes stale, and that is what stops this bricking the client.** The
    counters reset on the exchange's own window boundary, which we never see. A budget that
    refused forever on the strength of one high reading would need a restart to trade again,
    so a reading older than its window is treated as unknown and the request goes out. The
    cost is that a request may occasionally be sent into a limit we could have predicted;
    the alternative is a client that stops for good, which is worse.
    """

    weight_ceiling: int = WEIGHT_CEILING
    order_10s_ceiling: int = ORDER_COUNT_10S_CEILING
    order_1m_ceiling: int = ORDER_COUNT_1M_CEILING

    used_weight_1m: int = 0
    order_count_10s: int = 0
    order_count_1m: int = 0
    weight_observed_ms: int | None = field(default=None)
    orders_observed_ms: int | None = field(default=None)

    def observe(self, headers: Mapping[str, str], *, now_ms: int | None = None) -> None:
        """Record whichever counters this response carried.

        Header lookup is case-folded by hand rather than relying on `httpx.Headers`, so that
        a caller replaying a captured response out of a plain dict gets the same answer as
        one holding a live response object.
        """
        at = _now_ms() if now_ms is None else now_ms
        folded = {str(k).lower(): v for k, v in headers.items()}

        weight = _header_int(folded, "x-mbx-used-weight-1m")
        if weight is not None:
            self.used_weight_1m = weight
            self.weight_observed_ms = at

        ten_s = _header_int(folded, "x-mbx-order-count-10s")
        one_m = _header_int(folded, "x-mbx-order-count-1m")
        if ten_s is not None or one_m is not None:
            if ten_s is not None:
                self.order_count_10s = ten_s
            if one_m is not None:
                self.order_count_1m = one_m
            self.orders_observed_ms = at

    def check(self, *, is_order: bool, now_ms: int | None = None) -> None:
        """Raise `RateBudgetExceeded` rather than send a request that would breach a limit."""
        at = _now_ms() if now_ms is None else now_ms

        if (
            _fresh(self.weight_observed_ms, at, _WEIGHT_WINDOW_MS)
            and self.used_weight_1m >= self.weight_ceiling
        ):
            raise RateBudgetExceeded(
                f"the exchange reports {self.used_weight_1m} of request weight used this "
                f"minute, at or above the {self.weight_ceiling} ceiling. Nothing was sent. "
                "Wait for the next minute window; sending on would earn a 429 and then a "
                "418 IP ban that outlasts the session."
            )

        if not is_order:
            return

        if (
            _fresh(self.orders_observed_ms, at, _ORDER_10S_WINDOW_MS)
            and self.order_count_10s >= self.order_10s_ceiling
        ):
            raise RateBudgetExceeded(
                f"the exchange reports {self.order_count_10s} orders in the last 10 s, at or "
                f"above the {self.order_10s_ceiling} ceiling. Nothing was sent. Slow the "
                "strategy's submission rate; spec 7's max_orders_per_minute is the limit "
                "that should have caught this first."
            )

        if (
            _fresh(self.orders_observed_ms, at, _ORDER_1M_WINDOW_MS)
            and self.order_count_1m >= self.order_1m_ceiling
        ):
            raise RateBudgetExceeded(
                f"the exchange reports {self.order_count_1m} orders in the last minute, at or "
                f"above the {self.order_1m_ceiling} ceiling. Nothing was sent. Slow the "
                "strategy's submission rate."
            )

    def to_json(self) -> dict[str, Any]:
        return {
            "used_weight_1m": self.used_weight_1m,
            "weight_ceiling": self.weight_ceiling,
            "order_count_10s": self.order_count_10s,
            "order_count_1m": self.order_count_1m,
            "weight_observed_ms": self.weight_observed_ms,
            "orders_observed_ms": self.orders_observed_ms,
        }


class SignedRestClient:
    """Async client for the signed USD-M endpoints, holding no credential of its own.

    The key is read from the `KeySession` on every request rather than baked into the HTTP
    client's default headers. That is what makes `KeySession.wipe()` mean something: a
    client carrying its own copy of the header would keep trading happily after the kill
    switch had "wiped" the keys, which is the opposite of what spec 7.4 promises.
    """

    def __init__(
        self,
        base_url: str,
        keys: KeySession,
        *,
        recv_window_ms: int = DEFAULT_RECV_WINDOW_MS,
        timeout: float = 10.0,
        max_attempts: int = 5,
        budget: RateBudget | None = None,
    ) -> None:
        if not 0 < recv_window_ms <= MAX_RECV_WINDOW_MS:
            raise ValueError(
                f"recv_window_ms must be in 1..{MAX_RECV_WINDOW_MS}, got {recv_window_ms}. "
                "A wider window would be rejected by the exchange, and widening it to "
                "absorb clock drift hides the drift spec 11 wants reported."
            )
        self._base = base_url.rstrip("/")
        self._keys = keys
        self._recv_window_ms = recv_window_ms
        self._max_attempts = max_attempts
        self.budget = budget if budget is not None else RateBudget()

        self.drift_ms: int | None = None
        """Local clock minus exchange clock, in ms; `None` until `server_time_ms` has run."""

        self.rtt_ms: int | None = None
        """Round trip of the measurement that produced `drift_ms`, so a reader can judge it."""

        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=timeout,
            headers={"User-Agent": "perplab/0.1"},
        )

    async def __aenter__(self) -> SignedRestClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------------ signing

    def _signed_query(self, params: Mapping[str, Any]) -> str:
        """Build and sign the exact query string that will go on the wire.

        The timestamp is the local clock, uncorrected. Shifting it by the measured drift
        would make signing work on a machine whose clock is wrong, and every `recv_ms` that
        machine records would still be wrong -- so drift is surfaced (`drift_ms`,
        `clock_drift_warning`) and fixed at the source instead.
        """
        merged = {k: _encode_value(k, v) for k, v in params.items() if v is not None}
        merged["recvWindow"] = str(self._recv_window_ms)
        merged["timestamp"] = str(_now_ms())
        query = urlencode(merged)
        return f"{query}&signature={self._keys.sign(query)}"

    # ------------------------------------------------------------------------ requests

    async def _request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        policy: RetryPolicy | None = None,
        signed: bool = True,
        keyed: bool = True,
        is_order: bool = False,
        symbol: str = "",
        client_order_id: str = "",
    ) -> httpx.Response:
        """One request, retried according to `policy`, with the budget checked before send.

        `policy` defaults to `ORDER` for POST and PUT and `IDEMPOTENT` for everything else,
        so the dangerous default is the safe one. A call site that knows its POST is
        repeatable says so explicitly and explains why.
        """
        if policy is None:
            policy = (
                RetryPolicy.ORDER if method in ("POST", "PUT") else RetryPolicy.IDEMPOTENT
            )

        last: Exception | None = None
        for attempt in range(self._max_attempts):
            # **Checked before every attempt, not once before the loop.** Each attempt's
            # response headers update the observed weight, and the retry path is precisely
            # where a 429 storm happens -- so checking only on entry let a five-attempt retry
            # keep sending into a ceiling it had already watched itself cross, which is how
            # a rate-limit warning becomes an IP ban.
            self.budget.check(is_order=is_order)
            url = path
            if signed:
                url = f"{path}?{self._signed_query(params or {})}"
            elif params:
                url = f"{path}?{urlencode({k: _encode_value(k, v) for k, v in params.items() if v is not None})}"
            headers = {"X-MBX-APIKEY": self._keys.api_key_bytes} if keyed else None

            try:
                response = await self._client.request(method, url, headers=headers)
            except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                # No connection was established, so no bytes reached the exchange and no
                # order can exist. This is the only failure an order request may repeat.
                last = exc
            except httpx.HTTPError as exc:
                if policy is RetryPolicy.ORDER:
                    raise OrderOutcomeUnknown(
                        symbol=symbol,
                        client_order_id=client_order_id,
                        detail=f"{type(exc).__name__}: {_redact(str(exc))}",
                    ) from None
                last = exc
            else:
                self.budget.observe(response.headers)
                if response.status_code == 200:
                    return response

                code, message = _decode_error(response)
                error = BinanceRestError(response.status_code, code, _redact(message))

                if policy is RetryPolicy.ORDER:
                    if response.status_code >= 500:
                        # Binance documents 5xx as "execution status UNKNOWN", not as a
                        # rejection. A 429 or a 4xx did *not* reach the matching engine and
                        # is an ordinary refusal, so it is raised as one.
                        raise OrderOutcomeUnknown(
                            symbol=symbol,
                            client_order_id=client_order_id,
                            detail=f"HTTP {response.status_code}",
                        ) from error
                    raise error

                if response.status_code in (429, 418) or response.status_code >= 500:
                    last = error
                else:
                    raise error

            if attempt < self._max_attempts - 1:
                await asyncio.sleep(_BACKOFF_BASE_S**attempt)

        raise BinanceRestError(
            0,
            None,
            _redact(
                f"exhausted {self._max_attempts} attempts on {method} {path}: "
                f"{type(last).__name__}: {last}"
            ),
        )

    async def _json(self, method: str, path: str, params: Mapping[str, Any] | None = None, **kw: Any) -> Any:
        """Decode a response body.

        Binance sends every price, quantity and balance on these endpoints as a JSON
        *string*, so the default decoder produces no floats and nothing is lost here. The
        values still have to reach `to_scaled` or `parse_money` before any arithmetic
        touches them -- this method deliberately does not convert, because guessing which
        fields are money is how a float gets into the ledger.
        """
        return (await self._request(method, path, params, **kw)).json()

    # -------------------------------------------------------------------------- clock

    async def server_time_ms(self) -> int:
        """The exchange's clock, and the drift measurement spec 11 asks for.

        Drift is taken against the midpoint of the local interval that brackets the call,
        not against the reading before it. The difference is one round trip -- 200 ms on a
        bad link, a fifth of the 1 s threshold -- and attributing that to drift would raise
        a warning about a clock that is fine.
        """
        before = _now_ms()
        payload = await self._json(
            "GET", "/fapi/v1/time", None, signed=False, keyed=False
        )
        after = _now_ms()
        server = int(payload["serverTime"])
        self.rtt_ms = after - before
        self.drift_ms = (before + after) // 2 - server
        return server

    @property
    def clock_drift_warning(self) -> str | None:
        """Spec 11's Feed warning text, or `None` while the clock is good or unmeasured."""
        if self.drift_ms is None or abs(self.drift_ms) <= MAX_CLOCK_DRIFT_MS:
            return None
        direction = "ahead of" if self.drift_ms > 0 else "behind"
        return (
            f"local clock is {abs(self.drift_ms)} ms {direction} the exchange, past the "
            f"{MAX_CLOCK_DRIFT_MS} ms limit in spec 11. Signed requests will start failing "
            f"with -1021 once drift exceeds recvWindow ({self._recv_window_ms} ms). "
            "Re-sync the machine's clock (NTP); PerpLab will not shift timestamps to hide "
            "this, because the same clock stamps every recv_ms the collector records."
        )

    # ------------------------------------------------------------------------- account

    async def validate(self, *, alias: str) -> Money:
        """Spec 11's key-entry check: one lightweight signed request, recorded on the session.

        Returns the wallet balance and stores it, with the alias, on the `KeySession` -- the
        two things spec 11 permits the UI to show. A key that cannot do this is not a key
        this platform can trade with, so the failure is the caller's to surface at entry
        time rather than at first order.
        """
        payload = await self.account()
        balance = _wallet_balance(payload)
        self._keys.record_validation(alias=alias, balance=balance)
        return balance

    async def account(self) -> dict[str, Any]:
        """`GET /fapi/v2/account` -- balances, and the position list spec 6.7 reconciles."""
        return await self._json("GET", "/fapi/v2/account", {})

    async def position_risk(self, symbol: str) -> list[dict[str, Any]]:
        """`GET /fapi/v2/positionRisk` -- size, entry price and the exchange's own `P_liq`.

        The liquidation price here is the number spec 6.7's math validation compares our
        computed one against, which is the whole reason this is a separate call from
        `account()`.
        """
        return await self._json("GET", "/fapi/v2/positionRisk", {"symbol": symbol})

    async def open_orders(self, symbol: str) -> list[dict[str, Any]]:
        """`GET /fapi/v1/openOrders` -- half of the answer to an `OrderOutcomeUnknown`."""
        return await self._json("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    async def user_trades(self, symbol: str, from_id: int | None = None) -> list[dict[str, Any]]:
        """`GET /fapi/v1/userTrades` -- the other half, and the fill record parity reads.

        `from_id` walks the trade id sequence forward for the same reason the aggregate
        trade poller does: an id cursor either returns the next trade or returns nothing,
        so a missed fill is impossible rather than unlikely. A user-data-stream frame can be
        dropped; this sequence cannot skip unnoticed.
        """
        return await self._json(
            "GET", "/fapi/v1/userTrades", {"symbol": symbol, "fromId": from_id}
        )

    async def get_order(self, symbol: str, *, client_order_id: str) -> dict[str, Any]:
        """`GET /fapi/v1/order` -- the canonical answer to an `OrderOutcomeUnknown`.

        A POST that timed out has exactly three possible truths -- the venue never saw it,
        it is working, or it filled -- and this query distinguishes all three: a payload is
        the order's current state, and error -2013 ("Order does not exist") is the venue
        saying the request never landed. `resolve_unknown_outcomes` walks every unresolved
        id through this each reconciliation pass, which is what turns "unknown" from a
        permanent label into a bounded window.
        """
        return await self._json(
            "GET",
            "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
        )

    async def commission_rate(self, symbol: str) -> dict[str, Any]:
        """`GET /fapi/v1/commissionRate` -- what this account actually pays, per side.

        Exists because the platform's first live round trip halted on exactly this: the
        session's fee model said 0.05% taker, the venue charged this account 0.04%, and
        reconciliation correctly refused a wallet that disagreed by the difference. A fee
        *rate* typed into a dialog is a guess about someone else's billing; this endpoint is
        the bill. The live preflight reads it and adopts it into the session spec the same
        way the wallet balance is adopted -- the ledger must charge what the venue charges,
        or spec 6.7.3's to-the-cent comparison is measuring the operator's typing.

        Returns `{"symbol", "makerCommissionRate", "takerCommissionRate"}` with the rates as
        decimal-fraction strings (`"0.000400"` is four basis points).
        """
        return await self._json("GET", "/fapi/v1/commissionRate", {"symbol": symbol})

    # --------------------------------------------------------------------------- orders

    async def new_order(self, **params: Any) -> dict[str, Any]:
        """`POST /fapi/v1/order`. Requires `symbol` and `newClientOrderId`.

        The client order id is mandatory rather than defaulted, and it has to come from the
        caller, because it is the identity the *engine's* order object is keyed by. An id
        minted here would be unknown to the caller at exactly the moment it is needed: when
        this raises `OrderOutcomeUnknown` and something has to go and find out whether the
        order exists.
        """
        symbol = str(params.get("symbol") or "")
        client_order_id = str(params.get("newClientOrderId") or "")
        if not symbol:
            raise ValueError("new_order requires a symbol")
        if not client_order_id:
            raise ValueError(
                "new_order requires newClientOrderId. It is what makes a repeat idempotent "
                "at the exchange and what reconciliation looks the order up by after a "
                "timeout; without it an unknown outcome is unrecoverable."
            )
        return await self._json(
            "POST",
            "/fapi/v1/order",
            params,
            policy=RetryPolicy.ORDER,
            is_order=True,
            symbol=symbol,
            client_order_id=client_order_id,
        )

    async def cancel_order(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """`DELETE /fapi/v1/order`, by exchange order id or by client order id.

        Exactly one identifier, refused rather than resolved if both or neither are given.
        Two identifiers that disagree name two different orders, and picking one silently
        cancels an order nobody asked about.

        **A retry can succeed and still raise `-2011 Unknown order sent`.** The DELETE is
        idempotent in its *effect* and not in its *error*: if the first attempt reached the
        exchange and cancelled the order, the retry after a read timeout finds nothing left
        to cancel and reports so as a failure. `UNKNOWN_ORDER_CODE` names that reply, and
        `cancelled_or_already_gone` is the wrapper a caller should use when it does not care
        which of the two happened -- which is every caller in the halt path, where the goal
        is "this order is not working" rather than "I personally cancelled it".
        """
        if (order_id is None) == (client_order_id is None):
            raise ValueError(
                "cancel_order needs exactly one of order_id or client_order_id; "
                f"got order_id={order_id!r} and client_order_id={client_order_id!r}"
            )
        return await self._json(
            "DELETE",
            "/fapi/v1/order",
            {
                "symbol": symbol,
                "orderId": order_id,
                "origClientOrderId": client_order_id,
            },
            is_order=True,
            symbol=symbol,
        )

    async def cancel_all(self, symbol: str) -> dict[str, Any]:
        """`DELETE /fapi/v1/allOpenOrders` -- step 2 of spec 7's kill switch."""
        return await self._json(
            "DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol}, is_order=True, symbol=symbol
        )

    async def cancelled_or_already_gone(
        self,
        symbol: str,
        *,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """`cancel_order`, treating "no such order" as success.

        The kill switch wants an order *not working*; whether this call or a retry of it did
        the cancelling is a distinction without a difference. Separated from `cancel_order`
        rather than folded into it because the distinction genuinely matters elsewhere -- a
        reconciliation loop asking "did my cancel land?" needs to hear `-2011`, and a
        wrapper that swallowed it for everyone would remove the only evidence.
        """
        try:
            return await self.cancel_order(
                symbol, order_id=order_id, client_order_id=client_order_id
            )
        except BinanceRestError as exc:
            if exc.code == UNKNOWN_ORDER_CODE:
                return {
                    "symbol": symbol,
                    "orderId": order_id,
                    "origClientOrderId": client_order_id,
                    "status": "ALREADY_GONE",
                }
            raise

    async def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        """`POST /fapi/v1/leverage`.

        Marked idempotent explicitly: setting leverage to 5 twice leaves leverage at 5, so
        this is one of the POSTs a repeat cannot harm. The exception is stated here rather
        than inferred from the verb, so the default for a POST stays "do not repeat".
        """
        return await self._json(
            "POST",
            "/fapi/v1/leverage",
            {"symbol": symbol, "leverage": int(leverage)},
            policy=RetryPolicy.IDEMPOTENT,
            symbol=symbol,
        )

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        """`POST /fapi/v1/marginType` -- `ISOLATED` or `CROSSED`.

        Idempotent for the same reason as `set_leverage`, with one wrinkle worth stating
        here rather than at the call site: setting the margin type it already has is *not*
        a silent success. Binance answers `-4046 No need to change margin type`, an error
        response for a request that achieved exactly what was asked. `live.preflight`
        treats that code as success; nothing else should have to rediscover it.

        Note Binance's spelling: the value is `CROSSED`, not `CROSS`.
        """
        return await self._json(
            "POST",
            "/fapi/v1/marginType",
            {"symbol": symbol, "marginType": str(margin_type)},
            policy=RetryPolicy.IDEMPOTENT,
            symbol=symbol,
        )

    async def position_mode(self) -> dict[str, Any]:
        """`GET /fapi/v1/positionSide/dual` -- is the account in hedge mode?

        Read, never written. Flipping an account between one-way and hedge changes what
        every subsequent order means, and Binance refuses the change while any position is
        open or any order is working -- so it is an account-level decision the operator
        makes deliberately, not something a session should do on their behalf mid-run.
        `live.preflight` reads it to refuse a mismatch against the ledger's own mode.
        """
        return await self._json("GET", "/fapi/v1/positionSide/dual", {})

    # ---------------------------------------------------------------------- listen key

    async def listen_key_create(self) -> str:
        """`POST /fapi/v1/listenKey` -- the key the user-data socket connects with.

        Sent with the API key header and **no signature**. These three endpoints are
        USER_STREAM rather than USER_DATA: the key alone authorises them, and appending a
        `timestamp` and `signature` they do not expect invites a `-1104` about parameters
        that were not read. Safe to repeat -- a second call returns the currently active key
        and extends it rather than issuing a second one.
        """
        payload = await self._json(
            "POST", "/fapi/v1/listenKey", None, policy=RetryPolicy.IDEMPOTENT, signed=False
        )
        key = str(payload["listenKey"])
        if not key:
            raise BinanceRestError(0, None, "listenKey response carried an empty key")
        return key

    async def listen_key_keepalive(self) -> None:
        """`PUT /fapi/v1/listenKey` -- extends the key's 60-minute life.

        Returns nothing on purpose. Binance answers with an empty object, and a method that
        returned it would invite a caller to check a field that is never there.
        """
        await self._request(
            "PUT", "/fapi/v1/listenKey", None, policy=RetryPolicy.IDEMPOTENT, signed=False
        )

    async def listen_key_close(self) -> None:
        """`DELETE /fapi/v1/listenKey` -- ends the stream cleanly on shutdown."""
        await self._request("DELETE", "/fapi/v1/listenKey", None, signed=False)


def _wallet_balance(account_payload: Mapping[str, Any]) -> Money:
    """Read the wallet balance out of an `/fapi/v2/account` payload, exactly.

    `totalWalletBalance` is the figure spec 11 shows at key entry. It arrives as a decimal
    string and is parsed as one: `float()` here would put a lossy number in front of the
    user on the very screen where they decide the connection is correct.
    """
    for field_name in ("totalWalletBalance", "availableBalance"):
        value = account_payload.get(field_name)
        if value is not None:
            return parse_money(str(value))
    raise BinanceRestError(
        0,
        None,
        "account payload carried neither totalWalletBalance nor availableBalance; "
        "cannot validate the key against a balance (spec 11)",
    )


def _encode_value(name: str, value: Any) -> str:
    """Render one query parameter as the exchange expects to read it.

    Two conversions are wrong and both are silent. Python's `True` stringifies to `"True"`,
    which Binance does not accept as a boolean, so `reduceOnly=True` becomes a parameter
    error at best and an ignored flag at worst. And a `float` price is already the wrong
    number before it gets here -- `0.07` is not 0.07 -- so it is refused rather than
    rendered, with the same reasoning `core.money` gives for taking prices as strings.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        raise TypeError(
            f"parameter {name!r} was passed as a float ({value!r}). Prices and quantities "
            "are exact decimals: pass a string or a Decimal from perplab.core.money, "
            "because a float has already lost the value before this line runs."
        )
    if isinstance(value, Money):
        return money_to_str(value)
    return str(value)


def _header_int(headers: Mapping[str, Any], name: str) -> int | None:
    raw = headers.get(name)
    if raw is None:
        return None
    try:
        return int(str(raw))
    except ValueError:
        # A header we cannot parse is not a reason to fail a request that already
        # succeeded. It does mean the budget keeps its previous reading, which ages out on
        # its own rather than pinning the client to a stale number.
        return None


def _fresh(observed_ms: int | None, now_ms: int, window_ms: int) -> bool:
    return observed_ms is not None and now_ms - observed_ms < window_ms


def _now_ms() -> int:
    return int(time.time() * 1000)
