"""Tests for the signed REST client (spec 11), driven through `httpx.MockTransport`.

A mock transport exercises the genuine `httpx` client, the real URL assembly and the real
status classification -- everything except the socket -- which is what makes it possible to
assert the one property that matters most here and cannot be tested against a live exchange
at all: **which failures are allowed to be repeated.**

That rule, from the module under test: a request may only be repeated when it provably
never arrived. A `ConnectError` established no connection and sent no bytes, so repeating it
is free. A `ReadTimeout` on `POST /fapi/v1/order` means the request *was* sent and the
answer was lost -- the retry would open a second position on top of the first, and the
account ends the day with twice the exposure the strategy asked for while the backtest says
otherwise. There is no way to observe that difference from outside the process, so it is
pinned from inside.

The rest of the file covers what the exchange checks before it will answer at all
(signature, `timestamp`, `recvWindow`, the key header) and what keeps the IP out of a 418
ban (the rate-budget headers).
"""

from __future__ import annotations

import hashlib
import hmac
import time
import urllib.parse
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest

from perplab.exchange import signed
from perplab.exchange.keys import KeySession
from perplab.exchange.rest import BinanceRestError
from perplab.exchange.signed import (
    MAX_CLOCK_DRIFT_MS,
    MAX_RECV_WINDOW_MS,
    WEIGHT_CEILING,
    OrderOutcomeUnknown,
    RateBudget,
    RateBudgetExceeded,
    SignedRestClient,
)

BASE = "https://testnet.binancefuture.example"
API_KEY = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
API_SECRET = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1wi9UwyBGZQvcSCzz1WCU1KLUEMWQVfP"

Handler = Callable[[httpx.Request], httpx.Response]

ORDER = {
    "symbol": "BTCUSDT",
    "side": "BUY",
    "type": "MARKET",
    "quantity": "0.010",
    "newClientOrderId": "perplab-7f3a",
}
"""One market order, with the client order id every order on this path must carry."""


@asynccontextmanager
async def client(handler: Handler, **kwargs: Any) -> AsyncIterator[SignedRestClient]:
    """A `SignedRestClient` whose transport is `handler`.

    `SignedRestClient` builds its own `httpx.AsyncClient` and offers no seam to pass a
    transport in, so the one it made is closed and replaced. Worth naming rather than
    hiding: it is the only reach into a private in this file, and a `transport=` parameter
    on the constructor would remove the need for it.
    """
    keys = kwargs.pop("keys", None) or KeySession(API_KEY, API_SECRET)
    signed_client = SignedRestClient(BASE, keys, **kwargs)
    await signed_client.aclose()
    signed_client._client = httpx.AsyncClient(
        base_url=BASE, transport=httpx.MockTransport(handler)
    )
    try:
        yield signed_client
    finally:
        await signed_client.aclose()


def recording(
    *responses: httpx.Response | Exception,
) -> tuple[list[httpx.Request], Handler]:
    """A handler that replays `responses` in order and records what it was asked.

    An `Exception` in the sequence is raised rather than returned, which is how a transport
    failure is expressed. The last entry repeats, so a test that only cares about the first
    attempt does not have to enumerate the rest.
    """
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        item = responses[min(len(seen) - 1, len(responses) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    return seen, handler


def ok(payload: Any = None, **headers: str) -> httpx.Response:
    return httpx.Response(200, json=payload if payload is not None else {}, headers=headers)


def query_of(request: httpx.Request) -> dict[str, str]:
    return dict(urllib.parse.parse_qsl(request.url.query.decode()))


def signature_is_valid(request: httpx.Request) -> bool:
    """Whether this request's own signature verifies over its own query string.

    The exchange's check, exactly: everything before `&signature=`, HMAC-SHA256 with the
    secret. Used instead of comparing two attempts' signatures to each other, because a
    retry served without a real backoff can land inside the same millisecond as the
    attempt before it -- the timestamps then match, so the signatures legitimately do too,
    and an inequality assertion would fail for a reason that has nothing to do with the
    client.
    """
    payload, _, signature = request.url.query.decode().rpartition("&signature=")
    expected = hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return signature == expected


def now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the retry backoff instead of serving it.

    `_request` sleeps 1 s, 2 s, 4 s, 8 s between attempts, so a retry test that actually
    waited would spend fifteen seconds observing a scheduling decision. `asyncio` is used
    for exactly one thing in `signed.py` -- that sleep -- so substituting the name is
    complete rather than partial, and the recorded delays are asserted on directly.
    """
    slept: list[float] = []

    class _Recorder:
        @staticmethod
        async def sleep(seconds: float) -> None:
            slept.append(seconds)

    monkeypatch.setattr(signed, "asyncio", _Recorder)
    return slept


class TestWhatGoesOnTheWire:
    @pytest.mark.asyncio
    async def test_the_signature_covers_the_query_string_exactly_as_sent(self) -> None:
        """Recomputed from the bytes the transport actually received.

        This is the assertion the exchange makes: it takes everything before
        `&signature=`, HMACs it with the secret, and compares. Anything that rebuilds the
        query after signing -- letting an HTTP client re-encode a dict, say -- fails here
        for the same reason it would fail there, and there the only diagnostic is
        `-1022 Signature for this request is not valid`.
        """
        seen, handler = recording(ok({"totalWalletBalance": "1000.00"}))

        async with client(handler) as signed_client:
            await signed_client.account()

        sent = seen[0].url.query.decode()
        payload, _, signature = sent.rpartition("&signature=")
        assert signature == hmac.new(
            API_SECRET.encode(), payload.encode(), hashlib.sha256
        ).hexdigest()

    @pytest.mark.asyncio
    async def test_every_signed_request_carries_a_timestamp_and_recv_window(
        self,
    ) -> None:
        """Both are mandatory on a USD-M signed endpoint, and the window is bounded.

        The timestamp is asserted to be the *local* clock, bracketed by readings taken
        either side of the call, because the alternative -- shifting it by measured drift
        -- would make signing work on a machine whose clock is wrong while every `recv_ms`
        that machine records stayed wrong.
        """
        seen, handler = recording(ok([]))

        before = now_ms()
        async with client(handler, recv_window_ms=4_000) as signed_client:
            await signed_client.open_orders("BTCUSDT")
        after = now_ms()

        params = query_of(seen[0])
        assert params["recvWindow"] == "4000"
        assert before <= int(params["timestamp"]) <= after
        assert params["symbol"] == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_the_key_travels_in_the_header_and_never_in_the_query(self) -> None:
        seen, handler = recording(ok({}))

        async with client(handler) as signed_client:
            await signed_client.account()

        assert seen[0].headers["X-MBX-APIKEY"] == API_KEY
        assert API_KEY not in seen[0].url.query.decode()

    @pytest.mark.asyncio
    async def test_a_listen_key_request_is_keyed_but_not_signed(self) -> None:
        """The three listen-key endpoints are USER_STREAM, not USER_DATA.

        The key alone authorises them, and appending a `timestamp` and `signature` they do
        not expect invites a `-1104` about parameters that were not read.
        """
        seen, handler = recording(ok({"listenKey": "abc123"}))

        async with client(handler) as signed_client:
            assert await signed_client.listen_key_create() == "abc123"

        request = seen[0]
        assert request.method == "POST"
        assert request.url.query == b""
        assert request.headers["X-MBX-APIKEY"] == API_KEY

    @pytest.mark.asyncio
    async def test_a_bool_is_rendered_the_way_binance_reads_it(self) -> None:
        """Python's `True` stringifies to `"True"`, which is not a boolean to this API.

        The failure is silent in the worst direction: `reduceOnly` ignored means an order
        that was meant only to close a position can open one.
        """
        seen, handler = recording(ok({}))

        async with client(handler) as signed_client:
            await signed_client.new_order(**ORDER, reduceOnly=True)

        assert query_of(seen[0])["reduceOnly"] == "true"

    @pytest.mark.asyncio
    async def test_a_float_quantity_is_refused_rather_than_rendered(self) -> None:
        """`0.07` is already the wrong number before it reaches the encoder."""
        _, handler = recording(ok({}))

        async with client(handler) as signed_client:
            with pytest.raises(TypeError, match="float"):
                await signed_client.new_order(
                    symbol="BTCUSDT", newClientOrderId="perplab-1", quantity=0.01
                )

    @pytest.mark.asyncio
    async def test_a_recv_window_past_the_exchange_cap_is_refused(self) -> None:
        """Widening the window to absorb clock drift hides the drift spec 11 wants reported."""
        with pytest.raises(ValueError, match="recv_window_ms must be"):
            SignedRestClient(
                BASE, KeySession(API_KEY, API_SECRET), recv_window_ms=MAX_RECV_WINDOW_MS + 1
            )


class TestTheRetryPolicy:
    """The one property that cannot be tested against a live exchange."""

    @pytest.mark.asyncio
    async def test_a_read_timeout_on_a_new_order_is_not_retried(
        self, no_backoff: list[float]
    ) -> None:
        """The request was sent. The answer was lost. The order may exist.

        Repeating it opens a second position on top of the first, and the account ends the
        day with twice the exposure the strategy asked for. So exactly one attempt is made
        and `OrderOutcomeUnknown` is raised naming the client order id, which is the handle
        `openOrders` and `userTrades` need to find out which of working / filled / rejected
        actually happened (spec 6.7).
        """
        seen, handler = recording(httpx.ReadTimeout("read timed out"))

        async with client(handler, max_attempts=5) as signed_client:
            with pytest.raises(OrderOutcomeUnknown) as excinfo:
                await signed_client.new_order(**ORDER)

        assert len(seen) == 1, "an order request was repeated after a read timeout"
        assert not no_backoff, "the client backed off to retry an order it must not repeat"

        message = str(excinfo.value)
        assert ORDER["newClientOrderId"] in message
        assert "Do NOT resubmit" in message
        assert excinfo.value.client_order_id == ORDER["newClientOrderId"]
        assert excinfo.value.symbol == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_a_connect_error_on_a_new_order_is_retried_with_the_same_client_id(
        self, no_backoff: list[float]
    ) -> None:
        """No connection was established, so no bytes reached the exchange.

        This is the only order failure a repeat is allowed, and the repeat has to carry the
        identical `newClientOrderId`: that is what makes it idempotent at the exchange
        rather than merely unlikely to be needed, and what lets reconciliation recognise
        the order afterwards whichever attempt actually landed. Each attempt must also be
        signed afresh over its own query: a client that cached the first URL would send a
        second request whose `timestamp` is a backoff behind its signature, and after
        enough retries that request arrives outside its own `recvWindow` and is rejected
        for a reason the log would blame on the network.
        """
        seen, handler = recording(
            httpx.ConnectError("connection refused"),
            ok({"orderId": 42, "clientOrderId": ORDER["newClientOrderId"]}),
        )

        async with client(handler, max_attempts=5) as signed_client:
            result = await signed_client.new_order(**ORDER)

        assert result["orderId"] == 42
        assert len(seen) == 2
        assert no_backoff == [1.0], "expected one 2**0 second backoff between the attempts"

        first, second = (query_of(r) for r in seen)
        assert first["newClientOrderId"] == second["newClientOrderId"] == "perplab-7f3a"
        assert first["quantity"] == second["quantity"] == "0.010"
        assert int(second["timestamp"]) >= int(first["timestamp"])
        assert all(signature_is_valid(request) for request in seen)

    @pytest.mark.asyncio
    async def test_a_5xx_on_a_new_order_is_an_unknown_outcome_not_a_rejection(
        self, no_backoff: list[float]
    ) -> None:
        """Binance documents a 5xx on an order as "execution status UNKNOWN".

        Reading it as a failure is the same double-position bug arriving through a
        different door, so it neither retries nor reports failure -- it hands the caller
        the id to reconcile with.
        """
        seen, handler = recording(httpx.Response(503, text="service unavailable"))

        async with client(handler, max_attempts=5) as signed_client:
            with pytest.raises(OrderOutcomeUnknown, match="HTTP 503"):
                await signed_client.new_order(**ORDER)

        assert len(seen) == 1
        assert not no_backoff

    @pytest.mark.asyncio
    async def test_a_4xx_rejection_on_a_new_order_is_an_ordinary_error(
        self, no_backoff: list[float]
    ) -> None:
        """A 4xx did not reach the matching engine, so the outcome is known: nothing.

        Reporting it as unknown would send the caller off to reconcile against an order
        that provably does not exist, and would train it to distrust the one signal that
        means something.
        """
        seen, handler = recording(
            httpx.Response(400, json={"code": -2019, "msg": "Margin is insufficient."})
        )

        async with client(handler, max_attempts=5) as signed_client:
            with pytest.raises(BinanceRestError) as excinfo:
                await signed_client.new_order(**ORDER)

        assert not isinstance(excinfo.value, OrderOutcomeUnknown)
        assert excinfo.value.code == -2019
        assert len(seen) == 1
        assert not no_backoff

    @pytest.mark.asyncio
    async def test_a_429_on_a_new_order_is_a_refusal_rather_than_an_unknown_outcome(
        self, no_backoff: list[float]
    ) -> None:
        """Rate limiting happens before the matching engine, and it is not ambiguous."""
        seen, handler = recording(httpx.Response(429, json={"code": -1003, "msg": "Too many"}))

        async with client(handler, max_attempts=5) as signed_client:
            with pytest.raises(BinanceRestError) as excinfo:
                await signed_client.new_order(**ORDER)

        assert not isinstance(excinfo.value, OrderOutcomeUnknown)
        assert len(seen) == 1

    @pytest.mark.asyncio
    async def test_a_read_timeout_on_a_read_endpoint_is_retried(
        self, no_backoff: list[float]
    ) -> None:
        """The whole reason this is a separate client from `PublicRestClient`.

        Asking twice what the account balance is costs a little weight and nothing else, so
        the ordinary retry loop applies wherever a repeat cannot open a position.
        """
        seen, handler = recording(
            httpx.ReadTimeout("read timed out"), ok({"totalWalletBalance": "1000.00"})
        )

        async with client(handler, max_attempts=5) as signed_client:
            payload = await signed_client.account()

        assert payload["totalWalletBalance"] == "1000.00"
        assert len(seen) == 2
        assert no_backoff == [1.0]

    @pytest.mark.asyncio
    async def test_exhausting_the_attempts_reports_how_many_were_made(
        self, no_backoff: list[float]
    ) -> None:
        """Three attempts means two waits: 2**0 + 2**1 = 1 s then 2 s."""
        seen, handler = recording(httpx.Response(502, text="bad gateway"))

        async with client(handler, max_attempts=3) as signed_client:
            with pytest.raises(BinanceRestError, match="exhausted 3 attempts"):
                await signed_client.account()

        assert len(seen) == 3
        assert no_backoff == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_a_signature_is_redacted_out_of_an_exception_message(self) -> None:
        """Exception text travels into the Feed, the run record and pasted issue reports.

        A signature is not the secret and cannot be reversed into it, but it authorises one
        request until `recvWindow` elapses, and removing it costs nothing.
        """
        _, handler = recording(
            httpx.Response(
                400,
                json={"code": -1022, "msg": "bad signature=deadbeefcafe for this request"},
            )
        )

        async with client(handler) as signed_client:
            with pytest.raises(BinanceRestError) as excinfo:
                await signed_client.account()

        assert "signature=<redacted>" in str(excinfo.value)
        assert "deadbeefcafe" not in str(excinfo.value)


class TestTheRateBudget:
    """Refusing before the ceiling, because a 418 IP ban outlasts the trading session."""

    @pytest.mark.asyncio
    async def test_the_counters_are_read_off_every_response(self) -> None:
        """These are the exchange's own figures, and they are authoritative in a way a
        local counter is not: they include failed requests, retries, and anything else
        sharing this IP.
        """
        _, handler = recording(
            ok(
                {},
                **{
                    "X-MBX-USED-WEIGHT-1M": "137",
                    "X-MBX-ORDER-COUNT-10S": "3",
                    "X-MBX-ORDER-COUNT-1M": "19",
                },
            )
        )

        async with client(handler) as signed_client:
            await signed_client.new_order(**ORDER)
            budget = signed_client.budget

        assert budget.used_weight_1m == 137
        assert budget.order_count_10s == 3
        assert budget.order_count_1m == 19

    @pytest.mark.asyncio
    async def test_a_request_at_the_weight_ceiling_is_refused_before_it_is_sent(
        self,
    ) -> None:
        """Refused, not queued and not sent. The point is that nothing goes out."""
        seen, handler = recording(ok({}))
        budget = RateBudget(used_weight_1m=WEIGHT_CEILING, weight_observed_ms=now_ms())

        async with client(handler, budget=budget) as signed_client:
            with pytest.raises(RateBudgetExceeded, match="Nothing was sent"):
                await signed_client.account()

        assert seen == []

    @pytest.mark.asyncio
    async def test_a_reading_older_than_its_window_does_not_pin_the_client_shut(
        self,
    ) -> None:
        """The counters reset on a window boundary we never see.

        A budget that refused forever on the strength of one high reading would need a
        process restart before the account could trade again -- which, mid-session with
        positions open, is worse than occasionally sending into a limit we could have
        predicted.
        """
        seen, handler = recording(ok({}))
        budget = RateBudget(
            used_weight_1m=WEIGHT_CEILING, weight_observed_ms=now_ms() - 61_000
        )

        async with client(handler, budget=budget) as signed_client:
            await signed_client.account()

        assert len(seen) == 1

    @pytest.mark.asyncio
    async def test_an_unparseable_header_leaves_the_previous_reading_standing(
        self,
    ) -> None:
        """A header we cannot read is not a reason to fail a request that already worked.

        It does mean the budget keeps its last reading, which ages out on its own rather
        than pinning the client to a number nobody can check.
        """
        _, handler = recording(
            ok({}, **{"X-MBX-USED-WEIGHT-1M": "40"}),
            ok({}, **{"X-MBX-USED-WEIGHT-1M": "not-a-number"}),
        )

        async with client(handler) as signed_client:
            await signed_client.account()
            await signed_client.account()
            assert signed_client.budget.used_weight_1m == 40


class TestClockAndCallSiteRefusals:
    @pytest.mark.asyncio
    async def test_drift_is_measured_and_reported_and_never_corrected(self) -> None:
        """Spec 11 wants a Feed warning past 1 s of drift.

        The server is made to answer with a clock 5 000 ms behind ours, so `drift_ms` lands
        near +5 000 -- comfortably past the 1 000 ms threshold and far enough from it that
        the round trip cannot account for the verdict. The signed request that follows must
        still carry the *local* time: shifting our timestamps would make signing work on a
        machine whose clock is wrong, and the same clock stamps every `recv_ms` the
        collector records.
        """
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if request.url.path == "/fapi/v1/time":
                return httpx.Response(200, json={"serverTime": now_ms() - 5_000})
            return httpx.Response(200, json={})

        async with client(handler) as signed_client:
            await signed_client.server_time_ms()
            drift = signed_client.drift_ms
            warning = signed_client.clock_drift_warning

            before = now_ms()
            await signed_client.account()
            after = now_ms()

        assert drift is not None and drift > MAX_CLOCK_DRIFT_MS
        assert 4_000 < drift < 6_000
        assert warning is not None
        assert "ahead of" in warning and "will not shift timestamps" in warning
        assert before <= int(query_of(seen[-1])["timestamp"]) <= after

    @pytest.mark.asyncio
    async def test_a_good_clock_raises_no_warning(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"serverTime": now_ms()})

        async with client(handler) as signed_client:
            await signed_client.server_time_ms()
            assert signed_client.clock_drift_warning is None

    @pytest.mark.asyncio
    async def test_the_time_endpoint_is_neither_signed_nor_keyed(self) -> None:
        """It has to work before a credential is known to be good."""
        seen, handler = recording(httpx.Response(200, json={"serverTime": now_ms()}))

        async with client(handler) as signed_client:
            await signed_client.server_time_ms()

        assert seen[0].url.query == b""
        assert "X-MBX-APIKEY" not in seen[0].headers

    @pytest.mark.asyncio
    async def test_new_order_refuses_to_mint_its_own_client_order_id(self) -> None:
        """An id minted here would be unknown to the caller at the one moment it is needed:
        when the request times out and something has to go and find out whether the order
        exists.
        """
        _, handler = recording(ok({}))

        async with client(handler) as signed_client:
            with pytest.raises(ValueError, match="newClientOrderId"):
                await signed_client.new_order(symbol="BTCUSDT", quantity="0.01")

    @pytest.mark.asyncio
    async def test_cancel_order_refuses_both_identifiers_and_refuses_neither(self) -> None:
        """Two identifiers that disagree name two different orders.

        Picking one silently cancels an order nobody asked about, which is why this refuses
        rather than resolves.
        """
        _, handler = recording(ok({}))

        async with client(handler) as signed_client:
            with pytest.raises(ValueError, match="exactly one"):
                await signed_client.cancel_order("BTCUSDT", order_id=1, client_order_id="a")
            with pytest.raises(ValueError, match="exactly one"):
                await signed_client.cancel_order("BTCUSDT")

    @pytest.mark.asyncio
    async def test_validate_records_the_balance_on_the_session(self) -> None:
        """Spec 11's key-entry check: one lightweight signed request, and the two things the
        UI is allowed to show.
        """
        _, handler = recording(ok({"totalWalletBalance": "1234.56789012"}))
        keys = KeySession(API_KEY, API_SECRET)

        async with client(handler, keys=keys) as signed_client:
            balance = await signed_client.validate(alias="main")

        assert str(balance) == "1234.56789012"
        assert keys.alias == "main"
        assert keys.to_json()["balance"] == "1234.56789012"

    @pytest.mark.asyncio
    async def test_a_wiped_session_stops_the_client_dead(self) -> None:
        """The key is read from the session on every request, never cached in a header.

        That is what makes step 4 of the kill switch (spec 7) mean something: a client
        holding its own copy would keep trading happily after the keys were "wiped".
        """
        seen, handler = recording(ok({}))
        keys = KeySession(API_KEY, API_SECRET)

        async with client(handler, keys=keys) as signed_client:
            keys.wipe()
            with pytest.raises(Exception, match="wiped"):
                await signed_client.account()

        assert seen == []
