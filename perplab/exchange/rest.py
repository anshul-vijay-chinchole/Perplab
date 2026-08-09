"""Public Binance USD-M REST client.

Unsigned endpoints only. Signing, the in-memory key session, and everything else in spec
11 belong to the live-trading phase and are deliberately absent here -- Phase 1b records
public market data and needs no credentials at all, so there is no reason for this
process to be able to hold a key.

This client carries more weight than originally planned. `fstream.binance.com` was found
on 2026-08-02 to serve only *raw* event streams (`@trade`, `@depth`, `@bookTicker`) and to
silently suppress every aggregated or computed one (`@aggTrade`, `@markPrice`, `@kline`,
`@ticker`, `!forceOrder`) -- the server ACKs the subscription, lists it back under
`LIST_SUBSCRIPTIONS`, and then sends nothing. REST is therefore the source for mark price
and aggregate trades, not a fallback. See docs/DATA_AVAILABILITY.md finding F4.

`leverageBracket`'s documented endpoint is signed (verified 2026-08-01: unsigned returns
HTTP 401 `-2014`), which blocked the Phase 0 exit criterion. The same table is served
without credentials by the endpoint behind Binance's own public leverage-bracket page; see
`BRACKETS_PUBLIC_URL`. Finding F3 is closed by it.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx


def _retry_after_s(response: httpx.Response, *, default: float) -> float:
    """The `Retry-After` header in seconds, or `default` when absent or unreadable.

    Binance attaches it to 429 and 418 responses; ignoring it was M12's finding -- our
    own exponential schedule retried inside the window the exchange had named, which on
    a 418 extends the ban being waited out.
    """
    raw = response.headers.get("Retry-After")
    if raw is None:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        return default

__all__ = [
    "BinanceRestError",
    "PublicRestClient",
    "PRODUCTION_BASE",
    "TESTNET_BASE",
    "BRACKETS_PUBLIC_URL",
    "AGG_TRADES_MAX_LIMIT",
]

PRODUCTION_BASE = "https://fapi.binance.com"
TESTNET_BASE = "https://testnet.binancefuture.com"

BRACKETS_PUBLIC_URL = (
    "https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets"
)
"""Unsigned source for the leverage bracket table (spec 3.6, Phase 0 exit criterion).

`GET /fapi/v1/leverageBracket` is documented as USER_DATA and rejects unsigned callers, so
snapshotting brackets appeared to require API keys -- which would have meant this process
holding a credential purely to record public reference data. It does not: this endpoint
backs Binance's own public leverage-bracket page and returns the full table for every
symbol without authentication. Verified 2026-08-02: HTTP 200, 987 symbols, and BTCUSDT's
twelve tiers matching the documented schedule with internally consistent `cum` values.

It is not part of the documented API, so it is treated as what it is -- a public page's
backing endpoint. It may move. `snapshot_leverage_brackets` therefore reports failure
loudly rather than writing a partial snapshot, and the payload is stored raw so that a
change in shape is diagnosable from the archive rather than only from a stack trace.
"""

AGG_TRADES_MAX_LIMIT = 1000
"""Binance's per-request cap on `/fapi/v1/aggTrades`.

Named because the poller's gaplessness argument depends on it: a page that comes back
exactly this full may have been truncated, so the poller must immediately request the next
page from `last_id + 1` rather than waiting for its next tick.
"""


class BinanceRestError(RuntimeError):
    """A non-transient REST failure, carrying Binance's own error code where present."""

    def __init__(self, status: int, code: int | None, message: str) -> None:
        super().__init__(f"HTTP {status}" + (f" [{code}]" if code is not None else "") + f": {message}")
        self.status = status
        self.code = code


class PublicRestClient:
    """Async client for unsigned USD-M endpoints.

    Retries only on transient conditions -- network errors, 5xx, and 429/418 rate limits.
    A 4xx that is not a rate limit means the request itself is wrong, and retrying it
    burns weight against the limit without any chance of succeeding.
    """

    def __init__(
        self,
        base_url: str = PRODUCTION_BASE,
        *,
        timeout: float = 30.0,
        max_attempts: int = 5,
        budget: Any | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._max_attempts = max_attempts
        self._budget = budget
        """A `signed.RateBudget`, or anything with its `check`/`observe` shape (H9).

        The per-IP weight limit is one pool shared by every client on the machine, and
        for one release only the *signed* client counted against it -- while the live
        feed's own pollers (aggTrades at weight 20 per call per second, klines,
        premiumIndex) spent freely from the same pool. Two symbols of trade polling alone
        reached the ceiling, and the resulting 429 -> 418 -> ban arrived with positions
        open: the exact outcome the budget exists to prevent, delivered through the
        unbudgeted client. The live path passes the signed client's own budget in, so
        both clients draw against one measured pool; standalone collectors may pass none
        (they are the only tenant of their IP's weight while running unattended).
        Duck-typed to avoid a circular import -- `signed` already imports from here."""
        self._banned_until: float | None = None
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=timeout,
            headers={"User-Agent": "perplab/0.1"},
        )

    async def __aenter__(self) -> PublicRestClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return (await self._request(path, params)).json()

    async def _get_text(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        """Fetch a body as undecoded text.

        Needed wherever the response must be parsed with `json.loads(...,
        parse_float=Decimal)`. Binance sends bracket rates as bare JSON *numbers*, so
        letting `httpx` call `.json()` for us would convert `0.0065` to a double before any
        of our code sees it, and the precision is unrecoverable after that
        (`perplab.core.margin.brackets_from_payload` explains what that costs). Returning
        text keeps the choice of float parser with the caller.

        It is also what makes a snapshot byte-faithful: `json.dumps(json.loads(text))` is
        not the bytes the exchange served, and a reference snapshot exists precisely to be
        trusted over our own re-encoding of it.
        """
        return (await self._request(path, params, headers)).text

    async def _request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        # A recorded ban fails fast and locally (M12). A 418 means the IP is banned for
        # the window the exchange named, and every request sent inside it *extends* it --
        # so a poller loop that kept calling through its own cadence turned a two-minute
        # ban into an open-ended one. Refusing here costs the caller one exception per
        # poll and the venue nothing.
        if self._banned_until is not None:
            remaining = self._banned_until - time.monotonic()
            if remaining > 0:
                raise BinanceRestError(
                    418,
                    None,
                    f"IP ban in force for another {remaining:.0f}s; refusing to extend "
                    f"it by asking again",
                )
            self._banned_until = None

        last: Exception | None = None
        for attempt in range(self._max_attempts):
            if self._budget is not None:
                # Raises rather than sends when the shared pool is nearly spent (H9) --
                # the same pre-flight check the signed client makes, against the same
                # instance in the live path, so a poller cannot spend the weight an
                # order needs.
                self._budget.check(is_order=False)
            try:
                response = await self._client.get(path, params=params, headers=headers)
            except httpx.HTTPError as exc:
                last = exc
            else:
                if self._budget is not None:
                    self._budget.observe(response.headers)
                if response.status_code == 200:
                    return response

                code, message = self._decode_error(response)

                if response.status_code == 418:
                    # Banned. Retrying extends it; record the window and stop asking.
                    self._banned_until = time.monotonic() + _retry_after_s(
                        response, default=120.0
                    )
                    raise BinanceRestError(response.status_code, code, message)
                if response.status_code == 429 or response.status_code >= 500:
                    last = BinanceRestError(response.status_code, code, message)
                    if response.status_code == 429 and attempt < self._max_attempts - 1:
                        # The exchange says how long to stay away; believe it rather
                        # than our own schedule, which is what kept 429s escalating.
                        await asyncio.sleep(
                            max(2.0**attempt, _retry_after_s(response, default=0.0))
                        )
                        continue
                else:
                    raise BinanceRestError(response.status_code, code, message)

            # Exponential backoff: 1s, 2s, 4s, 8s.
            if attempt < self._max_attempts - 1:
                await asyncio.sleep(2.0**attempt)

        raise BinanceRestError(0, None, f"exhausted {self._max_attempts} attempts: {last}")

    @staticmethod
    def _decode_error(response: httpx.Response) -> tuple[int | None, str]:
        try:
            payload = response.json()
            return payload.get("code"), str(payload.get("msg", response.text[:200]))
        except ValueError:
            return None, response.text[:200]

    async def ping(self) -> None:
        """Connectivity check. Cheap (weight 1) and a clean preflight signal."""
        await self._get("/fapi/v1/ping")

    async def server_time_ms(self) -> int:
        """Binance's clock, for drift detection.

        Signed requests are rejected when local time drifts past `recvWindow`, and spec 11
        requires a Feed warning before drift causes failures rather than after. Nothing in
        Phase 1b signs anything, but drift also silently skews the `recv_ms` timestamps we
        record, so it is worth surfacing from day one.
        """
        return int((await self._get("/fapi/v1/time"))["serverTime"])

    async def exchange_info(self) -> dict[str, Any]:
        """Full `exchangeInfo` payload -- symbol filters, precisions, funding intervals.

        Returned raw and unparsed so the snapshot on disk is byte-faithful to what the
        exchange served. Parsing happens in `exchange.filters`; a snapshot that had
        already been through our parser would be worth less on the day the parser is
        found to be wrong.
        """
        return await self._get("/fapi/v1/exchangeInfo")

    async def leverage_brackets_text(self) -> str:
        """Full public leverage bracket table, as undecoded JSON text (spec 3.6).

        Text rather than a parsed object for two reasons that both matter: the caller must
        parse with `parse_float=Decimal` (the rates are bare JSON numbers), and the
        snapshot written to `userdata/reference/` must be the exchange's own bytes.

        Sends a browser `User-Agent`. This endpoint backs a public web page rather than
        the documented API, and the default `perplab/0.1` agent is not something it is
        obliged to serve; pinning a plausible one here keeps a reference snapshot from
        failing for a reason that has nothing to do with the data.
        """
        return await self._get_text(
            BRACKETS_PUBLIC_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) perplab/0.1"
                ),
                "Accept": "application/json",
            },
        )

    async def premium_index(self, symbol: str) -> dict[str, Any]:
        """Mark price, index price and current funding rate for one symbol (spec 3.4).

        This is the *only* working source for mark price on this endpoint: every
        `@markPrice` WebSocket variant is silently suppressed (finding F4). Weight 1, and
        the server stamps a fresh `time` every second, so polling at 1 Hz reproduces
        exactly what `@markPrice@1s` would have delivered.

        Spec 3.4 forbids computing or interpolating a mark price. Nothing here does: the
        value is recorded as served, and the engine carries the last observation forward
        between samples rather than filling between them.
        """
        return await self._get("/fapi/v1/premiumIndex", params={"symbol": symbol})

    async def open_interest(self, symbol: str) -> dict[str, Any]:
        """Present open interest for one symbol.

        Weight 1. Returns `{"openInterest": "...", "symbol": ..., "time": ...}` -- the
        *current* figure only. Binance publishes the historical series through
        `/futures/data/openInterestHist`, which is capped at 30 days, so anything older
        than that comes from the daily `metrics` archive and anything newer than the
        archive's edge has to be recorded as it happens. That is why this is polled rather
        than backfilled.

        The endpoint carries **no** long/short ratios. `metrics` rows written from here
        therefore have those columns null, which is the truthful encoding: not measured, as
        opposed to measured at zero.
        """
        return await self._get("/fapi/v1/openInterest", params={"symbol": symbol})

    async def klines(
        self, symbol: str, *, interval: str = "1m", limit: int = 3
    ) -> list[list[Any]]:
        """Recent candlesticks, oldest first. **The last row is the bar in progress.**

        This is the live session's bar source on *both* endpoints, and REST rather than
        `@kline_1m` deliberately. Production suppresses the kline stream (finding F4) while
        testnet serves it, so a feed that preferred the stream would give a session recorded
        on one venue a structurally different bar series from one recorded on the other --
        for the single series every indicator and every `on_bar` is built on. One source,
        one shape, at the cost of a couple of seconds' latency after each close.

        Weight 1 at these limits. Callers must drop the final row: it is the open bar, and
        handing it to a strategy is precisely the look-ahead spec 6.2 makes structurally
        impossible in a backtest.
        """
        return await self._get(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )

    async def funding_rate(self, symbol: str, *, limit: int = 2) -> list[dict[str, Any]]:
        """Realised funding settlements, oldest first.

        Spec 3.5 rule 2 wants the actual rates at the actual timestamps, and R17 records
        that Binance runs different intervals on different symbols and has changed the
        interval on existing ones -- so a live session reads the settlements that happened
        rather than projecting an eight-hour schedule.
        """
        return await self._get(
            "/fapi/v1/fundingRate", params={"symbol": symbol, "limit": limit}
        )

    async def agg_trades(
        self,
        symbol: str,
        *,
        from_id: int | None = None,
        limit: int = AGG_TRADES_MAX_LIMIT,
    ) -> list[dict[str, Any]]:
        """Aggregate trades, oldest first, optionally starting at a known id.

        `from_id` is what makes REST collection *gapless rather than probably-gapless*.
        Aggregate trade ids are dense and contiguous (verified 2026-08-02 across a page
        boundary), so a poller that always asks from `last_seen + 1` either receives the
        next trade or receives nothing -- it can never skip one unnoticed the way a
        dropped WebSocket frame can. An id sequence that is checkable after the fact is
        worth more here than the latency a push stream would have saved.

        A `from_id` past the head returns HTTP 200 with an empty list rather than an
        error, which is the poller's "caught up" signal.
        """
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id is not None:
            params["fromId"] = from_id
        return await self._get("/fapi/v1/aggTrades", params=params)
