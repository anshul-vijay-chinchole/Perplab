"""Macro and cross-asset collectors (Phase 11).

Two external sources, both polled on the existing `RestPoller` pattern so that backoff,
failure-as-data, DISCONNECT/RECONNECT events and the drift-free cadence are inherited
rather than reimplemented:

| dataset       | source                              | what it is                        |
|---------------|-------------------------------------|-----------------------------------|
| `macroGlobal` | CoinGecko `/api/v3/global`          | BTC dominance, total market cap    |
| `macroFx`     | Yahoo Finance chart API, `DX-Y.NYB` | the ICE US Dollar Index (DXY)      |

Both verified reachable without an API key on 2026-08-03. That verification is the point
rather than a footnote: spec review finding R1 exists because a whole phase was planned on
an assumption about an endpoint nobody had called, and *"cheap to check, expensive to
assume"* is the rule that came out of it.

## Three properties that decide whether this data can be trusted

**The source's own timestamp is the key, never poll time.** CoinGecko publishes
`updated_at` and Yahoo publishes `regularMarketTime`; both are the instant the *provider*
believes the value holds for. Keying on poll time would manufacture a fresh observation
every hour out of a snapshot that had not changed, and a strategy reading the series could
not tell a genuine move from a re-poll. `recv_ms` records when we asked, which is the
honest place for our own clock.

**A repeated source timestamp is dropped, not stored.** DXY does not trade at the weekend
and CoinGecko's snapshot updates every few minutes; polling hourly across either produces
runs of identical `ts_ms`. Storing them would inflate row counts, make the gap detector
believe coverage exists where the market was closed, and let a `count(*)` masquerade as a
measure of activity.

**Nothing is interpolated, and the weekend is not filled.** Yahoo's own daily close array
was observed on 2026-08-03 to contain a literal `null` mid-series. The rule the whole
platform runs on (spec 4.5) applies with no exception here: a missing observation stays
missing, and `strategy.macro` carries the *age* of the value it hands back so a strategy
can refuse a stale one on its own terms.

## Exactness

CoinGecko serves JSON *numbers*, not decimal strings. The body is parsed with
`parse_float=str`, which hands back the digits exactly as transmitted, and those strings
go into the platform's own `to_scaled` -- the same parser every price takes. Routing them
through a binary float first would discard the exactness the scaled-integer seam exists to
protect, and `decimal` is deliberately not imported here: `tests/unit/test_money.py`
confines it to the accounting layer, and this is market-data code.

See `schemas.MACRO_USD_UNIT` for why the USD aggregates are stored in whole dollars rather
than at the 10^8 price scale.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Mapping

import httpx

from perplab.core.money import to_scaled
from perplab.core.types import CollectorEventKind
from perplab.data.rest_poller import EventSink, RestPoller, RowSink, latest_recorded_ts
from perplab.data.schemas import SCHEMAS
from perplab.data.writer import ParquetBufferedWriter

__all__ = [
    "MACRO_DATASETS",
    "COINGECKO_URL",
    "YAHOO_CHART_URL",
    "DXY_SYMBOL",
    "CoinGeckoGlobalPoller",
    "DollarIndexPoller",
    "MacroService",
    "parse_global_payload",
    "parse_yahoo_payload",
]

MACRO_DATASETS = frozenset({"macroGlobal", "macroFx"})
"""Polled, non-Binance datasets. Listed for the same reason `REST_DATASETS` is: a
WebSocket outage explains silence on the market streams and says nothing about these."""

COINGECKO_URL = "https://api.coingecko.com/api/v3/global"

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

DXY_SYMBOL = "DX-Y.NYB"
"""ICE's US Dollar Index on Yahoo. **The index itself, not a proxy.**

`DX=F` (the front-month future) returns 404 on this endpoint, and a basket rebuilt from
spot FX rates would be a number this platform invented and then labelled DXY -- the exact
species of quiet wrongness spec 1.4 forbids. If this provider disappears, the honest move
is to record a different provider in the `source` column, not to compute a substitute.
"""

DEFAULT_GLOBAL_INTERVAL_S = 3600.0
"""Hourly. CoinGecko's free tier is a handful of calls per minute and this snapshot moves
on a scale of hours; polling faster would spend someone else's rate budget to re-read a
number that had not changed."""

DEFAULT_FX_INTERVAL_S = 3600.0
"""Hourly. DXY prints continuously through the FX week and not at all at the weekend, so
an hourly sample is dense relative to how the series is used (a regime input) and polite
to an undocumented endpoint."""

_USER_AGENT = "perplab/0.1 (personal research; contact: local)"
"""Yahoo's chart endpoint refuses a default python-httpx agent. Stated plainly rather than
disguised: this is an undocumented endpoint being used politely at one request an hour."""


def _truncate(text: str, places: int) -> str:
    """Cut a decimal string to `places` fractional digits, without arithmetic.

    `to_scaled` refuses more than eight decimal places rather than rounding -- the right
    call for exchange data, where an unexpected precision change should be loud. CoinGecko
    publishes a float repr with seventeen, so the excess is truncated here, deliberately
    and visibly, before the parser sees it. Truncation rather than rounding because these
    are *level* readings a strategy later differences: round-half-even would let a series
    tick up and down by one unit on an unchanged underlying value.
    """
    if "e" in text or "E" in text:
        raise ValueError(f"macro value in scientific notation is not handled: {text!r}")
    if "." not in text:
        return text
    whole, _, fraction = text.partition(".")
    return f"{whole}.{fraction[:places]}" if places else whole


def _scaled(text: str) -> int:
    """A decimal string -> int scaled by 10^8, through the platform's own parser."""
    return to_scaled(_truncate(text, 8))


def _scaled_percent_as_fraction(text: str) -> int:
    """A percentage string -> a *fraction* scaled by 10^8, exactly.

    The platform's convention is fractions (`max_drawdown_pct: 0.15` is fifteen percent),
    so nothing downstream has to remember which of two conventions this column follows.

    `to_scaled` gives percent x 10^8; integer-dividing by 100 gives fraction x 10^8, with
    no float anywhere. The eight places `to_scaled` allows on the *percentage* are the
    scale's full capacity for the fraction, so nothing is lost that could be stored.
    """
    return to_scaled(_truncate(text, 8)) // 100


def _whole_units(text: str) -> int:
    """A decimal string -> its integer part, exactly (see `schemas.MACRO_USD_UNIT`).

    String truncation rather than `int(float(text))`: the float is inexact at the twelfth
    significant figure, which is precisely where a trillion-dollar market cap lives.
    """
    value = int(_truncate(text, 0) or "0")
    if not -(2**63) <= value < 2**63:
        raise ValueError(f"macro USD aggregate out of int64 range: {text}")
    return value


_EPOCH_SECONDS_MIN = 946_684_800
"""2000-01-01T00:00:00Z. Nothing this module polls existed before the millennium."""

_EPOCH_SECONDS_MAX = 4_102_444_800
"""2100-01-01T00:00:00Z. A timestamp past it is a unit error, not a date."""


def _epoch_seconds_to_ms(value: int | str, field: str) -> int:
    """Convert a provider's epoch-*seconds* stamp to milliseconds, refusing other units.

    The `* 1000` below encodes an assumption -- that the provider publishes seconds --
    and an assumption multiplied by a thousand fails in the worst available way: a
    provider that switches to milliseconds (or microseconds, both exist in the wild)
    would file every row under `year=57046`, a partition no query's date predicate will
    ever visit, so collection would look perfectly healthy while writing rows that are
    invisible forever (finding M30). A century-wide sanity window costs nothing and turns
    that silent misfiling into a loud refusal naming the value and the unit it appears to
    be in. The window is generous on purpose: it exists to catch unit errors (three
    orders of magnitude), not to argue about clock skew.
    """
    seconds = int(_truncate(str(value), 0))
    if not _EPOCH_SECONDS_MIN <= seconds < _EPOCH_SECONDS_MAX:
        raise ValueError(
            f"{field} = {value!r} is not plausible epoch seconds (outside 2000-01-01 .. "
            f"2100-01-01). The provider likely changed its timestamp unit -- epoch "
            f"milliseconds divided into this parser would land rows in year "
            f"{1970 + seconds // 31_556_952}, invisible to every dated query -- so this "
            f"refuses rather than misfiling the row."
        )
    return seconds * 1000


def _number(payload: Mapping[str, Any], *path: str) -> str:
    """Pull a number out of a `parse_float=str` payload as its transmitted digits."""
    node: Any = payload
    for key in path:
        if not isinstance(node, Mapping) or key not in node:
            raise ValueError(f"macro payload has no {'.'.join(path)}")
        node = node[key]
    if isinstance(node, str):
        return node
    if isinstance(node, bool):
        raise ValueError(f"{'.'.join(path)} is a boolean, not a number")
    if isinstance(node, int):
        return str(node)
    # A float here means the body was parsed without `parse_float=str`, which would
    # silently cost exactness on every row. Refused loudly rather than accepted.
    raise ValueError(
        f"{'.'.join(path)} arrived as {type(node).__name__}; macro payloads must be "
        f"parsed with parse_float=str so the transmitted digits survive"
    )


def parse_global_payload(body: str, *, recv_ms: int) -> list[dict[str, Any]]:
    """CoinGecko `/global` → at most one `macroGlobal` row.

    Pure and separately testable: everything network-shaped stays in the poller, so the
    parsing rules that decide what lands on disk can be exercised against a captured body.
    """
    parsed = json.loads(body, parse_float=str)
    data = parsed.get("data") if isinstance(parsed, Mapping) else None
    if not isinstance(data, Mapping):
        raise ValueError("CoinGecko /global returned no `data` object")

    updated_at = data.get("updated_at")
    if isinstance(updated_at, bool) or not isinstance(updated_at, (int, str)):
        raise ValueError("CoinGecko /global returned no usable `updated_at`")
    ts_ms = _epoch_seconds_to_ms(updated_at, "CoinGecko /global updated_at")

    dominance_btc = _scaled_percent_as_fraction(
        _number(data, "market_cap_percentage", "btc")
    )
    try:
        dominance_eth = _scaled_percent_as_fraction(
            _number(data, "market_cap_percentage", "eth")
        )
    except ValueError:
        # ETH's line is not load-bearing for anything here; a payload without it should
        # still yield the BTC dominance the phase is actually about.
        dominance_eth = 0

    return [
        {
            "ts_ms": ts_ms,
            "recv_ms": recv_ms,
            "btc_dominance": dominance_btc,
            "eth_dominance": dominance_eth,
            "total_market_cap_usd": _whole_units(
                _number(data, "total_market_cap", "usd")
            ),
            "total_volume_usd": _whole_units(_number(data, "total_volume", "usd")),
            "active_cryptocurrencies": int(data.get("active_cryptocurrencies") or 0),
            "markets": int(data.get("markets") or 0),
        }
    ]


def parse_yahoo_payload(
    body: str, *, recv_ms: int, series: str, source: str
) -> list[dict[str, Any]]:
    """Yahoo chart JSON → at most one `macroFx` row, from `meta.regularMarketPrice`.

    The *meta* quote rather than the `indicators.quote[0].close` array, deliberately. The
    array is the historical bar series and was observed to contain a literal `null` for a
    session; the meta block is the provider's current reading with its own timestamp,
    which is the one thing a level series polled hourly actually needs. Backfilling the
    array is a separate job and would have to skip those nulls rather than fill them.
    """
    parsed = json.loads(body, parse_float=str)
    chart = parsed.get("chart") if isinstance(parsed, Mapping) else None
    if not isinstance(chart, Mapping):
        raise ValueError("Yahoo chart response has no `chart` object")
    if chart.get("error"):
        raise ValueError(f"Yahoo chart error: {chart['error']}")
    results = chart.get("result")
    if not isinstance(results, list) or not results:
        raise ValueError("Yahoo chart response carried no result")
    meta = results[0].get("meta") if isinstance(results[0], Mapping) else None
    if not isinstance(meta, Mapping):
        raise ValueError("Yahoo chart result has no `meta`")

    market_time = meta.get("regularMarketTime")
    if isinstance(market_time, bool) or not isinstance(market_time, (int, str)):
        raise ValueError("Yahoo chart meta has no usable `regularMarketTime`")

    return [
        {
            "ts_ms": _epoch_seconds_to_ms(market_time, "Yahoo chart regularMarketTime"),
            "recv_ms": recv_ms,
            "series": series,
            "value": _scaled(_number(meta, "regularMarketPrice")),
            "source": source,
        }
    ]


def _now_ms() -> int:
    return int(time.time() * 1000)


class _MacroPoller(RestPoller):
    """Shared plumbing: one HTTP GET, one parse, and the repeated-timestamp guard.

    The guard lives here rather than in each subclass because it is the property that
    makes both series meaningful, and a source that forgot it would look identical to one
    that did not -- until someone counted rows and believed the count.
    """

    def __init__(
        self,
        dataset: str,
        url: str,
        *,
        interval_s: float,
        on_rows: RowSink,
        on_event: EventSink,
        client: httpx.AsyncClient | None = None,
        timeout_s: float = 20.0,
        resume_from_ms: int | None = None,
    ) -> None:
        super().__init__(dataset, interval_s=interval_s, on_rows=on_rows, on_event=on_event)
        self.url = url
        self._timeout_s = timeout_s
        self._client = client
        self._owns_client = client is None
        # Seeded from the lake by `MacroService` (finding H26). The hourly cadence makes
        # the in-memory-only cursor *worse* here than for the 1 Hz pollers: CoinGecko's
        # snapshot holds for minutes and DXY for a whole weekend, so a service restart
        # inside that window re-recorded an `updated_at` the previous run had already
        # flushed -- a duplicate `ts_ms` in exactly the series whose row count the
        # repeated-timestamp guard exists to keep honest.
        self._last_ts_ms: int | None = resume_from_ms

    def parse(self, body: str, *, recv_ms: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch(self) -> list[dict[str, Any]]:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout_s, headers={"User-Agent": _USER_AGENT}
            )
        response = await self._client.get(self.url)
        response.raise_for_status()
        rows = self.parse(response.text, recv_ms=_now_ms())

        # A poll that returns nothing fresh is neither an error nor silence -- it is the
        # provider not having moved, and the absence of a row is already the honest record
        # of that. Emitting an event per unchanged hour would drown the collector's log in
        # a non-event.
        fresh: list[dict[str, Any]] = []
        for row in rows:
            if self._last_ts_ms is not None and row["ts_ms"] <= self._last_ts_ms:
                continue
            fresh.append(row)
            self._last_ts_ms = row["ts_ms"]
        return fresh

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


class CoinGeckoGlobalPoller(_MacroPoller):
    """BTC dominance and total crypto market cap, hourly (Phase 11)."""

    def __init__(
        self,
        *,
        on_rows: RowSink,
        on_event: EventSink,
        interval_s: float = DEFAULT_GLOBAL_INTERVAL_S,
        url: str = COINGECKO_URL,
        client: httpx.AsyncClient | None = None,
        resume_from_ms: int | None = None,
    ) -> None:
        super().__init__(
            "macroGlobal",
            url,
            interval_s=interval_s,
            on_rows=on_rows,
            on_event=on_event,
            client=client,
            resume_from_ms=resume_from_ms,
        )

    def parse(self, body: str, *, recv_ms: int) -> list[dict[str, Any]]:
        return parse_global_payload(body, recv_ms=recv_ms)


class DollarIndexPoller(_MacroPoller):
    """The ICE US Dollar Index, hourly (Phase 11)."""

    def __init__(
        self,
        *,
        on_rows: RowSink,
        on_event: EventSink,
        interval_s: float = DEFAULT_FX_INTERVAL_S,
        symbol: str = DXY_SYMBOL,
        url: str | None = None,
        source: str = "yahoo",
        series: str = "DXY",
        client: httpx.AsyncClient | None = None,
        resume_from_ms: int | None = None,
    ) -> None:
        super().__init__(
            "macroFx",
            url or YAHOO_CHART_URL.format(symbol=symbol),
            interval_s=interval_s,
            on_rows=on_rows,
            on_event=on_event,
            client=client,
            resume_from_ms=resume_from_ms,
        )
        self.series = series
        self.source = source

    def parse(self, body: str, *, recv_ms: int) -> list[dict[str, Any]]:
        return parse_yahoo_payload(
            body, recv_ms=recv_ms, series=self.series, source=self.source
        )


class MacroService:
    """Runs the macro pollers and writes what they produce (Phase 11).

    A separate process from the market collector rather than another poller inside it, and
    the reason is the 72-hour guarantee: the collector's whole purpose is not to miss a
    depth message, and hanging two third-party HTTP endpoints off its event loop puts an
    outage at CoinGecko or Yahoo in the same process as the thing that must not stop.
    They also run on completely different clocks -- 100 ms versus one hour.

    Both writers flush on a short interval because the row rate is one an hour: a
    size-triggered flush would leave a row in memory for days, and a crash would lose data
    that was never large enough to justify the risk.
    """

    def __init__(
        self,
        root: Path,
        *,
        global_interval_s: float = DEFAULT_GLOBAL_INTERVAL_S,
        fx_interval_s: float = DEFAULT_FX_INTERVAL_S,
    ) -> None:
        self.root = Path(root)
        self._writers = {
            dataset: ParquetBufferedWriter(
                self.root,
                dataset,
                SCHEMAS[dataset],
                symbol=None,  # symbolless: these describe the market, not an instrument
                max_rows=512,
                flush_interval_s=30.0,
            )
            for dataset in sorted(MACRO_DATASETS)
        }
        self._events = ParquetBufferedWriter(
            self.root,
            "collectorEvents",
            SCHEMAS["collectorEvents"],
            symbol=None,
            max_rows=512,
            flush_interval_s=30.0,
        )
        self.counts: dict[str, int] = dict.fromkeys(self._writers, 0)
        # Cursors seeded from the lake (finding H26): a restarted service must not
        # re-record the snapshot the previous run already flushed. One bounded query per
        # dataset -- the newest partition only -- at construction time.
        self._pollers = [
            CoinGeckoGlobalPoller(
                on_rows=self._on_rows,
                on_event=self._on_event,
                interval_s=global_interval_s,
                resume_from_ms=latest_recorded_ts(self.root, "macroGlobal", None),
            ),
            DollarIndexPoller(
                on_rows=self._on_rows,
                on_event=self._on_event,
                interval_s=fx_interval_s,
                resume_from_ms=latest_recorded_ts(self.root, "macroFx", None),
            ),
        ]

    def _on_rows(self, dataset: str, rows: Any) -> None:
        writer = self._writers[dataset]
        for row in rows:
            writer.append(row)
        self.counts[dataset] += len(rows)

    def _on_event(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        """Macro outages land in the same event stream as market outages.

        Deliberately the shared stream: the gap detector already treats DISCONNECT and
        RECONNECT as explaining a hole (spec 4.5), so a CoinGecko outage is accounted for
        by machinery that exists rather than by a second, parallel notion of downtime.
        """
        self._events.append(
            {
                "ts_ms": _now_ms(),
                "kind": kind.value if hasattr(kind, "value") else str(kind),
                "stream": stream,
                "detail": detail,
                "downtime_ms": int(downtime_ms),
            }
        )

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(poller.run(stop), name=f"macro-{poller.dataset}")
            for poller in self._pollers
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            # Flush before closing the clients, and close the clients whatever happened:
            # a cancelled service that left rows buffered would lose exactly the readings
            # taken during the interesting part of an outage.
            for writer in self._writers.values():
                writer.flush()
            self._events.flush()
            for poller in self._pollers:
                await poller.aclose()

    async def poll_once(self) -> dict[str, int]:
        """One poll of every source, then flush. For `perplab macro --once` and tests."""
        try:
            for poller in self._pollers:
                await poller._guarded(poller._poll_and_emit, "poll")
        finally:
            for writer in self._writers.values():
                writer.flush()
            self._events.flush()
            for poller in self._pollers:
                await poller.aclose()
        return dict(self.counts)
