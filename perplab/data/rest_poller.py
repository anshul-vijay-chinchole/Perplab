"""REST pollers for the market data streams this endpoint will not push (finding F4).

`fstream.binance.com` was found on 2026-08-02 to serve only *raw* event streams and to
silently suppress every aggregated or computed one. The evidence is not ambiguous:

- `@trade`, `@depth@100ms`, `@depth20@100ms` and `@bookTicker` all deliver normally;
- `@aggTrade`, every `@markPrice` variant, `@kline_*`, `@ticker`, `@miniTicker` and
  `!forceOrder@arr` deliver **nothing**, raw or combined;
- the server ACKs the dead subscriptions and lists them back under `LIST_SUBSCRIPTIONS`,
  so they are not misspelled;
- a combined subscription carrying both `bookTicker` and `aggTrade` delivers the first and
  not the second **over one TLS connection**, which rules out any network middlebox --
  nothing between us and Binance can read inside that stream to drop messages selectively.

So this is not a fallback for a flaky socket. For mark price it is the only source that
exists, and mark price is what liquidation is decided against (spec 3.7).

**Why polling is not a downgrade here.** A dropped WebSocket frame is invisible: nothing in
the data says a message was skipped. `aggTrades` ids are dense and contiguous, so a poller
that always asks from `last_seen + 1` either receives the next trade or receives nothing --
it cannot silently skip one, and the id sequence remains checkable after the fact. The
`recv_ms` column becomes poll time rather than push time and is correspondingly less useful
as a latency measure, but `ts_ms` is still the exchange's own clock, which is what the
engine orders on (spec 6.2).

Rate budget, measured 2026-08-02: `premiumIndex` costs weight 1, `aggTrades` costs 20. At
the defaults below (1 Hz and 0.5 Hz) that is 60 + 600 = 660 per minute against a 2400
limit, leaving room for retries and multi-page catch-up.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import duckdb

from perplab.core.money import to_scaled
from perplab.core.types import CollectorEventKind
from perplab.data.schemas import SYMBOLLESS_DATASETS, layout_for, normalise_symbol
from perplab.exchange.rest import AGG_TRADES_MAX_LIMIT, PublicRestClient

__all__ = [
    "OpenInterestPoller",
    "RestPoller",
    "MarkPricePoller",
    "AggTradePoller",
    "REST_DATASETS",
    "MAX_CATCHUP_PAGES",
    "latest_recorded_ts",
]

log = logging.getLogger("perplab.poller")

REST_DATASETS = frozenset({"markPrice", "aggTrades", "metrics"})
"""Datasets fed by polling rather than by the WebSocket.

The collector's staleness check consults this: a WebSocket disconnect explains silence on
`depth20` and `bookTicker` but says nothing about these two, and suspending their checks
alongside the socket's would hide a dead poller behind an unrelated network blip.
"""

MAX_CATCHUP_PAGES = 20
"""Pages one tick may walk before yielding.

Bounded because catch-up and liveness compete: after a long outage the poller could stay
inside one tick for minutes, during which nothing flushes and `stop` goes unread, so a
Ctrl-C would appear to hang. Twenty pages is 20,000 aggregate trades -- far more than a
tick's worth at any plausible rate -- and whatever remains is simply picked up next tick.
"""

RowSink = Callable[[str, Sequence[dict[str, Any]]], None]
EventSink = Callable[[CollectorEventKind, str, str, int], None]


def latest_recorded_ts(root: Path, dataset: str, symbol: str | None) -> int | None:
    """The newest timestamp the lake already holds for one dataset, from one partition.

    This is what a timestamp-deduplicating poller seeds its cursor from on start
    (finding H26). The cursors used to live in memory only, so every restart forgot
    them: the first poll after a restart re-fetched the sample the previous process had
    already flushed, and the lake gained two rows for one instant with different
    `recv_ms` -- reproducing exactly the last-observation-carried-forward tie the
    dedup exists to prevent, once per restart, forever.

    **Bounded on purpose.** Only the lexicographically newest partition directory is
    scanned, not the dataset's history: partition names (`date=YYYY-MM-DD`,
    `year=YYYY/month=MM`, `year=YYYY`) sort chronologically as strings by construction,
    and the newest timestamp necessarily lives in the newest non-empty partition. That
    bound is also why missing more history is harmless -- a duplicate is only possible
    when the restart happens inside the source's own update window (seconds for
    `premiumIndex`, minutes for open interest), so the newest partition always contains
    the only sample a fresh poll could collide with.

    Returns `None` for a dataset with no published files, which is the honest cold-start
    answer: there is nothing on disk a new sample could duplicate.
    """
    base = Path(root) / dataset
    if dataset not in SYMBOLLESS_DATASETS and symbol is not None:
        base = base / f"symbol={normalise_symbol(symbol)}"
    if not base.is_dir():
        return None

    directories: dict[Path, None] = {}
    for path in base.rglob("*.parquet"):
        if not path.name.startswith("."):
            directories[path.parent] = None
    if not directories:
        return None
    newest = max(directories, key=lambda p: p.relative_to(base).as_posix())

    column = layout_for(dataset).time_column
    glob = (newest / "*.parquet").as_posix().replace("'", "''")
    connection = duckdb.connect()
    try:
        row = connection.execute(
            f'SELECT max("{column}") FROM read_parquet(\'{glob}\')'
        ).fetchone()
    finally:
        connection.close()
    return int(row[0]) if row and row[0] is not None else None

_BACKOFF_INITIAL_S = 1.0
_BACKOFF_CAP_S = 60.0


class RestPoller:
    """A polling loop that reports its failures as data and never dies.

    Subclasses implement `fetch`, which returns rows ready for the writer. Everything about
    staying alive, pacing, backoff and making outages visible lives here, because those are
    exactly the properties that decide whether the Phase 1b exit criterion is checkable.

    Failures are emitted as DISCONNECT/RECONNECT against the poller's dataset name, reusing
    the same vocabulary the WebSocket manager writes. The gap detector already treats those
    as explaining a gap (spec 4.5), so a REST outage lands in the report the same way a
    socket outage does, with no new machinery and no new special case.
    """

    def __init__(
        self,
        dataset: str,
        *,
        interval_s: float,
        on_rows: RowSink,
        on_event: EventSink,
    ) -> None:
        self.dataset = dataset
        self.interval_s = interval_s
        self._on_rows = on_rows
        self._on_event = on_event
        self._failures = 0
        self._failing_since_ms: int | None = None

    async def fetch(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def prime(self) -> None:
        """Optional one-off setup before the first poll (e.g. establishing a cursor)."""

    async def run(self, stop: asyncio.Event) -> None:
        """Poll until `stop` is set.

        The cadence is driven from a monotonic deadline rather than `sleep(interval)` after
        each poll, so request latency does not accumulate into drift -- at 1 Hz, a 200 ms
        round trip would otherwise cost a sixth of the samples over an hour.
        """
        try:
            await self._guarded(self.prime, "prime")
        except asyncio.CancelledError:
            raise

        backoff = _BACKOFF_INITIAL_S
        next_at = time.monotonic()

        while not stop.is_set():
            next_at += self.interval_s

            ok = await self._guarded(self._poll_and_emit, "poll")
            if ok:
                backoff = _BACKOFF_INITIAL_S
                delay = max(0.0, next_at - time.monotonic())
                if delay == 0.0:
                    # Fell behind (slow endpoint, long catch-up). Re-baseline instead of
                    # firing a burst of immediate polls to "make up" the missed ticks,
                    # which would spend rate-limit weight precisely when the endpoint is
                    # already struggling.
                    next_at = time.monotonic()
            else:
                delay = backoff
                backoff = min(backoff * 2, _BACKOFF_CAP_S)
                next_at = time.monotonic() + delay

            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
                return
            except TimeoutError:
                pass

    async def _poll_and_emit(self) -> None:
        rows = await self.fetch()
        if rows:
            self._on_rows(self.dataset, rows)

    async def _guarded(self, fn: Callable[[], Any], phase: str) -> bool:
        """Run one step, converting any failure into a recorded event.

        Catches broadly and deliberately. A poller that dies on an unexpected exception
        takes its dataset silently offline for the rest of the run, which is the single
        worst outcome available here -- worse than a wrong value, because nothing in the
        data would say it happened.
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
                self.dataset,
                f"{phase} failed ({self._failures} consecutive): "
                f"{type(exc).__name__}: {exc}",
                0,
            )
            log.warning("%s %s failed: %s: %s", self.dataset, phase, type(exc).__name__, exc)
            return False

        if self._failures:
            downtime = _now_ms() - (self._failing_since_ms or _now_ms())
            self._on_event(
                CollectorEventKind.RECONNECT,
                self.dataset,
                f"recovered after {self._failures} consecutive failure(s)",
                downtime,
            )
            log.info("%s recovered after %d failure(s)", self.dataset, self._failures)
            self._failures = 0
            self._failing_since_ms = None
        return True


class MarkPricePoller(RestPoller):
    """Records the mark price series from `premiumIndex` (spec 3.4).

    Every `@markPrice` WebSocket variant is suppressed on this endpoint, so this is the
    only source. Polled at 1 Hz because the server stamps a fresh `time` every second --
    verified 2026-08-02: twelve consecutive polls produced twelve distinct timestamps at
    ~1000 ms spacing -- which reproduces the cadence `@markPrice@1s` would have given.

    Spec 3.4 forbids computing or interpolating a mark price, and nothing here does. The
    value is recorded exactly as served; between samples the engine carries the last
    observation forward rather than filling between them.

    `resume_from_ms` seeds the dedup cursor with the newest timestamp already in the lake
    (`latest_recorded_ts`), so a restart cannot re-write the sample the previous process
    flushed moments before it died (finding H26). `None` -- an empty lake -- starts the
    cursor empty, which is correct: there is nothing on disk to collide with.
    """

    def __init__(
        self,
        symbol: str,
        client: PublicRestClient,
        *,
        on_rows: RowSink,
        on_event: EventSink,
        interval_s: float = 1.0,
        resume_from_ms: int | None = None,
    ) -> None:
        super().__init__(
            "markPrice", interval_s=interval_s, on_rows=on_rows, on_event=on_event
        )
        self._symbol = symbol
        self._client = client
        self._last_ts_ms: int | None = resume_from_ms

    async def fetch(self) -> list[dict[str, Any]]:
        payload = await self._client.premium_index(self._symbol)
        ts_ms = int(payload["time"])

        # Drop a repeat of the timestamp we already hold. The endpoint re-serves the same
        # sample if polled twice within its update window, and a duplicated ts_ms would
        # both inflate the row count the gap detector reasons about and give a
        # last-observation-carried-forward read two candidate rows for one instant.
        if self._last_ts_ms is not None and ts_ms <= self._last_ts_ms:
            return []
        self._last_ts_ms = ts_ms

        return [
            {
                "ts_ms": ts_ms,
                "recv_ms": _now_ms(),
                "mark_price": to_scaled(payload["markPrice"]),
                "index_price": to_scaled(payload.get("indexPrice", "0")),
                "estimated_settle_price": to_scaled(
                    payload.get("estimatedSettlePrice", "0")
                ),
                "last_funding_rate": to_scaled(payload.get("lastFundingRate", "0")),
                "next_funding_ms": int(payload.get("nextFundingTime", 0)),
            }
        ]


class OpenInterestPoller(RestPoller):
    """Records open interest into the same `metrics` dataset the daily archive fills.

    **Same dataset, same cadence, same column.** `ctx.oi()` reads `metrics.sum_open_interest`
    and nothing else, so writing here makes the live period a continuation of the six years
    of history behind it rather than a second series a consumer would have to know about.
    The archive publishes every five minutes and this polls on the same cadence, so the two
    halves have the same density. They do not share the same *instants* -- see below; making
    them share instants was a defect rather than a feature. Note that `metrics` carries no
    gap rule (spec 4.5 defines none, and `gaps.DATASET_RULES` says so), so nothing checks
    this series for holes: the cadence matches the archive, not a detector.

    **The four ratio columns are left null.** `/fapi/v1/openInterest` does not carry them --
    the long/short ratios come from `/futures/data/*`, which is capped at 30 days and is a
    different question. Null is what "not measured" looks like in this schema; zero would be
    a measurement, and a strategy reading a long/short ratio of 0.0 would be reading a
    number nobody observed.

    **Timestamps are the endpoint's own, and are deliberately *not* snapped to the
    archive's grid.** Snapping looked tidier -- the two halves of the series would share one
    set of instants -- and it created a collision that nothing downstream could resolve. A
    later archive backfill over the live period produces a row with the *same* `create_time`
    and a different value; `_load_open_interest` reads both, and `OIDelta` then measures the
    archive-versus-live discrepancy at one instant instead of the change between two. On one
    fixture it reported 10.0 forever in place of the true 200.0, and which of the tied rows
    won depended on physical file order -- so the same lake contents gave different answers
    depending on ingest sequence, which is a spec 12.1 violation.

    Unsnapped, a live row and an archive row are two observations at two instants. They
    interleave, LOCF reads the later one, and a backfill densifies the series rather than
    contradicting it.
    """

    def __init__(
        self,
        symbol: str,
        client: PublicRestClient,
        *,
        on_rows: RowSink,
        on_event: EventSink,
        interval_s: float = 300.0,
        resume_from_ms: int | None = None,
    ) -> None:
        super().__init__(
            "metrics", interval_s=interval_s, on_rows=on_rows, on_event=on_event
        )
        self._symbol = symbol
        self._client = client
        # Seeded from the lake by the collector (`latest_recorded_ts`, finding H26):
        # an in-memory-only cursor re-recorded the last pre-restart sample on every
        # restart, giving LOCF two rows for one instant.
        self._last_ts_ms: int | None = resume_from_ms

    async def fetch(self) -> list[dict[str, Any]]:
        payload = await self._client.open_interest(self._symbol)
        ts_ms = int(payload.get("time") or _now_ms())
        if self._last_ts_ms is not None and ts_ms <= self._last_ts_ms:
            # The endpoint re-served a sample already held. Writing it twice would give a
            # last-observation-carried-forward read two candidate rows for one instant.
            return []
        self._last_ts_ms = ts_ms

        scaled = to_scaled(payload["openInterest"])
        return [
            {
                "create_time": ts_ms,
                "sum_open_interest": scaled,
                # Notional, which the archive carries and this endpoint does not. Null
                # rather than derived from a mark price: the archive's figure is Binance's
                # own, and a locally multiplied one would be a different quantity sharing
                # a column name.
                "sum_open_interest_value": None,
                "count_toptrader_long_short_ratio": None,
                "sum_toptrader_long_short_ratio": None,
                "count_long_short_ratio": None,
                "sum_taker_long_short_vol_ratio": None,
            }
        ]


class AggTradePoller(RestPoller):
    """Records aggregate trades by walking the id sequence forward (spec 6.4).

    Writes into the same `aggTrades` dataset the bulk archives fill, with the same schema,
    so the live period and the six years of history behind it stay one continuous series.
    That continuity is why this polls `aggTrades` rather than recording the `@trade` stream
    that *does* work here: `@trade` is individual fills, has no bulk counterpart, and would
    fork every downstream consumer into handling two trade shapes forever.

    **Gaplessness comes from `fromId`, not from polling quickly.** Each tick asks from
    `last_seen + 1`, so falling behind costs latency and never data. A page returned exactly
    full may have been truncated, so it is followed immediately rather than at the next
    tick.

    A restart resumes from the live head, not from the last id seen before the outage. The
    downtime is already recorded as a RESTART event, and the trades inside it are
    recoverable from the daily bulk archive -- which is the system's general answer for
    history behind the collector. Replaying an unbounded backlog through a weight-20
    endpoint on every restart would risk a rate-limit ban at exactly the moment the
    collector is trying to get back to healthy.
    """

    def __init__(
        self,
        symbol: str,
        client: PublicRestClient,
        *,
        on_rows: RowSink,
        on_event: EventSink,
        interval_s: float = 2.0,
    ) -> None:
        super().__init__(
            "aggTrades", interval_s=interval_s, on_rows=on_rows, on_event=on_event
        )
        self._symbol = symbol
        self._client = client
        self._next_id: int | None = None

    async def prime(self) -> None:
        """Anchor the cursor just past the current head."""
        head = await self._client.agg_trades(self._symbol, limit=1)
        if not head:
            raise RuntimeError("aggTrades returned no head record to anchor the cursor")
        self._next_id = int(head[-1]["a"]) + 1
        self._on_event(
            CollectorEventKind.CONNECT,
            "aggTrades",
            f"polling from aggregate trade id {self._next_id}",
            0,
        )

    async def _poll_and_emit(self) -> None:
        """`fetch` already delivered every page; there is nothing left to emit.

        Overridden because the base implementation emits once, after the whole fetch. For a
        paging cursor that is wrong -- see `fetch`.
        """
        await self.fetch()

    async def fetch(self) -> list[dict[str, Any]]:
        """Walk the cursor forward, **delivering each page before advancing past it**.

        The ordering here is the whole correctness argument. An earlier version accumulated
        every page into a local list and returned it at the end, which meant a failure on
        page N discarded pages 1..N-1 *while the cursor had already moved past them*. Those
        trades were then unreachable forever: the next poll asks from `_next_id`, the ids
        below it are never requested again, and because the cursor moved consistently the
        id-jump check below has nothing to notice. A 429 partway through a twenty-page
        catch-up -- 400 weight in one tick, so not a remote possibility -- silently lost
        thousands of trades with nothing in the lake to say so.

        Delivering first and advancing second makes the loss impossible rather than
        unlikely: if the next request fails, everything already fetched is with the writer
        and the cursor still names the first id that has not been delivered. The cost is
        that a retried tick may re-deliver nothing at all -- there is no double-write path,
        because the cursor only ever moves after a successful hand-off.
        """
        if self._next_id is None:
            await self.prime()

        collected: list[dict[str, Any]] = []
        for _ in range(MAX_CATCHUP_PAGES):
            page = await self._client.agg_trades(
                self._symbol, from_id=self._next_id, limit=AGG_TRADES_MAX_LIMIT
            )
            if not page:
                break

            first = int(page[0]["a"])
            if self._next_id is not None and first > self._next_id:
                # The ids we asked for do not exist. Recorded rather than silently
                # accepted: the sequence is dense in every observation so far, so a jump
                # is either an exchange-side discontinuity or a wrong assumption in this
                # poller, and both are things a later audit of the lake should be able to
                # see rather than infer.
                self._on_event(
                    CollectorEventKind.STALE,
                    "aggTrades",
                    f"aggregate trade id jump: asked from {self._next_id}, "
                    f"got {first} ({first - self._next_id} id(s) absent)",
                    0,
                )

            rows = [_agg_trade_row(row) for row in page]
            self._on_rows(self.dataset, rows)
            self._next_id = int(page[-1]["a"]) + 1
            collected.extend(rows)

            if len(page) < AGG_TRADES_MAX_LIMIT:
                break  # caught up; a short page cannot have been truncated

        return collected


def _agg_trade_row(row: dict[str, Any]) -> dict[str, Any]:
    """One REST aggregate trade in the `aggTrades` schema.

    Field-for-field identical to what the WebSocket handler produced, so the dataset does
    not change shape at the point collection switched sources.
    """
    return {
        "ts_ms": int(row["T"]),
        "recv_ms": _now_ms(),
        "agg_id": int(row["a"]),
        "price": to_scaled(row["p"]),
        "qty": to_scaled(row["q"]),
        "first_trade_id": int(row["f"]),
        "last_trade_id": int(row["l"]),
        # 'm' is the aggressor flag: True means the buyer was the maker, so the trade was
        # sell-aggressive. Inverting it reverses every queue-consumption decision in the
        # fill model (spec 6.4).
        "is_buyer_maker": bool(row["m"]),
    }


def _now_ms() -> int:
    return int(time.time() * 1000)
