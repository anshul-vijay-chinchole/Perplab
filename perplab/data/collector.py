"""The always-on market data collector (spec 1b).

This is the first component built, before the accounting engine and before the
backtester, for one reason: **depth history only exists if you record it**. Bulk L2 depth
is not downloadable (spec 4.2 / R1), so every day the collector is not running is a
permanent, unfillable hole in the L2 backtest window. Nothing else in PerpLab has that
property -- every other component can be written later at no cost.

Verification on 2026-08-01 found the same is now true of `bookTicker`: Binance published
it in bulk only between 2023-05-16 and 2024-03-30 and then stopped. Spec 4.2 assumed full
history and made it the primary fill-realism input, which is no longer achievable for
recent ranges. It is therefore recorded live here too, on identical reasoning. See
docs/DATA_AVAILABILITY.md finding F1.

Sources, all public and requiring no API keys:

| Dataset | Source | Note |
|---|---|---|
| `depth20` | WS `<sym>@depth20@100ms` | downsampled to 1 s (spec 4.4) |
| `bookTicker` | WS `<sym>@bookTicker` | added per finding F1 |
| `aggTrades` | REST `/fapi/v1/aggTrades` | `fromId` paging; `isBuyerMaker` preserved |
| `markPrice` | REST `/fapi/v1/premiumIndex` | recorded, never recomputed (spec 3.4) |
| `liquidations` | **none** | no public source remains (finding F4) |

The last three rows changed on 2026-08-02. `fstream.binance.com` serves only *raw* event
streams and silently suppresses every aggregated or computed one: `@aggTrade`, all
`@markPrice` variants, `@kline_*`, `@ticker` and `!forceOrder@arr` deliver nothing, while
the server still ACKs those subscriptions and lists them back under `LIST_SUBSCRIPTIONS`.
A combined subscription delivers `bookTicker` and not `aggTrade` over a single TLS
connection, which rules out anything on the network path. See `perplab.data.rest_poller`
for the full evidence and docs/DATA_AVAILABILITY.md finding F4.

`liquidations` has no substitute: the WebSocket stream is suppressed and
`GET /fapi/v1/allForceOrders` now returns HTTP 404. Rather than leave an empty dataset
that reads as an unexplained gap forever, the collector records one `UNAVAILABLE` event
per run naming it, so the gap detector accounts for the silence from data (spec 4.5).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from collections.abc import Sequence

from perplab.core.money import to_scaled
from perplab.core.types import CollectorEventKind
from perplab.data.rest_poller import (
    REST_DATASETS,
    AggTradePoller,
    MarkPricePoller,
    OpenInterestPoller,
    RestPoller,
    latest_recorded_ts,
)
from perplab.data.schemas import SCHEMAS, normalise_symbol
from perplab.data.writer import ParquetBufferedWriter
from perplab.exchange.rest import PRODUCTION_BASE, PublicRestClient
from perplab.exchange.ws import PRODUCTION_WS, StreamManager

__all__ = ["Collector", "HEARTBEAT_INTERVAL_S", "UNAVAILABLE_DATASETS"]

log = logging.getLogger("perplab.collector")

HEARTBEAT_INTERVAL_S = 10.0
"""Spec 4.5 mandates a heartbeat every 10 s even when nothing changes.

Without it, "no data" is ambiguous: an illiquid minute and a dead socket look identical
on disk. With it, a missing heartbeat is unambiguous evidence of an outage, which is what
makes the Phase 1b exit criterion ("zero *unexplained* gaps") a decidable question rather
than a judgement call."""

DEPTH_BUCKET_MS = 1_000
"""Spec 4.4: store depth at 1 s, not 100 ms. The finer resolution costs 10x the disk for
detail that only matters to strategies explicitly excluded in spec 1.3."""

_STATE_FILENAME = "collector_state.json"

MAX_SILENCE_S: dict[str, float] = {
    "depth20": 60.0,  # published every 100 ms
    "bookTicker": 60.0,  # every book change; never quiet on a major
    "aggTrades": 120.0,  # trade-driven, so allow for a genuinely thin patch
    "markPrice": 60.0,  # polled every 1 s
    "metrics": 900.0,  # open interest, polled onto the archive's 5 min grid
}
"""How long a dataset may go silent before it is reported as STALE.

`liquidations` is deliberately absent, now for two reasons. Forced orders were always
sparse -- hours can pass without one on a calm day -- so a staleness alarm on it would
fire constantly and train the operator to ignore the whole category. Since finding F4 it
also has no source at all, and a permanent alarm about a permanent condition is noise;
that case is stated once per run as an `UNAVAILABLE` event instead.

Thresholds are set well above each dataset's natural interval so ordinary jitter never
trips them. They exist to catch a source that has stopped, not one that is slow.
"""

UNAVAILABLE_DATASETS: dict[str, str] = {
    "liquidations": (
        "no public source: WS !forceOrder@arr is suppressed on this endpoint and "
        "GET /fapi/v1/allForceOrders returns HTTP 404 (withdrawn). Verified 2026-08-02."
    ),
}
"""Datasets the collector knows it cannot source, and why.

Stated as data so the reason travels with the lake. A future operator finding an empty
`liquidations/` directory should be able to learn why from the collector event stream
rather than from a comment in a file they have no reason to open, and the gap detector
should be able to reach the same conclusion mechanically.
"""


class Collector:
    """Records public market data for one symbol to the Parquet lake."""

    def __init__(
        self,
        symbol: str,
        root: Path,
        *,
        base_url: str = PRODUCTION_WS,
        rest_base_url: str = PRODUCTION_BASE,
        flush_interval_s: float = 60.0,
        mark_interval_s: float = 1.0,
        agg_interval_s: float = 2.0,
        oi_interval_s: float = 300.0,
    ) -> None:
        # The same canonicaliser every read path uses -- not a bare `.upper()`. The
        # symbol becomes a literal `symbol=` path component in every writer below, so an
        # unvalidated value is a path injection: `--symbol 'BTC/USDT'` created an
        # unparseable partition and `--symbol '../../..'` wrote outside the lake root
        # entirely (finding M29). `normalise_symbol` refuses separators and quotes
        # outright, and uppercases the rest, so what reaches the path is exactly what the
        # readers will later glob for.
        self.symbol = normalise_symbol(symbol)
        self._root = Path(root)
        self._base_url = base_url
        self._rest_base_url = rest_base_url
        self._mark_interval_s = mark_interval_s
        self._agg_interval_s = agg_interval_s
        self._oi_interval_s = oi_interval_s
        self._lower = self.symbol.lower()

        self._writers = {
            name: ParquetBufferedWriter(
                self._root,
                name,
                SCHEMAS[name],
                symbol=self.symbol,
                flush_interval_s=flush_interval_s,
            )
            for name in (
                "depth20",
                "bookTicker",
                "aggTrades",
                "markPrice",
                "liquidations",
                "metrics",
            )
        }
        # Collector events are not symbol-scoped: they describe the process, not the
        # instrument.
        self._events = ParquetBufferedWriter(
            self._root,
            "collectorEvents",
            SCHEMAS["collectorEvents"],
            flush_interval_s=flush_interval_s,
        )

        self._counts: dict[str, int] = dict.fromkeys(self._writers, 0)

        # Staleness tracking. Baselined at connection time rather than at first message,
        # so a stream that never delivers anything is reported too -- which is the case
        # that a "time since last message" check alone would miss forever.
        self._last_msg_s: dict[str, float] = {}
        self._stale_reported: set[str] = set()
        self._connected_at_s: float | None = None

        # REST-fed datasets need their own baseline. Theirs is set when the poller starts
        # and is never cleared by a socket event: a WebSocket disconnect explains silence
        # on depth20 and bookTicker and says nothing whatever about a poller, so sharing
        # `_connected_at_s` would let an unrelated network blip suppress the alarm on a
        # genuinely dead poller for as long as the socket stayed down.
        self._poller_started_s: dict[str, float] = {}

        # Depth downsampling state: hold the newest snapshot of the current second and
        # emit it when the second rolls over. Keeping the *last* rather than the first
        # matches last-observation-carried-forward, which is how the engine reads the
        # series between samples (spec 3.4).
        self._depth_bucket: int | None = None
        self._depth_pending: dict[str, Any] | None = None

    @property
    def streams(self) -> list[str]:
        """The WebSocket subscriptions, which are only the streams this endpoint serves.

        `@aggTrade`, `@markPrice@1s` and `!forceOrder@arr` were removed on 2026-08-02.
        Subscribing to a stream that delivers nothing is not harmless: the socket would
        carry three permanently dead subscriptions while `rest_poller` supplied the same
        two datasets, so any future restoration of those streams would produce duplicate
        rows rather than a clean switchover.
        """
        return [
            f"{self._lower}@depth20@100ms",
            f"{self._lower}@bookTicker",
        ]

    def _build_pollers(self, client: PublicRestClient) -> list[RestPoller]:
        # The dedup cursors are seeded from the lake, not started empty (finding H26).
        # Each poller drops a sample whose timestamp does not advance past its cursor,
        # and a cursor that lives only in memory forgets everything on restart: the first
        # poll after a restart re-fetched the sample the previous process had already
        # flushed, and the lake gained two rows for one instant with different `recv_ms`
        # -- exactly the tie the pollers' docstrings say the cursor exists to prevent.
        # `latest_recorded_ts` reads one newest partition's footer-bounded max, so the
        # cost is a single bounded query per dataset at startup. AggTradePoller is not
        # seeded: its cursor is the exchange's own dense id sequence, `prime` anchors it
        # at the live head deliberately, and the bulk archive is the documented backfill
        # for the downtime behind it.
        return [
            MarkPricePoller(
                self.symbol,
                client,
                on_rows=self._on_poll_rows,
                on_event=self._on_stream_event,
                interval_s=self._mark_interval_s,
                resume_from_ms=latest_recorded_ts(self._root, "markPrice", self.symbol),
            ),
            AggTradePoller(
                self.symbol,
                client,
                on_rows=self._on_poll_rows,
                on_event=self._on_stream_event,
                interval_s=self._agg_interval_s,
            ),
            OpenInterestPoller(
                self.symbol,
                client,
                on_rows=self._on_poll_rows,
                on_event=self._on_stream_event,
                interval_s=self._oi_interval_s,
                resume_from_ms=latest_recorded_ts(self._root, "metrics", self.symbol),
            ),
        ]

    async def run(self, stop: asyncio.Event) -> None:
        """Record until `stop` is set. Always flushes on the way out."""
        self._emit_startup_event()
        self._emit_unavailable_events()

        manager = StreamManager(
            self.streams,
            self._on_message,
            self._on_stream_event,
            base_url=self._base_url,
        )

        heartbeat = asyncio.create_task(self._heartbeat_loop(stop))
        async with PublicRestClient(self._rest_base_url) as client:
            pollers = self._build_pollers(client)
            for poller in pollers:
                self._poller_started_s[poller.dataset] = time.monotonic()
            poller_tasks = [
                asyncio.create_task(p.run(stop), name=f"poll-{p.dataset}")
                for p in pollers
            ]
            try:
                await manager.run(stop)
            finally:
                # The socket loop returning means `stop` was set or it gave up. Either
                # way the pollers must not outlive it: they hold the REST client that is
                # about to close, and a task still polling through `aclose` would raise
                # into a context nothing is watching.
                stop.set()
                for task in (heartbeat, *poller_tasks):
                    task.cancel()
                for task in (heartbeat, *poller_tasks):
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                # The held depth snapshot is real data, not scratch state. The
                # downsampler keeps the newest snapshot of the current second and only
                # emits it when the second rolls over -- a rollover that never comes on
                # the way out, so every clean stop used to discard the final second's
                # depth (finding L5). For the one dataset that cannot be re-downloaded,
                # "at most one flush interval of loss" (module contract above) must not
                # quietly exclude the very last observation.
                self._flush_depth_pending()
                self._write_event(
                    CollectorEventKind.SHUTDOWN, "collector", "clean shutdown", 0
                )
                self._flush_all(force=True)
                # Cleared only after the flush succeeds. If the flush raises, the state
                # file survives and the next run reports a RESTART -- which is correct,
                # because data really was lost.
                self._clear_state()

    def _emit_unavailable_events(self) -> None:
        """State once per run which datasets have no source, and why.

        Written at startup rather than when the empty dataset is noticed, because it will
        never be noticed -- nothing arrives to trigger a check. Recording it unprompted is
        what lets `gaps` account for the silence without a human supplying the reason.
        """
        for dataset, reason in UNAVAILABLE_DATASETS.items():
            self._write_event(CollectorEventKind.UNAVAILABLE, dataset, reason, 0)
            log.warning("%s is unavailable: %s", dataset, reason)

    # ------------------------------------------------------------------ lifecycle

    def _emit_startup_event(self) -> None:
        """Report a RESTART if the previous run did not shut down cleanly.

        The state file is the mechanism that makes process death visible in the *data*
        rather than only in a log. A clean shutdown removes it; a crash, an OOM kill, or a
        machine reboot leaves it behind with the last heartbeat timestamp still in it, so
        the next process can measure the downtime and record it.

        This is what makes "zero unexplained gaps" checkable: a gap accompanied by a
        RESTART record is accounted for, and one without is a genuine failure. The gap
        detector (spec 4.5) gets crash detection for free from machinery it already has.
        """
        state = self._read_state()
        now = _now_ms()
        if state is None:
            self._write_event(CollectorEventKind.CONNECT, "collector", "cold start", 0)
            return

        last = int(state.get("last_heartbeat_ms", 0))
        downtime = max(0, now - last) if last else 0
        self._write_event(
            CollectorEventKind.RESTART,
            "collector",
            f"previous run ended without clean shutdown (pid {state.get('pid')})",
            downtime,
        )
        log.warning("recovered from unclean shutdown; downtime %d ms", downtime)

    def _state_path(self) -> Path:
        return self._root / _STATE_FILENAME

    def _read_state(self) -> dict[str, Any] | None:
        try:
            return json.loads(self._state_path().read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

    def _write_state(self) -> None:
        path = self._state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "symbol": self.symbol,
                    "last_heartbeat_ms": _now_ms(),
                }
            ),
            encoding="utf-8",
        )
        os.replace(tmp, path)

    def _clear_state(self) -> None:
        try:
            self._state_path().unlink(missing_ok=True)
        except OSError:
            pass

    async def _heartbeat_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_INTERVAL_S)
                return
            except TimeoutError:
                pass

            counts = ", ".join(f"{k}={v}" for k, v in sorted(self._counts.items()))
            self._write_event(CollectorEventKind.HEARTBEAT, "collector", counts, 0)
            self._check_staleness()
            try:
                # Flush runs on the event loop rather than in a thread. The writer's buffer
                # is not synchronised, and swapping it across a thread boundary would risk
                # losing rows appended mid-flush -- a far worse failure than a brief pause.
                # Typical flushes are tens of milliseconds; the socket's receive queue
                # absorbs them.
                self._flush_all()
                self._write_state()
            except OSError as exc:
                # A full disk must not end the heartbeat. Unguarded, one `OSError` killed
                # this task outright: no further heartbeats, no staleness checks, no state
                # file -- the collector went dark in `collectorEvents` while still buffering
                # market data, and the exception then re-raised out of `await task` at
                # shutdown so the SHUTDOWN record and the final forced flush never ran. A
                # clean stop became indistinguishable from a crash, and the last buffer was
                # lost, all from an error the writer had already made safe to retry.
                #
                # Recorded as data, not just logged, so the gap it causes is explainable.
                self._write_event(
                    CollectorEventKind.STALE,
                    "collector",
                    f"flush failed, rows stay buffered for the next attempt: "
                    f"{type(exc).__name__}: {exc}",
                    0,
                )
                log.error("flush failed; retrying on the next heartbeat: %s", exc)

    def _check_staleness(self) -> None:
        """Report streams that have stopped delivering while the connection is healthy.

        A dead stream is invisible without this. The socket stays open, heartbeats keep
        being written, the other datasets keep growing, and one file simply stops -- so
        every signal the collector emits says "healthy" while data is being lost. The
        heartbeat already carried the per-stream counts that would have revealed it, but
        nothing was evaluating them, which meant noticing required a human to read the
        numbers and remember what they should look like.

        Reported once per outage, not once per heartbeat: a stream that has been dead for
        an hour should not produce 360 identical records. Recovery is reported too, so
        the outage has a measurable end.
        """
        now = time.monotonic()
        for dataset, limit in MAX_SILENCE_S.items():
            # Two independent baselines. A socket dataset is only checkable while the
            # socket is up (`_connected_at_s` is None while it is not, and the DISCONNECT
            # record is the explanation); a polled dataset is checkable from the moment
            # its poller started, regardless of what the socket is doing.
            started = (
                self._poller_started_s.get(dataset)
                if dataset in REST_DATASETS
                else self._connected_at_s
            )
            if started is None:
                continue

            baseline = self._last_msg_s.get(dataset, started)
            silent_for = now - baseline

            if silent_for > limit and dataset not in self._stale_reported:
                self._stale_reported.add(dataset)
                never = dataset not in self._last_msg_s
                since = "poller start" if dataset in REST_DATASETS else "connect"
                detail = (
                    f"{dataset}: no data for {silent_for:.0f}s (limit {limit:.0f}s)"
                    + (
                        f"; nothing received since {since} -- the source may not be"
                        " available on this endpoint"
                        if never
                        else ""
                    )
                )
                self._write_event(
                    CollectorEventKind.STALE, dataset, detail, int(silent_for * 1000)
                )
                log.warning("STALE %s", detail)

            elif silent_for <= limit and dataset in self._stale_reported:
                self._stale_reported.discard(dataset)
                self._write_event(
                    CollectorEventKind.HEARTBEAT, dataset, f"{dataset}: recovered", 0
                )
                log.info("stream recovered: %s", dataset)

    def _flush_all(self, *, force: bool = False) -> None:
        """Flush every writer.

        `force` is mandatory on the shutdown path. `maybe_flush` is conditional on the
        row/time thresholds, so using it to shut down discards whatever is still buffered
        -- including the SHUTDOWN record written moments earlier. That turns every clean
        stop into a small silent data loss, and worse, into a *missing* shutdown marker,
        which would make an orderly stop indistinguishable from a crash in the event log.
        """
        for writer in self._writers.values():
            writer.flush() if force else writer.maybe_flush()
        self._events.flush() if force else self._events.maybe_flush()

    # ------------------------------------------------------------------ ingest

    def _on_stream_event(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        if kind in (CollectorEventKind.CONNECT, CollectorEventKind.RECONNECT):
            if stream in REST_DATASETS:
                # A poller reporting its own recovery. It rebaselines only itself: the
                # pollers and the socket fail independently, and letting one poller's
                # reconnect clear the socket's liveness would forgive a genuinely dead
                # stream every time an unrelated REST call recovered.
                #
                # **The baseline moves only on a CONNECT, or on a RECONNECT that follows
                # data.** A poller emits RECONNECT every time an HTTP call recovers, which
                # is a statement about the *transport*, not about the data. Resetting the
                # silence clock on it meant an endpoint that answered every request with an
                # empty result while failing and recovering every fifty seconds was never
                # reported stale, however many hours it produced nothing — the counter was
                # pushed forward faster than the sixty-second limit could be reached, and
                # the dataset died silently while the event stream filled with reassuring
                # DISCONNECT/RECONNECT pairs. Measured: four simulated hours, zero rows,
                # zero STALE records; identical run without the flapping, one STALE record.
                #
                # The clock therefore starts at CONNECT and moves only when a row arrives
                # (`_mark_seen`). A RECONNECT after a genuine outage that then delivers data
                # resets it on the first row; one that delivers nothing keeps counting, and
                # says so — which is correct, because the data still is not flowing. The
                # DISCONNECT/RECONNECT pair explains the resulting gap either way.
                if kind is CollectorEventKind.CONNECT:
                    self._poller_started_s[stream] = time.monotonic()
                    self._last_msg_s.pop(stream, None)
                    self._stale_reported.discard(stream)
            else:
                # Rebaseline the socket. Without this, the silence spanning a reconnect
                # would be re-reported as staleness on the new connection, even though the
                # DISCONNECT/RECONNECT pair already explains it. Poller state is left
                # alone for the same reason, in the other direction.
                socket_datasets = set(MAX_SILENCE_S) - REST_DATASETS
                self._connected_at_s = time.monotonic()
                for dataset in socket_datasets:
                    self._last_msg_s.pop(dataset, None)
                self._stale_reported -= socket_datasets
        elif kind is CollectorEventKind.DISCONNECT and stream not in REST_DATASETS:
            # Suspend staleness checks while disconnected; the disconnect record is the
            # explanation, and a STALE record alongside it would be noise.
            self._connected_at_s = None

        self._write_event(kind, stream, detail, downtime_ms)
        log.info("%s %s: %s (downtime %d ms)", kind.value, stream, detail, downtime_ms)

    def _write_event(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        self._events.append(
            {
                "ts_ms": _now_ms(),
                "kind": kind.value,
                "stream": stream,
                "detail": detail[:500],
                "downtime_ms": downtime_ms,
            }
        )

    def _on_message(self, stream: str, data: dict[str, Any], recv_ms: int) -> None:
        """Route one frame. Runs on the socket read path, so it must stay cheap."""
        try:
            if stream.endswith("@depth20@100ms"):
                self._mark_seen("depth20")
                self._on_depth(data, recv_ms)
            elif stream.endswith("@bookTicker"):
                self._mark_seen("bookTicker")
                self._on_book_ticker(data, recv_ms)
        except (KeyError, ValueError, TypeError) as exc:
            # A malformed frame must not kill the collector -- losing the remaining
            # streams to salvage one bad message would be a far larger gap. It is
            # recorded as an event so it is visible in the data, not just the log.
            #
            # The stream label is "parse", deliberately naming nothing a dataset matches
            # (the same convention as `ws.UNRECOGNISED_STREAM`, for the same reason).
            # One bad frame is a parse incident on a socket that is still delivering,
            # not a disconnection of the stream -- and when this record carried the raw
            # stream name, `gaps._closing_times` could never pair it with a recovery
            # (nothing re-CONNECTs after a frame that was merely skipped), so a single
            # unparseable frame at 03:00 stood as the *explanation* for a six-hour tick
            # silence it had nothing to do with (finding H23). The stream name stays
            # visible in the detail, where a human can grep for it and no gap can borrow
            # it as an alibi.
            self._write_event(
                CollectorEventKind.DISCONNECT,
                "parse",
                f"unparseable payload on {stream}: {type(exc).__name__}: {exc}",
                0,
            )

    def _mark_seen(self, dataset: str) -> None:
        """Record liveness. Stamped before parsing, so a stream that is delivering but
        sending malformed frames still counts as alive -- that is a parse failure, which
        is reported separately, not a dead stream."""
        self._last_msg_s[dataset] = time.monotonic()

    def _on_depth(self, data: dict[str, Any], recv_ms: int) -> None:
        ts = int(data.get("T") or data["E"])
        bucket = ts // DEPTH_BUCKET_MS

        if self._depth_bucket is None:
            self._depth_bucket = bucket
        elif bucket != self._depth_bucket:
            if self._depth_pending is not None:
                self._writers["depth20"].append(self._depth_pending)
                self._counts["depth20"] += 1
            self._depth_bucket = bucket
            self._depth_pending = None

        bids = data.get("b", [])
        asks = data.get("a", [])
        self._depth_pending = {
            "ts_ms": ts,
            "recv_ms": recv_ms,
            "last_update_id": int(data.get("u", 0)),
            "bid_px": [to_scaled(lvl[0]) for lvl in bids],
            "bid_qty": [to_scaled(lvl[1]) for lvl in bids],
            "ask_px": [to_scaled(lvl[0]) for lvl in asks],
            "ask_qty": [to_scaled(lvl[1]) for lvl in asks],
        }

    def _flush_depth_pending(self) -> None:
        """Emit the depth snapshot still waiting for its second to roll over (L5).

        Called on the shutdown path only. Mid-run, the pending snapshot must *not* be
        emitted early -- the downsampler's contract is one row per second holding the
        newest snapshot of that second, and an early emit would either duplicate the
        second or freeze it before its final update. At shutdown there is no later
        update to wait for: what is pending is the last observation the run will ever
        have of the book, and it is exactly as real as the one the previous rollover
        published.
        """
        if self._depth_pending is not None:
            self._writers["depth20"].append(self._depth_pending)
            self._counts["depth20"] += 1
            self._depth_pending = None
            self._depth_bucket = None

    def _on_book_ticker(self, data: dict[str, Any], recv_ms: int) -> None:
        self._writers["bookTicker"].append(
            {
                "ts_ms": int(data.get("T") or data["E"]),
                "recv_ms": recv_ms,
                "update_id": int(data.get("u", 0)),
                "bid_px": to_scaled(data["b"]),
                "bid_qty": to_scaled(data["B"]),
                "ask_px": to_scaled(data["a"]),
                "ask_qty": to_scaled(data["A"]),
            }
        )
        self._counts["bookTicker"] += 1

    def _on_poll_rows(self, dataset: str, rows: Sequence[dict[str, Any]]) -> None:
        """Append rows produced by a REST poller (spec 3.4, 6.4).

        The pollers build rows in the dataset's own schema rather than handing back raw
        payloads, so this stays a sink and there is exactly one place per dataset where a
        field name is decided. `_mark_seen` is called from here, which means liveness
        tracks *data* rather than successful HTTP calls -- a poller that keeps polling
        happily while the endpoint returns empty forever is precisely the failure the
        staleness check exists to catch.
        """
        if not rows:
            # Deliberately not marked live. An empty batch is a *successful* poll that
            # produced nothing -- a deduplicated mark price that has stopped advancing, or
            # a trade feed returning nothing on a liquid symbol. Both are conditions the
            # staleness check should surface, and stamping liveness on the call rather than
            # on the data would hide exactly those.
            return

        writer = self._writers[dataset]
        for row in rows:
            writer.append(row)
        self._counts[dataset] += len(rows)
        self._mark_seen(dataset)


def _now_ms() -> int:
    return int(time.time() * 1000)
