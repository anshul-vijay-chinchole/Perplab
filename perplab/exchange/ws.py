"""Binance USD-M WebSocket stream manager with reconnect.

Everything the collector records arrives through here, so this module's job is not just
to deliver messages but to make every interruption **observable**. A dropout that is not
reported is indistinguishable from a quiet market, and the Phase 1b exit criterion ("72 h
with zero unexplained gaps") is only meaningful if the collector can tell those two apart
(spec 4.5).

Disconnects are normal, not exceptional: Binance closes every connection after 24 hours
regardless of health. A collector that treats disconnection as an error will log an error
every day forever, which trains you to ignore the log. It is reported as a lifecycle
event with its measured downtime instead.

**Raw mode (`raw_path`) exists for the user-data stream** (spec 11, spec 6.7). That socket
is `/ws/<listenKey>` and delivers *bare* frames -- `{"e": "ORDER_TRADE_UPDATE", ...}` with
no `stream` wrapper -- which the combined-stream demultiplexer below drops on the floor,
because a frame with no `stream` key is exactly what a subscription ack looks like. Silently
dropping a fill report is the worst failure available to a live session: the exchange has a
position that PerpLab's ledger does not, and nothing anywhere says so. Hence two changes
that belong together: a raw mode that passes the whole payload through, and a count of
every frame the combined mode did not recognise, so that "we ignored it" is a number
somebody can read rather than an absence.

**Downtime is also reported while it is still accumulating.** Spec 7 auto-triggers the kill
switch on a disconnection longer than `max_disconnect_seconds` *while a position is open*,
which is a decision that has to be taken during the outage. The DISCONNECT event carries
`downtime_ms=0` because at the moment it is written the outage has not happened yet, so
`on_backoff` fires once per retry with the downtime measured so far and gives the risk layer
something to count.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Sequence
from typing import Any

import websockets
from websockets.asyncio.client import connect

from perplab.core.types import CollectorEventKind

__all__ = ["StreamManager", "PRODUCTION_WS", "TESTNET_WS", "UNRECOGNISED_STREAM"]

PRODUCTION_WS = "wss://fstream.binance.com"
TESTNET_WS = "wss://stream.binancefuture.com"

_BACKOFF_INITIAL_S = 1.0
_BACKOFF_CAP_S = 60.0

UNRECOGNISED_STREAM = "unrecognised"
"""Pseudo-stream label for the count of frames the demultiplexer could not place.

Deliberately not a dataset name and not `collector`. `gaps._stream_covers_dataset` matches
a record to a dataset by this field, so a label that names nothing explains nothing: the
record lands in the lake where an operator can find it without granting any gap an alibi it
did not earn.
"""

MessageHandler = Callable[[str, dict[str, Any], int], None]
EventHandler = Callable[[CollectorEventKind, str, str, int], None]
BackoffHandler = Callable[[str, int], None]
"""`(label, downtime_ms)`, called once per reconnect attempt while still disconnected."""


class StreamManager:
    """Maintains one combined-stream connection, reconnecting indefinitely.

    A single combined stream is used rather than one connection per stream. Binance
    imposes per-IP connection limits, and more importantly a combined stream gives one
    connection state to reason about: either we are receiving everything or we are
    receiving nothing. Per-stream connections create partial-outage states where some
    datasets have a gap and others do not, which is far harder to detect and to explain
    afterwards.

    `on_message` runs inline on the read path and **must not block**. Buffering a row is
    fine; a disk flush or a network call is not. Blocking here stops us draining the
    socket, Binance's send buffer fills, and it drops the connection -- turning a slow
    consumer into a data gap.
    """

    def __init__(
        self,
        streams: Sequence[str],
        on_message: MessageHandler,
        on_event: EventHandler,
        *,
        base_url: str = PRODUCTION_WS,
        ping_interval: float = 20.0,
        ping_timeout: float = 20.0,
        raw_path: str | None = None,
        label: str | None = None,
        on_backoff: BackoffHandler | None = None,
    ) -> None:
        if not streams and raw_path is None:
            raise ValueError("at least one stream is required")
        self._streams = list(streams)
        self._on_message = on_message
        self._on_event = on_event
        self._base_url = base_url.rstrip("/")
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._raw_path = raw_path
        self._on_backoff = on_backoff

        # The label is what every event record carries, and in raw mode the path is a
        # listenKey -- a bearer credential for the account's own order flow. It must not
        # reach the lake, so the label is supplied separately and the path is scrubbed out
        # of any exception text before it is reported.
        self._label = label if label is not None else ",".join(self._streams)

        self._unrecognised = 0
        self._unrecognised_report_at = 1

    @property
    def url(self) -> str:
        if self._raw_path is not None:
            return f"{self._base_url}/ws/{self._raw_path}"
        return f"{self._base_url}/stream?streams={'/'.join(self._streams)}"

    @property
    def label(self) -> str:
        """The stream label written into every event this manager emits."""
        return self._label

    @property
    def unrecognised_frames(self) -> int:
        """Frames received and not delivered to `on_message`, for the whole run.

        Non-zero is not automatically wrong -- subscription acks land here -- but it is
        always worth a look, which is why it is a number rather than a `continue`.
        """
        return self._unrecognised

    async def run(self, stop: asyncio.Event) -> None:
        """Connect and pump messages until `stop` is set. Reconnects on any failure."""
        backoff = _BACKOFF_INITIAL_S
        disconnected_at_ms: int | None = None
        connected_before = False

        while not stop.is_set():
            try:
                async with connect(
                    self.url,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                    close_timeout=5.0,
                    open_timeout=20.0,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    downtime = (
                        _now_ms() - disconnected_at_ms if disconnected_at_ms else 0
                    )
                    self._on_event(
                        CollectorEventKind.RECONNECT
                        if connected_before
                        else CollectorEventKind.CONNECT,
                        self._label,
                        f"connected to {len(self._streams)} stream(s)"
                        if self._raw_path is None
                        else "connected to the raw stream",
                        downtime,
                    )
                    connected_before = True
                    disconnected_at_ms = None
                    # Only reset backoff once a connection actually succeeds. Resetting on
                    # attempt would hammer the endpoint during an outage and risk an IP ban.
                    backoff = _BACKOFF_INITIAL_S

                    await self._pump(ws, stop)

            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - any failure must lead to a retry
                if disconnected_at_ms is None:
                    disconnected_at_ms = _now_ms()
                self._on_event(
                    CollectorEventKind.DISCONNECT,
                    self._label,
                    self._sanitise(f"{type(exc).__name__}: {exc}"),
                    0,
                )

            if stop.is_set():
                break

            if disconnected_at_ms is None:
                # Clean close (Binance's 24 h cycle) rather than an error path.
                disconnected_at_ms = _now_ms()
                self._on_event(
                    CollectorEventKind.DISCONNECT,
                    self._label,
                    "stream closed by server",
                    0,
                )

            if self._on_backoff is not None:
                # Fired before the wait rather than after it, so the first notification
                # arrives at the start of the outage rather than a backoff interval into
                # it. By the time backoff has grown to its 60 s cap, "after" would mean a
                # minute of silence between reports on a socket that is already down --
                # and spec 7's default disconnect trigger is 30 s.
                self._on_backoff(self._label, _now_ms() - disconnected_at_ms)

            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff)
                break  # stop was set during backoff
            except TimeoutError:
                pass
            backoff = min(backoff * 2, _BACKOFF_CAP_S)

    async def _pump(self, ws: Any, stop: asyncio.Event) -> None:
        """Read messages until the socket closes or `stop` is set."""
        stop_task = asyncio.create_task(stop.wait())
        try:
            while not stop.is_set():
                recv_task = asyncio.create_task(ws.recv())
                done, _ = await asyncio.wait(
                    {recv_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_task in done:
                    recv_task.cancel()
                    # **Awaited, not merely cancelled.** A `recv` that had already failed
                    # when the cancel arrived keeps its exception, and an exception nobody
                    # retrieves is printed by asyncio at garbage-collection time -- so every
                    # clean shutdown ended with a "Task exception was never retrieved"
                    # traceback that looked like a crash and was not one.
                    try:
                        await recv_task
                    except (asyncio.CancelledError, Exception):  # noqa: BLE001
                        pass
                    return

                raw = recv_task.result()
                # recv_ms is stamped here, at the first moment we could possibly have
                # observed the message. Stamping it after parsing would fold our own
                # decode time into the measured exchange latency.
                recv_ms = _now_ms()

                try:
                    payload = json.loads(raw)
                except (ValueError, TypeError):
                    # Scrubbed like every other event this class emits. This was the one
                    # path that interpolated raw socket bytes into a message unfiltered,
                    # and it lives in raw mode's own code path -- where the URL carries a
                    # listen key.
                    self._on_event(
                        CollectorEventKind.DISCONNECT,
                        "parse",
                        self._sanitise(f"undecodable frame: {str(raw)[:120]}"),
                        0,
                    )
                    continue

                if not isinstance(payload, dict):
                    self._note_unrecognised(str(payload))
                    continue

                if self._raw_path is not None:
                    # A raw frame is the message: there is no envelope to unwrap, and the
                    # event name is the only label it carries. Passing the whole payload
                    # through is what the user-data stream needs -- its `ORDER_TRADE_UPDATE`
                    # keeps the order's fields at the top level alongside `e`.
                    name = str(payload.get("e") or "")
                    if not name:
                        self._note_unrecognised(json.dumps(payload)[:200])
                        continue
                    self._deliver(name, payload, recv_ms)
                    continue

                stream = payload.get("stream")
                data = payload.get("data")
                if stream is None or not isinstance(data, dict):
                    # Subscription acks and control frames land here, and so would a frame
                    # shape we have not met. Counted rather than dropped: an endpoint that
                    # quietly changed its envelope would otherwise present as a dataset that
                    # stopped filling, with a healthy socket and nothing in the log.
                    self._note_unrecognised(json.dumps(payload)[:200])
                    continue

                self._deliver(stream, data, recv_ms)
        except websockets.ConnectionClosed:
            return
        finally:
            stop_task.cancel()

    def _deliver(self, label: str, payload: dict[str, Any], recv_ms: int) -> None:
        """Hand one frame to the consumer, absorbing anything it throws.

        **The guard belongs here rather than in every handler.** `run`'s reconnect arm
        catches broadly, so without this an exception raised by the *consumer* -- one frame
        shape it cannot parse -- is caught there and reported as a DISCONNECT, and the socket
        is torn down and rebuilt. The consumer's bug then presents as an endless reconnect
        loop against an endpoint that is working perfectly, which is about the most
        misleading diagnosis this class could offer.
        """
        try:
            self._on_message(label, payload, recv_ms)
        except Exception as exc:  # noqa: BLE001 - see docstring
            self._on_event(
                CollectorEventKind.STALE,
                label,
                self._sanitise(f"handler raised: {type(exc).__name__}: {exc}"),
                0,
            )

    def _note_unrecognised(self, sample: str) -> None:
        """Count a frame we did not deliver, and report it at a rate that stays readable.

        Reported at 1, 10, 100, ... rather than on every occurrence. A frame shape we do not
        understand tends to arrive at the same rate as the ones we do, so reporting each one
        would write a 30-per-second event stream that buries the fault it is describing --
        and the event stream is also the gap-detection input (spec 4.5), so flooding it is
        not a cosmetic problem.
        """
        self._unrecognised += 1
        if self._unrecognised < self._unrecognised_report_at:
            return
        self._unrecognised_report_at *= 10
        self._on_event(
            CollectorEventKind.DISCONNECT,
            UNRECOGNISED_STREAM,
            self._sanitise(
                f"{self._unrecognised} unrecognised frame(s) on {self._label}; "
                f"most recent: {sample[:200]}"
            ),
            0,
        )

    def _sanitise(self, text: str) -> str:
        """Remove the raw path from anything about to be written as an event.

        In raw mode that path is a listenKey, which is a bearer token for the account's
        order flow. `websockets` puts the URI into several of its own exception messages,
        so a socket error would otherwise write the credential straight into the lake --
        the one place spec 11 is most explicit that it must never appear.
        """
        if not self._raw_path:
            return text
        return text.replace(self._raw_path, "<listen-key>")


def _now_ms() -> int:
    """Wall-clock epoch milliseconds.

    Used only for collector-side receive timestamps and downtime measurement. This is
    *not* an engine clock -- strategy code reads `ctx.now`, never wall time (spec 5.3).
    """
    return int(time.time() * 1000)
