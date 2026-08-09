"""Binance frames in, engine events out -- the live half of spec 6.1's data-source row.

Every event this module produces is **the same object a backtest replays**. A live trade
becomes `ticks.TradePrint`, a live ladder becomes `core.types.DepthSnapshot`, a live minute
of mark samples becomes `feed.MarkBar`. Nothing downstream can tell where an event came
from, which is what makes the shadow backtest of spec 6.7 a measurement of the fill model
rather than a measurement of two engines.

**Which streams actually work, measured rather than assumed.** `docs/DATA_AVAILABILITY.md`
finding F4 records that `fstream.binance.com` serves only *raw* per-event streams and
silently suppresses every aggregated or computed one -- the server ACKs the subscription,
lists it back, and sends nothing. Re-measured on 2026-08-03 for Phase 7:

| Stream | production | testnet |
|---|---|---|
| `@bookTicker` | works | works |
| `@depth20@100ms` | works | works |
| `@aggTrade` | **silent** | works |
| `@markPrice@1s` | **silent** | works |
| `@kline_1m` | **silent** | works |

REST serves all of it on both hosts. So the feed subscribes to what the endpoint delivers
and polls the rest, exactly as the collector does, and the choice is recorded in the tape's
`meta.json` rather than assumed by a reader.

**Two aggregation decisions, and both are about matching the backtester rather than about
being as fast as possible.** They are the largest silent-divergence risks in Phase 7.

*Mark price is aggregated into one-minute bars.* The engine's liquidation check probes low,
then high, then close of a `MarkBar` (spec 3.4's traversed range), and feeding it 1 Hz point
samples with `high == low == close` degenerates that to a close-only test -- it would miss
every liquidation the market immediately recovered from, which is a **one-sided** error
always in the paper session's favour. It would also make the equity series sixty times
denser than any backtest's, changing the basis of exposure, turnover and volatility. So the
1 Hz samples feed a running min/max and one `MARK_PRICE_UPDATE` is emitted per minute, at
the same cadence and with the same shape the lake's `markPriceKlines` has.

*Depth is downsampled to one second.* Live `depth20` arrives every 100 ms; the lake stores
the last snapshot per second (`collector.DEPTH_BUCKET_MS`). Applying every 100 ms frame
would give the paper session a strictly fresher book than any backtest this platform can
ever run, which makes the session unrepresentative of the thing it exists to validate.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from perplab.core.money import to_scaled
from perplab.core.types import Bar, CollectorEventKind, DepthSnapshot
from perplab.engine.clock import Event, EventKind
from perplab.engine.feed import BarStep, FundingPoint, MarkBar
from perplab.engine.reorder import SequenceAllocator, live_dataset_id
from perplab.engine.ticks import TopOfBook, TradePrint
from perplab.exchange.rest import PRODUCTION_BASE, TESTNET_BASE, PublicRestClient
from perplab.exchange.ws import PRODUCTION_WS, TESTNET_WS, StreamManager

__all__ = [
    "MARK_BAR_MS",
    "DEPTH_BUCKET_MS",
    "FUNDING_PUBLICATION_GRACE_MS",
    "Venue",
    "PRODUCTION",
    "TESTNET",
    "LiveFeed",
    "MarkAggregator",
    "FundingObservation",
]

log = logging.getLogger("perplab.live.feed")

MARK_BAR_MS = 60_000
"""Mark aggregation window. Matches `markPriceKlines`, which is what a backtest reads."""

DEPTH_BUCKET_MS = 1_000
"""Depth downsampling bucket. Matches `collector.DEPTH_BUCKET_MS`, which is what the lake
holds -- see the module docstring for why matching it beats being fresher."""

FUNDING_PUBLICATION_GRACE_MS = 60_000
"""How long the funding source holds the release watermark at a settlement it has not yet
been served a row for. See `LiveFeed._funding_horizon`.

**Measured, not chosen for roundness.** At the 10:00:00.000 UTC boundary on 2026-08-03,
`premiumIndex.nextFundingTime` advanced 638 ms after the settlement while the row itself
first appeared in `GET /fapi/v1/fundingRate` 1 926 ms after it, and the row's visibility
then flapped across the endpoint's nodes for a further ten seconds. So the wait is normally
seconds; this is a wide multiple of that.

Waiting for ever is not an option, and the reason is specific: this source keeps *reporting*
while it waits, so `ReorderBuffer` never declares it stalled, and a boundary whose row is
never published would freeze the whole session's dispatch for the rest of the run. Past this
much silence the settlement is written off with a `STALE` record naming it."""


@dataclass(frozen=True, slots=True)
class Venue:
    """One endpoint, and what it was measured to deliver.

    The stream lists are measurements, not documentation. Subscribing to a stream this
    endpoint does not serve is not harmless: the socket carries a permanently dead
    subscription while a poller supplies the same dataset, so if the stream is ever restored
    the session records every row twice.
    """

    name: str
    rest_base: str
    ws_base: str
    ws_streams: tuple[str, ...]
    """Per-symbol stream suffixes this endpoint actually delivers."""

    @property
    def polls_trades(self) -> bool:
        return not any(s.startswith("@aggTrade") for s in self.ws_streams)

    @property
    def polls_marks(self) -> bool:
        return not any(s.startswith("@markPrice") for s in self.ws_streams)


PRODUCTION = Venue(
    name="production",
    rest_base=PRODUCTION_BASE,
    ws_base=PRODUCTION_WS,
    # Measured 2026-08-03: bookTicker 2685 frames/12s, depth20 105/12s, everything
    # aggregated silent. See docs/DATA_AVAILABILITY.md F4.
    ws_streams=("@bookTicker", "@depth20@100ms"),
)

TESTNET = Venue(
    name="testnet",
    rest_base=TESTNET_BASE,
    ws_base=TESTNET_WS,
    # Measured 2026-08-03 on one combined connection: aggTrade 18, markPrice@1s 12,
    # bookTicker 37, depth20 29, kline_1m 13 -- all in 12 s. Testnet serves everything.
    ws_streams=("@bookTicker", "@depth20@100ms", "@aggTrade", "@markPrice@1s"),
)


@dataclass(frozen=True, slots=True)
class FundingObservation:
    """A funding rate with the settlement time that follows it.

    `feed.FundingPoint` carries no `next_funding_ms` because a backtest reads the whole
    schedule from the historical rows and never has to look forward. Live, the *only* source
    of the upcoming settlement is `premiumIndex.nextFundingTime`, and without it
    `AutoFlatten.before_funding_ms` -- a platform exit the operator explicitly asked for --
    silently never fires. Spec 3.5 rule 3 and R17 both forbid assuming an 8-hour schedule,
    so it is observed rather than derived.
    """

    symbol: str
    ts_ms: int
    rate: int
    next_funding_ms: int


@dataclass(frozen=True, slots=True)
class _SymbolWatermark:
    """How far one symbol of one source has delivered, and when it last said so.

    The same two-timestamp split `reorder._Source` makes, for the same reason: `through_ms`
    is a market-data instant and `reported_wall_ms` is a wall-clock one. Judging a symbol
    silent on `through_ms` would retire a minute-cadence source fifteen seconds into every
    session. `through_ms` is `None` for a symbol that has been covered by a source but has
    not delivered anything yet, which is not the same claim as "delivered up to instant 0".
    """

    through_ms: int | None
    reported_wall_ms: int


@dataclass(frozen=True, slots=True)
class _PendingSettlement:
    """A funding boundary that has passed with no `fundingRate` row served for it yet.

    `noticed_wall_ms` is when the mark stream's `nextFundingTime` advanced past it, not the
    settlement instant: the grace in `FUNDING_PUBLICATION_GRACE_MS` bounds how long *we* wait,
    and a session that started hours after a boundary must not inherit a wait that already
    expired before it existed.
    """

    settlement_ms: int
    noticed_wall_ms: int


class MarkAggregator:
    """Turns 1 Hz mark samples into the one-minute bars the engine expects.

    Emits a bar when a sample lands in a later minute than the one being accumulated, so the
    bar is complete before it is published -- there is no partially-formed bar any consumer
    can reach, which is the same guarantee spec 6.2 makes about klines.

    A minute with no samples produces no bar. That is correct rather than convenient: spec
    3.4 is last-observation-carried-forward and forbids inventing a mark, so a gap in the
    feed must read as a gap and not as a flat minute nobody observed.
    """

    def __init__(self, symbol: str, *, window_ms: int = MARK_BAR_MS) -> None:
        self.symbol = symbol
        self._window_ms = window_ms
        self._bucket: int | None = None
        self._high = 0
        self._low = 0
        self._close = 0
        self._samples = 0

    def offer(self, ts_ms: int, price_scaled: int) -> MarkBar | None:
        """Absorb one sample. Returns the bar that just completed, if any."""
        bucket = ts_ms // self._window_ms
        if self._bucket is not None and bucket < self._bucket:
            # An out-of-order sample from a minute already published. Dropped rather than
            # folded in: that bar has been dispatched and the engine has acted on it, so
            # re-opening it would mean the range it was told about later changed.
            return None

        finished: MarkBar | None = None
        if self._bucket is None:
            self._bucket = bucket
        elif bucket > self._bucket:
            finished = self._close_bucket()
            self._bucket = bucket
            self._samples = 0

        if self._samples == 0:
            self._high = price_scaled
            self._low = price_scaled
        else:
            self._high = max(self._high, price_scaled)
            self._low = min(self._low, price_scaled)
        self._close = price_scaled
        self._samples += 1
        return finished

    def flush(self) -> MarkBar | None:
        """Publish the minute in progress. Called when the session is stopping.

        The bar is short -- it covers only the part of the minute that was observed -- and
        that is the honest encoding: the session ended there, so nothing was observed after.

        **It is still stamped at the whole minute's end, up to 59 999 ms after the session
        stopped observing, and that is deliberate.** `close_time` is not a description of when
        the last sample arrived; it is this bar's identity on the 1-minute `markPriceKlines`
        grid, which is the key a backtest's row for the same minute carries. Everything
        downstream reads it that way -- `engine.source` recovers a mark bar's *open* as
        `close_time - MARK_BAR_MS + 1`, and `_absorb_mark` below reports its watermark from
        the same grid -- so stamping the last observed sample instead would put an off-grid
        `MarkBar` on the tape for the shadow to replay. The tail costs nothing measurable
        either: exposure and every other ratio metric charge the final sample's interval to
        the run's end whatever that sample is stamped, so re-stamping it moves them by zero.

        The one real wart is that `PaperSession.seal` records `ended_ms` from the wall clock,
        so the tape can hold a row later than its own declared end. That is a property of the
        seal, not of this stamp.
        """
        if self._bucket is None or self._samples == 0:
            return None
        bar = self._close_bucket()
        self._bucket = None
        self._samples = 0
        return bar

    def _close_bucket(self) -> MarkBar:
        assert self._bucket is not None
        return MarkBar(
            symbol=self.symbol,
            close_time=self._bucket * self._window_ms + self._window_ms - 1,
            high=self._high,
            low=self._low,
            close=self._close,
        )


class LiveFeed:
    """Subscribes, polls, and hands finished events to a sink.

    The sink runs on the socket read path and **must not block** -- the collector's own
    docstring explains why, and it applies identically here: a slow consumer stops us
    draining the socket, Binance's send buffer fills, and it drops the connection, turning a
    slow strategy into a data gap. `PaperSession` offers each event to a reorder buffer,
    which is a list insert.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        venue: Venue = TESTNET,
        timeframe_ms: int = 60_000,
        on_event: Callable[[Event, int], None],
        on_status: Callable[[CollectorEventKind, str, str, int], None],
        allocator: SequenceAllocator | None = None,
        on_complete_through: Callable[[str, int], None] | None = None,
        on_funding_time: Callable[[str, int], None] | None = None,
    ) -> None:
        self.symbols = tuple(s.upper() for s in symbols)
        self.venue = venue
        self.timeframe_ms = timeframe_ms
        self.budget: Any | None = None
        """A shared `RateBudget`, set by the live worker before `run` (H9).

        The per-IP weight pool is one pool: this feed's pollers and the signed order
        client spend from the same allowance, and while only the signed side counted,
        the pollers could spend the ceiling on aggTrades alone and the ban arrived with
        positions open. The worker assigns the signed client's own budget here so both
        clients pre-check and observe against a single measured pool. `None` -- a paper
        session with no signed client, or a standalone use -- keeps the old unbudgeted
        behaviour, which is safe exactly when nothing else shares the IP's weight."""
        self._on_event = on_event
        self._on_status = on_status
        self._on_complete_through = on_complete_through
        self._on_funding_time = on_funding_time
        self._seq = allocator if allocator is not None else SequenceAllocator()

        self._marks = {s: MarkAggregator(s) for s in self.symbols}
        self._depth_bucket: dict[str, int] = {}
        self._depth_pending: dict[str, DepthSnapshot] = {}
        self._last_bar_open: dict[str, int] = {}
        self._pending_bars: dict[int, dict[str, Bar]] = {}
        self._last_funding: dict[str, int] = {}
        self._next_funding: dict[str, int] = {}
        self._funding_due: dict[str, _PendingSettlement] = {}
        self._agg_cursor: dict[str, int] = {}
        self._complete: dict[str, dict[str, _SymbolWatermark]] = {}
        self._silent_symbols: set[tuple[str, str]] = set()
        self._bars_seeded: set[str] = set()

        self.counts: dict[str, int] = {
            "trades": 0,
            "marks": 0,
            "mark_bars": 0,
            "depth": 0,
            "book_ticker": 0,
            "bars": 0,
            "funding": 0,
        }
        self._last_frame_ms = 0

    # ----------------------------------------------------------------------- lifecycle

    @property
    def streams(self) -> list[str]:
        lower = [s.lower() for s in self.symbols]
        return [f"{sym}{suffix}" for sym in lower for suffix in self.venue.ws_streams]

    @property
    def last_frame_ms(self) -> int:
        """Wall clock of the most recent frame from any source, for the live monitor."""
        return self._last_frame_ms

    async def run(self, stop: asyncio.Event) -> None:
        """Stream and poll until `stop` is set."""
        manager = StreamManager(
            self.streams,
            self._on_message,
            self._on_status,
            base_url=self.venue.ws_base,
        )
        async with PublicRestClient(self.venue.rest_base, budget=self.budget) as client:
            tasks = [
                asyncio.create_task(self._poll_klines(client, stop), name="poll-klines"),
                asyncio.create_task(self._poll_funding(client, stop), name="poll-funding"),
            ]
            if self.venue.polls_marks:
                tasks.append(
                    asyncio.create_task(self._poll_marks(client, stop), name="poll-marks")
                )
            if self.venue.polls_trades:
                tasks.append(
                    asyncio.create_task(self._poll_trades(client, stop), name="poll-trades")
                )
            try:
                await manager.run(stop)
            finally:
                # The pollers hold the REST client that is about to close; a task still
                # polling through `aclose` would raise into a context nothing is watching.
                stop.set()
                for task in tasks:
                    task.cancel()
                for task in tasks:
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as exc:  # noqa: BLE001 - a shutdown reports, never raises
                        # A poller that died earlier stored its exception here, and this is
                        # a shutdown path: re-raising it out of `run` propagates through
                        # `PaperSession.run`'s finally and skips the final drain, so a
                        # session loses the tape's last events, its metrics and its manifest
                        # to a poller that stopped hours ago. Recorded instead, and recorded
                        # as data rather than only as a log line, because the gap it left in
                        # a dataset has to be explainable from the tape.
                        self._report_handler_failure(task.get_name(), exc)

    def flush(self) -> None:
        """Publish whatever is part-accumulated. Called once, when the session stops."""
        for symbol, aggregator in self._marks.items():
            bar = aggregator.flush()
            if bar is not None:
                self._emit_mark(bar)

    # ------------------------------------------------------------------ socket frames

    def _on_message(self, stream: str, data: dict[str, Any], recv_ms: int) -> None:
        """Route one frame. On the read path, so it stays cheap and never raises."""
        self._last_frame_ms = recv_ms
        try:
            if stream.endswith("@bookTicker"):
                self._on_book_ticker(data, recv_ms)
            elif "@depth20" in stream:
                self._on_depth(data, recv_ms)
            elif stream.endswith("@aggTrade"):
                self._on_agg_trade(data, recv_ms)
            elif "@markPrice" in stream:
                self._on_mark_frame(data, recv_ms)
        except (KeyError, ValueError, TypeError) as exc:
            # One malformed frame must never take the other streams down with it. Reported
            # as data so the gap it causes is explainable from the tape rather than only
            # from a log nobody kept.
            self._on_status(
                CollectorEventKind.DISCONNECT,
                stream,
                f"unparseable payload: {type(exc).__name__}: {exc}",
                0,
            )

    def _on_book_ticker(self, data: dict[str, Any], recv_ms: int) -> None:
        symbol = str(data["s"]).upper()
        top = TopOfBook(
            symbol=symbol,
            ts_ms=int(data.get("T") or data["E"]),
            bid_px=to_scaled(data["b"]),
            bid_qty=to_scaled(data["B"]),
            ask_px=to_scaled(data["a"]),
            ask_qty=to_scaled(data["A"]),
        )
        self.counts["book_ticker"] += 1
        self._emit(EventKind.BOOK_UPDATE, "bookTicker", symbol, top.ts_ms, top, recv_ms)

    def _on_depth(self, data: dict[str, Any], recv_ms: int) -> None:
        """Buffer to the second, publishing the last snapshot of each bucket.

        Last rather than first, matching last-observation-carried-forward -- which is how
        the engine reads the series between samples (spec 3.4) and how the collector wrote
        the lake this will be compared against.
        """
        symbol = str(data["s"]).upper()
        ts = int(data.get("T") or data["E"])
        bucket = ts // DEPTH_BUCKET_MS
        held = self._depth_bucket.get(symbol)
        if held is not None and bucket != held:
            pending = self._depth_pending.pop(symbol, None)
            if pending is not None:
                self.counts["depth"] += 1
                self._emit(
                    EventKind.BOOK_UPDATE,
                    "depth20",
                    symbol,
                    pending.ts_ms,
                    pending,
                    recv_ms,
                )
            # Opening a new bucket proves every earlier one has been published **for this
            # symbol**. Reported so the buffer holds its watermark just short of the open
            # bucket instead of racing past a snapshot that is still accumulating -- and
            # reported per symbol, because the same race across symbols is what dropped 42%
            # of one symbol's ladders in a measured two-symbol session. See
            # `_report_complete`.
            self._report_complete("depth20", symbol, bucket * DEPTH_BUCKET_MS - 1)
        self._depth_bucket[symbol] = bucket

        # **A reordered frame must not replace a newer ladder with an older one** (M18).
        # `depth20` is a partial-stream snapshot with an `u` (update id) that only moves
        # forward; frames can still arrive out of order across a reconnect seam, and the
        # unconditional overwrite let a stale ladder become the bucket's published truth.
        # Guarded on both clocks -- the update id where present, the event time always --
        # because either alone can be zero on a malformed frame.
        held = self._depth_pending.get(symbol)
        update_id = int(data.get("u", 0))
        if held is not None and (
            ts < held.ts_ms
            or (update_id and held.last_update_id and update_id < held.last_update_id)
        ):
            return

        bids = data.get("b", [])
        asks = data.get("a", [])
        self._depth_pending[symbol] = DepthSnapshot(
            symbol=symbol,
            ts_ms=ts,
            recv_ms=recv_ms,
            last_update_id=update_id,
            bid_px=tuple(to_scaled(level[0]) for level in bids),
            bid_qty=tuple(to_scaled(level[1]) for level in bids),
            ask_px=tuple(to_scaled(level[0]) for level in asks),
            ask_qty=tuple(to_scaled(level[1]) for level in asks),
        )

    def _on_agg_trade(self, data: dict[str, Any], recv_ms: int) -> None:
        symbol = str(data["s"]).upper()
        print_ = TradePrint(
            symbol=symbol,
            ts_ms=int(data["T"]),
            price_scaled=to_scaled(data["p"]),
            qty_scaled=to_scaled(data["q"]),
            # 'm' true means the buyer was the maker, so the trade was sell-aggressive.
            # Inverting it reverses every queue-consumption decision in the fill model
            # (spec 6.4).
            is_buyer_maker=bool(data["m"]),
            agg_id=int(data["a"]),
        )
        self.counts["trades"] += 1
        self._emit(EventKind.TRADE, "aggTrades", symbol, print_.ts_ms, print_, recv_ms)

    def _on_mark_frame(self, data: dict[str, Any], recv_ms: int) -> None:
        symbol = str(data["s"]).upper()
        self._absorb_mark(
            symbol, int(data["E"]), to_scaled(data["p"]), recv_ms, data.get("T")
        )
        rate = data.get("r")
        if rate not in (None, ""):
            self._note_funding_rate(symbol, to_scaled(str(rate)))

    def _absorb_mark(
        self,
        symbol: str,
        ts_ms: int,
        price_scaled: int,
        recv_ms: int,
        next_funding: Any = None,
    ) -> None:
        self.counts["marks"] += 1
        if next_funding not in (None, "", 0):
            self._note_next_funding(symbol, int(next_funding))
        finished = self._marks[symbol].offer(ts_ms, price_scaled)
        if finished is not None:
            self._emit_mark(finished, recv_ms)
        # Mark bars are stamped at minute ends, every earlier one has been published **for
        # this symbol**, and the next cannot arrive before this minute ends -- so the sound
        # claim is the instant just *before* this minute's close, not the instant it opened.
        # The weaker version pinned the buffer for a whole minute at a time; see
        # `_poll_klines` for what that cost and why it is not merely an efficiency point.
        #
        # Per symbol, and folded into a minimum by `_report_complete`: this symbol ticking
        # into a new minute says nothing about a minute another symbol has not finished, and
        # claiming otherwise dropped every mark bar of the later-ticking symbol.
        self._report_complete(
            "markPrice", symbol, (ts_ms // MARK_BAR_MS) * MARK_BAR_MS + MARK_BAR_MS - 2
        )

    def _note_next_funding(self, symbol: str, announced_ms: int) -> None:
        """Record the upcoming settlement, and notice when the last one has just passed.

        **The advance is the only live evidence that a settlement happened.** `fundingRate`
        publishes the row itself seconds later (`FUNDING_PUBLICATION_GRACE_MS`), and until it
        does, the funding source must not claim to be complete past the boundary -- so the
        instant it is about to stop announcing is remembered here, where it is observed,
        rather than re-derived from a schedule spec 3.5 rule 3 and R17 both forbid assuming.
        """
        previous = self._next_funding.get(symbol, 0)
        if previous and announced_ms > previous:
            # `setdefault`: if an even earlier boundary is still outstanding, that is the one
            # holding the watermark, and forgetting it would let the release cutoff cross it.
            self._funding_due.setdefault(
                symbol,
                _PendingSettlement(settlement_ms=previous, noticed_wall_ms=_now_ms()),
            )
        if announced_ms != previous and self._on_funding_time is not None:
            # **The engine has to be told, or a platform exit silently never fires.** A
            # backtest reads the whole settlement schedule from the funding rows before it
            # starts; a live session has no such table, and `AutoFlatten.before_funding_ms`
            # -- an exit the operator explicitly configured -- resolves through
            # `_next_funding_ms`, which returns `None` when the engine's schedule is empty.
            # `_flatten_reason` then returns `None` and the deadline never fires, with no
            # error and nothing in the log to say the guarantee was not kept.
            self._on_funding_time(symbol, announced_ms)
        self._next_funding[symbol] = announced_ms

    def _emit_mark(self, bar: MarkBar, recv_ms: int | None = None) -> None:
        self.counts["mark_bars"] += 1
        self._emit(
            EventKind.MARK_PRICE_UPDATE,
            "markPrice",
            bar.symbol,
            bar.close_time,
            bar,
            recv_ms if recv_ms is not None else _now_ms(),
        )

    def _note_funding_rate(self, symbol: str, rate_scaled: int) -> None:
        self._last_funding[symbol] = rate_scaled

    # ---------------------------------------------------------------------- polling

    SLOW_SOURCES: dict[str, int] = {
        "klines": 15_000,
        "funding": 120_000,
        "markPrice": 15_000,
        "depth20": 15_000,
    }
    """Sources whose events reach the buffer well after the instant they describe, and how
    long each may go silent before the buffer stops waiting for it.

    **All four are late by construction, not by accident**, and each for its own reason:

    - `klines` close at T and are not published until the next poll, so they arrive a second
      or so after the wall clock has passed T.
    - `markPrice` bars are aggregated over a minute and cannot be emitted until a sample from
      the *next* minute proves the minute ended -- so a bar stamped at T is emitted at T+1s.
    - `depth20` is downsampled to one second (matching the lake), so the snapshot kept for a
      bucket is published when the following bucket opens, a second after its timestamp.
    - `funding` settlements are only discoverable from a REST poll after the fact.

    Without per-source watermarks every one of these is dropped as late by a 250 ms window.
    Measured on a real testnet session: **zero bar closes reached the engine, the strategy
    never traded, and nothing raised.** That is the exact shape of silent wrong answer this
    platform exists to refuse -- a session that looks healthy and does nothing.

    The staleness bounds are generous multiples of each source's real cadence, so they mean
    "this source has stopped", never "this source is slow". See `ReorderBuffer.expect`.
    """

    def _report_complete(self, source: str, symbol: str, ts_ms: int) -> None:
        """Tell the buffer this source has delivered everything up to `ts_ms` for `symbol`.

        **Tracked per symbol and reported as the minimum across them**, because a source is
        only complete through an instant when *every* symbol it covers is. The two socket-fed
        bucketed sources used to report their own symbol's bucket into a single per-source
        `max()`, which is the opposite claim: the first symbol to tick into minute k + 1
        carried the whole source past minute k's close, and every other symbol's minute-k bar
        -- which by construction cannot be emitted until that symbol's own next sample arrives
        -- was then refused by `ReorderBuffer.offer` as late. Measured on two-symbol sessions:
        every mark bar of the later-ticking symbol lost, and 42% of one symbol's depth
        ladders. A symbol with no mark of its own is valued at its entry price, so its
        unrealised PnL reads exactly zero and it can never be liquidated.

        **A symbol that has not spoken yet holds the whole report back**, rather than the
        others reporting over it. `ReorderBuffer.release_records` already has the right
        answer for a source that has said nothing -- hold the cutoff at its staleness bound,
        no further -- and that bound is what stops a symbol that never starts from freezing
        the session.

        **A symbol that has gone silent past that same bound is dropped from the minimum**,
        loudly. Without it the minimum is a new way to freeze: one symbol whose stream dies
        pins the cutoff at its last watermark for ever, while the others keep reporting, so
        the buffer never declares the source stalled and dispatch stops for the rest of the
        run. The bound is `SLOW_SOURCES[source]`, the same one the buffer applies to the
        source as a whole, because it means the same thing one level down.

        **Reported on every poll, even when the value has not moved.** An unchanged watermark
        is still a liveness signal, and suppressing the repeat is not an optimisation -- it is
        silence. A minute-cadence kline source advances its completion point once a minute
        and polls every second; deduping meant the buffer heard nothing for up to sixty
        seconds, declared the source stalled at fifteen, stopped waiting for it, and dropped
        **every** bar close for the rest of the session. Measured twice on real testnet
        sessions before the cause was found, both times with no error raised anywhere.
        """
        now = _now_ms()
        stale_after_ms = self.SLOW_SOURCES[source]
        covered = self._complete.get(source)
        if covered is None:
            # The first report from a source starts every symbol's silence clock, so a symbol
            # that never delivers is retired on the same bound as one that stops.
            covered = {s: _SymbolWatermark(None, now) for s in self.symbols}
            self._complete[source] = covered
        held = covered.get(symbol)
        through_ms = ts_ms
        if held is not None and held.through_ms is not None:
            # Monotonic per symbol for the reason `ReorderBuffer.complete_through` is
            # monotonic per source: a report that went backwards would re-open a stretch the
            # buffer had already released.
            through_ms = max(held.through_ms, ts_ms)
        covered[symbol] = _SymbolWatermark(through_ms=through_ms, reported_wall_ms=now)
        self._silent_symbols.discard((source, symbol))

        if self._on_complete_through is None:
            return
        complete_through: list[int] = []
        for name in self.symbols:
            watermark = covered[name]
            if now - watermark.reported_wall_ms > stale_after_ms:
                self._note_symbol_silent(source, name, now - watermark.reported_wall_ms)
                continue
            if watermark.through_ms is None:
                return
            complete_through.append(watermark.through_ms)
        if complete_through:
            self._on_complete_through(source, min(complete_through))

    def _note_symbol_silent(self, source: str, symbol: str, silent_for_ms: int) -> None:
        """Record that one symbol of a source has stopped holding the watermark back.

        Once per stretch of silence, not once per report: this runs on the socket read path
        at up to ten frames a second, and the status log is also the gap-explanation record
        (spec 4.5), so repeating it would bury the thing it describes. `STALE` rather than
        `DISCONNECT` because the connection is fine -- and because `PaperSession` measures
        spec 7's disconnect trigger from `DISCONNECT`, which would turn one quiet symbol into
        a halted session.
        """
        if (source, symbol) in self._silent_symbols:
            return
        self._silent_symbols.add((source, symbol))
        detail = (
            f"no completion report for {silent_for_ms} ms, past this source's "
            f"{self.SLOW_SOURCES[source]} ms bound; it no longer holds the release watermark "
            f"back, so anything it delivers from here is dropped as late"
        )
        self._on_status(CollectorEventKind.STALE, live_dataset_id(source, symbol), detail, 0)
        log.warning("%s:%s silent for %d ms", source, symbol, silent_for_ms)

    async def _poll_klines(self, client: PublicRestClient, stop: asyncio.Event) -> None:
        """The universal bar source: `GET /fapi/v1/klines`, every two seconds.

        REST rather than `@kline_1m` deliberately, even on testnet where the stream works.
        Production suppresses it (finding F4), and a feed whose bar source depends on the
        endpoint would make a session recorded on one venue structurally different from one
        recorded on the other -- for the single most load-bearing series there is. One code
        path, one shape, at the cost of at most two seconds of latency after a bar closes.

        Only *closed* bars are emitted. The last row Binance returns is the bar in progress,
        and publishing it would hand the strategy a partially-formed bar -- the exact
        look-ahead that spec 6.2 makes structurally impossible in a backtest.
        """
        while not stop.is_set():
            for symbol in self.symbols:
                try:
                    rows = await client.klines(symbol, interval="1m", limit=3)
                except Exception as exc:  # noqa: BLE001 - a poller reports and continues
                    self._report_poll_failure("klines", exc)
                    break
                # **Everything done with the response is guarded too, not just the fetch.**
                # `_offer_bar` reaches the session's reorder buffer, which raises
                # `ReorderOverflow` when it is full and `OrderingViolation` on a duplicate
                # key; a malformed row raises out of the parse. Outside the guard any of
                # those ended this task, and nothing observes a poller task until the session
                # is over -- so the strategy stopped receiving `on_bar` for the rest of the
                # run while the monitor still read market='up', with no status entry, no
                # warning and no log line saying why. That is the same silent no-op
                # `SLOW_SOURCES` describes, reached a second way.
                try:
                    for row in rows[:-1]:
                        self._offer_bar(symbol, row)
                    # **Seeded after the whole first page, not after its first row.** A poll
                    # returns several closed bars, and every one of them closed before the
                    # session existed. Seeding per row let the second of them through,
                    # stamped minutes behind the engine's clock, where it was dropped as late
                    # -- so the very first bar of every session was lost to a bug that
                    # reported nothing.
                    self._bars_seeded.add(symbol)
                    if rows:
                        # **The instant just before the next bar close**, which is the
                        # strongest sound claim this source can make: klines produce events
                        # only at minute boundaries, every earlier one has been delivered,
                        # and the next cannot arrive before the open bar ends.
                        #
                        # Reporting the *open bar's start* instead was sound but far too
                        # weak. It does not move for a whole minute, so the buffer's `min()`
                        # held every trade, quote and mark stamped inside that minute until
                        # roughly a second after it ended: market data reached the engine in
                        # one-minute bursts with up to sixty seconds of processing lag.
                        # Event-time ordering and the parity result were unaffected -- events
                        # keep their exchange timestamps -- but `AutoFlatten` deadlines, the
                        # disconnect trigger and the live monitor all ran a minute behind
                        # reality, which on a platform whose whole claim is that paper and
                        # live behave alike is a real divergence.
                        self._report_complete("klines", symbol, int(rows[-1][6]) - 1)
                except Exception as exc:  # noqa: BLE001 - see above
                    self._report_handler_failure("klines", exc)
                    break
            if await _sleep_or_stop(stop, 1.0):
                return

    def _offer_bar(self, symbol: str, row: Sequence[Any]) -> None:
        """Assemble a closed kline into a `BarStep` once every symbol has one.

        Grouped rather than emitted per symbol because indicators for *all* symbols must
        advance before *any* `on_bar` runs (`feed.BarStep`). A pairs strategy reading the
        other leg in its first `on_bar` would otherwise get a value one bar stale.
        """
        open_time = int(row[0])
        if self._last_bar_open.get(symbol, -1) >= open_time:
            return
        self._last_bar_open[symbol] = open_time
        if symbol not in self._bars_seeded:
            # **The first poll returns bars that closed before the session existed.**
            # Recorded as the high-water mark and not dispatched: they belong to a market the
            # strategy was not watching, their timestamps are minutes behind the engine's
            # clock, and feeding them would make the first `on_bar` of every session fire on
            # stale data. Warm-up is the lake's job, not the feed's.
            return
        close_time = int(row[6])
        bar = Bar(
            symbol=symbol,
            open_time=open_time,
            close_time=close_time,
            open=to_scaled(row[1]),
            high=to_scaled(row[2]),
            low=to_scaled(row[3]),
            close=to_scaled(row[4]),
            volume=to_scaled(row[5]),
            quote_volume=to_scaled(row[7]),
            trades=int(row[8]),
        )
        pending = self._pending_bars.setdefault(close_time, {})
        pending[symbol] = bar
        if len(pending) < len(self.symbols):
            return
        del self._pending_bars[close_time]
        # Anything older than the step just completed can never complete: its missing
        # symbols' bars have been superseded. Dropped rather than left to grow, and dropped
        # rather than fed short -- a step missing a leg would let a pairs strategy compare
        # this minute against a stale one with no way to notice.
        for stale in [k for k in self._pending_bars if k < close_time]:
            del self._pending_bars[stale]
        step = BarStep(
            close_time=close_time, bars=tuple(pending[s] for s in self.symbols)
        )
        self.counts["bars"] += 1
        self._emit(EventKind.BAR_CLOSE, "klines", self.symbols[0], close_time, step, _now_ms())

    async def _poll_marks(self, client: PublicRestClient, stop: asyncio.Event) -> None:
        """`premiumIndex` at 1 Hz where the endpoint suppresses `@markPrice@1s`.

        The server stamps a fresh `time` every second, so this reproduces exactly what the
        stream would have delivered. Spec 3.4 forbids computing a mark price and nothing
        here does: the value is recorded as served.
        """
        last: dict[str, int] = {}
        while not stop.is_set():
            for symbol in self.symbols:
                try:
                    payload = await client.premium_index(symbol)
                except Exception as exc:  # noqa: BLE001
                    self._report_poll_failure("markPrice", exc)
                    break
                try:
                    # Guarded as well as the fetch: `_absorb_mark` emits into the session's
                    # reorder buffer and parses the payload, and either can raise. See
                    # `_poll_klines` for what an unguarded emit cost.
                    ts = int(payload["time"])
                    if last.get(symbol, -1) >= ts:
                        continue  # the endpoint re-served a sample already held
                    last[symbol] = ts
                    self._last_frame_ms = _now_ms()
                    self._absorb_mark(
                        symbol,
                        ts,
                        to_scaled(payload["markPrice"]),
                        self._last_frame_ms,
                        payload.get("nextFundingTime"),
                    )
                    rate = payload.get("lastFundingRate")
                    if rate not in (None, ""):
                        self._note_funding_rate(symbol, to_scaled(str(rate)))
                except Exception as exc:  # noqa: BLE001 - see above
                    self._report_handler_failure("markPrice", exc)
                    break
            if await _sleep_or_stop(stop, 1.0):
                return

    async def _poll_trades(self, client: PublicRestClient, stop: asyncio.Event) -> None:
        """`aggTrades` with a `fromId` cursor where `@aggTrade` is suppressed.

        Gaplessness comes from the cursor, not from polling quickly: each tick asks from
        `last_seen + 1`, so falling behind costs latency and never data. A dropped WebSocket
        frame, by contrast, leaves nothing in the record to say a message was skipped.
        """
        while not stop.is_set():
            for symbol in self.symbols:
                try:
                    await self._walk_trades(client, symbol)
                except Exception as exc:  # noqa: BLE001
                    self._report_poll_failure("aggTrades", exc)
                    break
            if await _sleep_or_stop(stop, 1.0):
                return

    async def _walk_trades(self, client: PublicRestClient, symbol: str) -> None:
        cursor = self._agg_cursor.get(symbol)
        if cursor is None:
            head = await client.agg_trades(symbol, limit=1)
            if not head:
                return
            self._agg_cursor[symbol] = int(head[-1]["a"]) + 1
            return
        page = await client.agg_trades(symbol, from_id=cursor, limit=1000)
        if not page:
            return
        recv = _now_ms()
        self._last_frame_ms = recv
        for row in page:
            self._on_agg_trade({**row, "s": symbol}, recv)
        self._agg_cursor[symbol] = int(page[-1]["a"]) + 1

    async def _poll_funding(self, client: PublicRestClient, stop: asyncio.Event) -> None:
        """Emit a `FUNDING_SETTLEMENT` for each settlement that lands during the session.

        Read from `fundingRate` rather than derived from a schedule, because spec 3.5 rule 2
        says to use the actual rates at the actual timestamps and R17 records that Binance
        varies the interval by symbol and has changed it on existing ones.

        **Settlements from before the session began are seeded, not emitted.** They already
        happened; replaying one would charge the account a cashflow it was never party to,
        at a timestamp hours behind the engine's clock. The first poll therefore only records
        what the latest settlement *was*, so the second poll can tell a new one from it.

        `nextFundingTime` is what makes funding cheap to wait for. No settlement can occur
        before it, so between boundaries this source is complete right up to the present and
        holds nothing back; only in the seconds after a boundary does it pin the watermark --
        which is what `_funding_horizon` exists to keep true.
        """
        seeded: set[str] = set()
        seen: dict[str, int] = {}
        while not stop.is_set():
            for symbol in self.symbols:
                try:
                    rows = await client.funding_rate(symbol, limit=2)
                except Exception as exc:  # noqa: BLE001
                    self._report_poll_failure("funding", exc)
                    break
                try:
                    # Guarded as well as the fetch, for the reason `_poll_klines` gives: the
                    # emit below reaches the session's reorder buffer, and an exception from
                    # there used to end this task with nothing recorded.
                    for row in rows:
                        ts = int(row["fundingTime"])
                        if seen.get(symbol, 0) >= ts:
                            continue
                        seen[symbol] = ts
                        if symbol not in seeded:
                            continue
                        rate = to_scaled(str(row["fundingRate"]))
                        self.counts["funding"] += 1
                        self._emit(
                            EventKind.FUNDING_SETTLEMENT,
                            "funding",
                            symbol,
                            ts,
                            FundingPoint(symbol=symbol, ts_ms=ts, rate=rate),
                            _now_ms(),
                        )
                    seeded.add(symbol)
                    horizon = self._funding_horizon(symbol, seen.get(symbol, 0))
                    if horizon:
                        self._report_complete("funding", symbol, horizon)
                except Exception as exc:  # noqa: BLE001 - see above
                    self._report_handler_failure("funding", exc)
                    break
            # **Faster while a settlement is outstanding.** The whole session's dispatch is
            # pinned behind an unpublished settlement (see `_funding_horizon`), so the window
            # is closed at the rate the other pollers already run at rather than at this
            # one's idle cadence. Idle, a source that only produces an event every eight
            # hours does not deserve a poll a second.
            if await _sleep_or_stop(stop, 1.0 if self._funding_due else 5.0):
                return

    def _funding_horizon(self, symbol: str, settled_through_ms: int) -> int:
        """How far the funding source can honestly claim to be complete for one symbol.

        Zero means "no claim at all", which is not the same as "complete through zero": the
        caller then stays silent and `ReorderBuffer` holds the cutoff at this source's
        staleness bound instead of at an instant nobody vouched for.

        **A settlement that has happened but has not been published yet holds the watermark
        at the boundary.** The mark stream advances `nextFundingTime` within a second of the
        boundary; `GET /fapi/v1/fundingRate` served the row itself 1 926 ms after it in the
        measurement behind `FUNDING_PUBLICATION_GRACE_MS`, and its visibility then flapped
        across nodes for another ten seconds. Reporting `now` in that gap is a false
        completeness claim, and it is the *normal* case rather than an edge one: the release
        watermark crosses the settlement instant, and the `FUNDING_SETTLEMENT` that arrives a
        poll later is refused as late. Measured end to end, that silently drops the cashflow
        -- `counts['funding']` still says 1, `funding_pnl` reads a confident 0, and
        `FUNDING_UNSETTLED` cannot fire because the engine never saw the event. On an
        eight-hourly schedule with a levered position it is the largest non-fill cashflow in
        the run.

        Without `nextFundingTime` the source says nothing. Reporting the last settlement seen
        instead looks conservative and is a live-lock: that instant can be eight hours old,
        it never moves, and because the source keeps reporting it the buffer never declares it
        stale -- so the session dispatches nothing at all, for ever.
        """
        now = _now_ms()
        pending = self._funding_due.get(symbol)
        if pending is not None:
            if settled_through_ms >= pending.settlement_ms:
                del self._funding_due[symbol]
            elif now - pending.noticed_wall_ms <= FUNDING_PUBLICATION_GRACE_MS:
                return min(now, pending.settlement_ms - 1)
            else:
                del self._funding_due[symbol]
                self._on_status(
                    CollectorEventKind.STALE,
                    live_dataset_id("funding", symbol),
                    f"no fundingRate row for the settlement at "
                    f"{pending.settlement_ms} within {FUNDING_PUBLICATION_GRACE_MS} ms of "
                    f"the boundary passing; the release watermark stops waiting for it, so "
                    f"that cashflow will be dropped as late if it is published now",
                    0,
                )
                log.warning(
                    "%s funding settlement at %d never published; giving up",
                    symbol,
                    pending.settlement_ms,
                )
        nxt = self._next_funding.get(symbol, 0)
        if not nxt:
            return 0
        return min(now, nxt - 1)

    def _report_poll_failure(self, dataset: str, exc: BaseException) -> None:
        self._on_status(
            CollectorEventKind.DISCONNECT,
            dataset,
            f"poll failed: {type(exc).__name__}: {exc}",
            0,
        )
        log.warning("%s poll failed: %s: %s", dataset, type(exc).__name__, exc)

    def _report_handler_failure(self, label: str, exc: BaseException) -> None:
        """Our own handling of a good response raised. Recorded, and the poller lives on.

        `label` is the dataset, or the task's name where a whole poller ended.

        Deliberately *not* `_report_poll_failure`: the endpoint answered, so calling this a
        disconnect would blame a working server for a bug on this side, and `PaperSession`
        measures spec 7's `max_disconnect_seconds` trigger from `DISCONNECT` records -- so a
        full reorder buffer would present as a socket outage and could halt a session holding
        an open position. `StreamManager._deliver` makes the same split on the socket path,
        with the same wording, for the same reason.
        """
        self._on_status(
            CollectorEventKind.STALE,
            label,
            f"handler raised: {type(exc).__name__}: {exc}",
            0,
        )
        log.warning("%s handler raised: %s: %s", label, type(exc).__name__, exc)

    # ---------------------------------------------------------------------- emitting

    def _emit(
        self,
        kind: EventKind,
        stream: str,
        symbol: str,
        ts_ms: int,
        payload: Any,
        recv_ms: int,
    ) -> None:
        """Stamp the spec-6.2 key and hand the event on.

        **`source_seq` is allocated here, never taken from the exchange.** `ticks` uses the
        aggregate-trade id as `source_seq` with a constant `dataset_id`, which is unique in
        the lake because a replay covers one symbol's rows at a time -- but Binance's agg ids
        are per-symbol, so live, two symbols printing the same id in the same millisecond
        would produce identical total-order keys and `EventQueue.pop` would raise
        `OrderingViolation` and kill a session holding an open position.
        """
        dataset_id, seq = self._seq.allocate_live(stream, symbol)
        self._on_event(
            Event(
                ts_ms=ts_ms,
                kind=kind,
                source_seq=seq,
                dataset_id=dataset_id,
                payload=payload,
            ),
            recv_ms,
        )

    def funding_times(self) -> dict[str, list[int]]:
        """Upcoming settlements observed from `nextFundingTime`, for the tape's meta."""
        return {s: [t] for s, t in self._next_funding.items() if t}


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    """Wait, returning `True` if the session was asked to stop meanwhile."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
        return True
    except TimeoutError:
        return False


def _now_ms() -> int:
    """Wall-clock epoch milliseconds. Not an engine clock -- the engine's clock is the
    timestamp on the event it is dispatching (spec 5.3)."""
    return int(time.time() * 1000)
