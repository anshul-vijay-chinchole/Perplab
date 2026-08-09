"""Tests for the REST pollers that replaced three suppressed WebSocket streams (F4).

These datasets are no longer optional extras: `markPrice` is what liquidation is decided
against (spec 3.7) and `aggTrades` drives the fill model (spec 6.4). The WebSocket source
for both delivers nothing on this endpoint, so everything below is testing the only path
by which that data now arrives.

The properties worth pinning are the ones that make polling *safe* rather than merely
working: the id cursor never skips, a full page is followed immediately rather than at the
next tick, duplicate mark samples are dropped, and no failure anywhere takes a poller
permanently offline without saying so in the data.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from perplab.core.types import CollectorEventKind
from perplab.data.rest_poller import (
    MAX_CATCHUP_PAGES,
    AggTradePoller,
    MarkPricePoller,
    RestPoller,
    latest_recorded_ts,
)
from perplab.data.schemas import SCHEMAS
from perplab.data.writer import ParquetBufferedWriter
from perplab.exchange.rest import AGG_TRADES_MAX_LIMIT

SYMBOL = "BTCUSDT"


class Recorder:
    """Collects what a poller emits, standing in for the collector."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, list[dict[str, Any]]]] = []
        self.events: list[tuple[CollectorEventKind, str, str, int]] = []

    def on_rows(self, dataset: str, rows: Any) -> None:
        self.rows.append((dataset, list(rows)))

    def on_event(
        self, kind: CollectorEventKind, stream: str, detail: str, downtime_ms: int
    ) -> None:
        self.events.append((kind, stream, detail, downtime_ms))

    @property
    def all_rows(self) -> list[dict[str, Any]]:
        return [r for _, rows in self.rows for r in rows]

    def kinds(self) -> list[CollectorEventKind]:
        return [k for k, _, _, _ in self.events]


class FakeClient:
    """A `PublicRestClient` shaped just enough for the pollers."""

    def __init__(self) -> None:
        self.premium: list[dict[str, Any]] = []
        self.pages: list[list[dict[str, Any]]] = []
        self.agg_calls: list[dict[str, Any]] = []
        self.fail_with: Exception | None = None

    async def premium_index(self, symbol: str) -> dict[str, Any]:
        if self.fail_with is not None:
            raise self.fail_with
        return self.premium.pop(0)

    async def agg_trades(
        self, symbol: str, *, from_id: int | None = None, limit: int = 1000
    ) -> list[dict[str, Any]]:
        if self.fail_with is not None:
            raise self.fail_with
        self.agg_calls.append({"from_id": from_id, "limit": limit})
        return self.pages.pop(0) if self.pages else []

    @property
    def fetch_calls(self) -> list[dict[str, Any]]:
        """Paging requests only, excluding the cursor-anchoring call `prime` makes.

        `prime` asks with no `from_id`, so counting it alongside the paging calls would
        make every page-count assertion below off by one for a reason unrelated to paging.
        """
        return [c for c in self.agg_calls if c["from_id"] is not None]


def mark(ts: int, price: str = "63000.5") -> dict[str, Any]:
    return {
        "time": ts,
        "markPrice": price,
        "indexPrice": "63001.25",
        "estimatedSettlePrice": "63002.00",
        "lastFundingRate": "0.00008473",
        "nextFundingTime": 1785686400000,
    }


def agg(agg_id: int, ts: int = 1_700_000_000_000) -> dict[str, Any]:
    return {
        "a": agg_id,
        "p": "63000.10",
        "q": "0.25",
        "f": agg_id * 2,
        "l": agg_id * 2 + 1,
        "T": ts + agg_id,
        "m": agg_id % 2 == 0,
    }


class TestMarkPricePoller:
    @pytest.mark.asyncio
    async def test_builds_a_row_in_the_dataset_schema(self) -> None:
        rec, client = Recorder(), FakeClient()
        client.premium = [mark(1_700_000_000_000, "63000.5")]
        poller = MarkPricePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        rows = await poller.fetch()

        assert len(rows) == 1
        row = rows[0]
        assert row["ts_ms"] == 1_700_000_000_000
        # Scaled int64, not float: the money seam (spec 3.1) reaches this far.
        assert row["mark_price"] == 6_300_050_000_000
        assert isinstance(row["mark_price"], int)
        assert row["next_funding_ms"] == 1785686400000

    @pytest.mark.asyncio
    async def test_drops_a_repeated_timestamp(self) -> None:
        """The endpoint re-serves the same sample if polled inside its update window.

        Recording it twice would give a last-observation-carried-forward read two candidate
        rows for one instant, and would inflate the row count the gap detector reasons
        about -- so a stalled mark price would look like a healthy one.
        """
        rec, client = Recorder(), FakeClient()
        client.premium = [mark(1000), mark(1000), mark(1001)]
        poller = MarkPricePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        assert len(await poller.fetch()) == 1
        assert await poller.fetch() == []
        assert len(await poller.fetch()) == 1

    @pytest.mark.asyncio
    async def test_drops_a_timestamp_that_goes_backwards(self) -> None:
        """A stale server response must not rewrite history behind the cursor."""
        rec, client = Recorder(), FakeClient()
        client.premium = [mark(5000), mark(4000)]
        poller = MarkPricePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        assert len(await poller.fetch()) == 1
        assert await poller.fetch() == []


class TestCursorSeeding:
    """Finding H26: an in-memory-only dedup cursor forgets everything on restart, so the
    first poll after every restart re-recorded the sample the previous process had
    already flushed -- two rows for one instant with different `recv_ms`."""

    @staticmethod
    def _flush_mark_rows(root: Path, timestamps: list[int]) -> None:
        with ParquetBufferedWriter(
            root, "markPrice", SCHEMAS["markPrice"], symbol=SYMBOL
        ) as writer:
            for ts in timestamps:
                writer.append(
                    {
                        "ts_ms": ts,
                        "recv_ms": ts + 3,
                        "mark_price": 6_300_050_000_000,
                        "index_price": 6_300_125_000_000,
                        "estimated_settle_price": 0,
                        "last_funding_rate": 8_473,
                        "next_funding_ms": 1_785_686_400_000,
                    }
                )

    def test_latest_recorded_ts_reads_the_newest_partition(self, tmp_path: Path) -> None:
        day = 86_400_000
        base = 1_704_067_200_000  # 2024-01-01T00:00:00Z
        self._flush_mark_rows(tmp_path, [base, base + 30_000])
        self._flush_mark_rows(tmp_path, [base + day, base + day + 45_000])

        assert latest_recorded_ts(tmp_path, "markPrice", SYMBOL) == base + day + 45_000

    def test_an_empty_lake_seeds_nothing(self, tmp_path: Path) -> None:
        assert latest_recorded_ts(tmp_path, "markPrice", SYMBOL) is None

    @pytest.mark.asyncio
    async def test_a_restarted_poller_does_not_rewrite_the_flushed_sample(self) -> None:
        """The restart scenario itself: the endpoint re-serves the sample the previous
        process flushed, and the seeded cursor drops it instead of storing a twin."""
        rec, client = Recorder(), FakeClient()
        client.premium = [mark(1_000), mark(1_001)]
        poller = MarkPricePoller(
            SYMBOL,
            client,
            on_rows=rec.on_rows,
            on_event=rec.on_event,
            resume_from_ms=1_000,  # what latest_recorded_ts returns for the flushed lake
        )

        assert await poller.fetch() == [], "the re-served sample is already on disk"
        assert len(await poller.fetch()) == 1, "a genuinely new sample still lands"


class TestAggTradePoller:
    @pytest.mark.asyncio
    async def test_primes_just_past_the_head(self) -> None:
        rec, client = Recorder(), FakeClient()
        client.pages = [[agg(500)]]
        poller = AggTradePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        await poller.prime()

        assert poller._next_id == 501
        assert rec.kinds() == [CollectorEventKind.CONNECT]

    @pytest.mark.asyncio
    async def test_walks_the_cursor_forward_without_gaps(self) -> None:
        rec, client = Recorder(), FakeClient()
        client.pages = [[agg(10)], [agg(11), agg(12)], [agg(13)]]
        poller = AggTradePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        await poller.prime()
        first = await poller.fetch()
        second = await poller.fetch()

        assert [r["agg_id"] for r in first] == [11, 12]
        assert [r["agg_id"] for r in second] == [13]
        # Every request asks from exactly one past the last id seen. That is the whole
        # gaplessness argument: it cannot skip an id without the request itself showing it.
        assert [c["from_id"] for c in client.fetch_calls] == [11, 13]

    @pytest.mark.asyncio
    async def test_follows_a_full_page_immediately(self) -> None:
        """A page returned exactly full may have been truncated.

        Waiting for the next tick would let the poller fall further behind on every tick
        during a burst, which is the one way `fromId` paging can turn latency into a
        backlog it never clears.
        """
        rec, client = Recorder(), FakeClient()
        full = [agg(i) for i in range(1, AGG_TRADES_MAX_LIMIT + 1)]
        client.pages = [[agg(0)], full, [agg(AGG_TRADES_MAX_LIMIT + 1)]]
        poller = AggTradePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        await poller.prime()
        rows = await poller.fetch()

        assert len(rows) == AGG_TRADES_MAX_LIMIT + 1
        assert len(client.fetch_calls) == 2

    @pytest.mark.asyncio
    async def test_a_short_page_ends_the_tick(self) -> None:
        rec, client = Recorder(), FakeClient()
        client.pages = [[agg(0)], [agg(1)], [agg(2)]]
        poller = AggTradePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        await poller.prime()
        rows = await poller.fetch()

        assert [r["agg_id"] for r in rows] == [1]
        assert len(client.fetch_calls) == 1

    @pytest.mark.asyncio
    async def test_catchup_is_bounded(self) -> None:
        """Catch-up must yield, or a long backlog starves flushing and `stop`.

        Without the cap the poller can stay inside one tick for minutes while nothing
        flushes and Ctrl-C goes unread, which reads to an operator as a hang.
        """
        rec, client = Recorder(), FakeClient()
        client.pages = [[agg(0)]] + [
            [agg(i * AGG_TRADES_MAX_LIMIT + j) for j in range(1, AGG_TRADES_MAX_LIMIT + 1)]
            for i in range(MAX_CATCHUP_PAGES + 5)
        ]
        poller = AggTradePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        await poller.prime()
        await poller.fetch()

        assert len(client.fetch_calls) == MAX_CATCHUP_PAGES

    @pytest.mark.asyncio
    async def test_reports_an_id_jump_rather_than_absorbing_it(self) -> None:
        """Ids have been dense in every observation, so a hole is worth recording.

        Absorbing it silently would leave the lake with a discontinuity that no later audit
        could distinguish from a collector bug.
        """
        rec, client = Recorder(), FakeClient()
        client.pages = [[agg(100)], [agg(105)]]
        poller = AggTradePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        await poller.prime()
        rows = await poller.fetch()

        assert [r["agg_id"] for r in rows] == [105]
        stale = [e for e in rec.events if e[0] is CollectorEventKind.STALE]
        assert len(stale) == 1
        assert "asked from 101" in stale[0][2] and "got 105" in stale[0][2]

    @pytest.mark.asyncio
    async def test_preserves_the_aggressor_flag(self) -> None:
        """Inverting `m` reverses every queue-consumption decision in the fill model."""
        rec, client = Recorder(), FakeClient()
        client.pages = [[agg(0)], [agg(1), agg(2)]]
        poller = AggTradePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        await poller.prime()
        rows = await poller.fetch()

        assert [r["is_buyer_maker"] for r in rows] == [False, True]

    @pytest.mark.asyncio
    async def test_prime_refuses_an_empty_head(self) -> None:
        rec, client = Recorder(), FakeClient()
        client.pages = [[]]
        poller = AggTradePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        with pytest.raises(RuntimeError, match="no head record"):
            await poller.prime()


class TestFailureHandling:
    """A poller that dies takes its dataset offline for the rest of the run."""

    @pytest.mark.asyncio
    async def test_a_failure_is_recorded_not_raised(self) -> None:
        rec, client = Recorder(), FakeClient()
        client.fail_with = RuntimeError("endpoint on fire")
        poller = MarkPricePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        assert await poller._guarded(poller._poll_and_emit, "poll") is False

        assert rec.kinds() == [CollectorEventKind.DISCONNECT]
        assert "endpoint on fire" in rec.events[0][2]
        assert rec.events[0][1] == "markPrice"

    @pytest.mark.asyncio
    async def test_recovery_is_recorded_with_downtime(self) -> None:
        """An outage needs a measurable end, not just a beginning."""
        rec, client = Recorder(), FakeClient()
        client.fail_with = RuntimeError("boom")
        poller = MarkPricePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        await poller._guarded(poller._poll_and_emit, "poll")
        await poller._guarded(poller._poll_and_emit, "poll")

        client.fail_with = None
        client.premium = [mark(1)]
        assert await poller._guarded(poller._poll_and_emit, "poll") is True

        assert rec.kinds() == [
            CollectorEventKind.DISCONNECT,
            CollectorEventKind.DISCONNECT,
            CollectorEventKind.RECONNECT,
        ]
        assert "2 consecutive failure(s)" in rec.events[-1][2]

    @pytest.mark.asyncio
    async def test_consecutive_failures_are_counted(self) -> None:
        rec, client = Recorder(), FakeClient()
        client.fail_with = RuntimeError("boom")
        poller = MarkPricePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        for _ in range(3):
            await poller._guarded(poller._poll_and_emit, "poll")

        assert "3 consecutive" in rec.events[-1][2]

    @pytest.mark.asyncio
    async def test_cancellation_is_not_swallowed(self) -> None:
        """Catching broadly must not extend to the shutdown signal.

        Swallowing `CancelledError` would make the poller unkillable and hang every clean
        stop, turning a graceful shutdown into a kill and a spurious RESTART record.
        """
        rec, client = Recorder(), FakeClient()
        poller = MarkPricePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event
        )

        async def cancelled() -> None:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await poller._guarded(cancelled, "poll")

    @pytest.mark.asyncio
    async def test_run_stops_promptly_when_asked(self) -> None:
        rec, client = Recorder(), FakeClient()
        client.premium = [mark(i) for i in range(1, 200)]
        poller = MarkPricePoller(
            SYMBOL, client, on_rows=rec.on_rows, on_event=rec.on_event, interval_s=0.01
        )
        stop = asyncio.Event()

        task = asyncio.create_task(poller.run(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)

        assert rec.all_rows, "expected at least one sample before stopping"

    @pytest.mark.asyncio
    async def test_run_survives_a_failing_endpoint(self) -> None:
        """The loop must keep going, not exit, when every poll fails."""
        rec, client = Recorder(), FakeClient()
        client.fail_with = RuntimeError("down")
        poller = RestPollerStub(on_rows=rec.on_rows, on_event=rec.on_event)
        stop = asyncio.Event()

        task = asyncio.create_task(poller.run(stop))
        await asyncio.sleep(0.05)
        assert not task.done(), "poller exited instead of backing off"
        stop.set()
        await asyncio.wait_for(task, timeout=3.0)


class RestPollerStub(RestPoller):
    """Always fails, to exercise the retry loop without a network."""

    def __init__(self, *, on_rows: Any, on_event: Any) -> None:
        super().__init__(
            "markPrice", interval_s=0.01, on_rows=on_rows, on_event=on_event
        )

    async def fetch(self) -> list[dict[str, Any]]:
        raise RuntimeError("always down")
