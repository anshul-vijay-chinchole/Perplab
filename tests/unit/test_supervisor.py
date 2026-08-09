"""Tests for crash supervision and collector restart reporting.

These cover the failure mode that would otherwise end a 72-hour unattended run silently:
the collector process dying with nothing to restart it and no record that it happened.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from perplab.data.collector import Collector
from perplab.data.supervisor import supervise
from perplab.data.writer import ParquetBufferedWriter


class TestSupervise:
    @pytest.mark.asyncio
    async def test_clean_return_is_not_a_crash(self) -> None:
        stop = asyncio.Event()
        calls = 0

        async def run_once() -> None:
            nonlocal calls
            calls += 1

        restarts = await supervise(run_once, stop)
        assert (calls, restarts) == (1, 0)

    @pytest.mark.asyncio
    async def test_restarts_after_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("perplab.data.supervisor.BACKOFF_INITIAL_S", 0.001)
        monkeypatch.setattr("perplab.data.supervisor.BACKOFF_CAP_S", 0.001)

        stop = asyncio.Event()
        attempts = 0

        async def flaky() -> None:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError("simulated crash in a stream handler")

        restarts = await supervise(flaky, stop)
        assert attempts == 3
        assert restarts == 2

    @pytest.mark.asyncio
    async def test_cancellation_propagates(self) -> None:
        """A supervisor that swallows CancelledError makes the process unkillable.

        That is a worse failure than the crash it is guarding against.
        """
        stop = asyncio.Event()

        async def cancelled() -> None:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await supervise(cancelled, stop)

    @pytest.mark.asyncio
    async def test_gives_up_after_max_restarts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("perplab.data.supervisor.BACKOFF_INITIAL_S", 0.001)
        monkeypatch.setattr("perplab.data.supervisor.BACKOFF_CAP_S", 0.001)
        stop = asyncio.Event()

        async def always_fails() -> None:
            raise RuntimeError("permanent fault")

        with pytest.raises(RuntimeError, match="exceeded 3 restarts"):
            await supervise(always_fails, stop, max_restarts=3)

    @pytest.mark.asyncio
    async def test_stop_ends_the_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("perplab.data.supervisor.BACKOFF_INITIAL_S", 0.001)
        stop = asyncio.Event()
        attempts = 0

        async def crash_then_stop() -> None:
            nonlocal attempts
            attempts += 1
            stop.set()
            raise RuntimeError("crash on the way out")

        await supervise(crash_then_stop, stop)
        assert attempts == 1, "must not restart once stop is set"


class TestRestartReporting:
    """A restart must land in the *data*, not only in a log file.

    This is what lets the spec 4.5 gap detector identify process death using machinery it
    already has, and it is what makes "zero unexplained gaps" decidable.
    """

    def _events(self, root: Path) -> list[dict[str, object]]:
        import pyarrow.parquet as pq

        files = sorted((root / "collectorEvents").rglob("*.parquet"))
        rows: list[dict[str, object]] = []
        for f in files:
            rows += pq.read_table(f).to_pylist()
        return rows

    def test_cold_start_records_connect(self, tmp_path: Path) -> None:
        collector = Collector("BTCUSDT", tmp_path)
        collector._emit_startup_event()
        collector._events.flush()

        kinds = [r["kind"] for r in self._events(tmp_path)]
        assert kinds == ["CONNECT"]

    def test_unclean_shutdown_records_restart_with_downtime(self, tmp_path: Path) -> None:
        """A leftover state file is the evidence that the last run died."""
        collector = Collector("BTCUSDT", tmp_path)
        collector._write_state()

        state = json.loads((tmp_path / "collector_state.json").read_text())
        state["last_heartbeat_ms"] -= 45_000
        (tmp_path / "collector_state.json").write_text(json.dumps(state))

        recovered = Collector("BTCUSDT", tmp_path)
        recovered._emit_startup_event()
        recovered._events.flush()

        events = self._events(tmp_path)
        assert [e["kind"] for e in events] == ["RESTART"]
        # Downtime is measured from the last heartbeat, so it is close to the 45 s we
        # backdated -- exact equality would be a clock-precision assertion, not a
        # behaviour one.
        assert 44_000 <= int(events[0]["downtime_ms"]) <= 60_000

    def test_clean_shutdown_clears_state(self, tmp_path: Path) -> None:
        collector = Collector("BTCUSDT", tmp_path)
        collector._write_state()
        assert (tmp_path / "collector_state.json").exists()

        collector._clear_state()
        assert not (tmp_path / "collector_state.json").exists()

        # A subsequent start must therefore look like a cold start, not a crash.
        fresh = Collector("BTCUSDT", tmp_path)
        fresh._emit_startup_event()
        fresh._events.flush()
        assert [e["kind"] for e in self._events(tmp_path)] == ["CONNECT"]


class TestMalformedFrames:
    def test_bad_payload_is_recorded_not_fatal(self, tmp_path: Path) -> None:
        """One unparseable frame must not take down the other four streams.

        Losing every stream to salvage one bad message would be a far larger gap than the
        message itself.
        """
        collector = Collector("BTCUSDT", tmp_path)
        # A subscribed stream, deliberately. `@aggTrade` was used here until it stopped
        # being subscribed (finding F4), at which point the frame routed nowhere, no parse
        # was attempted, and the test passed for the wrong reason -- it would no longer
        # have caught a handler that crashed the collector.
        collector._on_message("btcusdt@bookTicker", {"garbage": True}, 1_704_067_200_000)
        collector._events.flush()

        import pyarrow.parquet as pq

        files = sorted((tmp_path / "collectorEvents").rglob("*.parquet"))
        rows = [r for f in files for r in pq.read_table(f).to_pylist()]
        assert any("unparseable payload" in str(r["detail"]) for r in rows)


class TestWriterUnaffectedByRestart:
    def test_flush_on_shutdown_loses_nothing(self, tmp_path: Path) -> None:
        from perplab.data.schemas import AGG_TRADES

        with ParquetBufferedWriter(tmp_path, "aggTrades", AGG_TRADES, symbol="BTCUSDT") as w:
            w.append(
                {
                    "ts_ms": 1_704_067_200_000,
                    "recv_ms": 1_704_067_200_005,
                    "agg_id": 1,
                    "price": 1,
                    "qty": 1,
                    "first_trade_id": 1,
                    "last_trade_id": 1,
                    "is_buyer_maker": False,
                }
            )
        assert w.rows_written == 1
