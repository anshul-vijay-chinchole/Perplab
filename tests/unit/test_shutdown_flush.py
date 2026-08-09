"""Shutdown must not discard buffered data.

Regression test for a bug found during the first live run: the shutdown path called
`maybe_flush()`, which is conditional on the row-count and time thresholds. Anything
buffered below those thresholds was dropped on every clean stop -- including the
SHUTDOWN record itself, which made an orderly stop indistinguishable from a crash in the
event log.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from perplab.core.types import CollectorEventKind
from perplab.data.collector import Collector


def _events(root: Path) -> list[dict[str, object]]:
    files = sorted((root / "collectorEvents").rglob("*.parquet"))
    return [r for f in files for r in pq.read_table(f).to_pylist()]


def test_force_flush_writes_below_threshold(tmp_path: Path) -> None:
    """A single buffered row is far below any threshold and must still be written."""
    collector = Collector("BTCUSDT", tmp_path)
    collector._write_event(CollectorEventKind.SHUTDOWN, "collector", "clean shutdown", 0)

    collector._flush_all()  # conditional -- should write nothing
    assert not list((tmp_path / "collectorEvents").rglob("*.parquet"))

    collector._flush_all(force=True)
    assert [e["kind"] for e in _events(tmp_path)] == ["SHUTDOWN"]


@pytest.mark.asyncio
async def test_shutdown_persists_shutdown_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end: a clean stop leaves a SHUTDOWN record and no state file.

    The two together are what let the next start distinguish an orderly stop from a
    crash. Losing either one makes every restart look like a crash, which would bury the
    real crashes in noise.
    """

    async def fake_run(self: object, stop: asyncio.Event) -> None:
        return  # connect immediately, deliver nothing, return

    monkeypatch.setattr("perplab.exchange.ws.StreamManager.run", fake_run)

    collector = Collector("BTCUSDT", tmp_path)
    stop = asyncio.Event()
    stop.set()
    await collector.run(stop)

    kinds = [e["kind"] for e in _events(tmp_path)]
    assert "SHUTDOWN" in kinds
    assert not (tmp_path / "collector_state.json").exists()


@pytest.mark.asyncio
async def test_shutdown_flushes_the_pending_depth_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Finding L5: the depth downsampler holds the current second's newest snapshot and
    only emits it when the second rolls over -- a rollover that never comes on the way
    out, so every clean stop lost the final second's depth. For the one dataset that can
    never be re-downloaded, the last observation is exactly as irreplaceable as the rest.
    """

    async def fake_run(self: object, stop: asyncio.Event) -> None:
        return

    monkeypatch.setattr("perplab.exchange.ws.StreamManager.run", fake_run)

    collector = Collector("BTCUSDT", tmp_path)
    collector._on_depth(
        {
            "T": 1_704_067_200_500,
            "E": 1_704_067_200_500,
            "u": 42,
            "b": [["100.25", "1.5"]],
            "a": [["100.50", "2.5"]],
        },
        recv_ms=1_704_067_200_505,
    )
    assert collector._depth_pending is not None, "the snapshot is held, not yet written"

    stop = asyncio.Event()
    stop.set()
    await collector.run(stop)

    files = sorted((tmp_path / "depth20").rglob("*.parquet"))
    rows = [r for f in files for r in pq.read_table(f).to_pylist()]
    assert [r["ts_ms"] for r in rows] == [1_704_067_200_500]
    assert collector._depth_pending is None


@pytest.mark.asyncio
async def test_state_survives_a_failing_final_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the final flush fails, the next run must still report a RESTART.

    Data really was lost in that case, so reporting a clean shutdown would be a lie.
    """

    async def fake_run(self: object, stop: asyncio.Event) -> None:
        return

    monkeypatch.setattr("perplab.exchange.ws.StreamManager.run", fake_run)

    collector = Collector("BTCUSDT", tmp_path)
    collector._write_state()

    def boom(**_kw: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(collector, "_flush_all", boom)

    stop = asyncio.Event()
    stop.set()
    with pytest.raises(OSError):
        await collector.run(stop)

    assert (tmp_path / "collector_state.json").exists(), (
        "state must survive a failed flush so the loss is reported as a RESTART"
    )
