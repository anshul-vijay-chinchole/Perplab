"""Tests for dead-stream detection.

Motivated by a real miss found during the first live run: `aggTrades` and `markPrice`
delivered zero messages for the entire run while the socket stayed healthy and heartbeats
kept being written. The per-stream counts were in the heartbeat records the whole time,
but nothing evaluated them, so noticing required a human to read the numbers and know
what they should have been.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from perplab.core.types import CollectorEventKind
from perplab.data.collector import MAX_SILENCE_S, Collector
from perplab.data.rest_poller import REST_DATASETS


def _events(root: Path) -> list[dict[str, object]]:
    files = sorted((root / "collectorEvents").rglob("*.parquet"))
    return [r for f in files for r in pq.read_table(f).to_pylist()]


def _connect(collector: Collector, at: float) -> None:
    """Bring every source online at `at`.

    Two baselines, not one. Socket-fed datasets are checkable only while the socket is up;
    REST-fed ones are checkable from the moment their poller started and are unaffected by
    what the socket is doing. A helper that set only `_connected_at_s` would leave the
    polled datasets with no baseline, and every staleness assertion about them would pass
    by never being evaluated.
    """
    collector._connected_at_s = at
    for dataset in REST_DATASETS:
        collector._poller_started_s[dataset] = at


class TestStaleness:
    def test_reports_stream_that_never_delivers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The case that actually happened: subscribed, connected, zero messages ever.

        Baselining from connect time rather than from first message is what makes this
        detectable -- a plain "time since last message" check has no baseline to compare
        against and stays silent forever.
        """
        collector = Collector("BTCUSDT", tmp_path)
        _connect(collector, 1000.0)

        monkeypatch.setattr(
            "perplab.data.collector.time.monotonic",
            lambda: 1000.0 + MAX_SILENCE_S["aggTrades"] + 1,
        )
        collector._check_staleness()
        collector._events.flush()

        stale = [e for e in _events(tmp_path) if e["kind"] == "STALE"]
        assert {e["stream"] for e in stale} == {"aggTrades", "bookTicker", "depth20", "markPrice"}
        assert any("nothing received since connect" in str(e["detail"]) for e in stale)

    def test_reported_once_per_outage(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stream dead for an hour must not produce 360 identical records."""
        collector = Collector("BTCUSDT", tmp_path)
        _connect(collector, 1000.0)

        now = 1000.0 + MAX_SILENCE_S["depth20"] + 1
        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: now)
        for _ in range(5):
            collector._check_staleness()
        collector._events.flush()

        depth = [
            e
            for e in _events(tmp_path)
            if e["kind"] == "STALE" and e["stream"] == "depth20"
        ]
        assert len(depth) == 1

    def test_recovery_is_recorded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An outage needs a measurable end, not just a beginning."""
        collector = Collector("BTCUSDT", tmp_path)
        _connect(collector, 1000.0)

        now = 1000.0 + MAX_SILENCE_S["depth20"] + 1
        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: now)
        collector._check_staleness()
        assert "depth20" in collector._stale_reported

        collector._mark_seen("depth20")
        collector._check_staleness()
        assert "depth20" not in collector._stale_reported

        collector._events.flush()
        assert any(
            e["stream"] == "depth20" and "recovered" in str(e["detail"])
            for e in _events(tmp_path)
        )

    def test_healthy_streams_are_silent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        collector = Collector("BTCUSDT", tmp_path)
        _connect(collector, 1000.0)

        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: 1005.0)
        for dataset in MAX_SILENCE_S:
            collector._mark_seen(dataset)
        collector._check_staleness()
        collector._events.flush()

        assert not [e for e in _events(tmp_path) if e["kind"] == "STALE"]

    def test_liquidations_are_never_flagged(self) -> None:
        """Forced orders are legitimately sparse -- hours can pass on a calm day.

        Alarming on them would fire constantly and train the operator to ignore every
        STALE record, including the ones that matter.
        """
        assert "liquidations" not in MAX_SILENCE_S

    def test_socket_datasets_suspended_while_disconnected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The DISCONNECT record already explains the silence; STALE would be noise."""
        collector = Collector("BTCUSDT", tmp_path)
        _connect(collector, 1000.0)
        collector._on_stream_event(CollectorEventKind.DISCONNECT, "all", "dropped", 0)

        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: 99_999.0)
        collector._check_staleness()
        collector._events.flush()

        stale = {e["stream"] for e in _events(tmp_path) if e["kind"] == "STALE"}
        assert stale.isdisjoint({"depth20", "bookTicker"})

    def test_polled_datasets_survive_a_socket_disconnect(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dead poller must still be reported while the socket happens to be down.

        These datasets do not travel over the WebSocket at all (finding F4), so a
        DISCONNECT explains nothing about them. Sharing the socket's baseline would let any
        unrelated network blip silence the alarm on a genuinely dead poller for as long as
        the socket stayed down -- which is the exact failure mode staleness detection was
        added to catch, reintroduced through the back door.
        """
        collector = Collector("BTCUSDT", tmp_path)
        _connect(collector, 1000.0)
        collector._on_stream_event(CollectorEventKind.DISCONNECT, "all", "dropped", 0)

        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: 99_999.0)
        collector._check_staleness()
        collector._events.flush()

        stale = {e["stream"] for e in _events(tmp_path) if e["kind"] == "STALE"}
        assert stale == set(REST_DATASETS)

    def test_reconnect_rebaselines(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Silence spanning a reconnect is already explained by the reconnect pair."""
        collector = Collector("BTCUSDT", tmp_path)
        _connect(collector, 1000.0)

        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: 5000.0)
        collector._on_stream_event(CollectorEventKind.RECONNECT, "all", "back", 4000)
        collector._check_staleness()
        collector._events.flush()

        stale = {e["stream"] for e in _events(tmp_path) if e["kind"] == "STALE"}
        assert stale.isdisjoint({"depth20", "bookTicker"})
        assert collector._connected_at_s == 5000.0

    def test_socket_reconnect_does_not_rebaseline_the_pollers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A socket reconnect must not forgive a poller that has been dead for an hour.

        The two fail independently. Clearing every dataset's liveness on a socket event
        would hand the pollers a fresh baseline they did not earn, so a dead poller would
        be re-forgiven by each of the daily reconnects Binance forces -- and never
        reported at all.
        """
        collector = Collector("BTCUSDT", tmp_path)
        _connect(collector, 1000.0)

        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: 5000.0)
        collector._on_stream_event(CollectorEventKind.RECONNECT, "all", "back", 4000)

        assert collector._poller_started_s["markPrice"] == 1000.0
        collector._check_staleness()
        collector._events.flush()

        stale = {e["stream"] for e in _events(tmp_path) if e["kind"] == "STALE"}
        assert stale == set(REST_DATASETS)

    def test_a_socket_reconnect_does_not_falsely_age_a_healthy_poller(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A poller delivering normally must not be reported stale because the socket blipped.

        This is the other direction of the same separation, and it is the one a weaker test
        misses: clearing every dataset's last-message time on a socket event drops the
        poller's recent liveness and falls back to its much older start time, so a
        perfectly healthy mark price feed is reported dead every time Binance cycles the
        connection. Populating `_last_msg_s` first is what makes the difference observable
        -- with it empty, clearing it is a no-op and the bug hides.
        """
        collector = Collector("BTCUSDT", tmp_path)
        collector._connected_at_s = 0.0
        for dataset in REST_DATASETS:
            collector._poller_started_s[dataset] = 0.0

        # The pollers have been delivering happily right up to t=1000.
        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: 1000.0)
        for dataset in REST_DATASETS:
            collector._mark_seen(dataset)

        collector._on_stream_event(CollectorEventKind.RECONNECT, "all", "back", 500)

        # Well inside every threshold measured from the last message, and far outside them
        # measured from poller start.
        monkeypatch.setattr("perplab.data.collector.time.monotonic", lambda: 1050.0)
        collector._check_staleness()
        collector._events.flush()

        stale = {e["stream"] for e in _events(tmp_path) if e["kind"] == "STALE"}
        assert stale.isdisjoint(REST_DATASETS)
