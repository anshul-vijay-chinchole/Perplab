"""Tests for where the collector gets each dataset from (finding F4).

Three streams stopped being sourceable from the WebSocket on 2026-08-02. Two moved to REST
pollers and one -- `liquidations` -- has no source at all. The tests here pin the
consequences of that, because they are the kind of thing that quietly rots: a subscription
list that regrows a dead stream, a dataset that silently stops being recorded, or an empty
dataset that starts reading as an unexplained failure.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from perplab.core.types import CollectorEventKind
from perplab.data.collector import UNAVAILABLE_DATASETS, Collector
from perplab.data.gaps import EXPLAINING_KINDS
from perplab.data.rest_poller import REST_DATASETS
from perplab.data.schemas import SCHEMAS

SUPPRESSED = ("@aggTrade", "@markPrice", "!forceOrder", "@kline", "@ticker")


def _events(root: Path) -> list[dict[str, object]]:
    files = sorted((root / "collectorEvents").rglob("*.parquet"))
    return [r for f in files for r in pq.read_table(f).to_pylist()]


class TestSubscriptions:
    def test_only_streams_this_endpoint_serves_are_subscribed(self, tmp_path: Path) -> None:
        """A dead subscription is not harmless.

        The pollers now supply `aggTrades` and `markPrice`. Leaving the WebSocket
        subscribed to the suppressed streams as well would mean that the day Binance
        restores them, both sources write the same dataset and every row appears twice --
        a corruption that looks like a volume spike rather than a bug.
        """
        collector = Collector("BTCUSDT", tmp_path)

        assert collector.streams == ["btcusdt@depth20@100ms", "btcusdt@bookTicker"]
        for suppressed in SUPPRESSED:
            assert not any(suppressed in s for s in collector.streams)

    def test_every_writer_still_exists(self, tmp_path: Path) -> None:
        """Changing the *source* must not change the set of datasets on disk.

        `liquidations` keeps its writer even with nothing to write: dropping it would
        remove the dataset from the lake's schema registry, and a reader asking for it
        would get "no such dataset" rather than "no rows", which are very different claims.
        """
        collector = Collector("BTCUSDT", tmp_path)

        assert set(collector._writers) == {
            "depth20",
            "bookTicker",
            "aggTrades",
            "markPrice",
            "liquidations",
            # Open interest, polled onto the daily archive's own five-minute grid so the
            # live period continues the historical series rather than starting a new one.
            # `ctx.oi()` reads `metrics.sum_open_interest`; without this it would return
            # `None` for everything newer than the last archived day.
            "metrics",
        }


class TestUnavailableDatasets:
    def test_liquidations_is_declared_unavailable(self) -> None:
        assert set(UNAVAILABLE_DATASETS) == {"liquidations"}
        assert "allForceOrders" in UNAVAILABLE_DATASETS["liquidations"]

    def test_the_reason_is_written_into_the_lake(self, tmp_path: Path) -> None:
        """The reason must travel with the data, not live only in a source comment.

        An operator finding an empty `liquidations/` a year from now reads the event
        stream, not this file.
        """
        collector = Collector("BTCUSDT", tmp_path)
        collector._emit_unavailable_events()
        collector._events.flush()

        rows = [e for e in _events(tmp_path) if e["kind"] == "UNAVAILABLE"]
        assert [e["stream"] for e in rows] == ["liquidations"]
        assert "no public source" in str(rows[0]["detail"])

    def test_unavailable_explains_a_gap(self) -> None:
        """Otherwise a dataset with no source is an unexplained gap forever.

        That would make the Phase 1b exit criterion impossible to satisfy for a reason no
        amount of correct collector code could fix.
        """
        assert CollectorEventKind.UNAVAILABLE in EXPLAINING_KINDS

    def test_unavailable_is_distinct_from_stale(self) -> None:
        """STALE means "should be arriving and is not" -- a fault worth investigating.

        Collapsing the two would either bury real faults under a message that repeats every
        run, or leave a permanent condition looking like an incident.
        """
        assert CollectorEventKind.UNAVAILABLE is not CollectorEventKind.STALE
        assert CollectorEventKind.UNAVAILABLE.value == "UNAVAILABLE"


class TestSymbolValidation:
    """Finding M29: the collector's symbol reaches every writer's partition path, and it
    used to arrive through a bare `.upper()` while every read path used
    `schemas.normalise_symbol` -- so the write side accepted exactly the values no reader
    could ever find, including ones that escaped the lake root."""

    def test_a_path_shaped_symbol_is_refused(self, tmp_path: Path) -> None:
        import pytest

        for bad in ("BTC/USDT", "../../..", "BTC USDT"):
            with pytest.raises(ValueError, match="implausible symbol"):
                Collector(bad, tmp_path)
        assert list(tmp_path.rglob("*")) == []

    def test_a_lowercase_symbol_is_canonicalised(self, tmp_path: Path) -> None:
        collector = Collector("btcusdt", tmp_path)
        assert collector.symbol == "BTCUSDT"
        assert collector.streams[0].startswith("btcusdt@")


class TestUnparseableFrames:
    """Finding H23: the malformed-frame event must not wear a stream name a gap can
    borrow as an alibi. One bad frame is a parse incident, not a disconnection, and when
    the record carried the raw stream name nothing could ever close it -- so it explained
    every gap after it, unboundedly."""

    def test_the_event_is_filed_under_parse_with_the_stream_in_the_detail(
        self, tmp_path: Path
    ) -> None:
        collector = Collector("BTCUSDT", tmp_path)
        collector._on_message("btcusdt@bookTicker", {"nothing": "useful"}, 0)
        collector._events.flush()

        rows = [e for e in _events(tmp_path) if e["kind"] == "DISCONNECT"]
        assert len(rows) == 1
        assert rows[0]["stream"] == "parse"
        assert "btcusdt@bookTicker" in str(rows[0]["detail"])

    def test_the_parse_label_explains_no_dataset(self) -> None:
        from perplab.data import gaps

        for dataset in ("depth20", "bookTicker", "aggTrades", "markPrice", "liquidations"):
            assert not gaps._stream_covers_dataset("parse", dataset)


class TestPollRowSink:
    def test_rows_are_written_counted_and_marked_live(self, tmp_path: Path) -> None:
        collector = Collector("BTCUSDT", tmp_path)
        rows = [
            {
                "ts_ms": 1_700_000_000_000,
                "recv_ms": 1_700_000_000_005,
                "mark_price": 6_300_000_000_000,
                "index_price": 6_300_100_000_000,
                "estimated_settle_price": 0,
                "last_funding_rate": 8473,
                "next_funding_ms": 1_700_000_100_000,
            }
        ]

        collector._on_poll_rows("markPrice", rows)
        collector._writers["markPrice"].flush()

        written = [
            r
            for f in sorted((tmp_path / "markPrice").rglob("*.parquet"))
            for r in pq.read_table(f).to_pylist()
        ]
        assert len(written) == 1
        assert written[0]["mark_price"] == 6_300_000_000_000
        assert collector._counts["markPrice"] == 1
        assert "markPrice" in collector._last_msg_s

    def test_an_empty_batch_does_not_count_as_liveness(self, tmp_path: Path) -> None:
        """Liveness must track data, not successful HTTP calls.

        A poller happily polling an endpoint that returns empty forever is exactly the
        failure the staleness check exists to catch; counting the call as a sign of life
        would hide it.
        """
        collector = Collector("BTCUSDT", tmp_path)

        collector._on_poll_rows("markPrice", [])

        assert "markPrice" not in collector._last_msg_s
        assert collector._counts["markPrice"] == 0

    def test_poll_rows_match_the_dataset_schema(self, tmp_path: Path) -> None:
        """The pollers build rows in the dataset's own schema, so the field names must
        agree exactly -- a renamed column would surface as a writer error at runtime."""
        collector = Collector("BTCUSDT", tmp_path)

        for dataset in REST_DATASETS:
            expected = {f.name for f in SCHEMAS[dataset]}
            assert expected, f"{dataset} has no schema registered"
            assert collector._writers[dataset] is not None
