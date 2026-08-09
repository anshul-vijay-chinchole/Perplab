"""Macro sources: parsing, dedup, and the no-look-ahead contract (Phase 11)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from perplab.data.macro import (
    CoinGeckoGlobalPoller,
    DollarIndexPoller,
    MacroService,
    parse_global_payload,
    parse_yahoo_payload,
)
from perplab.data.query import query
from perplab.data.schemas import MACRO_USD_UNIT

SCALE = 10**8

# Trimmed captures of the real payloads, taken 2026-08-03. Real shapes rather than
# invented ones, for the reason tests/support.py gives about BTCUSDT_PAYLOAD: a
# hand-written fixture reproduces assumptions instead of reality.
GLOBAL_BODY = json.dumps(
    {
        "data": {
            "active_cryptocurrencies": 18109,
            "markets": 1509,
            "total_market_cap": {"usd": 2266388560088.1636, "btc": 35556087.585934944},
            "total_volume": {"usd": 54593834806.93682},
            "market_cap_percentage": {"btc": 56.432309330884635, "eth": 9.93},
            "updated_at": 1785777020,
        }
    }
)

YAHOO_BODY = json.dumps(
    {
        "chart": {
            "result": [
                {
                    "meta": {
                        "currency": "USD",
                        "symbol": "DX-Y.NYB",
                        "instrumentType": "INDEX",
                        "regularMarketPrice": 99.975,
                        "regularMarketTime": 1785776688,
                    },
                    # The historical array carries a literal null mid-series -- observed,
                    # not invented. Nothing may fill it.
                    "indicators": {"quote": [{"close": [100.8, 100.01, None, 99.975]}]},
                }
            ],
            "error": None,
        }
    }
)


# ------------------------------------------------------------------------- parsing


def test_global_payload_becomes_one_row_keyed_on_the_sources_own_clock() -> None:
    rows = parse_global_payload(GLOBAL_BODY, recv_ms=999)
    assert len(rows) == 1
    row = rows[0]
    # `updated_at`, not poll time: a re-poll of an unchanged snapshot must not look like
    # a fresh observation.
    assert row["ts_ms"] == 1785777020 * 1000
    assert row["recv_ms"] == 999


def test_dominance_is_stored_as_a_fraction_not_a_percentage() -> None:
    """The platform's convention is `0.15` for fifteen percent (spec 7's limits)."""
    row = parse_global_payload(GLOBAL_BODY, recv_ms=0)[0]
    assert row["btc_dominance"] / SCALE == pytest.approx(0.56432309, abs=1e-8)
    assert row["eth_dominance"] / SCALE == pytest.approx(0.0993, abs=1e-8)


def test_usd_aggregates_are_whole_dollars_and_survive_int64() -> None:
    """2.27e12 USD at the 10^8 price scale is 25x past int64 -- the reason for the unit."""
    row = parse_global_payload(GLOBAL_BODY, recv_ms=0)[0]
    assert MACRO_USD_UNIT == 1
    assert row["total_market_cap_usd"] == 2_266_388_560_088
    assert row["total_volume_usd"] == 54_593_834_806
    assert row["total_market_cap_usd"] < 2**63


def test_yahoo_payload_reads_the_meta_quote_with_its_own_timestamp() -> None:
    row = parse_yahoo_payload(YAHOO_BODY, recv_ms=7, series="DXY", source="yahoo")[0]
    assert row["ts_ms"] == 1785776688 * 1000
    assert row["value"] / SCALE == pytest.approx(99.975)
    assert row["series"] == "DXY"
    assert row["source"] == "yahoo"


def test_malformed_payloads_are_refused_with_a_reason() -> None:
    with pytest.raises(ValueError, match="no `data` object"):
        parse_global_payload(json.dumps({"nope": 1}), recv_ms=0)
    with pytest.raises(ValueError, match="updated_at"):
        parse_global_payload(json.dumps({"data": {"market_cap_percentage": {}}}), recv_ms=0)
    with pytest.raises(ValueError, match="Yahoo chart error"):
        parse_yahoo_payload(
            json.dumps({"chart": {"error": "boom", "result": None}}),
            recv_ms=0, series="DXY", source="yahoo",
        )
    with pytest.raises(ValueError, match="regularMarketTime"):
        parse_yahoo_payload(
            json.dumps({"chart": {"result": [{"meta": {"regularMarketPrice": 1}}]}}),
            recv_ms=0, series="DXY", source="yahoo",
        )


def test_a_millisecond_timestamp_is_refused_not_misfiled(tmp_path: Path) -> None:
    """Finding M30: `updated_at * 1000` assumes the provider publishes seconds.

    A provider switch to milliseconds would have filed every row under year 57046 --
    partitions no dated query ever visits, so collection looks healthy while writing rows
    that are invisible forever. The parser must refuse the implausible stamp loudly.
    """
    moved = json.loads(GLOBAL_BODY)
    moved["data"]["updated_at"] = 1785777020 * 1000  # the same instant, in milliseconds
    with pytest.raises(ValueError, match="not plausible epoch seconds"):
        parse_global_payload(json.dumps(moved), recv_ms=0)

    yahoo = json.loads(YAHOO_BODY)
    yahoo["chart"]["result"][0]["meta"]["regularMarketTime"] = 1785776688 * 1000
    with pytest.raises(ValueError, match="not plausible epoch seconds"):
        parse_yahoo_payload(json.dumps(yahoo), recv_ms=0, series="DXY", source="yahoo")


def test_a_pre_2000_timestamp_is_refused_too() -> None:
    """The other side of the window: zero or an epoch-in-microseconds-overflow artefact."""
    moved = json.loads(GLOBAL_BODY)
    moved["data"]["updated_at"] = 0
    with pytest.raises(ValueError, match="not plausible epoch seconds"):
        parse_global_payload(json.dumps(moved), recv_ms=0)


def test_a_payload_parsed_without_the_string_hook_would_be_refused() -> None:
    """The exactness guard: a float reaching `_number` means the body was parsed the
    lossy way, and that must fail loudly rather than cost precision on every row.

    `decimal` is deliberately absent from this module -- `test_money.py` confines it to
    the accounting seam -- so the transmitted digits are carried as strings straight into
    the platform's own `to_scaled`.
    """
    from perplab.data.macro import _number

    with pytest.raises(ValueError, match="parse_float=str"):
        _number({"a": {"b": 1.5}}, "a", "b")


# --------------------------------------------------------------------------- dedup


class _StubResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _StubClient:
    """Serves canned bodies in order, then repeats the last one forever."""

    def __init__(self, bodies: list[str]) -> None:
        self._bodies = bodies
        self.calls = 0

    async def get(self, url: str) -> _StubResponse:
        body = self._bodies[min(self.calls, len(self._bodies) - 1)]
        self.calls += 1
        return _StubResponse(body)

    async def aclose(self) -> None:
        return None


def _poll(poller) -> list[dict]:
    return asyncio.run(poller.fetch())


def test_an_unchanged_snapshot_is_dropped_rather_than_restored() -> None:
    """Polling hourly across a source that has not moved must not manufacture rows."""
    rows: list = []
    poller = CoinGeckoGlobalPoller(
        on_rows=lambda dataset, r: rows.extend(r),
        on_event=lambda *a: None,
        client=_StubClient([GLOBAL_BODY]),
    )
    first = _poll(poller)
    second = _poll(poller)
    third = _poll(poller)
    assert len(first) == 1
    assert second == [] and third == []


def test_a_moved_snapshot_is_recorded() -> None:
    moved = json.loads(GLOBAL_BODY)
    moved["data"]["updated_at"] = 1785777020 + 3600
    moved["data"]["market_cap_percentage"]["btc"] = 57.0
    poller = CoinGeckoGlobalPoller(
        on_rows=lambda *a: None,
        on_event=lambda *a: None,
        client=_StubClient([GLOBAL_BODY, json.dumps(moved)]),
    )
    assert len(_poll(poller)) == 1
    second = _poll(poller)
    assert len(second) == 1
    assert second[0]["btc_dominance"] / SCALE == pytest.approx(0.57)


def test_a_stale_weekend_quote_does_not_repeat_into_the_lake() -> None:
    """DXY does not print at the weekend; hourly polling must not fabricate a series."""
    poller = DollarIndexPoller(
        on_rows=lambda *a: None, on_event=lambda *a: None, client=_StubClient([YAHOO_BODY])
    )
    assert len(_poll(poller)) == 1
    assert _poll(poller) == []
    assert _poll(poller) == []


def test_a_failing_source_is_recorded_as_an_event_and_the_poller_survives() -> None:
    """Inherited from `RestPoller`: a source that dies must not take the loop with it."""
    events: list = []

    class _Broken(_StubClient):
        async def get(self, url: str):
            raise RuntimeError("coingecko is down")

    poller = CoinGeckoGlobalPoller(
        on_rows=lambda *a: None,
        on_event=lambda kind, stream, detail, downtime: events.append((stream, detail)),
        client=_Broken([]),
    )
    ok = asyncio.run(poller._guarded(poller._poll_and_emit, "poll"))
    assert ok is False
    assert events and events[0][0] == "macroGlobal"
    assert "coingecko is down" in events[0][1]


# ------------------------------------------------------------------- service + lake


def test_the_service_writes_both_datasets_into_the_lake(tmp_path: Path) -> None:
    service = MacroService(tmp_path)
    for poller, body in zip(service._pollers, [GLOBAL_BODY, YAHOO_BODY]):
        poller._client = _StubClient([body])
    counts = asyncio.run(service.poll_once())
    assert counts == {"macroGlobal": 1, "macroFx": 1}

    table = query(tmp_path, 'SELECT * FROM "macroGlobal"', datasets=("macroGlobal",))
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["btc_dominance"] == 56432309
    # Month-partitioned, symbolless: no `symbol=` component in the path.
    assert row["year"] == "2026" and row["month"] == "08"
    assert not any(p.name.startswith("symbol=") for p in (tmp_path / "macroGlobal").iterdir())

    fx = query(tmp_path, 'SELECT * FROM "macroFx"', datasets=("macroFx",))
    assert fx.to_pylist()[0]["series"] == "DXY"


def test_a_restarted_service_does_not_rewrite_the_flushed_snapshot(tmp_path: Path) -> None:
    """Finding H26 at the service level: the dedup cursor must survive a process restart.

    The first service polls once and flushes; a *new* service instance -- a restart --
    is served the identical provider bodies. Before the cursors were seeded from the
    lake, the second instance re-recorded both snapshots and the lake held two rows per
    `ts_ms`; now it recognises them as already stored.
    """
    first = MacroService(tmp_path)
    for poller, body in zip(first._pollers, [GLOBAL_BODY, YAHOO_BODY]):
        poller._client = _StubClient([body])
    assert asyncio.run(first.poll_once()) == {"macroGlobal": 1, "macroFx": 1}

    restarted = MacroService(tmp_path)
    for poller, body in zip(restarted._pollers, [GLOBAL_BODY, YAHOO_BODY]):
        poller._client = _StubClient([body])
    assert asyncio.run(restarted.poll_once()) == {"macroGlobal": 0, "macroFx": 0}

    table = query(tmp_path, 'SELECT count(*) AS n FROM "macroGlobal"', datasets=("macroGlobal",))
    assert table.to_pylist()[0]["n"] == 1


def test_the_unscaled_view_divides_dominance_and_leaves_whole_dollars_alone(
    tmp_path: Path,
) -> None:
    """The column-classification guard's whole point: `SCALED_COLUMNS` means divide by
    10^8, so a USD aggregate listed there would understate a trillion by 10^8."""
    service = MacroService(tmp_path)
    for poller, body in zip(service._pollers, [GLOBAL_BODY, YAHOO_BODY]):
        poller._client = _StubClient([body])
    asyncio.run(service.poll_once())

    table = query(
        tmp_path,
        'SELECT "btc_dominance", "total_market_cap_usd" FROM "macroGlobal_unscaled"',
        datasets=("macroGlobal",),
    )
    row = table.to_pylist()[0]
    assert row["btc_dominance"] == pytest.approx(0.56432309, abs=1e-9)
    assert row["total_market_cap_usd"] == 2_266_388_560_088
