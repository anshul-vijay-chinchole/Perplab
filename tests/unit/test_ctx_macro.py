"""`ctx.macro()`: causality, staleness, and honest absence (Phase 11).

The centrepiece is `test_a_reading_is_invisible_until_the_bar_after_it_published`. Macro
data arrives on its own clock, unrelated to the bar grid, which makes it the easiest place
in the platform to reintroduce look-ahead: a series joined on "nearest timestamp" rather
than "last at or before" reads the future by up to an hour and produces a backtest that
cannot be traded.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from perplab.data.schemas import MACRO_FX, MACRO_GLOBAL
from perplab.data.writer import ParquetBufferedWriter
from perplab.engine import ENGINE_VERSION
from perplab.engine.runspec import RunSpec
from perplab.engine.worker import execute_run
from perplab.store import db
from perplab.store.runs import RunStore
from perplab.strategy.context import DataUnavailable
from tests.engine_lake import MS_PER_MINUTE, build_lake, ramp_path
from tests.integration.test_run_worker import seed_strategy
from tests.support import BTCUSDT_PAYLOAD

SCALE = 10**8
START = 1_709_251_200_000  # 2024-03-01T00:00Z
MINUTES = 6 * 60

# A strategy that records what it saw, so the test can assert on the engine's own log
# rather than on a reimplementation of the lookup.
MACRO_SOURCE = '''
from perplab import Strategy

class MacroWatcher(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
        "datasets": ["klines", "macroGlobal", "macroFx"],
    }
    params = {}

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return
        dominance = ctx.macro("btc_dominance")
        dxy = ctx.macro("dxy")
        ctx.record("dominance", -1.0 if dominance is None else dominance.value)
        ctx.record("dominance_age_min", -1.0 if dominance is None else dominance.age_ms / 60000)
        ctx.record("dxy", -1.0 if dxy is None else dxy.value)
'''

NO_DECLARATION_SOURCE = '''
from perplab import Strategy

class Undeclared(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 0,
                "datasets": ["klines"]}
    params = {}

    def on_bar(self, ctx, bar):
        if ctx.warm:
            ctx.macro("dxy")
'''


def _userdata(root: Path, *, macro_rows=(), fx_rows=()) -> None:
    build_lake(
        root / "market", start_ms=START, minutes=MINUTES, trade_path=ramp_path(40_000.0, 1.0)
    )
    if macro_rows:
        with ParquetBufferedWriter(
            root / "market", "macroGlobal", MACRO_GLOBAL, symbol=None
        ) as writer:
            for row in macro_rows:
                writer.append(row)
    if fx_rows:
        with ParquetBufferedWriter(
            root / "market", "macroFx", MACRO_FX, symbol=None
        ) as writer:
            for row in fx_rows:
                writer.append(row)

    exchange = root / "reference" / "exchangeInfo"
    exchange.mkdir(parents=True, exist_ok=True)
    (exchange / "2024-01-01.json").write_text(
        json.dumps({"symbols": [BTCUSDT_PAYLOAD]}), encoding="utf-8"
    )
    brackets = root / "reference" / "leverageBracket"
    brackets.mkdir(parents=True, exist_ok=True)
    (brackets / "2024-01-01.json").write_text(
        json.dumps(
            [
                {
                    "symbol": "BTCUSDT",
                    "brackets": [
                        {"bracket": 1, "initialLeverage": 125, "notionalCap": 1_000_000_000,
                         "notionalFloor": 0, "maintMarginRatio": 0.004, "cum": 0.0}
                    ],
                }
            ]
        ),
        encoding="utf-8",
    )
    db.connect(root).close()


def _global_row(ts_ms: int, dominance: float, recv_ms: int | None = -1) -> dict:
    return {
        "ts_ms": ts_ms,
        "recv_ms": ts_ms if recv_ms == -1 else recv_ms,
        "btc_dominance": int(dominance * SCALE),
        "eth_dominance": 0,
        "total_market_cap_usd": 2_266_388_560_088,
        "total_volume_usd": 54_593_834_806,
        "active_cryptocurrencies": 18109,
        "markets": 1509,
    }


def _fx_row(ts_ms: int, value: float, recv_ms: int | None = -1) -> dict:
    return {
        "ts_ms": ts_ms,
        "recv_ms": ts_ms if recv_ms == -1 else recv_ms,
        "series": "DXY",
        "value": int(value * SCALE),
        "source": "yahoo",
    }


def _run(root: Path, source: str, name: str) -> tuple[RunStore, int]:
    strategy_id, version_id = seed_strategy(root, source, name=name)
    spec = RunSpec(
        strategy_id=strategy_id, version_id=version_id, version_no=1, strategy_name=name,
        code=source, class_name=None, params={}, symbols=("BTCUSDT",), timeframe="1m",
        start_ms=START, end_ms=START + MINUTES * MS_PER_MINUTE, seed=1,
        opening_balance="10000", leverage=5, maker_rate="0.0002", taker_rate="0.0005",
        fee_source="test", latency={"model": "fixed", "submit_ms": 10, "cancel_ms": 10},
        fill_tier="BAR_CLOSE", fill_model={"tier": "BAR_CLOSE", "slippage_bps": "1.0"},
        liquidation_recovery_pct="0", timeout_s=120.0, engine_version=ENGINE_VERSION,
    )
    runs = RunStore(root)
    run_id = runs.create(strategy_id=strategy_id, version_id=version_id, spec=spec.to_storage())
    execute_run(root, run_id, runs)
    return runs, run_id


def _records(runs: RunStore, run_id: int, name: str) -> list[tuple[int, float]]:
    out = []
    for event in runs.iter_events(run_id):
        if event.get("kind") == "RECORD" and event["payload"].get("name") == name:
            out.append((event["ts_ms"], event["payload"]["value"]))
    return out


# ------------------------------------------------------------------- no look-ahead


def test_a_reading_is_invisible_until_the_bar_after_it_published(tmp_path: Path) -> None:
    """**The no-look-ahead contract for macro.**

    One dominance reading published at 02:00:30 -- half a minute *inside* the 02:00 bar.
    The 02:00 bar closes at 02:00:59.999, so that bar may already see it; the 01:59 bar
    must not, and neither may anything earlier. A join on "nearest" rather than "last at
    or before" would leak it backwards by up to an hour.
    """
    published = START + 120 * MS_PER_MINUTE + 30_000
    _userdata(tmp_path, macro_rows=[_global_row(published, 0.55)])
    runs, run_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        seen = _records(runs, run_id, "dominance")
        assert seen, "the strategy recorded nothing"
        before = [value for ts, value in seen if ts < published]
        after = [value for ts, value in seen if ts >= published]
        assert before and set(before) == {-1.0}, (
            "a macro reading was visible before it published -- look-ahead"
        )
        assert after and all(value == pytest.approx(0.55) for value in after)
    finally:
        runs.close()


def test_the_reported_age_grows_with_the_bar_clock(tmp_path: Path) -> None:
    """LOCF without an age silently presents Friday's dollar as Sunday's."""
    published = START + 60 * MS_PER_MINUTE
    _userdata(tmp_path, macro_rows=[_global_row(published, 0.55)])
    runs, run_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        ages = [v for ts, v in _records(runs, run_id, "dominance_age_min") if v >= 0]
        assert ages == sorted(ages), "age must increase while no new reading publishes"
        assert ages[0] < 2.0
        assert ages[-1] > 100.0
    finally:
        runs.close()


def test_the_latest_reading_at_or_before_now_wins(tmp_path: Path) -> None:
    """Three readings an hour apart: each bar must see the most recent *past* one."""
    rows = [
        _global_row(START + 60 * MS_PER_MINUTE, 0.50),
        _global_row(START + 180 * MS_PER_MINUTE, 0.60),
        _global_row(START + 300 * MS_PER_MINUTE, 0.70),
    ]
    _userdata(tmp_path, macro_rows=rows)
    runs, run_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        seen = _records(runs, run_id, "dominance")
        at = lambda minute: next(  # noqa: E731 - a local lookup, not an API
            v for ts, v in seen if ts >= START + minute * MS_PER_MINUTE
        )
        assert at(30) == -1.0        # before the first reading
        assert at(120) == pytest.approx(0.50)
        assert at(240) == pytest.approx(0.60)
        assert at(330) == pytest.approx(0.70)
    finally:
        runs.close()


# ------------------------------------------------- the two clocks on a macro row


def test_a_reading_is_invisible_until_the_platform_received_it(tmp_path: Path) -> None:
    """**Publication time is not availability time, and the gap is a look-ahead.**

    Every other test in this file writes `recv_ms == ts_ms`, which makes the two clocks
    indistinguishable and lets a wrong rule pass. Real rows never look like that: measured
    against the live sources on 2026-08-03, CoinGecko's `updated_at` ran ~3.4 min behind
    receipt and Yahoo's `regularMarketTime` ~10.0 min, bounded above by the hourly poll.

    Here the provider stamps 01:00 and the poller does not have the row until 03:00. Bars
    between those two instants must see nothing: the value existed in the world, but the
    platform could not have known it, and a live session -- which can only read rows
    already written to the lake -- would not have known it either. Gating on `ts_ms` makes
    the backtest optimistic by the whole two hours, always in the favourable direction.
    """
    published = START + 60 * MS_PER_MINUTE
    received = START + 180 * MS_PER_MINUTE
    _userdata(tmp_path, macro_rows=[_global_row(published, 0.55, recv_ms=received)])
    runs, run_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        seen = _records(runs, run_id, "dominance")
        assert seen, "the strategy recorded nothing"
        between = [value for ts, value in seen if published <= ts < received]
        assert between and set(between) == {-1.0}, (
            "a macro reading was visible before the platform received it -- the backtest "
            "is reading data no live session could have had"
        )
        after = [value for ts, value in seen if ts >= received]
        assert after and all(value == pytest.approx(0.55) for value in after)
    finally:
        runs.close()


def test_the_age_is_measured_from_publication_not_from_receipt(tmp_path: Path) -> None:
    """The two clocks answer two different questions, and both answers must be right.

    *When may I see this?* is receipt. *How stale is it?* is publication. A reading that
    published two hours ago and reached us this minute is two hours old, and a strategy
    thresholding on `age_ms` to avoid trading a stale dollar has to be told so.
    """
    published = START + 60 * MS_PER_MINUTE
    received = START + 180 * MS_PER_MINUTE
    _userdata(tmp_path, macro_rows=[_global_row(published, 0.55, recv_ms=received)])
    runs, run_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        ages = [
            (ts, value)
            for ts, value in _records(runs, run_id, "dominance_age_min")
            if value >= 0
        ]
        assert ages, "the reading never became visible"
        first_ts, first_age = ages[0]
        # First sight is at receipt, but the value is already two hours old by then.
        assert first_ts >= received
        assert first_age == pytest.approx(120.0, abs=1.5), (
            "age was measured from receipt, which reports a two-hour-old dollar as fresh"
        )
    finally:
        runs.close()


def test_a_row_with_no_receive_time_falls_back_to_its_own_stamp(tmp_path: Path) -> None:
    """Backfilled rows have no local clock, so `ts_ms` is the only estimate available.

    Nothing backfills macro today; the bulk archives set `recv_ms` null on every other
    dataset for exactly this reason, so the null has to mean something rather than drop
    the row out of the query.
    """
    published = START + 60 * MS_PER_MINUTE
    _userdata(tmp_path, macro_rows=[_global_row(published, 0.55, recv_ms=None)])
    runs, run_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        seen = _records(runs, run_id, "dominance")
        after = [value for ts, value in seen if ts >= published]
        assert after and all(value == pytest.approx(0.55) for value in after), (
            "a row with a null recv_ms vanished instead of falling back to ts_ms"
        )
    finally:
        runs.close()


# ------------------------------------------------------------------ honest absence


def test_a_lake_with_no_macro_rows_flags_the_run_rather_than_failing_it(
    tmp_path: Path,
) -> None:
    """Optional by design -- but the absence goes on the record, not into silence."""
    _userdata(tmp_path)
    runs, run_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        summary = runs.get(run_id)
        assert summary.status == "done", summary.error
        assert "MACRO_MISSING" in summary.flags
        assert any("no macro rows" in w for w in summary.warnings)
        assert set(v for _, v in _records(runs, run_id, "dominance")) == {-1.0}
    finally:
        runs.close()


def test_calling_macro_without_declaring_the_dataset_raises(tmp_path: Path) -> None:
    """`None` would conflate "you did not ask" with "nothing published".

    The message has to name the fix, because the author's next question is "how do I turn
    this on" and the answer is one line in `requires`.
    """
    _userdata(tmp_path, macro_rows=[_global_row(START + 60 * MS_PER_MINUTE, 0.55)])
    with pytest.raises(DataUnavailable, match=r"requires\['datasets'\]"):
        _run(tmp_path, NO_DECLARATION_SOURCE, "Undeclared")


def test_the_fx_series_is_addressed_by_its_lowercased_name(tmp_path: Path) -> None:
    _userdata(
        tmp_path,
        macro_rows=[_global_row(START + 30 * MS_PER_MINUTE, 0.55)],
        fx_rows=[_fx_row(START + 30 * MS_PER_MINUTE, 99.975)],
    )
    runs, run_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        values = [v for _, v in _records(runs, run_id, "dxy") if v >= 0]
        assert values and values[-1] == pytest.approx(99.975)
    finally:
        runs.close()


def test_a_macro_run_is_deterministic(tmp_path: Path) -> None:
    """Spec 12.1 applies to macro like every other input."""
    rows = [_global_row(START + 60 * MS_PER_MINUTE, 0.55)]
    _userdata(tmp_path, macro_rows=rows, fx_rows=[_fx_row(START + 90 * MS_PER_MINUTE, 99.9)])
    runs, first_id = _run(tmp_path, MACRO_SOURCE, "MacroWatcher")
    try:
        strategy_id, version_id = seed_strategy(tmp_path, MACRO_SOURCE, name="MacroWatcher2")
        spec = RunSpec.from_storage(runs.read_json(first_id, "spec.json"))
        from dataclasses import replace

        second_spec = replace(spec, strategy_id=strategy_id, version_id=version_id)
        second_id = runs.create(
            strategy_id=strategy_id, version_id=version_id, spec=second_spec.to_storage()
        )
        execute_run(tmp_path, second_id, runs)
        assert runs.get(second_id).event_hash == runs.get(first_id).event_hash
    finally:
        runs.close()
