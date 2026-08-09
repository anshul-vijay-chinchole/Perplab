"""Budget enforcement, exact bounded replay and disk-backed artifact regression checks."""
from __future__ import annotations
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from perplab.resources import GiB, MiB, ResourcePolicy, ResourceGuard, ResourceLimitExceeded, admission_reason, worker_exit_reason


def lease(role="backtest", allowance=2*GiB, used=100*MiB, **values):
    return {"role": role, "status": "running", "allowance_bytes": allowance, "private_bytes": used, **values}


def test_policy_and_admission_protect_shared_budget_and_system_reserve():
    policy = ResourcePolicy()
    assert (policy.research_gib, policy.normal_gib, policy.hard_gib, policy.reserve_gib, policy.worker_gib, policy.research_workers) == (6, 8, 10, 4, 2, 1)
    assert admission_reason(policy, [], 5*GiB, 2*GiB) == "waiting to keep 4 GiB of system RAM available"
    assert "slot" in admission_reason(policy, [lease()], 20*GiB, 2*GiB)
    assert "research budget" in admission_reason(policy, [lease(allowance=5*GiB)], 20*GiB, 2*GiB, nested=True)
    assert "warning" in admission_reason(policy, [lease(warning=True)], 20*GiB, 2*GiB)
    assert admission_reason(policy, [], 8*GiB, 2*GiB) is None
    assert "backend" in admission_reason(policy, [], 20*GiB, 2*GiB, backend_bytes=7*GiB)
    assert "warning" in admission_reason(policy, [], 20*GiB, 2*GiB, research_bytes=5*GiB)


@pytest.mark.parametrize("values", [{"research_gib": 7}, {"hard_gib": 11}, {"worker_gib": 3}, {"reserve_gib": 3}, {"research_workers": 15}, {"spill_gib": float("nan")}])
def test_unprotected_policy_is_refused(values):
    with pytest.raises(ValueError):
        ResourcePolicy(**values)


def test_guard_fails_closed_before_work_and_reports_reason(tmp_path, monkeypatch):
    import perplab.resources as resources
    def unavailable(*args, **kwargs):
        raise ResourceLimitExceeded("memory enforcement failed: test")
    monkeypatch.setattr(resources, "memory_job", unavailable)
    with pytest.raises(ResourceLimitExceeded):
        with ResourceGuard(tmp_path, "backtest", directory=tmp_path / "run"):
            pytest.fail("work must not start")
    state = json.loads((tmp_path / "run/resources.json").read_text())
    assert state["status"] == "failed" and not state["enforced"]
    assert "enforcement failed" in state["termination_reason"]


def test_queue_heartbeats_until_memory_available(tmp_path, monkeypatch):
    import perplab.resources as resources
    monkeypatch.setattr(resources, "memory_job", lambda *a, **k: None)
    monkeypatch.setattr(resources, "close_job", lambda *a: None)
    monkeypatch.setattr(resources, "_read_leases", lambda con: [])
    available = [5*GiB]
    monkeypatch.setattr(resources, "system_memory", lambda: {"available_bytes": available[0], "total_bytes": 24*GiB})
    observed = []
    def beat():
        observed.append(json.loads((tmp_path / "run/resources.json").read_text()))
        if len(observed) == 2:
            available[0] = 10*GiB
    with ResourceGuard(tmp_path, "backtest", directory=tmp_path / "run", heartbeat=beat) as guard:
        assert guard.status == "running"
    assert len(observed) >= 3
    assert observed[0]["status"] == "queued" and "system RAM" in observed[0]["queue_reason"]


def test_exit_reasons_are_not_all_called_oom(tmp_path):
    assert worker_exit_reason(tmp_path, 0xC0000005) == "native_crash"
    assert worker_exit_reason(tmp_path, 1) == "worker_exited_without_result"
    (tmp_path / "resources.json").write_text(json.dumps({"termination_reason": "timeout: deadline"}))
    assert worker_exit_reason(tmp_path, 1).startswith("timeout")


@pytest.mark.skipif(sys.platform != "win32", reason="native Windows enforcement")
def test_native_limit_refuses_allocation_spike():
    code = '''
from perplab.resources import memory_job, process_memory, MiB
job = memory_job(None, process=192*MiB)
try:
    data = bytearray(512*MiB)
except MemoryError:
    print("refused", process_memory()["private_bytes"], flush=True)
else:
    raise AssertionError("allocation escaped the limit")
'''
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("refused")
    assert int(result.stdout.split()[1]) < 192*MiB


@pytest.mark.skipif(sys.platform != "win32", reason="native Windows descendant enforcement")
def test_native_aggregate_covers_descendants():
    code = '''
import subprocess, sys
from perplab.resources import memory_job, MiB
job = memory_job(None, total=200*MiB, process=160*MiB)
child = "import sys; from perplab.resources import MiB; data=bytearray(96*MiB); print('ready',flush=True); sys.stdin.readline()"
one = subprocess.Popen([sys.executable, '-c', child], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
try:
    assert one.stdout.readline().strip() == 'ready'
    two = subprocess.run([sys.executable, '-c', child], input='stop\\n', capture_output=True, text=True, timeout=10)
    assert two.returncode != 0 and 'MemoryError' in two.stderr, two.stderr
    print('aggregate enforced')
finally:
    one.communicate('stop\\n', timeout=10)
'''
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=40)
    assert result.returncode == 0, result.stderr
    assert "aggregate enforced" in result.stdout


def test_disk_event_hash_matches_original_bytes_and_history_pages_are_bounded(tmp_path):
    from perplab.engine.history import DiskSequence, history_scope
    from perplab.strategy.context import StrategyEvent
    from perplab.strategy.dryrun import event_hash
    from perplab.core.money import parse_money
    with history_scope():
        series = DiskSequence(tmp_path / "events.sqlite3", event_sequence=True)
        events = [StrategyEvent(seq=i, ts_ms=i*1000, kind="PROBE", payload={"cash": parse_money("123.400"), "text": "é", "nested": (i, True)}) for i in range(1300)]
        for event in events:
            series.append(event)
        assert event_hash(series) == event_hash(events)
        assert list(series[123:400]) == events[123:400]
        assert len(series._page) <= series.PAGE


def test_hourly_tick_and_disk_engine_match_full_range_replay(tmp_path, monkeypatch):
    from tests.engine_lake import build_lake, write_trades, ramp_path, MS_PER_MINUTE
    from tests.unit.test_backtest import Flipper, START
    from tests.support import btcusdt_filters, single_bracket_table
    from perplab.engine.backtest import BacktestConfig, BacktestEngine
    from perplab.engine.tiers import FillTier
    from perplab.engine.history import history_scope
    from perplab.core.money import parse_money
    from perplab.data.query import stream_query
    import perplab.engine.ticks as ticks
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=190, trade_path=ramp_path(40000, 1), funding=[(START+60*MS_PER_MINUTE, 0.001)])
    write_trades(lake, "BTCUSDT", [(START+i*30_000, 40000+i/2, 1, bool(i%2)) for i in range(380)])
    def execute(directory):
        strategy = Flipper()
        engine = BacktestEngine(root=lake, strategy=strategy, requirements=strategy.declared,
            config=BacktestConfig(symbols=("BTCUSDT",), timeframe="1m", start_ms=START, end_ms=START+190*MS_PER_MINUTE,
                opening_balance=parse_money("10000"), fill_tier=FillTier.TRADE_ONLY),
            filters={"BTCUSDT": btcusdt_filters()}, brackets={"BTCUSDT": single_bracket_table()}, history_directory=directory)
        result = engine.run()
        engine.account.reconcile()
        return result
    original = ticks.stream_windows
    monkeypatch.setattr(ticks, "stream_windows", lambda root, sql, *, datasets, params: stream_query(root, sql, datasets=datasets, params=params))
    baseline = execute(None)
    monkeypatch.setattr(ticks, "stream_windows", original)
    import perplab.engine.executor_base as runtime
    import perplab.engine.backtest as backtest
    monkeypatch.setattr(runtime, "MAX_EVENTS", 5)
    monkeypatch.setattr(backtest, "MAX_EVENTS", 5)
    with history_scope():
        bounded = execute(tmp_path / "history")
        assert bounded.ticks == baseline.ticks == 380
        assert bounded.event_hash == baseline.event_hash
        assert bounded.metrics.to_json() == baseline.metrics.to_json()
        assert bounded.attribution == baseline.attribution
        assert list(bounded.trades) == list(baseline.trades)
        assert list(bounded.equity) == list(baseline.equity)
        assert bounded.warnings == baseline.warnings


def trade(i, pnl):
    return {"index": i, "entry_ms": i*1000, "exit_ms": i*1000+1, "net_pnl": str(pnl), "mae": "-1", "mfe": "1", "duration_ms": 1}


def test_legacy_trade_sort_and_incremental_session_export(tmp_path):
    from perplab.engine.artifacts import trade_page, iter_trades, publish_trades
    path = tmp_path / "trades.json"
    rows = [trade(i, 300-i) for i in range(300)]
    path.write_text(json.dumps({"trades": rows}))
    page = trade_page(path, 0, 5, "net_pnl")
    assert page["total"] == 300 and page["trades"][0]["index"] == 299
    path.with_name("trades-index.sqlite3").unlink()
    publish_trades(path, rows[:150], 149)
    updated = {**rows[149], "net_pnl": "999"}
    publish_trades(path, rows[:149]+[updated]+rows[150:], 300)
    assert len(json.loads(path.read_text())["trades"]) == 200
    exported = list(iter_trades(path))
    assert len(exported) == 300 and exported[149]["net_pnl"] == "999"


def test_catalog_refreshes_and_keeps_files_with_missing_stats(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from perplab.data.catalog import catalog
    directory = tmp_path / "aggTrades"
    directory.mkdir()
    first = directory / "one.parquet"
    pq.write_table(pa.table({"ts_ms": [1, 2], "recv_ms": [2, 3]}), first, write_statistics=False)
    before = catalog(tmp_path, "aggTrades")
    assert len(before) == 1 and before[0]["min"] is None
    pq.write_table(pa.table({"ts_ms": [5, 6], "recv_ms": [7, 8]}), directory / "two.parquet")
    assert len(catalog(tmp_path, "aggTrades")) == 2


@pytest.mark.skipif(sys.platform != "win32", reason="native Windows monitor")
def test_ninety_percent_causes_controlled_stop(tmp_path):
    (tmp_path / "settings.json").write_text(json.dumps({"resource_worker_gib": .25}))
    code = '''
import sys, time
from pathlib import Path
from perplab.resources import ResourceGuard, checkpoint, ResourceLimitExceeded, MiB
try:
    with ResourceGuard(Path(sys.argv[1]), 'backtest', directory=Path(sys.argv[1])/'run'):
        data=bytearray(220*MiB)
        for i in range(40):
            time.sleep(.1)
            checkpoint('probe')
except ResourceLimitExceeded as exc:
    print(exc)
else:
    raise AssertionError('monitor failed to stop the job')
'''
    result = subprocess.run([sys.executable, "-c", code, str(tmp_path)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    state = json.loads((tmp_path / "run/resources.json").read_text())
    assert state["status"] == "failed" and state["warning"]
    assert state["termination_reason"].startswith("memory_limit")
    assert state["peak_bytes"] <= 256*MiB


@pytest.mark.skipif(sys.platform != "win32", reason="native Windows process-tree cleanup")
def test_unresponsive_research_is_forced_down_with_descendants(tmp_path):
    code = '''
import subprocess, sys, time
from pathlib import Path
from perplab.resources import ResourceGuard
with ResourceGuard(Path(sys.argv[1]), 'backtest', directory=Path(sys.argv[1])/'run') as guard:
    child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
    print(child.pid, flush=True)
    guard.request_stop('memory_limit: forced-stop probe')
    time.sleep(20)
'''
    process = subprocess.Popen([sys.executable, "-c", code, str(tmp_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    child = int(process.stdout.readline())
    started = time.monotonic()
    _, error = process.communicate(timeout=12)
    assert process.returncode == 73, error
    assert 4 <= time.monotonic()-started < 10
    from perplab.resources import process_memory
    for _ in range(20):
        if not process_memory(child):
            break
        time.sleep(.05)
    assert not process_memory(child)
    state = json.loads((tmp_path / "run/resources.json").read_text())
    assert "five seconds" in state["termination_reason"]


@pytest.mark.parametrize("flatten", [False, True])
def test_paper_resource_halt_cancels_orders_and_obeys_kill_policy(tmp_path, monkeypatch, flatten):
    from dataclasses import replace
    import perplab.live.session as live_session
    import perplab.live.worker as worker
    from tests.unit.test_session_lifecycle import _Clock, START_MS, RestingStop, paper_session, feed_minutes
    from perplab.store.killswitch import KillSwitchStore
    from perplab.engine.tiers import FillTier
    clock = _Clock(START_MS)
    monkeypatch.setattr(live_session, "_now_ms", clock)
    (tmp_path / "run").mkdir()
    session = paper_session(tmp_path / "run", RestingStop(), tier=FillTier.TRADE_ONLY)
    session.engine.config = replace(session.engine.config, kill_switch_flatten=flatten)
    session.engine.risk.kill_switch.flatten = flatten
    session.engine.on_halt = lambda: worker._arm_tripped_switch(tmp_path, 1, session.engine)
    session.engine.start()
    feed_minutes(session, clock)
    assert session.engine.account.positions
    session._halt_for_resources("memory_limit: probe")
    session._end()
    worker._arm_kill_switch(tmp_path, 1, session)
    assert session.engine.risk.kill_switch.trigger == "MEMORY_LIMIT"
    assert not any(order.is_open for order in session.engine.orders.values())
    assert bool(session.engine.account.positions) is not flatten
    with KillSwitchStore(tmp_path) as store:
        assert store.state().trigger == "MEMORY_LIMIT"
    session.seal()


def test_prepared_bars_preserve_week_boundary_and_partial_buckets(tmp_path):
    from tests.engine_lake import build_lake, ramp_path, MS_PER_MINUTE
    from tests.unit.test_backtest import START
    from perplab.engine.feed import load_bars
    from perplab.engine.history import history_scope
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=8*1440, trade_path=ramp_path(40000, .01))
    lo, hi = START+7*MS_PER_MINUTE, START+(8*1440-7)*MS_PER_MINUTE
    legacy, _ = load_bars(lake, ["BTCUSDT"], "4h", lo, hi)
    with history_scope():
        bounded, _ = load_bars(lake, ["BTCUSDT"], "4h", lo, hi, history_directory=tmp_path / "history")
        assert list(bounded) == list(legacy)


def test_coverage_metadata_fallback_filters_symbols_and_invalidates_cache(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from perplab.data.catalog import inventory
    directory = tmp_path / "klines"
    directory.mkdir()
    path = directory / "unpartitioned.parquet"
    table = pa.table({"symbol": ["ETHUSDT", "BTCUSDT", "BTCUSDT"],
                      "open_time": [0, 86_400_000, 259_200_000],
                      "close_time": [59_999, 86_459_999, 259_259_999]})
    pq.write_table(table, path, write_statistics=False)
    actual = inventory(tmp_path, "klines", "BTCUSDT", "open_time", end_clock="close_time", days=True)
    assert actual == {"rows": 2, "start_ms": 86_400_000, "end_ms": 259_260_000, "days": [1, 3]}
    pq.write_table(table.slice(1, 1), path, write_statistics=False)
    assert inventory(tmp_path, "klines", "BTCUSDT", "open_time", days=True)["rows"] == 1


def test_cache_disk_reservations_evict_closed_files_and_prevent_overcommit(tmp_path, monkeypatch):
    import sqlite3
    from types import SimpleNamespace
    import perplab.resources as resources
    monkeypatch.setattr(resources, "_registry", lambda root: sqlite3.connect(tmp_path / "registry.sqlite3"))
    monkeypatch.setattr(resources, "current_guard", lambda: SimpleNamespace(policy=SimpleNamespace(spill_gib=32_768/GiB)))
    root = tmp_path / "market"
    directory = root / "_replay" / "sorted"
    directory.mkdir(parents=True)
    old = directory / "old.parquet"
    old.write_bytes(b"x" * 8192)
    target = directory / "new.parquet"
    with resources.cache_reservation(root, 12_288, target=target):
        assert not old.exists()
        with pytest.raises(ResourceLimitExceeded, match="shared replay-cache"):
            with resources.cache_reservation(root, 8192):
                pytest.fail("concurrent reservations exceeded the shared cache allowance")
        target.write_bytes(b"x" * 4096)
    with resources.cache_reservation(root, 4096):
        assert target.exists()


def test_lab_point_journals_are_closed_and_removed_after_compact_result(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import importlib
    import perplab.resources as resources
    module = importlib.import_module("perplab.lab.sweep")
    from perplab.engine.history import DiskSequence
    job = tmp_path / "job"
    job.mkdir()
    preserved = job / "result.json"
    preserved.write_text("{}")
    monkeypatch.setattr(resources, "current_guard", lambda: SimpleNamespace(directory=job))
    def execute(root, point):
        history = module._history_directory.get()
        series = DiskSequence(history / "events.sqlite3")
        series.append({"value": 1})
        assert series[0] == {"value": 1}
        return "compact result"
    monkeypatch.setattr(module, "_run_point", execute)
    assert module.run_point(str(tmp_path), None) == "compact result"
    assert not list((job / "_points").glob("*/events.sqlite3"))
    assert preserved.read_text() == "{}"


def test_query_output_refuses_an_oversized_arrow_write_before_disk_growth():
    import io
    import pyarrow as pa
    from perplab.data.query_worker import _LimitedOutput
    file = io.BytesIO()
    output = _LimitedOutput(file, 4096)
    with pa.ipc.new_stream(output, pa.schema([("value", pa.int64())])) as writer:
        writer.write_batch(pa.record_batch([[1, 2, 3]], names=["value"]))
    assert pa.ipc.open_stream(file.getvalue()).read_all().num_rows == 3
    size = len(file.getvalue())
    with pytest.raises(ResourceLimitExceeded, match="query_response_limit"):
        output.write(b"x" * 4096)
    assert len(file.getvalue()) == size


def test_live_balance_adoption_keeps_the_enforced_policy_record(tmp_path, monkeypatch):
    from contextlib import closing
    import perplab.live.worker as worker
    import perplab.live.session as session_module
    from perplab.core.money import parse_money
    from perplab.store.runs import RunStore
    from tests.unit.test_session_lifecycle import _seed_run, _Clock, STOP_MS
    monkeypatch.setattr(session_module, "_now_ms", _Clock(STOP_MS))
    class EndProbe(Exception):
        pass
    async def venue(build, spec, secrets, run_id, stop):
        build(parse_money("1234.50"))
        raise EndProbe("adoption checked before any exchange stack is attached")
    monkeypatch.setattr(worker, "_execute_live", venue)
    with closing(RunStore(tmp_path)) as store:
        run_id = _seed_run(tmp_path, store)
        spec = store.read_json(run_id, "spec.json")
        policy = {"research_gib": 6, "worker_gib": 2}
        limits = [{"tree_bytes": 10*GiB, "process_bytes": 2*GiB}]
        spec.update(session_kind="live", resource_policy=policy, effective_resource_limits=limits)
        store.artefact(run_id, "spec.json").write_text(json.dumps(spec))
        store.request_stop(run_id, flatten=False, reason="test end")
        with pytest.raises(EndProbe):
            worker.execute_session(tmp_path, run_id, store, {})
        updated = store.read_json(run_id, "spec.json")
        assert parse_money(updated["opening_balance"]) == parse_money("1234.50")
        assert updated["resource_policy"] == policy
        assert updated["effective_resource_limits"] == limits
