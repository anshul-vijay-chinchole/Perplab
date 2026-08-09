"""The job worker, end to end, in a real subprocess -- and the spec 12.1 invariant.

> **Invariant:** identical inputs -> identical event-log SHA-256. This is enforced by a CI
> test, not by good intentions. A backtest you cannot reproduce is an anecdote.

The determinism test below is that CI test. It matters that the two runs happen in
*separate interpreters*, with different `PYTHONHASHSEED` values: comparing two runs inside
one process proves only that a process is deterministic, and the failure that actually
occurs -- a set or dict iteration order reaching the event log -- is invisible without
varying the seed. That is the same argument the Phase 3 validator's determinism probe makes,
applied one layer up to a real backtest over real bars.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from perplab.engine import ENGINE_VERSION
from perplab.engine.runspec import RunSpec
from perplab.store import db
from perplab.store.runs import RunStatus, RunStore
from tests.engine_lake import MS_PER_MINUTE, build_lake, wave_path
from tests.support import BTCUSDT_PAYLOAD

START = 1_709_251_200_000  # 2024-03-01T00:00:00Z
MINUTES = 600

STRATEGY_SOURCE = '''
from perplab import Strategy


class Wobble(Strategy):
    params = {"period": {"type": "int", "default": 5, "min": 2, "max": 50}}
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 50,
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        self.sma = ctx.indicators.sma(self.p.period)
        self.count = 0

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return
        self.count += 1
        # A set literal is iterated on purpose: it is the shape whose ordering varies with
        # PYTHONHASHSEED, so a determinism failure has somewhere to come from.
        tags = {"alpha", "beta", "gamma", "delta"}
        above = ctx.mark() > ctx.money(self.sma.value)
        if above and ctx.position().is_flat:
            ctx.buy(qty=ctx.money("0.05"), tag=sorted(tags)[0])
        elif not above and not ctx.position().is_flat:
            ctx.close()
        ctx.record("sma", self.sma.value)
'''

BROKEN_SOURCE = '''
from perplab import Strategy


class Broken(Strategy):
    params = {"period": {"type": "int", "default": 5, "min": 2, "max": 50}}
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1}

    def on_bar(self, ctx, bar):
        if ctx.warm:
            raise RuntimeError("deliberate strategy failure")
'''

_BRACKETS = """
[
  {
    "symbol": "BTCUSDT",
    "brackets": [
      {
        "bracket": 1,
        "initialLeverage": 125,
        "notionalCap": 1000000000000,
        "notionalFloor": 0,
        "maintMarginRatio": 0.004,
        "cum": 0
      }
    ]
  }
]
"""
"""Written as text so no Python float ever exists in the fixture.

`brackets_from_payload` refuses floats outright, because by the time one arrives the
precision is gone: `Decimal(0.004)` is `0.004000000000000000083...`, and that value
multiplies a six-figure notional inside every liquidation solve.
"""


def build_userdata(root: Path, *, snapshot_date: str = "2024-01-01") -> None:
    """A complete userdata tree: lake, reference snapshots, and an empty database."""
    build_lake(
        root / "market",
        start_ms=START,
        minutes=MINUTES,
        # A wave, not a ramp: a monotonic series never crosses its own moving average, so a
        # crossover strategy on one would trade identically whatever period it was given --
        # and a test that varied the period would then prove nothing.
        trade_path=wave_path(40_000.0, 600.0, 60),
        funding=[(START + 240 * MS_PER_MINUTE, 0.0001)],
    )
    exchange = root / "reference" / "exchangeInfo"
    exchange.mkdir(parents=True, exist_ok=True)
    (exchange / f"{snapshot_date}.json").write_text(
        json.dumps({"symbols": [BTCUSDT_PAYLOAD]}), encoding="utf-8"
    )
    brackets = root / "reference" / "leverageBracket"
    brackets.mkdir(parents=True, exist_ok=True)
    (brackets / f"{snapshot_date}.json").write_text(_BRACKETS, encoding="utf-8")
    db.connect(root).close()


def seed_strategy(root: Path, source: str, name: str = "Wobble") -> tuple[int, int]:
    connection = db.connect(root)
    with connection:
        strategy_id = connection.execute(
            "INSERT INTO strategies (name, created_ms, updated_ms) VALUES (?, 0, 0)",
            (name,),
        ).lastrowid
        version_id = connection.execute(
            """
            INSERT INTO strategy_versions
                (strategy_id, version_no, code, code_sha256, created_ms, valid)
            VALUES (?, 1, ?, 'sha', 0, 1)
            """,
            (strategy_id, source),
        ).lastrowid
    connection.close()
    return int(strategy_id), int(version_id)


def make_spec(strategy_id: int, version_id: int, source: str, *, seed: int = 3) -> RunSpec:
    return RunSpec(
        strategy_id=strategy_id,
        version_id=version_id,
        version_no=1,
        strategy_name="Wobble",
        code=source,
        class_name=None,
        params={"period": 5},
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START + 20 * MS_PER_MINUTE,
        end_ms=START + MINUTES * MS_PER_MINUTE,
        seed=seed,
        opening_balance="10000",
        leverage=10,
        maker_rate="0.0002",
        taker_rate="0.0005",
        fee_source="test",
        latency={"model": "fixed", "submit_ms": 120, "cancel_ms": 120},
        fill_tier="BAR_CLOSE",
        fill_model={"tier": "BAR_CLOSE", "slippage_bps": "1.0"},
        liquidation_recovery_pct="0",
        timeout_s=300.0,
        engine_version=ENGINE_VERSION,
    )


def run_worker(root: Path, run_id: int, *, hash_seed: str) -> subprocess.CompletedProcess[str]:
    """Execute the worker in a fresh interpreter with an explicit `PYTHONHASHSEED`."""
    environment = dict(os.environ)
    environment["PYTHONHASHSEED"] = hash_seed
    return subprocess.run(
        [sys.executable, "-m", "perplab.engine.worker", str(root), str(run_id)],
        cwd=str(Path(__file__).resolve().parents[2]),
        capture_output=True,
        text=True,
        timeout=300,
        env=environment,
        check=False,
    )


def start_and_wait(store: RunStore, run_id: int, timeout_s: float = 240.0) -> None:
    store.launch(run_id)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if store.get(run_id).status in RunStatus.TERMINAL:
            return
        time.sleep(0.2)
    raise AssertionError(f"run {run_id} did not finish within {timeout_s}s")


# --------------------------------------------------------------------------------- tests


@pytest.mark.slow
def test_a_run_executes_in_a_subprocess_and_writes_every_artefact(tmp_path: Path) -> None:
    build_userdata(tmp_path)
    strategy_id, version_id = seed_strategy(tmp_path, STRATEGY_SOURCE)
    store = RunStore(tmp_path)
    try:
        spec = make_spec(strategy_id, version_id, STRATEGY_SOURCE)
        run_id = store.create(
            strategy_id=strategy_id, version_id=version_id, spec=spec.to_storage()
        )
        start_and_wait(store, run_id)
        summary = store.get(run_id)
        assert summary.status == RunStatus.DONE, summary.error
        assert summary.event_hash
        assert summary.fill_tier == "BAR_CLOSE"
        assert summary.fills and summary.fills > 0

        directory = store.directory(run_id)
        for name in ("spec.json", "manifest.json", "metrics.json", "trades.json",
                     "events.jsonl", "equity.parquet"):
            assert (directory / name).exists(), name

        manifest = store.read_json(run_id, "manifest.json")
        # Spec 12.1's list, in full.
        repro = manifest["reproducibility"]
        for field in ("code_sha256", "params", "seed", "engine_version", "fees",
                      "latency", "fill_tier", "fill_model", "risk_limits", "event_hash",
                      "fill_model_tier", "platform_commit"):
            assert field in repro, field
        assert repro["event_hash"] == summary.event_hash
        # The manifest's range must cover the warm-up, not just the trading window.
        assert repro["data_start_ms"] < spec.start_ms

        # A snapshot dated before the range is the one in force, so nothing is approximate.
        assert "FILTERS_APPROXIMATE" not in summary.flags
        assert "BRACKETS_APPROXIMATE" not in summary.flags
        assert manifest["reference"]["exchangeInfo_in_force"] == "2024-01-01"

        metrics = store.read_json(run_id, "metrics.json")
        assert metrics["metrics"]["grid"] == "hourly"
        assert metrics["attribution"]["net_pnl"]

        trials = store.trials(strategy_id)
        assert trials["combinations"] == 1 and trials["evaluations"] == 1
    finally:
        store.close()


@pytest.mark.slow
def test_identical_inputs_produce_an_identical_event_hash_across_interpreters(
    tmp_path: Path,
) -> None:
    """Spec 12.1's invariant, enforced rather than intended.

    Two separate interpreters, two different `PYTHONHASHSEED` values, one lake. Comparing
    two runs inside one process would prove only that the process is deterministic; the
    failure this catches -- a set or dict iteration order reaching the event log -- is
    invisible unless the seed varies.
    """
    build_userdata(tmp_path)
    strategy_id, version_id = seed_strategy(tmp_path, STRATEGY_SOURCE)
    store = RunStore(tmp_path)
    try:
        hashes = []
        for hash_seed in ("1", "13"):
            spec = make_spec(strategy_id, version_id, STRATEGY_SOURCE)
            run_id = store.create(
                strategy_id=strategy_id, version_id=version_id, spec=spec.to_storage()
            )
            completed = run_worker(tmp_path, run_id, hash_seed=hash_seed)
            summary = store.get(run_id)
            assert summary.status == RunStatus.DONE, completed.stderr
            hashes.append(summary.event_hash)

        assert hashes[0] == hashes[1]
        assert hashes[0] is not None
    finally:
        store.close()


@pytest.mark.slow
def test_changing_one_input_changes_the_hash(tmp_path: Path) -> None:
    """The other half of the invariant. A hash that never moves proves nothing at all --
    it would be satisfied by hashing the empty string."""
    build_userdata(tmp_path)
    strategy_id, version_id = seed_strategy(tmp_path, STRATEGY_SOURCE)
    store = RunStore(tmp_path)
    try:
        baseline_spec = make_spec(strategy_id, version_id, STRATEGY_SOURCE)
        baseline_id = store.create(
            strategy_id=strategy_id, version_id=version_id, spec=baseline_spec.to_storage()
        )
        start_and_wait(store, baseline_id)

        changed = make_spec(strategy_id, version_id, STRATEGY_SOURCE)
        payload = changed.to_storage()
        payload["params"] = {"period": 9}
        other_id = store.create(
            strategy_id=strategy_id, version_id=version_id, spec=payload
        )
        start_and_wait(store, other_id)

        assert store.get(baseline_id).status == RunStatus.DONE
        assert store.get(other_id).status == RunStatus.DONE
        assert store.get(baseline_id).event_hash != store.get(other_id).event_hash
        # And the two parameter combinations are two trials, not one.
        assert store.trials(strategy_id)["combinations"] == 2
    finally:
        store.close()


@pytest.mark.slow
def test_a_strategy_that_raises_produces_a_failed_run_not_a_dead_worker(tmp_path: Path) -> None:
    """The traceback is the artefact. A worker that died silently would leave a row still
    claiming to be running, which is indistinguishable from one that hung."""
    build_userdata(tmp_path)
    strategy_id, version_id = seed_strategy(tmp_path, BROKEN_SOURCE, name="Broken")
    store = RunStore(tmp_path)
    try:
        spec = make_spec(strategy_id, version_id, BROKEN_SOURCE)
        run_id = store.create(
            strategy_id=strategy_id, version_id=version_id, spec=spec.to_storage()
        )
        start_and_wait(store, run_id)
        summary = store.get(run_id)
        assert summary.status == RunStatus.FAILED
        assert "deliberate strategy failure" in (summary.error or "")
        assert "Traceback" in (summary.error or "")
    finally:
        store.close()


@pytest.mark.slow
def test_a_range_past_the_lake_keeps_its_results(tmp_path: Path) -> None:
    """A completed run is not discarded over a manifest presence check.

    `build_manifest` runs after the engine, and `derive_fill_model_tier` refuses a range
    without a published file in *every* partition -- so a range extending a few days past
    the end of the lake, which the run form accepts as free text, used to throw away a
    finished backtest and leave nothing but `spec.json`. The engine had already read what
    was there and flagged the shortfall; the manifest now records that rather than raising.
    """
    build_userdata(tmp_path)
    strategy_id, version_id = seed_strategy(tmp_path, STRATEGY_SOURCE)
    store = RunStore(tmp_path)
    try:
        spec = make_spec(strategy_id, version_id, STRATEGY_SOURCE)
        payload = spec.to_storage()
        # 45 days past the end of the generated lake, which is what it takes to cross into
        # a *month* partition that was never written: klines partition by month, so a range
        # ending a few days late still resolves to the same directory and covers fine.
        payload["end_ms"] = START + (MINUTES + 45 * 24 * 60) * MS_PER_MINUTE
        run_id = store.create(
            strategy_id=strategy_id, version_id=version_id, spec=payload
        )
        start_and_wait(store, run_id)
        summary = store.get(run_id)
        assert summary.status == RunStatus.DONE, summary.error
        assert "COVERAGE_INCOMPLETE" in summary.flags
        assert any("dataset manifest could not be completed" in w for w in summary.warnings)
        # The results themselves survive.
        for name in ("metrics.json", "trades.json", "events.jsonl", "equity.parquet"):
            assert (store.directory(run_id) / name).exists(), name
        assert store.read_json(run_id, "manifest.json")["dataset"] is None
    finally:
        store.close()


@pytest.mark.slow
def test_a_snapshot_only_from_after_the_range_is_used_and_flagged(tmp_path: Path) -> None:
    """Spec 3.2 wants the snapshot in force at the range's start. None exists for any
    historical range on this deployment, and refusing every historical backtest is worse
    than using the nearest one and saying so."""
    build_userdata(tmp_path, snapshot_date="2026-08-01")
    strategy_id, version_id = seed_strategy(tmp_path, STRATEGY_SOURCE)
    store = RunStore(tmp_path)
    try:
        spec = make_spec(strategy_id, version_id, STRATEGY_SOURCE)
        run_id = store.create(
            strategy_id=strategy_id, version_id=version_id, spec=spec.to_storage()
        )
        start_and_wait(store, run_id)
        summary = store.get(run_id)
        assert summary.status == RunStatus.DONE, summary.error
        assert "FILTERS_APPROXIMATE" in summary.flags
        assert "BRACKETS_APPROXIMATE" in summary.flags
        manifest = store.read_json(run_id, "manifest.json")
        assert manifest["reference"]["exchangeInfo_used"] == "2026-08-01"
        assert manifest["reference"]["exchangeInfo_in_force"] is None
    finally:
        store.close()
