"""Parameter sweeps: grid expansion, parallel execution, and what happens to failures."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from perplab.engine.runspec import RunSpec
from perplab.lab.sweep import (
    SweepPoint,
    default_workers,
    expand_grid,
    points_from_spec,
    run_point,
    sweep,
)
from tests.engine_lake import MS_PER_MINUTE, build_lake, flat_path
from tests.support import BTCUSDT_PAYLOAD

START = 1_709_251_200_000

CODE = '''
from perplab.strategy import Strategy

class Buyer(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 0, "datasets": ["klines"]}
    params = {"size": {"type": "float", "default": 1.0, "min": 0.001, "max": 100.0}}

    def on_start(self, ctx):
        self.done = False

    def on_bar(self, ctx, bar):
        if ctx.warm and not self.done:
            self.done = True
            ctx.buy(qty=ctx.money(self.p.size))
'''


# ------------------------------------------------------------------------ grid expansion


def test_a_grid_expands_in_a_stable_order() -> None:
    """Sorted keys, values in the order given.

    Spec 8.5 counts trials, and a trial *sequence* that depended on dict iteration order
    would differ between interpreters -- so two people running the same sweep would be
    counting the same number of different experiments.
    """
    grid = {"b": [1, 2], "a": ["x", "y"]}
    assert expand_grid(grid) == [
        {"a": "x", "b": 1},
        {"a": "x", "b": 2},
        {"a": "y", "b": 1},
        {"a": "y", "b": 2},
    ]


def test_an_empty_grid_is_one_point_not_zero() -> None:
    """Sweeping nothing is running the strategy once, which is a useful thing to ask for."""
    assert expand_grid({}) == [{}]


def test_a_parameter_with_no_values_is_refused() -> None:
    """Silently producing zero points would look like a sweep that finished instantly."""
    with pytest.raises(ValueError, match="no values"):
        expand_grid({"size": []})


def test_default_workers_leaves_a_core_free() -> None:
    """The collector is very likely on the same machine, and it must not miss a message."""
    import os

    assert default_workers() == max(1, (os.cpu_count() or 2) - 1)
    assert default_workers() >= 1


# ----------------------------------------------------------------------------- execution


def _spec(**overrides) -> RunSpec:
    base = dict(
        strategy_id=1,
        version_id=1,
        version_no=1,
        strategy_name="Buyer",
        code=CODE,
        class_name="Buyer",
        params={"size": 1.0},
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 10 * MS_PER_MINUTE,
        seed=7,
        opening_balance="100000",
        leverage=5,
        maker_rate="0.0002",
        taker_rate="0.0005",
        fee_source="test",
        latency={"model": "fixed", "submit_ms": 10, "cancel_ms": 10},
        fill_tier="BAR_CLOSE",
        fill_model={"tier": "BAR_CLOSE", "slippage_bps": "1.0"},
        liquidation_recovery_pct="0",
        timeout_s=60.0,
        engine_version=1,
    )
    base.update(overrides)
    return RunSpec(**base)


def _lake(root: Path) -> Path:
    """A minimal lake plus the reference snapshots `resolve_filters` insists on.

    Dated 2024-01-01, before the range, so `resolve_filters` finds a snapshot *in force*
    at the start and the run is not flagged `FILTERS_APPROXIMATE`. A sweep whose every
    point carried an approximation flag would train the reader to ignore it.
    """
    import json

    build_lake(root / "market", start_ms=START, minutes=10, trade_path=flat_path(40_000.0))
    payloads = {
        "exchangeInfo": {"symbols": [BTCUSDT_PAYLOAD]},
        "leverageBracket": [
            {
                "symbol": "BTCUSDT",
                "brackets": [
                    {
                        "bracket": 1,
                        "initialLeverage": 125,
                        "notionalCap": 50_000,
                        "notionalFloor": 0,
                        "maintMarginRatio": 0.004,
                        "cum": 0.0,
                    },
                    {
                        "bracket": 2,
                        "initialLeverage": 100,
                        "notionalCap": 10_000_000,
                        "notionalFloor": 50_000,
                        "maintMarginRatio": 0.005,
                        "cum": 50.0,
                    },
                ],
            }
        ],
    }
    for kind, payload in payloads.items():
        directory = root / "reference" / kind
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "2024-01-01.json").write_text(json.dumps(payload), encoding="utf-8")
    return root


def test_a_sweep_runs_every_point_and_returns_them_in_grid_order(tmp_path: Path) -> None:
    """Order is a function of the grid, not of which worker finished first.

    Results arrive in completion order and are sorted back, so a sweep run twice on the
    same inputs produces the same list -- which is what makes it comparable with the sweep
    somebody ran last week.
    """
    root = _lake(tmp_path)
    points = points_from_spec(_spec(), {"size": [1.0, 2.0, 3.0]})
    assert [p.index for p in points] == [0, 1, 2]

    results = sweep(root, points, max_workers=1)
    assert [r.index for r in results] == [0, 1, 2]
    assert [r.params["size"] for r in results] == [1.0, 2.0, 3.0]
    assert all(r.ok for r in results), [r.error for r in results if not r.ok]


def test_bigger_size_moves_the_answer(tmp_path: Path) -> None:
    """A sanity check that the parameter actually reached the strategy.

    A sweep whose points all returned the same number would be a sweep that silently ran
    the default N times, and every statistic computed from it would be a statistic about
    one experiment.
    """
    root = _lake(tmp_path)
    results = sweep(root, points_from_spec(_spec(), {"size": [1.0, 5.0]}), max_workers=1)
    assert results[0].fills == 1
    assert results[1].fills == 1
    # Same flat tape, so both lose only fees -- but five times the size pays five times
    # the fee, which is the observable difference.
    assert Decimal(results[1].net_pnl) < Decimal(results[0].net_pnl)


def test_a_point_that_raises_is_reported_rather_than_dropped(tmp_path: Path) -> None:
    """A hole in a grid must not look like a complete grid.

    The failing points are usually the interesting ones -- a parameter set that liquidates,
    or one the exchange refuses -- so they are kept with their error attached.
    """
    root = _lake(tmp_path)
    broken = SweepPoint(
        index=0,
        params={},
        code="this is not python",
        class_name=None,
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + 10 * MS_PER_MINUTE,
        seed=1,
        opening_balance="10000",
        leverage=1,
        maker_rate="0.0002",
        taker_rate="0.0005",
        fee_source="test",
        latency={"model": "fixed", "submit_ms": 10, "cancel_ms": 10},
        fill_tier="BAR_CLOSE",
        liquidation_recovery_pct="0",
        timeout_s=60.0,
    )
    results = sweep(root, [broken], max_workers=1)
    assert len(results) == 1
    assert results[0].ok is False
    assert results[0].error is not None
    assert results[0].net_pnl is None


def test_a_sweep_carries_the_risk_limits_from_its_base_spec(tmp_path: Path) -> None:
    """Only the parameters vary. Everything else is held fixed by construction.

    A sweep whose points differed in fee schedule, latency or risk limits as well as in
    parameters would be a different experiment wearing a sweep's name, and its grid could
    not be read as a parameter surface.
    """
    spec = _spec(risk_limits={"max_position_notional": "1000", "max_leverage": None})
    points = points_from_spec(spec, {"size": [1.0, 2.0]})
    assert all(p.risk_limits == spec.risk_limits for p in points)

    root = _lake(tmp_path)
    results = sweep(root, points, max_workers=1)
    # 1 BTC at 40 000 is well past a 1 000 ceiling, so every point is refused.
    assert all(r.risk_rejects == 1 for r in results)
    assert all(r.fills == 0 for r in results)


def test_an_empty_sweep_is_not_an_error(tmp_path: Path) -> None:
    assert sweep(tmp_path, []) == []


def test_max_workers_below_one_is_refused(tmp_path: Path) -> None:
    """Zero workers would hang rather than fail, which is the worse way to be wrong."""
    root = _lake(tmp_path)
    with pytest.raises(ValueError, match="at least 1"):
        sweep(root, points_from_spec(_spec(), {"size": [1.0]}), max_workers=0)


def test_two_workers_produce_the_same_answers_as_one(tmp_path: Path) -> None:
    """Parallelism must not be observable in the results.

    Each point carries its own seed and reads a read-only lake, so the pool changes only
    wall time. The event hash is the check that actually proves it: two runs of the same
    point produce identical event logs, whatever process they ran in (spec 12.1).
    """
    root = _lake(tmp_path)
    points = points_from_spec(_spec(), {"size": [1.0, 2.0]})
    serial = sweep(root, points, max_workers=1)
    parallel = sweep(root, points, max_workers=2)
    assert [r.event_hash for r in serial] == [r.event_hash for r in parallel]
    assert [r.net_pnl for r in serial] == [r.net_pnl for r in parallel]


def test_progress_fires_once_per_point(tmp_path: Path) -> None:
    root = _lake(tmp_path)
    seen: list[int] = []
    sweep(
        root,
        points_from_spec(_spec(), {"size": [1.0, 2.0, 3.0]}),
        max_workers=1,
        on_result=lambda r: seen.append(r.index),
    )
    assert sorted(seen) == [0, 1, 2]
