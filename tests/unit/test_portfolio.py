"""Portfolio analysis and allocation modes (spec 9.5)."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from perplab.analytics.trades import TradeBuilder
from perplab.api.app import create_app
from perplab.engine.worker import execute_run
from perplab.lab.allocation import (
    custom_weights,
    equal_notional,
    fixed_fractional,
    inverse_volatility,
)
from perplab.lab.portfolio import portfolio_report
from perplab.store.runs import RunStore
from tests.engine_lake import MS_PER_MINUTE, build_lake, wave_path
from tests.integration.test_run_worker import seed_strategy
from tests.support import BTCUSDT_PAYLOAD
from perplab.store import db
from perplab.engine.runspec import RunSpec
from perplab.engine import ENGINE_VERSION

MS_PER_HOUR = 60 * MS_PER_MINUTE
START = 1_709_251_200_000


# ------------------------------------------------------------------------- allocation


def test_equal_notional_splits_the_budget_evenly() -> None:
    assert equal_notional(["A", "B", "C"], 300.0) == {"A": 100.0, "B": 100.0, "C": 100.0}
    with pytest.raises(ValueError, match="no symbols"):
        equal_notional([], 100.0)
    with pytest.raises(ValueError, match="duplicate"):
        equal_notional(["A", "A"], 100.0)
    with pytest.raises(ValueError, match="positive"):
        equal_notional(["A"], 0.0)


def test_inverse_volatility_weights_toward_the_quiet_symbol() -> None:
    weights = inverse_volatility({"CALM": 0.1, "WILD": 0.2}, 300.0)
    assert weights["CALM"] == pytest.approx(200.0)
    assert weights["WILD"] == pytest.approx(100.0)
    with pytest.raises(ValueError, match="non-positive volatility"):
        inverse_volatility({"BROKEN": 0.0}, 100.0)


def test_fixed_fractional_scales_with_symbol_count_by_design() -> None:
    """Ten symbols at 10% is 100% aggregate -- the mode's own meaning, not a bug."""
    weights = fixed_fractional(["A", "B", "C"], 1_000.0, 0.1)
    assert weights == {"A": 100.0, "B": 100.0, "C": 100.0}
    with pytest.raises(ValueError, match="fraction"):
        fixed_fractional(["A"], 1_000.0, 1.5)
    with pytest.raises(ValueError, match="equity"):
        fixed_fractional(["A"], 0.0, 0.1)


def test_custom_weights_normalise_and_refuse_direction() -> None:
    weights = custom_weights({"A": 3.0, "B": 1.0}, 400.0)
    assert weights["A"] == pytest.approx(300.0)
    assert weights["B"] == pytest.approx(100.0)
    with pytest.raises(ValueError, match="direction belongs to the strategy"):
        custom_weights({"A": -1.0}, 100.0)
    with pytest.raises(ValueError, match="sum to zero"):
        custom_weights({"A": 0.0}, 100.0)


# ------------------------------------------------------------------- trade builder


def test_net_pnl_by_symbol_tracks_closed_and_open_trades() -> None:
    builder = TradeBuilder()
    # Symbol A: open 1 @ 100, close 1 @ 110, fee 1 each leg -> net +8.
    builder.fill(
        ts_ms=1, symbol="A", signed_qty=Decimal(1), price=Decimal(100),
        fee=Decimal(1), realized=Decimal(0), qty_before=Decimal(0), qty_after=Decimal(1),
    )
    builder.fill(
        ts_ms=2, symbol="A", signed_qty=Decimal(-1), price=Decimal(110),
        fee=Decimal(1), realized=Decimal(10), qty_before=Decimal(1), qty_after=Decimal(0),
    )
    # Symbol B: open 1 @ 50, fee 1, marked at 53 -> running +2.
    builder.fill(
        ts_ms=3, symbol="B", signed_qty=Decimal(1), price=Decimal(50),
        fee=Decimal(1), realized=Decimal(0), qty_before=Decimal(0), qty_after=Decimal(1),
    )
    builder.mark(symbol="B", mark_price=Decimal(53), unrealized=Decimal(3))

    totals = builder.net_pnl_by_symbol()
    assert totals["A"] == Decimal(8)
    assert totals["B"] == Decimal(2)
    # And the totals agree with the rendered trades' own net PnL.
    snapshot = builder.snapshot()
    assert sum(t.net_pnl for t in snapshot if t.symbol == "A") == Decimal(8)


def test_a_flip_does_not_leak_the_old_trades_unrealized_into_the_new_one() -> None:
    builder = TradeBuilder()
    builder.fill(
        ts_ms=1, symbol="A", signed_qty=Decimal(1), price=Decimal(100),
        fee=Decimal(0), realized=Decimal(0), qty_before=Decimal(0), qty_after=Decimal(1),
    )
    builder.mark(symbol="A", mark_price=Decimal(120), unrealized=Decimal(20))
    # Flip: sell 2 at 120. The long closes (+20 realised), a short opens.
    builder.fill(
        ts_ms=2, symbol="A", signed_qty=Decimal(-2), price=Decimal(120),
        fee=Decimal(0), realized=Decimal(20), qty_before=Decimal(1), qty_after=Decimal(-1),
    )
    totals = builder.net_pnl_by_symbol()
    # Closed +20; the fresh short has no mark yet, so it contributes zero -- not the
    # stale +20 the old trade last saw.
    assert totals["A"] == Decimal(20)


# -------------------------------------------------------------------------- report


def _series(deltas: list[float]) -> list[float]:
    out = [0.0]
    for delta in deltas:
        out.append(out[-1] + delta)
    return out


def _report_inputs(
    a_deltas: list[float], b_deltas: list[float], *, names=("BTCUSDT", "ETHUSDT")
):
    count = len(a_deltas)
    times = [START + i * MS_PER_HOUR for i in range(count + 1)]
    return {
        "times": times,
        "symbol_pnl": {
            names[0]: _series(a_deltas),
            names[1]: _series(b_deltas),
        },
        "start_ms": START,
        "end_ms": START + count * MS_PER_HOUR,
    }


def test_correlation_is_over_pnl_changes_not_levels() -> None:
    """Two profitable symbols whose *daily fortunes* are opposite must correlate
    negatively -- their cumulative levels both rise, which is exactly the lie the
    level correlation would tell."""
    a = [1.0, 2.0, 1.0, 2.0, 1.0, 2.0]
    b = [2.0, 1.0, 2.0, 1.0, 2.0, 1.0]  # opposite fortune, still profitable
    report = portfolio_report(**_report_inputs(a, b), rolling_window=3)
    matrix = report["correlation"]["matrix"]
    assert matrix[0][1] == pytest.approx(-1.0)
    assert report["per_symbol"]["BTCUSDT"]["final_pnl"] == pytest.approx(9.0)


def test_rolling_correlation_sees_the_regime_shift_a_static_matrix_hides() -> None:
    """Spec 9.5: correlations converge in crashes; the rolling series is the evidence."""
    together = [1.0, 2.0] * 6
    a = together + together
    b = together + [-x for x in together]
    report = portfolio_report(**_report_inputs(a, b), rolling_window=6)
    pair = report["rolling_correlation"][0]
    assert pair["pair"] == ["BTCUSDT", "ETHUSDT"]
    assert pair["correlation"][0] == pytest.approx(1.0)
    assert pair["correlation"][-1] == pytest.approx(-1.0)


def test_an_untraded_symbol_is_flagged_and_correlates_with_nothing() -> None:
    report = portfolio_report(
        **_report_inputs([1.0, -1.0, 2.0, -2.0], [0.0, 0.0, 0.0, 0.0]),
        rolling_window=3,
    )
    assert report["correlation"]["matrix"][0][1] is None
    assert report["per_symbol"]["ETHUSDT"]["traded"] is False
    assert any("never traded" in w for w in report["warnings"])


def test_the_liquid_majors_warning_fires_only_for_an_all_major_list() -> None:
    majors = portfolio_report(
        **_report_inputs([1.0, -1.0, 2.0], [2.0, 1.0, -1.0]), rolling_window=3
    )
    assert any("currently-liquid major" in w for w in majors["warnings"])

    mixed = portfolio_report(
        **_report_inputs(
            [1.0, -1.0, 2.0], [2.0, 1.0, -1.0], names=("BTCUSDT", "OBSCUREUSDT")
        ),
        rolling_window=3,
    )
    assert not any("currently-liquid major" in w for w in mixed["warnings"])


def test_report_refusals() -> None:
    inputs = _report_inputs([1.0, 2.0, 3.0], [1.0, 2.0, 3.0])
    with pytest.raises(ValueError, match="at least two symbols"):
        portfolio_report(
            times=inputs["times"],
            symbol_pnl={"BTCUSDT": inputs["symbol_pnl"]["BTCUSDT"]},
            start_ms=inputs["start_ms"],
            end_ms=inputs["end_ms"],
        )
    with pytest.raises(ValueError, match="rolling_window"):
        portfolio_report(**inputs, rolling_window=2)
    bad = dict(inputs)
    bad["symbol_pnl"] = {"BTCUSDT": [0.0], "ETHUSDT": inputs["symbol_pnl"]["ETHUSDT"]}
    with pytest.raises(ValueError, match="samples for"):
        portfolio_report(**bad)


# ------------------------------------------------------------------- end to end


PAIR_SOURCE = '''
from perplab import Strategy

class Pair(Strategy):
    requires = {"symbols": ["BTCUSDT", "ETHUSDT"], "timeframe": "1m", "history": 0,
                "datasets": ["klines"]}
    params = {}

    def on_start(self, ctx):
        self.done = set()

    def on_bar(self, ctx, bar):
        if ctx.warm and bar.symbol not in self.done:
            self.done.add(bar.symbol)
            # 0.01 BTC at 40k clears the $50 minNotional; ETH at 3k needs 0.02 to.
            if bar.symbol == "BTCUSDT":
                ctx.buy(bar.symbol, qty=ctx.money("0.01"))
            else:
                ctx.sell(bar.symbol, qty=ctx.money("0.02"))
'''


def _two_symbol_userdata(root: Path, minutes: int) -> None:
    # Proportional waves, same phase. The strategy longs one and shorts the other, so
    # their per-period PnL deltas oscillate in exact opposition -- a *robustly* negative
    # correlation. (Two ramps looked like the obvious fixture and were degenerate: every
    # period's delta is the same, the only variance is the partial first period, and the
    # Pearson of two one-informative-sample series is a coin toss about boundary
    # effects.)
    build_lake(
        root / "market", symbol="BTCUSDT", start_ms=START, minutes=minutes,
        trade_path=wave_path(40_000.0, 600.0, 120),
    )
    build_lake(
        root / "market", symbol="ETHUSDT", start_ms=START, minutes=minutes,
        trade_path=wave_path(3_000.0, 60.0, 120),
    )
    eth_payload = dict(BTCUSDT_PAYLOAD, symbol="ETHUSDT")
    exchange = root / "reference" / "exchangeInfo"
    exchange.mkdir(parents=True, exist_ok=True)
    (exchange / "2024-01-01.json").write_text(
        json.dumps({"symbols": [BTCUSDT_PAYLOAD, eth_payload]}), encoding="utf-8"
    )
    bracket = {
        "bracket": 1, "initialLeverage": 125, "notionalCap": 1_000_000_000,
        "notionalFloor": 0, "maintMarginRatio": 0.004, "cum": 0.0,
    }
    brackets = root / "reference" / "leverageBracket"
    brackets.mkdir(parents=True, exist_ok=True)
    (brackets / "2024-01-01.json").write_text(
        json.dumps(
            [
                {"symbol": "BTCUSDT", "brackets": [bracket]},
                {"symbol": "ETHUSDT", "brackets": [bracket]},
            ]
        ),
        encoding="utf-8",
    )
    db.connect(root).close()


def test_a_two_symbol_run_records_per_symbol_curves_and_serves_the_report(
    tmp_path: Path,
) -> None:
    """The whole spec 9.5 chain: engine tracks, worker persists, endpoint reports.

    The strategy longs BTC and shorts ETH over proportional waves, so the two PnL
    curves move in exact opposition period by period: they must sum to the account's
    own PnL and correlate strongly negatively.
    """
    minutes = 6 * 60
    _two_symbol_userdata(tmp_path, minutes)
    strategy_id, version_id = seed_strategy(tmp_path, PAIR_SOURCE, name="Pair")
    spec = RunSpec(
        strategy_id=strategy_id,
        version_id=version_id,
        version_no=1,
        strategy_name="Pair",
        code=PAIR_SOURCE,
        class_name="Pair",
        params={},
        symbols=("BTCUSDT", "ETHUSDT"),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
        seed=11,
        opening_balance="100000",
        leverage=5,
        maker_rate="0.0002",
        taker_rate="0.0005",
        fee_source="test",
        latency={"model": "fixed", "submit_ms": 10, "cancel_ms": 10},
        fill_tier="BAR_CLOSE",
        fill_model={"tier": "BAR_CLOSE", "slippage_bps": "1.0"},
        liquidation_recovery_pct="0",
        timeout_s=120.0,
        engine_version=ENGINE_VERSION,
    )
    runs = RunStore(tmp_path)
    run_id = runs.create(
        strategy_id=strategy_id, version_id=version_id, spec=spec.to_storage()
    )
    execute_run(tmp_path, run_id, runs)
    summary = runs.get(run_id)
    assert summary.status == "done", summary.error

    path = runs.artefact(run_id, "per_symbol.parquet")
    assert path.exists()

    import pyarrow.parquet as pq

    table = pq.read_table(path)
    assert set(table.schema.names) == {"ts_ms", "BTCUSDT", "ETHUSDT"}
    btc = table.column("BTCUSDT").to_pylist()
    eth = table.column("ETHUSDT").to_pylist()
    times = table.column("ts_ms").to_pylist()
    assert len(btc) == len(eth) == len(times) > 0
    # Both actually traded: each curve leaves zero once its position is on.
    assert any(value != 0.0 for value in btc)
    assert any(value != 0.0 for value in eth)

    # The decomposition sums to the account's own PnL at the final sample.
    equity = pq.read_table(runs.artefact(run_id, "equity.parquet"))
    final_equity = equity.column("equity").to_pylist()[-1]
    assert btc[-1] + eth[-1] == pytest.approx(final_equity - 100_000.0, abs=1e-6)

    app = create_app(tmp_path)
    with TestClient(app) as client:
        response = client.get(f"/api/runs/{run_id}/portfolio?window=3")
        assert response.status_code == 200, response.json()
        report = response.json()
        assert report["symbols"] == ["BTCUSDT", "ETHUSDT"]
        off_diagonal = report["correlation"]["matrix"][0][1]
        assert off_diagonal is not None and off_diagonal < -0.9
        assert any("currently-liquid major" in w for w in report["warnings"])
        assert report["per_symbol"]["BTCUSDT"]["round_trips"] is None  # still open
    runs.close()


def test_a_single_symbol_run_serves_an_honest_404(tmp_path: Path) -> None:
    from tests.integration.test_run_worker import (
        STRATEGY_SOURCE,
        build_userdata,
        make_spec,
    )

    build_userdata(tmp_path)
    strategy_id, version_id = seed_strategy(tmp_path, STRATEGY_SOURCE)
    runs = RunStore(tmp_path)
    run_id = runs.create(
        strategy_id=strategy_id,
        version_id=version_id,
        spec=make_spec(strategy_id, version_id, STRATEGY_SOURCE).to_storage(),
    )
    execute_run(tmp_path, run_id, runs)
    assert not runs.artefact(run_id, "per_symbol.parquet").exists()

    app = create_app(tmp_path)
    with TestClient(app) as client:
        response = client.get(f"/api/runs/{run_id}/portfolio")
        assert response.status_code == 404
        assert "Single-symbol runs" in response.json()["detail"]
    runs.close()
