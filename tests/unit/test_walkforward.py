"""Walk-forward analysis: folds, the neighbourhood objective, stitching, WFE (spec 9.1)."""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from perplab.engine.runspec import RunSpec
from perplab.lab.sweep import SweepResult, expand_grid
from perplab.lab.walkforward import (
    Fold,
    FoldRecord,
    GridShape,
    WalkForwardConfig,
    build_folds,
    choose_index,
    objective_values,
    plateau_score,
    run_walkforward,
    _stitch,
    _wfe,
)
from tests.engine_lake import MS_PER_MINUTE, build_lake, ramp_path
from tests.support import BTCUSDT_PAYLOAD

MS_PER_HOUR = 60 * MS_PER_MINUTE
START = 1_709_251_200_000  # 2024-03-01T00:00Z, a whole UTC hour


# ----------------------------------------------------------------------------- folds


def test_anchored_folds_grow_the_is_window_and_tile_the_oos() -> None:
    folds, uncovered = build_folds(
        0, 9 * MS_PER_HOUR, is_ms=3 * MS_PER_HOUR, oos_ms=2 * MS_PER_HOUR
    )
    assert [f.to_json() for f in folds] == [
        {
            "index": 0,
            "is_start_ms": 0,
            "is_end_ms": 3 * MS_PER_HOUR,
            "oos_start_ms": 3 * MS_PER_HOUR,
            "oos_end_ms": 5 * MS_PER_HOUR,
        },
        {
            "index": 1,
            "is_start_ms": 0,
            "is_end_ms": 5 * MS_PER_HOUR,
            "oos_start_ms": 5 * MS_PER_HOUR,
            "oos_end_ms": 7 * MS_PER_HOUR,
        },
        {
            "index": 2,
            "is_start_ms": 0,
            "is_end_ms": 7 * MS_PER_HOUR,
            "oos_start_ms": 7 * MS_PER_HOUR,
            "oos_end_ms": 9 * MS_PER_HOUR,
        },
    ]
    assert uncovered == 0


def test_rolling_folds_keep_the_is_window_at_fixed_length() -> None:
    folds, _ = build_folds(
        0,
        9 * MS_PER_HOUR,
        is_ms=3 * MS_PER_HOUR,
        oos_ms=2 * MS_PER_HOUR,
        mode="rolling",
    )
    assert [(f.is_start_ms, f.is_end_ms) for f in folds] == [
        (0, 3 * MS_PER_HOUR),
        (2 * MS_PER_HOUR, 5 * MS_PER_HOUR),
        (4 * MS_PER_HOUR, 7 * MS_PER_HOUR),
    ]
    assert all(f.is_end_ms - f.is_start_ms == 3 * MS_PER_HOUR for f in folds)


def test_a_partial_final_window_is_reported_not_evaluated() -> None:
    """Spec-honesty: the tail the stitched curve never saw is stated, not silently dropped.

    A 3-day stub beside 2-hour folds would put incomparable windows in one table; leaving
    it out without saying so would let a curve claim it covered a range it did not.
    """
    folds, uncovered = build_folds(
        0, 10 * MS_PER_HOUR, is_ms=3 * MS_PER_HOUR, oos_ms=2 * MS_PER_HOUR
    )
    assert folds[-1].oos_end_ms == 9 * MS_PER_HOUR
    assert uncovered == MS_PER_HOUR


def test_a_range_too_short_for_one_fold_is_refused() -> None:
    with pytest.raises(ValueError, match="one fold needs"):
        build_folds(0, 4 * MS_PER_HOUR, is_ms=3 * MS_PER_HOUR, oos_ms=2 * MS_PER_HOUR)


def test_unknown_mode_and_bad_windows_are_refused() -> None:
    with pytest.raises(ValueError, match="unknown walk-forward mode"):
        build_folds(0, 10, is_ms=3, oos_ms=2, mode="sideways")
    with pytest.raises(ValueError, match="must be positive"):
        build_folds(0, 10, is_ms=0, oos_ms=2)
    with pytest.raises(ValueError, match="step_ms must be positive"):
        build_folds(0, 10, is_ms=3, oos_ms=2, step_ms=0)


# ------------------------------------------------------------------------- grid shape


def test_grid_shape_indexing_agrees_with_expand_grid() -> None:
    """The neighbourhood is only correct if index arithmetic matches grid expansion.

    `expand_grid` is the order the sweep runs and reports in; `GridShape.position` is how
    the objective finds neighbours. If they disagreed, the "neighbourhood" of a point
    would be a set of unrelated parameter sets and the median would be over noise.
    """
    grid = {"fast": [5, 10, 20], "slow": [50, 100]}
    shape = GridShape.from_grid(grid)
    combos = expand_grid(grid)
    assert shape.size == len(combos) == 6
    for index, combo in enumerate(combos):
        position = shape.position(index)
        rebuilt = {
            name: shape.values[axis][position[axis]]
            for axis, name in enumerate(shape.names)
        }
        assert rebuilt == combo, f"index {index} disagrees"


def test_neighbours_are_one_step_along_one_axis_only() -> None:
    """Von Neumann, not Moore: a diagonal point changed two parameters, which is two steps."""
    shape = GridShape.from_grid({"a": [1, 2, 3], "b": [10, 20, 30]})
    # Index 4 is the centre (a=2, b=20): neighbours are the four edge-adjacent points.
    assert sorted(shape.neighbours(4)) == [1, 3, 5, 7]
    # Index 0 is the corner (a=1, b=10): two neighbours, no wrap-around.
    assert sorted(shape.neighbours(0)) == [1, 3]


def test_a_one_point_grid_has_no_neighbours() -> None:
    shape = GridShape.from_grid({})
    assert shape.size == 1
    assert shape.neighbours(0) == ()


# -------------------------------------------------------------------------- objective


def _result(index: int, sharpe: float | None, *, ok: bool = True) -> SweepResult:
    return SweepResult(index=index, params={"i": index}, ok=ok, sharpe=sharpe)


def test_the_default_objective_prefers_a_plateau_over_an_isolated_spike() -> None:
    """The entire point of spec 9.1's default (R15).

    One axis, seven points: a plateau of 1.0s on the left, a lone spike (Sharpe 3.0)
    flanked by 0.2s on the right. `max_sharpe` picks the spike; the neighbourhood median
    scores the spike by its poor flanks and picks the plateau.

    The spike must be *isolated* -- flanked by poor values on both sides -- for the
    demotion to bite. A spike bordering a plateau inherits the plateau through its own
    neighbourhood median, and an edge point with a single neighbour has a two-element
    median that is just the mean. Both are spec-literal properties of "median of the point
    and its immediate neighbours", not defects, and the plateau *score* (9.4) is what
    exposes such a choice afterwards.
    """
    grid = {"p": [1, 2, 3, 4, 5, 6, 7]}
    shape = GridShape.from_grid(grid)
    results = [
        _result(0, 0.9),
        _result(1, 1.0),
        _result(2, 1.0),
        _result(3, 0.2),
        _result(4, 3.0),
        _result(5, 0.2),
        _result(6, 0.9),
    ]
    spike = objective_values(results, shape, "max_sharpe")
    assert choose_index(spike) == 4

    plateau = objective_values(results, shape, "nbhd_median_sharpe")
    # Index 4 (the spike): median(3.0, 0.2, 0.2) = 0.2. Index 1: median(1.0, 0.9, 1.0) = 1.0.
    assert plateau[4] == pytest.approx(0.2)
    assert plateau[1] == pytest.approx(1.0)
    assert choose_index(plateau) == 1


def test_an_undefined_point_is_ineligible_and_an_undefined_neighbour_is_excluded() -> None:
    shape = GridShape.from_grid({"p": [1, 2, 3]})
    results = [_result(0, None), _result(1, 2.0), _result(2, 1.0)]
    values = objective_values(results, shape, "nbhd_median_sharpe")
    assert values[0] is None  # its own Sharpe is undefined -> cannot be chosen
    # Index 1's neighbourhood is {2.0, None, 1.0}: the None is excluded, median(2.0, 1.0).
    assert values[1] == pytest.approx(1.5)
    # Index 2 sees {1.0, 2.0}.
    assert values[2] == pytest.approx(1.5)


def test_a_failed_point_cannot_be_chosen_even_with_a_recorded_sharpe() -> None:
    shape = GridShape.from_grid({"p": [1, 2]})
    results = [_result(0, 5.0, ok=False), _result(1, 0.5)]
    values = objective_values(results, shape, "nbhd_median_sharpe")
    assert values[0] is None
    assert choose_index(values) == 1


def test_choose_index_breaks_ties_deterministically_low() -> None:
    assert choose_index([1.0, 2.0, 2.0]) == 1
    assert choose_index([None, None]) is None
    assert choose_index([math.inf, 1.0]) == 1  # non-finite is not a defensible choice


def test_grid_size_mismatch_is_refused() -> None:
    shape = GridShape.from_grid({"p": [1, 2, 3]})
    with pytest.raises(ValueError, match="3 points but 2 results"):
        objective_values([_result(0, 1.0), _result(1, 1.0)], shape, "max_sharpe")


# ---------------------------------------------------------------------- plateau score


def test_plateau_score_reads_near_one_on_a_plateau_and_large_on_a_spike() -> None:
    shape = GridShape.from_grid({"p": [1, 2, 3]})
    plateau = [_result(0, 1.0), _result(1, 1.1), _result(2, 1.0)]
    assert plateau_score(plateau, shape, 1) == pytest.approx(1.1)

    spike = [_result(0, 0.1), _result(1, 3.0), _result(2, 0.1)]
    assert plateau_score(spike, shape, 1) == pytest.approx(30.0)


def test_plateau_score_refuses_an_unprofitable_neighbourhood() -> None:
    """A positive spike over losing neighbours must not become a negative 'score'.

    That configuration is the strongest overfit signal there is, and a column meant to
    hover around 1.0 would bury it as a sign flip. The record carries it in words
    (`neighbourhood_unprofitable`) instead.
    """
    shape = GridShape.from_grid({"p": [1, 2, 3]})
    results = [_result(0, -1.0), _result(1, 2.0), _result(2, -0.5)]
    assert plateau_score(results, shape, 1) is None


# -------------------------------------------------------------------------------- WFE


def test_wfe_is_the_ratio_of_annualised_returns() -> None:
    assert _wfe(0.10, 0.20) == pytest.approx(0.5)


def test_wfe_is_undefined_over_a_non_positive_is_return() -> None:
    """An OOS loss over an IS loss would read as *positive* efficiency."""
    assert _wfe(-0.10, -0.20) is None
    assert _wfe(0.10, 0.0) is None
    assert _wfe(None, 0.2) is None
    assert _wfe(0.1, None) is None


# ---------------------------------------------------------------------------- stitch


def _fold_record(index: int, start_ms: int, end_ms: int, oos: dict | None) -> FoldRecord:
    fold = Fold(
        index=index,
        is_start_ms=0,
        is_end_ms=start_ms,
        oos_start_ms=start_ms,
        oos_end_ms=end_ms,
    )
    return FoldRecord(
        fold=fold,
        grid=(),
        objective=(),
        chosen_index=0 if oos is not None else None,
        chosen_params={} if oos is not None else None,
        is_sharpe=None,
        is_total_return=None,
        is_annualised=None,
        is_net_pnl=None,
        is_max_drawdown=None,
        is_round_trips=None,
        oos=oos,
        is_halted=False,
        is_halt_limit=None,
        wfe=None,
        plateau=None,
        neighbours_defined=0,
        neighbours_total=0,
        neighbourhood_unprofitable=False,
        error=None,
    )


def test_stitching_compounds_across_folds_and_reports_the_additive_curve_too() -> None:
    """Fold 2 is scaled to open where fold 1 closed; the PnL curve just adds.

    Fold 1: 100 -> 110 (+10%). Fold 2: 100 -> 120 (+20%). Compounded end: 132.
    Additive end: 100 + 10 + 20 = 130. Both are true statements about different sizing
    assumptions, which is why both are kept.
    """
    records = [
        _fold_record(0, 1000, 2000, {"equity_ms": [1000, 1500, 2000], "equity": [100.0, 105.0, 110.0]}),
        _fold_record(1, 2000, 3000, {"equity_ms": [2000, 2500, 3000], "equity": [100.0, 90.0, 120.0]}),
    ]
    warnings: list[str] = []
    out = _stitch(records, 100.0, warnings)
    assert warnings == []
    assert out.scales == [1.0, pytest.approx(1.1)]
    assert out.equity[-1] == pytest.approx(132.0)
    assert out.pnl[-1] == pytest.approx(130.0)
    assert out.fold_index == [0, 0, 0, 1, 1]
    # 2000 appears once: fold 2's opening sample does not advance the clock past fold 1's
    # closing one, so it is dropped rather than duplicating the timestamp.
    assert out.times == [1000, 1500, 2000, 2500, 3000]


def test_a_sample_carried_in_from_before_the_window_is_clamped_to_its_start() -> None:
    """The slice's first sample is the equity in force at the boundary (LOCF); its own
    timestamp can precede the window, and a stitched curve must stay monotonic."""
    records = [
        _fold_record(0, 1000, 2000, {"equity_ms": [900, 1500], "equity": [100.0, 101.0]}),
    ]
    out = _stitch(records, 100.0, [])
    assert out.times == [1000, 1500]


def test_stitching_stops_at_a_bankrupt_fold_and_says_so() -> None:
    records = [
        _fold_record(0, 1000, 2000, {"equity_ms": [1000, 2000], "equity": [100.0, 0.0]}),
        _fold_record(1, 2000, 3000, {"equity_ms": [2000, 3000], "equity": [100.0, 200.0]}),
    ]
    warnings: list[str] = []
    out = _stitch(records, 100.0, warnings)
    assert out.truncated_at_fold == 1
    assert out.equity[-1] == pytest.approx(0.0)
    assert any("truncated" in w for w in warnings)


def test_a_fold_without_an_oos_evaluation_is_skipped_visibly() -> None:
    records = [
        _fold_record(0, 1000, 2000, None),
        _fold_record(1, 2000, 3000, {"equity_ms": [2000, 3000], "equity": [100.0, 110.0]}),
    ]
    out = _stitch(records, 100.0, [])
    assert out.fold_index == [1, 1]
    assert out.scales == [1.0]


def test_a_fold_stitching_zero_samples_appends_no_scale_and_warns() -> None:
    """A fold can hold OOS samples yet stitch none of them, and it must then be
    skipped *whole*: no scale, no PnL advance, and a warning -- not a phantom entry.

    Fold 1's engine series never advanced past its window start, so `_run_oos` returned
    the one carried-forward sample (LOCF, ts 1900 < window start 2000). Clamped to 2000
    it collides with fold 0's close and the clock check drops it -- zero stitched
    samples. The buggy version still appended fold 1's scale, desynchronising
    `fold_scales` from `set(fold_index)`, silently.

    Hand-derived: opening balance 100. Fold 0: 100 -> 110, scale 100/100 = 1.0.
    Fold 1: skipped whole; running stays 110, cumulative PnL stays 10. Fold 2 opens at
    100 with 110 carried, scale 110/100 = 1.1, equity 100 -> 120 stitched as
    110 -> 132; additive curve 100 + 10 + (120 - 100) = 130. So scales must be
    exactly [1.0, 1.1] -- one per stitched fold {0, 2} -- where the bug produced
    [1.0, 1.0, 1.1]: three scales for two stitched folds, and a reader pairing them up
    read fold 2's multiplier as 1.0.
    """
    records = [
        _fold_record(0, 1000, 2000, {"equity_ms": [1000, 2000], "equity": [100.0, 110.0]}),
        _fold_record(1, 2000, 3000, {"equity_ms": [1900], "equity": [110.0]}),
        _fold_record(2, 3000, 4000, {"equity_ms": [3000, 4000], "equity": [100.0, 120.0]}),
    ]
    warnings: list[str] = []
    out = _stitch(records, 100.0, warnings)
    assert out.fold_index == [0, 0, 2, 2]
    assert out.scales == [1.0, pytest.approx(1.1)]
    assert len(out.scales) == len(set(out.fold_index))
    assert out.times == [1000, 2000, 3000, 4000]
    assert out.equity == pytest.approx([100.0, 110.0, 110.0, 132.0])
    assert out.pnl == pytest.approx([100.0, 110.0, 110.0, 130.0])
    # Skipped out loud, like the empty-window case -- a fold missing from the curve
    # with no warning is exactly the quiet omission `_stitch`'s warnings exist for.
    assert any("fold 1" in w and "skips it" in w for w in warnings)


# ------------------------------------------------------------------------ end to end


CODE = '''
from perplab.strategy import Strategy

class Directional(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 0, "datasets": ["klines"]}
    params = {"direction": {"type": "int", "default": 1, "min": -1, "max": 1}}

    def on_start(self, ctx):
        self.done = False

    def on_bar(self, ctx, bar):
        if ctx.warm and not self.done:
            self.done = True
            if self.p.direction > 0:
                ctx.buy(qty=ctx.money("0.01"))
            elif self.p.direction < 0:
                ctx.sell(qty=ctx.money("0.01"))
'''


def _lake(root: Path, minutes: int) -> Path:
    import json

    build_lake(
        root / "market",
        start_ms=START,
        minutes=minutes,
        trade_path=ramp_path(40_000.0, 1.0),
    )
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


def _spec(minutes: int) -> RunSpec:
    return RunSpec(
        strategy_id=1,
        version_id=1,
        version_no=1,
        strategy_name="Directional",
        code=CODE,
        class_name="Directional",
        params={"direction": 1},
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=START,
        end_ms=START + minutes * MS_PER_MINUTE,
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


def _config(**overrides) -> WalkForwardConfig:
    base = dict(
        is_ms=3 * MS_PER_HOUR,
        oos_ms=2 * MS_PER_HOUR,
        grid={"direction": [-1, 0, 1]},
        max_workers=1,
    )
    base.update(overrides)
    return WalkForwardConfig(**base)


def test_a_walkforward_optimises_each_fold_and_stitches_the_oos_curve(
    tmp_path: Path,
) -> None:
    """The end-to-end shape of spec 9.1 on a rising market.

    On a monotonic ramp, long wins, short loses, flat never trades (and a flat equity
    series has no Sharpe at all, so direction=0 is ineligible rather than mediocre --
    which also exercises the undefined-point path with real engine output). Every fold
    must therefore choose direction=+1, and the stitched OOS curve must end above the
    opening balance.
    """
    root = _lake(tmp_path, minutes=9 * 60)
    trials: list[tuple[dict, float | None]] = []
    result = run_walkforward(
        root,
        _spec(9 * 60),
        _config(),
        on_trial=lambda params, sharpe: trials.append((dict(params), sharpe)),
    )

    assert len(result.folds) == 3
    assert result.uncovered_ms == 0
    for record in result.folds:
        assert record.error is None
        assert record.chosen_params == {"direction": 1}
        assert record.oos is not None and record.oos["fills"] == 1
        assert record.wfe is not None and record.wfe > 0

    assert result.truncated_at_fold is None
    assert result.stitched_equity[0] == pytest.approx(100_000.0, rel=1e-6)
    assert result.stitched_equity[-1] > 100_000.0
    assert result.stitched_total_return is not None and result.stitched_total_return > 0
    assert result.wfe_aggregate is not None and result.wfe_aggregate > 0
    # Times strictly increasing across fold boundaries.
    assert all(a < b for a, b in zip(result.stitched_ms, result.stitched_ms[1:]))
    # Spec 8.5: every IS grid point and every OOS evaluation is a trial.
    assert len(trials) == 3 * (3 + 1)


def test_a_walkforward_is_deterministic_across_runs(tmp_path: Path) -> None:
    """Same lake, same config -> same choices, same curve, same event hashes (spec 12.1)."""
    root = _lake(tmp_path, minutes=9 * 60)
    first = run_walkforward(root, _spec(9 * 60), _config())
    second = run_walkforward(root, _spec(9 * 60), _config())

    assert first.stitched_equity == second.stitched_equity
    assert first.stitched_ms == second.stitched_ms
    assert [f.chosen_params for f in first.folds] == [
        f.chosen_params for f in second.folds
    ]
    assert [f.oos["event_hash"] for f in first.folds] == [
        f.oos["event_hash"] for f in second.folds
    ]


def test_a_risk_halted_optimisation_is_flagged_loudly(tmp_path: Path) -> None:
    """Discovered on real data: a strategy tripping a risk limit days into every anchored
    window makes all folds optimise over the same short stub while the fold table reads
    like full windows. The record and the warnings must both say so.

    The construction needs a chosen point that halts *after* enough profitable hours for
    its Sharpe to be defined -- a point that halts on its first bar has no Sharpe, is
    ineligible, and exercises the wrong path. A wave does it: the long rides the rising
    half (a defined, excellent Sharpe), then the falling half trips a hair-trigger
    drawdown cap.
    """
    import json
    from dataclasses import replace as dc_replace

    from tests.engine_lake import wave_path
    from tests.support import BTCUSDT_PAYLOAD as PAYLOAD

    minutes = 12 * 60
    build_lake(
        tmp_path / "market",
        start_ms=START,
        minutes=minutes,
        trade_path=wave_path(40_000.0, 600.0, 6 * 60),  # one 6-hour period
    )
    for kind, payload in {
        "exchangeInfo": {"symbols": [PAYLOAD]},
        "leverageBracket": [
            {
                "symbol": "BTCUSDT",
                "brackets": [
                    {"bracket": 1, "initialLeverage": 125, "notionalCap": 10_000_000_000,
                     "notionalFloor": 0, "maintMarginRatio": 0.004, "cum": 0.0}
                ],
            }
        ],
    }.items():
        directory = tmp_path / "reference" / kind
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "2024-01-01.json").write_text(json.dumps(payload), encoding="utf-8")

    spec = dc_replace(
        _spec(minutes), risk_limits={"max_drawdown_pct": "0.00001"}
    )
    result = run_walkforward(
        root=tmp_path,
        base_spec=spec,
        config=_config(is_ms=4 * MS_PER_HOUR, oos_ms=2 * MS_PER_HOUR),
    )

    chosen_folds = [r for r in result.folds if r.chosen_index is not None]
    assert chosen_folds, "no fold chose anything -- the construction is wrong, not the code"
    assert all(r.is_halted for r in chosen_folds)
    assert all(r.is_halt_limit == "max_drawdown" for r in chosen_folds)
    assert all(r.to_json()["is_halted"] is True for r in chosen_folds)
    text = " ".join(result.warnings)
    assert "risk-halted" in text
    assert "near-identical stubs" in text


def test_overlapping_oos_windows_are_refused(tmp_path: Path) -> None:
    """Stitching overlapping windows would count the same days twice in one curve."""
    with pytest.raises(ValueError, match="overlap"):
        run_walkforward(
            tmp_path,
            _spec(9 * 60),
            _config(step_ms=MS_PER_HOUR),
        )


def test_config_json_round_trips() -> None:
    config = _config(mode="rolling", objective="max_sharpe", step_ms=2 * MS_PER_HOUR)
    rebuilt = WalkForwardConfig.from_json(config.to_json())
    assert rebuilt.is_ms == config.is_ms
    assert rebuilt.oos_ms == config.oos_ms
    assert rebuilt.step_ms == config.step_ms
    assert rebuilt.mode == "rolling"
    assert rebuilt.objective == "max_sharpe"
    assert rebuilt.grid == {"direction": [-1, 0, 1]}
    assert config.to_json()["objective_label"].endswith("(overfit-prone)")
