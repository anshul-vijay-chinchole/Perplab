"""Monte Carlo resampling: the four methods and their honesty rules (spec 9.2)."""

from __future__ import annotations

import pytest

from perplab.lab.montecarlo import (
    METHODS,
    MonteCarloConfig,
    MonteCarloInputs,
    run_montecarlo,
)


def _inputs(**overrides) -> MonteCarloInputs:
    base = dict(
        trade_pnls=(10.0, -5.0, 20.0, -10.0, 15.0),
        grid_returns=(0.01, -0.02, 0.015, 0.005, -0.01, 0.02, 0.0, 0.01),
        periods_per_year=365,
        grid_label="daily",
        opening_balance=1_000.0,
        max_drawdown_limit=None,
    )
    base.update(overrides)
    return MonteCarloInputs(**base)


def _config(**overrides) -> MonteCarloConfig:
    base = dict(iterations=500, seed=7)
    base.update(overrides)
    return MonteCarloConfig(**base)


# ------------------------------------------------------------------ trade permutation


def test_permutation_leaves_final_equity_invariant_and_varies_only_the_path() -> None:
    """Spec 9.2's mandatory caveat, as an assertion.

    Opening 100 with trades (-50, +100): both orderings end at 150, but the path through
    50 has a 50% drawdown and the path through 200 a 25% one. The final-equity
    distribution must therefore be a point mass, and the drawdown distribution must
    contain exactly those two values.
    """
    result = run_montecarlo(
        _inputs(trade_pnls=(-50.0, 100.0), opening_balance=100.0),
        _config(methods=("trade_permutation",)),
    )
    method = result["methods"]["trade_permutation"]
    finals = method["final_equity"]
    assert finals["min"] == finals["max"] == pytest.approx(150.0)
    drawdowns = method["max_drawdown"]
    assert drawdowns["min"] == pytest.approx(-0.5)
    assert drawdowns["max"] == pytest.approx(-0.25)
    assert method["sizing"] == "additive"
    assert method["sharpe"] is None
    assert "no time grid" in method["sharpe_unavailable_reason"]


def test_permutation_counts_ruin_when_a_path_touches_zero() -> None:
    """Opening 100, trades (-150, +200): the ordering that loses first goes to -50."""
    result = run_montecarlo(
        _inputs(trade_pnls=(-150.0, 200.0), opening_balance=100.0),
        _config(methods=("trade_permutation",)),
    )
    method = result["methods"]["trade_permutation"]
    assert method["prob_ruin"] is not None
    assert 0.0 < method["prob_ruin"] < 1.0


def test_breach_probability_reaches_on_equality_like_the_risk_layer() -> None:
    """Spec 7's convention: a limit is *reached*, not exceeded.

    Both orderings of (-50, +100) draw down at least 25%, so against a 0.25 limit the
    breach probability is exactly 1.0 -- which is only true if equality counts.
    """
    result = run_montecarlo(
        _inputs(
            trade_pnls=(-50.0, 100.0),
            opening_balance=100.0,
            max_drawdown_limit=0.25,
        ),
        _config(methods=("trade_permutation",)),
    )
    method = result["methods"]["trade_permutation"]
    assert method["prob_drawdown_breach"] == pytest.approx(1.0)
    assert method["drawdown_limit"] == 0.25


# ------------------------------------------------------------------- trade bootstrap


def test_bootstrap_finals_actually_vary() -> None:
    """Sampling with replacement must produce a distribution, not a point mass --
    a bootstrap whose finals were all equal would be the permutation wearing its name."""
    result = run_montecarlo(_inputs(), _config(methods=("trade_bootstrap",)))
    method = result["methods"]["trade_bootstrap"]
    finals = method["final_equity"]
    assert finals["count"] == 500
    assert finals["min"] < finals["max"]
    assert method["sharpe"] is None


# ------------------------------------------------------------------- block bootstrap


def test_block_bootstrap_on_constant_returns_is_a_point_mass_with_no_sharpe() -> None:
    """Every block of a constant series is the same block; the resample is the original.

    Final equity is exactly `opening * 1.01^n`, and a zero-variance return series has no
    Sharpe -- the distribution must come back `None` rather than infinite.
    """
    n = 16
    result = run_montecarlo(
        _inputs(grid_returns=(0.01,) * n, opening_balance=1_000.0),
        _config(methods=("block_bootstrap",)),
    )
    method = result["methods"]["block_bootstrap"]
    expected = 1_000.0 * 1.01**n
    assert method["final_equity"]["min"] == pytest.approx(expected)
    assert method["final_equity"]["max"] == pytest.approx(expected)
    assert method["sharpe"] is None
    assert method["block_length"] == 4  # round(sqrt(16))
    # A *measured* zero, not a refusal: constant +1% returns never reach zero, and the
    # method now reports that rather than claiming ruin is structurally unreachable --
    # which was false, since a grid return at or below -100% does floor a path.
    assert method["prob_ruin"] == 0.0
    assert method["ruin_unavailable_reason"] is None


def test_block_bootstrap_varies_and_respects_a_configured_block_length() -> None:
    result = run_montecarlo(
        _inputs(), _config(methods=("block_bootstrap",), block_length=2)
    )
    method = result["methods"]["block_bootstrap"]
    assert method["block_length"] == 2
    assert method["final_equity"]["min"] < method["final_equity"]["max"]
    assert method["sharpe"] is not None


def test_a_total_loss_return_floors_the_path_at_zero() -> None:
    """A -150% grid return cannot turn equity negative and flip every later sample."""
    result = run_montecarlo(
        _inputs(grid_returns=(0.01, -1.5, 0.02, 0.01)),
        _config(methods=("block_bootstrap",), block_length=1),
    )
    method = result["methods"]["block_bootstrap"]
    assert method["final_equity"]["min"] >= 0.0
    assert method["max_drawdown"]["min"] >= -1.0


# ---------------------------------------------------------------------- random start


def test_random_start_enumerates_every_start_exactly() -> None:
    """Exact, not sampled: constant returns make final equity a known function of skip."""
    n = 16
    result = run_montecarlo(
        _inputs(grid_returns=(0.01,) * n, opening_balance=1_000.0),
        _config(methods=("random_start",), max_skip_fraction=0.25),
    )
    method = result["methods"]["random_start"]
    series = method["series"]
    assert [row["skipped"] for row in series] == [0, 1, 2, 3, 4]
    for row in series:
        assert row["final_equity"] == pytest.approx(1_000.0 * 1.01 ** (n - row["skipped"]))
    # Skip zero is the run itself; more skipped periods of a gain means less final equity.
    finals = [row["final_equity"] for row in series]
    assert finals == sorted(finals, reverse=True)


def test_every_method_declares_its_sampling_so_enumeration_cannot_pass_for_draws() -> None:
    """`random_start` is an exact enumeration whose `iterations` is a candidate count,
    and it sits in one artefact beside methods that really did draw `config.iterations`
    paths. The distinction must be machine-readable -- a prose note a consumer would
    have to parse is not a field.

    Hand-derived: 16 grid returns at the default max_skip_fraction 0.25 give
    floor(16 * 0.25) = 4 as the deepest start, so starts 0..4 -- 5 evaluations, not the
    config's 500 -- and the method must say those 5 were exhaustive while
    block_bootstrap's 500 were random.
    """
    result = run_montecarlo(
        _inputs(grid_returns=(0.01, -0.02) * 8),
        _config(methods=("block_bootstrap", "random_start")),
    )
    exact = result["methods"]["random_start"]
    drawn = result["methods"]["block_bootstrap"]
    assert exact["sampling"] == "exhaustive"
    assert exact["iterations"] == 5
    assert drawn["sampling"] == "random"
    assert drawn["iterations"] == 500
    assert result["config"]["iterations"] == 500


def test_a_method_that_cannot_run_still_declares_its_sampling() -> None:
    """The refusal payload keeps the full shape (its own docstring's contract), so a
    consumer keying on `sampling` never meets a method row without one."""
    result = run_montecarlo(
        _inputs(trade_pnls=(5.0,), grid_returns=(0.01, 0.02)),
        _config(),
    )
    for name, expected in (
        ("trade_permutation", "random"),
        ("trade_bootstrap", "random"),
        ("block_bootstrap", "random"),
        ("random_start", "exhaustive"),
    ):
        method = result["methods"][name]
        assert method["error"] is not None
        assert method["sampling"] == expected


# ----------------------------------------------------------------------- determinism


def test_the_whole_artefact_is_deterministic_given_the_seed() -> None:
    first = run_montecarlo(_inputs(), _config())
    second = run_montecarlo(_inputs(), _config())
    assert first == second


def test_a_different_seed_moves_the_draws() -> None:
    first = run_montecarlo(_inputs(), _config(seed=1, methods=("trade_bootstrap",)))
    second = run_montecarlo(_inputs(), _config(seed=2, methods=("trade_bootstrap",)))
    assert (
        first["methods"]["trade_bootstrap"]["final_equity"]
        != second["methods"]["trade_bootstrap"]["final_equity"]
    )


def test_methods_draw_independently_so_the_list_does_not_couple_them() -> None:
    """Running one method alone gives the same numbers as running it among the four --
    each seeds its own generator, so adding a method cannot move another's draws."""
    alone = run_montecarlo(_inputs(), _config(methods=("trade_bootstrap",)))
    together = run_montecarlo(_inputs(), _config())
    assert (
        alone["methods"]["trade_bootstrap"]
        == together["methods"]["trade_bootstrap"]
    )


# ---------------------------------------------------------------------------- guards


def test_too_few_trades_reports_why_instead_of_omitting_the_method() -> None:
    result = run_montecarlo(
        _inputs(trade_pnls=(5.0,)),
        _config(methods=("trade_permutation", "trade_bootstrap")),
    )
    for name in ("trade_permutation", "trade_bootstrap"):
        method = result["methods"][name]
        assert method["error"] is not None and "not run" in method["error"]
        assert method["final_equity"] is None


def test_too_few_returns_reports_why() -> None:
    result = run_montecarlo(
        _inputs(grid_returns=(0.01, 0.02)),
        _config(methods=("block_bootstrap", "random_start")),
    )
    for name in ("block_bootstrap", "random_start"):
        assert result["methods"][name]["error"] is not None


def test_a_non_positive_opening_balance_is_refused() -> None:
    with pytest.raises(ValueError, match="opening balance"):
        run_montecarlo(_inputs(opening_balance=0.0), _config())


def test_config_validation_refuses_nonsense() -> None:
    with pytest.raises(ValueError, match="unknown Monte Carlo methods"):
        MonteCarloConfig.from_json({"methods": ["monte"]})
    with pytest.raises(ValueError, match="at least 100"):
        MonteCarloConfig.from_json({"iterations": 10})
    with pytest.raises(ValueError, match="block_length"):
        MonteCarloConfig.from_json({"block_length": 0})
    with pytest.raises(ValueError, match="max_skip_fraction"):
        MonteCarloConfig.from_json({"max_skip_fraction": 0.9})


def test_config_json_round_trips() -> None:
    config = MonteCarloConfig(
        iterations=2_000, seed=42, methods=("block_bootstrap",), block_length=8,
        max_skip_fraction=0.1,
    )
    rebuilt = MonteCarloConfig.from_json(config.to_json())
    assert rebuilt == config
    assert tuple(MonteCarloConfig.from_json({}).methods) == METHODS


def test_the_artefact_carries_the_sizing_caveats() -> None:
    result = run_montecarlo(_inputs(), _config())
    text = " ".join(result["caveats"])
    assert "does not change final equity" in text
    assert "fixed-notional" in text
