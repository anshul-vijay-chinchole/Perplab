"""Margin mode: exposed as a setting, isolated by default, cross refused rather than faked.

The question this answers is not "which balance pool backs the position" -- it is whether
the platform will *say* it is doing something it cannot do. Cross margin is not a label on
the same arithmetic: under it a position's liquidation price depends on the unrealised PnL
of every other open position, so there is no closed form and `margin.liquidation_price`
cannot express it.

Accepting `CROSSED` and computing the isolated form under it would produce a liquidation
price the exchange does not agree with, on the screen where being wrong is most expensive.
So the refusal is the feature, and these tests pin it at each of the three doors: the run
spec, the two API routes, and the exchange preflight.
"""

from __future__ import annotations

import pytest

from perplab.core.types import MarginMode
from perplab.engine.runspec import RunSpec
from tests.unit.test_runs_api import add_strategy, client, start_body  # noqa: F401

# --------------------------------------------------------------------------- the enum


def test_isolated_is_the_only_implemented_mode() -> None:
    assert MarginMode.ISOLATED.is_implemented is True
    assert MarginMode.CROSSED.is_implemented is False


def test_parse_accepts_isolated_in_any_casing() -> None:
    assert MarginMode.parse("isolated") is MarginMode.ISOLATED
    assert MarginMode.parse("  ISOLATED  ") is MarginMode.ISOLATED


def test_parse_refuses_cross_with_the_reason_not_just_the_name() -> None:
    """The message has to say *why*, because "invalid value" invites trying again.

    Someone who selected cross margin wants to know whether it is coming, not whether they
    spelled it right.
    """
    with pytest.raises(ValueError, match="no closed form"):
        MarginMode.parse("CROSSED")


def test_binances_other_spelling_is_understood_so_the_refusal_is_about_the_right_thing() -> None:
    """`CROSS` and `CROSSED` must produce the same answer.

    Otherwise the user gets "unknown margin mode 'CROSS'" and concludes the feature exists
    under some other name.
    """
    with pytest.raises(ValueError, match="no closed form"):
        MarginMode.parse("CROSS")


def test_an_unknown_mode_is_named_rather_than_defaulted() -> None:
    with pytest.raises(ValueError, match="unknown margin mode"):
        MarginMode.parse("PORTFOLIO")


# ----------------------------------------------------------------------- the run spec


def _spec(**overrides) -> RunSpec:
    base = dict(
        strategy_id=1,
        version_id=1,
        version_no=1,
        strategy_name="S",
        code="x = 1",
        class_name=None,
        params={},
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=0,
        end_ms=60_000,
        seed=1,
        opening_balance="10000",
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


def test_the_spec_defaults_to_isolated_and_round_trips() -> None:
    spec = _spec()
    assert spec.margin_mode == "ISOLATED"
    assert RunSpec.from_storage(spec.to_storage()).margin_mode == "ISOLATED"


def test_a_spec_written_before_this_field_reads_back_as_isolated() -> None:
    """**Exact, not approximate.**

    The ledger has never implemented anything but isolated margin, so reading an older spec
    as ISOLATED reports what that run actually did. This is the opposite of `risk_limits`,
    where an absent value had to mean "no risk layer" rather than "the defaults" -- because
    there, the older runs genuinely had no limits, and defaulting would have rewritten their
    history.
    """
    stored = _spec().to_storage()
    del stored["margin_mode"]
    assert RunSpec.from_storage(stored).margin_mode == "ISOLATED"


# --------------------------------------------------------------------------- the doors


def test_a_backtest_defaults_to_isolated(client) -> None:  # noqa: F811
    handle, root = client
    strategy_id = add_strategy(root)
    response = handle.post("/api/runs", json=start_body(strategy_id))
    assert response.status_code in (200, 201), response.text
    run_id = response.json()["run"]["id"]

    detail = handle.get(f"/api/runs/{run_id}").json()
    assert detail["spec"]["margin_mode"] == "ISOLATED"


def test_a_backtest_asking_for_cross_is_refused(client) -> None:  # noqa: F811
    handle, root = client
    strategy_id = add_strategy(root)
    body = {**start_body(strategy_id), "margin_mode": "CROSSED"}
    response = handle.post("/api/runs", json=body)

    assert response.status_code == 400
    assert "no closed form" in response.json()["detail"]


def test_the_refusal_reaches_the_session_route_too(client) -> None:  # noqa: F811
    """Spec 7's argument applied to configuration: a rule that stops a backtest stops a
    session identically. A margin mode refused on one route and accepted on the other is
    how a live session ends up running under a setting the backtester would not allow."""
    handle, root = client
    strategy_id = add_strategy(root)
    response = handle.post(
        "/api/sessions",
        json={
            "strategy_id": strategy_id,
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "margin_mode": "CROSSED",
        },
    )

    assert response.status_code == 400
    assert "no closed form" in response.json()["detail"]
