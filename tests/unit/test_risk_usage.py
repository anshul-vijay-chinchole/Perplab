"""The risk-usage panel's arithmetic, and the wire shape the monitor reads it from.

This file exists because the panel shipped before the number did. `LiveMonitor` rendered
`risk.usage` from the day it was written, but nothing in the platform ever *produced* a
`usage` key -- `RiskEngine.summary()` has no such field -- so the first monitor snapshot
of every session handed the component `undefined`, the component threw, and with no error
boundary above it the whole platform unmounted to a white window. TypeScript verified the
UI against a hand-written interface, not against the Python that writes the JSON, so the
gap was invisible to every check that ran. These tests pin the contract from the producing
side: the snapshot carries `usage`, and each row's fraction is computed the same way the
check that fires on that limit computes.

**The readings must agree with the checks, not merely exist.** A usage bar drawn from
different arithmetic than the refusal is worse than no bar -- it reads 80% at the moment
an order is refused, or 100% while orders keep going through. So each test here drives the
engine's real observe/check path into a known state and then asserts the reading matches
the number the check would use: daily loss from the *day's* open (not the run's), drawdown
from the peak (not the opening balance), the rate window over the same half-open minute.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from perplab.core.money import parse_money
from perplab.core.risk import KillSwitch, RiskEngine, RiskLimits


def money(text: str) -> Decimal:
    return parse_money(text)


def engine_with(limits: RiskLimits, equity: str = "10000") -> RiskEngine:
    return RiskEngine(
        limits=limits, starting_equity=money(equity), kill_switch=KillSwitch()
    )


def usage_by_limit(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    table = {row["limit"]: row for row in rows}
    assert len(table) == len(rows), "a limit may appear at most once"
    return table


NO_EXPOSURE: dict[str, Any] = {
    "notional": Decimal(0),
    "max_symbol_notional": Decimal(0),
    "open_orders": 0,
}


# ------------------------------------------------------------- the readings themselves


def test_daily_loss_is_scored_from_the_days_open_not_the_runs() -> None:
    """After a profitable day rolls over, the baseline is the rolled equity.

    Scored from the run's start, a strategy that made 10% yesterday could lose it all
    today and show a usage of zero -- the check would fire long before the bar filled.
    """
    engine = engine_with(RiskLimits(max_daily_loss_pct=money("0.02")))
    day = 86_400_000
    engine.observe_equity(1, money("11000"))  # day 0 ends up 1000
    engine.observe_equity(day + 1, money("11000"))  # day 1 opens at 11000
    engine.observe_equity(day + 2, money("10890"))  # loss of 110 against a limit of 200

    row = usage_by_limit(
        engine.usage(ts_ms=day + 2, equity=money("10890"), **NO_EXPOSURE)
    )["max_daily_loss"]
    # The budget is 2% of *starting* equity (10000 -> 200), exactly as `max_daily_loss`
    # derives it; the spend is measured from the day's open (11000 -> 110).
    assert row["used"] == "110.00000000"
    assert row["allowed"] == "200.00000000"
    assert row["fraction"] == pytest.approx(0.55)


def test_drawdown_is_scored_from_the_peak_not_the_opening_balance() -> None:
    engine = engine_with(RiskLimits(max_drawdown_pct=money("0.15")))
    engine.observe_equity(1, money("12000"))  # the peak
    engine.observe_equity(2, money("11100"))  # 7.5% below it

    row = usage_by_limit(
        engine.usage(ts_ms=2, equity=money("11100"), **NO_EXPOSURE)
    )["max_drawdown"]
    assert row["fraction"] == pytest.approx(0.5)  # 7.5% of a 15% budget
    assert row["allowed"] == "0.15000000"


def test_a_gain_is_zero_usage_of_a_loss_limit_not_negative() -> None:
    engine = engine_with(
        RiskLimits(max_daily_loss_pct=money("0.02"), max_drawdown_pct=money("0.15"))
    )
    engine.observe_equity(1, money("10500"))
    rows = usage_by_limit(engine.usage(ts_ms=1, equity=money("10500"), **NO_EXPOSURE))
    assert rows["max_daily_loss"]["fraction"] == 0.0
    # At its own peak the drawdown is exactly zero, and the formatted spend never carries
    # a minus sign a bar cannot draw.
    assert rows["max_drawdown"]["fraction"] == 0.0
    assert not rows["max_daily_loss"]["used"].startswith("-")


def test_min_equity_reads_as_budget_spent_toward_the_floor() -> None:
    """The distance from start to floor is the budget; the fall so far is the spend.

    Reported as a level, the most dangerous limit on the page would sit at 100% on a
    healthy run and drain toward zero as the run died -- inverted against every other bar.
    """
    engine = engine_with(RiskLimits(min_equity_pct=money("0.50")))
    engine.observe_equity(1, money("8000"))  # fallen 2000 of the 5000 to the floor

    row = usage_by_limit(
        engine.usage(ts_ms=1, equity=money("8000"), **NO_EXPOSURE)
    )["min_equity"]
    assert row["fraction"] == pytest.approx(0.4)
    assert row["used"] == "2000.00000000"
    assert row["allowed"] == "5000.00000000"


def test_leverage_and_notional_read_from_the_caller_supplied_exposure() -> None:
    engine = engine_with(
        RiskLimits(max_leverage=money("5"), max_position_notional=money("50000"))
    )
    rows = usage_by_limit(
        engine.usage(
            ts_ms=1,
            equity=money("10000"),
            notional=money("30000"),
            max_symbol_notional=money("20000"),
            open_orders=0,
        )
    )
    assert rows["max_leverage"]["fraction"] == pytest.approx(0.6)  # 3x of 5x
    assert rows["max_position_notional"]["fraction"] == pytest.approx(0.4)


def test_the_rate_window_is_the_same_half_open_minute_the_check_uses() -> None:
    """A submission exactly 60 000 ms old has aged out; one at 59 999 has not."""
    engine = engine_with(RiskLimits(max_orders_per_minute=30))
    engine._note_submission(1_000)
    engine._note_submission(2_000)

    # At ts 60_999 the cutoff is 999: both stamps are inside the window.
    inside = usage_by_limit(
        engine.usage(ts_ms=60_999, equity=money("10000"), **NO_EXPOSURE)
    )["max_orders_per_minute"]
    assert inside["used"] == "2"

    # At ts 61_000 the cutoff is 1_000, and the 1_000 stamp sits ON it -- out, exactly
    # as `_check_rate` pops `<= cutoff`. An inclusive reading here would show one more
    # submission than the check is about to count.
    at_edge = usage_by_limit(
        engine.usage(ts_ms=61_000, equity=money("10000"), **NO_EXPOSURE)
    )["max_orders_per_minute"]
    assert at_edge["used"] == "1"


def test_reading_usage_never_mutates_the_rate_deque() -> None:
    """`_check_rate` prunes because it is deciding; a report must not write.

    Polling the monitor once a second would otherwise become part of the trading logic --
    a session watched in two browser tabs would prune twice as often as one watched in one.
    """
    engine = engine_with(RiskLimits(max_orders_per_minute=30))
    engine._note_submission(1_000)
    engine._note_submission(2_000)
    before = list(engine._submissions)
    engine.usage(ts_ms=200_000, equity=money("10000"), **NO_EXPOSURE)
    assert list(engine._submissions) == before


def test_streak_counters_surface_and_reset_the_way_the_checks_do() -> None:
    engine = engine_with(RiskLimits(max_consecutive_losses=4, max_consecutive_rejections=5))
    engine.observe_trade_closed(1, money("-10"))
    engine.observe_trade_closed(2, money("-10"))
    engine.observe_rejection(3, "PRICE_FILTER")

    rows = usage_by_limit(engine.usage(ts_ms=3, equity=money("10000"), **NO_EXPOSURE))
    assert rows["max_consecutive_losses"]["used"] == "2"
    assert rows["max_consecutive_rejections"]["used"] == "1"

    # A win and an acceptance zero the streaks -- consecutive means consecutive.
    engine.observe_trade_closed(4, money("5"))
    engine.observe_acceptance()
    rows = usage_by_limit(engine.usage(ts_ms=4, equity=money("10000"), **NO_EXPOSURE))
    assert rows["max_consecutive_losses"]["used"] == "0"
    assert rows["max_consecutive_rejections"]["used"] == "0"


def test_unlimited_produces_no_rows_and_switches_produce_none_either() -> None:
    """`None` means unlimited, and a switch is not a budget.

    `halt_on_liquidation` and `max_disconnect_seconds` have no fraction to draw --
    reporting them at 0% forever would be a bar that can never move, and the monitor
    already draws the disconnect clock in its connection line from fresher data.
    """
    empty = engine_with(RiskLimits.unlimited()).usage(
        ts_ms=1, equity=money("10000"), **NO_EXPOSURE
    )
    assert empty == []

    switches_only = engine_with(
        RiskLimits(  # every budget off, both switches on
            max_position_notional=None,
            max_leverage=None,
            max_daily_loss_pct=None,
            max_drawdown_pct=None,
            max_open_orders=None,
            max_orders_per_minute=None,
            max_consecutive_losses=None,
            halt_on_liquidation=True,
            min_equity_pct=None,
            max_consecutive_rejections=None,
            max_disconnect_seconds=30,
        )
    ).usage(ts_ms=1, equity=money("10000"), **NO_EXPOSURE)
    assert switches_only == []


def test_every_row_has_the_shape_the_monitor_draws() -> None:
    """The four keys `RiskUsageList` reads, with `fraction` a plain float in [0, ...).

    The UI multiplies `fraction` into a bar width; a Decimal that JSON-serialises to a
    string would render a bar of width "0.55%"px -- silently zero.
    """
    engine = engine_with(RiskLimits())  # the defaults: five budgets in force
    engine.observe_equity(1, money("9900"))
    rows = engine.usage(ts_ms=1, equity=money("9900"), **NO_EXPOSURE)
    assert rows, "the default limits must produce readings"
    for row in rows:
        assert set(row) == {"limit", "used", "allowed", "fraction"}
        assert isinstance(row["used"], str)
        assert isinstance(row["allowed"], str)
        assert isinstance(row["fraction"], float)
        assert row["fraction"] >= 0.0


# ------------------------------------------------------- through the engine and the wire


def test_the_monitor_snapshot_carries_usage_for_a_session_with_limits(
    tmp_path: Path,
) -> None:
    """The end-to-end regression: `monitor()["risk"]["usage"]` exists and is drawable.

    This is the exact read `LiveMonitor` performs at the exact depth it performs it. The
    defect was never in the arithmetic -- there was no arithmetic -- but in the field not
    existing at all, so the assertion that matters most here is the plain `in`.
    """
    from tests.unit.test_live_wiring import _quiet_session

    session = _quiet_session(tmp_path / "usage", mode="paper")
    snapshot = session.monitor()

    assert "usage" in snapshot["risk"], "the monitor's risk block must carry usage"
    usage = snapshot["risk"]["usage"]
    assert isinstance(usage, list)
    # `_quiet_session` runs `RiskLimits.unlimited()`, so the honest reading is an empty
    # list -- present-and-empty, which the panel renders as "nothing bounds this session",
    # as distinct from absent, which it renders as "this panel is unavailable".
    assert usage == []
    # And the summary the run record stores is unchanged: usage is spliced into the
    # monitor's copy only, never persisted. See `RiskEngine.usage` for why.
    assert "usage" not in session.engine.risk.summary()


def test_engine_risk_usage_values_exposure_the_way_its_checks_do(tmp_path: Path) -> None:
    """`BacktestEngine.risk_usage` measures with the helpers the checks measure with.

    Open a real position through the ledger, then confirm the leverage reading is the
    position's notional at the current mark over equity -- the same
    exposure-times-`_risk_price` product `check_order` is fed.
    """
    from tests.unit.test_live_wiring import _quiet_session

    session = _quiet_session(tmp_path / "exposure", mode="paper")
    engine = session.engine
    engine.risk.limits = RiskLimits(max_leverage=money("5"))

    from tests.unit.test_live_wiring import SYMBOL

    mark = money("50000")
    now = engine.runtime.now_ms
    engine.account.update_mark(now + 1, SYMBOL, mark)
    engine.account.apply_fill(now + 2, SYMBOL, money("0.1"), mark, is_maker=False)

    rows = usage_by_limit(engine.risk_usage())
    leverage = rows["max_leverage"]
    # 0.1 BTC x 50 000 = 5 000 notional over 1 000 000 equity-ish -> 0.005x of a 5x cap.
    # Equity moved by the taker fee, so pin the arithmetic rather than the digits:
    expected = float(
        (money("5000") / engine.account.equity) / money("5")
    )
    assert leverage["fraction"] == pytest.approx(expected, rel=1e-9)
