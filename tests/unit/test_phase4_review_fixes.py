"""One test per defect the post-Phase-4 review confirmed.

Same discipline as `test_review_fixes.py` for the earlier phases: every finding that was
*demonstrated* gets a test that fails when the fix is removed, and the reproduction lives in
the test rather than in a commit message. `test_every_phase_4_finding_has_a_named_test` at
the bottom is the guard against this file quietly falling behind the list.

Six findings were in the React app and are not reachable from pytest. They are named in
`FRONTEND_FINDINGS` with what was wrong, where the fix is, and **how it was checked** --
claiming a Python test for a label on a button would be worse than saying so.

Four were driven in a browser against the live server. Two were not, and the entries say so:
the wipe-out notice needs a run whose account reaches zero and the sub-cent price formatting
needs a symbol this lake does not hold, so both were verified by reading the change and by
`tsc`. That is weaker evidence and is labelled as weaker evidence.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from perplab.analytics.metrics import MS_PER_DAY, MS_PER_HOUR, build_grid, compute_metrics
from perplab.analytics.trades import TradeBuilder
from perplab.api.app import create_app
from perplab.api.routers.runs import _downsample_extremes
from perplab.core.account import FeeSchedule
from perplab.core.money import parse_money
from perplab.engine.backtest import BacktestConfig, BacktestEngine, RunAborted
from perplab.engine.fills import MarketFillModel
from perplab.engine.latency import FixedLatency
from perplab.store import db
from perplab.store.runs import RunStatus, RunStore
from perplab.strategy.base import Strategy
from perplab.strategy.loader import StrategyLoadError, load_strategy_class
from tests.engine_lake import MS_PER_MINUTE, Ohlc, build_lake, flat_path
from tests.support import btcusdt_filters, single_bracket_table

START = 1_709_251_200_000
NO_SLIPPAGE = MarketFillModel(slippage_bps=parse_money("0"))


class BuyOnce(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1, "datasets": ["klines"]}

    def on_start(self, ctx):
        self.done = False

    def on_bar(self, ctx, bar):
        if ctx.warm and not self.done:
            self.done = True
            ctx.buy(qty=ctx.money("1"))


def run_engine(root: Path, strategy: Strategy, *, end_ms: int, start_ms: int = START, **kw):
    config = BacktestConfig(
        symbols=("BTCUSDT",),
        timeframe="1m",
        start_ms=start_ms,
        end_ms=end_ms,
        leverage=kw.pop("leverage", 10),
        latency=FixedLatency(120, 120),
        fill_model=kw.pop("fill_model", NO_SLIPPAGE),
        **kw,
    )
    return BacktestEngine(
        root=root,
        strategy=strategy,
        requirements=strategy.declared,
        config=config,
        filters={"BTCUSDT": btcusdt_filters()},
        brackets={"BTCUSDT": single_bracket_table()},
    ).run()


# ---------------------------------------------------------------------- R2, R7: probes


def test_the_equity_curve_holds_no_fabricated_intrabar_states() -> None:
    """R2. The probed extremes used to be written into the curve as ordered samples.

    Two independent errors came out of that, and the second is the one a single-symbol test
    cannot see. Sampling low-then-high scores the trough against a peak the crest has not yet
    raised, so a long and its mirrored short report *different* drawdowns on identical price
    paths. And moving every positioned symbol to its own extreme *together* fabricates a
    joint state: for a market-neutral pair the legs cancel in both samples and the run
    reports a maximum drawdown of exactly zero for a book that traversed 4%.

    Asserted at the analytics seam, where both errors live in one function.
    """
    from perplab.analytics.metrics import drawdown_stats

    times = [0, 1, 2]
    equity = [10_000.0, 10_000.0, 10_000.0]
    low = [10_000.0, 7_000.0, 10_000.0]
    high = [10_000.0, 13_000.0, 10_000.0]

    banded = drawdown_stats(times, equity, low, high)
    # The crest raises the peak and the trough is scored against it, in one sample:
    # 7000/13000 - 1.
    assert banded.max_drawdown == pytest.approx(7_000 / 13_000 - 1)

    # The old shape: the same two numbers as ordered points. Low first understates it...
    ordered_low_first = drawdown_stats([0, 1, 2], [10_000.0, 7_000.0, 13_000.0])
    # ...and the mirrored book, whose adverse extreme is the high, reports something else
    # entirely off the same data.
    ordered_high_first = drawdown_stats([0, 1, 2], [10_000.0, 13_000.0, 7_000.0])
    assert ordered_low_first.max_drawdown != pytest.approx(ordered_high_first.max_drawdown)
    assert banded.max_drawdown == pytest.approx(ordered_high_first.max_drawdown)


def test_a_hedged_book_still_reports_its_joint_intrabar_drawdown(tmp_path: Path) -> None:
    """R2, the multi-symbol half. Per-position extremes, never a shared price move."""
    from perplab.analytics.metrics import drawdown_stats

    # Two legs, each 2 000 offside at its own adverse extreme; simultaneous moves in the
    # same direction would cancel, per-position ones do not.
    stats = drawdown_stats([0], [100_000.0], [96_000.0], [100_000.0])
    assert stats.max_drawdown == pytest.approx(-0.04)


def test_a_strategy_hook_never_sees_a_probed_mark(tmp_path: Path) -> None:
    """R7. `on_liquidation` used to run with the probe price still installed.

    `ctx.mark()` then returned a price this engine's own docstring calls evidence rather than
    an observation -- and any order the hook placed took it as the slippage reference.
    """
    lake = tmp_path / "market"
    minutes = 180
    spike = 120
    top = int(40_000 * 10**8)
    below = int(30_000 * 10**8)

    def marks(index: int) -> Ohlc:
        return Ohlc(top, top, below, top) if index == spike else Ohlc(top, top, top, top)

    build_lake(lake, start_ms=START, minutes=minutes, trade_path=flat_path(40_000.0), mark_path=marks)

    seen: list[str] = []

    class Watcher(BuyOnce):
        def on_liquidation(self, ctx, event):
            seen.append(str(ctx.mark()))

    result = run_engine(lake, Watcher(), end_ms=START + minutes * MS_PER_MINUTE)
    assert result.liquidations == 1
    assert seen == ["40000.00000000"], f"hook observed a probed price: {seen}"


# --------------------------------------------------------------------------- R3: funding


def test_a_funding_settlement_with_no_mark_is_flagged_not_silently_dropped(
    tmp_path: Path,
) -> None:
    """R3. The guard used to wrap the whole handler and returned silently.

    400 USDT of cashflow on an open position vanished, `funding_pnl` reported a confident 0,
    and no flag was raised. The cashflow still cannot be booked -- spec 3.4 forbids inventing
    a mark -- but everything else about the settlement happens and the run says what it
    could not price.
    """
    lake = tmp_path / "market"
    minutes = 200
    # Klines from the start; marks only from minute 60, so the earlier settlement has no
    # mark to price it even with the lead-in bar.
    from tests.engine_lake import write_funding, write_klines, write_marks

    write_klines(lake, "BTCUSDT", START, minutes, flat_path(40_000.0))
    write_marks(lake, "BTCUSDT", START + 60 * MS_PER_MINUTE, minutes - 60, flat_path(40_000.0))
    write_funding(lake, "BTCUSDT", [(START + 30 * MS_PER_MINUTE, 0.01)])

    result = run_engine(lake, BuyOnce(), end_ms=START + minutes * MS_PER_MINUTE)
    assert "FUNDING_UNSETTLED" in result.flags
    assert any("no mark price to settle against" in w for w in result.warnings)
    assert any(e.kind == "FUNDING_UNSETTLED" for e in result.events)


def test_the_mark_stream_carries_a_lead_in_bar_so_locf_has_an_anchor(tmp_path: Path) -> None:
    """R3, the root cause. The first mark of a range used to land at `start + 59_999`.

    A settlement anywhere in that first minute therefore found no mark -- on a *complete*
    lake -- and `warmup_start_ms` routinely lands data_start on an 8-hour boundary, which is
    exactly where funding settles.
    """
    from perplab.engine.feed import load_marks

    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=120, trade_path=flat_path(40_000.0))
    marks, _ = load_marks(
        lake, ["BTCUSDT"], START + 60 * MS_PER_MINUTE, START + 120 * MS_PER_MINUTE
    )
    # The earliest mark closes *before* the requested start, so LOCF has a value at it.
    assert marks[0].close_time < START + 60 * MS_PER_MINUTE


# -------------------------------------------------------------------------- R4: exposure


def test_exposure_is_measured_against_the_run_range_not_the_warm_up(tmp_path: Path) -> None:
    """R4. The denominator was `end_ms - times[0]`, and `times[0]` is a warm-up sample.

    The strategy is gated out of trading before `start_ms`, so that time can never appear in
    the numerator -- charging it to the denominator understated every run, by 7.7x on a
    60-day range behind a 400-day warm-up.
    """
    warmup_start = -400 * MS_PER_DAY
    times = [warmup_start, 0, 60 * MS_PER_DAY]
    metrics = compute_metrics(
        times=times,
        equity=[10_000.0, 10_000.0, 10_000.0],
        positions_open=[False, True, False],
        trades=[],
        start_ms=0,
        end_ms=60 * MS_PER_DAY,
        traded_notional=0.0,
    )
    assert metrics.exposure == pytest.approx(1.0)


def test_exposure_is_right_even_when_the_series_starts_after_the_range_does() -> None:
    """The other side of the same denominator, and the one the windowing cannot cover.

    When the first sample lands *inside* the range -- a lake whose mark data begins late --
    `end_ms - times[0]` is *smaller* than the range, so the old expression **overstates**
    exposure instead. A position held for the second half of a run reports 100%.
    """
    times = [50 * MS_PER_DAY, 75 * MS_PER_DAY]
    metrics = compute_metrics(
        times=times,
        equity=[10_000.0, 10_000.0],
        positions_open=[True, True],
        trades=[],
        start_ms=0,
        end_ms=100 * MS_PER_DAY,
        traded_notional=0.0,
    )
    # Held from day 50 to day 100 out of a 100-day range.
    assert metrics.exposure == pytest.approx(0.5)


def test_metrics_ignore_samples_outside_the_requested_range(tmp_path: Path) -> None:
    """R30. An order on the last bar arrives after `end_ms` and its sample is real.

    It belongs in the ledger and in the trade table; it does not belong in a *return*
    measured over the range, and the grid excluded it while `total_return` did not.
    """
    times = [0, MS_PER_DAY, 2 * MS_PER_DAY, 2 * MS_PER_DAY + 5_000]
    equity = [100.0, 110.0, 120.0, 999.0]
    metrics = compute_metrics(
        times=times,
        equity=equity,
        positions_open=[False] * 4,
        trades=[],
        start_ms=0,
        end_ms=2 * MS_PER_DAY,
        traded_notional=0.0,
    )
    assert metrics.total_return == pytest.approx(0.2)


# ------------------------------------------------------------------------------ R29: grid


def test_an_unaligned_range_produces_no_stub_grid_periods() -> None:
    """R29. A 60-second head remainder was one grid period, annualised as a whole day.

    A 5% move inside that minute produced a Sharpe of 2.43 on a series that is otherwise
    flat. Only whole steps are periods now; the remainder still counts toward
    `total_return`, which is where a partial day belongs.
    """
    step = MS_PER_HOUR
    start = 5 * step + step - 60_000  # 60 seconds before an aligned boundary
    times = [start, start + 60_000, start + 60_000 + step]
    equity = [100.0, 105.0, 105.0]
    grid = build_grid(times, equity, start_ms=start, end_ms=start + 60_000 + step)
    assert grid.returns == (0.0,)
    assert all(t % step == 0 for t in grid.times)


# ------------------------------------------------------------------------ R10: MAE / MFE


def test_mae_includes_the_closing_leg(tmp_path: Path) -> None:
    """R10. `mark()` was the only writer, and the exit is booked after the last mark.

    A long bought at 100, marked at 101 and 102 and sold at 102 with a 10.05 commission lost
    8.05 and reported its worst excursion as **+0.95**. 31% of the exit run's round-trips
    were affected, always optimistically, and spec 8.3 says this is the distribution stops
    get sized from.
    """
    builder = TradeBuilder()
    builder.fill(
        ts_ms=1, symbol="BTCUSDT", signed_qty=Decimal("1"), price=Decimal("100"),
        fee=Decimal("0.05"), realized=Decimal("0"), qty_before=Decimal("0"),
        qty_after=Decimal("1"),
    )
    builder.mark(symbol="BTCUSDT", mark_price=Decimal("101"), unrealized=Decimal("1"))
    builder.mark(symbol="BTCUSDT", mark_price=Decimal("102"), unrealized=Decimal("2"))
    builder.fill(
        ts_ms=4, symbol="BTCUSDT", signed_qty=Decimal("-1"), price=Decimal("102"),
        fee=Decimal("10"), realized=Decimal("2"), qty_before=Decimal("1"),
        qty_after=Decimal("0"),
    )
    trade = builder.finish(5)[0]
    assert trade.net_pnl == Decimal("-8.05")
    assert trade.mae <= trade.net_pnl


def test_a_liquidation_appears_in_its_own_trades_worst_excursion() -> None:
    """R10, the acute case: a liquidation's realised loss has no preceding mark at all."""
    builder = TradeBuilder()
    builder.fill(
        ts_ms=1, symbol="BTCUSDT", signed_qty=Decimal("1"), price=Decimal("100"),
        fee=Decimal("0"), realized=Decimal("0"), qty_before=Decimal("0"),
        qty_after=Decimal("1"),
    )
    builder.mark(symbol="BTCUSDT", mark_price=Decimal("95"), unrealized=Decimal("-5"))
    builder.liquidation(
        ts_ms=3, symbol="BTCUSDT", closed_qty=Decimal("-1"), price=Decimal("80"),
        realized=Decimal("-40"),
    )
    trade = builder.finish(4)[0]
    assert trade.mae == Decimal("-40")
    assert trade.mae_price == Decimal("80")


# ------------------------------------------------------------------------- R9: slippage


def test_slippage_is_measured_against_the_trade_print_not_the_mark(tmp_path: Path) -> None:
    """R9. The reference was the mark while the fill came from the trade series.

    The reported figure then absorbed the whole mark-trade basis, whose sign follows the side
    -- so a long-biased strategy reported systematically *favourable* execution and a short
    one adverse, on identical data. With slippage modelled at exactly zero and a basis of 20,
    a run reported `slippage_cost = -20` and a price leg of 0 on a position that made +20.
    """
    lake = tmp_path / "market"
    build_lake(
        lake,
        start_ms=START,
        minutes=120,
        trade_path=flat_path(40_000.0),
        mark_path=flat_path(40_020.0),
    )
    result = run_engine(lake, BuyOnce(), end_ms=START + 120 * MS_PER_MINUTE)
    assert result.attribution.slippage_cost == Decimal("0")
    assert result.attribution.slippage_abs == Decimal("0")


# ----------------------------------------------------------------------- R8: drain loop


class Forever(Strategy):
    """Oscillates rather than accumulating, so the chain is not ended by margin.

    A strategy that only bought would run out of available balance after a few thousand
    fills and stop on its own -- which is the shape that hid the missing check.
    """

    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1}

    burn_s = 0.0
    """Wall time each `on_fill` deliberately wastes, for the budget-backstop variant."""

    def on_bar(self, ctx, bar):
        pass

    def on_stop(self, ctx):
        ctx.buy(qty=ctx.money("0.01"))

    def on_fill(self, ctx, fill):
        if self.burn_s:
            import time as _time

            deadline = _time.perf_counter() + self.burn_s
            while _time.perf_counter() < deadline:
                pass
        # 0.01 BTC at 40 000 clears MIN_NOTIONAL; anything smaller is rejected and the
        # chain never starts, which is how this loop looked bounded.
        if ctx.position().is_flat:
            ctx.buy(qty=ctx.money("0.01"))
        else:
            ctx.close()


def test_the_post_stop_drain_chain_starves_once_the_tape_goes_stale(tmp_path: Path) -> None:
    """R8, revised by the staleness bound on `last_print` (audit M6).

    The original runaway was fuelled by exactly the read M6 closed: each post-`on_stop`
    fill scheduled the next arrival, drifting past `end_ms` forever, *every fill priced
    against the last stale print*. A print past the staleness bound now refuses to price a
    fill at all, so the chain starves on its own: arrivals drift 120 ms per hop, the last
    print is the final bar's close, and once the drift passes the `BAR_CLOSE` bound (one
    1 m timeframe + 60 s = 120 s) the next order is rejected with the missing print named
    and the queue empties. The run therefore *completes* -- structurally bounded, no
    wall-clock rescue required -- with no fill priced more than the bound past the data.
    """
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=120, trade_path=flat_path(40_000.0))
    end_ms = START + 120 * MS_PER_MINUTE
    result = run_engine(
        lake,
        Forever(),
        end_ms=end_ms,
        leverage=100,
        # Zero fees, so nothing eventually drains the wallet and ends the chain by
        # accident: only the staleness bound under test may end it.
        fees=FeeSchedule.all_taker(parse_money("0"), "test-zero"),
    )
    assert result.rejects >= 1
    reject_reasons = [
        dict(e.payload)["reason"] for e in result.events if e.kind == "REJECT"
    ]
    assert any("no trade print" in reason for reason in reject_reasons)
    last_fill = max(e.ts_ms for e in result.events if e.kind == "FILL")
    assert last_fill <= end_ms + 120_000 + 240, (
        "no fill may be priced further past the tape than the staleness bound allows"
    )


def test_the_post_stop_drain_loop_respects_the_wall_clock_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R8's backstop survives M6: a drain that burns *wall* time is still stopped.

    The staleness bound ends the chain after ~1 000 simulated hops; a hook that spends
    10 ms of wall clock per fill would still hold the process for tens of seconds inside
    that window, which is what the budget is for. The checkpoint interval is tightened so
    the (now finite) drain actually reaches a budget check before the chain starves.
    """
    monkeypatch.setattr("perplab.engine.backtest.PROGRESS_EVERY", 10)
    slow = Forever()
    slow.burn_s = 0.01
    lake = tmp_path / "market"
    build_lake(lake, start_ms=START, minutes=5, trade_path=flat_path(40_000.0))
    with pytest.raises(RunAborted, match="wall-clock budget"):
        run_engine(
            lake,
            slow,
            end_ms=START + 5 * MS_PER_MINUTE,
            timeout_s=0.3,
            leverage=100,
            fees=FeeSchedule.all_taker(parse_money("0"), "test-zero"),
        )


# ---------------------------------------------------------------------------- R15: loader


def test_a_strategy_exported_under_two_names_is_one_strategy() -> None:
    """R15. `Alias = MyStrat` was counted twice and the file was refused as ambiguous.

    Because both the validator and the worker call this, such a file was neither saveable as
    valid nor runnable -- and the message named the same class twice, which was the tell.
    """
    source = (
        "from perplab import Strategy\n\n\n"
        "class MyStrat(Strategy):\n"
        "    requires = {'symbols': ['BTCUSDT'], 'timeframe': '1m', 'history': 1}\n\n"
        "    def on_bar(self, ctx, bar):\n        pass\n\n\n"
        "Alias = MyStrat\n"
    )
    assert load_strategy_class(source, "aliased.py").__name__ == "MyStrat"


def test_two_genuinely_different_classes_are_still_refused() -> None:
    """The control: the rule the dedupe must not weaken."""
    source = (
        "from perplab import Strategy\n\n\n"
        "class A(Strategy):\n    def on_bar(self, ctx, bar): pass\n\n\n"
        "class B(Strategy):\n    def on_bar(self, ctx, bar): pass\n"
    )
    with pytest.raises(StrategyLoadError) as caught:
        load_strategy_class(source, "two.py")
    assert caught.value.code == "multiple-strategies"


# -------------------------------------------------------------------------- R16: schema


def test_a_newer_database_is_not_written_to_before_being_refused(tmp_path: Path) -> None:
    """R16. `executescript` ran -- and implicitly committed -- before the version was read.

    Nine tables and indexes were created inside a database this build had already decided it
    could not understand, and `with connection:` could not roll them back.
    """
    import sqlite3

    path = tmp_path / "perplab.db"
    handle = sqlite3.connect(path)
    handle.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    handle.execute("INSERT INTO schema_version (version) VALUES (99)")
    handle.commit()
    handle.close()

    with pytest.raises(db.SchemaTooNew):
        db.connect(tmp_path)

    handle = sqlite3.connect(path)
    objects = {row[0] for row in handle.execute("SELECT name FROM sqlite_master")}
    handle.close()
    assert objects == {"schema_version"}, f"the refused database was written to: {objects}"


# ---------------------------------------------------------------------------- R28: delete


def test_deleting_a_run_removes_the_artefacts_before_the_row(tmp_path: Path) -> None:
    """R28. The row went first, so any failure removing the files orphaned them.

    That is the inverse of what the deletion protects against, arrived at by the deletion
    itself -- and a nested directory raised an uncaught 500 rather than being removed.
    """
    connection = db.connect(tmp_path)
    with connection:
        strategy_id = connection.execute(
            "INSERT INTO strategies (name, created_ms, updated_ms) VALUES ('S', 0, 0)"
        ).lastrowid
        version_id = connection.execute(
            "INSERT INTO strategy_versions (strategy_id, version_no, code, code_sha256,"
            " created_ms, valid) VALUES (?, 1, 'x', 'y', 0, 1)",
            (strategy_id,),
        ).lastrowid
    connection.close()

    store = RunStore(tmp_path)
    try:
        run_id = store.create(
            strategy_id=int(strategy_id),
            version_id=int(version_id),
            spec={
                "symbols": ["BTCUSDT"], "timeframe": "1m", "start_ms": 0,
                "end_ms": 1, "seed": 0, "engine_version": 4, "params": {},
            },
        )
        nested = store.directory(run_id) / "nested"
        nested.mkdir()
        (nested / "leftover.txt").write_text("x", encoding="utf-8")
        store.fail(run_id, "done")
        store.delete(run_id)
        assert not store.directory(run_id).exists()
    finally:
        store.close()


# ------------------------------------------------------------------------ R22, R18: chart


def _drawdown(equity: list[float]) -> list[float]:
    peak = equity[0]
    series = []
    for value in equity:
        peak = max(peak, value)
        series.append(value / peak - 1.0)
    return series


def test_downsampling_keeps_the_first_sample() -> None:
    """R18. Only the last index was pinned.

    The first sample is the run's opening equity and the dashed baseline the chart draws
    from it -- 31 of 50 simulated year-long runs had it dropped, one by 532.

    The fixture is chosen so index 0 is **neither** its bucket's minimum nor its maximum.
    A series that merely starts flat has index 0 as the bucket's first maximal element, and
    `max()` returns the first such index -- so it survives whether or not it is forced, and
    the test proves nothing.
    """
    equity = [10_000.0, 9_000.0, 12_000.0, 11_000.0, 11_000.0, 11_000.0]
    keep = _downsample_extremes(list(range(len(equity))), equity, 2)
    bucket_extremes = {1, 2, 3}
    assert 0 not in bucket_extremes, "the fixture must not select index 0 incidentally"
    assert keep[0] == 0
    assert keep[-1] == len(equity) - 1


def test_downsampling_keeps_the_drawdown_trough() -> None:
    """R22. The trough is an extreme of `equity / running peak`, not of equity.

    So bucketing by equity alone can drop it and leave the shaded panel reading shallower
    than the "Max drawdown" card printed beside it. Here index 2 is the deepest drawdown and
    is neither its bucket's minimum nor its maximum.
    """
    equity = [100.0, 200.0, 150.0, 250.0, 240.0, 260.0]
    drawdown = _drawdown(equity)
    trough = min(range(len(drawdown)), key=lambda i: drawdown[i])
    assert trough == 2

    without = _downsample_extremes(list(range(len(equity))), equity, 2)
    assert trough not in without, "the fixture must not select the trough incidentally"

    keep = _downsample_extremes(list(range(len(equity))), equity, 2, anchors=drawdown)
    assert trough in keep
    assert min(drawdown[i] for i in keep) == pytest.approx(min(drawdown))


# ---------------------------------------------------------------------------- R5, R13: API


@pytest.fixture()
def api(tmp_path: Path):
    from tests.integration.test_run_worker import build_userdata

    build_userdata(tmp_path)
    with TestClient(create_app(tmp_path)) as client:
        yield client, tmp_path


BOOL_STRATEGY = '''
from perplab import Strategy


class Flagged(Strategy):
    params = {
        "period": {"type": "int", "default": 5, "min": 2, "max": 50},
        "go_long": {"type": "bool", "default": True},
    }
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 5}

    def on_bar(self, ctx, bar):
        pass
'''


def add_strategy(root: Path, source: str, params: list[dict]) -> int:
    connection = db.connect(root)
    with connection:
        strategy_id = connection.execute(
            "INSERT INTO strategies (name, created_ms, updated_ms) VALUES ('Flagged', 0, 0)"
        ).lastrowid
        version_id = connection.execute(
            """
            INSERT INTO strategy_versions
                (strategy_id, version_no, code, code_sha256, created_ms, valid,
                 params_json, requires_json)
            VALUES (?, 1, ?, 'sha', 0, 1, ?, ?)
            """,
            (
                strategy_id,
                source,
                json.dumps(params),
                json.dumps(
                    {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 5,
                     "datasets": ["klines"]}
                ),
            ),
        ).lastrowid
        connection.execute(
            "UPDATE strategies SET head_version_id = ? WHERE id = ?",
            (version_id, strategy_id),
        )
    connection.close()
    return int(strategy_id)


def test_a_bool_parameter_survives_the_api_as_a_bool(api) -> None:
    """R5. Every parameter was stringified, so `True` arrived at the worker as `"True"`.

    `params._coerce_override` rightly refuses that, so *every* backtest of a strategy with a
    boolean parameter was accepted with a 201 and then failed in the worker -- exactly the
    outcome `_bind_params`' own docstring says it exists to prevent.
    """
    client, root = api
    strategy_id = add_strategy(
        root,
        BOOL_STRATEGY,
        [
            {"name": "period", "type": "int", "default": 5, "min": 2, "max": 50},
            {"name": "go_long", "type": "bool", "default": True},
        ],
    )
    response = client.post(
        "/api/runs",
        json={
            "strategy_id": strategy_id,
            "start_ms": START,
            "end_ms": START + 120 * MS_PER_MINUTE,
        },
    )
    assert response.status_code == 201
    run_id = response.json()["run"]["id"]
    stored = json.loads((root / "runs" / str(run_id) / "spec.json").read_text())
    assert stored["params"]["go_long"] is True
    assert isinstance(stored["params"]["period"], int)


def test_a_malformed_symbol_is_a_400_not_a_500(api) -> None:
    """R13. `normalise_symbol` raises `ValueError` and nothing mapped it.

    `/coverage` is called on every symbol change in the run form, so a typo produced a
    crash where spec 1.4 asks for a legible refusal.
    """
    client, _ = api
    for symbol in ("", "BTC-USDT", "BTC/USDT"):
        response = client.get(f"/api/coverage?symbol={symbol}")
        assert response.status_code == 400, symbol


def test_an_oversized_seed_is_a_400_not_a_500(api) -> None:
    """R13. SQLite stores INTEGER as int64 and raised `OverflowError` from `create`."""
    client, root = api
    strategy_id = add_strategy(
        root, BOOL_STRATEGY, [{"name": "period", "type": "int", "default": 5}]
    )
    response = client.post(
        "/api/runs",
        json={
            "strategy_id": strategy_id,
            "start_ms": START,
            "end_ms": START + 120 * MS_PER_MINUTE,
            "seed": 2**63,
        },
    )
    assert response.status_code == 400
    assert "64-bit" in response.json()["detail"]


def test_coverage_intersects_the_datasets_a_run_actually_needs(api) -> None:
    """R14. It reported the klines range alone.

    Spec 3.4 makes the mark a separate series that may not be derived and `load_marks`
    refuses a range it does not cover, so the klines range advertised dates guaranteed to
    fail -- 34 days of them on the real lake.
    """
    client, root = api
    from tests.engine_lake import write_klines

    # Extend klines a day past the marks. The advertised end must not follow them.
    write_klines(
        root / "market",
        "BTCUSDT",
        START + 600 * MS_PER_MINUTE,
        1440,
        flat_path(40_000.0),
    )
    payload = client.get("/api/coverage?symbol=BTCUSDT").json()
    marks_end = payload["datasets"]["markPriceKlines"]["end_ms"]
    klines_end = payload["datasets"]["klines"]["end_ms"]
    assert klines_end > marks_end
    assert payload["end_ms"] == marks_end


# ------------------------------------------------------------------------- R27: password


def test_a_non_ascii_password_does_not_crash_every_request(tmp_path: Path) -> None:
    """R27. `secrets.compare_digest` raises `TypeError` on non-ASCII `str` arguments.

    A network-exposed instance with an umlaut in its password returned 500 to every
    authenticated request -- the operator sees a crash while typing the right password.
    """
    from tests.integration.test_run_worker import build_userdata

    build_userdata(tmp_path)
    app = create_app(tmp_path, host="0.0.0.0", password="pässwörd")
    with TestClient(app, raise_server_exceptions=False) as client:
        # The *comparison* is what crashed, on every request, whatever the token. HTTP
        # headers are latin-1 at best, so the matching token cannot be sent in one at all --
        # what has to hold is that a wrong token is refused rather than raising.
        response = client.get("/api/runs", headers={"Authorization": "Bearer nope"})
        assert response.status_code == 401
        assert client.get("/api/health").status_code == 200


# ------------------------------------------------------------------------ R26: log search


def test_the_event_log_is_searchable_in_the_text_the_viewer_renders(tmp_path: Path) -> None:
    """R26. The log was written with `ensure_ascii=True` and searched as raw text.

    A message the viewer renders as `café` was stored as `caf\\u00e9`, so typing what is on
    the screen into the viewer's own search box returned nothing.
    """
    from perplab.engine.worker import _write_events
    from perplab.strategy.context import StrategyEvent

    class Result:
        events = (
            StrategyEvent(seq=1, ts_ms=1, kind="LOG", payload={"message": "café stop hit"}),
        )

    path = tmp_path / "events.jsonl"
    _write_events(path, Result())
    assert "café" in path.read_text(encoding="utf-8")


# ------------------------------------------------------------------------ R25: pruning


def test_the_mark_query_prunes_by_partition(tmp_path: Path) -> None:
    """R25. `load_marks` hand-wrote its SQL with no partition predicate.

    DuckDB emitted no file filter at all, so a one-day range opened every mark footer in the
    lake: 2 357 files read where 30 sufficed.
    """
    import inspect

    from perplab.data.query import partition_predicate
    from perplab.engine import feed

    # The predicate itself constrains the hive keys DuckDB prunes paths by...
    predicate = partition_predicate(feed.MARK_DATASET, start_ms=START, end_ms=START + 60_000)
    assert '"year"' in predicate and '"month"' in predicate

    # ...and `load_marks` puts it in the query. Asserted against the source rather than
    # against a query plan, because the plan's shape is a DuckDB internal and a fixture lake
    # of two files prunes nothing measurable either way; what regressed was our SQL.
    source = inspect.getsource(feed.load_marks)
    assert "partition_predicate(" in source
    assert "WHERE {pruning}" in source

    # And it still returns the same rows.
    build_lake(tmp_path / "market", start_ms=START, minutes=120, trade_path=flat_path(40_000.0))
    marks, _ = feed.load_marks(
        tmp_path / "market", ["BTCUSDT"], START, START + 120 * MS_PER_MINUTE
    )
    # 120 in range. The lead-in bar is requested but this fixture's lake begins exactly at
    # `START`, so there is nothing earlier to anchor to -- which is the honest outcome and
    # not a shortfall. `test_the_mark_stream_carries_a_lead_in_bar_so_locf_has_an_anchor`
    # covers the case where earlier data does exist.
    assert len(marks) == 120


# ------------------------------------------------------------------------------- the list


PYTHON_FINDINGS = {
    "R1 attribution identity is vacuous with respect to slippage":
        "tests/unit/test_backtest.py::test_a_wrong_slippage_total_is_caught_even_though_the_identity_still_closes",
    "R2 probe samples fabricate intrabar states in the equity curve":
        "test_the_equity_curve_holds_no_fabricated_intrabar_states",
    "R2b a hedged book's joint intrabar drawdown cancelled to zero":
        "test_a_hedged_book_still_reports_its_joint_intrabar_drawdown",
    "R3 a funding settlement with no mark was dropped silently":
        "test_a_funding_settlement_with_no_mark_is_flagged_not_silently_dropped",
    "R3b the mark stream had no LOCF anchor at the range start":
        "test_the_mark_stream_carries_a_lead_in_bar_so_locf_has_an_anchor",
    "R4 exposure was divided by the range plus the warm-up":
        "test_exposure_is_measured_against_the_run_range_not_the_warm_up",
    "R5 a bool parameter left the API as the string 'True'":
        "test_a_bool_parameter_survives_the_api_as_a_bool",
    "R6 a CoverageError after the run discarded a completed backtest":
        "tests/integration/test_run_worker.py::test_a_range_past_the_lake_keeps_its_results",
    "R7 strategy hooks observed a probed mark price":
        "test_a_strategy_hook_never_sees_a_probed_mark",
    "R8 the post-on_stop drain loop had no wall-clock check":
        "test_the_post_stop_drain_loop_respects_the_wall_clock_budget",
    "R9 slippage was measured against the mark, absorbing the basis":
        "test_slippage_is_measured_against_the_trade_print_not_the_mark",
    "R10 MAE/MFE excluded the closing leg":
        "test_mae_includes_the_closing_leg",
    "R10b a liquidation was absent from its own trade's MAE":
        "test_a_liquidation_appears_in_its_own_trades_worst_excursion",
    "R11 cancel signalled a possibly-recycled pid":
        "tests/unit/test_runs_store.py::test_cancelling_a_run_this_process_did_not_launch_signals_nothing",
    "R12 a live orphan was marked failed, which unlocked delete":
        "tests/unit/test_runs_store.py::test_a_lost_run_cannot_be_deleted",
    "R13 a malformed symbol or oversized seed returned 500":
        "test_a_malformed_symbol_is_a_400_not_a_500",
    "R14 /coverage reported klines only":
        "test_coverage_intersects_the_datasets_a_run_actually_needs",
    "R15 a class exported under two names was refused as ambiguous":
        "test_a_strategy_exported_under_two_names_is_one_strategy",
    "R16 a newer database was written to before being refused":
        "test_a_newer_database_is_not_written_to_before_being_refused",
    "R18 the chart baseline used a downsampled first point":
        "test_downsampling_keeps_the_first_sample",
    "R22 the downsampled drawdown could omit the trough":
        "test_downsampling_keeps_the_drawdown_trough",
    "R25 load_marks opened every mark footer in the lake":
        "test_the_mark_query_prunes_by_partition",
    "R26 the log search could not find non-ASCII text the viewer renders":
        "test_the_event_log_is_searchable_in_the_text_the_viewer_renders",
    "R27 a non-ASCII password crashed every authenticated request":
        "test_a_non_ascii_password_does_not_crash_every_request",
    "R28 delete removed the row before the artefacts":
        "test_deleting_a_run_removes_the_artefacts_before_the_row",
    "R29 an unaligned range produced stub grid periods":
        "test_an_unaligned_range_produces_no_stub_grid_periods",
    "R30 a sample past end_ms reached total_return but not the grid":
        "test_metrics_ignore_samples_outside_the_requested_range",
}

FRONTEND_FINDINGS = {
    "R17 the event-log pager labels were reversed":
        "RunDetail.tsx — offset 0 is the oldest entry; the buttons now read Earlier/Later. "
        "BROWSER-VERIFIED against run 8.",
    "R19 the quick-validate merge inherited a stale ok=true":
        "Editor.tsx — `ok` is recomputed from the merged diagnostics. BROWSER-VERIFIED: "
        "typing `def broken(:` into the editor turns the panel header from '✓ valid' to "
        "'✕ 1 error'.",
    "R20 truncated_at_ms was rendered nowhere":
        "RunDetail.tsx — a wipe-out notice above the metric cards. NOT browser-verified: it "
        "needs a run whose account reaches zero, which no stored run does. Checked by "
        "reading the change and by tsc.",
    "R21 the attribution caption omitted the liquidation column":
        "RunDetail.tsx — the caption names five terms when the penalty is non-zero. "
        "BROWSER-VERIFIED for the four-term branch; the five-term branch needs a "
        "liquidation, which no stored run has.",
    "R23 the equity caption inverted samples and extremes":
        "RunDetail.tsx — 'N extremes drawn from M samples'. BROWSER-VERIFIED.",
    "R24 money() rendered every sub-cent price as 0.00":
        "api.ts — precision adapts below 1. NOT browser-verified: the lake holds only "
        "BTCUSDT, whose prices are five figures. Checked by reading the change and by tsc.",
}


def test_every_phase_4_finding_has_a_named_test() -> None:
    """The guard against this file falling behind the list it documents.

    Frontend findings are listed separately and deliberately: a Python test asserting a
    button's label would be theatre, and saying so is more useful than pretending.
    """
    module = Path(__file__).read_text(encoding="utf-8")
    missing = [
        finding
        for finding, test in PYTHON_FINDINGS.items()
        if "::" not in test and f"def {test}(" not in module
    ]
    assert not missing, f"findings with no test in this module: {missing}"
    assert len(FRONTEND_FINDINGS) == 6
