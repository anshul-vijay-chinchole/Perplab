"""The shadow backtest: replaying a session's own recording (spec 6.7.1).

Spec 6.7.1 asks for a backtest re-run "over that window with the same strategy version, seed,
and params", and a parity report attached to it. The report is only a measurement of the
*fill model* if everything else was held identical, so what this file pins is the holding:

- `shadow_spec` carries the session's inputs across untouched and changes exactly the three
  things being tested -- where the data comes from, what kind of run this is, and the range
  the session actually observed. The latency model is emphatically *not* one of them: it is
  re-run rather than re-sampled, because a re-sample perturbs the shared latency RNG and the
  report cannot tell that apart from the fill-model divergence it is measuring.
- the fill tier is read off the tape and never re-resolved. `tiers.resolve_tier` judges
  coverage from the lake, the lake lags roughly a day, so a re-resolved shadow demotes to
  `BAR_CLOSE` and a `BOOK_WALK` session is then compared against a bar-close replay. That
  divergence lands in the report as fill-model divergence, which is the single most likely
  way this whole mechanism gets quietly ruined.
- a shadow is not a trial. Spec 8.5's `N` counts parameter combinations *evaluated*, and a
  replay of an evaluation is not a second evaluation.

`tests/unit/test_tape.py` covers the recording itself -- the round trip, the crash
tolerance, the refusals. Nothing here repeats it: tapes are built with `TapeWriter` and read
back only through the things under test.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from perplab.core.types import Bar, DepthSnapshot
from perplab.engine import ENGINE_VERSION
from perplab.engine.clock import Event, EventKind
from perplab.engine.feed import BarStep, MarkBar
from perplab.engine.fills import fill_model_for_tier
from perplab.engine.runspec import SPEC_VERSION, RunSpec
from perplab.engine.source import TapeNotSealed, TapeSource
from perplab.engine.tape import TAPE_SOURCE_PREFIX, TapeReader, TapeWriter
from perplab.engine.ticks import TradePrint
from perplab.engine.worker import execute_run
from perplab.live.shadow import ShadowError, create_shadow, shadow_spec
from perplab.store import db
from perplab.store.runs import RunStatus, RunStore
from tests.support import BTCUSDT_PAYLOAD

SCALE = 10**8
"""Market data is scaled int64 everywhere below the accounting seam."""

MS_PER_MINUTE = 60_000
START = 1_709_251_200_000
"""2024-03-01T00:00:00Z, on a minute boundary so every bar close lands at `+59_999`."""

MINUTES = 5
"""Minutes of session the worker fixture's tape holds. Small on purpose: the properties
under test are about which *inputs* the replay uses, not about how many rows it can chew."""

PAPER_TIER = "BOOK_WALK"
"""What the session executed at. Deliberately the top tier, because it is the one a
re-resolution against a lake that does not hold this window yet would demote away from."""

BRACKETS = """
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
"""Written as text so no Python float ever exists in the fixture -- `Decimal(0.004)` is
`0.004000000000000000083...`, and that value multiplies a notional inside every liquidation
solve. Same reasoning as `tests/integration/test_run_worker.py`."""

STRATEGY_SOURCE = '''
from perplab import Strategy


class Ladder(Strategy):
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 2,
        # depth20 is declared on purpose. `BacktestEngine._check_declarations` refuses a
        # depth20 declaration below BOOK_WALK, so a shadow whose tier had been re-derived
        # from an empty lake could not even start -- which makes the tier's provenance
        # visible in this run rather than only in its numbers.
        "datasets": ["klines", "depth20"],
    }

    def on_start(self, ctx):
        self.bought = False

    def on_bar(self, ctx, bar):
        if not ctx.warm or self.bought:
            return
        self.bought = True
        ctx.buy(qty=ctx.money("0.01"))
'''


# ------------------------------------------------------------------------------- fixtures


def build_userdata(root: Path, *, snapshot_date: str = "2024-01-01") -> None:
    """Reference snapshots and an empty database -- everything a tape replay reads.

    Follows `tests/integration/test_run_worker.py`'s fixture with one deliberate omission:
    **there is no Parquet lake here.** A run whose `source` is `tape:<id>` opens none -- the
    tier is not re-resolved, the dataset manifest is not built, and the market data comes out
    of `tape/market.jsonl`. Building a lake anyway would let a replay that quietly fell back
    to one still pass.
    """
    exchange = root / "reference" / "exchangeInfo"
    exchange.mkdir(parents=True, exist_ok=True)
    (exchange / f"{snapshot_date}.json").write_text(
        json.dumps({"symbols": [BTCUSDT_PAYLOAD]}), encoding="utf-8"
    )
    brackets = root / "reference" / "leverageBracket"
    brackets.mkdir(parents=True, exist_ok=True)
    (brackets / f"{snapshot_date}.json").write_text(BRACKETS, encoding="utf-8")
    db.connect(root).close()


def seed_strategy(root: Path, source: str, name: str = "Ladder") -> tuple[int, int]:
    """Insert a strategy and one version, so a run row's foreign keys resolve."""
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


def paper_spec(strategy_id: int = 7, version_id: int = 13, **overrides: Any) -> RunSpec:
    """A finished paper session's spec, with a distinct value in every field.

    Distinct values matter for the field-by-field comparison: a shadow that dropped a field
    to its default would still equal the session's spec if the session's value happened to be
    the default too.
    """
    fields: dict[str, Any] = {
        "strategy_id": strategy_id,
        "version_id": version_id,
        "version_no": 3,
        "strategy_name": "Ladder",
        "code": STRATEGY_SOURCE,
        "class_name": "Ladder",
        "params": {"legs": 2, "threshold": "0.5"},
        "symbols": ("BTCUSDT",),
        "timeframe": "1m",
        "start_ms": START,
        "end_ms": START + 480 * MS_PER_MINUTE,
        "seed": 90_210,
        "opening_balance": "12500.75",
        "leverage": 7,
        "maker_rate": "0.00016",
        "taker_rate": "0.00040",
        "fee_source": "commissionRate:2026-08-01",
        "latency": {"model": "fixed", "submit_ms": 250, "cancel_ms": 310},
        "fill_tier": PAPER_TIER,
        "fill_model": fill_model_for_tier(PAPER_TIER).to_json(),
        "liquidation_recovery_pct": "0.005",
        "timeout_s": 3_600.0,
        "engine_version": ENGINE_VERSION,
        "risk_limits": {"max_leverage": "5", "max_drawdown_pct": "15"},
        "auto_flatten": {"max_hold_ms": 3_600_000, "before_funding_ms": 300_000},
        "kill_switch_flatten": True,
        "source": "",
        "endpoint": "testnet",
        "reorder_buffer_ms": 250,
        "session_kind": "paper",
    }
    fields.update(overrides)
    return RunSpec(**fields)


def open_meta(**overrides: Any) -> dict[str, Any]:
    """The `meta.json` a paper session hands `TapeWriter` before its first row.

    Shaped like `live.session.Session`'s: the endpoint, the reorder window, the executed fill
    tier and the data start are written at open so that even a crashed session leaves a tape
    something can interpret.
    """
    meta: dict[str, Any] = {
        "run_id": 41,
        "mode": "paper",
        "endpoint": "testnet",
        "reorder_buffer_ms": 250,
        "fill_tier": PAPER_TIER,
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "data_start_ms": START,
    }
    meta.update(overrides)
    return meta


def sealed_meta(**overrides: Any) -> dict[str, Any]:
    """What `TapeReader.meta()` returns for a session that stopped cleanly.

    `Session.seal` adds the end instant, the funding schedule it observed and its tallies, so
    a reader sees the open-time block plus those. Built here rather than by writing a tape,
    because `shadow_spec` takes the metadata itself and nothing else.
    """
    meta = open_meta()
    meta.update(
        {
            "ended_ms": START + MINUTES * MS_PER_MINUTE,
            "funding_times": {},
            "counts": seal_counts(4 * MINUTES, bar_closes=MINUTES),
            "sealed": True,
        }
    )
    meta.update(overrides)
    return meta


def seal_counts(released: int, *, late_dropped: int = 0, bar_closes: int = 0) -> dict[str, Any]:
    """`ReorderBuffer.stats()`-shaped tallies, as `Session.seal` merges them."""
    return {
        "window_ms": 250,
        "offered": released + late_dropped,
        "released": released,
        "late_dropped": late_dropped,
        "still_held": 0,
        "max_lag_ms": 0,
        "bar_closes": bar_closes,
    }


def session_events() -> list[Event]:
    """`MINUTES` minutes of a BOOK_WALK session, already in spec 6.2 total order.

    One ladder, one print, one mark and one bar close per minute. The ladder is published at
    `+55_000` rather than at the top of the minute so that an order submitted on the bar
    close and arriving a few hundred milliseconds later still finds a quote inside
    `book.MAX_QUOTE_STALENESS_MS`.
    """
    events: list[Event] = []
    for minute in range(MINUTES):
        base = START + minute * MS_PER_MINUTE
        close = base + MS_PER_MINUTE - 1
        events.append(
            Event(
                ts_ms=base + 55_000,
                kind=EventKind.BOOK_UPDATE,
                source_seq=minute,
                dataset_id="depth20:BTCUSDT",
                payload=DepthSnapshot(
                    symbol="BTCUSDT",
                    ts_ms=base + 55_000,
                    recv_ms=base + 55_040,
                    last_update_id=1_000 + minute,
                    bid_px=(59_990 * SCALE, 59_980 * SCALE),
                    bid_qty=(5 * SCALE, 5 * SCALE),
                    ask_px=(60_010 * SCALE, 60_020 * SCALE),
                    ask_qty=(5 * SCALE, 5 * SCALE),
                ),
            )
        )
        events.append(
            Event(
                ts_ms=base + 55_500,
                kind=EventKind.TRADE,
                source_seq=minute,
                dataset_id="aggTrades:BTCUSDT",
                payload=TradePrint(
                    symbol="BTCUSDT",
                    ts_ms=base + 55_500,
                    price_scaled=60_000 * SCALE,
                    qty_scaled=SCALE // 2,
                    is_buyer_maker=False,
                    agg_id=500_000 + minute,
                ),
            )
        )
        events.append(
            Event(
                ts_ms=close,
                kind=EventKind.MARK_PRICE_UPDATE,
                source_seq=minute,
                dataset_id="markPrice:BTCUSDT",
                payload=MarkBar(
                    symbol="BTCUSDT",
                    close_time=close,
                    high=60_050 * SCALE,
                    low=59_950 * SCALE,
                    close=60_000 * SCALE,
                ),
            )
        )
        events.append(
            Event(
                ts_ms=close,
                kind=EventKind.BAR_CLOSE,
                source_seq=minute,
                dataset_id="klines:BTCUSDT",
                payload=BarStep(
                    close_time=close,
                    bars=(
                        Bar(
                            symbol="BTCUSDT",
                            open_time=base,
                            close_time=close,
                            open=60_000 * SCALE,
                            high=60_050 * SCALE,
                            low=59_950 * SCALE,
                            close=60_000 * SCALE,
                            volume=10 * SCALE,
                            quote_volume=600_000 * SCALE,
                            trades=100,
                        ),
                    ),
                ),
            )
        )
    return events


def write_tape(
    directory: Path,
    events: list[Event],
    *,
    meta: dict[str, Any] | None = None,
    sealed: bool = True,
    late_dropped: int = 0,
    bar_closes: int | None = None,
    ended_ms: int | None = None,
    funding_times: dict[str, list[int]] | None = None,
) -> dict[str, Any]:
    """Record `events` into `directory/tape/`. Returns the metadata a reader would see."""
    writer = TapeWriter(directory, meta=meta or open_meta())
    for event in events:
        recv = getattr(event.payload, "recv_ms", None)
        writer.append_market(event, recv_ms=int(recv if recv is not None else event.ts_ms + 40))
    if sealed:
        writer.seal(
            ended_ms=(
                ended_ms if ended_ms is not None else START + MINUTES * MS_PER_MINUTE
            ),
            funding_times=funding_times or {},
            counts=seal_counts(
                len(events),
                late_dropped=late_dropped,
                bar_closes=(
                    bar_closes
                    if bar_closes is not None
                    else sum(1 for e in events if e.kind is EventKind.BAR_CLOSE)
                ),
            ),
        )
    else:
        writer.close()
    # Read back rather than taken from `meta_snapshot()`: the snapshot is the writer's own
    # view and does not carry the tallies `seal` merged in, which is exactly what a shadow
    # reads.
    return TapeReader(directory).meta()


def prepare(source: TapeSource) -> Any:
    """`TapeSource.prepare` never reads the engine, so this hands it nothing.

    That is not a shortcut, it is the property: everything a tape replay needs -- the range,
    the bar count, the funding schedule, the flags -- comes out of `meta.json`, because the
    engine's own view of those was built from a lake that does not hold this window.
    """
    return source.prepare(cast(Any, None))


def make_runs(root: Path, store: RunStore, **meta_overrides: Any) -> tuple[int, int, RunSpec]:
    """A finished paper run with a tape, plus the queued shadow of it.

    The shadow row is created directly rather than through `live.shadow.create_shadow`,
    which also `launch`es a worker subprocess; these tests drive `execute_run` in-process so
    that `resolve_tier` can be monkeypatched.
    """
    strategy_id, version_id = seed_strategy(root, STRATEGY_SOURCE)
    paper = paper_spec(
        strategy_id,
        version_id,
        end_ms=START + MINUTES * MS_PER_MINUTE,
        opening_balance="10000",
        leverage=10,
        params={},
        risk_limits={},
        auto_flatten={},
        kill_switch_flatten=False,
        timeout_s=300.0,
    )
    paper_run_id = store.create(
        strategy_id=strategy_id,
        version_id=version_id,
        spec=paper.to_storage(),
        label="paper session",
        mode="paper",
    )
    meta = write_tape(
        store.directory(paper_run_id),
        session_events(),
        meta=open_meta(run_id=paper_run_id, **meta_overrides),
    )
    shadow = shadow_spec(paper, paper_run_id, meta)
    shadow_run_id = store.create(
        strategy_id=strategy_id,
        version_id=version_id,
        spec=shadow.to_storage(),
        label=f"shadow of run {paper_run_id}",
        mode="shadow",
    )
    store.link_shadow(shadow_run_id, paper_run_id)
    return paper_run_id, shadow_run_id, paper


# ------------------------------------------------------------------------- shadow_spec


def test_a_shadow_carries_every_session_input_across_and_changes_only_four_fields() -> None:
    """Spec 12.1's inputs are copied verbatim; only the three testable things move.

    The session's spec and the shadow's are compared field by field over
    `RunSpec.__dataclass_fields__`, and the set that differs must be exactly
    `{source, session_kind, start_ms, end_ms}` -- where it reads, what kind of run it is, and
    the window the session actually observed. Every other field is an input spec 12.1
    records, and a shadow that changed one would make the parity report a comparison of two
    different runs rather than of two fill models.

    Enumerating the fields rather than listing them by hand is what makes this catch the
    failure that actually happens: a field added to `RunSpec` and forgotten in `shadow_spec`.

    The range is derived from the tape alone: `data_start_ms` is `START + 3` minutes and
    `ended_ms` is `START + 91` minutes, so the shadow trades the 88 minutes the session saw
    rather than the 480 it was configured for. The tape below also carries latency samples,
    and `latency` is deliberately *not* in the set that moves: see
    `test_a_shadow_reruns_the_latency_model_rather_than_sampling_its_own_draws`.
    """
    paper = paper_spec()
    meta = sealed_meta(
        data_start_ms=START + 3 * MS_PER_MINUTE,
        ended_ms=START + 91 * MS_PER_MINUTE,
        counts=seal_counts(4_096, bar_closes=88),
        latency_samples={"submit": [90, 110, 140], "cancel": [180, 190]},
    )

    shadow = shadow_spec(paper, 41, meta)

    changed = {
        name
        for name in RunSpec.__dataclass_fields__
        if getattr(shadow, name) != getattr(paper, name)
    }
    assert changed == {"source", "session_kind", "start_ms", "end_ms"}

    # The four that moved, and what each of them moved to.
    assert shadow.source == f"{TAPE_SOURCE_PREFIX}41"
    assert shadow.session_kind == "shadow"
    assert shadow.start_ms == START + 3 * MS_PER_MINUTE
    assert shadow.end_ms == START + 91 * MS_PER_MINUTE
    assert shadow.end_ms - shadow.start_ms == 88 * MS_PER_MINUTE
    assert shadow.latency == paper.latency

    # Spelled out for the ones the parity report's attributability rests on, so a future
    # reader can see them named rather than having to trust the set comparison above.
    assert shadow.seed == 90_210
    assert shadow.params == {"legs": 2, "threshold": "0.5"}
    assert shadow.code == paper.code
    assert shadow.code_sha256 == paper.code_sha256
    assert (shadow.maker_rate, shadow.taker_rate) == ("0.00016", "0.00040")
    assert shadow.fee_source == "commissionRate:2026-08-01"
    assert shadow.leverage == 7
    assert shadow.opening_balance == "12500.75"
    assert shadow.risk_limits == {"max_leverage": "5", "max_drawdown_pct": "15"}
    assert shadow.auto_flatten == {"max_hold_ms": 3_600_000, "before_funding_ms": 300_000}
    assert shadow.kill_switch_flatten is True
    assert shadow.liquidation_recovery_pct == "0.005"
    assert shadow.fill_tier == PAPER_TIER
    assert shadow.fill_model == paper.fill_model

    # Copies, not aliases: a later edit to one run's params must not reach the other's.
    assert shadow.params is not paper.params
    assert shadow.risk_limits is not paper.risk_limits
    assert shadow.fill_model is not paper.fill_model

    # Read off the tape rather than off the session's spec. They agree here because a
    # session records what it ran with; the point is which of the two was consulted.
    assert shadow.endpoint == meta["endpoint"] == "testnet"
    assert shadow.reorder_buffer_ms == meta["reorder_buffer_ms"] == 250


def test_a_shadows_fill_tier_is_the_one_the_session_executed_at() -> None:
    """The tape's tier wins over the tier the session's spec asked for.

    The two differ whenever a session degraded: `RunSpec.fill_tier` is the *request*, and
    `meta.json`'s is what the engine actually ran at. This session asked for `BOOK_WALK` and
    executed at `BOOK_TICKER`, so the shadow must ask for `BOOK_TICKER` -- replaying at
    `BOOK_WALK` would price the shadow's fills off a ladder the session never used, and the
    report would call the difference fill-model divergence.
    """
    paper = paper_spec(fill_tier="BOOK_WALK")
    meta = sealed_meta(fill_tier="BOOK_TICKER", ended_ms=START + 60_000)
    shadow = shadow_spec(paper, 41, meta)

    assert paper.fill_tier == "BOOK_WALK"
    assert shadow.fill_tier == "BOOK_TICKER"


def test_a_shadow_reruns_the_latency_model_rather_than_sampling_its_own_draws() -> None:
    """A tape *with* samples is the reachable case, and it must still change nothing.

    The engine's latency stream is shared by submits, cancels and amends, so any perturbation
    of it shifts every later draw and re-prices every later fill -- which the parity report
    cannot tell apart from execution divergence. Re-running the session's own model under the
    session's own seed is what reproduces the sequence; sampling a pool built from that
    sequence re-draws it in a different order, which is a perturbation wearing the word
    "replay".

    The tape below carries samples, which is what every sealed tape carries: `Session.seal`
    always calls `latency_samples`, and under the local fill simulator -- the only transport
    a shipped session has -- the engine stamps `arrival_ts > submit_ts` on every order the
    latency model delayed. The old `if not submits` guard was therefore reachable only by a
    session that submitted nothing, and every other shadow silently ran `empirical`.

    One of the samples is 520 120 ms: `BacktestEngine._fire_trigger` re-stamps `arrival_ts`
    when a stop fires and leaves `submit_ts` at submission, so a stop that rested about nine
    minutes is recorded as a nine-minute "latency". It is in the fixture because a pool
    holding it sends a random subset of the shadow's orders minutes past the market they were
    reacting to, and because it shows these numbers are not all latencies to begin with.
    """
    paper = paper_spec(latency={"model": "fixed", "submit_ms": 250, "cancel_ms": 310})
    meta = sealed_meta(
        ended_ms=START + 60_000,
        latency_samples={"submit": [120, 120, 520_120], "cancel": []},
    )

    shadow = shadow_spec(paper, 41, meta)

    assert shadow.latency == {"model": "fixed", "submit_ms": 250, "cancel_ms": 310}
    assert shadow.latency == paper.latency
    assert shadow.latency is not paper.latency


def test_a_session_that_measured_no_latencies_keeps_the_model_it_ran_with() -> None:
    """The same answer from the other side: an absent `latency_samples` changes nothing.

    A session running the local fill simulator has no *measured* latency -- its orders were
    delayed by the model in its own spec -- so replaying with that model and the same seed
    reproduces the same draws. Substituting anything else, including a pool built from those
    simulated draws, would perturb the shared latency RNG and re-price every later fill,
    which is precisely the divergence the parity report must not manufacture.

    This tape carries no samples at all, which no `Session.seal` produces; it is here so that
    the property is pinned on both shapes of metadata rather than only on the reachable one.
    """
    paper = paper_spec(latency={"model": "fixed", "submit_ms": 250, "cancel_ms": 310})
    shadow = shadow_spec(paper, 41, sealed_meta(ended_ms=START + 60_000))

    assert shadow.latency == {"model": "fixed", "submit_ms": 250, "cancel_ms": 310}
    assert shadow.latency == paper.latency
    assert shadow.latency is not paper.latency


def test_a_session_whose_spec_names_no_latency_model_is_refused_not_defaulted() -> None:
    """There is no default that would be honest here, so the shadow refuses to be built.

    A spec with no `model` key is not a session that ran without latency -- `latency_from_json`
    raises on it too -- and a shadow that picked `fixed` to get past it would re-price every
    fill in the run while keeping the run's identity (spec 12.1). The parity report would then
    read this module's own substitution as fill-model divergence, which is the one thing it
    must never do.
    """
    with pytest.raises(ShadowError, match="records no latency model"):
        shadow_spec(paper_spec(latency={}), 41, sealed_meta(ended_ms=START + 60_000))


def test_a_tape_that_recorded_nothing_is_refused_rather_than_replayed() -> None:
    """A parity of zero fills against zero fills is not a measurement.

    `released` is zero -- the reorder buffer dispatched nothing, which is what a feed that
    never connected leaves behind. Building a shadow anyway would replay an empty market,
    report every delta as 0.0, and pass spec 6.7.2's threshold by having measured nothing.
    """
    with pytest.raises(ShadowError, match="recorded no dispatched events"):
        shadow_spec(
            paper_spec(),
            41,
            sealed_meta(ended_ms=START + 60_000, counts=seal_counts(0)),
        )


def test_a_tape_covering_no_time_is_refused_rather_than_replayed() -> None:
    """`ended_ms` at or before `data_start_ms` is a session that stopped before it started.

    Both are `START` here, so the window is 0 ms. A run over an empty range is not a shorter
    comparison, it is no comparison -- and it would be reported as a clean parity.
    """
    with pytest.raises(ShadowError, match="covers no time"):
        shadow_spec(
            paper_spec(),
            41,
            sealed_meta(data_start_ms=START, ended_ms=START, counts=seal_counts(12)),
        )


def test_create_shadow_refuses_a_session_whose_tape_is_unsealed(tmp_path: Path) -> None:
    """An unsealed tape means the session crashed rather than stopping.

    Its last rows may be truncated and its counts are unknown, so a parity report built on it
    would compare a complete backtest against a truncated session and attribute the missing
    tail to the fill model. `create_shadow` refuses before it reads the spec, so this needs
    no run row -- only the directory the session was writing into.
    """
    build_userdata(tmp_path)
    store = RunStore(tmp_path)
    try:
        directory = store.directory(41)
        directory.mkdir(parents=True, exist_ok=True)
        write_tape(directory, session_events()[:4], sealed=False)

        with pytest.raises(ShadowError, match="unsealed"):
            create_shadow(tmp_path, 41, store)
    finally:
        store.close()


# -------------------------------------------------------------------------- TapeSource


def test_tape_source_refuses_an_unsealed_tape_by_default(tmp_path: Path) -> None:
    """The default is refusal, because the counts of a crashed session are unknown.

    A truncated recording compared against a complete backtest produces a final-PnL delta
    that includes whatever was lost, and nothing in the report would say so.
    """
    write_tape(tmp_path, session_events()[:4], sealed=False)

    with pytest.raises(TapeNotSealed, match="no seal"):
        prepare(TapeSource(tmp_path))


def test_an_unsealed_tape_can_be_replayed_when_asked_and_the_run_says_so(
    tmp_path: Path,
) -> None:
    """`allow_unsealed=True` is accepting a known unknown, so the run carries the flag.

    The four events written below are all replayed -- accepting the tape means accepting all
    of it -- and the run is flagged `TAPE_UNSEALED` with a warning that names the reason. A
    silent acceptance would be the worst of the three options: a parity report over a
    truncated session that reads exactly like one over a complete session.
    """
    events = session_events()[:4]
    write_tape(tmp_path, events, sealed=False)

    prepared = prepare(TapeSource(tmp_path, allow_unsealed=True))

    assert "TAPE_UNSEALED" in prepared.flags
    assert any("did not stop cleanly" in warning for warning in prepared.warnings)
    replayed = list(prepared.streams[0])
    assert len(replayed) == 4
    assert [event.key for event in replayed] == [event.key for event in events]


def test_a_tape_that_dropped_late_frames_flags_and_warns(tmp_path: Path) -> None:
    """Frames the reorder window had already passed were dropped, not dispatched late.

    The seal below records `late_dropped=3`. Both halves of the parity report replay what was
    dispatched, so they agree with each other -- and neither saw those three observations,
    which is a fact about the *session* that a reader comparing it against the lake later
    needs told. The warning quotes the count the tape recorded.
    """
    write_tape(tmp_path, session_events(), late_dropped=3)

    prepared = prepare(TapeSource(tmp_path))

    assert "TAPE_LATE_FRAMES" in prepared.flags
    assert any("3 frame(s)" in warning for warning in prepared.warnings)


def test_a_sealed_tape_replays_clean(tmp_path: Path) -> None:
    """The ordinary case carries neither flag, so the two above mean something.

    A test that only ever asserts a flag is present would pass against code that always
    sets it.
    """
    write_tape(tmp_path, session_events())

    prepared = prepare(TapeSource(tmp_path))

    assert prepared.flags == ()
    assert prepared.warnings == ()


def test_the_replayed_range_and_bar_count_come_from_the_tape_not_from_the_engine(
    tmp_path: Path,
) -> None:
    """`data_start_ms`, `total_bars` and the funding schedule are all read from `meta.json`.

    The tape below starts at `START + 7` minutes and its seal counts `MINUTES` bar closes --
    one per minute of `session_events()`. Re-deriving either from the lake is what a shadow
    must never do, because the bulk archive does not hold this window yet; `prepare` is
    handed no engine at all here, which is the sharpest form of that assertion.

    The two funding instants are written out of order and must come back sorted, because
    `_next_funding_ms` bisects the list `Prepared.funding_times` seeds.
    """
    later = START + 8 * 60 * MS_PER_MINUTE
    earlier = START + 30 * MS_PER_MINUTE
    write_tape(
        tmp_path,
        session_events(),
        meta=open_meta(data_start_ms=START + 7 * MS_PER_MINUTE),
        funding_times={"BTCUSDT": [later, earlier]},
    )

    prepared = prepare(TapeSource(tmp_path))

    assert prepared.data_start_ms == START + 7 * MS_PER_MINUTE
    assert prepared.total_bars == MINUTES
    assert prepared.funding_times == {"BTCUSDT": [earlier, later]}
    assert len(prepared.streams) == 1
    assert [event.key for event in prepared.streams[0]] == [
        event.key for event in session_events()
    ]


# ------------------------------------------------------- the worker's tape-replay branch


def test_a_tape_sourced_run_never_re_resolves_its_fill_tier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The single most likely way the parity report gets silently ruined.

    `tiers.resolve_tier` judges coverage from the Parquet lake. The bulk archive lags roughly
    a day, so the window a session has just traded is not in it -- every shadow would resolve
    to `BAR_CLOSE` with `COVERAGE_INCOMPLETE`, the worker's substitution rule would swap in
    that tier's default fill model, and a `BOOK_WALK` session would be compared against a
    bar-close replay with the difference reported as fill-model divergence.

    So `resolve_tier` is replaced with something that raises. The run must still complete,
    and must complete at the `BOOK_WALK` the tape recorded -- with no model substitution,
    because the tier it executes at is the tier its spec asked for. The fixture builds no
    lake at all, so a replay that reached for one would fail here too.
    """

    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "resolve_tier was called for a tape replay; the tier must come from the tape"
        )

    monkeypatch.setattr("perplab.engine.worker.resolve_tier", refuse)

    build_userdata(tmp_path)
    store = RunStore(tmp_path)
    try:
        paper_run_id, shadow_run_id, _ = make_runs(tmp_path, store)
        execute_run(tmp_path, shadow_run_id, store)

        summary = store.get(shadow_run_id)
        assert summary.status == RunStatus.DONE, summary.error
        assert summary.fill_tier == PAPER_TIER
        assert "FILL_MODEL_SUBSTITUTED" not in summary.flags
        assert "TAPE_REPLAY" in summary.flags

        manifest = store.read_json(shadow_run_id, "manifest.json")
        assert manifest["tier"]["tier"] == PAPER_TIER
        assert manifest["tier"]["available"] == PAPER_TIER
        assert manifest["tier"]["flags"] == ["TAPE_REPLAY"]
        assert "the session's own recording" in manifest["tier"]["reason"]
        # The executed model is the tier's, not a demoted stand-in -- which is the fill-side
        # consequence of the tier having come from the tape.
        assert manifest["reproducibility"]["fill_model_tier"] == PAPER_TIER
        assert manifest["reproducibility"]["executed_fill_model"]["tier"] == PAPER_TIER
        assert manifest["reproducibility"]["source"] == f"{TAPE_SOURCE_PREFIX}{paper_run_id}"
        assert manifest["reproducibility"]["session_kind"] == "shadow"
        # No dataset manifest: a shadow read a tape, and fingerprinting lake files it never
        # opened would be a claim about the wrong data.
        assert manifest["dataset"] is None
    finally:
        store.close()


def test_a_shadow_replays_the_market_the_session_recorded(tmp_path: Path) -> None:
    """The replay dispatches the tape's own events, and trades on them.

    `session_events()` writes one bar close per minute for `MINUTES` minutes, so the run must
    report exactly `MINUTES` bars -- the number comes from the fixture, not from the lake,
    which this fixture does not build. The strategy declares two bars of history and buys
    once on the first bar it is allowed to trade on, so exactly one fill is expected, priced
    off the ladder the tape carries.
    """
    build_userdata(tmp_path)
    store = RunStore(tmp_path)
    try:
        _, shadow_run_id, _ = make_runs(tmp_path, store)
        execute_run(tmp_path, shadow_run_id, store)

        summary = store.get(shadow_run_id)
        assert summary.status == RunStatus.DONE, summary.error
        metrics = store.read_json(shadow_run_id, "metrics.json")["summary"]
        assert metrics["bars"] == MINUTES
        assert metrics["orders"] == 1
        assert metrics["fills"] == 1
        assert metrics["rejects"] == 0
    finally:
        store.close()


def test_a_shadow_records_no_trial(tmp_path: Path) -> None:
    """Spec 8.5's `N` counts evaluations, and a replay of one is not a second one.

    The bias in a maximum over `N` draws grows with `N`, so counting a shadow inflates the
    multiple-testing correction for a search nobody performed. Worse, `best_run_id` would
    point at a replay rather than at the run someone actually chose to make.

    The session's own evaluation is recorded first, by hand, with a Sharpe of 0.5 -- so the
    counter starts at exactly 1 evaluation over 1 combination and `best_run_id` is the paper
    run. Running the shadow over the same parameters must leave both untouched. Recording the
    trial by hand rather than by running a second backtest is what makes the assertion
    discriminating: the counter is demonstrably live, and the shadow still does not move it.
    """
    build_userdata(tmp_path)
    store = RunStore(tmp_path)
    try:
        paper_run_id, shadow_run_id, paper = make_runs(tmp_path, store)
        store.record_trial(
            strategy_id=paper.strategy_id,
            params=paper.params,
            sharpe=0.5,
            run_id=paper_run_id,
        )
        before = store.trials(paper.strategy_id)
        assert (before["combinations"], before["evaluations"]) == (1, 1)

        execute_run(tmp_path, shadow_run_id, store)
        assert store.get(shadow_run_id).status == RunStatus.DONE

        after = store.trials(paper.strategy_id)
        assert after["combinations"] == 1
        assert after["evaluations"] == 1
        assert after["best_sharpe"] == 0.5
    finally:
        store.close()

    connection = db.connect(tmp_path)
    try:
        rows = connection.execute(
            "SELECT params_sha, evaluations, best_run_id FROM strategy_trials"
        ).fetchall()
    finally:
        connection.close()
    assert len(rows) == 1
    assert int(rows[0]["evaluations"]) == 1
    assert int(rows[0]["best_run_id"]) == paper_run_id


# ---------------------------------------------------------------------- RunSpec v4


def test_a_version_4_run_spec_round_trips_the_phase_7_fields() -> None:
    """`source`, `endpoint`, `reorder_buffer_ms` and `session_kind` survive storage.

    They are spec 12.1 inputs: two runs identical in every other field but reading different
    market data are not reproductions of each other, and a reorder window that changed what
    the strategy saw and when belongs beside the seed. A field that can move the answer and
    is not stored turns the reproducibility hash from a proof into a coincidence.
    """
    spec = paper_spec(
        source=f"{TAPE_SOURCE_PREFIX}41",
        endpoint="production",
        reorder_buffer_ms=750,
        session_kind="shadow",
    )

    stored = json.loads(json.dumps(spec.to_storage()))
    assert stored["spec_version"] == SPEC_VERSION == 4

    back = RunSpec.from_storage(stored)
    assert back.source == f"{TAPE_SOURCE_PREFIX}41"
    assert back.endpoint == "production"
    assert back.reorder_buffer_ms == 750
    assert back.session_kind == "shadow"
    assert back == spec


def test_a_version_3_run_spec_upgrades_to_what_that_run_actually_was() -> None:
    """Exact, not a guess: every version-3 spec was written before papertrading existed.

    Such a run read the lake, touched no exchange, buffered nothing and was not a session --
    so empty `source`, empty `endpoint`, empty `session_kind` and a zero reorder window are
    a description of it rather than a default applied to it. The version bump matters in the
    *forward* direction: a version-3 reader handed a paper run's spec would accept it, drop
    `source`, and replay a session's recording out of the lake instead.
    """
    stored = json.loads(json.dumps(paper_spec().to_storage()))
    old = {
        key: value
        for key, value in stored.items()
        if key not in ("source", "endpoint", "reorder_buffer_ms", "session_kind")
    }
    old["spec_version"] = 3

    upgraded = RunSpec.from_storage(old)

    assert upgraded.source == ""
    assert upgraded.endpoint == ""
    assert upgraded.reorder_buffer_ms == 0
    assert upgraded.session_kind == ""
    # Everything version 3 did carry is unchanged by the upgrade.
    assert upgraded.seed == 90_210
    assert upgraded.risk_limits == {"max_leverage": "5", "max_drawdown_pct": "15"}
    assert upgraded.fill_tier == PAPER_TIER
