"""The shadow backtest -- spec 6.7.1, and the only thing that keeps the backtester honest.

> *"Every paper/live session records its exact market-data inputs. On session end, a backtest
> is automatically re-run over that window with the same strategy version, seed, and params.
> A parity report is attached to the run."*

Architecture alone does not prevent divergence between a backtest and a live session; it has
to be measured. This module builds the measurement: it takes a finished paper session, writes
a sibling run whose only differences from the session are the ones being tested, and queues
it through the ordinary worker.

**What is deliberately held identical, and why each one is a way the report could lie:**

- *The market data*, by replaying the session's tape rather than the lake. The lake lags a
  day, is downsampled on its own schedule, and would silently restore observations the
  session never received -- so a lake replay would attribute a missing frame to the fill
  model.
- *The fill tier*, taken from the tape's `meta.json`. `tiers.resolve_tier` judges coverage
  from the lake, the lake does not hold this window yet, so every shadow would demote to
  `BAR_CLOSE` and a `BOOK_WALK` session would be compared against a bar-close replay.
- *The latencies*, by re-running the session's own latency model under the session's own
  seed -- not by re-sampling what the session measured. The engine's latency RNG is one
  stream shared by submits, cancels and amends, so any perturbation of it shifts every
  subsequent draw and re-prices every later fill, and the report would then be measuring RNG
  desynchronisation. Re-sampling *is* a perturbation, which is the correction this bullet
  carries: see `_replay_latency` for the divergence a perfect backtest was reported as while
  this module claimed to be holding latency fixed.
- *The range*, set to the window the session actually observed rather than the one it was
  configured for. A session stopped early would otherwise have its shadow trade on into data
  the session never saw.

**What is deliberately allowed to differ** is exactly one thing: the fills. That is the
quantity spec 6.7.2 asks the report to judge, and holding everything else fixed is what makes
the answer attributable.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from perplab.engine.runspec import RunSpec
from perplab.engine.tape import TAPE_SOURCE_PREFIX, TapeReader
from perplab.store.runs import RunStore

__all__ = ["ShadowError", "shadow_spec", "create_shadow", "write_parity"]


class ShadowError(RuntimeError):
    """A shadow backtest cannot be built from this session."""


def shadow_spec(paper_spec: RunSpec, paper_run_id: int, tape_meta: dict[str, Any]) -> RunSpec:
    """The session's spec, re-pointed at its own recording.

    The seed, params, code, fees, leverage, opening balance and risk limits are carried over
    untouched -- those are the inputs spec 12.1 records, and changing any of them would make
    the comparison a comparison of two different runs.
    """
    counts = tape_meta.get("counts", {}) or {}
    start_ms = int(tape_meta.get("data_start_ms", paper_spec.start_ms))
    end_ms = int(tape_meta.get("ended_ms") or paper_spec.end_ms)
    if end_ms <= start_ms:
        raise ShadowError(
            f"the tape for run {paper_run_id} covers no time ({start_ms}..{end_ms}); the "
            "session stopped before it observed anything, so there is nothing to compare."
        )
    if not counts.get("released"):
        raise ShadowError(
            f"the tape for run {paper_run_id} recorded no dispatched events, so a shadow "
            "backtest would replay an empty market and report a parity of zero against "
            "zero. Check the session's status log for a feed that never connected."
        )

    latency = _replay_latency(paper_spec, paper_run_id)
    return RunSpec(
        strategy_id=paper_spec.strategy_id,
        version_id=paper_spec.version_id,
        version_no=paper_spec.version_no,
        strategy_name=paper_spec.strategy_name,
        code=paper_spec.code,
        class_name=paper_spec.class_name,
        params=dict(paper_spec.params),
        symbols=paper_spec.symbols,
        timeframe=paper_spec.timeframe,
        start_ms=start_ms,
        end_ms=end_ms,
        seed=paper_spec.seed,
        opening_balance=paper_spec.opening_balance,
        leverage=paper_spec.leverage,
        maker_rate=paper_spec.maker_rate,
        taker_rate=paper_spec.taker_rate,
        fee_source=paper_spec.fee_source,
        latency=latency,
        # From the tape, never re-resolved. See the module docstring.
        fill_tier=str(tape_meta.get("fill_tier", paper_spec.fill_tier)),
        fill_model=dict(paper_spec.fill_model),
        liquidation_recovery_pct=paper_spec.liquidation_recovery_pct,
        timeout_s=paper_spec.timeout_s,
        engine_version=paper_spec.engine_version,
        risk_limits=dict(paper_spec.risk_limits),
        auto_flatten=dict(paper_spec.auto_flatten),
        kill_switch_flatten=paper_spec.kill_switch_flatten,
        source=f"{TAPE_SOURCE_PREFIX}{paper_run_id}",
        endpoint=str(tape_meta.get("endpoint", "")),
        reorder_buffer_ms=int(tape_meta.get("reorder_buffer_ms", 0) or 0),
        session_kind="shadow",
    )


def _replay_latency(paper_spec: RunSpec, paper_run_id: int) -> dict[str, Any]:
    """The latency model the shadow runs with: **the session's own, carried across unchanged.**

    Same model, same seed, same strategy, same tape -- so the shadow's latency RNG draws the
    identical sequence the session drew and every order leaves and lands where it did. That is
    what makes the parity report a measurement of the fill model and nothing else.

    **This used to substitute an `empirical` model built from `meta.json`'s
    `latency_samples`, and that was wrong three times over.** All three are named because each
    one on its own justifies this function being four lines:

    1. *It was not a replay.* `EmpiricalLatency` draws uniformly with replacement, so a pool
       built from the session's own draws re-draws them in a different order. Against a
       session running spec 6.3's default lognormal, per-order arrivals moved by -236 ms to
       +411 ms and a backtest that had reproduced the session exactly was reported as a
       6.5%-of-gross PnL divergence -- flagged with spec 6.7.2's own wording, which is the
       sentence that says the fill model needs recalibrating.
    2. *The guard that was supposed to prevent that was unreachable.* It read
       `if not submits`, and `PaperSession.latency_samples` collects
       `arrival_ts - submit_ts` for every order the engine stamped -- which, under the local
       fill simulator that every shipped session uses, is every order with a non-zero
       latency. The fallback fired for a session that submitted nothing, and for nothing
       else, while this function's docstring described it as the ordinary case.
    3. *The samples are not all latencies.* `BacktestEngine._fire_trigger` re-stamps
       `arrival_ts` when a stop fires and leaves `submit_ts` at the original submission, so a
       stop that rested nine minutes contributes a 520 000 ms "latency". One of those in the
       pool -- and stop losses are ordinary -- and a random subset of the shadow's orders
       arrive minutes after the market they were reacting to.

    Spec 6.3 does reserve `empirical` for *"latencies measured during your own paper
    sessions"*, and it is the right model the day a session's orders are filled by a real
    exchange instead of by the simulator. It is not the right model for draws the simulator
    took from the spec being replayed, and **nothing on the tape distinguishes the two** --
    so re-enabling that path starts with the session recording which of them it measured,
    not with this function guessing.
    """
    latency = dict(paper_spec.latency)
    if not latency.get("model"):
        # Refused rather than defaulted, on `latency_from_json`'s own argument: a shadow
        # given a substituted model re-prices every fill in the run while keeping the run's
        # identity (spec 12.1), and here it would also hand that substitution to the parity
        # report to attribute to the fill model.
        raise ShadowError(
            f"run {paper_run_id}'s spec records no latency model ({paper_spec.latency!r}), "
            "so there is nothing for the shadow to replay. A shadow that picked a default "
            "would re-price every fill in the run and the parity report would read the "
            "difference as fill-model divergence."
        )
    return latency


def create_shadow(root: Path | str, paper_run_id: int, store: RunStore) -> int:
    """Create and queue the shadow for a finished paper session. Returns its run id."""
    directory = store.directory(paper_run_id)
    reader = TapeReader(directory)
    if not reader.sealed:
        raise ShadowError(
            f"run {paper_run_id}'s tape is unsealed, so the session crashed rather than "
            "stopping. Its recording may end mid-observation, and a parity report built on "
            "it would compare a complete backtest against a truncated session."
        )
    paper_spec = RunSpec.from_storage(store.read_json(paper_run_id, "spec.json"))
    spec = shadow_spec(paper_spec, paper_run_id, reader.meta())

    run_id = store.create(
        strategy_id=spec.strategy_id,
        version_id=spec.version_id,
        spec=spec.to_storage(),
        label=f"shadow of run {paper_run_id}",
        mode="shadow",
    )
    store.link_shadow(run_id, paper_run_id)
    store.launch(run_id)
    return run_id


def write_parity(store: RunStore, paper_run_id: int, shadow_run_id: int) -> dict[str, Any]:
    """Build the spec 6.7.1 report and publish it into the paper run's directory.

    Written to the *paper* run, because that is the run a person opens to ask whether the
    backtester was telling the truth about it.
    """
    from perplab.analytics.parity import build_parity

    report = build_parity(store.directory(paper_run_id), store.directory(shadow_run_id))
    payload = {
        "paper_run_id": paper_run_id,
        "shadow_run_id": shadow_run_id,
        **report.to_json(),
    }
    path = store.artefact(paper_run_id, "parity.json")
    tmp = path.parent / f".{path.name}.tmp"
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    tmp.replace(path)
    return payload
