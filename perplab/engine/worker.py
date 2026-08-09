"""The job worker -- one backtest, one process (spec 2.3).

```
python -m perplab.engine.worker <userdata-root> <run-id>
```

**Why a process rather than a thread.** Spec 2.3 puts backtests in a pool of worker
processes, "isolated so an infinite loop in strategy code kills one worker, not the
platform", and spec 11 says the API server never runs strategy code at all. A thread cannot
be killed; a process can. That is the entire argument, and it is the reason a strategy with
an accidental `while True:` costs one cancelled run rather than a restart of the server the
author needs in order to fix it.

**Everything the run recorded is written before the status says `done`.** The status
transition is the last statement executed. A run marked complete whose directory is
half-written would be *believed*, which is strictly worse than one marked failed.

**The manifest is built after the run, not before.** Spec 4.6's manifest has to cover the
data the run actually read, and that includes warm-up -- a strategy declaring 400 bars of
history read 400 bars from before its own start date. How far back that reaches is not known
until `on_start` has registered the indicators, so the honest range is the one the engine
reports when it finishes.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Sequence
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from perplab.core.account import FeeSchedule
from perplab.core.money import parse_money
from perplab.core.risk import RiskLimits
from perplab.data.gaps import detect_gaps
from perplab.data.manifest import CoverageError, build_manifest, market_root
from perplab.engine import ENGINE_VERSION
from perplab.engine.backtest import (
    AutoFlatten,
    BacktestConfig,
    BacktestEngine,
    BacktestResult,
)
from perplab.engine.fills import fill_model_for_tier, fill_model_from_json
from perplab.engine.latency import latency_from_json
from perplab.engine.source import TapeSource
from perplab.engine.tape import TAPE_SOURCE_PREFIX
from perplab.engine.tiers import resolve_tier, tier_from_name
from perplab.engine.runspec import (
    RunSpec,
    platform_commit,
    resolve_brackets,
    resolve_filters,
)
from perplab.store.runs import PROGRESS_EQUITY_NAME, RunStore
from perplab.strategy.loader import load_strategy_class

__all__ = ["execute_run", "main"]

MANIFEST_DATASETS = ("klines", "markPriceKlines", "funding")
"""Datasets gap detection scans row by row, for the manifest attached to a finished run.

Deliberately narrower than `TIER_DATASETS`. This pass runs over the range the engine
actually read -- warm-up included -- and a row-level scan of a year of `bookTicker` would
add more wall time than the backtest itself. The tick datasets are still fingerprinted by
`build_manifest`, which walks file metadata rather than rows.
"""

TIER_DATASETS = ("klines", "markPriceKlines", "aggTrades", "bookTicker", "depth20")
"""Datasets whose gaps can *demote a tier*, scanned over the requested range only.

Wider than `MANIFEST_DATASETS` because this is the pass whose verdict changes what the run
executes: presence is judged at partition granularity, so without a gap report a day the
collector was down for nine hours of would win `BOOK_WALK` on the strength of one part-file.
Narrower in *range* than the manifest pass, and that is the trade that makes it affordable --
the requested range rather than the warm-up-extended one, because tick coverage over
warm-up bars nobody trades on is not a reason to lower anyone's fidelity.
"""


@dataclass(frozen=True, slots=True)
class _TapeResolution:
    """What `resolve_tier` would have returned, taken from the tape instead.

    Shaped like `tiers.TierResolution` so everything downstream -- the manifest block, the
    substitution check, the run row -- is written by exactly the same code for a shadow as
    for a backtest. A branch at each of those sites would be four places for the two paths to
    drift apart.
    """

    tier: Any
    available: Any
    reason: str
    flags: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "tier": self.tier.name,
            "available": self.available.name,
            "reason": self.reason,
            "flags": list(self.flags),
        }


def _tape_resolution(tier: Any) -> _TapeResolution:
    return _TapeResolution(
        tier=tier,
        available=tier,
        reason=(
            "replayed from the session's own recording, so the tier is the one the session "
            "executed at rather than one derived from lake coverage -- the bulk archive "
            "does not yet hold this window, and re-deriving would demote every shadow"
        ),
        flags=("TAPE_REPLAY",),
    )


def _gap_report(root: Path, symbols: Sequence[str], start_ms: int, end_ms: int) -> list[Any]:
    """Gaps in the tier-deciding datasets over the requested range."""
    gaps: list[Any] = []
    for symbol in symbols:
        report = detect_gaps(
            market_root(root), symbol, start_ms, end_ms, datasets=TIER_DATASETS
        )
        gaps.extend(report.gaps)
    return gaps


def execute_run(root: Path, run_id: int, store: RunStore) -> None:
    """Run one backtest to completion and record everything it produced."""
    store.mark_running(run_id)
    spec = RunSpec.from_storage(store.read_json(run_id, "spec.json"))

    filters, filter_reference = resolve_filters(root, spec.symbols, spec.start_ms)
    brackets, bracket_reference = resolve_brackets(root, spec.symbols, spec.start_ms)

    cls = load_strategy_class(spec.code, filename=f"{spec.strategy_name}.py")
    strategy = cls(spec.params)
    requirements = strategy.declared

    replaying_tape = spec.source.startswith(TAPE_SOURCE_PREFIX)
    source: Any = None
    if replaying_tape:
        # **A shadow does not re-resolve its tier, and this is load-bearing.**
        # `resolve_tier` judges coverage from the lake; the bulk archive lags about a day, so
        # the window a session has just traded is not in it. Every shadow would therefore
        # demote to `BAR_CLOSE` with `COVERAGE_INCOMPLETE`, the substitution below would swap
        # in that tier's default fill model, and the parity report would compare a
        # `BOOK_WALK` session against a bar-close replay and report the difference as
        # fill-model divergence. The tape records the tier the session executed at; that is
        # the fact, and it is used.
        tape_run_id = int(spec.source[len(TAPE_SOURCE_PREFIX) :])
        source = TapeSource(store.directory(tape_run_id))
        resolution = _tape_resolution(tier_from_name(spec.fill_tier))
    else:
        # **The tier is resolved before the engine exists.** Spec 4.2's rule is that a
        # fidelity downgrade is recorded and never silent, and the only way to keep that is
        # to decide once, from the lake, with the gap report in hand -- rather than in
        # whichever fill path first notices a dataset it cannot read. The range used is the
        # *requested* one; the engine may read a little further back for warm-up, and
        # demanding tick coverage over warm-up bars nobody trades on would demote runs for
        # no reason.
        gaps = _gap_report(root, spec.symbols, spec.start_ms, spec.end_ms)
        resolution = resolve_tier(
            root,
            spec.symbols,
            spec.start_ms,
            spec.end_ms,
            requested=tier_from_name(spec.fill_tier),
            gaps=gaps,
        )

    config = BacktestConfig(
        symbols=spec.symbols,
        timeframe=spec.timeframe,
        start_ms=spec.start_ms,
        end_ms=spec.end_ms,
        seed=spec.seed,
        opening_balance=parse_money(spec.opening_balance),
        leverage=spec.leverage,
        hedge_mode=spec.hedge_mode,
        fees=FeeSchedule(
            maker_rate=parse_money(spec.maker_rate),
            taker_rate=parse_money(spec.taker_rate),
            source=spec.fee_source,
        ),
        latency=latency_from_json(spec.latency),
        fill_tier=resolution.tier,
        # The stored model is used only when it belongs to the tier the run actually
        # executes at. A degraded run carries a `BOOK_WALK` model in its spec and executes
        # at `BOOK_TICKER`; using the stored one would apply a depth-exhaustion penalty in a
        # model that never walks a ladder, so the tier's default is taken instead and the
        # substitution is visible in the manifest's own `fill_model` block.
        fill_model=(
            fill_model_from_json(dict(spec.fill_model))
            if spec.fill_model.get("tier") == resolution.tier.name
            else fill_model_for_tier(resolution.tier.name)
        ),
        liquidation_recovery_pct=parse_money(spec.liquidation_recovery_pct),
        timeout_s=spec.timeout_s,
        # Read from the stored spec rather than defaulted here. `RiskLimits.from_json({})`
        # is `unlimited()`, not spec 7's table, so every Phase 4 and Phase 5 run replays as
        # what it was -- a run with no risk layer -- rather than acquiring limits nobody set.
        risk=RiskLimits.from_json(spec.risk_limits),
        auto_flatten=AutoFlatten.from_json(spec.auto_flatten),
        kill_switch_flatten=spec.kill_switch_flatten,
    )

    engine = BacktestEngine(
        root=market_root(root),
        strategy=strategy,
        requirements=requirements,
        config=config,
        filters=filters,
        brackets=brackets,
        flags=(
            *filter_reference.flags,
            *bracket_reference.flags,
            *resolution.flags,
        ),
        progress=lambda bars, total: store.progress(run_id, bars, total),
        on_equity=lambda snapshot: snapshot.publish(
            store.directory(run_id) / PROGRESS_EQUITY_NAME
        ),
        source=source,
    )
    result = engine.run()

    if replaying_tape:
        # **The dataset manifest describes the lake, and a shadow did not read the lake.**
        # `build_manifest` fingerprints the Parquet files covering the range so a re-run can
        # be compared against them; over a window the bulk archive has not published yet it
        # would either raise `CoverageError` or fingerprint files this run never opened, and
        # either way the manifest would be a claim about the wrong data. The tape's own
        # `meta.json` is the fingerprint for a replay, and it travels with the run.
        manifest, extra_flags, coverage_error = None, ("TAPE_REPLAY",), None
    else:
        manifest, extra_flags, coverage_error = _build_manifest(
            root, spec, result, resolution
        )
    substituted = spec.fill_model.get("tier") != resolution.tier.name
    flags = tuple(
        sorted(
            set(result.flags)
            | set(extra_flags)
            | ({"FILL_MODEL_SUBSTITUTED"} if substituted else set())
        )
    )
    warnings = list(result.warnings)
    if result.halt_reason is not None:
        flags = tuple(sorted(set(flags) | {"RISK_HALTED"}))
    if substituted:
        warnings.append(
            f"this run asked for the {spec.fill_model.get('tier')} fill model and executed "
            f"at {resolution.tier.name}, so that tier's default parameters were used "
            f"instead: {config.resolved_fill_model().to_json()}. Any parameter set on the "
            f"requested model had no effect on these numbers."
        )
    if coverage_error:
        warnings.append(
            "the dataset manifest could not be completed for this range: "
            f"{coverage_error} The run's own results are unaffected -- it read what the "
            "lake held -- but there is no fingerprint to compare a re-run against (spec 4.6)."
        )

    directory = store.directory(run_id)
    _write_events(directory / "events.jsonl", result)
    _write_equity(directory / "equity.parquet", result)
    # The finished series supersedes the preview, and leaving both would let a reader that
    # checked the preview first serve a thinned curve for a run that has an exact one.
    (directory / PROGRESS_EQUITY_NAME).unlink(missing_ok=True)
    if result.symbol_pnl:
        _write_symbol_pnl(directory / "per_symbol.parquet", result)
    _write_json(
        directory / "trades.json",
        {"trades": [trade.to_json() for trade in result.trades]},
    )
    _write_json(
        directory / "metrics.json",
        {
            "metrics": result.metrics.to_json(),
            "attribution": result.attribution.to_json(),
            "summary": {
                **result.summary(),
                "flags": list(flags),
                "warnings": warnings,
            },
            "risk_breaches": [b.to_json() for b in result.risk_breaches],
        },
    )
    _write_json(
        directory / "manifest.json",
        {
            # Spec 12.1's list, in full. Every field here can move the answer; anything that
            # can move the answer and is *not* here turns the reproducibility hash from a
            # proof into a coincidence.
            # Spec 12.1's list is a record of a run's **inputs**, so the requested tier and
            # the requested model parameters stay exactly as the spec stored them. The
            # *executed* values sit beside them under their own names -- overwriting the
            # inputs with the outputs made two runs with different requests produce identical
            # reproducibility blocks, and discarded a `depth_exhaustion_pct` the user had
            # explicitly set on a run that then degraded away from `BOOK_WALK`.
            "reproducibility": {
                **spec.to_json(),
                "platform_commit": platform_commit(),
                "event_hash": result.event_hash,
                "executed_engine_version": ENGINE_VERSION,
                "fill_model_tier": result.fill_tier,
                "executed_fill_model": config.resolved_fill_model().to_json(),
                "data_start_ms": result.data_start_ms,
            },
            "tier": resolution.to_json(),
            "risk": result.risk_summary,
            "reference": {
                "exchangeInfo_used": filter_reference.used,
                "exchangeInfo_in_force": filter_reference.in_force,
                "leverageBracket_used": bracket_reference.used,
                "leverageBracket_in_force": bracket_reference.in_force,
            },
            "dataset": None if manifest is None else manifest.to_json(),
            "flags": list(flags),
            "warnings": warnings,
        },
    )

    store.complete(
        run_id,
        event_hash=result.event_hash,
        fill_tier=result.fill_tier,
        tier_reason=resolution.reason,
        flags=flags,
        warnings=warnings,
        net_pnl=str(result.attribution.net_pnl),
        sharpe=result.metrics.sharpe,
        max_drawdown=result.metrics.max_drawdown,
        round_trips=result.metrics.trades.round_trips,
        fills=result.fills,
        bars=result.bars,
    )
    if not replaying_tape:
        # **A shadow is not a trial.** Spec 8.5's counter exists to keep the multiple-testing
        # correction honest: `N` is the number of parameter combinations *evaluated*, and the
        # bias in a maximum over `N` draws grows with it. A shadow re-executes a combination
        # that has already been counted, so counting it again inflates `N` for a search
        # nobody performed -- and if the shadow's Sharpe happened to come out higher, the
        # strategy's "best run" link would point at a replay rather than at a real run.
        store.record_trial(
            strategy_id=spec.strategy_id,
            params=spec.params,
            sharpe=result.metrics.sharpe,
            run_id=run_id,
        )


def _build_manifest(
    root: Path, spec: RunSpec, result: BacktestResult, resolution: Any
) -> tuple[Any, tuple[str, ...], str | None]:
    """Spec 4.6's dataset manifest, over the range the run actually read.

    Gap detection runs first because the manifest consumes its verdict: an unexplained hole
    inside a tier's inputs demotes that tier, so a day with one part-file does not win a
    fidelity the data cannot support.

    The tier recorded here is derived over the *warm-up-extended* range, which can be lower
    than the one the run executed at -- `resolve_tier` deliberately judges the requested
    range only. The two are compared rather than silently reconciled: a manifest tier below
    the executed tier means the run read warm-up bars from a period with thinner coverage,
    which is worth a flag and is not worth invalidating a result over.
    """
    flags: list[str] = []
    gaps: list[Any] = []
    for symbol in spec.symbols:
        report = detect_gaps(
            market_root(root),
            symbol,
            result.data_start_ms,
            spec.end_ms,
            datasets=MANIFEST_DATASETS,
        )
        gaps.extend(report.gaps)

    try:
        manifest = build_manifest(
            root, list(spec.symbols), result.data_start_ms, spec.end_ms, gaps=gaps
        )
    except CoverageError as exc:
        # **A completed run is not thrown away over a presence check.** `build_manifest`
        # runs after the engine, and `derive_fill_model_tier` refuses a range without a
        # published file in *every* partition -- so a range extending a few days past the
        # end of the lake, which the run form accepts as free text, used to discard a
        # finished backtest and leave nothing but `spec.json`. The engine had already read
        # what was there and flagged the shortfall; the manifest records the same fact
        # rather than raising over it, and the run keeps its results.
        return None, tuple(sorted({"COVERAGE_INCOMPLETE", *flags})), str(exc)

    # Compared against what the lake *supports* over the traded range, not against what the
    # run executed at. Those differ whenever a run deliberately asks for less than it could
    # have had -- which is `TIER_BELOW_DATA`, an entirely different fact -- so comparing
    # against `resolution.tier` flagged every default `BOOK_TICKER` run over the collector's
    # own window as a warm-up anomaly. And only the *lower* direction is what this flag
    # claims: the manifest tier is derived over the warm-up-extended range, so it can drop
    # below `available` when the warm-up reaches into thinner coverage, and cannot
    # meaningfully rise above it.
    if tier_from_name(manifest.fill_model_tier) < resolution.available:
        flags.append("WARMUP_TIER_DIFFERS")
    flags.extend(manifest.flags)
    return manifest, tuple(sorted(set(flags))), None


def _write_events(path: Path, result: BacktestResult) -> None:
    """The strategy event log, one JSON object per line.

    JSONL rather than one array so the viewer can page it without parsing the whole file,
    and so a run whose log is hundreds of megabytes is still greppable from a shell. Written
    to a temporary name and renamed, like every other artefact: a truncated log would parse
    right up to the point it stopped.

    `ensure_ascii=False`, because `read_events` searches the **raw line**. With the default,
    a log entry the viewer renders as `café` is stored as `caf\\u00e9`, and typing what is on
    the screen into the viewer's own search box returned nothing. The file is UTF-8 and every
    reader of it opens it as UTF-8.
    """
    tmp = path.parent / f".{path.name}.tmp"
    with tmp.open("w", encoding="utf-8") as handle:
        for event in result.events:
            handle.write(
                json.dumps(
                    event.to_json(),
                    separators=(",", ":"),
                    default=str,
                    ensure_ascii=False,
                )
                + "\n"
            )
    tmp.replace(path)


def _write_equity(path: Path, result: BacktestResult) -> None:
    """The mark-to-market series as Parquet.

    Parquet, not JSON: a year of 1-minute marks with a position open for half of it is
    close to a million samples, and the drawdown figures in spec 8.2 are computed over
    *every* one of them. Compressed columns make that a few megabytes; a JSON array of the
    same points is forty.
    """
    table = pa.table(
        {
            "ts_ms": pa.array(result.equity_ms, type=pa.int64()),
            "equity": pa.array(result.equity, type=pa.float64()),
            # The intrabar band travels with the sample. Without it the results page would
            # compute its shaded drawdown from the close series alone while the metric card
            # beside it used the band, and the two would disagree -- by 0.008 percentage
            # points on the exit run, which is small and is exactly the kind of discrepancy
            # that costs an hour to explain.
            "equity_low": pa.array(result.equity_low, type=pa.float64()),
            "equity_high": pa.array(result.equity_high, type=pa.float64()),
            "position_open": pa.array(result.position_open, type=pa.bool_()),
        }
    )
    tmp = path.parent / f".{path.name}.tmp"
    pq.write_table(table, tmp, compression="zstd", compression_level=3)
    tmp.replace(path)


def _write_symbol_pnl(path: Path, result: BacktestResult) -> None:
    """Per-symbol cumulative PnL for multi-symbol runs, on the equity sample cadence.

    What the spec 9.5 portfolio report reads. Only written when the engine tracked it
    (two or more symbols); a single-symbol run's absence of this file is the honest
    signal the portfolio endpoint turns into its 404.
    """
    columns: dict[str, Any] = {
        "ts_ms": pa.array(result.equity_ms, type=pa.int64())
    }
    for symbol in sorted(result.symbol_pnl):
        columns[symbol] = pa.array(result.symbol_pnl[symbol], type=pa.float64())
    table = pa.table(columns)
    tmp = path.parent / f".{path.name}.tmp"
    pq.write_table(table, tmp, compression="zstd", compression_level=3)
    tmp.replace(path)


def _write_json(path: Path, payload: Any) -> None:
    tmp = path.parent / f".{path.name}.tmp"
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    tmp.replace(path)




def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m perplab.engine.worker",
        description="Execute one stored backtest run. Started by the API server.",
    )
    parser.add_argument("root", type=Path, help="the userdata root")
    parser.add_argument("run_id", type=int)
    args = parser.parse_args(argv)

    store = RunStore(args.root)
    try:
        execute_run(args.root, args.run_id, store)
    except BaseException as exc:  # noqa: BLE001 - a failed run is data, not a crash
        # Including `KeyboardInterrupt` and `SystemExit`: a cancelled worker should leave a
        # row saying what happened rather than one still claiming to be running, which is
        # indistinguishable from a run that hung.
        detail = f"{type(exc).__name__}: {exc}\n\n" + "".join(
            traceback.format_exception(exc)
        )
        try:
            store.fail(args.run_id, detail)
        finally:
            print(detail, file=sys.stderr)
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    raise SystemExit(main())
