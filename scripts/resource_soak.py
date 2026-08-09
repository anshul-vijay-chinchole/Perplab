"""Repeatable, governed RAM probes: growing histories or real tick-fidelity CVD replay.

Every run leaves resources.json, memory.csv and report.json under userdata/_benchmarks.
No orders are sent to an exchange. Market archives are read-only.
"""
from __future__ import annotations
import argparse
import csv
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from perplab.resources import ResourceGuard, checkpoint, process_memory


def epoch(value: str) -> int:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(stamp.replace(tzinfo=timezone.utc).timestamp()*1000) if stamp.tzinfo is None else int(stamp.timestamp()*1000)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("userdata"))
    parser.add_argument("--history-rows", type=int, default=0, help="append this many event/equity samples without retaining them in RAM")
    parser.add_argument("--start", help="CVD replay UTC start, e.g. 2024-01-01")
    parser.add_argument("--end", help="CVD replay exclusive UTC end")
    parser.add_argument("--timeout", type=float, default=3600)
    args = parser.parse_args()
    if args.history_rows < 0 or not args.history_rows and not (args.start and args.end):
        parser.error("choose --history-rows or both --start and --end")
    directory = args.root / "_benchmarks" / uuid.uuid4().hex
    directory.mkdir(parents=True)
    with (directory / "memory.csv").open("w", newline="", encoding="utf-8") as profile:
        writer = csv.writer(profile)
        writer.writerow(["elapsed_s", "stage", "private_bytes", "peak_bytes", "status", "queue_reason"])
        started = time.monotonic()
        def sample():
            state = guard._state()
            writer.writerow([round(time.monotonic()-started, 3), state["stage"], state.get("private_bytes"), state.get("peak_bytes"), state["status"], state["queue_reason"]])
            profile.flush()
        guard = ResourceGuard(args.root, "backtest", directory=directory, heartbeat=sample)
        with guard:
            from perplab.engine.history import history_scope
            with history_scope():
                report = histories(directory, args.history_rows) if args.history_rows else replay(args.root, directory, epoch(args.start), epoch(args.end), args.timeout)
            sample()
            report.update(elapsed_s=time.monotonic()-started, memory=process_memory(), policy=guard._state()["policy"])
            (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"directory": str(directory.resolve()), **report}, indent=2))
    return 0


def histories(directory: Path, rows: int) -> dict:
    from perplab.engine.history import DiskSequence
    from perplab.strategy.context import StrategyEvent
    from perplab.strategy.dryrun import event_hash
    from perplab.engine.artifacts import write_columns, equity_chart
    import pyarrow as pa
    events = DiskSequence(directory / "events.sqlite3", event_sequence=True)
    times = DiskSequence(directory / "times.sqlite3")
    equity = DiskSequence(directory / "equity.sqlite3")
    baseline = middle = None
    for i in range(rows):
        events.append(StrategyEvent(seq=i, ts_ms=i*60_000, kind="CVD_SAMPLE", payload={"value": (i%101)-50}))
        times.append(i*60_000)
        equity.append(10000.0+(i%1000))
        if i == rows//4:
            baseline = process_memory()["private_bytes"]
        if i == rows//2:
            middle = process_memory()["private_bytes"]
        if i%4096 == 0:
            checkpoint("history_soak")
    final = process_memory()["private_bytes"]
    checkpoint("finalization")
    write_columns(directory / "equity.parquet", {"ts_ms": times, "equity": equity}, {"ts_ms": pa.int64(), "equity": pa.float64()})
    chart = equity_chart(directory / "equity.parquet")
    return {"mode": "histories", "rows": rows, "event_hash": event_hash(events), "quarter_bytes": baseline, "middle_bytes": middle, "end_bytes": final,
            "growth_after_quarter_bytes": final-baseline if baseline else None, "chart_points": chart["returned"]}


def replay(root: Path, directory: Path, start: int, end: int, timeout: float) -> dict:
    from perplab.strategy.base import Strategy
    from perplab.engine.backtest import BacktestEngine, BacktestConfig
    from perplab.engine.runspec import resolve_filters, resolve_brackets
    from perplab.engine.tiers import FillTier
    from perplab.core.money import parse_money
    from perplab.engine.worker import _write_equity, _write_events
    class CVDProbe(Strategy):
        requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 1, "datasets": ["klines", "aggTrades"]}
        def on_start(self, ctx):
            self.cvd = ctx.indicators.cvd()
            self.seen = 0
        def on_tick(self, ctx, trade):
            self.seen += 1
        def on_bar(self, ctx, bar):
            ctx.log.info("CVD", value=self.cvd.value, ticks=self.seen)
    strategy = CVDProbe()
    filters, _ = resolve_filters(root, ("BTCUSDT",), start)
    brackets, _ = resolve_brackets(root, ("BTCUSDT",), start)
    engine = BacktestEngine(root=root / "market", strategy=strategy, requirements=strategy.declared,
        config=BacktestConfig(symbols=("BTCUSDT",), timeframe="1m", start_ms=start, end_ms=end, fill_tier=FillTier.TRADE_ONLY,
                             opening_balance=parse_money("10000"), timeout_s=timeout),
        filters=filters, brackets=brackets, history_directory=directory / "_history")
    result = engine.run()
    engine.account.reconcile()
    _write_events(directory / "events.jsonl", result)
    _write_equity(directory / "equity.parquet", result)
    return {"mode": "real_cvd", "start_ms": start, "end_ms": end, "ticks": result.ticks, "bars": result.bars, "event_hash": result.event_hash,
            "fill_tier": result.fill_tier, "warnings": list(result.warnings), "metrics": result.metrics.to_json()}


if __name__ == "__main__":
    raise SystemExit(main())
