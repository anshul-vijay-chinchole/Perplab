"""Time the Phase 1 query criterion against the real lake, and show why it holds.

Spec 13's Phase 1 exit criterion is "any symbol/range queryable in <2 s". A single elapsed
number cannot support that claim: a query can come in under budget because it pruned
correctly or because the lake is small, and only one of those survives a full backfill. So
every case below reports three things -- connect time, execute time, and how many Parquet
files the plan will actually open out of how many the view can see.

The split is the diagnosis. Connect time is view construction globbing the lake, so it
grows with the *file count* and is fixed by narrowing `--dataset` or by compaction. Execute
time is the scan, so it grows with the rows actually read and is fixed by pruning. A run
that misses the budget with 40 ms of connect and 3 s of execute is a different problem from
one with 2 s of connect and 40 ms of execute, and the two have no fix in common.

    .venv/Scripts/python.exe scripts/query_benchmark.py --start 2025-01-01 --end 2025-12-31

Exit code is non-zero if any case misses the budget, or if a case whose predicate does
exclude some partition still read every file -- the second being a pass that would stop
being one on a bigger lake. A case that legitimately covers the whole dataset is not held
to that: there is nothing there to discard, and demanding pruning of it would fail a
correct plan.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from perplab.data.bulk_layout import datetime_str_to_ms  # noqa: E402
from perplab.data.query import (  # noqa: E402
    hive_columns,
    partition_predicate,
    pruning_report,
    query,
    timed_query,
    timeframe_sql,
)
from perplab.data.schemas import market_root  # noqa: E402

_MS_PER_DAY = 86_400_000


@dataclass(frozen=True, slots=True)
class Case:
    """One timed query, plus whether its layout offers anything to prune on at all.

    `prunable` is a fact about the *dataset*, not about this run: `funding` partitions by
    symbol only (granularity `none`), so there is no time partition to discard and
    demanding pruning of it would be demanding something the layout cannot do.

    Whether pruning is *expected* on a given run is a separate question, answered by
    `_expect_pruning` from the predicate and the partitions on disk.
    """

    name: str
    sql: str
    datasets: tuple[str, ...]
    start_ms: int | None = None
    end_ms: int | None = None
    """The range *this case's* predicate covers, which is not always the run's range: the
    point lookup asks for one minute inside it. Carried per case because the pruning
    expectation is computed from the predicate, and computing it from the run's range
    would have judged the point lookup against a filter it never used."""
    prunable: bool = True


def _cases(symbol: str, start_ms: int, end_ms: int) -> tuple[Case, ...]:
    kline_where = partition_predicate(
        "klines", symbol=symbol, start_ms=start_ms, end_ms=end_ms
    )
    # A point lookup inside one day, phrased the way a caller who knows the minute would:
    # the partition predicate narrows to the month and the open_time equality does the
    # rest from row-group statistics.
    point_ms = start_ms + 37 * _MS_PER_DAY + 13 * 3_600_000 + 27 * 60_000
    point_where = partition_predicate(
        "klines", symbol=symbol, start_ms=point_ms, end_ms=point_ms + 60_000
    )
    return (
        Case(
            "1m klines, full range",
            f'SELECT * FROM "klines" WHERE {kline_where} '
            f'AND "open_time" >= {start_ms} AND "open_time" < {end_ms} '
            f'ORDER BY "open_time"',
            ("klines",),
            start_ms,
            end_ms,
        ),
        Case(
            "1m klines, count only",
            f'SELECT count(*), min("open_time"), max("open_time") FROM "klines" '
            f'WHERE {kline_where} AND "open_time" >= {start_ms} AND "open_time" < {end_ms}',
            ("klines",),
            start_ms,
            end_ms,
        ),
        Case(
            "derived 1h bars",
            timeframe_sql("1h", symbol=symbol, start_ms=start_ms, end_ms=end_ms),
            ("klines",),
            start_ms,
            end_ms,
        ),
        Case(
            "derived 4h bars",
            timeframe_sql("4h", symbol=symbol, start_ms=start_ms, end_ms=end_ms),
            ("klines",),
            start_ms,
            end_ms,
        ),
        Case(
            "derived 1d bars",
            timeframe_sql("1d", symbol=symbol, start_ms=start_ms, end_ms=end_ms),
            ("klines",),
            start_ms,
            end_ms,
        ),
        Case(
            "funding range",
            f'SELECT * FROM "funding" WHERE '
            f'{partition_predicate("funding", symbol=symbol)} '
            f'AND "calc_time" >= {start_ms} AND "calc_time" < {end_ms} '
            f'ORDER BY "calc_time"',
            ("funding",),
            start_ms,
            end_ms,
            prunable=False,
        ),
        Case(
            "point lookup, one minute",
            f'SELECT * FROM "klines" WHERE {point_where} AND "open_time" = {point_ms}',
            ("klines",),
            point_ms,
            point_ms + 60_000,
        ),
        Case(
            "markPriceKlines, full range",
            f'SELECT count(*) FROM "markPriceKlines" WHERE '
            f'{partition_predicate("markPriceKlines", symbol=symbol, start_ms=start_ms, end_ms=end_ms)} '
            f'AND "open_time" >= {start_ms} AND "open_time" < {end_ms}',
            ("markPriceKlines",),
            start_ms,
            end_ms,
        ),
    )


def _expect_pruning(
    root: Path, dataset: str, symbol: str, start_ms: int | None, end_ms: int | None
) -> bool:
    """Does this case's predicate actually exclude any partition that exists on disk?

    The question is not "is the range small" but "is there anything to discard", and only
    the second one is answerable without guessing. A query for the whole of a dataset's
    history keeps every partition, so the honest plan opens every file and DuckDB emits no
    file filter at all -- which from the plan alone is indistinguishable from a predicate
    that failed to push down. Two earlier versions of this check both got it wrong from
    the other side: `always expect pruning` failed six correct full-history plans, and
    `expect pruning when the range is a strict subset` still failed `markPriceKlines`,
    whose year partitions are all inside a range that starts eight days after its data
    does.

    So the expectation is computed the same way DuckDB decides: apply
    `partition_predicate` to the distinct partition keys the view can see, and expect
    pruning exactly when that keeps fewer than all of them. Cheap -- it reads Hive path
    components, not row data -- and it cannot disagree with the predicate under test,
    because it *is* the predicate under test.
    """
    keys = hive_columns(dataset)
    if not keys:
        return False
    projection = ", ".join(f'"{key}"' for key in keys)
    predicate = partition_predicate(
        dataset, symbol=symbol, start_ms=start_ms, end_ms=end_ms
    )
    row = query(
        root,
        f'WITH parts AS (SELECT DISTINCT {projection} FROM "{dataset}") '
        f"SELECT count(*) AS total, count(*) FILTER (WHERE {predicate}) AS kept "
        f"FROM parts",
        datasets=(dataset,),
    ).to_pylist()[0]
    return bool(row["total"]) and row["kept"] < row["total"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="query_benchmark",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--root", default="userdata")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--end", required=True, metavar="YYYY-MM-DD", help="inclusive")
    parser.add_argument("--budget", type=float, default=2.0)
    parser.add_argument(
        "--repeat",
        type=int,
        default=2,
        help=(
            "runs per case; the slowest is reported. A first run pays the OS file-cache "
            "miss, and quoting only the warm number would be quoting a benchmark rather "
            "than a criterion"
        ),
    )
    parser.add_argument(
        "--all-datasets",
        action="store_true",
        help=(
            "build every view rather than only the one each case reads, which is what a "
            "caller who has not narrowed --dataset actually pays"
        ),
    )
    args = parser.parse_args(argv)

    start_ms = datetime_str_to_ms(f"{args.start} 00:00:00")
    end_ms = datetime_str_to_ms(f"{args.end} 00:00:00") + _MS_PER_DAY
    root = market_root(args.root)

    print(f"lake     : {root.resolve()}")
    print(f"symbol   : {args.symbol}")
    print(f"range    : {args.start} .. {args.end} (inclusive)")
    print(f"budget   : {args.budget:g} s   repeats: {args.repeat} (slowest reported)")
    print()
    header = (
        f"{'case':<32} {'rows':>10} {'connect':>9} {'execute':>9} {'total':>9}  "
        f"{'files read':>12}  verdict"
    )
    print(header)
    print("-" * len(header))

    failures: list[str] = []
    for case in _cases(args.symbol, start_ms, end_ms):
        datasets = None if args.all_datasets else case.datasets
        worst = None
        rows = 0
        for _ in range(max(1, args.repeat)):
            table, timing = timed_query(root, case.sql, datasets=datasets)
            rows = table.num_rows
            if worst is None or timing.total_s > worst.total_s:
                worst = timing
        assert worst is not None

        scans = pruning_report(root, case.sql, datasets=datasets)
        read = sum(s.files_scanned or 0 for s in scans)
        total = sum(s.files_total or 0 for s in scans)
        pruned = any(s.prunes for s in scans)
        files = f"{read}/{total}" if total else "unknown"

        expect_pruning = case.prunable and all(
            _expect_pruning(root, dataset, args.symbol, case.start_ms, case.end_ms)
            for dataset in case.datasets
        )
        verdict = []
        if not worst.within(args.budget):
            verdict.append("OVER BUDGET")
            failures.append(f"{case.name}: {worst.total_s:.3f} s > {args.budget:g} s")
        if expect_pruning and not pruned:
            verdict.append("NO PRUNING")
            failures.append(f"{case.name}: read {files} files, the predicate pruned nothing")
        elif not expect_pruning:
            verdict.append("whole dataset in range, nothing to prune")
        print(
            f"{case.name:<32} {rows:>10,} {worst.connect_s * 1000:>8.0f}ms "
            f"{worst.execute_s * 1000:>8.0f}ms {worst.total_s * 1000:>8.0f}ms  "
            f"{files:>12}  {', '.join(verdict) or 'ok'}"
        )

    print()
    if failures:
        for failure in failures:
            print(f"  FAIL {failure}")
        return 1
    print(
        f"All cases inside {args.budget:g} s. Every case whose predicate excludes a "
        f"partition that exists on disk pruned by path; the rest legitimately cover the "
        f"whole dataset and have nothing to discard."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
