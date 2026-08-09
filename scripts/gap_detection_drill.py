"""Run the corrupted-sample drill against the real ingested lake.

`tests/integration/test_corrupted_sample.py` runs the same injections against a five-day
sample this repo generates, so the drill is exercised on every `pytest` run without
needing a backfill on disk. This script points the identical harness at real BTCUSDT
archives, which answers a question the generated sample cannot: whether the detector says
the same things about data Binance actually published -- with its real file sizes, real
row counts and whatever oddities six and a half years of an exchange's history contains.

It reads the real lake and writes only to a scratch directory, and it checks that claim
rather than asserting it: the source range is fingerprinted by spec 4.6's own
`(path, size, mtime_ns)` rule before and after, and a difference fails the run.

    .venv/Scripts/python.exe scripts/gap_detection_drill.py --start 2026-06-01 --end 2026-06-30

Exit code is 0 only if every injection produced exactly the report it was supposed to and
the source came back byte-identical. Pick a range whose control run is clean: the drill
compares against exactly the injected damage, so a pre-existing real gap in the chosen
month shows up as a phantom against every injection and the six resulting failures all
point at the detector rather than at the range. `perplab gaps` over the candidate range
first is the cheap way to choose one.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

# Importable both as `python scripts/gap_detection_drill.py` from the repo root and as a
# module. The harness lives under `tests/` because it is test scaffolding rather than
# product code, and duplicating it here so the script could stand alone would give the
# repo two drills that could disagree about what a correct report is.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from perplab.data.bulk_layout import datetime_str_to_ms  # noqa: E402
from perplab.data.schemas import market_root  # noqa: E402
from tests.integration.corruption import DRILL_DATASETS, DrillPlan, run_drill  # noqa: E402

_MS_PER_DAY = 86_400_000


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="gap_detection_drill",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--root", default="userdata", help="the userdata directory")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    parser.add_argument(
        "--end",
        required=True,
        metavar="YYYY-MM-DD",
        help="inclusive, matching every other --end in this project",
    )
    parser.add_argument(
        "--workspace",
        metavar="PATH",
        help="where the scratch lakes go; a temporary directory by default, removed after",
    )
    args = parser.parse_args(argv)

    start_ms = datetime_str_to_ms(f"{args.start} 00:00:00")
    end_ms = datetime_str_to_ms(f"{args.end} 00:00:00") + _MS_PER_DAY
    if end_ms <= start_ms:
        raise SystemExit(f"--end {args.end!r} precedes --start {args.start!r}")

    source = market_root(args.root)
    if not source.is_dir():
        raise SystemExit(f"no lake at {source}; ingest a range before drilling it")

    owned = args.workspace is None
    workspace = Path(args.workspace) if args.workspace else Path(tempfile.mkdtemp(prefix="perplab-drill-"))
    plan = DrillPlan(symbol=args.symbol, start_ms=start_ms, end_ms=end_ms)
    try:
        report = run_drill(source, workspace, plan)
    finally:
        if owned:
            # Best effort: a scratch lake left behind after a crash is untidy, not unsafe,
            # and failing the run over a locked file would hide the drill's own verdict.
            shutil.rmtree(workspace, ignore_errors=True)

    print(report.render())
    print(f"\ndatasets drilled: {', '.join(DRILL_DATASETS)}")
    if report.ok:
        print("Drill PASSED -- the report named exactly the injected damage, no more.")
        return 0
    print(
        "Drill FAILED. A missed gap and a phantom gap are equally disqualifying; if the "
        "control line above is not clean, the chosen range already had a real gap and "
        "every other line is measuring that instead."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
