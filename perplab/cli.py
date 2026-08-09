"""PerpLab command line entry point.

Spec 1.4 requires that no *normal workflow* needs a terminal. That is a v1 product goal
for strategy authoring, backtesting, and trading -- not a constraint on infrastructure.
Starting a long-lived recorder is an install-time operation, and putting it behind a UI
that must itself be running would make the collector less reliable, not more.

Commands:
    preflight            check the machine is fit for an unattended run
    snapshot-reference   write dated exchangeInfo + leverageBracket snapshots (Phase 0)
    collect              record market data until interrupted
    verify-bulk          re-verify bulk dataset availability (spec Appendix B)
    ingest               backfill bulk archives into the Parquet lake (spec 4.5)
    gaps                 report gaps in the lake, explained and unexplained (spec 4.5)
    manifest             compute, save or diff a dataset manifest (spec 4.6)
    query                run SQL against the lake views (spec 4.3, Phase 1 exit criterion)
    serve                run the API server and web UI (spec 2.3, Phase 3)

**`--root` is the userdata directory, and the lake is one level inside it.** Market data
lives at `<root>/market/` and reference snapshots at `<root>/reference/`, so every command
below except `manifest` passes `schemas.market_root(args.root)` to its module while
`manifest` gets `args.root` itself -- it is the only one that records both. Getting this
backwards does not raise; it silently finds an empty lake. `market_root` is the single
place the extra level is applied.

**Every `--start` / `--end` is an inclusive UTC date.** The underlying modules speak two
range conventions -- `ingest_range` takes inclusive archive periods, everything else takes
half-open epoch-millisecond ranges -- and exposing both at the command line would mean
`--end 2024-03-01` covering March 1st for one command and stopping at midnight before it
for another. One convention is applied here and converted at the boundary.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from perplab.data.collector import Collector
from perplab.data.ingest_bulk import DEFAULT_CONCURRENCY as INGEST_DEFAULT_CONCURRENCY
from perplab.data.reference import snapshot_reference
from perplab.data.schemas import market_root
from perplab.data.supervisor import supervise
from perplab.exchange.rest import PublicRestClient
from perplab.exchange.ws import PRODUCTION_WS, TESTNET_WS

API_DEFAULT_HOST = "127.0.0.1"
API_DEFAULT_PORT = 8756
"""Defaults duplicated from `perplab.api.app` rather than imported.

`cmd_serve` imports FastAPI lazily so the collector does not pay for it, and importing the
constants eagerly here would defeat that for the sake of two literals. The API module
asserts they agree (`tests/unit/test_api.py`), so they cannot drift silently."""

DEFAULT_ROOT = Path("userdata")
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_QUERY_BUDGET_S = 2.0
"""Spec 13's Phase 1 exit criterion: any symbol/range queryable in under two seconds.
`query --timing` fails the process when a query misses it, so the criterion can be checked
by a script rather than eyeballed."""

_MS_PER_DAY = 86_400_000

log = logging.getLogger("perplab")

_DESCRIPTION = "Binance USD-M perpetual futures research platform."

_EPILOG = """\
Conventions shared by every command below:

  --root      the userdata directory. Market data lives one level inside it, at
              <root>/market/, and reference snapshots at <root>/reference/.
  --start     inclusive UTC date, YYYY-MM-DD.
  --end       inclusive UTC date. --end 2026-08-01 covers the whole of that day.

Exit codes are meant to be used: `gaps` fails if any gap is unexplained,
`manifest --diff` fails if the lake moved since the saved run, and
`query --timing` fails if the query misses its budget.

Always run `ingest --dry-run` first. It reads only the local ledger, makes no
network calls, and one plausible command can otherwise pull tens of gigabytes.
"""
"""Help text for argparse, kept apart from `__doc__`.

`description=__doc__` was collapsing the module docstring's paragraphs into a single
justified wall, which got steadily less readable as the file grew. The docstring is for
whoever opens this file; this is for whoever runs `--help`, and the two audiences want
different things. `RawDescriptionHelpFormatter` keeps the layout below as written.
"""


# --------------------------------------------------------------------------- preflight


def _powercfg_ac_index(setting: str) -> int | None:
    """Read a power setting's AC index, or None if it cannot be determined.

    Returns None rather than raising: a preflight that crashes on an unexpected powercfg
    output format is worse than one that reports "unknown" and lets the operator check
    manually.
    """
    try:
        result = subprocess.run(
            ["powercfg", "/q", "SCHEME_CURRENT", "SUB_SLEEP", setting],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    match = re.search(
        r"Current AC Power Setting Index:\s*(0x[0-9a-fA-F]+)", result.stdout
    )
    return int(match.group(1), 16) if match else None


def cmd_preflight(args: argparse.Namespace) -> int:
    """Check the machine can survive an unattended run.

    A laptop that sleeps mid-run kills the process outright -- not a socket drop, a hard
    stop. The watchdog recovers after a reboot, but sleep and hibernate produce long
    silent gaps that look exactly like collector bugs and are not. Surfacing this at
    hour 0 is the difference between a 72-hour test that means something and one that
    quietly measured the machine's idle timeout.
    """
    root = Path(args.root)
    problems: list[str] = []
    warnings: list[str] = []

    print(f"python           : {sys.version.split()[0]}")
    print(f"data root        : {root.resolve()}")

    if sys.platform == "win32":
        standby = _powercfg_ac_index("STANDBYIDLE")
        hibernate = _powercfg_ac_index("HIBERNATEIDLE")

        for label, value in (("sleep (AC)", standby), ("hibernate (AC)", hibernate)):
            if value is None:
                print(f"{label:17}: UNKNOWN (could not read powercfg)")
                warnings.append(f"could not determine {label}")
            elif value == 0:
                print(f"{label:17}: disabled  OK")
            else:
                print(f"{label:17}: {value // 60} min  PROBLEM")
                problems.append(
                    f"{label} is {value // 60} min; the process will be killed mid-run"
                )
    else:
        print("power settings   : skipped (not Windows)")

    root.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(root)
    free_gb = usage.free / 1024**3
    print(f"disk free        : {free_gb:.1f} GB")
    # BTCUSDT across these five streams runs roughly 1-2 GB/day compressed. 20 GB is
    # about ten days of headroom -- enough to notice and act before it becomes a gap.
    if free_gb < 20:
        problems.append(f"only {free_gb:.1f} GB free; a full disk mid-run loses data")

    drift = _clock_drift_ms(args.testnet)
    if drift is None:
        print("clock drift      : UNKNOWN (could not reach Binance)")
        warnings.append("could not reach Binance to measure clock drift")
    else:
        print(f"clock drift      : {drift:+d} ms")
        # Spec 11 requires a warning past 1 s: signed requests are rejected outside
        # recvWindow, and drift also skews every recv_ms we record.
        if abs(drift) > 1000:
            problems.append(f"clock drift {drift} ms exceeds 1 s; NTP sync required")

    print()
    for problem in problems:
        print(f"  PROBLEM: {problem}")
    for warning in warnings:
        print(f"  WARNING: {warning}")

    if problems:
        print("\nPreflight FAILED. Fix the problems above before starting a long run.")
        print("  Disable sleep on AC:  powercfg /change standby-timeout-ac 0")
        print("                        powercfg /change hibernate-timeout-ac 0")
        return 1

    print("\nPreflight OK." if not warnings else "\nPreflight OK, with warnings.")
    return 0


def _clock_drift_ms(testnet: bool) -> int | None:
    """Local clock minus Binance server clock, in milliseconds."""

    async def measure() -> int:
        base = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
        async with PublicRestClient(base) as client:
            before = int(time.time() * 1000)
            server = await client.server_time_ms()
            after = int(time.time() * 1000)
            # Midpoint of the request window cancels most of the round-trip latency.
            return (before + after) // 2 - server

    try:
        return asyncio.run(measure())
    except Exception:  # noqa: BLE001 - preflight must never be the thing that fails
        return None


# ------------------------------------------------------------------------- reference


def cmd_snapshot_reference(args: argparse.Namespace) -> int:
    """Write today's reference snapshots (spec 3.2/3.6, Phase 0 exit criterion).

    Both kinds are attempted and reported separately. `leverageBracket` was skipped
    entirely until 2026-08-02 on the grounds that its documented endpoint is signed; it is
    also served unauthenticated (finding F3, now closed), so there is no longer any part of
    Phase 0 that needs credentials.
    """

    async def run() -> int:
        base = "https://testnet.binancefuture.com" if args.testnet else "https://fapi.binance.com"
        async with PublicRestClient(base) as client:
            results = await snapshot_reference(Path(args.root), client)

        for kind in ("exchangeInfo", "leverageBracket"):
            if kind in results:
                print(f"{kind} -> {results[kind]}")
            else:
                # Non-zero exit: a missing snapshot is a day of reference data that cannot
                # be recovered later, so it must be visible to whatever scheduled this.
                print(f"{kind}: FAILED -- see the log above", file=sys.stderr)

        return 0 if len(results) == 2 else 1

    return asyncio.run(run())


# --------------------------------------------------------------------------- collect


def cmd_collect(args: argparse.Namespace) -> int:
    root = Path(args.root) / "market"
    base_url = TESTNET_WS if args.testnet else PRODUCTION_WS

    async def run() -> int:
        stop = asyncio.Event()
        _install_signal_handlers(stop)

        collector = Collector(args.symbol, root, base_url=base_url)
        print(f"recording {collector.symbol} -> {root.resolve()}")
        for stream in collector.streams:
            print(f"  {stream}")
        print("Ctrl+C to stop.\n")

        # A short ticker keeps the Windows proactor loop responsive to Ctrl+C. Without
        # it the loop can sit blocked in a socket wait and ignore the signal until the
        # next message arrives -- which, on a quiet stream, could be a long time.
        ticker = asyncio.create_task(_tick(stop))
        try:
            if args.supervise:
                restarts = await supervise(lambda: collector.run(stop), stop)
                if restarts:
                    print(f"\ncollector restarted {restarts} time(s)")
            else:
                await collector.run(stop)
        finally:
            ticker.cancel()
        return 0

    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


async def _tick(stop: asyncio.Event) -> None:
    while not stop.is_set():
        await asyncio.sleep(0.2)


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """Set `stop` on SIGINT/SIGTERM so shutdown flushes buffers instead of dropping them.

    `loop.add_signal_handler` is not implemented on Windows, so this goes through
    `signal.signal` plus `call_soon_threadsafe`.
    """
    loop = asyncio.get_running_loop()

    def handler(_signum: int, _frame: object) -> None:
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handler)
        except (ValueError, OSError, AttributeError):
            pass  # not available on this platform/thread


# ------------------------------------------------------------------------ verify-bulk


def cmd_macro(args: argparse.Namespace) -> int:
    """Collect macro context (Phase 11).

    A separate process from `collect` on purpose: the market collector's job is to not
    miss a depth message over 72 hours, and two third-party HTTP endpoints on its event
    loop would put someone else's outage in that process. See `MacroService`.
    """
    from perplab.data.macro import MacroService

    root = Path(args.root) / "market"
    interval = args.interval_s
    service = MacroService(
        root,
        **(
            {}
            if interval is None
            else {"global_interval_s": interval, "fx_interval_s": interval}
        ),
    )

    async def run() -> int:
        if args.once:
            counts = await service.poll_once()
            for dataset in sorted(counts):
                print(f"{dataset}: {counts[dataset]} new row(s)")
            print(f"-> {root.resolve()}")
            # Zero rows is a legitimate answer, not a failure: the sources publish on
            # their own clocks and a poll landing between updates has nothing new to
            # record. Reported as the number it is rather than as an error.
            return 0

        stop = asyncio.Event()
        _install_signal_handlers(stop)
        print(f"collecting macro context -> {root.resolve()}")
        print("  macroGlobal  CoinGecko /global (BTC dominance, total market cap)")
        print("  macroFx      Yahoo DX-Y.NYB (ICE US Dollar Index)")
        print("Ctrl+C to stop.\n")
        ticker = asyncio.create_task(_tick(stop))
        try:
            await service.run(stop)
        finally:
            ticker.cancel()
        for dataset in sorted(service.counts):
            print(f"{dataset}: {service.counts[dataset]} row(s) this session")
        return 0

    return asyncio.run(run())


def cmd_verify_bulk(args: argparse.Namespace) -> int:
    from perplab.data.bulk_availability import report

    return report(args.symbol)


# ---------------------------------------------------------------------------- lake args


def _day_start_ms(date: str) -> int:
    """Midnight UTC on an inclusive `YYYY-MM-DD` bound.

    Routed through `bulk_layout.datetime_str_to_ms` rather than `datetime.strptime` for
    the reason that module gives: a naive `datetime` picks up the machine's local zone on
    `.timestamp()`, and a range that starts an hour early reads a partition that was never
    requested. It also rejects impossible dates such as `2026-02-31`, so a typo fails here
    rather than as an empty result.
    """
    from perplab.data.bulk_layout import datetime_str_to_ms

    return datetime_str_to_ms(f"{date} 00:00:00")


def _inclusive_range_ms(start: str, end: str) -> tuple[int, int]:
    """`--start`/`--end` inclusive dates -> the half-open `[start_ms, end_ms)` the modules take.

    The end date is widened by a day, because `--end 2024-03-01` means "through the first
    of March" everywhere in this CLI. Doing that conversion in one place is what keeps
    `ingest`, `gaps`, `manifest` and `query` covering the same days for the same arguments;
    when each command converted for itself, an off-by-one-day was a plausible way for a gap
    report to disagree with the ingest that produced the data.
    """
    start_ms = _day_start_ms(start)
    end_ms = _day_start_ms(end) + _MS_PER_DAY
    if end_ms <= start_ms:
        raise SystemExit(f"--end {end!r} precedes --start {start!r}")
    return start_ms, end_ms


# ---------------------------------------------------------------------------- ingest


def cmd_ingest(args: argparse.Namespace) -> int:
    """Backfill bulk archives (spec 4.5).

    `--dry-run` is not a convenience. One unremarkable command -- `--dataset bookTicker`
    over its published window -- is roughly 77 GB across 320 requests, and the operator
    should learn that before starting rather than when the disk fills at hour nine. The
    plan is computed from the local receipt ledger and the coverage table with no network
    traffic at all, so it is free to run and there is no reason not to.

    Datasets are ingested one after another rather than in parallel. `--workers` already
    parallelises within a dataset, and four concurrent transfers is the courteous ceiling
    for a free public mirror (see `DEFAULT_CONCURRENCY`); multiplying that by the number of
    datasets would be six times ruder for no extra throughput on any normal connection.
    """
    from perplab.data.ingest_bulk import (
        PHASE1_DATASETS,
        HttpFetcher,
        IngestStatus,
        TextProgress,
        format_bytes,
        ingest_range,
        plan_range,
        sweep_stale_downloads,
    )

    root = market_root(args.root)
    datasets = tuple(args.dataset) if args.dataset else PHASE1_DATASETS

    if args.dry_run:
        plans = [
            plan_range(root, args.symbol, dataset, args.start, args.end, force=args.force)
            for dataset in datasets
        ]
        for plan in plans:
            print(plan.render())
            print()

        files = sum(len(p.to_fetch) for p in plans)
        estimates = [p.estimated_bytes for p in plans]
        total = sum(e for e in estimates if e is not None)
        unknown = sum(1 for p, e in zip(plans, estimates) if e is None and p.to_fetch)
        print(
            f"TOTAL: {files} archive(s), ~{format_bytes(total)}"
            + (f" plus {unknown} dataset(s) of unknown size" if unknown else "")
        )
        print("Nothing was downloaded. Re-run without --dry-run to fetch.")
        _print_unavailable_note()
        return 0

    # Only matters after a hard kill or a power cut -- `ingest_archive` removes its own
    # temporary in a `finally`. Run first so a previous run's abandoned quarter-gigabyte
    # download is not still occupying the disk this one is about to need.
    swept = sweep_stale_downloads(root)
    if swept:
        print(f"swept {swept} abandoned download(s) from a previous run")

    exit_code = 0
    progress = TextProgress()
    with HttpFetcher() as fetcher:
        for dataset in datasets:
            report = ingest_range(
                root,
                args.symbol,
                dataset,
                args.start,
                args.end,
                fetcher=fetcher,
                concurrency=args.workers,
                force=args.force,
                progress=progress,
            )
            print()
            print(report.render())
            exit_code = max(exit_code, report.exit_code)
            if report.interrupted:
                # Stop the whole run, not just this dataset. Carrying on to the next one
                # after the user asked to stop is the behaviour that makes people reach
                # for the process manager.
                break
            if report.count(IngestStatus.WRITTEN):
                print(
                    f"  next: perplab gaps --symbol {args.symbol} "
                    f"--start {args.start} --end {args.end}"
                )

    _print_unavailable_note()
    return exit_code


def _print_unavailable_note() -> None:
    """Say what cannot be fetched, on every run, whether or not it was asked for.

    `liquidationSnapshot` is 404 at both the daily and monthly paths (finding F2) and is
    therefore absent from `PHASE1_DATASETS`. Absent and unmentioned would be
    indistinguishable from forgotten -- which is how the spec came to describe it as
    available in the first place -- so the registry keeps it with its caveat and this
    prints it. It is a note rather than a failure: making the default command exit non-zero
    every time would teach an operator to ignore the exit code, which is the one signal
    that says whether the backfill worked.
    """
    from perplab.data.bulk_layout import bulk_dataset

    entry = bulk_dataset("liquidationSnapshot")
    print(f"\nnote: {entry.name} is unavailable by design. {entry.caveat}")


# ------------------------------------------------------------------------------- gaps


def cmd_gaps(args: argparse.Namespace) -> int:
    """Run gap detection and print the report (spec 4.5).

    Exits non-zero when any gap is *unexplained*. That is Phase 1b's exit criterion stated
    as a process exit code -- an explained gap is still missing data and is still printed,
    but its cause is recorded in the collector's own event stream, and a run that halted
    for a known reboot is not the same as one that lost an hour for no reason anybody can
    name.

    The default checks every registered dataset, which on a bulk-only lake reports the
    collector-only ones as wholly missing. That is loud, and deliberately so: a report that
    quietly omitted them would be indistinguishable from one that found them healthy. Pass
    `--dataset` to narrow it.
    """
    from perplab.data.gaps import detect_gaps, render_report

    start_ms, end_ms = _inclusive_range_ms(args.start, args.end)

    # Never evaluate past the present. `--end` is an inclusive *date*, so asking about
    # today extends the range to tomorrow's midnight and the unrecorded remainder of the
    # day is reported as missing data -- an unexplained gap that grows as you ask earlier
    # in the day and vanishes tomorrow. Data for a time that has not happened yet cannot be
    # absent, and a report that always shows a scary UNEXPLAINED when run on the current
    # day is one the operator learns to ignore, which is the exact failure this whole
    # subsystem exists to prevent.
    #
    # Clamping only removes the future. A collector that died an hour ago still leaves a
    # real gap between its last row and now, and that is still reported.
    now_ms = int(time.time() * 1000)
    clamped = end_ms > now_ms
    if clamped:
        end_ms = now_ms
    if end_ms <= start_ms:
        print(
            f"nothing to evaluate: the requested range starts at or after the current "
            f"time ({args.start} .. {args.end})",
            file=sys.stderr,
        )
        return 0

    report = detect_gaps(
        market_root(args.root),
        args.symbol,
        start_ms,
        end_ms,
        datasets=tuple(args.dataset) if args.dataset else None,
    )
    print(render_report(report))
    if clamped:
        print(
            "\nnote: the range was clamped to the present; the remainder of "
            f"{args.end} has not happened yet and is not evaluated."
        )

    unexplained = report.unexplained
    if unexplained:
        print(
            f"\n{len(unexplained)} unexplained gap(s). Phase 1b's exit criterion is that "
            f"this is zero."
        )
        return 1
    print("\nNo unexplained gaps.")
    return 0


# --------------------------------------------------------------------------- manifest


def cmd_manifest(args: argparse.Namespace) -> int:
    """Compute a dataset manifest, optionally saving or diffing it (spec 4.6).

    Note that this is the one command handed `--root` itself rather than
    `market_root(--root)`: a manifest records the `reference/` snapshot a run validated its
    orders against as well as the market data it read, and both paths have to be relative
    to one base.

    `--gaps` runs detection first and folds the result in. That is not merely storage: an
    unexplained gap in a fill model's input dataset demotes the tier, because coverage is
    judged at partition granularity and a day holding one part-file counts as a covered day
    even if the collector was down for nine hours of it. Without `--gaps` the tier is the
    coverage-only answer, which is the honest one for a caller that has not looked.
    """
    from perplab.data.gaps import detect_gaps
    from perplab.data.manifest import build_manifest, diff_manifests, read_manifest, write_manifest

    start_ms, end_ms = _inclusive_range_ms(args.start, args.end)
    # Clamped for the same reason `cmd_gaps` clamps, and it matters more here. There the
    # unlived remainder of today produced a scary line in a report; here it is *acted on* --
    # `--gaps` feeds detection into `derive_fill_model_tier`, and phantom gaps over hours
    # that have not happened yet demote a run's fill model. A manifest built this morning
    # recorded `BAR_CLOSE` and `LOW_FIDELITY`, permanently, for a lake that was complete up
    # to the moment it was asked.
    now_ms = int(time.time() * 1000)
    clamped_end = min(end_ms, now_ms)
    symbols = args.symbol or [DEFAULT_SYMBOL]

    gaps = None
    if args.gaps:
        # One report per symbol: gap detection is scoped to a symbol, and a manifest may
        # name several. Concatenated rather than merged -- two symbols down for the same
        # hour is two gaps, because each has to be refetched separately.
        collected = []
        for symbol in symbols:
            if clamped_end > start_ms:
                collected.extend(
                    detect_gaps(market_root(args.root), symbol, start_ms, clamped_end).gaps
                )
        gaps = collected
        if clamped_end < end_ms:
            print(
                "note: gap detection was clamped to the present; the remainder of the "
                "requested range has not happened yet.",
                file=sys.stderr,
            )

    manifest = build_manifest(args.root, symbols, start_ms, end_ms, gaps=gaps)

    print(f"symbols          : {', '.join(manifest.symbols)}")
    print(f"range            : {args.start} .. {args.end} (inclusive)")
    print(f"fill_model_tier  : {manifest.fill_model_tier}")
    print(f"flags            : {', '.join(manifest.flags) or 'none'}")
    print(f"gaps recorded    : {len(manifest.gaps)}")
    print("datasets:")
    for key in sorted(manifest.datasets):
        entry = manifest.datasets[key]
        print(
            f"  {key:<20} {entry.files:>6} files  {entry.rows:>14,} rows  "
            f"{entry.sha256[:12]}"
        )
    if not manifest.datasets:
        print("  (none -- no published file overlaps this range)")

    exit_code = 0
    if args.diff:
        saved = read_manifest(args.diff)
        difference = diff_manifests(saved, manifest)
        print()
        print(difference.report())
        # Spec 4.6 requires a recomputation that differs to "warn loudly". A non-zero exit
        # is what makes that survive being run from a script rather than read by a person.
        exit_code = 1 if difference.changed else 0

    if args.out:
        print(f"\nwrote {write_manifest(manifest, args.out)}")

    return exit_code


# ------------------------------------------------------------------------------ query


def cmd_query(args: argparse.Namespace) -> int:
    """Run SQL against the lake views (spec 4.3).

    The views are the plain dataset names (`klines`, `aggTrades`, ...) carrying raw scaled
    int64, plus a `_unscaled` companion of each that divides by 10^8 into `DOUBLE` for
    reading. The scaled views are the ones to compute with; see `query.UNSCALED_SUFFIX` for
    why a `DOUBLE` from here must never reach the accounting layer.

    `--timing` reports connect and execute time separately and fails the process if their
    sum misses `--budget`. The split matters when it does miss: connect time is view
    construction globbing a lake with too many small files, execute time is a scan that
    should have pruned, and one total cannot tell them apart.
    """
    from perplab.data.query import query, timed_query

    root = market_root(args.root)
    datasets = tuple(args.dataset) if args.dataset else None

    if args.timing:
        table, timing = timed_query(root, args.sql, datasets=datasets)
    else:
        table, timing = query(root, args.sql, datasets=datasets), None

    print(_render_table(table, args.limit))

    if timing is None:
        return 0

    print(
        f"\n{timing.rows} row(s)  connect {timing.connect_s * 1000:.0f} ms  "
        f"execute {timing.execute_s * 1000:.0f} ms  total {timing.total_s * 1000:.0f} ms"
    )
    if timing.within(args.budget):
        print(f"within the {args.budget:g} s budget (spec 13, Phase 1 exit criterion)")
        return 0
    print(
        f"OVER the {args.budget:g} s budget. If connect dominates, the lake has too many "
        f"part-files or too many datasets are in scope -- narrow it with --dataset. If "
        f"execute dominates, the scan is not pruning; check the partition predicate."
    )
    return 1


def _render_table(table: Any, limit: int) -> str:
    """Render an Arrow table as fixed-width text, truncated to `limit` rows.

    Deliberately plain, and deliberately truncating rather than streaming: this is a
    diagnostic surface, and a query that returns two million rows to a terminal has
    answered a question nobody meant to ask. The row count printed is the true one, so a
    truncated display cannot be mistaken for the whole result.
    """
    names = table.column_names
    if not names:
        return "(no columns)"

    rows = table.slice(0, max(limit, 0)).to_pylist()
    cells = [[("" if r.get(n) is None else str(r.get(n))) for n in names] for r in rows]
    widths = [
        min(max([len(n)] + [len(row[i]) for row in cells]), 40) for i, n in enumerate(names)
    ]

    def line(values: list[str]) -> str:
        return "  ".join(v[: widths[i]].ljust(widths[i]) for i, v in enumerate(values))

    out = [line(list(names)), line(["-" * w for w in widths])]
    out.extend(line(row) for row in cells)
    if table.num_rows > len(rows):
        out.append(f"... {table.num_rows - len(rows)} more row(s) of {table.num_rows}")
    return "\n".join(out)


# ---------------------------------------------------------------------------- serve


def cmd_serve(args: argparse.Namespace) -> int:
    """Run the API server (spec 2.3, process 1).

    Imported lazily. `uvicorn` and `fastapi` pull in a substantial dependency tree, and
    every other command in this file -- including the collector, which has to start on a
    machine where the UI has never been used -- would otherwise pay for it at import time.
    """
    try:
        import uvicorn

        from perplab.api.app import create_app
    except ImportError as exc:  # pragma: no cover - depends on the install extras
        print(f"the API server needs fastapi and uvicorn: {exc}", file=sys.stderr)
        print("  pip install 'fastapi>=0.115' 'uvicorn[standard]>=0.30'", file=sys.stderr)
        return 1

    root = Path(args.root)
    try:
        app = create_app(root, host=args.host, password=args.password)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if app.state.exposed:
        # Not a log line. Spec 11 binds to loopback by default precisely because a
        # trading platform on an open interface is an incident, and the one thing worse
        # than exposing it is exposing it without noticing.
        print(
            f"WARNING: binding to {args.host} — PerpLab is reachable from the network.\n"
            "         A password is set, but the API keys this platform can hold are\n"
            "         worth more than a bearer token. Prefer an SSH tunnel.",
            file=sys.stderr,
        )

    print(f"PerpLab API on http://{args.host}:{args.port}  (docs at /api/docs)")
    if args.reload:
        # Reload needs an import string, because uvicorn re-imports the app in a fresh
        # process after every source change -- so the configured `app` built above cannot
        # be handed over. An earlier version passed the factory anyway, and uvicorn called
        # it with no arguments: the reloaded app took `create_app`'s defaults, which meant
        # `--root` was ignored, and `--password` produced an app with **no authentication
        # middleware at all** while still bound to the requested interface. The spec-11
        # check passed on the app that was then thrown away.
        #
        # The settings travel through the environment instead, which is what survives the
        # re-import. `app_from_env` reads them back and applies the same refusal.
        os.environ["PERPLAB_ROOT"] = str(root)
        os.environ["PERPLAB_HOST"] = args.host
        if args.password:
            os.environ["PERPLAB_PASSWORD"] = args.password
        else:
            os.environ.pop("PERPLAB_PASSWORD", None)

    uvicorn.run(
        "perplab.api.app:app_from_env" if args.reload else app,
        host=args.host,
        port=args.port,
        reload=args.reload,
        factory=args.reload,
        log_level="debug" if args.verbose else "info",
    )
    return 0


# ------------------------------------------------------------------------------ main


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="perplab",
        description=_DESCRIPTION,
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="data directory")
    parser.add_argument("--testnet", action="store_true", help="use Binance testnet")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("preflight", help="check the machine is fit for an unattended run")
    sub.add_parser(
        "snapshot-reference",
        help="write dated exchangeInfo + leverageBracket snapshots",
    )

    collect = sub.add_parser("collect", help="record market data until interrupted")
    collect.add_argument("--symbol", default=DEFAULT_SYMBOL)
    collect.add_argument(
        "--supervise",
        action="store_true",
        help="restart the collector in-process if it crashes",
    )

    macro = sub.add_parser(
        "macro", help="collect macro context: BTC dominance, total market cap, DXY"
    )
    macro.add_argument(
        "--once",
        action="store_true",
        help="poll every source once and exit, instead of running until interrupted",
    )
    macro.add_argument(
        "--interval-s",
        type=float,
        default=None,
        help="seconds between polls (default hourly; both sources move slowly)",
    )

    verify = sub.add_parser("verify-bulk", help="re-check bulk dataset availability")
    verify.add_argument("--symbol", default=DEFAULT_SYMBOL)

    ingest = sub.add_parser("ingest", help="backfill bulk archives into the lake")
    ingest.add_argument("--symbol", default=DEFAULT_SYMBOL)
    ingest.add_argument(
        "--dataset",
        action="append",
        metavar="NAME",
        help=(
            "repeatable; defaults to the Phase 1 set. Accepts either the archive name "
            "(fundingRate) or the lake name (funding)"
        ),
    )
    ingest.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    ingest.add_argument("--end", required=True, metavar="YYYY-MM-DD")
    ingest.add_argument(
        "--workers",
        type=int,
        default=INGEST_DEFAULT_CONCURRENCY,
        help="parallel downloads within one dataset",
    )
    ingest.add_argument(
        "--dry-run",
        action="store_true",
        help="list what would be fetched and roughly how many bytes; downloads nothing",
    )
    ingest.add_argument(
        "--force",
        action="store_true",
        help="re-fetch archives that already have a receipt",
    )

    gaps = sub.add_parser("gaps", help="report gaps in the lake")
    gaps.add_argument("--symbol", default=DEFAULT_SYMBOL)
    gaps.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    gaps.add_argument("--end", required=True, metavar="YYYY-MM-DD")
    gaps.add_argument(
        "--dataset",
        action="append",
        metavar="NAME",
        help="repeatable; defaults to every registered dataset, which is loud on purpose",
    )

    manifest = sub.add_parser("manifest", help="compute, save or diff a dataset manifest")
    manifest.add_argument("--symbol", action="append", metavar="SYMBOL")
    manifest.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    manifest.add_argument("--end", required=True, metavar="YYYY-MM-DD")
    manifest.add_argument("--out", metavar="PATH", help="write the manifest here")
    manifest.add_argument(
        "--diff",
        metavar="PATH",
        help="compare against a saved manifest; non-zero exit if it differs",
    )
    manifest.add_argument(
        "--gaps",
        action="store_true",
        help="run gap detection first and let it constrain the fill model tier",
    )

    serve = sub.add_parser("serve", help="run the API server and the web UI")
    serve.add_argument("--host", default=API_DEFAULT_HOST)
    serve.add_argument("--port", type=int, default=API_DEFAULT_PORT)
    serve.add_argument(
        "--password",
        default=None,
        help=(
            "bearer token required for every request. Mandatory when --host is not "
            "loopback (spec 11)"
        ),
    )
    serve.add_argument(
        "--reload", action="store_true", help="restart on source changes (development)"
    )

    query = sub.add_parser("query", help="run SQL against the lake views")
    query.add_argument("sql", help="SQL over the dataset views, e.g. 'SELECT * FROM klines'")
    query.add_argument(
        "--dataset",
        action="append",
        metavar="NAME",
        help="repeatable; build views for these datasets only, which speeds up connect",
    )
    query.add_argument("--limit", type=int, default=20, help="rows to display")
    query.add_argument(
        "--timing",
        action="store_true",
        help="measure the query against the <2 s Phase 1 exit criterion",
    )
    query.add_argument("--budget", type=float, default=DEFAULT_QUERY_BUDGET_S)

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # httpx logs one INFO line per request. That is useful for a one-shot command and
    # actively harmful for the collector, which issues about 1.5 requests a second once the
    # REST pollers are running (finding F4) -- roughly 390,000 lines and tens of megabytes
    # across a 72 h unattended run, burying every collector event that actually matters in
    # a log nobody can then read. Raised to WARNING unless --verbose was asked for, in
    # which case the operator has explicitly said they want the noise.
    #
    # **`serve` is the exception, and it stays at WARNING even under --verbose.** The API
    # process makes signed requests -- key validation on connect -- and httpx's per-request
    # line carries the complete URL, `signature=<hex>` included. A signature is valid for
    # the whole recvWindow, and spec 11 forbids credential material in a log line;
    # `exchange.signed._redact` exists precisely so no exception ever prints one, and an
    # opt-in verbosity flag must not become the one path that does.
    if not args.verbose or args.command == "serve":
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)

    handlers = {
        "preflight": cmd_preflight,
        "snapshot-reference": cmd_snapshot_reference,
        "collect": cmd_collect,
        "macro": cmd_macro,
        "verify-bulk": cmd_verify_bulk,
        "ingest": cmd_ingest,
        "gaps": cmd_gaps,
        "manifest": cmd_manifest,
        "query": cmd_query,
        "serve": cmd_serve,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
