# Resource policy and bounded replay

Implemented October 2026. Restart the backend and collectors through the PerpLab
launcher after installing this change. Existing processes retain their imported
code and Windows job limits until they exit. Settings can lower the budgets;
raising a previously lowered native ceiling requires every PerpLab process using
this checkout to exit and restart.

## Defaults

| Setting | Default | Meaning |
|---|---:|---|
| `resource_research_gib` | 6 GiB | Shared committed memory for backtests, Lab, shadow replay, backfills, CLI research, validation and heavy SQL |
| `resource_normal_gib` | 8 GiB | Backend pressure threshold: stop research first |
| `resource_hard_gib` | 10 GiB | Native hard committed-memory ceiling for backend descendants |
| `resource_reserve_gib` | 4 GiB | Minimum available physical system RAM |
| `resource_worker_gib` | 2 GiB | Research/session process allowance; ordinary workers also have a 2 GiB tree ceiling |
| `resource_research_workers` | 1 | Serial admission; at most two when explicitly enabled and headroom permits |
| `resource_spill_gib` | 32 GiB | Shared allowance for managed query spill, replay caches and query IPC |

GiB means 1,073,741,824 bytes. These are ceilings, not reservations of physical RAM.
Admission accounts for memory still promised to other research jobs, native
aggregate usage, backend headroom and system availability. A Lab parent can run
points in its own process; it falls back to serial execution when two nested
workers would hold reservations that cannot fit. The old CPU-count default is gone.

Windows Job Objects enforce the backend and research totals across processes.
Unnamed leaf jobs enforce worker limits and kill descendants on forced exit. Lab
and CLI launcher trees can contain nested workers within the 6 GiB research total;
each descendant still has its process limit. Validator processes have a 1 GiB
maximum. API and collector processes have 2 GiB process ceilings. The registry and
job names are tied to this checkout, so an alternate CLI data root cannot obtain
an independent budget. Heavy SQL launched by the API runs in a research child.
Expensive workers refuse to start if native enforcement cannot be established.
This implementation requires Windows for hard aggregate enforcement.

Committed memory is different from physical working-set usage. Windows allocations,
other applications and the frontend browser are outside the backend job. Available
physical RAM is monitored separately; the governor cannot guarantee that an
unrelated driver, application or hardware failure will never crash the PC.

## Queue, warnings and shutdown

The governor samples once a second. Queued workers keep heartbeats and publish why
they are waiting. At 80% of a process allowance, or the shared research allowance,
additional research launches pause. Research requests a controlled stop at 90%,
when the physical reserve is exhausted, or when backend/service pressure requires
headroom. A research process tree that does not stop within five seconds is killed.

Paper/live sessions reaching their threshold stop new orders and invoke the
existing risk halt: cancel outstanding orders, flatten only if
`kill_switch_flatten` is configured, and persist the kill switch before cleanup.
Research is stopped first under shared system pressure. Sessions get up to 30
seconds for cleanup; a forced shutdown records that cleanup is incomplete and
remaining exposure needs review. Final venue reconciliation determines whether a
live flatten was verified. A resource failure remains a failed run with an explicit
reason, even when partial artifacts were saved.

Settings shows aggregate usage, available RAM, enforcement and queue reasons.
Run and Lab detail panels show current/peak committed memory, allowance, execution
stage and termination reason. `GET /api/resources` exposes the same information.
Each new run records requested policy and effective native limits in its spec;
`resources.json` is the final diagnostic record. Lab summaries also record usage.
Memory limits, timeouts, native crashes and workers lost without results are
distinct reasons. A query refused under pressure returns HTTP 503 with a retry hint.

## Replay, caching and results

- Tick, quote, depth and mark reads use half-open hourly windows and 16,384-row
  Arrow batches. One continuous engine retains indicators, positions, pending
  orders, funding, source sequence numbers and receive-time ordering.
- Each process uses independent cursors on one DuckDB instance, with a shared
  512 MiB database allowance and two query threads. Native/Python allocations are
  still subject to the Windows process limit.
- Bar preparation uses UTC-aligned seven-day windows and reusable disk sequences.
  Timeframe buckets and warm-up carry their original semantics across boundaries.
- Gap and duplicate checks use bounded partitions with boundary state, plus
  file-fingerprint caches. Overlapping aggregate-trade ID ranges use an exact
  externally spilled fallback rather than assuming uniqueness.
- A Parquet footer catalog prunes replay files conservatively across both receive
  and exchange clocks. Unknown statistics fall back to reads. Coverage counts and
  bounds use metadata where exact; observed bar days use bounded clock reads.
- Fully consumed sorted windows and prepared bars are reusable caches. Original
  market archives and run fingerprints are preserved. Cache keys include source
  path, size and modification time, SQL and window parameters.
- Managed disk reservations divide temporary storage between native spill and
  cache/query output. Closed caches can be evicted. DuckDB spill directories are
  separate per process; abandoned spills and unfinished cache files are cleaned.
  Durable market data and saved run artifacts are outside this temporary-disk
  allowance. Disposable Lab point journals are removed after their compact result;
  temporary OOS/stitch files are removed after publication.
- Equity, completed trades, events, accounting journals and archived terminal
  orders are disk-backed. Recent messages/orders and active strategy state remain
  bounded. A single history record larger than 16 MiB is refused.
- The canonical event hash is updated incrementally using the same JSON bytes.
  Accounting reconciliation and full-resolution metrics read bounded histories.
  The small in-memory engine mode and readers for existing artifacts remain.
- Charts keep the 4,000-point ceiling; new runs publish a bounded price summary.
  Trades default to 200 per page, with full-archive sorting and a maximum page of
  2,000. CSV exports stream all trades. Active sessions publish a recent preview
  while their disk index retains the complete trade history.

No tick data is replaced by candles for a strategy requiring ticks. No GPU runtime
or dependencies were added. A later GPU experiment can target numerical Monte
Carlo/comparison workloads, with one worker, a 4 GiB VRAM cap, host buffers charged
to research, and seeded CPU/GPU parity and transfer-inclusive timing checks. The
Python strategy engine, Decimal accounting, API and exchange handling still use RAM.

## Verification and its limits

The complete regression run passed 2,654 tests in 540.63 seconds. After the final
policy-record and cleanup changes, 201 focused checks and all 26 resource tests
passed. TypeScript checking, the frontend production build, Python compilation
and the Git whitespace check also passed.

The resource regression tests cover native allocation refusal, descendant totals,
admission/queue heartbeats, fail-closed startup, controlled/forced stop, configured
paper kill policy, exact event-hash/accounting/fill/funding equivalence across
hourly windows, prepared-bar boundaries, source-cache invalidation, temporary-disk
reservations, Lab history cleanup, pagination and bounded query output. Run:

```powershell
.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

The frontend production build runs TypeScript checking and Vite compilation.
Repeatable memory probes are in `scripts/resource_soak.py`; they place a report,
one-second memory samples and the resource diagnostic under `userdata/_benchmarks`.

```powershell
.venv\Scripts\python.exe scripts/resource_soak.py --history-rows 1000000
.venv\Scripts\python.exe scripts/resource_soak.py --start 2026-08-01T00:00:00Z --end 2026-08-01T03:00:00Z --timeout 600
```

Measured on this PC:

| Probe | Result |
|---|---|
| 1,000,000 event/equity samples, storage and finalization | About 38 MB committed during writing, no growth after the first quarter; about 57 MiB peak through finalization; 2,664 chart points |
| Real BTCUSDT CVD indicator, three hours of trade-fidelity replay | 44,341 ticks, 182 bars including warm-up; 39.52 seconds; 279.32 MiB peak committed memory |

Reports: `userdata/_benchmarks/36c71808a6304d81a66472afbdba29dd/report.json`
and `userdata/_benchmarks/e760f9c1814a4551a26b72366f3c67c1/report.json`.
The first probe is a history-storage soak, **not a full multi-year market replay**.
The CVD probe calculates the indicator without placing orders; it establishes tick
processing and memory behavior, not strategy profitability or exchange execution.
It reported no funding settlement inside that three-hour window.

Full month/year CVD replays, a multi-year complete replay, a long paper session and
a real-venue live cleanup soak have not been verified by these short probes.
Use the same replay harness with the desired UTC range and a suitable timeout;
the governor applies to preparation, replay and finalization throughout.

## BTCUSDT inventory from the October audit

28.82 GiB (30.95 GB decimal) on disk. All dates below are UTC; this change does not
refresh or download market archives.

| Data | Rows | Coverage |
|---|---:|---|
| Aggregate trades | 3,384,196,056 | 2019-12-31 to 2026-08-05 |
| 1-minute candles | 3,467,520 | 2019-12-31 to 2026-08-03 |
| 1-minute mark-price candles | 3,469,627 | 2019-12-23 to 2026-08-03 |
| Funding settlements | 7,212 | 2020-01-01 to 2026-07-31 |
| Best bid/ask quotes | 155,720,918 | 2024-03-24 to 30 and 2026-08-01 to 05 |
| 20-level book snapshots | 304,938 | 2026-08-01 to 05 |
| Recorded mark-price updates | 284,086 | 2026-08-02 to 05 |
| Open interest/positioning metrics | 535,533 | 2020-09-01 to 2026-08-05; 563 missing days |

BTC minute candles had no missing or duplicate minutes across their recorded range.
Mark candles had six missing early days and three later gaps totaling 55 minutes.
Trade files covering every day do not prove uninterrupted or unique tick coverage.
Book snapshots retain the best 20 levels at approximately one snapshot per second.
Long history generally supports trade fidelity; quotes cover 12 days and book walking
five partial recording days. There is no individual-order/queue-level historical feed.
The stored data is stale relative to October 2026.
