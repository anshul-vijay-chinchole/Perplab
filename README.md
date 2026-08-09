# PerpLab

Code-first algorithmic trading platform for Binance USDⓈ-M perpetual futures.
Single user, single exchange, local-first. See [PERPLAB_SPEC.md](PERPLAB_SPEC.md) for the
authoritative specification — this README covers only what is built so far.

## Status

**Phases 0 through 7 and 9 through 11 — built and verified.** Phase 8 (live trading
against a real account) is deferred by explicit decision until the exchange account
exists; 9, 10 and 11 were built ahead of it because none of the Lab, the UI or the macro
collectors touches an exchange. Two wall-clock criteria are running rather than
outstanding work: Phase 1b's 72-hour collector soak, and Phase 7's 48-hour papertrade.

| Phase | Scope | State |
|---|---|---|
| 0 | Types, money seam, exchange clients, filters, reference snapshots | ✅ |
| 1b | Collector: market data → Parquet, heartbeat, crash recovery | ⏳ 72 h clock running — see below |
| 1 | Bulk ingestion, checksum verify, gap detection, manifest, DuckDB views | ✅ with caveats |
| 2 | Accounting core (§3): positions, fills, fees, funding, margin, liquidation, invariants | ✅ |
| 3 | Strategy API, indicators, validator, Monaco editor, library CRUD/versioning | ✅ |
| 4 | Backtest engine: event loop, ordering, no-look-ahead, market orders, metrics, results page | ✅ |
| 5 | Realism: tick fills, limit queue, partial fills, stops/TP/trailing, depth walking, tier degradation | ✅ |
| 6 | Risk limits, kill switch, invariant auto-triggers, order amendment, auto-flatten | ✅ |
| 7 | Papertrading, session tape, shadow backtest + parity report, live monitor, Feed tab, kill switch | ⏳ 48 h clock — mechanism proven end to end |
| 8 | Live trading, key session flow, exchange reconciliation | ⏸ deferred until the account exists — code-complete, unverified |
| 9 | The Lab: walk-forward, Monte Carlo, regimes, overfitting diagnostics, portfolio, comparison | ✅ |
| 10 | UX: Dashboard, Settings, Coverage + Exchange Connection panels, ⌘K palette, CSV/PNG exports | ✅ |
| 11 | Macro context: BTC dominance + total market cap, DXY, cross-coin; `ctx.macro()` | ✅ |

Per-phase evidence is in [docs/PHASE_SIGNOFF.md](docs/PHASE_SIGNOFF.md), including the
mutation-testing numbers each phase was signed off against — Phase 7's were 40 deliberate
breakages, 39 killed and 1 proved unkillable; Phase 9's were 18 breakages, 16 killed
outright and 2 that exposed genuine test holes, both closed. Phases 9–11 also went through
an adversarial review whose findings, and the fixes for them, are recorded there: four of
them were the platform *asserting* something untrue, including an armed kill-switch badge
that had never rendered.

Phase 7's exit criterion is *"a strategy papertrades 48 h; parity report shows <5% PnL
divergence vs shadow backtest"* (spec §13). The 48 hours is wall time; the mechanism it
measures is built and was demonstrated end to end against the real Binance testnet — a
five-minute session traded 5 fills through the API and its own worker process, sealed its
tape, and its shadow backtest reproduced **the same event-log hash, the same net PnL to
twenty decimal places, and 0.00 bps average fill deviation**. See
[Papertrading](#papertrading-phase-7) below.

Phase 6's exit criterion is *"Every limit demonstrably halts a deliberately misbehaving
test strategy"* (spec §13). All nine of §7's limits have one, plus the invariant and
repeated-rejection auto-triggers. See [Risk limits](#risk-limits-phase-6) below.

Phase 5's exit criterion is *"All golden scenarios reproduce; fill tier correctly degrades
and is surfaced in the UI"* (spec §13), and both clauses are met — 32 golden scenarios, every
expected number derived in the test's own docstring from prices the test wrote, and a tier
that is resolved from the lake rather than accepted from the caller, recorded on the run row
and shown as a badge beside every result. See [Fill tiers](#fill-tiers-phase-5) below.

Phase 4's exit criterion is *"EMA cross backtests on 1 year of BTCUSDT; deterministic across
two runs; look-ahead test passes"* (spec §13), and all three are met — 35 081 bars in 20.8 s,
two runs in separate worker processes producing the same event-log SHA-256, and a truncated
run that is an exact prefix of the full one. See
[Backtesting](#backtesting-phase-4) below.

Phase 3's exit criterion is *"write, save, validate, and version a strategy entirely
in-browser"* (spec §13), and it is met: `perplab serve`, open `http://127.0.0.1:8756`, press
**New**. The editor is Monaco with server-side diagnostics in the gutter; every save runs the
six-stage validator of §5.5 and writes an immutable version row. See
[Strategy authoring](#strategy-authoring-phase-3) below.

Two of the six validation stages are worth calling out, because they are the ones that catch
bugs nothing else does. The **look-ahead test** of §12.3 runs against the whole indicator
library in CI: every indicator is fed 400 bars and then 300, and the shorter run's values
must equal the longer run's prefix exactly. It has a negative control — a centred moving
average written the way one has to be written incrementally — so the test is known to be
able to fail. The **determinism probe** re-runs the smoke test in separate interpreters under
different `PYTHONHASHSEED` values; running it twice in one process would prove only that a
process is deterministic, and the failure §5.5 names ("usually a set/dict iteration order")
is invisible without varying the seed. It uses six seeds rather than one, because whether
two interpreters order a *given* small set differently is close to a coin flip —
`{"long", "short"}`, about the most likely thing a strategy iterates, is one of the pairs
seeds 0 and 1 happen to agree on. A static rule flags set iteration directly as well, since
neither check alone is airtight.

Phase 2's exit criterion is *"§3.9 worked example reproduces exactly; all property tests
green"* (spec §13). Both are met — `tests/golden/test_worked_example.py` walks t₀ through t₅
on a single `Account` and closes with the spec's own reconciliation, and the Hypothesis
suite drives random fill/funding/mark sequences against all nine §3.10 invariants. Building
it surfaced two places where the spec contradicts itself; both are resolved and recorded in
[docs/ACCOUNTING_NOTES.md](docs/ACCOUNTING_NOTES.md).

The accounting core went through two adversarial review rounds. The first found seven
defects in code that already passed 874 tests and reproduced §3.9 exactly — every one of
them in behaviour the spec supplies no number for. The second found that six of the *fixes*
were unpinned: the code was correct and the tests did not prove it, demonstrated by mutating
the source and watching the suite stay green. All are now pinned, verified by putting each
defect back and watching a test fail.

Phase 1b is ⏳ rather than ✅ because its criterion is a 72-hour measurement, not a build
step. Everything blocking it is fixed and the clock is running — **restarted on 2026-08-02**,
because the platform review found a data-loss defect in the collector and seventy-two hours
of running on that code would have measured the wrong thing. See
[docs/PHASE_SIGNOFF.md](docs/PHASE_SIGNOFF.md).

The blocker was that `aggTrade`, `markPrice` and `!forceOrder@arr` delivered **zero frames**
from the production WebSocket endpoint while `depth20` and `bookTicker` flowed normally. A
sixteen-stream sweep found the rule: **every raw per-event stream works and every aggregated
or computed one is silent.** The server ACKs the dead subscriptions and lists them back
under `LIST_SUBSCRIPTIONS`, and a combined subscription delivers `bookTicker` but not
`aggTrade` *over one TLS connection* — which rules out anything on the network path, since
nothing in between can read inside that stream to drop messages selectively.

Mark price and aggregate trades are now sourced over REST, verified gapless: 7005 aggregate
trades collected with **zero id holes and zero duplicates**, and mark price at a genuine
1.00 samples/second. `liquidations` has no source at all — the stream is suppressed *and*
`GET /fapi/v1/allForceOrders` returns 404 — so the collector records one `UNAVAILABLE` event
per run and the gap detector accounts for the silence from data rather than from someone
remembering why. That is a narrowing of Phase 1b's declared scope and is written up as one.

See finding F4 in [docs/DATA_AVAILABILITY.md](docs/DATA_AVAILABILITY.md).

**One manual step remains before the 72 h claim is honest:** sleep is still enabled on AC
power (120 min), so the most likely cause of a broken run is not the collector.

```bash
powercfg /change standby-timeout-ac 0
```

Phase 1's exit criterion is "any symbol/range queryable in under 2 s, and the gap report is
accurate on a deliberately corrupted sample" (spec §13). Both are met. The slowest query
observed anywhere was **1124 ms against the full 6.5-year history** — 3,463,200 1-minute
bars across 2405 files — leaving roughly 1.8× headroom on the largest range the lake can
express. The corrupted-sample drill injects six kinds of damage and checks the report in
both directions: a missed gap and an invented gap both fail it.

Three things sit *outside* that criterion and should not be read as verified. See
**Caveats** below.

### What is actually in the lake

Backfilled for BTCUSDT on 2026-08-02. This table is what is on disk, not what is available
— see [docs/DATA_AVAILABILITY.md](docs/DATA_AVAILABILITY.md) for the difference.

| Dataset | Range on disk | Files | Rows | Size | |
|---|---|---|---|---|---|
| `klines` 1m | 2019-12-31 → 2026-07-31 | 2405 | 3,463,200 | 208 MB | complete, zero gaps |
| `markPriceKlines` 1m | 2019-12-23 → 2026-07-31 | 2357 | 3,393,307 | 111 MB | 56 days never published (F7) |
| `funding` | 2020-01 → 2026-07 | 79 | 7,212 | 152 KB | complete, zero missed settlements |
| `metrics` | 2020-09-01 → 2026-07-31 | 1597 | 534,563 | 21 MB | **563 days missing** (F8) |
| `aggTrades` | 14 days, two windows | 14 | 18,041,950 | 144 MB | **sample only** |
| `bookTicker` | 2024-03-24 → 2024-03-30 | 7 | 91,449,747 | 558 MB | **sample only** |
| `liquidationSnapshot` | — | 0 | 0 | — | not published anywhere (F2) |
| `bookDepth` | — | 0 | 0 | — | available, never ingested |

**Read the last two columns before planning anything on this.**

- **`aggTrades` and `bookTicker` are seven-day samples, not history.** `aggTrades` holds
  2026-07-13..19 and 2024-03-24..30; `bookTicker` holds 2024-03-24..30, the last seven days
  Binance ever published. That is 14 days out of 2405 available for `aggTrades`, and 7 out
  of 320 for `bookTicker`. They exist to exercise the `TRADE_ONLY` and `BOOK_TICKER` fill
  tiers against real data, not to back a backtest. A full `aggTrades` history is about
  6–8 hours and 24 GB; a full `bookTicker` window is about 6 hours and 25 GB. Neither was
  attempted — see [docs/INGESTION.md](docs/INGESTION.md).
- **`metrics` is missing 19 months.** 563 of 2160 days fail to parse because two ratio
  columns are published beyond the lake's 8-decimal precision. `to_scaled` refused to round
  them and nothing was written, which is correct behaviour with a real cost. Finding F8.
- The collector's own datasets (`depth20`, `markPrice`, `liquidations`, `collectorEvents`,
  and `bookTicker`/`aggTrades` from today forward) accumulate only while it runs.

## Why the collector was built first

Bulk L2 depth is not downloadable (spec §4.2 / R1), so **depth history exists only from
the moment you start recording**. Every day the collector is not running is a permanent
hole in the L2 backtest window. No other component has that property — the accounting
engine written next month is exactly as good as one written today.

Verification on 2026-08-01 found the same is now true of `bookTicker`: Binance published
it in bulk only between 2023-05-16 and 2024-03-30, then stopped. Spec §4.2 assumed full
history and built the fill model on it. It is now recorded live too.
See [docs/DATA_AVAILABILITY.md](docs/DATA_AVAILABILITY.md).

## Setup

```bash
python -m venv .venv
.venv/Scripts/python.exe -m pip install -e ".[dev]"
```

Requires Python 3.11+ (developed on 3.14). No API keys — every stream used so far is
public.

The web UI needs Node 18+ and one build:

```bash
cd frontend && npm install && npm run build
```

`perplab serve` picks up `frontend/dist` if it is there and explains how to build it if it
is not. Monaco is bundled locally rather than fetched from a CDN — a tool that binds to
`127.0.0.1` should not stop working when the network does, and the page holding the code
that trades your money should not be executing a script fetched from someone else's host.

### Opening the platform with one click

`perplab serve` hosts the API *and* the built UI on `http://127.0.0.1:8756`, so the whole
platform is one process plus a browser window. The launcher automates exactly that:

```bash
powershell -ExecutionPolicy Bypass -File scripts/launcher/install_shortcut.ps1
```

creates a **PerpLab** shortcut on the desktop. Double-clicking it starts the server
hidden (idempotent — a second click just opens the window), waits for `/api/health`,
and opens an app-mode Edge window with no address bar. Server output lands in
`userdata/logs/serve.log`. `stop-platform.bat` stops the server — found by its port,
never by process name, so a running collector is untouched. After editing frontend
code, re-run `npm run build`; the server picks up the new `dist` without restarting.

## Usage

Check the machine can survive an unattended run:

```bash
.venv/Scripts/python.exe -m perplab preflight
```

Snapshot exchange reference data (Phase 0 exit criterion):

```bash
.venv/Scripts/python.exe -m perplab snapshot-reference
```

Start recording:

```bash
.venv/Scripts/python.exe -m perplab collect --symbol BTCUSDT --supervise
```

Re-verify what Binance actually publishes in bulk:

```bash
.venv/Scripts/python.exe -m perplab verify-bulk
```

For unattended operation, review then run `scripts/install_watchdog.ps1` — it registers a
Scheduled Task that relaunches the collector after a crash or reboot.

### Backfilling and reading the lake (Phase 1)

[docs/INGESTION.md](docs/INGESTION.md) is the operator's guide — costs, timings, resuming,
reading a gap report. The short version:

Always plan a backfill before starting one. `--dry-run` reads the local receipt ledger
only, makes no network calls, and prints what would be fetched and roughly how large it is:

```bash
.venv/Scripts/python.exe -m perplab ingest --dry-run --symbol BTCUSDT \
    --dataset bookTicker --start 2023-05-16 --end 2024-03-30
```
```
bookTicker BTCUSDT: 313 of 320 periods to fetch, ~70.0 GB
  7 already ingested (receipt + Parquet on disk); --force would re-fetch them
  first: 2023-05-16, 2023-05-17, 2023-05-18, 2023-05-19, 2023-05-20 ...
  caveat: Published only 2023-05-16 .. 2024-03-30, then discontinued -- see ...

TOTAL: 313 archive(s), ~70.0 GB
Nothing was downloaded. Re-run without --dry-run to fetch.
```

That estimate is deliberately high (finding F12 — the measured figure is nearer 43 GB),
because over-estimating a disk warning is the safe direction. Either way it is the number
to see at second zero rather than at hour nine.

Drop `--dry-run` to fetch. Every archive's `.CHECKSUM` is fetched *before* the archive and
is the only thing that unlocks parsing, and completion is recorded per archive, so an
interrupted run resumes rather than restarting:

```bash
.venv/Scripts/python.exe -m perplab ingest --symbol BTCUSDT \
    --dataset klines --start 2019-12-31 --end 2026-07-31
```

With no `--dataset` the six Phase 1 datasets are fetched in turn. Everything except the two
tick datasets — klines, markPriceKlines, fundingRate and metrics, 7057 archives covering
6.5 years — took **23 minutes, 250 MB downloaded, 340 MB on disk**. The tick datasets are a
different order of magnitude; see [docs/INGESTION.md](docs/INGESTION.md) before starting one.

Then check what is missing, record what was read, and query it:

```bash
.venv/Scripts/python.exe -m perplab gaps --symbol BTCUSDT \
    --dataset klines --dataset funding --start 2019-12-31 --end 2026-07-31

.venv/Scripts/python.exe -m perplab manifest --symbol BTCUSDT \
    --start 2019-12-31 --end 2026-07-31 --gaps --out run-manifest.json

.venv/Scripts/python.exe -m perplab query --timing \
    "SELECT year, month, count(*) AS bars FROM klines WHERE year='2025' GROUP BY 1,2 ORDER BY 1,2"
```
```
year  month  bars
----  -----  -----
2025  01     44640
2025  02     40320
...
12 row(s)  connect 457 ms  execute 106 ms  total 562 ms
within the 2 s budget (spec 13, Phase 1 exit criterion)
```

Constrain the Hive partition columns (`year`/`month` for klines, `date` for the tick
datasets) and the scan opens only the files it needs. Connect time is the view glob;
execute time is the scan. Both are reported separately so a future breach is diagnosable
rather than guessable.

`query` exposes each dataset as a view carrying raw scaled int64, plus a `_unscaled`
companion that divides by 10⁸ into `DOUBLE` for reading. Compute on the scaled views; a
`DOUBLE` from the unscaled ones must never reach the accounting layer.

```bash
.venv/Scripts/python.exe -m perplab query \
    "SELECT open_time, close FROM klines_unscaled ORDER BY open_time DESC LIMIT 3"
```
```
open_time      close
-------------  -------
1785542340000  62859.9
```

`gaps` exits non-zero if any gap is *unexplained*, `manifest --diff` exits non-zero if the
lake has moved since a saved run, and `query --timing` exits non-zero if the query misses
the 2 s Phase 1 budget — so all three work from a script as well as from a terminal.

`--start` and `--end` are inclusive UTC dates on every command. `--root` is the userdata
directory; market data lives one level inside it at `<root>/market/`.

### Strategy authoring (Phase 3)

Build the UI once, then start the server:

```bash
cd frontend && npm install && npm run build
```

```bash
.venv/Scripts/python.exe -m perplab serve
```

Open `http://127.0.0.1:8756`. The server binds to loopback; binding anywhere else requires
`--password` and the command refuses without one (spec §11).

A strategy is one file with one class:

```python
from perplab import Strategy


class EMACross(Strategy):
    params = {
        "fast": {"type": "int", "default": 12, "min": 2, "max": 200},
        "risk": {"type": "decimal", "default": "0.01", "min": "0.001", "max": "0.05"},
    }
    requires = {"symbols": ["BTCUSDT"], "timeframe": "15m", "history": 26}

    def on_start(self, ctx):
        self.fast = ctx.indicators.ema(self.p.fast)
        self.slow = ctx.indicators.ema(26)

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return
        if self.fast.crossed_above(self.slow) and ctx.position().is_flat:
            ctx.buy(qty=ctx.risk.size_by_notional(ctx.account.equity / 10))
        elif self.fast.crossed_below(self.slow) and ctx.position().is_long:
            ctx.close()
```

Three things about that snippet are enforced rather than advised:

- **Decimal params are declared as strings.** `0.01` written as a Python float is already a
  different number, and that number then multiplies a notional. The validator refuses it
  where the literal still has a line number.
- **Indicators are built in `on_start`.** That is where they register with the run, and it is
  where the engine derives the warm-up length from — cross-checked against
  `requires["history"]`, so a 200-period EMA cannot produce signals from twelve bars.
- **`ctx.buy` raises during warm-up.** The gate is in the context, not in the strategy. A
  guarantee that depends on the author remembering `if not ctx.warm` is not a guarantee.

Every save runs six stages (§5.5) and shows failures in the Monaco gutter, never as a
terminal traceback: parse → static scan → structure → params → a 500-bar smoke run in a
sandboxed worker under a 10 s timeout → a determinism probe. **The API server never executes
strategy code**; validation shells out to `perplab.strategy.sandbox`, so an unbounded loop
costs one worker rather than the editor you would fix it in.

A save with validation errors is still stored. Spec §5.6 asks for a version row per save; a
version that records `valid = 0` and carries its diagnostics is better than losing an
author's half-finished work, and the Runs tab refuses to backtest one. A save whose code is
byte-identical to the current head returns that head instead — see
[Known deviations](#known-deviations-from-the-spec).

### Backtesting (Phase 4)

Press **Backtest** in the editor, or **New backtest** on the Runs tab. A run replays real
`klines`, `markPriceKlines` and `funding` through the same accounting engine live trading
will use, in a **separate worker process** — spec §2.3's isolation rule, which is what makes
a strategy with an accidental `while True:` cost one cancelled run rather than the server.

Three things decide whether the numbers mean anything, and all three are visible on the
results page rather than buried:

**Which print a fill takes.** A 1-minute kline dates exactly two prices: the `open` is the
bar's first trade and the `close` is its last. The `high` and `low` certainly happened, but
nothing says *when*, so neither can price a fill without inventing a timestamp. The rule is
therefore *a market order arriving at `T` fills at the most recent print at or before `T`* —
which, for an order submitted at a bar close with any non-zero latency, is the next bar's
open. Filling at the close of the bar containing the arrival would use a price nobody knew
until the bar was over.

**Latency is not zero by default.** With zero latency an order arrives at the same instant it
was decided and fills at the very print the strategy just looked at. The two prices are
usually close — a bar closes at `…:59.999` and the next opens a millisecond later — so at
this tier the gap is one of *causality* rather than magnitude. `FixedLatency(0)` is available
and flags the run `ZERO_LATENCY`.

**Slippage is an assumption, and is labelled as one.** At `BAR_CLOSE` the default 1 bp is far
worse than BTCUSDT's real half-spread (a 0.1 tick on a 60 000 price is about 0.008 bp). It is
not standing in for the spread; it prices the *timing* uncertainty that tier cannot resolve.
The three higher tiers replace it with something derived from data — see below.

The results page carries the equity curve with drawdown shaded beneath it, price with trade
markers, the §8.2 metric set, the §8.4 attribution bar, a sortable trade table with MAE/MFE
and CSV export, and the raw event log. Beside the headline Sharpe is the §8.5 trials counter:
after 500 parameter combinations, the best Sharpe is upward-biased by about `√(2 ln 500) ≈
3.5` standard deviations under the null, and a Sharpe with no `N` beside it invites exactly
that mistake.

Drawdown is measured on **every** mark-to-market tick, not on grid closes — a year-long run
produced 527 313 of them — and each sample carries the *band* the mark traversed inside its
minute, so an excursion that recovered before the close is still scored. The chart is
downsampled by keeping each bucket's minimum and maximum plus the first sample, the last, and
the drawdown trough, so the shaded panel and the "Max drawdown" card are the same number
rather than two that nearly agree.

### Fill tiers (Phase 5)

A run's **fill tier** decides what its numbers are worth, so it is derived from the lake and
never accepted from the caller. Spec §4.2's table, and what each tier can execute:

| Tier | Input | Market order fills at | Limit + TIF | Stop / TP / trailing |
|---|---|---|---|---|
| `BOOK_WALK` | `depth20` | the volume-weighted walk down the ladder, plus 0.10% beyond level 20 | ✅ visible queue | ✅ |
| `BOOK_TICKER` | `bookTicker` + `aggTrades` | the far touch plus `k·√(size ÷ last minute's notional)` | ✅ queue seen at the touch | ✅ |
| `TRADE_ONLY` | `aggTrades` | the **next** print at or after arrival, plus a stated spread | ✗ | ✅ |
| `BAR_CLOSE` | `klines` | the **most recent** bar print before arrival, plus a fixed offset | ✗ | ✗ |

**The two refusals are spec §6.4's own argument applied where its inputs are missing.** A
limit order needs a resting size to sit behind; without one, *"touching a limit price is not a
fill"* is unenforceable, and an unenforced version of that rule is exactly the fiction the
spec calls "the single most common way limit strategies look profitable and are not". A stop
needs a tape fine enough to fill against once it triggers; at `BAR_CLOSE` the fill would land
at the next bar's open, up to a whole timeframe after the trigger — a delayed market order
wearing a stop's name. A strategy that gets a clear "not at this tier" learns something true.

**A downgrade is never invisible** (§4.2 decision 3). The run row stores what was asked for
beside what was executed; the Runs table shows both; the results page prints the reason and
what was lost. And the New Backtest dialog runs the *same resolution the worker will run*,
over the same range, before anything is queued — so "this range only supports TRADE_ONLY,
limit orders will be refused" appears before you wait for the run rather than after.

On this lake, that resolution currently answers:

| Range | Best tier | Why |
|---|---|---|
| 2024-03-25 … 27 | `BOOK_TICKER` | bulk `bookTicker` covers 2023-05-16 … 2024-03-30 |
| 2026-07-14 … 16 | `TRADE_ONLY` | `aggTrades` only |
| 2025-08-01 … 2026-08-01 | `BAR_CLOSE` | no tick coverage across the year |
| 2026-08-02 … 03 | *unrunnable* | the collector has depth the bulk kline archive has not published yet |

That last row is the one worth reading twice. `depth20` exists for it and nothing else does,
so a tier check that only looked at the tick datasets would advertise the best fill model in
the platform for a range with no bars to drive a strategy at all.

**The queue model, in one line.** Our position in the queue is unknowable without L3 data, so
it is tracked as an *upper bound*: everything resting at the level when we join is ahead of
us, same-side prints consume the front of it, and every fresh observation tightens the bound
by `min`. That last step is exactly right rather than merely convenient — the published size
at a level counts *everyone* there, and the orders ahead of us are a subset, so an observation
can never be smaller than the truth. It captures cancellations ahead of us, which no
trade-consumption rule can see, and it never claims a better position than the data permits.

**Book state is pulled, not queued.** A day of BTCUSDT `bookTicker` is ~15 million rows, and
pushing each through the event queue would build 15 million event objects to answer a question
only asked at order arrivals. The book is a *state*: the rows are scanned in C++, delivered as
Arrow batches, and located by binary search. Two days of `BOOK_TICKER` replay — 4.2 million
trade events against ~30 million book rows — takes 47 s.

### Risk limits (Phase 6)

Spec §7's limits run in the **shared core** (`perplab/core/risk.py`), so a limit that stops a
backtest will stop live trading identically. Two kinds, and they are not interchangeable:

| Limit | Default | Action |
|---|---|---|
| `max_position_notional` | none | reject the order |
| `max_leverage` | 5× | reject the order |
| `max_open_orders` | 10 | reject the order |
| `max_orders_per_minute` | 30 | reject the order — the runaway-loop guard |
| `max_daily_loss_pct` | 0.02 | halt the run |
| `max_drawdown_pct` | 0.15 | halt the run |
| `min_equity_pct` | 0.50 | halt the run |
| `max_consecutive_losses` | none | halt the run |
| `halt_on_liquidation` | on | halt the run |
| `max_consecutive_rejections` | 5 | trip the kill switch |

Those defaults are what the **New Backtest dialog** offers. The engine's own default is
*no limits at all*, deliberately: a default that silently halted a Phase 4 run would change
an answer nobody asked to change, and a stored run with an empty `risk_limits` genuinely had
none. A person opening the dialog is choosing, so choosing nothing gets them §7's table; the
opt-out is one checkbox and the run is badged `RISK_UNBOUNDED` for it.

Three properties worth knowing before you trust a number:

- **Size limits are checked against the position the order would produce, counting everything
  already in flight** — not against the position as it stands. Ten orders submitted inside one
  bar are ten orders in flight before any of them fills.
- **Reduce-only orders are never refused.** They can only shrink a position, so no size and no
  count limit applies to them. An account that has breached a limit is exactly the account
  whose exits have to work.
- **A halted run's metrics cover the period up to the halt**, not the range that was asked
  for. Measuring against the full range put an unobserved flat window into every ratio and
  produced a positive Sharpe for a run that was stopped at a loss.

Every refusal is a `RISK_REJECT` in the event log and a row in the results page's Risk
section, with the observed value and the limit beside it. A risk layer that quietly drops
orders produces an equity curve indistinguishable from a strategy that declined to trade.

### Order amendment, `on_cancel`, and auto-flatten (Phase 6)

```python
oid = ctx.buy(qty=ctx.money("0.5"), type="LIMIT", price=bid, tif="GTC")
ctx.modify(oid, price=bid - tick)        # takes latency, and costs queue position
ctx.cancel(oid)

def on_cancel(self, ctx, event):          # CANCELLED / EXPIRED / REJECTED
    if event.status == "EXPIRED":
        ...                               # a post-only that never entered
```

`qty` on `modify` is the order's **new total**, matching Binance's own endpoint. Priority is
kept only on a strict size *decrease*; a reprice or a size increase goes to the back of the
queue, which is what the exchange does and the reason an amend is worth modelling separately
from cancel-and-replace.

`AutoFlatten(max_hold_ms=..., before_funding_ms=...)` is a platform guarantee rather than
something each strategy re-implements. Both are off unless asked for. The hold clock runs from
when the position **opened** — a strategy that scales into a winner still hits its deadline —
and a flip restarts it, because the exposure being bounded is the new one.
`before_funding_ms` must exceed the 60 s mark cadence: the deadline is checked once per mark
bar, so a shorter window is first noticed too late for the closing order to land.

### Parameter sweeps (Phase 6, a Phase 9 feature landed early)

```python
from perplab.lab.sweep import points_from_spec, sweep

points = points_from_spec(spec, {"fast": [8, 12, 16], "slow": [50, 100]})
results = sweep(root, points, max_workers=4)
```

Processes, not threads — a backtest is pure Python bytecode, so a threaded sweep would take
exactly as long while looking faster in the code. Points are returned in grid order whatever
order they finish in, and a point that raises is kept with its error rather than dropped: a
hole in a grid must not look like a complete grid, and the holes are usually the interesting
part.

The sweep draws no conclusion and computes no statistic. §8.5's trials counter still applies
to whatever you do with the output — its best result is a maximum over N draws, not an
estimate of anything.

### Papertrading (Phase 7)

A paper session runs a strategy against the live market on a wall clock, records exactly what
it saw, and then replays that recording as a backtest so the two can be compared. The
comparison is the point: spec §6.7 says architecture alone does not prevent a backtester from
being optimistic — it has to be measured.

```bash
perplab serve
```

Open the **Data & Feed** tab, then start a session from a strategy card. Or from the API:

```bash
curl -X POST http://127.0.0.1:8756/api/sessions -H 'content-type: application/json' -d '{"strategy_id":1,"symbols":["BTCUSDT"],"timeframe":"1m","endpoint":"testnet","fill_tier":"BOOK_WALK","max_runtime_s":172800}'
```

The session runs in **its own process**, like a backtest worker and for the same reason: a
strategy with a runaway loop costs one killable process rather than the server you need in
order to fix it, and a session additionally holds API keys. A live run shows a real-time
monitor instead of a results page — position, live PnL, distance to liquidation, recent
fills, connection state, risk-limit usage bars and a stop control.

**It is the same engine.** Not a live engine that resembles the backtester: the same
`BacktestEngine` class, the same order lifecycle, the same risk checks, the same ledger, the
same metrics. Spec §6.1's table names exactly two things a mode may change — where market
data comes from and where orders go — so those are the only two seams the platform has. That
is what makes the parity report a measurement of the fill model rather than a measurement of
two codebases that were written to agree and no longer do.

#### The session tape and the shadow backtest

Every session writes a **tape**: one JSONL row per market event, in the order the engine
dispatched it, plus the exchange reports it received. When the session ends, the tape is
replayed through the ordinary backtest worker as a *shadow* run, and a parity report is
attached to the session:

| Quantity | What it says |
|---|---|
| fill-count delta | did the same orders fill? |
| average fill-price delta (bps) | did they fill at the same prices? |
| final-PnL delta | did the run end in the same place? |
| unmatched fills | orders that filled in one and not the other |

Flagged past **5% of gross PnL** or **3 bps** average deviation (spec §6.7.2). Persistent
divergence means the fill model needs recalibration, which is the feedback loop that makes
the backtester get more honest over time.

The shadow replays the **tape**, never the lake. Three reasons, each of which would otherwise
put a difference in the report that has nothing to do with fills: the bulk archive lags about
a day so the window is not in it yet; the collector's lake is downsampled on its own
schedule; and a frame the session genuinely never received would reappear in a lake replay.
The shadow also takes its fill tier from the tape rather than re-deriving it — `resolve_tier`
judges coverage from the lake, which does not hold this window, so every shadow would demote
to `BAR_CLOSE` and be compared against a `BOOK_WALK` session.

#### Two aggregations that exist to match the backtester

Live data is finer than anything a backtest can read, and using it raw would make the paper
session unrepresentative of the thing it validates.

- **Mark price is aggregated into one-minute bars**, the shape and cadence
  `markPriceKlines` has. The liquidation check probes low, then high, then close of a bar;
  feeding it 1 Hz point samples where all three are equal degenerates it to a close-only test
  and misses every liquidation the market immediately recovered from — an error that is
  one-sided and always in the session's favour.
- **Depth is downsampled to one second**, matching what the collector writes.

Both choices are recorded in the tape's `meta.json` rather than assumed by a reader.

#### Ordering, live

Spec §6.2's total order is a replay concept, and it still has to hold live or the shadow
cannot reproduce the session. Events are held in a short reorder buffer (250 ms) and released
in full `(ts_ms, kind_priority, source_seq, dataset_id)` order.

Sources that are discovered *after* the instant they describe — a kline published a second
after it closes, a mark bar that cannot be emitted until the next minute proves the last one
ended — declare themselves, and the buffer will not release past a point such a source may
still have events for. Without that, **every bar close was dropped as late**: the engine
received none, the strategy never traded, and the session completed successfully with an
empty trade table. That was found by running it, not by reading it.

Late frames are dropped and counted, never forced through — pushing an event into the past
raises out of the event queue, which would end a session holding an open position.

#### Exchange connection (spec §11)

Keys are entered in the Data & Feed tab, validated with a signed balance request, and held
**in the API process's memory only**. Never written to disk, a log line, an exception, the
database or an HTTP response; only the account alias and balance are ever echoed back. A
session worker receives them as one line on its stdin — the only channel that is neither a
file nor an environment variable. Twelve hours of inactivity wipes them.

Enable **Reading + Futures Trading** only. Never enable Withdrawals.

#### Kill switch

Red, top bar, reachable from every tab, no keyboard shortcut. One click, a confirm modal
stating exactly what it will do, then: every session is asked to stop (so each cancels its own
resting orders before dying), and the switch is armed. **Cancel-only by default** — spec §7.3
is explicit that force-closing everything at market during a flash crash can be worse than
the exposure.

The armed state is **on disk**. It used to live in one process's memory, so restarting the
API silently disarmed it — the exact inverse of spec §7.6's guarantee, and worse because the
thing that trips the switch is often the thing that kills the process. An armed switch
refuses a new session with a 409 until someone un-arms it explicitly.

#### What has not been proven

**Testnet order execution has never sent an order to Binance.** The credential layer, the
signed client, the user-data stream, the order transport and the 60-second reconciliation
loop are built and tested against a mock; verifying them needs account credentials. Sessions
today run local fill simulation against live testnet market data, which is the other option
spec §6.1's table names.

**The parity report has only ever been observed at zero.** A session filled by the local
simulator, replayed against its own recording, *should* agree exactly — and does, to twenty
decimal places. That the report can detect a real divergence is checked by tests that
construct one, not by a live observation.

### The Lab (Phase 9)

Five tools that ask whether a backtest result is real. All of them run as background jobs
with a cancel button, because a walk-forward is dozens of engine runs and a sweep-sized wait
with no way out is its own kind of defect.

| Tool | What it answers |
|---|---|
| Walk-forward | Does the parameter chosen in-sample survive out-of-sample? Anchored or rolling, with a stitched OOS equity curve |
| Monte Carlo | How much of this curve was luck? Four resampling methods, each with its own caveat stated |
| Regimes | Where does the edge actually live — which volatility, trend, funding and liquidation-cascade buckets |
| Overfitting | Plateau score, IS-vs-OOS slope, parameter sensitivity, decay |
| Portfolio | What happens across symbols, with rolling correlation rather than one number |

Two defaults worth knowing, both from spec §9. The walk-forward objective defaults to
**neighbourhood-median Sharpe**, not max Sharpe: picking the single best grid point is how
you select a spike that will not repeat, and max-Sharpe is offered but labelled
overfit-prone. Regime buckets are **causal** — expanding-window quantiles or fixed
thresholds only, never quantiles computed over the whole run, which would label a bar using
data from its own future.

The stitched OOS curve comes in two forms, compounded and additive, because which one is
true depends on whether the strategy sizes off equity. Presenting one silently would be a
claim about a decision the user makes.

### Macro context (Phase 11)

Optional, supplementary, and off unless a strategy asks for it:

```bash
python -m perplab macro
```

Hourly polls of CoinGecko `/global` (BTC dominance, total market cap) and Yahoo `DX-Y.NYB`
(the ICE dollar index), into `macroGlobal` and `macroFx`. It runs as a **separate process
from `collect`** on purpose: the market collector's job is to not miss a depth message over
72 hours, and two third-party HTTP endpoints on its event loop would put someone else's
outage inside that process. Cross-coin context needs no macro machinery at all — it is the
ordinary collector and `perplab ingest --symbol ETHUSDT`.

Declare the dataset, then read it:

```python
requires = {"symbols": ["BTCUSDT"], "timeframe": "15m", "datasets": ["klines", "macroFx"]}

def on_bar(self, ctx, bar):
    dxy = ctx.macro("dxy")
    if dxy is not None and dxy.age_hours < 6:
        ...
```

Three things about that call. It returns **`None`, not an exception**, when the lake holds
no macro rows — macro is a signal input, not a dependency, so the run proceeds, with a
`MACRO_MISSING` flag and a warning so a flat result is never mistaken for a signal that said
nothing. Calling it *without* declaring the dataset **raises**, because "you did not ask for
this" and "nothing published" are different facts. And it hands back a view with `age_ms`
rather than a bare float, because the dollar does not print at the weekend and
last-observation-carried-forward without an age presents Friday's close as Sunday's.

**Two clocks, and they answer different questions.** Every macro row stores both the
provider's timestamp and the instant our poller received it; measured against the live
sources these differ by 3–10 minutes, and up to an hour at the default poll interval. A
reading becomes visible to a strategy at *receipt* — gating on the provider's stamp would
hand the backtest data no live session could have had — while `age_ms` is measured from
*publication*, because that is what staleness means. Getting this wrong is invisible in a
backtest and always flattering; it was found by rechecking the phase a day later, and is
written up in [docs/PHASE_SIGNOFF.md](docs/PHASE_SIGNOFF.md).

## Hedge mode and running several strategies at once

**Hedge mode gives a symbol two positions**, a `LONG` and a `SHORT`, tracked separately in
every number that matters: separate entry prices, so separate unrealised PnL; separate
isolated margin, so separate liquidation prices; separate funding, charged to each leg's own
allocation. They are never netted. A long 0.1 at 50 000 and a short 0.05 at 52 000 on the same
symbol have liquidation prices of 45 180.72 and 56 972.11 — one below the mark, one above —
and either leg can be liquidated while the other keeps trading.

Turn it on per run, in the New Backtest or Start Session dialog. One-way mode is the default
and is unchanged.

**Every order must name a side.** In hedge mode a `SELL` means either "reduce the long" or
"open the short", and nothing about the order says which:

```python
ctx.buy(qty=..., position_side="LONG")     # open or increase the long
ctx.sell(qty=..., position_side="LONG")    # reduce the long -- cannot exceed it
ctx.sell(qty=..., position_side="SHORT")   # open or increase the short

long, short = ctx.positions()              # both legs, longs first
ctx.close(position_side="SHORT")           # close one leg
ctx.close_all()                            # flatten the symbol
```

`ctx.buy()` with no side is refused rather than routed by guesswork, and so is `ctx.position()`
— a symbol with two positions makes "the position" a question with two answers. A sell beyond
the long side is refused too: it closes the long, it does not flip into a short. That is the
exchange's own behaviour, and it is why spec §3.3's case C has no hedge-mode equivalent.

**Exposure is the sum of both sides, not the net.** `max_position_notional` measures
`|Q_long| + |Q_short|`, because both legs post margin and either can be destroyed. Netting
would leave a market-neutral book with no ceiling at all — and a market-neutral book stops
being neutral the instant one leg is liquidated.

### One strategy per symbol, per account

**A symbol on an account belongs to one running session.** Starting a session claims its
symbols, and a second session asking for a symbol that is already running is refused —
*whatever* it asked for, a matching leverage included:

```
BTCUSDT: run #41 is already trading it at leverage 5x, ISOLATED, one-way mode
         — the same configuration you asked for, which does not make it shareable
```

The reason is the position, not the settings. Binance holds **one position per symbol and
side for the whole account**: there is no strategy field on an order and no per-strategy
position, so two sessions trading one symbol have their fills merged into a single position
with one entry price, one margin allocation and one liquidation price — while each session's
ledger goes on tracking only the fills it sent. Both then report numbers the account does not
have, and the gap widens with every fill the *other* one makes.

That is an exchange limitation rather than a modelling gap, which is why it is a refusal
rather than a feature. A per-strategy liquidation price on a merged position has no referent:
the venue liquidates the merged position at the merged price and takes both strategies down
together. Run one strategy per symbol, or give the second strategy its own Binance account.

Side is not an escape hatch. Hedge mode does keep `LONG` and `SHORT` apart at the venue, so
two sessions confined to opposite legs genuinely would not merge — but nothing in a session
declares a side, and a strategy may buy or sell at any tick, so it is not checkable at start
time. Symbol-level ownership is what is enforceable, and it is the stricter rule.

Leverage, margin mode and position mode are **account settings scoped to a symbol** in the
same way: `POST /fapi/v1/leverage` takes a symbol and applies account-wide, with no
per-strategy scope and no `positionSide`. Ownership subsumes that case, but the refusal still
names what is in force, and the Start Session dialog shows it before you submit — so the
refusal is not the first you hear of it.

### Concurrent sessions

Several paper or live sessions run at once, one process each, with their own run directory,
ledger, risk engine and tape — **on different symbols**. **Risk limits apply per session and
are not pooled**: two strategies each under a 5x cap are two accounts-worth of exposure on one
real account, and nothing sums them. That is deliberate; if it should change,
`test_risk_limits_are_not_pooled_across_sessions` is where the decision is recorded.

The kill switch remains machine-wide: firing it stops every session and blocks new ones until
it is explicitly un-armed (spec §7.6).

## Design decisions worth knowing

**Scaled int64, not float or Decimal, for market data.** Prices and quantities are stored
as integers scaled by 10⁸. `float` cannot represent 0.07 exactly and the error compounds;
`Decimal` round-trips exactly but is too slow for the data path. `Decimal` appears only at
the accounting boundary, and a test enforces that no other module imports it
(`test_money.py::test_decimal_is_confined_to_the_accounting_seam`).

**Every gap must be explainable.** The collector writes a heartbeat every 10 s even when
idle, so "no data" is never ambiguous. Disconnects, restarts, and dead streams each write
a record, which means a gap either has a matching explanation or is a real failure —
making the Phase 1b exit criterion ("72 h, zero *unexplained* gaps") decidable rather than
a judgement call.

**Crash recovery leaves evidence in the data, not just the log.** An unclean exit leaves a
state file behind; the next run detects it and writes a `RESTART` record with the measured
downtime. Process death is therefore caught by the same gap detector as everything else.

**Atomic writes.** Parquet files are written to `.tmp` and renamed. A truncated Parquet
makes the whole dataset unreadable — DuckDB raises `No magic bytes found at end of file`
for the glob, not just for the bad file — so a crash mid-write without this would take the
lake down rather than shorten one partition. (The original reasoning here said truncated
files read as *short*; measured, they do not. Same conclusion, firmer ground — finding F13.)

**CSV headers are sniffed per file, never configured.** Binance's bulk archives sometimes
open with a header row and sometimes with data, and the answer differs *between two files
of the same dataset*. The boundary is 2022-08-11, but the era before it is not uniformly
headerless — `klines` alone has seven separate headerless runs with headered islands
between them. There is no era rule that is correct.

Both wrong answers ingest cleanly and exit 0. Assume a header where there is none and the
first bar of 924 archives vanishes; assume none where there is one and the header becomes a
junk row at the front of the partition. Gap detection would eventually surface the first of
those for `klines` — as 924 one-minute gaps, for whoever runs a report over the whole
2019–2022 era and reads it — but for `aggTrades` one dropped trade a day is beneath every
threshold in the system and is lost permanently. `bulk_layout.is_header_line` compares the
first line against the dataset's known column names, not "does field 0 parse as a number",
which would classify `metrics` data rows as headers because they open with a datetime
string. Finding F5.

**One Parquet file per archive for bulk; part-files for the collector.** These are opposite
answers to the same question and both are right. A live collector cannot hold a day in
memory and cannot know when a day is finished, so it emits `part-<epoch_ms>-<seq>.parquet`
as it flushes. A bulk archive is a closed, complete unit, so it is written as exactly one
file published with one `os.replace` — `data.parquet` where one archive fills a partition,
`<period>.parquet` where a partition spans several (klines are daily archives into monthly
partitions). Rewriting a whole month on the arrival of each of its days would turn a 31-file
ingest into 496 file-writes and would put already-safe days at risk on every crash.

The two naming schemes are also a safety interlock. `aggTrades` and `bookTicker` have two
producers, one lake root and one partition path, and their filenames do not collide — so an
`os.replace` would overwrite *nothing* and both files would survive, with every overlapping
row counted twice by the `**/*.parquet` glob that query, gaps and manifest all use. Nothing
downstream could notice: the manifest sees a two-file partition, which is what a healthy
collector day looks like, and gap detection only measures silence, which duplicates shorten.
So `ingest` refuses a partition already holding `part-*.parquet` rather than adding to it —
checked before the download to save the bytes, and again immediately before the publish
because the collector is live and can flush during the minutes a transfer takes.

**Every archive is checksum-verified, and the ordering is structural.** The `.CHECKSUM`
sibling is fetched *before* the archive, and the digest it carries is the only thing that
unlocks parsing — there is no code path from bytes on disk to rows in Parquet that skips
the comparison. A 300-byte request also discovers an unpublished day for free rather than
after 240 MB. All 7,022 archives fetched on 2026-08-02 verified. That is not the same as
the data being sound: finding F9 is 267 days of `metrics` published with every row
duplicated, which passes its own checksum because the duplication is in the content.

**Resumability is a receipt per archive, written last.** Completion is recorded in
`<lake>/_ingest/<dataset>/symbol=<SYM>/<period>.json`, published atomically *after* the
Parquet it describes. A crash mid-download leaves a `.zip.part` in scratch and nothing in
the lake; a crash mid-write leaves a dot-prefixed `.tmp` and no receipt; a crash between
the write and the receipt means the archive is fetched again and the same deterministic
path is atomically replaced. No interleaving leaves a half-written partition that reads as
done, and the recovery action is always the same idempotent re-ingest. The ledger lives
*beside* the dataset directories, not inside them, because the manifest hashes the sorted
file list under each dataset and a JSON receipt filed next to the Parquet would make every
ingest look like a data change.

## Caveats — what Phase 7 does not cover

- **No order has ever been sent to Binance.** The credential layer, the signed client, the
  user-data stream, the order transport and the 60-second reconciliation loop are built and
  tested against a mock exchange. Verifying them needs account credentials, and code that
  places real orders should not be signed off against a fake. Sessions today run local fill
  simulation against live testnet market data — the other option §6.1's table names.
- **`ctx.modify()` raises in a live session.** This is a genuine mode difference and it is
  not one §6.1 sanctions. Binance publishes `PUT /fapi/v1/order`, so it is not an exchange
  limitation: the amend rules — queue-priority retention, the refusal when an amendment
  overtakes its own order, the rewrite of the working quantity — live inside the engine, and
  a transport cannot reach them without re-implementing ledger logic on the wrong side of the
  seam. Degrading silently to cancel-and-replace would be worse, because it always loses
  queue position. Closing this properly needs an engine-side entry point a transport can
  call; until then the call raises with the alternative named rather than doing something
  different from what a backtest would have done.
- **The parity report has only ever been observed at zero.** A session filled by the local
  simulator and replayed against its own recording *should* agree exactly, and does — to
  twenty decimal places. That the report can detect a real divergence is established by tests
  that construct one, not by having seen one.
- **The 48-hour criterion is wall time.** The mechanism is proven end to end at five minutes.
- **Sessions are single-process and single-machine.** Nothing coordinates two sessions on one
  account, and nothing stops you starting them. The exchange would net their positions while
  each ledger believed it held its own.

## Caveats — what Phase 6 does not cover

- **The kill switch's live half now exists** (Phase 7): sessions are asked to stop through a
  control file, each cancels its own resting orders as it shuts down, and the armed state
  survives a restart. The one part still unproven against a real exchange is the cancel
  itself — see the Phase 7 caveats above.
- **Both remaining §7 auto-triggers are now wired** (Phase 7): WebSocket disconnection beyond
  `max_disconnect_seconds` while a position is open, and live↔exchange reconciliation
  mismatch. The first is exercised against a scripted feed; the second against a mock
  exchange, for the reason in the Phase 7 caveats.
- **`before_funding_ms` is bounded below by the mark cadence.** The deadline is evaluated once
  per mark bar, so a window under 60 s is refused at construction rather than half-kept.
- **A refused auto-flatten is retried on the next mark, not immediately.** If the exchange
  will not accept the exit — a residue below `MIN_NOTIONAL`, a position above
  `MARKET_LOT_SIZE` — the position is held past its deadline and the run says so
  (`AUTO_FLATTEN_UNMET`).
- **The sweep has no UI and no CLI.** It is a Python entry point. The Lab tab is Phase 9.

## Caveats — what Phases 4 and 5 do not cover

The exit criteria are met, and they are narrower than "you can trust a backtest". What sits
outside them:

1. **Tick coverage is the binding constraint, not the engine.** All four tiers work, but the
   lake holds `aggTrades` for only 15 days and `depth20` for two. A year-long run is
   `BAR_CLOSE` because that is what the data supports — the honest answer, and a real limit
   on what you can currently study at high fidelity.
2. **Our own orders do not move the market.** The ladder is not depleted by our fills, and a
   resting order of ours never absorbs an aggressor that historically printed through it. The
   through-print rule bounds a fill by the aggressor's own size, which keeps the counterfactual
   from contradicting itself, but the deeper limitation is inherent to replaying a tape that
   never contained us.
3. **Mark price stays at 1-minute resolution at every tier.** The collector's 1 s `markPrice`
   stream exists for two days; `markPriceKlines` covers six years. Keeping one mark source
   means changing the fill tier changes *only* fills, which makes a tier comparison a
   controlled experiment. A mark-triggered stop therefore fires at the close of the minute
   whose range contained its level — up to a minute late, which is the pessimistic direction
   for both a stop and a take-profit.
4. **No `empirical` latency model** (§6.3's third option). It samples latencies measured
   during your own paper sessions, and there are no paper sessions until Phase 7. A model
   calibrated on nothing would be `lognormal` wearing a more convincing name.
5. **The reference snapshots are approximate for every historical range.** Snapshotting began
   on 2026-08-01, so a 2025 backtest validates orders against the nearest available
   `exchangeInfo` and computes maintenance margin from a later bracket table. Both are
   flagged (`FILTERS_APPROXIMATE`, `BRACKETS_APPROXIMATE`) and the snapshot actually used is
   recorded next to the one that should have applied.
6. **No risk layer.** Spec §7's limits and kill switch are Phase 6. `risk_limits` is recorded
   as an empty mapping in every run manifest rather than omitted, so a run that predates the
   limits cannot later be mistaken for one that ran under limits nobody wrote down.
7. **`on_market_liquidation` never fires**, because the market-wide forced-order feed has no
   public source on this deployment. Declaring `liquidations` in `requires` is refused rather
   than silently ignored.

## Caveats — what Phase 3 does not cover

Phase 3's exit criterion is authoring, not execution, and the boundary is worth stating
because the UI is convincing enough to obscure it:

1. **Nothing runs against real market data yet.** The validator's smoke run uses a synthetic
   random walk. It answers "does this code run at all" — which catches an unbound name in a
   branch that only fires after a crossover, and catches it in a second rather than twenty
   minutes into a backtest — and it answers nothing about whether the strategy is any good.
2. **The smoke-run harness is not the backtest engine.** No latency model, no fill model, no
   fees, no funding cashflow into a real ledger, no margin, no liquidation. Market orders
   fill instantly at the mark and resting orders never fill. It tracks a position only so
   that `ctx.close()` and `ctx.stop_loss()` have something to act on; the PnL it computes is
   never surfaced, because a number that looks like a result would be taken for one.
3. **`ctx.risk.max_allowed` is a margin bound, not a limit.** The §7 per-run risk limits are
   Phase 6 and do not exist, so the number it returns will get *smaller*. Documented that
   way round deliberately: a bound that later tightens is safe to have believed.
4. **The static scanner is a stability boundary, not a security sandbox** (§2.3). It catches
   `datetime.now()` written without thinking on a Tuesday. It does not, and is not trying
   to, stop someone determined to defeat it. Do not import strategies from strangers.

## Caveats — what Phase 1 does not cover

The exit criterion is met. These three sit outside it and are not verified by it:

1. **`metrics` full history.** 563 days cannot be parsed at the lake's 8-decimal precision
   (F8). Fixing it is a decision about the numeric seam, not a bug fix, and was not taken
   unilaterally mid-verification.
2. **The tick gap rule's true-positive path has never fired on real data.** Both sampled
   weeks of `aggTrades` and `bookTicker` are clean, so `detect_tick_gaps` finding a genuine
   dropout is exercised only by unit fixtures and the corrupted-sample drill. It has never
   been shown to catch one in the wild, because there has not been one to catch.
3. **`bookDepth` and the collector-side datasets were not part of this backfill.**
   `bookDepth` is published and available; nobody has ingested a single archive of it, and
   `bulk_layout.parse_book_depth_row` still carries a warning that its per-column types were
   inferred from column names rather than verified against a sample row.

Related but separate: the klines missing-bar rule has now been shown to be structurally
unable to fire on this instrument (F10), and `metrics` duplication is invisible to every
reporting surface the project has (F9). Both are recorded, neither is fixed.

## Layout

```
perplab/
  core/      types.py, money.py          # conventions and the numeric seam
             account.py                  # Phase 2: the ledger
             margin.py, funding.py       # Phase 2: brackets/liquidation, settlement
             invariants.py               # Phase 2: the nine §3.10 conservation checks
             sizing.py                   # Phase 3: ctx.risk, behind the Decimal seam
  exchange/  rest.py, ws.py, filters.py  # public Binance clients
  data/      collector.py, writer.py, schemas.py, supervisor.py, reference.py
             rest_poller.py              # mark price + aggTrades, per finding F4
             bulk_availability.py, bulk_layout.py, ingest_bulk.py   # Phase 1 write side
             gaps.py, manifest.py, query.py                         # Phase 1 read side
  strategy/  base.py, params.py          # Phase 3: the user-facing Strategy API
             indicators.py               # causal indicator library (§5.4)
             context.py                  # ctx: reads, orders, risk, log
             scan.py, validate.py, sandbox.py     # the six-stage validator (§5.5)
             loader.py                   # source -> class, shared by validator and worker
             dryrun.py, synthetic.py     # smoke-run harness and its data
             library.py, template.py     # CRUD, versioning, .perplab bundles (§5.6)
  engine/    clock.py                    # Phase 4: the §6.2 total event ordering
             feed.py                     # lake -> ordered event streams
             fills.py, latency.py        # §6.4 BAR_CLOSE fills, §6.3 latency
             executor_base.py            # what backtest/paper/live share (§6.1)
             backtest.py, worker.py      # the engine, and the isolated job process (§2.3)
             runspec.py                  # the §12.1 reproducibility record
  analytics/ metrics.py, trades.py, attribution.py   # §8.2, §8.3, §8.4
  store/     db.py                       # SQLite schema for strategies/versions/runs
             runs.py                     # run rows, artefacts, the §8.5 trials counter
  api/       app.py, deps.py, routers/   # FastAPI, loopback by default
  cli.py
frontend/    React 18 + TS + Vite + Monaco; `npm run build`, served by `perplab serve`
docs/        PHASE_SIGNOFF.md            # per-phase exit criteria and their evidence
             DATA_AVAILABILITY.md        # empirical findings vs the spec (F1-F14)
             ACCOUNTING_NOTES.md         # Phase 2 findings and deliberate deviations
             INGESTION.md                # operator's guide to backfilling
tests/       unit/ integration/ golden/ property/     # golden = hand-computed §12.2 cases
             unit/test_indicators.py     # incl. the §12.3 look-ahead test + its control
scripts/     install_watchdog.ps1, gap_detection_drill.py, query_benchmark.py
tests/unit/, tests/integration/
userdata/    market lake, reference snapshots, logs (gitignored)
```

## Known deviations from the spec

Each is documented at the point of deviation:

- **Part-files instead of one `data.parquet` per partition, for the collector** (§4.3). It
  cannot write one file per day atomically without buffering the whole day. Hive globbing
  reads a multi-file partition identically; a compaction step comes later. Bulk ingestion
  *does* write one file per archive — see the design note above for why the two differ and
  what the difference protects.
- **Daily partitions for all collector datasets** (§4.3 uses year/month for some). Bulk
  ingestion receives a month at once; a collector appends continuously, and daily
  partitions bound the blast radius of one corrupted file.
- **`bookTicker` recorded live**, which §4.2 did not call for — see finding F1.
- **`markPrice` and `aggTrades` collected over REST rather than WebSocket** (§1b names them
  as streams). The streams deliver nothing on this endpoint and REST does; the `fromId`
  cursor makes the trade sequence verifiably gapless rather than merely probably so. See
  finding F4.
- **`liquidations` is not collected at all** (§1b names it). No public source remains — the
  stream is suppressed and `allForceOrders` returns 404. Recorded as an `UNAVAILABLE`
  collector event per run so the absence is explained in the data rather than assumed
  (F2, F4).
- **`markPriceKlines` added to the Phase 1 backfill set, `liquidations` removed** (§13
  names "klines, aggTrades, bookTicker, funding, metrics, liquidations"). Liquidations
  cannot be fetched at all (F2), and including them would make every default run exit
  non-zero, which teaches an operator to ignore the one signal that says whether the
  backfill worked; they are *reported* on every run instead. `markPriceKlines` is the same
  size and cost as `klines` and has a real gap rule, so omitting it would make the default
  gap report open with "all bars absent" for a dataset nobody was told to fetch.
- **The engine defaults to *no* risk limits, where §7 gives every limit a default.** A
  default that silently halted a Phase 4 run would change an answer nobody asked to change,
  and a stored run with an empty `risk_limits` genuinely had none — reading it back as §7's
  table would report a run that never rejected an order as one that ran under a 5x cap. The
  API and the New Backtest dialog apply §7's defaults, because a person choosing nothing
  should get the limits the spec wrote; the engine's default is "nobody said", and it is
  badged `RISK_UNBOUNDED`.
- **Open interest is recorded with the endpoint's own timestamps, not on the archive's
  5-minute grid.** Snapping the two halves of the series onto shared instants creates a
  collision a later backfill cannot resolve — `ctx.oi()` would then depend on ingest order,
  which §12.1 forbids. Unsnapped, live and archive rows interleave and LOCF reads the later
  one.
- **`perplab/lab/sweep.py` lands a Phase 9 feature in Phase 6.** Every other Lab tool is a
  way of *interpreting* results and belongs behind the Phase 8 live sign-off; a sweep is
  not — it runs N backtests and reports what each returned, drawing no conclusion and adding
  no statistic. It has no UI and no CLI, which is where the phase boundary is honoured.
- **`before_funding_ms` is refused below 60 000 ms**, where §7 places no bound on it. The
  deadline is checked once per mark bar, so a shorter window is first noticed too late for
  the closing order to land — the run would report an `AUTO_FLATTEN` and pay the funding
  anyway, which is the platform claiming a guarantee it broke.
- **No fifth gap rule for exchange halts**, though the data now clearly warrants one
  (finding F10). Spec §4.5 defines four; adding a fifth with an unreviewed liquidity
  threshold is a decision, not a fix.
- **A save whose code is byte-identical to the head does not write a new version row**
  (§5.6 says "every save writes a new immutable version row"). A version exists to answer
  "which code produced this equity curve", and identical code is the same answer — so the
  duplicate row would differ only in its timestamp while making the history harder to read,
  and ⌘S is pressed constantly. The API returns `created: false` so the editor says "no
  changes" rather than claiming a version it did not make. A save that supplies a *new
  message* against identical code does write a row, because that is a change to the record.
- **Tailwind is present but the design system is plain CSS custom properties** (§2.2 lists
  Tailwind; §10.1 specifies "one stylesheet, two token sets" on `:root[data-theme]`). Both
  are used — Tailwind for layout utilities, CSS variables for every colour and metric — and
  the token swap is what makes the theme toggle a single attribute change rather than a
  second set of components.
- **The Strategies tab is a list, not a card grid** (§10.3). The grid's per-card content —
  last-run sparkline, cumulative trials count — does not exist until Phase 4 produces runs,
  and a grid of cards with three empty slots each says less than a dense list does. Every
  operation §10.3 names is present. The grid arrives with the data that fills it.
- **The params preview is read-only in the editor.** It renders the auto-generated form from
  the declaration so a bad declaration is caught while the code is open. The *editable* copy
  lives in the New Backtest dialog, which is where a parameter value actually goes somewhere.
- **`slippage_cost` in the attribution is signed, not `|fill − reference| × qty`** (§8.4
  writes the absolute value). The section also requires the four components to sum *exactly*
  to net PnL, and those two cannot both hold: when execution is better than the reference —
  price drifting your way during the latency window — an absolute value charges a cost that
  was a gain, and the identity is off by twice it. The signed form makes the arithmetic
  exact; `slippage_abs` carries §8.4's literal figure alongside it.
- **There is no "spread-implied" queue estimate at `BOOK_TICKER`** (§6.4 offers one:
  `Q_ahead` = visible resting size at `BOOK_WALK` "or spread-implied estimate" otherwise).
  `bookTicker` publishes the size at the *touch* and nothing else, so a level at the touch
  gets a real measurement and a level behind it gets none. Rather than infer one from the
  spread — a shape nothing in the data supports — an unobserved level simply never fills on
  an at-level print; it fills when the market comes to it and the size becomes measurable, or
  when a trade goes through it. Strictly more conservative than the spec's version, and the
  alternative was inventing a number the results page could not distinguish from a
  measurement.
- **A trade printing *through* a resting limit fills it up to the aggressor's own size, not
  "fully"** (§6.4 says fully). Implemented literally, a 0.5 BTC print one tick below a 100 BTC
  bid would hand back a 100 BTC maker fill — a counterfactual that contradicts itself, since
  had that 100 BTC really been resting there the 0.5 BTC aggressor would have been absorbed at
  our price and never printed below it. In the ordinary case the two rules agree: Binance
  aggregates `aggTrades` by price, so a sweep large enough to print through a level emits a row
  **at** it first, and that row has already filled us. The deviation bites only where the
  literal rule is incoherent, and it bites conservatively — we fill less, never more.
- **Limit orders require `BOOK_TICKER` or better; stops require `TRADE_ONLY` or better**
  (§6.4 does not scope them by tier). Both refusals follow from §6.4's own reasoning applied
  where its inputs are missing — see [Fill tiers](#fill-tiers-phase-5).
- **The slippage reference is the top-of-book mid where a book exists** (§8.4 says "the
  reference price at signal time" without naming it). A fill takes the far touch, so measuring
  against the mid charges each side an honest half-spread and charges them the *same* one;
  every other candidate is side-dependent. Below the book tiers it is the last print, and the
  mark only when neither exists.
- **`callback_rate` is a fraction, not a percent.** Binance's own field is in percent, so
  their `1.0` is `0.01` here. Validated to `(0, 1)` at submission, because a rate read as
  percent when it means fraction places the stop a hundred times further away than intended —
  a strategy that then never stops out and looks wonderful right up until it does.
- **`liquidation_cost` is a fifth attribution column** (§8.4 names four). Carried forward
  from Phase 2: a liquidation forfeits the whole isolated allocation, mixing the price move
  the position suffered with the clearance penalty on top of it, and folding the penalty into
  `price_pnl` charges it to the strategy's price edge. The most visible case is a position
  liquidated *while in profit* after funding drained its margin.
- **The queue carries no `ORDER_SUBMIT` event**, though §6.2's priority table lists one at 7.
  Submission is synchronous with the hook that performs it — `ctx.buy()` returns an order id —
  so there is nothing to schedule. The submit *is* recorded in the strategy event log at its
  own timestamp. Priority 8, `ORDER_ARRIVAL`, carries three kinds of instruction reaching the
  matching engine — a submit, a cancel, and a parked market order's deadline — because all
  three are the same event, and giving a cancel its own priority would mean renumbering §6.2's
  table. A priority table that changes is a reproducibility contract that does not exist.
- **`BOOK_UPDATE` (priority 3) is never queued either.** Book state is pulled to a horizon the
  event loop sets rather than dispatched row by row, which is exactly equivalent — a snapshot
  *replaces* its predecessor, so applying every row up to `T` and keeping the last leaves the
  state this returns — and is the difference between a two-day `BOOK_TICKER` run taking 47 s
  and taking an hour. The horizon is `T` for events at or after priority 3 and `T − 1` for the
  three that precede it, which reproduces §6.2's ordering to the millisecond.
- **A mark bar's high and low never become the mark.** §3.4's rule is
  last-observation-carried-forward, so the sample at a bar's close time is its close. The
  high and low are evidence that the mark *traversed* that range during the minute, which is
  what the liquidation check needs and nothing else does: they are probed at
  `LIQUIDATION_CHECK` and the close is restored immediately after. Checking only the close
  would miss every liquidation the market recovered from inside the same minute.
- **The event-log viewer is server-paged rather than virtualised** (§10.3 says virtualised).
  Same goal, opposite side of the wire, and the paged form is strictly better for the filter:
  a browser-side search over a downloaded page reports "no matches" for entries plainly in a
  million-line log.
- **The intrabar drawdown is the *pessimistic* pairing of a range whose ordering the data
  cannot resolve.** A mark bar says the price reached its high and its low inside that minute
  and says nothing about which came first, so the trough is scored against a peak that
  includes the crest. That is the worse of the two possible readings, chosen deliberately per
  §1.4; the alternative would report a drawdown that depends on which extreme the engine
  happened to look at first, which is how a long and its mirrored short came to report
  different numbers on identical price paths.
