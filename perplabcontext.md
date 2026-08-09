# PerpLab — Complete Context Reference

> **Purpose of this file.** A single self-contained briefing on PerpLab: what it is, what it
> can do, how every screen and command works, what data exists, and — above all — how to
> write correct strategies for it, from a first moving-average cross to a multi-symbol,
> regime-switching, risk-managed system.
>
> **Audience.** A capable reader or LLM with no prior exposure to this codebase. Everything
> needed to write a working strategy is here; you should not need to open the source.
>
> **Accuracy.** Every signature, default, enum member, error string and number below was read
> out of the source or measured against the live installation, not recalled. Where the
> platform *refuses* to do something, that refusal is documented as carefully as the
> features — the refusals are the most common source of surprise.
>
> **Snapshot.** Verified 2026-08-05 against `PerpLab 0.1.0` · `ENGINE_VERSION = 5` ·
> `SPEC_VERSION = 4` (RunSpec) · `SCHEMA_VERSION = 8` (SQLite). Repo root
> `C:\Users\Anshul\Desktop\web+apps\PerpLab`. Test suite: 2,615 passing.

---

## Table of contents

1. [What PerpLab is](#1-what-perplab-is)
2. [The mental model](#2-the-mental-model)
3. [Architecture](#3-architecture)
4. [The data — what exists, what doesn't](#4-the-data)
5. [Fill tiers — the most important concept](#5-fill-tiers)
6. [Terminal commands](#6-terminal-commands)
7. [The user interface](#7-the-user-interface)
8. [Writing a strategy](#8-writing-a-strategy)
9. [The complete `ctx` API](#9-the-complete-ctx-api)
10. [Indicators](#10-indicators)
11. [Orders and execution](#11-orders-and-execution)
12. [Accounting, margin and liquidation](#12-accounting-margin-and-liquidation)
13. [Risk limits and the kill switch](#13-risk-limits-and-the-kill-switch)
14. [Analytics and metrics](#14-analytics-and-metrics)
15. [The Lab](#15-the-lab)
16. [Paper and live sessions](#16-paper-and-live-sessions)
17. [Reproducibility](#17-reproducibility)
18. [Worked examples](#18-worked-examples)
19. [Anti-patterns and pre-flight checklist](#19-anti-patterns-and-pre-flight-checklist)

---

## 1. What PerpLab is

A locally-hosted, single-user web application for researching, backtesting, stress-testing
and running algorithmic trading strategies on **Binance USDⓈ-M perpetual futures**.

Strategies are **Python files**, not drag-and-drop blocks. You write a class, the platform
validates it, backtests it against a local Parquet data lake, stress-tests it in the Lab,
paper-trades it against the live tape, and — when you choose — trades it live.

### Scope

| In scope | Out of scope |
|---|---|
| Binance USDⓈ-M perpetuals (USDT-margined) | Spot, COIN-M, options, other exchanges |
| Isolated **and** cross margin | — |
| One-way **and** hedge position mode | — |
| Bar strategies (1m → 1d) and tick strategies (~1s) | Sub-second latency-optimised execution |
| Backtest, paper (testnet), live | Full L3 order-book reconstruction |
| Walk-forward, Monte Carlo, regime, overfitting, portfolio | Queue-position-exact fills (approximated) |

> Note: `PERPLAB_SPEC.md` §1.2–1.3 still lists cross margin and hedge mode as deferred to v2.
> Both were subsequently built. This document reflects the code, which is ahead of that
> section of the spec.

### Design principles (these explain most of the platform's behaviour)

1. **Deterministic.** Same inputs + same seed → byte-identical event log. Enforced by test.
2. **Event-sourced.** Account state is a fold over an append-only event log, never mutated
   ad hoc. Debugging is replay.
3. **Conservative by default.** Where a modelling choice is ambiguous, the one that makes the
   strategy look *worse* is chosen.
4. **Fail loudly.** Missing data, unreachable brackets, unsupported sizes → hard error, never
   silent interpolation. You will meet this principle constantly; it is not a bug.
5. **Never quietly wrong.** A number the platform cannot compute honestly is `None` and renders
   as an em dash, never as `0`.

---

## 2. The mental model

```
write strategy → validate → backtest → Lab (stress) → paper → live
      ↑                                                          │
      └───────────────────── iterate ────────────────────────────┘
```

Four concepts carry most of the weight:

**A run is an immutable record.** Every backtest, paper session and live session is a "run"
with an id, a frozen spec, an event log, and a set of artefacts on disk. Runs are never
edited. Re-running with identical inputs reproduces the identical event-log hash.

**The engine is one engine.** Backtest, paper and live all execute the *same*
`BacktestEngine` against the same `Context`. Only two things are substituted: the market
**feed** (lake replay vs live websocket) and the order **transport** (simulated vs signed
REST). This is why a paper session can run a "shadow backtest" of itself and compare.

**Warm-up is enforced, not advised.** Until the strategy has seen
`max(indicator warm-up, requires["history"])` bars, every order call raises. You cannot
accidentally trade on an under-filled indicator.

**Fill tiers decide what your backtest means.** See [§5](#5-fill-tiers). This is the single
most important thing to understand before trusting a result.

---

## 3. Architecture

### Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.11+, FastAPI, asyncio |
| Query | DuckDB over Hive-partitioned Parquet |
| Metadata | SQLite (`SCHEMA_VERSION = 8`) |
| Money | `decimal.Decimal` behind a scaled-`int64` storage seam |
| Frontend | React + TypeScript, Vite, TanStack Query, Zustand, Monaco |
| Charts | Hand-drawn inline SVG — no charting library, no CDN |

The frontend has **no CDN dependency and no router**. It renders with the network unplugged;
navigation is Zustand state.

### Processes

These are separate OS processes, deliberately:

| Process | Command | Role |
|---|---|---|
| API server | `perplab serve` | REST API + serves the built UI. Port **8756**. |
| Collector | `perplab collect` | Records live market data to the lake. Runs forever. |
| Backtest worker | spawned by API | Executes one run, writes artefacts, exits. |
| Session worker | spawned by API | Runs one paper/live session. |
| Ingest worker | spawned by API | Backfills bulk archives. |

**The collector is independent of the API server on purpose.** Its job is to never stop
recording; tying it to the web server would mean a UI restart costs data that can never be
recovered (depth and bookTicker exist only from the moment you record them).

**Consequence worth knowing:** a paper/live session's worker is a child of the API server.
Restarting `perplab serve` kills running sessions, which are then marked `lost` — artefacts
are kept, but the session ends.

### Storage layout

```
userdata/
├── market/                      the data lake (Parquet, ZSTD)
│   ├── aggTrades/symbol=BTCUSDT/date=YYYY-MM-DD/*.parquet
│   ├── bookTicker/symbol=…/date=…/
│   ├── depth20/, markPrice/, collectorEvents/
│   ├── klines/symbol=…/interval=1m/…
│   ├── markPriceKlines/symbol=…/year=YYYY/month=MM/
│   ├── funding/, metrics/, macroGlobal/, macroFx/
│   └── _ingest/                 receipt ledger (proof of what was ingested)
├── reference/                   dated exchangeInfo + leverageBracket snapshots
├── runs/<run_id>/               per-run artefacts
│   ├── spec.json                the frozen, hashed run inputs
│   ├── events.jsonl             the event log (hashed for reproducibility)
│   ├── equity.parquet           final mark-to-market series
│   ├── progress_equity.json     in-progress preview (deleted on completion)
│   ├── trades.json, metrics.json, manifest.json
│   ├── monitor.json             live session snapshot
│   └── tape/                    sealed live tape (sessions only)
└── perplab.db                   SQLite: strategies, versions, runs, lab jobs, claims
```

**Tick datasets are `date=` partitioned** (one directory per UTC day, 1:1 with Binance's daily
archives). **Bar datasets are `year=`/`month=` partitioned.** This matters if you query the
lake directly.

---

## 4. The data

### 4.1 What is actually on disk right now

Measured read-only against `userdata/market` on **2026-08-05**. A collector is running, so
tick counts grow continuously. **Total on disk: 29.6 GB.**

| Dataset | Rows | Range (UTC) | Days covered / spanned | Size |
|---|---|---|---|---|
| `aggTrades` | 3,384,089,858 | 2019-12-31 → now | **2,410 / 2,410** (zero missing) | 27.9 GB |
| `bookTicker` | 153,274,584 | 2024-03-24 → now | **12 / 865** ⚠️ | 1.2 GB |
| `klines` (1m) | 3,479,040 | 2019-12-31 → 2026-08-03 | **2,408 / 2,408** (zero missing) | 216 MB |
| `markPriceKlines` (1m) | 3,469,627 | 2019-12-23 → 2026-08-03 | 2,410 / 2,416 (6 missing) | 120 MB |
| `funding` | 7,212 | 2020-01-01 → 2026-07-31 | 2,404 / 2,404 | <1 MB |
| `metrics` | 535,497 | 2020-09-01 → now | **1,602 / 2,165** ⚠️ | 26 MB |
| `depth20` | 293,998 | 2026-08-01 → now | 5 / 5 | 56 MB |
| `markPrice` | 274,021 | 2026-08-02 → now | 4 / 4 | 21 MB |
| `collectorEvents` | 29,616 | 2026-08-01 → now | 5 / 5 | 22 MB |
| `macroGlobal` / `macroFx` | 2 / 2 | 2026-08-03 | 1 / 1 | <1 MB |

**Symbols.** Everything is `BTCUSDT` only, except `klines`, which also holds `ETHUSDT` for
8 days (2026-06-01 → 06-08).

**Absent entirely (no directory on disk):** `liquidations/`, `bookDepth/`.

### 4.2 Known data limitations — read these before trusting a backtest

These are documented findings in `docs/DATA_AVAILABILITY.md`, verified against the venue:

- **F1 — `bookTicker` bulk history is only 2023-05-16 → 2024-03-30.** Binance stopped
  publishing it. This lake holds 12 days out of an 865-day span (7 archive days + 5 collector
  days). *Consequence: `BOOK_TICKER`-tier backtests are impossible for ~99% of history.*
- **F2 — Liquidation data is unavailable at any source.** The websocket `!forceOrder@arr` is
  silent on this endpoint and `GET /fapi/v1/allForceOrders` returns HTTP 404 (withdrawn).
  `on_market_liquidation` therefore **never fires**.
- **F4 — On production `fstream.binance.com`, every *aggregated* stream is silent.**
  `@aggTrade`, `@markPrice`, `@kline_1m`, `@ticker` deliver nothing; raw per-event streams
  (`@trade`, `@depth20@100ms`, `@bookTicker`) work. PerpLab works around this with a REST
  poller (`premiumIndex` at 1 Hz, `aggTrades` with `fromId` paging at 0.5 Hz).
- **F8 — 563 `metrics` days cannot be ingested.** Two ratio columns were published at full
  binary-float precision (16 fractional digits) and the scaled-int seam refuses >8 dp rather
  than silently rounding.
- **F9 — 267 `metrics` days contain duplicated rows** (75,259 duplicates, 14%) and the
  archives still pass their checksums. Any mean/sum/z-score over `metrics` before 2021-05-22
  is double-counted.
- **F10 — Binance publishes flat zero-volume bars through halts** (273 such bars). A
  gap-detection rule keyed on missing bars will never fire on BTCUSDT.

### 4.3 What each dataset is for

| Dataset | Contents | Used for |
|---|---|---|
| `klines` | OHLCV 1m bars | Bar strategies, `BAR_CLOSE` fills |
| `aggTrades` | Every aggregated trade, with aggressor side | `TRADE_ONLY` fills, `ctx.on_tick`, CVD |
| `bookTicker` | Best bid/ask updates | `BOOK_TICKER` fills, spread |
| `depth20` | Top-20 ladder snapshots (1s) | `BOOK_WALK` fills, `ctx.book()` |
| `markPriceKlines` | Mark price 1m bars | Liquidation & unrealised PnL marking |
| `funding` | Realised funding settlements (8h) | Funding cost, `ctx.funding()` |
| `metrics` | Open interest, long/short ratios | `ctx.oi()`, positioning signals |
| `macroGlobal` / `macroFx` | BTC dominance, market cap, DXY | `ctx.macro()` |

---

## 5. Fill tiers

**This determines what your backtest result actually means.** Four tiers, in ascending
fidelity:

| Tier | Needs | Models | Cannot model |
|---|---|---|---|
| `BAR_CLOSE` | `klines` | Fill at the bar's close price | Spread, slippage, intrabar path |
| `TRADE_ONLY` | `aggTrades` | Fill against real prints, aggressor-aware | Whether a *resting limit* order would fill |
| `BOOK_TICKER` | `bookTicker` | Crossing a real bid/ask spread | Depth beyond the top of book |
| `BOOK_WALK` | `depth20` | Walking a real ladder, size-aware slippage | True queue position (needs L3) |

The tiers are ordered: `BAR_CLOSE < TRADE_ONLY < BOOK_TICKER < BOOK_WALK`.

### What this lake can support

| Tier | Usable range |
|---|---|
| `BAR_CLOSE` | 2019-12-31 → now (full history) |
| `TRADE_ONLY` | 2019-12-31 → now (full history) |
| `BOOK_TICKER` | 12 days only |
| `BOOK_WALK` | 2026-08-01 → now (collector only) |

**Practical rule: for any backtest longer than a few days, you are at `TRADE_ONLY`.**

The engine picks the highest tier whose inputs are present for the requested range and
**degrades automatically** if data is missing, flagging the run `FILL_TIER_LIMITED_BY_GAPS`.
If nothing at all is supportable it raises `CoverageError` rather than silently falling back.

### The strategic consequence

`TRADE_ONLY` can tell you a market order filled and at roughly what price. It **cannot** tell
you whether a resting limit order at a given price would have been hit, because a trade tape
records what traded, not what rested. Any strategy whose edge depends on that question is
**not backtestable here**:

| Strategy type | Why it can't be backtested |
|---|---|
| Market making | Entire edge is capturing spread on resting quotes |
| Grid / ladder trading | Every fill is a resting limit order |
| Iceberg / large-order execution | Needs depth over time to model impact |
| Cross-venue / spot-futures arbitrage | Needs synchronised books from both venues |

**These can still be paper-traded and live-traded**, because those modes read the real live
book as it happens rather than reconstructing it from history. The limitation is specific to
*backtesting*.

---

## 6. Terminal commands

Entry point: `python -m perplab <command>` (or `perplab <command>` if installed).

```
usage: perplab [-h] [--root ROOT] [--testnet] [-v]
               {preflight,snapshot-reference,collect,macro,verify-bulk,
                ingest,gaps,manifest,serve,query} ...
```

### Global flags

| Flag | Default | Effect |
|---|---|---|
| `--root ROOT` | `userdata` | The data directory. Lake at `<root>/market/`. |
| `--testnet` | off | Use Binance testnet. Consumed by `preflight`, `snapshot-reference`, `collect` only. |
| `-v` / `--verbose` | off | `DEBUG` logging. **Ignored by `serve`** — deliberately, because httpx logs full signed URLs including `signature=<hex>`, and credentials must never reach a log. |

### The ten subcommands

| Command | What it does |
|---|---|
| `preflight` | Check the machine is fit for an unattended run (power/sleep, disk, clock drift) |
| `snapshot-reference` | Write dated `exchangeInfo` + `leverageBracket` snapshots |
| `collect` | Record market data until interrupted |
| `macro` | Collect BTC dominance, total market cap, DXY |
| `verify-bulk` | Re-check which bulk datasets Binance still publishes |
| `ingest` | Backfill bulk archives into the lake |
| `gaps` | Report gaps in the lake |
| `manifest` | Compute, save or diff a dataset manifest |
| `serve` | Run the API server and web UI |
| `query` | Run SQL against the lake views |

### The commands you will actually use

```bash
python -m perplab serve
```
Starts everything. Open `http://127.0.0.1:8756`. This is the only command needed for normal
work — the UI covers every routine workflow.

```bash
python -m perplab collect --supervise
```
Records live market data. `--supervise` restarts the collector in-process if it crashes.
Needed only if you want `BOOK_WALK`/`BOOK_TICKER` data going forward. Does **not** survive a
machine reboot without an external scheduled task.

```bash
python -m perplab ingest --dry-run --dataset klines --start 2024-01-01 --end 2024-12-31
```
**Always dry-run first.** It reads only the local receipt ledger, makes no network calls.
One plausible command can otherwise pull tens of gigabytes.

```bash
python -m perplab gaps --dataset klines --start 2024-01-01 --end 2024-12-31
```
Exits non-zero if any gap is unexplained. A gap with a matching collector `RESTART`/
`DISCONNECT` record is *explained*; one without is a real hole.

```bash
python -m perplab query "SELECT count(*) FROM klines WHERE symbol='BTCUSDT'"
```
SQL over the lake's DuckDB views.

**Exit codes are meaningful:** `gaps` fails if any gap is unexplained, `manifest --diff` fails
if the lake moved since the saved run, `query --timing` fails if the query misses its budget.

### Windows operational notes

- `PerpLab.vbs` — one-click launcher (starts the server and opens the browser).
- `stop-platform.bat` — stops it.
- Before an unattended multi-day collector run: disable sleep on AC
  (`powercfg /change standby-timeout-ac 0`), confirm no scheduled Windows Update restart,
  and check disk headroom (~1–2 GB/day). `perplab preflight` surfaces these.

---

## 7. The user interface

A single-page app at `http://127.0.0.1:8756`. Three fixed bands: a 48 px chrome, the page
body, a 24 px footer. **No router, no URL state** — navigation is in-memory.

### Persistent chrome (always visible, on every tab)

`PerpLab` wordmark · **Dashboard | Strategies | Runs | Lab | Data & Feed | Settings** ·
session badge · `⌘K` · `◐` theme toggle · **KILL**.

Two elements never move: the **session badge** (answers "is real money at risk") and the
**KILL** button. KILL deliberately has **no keyboard shortcut and no command-palette entry** —
it must be a deliberate physical act. When the kill switch is armed, an armed badge *replaces*
the session badge, because an armed switch is a state of the whole platform.

KILL stays **enabled when the `/sessions` request fails**: a failed query means *unknown*, not
"zero sessions", and the one moment you need the kill switch is the moment the API is sick.

### A design rule that runs through everything

**A failed request is never rendered as an empty-but-healthy state.** You will see
`△ Could not reach the API for X — this card is not saying there are none, it is saying it
does not know.` Colour is never the only signal: a stale row is also prefixed with the word
`stale`, so meaning survives a screenshot or colour-blindness.

### Tab 1 — Dashboard

Answers three questions on one screen: *is money at risk, what ran recently, is the data
fresh.* Every card links into the tab that owns it.

- **Session hero** — the money question in the largest type on screen. Idle: "No session
  running · Nothing is at risk." Active: strategy, symbols, run id, live session PnL (which
  flashes toward the direction of a change), start time, `open monitor →`.
- **Action row** — `New backtest`, `Start paper session`, `Open a strategy`.
- **Cards** — Recent runs (newest 6), Lab (newest 5 jobs), Data freshness per dataset (rows
  older than 6 h marked `△ stale`).

### Tab 2 — Strategies

With nothing open: a full-width **card grid**. Open one and it becomes a 280 px **sidebar
beside the Monaco editor**, so switching strategies never costs the editor.

- **Toolbar**: `+ New strategy`, `Import` (`.py` or `.perplab`), search, tag filters,
  `Archived` toggle.
- **Card**: name, version, notes, last-run result, and three verbs — `Open`, `Backtest`,
  `Session`. An `✕` glyph marks a version that does not validate (glyph *and* colour).
- **Editor**: Monaco with server-side diagnostics. Two validation paths — a debounced
  **quick** static check while typing, and the **full** pipeline (including smoke run and
  determinism probe) on `⌘S`.
- **Save is never blocked by errors.** Broken code is stored with a flag recording that it did
  not validate — refusing to save loses work.
- **Backtest is blocked while dirty**, with the reason: *"a run records the version it
  executed, and an unsaved buffer has no version to record."*
- **History** drawer for versions; `Export` downloads the strategy.

### Tab 3 — Runs

Two panes with a **draggable divider** (hover the seam; drag, or use ←/→ keys; double-click
resets). The list narrows rather than disappearing, because a result is something you compare
against its neighbours.

- **Left — run list**: id, strategy, range, status, net PnL, Sharpe, MaxDD, trades, fill
  tier, badges. Toolbar has `Start session`, `New backtest`, `Archived`, CSV export.
- **Right — run detail**: header with status and actions; for a running session, the **live
  monitor**; then Equity (with drawdown shaded), Price & trades, metric cards, PnL
  attribution, the trade table, and the event log.
- **The equity chart updates live while a run is in progress** and is labelled as a preview:
  its drawdown is measured against the highest peak *so far* and can only deepen.

### Tab 4 — Lab

Job list plus a new-job form. Tools: walk-forward, Monte Carlo, regime analysis, overfitting
diagnostics, portfolio backtest, comparison, parameter sweep. Results render as dedicated
views per tool. Entry point is usually `Send to Lab` from a completed run.

### Tab 5 — Data & Feed

- **Coverage** — a bar per dataset showing *real* per-day coverage. Bars are filled only over
  days that hold data, so interior holes are visible; the caption reads `N of M days · K gaps`.
- **Refresh** — buttons to top up candles/trades from the last stored date to now.
- **Collector** — read-only status card: pid, run start, heartbeat age, restart count,
  datasets recording, caveats. It deliberately **never shows a completion percentage**.
- **Exchange connection** — where an API key is connected.
- **Feed** — the session event stream, tailed live with a severity filter.

### Tab 6 — Settings

Defaults for new runs and sessions (leverage, balance, fees, risk limits), theme, and
diagnostics.

### Dialogs

**New backtest** — strategy + version, date range, starting balance, leverage, seed, fill
tier, fees, risk limits.

**Start session** — mode (**Paper** / **Live**), strategy + version, symbols, timeframe,
leverage, margin mode, duration, risk limits. Choosing **Live** shows a red gate:
`LIVE mode sends real signed orders to {endpoint}` and explains that before the first order
the worker verifies the account is flat, sets leverage and margin mode at the exchange and
verifies the echo, adopts the venue's wallet as the opening balance, and reconciles every
60 seconds.

### Keyboard

| Key | Action |
|---|---|
| `⌘K` / `Ctrl-K` | Command palette — jump anywhere |
| `⌘S` / `Ctrl-S` | Save + full validation (editor) |
| `←` / `→` | Resize the Runs divider (when focused) |
| `Home` / `Esc` | Reset the divider |

### Run badges

| Badge | Meaning |
|---|---|
| `BOOK_WALK` / `BOOK_TICKER` / `TRADE_ONLY` / `BAR_CLOSE` | The fill tier the run actually executed at |
| `PAPER` / `LIVE` | Session mode |
| `RISK_UNBOUNDED` | **No risk limit with a measurable budget was in force** |
| `FILL_TIER_LIMITED_BY_GAPS` | The tier was demoted because input data had holes |
| `RISK_HALTED` | A risk limit stopped the run |
| `TAPE_REPLAY` | A shadow run replaying a session's sealed tape |
| `lost` (status) | The worker died with the API server; artefacts kept, session ended |

---

## 8. Writing a strategy

### 8.1 Anatomy

A strategy is one Python file containing one class that subclasses `Strategy`. Three parts:
two class attributes (`params`, `requires`) and some hooks.

```python
from perplab import Strategy


class MyStrategy(Strategy):
    """One-line description of the edge this is trying to capture."""

    params = {
        "fast": {"type": "int", "default": 12, "min": 2, "max": 200},
        "slow": {"type": "int", "default": 26, "min": 3, "max": 400},
    }

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "15m",
        "history": 26,          # warm-up bars; must cover the slowest indicator
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        # Build indicators HERE, not in __init__ — this is where they register with
        # the run and where the engine derives the warm-up length from.
        self.fast = ctx.indicators.ema(self.p.fast)
        self.slow = ctx.indicators.ema(self.p.slow)

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return

        position = ctx.position()

        if self.fast.crossed_above(self.slow) and position.is_flat:
            ctx.buy(qty=ctx.risk.size_by_notional(ctx.account.equity / 10))
        elif self.fast.crossed_below(self.slow) and position.is_long:
            ctx.close()
```

That is the exact template the editor opens for a new strategy, and it is covered by the
platform's own test suite — the template you are handed cannot drift into failing the
validator you are about to meet.

### 8.2 `requires` — the data declaration

Declared up front so the engine can check data coverage **before** the run starts, rather
than failing 60% of the way through.

```python
requires = {
    "symbols": ["BTCUSDT"],          # REQUIRED — non-empty list of strings
    "timeframe": "15m",              # REQUIRED — see the table below
    "history": 26,                   # optional, default 0 — warm-up bars
    "datasets": ["klines"],          # optional, default ["klines"]
}
```

| Key | Type | Default | Notes |
|---|---|---|---|
| `symbols` | `list[str]` | **required** | Non-empty. Upper-cased. Duplicates refused. A bare string is refused with a message telling you to write a list. |
| `timeframe` | `str` | **required** | One of the valid timeframes |
| `history` | `int` | `0` | Warm-up bars. Must be ≥ 0, not a bool. |
| `datasets` | `list[str]` | `["klines"]` | Must be from `KNOWN_DATASETS` |

**Unknown keys are refused**, with the message naming the four allowed ones. This is a common
first error — there is no `warmup` key (it is `history`), and no `leverage` key.

**Valid datasets** (`KNOWN_DATASETS`):
`klines`, `aggTrades`, `bookTicker`, `depth20`, `markPrice`, `funding`, `metrics`,
`liquidations`, `macroGlobal`, `macroFx`.

> `liquidations` is listed because it is a legitimate thing to *ask* for, not because it is
> available. Declaring it produces a validation **warning** and `on_market_liquidation` will
> never fire (finding F2).

Declaring `macroGlobal` or `macroFx` is what makes `ctx.macro()` answer instead of raising.

**`symbols[0]` is the default symbol** for every `ctx` call that takes an optional symbol. In
a multi-symbol run, passing a symbol not in this list raises `ValueError`.

### 8.3 `params` — the configuration schema

Declared params drive the auto-generated config form in the UI and the Lab's parameter sweep.

```python
params = {
    "fast":     {"type": "int",     "default": 12,     "min": 2,      "max": 200},
    "risk":     {"type": "decimal", "default": "0.01", "min": "0.001", "max": "0.05"},
    "use_stop": {"type": "bool",    "default": True},
    "mode":     {"type": "str",     "default": "trend", "choices": ["trend", "revert"]},
}
```

Six types (`PARAM_TYPES`):

| Type | `self.p.x` is | Extra keys |
|---|---|---|
| `int` | `int` | `min`, `max` |
| `float` | `float` | `min`, `max` |
| `decimal` | `Decimal` (exact) | `min`, `max` — write them as **strings** |
| `bool` | `bool` | — |
| `str` | `str` | — |
| `choice` | `str` | `choices` (**required**) |

Every type also accepts `label` and `help`, which drive the config form's presentation.

**Write decimals as strings.** `0.01` as a Python float is not exactly 0.01, and that error
would multiply a notional. `{"type": "decimal", "default": "0.01"}` is exact.

Access them as `self.p.<name>`. Params are bound **before** `on_start`, so there is no window
in which a hook can see half-bound parameters. Unknown override keys are an error rather than
a silent no-op — a run configured with `{"fastt": 8}` that quietly used `fast=12` would
produce a result attributed to the wrong parameters, which is worse than a failed run.

### 8.4 Hooks

All hooks are optional and default to doing nothing. **You must implement at least one of
`on_bar` or `on_tick`.**

| Hook | Signature | Fires |
|---|---|---|
| `on_start` | `(self, ctx)` | Once, before any data. Build indicators here. |
| `on_bar` | `(self, ctx, bar)` | Once per **closed** bar of `requires["timeframe"]` |
| `on_tick` | `(self, ctx, trade)` | Per aggregate trade — `TRADE_ONLY` tier and above |
| `on_fill` | `(self, ctx, fill)` | When one of your orders fills, in whole or in part |
| `on_cancel` | `(self, ctx, event)` | When an order leaves the book unfilled (cancel/expiry/reject) |
| `on_funding` | `(self, ctx, event)` | At each funding settlement affecting an open position |
| `on_liquidation` | `(self, ctx, event)` | When **your** position is liquidated |
| `on_market_liquidation` | `(self, ctx, event)` | Another participant's liquidation — **never fires** (F2) |
| `on_stop` | `(self, ctx)` | Once after the last event, before final mark-to-market |

**The bar is always closed.** There is no representation of a forming bar anywhere in this
codebase — that is the structural half of the no-look-ahead guarantee.

### 8.5 The rules that trip people up

**1. Warm-up blocks orders, and it is enforced by the platform.**
```python
def on_bar(self, ctx, bar):
    if not ctx.warm:
        return          # ← without this, ctx.buy() raises WarmupViolation
```
The gate is `bars_seen >= max(indicator warm-up, requires["history"])`.

**2. A submitted order is not a position.** Orders take latency to reach the exchange and
fill on a later event. You cannot submit an entry and attach a stop to it in the same hook:

```python
# WRONG — raises: "ctx.stop_loss() protects an open position and BTCUSDT is flat"
ctx.buy(qty=qty)
ctx.stop_loss(stop_price=stop)

# RIGHT — attach on the bar after the fill lands, or in on_fill
def on_fill(self, ctx, fill):
    ctx.stop_loss(stop_price=self.pending_stop)
```

**3. Money is `Decimal`, and `Decimal * float` raises `TypeError`.**
```python
qty = ctx.account.equity / 10                    # ✓ Decimal / int is exact
qty = ctx.account.equity * 0.1                   # ✗ TypeError
qty = ctx.account.equity * ctx.money("0.1")      # ✓
qty = ctx.money(self.atr.value)                  # ✓ indicator float → exact Money
```
`ctx.money()` is the one place indicator floats legitimately become exact quantities.

**4. Indicators are built in `on_start`, never `__init__`.** They attach to the run's
indicator registry, which is what drives them on each closed bar and what the engine reads to
derive warm-up.

**5. `ctx` is sealed.** You cannot set attributes on it (`ctx.foo = 1` raises). Store state on
`self`.

**6. Live mode refuses `ctx.set_leverage()`.** Set leverage on the Start Session form instead.
Backtest and paper apply it exactly.

### 8.6 Validation

Pressing `⌘S` runs the full pipeline. It **saves regardless** — a version that fails
validation is stored with a flag, because refusing to store broken code loses work. But an
invalid version **cannot be backtested**.

Stages: import in a sandbox → static scan → params/requires parse → **smoke run** over 500
bars of synthetic data → determinism probe (the run is executed twice under different
`PYTHONHASHSEED` values and the event hashes compared).

Diagnostics come back at three severities. `error` fails; `warning` and `info` do not.

Useful things the smoke run catches: crashes in any hook, non-determinism, orders that never
fire (warned as "the strategy placed no orders across 500 synthetic bars"), and warm-up
declarations that do not match your indicators.

**What it cannot catch** (documented asymmetries — validation must never be *more permissive*
than the run, and these are the places it is *less* informed):
- The smoke run has no bracket table, so a leverage above a symbol's real bracket passes here
  and is refused at run start. (Anything above 125× *is* caught.)
- `ctx.book()` succeeds in the smoke run regardless of tier; a real run below `BOOK_WALK`
  raises `DataUnavailable`.
- The smoke run always has `hedge_mode=False`.

---

## 9. The complete `ctx` API

`ctx` is the only surface a strategy has. It holds no state itself — every call goes through
to the engine. **W** below marks calls gated by warm-up (they raise `WarmupViolation` before
the gate opens). `sym` means `symbol: str | None = None`, defaulting to `requires["symbols"][0]`.

### Clock and run state

| Call | Returns | Notes |
|---|---|---|
| `ctx.now` | `int` | Engine clock, epoch **ms UTC**. Never wall clock. |
| `ctx.warm` | `bool` | `bars_seen >= max(indicator warm-up, requires["history"])` |
| `ctx.bars_seen` | `int` | Closed bars dispatched so far |
| `ctx.fill_tier` | `FillTier` | `BAR_CLOSE < TRADE_ONLY < BOOK_TICKER < BOOK_WALK` |
| `ctx.hedge_mode` | `bool` | |
| `ctx.symbols` | `tuple[str, ...]` | `[0]` is the default symbol |
| `ctx.timeframe` | `str` | |
| `ctx.indicators` | `SealedIndicators` | Registration and reads only |
| `ctx.rng` | `random.Random` | Seeded from the run — reproducible |

### Market reads

| Call | Returns | Raises |
|---|---|---|
| `ctx.mark(sym)` | `Money` | `RuntimeError` before the first mark |
| `ctx.spread(sym)` | `SpreadView \| None` | `None` at `BAR_CLOSE`/`TRADE_ONLY` |
| `ctx.book(sym)` | `DepthSnapshot` | `DataUnavailable` below `BOOK_WALK` |
| `ctx.funding(sym)` | `FundingView` | — |
| `ctx.oi(sym)` | `float \| None` | — |
| `ctx.macro(name)` | `MacroView \| None` | `DataUnavailable` if undeclared |
| `ctx.tick_size(sym)` | `Money` | `DataUnavailable` where no filters |

`SpreadView` has `.bid`, `.ask`, `.mid`, `.absolute`. `FundingView` has `.last_rate`,
`.next_settlement_ms`, `.predicted_rate` (**always `None`** — history holds settlements, not
the forecasts that preceded them).

### Account and positions

| Call | Returns |
|---|---|
| `ctx.account` | `AccountView` — `.equity`, `.wallet`, `.available`, `.used_margin` |
| `ctx.position(sym, position_side=None)` | `PositionView` |
| `ctx.positions(sym)` | `tuple[PositionView, ...]` — longs first |

`PositionView`: `.qty`, `.entry_price`, `.mark_price`, `.margin`, `.unrealized_pnl`,
`.liquidation_price`, `.is_flat`, `.is_long`, `.is_short`, `.age_hours`.

In hedge mode `ctx.position()` **refuses** without a `position_side` rather than guessing —
a symbol holds two positions there, and returning either would make a one-way strategy run
silently against half its book.

### Leverage

| Call | Returns | Raises |
|---|---|---|
| `ctx.leverage(sym)` | `int` | — |
| `ctx.set_leverage(leverage, sym)` | `None` | `TypeError` non-int; `ValueError` if <1×, position open, or above bracket; **`NotImplementedError` in live mode** |

The UI number is the default; `ctx.set_leverage()` overrides it from the point of the call.
**Only while flat** — an open position keeps the leverage it opened at, because Binance's
mid-position margin re-resolution has edge cases the ledger declines to model on guesswork.

### Order submission — all **W**

```python
ctx.buy(sym, *, qty, type=MARKET, price=None, tif=GTC,
        reduce_only=False, position_side=None, client_id=None, tag=None) -> str
ctx.sell(...)  # same signature
ctx.close(sym, qty=None, *, position_side=None, tag=None) -> str | None
ctx.close_all(sym, *, tag=None) -> tuple[str, ...]
```

`ctx.close()` returns `None` when already flat rather than submitting a zero-quantity order,
and clamps `qty` to the position size. In hedge mode, `position_side` is **required** and
decides what a buy *means* (open long vs reduce short).

### Protective orders — all **W**, all require an open position

```python
ctx.stop_loss(sym, *, stop_price, qty=None, working_type=MARK_PRICE,
              position_side=None, tag=None) -> str | None
ctx.take_profit(sym, *, stop_price, qty=None, ...) -> str | None
ctx.trailing_stop(sym, *, callback_rate, qty=None, ...) -> str | None
```

`callback_rate` is a **fraction in (0, 1)** — Binance's `1.0` (meaning 1%) is `0.01` here.

### Order management

| Call | Gated? | Notes |
|---|---|---|
| `ctx.cancel(order_id)` | no | |
| `ctx.cancel_all(symbol=None)` | no | `None` means **all symbols** |
| `ctx.open_orders(symbol=None)` | no | `None` means **all symbols** |
| `ctx.modify(order_id, *, price=None, qty=None)` | **W** | `qty` is the new **total**, not a delta |

### Risk and sizing

| Call | Returns |
|---|---|
| `ctx.risk.size_by_stop(entry, stop, risk_fraction, *, symbol=None)` | `Money`, floored to step size |
| `ctx.risk.size_by_notional(notional, *, symbol=None)` | `Money`, floored to step size |
| `ctx.risk.max_allowed(symbol=None)` | `Money` — margin bound only |

Sizes are always **floored** to the step size, never rounded up: rounding a computed size up
turns a 1% risk into 1.7% on a small account.

### Logging, recording, utility

| Call | Notes |
|---|---|
| `ctx.log.info/warn/error(message, **fields)` | Goes to the event log and the Feed tab |
| `ctx.record(name, value)` | A custom time series on the run. Finite `int`/`float` only |
| `ctx.money(value)` | `str` exact, `int` exact, `float` 8 dp lossy, `bool` refused |

### Exceptions

| Type | Trigger |
|---|---|
| `WarmupViolation` | Any **W** call before warm-up completes |
| `DataUnavailable` | `ctx.book()` below `BOOK_WALK`; `ctx.macro()` undeclared; `ctx.tick_size()` with no filters |
| `UnsupportedOrder` | Amending a non-`LIMIT` order |
| `EventLogFull` | More than 2,000,000 events |
| `NotImplementedError` | `ctx.set_leverage()` in live mode |
| `AttributeError` | Any `ctx.x = …` (Context is sealed) |
| `ValueError` / `TypeError` | Argument validation throughout |

---

## 10. Indicators

Sixteen indicators, all **causal** (they can only see closed bars). Build them in `on_start`
via `ctx.indicators.<name>(...)`, which both constructs and registers them.

| Factory | Warm-up | Derived series |
|---|---|---|
| `ctx.indicators.sma(period, *, source="close", symbol=None)` | `period` | — |
| `ctx.indicators.ema(period, *, source="close", symbol=None)` | `period` | — |
| `ctx.indicators.wma(period, *, source="close", symbol=None)` | `period` | — |
| `ctx.indicators.rsi(period=14, *, source="close", symbol=None)` | `period + 1` | — |
| `ctx.indicators.macd(fast=12, slow=26, signal=9, *, source="close", symbol=None)` | `slow + signal - 1` | `.signal`, `.histogram` |
| `ctx.indicators.atr(period=14, *, symbol=None)` | `period + 1` | — |
| `ctx.indicators.adx(period=14, *, symbol=None)` | `2 * period` | `.plus_di`, `.minus_di` |
| `ctx.indicators.bollinger(period=20, deviations=2.0, *, source="close", symbol=None)` | `period` | `.upper`, `.lower`, `.bandwidth`, `.percent_b` |
| `ctx.indicators.donchian(period=20, *, symbol=None)` | `period + 1` | `.upper`, `.lower` |
| `ctx.indicators.vwap(session_ms=86_400_000, *, source="hlc3", symbol=None)` | `1` | — |
| `ctx.indicators.realised_volatility(period=20, *, source="close", symbol=None)` | `period + 1` | — |
| `ctx.indicators.obv(*, symbol=None)` | `2` | — |
| `ctx.indicators.cvd(*, symbol=None)` | `1` | fed by **trades** |
| `ctx.indicators.book_imbalance(levels=5, *, symbol=None)` | `1` | fed by **depth** |
| `ctx.indicators.funding_mean(period=3, *, symbol=None)` | `period` | fed by **funding** |
| `ctx.indicators.oi_delta(period=1, *, symbol=None)` | `period + 1` | fed by **open interest** |

`source` accepts `"open"`, `"high"`, `"low"`, `"close"`, `"hlc3"`, etc.

### Reading them

```python
self.fast.value              # float | None — None until warm
self.fast.ready              # bool
self.fast.crossed_above(other)   # crossing detection, other = indicator or number
self.fast.crossed_below(other)
```

`.value` is `None` before warm-up. Guard with `if not ctx.warm: return` and you will never
see it.

### Behaviours worth knowing

- **EMA is seeded with the SMA of the first `period` values**, not the first value alone —
  the convention TA-Lib and TradingView use, so results match a chart.
- **RSI reads 50, not 0, through a dead tape.** If the last `period` price changes are all
  zero, the reading is 50. The textbook formula prints exactly `0.0` for thousands of flat
  bars, so `if rsi.value < 30: buy` would fire on every one of them. This is a deliberate,
  documented departure from TA-Lib.
- **`macd.value` is the MACD line**, so `macd.crossed_above(macd.signal)` reads the way it
  does on a chart. `.ready` means the *signal* has a value, not just the line.
- **MACD refuses reversed periods** (`fast >= slow`) — reversed, the histogram's sign inverts
  and every signal reads backwards.

### Warm-up arithmetic

The engine's gate is `max(slowest indicator warm-up, requires["history"])`. Declaring
`history` **larger** than your indicators need is fine and produces an `info` diagnostic;
declaring it smaller is harmless because the indicator warm-up wins. Declare it honestly —
it is what the coverage check uses to decide how much data to load before your range starts.

---

## 11. Orders and execution

### Order types and time-in-force

`OrderType`: `MARKET`, `LIMIT`, `STOP_MARKET`, `TAKE_PROFIT_MARKET`, `TRAILING_STOP_MARKET`.
`TimeInForce`: `GTC`, `IOC`, `FOK`, `GTX` (post-only).

```python
ctx.buy(qty=ctx.money("0.01"))                                   # market
ctx.buy(qty=q, type="LIMIT", price=ctx.money("64000"), tif="GTC")  # resting limit
ctx.buy(qty=q, type="LIMIT", price=p, tif="GTX")                 # post-only (maker or reject)
ctx.sell(qty=q, reduce_only=True)                                # never opens a short
```

### The order lifecycle

1. **Submit** — the intent is validated (filters, step size, min notional, risk limits) and
   an id returned. Nothing has happened at the venue yet.
2. **Latency** — the order takes modelled time to arrive. This is why an entry and its stop
   cannot be placed in the same hook.
3. **Fill** — priced at *arrival*, against the tape/book in force then. `on_fill` dispatches.
4. **Rest** (limits) — sits in the book until filled, cancelled or expired.

### Latency model

Latency is drawn from a seeded RNG that is **separate from `ctx.rng`**. Sharing would make
every order's latency depend on how many random numbers the *strategy* had consumed, so adding
one `ctx.rng.random()` to a diagnostic line would re-price every fill in the run.

### Funding

Settled every 8 hours against open positions. A long pays when the rate is positive.
`on_funding` fires with the symbol, rate, mark price and the signed payment. Funding is a real
cost line in PnL attribution — for a position held across many settlements it can dominate.

### Liquidation

Computed from the position's own leverage and the bracket table snapshotted at run time. If
the mark price reaches the liquidation price, the position is force-closed and the isolated
margin is forfeit. `on_liquidation` fires. By default a liquidation also **halts the run**
(`halt_on_liquidation=True`).

### Auto-flatten

The platform can close positions automatically at a configured time or before funding —
independent of strategy logic, so a strategy that forgets to exit does not carry risk
overnight by accident.

---

## 12. Accounting, margin and liquidation

- **Money is `Decimal` everywhere.** A scaled-`int64` seam is used for storage; `Decimal` is
  used for all arithmetic. Floats never touch the ledger.
- **Prices round against the trader; quantities round down** to `stepSize`, never up.
- **Entry price is volume-weighted** when adding to a position.
- **Isolated margin** is the default; cross is supported.
- **Leverage is per symbol**, not per side — Binance's `POST /fapi/v1/leverage` takes a symbol
  and nothing else, so a hedge's two legs necessarily share one leverage.
- **Fees**: maker/taker taken from the venue at preflight in live; the standard USDⓈ-M taker
  rate (0.05%) is the backtest default. Fees are booked from fills, not estimated.
- **Nine conservation invariants** are checked at runtime (spec 3.10). At end of run,
  `reconcile()` replays the whole event log from the opening balance and compares it against
  live state — a fill mis-booked identically into both the wallet and the totals is caught
  here.

---

## 13. Risk limits and the kill switch

Risk checks run in the **shared core**, so a limit that stops a backtest stops live trading
identically. Two actions, never interchangeable: **REJECT** (refuse this order, run continues)
and **HALT** (stop the run and close out — the account state is the problem, not the order).

| Limit | Default | Action | Trips when |
|---|---|---|---|
| `max_position_notional` | `None` | REJECT | notional > limit |
| `max_leverage` | `5` | REJECT | leverage > limit, or equity ≤ 0 |
| `max_daily_loss_pct` | `0.02` (2%) | HALT | loss ≥ starting equity × pct |
| `max_drawdown_pct` | `0.15` (15%) | HALT | drawdown from peak ≥ pct |
| `max_open_orders` | `10` | REJECT | open orders ≥ limit |
| `max_orders_per_minute` | `30` | REJECT | rate ≥ limit |
| `max_consecutive_losses` | `None` | HALT | streak ≥ limit |
| `halt_on_liquidation` | `True` | HALT | any liquidation |
| `min_equity_pct` | `0.50` (50%) | HALT | equity ≤ start × pct |
| `max_consecutive_rejections` | `5` | HALT | streak ≥ limit |
| `max_disconnect_seconds` | `None` (sessions set 30) | HALT | socket down > limit **and** a position is open |

**The equality rule:** a *size* limit is a ceiling, so equality passes (`notional > max`). A
*count* or *loss* limit is **reached**, so equality breaches (`open_orders >= max`,
`loss >= max`).

**Percentages are fractions.** `2` means 200%, not 2%. The validator refuses out-of-range
values with: *"a limit written as a percent — 2 rather than 0.02 — would never fire."*

**Exposure is projected, not current.** Checking the position as it stands now "passes every
order right up to the one that ruins the account, and then passes that one too". So the check
is against `max(|position + all buys that could still fill|, |position − all sells|)`.

**`RISK_UNBOUNDED`** on a run means neither a notional nor a leverage ceiling was set —
nothing bounded position size. Fine for a paper test; before any live session, set them.

**Kill switch** — the red KILL button. Arms platform-wide, halts running sessions, and
optionally flattens. Un-arming is the operator's action, never automatic.

---

## 14. Analytics and metrics

Every completed run produces:

**Metrics** — total/annualised return, Sharpe, Sortino, Calmar, max drawdown (measured on
every mark-to-market tick, not on bar closes), volatility, ulcer index, exposure, turnover,
`periods_per_year`.

**Trade stats** — a trade is a **round trip** (flat → flat): count, win rate, average win/loss,
profit factor, expectancy, largest win/loss, average holding time, consecutive win/loss
streaks.

**PnL attribution** — the exact spec-8.4 split, which **sums to net PnL exactly**:

```
price + funding + fees + slippage (+ liquidation penalty) = net PnL
```

This is a genuine identity, not an approximation. If a run lost money, this tells you which
line did it — and for high-frequency strategies the answer is very often `fees`.

**A worked example from this installation:** a braindead 15-minute flip-flop over 173 bars
produced gross trading PnL of about **+4.02** and fees of about **−7.41**, netting **−3.39**.
The strategy was right on price and lost anyway. That is the attribution panel earning its
place.

---

## 15. The Lab

Stress-testing tools that consume a completed run. Entry point: `Send to Lab` on a run.

| Tool | What it answers |
|---|---|
| **Walk-forward** | Does the edge survive out-of-sample? Rolling in-sample fit → out-of-sample test. |
| **Monte Carlo** | How much of the result was luck? Resamples the trade sequence to produce a distribution of outcomes and drawdowns. |
| **Regime analysis** | Where does it work? Splits performance by volatility/trend regime. |
| **Overfitting diagnostics** | Is the parameter choice a peak or a plateau? Flags results that depend on an exact parameter value. |
| **Portfolio** | How do several strategies combine? Correlation and aggregate risk. |
| **Comparison** | Side-by-side runs. |
| **Sweep** | Grid over declared `params`. |

**Read the sweep and the overfitting diagnostic together.** A parameter set that is a lone
spike in a sweep surface is almost always overfitted; one sitting on a broad plateau is more
likely real.

---

## 16. Paper and live sessions

Both run the **same engine** as a backtest. Only the feed and the transport differ.

| | Paper | Live |
|---|---|---|
| Market data | Real live Binance feed | Real live Binance feed |
| Orders | Simulated locally | **Real signed orders** |
| Money | Simulated balance | Your actual account |
| Opening balance | You choose | Adopted from the venue's wallet |
| `ctx.set_leverage()` | Works | **Refused** — set it on the form |

### Cold start is deliberate

A session does **not** preload historical bars. A 50-bar EMA needs 50 live bars before it
trades, and `ctx.buy` raises `WarmupViolation` until then. Three reasons: the lake may have
holes right up to "now"; preloading would break shadow-backtest parity (the tape is the
shadow's only input); and cold-and-honest beats warm-and-silent.

### The live preflight sequence

Before the first order, the worker:
1. Verifies the account is **flat** on the chosen symbols with no working orders.
2. Sets **leverage and margin mode** at the exchange and verifies the echo.
3. Adopts the venue's wallet as the ledger's opening balance.
4. Claims the symbols (see below).

Then it reconciles against the exchange every **60 seconds**; any mismatch beyond tick/step
tolerance trips the kill switch.

### The symbol claim system

Leverage, margin mode and position mode are **account state scoped to a symbol** at Binance —
`POST /fapi/v1/leverage` takes a symbol and nothing else. So two strategies trading BTCUSDT
simultaneously necessarily share one leverage, and their positions merge at the venue.

PerpLab therefore enforces **one session per symbol per account**, recorded in a claims table.
A second session on the same symbol with a different leverage is refused, with the conflict
shown. This is why per-strategy attribution on a shared symbol is not offered — it would be
a fiction.

### Shadow backtest and parity report

When a session ends, PerpLab automatically replays the session's **sealed tape** through the
backtest engine and produces a **parity report** comparing the two. This is how you find out
whether your backtest's fill model matches reality — divergence is measured, not assumed.

### API key security rules

- Keys live **in process memory only** — never written to disk, database, log or UI.
- Keys must be **Reading + Futures only**. Never enable withdrawals.
- Signed URLs are never logged; `serve` stays at `WARNING` even under `--verbose` precisely
  because httpx would otherwise print `signature=<hex>`.

---

## 17. Reproducibility

- **`RunSpec` is frozen and hashed.** Symbols, timeframe, range, seed, leverage, fees, fill
  model, risk limits, strategy code SHA-256, engine version.
- **The event log is hashed.** Identical inputs + identical seed → identical
  `event_hash`. This is enforced by test, and the validator's determinism probe runs your
  strategy twice under different `PYTHONHASHSEED` values to catch set/dict-ordering
  dependencies before they reach a run.
- **The dataset manifest** fingerprints the Parquet files covering the range, so a re-run can
  prove it read the same bytes.
- **Version stamps**: `ENGINE_VERSION = 5`, `SPEC_VERSION = 4`, `SCHEMA_VERSION = 8`.

If two runs disagree, the manifest and the event hash tell you whether the cause was the code,
the config, or the data.

---

## 18. Worked examples

**Every strategy in this document — all seven, including the snippets above — was extracted
and run through the real `validate_code` pipeline (sandbox, static scan, 500-bar smoke run,
determinism probe). All seven returned `ok=True`:**

```
PASS  MyStrategy               warmup=26   orders=16
PASS  FlipFlop                 warmup=0    orders=67
PASS  SMACross                 warmup=30   orders=12
PASS  ATRBreakout              warmup=21   orders=2
PASS  MeanReversionMaker       warmup=20   orders=2
PASS  RegimeGatedMomentum      warmup=120  orders=6
PASS  Minimal                  warmup=0    orders=1
```

They are copy-pasteable as-is.

### 18.1 Level 0 — a smoke test with no edge

For proving the platform works end to end. No indicators, so **no warm-up wait at all** — it
trades on bar 1.

```python
from perplab import Strategy

HOLD_BARS = 10
COOLDOWN_BARS = 5


class FlipFlop(Strategy):
    """Buys, holds, closes, waits, repeats. No edge — this exists to prove the plumbing."""

    params = {
        "qty": {"type": "float", "default": 0.01, "min": 0.001, "max": 1.0},
    }
    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1m",
        "history": 0,
    }

    def on_start(self, ctx):
        self.entry_bar = None

    def on_bar(self, ctx, bar):
        pos = ctx.position()

        if pos.is_flat:
            if self.entry_bar is None or ctx.bars_seen - self.entry_bar >= COOLDOWN_BARS:
                ctx.buy(qty=ctx.money(self.p.qty))
                self.entry_bar = ctx.bars_seen
            return

        if self.entry_bar is not None and ctx.bars_seen - self.entry_bar >= HOLD_BARS:
            ctx.close()
            self.entry_bar = ctx.bars_seen
```

> Validator: `ok=True`, `warmup=0`, 500 bars, 67 orders.
> Expect it to **lose money to fees** — that is the point of running it.

### 18.2 Level 1 — SMA crossover with notional sizing

```python
from perplab import Strategy


class SMACross(Strategy):
    """Long-only SMA crossover, fixed fraction of equity per entry."""

    params = {
        "fast": {"type": "int", "default": 10, "min": 2, "max": 100},
        "slow": {"type": "int", "default": 30, "min": 3, "max": 400},
        "equity_fraction": {
            "type": "decimal",
            "default": "0.10",
            "min": "0.01",
            "max": "1.0",
            "label": "Equity per entry",
            "help": "Notional target as a fraction of current equity.",
        },
    }

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "15m",
        "history": 30,
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        self.fast = ctx.indicators.sma(self.p.fast)
        self.slow = ctx.indicators.sma(self.p.slow)

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return

        position = ctx.position()

        if self.fast.crossed_above(self.slow) and position.is_flat:
            notional = ctx.account.equity * self.p.equity_fraction
            qty = ctx.risk.size_by_notional(notional)
            if qty > 0:
                ctx.buy(qty=qty, tag="sma-entry")
        elif self.fast.crossed_below(self.slow) and position.is_long:
            ctx.close(tag="sma-exit")

        ctx.record("spread_pct", (self.fast.value - self.slow.value) / self.slow.value * 100)
```

> Validator: `ok=True`, `warmup=30`, 500 bars, 12 orders, zero diagnostics.
>
> `ctx.account.equity * self.p.equity_fraction` is `Money * Money` — exact. A float literal
> there would raise `TypeError`.

### 18.3 Level 2 — breakout with a stop, attached correctly

This is the **canonical pattern**: entry in `on_bar`, protective stop in `on_fill`.

```python
from perplab import Strategy


class ATRBreakout(Strategy):
    """Donchian breakout, risk-sized off the ATR, with the stop attached on fill.

    The entry and its protective stop are deliberately in two different hooks: an order
    submitted inside `on_bar` is still in flight when that hook returns, so
    `ctx.stop_loss()` called there would raise "protects an open position and BTCUSDT is
    flat". `on_fill` runs once the fill has been booked, which is where the position the
    stop protects actually exists.
    """

    params = {
        "channel": {"type": "int", "default": 20, "min": 5, "max": 200},
        "atr_period": {"type": "int", "default": 14, "min": 2, "max": 100},
        "stop_atr": {"type": "decimal", "default": "2.0", "min": "0.5", "max": "10"},
        "risk": {"type": "decimal", "default": "0.01", "min": "0.001", "max": "0.05"},
        "leverage": {"type": "int", "default": 3, "min": 1, "max": 125},
        "allow_shorts": {"type": "bool", "default": False},
    }

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "1h",
        "history": 21,
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        # Leverage is set here, before anything can be open: `ctx.set_leverage` is
        # refused while a position exists on the symbol.
        ctx.set_leverage(self.p.leverage)
        self.channel = ctx.indicators.donchian(self.p.channel)
        self.atr = ctx.indicators.atr(self.p.atr_period)
        self.pending_stop = None

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return

        position = ctx.position()
        price = ctx.mark()
        atr = ctx.money(self.atr.value)

        if not position.is_flat:
            return
        if ctx.open_orders():
            # Something is still in flight; do not stack a second entry on top of it.
            return

        # `.upper.prev` is the channel as it stood *before* this bar was folded in —
        # comparing against `.upper` would compare the bar with a channel that already
        # contains it, and nothing would ever break out.
        upper = ctx.money(self.channel.upper.prev)
        lower = ctx.money(self.channel.lower.prev)

        if price > upper:
            stop = price - self.p.stop_atr * atr
            self._enter(ctx, "BUY", entry=price, stop=stop)
        elif self.p.allow_shorts and price < lower:
            stop = price + self.p.stop_atr * atr
            self._enter(ctx, "SELL", entry=price, stop=stop)

    def _enter(self, ctx, side, *, entry, stop):
        qty = ctx.risk.size_by_stop(entry=entry, stop=stop, risk_fraction=self.p.risk)
        if qty <= 0:
            ctx.log.warn("size floored to zero", entry=str(entry), stop=str(stop))
            return
        if side == "BUY":
            ctx.buy(qty=qty, tag="breakout")
        else:
            ctx.sell(qty=qty, tag="breakout")
        self.pending_stop = stop

    def on_fill(self, ctx, fill):
        if fill.reduce_only or self.pending_stop is None:
            return
        ctx.stop_loss(stop_price=self.pending_stop, tag="protective")
        ctx.log.info("stop attached", fill_price=str(fill.price), stop=str(self.pending_stop))
        self.pending_stop = None

    def on_cancel(self, ctx, event):
        # Fires for CANCELLED, EXPIRED and REJECTED alike; `status` tells them apart.
        ctx.log.warn("order ended unfilled", order_id=event.order_id, status=event.status)
        if event.status == "REJECTED":
            self.pending_stop = None
```

> Validator: `ok=True`, hooks `on_bar/on_cancel/on_fill/on_start`, `warmup=21`, zero
> diagnostics.
>
> `history: 21` is exactly `Donchian(20).warmup == period + 1`, which is why there is no
> `warmup-generous` info line.

### 18.4 Level 3 — resting limit orders and requoting

Demonstrates post-only quoting, `ctx.modify`, cleanup in `on_stop`, and the `tick_size`
guard the validator requires.

```python
from perplab import Strategy


class MeanReversionMaker(Strategy):
    """Post-only mean reversion: quote the far band, requote when the quote dies.

    Two harness asymmetries this is written around:
      * Resting orders never fill in the validator's smoke run, so `on_fill` is not
        exercised there. The code is still correct; validation simply cannot prove it.
      * `ctx.tick_size()` raises DataUnavailable under the smoke run (the synthetic market
        carries no exchange filters), so it is guarded and falls back to a declared param.
        Without the guard the strategy fails validation while running fine.
    """

    params = {
        "period": {"type": "int", "default": 20, "min": 5, "max": 200},
        "deviations": {"type": "float", "default": 2.0, "min": 0.5, "max": 5.0},
        "rsi_period": {"type": "int", "default": 14, "min": 2, "max": 100},
        "rsi_floor": {"type": "float", "default": 30.0, "min": 1.0, "max": 50.0},
        "notional": {"type": "decimal", "default": "500", "min": "10", "max": "100000"},
        "fallback_tick": {"type": "decimal", "default": "0.10", "min": "0.00000001"},
        "side": {
            "type": "choice",
            "default": "long_only",
            "choices": ["long_only", "short_only", "both"],
            "label": "Which side to quote",
        },
    }

    requires = {
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "history": 20,
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        self.bands = ctx.indicators.bollinger(self.p.period, self.p.deviations)
        self.rsi = ctx.indicators.rsi(self.p.rsi_period)
        self.quote_id = None

    def _tick(self, ctx):
        try:
            return ctx.tick_size()
        except RuntimeError:
            # DataUnavailable subclasses RuntimeError. The smoke run carries no
            # exchange filters; a real run does.
            return self.p.fallback_tick

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return

        if not ctx.position().is_flat:
            return

        if self.rsi.value is None or self.rsi.value > self.p.rsi_floor:
            if self.quote_id is not None:
                ctx.cancel(self.quote_id)
                self.quote_id = None
            return

        tick = self._tick(ctx)
        target = ctx.money(self.bands.lower.value)
        target = target - (target % tick)          # align to the tick grid

        qty = ctx.risk.size_by_notional(self.p.notional)
        if qty <= 0:
            return

        if self.quote_id is None:
            self.quote_id = ctx.buy(
                qty=qty, type="LIMIT", price=target, tif="GTX", tag="quote"
            )
        else:
            ctx.modify(self.quote_id, price=target, qty=qty)

    def on_fill(self, ctx, fill):
        if fill.reduce_only:
            return
        self.quote_id = None
        ctx.take_profit(stop_price=ctx.money(self.bands.value), tag="revert-target")

    def on_cancel(self, ctx, event):
        if event.order_id == self.quote_id:
            self.quote_id = None

    def on_stop(self, ctx):
        ctx.cancel_all()
        ctx.close_all()
```

> ⚠️ **A backtest of this strategy is not trustworthy below `BOOK_TICKER` tier** — its edge
> depends on whether a resting quote would have been hit, which a trade tape cannot answer.
> See [§5](#5-fill-tiers). Paper-trade it instead.

### 18.5 Level 4 — multi-symbol with regime gating

```python
from perplab import Strategy


class RegimeGatedMomentum(Strategy):
    """Momentum, but only in trending regimes, sized down when volatility is high."""

    params = {
        "fast": {"type": "int", "default": 20, "min": 5, "max": 100},
        "slow": {"type": "int", "default": 60, "min": 10, "max": 400},
        "adx_floor": {"type": "float", "default": 25.0, "min": 10.0, "max": 50.0},
        "risk": {"type": "decimal", "default": "0.005", "min": "0.001", "max": "0.02"},
        "vol_target": {"type": "decimal", "default": "0.02", "min": "0.005", "max": "0.10"},
    }

    requires = {
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "timeframe": "1h",
        "history": 120,
        "datasets": ["klines"],
    }

    def on_start(self, ctx):
        self.sig = {}
        for symbol in ctx.symbols:
            self.sig[symbol] = {
                "fast": ctx.indicators.ema(self.p.fast, symbol=symbol),
                "slow": ctx.indicators.ema(self.p.slow, symbol=symbol),
                "adx": ctx.indicators.adx(14, symbol=symbol),
                "vol": ctx.indicators.realised_volatility(20, symbol=symbol),
                "atr": ctx.indicators.atr(14, symbol=symbol),
            }
        self.pending = {}

    def on_bar(self, ctx, bar):
        if not ctx.warm:
            return

        symbol = bar.symbol
        s = self.sig[symbol]
        position = ctx.position(symbol)

        trending = s["adx"].value is not None and s["adx"].value >= self.p.adx_floor

        # Exit whenever the regime stops being one we trade.
        if not position.is_flat and not trending:
            ctx.close(symbol, tag="regime-off")
            return

        if not position.is_flat or not trending:
            return
        if ctx.open_orders(symbol):
            return

        if not s["fast"].crossed_above(s["slow"]):
            return

        price = ctx.mark(symbol)
        atr = ctx.money(s["atr"].value)
        stop = price - 2 * atr

        # Scale risk down when realised volatility is above target.
        realised = ctx.money(s["vol"].value or 0)
        scale = ctx.money(1)
        if realised > 0:
            scale = min(ctx.money(1), self.p.vol_target / realised)

        qty = ctx.risk.size_by_stop(
            entry=price, stop=stop, risk_fraction=self.p.risk * scale, symbol=symbol
        )
        if qty > 0:
            ctx.buy(symbol, qty=qty, tag="momo")
            self.pending[symbol] = stop

    def on_fill(self, ctx, fill):
        stop = self.pending.pop(fill.symbol, None)
        if stop is not None and not fill.reduce_only:
            ctx.stop_loss(fill.symbol, stop_price=stop, tag="protective")
```

Points to note:
- Indicators are **per symbol** — pass `symbol=` when registering, and keep them in a dict.
- `bar.symbol` tells you which symbol's bar you are handling.
- Every `ctx` call takes the symbol explicitly in a multi-symbol run.
- `self.p.risk * scale` is `Decimal * Decimal` — exact.

> ⚠️ **This one validates but will not backtest far on this installation.** Validation uses
> synthetic data, so any symbol passes. The lake holds only **8 days of ETHUSDT klines**
> (2026-06-01 → 06-08); a real run over a longer range raises `CoverageError`. Either ingest
> ETHUSDT first or change `requires["symbols"]` to `["BTCUSDT"]`.

---

## 19. Anti-patterns and pre-flight checklist

### Errors you will hit, and what they mean

| Message | Cause | Fix |
|---|---|---|
| `` `requires["symbols"]` must be a non-empty list of symbols `` | `requires` missing or at module level | Make it a **class attribute** |
| `on_bar() takes 2 positional arguments but 3 were given` | Wrong hook signature | `def on_bar(self, ctx, bar)` |
| `ctx.stop_loss() protects an open position and BTCUSDT is flat` | Stop attached in the same hook as the entry | Attach in `on_fill` |
| `... is blocked during warm-up: N of M bars fed` | Ordering before `ctx.warm` | `if not ctx.warm: return` |
| `unsupported operand type(s) for *: 'decimal.Decimal' and 'float'` | Money × float | `ctx.money("0.1")` or a `decimal` param |
| `cannot change leverage while a position is open` | `set_leverage` while holding | Call it in `on_start` or when flat |
| `'ETHUSDT' is not in this run` | Symbol not declared | Add it to `requires["symbols"]` |
| `unknown risk limit(s) ...` | Misspelled limit | A misspelled limit would silently do nothing |

### Anti-patterns

- **Don't** build indicators in `__init__` — they will not register.
- **Don't** assume an order filled because you submitted it. Check `ctx.position()`, or use
  `on_fill`.
- **Don't** use `ctx.book()` in a strategy you intend to backtest over history — only 5 days
  of depth exist.
- **Don't** trust a `TRADE_ONLY` backtest of a resting-limit strategy.
- **Don't** run live with `RISK_UNBOUNDED`.
- **Don't** tune parameters against the whole history and call it validated — that is what
  walk-forward and the overfitting diagnostic are for.
- **Don't** store mutable state on `ctx`; it is sealed. Use `self`.

### Before you trust a backtest

1. What **fill tier** did it execute at? (Badge on the run.) If `TRADE_ONLY`, does your edge
   depend on resting fills?
2. Is `FILL_TIER_LIMITED_BY_GAPS` set?
3. Does **PnL attribution** show the edge in `price`, or is the result an artefact of one
   line?
4. How many **round trips**? A Sharpe from 11 trades is noise.
5. Does it survive **walk-forward**?
6. Is the parameter a **plateau or a spike** in the sweep?
7. What does **Monte Carlo** say the drawdown distribution looks like?

### Before you go live

1. Paper-trade the exact strategy first, for a meaningful duration.
2. Read the **parity report** — does the paper session match its shadow backtest?
3. Set **real risk limits**. `RISK_UNBOUNDED` means nothing bounds size, loss, streak or rate.
4. API key: **Reading + Futures only**. Never enable withdrawals.
5. Start small. Leverage on the Start Session form; the venue applies it before your first
   order and the session aborts if the echo disagrees.
6. Know where the **KILL** button is.

---

## Appendix — quick reference

### Minimum viable strategy

```python
from perplab import Strategy


class Minimal(Strategy):
    requires = {"symbols": ["BTCUSDT"], "timeframe": "1m", "history": 0}

    def on_bar(self, ctx, bar):
        if ctx.position().is_flat:
            ctx.buy(qty=ctx.money("0.01"))
```

### Timeframes

`1m` `3m` `5m` `15m` `30m` `1h` `2h` `4h` `6h` `8h` `12h` `1d` `3d` `1w`

### Datasets

`klines` `aggTrades` `bookTicker` `depth20` `markPrice` `funding` `metrics` `liquidations`*
`macroGlobal` `macroFx`
&nbsp;&nbsp;*\* declarable but never delivers — see finding F2*

### Param types

`int` `float` `decimal` `bool` `str` `choice`

### Fill tiers, weakest to strongest

`BAR_CLOSE` → `TRADE_ONLY` → `BOOK_TICKER` → `BOOK_WALK`

### The three commands

```bash
python -m perplab serve                    # everything
python -m perplab collect --supervise      # record live data
python -m perplab ingest --dry-run ...     # always dry-run first
```
