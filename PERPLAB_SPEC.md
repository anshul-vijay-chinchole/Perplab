# PerpLab — Technical Specification & Build Plan

**Version:** 1.0 (post-review)
**Target:** Personal, code-first algorithmic trading platform for Binance USDⓈ-M perpetual futures
**Status:** Ready to build
**Author:** Anshul ("Ace")

---

## 0. How to Use This Document

This is the single source of truth for building PerpLab. It is written to be implementable section by section without further design work.

Three things are non-negotiable and every implementation decision defers to them:

1. **Realism over convenience.** If a shortcut makes the backtester optimistic, it is a bug, not a simplification.
2. **One strategy, three engines.** Strategy code is byte-identical across backtest, papertrade, and live. Any divergence is an architecture defect.
3. **The math is checked, not assumed.** Every accounting formula in §3 has a derivation and a worked numeric example. Implementations must reproduce those examples exactly before anything else is built on top.

§14 (Review Log) records issues found during the review pass and how each was resolved. §15 (Known Limitations) is the honest list of what this platform will still get wrong — read it before trusting any result.

---

## 1. Product Definition

### 1.1 What PerpLab Is

A locally-hosted web application for researching, testing, and running algorithmic trading strategies on Binance USDⓈ-M perpetual futures. Single user. Single exchange. Code-first — strategies are Python files, not drag-and-drop blocks.

The core loop:

```
write strategy → backtest → stress test (Lab) → papertrade → live
      ↑                                                        │
      └────────────────── iterate ─────────────────────────────┘
```

### 1.2 In Scope

- Binance USDⓈ-M perpetuals only (USDT-margined). No spot, no COIN-M, no options, no other exchanges.
- Isolated margin, one-way position mode, in v1.
- Bar-level and tick-level strategies down to ~1 second resolution.
- Backtesting, papertrading (Binance testnet), live trading.
- Advanced validation: walk-forward, Monte Carlo, regime analysis, overfitting diagnostics, portfolio backtesting.

### 1.3 Out of Scope (Deliberate — "T3")

These are excluded because they require infrastructure whose cost is unjustifiable for non-HFT strategies:

| Excluded | Why |
|---|---|
| Full L3 order book reconstruction | Requires order-lifecycle data; months of engineering; no edge at our timescales |
| Queue-position-exact fill simulation | Needs L3; approximated instead (§6.5) |
| Spoofing / iceberg detection | Research toys at our frequency |
| Sub-second latency-optimised execution | We are not competing on speed |
| Market making / quote management | Different business entirely |
| Cross-exchange arbitrage | No multi-exchange infra by design |
| Cross margin mode | Deferred to v2 — isolated is simpler and safer (§3.7) |
| Hedge mode (simultaneous long+short) | Deferred to v2 |

### 1.4 Design Principles

- **Deterministic.** Same inputs + same seed → byte-identical output. Enforced by test.
- **Event-sourced.** Account state is derived by folding an append-only event log, never mutated ad hoc. Debugging is replay.
- **Conservative by default.** Where a modelling choice is ambiguous, pick the one that makes the strategy look worse.
- **Fail loudly.** Missing data, unreachable brackets, unsupported order sizes → hard error, never silent interpolation.
- **Everything on-platform.** No terminal for any normal workflow. Terminal is for `docker compose up` / `python -m perplab` at install time only.

---

## 2. System Architecture

### 2.1 Component Map

```
┌──────────────────────────────────────────────────────────────┐
│ FRONTEND  (React + TypeScript + Vite)                        │
│  Dashboard │ Strategies │ Runs │ Lab │ Data & Feed │ Settings│
│  Monaco editor · lightweight-charts · WS live feed           │
└───────────────┬──────────────────────────────────────────────┘
                │ REST (control) + WebSocket (live state)
┌───────────────▼──────────────────────────────────────────────┐
│ API LAYER  (FastAPI, async)                                  │
│  strategy CRUD · run orchestration · data mgmt · key session │
└───────────────┬──────────────────────────────────────────────┘
                │
    ┌───────────┼────────────────┬──────────────────┐
    │           │                │                  │
┌───▼─────┐ ┌───▼──────────┐ ┌───▼────────────┐ ┌───▼─────────┐
│ JOB     │ │ EXECUTION    │ │ DATA SERVICE   │ │ KEY SESSION │
│ RUNNER  │ │ CORE         │ │ ingest·gap·    │ │ in-memory   │
│ process │ │ (shared by   │ │ query·version  │ │ only        │
│ pool    │ │  all 3 modes)│ │                │ │             │
└───┬─────┘ └───┬──────────┘ └───┬────────────┘ └───┬─────────┘
    │           │                │                  │
    │      ┌────▼─────┐    ┌─────▼──────┐      ┌────▼──────┐
    │      │ ACCOUNT  │    │ Parquet /  │      │ Binance   │
    │      │ ENGINE   │    │ DuckDB     │      │ REST + WS │
    │      │ (§3)     │    │ local disk │      │           │
    │      └──────────┘    └────────────┘      └───────────┘
    │
┌───▼──────────────────────────────────────────────────────────┐
│ COLLECTOR SERVICE (always-on, independent of everything else) │
│  records depth20 · markPrice · liquidations · aggTrades       │
│  → history only accumulates forward, so this starts on day 1  │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| Language (backend) | Python 3.11+ | Ecosystem, and you already know it from TradeLab |
| API | FastAPI (async) | Native WS support, typed, fast to build |
| Compute | NumPy + Polars | Polars over pandas: faster, lazy evaluation, better Parquet integration, no index footguns |
| Query layer | DuckDB | SQL directly over Parquet, no server process, handles 100+ GB on a laptop |
| Storage (market data) | Parquet on local disk | Columnar, compresses ~5–10×, partition pruning |
| Storage (metadata) | SQLite | Strategies, runs, jobs, versions, dataset manifests. Single file, zero ops |
| Job execution | `ProcessPoolExecutor` + SQLite jobs table | No Redis/Celery ops burden for a single-user app. Upgradeable later |
| Money math | `decimal.Decimal` at accounting layer | Float accumulation error in balances is unacceptable (§3.1) |
| Frontend | React 18 + TypeScript + Vite | Familiar from LiquidSim/Momentum |
| Editor | Monaco | VS Code's editor; Python syntax, inline diagnostics |
| Price charts | lightweight-charts (TradingView) | Purpose-built for financial series, handles 1M+ points |
| Stat charts | visx or Recharts | Heatmaps, histograms, scatter |
| Styling | Tailwind + CSS variables for theming | Dark/light via token swap, not duplicated stylesheets |
| Server state | TanStack Query | Caching, polling, invalidation |
| UI state | Zustand | Familiar from LiquidSim |

### 2.3 Process Model

Four long-lived processes:

1. **API server** — FastAPI/uvicorn. Never runs strategy code. Never blocks.
2. **Collector** — one asyncio process holding all Binance WebSocket subscriptions, writing to disk. Runs even when no strategy is active, because depth history only exists if you record it.
3. **Job runner pool** — N worker processes. Each backtest, walk-forward fold, or Monte Carlo batch runs here. Isolated so an infinite loop in strategy code kills one worker, not the platform.
4. **Live/paper session process** — one process per active live or paper strategy. Isolation means one strategy crashing cannot take down another that holds a real position.

**Strategy code isolation is a *stability* boundary, not a *security* boundary.** It is your own code. Workers get a wall-clock timeout, a memory cap, and no filesystem write access outside their run directory — enough to stop a bug from eating the machine, not enough to stop deliberate malice. Do not import strategies from strangers.

### 2.4 Repository Layout

```
perplab/
├── perplab/
│   ├── core/
│   │   ├── types.py            # Order, Fill, Position, Bar, Event dataclasses
│   │   ├── money.py            # Decimal helpers, quantisation to tick/step
│   │   ├── account.py          # §3 accounting engine — the crown jewels
│   │   ├── margin.py           # brackets, IM, MM, liquidation
│   │   ├── funding.py          # funding settlement
│   │   └── invariants.py       # runtime conservation assertions
│   ├── engine/
│   │   ├── clock.py            # deterministic event ordering
│   │   ├── executor_base.py    # the shared ExecutionEngine contract
│   │   ├── backtest.py
│   │   ├── paper.py
│   │   ├── live.py
│   │   ├── fills.py            # fill models (§6.4–6.5)
│   │   └── latency.py
│   ├── strategy/
│   │   ├── base.py             # Strategy base class — user-facing API
│   │   ├── context.py          # data/indicator/risk accessors
│   │   ├── indicators.py       # causal indicator library
│   │   └── validate.py         # import-time validation
│   ├── data/
│   │   ├── schemas.py
│   │   ├── ingest_bulk.py      # data.binance.vision
│   │   ├── ingest_rest.py
│   │   ├── collector.py        # live WS recorder
│   │   ├── gaps.py
│   │   ├── manifest.py         # dataset hashing/versioning
│   │   └── query.py            # DuckDB views
│   ├── analytics/
│   │   ├── metrics.py          # §8 formulas
│   │   ├── trades.py           # round-trip reconstruction
│   │   └── attribution.py      # PnL split: price / funding / fees
│   ├── lab/
│   │   ├── walkforward.py
│   │   ├── montecarlo.py
│   │   ├── regime.py
│   │   ├── overfit.py
│   │   └── portfolio.py
│   ├── risk/
│   │   ├── limits.py
│   │   └── killswitch.py
│   ├── exchange/
│   │   ├── rest.py             # signed REST client
│   │   ├── ws.py               # stream manager w/ reconnect
│   │   ├── filters.py          # exchangeInfo rules
│   │   └── keys.py             # in-memory key session
│   └── api/                    # FastAPI routers
├── frontend/
├── userdata/                   # gitignored
│   ├── strategies/
│   ├── market/                 # Parquet lake
│   ├── runs/
│   ├── reference/              # exchangeInfo + leverageBracket snapshots
│   └── perplab.db              # SQLite
└── tests/
    ├── unit/
    ├── golden/                 # hand-computed scenarios (§12)
    └── property/
```

---

## 3. Domain Model & Financial Mathematics

**This section is the foundation. Implement and test it before writing a single line of engine code.**

### 3.1 Conventions

| Concept | Convention |
|---|---|
| Time | Integer epoch **milliseconds, UTC**. No naive datetimes anywhere, ever. |
| Bar timestamp | A bar is keyed by its **open time** (Binance convention). It has a distinct `close_time`. A strategy may only see a bar at or after its `close_time`. |
| Position sign | `Q` = signed quantity. `Q > 0` long, `Q < 0` short, `Q = 0` flat. `q = |Q|`. |
| Prices | Always quote currency (USDT). |
| Money/quantity math | `Decimal`, quantised to the symbol's `tickSize` (prices) and `stepSize` (quantities). |
| Indicator math | `float64` is fine — indicators are heuristics, not ledgers. Never let a float touch a balance. |
| Fees | Always positive numbers, always **subtracted** from wallet balance. |
| Rounding | Prices: round **against** the trader (buy limit rounds down, sell limit rounds up). Quantities: round **down** to `stepSize`, never up. |

**Why Decimal matters:** a backtest over 2 years of 1-minute BTC data with a moderately active strategy produces on the order of 10⁴–10⁵ balance mutations. Float64 error is individually tiny but the reconciliation assertions in §3.10 will fail on it, and you will waste days hunting a "bug" that is just IEEE-754. Accounting events are rare compared to data events, so Decimal's cost is negligible.

### 3.2 Instrument Model — Exchange Filters

Every symbol carries a filter set from `GET /fapi/v1/exchangeInfo`. **Orders that violate these are rejected by Binance in live and must be rejected identically in backtest.** A backtest that lets you buy 0.13847362 BTC when `stepSize = 0.001` is fiction.

| Filter | Field | Enforcement |
|---|---|---|
| `PRICE_FILTER` | `tickSize`, `minPrice`, `maxPrice` | Quantise all limit/stop prices |
| `LOT_SIZE` | `stepSize`, `minQty`, `maxQty` | Quantise all quantities; reject below `minQty` |
| `MARKET_LOT_SIZE` | separate `stepSize`/`maxQty` for market orders | Market orders often have a lower max |
| `MIN_NOTIONAL` | `notional` | Reject if `qty × price < minNotional` (commonly 5 USDT — read it, don't hardcode) |
| `PERCENT_PRICE` | `multiplierUp/Down` | Limit orders too far from mark price are rejected |
| `MAX_NUM_ORDERS` | count | Cap simultaneous open orders |
| `MAX_NUM_ALGO_ORDERS` | count | Cap stop/TP orders separately |
| Precision | `pricePrecision`, `quantityPrecision` | Decimal places |

**Filters change over time.** Snapshot `exchangeInfo` and `leverageBracket` on every data refresh into `userdata/reference/`, timestamped. A backtest over 2023 data must use the 2023 snapshot, not today's. If no snapshot exists for the backtest period, the run is flagged `FILTERS_APPROXIMATE` in its metadata and the results page shows a warning badge. Do not silently use current filters for old data.

### 3.3 Position & Account Accounting

**State:**

```
wallet_balance   W   Decimal, changes ONLY via realized PnL, fees, funding
position         Q   signed Decimal
entry_price      Pe  Decimal, VWAP of the open position
mark_price       Pm  Decimal, from the exchange (never recomputed — §3.4)
```

**Unrealised PnL:**

```
uPnL = Q · (Pm − Pe)
```
*Check:* long 1 @ 50 000, mark 51 000 → `1 × 1000 = +1000` ✓. Short 1 (`Q = −1`) @ 50 000, mark 49 000 → `−1 × (−1000) = +1000` ✓.

**Equity (margin balance):**
```
E = W + uPnL
```

**Fill application** — the single most bug-prone function in the codebase. Given existing signed position `Q`, entry `Pe`, and an incoming fill of signed quantity `f` at price `Pf`:

**Case A — open or increase** (`Q = 0` or `sign(f) = sign(Q)`):
```
Pe' = (|Q|·Pe + |f|·Pf) / (|Q| + |f|)
Q'  = Q + f
realized = 0
```

**Case B — reduce** (`sign(f) ≠ sign(Q)` and `|f| ≤ |Q|`):
```
realized = sign(Q) · |f| · (Pf − Pe)
Q'  = Q + f
Pe' = Pe                          (unchanged — critical)
if Q' = 0: Pe' = None
```

**Case C — flip** (`sign(f) ≠ sign(Q)` and `|f| > |Q|`):
```
realized = sign(Q) · |Q| · (Pf − Pe)
Q'  = Q + f
Pe' = Pf                          (residual opens at the fill price)
```

In all cases:
```
fee = |f| · Pf · fee_rate
W' = W + realized − fee
```

*Check Case B, long:* `Q=+1, Pe=50000`, sell `0.5 @ 52000` → `realized = +1 × 0.5 × 2000 = +1000` ✓
*Check Case B, short:* `Q=−1, Pe=50000`, buy `0.5 @ 48000` → `realized = −1 × 0.5 × (−2000) = +1000` ✓
*Check Case C:* `Q=+1, Pe=50000`, sell `1.5 @ 52000` → `realized = +1000`, `Q' = −0.5`, `Pe' = 52000` ✓

### 3.4 Mark Price

Mark price is the manipulation-resistant reference used for **unrealised PnL and liquidation triggers**. It is *not* the last traded price. Fills happen at traded/book prices; risk happens at mark price. Conflating the two is a classic and expensive bug.

Binance's construction (documented for understanding, **not** reimplemented):

```
Mark = median( Price₁ , Price₂ , ContractPrice )

Price₁ = IndexPrice × (1 + LastFundingRate × (timeToNextFunding / fundingInterval))
Price₂ = IndexPrice + MovingAverage(basis)
ContractPrice = last traded perp price
IndexPrice    = volume-weighted spot price across constituent exchanges
```

**PerpLab does not compute mark price.** It ingests and stores Binance's own mark price series (`<symbol>@markPrice@1s` live; `markPriceKlines` in bulk history). Recomputing it would require multi-exchange index constituents we deliberately do not have, and would guarantee divergence from the exchange that actually liquidates you.

Between stored mark price samples, mark price is held constant (step function, last-observation-carried-forward). It is **never** interpolated — interpolation invents prices that never existed and can fabricate or hide liquidations.

### 3.5 Funding

**Payment formula**, applied at each settlement timestamp `t`:

```
funding_cashflow = − Q · Pm(t) · F(t)
W' = W + funding_cashflow
```

Where `F(t)` is the funding rate at settlement and `Pm(t)` is the mark price at that instant.

*Sign check:*
- Long 1 BTC, mark 50 000, `F = +0.0001` → `−1 × 50000 × 0.0001 = −5.00`. Long pays 5 USDT ✓
- Short 1 BTC (`Q = −1`), same → `+5.00`. Short receives ✓
- Long, `F = −0.0001` → `−1 × 50000 × (−0.0001) = +5.00`. Long receives ✓

**Rules:**

1. **Discrete events, never amortised.** A position open at the settlement instant pays/receives in full. A position closed one second before pays nothing. Strategies that trade around funding depend entirely on this being exact.
2. **Use actual historical rates and timestamps** from `GET /fapi/v1/fundingRate` (also available in bulk). Do not compute the rate from the premium index formula, and do not assume a schedule.
3. **Do not hardcode an 8-hour interval.** Binance runs different intervals on different symbols, and has changed intervals on existing symbols. Read `fundingIntervalHours` per symbol *and* derive actual settlement times from the historical record.
4. **Funding is applied before the liquidation check** in the event loop. A funding payment reduces margin balance and can itself trigger liquidation. Ordering matters (§6.2).
5. Funding is tracked separately in PnL attribution (§8.4) — for perp strategies, knowing what fraction of PnL came from funding versus price is essential.

### 3.6 Margin & Leverage Brackets

Binance uses tiered maintenance margin. Larger notional → higher maintenance margin rate and lower max leverage.

Pull from `GET /fapi/v1/leverageBracket` and snapshot it. **Never hardcode the table** — it changes per symbol and over time.

```
notional  N   = q · Pm
bracket   i   = the bracket where notionalFloor ≤ N ≤ notionalCap
MMR_i         = maintenance margin rate for bracket i
MA_i          = cumulative maintenance amount (deduction) for bracket i

MM = N · MMR_i − MA_i
IM = N_entry / L          where L ≤ bracket max leverage
```

**The circularity:** the bracket depends on notional, notional depends on mark price, and the liquidation price we are solving for *is* a mark price. Resolution:

1. Resolve the bracket using notional at the current mark price.
2. Solve for `P_liq` (§3.7).
3. Recompute notional at `P_liq`. If it falls in a *different* bracket, re-solve with that bracket's `MMR`/`MA` and repeat.
4. Iterate to a fixed point (converges in ≤ 3 iterations in practice). Cap at 8 iterations; if it does not converge, raise — do not return a guess.

### 3.7 Liquidation

**Derivation** (isolated margin, single position, one-way mode). Liquidation triggers when margin balance falls to maintenance margin:

```
W + Q·(Pm − Pe)  ≤  q·Pm·MMR − MA
```

Solve for `Pm`:
```
W + Q·Pm − Q·Pe  =  q·MMR·Pm − MA
W − Q·Pe + MA    =  Pm·(q·MMR − Q)
```

```
┌──────────────────────────────────────┐
│  P_liq = (W − Q·Pe + MA)             │
│          ─────────────────           │
│            (q·MMR − Q)               │
└──────────────────────────────────────┘
```

Where `W` here is the **isolated margin allocated to this position** (initial margin plus any added margin), not the whole wallet.

*Worked check — 10× long:* `Q = +1, q = 1, Pe = 50 000, W = 5 000, MMR = 0.004, MA = 0`
```
P_liq = (5000 − 50000 + 0) / (1×0.004 − 1) = −45000 / −0.996 = 45 180.72
```

*Worked check — 10× short:* `Q = −1, q = 1, Pe = 50 000, W = 5 000, MMR = 0.004, MA = 0`
```
P_liq = (5000 + 50000 + 0) / (1×0.004 + 1) = 55000 / 1.004 = 54 780.88
```

**Bankruptcy price** (margin balance = 0):
```
P_bank = Pe − W/Q
```
Long: `50000 − 5000/1 = 45 000`. Short: `50000 − 5000/(−1) = 55 000`.

*Sanity invariant, assert in code:* for a long, `P_bank < P_liq < Pe`; for a short, `Pe < P_liq < P_bank`. Check: `45 000 < 45 180 < 50 000` ✓ and `50 000 < 54 781 < 55 000` ✓. If this invariant ever fails, the bracket resolution is wrong.

**Trigger condition** — evaluated against the **mark price series**, not last price:
- Long: liquidated when `Pm ≤ P_liq`
- Short: liquidated when `Pm ≥ P_liq`

**Liquidation modelling in backtest (conservative):**

When triggered, the position is closed and **the entire isolated margin `W` allocated to that position is lost**. This is the realistic outcome for isolated margin: Binance's liquidation engine closes the position and the clearance fee plus adverse fill consume the remaining margin. Modelling a "clean" close at exactly `P_liq` with leftover margin returned would be optimistic and would make blow-up scenarios look survivable.

Configurable knob `liquidation_recovery_pct` (default `0.0`) allows a fraction of remaining margin to be returned, for sensitivity analysis. Default assumption is total loss.

**Not modelled in v1** (see §15): partial/tiered liquidation of very large positions, auto-deleveraging (ADL), insurance fund dynamics.

**Why isolated-only in v1:** under cross margin, liquidation price for one symbol depends on wallet balance and the unrealised PnL of every other open position, so there is no closed form — it requires numerically solving for one symbol's mark price while holding all others fixed, and it re-solves on every price change of any symbol. That is a meaningful chunk of engineering for a feature that also makes one bad strategy able to liquidate every other position in the account. Isolated is both simpler and safer. Cross is a v2 item.

### 3.8 Fees

```
fee = |fill_qty| · fill_price · rate
```

`rate` depends on maker/taker and VIP tier, with an optional BNB discount. **Do not hardcode.** Fetch from `GET /fapi/v1/commissionRate` when a live session is connected, cache into the reference snapshot, and expose as an overridable run parameter so backtests can be run at a pessimistic rate.

**Maker vs taker determination in backtest:**
- Market order → always taker.
- Limit order that would cross the spread on submission → taker (Binance treats it as an immediate-or-partial taker fill).
- Limit order that rests and is later filled → maker.
- `STOP_MARKET` / `TAKE_PROFIT_MARKET` when triggered → taker.
- Liquidation → taker, plus total-margin-loss model above.

Defaulting everything to taker is a safe conservative fallback and is the recommended initial setting until the maker/taker classifier is validated against real testnet fills.

### 3.9 End-to-End Worked Example

This example is a **golden test** (§12). The implementation must reproduce every number exactly.

**Setup:** BTCUSDT, isolated, 10× leverage, taker fee 0.05% (0.0005), `stepSize = 0.001`, `tickSize = 0.10`, `MMR = 0.004`, `MA = 0`. Starting wallet `W₀ = 10 000.00 USDT`.

**t₀ — open long 0.1 BTC @ 50 000.00 (taker)**
```
notional      = 0.1 × 50 000        = 5 000.00
IM            = 5 000 / 10          =   500.00   (isolated margin allocated)
fee           = 5 000 × 0.0005      =     2.50
W             = 10 000 − 2.50       = 9 997.50
Q = +0.1 ,  Pe = 50 000.00
P_liq = (500 − 0.1×50 000 + 0) / (0.1×0.004 − 0.1)
      = (500 − 5 000) / (0.0004 − 0.1)
      = −4 500 / −0.0996 = 45 180.72
```

**t₁ — mark price 51 000.00**
```
uPnL = 0.1 × (51 000 − 50 000) = +100.00
E    = 9 997.50 + 100.00       = 10 097.50
```

**t₂ — funding settlement, F = +0.0001, mark 51 000.00**
```
cashflow = −0.1 × 51 000 × 0.0001 = −0.51   (long pays)
W        = 9 997.50 − 0.51        = 9 996.99
E        = 9 996.99 + 100.00      = 10 096.99
```

**t₃ — add 0.1 BTC @ 52 000.00 (taker, Case A)**
```
Pe' = (0.1×50 000 + 0.1×52 000) / 0.2 = 51 000.00
Q'  = 0.2
fee = 0.1 × 52 000 × 0.0005 = 2.60
W   = 9 996.99 − 2.60       = 9 994.39
```

**t₄ — close 0.15 BTC @ 53 000.00 (taker, Case B)**
```
realized = +1 × 0.15 × (53 000 − 51 000) = +300.00
fee      = 0.15 × 53 000 × 0.0005        =    3.975
W        = 9 994.39 + 300.00 − 3.975     = 10 290.415
Q'       = 0.05 ,  Pe' = 51 000.00       (entry unchanged)
```

**t₅ — mark 53 000.00, final mark-to-market**
```
uPnL = 0.05 × (53 000 − 51 000) = +100.00
E    = 10 290.415 + 100.00      = 10 390.415
```

**Reconciliation (§3.10):**
```
Σ realized       = +300.000
Σ fees           =   −9.075   (2.50 + 2.60 + 3.975)
Σ funding        =   −0.510
W₀ + Σ           = 10 000 + 300.000 − 9.075 − 0.510 = 10 290.415 ✓ matches W
E = W + uPnL     = 10 290.415 + 100.00 = 10 390.415 ✓
```

### 3.10 Conservation Invariants

These run as **assertions inside the engine**, not just as tests. They catch entire classes of accounting bug the moment they occur rather than 40 000 bars later.

| # | Invariant | Check point |
|---|---|---|
| I1 | `W == W₀ + Σrealized − Σfees + Σfunding` | Every wallet mutation |
| I2 | `E == W + Q·(Pm − Pe)` | Every mark price update |
| I3 | `Q == Σ(signed fills)` | Every fill |
| I4 | `Q == 0 ⟺ Pe is None` | Every fill |
| I5 | `W ≥ 0` unless a liquidation event was emitted | Every wallet mutation |
| I6 | Every price is an exact multiple of `tickSize`; every qty an exact multiple of `stepSize` | Every order submission |
| I7 | For a long: `P_bank < P_liq < Pe`; for a short: `Pe < P_liq < P_bank` | Every liquidation-price recompute |
| I8 | Event log timestamps are monotonically non-decreasing | Every event append |
| I9 | Sum of per-trade PnL (round-trips) + open-position uPnL == total PnL | End of run |

In backtest and paper, a failed invariant **aborts the run** with the full event log dumped. In live, a failed invariant **triggers the kill switch immediately** — an accounting engine that has lost track of state must not keep sending orders.

Tolerance: exact equality with `Decimal`. If you find yourself adding an epsilon, a float has leaked into the accounting layer — fix that instead.

---

## 4. Data Layer

### 4.1 Datasets

| Dataset | Purpose | Resolution | Source |
|---|---|---|---|
| `klines` | Backtest bars, indicators | 1m base; higher TFs derived | Bulk + REST |
| `aggTrades` | Fill realism, liquidation timing, CVD, queue model | Tick | Bulk + WS |
| `bookTicker` | Best bid/ask (spread), top-of-book fills | Tick | Bulk + WS |
| `depth20` | Book walking, imbalance, liquidity walls | 100 ms stream → stored at 1 s | **WS collector only** |
| `markPriceKlines` / `markPrice` stream | uPnL, liquidation trigger | 1 s live, 1 m bulk | Bulk + WS |
| `fundingRate` | Funding settlement events | Per settlement | Bulk + REST |
| `metrics` | Open interest, long/short ratios | 5 min | Bulk |
| `liquidations` | Cascade/sweep detection and strategies | Event | Bulk snapshot + WS |
| `reference` | exchangeInfo, leverageBracket, commissionRate | Snapshot per refresh | REST |

### 4.2 Source Availability — Reality Check

**This was the single biggest correction found during review.** The original plan assumed historical 20-level depth was downloadable. It is not, and building on that assumption would have stalled the Lab tab months in.

What is actually available in bulk from `data.binance.vision` (free, no auth) for `futures/um`:

- `klines`, `aggTrades`, `trades` — full history, free ✓
- `bookTicker` — **tick-level best bid/ask, full history, free** ✓ (this is the useful discovery)
- `bookDepth` — depth aggregated into **percentage bands from mid price**, not a raw price ladder. Useful as a *feature* (how much notional sits within ±1%), useless for exact book walking.
- `metrics` — open interest and ratios ✓
- `fundingRate` ✓
- `liquidationSnapshot` ✓
- `markPriceKlines`, `indexPriceKlines`, `premiumIndexKlines` ✓

What is **not** freely available:

- Raw N-level historical order book. Binance offers a historical order book data service (`T_DEPTH` tick-level L2, `S_DEPTH` snapshots) but access is gated behind VIP account status, and the free `S_DEPTH` sample has historically been limited to a 2-level snapshot for BTCUSDT only. Third-party vendors (Tardis.dev, Amberdata) sell it.

**Consequences — three concrete decisions:**

1. **Start the collector on day one of the build.** Depth history exists only from the moment you begin recording. Every day of delay is a permanent hole in your L2 backtest range. This moves the collector from "Phase 7 infrastructure" to **Phase 1b**, before the backtest engine exists.
2. **`bookTicker` becomes the primary fill-realism input for long historical ranges.** Tick-level best bid/ask across full history gives real spreads and real top-of-book prices — far better than assuming a flat slippage percentage, and it covers years, not just the period since your collector started.
3. **L2 book-walking backtests are scoped to the collector's coverage window.** The Runs tab must surface this explicitly: if a run's date range extends before depth coverage begins, the fill model silently degrades from `BOOK_WALK` to `BOOK_TICKER`. That degradation is recorded in run metadata and shown as a badge on the results page. Never let a fill-model downgrade happen invisibly.

Fill-model tiers, in descending fidelity:

| Tier | Requires | Used when |
|---|---|---|
| `BOOK_WALK` | `depth20` | Collector coverage exists for the range |
| `BOOK_TICKER` | `bookTicker` + `aggTrades` | Long historical ranges (default for most backtests) |
| `TRADE_ONLY` | `aggTrades` | bookTicker gap |
| `BAR_CLOSE` | `klines` | Explicit opt-in only; flagged `LOW_FIDELITY` on the results page |

### 4.3 Storage Layout

```
userdata/market/
  klines/symbol=BTCUSDT/interval=1m/year=2025/month=03/data.parquet
  aggTrades/symbol=BTCUSDT/date=2025-03-14/data.parquet
  bookTicker/symbol=BTCUSDT/date=2025-03-14/data.parquet
  depth20/symbol=BTCUSDT/date=2025-03-14/data.parquet
  markPrice/symbol=BTCUSDT/year=2025/month=03/data.parquet
  funding/symbol=BTCUSDT/data.parquet
  metrics/symbol=BTCUSDT/year=2025/data.parquet
  liquidations/symbol=BTCUSDT/date=2025-03-14/data.parquet
```

Hive-style partitioning so DuckDB prunes by symbol/date without reading files. Compression: ZSTD level 3 (better ratio than Snappy, still fast to decode). Row groups ~128 MB.

**Store 1-minute klines only.** Derive 5m/15m/1h/4h/1d by aggregation at query time. Storing every timeframe separately invites the two versions to disagree, and the aggregation is cheap in DuckDB. The one rule: aggregation must respect UTC boundaries and must produce bars whose `close_time` is exact, or the no-look-ahead guarantee (§6.2) breaks at timeframe boundaries.

### 4.4 Sizing

Per symbol per year, ZSTD-compressed Parquet, order-of-magnitude:

| Dataset | BTCUSDT (high liquidity) | Mid-cap alt |
|---|---|---|
| klines 1m | ~15 MB | ~15 MB |
| aggTrades | 3–8 GB | 0.5–2 GB |
| bookTicker | 8–20 GB | 1–4 GB |
| depth20 @ 1 s | 5–10 GB | 3–6 GB |
| markPrice 1 s | ~200 MB | ~200 MB |
| funding / metrics / liquidations | < 100 MB combined | < 100 MB |

**Realistic personal footprint:** 3 symbols × 2 years, with `aggTrades` + `bookTicker` + 1 s `depth20` → **roughly 60–120 GB**. Comfortable on a laptop SSD.

**Do not snapshot depth at 100 ms.** That is 10× the storage for resolution that only matters to strategies you have explicitly excluded (§1.3). 1 s is the right choice for liquidity walls and cascade detection.

Storage policy, enforced by the Data tab:
- `klines`, `funding`, `metrics`, `markPrice` — keep full history, negligible cost
- `aggTrades`, `bookTicker`, `depth20` — rolling window (default 24 months), only for symbols on the active list
- Retention job shows what it will delete before deleting, and never deletes data referenced by a saved run's dataset manifest without explicit confirmation

### 4.5 Ingestion, Gaps, Integrity

**Ingest pipeline:** bulk zip download → checksum verify (Binance publishes `.CHECKSUM` files — verify them, silently corrupted archives are a real failure mode) → parse → normalise types → write Parquet → update manifest.

**Gap detection** runs after every ingest and is stored per symbol/dataset:

- *Klines:* expected bar count for the interval vs actual. Note that Binance emits zero-volume bars for illiquid periods rather than omitting them — a zero-volume bar is **data**, an absent bar is a **gap**. Distinguish them.
- *Tick datasets:* gap if the inter-record interval exceeds a threshold (default 60 s) during a period where klines show non-zero volume. This cross-check catches collector dropouts that a naive "no records" test would misread as a quiet market.
- *Funding:* gap if the interval between consecutive settlements exceeds `1.5 × fundingIntervalHours`.
- *Depth (collector):* the collector writes a heartbeat record every 10 s even when nothing changes, so a WS dropout is unambiguous rather than inferred.

**Gap policy for backtests — a run over a gapped range does one of three things, chosen in run config, never defaulted silently:**

| Mode | Behaviour |
|---|---|
| `STRICT` (default) | Refuse to run. Show which gaps and how long. |
| `HALT_TRADING` | Run, but treat gaps as "no execution possible": no fills, no new orders, existing positions held and marked at last known price. Realistic simulation of an outage. |
| `SKIP` | Jump over the gap. **Requires typed confirmation** and permanently flags the run `GAP_SKIPPED`. |

Interpolating across a gap is never offered. It manufactures prices that never traded.

### 4.6 Dataset Versioning

Every run stores a **dataset manifest**:

```json
{
  "symbols": ["BTCUSDT"],
  "range": {"start_ms": 1704067200000, "end_ms": 1735689600000},
  "datasets": {
    "klines_1m":  {"files": 24, "rows": 527040, "sha256": "a3f2..."},
    "aggTrades":  {"files": 366, "rows": 891203847, "sha256": "9c11..."},
    "bookTicker": {"files": 366, "rows": 2103847221, "sha256": "44be..."}
  },
  "reference": {"exchangeInfo_snapshot": "2024-01-02", "leverageBracket_snapshot": "2024-01-02"},
  "gaps": [],
  "fill_model_tier": "BOOK_TICKER"
}
```

The `sha256` is over the sorted list of `(file_path, file_size, file_mtime_ns)` — cheap to compute and sufficient to detect any change. Re-running a saved run recomputes the manifest and **warns loudly if it differs**. "Why doesn't this backtest match the one I ran last month" should be a two-second answer, not an investigation.

---

## 5. Strategy API

### 5.1 Base Class

```python
from perplab import Strategy, Bar, Order, Position
from decimal import Decimal

class EMACross(Strategy):
    # Declared params drive the auto-generated config form in the UI.
    params = {
        "fast": {"type": "int",     "default": 12,  "min": 2,  "max": 200},
        "slow": {"type": "int",     "default": 26,  "min": 3,  "max": 400},
        "risk": {"type": "decimal", "default": "0.01", "min": "0.001", "max": "0.05"},
    }

    # Declares data needs up front so the engine can validate coverage
    # BEFORE the run starts, instead of failing 60% of the way through.
    requires = {
        "symbols":   ["BTCUSDT"],
        "timeframe": "15m",
        "history":   400,          # warm-up bars needed before first signal
        "datasets":  ["klines", "funding"],
    }

    def on_start(self, ctx):
        self.fast = ctx.indicators.ema(self.p.fast)
        self.slow = ctx.indicators.ema(self.p.slow)

    def on_bar(self, ctx, bar: Bar):
        if not ctx.warm:                       # engine gates warm-up for you
            return
        pos = ctx.position("BTCUSDT")

        if self.fast.crossed_above(self.slow) and pos.qty == 0:
            ctx.buy("BTCUSDT", qty=ctx.risk.size_by_stop(
                entry=bar.close,
                stop=bar.close * (1 - self.p.risk),
                risk_fraction=self.p.risk,
            ))
        elif self.fast.crossed_below(self.slow) and pos.qty > 0:
            ctx.close("BTCUSDT")

    # Optional hooks
    def on_tick(self, ctx, trade): ...
    def on_fill(self, ctx, fill): ...
    def on_funding(self, ctx, event): ...
    def on_liquidation(self, ctx, event): ...   # own position liquidated
    def on_market_liquidation(self, ctx, event):...# someone else's — cascade signals
    def on_stop(self, ctx): ...
```

### 5.2 Lifecycle

```
validate → on_start → [warm-up bars: indicators update, orders BLOCKED]
        → on_bar / on_tick / on_funding / on_fill / ...  (live loop)
        → on_stop → final mark-to-market → metrics
```

The engine enforces warm-up. During warm-up, indicators receive data but `ctx.buy/sell/close` raise. This removes an entire class of "strategy traded on a 3-period EMA that thought it was 200-period" bug, and it makes walk-forward folds honest: the OOS window's indicators are warmed on the immediately preceding data, exactly as they would be in live trading.

### 5.3 Context API

```python
ctx.now                       # int epoch ms — the engine's clock, never wall clock
ctx.warm                      # bool
ctx.position(symbol)          # Position(qty, entry_price, upnl, liq_price, margin)
ctx.account                   # wallet_balance, equity, available, used_margin
ctx.mark(symbol)              # current mark price
ctx.funding(symbol)           # last_rate, next_settlement_ms, predicted_rate
ctx.oi(symbol)                # latest open interest
ctx.book(symbol)              # depth snapshot — RAISES if fill tier < BOOK_WALK
ctx.spread(symbol)            # best bid/ask

ctx.buy(symbol, qty, type="MARKET", price=None, tif="GTC", reduce_only=False,
        client_id=None, tag=None)
ctx.sell(...)
ctx.close(symbol, qty=None)                  # reduce_only market by default
ctx.cancel(order_id) / ctx.cancel_all(symbol)
ctx.stop_loss(symbol, stop_price, qty=None)
ctx.take_profit(symbol, stop_price, qty=None)
ctx.trailing_stop(symbol, callback_rate, qty=None)

ctx.risk.size_by_stop(entry, stop, risk_fraction)
ctx.risk.size_by_notional(notional)
ctx.risk.max_allowed(symbol)                  # after limits, filters, margin

ctx.log.info/warn/error(msg, **fields)        # structured → run event log & Feed tab
ctx.record(name, value)                       # custom time series, charted on results page
```

**`ctx.now` is the engine clock.** Strategy code that calls `time.time()` or `datetime.now()` is a look-ahead vector and a live/backtest divergence. The validator (§5.5) rejects it.

### 5.4 Indicators — Causality Rules

Provided causal: EMA, SMA, WMA, RSI, MACD, ATR, ADX, Bollinger, Donchian, VWAP (session-anchored), realised volatility, OBV, CVD (from aggTrades, using `isBuyerMaker` for aggressor side), book imbalance (BOOK_WALK tier only), rolling funding mean, OI delta.

Hard rules:
1. An indicator updates **only on a closed bar**, and its value at bar `i` uses data through bar `i` only.
2. No indicator has a `center=True` / centred-window option. Centred moving averages are the most elegant look-ahead bug in existence and are simply not offered.
3. Indicators expose `.value`, `.prev`, `.series(n)`, `.crossed_above(other)`, `.crossed_below(other)`. `crossed_*` compares `(prev, current)` pairs — it is a discrete edge signal, never inferred from level comparison. (This is the `actionSeq` pattern from LiquidSim, and it is here for the same reason: latching on a level instead of an edge causes silent repeat-firing.)
4. Warm-up length is derived automatically from the indicator set and cross-checked against `requires["history"]`. Mismatch → validation error before the run.

### 5.5 Import-Time Validation

Every save/upload runs, in order — failures shown inline in the Monaco gutter, never as a terminal traceback:

1. **Parse** — `ast.parse`. Syntax errors → line/column highlight.
2. **AST scan** — reject `time.time`, `datetime.now`, `datetime.today`, `random` without `ctx.rng`, bare `open()`, `requests`/`urllib`/`socket`, `os.system`/`subprocess`, `exec`/`eval`. Each with a message explaining *why* (determinism, look-ahead, or reproducibility), not just "forbidden".
3. **Structure** — exactly one `Strategy` subclass; `on_bar` or `on_tick` present; `params` and `requires` well-formed.
4. **Params** — types valid, defaults within declared bounds.
5. **Smoke run** — instantiate and run 500 synthetic bars in a sandboxed worker with a 10 s timeout. Catches import-time crashes, unbound names, and shape errors before you burn 20 minutes of backtest.
6. **Determinism probe** — smoke run twice, compare event log hashes. Divergence means hidden nondeterminism (usually a set/dict iteration order or an unseeded RNG). Fail with a pointer to the likely cause.

`ctx.rng` is a seeded `random.Random` derived from the run seed. Any strategy needing randomness uses it, so runs stay reproducible.

### 5.6 Strategy Library Operations

- **New** — Monaco opens with a commented template
- **Import** — `.py` file, or a `.perplab` bundle (zip: code + `manifest.json` with params, tags, notes, and originating run summary)
- **Export** — produces the same bundle; portable across machines
- **Archive** — soft-hide, fully recoverable, excluded from library default view and from "trials" counting (§9.4)
- **Delete** — hard delete, typed confirmation, **blocked** if any non-archived run or an active live/paper session references it. Runs must remain reproducible.
- **Version history** — every save writes a new immutable version row (code + hash + timestamp). Diff view between any two versions. Every run records the exact version hash it used, so "which code produced this equity curve" is always answerable.

---

## 6. Execution Engine

### 6.1 Unified Contract

```python
class ExecutionEngine(Protocol):
    def submit(self, order: Order) -> OrderAck: ...
    def cancel(self, order_id: str) -> None: ...
    def state(self) -> AccountState: ...
    def advance(self) -> Iterator[Event]: ...   # backtest: replay; paper/live: stream
```

**Shared by all three modes (single implementation, no duplicates):**
- Account/position accounting (§3.3)
- Fee computation (§3.8)
- Funding application (§3.5)
- Margin, bracket resolution, liquidation price (§3.6–3.7)
- Filter validation and quantisation (§3.2)
- Risk limit checks (§7)
- Position sizing helpers
- Event log format and metric computation

**Differs by mode — and only this:**

| | Backtest | Paper | Live |
|---|---|---|---|
| Data source | Parquet replay | Binance WS | Binance WS |
| Clock | Simulated, event-driven | Wall clock | Wall clock |
| Order destination | Fill simulator (§6.4) | Testnet REST *or* local sim | Production REST |
| Fill source | Modelled from stored data | Testnet user-data stream *or* sim | User-data stream |
| Latency | Modelled (§6.3) | Real | Real |
| Liquidation | Modelled (§3.7) | Exchange (testnet) | Exchange |
| Speed | As fast as possible | Real time | Real time |

The rule that keeps this honest: **if a piece of logic could live in the shared core, it must.** The mode-specific classes should be thin. When you catch yourself writing the same rule twice in `backtest.py` and `live.py`, that is the defect §14-I8 was about.

### 6.2 Deterministic Event Loop

Every backtest event carries `(timestamp_ms, sequence, kind)`. Sorting by timestamp alone is **not** deterministic — thousands of events share a millisecond, and Python's sort stability over an arbitrary file read order is not a guarantee you should rely on across machines.

**Total ordering:** `(timestamp_ms, kind_priority, source_seq, dataset_id)` — fully deterministic, no ties possible.

**Within a single timestamp, `kind_priority` is fixed:**

```
0  MARK_PRICE_UPDATE     mark price moves first — risk state is current
1  FUNDING_SETTLEMENT    funding debits/credits the wallet
2  LIQUIDATION_CHECK     evaluated against updated mark AND post-funding wallet
3  BOOK_UPDATE           depth snapshot applied
4  TRADE                 aggTrade — drives resting-order fill checks
5  ORDER_FILL_CHECK      resting limit/stop orders evaluated against the trade
6  BAR_CLOSE             indicators update; on_bar invoked
7  ORDER_SUBMIT          strategy orders enter the latency queue
8  ORDER_ARRIVAL         orders whose latency has elapsed become live
```

The ordering is not arbitrary — three of these placements are load-bearing:

- **Funding (1) before liquidation check (2):** a funding payment reduces margin balance and can itself cause liquidation. Checking liquidation first would let a position survive a funding payment that should have killed it.
- **Liquidation check (2) before fill checks (5):** if you are liquidated, your resting orders are cancelled. They must not fill in the same instant.
- **Bar close (6) after trade (4):** the bar's closing trade is part of the bar. Invoking `on_bar` before processing the final trade would give the strategy a bar that had not finished forming.

**No look-ahead, enforced structurally:** a bar object is only constructed and emitted at `bar.close_time`. The partially-formed current bar has no representation the strategy can reach — there is no `ctx.current_bar` to misuse. Bars are immutable frozen dataclasses.

### 6.3 Latency Model

```
signal generated at T
  ↓  submit_latency ~ Distribution        (default: lognormal, median 120 ms, p99 600 ms)
order reaches matching engine at T + L
  ↓
fill evaluated against market data at or after T + L, never before
  ↓  ack_latency (informational; affects when on_fill is invoked)
strategy learns of the fill
```

Configurable per run: `fixed` (deterministic, use for golden tests), `lognormal` (default, realistic), or `empirical` (sampled from latencies measured during your own paper sessions — best available estimate, and free once you have paper history).

Cancels have latency too. A cancel issued at `T` does not protect you from a fill at `T + 50 ms` if cancel latency is 120 ms. This is a real and commonly-ignored source of backtest optimism.

Default latency is deliberately **not zero**. Zero-latency backtests systematically overstate performance for anything reacting to fast moves — which includes every liquidation-cascade strategy you plan to write.

### 6.4 Fill Models

**Market order (BOOK_WALK tier):** walk the depth snapshot at or after arrival time, consuming liquidity level by level:

```
remaining = qty ; cost = 0
for (price, size) in book_side_levels:          # best first
    take = min(remaining, size)
    cost += take × price
    remaining -= take
    if remaining == 0: break
if remaining > 0:                                # exhausted 20 levels
    penalty_price = worst_level_price × (1 ± depth_exhaustion_penalty)
    cost += remaining × penalty_price
    log WARNING "order exceeded visible depth"   # surfaces on results page
avg_fill = cost / qty
```

`depth_exhaustion_penalty` defaults to 0.10% beyond the worst visible level, and is deliberately pessimistic. If a strategy routinely triggers this warning, the position sizing is unrealistic for the instrument, and the results page says so rather than quietly filling at a fantasy price.

**Market order (BOOK_TICKER tier):** fill at the best ask (buy) / best bid (sell) at arrival time, plus a size-dependent impact term:
```
impact_bps = k × sqrt(order_notional / recent_1min_notional_volume)
```
Square-root impact is the standard empirical form and degrades gracefully. `k` is calibrated once from your own paper-trading fills (compare realised slippage to the model) — until then, default `k = 10` bps, chosen to be pessimistic.

**Market order (TRADE_ONLY tier):** fill at the next trade price after arrival, plus a fixed conservative spread assumption.

**Limit order — the queue-position approximation.** Without L3 data you cannot know your true queue position, so PerpLab uses a defensible proxy:

```
On arrival at price L:
    Q_ahead = visible resting size at level L        (BOOK_WALK)
              or spread-implied estimate             (BOOK_TICKER)

Then, as trades arrive at price L on the passive side:
    Q_ahead -= trade_size
    once Q_ahead <= 0, subsequent volume at L fills our order

Immediate full fill only if price trades strictly THROUGH L
    (buy limit at L fills fully when a trade prints at price < L)
```

Trade aggressor side comes from `aggTrade.isBuyerMaker`: `true` means the buyer was the maker, so the trade was **sell-aggressive** and consumes bid-side queue.

Touching a limit price is not a fill. Requiring price to trade *through* the level — or to consume the queue ahead — is the difference between a limit-order backtest that is roughly honest and one that is pure fiction. This is the single most common way limit strategies look profitable and are not.

**Stop / take-profit orders:** trigger evaluated against **mark price** by default (Binance's default `workingType = MARK_PRICE`), configurable to `CONTRACT_PRICE`. On trigger, they become market orders and take the market-order path — including latency and slippage. A stop is not a guaranteed price.

**Trailing stops:** track the extreme (highest mark since entry for a long) and trigger at `callback_rate` retracement. Update on every mark price event, not on bar close, matching Binance behaviour.

### 6.5 Partial Fills

- Market orders: filled to the size available; the unfilled remainder is handled per the depth-exhaustion rule above.
- Limit orders: fill incrementally as queue is consumed. Partial fill emits `on_fill` per increment, and the position/entry-price update runs per increment (Case A/B/C, §3.3) — not batched at the end. Strategies that assume "one order, one fill" are wrong in live and must be wrong in backtest too.
- Time-in-force: `GTC`, `IOC` (fill what's available now, cancel rest), `FOK` (all or nothing), `GTX` (post-only — cancelled if it would cross). Post-only must be modelled: it is how you guarantee maker fees, and it is also how orders silently fail to enter.

### 6.6 Liquidation Engine

Evaluated at priority 2 in every event loop iteration, after mark update and funding:

```
if Q != 0:
    recompute P_liq (with bracket fixed-point, §3.6)
    if (Q > 0 and Pm <= P_liq) or (Q < 0 and Pm >= P_liq):
        emit LIQUIDATION event
        close position at P_liq (taker path)
        W -= remaining isolated margin × (1 - liquidation_recovery_pct)
        cancel all resting orders for the symbol
        invoke on_liquidation
        if risk.halt_on_liquidation: stop the run
```

Because mark price is stored at 1 s resolution and held flat between samples (§3.4), liquidation timing is accurate to ~1 s. That is more than sufficient at our timescales; sub-second liquidation precision is an excluded (T3) concern.

### 6.7 Backtest ↔ Live Parity Testing

Architecture alone does not prevent divergence — it has to be measured.

1. **Shadow backtest.** Every paper/live session records its exact market-data inputs. On session end, a backtest is automatically re-run over that window with the same strategy version, seed, and params. A **parity report** is attached to the run: fill-count delta, average fill-price delta (bps), final-PnL delta, and any orders that filled in one and not the other.
2. **Divergence alerting.** If final PnL differs by more than a threshold (default 5% of gross PnL, or 3 bps average fill deviation), the Runs tab flags it. Persistent divergence means the fill model needs recalibration — that is exactly the feedback loop that makes the backtester get more honest over time.
3. **Live↔exchange reconciliation.** Every 60 s during a live session, fetch account state from Binance and compare against PerpLab's internal accounting: wallet balance, position size, entry price, unrealised PnL, liquidation price. Any mismatch beyond `tickSize`/`stepSize` tolerance **triggers the kill switch**. This is the check that catches a missed fill or a dropped user-data-stream message before it becomes an unhedged position.
4. **Math validation against the exchange.** On testnet, open a real position and compare PerpLab's computed `P_liq`, funding payments, and fees against Binance's reported values. This is the acceptance test for §3, and it is worth doing before trusting any live number.

---

## 7. Risk Layer & Kill Switch

Risk checks run in the **shared core**, so a limit that stops a backtest stops live trading identically. Every limit is evaluated pre-submission; violations reject the order and log the reason to the Feed.

**Per-run limits:**

| Limit | Default | Action on breach |
|---|---|---|
| `max_position_notional` | — (required) | Reject order |
| `max_leverage` | 5× | Reject order |
| `max_daily_loss` | 2% of starting equity | Halt run, close positions |
| `max_drawdown` | 15% from peak equity | Halt run, close positions |
| `max_open_orders` | 10 | Reject order |
| `max_orders_per_minute` | 30 | Reject + warn (runaway-loop guard) |
| `max_consecutive_losses` | — (optional) | Halt run |
| `halt_on_liquidation` | `true` | Stop immediately |
| `min_equity` | 50% of start | Halt run |

**Drawdown is measured on mark-to-market equity**, not closed-trade PnL. A strategy sitting in a 40% unrealised loss is in a 40% drawdown regardless of whether it has "realised" anything.

**The Kill Switch** — persistent, red, top bar, reachable from every tab, never behind a menu:

1. Immediately stops all live and paper strategy processes
2. Cancels all open orders on the exchange
3. Optionally closes all positions at market (**default: cancel-only**, because force-closing everything at market during a flash crash can be worse than the exposure — the choice is a Settings toggle and the button text states which behaviour is armed)
4. Wipes API keys from memory, ending the key session
5. Writes a `KILL_SWITCH` event to every active run's log with the timestamp and trigger source
6. Requires an explicit un-arm action before any live session can start again

**Auto-triggers:** conservation invariant failure (§3.10), live↔exchange reconciliation mismatch (§6.7), WS disconnection exceeding `max_disconnect_seconds` (default 30) while a position is open, or repeated order rejections from the exchange (default 5 consecutive).

The kill switch red (`#DC2626`) is reserved. No other element in the UI uses it — not error toasts, not negative PnL. When that colour appears, it means exactly one thing.

---

## 8. Analytics & Metrics

### 8.1 Conventions That Change the Answer

Three definitional choices that are routinely got wrong and that silently inflate results:

1. **Annualisation factor is 365, not 252.** Crypto trades 24/7/365. Using the equities convention overstates Sharpe by a factor of `√(365/252) ≈ 1.20`. Hourly: 8 760. Daily: 365.
2. **Returns are computed on a fixed time grid, not per trade.** Default daily UTC boundaries (hourly for backtests shorter than 60 days). Per-trade "Sharpe" is a different, non-comparable statistic and is reported separately and labelled as such.
3. **A trade is a round-trip: flat → flat.** Scale-ins and partial exits are *legs* within one trade, not separate trades. Counting legs as trades inflates trade count and distorts win rate. Both are shown; the round-trip figure is the headline.

### 8.2 Formulas

Let `E_t` be mark-to-market equity on the return grid, `r_t = E_t/E_{t−1} − 1`, `A` = periods per year, `N` = number of periods.

```
Sharpe          = (mean(r) − rf_p) / stdev(r, ddof=1) × √A
                  rf_p = (1 + rf_annual)^(1/A) − 1        (default rf_annual = 0)

Sortino         = (mean(r) − MAR_p) / DD × √A
                  DD = √( Σ min(r_t − MAR_p, 0)² / N )     (divide by N, all periods)

MaxDD           = min_t ( E_t / cummax(E)_t − 1 )
                  computed on EVERY engine mark-to-market tick, not grid closes

CAGR            = (E_T / E_0)^(365 / days) − 1             (undefined if E_T ≤ 0 → report −100%)
Calmar          = CAGR / |MaxDD|
ProfitFactor    = Σ(winning trade PnL) / |Σ(losing trade PnL)|   (∞ / n/a if no losses)
Expectancy      = mean(trade PnL)
WinRate         = wins / round_trips
PayoffRatio     = mean(winning PnL) / |mean(losing PnL)|
Exposure        = fraction of wall time with |Q| > 0
Turnover        = Σ|traded notional| / mean(equity)
UlcerIndex      = √( mean( DD_t² ) )
```

**Note on MaxDD:** grid-close drawdown understates the real number, sometimes badly. Track the running peak on *every* mark-to-market update inside the engine and carry it into the metrics as `max_drawdown_intraperiod`. Report the grid-close figure too, labelled, so comparisons with other tools remain possible.

### 8.3 Per-Trade Metrics

For every round-trip: entry/exit time and price, side, max size, realised PnL, fees paid, funding paid/received, duration, and:

- **MAE** (maximum adverse excursion) — worst unrealised loss during the trade
- **MFE** (maximum favourable excursion) — best unrealised gain during the trade

MAE/MFE are the most practically useful trade-level statistics available: an MAE distribution tells you empirically where stops should sit, and a high-MFE/low-realised-PnL profile tells you exits are leaving money on the table. Both are computed from the mark price series inside the engine, not reconstructed afterwards.

### 8.4 PnL Attribution — Perp-Specific

Every run decomposes net PnL into four components that must sum exactly to the total (invariant I9):

```
net_pnl = price_pnl + funding_pnl − fees − slippage_cost
```

- `price_pnl` — realised + unrealised from price movement
- `funding_pnl` — sum of all funding cashflows (signed)
- `fees` — all commissions
- `slippage_cost` — Σ |actual fill price − reference price at signal time| × qty

This decomposition is the fastest way to spot a strategy that "works" only because the fee model was too generous, or one whose entire edge is funding capture (worth knowing — that is a different, more fragile edge than a price edge).

### 8.5 Trials Counter (Multiple-Testing Honesty)

The metadata DB tracks, per strategy, the **cumulative number of parameter combinations ever evaluated** across every backtest, grid search, and walk-forward optimisation. This count is displayed alongside the strategy's best Sharpe.

Reason: the best result from `N` trials is upward-biased by roughly `√(2 ln N)` standard deviations under the null. After 500 grid combinations, a Sharpe of 1.8 is unremarkable noise. This counter is the cheapest possible defence against fooling yourself, and it costs one integer column.

---

## 9. The Lab (T2)

Every Lab tool operates on a completed run and produces a new artefact linked to it.

### 9.1 Walk-Forward

**Config:** IS length, OOS length, step (default = OOS length), mode `anchored` (expanding IS) or `rolling` (fixed IS), optimisation objective, parameter grid.

**Per fold:** optimise on IS → evaluate the chosen params on OOS → record both.

**Optimisation objective — default is not `max(Sharpe)`.** The default is **neighbourhood-median Sharpe**: for each parameter point, take the median Sharpe of that point and its immediate grid neighbours. This selects plateaus over spikes, which is precisely the anti-overfitting property you want, and it costs nothing extra because the grid is already computed. `max(Sharpe)` remains selectable and is labelled `(overfit-prone)` in the UI.

**Output:**
- Stitched OOS equity curve — the headline result, and the only number worth quoting
- Per-fold IS vs OOS table
- **WFE** = annualised OOS return / annualised IS return, per fold and aggregate. WFE well below ~0.5 consistently means the optimisation is fitting noise.
- Parameter stability plot: chosen parameter values across folds. Values that jump around every fold indicate there is no stable optimum to find.

**Warm-up:** each OOS window is preceded by `requires["history"]` bars fed in warm-up mode (indicators update, orders blocked). This is realistic, not leakage — in live trading you *would* have that history. Leakage would be selecting parameters using OOS data, which the fold structure prevents.

### 9.2 Monte Carlo

Four methods, each answering a different question:

| Method | Question answered |
|---|---|
| Trade-order permutation (no replacement) | How much of my drawdown profile was luck of sequencing? |
| Trade bootstrap (with replacement) | What is the sampling distribution of my performance? |
| Block bootstrap on returns | Same, but preserving autocorrelation — the more honest version for time series |
| Random start / skip-first-N | How dependent is the result on when I happened to start? |

**A caveat the UI must state explicitly, because it is widely misunderstood:** under fixed-notional position sizing, permuting trade order **does not change final PnL at all** — only the path and therefore the drawdown. That is not a bug, it is the entire point of that test. Under percent-of-equity (compounding) sizing, order changes final equity too. The results panel shows which sizing mode was in effect and interprets accordingly.

**Output:** distributions of final equity, MaxDD, Sharpe, with 5/25/50/75/95 percentiles; probability of hitting the configured `max_drawdown` limit; probability of ruin. Default 10 000 iterations, parallelised across the worker pool.

### 9.3 Regime Analysis

**Regimes must be defined causally or the analysis is itself look-ahead.** Bucketing by quantiles computed over the whole sample uses future information to label the past. PerpLab uses:

- **Expanding-window quantiles** — at time `t`, the volatility quantile boundaries use only data up to `t`, or
- **Fixed absolute thresholds** set in advance

Definitions:
- *Volatility:* trailing 30-day realised vol → low / normal / high
- *Trend vs range:* ADX(14) on the daily bar, threshold 25
- *Funding regime:* trailing mean funding rate → negative / neutral / positive (perp-specific and genuinely informative — many strategies only work in one funding environment)
- *Cascade periods:* windows within N minutes of a liquidation cluster exceeding a notional threshold

Output: full metric set per regime bucket, plus the count of periods in each bucket. A regime with 4 observations is not evidence and the UI greys out its statistics rather than presenting a meaningless Sharpe.

### 9.4 Overfitting Diagnostics

- **Parameter sensitivity heatmap** (2 params) / parallel coordinates (>2), coloured by objective
- **Plateau score** = `chosen_point_metric / mean(neighbourhood_metric)`. Near 1.0 = robust plateau. Much greater than 1.0 = isolated spike = almost certainly noise. Displayed as a single prominent number, because it is the most actionable overfitting signal available.
- **IS vs OOS scatter** across folds with fitted slope. Slope near 0 means IS performance carries no information about OOS performance — the optimisation is doing nothing.
- **Performance decay** — OOS metric vs fold index. A downward trend means the edge is decaying or was never there.
- **Trials count** (§8.5) shown alongside every headline metric.
- *Optional/advanced:* PBO via CSCV (combinatorially symmetric cross-validation) — the rigorous version. Implement only after the rest is solid.

### 9.5 Portfolio Backtesting

Single event loop across all symbols, one shared account and one shared risk budget.

- Allocation modes: equal notional, inverse-volatility weighted, fixed fractional, custom weights
- Per-symbol *and* portfolio-level risk limits, both enforced
- Correlation matrix of per-symbol equity curves, plus rolling correlation (correlations converge to 1 in crashes, which is exactly when it matters — a static matrix hides this)
- Under isolated margin, each position's margin is siloed but the wallet is shared: a liquidation on one symbol reduces the wallet available to all others. The engine models this correctly.
- **Symbol-selection bias warning:** if the symbol list contains only currently-liquid majors, the results page says so. Robustness claims require at least one symbol that went through a genuinely bad period — a delisting scare, a depeg, a liquidity collapse.

### 9.6 Multi-Strategy Comparison

Select N runs → aligned equity curves on one chart, side-by-side metric table, correlation of daily returns between strategies, and a naive combined-portfolio curve. Only runs sharing a dataset manifest range are comparable; the UI blocks mismatched comparisons rather than producing a misleading overlay.

---

## 10. Frontend & UX

### 10.1 Design System

**Aesthetic:** precise, dense, monochrome-first. Bloomberg terminal restraint, modern SaaS polish. Colour carries meaning — it is never decoration.

```
DARK (default)                       LIGHT
--bg          #0A0A0B                #FFFFFF
--surface     #141416                #F7F7F8
--surface-2   #1C1C1F                #EFEFF1
--border      #2A2A2E                #E2E2E5
--text        #F5F5F7                #18181B
--text-dim    #A1A1AA                #52525B
--text-mute   #6B6B72                #8A8A93
--accent      #E5E5E7                #18181B      (interactive; monochrome by design)
--pos         #16A34A                #15803D
--neg         #DC2626  ← RESERVED for kill switch only in chrome
--warn        #D97706                #B45309
--info        #0891B2                #0E7490
```

Themed entirely via CSS custom properties on `:root[data-theme]`. One stylesheet, two token sets — never duplicated components.

**Typography:** Inter for UI. JetBrains Mono for all numbers, code, IDs, and timestamps. `font-variant-numeric: tabular-nums` on every numeric cell so columns align and changing digits do not cause reflow jitter in live views.

**Density:** 8 px spacing grid. 32 px table rows. Information-dense by intent — this is a research tool, not a landing page.

**Numbers:** always signed and always colour-coded, **plus** an arrow glyph. Colour alone fails for colour-blind users and fails in screenshots. Currency to 2 dp, quantities to the symbol's `stepSize` precision, percentages to 2 dp, bps where more legible.

### 10.2 Persistent Chrome

```
┌────────────────────────────────────────────────────────────────────────┐
│ PerpLab   Dashboard Strategies Runs Lab Data&Feed Settings             │
│                          ● LIVE: EMACross/BTCUSDT   [◐]   [ KILL ]     │
└────────────────────────────────────────────────────────────────────────┘
```

- **Session badge:** green `● LIVE` with strategy/symbol, amber `● PAPER`, grey `○ No exchange connected`. Always answers "is real money at risk right now" without a click.
- **Kill switch:** red, always present, always enabled during any active session. Single click → confirm modal stating exactly what it will do (cancel-only vs close-all, per Settings) → executes.
- **Theme toggle:** `◐`.

### 10.3 Tabs

**Dashboard** — equity sparkline (live account if connected), active sessions with live PnL, last 5 runs, open positions with liquidation-price proximity bars, data freshness indicators, collector health. Everything is a link into the relevant tab.

**Strategies** — card grid: name, tags, version, last-run summary sparkline, cumulative trials count. Per card: `Backtest` · `Paper` · `Go Live` · `Edit`, overflow menu for Export / Archive / Delete. Top bar: `New` · `Import` · search · tag filter · Archived toggle. Editor view is a full-height Monaco with a validation panel (inline gutter diagnostics), a params preview showing the auto-generated form, and a version history drawer with diffs.

**Runs** — filterable table (strategy, symbol, mode, date, status, key metrics). Multi-select → Compare. Run detail page:
- Header: strategy@version, symbol(s), range, mode, params, seed, dataset manifest hash, fill-model tier badge, any warning badges (`GAP_SKIPPED`, `FILTERS_APPROXIMATE`, `LOW_FIDELITY`, `DEPTH_EXHAUSTED`)
- Equity curve with drawdown shaded beneath, trade markers overlaid on price
- Metric cards (§8), with the trials counter beside the headline Sharpe
- PnL attribution bar (price / funding / fees / slippage)
- Trade table with MAE/MFE columns, sortable, CSV export
- Event log viewer — virtualised, searchable, filterable by kind and severity
- Parity report if a shadow backtest exists
- `Send to Lab` button

Live/paper runs show a real-time monitor instead: position, live PnL, liq-price distance, recent fills, order book snapshot, connection status, risk-limit usage bars, and a per-strategy stop control.

**Lab** — pick a completed run, pick a tool, configure, run as a job. Each tool has its own results view (§9). Lab artefacts are saved and linked back to the source run permanently.

**Data & Feed** — three panels:
- *Coverage:* per symbol × dataset — date range, row count, size on disk, gap count, last updated. Gaps clickable to a detail view. Depth-collector coverage shown as a distinct bar, since it defines the L2 backtest window. Buttons: Pull / Refresh / Verify checksums / Retention preview.
- *Exchange Connection:* masked API key + secret inputs, `Connect`, connection status with account alias and balance, session expiry countdown, `Disconnect`. Explicit inline notice: keys are held in memory for this session only and are never written to disk.
- *Feed:* live virtualised log — WS connection events, ingested data rates, order lifecycle, errors, rate-limit warnings, reconnects, risk-limit rejections. Filter by severity and source. This is the first place to look when something is wrong.

**Settings** — theme, default risk limits, default leverage and margin mode, fee override, latency model default, kill-switch behaviour (cancel-only vs close-all), retention policy, alert webhooks (Telegram/Discord), data directory, worker count.

### 10.4 Interaction Rules

- Destructive and live actions require confirmation. `Go Live` requires **typing the symbol name** — muscle memory should never be able to start a live session.
- Long-running jobs are non-blocking: progress bar, ETA, cancellable, and you can navigate away. Completion emits a toast and, if configured, a webhook.
- Every table exports CSV. Every chart exports PNG.
- Keyboard: `⌘K` command palette (jump to strategy/run, start backtest), `⌘S` save in editor, `Esc` closes modals. The kill switch has **no** keyboard shortcut — deliberately, to prevent accidental firing.
- Empty states are instructional, not decorative: an empty Runs tab explains how to start a backtest.

---

## 11. Security Model

**API keys:**
- Entered in-platform (Data & Feed tab), masked inputs, transmitted over local HTTPS
- Held **only in backend process memory**, in a session object. Never written to disk, database, config file, log line, browser storage, or crash dump.
- Validated on entry with a lightweight signed request (account balance). Never echoed back to the UI — only the account alias and balance are shown.
- Session expiry: default 12 h of inactivity → keys wiped, live sessions halted with positions left open and an alert fired (a forced market close on session expiry would be worse than the exposure).
- `Disconnect` and the kill switch both wipe keys immediately.

**Binance-side hardening (documented in the UI at key-entry time):**
- Enable **Reading** + **Futures Trading** only. **Never enable Withdrawals.** A leaked key must be unable to move funds, only to trade.
- Use the **IP whitelist** once the backend's address is stable.
- Use a dedicated sub-account for algo trading if available.

**Platform surface:**
- Binds to `127.0.0.1` by default. Exposing it to a network is opt-in and gated behind an explicit config flag with a warning.
- Single-user; no auth by default on localhost. If network exposure is enabled, a password becomes mandatory (enforced in code, not documentation).
- Strategy code isolation is a stability boundary, not a security sandbox (§2.3).
- Signing uses HMAC-SHA256 over the query string with a `timestamp` and `recvWindow`. The backend must be NTP-synced or requests are rejected; clock drift beyond 1 s raises a Feed warning before it causes failures.

---

## 12. Reproducibility & Testing

### 12.1 Reproducibility Contract

Every run stores: strategy code hash + version id, engine version, full param set, seed, dataset manifest (§4.6), reference-snapshot ids, fill model tier, latency model config, risk limits, and the platform's git commit.

**Invariant:** identical inputs → identical event-log SHA-256. This is enforced by a CI test, not by good intentions. A backtest you cannot reproduce is an anecdote.

### 12.2 Test Suite

| Layer | Content |
|---|---|
| **Golden tests** | §3.9 worked example, reproduced to the exact Decimal. Plus: liquidation of a long, liquidation of a short, position flip, funding at exactly the settlement millisecond, partial fill sequence, depth exhaustion, bracket boundary crossing. |
| **Property tests** | Random fill/funding/mark sequences → all §3.10 invariants hold. Random order sizes → all §3.2 filters respected. Hypothesis-based. |
| **Determinism tests** | Same seed twice → identical log hash. Different worker counts → identical results. |
| **Look-ahead tests** | A strategy that tries to read the current forming bar must fail. A strategy fed data truncated at bar `i` must produce identical signals up to bar `i` as one fed the full series — the definitive look-ahead test, and it catches bugs no code review will. |
| **Exchange-parity tests** | Against testnet: computed `P_liq` vs Binance's reported value; computed funding vs income history; computed fees vs actual commissions. These validate §3 against reality. |
| **Regression tests** | A stored reference run's metrics must not change across engine versions without an explicit, reviewed changelog entry. |

### 12.3 The Look-Ahead Test, Specifically

Worth its own note because it is the highest-value test in the suite and is rarely written:

```
run_full     = backtest(strategy, data[0:N])
run_truncate = backtest(strategy, data[0:N-100])
assert run_truncate.events == run_full.events[: len(run_truncate.events)]
```

If a strategy or an indicator peeks forward by even one bar, these diverge. Run it against every strategy at import time as an optional deep-validation step, and against the built-in indicator library in CI.

---

## 13. Build Plan

Each phase has an **exit criterion**. Do not start the next phase until it is met — this is the discipline that turns a plan into a platform.

| Phase | Scope | Exit criterion |
|---|---|---|
| **0** | Repo, config, types, Decimal/money layer, SQLite schema, reference snapshotting (`exchangeInfo`, `leverageBracket`, `commissionRate`) | Reference data pulled and versioned on disk |
| **1** | Bulk ingestion (klines, aggTrades, bookTicker, funding, metrics, liquidations), checksum verify, Parquet writer, DuckDB views, gap detection | Any symbol/range queryable in <2 s; gap report accurate on a deliberately corrupted sample |
| **1b** | **Collector service** — depth20, markPrice, aggTrades, liquidations, with reconnect and heartbeat | Runs unattended 72 h with zero unexplained gaps. **Start this before Phase 2 — depth history only accumulates forward.** |
| **2** | Accounting core: positions, fills, fees, funding, brackets, margin, liquidation, invariants | §3.9 worked example reproduces exactly; all property tests green |
| **3** | Strategy API, indicator library, validator, Monaco editor, library CRUD/import/export/archive/versioning | Write, save, validate, and version a strategy entirely in-browser |
| **4** | Backtest engine: event loop, ordering, no-look-ahead, market orders, metrics, results page | EMA cross backtests on 1 year of BTCUSDT; deterministic across two runs; look-ahead test passes |
| **5** | Realism layer: tick fills, latency, limit queue model, partial fills, stops/TP/trailing, depth walking, fill-tier degradation | All golden scenarios reproduce; fill tier correctly degrades and is surfaced in the UI |
| **6** | Risk limits, kill switch, invariant auto-triggers | Every limit demonstrably halts a deliberately misbehaving test strategy |
| **7** | Papertrading on testnet, live monitor, Feed tab, shadow backtest + parity report | A strategy papertrades 48 h; parity report shows <5% PnL divergence vs shadow backtest |
| **8** | Live trading, key session flow, exchange reconciliation loop | **A real minimum-notional round trip on production, reconciled against Binance's own reported fills, fees, funding, and PnL to the cent.** |
| **9** | Lab: walk-forward, Monte Carlo, regime, overfitting, portfolio, comparison | Full walk-forward on a real strategy producing a stitched OOS curve |
| **10** | UX polish, theming, keyboard, empty states, exports | End-to-end pass with no dead ends |

**A note on Phase 8.** TradeLab reached code-complete through its live phase but never got the real one-share sign-off. That gap is the difference between a platform that works and a platform you believe works. Phase 8's exit criterion is deliberately written as an *executed trade reconciled to the cent*, not "live code written". Everything before it is theory; that trade is the only thing that converts the theory into evidence. Treat Phase 9 as locked until Phase 8's criterion is genuinely met.

---

## 14. Review Log

Issues found during the review pass of the draft plan, and their resolutions. All are incorporated above.

| # | Issue | Severity | Resolution |
|---|---|---|---|
| R1 | Plan assumed historical 20-level depth was downloadable in bulk. It is not — free bulk `bookDepth` is percentage-banded, and raw L2 history is VIP-gated or vendor-purchased. | **Critical** | §4.2 — fill-model tiers with `bookTicker` (which *is* freely available tick-level) as the primary long-history source; collector moved to Phase 1b so depth history starts accumulating immediately |
| R2 | Exchange filters (`tickSize`, `stepSize`, `minNotional`, `PERCENT_PRICE`) were absent. Backtests would fill impossible order sizes. | **Critical** | §3.2 — filters enforced identically in backtest and live; historical snapshots versioned |
| R3 | Float accumulation in balances would break reconciliation over 10⁴+ events. | High | §3.1 — `Decimal` at the accounting layer, floats confined to indicators |
| R4 | No deterministic tie-break for same-millisecond events → non-reproducible backtests. | High | §6.2 — total ordering on `(ts, kind_priority, source_seq, dataset_id)` |
| R5 | Event ordering did not specify that funding must precede the liquidation check. Positions would survive funding payments that should have liquidated them. | High | §6.2 — priority 1 before 2, with the reasoning documented |
| R6 | Maintenance-margin bracket selection is circular with liquidation price. | High | §3.6 — fixed-point iteration with convergence cap, raises rather than guessing |
| R7 | Liquidation modelled as a clean close returning leftover margin — optimistic. | High | §3.7 — total isolated-margin loss by default; recovery fraction configurable for sensitivity only |
| R8 | Cross margin implied but has no closed-form liquidation price and lets one strategy sink the account. | High | §3.7 — isolated-only in v1, cross deferred to v2 with rationale |
| R9 | Limit fills would have been modelled on price *touching* the level — the classic fiction. | High | §6.4 — queue-consumption proxy using `aggTrade` aggressor side; through-trades fill, touches do not |
| R10 | Sharpe annualisation would have used 252. Crypto is 24/7. | Medium | §8.1 — A = 365 daily / 8760 hourly, stated explicitly |
| R11 | MaxDD computed on daily closes understates the true figure. | Medium | §8.2 — tracked on every mark-to-market tick; grid figure also reported and labelled |
| R12 | "Trade" was undefined — legs vs round-trips. Inflates trade count and distorts win rate. | Medium | §8.1 — round-trip is canonical; legs reported separately |
| R13 | Regime tagging via whole-sample quantiles is itself look-ahead. | Medium | §9.3 — expanding-window quantiles or fixed thresholds only |
| R14 | Monte Carlo trade-shuffling under fixed sizing does not change final PnL — would look like a broken feature. | Medium | §9.2 — behaviour stated in the UI, four methods offered for different questions |
| R15 | Grid-search default `max(Sharpe)` maximises overfitting. | Medium | §9.1 — neighbourhood-median objective as default; plateau score in §9.4 |
| R16 | Multiple-testing bias uncounted across repeated optimisation. | Medium | §8.5 — cumulative trials counter per strategy, shown beside headline metrics |
| R17 | Funding interval hardcoded at 8 h; Binance varies it by symbol and has changed it. | Medium | §3.5 — read per symbol and derive from actual historical settlements |
| R18 | Backtest and live risked duplicate implementations of fee/sizing/risk logic. | Medium | §6.1 — shared core is mandatory; §6.7 shadow-backtest parity measures divergence rather than assuming it away |
| R19 | Cancel latency unmodelled — cancels assumed instant. | Medium | §6.3 — cancels carry latency; a cancel does not protect against a fill inside the latency window |
| R20 | Mark price interpolation between samples would fabricate liquidations. | Medium | §3.4 — step function, LOCF, never interpolated |
| R21 | Zero-volume klines vs absent klines conflated in gap detection. | Low | §4.5 — distinguished; cross-checked against tick datasets |
| R22 | No reconciliation between platform state and exchange state during live. | High | §6.7 — 60 s reconciliation loop, mismatch fires the kill switch |
| R23 | Storing every timeframe separately invites disagreement between them. | Low | §4.3 — store 1 m only, derive the rest at query time |
| R24 | Kill switch defaulting to close-all can be worse than the exposure during a flash crash. | Medium | §7 — default cancel-only, behaviour stated on the button, configurable |
| R25 | Strategies calling `time.time()`/`datetime.now()` break determinism and live/backtest parity. | Medium | §5.5 — AST validator rejects them; `ctx.now` is the only clock |

---

## 15. Known Limitations

Honest list. Read before trusting any number this platform produces.

1. **No L3 data.** The limit-fill queue model (§6.4) is an approximation. Strategies whose edge depends on queue priority cannot be validated here.
2. **Depth history is forward-only.** L2 book-walking backtests are limited to the collector's coverage window. Older ranges fall back to `bookTicker` fidelity.
3. **Depth is 20 levels.** Orders exceeding visible depth use a penalty extrapolation, which is a guess — a pessimistic one, but a guess.
4. **Liquidation execution is simplified.** Binance performs tiered/partial liquidation on large positions; PerpLab models a single full liquidation. Auto-deleveraging (ADL) and insurance-fund dynamics are not modelled at all.
5. **Slippage during cascades is probably still optimistic.** Depth data during violent moves is exactly when snapshots are least representative. Treat cascade-strategy backtest results as an upper bound.
6. **Latency is modelled, not measured** (until you calibrate it from paper sessions). Defaults are chosen pessimistically, but they are defaults.
7. **Exchange outages, API errors, and partial system failures** in live trading cannot be fully reproduced in backtest. `HALT_TRADING` gap mode approximates this; it does not replicate it.
8. **Isolated margin, one-way mode only.** Cross margin and hedge mode change liquidation mathematics substantially and are not supported in v1.
9. **Single exchange.** No cross-venue price validation. If Binance's mark price does something unusual, PerpLab will faithfully reproduce it.
10. **Symbol-selection bias is your responsibility.** The platform warns; it cannot fix a symbol list that only contains survivors.
11. **Fee tiers change** and VIP level affects them. Backtests use whatever rate is configured; the honest default is a pessimistic one.
12. **Regulatory access is out of scope.** Verify the current status of Binance derivatives access from your jurisdiction before connecting real capital — this is a live, changing situation and nothing in this document should be treated as current on that point.

---

## Appendix A — Feature Tiers

| Tier | Features |
|---|---|
| **T0 Essential** | Backtest engine · order fill simulation · papertrade engine · live engine · position & PnL accounting · risk kill-switch |
| **T1 Basic** | Book-data slippage & fee modelling · funding PnL at exact settlements · mark-price liquidation simulation · walk-forward · parameter grid search · stop-loss/TP/trailing · in-sample/out-of-sample split |
| **T2 Lab** | Liquidation-cascade (sweep) backtesting · Monte Carlo (4 methods) · L2 book replay · regime-tagged backtesting · multi-asset portfolio backtesting · overfitting diagnostics · multi-strategy comparison |
| **T3 Excluded** | L3 book reconstruction · queue-position fills · spoofing/iceberg detection · latency-optimised execution · market-making · cross-exchange arbitrage |

## Appendix B — Binance Endpoint & Stream Reference

**Base URLs:** production `https://fapi.binance.com` · testnet `https://testnet.binancefuture.com` · bulk data `https://data.binance.vision`

**REST (unsigned):** `/fapi/v1/exchangeInfo` · `/fapi/v1/klines` · `/fapi/v1/aggTrades` · `/fapi/v1/depth` · `/fapi/v1/fundingRate` · `/fapi/v1/premiumIndex` · `/futures/data/openInterestHist`

**REST (signed):** `/fapi/v2/account` · `/fapi/v2/positionRisk` · `/fapi/v1/order` · `/fapi/v1/allOpenOrders` · `/fapi/v1/leverage` · `/fapi/v1/marginType` · `/fapi/v1/leverageBracket` · `/fapi/v1/commissionRate` · `/fapi/v1/income` · `/fapi/v1/listenKey`

**WebSocket streams:** `<symbol>@kline_<interval>` · `<symbol>@aggTrade` · `<symbol>@bookTicker` · `<symbol>@depth20@100ms` · `<symbol>@markPrice@1s` · `!forceOrder@arr` · user data stream via `listenKey` (keepalive every 30 min or it expires at 60)

**Bulk (`data/futures/um/daily|monthly/`):** `klines` · `aggTrades` · `trades` · `bookTicker` · `bookDepth` (percentage-banded) · `metrics` · `fundingRate` · `liquidationSnapshot` · `markPriceKlines` · `indexPriceKlines` · `premiumIndexKlines` — each with a `.CHECKSUM` file that must be verified

**Signing:** HMAC-SHA256 over the query string; API key in the `X-MBX-APIKEY` header; `timestamp` + `recvWindow` required; NTP sync mandatory.

**Verify before building:** endpoint paths, filter names, funding intervals, fee tiers, and bulk-dataset availability all change. Pull them at runtime and snapshot them; do not hardcode anything in this appendix.

## Appendix C — Glossary

**Mark price** — manipulation-resistant reference price; drives unrealised PnL and liquidation. Not the last traded price.
**Index price** — volume-weighted spot price across constituent exchanges; the anchor for mark price.
**Funding rate** — periodic payment between longs and shorts that tethers perp price to spot. Positive: longs pay shorts.
**Basis** — perp price minus index price.
**Open interest** — total notional of open contracts.
**Maintenance margin (MM)** — minimum equity required to keep a position open. Falling below it triggers liquidation.
**Initial margin (IM)** — margin required to open, equal to notional divided by leverage.
**Bankruptcy price** — price at which margin balance reaches zero. Beyond the liquidation price.
**MAE / MFE** — maximum adverse / favourable excursion within a trade.
**WFE** — walk-forward efficiency: out-of-sample return divided by in-sample return.
**Round-trip** — one trade, flat to flat, regardless of how many legs it took.
**LOCF** — last observation carried forward.

---

*End of specification. Build order is §13. Do not skip Phase 8's exit criterion.*
