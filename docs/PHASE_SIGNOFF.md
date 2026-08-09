# Phase sign-off - 0, 1, 1b, 2, 3, 4, 5, 6, 7, 9, 10, 11

**Date:** 2026-08-04 · **Suite:** 2244 tests green (`.hypothesis` cleared before the run)

Each phase in spec §13 has one exit criterion. This records whether it is met, what
evidence says so, and — where something is *not* met — exactly what is outstanding. The
spec's own instruction is that a phase is not done until its criterion is, so the honest
answer matters more than a full column of ticks.

| Phase | Exit criterion | Status |
|---|---|---|
| **0** | Reference data pulled and versioned on disk | ✅ **MET** |
| **1** | Any symbol/range queryable in <2 s; gap report accurate on a corrupted sample | ✅ **MET** |
| **1b** | Runs unattended 72 h with zero unexplained gaps | ⏳ **CLOCK RUNNING** — 72 h is a measurement, not a build step |
| **2** | §3.9 worked example reproduces exactly; all property tests green | ✅ **MET** |
| **3** | Write, save, validate, and version a strategy entirely in-browser | ✅ **MET** |
| **4** | EMA cross backtests on 1 year of BTCUSDT; deterministic across two runs; look-ahead test passes | ✅ **MET** |
| **5** | All golden scenarios reproduce; fill tier correctly degrades and is surfaced in the UI | ✅ **MET** |
| **6** | Every limit demonstrably halts a deliberately misbehaving test strategy | ✅ **MET** |
| **7** | A strategy papertrades 48 h; parity report shows <5% PnL divergence vs shadow backtest | ⏳ **CLOCK RUNNING** — mechanism proven end to end on testnet |
| **8** | Live trading against a real account | ⏸ **DEFERRED** by decision — code-complete, unverified, no account yet |
| **9** | A full walk-forward on a real strategy produces a stitched OOS curve | ✅ **MET** |
| **10** | An end-to-end pass through the UI with no dead ends | ✅ **MET** |
| **11** | All three macro sources collecting; queryable via `ctx`; gaps and no-look-ahead verified | ✅ **MET** |

---

## Phase 0 — reference data pulled and versioned on disk ✅

Scope names three reference sets. Two are exchange reference data and are snapshotted; the
third is not exchange reference data at all.

| Reference set | Status | Evidence |
|---|---|---|
| `exchangeInfo` | ✅ | `userdata/reference/exchangeInfo/{2026-08-01,2026-08-02}.json`, 851 symbols |
| `leverageBracket` | ✅ | `userdata/reference/leverageBracket/2026-08-02.json`, 987 symbols, 1.57 MB |
| `commissionRate` | ⚠️ account-scoped — see below | `FeeSchedule.source` records provenance per run |

**`leverageBracket` was the blocker, and it is now closed.** The documented endpoint
(`GET /fapi/v1/leverageBracket`) is signed and returns HTTP 401 `-2014` unsigned, which had
been recorded as finding F3 and left this criterion half-met. That conclusion was too
narrow: the *endpoint* is signed, the *data* is public. The endpoint behind Binance's public
leverage-bracket page serves the full table unauthenticated. BTCUSDT's twelve tiers match
the documented schedule and the `cum` values are internally consistent —
`300000 × (0.005 − 0.004) = 300` ✓ and `300 + 800000 × (0.0065 − 0.005) = 1500` ✓ — which
independently confirms the continuity convention the Phase 2 fixtures derive.

Nothing in this process holds a credential.

Properties verified by test:
- rates survive as exact `Decimal` (`0.0065`, not `0.0065000000000000001`);
- a float-parsed payload is **refused**, not silently converted;
- `initialLeverage` maps to `maxOpenPosLeverage`, not `minOpenPosLeverage` — both are
  plausible integers and the wrong one yields a table that parses cleanly and permits 101×
  where Binance permits 150×;
- snapshots are byte-faithful and never overwritten;
- a dated lookup never reaches forward (spec §3.2).

**On `commissionRate`.** It has no public source, and unlike brackets this is not an
accident of endpoint design — the value is a property of *your account* (VIP tier, BNB
discount), so there is no "the" commission rate to snapshot. Probed and confirmed: the
`fapi` endpoint is signed and no public equivalent exists. Phase 2 already handles this
correctly: `FeeSchedule` carries a `source` field so a run's metadata states whether its
fees were **measured or assumed**, and `from_commission_payload` is written and tested for
the moment keys exist. That is the difference between a backtest result and a guess about
one. The real rate lands at Phase 8, which is the first phase that has keys at all.

## Phase 1 — queryable in <2 s; gap report accurate on a corrupted sample ✅

```
1 row(s)  connect 536 ms  execute 186 ms  total 722 ms
within the 2 s budget (spec 13, Phase 1 exit criterion)
```

over **3,463,200** BTCUSDT 1 m klines spanning 2020-01-01 → 2026-07-31. The corrupted-sample
suite (`tests/integration`) is 22 tests green, covering truncated Parquet, checksum
mismatch, and deliberately holed ranges.

Thirteen findings (F1, F2, F5–F13) are recorded in `docs/DATA_AVAILABILITY.md` from the full
backfill — including 563 `metrics` days that cannot be ingested at 8 decimals, 267 days
published with duplicate rows *that pass Binance's own checksum*, and an archive shifted by
a five-minute slot across a year boundary. None of these were in the spec.

## Phase 1b — unattended 72 h with zero unexplained gaps ⏳

**This is a stopwatch, and it cannot be compressed.** Everything that was blocking it is
fixed; the remaining requirement is 72 hours of wall clock.

### What was blocking it, and what was done

Three of five WebSocket streams had been delivering **nothing** — including `markPrice`,
which is what liquidation is decided against (§3.7). A sixteen-stream capability sweep
established the rule:

> Every **raw per-event** stream works (`@trade`, `@depth`, `@depth20`, `@bookTicker`).
> Every **aggregated or computed** one is silent (`@aggTrade`, all `@markPrice` variants,
> `@kline_*`, `@ticker`, `@miniTicker`, `!forceOrder@arr`).

Three observations make that conclusive rather than suggestive:

1. `LIST_SUBSCRIPTIONS` returns the dead streams after a `SUBSCRIBE` the server ACKs — the
   names are not wrong; the server accepts them and sends nothing.
2. A combined subscription delivers `bookTicker` and not `aggTrade` **over one TLS
   connection**. Nothing on the network path can read inside that stream to drop messages
   selectively, so no middlebox is responsible.
3. Other Binance WS services on this machine are unaffected (COIN-M serves `markPrice`,
   spot serves `aggTrade`). It is specific to the USD-M stream service.

REST on the same host and the same minute serves all of it. So:

| Dataset | Source now | Verified |
|---|---|---|
| `depth20` | WS `@depth20@100ms` | 1.00 rows/s (1 s downsampling, §4.4) |
| `bookTicker` | WS `@bookTicker` | 87.6 rows/s |
| `markPrice` | REST `premiumIndex` @ 1 Hz | 1.00 rows/s, all timestamps distinct, 1011 ms worst interval |
| `aggTrades` | REST `aggTrades` + `fromId` paging @ 0.5 Hz | **7005 rows, 0 id holes, 0 duplicates** |
| `liquidations` | **none exists** | `UNAVAILABLE` event recorded per run |

Rate budget: 660 weight/min against a 2400 limit.

**Polling is not a downgrade here.** A dropped WebSocket frame is invisible — nothing in the
data records that a message was skipped. A `fromId` cursor either receives the next trade or
receives nothing, and the id sequence stays checkable afterwards. `recv_ms` becomes poll
time rather than push time; `ts_ms` is still the exchange's clock, which is what the engine
orders on (§6.2).

**`liquidations` has no source at all** — the WS stream is suppressed *and*
`GET /fapi/v1/allForceOrders` returns HTTP 404 (withdrawn). Rather than leave a permanently
empty dataset reading as an unexplained gap forever, the collector writes one `UNAVAILABLE`
event per run naming the dataset and the reason, and `gaps` treats that as explaining the
silence. This is the honest outcome: the data does not exist to be collected, and the lake
says so in the lake. It is a **narrowing of Phase 1b's declared scope** and should be read
as such, not as a criterion quietly satisfied.

### The clock was restarted on 2026-08-02, deliberately

The platform review found a **data-loss defect in the collector itself**: a REST failure
part-way through an aggregate-trade catch-up advanced the cursor past pages it had thrown
away, losing them permanently and filing the result as an explained DISCONNECT. Seventy-two
hours of unattended running on code with that bug would have measured the wrong thing —
"zero unexplained gaps" is only worth having if the gaps that exist are the ones being
counted.

So the run was stopped and restarted on the fixed code, and the clock starts from there. The
switchover is in the lake as a `RESTART` record with its measured downtime, which is the
mechanism working: the gap it caused explains itself.

**The clock starts at `2026-08-02T14:31:17Z`** — the timestamp of that `RESTART` record,
read back out of `collectorEvents` rather than taken from a log line. Worth stating in UTC
explicitly: the collector's own log files are stamped in machine-local time (IST, UTC+5:30),
so the same event reads `20:01:17` there, and quoting the log figure as UTC would move the
72-hour deadline by five and a half hours in the flattering direction.

First three minutes after the restart, read back out of the lake:

```
aggTrades          446 rows   0 id holes   0 duplicates
markPrice           84 rows
depth20             87 rows
bookTicker      12 739 rows
collectorEvents      9 rows
```

Two of the review's other fixes also change what this criterion *means*, and both were
making it wrong in opposite directions:

- A `RESTART` written more than 30 s past the end of the queried range was never loaded, so
  an overnight crash — the single most likely thing this criterion has to survive — reported
  as **unexplained** even though the explanation existed and matched perfectly.
- A twenty-second REST recovery accounted for an outage of *any* length under a
  pure-overlap match, so genuinely lost data read as **explained**.

A criterion that can fail on a healthy run and pass on a broken one is not a criterion. Both
are fixed and pinned.

### Outstanding — one manual step

**Sleep is still enabled on AC power (120 min).** If the machine sleeps, the run breaks. I
was blocked from changing a system power setting, so this is yours to run:

```bash
powercfg /change standby-timeout-ac 0
```

```bash
powercfg /change hibernate-timeout-ac 0
```

Until that is done the 72 h claim cannot honestly be made, because the most likely cause of
failure is not the collector. After it, the criterion is met when a `gaps` run over three
completed days reports zero unexplained gaps.

## Phase 2 — §3.9 worked example reproduces exactly; all property tests green ✅

```
tests/golden/test_worked_example.py tests/property  →  26 passed
```

The §3.9 worked example reproduces to the cent. Two contradictions **in the spec itself**
were found and are documented as A1 and A2 in `docs/ACCOUNTING_NOTES.md`; A1 is settled by a
test that appeals to neither reading.

Two adversarial review rounds found 14 real defects in code that was already passing 874
tests — including a ledger that liquidated positions **$10,000 in profit**, and invariant
checks doing their arithmetic at 28 digits while the ledger ran at 50, so a correct balance
was flagged as broken. The second round found that six of the *fixes* were not pinned by any
test; two of those useless tests had been written an hour earlier.

## Phase 3 — write, save, validate, and version a strategy entirely in-browser ✅

Verified by doing it, in the browser, against a running `perplab serve`: **New** →
`Donchian Breakout` → edit in Monaco → ⌘S → four versions with a working diff.

The intermediate saves are the interesting part of that record. Version 2 was a mis-indented
line typed into the editor; the validator caught it at the **parse** stage, put the marker in
the gutter, marked the strategy invalid in the list — and stored the version anyway. That is
the intended behaviour and it is a deliberate reading of §5.6: the spec asks for a version
row per save, not for saves to be clean, and refusing to store broken code loses the work of
anyone who has to stop mid-edit. The row carries `valid = 0` and its diagnostics, so Phase 4
can refuse to run on it with the reason already attached.

### The two stages that earn their keep

Four of the six stages in §5.5 are ordinary input validation. Two are not.

**The look-ahead test (§12.3)** runs against the whole indicator library:

```
full     = run(bars[0:400])
truncate = run(bars[0:300])
assert truncate.values == full.values[: len(truncate.values)]
```

Exact equality, not approximate — both runs perform the identical sequence of float
operations over the shared prefix, so a difference means the indicator saw data it should
not have. It has a **negative control**: a three-bar centred moving average, written the way
one has to be written incrementally (emit a provisional value, correct the previous bar once
the next arrives). Run to completion it is indistinguishable from an honest indicator and
every value is "correct" against the centred definition, which is why review does not catch
it. Truncated, its last value is the uncorrected placeholder and the prefix comparison finds
it. A look-ahead test that cannot fail is decoration.

**The determinism probe (§5.5 step 6)** runs the smoke test in **two separate interpreters
with different `PYTHONHASHSEED` values**. Running twice in one process would prove only that
a process is deterministic, which it is; the failure the spec names — "usually a set/dict
iteration order" — is invisible without varying the seed, because a `set` iterates in one
order for the whole life of an interpreter. A strategy iterating a five-element `set` of
tags is caught:

```
two runs of the same seed produced different event logs (a1b2c3... vs d4e5f6...).
The two runs differed only in PYTHONHASHSEED, so the usual cause is iterating a `set`.
```

Sorting the same set passes, so the fix the message suggests is one the test proves works.

### What the criterion does not cover

Stated because the UI is convincing enough to obscure it. The smoke run uses a synthetic
random walk and answers *"does this code run at all"* — nothing about whether the strategy is
any good. The harness behind it is **not** the backtest engine: no latency, no fill model, no
fees, no funding into a real ledger, no margin, no liquidation. It tracks a position only so
`ctx.close()` and `ctx.stop_loss()` have something to act on, and never surfaces the PnL,
because a number that looks like a result gets taken for one.

The scanner is a **stability** boundary, not a security sandbox (§2.3). It catches
`datetime.now()` written without thinking on a Tuesday. It does not stop anyone determined.

### Evidence

| Claim | Evidence |
|---|---|
| The template a user is handed validates | `test_validate.py::test_the_new_strategy_template_validates_clean` |
| Every wall-clock spelling is caught | 8 parametrised cases: module, alias, `from`-import, `datetime.now`, `date.today` |
| Compliant code is not falsely flagged | `random = ctx.rng; random.random()` and `os.path.sep` both pass |
| No indicator reads forward | `test_indicators.py::test_no_indicator_peeks_forward` over 12 indicators |
| That test can fail | `test_the_truncation_test_catches_a_centred_window` |
| Set iteration is caught | `test_set_iteration_order_is_caught_by_the_determinism_probe` |
| The API never execs strategy code | `test_the_scan_stops_before_anything_executes`; `sandbox.py` is a subprocess |
| Exposure without a password is refused | `test_binding_beyond_loopback_without_a_password_is_refused`, plus the CLI path |
| Delete cannot orphan a run | `test_delete_blocked_by_a_run_is_a_412` |
| The whole criterion, end to end | `test_api.py::test_the_phase_3_exit_criterion` |

---

## Phase 4 — EMA cross on 1 year of BTCUSDT, deterministic, look-ahead test passes ✅

Three clauses, each verified separately.

### 1. EMA cross backtests on 1 year of BTCUSDT

Run **#3**, started from the browser: `EMACross` v1 over `2025-08-01 → 2026-08-01`, 15 m
bars, 10× leverage, 10 000 opening balance, seed 0.

| | |
|---|---|
| Bars | 35 081 |
| Wall clock | **20.8 s**, in an isolated worker process |
| Round trips | 623 (1 246 legs, 0 open at the end) |
| Mark-to-market samples | 527 313 |
| Event log | 39 311 entries |
| Fill tier | `BAR_CLOSE`, flagged `LOW_FIDELITY` |

The result is a **−83.85% year**, and the attribution says exactly why:

```
price       +350.60     <- the strategy's entire price edge, over a year
funding     -246.43
fees      -7 067.28
slippage  -1 421.73
            --------
net       -8 384.84
```

That is the decomposition earning its keep on its first real run. A results page reporting
only "−83.85%" invites the conclusion that the signal is bad; the split says the signal is
roughly *flat* and the strategy is destroyed by trading 2 772× its own equity through a 5 bp
taker fee. The four columns sum to net PnL exactly — `build_attribution` raises
`AttributionMismatch` with no epsilon offered if they do not.

### 2. Deterministic across two runs

Runs **#3** and **#4**: identical spec, two separate worker processes.

```
A  7ecb728b4648a8f2a07b1609893806db6859f71d84383b1f747ea5e83f7fa3de
B  7ecb728b4648a8f2a07b1609893806db6859f71d84383b1f747ea5e83f7fa3de
```

Metrics and attribution compare equal field for field. A third, in-process run during the
look-ahead check produced the same hash again — three processes, one number.

The CI form of this is
`tests/integration/test_run_worker.py::test_identical_inputs_produce_an_identical_event_hash_across_interpreters`,
which runs the worker twice under **different `PYTHONHASHSEED` values**. Comparing two runs
inside one process proves only that a process is deterministic; the failure this catches — a
set or dict iteration order reaching the event log — is invisible unless the seed varies, and
the fixture strategy iterates a set literal on purpose so there is something to catch. Its
control is `test_changing_one_input_changes_the_hash`: a hash that never moves would be
satisfied by hashing the empty string.

### 3. Look-ahead test passes

Spec §12.3, on the engine rather than on the indicators, over the exit criterion's own
configuration: the full year against the same run truncated by 100 bars.

```
full events 39311   truncated 39201   compared 39201 vs 39201
LOOK-AHEAD TEST: PASS
```

Compared **by timestamp**, not by index, and the difference is a real one worth stating: an
order in flight at the truncation boundary fills against different data in the two runs,
because the full run has the next bar's open and the truncated one does not. That is data
availability, not look-ahead. Every event at or before the last instant the truncated run
could see is identical.

The same property is asserted on synthetic lakes in
`tests/unit/test_backtest.py::test_the_truncated_run_is_a_prefix_of_the_full_one`.

All three clauses were re-verified after the review's fixes. The hash moved -- the fixes
change fills and therefore the log -- and the three properties hold on the new engine: three
processes produced `7ecb728b…`, and the truncated run is still an exact prefix.

### What the criterion does not cover

The criterion is about *one* strategy, *one* fill tier and *one* order type, so the caveats
in the README's [Phase 4 section](../README.md#caveats--what-phase-4-does-not-cover) are not
hedging — they are the boundary of what has been demonstrated. In particular: market orders
only, `BAR_CLOSE` fidelity only, no partial fills, and approximate reference snapshots for
every historical range.

### Evidence

| Claim | Evidence |
|---|---|
| The §6.2 priority table is exact and admits no ties | `test_clock.py` (13 cases, incl. a deliberate tie being fatal) |
| Funding settles before the liquidation check (R5) | `test_backtest.py::test_funding_settles_before_the_liquidation_check`, with a no-funding control |
| An intra-bar mark excursion liquidates | `test_an_intra_bar_mark_excursion_liquidates_even_when_the_close_recovers`, with a one-tick-above control |
| The mark settles back on the close after probing | `test_the_mark_settles_back_on_the_close_after_probing_the_extremes` |
| A fill takes the next print, not the one that triggered it | `test_a_market_order_fills_at_the_next_bar_open_not_the_signal_bar_close` |
| Fill prices round *against* the trader | `test_fills.py::test_a_fill_price_rounds_against_the_trader_not_with_them` |
| Trade prices and mark prices are not interchangeable | `test_the_trade_series_and_the_mark_series_are_not_interchangeable` |
| Limit/stop orders are refused, not approximated | `test_limit_orders_are_refused_with_a_reason_rather_than_approximated` |
| No order is placed before the requested start date | `test_no_order_is_placed_before_the_requested_start_date` |
| The §8.2 formulas match hand computation | `test_metrics.py` (21 cases, each expectation derived in a comment) |
| Sortino divides by N, not by the losing count | `test_sortino_divides_by_every_period_not_only_the_losing_ones` |
| Intraperiod drawdown beats the grid figure | `test_drawdown_is_measured_on_every_tick_and_beats_the_grid_figure` |
| A trade is flat → flat | `test_a_scale_in_and_scale_out_is_one_trade_not_four` |
| A flip splits its fee by quantity | `test_a_flip_closes_one_round_trip_and_opens_another` |
| The §8.4 identity closes with no epsilon | `test_the_four_way_split_closes_exactly` + `test_a_mismatched_ledger_is_fatal_with_no_epsilon_offered` |
| The §8.5 counter counts combinations, not evaluations | `test_the_trials_counter_counts_combinations_not_evaluations` |
| A dead worker stops claiming to be running | `test_a_worker_that_vanishes_stops_claiming_to_be_running` |
| An invalid version cannot be backtested | `test_runs_api.py::test_an_invalid_version_cannot_be_backtested` |
| Downsampling keeps both extremes | `test_downsampling_keeps_both_extremes_of_every_bucket` |
| The whole criterion, end to end | `test_runs_api.py::test_a_run_starts_completes_and_serves_every_artefact` |

### Platform review — after Phase 4

Four independent reviews again, each required to demonstrate a finding numerically before
reporting it. **29 defects were confirmed and fixed**, in code a 1 413-test suite was passing.

The four that would have changed what a user concluded:

- **Spec 8.4's identity could not see the slippage term.** `price_pnl` is defined as the
  ledger's figure *plus* the signed slippage, so the term enters the sum with `+1` and leaves
  with `-1` and cancels: setting the accumulator to zero, or to ten times its value, left
  `build_attribution` perfectly happy. And `price_pnl` is the number that answers "is this a
  price edge or an execution artefact" — on this very run, a reported price leg of +428
  against a realised −1 071, a sign change produced *entirely* by that unverified figure. The
  accumulator is now rebuilt from the recorded fill and reference prices and compared.
- **Slippage was measured against the mark while fills came from the trade series**, so the
  reported cost absorbed the whole mark-trade basis — whose sign follows the side, meaning a
  long-biased strategy reported systematically *favourable* execution and a short one adverse
  on identical data. With slippage modelled at exactly zero and a basis of 20, a run reported
  `slippage_cost = −20` and a price leg of 0 on a position that made +20.
- **The probed mark extremes were written into the equity curve as ordered samples.** Two
  errors at once: the trough was scored against a peak the crest had not yet raised, so a
  long and its mirrored short reported *different* drawdowns on identical price paths (−30%
  vs −46%); and every positioned symbol was moved to its extreme together, so a
  market-neutral pair cancelled in both samples and reported a maximum drawdown of exactly
  **zero** for a book that traversed 4%. The extremes are now a band carried with the close
  sample, and nothing observes a probe price — including strategy hooks, which used to see
  one as `ctx.mark()`.
- **MAE and MFE never included the closing leg.** `mark()` was their only writer and the exit
  is booked after the last mark, so a trade that *lost* 8.05 could report its worst excursion
  as **+0.95**. 194 of this run's 623 round-trips (31.1%) were affected, always optimistically,
  and a liquidation — whose realised loss has no preceding mark at all — was absent from its
  own trade's MAE entirely. Spec 8.3 says this distribution is what stops get sized from; the
  5th-percentile MAE was 6% too tight. After the fix, **0 of 623** report an MAE better than
  where they ended.

Two more were silent data loss of a different kind: a funding settlement whose timestamp
preceded the first mark sample was dropped in full — no cashflow, no event, no flag, and
`funding_pnl` reporting a confident 0 — and it was reachable on a *complete* lake, because
the first mark of any range landed at `start + 59 999 ms` while `warmup_start_ms` routinely
puts the range start on an 8-hour boundary, which is exactly where funding settles.

Four more would have destroyed work or refused it: a completed backtest was discarded by a
manifest *presence* check that ran after the engine; a live orphaned worker was marked
`failed`, which unlocked `delete` and removed its directory out from under it; cancelling a
run this server did not launch signalled a possibly-recycled pid and was demonstrated killing
an unrelated process; and every backtest of a strategy declaring a `bool` parameter was
accepted with a 201 and then failed in the worker, 100% of the time.

The full list, with the reproduction for each, is in
`tests/unit/test_phase4_review_fixes.py`. Six findings were in the React app and are named
there separately — a Python test asserting a button's label would be theatre.

### Two defects the new tests found before the review did

Worth recording because both were in code that already passed everything else:

- **`_cagr` raised `OverflowError` on a short, profitable run.** Annualising is
  `ratio ** (365/days)`, and a one-hour backtest that tripled gives `3 ** 8760` — outside
  float entirely, not merely large. `OverflowError` is an `ArithmeticError`, so the
  `math.isfinite` guard never saw it, and the whole metrics computation died. The better the
  short run went, the more likely it was to produce nothing at all.
- **A `queued` run was reaped the instant it was read back.** `create` left `heartbeat_ms`
  null and `_reap` treated null as "never checked in", so every created-but-not-yet-launched
  run failed immediately. The heartbeat is now stamped at insert, and a row without one is
  judged by `created_ms` rather than presumed dead.

---

## Phase 5 — all golden scenarios reproduce; the fill tier degrades and is surfaced ✅

Two clauses. The first is a standard about *how* a thing is tested; the second is a promise
about what the user is told.

### 1. All golden scenarios reproduce

**32 scenarios** in `tests/unit/test_golden_scenarios.py`, and what makes them golden is that
every expected number is derived in the test's own docstring, from prices the test wrote into
the lake, in arithmetic a reader can check without running anything. That is a deliberately
harder standard than "assert whatever the engine printed": a test that records the current
answer passes forever, including after the answer becomes wrong.

They cover every fill path spec 6.4 and 6.5 describe:

| | |
|---|---|
| `BOOK_WALK` | the ladder walk, the depth-exhaustion penalty on both sides, one fill at the VWAP |
| `BOOK_TICKER` | `k·√(notional ÷ 1 min notional)` to four decimal places, and the refusal when that denominator is zero |
| `TRADE_ONLY` | the fill lands on the **next** print and carries *its* timestamp, not the arrival's; the 60 s deadline |
| Limit queue | a touch is not a fill; the queue consumed then filled; three increments, three `on_fill`s; the aggressor-side test; the through-trade bound; the `min` refinement |
| Time in force | `GTC` rests · `IOC` takes and expires the rest · `FOK` leaves nothing behind · `GTX` expires rather than crossing |
| Triggers | a mark stop firing inside a bar's range · take-profit in the opposite direction · its control · the trailing ratchet · a contract-price stop on the tape · the reference re-stamped at the trigger |
| Tiers | a limit refused at `TRADE_ONLY`, a stop refused at `BAR_CLOSE`, both by name |
| Fees | maker 7.99980000 and taker 20.00005000 on the same run |

One example, in full, because it is the shape of all of them. Ladder `1 @ 40 000.10` and
`5 @ 40 000.20`; a buy of 3:

```
cost = 1 × 40 000.10 + 2 × 40 000.20 = 120 000.50
avg  = 120 000.50 / 3               = 40 000.1666…
tick, rounded against the buyer      → 40 000.20
```

Had the model taken the touch for the whole order it would have paid 40 000.10 — a tenth of
a tick per unit, always in the trader's favour, which is the shape of error that compounds.

Determinism is asserted at the tier with the most to get wrong rather than the least: three
runs of a `BOOK_TICKER` scenario that merges a trade tape with a lazily-pulled book, schedules
fill checks from inside the loop, and consumes a queue whose bound depends on the order
observations arrived in — one hash.

### 2. The fill tier correctly degrades and is surfaced in the UI

Spec 4.2 decision 3: *"That degradation is recorded in run metadata and shown as a badge on
the results page. **Never let a fill-model downgrade happen invisibly.**"*

The tier is **derived from the lake, never accepted from the caller**. On the real lake:

| Range | Asked | Got | Why |
|---|---|---|---|
| 2024-03-25 … 27 | `BOOK_WALK` | `BOOK_TICKER` | no depth before the collector started |
| 2026-07-14 … 16 | `BOOK_WALK` | `TRADE_ONLY` | `aggTrades` only |
| 2025-08-01 … 2026-08-01 | `BOOK_WALK` | `BAR_CLOSE` | no tick coverage across a year |
| 2024-03-25 … 27 | `BAR_CLOSE` | `BAR_CLOSE` | *asked for less than it could have had* — `TIER_BELOW_DATA`, not a degradation |

That last row is why there are two flags. A deliberate low-fidelity sweep and a data
limitation both produce "a tier below the best available", and collapsing them would make the
degradation badge meaningless through familiarity.

Run **#10** is the end-to-end demonstration: `EMACross` over 2024-03-25 → 27, asking for
`BOOK_WALK`. It executed at `BOOK_TICKER`, and the Runs table shows

```
BOOK_TICKER   ↓ asked for BOOK_WALK      [BRACKETS_APPROXIMATE] [FILTERS_APPROXIMATE] [TIER_DEGRADED]
```

with the reason in full on the results page: *"asked for BOOK_WALK but the lake only supports
BOOK_TICKER over this range, so the run executed at BOOK_TICKER. Lost: book. L2 depth exists
only from the moment the collector started recording (spec 4.2)…"*

**And it is said before the run, not only after.** The New Backtest dialog calls the same
`resolve_tier` the worker will call, over the same range, as the dates are typed — so
*"↓ This range only supports BOOK_TICKER, so the run will execute there instead of at
BOOK_WALK. Unavailable: `ctx.book()`."* appears before anything is queued. Showing a
degradation only on the results page is necessary and late: the user has already waited.

### Throughput, on real data

| Tier | Range | Ticks | Events | Wall |
|---|---|---|---|---|
| `BOOK_WALK` | 2026-08-01, 30 min of collector depth | — | 114 | 0.8 s |
| `TRADE_ONLY` | 2026-07-14 … 16 | 2 497 179 | 2 506 236 | 16.6 s |
| `BOOK_TICKER` | 2024-03-25 … 27 | 4 155 247 | 4 164 150 | 47.4 s |
| `BAR_CLOSE` | 2025-08-01 … 2026-08-01 | — | 2 644 779 | 49.4 s |

The `BOOK_TICKER` row is the one that needed a design decision. Two days is ~30 million
`bookTicker` rows, and pushing each through the event queue would have built 30 million event
objects to answer a question only asked at order arrivals. Book state is *pulled* instead —
scanned in C++, delivered as Arrow batches, located by binary search — which is exactly
equivalent, because a snapshot replaces its predecessor rather than incrementing it.

The `BOOK_WALK` row also produced the honest failure worth recording: 5 of its 13 orders were
**rejected**, every one with *"no depth snapshot is in force at this instant"*. They were
submitted before 08:07:23, when the collector's depth stream starts. The staleness bound
refused to walk a book nobody had recorded rather than filling against one from an hour
earlier.

The same run showed the side-symmetry the new mid reference was chosen for:

```
BUY  0.01 at 63052.30   ref 63052.25   slippage 0.0005
SELL 0.01 at 63054.90   ref 63054.95   slippage 0.0005
```

Exactly half a tick each way. Measured against the *last print* instead, the two sides would
have carried the trade-versus-book basis with opposite signs.

### What the criterion does not cover

`aggTrades` covers 15 days of this lake and `depth20` covers two, so tick coverage — not the
engine — is what bounds high-fidelity work today. The other limits are in the README's
[Phase 4/5 caveats](../README.md#caveats--what-phases-4-and-5-do-not-cover): our own orders
do not move the market, the mark stays at 1-minute resolution at every tier, and there is no
`empirical` latency model until paper trading exists to calibrate one.

### Platform review — after Phase 5

Four independent reviews again, each required to demonstrate a finding numerically before
reporting it. **24 defects were confirmed and fixed** — 19 that changed behaviour, five in the
UI or the documentation — in code a 1 493-test suite was passing, plus one I found myself
while running the exit demonstration.

The recurring shape is worth naming, because it is not "a line was wrong": **five of them
were comments asserting a property the code did not have.** Two claimed a choice was
pessimistic while implementing the opposite. A comment is not a test, and a wrong comment is
worse than none — it is the reason nobody looks again.

The five that changed what a user would conclude:

- **An unobserved queue level filled better than any observed one.** The `None` branch set
  `queue_ahead = 0` and called it "the only reading that cannot manufacture a fill out of an
  unknown queue". Zero means *front of the queue*. A buy limit behind an unknown queue filled
  5 BTC where a measured queue of 5 filled 2 and a measured queue of 1 000 filled nothing —
  the run was rewarded for having *less* data, which is exactly the fiction spec 6.4 names as
  "the single most common way limit strategies look profitable and are not". Reachable
  whenever the book goes dark and the tape does not.
- **Trailing stops were asymmetric by side.** The ratchet and the trigger test shared one loop
  over `[high, low, close]`, so a long-protecting SELL ratcheted on the high and fired on the
  low, while the mirrored short-protecting BUY — whose favourable extreme is the *low* — was
  tested against the high before ratcheting. Same bar, same callback rate, both orders ending
  at the identical trigger level 99.99, and only one of them exited. Fixed by separating the
  passes; the fix then created its own regression (below).
- **A cancel issued before its order landed was discarded, not delivered.** Right for a market
  order, which fills at arrival; wrong for everything that rests. A strategy that quoted and
  pulled inside one latency window could never pull — the order stayed on the book and filled
  sixty seconds later, leaving the run holding a position it had explicitly cancelled. Losing
  the *race* was already modelled correctly one function away.
- **Order-entry size floors were applied to partial fills.** `filters.py` had said in prose
  since Phase 2 that they must not be. A 0.001 BTC sweep against a live 1 BTC bid produced an
  increment worth 39.99 against a 50 minimum, and **rejected the whole order** — reporting
  `ORDERS_REJECTED` for something the strategy never did. The first fix keyed the exemption on
  `is_maker`, which left the taker half: the crossing part of a marketable limit into a thin
  touch died the same way.
- **Trade- and depth-driven indicators were never driven at all.** `CVD` and `BookImbalance`
  are in the library, spec 5.4 lists them, and the engine called neither `indicators.on_trade`
  nor `on_depth`. A strategy using either crashed mid-run comparing `None`, or — if it guarded
  on `.ready` — completed silently having never traded. `CVD` also divided by the scale twice:
  every volume in that series was a hundred-millionth of its true size, on a line whose whole
  purpose is its slope.

Two more were about telling the truth rather than computing it. The reproducibility block
**overwrote spec 12.1's recorded inputs with the executed outputs**, so a run that asked for
`depth_exhaustion_pct=0.25` and then degraded stored the `BOOK_TICKER` default instead, and two
runs with different requests produced byte-identical records. And `WARMUP_TIER_DIFFERS` fired
on *every* default run over the collector's own window, because it compared the manifest's
tier against what the run executed at rather than against what the lake supports — flagging a
deliberate choice as an anomaly, which is how a badge stops meaning anything.

**A regression from one of my own fixes, caught by the next reviewer.** Separating the ratchet
from the trigger test is right for a mark bar's range, which says *what* the mark reached and
never when. Applied to the trade tape it is within-millisecond look-ahead: a print at 39 700
fired a stop sitting at 39 600 only because a *later* print at 41 000 had already moved the
level to 40 590. The tell was that reversing the two prints changed nothing — a causal model
cannot be indifferent to the order of its own inputs. `observe_price` now takes `ordered`, and
two tests assert that reversing an ordered tape *does* change the answer.

**One I found myself**, running the exit demonstration rather than reading the code:
`resolve_tier` promised `BOOK_WALK` for 2026-08-02 — a day the collector has depth for and the
bulk kline archive has not published yet. A tier check that reads only the tick datasets will
advertise the best fill model in the platform for a range with no bars to drive a strategy at
all. The first fix used partition presence, which is *month*-granular for klines and therefore
still passed; the second reviewer demonstrated that a range half outside coverage ran to
completion on half its data with no flag. It is checked by row bounds now.

Three were pre-existing and predate Phase 5: a migration race that produced
`duplicate column name` in 10 of 60 concurrent opens — reached from the worker's startup,
*outside* the `try` that turns a failed run into data, so the run vanished without recording
why; a `SchemaTooNew` refusal that left the connection open and the file locked on Windows,
while telling the reader to replace it; and `strategy_trials.best_run_id` left dangling after a
run was deleted.

### Evidence

| Claim | Evidence |
|---|---|
| Every fill path matches hand-computed arithmetic | `test_golden_scenarios.py` (32 scenarios) |
| A touch is not a fill | `test_a_trade_at_the_limit_price_behind_a_queue_is_not_a_fill` |
| Aggressor side is not inverted | `test_a_buy_aggressive_print_at_our_bid_does_not_touch_us` |
| A through-trade is bounded by the aggressor's size | `test_a_print_through_our_level_fills_us_bounded_by_the_aggressor_size` |
| The queue bound only tightens | `test_the_queue_bound_only_ever_tightens` |
| An unobserved level cannot fill on an at-level print | `test_phase5_review_fixes.py::test_an_unobserved_level_never_fills_on_an_at_level_print` + two controls |
| Partial fills emit `on_fill` per increment | `test_a_partial_fill_emits_on_fill_per_increment` |
| Post-only expires rather than crossing | `test_post_only_is_expired_rather_than_crossed` |
| `FOK` leaves no partial behind | `test_fill_or_kill_leaves_no_partial_behind` |
| A stop is not a guaranteed price | `test_a_mark_triggered_stop_fires_inside_the_bar_range_and_fills_as_a_market_order` |
| Trailing stops are side-symmetric | `test_a_trailing_stop_fires_on_the_same_bar_whichever_side_it_protects` + the engine-level twin |
| The tape is causal, the mark range is not ordered | `test_a_contract_price_trailing_stop_does_not_ratchet_on_a_later_print` + `test_the_reversed_tape_gives_the_opposite_answer` |
| A cancel is always delivered | `test_a_cancel_issued_before_the_order_lands_still_cancels_it` |
| No order is left permanently open | `test_a_market_order_clamped_at_arrival_does_not_stay_open_forever`, `..._unfillable_residue` |
| An increment is not held to order-entry floors | `test_a_partial_increment_is_not_held_to_an_order_s_size_floors`, `test_the_crossing_part_of_a_limit_into_a_thin_touch_is_not_rejected` |
| The tier is derived, degrades, and is distinguishable from a choice | `test_tiers.py` (15 cases) |
| The published capability matrix matches what the engine enforces | `test_the_published_capability_matrix_matches_what_the_engine_enforces` |
| A range with depth but no bars is not runnable | `test_a_range_with_depth_but_no_bars_is_not_a_book_walk_range`, `..._half_outside_the_bars_...` |
| Determinism at the tick tiers | `test_two_identical_runs_over_tick_data_produce_the_same_event_hash` |

---

## Phase 6 — every limit demonstrably halts a misbehaving strategy ✅

Spec §13's criterion is one sentence with two halves that are easy to conflate. Spec §7's
table has an **action** column with two values, and a suite that only checked "something
happened" would pass on an engine that had them backwards:

- **`REJECT`** refuses one order and the run continues.
- **`HALT`** stops the run and closes out.

Getting them the wrong way round is the worse failure of the two, because a strategy whose
orders are silently rejected looks exactly like a strategy that decided not to trade.

**Each of the nine limits gets a strategy written to breach exactly it**, in
`tests/unit/test_risk_limits.py` (36 cases). Not one strategy driven by a flag: the
misbehaviour sits in the test body next to the number the limit is set to, so the arithmetic
can be checked without running anything.

| Limit | Action | Demonstration |
|---|---|---|
| `max_position_notional` | REJECT | 1 BTC at 40 000 fits under 50 000; the second projects 80 000 and is refused |
| `max_leverage` | REJECT | 5× on 100 000 permits 12.5 BTC; an order for 13 projects 5.2× |
| `max_open_orders` | REJECT | six limits asked for under a ceiling of three; three admitted, three refused |
| `max_orders_per_minute` | REJECT | ten submissions in one bar under a ceiling of four |
| `max_drawdown` | HALT | 1 BTC long into a falling ramp; realised PnL stays zero throughout |
| `max_daily_loss` | HALT | boundary tested on the unit — see below |
| `min_equity` | HALT | a 50% floor on 100 000, reached by a 2 BTC long at a 15 000 mark |
| `max_consecutive_losses` | HALT | three losing round-trips against a falling tape |
| `halt_on_liquidation` | HALT | 20× into a 5% adverse move |
| *auto-triggers* | KILL | invariant failure re-raises; five exchange rejections trip the switch |

### The two failure modes that were asked for by name

**The fill order-of-operations bug.** A size limit evaluated against the position *as it
stands* passes every order right up to the one that ruins the account, and then passes that
one too — the order under consideration is precisely the thing that would breach the limit.
Checking against the position the order *would produce* is necessary and not sufficient: a
strategy that submits ten orders inside one bar has ten in flight before any of them fills,
and each one measured against the position plus itself is comfortably inside the limit.

`projected_exposure` therefore counts everything working or pending, as a **bound over fill
orderings** rather than as a net:

```
upper = position + every buy that could still fill
lower = position - every sell that could still fill
exposure = max(|upper|, |lower|)
```

The net understates it. A long of 10 with a working buy of 5 and a new sell of 20 nets to
|10 + 5 − 20| = 5, while the path where the buy fills first reaches 15 — and 15 is the number
the account has to survive. Brute-forced over every interleaving for positions in [−4, 4] and
queues up to 3, the bound is the exact supremum rather than a loose one.

**The daily-loss reset boundary.** Two independent things had to be got right, and only one
of them was obvious.

The day is the UTC calendar day, `ts_ms // 86_400_000`, so a timestamp of exactly
`00:00:00.000` opens the new day rather than closing the old one. That part is a one-line
choice. The part that was wrong is that **no equity sample ever lands on the boundary**:
samples arrive at bar closes, so the first observation of a UTC day is up to a whole bar
late. Baselining the day on it made everything inside that bar belong to neither day — a run
closing 1 March at 99 976 and opening 2 March at 95 976 had a 4 000 loss, twice its 2 000
limit, attributed to nothing at all, and did not halt. The baseline is now the last
observation of the previous day carried forward, which is §3.4's own LOCF rule.

The threshold is a fixed amount from the **run's** starting equity, not from the day's. §7's
"2% of starting equity" describes the default *value*; the alternative reading tightens the
leash every time the strategy loses, which is a behaviour somebody should have to ask for.

### The kill switch, and what it deliberately is not

§7 lists six things the kill switch does. Three — stopping strategy processes, cancelling at
the exchange, wiping the key session — are actions on a live session, and there is no live
session until Phase 7. The other three are **state**, and state is what a backtest can
exercise now: the trip is recorded with its timestamp and trigger, cancel-only is the default
(§7.3: force-closing everything at market during a flash crash can be worse than the
exposure), and `require_clear()` refuses to start until somebody un-arms it.

So `KillSwitch` holds the state and names the live actions as callbacks Phase 7 will supply.
The alternative was writing the live half now against an exchange client that does not exist,
which produces code that has never run and a sign-off that means less than it says.

### What a halted run reports

A halt stops the run **between events, never inside one**. A breach can be detected halfway
through booking a fill; unwinding there would leave the ledger and the trade builder
disagreeing, and the invariants would then fail for a reason that has nothing to do with the
limit that fired.

The consequence that mattered most was subtler. `_finalise` used to advance to the end of the
*requested* range and take a last equity sample there, carrying the halt's equity flat across
a window the run never observed — and every ratio metric is computed on a grid over that
range. A run that lost 10% in twenty-one minutes of a three-day window reported
`volatility = 0.0` and `sharpe = None`, because all seventy-one hourly grid points fell after
the halt. With the kill switch armed it was worse: the single grid step containing the forced
exit was the only non-zero return, and the run reported a **Sharpe of +11.1** and 99.95%
exposure while flat. A halted run is now finalised where it stopped, and the warning says so.

### Also built, at the same time

Four gaps were raised against the platform. One of them was not a gap; the other three were.

**Open interest was half-built.** `ctx.oi()`, the OI indicators and the bulk `metrics` archive
were all wired end to end — but the collector recorded no OI at all, so the series stopped
at the last archived day and `ctx.oi()` returned `None` for everything newer. An
`OpenInterestPoller` now records it live into the same `metrics` dataset. Timestamps are the
endpoint's own and are deliberately **not** snapped to the archive's five-minute grid:
snapping looked tidier and created a collision a later backfill could not resolve, where
`OIDelta` measured the archive-versus-live discrepancy at one instant instead of the change
between two, and which row won depended on physical file order.

**Cancel existed; modify and `on_cancel` did not.** `ctx.modify(order_id, price=, qty=)` now
amends a resting limit order, and the two things it does *not* hide are the point:

- It takes latency. A fill landing inside its flight time fills at the old price and the old
  size — §6.3's R19 again.
- It usually costs queue position. Moving the price or increasing the size sends the order to
  the back; only a strict decrease keeps the place. Queue position is the most valuable thing
  a maker strategy owns, and a model that hands it back for free makes every quoting strategy
  look better than it is.

`on_cancel` fires for cancels, expiries **and** rejections, with `status` distinguishing
them. Firing on only one would be the trap: a strategy that quotes both sides and waits for
`on_cancel` before requoting would hang forever the first time a post-only order silently
failed to enter, which is §6.5's named hazard.

**No platform-level flatten.** Crypto has no market close, so there is no end-of-day flatten
to inherit from equities. What it has instead are two deadlines every strategy otherwise
re-implements: `max_hold_ms` and `before_funding_ms`. Both are off by default — a platform
that flattens positions nobody asked it to flatten is not reporting the strategy — and the
exit is a market order through the ordinary path, paying the spread, the latency and the fee.
The hold clock runs from when the position **opened**, not from the last increment, or a
strategy that scales into a winner would never reach any deadline.

**No parallel parameter sweep.** The premise was half right: there was no sweep at all, serial
or otherwise — it is a Phase 9 Lab feature. `perplab/lab/sweep.py` lands it early because it
is the one Lab tool that is not an *interpretation*: it runs N backtests and reports what each
returned, drawing no conclusion and adding no statistic that could be wrong. Processes rather
than threads, because backtests are pure Python bytecode and a threaded "parallel" sweep would
take exactly as long while looking faster in the code.

### Review — 38 defects

Four independent reviews again, each required to demonstrate a finding numerically before
reporting it. **38 defects were confirmed and fixed**, in code a 1 587-test suite was
passing. Nine of them were comments or docstrings asserting a property the code did not
have — the same recurring shape as Phase 5, and worth counting separately because a wrong
comment is worse than none: it is the reason nobody looks again.

The five that would have changed a conclusion:

- **The drawdown limit was fed the intrabar trough for both the peak and the fall**, so its
  drawdown was systematically smaller than the one the results page reported. A run publishing
  a 19.00% max drawdown against a configured 15% ceiling never halted — and the comment
  claiming the two "cannot disagree" was false.
- **A halted run's metrics covered the range it never reached** — the Sharpe of +11.1 above.
- **`max_open_orders` and `max_orders_per_minute` refused reduce-only exits.** Four resting
  quotes under a ceiling of four, and the strategy's own `ctx.close()` was refused; the
  platform's `AutoFlatten` hit the same wall. An account that has breached a limit is
  precisely the account whose exits have to work.
- **An amendment could overtake the order it amends**, which put the order on the resting book
  twice and let it fill from a print published *before its own arrival* — look-ahead, in the
  one module whose docstring promises there is none. Not a corner case: 13 of 20 seeds under
  the default latency model.
- **A failed `openInterest` poll suspended the staleness alarm on `depth20` and `bookTicker`**,
  because `metrics` was wired into the collector as though it were a socket stream.
- **An `on_cancel` hook that placed an order could hang the run.** `_drain_order_ends`
  documented a bound it did not have -- "an order can only leave the book once" is false when
  a hook *creates* orders, and one refused by the risk layer terminates synchronously and
  appends to the list being drained. One cancel produced twelve thousand dispatches in
  twenty-four seconds with a two-second `timeout_s`, because `_checkpoint` only runs between
  top-level events.

Two more were the platform lying about itself rather than computing wrongly: a rejected
auto-flatten latched its symbol forever, was never retried, and was still reported as
`auto_flattens: 1` with the `AUTO_FLATTENED` flag; and the results page printed *"nothing was
refused because nothing could be"* directly above a table of twenty refusals, because
`RISK_LIMITED` was derived from the size limits alone.

And `SPEC_VERSION` had not been bumped. Backwards was safe — a version-2 file replays
correctly — but a *new* file also declared 2, so a Phase 5 reader would accept it and silently
drop the limits, replaying a run executed under a 5× cap as unconstrained. Refusing an unknown
version is that field's entire job.

### Exit criterion

| Claim | Where it is checked |
|---|---|
| Every REJECT limit refuses the order and lets the run continue | `test_risk_limits.py`, one strategy per limit |
| Every HALT limit stops the run and records the reason | same file, halt section |
| The size limit counts orders still in flight | `test_the_size_limit_counts_orders_still_in_flight` |
| A reduce-only exit is never refused | `test_a_reduce_only_order_is_never_refused_for_size`, `test_a_reduce_only_exit_survives_the_count_limits` |
| The daily baseline rolls at UTC midnight, carried forward | `test_the_daily_loss_baseline_rolls_at_utc_midnight`, `test_a_loss_that_straddles_midnight_is_not_lost_between_two_days` |
| The daily threshold is fixed from the run's start | `test_the_daily_threshold_is_a_fixed_amount_from_the_runs_own_start` |
| An out-of-order sample cannot reset the day | `test_an_out_of_order_sample_cannot_reset_the_day` |
| Drawdown is scored crest-to-trough | `test_the_drawdown_peak_rises_on_the_crest_not_the_trough`, `test_the_risk_layer_sees_the_intrabar_trough_not_the_close` |
| Drawdown is mark-to-market, not closed-trade | `test_max_drawdown_halts_on_mark_to_market_not_on_realised_pnl` |
| The kill switch defaults to cancel-only | `test_the_kill_switch_defaults_to_cancel_only` |
| Arming flatten closes and says so | `test_arming_flatten_closes_the_position_and_says_so` |
| A halted run refuses further orders | `test_a_halted_run_refuses_further_orders` |
| An invariant failure trips the switch and refuses to report | `test_an_invariant_failure_trips_the_switch_and_refuses_to_report` |
| The risk layer's own rejections do not trip the rejection trigger | `test_a_risk_rejection_does_not_count_towards_the_rejection_auto_trigger` |
| An amendment costs queue position, measured not labelled | `test_an_amended_order_actually_goes_to_the_back_of_the_queue` |
| An amendment takes latency | `test_an_amendment_takes_latency_like_any_other_instruction` |
| `on_cancel` fires for a post-only that never entered | `test_on_cancel_fires_when_a_post_only_order_silently_fails_to_enter` |
| The hold clock runs from the open, and a flip restarts it | `test_the_hold_clock_runs_from_the_open_not_from_the_last_increment`, `test_a_flip_restarts_the_hold_clock` |
| A refused flatten unlatches and is not counted | `test_a_refused_flatten_unlatches_and_is_not_counted` |
| A sweep is deterministic and ordered by its grid | `test_two_workers_produce_the_same_answers_as_one`, `test_a_sweep_runs_every_point_and_returns_them_in_grid_order` |
| An empty `risk_limits` replays as no limits, not as the defaults | `test_the_engines_default_is_no_limits_and_it_says_so` |

---

## Platform review — after Phase 3

Four independent reviews ran over the whole platform, each required to *demonstrate* a
finding numerically before reporting it. **28 defects were confirmed and fixed**, every one
of them in code that a 1 244-test suite was already passing.

The severe ones were data loss, and both were invisible by construction:

- **A REST failure part-way through an aggregate-trade catch-up skipped trades
  permanently.** Pages were accumulated into a local list and returned at the end, so a
  failure on page three discarded pages one and two *while the cursor had already moved
  past them*. Those ids are never requested again, and because the cursor moved
  consistently the id-jump check has nothing to notice. Demonstrated: 2 000 trades gone,
  reported as a DISCONNECT/RECONNECT pair — which the gap detector then filed as
  **explained**. The cursor now advances only after a page has been handed to the writer.
- **A partial flush failure duplicated every partition already written.** The buffer was
  cleared after the whole loop, so a failure on partition three left one and two on disk
  *and* still buffered, and the documented retry wrote them again. The duplicates land in
  tick datasets, where `duplicate_rows` is not computed (F9), so nothing reported them.

Two more turned the Phase 1b criterion itself into a lie in opposite directions: an
overnight crash's RESTART record sat outside the 30-second event window and read as
**unexplained**, while a twenty-second REST recovery accounted for a **24-hour** outage
under a pure-overlap match. And in the accounting core, `_require_margin` omitted the fill's
realised PnL — invisible everywhere except a Case C flip, the one fill that both realises
and needs margin, where a losing flip left the account with a **negative available balance**
and a position its wallet could not fund.

The full list, with the reproduction for each, is in `tests/unit/test_review_fixes.py`.
Two findings were deliberately **not** fixed and are named there with the reasoning:
`writer.py`'s missing `fsync`, and the constant-time password comparison, which no
functional test can distinguish from `==`.

## Verification discipline

A passing suite proves a fix works, not that it is *pinned*. Every fix was re-broken and
watched to fail:

| Mutation set | Result |
|---|---|
| Phase 2 accounting core (13) | **13 killed** |
| F4 / Phase 0 work (20) | **19 killed**, 1 proven equivalent |
| `gaps` future-clamping (1) | **1 killed** |
| Phase 3 + review fixes (54) | **53 killed**, 1 proven equivalent |
| Phase 4 engine and analytics (22) | **21 killed**, 1 proven equivalent |
| Phase 4 review fixes (24) | **24 killed** |
| Phase 5 realism layer + review fixes (44) | **34 killed**, 10 survived → gaps closed → **44 killed** |
| Phase 6 risk layer + review fixes (48) | **31 killed**, 17 survived → gaps closed → 40 killed, 3 survived → fixtures rebuilt → **48 killed**, re-run on the final code |

Survivors are investigated, not patched until green. `except asyncio.CancelledError: raise`
was checked rather than excused — `CancelledError` derives from `BaseException`, so the
surrounding `except Exception` never caught it and the mutation changes no behaviour.
`secrets.compare_digest` versus `==` is the same: both accept the right token and reject the
wrong one, and the difference is timing-attack resistance. Both clauses stay.

**Phase 6's pass found the largest gap yet, and it is the same pattern with a new face.**
Forty-eight deliberate defects, seventeen survivors — more than a third. Two of those
survivors are worth naming because they are not "the fixture was too simple":

- **A test that asserts a label does not test the behaviour the label describes.** The
  amendment test checked that the event log said `queue_priority: "lost"`. That string is
  computed from the same expression as the reset, so a mutation that stopped resetting
  `queue_ahead` still printed "lost" and passed. The replacement measures the queue itself.
- **A flat 40 000 book cannot distinguish the mark from the far touch**, so a risk limit
  priced at the wrong one survived an entire suite of them. `test_phase6_mutation_gaps.py`
  quotes a book 1 000 wide and puts the ceiling between the two answers.

The rest were the familiar shape: a drawdown series with no intrabar band cannot tell a peak
taken from the crest from one taken from the trough; a `max_consecutive_losses` limit of 2
cannot tell whether a flat trade was counted, because both readings reach 2; a smoke run
whose synthetic ladder sits at the mark cannot tell a modelled fill from a mark fill.

**Three of the replacements were themselves too weak, and the second pass caught them.**
A book quoted 39 000 / 41 000 against a 40 000 mark has a *mid* of exactly 40 000, so the
fixture written specifically to separate "priced at the mark" from "priced at the reference"
still could not. Widening it to 39 000 / 43 000 separated the numbers and put the ask outside
`PERCENT_PRICE`'s 5% ceiling, so the fill was refused by a filter and the test measured
nothing at all; 39 500 / 41 900 is wide enough to separate and narrow enough to trade. The
flatten-latch fixture had the same problem in time rather than price: at 10 ms of latency the
exit fills long before the next mark, so one order is sent whether or not the latch exists —
only a latency above the 60 s mark cadence puts the exit genuinely in flight. And the sweep's
ordering test passed its points in index order, where sorted and unsorted are identical.

The third pass killed all forty-eight.

**Phase 5's pass is the clearest earlier instance of the pattern this table exists to catch.** The
suite was green, four reviews were finished, and forty-four deliberate defects still found
**ten** places where the model could be wrong and nothing would notice. Every one of the ten
had the same shape, and it is the same shape Phase 4's pass found:

- a book quoted at a flat 40 000 cannot distinguish "the row in force at T" from "the row
  after it" — so a one-row look-ahead in the book stream survived;
- a tape and a kline series both at 40 000 cannot distinguish one account of the market from
  two — so emitting the kline's own prints alongside the tick tape survived;
- a `MARKET_LOT_SIZE` whose numbers equal `LOT_SIZE`'s cannot say which filter ran — so
  validating a triggered stop against the wrong one survived.

`tests/unit/test_phase5_mutation_gaps.py` exists to close them, and each test constructs the
coincidence its predecessor lacked: a book that moves by a tick a second, bars 500 apart from
their tape, a 500 BTC position that is legal to accumulate and illegal to exit in one market
order. Two of those fixtures needed a second attempt themselves — a 10% mark drop liquidated
the position before the stop could fire, and a 25% bar/tape gap tripped `PERCENT_PRICE`
before it could test anything, both of which are the same failure in miniature. The re-run
killed all forty-four.

**Five earlier mutations exposed genuine test gaps**, with the same shape — each test looked
correct and asserted the right thing about a fixture that could not tell the answers apart:

- an empty poll batch counted as liveness;
- a socket reconnect rebaselined the pollers — the test never populated `_last_msg_s`, so
  clearing it was a no-op;
- the ADX exclusivity test used an *inside* bar, where both directional moves are negative
  and the correct and incorrect implementations agree. Only an **outside** bar — higher
  high *and* lower low — distinguishes them;
- the SMA re-sync test used volumes that are exactly representable as doubles, so every
  incremental step cancelled precisely and the test passed with or without the fix;
- the set-canonicalisation test used a four-element set, which has a one-in-24 chance of
  iterating in sorted order under whatever hash seed the process gets. At eight elements it
  is one in 40 320.

The last three were found only because the mutation was run. Each is now pinned by a test
that fails when the fix is removed.

**Phase 4's pass repeated the pattern exactly.** Twenty-two mutations, and the four
survivors were all the same failure: a fixture that could not tell the two answers apart.

- *Running the liquidation check inline in the mark handler* survived, because real funding
  settles at `HH:00:00.000` and a 1-minute mark bar closes at `:59.999` — the two never
  share a millisecond, so scheduling it at priority 2 and running it inline give the same
  answer on any real range. Constructing the coincidence makes them differ decisively: the
  inline check consumes the bar's traversed range *before* funding is applied, so a low that
  only crosses `P_liq` after the payment is never tested against it. That is R5 again, in
  the one shape the ordinary data shape hides.
- *Never emitting the bar-open print* survived because the fixture used a straight ramp,
  where a bar's close **is** the next bar's open. Filling at the price the strategy just saw
  and filling at the next print were the same number. A path with a gap between the close
  and the following open separates them by 50 on every trade.
- *Trusting the event-log kind prefilter without re-checking* survived because the fixture
  put the decoy in a string value, and JSON escapes the quotes inside one — so the raw
  substring never appeared and the re-check was unreachable. The reachable shape is a nested
  *key*: `ctx.log.info("x", kind="FILL")` writes `"fields":{"kind":"FILL"}` unescaped.
- *Clamping a reduce-only order at submission instead of arrival* survived because nothing
  in the suite shrank a position during a latency window. A partial close followed by a full
  close in the same bar does: without the clamp the second order flips the position short.

The one surviving mutation is **proven equivalent**: leaving `heartbeat_ms` null at insert
changes nothing, because the reaper falls back to `created_ms`. Removing *both* defences is
killed, which is the check that the redundancy is deliberate rather than accidental.

**The review fixes were mutated too, and two of those survivors were the same lesson a third
time.** Reverting the exposure denominator survived because the windowing fix, made for a
different finding, already covers the common case — a series that begins *after* the range
does is what separates them. And forcing the first sample into the downsampled chart survived
because the fixture started flat, and `max()` returns the first maximal index, so index 0 was
selected incidentally whether or not it was forced. Both fixtures were rebuilt to be
decisive; all 24 mutations are now killed.

## What changed today

**New:** `perplab/core/risk.py` (the risk layer and kill switch) · `perplab/lab/sweep.py` and
`perplab/lab/__init__.py` (parallel parameter sweeps) · `tests/unit/test_risk_limits.py` (36)
· `tests/unit/test_order_lifecycle.py` (19) · `tests/unit/test_sweep.py` (12) ·
`tests/unit/test_phase6_mutation_gaps.py` (16).

**Changed:** `engine/backtest.py` (risk checks on the submit path, the halt, `_perform_halt`,
`_force_flatten`, `ctx.modify`'s arrival, `on_cancel` dispatch, auto-flatten, truncated
finalisation) · `engine/executor_base.py` (`modify_hook`) · `engine/runspec.py`
(`SPEC_VERSION` 3, `_upgrade_v2`, three new recorded inputs) · `engine/worker.py` (risk config
in, breaches out) · `strategy/context.py` (`ctx.modify`, `OrderEnd`, `UnsupportedOrder` moved
here) · `strategy/base.py` (`on_cancel`) · `strategy/dryrun.py` (reconciled onto
`core.account` and `engine.fills`) · `data/rest_poller.py` + `data/collector.py` +
`exchange/rest.py` (open interest recorded live) · `api/routers/runs.py` (risk request fields)
· frontend `api.ts`, `RunList.tsx`, `RunDetail.tsx`.

**The dry run and the backtester now share their arithmetic.** `DryRunRuntime` had its own
copy of the spec 3.3 entry-price cases and filled market orders exactly at the mark, with no
spread and no fees. It now uses `core.account.Account` as its ledger and
`engine.fills.BookWalkFillModel` to price, against a synthetic ladder. Two implementations of
the same arithmetic agree on the day they are written, and the smoke run is where a
divergence would be least visible -- nobody reads its numbers, so nobody would notice them
drifting. What stays different is the *fixture* -- a synthetic ladder, no latency, no resting
fills -- and each of those is stated rather than hidden.

The reconciliation immediately paid for itself: the review found the smoke run had been
setting its mark to `from_scaled(trade.price)`, dividing an already-unscaled float by 10^8,
so every mark during the tick loop was 0.0003 instead of 30 000. The engine reads
`price_scaled` there and always had.

**Phase 1b's clock.** The collector is live and current to the second. The last collector
outage was the restart at 2026-08-02T14:31:17Z, so the clean window is **8 h 45 m of 72** as
of 2026-08-02T23:16Z and completes 2026-08-05T14:31Z. The `klines`, `markPriceKlines` and
`funding` gaps the report shows for today are the bulk archive publishing a day in arrears,
not collector gaps.

**The running collector predates the open-interest poller**, so `metrics` still ends at the
last archived day. It picks up on the next restart, which the 72-hour clock means should not
be forced.

**One phantom, recorded because it cost an hour.** A real six-point sweep run serially and
then in parallel produced *different answers* — which would be a determinism failure of the
first order. It was not: the mutation harness was running in the background at the time, and
`ProcessPoolExecutor` spawns fresh interpreters that re-import the source from disk. The
parallel workers were reading a mutated engine while the parent had already imported the
clean one. Re-run in isolation, six points agree exactly across serial, parallel and
one-at-a-time — same event hashes, same PnL to the last place. The lesson is about the
harness rather than the platform: nothing that spawns interpreters can share a machine with
a process editing the source under them.

Re-run clean, six points over a month of BTCUSDT: **31.6 s serial, 11.3 s on three workers**,
identical event hashes and identical PnL to the last decimal place. Every point inherited run
#11's limits and halted on `max_daily_loss`, which is the property that makes a grid readable
as a parameter surface -- only the parameters varied.

**Running the sweep for real did find one thing**, though, and only running it could have.
On Windows and macOS a process pool starts workers by re-importing the calling module, so a
sweep called from a REPL or a `python -` script kills every worker before it runs a line.
`sweep()` now turns that `BrokenProcessPool` into a message naming the cause and the fix.

---

## Phase 7 — a strategy papertrades, and the parity report says whether the backtester was telling the truth

**Criterion (spec §13):** *"A strategy papertrades 48 h; parity report shows <5% PnL
divergence vs shadow backtest."*

**Status: ⏳ CLOCK RUNNING.** Both halves of the criterion are built and demonstrated end to
end against the real Binance testnet; the 48 hours is wall time, like Phase 1b's 72, and is
the one thing that cannot be built.

### What ran

A five-minute paper session, started through the real API and executed in its own worker
process exactly as a 48-hour one would be:

```
POST /api/sessions          -> run 1, mode=paper, endpoint=testnet, BOOK_WALK
  live testnet market data, own process, monitor polled every 10 s
  5 bars, 5 orders, 5 fills, 3 314 events dispatched, tape sealed
  status=done  net_pnl=-0.46540030000000000000

create_shadow(run 1)        -> run 2, mode=shadow, source="tape:1"
  replayed the session's own recording through the ordinary backtest worker
  status=done  net_pnl=-0.46540030000000000000

parity report (spec §6.7.1)
  fills            paper 5, shadow 5, delta 0, matched 5
  avg fill delta   0.00000000 bps
  final PnL delta  0.00000000000000000000  (0.00% of gross)
  unmatched        none, either direction
  diverged         false
```

Run twice, twenty minutes apart, on different market data. Both agreed exactly.

**A zero is the right answer here, and it is also the weakest possible evidence.** A session
whose orders are filled by the local simulator, replayed against its own recording, *should*
agree to the last decimal place — anything else would mean the tape or the engine split was
wrong. What it does not show is that the report can detect a divergence, which is the thing
it exists for.

### The architecture, and why it is the one spec §6.1 asks for

Spec §6.1's rule is *"if a piece of logic could live in the shared core, it must"*, and its
table names exactly two things that may differ between backtest, paper and live: where market
data comes from, and where orders go. So those are the only two seams:

```
Engine (engine/backtest.py -- one class, all three modes)
  |- source:    MarketSource   -- LakeSource | TapeSource | the live push
  \- transport: OrderTransport -- SimulatedTransport | ExchangeTransport
```

A paper session runs **the same `BacktestEngine`** — the same order lifecycle, the same risk
checks, the same ledger, the same trade builder, the same metrics. `run()` was split into
`start()` / `step(event)` / `drain()` / `finish()` / `result()` so a wall clock can drive the
same handlers a replay drives, and nothing below `step` knows which it is.

**The regression gate on that split is bit-exactness, not a green suite.** No test in the
repo pins a golden event-log hash, so the suite alone could not have caught a reordering.
The three stored runs whose input data has not changed (`userdata/runs/10, 11, 12`, covering
2024-03-25 → 2024-04-30, one of them at a tick tier) were replayed through the refactored
engine and reproduced their recorded hashes **bit-identically**, before and after the
transport seam was added.

The seven older runs did *not* reproduce, and that is correct rather than alarming: two of
them had already recorded two different hashes for the same spec before Phase 7 began, which
is only possible if the engine's behaviour moved between their executions. It did — Phase 5
changed how stops and limit orders fill, Phase 6 added the risk layer — and **`ENGINE_VERSION`
stayed at 4 through both.** That is a real defect in its own right: spec §12.2's rule is that
a stored reference run's metrics must not change across engine versions without a reviewed
changelog entry, and a version that never moves makes the rule unfalsifiable. Bumped to 5,
with the lesson recorded in the constant's own docstring.

### The session tape, and why the shadow does not read the lake

Spec §6.7.1 wants the shadow re-run *"over that window"*. Reading the lake would have been
easier and is wrong three separate ways: the bulk archive lags about a day so the window is
not in it; the collector's lake is downsampled on its own schedule; and a WebSocket frame the
session genuinely never received would reappear in a lake replay, so the report would blame
the fill model for a missing observation. The tape is written from the reorder buffer's
*output*, in dispatch order, so replaying it reproduces the session's own event sequence.

Two aggregation decisions in the live feed exist solely to match what a backtest reads, and
both would have been silent if got wrong:

- **Mark price is aggregated into one-minute bars.** The liquidation check probes low, then
  high, then close of a `MarkBar`; feeding it 1 Hz point samples with `high == low == close`
  degenerates that to a close-only test, missing every liquidation the market immediately
  recovered from. That error is **one-sided and always in the session's favour**, so it would
  have quietly consumed the whole 5% divergence budget.
- **Depth is downsampled to one second**, matching `collector.DEPTH_BUCKET_MS`. Applying every
  100 ms frame would give the paper session a strictly fresher book than any backtest this
  platform can run, making the session unrepresentative of the thing it validates.

### What running it for real found, and nothing else could have

Six defects, every one of them silent — no exception, no warning, a session that looked
healthy and did nothing:

1. **Every polled kline was dropped as late.** A 1-minute bar closes at T and is published a
   second later, by which time a wall-clock reorder watermark has passed T. The engine
   received **zero bar closes**, the strategy never traded, and the run completed
   "successfully" with an empty trade table. Fixed with per-source watermarks
   (`ReorderBuffer.expect`).
2. **Staleness was measured against the wrong quantity** — how far *behind* a source's
   watermark was, rather than how long since it last reported. A minute-cadence source is
   legitimately sixty seconds behind and perfectly healthy; every one was declared dead
   fifteen seconds in.
3. **A repeated watermark was deduped away.** Reporting an unchanged completion point is a
   liveness signal; suppressing it made a healthy source look silent, which fed straight into
   defect 2.
4. **The first kline page replayed pre-session bars**, seeded per row instead of per page, so
   the second closed bar of the first poll was dispatched minutes behind the engine's clock.
5. **The drain loop popped scheduled order-arrivals**, dragging the clock past market data
   that had not been released, so the next frame was refused as out of order. One frame lost
   per session, reported only as a status-log line.
6. **Klines pinned the release watermark for a whole minute.** Sound but far too weak: market
   data reached the engine in one-minute bursts, so `AutoFlatten` deadlines, the disconnect
   trigger and the live monitor all ran up to sixty seconds behind the market. The correct
   claim is the instant *before* the next bar close, not the instant the current bar opened.

Every one is now a regression test, and each was verified to fail against the reverted code.

### Spec §11, the credential path

Keys live in one process's memory and are never written to disk, a log line, an exception, the
database or an HTTP response. `KeySession` holds them as `bytearray` so `wipe()` can reach
them, exposes bytes rather than `str` everywhere except one deliberately-named method, and a
test reads the module's own source to assert it contains no `open(`, no `Path` and no
`logging`. A session worker receives them as one JSON line on its stdin, which is the only
channel that is neither a file nor an environment variable — `spec.json` is on disk and `env=`
is inherited by every grandchild and readable from the process table.

The kill switch's armed state is now **on disk**. It lived in one process's memory, so
restarting the API silently disarmed it — the exact inverse of spec §7.6's *"requires an
explicit un-arm action before any live session can start again"*, and the failure is
correlated with the trip, because the thing that trips the switch is often the thing that
kills the process.

### Review — 34 defects, every one reproduced twice

Five independent reviews, each given one lens and required to **demonstrate a finding
numerically before reporting it**; then every finding handed to a separate agent whose
instructions began *"your default is that it is wrong"* and who had to reproduce it with its
own script rather than the reviewer's. **34 confirmed, 5 refuted, 14 recorded as unverified
suspicions.** Refuting five is the part that makes the thirty-four worth believing.

The largest cluster was one omission with six faces. `BacktestEngine.run()` is the
composition `start -> step* -> drain -> finish -> result`, and `PaperSession` ran three
fifths of it: `start`, `step`, `result`. Every phase it skipped was load-bearing.

**`finish` is the only place `effective_end_ms` moves off the nominal end** — which, for a
session the API builds, is forty-eight hours away by default. So a five-minute session
published metrics measured over forty-eight hours it never observed:

```
                        as shipped        truth
exposure                0.0622            0.9676     15.6x wrong
days                    2.0               0.128
sharpe                  24.18             --         3 real hourly returns
volatility              0.1018            0.0013         + 44 fabricated zeros
```

On a *stored* run replayed three ways — `run()`, the phases by hand, and the session's loop —
all three produced the identical event hash and identical PnL, and the session's loop
reported exposure 35.8x wrong and Sharpe 3.3x wrong. Identical events, different answers,
because one of them measured a window nobody watched. And those numbers do not stay on the
page: `store.complete` writes the Sharpe onto the run row the Runs tab sorts by, and
`record_trial` enters it into spec 8.5's multiple-testing counter as a genuine evaluation
against real market data.

The same omission also meant **`on_stop` never ran** (a strategy that flattens at shutdown
ended holding its position, zero round-trips, and its own shadow — which does go through
`run()` — closed the position, so the parity report reported the difference as a fill-model
divergence), **`Account.reconcile()` never ran** on the one mode that trades a live market,
and **spec 7's invariant auto-trigger did not exist in paper mode**.

The other clusters, in one line each:

| Cluster | What was wrong |
|---|---|
| Watermarks per symbol | `markPrice` and `depth20` reported one watermark across symbols with a `max`, where the rule is the minimum. 42% of ladders dropped in a two-symbol session, 55% in a three-symbol one — while the function's own docstring said "the minimum". |
| Risk limits off | A session started through the API ran with **every spec-7 limit unset**: `risk_limits` defaulted to `{}` and `RiskLimits.from_json({})` is `unlimited()`. |
| Shadow never built | `create_shadow` and `write_parity` had no caller anywhere in production. Spec 6.7.1's report was code nothing ran. |
| Disconnect trigger | Evaluated only when the socket came *back* — so a socket that never returns, which is the case spec 7 is most about, never halted. And a REST poll failure latched the socket's downtime clock: one kline 429 plus a genuine 2 s outage ninety seconds later halted a healthy session on a reported 92 000 ms against a 30 s ceiling. |
| Parity matching | A ±2 s greedy match on `(symbol, side, qty)` paired unrelated fills: **50 bps reported where the truth was 0, and 0.00 bps where the truth was -909**. Now matched on the order identity the engine already stamps. |
| Shadow latency | `_replay_latency` substituted an `empirical` model built from the session's own draws — which `EmpiricalLatency` then re-draws *with replacement*, in a different order. Per-order arrivals moved by -236 ms to +411 ms, and a backtest that had reproduced its session exactly was reported as a 6.5%-of-gross divergence, flagged with spec 6.7.2's own wording. Worse, the pool was poisoned: `_fire_trigger` re-stamps `arrival_ts` when a stop fires, so a stop that rested nine minutes contributed a 520 000 ms "latency". |
| Spec 11 | `/api/exchange/connect` called a method that does not exist, so a credential could never be entered — and the 400 blamed the user's API key. The kill switch did not wipe the keys. A 422 echoed the secret back in the response body. |
| A wedged worker | `launch_session` used `stderr=PIPE` with nothing reading it until exit. Three pollers logging one warning a second fill Windows's 4 KB pipe in about eleven seconds; the child then blocks inside `write` and can never reach its own exit, so `poll()` never returns and the read that would free it never happens. The session stopped observing the market, stopped heartbeating and stopped reading `control.json` — so the stop button and the kill switch both reported success and did nothing, over a position the run still held. The backtest worker had the same shape and is fixed too. |

**One finding fell between two agents and was nearly lost.** `_PushSource` supplied no
funding schedule, so `_next_funding_ms` returned `None`, `_flatten_reason` returned `None`,
and `AutoFlatten.before_funding_ms` — an exit the operator configures by name — silently
never fired, while the shadow *was* handed a schedule from the tape and would honour it. Each
of the two agents whose files it touched left it for the other. It is fixed and pinned.

None of this was reachable by reading the code, and none of it was reachable by running the
suite, which was green throughout at 1 880 tests.

### Mutation testing — 40 deliberate breakages, 39 killed and 1 proved unkillable

A green suite proves the tests pass. It does not prove they would fail if the code were
wrong, and those are different claims. So the same discipline as Phases 5 and 6: apply a
plausible *wrong implementation* one at a time, run the suite, and count the mutation killed
only if something fails. A survivor is a hole in the tests, not a licence to change the code.

The forty are not random character swaps. Most encode a mistake that was actually made and
then fixed during this phase — `M01` reverts the staleness fix, `M06` pins the kline watermark
back at the open bar's start, `M24` lets a shadow re-resolve its tier from a lake that does not
hold the session's window — and the rest are the mistake the next person would make in the same
place. Every target file is backed up before anything is touched, restored from the backup
rather than by reversing the edit, and verified by sha256 afterwards.

**The first pass killed 25 of 40, and the instructive number is the other fifteen.**

Six of those fifteen never ran at all. Their anchors quoted code the adversarial review had
since rewritten, so `text.count(old) != 1` and the harness skipped them — printing `SKIP`,
which is not `KILLED`, and which a tally counting only survivors would have read as a pass.
That is a failure mode a mutation harness has and a test suite does not: **it can silently test
nothing.** Two were indentation mismatches, one was a `￿` escape that reached the file as
literal backslash-u text, and three quoted rewritten code. All six were re-anchored, the run now
refuses to start unless every anchor resolves to exactly one site, and `M05` was rewritten to
encode the historical bug itself — seeding the kline high-water mark *per row* rather than after
the whole first page, which is what let the second bar of the first page through, stale — rather
than a paraphrase of it. `M26` was retired rather than re-anchored: the empirical-latency
substitution it broke had itself been deleted as a defect, so what replaced it is the defect one
level up, a shadow replaying under any substituted model instead of the session's own.

Nine were genuine survivors, and one of those was only reachable once the six were repaired:

| Tag | The wrong implementation the suite could not see | Why it now fails |
|---|---|---|
| `M07` | The drain horizon is the last released event rather than the release frontier. | The existing test pins that a scheduled arrival may not *lead* the released data; this is the same bound read the other way, so `records[-1]` passed it and was still wrong. On a symbol that goes quiet the watermark advances past a matured arrival while `release_records` returns nothing — the order then waits for the next frame that happens to arrive, and fills at a price the market moved to meanwhile. |
| `M11` | A declared source that has *never* reported stops holding the watermark back. | Every other slow-source test reported at least once before asserting, leaving the first fifteen seconds of every session — the stretch a session starts in — untested. |
| `M31` | A halted engine keeps dispatching. | Asserted on the clock, not on a return value: `_dispatch` advances `runtime.now_ms` first, so an unchanged clock proves the event never entered dispatch rather than being handled quietly. |
| `M32` | A halt requested between events is performed twice. | `_halt_pending` is never cleared, so `halted` is the only guard — and the session loop calls `perform_pending_halt` after *every* drain. A second pass writes a second `KILL_SWITCH` into the hashed log and, with flatten armed, sends a second market order to close an already-closed position, which on a live venue opens one the other way. |
| `M33` | A live session inherits the backtest's wall-clock budget. | Every existing test left `timeout_s` at its 900 s default, where both versions compute the same deadline, so the `<= 0` branch was never exercised at all. A session passing 0 now aborts at event 2 000 against a 3 001-event run that completes. |
| `M34` | A top-of-book quote is applied as though it were a full ladder. | Nothing in the suite had ever pushed a `bookTicker` quote through `_dispatch` — with a `DepthSnapshot` payload the two versions are byte-identical. A 0.01 buy now walks two levels for 60 005, where a one-level "ladder" fills the whole size at the 60 000 touch and reports no slippage. |
| `M35` | A filled auto-flatten leaks its bookkeeping entry. | Three fired deadlines leak three entries per session, unbounded — and any code asking "is this a platform order" then gets the wrong answer for an order that no longer exists. |
| `M37` | A keyed-but-unsigned request does not refresh the idle timer. | Spec §11's 12-hour idle expiry wipes the keys. `api_key_bytes` is read by every keyed-but-unsigned request, listenKey keepalives among them, so a session doing nothing but keeping its user-data stream alive expired mid-position while genuinely active. |
| `M39` | A session runs code the validator rejected. | The endpoint launched a strategy version that had failed validation — look-ahead, a banned import, a syntax error — straight at a live market. Now 400, with no run row created. |

**One survivor is not a hole, and establishing that took more work than a test would have.**
`M15` deletes the `_samples == 0` half of `MarkAggregator.flush`'s guard, which on the face of
it lets a minute nobody observed publish a mark bar priced at zero — exactly what spec §3.4
forbids inventing. It is unkillable, because the state the two versions disagree about is
unreachable: at every call boundary `_bucket is None` if and only if `_samples == 0`.
`__init__` establishes that; `flush` either returns without writing or writes both fields
together; and `offer`'s zeroing of `_samples` is transient, with no `return` and nothing that
can raise between it and the increment. Re-entrancy does not defeat it either — the sink runs
only after `offer`/`flush` has already returned, and neither contains an `await`, so no poller
can interleave inside them. A brute force over every call sequence of length ≤ 4 at three window
sizes, 14 040 sequences, found the invariant held at every boundary. The guard is therefore
defensive against a state the class cannot enter, which is fine code, and a test that poked
`_bucket` by hand to force it would be asserting on a configuration that does not exist.
**Recorded as equivalent rather than closed**, because the alternative is a test that exists to
move a number.

The suite went from 1 995 to 2 004 tests across the two passes.

### What is not done

- **The 48-hour clock.** Wall time.
- **Testnet order execution has never sent an order to Binance.** The credential layer, the
  signed client with its order-safe retry policy, the user-data stream and the transport seam
  are built and tested against a mock; without account credentials none of it can be verified
  against the real exchange, and code that sends real orders should not be signed off on a
  mock. The session runs local fill simulation against live testnet market data, which is the
  other option spec §6.1's table names explicitly.
- **The parity report has only ever been observed at zero.** See above.

## Phase 9 — a full walk-forward on a real strategy produced a stitched OOS curve ✅

Built with Phase 8 deferred by explicit decision (the spec's own ordering note was raised
and overridden): every Phase 9 tool operates on completed runs and the lake, and none of it
touches an exchange.

### What was built

- **Walk-forward** (`lab/walkforward.py`) — anchored/rolling folds over any completed lake
  backtest; per fold, the parameter grid is swept in-sample through the same
  `execute_point` assembly a single run uses, the winner chosen by **neighbourhood-median
  Sharpe** by default (`max Sharpe` selectable and labelled overfit-prone everywhere it
  appears), then evaluated once out-of-sample. The stitched OOS curve is produced in
  **both** sizing readings — compounded as the headline, additive (fixed-notional) beside
  it — because which one is true depends on how the strategy sizes, and the platform
  cannot know. WFE per fold and aggregate, refusing the ratio outright over a non-positive
  IS return rather than letting an OOS loss over an IS loss read as positive efficiency.
- **Monte Carlo** (`lab/montecarlo.py`) — the four spec 9.2 methods. Trade permutation
  *asserts* final-equity invariance every iteration rather than documenting it; each
  method reports only the statistics its resampling can honestly support (a shuffled bag
  of trade PnLs has no time grid, so it has no Sharpe, and the artefact says why);
  `random_start` enumerates every start exactly instead of sampling; ruin is `null` with
  a reason under compounding, where the arithmetic cannot cross zero.
- **Regimes** (`lab/regimes.py`) — causal by construction: expanding-window vol tertiles
  or fixed thresholds, ADX(14) and realised vol reused from the **strategy library's own
  indicators** rather than re-derived, funding regime over a trailing *time* window
  (settlement counts vary by symbol, R17), cascade windows reaching only forward from
  observed liquidation clusters. Labels step at bar close, periods are labelled at their
  open, unlabellable stretches are counted as `unclassified`, thin buckets are greyed and
  never hidden, and a missing liquidation dataset makes the cascade dimension report
  itself unavailable — "quiet" would have been a claim, not an observation.
- **Overfitting diagnostics** (`lab/overfit.py`) — computed *with* every walk-forward
  rather than as a separate job, because an optional overfitting check is one that gets
  skipped on exactly the runs that need it: plateau score (per fold and median),
  IS-vs-OOS scatter with fitted slope, performance decay, the sensitivity surface
  aggregated across folds, and the spec 8.5 trials context carried verbatim.
- **Portfolio** (spec 9.5) — the engine now records per-symbol cumulative net PnL on the
  equity cadence for multi-symbol runs (decomposed from the trade builder's own ledger,
  summing exactly to the account's PnL), persisted as `per_symbol.parquet` and served as
  correlations over per-period *changes* — levels of two profitable symbols correlate
  near 1.0 whether or not their daily fortunes are related — plus rolling correlation,
  because correlations converge in crashes and a static matrix hides exactly that. The
  all-liquid-majors selection-bias warning fires on symbol lists that have never traded
  through a bad period. Allocation modes ship as pure, causality-agnostic functions bound
  through strategy params, never behind the strategy's back.
- **Comparison** (spec 9.6) — synchronous, refusing mismatched ranges with a message that
  names both runs rather than overlaying incomparable curves.
- **Lab jobs** — schema v5, a `LabStore` mirroring `RunStore`'s lifecycle discipline
  (artefacts before status, heartbeats, the `lost`-vs-`failed` distinction), a subprocess
  worker per job, cooperative cancellation (a nested sweep pool cannot be safely killed
  mid-point), grid evaluations counted into the spec 8.5 trials table with `best_run_id`
  honestly NULL when the best evaluation has no run row, and runs with Lab artefacts
  refusing deletion — spec 9 says the link is permanent, so destroying the analysis takes
  an explicit act. Full Lab tab in the UI with per-tool result views.

### The exit criterion, and the bug it caught

EMACross over six months of real BTCUSDT (2025-11-01 → 2026-05-01, 15 m), four anchored
folds, a six-point fast×slow grid, submitted through the production HTTP chain and executed
by a real worker process: **28 engine runs, a stitched OOS curve of 173 163 samples**, IS
Sharpe differing per fold (−4.81/−4.14/−3.01/−2.71), the chosen parameters moving across
folds (8/26 → 20/26), and an honest verdict — **−33.8% out-of-sample, every fold flagged
`neighbourhood_unprofitable`, WFE and plateau refusing to produce numbers** that would have
dressed a strategy with no edge in this period as one with a measurable efficiency.

The first attempt at this run is why the criterion is worth having. Under the default risk
limits the strategy tripped `max_daily_loss` two days into the range, and because anchored
folds share their start, **all four IS sweeps optimised over the same two-day stub while
the fold table read like 60-to-150-day windows** — bit-identical grid results across folds
were the tell, and the default-parameter grid point reproduced the base run's Sharpe to the
last digit. Not a windowing bug (proven by re-running two windows with risk off: −3.83 vs
−3.16), but a silence bug: the halt was recorded three clicks deep in the grid JSON and
nowhere the reader looks. Fold records now carry `is_halted`/`is_halt_limit`, every halted
evaluation warns, an all-folds-halted walk-forward states that it measured the risk limits
rather than the parameters, and the UI badges both IS and OOS halts. A regression test
constructs the scenario deliberately.

### Mutation testing — 18 deliberate breakages, 18 killed (2 after closing real holes)

Same discipline as Phases 5–7, anchors verified to resolve exactly once before any run.
First pass: **16 of 18 killed**. The two survivors were both genuine test holes, both
suspected in advance and included to find out:

| Tag | The wrong implementation the suite could not see | What now catches it |
|---|---|---|
| `M13` | `sharpe_ratio` switched to the population variance (`ddof=0`), flattering every small sample. | Every test that touched it had used the function as its own oracle. `test_lab_stats.py` now states the number by hand: returns (0.01, 0.02, 0.03) have a Sharpe of exactly `2·√A`. |
| `M14` | Regime labels made effective at the daily bar's *open* — a one-day look-ahead that no prefix-property test can see, because appending future days still relabels nothing. | A mid-day probe: an instant inside the violent day must still wear the calm label its last *closed* bar earned, for both the volatility and trend series. |

The killed sixteen include the anti-overfitting objective poisoned by undefined
neighbours, anchored folds silently behaving as rolling, stitching that stops compounding,
the WFE sign-flip guard, drawdown measured against the opening instead of the running
peak, breach-on-equality, single-return blocks destroying the autocorrelation the block
bootstrap exists to preserve, the funding dead band collapsing, cascade windows of zero
length, zero-variance correlations reporting 0.0 as if independence were a finding, and
portfolio correlations over PnL levels instead of changes.

### What is not done

- **PBO via CSCV** — spec 9.4 marks it optional and says to implement it only after the
  rest is solid. Deferred deliberately, and recorded here so it is a decision rather than
  an omission.
- **Portfolio backtests over real multi-symbol data** — the engine path, the per-symbol
  artefact and the report are built and tested against synthetic two-symbol lakes; the
  real lake holds only BTCUSDT until Phase 11 adds ETH and others.
- **Everything Phase 8 defers** — unchanged from the Phase 7 list above.

## Phase 10 — an end-to-end pass with no dead ends ✅

The audit was the point: Phases 3–9 each built the UI their own feature needed, so the
question was what a person clicking every control actually hits. Three tabs in the chrome
were disabled placeholders and one was a third of itself.

### What was missing, and what closed it

- **Dashboard** (`Dashboard.tsx`) — was a disabled tab. Now the "is real money at risk,
  what ran recently, is the data fresh" screen: active sessions with live PnL and the
  armed-kill-switch banner, last five runs, recent Lab jobs, per-dataset freshness with
  the tick datasets' live edge called out as the L2 backtest window. Composed entirely
  from existing endpoints — a dashboard with its own aggregation API would be a second
  set of numbers to disagree with the tabs it links to. Everything is a link into the
  tab that owns it.
- **Settings** (`routers/settings.py`, `Settings.tsx`) — was a disabled tab, and the
  spec-7 defaults were hard-coded in a React component. Now one atomically-written JSON
  file with a typed API. `extra="forbid"` on the model: a misspelled key is a 422 naming
  it, because a setting that silently does nothing is the risk-limit failure mode wearing
  a preferences dialog. Decimal strings are parsed at PUT time so a fee of `"0.00o2"` is
  a 400 now rather than a crash weeks later. Settings are *defaults* the run form starts
  from — the stored run spec still records what each run actually used (spec 12.1).
- **Data & Feed's missing two-thirds** (`DataTab.tsx`) — spec 10.3 specifies Coverage,
  Exchange Connection and Feed; only the Feed existed, and `App.tsx` carried a comment
  admitting it. Coverage now shows per-dataset ranges and row counts against the real
  lake (3.46 M klines back to 2019, 116 M `bookTicker` rows, `depth20` from the day the
  collector started) with the runnable range stated as the bars∩marks intersection and a
  note that tick coverage decides *tier*, not whether a run is possible.
  **Exchange Connection is the gap that mattered**: there had been nowhere in the
  application to enter an API key at all, which is the first thing Phase 8 needs. The
  inputs are `type="password"` with `autoComplete="off"` — the secret must not be
  shoulder-readable and must not reach the browser's saved-password store, which is on
  disk, the one place spec 11 forbids — and both fields are cleared the instant the
  server has them. Spec 11's promise is printed on the form, not in documentation.
- **⌘K command palette** (`Palette.tsx`) — jump to any strategy, run, or tab; start a
  backtest. The kill switch deliberately has **no entry and no shortcut**, per spec 10.4,
  so muscle memory can never fire it.
- **Exports** (`lib/export.ts`, `Export.tsx`) — every chart has a PNG button and every
  table a CSV button. PNG is the interesting one: a serialised SVG has no CSS custom
  properties, so every `var(--pos)` would render black on transparent; the exporter
  resolves the theme's variables to literals against the live document, fills the
  background and renders at 2×. Verified in the browser rather than assumed — the
  serialised markup contains no unresolved `var(`, the image loads, and the canvas has
  ink rather than being a blank rectangle.
- **Esc closes the New Run dialog** — the last modal that did not, bound only while open.

### The end-to-end pass

Driven through the real application against the real lake and database. All six tabs
enabled and rendering; ⌘K opened, filtered to a run, Enter navigated to it and closed;
the Runs page showed two PNG buttons, the runs-table CSV, the trades CSV and Send to
Lab; the Lab compared two runs over one range (aligned curves, correlation matrix, naive
blend) and **refused a mismatched pair with the server's own message naming both
ranges**; Data & Feed rendered all three panels with live coverage. One cosmetic defect
found and fixed on the way — a heading rendering the literal `{daily/hourly}`.

The comparison also made the Phase 9 halt-visibility fix legible from the UI alone: run
#13 (risk limits on) made **3 round trips** before halting, run #14 (same strategy and
range, limits off) made **294**. Two runs of one strategy differing by two orders of
magnitude in activity is exactly the thing that must not be invisible.

### What is not done

- **Alert webhooks** (Telegram/Discord) — listed in spec 10.3's Settings and not built.
  Nothing else depends on them, and a webhook that has never delivered a message is not
  a feature. Deferred deliberately.
- **Retention policy and data-directory controls** — the retention machinery exists in
  the collector; exposing it as a Settings control is deferred with the same argument.
- **Coverage's gap-count column and the Pull/Verify buttons** — the gap report and the
  ingest CLI both exist; wiring them into the panel is queued rather than claimed.

## Phase 11 — cross-asset macro data, optional by design ✅

Additive only: no change to the engine's accounting, execution or risk paths. Three
sources, on the existing collector pattern.

### The endpoints were verified before anything was built on them

Review finding R1 exists because a phase was once planned on an assumption about an
endpoint nobody had called. So the first act of this phase was calling them:

| source | result |
|---|---|
| CoinGecko `/api/v3/global` | **200, keyless.** BTC dominance, total market cap, `updated_at` |
| Yahoo chart `DX-Y.NYB` | **200, keyless.** The ICE US Dollar Index itself, with `regularMarketTime` |
| Yahoo chart `DX=F` | 404 — the front-month future is not available here |
| Stooq (three symbol spellings) | 404 / JavaScript challenge |
| Frankfurter | 200, but it serves FX rates, not DXY |

That last row decided the design. A dollar index *rebuilt from spot FX rates* would have
been a number this platform invented and then labelled DXY, which is precisely the quiet
wrongness spec 1.4 forbids. If Yahoo disappears, the honest move is a different provider
recorded in the `source` column — never a computed substitute.

### Three properties that decide whether the data can be trusted

- **The source's own timestamp is the key, never poll time.** Keying on poll time
  manufactures a fresh observation every hour out of a snapshot that has not moved, and a
  strategy could not then tell a real move from a re-poll. Our clock goes in `recv_ms`.
- **A repeated source timestamp is dropped.** DXY does not print at the weekend; hourly
  polling across it would otherwise write a flat series that the gap detector reads as
  coverage and a `count(*)` reads as activity.
- **Nothing is interpolated.** Yahoo's own daily close array was observed to contain a
  literal `null` mid-series — the case the rule exists for.

### `ctx.macro()` and the no-look-ahead contract

Modelled on the existing open-interest path, so causality is enforced by the same shape:
readings are loaded per run, and `_consume_macro(bar_close)` advances a cursor that never
passes the bar's close. A reading the platform had not yet received when the bar closed is
invisible until the bar after it arrives.

**That cursor originally compared the wrong clock, and the recheck is what found it.** See
"The recheck found a look-ahead the build could not have seen" below.

`ctx.macro()` returns a **`MacroView` carrying `age_ms`**, not a bare float. Last-observation-
carried-forward is the only honest read of a level series (spec 3.4), but LOCF without an
age silently presents Friday's dollar as Sunday's — so the age is handed to the caller to
judge rather than thresholded here.

Two absences that are different facts, and are answered differently: a strategy that never
declared a macro dataset gets a **raise** naming the one-line fix; a strategy that declared
one over a lake holding no rows gets `None`, plus a `MACRO_MISSING` flag and a warning on
the run. Optional means the run proceeds, not that the absence is silent.

### The recheck found a look-ahead the build could not have seen

Phase 11 was signed off on 2026-08-02 with one row in each macro dataset, which is all a
freshly-started poller can produce. The agreed plan was to recheck it once time had passed.
On 2026-08-03 a second poll landed, and the second row is what made the defect visible.

Every macro row carries two clocks:

| | Meaning | Source |
|---|---|---|
| `ts_ms` | the instant the value *holds for* | CoinGecko `updated_at`, Yahoo `regularMarketTime` |
| `recv_ms` | the instant *we had it in hand* | our poller's local clock |

With one row per dataset they are just two numbers. With two rows the lag is measurable and
consistent:

```
macroGlobal   ts=1785777621000  recv=1785777823518  lag=202.5s
macroGlobal   ts=1785779423000  recv=1785779644194  lag=221.2s
macroFx       ts=1785777222000  recv=1785777823855  lag=601.9s
macroFx       ts=1785779043000  recv=1785779644536  lag=601.5s
```

CoinGecko runs ~3.4 min behind, Yahoo's DXY ~10.0 min — and those are the *floor*, measured
across a 30-minute gap. The documented default poll is hourly, so the true bound on the lag
is the provider's publication delay **plus the poll interval**: up to about an hour.

`load_macro` windowed and ordered on `ts_ms`, and `_consume_macro` compared `ts_ms` to the
bar close. Its docstring called that comparison "the no-look-ahead guarantee for macro". It
was the opposite: a strategy saw every macro reading between three minutes and an hour
before the platform could possibly have known it. The error is silent, systematic, and
always in the favourable direction.

**Why the storage rule was still right.** Keying *storage* on the source's stamp is correct
and the module argues it well — it is what lets a re-poll of an unchanged snapshot collapse
to one observation instead of manufacturing a fresh hourly reading. The mistake was reusing
that key for a second, unrelated question. They are genuinely different:

- *When may a strategy see this?* → `recv_ms`. Availability.
- *How stale is it?* → `ts_ms`. Age.

**The fix** windows, orders, and gates on `COALESCE(recv_ms, ts_ms)`, and carries `ts_ms`
alongside so `MacroView.age_ms` still reports the value's real age. A reading that published
40 minutes ago and reached us 10 minutes ago is now invisible for its first 10 minutes and
then reported as 40 minutes old — both answers right, from the clock that answers each. The
`COALESCE` covers a backfilled row with no local receive time, matching how the bulk
archives already leave `recv_ms` null on every other dataset.

**This would have shown up as a live-vs-backtest divergence, not as a crash.** A live
session runs the same engine but can only consume rows physically written to the lake, so
live was never optimistic — only the backtest was. Phase 7's parity report is the machinery
that would eventually have caught it, months later and attributed to something else.

**Why the existing tests passed.** `test_ctx_macro.py` built every fixture row with
`recv_ms = ts_ms`, which makes the two clocks indistinguishable and lets either rule pass.
The centrepiece test named itself "the no-look-ahead contract for macro" and could not fail
under the defect. Three tests now separate the clocks: one where the provider stamps 01:00
and the poller receives at 03:00 (nothing visible in between), one asserting the age is
measured from publication rather than receipt, and one for the null-`recv_ms` fallback.
Reverting the comparison to `ts_ms` fails the first two — verified, not assumed.

**Checked for the same bug class elsewhere, and it is not there.** Open interest is the one
other series consumed by a cursor of this shape, and `metrics` has no `recv_ms` column at
all: it is a bulk-only dataset whose `create_time` is Binance's own snapshot instant, with
no poller sitting between the exchange and the row. There is no second clock to confuse, so
gating it on `create_time` is correct — recorded here so nobody later "fixes" open interest
by analogy with macro and introduces a lag that does not exist.

The general lesson is the one Phase 7 already paid for once: **a fixture that collapses two
distinct fields into one value cannot test the distinction between them.** Both times the
test was well-named and well-intentioned and tested nothing.

### Two platform guards caught real mistakes mid-build

Worth recording because both are guards written in earlier phases, doing exactly their job:

- **`to_scaled` refused the market cap.** 2.27e12 USD at the 10⁸ price scale is 2.3e20 —
  twenty-five times past int64. The money layer raised rather than wrapping. USD aggregates
  are therefore stored in **whole dollars**, which loses nothing: CoinGecko serves this as a
  JSON float, so the digits below the twelfth significant figure were never data.
- **`query.py`'s column-classification totality check refused the schema.** Every numeric
  lake column must be declared raw or scaled at import. My first attempt stored the
  aggregates at a private cent scale, which `SCALED_COLUMNS` ("divide by 10⁸") would have
  unscaled **10⁶× wrong**. The guard failed the import before a single row was written.

### Cross-coin needed no new code

Spec 11 said the third source is "the already-existing Binance pipeline — just add more
symbols", and that turned out to be literally true: `perplab ingest --symbol ETHUSDT`
pulled 11,520 real 1-minute bars, and BTC/ETH minute returns over that week correlate at
**0.895**. The work here was verification, not implementation, and it is recorded as such
rather than dressed up as a feature.

### Exit criterion

- **All three sources collecting** — verified against live endpoints; `perplab macro --once`
  wrote real `macroGlobal` and `macroFx` rows into the production lake, month-partitioned
  and symbolless, and the unscaled view renders dominance as a fraction while leaving
  whole-dollar market cap undivided.
- **Queryable via `ctx` in a test strategy** — `tests/unit/test_ctx_macro.py` runs real
  engine backtests against a lake seeded with macro rows and asserts the values, the ages,
  the LOCF selection, the flag, the raise, and determinism.
- **No look-ahead and gap handling verified** — the centrepiece test publishes one reading
  30 seconds *inside* a bar and asserts every earlier bar saw nothing. **Amended on the
  recheck**: that test alone was not sufficient, because its fixture set `recv_ms = ts_ms`.
  Three further tests separate publication time from receipt time; see above.

### What is not done

- **A real-lake backtest reading real macro rows.** The macro series began accumulating on
  2026-08-02; the kline lake ends 2026-07-31. There is no overlapping range yet, exactly as
  `depth20` had none on the day the collector started (Phase 1b). Until then the causality
  and gap behaviour are verified against seeded lakes — which is the only way to test a
  specific boundary anyway — and the collectors against the real endpoints.
- **Historical macro backfill.** Yahoo's chart endpoint carries a daily close array that
  could backfill DXY, and it is the array with the observed `null`; any backfill must skip
  those rather than fill them. Not attempted here.
- **`macroFx` beyond DXY.** The schema carries a `series` column and is ready; only DXY is
  collected.

## Adversarial review of Phases 9, 10 and 11

Three independent reviewers over the Lab's statistics, the backend plumbing, and the
frontend. They found **thirty-eight issues**; the ones below were verified against the
code and fixed. This section exists because the failure mode being hunted is not "a
crash" — it is a number that looks right, or a claim in a docstring the code does not
honour, and several of these were exactly that.

### The four that were the platform lying

| What it claimed | What was true |
|---|---|
| `prob_ruin: null` on both compounding methods, *"a compounded path multiplies by `1 + r > 0` factors and cannot cross zero"* | `_multiplicative_path_stats` floors at `max(0.0, 1.0 + r)`, and `build_grid` emits a return at or below −100% whenever a run's equity crosses zero inside a grid step. On a blown-up run **100% of `random_start` paths ended at zero** and the panel withheld the ruin probability, citing a guarantee the code did not have. Now measured, and a ruined path's Sharpe stops at the return that killed it. |
| The regime table's win rate | `wins / (wins + losses)`, dropping scratches, against `analytics.metrics`' `wins / closed`. Ten wins, ten losses, ten scratches: **run page 0.333, regime bucket 0.500**, same trades. Now the metrics convention, with a `trade_scratches` column beside it. |
| The armed **kill switch badge** in the chrome and on the Dashboard | `GET /kill` returns `{kill: null | trip}` and the client read `.armed` off the envelope — `undefined` forever. The one safety interlock in the platform **had never rendered**, in either place. The client now unwraps at the boundary; `flatten` also corrected to the server's `flattened`. |
| Settings: *"they apply as defaults to the next run form you open"* | Nothing read the file. Worst case: an operator setting **Halt behaviour → close-all** and believing the kill switch would flatten. The run form now genuinely seeds from `/settings`, and the kill dialog reads `kill_switch_flatten` — which is where its own docstring always said the behaviour lived. |

### The kill-switch UI surface had never worked, in any of its three parts

Pulling the first thread found the rest. All three hid behind a green `tsc` run, because
none of them is visible to TypeScript — they are wire-shape mismatches:

1. **The armed badge could not render.** `GET /kill` answers `{kill: null | trip}`; both
   the chrome and the Dashboard read `.armed` off the envelope, which is `undefined`
   forever.
2. **The fire button was permanently disabled.** The confirm dialog derived its behaviour
   from `kill.flatten` — a field that does not exist (the trip records `flattened`) and
   which is absent anyway whenever nothing is armed, i.e. every time an operator opens the
   dialog to fire it. It now reads Settings, so the button works *and* the Settings claim
   became true.
3. **The un-arm button did nothing and said it worked.** It POSTed no body, so FastAPI
   refused it 422, and the response was parsed as `{kill}` when the endpoint answers
   `{cleared}` — so the client reported "un-armed" regardless. Spec 7.6's *"explicit
   un-arm before any live session can start again"* had no working path.

Verified end to end against the running platform rather than argued: the switch was fired
with no sessions and no keys connected (so it stopped nothing and wiped nothing), the
badge was observed rendering `KILL ARMED` with the button enabled, the un-arm button was
clicked, `GET /kill` returned `null`, the badge cleared, and the audit row retained
`cleared_by: operator` for the incident history spec 7.6 requires. The platform was left
exactly as found.

One note on process: the first check of the badge fix showed it *still* broken, and the
cause was a stale bundle rather than a bad fix — worth recording because "verify, then
believe the verification" cuts both ways.

### The rest

- **`LabStore` shared one SQLite connection and one handle map across FastAPI's thread
  pool with no lock**, while `db.py`'s own docstring asserts the connection *is* lock-
  guarded. Two demonstrated consequences: a `KeyError` 500 on a routine `GET /lab/jobs`,
  and `with self._connection:` publishing another thread's uncommitted writes so a
  rollback silently did not roll back. One `RLock` now guards both.
- **A `lost` Lab job was unrecoverable and permanently blocked deleting its source run** —
  delete refused ("cancel it first"), cancel was a no-op for terminal statuses, and the run
  refused ("delete those Lab jobs first"): three refusals pointing at each other, reachable
  by restarting the API server mid-job. Lab jobs are now deletable from `lost`, with the
  argument written down for why a run is not.
- **Any out-of-range integer id returned 500** across seven endpoints (SQLite binds int64;
  FastAPI's `int` is arbitrary-precision). Now 404.
- **Walk-forward configs were not validated at submit**, contradicting the router
  docstring's own example. `is_ms=1, oos_ms=1` over a year passed every check and then
  asked `build_folds` for ~3×10¹⁰ folds — an uncancellable OOM from a request that
  returned 201. Windows, step, objective, worker count and fold count are now checked
  before a row exists.
- **`GET /settings` 500'd on any file the model would not accept**, including a stray
  forward-compatible key — turning the tab the docstring invites you to hand-edit into a
  dead end. It now falls back to defaults and reports the problem.
- **The WFE aggregate compared mismatched fold sets** while its comment claimed it did
  not; **`uncovered_tail_ms` hid interior gaps** (300 ms unevaluated reported as 100) and
  is now `uncovered_ms`; **portfolio `final_pnl` dropped the trailing partial period**,
  the one column a reader reconciles against the ledger.
- **The sensitivity heatmap shaded with `--pos`** on a *relative* scale, so an all-losing
  grid still painted its least-bad cell green — under a comment claiming monochrome.
- **Lab Delete had no confirmation** (spec 10.4), sitting where Stop had been a moment
  earlier. **Failed requests rendered as "you have none"** on the Dashboard and the Lab
  list. **Signed numbers had no arrow glyph** (spec 10.1).
- Two Phase 11 defects were caught by *platform guards*, not by review: `to_scaled`
  refusing a market cap 25× past int64, and `query.py`'s column-classification totality
  check refusing a schema whose USD columns would have been unscaled 10⁶× wrong. A third,
  `test_money.py`'s decimal-seam guard, refused `macro.py`'s `decimal` import — resolved
  by parsing with `parse_float=str` and feeding the transmitted digits to the platform's
  own `to_scaled`, which is strictly better than what it replaced.

`tests/unit/test_phase9_review_fixes.py` holds a regression for each, grouped by the claim
the defect made falsely. Every one fails against the code as it was written.

### Not fixed, recorded

- Cancellation is honoured by the walk-forward only; Monte Carlo and regimes run to
  completion. Documented at both the store and the worker rather than implied away.
- Spec 9.5's portfolio report has an API client and types but no UI panel.
- Spec 10.3's Coverage panel lacks size-on-disk, gap counts and the Pull/Verify buttons.
- The `random_start` block bootstrap under-samples series endpoints (non-circular blocks);
  now disclosed in the method's own notes rather than left to be deduced.

## Account configuration — margin mode, multi-asset, leverage

Four decisions taken deliberately, ahead of Phase 8. Two of them were stated as
assumptions to confirm, and **both assumptions turned out to be wrong** — recorded here
because in each case the wrongness was invisible from the outside.

### Leverage was configured everywhere except the exchange

`SignedRestClient.set_leverage` was implemented, documented as idempotent, covered by
tests — and had **no callers anywhere in the platform**. Leverage was a per-strategy
setting, carried through `RunSpec`, and applied to the ledger at
`BacktestEngine.start` → `Account.set_leverage`. Nothing ever sent it to Binance.

The consequence is the dangerous kind of quiet. A run configured at 5x would have opened a
position against an account still on whatever leverage was last set by hand — Binance's
default is 20x. The position would be four times the intended size, and the liquidation
price on the Live Monitor would have been computed from the 5x the ledger believed in.
Every number on screen would have looked reasonable. The real liquidation would have
arrived first.

`live/preflight.py` now sets margin type and leverage per symbol before any order, and
three things about it are deliberate:

- **The leverage echo is verified, not assumed.** `POST /fapi/v1/leverage` answers with the
  leverage it applied, and Binance bounds leverage by the symbol's bracket table. A request
  for 20x that returns 10x is a successful HTTP call and a silently wrong account. A
  mismatch aborts rather than adopting the exchange's number — adopting it would change the
  run's configuration out from under the strategy, and a walk-forward whose leverage moved
  mid-study is not comparable with itself.
- **`-4046` is treated as success.** Binance reports "No need to change margin type" as an
  *error* when the symbol already has the mode requested. Treating it as a failure would
  make a correctly configured account the only kind that cannot trade.
- **It cannot be skipped.** `ExchangeTransport` — the only thing in the platform that sends
  an order to a real venue — takes the `PreflightReport` as a required argument and refuses
  construction unless it covers every symbol the engine can trade, at the leverage the
  ledger is using. A caller who skips the preflight has nothing to pass. This is the guard
  that makes the original defect unrepeatable rather than merely fixed.

The preflight also refuses a **hedge-mode account** up front. `reconcile._one_position`
already caught that, but only on the first reconciliation pass — up to sixty seconds after
the first order. Moving it to preflight puts the failure before any exposure exists.

### Margin mode: the assumption that it needs no accounting changes is wrong

The proposal was that isolated vs cross "only affects which balance pool backs a position,
not how PnL/liquidation is calculated for it". It affects both, and `core/account.py` had
already said so: under cross margin a symbol's liquidation price depends on the unrealised
PnL of every other open position, so **there is no closed form**. `margin.liquidation_price`
solves the isolated form against one position's own allocation and cannot express cross at
all; `isolated_margin`, `reserved_margin` and `available_balance` all change meaning too.

So `CROSSED` is exposed and **refused**, at every door — `MarginMode.parse`, the runs
route, the sessions route, and the preflight — with one shared message that explains the
arithmetic rather than complaining about the value. Three readings were available and only
one is honest:

| Option | Why not |
|---|---|
| Hide it | Someone who wants cross margin gets no answer at all |
| Accept it, compute isolated math under a cross label | The platform reports a liquidation price the exchange does not agree with, on the screen where being wrong is most expensive |
| Accept it, implement cross properly | Correct, and a larger project than hedge mode — an account-wide iterative solve |

`RunSpec.margin_mode` defaults to `ISOLATED`, and **that default is exact rather than a
guess**: the ledger has never implemented anything else, so reading an older spec as
isolated reports what that run actually did. This is the opposite of `risk_limits`, where
an absent value had to mean "no risk layer" rather than "the defaults", because those runs
genuinely had none.

### Multi-asset mode: verified off, and off structurally

Asked as a verification pass, and the result is stronger than "no path enables it":

- `grep "asset" perplab/core/*.py` returns **nothing**. There is one `wallet: Decimal` and
  no currency dimension anywhere in the ledger. Multi-assets mode is not disabled by
  configuration — it is unrepresentable.
- The live side reads only the single aggregate `totalWalletBalance`
  (`reconcile._wallet_balance`), never the per-asset `assets[]` array, and that function
  already documents why multi-assets mode would break the comparison it performs.
- `perplab/live/`, `perplab/engine/` and `perplab/core/` contain no `marginAsset`,
  `quoteAsset` or collateral handling of any kind.

**One real hole, now closed.** Nothing *enforced* USDT quoting. The USD-M venue is not a
USDT venue: `BTCUSDC` and other USDC-margined perpetuals trade on the same `fapi` endpoint,
with the same payload shape, and nothing in a symbol's name distinguishes them.
`filters.py` parsed `contractType` but never `quoteAsset`, so such a symbol would have been
ingested and its PnL added to a USDT wallet as though the currencies were interchangeable —
silently, because with no asset field there is nothing for the mismatch to disagree with.

`assert_supported_quote` now runs in `resolve_filters`, the last point before a symbol can
reach the ledger. It carries a deliberate asymmetry: a **known-wrong** asset raises, an
**unknown** one (an older snapshot taken before the field was parsed) passes. Refusing the
unknown case would make the platform's own reference history unusable to guard against a
risk it does not carry — it has only ever been pointed at USDT pairs. Refusing what can be
proven wrong while not claiming to have checked what could not be read is the honest split.

### Not done

- ~~**A session-start form.** `startSession` exists in the API client with no UI caller;
  sessions are still started by `curl`, so the margin-mode selector was added to the New
  Backtest dialog only. Pre-existing, and named here rather than left to be discovered.~~
  **Closed 2026-08-03** — `frontend/src/components/SessionDialog.tsx`, mounted in `App.tsx`
  and reached from the "Start session" button on the Runs tab. See "The Start Session
  screen" below.
- **Verification against a real account.** Every refusal above is exercised against a fake
  client that returns hand-written payloads. The `-4046`/`-4048` codes and the
  `dualSidePosition` shapes are from Binance's documentation, not from a live response.

### Two things the platform's own guards caught during this work

Worth recording because both were the existing machinery doing exactly its job:

- **`test_money.py::test_decimal_is_confined_to_the_accounting_seam`** failed the moment
  `PositionSide` gained a helper taking a `Decimal`. `core/types.py` is imported by the
  collector and the whole data path, so that import would have dragged the ledger's numeric
  type across the seam spec 3.1 draws. The helper is now `opening_sign -> int` and needs no
  `Decimal` at all — a better primitive, arrived at by being refused the obvious one.
- **A three-test failure that was not a defect.** A full-suite run was launched in the
  background and then source was edited while it ran, so it imported a half-written
  `account.py` and reported three `test_validate.py` failures. Re-run against the settled
  tree: green. Recorded because the first instinct on seeing three failures is to go
  looking for the bug, and the actual lesson is procedural — do not edit the tree under a
  running suite.

---

## Hedge mode, shared leverage and concurrent sessions ✅

Four pieces of work, done together because they are one change: a symbol can now hold two
positions, and more than one strategy can be trading at once, and those two facts collide at
exactly one point — the exchange settings that are shared per symbol.

**Suite: 2244 green** (2185 → 2244). **Mutation testing: 29/29 caught**, plus one mutation
tried and found equivalent, recorded rather than deleted.
`scripts/mutate_hedge_and_concurrency.py` is the harness; re-run it after any change to
`core/account.py`, `core/risk.py`, `analytics/trades.py` or `store/claims.py`.

### Hedge mode — finished end to end

Positions are keyed `(symbol, PositionSide)`. The two legs of a hedge are independent in
every number that matters: each carries its own entry price, so each has its own unrealised
PnL; each posts its own isolated margin, so each has its own liquidation price; each pays or
receives funding on its own quantity, against its own allocation.

The items scoped as outstanding in the previous sign-off, and where they stand now:

| Was outstanding | Now |
|---|---|
| `positions` / `_signed_fills` keyed by symbol | keyed `(symbol, PositionSide)` |
| **I3 broken under hedge** | restated per side — see below |
| `apply_fill` has no `position_side` | required in hedge mode, refused if absent |
| case C (flip) unhandled | `HedgeFlipRefused`, matching the exchange |
| `analytics.trades` flat-to-flat per symbol | per side; `Trade.position_side` added |
| `projected_exposure` one signed quantity | `gross_projected_exposure`, sum of both sides |
| `LiveMonitor` duplicate React keys | keyed `(symbol, side, index)`, one row per leg |
| no dual-position goldens | 22 hand-derived, in `tests/golden/test_hedge_mode.py` |

**I3 is restated, not relaxed.** Spec 3.10 states `Q == Σ(signed fills)` because spec 3
describes a one-way account, where a symbol *is* a position. In hedge mode a buy routed to
the short side reduces it, so the sum of a symbol's fills equals **neither** leg's quantity —
a per-symbol I3 would fire on a correct account, and an invariant that fires on correct code
is one somebody switches off. Asserted per `(symbol, side)` it is exactly the spec's own
claim: this position holds what was filled into it. `check_position_sum` is unchanged; only
what "this position" addresses moved.

**Spec 3.3's case C does not exist in hedge mode.** A sell of 3 against a long of 1 closes
the long and stops; it does not open a short of 2. That is the exchange's own behaviour — a
`SELL` carrying `positionSide=LONG` is a closing order — and it is refused rather than
accommodated, because "sell 3" might have meant "close it" or "close it and go short 2", and
filling the smaller reading reports a strategy doing something it did not ask for.

**Nothing infers a side.** `apply_fill`, `ctx.buy`, `ctx.sell` and `ctx.close` all require an
explicit `position_side` in a hedge run and refuse without one. A `SELL` means "reduce the
long" or "open the short" depending on that field alone, and those two orders leave the
account in states that differ by the entire position. Binance treats it the same way:
`positionSide` is mandatory on a hedge-mode order. `ctx.positions()` returns both legs and
`ctx.close_all()` is the explicit form of "flatten this symbol", so `ctx.position()` never
has to guess.

### The goldens, and what they are built to catch

Spec 3.9's worked example is inherently one-way, so every figure was derived from the spec's
*formulas* and written into the test docstrings before the code ran. The long leg is
deliberately identical to spec 3.9's own position — 0.1 BTC at 50 000 on 10x — so it must
reproduce the published `P_liq = 45 180.72` exactly. That is the anchor: it proves the hedge
machinery does not perturb a position the spec already pinned.

Three of them are worth naming:

- **One symbol, two liquidation prices, straddling the mark.** Long 0.1 @ 50 000 solves to
  45 180.72; short 0.05 @ 52 000 solves to 56 972.11. I7 holds on each leg separately. There
  is no single price at which this symbol liquidates, and any build reporting one is
  reporting a number that does not exist.
- **One funding settlement moves the two legs in opposite directions.** At +0.0001 on a mark
  of 51 000 the long pays 0.510 and the short receives 0.255; the long's liquidation climbs
  5.12 *toward* the mark and the short's climbs 5.08 *away* from it. A build that settled on
  the net position, or charged both legs the same sign, produces neither number — and one
  that charged funding only to the wallet leaves both liquidation prices unmoved, which is
  the defect `Position.funding_paid` exists to prevent (spec 6.2 R5).
- **A hedge with no safe mark.** Long 1 @ 50 000 and short 1 @ 40 000 on 10x trigger at
  45 180.72 and 43 824.70 respectively — the short's *below* the long's — so under one the
  long is gone, over the other the short is, and in between both are. Netting reports this as
  flat: long 1, short 1, net zero, no risk. It is an account that cannot survive any price at
  all, and it is the sharpest available argument for `gross_qty` as the exposure rule.

### Exposure is the sum of both sides

The agreed reading, and the conservative one. Both legs post their own margin and either can
be liquidated while the other survives, so the quantity a bad tick can destroy is
`|Q_long| + |Q_short|`. Netting is the trap: a long 10 against a short 10 nets to zero, so a
netted `max_position_notional` imposes no ceiling at all on a market-neutral book — and a
market-neutral book stops being neutral the moment one leg is liquidated, at which point the
survivor is a naked 10 the limit never saw.

`side_projected_exposure` is one term rather than two for a hedged side, and the missing term
is a fact rather than a shortcut: a hedge side cannot cross zero, so the shrinking direction
can only reduce it. Keeping the one-way `max(|q + buys|, |q − sells|)` would report a long of
0.1 with a resting sell of 0.5 as an exposure of 0.4 — a *short* of 0.4 that hedge mode makes
unreachable — and would refuse the exit a strategy needs most.

### Shared leverage — the constraint, enforced

> **Superseded 2026-08-04 — see "Symbol ownership" below.** This section describes the
> narrower rule that shipped first: a symbol was shareable as long as both sessions agreed
> about leverage, margin mode and position mode. It is kept because the reasoning under it is
> still correct and still load-bearing; what changed is that agreeing about the settings turns
> out not to be sufficient.

**Leverage on Binance USDⓈ-M is account state scoped to a symbol.** `POST /fapi/v1/leverage`
takes `symbol` and `leverage` and nothing else: no strategy, no sub-account, no
`positionSide`. Two strategies trading BTCUSDT concurrently share one leverage whether or not
either knows it, and so do the two legs of a hedge. There is nothing to design around — the
only question was whether the platform enforces it or discovers it.

Discovering it is silent and one-directional. Session B's preflight reconfigures the symbol
under session A; A's transport preflight check ran once at construction and never runs again;
A keeps sizing positions and solving `P_liq` at a leverage the venue stopped using, with the
liquidation price on its monitor sitting *further* from the mark than the real one. Both
sessions report plausible numbers.

So `store/claims.py` holds a claim per `(run, symbol)` in SQLite, and a second session asking
for a different leverage, margin mode or position mode on a running symbol is **refused with
the number in force and the run holding it**. Refusing is the design: adopting the running
leverage silently would change a run's configuration out from under the strategy that asked
for it. Taken at the API before the worker spawns, re-taken by the worker (a check only in
the caller is a check another caller can skip), released in the worker's `finally` on the
crash path as well as the clean one, and pruned against the live run set on every read — a
stale claim that blocked a symbol forever would be a safety mechanism an operator learns to
delete.

`GET /api/symbol-claims` feeds the Start Session form, so the leverage in force is visible
*before* submitting rather than after being refused.

### Symbol ownership — one strategy per symbol, per account ✅

**Decided 2026-08-04: one strategy per Binance account going forward**, and the claim
tightened from "no disagreeing about a symbol" to "no sharing a symbol at all".

The rule above had a hole, and the hole was the permissive branch: two sessions that agreed
about leverage, margin mode and position mode were allowed to run on one symbol. The
agreement was never what mattered. **Binance holds one position per `(symbol, positionSide)`
for the whole account** — no strategy field on an order, no per-strategy position, nothing in
the API that could keep two strategies' BTCUSDT longs apart. Two sessions on one symbol have
their fills merged into that single position: one quantity, one blended entry price, one
margin allocation, one liquidation price. Each session's ledger goes on tracking the fills it
sent as though they were a position of its own, so both report an entry price, a PnL and a
liquidation price the account does not have, and the divergence grows with every fill the
*other* session makes.

**The alternative was considered and rejected.** Re-keying `positions` and `_signed_fills` to
`(symbol, side, strategy_id)` would give each strategy its own entry price and PnL locally —
and those numbers would be fiction the moment they touched the venue. A per-strategy
liquidation price on a merged position has no referent: the exchange liquidates the merged
position at the merged price and destroys both strategies together, so a platform reporting
two separate liquidation prices would be reporting two prices that will never trigger and
hiding the one that will. That is precisely the "quietly wrong answer" this project exists to
avoid, and no amount of local bookkeeping recovers an attribution the exchange never had.

So the claim is now **exclusive**: any overlap with a running session on the same endpoint is
refused, matching configuration included. The refusal says so explicitly rather than listing
an empty set of differences — an operator who matched the running session's settings on
purpose has to be told that was not the problem, or the next thing they try is matching them
harder. It also names the way out: different symbol, stop the running session, or a separate
Binance account.

**Side is not the escape hatch it looks like.** In hedge mode the venue really does keep
`LONG` and `SHORT` apart, so two sessions confined to opposite legs would not merge. It is not
allowed anyway, because **nothing in a session declares a side**: `StartSessionRequest`
carries symbols, leverage, margin mode and position mode, and a strategy is free to buy or
sell at any tick. Disjointness would be a promise rather than a checkable fact, and the
failure when it broke would be a silently merged position — the thing being prevented.
`test_hedge_mode_does_not_make_a_symbol_shareable` records the decision.

Everything else about the claim is unchanged: taken at the API before the worker spawns,
re-taken by the worker, released in its `finally` on the crash path, pruned against the live
run set on every read, `BEGIN IMMEDIATE` across the read and the write. `SymbolClaims` and its
table did not change shape; `LeverageConflict` was renamed `SymbolConflict`, because an
identical configuration now collides just as hard as a different one and a traceback naming
leverage would be pointing at the wrong thing.

**Verification.** 4 new tests in `tests/unit/test_symbol_claims.py`, two existing ones
inverted (`test_a_second_run_may_join_at_the_same_configuration` and its API twin asserted
exactly the behaviour now refused). The mutation harness gained the regression as a named
mutation — *"a symbol is shareable when both sessions agree about the configuration"*, which
restores the old predicate verbatim — plus one for endpoint scoping; the obsolete
*"only leverage is compared"* mutation was removed with the code it targeted.
**Concurrency group: 6/6 caught.**

### Concurrent sessions

**The platform already ran N sessions at once** — one subprocess each, per-run directories,
per-run `RiskEngine`, WAL-mode SQLite — and nothing capped it. What was missing was safety at
two seams, both now closed:

- **`RunStore` held a `check_same_thread=False` connection with no lock**, while every route
  is a plain `def` that FastAPI serves from a thread pool. `LabStore` and `StrategyLibrary`
  both document having fixed exactly this; `RunStore` never had. `_reap` mutates two dicts on
  every `get` and every `list`, which the Live Monitor polls once a second per session, so
  two polls landing on the same just-exited worker turned a routine monitor request into a
  `KeyError` and a 500.
- **`SymbolClaims.claim` is a read-then-write**, and its readers are in different
  *connections* — the API opens one per request, each worker opens its own — so an instance
  lock protects nothing between them. It takes `BEGIN IMMEDIATE`, the same write lock
  `KillSwitchStore.arm` takes and for the same reason. Found by a test that **hung rather
  than failed**, which is its own lesson: the test now opens both stores before starting the
  threads and gives the barrier a timeout, because a thread that fails before a barrier
  leaves the other blocked forever.

**Risk limits are per session and stay that way**, as asked. That is a decision with a
consequence worth stating rather than leaving implicit: nothing sums two sessions' exposure,
so two strategies each under a 5x cap are two accounts-worth of exposure on one real account.
`test_risk_limits_are_not_pooled_across_sessions` pins it, and its docstring names what would
have to be decided if pooling were ever wanted.

### The Start Session screen

There was no way to start a session from the app — only `curl` — and the margin-mode selector
existed solely in the New Backtest dialog, so the one mode that touches a real exchange was
the one with no UI. `frontend/src/components/SessionDialog.tsx` configures strategy, symbols,
timeframe, venue, balance, leverage, margin mode, **position mode**, fill tier, runtime cap
and the full spec 7 risk table, and renders the symbol's current shared leverage inline.

### Three defects found on the way, none of them in scope

- **`liq_distance_pct` was a percent where everything reading it expected a fraction.** A
  position 5% from liquidation arrived as `5.0`, rendered as "500.00%", and never crossed the
  `< 0.05` threshold that turns the proximity bar red. The warning on the single most
  dangerous number on the Live Monitor was silently off, and nothing failed — because nothing
  checked the unit. Fixed at the server, and pinned against the *published payload* rather
  than a recomputation: the first version of that test recomputed the field itself, which
  left the payload free to disagree with it.
- **`api.startSession` was typed `{ run_id }` and the server has always answered `{ run }`.**
  Invisible because the function had zero callers; the first one would have navigated to a
  run that did not exist. The wire-shape class again — TypeScript cannot see a JSON body, so
  the type was a memory of the endpoint rather than a reading of it.
- **A duplicate React key on the monitor's position table.** `key={position.symbol}` gave two
  hedge legs the same key, and React reconciles those as one row whose values flicker between
  them. Reachable today *without* hedge mode, by repeating a symbol in a session's own config.

### What a mutation run says the tests are worth

29 of 29 deliberate wrong implementations are caught. Eight survived the first pass and each
became a test; two of those were instructive in themselves:

- **One survivor was a fault in the harness**, not the suite: the mutation's test list did not
  include the file holding the test that catches it. A mutation harness that runs the wrong
  tests reports a false gap and sends someone to write a test that already exists.
- **One survivor is a genuine equivalent mutant.** Moving `RunStore.create`'s
  `cursor.lastrowid` read outside the transaction changes nothing, because
  `sqlite3.Cursor.lastrowid` is stamped on the cursor at `execute` time rather than read back
  from the connection — verified directly. **The justification originally written into that
  line was wrong**, and has been corrected in place rather than left standing as a
  plausible-sounding reason for a change that does not need one. It is recorded in the
  harness's `EQUIVALENT` list, because "we tried it and it is equivalent" is a different
  statement from "we did not think of it", and only one of them needs revisiting.
