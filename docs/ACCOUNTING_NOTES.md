# Accounting Core — Findings and Deliberate Deviations

**Phase 2 (spec §3).** Everything the platform ever reports about a strategy is derived from
`perplab/core/account.py`, `margin.py`, `funding.py` and `invariants.py`. This file records
the places where implementing §3 required a decision the spec did not make, or made
inconsistently — so that a future reader finds the reasoning rather than re-deriving it, or
worse, "fixing" the code back to the spec's error.

Each entry states what the spec says, what was implemented, and why. Where the spec is
wrong, that is said plainly and the test that pins the correct behaviour is named.

---

## A1 — Spec §3.3's Case C worked check contradicts its own formula

**Spec says (both, in the same block):**

```
Case C — flip:  realized = sign(Q) · |Q| · (Pf − Pe)
Check Case C:   Q=+1, Pe=50000, sell 1.5 @ 52000 → realized = +1000
```

The formula gives `+1 × 1 × (52 000 − 50 000) = +2000`. The check says `+1000`.

**Implemented:** the formula. `+2000`.

**Why.** The flip closes the *entire* 1 BTC long from 50 000 to 52 000 before opening the
0.5 residual short, so it must realise exactly what an outright close of 1 BTC would have —
2 000. The stated `+1000` is Case B's answer (that check sells only 0.5, and 0.5 × 2 000 =
1 000) carried into Case C by transcription. Both numbers are round and plausible, which is
precisely why this needed a test rather than a reading.

**Pinned by:** `tests/golden/test_fills.py::TestCaseCFlip::test_spec_flip`, and more
usefully by `test_flip_realises_exactly_the_closed_portion`, which settles the question
without appealing to either the formula or the check: selling 1.5 must realise what selling
1.0 realises, because the extra 0.5 opens a new position rather than closing an old one.

**Impact had it been coded to the stated check:** realised PnL halved on every flip in the
platform, with the missing half silently credited to the residual position's cost basis.

---

## A2 — §3.3 books funding to the wallet; §6.2/R5 requires it to reach the margin

**Spec says:**

- §3.3: `W' = W + funding_cashflow` — funding moves the *wallet*.
- §3.7: the `W` in `P_liq = (W − Q·Pe + MA) / (q·MMR − Q)` is "the isolated margin allocated
  to this position (initial margin plus any added margin), **not the whole wallet**".
- §6.2/R5 (a High-severity review finding): funding must be settled *before* the liquidation
  check because "a funding payment reduces margin balance and can itself cause liquidation.
  Checking liquidation first would let a position survive a funding payment that should have
  killed it."

Those three cannot all hold. If funding only touches the wallet, and `P_liq` only reads the
isolated allocation, then **no funding payment can ever move a liquidation price** — R5
becomes a rule about an ordering that cannot matter, and the bug it was raised against is
not fixed by the ordering it prescribes.

**Implemented:** funding is charged against both. `Position.funding_paid` accumulates the
signed cashflow and reduces `isolated_margin`, which is what `liquidation_price` receives.
The wallet arithmetic in §3.3 and §3.9 is untouched.

**Why this and not the alternatives.** It is also what Binance does — on an isolated
position, funding settles against that position's own margin. And it is the narrowest change
that makes R5 true: fees and realised PnL deliberately do **not** follow suit, because §3.9
computes `P_liq = 45 180.72` from an initial margin of exactly `500.00` on a position that
had already paid a `2.50` fee. Charging fees to the allocation as well would give
`45 205.82` and break the worked example, which is Phase 2's exit criterion.

**Consequences that had to be handled:**

- Accumulated funding can drive the allocation to zero or below. `Position.margin_exhausted`
  detects that and `check_liquidations` liquidates *without solving*, because `P_liq` at a
  zero allocation comes out **above** the entry price for a long — which would trip I7 and
  report a bracket-resolution failure for something that is not one.
- A flip resets `funding_paid` to zero. The residual is a new position on the other side,
  and letting the old long's funding history erode the new short's margin would move a
  liquidation price for a reason that no longer exists.
- A reduce keeps it whole. Funding already paid does not come back when a position is
  trimmed; this is the conservative direction (liquidation marginally closer) and avoids a
  division.

**Pinned by:** `tests/golden/test_funding.py::TestFundingBeforeLiquidationCheck`, which
demonstrates R5 by running the two orderings against the same account and showing they
disagree — `test_funding_first_kills_it` versus `test_checking_first_lets_it_survive_the_payment`.

---

## A3 — `leverageBracket` publishes JSON *numbers*, not decimal strings

**Not in the spec at all.** Every other USD-M endpoint sends prices and rates as decimal
strings precisely so clients can avoid binary floating point. `GET /fapi/v1/leverageBracket`
does not: `maintMarginRatio` arrives as a bare JSON number.

A default `json.loads` therefore produces `0.004` as an IEEE-754 double **before any PerpLab
code sees it**, and no amount of `Decimal(...)` afterwards recovers the lost bits —
`Decimal(0.004)` is `0.004000000000000000083266726...`. That value then multiplies a
six-figure notional inside every liquidation solve, and §3.10 forbids the epsilon that would
be needed to absorb the result.

**Implemented:** `margin.load_bracket_snapshot` parses with `parse_float=Decimal`, and
`brackets_from_payload` **rejects** a float rather than converting it — by the time one
arrives the precision is already gone, and accepting it would make the failure invisible.
The error message names the fix.

**Pinned by:** `tests/unit/test_margin.py::TestPayloadParsing::test_a_float_is_refused_rather_than_converted`.

**Resolved 2026-08-02.** This note previously read "still open: no real bracket snapshot
exists, because `leverageBracket` is signed and no API keys are configured". The *endpoint*
is signed; the *data* is public, and it is now snapshotted from the endpoint behind
Binance's public leverage-bracket page —
`userdata/reference/leverageBracket/2026-08-02.json`, 987 symbols. `DATA_AVAILABILITY.md`
F3 is closed.

Two things fell out of that which are worth recording here:

- **This note's own concern was validated by the real data.** The public payload publishes
  rates as bare JSON numbers including `0.0333`, which no binary float represents exactly.
  The float guard described above — written against a hypothetical — refused the first real
  payload parsed the wrong way. It is now pinned against captured production data in
  `tests/unit/test_reference.py`, not only against a constructed fixture.
- **The constructed fixtures turned out to be right.** `graduated_bracket_table` derives
  each maintenance amount from the continuity requirement rather than quoting remembered
  values, and Binance's published BTCUSDT table satisfies exactly that relation
  (`300000 × (0.005 − 0.004) = 300`; `300 + 800000 × (0.0065 − 0.005) = 1500`). A test now
  asserts the continuity property against the real tiers, so the convention is evidence
  rather than inference.

The fixtures in `tests/support.py` remain constructed on purpose: a golden test should state
the numbers it depends on. The real snapshot is what a *backtest* must price margin against,
and `load_bracket_snapshot` is the only path that reads it.

---

## A4 — The spec's worked examples assume a flat maintenance rate

§3.7's short check computes `P_liq = 55 000 / 1.004 = 54 780.88` at `MMR = 0.004`. But 1 BTC
at 54 780.88 is a 54 780.88 notional, which on any realistic BTCUSDT table has already left
the lowest tier — so the answer contradicts the tier it was computed under, which is exactly
the circularity §3.6 raises and then resolves by iteration.

Under a graduated table the same short solves to **54 776.12** in two passes (tier 2:
`MMR = 0.005`, `MA = 50`).

**Not a defect in either place** — the spec is illustrating the formula, not the tier
selection. It is recorded because it determines how the tests are built: reproducing the
spec's numbers requires `single_bracket_table`, and using a graduated table there would fail
the golden test for a correct reason. The 4.76 USDT difference is the finding worth keeping
in view: an implementation that skipped the iteration would be wrong by an amount far too
small to notice, and wrong in the optimistic direction — it reports the liquidation as
further away than it is.

**Pinned by:** `tests/golden/test_liquidation.py::TestBracketCrossing`.

---

## A4b — What the adversarial review found, and what it cost

The first working version of the accounting core passed 874 tests, reproduced §3.9 exactly,
and was wrong in seven ways. Recording them because the pattern matters more than any one
of them: **every single defect was in behaviour the spec does not supply a number for.**
The golden tests could not have caught any of them, because there was nothing to compare
against.

| # | Defect | Severity | Now pinned by |
|---|---|---|---|
| 1 | An exhausted margin allocation short-circuited straight to liquidation, skipping spec 3.7's trigger inequality. That inequality carries unrealised PnL, so a position 10 000 USDT in profit was being liquidated and its profit deleted. | **critical** | `test_funding.py::test_an_exhausted_allocation_does_not_by_itself_liquidate` |
| 2 | `invariants.py` did its arithmetic in the ambient 28-digit context while the ledger runs at 50. A perfectly correct wallet failed I1 by ~1e-23. | **critical** | `test_account_review_fixes.py::TestInvariantPrecision` |
| 3 | A partial close scaled `base_margin` with the position but carried `funding_paid` and `extra_margin` over whole, so trimming 90% of a position left the remaining 10% holding 100% of its funding history. De-risking could trigger the liquidation it was meant to avoid. | high | `TestAllocationScalesWithAPartialClose` |
| 4 | I9 was wired so it could never fail — `equity` is `wallet + uPnL`, so passing both cancelled the term and reduced the check to I1. | high | `TestReconcileReplaysTheLog` |
| 5 | I5's liquidation exemption was `self.liquidations > 0`, a run-wide latch: after the first liquidation, any overdraw was permitted forever. | medium | `TestI5IsNotLatchedOff` |
| 6 | `check_liquidations` never called `_touch`, so LIQUIDATION events were exempt from I8 and could be appended out of order — which breaks spec 12.1's reproducibility contract. | medium | `TestI8CoversLiquidations` |
| 7 | A symbol with no bracket table was skipped silently by the liquidation sweep, so a run with an incomplete snapshot never liquidated it and reported an equity curve with no downside bound. | medium | `test_account.py::test_opening_a_position_with_no_bracket_table_is_refused` |

Two further points are worth keeping.

**The precision defect (#2) was found by the property suite, not by a reviewer** — and only
after the suite was widened. The original properties left every account at the 1× default,
which makes `initial_margin` (`notional / leverage`) an exact division and never produces
the long operands that expose the bug. `LEVERAGES` now generates 1, 3, 7, 10 and 20; 3 and
7 are there precisely because they do not divide a round notional evenly.

**Fixing #4 changed what `reconcile()` is.** It no longer restates I1 over the accumulators;
it replays the event log from the opening balance and compares the result against live
state. That is genuinely independent — it shares no accumulator with the write path — so it
catches a fill mis-booked *identically* into both the wallet and the totals, which is the
one failure I1 is structurally blind to. Writing the test for it then exposed a hole in the
replay itself: iterating the symbols found in the log misses a position held in state that
no event accounts for, which is the most important case. It now walks the union.

One reviewer finding was **not** reproduced and is recorded as such: the bracket max-leverage
check used to sit behind an early return, so any fill that released margin skipped it. No
sequence reachable through the public API exploits that — a position's leverage is fixed at
open, and every fill that pushes its notional into a stricter tier also raises the margin
requirement. The check was made unconditional anyway, and the comment in `_require_margin`
says plainly that this is defensive rather than a fix for a demonstrated failure.

### The second round found the fixes were not *pinned*

An adversarial verifier re-ran all 23 candidate findings against the fixed tree. It refuted
15 — every one of the defects above was genuinely gone — and confirmed 7 more, of a
different kind: **the code was now correct and the tests did not prove it.** Each was
demonstrated by mutating the source and watching all 887 tests stay green.

| Mutation that survived | What it means |
|---|---|
| `allocated_margin` uses the signed `isolated_margin` instead of the floored `reserved_margin` | An overdrawn allocation gets credited back as spendable balance — the wallet spends the same money twice |
| `self._wallet_was_negative = False` | I5's latch never sets, so a correct ledger aborts the run |
| The liquidation trigger tightened from `<=` to `<` | A position marked exactly at `P_liq` survives |
| The I7 call deleted from `_solve_liquidation` | The ledger's own solve is unguarded; every I7 test called the free function by hand |
| I3/I4 deleted from `_check_after_fill` | Same shape: the free functions were covered, `Account` calling them was not |
| `quantize_money` dropped from `isolated_margin` | Killed by the property suite about half the time — a probabilistic pin is not a pin |

Two of those were in tests written *in the same session as the fix*. The I5 test named for
the latch left the wallet at exactly `0.00` — so the latch never set — and then called
`update_mark`, which never reaches `_check_wallet` at all. It asserted `wallet <= 0` and was
satisfied by zero. That is the failure mode worth remembering: a test can name a branch,
read as though it exercises it, and never execute it.

All seven are now pinned, and the pinning was verified the only way it can be — by putting
each defect back and watching a test fail. `scratchpad/verify_mutations_killed.py` runs 13
mutations (the 7 above plus 6 covering the new order-filter code and the funding-to-margin
rule); all 13 are killed.

The seventh confirmed finding was a real gap rather than a test gap, and is A4c below.

## A4c — Spec 3.2's size filters were parsed and enforced nowhere

`minQty`, `maxQty`, `minNotional`, `MARKET_LOT_SIZE` and `PERCENT_PRICE` were read out of
`exchangeInfo` by `parse_symbol` and then consulted by **nothing**. A 0.001 BTC fill at
20 000 — a notional of 20 against BTCUSDT's published minimum of 50 — was accepted, as was
5 000 BTC against a `maxQty` of 1 000. Spec 3.2 is categorical: "orders that violate these
are rejected by Binance in live and must be rejected identically in backtest." Spec 12.2's
required property test ("random order sizes → all 3.2 filters respected") did not exist.

**Not fixed inside `Account`, deliberately.** These are constraints on *orders*, and the
ledger only ever sees *fills*. The distinction is load-bearing rather than pedantic: a
partial fill of 0.001 BTC against a larger order is completely legitimate even though its
own notional is far below `minNotional`, so enforcing that filter at fill time would reject
the correct behaviour of the fill model spec 6.5 describes. What `Account` does enforce on a
fill is I6 — tick and step — because a fill price the exchange could not print is a price
the fill model invented.

The check set now lives in `exchange/filters.validate_order`, next to the data it reads,
operating on scaled ints so it stays on the integer side of the money seam and its modulo
tests are exact. `PERCENT_PRICE` is skipped rather than guessed at when no mark price is
supplied; order-count limits are excluded because they are properties of the open-order
book rather than of a single order, and belong to the execution engine in Phase 4.

**Pinned by:** `tests/property/test_order_filters.py` — spec 12.2's missing property test,
which asserts acceptance implies compliance by re-deriving every bound rather than calling
the same helper, plus the asymmetry that makes `MARKET_LOT_SIZE` a separate filter at all
(BTCUSDT caps market orders at 120 BTC and limit orders at 1 000).

## A5 — Decimal context precision, and the one value that must be quantised

§3.10 states the invariants with **exact equality and no epsilon**, and adds that reaching
for a tolerance means a float has leaked. Making that survivable took two decisions the spec
does not mention:

1. **`money.ACCOUNTING_CONTEXT` runs at 50 significant digits**, applied via
   `decimal.localcontext` rather than by mutating the global context. `Decimal` *addition*
   rounds to the context precision like any other operation, so at the default 28 digits a
   wallet in the 10⁴ range accumulating a realised PnL with 16 decimal places is one
   operation away from a rounded sum — and then `W` and `W₀ + Σrealized − Σfees + Σfunding`
   disagree in the last place, which reads exactly like the float leak I1 exists to catch.

2. **Every amount produced by a division is quantised to 8 decimal places.** Division is the
   only operation that grows a `Decimal` to the full context precision, and an operand
   carrying forty-odd decimal places against a six-figure balance sits at the 50-digit
   ceiling — so the next addition rounds and I1 parts company with itself in the last place.

   Three amounts qualify, and all three reach the ledger:

   - **Entry price** (`money.quantize_entry_price`) — §3.3's VWAP.
   - **Initial margin** (`margin.initial_margin`) — `notional / leverage`, which is exact
     only when the leverage happens to divide the notional. At 7× it does not, and the
     result reaches the wallet through a liquidation's realised loss.
   - **The isolated allocation** (`Position.isolated_margin`) — it inherits a division
     through `Position.scaled()` on every partial close.

   Eight decimals is `SCALE_EXP`, the precision every price in the Parquet lake is stored at,
   so a quantised value round-trips through the storage seam without loss. Rounding is
   half-even rather than against-the-trader: these are recorded balances and averages, not
   order prices, and biasing them would misstate PnL in a fixed direction on every position.

3. **The invariant checks evaluate inside `ACCOUNTING_CONTEXT` too.** This is not a detail —
   a check computed at a different precision than the state it checks is not a check. See
   A4b #2: the ledger was correct and I1 failed anyway, by an amount that looks exactly
   like the float leak the invariant exists to catch.

---

## A6 — Valuation may fall back to entry price; risk may not

Between the first fill and the first mark sample there is no mark to value a position
against. `equity` and `unrealized_pnl` fall back to the entry price, which yields
`uPnL = 0` — the honest answer, and the one that keeps `equity` usable in a state the engine
legitimately passes through.

This is **not** a back door around §3.4's "mark price is ingested, never inferred". Nothing
that costs money uses the fallback: funding goes through the strict `_mark()` and raises,
and `check_liquidations` skips symbols with no recorded mark outright. A position is never
liquidated, and a cashflow never settled, against an entry price standing in for a mark.
Once the first real sample arrives for a symbol the fallback can never be consulted again.

The invariant and the property it checks must agree on this, or I2 would fail on a correct
account for the length of that window — so `_position_view` uses the same fallback.

---

## A7 — What Phase 2 deliberately does not model

Recorded so these read as decisions rather than oversights. All are consistent with §3.7 and
§15.

| Not modelled | Why |
|---|---|
| Cross margin | §3.7: no closed form — one symbol's liquidation price depends on every other position's unrealised PnL, and one bad strategy could liquidate the whole account. v2. |
| Hedge mode (simultaneous long *and* short on one symbol) | Doubles the state and every fill case in §3.3, and exists mostly to let a trader avoid realising a loss. One position per symbol, one-way. |
| Partial / tiered liquidation of very large positions, ADL, insurance fund | §3.7 names all three as out of scope for v1. |
| Maker rebates (negative fee rates) | §3.1 says fees are positive and subtracted. A negative rate is refused by `FeeSchedule` rather than flowing through as a credit that I1 books as a fee. |
| Changing leverage on an open position | Binance permits it; the resulting margin adjustment has edge cases that would be modelled on guesswork. Refused loudly. `add_margin` covers the case that actually matters. |
| Slippage cost (the fourth term of §8.4's decomposition) | It is measured against the reference price *at signal time*, which only the execution engine knows. `Account.attribution()` returns the other three and the engine adds it in Phase 4. |

---

## Verification

```bash
.venv/Scripts/python.exe -m pytest tests/golden tests/property tests/unit/test_account.py tests/unit/test_margin.py tests/unit/test_invariants.py -q
```

Phase 2's exit criterion (spec §13) is *"§3.9 worked example reproduces exactly; all property
tests green"*. The first half is `tests/golden/test_worked_example.py::test_full_sequence`,
which walks t₀ through t₅ on one `Account` and closes with the spec's own reconciliation —
per-step tests with fresh fixtures cannot catch accumulated drift, which is the failure the
reconciliation is there to find.
