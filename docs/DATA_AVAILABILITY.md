# Bulk Data Availability — Verified

**Verified:** 2026-08-01, empirically, against the S3 listing API for `BTCUSDT`.
**Method:** paged `GET https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?list-type=2&prefix=…`

**Extended:** 2026-08-02, by actually fetching it. F1–F4 come from *listing* what Binance
publishes. F5 onwards come from downloading 7,022 archives for BTCUSDT, verifying every
`.CHECKSUM`, and parsing every row of them. A listing tells you a key exists. It does not
tell you that the file has no header row, that two of its columns carry sixteen significant
decimal places, or that every line in it appears twice.

Spec Appendix B ends with "Verify before building — bulk-dataset availability changes."
This document is that verification. Re-run it before building any new ingestion path:

```bash
.venv/Scripts/python.exe -m perplab verify-bulk --symbol BTCUSDT
```

(That command is `perplab.data.bulk_availability.report`. Earlier revisions of this file
pointed at a `scripts/verify_bulk_availability.py`, which does not exist and never did.)

## Results

| Dataset (`futures/um`) | Coverage | Status |
|---|---|---|
| `klines` 1m | 2019-12-31 → 2026-07-30 | ✅ current, no holes — 2405/2405 fetched |
| `aggTrades` | 2019-12-31 → 2026-07-31 | ✅ current |
| `markPriceKlines` 1m | 2019-12-23 → 2026-07-30 | ⚠️ current, **7 interior holes** — F7 |
| `bookDepth` (%-banded) | 2023-01-01 → 2026-07-31 | ✅ current, never ingested |
| `metrics` | 2020-09-01 → 2026-07-31 | ⚠️ current, but **563 days unparseable** (F8) and **267 days duplicated** (F9) |
| `fundingRate` (monthly) | 2020-01 → 2026-07 | ✅ current, no holes — 79/79 fetched |
| **`bookTicker`** | **2023-05-16 → 2024-03-30** | ⚠️ **discontinued** — F1 |
| `liquidationSnapshot` | — | ❌ not published at any path — F2 |

Checksums confirmed present: 320 `.zip` / 320 `.CHECKSUM` for `bookTicker`, 2405 / 2405
for `aggTrades`. A spot `HEAD` of both the archive and its `.CHECKSUM` returned 200. Of the
7,022 archives fetched on 2026-08-02, **zero failed checksum verification** — the
`.CHECKSUM` sibling is real, correct, and worth the extra request every time. It is also
not sufficient: see F9, where the corruption is in the content and the checksum passes.

## Finding F1 — `bookTicker` bulk history is 10.5 months, not full history

**This contradicts spec 4.2 and invalidates its central architectural decision.**

Spec 4.2 states `bookTicker` is "tick-level best bid/ask, **full history**, free ✓ (this
is the useful discovery)", and builds on it:

> **`bookTicker` becomes the primary fill-realism input for long historical ranges.**
> Tick-level best bid/ask across full history … covers years, not just the period since
> your collector started.

It does not cover years. Bulk `bookTicker` for BTCUSDT begins 2023-05-16, ends
2024-03-30, and has not been published for **28 months**. Monthly files agree (12 files,
2023-05 → 2024-04). It is 320 days out of a 6.5-year instrument history.

This is the same failure mode as R1: an unverified assumption about bulk availability
load-bearing for the fill model. R1 fixed the depth assumption and introduced this one.

**Actual fill-tier coverage, corrected:**

```
2019-12 ─────────────────── 2023-05 ── 2024-03 ─────────────────── 2026-08 ──▶
        TRADE_ONLY                     TRADE_ONLY                  BOOK_WALK
        (aggTrades)        BOOK_TICKER (aggTrades)                 (collector,
                           (10.5 mo)                                from today)
```

`BOOK_TICKER` cannot be "the default for most backtests" (spec 4.2 table). `TRADE_ONLY`
is the real default for roughly 5.5 of the last 6.5 years.

### Response 1 — record `bookTicker` live, starting now

Added `btcusdt@bookTicker` to the collector's stream set. It is a free public stream and
is small next to `depth20`, and the reasoning is identical to why the collector exists at
all: bulk publication has already stopped once, so every day we do not record it is a
permanent hole. Without this the gap from 2024-03-30 onward grows forever.

### Response 2 — `TRADE_ONLY` must carry more weight than the spec assumed

Since `TRADE_ONLY` now covers most of the usable backtest range, "fill at the next trade
price plus a fixed conservative spread assumption" (spec 6.4) is too crude to be the
workhorse. `aggTrades` supports a genuine empirical spread estimate: consecutive trades
with opposite `isBuyerMaker` flags bracket the touch, so their price difference is a
direct observation of the effective spread, with no econometrics required.

Deferred to the Phase 5 realism slice — recorded here so it is not rediscovered late.

## Finding F2 — `liquidationSnapshot` is not published

Listed in spec 4.1 and Appendix B; the documented daily path returns no keys. The live
`!forceOrder@arr` stream is therefore the **only** source of liquidation data, which
strengthens the day-one case for the collector. Note that the public stream is throttled
to at most one order per symbol per second, so it samples cascades rather than recording
them completely — counts derived from it are a lower bound.

**Re-verified 2026-08-02, at both cadences.** The original check covered the daily path
only, which left open the reading that Binance had merely moved the dataset to monthly
files the way it does for `fundingRate`. It has not: `daily/liquidationSnapshot/BTCUSDT/`
and `monthly/liquidationSnapshot/BTCUSDT/` both 404, archive and `.CHECKSUM` alike. There
is no path at which this dataset exists.

This is now re-checked on every ingest run rather than remembered. `ingest` prints the
`liquidationSnapshot` caveat unconditionally, whether or not the dataset was asked for
(`cli._print_unavailable_note`), and asking for it explicitly returns `UNAVAILABLE` with a
non-zero exit rather than an empty success. A dataset that is quietly omitted from a run
is indistinguishable from one that was forgotten, which is exactly how the spec came to
list this as available.

## Finding F4 — three WebSocket streams deliver nothing from this machine

Measured against `wss://fstream.binance.com`, on both the combined (`/stream?streams=`)
and raw (`/ws/<stream>`) endpoints, for BTCUSDT and ETHUSDT:

| Stream | Result |
|---|---|
| `<sym>@bookTicker` | ✅ ~33 msg/s |
| `<sym>@depth20@100ms` | ✅ ~10 msg/s |
| `<sym>@trade` | ✅ individual trades flow |
| `<sym>@aggTrade` | ❌ zero messages |
| `<sym>@markPrice@1s`, `<sym>@markPrice` | ❌ zero messages |
| `<sym>@kline_1m` | ❌ zero messages |
| `!forceOrder@arr` | ❌ zero messages |

Not a subscription-order or URL-encoding problem: each fails alone, on the raw endpoint,
for multiple symbols. Not a data problem either -- REST `/fapi/v1/aggTrades` and
`/fapi/v1/premiumIndex` both return live values for the same symbols, and `@trade` (the
unaggregated sibling of `@aggTrade`) works.

The split does not correspond to anything in Binance's own documentation, where all seven
are standard. The likely explanation is the network path from this machine rather than
the exchange, so **this finding is about the environment and needs re-testing elsewhere
before any design change is made.** Recording `@trade` instead of `@aggTrade` would also
create a live/backtest asymmetry, since bulk history is published only as `aggTrades` --
exactly the divergence spec 6.1 exists to prevent. Not worth doing on one machine's
evidence.

**What was worth doing** is what this exposed in our own code: the collector recorded
`aggTrades=0` in every heartbeat for two full runs and never complained. The counts were
right there in the data and nothing evaluated them. A stream that dies silently while the
socket stays healthy is the quietest failure the collector has, and it would have
invalidated a 72-hour run without leaving an obvious trace.

Fixed by adding `STALE` detection (`MAX_SILENCE_S` in `data/collector.py`): each stream
with a guaranteed cadence is checked against a silence threshold, baselined at connect
time so a stream that *never* delivers is caught rather than being invisible forever.
`liquidations` is deliberately exempt -- forced orders are genuinely sparse, and an alarm
that cries wolf teaches you to ignore the category.

### F4 update, 2026-08-02 — still true, and now localised to the production endpoint

Re-verified on a fresh collector run. Production behaved exactly as before: 28 minutes
uptime, zero frames on `aggTrade`, `markPrice@1s` and `!forceOrder@arr`, while `depth20`
and `bookTicker` flowed normally. The `STALE` machinery worked as designed -- both dead
streams were reported within their thresholds and the records are in `collectorEvents`
alongside `CONNECT`, `RESTART` and `SHUTDOWN`, so the outage is explainable from the lake
rather than only from the log.

The new measurement is the **testnet comparison**, which F4 did not have:

| Stream | `fstream.binance.com` (prod) | `fstream.binancefuture.com` (testnet) |
|---|---|---|
| `btcusdt@bookTicker` | ✅ 218 frames / 8 s | ✅ 46 frames / 10 s |
| `btcusdt@aggTrade` | ❌ 0 | ✅ 20 frames / 10 s |
| `btcusdt@markPrice@1s` | ❌ 0 | ✅ 10 frames / 10 s (`e: markPriceUpdate`) |

Same machine, same network, same process, same stream names, seconds apart. That settles
what F4 could only call likely: **the collector's stream names, routing and subscription
are all correct**, and the restriction is specific to the production WebSocket endpoint on
this network path. Production REST on the same host continues to serve the same data --
`premiumIndex`, `aggTrades` and `trades` all returned live values during the outage, and a
1-minute kline over the window reported 138 trades, so the market was not quiet.

Three consequences:

1. **Phase 7 is unaffected.** Papertrading runs on testnet, which delivers everything.
2. **Phase 1b's 72-hour exit criterion cannot currently be met on production.** Three of
   the five datasets would record zero rows for the whole run. They would be *explained*
   (the `STALE` records are there), but "zero unexplained gaps" over a lake missing 60% of
   its streams is not the criterion in spirit.
3. **Phase 2 is unaffected**, because the accounting core consumes bulk history --
   `markPriceKlines` and `fundingRate` are fully backfilled -- not the live feed.

The two streams that *do* work are the two that matter most for long-run value: §4.2 names
`bookTicker` the primary long-history source for fill realism, and `depth20` is the one
series that cannot be bought or backfilled at any price (R1). Leaving the collector running
on production is therefore still the right call, and switching it to testnet would be
actively harmful -- testnet prices diverge from production, so the depth history it
accumulated would be worthless for backtesting.

### F4 resolution, 2026-08-02 — the endpoint serves raw events only, and the fix is REST

The earlier updates narrowed this to "the production endpoint, not our code". A capability
sweep across sixteen stream names settled what the rule actually is.

| Stream | Result | Kind |
|---|---|---|
| `btcusdt@trade` | **131 frames / 6 s** | raw event |
| `btcusdt@depth@100ms` | **59 frames / 6 s** | raw event |
| `btcusdt@depth20@100ms` | works | raw event |
| `btcusdt@bookTicker` | **89 frames / 6 s** | raw event |
| `btcusdt@aggTrade` | silent | aggregated |
| `btcusdt@markPrice@1s`, `@markPrice`, `!markPrice@arr`, `!markPrice@arr@1s` | silent | computed |
| `btcusdt@kline_1m` | silent | aggregated |
| `btcusdt@ticker`, `!ticker@arr`, `@miniTicker`, `!miniTicker@arr` | silent | aggregated |
| `!forceOrder@arr`, `btcusdt@forceOrder` | silent | special event |

**Every raw per-event stream works; every aggregated or computed one is silent.** Three
further observations make this conclusive rather than suggestive:

1. `LIST_SUBSCRIPTIONS` returns `["btcusdt@aggTrade","btcusdt@markPrice@1s","!forceOrder@arr"]`
   after a `SUBSCRIBE` that the server ACKs with `{"result":null,"id":1}`. The names are not
   wrong and the subscription is not rejected — the server accepts them and sends nothing.
2. A combined subscription carrying `bookTicker` *and* `aggTrade` delivers the first and not
   the second **over one TLS connection**. No middlebox can read inside that stream to drop
   messages selectively, so nothing on the network path can be responsible.
3. Other Binance WS services on the same machine are unaffected: COIN-M
   (`dstream.binance.com`) serves `markPrice` and spot (`stream.binance.com`) serves
   `aggTrade`. It is specific to the USD-M futures stream service.

**REST, on the same host and the same minute, serves all of it.** `premiumIndex` (weight 1),
`aggTrades` (weight 20), `trades`, `fundingRate`, `depth` and `ticker/24hr` all return HTTP
200 with live data. `GET /fapi/v1/allForceOrders` returns **HTTP 404** — Binance withdrew it.

**What was built.** `perplab/data/rest_poller.py`:

- `markPrice` ← `premiumIndex` at 1 Hz. Verified to stamp a fresh `time` every second
  (12 polls → 12 distinct timestamps, ~1000 ms apart), so this reproduces `@markPrice@1s`
  exactly. Repeat timestamps are dropped rather than recorded twice.
- `aggTrades` ← `/fapi/v1/aggTrades` with `fromId` paging at 0.5 Hz, into the *same* dataset
  and schema the bulk archives fill, so the live period and the history behind it stay one
  series. Ids are dense and contiguous across page boundaries, and a `fromId` past the head
  returns 200 with an empty list, which is the "caught up" signal.
- Rate budget: 60 + 600 = **660 weight/min against a 2400 limit**, leaving room for retries
  and multi-page catch-up.

**Why polling is not a downgrade.** A dropped WebSocket frame is invisible — nothing in the
data records that a message was skipped. A `fromId` cursor either receives the next trade or
receives nothing, and the id sequence stays checkable after the fact. `recv_ms` becomes poll
time rather than push time and is correspondingly weaker as a latency measure; `ts_ms` is
still the exchange's own clock, which is what the engine orders on (§6.2).

**`btcusdt@trade` works and was deliberately not used.** It is individual fills rather than
aggregated ones, has no bulk counterpart, and adopting it would fork every downstream
consumer into handling two trade shapes forever. It is recorded here as a verified fallback
if the REST route ever becomes unavailable.

**`liquidations` has no source at all** — WS suppressed, `allForceOrders` withdrawn. Rather
than leave an empty dataset that reads as an unexplained gap forever, the collector writes
one `UNAVAILABLE` collector event per run naming the dataset and the reason, and `gaps`
treats that as explaining the silence (§4.5). This is the honest outcome: the data does not
exist to be collected, and the lake says so in the lake.

**Live verification, 90 s against production:** depth20 90 rows, bookTicker 6741, aggTrades
293 with **0 id holes and 0 duplicates**, markPrice 90 samples with 90 distinct timestamps
and a 1011 ms worst-case interval, liquidations 0 rows with an `UNAVAILABLE` record, and no
`STALE` events.

## Finding F3 — `leverageBracket` requires API keys — **CLOSED 2026-08-02**

`GET /fapi/v1/leverageBracket` unsigned returns `HTTP 401 {"code":-2014}`. It is a signed
endpoint, and on that basis bracket snapshotting was skipped, leaving the Phase 0 exit
criterion ("reference data pulled and versioned on disk") half-met.

**That conclusion was too narrow.** The documented endpoint is signed; the *data* is public.
Binance renders these brackets on a public web page, and the endpoint behind it —

```
GET https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets
```

— returns HTTP 200 unauthenticated with the full table for **987 symbols**. BTCUSDT's twelve
tiers match the documented schedule, and the `cum` values are internally consistent
(tier 2: `300000 × (0.005 − 0.004) = 300` ✓; tier 3: `300 + 800000 × (0.0065 − 0.005) = 1500` ✓),
which independently confirms the continuity convention the Phase 2 fixtures derive.

Phase 0 is therefore fully met, and **nothing in this process holds a credential.**

Three details that matter:

- **The field names differ.** `bracketSeq`, `bracketNotionalFloor`, `bracketNotionalCap`,
  `bracketMaintenanceMarginRate`, `cumFastMaintenanceAmount`, `maxOpenPosLeverage`. They are
  normalised onto the documented names by a mapping table in `core/margin.py` so both shapes
  share one parse path. Note `initialLeverage` maps to `maxOpenPosLeverage`, not
  `minOpenPosLeverage` — both are plausible integers, and picking the wrong one yields a
  table that parses cleanly and permits 101× where the exchange permits 150×.
- **Rates arrive as bare JSON numbers**, e.g. `0.0333`, which no binary float represents
  exactly. The float guard written in Phase 2 (`brackets_from_payload` refuses a `float`
  rather than converting it) earns its place here for the first time: the payload must be
  read with `json.loads(..., parse_float=Decimal)`.
- **It is not part of the documented API**, so it may move. `snapshot_leverage_brackets`
  validates before archiving — the document must decode *and* produce a real `BracketTable`,
  ordering and contiguity included — and fails loudly rather than filing an error envelope
  as though it were reference data.

---

The findings below were established on 2026-08-02 by the full BTCUSDT backfill. Where a
count is given it is a count of real archives, not a sample.

## Finding F5 — CSV header rows are conditional per *file*, and the pattern is not an era

Nothing in spec §4 or in Binance's own documentation mentions this. Some bulk archives
open with a header row naming the columns and some open with data, and which you get is a
property of the individual file.

Measured across every archive ingested:

| Dataset | Archives | Headered | Headerless |
|---|---|---|---|
| `klines` 1m | 2405 | 1481 | 924 |
| `markPriceKlines` 1m | 2357 | 1483 | 874 |
| `fundingRate` | 79 | 79 | 0 |
| `metrics` | 1597 | 1597 | 0 |
| `aggTrades` | 14 | 14 | 0 |
| `bookTicker` | 7 | 7 | 0 |

**The boundary date is 2022-08-11.** For both kline-shaped datasets, every archive from
2022-08-11 onward carries a header and the last headerless archive is 2022-08-10. The two
datasets flipped on the same day, which suggests one change to the publishing pipeline
rather than a per-dataset decision.

**The era before it is not uniformly headerless, and that is the part that matters.**
Scattered through the pre-2022-08-11 range are islands of headered days:

```
klines            2020-09-15..21, 2020-09-28, 2020-12-28..2021-01-01,
                  2021-07-09..13, 2021-12-28..2022-01-01, 2022-03-28..04-03
markPriceKlines   2019-12-23..24, 2020-01-19, 2020-02-04, 2020-02-06, 2020-02-10,
                  2020-02-19, 2020-05-26, 2020-06-17, 2020-07-21,
                  2020-12-28..2021-01-01, 2021-07-09..13, 2021-12-28..2022-01-01,
                  2022-03-28..04-03
```

Four of those islands are the same days in both datasets, and they sit around year ends and
half-year boundaries, which reads like a handful of days having been regenerated later
under the newer format. Whatever the cause, the consequence is settled: **there is no rule
of the form "headers from date D" that is correct.** `klines` alone has seven separate
contiguous headerless runs. A configuration flag per dataset, or per dataset and era, gets
some days wrong however it is set.

`fundingRate` and `metrics` carry a header in every era back to 2020-01 and 2020-09
respectively, with no exceptions in 1,676 archives.

**Both wrong answers lose data, and the ingest reports success either way. That is why
this is sniffed per file.**

- Assume a header where there is none, and the first data row of every pre-2022-08-11
  archive is dropped — one bar per file across 924 files for `klines`. No error is raised
  and the run exits 0.
- Assume no header where there is one, and `int("open_time")` either crashes — which is
  the good outcome — or, once someone wraps it in a `try`, the header is coerced into a
  junk row that sorts to the front of the partition and looks like a real bar.

**How far downstream the first mistake would travel depends entirely on the dataset**, and
the difference is worth stating precisely rather than waving at:

- For `klines` and `markPriceKlines` it is recoverable. Spec 4.5's rule 1 walks `lag()` over
  distinct `open_time` values and reports any span wider than one interval, so 924 dropped
  first-bars surface as 924 one-minute gaps between each day's 23:59 bar and the next day's
  00:01. Loud — but only for someone who runs gap detection across the whole 2019–2022 era
  and reads a report with 924 entries in it.
- For `aggTrades` and `bookTicker` it is permanent. The tick rule tests for inter-record
  silence above a 60-second threshold. One dropped trade at the top of a day, among tens of
  millions, is beneath every threshold in the system and no rule will ever see it.
- For `fundingRate` and `metrics` the question does not arise, since both are always
  headered — but neither has a gap rule at all (`GapRule.NONE`), so nothing would have
  caught it if it did.

So the safety net exists for exactly one of the six datasets, and only if someone goes
looking. Sniffing per file is what makes the net unnecessary.

The implementation is `bulk_layout.is_header_line`. It compares the first line against the
dataset's verified column names rather than testing whether field 0 parses as a number:
`metrics` and `bookDepth` open every *data* row with a datetime string, so the numeric test
classifies their data as headers. It gives the right answer for those two datasets today
only because they happen to be always-headered, which is a coincidence and not a reason.

**Not verified here:** whether `aggTrades` and `bookTicker` were also headerless before
2022-08-11. Every archive of those two on disk is from 2024-03 or 2026-07 and all are
headered, so this document says nothing about their early era. Per-file sniffing means
nothing depends on the answer, but do not read the table above as evidence either way.

## Finding F6 — `fundingRate` bulk is the only published source of `fundingIntervalHours`

Spec 3.5 rule 3 and review finding R17 both forbid assuming an 8-hour funding interval:
Binance runs different intervals on different symbols and has changed the interval on
existing ones. Spec 3.5 says to read `fundingIntervalHours` per symbol from `exchangeInfo`.

**`exchangeInfo` does not carry the field.** Verified 2026-08-01 across all 851 USD-M
symbols in the payload; `tests/unit/test_filters.py::test_funding_interval_absent_from_exchange_info`
pins it. That test documented the absence and left the question of where to get the real
value open.

**The bulk `fundingRate` archive carries it, as its second column.** Every settlement row
is `calc_time, funding_interval_hours, last_funding_rate`, and the value is a plain integer
count of hours. Across all 7,212 BTCUSDT settlements from 2020-01 to 2026-07 it is
constant at 8 — but that is now an *observation of the whole history* rather than the
assumption the spec forbids, and a symbol or an era where it differs will read as different
without any code changing.

This closes the open question. The archive is authoritative, `parse_funding_row` stores the
column unscaled, and `gaps.detect_funding_gaps` reads it from the ingested data to compute
its `1.5 ×` threshold rather than taking a constant. Funding is therefore the one dataset
in the lake whose gap rule has no hardcoded number in it at all.

The practical consequence is an ordering constraint: **funding must be ingested before its
own gap rule can run.** That is not a limitation to work around — it is the rule refusing
to guess.

## Finding F7 — `markPriceKlines` 404s *inside* its own documented coverage window

> **CORRECTED 2026-08-05. The conclusion below was wrong.** F7 probed only the *daily*
> endpoint and read 56 404s as "never published". Fifty of those days are published — in the
> *monthly* archive for the same month. `monthly/markPriceKlines/BTCUSDT/1m/BTCUSDT-1m-
> 2021-01.zip` returns 200 and carries 44,640 rows: 31 × 1440, every day of January 2021 at
> full length, including the fourteen the daily endpoint refuses. All fifty have been
> ingested (`fill_days_from_monthly`), verified against the archive's own `.CHECKSUM`, and
> the lake now holds **2,410 of 2,416 days** with **zero duplicate `open_time`**.
>
> **Still absent: the six days 2019-12-25 .. 2019-12-30.** `monthly/...2019-12.zip` is a 404
> as well, so those are genuinely published at neither cadence — the only part of the
> original finding that survives.
>
> The lesson is the one the rest of this document keeps arriving at: **a 404 locates a key,
> it does not establish a fact.** "Not at this URL" became "Binance never published it"
> without the second endpoint ever being tried, and that inference then sat in the barren
> ledger telling every future top-up not to bother looking.

### Original finding, as written

`PUBLISHED_COVERAGE` records `markPriceKlines` as 2019-12-23 → current. That is true at
both endpoints and hides seven holes in between. Of 2,413 daily archives requested for
2019-12-23 .. 2026-07-31, **56 returned 404**, in these runs:

```
2019-12-25 .. 2019-12-30   (6 days)
2021-01-18 .. 2021-02-20  (34 days)
2021-03-22 .. 2021-03-26   (5 days)
2021-05-24 .. 2021-05-28   (5 days)
2021-06-07 .. 2021-06-08   (2 days)
2021-06-10 .. 2021-06-11   (2 days)
2021-06-27 .. 2021-06-28   (2 days)
```

Thirty-four consecutive days of a dataset that the S3 listing presents as continuous. The
first archive of all, 2019-12-23, is also partial: it starts at 11:58Z, so 718 bars are
absent before the first bar exists at all.

`klines` over the same range has **zero** holes — 2405 of 2405 archives fetched. So this is
specific to the mark-price series, not a general property of the publisher, and a range
that looks safe because klines are complete may still be missing mark prices. Anything that
joins the two (funding PnL, liquidation-price reconstruction) has to handle it.

**The ingest path already reports this correctly** — it distinguishes `MISSING` (a 404
inside the window, worth investigating) from `UNPUBLISHED` (a 404 outside it, expected) —
so the holes surfaced as 56 `MISSING` outcomes rather than being papered over. What was
missing is this finding. A coverage table with two endpoints in it cannot express an
interior hole, and nobody reading `2019-12-23 → current` would think to check.

## Finding F8 — 563 `metrics` days cannot be ingested: two ratio columns exceed 8 decimals

The project stores every price, quantity and rate as an int64 scaled by 10⁸
(`perplab.core.money`). `to_scaled` refuses a value carrying more than 8 significant
decimal places rather than rounding it.

Most `metrics` values are published with sixteen decimal places of *trailing zeros* —
`6858675634.7062540000000000` — which `to_scaled` strips before deciding anything, so they
scale exactly. Spot-checked on 2026-07-15: 16 fractional digits written, at most 8
significant.

**`count_toptrader_long_short_ratio` and `count_long_short_ratio` are different.** They are
ratios of integer account counts, and for a nineteen-month era Binance published them at
full binary-float precision:

```
BTCUSDT-metrics-2021-12-01.csv line 2
  metrics.count_toptrader_long_short_ratio: value '4.151199024522422'
  has more than 8 decimal places
```

Significant fractional digits across the failures: 13 digits ×4, 14 ×28, 15 ×258, 16 ×834.

**Affected: 563 of the 2,160 days requested.** Not one continuous block, which is worth
knowing before anyone writes a range check:

```
2021-12-01 .. 2021-12-30   (30 days)
2022-01-19 .. 2022-01-29   (11 days)
2022-01-31 .. 2022-09-18  (231 days)
2022-09-22 .. 2023-03-12  (172 days)
2023-03-14 .. 2023-07-09  (118 days)
2025-12-31                  (1 day)
```

That is 562 of the 586 days in 2021-12-01 .. 2023-07-09 — twenty-four days inside the era
parse cleanly — plus one isolated day at the end of 2025.

`MalformedArchive` was raised per file, nothing was written for those days, and the ingest
leg exited non-zero. That is spec 1.4 behaviour working exactly as intended. It also means
**nineteen months of open-interest history is absent from the lake**, and that is the one
part of the Phase 1 backfill that was requested and not delivered.

**The seam was deliberately not widened.** Three fixes exist and all three are design
decisions rather than bug fixes:

- raise `SCALE_EXP` above 8 — a project-wide change with consequences in every module that
  touches money, and int64 headroom is finite;
- give the two ratio columns a separate `RATIO_SCALE` — plausible, since they are counts
  over counts and not money at all, but it puts two numeric regimes in one lake;
- round the two columns at ingest — the cheapest and the only one that is certainly wrong,
  because it makes `to_scaled`'s refusal conditional on which column is being read.

None of them belongs in a verification pass. Recorded here so that whoever picks it up sees
the measured distribution rather than rediscovering it.

## Finding F9 — 267 `metrics` days are published with duplicated rows, and the checksum passes

The lake holds 534,563 `metrics` rows against **459,304 distinct `create_time` values**.
75,259 rows — 14% — are duplicates.

```
2020-09-01 .. 2021-05-21   263 consecutive days, wholesale doubling
                           (2020-09-01: 444 rows for 222 slots)
2024-04-08, 2024-05-01, 2026-06-12, 2026-06-21
                           one duplicated row each
```

The first run is the entire first nine months the dataset existed. Verified at source: the
`BTCUSDT-metrics-2020-12-31` archive contains 576 lines for 288 five-minute slots, each
line byte-identical to its twin.

**The zip passes its published sha256.** The duplication is in the content Binance
published, so checksum verification — which is structural in this pipeline and cannot be
skipped — cannot see it, and correctly does not fire.

**Nor can gap detection, for a reason worth fixing.** `gaps.Coverage.duplicate_rows`
exists, is exactly the right measurement, and is computed **only by the kline rule**.
`metrics` is `GapRule.NONE` (its 5-minute cadence has no expected-count model), so no rule
runs over it and the field is never populated. The duplication is therefore invisible on
every reporting surface the project has.

Consequence: **any mean, sum or z-score over `metrics` before 2021-05-22 is double-counted
today.** Open interest summed over that era is twice the truth.

`klines`, `markPriceKlines` and `funding` were checked the same way and are duplicate-free
— 3,463,200 kline rows with 3,463,200 distinct `open_time`, 7,212 settlements all distinct.

The cheap fix is to compute `duplicate_rows` for every dataset that has a timestamp column,
independently of which gap rule applies. That is a change to `gaps.py` and is not made here.

## Finding F10 — Binance publishes flat bars through halts, so the missing-bar rule never fires

Spec 4.5 rule 1 detects kline gaps by comparing expected bar count against actual. Over
**3,463,200 bars spanning 2019-12-31 .. 2026-07-31 it found nothing**: zero missing bars,
zero duplicate timestamps. Six and a half years of a real exchange with no gaps in its
1-minute series.

That is not because nothing went wrong. It is because Binance does not omit bars during an
outage — it publishes flat placeholder bars. All 273 zero-volume bars in the history form
13 contiguous runs, every one with `open=high=low=close`, `quote_volume=0`, both taker
volumes 0 and `count=0`:

```
2020-09-27 (2m)   2021-03-02 (59m)  2022-05-01 (29m)  2022-05-28 (35m)
2023-09-12 (19m)  2023-11-14 (3m)   2024-10-28 (14m, 1m, 74m)
2025-01-14 (2m)   2025-01-29 (13m, 4m)  2025-08-29 (18m)
```

Seventy-four consecutive minutes with zero trades on the most liquid perpetual in existence
is not a quiet market. The price gapped 69566.10 → 69650.00 across that window
(2024-10-28 20:00–21:14Z), which is what a matched-engine halt looks like from outside.

**Both behaviours here are individually correct and together they leave a blind spot.**
Rule 1 judges presence on `open_time` alone and never consults `volume`, precisely so a
genuinely quiet minute is never reported as a gap (review finding R21). That is right. But
the only evidence of an outage that klines carry *is* the flat run, so the rule is
deliberately built to classify the sole available signal as data. **Rule 1 will never fire
on BTCUSDT.**

Detecting halts needs a fifth rule — "a run of flat zero-volume bars on a liquid symbol" —
which is not invented here. Spec 4.5 defines four rules; a fifth with an unreviewed
liquidity threshold does not belong beside them without a decision. The 13 runs above are
recorded so that whoever writes it has a labelled test set on the first day.

## Finding F11 — one `metrics` archive is shifted by a five-minute slot across a year boundary

`ingest_bulk._check_partition_containment` refuses an archive whose earliest or latest row
resolves to a different partition from the one its period names. Its docstring read "Not
observed firing against any real archive." It has now fired.

`BTCUSDT-metrics-2025-12-31` runs **2025-12-31 00:05:00 .. 2026-01-01 00:00:00** — shifted
one slot late — so its final row belongs to `year=2026` while the archive's period resolves
to `year=2025`. The guard refused it; the day is one of the 563 absent from the lake, and
appears in F8's list for this reason rather than for the decimal-precision reason.

Probed for systemic behaviour: 2020, 2021, 2022, 2023 and 2024 `-12-31` all run
00:00:00 .. 23:55:00 correctly. It is a one-off publication defect on exactly one day.

Worth recording because of what the alternative was. Without the guard those 288 rows would
have been filed under `year=2025`, where the last of them is a row timestamped 2026 that no
query for 2026 would ever find and no gap detector could explain — a hole with no evidence
of itself anywhere on disk.

## Finding F12 — real bulk volumes, measured; spec 4.4's sizing table understates `bookTicker`

Spec 4.4 sizes storage per symbol per year and gives `bookTicker` at 8–20 GB for BTCUSDT.
Measured, from 7 real days (2024-03-24 .. 30, the last 7 days that exist):

| | Downloaded (zipped) | On disk (Parquet, zstd) |
|---|---|---|
| `bookTicker` | 141.6 MB/day | 79.8 MB/day |
| `aggTrades` | 12.0 MB/day (2026-07), 19.1 MB/day (2024-03) | 10.3 MB/day |
| `klines` 1m | 59.5 KB/day | 88.7 KB/day |
| `markPriceKlines` 1m | 36.0 KB/day | 48.4 KB/day |
| `metrics` | 10.9 KB/day | 13.6 KB/day |
| `fundingRate` | 0.87 KB/month | 1.9 KB/month |

Note that the four small datasets land on disk *larger* than they arrived. Their CSVs are
short ASCII decimals that gzip very well, and the lake stores every one of those as a full
scaled int64. That is the price of the numeric seam and it is a rounding error in absolute
terms — the whole of `klines`, `markPriceKlines`, `metrics` and `funding` for 6.5 years is
340 MB.

**`bookTicker` at 79.8 MB/day of Parquet is 28.4 GB/year**, above the top of spec 4.4's
range, and the surviving 320-day window alone is roughly **44 GB to download and 25 GB on
disk**. Spec 4.4's realistic-footprint figure ("3 symbols × 2 years → 60–120 GB") assumes
`bookTicker` is available across those two years; F1 says it is not, so that number is
wrong in the other direction for a different reason.

`aggTrades` at 10.3 MB/day is 3.7 GB/year, inside spec 4.4's 3–8 GB band; a full 2405-day
history would be about 24 GB on disk. The sizing table is right about `aggTrades` and
understates `bookTicker` by roughly half.

**The repo's own `TYPICAL_BYTES_PER_PERIOD` over-estimates every dataset, by 1.7× to 8.3×**
— it declares 240 MB/day for `bookTicker` against 141.6 measured, 80 MB/day for `aggTrades`
against 12–19, 350 KB/day for `klines` against 59.5. Its dry-run figures ("full `aggTrades`
= 179.2 GB", "full `bookTicker` = 71.5 GB") are correspondingly high.

**The constants were deliberately not lowered.** `aggTrades` volume varies enormously by
era — the 2024-03 sample is 60% larger than the 2026-07 one, and a 2021 bull-market day
will be larger again — so a blanket reduction fitted to fourteen days risks
under-estimating the backfill it is meant to warn about. Under-estimating is the unsafe
direction for a disk-capacity warning; over-estimating merely makes an operator cautious.
The over-estimate is intentional and now the magnitude is on record.

## Finding F13 — a truncated Parquet raises; it does not read short

`writer.py`'s argument for atomic publishing states that "DuckDB reads truncated files as
*short* rather than *broken* — a silent data loss". Measured against duckdb 1.x, that is
not what happens:

```
InvalidInputException: No magic bytes found at end of file
```

The conclusion — write `.tmp`, then `os.replace` — is unchanged and in fact rests on
firmer ground: a single truncated file makes the **entire dataset unqueryable** through the
`**/*.parquet` glob, rather than quietly shortening one partition. Loud and total beats
silent and partial, but it is a different failure and the reasoning should say so.

The observed behaviour is pinned by
`tests/integration/test_corrupted_sample.py::test_truncated_file_fails_loudly_rather_than_reading_short`,
which asserts on the `magic bytes` message, so a future DuckDB release that makes
truncation quiet again breaks a test rather than a backtest.

Two docstrings still carry the original wording — `writer.py`'s opening paragraph and
`manifest._row_count`, which cites it. Both reach the right conclusion by the wrong route.
Correcting them is a source change, not a documentation one, and is left for whoever
touches those files next.

---

## Finding F14 — the collector's own defects, found by review rather than by the lake

Not observations about Binance. These are four defects in *this* code, all confirmed by
reproduction on 2026-08-02, all of which the 1 244-test suite was passing over. They are
recorded here rather than only in the commit log because each one changes how a number in
this document should be read.

### F14a — an aggregate-trade catch-up lost pages on a mid-walk failure

`AggTradePoller.fetch` accumulated pages into a local list and returned it at the end, while
advancing `_next_id` after each page. A failure on page three therefore discarded pages one
and two **after the cursor had already moved past them**. The next poll asks from the
cursor; the ids below it are never requested again; and because the cursor moved
consistently, the id-jump check has nothing to notice.

Reproduced: a 3 500-trade backlog with the third page failing lost 2 000 ids and reported
only a DISCONNECT/RECONNECT pair. A twenty-page catch-up costs 400 weight in one tick, so a
429 partway through is the expected trigger rather than an exotic one.

Fixed by delivering each page to the writer *before* advancing past it. The "0 id holes, 0
duplicates" figure quoted for the current run was measured on the fixed code.

### F14b — a partial flush duplicated the partitions that had already landed

`ParquetBufferedWriter.flush` cleared the buffer only after every partition had been
written, so a failure on partition three left partitions one and two on disk **and** still
buffered — and the documented retry wrote them a second time. The duplicates land in tick
datasets, where `Coverage.duplicate_rows` is not computed (finding F9), so no reporting
surface in the project would ever have shown them.

Second-order, same cause: `_flush_all` was unguarded inside the heartbeat loop, so one
`OSError` killed that task. No further heartbeats, no staleness checks, no state file — the
collector went dark in `collectorEvents` while still buffering market data — and the
exception then re-raised at shutdown, skipping the `SHUTDOWN` record and the final forced
flush. A clean stop became indistinguishable from a crash and the last buffer was lost.

### F14c — the gap detector could not see a RESTART written after the range

`load_collector_events` widened its query by 30 s at each end. But a RESTART is written when
the collector *comes back*, and it accounts for the outage before it — so the record that
explains the last gap in a range can be written hours later. An overnight crash therefore
reported as **unexplained** on every `--end yesterday` run, which is the exact query the
Phase 1b criterion is checked with. The explanation existed in the lake and matched
perfectly; it was simply never loaded.

Fixed with a separate, narrow lookahead query for downtime-carrying records only, so the
cost is a handful of lifecycle rows rather than a week of ten-second heartbeats.

### F14d — an instantaneous record explained an outage of any length

`explain_gaps` matched on *overlap*, which is the right test for a record that opens an
open-ended outage (a socket DISCONNECT means "down from here") and the wrong one for a
record that measures a bounded incident. A REST poller writes a DISCONNECT when one request
fails and a RECONNECT with `downtime_ms` when the next succeeds — and under overlap that
twenty-second incident accounted for a twenty-four-hour hole. Combined with F14a, genuinely
lost trades were being filed as explained.

Each record is now held to the stretch it actually speaks for: a measured downtime for
RESTART/RECONNECT, and "until the next recovery on this stream" for DISCONNECT/STALE.

### Still open, and deliberately

`writer.py` renames a completed temp file into place, which is atomic — but nothing is
`fsync`ed, neither the file nor the directory. After a power cut the rename can be durable
while the data is not, publishing a short or zero-length file under a final name. Not
reproducible without crash injection, and fixing it costs a synchronous flush per partition
on the collector's hot path. Recorded rather than changed on a suspicion; the module's
docstring claims more crash-safety than the code currently provides.
