# Backfilling the Lake — Operator's Guide

For whoever has to run this at two in the morning six months from now, having forgotten
everything. It assumes you know what a Parquet file is and nothing else about this project.

Every timing and byte figure here comes from the full BTCUSDT backfill of 2026-08-02. Where
a number is an extrapolation rather than a measurement, it says so.

Run everything as `.venv/Scripts/python.exe -m perplab <command>`, abbreviated to
`perplab` from here on.

---

## The thirty-second version

```bash
perplab ingest --dry-run --symbol BTCUSDT --dataset klines --start 2024-01-01 --end 2024-01-31
perplab ingest           --symbol BTCUSDT --dataset klines --start 2024-01-01 --end 2024-01-31
perplab gaps             --symbol BTCUSDT --dataset klines --start 2024-01-01 --end 2024-01-31
```

Look, fetch, check. **Keep the `--dataset`** until you have read the cost table below:
without it, `ingest` fetches all six Phase 1 datasets, and one month of `bookTicker` alone
is over 4 GB.

`--start` and `--end` are **inclusive UTC dates** on every command. `--end 2024-01-31`
covers the whole of the 31st. `--root` defaults to `userdata`, and the lake lives one level
inside it at `userdata/market/`.

If you were interrupted, run the exact same `ingest` command again. That is the whole
recovery procedure; the rest of this document explains why it is safe.

---

## Before you start

**1. Check the machine can survive it.**

```bash
perplab preflight
```

A backfill of the tick datasets runs for hours. A laptop that sleeps mid-run is not a
socket drop, it is a hard process kill. `preflight` checks sleep and hibernate timers on
AC, free disk, and clock drift, and tells you the `powercfg` incantation if the timers are
wrong.

**2. Always dry-run first.** It reads only the local receipt ledger and the coverage table.
No network calls, not even a `HEAD`. It costs nothing and there is no reason to skip it.

```bash
perplab ingest --dry-run --symbol BTCUSDT --dataset bookTicker \
    --start 2023-05-16 --end 2024-03-30
```
```
bookTicker BTCUSDT: 313 of 320 periods to fetch, ~70.0 GB
  7 already ingested (receipt + Parquet on disk); --force would re-fetch them
  first: 2023-05-16, 2023-05-17, 2023-05-18, 2023-05-19, 2023-05-20 ...
  caveat: Published only 2023-05-16 .. 2024-03-30, then discontinued ...

TOTAL: 313 archive(s), ~70.0 GB
Nothing was downloaded. Re-run without --dry-run to fetch.
```

**Seventy gigabytes from one unremarkable command.** That is the entire reason `--dry-run`
exists. The estimate is deliberately high — the measured figure is nearer 43 GB (finding
F12) — because under-estimating a disk warning is the direction that costs you a
half-backfilled lake at hour nine, and a half-backfilled lake is indistinguishable from a
genuinely incomplete history until you go looking.

The dry-run also tells you three things you cannot get any other way:

- **how much is already done** — periods with a receipt *and* their Parquet still on disk;
- **how much falls outside published coverage** — those will 404 by publication, not by
  failure, and are not counted in the byte estimate;
- **the registry caveat** for the dataset, which is where `bookTicker` tells you its
  history stopped in 2024 and `metrics` tells you its values may be blank.

---

## What it costs

Measured on 2026-08-02, BTCUSDT, at the default `--workers 4` on a domestic connection.

| Dataset | Archives | Wall clock | Rate | Downloaded | On disk |
|---|---|---|---|---|---|
| `klines` 1m | 2405 (6.5 y) | 8m 32s | 4.7 files/s | 143 MB | 208 MB |
| `markPriceKlines` 1m | 2413 (6.5 y) | 8m 09s | 4.9 files/s | 83 MB | 111 MB |
| `metrics` | 2160 (5.9 y) | 6m 18s | 5.7 files/s | 24 MB | 21 MB |
| `fundingRate` | 79 (6.5 y) | 13s | 6.1 files/s | 0.07 MB | 0.15 MB |
| `aggTrades` | 7 days | 33s / 50s | 2.5–2.7 MB/s | 84–134 MB | 144 MB (14 d) |
| `bookTicker` | 7 days | 7m 41s | 2.2 MB/s | 991 MB | 558 MB |

`aggTrades` appears twice because two different weeks were fetched: 2026-07-13..19 in 33s
and 2024-03-24..30 in 50s. Same seven days, half again as long, because the 2024 week
carries 60% more bytes per day.

**The four small datasets are round-trip-bound, not bandwidth-bound.** Each archive is tens
of kilobytes and costs two HTTP requests (the `.CHECKSUM`, then the archive), so throughput
sits at 5–6 files per second regardless of how fat your pipe is. More `--workers` would
help; see the note on courtesy below before you reach for it.

**The two tick datasets are bandwidth-bound**, and both settle around 2.2–2.7 MB/s at
four concurrent transfers.

### Planning a large backfill

| | Archives | Time | Downloaded | On disk |
|---|---|---|---|---|
| Everything except tick data, full history | 7057 | **~23 min** | ~250 MB | ~340 MB |
| `bookTicker`, its whole 320-day window | 320 | **~6 h** | ~44 GB | ~25 GB |
| `aggTrades`, full 2405-day history | 2405 | **~3–5 h** | ~29–46 GB | ~24 GB |

The first row is measured. The other two are extrapolations from seven real days each and
carry a specific risk: **`aggTrades` and `bookTicker` volumes vary enormously by era.** The
2024-03 sample is 60% larger per day than the 2026-07 one, and a 2021 bull-market day will
be larger again. Budget above the top of those ranges, not the middle.

Neither large backfill has been attempted. If you are the first to run one, please replace
the extrapolation in this table with what actually happened.

### On `--workers`

The default is 4 and it is a deliberate ceiling, not a shrug. `data.binance.vision` is a
free public mirror; a backfill that hammers it is both rude and likely to be throttled into
being slower than four would have been. Datasets are also ingested one after another rather
than in parallel, so the default run makes four concurrent requests total, not
four-per-dataset.

Raise it if you have a reason. Do not raise it because a 2400-file run feels slow.

---

## Running it

```bash
perplab ingest --symbol BTCUSDT --start 2019-12-31 --end 2026-07-31
```

With no `--dataset` this fetches the six Phase 1 datasets in order: `klines`,
`markPriceKlines`, `aggTrades`, `bookTicker`, `fundingRate`, `metrics`. That order is
publication order by usefulness, not cheapest-first — `klines` is what the tick gap rules
cross-check against, so a run you interrupt halfway still leaves a lake whose gap report
can be computed.

Narrow it with repeatable `--dataset`, which accepts either the archive name or the lake
name (`fundingRate` and `funding` both work):

```bash
perplab ingest --symbol BTCUSDT --dataset klines --dataset funding \
    --start 2019-12-31 --end 2026-07-31
```

Progress is one line per completed archive, written to stderr, plus a heartbeat every five
seconds while a large transfer is in flight:

```
ingest klines BTCUSDT: 3 periods
[    1/3    ] klines BTCUSDT 2026-07-30  skipped              0 rows        0 B  | skip 1  0 B/s  ETA 00:00:00
[    2/3    ] klines BTCUSDT 2026-07-29  skipped              0 rows        0 B  | skip 2  0 B/s  ETA 00:00:00
[    3/3    ] klines BTCUSDT 2026-07-31  skipped              0 rows        0 B  | skip 3  0 B/s  ETA 00:00:00

klines BTCUSDT: 3/3 periods in 00:00:00
  skipped 3
  0 rows, 0 B downloaded
```

It is a log, not a status bar that redraws in place. When a 2400-file run fails at hour six
the questions are always "which files" and "was it slowing down", and only a scrollback
answers those. Periods complete out of order because four workers are running; the final
report is sorted, so two runs over the same range produce identical summaries.

**Read the exit code.** Zero means every requested period was accounted for and none of
them failed. Anything else means the range you asked for is not fully on disk, and that
includes an interruption — the data already written is sound, but a script that treats a
partial backfill as complete will backtest over a range it does not have.

Each archive ends in exactly one of six states:

| Status | Meaning |
|---|---|
| `written` | Downloaded, checksum verified, parsed, published. |
| `skipped` | Already done — receipt present and its Parquet still on disk. |
| `unpublished` | 404, **outside** the dataset's published window. Expected; not a failure. |
| `missing` | 404, **inside** the published window. A real hole — investigate. |
| `failed` | Downloaded but could not be used. The error is printed in full. |
| `unavailable` | The dataset is not published in bulk at all. Only `liquidationSnapshot`. |

`unpublished` versus `missing` is the distinction most worth understanding. Both are 404s.
The first is the documented edge of a dataset's history; the second is a hole in the middle
of it, and `markPriceKlines` has 56 of them (finding F7). Collapsing the two would make a
discontinued dataset look like a flaky network.

---

## Resuming an interrupted run

**Run the same command again.** That is all.

Ctrl+C is cooperative: in-flight downloads check a stop flag on every chunk, so workers
unwind in seconds rather than finishing a quarter-gigabyte transfer nobody is waiting for.
The report comes back marked `INTERRUPTED` with the untried periods counted and a non-zero
exit.

Nothing can be corrupted by stopping at any point, because completion is a receipt written
*last*:

```
<lake>/_ingest/<dataset>/symbol=<SYM>/<period>.json
```

Published atomically, and only after the Parquet file it describes has itself been
published atomically. So:

- **crash mid-download** → a `.zip.part` in `_ingest/tmp/`; nothing in the lake;
- **crash mid-write** → a dot-prefixed `.tmp` in the partition; no receipt; no reader sees it;
- **crash after the write, before the receipt** → a complete Parquet and no receipt, so the
  archive is fetched again and the same deterministic path is atomically replaced.

There is no interleaving in which a half-written partition reads as done, and the recovery
action is always the same idempotent re-ingest. Re-running a completed range costs one
`stat` per period and prints `skipped`.

A hard kill or a power cut can leave a `.zip.part` behind. The next `ingest` sweeps
anything in `_ingest/tmp/` older than 24 hours and says so; the age threshold is what makes
that safe to do while another backfill is running.

**`--force` ignores receipts and re-fetches.** Use it when you have changed a parser and
need the rows rebuilt. It does *not* relax the collector-overlap check — see below.

### Two things that stop a resume

**`PartitionConflict`.** The partition already holds `part-*.parquet` files written by the
live collector. Bulk publishes `data.parquet`, so nothing would be overwritten: both files
would survive and every overlapping row would be counted twice by the `**/*.parquet` glob
that query, gaps and manifest all use — silently, because nothing downstream can tell a
doubled partition from a healthy multi-part collector day. So it is refused. Bulk archives
are meant to fill history *behind* the collector's start: narrow `--end` to stop before the
day the collector began, or move the collector's files out of that partition first.

**The receipt version.** Receipts carry a `version`; if the on-disk shape of a partition
changes, `RECEIPT_VERSION` is bumped and older receipts are ignored rather than trusted.
The symptom is a resume that re-downloads everything. That is intended — a partition
written under different rules that claims to be current is worse than a re-download.

---

## Reading a gap report

```bash
perplab gaps --symbol BTCUSDT --dataset markPriceKlines --dataset klines \
    --start 2021-01-01 --end 2021-03-31
```
```
Gap report -- BTCUSDT  2021-01-01T00:00:00.000Z .. 2021-04-01T00:00:00.000Z
  2 gap(s), 2 unexplained, 39d 00h 00m 00s missing in total

  markPriceKlines  (2 gap(s))
     34d 00h 00m 00s  2021-01-18T00:00:00.000Z .. 2021-02-21T00:00:00.000Z  MISSING_BARS
        48960 bar(s) absent
        UNEXPLAINED
      5d 00h 00m 00s  2021-03-22T00:00:00.000Z .. 2021-03-27T00:00:00.000Z  MISSING_BARS
        7200 bar(s) absent
        UNEXPLAINED

  coverage
    markPriceKlines    73440/129600 bars      2021-01-01T00:00:00.000Z .. 2021-03-31T23:59:00.000Z
        73440 zero-volume bar(s) -- data, not gaps
    klines             129600/129600 bars     2021-01-01T00:00:00.000Z .. 2021-03-31T23:59:00.000Z
        59 zero-volume bar(s) -- data, not gaps

2 unexplained gap(s). Phase 1b's exit criterion is that this is zero.
```

That is a real report over real data, and it contains four things worth learning to read.

**Read the coverage block first, not the gap list.** "No gaps" over a dataset with zero
rows is not good news, and a gap list cannot tell you the difference. `129600/129600 bars`
is what complete looks like. Anything else, gaps or no gaps, is a question.

**`zero-volume bars -- data, not gaps` is not a warning.** Binance publishes flat
placeholder bars through quiet periods rather than omitting them, so a zero-volume bar is
an observation and an absent bar is a hole. The count is printed so you can confirm they
were counted as data. `markPriceKlines` shows *every* bar as zero-volume because a computed
mark price has no volume concept — that is correct and permanent.

The uncomfortable corollary: Binance also publishes flat bars through **matched-engine
halts**, so the missing-bar rule finds nothing during a real outage. There are 13 such runs
in BTCUSDT's history, the longest 74 minutes. See finding F10; there is no rule for them
yet.

**`UNEXPLAINED` means different things for bulk and collector datasets.** For a collector
dataset it is the real signal: the collector writes a heartbeat every 10 s whether or not
anything happened, so a hole with no `CONNECT`/`RECONNECT`/`DISCONNECT`/`RESTART`/`STALE`/
`SHUTDOWN` record beside it is evidence the process was down for a reason nobody recorded.
That is the Phase 1b exit criterion. For a **bulk** dataset there is nothing that could ever
explain a gap, so `UNEXPLAINED` is the permanent, correct answer and the two above are
Binance not having published those days. Do not go looking for a bug.

**`gaps` exits non-zero if anything is unexplained**, which is what makes it usable from a
script. On a bulk-only lake that means the default invocation — every registered dataset —
will fail, loudly, because the collector-only datasets have no rows. That is deliberate: a
report that quietly omitted them would be indistinguishable from one that found them
healthy. Pass `--dataset` to narrow it to what you actually ingested.

### The one dependency worth knowing

The tick rules (`aggTrades`, `bookTicker`, `depth20`) test for inter-record silence **only
during minutes where klines show non-zero volume**. Without that cross-check, a genuinely
quiet market reads as a collector dropout. It means **tick gap detection depends on klines
being ingested for the same range**, and when they are not, the report says so under
`not evaluated` rather than guessing.

Ingest klines first. The default dataset order already does.

---

## The manifest, and what it is for

```bash
perplab manifest --symbol BTCUSDT --start 2024-03-24 --end 2024-03-30 \
    --gaps --out run-manifest.json
```
```
symbols          : BTCUSDT
range            : 2024-03-24 .. 2024-03-30 (inclusive)
fill_model_tier  : BOOK_TICKER
flags            : BRACKETS_APPROXIMATE, FILTERS_APPROXIMATE
gaps recorded    : 2
datasets:
  aggTrades                 7 files      10,893,232 rows  a1affbaa8e97
  bookTicker                7 files      91,449,747 rows  6190479edc14
  funding                  79 files           7,212 rows  2353d6f77cde
  klines_1m                31 files          44,640 rows  c319df61c6aa
  markPriceKlines          31 files          44,640 rows  ea20da214f59
  metrics                 366 files         105,281 rows  124b02787f0b
```

**It answers one question: "why doesn't this backtest match the one I ran last month?"**
Six months from now the lake has been backfilled further, re-downloaded after a corrupt
archive, and extended by the collector. The same strategy over the same dates prints a
different curve. Without a manifest that is an investigation. With one it is a diff.

```bash
perplab manifest --symbol BTCUSDT --start 2024-03-24 --end 2024-03-30 \
    --diff run-manifest.json          # non-zero exit if the lake has moved
```

Three things about the output are not obvious:

- **The `sha256` is over `(path, size, mtime_ns)`, not the file contents.** Hashing hundreds
  of gigabytes on every run would make the check expensive enough that people turn it off,
  and a check that is off detects nothing. Stat metadata catches every realistic mutation:
  a re-ingest moves mtime, a compaction changes the path set, a truncated download changes
  size. Content integrity is Binance's `.CHECKSUM` at ingest time, which is where it belongs.
- **The file counts are per *partition*, not per day.** This range is seven days, but
  `funding` shows all 79 files because funding is a single unpartitioned dataset, `metrics`
  shows 366 because it is year-partitioned, and `klines_1m` shows 31 because March 2024 is
  one monthly partition. The manifest fingerprints whole partitions that overlap the range.
  A change anywhere in March 2024 will show up in a manifest for one week of it. That is
  correct — those files are what the query actually reads — but it surprises people.
- **`fill_model_tier` is derived from what is on disk, never passed in.** Here it is
  `BOOK_TICKER` because this is the one week `bookTicker` exists for. Ask for almost any
  other week and you will get `TRADE_ONLY`, because bulk `bookTicker` covers 320 days out
  of 6.5 years (finding F1). A silent fill-model downgrade is the thing spec §4.2 exists to
  prevent, so it is observed rather than assumed.

`--gaps` runs detection first and folds the result in. That is not merely storage: coverage
is judged at partition granularity, so a day holding one part-file counts as covered even
if the collector was down for nine hours of it. An unexplained gap in a tier's input
dataset demotes the tier and raises `FILL_TIER_LIMITED_BY_GAPS`. Without `--gaps` you get
the coverage-only answer, which is the honest one for a caller that has not looked.

The two gaps recorded above are `collectorEvents` ("no collector records at all over the
range") and `depth20` ("no records for 7d while 10080 kline bars traded") — the collector
did not exist in March 2024. The tier stays `BOOK_TICKER` because neither is an input to
it. A gap in `aggTrades` or `bookTicker` over the same range *would* have demoted it, which
is the behaviour to check for if a tier ever comes back higher than you expected.

`--diff` exits non-zero when the lake has moved. Spec §4.6 requires a recomputation that
differs to "warn loudly"; a non-zero exit is what makes that survive being run from a
script rather than read by a person.

---

## When something goes wrong

**`ChecksumMismatch`** — the downloaded bytes do not hash to the published digest. Nothing
was written; the archive was not even decoded, because a corrupt zip that still inflates
yields plausible-looking rows and those are worse than no rows. Re-run; it is almost always
a bad transfer. If it repeats on the same archive, the object on the mirror is bad and
there is nothing this end can do.

**`MalformedChecksum: CHECKSUM names X but the archive requested was Y`** — the mirror
served a checksum belonging to a different archive. Verifying against it would prove the
wrong file intact. Re-run.

**`MalformedArchive: ... line N: ValueError: value '4.15119902...' has more than 8 decimal
places`** — the archive is fine and the lake's numeric precision is not enough for it. This
is `metrics`, 563 days of it, mostly across 2021-12-01 .. 2023-07-09. Nothing was written
for those days and nothing will be until someone decides how to widen the seam. See finding
F8. **Do not "fix" this by rounding**; `to_scaled` refusing to round is the guarantee the
whole accounting layer rests on.

**`MalformedArchive: ... resolves to partition X but the archive's period resolves to Y`** —
the archive contains rows that do not belong to the date it is named for. Observed exactly
once, on `BTCUSDT-metrics-2025-12-31`, which is shifted one five-minute slot and spills its
last row into 2026 (finding F11). Writing it anyway would file real rows where no query for
them would look. There is no workaround and there should not be one.

**A dataset silently missing from the run.** It is not silent — check the note printed at
the end of every `ingest`. `liquidationSnapshot` is 404 at both the daily and monthly paths
and is excluded from the default set for that reason (finding F2); the live
`!forceOrder@arr` collector stream is the only source of liquidation data that exists.

**A run that seems to hang on Ctrl+C.** It should not; workers check the stop flag per
chunk and the main thread joins in short slices. If it does, you are looking at a
bookTicker archive mid-transfer and it will unwind within a chunk.

---

## Potholes that are not bugs

Six months from now these will each cost you twenty minutes if you have not read them.

1. **`metrics` has a 19-month hole** and always will until the numeric seam decision is
   made: 562 of the 586 days in 2021-12-01 .. 2023-07-09, plus 2025-12-31. Twenty-four days
   inside that window *did* parse, so a plain range check will not tell you which days you
   have. Finding F8.
2. **`metrics` before 2021-05-22 is double-counted.** 263 consecutive days (2020-09-01 ..
   2021-05-21) are published with every row duplicated, plus four later days carrying one
   duplicate each — 267 in all, 75,259 excess rows. The archives pass their own checksums
   and no gap rule runs over `metrics`, so nothing in the pipeline reports it. Any mean or
   sum over that era is wrong by a factor of two today. Finding F9.
3. **`markPriceKlines` 404s inside its own coverage window** — 56 days, 34 of them
   consecutive. `klines` over the same range is complete, so a range that looks safe may
   still be missing mark prices. Finding F7.
4. **`bookTicker` bulk stops on 2024-03-30 and never resumes.** Spec §4.2 calls it "full
   history" and builds the fill model on it. It is 320 days. Finding F1.
5. **`bookDepth` has never been ingested by anyone**, and its per-column types were inferred
   from column names rather than verified against a real row. Spot-check one archive against
   `bulk_layout.parse_book_depth_row` before trusting a backfill of it.
6. **CSV header rows are conditional per file.** You will not hit this — it is handled — but
   if you write a new parser, sniff per file. There is no era rule that is correct.
   Finding F5.

All findings live in [DATA_AVAILABILITY.md](DATA_AVAILABILITY.md).
