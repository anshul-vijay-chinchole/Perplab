/**
 * Data & Feed (spec 10.3): Coverage, Refresh, Collector, Exchange Connection, Feed.
 *
 * The Feed was built in Phase 7; the rest land here. Coverage answers "what can I actually
 * backtest", Refresh is how a gap in that answer gets filled and — more importantly — how
 * what the fill *did* gets reported, Collector is a read-only window onto the recorder, and
 * Exchange Connection is the only place a key is ever entered, with spec 11's promise about
 * that key stated on the form rather than in documentation nobody reads.
 */

import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import {
  ApiError,
  api,
  formatDuration,
  formatTime,
  money,
  type CollectorStatus,
  type DataUpdateKind,
  type IngestConflict,
  type IngestJob,
  type RefreshPlan,
} from '../api'
import { useDebouncedValue } from '../lib/hooks'
import { exchangeStatusQuery } from '../lib/queries'
import { useUi } from '../store'
import { FeedPanel } from './Feed'

export function DataAndFeed() {
  // One symbol for the whole column. Coverage answers "what is missing" and Refresh is the
  // thing that fills it, so two independent symbol inputs would let someone read one
  // symbol's gaps and refresh another's — with both panels on screen looking agreeable.
  const [symbol, setSymbol] = useState('BTCUSDT')
  return (
    <main className="flex flex-1" style={{ minHeight: 0 }}>
      <div className="pl-scroll" style={{ width: 440, borderRight: '1px solid var(--border)', minWidth: 0 }}>
        <CoveragePanel symbol={symbol} onSymbol={setSymbol} />
        <RefreshPanel symbol={symbol} />
        <CollectorPanel />
        <ExchangePanel />
      </div>
      <FeedPanel />
    </main>
  )
}

/** Which datasets decide what, stated in the panel rather than assumed known. */
const DATASET_NOTES: Record<string, string> = {
  klines: 'price bars — every backtest needs these',
  markPriceKlines: 'mark price — liquidation and funding are priced off this, and it may never be derived from klines (spec 3.4)',
  aggTrades: 'trade prints — the TRADE_ONLY fill tier',
  bookTicker: 'top of book — the BOOK_TICKER tier, and the long-history tick source',
  depth20: 'L2 ladder — the BOOK_WALK tier; only exists from the day the collector started',
}

/** A dataset whose coverage reaches within this of "now" is drawn in the live colour;
 *  anything staler is drawn muted — the bar says at a glance whether the collector kept up. */
const FRESH_WITHIN_MS = 6 * 60 * 60 * 1000

/** A segment narrower than this still renders as a visible sliver rather than as zero
 *  pixels. Seven days inside a six-year window is 0.29% of the track — under a pixel at any
 *  realistic width — and a day of real data that draws as nothing is the same lie as a hole
 *  that draws as data. The exact day counts are printed beneath every bar, so the floor
 *  distorts a shape whose record is stated in full immediately below it. */
const MIN_SEGMENT_PCT = 0.45

/** One dataset drawn as a horizontal track: **one filled block per contiguous run of days
 *  that actually holds data**, positioned inside the overall [min start, max end] window.
 *
 *  This used to draw a single block from `start_ms` to `end_ms`, which asserted that
 *  everything between the first row and the last row was present. For `bookTicker` that was
 *  wrong by two orders of magnitude — 12 days of data inside an 865-day span, rendered as a
 *  solid bar reading as two and a half years of continuous tick history, in the one panel
 *  someone consults to find out what they can backtest over. `markPriceKlines` was quietly
 *  hiding 56 missing days the same way. The bar is still a shape and not the record, but a
 *  shape may not claim what nobody measured. */
function CoverageTrack({
  segments,
  windowStart,
  windowEnd,
  fresh,
}: {
  segments: { start_ms: number; end_ms: number }[]
  windowStart: number
  windowEnd: number
  fresh: boolean
}) {
  const span = Math.max(1, windowEnd - windowStart)
  return (
    <div
      style={{
        position: 'relative',
        height: 10,
        borderRadius: 3,
        background: 'var(--surface-2)',
        border: '1px solid var(--border)',
        overflow: 'hidden',
      }}
    >
      {segments.map((segment) => {
        const left = Math.max(0, Math.min(100, ((segment.start_ms - windowStart) / span) * 100))
        const width = Math.min(
          Math.max(MIN_SEGMENT_PCT, ((segment.end_ms - segment.start_ms) / span) * 100),
          100 - left,
        )
        return (
          <div
            key={segment.start_ms}
            style={{
              position: 'absolute',
              top: 0,
              bottom: 0,
              left: `${left}%`,
              width: `${width}%`,
              background: fresh
                ? 'color-mix(in srgb, var(--pos) 55%, var(--surface-3))'
                : 'color-mix(in srgb, var(--text-mute) 55%, var(--surface-3))',
            }}
          />
        )
      })}
    </div>
  )
}

/** The day count in words, beside the bounds. The bar can only ever be a shape — this is the
 *  record, and it is what makes a hole a number rather than a thing you had to notice.
 *
 *  Continuous coverage says so explicitly rather than staying silent, because "12 of 865
 *  days" is only alarming if "2,410 days, continuous" is what the healthy case looks like.
 *  Silence would leave the reader unable to tell a complete dataset from an unmeasured one. */
function CoveredDays({
  range,
}: {
  range: { days_covered: number; days_spanned: number; segments: { start_ms: number }[] }
}) {
  if (range.days_spanned <= 0) return null
  const whole = range.days_covered >= range.days_spanned
  const gaps = Math.max(0, range.segments.length - 1)
  return (
    <span style={{ color: whole ? 'var(--text-mute)' : 'var(--warn)' }}>
      {' · '}
      {whole
        ? `${range.days_covered.toLocaleString('en-US')} days, continuous`
        : `${range.days_covered.toLocaleString('en-US')} of ${range.days_spanned.toLocaleString(
            'en-US',
          )} days · ${gaps === 1 ? '1 gap' : `${gaps} gaps`}`}
    </span>
  )
}

/** An empty track for a dataset with nothing in it — the absence is drawn, not skipped. */
function EmptyTrack() {
  return (
    <div
      style={{
        height: 10,
        borderRadius: 3,
        background: 'var(--surface-2)',
        border: '1px solid var(--border)',
      }}
    />
  )
}

function CoveragePanel({ symbol, onSymbol }: { symbol: string; onSymbol: (next: string) => void }) {
  // Debounced + placeholder-kept: the raw input fired one coverage request per keystroke
  // ("B", "BT", "BTC", …), each a fresh cache entry that flashed the skeleton. The query
  // waits for the typing to settle and keeps the previous answer on screen while the new
  // one is in flight — the panel changes when the data does, not while it is being asked.
  const settled = useDebouncedValue(symbol, 400)
  const coverage = useQuery({
    queryKey: ['coverage', settled],
    queryFn: () => api.coverage(settled),
    enabled: settled.trim() !== '',
    placeholderData: keepPreviousData,
    retry: 0,
  })

  // The overall window every track is positioned within: min start to max end across all
  // datasets that have anything at all.
  const datasets = coverage.data == null ? [] : Object.entries(coverage.data.datasets)
  let windowStart = Infinity
  let windowEnd = -Infinity
  for (const [, range] of datasets) {
    if (range.start_ms != null) windowStart = Math.min(windowStart, range.start_ms)
    if (range.end_ms != null) windowEnd = Math.max(windowEnd, range.end_ms)
  }
  const hasWindow = Number.isFinite(windowStart) && Number.isFinite(windowEnd)
  const now = Date.now()

  return (
    <section className="p-3" style={{ borderBottom: '1px solid var(--border)' }}>
      <div className="flex items-center gap-2" style={{ marginBottom: 10 }}>
        <h2 className="pl-heading" style={{ margin: 0 }}>
          Coverage
        </h2>
        <input
          className="pl-input mono"
          value={symbol}
          onChange={(event) => onSymbol(event.target.value.toUpperCase())}
          style={{ width: 120, marginLeft: 'auto' }}
        />
      </div>

      {coverage.isError ? (
        <p style={{ fontSize: 12, color: 'var(--warn)' }}>
          {coverage.error instanceof ApiError ? coverage.error.message : String(coverage.error)}
        </p>
      ) : coverage.data == null ? (
        <div aria-hidden={true}>
          <div className="pl-skel" style={{ height: 11, width: '64%', marginBottom: 12 }} />
          {[0, 1, 2, 3].map((i) => (
            <div key={i} style={{ marginBottom: 12 }}>
              <div className="pl-skel" style={{ height: 11, width: 96 + i * 22, marginBottom: 5 }} />
              <div className="pl-skel" style={{ height: 10, borderRadius: 3 }} />
            </div>
          ))}
        </div>
      ) : (
        <>
          <p className="mono" style={{ fontSize: 11, color: 'var(--text-dim)', margin: '0 0 10px' }}>
            runnable range:{' '}
            {coverage.data.start_ms == null || coverage.data.end_ms == null ? (
              <span style={{ color: 'var(--warn)' }}>none — bars and marks do not overlap</span>
            ) : (
              <>
                {formatTime(coverage.data.start_ms)} → {formatTime(coverage.data.end_ms)}
                <span style={{ color: 'var(--text-mute)' }}>
                  {' '}
                  · {coverage.data.bars.toLocaleString('en-US')} bars
                </span>
              </>
            )}
          </p>

          {datasets.map(([dataset, range]) => {
            // Bar datasets carry `bars`, tick datasets carry `rows` — both in the typed
            // payload now, shown under whichever name arrived.
            const rows = range.bars ?? range.rows ?? null
            const start = range.start_ms
            const end = range.end_ms
            return (
              <div key={dataset} style={{ marginBottom: 12 }}>
                <div className="flex items-baseline gap-2" style={{ marginBottom: 4 }}>
                  <span className="mono" style={{ fontSize: 11 }} data-tip={DATASET_NOTES[dataset]}>
                    {dataset}
                  </span>
                  {rows != null ? (
                    <span
                      className="mono"
                      style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--text-mute)' }}
                    >
                      {rows.toLocaleString('en-US')} rows
                    </span>
                  ) : null}
                </div>
                {start != null && end != null && hasWindow ? (
                  <>
                    <CoverageTrack
                      segments={range.segments}
                      windowStart={windowStart}
                      windowEnd={windowEnd}
                      fresh={now - end <= FRESH_WITHIN_MS}
                    />
                    <div className="mono" style={{ fontSize: 10.5, color: 'var(--text-dim)', marginTop: 3 }}>
                      {formatTime(start)} → {formatTime(end)}
                      <CoveredDays range={range} />
                    </div>
                  </>
                ) : (
                  <>
                    <EmptyTrack />
                    <div className="mono" style={{ fontSize: 10.5, color: 'var(--text-mute)', marginTop: 3 }}>
                      no data
                    </div>
                  </>
                )}
              </div>
            )
          })}

          <p style={{ fontSize: 10, color: 'var(--text-mute)', margin: '6px 0 0' }}>
            The runnable range is the intersection of bars and marks only. The tick
            datasets decide which <i>fill tier</i> a range can reach, which is a different
            question from whether it can run at all — a range with no depth still runs, at
            a lower tier, and says so.
          </p>
          <p style={{ fontSize: 10, color: 'var(--text-mute)', margin: '4px 0 0' }}>
            Each bar is filled only over days that hold data, so a dataset spanning years
            while holding a handful of days reads as the handful it is. Coverage is counted
            by day: a day with data is drawn whole, and a partial day is not distinguished
            from a complete one. Whether the rows <i>within</i> a covered day have holes is a
            separate question, and the gap report is what answers it.
          </p>
        </>
      )}
    </section>
  )
}

/* ---------------------------------------------------------------------- refresh */

/** The four statuses an ingest job can no longer leave. Same set, same reasoning, as
 *  `Lab.tsx` — including `lost`, which is the absence of a report rather than a failure. */
const TERMINAL = new Set(['done', 'failed', 'cancelled', 'lost'])

/** Bytes for display, or `null` when there is no number to display.
 *
 *  Returning `null` rather than `'0 B'` or `'—'` is deliberate: every call site is then
 *  forced to write, in words, what an absent estimate means. An unknown download size
 *  rendered as `0 B` is the confirmation dialogue lying about the one figure it exists to
 *  disclose. */
function formatBytes(value: number | null | undefined): string | null {
  if (value == null || !Number.isFinite(value)) return null
  if (value < 1024) return `${Math.round(value)} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let scaled = value / 1024
  let unit = 0
  while (scaled >= 1024 && unit < units.length - 1) {
    scaled /= 1024
    unit += 1
  }
  return `${scaled.toFixed(scaled >= 100 ? 0 : 1)} ${units[unit]}`
}

/**
 * Refresh (spec 10.3).
 *
 * Three steps, and the third is the one that matters. The plan is asked for first so the
 * download size can be stated before anything is downloaded; the job is then watched while
 * it runs; and when it finishes the server's own verdict is rendered — `attention` never
 * as a tick, and every conflict's measured sentence in full. A refresh that quietly
 * declined half its archives and reported a green tick is worse than no refresh at all,
 * because the lake then has a hole nobody is looking for.
 */
function RefreshPanel({ symbol }: { symbol: string }) {
  const queryClient = useQueryClient()
  const notify = useUi((s) => s.notify)
  // The plan awaiting a yes. Holds the plan itself rather than just a flag: what is
  // confirmed is that exact plan, and the job is started for `plan.symbol` — not for
  // whatever the input above says by the time the button is pressed.
  const [pendingPlan, setPendingPlan] = useState<{
    kind: DataUpdateKind
    plan: RefreshPlan
    conflicts: string[]
  } | null>(null)
  // Every refusal the server wrote, kept verbatim. A 409 here is not a bug to be swallowed
  // — it is the server explaining that the collector is reconnecting, or that another
  // refresh holds the lock, or that the disk will not take it.
  const [refusal, setRefusal] = useState<string | null>(null)
  const [watchedId, setWatchedId] = useState<number | null>(null)

  const updates = useQuery({
    queryKey: ['data-updates'],
    queryFn: () => api.dataUpdates(20),
    refetchInterval: (query) =>
      (query.state.data?.jobs ?? []).some((job) => !TERMINAL.has(job.status)) ? 2000 : 15000,
    retry: 0,
  })
  const jobs = updates.data?.jobs ?? []
  const active = jobs.find((job) => job.status === 'queued' || job.status === 'running') ?? null

  // Watch the job this panel started; failing that, whatever is running (a refresh started
  // in another tab, or before a reload); failing that, the newest one, so the last result
  // survives a page refresh instead of vanishing with the component.
  const detailId = watchedId ?? active?.id ?? null
  const detail = useQuery({
    queryKey: ['data-update', detailId],
    queryFn: () => api.dataUpdate(detailId as number),
    enabled: detailId != null,
    // Same shape as the Lab job poll: 2s while the job can still change, and stopped dead
    // the moment it cannot. A terminal job polled forever is a request per two seconds
    // asking a question whose answer can no longer differ.
    refetchInterval: (query) =>
      query.state.data && TERMINAL.has(query.state.data.job.status) ? false : 2000,
    retry: 0,
  })
  const newest =
    jobs.length > 0 ? jobs.reduce((a, b) => (b.created_ms > a.created_ms ? b : a)) : null
  const job = detail.data?.job ?? newest

  const start = useMutation({
    mutationFn: (args: { kind: DataUpdateKind; symbol: string }) =>
      api.startDataUpdate(args.kind, args.symbol),
    onSuccess: (result) => {
      setPendingPlan(null)
      setRefusal(null)
      setWatchedId(result.job.id)
      // Seed the detail cache with the job the server just handed back. Without this the
      // panel has no job it knows is running until the list poll lands, and for that gap
      // both buttons re-enable — offering a second refresh that the server would refuse
      // and, worse, reading as though the first had not started.
      queryClient.setQueryData(['data-update', result.job.id], result)
      notify(`Update queued — ${result.job.kind} ${result.job.symbol}`)
      void queryClient.invalidateQueries({ queryKey: ['data-updates'] })
    },
    onError: (err) => {
      // The dialogue closes and the reason takes its place. Leaving the dialogue up under
      // an error invites a second click on a button that has already been refused.
      setPendingPlan(null)
      setRefusal(err instanceof ApiError ? err.message : String(err))
    },
  })

  const plan = useMutation({
    mutationFn: (args: { kind: DataUpdateKind; symbol: string }) =>
      api.planDataUpdate(args.kind, args.symbol),
    onMutate: () => setRefusal(null),
    onSuccess: (result, args) => {
      // Confirmation only when the server asks for one. A dialogue on every refresh is a
      // dialogue nobody reads, which is how the one that mattered gets clicked through.
      if (result.plan.needs_confirmation) {
        setPendingPlan({ kind: args.kind, plan: result.plan, conflicts: result.conflicts_predicted })
      } else {
        start.mutate({ kind: args.kind, symbol: result.plan.symbol })
      }
    },
    onError: (err) => setRefusal(err instanceof ApiError ? err.message : String(err)),
  })

  const cancel = useMutation({
    mutationFn: (id: number) => api.cancelDataUpdate(id),
    onSuccess: () => {
      notify('Stop requested — the worker stops after the archive it is on', 'warn')
      void queryClient.invalidateQueries({ queryKey: ['data-update'] })
      void queryClient.invalidateQueries({ queryKey: ['data-updates'] })
    },
    onError: (err) => setRefusal(err instanceof ApiError ? err.message : String(err)),
  })

  const trimmed = symbol.trim()
  // Both buttons, not just the one that was pressed: the refusal for a second concurrent
  // refresh is the same 409 either way, and offering a button that can only be refused is
  // an offer that is not real. The watched job counts as well as the list's — the list is
  // up to one poll behind the job this panel just started.
  const busy =
    active != null ||
    (job != null && !TERMINAL.has(job.status)) ||
    plan.isPending ||
    start.isPending

  return (
    <section className="p-3" style={{ borderBottom: '1px solid var(--border)' }}>
      <h2 className="pl-heading" style={{ margin: '0 0 10px' }}>
        Refresh
      </h2>

      <div className="flex gap-2" style={{ marginBottom: 8 }}>
        <button
          type="button"
          className="pl-btn"
          disabled={busy || trimmed === ''}
          onClick={() => plan.mutate({ kind: 'candles', symbol: trimmed })}
        >
          Update candles
        </button>
        <button
          type="button"
          className="pl-btn"
          disabled={busy || trimmed === ''}
          onClick={() => plan.mutate({ kind: 'trades', symbol: trimmed })}
        >
          Update trades
        </button>
        {plan.isPending ? (
          <span style={{ alignSelf: 'center', fontSize: 11, color: 'var(--text-mute)' }}>
            checking what is missing…
          </span>
        ) : null}
      </div>

      <p style={{ fontSize: 10, color: 'var(--text-mute)', margin: '0 0 8px' }}>
        Backfills <span className="mono">{trimmed === '' ? 'the symbol above' : trimmed}</span>{' '}
        from Binance's bulk archive. The archive is published a day in arrears, so it fills
        history — it does not catch the lake up to now. Where the archive and a period the
        collector already recorded disagree, the disagreement is reported rather than
        resolved.
      </p>

      {refusal != null ? (
        <div
          className="pl-panel p-2"
          style={{
            marginBottom: 8,
            borderColor: 'color-mix(in srgb, var(--warn) 45%, var(--border))',
          }}
        >
          <div className="flex items-baseline gap-2">
            <span className="sev-warning" style={{ fontSize: 11 }}>
              △
            </span>
            {/* The server's sentence, unedited. It says which of the four refusals this is
                and what to do about it; a summary here would delete the reason. */}
            <p
              className="mono"
              style={{
                fontSize: 11,
                color: 'var(--warn)',
                margin: 0,
                whiteSpace: 'pre-wrap',
                flex: 1,
                minWidth: 0,
              }}
            >
              {refusal}
            </p>
            <button
              type="button"
              className="pl-btn"
              style={{ height: 20, fontSize: 10, padding: '0 6px' }}
              onClick={() => setRefusal(null)}
            >
              Dismiss
            </button>
          </div>
        </div>
      ) : null}

      {job != null ? (
        <JobCard
          job={job}
          onCancel={() => cancel.mutate(job.id)}
          cancelling={cancel.isPending}
        />
      ) : updates.isPending ? (
        <div className="pl-skel" style={{ height: 52 }} aria-hidden={true} />
      ) : null}

      {pendingPlan != null ? (
        <ConfirmPlan
          kind={pendingPlan.kind}
          plan={pendingPlan.plan}
          conflicts={pendingPlan.conflicts}
          submitting={start.isPending}
          onCancel={() => setPendingPlan(null)}
          onConfirm={() =>
            start.mutate({ kind: pendingPlan.kind, symbol: pendingPlan.plan.symbol })
          }
        />
      ) : null}
    </section>
  )
}

/** The confirmation step, shown only when `plan.needs_confirmation`.
 *
 *  It states two things before the download starts: how many archives, and how large. The
 *  size is the reason this dialogue exists, so a `null` estimate is stated as unknown and
 *  drawn in the warning colour — it is not rounded to zero and it is not left out, because
 *  a missing figure read as a small one is how someone agrees to fifty gigabytes. */
function ConfirmPlan({
  kind,
  plan,
  conflicts,
  submitting,
  onCancel,
  onConfirm,
}: {
  kind: DataUpdateKind
  plan: RefreshPlan
  conflicts: string[]
  submitting: boolean
  onCancel: () => void
  onConfirm: () => void
}) {
  // Esc closes (spec 10.4). Closing is the safe direction — it downloads nothing.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onCancel()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onCancel])

  const size = formatBytes(plan.estimated_bytes)
  const archives = plan.archives.toLocaleString('en-US')

  return (
    <div
      className="pl-scrim"
      style={{ zIndex: 70, paddingTop: 80 }}
      onClick={onCancel}
      role="dialog"
      aria-modal="true"
    >
      <div
        className="pl-modal p-4 flex flex-col gap-3"
        style={{ width: 'min(560px, calc(100vw - 32px))', textAlign: 'left' }}
        onClick={(event) => event.stopPropagation()}
      >
        <h2 style={{ margin: 0, fontSize: 14 }}>
          Update {kind} for <span className="mono">{plan.symbol}</span>?
        </h2>

        <p style={{ margin: 0, fontSize: 12, color: 'var(--text-dim)' }}>
          This downloads <b className="mono">{archives}</b> archive
          {plan.archives === 1 ? '' : 's'}
          {size == null ? (
            <>
              {' '}
              of <b style={{ color: 'var(--warn)' }}>unknown total size</b> — the server could
              not estimate it, so this may be much larger than the archive count suggests.
            </>
          ) : (
            <>
              {' '}
              totalling roughly <b className="mono">{size}</b>.
            </>
          )}
        </p>

        {plan.datasets.length > 0 ? (
          <table className="pl-table" style={{ fontSize: 11 }}>
            <thead>
              <tr>
                <th>dataset</th>
                <th>range</th>
                <th style={{ textAlign: 'right' }}>archives</th>
                <th style={{ textAlign: 'right' }}>size</th>
              </tr>
            </thead>
            <tbody>
              {plan.datasets.map((dataset) => {
                const each = formatBytes(dataset.estimated_bytes)
                return (
                  <tr key={dataset.dataset}>
                    <td className="mono" style={{ color: 'var(--text)' }}>
                      {dataset.dataset}
                      {dataset.note != null && dataset.note !== '' ? (
                        <div style={{ fontSize: 10, color: 'var(--text-mute)', whiteSpace: 'pre-wrap' }}>
                          {dataset.note}
                        </div>
                      ) : null}
                    </td>
                    <td className="mono">
                      {dataset.start ?? '?'} → {dataset.end ?? '?'}
                      {dataset.periods.length > 0 ? (
                        <div style={{ fontSize: 10, color: 'var(--text-mute)' }}>
                          {dataset.periods.length.toLocaleString('en-US')} period
                          {dataset.periods.length === 1 ? '' : 's'}
                        </div>
                      ) : null}
                    </td>
                    <td className="mono" style={{ textAlign: 'right' }}>
                      {dataset.archives.toLocaleString('en-US')}
                    </td>
                    <td
                      className="mono"
                      style={{ textAlign: 'right', color: each == null ? 'var(--warn)' : undefined }}
                    >
                      {each ?? 'unknown'}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        ) : null}

        {plan.notes.length > 0 ? (
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 11, color: 'var(--text-dim)' }}>
            {plan.notes.map((note, index) => (
              <li key={index} style={{ whiteSpace: 'pre-wrap' }}>
                {note}
              </li>
            ))}
          </ul>
        ) : null}

        {conflicts.length > 0 ? (
          <div
            className="pl-panel p-2"
            style={{ borderColor: 'color-mix(in srgb, var(--warn) 45%, var(--border))' }}
          >
            <div className="pl-heading" style={{ color: 'var(--warn)', marginBottom: 4 }}>
              △ {conflicts.length} conflict{conflicts.length === 1 ? '' : 's'} predicted
            </div>
            {conflicts.map((line, index) => (
              <p
                key={index}
                className="mono"
                style={{ fontSize: 11, color: 'var(--text)', margin: index === 0 ? 0 : '5px 0 0', whiteSpace: 'pre-wrap' }}
              >
                {line}
              </p>
            ))}
            <p style={{ fontSize: 10, color: 'var(--text-mute)', margin: '6px 0 0' }}>
              These periods already hold recorded data that the archive disagrees with. The
              refresh reports the disagreement; it does not overwrite what is there.
            </p>
          </div>
        ) : null}

        <div className="flex gap-2">
          <button type="button" className="pl-btn pl-btn-primary" disabled={submitting} onClick={onConfirm}>
            {submitting ? 'Starting…' : `Download ${archives} archive${plan.archives === 1 ? '' : 's'}`}
          </button>
          <button type="button" className="pl-btn" disabled={submitting} onClick={onCancel}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  )
}

/** One refresh, live or finished.
 *
 *  The header carries the verdict, and the verdict is the server's: `attention` is drawn in
 *  the warning colour with its own glyph, and the tick is only ever reachable through
 *  `verdict === 'clean'`. A `done` job with no summary at all is its own third case —
 *  neither clean nor attention, because nothing was reported either way. */
function JobCard({
  job,
  onCancel,
  cancelling,
}: {
  job: IngestJob
  onCancel: () => void
  cancelling: boolean
}) {
  const terminal = TERMINAL.has(job.status)
  const summary = job.summary
  const attention = summary != null && summary.verdict === 'attention'
  const clean = summary != null && summary.verdict === 'clean'
  const bad = job.status === 'failed' || job.status === 'lost' || attention

  return (
    <div
      className="pl-panel p-2"
      style={{
        borderColor: bad
          ? 'color-mix(in srgb, var(--warn) 45%, var(--border))'
          : undefined,
      }}
    >
      <div className="flex items-baseline gap-2" style={{ fontSize: 11 }}>
        <span className="mono" style={{ color: 'var(--text)' }}>
          {job.kind} {job.symbol}
        </span>
        <span className="mono" style={{ color: 'var(--text-mute)' }}>
          #{job.id}
        </span>
        <span
          className="mono"
          style={{
            marginLeft: 'auto',
            color: attention
              ? 'var(--warn)'
              : job.status === 'failed' || job.status === 'lost'
                ? 'var(--down)'
                : clean
                  ? 'var(--pos)'
                  : 'var(--text-dim)',
          }}
        >
          {/* The one place a tick is printed, and it is behind `clean`. */}
          {job.status === 'done' && clean
            ? '✓ clean'
            : job.status === 'done' && attention
              ? '△ needs attention'
              : job.status === 'done'
                ? '△ finished with no summary'
                : job.status === 'failed'
                  ? '✕ failed'
                  : job.status === 'lost'
                    ? '✕ lost — the worker stopped reporting'
                    : job.status === 'cancelled'
                      ? 'cancelled'
                      : job.cancel_requested
                        ? 'stopping…'
                        : job.status}
        </span>
      </div>

      {!terminal ? (
        <>
          <div className="flex items-center gap-2" style={{ marginTop: 6 }}>
            <span className="pl-meter" style={{ flex: 1 }}>
              <span
                className="pl-meter-fill"
                style={{
                  width:
                    job.progress_total > 0
                      ? `${Math.min(100, (job.progress_done / job.progress_total) * 100)}%`
                      : '0%',
                  background: 'var(--info)',
                }}
              />
            </span>
            <span className="mono" style={{ fontSize: 11, color: 'var(--text-dim)', flex: 'none' }}>
              {job.progress_done.toLocaleString('en-US')}/
              {job.progress_total.toLocaleString('en-US')}
            </span>
            <button
              type="button"
              className="pl-btn"
              style={{ height: 22, fontSize: 10, padding: '0 8px', flex: 'none' }}
              disabled={cancelling || job.cancel_requested}
              onClick={onCancel}
            >
              {job.cancel_requested ? 'Stopping' : 'Stop'}
            </button>
          </div>
          <div className="mono" style={{ fontSize: 10, color: 'var(--text-mute)', marginTop: 4 }}>
            {job.status === 'queued'
              ? 'queued — the worker process is starting'
              : `running since ${job.started_ms == null ? '—' : formatTime(job.started_ms)}`}
          </div>
        </>
      ) : null}

      {job.error != null && job.error !== '' ? (
        <pre
          className="mono"
          style={{
            fontSize: 11,
            color: 'var(--down)',
            whiteSpace: 'pre-wrap',
            margin: '6px 0 0',
          }}
        >
          {job.error}
        </pre>
      ) : null}

      {summary != null ? <JobSummary summary={summary} /> : null}
    </div>
  )
}

/** The tallies, the conflicts and the notes. Everything the server measured, none of it
 *  averaged into a single reassuring line. */
function JobSummary({ summary }: { summary: NonNullable<IngestJob['summary']> }) {
  const downloaded = formatBytes(summary.bytes_downloaded)
  const counts: [string, number, boolean][] = [
    ['written', summary.written, false],
    ['skipped', summary.skipped, false],
    ['declined', summary.declined, summary.declined > 0],
    ['missing', summary.missing, summary.missing > 0],
    ['failed', summary.failed, summary.failed > 0],
  ]

  return (
    <div style={{ marginTop: 8 }}>
      <div className="flex flex-wrap gap-2" style={{ fontSize: 11 }}>
        {counts.map(([label, value, warn]) => (
          <span key={label} className="mono" style={{ color: warn ? 'var(--warn)' : 'var(--text-dim)' }}>
            {label} {value.toLocaleString('en-US')}
          </span>
        ))}
      </div>
      <div className="mono" style={{ fontSize: 10, color: 'var(--text-mute)', marginTop: 3 }}>
        {summary.rows.toLocaleString('en-US')} rows · {downloaded ?? 'download size not recorded'}
      </div>

      {summary.datasets.length > 0 ? (
        <table className="pl-table" style={{ fontSize: 11, marginTop: 8 }}>
          <thead>
            <tr>
              <th>dataset</th>
              <th style={{ textAlign: 'right' }}>written</th>
              <th style={{ textAlign: 'right' }}>declined</th>
              <th style={{ textAlign: 'right' }}>missing</th>
              <th style={{ textAlign: 'right' }}>failed</th>
            </tr>
          </thead>
          <tbody>
            {summary.datasets.map((row) => (
              <tr key={row.dataset}>
                <td className="mono" style={{ color: 'var(--text)' }}>
                  {row.dataset}
                </td>
                <td className="mono" style={{ textAlign: 'right' }}>
                  {row.written.toLocaleString('en-US')}
                </td>
                <td
                  className="mono"
                  style={{ textAlign: 'right', color: row.declined > 0 ? 'var(--warn)' : undefined }}
                >
                  {row.declined.toLocaleString('en-US')}
                </td>
                <td
                  className="mono"
                  style={{ textAlign: 'right', color: row.missing > 0 ? 'var(--warn)' : undefined }}
                >
                  {row.missing.toLocaleString('en-US')}
                </td>
                <td
                  className="mono"
                  style={{ textAlign: 'right', color: row.failed > 0 ? 'var(--down)' : undefined }}
                >
                  {row.failed.toLocaleString('en-US')}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}

      <ConflictList conflicts={summary.conflicts} />

      {summary.notes.length > 0 ? (
        <ul style={{ margin: '8px 0 0', paddingLeft: 18, fontSize: 11, color: 'var(--text-dim)' }}>
          {summary.notes.map((note, index) => (
            <li key={index} style={{ whiteSpace: 'pre-wrap' }}>
              {note}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  )
}

/**
 * Every conflict the refresh found, with its `note` printed in full.
 *
 * The note is the measured sentence — which hours the partition actually holds against
 * which hours the period nominally covers, and how much of the difference is real. It is
 * the entire evidence for the verdict, so it is rendered verbatim, in the reading colour
 * rather than the muted one, and never truncated.
 *
 * Collapsing: only a set of conflicts whose gaps were all *measured as exactly zero* may
 * start collapsed. A `null` gap is not zero — it means the overlap could not be measured at
 * all — and folding either kind away by default would hide the finding behind a click
 * nobody has a reason to make.
 */
function ConflictList({ conflicts }: { conflicts: IngestConflict[] }) {
  if (conflicts.length === 0) return null
  const mustShow = conflicts.some((conflict) => conflict.missing_ms == null || conflict.missing_ms !== 0)
  const body = (
    <>
      {conflicts.map((conflict, index) => (
        <div
          key={`${conflict.dataset}·${conflict.period}·${index}`}
          className="pl-panel p-2"
          style={{
            marginTop: 6,
            background: 'var(--surface-2)',
            borderColor: 'color-mix(in srgb, var(--warn) 40%, var(--border))',
          }}
        >
          <div className="flex items-baseline gap-2 mono" style={{ fontSize: 11 }}>
            <span style={{ color: 'var(--text)' }}>{conflict.dataset}</span>
            <span style={{ color: 'var(--text-dim)' }}>{conflict.period}</span>
            <span
              style={{
                marginLeft: 'auto',
                color: conflict.missing_ms == null ? 'var(--warn)' : 'var(--text-dim)',
              }}
            >
              {/* `null` is "could not be measured", and says so. Rendering it as 0 — or as
                  nothing — reports an unmeasured gap as an absent one. */}
              {conflict.missing_ms == null
                ? '△ gap could not be measured'
                : `${formatDuration(conflict.missing_ms)} missing`}
            </span>
          </div>
          <p
            className="mono"
            style={{
              fontSize: 11,
              color: 'var(--text)',
              margin: '5px 0 0',
              whiteSpace: 'pre-wrap',
              lineHeight: 1.55,
            }}
          >
            {conflict.note}
          </p>
          <div className="mono" style={{ fontSize: 10, color: 'var(--text-mute)', marginTop: 4 }}>
            collector rows:{' '}
            {conflict.collector_rows == null
              ? 'not counted'
              : conflict.collector_rows.toLocaleString('en-US')}
          </div>
        </div>
      ))}
    </>
  )

  const heading = `${conflicts.length} conflict${conflicts.length === 1 ? '' : 's'}`
  if (!mustShow) {
    return (
      <details style={{ marginTop: 8 }}>
        <summary className="pl-heading" style={{ cursor: 'pointer', color: 'var(--text-mute)' }}>
          {heading} — every gap measured at zero
        </summary>
        {body}
      </details>
    )
  }
  return (
    <div style={{ marginTop: 8 }}>
      <div className="pl-heading" style={{ color: 'var(--warn)' }}>
        △ {heading}
      </div>
      {body}
    </div>
  )
}

/* -------------------------------------------------------------------- collector */

/** A heartbeat older than this is drawn in the warning colour. The age itself is always
 *  printed beside the dot — the colour is a hint about a number that is on screen, never a
 *  substitute for it. */
const HEARTBEAT_FRESH_MS = 90 * 1000

/**
 * Collector status — read only, on purpose.
 *
 * Two things are deliberately absent, and both absences are load-bearing:
 *
 * 1. **No start or stop control.** The collector is recording depth and book data that
 *    exists nowhere else — Binance does not publish it in the bulk archive, so a minute not
 *    recorded is a minute that can never be obtained. A stop button one misclick from the
 *    rest of this page would trade a permanent hole in the lake for a convenience.
 *
 * 2. **No completion figure — no bar, no percentage, no target.** Whether the recording is
 *    yet sufficient is a conjunction of several conditions, one of which is a measurement
 *    the server does not perform here and another of which is a manual precondition nothing
 *    in this process can verify. Elapsed time alone is only one term of that conjunction, so
 *    drawing it as a percentage would assert a completion nobody has computed. The facts
 *    below are reported; the judgement is left to the reader.
 */
function CollectorPanel() {
  const status = useQuery({
    queryKey: ['collector'],
    queryFn: () => api.collector(),
    refetchInterval: 15000,
    retry: 0,
  })

  return (
    <section className="p-3" style={{ borderBottom: '1px solid var(--border)' }}>
      <h2 className="pl-heading" style={{ margin: '0 0 10px' }}>
        Collector
      </h2>
      {status.isError ? (
        <p className="mono" style={{ fontSize: 11, color: 'var(--warn)', margin: 0, whiteSpace: 'pre-wrap' }}>
          △{' '}
          {status.error instanceof ApiError ? status.error.message : String(status.error)}
        </p>
      ) : status.data == null ? (
        <div aria-hidden={true}>
          <div className="pl-skel" style={{ height: 12, width: '58%', marginBottom: 6 }} />
          <div className="pl-skel" style={{ height: 11, width: '40%' }} />
        </div>
      ) : (
        <CollectorCard collector={status.data.collector} />
      )}
    </section>
  )
}

function CollectorCard({ collector }: { collector: CollectorStatus }) {
  const now = Date.now()
  const fresh =
    collector.heartbeat_age_ms != null && collector.heartbeat_age_ms <= HEARTBEAT_FRESH_MS
  // The server's own elapsed measure is preferred; `now - run_started_ms` is the fallback,
  // and it is this browser's clock rather than the server's, so it is only used when there
  // is nothing better.
  const elapsedMs =
    collector.run_elapsed_ms ??
    (collector.run_started_ms == null ? null : Math.max(0, now - collector.run_started_ms))

  return (
    <div className="pl-panel p-2">
      <div className="flex items-center gap-2" style={{ fontSize: 12 }}>
        <span
          className={fresh && collector.state_file_present ? 'pl-dot pl-dot-live' : 'pl-dot'}
          style={
            !collector.state_file_present
              ? undefined
              : fresh
                ? { background: 'var(--pos)', color: 'var(--pos)' }
                : { background: 'var(--warn)', color: 'var(--warn)' }
          }
        />
        <span style={{ color: 'var(--text-dim)' }}>
          {!collector.state_file_present
            ? 'no state file — nothing below is known'
            : fresh
              ? 'recording'
              : 'no recent heartbeat'}
        </span>
        <span className="mono" style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--text-mute)' }}>
          {collector.pid == null ? 'pid not recorded' : `pid ${collector.pid}`}
        </span>
      </div>

      <div className="mono" style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 6 }}>
        {/* Deliberately "current run started X ago", never a single elapsed figure standing
            alone: an elapsed number with no start behind it reads as a claim about the
            process's whole life, when a restart begins a new run. */}
        {elapsedMs == null
          ? 'current run start not recorded'
          : `current run started ${formatDuration(elapsedMs)} ago`}
        {collector.run_started_ms != null ? (
          <span style={{ color: 'var(--text-mute)' }}> · {formatTime(collector.run_started_ms)}</span>
        ) : elapsedMs != null ? (
          <span style={{ color: 'var(--text-mute)' }}> · start timestamp not recorded</span>
        ) : null}
      </div>

      <div className="mono" style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 3 }}>
        heartbeat{' '}
        {collector.heartbeat_age_ms == null ? (
          <span style={{ color: 'var(--warn)' }}>age not recorded</span>
        ) : (
          <span style={{ color: fresh ? 'var(--text-dim)' : 'var(--warn)' }}>
            {formatDuration(collector.heartbeat_age_ms)} ago
          </span>
        )}
        {collector.last_heartbeat_ms != null ? (
          <span style={{ color: 'var(--text-mute)' }}> · {formatTime(collector.last_heartbeat_ms)}</span>
        ) : null}
      </div>

      <div className="mono" style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 3 }}>
        {/* `null` restarts is "not recorded", not "none". Drawing an unknown restart count
            as 0 would report an unmonitored process as a stable one. */}
        {collector.restarts_since_run_start == null ? (
          <span style={{ color: 'var(--warn)' }}>restarts since run start: not recorded</span>
        ) : (
          `${collector.restarts_since_run_start.toLocaleString('en-US')} restart${
            collector.restarts_since_run_start === 1 ? '' : 's'
          } since the run started`
        )}
      </div>

      <div className="flex flex-wrap items-center gap-2" style={{ marginTop: 6 }}>
        <span style={{ fontSize: 10, color: 'var(--text-mute)' }}>recording</span>
        {collector.datasets_recording.length === 0 ? (
          <span className="mono" style={{ fontSize: 11, color: 'var(--warn)' }}>
            no datasets
          </span>
        ) : (
          collector.datasets_recording.map((dataset) => (
            <span key={dataset} className="pl-tag mono" data-tip={DATASET_NOTES[dataset]}>
              {dataset}
            </span>
          ))
        )}
      </div>

      {collector.caveats.length > 0 ? (
        <ul
          style={{
            margin: '8px 0 0',
            paddingLeft: 18,
            fontSize: 11,
            color: 'var(--warn)',
          }}
        >
          {collector.caveats.map((caveat, index) => (
            <li key={index} style={{ whiteSpace: 'pre-wrap' }}>
              {caveat}
            </li>
          ))}
        </ul>
      ) : null}

      <p style={{ fontSize: 10, color: 'var(--text-mute)', margin: '8px 0 0' }}>
        Read-only. There is no start or stop here: the depth and book data being recorded is
        not published in any archive, so a minute not recorded is a minute that cannot be
        obtained later. No completion figure is shown either — whether the recording is yet
        sufficient depends on conditions this endpoint does not measure, and a percentage
        would assert an answer the server has not computed.
      </p>
    </div>
  )
}

/**
 * Exchange Connection (spec 10.3, spec 11).
 *
 * The inputs are `type="password"` and `autoComplete="off"`: the secret must not be
 * shoulder-readable and must not land in the browser's saved-password store, which is on
 * disk — the one place spec 11 says a key may never be. Nothing here writes to
 * `localStorage`, and the values live in component state that unmounts with the panel.
 */
function ExchangePanel() {
  const queryClient = useQueryClient()
  const notify = useUi((s) => s.notify)
  const [apiKey, setApiKey] = useState('')
  const [apiSecret, setApiSecret] = useState('')
  const [endpoint, setEndpoint] = useState<'testnet' | 'production'>('testnet')
  const [error, setError] = useState<string | null>(null)

  // Shared options (`lib/queries.ts`): this key is also the chrome's badge query, and two
  // registrations with different options let mount order pick which set governed.
  const status = useQuery(exchangeStatusQuery)

  const connect = useMutation({
    mutationFn: () => api.exchangeConnect({ api_key: apiKey, api_secret: apiSecret, endpoint }),
    onSuccess: () => {
      // Cleared the instant the server has them: the fields exist to carry the key to the
      // backend once, not to hold it.
      setApiKey('')
      setApiSecret('')
      setError(null)
      notify(`Connected to ${endpoint}`)
      void queryClient.invalidateQueries({ queryKey: ['exchange-status'] })
    },
    onError: (err) => setError(err instanceof ApiError ? err.message : String(err)),
  })

  const disconnect = useMutation({
    mutationFn: () => api.exchangeDisconnect(),
    onSuccess: (result) => {
      // The stopped sessions are the half of this action an operator cannot see from the
      // connection panel: a live session's worker signs with its own credential copy, so
      // disconnecting also stops it -- cancel-only, positions deliberately left open.
      notify(
        result.stopped.length > 0
          ? `Disconnected — key wiped. Live session${result.stopped.length > 1 ? 's' : ''} ` +
              `#${result.stopped.join(', #')} stopped with positions left open — check ${
                result.stopped.length > 1 ? 'them' : 'it'
              } in the Live Monitor.`
          : 'Disconnected — the key is wiped from backend memory',
        'warn',
      )
      void queryClient.invalidateQueries({ queryKey: ['exchange-status'] })
      void queryClient.invalidateQueries({ queryKey: ['runs'] })
    },
  })

  const connected = status.data?.connected === true

  return (
    <section className="p-3">
      <h2 className="pl-heading" style={{ margin: '0 0 10px' }}>
        Exchange connection
      </h2>

      {status.isPending ? (
        <div className="pl-panel p-2" aria-hidden={true} style={{ marginBottom: 8 }}>
          <div className="pl-skel" style={{ height: 12, width: '62%', marginBottom: 6 }} />
          <div className="pl-skel" style={{ height: 11, width: '44%' }} />
        </div>
      ) : connected ? (
        <div className="pl-panel p-2" style={{ marginBottom: 8 }}>
          <div className="flex items-center gap-2" style={{ fontSize: 12 }}>
            <span
              className="pl-dot pl-dot-live"
              style={{ background: 'var(--pos)', color: 'var(--pos)' }}
            />
            <span
              className="pl-tag"
              style={
                status.data?.endpoint === 'production'
                  ? {
                      color: 'var(--warn)',
                      borderColor: 'color-mix(in srgb, var(--warn) 45%, var(--border))',
                    }
                  : undefined
              }
            >
              {status.data?.endpoint}
            </span>
            <span style={{ color: 'var(--text-dim)' }}>{status.data?.alias ?? 'connected'}</span>
            <span className="mono" style={{ marginLeft: 'auto' }}>
              {status.data?.balance == null ? '—' : `${money(status.data.balance)} USDT`}
            </span>
          </div>
          <div className="mono" style={{ fontSize: 11, color: 'var(--text-mute)', marginTop: 4 }}>
            session expires in{' '}
            {status.data?.expires_in_s == null
              ? '—'
              : `${Math.floor(status.data.expires_in_s / 3600)}h ${Math.floor((status.data.expires_in_s % 3600) / 60)}m`}
            {status.data?.drift_ms == null ? '' : ` · clock drift ${status.data.drift_ms} ms`}
          </div>
          <button
            className="pl-btn pl-btn-danger mt-2"
            onClick={() => disconnect.mutate()}
            disabled={disconnect.isPending}
          >
            Disconnect
          </button>
        </div>
      ) : (
        <>
          <div className="flex items-center gap-2" style={{ marginBottom: 8, fontSize: 12 }}>
            <span className="pl-dot" />
            <span style={{ color: 'var(--text-mute)' }}>not connected</span>
          </div>
          <div className="flex gap-2" style={{ marginBottom: 8 }}>
            <button
              className={endpoint === 'testnet' ? 'pl-btn pl-btn-primary' : 'pl-btn'}
              onClick={() => setEndpoint('testnet')}
            >
              Testnet
            </button>
            <button
              className={endpoint === 'production' ? 'pl-btn pl-btn-primary' : 'pl-btn'}
              onClick={() => setEndpoint('production')}
            >
              Production
            </button>
          </div>
          <input
            className="pl-input mono"
            type="password"
            autoComplete="off"
            placeholder="API key"
            value={apiKey}
            onChange={(event) => setApiKey(event.target.value)}
            style={{ width: '100%', marginBottom: 6 }}
          />
          <input
            className="pl-input mono"
            type="password"
            autoComplete="off"
            placeholder="API secret"
            value={apiSecret}
            onChange={(event) => setApiSecret(event.target.value)}
            style={{ width: '100%', marginBottom: 6 }}
          />
          <button
            className="pl-btn pl-btn-primary"
            disabled={connect.isPending || apiKey === '' || apiSecret === ''}
            onClick={() => connect.mutate()}
          >
            {connect.isPending ? 'Validating…' : `Connect to ${endpoint}`}
          </button>
          {error != null ? (
            <p className="mono sev-error" style={{ fontSize: 11, marginTop: 6, whiteSpace: 'pre-wrap' }}>
              {error}
            </p>
          ) : null}
        </>
      )}

      {/* Spec 10.3 requires this notice inline, and spec 11 is what it is promising. */}
      <div className="pl-panel p-2" style={{ marginTop: 10, background: 'var(--surface-2)' }}>
        <p style={{ fontSize: 10, color: 'var(--text-mute)', margin: 0 }}>
          Keys are held in this backend process's memory for this session only. They are
          never written to disk, to the database, to a log, or to this browser's storage, and
          are never sent back to the page. Disconnect and the kill switch both wipe them
          immediately, and an idle session wipes them after 12 hours — leaving any open
          position open, because closing it silently would be a trade nobody asked for.
        </p>
        <p style={{ fontSize: 10, color: 'var(--text-mute)', margin: '6px 0 0' }}>
          The key needs <b>Reading</b> and <b>Futures Trading</b> only. Never enable
          withdrawals, and set an IP whitelist.
        </p>
      </div>
    </section>
  )
}
