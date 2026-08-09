/**
 * The Feed (spec 10.3) -- *"the first place to look when something is wrong"*.
 *
 * WS connection events, ingest rates, order lifecycle, errors, rate-limit warnings,
 * reconnects and risk-layer refusals, in one stream, in the order they happened. The value
 * of the panel is entirely in that ordering: an order rejected because a socket dropped
 * eleven seconds earlier is two lines that only mean something next to each other.
 *
 * **Paged by cursor (`since_seq`), never by offset.** The log is being appended to while it
 * is read. An offset window over a growing log slides backwards under the reader -- ask for
 * `offset=0,limit=400` twice with fifty new lines in between and the second answer repeats
 * 350 of the first and silently drops the fifty. The run detail page's event log can afford
 * offsets because a finished run's log is fixed; this one cannot. `since_seq` addresses a
 * position in the log rather than a distance from its end, so the reader's window is stable
 * no matter how fast the writer runs.
 *
 * **Virtualised on a fixed row height.** A session logs tens of thousands of lines; mounting
 * one DOM node per line makes scrolling the panel the most expensive thing in the browser.
 * The rows are `.pl-feed-row` at exactly 22 px with no wrapping, because the window is
 * computed from a row count -- a row that wrapped to two lines would put every row below it
 * at the wrong offset.
 *
 * **Retention is bounded.** A monitor left open overnight would otherwise hold every line of
 * a twelve-hour session in browser memory. The oldest are dropped and the count of what was
 * dropped is shown, rather than trimming quietly -- a log that silently forgets its beginning
 * is worse than one that admits to it.
 */

import { useQuery } from '@tanstack/react-query'
import { useEffect, useMemo, useRef, useState } from 'react'
import {
  api,
  FEED_SEVERITIES,
  formatClock,
  type FeedEntry,
  type Run,
  type Severity,
} from '../api'
import { sessionsQuery } from '../lib/queries'
import { useUi } from '../store'

/** Must match `.pl-feed-row`'s height in `styles.css`. The virtual window is a row count
 *  times this number, so the two disagreeing shows up as rows drifting out of the viewport
 *  as you scroll -- a bug that looks like a rendering glitch and is arithmetic. */
const ROW_HEIGHT = 22

/** Rows rendered beyond each edge of the viewport, so a fast scroll does not expose blank
 *  space before the next frame. */
const OVERSCAN = 8

const PAGE_LIMIT = 400

/** Lines held in the browser before the oldest are dropped. Five thousand is roughly two
 *  hours of a chatty session and about 110 000 px of scroll -- past that the panel is not
 *  how anyone reads back, the exported log is. */
const MAX_ENTRIES = 5000

const SEVERITY_GLYPH: Record<string, string> = { error: '✕', warning: '△', info: '·' }

/** Severity carries a glyph as well as a colour, for the same reason every number here
 *  carries an arrow (spec 10.1): a red line and an amber line are the same line in a
 *  greyscale screenshot, and screenshots are how these get reported. */
function severityClass(severity: string): string {
  if (severity === 'error') return 'sev-error'
  if (severity === 'warning') return 'sev-warning'
  return 'sev-info'
}

export function Feed({ runId }: { runId: number }) {
  const [severity, setSeverity] = useState<Severity | ''>('')
  const [kind, setKind] = useState('')
  const [live, setLive] = useState(true)

  const [entries, setEntries] = useState<FeedEntry[]>([])
  const [cursor, setCursor] = useState(0)
  const [dropped, setDropped] = useState(0)
  const [kinds, setKinds] = useState<string[]>([])
  // Whether the server has answered at least once *since the filter last changed*. Kept
  // sticky here rather than read off `page.isSuccess`: the cursor is part of the query key
  // and `gcTime` is 0, so every cursor advance mints a brand-new query whose `isSuccess`
  // is false while it is in flight — about once a second on a live tail. Reading the flag
  // off the current query instance flickered the panel back to the loading skeleton each
  // poll, so an answered-empty filter could never settle on "nothing matched".
  const [answered, setAnswered] = useState(false)
  // The high-water mark of what has been accepted. React 18's StrictMode mounts effects
  // twice in development and a re-mounted query replays its cached page, so the append path
  // has to be idempotent -- without this the panel shows every line twice on a dev reload.
  const lastSeq = useRef(-1)

  // A filter change is a different stream, not a subset of the one already held: the server
  // decides what matches, so entries fetched under the old filter say nothing about what the
  // new one would have returned for the same range. Restarting from seq 0 re-reads the log
  // through the new filter, which is the only answer that is actually about the filter.
  useEffect(() => {
    setEntries([])
    setCursor(0)
    setDropped(0)
    setAnswered(false)
    lastSeq.current = -1
  }, [runId, severity, kind])

  useEffect(() => {
    setKinds([])
  }, [runId])

  const page = useQuery({
    // `kind`, not `source`: the route filters on severity and kind (`sessions.py
    // run_feed`). A `source` param used to be sent here — the endpoint has never accepted
    // one, so the dropdown wired to it filtered nothing.
    queryKey: ['feed', runId, severity, kind, cursor],
    queryFn: () =>
      api.feed(runId, {
        sinceSeq: cursor,
        limit: PAGE_LIMIT,
        severity: severity || undefined,
        kind: kind || undefined,
      }),
    // Polling only while following. A paused feed that kept fetching would keep advancing
    // the cursor and keep trimming the front of the buffer -- the reader's window would
    // scroll out from under them while they were reading it.
    refetchInterval: live ? 1000 : false,
    staleTime: 0,
    // Pages are consumed once and appended into `entries`. Keeping them cached as well would
    // hold a second copy of the whole session's log for no reader.
    gcTime: 0,
    retry: 0,
  })

  const pageAnswered = page.isSuccess
  useEffect(() => {
    if (pageAnswered) setAnswered(true)
  }, [pageAnswered])

  const data = page.data
  useEffect(() => {
    if (!data) return
    const fresh = data.entries.filter((entry) => entry.seq > lastSeq.current)
    if (fresh.length) {
      lastSeq.current = fresh[fresh.length - 1].seq
      // Computed against the `entries` this effect closed over rather than inside a
      // functional updater. `setDropped` used to live *inside* the `setEntries` updater —
      // an updater React is free to invoke twice (and does, under StrictMode), so every
      // trimmed page counted its dropped lines double. The idempotence guard on `lastSeq`
      // above is what makes the direct read safe: a re-run of this effect finds nothing
      // fresh and changes nothing.
      const merged = entries.concat(fresh)
      const overflow = Math.max(0, merged.length - MAX_ENTRIES)
      if (overflow > 0) setDropped((count) => count + overflow)
      setEntries(overflow > 0 ? merged.slice(overflow) : merged)
      setKinds((previous) => {
        const seen = new Set(previous)
        for (const entry of fresh) seen.add(entry.kind)
        return seen.size === previous.length ? previous : [...seen].sort()
      })
    }
    // A page that returned nothing still advances nothing, and re-requesting the same cursor
    // is exactly what tailing is. Only a forward move is taken: a server that answered with a
    // lower `next_seq` would otherwise walk the reader backwards through the log.
    if (data.next_seq > cursor) setCursor(data.next_seq)
    // `entries` is in the dependency list because the merge reads it directly; the
    // `lastSeq` guard keeps the re-run from appending anything twice.
  }, [data, cursor, entries])

  return (
    <div className="flex flex-col" style={{ minHeight: 0, flex: 1 }}>
      <div className="flex items-center gap-2 shrink-0" style={{ padding: '0 0 8px' }}>
        <select
          className="pl-input"
          style={{ width: 120 }}
          value={severity}
          onChange={(event) => setSeverity(event.target.value as Severity | '')}
        >
          <option value="">All severities</option>
          {FEED_SEVERITIES.map((value) => (
            <option key={value} value={value}>
              {value}
            </option>
          ))}
        </select>
        <select
          className="pl-input"
          style={{ width: 160 }}
          value={kind}
          onChange={(event) => setKind(event.target.value)}
          title="Event kind — the one row filter the feed route actually supports."
        >
          <option value="">All kinds</option>
          {kinds.map((value) => (
            <option key={value} value={value}>
              {value}
            </option>
          ))}
          {/* A kind that has been filtered to but has since scrolled out of the observed
              set would otherwise vanish from its own dropdown mid-selection. */}
          {kind && !kinds.includes(kind) ? <option value={kind}>{kind}</option> : null}
        </select>
        <button
          className="pl-btn"
          onClick={() => setLive((value) => !value)}
          title={
            live
              ? 'Stop fetching. The buffer stops moving so a line can be read without it scrolling away.'
              : 'Resume tailing from where the cursor stopped. Nothing written while paused is skipped.'
          }
        >
          {live ? '❙❙ Pause' : '▶ Resume'}
        </button>
        <span className="flex-1" />
        <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
          {entries.length.toLocaleString()} line{entries.length === 1 ? '' : 's'}
          {dropped ? ` · ${dropped.toLocaleString()} older dropped` : ''} · seq {cursor}
        </span>
      </div>

      {page.isError ? (
        <p className="sev-error" style={{ margin: '0 0 8px', fontSize: 11 }}>
          The feed stopped answering: {String(page.error)}
        </p>
      ) : null}

      <FeedRows entries={entries} live={live} answered={answered} />
    </div>
  )
}

/* -------------------------------------------------------------------- the virtual list */

function FeedRows({
  entries,
  live,
  answered,
}: {
  entries: FeedEntry[]
  live: boolean
  /** Whether the server has answered at least once. An empty list before the first answer is
   *  "loading"; an empty list after it is genuinely an empty feed, and the two need
   *  different words. */
  answered: boolean
}) {
  const viewport = useRef<HTMLDivElement | null>(null)
  const [height, setHeight] = useState(320)
  const [scrollTop, setScrollTop] = useState(0)
  // Tail-follow, and it turns itself off the moment the reader scrolls away from the bottom.
  // A log that yanks itself back down while a line is being read is a log that cannot be
  // read at all, which is the failure mode of every terminal that auto-scrolls unasked.
  const [follow, setFollow] = useState(true)

  useEffect(() => {
    const node = viewport.current
    if (!node) return
    setHeight(node.clientHeight)
    const observer = new ResizeObserver(() => setHeight(node.clientHeight))
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  // Keyed on the newest seq, not on `entries.length`. Once the buffer is full the length
  // stops changing while the content keeps moving, and a follow that watched the length
  // would silently stop following at exactly the point a long-running session becomes the
  // thing worth watching.
  const tailSeq = entries.length ? entries[entries.length - 1].seq : 0
  useEffect(() => {
    const node = viewport.current
    if (!node || !follow) return
    node.scrollTop = node.scrollHeight
  }, [tailSeq, follow, height])

  const slice = useMemo(() => {
    const first = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN)
    const count = Math.ceil(height / ROW_HEIGHT) + OVERSCAN * 2
    return { first, rows: entries.slice(first, first + count) }
  }, [entries, scrollTop, height])

  return (
    <div style={{ position: 'relative', flex: 1, minHeight: 120 }}>
      <div
        ref={viewport}
        className="pl-scroll pl-panel"
        style={{
          position: 'absolute',
          inset: 0,
          borderRadius: 'var(--radius-sm)',
          background: 'var(--bg)',
        }}
        onScroll={(event) => {
          const node = event.currentTarget
          setScrollTop(node.scrollTop)
          setFollow(node.scrollHeight - node.scrollTop - node.clientHeight < ROW_HEIGHT)
        }}
      >
        {entries.length === 0 ? (
          answered ? (
            <p style={{ fontSize: 12, color: 'var(--text-mute)', padding: 12, margin: 0 }}>
              Nothing in the feed yet for this filter. The feed fills as the session runs:
              socket connects, ingested frames, every order and its outcome, and anything
              the risk layer refused.
            </p>
          ) : (
            <div aria-hidden={true} style={{ padding: 10 }}>
              {[0, 1, 2, 3, 4, 5].map((i) => (
                <div
                  key={i}
                  className="pl-skel"
                  style={{ height: 12, marginBottom: 10, width: `${92 - i * 11}%` }}
                />
              ))}
            </div>
          )
        ) : (
          <div style={{ height: entries.length * ROW_HEIGHT, position: 'relative' }}>
            <div style={{ position: 'absolute', top: slice.first * ROW_HEIGHT, left: 0, right: 0 }}>
              {slice.rows.map((entry) => (
                <Row key={entry.seq} entry={entry} />
              ))}
            </div>
          </div>
        )}
      </div>

      {!follow && entries.length ? (
        <button
          className="pl-btn"
          style={{ position: 'absolute', right: 16, bottom: 12, height: 24, boxShadow: 'var(--shadow-1)' }}
          onClick={() => setFollow(true)}
        >
          ↓ {live ? 'Follow the tail' : 'Jump to the end'}
        </button>
      ) : null}
    </div>
  )
}

/* ----------------------------------------------------------------- the display line */

/** One payload field as text, or `null` when it is absent or not a scalar. */
function field(payload: Record<string, unknown>, key: string): string | null {
  const value = payload?.[key]
  if (typeof value === 'string') return value === '' ? null : value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  return null
}

function joinParts(...parts: (string | null)[]): string {
  return parts.filter((part): part is string => part != null && part !== '').join(' ')
}

/**
 * The human line for a feed row, derived from `kind` + `payload`.
 *
 * Derived here because the wire has never carried prose: entries are
 * `{seq, ts_ms, kind, payload, severity, source}` and nothing else (`sessions.py
 * run_feed`). The cases below cover the kinds the engine and the live session actually
 * emit — the payload shapes are read from `backtest.py`'s `runtime.emit` calls,
 * `context.py`'s LOG/RECORD, and the session's connection status log. An unknown kind is
 * not blanked: its payload is rendered compactly, so a kind added on the server tomorrow
 * shows up here as data rather than as an empty column.
 */
function describeEntry(entry: FeedEntry): string {
  const p = entry.payload ?? {}
  const order = field(p, 'order_id')
  const reason = field(p, 'reason') ?? field(p, 'detail')
  switch (entry.kind) {
    case 'ORDER':
      return joinParts(
        field(p, 'side'),
        field(p, 'qty'),
        field(p, 'symbol'),
        field(p, 'type'),
        field(p, 'price') != null ? `@ ${field(p, 'price')}` : null,
        order != null ? `(${order})` : null,
      )
    case 'FILL':
      return joinParts(
        field(p, 'side'),
        field(p, 'qty'),
        field(p, 'symbol'),
        field(p, 'price') != null ? `@ ${field(p, 'price')}` : null,
        // Absent renders nothing: claiming "taker" about a fill that did not say would be
        // a statement about the fee schedule nothing established.
        field(p, 'is_maker') == null ? null : field(p, 'is_maker') === 'true' ? 'maker' : 'taker',
        field(p, 'fee') != null ? `· fee ${field(p, 'fee')}` : null,
        field(p, 'realized') != null ? `· realised ${field(p, 'realized')}` : null,
      )
    case 'ORDER_WORKING':
      return joinParts(
        order != null ? `${order} working` : 'working',
        field(p, 'price') != null ? `at ${field(p, 'price')}` : null,
        field(p, 'trigger_price') != null ? `trigger ${field(p, 'trigger_price')}` : null,
        reason,
      )
    case 'TRIGGER':
      return joinParts(
        field(p, 'type'),
        order,
        'triggered',
        field(p, 'trigger_price') != null ? `at ${field(p, 'trigger_price')}` : null,
      )
    case 'MODIFY':
    case 'MODIFIED':
      return joinParts(
        order,
        field(p, 'price') != null ? `price ${field(p, 'price')}` : null,
        field(p, 'qty') != null ? `qty ${field(p, 'qty')}` : null,
        field(p, 'remaining') != null ? `remaining ${field(p, 'remaining')}` : null,
        field(p, 'queue_priority') != null ? `· queue ${field(p, 'queue_priority')}` : null,
      )
    case 'REJECT':
    case 'MODIFY_REJECTED':
    case 'PLATFORM_ORDER_REJECTED':
    case 'CANCEL':
    case 'EXPIRE':
    case 'EXPIRED':
      return joinParts(order, reason != null ? `— ${reason}` : null)
    case 'CANCEL_TOO_LATE':
      return joinParts(
        order,
        '— the cancel arrived after the order was already at the exchange',
      )
    case 'RISK_REJECT':
    case 'RISK_HALT': {
      // A RiskBreach: limit, detail, observed, allowed (core/risk.py to_json).
      const limit = field(p, 'limit')
      const observed = field(p, 'observed')
      const allowed = field(p, 'allowed')
      return joinParts(
        limit != null ? `${limit}:` : null,
        field(p, 'detail'),
        observed != null && allowed != null ? `(observed ${observed}, limit ${allowed})` : null,
      )
    }
    case 'KILL_SWITCH':
      return joinParts(
        field(p, 'trigger'),
        field(p, 'detail') != null ? `— ${field(p, 'detail')}` : null,
        field(p, 'flatten') === 'true' ? '· close-all' : '· cancel-only',
      )
    case 'LIQUIDATION':
      return joinParts(
        field(p, 'symbol'),
        field(p, 'position_side'),
        'liquidated',
        field(p, 'mark_price') != null ? `at mark ${field(p, 'mark_price')}` : null,
        field(p, 'trigger_price') != null ? `(trigger ${field(p, 'trigger_price')})` : null,
      )
    case 'LIQUIDATION_PROXIMITY':
      return joinParts(
        field(p, 'symbol'),
        'approaching liquidation',
        field(p, 'liquidation_price') != null ? `at ${field(p, 'liquidation_price')}` : null,
      )
    case 'FUNDING':
      return joinParts(
        field(p, 'symbol'),
        field(p, 'rate') != null ? `rate ${field(p, 'rate')}` : null,
        field(p, 'payment') != null ? `paid ${field(p, 'payment')}` : null,
      )
    case 'FUNDING_UNSETTLED':
      return joinParts(
        field(p, 'symbol'),
        field(p, 'rate') != null ? `rate ${field(p, 'rate')}` : null,
        '— not booked:',
        reason,
      )
    case 'AUTO_FLATTEN':
    case 'FORCE_FLATTEN':
      return joinParts(field(p, 'symbol'), reason != null ? `— ${reason}` : null, order)
    case 'AUTO_FLATTEN_FAILED':
    case 'FORCE_FLATTEN_FAILED':
      return joinParts(
        field(p, 'symbol'),
        '— the exit was refused:',
        reason,
        order != null ? `(${order})` : null,
      )
    case 'NO_QUOTE_FILL':
    case 'DEPTH_EXHAUSTED':
      return joinParts(field(p, 'symbol'), order, reason)
    case 'LOG':
      return joinParts(
        field(p, 'level') != null ? `[${field(p, 'level')}]` : null,
        field(p, 'message'),
      )
    case 'RECORD':
      return joinParts(field(p, 'name'), '=', field(p, 'value'))
    case 'STATUS':
    case 'CONNECT':
    case 'DISCONNECT':
    case 'RECONNECT':
    case 'STALE':
    case 'HEARTBEAT':
      return joinParts(
        field(p, 'stream'),
        field(p, 'detail'),
        field(p, 'downtime_ms') != null && field(p, 'downtime_ms') !== '0'
          ? `(down ${field(p, 'downtime_ms')} ms)`
          : null,
      )
    default:
      // Unknown kind: the payload is the line. The kind already has its own column, so
      // rendering the JSON here keeps the row saying *something* true rather than blank.
      return compactPayload(p)
  }
}

function compactPayload(payload: Record<string, unknown>): string {
  const text = JSON.stringify(payload ?? {})
  return text === '{}' ? '' : text
}

function Row({ entry }: { entry: FeedEntry }) {
  const line = describeEntry(entry)
  const payload = compactPayload(entry.payload)
  return (
    <div
      className="pl-feed-row"
      title={`${entry.severity} · ${entry.source} · ${entry.kind}\n${line || '(no detail)'}\n${payload}`}
    >
      <span style={{ color: 'var(--text-mute)', width: 62, textAlign: 'right', flex: 'none' }}>
        {entry.seq}
      </span>
      <span style={{ color: 'var(--text-mute)', flex: 'none' }}>{formatClock(entry.ts_ms)}</span>
      <span className={severityClass(entry.severity)} style={{ width: 10, flex: 'none' }}>
        {SEVERITY_GLYPH[entry.severity] ?? '·'}
      </span>
      <span style={{ color: 'var(--text-dim)', width: 96, flex: 'none', overflow: 'hidden' }}>
        {entry.source}
      </span>
      <span style={{ color: 'var(--text-dim)', width: 130, flex: 'none', overflow: 'hidden' }}>
        {entry.kind}
      </span>
      <span
        className={entry.severity === 'error' ? 'sev-error' : undefined}
        style={{ color: entry.severity === 'error' ? undefined : 'var(--text)', flex: 'none' }}
      >
        {line}
      </span>
      <span style={{ color: 'var(--text-mute)', overflow: 'hidden', textOverflow: 'ellipsis' }}>
        {payload}
      </span>
    </div>
  )
}

/* ------------------------------------------------------------------ the Data & Feed tab */

/**
 * The Feed panel as the tab renders it, with the run picker in front of it.
 *
 * The picker exists because the feed is per-run and the tab is not. Active sessions are
 * listed first and separately: when something is wrong the run being asked about is almost
 * always the one still running, and hunting for it in a list sorted by id is the wrong first
 * ten seconds of an incident.
 */
export function FeedPanel() {
  const feedRunId = useUi((s) => s.feedRunId)
  const setFeedRunId = useUi((s) => s.setFeedRunId)

  // Shared options (`lib/queries.ts`): this key is also registered by the chrome and the
  // dashboard, and per-component options meant mount order decided the polling interval.
  const sessions = useQuery(sessionsQuery)
  const runs = useQuery({ queryKey: ['runs', false], queryFn: () => api.runs({ archived: false }) })

  const active = sessions.data?.sessions ?? []
  const others = (runs.data?.runs ?? []).filter((run) => !active.some((s) => s.id === run.id))

  // Default to a running session, then to the newest run. Chosen once and then left alone --
  // re-picking on every poll would move the panel out from under a reader the moment a new
  // run appeared.
  const firstActiveId = active[0]?.id
  const firstOtherId = others[0]?.id
  useEffect(() => {
    if (feedRunId != null) return
    const first = firstActiveId ?? firstOtherId
    if (first != null) setFeedRunId(first)
  }, [feedRunId, firstActiveId, firstOtherId, setFeedRunId])

  return (
    <section className="flex flex-col flex-1 p-3 gap-2" style={{ minWidth: 0, minHeight: 0 }}>
      <div className="flex items-baseline gap-3">
        <h1 className="pl-heading" style={{ margin: 0 }}>Feed</h1>
        <p style={{ margin: 0, fontSize: 11, color: 'var(--text-mute)' }}>
          Connection events, ingest, order lifecycle, rate-limit warnings and risk-layer
          refusals, in the order they happened. The first place to look when something is
          wrong.
        </p>
      </div>

      <div className="flex items-center gap-2 shrink-0">
        <select
          className="pl-input mono"
          style={{ width: 380 }}
          value={feedRunId ?? ''}
          onChange={(event) => setFeedRunId(event.target.value === '' ? null : Number(event.target.value))}
        >
          <option value="">Select a run…</option>
          {active.length ? (
            <optgroup label="Active sessions">
              {active.map((run) => (
                <option key={run.id} value={run.id}>
                  {runLabel(run)}
                </option>
              ))}
            </optgroup>
          ) : null}
          {others.length ? (
            <optgroup label="Runs">
              {others.map((run) => (
                <option key={run.id} value={run.id}>
                  {runLabel(run)}
                </option>
              ))}
            </optgroup>
          ) : null}
        </select>
      </div>

      {feedRunId == null ? (
        sessions.isPending || runs.isPending ? (
          <div aria-hidden={true} style={{ maxWidth: 520 }}>
            <div className="pl-skel" style={{ height: 12, width: '58%', marginBottom: 8 }} />
            <div className="pl-skel" style={{ height: 12, width: '36%' }} />
          </div>
        ) : (
          <p style={{ fontSize: 12, color: 'var(--text-mute)', margin: 0 }}>
            Pick a run above. Every run has a feed, including finished backtests -- for those it
            is a static log rather than a tail.
          </p>
        )
      ) : (
        <Feed key={feedRunId} runId={feedRunId} />
      )}
    </section>
  )
}

function runLabel(run: Run): string {
  return `#${run.id} ${run.strategy_name} · ${run.symbols.join(',')} · ${run.mode} · ${run.status}`
}
