/**
 * Dashboard (spec 10.3): "is real money at risk, what ran recently, is the data fresh" —
 * one screen, everything a link into the tab that owns it. Composed entirely from
 * existing endpoints; a dashboard with its own aggregation API would be a second set of
 * numbers to disagree with the tabs it links to.
 *
 * The layout is a hero answering the money question, a row of the three actions that
 * start everything, then a responsive grid of context cards. The hero PnL flashes toward
 * the direction it moved (`useValueFlash`) — on a control centre the *change* is the
 * information, and a number that updates silently under a reader's eyes is easy to miss.
 */

import { useQuery } from '@tanstack/react-query'
import { api, formatTime, money, num, signed, type Run } from '../api'
import { useValueFlash } from '../lib/hooks'
import { killQuery, sessionsQuery } from '../lib/queries'
import { useUi } from '../store'
import { JobStatus } from './Lab'

const TERMINAL = new Set(['done', 'failed', 'cancelled', 'lost'])

export function DashboardPanel() {
  const setTab = useUi((s) => s.setTab)
  const selectRun = useUi((s) => s.selectRun)
  const selectLabJob = useUi((s) => s.selectLabJob)
  const openRunDraft = useUi((s) => s.openRunDraft)
  const openSessionDraft = useUi((s) => s.openSessionDraft)

  // Shared options (`lib/queries.ts`): these keys are also registered by the chrome, and
  // two registrations with different intervals meant mount order chose which one governed.
  const sessions = useQuery(sessionsQuery)
  const runs = useQuery({ queryKey: ['runs', 'dashboard'], queryFn: () => api.runs(), refetchInterval: 5000 })
  const kill = useQuery(killQuery)
  const jobs = useQuery({ queryKey: ['lab-jobs'], queryFn: () => api.labJobs(), refetchInterval: 5000 })
  const coverage = useQuery({
    queryKey: ['coverage', 'BTCUSDT'],
    queryFn: () => api.coverage('BTCUSDT'),
    refetchInterval: 60_000,
  })

  const active = (sessions.data?.sessions ?? []).filter((run) => !TERMINAL.has(run.status))
  const recent = (runs.data?.runs ?? []).slice(0, 6)
  const recentJobs = (jobs.data?.jobs ?? []).slice(0, 5)

  return (
    <main className="flex-1 p-4 pl-scroll" style={{ overflowY: 'auto' }}>
      <div className="flex flex-col gap-3" style={{ maxWidth: 1200, margin: '0 auto' }}>
        {kill.data?.armed ? (
          <div
            className="pl-panel flex items-center gap-2 px-3 py-2"
            style={{
              borderColor: 'color-mix(in srgb, var(--down) 45%, var(--border))',
              fontSize: 12,
              color: 'var(--down)',
            }}
          >
            ⛔ Kill switch armed — {kill.data.trigger ?? 'operator'}. Un-arm from the header
            before any session can start (spec 7.6).
          </div>
        ) : null}

        <SessionHero
          active={active}
          loading={sessions.isLoading}
          unreachable={sessions.isError}
          onOpen={(id) => {
            setTab('Runs')
            selectRun(id)
          }}
        />

        {/* The three verbs the platform exists for, one click from arrival. */}
        <div className="flex flex-wrap items-center gap-2">
          <button className="pl-btn pl-btn-primary" onClick={() => openRunDraft(-1)}>
            New backtest
          </button>
          <button className="pl-btn" onClick={() => openSessionDraft(true)}>
            Start paper session
          </button>
          <button className="pl-btn" onClick={() => setTab('Strategies')}>
            Open a strategy
          </button>
          <span className="flex-1" />
          <span style={{ fontSize: 11, color: 'var(--text-mute)' }}>
            <span className="pl-kbd">⌘K</span> jumps anywhere
          </span>
        </div>

        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
            gap: 12,
            alignItems: 'start',
          }}
        >
          <Card title="Recent runs" onMore={() => setTab('Runs')}>
            {runs.isLoading ? (
              <Skeleton rows={4} />
            ) : runs.isError ? (
              <Unreachable what="runs" />
            ) : recent.length === 0 ? (
              <Empty>No runs yet — start a backtest from the button above.</Empty>
            ) : (
              recent.map((run) => (
                <Line key={run.id} onClick={() => { setTab('Runs'); selectRun(run.id) }}>
                  <span className="mono" style={{ color: 'var(--text-mute)' }}>#{run.id}</span>
                  <span className="truncate">{run.strategy_name}</span>
                  <StatusWord status={run.status} />
                  <span className="mono" style={{ marginLeft: 'auto', color: pnlColour(run.net_pnl) }}>
                    {run.net_pnl == null ? (run.sharpe == null ? '—' : `S ${num(run.sharpe)}`) : signed(money(run.net_pnl), Number(run.net_pnl))}
                  </span>
                </Line>
              ))
            )}
          </Card>

          <Card title="Lab" onMore={() => setTab('Lab')}>
            {jobs.isLoading ? (
              <Skeleton rows={4} />
            ) : jobs.isError ? (
              <Unreachable what="Lab jobs" />
            ) : recentJobs.length === 0 ? (
              <Empty>No Lab jobs yet — open a completed run and press Send to Lab.</Empty>
            ) : (
              recentJobs.map((job) => (
                <Line key={job.id} onClick={() => { setTab('Lab'); selectLabJob(job.id) }}>
                  {job.tool} <span className="mono" style={{ color: 'var(--text-mute)' }}>run #{job.run_id}</span>
                  <span style={{ marginLeft: 'auto' }}><JobStatus job={job} /></span>
                </Line>
              ))
            )}
          </Card>

          <Card title="Data freshness · BTCUSDT" onMore={() => setTab('Data & Feed')}>
            {coverage.isLoading ? (
              <Skeleton rows={5} />
            ) : coverage.isError ? (
              <Unreachable what="coverage" />
            ) : coverage.data == null ? (
              <Empty>Loading coverage…</Empty>
            ) : (
              Object.entries(coverage.data.datasets).map(([dataset, range]) => {
                const end = range.end_ms
                const ageMs = end == null ? null : Date.now() - end
                const stale = ageMs != null && ageMs > 6 * 3_600_000
                return (
                  <Line key={dataset}>
                    <span className="mono">{dataset}</span>
                    <span
                      className="mono"
                      style={{
                        marginLeft: 'auto',
                        color: end == null ? 'var(--text-mute)' : stale ? 'var(--warn)' : 'var(--text-dim)',
                      }}
                    >
                      {/* Colour is never the only signal (spec 10.1): a stale row is
                          marked in text too, so it survives a screenshot. */}
                      {end == null ? 'no data' : `${stale ? '△ stale · ' : ''}to ${formatTime(end)}`}
                    </span>
                  </Line>
                )
              })
            )}
            <p style={{ fontSize: 10.5, color: 'var(--text-mute)', margin: '8px 0 0' }}>
              The depth datasets only accumulate while the collector runs — their end time
              is the live edge of the L2 backtest window.
            </p>
          </Card>
        </div>
      </div>
    </main>
  )
}

/* ------------------------------------------------------------------------------ hero */

/** The money question, answered in the largest type on the screen. */
function SessionHero({
  active,
  loading,
  unreachable,
  onOpen,
}: {
  active: Run[]
  loading: boolean
  unreachable: boolean
  onOpen: (runId: number) => void
}) {
  const shown = active.find((run) => run.mode === 'live') ?? active[0]
  const pnl = shown?.net_pnl == null ? null : Number(shown.net_pnl)
  const flash = useValueFlash(pnl)

  if (loading) {
    return (
      <div className="pl-panel p-4 flex flex-col gap-2">
        <span className="pl-skel" style={{ width: 120, height: 12 }} />
        <span className="pl-skel" style={{ width: 220, height: 30 }} />
      </div>
    )
  }
  if (unreachable) {
    return (
      <div className="pl-panel p-4">
        <Unreachable what="sessions" />
      </div>
    )
  }
  if (shown == null) {
    return (
      <div className="pl-panel p-4 flex items-center gap-4">
        <div className="flex flex-col gap-1">
          <span className="pl-heading">No session running</span>
          <span style={{ fontSize: 12.5, color: 'var(--text-dim)' }}>
            Nothing is at risk. Paper sessions start from the button below; the exchange key
            connects under Data &amp; Feed.
          </span>
        </div>
      </div>
    )
  }

  const live = shown.mode === 'live'
  const colour = pnl == null ? 'var(--text)' : pnl > 0 ? 'var(--pos)' : pnl < 0 ? 'var(--down)' : 'var(--text)'
  const others = active.length - 1
  return (
    <div
      className="pl-panel p-4 flex flex-wrap items-center gap-x-8 gap-y-3"
      role="button"
      onClick={() => onOpen(shown.id)}
      style={{ cursor: 'pointer' }}
      title="Open the live monitor for this session"
    >
      <div className="flex flex-col gap-1">
        <span className="pl-heading flex items-center gap-2">
          <span
            className="pl-dot pl-dot-live"
            style={{ background: live ? 'var(--pos)' : 'var(--warn)', color: live ? 'var(--pos)' : 'var(--warn)' }}
          />
          {live ? 'Live session' : 'Paper session'}
          {others > 0 ? ` · +${others} more` : ''}
        </span>
        <span style={{ fontSize: 13 }}>
          {shown.strategy_name}
          <span className="mono" style={{ color: 'var(--text-mute)' }}>
            {' '}v{shown.version_no} · {shown.symbols.join(', ')} · run #{shown.id}
          </span>
        </span>
      </div>
      <div className="flex flex-col gap-1">
        <span className="pl-card-label">Session PnL</span>
        <span className={`pl-kpi ${flash}`} style={{ color: colour }}>
          {pnl == null ? '—' : signed(money(shown.net_pnl!), pnl)}
        </span>
      </div>
      {shown.started_ms != null ? (
        <div className="flex flex-col gap-1">
          <span className="pl-card-label">Started</span>
          <span className="mono" style={{ fontSize: 14, color: 'var(--text-dim)' }}>
            {formatTime(shown.started_ms)}
          </span>
        </div>
      ) : null}
      <span className="flex-1" />
      <span style={{ fontSize: 11, color: 'var(--text-mute)' }}>open monitor →</span>
    </div>
  )
}

/* ----------------------------------------------------------------------------- pieces */

function Card({ title, onMore, children }: { title: string; onMore?: () => void; children: React.ReactNode }) {
  return (
    <div className="pl-panel p-3">
      <div className="flex items-center" style={{ marginBottom: 8 }}>
        <h3 className="pl-heading" style={{ margin: 0 }}>{title}</h3>
        {onMore != null ? (
          <button
            type="button"
            className="pl-btn"
            style={{ marginLeft: 'auto', fontSize: 10, height: 20, padding: '0 6px' }}
            onClick={onMore}
          >
            open →
          </button>
        ) : null}
      </div>
      {children}
    </div>
  )
}

function Line({ children, onClick }: { children: React.ReactNode; onClick?: () => void }) {
  return (
    <div
      className="flex items-center gap-2"
      role={onClick != null ? 'button' : undefined}
      onClick={onClick}
      style={{
        fontSize: 12,
        padding: '4px 6px',
        margin: '0 -6px',
        borderRadius: 5,
        cursor: onClick != null ? 'pointer' : undefined,
        transition: 'background 100ms var(--ease)',
      }}
      onMouseEnter={(e) => {
        if (onClick != null) (e.currentTarget as HTMLElement).style.background = 'var(--surface-2)'
      }}
      onMouseLeave={(e) => {
        ;(e.currentTarget as HTMLElement).style.background = 'transparent'
      }}
    >
      {children}
    </div>
  )
}

function StatusWord({ status }: { status: string }) {
  const colour =
    status === 'done' ? 'var(--text-mute)'
    : status === 'running' ? 'var(--info)'
    : status === 'failed' || status === 'lost' ? 'var(--down)'
    : 'var(--text-mute)'
  return (
    <span className="mono" style={{ fontSize: 11, color: colour }}>
      {status}
    </span>
  )
}

function pnlColour(net: string | null): string {
  if (net == null) return 'var(--text-dim)'
  const value = Number(net)
  return value > 0 ? 'var(--pos)' : value < 0 ? 'var(--down)' : 'var(--text-dim)'
}

/** A failed request must never render as "there is none of this" (spec 1.4 applied to
 *  the UI): absence of evidence is not evidence of absence, and the Dashboard's whole
 *  job is answering "is real money at risk right now". */
function Unreachable({ what }: { what: string }) {
  return (
    <p style={{ fontSize: 11, color: 'var(--warn)', margin: 0 }}>
      △ Could not reach the API for {what} — this card is not saying there are none, it is
      saying it does not know.
    </p>
  )
}

function Empty({ children }: { children: React.ReactNode }) {
  return <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>{children}</p>
}

function Skeleton({ rows }: { rows: number }) {
  return (
    <div className="flex flex-col gap-2" aria-hidden="true">
      {Array.from({ length: rows }, (_, i) => (
        <span key={i} className="pl-skel" style={{ height: 14, width: `${88 - (i % 3) * 14}%` }} />
      ))}
    </div>
  )
}
