/**
 * The run detail page (spec 10.3).
 *
 * The layout is an argument, in order: *what was run* (header and badges), *what happened*
 * (equity, drawdown, price with trade markers), *how well* (metric cards), *where the money
 * came from* (attribution), then the two evidence panes — trades and the raw event log.
 * Someone who stops reading after the first screen should already know whether to trust the
 * numbers, which is why the caveats are badges at the top rather than a footnote.
 *
 * **The trials counter sits beside Sharpe**, as spec 8.5 requires. It is the cheapest
 * defence against fooling yourself that exists: after 500 parameter combinations the best
 * Sharpe is upward-biased by roughly `sqrt(2 ln 500) ≈ 3.5` standard deviations under the
 * null, and a headline number with no `N` beside it invites exactly that mistake.
 *
 * **Undefined metrics render as an em dash, not as zero.** A Sortino with no losing period
 * is genuinely undefined; printing `0.00` would rank it below a strategy that had one.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import {
  api,
  ApiError,
  formatDate,
  formatDuration,
  formatTime,
  money,
  num,
  pct,
  type Metrics,
  type RiskBreach,
  type RunDetail as RunDetailPayload,
  type Trade,
} from '../api'
import { useCopy } from '../lib/hooks'
import { useUi } from '../store'
import { AttributionBar, EquityChart, PriceChart } from './Charts'
import { ChartFrame } from './Export'
import { ErrorBoundary } from './ErrorBoundary'
import { LiveMonitor } from './LiveMonitor'
import { ParityReport } from './ParityReport'
import { Badges, StatusCell, TierLine } from './RunList'

/** `lost` belongs here or the page polls forever.
 *
 *  A lost run is one whose worker is gone, so nothing will ever move it to `done` -- leaving
 *  it out of this set meant a 1.2 s poll continuing for as long as the tab stayed open,
 *  against a run whose row can no longer change. */
const TERMINAL = new Set(['done', 'failed', 'cancelled', 'lost'])

/** Paper and live sessions are runs, and almost every part of this page applies to them --
 *  but *while they are running* they are asked a different set of questions, and that is
 *  what `LiveMonitor` answers (spec 10.3). */
const SESSION_MODES = new Set(['paper', 'live'])

export function RunDetail({ runId }: { runId: number }) {
  const client = useQueryClient()
  const notify = useUi((s) => s.notify)
  const selectRun = useUi((s) => s.selectRun)

  const detail = useQuery({
    queryKey: ['run', runId],
    queryFn: () => api.run(runId),
    refetchInterval: (query) => (TERMINAL.has(query.state.data?.run.status ?? '') ? false : 1200),
  })

  const status = detail.data?.run.status ?? ''
  const done = status === 'done'
  // **`running`, not just `done`.** The worker republishes a thinned equity curve on its
  // progress cadence and the trades file on its checkpoint, so both answer while the run is
  // in flight -- and a run watched from the moment it starts is the whole point of the
  // panel. `enabled: done` alone left the page blank until the last event was dispatched.
  const live = status === 'running' || status === 'queued'
  // Faster than the 1200ms run poll would be pointless: the backtest worker rewrites the
  // preview every 2s (PROGRESS_INTERVAL_S) and a session every 5s (EQUITY_INTERVAL_S), so a
  // tighter interval only re-reads bytes that have not changed.
  const streaming = live ? 2000 : (false as const)
  const equity = useQuery({
    queryKey: ['equity', runId],
    queryFn: () => api.equity(runId),
    enabled: done || live,
    refetchInterval: streaming,
    // A run whose first checkpoint has not landed yet 404s. That is an expected state for a
    // few seconds, not a failure worth backing off from — the next poll is the retry.
    retry: false,
  })
  const price = useQuery({ queryKey: ['price', runId], queryFn: () => api.price(runId), enabled: done })
  const trades = useQuery({
    queryKey: ['trades', runId],
    queryFn: () => api.trades(runId),
    enabled: done || live,
    refetchInterval: streaming,
    retry: false,
  })
  const trials = useQuery({
    queryKey: ['trials', detail.data?.run.strategy_id],
    queryFn: () => api.trials(detail.data!.run.strategy_id),
    enabled: detail.data != null,
  })

  const invalidate = () => {
    client.invalidateQueries({ queryKey: ['runs'] })
    client.invalidateQueries({ queryKey: ['run', runId] })
  }

  const cancel = useMutation({
    mutationFn: () => api.cancelRun(runId),
    onSuccess: () => {
      invalidate()
      // For a session this endpoint is TerminateProcess, and the toast must not soften
      // that: nothing was cancelled at the exchange, so resting orders are the operator's
      // problem now and the toast is where they hear it.
      notify(
        SESSION_MODES.has(detail.data?.run.mode ?? '')
          ? 'Session worker terminated. No exchange orders were cancelled — check the venue for resting orders and open positions.'
          : 'Run cancelled',
        'warn',
      )
    },
    onError: (e) => notify(e instanceof ApiError ? e.message : String(e), 'error'),
  })
  const archive = useMutation({
    mutationFn: (archived: boolean) => api.archiveRun(runId, archived),
    onSuccess: () => invalidate(),
    onError: (e) => notify(e instanceof ApiError ? e.message : String(e), 'error'),
  })
  const remove = useMutation({
    mutationFn: () => api.deleteRun(runId),
    onSuccess: () => {
      invalidate()
      selectRun(null)
      notify('Run deleted')
    },
    onError: (e) => notify(e instanceof ApiError ? e.message : String(e), 'error'),
  })

  if (detail.isLoading) return <Panel>Loading run…</Panel>
  if (detail.isError) return <Panel>{String(detail.error)}</Panel>
  const data = detail.data!
  const run = data.run
  const spec = data.spec as Record<string, unknown>
  const session = SESSION_MODES.has(run.mode)

  return (
    <section className="pl-scroll flex-1" style={{ minWidth: 0 }}>
      <div className="p-3 flex flex-col gap-3" style={{ maxWidth: 1180 }}>
        {/* ------------------------------------------------------------------ header */}
        <div className="flex items-start gap-3">
          <div style={{ flex: 1, minWidth: 0 }}>
            <h1 className="flex items-center gap-2 flex-wrap" style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>
              {run.strategy_name}
              <span className="mono" style={{ color: 'var(--text-mute)', fontSize: 13, fontWeight: 400 }}>
                v{run.version_no} · run #{run.id}
              </span>
              <CopyId runId={run.id} />
              {run.label ? (
                <span style={{ color: 'var(--text-dim)', fontSize: 13, fontWeight: 400 }}>{run.label}</span>
              ) : null}
            </h1>
            <p className="mono" style={{ margin: '4px 0 0', fontSize: 11, color: 'var(--text-dim)' }}>
              {run.symbols.join(', ')} · {run.timeframe} ·{' '}
              {run.start_ms && run.end_ms ? `${formatDate(run.start_ms)} → ${formatDate(run.end_ms)}` : '—'} ·
              seed {run.seed} · engine v{run.engine_version}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <StatusCell run={run} />
            {/* A running session is stopped from the monitor below, not from here. The two
                are not the same action: cancelling a backtest ends a computation, while
                stopping a session has to decide what happens to an open position, and a
                button that never asked that question would be answering it by default. */}
            {!TERMINAL.has(run.status) ? (
              session ? (
                // The escalation `sessions.py`'s module docstring names: Stop asks the
                // session to shut down in order (cancel its orders, honour the flatten
                // choice); this kills the worker process outright — TerminateProcess, no
                // shutdown sequence. It was hidden for every session, which left a wedged
                // event loop with no UI escape at all.
                <button
                  className="pl-btn pl-btn-danger"
                  title={
                    'Kill the worker process (TerminateProcess). No orders are cancelled ' +
                    'at the exchange and positions are left exactly as they are — the ' +
                    'session gets no chance to shut down cleanly. Last resort for a wedged ' +
                    'session that ignores Stop.'
                  }
                  onClick={() => {
                    if (
                      window.confirm(
                        `Terminate session #${run.id}'s worker process?\n\n` +
                          'This is TerminateProcess, not a stop: NO orders are cancelled at ' +
                          'the exchange, positions are left exactly as they are, and the ' +
                          'session cannot shut down cleanly. Any resting order keeps ' +
                          'working at the venue until you cancel it yourself.\n\n' +
                          'Use Stop session (on the monitor below) first; terminate only ' +
                          'when a wedged session has ignored it.',
                      )
                    )
                      cancel.mutate()
                  }}
                >
                  Terminate
                </button>
              ) : (
                <button
                  className="pl-btn pl-btn-danger"
                  onClick={() => {
                    // Confirmed like Delete is (spec 10.4): the two buttons share this
                    // corner of the header, and one misclick on an unconfirmed Cancel
                    // used to kill a multi-hour backtest with nothing to resume.
                    if (
                      window.confirm(
                        `Cancel run #${run.id}? The backtest stops where it is and cannot be resumed.`,
                      )
                    )
                      cancel.mutate()
                  }}
                >
                  Cancel
                </button>
              )
            ) : (
              <>
                {/* Spec 10.3: every completed backtest offers the Lab. Sessions do not —
                    a walk-forward re-runs the lake, and a session's data is its tape. */}
                {run.status === 'done' && !session ? <SendToLab runId={run.id} /> : null}
                <button className="pl-btn" onClick={() => archive.mutate(!run.archived)}>
                  {run.archived ? 'Unarchive' : 'Archive'}
                </button>
                <button
                  className="pl-btn pl-btn-danger"
                  onClick={() => {
                    if (window.confirm(`Delete run #${run.id} and its artefacts?`)) remove.mutate()
                  }}
                >
                  Delete
                </button>
              </>
            )}
          </div>
        </div>

        <TierLine run={run} />
        <Badges flags={run.flags} />

        {run.tier_degraded && run.tier_reason ? (
          <div
            className="pl-panel p-2"
            style={{ borderColor: 'color-mix(in srgb, var(--warn) 40%, var(--border))' }}
          >
            <p style={{ margin: 0, fontSize: 12, color: 'var(--text-dim)' }}>{run.tier_reason}</p>
          </div>
        ) : null}

        <HaltBanner detail={detail.data} />

        {run.warnings.length ? (
          <div className="pl-panel p-2" style={{ borderColor: 'color-mix(in srgb, var(--warn) 40%, var(--border))' }}>
            {run.warnings.map((warning, index) => (
              <p key={index} style={{ margin: index ? '6px 0 0' : 0, fontSize: 12, color: 'var(--text-dim)' }}>
                <span style={{ color: 'var(--warn)' }}>△ </span>
                {warning}
              </p>
            ))}
          </div>
        ) : null}

        {run.status === 'failed' ? (
          <div className="pl-panel p-3" style={{ borderColor: 'color-mix(in srgb, var(--down) 45%, var(--border))' }}>
            <p style={{ margin: 0, fontSize: 12, color: 'var(--down)' }}>The run failed.</p>
            <pre
              className="mono pl-scroll"
              style={{ margin: '6px 0 0', fontSize: 11, maxHeight: 260, whiteSpace: 'pre-wrap' }}
            >
              {run.error}
            </pre>
          </div>
        ) : null}

        {/* Before the "is it done" gate, deliberately. Everything below that gate is a
            results page, and a running session has no results yet -- it has a position, a
            distance to liquidation and a connection, which is what this shows instead
            (spec 10.3). */}
        {/* Boundaried on its own. The monitor draws the most fields of any view here and
            polls them once a second, so it is the most likely thing on the page to meet a
            shape it did not expect -- and it is the view an operator is looking at while
            real money is at the exchange. A fault in it must cost the monitor, not the run
            page and not the stop controls in the Runs list. */}
        {session && !TERMINAL.has(run.status) ? (
          <ErrorBoundary
            what="The live monitor"
            resetKey={runId}
            note={`Session #${runId} is unaffected — stop it from the Runs list, or use the kill switch.`}
          >
            <LiveMonitor runId={runId} />
          </ErrorBoundary>
        ) : null}

        {!done ? null : <RiskSection detail={detail.data} />}

        {/* --------------------------------------------------------------- charts */}
        {/* Outside the `done` guard. The worker republishes a thinned equity curve while
            the run is in flight, and watching it fill in is what makes a long run legible
            -- the alternative is an empty panel for an hour and everything at once. */}
        {done || live ? (
          <Section
            title="Equity"
            subtitle={
              equity.data?.partial
                ? 'Still running: the curve grows as the run proceeds, and drawdown is measured against the highest peak so far.'
                : 'Drawdown shaded beneath, measured on every mark-to-market tick.'
            }
          >
            {equity.data ? (
              <>
                <ChartFrame name={`run-${run.id}-equity`}>
                  <EquityChart ts={equity.data.ts} equity={equity.data.equity} drawdown={equity.data.drawdown} />
                </ChartFrame>
                <p className="mono" style={{ fontSize: 11, color: 'var(--text-mute)', margin: '4px 0 0' }}>
                  {equity.data.returned.toLocaleString()} extremes drawn from{' '}
                  {equity.data.samples.toLocaleString()} mark-to-market samples
                  {equity.data.partial ? ' so far' : ''}
                </p>
              </>
            ) : live ? (
              // Distinct from `<Loading/>`: a running run with no curve yet is not slow, it
              // is early. The first checkpoint lands a couple of seconds in.
              <p style={{ fontSize: 12, color: 'var(--text-mute)', margin: 0 }}>
                Waiting for the first mark-to-market checkpoint…
              </p>
            ) : (
              <Loading />
            )}
          </Section>
        ) : null}

        {!done ? null : (
          <>
            <Section title="Price and trades" subtitle="▲ entry · ▼ exit · green won, red lost, amber liquidated.">
              {price.data && trades.data ? (
                <ChartFrame name={`run-${run.id}-price`}>
                  <PriceChart ts={price.data.ts} close={price.data.close} trades={trades.data.trades} />
                </ChartFrame>
              ) : (
                <Loading />
              )}
            </Section>

            {/* -------------------------------------------------------------- metrics */}
            {data.metrics ? (
              <MetricCards
                metrics={data.metrics}
                trials={trials.data}
                netPnl={data.attribution?.net_pnl ?? run.net_pnl}
              />
            ) : null}

            {data.attribution ? (
              <Section
                title="PnL attribution"
                subtitle={
                  // A liquidation forfeits the whole isolated allocation, and that penalty is
                  // its own column (see attribution.py). Naming only four terms made the
                  // caption arithmetically false for exactly the runs that most need reading.
                  Number(data.attribution.liquidation_cost) !== 0
                    ? 'Price, funding, fees, slippage and the liquidation penalty sum exactly to net PnL (spec 8.4).'
                    : 'Price, funding, fees and slippage sum exactly to net PnL (spec 8.4).'
                }
              >
                <AttributionBar attribution={data.attribution} />
              </Section>
            ) : null}

            {trades.data ? <TradeTable runId={runId} trades={trades.data.trades} /> : null}

            <EventLog runId={runId} />

            {/* Only asked for on a session: a plain backtest has nothing to be a shadow of,
                and the endpoint would 404 on every one of them. The component renders
                nothing when the session has no shadow run either. */}
            {session ? <ParityReport runId={runId} /> : null}

            <Section title="Reproducibility" subtitle="Identical inputs must produce an identical event-log hash (spec 12.1).">
              <dl className="mono" style={{ fontSize: 11, margin: 0, display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '2px 12px' }}>
                <Row k="event_hash" v={run.event_hash ?? '—'} />
                <Row k="code_sha256" v={String(spec.code_sha256 ?? '—')} />
                <Row k="params" v={JSON.stringify(spec.params ?? {})} />
                <Row k="fees" v={JSON.stringify(spec.fees ?? {})} />
                <Row k="latency" v={JSON.stringify(spec.latency ?? {})} />
                <Row k="fill_tier requested" v={String(spec.fill_tier ?? '—')} />
                <Row k="fill_tier executed" v={String(run.fill_tier ?? '—')} />
                {/* The model that actually priced the fills, out of the manifest — not the
                    one in the request. A degraded run carries the requested tier's
                    parameters in its spec and never uses them, and showing those beside
                    "executed: BOOK_TICKER" would say a depth-exhaustion penalty was applied
                    by a model that never walks a ladder. */}
                <Row k="fill_model executed" v={JSON.stringify(executedModel(data.manifest, spec))} />
                <Row k="opening_balance" v={String(spec.opening_balance ?? '—')} />
                <Row k="leverage" v={String(spec.leverage ?? '—')} />
                <Row
                  k="dataset sha"
                  v={datasetLine(data.manifest)}
                />
                <Row k="platform_commit" v={commitLine(data.manifest)} />
                <Row k="duration" v={formatDuration(run.duration_ms)} />
                <Row k="finished" v={run.finished_ms ? formatTime(run.finished_ms) : '—'} />
              </dl>
            </Section>
          </>
        )}
      </div>
    </section>
  )
}

function executedModel(
  manifest: Record<string, unknown> | undefined,
  spec: Record<string, unknown>,
): unknown {
  const repro = manifest?.reproducibility as Record<string, unknown> | undefined
  return repro?.fill_model ?? spec.fill_model ?? {}
}

function datasetLine(manifest: Record<string, unknown> | undefined): string {
  const dataset = manifest?.dataset as { datasets?: Record<string, { sha256: string; rows: number }> } | undefined
  if (!dataset?.datasets) return '—'
  return Object.entries(dataset.datasets)
    .map(([name, entry]) => `${name} ${entry.sha256.slice(0, 10)} (${entry.rows.toLocaleString()} rows)`)
    .join('  ·  ')
}

function commitLine(manifest: Record<string, unknown> | undefined): string {
  const repro = manifest?.reproducibility as { platform_commit?: string | null } | undefined
  // `null` is recorded when there is no repository, and it is shown as such. A placeholder
  // would look like a recorded value and let two runs from different code look identical.
  return repro?.platform_commit ?? 'none (not a git checkout)'
}

function Row({ k, v }: { k: string; v: string }) {
  return (
    <>
      <dt style={{ color: 'var(--text-mute)' }}>{k}</dt>
      <dd style={{ margin: 0, color: 'var(--text-dim)', wordBreak: 'break-all' }}>{v}</dd>
    </>
  )
}

/* ---------------------------------------------------------------------- metric cards */

function MetricCards({
  metrics,
  trials,
  netPnl,
}: {
  metrics: Metrics
  trials: { combinations: number; selection_bias_sd: number | null } | undefined
  netPnl: string | null
}) {
  const pnl = netPnl == null ? null : Number(netPnl)
  return (
    <Section
      title="Metrics"
      subtitle={`Returns on a ${metrics.grid} grid over ${metrics.periods} whole periods, annualised by ${metrics.periods_per_year} (spec 8.1).`}
    >
      {/* The account reached zero. Every ratio below is computed over the periods *before*
          that, so it has to be said out loud — a Sharpe over the run-up to a wipe-out is a
          number about a strategy that no longer exists. */}
      {metrics.truncated_at_ms != null ? (
        <p
          className="mono mb-2"
          style={{ fontSize: 11, color: 'var(--down)' }}
        >
          ⚠ equity reached zero at {formatTime(metrics.truncated_at_ms)}. The return series
          stops there, so Sharpe, Sortino and volatility describe only the {metrics.periods}{' '}
          period(s) before the account was wiped out.
        </p>
      ) : null}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(150px, 1fr))',
          gap: 8,
        }}
      >
        <Card
          label="Net PnL"
          value={pnl == null ? '—' : money(netPnl)}
          colour={pnl == null ? undefined : pnl >= 0 ? 'var(--pos)' : 'var(--down)'}
        />
        <Card label="Total return" value={pct(metrics.total_return)} />
        <Card
          label="Sharpe"
          value={num(metrics.sharpe)}
          note={
            trials
              ? `${trials.combinations} param combination${trials.combinations === 1 ? '' : 's'} tried` +
                (trials.selection_bias_sd != null
                  ? ` · best-of-N bias ≈ ${trials.selection_bias_sd.toFixed(2)} sd`
                  : '')
              : undefined
          }
        />
        <Card label="Sortino" value={num(metrics.sortino)} />
        <Card label="CAGR" value={pct(metrics.cagr)} />
        <Card label="Calmar" value={num(metrics.calmar)} />
        <Card
          label="Max drawdown"
          value={pct(metrics.max_drawdown)}
          colour="var(--down)"
          note={`grid-close ${pct(metrics.max_drawdown_grid)}`}
        />
        <Card label="Ulcer index" value={pct(metrics.ulcer_index)} />
        <Card label="Volatility" value={pct(metrics.volatility)} />
        <Card label="Exposure" value={pct(metrics.exposure, 1)} />
        <Card label="Turnover" value={num(metrics.turnover, 1) + '×'} />
        <Card
          label="Round trips"
          value={String(metrics.trades.round_trips)}
          note={`${metrics.trades.legs} legs · ${metrics.trades.open_trades} open`}
        />
        <Card label="Win rate" value={pct(metrics.trades.win_rate, 1)} note={`${metrics.trades.wins}W / ${metrics.trades.losses}L`} />
        <Card label="Profit factor" value={num(metrics.trades.profit_factor)} />
        <Card label="Payoff ratio" value={num(metrics.trades.payoff_ratio)} />
        <Card label="Expectancy" value={num(metrics.trades.expectancy)} note="per round trip" />
        <Card label="Avg duration" value={formatDuration(metrics.trades.avg_duration_ms)} />
        <Card
          label="Per-trade Sharpe"
          value={num(metrics.trades.per_trade_sharpe)}
          note="not annualised; not comparable to Sharpe"
        />
      </div>
    </Section>
  )
}

function Card({
  label,
  value,
  note,
  colour,
}: {
  label: string
  value: string
  note?: string
  colour?: string
}) {
  return (
    <div className="pl-card">
      <div className="pl-card-label">{label}</div>
      <div className="pl-card-value" style={{ color: colour ?? 'var(--text)' }}>
        {value}
      </div>
      {note ? (
        <div style={{ fontSize: 10, color: 'var(--text-mute)', marginTop: 2 }}>{note}</div>
      ) : null}
    </div>
  )
}

/* ----------------------------------------------------------------------- trade table */

type SortKey = 'index' | 'entry_ms' | 'net_pnl' | 'mae' | 'mfe' | 'duration_ms'

function TradeTable({ runId, trades }: { runId: number; trades: Trade[] }) {
  const [sort, setSort] = useState<SortKey>('index')
  const [descending, setDescending] = useState(false)

  // Only a hedged run gets the column (matching the monitor's convention): in one-way mode
  // every row would read BOTH, which is noise. In hedge mode it is the only field that
  // tells a symbol's two rows apart — the CSV export always carried it while the table
  // showed two indistinguishable rows per symbol.
  const hedged = trades.some(
    (trade) => trade.position_side != null && trade.position_side !== 'BOTH',
  )

  const sorted = useMemo(() => {
    const copy = trades.slice()
    copy.sort((a, b) => {
      const av = sortValue(a, sort)
      const bv = sortValue(b, sort)
      return descending ? bv - av : av - bv
    })
    return copy
  }, [trades, sort, descending])

  const head = (key: SortKey, label: string, align: 'left' | 'right' = 'right') => (
    <th
      onClick={() => {
        if (sort === key) setDescending((d) => !d)
        else {
          setSort(key)
          setDescending(false)
        }
      }}
      style={{
        padding: '5px 8px',
        textAlign: align,
        fontWeight: 500,
        color: sort === key ? 'var(--text)' : 'var(--text-mute)',
        borderBottom: '1px solid var(--border)',
        cursor: 'pointer',
        whiteSpace: 'nowrap',
      }}
    >
      {label}
      {sort === key ? (descending ? ' ↓' : ' ↑') : ''}
    </th>
  )

  return (
    <Section
      title={`Trades (${trades.length})`}
      subtitle="A trade is flat → flat; scale-ins and partial exits are legs within one (spec 8.1)."
      action={
        <a className="pl-btn" href={api.tradesCsvUrl(runId)} download>
          Export CSV
        </a>
      }
    >
      <div className="pl-scroll" style={{ maxHeight: 380 }}>
        <table className="w-full mono" style={{ borderCollapse: 'collapse', fontSize: 11 }}>
          <thead>
            <tr>
              {head('index', '#', 'left')}
              <th style={{ padding: '5px 8px', textAlign: 'left', fontWeight: 500, color: 'var(--text-mute)', borderBottom: '1px solid var(--border)' }}>
                Side
              </th>
              {hedged ? (
                <th
                  title="Which of the symbol's two positions the round-trip lived in (hedge mode). Side says which way the trade faced; this says which book it was in."
                  style={{ padding: '5px 8px', textAlign: 'left', fontWeight: 500, color: 'var(--text-mute)', borderBottom: '1px solid var(--border)', cursor: 'help' }}
                >
                  Slot
                </th>
              ) : null}
              {head('entry_ms', 'Entry')}
              <th style={{ padding: '5px 8px', textAlign: 'right', fontWeight: 500, color: 'var(--text-mute)', borderBottom: '1px solid var(--border)' }}>
                Exit
              </th>
              <th style={{ padding: '5px 8px', textAlign: 'right', fontWeight: 500, color: 'var(--text-mute)', borderBottom: '1px solid var(--border)' }}>
                Qty
              </th>
              {head('net_pnl', 'Net PnL')}
              <th style={{ padding: '5px 8px', textAlign: 'right', fontWeight: 500, color: 'var(--text-mute)', borderBottom: '1px solid var(--border)' }}>
                Fees
              </th>
              <th style={{ padding: '5px 8px', textAlign: 'right', fontWeight: 500, color: 'var(--text-mute)', borderBottom: '1px solid var(--border)' }}>
                Funding
              </th>
              {head('mae', 'MAE')}
              {head('mfe', 'MFE')}
              {head('duration_ms', 'Held')}
              <th style={{ padding: '5px 8px', textAlign: 'left', fontWeight: 500, color: 'var(--text-mute)', borderBottom: '1px solid var(--border)' }}>
                Closed by
              </th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((trade) => {
              const pnl = Number(trade.net_pnl)
              return (
                <tr key={trade.index}>
                  <Cell align="left">{trade.index}</Cell>
                  <Cell align="left" colour={trade.side === 'LONG' ? 'var(--pos)' : 'var(--down)'}>
                    {trade.side}
                  </Cell>
                  {hedged ? (
                    <Cell align="left" colour="var(--text-dim)">
                      {trade.position_side ?? 'BOTH'}
                    </Cell>
                  ) : null}
                  <Cell title={formatTime(trade.entry_ms)}>{money(trade.entry_price)}</Cell>
                  <Cell title={trade.exit_ms ? formatTime(trade.exit_ms) : 'still open'}>
                    {trade.exit_price ? money(trade.exit_price) : '—'}
                  </Cell>
                  <Cell>{trade.max_qty}</Cell>
                  <Cell colour={pnl >= 0 ? 'var(--pos)' : 'var(--down)'}>{money(trade.net_pnl)}</Cell>
                  <Cell colour="var(--text-mute)">{money(trade.fees)}</Cell>
                  <Cell colour={Number(trade.funding) >= 0 ? 'var(--text-dim)' : 'var(--text-mute)'}>
                    {money(trade.funding)}
                  </Cell>
                  <Cell colour="var(--down)" title={`worst mark ${money(trade.mae_price)}`}>
                    {money(trade.mae)}
                  </Cell>
                  <Cell colour="var(--pos)" title={`best mark ${money(trade.mfe_price)}`}>
                    {money(trade.mfe)}
                  </Cell>
                  <Cell>{formatDuration(trade.duration_ms)}</Cell>
                  <Cell
                    align="left"
                    colour={
                      trade.close_reason === 'liquidation'
                        ? 'var(--warn)'
                        : trade.close_reason === 'open'
                          ? 'var(--text-mute)'
                          : undefined
                    }
                  >
                    {trade.close_reason}
                  </Cell>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </Section>
  )
}

function sortValue(trade: Trade, key: SortKey): number {
  if (key === 'index') return trade.index
  if (key === 'entry_ms') return trade.entry_ms
  if (key === 'duration_ms') return trade.duration_ms ?? 0
  return Number(trade[key])
}

function Cell({
  children,
  align = 'right',
  colour,
  title,
}: {
  children: React.ReactNode
  align?: 'left' | 'right'
  colour?: string
  title?: string
}) {
  return (
    <td
      title={title}
      style={{
        padding: '4px 8px',
        textAlign: align,
        color: colour,
        borderBottom: '1px solid var(--border)',
        whiteSpace: 'nowrap',
      }}
    >
      {children}
    </td>
  )
}

/* ------------------------------------------------------------------------- event log */

// Every kind the engine emits. Five were missing after Phase 5 and all five were the new
// ones — including EXPIRE, which is where a post-only order that silently failed to enter
// lands, and TRIGGER, which is the only record that a stop fired.
const KINDS = [
  '',
  'ORDER',
  'ORDER_WORKING',
  'TRIGGER',
  'FILL',
  'REJECT',
  'CANCEL',
  'CANCEL_TOO_LATE',
  'EXPIRE',
  'LIQUIDATION',
  'FUNDING',
  'FUNDING_UNSETTLED',
  'LOG',
  'RECORD',
]

/**
 * The event-log viewer.
 *
 * Paged on the **server**, not in the browser. The alternative — fetch the log and filter it
 * here — means a search that only looks at what was already downloaded, so a run with a
 * million entries would report "no matches" for something plainly in the log. The page size
 * is the window; the filter and the search run over the whole file.
 */
function EventLog({ runId }: { runId: number }) {
  const [offset, setOffset] = useState(0)
  const [kind, setKind] = useState('')
  const [search, setSearch] = useState('')
  const limit = 100

  const events = useQuery({
    queryKey: ['events', runId, offset, kind, search],
    queryFn: () => api.events(runId, { offset, limit, kind: kind || undefined, q: search || undefined }),
  })

  const total = events.data?.total ?? 0

  return (
    <Section
      title="Event log"
      subtitle="Everything the strategy decided, in the order the engine processed it. This is what the reproducibility hash covers."
      action={
        <span className="flex items-center gap-2">
          <select
            className="pl-input"
            style={{ width: 130 }}
            value={kind}
            onChange={(e) => {
              setKind(e.target.value)
              setOffset(0)
            }}
          >
            {KINDS.map((k) => (
              <option key={k} value={k}>
                {k || 'All kinds'}
              </option>
            ))}
          </select>
          <input
            className="pl-input"
            style={{ width: 170 }}
            placeholder="search…"
            value={search}
            onChange={(e) => {
              setSearch(e.target.value)
              setOffset(0)
            }}
          />
        </span>
      }
    >
      <div className="pl-scroll" style={{ maxHeight: 320 }}>
        <table className="w-full mono" style={{ borderCollapse: 'collapse', fontSize: 11 }}>
          <tbody>
            {(events.data?.events ?? []).map((event) => (
              <tr key={event.seq}>
                <Cell align="left" colour="var(--text-mute)">
                  {event.seq}
                </Cell>
                <Cell align="left" colour="var(--text-mute)">
                  {formatTime(event.ts_ms)}
                </Cell>
                <Cell align="left" colour={kindColour(event.kind)}>
                  {event.kind}
                </Cell>
                <td
                  style={{
                    padding: '4px 8px',
                    color: 'var(--text-dim)',
                    borderBottom: '1px solid var(--border)',
                    wordBreak: 'break-all',
                  }}
                >
                  {JSON.stringify(event.payload)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {events.data && events.data.events.length === 0 ? (
          <p style={{ fontSize: 12, color: 'var(--text-mute)', padding: 8 }}>No matching entries.</p>
        ) : null}
      </div>
      <div className="flex items-center gap-2 mt-2">
        {/* Offset 0 is the *oldest* entry: the log is written in engine order and paged
            front to back, so a larger offset is later in the run. The labels said the
            opposite, which made every attempt to reach the end of a run walk to its start. */}
        <button className="pl-btn" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - limit))}>
          ← Earlier
        </button>
        <button
          className="pl-btn"
          disabled={offset + limit >= total}
          onClick={() => setOffset(offset + limit)}
        >
          Later →
        </button>
        <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
          {total ? `${offset + 1}–${Math.min(offset + limit, total)} of ${total.toLocaleString()}` : '0'}
        </span>
      </div>
    </Section>
  )
}

function kindColour(kind: string): string | undefined {
  if (kind === 'FILL') return 'var(--pos)'
  if (kind === 'REJECT' || kind === 'LIQUIDATION') return 'var(--down)'
  if (kind === 'CANCEL' || kind === 'CANCEL_TOO_LATE') return 'var(--warn)'
  if (kind === 'FUNDING') return 'var(--info)'
  return undefined
}

/* -------------------------------------------------------------------------- shells */

function Section({
  title,
  subtitle,
  action,
  children,
}: {
  title: string
  subtitle?: string
  action?: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <div className="pl-panel p-3">
      <div className="flex items-start gap-3 mb-2">
        <div style={{ flex: 1, minWidth: 0 }}>
          <h2 className="pl-heading" style={{ margin: 0 }}>{title}</h2>
          {subtitle ? (
            <p style={{ margin: '3px 0 0', fontSize: 11, color: 'var(--text-mute)' }}>{subtitle}</p>
          ) : null}
        </div>
        {action}
      </div>
      {children}
    </div>
  )
}

/** "Send to Lab" (spec 10.3): jumps to the Lab tab with this run preselected in the
 *  new-job form. The artefact the job produces is then linked back here permanently. */
function SendToLab({ runId }: { runId: number }) {
  const sendToLab = useUi((s) => s.sendToLab)
  return (
    <button className="pl-btn" onClick={() => sendToLab(runId)}>
      Send to Lab
    </button>
  )
}

function Panel({ children }: { children: React.ReactNode }) {
  return (
    <section className="flex-1 p-6" style={{ fontSize: 12, color: 'var(--text-mute)' }}>
      {children}
    </section>
  )
}

/** Run ids get pasted into the Feed picker, `curl`s and bug notes — one click, no drag-select. */
function CopyId({ runId }: { runId: number }) {
  const { copied, copy } = useCopy()
  return (
    <button
      type="button"
      className="pl-btn pl-btn-icon"
      style={{ height: 20, width: 20, fontSize: 10 }}
      title={copied ? 'Copied' : `Copy run id ${runId}`}
      aria-label={`Copy run id ${runId}`}
      onClick={() => copy(String(runId))}
    >
      {copied ? '✓' : '⧉'}
    </button>
  )
}

function Loading() {
  return (
    <div className="flex items-center justify-center" style={{ height: 120, fontSize: 12, color: 'var(--text-mute)' }}>
      Loading…
    </div>
  )
}


/** Spec 7's halt, stated before anything else on the page.
 *
 *  A halted run's metrics describe a run that was *stopped*, not one that finished, and
 *  every number below this banner has to be read that way. Putting it under the charts
 *  would let someone read a Sharpe ratio for eleven days of a ninety-day range and never
 *  learn that the other seventy-nine did not happen. */
function HaltBanner({ detail }: { detail?: RunDetailPayload }) {
  const halt = (detail?.summary as { halt_reason?: RiskBreach | null } | undefined)?.halt_reason
  if (!halt) return null
  return (
    <div
      className="pl-panel p-3"
      style={{ borderColor: 'color-mix(in srgb, var(--down) 55%, var(--border))' }}
    >
      <p style={{ margin: 0, fontSize: 12, color: 'var(--down)', fontWeight: 600 }}>
        Halted by the risk layer: {halt.limit}
      </p>
      <p style={{ margin: '4px 0 0', fontSize: 12, color: 'var(--text-dim)' }}>
        {halt.detail} - observed <span className="mono">{halt.observed}</span> against a limit
        of <span className="mono">{halt.allowed}</span>.
      </p>
      <p style={{ margin: '4px 0 0', fontSize: 11, color: 'var(--text-mute)' }}>
        The run stopped here. Metrics below cover the part that ran, and there is no data
        after this point because there was no run after this point.
      </p>
    </div>
  )
}

/** Every refusal, in one table.
 *
 *  A risk layer that quietly drops orders produces a strategy whose backtest shows it doing
 *  something it never did: the equity curve of a strategy that was mostly blocked looks
 *  exactly like the equity curve of a strategy that mostly declined to trade. This table is
 *  what tells them apart, so it is rendered whenever there is anything in it. */
function RiskSection({ detail }: { detail?: RunDetailPayload }) {
  const breaches = detail?.risk_breaches ?? []
  const risk = (detail?.summary as { risk?: RiskSummary } | undefined)?.risk
  if (!breaches.length && !risk) return null
  const rejects = breaches.filter((b) => b.action === 'REJECT')
  const limits = risk?.limits ?? {}
  // Two independent facts, and reading one off the other was a bug. `anyLimit` says a risk
  // layer was in force; `unbounded` says nothing capped position size. A run with only a
  // rate limit is both — and the page used to print "nothing was refused because nothing
  // could be" directly above a table of twenty refusals.
  const active = Object.entries(limits).filter(([, v]) => v != null && v !== false)
  const anyLimit = active.length > 0
  const unbounded = limits.max_leverage == null && limits.max_position_notional == null
  return (
    <Section
      title="Risk"
      subtitle={
        !anyLimit
          ? 'No limits were set on this run. Nothing below was refused because nothing could be.'
          : unbounded
            ? 'Limits were in force, but none of them capped position size.'
            : 'Every order the risk layer refused, and why. A blocked strategy and a quiet one look identical without this.'
      }
    >
      <LimitsInForce limits={limits} />
      {rejects.length ? (
        <table className="pl-table mono" style={{ fontSize: 11 }}>
          <thead>
            <tr>
              <th style={{ textAlign: 'left' }}>limit</th>
              <th style={{ textAlign: 'right' }}>count</th>
              <th style={{ textAlign: 'right' }}>worst observed</th>
              <th style={{ textAlign: 'right' }}>allowed</th>
            </tr>
          </thead>
          <tbody>
            {summariseBreaches(rejects).map((row) => (
              <tr key={row.limit}>
                <td>{row.limit}</td>
                <td style={{ textAlign: 'right' }}>{row.count}</td>
                <td style={{ textAlign: 'right' }}>{row.worst}</td>
                <td style={{ textAlign: 'right' }}>{row.allowed}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p style={{ fontSize: 12, color: 'var(--text-mute)', margin: 0 }}>
          {anyLimit ? 'No order was refused.' : 'No limits were set.'}
        </p>
      )}
    </Section>
  )
}

interface RiskSummary {
  limits?: Record<string, string | number | boolean | null>
  halted?: boolean
  breach_count?: number
  rejected_orders?: number
  peak_equity?: string
}

/** Group refusals by limit, keeping the most extreme observation of each.
 *
 *  Most extreme by string comparison would be wrong -- '9' sorts above '80000' -- so the
 *  comparison is numeric, and a non-numeric observation (a count, or the word 'inf') falls
 *  back to the first one seen rather than being silently dropped. */
function summariseBreaches(breaches: RiskBreach[]) {
  const byLimit = new Map<string, { limit: string; count: number; worst: string; allowed: string }>()
  for (const breach of breaches) {
    const existing = byLimit.get(breach.limit)
    if (!existing) {
      byLimit.set(breach.limit, {
        limit: breach.limit,
        count: 1,
        worst: breach.observed,
        allowed: breach.allowed,
      })
      continue
    }
    existing.count += 1
    const next = Number(breach.observed)
    const current = Number(existing.worst)
    if (Number.isFinite(next) && Number.isFinite(current) && next > current) {
      existing.worst = breach.observed
    }
  }
  return [...byLimit.values()].sort((a, b) => b.count - a.count)
}


/** The limits a run actually executed under.
 *
 *  Spec 12.1 names risk limits among a run's recorded inputs, and until this existed they
 *  were shown nowhere on the page — only inferable from a badge, and the badge was wrong
 *  for a whole class of runs. A limit nobody can see is a limit nobody can check. */
function LimitsInForce({ limits }: { limits: Record<string, string | number | boolean | null> }) {
  const rows = LIMIT_ORDER.filter((key) => limits[key] != null && limits[key] !== false)
  if (!rows.length) return null
  return (
    <div style={{ marginBottom: 10 }}>
      <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '0 0 4px' }}>In force</p>
      <span className="flex flex-wrap gap-1">
        {rows.map((key) => (
          <span key={key} className="pl-tag mono" title={LIMIT_HELP[key] ?? key}>
            {key} {typeof limits[key] === 'boolean' ? 'on' : String(limits[key])}
          </span>
        ))}
      </span>
    </div>
  )
}

/** Ordered so the two that bound size come first — those are the ones whose absence makes a
 *  run unbounded, and the ones a reader looks for. */
const LIMIT_ORDER = [
  'max_position_notional',
  'max_leverage',
  'max_drawdown_pct',
  'max_daily_loss_pct',
  'min_equity_pct',
  'max_open_orders',
  'max_orders_per_minute',
  'max_consecutive_losses',
  'max_consecutive_rejections',
  'halt_on_liquidation',
]

const LIMIT_HELP: Record<string, string> = {
  max_position_notional:
    'Ceiling on projected exposure times the mark, counting every order still in flight. Rejects the order.',
  max_leverage: 'Projected gross notional across all symbols, over equity. Rejects the order.',
  max_drawdown_pct:
    'Fraction below peak mark-to-market equity, scored on the intrabar band rather than the close. Halts the run.',
  max_daily_loss_pct:
    "Fraction of the run's starting equity, measured from each UTC day's close-of-previous-day equity. Halts the run.",
  min_equity_pct: 'Fraction of the starting balance. Halts the run at or below it.',
  max_open_orders: 'Ceiling on live orders. Reduce-only exits are exempt.',
  max_orders_per_minute: 'Rolling-minute submission ceiling — the runaway-loop guard.',
  max_consecutive_losses: 'Closed round-trips that lost in a row. Halts the run.',
  max_consecutive_rejections:
    'Consecutive rejections from the exchange or the ledger. Trips the kill switch. Risk-layer refusals do not count.',
  halt_on_liquidation: 'Any liquidation stops the run immediately.',
}
