/**
 * The Runs table and the New Run dialog (spec 10.3).
 *
 * **Polling is conditional on there being something to poll for.** `refetchInterval` returns
 * `false` once every run is terminal, so an idle Runs tab is silent. A fixed interval would
 * keep a laptop's radio and a DuckDB-backed process awake all day to re-fetch a list that
 * cannot have changed.
 *
 * **A run that failed is a first-class row, not a hidden one.** Its error is the whole point
 * of looking at it, so the status cell carries the message on hover and the detail page
 * prints it in full. Filtering failures out of the default view is how someone concludes a
 * strategy "has no runs" when it has six that all crashed on the same line.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useMemo, useState } from 'react'
import {
  api,
  ApiError,
  FILL_TIERS,
  formatDate,
  formatDuration,
  money,
  num,
  parseUtcDate,
  pct,
  POSITION_MODE_BLURBS,
  RISK_DEFAULTS,
  RUNS_PAGE_LIMIT,
  MARGIN_MODE_BLURBS,
  TIER_BLURBS,
  type FillTier,
  type MarginMode,
  type Run,
  type Strategy,
  type TierCapabilities,
  type TierPreview,
} from '../api'
import { useUi } from '../store'
import { CsvButton } from './Export'

/** `lost` is terminal. A run whose worker has gone can never report anything again, so a
 *  poller waiting for it to finish waits forever -- which is what this list did, at 1.5 s,
 *  for as long as the tab stayed open. */
const TERMINAL = new Set(['done', 'failed', 'cancelled', 'lost'])

export function RunList() {
  const selectedRunId = useUi((s) => s.selectedRunId)
  const selectRun = useUi((s) => s.selectRun)
  const openRunDraft = useUi((s) => s.openRunDraft)
  const openSessionDraft = useUi((s) => s.openSessionDraft)
  const [archived, setArchived] = useState(false)

  const runs = useQuery({
    queryKey: ['runs', archived],
    queryFn: () => api.runs({ archived }),
    refetchInterval: (query) => {
      const data = query.state.data
      if (!data) return 2000
      return data.runs.some((run) => !TERMINAL.has(run.status)) ? 1500 : false
    },
  })

  const items = runs.data?.runs ?? []

  return (
    <section className="flex flex-col flex-1" style={{ minWidth: 0 }}>
      <div
        className="flex items-center gap-2 px-3 shrink-0"
        style={{ height: 40, borderBottom: '1px solid var(--border)' }}
      >
        <button
          className="pl-btn"
          onClick={() => openSessionDraft(true)}
          title="Start a paper or live session against the exchange. Shows what leverage other running sessions already hold on a symbol before you commit."
        >
          Start session
        </button>
        <button className="pl-btn pl-btn-primary" onClick={() => openRunDraft(-1)}>
          New backtest
        </button>
        <span className="flex-1" />
        <label className="flex items-center gap-1.5" style={{ fontSize: 12, color: 'var(--text-dim)' }}>
          <input type="checkbox" checked={archived} onChange={(e) => setArchived(e.target.checked)} />
          Archived
        </label>
        <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
          {items.length} run{items.length === 1 ? '' : 's'}
          {/* A list exactly one page long is almost certainly cut, not complete — the
              server returns the newest `limit` rows with no truncation marker of its own,
              so an unlabelled 200 reads as "everything" while older runs silently vanish
              (and so does this CSV export). Said here, where the count is. */}
          {items.length >= RUNS_PAGE_LIMIT ? (
            <span style={{ color: 'var(--warn)' }} title="The server returns the newest runs up to this page size. Older runs exist but are not listed here (or in the CSV export above).">
              {' '}· newest {RUNS_PAGE_LIMIT} only — older runs not shown
            </span>
          ) : null}
        </span>
        <CsvButton
          name="runs"
          headers={['id', 'strategy', 'version', 'mode', 'status', 'label', 'symbols', 'start_ms', 'end_ms', 'seed', 'fill_tier', 'net_pnl', 'sharpe', 'max_drawdown', 'round_trips', 'fills']}
          rows={items.map((run) => [
            run.id, run.strategy_name, run.version_no, run.mode, run.status, run.label,
            run.symbols.join(' '), run.start_ms, run.end_ms, run.seed, run.fill_tier,
            run.net_pnl, run.sharpe, run.max_drawdown, run.round_trips, run.fills,
          ])}
        />
      </div>

      <div className="pl-scroll flex-1">
        {runs.isLoading ? (
          <Placeholder text="Loading runs…" />
        ) : items.length === 0 ? (
          <Placeholder
            text={
              archived
                ? 'No archived runs.'
                : 'No runs yet. Press New backtest, or open a strategy and run it from the editor.'
            }
          />
        ) : (
          <table className="w-full" style={{ borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ color: 'var(--text-mute)', textAlign: 'left' }}>
                <Th>Run</Th>
                <Th>Strategy</Th>
                <Th>Range</Th>
                <Th>Status</Th>
                <Th align="right">Net PnL</Th>
                <Th align="right">Sharpe</Th>
                <Th align="right">MaxDD</Th>
                <Th align="right">Trades</Th>
                <Th>Fill tier</Th>
                <Th>Badges</Th>
              </tr>
            </thead>
            <tbody>
              {items.map((run) => (
                <RunRow
                  key={run.id}
                  run={run}
                  selected={run.id === selectedRunId}
                  onSelect={() => selectRun(run.id)}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  )
}

function Th({ children, align = 'left' }: { children: React.ReactNode; align?: 'left' | 'right' }) {
  return (
    <th
      style={{
        padding: '7px 10px',
        fontWeight: 600,
        fontSize: 10.5,
        letterSpacing: '0.05em',
        textTransform: 'uppercase',
        color: 'var(--text-mute)',
        textAlign: align,
        borderBottom: '1px solid var(--border)',
        position: 'sticky',
        top: 0,
        background: 'var(--bg)',
        zIndex: 1,
      }}
    >
      {children}
    </th>
  )
}

function Td({
  children,
  align = 'left',
  mono = false,
  colour,
  title,
}: {
  children: React.ReactNode
  align?: 'left' | 'right'
  mono?: boolean
  colour?: string
  title?: string
}) {
  return (
    <td
      title={title}
      className={mono ? 'mono' : undefined}
      style={{
        padding: '5px 10px',
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

function RunRow({ run, selected, onSelect }: { run: Run; selected: boolean; onSelect: () => void }) {
  const pnl = run.net_pnl == null ? null : Number(run.net_pnl)
  return (
    <tr
      onClick={onSelect}
      style={{
        cursor: 'pointer',
        background: selected ? 'var(--surface-2)' : undefined,
        transition: 'background 100ms var(--ease)',
      }}
      onMouseEnter={(e) => {
        if (!selected) e.currentTarget.style.background = 'color-mix(in srgb, var(--surface-2) 60%, transparent)'
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = selected ? 'var(--surface-2)' : ''
      }}
    >
      <Td mono colour="var(--text-dim)">
        #{run.id}
        {run.label ? <span style={{ marginLeft: 6, color: 'var(--text-mute)' }}>{run.label}</span> : null}
      </Td>
      <Td>
        {run.strategy_name}
        <span className="mono" style={{ color: 'var(--text-mute)', marginLeft: 4 }}>
          v{run.version_no}
        </span>
      </Td>
      <Td mono colour="var(--text-dim)">
        {run.start_ms && run.end_ms
          ? `${formatDate(run.start_ms)} → ${formatDate(run.end_ms)} · ${run.timeframe}`
          : '—'}
      </Td>
      <Td>
        <StatusCell run={run} />
      </Td>
      <Td mono align="right" colour={pnl == null ? undefined : pnl >= 0 ? 'var(--pos)' : 'var(--down)'}>
        {pnl == null ? '—' : money(run.net_pnl)}
      </Td>
      <Td mono align="right">
        {num(run.sharpe)}
      </Td>
      <Td mono align="right" colour={run.max_drawdown ? 'var(--down)' : undefined}>
        {pct(run.max_drawdown, 1)}
      </Td>
      <Td mono align="right">
        {run.round_trips ?? '—'}
      </Td>
      <Td>
        <TierLine run={run} />
      </Td>
      <Td>
        <Badges flags={run.flags} />
      </Td>
    </tr>
  )
}

export function StatusCell({ run }: { run: Run }) {
  if (run.status === 'running' || run.status === 'queued') {
    // A session has no total to be a fraction of: it runs until it is stopped, so
    // `progress_total` is zero and the percentage was rendering as a permanent `0%`. A
    // progress bar that never moves reads as a stuck run, which is the opposite of the
    // truth. Sessions get the bar count they have actually processed instead.
    if (run.progress_total <= 0 && run.status === 'running') {
      return (
        <span className="flex items-center gap-2" style={{ color: 'var(--text-dim)' }}>
          <span className="pl-spin" style={{ display: 'inline-block' }}>
            ◐
          </span>
          <span className="mono" style={{ fontSize: 11 }}>
            {run.progress_bars.toLocaleString()} bars
          </span>
        </span>
      )
    }
    const share = run.progress_total > 0 ? run.progress_bars / run.progress_total : 0
    return (
      <span className="flex items-center gap-2" style={{ color: 'var(--text-dim)' }}>
        <span className="pl-spin" style={{ display: 'inline-block' }}>
          ◐
        </span>
        <span className="pl-meter" style={{ width: 56, minWidth: 56, flex: 'none' }}>
          <span className="pl-meter-fill" style={{ width: `${Math.round(share * 100)}%` }} />
        </span>
        <span className="mono" style={{ fontSize: 11 }}>
          {run.status === 'queued' ? 'queued' : `${Math.round(share * 100)}%`}
        </span>
      </span>
    )
  }
  if (run.status === 'done') {
    return (
      <span style={{ color: 'var(--pos)' }} title={`took ${formatDuration(run.duration_ms)}`}>
        ✓ done
      </span>
    )
  }
  if (run.status === 'cancelled') {
    return <span style={{ color: 'var(--text-mute)' }}>■ cancelled</span>
  }
  // Not `failed`. A lost run reported nothing at all -- the worker went away without writing
  // a status, so there is no error to show and the artefacts on disk simply stop mid-run.
  // Rendering it as `✕ failed` sent people looking for a traceback that was never written,
  // and hid the one thing that is actually true: the reason is outside this run.
  if (run.status === 'lost') {
    return (
      <span
        style={{ color: 'var(--warn)' }}
        title={
          run.error ??
          'The worker process stopped without recording an outcome -- killed, out of memory, ' +
            'or the machine went down. Whatever the run wrote before that is on disk and is ' +
            'as far as it got; it cannot be resumed.'
        }
      >
        ⊘ lost
      </span>
    )
  }
  return (
    <span style={{ color: 'var(--down)' }} title={run.error ?? undefined}>
      ✕ failed
    </span>
  )
}

/** Warning badges (spec 10.3). Each is a claim about how far to trust the numbers, so each
 *  carries its own explanation rather than an abbreviation nobody can decode later. */
const BADGE_HELP: Record<string, string> = {
  LOW_FIDELITY:
    'Fills were modelled at the BAR_CLOSE tier: a market order takes the most recent bar print before it arrives, plus a fixed offset. Limit and stop orders are refused at this tier — spec 4.2 calls it explicit opt-in only.',
  TIER_DEGRADED:
    'This run asked for a higher-fidelity fill model than the lake can support over its range, and executed at a lower one. See the tier line for what was lost.',
  TIER_BELOW_DATA:
    'This run executed below the tier its data could have supported. That was the request, not a limitation — a deliberate low-fidelity sweep.',
  TICK_GAPS_TOLERATED:
    'The tick dataset this run priced fills from has holes in it, covering under 1% of the range — so the run kept its tier instead of demoting every bar to BAR_CLOSE to protect that fraction. The tier line names the dataset and the duration, and the gap report lists each hole. Fills that landed inside one were modelled without those records.',
  DEPTH_EXHAUSTED:
    'An order was larger than every published depth level and its remainder filled at the exhaustion penalty. If this recurs the position sizing is unrealistic for the instrument.',
  MAKER_FILLS:
    'At least one order filled passively, paying the maker rate. Fees for this run depend on the maker/taker split, not on the taker rate alone.',
  WARMUP_TIER_DIFFERS:
    'The warm-up period reaches back into data with thinner coverage than the traded range. The run executed at the traded range’s tier; the manifest records the lower one.',
  COVERAGE_INCOMPLETE:
    'The lake does not hold a published file in every partition of this range, so no dataset fingerprint could be built. The run’s own results are unaffected — there is simply nothing to compare a re-run against.',
  FILTERS_APPROXIMATE:
    'No exchangeInfo snapshot exists from before this range, so the nearest available one was used. Tick size, step size and minimum notional may not be the ones that applied at the time (spec 3.2).',
  BRACKETS_APPROXIMATE:
    'No leverageBracket snapshot exists from before this range. Maintenance margin — and therefore the liquidation price — is computed from a later table.',
  GAP_SKIPPED:
    'Some bars were built from fewer than their full complement of 1-minute bars, or mark samples are missing. OHLC is real; volume is understated and risk is checked less often across the hole.',
  FUNDING_MISSING:
    'No funding settlements were found in this range. A perp backtest without funding overstates shorts and understates longs.',
  FUNDING_UNSETTLED:
    'A funding settlement had no mark price to settle against, so its cashflow was not booked. Spec 3.4 forbids inferring a mark, and a made-up one would move a real balance — so the funding column understates what the position actually paid. Ingest the missing markPriceKlines before trusting the attribution.',
  FILL_MODEL_SUBSTITUTED:
    'This run degraded to a lower tier, so the fill model it executed with is that tier’s default — any parameter set on the requested model had no effect on these numbers. The executed model is in the Reproducibility block.',
  ZERO_LATENCY:
    'Latency was set to zero, so every order filled at the print that triggered it. These numbers are an upper bound, not an estimate.',
  ORDERS_REJECTED:
    'At least one order was refused at arrival — by an exchange filter or for want of margin. See the REJECT entries in the event log.',
  LIQUIDATED: 'A position was liquidated during this run.',
  WARMUP_SHORT:
    'The lake does not hold enough history before the start date to warm this strategy up, so its first trades happen later than the range begins.',
  FILL_TIER_LIMITED_BY_GAPS:
    'A dataset that would have supported a better fill model has an unexplained gap inside the range, so the tier was demoted.',
  FILTERS_CHANGED_MID_RANGE:
    'The exchange filters changed part-way through this range; orders were validated against the opening snapshot throughout.',
  BRACKETS_CHANGED_MID_RANGE:
    'The leverage brackets changed part-way through this range; margin was computed from the opening snapshot throughout.',
  RISK_LIMITED:
    'This run had risk limits in force (spec 7). Orders that would have breached one were refused, and the Risk section lists them.',
  RISK_UNBOUNDED:
    'No ceiling on position size: neither a notional limit nor a leverage limit was set, so nothing bounded how large this strategy could get. Independent of RISK_LIMITED — a run can have limits in force and still be unbounded in size.',
  AUTO_FLATTEN_UNMET:
    'A platform auto-flatten did not close the position — the exchange refused the exit. The deadline is retried on the next mark, and until it succeeds the position is held past the limit that was asked for.',
  RISK_REJECTED:
    'At least one order was refused by the risk layer. The equity curve is of a strategy that was partly prevented from trading, not of the strategy as written.',
  RISK_HALTED:
    'The run stopped early because a risk limit was breached. Metrics describe the part that ran; there is no data after the halt.',
  AUTO_FLATTENED:
    'The platform closed a position on a deadline the strategy did not set - a maximum hold time, or a funding settlement.',
}

export function Badges({ flags }: { flags: string[] }) {
  if (!flags.length) return null
  return (
    <span className="flex flex-wrap gap-1">
      {flags.map((flag) => (
        <span
          key={flag}
          className="pl-tag"
          title={BADGE_HELP[flag] ?? flag}
          style={{
            color: flag === 'LIQUIDATED' || flag === 'ZERO_LATENCY' ? 'var(--warn)' : undefined,
            cursor: 'help',
          }}
        >
          {flag}
        </span>
      ))}
    </span>
  )
}

/** Spec 4.2: *"That degradation is recorded in run metadata and shown as a badge on the
 *  results page. Never let a fill-model downgrade happen invisibly."*
 *
 *  Rendered wherever a run's tier is shown, so the answer to "why does this say
 *  BOOK_TICKER when I asked for BOOK_WALK" is always in the same place as the tier itself. */
export function TierLine({ run }: { run: Run }) {
  if (!run.fill_tier) return null
  const degraded = run.tier_degraded
  return (
    <span className="flex flex-wrap items-center gap-1">
      <span
        className="pl-tag mono"
        title={TIER_BLURBS[run.fill_tier as FillTier] ?? run.fill_tier}
        style={{ cursor: 'help' }}
      >
        {run.fill_tier}
      </span>
      {degraded ? (
        <span
          className="pl-tag"
          title={run.tier_reason ?? undefined}
          style={{ color: 'var(--warn)', cursor: 'help' }}
        >
          ↓ asked for {run.requested_fill_tier}
        </span>
      ) : null}
    </span>
  )
}

/** The same question asked *before* the run: what will this range actually execute at. */
function TierPreviewNote({
  preview,
  loading,
  error,
}: {
  preview: TierPreview | undefined
  loading: boolean
  error: unknown
}) {
  if (loading) {
    return (
      <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>
        checking what this range supports…
      </p>
    )
  }
  if (error) {
    return (
      <p style={{ fontSize: 11, color: 'var(--down)', margin: 0 }}>
        {error instanceof ApiError ? error.message : String(error)}
      </p>
    )
  }
  if (!preview) return null

  if (!preview.degraded) {
    return (
      <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>
        This range supports {preview.available}; the run will execute at {preview.tier}.{' '}
        <span title="This check reads which partitions hold published files. The worker additionally scans for holes inside them — too slow to do here, over a year of tick data — and a dataset holed across more than 1% of the range is dropped from the tiers it feeds. The run's own tier badge is the authoritative one, and it can only ever be lower than this.">
          Holes inside those files are checked when the run starts and can lower it. ⓘ
        </span>
      </p>
    )
  }
  return (
    <p style={{ fontSize: 11, color: 'var(--warn)', margin: 0 }}>
      ↓ This range only supports <strong>{preview.available}</strong>, so the run will execute
      there instead of at {preview.requested}. {lostSentence(preview)}
    </p>
  )
}

/** Names in the capability matrix are keys, not English. `limit, trigger orders will be
 *  refused` is what happens when they are joined raw, and a warning nobody can parse is a
 *  warning nobody reads. */
const CAPABILITY_WORDS: Record<string, string> = {
  limit: 'limit orders',
  trigger: 'stops, take-profits and trailing stops',
  book: 'ctx.book()',
}

function lostSentence(preview: TierPreview): string {
  const requested = preview.all_capabilities[preview.requested]
  if (!requested) return ''
  const lost = Object.keys(CAPABILITY_WORDS).filter(
    (name) =>
      requested[name as keyof TierCapabilities] &&
      !preview.capabilities[name as keyof TierCapabilities],
  )
  if (!lost.length) return ''
  const words = lost.map((name) => CAPABILITY_WORDS[name])
  const list =
    words.length === 1
      ? words[0]
      : `${words.slice(0, -1).join(', ')} and ${words[words.length - 1]}`
  // "Unavailable: x" rather than "X will be unavailable", so the sentence never has to
  // capitalise its first word -- one of them is `ctx.book()`, and `Ctx.book()` is a
  // different function.
  return `Unavailable: ${list}.`
}

function Placeholder({ text }: { text: string }) {
  return (
    <div className="p-6" style={{ fontSize: 12, color: 'var(--text-mute)', maxWidth: 620 }}>
      {text}
    </div>
  )
}

/* ------------------------------------------------------------------ new run dialog */

export function NewRunDialog() {
  const draftFor = useUi((s) => s.runDraftFor)
  const openRunDraft = useUi((s) => s.openRunDraft)
  const selectRun = useUi((s) => s.selectRun)
  const setTab = useUi((s) => s.setTab)
  const notify = useUi((s) => s.notify)
  const client = useQueryClient()

  // Its own key rather than sharing the list's. The sidebar's query is filtered by search,
  // tag and archived state; the dialog must offer every strategy regardless of what the
  // list happens to be showing, and reusing the key would hand it whatever filter was last
  // typed.
  const strategies = useQuery({
    queryKey: ['strategies-for-run'],
    queryFn: () => api.list(),
    enabled: draftFor != null,
  })
  // Spec 10.3's Settings are the *starting values* for this form. Fetched when the
  // dialog opens rather than held in a store, so a change saved in another tab is
  // picked up the next time a run is drafted.
  const settings = useQuery({
    queryKey: ['settings'],
    queryFn: api.settings,
    enabled: draftFor != null,
  })
  const [strategyId, setStrategyId] = useState<number | null>(null)
  const [label, setLabel] = useState('')
  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [balance, setBalance] = useState('10000')
  const [leverage, setLeverage] = useState('10')
  const [marginMode, setMarginMode] = useState<MarginMode>('ISOLATED')
  const [hedgeMode, setHedgeMode] = useState(false)
  const [seed, setSeed] = useState('0')
  const [slippage, setSlippage] = useState('1.0')
  const [impactK, setImpactK] = useState('10')
  const [depthPenalty, setDepthPenalty] = useState('0.001')
  const [tradeSpread, setTradeSpread] = useState('1.0')
  const [tier, setTier] = useState<FillTier>('BOOK_TICKER')
  const [latency, setLatency] = useState('120')
  const [latencyModel, setLatencyModel] = useState<'fixed' | 'lognormal'>('lognormal')
  const [latencyP99, setLatencyP99] = useState('600')
  const [params, setParams] = useState<Record<string, string>>({})
  // Spec 7's table, on by default. The engine's own default is no limits -- so that a
  // Phase 4 run replays as what it was -- but a person opening this dialog is choosing, and
  // choosing nothing should get them the limits the spec wrote rather than an unbounded
  // account. Turning them off is one click, and the run is badged RISK_UNBOUNDED for it.
  const [riskOn, setRiskOn] = useState(true)
  const [maxLeverage, setMaxLeverage] = useState<string>(RISK_DEFAULTS.max_leverage)
  const [maxNotional, setMaxNotional] = useState('')
  const [maxDailyLoss, setMaxDailyLoss] = useState<string>(RISK_DEFAULTS.max_daily_loss_pct)
  const [maxDrawdown, setMaxDrawdown] = useState<string>(RISK_DEFAULTS.max_drawdown_pct)
  const [maxOpenOrders, setMaxOpenOrders] = useState(String(RISK_DEFAULTS.max_open_orders))
  const [maxPerMinute, setMaxPerMinute] = useState(String(RISK_DEFAULTS.max_orders_per_minute))
  const [minEquity, setMinEquity] = useState<string>(RISK_DEFAULTS.min_equity_pct)
  const [haltOnLiquidation, setHaltOnLiquidation] = useState(true)
  const [flattenOnHalt, setFlattenOnHalt] = useState(false)
  const [maxHoldMinutes, setMaxHoldMinutes] = useState('')
  const [beforeFundingMinutes, setBeforeFundingMinutes] = useState('')

  const [seeded, setSeeded] = useState(false)
  useEffect(() => {
    if (draftFor == null) {
      setSeeded(false)
      return
    }
    const stored = settings.data?.settings
    if (stored == null || seeded) return
    setSeeded(true)
    setLeverage(String(stored.default_leverage))
    setLatencyModel(stored.latency_model === 'fixed' ? 'fixed' : 'lognormal')
    setLatency(String(stored.submit_ms))
    setRiskOn(stored.risk_enabled)
    setMaxLeverage(stored.max_leverage ?? '')
    setMaxDailyLoss(stored.max_daily_loss_pct ?? '')
    setMaxDrawdown(stored.max_drawdown_pct ?? '')
    if (stored.max_open_orders != null) setMaxOpenOrders(String(stored.max_open_orders))
    if (stored.max_orders_per_minute != null) setMaxPerMinute(String(stored.max_orders_per_minute))
    setMinEquity(stored.min_equity_pct ?? '')
    setFlattenOnHalt(stored.kill_switch_flatten)
  }, [draftFor, settings.data, seeded])

  // Esc closes, like every other modal (spec 10.4). Bound only while open, so a stray
  // Esc in the editor does not clear a dialog nobody can see.
  useEffect(() => {
    if (draftFor == null) return
    const handler = (event: KeyboardEvent) => {
      if (event.key === 'Escape') openRunDraft(null)
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [draftFor, openRunDraft])

  const chosen: Strategy | undefined = useMemo(
    () => strategies.data?.strategies.find((s) => s.id === strategyId),
    [strategies.data, strategyId],
  )
  const symbol = chosen?.head?.requires?.symbols?.[0] ?? 'BTCUSDT'

  const coverage = useQuery({
    queryKey: ['coverage', symbol],
    queryFn: () => api.coverage(symbol),
    enabled: draftFor != null,
  })

  // Preselect the strategy the dialog was opened for, and default the range to the last
  // year of data the lake actually has. Offering dates outside coverage produces a run that
  // fails inside the worker — a correct refusal, and a poor way to learn what is available.
  useEffect(() => {
    if (draftFor == null) return
    const list = strategies.data?.strategies ?? []
    const initial = draftFor > 0 ? draftFor : (list.find((s) => s.head?.valid)?.id ?? list[0]?.id ?? null)
    setStrategyId((current) => current ?? initial)
  }, [draftFor, strategies.data])

  useEffect(() => {
    if (!coverage.data?.start_ms || !coverage.data?.end_ms) return
    const hi = coverage.data.end_ms
    const lo = Math.max(coverage.data.start_ms, hi - 365 * 86_400_000)
    setStart((current) => current || formatDate(lo))
    setEnd((current) => current || formatDate(hi))
  }, [coverage.data])

  useEffect(() => {
    if (!chosen?.head?.params) return
    const next: Record<string, string> = {}
    for (const spec of chosen.head.params) next[spec.name] = String(spec.default)
    setParams(next)
  }, [chosen?.head?.id])

  const start_ms = parseUtcDate(start)
  const end_ms = parseUtcDate(end)
  const invalidRange = start_ms == null || end_ms == null || end_ms <= start_ms
  const invalidVersion = chosen != null && chosen.head != null && !chosen.head.valid

  // Spec 4.2 decision 3 wants a downgrade recorded and visible. Showing it on the results
  // page is necessary and late -- the user has already waited for the run -- so the same
  // resolution the worker will perform is run here, against the same range, before anything
  // is queued. Disabled while the range is invalid: previewing a nonsense range would answer
  // a question nobody asked and would 400.
  const preview = useQuery({
    queryKey: ['tier-preview', symbol, start_ms, end_ms, tier],
    queryFn: () => api.tierPreview(symbol, start_ms!, end_ms!, tier),
    enabled: draftFor != null && !invalidRange,
    retry: false,
  })

  const mutation = useMutation({
    mutationFn: () =>
      api.startRun({
        strategy_id: strategyId!,
        label,
        params,
        start_ms: start_ms!,
        end_ms: end_ms!,
        seed: Number(seed) || 0,
        opening_balance: balance,
        leverage: Number(leverage) || 1,
        margin_mode: marginMode,
        hedge_mode: hedgeMode,
        fill_tier: tier,
        slippage_bps: slippage,
        impact_k_bps: impactK,
        depth_exhaustion_pct: depthPenalty,
        trade_spread_bps: tradeSpread,
        latency_model: latencyModel,
        latency_submit_ms: Number(latency) || 0,
        latency_p99_ms: Number(latencyP99) || 600,
        risk_enabled: riskOn,
        // An empty box means *no limit*, not zero. Sending 0 would be a ceiling nothing can
        // satisfy, and the server refuses it -- correctly, and that is not what a blank
        // field means.
        max_leverage: blankIsNone(maxLeverage),
        max_position_notional: blankIsNone(maxNotional),
        max_daily_loss_pct: blankIsNone(maxDailyLoss),
        max_drawdown_pct: blankIsNone(maxDrawdown),
        max_open_orders: blankIsNoneInt(maxOpenOrders),
        max_orders_per_minute: blankIsNoneInt(maxPerMinute),
        min_equity_pct: blankIsNone(minEquity),
        halt_on_liquidation: haltOnLiquidation,
        kill_switch_flatten: flattenOnHalt,
        max_hold_ms: minutesToMs(maxHoldMinutes),
        before_funding_ms: minutesToMs(beforeFundingMinutes),
      }),
    onSuccess: (data) => {
      client.invalidateQueries({ queryKey: ['runs'] })
      openRunDraft(null)
      setTab('Runs')
      selectRun(data.run.id)
      notify(`Run #${data.run.id} started`)
    },
    onError: (error) => notify(error instanceof ApiError ? error.message : String(error), 'error'),
  })

  if (draftFor == null) return null

  return (
    <div
      className="pl-scrim"
      style={{ paddingTop: 64 }}
      onClick={() => openRunDraft(null)}
    >
      <div
        className="pl-modal pl-scroll p-4 flex flex-col gap-3"
        style={{ width: 'min(560px, calc(100vw - 32px))', maxHeight: '80vh', overflow: 'auto' }}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 style={{ margin: 0, fontSize: 14 }}>New backtest</h2>

        <Field label="Strategy">
          <select
            className="pl-input"
            value={strategyId ?? ''}
            onChange={(e) => setStrategyId(Number(e.target.value))}
          >
            {(strategies.data?.strategies ?? []).map((s) => (
              <option key={s.id} value={s.id}>
                {s.name} {s.head ? `(v${s.head.version_no}${s.head.valid ? '' : ' — invalid'})` : ''}
              </option>
            ))}
          </select>
        </Field>

        <div className="flex gap-3">
          <Field label="Start (UTC)">
            <input className="pl-input mono" value={start} onChange={(e) => setStart(e.target.value)} placeholder="2025-08-01" />
          </Field>
          <Field label="End (UTC, exclusive)">
            <input className="pl-input mono" value={end} onChange={(e) => setEnd(e.target.value)} placeholder="2026-08-01" />
          </Field>
        </div>
        {coverage.data?.start_ms && coverage.data?.end_ms ? (
          <p className="mono" style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>
            {symbol} klines cover {formatDate(coverage.data.start_ms)} → {formatDate(coverage.data.end_ms)}
          </p>
        ) : null}

        <div className="flex gap-3">
          <Field label="Balance">
            <input className="pl-input mono" value={balance} onChange={(e) => setBalance(e.target.value)} />
          </Field>
          <Field label="Leverage">
            <input className="pl-input mono" value={leverage} onChange={(e) => setLeverage(e.target.value)} />
          </Field>
          <Field label="Seed">
            <input className="pl-input mono" value={seed} onChange={(e) => setSeed(e.target.value)} />
          </Field>
        </div>

        <Field label="Margin mode" hint={MARGIN_MODE_BLURBS[marginMode]}>
          <select
            className="pl-input"
            value={marginMode}
            onChange={(e) => setMarginMode(e.target.value as MarginMode)}
          >
            <option value="ISOLATED">ISOLATED</option>
            <option value="CROSSED">CROSSED (not implemented)</option>
          </select>
        </Field>

        {/* The backtest route has always accepted `hedge_mode` (`runs.py
            StartRunRequest`); only the session dialog offered it, so a hedged strategy
            could be paper-traded but never backtested from the UI. */}
        <Field label="Position mode" hint={POSITION_MODE_BLURBS[hedgeMode ? 'hedge' : 'one-way']}>
          <select
            className="pl-input"
            value={hedgeMode ? 'hedge' : 'one-way'}
            onChange={(e) => setHedgeMode(e.target.value === 'hedge')}
          >
            <option value="one-way">one-way</option>
            <option value="hedge">hedge — long and short per symbol at once</option>
          </select>
        </Field>

        <Field label="Fill model" hint={TIER_BLURBS[tier]}>
          <select
            className="pl-input"
            value={tier}
            onChange={(e) => setTier(e.target.value as FillTier)}
          >
            {FILL_TIERS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </Field>
        <TierPreviewNote preview={preview.data} loading={preview.isLoading} error={preview.error} />

        <div className="flex gap-3">
          {tier === 'BAR_CLOSE' ? (
            <Field label="Slippage (bps)" hint="Adverse offset on every market fill. Prices the timing uncertainty this tier cannot resolve, not the spread.">
              <input className="pl-input mono" value={slippage} onChange={(e) => setSlippage(e.target.value)} />
            </Field>
          ) : null}
          {tier === 'TRADE_ONLY' ? (
            <Field label="Spread (bps)" hint="A print says a trade happened, not which side of the book it was on. This is the charge for that uncertainty.">
              <input className="pl-input mono" value={tradeSpread} onChange={(e) => setTradeSpread(e.target.value)} />
            </Field>
          ) : null}
          {tier === 'BOOK_TICKER' ? (
            <Field label="Impact k (bps)" hint="Spec 6.4: impact_bps = k × √(order notional ÷ last minute's notional). Calibrate from real fills; 10 is the pessimistic default.">
              <input className="pl-input mono" value={impactK} onChange={(e) => setImpactK(e.target.value)} />
            </Field>
          ) : null}
          {tier === 'BOOK_WALK' ? (
            <Field label="Depth penalty" hint="Fraction added beyond the deepest published level. 0.001 is 0.10%, deliberately pessimistic.">
              <input className="pl-input mono" value={depthPenalty} onChange={(e) => setDepthPenalty(e.target.value)} />
            </Field>
          ) : null}
          <Field
            label="Latency"
            hint="Spec 6.3: lognormal is the realistic default; fixed is deterministic and is what golden tests use. Zero lets orders fill at the print that triggered them, and flags the run."
          >
            <select
              className="pl-input"
              value={latencyModel}
              onChange={(e) => setLatencyModel(e.target.value as 'fixed' | 'lognormal')}
            >
              <option value="lognormal">lognormal</option>
              <option value="fixed">fixed</option>
            </select>
          </Field>
          <Field label={latencyModel === 'lognormal' ? 'Median (ms)' : 'Latency (ms)'}>
            <input className="pl-input mono" value={latency} onChange={(e) => setLatency(e.target.value)} />
          </Field>
          {latencyModel === 'lognormal' ? (
            <Field label="p99 (ms)" hint="Must exceed the median — a distribution whose tail is below its centre is not one.">
              <input className="pl-input mono" value={latencyP99} onChange={(e) => setLatencyP99(e.target.value)} />
            </Field>
          ) : null}
          <Field label="Label">
            <input className="pl-input" value={label} onChange={(e) => setLabel(e.target.value)} placeholder="optional" />
          </Field>
        </div>

        <details className="pl-panel" style={{ padding: '8px 10px' }}>
          <summary style={{ cursor: 'pointer', fontSize: 12 }}>
            Risk limits{' '}
            <span style={{ color: riskOn ? 'var(--text-mute)' : 'var(--warn)' }}>
              {riskOn ? 'spec 7 defaults' : 'OFF - this run has no ceiling on position size'}
            </span>
          </summary>
          <div style={{ marginTop: 8 }} className="flex flex-col gap-3">
            <label className="flex items-center gap-2" style={{ fontSize: 12 }}>
              <input type="checkbox" checked={riskOn} onChange={(e) => setRiskOn(e.target.checked)} />
              Enforce risk limits
            </label>
            {riskOn ? (
              <>
                <div className="flex flex-wrap gap-3">
                  <Field
                    label="Max leverage"
                    hint="Projected gross notional across all symbols, over equity. Blank means no ceiling."
                  >
                    <input
                      className="pl-input mono"
                      value={maxLeverage}
                      onChange={(e) => setMaxLeverage(e.target.value)}
                      placeholder="none"
                    />
                  </Field>
                  <Field
                    label="Max position (USDT)"
                    hint="Ceiling on projected exposure times the mark, counting every order still in flight - not just the position as it stands."
                  >
                    <input
                      className="pl-input mono"
                      value={maxNotional}
                      onChange={(e) => setMaxNotional(e.target.value)}
                      placeholder="none"
                    />
                  </Field>
                  <Field label="Max open orders">
                    <input
                      className="pl-input mono"
                      value={maxOpenOrders}
                      onChange={(e) => setMaxOpenOrders(e.target.value)}
                      placeholder="none"
                    />
                  </Field>
                  <Field label="Max orders / min" hint="The runaway-loop guard, not a trading limit.">
                    <input
                      className="pl-input mono"
                      value={maxPerMinute}
                      onChange={(e) => setMaxPerMinute(e.target.value)}
                      placeholder="none"
                    />
                  </Field>
                </div>
                <div className="flex flex-wrap gap-3">
                  <Field
                    label="Daily loss"
                    hint="A fraction - 0.02 is 2% - of the run's starting equity, measured from each UTC day's open. Halts the run."
                  >
                    <input
                      className="pl-input mono"
                      value={maxDailyLoss}
                      onChange={(e) => setMaxDailyLoss(e.target.value)}
                      placeholder="none"
                    />
                  </Field>
                  <Field
                    label="Max drawdown"
                    hint="A fraction below peak mark-to-market equity, not closed-trade PnL. Halts the run."
                  >
                    <input
                      className="pl-input mono"
                      value={maxDrawdown}
                      onChange={(e) => setMaxDrawdown(e.target.value)}
                      placeholder="none"
                    />
                  </Field>
                  <Field label="Min equity" hint="A fraction of the starting balance. Halts the run.">
                    <input
                      className="pl-input mono"
                      value={minEquity}
                      onChange={(e) => setMinEquity(e.target.value)}
                      placeholder="none"
                    />
                  </Field>
                </div>
                <label className="flex items-center gap-2" style={{ fontSize: 12 }}>
                  <input
                    type="checkbox"
                    checked={haltOnLiquidation}
                    onChange={(e) => setHaltOnLiquidation(e.target.checked)}
                  />
                  Halt on liquidation
                </label>
                <label className="flex items-start gap-2" style={{ fontSize: 12 }}>
                  <input
                    type="checkbox"
                    checked={flattenOnHalt}
                    onChange={(e) => setFlattenOnHalt(e.target.checked)}
                    style={{ marginTop: 3 }}
                  />
                  <span>
                    Close positions on halt
                    <span style={{ color: 'var(--text-mute)' }}>
                      {' '}
                      - off means cancel-only, which is spec 7.3's default: force-closing
                      everything at market during a crash can be worse than the exposure.
                    </span>
                  </span>
                </label>
              </>
            ) : null}
            <div className="flex flex-wrap gap-3">
              <Field
                label="Flatten after (min)"
                hint="A platform-enforced exit, measured from when the position opened rather than from the last increment. Blank means never."
              >
                <input
                  className="pl-input mono"
                  value={maxHoldMinutes}
                  onChange={(e) => setMaxHoldMinutes(e.target.value)}
                  placeholder="never"
                />
              </Field>
              <Field
                label="Flatten before funding (min)"
                hint="Close this long before a settlement, read from the funding series the run loads rather than a nominal 8-hour grid. Blank means never."
              >
                <input
                  className="pl-input mono"
                  value={beforeFundingMinutes}
                  onChange={(e) => setBeforeFundingMinutes(e.target.value)}
                  placeholder="never"
                />
              </Field>
            </div>
          </div>
        </details>

        {chosen?.head?.params?.length ? (
          <div>
            <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '0 0 4px' }}>Parameters</p>
            <div className="flex flex-wrap gap-3">
              {chosen.head.params.map((spec) => (
                <Field key={spec.name} label={spec.label ?? spec.name} hint={spec.help}>
                  <input
                    className="pl-input mono"
                    value={params[spec.name] ?? String(spec.default)}
                    onChange={(e) => setParams((p) => ({ ...p, [spec.name]: e.target.value }))}
                  />
                </Field>
              ))}
            </div>
          </div>
        ) : null}

        {invalidVersion ? (
          <p style={{ fontSize: 12, color: 'var(--down)', margin: 0 }}>
            The head version of this strategy did not pass validation, so it cannot be backtested. Fix
            the diagnostics in the editor and save again.
          </p>
        ) : null}

        <div className="flex items-center gap-2">
          <button className="pl-btn" onClick={() => openRunDraft(null)}>
            Cancel
          </button>
          <span className="flex-1" />
          <button
            className="pl-btn pl-btn-primary"
            disabled={strategyId == null || invalidRange || invalidVersion || mutation.isPending}
            onClick={() => mutation.mutate()}
          >
            {mutation.isPending ? 'Starting…' : 'Run backtest'}
          </button>
        </div>
      </div>
    </div>
  )
}

export function Field({
  label,
  hint,
  children,
}: {
  label: string
  hint?: string
  children: React.ReactNode
}) {
  return (
    <label className="flex flex-col gap-1" style={{ flex: 1, minWidth: 120 }}>
      <span style={{ fontSize: 11, color: 'var(--text-dim)' }} title={hint}>
        {label}
        {hint ? <span style={{ color: 'var(--text-mute)' }}> ⓘ</span> : null}
      </span>
      {children}
    </label>
  )
}

/** A blank field means *no limit*, not zero.
 *
 *  Zero is a ceiling nothing can satisfy, and the server refuses it. Mapping blank to zero
 *  would turn "I did not set this" into "reject everything", which the user would meet as a
 *  run whose every order was refused for a limit they never typed. */
export function blankIsNone(value: string): string | null {
  const trimmed = value.trim()
  return trimmed === '' ? null : trimmed
}

export function blankIsNoneInt(value: string): number | null {
  const trimmed = value.trim()
  if (trimmed === '') return null
  const parsed = Number(trimmed)
  return Number.isFinite(parsed) ? Math.trunc(parsed) : null
}

/** Minutes in the form, milliseconds on the wire. The form asks for minutes because a
 *  hold limit is a human decision in human units; the engine works in the millisecond grid
 *  every timestamp in this platform uses. */
export function minutesToMs(value: string): number | null {
  const minutes = blankIsNoneInt(value)
  return minutes == null || minutes <= 0 ? null : minutes * 60_000
}
