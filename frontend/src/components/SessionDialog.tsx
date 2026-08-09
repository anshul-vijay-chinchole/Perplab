/**
 * Start a paper/live session from the app (spec 10.2, 10.3).
 *
 * Until this existed the only way to start a session was `curl` against `POST /api/sessions`,
 * and the margin-mode selector lived in the New Backtest dialog alone — so the one mode that
 * touches a real exchange was the one with no UI. That is the gap this closes.
 *
 * **It shows who already owns a symbol before you submit.** A symbol on an account belongs to
 * one running session: Binance holds one position per symbol and side for the whole account,
 * with no per-strategy scope, so two sessions on one symbol would have their fills merged into
 * a single position while each ledger reported only its own half. `GET /api/symbol-claims`
 * reports what every running session already holds, and this form renders it inline rather
 * than letting an operator fill in the whole thing and then be refused.
 *
 * The refusal in `POST /api/sessions` is still the authority. A claim taken between this
 * read and that submit is a race a form cannot close, and a check that lived only here would
 * be a check `curl` skips.
 *
 * **The risk fields are the same declaration the backtest dialog sends.** They are shared
 * rather than re-typed because a session's limits differing from a backtest's by a typo is
 * exactly what spec 7's "a limit that stops a backtest stops live trading identically" rules
 * out — and the mode that watches a live market is the wrong one of the two to get wrong.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useMemo, useState } from 'react'
import {
  api,
  ApiError,
  FILL_TIERS,
  MARGIN_MODE_BLURBS,
  POSITION_MODE_BLURBS,
  RISK_DEFAULTS,
  TIER_BLURBS,
  type FillTier,
  type MarginMode,
  type Strategy,
  type SymbolClaim,
} from '../api'
import { exchangeStatusQuery } from '../lib/queries'
import { useUi } from '../store'
import { Field, blankIsNone, blankIsNoneInt, minutesToMs } from './RunList'

type Endpoint = 'testnet' | 'production'
type SessionMode = 'paper' | 'live'

export function StartSessionDialog() {
  const open = useUi((s) => s.sessionDraftOpen)
  const setOpen = useUi((s) => s.openSessionDraft)
  const setTab = useUi((s) => s.setTab)
  const selectRun = useUi((s) => s.selectRun)
  const setFeedRunId = useUi((s) => s.setFeedRunId)
  const notify = useUi((s) => s.notify)
  const client = useQueryClient()

  const strategies = useQuery({
    queryKey: ['strategies-for-session'],
    queryFn: () => api.list(),
    enabled: open,
  })
  const settings = useQuery({
    queryKey: ['settings'],
    queryFn: api.settings,
    enabled: open,
  })

  const [strategyId, setStrategyId] = useState<number | null>(null)
  const [label, setLabel] = useState('')
  const [symbolText, setSymbolText] = useState('BTCUSDT')
  const [timeframe, setTimeframe] = useState('1m')
  const [endpoint, setEndpoint] = useState<Endpoint>('testnet')
  const [mode, setMode] = useState<SessionMode>('paper')
  const [balance, setBalance] = useState('10000')
  const [leverage, setLeverage] = useState('5')
  const [marginMode, setMarginMode] = useState<MarginMode>('ISOLATED')
  const [hedgeMode, setHedgeMode] = useState(false)
  const [tier, setTier] = useState<FillTier>('BOOK_WALK')
  const [maxRuntimeHours, setMaxRuntimeHours] = useState('')
  const [seed, setSeed] = useState('0')

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

  // Seeded once per opening, latched so a background refetch cannot overwrite an edit in
  // progress — the same rule the backtest dialog uses and for the same reason.
  const [seeded, setSeeded] = useState(false)
  useEffect(() => {
    if (!open) {
      setSeeded(false)
      return
    }
    const stored = settings.data?.settings
    if (stored == null || seeded) return
    setSeeded(true)
    setLeverage(String(stored.default_leverage))
    setRiskOn(stored.risk_enabled)
    setMaxLeverage(stored.max_leverage ?? '')
    setMaxDailyLoss(stored.max_daily_loss_pct ?? '')
    setMaxDrawdown(stored.max_drawdown_pct ?? '')
    if (stored.max_open_orders != null) setMaxOpenOrders(String(stored.max_open_orders))
    if (stored.max_orders_per_minute != null)
      setMaxPerMinute(String(stored.max_orders_per_minute))
    setMinEquity(stored.min_equity_pct ?? '')
    setFlattenOnHalt(stored.kill_switch_flatten)
  }, [open, settings.data, seeded])

  useEffect(() => {
    if (!open) return
    const handler = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false)
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [open, setOpen])

  const symbols = useMemo(
    () =>
      symbolText
        .split(/[,\s]+/)
        .map((s) => s.trim().toUpperCase())
        .filter(Boolean),
    [symbolText],
  )
  // Deduplicated before it is sent. A repeated symbol produced two identical monitor rows,
  // and a session configured for "BTCUSDT, btcusdt" is asking for one symbol twice rather
  // than for two.
  const uniqueSymbols = useMemo(() => Array.from(new Set(symbols)), [symbols])

  // Polled while the dialog is open: another session can start between opening the form and
  // submitting it, and the whole value of this panel is being current.
  const claims = useQuery({
    queryKey: ['symbol-claims', endpoint],
    queryFn: () => api.symbolClaims(endpoint),
    enabled: open,
    refetchInterval: open ? 5000 : false,
  })

  // Polled only while LIVE is selected: the key can expire or be disconnected between
  // opening this form and submitting it, and the gate below is only worth having if it is
  // current. The server's refusal is still the authority — this is the readable version.
  // Options come from the shared declaration (`lib/queries.ts`); only the enable gate is
  // this dialog's own. A disabled query does not poll, so the shared interval is inert
  // while the dialog is closed or the mode is paper.
  const exchange = useQuery({
    ...exchangeStatusQuery,
    enabled: open && mode === 'live',
  })
  const exchangeConnected = exchange.data?.connected === true
  const exchangeEndpoint = exchange.data?.endpoint ?? null
  const liveBlocked =
    mode === 'live' &&
    (endpoint === 'production' ||
      exchange.isLoading ||
      !exchangeConnected ||
      exchangeEndpoint !== endpoint)

  const relevant: SymbolClaim[] = useMemo(
    () => (claims.data?.claims ?? []).filter((c) => uniqueSymbols.includes(c.symbol)),
    [claims.data, uniqueSymbols],
  )
  // Any overlap blocks, whatever the configuration. This used to filter `relevant` down to
  // the claims that *disagreed* about leverage, margin mode or position mode, on the reading
  // that matching settings made a symbol shareable. They do not: the exchange merges the two
  // sessions' fills into one position regardless, and the settings were never the thing that
  // collided. See `store/claims.py`.
  const conflicts = relevant

  const chosen: Strategy | undefined = useMemo(
    () => strategies.data?.strategies.find((s) => s.id === strategyId),
    [strategies.data, strategyId],
  )
  const invalidVersion = chosen != null && chosen.head != null && !chosen.head.valid
  const versionId = chosen?.head?.id ?? null

  const mutation = useMutation({
    mutationFn: () =>
      api.startSession({
        strategy_id: strategyId!,
        version_id: versionId!,
        symbols: uniqueSymbols,
        timeframe,
        label,
        endpoint,
        mode,
        opening_balance: balance,
        leverage: Number(leverage) || 1,
        margin_mode: marginMode,
        hedge_mode: hedgeMode,
        fill_tier: tier,
        seed: Number(seed) || 0,
        // Hours in the form, seconds on the wire. Blank means "until I stop it", which the
        // server reads as 0 rather than as a deadline of zero seconds.
        max_runtime_s: hoursToSeconds(maxRuntimeHours),
        risk_enabled: riskOn,
        max_leverage: blankIsNone(maxLeverage),
        max_position_notional: blankIsNone(maxNotional),
        max_daily_loss_pct: blankIsNone(maxDailyLoss),
        max_drawdown_pct: blankIsNone(maxDrawdown),
        max_open_orders: blankIsNoneInt(maxOpenOrders),
        max_orders_per_minute: blankIsNoneInt(maxPerMinute),
        min_equity_pct: blankIsNone(minEquity),
        halt_on_liquidation: haltOnLiquidation,
        kill_switch_flatten: flattenOnHalt,
        auto_flatten: {
          max_hold_ms: minutesToMs(maxHoldMinutes),
          before_funding_ms: minutesToMs(beforeFundingMinutes),
        },
      }),
    onSuccess: (data) => {
      client.invalidateQueries({ queryKey: ['runs'] })
      client.invalidateQueries({ queryKey: ['symbol-claims'] })
      setOpen(false)
      setTab('Runs')
      selectRun(data.run.id)
      setFeedRunId(data.run.id)
      notify(
        mode === 'live'
          ? `LIVE session #${data.run.id} started on ${endpoint} — real orders`
          : `Session #${data.run.id} started on ${endpoint}`,
      )
    },
    onError: (error) =>
      notify(error instanceof ApiError ? error.message : String(error), 'error'),
  })

  if (!open) return null

  return (
    <div
      className="pl-scrim"
      style={{ paddingTop: 48 }}
      onClick={() => setOpen(false)}
    >
      <div
        className="pl-modal pl-scroll p-4 flex flex-col gap-3"
        style={{ width: 'min(620px, calc(100vw - 32px))', maxHeight: '86vh', overflow: 'auto' }}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 style={{ margin: 0, fontSize: 14 }}>Start session</h2>

        <Field label="Strategy">
          <select
            className="pl-input"
            value={strategyId ?? ''}
            onChange={(e) => setStrategyId(Number(e.target.value))}
          >
            <option value="">Choose a strategy…</option>
            {(strategies.data?.strategies ?? []).map((s) => (
              <option key={s.id} value={s.id}>
                {s.name}{' '}
                {s.head ? `(v${s.head.version_no}${s.head.valid ? '' : ' — invalid'})` : ''}
              </option>
            ))}
          </select>
        </Field>

        <div className="flex gap-3">
          <Field
            label="Symbols"
            hint="Comma or space separated. Each one is claimed at the exchange for the life of the session."
          >
            <input
              className="pl-input mono"
              value={symbolText}
              onChange={(e) => setSymbolText(e.target.value)}
            />
          </Field>
          <Field label="Timeframe">
            <input
              className="pl-input mono"
              value={timeframe}
              onChange={(e) => setTimeframe(e.target.value)}
            />
          </Field>
          <Field label="Label">
            <input className="pl-input" value={label} onChange={(e) => setLabel(e.target.value)} />
          </Field>
        </div>

        <div className="flex gap-3">
          <Field
            label="Mode"
            hint={
              mode === 'live'
                ? 'Real signed orders go to the venue. Fills come back from the exchange, and the account is reconciled against it every 60s.'
                : 'Orders are priced by the local fill simulator against live market data. Nothing reaches the exchange.'
            }
          >
            <select
              className="pl-input"
              value={mode}
              onChange={(e) => setMode(e.target.value as SessionMode)}
              style={mode === 'live' ? { borderColor: 'var(--down)', color: 'var(--down)' } : undefined}
            >
              <option value="paper">PAPER — simulated fills</option>
              <option value="live">LIVE — real orders</option>
            </select>
          </Field>
          <Field
            label="Venue"
            hint="testnet and production are separate accounts. Their symbol claims do not collide, and neither do their balances."
          >
            <select
              className="pl-input"
              value={endpoint}
              onChange={(e) => setEndpoint(e.target.value as Endpoint)}
            >
              <option value="testnet">testnet</option>
              <option value="production">production — real money</option>
            </select>
          </Field>
          <Field
            label="Balance"
            hint={
              mode === 'live'
                ? "Ignored for a LIVE session: the ledger opens at the venue's real wallet balance, or the 60s reconciliation would halt on a difference that means nothing."
                : undefined
            }
          >
            <input
              className="pl-input mono"
              value={balance}
              onChange={(e) => setBalance(e.target.value)}
              disabled={mode === 'live'}
              style={mode === 'live' ? { opacity: 0.5 } : undefined}
            />
          </Field>
          <Field label="Leverage">
            <input
              className="pl-input mono"
              value={leverage}
              onChange={(e) => setLeverage(e.target.value)}
            />
          </Field>
          <Field label="Seed">
            <input className="pl-input mono" value={seed} onChange={(e) => setSeed(e.target.value)} />
          </Field>
        </div>

        <div className="flex gap-3">
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
          <Field
            label="Position mode"
            hint={POSITION_MODE_BLURBS[hedgeMode ? 'hedge' : 'one-way']}
          >
            <select
              className="pl-input"
              value={hedgeMode ? 'hedge' : 'one-way'}
              onChange={(e) => setHedgeMode(e.target.value === 'hedge')}
            >
              <option value="one-way">One-way — one position per symbol</option>
              <option value="hedge">Hedge — long and short at once</option>
            </select>
          </Field>
          <Field label="Fill tier" hint={TIER_BLURBS[tier]}>
            <select
              className="pl-input"
              value={tier}
              onChange={(e) => setTier(e.target.value as FillTier)}
            >
              {FILL_TIERS.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </Field>
          <Field
            label="Stop after (h)"
            hint="Blank runs until you stop it. The session keeps its position either way — stopping is not flattening unless you ask for it."
          >
            <input
              className="pl-input mono"
              value={maxRuntimeHours}
              onChange={(e) => setMaxRuntimeHours(e.target.value)}
              placeholder="no limit"
            />
          </Field>
        </div>

        {hedgeMode ? (
          <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>
            In hedge mode every order must name a side — <code>ctx.buy(position_side="LONG")</code>{' '}
            opens the long, <code>ctx.sell(position_side="LONG")</code> reduces it, and neither
            can flip it. A strategy that calls <code>ctx.buy()</code> with no side is refused
            rather than routed by guesswork. The Binance account must be in hedge mode too; the
            session preflight refuses a mismatch in either direction, before the first order.
          </p>
        ) : null}

        {mode === 'live' ? (
          <div
            className="pl-panel"
            style={{ padding: '8px 10px', borderColor: 'var(--down)' }}
          >
            <p style={{ fontSize: 12, margin: 0, color: 'var(--down)' }}>
              LIVE mode sends real signed orders to {endpoint}.
            </p>
            <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '6px 0 0' }}>
              Before the first order the worker verifies the account is flat on{' '}
              {uniqueSymbols.join(', ') || 'the chosen symbols'} with no working orders, sets
              leverage and margin mode at the exchange and verifies the echo, and adopts the
              venue's wallet as the ledger's opening balance. Fills come back on the
              user-data stream and the account is reconciled against the exchange every 60
              seconds — any mismatch beyond tick/step tolerance trips the kill switch.
            </p>
            {endpoint === 'production' ? (
              <p style={{ fontSize: 11, color: 'var(--down)', margin: '6px 0 0' }}>
                Production live trading is disabled until the Phase 8 exit criterion has
                been met on testnet: one real order placed, filled and reconciled to the
                cent. Switch the venue to testnet.
              </p>
            ) : exchange.isLoading ? (
              <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '6px 0 0' }}>
                Checking the exchange connection…
              </p>
            ) : !exchangeConnected ? (
              <p style={{ fontSize: 11, color: 'var(--down)', margin: '6px 0 0' }}>
                No exchange key is connected. Enter your API key in the Data &amp; Feed tab
                first — a live session cannot sign without one.
              </p>
            ) : exchangeEndpoint !== endpoint ? (
              <p style={{ fontSize: 11, color: 'var(--down)', margin: '6px 0 0' }}>
                The connected key was validated against {exchangeEndpoint ?? 'nothing'} and
                this session asks for {endpoint}. Reconnect against {endpoint} in the Data
                &amp; Feed tab.
              </p>
            ) : (
              <p style={{ fontSize: 11, color: 'var(--up)', margin: '6px 0 0' }}>
                Exchange connected on {endpoint}
                {exchange.data?.balance != null
                  ? ` — wallet ${exchange.data.balance} USDT`
                  : ''}
                . The ledger will open at the venue's wallet balance.
              </p>
            )}
          </div>
        ) : null}

        <SymbolClaimNotice
          symbols={uniqueSymbols}
          claims={relevant}
          loading={claims.isLoading}
        />

        <details className="pl-panel" style={{ padding: '8px 10px' }}>
          <summary style={{ cursor: 'pointer', fontSize: 12 }}>
            Risk limits{' '}
            <span style={{ color: riskOn ? 'var(--text-mute)' : 'var(--warn)' }}>
              {riskOn
                ? 'spec 7 defaults, applied to this session alone'
                : 'OFF - this session has no ceiling on position size'}
            </span>
          </summary>
          <div style={{ marginTop: 8 }} className="flex flex-col gap-3">
            <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>
              These apply <strong>per session</strong>, not pooled across sessions. Two
              strategies each running under a 5x cap are two accounts-worth of exposure on one
              real account — nothing here sums them, because nothing can size a limit you did
              not ask for.
            </p>
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
                    hint="Ceiling on projected exposure times the mark, counting every order still in flight. In hedge mode both sides are summed, not netted."
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
                    hint="A fraction - 0.02 is 2% - of starting equity, measured from each UTC day's open. Halts the session."
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
                    hint="A fraction below peak mark-to-market equity, not closed-trade PnL. Halts the session."
                  >
                    <input
                      className="pl-input mono"
                      value={maxDrawdown}
                      onChange={(e) => setMaxDrawdown(e.target.value)}
                      placeholder="none"
                    />
                  </Field>
                  <Field label="Min equity" hint="A fraction of the starting balance. Halts the session.">
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
                hint="Measured from when the position opened rather than from the last increment. In hedge mode each side has its own clock. Blank means never."
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
                hint="Read from the funding series the session loads rather than a nominal 8-hour grid. Blank means never."
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

        {invalidVersion ? (
          <p style={{ fontSize: 12, color: 'var(--down)', margin: 0 }}>
            The head version of this strategy did not pass validation, so it cannot trade. Fix
            the diagnostics in the editor and save again.
          </p>
        ) : null}

        <div className="flex items-center gap-2">
          <button className="pl-btn" onClick={() => setOpen(false)}>
            Cancel
          </button>
          <span className="flex-1" />
          <button
            className="pl-btn pl-btn-primary"
            disabled={
              strategyId == null ||
              versionId == null ||
              uniqueSymbols.length === 0 ||
              invalidVersion ||
              conflicts.length > 0 ||
              liveBlocked ||
              mutation.isPending
            }
            style={
              mode === 'live'
                ? { background: 'var(--down)', borderColor: 'var(--down)' }
                : undefined
            }
            onClick={() => mutation.mutate()}
          >
            {mutation.isPending
              ? 'Starting…'
              : mode === 'live'
                ? `Start LIVE on ${endpoint} — real orders`
                : `Start paper on ${endpoint}`}
          </button>
        </div>
      </div>
    </div>
  )
}

/** Who already owns these symbols on this account.
 *
 *  Rendered whether or not there is a conflict. "Nothing else is running on BTCUSDT" is a
 *  fact worth stating before starting a live session, and a panel that appears only on
 *  failure teaches the reader that its absence means nothing was checked.
 *
 *  Any claim here blocks the submit. There is deliberately no "same configuration, so this
 *  session can join" state — that reading was the defect: what merges at the exchange is the
 *  position, not the settings around it. */
function SymbolClaimNotice({
  symbols,
  claims,
  loading,
}: {
  symbols: string[]
  claims: SymbolClaim[]
  loading: boolean
}) {
  if (!symbols.length) return null
  if (loading) {
    return (
      <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>
        Checking what is already running on {symbols.join(', ')}…
      </p>
    )
  }
  if (!claims.length) {
    return (
      <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>
        Nothing else is running on {symbols.join(', ')}. This session will set their leverage
        and margin mode at the exchange before its first order.
      </p>
    )
  }
  return (
    <div className="pl-panel" style={{ padding: '8px 10px', borderColor: 'var(--down)' }}>
      <p style={{ fontSize: 12, margin: '0 0 6px', color: 'var(--down)' }}>
        These symbols are already being traded by a running session
      </p>
      <table className="pl-table mono" style={{ fontSize: 11 }}>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Run</th>
            <th style={{ textAlign: 'right' }}>Leverage</th>
            <th>Margin</th>
            <th>Position mode</th>
          </tr>
        </thead>
        <tbody>
          {claims.map((claim) => (
            <tr key={`${claim.symbol}:${claim.run_id}`}>
              <td>{claim.symbol}</td>
              <td>#{claim.run_id}</td>
              <td style={{ textAlign: 'right' }}>{claim.leverage}x</td>
              <td>{claim.margin_mode}</td>
              <td>{claim.hedge_mode ? 'hedge' : 'one-way'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '6px 0 0' }}>
        Binance holds <strong>one position per symbol and side for the whole account</strong>,
        with no per-strategy scope. A second session on the same symbol would have its fills
        merged into the running session's position — one entry price, one margin allocation,
        one liquidation price — while both ledgers went on reporting only their own half.
        Matching the configuration above does not help, because it is the position that merges
        and not the settings. Pick a different symbol, stop the running session, or use a
        separate Binance account.
      </p>
    </div>
  )
}

/** Hours in the form, seconds on the wire. Blank means no deadline, which the server reads
 *  as `0` — a session runs until it is stopped unless somebody says otherwise. */
function hoursToSeconds(value: string): number {
  const trimmed = value.trim()
  if (trimmed === '') return 0
  const hours = Number(trimmed)
  return Number.isFinite(hours) && hours > 0 ? hours * 3600 : 0
}
