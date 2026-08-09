/**
 * Typed client for the PerpLab API.
 *
 * Every response shape here mirrors a Python `to_json()` on the server. They are hand-kept
 * in sync rather than generated: the surface is small, and a generator would add a build
 * step to a project whose whole point is that it runs on one laptop with no ceremony.
 * `tests/unit/test_api.py` asserts the field names, so a rename on the server breaks a
 * Python test rather than a React runtime.
 */

export type Severity = 'error' | 'warning' | 'info'

export interface Diagnostic {
  severity: Severity
  stage: string
  code: string
  message: string
  line: number
  column: number
  end_line: number
  end_column: number
}

export interface ParamSpec {
  name: string
  type: 'int' | 'float' | 'decimal' | 'bool' | 'str' | 'choice'
  default: string | number | boolean
  min?: string | number
  max?: string | number
  choices?: string[]
  label?: string
  help?: string
}

export interface Requires {
  symbols: string[]
  timeframe: string
  history: number
  datasets: string[]
}

export interface ValidationResult {
  ok: boolean
  diagnostics: Diagnostic[]
  class_name: string | null
  params: ParamSpec[]
  requires: Requires | null
  hooks: string[]
  warmup: number | null
  indicator_warmup: number | null
  bars: number | null
  orders: number | null
  event_hash: string | null
  stdout: string
}

export interface StrategyVersion {
  id: number
  strategy_id: number
  version_no: number
  code?: string
  code_sha256: string
  created_ms: number
  message: string
  class_name: string | null
  params: ParamSpec[]
  requires: Requires | null
  valid: boolean
  diagnostics: Diagnostic[]
}

export interface Strategy {
  id: number
  name: string
  notes: string
  created_ms: number
  updated_ms: number
  archived_ms: number | null
  archived: boolean
  tags: string[]
  head: StrategyVersion | null
  version_count: number
  run_count: number
}

export interface SaveResponse {
  strategy: Strategy
  version: StrategyVersion
  created: boolean
  validation: ValidationResult
  warnings?: string[]
}

/** `lost` is not a failure the run reported -- it is the absence of any report at all.
 *
 *  A run marked `running` whose worker is no longer alive can never reach a terminal status
 *  by itself, because the process that would have written one is gone. It is terminal here
 *  so the UI stops waiting for a message nobody will send, and it is its own value rather
 *  than `failed` because the two call for different work: `failed` carries a traceback from
 *  the strategy, `lost` means the artefacts on disk stop mid-run and the reason is outside
 *  them. */
export type RunStatus = 'queued' | 'running' | 'done' | 'failed' | 'cancelled' | 'lost'

export interface Run {
  id: number
  strategy_id: number
  strategy_name: string
  version_id: number
  version_no: number
  mode: string
  status: RunStatus
  label: string
  symbols: string[]
  timeframe: string
  start_ms: number | null
  end_ms: number | null
  seed: number | null
  engine_version: number | null
  fill_tier: string | null
  requested_fill_tier: string | null
  tier_reason: string | null
  tier_degraded: boolean
  created_ms: number
  started_ms: number | null
  finished_ms: number | null
  archived_ms: number | null
  archived: boolean
  duration_ms: number | null
  event_hash: string | null
  flags: string[]
  warnings: string[]
  error: string | null
  progress_bars: number
  progress_total: number
  net_pnl: string | null
  sharpe: number | null
  max_drawdown: number | null
  round_trips: number | null
  fills: number | null
}

export interface TradeStats {
  round_trips: number
  legs: number
  wins: number
  losses: number
  scratches: number
  win_rate: number | null
  profit_factor: number | null
  expectancy: number | null
  payoff_ratio: number | null
  avg_win: number | null
  avg_loss: number | null
  largest_win: number | null
  largest_loss: number | null
  avg_duration_ms: number | null
  per_trade_sharpe: number | null
  open_trades: number
  liquidated: number
}

export interface Metrics {
  grid: string
  periods: number
  periods_per_year: number
  sharpe: number | null
  sortino: number | null
  cagr: number | null
  calmar: number | null
  max_drawdown: number | null
  max_drawdown_grid: number | null
  max_drawdown_ms: number | null
  ulcer_index: number | null
  exposure: number | null
  turnover: number | null
  volatility: number | null
  total_return: number | null
  days: number
  truncated_at_ms: number | null
  trades: TradeStats
}

/** Every field is an exact decimal string — see the note in `perplab/api/routers/runs.py`. */
export interface Attribution {
  price_pnl: string
  funding_pnl: string
  fees: string
  slippage_cost: string
  slippage_abs: string
  liquidation_cost: string
  net_pnl: string
  realized_pnl: string
  unrealized_pnl: string
}

export interface Trade {
  index: number
  symbol: string
  side: 'LONG' | 'SHORT'
  /** Which position slot the round-trip lived in: `BOTH` one-way, `LONG`/`SHORT` hedged.
   *  Distinct from `side` — in hedge mode a symbol shows two rows and this is the only
   *  field that tells the legs apart. Optional: `trades.json` written before hedge mode
   *  has no such key, and absent means the one-way trade it was. */
  position_side?: PositionSide
  entry_ms: number
  exit_ms: number | null
  entry_price: string
  exit_price: string | null
  max_qty: string
  realized_pnl: string
  fees: string
  funding: string
  net_pnl: string
  mae: string
  mfe: string
  mae_price: string
  mfe_price: string
  legs: number
  duration_ms: number | null
  close_reason: 'signal' | 'liquidation' | 'open'
}

export interface RiskBreach {
  limit: string
  action: 'REJECT' | 'HALT'
  ts_ms: number
  observed: string
  allowed: string
  detail: string
  symbol: string | null
}

export interface RunDetail {
  run: Run
  spec: Record<string, unknown>
  metrics?: Metrics
  attribution?: Attribution
  summary?: Record<string, unknown>
  manifest?: Record<string, unknown>
  /** Empty for every run completed before Phase 6, which is not the same as "no breaches"
   *  — those runs had no risk layer at all. The summary's `risk.limits` block is what
   *  tells the two apart. */
  risk_breaches?: RiskBreach[]
}

export interface EquitySeries {
  ts: number[]
  equity: number[]
  /** `null` where the running peak was non-positive — a fall from a peak of nothing is
   *  undefined. The server sends `list[float | None]`; drawing a null as 0 would put a
   *  flat line under an account that was at or below zero. */
  drawdown: (number | null)[]
  samples: number
  returned: number
  /** True while the run is still going: this is the preview the worker republishes on its
   *  progress cadence, not the finished `equity.parquet`. Its drawdown is measured against
   *  the highest peak *so far* and can only deepen, so it is not yet the run's drawdown. */
  partial: boolean
}

export interface PriceSeries {
  symbol: string
  timeframe: string
  ts: number[]
  close: number[]
  bars: number
}

export interface RunEvent {
  seq: number
  ts_ms: number
  kind: string
  payload: Record<string, unknown>
}

export interface Trials {
  combinations: number
  evaluations: number
  best_sharpe: number | null
  selection_bias_sd: number | null
}

/** Spec 7's table on the wire, shared by a backtest request and a paper session request.
 *
 *  One declaration rather than two identical ones: spec 7's whole claim is that *"a limit
 *  that stops a backtest stops live trading identically"*, and two copies of the field list
 *  is how a limit ends up spelled one way in the run body and another in the session body --
 *  at which point the paper session silently runs without it. */
export interface RiskLimitFields {
  risk_enabled?: boolean
  max_position_notional?: string | null
  max_leverage?: string | null
  max_daily_loss_pct?: string | null
  max_drawdown_pct?: string | null
  max_open_orders?: number | null
  max_orders_per_minute?: number | null
  max_consecutive_losses?: number | null
  halt_on_liquidation?: boolean
  min_equity_pct?: string | null
  max_consecutive_rejections?: number | null
  kill_switch_flatten?: boolean
}

export interface StartRunBody extends RiskLimitFields {
  strategy_id: number
  version_no?: number
  label?: string
  params?: Record<string, string | number | boolean>
  symbols?: string[]
  timeframe?: string
  start_ms: number
  end_ms: number
  seed?: number
  opening_balance?: string
  leverage?: number
  margin_mode?: MarginMode
  /** Spec 3.3 extended — the backtest route accepts it (`runs.py StartRunRequest`), so a
   *  hedged strategy can be backtested as what it is rather than only sessioned. */
  hedge_mode?: boolean
  maker_rate?: string
  taker_rate?: string
  latency_model?: 'fixed' | 'lognormal'
  latency_submit_ms?: number
  latency_p99_ms?: number
  fill_tier?: FillTier
  slippage_bps?: string
  impact_k_bps?: string
  depth_exhaustion_pct?: string
  trade_spread_bps?: string
  max_hold_ms?: number | null
  before_funding_ms?: number | null
}

/** Spec 7's table, as the New Backtest dialog offers it.
 *
 *  These are the API's defaults restated for the form, not a second source of truth: the
 *  request omits any field the user did not touch, and the server applies the same values.
 *  They are listed here so the dialog can *show* what a run will be constrained by before
 *  it is queued — a limit nobody saw is a limit nobody chose. */
export const RISK_DEFAULTS = {
  max_leverage: '5',
  max_daily_loss_pct: '0.02',
  max_drawdown_pct: '0.15',
  max_open_orders: 10,
  max_orders_per_minute: 30,
  min_equity_pct: '0.50',
  max_consecutive_rejections: 5,
} as const

export type FillTier = 'BAR_CLOSE' | 'TRADE_ONLY' | 'BOOK_TICKER' | 'BOOK_WALK'

export interface TierCapabilities {
  market: boolean
  limit: boolean
  trigger: boolean
  book: boolean
}

export interface TierPreview {
  tier: FillTier
  requested: FillTier
  available: FillTier
  degraded: boolean
  reason: string | null
  capabilities: TierCapabilities
  preview: boolean
  all_capabilities: Record<FillTier, TierCapabilities>
}

/** Descending fidelity, matching spec 4.2's table. The order is what the tier selector
 *  renders and what decides which options sit above the available ceiling. */
export const FILL_TIERS: FillTier[] = ['BOOK_WALK', 'BOOK_TICKER', 'TRADE_ONLY', 'BAR_CLOSE']

export const TIER_BLURBS: Record<FillTier, string> = {
  BOOK_WALK:
    'Walks the 20-level depth ladder, level by level, and charges a stated penalty beyond it. Needs depth20 — which exists only from the day the collector started recording.',
  BOOK_TICKER:
    'Fills at the far touch plus k·√(size ÷ recent volume). Spec 4.2 calls this the default for most backtests.',
  TRADE_ONLY:
    'Fills at the next print after arrival, plus a stated spread. No book, so limit orders are refused rather than approximated.',
  BAR_CLOSE:
    'Fills at the most recent bar print, plus a fixed offset. Explicit opt-in only — fast, and flagged LOW_FIDELITY.',
}

/** Which balance pool backs a position (spec 3.7).
 *
 *  `CROSSED` is offered and refused rather than hidden. Hiding it would leave someone who
 *  wants cross margin with no answer at all; accepting it would be worse, because the
 *  ledger would price the position with the isolated closed form and report a liquidation
 *  price the exchange does not agree with. */
export type MarginMode = 'ISOLATED' | 'CROSSED'

/** Which of a symbol's positions something refers to — Binance's own vocabulary.
 *
 *  `BOTH` is a one-way account's single position; `LONG` and `SHORT` are the two a hedge
 *  account holds at once, each with its own entry price, margin and liquidation price. */
export type PositionSide = 'BOTH' | 'LONG' | 'SHORT'

export const POSITION_MODE_BLURBS: Record<'one-way' | 'hedge', string> = {
  'one-way':
    'One position per symbol. A fill that crosses zero flips it: selling more than you hold turns a long into a short.',
  hedge:
    'A long and a short position per symbol at once, tracked separately — separate entry prices, separate margin, separate liquidation prices, and either can be liquidated while the other survives. Every order must say which side it is for, and the Binance account has to be in hedge mode too (it refuses the switch while any position is open).',
}

export const MARGIN_MODE_BLURBS: Record<MarginMode, string> = {
  ISOLATED:
    'Each position is backed by its own margin allocation, and a liquidation destroys only that allocation. The only mode the ledger prices, and the one the exchange preflight sets per symbol before a session trades.',
  CROSSED:
    'Not implemented — selecting this refuses the run. Under cross margin a position’s liquidation price depends on the unrealised PnL of every other open position, so there is no closed form for it, and one bad position can liquidate every other one in the account.',
}

/* ------------------------------------------------------------------ phase 7: live wire */

/**
 * The key session (spec 11).
 *
 * `balance` is a string for the same reason every other monetary field here is: it is the
 * account the platform is about to trade, and a JSON number has already been through a
 * float by the time it arrives.
 *
 * `drift_ms` is this machine's clock against the exchange's. It is on the status object
 * rather than buried in diagnostics because a signed request is rejected outright once the
 * two disagree by more than the receive window, and "connected but every order is refused"
 * is otherwise a mystery with no visible cause.
 */
export interface ExchangeStatus {
  connected: boolean
  alias: string | null
  balance: string | null
  /** When the key session was validated (`KeySession.to_json`). Absent on the disconnected
   *  payload, which spells the fields out by hand and has no session to date. */
  connected_ms?: number
  /** Seconds until the in-memory key session expires, or `null` when nothing is connected. */
  expires_in_s: number | null
  endpoint: 'testnet' | 'production' | null
  drift_ms: number | null
}

export interface ConnectExchangeBody {
  api_key: string
  api_secret: string
  endpoint: 'testnet' | 'production'
}

export interface StartSessionBody extends RiskLimitFields {
  strategy_id: number
  /** The exact version, not the strategy's head. A session started against "whatever is
   *  current" would change strategy under itself the moment the editor is saved, and the
   *  parity report would then be comparing two different programs. */
  version_id: number
  symbols: string[]
  timeframe: string
  label?: string
  opening_balance?: string
  leverage?: number
  margin_mode?: MarginMode
  hedge_mode?: boolean
  /** Which venue. `testnet` is the server's default and the one to start on; `production`
   *  is a real account with real money, and the two are separate accounts whose symbol
   *  claims do not collide. */
  endpoint?: 'testnet' | 'production'
  /** `paper` (default) prices fills locally against the live feed; `live` sends real
   *  signed orders to the endpoint above (spec 13, Phase 8). A live start is refused
   *  unless the exchange is connected on the same endpoint, and production live is
   *  refused outright until the Phase 8 exit criterion is met on testnet. */
  mode?: 'paper' | 'live'
  seed?: number
  fill_tier?: FillTier
  maker_rate?: string
  taker_rate?: string
  reorder_buffer_ms?: number
  max_runtime_s?: number
  auto_flatten?: Record<string, number | null>
}

/** One running session's hold on one symbol's exchange configuration.
 *
 *  Leverage, margin mode and position mode are **account settings scoped to a symbol** at
 *  Binance — `POST /fapi/v1/leverage` takes a symbol and applies account-wide, with no
 *  per-strategy scope. Two strategies on one symbol therefore share one setting, and the
 *  Start Session form reads these so an operator sees what is already in force before
 *  submitting rather than being refused afterwards. */
export interface SymbolClaim {
  run_id: number
  symbol: string
  leverage: number
  margin_mode: MarginMode
  hedge_mode: boolean
  endpoint: string
  claimed_ms: number
  released_ms: number | null
}

/** Which way each half of the connection is, for the two dots in the monitor.
 *
 *  `user` is `'n/a'` rather than `'down'` for a paper session with no exchange attached:
 *  there is no user-data stream to be down, and rendering that as a failure would train the
 *  eye to ignore the one indicator that means an order update was missed. */
export interface MonitorConnection {
  market: 'up' | 'down'
  user: 'up' | 'down' | 'n/a'
  last_frame_ms: number
  pollers_down?: string[]
}

export interface MonitorAccount {
  equity: string
  wallet: string
  available: string
  used_margin: string
}

export interface MonitorPosition {
  symbol: string
  qty: string
  entry_price: string
  mark_price: string
  unrealized_pnl: string
  /** `null` when the position is flat, or when no bracket table supports a price for it.
   *  Never a placeholder -- a liquidation price that is actually unknown, drawn as a number,
   *  is the single most dangerous figure this page could print. */
  liquidation_price: string | null
  /** Distance from mark to liquidation as a **fraction** of the mark, matching every other
   *  `_pct` field in this platform (`max_drawdown_pct: 0.15` is fifteen percent).
   *
   *  The server sent this multiplied by 100 until it was fixed in `live/session.py`, which
   *  meant a position 5% from liquidation arrived as `5.0`, rendered as "500.00%", and never
   *  crossed the `< 0.05` threshold that turns the proximity bar red — the warning on the
   *  single most dangerous number on the page was silently off. */
  liq_distance_pct: number | null
  margin: string
  /** Which of the symbol's positions this row is: `BOTH` in one-way mode, `LONG` or `SHORT`
   *  in hedge mode. Optional so a monitor payload written before hedge mode reads as the
   *  one-way row it was, rather than as a missing field. */
  position_side?: PositionSide
}

/** One fill from the session's own log, reduced to what the monitor shows. */
export interface SessionFill {
  ts_ms: number
  symbol: string
  side: 'BUY' | 'SELL'
  qty: string
  price: string
  fee?: string
  realized_pnl?: string
  maker?: boolean
  order_id?: string
  tag?: string | null
}

/** One risk limit and how much of it the session has spent (spec 7).
 *
 *  `fraction` is `used / allowed` precomputed by the server, because `used` and `allowed`
 *  are exact decimal strings and dividing them in the browser means dividing two floats to
 *  decide the colour of a bar. */
export interface RiskUsage {
  limit: string
  used: string
  allowed: string
  fraction: number
}

/** The risk block of a monitor snapshot: `RiskEngine.summary()` with `usage` spliced in.
 *
 *  **This interface once declared a `breaches: RiskBreach[]` the server has never sent, and
 *  a `usage` the server did not send either.** TypeScript cannot check a hand-written type
 *  against a Python dict, so both read as present, `risk.usage` came back `undefined`, and
 *  `RiskUsageList` threw `undefined.length` on the first monitor snapshot of every session
 *  — blanking the whole app, because there was no error boundary then. Every field below is
 *  now one `summary()` emits; the breach *list* is not among them (only `breach_count` is),
 *  and it is reached through the run detail endpoint instead. */
export interface MonitorRisk {
  limits: Record<string, string | number | boolean | null>
  halted: boolean
  halt_reason: RiskBreach | null
  breach_count: number
  breaches_dropped: number
  rejected_orders: number
  peak_equity: string
  kill_switch: {
    flatten: boolean
    tripped_at_ms: number | null
    trigger: string | null
    detail: string
  }
  usage: RiskUsage[]
}

/** What left for the venue and what came back — present only on a live session.
 *
 *  `unknown_outcomes` and `unresolved` are the numbers to watch: an order in there was
 *  sent and never answered, may be working at the exchange with nothing in the ledger to
 *  say so, and is settled by the 60-second reconciliation rather than by hope.
 *  `foreign_reports` non-zero means something other than this session is trading the
 *  account. */
export interface MonitorTransport {
  placed: number
  acks: number
  cancels_sent: number
  reports: number
  fills_booked: number
  rejections: number
  unknown_outcomes: number
  unresolved: string[]
  foreign_reports: number
  foreign_client_order_ids: string[]
  exchange_closures: number
  duplicate_reports: number
  dropped_frames: number
  cancel_failures: number
  queued: number
}

/** How the spec 6.7.3 loop is faring — present only on a live session.
 *
 *  `blind_for_ms` is the number an operator acts on: how long the account has gone
 *  unverified. A fetch failure is never a mismatch — see `live/reconcile.py`. */
export interface MonitorReconcile {
  interval_s: number
  passes: number
  mismatches: number
  fetch_failures: number
  consecutive_failures: number
  blind_for_ms: number
  last_pass: {
    ts_ms: number
    fetched: boolean
    error: string
    mismatches: string[]
    checks: {
      field: string
      symbol: string | null
      ours: string
      theirs: string
      tolerance: string
      delta: string
      matched: boolean
    }[]
    skipped: { field: string; why: string }[]
  } | null
}

/** `PaperSession.monitor()`'s own payload — the `monitor` half of the envelope below.
 *
 *  Diffed field-by-field against the Python builder (`live/session.py monitor()`), because
 *  this app's worst historical defects were wire shapes tsc cannot check. Two traps this
 *  type used to fall into, recorded here so they stay dead:
 *  - the run's `status` string and `started_ms` are **not** in this payload — they live on
 *    the envelope's `run` row, and reading them here rendered `undefined · up NaN`;
 *  - `status` here is the connection **status log** (a list of feed events), not a run
 *    status. Merging the two payloads naively clobbers one with the other. */
export interface SessionMonitor {
  run_id: number
  endpoint: string
  /** `paper` or `live`. Older monitor payloads predate the field, and absent means paper —
   *  every session written before live mode existed was one. */
  mode?: 'paper' | 'live'
  /** The **server's** clock. Every age on the monitor is measured against this rather than
   *  against `Date.now()`: a browser whose clock is a minute behind would otherwise draw a
   *  live feed as a minute stale, and the operator would go looking for a dead socket. */
  now_ms: number
  engine_now_ms: number
  halted: boolean
  stopped_reason: string | null
  connection: MonitorConnection
  account: MonitorAccount
  positions: MonitorPosition[]
  risk: MonitorRisk
  /** Present and non-null only on a live session. Absence and `null` both mean paper. */
  transport?: MonitorTransport | null
  reconcile?: MonitorReconcile | null
  /** Free-form tallies (bars seen, orders sent, rejects, reconnects). Rendered by key, so a
   *  counter the server adds shows up here without a frontend change. */
  counts: Record<string, number>
  /** Newest-first fills, capped server-side. Optional because monitor.json files written
   *  before the field existed do not carry it — absent renders as an empty table, never a
   *  crash. */
  recent_fills?: SessionFill[]
  warmup?: { bars_required: number; bars_seen: number; warm: boolean }
  /** The connection status LOG — see the interface docstring. Not a run status. */
  status?: {
    ts_ms: number
    kind: string
    stream: string
    detail: string
    downtime_ms: number
  }[]
}

/** What `GET /sessions/{id}/monitor` actually returns: the run row beside the session's
 *  last published snapshot. `monitor` is null until the session's first checkpoint writes
 *  monitor.json (up to a minute in), and stays null for a session that crashed before one
 *  — the run row is what distinguishes those. */
export interface SessionMonitorEnvelope {
  run: Run
  monitor: SessionMonitor | null
}

/** One line of the Feed (spec 10.3). `payload` carries whatever the emitter attached.
 *
 *  There is deliberately no `message` field. The server has never sent one — the display
 *  line is *derived* from `kind` + `payload` in the client (see `Feed.tsx`), because the
 *  wire carries the event, not prose about it. This type once declared `message: string`,
 *  tsc cannot check a hand-written type against a Python dict, and the Feed rendered a
 *  permanently blank column with `undefined` in every tooltip. */
export interface FeedEntry {
  seq: number
  ts_ms: number
  kind: string
  severity: Severity
  source: string
  payload: Record<string, unknown>
}

/**
 * A page of the Feed, addressed by cursor.
 *
 * `next_seq` is where the *next* request starts, and it is the server's answer rather than
 * `last entry seq + 1` computed here -- an empty page still advances nothing and still has
 * to be resumable. Offsets are not used anywhere in this stream: the log is being appended
 * to while it is read, and an offset window over a growing log slides backwards under the
 * reader, showing the same entries twice and skipping the ones written in between.
 */
export interface FeedPage {
  entries: FeedEntry[]
  next_seq: number
}

export const FEED_SEVERITIES: Severity[] = ['error', 'warning', 'info']

/** Spec 6.7.1's four quantities and spec 6.7.2's verdict, exactly as `parity.json` holds
 *  them. Every monetary and basis-point field is an exact decimal string. */
export interface ParityFill {
  ts_ms: number
  seq: number
  symbol: string
  side: string
  qty: string
  price: string
  order_id: string
  tag: string | null
}

export interface ParityPayload {
  fills: {
    paper: number
    shadow: number
    /** Quantity 1, shadow minus paper. Zero does not mean the fills agreed -- a run that
     *  missed one and invented another reports zero, which is why `paper_only` and
     *  `shadow_only` are lists and not counts. */
    delta: number
    matched: number
    /** Quantity 2, signed against the trader. `null` when nothing matched, never zero:
     *  zero is a claim that the fills agreed. */
    avg_delta_bps: string | null
    avg_abs_delta_bps: string | null
    paper_only: ParityFill[]
    shadow_only: ParityFill[]
  }
  pnl: {
    paper_net: string
    shadow_net: string
    /** Quantity 3, shadow minus paper. Positive means the model overstated. */
    delta: string
    gross: string
    /** `null` when the session's round-trips realised nothing, so there is no scale to
     *  measure the difference against. */
    delta_fraction: string | null
  }
  diverged: boolean
  reasons: string[]
  thresholds: {
    pnl_fraction_of_gross: string
    avg_fill_delta_bps: string
    match_window_ms: number
  }
}

/**
 * The kill switch's own state, which outlives any one session (spec 7).
 *
 * It is server state, not browser state. Spec 7.6 requires an explicit un-arm before a live
 * session can start again, and a flag kept in this tab would be un-armed by a page reload --
 * which is to say the one safety interlock in the platform would be defeated by F5.
 */
export interface KillState {
  armed: boolean
  /** What fired it: an operator, or one of spec 7's auto-triggers. `null` when un-armed. */
  trigger: string | null
  detail: string | null
  armed_ms: number | null
  /** Whether the trip closed positions, from the trip record's own `flattened` field.
   *
   *  `undefined` when nothing is armed — the confirm dialog needs to distinguish "this
   *  trip flattened" from "there is no trip", because it states the behaviour in words
   *  before anything is sent and must not guess. */
  flatten: boolean | undefined
}

/** The `/kill` payload as the server actually sends it: `{kill: null | trip}`.
 *
 *  Kept separate from `KillState` because the two really are different shapes, and the
 *  bug this exists to prevent was reading `.armed` straight off the envelope — which is
 *  `undefined` forever, so the armed badge in the chrome and the Dashboard's armed banner
 *  could never render. The one safety interlock in the platform was invisible. */
interface KillEnvelope {
  kill: {
    armed: boolean
    trigger: string | null
    detail: string | null
    armed_ms: number | null
    flattened: boolean
  } | null
}

/** `POST /kill`'s full answer (`sessions.py kill()`): the trip plus what firing actually
 *  did — and failed to do.
 *
 *  `stop_failures` is the field that must never be dropped: each entry is a session whose
 *  control file could not be written, which means it was **never asked to stop and may
 *  still be trading**. The client used to pipe this response through `unwrapKill`,
 *  discarding all three action fields, and then toasted unconditional success. */
export interface KillFireResult {
  kill: KillState
  /** Run ids that were successfully asked to stop. */
  stopped: number[]
  /** Whether an in-memory key session existed and was destroyed. `false` is normal when
   *  nothing was connected — not a failure. */
  keys_wiped: boolean
  /** Human-readable lines, one per session the switch could NOT stop. */
  stop_failures: string[]
}

const UNARMED: KillState = {
  armed: false,
  trigger: null,
  detail: null,
  armed_ms: null,
  flatten: undefined,
}

function unwrapKill(payload: KillEnvelope): KillState {
  const trip = payload.kill
  if (trip == null || !trip.armed) return UNARMED
  return {
    armed: true,
    trigger: trip.trigger,
    detail: trip.detail,
    armed_ms: trip.armed_ms,
    // The server's field is `flattened` (what this trip did), not `flatten`.
    flatten: trip.flattened,
  }
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...init,
    headers: {
      ...(init?.body instanceof FormData ? {} : { 'Content-Type': 'application/json' }),
      ...(init?.headers ?? {}),
    },
  })
  if (!response.ok) {
    // The server sends `{detail: "..."}` for every handled failure, and those messages are
    // written to be read by a person. Surfacing the raw status instead would replace an
    // explanation with a number.
    let detail = `${response.status} ${response.statusText}`
    try {
      const body = await response.json()
      if (typeof body?.detail === 'string') detail = body.detail
      else if (Array.isArray(body?.detail)) detail = JSON.stringify(body.detail)
    } catch {
      /* body was not JSON; the status line is all we have */
    }
    throw new ApiError(detail, response.status)
  }
  return (await response.json()) as T
}

/** The page size `api.runs` asks for, exported so list views can tell "everything" from
 *  "the newest page of a longer history" — a list exactly this long is probably truncated. */
export const RUNS_PAGE_LIMIT = 200

export const api = {
  /** `root` is only present on a loopback-only instance — a network-exposed one withholds
   *  the filesystem path (see `app.py health()`). Typed optional so nothing renders a
   *  literal `undefined` for it. */
  health: () =>
    request<{ status: string; version: string; exposed: boolean; root?: string }>('/health'),

  template: () => request<{ template: string; example: string }>('/template'),

  validate: (code: string, quick = false) =>
    request<ValidationResult>('/validate', {
      method: 'POST',
      body: JSON.stringify({ code, quick, filename: 'strategy.py' }),
    }),

  list: (opts: { archived?: boolean; search?: string; tag?: string } = {}) => {
    const query = new URLSearchParams()
    if (opts.archived) query.set('archived', 'true')
    if (opts.search) query.set('search', opts.search)
    if (opts.tag) query.set('tag', opts.tag)
    const suffix = query.toString()
    return request<{ strategies: Strategy[] }>(`/strategies${suffix ? `?${suffix}` : ''}`)
  },

  get: (id: number) => request<{ strategy: Strategy }>(`/strategies/${id}`),

  create: (body: { name: string; code?: string; notes?: string; tags?: string[] }) =>
    request<SaveResponse>('/strategies', { method: 'POST', body: JSON.stringify(body) }),

  save: (id: number, code: string, message = '') =>
    request<SaveResponse>(`/strategies/${id}/save`, {
      method: 'POST',
      body: JSON.stringify({ code, message }),
    }),

  updateMeta: (id: number, body: { name?: string; notes?: string; tags?: string[] }) =>
    request<{ strategy: Strategy }>(`/strategies/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    }),

  archive: (id: number, archived: boolean) =>
    request<{ strategy: Strategy }>(`/strategies/${id}/archive`, {
      method: 'POST',
      body: JSON.stringify({ archived }),
    }),

  remove: (id: number, confirmName: string) =>
    request<{ deleted: number }>(
      `/strategies/${id}?confirm_name=${encodeURIComponent(confirmName)}`,
      { method: 'DELETE' },
    ),

  versions: (id: number) =>
    request<{ versions: StrategyVersion[] }>(`/strategies/${id}/versions`),

  version: (id: number, no: number) =>
    request<{ version: StrategyVersion }>(`/strategies/${id}/versions/${no}`),

  diff: (id: number, left: number, right: number) =>
    request<{ diff: string; left: number; right: number }>(
      `/strategies/${id}/diff?left=${left}&right=${right}`,
    ),

  exportUrl: (id: number, version?: number) =>
    `/api/strategies/${id}/export${version ? `?version=${version}` : ''}`,

  import: async (file: File) => {
    const form = new FormData()
    form.append('file', file)
    return request<SaveResponse>('/strategies/import', { method: 'POST', body: form })
  },

  // ------------------------------------------------------------------------- runs

  coverage: (symbol: string) =>
    request<{
      symbol: string
      start_ms: number | null
      end_ms: number | null
      bars: number
      /** Bar datasets carry `bars`, tick datasets carry `rows` (`runs.py get_coverage`).
       *  Both optional here because each entry has exactly one of them.
       *
       *  `segments` are the half-open `[start_ms, end_ms)` runs of days that actually hold
       *  data, and they are the reason `start_ms`/`end_ms` are not enough on their own: a
       *  dataset can span 865 days and hold 12 of them, which is `bookTicker` today. Always
       *  present — an empty dataset has `[]` — so the renderer never has to guess. */
      datasets: Record<
        string,
        {
          start_ms: number | null
          end_ms: number | null
          bars?: number
          rows?: number
          segments: { start_ms: number; end_ms: number }[]
          days_covered: number
          days_spanned: number
        }
      >
    }>(`/coverage?symbol=${encodeURIComponent(symbol)}`),

  /** What tier a range would actually execute at, asked *before* anything is queued.
   *  Spec 4.2 wants the downgrade visible on the results page; showing it there is
   *  necessary and late, because the user has already waited for the run. */
  tierPreview: (symbol: string, startMs: number, endMs: number, requested: FillTier) =>
    request<TierPreview>(
      `/tiers?symbol=${encodeURIComponent(symbol)}&start_ms=${startMs}&end_ms=${endMs}` +
        `&requested=${requested}`,
    ),

  runs: (opts: { strategyId?: number; archived?: boolean; limit?: number } = {}) => {
    const query = new URLSearchParams()
    if (opts.strategyId != null) query.set('strategy_id', String(opts.strategyId))
    if (opts.archived) query.set('archived', 'true')
    // Always explicit. The server defaults to 200 newest; relying on that default meant
    // the client could not know what ceiling its list was cut at, so a 200-row answer was
    // indistinguishable from a complete one.
    query.set('limit', String(opts.limit ?? RUNS_PAGE_LIMIT))
    return request<{ runs: Run[] }>(`/runs?${query.toString()}`)
  },

  run: (id: number) => request<RunDetail>(`/runs/${id}`),

  startRun: (body: StartRunBody) =>
    request<{ run: Run }>('/runs', { method: 'POST', body: JSON.stringify(body) }),

  cancelRun: (id: number) => request<{ run: Run }>(`/runs/${id}/cancel`, { method: 'POST' }),

  archiveRun: (id: number, archived: boolean) =>
    request<{ run: Run }>(`/runs/${id}/archive`, {
      method: 'POST',
      body: JSON.stringify({ archived }),
    }),

  deleteRun: (id: number) => request<{ deleted: number }>(`/runs/${id}`, { method: 'DELETE' }),

  equity: (id: number, points = 2000) =>
    request<EquitySeries>(`/runs/${id}/equity?points=${points}`),

  price: (id: number, points = 1200) => request<PriceSeries>(`/runs/${id}/price?points=${points}`),

  trades: (id: number) => request<{ trades: Trade[] }>(`/runs/${id}/trades`),

  events: (
    id: number,
    opts: { offset?: number; limit?: number; kind?: string; q?: string } = {},
  ) => {
    const query = new URLSearchParams()
    query.set('offset', String(opts.offset ?? 0))
    query.set('limit', String(opts.limit ?? 200))
    if (opts.kind) query.set('kind', opts.kind)
    if (opts.q) query.set('q', opts.q)
    return request<{ events: RunEvent[]; total: number; offset: number; limit: number }>(
      `/runs/${id}/events?${query.toString()}`,
    )
  },

  trials: (strategyId: number) => request<Trials>(`/strategies/${strategyId}/trials`),

  tradesCsvUrl: (id: number) => `/api/runs/${id}/trades.csv`,

  // ------------------------------------------------------------------- phase 7: live

  exchangeStatus: () => request<ExchangeStatus>('/exchange/status'),

  /** Keys go over the wire once and are held in backend memory only (spec 11). Nothing in
   *  this module writes them to `localStorage`, and nothing may: the whole point of the
   *  in-memory key session is that closing the process ends it. */
  exchangeConnect: (body: ConnectExchangeBody) =>
    request<ExchangeStatus>('/exchange/connect', { method: 'POST', body: JSON.stringify(body) }),

  /** Beyond the wiped state: `stopped` lists the live sessions this disconnect halted
   *  (cancel-only, positions deliberately left open — spec 11's expiry rule applied to a
   *  revoked credential). Surfaced so the operator hears it from the toast rather than
   *  from finding a stopped run later. */
  exchangeDisconnect: () =>
    request<ExchangeStatus & { keys_wiped: boolean; stopped: number[] }>(
      '/exchange/disconnect',
      { method: 'POST' },
    ),

  sessions: () => request<{ sessions: Run[] }>('/sessions'),

  /** Start a paper session. Returns `{ run }`, matching `POST /api/sessions`.
   *
   *  **This was typed `{ run_id: number }` and the server has always answered `{ run }`.**
   *  Nothing caught it because nothing called it — the endpoint was reachable only by
   *  `curl` until the Start Session screen existed — and TypeScript cannot see a wire shape,
   *  so the first caller would have read `data.run_id` as `undefined` and navigated to a
   *  run that did not exist. Typed from the router now, not from memory. */
  startSession: (body: StartSessionBody) =>
    request<{ run: Run }>('/sessions', { method: 'POST', body: JSON.stringify(body) }),

  /** What every running session has configured at the exchange, per symbol.
   *
   *  Advisory: the refusal in `POST /api/sessions` is the authority, because a claim taken
   *  between this read and that submit is exactly the race a form cannot close. */
  symbolClaims: (endpoint: string) =>
    request<{ endpoint: string; claims: SymbolClaim[] }>(
      `/symbol-claims?endpoint=${encodeURIComponent(endpoint)}`,
    ),

  /** The envelope, deliberately not unwrapped here: the run row and the snapshot answer
   *  different questions (is the session alive vs what is it holding), they collide on the
   *  key `status`, and a merge at this seam is exactly how the old flat type came to
   *  describe a wire that never existed. The component reads both halves by name. */
  monitor: (id: number) => request<SessionMonitorEnvelope>(`/sessions/${id}/monitor`),

  /** `flatten` is stated by the caller on every call rather than defaulted here. Whether
   *  stopping closes positions or merely stops trading is the difference between ending a
   *  session and taking a market exit in whatever the book looks like at that instant, and a
   *  default would let that decision be made by whoever wrote this line. */
  /** `already_finished: true` means the session was terminal before the request arrived —
   *  nothing was asked to stop and nothing is closing. Callers must branch their toast on
   *  it: "positions closing at market" about a session that finished an hour ago is a lie. */
  stopSession: (id: number, flatten: boolean) =>
    request<{ run: Run; already_finished: boolean }>(`/sessions/${id}/stop`, {
      method: 'POST',
      body: JSON.stringify({ flatten }),
    }),

  /** The route's real filters are `severity` and `kind` (`sessions.py run_feed`). This
   *  client used to send a `source` param the endpoint has never accepted — the dropdown
   *  wired to it filtered nothing — and never sent `kind`, which it does support. */
  feed: (
    id: number,
    opts: { sinceSeq?: number; limit?: number; severity?: string; kind?: string } = {},
  ) => {
    const query = new URLSearchParams()
    query.set('since_seq', String(opts.sinceSeq ?? 0))
    query.set('limit', String(opts.limit ?? 400))
    if (opts.severity) query.set('severity', opts.severity)
    if (opts.kind) query.set('kind', opts.kind)
    return request<FeedPage>(`/runs/${id}/feed?${query.toString()}`)
  },

  /** 404 when the session has no shadow backtest. That is a normal answer, not a failure --
   *  callers check `ApiError.status` and render nothing.
   *
   *  **The endpoint answers `{parity: report}`, not the report.** This was typed as the bare
   *  report and read straight off the envelope, so `report.fills` was `undefined` and the
   *  first run that actually had a parity report took the whole Runs tab down with
   *  `Cannot read properties of undefined (reading 'avg_delta_bps')`. `request` casts its
   *  response with `as T` and cannot see the difference, which is why the unwrap lives here
   *  next to the URL rather than in the component. */
  parity: (id: number) =>
    request<{ parity: ParityPayload }>(`/runs/${id}/parity`).then((payload) => payload.parity),

  killState: () => request<KillEnvelope>('/kill').then(unwrapKill),

  /** Fire the switch and return **everything** the server said about it.
   *
   *  This used to be typed as the bare `KillEnvelope` and piped through `unwrapKill`,
   *  which silently discarded `stopped`, `keys_wiped` and — worst — `stop_failures`: a
   *  transient failure to write one session's control file meant the operator saw a
   *  success toast while that session was still trading. The emergency endpoint's answer
   *  is surfaced whole; the caller decides what "success" means. */
  fireKill: (flatten: boolean) =>
    request<
      KillEnvelope & { stopped: number[]; keys_wiped: boolean; stop_failures: string[] }
    >('/kill', {
      method: 'POST',
      body: JSON.stringify({ flatten }),
    }).then(
      (payload): KillFireResult => ({
        kill: unwrapKill(payload),
        stopped: payload.stopped ?? [],
        keys_wiped: payload.keys_wiped ?? false,
        stop_failures: payload.stop_failures ?? [],
      }),
    ),

  /** Spec 7.6's explicit un-arm.
   *
   *  **Sends a body, and re-reads the state afterwards.** Two bugs lived in the one line
   *  this replaces: the POST carried no body, so FastAPI refused it 422 and the un-arm
   *  button had never actually worked; and the response was parsed as `{kill}` when the
   *  endpoint answers `{cleared}`, so the client reported "un-armed" whatever happened --
   *  including when nothing had. Re-reading `/kill` costs one request on a rare action
   *  and makes the answer the server's rather than this module's guess. */
  unarmKill: async (actor = 'operator'): Promise<KillState> => {
    await request<{ cleared: unknown }>('/kill/unarm', {
      method: 'POST',
      body: JSON.stringify({ actor }),
    })
    return request<KillEnvelope>('/kill').then(unwrapKill)
  },

  // --------------------------------------------------------------------- phase 9: lab

  labJobs: (opts: { runId?: number } = {}) => {
    const query = new URLSearchParams()
    if (opts.runId != null) query.set('run_id', String(opts.runId))
    const suffix = query.toString()
    return request<{ jobs: LabJob[] }>(`/lab/jobs${suffix ? `?${suffix}` : ''}`)
  },

  labJob: (id: number) => request<{ job: LabJob }>(`/lab/jobs/${id}`),

  submitLabJob: (body: { run_id: number; tool: LabTool; config: Record<string, unknown>; label?: string }) =>
    request<{ job: LabJob }>('/lab/jobs', { method: 'POST', body: JSON.stringify(body) }),

  /** 404 until the worker has finished — callers check `ApiError.status`, like `parity`. */
  labResult: (id: number) => request<LabResult>(`/lab/jobs/${id}/result`),

  labStitched: (id: number, points = 2000) =>
    request<StitchedSeries>(`/lab/jobs/${id}/stitched?points=${points}`),

  cancelLabJob: (id: number) =>
    request<{ job: LabJob }>(`/lab/jobs/${id}/cancel`, { method: 'POST' }),

  deleteLabJob: (id: number) =>
    request<{ deleted: number }>(`/lab/jobs/${id}`, { method: 'DELETE' }),

  /** 409 when the runs do not share a range — the refusal spec 9.6 requires, and its
   *  message names the runs, so it is surfaced verbatim rather than summarised. */
  compareRuns: (ids: number[]) =>
    request<ComparePayload>(`/lab/compare?runs=${ids.join(',')}`),

  /** 404 for single-symbol runs, with a detail that says why — a normal answer. */
  portfolio: (runId: number, window = 30) =>
    request<PortfolioReport>(`/runs/${runId}/portfolio?window=${window}`),

  // ---------------------------------------------------------------- phase 10: settings

  /** `problem` is non-null when the stored file could not be used as written — the
   *  settings shown are then platform defaults (or the stored values with a bad field),
   *  and saving overwrites the file. Surfaced, never swallowed: a user editing on top of
   *  defaults they believe are their saved values is exactly the silent-override failure
   *  the settings module's docstring warns about. */
  settings: () => request<{ settings: ServerSettings; problem: string | null }>('/settings'),

  saveSettings: (body: ServerSettings) =>
    request<{ settings: ServerSettings; problem: string | null }>('/settings', {
      method: 'PUT',
      body: JSON.stringify(body),
    }),

  // ----------------------------------------------------------- phase 11: data refresh

  /** Ask what a refresh *would* do, without doing it.
   *
   *  Two separate calls rather than one call with a callback, because the answer to the
   *  first is shown to a person and the second only happens if that person agrees.
   *  `conflicts_predicted` is the server's advance warning that the archive and the lake
   *  already disagree somewhere in the requested range. */
  planDataUpdate: (kind: DataUpdateKind, symbol: string) => {
    const body: DataUpdateBody = { kind, symbol, dry_run: true }
    return request<{ plan: RefreshPlan; conflicts_predicted: string[] }>('/data/update', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  /** Actually run it. `confirm` is always sent, because this function is only ever reached
   *  after the plan has been shown — either it needed confirmation and got it, or it said
   *  it did not need any. 409 here is a refusal with a reason (a lock held, the collector
   *  reconnecting, no disk), and its `detail` is the message the caller must surface as
   *  written. */
  startDataUpdate: (kind: DataUpdateKind, symbol: string) => {
    const body: DataUpdateBody = { kind, symbol, dry_run: false, confirm: true }
    return request<{ job: IngestJob }>('/data/update', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  dataUpdates: (limit = 20) => request<{ jobs: IngestJob[] }>(`/data/updates?limit=${limit}`),

  dataUpdate: (id: number) => request<{ job: IngestJob }>(`/data/updates/${id}`),

  /** Returns the job, not a bare acknowledgement: `cancel_requested` flips to true while
   *  `status` may still be `running`, and the caller needs both to say "stopping" rather
   *  than "stopped". */
  cancelDataUpdate: (id: number) =>
    request<{ job: IngestJob }>(`/data/updates/${id}/cancel`, { method: 'POST' }),

  collector: () => request<{ collector: CollectorStatus }>('/data/collector'),
}

/** Server-side defaults (spec 10.3 Settings). Theme is browser-local and absent here. */
export interface ServerSettings {
  default_leverage: number
  maker_rate: string
  taker_rate: string
  latency_model: string
  submit_ms: number
  risk_enabled: boolean
  max_leverage: string | null
  max_daily_loss_pct: string | null
  max_drawdown_pct: string | null
  max_open_orders: number | null
  max_orders_per_minute: number | null
  min_equity_pct: string | null
  kill_switch_flatten: boolean
  sweep_workers: number | null
}

// ------------------------------------------------------------------------ lab types

export type LabTool = 'walkforward' | 'montecarlo' | 'regimes'

export interface LabJob {
  id: number
  run_id: number
  tool: LabTool
  status: RunStatus
  label: string
  config: Record<string, unknown>
  created_ms: number
  started_ms: number | null
  finished_ms: number | null
  progress_done: number
  progress_total: number
  summary: Record<string, unknown> | null
  error: string | null
}

/** One grid point of a walk-forward fold's IS sweep (`SweepResult.to_json`). */
export interface GridPoint {
  index: number
  params: Record<string, unknown>
  ok: boolean
  sharpe: number | null
  net_pnl: string | null
  max_drawdown: number | null
  round_trips: number | null
  total_return: number | null
  error: string | null
  event_hash: string | null
}

export interface FoldOos {
  start_ms: number
  end_ms: number
  sharpe: number | null
  sortino: number | null
  total_return: number | null
  annualised_return: number | null
  net_pnl: string
  max_drawdown: number | null
  round_trips: number
  fills: number
  halted: boolean
  flags: string[]
}

export interface FoldRecord {
  index: number
  is_start_ms: number
  is_end_ms: number
  oos_start_ms: number
  oos_end_ms: number
  grid: GridPoint[]
  objective: (number | null)[]
  chosen_index: number | null
  chosen_params: Record<string, unknown> | null
  is: {
    sharpe: number | null
    total_return: number | null
    annualised_return: number | null
    net_pnl: string | null
    max_drawdown: number | null
    round_trips: number | null
  }
  oos: FoldOos | null
  is_halted: boolean
  is_halt_limit: string | null
  wfe: number | null
  plateau_score: number | null
  neighbours_defined: number
  neighbours_total: number
  neighbourhood_unprofitable: boolean
  error: string | null
}

export interface WalkForwardPayload {
  config: {
    is_ms: number
    oos_ms: number
    step_ms: number | null
    mode: string
    objective: string
    objective_label: string
    grid: Record<string, unknown[]>
  }
  folds: FoldRecord[]
  fold_scales: number[]
  truncated_at_fold: number | null
  opening_balance: number
  oos_days: number
  wfe_aggregate: number | null
  wfe_aggregate_definition: string
  wfe_median: number | null
  stitched_total_return: number | null
  stitched_annualised: number | null
  stitched_samples: number
  stability: {
    axes: Record<
      string,
      { values: unknown[]; chosen: (unknown | null)[]; chosen_position: (number | null)[]; distinct: number }
    >
    folds: number
  }
  uncovered_ms: number
  warnings: string[]
}

export interface OverfitPayload {
  plateau: {
    per_fold: (number | null)[]
    median: number | null
    neighbourhood_unprofitable_folds: number
    reading: string
  }
  is_vs_oos: {
    points: { fold: number; is_sharpe: number | null; oos_sharpe: number | null }[]
    fit: { slope: number; intercept: number; correlation: number | null; points: number } | null
    reading: string
  }
  decay: {
    sharpe_by_fold: { fold: number; oos_sharpe: number | null }[]
    sharpe_fit: { slope: number; intercept: number; correlation: number | null; points: number } | null
    annualised_return_fit: { slope: number } | null
    reading: string
  }
  sensitivity: {
    axes: Record<string, unknown[]>
    render: 'heatmap' | 'parallel_coordinates'
    points: {
      index: number
      params: Record<string, unknown>
      mean_objective: number | null
      mean_sharpe: number | null
      folds_defined: number
      times_chosen: number
    }[]
  }
  trials: { combinations: number; evaluations: number; best_sharpe: number | null; selection_bias_sd: number | null } | null
}

export interface McDistribution {
  count: number
  mean: number
  median: number
  min: number
  max: number
  percentiles: Record<string, number>
}

export interface McMethod {
  method: string
  sizing: 'additive' | 'multiplicative'
  iterations: number
  /** How `iterations` was produced: 'exhaustive' means every candidate evaluated once
   * (random_start), so its figures are exact enumerations, not Monte Carlo draws.
   * Optional: artefacts written before the field exist without it. */
  sampling?: 'random' | 'exhaustive'
  final_equity: McDistribution | null
  max_drawdown: McDistribution | null
  sharpe: McDistribution | null
  sharpe_unavailable_reason: string | null
  prob_drawdown_breach: number | null
  drawdown_limit: number | null
  prob_ruin: number | null
  ruin_unavailable_reason: string | null
  notes: string[]
  error: string | null
  block_length?: number
  series?: { skipped: number; final_equity: number; max_drawdown: number | null; sharpe: number | null }[]
}

export interface MonteCarloPayload {
  config: Record<string, unknown>
  inputs: {
    trades: number
    grid_returns: number
    grid: string
    periods_per_year: number
    opening_balance: number
    max_drawdown_limit: number | null
  }
  methods: Record<string, McMethod>
  caveats: string[]
}

export interface RegimeBucket {
  periods: number
  period_share: number
  mean_return: number | null
  total_return: number | null
  total_return_floored: boolean
  sharpe_conditional: number | null
  best_period: number | null
  worst_period: number | null
  trades: number
  trade_net_pnl: number | null
  trade_win_rate: number | null
  trade_expectancy: number | null
  thin: boolean
}

export interface RegimeDimension {
  name: string
  available: boolean
  reason?: string
  buckets: Record<string, RegimeBucket>
  changes: { ms: number; label: string }[]
  [extra: string]: unknown
}

export interface RegimesPayload {
  config: Record<string, unknown>
  grid: string
  periods: number
  periods_per_year: number
  trades: number
  dimensions: Record<string, RegimeDimension>
  notes: string[]
}

export interface LabResult {
  tool: LabTool
  job: LabJob
  walkforward?: WalkForwardPayload
  overfit?: OverfitPayload
  montecarlo?: MonteCarloPayload
  regimes?: RegimesPayload
}

export interface StitchedSeries {
  ts: number[]
  equity: number[]
  pnl: number[]
  fold: number[]
  samples: number
  returned: number
}

export interface ComparePayload {
  run_ids: number[]
  start_ms: number
  end_ms: number
  boundaries: number[]
  curves: { run_id: number; label: string; equity_normalised: number[]; metrics: Record<string, unknown> | null }[]
  correlation: {
    run_ids: number[]
    matrix: (number | null)[][]
    /** Per-cell count of boundaries where both runs' returns were defined — the basis of
     * each pairwise-complete correlation. Optional: pre-existing artefacts lack it. */
    defined_returns?: number[][]
  }
  combined: { equity_normalised: number[]; definition: string }
}

export interface PortfolioReport {
  run_id: number
  symbols: string[]
  grid: string
  periods: number
  rolling_window: number
  correlation: { symbols: string[]; matrix: (number | null)[][] }
  rolling_correlation: { pair: [string, string]; ts: number[]; correlation: (number | null)[] }[]
  per_symbol: Record<string, { final_pnl: number; round_trips: number | null; traded: boolean }>
  warnings: string[]
  notes: string[]
}

// -------------------------------------------------- phase 11: data refresh ("update")

/** What a refresh can be asked for. The server takes the string; these two are the only
 *  values the buttons offer. */
export type DataUpdateKind = 'candles' | 'trades'

/** The request body of `POST /api/data/update`, spelled once.
 *
 *  `dry_run` and `confirm` are the two halves of the same interlock: the plan is asked for
 *  with `dry_run: true`, and the work is asked for with `confirm: true, dry_run: false`.
 *  Sending neither is refused 409 when the plan says confirmation is required, which is the
 *  point — a large download is never started by a single click. */
export interface DataUpdateBody {
  kind: DataUpdateKind
  symbol: string
  dry_run?: boolean
  confirm?: boolean
}

/** One period where the bulk archive and the lake do not agree.
 *
 *  `note` is the measured sentence the server composed — it names the partition's real
 *  bounds against the period's nominal bounds and how much of the difference is real. It is
 *  rendered **verbatim**, never summarised: it is the whole evidence for the verdict.
 *
 *  `missing_ms` is `null` when the overlap could not be measured at all, which is a
 *  different answer from `0` ("measured, nothing missing"). Rendering `null` as zero, or as
 *  a tick, would report an unmeasured gap as a clean one. `collector_rows` is `null` on the
 *  same terms. */
export interface IngestConflict {
  dataset: string
  period: string
  collector_rows: number | null
  missing_ms: number | null
  note: string
}

/** Per-dataset tallies inside a finished job's summary. */
export interface IngestDatasetResult {
  dataset: string
  written: number
  declined: number
  missing: number
  failed: number
}

/** What a finished ingest actually did.
 *
 *  `verdict` is the server's own judgement and the only one the UI may render: `attention`
 *  means something in here needs reading, and it must never be drawn as a plain success. */
export interface IngestSummary {
  verdict: 'clean' | 'attention'
  written: number
  skipped: number
  declined: number
  missing: number
  failed: number
  rows: number
  bytes_downloaded: number
  notes: string[]
  conflicts: IngestConflict[]
  datasets: IngestDatasetResult[]
}

/** One refresh, queued or finished. `status` carries the same six values a `Run` does,
 *  including `lost` — see the `RunStatus` note; the reasoning is identical.
 *
 *  `cancel_requested` is not a status: a job can be asked to stop and still be `running`
 *  until the worker reaches a point it can stop at. */
export interface IngestJob {
  id: number
  kind: string
  symbol: string
  status: RunStatus
  created_ms: number
  started_ms: number | null
  finished_ms: number | null
  progress_done: number
  progress_total: number
  summary: IngestSummary | null
  error: string | null
  cancel_requested: boolean
}

/** One dataset's share of a plan. `start`/`end` are the server's own strings (period
 *  labels, not epoch ms) and are printed as sent. */
export interface RefreshPlanDataset {
  dataset: string
  start: string | null
  end: string | null
  archives: number
  periods: string[]
  estimated_bytes: number | null
  note: string | null
}

/** What a refresh *would* do, answered before it does it.
 *
 *  `estimated_bytes` is `null` when the size could not be estimated. That is stated as
 *  unknown in the confirmation dialogue — never rendered as `0`, and never omitted, because
 *  the size is the one figure the confirmation exists to disclose. */
export interface RefreshPlan {
  kind: string
  symbol: string
  archives: number
  estimated_bytes: number | null
  needs_confirmation: boolean
  notes: string[]
  datasets: RefreshPlanDataset[]
}

/** The recorder's own state, read-only.
 *
 *  Every numeric field is nullable because the state file may be absent, partial, or
 *  written by a process that has since died — "not recorded" and "zero" are different
 *  answers and this endpoint distinguishes them.
 *
 *  Note what is *not* here: any completion figure. The server reports elapsed time,
 *  heartbeat age and restarts; it does not compute whether the recording is sufficient, so
 *  no percentage may be derived from these fields. */
export interface CollectorStatus {
  state_file_present: boolean
  pid: number | null
  last_heartbeat_ms: number | null
  heartbeat_age_ms: number | null
  run_started_ms: number | null
  run_elapsed_ms: number | null
  restarts_since_run_start: number | null
  datasets_recording: string[]
  caveats: string[]
}

/** UTC date only, for range pickers and run headers. */
export function formatDate(ms: number): string {
  const d = new Date(ms)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`
}

/** Parse `YYYY-MM-DD` as **UTC midnight**, never local.
 *
 * `new Date("2025-01-01")` is already UTC, but `new Date(2025, 0, 1)` is local — and the two
 * spellings sit one autocomplete apart. Being explicit here keeps a range picked in the
 * browser identical to the range the engine replays, which is otherwise off by the user's
 * offset and produces a backtest that quietly starts a day early. */
export function parseUtcDate(text: string): number | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text.trim())
  if (!match) return null
  const ms = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]))
  return Number.isFinite(ms) ? ms : null
}

/** Percent with a fixed number of places, or an em dash for a genuinely undefined metric. */
export function pct(value: number | null | undefined, places = 2): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return `${(value * 100).toFixed(places)}%`
}

export function num(value: number | null | undefined, places = 2): string {
  if (value == null || !Number.isFinite(value)) return '—'
  return value.toFixed(places)
}

/** Money for display. The exact string stays in the payload; this is the rendering.
 *
 * The precision adapts to the magnitude. A fixed 2 dp renders every price on a sub-cent
 * symbol as `0.00` — including the 1000-prefixed pairs whose 1e-8 tick is the stated reason
 * the storage scale is 10^8 at all. At or above 1 the two places a balance is read in are
 * kept; below that the places grow so the number still says something.
 */
export function money(value: string | null | undefined, places?: number): string {
  if (value == null) return '—'
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return value
  const magnitude = Math.abs(parsed)
  const digits =
    places ??
    (magnitude === 0 || magnitude >= 1
      ? 2
      : Math.min(8, 2 - Math.floor(Math.log10(magnitude))))
  return parsed.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return '—'
  const seconds = Math.round(ms / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ${minutes % 60}m`
  return `${Math.floor(hours / 24)}d ${hours % 24}h`
}

/** Wall clock to the millisecond, UTC, for live views.
 *
 * `formatTime` stops at minutes, which is unreadable in a feed: order, ack and fill for one
 * decision land inside the same minute, and a log that renders all three as the same
 * timestamp cannot show the sequence that is the whole reason for reading it. */
export function formatClock(ms: number): string {
  const d = new Date(ms)
  const pad = (n: number) => String(n).padStart(2, '0')
  return (
    `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}.` +
    String(d.getUTCMilliseconds()).padStart(3, '0')
  )
}

/** The direction glyph that accompanies every signed number (spec 10.1).
 *
 * Colour alone fails for colour-blind readers and fails in a screenshot, and a screenshot of
 * a PnL figure is how most of these numbers get discussed. `·` for exactly zero rather than
 * an arrow: zero has no direction, and drawing it as flat-but-up is a small lie in the one
 * place the platform is claiming to be exact. */
export function arrow(value: number): string {
  return value > 0 ? '▲' : value < 0 ? '▼' : '·'
}

/** `arrow` + an explicit `+` + an already-formatted magnitude.
 *
 * `toLocaleString` renders a leading `-` and nothing at all for a positive, so a column of
 * PnL figures is otherwise signed only half the time -- and an unsigned 1,240.00 beside a
 * -300.00 reads as a magnitude rather than as a gain. */
export function signed(text: string, value: number): string {
  return `${arrow(value)} ${value > 0 ? '+' : ''}${text}`
}

export function formatTime(ms: number): string {
  // UTC everywhere, matching the rest of the platform (spec 3.1). A local-time timestamp
  // next to a UTC bar time is how someone concludes a run started an hour before it did.
  const d = new Date(ms)
  const pad = (n: number) => String(n).padStart(2, '0')
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
    `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
  )
}
