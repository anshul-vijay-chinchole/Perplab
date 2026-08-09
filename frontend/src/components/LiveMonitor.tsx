/**
 * The real-time monitor for a paper or live session (spec 10.3).
 *
 * *"Live/paper runs show a real-time monitor instead"* -- instead of the results page, which
 * is a page about a run that is over. A session in progress is asked different questions:
 * what is open, how far is it from liquidation, is the data still arriving, how much of each
 * risk limit is spent, and how do I stop it. Those five, in that order, are this file.
 *
 * **It polls, and it does not open a WebSocket.** The API's password middleware covers HTTP
 * only, so a WS endpoint on the same server would be an unauthenticated door into an
 * instance that spec 11 lets the operator bind to a LAN address. A second auth path for one
 * live view is a worse trade than a 1 Hz poll, and at 1 Hz the monitor is already faster than
 * the mark price it is drawing (spec 3.4 publishes on a 1 s grid).
 *
 * **Every age is measured against the server's `now_ms`, never `Date.now()`.** A browser
 * clock a minute behind the host would otherwise render a healthy feed as a minute stale, and
 * the operator would go hunting for a socket that never dropped.
 *
 * **The liquidation bar fills as the position gets closer.** The reading grows with the
 * danger, so a glance at a row full of bar is a glance at a position about to be closed by
 * the exchange. A bar that emptied instead would put the alarming state at the invisible end.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import {
  api,
  ApiError,
  formatClock,
  formatDuration,
  money,
  pct,
  signed,
  type MonitorPosition,
  type MonitorReconcile,
  type MonitorTransport,
  type RiskUsage,
  type SessionFill,
  type SessionMonitor,
} from '../api'
import { useValueFlash } from '../lib/hooks'
import { useUi } from '../store'

const TERMINAL = new Set(['done', 'failed', 'cancelled', 'lost'])

/** The reference distance the liquidation bar is drawn against.
 *
 *  A proximity bar needs a far end, and "distance to liquidation" has no natural one -- a
 *  flat-ish position is thousands of percent away. Twenty percent is the choice: inside it
 *  the picture is the useful reading, and beyond it the number is, so the bar sits empty and
 *  the percentage carries the meaning. Stated as a constant because it is a display choice
 *  and not a risk threshold -- nothing is decided on it. */
const LIQ_BAR_SCALE = 0.2

/** Spec 7's default `max_disconnect_seconds`, which is the point at which a quiet feed stops
 *  being a quiet feed and becomes a kill-switch auto-trigger. The monitor colours the frame
 *  age against the same number so the two never disagree about what "stale" means. */
const DISCONNECT_LIMIT_MS = 30_000

export function LiveMonitor({ runId }: { runId: number }) {
  const monitor = useQuery({
    queryKey: ['monitor', runId],
    queryFn: () => api.monitor(runId),
    // 1 Hz while it runs, and silent once it stops. `staleTime: 0` for the same reason the
    // chrome badge sets it: the global 5 s default would hand this view a cached account
    // balance, which is the one number on the page nobody would think to distrust.
    // The run status lives on the envelope's run row -- the snapshot's own `status` key is
    // the connection log, and gating on it left this polling terminal sessions forever.
    refetchInterval: (query) =>
      TERMINAL.has(query.state.data?.run.status ?? '') ? false : 1000,
    staleTime: 0,
    retry: 0,
  })

  if (monitor.isLoading) {
    return (
      <div className="pl-panel p-3" style={{ fontSize: 12, color: 'var(--text-mute)' }}>
        Connecting to the session monitor…
      </div>
    )
  }
  if (monitor.isError) {
    return (
      <div className="pl-panel p-3" style={{ borderColor: 'color-mix(in srgb, var(--down) 45%, var(--border))' }}>
        <p style={{ margin: 0, fontSize: 12, color: 'var(--down)' }}>
          The monitor is unreachable: {String(monitor.error)}
        </p>
        <p style={{ margin: '4px 0 0', fontSize: 11, color: 'var(--text-mute)' }}>
          The session itself may still be running -- this says the API did not answer, not
          that the strategy stopped. The Feed is the place to check next.
        </p>
      </div>
    )
  }

  const run = monitor.data!.run
  const data = monitor.data!.monitor

  // The snapshot arrives with the session's first checkpoint, up to a minute after start
  // -- and never, for a session that died before one. The run row tells those two apart,
  // so it is shown rather than a spinner that cannot end.
  if (data == null) {
    const finished = TERMINAL.has(run.status)
    return (
      <div className="pl-panel p-3 flex flex-col gap-2">
        <div className="flex items-start gap-3">
          <p style={{ margin: 0, fontSize: 12, color: 'var(--text-mute)', flex: 1 }}>
            {finished
              ? `Session #${run.id} ended ${run.status} before publishing a monitor snapshot.`
              : `Session #${run.id} is ${run.status} — the first monitor snapshot is published within a minute of the session starting.`}
          </p>
          <StopControl runId={runId} status={run.status} />
        </div>
        {run.error ? (
          <p className="mono" style={{ margin: 0, fontSize: 11, color: 'var(--down)' }}>
            {run.error}
          </p>
        ) : null}
      </div>
    )
  }

  const stale = data.now_ms - data.connection.last_frame_ms

  return (
    <div className="flex flex-col gap-3">
      <div className="pl-panel p-3 flex flex-col gap-3">
        <div className="flex items-start gap-3">
          <div style={{ flex: 1, minWidth: 0 }}>
            <h2 style={{ margin: 0, fontSize: 13 }}>
              Live monitor
              {data.mode === 'live' ? (
                <span
                  className="mono"
                  title="Real orders: this session's fills come from the exchange, not the simulator."
                  style={{
                    fontSize: 10,
                    fontWeight: 700,
                    color: 'var(--down)',
                    border: '1px solid var(--down)',
                    borderRadius: 3,
                    padding: '1px 5px',
                    marginLeft: 8,
                    letterSpacing: '0.06em',
                  }}
                >
                  LIVE
                </span>
              ) : null}
              <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)', marginLeft: 8 }}>
                {run.status} · up{' '}
                {formatDuration(data.now_ms - (run.started_ms ?? run.created_ms))}
              </span>
            </h2>
            <ConnectionLine market={data.connection.market} user={data.connection.user} stale={stale} />
          </div>
          <StopControl runId={runId} status={run.status} />
        </div>

        {data.risk.halted ? (
          <div
            className="p-2"
            style={{
              border: '1px solid color-mix(in srgb, var(--down) 55%, var(--border))',
              borderRadius: 4,
            }}
          >
            <p style={{ margin: 0, fontSize: 12, color: 'var(--down)', fontWeight: 600 }}>
              Halted by the risk layer.
            </p>
            <p style={{ margin: '2px 0 0', fontSize: 11, color: 'var(--text-mute)' }}>
              The strategy is no longer submitting orders. Anything still open below is
              exposure the halt did not close.
            </p>
          </div>
        ) : null}

        <AccountRow monitor={data} />
      </div>

      <Panel
        title="Positions"
        subtitle="Liquidation proximity is drawn against a 20% reference; past that the number is the reading, not the bar."
      >
        <PositionTable positions={data.positions} />
      </Panel>

      <Panel
        title="Risk limits"
        subtitle="How much of each spec 7 limit this session has spent. A bar at full is the order after this one being refused."
      >
        <RiskUsageList usage={data.risk.usage} />
      </Panel>

      {data.transport ? (
        <Panel
          title="Exchange orders"
          subtitle="What left for the venue and what came back. Unresolved means sent and never answered — settled by reconciliation, never by assumption."
        >
          <TransportStats transport={data.transport} />
        </Panel>
      ) : null}

      {data.reconcile ? (
        <Panel
          title="Reconciliation"
          subtitle="Every 60s the account is fetched from Binance and five quantities compared against the ledger. A mismatch beyond tick/step tolerance trips the kill switch."
        >
          <ReconcileStats reconcile={data.reconcile} />
        </Panel>
      ) : null}

      <Panel title="Recent fills" subtitle="Newest first, straight off the session's own event log.">
        <FillTable fills={data.recent_fills ?? []} />
      </Panel>

      <Counts counts={data.counts} />
    </div>
  )
}

/* -------------------------------------------------------------------- live transport */

function TransportStats({ transport }: { transport: MonitorTransport }) {
  const alarm =
    transport.unknown_outcomes > 0 ||
    transport.foreign_reports > 0 ||
    transport.dropped_frames > 0 ||
    transport.cancel_failures > 0
  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap gap-x-4 gap-y-1 mono" style={{ fontSize: 11 }}>
        <Stat label="placed" value={String(transport.placed)} />
        <Stat label="acked" value={String(transport.acks)} />
        <Stat label="fills booked" value={String(transport.fills_booked)} />
        <Stat label="cancels sent" value={String(transport.cancels_sent)} />
        <Stat label="rejected" value={String(transport.rejections)} />
        <Stat label="queued" value={String(transport.queued)} />
      </div>
      {alarm ? (
        <div
          className="p-2"
          style={{
            border: '1px solid color-mix(in srgb, var(--down) 55%, var(--border))',
            borderRadius: 4,
            fontSize: 11,
          }}
        >
          {transport.unknown_outcomes > 0 ? (
            <p style={{ margin: 0, color: 'var(--down)' }}>
              {transport.unknown_outcomes} order request(s) were sent and never answered
              {transport.unresolved.length
                ? ` (unresolved: ${transport.unresolved.join(', ')})`
                : ''}
              . They may be working at the exchange with nothing in the ledger to say so.
              Reconciliation settles them; do not resubmit.
            </p>
          ) : null}
          {transport.foreign_reports > 0 ? (
            <p style={{ margin: '4px 0 0', color: 'var(--down)' }}>
              {transport.foreign_reports} execution report(s) carried client order ids this
              session never issued — something else is trading this account.
            </p>
          ) : null}
          {transport.exchange_closures > 0 ? (
            <p style={{ margin: '4px 0 0', color: 'var(--down)' }}>
              {transport.exchange_closures} exchange-initiated close(s): a liquidation or
              ADL closed a position from the venue's side.
            </p>
          ) : null}
          {transport.dropped_frames > 0 ? (
            <p style={{ margin: '4px 0 0', color: 'var(--warn)' }}>
              {transport.dropped_frames} execution report(s) were missed and re-derived
              from cumulative quantities — entry prices are uncertain until the next
              reconciliation pass.
            </p>
          ) : null}
          {transport.cancel_failures > 0 ? (
            <p style={{ margin: '4px 0 0', color: 'var(--warn)' }}>
              {transport.cancel_failures} cancel(s) failed — those orders may still be
              working at the exchange.
            </p>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}

function ReconcileStats({ reconcile }: { reconcile: MonitorReconcile }) {
  const last = reconcile.last_pass
  const blindS = Math.floor(reconcile.blind_for_ms / 1000)
  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap gap-x-4 gap-y-1 mono" style={{ fontSize: 11 }}>
        <Stat label="passes" value={String(reconcile.passes)} />
        <Stat label="mismatches" value={String(reconcile.mismatches)} />
        <Stat label="fetch failures" value={String(reconcile.fetch_failures)} />
        <Stat
          label="unverified for"
          value={reconcile.blind_for_ms > 0 ? `${blindS}s` : '0s'}
        />
      </div>
      {reconcile.blind_for_ms > 120_000 ? (
        <p style={{ margin: 0, fontSize: 11, color: 'var(--warn)' }}>
          The account has gone {formatDuration(reconcile.blind_for_ms)} without a
          successful check. Nothing is known to be wrong — and nothing is being verified.
        </p>
      ) : null}
      {last && last.fetched && last.mismatches.length > 0 ? (
        <div
          className="p-2"
          style={{
            border: '1px solid color-mix(in srgb, var(--down) 55%, var(--border))',
            borderRadius: 4,
          }}
        >
          <p style={{ margin: 0, fontSize: 11, color: 'var(--down)', fontWeight: 600 }}>
            The exchange disagrees with the ledger
          </p>
          <table className="pl-table mono" style={{ fontSize: 11, marginTop: 4 }}>
            <thead>
              <tr>
                <th>Field</th>
                <th style={{ textAlign: 'right' }}>Ours</th>
                <th style={{ textAlign: 'right' }}>Exchange</th>
                <th style={{ textAlign: 'right' }}>Δ</th>
              </tr>
            </thead>
            <tbody>
              {last.checks
                .filter((check) => !check.matched)
                .map((check) => (
                  <tr key={`${check.field}:${check.symbol ?? ''}`}>
                    <td>
                      {check.field}
                      {check.symbol ? ` (${check.symbol})` : ''}
                    </td>
                    <td style={{ textAlign: 'right' }}>{check.ours}</td>
                    <td style={{ textAlign: 'right' }}>{check.theirs}</td>
                    <td style={{ textAlign: 'right', color: 'var(--down)' }}>
                      {check.delta}
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      ) : last && last.fetched ? (
        <p style={{ margin: 0, fontSize: 11, color: 'var(--text-mute)' }}>
          Last pass agreed on every checked field
          {last.skipped.length ? ` (${last.skipped.length} skipped — flat or no data)` : ''}.
        </p>
      ) : last ? (
        <p style={{ margin: 0, fontSize: 11, color: 'var(--warn)' }}>
          Last pass could not fetch the account: {last.error}
        </p>
      ) : (
        <p style={{ margin: 0, fontSize: 11, color: 'var(--text-mute)' }}>
          No pass has completed yet — the first runs 60s after the session starts.
        </p>
      )}
    </div>
  )
}

/* ------------------------------------------------------------------------- connection */

function ConnectionLine({
  market,
  user,
  stale,
}: {
  market: 'up' | 'down'
  user: 'up' | 'down' | 'n/a'
  stale: number
}) {
  // Coloured against spec 7's disconnect trigger rather than against a feel-good threshold:
  // amber means "getting close to the thing that fires the kill switch", red means "past it".
  const staleColour =
    stale > DISCONNECT_LIMIT_MS
      ? 'var(--down)'
      : stale > DISCONNECT_LIMIT_MS / 3
        ? 'var(--warn)'
        : 'var(--text-mute)'
  return (
    <p className="mono flex items-center gap-3" style={{ margin: '4px 0 0', fontSize: 11 }}>
      <Dot state={market} label="market data" />
      <Dot state={user} label="user stream" />
      <span style={{ color: staleColour }}>
        last frame {stale < 0 ? '0' : Math.round(stale / 100) / 10}s ago
        {stale > DISCONNECT_LIMIT_MS ? ' — past the disconnect trigger' : ''}
      </span>
    </p>
  )
}

/** A dot plus its word. The word is not decoration: colour alone fails for colour-blind
 *  readers and fails in the screenshot someone pastes into a message asking what went
 *  wrong (spec 10.1). */
function Dot({ state, label }: { state: 'up' | 'down' | 'n/a'; label: string }) {
  const colour =
    state === 'up' ? 'var(--pos)' : state === 'down' ? 'var(--down)' : 'var(--text-mute)'
  return (
    <span
      className="flex items-center gap-1.5"
      style={{ color: state === 'down' ? 'var(--down)' : 'var(--text-dim)' }}
      title={
        state === 'n/a'
          ? `No ${label} exists for this session -- a paper run with no exchange attached has no user stream to lose.`
          : `${label} is ${state}`
      }
    >
      {/* The dot breathes while the stream is up: "connected and flowing" and "connected
          when this last rendered" should not look identical on a live view. */}
      <span
        className={state === 'up' ? 'pl-dot pl-dot-live' : 'pl-dot'}
        style={{ background: colour, color: colour }}
      />
      {label} {state}
    </span>
  )
}

/* ---------------------------------------------------------------------------- account */

/**
 * The account, as the server reports it.
 *
 * There is deliberately no "total unrealised PnL" card here. Adding one would mean summing
 * the positions' exact decimal strings as JavaScript floats and printing the result as money,
 * and this platform does not compute money outside the accounting seam -- `money()` parses a
 * server figure to render it, which is not the same as manufacturing a new one. Equity
 * already carries the mark-to-market, and each position's own unrealised PnL is a column in
 * the table below. If the headline figure is wanted it belongs in `monitor.account`, computed
 * once in Python, not summed twice in two browsers.
 *
 * Equity is the headline: it is the mark-to-market answer to "how is this going", so it is
 * set in KPI type and flashes toward the direction of each move. The flash reads the parsed
 * number only to pick a direction — the digits rendered are still the server's string.
 */
function AccountRow({ monitor }: { monitor: SessionMonitor }) {
  const account = monitor.account
  const equity = Number(account.equity)
  const flash = useValueFlash(Number.isFinite(equity) ? equity : null)
  return (
    <div className="flex flex-wrap items-stretch gap-2">
      <div className="pl-card" style={{ minWidth: 190 }}>
        <div className="pl-card-label">Equity</div>
        <div className={`pl-kpi ${flash}`}>{money(account.equity)}</div>
      </div>
      <Stat label="Wallet" value={money(account.wallet)} />
      <Stat label="Available" value={money(account.available)} />
      <Stat label="Used margin" value={money(account.used_margin)} />
    </div>
  )
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="pl-card">
      <div className="pl-card-label">{label}</div>
      <div className="pl-card-value">{value}</div>
    </div>
  )
}

/* -------------------------------------------------------------------------- positions */

function PositionTable({ positions }: { positions: MonitorPosition[] }) {
  if (!positions.length) {
    return (
      <p style={{ margin: 0, fontSize: 12, color: 'var(--text-mute)' }}>
        Flat. Nothing is open, so nothing can be liquidated.
      </p>
    )
  }
  return (
    <div className="pl-scroll">
      <table className="pl-table mono" style={{ fontSize: 11 }}>
        <thead>
          <tr>
            <th>Symbol</th>
            <th style={{ textAlign: 'right' }}>Qty</th>
            <th style={{ textAlign: 'right' }}>Entry</th>
            <th style={{ textAlign: 'right' }}>Mark</th>
            <th style={{ textAlign: 'right' }}>Margin</th>
            <th style={{ textAlign: 'right' }}>Unrealised</th>
            <th style={{ textAlign: 'right' }}>Liq price</th>
            <th style={{ minWidth: 150 }}>To liquidation</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((position, index) => {
            const qty = Number(position.qty)
            const pnl = Number(position.unrealized_pnl)
            const side = position.position_side ?? 'BOTH'
            const hedged = side !== 'BOTH'
            // In hedge mode the *slot* is the identity and the sign follows it; in one-way
            // mode the sign is all there is. Labelling a flat hedge leg from its quantity
            // would call the short side "LONG" the moment it closed.
            const label = hedged ? side : qty >= 0 ? 'LONG' : 'SHORT'
            const colour = label === 'LONG' ? 'var(--pos)' : 'var(--down)'
            return (
              // **Keyed by symbol *and* side.** One symbol can hold two positions in hedge
              // mode, so `key={position.symbol}` gave two rows the same key: React then
              // reconciles them as one, and the second leg's numbers overwrite the first's
              // in place — a long and a short rendered as a single row whose values flicker
              // between them. The index is included so a repeated symbol in the run's own
              // config cannot collide either.
              <tr key={`${position.symbol}:${side}:${index}`} className="pl-enter">
                <td style={{ color: 'var(--text)' }}>
                  {position.symbol}
                  <span style={{ color: colour, marginLeft: 6 }}>{label}</span>
                  {hedged ? (
                    <span
                      style={{ color: 'var(--text-mute)', marginLeft: 6 }}
                      title="Hedge mode: this symbol holds a long and a short position at once. Each has its own entry price, its own margin and its own liquidation price, and either can be liquidated while the other survives."
                    >
                      hedge
                    </span>
                  ) : null}
                </td>
                <td style={{ textAlign: 'right' }}>{position.qty}</td>
                <td style={{ textAlign: 'right' }}>{money(position.entry_price)}</td>
                <td style={{ textAlign: 'right', color: 'var(--text)' }}>{money(position.mark_price)}</td>
                <td style={{ textAlign: 'right' }}>{money(position.margin)}</td>
                <td style={{ textAlign: 'right' }}>
                  <Signed value={pnl} text={money(position.unrealized_pnl)} />
                </td>
                <td style={{ textAlign: 'right' }}>
                  {position.liquidation_price == null ? (
                    <span
                      style={{ color: 'var(--text-mute)' }}
                      title="No liquidation price is defined for this position -- it is flat, or no maintenance-margin bracket covers it. Not the same as being safe."
                    >
                      —
                    </span>
                  ) : (
                    money(position.liquidation_price)
                  )}
                </td>
                <td>
                  <LiquidationBar distance={position.liq_distance_pct} />
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

/** How close the mark is to the liquidation price, as a bar that grows with the danger. */
function LiquidationBar({ distance }: { distance: number | null }) {
  if (distance == null) {
    return (
      <span style={{ fontSize: 11, color: 'var(--text-mute)' }} title="No liquidation price to measure against.">
        —
      </span>
    )
  }
  const proximity = Math.min(1, Math.max(0, 1 - distance / LIQ_BAR_SCALE))
  const colour = distance < 0.05 ? 'var(--down)' : distance < 0.1 ? 'var(--warn)' : 'var(--text-dim)'
  return (
    <span className="flex items-center gap-2">
      <Meter fraction={proximity} colour={colour} />
      <span className="mono" style={{ fontSize: 11, color: colour, minWidth: 56, textAlign: 'right' }}>
        {pct(distance)}
      </span>
    </span>
  )
}

/* ------------------------------------------------------------------------------- risk */

function RiskUsageList({ usage }: { usage: RiskUsage[] | undefined }) {
  // **Missing and empty are different answers and must not render the same.** An absent
  // field is a server that did not send what this panel is about; an empty array is a
  // session with nothing spendable to show. Collapsing them is how "no limits are in
  // force" came to be printed over a session running under a 5x leverage cap.
  if (usage == null) {
    return (
      <p style={{ margin: 0, fontSize: 12, color: 'var(--warn)' }}>
        This snapshot carried no usage figures, so nothing here is known — which is not the
        same as nothing being in force. The limits themselves are listed in the run's risk
        section; treat this panel as unavailable rather than as empty.
      </p>
    )
  }
  if (!usage.length) {
    return (
      <p style={{ margin: 0, fontSize: 12, color: 'var(--text-mute)' }}>
        No limit with a measurable budget is in force: nothing bounds this session's size,
        loss, streak or order rate. A liquidation halt or a disconnect trigger may still be
        set — neither is a budget, so neither is drawn here.
      </p>
    )
  }
  return (
    <div className="flex flex-col gap-2">
      {usage.map((entry) => {
        // A fraction at or over 1 is the limit reached, not approached: spec 7 makes count
        // and loss limits breach on equality, so a full bar is already the refusal.
        const colour =
          entry.fraction >= 1 ? 'var(--down)' : entry.fraction >= 0.75 ? 'var(--warn)' : 'var(--text-dim)'
        return (
          <div key={entry.limit} className="flex items-center gap-3">
            <span className="mono" style={{ fontSize: 11, width: 190, color: 'var(--text-dim)' }}>
              {entry.limit}
            </span>
            <span style={{ flex: 1, minWidth: 80 }}>
              <Meter fraction={Math.min(1, entry.fraction)} colour={colour} />
            </span>
            <span className="mono" style={{ fontSize: 11, color: colour, width: 64, textAlign: 'right' }}>
              {pct(entry.fraction, 0)}
            </span>
            <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)', width: 190, textAlign: 'right' }}>
              {entry.used} / {entry.allowed}
            </span>
          </div>
        )
      })}
    </div>
  )
}

/* ------------------------------------------------------------------------------ fills */

function FillTable({ fills }: { fills: SessionFill[] }) {
  if (!fills.length) {
    return (
      <p style={{ margin: 0, fontSize: 12, color: 'var(--text-mute)' }}>
        Nothing has filled yet.
      </p>
    )
  }
  return (
    <div className="pl-scroll" style={{ maxHeight: 240 }}>
      <table className="pl-table mono" style={{ fontSize: 11 }}>
        <thead>
          <tr>
            <th>Time (UTC)</th>
            <th>Symbol</th>
            <th>Side</th>
            <th style={{ textAlign: 'right' }}>Qty</th>
            <th style={{ textAlign: 'right' }}>Price</th>
            <th style={{ textAlign: 'right' }}>Fee</th>
            <th style={{ textAlign: 'right' }}>Realised</th>
            <th>Liquidity</th>
          </tr>
        </thead>
        <tbody>
          {/* Rows are keyed by the fill's own identity, so an arriving fill mounts a new
              row and the entrance animation marks it as new — rows already on screen never
              re-animate on a poll. */}
          {fills.map((fill) => (
            <tr key={`${fill.ts_ms}-${fill.order_id ?? ''}-${fill.price}-${fill.qty}`} className="pl-enter">
              <td>{formatClock(fill.ts_ms)}</td>
              <td style={{ color: 'var(--text)' }}>{fill.symbol}</td>
              <td style={{ color: fill.side === 'BUY' ? 'var(--pos)' : 'var(--down)' }}>{fill.side}</td>
              <td style={{ textAlign: 'right' }}>{fill.qty}</td>
              <td style={{ textAlign: 'right', color: 'var(--text)' }}>{money(fill.price)}</td>
              <td style={{ textAlign: 'right' }}>{fill.fee == null ? '—' : money(fill.fee)}</td>
              <td style={{ textAlign: 'right' }}>
                {fill.realized_pnl == null ? (
                  '—'
                ) : (
                  <Signed value={Number(fill.realized_pnl)} text={money(fill.realized_pnl)} />
                )}
              </td>
              {/* Maker or taker decides which fee schedule this fill paid, so it is a column
                  rather than a tooltip -- a session whose fills all turned taker is paying a
                  different cost base than the one its backtest assumed. */}
              <td>{fill.maker == null ? '—' : fill.maker ? 'maker' : 'taker'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ----------------------------------------------------------------------------- counts */

function Counts({ counts }: { counts: Record<string, number> }) {
  const entries = Object.entries(counts)
  if (!entries.length) return null
  return (
    <div className="flex flex-wrap items-center gap-1">
      {entries.map(([key, value]) => (
        <span key={key} className="pl-tag mono">
          {key} {value.toLocaleString()}
        </span>
      ))}
    </div>
  )
}

/* ------------------------------------------------------------------------------- stop */

/**
 * The per-strategy stop (spec 10.3).
 *
 * It is not the kill switch and does not borrow its colour: this stops one session and
 * leaves the rest of the platform, the other sessions and the key session alone. The confirm
 * states which of the two behaviours the checkbox is currently asking for, because "stop"
 * meaning *leave the position open* and "stop" meaning *sell it at market right now* are
 * different instructions with opposite risks.
 */
function StopControl({ runId, status }: { runId: number; status: string }) {
  const client = useQueryClient()
  const notify = useUi((s) => s.notify)
  const [confirming, setConfirming] = useState(false)
  const [flatten, setFlatten] = useState(false)

  const stop = useMutation({
    mutationFn: () => api.stopSession(runId, flatten),
    onSuccess: (result) => {
      client.invalidateQueries({ queryKey: ['sessions'] })
      client.invalidateQueries({ queryKey: ['runs'] })
      client.invalidateQueries({ queryKey: ['run', runId] })
      client.invalidateQueries({ queryKey: ['monitor', runId] })
      setConfirming(false)
      // `already_finished` means the session was terminal before this request arrived —
      // nothing was asked to stop, so nothing is "closing at market". Toasting the stop
      // wording anyway would describe an exit the venue never saw.
      notify(
        result.already_finished
          ? `Session #${runId} had already finished (${result.run.status}) — there was nothing to stop.`
          : flatten
            ? 'Session stopping; positions closing at market.'
            : 'Session stopping; positions left open.',
        'warn',
      )
    },
    onError: (error) => notify(error instanceof ApiError ? error.message : String(error), 'error'),
  })

  if (TERMINAL.has(status)) return null

  if (!confirming) {
    return (
      <button className="pl-btn pl-btn-danger" onClick={() => setConfirming(true)}>
        Stop session
      </button>
    )
  }
  return (
    <div className="pl-panel p-2 flex flex-col gap-2" style={{ width: 300, background: 'var(--surface-2)' }}>
      <p style={{ margin: 0, fontSize: 12 }}>
        Stop session #{runId}?
        <span style={{ color: 'var(--text-mute)' }}>
          {' '}
          The strategy stops submitting orders and every working order is cancelled.
        </span>
      </p>
      <label className="flex items-start gap-2" style={{ fontSize: 11 }}>
        <input
          type="checkbox"
          checked={flatten}
          onChange={(event) => setFlatten(event.target.checked)}
          style={{ marginTop: 3 }}
        />
        <span>
          Close open positions at market
          <span style={{ color: 'var(--text-mute)' }}>
            {' '}
            — off leaves them open, which is spec 7.3's default. Nothing about stopping a
            strategy makes this instant a good price.
          </span>
        </span>
      </label>
      <div className="flex items-center gap-2">
        <button className="pl-btn" onClick={() => setConfirming(false)} autoFocus>
          Keep running
        </button>
        <span className="flex-1" />
        <button className="pl-btn pl-btn-danger" disabled={stop.isPending} onClick={() => stop.mutate()}>
          {stop.isPending ? 'Stopping…' : flatten ? 'Stop and close' : 'Stop, keep positions'}
        </button>
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------------- primitives */

/**
 * A signed, coloured, arrow-carrying number (spec 10.1).
 *
 * `value` decides the sign, the arrow and the colour; `text` is the already-formatted
 * magnitude, so the caller keeps control of precision -- currency to two places, a rate in
 * basis points, a quantity at the symbol's step size. Splitting them is what stops this
 * component from having to know which of those it is rendering.
 */
export function Signed({ value, text, title }: { value: number; text: string; title?: string }) {
  const colour = value > 0 ? 'var(--pos)' : value < 0 ? 'var(--down)' : 'var(--text-dim)'
  return (
    <span className="mono" style={{ color: colour }} title={title}>
      {signed(text, value)}
    </span>
  )
}

/** The bar primitive. `fraction` is clamped here so no caller can draw past the end of the
 *  track -- a 140%-wide fill silently overflows its container and reads as exactly full. */
function Meter({ fraction, colour }: { fraction: number; colour?: string }) {
  const clamped = Number.isFinite(fraction) ? Math.min(1, Math.max(0, fraction)) : 0
  return (
    <span className="pl-meter" role="img" aria-label={`${Math.round(clamped * 100)} percent`}>
      <span
        className="pl-meter-fill"
        style={{ width: `${clamped * 100}%`, background: colour ?? 'var(--text-dim)' }}
      />
    </span>
  )
}

function Panel({
  title,
  subtitle,
  children,
}: {
  title: string
  subtitle?: string
  children: React.ReactNode
}) {
  return (
    <div className="pl-panel p-3">
      <h2 className="pl-heading" style={{ margin: 0 }}>{title}</h2>
      {subtitle ? (
        <p style={{ margin: '3px 0 10px', fontSize: 11, color: 'var(--text-mute)' }}>{subtitle}</p>
      ) : (
        <div style={{ height: 10 }} />
      )}
      {children}
    </div>
  )
}
