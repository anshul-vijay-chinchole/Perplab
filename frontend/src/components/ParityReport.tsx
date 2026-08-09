/**
 * The backtest-live parity report (spec 6.7.1), on the run detail page.
 *
 * Spec 6.7 opens with the sentence this panel serves: *"Architecture alone does not prevent
 * divergence -- it has to be measured."* A shared core makes the backtester and the live
 * engine account identically; it does not make the *fill model* right, and nothing inside
 * either run can tell you whether it is. The evidence is a paper session and a backtest
 * re-run over the same window, compared fill by fill -- and this is where that comparison is
 * read.
 *
 * **Every delta is shadow minus paper**, so a positive number is the model overstating what
 * really happened. The direction is printed on the panel rather than left to the reader,
 * because a sign error in a divergence report is the wrong-number-that-looks-right this
 * platform treats as its worst outcome.
 *
 * **The unmatched fills are listed, not counted.** A fill the backtest invented and a fill
 * the session got that the backtest missed are different diseases with opposite causes, and
 * a single "12 unmatched" cannot tell them apart. They are also the quantity that a
 * fill-count delta of zero can hide completely -- one missed and one invented nets to zero.
 *
 * A run with no shadow backtest renders nothing at all. The endpoint answers 404 for that
 * case, and 404 here is an ordinary answer rather than a failure: a plain backtest has
 * nothing to be compared against.
 */

import { useQuery } from '@tanstack/react-query'
import { api, ApiError, formatClock, money, num, pct, type ParityFill } from '../api'
import { Signed } from './LiveMonitor'

export function ParityReport({ runId }: { runId: number }) {
  const parity = useQuery({
    queryKey: ['parity', runId],
    queryFn: () => api.parity(runId),
    // No retry: 404 is the expected answer for most runs, and retrying it turns "this run has
    // no shadow backtest" into two requests and a delay before the page settles.
    retry: false,
  })

  if (parity.isError) {
    const error = parity.error
    if (error instanceof ApiError && error.status === 404) return null
    return (
      <div className="pl-panel p-3">
        <h2 style={{ margin: 0, fontSize: 13 }}>Parity report</h2>
        <p style={{ margin: '4px 0 0', fontSize: 11, color: 'var(--down)' }}>
          The report could not be read: {String(error)}. That is not the same as the two runs
          agreeing -- nothing has been compared.
        </p>
      </div>
    )
  }
  if (!parity.data) return null

  const report = parity.data
  const fills = report.fills
  const pnl = report.pnl
  const avgBps = fills.avg_delta_bps == null ? null : Number(fills.avg_delta_bps)
  const pnlDelta = Number(pnl.delta)
  const unmatched = fills.paper_only.length + fills.shadow_only.length

  return (
    <div className="pl-panel p-3 flex flex-col gap-3">
      <div>
        <h2 style={{ margin: 0, fontSize: 13 }}>
          Parity report
          {report.diverged ? (
            <span
              className="pl-tag"
              style={{ marginLeft: 8, color: 'var(--down)', borderColor: 'var(--down)' }}
              title="Spec 6.7.2's divergence flag. The reasons below name the number that raised it."
            >
              DIVERGED
            </span>
          ) : (
            <span className="pl-tag" style={{ marginLeft: 8, color: 'var(--pos)' }}>
              WITHIN THRESHOLDS
            </span>
          )}
        </h2>
        <p style={{ margin: '2px 0 0', fontSize: 11, color: 'var(--text-mute)' }}>
          This session against a backtest re-run over its own window, same version, seed and
          params. Every delta is <b>shadow minus paper</b>: positive means the model overstated
          what actually happened.
        </p>
      </div>

      {report.reasons.length ? (
        <div
          className="p-2"
          style={{ border: '1px solid color-mix(in srgb, var(--down) 50%, var(--border))', borderRadius: 4 }}
        >
          {report.reasons.map((reason, index) => (
            <p
              key={index}
              style={{ margin: index ? '6px 0 0' : 0, fontSize: 12, color: 'var(--text-dim)' }}
            >
              <span style={{ color: 'var(--down)' }}>✕ </span>
              {reason}
            </p>
          ))}
          <p style={{ margin: '6px 0 0', fontSize: 11, color: 'var(--text-mute)' }}>
            Persistent divergence means the fill model needs recalibration, not that this one
            session went badly (spec 6.7.2).
          </p>
        </div>
      ) : null}

      {/* Spec 6.7.1's four quantities, in its order. */}
      <div className="flex flex-wrap gap-2">
        <Quantity
          label="Fill-count delta"
          body={<Signed value={fills.delta} text={String(Math.abs(fills.delta))} />}
          note={`shadow ${fills.shadow} · paper ${fills.paper} · ${fills.matched} matched`}
          help="Zero here does not mean the fills agreed: one missed and one invented nets to zero, which is why the unmatched lists exist."
        />
        <Quantity
          label="Avg fill-price delta"
          body={
            avgBps == null ? (
              <span className="mono" style={{ color: 'var(--text-mute)' }}>
                —
              </span>
            ) : (
              <Signed value={avgBps} text={`${num(avgBps)} bps`} />
            )
          }
          note={
            avgBps == null
              ? 'no fill matched in both runs'
              : `|avg| ${num(Number(fills.avg_abs_delta_bps ?? 0))} bps over ${fills.matched} matched`
          }
          help="Signed against the trader: positive means the shadow filled worse than the session did. A negative average is the one to act on -- the backtest filled better than reality, which is the direction that flatters a strategy into being traded."
        />
        <Quantity
          label="Final-PnL delta"
          body={<Signed value={pnlDelta} text={money(pnl.delta)} />}
          note={`paper ${money(pnl.paper_net)} · shadow ${money(pnl.shadow_net)}`}
          help="From attribution.net_pnl on both sides, parsed as exact decimals."
        />
        <Quantity
          label="Unmatched fills"
          body={
            <span className="mono" style={{ color: unmatched ? 'var(--warn)' : 'var(--text-dim)' }}>
              {unmatched}
            </span>
          }
          note={`${fills.paper_only.length} paper-only · ${fills.shadow_only.length} shadow-only`}
          help="Orders that filled in one run and not the other. They contribute to no average -- a fill with no counterpart has no price to compare against."
        />
      </div>

      <div className="mono flex flex-wrap gap-x-6 gap-y-1" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
        <span>
          gross PnL <span style={{ color: 'var(--text-dim)' }}>{money(pnl.gross)}</span>
        </span>
        <span title="net_pnl_delta / gross_pnl. Undefined when the session's round-trips realised nothing -- there is then no scale at which any difference would be small, and any difference is flagged.">
          delta / gross{' '}
          <span style={{ color: 'var(--text-dim)' }}>
            {pnl.delta_fraction == null ? 'undefined (gross was zero)' : pct(Number(pnl.delta_fraction))}
          </span>
        </span>
        <span>
          thresholds{' '}
          <span style={{ color: 'var(--text-dim)' }}>
            {pct(Number(report.thresholds.pnl_fraction_of_gross))} of gross ·{' '}
            {report.thresholds.avg_fill_delta_bps} bps · {report.thresholds.match_window_ms} ms match
            window
          </span>
        </span>
      </div>

      <UnmatchedTable
        title="Filled in the session, never in the backtest"
        subtitle="The fill model missed these. A backtest that never takes these trades is not the strategy that ran."
        fills={fills.paper_only}
      />
      <UnmatchedTable
        title="Filled in the backtest, never in the session"
        subtitle="The fill model invented these. Every metric on the backtest page includes trades that did not happen."
        fills={fills.shadow_only}
      />
    </div>
  )
}

function Quantity({
  label,
  body,
  note,
  help,
}: {
  label: string
  body: React.ReactNode
  note: string
  help: string
}) {
  return (
    <div className="pl-panel px-3 py-2" style={{ minWidth: 190, background: 'var(--surface-2)' }}>
      <div style={{ fontSize: 11, color: 'var(--text-mute)', cursor: 'help' }} title={help}>
        {label} <span style={{ color: 'var(--text-mute)' }}>ⓘ</span>
      </div>
      <div style={{ fontSize: 16 }}>{body}</div>
      <div style={{ fontSize: 10, color: 'var(--text-mute)', marginTop: 2 }}>{note}</div>
    </div>
  )
}

function UnmatchedTable({
  title,
  subtitle,
  fills,
}: {
  title: string
  subtitle: string
  fills: ParityFill[]
}) {
  if (!fills.length) return null
  return (
    <div>
      <p style={{ margin: 0, fontSize: 12 }}>
        {title} <span className="mono" style={{ color: 'var(--text-mute)' }}>({fills.length})</span>
      </p>
      <p style={{ margin: '2px 0 6px', fontSize: 11, color: 'var(--text-mute)' }}>{subtitle}</p>
      <div className="pl-scroll" style={{ maxHeight: 220 }}>
        <table className="pl-table mono" style={{ fontSize: 11 }}>
          <thead>
            <tr>
              <th>Time (UTC)</th>
              <th>Symbol</th>
              <th>Side</th>
              <th style={{ textAlign: 'right' }}>Qty</th>
              <th style={{ textAlign: 'right' }}>Price</th>
              <th>Tag</th>
            </tr>
          </thead>
          <tbody>
            {fills.map((fill) => (
              <tr key={`${fill.seq}-${fill.ts_ms}-${fill.order_id}`}>
                <td>{formatClock(fill.ts_ms)}</td>
                <td style={{ color: 'var(--text)' }}>{fill.symbol}</td>
                <td style={{ color: fill.side === 'BUY' ? 'var(--pos)' : 'var(--down)' }}>{fill.side}</td>
                <td style={{ textAlign: 'right' }}>{fill.qty}</td>
                <td style={{ textAlign: 'right', color: 'var(--text)' }}>{money(fill.price)}</td>
                <td>{fill.tag ?? '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
