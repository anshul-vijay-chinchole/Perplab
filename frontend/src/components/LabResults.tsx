/**
 * Per-tool Lab result views (spec 9), plus the multi-run compare view (spec 9.6).
 *
 * Rendering rules the spec makes load-bearing:
 * - The stitched OOS curve is the walk-forward's headline; the additive (fixed-notional)
 *   variant is drawn beside it because which one is true depends on the strategy's sizing,
 *   and the platform cannot know (same honesty rule as the Monte Carlo caveat).
 * - `max Sharpe` is always labelled overfit-prone — the label travels in the artefact.
 * - The plateau score is "displayed as a single prominent number" (spec 9.4).
 * - Regime buckets that are too thin to be evidence are greyed, not hidden (spec 9.3).
 * - Mismatched comparisons surface the server's refusal verbatim (spec 9.6).
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import {
  ApiError,
  api,
  formatDuration,
  arrow,
  money,
  num,
  pct,
  type FoldRecord,
  type McDistribution,
  type McMethod,
  type OverfitPayload,
  type RegimeDimension,
  type WalkForwardPayload,
} from '../api'
import { useUi } from '../store'
import { ChartFrame, CsvButton } from './Export'
import { JobStatus } from './Lab'

const TERMINAL = new Set(['done', 'failed', 'cancelled', 'lost'])
const W = 1000

/** Series colours, in assignment order. All carry meaning elsewhere in the app, but in a
 *  multi-series overlay they are only identity — the legend is the second signal. */
const SERIES_COLOURS = ['var(--info)', 'var(--pos)', 'var(--warn)', 'var(--down)', 'var(--text-dim)', 'var(--accent)']

// ---------------------------------------------------------------------- job detail

export function LabJobDetail({ jobId }: { jobId: number }) {
  const queryClient = useQueryClient()
  const notify = useUi((s) => s.notify)
  const selectJob = useUi((s) => s.selectLabJob)

  const jobQuery = useQuery({
    queryKey: ['lab-job', jobId],
    queryFn: () => api.labJob(jobId),
    refetchInterval: (query) =>
      query.state.data && TERMINAL.has(query.state.data.job.status) ? false : 1500,
  })
  const job = jobQuery.data?.job
  const done = job?.status === 'done'
  const result = useQuery({
    queryKey: ['lab-result', jobId],
    queryFn: () => api.labResult(jobId),
    enabled: done,
    staleTime: Infinity,
  })

  const cancel = useMutation({
    mutationFn: () => api.cancelLabJob(jobId),
    onSuccess: () => {
      notify('Stop requested — the worker stops at its next point boundary', 'warn')
      void queryClient.invalidateQueries({ queryKey: ['lab-job', jobId] })
    },
  })
  const remove = useMutation({
    mutationFn: () => api.deleteLabJob(jobId),
    onSuccess: () => {
      notify('Lab job deleted')
      selectJob(null)
      void queryClient.invalidateQueries({ queryKey: ['lab-jobs'] })
    },
    onError: (err) => notify(err instanceof ApiError ? err.message : String(err), 'error'),
  })

  if (job == null) {
    if (jobQuery.isError) {
      return (
        <section className="flex-1 p-4" style={{ fontSize: 12, color: 'var(--warn)' }}>
          {`△ Could not load Lab job #${jobId}: ${jobQuery.error instanceof ApiError ? jobQuery.error.message : String(jobQuery.error)}`}
        </section>
      )
    }
    return (
      <section className="flex-1 p-4">
        <div className="pl-skel" style={{ height: 14, width: 240, marginBottom: 14 }} />
        <div className="pl-skel" style={{ height: 84, maxWidth: 1200, marginBottom: 12 }} />
        <div className="pl-skel" style={{ height: 240, maxWidth: 1200 }} />
      </section>
    )
  }

  return (
    <section className="flex-1 flex flex-col" style={{ minWidth: 0 }}>
      <div
        className="flex items-center gap-3 px-3 shrink-0"
        style={{ height: 40, borderBottom: '1px solid var(--border)' }}
      >
        <span style={{ fontSize: 13, fontWeight: 600 }}>
          {job.tool === 'walkforward' ? 'Walk-forward' : job.tool === 'montecarlo' ? 'Monte Carlo' : 'Regimes'}{' '}
          <span className="mono" style={{ color: 'var(--text-mute)' }}>#{job.id}</span>
        </span>
        <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)' }}>run #{job.run_id}</span>
        <JobStatus job={job} />
        {job.status === 'running' && job.progress_total > 0 ? (
          <span className="pl-meter" style={{ width: 120, flex: 'none' }}>
            <span
              className="pl-meter-fill"
              style={{
                width: `${Math.min(100, (job.progress_done / job.progress_total) * 100)}%`,
                background: 'var(--info)',
              }}
            />
          </span>
        ) : null}
        <span className="flex-1" />
        {!TERMINAL.has(job.status) ? (
          <button type="button" className="pl-btn" onClick={() => cancel.mutate()} disabled={cancel.isPending}>
            Stop
          </button>
        ) : (
          <button
            type="button"
            className="pl-btn pl-btn-danger"
            disabled={remove.isPending}
            onClick={() => {
              // Spec 10.4: destructive actions confirm. This button occupies the same
              // position Stop had a moment earlier, so a misclick would otherwise
              // destroy a multi-hour walk-forward with no prompt.
              if (window.confirm(`Delete Lab job #${jobId} and its artefacts? The source run keeps its own data.`))
                remove.mutate()
            }}
          >
            Delete
          </button>
        )}
      </div>

      <div className="flex-1 p-4 pl-scroll" style={{ overflowY: 'auto' }}>
        {job.error ? (
          <pre
            className="mono pl-panel p-3"
            style={{ fontSize: 11, color: 'var(--down)', whiteSpace: 'pre-wrap', margin: '0 0 12px' }}
          >
            {job.error}
          </pre>
        ) : null}
        {!done ? (
          <p style={{ fontSize: 12, color: 'var(--text-dim)' }}>
            {job.status === 'running'
              ? 'Running. A walk-forward is one backtest per grid point per fold, plus one out-of-sample run per fold — progress counts those steps.'
              : job.status === 'queued'
                ? 'Queued — the worker process is starting.'
                : 'This job produced no result.'}
          </p>
        ) : result.data?.walkforward != null ? (
          <WalkForwardView jobId={jobId} payload={result.data.walkforward} overfit={result.data.overfit ?? null} />
        ) : result.data?.montecarlo != null ? (
          <MonteCarloView payload={result.data.montecarlo} />
        ) : result.data?.regimes != null ? (
          <RegimesView payload={result.data.regimes} />
        ) : result.isLoading ? (
          <div className="flex flex-col" style={{ gap: 12, maxWidth: 1200 }}>
            <div className="pl-skel" style={{ height: 84 }} />
            <div className="pl-skel" style={{ height: 260 }} />
            <div className="pl-skel" style={{ height: 180 }} />
          </div>
        ) : (
          // A `done` header above an empty body reads as "this produced nothing".
          <p style={{ fontSize: 12, color: 'var(--warn)' }}>
            △ This job is marked done but its result could not be loaded
            {result.error instanceof ApiError ? `: ${result.error.message}` : '.'}{' '}
            The artefact may have been removed from the job directory.
          </p>
        )}
      </div>
    </section>
  )
}

// -------------------------------------------------------------------- walk-forward

function WalkForwardView({
  jobId,
  payload,
  overfit,
}: {
  jobId: number
  payload: WalkForwardPayload
  overfit: OverfitPayload | null
}) {
  const stitched = useQuery({
    queryKey: ['lab-stitched', jobId],
    queryFn: () => api.labStitched(jobId),
    staleTime: Infinity,
  })

  return (
    <div className="flex flex-col" style={{ maxWidth: 1200, width: '100%', gap: 12 }}>
      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="pl-heading" style={{ margin: '0 0 10px' }}>Walk-forward summary</h3>
        <div className="flex flex-wrap gap-2">
          <Card label="Stitched OOS return" value={pct(payload.stitched_total_return)} signedBy={payload.stitched_total_return} />
          <Card label="Annualised" value={pct(payload.stitched_annualised)} signedBy={payload.stitched_annualised} />
          <Card
            label="WFE aggregate"
            value={num(payload.wfe_aggregate)}
            title={payload.wfe_aggregate_definition + '. Consistently below ~0.5 means the optimisation is fitting noise (spec 9.1).'}
          />
          <Card label="WFE median" value={num(payload.wfe_median)} />
          <Card label="Folds" value={String(payload.folds.length)} />
          <Card label="Objective" value={payload.config.objective_label} warn={payload.config.objective === 'max_sharpe'} />
          {overfit?.trials ? (
            <Card
              label="Trials"
              value={`${overfit.trials.combinations}`}
              title={`${overfit.trials.evaluations} evaluations across ${overfit.trials.combinations} distinct parameter combinations. The best of N trials is upward-biased ~${num(overfit.trials.selection_bias_sd)} standard deviations under the null (spec 8.5).`}
            />
          ) : null}
        </div>

        {payload.warnings.length > 0 ? (
          <div style={{ fontSize: 11, color: 'var(--warn)', marginTop: 10 }}>
            {payload.warnings.map((warning) => (
              <div key={warning}>⚠ {warning}</div>
            ))}
          </div>
        ) : null}
        {payload.uncovered_ms > 0 ? (
          <p style={{ fontSize: 11, color: 'var(--text-dim)', margin: '8px 0 0' }}>
            The final {formatDuration(payload.uncovered_ms)} of the range was shorter than one
            OOS window and was never evaluated.
          </p>
        ) : null}
      </section>

      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="pl-heading" style={{ margin: 0 }}>Stitched out-of-sample equity</h3>
        <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '2px 0 10px' }}>
          compounded (solid) · fixed-notional PnL (dashed) — which is true depends on how the
          strategy sizes
        </p>
        {stitched.data != null && stitched.data.ts.length >= 2 ? (
          <ChartFrame name={`walkforward-${jobId}-stitched`}>
            <StitchedChart
              ts={stitched.data.ts}
              equity={stitched.data.equity}
              pnl={stitched.data.pnl}
              folds={stitched.data.fold}
            />
          </ChartFrame>
        ) : stitched.isLoading ? (
          <div className="pl-skel" style={{ height: 240 }} />
        ) : (
          <p style={{ fontSize: 12, color: 'var(--text-mute)', margin: 0 }}>
            No stitched samples — every fold failed or was skipped.
          </p>
        )}
      </section>

      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="flex items-center gap-2 pl-heading" style={{ margin: '0 0 10px' }}>
          Per-fold IS vs OOS
          <CsvButton
            name={`walkforward-${jobId}-folds`}
            headers={['fold', 'chosen_params', 'is_sharpe', 'is_annualised', 'oos_sharpe', 'oos_annualised', 'oos_net_pnl', 'wfe', 'plateau_score', 'is_halted', 'error']}
            rows={payload.folds.map((fold) => [
              fold.index,
              fold.chosen_params == null ? null : JSON.stringify(fold.chosen_params),
              fold.is.sharpe,
              fold.is.annualised_return,
              fold.oos?.sharpe ?? null,
              fold.oos?.annualised_return ?? null,
              fold.oos?.net_pnl ?? null,
              fold.wfe,
              fold.plateau_score,
              fold.is_halted,
              fold.error,
            ])}
          />
        </h3>
        <FoldTable folds={payload.folds} />
      </section>

      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="pl-heading" style={{ margin: '0 0 10px' }}>Parameter stability</h3>
        <StabilityPanel stability={payload.stability} />
      </section>

      {overfit != null ? <OverfitPanel overfit={overfit} /> : null}
    </div>
  )
}

function StitchedChart({
  ts,
  equity,
  pnl,
  folds,
  height = 240,
}: {
  ts: number[]
  equity: number[]
  pnl: number[]
  folds: number[]
  height?: number
}) {
  const x = linear([ts[0], ts[ts.length - 1]], [0, W])
  const domain = padded(extent([...equity, ...pnl]))
  const y = linear(domain, [height - 6, 6])
  const up = equity[equity.length - 1] >= equity[0]

  const boundaries: number[] = []
  for (let i = 1; i < folds.length; i += 1) {
    if (folds[i] !== folds[i - 1]) boundaries.push(ts[i])
  }

  return (
    <svg viewBox={`0 0 ${W} ${height}`} style={{ width: '100%', height, display: 'block' }} preserveAspectRatio="none" role="img" aria-label="Stitched out-of-sample equity">
      {boundaries.map((boundary) => (
        <line
          key={boundary}
          x1={x(boundary)}
          x2={x(boundary)}
          y1={0}
          y2={height}
          stroke="var(--border)"
          strokeWidth={1}
          vectorEffect="non-scaling-stroke"
        />
      ))}
      <line
        x1={0}
        x2={W}
        y1={y(equity[0])}
        y2={y(equity[0])}
        stroke="var(--text-mute)"
        strokeWidth={1}
        strokeDasharray="3 3"
        vectorEffect="non-scaling-stroke"
      />
      <path d={path(ts, pnl, x, y)} fill="none" stroke="var(--text-mute)" strokeWidth={1} strokeDasharray="5 4" vectorEffect="non-scaling-stroke" opacity={0.9} />
      {/* Colour means profit or loss here — the run ended above or below where it started. */}
      <path d={path(ts, equity, x, y)} fill="none" stroke={up ? 'var(--pos)' : 'var(--down)'} strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
    </svg>
  )
}

function FoldTable({ folds }: { folds: FoldRecord[] }) {
  return (
    <div className="pl-scroll" style={{ overflowX: 'auto' }}>
      <table className="pl-table mono" style={{ fontSize: 11, minWidth: 760 }}>
        <thead>
          <tr>
            <th>Fold</th>
            <th style={{ textAlign: 'left' }}>Chosen params</th>
            <th style={{ textAlign: 'right' }}>IS Sharpe</th>
            <th style={{ textAlign: 'right' }}>IS ann.</th>
            <th style={{ textAlign: 'right' }}>OOS Sharpe</th>
            <th style={{ textAlign: 'right' }}>OOS ann.</th>
            <th style={{ textAlign: 'right' }}>OOS PnL</th>
            <th style={{ textAlign: 'right' }} title="Annualised OOS return / annualised IS return. Well below ~0.5 consistently means the optimisation is fitting noise (spec 9.1).">
              WFE
            </th>
            <th style={{ textAlign: 'right' }} title="Chosen Sharpe / mean of grid-neighbour Sharpes. Near 1.0 = plateau; much greater = isolated spike, almost certainly noise (spec 9.4).">
              Plateau
            </th>
          </tr>
        </thead>
        <tbody>
          {folds.map((fold) => (
            <tr key={fold.index} style={{ color: fold.error != null ? 'var(--text-mute)' : undefined }}>
              <td>{fold.index}</td>
              <td style={{ textAlign: 'left' }}>
                {fold.error != null ? (
                  <span style={{ color: 'var(--down)' }}>{fold.error}</span>
                ) : (
                  JSON.stringify(fold.chosen_params)
                )}
                {fold.neighbourhood_unprofitable ? (
                  <span style={{ color: 'var(--warn)', marginLeft: 6 }} title="The chosen point's grid neighbours lose money on average — the strongest overfit signal there is.">
                    ⚠ unprofitable neighbourhood
                  </span>
                ) : null}
                {fold.is_halted ? (
                  <span style={{ color: 'var(--warn)', marginLeft: 6 }} title={`The chosen in-sample evaluation risk-halted on ${fold.is_halt_limit} — the optimisation saw only the window before the halt, not the full fold.`}>
                    ⚠ IS halted
                  </span>
                ) : null}
                {fold.oos?.halted ? (
                  <span style={{ color: 'var(--warn)', marginLeft: 6 }} title="The out-of-sample evaluation risk-halted; its figures cover only the window before the halt.">
                    ⚠ OOS halted
                  </span>
                ) : null}
              </td>
              <td style={{ textAlign: 'right' }}>{num(fold.is.sharpe)}</td>
              <td style={{ textAlign: 'right' }}>{pct(fold.is.annualised_return)}</td>
              <td style={{ textAlign: 'right' }}>{num(fold.oos?.sharpe ?? null)}</td>
              <td style={{ textAlign: 'right' }}>{pct(fold.oos?.annualised_return ?? null)}</td>
              <td style={{ textAlign: 'right' }}>{money(fold.oos?.net_pnl ?? null)}</td>
              <td style={{ textAlign: 'right' }}>{num(fold.wfe)}</td>
              <td style={{ textAlign: 'right' }}>{num(fold.plateau_score)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function StabilityPanel({ stability }: { stability: WalkForwardPayload['stability'] }) {
  const axes = Object.entries(stability.axes)
  if (axes.length === 0) return <p style={{ fontSize: 12, color: 'var(--text-mute)', margin: 0 }}>Single-point grid — nothing to be unstable.</p>
  return (
    <div className="flex flex-wrap gap-3">
      {axes.map(([name, axis]) => (
        <div key={name} className="pl-panel" style={{ minWidth: 220, padding: 10, background: 'var(--surface-2)' }}>
          <div className="flex items-baseline gap-2" style={{ fontSize: 11 }}>
            <span className="mono" style={{ fontWeight: 600 }}>{name}</span>
            <span style={{ color: axis.distinct > Math.max(1, stability.folds / 2) ? 'var(--warn)' : 'var(--text-mute)' }}>
              {axis.distinct} distinct value{axis.distinct === 1 ? '' : 's'} across {stability.folds} folds
              {axis.distinct > Math.max(1, stability.folds / 2) ? ' — no stable optimum' : ''}
            </span>
          </div>
          {/* Chosen value's position on the axis per fold: a strip, not a line chart,
              because axis values need not be numeric or evenly spaced. */}
          <div className="flex gap-1 mt-2">
            {axis.chosen_position.map((position, fold) => (
              <div key={fold} title={`fold ${fold}: ${axis.chosen[fold] == null ? 'no point chosen' : String(axis.chosen[fold])}`} className="flex flex-col" style={{ gap: 1 }}>
                {axis.values.map((_, index) => (
                  <div
                    key={index}
                    style={{
                      width: 14,
                      height: 8,
                      borderRadius: 1,
                      background: position === axis.values.length - 1 - index ? 'var(--info)' : 'var(--surface-3)',
                    }}
                  />
                ))}
              </div>
            ))}
          </div>
          <div className="mono" style={{ fontSize: 10, color: 'var(--text-mute)', marginTop: 6 }}>
            {axis.chosen.map((value) => (value == null ? '—' : String(value))).join(' · ')}
          </div>
        </div>
      ))}
    </div>
  )
}

function OverfitPanel({ overfit }: { overfit: OverfitPayload }) {
  return (
    <section className="pl-panel" style={{ padding: 12 }}>
      <h3 className="pl-heading" style={{ margin: '0 0 10px' }}>Overfitting diagnostics</h3>
      <div className="flex flex-wrap gap-2 mb-3">
        {/* Spec 9.4: the single most actionable number, displayed prominently. */}
        <div className="pl-card" style={{ minWidth: 180 }} data-tip={overfit.plateau.reading}>
          <div className="pl-card-label">Plateau score (median)</div>
          <div
            className="pl-kpi"
            style={{
              color:
                overfit.plateau.median == null
                  ? 'var(--text-mute)'
                  : overfit.plateau.median > 1.5
                    ? 'var(--warn)'
                    : 'var(--text)',
            }}
          >
            {num(overfit.plateau.median)}
          </div>
          {overfit.plateau.neighbourhood_unprofitable_folds > 0 ? (
            <div style={{ fontSize: 10, color: 'var(--warn)' }}>
              {overfit.plateau.neighbourhood_unprofitable_folds} fold(s) chose a point whose
              neighbours lose money
            </div>
          ) : null}
        </div>
        <Card
          label="IS→OOS slope"
          value={num(overfit.is_vs_oos.fit?.slope ?? null)}
          title={overfit.is_vs_oos.reading}
        />
        <Card
          label="OOS decay / fold"
          value={num(overfit.decay.sharpe_fit?.slope ?? null)}
          title={overfit.decay.reading}
          warn={(overfit.decay.sharpe_fit?.slope ?? 0) < 0}
        />
      </div>

      <div className="flex flex-wrap gap-4">
        <div>
          <h4 className="pl-heading" style={{ margin: '0 0 6px' }}>IS vs OOS Sharpe by fold</h4>
          <Scatter
            points={overfit.is_vs_oos.points
              .filter((p) => p.is_sharpe != null && p.oos_sharpe != null)
              .map((p) => ({ x: p.is_sharpe as number, y: p.oos_sharpe as number, label: `fold ${p.fold}` }))}
            fit={overfit.is_vs_oos.fit}
          />
        </div>
        <div style={{ flex: 1, minWidth: 320 }}>
          <h4 className="pl-heading" style={{ margin: '0 0 6px' }}>
            Parameter sensitivity ({overfit.sensitivity.render === 'heatmap' ? 'mean objective across folds' : 'mean objective per point'})
          </h4>
          <Sensitivity sensitivity={overfit.sensitivity} />
        </div>
      </div>
    </section>
  )
}

function Scatter({
  points,
  fit,
  size = 200,
}: {
  points: { x: number; y: number; label: string }[]
  fit: { slope: number; intercept: number } | null
  size?: number
}) {
  if (points.length === 0) return <p style={{ fontSize: 11, color: 'var(--text-mute)' }}>No folds with both Sharpes defined.</p>
  const xDomain = padded(extent(points.map((p) => p.x)))
  const yDomain = padded(extent(points.map((p) => p.y)))
  const x = linear(xDomain, [24, size - 6])
  const y = linear(yDomain, [size - 18, 6])
  return (
    <svg viewBox={`0 0 ${size} ${size}`} style={{ width: size, height: size }} role="img" aria-label="IS versus OOS Sharpe">
      <rect x={24} y={6} width={size - 30} height={size - 24} fill="none" stroke="var(--border)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
      {fit != null ? (
        <line
          x1={x(xDomain[0])}
          y1={y(fit.intercept + fit.slope * xDomain[0])}
          x2={x(xDomain[1])}
          y2={y(fit.intercept + fit.slope * xDomain[1])}
          stroke="var(--text-mute)"
          strokeWidth={1}
          strokeDasharray="4 3"
          vectorEffect="non-scaling-stroke"
        />
      ) : null}
      {points.map((point) => (
        <circle key={point.label} cx={x(point.x)} cy={y(point.y)} r={3} fill="var(--text)" opacity={0.85}>
          <title>{`${point.label}: IS ${point.x.toFixed(2)} → OOS ${point.y.toFixed(2)}`}</title>
        </circle>
      ))}
      <text x={24} y={size - 4} fill="var(--text-mute)" fontSize={9}>IS →</text>
      <text x={4} y={16} fill="var(--text-mute)" fontSize={9}>OOS ↑</text>
    </svg>
  )
}

function Sensitivity({ sensitivity }: { sensitivity: OverfitPayload['sensitivity'] }) {
  const names = Object.keys(sensitivity.axes)
  const defined = sensitivity.points.filter((p) => p.mean_objective != null)
  if (defined.length === 0) return <p style={{ fontSize: 11, color: 'var(--text-mute)' }}>No point produced a defined objective.</p>
  const [lo, hi] = extent(defined.map((p) => p.mean_objective as number))

  const shade = (value: number | null) => {
    if (value == null) return 'transparent'
    const t = hi === lo ? 0.5 : (value - lo) / (hi - lo)
    // **Monochrome, and it has to be.** The scale is *relative* to this grid's own
    // best and worst, so a grid where every point loses money still has a brightest
    // cell -- painting that green (spec 10.1 assigns --pos to positive) told the reader
    // "there is a good region here" about a surface with no good region. Intensity
    // alone carries the ranking; the number is printed in every cell, and the sign is
    // the reader's to see.
    return `color-mix(in srgb, var(--text-dim) ${Math.round(t * 55)}%, var(--surface-2))`
  }

  if (sensitivity.render === 'heatmap' && names.length === 2) {
    const [rowAxis, colAxis] = names
    const rows = sensitivity.axes[rowAxis]
    const cols = sensitivity.axes[colAxis]
    const byKey = new Map(defined.map((p) => [`${String(p.params[rowAxis])}|${String(p.params[colAxis])}`, p]))
    return (
      <div className="pl-scroll" style={{ overflowX: 'auto' }}>
        <table className="mono" style={{ fontSize: 10, borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={{ padding: '4px 8px', color: 'var(--text-mute)', fontWeight: 500 }}>{rowAxis} \ {colAxis}</th>
              {cols.map((col) => (
                <th key={String(col)} style={{ padding: '4px 8px', color: 'var(--text-dim)' }}>{String(col)}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={String(row)}>
                <td style={{ padding: '4px 8px', color: 'var(--text-dim)' }}>{String(row)}</td>
                {cols.map((col) => {
                  const point = byKey.get(`${String(row)}|${String(col)}`)
                  const value = point?.mean_objective ?? null
                  return (
                    <td
                      key={String(col)}
                      style={{ padding: '4px 8px', textAlign: 'right', minWidth: 44, background: shade(value), border: '1px solid var(--bg)', cursor: 'default' }}
                      data-tip={
                        point != null
                          ? `mean objective ${String(point.mean_objective)} · ${JSON.stringify(point.params)} · chosen ${point.times_chosen}× · defined in ${point.folds_defined} fold(s)`
                          : 'undefined'
                      }
                    >
                      {value == null ? '—' : value.toFixed(2)}
                      {point != null && point.times_chosen > 0 ? ' •' : ''}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
        <div style={{ fontSize: 10, color: 'var(--text-mute)', marginTop: 4 }}>• = chosen in at least one fold</div>
      </div>
    )
  }

  // One axis, or more than two: a ranked table reads better than fake geometry.
  const ranked = [...sensitivity.points].sort((a, b) => (b.mean_objective ?? -Infinity) - (a.mean_objective ?? -Infinity))
  return (
    <table className="pl-table mono" style={{ fontSize: 11 }}>
      <thead>
        <tr>
          <th style={{ textAlign: 'left' }}>Params</th>
          <th style={{ textAlign: 'right' }}>Mean objective</th>
          <th style={{ textAlign: 'right' }}>Mean Sharpe</th>
          <th style={{ textAlign: 'right' }}>Chosen</th>
        </tr>
      </thead>
      <tbody>
        {ranked.map((point) => (
          <tr key={point.index} style={{ background: shade(point.mean_objective) }}>
            <td style={{ textAlign: 'left' }}>{JSON.stringify(point.params)}</td>
            <td style={{ textAlign: 'right' }}>{num(point.mean_objective)}</td>
            <td style={{ textAlign: 'right' }}>{num(point.mean_sharpe)}</td>
            <td style={{ textAlign: 'right' }}>{point.times_chosen}×</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

// -------------------------------------------------------------------- monte carlo

const MC_TITLES: Record<string, string> = {
  trade_permutation: 'Trade-order permutation — how much of the drawdown was luck of sequencing?',
  trade_bootstrap: 'Trade bootstrap — the sampling distribution of performance',
  block_bootstrap: 'Block bootstrap on returns — same, preserving autocorrelation',
  random_start: 'Random start — how dependent is the result on when it started?',
}

function MonteCarloView({ payload }: { payload: NonNullable<import('../api').LabResult['montecarlo']> }) {
  return (
    <div className="flex flex-col" style={{ maxWidth: 1200, width: '100%', gap: 12 }}>
      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="pl-heading" style={{ margin: '0 0 10px' }}>Monte Carlo inputs</h3>
        <div className="flex flex-wrap gap-2">
          <Card label="Trades" value={String(payload.inputs.trades)} />
          <Card label={`${payload.inputs.grid} returns`} value={String(payload.inputs.grid_returns)} />
          <Card label="Opening" value={money(String(payload.inputs.opening_balance))} />
          <Card
            label="Drawdown limit"
            value={payload.inputs.max_drawdown_limit == null ? 'none set' : pct(payload.inputs.max_drawdown_limit)}
          />
        </div>
        <div style={{ fontSize: 11, color: 'var(--text-dim)', marginTop: 10 }}>
          {payload.caveats.map((caveat) => (
            <p key={caveat} style={{ margin: '2px 0' }}>▸ {caveat}</p>
          ))}
        </div>
      </section>
      {Object.values(payload.methods).map((method) => (
        <McMethodCard key={method.method} method={method} />
      ))}
    </div>
  )
}

function McMethodCard({ method }: { method: McMethod }) {
  const hasDistributions =
    method.final_equity != null || method.max_drawdown != null || method.sharpe != null
  return (
    <section className="pl-panel" style={{ padding: 12 }}>
      <div className="flex items-baseline gap-2 flex-wrap">
        <h3 style={{ fontSize: 12, margin: 0 }}>{MC_TITLES[method.method] ?? method.method}</h3>
        <span className="mono" style={{ fontSize: 10, color: 'var(--text-mute)' }}>
          {method.sizing === 'additive' ? 'fixed-notional (additive)' : 'compounding (multiplicative)'} ·{' '}
          {method.iterations.toLocaleString()} iterations
          {method.block_length != null ? ` · blocks of ${method.block_length}` : ''}
        </span>
      </div>
      {method.error != null ? (
        <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '6px 0 0' }}>{method.error}</p>
      ) : (
        <>
          <div className="pl-scroll" style={{ overflowX: 'auto' }}>
            <table className="pl-table mono" style={{ fontSize: 11, marginTop: 8 }}>
              <thead>
                <tr>
                  <th style={{ textAlign: 'left' }}>Metric</th>
                  <th style={{ textAlign: 'right' }}>p5</th>
                  <th style={{ textAlign: 'right' }}>p25</th>
                  <th style={{ textAlign: 'right' }}>median</th>
                  <th style={{ textAlign: 'right' }}>p75</th>
                  <th style={{ textAlign: 'right' }}>p95</th>
                </tr>
              </thead>
              <tbody>
                <DistRow label="Final equity" dist={method.final_equity} render={(v) => money(String(v))} />
                <DistRow label="Max drawdown" dist={method.max_drawdown} render={(v) => pct(v)} />
                {method.sharpe != null ? (
                  <DistRow label="Sharpe" dist={method.sharpe} render={(v) => num(v)} />
                ) : (
                  <tr>
                    <td style={{ textAlign: 'left' }}>Sharpe</td>
                    <td colSpan={5} style={{ textAlign: 'left', color: 'var(--text-mute)' }}>
                      {method.sharpe_unavailable_reason ?? 'not available'}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {hasDistributions ? (
            <div style={{ marginTop: 12 }}>
              {/* The artefact stores summary percentiles, not the raw iteration draws
                  (`McDistribution` in api.ts) — so these strips mark exactly what exists
                  and are deliberately not a histogram. */}
              <div style={{ fontSize: 10.5, color: 'var(--text-mute)', marginBottom: 8 }}>
                Percentile strips — min/max ticks, p5–p95 whisker, p25–p75 box, median line.
                Drawn from stored percentiles; no raw samples exist to histogram.
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '110px minmax(0, 1fr)', gap: '10px 12px', alignItems: 'center' }}>
                {method.final_equity != null ? (
                  <>
                    <span className="pl-card-label">Final equity</span>
                    <McPercentileStrip dist={method.final_equity} render={(v) => money(String(v))} />
                  </>
                ) : null}
                {method.max_drawdown != null ? (
                  <>
                    <span className="pl-card-label">Max drawdown</span>
                    <McPercentileStrip
                      dist={method.max_drawdown}
                      render={(v) => pct(v)}
                      marker={method.drawdown_limit != null ? { value: method.drawdown_limit, label: 'drawdown limit' } : undefined}
                    />
                  </>
                ) : null}
                {method.sharpe != null ? (
                  <>
                    <span className="pl-card-label">Sharpe</span>
                    <McPercentileStrip dist={method.sharpe} render={(v) => num(v)} />
                  </>
                ) : null}
              </div>
            </div>
          ) : null}
          <div className="flex gap-4 mt-2 mono" style={{ fontSize: 11 }}>
            {method.prob_drawdown_breach != null ? (
              <span>
                P(drawdown ≥ {pct(method.drawdown_limit)}):{' '}
                <b style={{ color: method.prob_drawdown_breach > 0.05 ? 'var(--warn)' : 'var(--text)' }}>
                  {pct(method.prob_drawdown_breach)}
                </b>
              </span>
            ) : null}
            <span title={method.ruin_unavailable_reason ?? undefined}>
              P(ruin): {method.prob_ruin == null ? 'n/a' : pct(method.prob_ruin)}
            </span>
          </div>
          {method.notes.map((note) => (
            <p key={note} style={{ fontSize: 10, color: 'var(--text-mute)', margin: '4px 0 0' }}>{note}</p>
          ))}
          {method.series != null && method.series.length >= 2 ? (
            <RandomStartStrip series={method.series} />
          ) : null}
        </>
      )}
    </section>
  )
}

function DistRow({
  label,
  dist,
  render,
}: {
  label: string
  dist: McMethod['final_equity']
  render: (value: number) => string
}) {
  if (dist == null)
    return (
      <tr>
        <td style={{ textAlign: 'left' }}>{label}</td>
        <td colSpan={5} style={{ color: 'var(--text-mute)', textAlign: 'left' }}>—</td>
      </tr>
    )
  return (
    <tr>
      <td style={{ textAlign: 'left' }}>{label}</td>
      {['5', '25', '50', '75', '95'].map((q) => (
        <td key={q} style={{ textAlign: 'right' }}>{render(dist.percentiles[q])}</td>
      ))}
    </tr>
  )
}

const STRIP_W = 460
const STRIP_H = 18

/** Horizontal percentile strip: min/max ticks, a p5–p95 whisker, a p25–p75 box and a
 *  median line, on the metric's own linear scale. Honest by construction — every mark is
 *  a number the artefact actually contains. */
function McPercentileStrip({
  dist,
  render,
  marker,
}: {
  dist: McDistribution
  render: (value: number) => string
  marker?: { value: number; label: string }
}) {
  const q = dist.percentiles
  const p5 = q['5'] as number | undefined
  const p25 = q['25'] as number | undefined
  const p50 = q['50'] as number | undefined
  const p75 = q['75'] as number | undefined
  const p95 = q['95'] as number | undefined
  if (
    p5 == null || p25 == null || p50 == null || p75 == null || p95 == null ||
    ![p5, p25, p50, p75, p95].every((v) => Number.isFinite(v))
  )
    return null
  const domain = padded([Math.min(dist.min, p5), Math.max(dist.max, p95)], 0.04)
  const x = linear(domain, [4, STRIP_W - 4])
  const mid = STRIP_H / 2
  return (
    <div style={{ minWidth: 0 }}>
      <svg viewBox={`0 0 ${STRIP_W} ${STRIP_H}`} style={{ width: '100%', height: STRIP_H, display: 'block' }} preserveAspectRatio="none" role="img" aria-label="Percentile strip">
        <line x1={x(dist.min)} x2={x(dist.min)} y1={mid - 3} y2={mid + 3} stroke="var(--text-mute)" strokeWidth={1} opacity={0.55} vectorEffect="non-scaling-stroke" />
        <line x1={x(dist.max)} x2={x(dist.max)} y1={mid - 3} y2={mid + 3} stroke="var(--text-mute)" strokeWidth={1} opacity={0.55} vectorEffect="non-scaling-stroke" />
        <line x1={x(p5)} x2={x(p95)} y1={mid} y2={mid} stroke="var(--text-mute)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
        <line x1={x(p5)} x2={x(p5)} y1={mid - 4} y2={mid + 4} stroke="var(--text-mute)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
        <line x1={x(p95)} x2={x(p95)} y1={mid - 4} y2={mid + 4} stroke="var(--text-mute)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
        <rect x={x(p25)} y={3} width={Math.max(1, x(p75) - x(p25))} height={STRIP_H - 6} fill="var(--surface-3)" stroke="var(--border-strong)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
        <line x1={x(p50)} x2={x(p50)} y1={2} y2={STRIP_H - 2} stroke="var(--text)" strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
        {marker != null && marker.value >= domain[0] && marker.value <= domain[1] ? (
          <line x1={x(marker.value)} x2={x(marker.value)} y1={1} y2={STRIP_H - 1} stroke="var(--warn)" strokeWidth={1} strokeDasharray="3 2" vectorEffect="non-scaling-stroke">
            <title>{`${marker.label}: ${render(marker.value)}`}</title>
          </line>
        ) : null}
      </svg>
      <div className="flex justify-between mono" style={{ fontSize: 10, color: 'var(--text-mute)', marginTop: 2 }}>
        <span>p5 {render(p5)}</span>
        <span style={{ color: 'var(--text-dim)' }}>median {render(p50)}</span>
        <span>p95 {render(p95)}</span>
      </div>
    </div>
  )
}

function RandomStartStrip({
  series,
  height = 90,
}: {
  series: { skipped: number; final_equity: number }[]
  height?: number
}) {
  const xs = series.map((row) => row.skipped)
  const ys = series.map((row) => row.final_equity)
  const x = linear([xs[0], xs[xs.length - 1]], [0, W])
  const domain = padded(extent(ys))
  const y = linear(domain, [height - 4, 4])
  return (
    <div style={{ marginTop: 8 }}>
      <div style={{ fontSize: 10, color: 'var(--text-mute)', marginBottom: 2 }}>Final equity vs skipped periods (every start, exact)</div>
      <svg viewBox={`0 0 ${W} ${height}`} style={{ width: '100%', height, display: 'block' }} preserveAspectRatio="none">
        <line x1={0} x2={W} y1={height - 0.5} y2={height - 0.5} stroke="var(--border)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
        <path d={path(xs, ys, x, y)} fill="none" stroke="var(--text)" strokeWidth={1.2} vectorEffect="non-scaling-stroke" />
      </svg>
    </div>
  )
}

// ------------------------------------------------------------------------ regimes

const DIMENSION_TITLES: Record<string, string> = {
  volatility: 'Volatility (trailing realised, daily closes)',
  trend: 'Trend vs range (ADX on the daily bar)',
  funding: 'Funding regime (trailing settlement mean)',
  cascade: 'Cascade periods (after liquidation clusters)',
}

function RegimesView({ payload }: { payload: NonNullable<import('../api').LabResult['regimes']> }) {
  return (
    <div className="flex flex-col" style={{ maxWidth: 1200, width: '100%', gap: 12 }}>
      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="pl-heading" style={{ margin: '0 0 10px' }}>Regimes summary</h3>
        <div className="flex flex-wrap gap-2">
          <Card label={`${payload.grid} periods`} value={String(payload.periods)} />
          <Card label="Closed trades" value={String(payload.trades)} />
        </div>
      </section>
      {Object.values(payload.dimensions).map((dimension) => (
        <DimensionCard key={dimension.name} dimension={dimension} />
      ))}
      <section className="pl-panel" style={{ padding: 12, fontSize: 11, color: 'var(--text-dim)' }}>
        <h3 className="pl-heading" style={{ margin: '0 0 6px' }}>Notes</h3>
        {payload.notes.map((note) => (
          <p key={note} style={{ margin: '2px 0' }}>▸ {note}</p>
        ))}
      </section>
    </div>
  )
}

function DimensionCard({ dimension }: { dimension: RegimeDimension }) {
  return (
    <section className="pl-panel" style={{ padding: 12 }}>
      <h3 className="flex items-center gap-2" style={{ fontSize: 12, margin: '0 0 8px' }}>
        {DIMENSION_TITLES[dimension.name] ?? dimension.name}
        {dimension.available ? (
          <CsvButton
            name={`regimes-${dimension.name}`}
            headers={['regime', 'periods', 'period_share', 'mean_return', 'sharpe_conditional', 'total_return', 'trades', 'trade_win_rate', 'trade_net_pnl', 'thin']}
            rows={Object.entries(dimension.buckets).map(([label, bucket]) => [
              label, bucket.periods, bucket.period_share, bucket.mean_return,
              bucket.sharpe_conditional, bucket.total_return, bucket.trades,
              bucket.trade_win_rate, bucket.trade_net_pnl, bucket.thin,
            ])}
          />
        ) : null}
      </h3>
      {!dimension.available ? (
        <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: 0 }}>{dimension.reason}</p>
      ) : (
        <div className="pl-scroll" style={{ overflowX: 'auto' }}>
          <table className="pl-table mono" style={{ fontSize: 11 }}>
            <thead>
              <tr>
                <th style={{ textAlign: 'left' }}>Regime</th>
                <th style={{ textAlign: 'right' }}>Periods</th>
                <th style={{ textAlign: 'right' }}>Share</th>
                <th style={{ textAlign: 'right' }}>Mean ret.</th>
                <th style={{ textAlign: 'right' }} title="Conditional: the run's periods in this regime, annualised on the grid factor. Not the return of trading only this regime.">
                  Sharpe*
                </th>
                <th style={{ textAlign: 'right' }}>Total ret.</th>
                <th style={{ textAlign: 'right' }}>Trades</th>
                <th style={{ textAlign: 'right' }}>Win rate</th>
                <th style={{ textAlign: 'right' }}>Trade PnL</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(dimension.buckets).map(([label, bucket]) => (
                // Spec 9.3: thin buckets are greyed, not hidden — too few observations is
                // itself a finding.
                <tr
                  key={label}
                  style={{ color: bucket.thin ? 'var(--text-mute)' : undefined }}
                  title={bucket.thin ? 'Too few periods or trades to be evidence (spec 9.3) — shown greyed, not hidden.' : undefined}
                >
                  <td style={{ textAlign: 'left' }}>
                    {label}
                    {bucket.thin ? ' ·thin' : ''}
                  </td>
                  <td style={{ textAlign: 'right' }}>{bucket.periods}</td>
                  <td style={{ textAlign: 'right' }}>{pct(bucket.period_share, 1)}</td>
                  <td style={{ textAlign: 'right' }}>{pct(bucket.mean_return, 3)}</td>
                  <td style={{ textAlign: 'right' }}>{num(bucket.sharpe_conditional)}</td>
                  <td style={{ textAlign: 'right' }} title={bucket.total_return_floored ? 'compounding reached -100% and stopped early' : undefined}>
                    {pct(bucket.total_return)}{bucket.total_return_floored ? ' ⊘' : ''}
                  </td>
                  <td style={{ textAlign: 'right' }}>{bucket.trades}</td>
                  <td style={{ textAlign: 'right' }}>{pct(bucket.trade_win_rate, 0)}</td>
                  <td style={{ textAlign: 'right' }}>{bucket.trade_net_pnl == null ? '—' : money(String(bucket.trade_net_pnl))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}

// ------------------------------------------------------------------------ compare

export function CompareView() {
  const runs = useQuery({ queryKey: ['runs', 'compare'], queryFn: () => api.runs() })
  const completed = useMemo(
    () => (runs.data?.runs ?? []).filter((run) => run.status === 'done'),
    [runs.data],
  )
  const [picked, setPicked] = useState<number[]>([])
  const comparison = useQuery({
    queryKey: ['compare', picked.join(',')],
    queryFn: () => api.compareRuns(picked),
    enabled: picked.length >= 2,
    retry: 0,
  })

  const toggle = (id: number) =>
    setPicked((current) =>
      current.includes(id) ? current.filter((x) => x !== id) : current.length >= 8 ? current : [...current, id],
    )

  return (
    <section className="flex-1 p-4 pl-scroll" style={{ overflowY: 'auto', minWidth: 0 }}>
      <h2 className="pl-heading" style={{ margin: '0 0 8px' }}>Compare runs</h2>
      <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '0 0 8px' }}>
        Pick two to eight completed runs. Only runs over the <b>same range</b> compare — the
        server refuses mismatches rather than overlaying incomparable curves (spec 9.6).
      </p>
      {runs.isLoading ? (
        <div className="flex flex-wrap gap-1 mb-3">
          {[0, 1, 2, 3, 4, 5].map((chip) => (
            <div key={chip} className="pl-skel" style={{ width: 132, height: 28 }} />
          ))}
        </div>
      ) : (
        <div className="flex flex-wrap gap-1 mb-3">
          {completed.map((run) => (
            <button
              key={run.id}
              type="button"
              className="pl-btn mono"
              onClick={() => toggle(run.id)}
              style={{
                fontSize: 11,
                background: picked.includes(run.id) ? 'var(--surface-2)' : undefined,
                borderColor: picked.includes(run.id) ? 'var(--text-dim)' : undefined,
              }}
            >
              #{run.id} {run.strategy_name} v{run.version_no}
            </button>
          ))}
        </div>
      )}

      {picked.length < 2 ? null : comparison.isError ? (
        <div className="pl-panel p-3" style={{ fontSize: 12, color: 'var(--warn)' }}>
          {comparison.error instanceof ApiError ? comparison.error.message : String(comparison.error)}
        </div>
      ) : comparison.data != null ? (
        <ComparisonResult payload={comparison.data} />
      ) : (
        <div className="flex flex-col" style={{ gap: 12, maxWidth: 1200 }}>
          <div className="pl-skel" style={{ height: 264 }} />
          <div className="pl-skel" style={{ height: 140 }} />
        </div>
      )}
    </section>
  )
}

function ComparisonResult({ payload }: { payload: import('../api').ComparePayload }) {
  const height = 240
  const ts = payload.boundaries
  const allValues = payload.curves.flatMap((curve) => curve.equity_normalised).concat(payload.combined.equity_normalised)
  const x = linear([ts[0], ts[ts.length - 1]], [0, W])
  const domain = padded(extent(allValues))
  const y = linear(domain, [height - 6, 6])

  return (
    <div className="flex flex-col" style={{ maxWidth: 1200, width: '100%', gap: 12 }}>
      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="pl-heading" style={{ margin: '0 0 10px' }}>Aligned equity</h3>
        <ChartFrame name={`compare-${payload.run_ids.join('-')}`}>
        <svg viewBox={`0 0 ${W} ${height}`} style={{ width: '100%', height, display: 'block' }} preserveAspectRatio="none" role="img" aria-label="Aligned equity curves">
          <line x1={0} x2={W} y1={y(1)} y2={y(1)} stroke="var(--text-mute)" strokeWidth={1} strokeDasharray="3 3" vectorEffect="non-scaling-stroke" />
          {payload.curves.map((curve, index) => (
            <path
              key={curve.run_id}
              d={path(ts, curve.equity_normalised, x, y)}
              fill="none"
              stroke={SERIES_COLOURS[index % SERIES_COLOURS.length]}
              strokeWidth={1.4}
              vectorEffect="non-scaling-stroke"
            />
          ))}
          <path
            d={path(ts, payload.combined.equity_normalised, x, y)}
            fill="none"
            stroke="var(--text)"
            strokeWidth={1}
            strokeDasharray="6 4"
            vectorEffect="non-scaling-stroke"
            opacity={0.7}
          />
        </svg>
        </ChartFrame>
        <div className="flex flex-wrap gap-3 mt-2 mono" style={{ fontSize: 11 }}>
          {payload.curves.map((curve, index) => (
            <span key={curve.run_id} className="flex items-center gap-1.5">
              <span style={{ width: 12, height: 2, background: SERIES_COLOURS[index % SERIES_COLOURS.length], display: 'inline-block' }} />
              #{curve.run_id} {curve.label}
            </span>
          ))}
          <span className="flex items-center gap-1.5" title={payload.combined.definition}>
            <span style={{ width: 12, height: 0, borderTop: '2px dashed var(--text)', display: 'inline-block', opacity: 0.7 }} />
            naive combined
          </span>
        </div>
      </section>

      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="flex items-center gap-2 pl-heading" style={{ margin: '0 0 10px' }}>
          Metrics
          <CsvButton
            name={`compare-${payload.run_ids.join('-')}-metrics`}
            headers={['run_id', 'label', 'sharpe', 'sortino', 'max_drawdown', 'total_return']}
            rows={payload.curves.map((curve) => {
              const metrics = (curve.metrics ?? {}) as Record<string, unknown>
              return [
                curve.run_id, curve.label,
                metrics.sharpe as number | null, metrics.sortino as number | null,
                metrics.max_drawdown as number | null, metrics.total_return as number | null,
              ]
            })}
          />
        </h3>
        <div className="pl-scroll" style={{ overflowX: 'auto' }}>
          <table className="pl-table mono" style={{ fontSize: 11 }}>
            <thead>
              <tr>
                <th style={{ textAlign: 'left' }}>Run</th>
                <th style={{ textAlign: 'right' }}>Sharpe</th>
                <th style={{ textAlign: 'right' }}>Sortino</th>
                <th style={{ textAlign: 'right' }}>Max DD</th>
                <th style={{ textAlign: 'right' }}>Total return</th>
                <th style={{ textAlign: 'right' }}>Round trips</th>
              </tr>
            </thead>
            <tbody>
              {payload.curves.map((curve) => {
                const metrics = (curve.metrics ?? {}) as Record<string, unknown>
                const trades = (metrics.trades ?? {}) as Record<string, unknown>
                return (
                  <tr key={curve.run_id}>
                    <td style={{ textAlign: 'left' }}>#{curve.run_id} {curve.label}</td>
                    <td style={{ textAlign: 'right' }}>{num(metrics.sharpe as number | null)}</td>
                    <td style={{ textAlign: 'right' }}>{num(metrics.sortino as number | null)}</td>
                    <td style={{ textAlign: 'right' }}>{pct(metrics.max_drawdown as number | null)}</td>
                    <td style={{ textAlign: 'right' }}>{pct(metrics.total_return as number | null)}</td>
                    <td style={{ textAlign: 'right' }}>{String(trades.round_trips ?? '—')}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </section>

      {/* The grid is a function of the shared range, so "period" is exact without the
          panel having to name daily or hourly — and naming the wrong one would be worse
          than not naming it. */}
      <section className="pl-panel" style={{ padding: 12 }}>
        <h3 className="pl-heading" style={{ margin: '0 0 10px' }}>Correlation of per-period returns</h3>
        <table className="mono" style={{ fontSize: 11, borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th />
              {payload.correlation.run_ids.map((id) => (
                <th key={id} style={{ padding: '3px 10px', color: 'var(--text-dim)' }}>#{id}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {payload.correlation.matrix.map((row, i) => (
              <tr key={payload.correlation.run_ids[i]}>
                <td style={{ padding: '3px 10px', color: 'var(--text-dim)' }}>#{payload.correlation.run_ids[i]}</td>
                {row.map((value, j) => (
                  <td key={j} style={{ padding: '3px 10px', textAlign: 'right', color: value == null ? 'var(--text-mute)' : Math.abs(value) > 0.7 && i !== j ? 'var(--warn)' : undefined }}>
                    {value == null ? '—' : value.toFixed(2)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  )
}

// ------------------------------------------------------------------------ shared

function Card({
  label,
  value,
  title,
  warn,
  signedBy,
}: {
  label: string
  value: string
  title?: string
  warn?: boolean
  signedBy?: number | null
}) {
  const colour =
    warn === true
      ? 'var(--warn)'
      : signedBy == null
        ? 'var(--text)'
        : signedBy > 0
          ? 'var(--pos)'
          : signedBy < 0
            ? 'var(--down)'
            : 'var(--text)'
  return (
    <div className="pl-card" data-tip={title}>
      <div className="pl-card-label">{label}</div>
      <div className="pl-card-value" style={{ color: colour }}>
        {/* Spec 10.1: a signed number carries an arrow as well as a colour, because
            colour alone fails for colour-blind readers and in a screenshot -- which is
            how most of these figures get discussed. */}
        {signedBy == null ? value : `${arrow(signedBy)} ${signedBy > 0 ? '+' : ''}${value}`}
      </div>
    </div>
  )
}

// SVG helpers, matching Charts.tsx's conventions.

interface Scale {
  (value: number): number
}

function linear(domain: [number, number], range: [number, number]): Scale {
  const [d0, d1] = domain
  const [r0, r1] = range
  const span = d1 - d0
  if (!Number.isFinite(span) || span === 0) return () => (r0 + r1) / 2
  return (value: number) => r0 + ((value - d0) / span) * (r1 - r0)
}

function path(xs: number[], ys: number[], x: Scale, y: Scale): string {
  let out = ''
  for (let i = 0; i < xs.length; i += 1) {
    out += `${i === 0 ? 'M' : 'L'}${x(xs[i]).toFixed(2)} ${y(ys[i]).toFixed(2)}`
  }
  return out
}

function extent(values: number[]): [number, number] {
  let lo = Infinity
  let hi = -Infinity
  for (const value of values) {
    if (value < lo) lo = value
    if (value > hi) hi = value
  }
  if (!Number.isFinite(lo)) return [0, 1]
  return [lo, hi]
}

function padded([lo, hi]: [number, number], fraction = 0.06): [number, number] {
  const pad = (hi - lo) * fraction || Math.abs(hi) * 0.01 || 1
  return [lo - pad, hi + pad]
}
