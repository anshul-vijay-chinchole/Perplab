/**
 * The Lab tab (spec 9, 10.3): pick a completed run, pick a tool, configure, run as a job.
 *
 * Layout mirrors the Runs tab: the job list narrows when a job is open rather than being
 * replaced, because Lab artefacts are things you compare against their neighbours. The
 * right panel is either a job's results (per-tool views in `LabResults.tsx`), the compare
 * view, or the new-job form.
 *
 * Jobs poll at 2 s while any is non-terminal, matching the run list's cadence. Progress is
 * the worker's own step counter (grid points × folds for a walk-forward), not a guess.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { ApiError, api, formatTime, type LabJob, type LabTool, type Run } from '../api'
import { useUi } from '../store'
import { CompareView, LabJobDetail } from './LabResults'

const TERMINAL = new Set(['done', 'failed', 'cancelled', 'lost'])

const TOOL_LABELS: Record<LabTool, string> = {
  walkforward: 'Walk-forward',
  montecarlo: 'Monte Carlo',
  regimes: 'Regimes',
}

const HOUR_MS = 3_600_000
const DAY_MS = 24 * HOUR_MS

/** Parse a human-typed number, accepting the separators people actually type.
 *
 *  `Number('10,000')` is `NaN`, and `JSON.stringify(NaN)` is `null` — so a config typed
 *  as "10,000" iterations went over the wire as `iterations: null`, and the server's
 *  refusal named a missing field while the input visibly held a value. Commas,
 *  underscores and spaces are grouping, not meaning; anything else unparseable is a
 *  `null` the caller must refuse *before* submitting, naming the field. */
function parseHumanNumber(text: string): number | null {
  const cleaned = text.trim().replace(/[,_\s]/g, '')
  if (cleaned === '') return null
  const value = Number(cleaned)
  return Number.isFinite(value) ? value : null
}

export function LabPanel() {
  const selectedJobId = useUi((s) => s.selectedLabJobId)
  const selectJob = useUi((s) => s.selectLabJob)
  const draftRunId = useUi((s) => s.labDraftRunId)
  const sendToLab = useUi((s) => s.sendToLab)
  const [mode, setMode] = useState<'jobs' | 'new' | 'compare'>(draftRunId != null ? 'new' : 'jobs')

  const jobs = useQuery({
    queryKey: ['lab-jobs'],
    queryFn: () => api.labJobs(),
    refetchInterval: (query) =>
      (query.state.data?.jobs ?? []).some((job) => !TERMINAL.has(job.status)) ? 2000 : 10000,
  })

  const showDetail = mode === 'jobs' && selectedJobId != null
  const narrow = showDetail || mode !== 'jobs'

  return (
    <main className="flex flex-1" style={{ minHeight: 0 }}>
      <div
        className="flex flex-col"
        style={{
          width: narrow ? 380 : '100%',
          borderRight: narrow ? '1px solid var(--border)' : undefined,
          minWidth: 0,
        }}
      >
        <div
          className="flex items-center gap-2 px-3 shrink-0"
          style={{ height: 40, borderBottom: '1px solid var(--border)' }}
        >
          <span className="pl-heading">Lab</span>
          <span className="flex-1" />
          <button type="button" className="pl-btn" onClick={() => { setMode('new') }}>
            New job
          </button>
          <button type="button" className="pl-btn" onClick={() => { setMode('compare'); selectJob(null) }}>
            Compare runs
          </button>
        </div>
        <JobList
          jobs={jobs.data?.jobs ?? []}
          loading={jobs.isLoading}
          error={jobs.isError ? String(jobs.error) : null}
          selectedId={showDetail ? selectedJobId : null}
          onSelect={(id) => {
            // Clear the Send-to-Lab draft: leaving it set makes a later remount of this
            // panel jump straight back into the new-job form, discarding the job the
            // user had open.
            if (draftRunId != null) sendToLab(null)
            setMode('jobs')
            selectJob(id)
          }}
        />
      </div>
      {mode === 'new' ? (
        <NewJobForm
          presetRunId={draftRunId}
          onDone={(jobId) => {
            setMode('jobs')
            selectJob(jobId)
          }}
          onCancel={() => setMode('jobs')}
        />
      ) : mode === 'compare' ? (
        <CompareView />
      ) : selectedJobId != null ? (
        <LabJobDetail key={selectedJobId} jobId={selectedJobId} />
      ) : null}
    </main>
  )
}

function JobList({
  jobs,
  loading,
  error,
  selectedId,
  onSelect,
}: {
  jobs: LabJob[]
  loading: boolean
  error: string | null
  selectedId: number | null
  onSelect: (id: number) => void
}) {
  if (loading)
    return (
      <div className="p-3 flex flex-col" style={{ gap: 8 }}>
        {[0, 1, 2, 3, 4].map((row) => (
          <div key={row} className="pl-skel" style={{ height: 22 }} />
        ))}
      </div>
    )
  // A failed request is not an empty list. Rendering the instructional "no jobs yet"
  // copy here would state a fact about the user's data that nothing established.
  if (error != null)
    return (
      <div className="p-3" style={{ fontSize: 12, color: 'var(--warn)' }}>
        △ Could not load Lab jobs — this is not "you have none". {error}
      </div>
    )
  if (jobs.length === 0) {
    // Instructional, not decorative (spec 10.4).
    return (
      <div className="p-4" style={{ fontSize: 12, color: 'var(--text-dim)', maxWidth: 520 }}>
        <p style={{ margin: '0 0 8px' }}>
          No Lab jobs yet. Every Lab tool operates on a <b>completed run</b> and saves its
          artefact linked to that run permanently.
        </p>
        <p style={{ margin: 0 }}>
          Press <b>New job</b>, or open a finished backtest in the Runs tab and press{' '}
          <b>Send to Lab</b>. Walk-forward re-optimises fold by fold and stitches the
          out-of-sample curve — the only number worth quoting. Monte Carlo resamples the
          run's trades and returns. Regimes buckets its performance by causally-defined
          market conditions.
        </p>
      </div>
    )
  }
  return (
    <div className="flex-1 pl-scroll" style={{ overflowY: 'auto' }}>
      <table className="pl-table" style={{ width: '100%' }}>
        <thead>
          <tr>
            <th style={{ textAlign: 'left' }}>Job</th>
            <th style={{ textAlign: 'left' }}>Run</th>
            <th style={{ textAlign: 'left' }}>Status</th>
            <th style={{ textAlign: 'right' }}>Created</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr
              key={job.id}
              onClick={() => onSelect(job.id)}
              style={{
                cursor: 'pointer',
                background: job.id === selectedId ? 'var(--surface-2)' : undefined,
              }}
            >
              <td>
                <span style={{ fontWeight: 500 }}>{TOOL_LABELS[job.tool]}</span>
                {job.label ? (
                  <span style={{ color: 'var(--text-mute)', marginLeft: 6 }}>{job.label}</span>
                ) : null}
              </td>
              <td className="mono">#{job.run_id}</td>
              <td>
                <JobStatus job={job} />
              </td>
              <td className="mono" style={{ textAlign: 'right', color: 'var(--text-mute)' }}>
                {formatTime(job.created_ms)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function JobStatus({ job }: { job: LabJob }) {
  const colour =
    job.status === 'done'
      ? 'var(--pos)'
      : job.status === 'running'
        ? 'var(--info)'
        : job.status === 'queued'
          ? 'var(--text-dim)'
          : job.status === 'cancelled'
            ? 'var(--text-mute)'
            : 'var(--down)'
  const progress =
    job.status === 'running' && job.progress_total > 0
      ? ` ${job.progress_done}/${job.progress_total}`
      : ''
  return (
    <span className="mono" style={{ fontSize: 11, color: colour }}>
      {job.status}
      {progress}
    </span>
  )
}

// ------------------------------------------------------------------------ the form

function NewJobForm({
  presetRunId,
  onDone,
  onCancel,
}: {
  presetRunId: number | null
  onDone: (jobId: number) => void
  onCancel: () => void
}) {
  const queryClient = useQueryClient()
  const notify = useUi((s) => s.notify)
  const sendToLab = useUi((s) => s.sendToLab)
  const [tool, setTool] = useState<LabTool>('walkforward')
  const [runId, setRunId] = useState<string>(presetRunId != null ? String(presetRunId) : '')
  const [label, setLabel] = useState('')
  // Walk-forward fields kept as text and parsed on submit: the server re-validates and
  // its refusal text is the authority; this form only catches the obvious.
  const [isDays, setIsDays] = useState('90')
  const [oosDays, setOosDays] = useState('30')
  const [wfMode, setWfMode] = useState<'anchored' | 'rolling'>('anchored')
  const [objective, setObjective] = useState('nbhd_median_sharpe')
  const [gridText, setGridText] = useState('{\n  "period": [10, 20, 30]\n}')
  const [iterations, setIterations] = useState('10000')
  const [seed, setSeed] = useState('0')
  const [error, setError] = useState<string | null>(null)

  const runs = useQuery({ queryKey: ['runs', 'lab-form'], queryFn: () => api.runs() })
  const completed = useMemo(
    () => (runs.data?.runs ?? []).filter((run) => run.status === 'done' && run.mode === 'backtest'),
    [runs.data],
  )

  const submit = useMutation({
    mutationFn: (body: { run_id: number; tool: LabTool; config: Record<string, unknown>; label?: string }) =>
      api.submitLabJob(body),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: ['lab-jobs'] })
      sendToLab(null)
      notify(`Lab job #${data.job.id} started`)
      onDone(data.job.id)
    },
    onError: (err) => setError(err instanceof ApiError ? err.message : String(err)),
  })

  const submitForm = () => {
    setError(null)
    const run = Number(runId)
    if (!Number.isInteger(run) || run <= 0) {
      setError('pick a completed run to operate on')
      return
    }
    let config: Record<string, unknown>
    if (tool === 'walkforward') {
      let grid: unknown
      try {
        grid = JSON.parse(gridText)
      } catch (parseError) {
        setError(`the parameter grid is not valid JSON: ${String(parseError)}`)
        return
      }
      // Refused here, naming the field and the text as typed. An unparseable number used
      // to become NaN, which JSON serialises as null — so the server's refusal named a
      // *missing* field while the input visibly read "10,000".
      const is = parseHumanNumber(isDays)
      const oos = parseHumanNumber(oosDays)
      if (is == null || is <= 0) {
        setError(`in-sample days is not a positive number: ${JSON.stringify(isDays)}`)
        return
      }
      if (oos == null || oos <= 0) {
        setError(`out-of-sample days is not a positive number: ${JSON.stringify(oosDays)}`)
        return
      }
      config = {
        is_ms: Math.round(is * DAY_MS),
        oos_ms: Math.round(oos * DAY_MS),
        mode: wfMode,
        objective,
        grid,
      }
    } else if (tool === 'montecarlo') {
      const iters = parseHumanNumber(iterations)
      const seedValue = parseHumanNumber(seed)
      if (iters == null || !Number.isInteger(iters) || iters <= 0) {
        setError(`iterations is not a positive whole number: ${JSON.stringify(iterations)}`)
        return
      }
      if (seedValue == null || !Number.isInteger(seedValue)) {
        setError(`seed is not a whole number: ${JSON.stringify(seed)}`)
        return
      }
      config = { iterations: iters, seed: seedValue }
    } else {
      config = {}
    }
    submit.mutate({ run_id: run, tool, config, label: label || undefined })
  }

  return (
    <section className="flex-1 p-4 pl-scroll" style={{ overflowY: 'auto', minWidth: 0 }}>
      <div className="pl-panel" style={{ maxWidth: 640, width: '100%', padding: 16 }}>
        <h2 className="pl-heading" style={{ margin: '0 0 16px' }}>New Lab job</h2>

        <FormRow label="Run" help="Lab tools operate on completed backtests (spec 9).">
          {runs.isLoading ? (
            <div className="pl-skel" style={{ height: 28 }} />
          ) : (
            <select className="pl-input" value={runId} onChange={(e) => setRunId(e.target.value)}>
              <option value="">— pick a completed run —</option>
              {completed.map((run: Run) => (
                <option key={run.id} value={run.id}>
                  #{run.id} · {run.strategy_name} v{run.version_no} · {run.symbols.join(', ')}
                  {run.label ? ` · ${run.label}` : ''}
                </option>
              ))}
            </select>
          )}
        </FormRow>

        <FormRow label="Tool" help="">
          <div className="flex gap-1">
            {(Object.keys(TOOL_LABELS) as LabTool[]).map((name) => (
              <button
                key={name}
                type="button"
                className="pl-btn"
                onClick={() => setTool(name)}
                style={{
                  background: tool === name ? 'var(--surface-3)' : 'transparent',
                  borderColor: tool === name ? 'var(--border-strong)' : 'var(--border)',
                  color: tool === name ? 'var(--text)' : 'var(--text-dim)',
                  fontWeight: tool === name ? 550 : 450,
                }}
              >
                {TOOL_LABELS[name]}
              </button>
            ))}
          </div>
        </FormRow>

        {tool === 'walkforward' ? (
          <>
            <div className="flex gap-3">
              <FormRow label="In-sample (days)" help="Optimisation window per fold.">
                <input className="pl-input mono" value={isDays} onChange={(e) => setIsDays(e.target.value)} style={{ width: 90 }} />
              </FormRow>
              <FormRow label="Out-of-sample (days)" help="Evaluation window; folds tile the range with it.">
                <input className="pl-input mono" value={oosDays} onChange={(e) => setOosDays(e.target.value)} style={{ width: 90 }} />
              </FormRow>
              <FormRow label="Mode" help="Anchored grows the IS window; rolling slides it.">
                <select className="pl-input" value={wfMode} onChange={(e) => setWfMode(e.target.value as 'anchored' | 'rolling')}>
                  <option value="anchored">anchored</option>
                  <option value="rolling">rolling</option>
                </select>
              </FormRow>
            </div>
            <FormRow
              label="Objective"
              help="The default selects plateaus over spikes, which is the anti-overfitting property you want (spec 9.1)."
            >
              <select className="pl-input" value={objective} onChange={(e) => setObjective(e.target.value)}>
                <option value="nbhd_median_sharpe">neighbourhood-median Sharpe (default)</option>
                <option value="max_sharpe">max Sharpe (overfit-prone)</option>
              </select>
            </FormRow>
            <FormRow
              label="Parameter grid"
              help='JSON object of parameter name to values, e.g. {"fast": [5, 10], "slow": [50, 100]}. Every combination is evaluated per fold and counted as a trial (spec 8.5).'
            >
              <textarea
                className="pl-input mono"
                value={gridText}
                onChange={(e) => setGridText(e.target.value)}
                rows={5}
                style={{ width: '100%', height: 'auto', minHeight: 104, padding: '6px 8px', lineHeight: 1.5, resize: 'vertical' }}
              />
            </FormRow>
          </>
        ) : tool === 'montecarlo' ? (
          <div className="flex gap-3">
            <FormRow label="Iterations" help="Default 10 000 (spec 9.2).">
              <input className="pl-input mono" value={iterations} onChange={(e) => setIterations(e.target.value)} style={{ width: 110 }} />
            </FormRow>
            <FormRow label="Seed" help="Same seed, same draws — the artefact is reproducible.">
              <input className="pl-input mono" value={seed} onChange={(e) => setSeed(e.target.value)} style={{ width: 90 }} />
            </FormRow>
          </div>
        ) : (
          <p style={{ fontSize: 12, color: 'var(--text-dim)' }}>
            Regime definitions are causal by construction — expanding-window volatility
            tertiles, ADX(14) against 25 on the daily bar, trailing funding mean, and
            liquidation-cluster windows — with the defaults from spec 9.3. Periods the
            history cannot label are counted as <i>unclassified</i>, never guessed.
          </p>
        )}

        <FormRow label="Label" help="Optional, shows in the job list.">
          <input className="pl-input" value={label} onChange={(e) => setLabel(e.target.value)} style={{ width: 260 }} />
        </FormRow>

        {error ? (
          <p className="mono" style={{ fontSize: 12, color: 'var(--down)', whiteSpace: 'pre-wrap' }}>{error}</p>
        ) : null}

        <div className="flex gap-2" style={{ marginTop: 16, paddingTop: 14, borderTop: '1px solid var(--border)' }}>
          <button type="button" className="pl-btn pl-btn-primary" onClick={submitForm} disabled={submit.isPending}>
            {submit.isPending ? 'Submitting…' : 'Run'}
          </button>
          <button type="button" className="pl-btn" onClick={() => { sendToLab(null); onCancel() }}>
            Cancel
          </button>
        </div>
      </div>
    </section>
  )
}

function FormRow({ label, help, children }: { label: string; help: string; children: React.ReactNode }) {
  return (
    <div style={{ margin: '0 0 14px' }}>
      <div className="pl-card-label" style={{ marginBottom: 5 }}>{label}</div>
      {children}
      {help ? <div style={{ fontSize: 11, color: 'var(--text-mute)', marginTop: 4, lineHeight: 1.5 }}>{help}</div> : null}
    </div>
  )
}
