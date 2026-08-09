import type { Diagnostic, ParamSpec, Requires, ValidationResult } from '../api'

const GLYPH: Record<string, string> = { error: '✕', warning: '△', info: 'i' }

/**
 * The validation panel beneath the editor (spec 10.3).
 *
 * Every diagnostic shows severity as a **glyph and** a colour, and carries the stage that
 * produced it. The stage is not decoration: "this failed at `scan`" means the rule is
 * static and the fix is textual, while "this failed at `smoke`" means the code ran and
 * broke, and the two need different reactions from the reader.
 */
export function DiagnosticsPanel({
  result,
  running,
  onJump,
}: {
  result: ValidationResult | null
  running: boolean
  onJump: (line: number, column: number) => void
}) {
  if (running && !result) {
    return (
      <div className="flex flex-col" style={{ height: '100%' }}>
        <div
          className="flex items-center gap-2 px-2 shrink-0"
          style={{ height: 28, borderBottom: '1px solid var(--border)' }}
        >
          <span className="pl-spin" style={{ color: 'var(--text-mute)' }}>◐</span>
          <span className="pl-heading">Validating…</span>
        </div>
        <div className="p-2" aria-hidden={true}>
          {[0, 1, 2].map((i) => (
            <div
              key={i}
              className="pl-skel"
              style={{ height: 12, marginBottom: 8, width: `${78 - i * 14}%` }}
            />
          ))}
        </div>
      </div>
    )
  }
  if (!result) {
    return (
      <Shell>
        Press <kbd className="pl-kbd">⌘S</kbd> to validate and save.
      </Shell>
    )
  }

  const { diagnostics } = result
  const errors = diagnostics.filter((d) => d.severity === 'error').length
  const warnings = diagnostics.filter((d) => d.severity === 'warning').length

  return (
    <div className="flex flex-col" style={{ height: '100%' }}>
      <div
        className="flex items-center gap-3 px-2 shrink-0"
        style={{ height: 28, borderBottom: '1px solid var(--border)', fontSize: 11 }}
      >
        <span className={result.ok ? 'mono' : 'mono sev-error'} style={result.ok ? { color: 'var(--pos)' } : undefined}>
          {result.ok ? '✓ valid' : `✕ ${errors} error${errors === 1 ? '' : 's'}`}
        </span>
        {warnings > 0 && <span className="mono sev-warning">△ {warnings}</span>}
        {running && <span className="pl-spin" style={{ color: 'var(--text-mute)' }}>◐</span>}
        <span className="flex-1" />
        {result.class_name && (
          <span className="mono" style={{ color: 'var(--text-mute)' }}>
            {result.class_name}
          </span>
        )}
        {result.warmup != null && (
          <span
            className="mono"
            style={{ color: 'var(--text-mute)' }}
            title="warm-up bars used by the smoke run / derived from the indicator set"
          >
            warmup {result.warmup}
            {result.indicator_warmup != null && result.indicator_warmup !== result.warmup
              ? ` (indicators ${result.indicator_warmup})`
              : ''}
          </span>
        )}
        {result.orders != null && (
          <span className="mono" style={{ color: 'var(--text-mute)' }} title="orders placed on synthetic data">
            {result.orders} orders / {result.bars} bars
          </span>
        )}
        {result.event_hash && (
          <span
            className="mono"
            style={{ color: 'var(--text-mute)' }}
            title="event-log hash; identical across two runs with different PYTHONHASHSEED"
          >
            {result.event_hash.slice(0, 10)}
          </span>
        )}
      </div>

      <div className="pl-scroll flex-1">
        {diagnostics.length === 0 && (
          <p className="p-2" style={{ fontSize: 12, color: 'var(--text-mute)' }}>
            {/* `bars` is null when this result was reconstructed from a stored version
                rather than produced by a run just now. Saying "ran 0 synthetic bars" there
                would describe a smoke run that did not happen on this page load. */}
            {result.bars == null ? (
              <>No findings recorded for this version.</>
            ) : (
              <>
                No findings. Parsed, scanned, ran {result.bars} synthetic bars, and produced
                the same event log twice under different hash seeds.
              </>
            )}
          </p>
        )}
        {diagnostics.map((diagnostic, index) => (
          <Item key={index} diagnostic={diagnostic} onJump={onJump} />
        ))}
        {result.stdout.trim() && (
          <details className="px-2 py-1" style={{ borderTop: '1px solid var(--border)' }}>
            <summary className="pl-heading" style={{ cursor: 'pointer', lineHeight: '20px' }}>
              Output printed by the strategy
            </summary>
            <pre
              className="mono pl-scroll"
              style={{
                fontSize: 11,
                maxHeight: 160,
                color: 'var(--text-dim)',
                margin: '6px 0 4px',
                padding: 8,
                background: 'var(--surface-2)',
                border: '1px solid var(--border)',
                borderRadius: 'var(--radius-sm)',
              }}
            >
              {result.stdout}
            </pre>
          </details>
        )}
      </div>
    </div>
  )
}

function Item({
  diagnostic,
  onJump,
}: {
  diagnostic: Diagnostic
  onJump: (line: number, column: number) => void
}) {
  return (
    <button
      className="w-full text-left flex gap-2 px-2 py-1"
      style={{ borderBottom: '1px solid var(--border)', cursor: 'pointer' }}
      onClick={() => onJump(diagnostic.line, diagnostic.column)}
    >
      <span className={`sev-${diagnostic.severity} mono`} style={{ width: 12 }}>
        {GLYPH[diagnostic.severity] ?? '•'}
      </span>
      <span className="mono shrink-0" style={{ fontSize: 11, color: 'var(--text-mute)', width: 52 }}>
        L{diagnostic.line}
      </span>
      <span className="pl-tag shrink-0" style={{ width: 78, textAlign: 'center' }}>
        {diagnostic.stage}
      </span>
      <span style={{ fontSize: 12, whiteSpace: 'pre-wrap' }}>{diagnostic.message}</span>
    </button>
  )
}

function Shell({ children }: { children: React.ReactNode }) {
  return (
    <p className="p-2" style={{ fontSize: 12, color: 'var(--text-mute)' }}>
      {children}
    </p>
  )
}

/**
 * The auto-generated config form (spec 10.3, "params preview").
 *
 * Read-only in Phase 3. It exists so the author can see what their `params` declaration
 * produces without starting a backtest — the whole point of declaring params is that the
 * form is generated, and a declaration that renders badly is worth catching while the code
 * is open. It becomes editable in Phase 4, where the values it collects have a run to
 * configure.
 */
export function ParamsPreview({
  params,
  requires,
}: {
  params: ParamSpec[]
  requires: Requires | null
}) {
  return (
    <div className="pl-scroll" style={{ height: '100%' }}>
      <Section title="Requires">
        {requires ? (
          <dl className="grid gap-x-3 gap-y-1 m-0" style={{ gridTemplateColumns: 'auto 1fr', fontSize: 12 }}>
            <Field label="symbols" value={requires.symbols.join(', ')} />
            <Field label="timeframe" value={requires.timeframe} />
            <Field label="history" value={`${requires.history} bars`} />
            <Field label="datasets" value={requires.datasets.join(', ')} />
          </dl>
        ) : (
          <Muted>Not declared.</Muted>
        )}
      </Section>

      <Section title={`Params (${params.length})`}>
        {params.length === 0 && <Muted>None declared. The config form would be empty.</Muted>}
        <div className="flex flex-col gap-2">
          {params.map((param) => (
            <div key={param.name} className="flex flex-col gap-0.5">
              <label className="flex items-baseline gap-1.5" style={{ fontSize: 12 }}>
                <span style={{ fontWeight: 500 }}>{param.label ?? param.name}</span>
                <span className="pl-tag">{param.type}</span>
              </label>
              {param.type === 'bool' ? (
                <input type="checkbox" checked={Boolean(param.default)} readOnly disabled />
              ) : param.type === 'choice' ? (
                <select className="pl-input" value={String(param.default)} disabled>
                  {(param.choices ?? []).map((choice) => (
                    <option key={choice}>{choice}</option>
                  ))}
                </select>
              ) : (
                <input
                  className="pl-input mono"
                  value={String(param.default)}
                  readOnly
                  disabled
                />
              )}
              <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
                {param.min !== undefined || param.max !== undefined
                  ? `range ${param.min ?? '−∞'} … ${param.max ?? '∞'}`
                  : 'unbounded'}
              </span>
              {param.help && (
                <span style={{ fontSize: 11, color: 'var(--text-dim)' }}>{param.help}</span>
              )}
            </div>
          ))}
        </div>
        <p className="mt-3" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
          Read-only preview. The form becomes editable when there is a run to configure
          (Phase 4).
        </p>
      </Section>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="p-3" style={{ borderBottom: '1px solid var(--border)' }}>
      <h3 className="pl-heading" style={{ margin: '0 0 8px' }}>
        {title}
      </h3>
      {children}
    </section>
  )
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <>
      <dt style={{ color: 'var(--text-mute)' }}>{label}</dt>
      <dd className="mono m-0">{value}</dd>
    </>
  )
}

function Muted({ children }: { children: React.ReactNode }) {
  return <p style={{ fontSize: 12, color: 'var(--text-mute)', margin: 0 }}>{children}</p>
}
