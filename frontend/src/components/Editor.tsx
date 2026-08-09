import { useCallback, useEffect, useRef, useState } from 'react'
import Editor, { type Monaco, type OnMount } from '@monaco-editor/react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { editor } from 'monaco-editor'
import { api, type Strategy, type StrategyVersion, type ValidationResult } from '../api'
import { useUi } from '../store'
import { DiagnosticsPanel, ParamsPreview } from './Diagnostics'
import { VersionDrawer } from './VersionDrawer'

/**
 * The full-height Monaco editor with inline gutter diagnostics (spec 10.3).
 *
 * Three behaviours to know about:
 *
 * **Diagnostics come from the server, always.** Monaco's own Python support is a syntax
 * highlighter; every rule that matters here — look-ahead, determinism, the smoke run — is
 * enforced by `strategy.validate` in Python. Duplicating any of it in TypeScript would give
 * two answers to the same question, and the browser's would be the one that is wrong.
 *
 * **Two validation paths, deliberately.** Typing triggers a debounced `quick` validation
 * (static stages only, no subprocess). ⌘S runs the full pipeline including the smoke run
 * and the determinism probe, then saves. Running the smoke run per keystroke would spawn
 * two interpreters every 400 ms.
 *
 * **Save is never blocked by errors.** Spec 5.6 wants a version row per save; refusing to
 * store broken code loses work when the author has to stop mid-edit. The version records
 * that it did not validate.
 */
export function StrategyEditor({ strategyId }: { strategyId: number }) {
  const queryClient = useQueryClient()
  const { notify, versionsOpen, setVersionsOpen, openRunDraft } = useUi()
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<Monaco | null>(null)
  const [code, setCode] = useState<string | null>(null)
  const [baseline, setBaseline] = useState('')
  const [result, setResult] = useState<ValidationResult | null>(null)
  const [pane, setPane] = useState<'diagnostics' | 'params'>('diagnostics')
  const [confirmDelete, setConfirmDelete] = useState(false)
  const theme = useUi((s) => s.theme)

  const detail = useQuery({
    queryKey: ['strategy', strategyId],
    queryFn: () => api.get(strategyId),
  })
  const strategy = detail.data?.strategy

  // Load the head version into the editor when the selection changes. Keyed on the version
  // id rather than on the strategy id so that saving (which mints a new version) does not
  // clobber what is in the buffer, while switching strategies does replace it.
  const headVersionId = strategy?.head?.id ?? null
  const loadedRef = useRef<number | null>(null)
  useEffect(() => {
    if (!strategy?.head?.code || headVersionId == null) return
    if (loadedRef.current === headVersionId) return
    loadedRef.current = headVersionId
    setCode(strategy.head.code)
    setBaseline(strategy.head.code)
    setResult(
      strategy.head.diagnostics.length || strategy.head.params.length
        ? {
            ok: strategy.head.valid,
            diagnostics: strategy.head.diagnostics,
            class_name: strategy.head.class_name,
            params: strategy.head.params,
            requires: strategy.head.requires,
            hooks: [],
            warmup: null,
            indicator_warmup: null,
            bars: null,
            orders: null,
            event_hash: null,
            stdout: '',
          }
        : null,
    )
  }, [strategy, headVersionId])

  // Monotonic ticket for quick validations. The debounce below cannot serialise the
  // *responses*: two keystroke bursts 600 ms apart issue two requests, and if the first
  // (about older code) lands after the second, its diagnostics overwrite the newer ones —
  // the panel then describes a buffer that no longer exists until the next keystroke.
  // Each request carries the ticket it was issued under; a response whose ticket is no
  // longer current is dropped on the floor.
  const validateSeq = useRef(0)

  const quick = useMutation({
    mutationFn: ({ source }: { source: string; seq: number }) => api.validate(source, true),
    onSuccess: (validation, { seq }) => {
      if (seq !== validateSeq.current) return
      // Merge rather than replace: the quick pass runs the two static stages only, so
      // overwriting a full result with it would erase the smoke-run findings and make the
      // panel claim a strategy is clean when the last real run said otherwise.
      //
      // But `ok` must be **recomputed from the merged diagnostics**, not inherited. Spreading
      // `...previous` carried the last full pass's `ok: true` over a merge that had just
      // added a syntax error, so the panel rendered a green "✓ valid" header directly above
      // the error it was listing — the same failure the merge exists to prevent, pointing
      // the other way.
      setResult((previous) => {
        if (!previous) return validation
        const diagnostics = mergeDiagnostics(previous, validation)
        return {
          ...previous,
          diagnostics,
          ok: !diagnostics.some((d) => d.severity === 'error'),
        }
      })
    },
  })

  const save = useMutation({
    mutationFn: ({ source, message }: { source: string; message: string }) =>
      api.save(strategyId, source, message),
    onSuccess: (response) => {
      // A full save's result is authoritative — retire every quick pass still in flight
      // so a stale static-only response cannot land on top of it.
      validateSeq.current += 1
      setResult(response.validation)
      setBaseline(response.version.code ?? code ?? '')
      loadedRef.current = response.version.id
      queryClient.invalidateQueries({ queryKey: ['strategies'] })
      queryClient.invalidateQueries({ queryKey: ['strategy', strategyId] })
      queryClient.invalidateQueries({ queryKey: ['versions', strategyId] })
      if (!response.created) notify('No changes — still on v' + response.version.version_no, 'warn')
      else if (response.validation.ok) notify(`Saved v${response.version.version_no}`)
      else notify(`Saved v${response.version.version_no} with validation errors`, 'warn')
    },
    onError: (error: Error) => notify(error.message, 'error'),
  })

  const archive = useMutation({
    mutationFn: (archived: boolean) => api.archive(strategyId, archived),
    onSuccess: (response) => {
      queryClient.invalidateQueries({ queryKey: ['strategies'] })
      queryClient.invalidateQueries({ queryKey: ['strategy', strategyId] })
      notify(response.strategy.archived ? 'Archived' : 'Restored')
    },
    onError: (error: Error) => notify(error.message, 'error'),
  })

  const remove = useMutation({
    mutationFn: (name: string) => api.remove(strategyId, name),
    onSuccess: () => {
      setConfirmDelete(false)
      queryClient.invalidateQueries({ queryKey: ['strategies'] })
      useUi.getState().select(null)
      notify('Deleted')
    },
    onError: (error: Error) => notify(error.message, 'error'),
  })

  const dirty = code !== null && code !== baseline

  const doSave = useCallback(() => {
    const source = editorRef.current?.getValue() ?? code
    if (source == null) return
    save.mutate({ source, message: '' })
  }, [code, save])

  // The ref dance exists because Monaco binds its command handler once, at mount, and would
  // otherwise close over the first render's `doSave` — saving whatever the buffer held when
  // the editor was created, forever.
  const saveRef = useRef(doSave)
  saveRef.current = doSave

  const onMount: OnMount = (instance, monaco) => {
    editorRef.current = instance
    monacoRef.current = monaco
    instance.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => saveRef.current())
  }

  // Push diagnostics into the gutter. Monaco owns markers per model+owner, so setting the
  // full list each time replaces the previous one — no accumulation, no stale underline.
  useEffect(() => {
    const monaco = monacoRef.current
    const model = editorRef.current?.getModel()
    if (!monaco || !model) return
    monaco.editor.setModelMarkers(
      model,
      'perplab',
      (result?.diagnostics ?? []).map((diagnostic) => ({
        severity:
          diagnostic.severity === 'error'
            ? monaco.MarkerSeverity.Error
            : diagnostic.severity === 'warning'
              ? monaco.MarkerSeverity.Warning
              : monaco.MarkerSeverity.Info,
        message: `[${diagnostic.stage}/${diagnostic.code}] ${diagnostic.message}`,
        startLineNumber: diagnostic.line,
        startColumn: diagnostic.column,
        endLineNumber: diagnostic.end_line,
        endColumn: diagnostic.end_column,
      })),
    )
  }, [result])

  // Debounced quick validation. 500 ms is long enough that a burst of typing produces one
  // request, short enough that a pause feels answered. The ticket is taken when the
  // request actually fires, so every earlier in-flight response is stale from that moment.
  useEffect(() => {
    if (code == null) return
    const source = code
    const timer = window.setTimeout(() => {
      validateSeq.current += 1
      quick.mutate({ source, seq: validateSeq.current })
    }, 500)
    return () => window.clearTimeout(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [code])

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {
        event.preventDefault()
        saveRef.current()
      }
      if (event.key === 'Escape') setConfirmDelete(false)
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  if (detail.isLoading) return <Centre>Loading…</Centre>
  if (detail.isError) return <Centre kind="error">{(detail.error as Error).message}</Centre>
  if (!strategy) return <Centre>Not found.</Centre>

  return (
    <div className="flex flex-1" style={{ minWidth: 0 }}>
      <div className="flex flex-col flex-1" style={{ minWidth: 0 }}>
        <div
          className="flex items-center gap-2 px-2 shrink-0"
          style={{ height: 36, borderBottom: '1px solid var(--border)', background: 'var(--surface)' }}
        >
          <span style={{ fontWeight: 500 }}>{strategy.name}</span>
          <span className="mono pl-tag">v{strategy.head?.version_no ?? 0}</span>
          {dirty && (
            <span className="mono" style={{ fontSize: 11, color: 'var(--warn)' }}>
              ● unsaved
            </span>
          )}
          <span className="flex-1" />
          <button
            className="pl-btn pl-btn-primary"
            onClick={doSave}
            disabled={save.isPending || code == null}
            title="Validate and save (⌘S)"
          >
            {save.isPending ? 'Validating…' : 'Save'}
          </button>
          <button
            className="pl-btn"
            onClick={() => openRunDraft(strategyId)}
            disabled={dirty || !strategy.head?.valid}
            title={
              dirty
                ? 'Save first — a run records the version it executed, and an unsaved buffer has no version to record.'
                : strategy.head?.valid
                  ? 'Backtest this version'
                  : 'This version did not pass validation, so it cannot be backtested.'
            }
          >
            Backtest
          </button>
          <button className="pl-btn" onClick={() => setVersionsOpen(!versionsOpen)}>
            History
          </button>
          <a className="pl-btn" href={api.exportUrl(strategyId)} download>
            Export
          </a>
          <button className="pl-btn" onClick={() => archive.mutate(!strategy.archived)}>
            {strategy.archived ? 'Restore' : 'Archive'}
          </button>
          <button className="pl-btn pl-btn-danger" onClick={() => setConfirmDelete(true)}>
            Delete
          </button>
        </div>

        <div className="flex-1" style={{ minHeight: 0 }}>
          <Editor
            height="100%"
            language="python"
            theme={theme === 'dark' ? 'vs-dark' : 'vs'}
            value={code ?? ''}
            onChange={(value) => setCode(value ?? '')}
            onMount={onMount}
            options={{
              fontFamily: "'JetBrains Mono', 'Cascadia Mono', Consolas, monospace",
              fontSize: 13,
              minimap: { enabled: false },
              scrollBeyondLastLine: false,
              renderWhitespace: 'selection',
              tabSize: 4,
              insertSpaces: true,
              rulers: [88],
              automaticLayout: true,
              lineNumbersMinChars: 3,
              padding: { top: 8 },
            }}
          />
        </div>

        <div
          className="flex flex-col shrink-0"
          style={{ height: 220, borderTop: '1px solid var(--border)', background: 'var(--surface)' }}
        >
          <div className="flex shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
            {(['diagnostics', 'params'] as const).map((name) => (
              <button
                key={name}
                onClick={() => setPane(name)}
                className="px-3"
                style={{
                  height: 26,
                  fontSize: 11,
                  textTransform: 'uppercase',
                  letterSpacing: '0.06em',
                  color: pane === name ? 'var(--text)' : 'var(--text-mute)',
                  background: pane === name ? 'var(--surface-2)' : 'transparent',
                  borderRight: '1px solid var(--border)',
                  cursor: 'pointer',
                }}
              >
                {name === 'params' ? 'Params preview' : 'Validation'}
              </button>
            ))}
          </div>
          <div className="flex-1" style={{ minHeight: 0 }}>
            {pane === 'diagnostics' ? (
              <DiagnosticsPanel
                result={result}
                running={save.isPending || quick.isPending}
                onJump={(line, column) => {
                  editorRef.current?.revealLineInCenter(line)
                  editorRef.current?.setPosition({ lineNumber: line, column })
                  editorRef.current?.focus()
                }}
              />
            ) : (
              <ParamsPreview
                params={result?.params ?? strategy.head?.params ?? []}
                requires={result?.requires ?? strategy.head?.requires ?? null}
              />
            )}
          </div>
        </div>
      </div>

      {versionsOpen && (
        <VersionDrawer
          strategyId={strategyId}
          onClose={() => setVersionsOpen(false)}
          onLoadVersion={(version: StrategyVersion) => {
            if (version.code == null) return
            setCode(version.code)
            notify(`Loaded v${version.version_no} into the editor — not saved yet`, 'warn')
          }}
        />
      )}

      {confirmDelete && (
        <DeleteModal
          strategy={strategy}
          pending={remove.isPending}
          onCancel={() => setConfirmDelete(false)}
          onConfirm={(name) => remove.mutate(name)}
        />
      )}
    </div>
  )
}

/**
 * Typed-name confirmation (spec 10.4: destructive actions require confirmation).
 *
 * The name is typed rather than clicked because the server requires it too — the check
 * exists in both places and neither is decoration. The dialog stops the accidental click;
 * the server check stops the mistaken API call.
 */
function DeleteModal({
  strategy,
  pending,
  onCancel,
  onConfirm,
}: {
  strategy: Strategy
  pending: boolean
  onCancel: () => void
  onConfirm: (name: string) => void
}) {
  const [typed, setTyped] = useState('')
  return (
    <div
      className="pl-scrim"
      style={{ alignItems: 'center' }}
      onClick={onCancel}
    >
      <form
        className="pl-modal p-4 flex flex-col gap-3"
        style={{ width: 'min(440px, calc(100vw - 32px))' }}
        onClick={(event) => event.stopPropagation()}
        onSubmit={(event) => {
          event.preventDefault()
          onConfirm(typed)
        }}
      >
        <h2 style={{ margin: 0, fontSize: 14 }}>Delete {strategy.name}?</h2>
        <p style={{ margin: 0, fontSize: 12, color: 'var(--text-dim)' }}>
          This removes the strategy and all {strategy.version_count} of its versions,
          permanently. It is refused while any non-archived run references it, because those
          runs would stop being reproducible.
        </p>
        <p style={{ margin: 0, fontSize: 12, color: 'var(--text-dim)' }}>
          <b>Archive</b> is the recoverable alternative: it hides the strategy and excludes
          it from the trials counter without destroying anything.
        </p>
        <label style={{ fontSize: 12 }}>
          Type <span className="mono">{strategy.name}</span> to confirm
          <input
            className="pl-input mono mt-1"
            autoFocus
            value={typed}
            onChange={(event) => setTyped(event.target.value)}
          />
        </label>
        <div className="flex gap-2 justify-end">
          <button className="pl-btn" type="button" onClick={onCancel}>
            Cancel
          </button>
          <button
            className="pl-btn pl-btn-danger"
            type="submit"
            disabled={typed !== strategy.name || pending}
          >
            Delete permanently
          </button>
        </div>
      </form>
    </div>
  )
}

/** Keep smoke-run findings visible while the quick pass refreshes the static ones. */
function mergeDiagnostics(previous: ValidationResult, quick: ValidationResult) {
  const staticStages = new Set(['parse', 'scan'])
  const kept = previous.diagnostics.filter((d) => !staticStages.has(d.stage))
  // If the static pass now reports an error, the older smoke-run findings describe code
  // that no longer exists — drop them rather than showing two versions' problems at once.
  if (quick.diagnostics.some((d) => d.severity === 'error')) return quick.diagnostics
  return [...quick.diagnostics, ...kept]
}

function Centre({ children, kind }: { children: React.ReactNode; kind?: 'error' }) {
  return (
    <div
      className="flex-1 flex items-center justify-center"
      style={{ fontSize: 12, color: kind === 'error' ? 'var(--down)' : 'var(--text-mute)' }}
    >
      {children}
    </div>
  )
}
