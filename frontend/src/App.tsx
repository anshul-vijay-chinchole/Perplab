import { useCallback, useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from './api'
import { Chrome, Toast } from './components/Chrome'
import { ErrorBoundary } from './components/ErrorBoundary'
import { StrategyEditor } from './components/Editor'
import { DataAndFeed } from './components/DataTab'
import { LabPanel } from './components/Lab'
import { CommandPalette } from './components/Palette'
import { DashboardPanel } from './components/Dashboard'
import { SettingsPanel } from './components/Settings'
import { RunDetail } from './components/RunDetail'
import { NewRunDialog, RunList } from './components/RunList'
import { StartSessionDialog } from './components/SessionDialog'
import { StrategyLibrary, StrategyList } from './components/StrategyList'
import { RUNS_PANE_DETAIL_MIN, RUNS_PANE_MIN, useUi } from './store'

export function App() {
  const tab = useUi((s) => s.tab)
  const selectedId = useUi((s) => s.selectedId)
  const selectedRunId = useUi((s) => s.selectedRunId)
  const client = useQueryClient()
  // While errored, keep asking. `retry: 0` with no refetch meant opening the tab half a
  // second before `perplab serve` bound its port left the offline card up permanently —
  // one failed request, frozen forever. The interval only runs in the error state, so a
  // healthy instance is not polled.
  const health = useQuery({
    queryKey: ['health'],
    queryFn: api.health,
    retry: 0,
    refetchInterval: (query) => (query.state.status === 'error' ? 3000 : false),
  })

  return (
    <div className="flex flex-col" style={{ height: '100%' }}>
      <Chrome tab={tab} />
      {/* `key={tab}` restarts the entrance fade on every tab switch — a new page feels
          placed rather than swapped. The animation is 160ms and opacity-only, so it is
          felt, not watched. */}
      {/* The boundary sits *inside* the chrome, keyed by tab. A render fault in page
          content must cost that page and nothing above it — the root boundary's fallback
          replaces <Chrome/> too, which removes the KILL button at exactly the moment an
          operator may need it. Resetting evicts the query cache so Try Again is a real
          retry rather than a re-throw of the cached payload (see ErrorBoundary.onReset). */}
      <div key={tab} className="pl-page flex flex-col flex-1" style={{ minHeight: 0 }}>
        <ErrorBoundary
          what={`The ${tab} tab`}
          resetKey={tab}
          note="The chrome above still works: the KILL switch, the tabs and the session badge are unaffected."
          onReset={() => client.clear()}
        >
        {health.isError ? (
          <Offline message={(health.error as Error).message} onRetry={() => health.refetch()} retrying={health.isFetching} />
        ) : tab === 'Data & Feed' ? (
          <DataAndFeed />
        ) : tab === 'Dashboard' ? (
          <DashboardPanel />
        ) : tab === 'Settings' ? (
          <SettingsPanel />
        ) : tab === 'Lab' ? (
          <LabPanel />
        ) : tab === 'Runs' ? (
          <RunsTab selectedRunId={selectedRunId} />
        ) : selectedId == null ? (
          /* No strategy open: the full-width library grid. Opening one swaps to the dense
             sidebar + editor, so switching strategies never costs the editor. */
          <main className="flex flex-1" style={{ minHeight: 0 }}>
            <StrategyLibrary />
          </main>
        ) : (
          <main className="flex flex-1" style={{ minHeight: 0 }}>
            <StrategyList />
            <StrategyEditor key={selectedId} strategyId={selectedId} />
          </main>
        )}
        </ErrorBoundary>
      </div>
      <NewRunDialog />
      <StartSessionDialog />
      <CommandPalette />
      <Toast />
      <footer
        className="mono flex items-center gap-3 px-2 shrink-0"
        style={{
          height: 24,
          borderTop: '1px solid var(--border)',
          background: 'var(--surface)',
          fontSize: 10.5,
          color: 'var(--text-mute)',
        }}
      >
        <span>PerpLab {health.data?.version ?? '—'}</span>
        <span>Phase 7 · Paper trading</span>
        <span className="flex-1" />
        <span className="flex items-center gap-1"><span className="pl-kbd">⌘K</span> jump</span>
        <span className="flex items-center gap-1"><span className="pl-kbd">⌘S</span> save</span>
      </footer>
    </div>
  )
}

/** Keyboard nudge per arrow press, in px. Large enough to be worth pressing, small enough
 *  that holding the key still feels like a drag rather than a jump. */
const RUNS_PANE_STEP = 16

function RunsTab({ selectedRunId }: { selectedRunId: number | null }) {
  const width = useUi((s) => s.runsPaneWidth)
  const setWidth = useUi((s) => s.setRunsPaneWidth)
  const hostRef = useRef<HTMLElement | null>(null)
  const paneRef = useRef<HTMLDivElement | null>(null)
  const [dragging, setDragging] = useState(false)
  const [host, setHost] = useState(0)
  const [paneNow, setPaneNow] = useState(0)

  // The upper bound depends on the window, so it has to be measured rather than assumed.
  // Without this, a width dragged wide on a large monitor would leave the detail pane a
  // sliver after the window shrank — the stored number is only meaningful next to a size.
  //
  // The pane is observed too, and only so the separator can report `aria-valuenow`
  // honestly: reading `paneRef.current` during render returns the width from the *previous*
  // render, so a screen reader would trail the drag by one frame and land on the pre-drag
  // number when it stopped.
  useEffect(() => {
    const el = hostRef.current
    const pane = paneRef.current
    if (el == null || pane == null) return
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (entry.target === el) setHost(entry.contentRect.width)
        else setPaneNow(entry.contentRect.width)
      }
    })
    observer.observe(el)
    observer.observe(pane)
    setHost(el.getBoundingClientRect().width)
    setPaneNow(pane.getBoundingClientRect().width)
    return () => observer.disconnect()
  }, [])

  const max = Math.max(RUNS_PANE_MIN, host - RUNS_PANE_DETAIL_MIN)
  const clamp = useCallback(
    (px: number) => Math.min(Math.max(px, RUNS_PANE_MIN), max),
    [max],
  )

  const onPointerDown = (event: React.PointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return
    const pane = paneRef.current
    const el = hostRef.current
    if (pane == null || el == null) return
    event.preventDefault()
    try {
      event.currentTarget.setPointerCapture(event.pointerId)
    } catch {
      // Capture is an optimisation, not the mechanism: the move and up listeners are on
      // `window`, so the drag works without it. Throwing here would abort a drag over a
      // pointer id the browser has already released.
    }
    const origin = event.clientX
    const start = pane.getBoundingClientRect().width
    setDragging(true)

    const move = (e: PointerEvent) => {
      const next = clamp(start + (e.clientX - origin))
      setWidth(next)
      // Kept in step here as well as from the ResizeObserver. The observer delivers on the
      // rendering lifecycle, so during a fast drag `aria-valuenow` would trail the pane by a
      // frame; this is the value we just asked for, so it cannot.
      setPaneNow(next)
    }
    const end = () => {
      setDragging(false)
      window.removeEventListener('pointermove', move)
      window.removeEventListener('pointerup', end)
      window.removeEventListener('pointercancel', end)
    }
    // On `window`, not the handle: pointer capture keeps events flowing to the handle, but
    // listening on the window as well means a capture lost to a devtools break or an alt-tab
    // still ends the drag instead of leaving the pane glued to the cursor.
    window.addEventListener('pointermove', move)
    window.addEventListener('pointerup', end)
    window.addEventListener('pointercancel', end)
  }

  const onKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    const current = paneRef.current?.getBoundingClientRect().width ?? RUNS_PANE_MIN
    const step = (delta: number) => {
      const next = clamp(current + delta)
      setWidth(next)
      setPaneNow(next)
    }
    if (event.key === 'ArrowLeft') step(-RUNS_PANE_STEP)
    else if (event.key === 'ArrowRight') step(RUNS_PANE_STEP)
    else if (event.key === 'Home' || event.key === 'Escape') setWidth(null)
    else return
    event.preventDefault()
  }

  const split = selectedRunId != null
  // A stored width is clamped on the way out rather than on the way in, so shrinking the
  // window and widening it again restores the width the user chose instead of the one their
  // smallest window allowed.
  const paneWidth = !split ? '100%' : width == null ? 'clamp(320px, 30vw, 460px)' : `${clamp(width)}px`

  return (
    <main className="flex flex-1" style={{ minHeight: 0 }} ref={hostRef}>
      {/* The list narrows once a run is open rather than being replaced. A results page
          is something you compare against its neighbours, and losing the list to see one
          run means navigating back to reach the next. */}
      <div
        ref={paneRef}
        className="flex"
        style={{
          width: paneWidth,
          minWidth: 0,
          flexShrink: 0,
          // Dropped while dragging: an eased width chases the pointer instead of tracking
          // it, which reads as lag rather than as motion design.
          transition: dragging ? undefined : 'width 200ms var(--ease)',
        }}
      >
        <RunList />
      </div>
      {split ? (
        <div
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize the run list"
          aria-valuenow={Math.round(paneNow)}
          aria-valuemin={RUNS_PANE_MIN}
          aria-valuemax={Math.round(max)}
          tabIndex={0}
          onPointerDown={onPointerDown}
          onKeyDown={onKeyDown}
          onDoubleClick={() => setWidth(null)}
          title="Drag to resize · double-click to reset"
          className="pl-splitter"
          data-dragging={dragging ? '' : undefined}
        />
      ) : null}
      {selectedRunId == null ? null : <RunDetail key={selectedRunId} runId={selectedRunId} />}
    </main>
  )
}

function Offline({
  message,
  onRetry,
  retrying,
}: {
  message: string
  onRetry: () => void
  retrying: boolean
}) {
  return (
    <main className="flex-1 flex items-center justify-center p-8">
      <div className="pl-panel p-4" style={{ maxWidth: 520 }}>
        <h2 style={{ margin: '0 0 8px', fontSize: 14, color: 'var(--down)' }}>
          Cannot reach the PerpLab API
        </h2>
        <p style={{ fontSize: 12, color: 'var(--text-dim)', margin: '0 0 8px' }}>{message}</p>
        <p style={{ fontSize: 12, color: 'var(--text-dim)', margin: 0 }}>
          Start it with:
        </p>
        <pre className="mono" style={{ fontSize: 12, margin: '6px 0 0' }}>
          perplab serve
        </pre>
        <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '8px 0 8px' }}>
          Retrying automatically every few seconds — this card clears itself once the API
          answers.
        </p>
        <button className="pl-btn" onClick={onRetry} disabled={retrying}>
          {retrying ? 'Checking…' : 'Retry now'}
        </button>
      </div>
    </main>
  )
}
