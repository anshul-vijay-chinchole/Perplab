import { create } from 'zustand'

/**
 * UI state (spec 2.2 — Zustand for UI state, TanStack Query for server state).
 *
 * The split is worth keeping strict: anything the server owns lives in Query so that a
 * mutation invalidates one cache and every view updates. Anything that only exists in this
 * browser tab lives here. A strategy list kept in Zustand would need manual refresh calls
 * scattered through the components, and one of them would eventually be missed.
 */

type Theme = 'dark' | 'light'

const THEME_KEY = 'perplab.theme'
const RUNS_PANE_KEY = 'perplab.runsPaneWidth'

/** Bounds for the Runs tab's draggable list pane, in px.
 *
 *  The minimum is not cosmetic: `RunList`'s ten columns are all `white-space: nowrap`, so a
 *  narrower pane does not reflow, it just hides columns behind a horizontal scrollbar. The
 *  maximum is expressed as room left for the detail pane rather than as a width for the
 *  list, because the thing that must stay usable is whichever pane is being squeezed.
 */
export const RUNS_PANE_MIN = 280
export const RUNS_PANE_DETAIL_MIN = 420

function readRunsPaneWidth(): number | null {
  try {
    const raw = localStorage.getItem(RUNS_PANE_KEY)
    if (raw == null) return null
    const px = Number(raw)
    // A stored NaN or a width from a much wider monitor is discarded rather than clamped:
    // `App` clamps against the live container anyway, and honouring junk here would make a
    // corrupt key sticky.
    return Number.isFinite(px) && px >= RUNS_PANE_MIN ? px : null
  } catch {
    return null
  }
}

function readTheme(): Theme {
  if (typeof document === 'undefined') return 'dark'
  const attr = document.documentElement.dataset.theme
  return attr === 'light' ? 'light' : 'dark'
}

export type Tab = 'Dashboard' | 'Strategies' | 'Runs' | 'Lab' | 'Data & Feed' | 'Settings'

interface UiState {
  theme: Theme
  toggleTheme: () => void

  tab: Tab
  setTab: (tab: Tab) => void

  selectedId: number | null
  select: (id: number | null) => void

  selectedRunId: number | null
  selectRun: (id: number | null) => void

  /** Width in px of the Runs tab's list pane, or `null` to use the responsive default.
   *
   *  Lives here rather than in a component because both panes are remounted out from under
   *  any local state: `<div key={tab}>` discards it on every tab switch and
   *  `<RunDetail key={selectedRunId}>` discards it on every row click. A dragged width that
   *  reset when you opened the next run would be worse than not being draggable. */
  runsPaneWidth: number | null
  setRunsPaneWidth: (px: number | null) => void

  /** Which run the Data & Feed tab's Feed panel is tailing.
   *
   *  Separate from `selectedRunId` on purpose: the Feed is the first place to look when
   *  something is wrong (spec 10.3), and tying it to the Runs table's selection would move
   *  it every time someone clicked a row to check an unrelated backtest -- losing the tail
   *  position on the session they were watching. */
  feedRunId: number | null
  setFeedRunId: (id: number | null) => void

  /** Strategy the New Run dialog is being opened for, or `null` when it is closed. */
  runDraftFor: number | null
  openRunDraft: (strategyId: number | null) => void

  /** Whether the Start Session dialog is open.
   *
   *  A boolean rather than a strategy id like `runDraftFor`, because a session is started
   *  from the Runs tab rather than from a strategy's editor -- there is no "run *this* one"
   *  entry point to carry, and the form picks the strategy itself. */
  sessionDraftOpen: boolean
  openSessionDraft: (open: boolean) => void

  /** Which Lab job's results are open, or `null` for the Lab tab's landing state. */
  selectedLabJobId: number | null
  selectLabJob: (id: number | null) => void

  /** Run preselected in the Lab's new-job form — how "Send to Lab" (spec 10.3) arrives.
   *
   *  Separate from `selectedRunId`: sending a run to the Lab must not depend on it still
   *  being the Runs table's selection by the time the Lab tab renders. */
  labDraftRunId: number | null
  sendToLab: (runId: number | null) => void

  showArchived: boolean
  setShowArchived: (value: boolean) => void

  search: string
  setSearch: (value: string) => void

  tag: string | null
  setTag: (value: string | null) => void

  versionsOpen: boolean
  setVersionsOpen: (value: boolean) => void

  toast: { text: string; kind: 'ok' | 'warn' | 'error' } | null
  notify: (text: string, kind?: 'ok' | 'warn' | 'error') => void
  clearToast: () => void
}

export const useUi = create<UiState>((set) => ({
  theme: readTheme(),
  toggleTheme: () =>
    set((state) => {
      const next: Theme = state.theme === 'dark' ? 'light' : 'dark'
      document.documentElement.dataset.theme = next
      try {
        localStorage.setItem(THEME_KEY, next)
      } catch {
        /* private browsing; the theme simply does not persist */
      }
      return { theme: next }
    }),

  // The Dashboard is the landing tab: it answers "is real money at risk" before anything
  // else is clicked, which is the question spec 10.3 puts first.
  tab: 'Dashboard',
  setTab: (tab) => set({ tab }),

  selectedId: null,
  select: (id) => set({ selectedId: id, versionsOpen: false }),

  selectedRunId: null,
  selectRun: (selectedRunId) => set({ selectedRunId }),

  runsPaneWidth: readRunsPaneWidth(),
  setRunsPaneWidth: (runsPaneWidth) =>
    set(() => {
      try {
        if (runsPaneWidth == null) localStorage.removeItem(RUNS_PANE_KEY)
        else localStorage.setItem(RUNS_PANE_KEY, String(Math.round(runsPaneWidth)))
      } catch {
        /* private browsing; the width simply does not persist */
      }
      return { runsPaneWidth }
    }),

  feedRunId: null,
  setFeedRunId: (feedRunId) => set({ feedRunId }),

  runDraftFor: null,
  openRunDraft: (runDraftFor) => set({ runDraftFor }),

  sessionDraftOpen: false,
  openSessionDraft: (sessionDraftOpen) => set({ sessionDraftOpen }),

  selectedLabJobId: null,
  selectLabJob: (selectedLabJobId) => set({ selectedLabJobId }),

  labDraftRunId: null,
  sendToLab: (labDraftRunId) =>
    set(
      labDraftRunId == null
        ? { labDraftRunId }
        : { labDraftRunId, tab: 'Lab', selectedLabJobId: null },
    ),

  showArchived: false,
  setShowArchived: (showArchived) => set({ showArchived }),

  search: '',
  setSearch: (search) => set({ search }),

  tag: null,
  setTag: (tag) => set({ tag }),

  versionsOpen: false,
  setVersionsOpen: (versionsOpen) => set({ versionsOpen }),

  toast: null,
  notify: (text, kind = 'ok') => set({ toast: { text, kind } }),
  clearToast: () => set({ toast: null }),
}))
