import { useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, formatTime, money, num, signed, type Run, type Strategy } from '../api'
import { useUi } from '../store'

/**
 * The strategy library (spec 10.3, "Strategies").
 *
 * Two shapes, one dataset. With no strategy open, the tab is a full-width **card grid**
 * (`StrategyLibrary`) — name, tags, validity, and the latest run's outcome per card, with
 * the three verbs (Open / Backtest / Session) on the card itself. With a strategy open,
 * the grid gives way to the dense **sidebar list** (`StrategyList`) beside the editor, so
 * switching between strategies never costs the editor.
 *
 * An earlier revision was list-only, on the argument that the grid's per-card content did
 * not exist before Phase 4 produced runs. It exists now — `run_count` and the runs API's
 * per-run Sharpe/PnL — so the grid renders real outcomes rather than empty slots.
 */

/* ------------------------------------------------------------------ shared mutations */

function useCreateImport() {
  const queryClient = useQueryClient()
  const select = useUi((s) => s.select)
  const notify = useUi((s) => s.notify)

  const create = useMutation({
    mutationFn: (name: string) => api.create({ name }),
    onSuccess: (response) => {
      queryClient.invalidateQueries({ queryKey: ['strategies'] })
      select(response.strategy.id)
      notify(`Created ${response.strategy.name}`)
    },
    onError: (error: Error) => notify(error.message, 'error'),
  })

  const importFile = useMutation({
    mutationFn: (file: File) => api.import(file),
    onSuccess: (response) => {
      queryClient.invalidateQueries({ queryKey: ['strategies'] })
      select(response.strategy.id)
      const warnings = response.warnings ?? []
      if (warnings.length) notify(warnings.join(' '), 'warn')
      else notify(`Imported ${response.strategy.name}`)
    },
    onError: (error: Error) => notify(error.message, 'error'),
  })

  return { create, importFile }
}

/* ------------------------------------------------------------------------ card grid */

/** Full-width library view, shown while no strategy is open in the editor. */
export function StrategyLibrary() {
  const { select, showArchived, setShowArchived, search, setSearch, tag, setTag } = useUi()
  const openRunDraft = useUi((s) => s.openRunDraft)
  const openSessionDraft = useUi((s) => s.openSessionDraft)
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const fileInput = useRef<HTMLInputElement>(null)
  const { create, importFile } = useCreateImport()

  const list = useQuery({
    queryKey: ['strategies', { showArchived, search, tag }],
    queryFn: () => api.list({ archived: showArchived, search: search || undefined, tag: tag ?? undefined }),
  })
  // One fetch of recent runs feeds every card's "last result" line. Reusing the runs
  // endpoint rather than growing a per-strategy aggregation keeps the dashboard rule:
  // one source of numbers, no second set to disagree with the Runs tab.
  const runs = useQuery({ queryKey: ['runs', 'library'], queryFn: () => api.runs() })

  const lastRun = useMemo(() => {
    const map = new Map<number, Run>()
    for (const run of runs.data?.runs ?? []) {
      if (!map.has(run.strategy_id)) map.set(run.strategy_id, run)
    }
    return map
  }, [runs.data])

  const strategies = list.data?.strategies ?? []
  const allTags = [...new Set(strategies.flatMap((s) => s.tags))].sort()

  return (
    <section className="flex-1 flex flex-col" style={{ minWidth: 0 }}>
      <div
        className="flex items-center gap-2 px-3 shrink-0 flex-wrap"
        style={{ minHeight: 44, borderBottom: '1px solid var(--border)', background: 'var(--surface)' }}
      >
        <button className="pl-btn pl-btn-primary" onClick={() => setCreating(true)}>
          + New strategy
        </button>
        <button className="pl-btn" onClick={() => fileInput.current?.click()} disabled={importFile.isPending}>
          Import
        </button>
        <input
          ref={fileInput}
          type="file"
          accept=".py,.perplab,application/zip,text/x-python"
          className="hidden"
          onChange={(event) => {
            const file = event.target.files?.[0]
            if (file) importFile.mutate(file)
            // Reset so re-importing the same file fires a change event again. Without
            // this, a failed import cannot be retried without picking a different file.
            event.target.value = ''
          }}
        />
        <input
          className="pl-input"
          placeholder="Search name or notes…"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          style={{ width: 220 }}
        />
        {allTags.map((name) => (
          <button
            key={name}
            className="pl-tag"
            style={{
              cursor: 'pointer',
              color: tag === name ? 'var(--accent-ink)' : 'var(--text-dim)',
              background: tag === name ? 'var(--accent)' : undefined,
              borderColor: tag === name ? 'var(--accent)' : 'var(--border)',
              transition: 'all 120ms var(--ease)',
            }}
            onClick={() => setTag(tag === name ? null : name)}
          >
            {name}
          </button>
        ))}
        <span className="flex-1" />
        <label className="flex items-center gap-1.5" style={{ fontSize: 11, color: 'var(--text-dim)' }}>
          <input
            type="checkbox"
            checked={showArchived}
            onChange={(event) => setShowArchived(event.target.checked)}
          />
          Archived
        </label>
      </div>

      {creating && (
        <form
          className="p-2 flex gap-1.5 items-center shrink-0"
          style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-2)' }}
          onSubmit={(event) => {
            event.preventDefault()
            if (newName.trim()) create.mutate(newName.trim())
          }}
        >
          <input
            className="pl-input"
            autoFocus
            placeholder="Strategy name"
            value={newName}
            onChange={(event) => setNewName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Escape') setCreating(false)
            }}
            style={{ maxWidth: 320 }}
          />
          <button className="pl-btn pl-btn-primary" type="submit" disabled={!newName.trim() || create.isPending}>
            Create
          </button>
          <button className="pl-btn" type="button" onClick={() => setCreating(false)}>
            Cancel
          </button>
        </form>
      )}

      <div className="pl-scroll flex-1 p-4">
        {list.isLoading ? (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 12 }}>
            {Array.from({ length: 6 }, (_, i) => (
              <div key={i} className="pl-panel p-3 flex flex-col gap-2" aria-hidden="true">
                <span className="pl-skel" style={{ height: 16, width: '55%' }} />
                <span className="pl-skel" style={{ height: 12, width: '80%' }} />
                <span className="pl-skel" style={{ height: 12, width: '40%' }} />
              </div>
            ))}
          </div>
        ) : list.isError ? (
          <p style={{ fontSize: 12, color: 'var(--down)' }}>{(list.error as Error).message}</p>
        ) : strategies.length === 0 ? (
          <EmptyLibrary filtered={Boolean(search || tag)} />
        ) : (
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
              gap: 12,
              maxWidth: 1200,
              margin: '0 auto',
            }}
          >
            {strategies.map((strategy) => (
              <StrategyCard
                key={strategy.id}
                strategy={strategy}
                last={lastRun.get(strategy.id)}
                runsUnknown={runs.isError}
                onOpen={() => select(strategy.id)}
                onBacktest={() => openRunDraft(strategy.id)}
                onSession={() => openSessionDraft(true)}
              />
            ))}
          </div>
        )}
      </div>
    </section>
  )
}

function StrategyCard({
  strategy,
  last,
  runsUnknown,
  onOpen,
  onBacktest,
  onSession,
}: {
  strategy: Strategy
  last: Run | undefined
  /** The runs query failed: `last === undefined` then means *unknown*, not "never run". */
  runsUnknown: boolean
  onOpen: () => void
  onBacktest: () => void
  onSession: () => void
}) {
  const invalid = strategy.head ? !strategy.head.valid : false
  const [hover, setHover] = useState(false)
  const pnl = last?.net_pnl == null ? null : Number(last.net_pnl)
  return (
    <div
      className="pl-panel p-3 flex flex-col gap-2"
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      onClick={onOpen}
      role="button"
      style={{
        cursor: 'pointer',
        opacity: strategy.archived ? 0.55 : 1,
        borderColor: hover ? 'var(--border-strong)' : 'var(--border)',
        boxShadow: hover ? 'var(--shadow-1)' : 'none',
        transform: hover ? 'translateY(-1px)' : 'none',
        transition: 'border-color 140ms var(--ease), box-shadow 140ms var(--ease), transform 140ms var(--ease)',
      }}
    >
      <div className="flex items-center gap-1.5" style={{ minWidth: 0 }}>
        {/* Glyph plus colour, never colour alone (spec 10.1). */}
        {invalid && (
          <span className="sev-error" title="the current version does not validate">
            ✕
          </span>
        )}
        <span className="truncate" style={{ fontWeight: 550, fontSize: 13.5 }}>
          {strategy.name}
        </span>
        {strategy.archived && <span className="pl-tag">archived</span>}
        <span className="mono" style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--text-mute)' }}>
          v{strategy.head?.version_no ?? 0}
        </span>
      </div>

      {strategy.notes ? (
        <p
          className="truncate"
          style={{ margin: 0, fontSize: 11.5, color: 'var(--text-dim)' }}
          title={strategy.notes}
        >
          {strategy.notes}
        </p>
      ) : null}

      <div className="mono flex items-center gap-2 flex-wrap" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
        <span>{formatTime(strategy.updated_ms)}</span>
        <span>·</span>
        <span>{strategy.run_count} run{strategy.run_count === 1 ? '' : 's'}</span>
        {strategy.tags.map((name) => (
          <span key={name} className="pl-tag">
            {name}
          </span>
        ))}
      </div>

      {last != null ? (
        <div
          className="mono flex items-center gap-2 px-2 py-1"
          style={{
            fontSize: 11,
            background: 'var(--surface-2)',
            borderRadius: 5,
            color: 'var(--text-dim)',
          }}
          title={`Latest run #${last.id} — ${last.status}`}
        >
          <span style={{ color: 'var(--text-mute)' }}>last</span>
          <span>#{last.id}</span>
          <span
            style={{
              color:
                last.status === 'failed' || last.status === 'lost'
                  ? 'var(--down)'
                  : last.status === 'running'
                    ? 'var(--info)'
                    : 'var(--text-mute)',
            }}
          >
            {last.status}
          </span>
          <span style={{ marginLeft: 'auto', color: pnl == null ? 'var(--text-mute)' : pnl > 0 ? 'var(--pos)' : pnl < 0 ? 'var(--down)' : 'var(--text-dim)' }}>
            {pnl == null ? (last.sharpe == null ? '' : `S ${num(last.sharpe)}`) : signed(money(last.net_pnl!), pnl)}
          </span>
        </div>
      ) : runsUnknown ? (
        // A failed runs query is not "never run". A strategy with dozens of backtests must
        // not read as untested because one request failed — this card does not know, and
        // says so (the design creed: unknown is never rendered as an empty-but-healthy state).
        <div
          className="mono px-2 py-1"
          style={{ fontSize: 11, color: 'var(--warn)', background: 'var(--surface-2)', borderRadius: 5 }}
          title="The runs API did not answer, so this card cannot say whether the strategy has run."
        >
          △ run history unreachable
        </div>
      ) : (
        <div className="mono px-2 py-1" style={{ fontSize: 11, color: 'var(--text-mute)', background: 'var(--surface-2)', borderRadius: 5 }}>
          never run
        </div>
      )}

      {/* The verbs live on the card, so common workflows are one click from the grid.
          stopPropagation keeps them from also opening the editor. */}
      <div className="flex items-center gap-1.5" onClick={(event) => event.stopPropagation()}>
        <button className="pl-btn" style={{ height: 24, fontSize: 11 }} onClick={onOpen}>
          Open
        </button>
        <button className="pl-btn" style={{ height: 24, fontSize: 11 }} onClick={onBacktest}>
          Backtest
        </button>
        <button
          className="pl-btn"
          style={{ height: 24, fontSize: 11 }}
          onClick={onSession}
          title="Start a paper session (the dialog pre-selects nothing; pick this strategy there)"
        >
          Session
        </button>
      </div>
    </div>
  )
}

function EmptyLibrary({ filtered }: { filtered: boolean }) {
  return (
    <div className="flex items-center justify-center" style={{ minHeight: 240 }}>
      <div style={{ maxWidth: 420, textAlign: 'center' }}>
        {filtered ? (
          <p style={{ fontSize: 12.5, color: 'var(--text-mute)' }}>Nothing matches that filter.</p>
        ) : (
          <>
            <p style={{ fontSize: 13, color: 'var(--text-dim)', margin: '0 0 8px' }}>
              No strategies yet.
            </p>
            <p style={{ fontSize: 12, color: 'var(--text-mute)', margin: 0 }}>
              <b>New strategy</b> opens a commented template in the editor. Write it, press
              ⌘S, and the validator runs before anything is stored.
            </p>
          </>
        )}
      </div>
    </div>
  )
}

/* --------------------------------------------------------------------- sidebar list */

/** Dense sidebar, shown beside the editor once a strategy is open. */
export function StrategyList() {
  const { selectedId, select, showArchived, setShowArchived, search, setSearch, tag, setTag } =
    useUi()
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const fileInput = useRef<HTMLInputElement>(null)
  const { create, importFile } = useCreateImport()

  const list = useQuery({
    queryKey: ['strategies', { showArchived, search, tag }],
    queryFn: () => api.list({ archived: showArchived, search: search || undefined, tag: tag ?? undefined }),
  })

  const strategies = list.data?.strategies ?? []
  const allTags = [...new Set(strategies.flatMap((s) => s.tags))].sort()

  return (
    <aside
      className="flex flex-col shrink-0"
      style={{ width: 280, borderRight: '1px solid var(--border)', background: 'var(--surface)' }}
    >
      <div className="flex items-center gap-1.5 p-2" style={{ borderBottom: '1px solid var(--border)' }}>
        <button className="pl-btn pl-btn-primary" onClick={() => setCreating(true)}>
          + New
        </button>
        <button className="pl-btn" onClick={() => fileInput.current?.click()} disabled={importFile.isPending}>
          Import
        </button>
        <span className="flex-1" />
        <button
          className="pl-btn pl-btn-icon"
          title="Back to the library grid"
          aria-label="Back to the library grid"
          onClick={() => select(null)}
        >
          ▤
        </button>
        <input
          ref={fileInput}
          type="file"
          accept=".py,.perplab,application/zip,text/x-python"
          className="hidden"
          onChange={(event) => {
            const file = event.target.files?.[0]
            if (file) importFile.mutate(file)
            // Reset so re-importing the same file fires a change event again. Without
            // this, a failed import cannot be retried without picking a different file.
            event.target.value = ''
          }}
        />
      </div>

      {creating && (
        <form
          className="p-2 flex flex-col gap-1.5"
          style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-2)' }}
          onSubmit={(event) => {
            event.preventDefault()
            if (newName.trim()) create.mutate(newName.trim())
          }}
        >
          <input
            className="pl-input"
            autoFocus
            placeholder="Strategy name"
            value={newName}
            onChange={(event) => setNewName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Escape') setCreating(false)
            }}
          />
          <div className="flex gap-1.5">
            <button className="pl-btn pl-btn-primary" type="submit" disabled={!newName.trim() || create.isPending}>
              Create
            </button>
            <button className="pl-btn" type="button" onClick={() => setCreating(false)}>
              Cancel
            </button>
          </div>
        </form>
      )}

      <div className="p-2 flex flex-col gap-1.5" style={{ borderBottom: '1px solid var(--border)' }}>
        <input
          className="pl-input"
          placeholder="Search name or notes"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
        <label className="flex items-center gap-1.5" style={{ fontSize: 11, color: 'var(--text-dim)' }}>
          <input
            type="checkbox"
            checked={showArchived}
            onChange={(event) => setShowArchived(event.target.checked)}
          />
          Show archived
        </label>
        {allTags.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {allTags.map((name) => (
              <button
                key={name}
                className="pl-tag"
                style={{
                  cursor: 'pointer',
                  color: tag === name ? 'var(--text)' : 'var(--text-dim)',
                  borderColor: tag === name ? 'var(--text-dim)' : 'var(--border)',
                }}
                onClick={() => setTag(tag === name ? null : name)}
              >
                {name}
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="pl-scroll flex-1">
        {list.isLoading && (
          <div className="p-2 flex flex-col gap-2" aria-hidden="true">
            {Array.from({ length: 5 }, (_, i) => (
              <span key={i} className="pl-skel" style={{ height: 30 }} />
            ))}
          </div>
        )}
        {list.isError && <Empty kind="error">{(list.error as Error).message}</Empty>}
        {list.isSuccess && strategies.length === 0 && (
          <Empty>
            {search || tag ? (
              <>Nothing matches that filter.</>
            ) : (
              <>
                No strategies yet.
                <br />
                <br />
                <b>New</b> opens a commented template in the editor. Write it, press ⌘S, and
                the validator runs before anything is stored.
              </>
            )}
          </Empty>
        )}
        {strategies.map((strategy) => (
          <Row
            key={strategy.id}
            strategy={strategy}
            selected={strategy.id === selectedId}
            onSelect={() => select(strategy.id)}
          />
        ))}
      </div>
    </aside>
  )
}

function Row({
  strategy,
  selected,
  onSelect,
}: {
  strategy: Strategy
  selected: boolean
  onSelect: () => void
}) {
  const invalid = strategy.head ? !strategy.head.valid : false
  return (
    <button
      onClick={onSelect}
      className="w-full text-left px-2 py-1.5 flex flex-col gap-0.5"
      style={{
        borderBottom: '1px solid var(--border)',
        background: selected ? 'var(--surface-2)' : 'transparent',
        borderLeft: `2px solid ${selected ? 'var(--accent)' : 'transparent'}`,
        opacity: strategy.archived ? 0.55 : 1,
        cursor: 'pointer',
        transition: 'background 100ms var(--ease)',
      }}
    >
      <span className="flex items-center gap-1.5" style={{ minWidth: 0 }}>
        {/* Glyph plus colour, never colour alone (spec 10.1). */}
        {invalid && (
          <span className="sev-error" title="the current version does not validate">
            ✕
          </span>
        )}
        <span className="truncate" style={{ fontWeight: 500 }}>
          {strategy.name}
        </span>
        {strategy.archived && <span className="pl-tag">archived</span>}
      </span>
      <span className="mono flex items-center gap-2" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
        <span>v{strategy.head?.version_no ?? 0}</span>
        <span>{formatTime(strategy.updated_ms)}</span>
        {strategy.run_count > 0 && <span>{strategy.run_count} runs</span>}
      </span>
      {strategy.tags.length > 0 && (
        <span className="flex flex-wrap gap-1 mt-0.5">
          {strategy.tags.map((tag) => (
            <span key={tag} className="pl-tag">
              {tag}
            </span>
          ))}
        </span>
      )}
    </button>
  )
}

/** Empty states are instructional, not decorative (spec 10.4). */
function Empty({ children, kind }: { children: React.ReactNode; kind?: 'error' }) {
  return (
    <p
      className="p-3"
      style={{ fontSize: 12, color: kind === 'error' ? 'var(--down)' : 'var(--text-mute)' }}
    >
      {children}
    </p>
  )
}
