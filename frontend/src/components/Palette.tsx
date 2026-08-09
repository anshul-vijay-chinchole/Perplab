/**
 * The ⌘K command palette (spec 10.4): jump to a strategy or run, start a backtest or a
 * paper session, switch tabs, toggle the theme. Esc closes. The kill switch deliberately
 * has NO entry and no shortcut — spec 10.4 forbids one, so muscle memory can never fire it.
 *
 * Also opens on the chrome's ⌘K button via the `perplab:palette` custom event, so the
 * palette is discoverable by pointer as well as by keyboard.
 */

import { useQuery } from '@tanstack/react-query'
import { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api'
import { useUi, type Tab } from '../store'

interface Action {
  key: string
  label: string
  hint: string
  run: () => void
}

export function CommandPalette() {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const setTab = useUi((s) => s.setTab)
  const select = useUi((s) => s.select)
  const selectRun = useUi((s) => s.selectRun)
  const openRunDraft = useUi((s) => s.openRunDraft)
  const openSessionDraft = useUi((s) => s.openSessionDraft)
  const toggleTheme = useUi((s) => s.toggleTheme)

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        setOpen((current) => !current)
        setQuery('')
      } else if (event.key === 'Escape') {
        setOpen(false)
      }
    }
    const onOpen = () => {
      setOpen(true)
      setQuery('')
    }
    window.addEventListener('keydown', onKey)
    window.addEventListener('perplab:palette', onOpen)
    return () => {
      window.removeEventListener('keydown', onKey)
      window.removeEventListener('perplab:palette', onOpen)
    }
  }, [])

  useEffect(() => {
    if (open) inputRef.current?.focus()
  }, [open])

  const strategies = useQuery({
    queryKey: ['strategies', 'palette'],
    queryFn: () => api.list(),
    enabled: open,
  })
  const runs = useQuery({
    queryKey: ['runs', 'palette'],
    queryFn: () => api.runs(),
    enabled: open,
  })

  const actions = useMemo<Action[]>(() => {
    const out: Action[] = []
    out.push({
      key: 'new:backtest',
      label: 'New backtest…',
      hint: 'action',
      run: () => openRunDraft(-1),
    })
    out.push({
      key: 'new:session',
      label: 'Start paper session…',
      hint: 'action',
      run: () => openSessionDraft(true),
    })
    out.push({
      key: 'theme',
      label: 'Toggle light/dark theme',
      hint: 'action',
      run: toggleTheme,
    })
    for (const tab of ['Dashboard', 'Strategies', 'Runs', 'Lab', 'Data & Feed', 'Settings'] as Tab[]) {
      out.push({
        key: `tab:${tab}`,
        label: `Go to ${tab}`,
        hint: 'tab',
        run: () => setTab(tab),
      })
    }
    for (const strategy of strategies.data?.strategies ?? []) {
      out.push({
        key: `strategy:${strategy.id}`,
        label: strategy.name,
        hint: 'open strategy',
        run: () => {
          setTab('Strategies')
          select(strategy.id)
        },
      })
      out.push({
        key: `backtest:${strategy.id}`,
        label: `Backtest ${strategy.name}`,
        hint: 'new run',
        run: () => openRunDraft(strategy.id),
      })
    }
    for (const run of (runs.data?.runs ?? []).slice(0, 30)) {
      out.push({
        key: `run:${run.id}`,
        label: `Run #${run.id} · ${run.strategy_name} v${run.version_no}${run.label ? ` · ${run.label}` : ''}`,
        hint: run.status,
        run: () => {
          setTab('Runs')
          selectRun(run.id)
        },
      })
    }
    return out
  }, [strategies.data, runs.data, setTab, select, selectRun, openRunDraft, openSessionDraft, toggleTheme])

  const needle = query.trim().toLowerCase()
  const visible = (
    needle === ''
      ? actions
      : actions.filter((action) => action.label.toLowerCase().includes(needle))
  ).slice(0, 12)
  const [cursor, setCursor] = useState(0)
  useEffect(() => setCursor(0), [needle, open])

  if (!open) return null
  return (
    <div
      className="pl-scrim"
      style={{ zIndex: 60, paddingTop: '15vh' }}
      onClick={() => setOpen(false)}
    >
      <div
        className="pl-modal"
        style={{ width: 'min(520px, calc(100vw - 32px))', height: 'fit-content', overflow: 'hidden' }}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)', paddingRight: 10 }}>
          <input
            ref={inputRef}
            className="pl-input"
            placeholder="Jump to a strategy, run, or tab…"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'ArrowDown') setCursor((c) => Math.min(c + 1, visible.length - 1))
              else if (event.key === 'ArrowUp') setCursor((c) => Math.max(c - 1, 0))
              else if (event.key === 'Enter' && visible[cursor] != null) {
                visible[cursor].run()
                setOpen(false)
              }
            }}
            style={{ border: 0, borderRadius: 0, height: 38, fontSize: 13, background: 'transparent', outline: 'none' }}
          />
          <span className="pl-kbd">esc</span>
        </div>
        <div className="pl-scroll" style={{ maxHeight: 340, overflowY: 'auto', padding: '4px 0' }}>
          {visible.length === 0 ? (
            <div style={{ padding: '10px 12px', fontSize: 12, color: 'var(--text-mute)' }}>Nothing matches.</div>
          ) : (
            visible.map((action, index) => (
              <button
                key={action.key}
                type="button"
                onMouseEnter={() => setCursor(index)}
                onClick={() => {
                  action.run()
                  setOpen(false)
                }}
                className="flex items-center gap-2"
                style={{
                  display: 'flex', width: '100%', textAlign: 'left', border: 0, padding: '7px 12px',
                  fontSize: 12.5, cursor: 'pointer',
                  background: index === cursor ? 'var(--surface-2)' : 'transparent',
                  color: 'var(--text)',
                  transition: 'background 80ms var(--ease)',
                }}
              >
                <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {action.label}
                </span>
                <span className="mono" style={{ fontSize: 10, color: 'var(--text-mute)' }}>{action.hint}</span>
              </button>
            ))
          )}
        </div>
        <div
          className="flex items-center gap-3 px-3"
          style={{ height: 26, borderTop: '1px solid var(--border)', fontSize: 10.5, color: 'var(--text-mute)' }}
        >
          <span><span className="pl-kbd">↑↓</span> navigate</span>
          <span><span className="pl-kbd">↵</span> open</span>
          <span className="flex-1" />
          <span>The kill switch is never here — it has no shortcut by design.</span>
        </div>
      </div>
    </div>
  )
}
