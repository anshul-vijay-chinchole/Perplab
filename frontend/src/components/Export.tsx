/**
 * Export affordances (spec 10.4): a frame that gives any SVG chart a PNG button, and a
 * small CSV button for tables. Deliberately quiet — 10 px controls in the corner, because
 * every chart carries one and a loud button repeated twelve times is chrome, not signal.
 */

import { useRef } from 'react'
import { downloadCsv, downloadPng } from '../lib/export'
import { useUi } from '../store'

export function ChartFrame({
  name,
  children,
}: {
  name: string
  children: React.ReactNode
}) {
  const ref = useRef<HTMLDivElement>(null)
  const notify = useUi((s) => s.notify)
  return (
    <div ref={ref} style={{ position: 'relative' }}>
      {children}
      <button
        type="button"
        className="pl-btn"
        title="Export this chart as PNG"
        style={{ position: 'absolute', top: 2, right: 2, fontSize: 10, padding: '1px 6px', opacity: 0.7 }}
        onClick={() => {
          const svg = ref.current?.querySelector('svg')
          if (svg == null) return
          // A failed export must say so (M55): before this, every failure path in
          // downloadPng returned silently and a broken export looked like a slow one.
          downloadPng(svg, `${name}.png`).catch((error: unknown) => {
            notify(`PNG export failed: ${error instanceof Error ? error.message : String(error)}`, 'error')
          })
        }}
      >
        PNG
      </button>
    </div>
  )
}

export function CsvButton({
  name,
  headers,
  rows,
}: {
  name: string
  headers: string[]
  rows: (string | number | boolean | null | undefined)[][]
}) {
  return (
    <button
      type="button"
      className="pl-btn"
      title="Export this table as CSV"
      style={{ fontSize: 10, padding: '1px 6px' }}
      onClick={() => downloadCsv(`${name}.csv`, headers, rows)}
    >
      CSV
    </button>
  )
}
