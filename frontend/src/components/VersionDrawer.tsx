import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, formatTime, type StrategyVersion } from '../api'

/**
 * Version history with a diff view (spec 5.6, 10.3).
 *
 * Two versions are selectable at once and the diff renders between them. The default pair
 * is head and the one before it, because "what did I just change" is the question this
 * drawer is opened to answer nine times out of ten.
 */
export function VersionDrawer({
  strategyId,
  onClose,
  onLoadVersion,
}: {
  strategyId: number
  onClose: () => void
  onLoadVersion: (version: StrategyVersion) => void
}) {
  const versions = useQuery({
    queryKey: ['versions', strategyId],
    queryFn: () => api.versions(strategyId),
  })
  const list = versions.data?.versions ?? []
  const [right, setRight] = useState<number | null>(null)
  const [left, setLeft] = useState<number | null>(null)

  const rightNo = right ?? list[0]?.version_no ?? null
  const leftNo = left ?? list[1]?.version_no ?? null

  const diff = useQuery({
    queryKey: ['diff', strategyId, leftNo, rightNo],
    queryFn: () => api.diff(strategyId, leftNo!, rightNo!),
    enabled: leftNo != null && rightNo != null && leftNo !== rightNo,
  })

  return (
    <aside
      className="flex flex-col shrink-0"
      style={{ width: 420, borderLeft: '1px solid var(--border)', background: 'var(--surface)' }}
    >
      <div
        className="flex items-center gap-2 px-2 shrink-0"
        style={{ height: 32, borderBottom: '1px solid var(--border)' }}
      >
        <span style={{ fontSize: 12, fontWeight: 500 }}>Version history</span>
        <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
          {list.length}
        </span>
        <span className="flex-1" />
        <button className="pl-btn" onClick={onClose} style={{ height: 22 }}>
          Close
        </button>
      </div>

      <div className="pl-scroll shrink-0" style={{ maxHeight: '45%' }}>
        {list.length === 0 && (
          <p className="p-2" style={{ fontSize: 12, color: 'var(--text-mute)' }}>
            No versions yet.
          </p>
        )}
        {list.map((version, index) => (
          <div
            key={version.id}
            className="flex items-center gap-2 px-2 py-1"
            style={{
              borderBottom: '1px solid var(--border)',
              background:
                version.version_no === rightNo || version.version_no === leftNo
                  ? 'var(--surface-2)'
                  : 'transparent',
            }}
          >
            <span className="mono shrink-0" style={{ width: 34, fontSize: 12 }}>
              v{version.version_no}
              {index === 0 && <span style={{ color: 'var(--text-mute)' }}>*</span>}
            </span>
            <span className="mono shrink-0" style={{ fontSize: 11, color: 'var(--text-mute)', width: 104 }}>
              {formatTime(version.created_ms)}
            </span>
            <span
              className={version.valid ? '' : 'sev-error'}
              title={version.valid ? 'validated clean' : 'saved with validation errors'}
              style={{ width: 12 }}
            >
              {version.valid ? '✓' : '✕'}
            </span>
            <span className="truncate flex-1" style={{ fontSize: 11, color: 'var(--text-dim)' }}>
              {version.message || '—'}
            </span>
            <button
              className="pl-btn shrink-0"
              style={{ height: 20, fontSize: 11 }}
              title="Load this version into the editor. It is not saved until you press ⌘S."
              onClick={() =>
                api.version(strategyId, version.version_no).then((r) => onLoadVersion(r.version))
              }
            >
              Load
            </button>
            <input
              type="radio"
              name="diff-left"
              title="diff from"
              checked={version.version_no === leftNo}
              onChange={() => setLeft(version.version_no)}
            />
            <input
              type="radio"
              name="diff-right"
              title="diff to"
              checked={version.version_no === rightNo}
              onChange={() => setRight(version.version_no)}
            />
          </div>
        ))}
      </div>

      <div
        className="flex items-center gap-2 px-2 shrink-0"
        style={{ height: 26, borderTop: '1px solid var(--border)', borderBottom: '1px solid var(--border)' }}
      >
        <span className="mono" style={{ fontSize: 11, color: 'var(--text-mute)' }}>
          {leftNo != null && rightNo != null ? `v${leftNo} → v${rightNo}` : 'select two versions'}
        </span>
      </div>

      <div className="pl-scroll flex-1">
        {leftNo === rightNo && (
          <p className="p-2" style={{ fontSize: 12, color: 'var(--text-mute)' }}>
            Pick two different versions to compare.
          </p>
        )}
        {diff.data && <DiffView text={diff.data.diff} />}
      </div>
    </aside>
  )
}

function DiffView({ text }: { text: string }) {
  if (!text.trim()) {
    return (
      <p className="p-2" style={{ fontSize: 12, color: 'var(--text-mute)' }}>
        Identical.
      </p>
    )
  }
  return (
    <pre className="mono m-0" style={{ fontSize: 11, lineHeight: 1.6 }}>
      {text.split('\n').map((line, index) => {
        // The leading +/- character is kept in the rendered line rather than stripped and
        // replaced by colour. A diff read in a screenshot, or by someone who cannot
        // distinguish the two hues, is still a diff (spec 10.1).
        const cls = line.startsWith('+++') || line.startsWith('---') || line.startsWith('@@')
          ? 'diff-meta'
          : line.startsWith('+')
            ? 'diff-add'
            : line.startsWith('-')
              ? 'diff-del'
              : ''
        return (
          <div key={index} className={cls} style={{ padding: '0 8px' }}>
            {line || ' '}
          </div>
        )
      })}
    </pre>
  )
}
