import type { ResourceUsage } from '../api'

export function ResourceStatus({ usage }: { usage: ResourceUsage }) {
  const gib = (bytes: number) => ((bytes ?? 0) / 1024 ** 3).toFixed(2)
  return (
    <div className="pl-panel p-2" style={{ fontSize: 12, color: 'var(--text-dim)', marginBottom: 12 }}>
      Memory {gib(usage.private_bytes)} GiB · Peak {gib(usage.peak_bytes)} GiB · Limit {gib(usage.allowance_bytes)} GiB · {usage.stage.replaceAll('_', ' ')}
      {usage.queue_reason ? <p className="mt-1">Queued: {usage.queue_reason}</p> : null}
      {usage.warning ? <p className="mt-1" style={{ color: 'var(--warn)' }}>Approaching the memory limit. Additional jobs are paused.</p> : null}
      {usage.termination_reason ? <p className="mt-1" style={{ color: 'var(--warn)' }}>{usage.termination_reason}{usage.failure_stage ? ` (during ${usage.failure_stage.replaceAll('_', ' ')})` : ''}</p> : null}
    </div>
  )
}
