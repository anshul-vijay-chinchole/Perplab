/**
 * Settings (spec 10.3): the defaults the platform applies when the user does not choose.
 *
 * Server-side settings only — theme lives in the browser and toggles from the chrome.
 * Every field here is a *default* the run form starts from; the stored run spec records
 * what a run actually used, so changing a setting never rewrites history.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { ApiError, api, type ServerSettings } from '../api'
import { playSound, setSoundsEnabled, soundsEnabled } from '../lib/sound'
import { useUi } from '../store'

export function SettingsPanel() {
  const queryClient = useQueryClient()
  const notify = useUi((s) => s.notify)
  const settings = useQuery({ queryKey: ['settings'], queryFn: api.settings })
  const [draft, setDraft] = useState<ServerSettings | null>(null)
  useEffect(() => {
    if (settings.data != null && draft == null) setDraft(settings.data.settings)
  }, [settings.data, draft])

  const save = useMutation({
    mutationFn: (body: ServerSettings) => api.saveSettings(body),
    onSuccess: () => {
      notify('Settings saved — they apply as defaults to the next run form you open')
      void queryClient.invalidateQueries({ queryKey: ['settings'] })
    },
    onError: (error) =>
      notify(error instanceof ApiError ? error.message : String(error), 'error'),
  })

  if (draft == null) {
    // A failed request is not a slow one. Without this branch the shimmer skeleton below
    // ran forever on any error — a loading state that can never finish is the UI
    // converting "the API did not answer" into "still working on it".
    if (settings.isError) {
      return (
        <main className="flex-1 p-4" style={{ overflowY: 'auto' }}>
          <div className="pl-panel p-3" style={{ maxWidth: 560, borderColor: 'color-mix(in srgb, var(--down) 45%, var(--border))' }}>
            <h2 style={{ margin: '0 0 6px', fontSize: 14, color: 'var(--down)' }}>
              Settings could not be loaded
            </h2>
            <p style={{ fontSize: 12, color: 'var(--text-dim)', margin: '0 0 10px' }}>
              {settings.error instanceof ApiError
                ? settings.error.message
                : String(settings.error)}
            </p>
            <button className="pl-btn" onClick={() => settings.refetch()} disabled={settings.isFetching}>
              {settings.isFetching ? 'Retrying…' : 'Retry'}
            </button>
          </div>
        </main>
      )
    }
    return (
      <main className="flex-1 p-4" style={{ overflowY: 'auto' }}>
        <div aria-hidden={true} style={{ maxWidth: 560 }}>
          <div className="pl-skel" style={{ height: 14, width: 120, marginBottom: 6 }} />
          <div className="pl-skel" style={{ height: 11, width: '80%', marginBottom: 16 }} />
          {[0, 1, 2].map((i) => (
            <div key={i} className="pl-panel p-3" style={{ marginBottom: 12 }}>
              <div className="pl-skel" style={{ height: 11, width: 150, marginBottom: 12 }} />
              <div className="pl-skel" style={{ height: 28, marginBottom: 8 }} />
              <div className="pl-skel" style={{ height: 28, width: '72%' }} />
            </div>
          ))}
        </div>
      </main>
    )
  }

  const set = <K extends keyof ServerSettings>(key: K, value: ServerSettings[K]) =>
    setDraft({ ...draft, [key]: value })

  return (
    <main className="flex-1 p-4" style={{ overflowY: 'auto' }}>
      <div style={{ maxWidth: 560 }}>
        <h2 style={{ fontSize: 14, margin: '0 0 4px' }}>Settings</h2>
        <p style={{ fontSize: 11, color: 'var(--text-mute)', margin: '0 0 16px' }}>
          Starting values for the New backtest form — it can still override any of them,
          and every run's spec records what it actually used, so changing a setting never
          rewrites history. Theme toggles from the ◐ in the top bar and stays in this
          browser.
        </p>

        {settings.data?.problem ? (
          // The stored file could not be used as written (`settings.py load_settings`):
          // what is on screen is defaults, or the stored values with a bad field. Said out
          // loud, because a user editing on top of values they believe are their saved
          // ones — and then saving over the file — must be doing it knowingly.
          <div
            className="pl-panel p-2"
            style={{ marginBottom: 12, borderColor: 'color-mix(in srgb, var(--warn) 45%, var(--border))' }}
          >
            <p style={{ margin: 0, fontSize: 12, color: 'var(--text-dim)' }}>
              <span style={{ color: 'var(--warn)' }}>△ </span>
              {settings.data.problem}
            </p>
          </div>
        ) : null}

        <Section title="Run defaults">
          <Row label="Leverage">
            <input className="pl-input mono" style={{ width: 80 }} value={String(draft.default_leverage)}
              onChange={(e) => set('default_leverage', Number(e.target.value) || 1)} />
          </Row>
          <Row label="Maker fee rate">
            <input className="pl-input mono" style={{ width: 110 }} value={draft.maker_rate}
              onChange={(e) => set('maker_rate', e.target.value)} />
          </Row>
          <Row label="Taker fee rate">
            <input className="pl-input mono" style={{ width: 110 }} value={draft.taker_rate}
              onChange={(e) => set('taker_rate', e.target.value)} />
          </Row>
          <Row label="Latency model">
            <select className="pl-input" style={{ width: 160 }} value={draft.latency_model}
              onChange={(e) => set('latency_model', e.target.value)}>
              <option value="lognormal">lognormal</option>
              <option value="fixed">fixed</option>
            </select>
          </Row>
          <Row label="Submit latency (ms)">
            <input className="pl-input mono" style={{ width: 80 }} value={String(draft.submit_ms)}
              onChange={(e) => set('submit_ms', Number(e.target.value) || 0)} />
          </Row>
        </Section>

        <Section title="Default risk limits (spec 7)">
          <Row label="Risk layer on by default">
            <input type="checkbox" checked={draft.risk_enabled}
              onChange={(e) => set('risk_enabled', e.target.checked)} />
          </Row>
          <Row label="Max leverage">
            <input className="pl-input mono" style={{ width: 80 }} value={draft.max_leverage ?? ''}
              onChange={(e) => set('max_leverage', e.target.value || null)} />
          </Row>
          <Row label="Max daily loss (fraction)">
            <input className="pl-input mono" style={{ width: 80 }} value={draft.max_daily_loss_pct ?? ''}
              onChange={(e) => set('max_daily_loss_pct', e.target.value || null)} />
          </Row>
          <Row label="Max drawdown (fraction)">
            <input className="pl-input mono" style={{ width: 80 }} value={draft.max_drawdown_pct ?? ''}
              onChange={(e) => set('max_drawdown_pct', e.target.value || null)} />
          </Row>
        </Section>

        <Section title="Kill switch (spec 7.3)">
          <Row label="Halt behaviour">
            <select className="pl-input" value={draft.kill_switch_flatten ? 'flatten' : 'cancel'}
              onChange={(e) => set('kill_switch_flatten', e.target.value === 'flatten')}>
              <option value="cancel">cancel-only (default) — stop trading, keep positions</option>
              <option value="flatten">close-all — market-exit every position on halt</option>
            </select>
          </Row>
          <p style={{ fontSize: 11, color: 'var(--text-dim)', margin: '4px 0 0' }}>
            Cancel-only is the spec's default: during a flash crash a forced market exit
            can be worse than the exposure. The kill dialog states in words what it is
            about to do, every time, before anything is sent.
          </p>
          <p style={{ fontSize: 10, color: 'var(--text-mute)', margin: '6px 0 0' }}>
            The kill dialog reads this value, states it in words, and repeats it on the
            button — so the last thing read before the click is the behaviour being
            bought.
          </p>
        </Section>

        <BrowserSection />

        <div className="flex gap-2 mt-4">
          <button className="pl-btn pl-btn-primary" disabled={save.isPending}
            onClick={() => save.mutate(draft)}>
            {save.isPending ? 'Saving…' : 'Save settings'}
          </button>
          <button
            className="pl-btn"
            // Guarded: reverting to `null` while the query has no data dropped the whole
            // panel back into the skeleton it can never leave. No server copy, no revert.
            disabled={settings.data == null}
            title={settings.data == null ? 'No server copy loaded to revert to.' : undefined}
            onClick={() => {
              if (settings.data != null) setDraft(settings.data.settings)
            }}
          >
            Revert
          </button>
        </div>
      </div>
    </main>
  )
}

/** Preferences of this browser, not of the platform — stored in localStorage beside the
 *  theme, never sent to the server, and excluded from the Save button above on purpose:
 *  saving run defaults must not silently commit an audio preference, or vice versa. */
function BrowserSection() {
  const [sounds, setSounds] = useState(soundsEnabled)
  return (
    <Section title="This browser">
      <Row label="Feedback sounds">
        <input
          type="checkbox"
          checked={sounds}
          onChange={(e) => {
            const on = e.target.checked
            setSoundsEnabled(on)
            setSounds(on)
            // Immediate audition — flipping a sound toggle silently would leave "did
            // that work?" unanswered until the next notification.
            if (on) playSound('ok')
          }}
        />
        <span style={{ fontSize: 11, color: 'var(--text-mute)' }}>
          Quiet synthesised tones on notifications — session started, run finished,
          errors. Off by default.
        </span>
      </Row>
      <Row label="Theme">
        <span style={{ fontSize: 11, color: 'var(--text-mute)' }}>
          Toggles from the ◐ in the top bar; persists per browser.
        </span>
      </Row>
    </Section>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="pl-panel p-3" style={{ marginBottom: 12 }}>
      <h3 className="pl-heading" style={{ margin: '0 0 10px' }}>{title}</h3>
      {children}
    </section>
  )
}

/** Label column fixed at 200px so every value across every section starts on the same
 *  vertical line — the page reads as one grid, not a stack of ragged forms. */
function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: '200px 1fr',
        alignItems: 'center',
        columnGap: 12,
        marginBottom: 8,
      }}
    >
      <span style={{ fontSize: 12, color: 'var(--text-dim)' }}>{label}</span>
      <div className="flex items-center gap-2">{children}</div>
    </div>
  )
}
