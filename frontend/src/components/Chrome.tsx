/**
 * Persistent chrome (spec 10.2) -- the session badge and the kill switch.
 *
 * Both are permanent fixtures, and both answer a question that must never require a click.
 * The badge answers *is real money at risk right now*; the kill switch is the control that
 * has to be found without looking, which is why it never moves and is never behind a menu.
 *
 * **The badge polls with `staleTime: 0`.** The global default in `main.tsx` is five seconds,
 * which is right for a strategy list and wrong here: a badge that is allowed to be five
 * seconds stale can show `● PAPER` for a session that has already halted, or -- worse --
 * show nothing for one that has just started. The one indicator whose entire purpose is to
 * be current is the one indicator that may not be cached.
 *
 * **The kill switch has no keyboard shortcut, deliberately (spec 10.4).** Every other
 * control here would be improved by one. This one is a market order for the whole account,
 * and muscle memory must not be able to reach it. For the same reason the confirm dialog
 * focuses *Cancel*, not the confirm button: a stray Enter closes the dialog, it does not
 * fire the switch.
 *
 * **The reserved red.** `--neg` (#DC2626) appears in this file and in no other, so that when
 * it appears it means exactly one thing (spec 10.1). Negative PnL, error toasts and failed
 * runs use `--down`, which is deliberately a different token in the same hue family.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import {
  api,
  ApiError,
  formatTime,
  type ExchangeStatus,
  type KillFireResult,
  type KillState,
  type Run,
} from '../api'
import { exchangeStatusQuery, killQuery, sessionsQuery } from '../lib/queries'
import { playSound } from '../lib/sound'
import { useUi, type Tab } from '../store'

const TERMINAL = new Set(['done', 'failed', 'cancelled', 'lost'])

const TABS: Tab[] = ['Dashboard', 'Strategies', 'Runs', 'Lab', 'Data & Feed', 'Settings']

export function Chrome({ tab }: { tab: string }) {
  const theme = useUi((s) => s.theme)
  const toggleTheme = useUi((s) => s.toggleTheme)
  const setTab = useUi((s) => s.setTab)
  const [confirming, setConfirming] = useState(false)

  // Options come from `lib/queries.ts` — these three keys are registered in several
  // components, and per-key options declared twice with different intervals meant mount
  // order decided how stale the kill badge was allowed to be.
  const sessions = useQuery(sessionsQuery)
  const kill = useQuery(killQuery)
  const exchange = useQuery(exchangeStatusQuery)

  const active = (sessions.data?.sessions ?? []).filter((run) => !TERMINAL.has(run.status))
  const armed = kill.data?.armed === true
  // A failed /sessions query is *unknown*, not "zero sessions". The strategy workers are
  // separate processes: the API being unreachable says nothing about whether they are
  // trading, and the one control that must survive telemetry loss is this one. Disabling
  // the button here is the UI converting "I don't know" into "there is nothing to stop" —
  // the exact inversion the design creed forbids — so an errored query keeps it fireable.
  const sessionsUnknown = sessions.isError
  // Armed with nothing running still enables the button: the switch is a persistent piece of
  // state, and an operator looking at an armed chrome must be able to reach the dialog that
  // explains it rather than finding the control greyed out.
  const canFire = active.length > 0 || armed || sessionsUnknown

  return (
    <header
      className="px-3 shrink-0"
      style={{
        height: 48,
        background: 'var(--surface)',
        borderBottom: '1px solid var(--border)',
        // Three tracks, not one flex row: the tabs sit in an `auto` column between two
        // equal `1fr` shoulders, so they are centred on the *window* rather than on
        // whatever space the brand and the controls happen to leave. Both shoulders are
        // `min-width: 0` and clip, which is what keeps the centre still -- a session badge
        // that grows when a strategy with a long name starts must not nudge the tabs.
        display: 'grid',
        gridTemplateColumns: '1fr auto 1fr',
        alignItems: 'stretch',
        columnGap: 16,
      }}
    >
      <div className="flex items-center" style={{ minWidth: 0, overflow: 'hidden' }}>
        {/* The wordmark, and nothing beside it. It is real text in a display face, so it
            still reads as "PerpLab" to a screen reader and to find-in-page. */}
        {/* 16px, not the 13.5px the old sans name sat at. At 16 the face's own metrics
            give a 16px-tall ink box in a 48px bar -- exactly a third of the chrome, and
            it centres on the whole pixel rather than the half. */}
        <span className="pl-wordmark select-none" style={{ fontSize: 16 }}>
          PerpLab
        </span>
      </div>

      <nav className="flex items-center justify-center self-stretch" aria-label="Primary">
        {TABS.map((name) => {
          const activeTab = name === tab
          return (
            <button
              key={name}
              type="button"
              onClick={() => setTab(name)}
              aria-current={activeTab ? 'page' : undefined}
              className="px-2.5 self-stretch"
              style={{
                fontSize: 12.5,
                fontWeight: activeTab ? 550 : 450,
                border: 0,
                background: 'transparent',
                color: activeTab ? 'var(--text)' : 'var(--text-dim)',
                cursor: 'pointer',
                position: 'relative',
                transition: 'color 140ms var(--ease)',
              }}
            >
              {name}
              {/* Underline rather than a filled pill: the active tab reads as a place,
                  not a pressed button. */}
              <span
                aria-hidden="true"
                style={{
                  position: 'absolute',
                  left: 8,
                  right: 8,
                  bottom: -1,
                  height: 2,
                  borderRadius: 1,
                  background: activeTab ? 'var(--accent)' : 'transparent',
                  transition: 'background 140ms var(--ease)',
                }}
              />
            </button>
          )
        })}
      </nav>

      <div className="flex items-center justify-end gap-4" style={{ minWidth: 0 }}>
        {armed ? <ArmedBadge kill={kill.data!} /> : <SessionBadge active={active} exchange={exchange.data} />}

        <button
          className="pl-btn pl-btn-icon"
          onClick={() => window.dispatchEvent(new CustomEvent('perplab:palette'))}
          title="Jump anywhere (⌘K)"
          aria-label="Open command palette"
        >
          ⌘K
        </button>

        <button
          className="pl-btn pl-btn-icon"
          onClick={toggleTheme}
          title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
          aria-label="Toggle theme"
        >
          ◐
        </button>

        <button
          className="pl-btn"
          disabled={!canFire}
          onClick={() => setConfirming(true)}
          title={
            sessionsUnknown
              ? 'The session list is unreachable, which is not the same as empty — a strategy ' +
                'worker may still be trading. Firing tells the server to stop everything it knows about.'
              : canFire
                ? 'Stop every session, cancel every open order, and wipe the API keys from memory.'
                : 'No active session and the switch is not armed -- there is nothing to stop.'
          }
          style={{
            borderColor: 'color-mix(in srgb, var(--neg) 40%, var(--border))',
            background: armed ? 'color-mix(in srgb, var(--neg) 16%, var(--surface-2))' : undefined,
            color: 'var(--neg)',
            fontWeight: 600,
            letterSpacing: '0.04em',
            flexShrink: 0,
          }}
        >
          KILL
        </button>

        {confirming ? (
          <KillDialog
            kill={kill.data}
            active={active}
            sessionsUnknown={sessionsUnknown}
            onClose={() => setConfirming(false)}
          />
        ) : null}
      </div>
    </header>
  )
}

/* ---------------------------------------------------------------------- session badge */

/** Spec 10.2's three states, and one more the spec's list implies.
 *
 *  Green and amber are about a *session*; grey is about the *connection*. A connected
 *  exchange with nothing running is neither "LIVE" nor "no exchange connected", and rendering
 *  it as the latter would say the keys are gone when they are still in memory and still
 *  spendable. It gets the grey circle and says what it is. */
function SessionBadge({
  active,
  exchange,
}: {
  active: Run[]
  exchange: ExchangeStatus | undefined
}) {
  const live = active.find((run) => run.mode === 'live')
  const paper = active.find((run) => run.mode === 'paper')
  const shown = live ?? paper
  if (shown) {
    const colour = live ? 'var(--pos)' : 'var(--warn)'
    const others = active.length - 1
    return (
      <span
        className="mono flex items-center gap-2"
        style={{ fontSize: 11, color: colour, minWidth: 0 }}
        title={
          live
            ? 'A live session is running. Real money is at risk right now.'
            : 'A paper session is running against live market data. No real money is at risk.'
        }
      >
        {/* The dot breathes while a session runs -- "running" and "was running when this
            last rendered" should not look identical. */}
        <span className="pl-dot pl-dot-live" style={{ background: colour, color: colour, flexShrink: 0 }} />
        <span style={{ fontWeight: 600, flexShrink: 0 }}>{live ? 'LIVE' : 'PAPER'}</span>
        {/* Only the *name* is allowed to truncate, and it truncates rather than widening
            the badge: the state word and the dot are the part that must always be legible,
            and a growing badge would shove the centred tabs sideways. The title above
            carries the full text. */}
        <span
          style={{
            color: 'var(--text-dim)',
            minWidth: 0,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {shown.strategy_name}/{shown.symbols.join(',') || '—'}
          {others > 0 ? ` +${others}` : ''}
        </span>
      </span>
    )
  }
  if (exchange?.connected) {
    return (
      <span
        className="mono flex items-center gap-2"
        style={{
          fontSize: 11,
          color: 'var(--text-dim)',
          minWidth: 0,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
        title="Keys are held in memory and the account is reachable, but no strategy is running."
      >
        <span className="pl-dot" style={{ flexShrink: 0 }} />
        {exchange.alias ?? 'connected'}
        {exchange.endpoint ? ` · ${exchange.endpoint}` : ''} · no session
      </span>
    )
  }
  return (
    <span
      className="mono flex items-center gap-2"
      style={{ fontSize: 11, color: 'var(--text-mute)', whiteSpace: 'nowrap' }}
    >
      <span
        className="pl-dot"
        style={{ background: 'transparent', border: '1px solid var(--text-mute)', flexShrink: 0 }}
      />
      No exchange connected
    </span>
  )
}

/** What the chrome shows once the switch has fired.
 *
 *  It replaces the session badge rather than sitting beside it. Spec 7.6 makes an armed
 *  switch a state of the whole platform -- nothing can start until it is un-armed -- so the
 *  slot that answers "what is running" has exactly one honest answer while it is armed. */
function ArmedBadge({ kill }: { kill: KillState }) {
  const client = useQueryClient()
  const notify = useUi((s) => s.notify)
  const unarm = useMutation({
    mutationFn: api.unarmKill,
    onSuccess: () => {
      client.invalidateQueries({ queryKey: ['kill'] })
      notify('Kill switch un-armed. Sessions can start again.', 'warn')
    },
    onError: (error) => notify(error instanceof ApiError ? error.message : String(error), 'error'),
  })
  return (
    <span className="mono flex items-center gap-2" style={{ fontSize: 11 }}>
      <span
        className="flex items-center gap-1.5"
        style={{ color: 'var(--neg)', fontWeight: 600 }}
        title={
          `${kill.detail ?? 'The kill switch is armed.'}\n` +
          `Armed ${kill.armed_ms ? formatTime(kill.armed_ms) : 'at an unrecorded time'}. ` +
          'No live or paper session can start until it is un-armed (spec 7.6).'
        }
      >
        <span>■</span> KILL ARMED
      </span>
      <span style={{ color: 'var(--text-dim)' }}>{kill.trigger ?? 'operator'}</span>
      <button className="pl-btn" style={{ height: 22 }} onClick={() => unarm.mutate('operator')} disabled={unarm.isPending}>
        {unarm.isPending ? 'Un-arming…' : 'Un-arm'}
      </button>
    </span>
  )
}

/* ----------------------------------------------------------------------- confirm dialog */

/**
 * The confirmation, which states what will happen before it happens (spec 10.2).
 *
 * The list is spec 7's five steps, and step 3 is rendered from `kill.flatten` rather than
 * being described in general terms. "This may close your positions" is not a statement an
 * operator can act on -- the two behaviours have opposite risks, and which one is armed lives
 * in Settings where it cannot be seen from here. The button label repeats the answer, so the
 * last thing read before the click is the behaviour being bought.
 */
function KillDialog({
  kill,
  active,
  sessionsUnknown,
  onClose,
}: {
  /** The armed trip, when there is one -- so the dialog can report what already fired
   *  rather than silently offering to fire it again. The *behaviour* to arm comes from
   *  Settings, below. */
  kill: KillState | undefined
  active: Run[]
  /** The /sessions query failed: the list below is unknown, not empty. */
  sessionsUnknown: boolean
  onClose: () => void
}) {
  const client = useQueryClient()
  const notify = useUi((s) => s.notify)
  // **The armed behaviour comes from Settings**, which is where this docstring always
  // said it lived. It used to be read off `kill.flatten` -- the *armed trip's* record --
  // which is `undefined` whenever nothing is armed, i.e. every time an operator opens
  // this dialog to fire it. The button below is disabled while `flatten` is undefined,
  // so the kill switch could not be fired from the UI at all.
  //
  // `undefined` while Settings is still loading is still the right refusal: assuming
  // cancel-only would describe the safer behaviour while possibly performing the
  // destructive one. A *failed* Settings read is different — refusing forever would leave
  // the one safety control unfireable behind a telemetry fault, so that case falls back
  // to cancel-only (the safer behaviour, spec 7.3's default), states the failure, and
  // offers a retry.
  const settings = useQuery({ queryKey: ['settings'], queryFn: api.settings, retry: 0 })
  const settingsFailed = settings.isError
  const flatten =
    settings.data != null ? settings.data.settings.kill_switch_flatten : settingsFailed ? false : undefined
  const alreadyArmed = kill?.armed === true

  // The partial-failure answer, held so the dialog can stay open and show it. A toast is
  // the wrong shape for "a session could not be stopped and may still be trading" — it
  // disappears on its own schedule, and this must not.
  const [failed, setFailed] = useState<KillFireResult | null>(null)

  const fire = useMutation({
    mutationFn: () => api.fireKill(flatten === true),
    onSuccess: (result) => {
      client.invalidateQueries({ queryKey: ['kill'] })
      client.invalidateQueries({ queryKey: ['sessions'] })
      client.invalidateQueries({ queryKey: ['runs'] })
      if (result.stop_failures.length > 0) {
        // NOT a success. The switch armed and the keys were handled, but at least one
        // session was never asked to stop. The dialog stays open with the banner below;
        // the toast is only the attention-getter.
        setFailed(result)
        notify(
          `Kill switch fired, but ${result.stop_failures.length} session(s) could not be ` +
            'stopped and may still be trading — see the kill dialog.',
          'error',
        )
        return
      }
      onClose()
      const stopped = result.stopped.length
      notify(
        (stopped > 0
          ? `Kill switch fired: ${stopped} session${stopped === 1 ? '' : 's'} told to stop, `
          : 'Kill switch fired: no session was running, ') +
          (result.kill.flatten
            ? 'orders cancelled and positions closing at market.'
            : 'orders cancelled, positions left open.') +
          (result.keys_wiped ? ' API keys wiped from memory.' : ' No API key was held.'),
        'error',
      )
    },
    onError: (error) => notify(error instanceof ApiError ? error.message : String(error), 'error'),
  })

  // Esc closes modals (spec 10.4). Closing is the safe direction, so this is the one keyboard
  // path the kill switch has -- and it cancels.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <div
      className="pl-scrim"
      style={{ zIndex: 70, paddingTop: 80 }}
      onClick={onClose}
      role="dialog"
      aria-modal="true"
    >
      <div
        className="pl-modal p-4 flex flex-col gap-3"
        style={{
          width: 'min(520px, calc(100vw - 32px))',
          borderColor: 'color-mix(in srgb, var(--neg) 55%, var(--border))',
          textAlign: 'left',
        }}
        onClick={(event) => event.stopPropagation()}
      >
        <h2 style={{ margin: 0, fontSize: 14, color: 'var(--neg)' }}>Fire the kill switch?</h2>

        <p style={{ margin: 0, fontSize: 12, color: 'var(--text-dim)' }}>
          {sessionsUnknown
            ? 'The session list is unreachable, so what is running is unknown — not necessarily nothing. Firing tells the server to stop every session it knows about.'
            : active.length === 0
              ? 'Nothing is running. Firing now cancels any order still working on the exchange and re-arms the switch.'
              : `This stops ${active.length} running session${active.length === 1 ? '' : 's'}:`}
        </p>
        {active.length ? (
          <ul className="mono" style={{ margin: 0, paddingLeft: 18, fontSize: 11, color: 'var(--text-dim)' }}>
            {active.map((run) => (
              <li key={run.id}>
                #{run.id} {run.strategy_name} · {run.symbols.join(',')} ·{' '}
                <span style={{ color: run.mode === 'live' ? 'var(--pos)' : 'var(--warn)' }}>{run.mode}</span>
              </li>
            ))}
          </ul>
        ) : null}

        <ol style={{ margin: 0, paddingLeft: 18, fontSize: 12, color: 'var(--text-dim)' }}>
          <li>Stop all live and paper strategy processes.</li>
          <li>Cancel every open order on the exchange.</li>
          <li style={{ color: 'var(--text)' }}>
            {settingsFailed ? (
              <>
                <b>Leave open positions open</b> — cancel-only. Settings could not be read (
                {settings.error instanceof ApiError ? settings.error.message : String(settings.error)}
                ), so the safer of the two behaviours applies instead of the one you may have
                armed there.{' '}
                <button
                  className="pl-btn"
                  style={{ height: 20, fontSize: 10, padding: '0 6px' }}
                  onClick={() => settings.refetch()}
                  disabled={settings.isFetching}
                >
                  {settings.isFetching ? 'Retrying…' : 'Retry Settings'}
                </button>
              </>
            ) : flatten === undefined ? (
              'Reading the armed behaviour from the server…'
            ) : flatten ? (
              <>
                <b>Close all open positions at market.</b> Settings has close-all armed. In a
                fast market the exit price is whatever the book holds at that instant.
              </>
            ) : (
              <>
                <b>Leave open positions open</b> -- cancel-only, spec 7.3's default. Exposure
                survives; only the orders go.
              </>
            )}
          </li>
          <li>Wipe the API keys from memory, ending the key session.</li>
          <li>Write a KILL_SWITCH event to every active run's log.</li>
        </ol>

        <p style={{ margin: 0, fontSize: 11, color: 'var(--text-mute)' }}>
          The switch stays armed afterwards. No live or paper session can start until it is
          explicitly un-armed (spec 7.6).
        </p>

        {alreadyArmed ? (
          <p style={{ margin: 0, fontSize: 11, color: 'var(--warn)' }}>
            △ The switch is <b>already armed</b>
            {kill?.trigger ? ` (${kill.trigger})` : ''} — firing again repeats the stop and
            re-arms it. Un-arm from the badge when the cause has been dealt with.
          </p>
        ) : null}

        {failed ? (
          // The partial-failure banner (never a success toast): the switch armed, but the
          // sessions below were never asked to stop. This stays on screen until the
          // operator closes it — it is the most important line in the dialog.
          <div
            className="p-2"
            style={{
              border: '1px solid var(--neg)',
              borderRadius: 4,
              background: 'color-mix(in srgb, var(--neg) 10%, var(--surface-2))',
            }}
          >
            <p style={{ margin: 0, fontSize: 12, color: 'var(--neg)', fontWeight: 600 }}>
              The switch armed, but {failed.stop_failures.length} session
              {failed.stop_failures.length === 1 ? ' was' : 's were'} NOT stopped and may
              still be trading:
            </p>
            <ul className="mono" style={{ margin: '4px 0 0', paddingLeft: 18, fontSize: 11, color: 'var(--text-dim)' }}>
              {failed.stop_failures.map((line, index) => (
                <li key={index}>{line}</li>
              ))}
            </ul>
            <p style={{ margin: '4px 0 0', fontSize: 11, color: 'var(--text-dim)' }}>
              {failed.stopped.length
                ? `Session${failed.stopped.length === 1 ? '' : 's'} #${failed.stopped.join(', #')} ${
                    failed.stopped.length === 1 ? 'was' : 'were'
                  } told to stop. `
                : 'No session was successfully told to stop. '}
              {failed.keys_wiped
                ? 'The API keys were wiped.'
                : 'No API key was held to wipe.'}{' '}
              Fire again to retry, stop the listed sessions from their run pages, or
              terminate them from the Runs list as a last resort.
            </p>
          </div>
        ) : null}

        <div className="flex items-center gap-2">
          {/* Cancel is focused, not the confirm button. A dialog that fires on Enter is a
              keyboard shortcut for the kill switch by another name. */}
          <button className="pl-btn" onClick={onClose} autoFocus>
            Cancel
          </button>
          <span className="flex-1" />
          <button
            className="pl-btn"
            disabled={flatten === undefined || fire.isPending}
            onClick={() => fire.mutate()}
            style={{
              borderColor: 'var(--neg)',
              color: 'var(--neg)',
              fontWeight: 600,
            }}
          >
            {fire.isPending
              ? 'Firing…'
              : flatten === undefined
                ? 'Waiting for the armed behaviour…'
                : failed
                  ? 'Fire again — retry stopping every session'
                  : flatten
                    ? 'Fire — cancel orders and CLOSE ALL positions'
                    : 'Fire — cancel orders only, leave positions'}
          </button>
        </div>
      </div>
    </div>
  )
}

/* -------------------------------------------------------------------------------- toast */

/** Toast for mutation outcomes (spec 10.4: completion emits a toast).
 *
 *  Also the audio funnel: every notification in the app arrives through `notify`, so
 *  playing the (optional, default-off) feedback sound here means no component ever has
 *  to remember to. */
export function Toast() {
  const toast = useUi((s) => s.toast)
  const clear = useUi((s) => s.clearToast)

  useEffect(() => {
    if (!toast) return
    playSound(toast.kind)
    const timer = window.setTimeout(clear, toast.kind === 'error' ? 8000 : 3500)
    return () => window.clearTimeout(timer)
  }, [toast, clear])

  if (!toast) return null
  const colour =
    toast.kind === 'error' ? 'var(--down)' : toast.kind === 'warn' ? 'var(--warn)' : 'var(--pos)'
  return (
    <div
      role="status"
      className="pl-panel pl-toast flex items-start gap-2 px-3 py-2"
      style={{
        maxWidth: 'min(460px, calc(100vw - 32px))',
        borderColor: `color-mix(in srgb, ${colour} 40%, var(--border))`,
        background: 'var(--surface-2)',
        cursor: 'pointer',
      }}
      onClick={clear}
    >
      <span style={{ color: colour }}>{toast.kind === 'ok' ? '✓' : toast.kind === 'warn' ? '△' : '✕'}</span>
      <span style={{ fontSize: 12 }}>{toast.text}</span>
    </div>
  )
}
