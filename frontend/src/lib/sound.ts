/**
 * Optional feedback sounds — synthesised, not sampled.
 *
 * WebAudio oscillators instead of audio files, for the same reason the charts are
 * hand-drawn SVG: this app must work with no network and carries no binary assets.
 *
 * **Off by default, and silent until a click.** Browsers refuse AudioContext before a
 * user gesture anyway, and a trading platform that beeps unprompted on first open has
 * misread the room. The toggle lives in Settings and persists per browser in
 * localStorage — it is a preference of this chair, not of the account, so it does not
 * belong in the server's settings store.
 */

const KEY = 'perplab.sounds'

export function soundsEnabled(): boolean {
  try {
    return localStorage.getItem(KEY) === 'on'
  } catch {
    return false
  }
}

export function setSoundsEnabled(on: boolean): void {
  try {
    localStorage.setItem(KEY, on ? 'on' : 'off')
  } catch {
    /* private browsing; the preference simply does not persist */
  }
}

let context: AudioContext | null = null

function ctx(): AudioContext | null {
  if (typeof AudioContext === 'undefined') return null
  if (context == null) context = new AudioContext()
  return context
}

/** One short tone. Quiet by design — feedback, not an alarm. */
function tone(freq: number, ms: number, delayMs = 0, gain = 0.04): void {
  const audio = ctx()
  if (audio == null) return
  const start = audio.currentTime + delayMs / 1000
  const osc = audio.createOscillator()
  const amp = audio.createGain()
  osc.type = 'sine'
  osc.frequency.value = freq
  amp.gain.setValueAtTime(0, start)
  amp.gain.linearRampToValueAtTime(gain, start + 0.01)
  amp.gain.exponentialRampToValueAtTime(0.0001, start + ms / 1000)
  osc.connect(amp)
  amp.connect(audio.destination)
  osc.start(start)
  osc.stop(start + ms / 1000 + 0.05)
}

export type SoundKind = 'ok' | 'warn' | 'error'

/** Play the sound for an event kind, if sounds are on. Safe to call unconditionally. */
export function playSound(kind: SoundKind): void {
  if (!soundsEnabled()) return
  try {
    if (kind === 'ok') {
      tone(660, 90)
      tone(880, 110, 70)
    } else if (kind === 'warn') {
      tone(440, 140)
    } else {
      tone(220, 160)
      tone(196, 180, 120)
    }
  } catch {
    /* an audio failure must never break the UI */
  }
}
