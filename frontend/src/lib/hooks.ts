/**
 * Shared micro-interaction hooks. Small on purpose: each one earns its place by being
 * used from more than one component.
 */

import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * A CSS class that flashes an element toward the direction a live number just moved —
 * `pl-flash-up` / `pl-flash-down` from styles.css — then settles back to its own colour.
 *
 * The first observation never flashes: a number *appearing* is not a number *moving*, and
 * a dashboard that lights up green on mount is claiming a gain that did not happen.
 */
export function useValueFlash(value: number | null | undefined): string {
  const prev = useRef<number | null | undefined>(undefined)
  const [cls, setCls] = useState('')
  const timer = useRef<number | undefined>(undefined)

  useEffect(() => {
    const before = prev.current
    prev.current = value
    if (before === undefined || before == null || value == null || value === before) return
    // Clear, then set on the next frame, so a second move in the same direction restarts
    // the animation instead of being swallowed by an identical className.
    setCls('')
    const direction = value > before ? 'pl-flash-up' : 'pl-flash-down'
    const raf = requestAnimationFrame(() => setCls(direction))
    if (timer.current !== undefined) window.clearTimeout(timer.current)
    timer.current = window.setTimeout(() => setCls(''), 950)
    return () => cancelAnimationFrame(raf)
  }, [value])

  // The settle-back timer outlives the per-value cleanup on purpose (clearing it there
  // would freeze the flash class on), so it needs its own unmount cleanup: this hook sits
  // on trees that unmount on every tab switch, and an orphaned 950 ms timer per switch is
  // a setState against an unmounted component, once per KPI, forever.
  useEffect(
    () => () => {
      if (timer.current !== undefined) window.clearTimeout(timer.current)
    },
    [],
  )

  return cls
}

/**
 * A value that only settles after `delayMs` of quiet.
 *
 * Exists for inputs that drive a server query per change: coverage lookups keyed on a
 * symbol box were issuing one request per keystroke ("B", "BT", "BTC", …), each minting a
 * fresh cache entry. The timer is cleaned up on unmount and on every change, so no state
 * is set after the tree is gone.
 */
export function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value)
  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(value), delayMs)
    return () => window.clearTimeout(timer)
  }, [value, delayMs])
  return debounced
}

/**
 * Copy-to-clipboard with a transient "copied" state for the button that triggered it.
 * Falls back through `execCommand` for non-secure contexts; this app is loopback-only,
 * which browsers treat as secure, so the fallback is belt and braces.
 */
export function useCopy(): { copied: boolean; copy: (text: string) => void } {
  const [copied, setCopied] = useState(false)
  const timer = useRef<number | undefined>(undefined)

  // Same unmount discipline as `useValueFlash`: the 1400 ms "copied" reset must not fire
  // into a component that a tab switch has already unmounted.
  useEffect(
    () => () => {
      if (timer.current !== undefined) window.clearTimeout(timer.current)
    },
    [],
  )

  const copy = useCallback((text: string) => {
    const done = () => {
      setCopied(true)
      if (timer.current !== undefined) window.clearTimeout(timer.current)
      timer.current = window.setTimeout(() => setCopied(false), 1400)
    }
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(text).then(done, done)
    } else {
      const area = document.createElement('textarea')
      area.value = text
      area.style.position = 'fixed'
      area.style.opacity = '0'
      document.body.appendChild(area)
      area.select()
      document.execCommand('copy')
      area.remove()
      done()
    }
  }, [])

  return { copied, copy }
}
