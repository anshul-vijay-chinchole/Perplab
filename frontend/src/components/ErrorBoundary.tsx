/**
 * The last thing between a render fault and a blank window.
 *
 * React unmounts the entire root when a render throws and nothing catches it. With no
 * boundary anywhere in this app that meant one bad field in one panel produced a white
 * page -- and it did: `LiveMonitor` read a `risk.usage` array the server has never sent,
 * `undefined.length` threw on the first monitor snapshot, and the operator was left
 * staring at nothing while a live session held an open position at the exchange.
 *
 * **The failure mode this exists to prevent is not the crash, it is the ambiguity.** A
 * blank window says nothing about whether the session is still running, whether the order
 * went in, or whether the position is still open. So the fallback's first job is to state
 * what is *not* affected: this is a drawing fault in the browser, the session runs in its
 * own process, and nothing here has closed a position or cancelled an order. Its second
 * job is to point at the controls that still work, because the one thing an operator must
 * never lose is the ability to stop a session.
 *
 * **It shows the error text and the component stack rather than a friendly apology.** This
 * is a single-user tool whose user writes the code; "something went wrong" would cost the
 * exact information needed to fix it, and there is no support desk to escalate to.
 *
 * `resetKey` re-arms the boundary when it changes -- the run being viewed, the tab being
 * shown -- so navigating away from a broken panel does not leave the error frozen over a
 * page that would now render.
 */

import React from 'react'

interface Props {
  children: React.ReactNode
  /** What broke, in the operator's terms: "the live monitor", "PerpLab". */
  what: string
  /** Changing this clears the error and retries the subtree. */
  resetKey?: unknown
  /** Extra reassurance specific to the subtree; the generic line is always shown. */
  note?: string
  /** Runs when "Try again" is pressed, *before* the subtree remounts.
   *
   *  Exists so the caller can evict the React Query cache. Without it "Try again" only
   *  cleared this boundary's local state: the payload that threw was still cached (5-min
   *  `gcTime`), the remounted subtree read it synchronously, and the boundary re-armed
   *  into the same error on the same frame — a retry button that could never work. */
  onReset?: () => void
}

interface State {
  error: Error | null
  stack: string
}

export class ErrorBoundary extends React.Component<Props, State> {
  state: State = { error: null, stack: '' }

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { error }
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    // Kept on the console too. The panel below is what the operator reads; this is what
    // survives a copy-paste into an issue, with the full stack the panel truncates.
    console.error(`[PerpLab] render fault in ${this.props.what}:`, error, info)
    this.setState({ stack: info.componentStack ?? '' })
  }

  componentDidUpdate(prev: Props) {
    if (prev.resetKey !== this.props.resetKey && this.state.error !== null) {
      this.setState({ error: null, stack: '' })
    }
  }

  render() {
    const { error, stack } = this.state
    if (error === null) return this.props.children

    return (
      <div
        className="pl-panel p-3 flex flex-col gap-2"
        style={{ borderColor: 'color-mix(in srgb, var(--down) 45%, var(--border))' }}
      >
        <h2 style={{ margin: 0, fontSize: 13, color: 'var(--down)' }}>
          {this.props.what} failed to draw
        </h2>
        <p style={{ margin: 0, fontSize: 12, color: 'var(--text-dim)' }}>
          This is a fault in the browser, not in the engine. Sessions run in their own
          processes and are unaffected: nothing here has stopped a strategy, cancelled an
          order or closed a position.
          {this.props.note ? ` ${this.props.note}` : ''}
        </p>
        <pre
          className="mono"
          style={{
            margin: 0,
            padding: 8,
            fontSize: 11,
            color: 'var(--down)',
            background: 'var(--surface-2)',
            borderRadius: 4,
            whiteSpace: 'pre-wrap',
            overflowX: 'auto',
          }}
        >
          {error.message || String(error)}
          {stack ? `\n${stack.trim().split('\n').slice(0, 8).join('\n')}` : ''}
        </pre>
        <div className="flex items-center gap-2">
          <button
            className="pl-btn"
            onClick={() => {
              // Cache first, then re-arm: the remount reads the caches synchronously, so
              // evicting after clearing the error would retry against the bad payload.
              this.props.onReset?.()
              this.setState({ error: null, stack: '' })
            }}
          >
            Try again
          </button>
          <button className="pl-btn" onClick={() => window.location.reload()}>
            Reload PerpLab
          </button>
        </div>
      </div>
    )
  }
}
