/**
 * Run charts (spec 10.3), hand-drawn as SVG.
 *
 * No charting library. Not asceticism — the constraint is that this app must work with no
 * network at all, so anything it renders has to be in the bundle, and the two charts a
 * results page needs are a polyline and a filled area. A library would add hundreds of
 * kilobytes and its own opinions about what a "series" is.
 *
 * **A fixed viewBox with `vector-effect="non-scaling-stroke"`.** The SVG scales to its
 * container, so a stroke declared as 1 would scale with it and a wide chart would draw a
 * fat line while a narrow one drew a hairline. The attribute pins stroke width to device
 * pixels regardless of the transform.
 *
 * **Colour always has a second signal.** Spec 10.1: colour is meaning, never decoration, and
 * colour alone fails for a colour-blind reader and in a greyscale screenshot. Trade markers
 * are shaped by direction (▲ entry, ▼ exit) as well as coloured by outcome, and the
 * attribution bar is labelled.
 */

import { useCallback, useId, useState } from 'react'
import { formatTime, money, pct } from '../api'
import type { Attribution, Trade } from '../api'

const W = 1000

/* ------------------------------------------------------------------------- hover */

/**
 * Shared hover state for a time-series SVG stretched with `preserveAspectRatio="none"`.
 *
 * Mouse pixels are not SVG user units under that stretch, so the maths stays in
 * fractions: pointer x as a fraction of the container maps to a timestamp, and a binary
 * search finds the nearest sample. The crosshair is drawn back inside the SVG at the
 * sample's own scaled x, and the value readout is an HTML overlay — which also keeps
 * both out of PNG exports, since they only exist while a pointer is over the chart.
 */
function useSeriesHover(ts: number[]) {
  const [index, setIndex] = useState<number | null>(null)
  const [frac, setFrac] = useState(0)

  const onMove = useCallback(
    (event: React.MouseEvent<HTMLDivElement>) => {
      if (ts.length < 2) return
      const rect = event.currentTarget.getBoundingClientRect()
      // A zero-width rect (hidden tab, mid-layout) would make the fraction NaN and pin
      // the crosshair to a sample the pointer is nowhere near.
      if (rect.width <= 0) return
      const fraction = Math.min(1, Math.max(0, (event.clientX - rect.left) / rect.width))
      const t0 = ts[0]
      const t1 = ts[ts.length - 1]
      const target = t0 + fraction * (t1 - t0)
      let lo = 0
      let hi = ts.length - 1
      while (hi - lo > 1) {
        const mid = (lo + hi) >> 1
        if (ts[mid] < target) lo = mid
        else hi = mid
      }
      const nearest = target - ts[lo] <= ts[hi] - target ? lo : hi
      setIndex(nearest)
      setFrac(fraction)
    },
    [ts],
  )
  const onLeave = useCallback(() => setIndex(null), [])

  return { index, frac, onMove, onLeave }
}

/** The floating value readout. Flips sides past the midpoint so it never leaves the frame. */
function HoverReadout({ frac, lines }: { frac: number; lines: Array<[string, string, string?]> }) {
  const right = frac > 0.55
  return (
    <div
      className="mono pl-panel"
      style={{
        position: 'absolute',
        top: 6,
        left: `${frac * 100}%`,
        transform: right ? 'translateX(calc(-100% - 10px))' : 'translateX(10px)',
        padding: '5px 8px',
        fontSize: 11,
        lineHeight: 1.6,
        background: 'var(--surface-2)',
        boxShadow: 'var(--shadow-1)',
        pointerEvents: 'none',
        whiteSpace: 'nowrap',
        zIndex: 5,
      }}
    >
      {lines.map(([label, value, colour]) => (
        <div key={label} className="flex items-center gap-3">
          <span style={{ color: 'var(--text-mute)' }}>{label}</span>
          <span style={{ marginLeft: 'auto', color: colour ?? 'var(--text)' }}>{value}</span>
        </div>
      ))}
    </div>
  )
}

interface Scale {
  (value: number): number
}

function linear(domain: [number, number], range: [number, number]): Scale {
  const [d0, d1] = domain
  const [r0, r1] = range
  const span = d1 - d0
  // A flat series has no span to divide by. Pinning it to the middle of the range is the
  // honest rendering: the value never moved, so neither should the line.
  if (!Number.isFinite(span) || span === 0) return () => (r0 + r1) / 2
  return (value: number) => r0 + ((value - d0) / span) * (r1 - r0)
}

function path(xs: number[], ys: number[], x: Scale, y: Scale): string {
  let out = ''
  for (let i = 0; i < xs.length; i += 1) {
    out += `${i === 0 ? 'M' : 'L'}${x(xs[i]).toFixed(2)} ${y(ys[i]).toFixed(2)}`
  }
  return out
}

/** The area between a gapped series and `baseline`, as one closed shape per defined run. */
function gappedArea(
  xs: number[],
  ys: (number | null)[],
  x: Scale,
  y: Scale,
  baseline: number,
): string {
  let out = ''
  let run: number[] = []
  const flush = () => {
    if (run.length === 0) return
    const first = run[0]
    const last = run[run.length - 1]
    let shape = ''
    for (let k = 0; k < run.length; k += 1) {
      const i = run[k]
      shape += `${k === 0 ? 'M' : 'L'}${x(xs[i]).toFixed(2)} ${y(ys[i] as number).toFixed(2)}`
    }
    shape += `L${x(xs[last]).toFixed(2)} ${baseline.toFixed(2)}`
    shape += `L${x(xs[first]).toFixed(2)} ${baseline.toFixed(2)}Z`
    out += shape
    run = []
  }
  for (let i = 0; i < xs.length; i += 1) {
    if (ys[i] == null) flush()
    else run.push(i)
  }
  flush()
  return out
}

/** `path`, but a `null` breaks the line instead of being drawn.
 *
 * Drawdown is `null` wherever the running peak was non-positive, which the server documents
 * as undefined rather than zero. Feeding those to `path` drew them at `y(null) === y(0)` —
 * a confident flat line along the zero axis exactly where the account was at or below
 * nothing, which is the one place the chart most needs to not invent a number. Each run of
 * defined samples starts a fresh subpath, so a gap reads as a gap.
 */
function gappedPath(xs: number[], ys: (number | null)[], x: Scale, y: Scale): string {
  let out = ''
  let pen = false
  for (let i = 0; i < xs.length; i += 1) {
    const value = ys[i]
    if (value == null) {
      pen = false
      continue
    }
    out += `${pen ? 'L' : 'M'}${x(xs[i]).toFixed(2)} ${y(value).toFixed(2)}`
    pen = true
  }
  return out
}

function extent(values: number[]): [number, number] {
  let lo = Infinity
  let hi = -Infinity
  for (const value of values) {
    if (value < lo) lo = value
    if (value > hi) hi = value
  }
  if (!Number.isFinite(lo)) return [0, 1]
  return [lo, hi]
}

function padded([lo, hi]: [number, number], fraction = 0.06): [number, number] {
  const pad = (hi - lo) * fraction || Math.abs(hi) * 0.01 || 1
  return [lo - pad, hi + pad]
}

function GridLines({ y, ticks, width }: { y: Scale; ticks: number[]; width: number }) {
  return (
    <g>
      {ticks.map((tick) => (
        <line
          key={tick}
          x1={0}
          x2={width}
          y1={y(tick)}
          y2={y(tick)}
          stroke="var(--border)"
          strokeWidth={1}
          vectorEffect="non-scaling-stroke"
          opacity={0.6}
        />
      ))}
    </g>
  )
}

function niceTicks([lo, hi]: [number, number], count = 4): number[] {
  if (!Number.isFinite(lo) || !Number.isFinite(hi) || hi <= lo) return []
  const raw = (hi - lo) / count
  const magnitude = 10 ** Math.floor(Math.log10(raw))
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((s) => s >= raw) ?? magnitude * 10
  const out: number[] = []
  for (let tick = Math.ceil(lo / step) * step; tick <= hi; tick += step) out.push(tick)
  return out
}

/**
 * Equity curve with the drawdown shaded beneath it (spec 10.3).
 *
 * Two stacked panels sharing one time axis rather than a twin y-axis. A drawdown drawn on a
 * second scale over the same area invites reading the crossing point as meaningful, and it
 * is not — they are different units.
 */
export function EquityChart({
  ts,
  equity,
  drawdown,
  height = 260,
}: {
  ts: number[]
  equity: number[]
  /** `null` where the running peak was non-positive — undefined, not zero. See `gappedPath`. */
  drawdown: (number | null)[]
  height?: number
}) {
  const clip = useId()
  const hover = useSeriesHover(ts)
  if (ts.length < 2) return <Empty height={height} label="No equity samples" />

  const equityH = Math.round(height * 0.68)
  const ddTop = equityH + 14
  const ddH = height - ddTop - 2

  const x = linear([ts[0], ts[ts.length - 1]], [0, W])
  const yDomain = padded(extent(equity))
  const y = linear(yDomain, [equityH, 4])
  // Nulls are excluded rather than spread into `Math.min`, where they coerce to 0 and so
  // quietly act as "no drawdown here" — the opposite of what an undefined sample means.
  const ddDefined = drawdown.filter((v): v is number => v != null)
  const ddDomain: [number, number] = [Math.min(...ddDefined, -0.0001), 0]
  const yDd = linear(ddDomain, [ddTop + ddH, ddTop])

  const start = equity[0]
  const end = equity[equity.length - 1]
  const up = end >= start

  // One closed shape per run of defined samples, rather than one shape closed back to the
  // first timestamp. Closing across a gap would fill the undefined stretch with the same
  // red as a measured drawdown.
  const area = gappedArea(ts, drawdown, x, yDd, yDd(0))

  const i = hover.index
  return (
    <div style={{ position: 'relative' }} onMouseMove={hover.onMove} onMouseLeave={hover.onLeave}>
      <svg
        viewBox={`0 0 ${W} ${height}`}
        style={{ width: '100%', height, display: 'block' }}
        preserveAspectRatio="none"
        role="img"
        aria-label="Equity curve with drawdown"
      >
        <defs>
          <clipPath id={clip}>
            <rect x={0} y={0} width={W} height={height} />
          </clipPath>
        </defs>
        <g clipPath={`url(#${clip})`}>
          <GridLines y={y} ticks={niceTicks(yDomain)} width={W} />
          {/* The opening balance, so "made money" is a line crossing rather than a mental
              subtraction. */}
          <line
            x1={0}
            x2={W}
            y1={y(start)}
            y2={y(start)}
            stroke="var(--text-mute)"
            strokeWidth={1}
            strokeDasharray="3 3"
            vectorEffect="non-scaling-stroke"
          />
          <path
            d={path(ts, equity, x, y)}
            fill="none"
            stroke={up ? 'var(--pos)' : 'var(--down)'}
            strokeWidth={1.5}
            vectorEffect="non-scaling-stroke"
          />
          <path d={area} fill="var(--down)" opacity={0.22} stroke="none" />
          <path
            d={gappedPath(ts, drawdown, x, yDd)}
            fill="none"
            stroke="var(--down)"
            strokeWidth={1}
            vectorEffect="non-scaling-stroke"
            opacity={0.8}
          />
          <line
            x1={0}
            x2={W}
            y1={yDd(0)}
            y2={yDd(0)}
            stroke="var(--border)"
            strokeWidth={1}
            vectorEffect="non-scaling-stroke"
          />
          {i != null ? (
            <g>
              <line
                x1={x(ts[i])}
                x2={x(ts[i])}
                y1={0}
                y2={height}
                stroke="var(--text-mute)"
                strokeWidth={1}
                vectorEffect="non-scaling-stroke"
                opacity={0.7}
              />
              <circle cx={x(ts[i])} cy={y(equity[i])} r={3} fill={up ? 'var(--pos)' : 'var(--down)'} stroke="var(--bg)" strokeWidth={1} />
              {/* No dot where drawdown is undefined: one at `yDd(0)` would sit on the zero
                  axis and read as "no drawdown here", which is a claim, not an absence. */}
              {drawdown[i] == null ? null : (
                <circle cx={x(ts[i])} cy={yDd(drawdown[i] as number)} r={2.5} fill="var(--down)" stroke="var(--bg)" strokeWidth={1} />
              )}
            </g>
          ) : null}
        </g>
      </svg>
      {i != null ? (
        <HoverReadout
          frac={hover.frac}
          lines={[
            ['time', formatTime(ts[i])],
            ['equity', money(String(equity[i])), equity[i] >= start ? 'var(--pos)' : 'var(--down)'],
            ['drawdown', pct(drawdown[i]), (drawdown[i] ?? 0) < 0 ? 'var(--down)' : undefined],
          ]}
        />
      ) : null}
    </div>
  )
}

/**
 * Price with trade markers overlaid (spec 10.3).
 *
 * Markers are placed at the trade's own entry and exit *prices*, not at the sampled price
 * nearest in time. The price series is downsampled for rendering and a marker snapped to a
 * kept sample would sit visibly off the line it belongs to — and worse, would move when the
 * window was resized.
 */
export function PriceChart({
  ts,
  close,
  trades,
  height = 220,
}: {
  ts: number[]
  close: number[]
  trades: Trade[]
  height?: number
}) {
  const hover = useSeriesHover(ts)
  if (ts.length < 2) return <Empty height={height} label="No price series" />

  const x = linear([ts[0], ts[ts.length - 1]], [0, W])
  const priceValues = close.slice()
  for (const trade of trades) {
    priceValues.push(Number(trade.entry_price))
    if (trade.exit_price) priceValues.push(Number(trade.exit_price))
  }
  const yDomain = padded(extent(priceValues.filter(Number.isFinite)))
  const y = linear(yDomain, [height - 6, 6])

  const i = hover.index
  return (
    <div style={{ position: 'relative' }} onMouseMove={hover.onMove} onMouseLeave={hover.onLeave}>
    <svg
      viewBox={`0 0 ${W} ${height}`}
      style={{ width: '100%', height, display: 'block' }}
      preserveAspectRatio="none"
      role="img"
      aria-label="Price with trade markers"
    >
      <GridLines y={y} ticks={niceTicks(yDomain)} width={W} />
      <path
        d={path(ts, close, x, y)}
        fill="none"
        stroke="var(--text-dim)"
        strokeWidth={1}
        vectorEffect="non-scaling-stroke"
      />
      {trades.map((trade) => {
        const won = Number(trade.net_pnl) >= 0
        const colour = trade.close_reason === 'liquidation' ? 'var(--warn)' : won ? 'var(--pos)' : 'var(--down)'
        const ex = x(trade.entry_ms)
        const ey = y(Number(trade.entry_price))
        const xx = trade.exit_ms == null ? null : x(trade.exit_ms)
        const xy = trade.exit_price == null ? null : y(Number(trade.exit_price))
        return (
          <g key={trade.index}>
            {xx != null && xy != null && (
              <line
                x1={ex}
                y1={ey}
                x2={xx}
                y2={xy}
                stroke={colour}
                strokeWidth={1}
                vectorEffect="non-scaling-stroke"
                opacity={0.55}
              />
            )}
            <Marker x={ex} y={ey} up={trade.side === 'LONG'} colour={colour} />
            {xx != null && xy != null && (
              <Marker x={xx} y={xy} up={trade.side !== 'LONG'} colour={colour} />
            )}
          </g>
        )
      })}
      {i != null ? (
        <g>
          <line
            x1={x(ts[i])}
            x2={x(ts[i])}
            y1={0}
            y2={height}
            stroke="var(--text-mute)"
            strokeWidth={1}
            vectorEffect="non-scaling-stroke"
            opacity={0.7}
          />
          <circle cx={x(ts[i])} cy={y(close[i])} r={3} fill="var(--text)" stroke="var(--bg)" strokeWidth={1} />
        </g>
      ) : null}
    </svg>
    {i != null ? (
      <HoverReadout
        frac={hover.frac}
        lines={[
          ['time', formatTime(ts[i])],
          ['price', money(String(close[i]))],
        ]}
      />
    ) : null}
    </div>
  )
}

/** A triangle. `preserveAspectRatio="none"` stretches the viewBox, so the marker is drawn
 *  from an explicit transform-free path in user units and kept small enough that the
 *  distortion is not readable as a different shape. */
function Marker({ x, y, up, colour }: { x: number; y: number; up: boolean; colour: string }) {
  const s = 4
  const d = up
    ? `M${x} ${y - s}L${x + s} ${y + s}L${x - s} ${y + s}Z`
    : `M${x} ${y + s}L${x + s} ${y - s}L${x - s} ${y - s}Z`
  return <path d={d} fill={colour} stroke="var(--bg)" strokeWidth={0.5} vectorEffect="non-scaling-stroke" />
}

/**
 * The PnL attribution bar (spec 8.4, 10.3).
 *
 * Contributions are drawn proportional to their **absolute** size, because that is what
 * "where did the money go" means: a strategy whose price edge made 1 400 and whose fees took
 * 1 465 has a net near zero, and a bar scaled by the net would render two large opposing
 * forces as nothing at all.
 */
/** The value at display precision, with negative zero normalised away.
 *
 *  `(-0.001).toFixed(2)` is `"-0.00"` — a magnitude of nothing wearing the loss sign, and
 *  the legend then paints it in the loss colour. Rounding *first* and deciding sign and
 *  colour from the rounded value keeps the printed digits and their colour telling the
 *  same story; the `+ 0` folds IEEE `-0` into `0` so no sign survives on nothing. */
function displayValue(value: number): number {
  return Number(value.toFixed(2)) + 0
}

export function AttributionBar({ attribution }: { attribution: Attribution }) {
  const parts = [
    { key: 'Price', value: Number(attribution.price_pnl), colour: 'var(--pos)' },
    { key: 'Funding', value: Number(attribution.funding_pnl), colour: 'var(--info)' },
    { key: 'Fees', value: -Number(attribution.fees), colour: 'var(--warn)' },
    { key: 'Slippage', value: -Number(attribution.slippage_cost), colour: 'var(--text-mute)' },
    { key: 'Liquidation', value: Number(attribution.liquidation_cost), colour: 'var(--down)' },
  ].filter((part) => Number.isFinite(part.value) && part.value !== 0)

  const total = parts.reduce((sum, part) => sum + Math.abs(part.value), 0)
  if (total === 0) return <div style={{ fontSize: 12, color: 'var(--text-mute)' }}>Nothing to attribute — no fills.</div>

  return (
    <div>
      <div className="flex" style={{ height: 22, borderRadius: 4, overflow: 'hidden' }}>
        {parts.map((part) => (
          <div
            key={part.key}
            title={`${part.key}: ${displayValue(part.value).toFixed(2)}`}
            style={{
              width: `${(Math.abs(part.value) / total) * 100}%`,
              background: part.colour,
              opacity: part.value >= 0 ? 0.85 : 0.45,
            }}
          />
        ))}
      </div>
      <div className="flex flex-wrap gap-3 mt-2">
        {parts.map((part) => {
          // Sign and colour come from the *rounded* value: a contribution that rounds to
          // zero at two places is drawn neutral, never as "-0.00" in the loss colour.
          const shown = displayValue(part.value)
          return (
            <span key={part.key} className="mono flex items-center gap-1.5" style={{ fontSize: 11 }}>
              <span
                style={{
                  width: 8,
                  height: 8,
                  borderRadius: 2,
                  background: part.colour,
                  opacity: part.value >= 0 ? 0.85 : 0.45,
                  display: 'inline-block',
                }}
              />
              <span style={{ color: 'var(--text-dim)' }}>{part.key}</span>
              <span
                style={{
                  color: shown > 0 ? 'var(--pos)' : shown < 0 ? 'var(--down)' : 'var(--text-dim)',
                }}
              >
                {shown > 0 ? '+' : ''}
                {shown.toFixed(2)}
              </span>
            </span>
          )
        })}
      </div>
    </div>
  )
}

function Empty({ height, label }: { height: number; label: string }) {
  return (
    <div
      className="flex items-center justify-center"
      style={{ height, fontSize: 12, color: 'var(--text-mute)' }}
    >
      {label}
    </div>
  )
}
