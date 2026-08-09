/**
 * Client-side exports (spec 10.4): every table exports CSV, every chart exports PNG.
 *
 * Client-side deliberately: the data on screen is the data exported — same filters, same
 * downsampling, same ordering. A server-side export that re-queried could disagree with
 * the table above it, and an export exists to be pasted somewhere as evidence.
 */

function download(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  // Appended before the click, revoked on a delay. `click()` on a detached anchor with a
  // synchronous revoke right behind it works in Chromium by luck of scheduling; Firefox
  // and Safari can start the navigation after the URL is already dead and silently save
  // nothing. The DOM node and the URL are both cleaned up once the download has had time
  // to begin — the browser keeps its own reference from there.
  document.body.appendChild(anchor)
  anchor.click()
  window.setTimeout(() => {
    anchor.remove()
    URL.revokeObjectURL(url)
  }, 1000)
}

/** Leading characters a spreadsheet interprets as a formula (OWASP CSV-injection set). */
const FORMULA_LEADS = new Set(['=', '+', '-', '@', '\t', '\r'])

/** A bare signed decimal — the shape every money string in an exported table takes. */
const PLAIN_NUMBER = /^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$/

/** RFC 4180-ish: quote when needed, double embedded quotes, CRLF rows.
 *
 * Null and undefined become empty cells, not the string "null" — an empty cell reads as
 * "no value" in every spreadsheet, which is what an undefined metric means.
 *
 * Formula injection is neutralised the same way the server's trades.csv route does it
 * (`_csv_safe` in perplab/api/routers/runs.py): these files exist to be opened in a
 * spreadsheet, and Excel executes a cell that begins with `=`, `+`, `-`, `@`, a tab or a
 * carriage return on open — quoted or not. The OWASP remedy is a leading apostrophe,
 * which Excel displays as text and drops from the value. It is applied only to strings
 * that could be a formula: a string that parses as a plain signed number is left alone,
 * because exact money strings (`"-30.05000000"`) begin with `-` by design and a `-` that
 * starts a number cannot start a formula call. Actual numbers and booleans are never
 * touched. */
export function downloadCsv(
  filename: string,
  headers: string[],
  rows: (string | number | boolean | null | undefined)[][],
): void {
  const escape = (value: string | number | boolean | null | undefined): string => {
    if (value == null) return ''
    let text = String(value)
    if (typeof value === 'string' && text.length > 0 && FORMULA_LEADS.has(text[0]) && !PLAIN_NUMBER.test(text)) {
      text = `'${text}`
    }
    return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text
  }
  const body = [headers, ...rows].map((row) => row.map(escape).join(',')).join('\r\n')
  download(new Blob([body], { type: 'text/csv;charset=utf-8' }), filename)
}

/** Total-pixel ceiling for the export canvas.
 *
 * Safari refuses to allocate a canvas past ~16.7M pixels and reports it by rendering
 * nothing, so a 2x raster of a 4K-wide chart used to fail without a word. Staying under
 * the ceiling with margin means the scale degrades before the export does. */
const MAX_CANVAS_PIXELS = 16_000_000

/**
 * Serialise an SVG chart to a PNG at up to 2x for legibility.
 *
 * The SVG is cloned and given explicit pixel dimensions plus the theme's colours resolved
 * to literals — a serialised SVG has no CSS custom properties, so without resolution every
 * `var(--pos)` line would render black on transparent.
 *
 * Returns a promise so failure has somewhere to go. The old version failed silently three
 * ways — no `onerror` on the rasterising image, a bare return on a null 2d context, and a
 * swallowed null blob — so a failed export was indistinguishable from a slow one, and an
 * export exists to be evidence. Every failure path now rejects with the stage named, and
 * the scale drops below 2x (never past the export itself) when 2x would cross the canvas
 * ceiling: a huge chart exports smaller rather than not at all.
 */
export function downloadPng(svg: SVGSVGElement, filename: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const rect = svg.getBoundingClientRect()
    const width = Math.max(1, Math.round(rect.width))
    const height = Math.max(1, Math.round(rect.height))
    const scale = Math.min(2, Math.sqrt(MAX_CANVAS_PIXELS / (width * height)))

    const clone = svg.cloneNode(true) as SVGSVGElement
    clone.setAttribute('width', String(width))
    clone.setAttribute('height', String(height))
    clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg')

    // Resolve CSS variables to literal colours on every element that uses one.
    const computed = getComputedStyle(document.documentElement)
    const resolve_ = (value: string): string =>
      value.replace(/var\((--[a-z0-9-]+)\)/gi, (_, name: string) => computed.getPropertyValue(name).trim() || '#888')
    const originals = svg.querySelectorAll<SVGElement>('*')
    clone.querySelectorAll<SVGElement>('*').forEach((element, index) => {
      for (const attr of ['stroke', 'fill'] as const) {
        const value = element.getAttribute(attr)
        if (value?.includes('var(')) element.setAttribute(attr, resolve_(value))
      }
      // Inline styles can carry vars too (rare in our charts, cheap to cover).
      const style = element.getAttribute('style')
      if (style?.includes('var(')) element.setAttribute('style', resolve_(style))
      void originals[index]
    })

    const background = computed.getPropertyValue('--bg').trim() || '#0A0A0B'
    const source = new XMLSerializer().serializeToString(clone)
    const image = new Image()
    image.onerror = () => reject(new Error('the chart SVG failed to rasterise'))
    image.onload = () => {
      const canvas = document.createElement('canvas')
      canvas.width = Math.max(1, Math.round(width * scale))
      canvas.height = Math.max(1, Math.round(height * scale))
      const context = canvas.getContext('2d')
      if (context == null) {
        reject(new Error('the browser refused a 2d canvas context'))
        return
      }
      context.fillStyle = background
      context.fillRect(0, 0, canvas.width, canvas.height)
      context.drawImage(image, 0, 0, canvas.width, canvas.height)
      canvas.toBlob((blob) => {
        if (blob == null) {
          reject(new Error('the browser produced no PNG data for the export canvas'))
          return
        }
        download(blob, filename)
        resolve()
      }, 'image/png')
    }
    image.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(source)}`
  })
}
