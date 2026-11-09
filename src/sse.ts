/**
 * Minimal, allocation-light Server-Sent Events parser over a fetch body.
 * Handles multi-line `data:` payloads, comments, CRLF and UTF-8 sequences
 * split across chunks.
 */

export interface SseMessage {
  id?: string
  event: string
  data: string
}

export class SseParser {
  private buffer = ''
  private readonly decoder = new TextDecoder('utf-8')

  /** Feed a chunk and get every complete message it closed. */
  push(chunk: Uint8Array | string): SseMessage[] {
    this.buffer += typeof chunk === 'string' ? chunk : this.decoder.decode(chunk, { stream: true })
    const messages: SseMessage[] = []
    let boundary = findBoundary(this.buffer)
    while (boundary) {
      const block = this.buffer.slice(0, boundary.index)
      this.buffer = this.buffer.slice(boundary.index + boundary.length)
      const message = parseBlock(block)
      if (message) messages.push(message)
      boundary = findBoundary(this.buffer)
    }
    return messages
  }

  /** Flush a trailing block when the stream ends without a blank line. */
  finish(): SseMessage[] {
    this.buffer += this.decoder.decode()
    const rest = this.buffer
    this.buffer = ''
    const message = rest.trim() ? parseBlock(rest) : null
    return message ? [message] : []
  }
}

function findBoundary(text: string): { index: number; length: number } | null {
  const lf = text.indexOf('\n\n')
  const crlf = text.indexOf('\r\n\r\n')
  if (lf === -1 && crlf === -1) return null
  if (crlf !== -1 && (lf === -1 || crlf < lf)) return { index: crlf, length: 4 }
  return { index: lf, length: 2 }
}

function parseBlock(rawBlock: string): SseMessage | null {
  // A UTF-8 BOM may precede the very first field; it is not part of the name.
  const block = rawBlock.startsWith('\uFEFF') ? rawBlock.slice(1) : rawBlock
  let id: string | undefined
  let event = 'message'
  const data: string[] = []
  for (const rawLine of block.split(/\r?\n/)) {
    if (!rawLine || rawLine.startsWith(':')) continue
    const colon = rawLine.indexOf(':')
    const field = colon === -1 ? rawLine : rawLine.slice(0, colon)
    let value = colon === -1 ? '' : rawLine.slice(colon + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    switch (field) {
      case 'id':
        id = value
        break
      case 'event':
        event = value || 'message'
        break
      case 'data':
        data.push(value)
        break
      default:
        break
    }
  }
  if (data.length === 0 && id === undefined && event === 'message') return null
  return { id, event, data: data.join('\n') }
}

export async function* readSse(
  body: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncGenerator<SseMessage, void, undefined> {
  const reader = body.getReader()
  const parser = new SseParser()
  const onAbort = () => {
    reader.cancel().catch(() => undefined)
  }
  signal?.addEventListener('abort', onAbort, { once: true })
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      if (value) {
        for (const message of parser.push(value)) yield message
      }
    }
    for (const message of parser.finish()) yield message
  } finally {
    signal?.removeEventListener('abort', onAbort)
    reader.releaseLock()
  }
}
