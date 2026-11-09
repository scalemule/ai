/**
 * Transport seam. `@scalemule/ai` never depends on `@scalemule/sdk`; hosts
 * supply a transport that adds their credentials (API key, session bearer).
 */

export interface AiTransportRequest {
  method: 'GET' | 'POST'
  /** Path relative to the gateway origin, e.g. `/v1/ai/runs`. */
  path: string
  body?: unknown
  headers?: Record<string, string>
  signal?: AbortSignal
  accept?: 'json' | 'event-stream'
}

export interface AiTransportResponse {
  status: number
  headers: { get(name: string): string | null }
  body: ReadableStream<Uint8Array> | null
  json(): Promise<unknown>
  text(): Promise<string>
}

export interface AiTransport {
  send(request: AiTransportRequest): Promise<AiTransportResponse>
}

export type HeaderSource = Record<string, string> | (() => Record<string, string> | Promise<Record<string, string>>)

export interface FetchTransportOptions {
  /** Gateway origin, e.g. `https://api.scalemule.com`. */
  baseUrl: string
  /** Credential headers (`x-api-key`, `Authorization`). Evaluated per request. */
  headers?: HeaderSource
  fetch?: typeof fetch
  /** Per-request timeout for JSON calls (default 30000 ms). Streams are not timed out. */
  defaultTimeoutMs?: number
}

/** Headers that must never leave a client: platform-internal or gateway-owned. */
const FORBIDDEN_HEADERS = new Set([
  'x-sm-internal-token',
  'x-sm-service-id',
  'x-app-id',
  'x-effective-account-id',
  'x-user-id',
  'x-member-id',
  'x-app-key-id',
  'x-app-key-type',
  'x-app-key-purpose',
  'x-actor-type',
  'x-sm-admin-role',
])

export function sanitizeHeaders(headers: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [name, value] of Object.entries(headers)) {
    if (!FORBIDDEN_HEADERS.has(name.toLowerCase()) && value !== undefined && value !== null) {
      out[name] = value
    }
  }
  return out
}

async function resolveHeaders(source: HeaderSource | undefined): Promise<Record<string, string>> {
  if (!source) return {}
  return typeof source === 'function' ? await source() : source
}

export function createFetchTransport(options: FetchTransportOptions): AiTransport {
  const baseUrl = options.baseUrl.replace(/\/+$/, '')
  const fetchImpl = options.fetch ?? (typeof fetch === 'function' ? fetch : undefined)
  if (!fetchImpl) {
    throw new Error('@scalemule/ai: no fetch implementation available; pass `fetch` in createFetchTransport options')
  }
  const timeoutMs = options.defaultTimeoutMs ?? 30_000

  return {
    async send(request) {
      const headers = new Headers(sanitizeHeaders(await resolveHeaders(options.headers)))
      for (const [name, value] of Object.entries(sanitizeHeaders(request.headers ?? {}))) {
        headers.set(name, value)
      }
      headers.set('accept', request.accept === 'event-stream' ? 'text/event-stream' : 'application/json')
      let body: string | undefined
      if (request.body !== undefined) {
        body = JSON.stringify(request.body)
        headers.set('content-type', 'application/json')
      }

      const controller = new AbortController()
      const cleanups: Array<() => void> = []
      if (request.signal) {
        if (request.signal.aborted) controller.abort(request.signal.reason)
        const onAbort = () => controller.abort(request.signal?.reason)
        request.signal.addEventListener('abort', onAbort, { once: true })
        cleanups.push(() => request.signal?.removeEventListener('abort', onAbort))
      }
      if (request.accept !== 'event-stream' && timeoutMs > 0) {
        const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs)
        cleanups.push(() => clearTimeout(timer))
      }
      try {
        const response = await fetchImpl(`${baseUrl}${request.path}`, {
          method: request.method,
          headers,
          body,
          signal: controller.signal,
          redirect: 'error',
        })
        return {
          status: response.status,
          headers: response.headers,
          body: response.body,
          json: () => response.json(),
          text: () => response.text(),
        }
      } finally {
        if (request.accept !== 'event-stream') {
          for (const cleanup of cleanups) cleanup()
        }
      }
    },
  }
}
