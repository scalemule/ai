/**
 * Transport seam. `@scalemule/ai` never depends on `@scalemule/sdk`; hosts
 * supply a transport that adds their credentials (API key, session bearer).
 */

export interface AiTransportRequest {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE'
  /** Path relative to the gateway origin, e.g. `/v1/ai/runs`. */
  path: string
  body?: unknown
  headers?: Record<string, string>
  signal?: AbortSignal
  accept?: 'json' | 'event-stream'
  /** Per-request timeout for JSON calls; overrides the transport default. Ignored for streams. */
  timeoutMs?: number
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

/**
 * Headers that must never leave a client: everything the gateway owns or
 * injects. Matched by prefix, with the few client-legitimate names allowed.
 */
const FORBIDDEN_PREFIXES = ['x-sm-', 'x-app-', 'x-user-', 'x-member-', 'x-actor-', 'x-agent-', 'x-effective-', 'x-geo-', 'x-contextgw-', 'x-mule-', 'x-is-', 'x-client-id', 'x-real-client-ip']
const ALLOWED_EXACT = new Set(['x-sm-workspace-id'])

export function isForbiddenHeader(name: string): boolean {
  const lower = name.toLowerCase()
  if (ALLOWED_EXACT.has(lower)) return false
  return FORBIDDEN_PREFIXES.some((prefix) => lower.startsWith(prefix))
}

export function sanitizeHeaders(headers: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [name, value] of Object.entries(headers)) {
    if (!isForbiddenHeader(name) && value !== undefined && value !== null) {
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

      const streaming = request.accept === 'event-stream'
      const controller = new AbortController()
      const cleanups: Array<() => void> = []
      let cleaned = false
      const cleanup = () => {
        if (cleaned) return
        cleaned = true
        for (const fn of cleanups) fn()
      }
      if (request.signal) {
        if (request.signal.aborted) controller.abort(request.signal.reason)
        const onAbort = () => controller.abort(request.signal?.reason)
        request.signal.addEventListener('abort', onAbort, { once: true })
        cleanups.push(() => request.signal?.removeEventListener('abort', onAbort))
      }
      const effectiveTimeout = request.timeoutMs ?? timeoutMs
      if (!streaming && effectiveTimeout > 0) {
        const timer = setTimeout(() => controller.abort(new Error('timeout')), effectiveTimeout)
        cleanups.push(() => clearTimeout(timer))
      }
      let response: Response
      try {
        response = await fetchImpl(`${baseUrl}${request.path}`, {
          method: request.method,
          headers,
          body,
          signal: controller.signal,
          redirect: 'error',
        })
      } catch (error) {
        cleanup()
        throw error
      }
      // For JSON calls the timeout and abort listener stay armed until the
      // body is consumed, so a stalled body can still time out or be aborted.
      // Streams keep the abort listener for their whole life.
      const consumed = async <T>(read: () => Promise<T>): Promise<T> => {
        try {
          return await read()
        } finally {
          if (!streaming) cleanup()
        }
      }
      return {
        status: response.status,
        headers: response.headers,
        body: response.body,
        json: () => consumed(() => response.json()),
        text: () => consumed(() => response.text()),
      }
    },
  }
}
