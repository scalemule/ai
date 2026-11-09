import { generateIdempotencyKey } from './idempotency'
import { readSse } from './sse'
import type { AiTransport, AiTransportResponse } from './transport'
import {
  isTerminalStatus,
  type AiError,
  type AiOperation,
  type AiOperationRequestOptions,
  type AiOperationSettings,
  type AiOperationSettingsPatch,
  type AiRequestOptions,
  type AiResult,
  type AiRun,
  type AiRunEvent,
  type CreateRunInput,
  type EventsOptions,
  type SummarizeInput,
  type SummarizeOptions,
  type SummarizeOutput,
  type SummarizeResult,
  type WaitOptions,
} from './types'

export const MAX_WAIT_MS = 20_000
const DEFAULT_WAIT_TIMEOUT_MS = 60_000
const DEFAULT_POLL_MS = 750
const MAX_POLL_MS = 5_000
const CREATE_RETRIES = 2
const OPERATION_KEY = /^[a-z0-9][a-z0-9_-]{0,99}$/
const OPTION_KEY = /^[a-z0-9][a-z0-9_-]{0,39}$/

export interface ScaleMuleAiOptions {
  transport: AiTransport
  /** Gateway prefix for the facade (default `/v1/ai`). */
  basePath?: string
  /** Default account for user-delegated calls (`x-requested-account-id`). */
  accountId?: string
  /** Override idempotency key generation (tests). */
  generateIdempotencyKey?: () => string
}

/** Thrown only by async iterators, where `{ data, error }` cannot be returned. */
export class AiFacadeError extends Error {
  readonly error: AiError
  constructor(error: AiError) {
    super(error.message)
    this.name = 'AiFacadeError'
    this.error = error
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason ?? new Error('aborted'))
      return
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    const onAbort = () => {
      clearTimeout(timer)
      reject(signal?.reason ?? new Error('aborted'))
    }
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}

function localError(code: string, message: string, details?: unknown): AiError {
  return { code, message, status: 0, details }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

async function parseError(response: AiTransportResponse): Promise<AiError> {
  let body: unknown = null
  try {
    body = await response.json()
  } catch {
    try {
      body = await response.text()
    } catch {
      body = null
    }
  }
  const envelope = isRecord(body) ? body : {}
  const error = isRecord(envelope.error) ? envelope.error : envelope
  const meta = isRecord(error.meta) ? error.meta : {}
  const code = typeof error.code === 'string' ? error.code : `HTTP_${response.status}`
  const message = typeof error.message === 'string' ? error.message : `Request failed with status ${response.status}`
  const result: AiError = { code, message, status: response.status }
  const requestId = (typeof meta.request_id === 'string' && meta.request_id) || response.headers.get('x-request-id')
  if (requestId) result.requestId = requestId
  if (typeof error.field === 'string') result.field = error.field
  if (error.details !== undefined) result.details = error.details
  const retryAfterMs = parseRetryAfter(response.headers.get('retry-after')) ?? retryAfterFromBody(error.details) ?? retryAfterFromBody(meta)
  if (retryAfterMs !== undefined) result.retryAfterMs = retryAfterMs
  return result
}

function parseRetryAfter(value: string | null): number | undefined {
  if (!value) return undefined
  const seconds = Number(value)
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1000)
  const at = Date.parse(value)
  return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now())
}

function retryAfterFromBody(source: unknown): number | undefined {
  if (!isRecord(source)) return undefined
  const seconds = source.retry_after
  return typeof seconds === 'number' && seconds >= 0 ? Math.round(seconds * 1000) : undefined
}

async function parseData<T>(response: AiTransportResponse): Promise<AiResult<T>> {
  if (response.status < 200 || response.status >= 300) {
    return { data: null, error: await parseError(response) }
  }
  let body: unknown
  try {
    body = await response.json()
  } catch (cause) {
    return { data: null, error: localError('INVALID_RESPONSE', 'The AI facade returned a non-JSON body', cause) }
  }
  const data = isRecord(body) && 'data' in body ? body.data : body
  return { data: data as T, error: null }
}

function retryDelayMs(error: AiError, attempt: number): number | null {
  if (error.status === 0 && error.code !== 'ABORTED') return 300 * 2 ** attempt
  if (error.status === 429) return Math.min(error.retryAfterMs ?? 1_000, 5_000)
  if (error.status >= 500) return Math.min(error.retryAfterMs ?? 300 * 2 ** attempt, 5_000)
  return null
}

export class ScaleMuleAi {
  private readonly transport: AiTransport
  private readonly basePath: string
  private readonly defaultAccountId?: string
  private readonly newKey: () => string

  constructor(options: ScaleMuleAiOptions) {
    if (!options?.transport) throw new Error('@scalemule/ai: a transport is required')
    this.transport = options.transport
    this.basePath = (options.basePath ?? '/v1/ai').replace(/\/+$/, '')
    this.defaultAccountId = options.accountId
    this.newKey = options.generateIdempotencyKey ?? generateIdempotencyKey
  }

  private headersFor(options?: { accountId?: string; headers?: Record<string, string> }): Record<string, string> {
    const headers: Record<string, string> = { ...(options?.headers ?? {}) }
    const accountId = options?.accountId ?? this.defaultAccountId
    if (accountId) headers['x-requested-account-id'] = accountId
    return headers
  }

  private async send<T>(
    method: 'GET' | 'POST' | 'PUT' | 'DELETE',
    path: string,
    options: { body?: unknown; headers?: Record<string, string>; signal?: AbortSignal; retries?: number; timeoutMs?: number },
  ): Promise<AiResult<T>> {
    const retries = options.retries ?? 0
    let lastError: AiError | null = null
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      let result: AiResult<T>
      try {
        const response = await this.transport.send({
          method,
          path: `${this.basePath}${path}`,
          body: options.body,
          headers: options.headers,
          signal: options.signal,
          accept: 'json',
          timeoutMs: options.timeoutMs,
        })
        result = await parseData<T>(response)
      } catch (cause) {
        const aborted = options.signal?.aborted === true
        result = {
          data: null,
          error: localError(aborted ? 'ABORTED' : 'NETWORK_ERROR', aborted ? 'The request was aborted' : 'The AI facade could not be reached', cause),
        }
      }
      if (!result.error) return result
      lastError = result.error
      const delay = attempt < retries ? retryDelayMs(result.error, attempt) : null
      if (delay === null) break
      try {
        await sleep(delay, options.signal)
      } catch {
        return { data: null, error: localError('ABORTED', 'The request was aborted') }
      }
    }
    return { data: null, error: lastError ?? localError('UNKNOWN', 'Unknown failure') }
  }

  readonly runs = {
    /**
     * Create a run. The idempotency key is fixed before the first attempt, so
     * retries (ours or the network's) can only ever return the same run.
     */
    create: async <TOutput = unknown>(input: CreateRunInput, options: AiRequestOptions = {}): Promise<AiResult<AiRun<TOutput>>> => {
      if (!input?.operation) return { data: null, error: localError('INPUT_INVALID', 'operation is required') }
      if (!isRecord(input.input)) return { data: null, error: localError('INPUT_INVALID', 'input must be an object') }
      const idempotencyKey = options.idempotencyKey ?? this.newKey()
      const body: Record<string, unknown> = { operation: input.operation, input: input.input }
      if (input.version !== undefined) body.version = input.version
      if (input.waitMs !== undefined) body.wait_ms = Math.max(0, Math.min(MAX_WAIT_MS, Math.floor(input.waitMs)))
      return this.send<AiRun<TOutput>>('POST', '/runs', {
        body,
        headers: { ...this.headersFor(options), 'x-idempotency-key': idempotencyKey },
        signal: options.signal,
        timeoutMs: options.timeoutMs,
        retries: CREATE_RETRIES,
      })
    },

    get: async <TOutput = unknown>(runId: string, options: AiRequestOptions = {}): Promise<AiResult<AiRun<TOutput>>> => {
      return this.send<AiRun<TOutput>>('GET', `/runs/${encodeURIComponent(runId)}`, {
        headers: this.headersFor(options),
        signal: options.signal,
        timeoutMs: options.timeoutMs,
        retries: 1,
      })
    },

    cancel: async <TOutput = unknown>(runId: string, options: AiRequestOptions = {}): Promise<AiResult<AiRun<TOutput>>> => {
      return this.send<AiRun<TOutput>>('POST', `/runs/${encodeURIComponent(runId)}/cancel`, {
        headers: this.headersFor(options),
        signal: options.signal,
        timeoutMs: options.timeoutMs,
      })
    },

    /** Poll until the run is terminal (or needs approval). Never throws. */
    wait: async <TOutput = unknown>(runId: string, options: WaitOptions = {}): Promise<AiResult<AiRun<TOutput>>> => {
      const deadline = Date.now() + (options.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS)
      const stopOnApproval = options.stopOnApproval ?? true
      let interval = options.pollIntervalMs ?? DEFAULT_POLL_MS
      for (;;) {
        const result = await this.runs.get<TOutput>(runId, { accountId: options.accountId, headers: options.headers, signal: options.signal })
        if (result.error) return result
        const run = result.data
        if (isTerminalStatus(run.status) || (stopOnApproval && run.status === 'waiting_for_approval')) return result
        if (Date.now() >= deadline) {
          return { data: null, error: { code: 'WAIT_TIMEOUT', message: 'The run did not finish in time; it continues server-side', status: 0, run } }
        }
        const hinted = run.error?.retry_after_ms
        const delay = Math.min(hinted && hinted > 0 ? hinted : interval, MAX_POLL_MS, Math.max(0, deadline - Date.now()))
        try {
          await sleep(delay, options.signal)
        } catch {
          return { data: null, error: localError('ABORTED', 'Waiting was aborted') }
        }
        interval = Math.min(Math.round(interval * 1.5), MAX_POLL_MS)
      }
    },

    /**
     * Lifecycle events as an async iterable. Ends after the server's terminal
     * `end` event; reconnects with `Last-Event-ID` if the stream drops first.
     * Throws `AiFacadeError` when the stream cannot be opened.
     */
    events: (runId: string, options: EventsOptions = {}): AsyncIterable<AiRunEvent> => {
      const self = this
      return {
        [Symbol.asyncIterator]: async function* () {
          let after = options.after ?? 0
          let reconnects = 0
          let lastDropCause: unknown
          const maxReconnects = options.maxReconnects ?? 5
          for (;;) {
            const headers = self.headersFor(options)
            if (after > 0) headers['last-event-id'] = String(after)
            let response: AiTransportResponse
            try {
              response = await self.transport.send({
                method: 'GET',
                path: `${self.basePath}/runs/${encodeURIComponent(runId)}/events`,
                headers,
                signal: options.signal,
                accept: 'event-stream',
              })
            } catch (cause) {
              if (options.signal?.aborted) return
              if (reconnects >= maxReconnects) throw new AiFacadeError(localError('NETWORK_ERROR', 'Could not open the event stream', cause))
              reconnects += 1
              await sleep(Math.min(1_000 * reconnects, 5_000), options.signal)
              continue
            }
            if (response.status < 200 || response.status >= 300) {
              throw new AiFacadeError(await parseError(response))
            }
            if (!response.body) throw new AiFacadeError(localError('INVALID_RESPONSE', 'The event stream has no body'))
            let ended = false
            try {
              for await (const message of readSse(response.body, options.signal)) {
                if (message.event === 'end') {
                  ended = true
                  break
                }
                if (!message.data) continue
                let parsed: unknown
                try {
                  parsed = JSON.parse(message.data)
                } catch {
                  continue
                }
                if (!isRecord(parsed)) continue
                const event = parsed as unknown as AiRunEvent
                if (typeof event.sequence === 'number' && event.sequence > after) after = event.sequence
                yield event
                if (isTerminalStatus(event.type)) {
                  ended = true
                }
              }
            } catch (cause) {
              // A mid-stream drop is a disconnect, not a failure: reconnect
              // below with Last-Event-ID unless the caller aborted.
              if (options.signal?.aborted) return
              lastDropCause = cause
            }
            if (ended || options.signal?.aborted) return
            if (reconnects >= maxReconnects) {
              // Never let the consumer mistake "gave up" for "finished".
              throw new AiFacadeError(
                localError('STREAM_INCOMPLETE', 'The event stream dropped before the run finished and could not be resumed', lastDropCause),
              )
            }
            reconnects += 1
            await sleep(Math.min(1_000 * reconnects, 5_000), options.signal)
          }
        },
      }
    },
  }

  /**
   * Operation selection (ADR-2026-11-09). Clients name option keys only; the
   * platform maps them to agents and models the application approved.
   */
  readonly operations = {
    /** Operations configured for the caller's account, with their options. */
    list: async (options: AiOperationRequestOptions = {}): Promise<AiResult<AiOperation[]>> => {
      return this.send<AiOperation[]>('GET', '/operations', {
        headers: this.headersFor(options),
        signal: options.signal,
        timeoutMs: options.timeoutMs,
        retries: 1,
      })
    },

    /** Set the signed-in user's own preference among the account's enabled options. */
    setPreference: async (operation: string, option: string, options: AiOperationRequestOptions = {}): Promise<AiResult<AiOperation>> => {
      if (!OPERATION_KEY.test(operation ?? '')) return { data: null, error: localError('INPUT_INVALID', 'operation must be a lowercase operation key') }
      if (!OPTION_KEY.test(option ?? '')) return { data: null, error: localError('INPUT_INVALID', 'option must be a lowercase option key') }
      return this.send<AiOperation>('PUT', `/operations/${encodeURIComponent(operation)}/preference`, {
        body: { option },
        headers: this.headersFor(options),
        signal: options.signal,
        timeoutMs: options.timeoutMs,
      })
    },

    /** Clear the signed-in user's own preference; the account default applies again. */
    clearPreference: async (operation: string, options: AiOperationRequestOptions = {}): Promise<AiResult<AiOperation>> => {
      if (!OPERATION_KEY.test(operation ?? '')) return { data: null, error: localError('INPUT_INVALID', 'operation must be a lowercase operation key') }
      return this.send<AiOperation>('DELETE', `/operations/${encodeURIComponent(operation)}/preference`, {
        headers: this.headersFor(options),
        signal: options.signal,
        timeoutMs: options.timeoutMs,
      })
    },

    /** Account settings and the application policy that applies (account admins). */
    getSettings: async (operation: string, options: AiOperationRequestOptions = {}): Promise<AiResult<AiOperationSettings>> => {
      if (!OPERATION_KEY.test(operation ?? '')) return { data: null, error: localError('INPUT_INVALID', 'operation must be a lowercase operation key') }
      return this.send<AiOperationSettings>('GET', `/operations/${encodeURIComponent(operation)}/settings`, {
        headers: this.headersFor(options),
        signal: options.signal,
        timeoutMs: options.timeoutMs,
        retries: 1,
      })
    },

    /**
     * Change account settings (account admins). Omitted fields are unchanged;
     * `null` clears `default_option` / `allow_user_choice` back to inherit and
     * `enabled_options` to none.
     */
    updateSettings: async (operation: string, patch: AiOperationSettingsPatch, options: AiOperationRequestOptions = {}): Promise<AiResult<AiOperationSettings>> => {
      if (!OPERATION_KEY.test(operation ?? '')) return { data: null, error: localError('INPUT_INVALID', 'operation must be a lowercase operation key') }
      if (!isRecord(patch)) return { data: null, error: localError('INPUT_INVALID', 'patch must be an object') }
      const body: Record<string, unknown> = {}
      if ('default_option' in patch) {
        if (patch.default_option !== null && (typeof patch.default_option !== 'string' || !OPTION_KEY.test(patch.default_option))) {
          return { data: null, error: localError('INPUT_INVALID', 'default_option must be a lowercase option key or null') }
        }
        body.default_option = patch.default_option
      }
      if ('allow_user_choice' in patch) {
        if (patch.allow_user_choice !== null && typeof patch.allow_user_choice !== 'boolean') {
          return { data: null, error: localError('INPUT_INVALID', 'allow_user_choice must be a boolean or null') }
        }
        body.allow_user_choice = patch.allow_user_choice
      }
      if ('enabled_options' in patch) {
        if (patch.enabled_options !== null && (!Array.isArray(patch.enabled_options) || !patch.enabled_options.every((key) => typeof key === 'string' && OPTION_KEY.test(key)))) {
          return { data: null, error: localError('INPUT_INVALID', 'enabled_options must be lowercase option keys or null') }
        }
        body.enabled_options = patch.enabled_options
      }
      if ('status' in patch) {
        if (patch.status !== 'active' && patch.status !== 'paused') {
          return { data: null, error: localError('INPUT_INVALID', "status must be 'active' or 'paused'") }
        }
        body.status = patch.status
      }
      if (Object.keys(body).length === 0) return { data: null, error: localError('INPUT_INVALID', 'Nothing to change') }
      return this.send<AiOperationSettings>('PUT', `/operations/${encodeURIComponent(operation)}/settings`, {
        body,
        headers: this.headersFor(options),
        signal: options.signal,
        timeoutMs: options.timeoutMs,
      })
    },
  }

  /** Summarize text through the `summarize` operation and wait for the result. */
  async summarize(input: SummarizeInput, options: SummarizeOptions = {}): Promise<AiResult<SummarizeResult>> {
    if (typeof input?.text !== 'string' || input.text.length === 0) {
      return { data: null, error: localError('INPUT_INVALID', 'text is required') }
    }
    const payload: Record<string, unknown> = { text: input.text }
    if (input.format) payload.format = input.format
    if (input.max_characters !== undefined) payload.max_characters = input.max_characters
    if (input.language) payload.language = input.language
    const started = Date.now()
    const timeoutMs = options.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS
    const created = await this.runs.create<SummarizeOutput>(
      { operation: 'summarize', input: payload, waitMs: Math.min(options.waitMs ?? MAX_WAIT_MS, timeoutMs) },
      options,
    )
    if (created.error) return { data: null, error: created.error }
    let run = created.data
    if (!isTerminalStatus(run.status) && run.status !== 'waiting_for_approval') {
      const remaining = Math.max(0, timeoutMs - (Date.now() - started))
      const waited = await this.runs.wait<SummarizeOutput>(run.id, {
        timeoutMs: remaining,
        signal: options.signal,
        accountId: options.accountId,
        headers: options.headers,
      })
      if (waited.error) return { data: null, error: waited.error }
      run = waited.data
    }
    if (run.status === 'succeeded' && run.output && typeof run.output.summary === 'string') {
      return { data: { ...run.output, run }, error: null }
    }
    const code =
      run.error?.code ??
      (run.status === 'succeeded' ? (run.output_expired ? 'RUN_OUTPUT_EXPIRED' : 'OUTPUT_UNAVAILABLE') : `RUN_${run.status.toUpperCase()}`)
    const message = run.error?.message ?? `The summarize run ended with status ${run.status}`
    return { data: null, error: { code, message, status: 200, run } }
  }
}

export function createAi(options: ScaleMuleAiOptions): ScaleMuleAi {
  return new ScaleMuleAi(options)
}
