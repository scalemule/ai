/**
 * Public wire types for the ScaleMule AI facade (`/v1/ai`).
 *
 * Nothing here exposes agents, bindings, profiles, models, prompts or
 * reservations: operations are named capabilities the platform activates per
 * account, and the only knobs are the operation's own input fields.
 */

export type AiRunStatus =
  | 'queued'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'waiting_for_approval'
  | 'cancelling'
  | 'cancelled'
  | 'expired'

export const AI_TERMINAL_STATUSES: readonly AiRunStatus[] = ['succeeded', 'failed', 'cancelled', 'expired']

export function isTerminalStatus(status: string): boolean {
  return (AI_TERMINAL_STATUSES as readonly string[]).includes(status)
}

export type AiErrorClass = 'retryable' | 'terminal' | 'uncertain'

export interface AiRunError {
  /** Stable code such as INPUT_INVALID, OUTPUT_SCHEMA_INVALID, BUDGET_EXHAUSTED. */
  code: string
  class: AiErrorClass | string
  message: string
  retry_after_ms?: number
}

export interface AiRunApproval {
  approval_id: string
  expires_at?: string
}

export interface AiRun<TOutput = unknown> {
  id: string
  operation: string
  version: number
  status: AiRunStatus | string
  attempt: number
  created_at: string
  started_at?: string
  ended_at?: string
  /** Present only while a succeeded run's output is retained. */
  output?: TOutput
  /** True when the output existed but has passed its retention window. */
  output_expired: boolean
  output_expires_at?: string
  error?: AiRunError
  approval?: AiRunApproval
  cancel_requested: boolean
}

export interface AiRunEvent {
  run_id: string
  sequence: number
  type: string
  at: string
  data?: Record<string, unknown>
}

export interface CreateRunInput {
  /** Operation key, e.g. `summarize`. */
  operation: string
  /** Pin a definition version; omit for the account's active version. */
  version?: number
  /** Operation-specific input validated server-side against the definition schema. */
  input: Record<string, unknown>
  /** Block up to this long (max 20000 ms) for completion before responding. */
  waitMs?: number
}

export interface AiRequestOptions {
  /**
   * Caller-chosen idempotency key. Generated before the first request when
   * omitted, so network retries never create a second run.
   */
  idempotencyKey?: string
  /**
   * Account (workspace) to run under for user-delegated calls. Sent as
   * `x-requested-account-id`; the gateway verifies membership. Server
   * credentials bound to an account do not need it.
   */
  accountId?: string
  signal?: AbortSignal
  timeoutMs?: number
  headers?: Record<string, string>
}

export interface WaitOptions {
  /** Give up after this long (default 60000 ms). The run keeps going server-side. */
  timeoutMs?: number
  /** Poll interval (default 750 ms, capped by retry hints from the server). */
  pollIntervalMs?: number
  /** Also stop when the run needs a human approval (default true). */
  stopOnApproval?: boolean
  signal?: AbortSignal
  accountId?: string
  headers?: Record<string, string>
}

export interface EventsOptions {
  /** Replay events after this sequence (also sent as `Last-Event-ID`). */
  after?: number
  signal?: AbortSignal
  /** Reconnect attempts after the stream drops without a terminal event (default 5). */
  maxReconnects?: number
  accountId?: string
  headers?: Record<string, string>
}

export interface AiError {
  code: string
  message: string
  /** HTTP status of the failing response, or 0 when the failure was local (network, abort, timeout). */
  status: number
  requestId?: string
  field?: string
  details?: unknown
  /** The run, when the error describes a run that finished unsuccessfully. */
  run?: AiRun
}

export type AiResult<T> = { data: T; error: null } | { data: null; error: AiError }

export type SummarizeFormat = 'brief' | 'bullets' | 'paragraph'

export interface SummarizeInput {
  text: string
  format?: SummarizeFormat
  /** Character budget for the summary (80..4000, default 800). */
  max_characters?: number
  /** BCP-47-ish language code such as `en` or `pt-BR`. */
  language?: string
}

export interface SummarizeOutput {
  summary: string
  format: SummarizeFormat
  truncated: boolean
}

export interface SummarizeResult extends SummarizeOutput {
  run: AiRun<SummarizeOutput>
}

export interface SummarizeOptions extends AiRequestOptions {
  /** Total time to wait for the summary (default 60000 ms). */
  timeoutMs?: number
  /** Server-side wait before the first response (default 20000 ms, the maximum). */
  waitMs?: number
}
