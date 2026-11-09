import { describe, expect, it, vi } from 'vitest'
import { AiFacadeError, ScaleMuleAi } from './client'
import type { AiTransport, AiTransportRequest, AiTransportResponse } from './transport'

function jsonResponse(status: number, body: unknown, headers: Record<string, string> = {}): AiTransportResponse {
  const map = new Headers(headers)
  return {
    status,
    headers: map,
    body: null,
    json: async () => body,
    text: async () => JSON.stringify(body),
  }
}

function streamResponse(text: string): AiTransportResponse {
  return {
    status: 200,
    headers: new Headers({ 'content-type': 'text/event-stream' }),
    body: new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(text))
        controller.close()
      },
    }),
    json: async () => ({}),
    text: async () => text,
  }
}

function run(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: 'run-1',
    operation: 'summarize',
    version: 1,
    status: 'queued',
    attempt: 0,
    created_at: '2026-11-09T00:00:00Z',
    output_expired: false,
    cancel_requested: false,
    ...overrides,
  }
}

function fakeTransport(responder: (request: AiTransportRequest, call: number) => AiTransportResponse | Promise<AiTransportResponse>) {
  const requests: AiTransportRequest[] = []
  const transport: AiTransport = {
    send: vi.fn(async (request: AiTransportRequest) => {
      requests.push(request)
      return responder(request, requests.length)
    }),
  }
  return { transport, requests }
}

describe('ScaleMuleAi.runs.create', () => {
  it('sends the idempotency key on the first attempt and reuses it on retry', async () => {
    const { transport, requests } = fakeTransport((_request, call) =>
      call === 1 ? jsonResponse(503, { success: false, error: { code: 'UPSTREAM', message: 'down' } }) : jsonResponse(202, { success: true, data: run() }),
    )
    const ai = new ScaleMuleAi({ transport, generateIdempotencyKey: () => 'ai_fixed' })
    const result = await ai.runs.create({ operation: 'summarize', version: 1, input: { text: 'hi' }, waitMs: 99_999 })
    expect(result.error).toBeNull()
    expect(result.data?.id).toBe('run-1')
    expect(requests).toHaveLength(2)
    for (const request of requests) {
      expect(request.method).toBe('POST')
      expect(request.path).toBe('/v1/ai/runs')
      expect(request.headers?.['x-idempotency-key']).toBe('ai_fixed')
      expect(request.body).toEqual({ operation: 'summarize', version: 1, input: { text: 'hi' }, wait_ms: 20_000 })
    }
  })

  it('does not retry client errors and maps the envelope error', async () => {
    const { transport, requests } = fakeTransport(() =>
      jsonResponse(409, { success: false, error: { code: 'IDEMPOTENCY_KEY_REUSED', message: 'reused', meta: { request_id: 'req_1' } } }),
    )
    const ai = new ScaleMuleAi({ transport })
    const result = await ai.runs.create({ operation: 'summarize', input: { text: 'hi' } })
    expect(result.data).toBeNull()
    expect(result.error).toMatchObject({ code: 'IDEMPOTENCY_KEY_REUSED', status: 409, requestId: 'req_1' })
    expect(requests).toHaveLength(1)
  })

  it('sends the account selector header and never internal headers', async () => {
    const { transport, requests } = fakeTransport(() => jsonResponse(201, { success: true, data: run({ status: 'succeeded' }) }))
    const ai = new ScaleMuleAi({ transport, accountId: 'acct-default' })
    await ai.runs.create({ operation: 'summarize', input: { text: 'hi' } }, { accountId: 'acct-override', headers: { 'x-trace': '1' } })
    expect(requests[0]?.headers).toMatchObject({ 'x-requested-account-id': 'acct-override', 'x-trace': '1' })
    expect(Object.keys(requests[0]?.headers ?? {})).not.toContain('x-app-id')
  })

  it('rejects malformed input locally', async () => {
    const { transport, requests } = fakeTransport(() => jsonResponse(500, {}))
    const ai = new ScaleMuleAi({ transport })
    expect((await ai.runs.create({ operation: '', input: {} })).error?.code).toBe('INPUT_INVALID')
    expect((await ai.runs.create({ operation: 'summarize', input: 'nope' as unknown as Record<string, unknown> })).error?.code).toBe('INPUT_INVALID')
    expect(requests).toHaveLength(0)
  })
})

describe('ScaleMuleAi.runs.wait and summarize', () => {
  it('polls until terminal and returns the run', async () => {
    const statuses = ['queued', 'running', 'succeeded']
    const { transport, requests } = fakeTransport((_request, call) =>
      jsonResponse(200, { success: true, data: run({ status: statuses[Math.min(call - 1, 2)], output: { summary: 'ok', format: 'brief', truncated: false } }) }),
    )
    const ai = new ScaleMuleAi({ transport })
    const result = await ai.runs.wait('run-1', { pollIntervalMs: 1 })
    expect(result.data?.status).toBe('succeeded')
    expect(requests.every((request) => request.method === 'GET' && request.path === '/v1/ai/runs/run-1')).toBe(true)
    expect(requests.length).toBe(3)
  })

  it('times out without throwing and reports the last run', async () => {
    const { transport } = fakeTransport(() => jsonResponse(200, { success: true, data: run({ status: 'running' }) }))
    const ai = new ScaleMuleAi({ transport })
    const result = await ai.runs.wait('run-1', { timeoutMs: 5, pollIntervalMs: 1 })
    expect(result.error?.code).toBe('WAIT_TIMEOUT')
    expect(result.error?.run?.status).toBe('running')
  })

  it('summarize returns the output on success and the run error on failure', async () => {
    const ok = fakeTransport(() =>
      jsonResponse(201, { success: true, data: run({ status: 'succeeded', output: { summary: 'Привет 🚀', format: 'brief', truncated: true } }) }),
    )
    const okResult = await new ScaleMuleAi({ transport: ok.transport }).summarize({ text: 'long text', format: 'brief', max_characters: 100 })
    expect(okResult.data).toMatchObject({ summary: 'Привет 🚀', format: 'brief', truncated: true })
    expect(ok.requests[0]?.body).toEqual({ operation: 'summarize', input: { text: 'long text', format: 'brief', max_characters: 100 }, wait_ms: 20_000 })

    const failed = fakeTransport(() =>
      jsonResponse(202, { success: true, data: run({ status: 'failed', error: { code: 'OUTPUT_SCHEMA_INVALID', class: 'terminal', message: 'bad' } }) }),
    )
    const failedResult = await new ScaleMuleAi({ transport: failed.transport }).summarize({ text: 'x' })
    expect(failedResult.data).toBeNull()
    expect(failedResult.error).toMatchObject({ code: 'OUTPUT_SCHEMA_INVALID', run: { status: 'failed' } })

    const empty = await new ScaleMuleAi({ transport: ok.transport }).summarize({ text: '' })
    expect(empty.error?.code).toBe('INPUT_INVALID')
  })
})

describe('ScaleMuleAi.runs.events', () => {
  it('yields parsed events, sends Last-Event-ID and stops on end', async () => {
    const stream =
      'id: 2\nevent: running\ndata: {"run_id":"run-1","sequence":2,"type":"running","at":"t"}\n\n' +
      'id: 3\nevent: succeeded\ndata: {"run_id":"run-1","sequence":3,"type":"succeeded","at":"t","data":{"status":"succeeded"}}\n\n' +
      'event: end\ndata: terminal\n\n'
    const { transport, requests } = fakeTransport(() => streamResponse(stream))
    const ai = new ScaleMuleAi({ transport })
    const seen: number[] = []
    for await (const event of ai.runs.events('run-1', { after: 1 })) seen.push(event.sequence)
    expect(seen).toEqual([2, 3])
    expect(requests[0]?.accept).toBe('event-stream')
    expect(requests[0]?.headers?.['last-event-id']).toBe('1')
    expect(requests).toHaveLength(1)
  })

  it('throws AiFacadeError when the stream is refused', async () => {
    const { transport } = fakeTransport(() => jsonResponse(404, { success: false, error: { code: 'NOT_FOUND', message: 'nope' } }))
    const ai = new ScaleMuleAi({ transport })
    await expect(async () => {
      for await (const _event of ai.runs.events('run-x')) {
        // unreachable
      }
    }).rejects.toBeInstanceOf(AiFacadeError)
  })
})

describe('ScaleMuleAi resilience', () => {
  it('reconnects with Last-Event-ID after a mid-stream drop and keeps going', async () => {
    let call = 0
    const seenHeaders: Array<string | undefined> = []
    const transport: AiTransport = {
      send: vi.fn(async (request: AiTransportRequest) => {
        call += 1
        if (call === 1) {
          // Deliver one event, then fail the next read (a reset mid-stream).
          let pulls = 0
          const body = new ReadableStream<Uint8Array>({
            pull(controller) {
              pulls += 1
              if (pulls === 1) {
                controller.enqueue(new TextEncoder().encode('id: 1\nevent: queued\ndata: {"run_id":"r","sequence":1,"type":"queued","at":"t"}\n\n'))
              } else {
                controller.error(new Error('connection reset'))
              }
            },
          })
          return { status: 200, headers: new Headers(), body, json: async () => ({}), text: async () => '' }
        }
        seenHeaders.push(request.headers?.['last-event-id'])
        return streamResponse('id: 2\nevent: succeeded\ndata: {"run_id":"r","sequence":2,"type":"succeeded","at":"t"}\n\nevent: end\ndata: terminal\n\n')
      }),
    }
    const ai = new ScaleMuleAi({ transport })
    const seen: number[] = []
    for await (const event of ai.runs.events('r')) seen.push(event.sequence)
    expect(seen).toEqual([1, 2])
    expect(call).toBe(2)
    expect(seenHeaders).toEqual(['1'])
  })

  it('throws STREAM_INCOMPLETE instead of ending silently when reconnects are exhausted', async () => {
    const transport: AiTransport = {
      send: vi.fn(async () => streamResponse('id: 1\nevent: running\ndata: {"run_id":"r","sequence":1,"type":"running","at":"t"}\n\n')),
    }
    const ai = new ScaleMuleAi({ transport })
    await expect(async () => {
      for await (const _event of ai.runs.events('r', { maxReconnects: 0 })) {
        // consume
      }
    }).rejects.toMatchObject({ error: { code: 'STREAM_INCOMPLETE' } })
  })

  it('passes timeoutMs to the transport and honours Retry-After on 429', async () => {
    const { transport, requests } = fakeTransport((_request, call) =>
      call === 1
        ? jsonResponse(429, { success: false, error: { code: 'RATE_LIMIT_EXCEEDED', message: 'slow down' } }, { 'retry-after': '0' })
        : jsonResponse(200, { success: true, data: run() }),
    )
    const ai = new ScaleMuleAi({ transport })
    const result = await ai.runs.get('run-1', { timeoutMs: 1234 })
    expect(result.error).toBeNull()
    expect(requests[0]?.timeoutMs).toBe(1234)
    expect(requests).toHaveLength(2)
  })
})
