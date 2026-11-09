import { describe, expect, it, vi } from 'vitest'
import { createFetchTransport, sanitizeHeaders } from './transport'

describe('createFetchTransport', () => {
  it('adds credentials, JSON content type and strips gateway-owned headers', async () => {
    const fetcher = vi.fn(async () => new Response(JSON.stringify({ success: true, data: {} }), { status: 200 }))
    const transport = createFetchTransport({
      baseUrl: 'https://api.example.com/',
      headers: async () => ({ 'x-api-key': 'pk', Authorization: 'Bearer t', 'x-app-id': 'forged', 'x-sm-internal-token': 'forged' }),
      fetch: fetcher as unknown as typeof fetch,
    })
    const response = await transport.send({ method: 'POST', path: '/v1/ai/runs', body: { a: 1 }, headers: { 'x-idempotency-key': 'k', 'x-effective-account-id': 'forged' } })
    expect(response.status).toBe(200)
    const [url, init] = fetcher.mock.calls[0] as unknown as [string, RequestInit]
    expect(url).toBe('https://api.example.com/v1/ai/runs')
    const headers = init.headers as Headers
    expect(headers.get('x-api-key')).toBe('pk')
    expect(headers.get('authorization')).toBe('Bearer t')
    expect(headers.get('x-idempotency-key')).toBe('k')
    expect(headers.get('content-type')).toBe('application/json')
    expect(headers.get('accept')).toBe('application/json')
    for (const name of ['x-app-id', 'x-sm-internal-token', 'x-effective-account-id']) expect(headers.has(name)).toBe(false)
    expect(init.redirect).toBe('error')
    expect(init.body).toBe('{"a":1}')
  })

  it('requests event streams with the right accept header', async () => {
    const fetcher = vi.fn(async () => new Response('', { status: 200 }))
    const transport = createFetchTransport({ baseUrl: 'https://api.example.com', fetch: fetcher as unknown as typeof fetch })
    await transport.send({ method: 'GET', path: '/v1/ai/runs/r/events', accept: 'event-stream' })
    const [, init] = fetcher.mock.calls[0] as unknown as [string, RequestInit]
    expect((init.headers as Headers).get('accept')).toBe('text/event-stream')
  })

  it('sanitizeHeaders is case-insensitive', () => {
    expect(sanitizeHeaders({ 'X-App-Id': 'x', 'X-Custom': 'y' })).toEqual({ 'X-Custom': 'y' })
  })
})

describe('createFetchTransport lifecycle', () => {
  it('keeps the abort listener armed until a JSON body is consumed', async () => {
    const controllerRef: { signal?: AbortSignal } = {}
    const fetcher = vi.fn(async (_url: string, init: RequestInit) => {
      controllerRef.signal = init.signal as AbortSignal
      return new Response(JSON.stringify({ ok: true }), { status: 200 })
    })
    const transport = createFetchTransport({ baseUrl: 'https://api.example.com', fetch: fetcher as unknown as typeof fetch })
    const caller = new AbortController()
    const response = await transport.send({ method: 'GET', path: '/v1/ai/runs/r', signal: caller.signal })
    expect(controllerRef.signal?.aborted).toBe(false)
    caller.abort()
    expect(controllerRef.signal?.aborted).toBe(true)
    await response.json()
  })

  it('blocks every gateway-owned header family', () => {
    for (const name of ['x-sm-internal-token', 'X-App-Key-Type', 'x-user-email', 'x-member-role-level', 'x-actor-type', 'x-effective-account-id', 'x-geo-country']) {
      expect(sanitizeHeaders({ [name]: 'x' })).toEqual({})
    }
    expect(sanitizeHeaders({ 'x-sm-workspace-id': 'w', 'x-requested-account-id': 'a', 'x-idempotency-key': 'k' })).toEqual({ 'x-sm-workspace-id': 'w', 'x-requested-account-id': 'a', 'x-idempotency-key': 'k' })
  })
})
