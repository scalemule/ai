import { describe, expect, it } from 'vitest'
import { SseParser, readSse } from './sse'

describe('SseParser', () => {
  it('parses ids, events, multi-line data, comments and CRLF', () => {
    const parser = new SseParser()
    const messages = parser.push(
      ': keepalive\n\nid: 3\nevent: succeeded\ndata: {"a":1,\ndata: "b":2}\n\nevent: end\r\ndata: terminal\r\n\r\n',
    )
    expect(messages).toEqual([
      { id: '3', event: 'succeeded', data: '{"a":1,\n"b":2}' },
      { id: undefined, event: 'end', data: 'terminal' },
    ])
  })

  it('reassembles UTF-8 sequences split across chunks', () => {
    const encoded = new TextEncoder().encode('data: {"summary":"Привет 🚀 日本"}\n\n')
    const parser = new SseParser()
    const first = parser.push(encoded.slice(0, 21))
    const second = parser.push(encoded.slice(21))
    expect(first).toEqual([])
    expect(second[0]?.data).toBe('{"summary":"Привет 🚀 日本"}')
  })

  it('reads a stream and flushes a trailing block', async () => {
    const chunks = ['id: 1\nevent: queued\ndata: {"sequence":1}\n\nid: 2\nevent: run', 'ning\ndata: {"sequence":2}']
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(new TextEncoder().encode(chunk))
        controller.close()
      },
    })
    const seen: string[] = []
    for await (const message of readSse(stream)) seen.push(`${message.id}:${message.event}`)
    expect(seen).toEqual(['1:queued', '2:running'])
  })
})
