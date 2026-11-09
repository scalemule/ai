# @scalemule/ai

Typed client for the ScaleMule AI facade (`/v1/ai`). Framework-neutral, zero dependencies.

AI operations are platform-activated capabilities (today: `summarize`). You send the operation's
input; ScaleMule runs it under the account's governed agent, enforces budgets and returns a typed
result. There is no model, prompt or tool configuration on the client, by design.

```ts
import { createAi, createFetchTransport } from '@scalemule/ai'

const ai = createAi({
  transport: createFetchTransport({
    baseUrl: 'https://api.scalemule.com',
    headers: () => ({ 'x-api-key': process.env.SCALEMULE_API_KEY! }), // server-only, account-bound key
  }),
})

const { data, error } = await ai.summarize({ text: longText, format: 'bullets', max_characters: 600 })
if (error) console.error(error.code, error.message)
else console.log(data.summary, data.truncated)
```

Lower level: `ai.runs.create()`, `ai.runs.get()`, `ai.runs.wait()`, `ai.runs.cancel()` and
`for await (const event of ai.runs.events(runId))`.

Most apps should not use this package directly:

- Browser or universal code: `@scalemule/sdk` exposes the same API as `sm.ai`.
- Next.js: `@scalemule/nextjs/server/ai` gives you a cookie-session proxy (`createAiRoutes`) and a
  server-only client (`createServiceAiClient`).

Every create call carries an `x-idempotency-key` generated before the first attempt, so retries can
only ever return the same run. All methods return `{ data, error }` and never throw, except the
`events()` async iterator which throws `AiFacadeError` if the stream cannot be opened.
