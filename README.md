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

## Choosing which agent runs an operation

Applications approve **options** (an agent + model pair under a stable key such as `coral`), set one
as the application default, and let accounts override it. End users pick among the options their
account enabled. Nothing on the client ever names an agent, model, provider key or prompt.

```ts
const { data: operations } = await ai.operations.list()
// [{ operation: 'summarize', selected: 'coral', selection_source: 'application',
//    user_choice_allowed: true, options: [{ key: 'coral', name: 'Coral', selectable: true, ... }] }]

await ai.operations.setPreference('summarize', 'fast')   // the signed-in user's own choice
await ai.operations.clearPreference('summarize')         // back to the account default

// Account admins:
await ai.operations.getSettings('summarize')
await ai.operations.updateSettings('summarize', {
  default_option: 'fast',          // or null to inherit the application default again
  allow_user_choice: true,         // or null to inherit
  enabled_options: ['fast', 'coral'],
  status: 'active',                // 'paused' switches the operation off for this account
})
```

Errors: `ACTIVATION_UNAVAILABLE` (nothing configured), `USER_CHOICE_DISABLED`, `OPTION_NOT_ALLOWED`,
`OPTION_UNKNOWN`, `OFFERING_RETIRED`, `ACCOUNT_OVERRIDE_DISABLED`, `ACCOUNT_ADMIN_REQUIRED`,
`USER_PRINCIPAL_REQUIRED` (service keys cannot hold a preference). A run whose configuration changed
between queue and execution ends with `ACTIVATION_CHANGED`; simply run it again.

Most apps should not use this package directly:

- Browser or universal code: `@scalemule/sdk` exposes the same API as `sm.ai`.
- Next.js: `@scalemule/nextjs/server/ai` gives you a cookie-session proxy (`createAiRoutes`) and a
  server-only client (`createServiceAiClient`).

Every create call carries an `x-idempotency-key` generated before the first attempt, so retries can
only ever return the same run. All methods return `{ data, error }` and never throw, except the
`events()` async iterator which throws `AiFacadeError` if the stream cannot be opened.
