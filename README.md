# Mule (`@fungi.computer/mule`)

Mule runs Pi's model and tool loop inside Effect. Use it when your host already
owns the model, messages and tools and needs an awaited, cancellable run. Pi's
models, messages, provider replay fields and stream events keep their original
types.

## Set up

```sh
npm install @fungi.computer/mule
```

The package ships ESM only and depends on `effect` and Pi
(`@earendil-works/pi-ai`) at runtime, plus `@fungi.computer/cairn` for IDs. Mule
does not require or install the MCP SDK. Consumers that type-check dependency
declarations (`skipLibCheck: false`) must install Pi’s Google GenAI optional
declaration dependency themselves:

```sh
npm install --save-dev @modelcontextprotocol/sdk
```

The root entrypoint exports the Promise facade, `mule.run`.
`@fungi.computer/mule/effect` exports the canonical `muleEffect.run`. The
Promise facade adapts tools, hooks and listeners into that same Effect program,
so public Promise declarations do not require Effect types.

## Quick start without a provider account

This deterministic example uses the public testing helper. It makes no network
request and needs no credential.

```ts
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { mule } from "@fungi.computer/mule";
import { fauxModel } from "@fungi.computer/mule/testing";

const faux = fauxModel([fauxAssistantMessage("pong")]);
const outcome = await mule.run(
  {
    model: faux.model,
    retryable: faux.retryable,
    stream: faux.stream,
    systemPrompt: "Reply once.",
    messages: [{ role: "user", content: "ping", timestamp: 1 }],
  },
  () => {},
);
console.log(outcome.status);
```

The result is `completed`. No active stream remains when the awaited run
settles. The [test helper](test/support.ts) uses this provider and input shape,
and [facade tests](test/promise-facade.test.ts) compare Promise and Effect
lifecycles. This fake provider checks orchestration rather than a real supplier.

For a real model, supply Pi's `Model<Api>` and either its normal `streamSimple`
or an explicit provider stream. The host supplies credentials and billing. Await
the listener: its completion is part of the run's lifecycle.

## Tools and control

A tool pairs Pi's descriptor with `prepare(call, signal)`. Preparation decodes
and authorizes the exact call. It returns either a typed rejection or a prepared
executor. The Promise executor yields parts through an `AsyncIterable`. The
Effect executor uses a Stream. Every executor must observe cancellation.

Run, turn, message and tool events carry causal identities. They let the host
record which execution produced an observation. They do not create durable
Session storage. Hosts may select parallel or sequential tool execution, steer
active work and queue follow-up input through the existing control interfaces.

A tool opts into safe crash replay only when it is read-only or explicitly
idempotent. The host persists the checkpoints needed to use `resume`. Declaring
`replay: "safe"` does not make an external write idempotent. Pi's provider
messages and replay fields pass through unchanged.

## Cancellation, retries and errors

Abort the Promise facade's signal or interrupt the Effect fiber. Mule aborts the
Pi stream and running tools and records one cancelled `run_end`. The Promise
facade preserves an `Error` abort reason and rejects with it. A non-`Error`
reason becomes an `AbortError`. An operation that ignores abort still needs its
host to account for the backing work.

Tool preparation rejections and execution failures become model-visible error
results, so the run can continue. Fatal model or control failures produce a
failed `run_end`. A failed listener cannot be trusted to record that terminal
event. Preserve the rejection and its cause when reporting it.

`retry` selects Pi's bounded retry policy for each assistant call. Omitting it
means no retry. Provider Retry-After evidence and retry observations use the
existing run interfaces. Mule does not choose a provider policy for your host.
`modelStreamIdleTimeoutMs` bounds silence between provider events.

Mule owns no model catalog, credential store, durable Session database,
compaction service or process-restart scheduler. Compose those in the host.

## API and examples

Use the [generated API reference](docs/api/README.md) and the exports in
[package.json](package.json).
[Cancellation parity](test/cancellation-parity.test.ts),
[retry policy](test/retry-policy.test.ts) and
[tool batch](test/tool-batch.test.ts) tests exercise the shared loop.

[The prompt example](examples/prompt.ts) is an optional live-provider caller. It
requires `CAMEL_API_KEY` supplied through your local environment. Its endpoint
and model overrides belong to that example, not to Mule's API. It is not needed
for the deterministic quick start. Do not commit credentials.

## Contribute and license

Add an orchestration regression using the existing faux provider. From the root:

```sh
pnpm --dir packages/mule run test
pnpm --dir packages/mule run docs
```

MIT. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).
