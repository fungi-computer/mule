import { expect, it, vi } from "vitest";
import * as Schema from "effect/Schema";
import { normalizeModel, streamModel } from "../src/effect.js";
import { decodeProviderData } from "../src/provider-data.js";
import { fauxModel, type FauxResponseFactory } from "../src/testing.js";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import type { Api, Model, ProviderData } from "../src/types.js";

const model: Model<"openai-completions"> = {
  id: "typed-boundary-test",
  name: "Typed boundary test",
  provider: "openai",
  api: "openai-completions",
  baseUrl: "https://provider.invalid/v1",
  reasoning: false,
  input: ["text"],
  contextWindow: 4096,
  maxTokens: 128,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};

it("retains catalog sampling defaults and thinking-level overrides through validation", () => {
  const catalogModel = {
    ...model,
    samplingParams: { top_p: 0.9, stop: ["end"], extra: { enabled: true } },
    samplingParamsByThinkingLevel: { high: { top_k: 40 } },
  };
  expect(normalizeModel(catalogModel)).toEqual(catalogModel);
});

it("accepts provider optional fields and Bedrock binary image content", () => {
  const payload = {
    modelId: "bedrock-test",
    toolConfig: undefined,
    messages: [
      {
        role: "user",
        content: [{ image: { source: { bytes: new Uint8Array([1, 2]) } } }],
      },
    ],
  };
  expect(decodeProviderData(payload)).toEqual(payload);
});

it("preserves Google's request cancellation signal in a decoded payload", () => {
  const controller = new AbortController();
  const decoded = decodeProviderData({
    model: "google-test",
    config: { abortSignal: controller.signal },
  });
  expect(decoded).toEqual({
    model: "google-test",
    config: { abortSignal: controller.signal },
  });
  const readSignal = Schema.decodeUnknownSync(
    Schema.Struct({
      config: Schema.Struct({ abortSignal: Schema.instanceOf(AbortSignal) }),
    }),
  );
  expect(readSignal(decoded).config.abortSignal).toBe(controller.signal);
});

it("awaits a payload replacement and observes the real Pi provider's parsed events", async () => {
  const reply =
    [
      {
        id: "test",
        choices: [
          {
            index: 0,
            delta: { role: "assistant", content: "hello" },
            finish_reason: null,
          },
        ],
      },
      {
        id: "test",
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
        usage: { prompt_tokens: 4, completion_tokens: 2, total_tokens: 6 },
      },
    ]
      .map((event) => `data: ${JSON.stringify(event)}\n\n`)
      .join("") + "data: [DONE]\n\n";
  const decodeRequest = Schema.decodeUnknownSync(
    Schema.fromJsonString(
      Schema.Struct({ temperature: Schema.Number, top_p: Schema.Number }),
    ),
  );
  const transport = vi.fn<typeof globalThis.fetch>((_input, init) => {
    expect(decodeRequest(init?.body)).toEqual({ temperature: 0.6, top_p: 0.8 });
    return Promise.resolve(
      new Response(reply, { headers: { "content-type": "text/event-stream" } }),
    );
  });
  const events: ProviderData[] = [];
  const onPayload = vi.fn((payload: ProviderData, selected: Model<Api>) => {
    expect(selected.id).toBe(model.id);
    return Promise.resolve({ ...payload, temperature: 0.6 });
  });
  const stream = await streamModel(
    model,
    { messages: [{ role: "user", content: "hello", timestamp: 1 }] },
    {
      apiKey: "synthetic-test-key",
      fetch: transport,
      samplingParams: { top_p: 0.8 },
      onPayload,
      onProviderStreamEvent: (event) => {
        events.push(event);
      },
    },
  );
  expect((await stream.result()).stopReason).toBe("stop");
  expect(onPayload).toHaveBeenCalledOnce();
  expect(transport).toHaveBeenCalledOnce();
  expect(events).toHaveLength(2);
});

it("keeps request options and factory state on the maintained faux provider", async () => {
  const factory = vi.fn<FauxResponseFactory>((_context, options, state) => {
    expect(options?.samplingParams).toEqual({ top_k: 12 });
    expect(options?.metadata).toEqual({ purpose: "test" });
    expect(state.callCount).toBe(1);
    return fauxAssistantMessage("done");
  });
  const faux = fauxModel([factory]);
  const stream = faux.stream(
    faux.model,
    { messages: [] },
    {
      samplingParams: { top_k: 12 },
      metadata: { purpose: "test" },
    },
  );
  expect((await stream.result()).stopReason).toBe("stop");
  expect(factory).toHaveBeenCalledOnce();
});
