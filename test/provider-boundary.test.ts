import { expect, it, vi } from "vitest";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import { normalizeModel, run, streamModel } from "../src/effect.js";
import { decodeProviderData } from "../src/provider-data.js";
import { fauxModel, type FauxResponseFactory } from "../src/testing.js";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import type { Api, Message, Model, ProviderData } from "../src/types.js";
import { fauxRun, userMessage } from "./support.js";

it("repairs resumed history before a host-supplied provider adapter sees it", async () => {
  const { settings } = fauxRun([fauxAssistantMessage("resumed")]);
  const stream = vi.fn(settings.stream);
  const messages: Message[] = [
    userMessage("read"),
    {
      role: "toolResult",
      toolCallId: "lost",
      toolName: "read",
      content: [{ type: "text", text: "orphan output" }],
      isError: false,
      timestamp: 1,
    },
    fauxAssistantMessage([
      { type: "toolCall", id: "pending", name: "read", arguments: {} },
    ]),
    userMessage("continue"),
  ];
  const before = structuredClone(messages);
  const outcome = await Effect.runPromise(
    run({ ...settings, messages, stream }),
  );
  expect(outcome.status).toBe("completed");
  expect(stream).toHaveBeenCalledOnce();
  expect(
    stream.mock.calls[0]?.[1].messages.filter(
      (message) => message.role === "toolResult",
    ),
  ).toEqual([
    {
      role: "toolResult",
      toolCallId: "pending",
      toolName: "read",
      content: [{ type: "text", text: "Tool execution interrupted." }],
      isError: true,
      timestamp: 1,
    },
  ]);
  expect(messages).toEqual(before);
});

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

it.each(["openai-responses", "anthropic-messages"] as const)(
  "%s resumes interrupted history without orphaned tool outputs",
  async (api) => {
    const selected = {
      ...model,
      api,
      provider: api === "openai-responses" ? "openai" : "anthropic",
    };
    const interrupted = {
      ...fauxAssistantMessage([
        { type: "toolCall", id: "lost", name: "read", arguments: {} },
      ]),
      api,
      provider: selected.provider,
      model: selected.id,
      stopReason: "aborted" as const,
    };
    // A persisted partial followed by its durable tool result after a restart.
    const messages: Message[] = [
      { role: "user", content: "Read the file", timestamp: 1 },
      interrupted,
      {
        role: "toolResult",
        toolCallId: "lost",
        toolName: "read",
        content: [{ type: "text", text: "orphan output" }],
        isError: false,
        timestamp: 2,
      },
      {
        ...interrupted,
        stopReason: "toolUse",
        content: [
          { type: "toolCall", id: "done", name: "read", arguments: {} },
          { type: "toolCall", id: "pending", name: "read", arguments: {} },
        ],
      },
      { role: "system", content: "Keep going", timestamp: 2 },
      {
        role: "toolResult",
        toolCallId: "done",
        toolName: "read",
        content: [{ type: "text", text: "saved result" }],
        isError: false,
        timestamp: 2,
      },
      {
        role: "toolResult",
        toolCallId: "done",
        toolName: "read",
        content: [{ type: "text", text: "duplicate output" }],
        isError: false,
        timestamp: 2,
      },
      { role: "user", content: "Continue", timestamp: 3 },
    ];
    const before = structuredClone(messages);
    const onPayload = vi.fn<(payload: ProviderData) => undefined>();
    const reply =
      api === "openai-responses"
        ? 'data: {"type":"response.completed","response":{"status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}\n\n'
        : 'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","content":[],"model":"test","usage":{"input_tokens":1,"output_tokens":0}}}\n\nevent: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}\n\nevent: message_stop\ndata: {"type":"message_stop"}\n\n';
    const stream = await streamModel(
      selected,
      { messages },
      {
        apiKey: "synthetic-test-key",
        onPayload,
        fetch: () =>
          Promise.resolve(
            new Response(reply, {
              headers: { "content-type": "text/event-stream" },
            }),
          ),
      },
    );
    expect((await stream.result()).stopReason).toBe("stop");
    expect(onPayload).toHaveBeenCalledOnce();
    expect(JSON.stringify(onPayload.mock.calls[0]?.[0])).not.toContain(
      "orphan output",
    );
    expect(JSON.stringify(onPayload.mock.calls[0]?.[0])).not.toContain(
      "duplicate output",
    );
    const payload = onPayload.mock.calls[0]?.[0];
    if (api === "openai-responses") {
      const input = Schema.decodeUnknownSync(
        Schema.Array(
          Schema.Record(
            Schema.String,
            Schema.Union([Schema.Json, Schema.Undefined]),
          ),
        ),
      )(payload?.input);
      expect(input).toContainEqual({
        type: "function_call_output",
        call_id: "done",
        output: "saved result",
      });
      expect(input).toContainEqual({
        type: "function_call_output",
        call_id: "pending",
        output: "Tool execution interrupted.",
      });
    } else {
      const requestMessages = Schema.decodeUnknownSync(
        Schema.Array(
          Schema.Struct({
            content: Schema.Union([
              Schema.String,
              Schema.Array(Schema.Record(Schema.String, Schema.Json)),
            ]),
          }),
        ),
      )(payload?.messages);
      const blocks = requestMessages.flatMap(({ content }) =>
        Schema.is(Schema.String)(content) ? [] : content,
      );
      expect(
        blocks.find((block) => block.tool_use_id === "done"),
      ).toMatchObject({
        type: "tool_result",
        content: "saved result",
      });
      expect(
        blocks.find((block) => block.tool_use_id === "pending"),
      ).toMatchObject({
        type: "tool_result",
        content: "Tool execution interrupted.",
        is_error: true,
      });
    }
    expect(messages).toEqual(before);
  },
);

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
