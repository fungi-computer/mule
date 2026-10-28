import { streamSimple } from "@earendil-works/pi-ai/api/openai-codex-responses";
import { openaiCodexProvider } from "@earendil-works/pi-ai/providers/openai-codex";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { expect, it, vi } from "vitest";

import { decodeProviderData } from "../src/provider-data.js";
import { normalizeModel } from "../src/provider-model.js";
import {
  isRetryableProviderRejection,
  observeProviderStatus,
} from "../src/provider-status.js";
import { fauxModel } from "../src/testing.js";
import { streamPi } from "../src/provider.js";

it("owns the retry decision for every typed provider rejection", () => {
  expect(isRetryableProviderRejection(undefined)).toBe(false);
  expect(isRetryableProviderRejection({ code: "usage-limit-reached" })).toBe(
    false,
  );
  expect(
    isRetryableProviderRejection({ code: "provider_rejected", status: 400 }),
  ).toBe(false);
  expect(isRetryableProviderRejection({ code: "provider_unavailable" })).toBe(
    true,
  );
  expect(
    isRetryableProviderRejection({ code: "rate-limit", retryAfterMs: 0 }),
  ).toBe(true);
  expect(isRetryableProviderRejection({ code: "overloaded" })).toBe(true);
});

it("classifies the captured Anthropic account rate-limit body without inventing a subscription reset", async () => {
  // Captured in .fungi/agents/local_store_kn4kpus798sgau03jhrxd/agent.sqlite,
  // session_entries.providerError, 2026-10-27. No headers were retained.
  // Request identity removed; no credential was present in the body.
  const body = {
    type: "error",
    error: {
      type: "rate_limit_error",
      message:
        "This request would exceed your account's rate limit. Please try again later.",
    },
  };
  const provider = observeProviderStatus({
    fetch: () => Promise.resolve(Response.json(body, { status: 429 })),
  });
  await provider.fetch("https://api.anthropic.com/v1/messages");
  expect(provider.rejection()).toEqual({ code: "rate-limit" });
});

it("decodes documented Anthropic unified rejected headers into bounded subscription facts, never prose", async () => {
  // Header shape: Claude Code's unified rate-limit contract; not a captured
  // Fungi response (the persisted SDK error above did not retain headers).
  const provider = observeProviderStatus({
    fetch: () =>
      Promise.resolve(
        Response.json(
          {
            type: "error",
            error: { type: "rate_limit_error", message: "ignored" },
          },
          {
            status: 429,
            headers: {
              "anthropic-ratelimit-unified-status": "rejected",
              "anthropic-ratelimit-unified-reset": "1793116800",
              "anthropic-ratelimit-unified-representative-claim": "five_hour",
              "retry-after": "0",
            },
          },
        ),
      ),
  });
  await provider.fetch("https://api.anthropic.com/v1/messages");
  expect(provider.rejection()).toEqual({
    code: "usage-limit-reached",
    resetAt: 1793116800000,
    limit: "five_hour",
  });
});

it.each([
  ["12", 12000],
  ["0", 0],
  ["Infinity", undefined],
  ["-1", undefined],
  ["9999999999999999999", undefined],
  ["not a delay", undefined],
])(
  "decodes documented Retry-After %s without unbounded facts",
  async (header, expected) => {
    const provider = observeProviderStatus({
      fetch: () =>
        Promise.resolve(
          Response.json(
            { error: { code: "rate_limit_exceeded" } },
            { status: 429, headers: { "retry-after": header } },
          ),
        ),
    });
    await provider.fetch("https://api.openai.com/v1/responses");
    expect(provider.rejection()).toEqual(
      expected === undefined
        ? { code: "rate-limit" }
        : { code: "rate-limit", retryAfterMs: expected },
    );
  },
);

it("recognizes a documented Anthropic overloaded stream event through Pi's parsed-event hook", async () => {
  const provider = observeProviderStatus();
  await provider.onProviderStreamEvent(
    {
      type: "error",
      error: { type: "overloaded_error", message: "ignored" },
    },
    fauxModel([]).model,
  );
  expect(provider.rejection()).toEqual({ code: "overloaded" });
});

it("retains a documented Anthropic overload that Pi throws before its parsed-event hook", async () => {
  const [selected] = anthropicProvider().getModels();
  if (selected === undefined)
    throw new Error("Pi must supply an Anthropic model");
  const model = normalizeModel(selected);
  const body = {
    type: "error",
    error: { type: "overloaded_error", message: "Overloaded" },
  };
  const transport = vi.fn(() =>
    Promise.resolve(
      new Response(`event: error\ndata: ${JSON.stringify(body)}\n\n`, {
        headers: { "content-type": "text/event-stream" },
      }),
    ),
  );
  const onProviderStreamEvent = vi.fn();
  const provider = observeProviderStatus({
    fetch: transport,
    onProviderStreamEvent,
  });
  const stream = await streamPi(
    model,
    { messages: [] },
    {
      apiKey: "synthetic-anthropic-key",
      maxRetries: 0,
      fetch: provider.fetch,
      onProviderStreamEvent: provider.onProviderStreamEvent,
    },
  );
  const message = await stream.result();
  expect(message.stopReason).toBe("error");
  expect(transport).toHaveBeenCalledTimes(1);
  // Pi 1.3.0 iterateAnthropicEvents throws the JSON error envelope before
  // yielding; its catch retains that envelope as the terminal errorMessage.
  expect(onProviderStreamEvent).not.toHaveBeenCalled();
  expect(provider.rejection(message)).toEqual({ code: "overloaded" });
});

it.each(["requests", "tokens", "input-tokens", "output-tokens"])(
  "decodes the documented exhausted Anthropic API-key %s reset without calling it a subscription cap",
  async (dimension) => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-27T12:00:00Z"));
    try {
      const provider = observeProviderStatus({
        fetch: () =>
          Promise.resolve(
            Response.json(
              { error: { type: "rate_limit_error" } },
              {
                status: 429,
                headers: {
                  [`anthropic-ratelimit-${dimension}-remaining`]: "0",
                  [`anthropic-ratelimit-${dimension}-reset`]:
                    "2026-10-27T12:00:15Z",
                },
              },
            ),
          ),
      });
      await provider.fetch("https://api.anthropic.com/v1/messages");
      expect(provider.rejection()).toEqual({
        code: "rate-limit",
        retryAfterMs: 15000,
      });
    } finally {
      vi.useRealTimers();
    }
  },
);

it("waits for the latest known exhausted API window or Retry-After floor", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-27T12:00:00Z"));
  try {
    const provider = observeProviderStatus({
      fetch: () =>
        Promise.resolve(
          Response.json(
            { error: { type: "rate_limit_error" } },
            {
              status: 429,
              headers: {
                "retry-after": "5",
                "anthropic-ratelimit-requests-remaining": "0",
                "anthropic-ratelimit-requests-reset": "2026-10-27T12:00:15Z",
                "anthropic-ratelimit-input-tokens-remaining": "0",
                "anthropic-ratelimit-input-tokens-reset":
                  "2026-10-27T12:00:30Z",
              },
            },
          ),
        ),
    });
    await provider.fetch("https://api.anthropic.com/v1/messages");
    expect(provider.rejection()).toEqual({
      code: "rate-limit",
      retryAfterMs: 30000,
    });
  } finally {
    vi.useRealTimers();
  }
});

it.each([
  "overloaded_error: try again",
  JSON.stringify({
    error: { type: "invalid_request_error", message: "overloaded_error" },
  }),
  JSON.stringify({
    error: { type: "overloaded_error", message: "x".repeat(16 * 1024) },
  }),
])(
  "does not classify unknown or oversized terminal prose as a structured stream overload",
  (errorMessage) => {
    const provider = observeProviderStatus();
    expect(
      provider.rejection(
        fauxAssistantMessage("", {
          stopReason: "error",
          errorMessage,
        }),
      ),
    ).toBeUndefined();
  },
);

it.each([
  ["1", "2026-10-27T12:00:15Z"],
  ["0", "not a date"],
  ["0", "2026-10-27T11:59:59Z"],
  ["0", "2026-10-27T12:02:00Z"],
])(
  "does not invent bounded API-key timing from remaining=%s reset=%s",
  async (remaining, reset) => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-27T12:00:00Z"));
    try {
      const provider = observeProviderStatus({
        fetch: () =>
          Promise.resolve(
            Response.json(
              { error: { type: "rate_limit_error" } },
              {
                status: 429,
                headers: {
                  "anthropic-ratelimit-requests-remaining": remaining,
                  "anthropic-ratelimit-requests-reset": reset,
                },
              },
            ),
          ),
      });
      await provider.fetch("https://api.anthropic.com/v1/messages");
      expect(provider.rejection()).toEqual({ code: "rate-limit" });
    } finally {
      vi.useRealTimers();
    }
  },
);

it("recognizes a documented Anthropic HTTP 529 overloaded response", async () => {
  const provider = observeProviderStatus({
    fetch: () =>
      Promise.resolve(
        Response.json(
          {
            type: "error",
            error: { type: "overloaded_error", message: "ignored" },
          },
          { status: 529 },
        ),
      ),
  });
  await provider.fetch("https://api.anthropic.com/v1/messages");
  expect(provider.rejection()).toEqual({ code: "overloaded" });
});

it("keeps undocumented unified claims and invalid reset times unknown", async () => {
  const provider = observeProviderStatus({
    fetch: () =>
      Promise.resolve(
        Response.json(
          { type: "error", error: { type: "rate_limit_error" } },
          {
            status: 429,
            headers: {
              "anthropic-ratelimit-unified-status": "rejected",
              "anthropic-ratelimit-unified-reset": "Infinity",
              "anthropic-ratelimit-unified-representative-claim":
                "some_new_window",
            },
          },
        ),
      ),
  });
  await provider.fetch("https://api.anthropic.com/v1/messages");
  expect(provider.rejection()).toEqual({ code: "usage-limit-reached" });
});

it("decodes a documented HTTP Retry-After date using the maintained date decoder", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-27T14:00:00Z"));
  try {
    const provider = observeProviderStatus({
      fetch: () =>
        Promise.resolve(
          new Response(null, {
            status: 429,
            headers: { "retry-after": "Tue, 27 Oct 2026 14:00:15 GMT" },
          }),
        ),
    });
    await provider.fetch("https://provider.invalid/model");
    expect(provider.rejection()).toEqual({
      code: "rate-limit",
      retryAfterMs: 15000,
    });
  } finally {
    vi.useRealTimers();
  }
});

it("retains a documented OpenAI parsed subscription usage refusal after an HTTP 200 response", async () => {
  // Pi 1.3.0's Codex mapCodexEvents delivers this parsed JSON envelope to
  // onProviderStreamEvent before normalizing response.failed into an Error.
  const provider = observeProviderStatus({
    fetch: () => Promise.resolve(new Response(null, { status: 200 })),
  });
  const response = await provider.fetch("https://provider.invalid/responses");
  expect(response.status).toBe(200);
  await provider.onProviderStreamEvent(
    {
      type: "response.failed",
      sequence_number: 1,
      response: {
        id: "response_usage_limit",
        status: "failed",
        error: {
          code: "subscription_sharing_usage_limit_exceeded",
          message: "503 service unavailable",
        },
      },
    },
    fauxModel([]).model,
  );
  expect(provider.rejection()).toEqual({ code: "usage-limit-reached" });
});

it("retains typed HTTP evidence without replacing the request transport or consuming its response", async () => {
  const response = new Response("provider text is not classification", {
    status: 503,
  });
  const transport = vi.fn(() => Promise.resolve(response));
  const provider = observeProviderStatus({ fetch: transport });
  const init = { signal: new AbortController().signal };
  expect(await provider.fetch("https://provider.invalid/responses", init)).toBe(
    response,
  );
  expect(transport).toHaveBeenCalledExactlyOnceWith(
    "https://provider.invalid/responses",
    init,
  );
  expect(provider.rejection()).toEqual({
    code: "provider_unavailable",
    status: 503,
  });
  expect(await response.text()).toBe("provider text is not classification");
});

it("preserves the documented OpenAI structured HTTP 429 subscription refusal and response body", async () => {
  const body = {
    error: {
      code: "subscription_sharing_usage_limit_exceeded",
      message: "503 service unavailable",
    },
  };
  const response = Response.json(body, { status: 429 });
  const provider = observeProviderStatus({
    fetch: () => Promise.resolve(response),
  });
  expect(await provider.fetch("https://provider.invalid/responses")).toBe(
    response,
  );
  expect(provider.rejection()).toEqual({ code: "usage-limit-reached" });
  expect(await response.json()).toEqual(body);
});

it("keeps an HTTP 429 insufficient_quota refusal terminal despite transient prose", async () => {
  const provider = observeProviderStatus({
    fetch: () =>
      Promise.resolve(
        Response.json(
          { error: { code: "insufficient_quota", message: "503 retry me" } },
          { status: 429 },
        ),
      ),
  });
  await provider.fetch("https://provider.invalid/responses");
  expect(provider.rejection()).toEqual({
    code: "provider_rejected",
    status: 429,
  });
});

it("records transport failure without replacing the thrown cause", async () => {
  const cause = new TypeError("transport failed");
  const provider = observeProviderStatus({
    fetch: () => Promise.reject(cause),
  });
  await expect(
    provider.fetch("https://provider.invalid/responses"),
  ).rejects.toBe(cause);
  expect(provider.rejection()).toEqual({ code: "provider_unavailable" });
});

it("preserves cancellation without classifying it as provider unavailability", async () => {
  const controller = new AbortController();
  const cause = new DOMException("Cancelled", "AbortError");
  controller.abort(cause);
  const provider = observeProviderStatus({
    fetch: () => Promise.reject(cause),
  });
  await expect(
    provider.fetch("https://provider.invalid/responses", {
      signal: controller.signal,
    }),
  ).rejects.toBe(cause);
  expect(provider.rejection()).toBeUndefined();
});

it("does not infer a usage refusal from error prose or unrelated parsed events", async () => {
  const provider = observeProviderStatus();
  const model = fauxModel([]).model;
  await provider.onProviderStreamEvent(
    {
      type: "response.failed",
      response: {
        error: {
          code: "other",
          message: "subscription_sharing_usage_limit_exceeded",
        },
      },
    },
    model,
  );
  await provider.onProviderStreamEvent(
    {
      type: "response.output_text.delta",
      delta: "subscription_sharing_usage_limit_exceeded",
    },
    model,
  );
  expect(provider.rejection()).toBeUndefined();
});

it("bounds the retained JSON error copy without consuming the original body", async () => {
  const body = {
    error: {
      code: "subscription_sharing_usage_limit_exceeded",
      message: "x".repeat(16 * 1024),
    },
  };
  const response = Response.json(body, { status: 429 });
  const provider = observeProviderStatus({
    fetch: () => Promise.resolve(response),
  });
  expect(await provider.fetch("https://provider.invalid/responses")).toBe(
    response,
  );
  expect(provider.rejection()).toEqual({
    code: "rate-limit",
  });
  expect(await response.json()).toEqual(body);
});

it("keeps evidence request-local and replaces it when a new HTTP request succeeds", async () => {
  const response = Response.json(
    { error: { code: "subscription_sharing_usage_limit_exceeded" } },
    { status: 429 },
  );
  const transport = vi
    .fn()
    .mockResolvedValueOnce(response)
    .mockResolvedValueOnce(new Response(null, { status: 200 }));
  const provider = observeProviderStatus({ fetch: transport });
  await provider.fetch("https://provider.invalid/responses");
  expect(provider.rejection()).toEqual({ code: "usage-limit-reached" });
  expect(observeProviderStatus().rejection()).toBeUndefined();
  await provider.fetch("https://provider.invalid/responses");
  expect(provider.rejection()).toBeUndefined();
});

it("reports error-copy cancellation failure without replacing the provider response", async () => {
  const cause = new Error("provider body cancellation failed");
  const report = vi.spyOn(console, "error").mockImplementation(() => undefined);
  try {
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array(16 * 1024 + 1));
        },
        cancel() {
          return Promise.reject(cause);
        },
      }),
      { status: 429 },
    );
    const provider = observeProviderStatus({
      fetch: () => Promise.resolve(response),
    });
    expect(await provider.fetch("https://provider.invalid/responses")).toBe(
      response,
    );
    expect(provider.rejection()).toEqual({
      code: "rate-limit",
    });
    await expect(response.body?.cancel()).rejects.toBe(cause);
    await vi.waitFor(() => {
      expect(report).toHaveBeenCalledExactlyOnceWith(
        "Provider error-copy cancellation failed",
        cause,
      );
    });
  } finally {
    report.mockRestore();
  }
});

it("propagates the composed parsed-event observer's rejection", async () => {
  const cause = new Error("observer failed");
  const provider = observeProviderStatus({
    onProviderStreamEvent: () => Promise.reject(cause),
  });
  await expect(
    provider.onProviderStreamEvent(
      { type: "response.output_text.delta", delta: "hello" },
      fauxModel([]).model,
    ),
  ).rejects.toBe(cause);
});

it("observes Pi's maintained decoder before it normalizes a documented OpenAI HTTP 200 failed response", async () => {
  const [model] = openaiCodexProvider().getModels();
  if (model === undefined) throw new Error("Pi must supply a Codex model");
  const failed = {
    type: "response.failed",
    sequence_number: 1,
    response: {
      id: "response_usage_limit",
      status: "failed",
      error: {
        code: "subscription_sharing_usage_limit_exceeded",
        message: "503 service unavailable",
      },
    },
  };
  const transport = vi.fn(() =>
    Promise.resolve(
      new Response(`data: ${JSON.stringify(failed)}\n\n`, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    ),
  );
  const onProviderStreamEvent = vi.fn();
  const provider = observeProviderStatus({
    fetch: transport,
    onProviderStreamEvent,
  });
  // Entirely synthetic SDK transport input, never a credential store or login.
  const payload = Buffer.from(
    JSON.stringify({
      "https://api.openai.com/auth": { chatgpt_account_id: "test-account" },
    }),
  ).toString("base64");
  const message = await streamSimple(
    model,
    normalizeContext({ messages: [] }),
    {
      apiKey: `e30.${payload}.test`,
      transport: "sse",
      maxRetries: 0,
      fetch: provider.fetch,
      onProviderStreamEvent: (data, selected) =>
        provider.onProviderStreamEvent(
          decodeProviderData(data),
          normalizeModel(selected),
        ),
    },
  ).result();
  expect(message.stopReason).toBe("error");
  expect(transport).toHaveBeenCalledTimes(1);
  expect(onProviderStreamEvent).toHaveBeenCalledExactlyOnceWith(failed, model);
  expect(provider.rejection()).toEqual({ code: "usage-limit-reached" });
});
