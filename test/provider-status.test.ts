import { streamSimple } from "@earendil-works/pi-ai/api/openai-codex-responses";
import { openaiCodexProvider } from "@earendil-works/pi-ai/providers/openai-codex";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { expect, it, vi } from "vitest";

import { observeProviderStatus } from "../src/provider-status.js";
import { fauxModel } from "../src/testing.js";

it("retains a parsed subscription usage refusal after an HTTP 200 response", async () => {
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

it("preserves the retained structured HTTP 429 subscription refusal and response body", async () => {
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
    code: "provider_unavailable",
    status: 429,
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
      code: "provider_unavailable",
      status: 429,
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

it("observes Pi's maintained decoder before it normalizes an HTTP 200 failed response", async () => {
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
      onProviderStreamEvent: provider.onProviderStreamEvent,
    },
  ).result();
  expect(message.stopReason).toBe("error");
  expect(transport).toHaveBeenCalledTimes(1);
  expect(onProviderStreamEvent).toHaveBeenCalledExactlyOnceWith(failed, model);
  expect(provider.rejection()).toEqual({ code: "usage-limit-reached" });
});
