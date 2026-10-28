import { expect, expectTypeOf, it, vi } from "vitest";
import * as Effect from "effect/Effect";

import type { AssistantMessage } from "../src/types.js";
import type { MuleRunSettings } from "../src/contract.js";
import {
  isRetryableProviderRejection,
  observeProviderStatus,
  mule,
} from "../src/index.js";
import {
  isRetryableProviderRejection as effectIsRetryableProviderRejection,
  observeProviderStatus as effectObserveProviderStatus,
  run,
} from "../src/effect.js";
import { fauxModel } from "../src/testing.js";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";

it("requires a boolean-returning decision on every Mule run", () => {
  expectTypeOf<Pick<MuleRunSettings, "retryable">>().toEqualTypeOf<{
    readonly retryable: (message: AssistantMessage) => boolean;
  }>();
});

it("shares the request-local provider observer on both roots", () => {
  expect(effectObserveProviderStatus).toBe(observeProviderStatus);
  expect(effectIsRetryableProviderRejection).toBe(isRetryableProviderRejection);
  expect(observeProviderStatus().rejection()).toBeUndefined();
});

it("does not infer a faux provider retry decision from error prose", () => {
  const faux = fauxModel([]);
  expect(
    faux.retryable(
      fauxAssistantMessage("", {
        stopReason: "error",
        errorMessage: "503 rate limit retry me",
      }),
    ),
  ).toBe(false);
});

it.each([
  { root: "promise", enabled: false },
  { root: "promise", enabled: true },
  { root: "effect", enabled: false },
  { root: "effect", enabled: true },
])(
  "honors a typed terminal decision on $root with retry enabled=$enabled despite transient prose",
  async ({ root, enabled }) => {
    const faux = fauxModel([
      fauxAssistantMessage("", {
        stopReason: "error",
        errorMessage: "503 Service Unavailable",
      }),
      fauxAssistantMessage("recovered"),
    ]);
    const retryable = vi.fn(() => false);
    const settings: MuleRunSettings = {
      model: faux.model,
      stream: faux.stream,
      retryable,
      systemPrompt: "",
      messages: [{ role: "user", content: "Reply", timestamp: 1 }],
      retry: enabled
        ? { enabled: true, maxRetries: 1, baseDelayMs: 1, maxAgentDelayMs: 1 }
        : undefined,
    };
    const outcome =
      root === "promise"
        ? await mule.run(settings)
        : await Effect.runPromise(run(settings));
    expect(outcome.status).toBe("failed");
    expect(faux.state.callCount).toBe(1);
    expect(retryable).toHaveBeenCalledTimes(enabled ? 1 : 0);
  },
);

it.each(["promise", "effect"])(
  "honors a typed transient on %s despite terminal-looking prose",
  async (root) => {
    const faux = fauxModel([
      fauxAssistantMessage("", {
        stopReason: "error",
        errorMessage: "insufficient_quota billing exhausted",
      }),
      fauxAssistantMessage("recovered"),
    ]);
    const retryable = vi.fn(() => true);
    const settings: MuleRunSettings = {
      model: faux.model,
      stream: faux.stream,
      retryable,
      systemPrompt: "",
      messages: [{ role: "user", content: "Reply", timestamp: 1 }],
      retry: { enabled: true, maxRetries: 1, baseDelayMs: 1 },
    };
    const outcome =
      root === "promise"
        ? await mule.run(settings)
        : await Effect.runPromise(run(settings));
    expect(outcome.status).toBe("completed");
    expect(faux.state.callCount).toBe(2);
    expect(retryable).toHaveBeenCalledOnce();
  },
);
