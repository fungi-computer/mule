import { expect, it } from "vitest";
import * as Schema from "effect/Schema";
import { providerDisplayNames, type ModelFailure } from "../src/index.js";
import {
  ModelFailureSchema,
  providerDisplayNames as effectProviderNames,
} from "../src/effect.js";

it("both Mule doors export the same provider-name table", () => {
  expect(effectProviderNames).toBe(providerDisplayNames);
  expect(providerDisplayNames.get("openai-codex")).toBe("OpenAI");
  expect(providerDisplayNames.get("cloudflare-workers-ai")).toBe(
    "Cloudflare Workers AI",
  );
  expect(providerDisplayNames.get("custom-ai")).toBeUndefined();
});

it.each([
  { code: "usage-limit-reached", cause: "quota" },
  { code: "usage-limit-reached", resetAt: 1234, limit: "five_hour" },
  { code: "rate-limit", retryAfterMs: 1000 },
  { code: "overloaded" },
  { code: "provider_unavailable", status: 503 },
  { code: "provider_rejected", status: 400 },
  { code: "setup_required" },
  { code: "tools_unsupported" },
  { code: "request_failed" },
] satisfies ModelFailure["cause"][])(
  "round-trips typed $code facts without a text protocol",
  (cause) => {
    const failure = { provider: "openai-codex", cause };
    const codec = Schema.fromJsonString(ModelFailureSchema);
    const stored = Schema.encodeSync(codec)(failure);
    expect(Schema.decodeUnknownSync(codec)(stored)).toEqual(failure);
  },
);

it("rejects raw provider codes instead of promoting them to a typed cause", () => {
  expect(() =>
    Schema.decodeUnknownSync(ModelFailureSchema)({
      provider: "openai",
      cause: { code: "insufficient_quota" },
    }),
  ).toThrow();
});
