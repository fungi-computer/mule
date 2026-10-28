import type { AssistantMessage, SimpleStreamOptions } from "./types.js";
import { DateTime, Option, Schema } from "effect";

const usageLimitCode = "subscription_sharing_usage_limit_exceeded";
const providerError = Schema.Struct({
  error: Schema.Struct({
    code: Schema.optional(Schema.String),
    type: Schema.optional(Schema.String),
  }),
});
const decodeFailedEvent = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("response.failed"),
    response: providerError,
  }),
);
const decodeErrorReply = Schema.decodeUnknownOption(
  Schema.fromJsonString(providerError),
);
const decodeBodyChunk = Schema.decodeUnknownSync(Schema.instanceOf(Uint8Array));
const errorBodyBytes = 16 * 1024;
const decodeSeconds = Schema.decodeUnknownOption(
  Schema.NumberFromString.check(Schema.isBetween({ minimum: 0, maximum: 60 })),
);
const decodeReset = Schema.decodeUnknownOption(
  Schema.NumberFromString.check(
    Schema.isInt(),
    Schema.isBetween({ minimum: 0, maximum: 8_640_000_000_000 }),
  ),
);
const decodeLimit = Schema.decodeUnknownOption(
  Schema.Literals([
    "five_hour",
    "seven_day",
    "seven_day_opus",
    "seven_day_sonnet",
  ]),
);
const decodeRejected = Schema.decodeUnknownOption(Schema.Literal("rejected"));
const decodeExhausted = Schema.decodeUnknownOption(Schema.Literal("0"));
const decodeDate = Schema.decodeUnknownOption(Schema.DateTimeUtcFromString);
const decodeOverloaded = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("error"),
    error: Schema.Struct({ type: Schema.Literal("overloaded_error") }),
  }),
);

function resetDelay(value: string | null, now: number): number | undefined {
  const date = decodeDate(value);
  if (Option.isNone(date)) return undefined;
  const delay = DateTime.toEpochMillis(date.value) - now;
  return delay >= 0 && delay <= 60_000 ? Math.ceil(delay) : undefined;
}

function rateLimit(headers: Headers): ProviderRejection {
  const now = Date.now();
  const header = headers.get("retry-after");
  const seconds = decodeSeconds(header);
  let delay = Option.isSome(seconds)
    ? Math.ceil(seconds.value * 1000)
    : resetDelay(header, now);
  // API-key rate windows are not subscription caps. Only an explicitly
  // exhausted dimension supplies a retry floor; missing facts stay unknown.
  for (const dimension of [
    "requests",
    "tokens",
    "input-tokens",
    "output-tokens",
  ]) {
    if (
      Option.isNone(
        decodeExhausted(
          headers.get(`anthropic-ratelimit-${dimension}-remaining`),
        ),
      )
    )
      continue;
    const reset = resetDelay(
      headers.get(`anthropic-ratelimit-${dimension}-reset`),
      now,
    );
    if (reset !== undefined) delay = Math.max(delay ?? 0, reset);
  }
  return delay !== undefined
    ? { code: "rate-limit", retryAfterMs: delay }
    : { code: "rate-limit" };
}

function usageLimit(headers: Headers): ProviderRejection {
  const reset = decodeReset(headers.get("anthropic-ratelimit-unified-reset"));
  const limit = decodeLimit(
    headers.get("anthropic-ratelimit-unified-representative-claim"),
  );
  let result: ProviderRejection = { code: "usage-limit-reached" };
  if (Option.isSome(reset)) result = { ...result, resetAt: reset.value * 1000 };
  if (Option.isSome(limit)) result = { ...result, limit: limit.value };
  return result;
}

/** Pi's pinned provider-request transient classes, kept as typed evidence. */
const transientStatus = (status: number): boolean =>
  status === 408 ||
  status === 409 ||
  status === 429 ||
  (status >= 500 && status <= 599);

/** Retain Local's bounded error-copy observation; Pi still owns its response. */
async function providerErrorKind(
  response: Response,
): Promise<string | undefined> {
  const body = response.clone().body;
  if (body === null) return undefined;
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let bytes = 0;
  let text = "";
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      const value = decodeBodyChunk(chunk.value);
      bytes += value.byteLength;
      if (bytes > errorBodyBytes) return undefined;
      text += decoder.decode(value, { stream: true });
    }
    text += decoder.decode();
    const decoded = decodeErrorReply(text);
    return Option.isSome(decoded)
      ? (decoded.value.error.code ?? decoded.value.error.type)
      : undefined;
  } finally {
    // A tee's cancellation may await the original consumer. Do not hold Pi's
    // response hostage to that consumer; dispose only our observation branch.
    void reader.cancel().catch((cause: unknown) => {
      console.error("Provider error-copy cancellation failed", cause);
    });
    reader.releaseLock();
  }
}

/** Bounded provider evidence, never response bodies, headers or error prose. */
export type ProviderRejection =
  | Readonly<{
      code: "usage-limit-reached";
      /** Provider-declared epoch milliseconds, absent when unknown. */
      resetAt?: number;
      limit?: "five_hour" | "seven_day" | "seven_day_opus" | "seven_day_sonnet";
    }>
  | Readonly<{ code: "rate-limit"; retryAfterMs?: number }>
  | Readonly<{ code: "overloaded" }>
  | Readonly<{ code: "provider_unavailable"; status?: number }>
  | Readonly<{ code: "provider_rejected"; status: number }>;

/** Retry only typed transient failures, never usage exhaustion or rejection. */
export function isRetryableProviderRejection(
  rejection: ProviderRejection | undefined,
): boolean {
  if (rejection === undefined) return false;
  switch (rejection.code) {
    case "provider_unavailable":
    case "rate-limit":
    case "overloaded":
      return true;
    case "usage-limit-reached":
    case "provider_rejected":
      return false;
  }
}

/** One provider call's transport and parsed-event observations. */
export type ProviderStatusRecorder = Readonly<{
  /** Request-local fetch observer, retaining the supplied transport. */
  fetch: typeof globalThis.fetch;
  /** Pi's maintained parsed-event hook, composed with the supplied observer. */
  onProviderStreamEvent: NonNullable<
    SimpleStreamOptions["onProviderStreamEvent"]
  >;
  /** Latest typed failure evidence for this call, or no observed failure. */
  rejection: (message?: AssistantMessage) => ProviderRejection | undefined;
}>;

/**
 * Observe one call's typed HTTP and parsed-event evidence. Pi owns stream
 * decoding; this module retains no provider prose.
 */
export function observeProviderStatus(
  options?: Pick<SimpleStreamOptions, "fetch" | "onProviderStreamEvent">,
): ProviderStatusRecorder {
  let rejection: ProviderRejection | undefined;
  const upstream = options?.fetch ?? globalThis.fetch;
  const observed: typeof globalThis.fetch = async (input, init) => {
    rejection = undefined;
    let response: Response;
    try {
      response = await upstream(input, init);
    } catch (cause) {
      if (init?.signal?.aborted !== true)
        rejection = { code: "provider_unavailable" };
      throw cause;
    }
    const status = response.status;
    const code =
      status === 429 || status === 529
        ? await providerErrorKind(response)
        : undefined;
    const subscriptionRejected =
      status === 429 &&
      Option.isSome(
        decodeRejected(
          response.headers.get("anthropic-ratelimit-unified-status"),
        ),
      );
    if (status < 400 || status > 599) rejection = undefined;
    else if (subscriptionRejected) rejection = usageLimit(response.headers);
    else if (code === usageLimitCode)
      rejection = { code: "usage-limit-reached" };
    else if (code === "overloaded_error") rejection = { code: "overloaded" };
    else if (status === 429 && code !== "insufficient_quota")
      rejection = rateLimit(response.headers);
    else if (transientStatus(status) && code !== "insufficient_quota")
      rejection = { code: "provider_unavailable", status };
    else rejection = { code: "provider_rejected", status };
    return response;
  };
  return {
    fetch: Object.assign(observed, upstream),
    onProviderStreamEvent: async (data, model) => {
      const failed = decodeFailedEvent(data);
      if (
        Option.isSome(failed) &&
        failed.value.response.error.code === usageLimitCode
      )
        rejection = { code: "usage-limit-reached" };
      if (Option.isSome(decodeOverloaded(data)))
        rejection = { code: "overloaded" };
      await options?.onProviderStreamEvent?.(data, model);
    },
    rejection: (message) => {
      if (rejection !== undefined) return rejection;
      // Pi throws Anthropic event:error before its parsed-event hook and
      // retains that JSON envelope verbatim as the terminal errorMessage.
      // Decode that maintained producer's representation, never match prose.
      const text = message?.errorMessage;
      if (
        message?.stopReason !== "error" ||
        text === undefined ||
        text.length > errorBodyBytes ||
        new TextEncoder().encode(text).byteLength > errorBodyBytes
      )
        return undefined;
      const decoded = decodeErrorReply(text);
      return Option.isSome(decoded) &&
        decoded.value.error.type === "overloaded_error"
        ? { code: "overloaded" }
        : undefined;
    },
  };
}
