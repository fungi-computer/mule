import type { SimpleStreamOptions } from "./types.js";
import { Option, Schema } from "effect";

const usageLimitCode = "subscription_sharing_usage_limit_exceeded";
const providerError = Schema.Struct({
  error: Schema.Struct({
    code: Schema.String,
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

/** Pi's pinned provider-request transient classes, kept as typed evidence. */
const transientStatus = (status: number): boolean =>
  status === 408 ||
  status === 409 ||
  status === 429 ||
  (status >= 500 && status <= 599);

/** Retain Local's bounded error-copy observation; Pi still owns its response. */
async function providerErrorCode(
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
    return Option.isSome(decoded) ? decoded.value.error.code : undefined;
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
  | Readonly<{ code: "usage-limit-reached" }>
  | Readonly<{ code: "provider_unavailable"; status?: number }>
  | Readonly<{ code: "provider_rejected"; status: number }>;

/** One provider call's transport and parsed-event observations. */
export type ProviderStatusRecorder = Readonly<{
  /** Request-local fetch observer, retaining the supplied transport. */
  fetch: typeof globalThis.fetch;
  /** Pi's maintained parsed-event hook, composed with the supplied observer. */
  onProviderStreamEvent: NonNullable<
    SimpleStreamOptions["onProviderStreamEvent"]
  >;
  /** Latest typed failure evidence for this call, or no observed failure. */
  rejection: () => ProviderRejection | undefined;
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
    const code = status === 429 ? await providerErrorCode(response) : undefined;
    rejection =
      status < 400 || status > 599
        ? undefined
        : code === usageLimitCode
          ? { code: "usage-limit-reached" }
          : transientStatus(status) && code !== "insufficient_quota"
            ? { code: "provider_unavailable", status }
            : { code: "provider_rejected", status };
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
      await options?.onProviderStreamEvent?.(data, model);
    },
    rejection: () => rejection,
  };
}
