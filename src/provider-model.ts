import * as Schema from "effect/Schema";
import type { Api, Model, ProviderId } from "./types.js";

/** Display names for recognized provider ids; caller-defined ids display as written. */
export const providerDisplayNames: ReadonlyMap<ProviderId, string> = new Map([
  ["openai", "OpenAI"],
  ["openai-codex", "OpenAI"],
  ["anthropic", "Anthropic"],
  ["cloudflare-workers-ai", "Cloudflare Workers AI"],
]);

const sampling = Schema.Record(Schema.String, Schema.Json);
const decodeSampling = Schema.decodeUnknownSync(
  Schema.Struct({
    samplingParams: Schema.optionalKey(sampling),
    samplingParamsByThinkingLevel: Schema.optionalKey(
      Schema.Struct({
        off: Schema.optionalKey(sampling),
        minimal: Schema.optionalKey(sampling),
        low: Schema.optionalKey(sampling),
        medium: Schema.optionalKey(sampling),
        high: Schema.optionalKey(sampling),
        xhigh: Schema.optionalKey(sampling),
        max: Schema.optionalKey(sampling),
      }),
    ),
  }),
);

/** Validate a catalog model's opaque sampling fields before it enters Mule. */
export function normalizeModel<TApi extends Api>(
  model: Omit<Model<TApi>, "samplingParams" | "samplingParamsByThinkingLevel">,
): Model<TApi> {
  return { ...model, ...decodeSampling(model) };
}
