import * as Schema from "effect/Schema";
import type { ProviderData, ProviderValue } from "./types.js";

const providerValue: Schema.Codec<ProviderValue> = Schema.suspend(() =>
  Schema.Union([
    Schema.Undefined,
    Schema.Null,
    Schema.Boolean,
    Schema.Number,
    Schema.String,
    Schema.Uint8Array,
    Schema.instanceOf(AbortSignal),
    Schema.Array(providerValue),
    providerData,
  ]),
);
const providerData: Schema.Codec<ProviderData> = Schema.Record(
  Schema.String,
  providerValue,
);
/** Decode Pi's parsed request/event data before it enters an owned hook. */
export function decodeProviderData(data: unknown): ProviderData {
  return Schema.decodeUnknownSync(providerData)(data);
}
