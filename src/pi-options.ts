import * as Schema from "effect/Schema";
import type { SimpleStreamOptions as PiOptions } from "@earendil-works/pi-ai";
import { decodeProviderData } from "./provider-data.js";
import { normalizeModel } from "./provider-model.js";
import type { SimpleStreamOptions } from "./types.js";

/** Adapt Pi's parsed payload and event boundary before invoking owned hooks. */
export function piOptions(
  options: SimpleStreamOptions | undefined,
): PiOptions | undefined {
  if (options === undefined) return undefined;
  const { onPayload, onProviderStreamEvent, onResponse, ...rest } = options;
  const adapted: PiOptions = { ...rest };
  if (onPayload !== undefined)
    adapted.onPayload = (payload, model) =>
      onPayload(decodeProviderData(payload), normalizeModel(model));
  if (onProviderStreamEvent !== undefined)
    adapted.onProviderStreamEvent = (event, model) =>
      onProviderStreamEvent(decodeProviderData(event), normalizeModel(model));
  if (onResponse !== undefined)
    adapted.onResponse = (response, model) =>
      onResponse(response, normalizeModel(model));
  return adapted;
}

/** Restore the owned options shape when Pi calls a scripted provider factory. */
export function muleOptions(
  options: PiOptions | undefined,
): SimpleStreamOptions | undefined {
  if (options === undefined) return undefined;
  const { samplingParams, metadata, onPayload, ...rest } = options;
  const adapted: SimpleStreamOptions = { ...rest };
  const decodeObject = Schema.decodeUnknownSync(
    Schema.Record(Schema.String, Schema.Json),
  );
  if (samplingParams !== undefined)
    adapted.samplingParams = decodeObject(samplingParams);
  if (metadata !== undefined) adapted.metadata = decodeObject(metadata);
  if (onPayload !== undefined)
    adapted.onPayload = async (payload, model) => {
      const result = await onPayload(payload, model);
      return result === undefined ? undefined : decodeProviderData(result);
    };
  return adapted;
}
