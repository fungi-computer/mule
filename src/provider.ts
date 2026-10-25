import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { piOptions } from "./pi-options.js";
import type { MuleStreamFunction } from "./contract.js";

/** Pi's maintained stream behind Mule's structural provider port. */
export const streamPi: MuleStreamFunction = async (model, context, options) => {
  const { streamSimple } = await import("@earendil-works/pi-ai/compat");
  return streamSimple(model, normalizeContext(context), piOptions(options));
};
