import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { piOptions } from "./pi-options.js";
import type { MuleStreamFunction } from "./contract.js";
import type { Context, Message } from "./types.js";

/**
 * Drop the tool results a provider rejects: a result for a call outside the
 * turn it answers (Pi omits error and aborted assistant turns but keeps their
 * results) and a second result for one call. An unanswered call is the
 * Session store's to record; this never invents a result, rewrites the stored
 * transcript or reruns a tool.
 */
export function prepareProviderContext(context: Context): Context {
  const pending = new Set<string>();
  const messages: Message[] = [];
  for (const message of context.messages) {
    if (message.role === "toolResult") {
      if (pending.delete(message.toolCallId)) messages.push(message);
      continue;
    }
    // Pi moves system updates after the complete result batch.
    if (message.role !== "system") pending.clear();
    if (message.role === "assistant") {
      // Pi omits incomplete assistant turns, including their tool calls.
      if (message.stopReason === "error" || message.stopReason === "aborted")
        continue;
      for (const block of message.content)
        if (block.type === "toolCall") pending.add(block.id);
    }
    messages.push(message);
  }
  return normalizeContext({ ...context, messages });
}

/** Pi's maintained stream behind Mule's structural provider port. */
export const streamPi: MuleStreamFunction = async (model, context, options) => {
  const { streamSimple } = await import("@earendil-works/pi-ai/compat");
  return streamSimple(
    model,
    prepareProviderContext(context),
    piOptions(options),
  );
};
