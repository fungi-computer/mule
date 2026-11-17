import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { piOptions } from "./pi-options.js";
import type { MuleStreamFunction } from "./contract.js";
import type { Context, Message, ToolCall } from "./types.js";

/** Request-only repair: never rewrite the stored transcript or rerun a tool. */
export function prepareProviderContext(context: Context): Context {
  const pending = new Map<string, ToolCall>();
  const messages: Message[] = [];
  const closePending = (timestamp: number) => {
    for (const call of pending.values())
      messages.push({
        role: "toolResult",
        toolCallId: call.id,
        toolName: call.name,
        content: [{ type: "text", text: "Tool execution interrupted." }],
        isError: true,
        timestamp,
      });
    pending.clear();
  };
  for (const message of context.messages) {
    if (message.role === "toolResult") {
      if (pending.delete(message.toolCallId)) messages.push(message);
      continue;
    }
    // Pi moves system updates after the complete result batch.
    if (message.role !== "system") closePending(message.timestamp);
    if (message.role === "assistant") {
      // Pi omits incomplete assistant turns, including their tool calls.
      if (message.stopReason === "error" || message.stopReason === "aborted")
        continue;
      for (const block of message.content)
        if (block.type === "toolCall") pending.set(block.id, block);
    }
    messages.push(message);
  }
  closePending(context.messages.at(-1)?.timestamp ?? 0);
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
