import { fauxModel } from "../src/testing.js";
import type {
  Api,
  AssistantMessage,
  Message,
  Model,
} from "@earendil-works/pi-ai";
import { type FauxResponseStep } from "../src/testing.js";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";

import type {
  MuleEvent,
  MuleRunSettings,
  MuleStreamFunction,
} from "../src/contract.js";

export const userMessage = (content: string): Message => ({
  role: "user",
  content,
  timestamp: 1,
});

export type Deferred<A = void> = {
  readonly promise: Promise<A>;
  readonly resolve: (value: A) => void;
};

export function deferred<A = void>(): Deferred<A> {
  let settle!: (value: A) => void;
  const promise = new Promise<A>((resolve) => {
    settle = resolve;
  });
  return { promise, resolve: settle };
}

/** Pi's faux provider plus the settings every faux Mule run needs. */
export function fauxRun(responses: FauxResponseStep[]) {
  const faux = fauxModel([], { tokenSize: { min: 64, max: 64 } });
  faux.setResponses(responses);
  const stream: MuleStreamFunction = faux.stream;
  return {
    faux,
    settings: {
      model: faux.model,
      retryable: () => false,
      stream,
      systemPrompt: "Test.",
      messages: [userMessage("Go")],
    } satisfies MuleRunSettings,
  };
}

function partialAssistant(model: Model<Api>, text: string): AssistantMessage {
  return {
    role: "assistant",
    content: [{ type: "text", text }],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "stop",
    timestamp: 1,
  };
}

/**
 * A Pi stream that emits one text delta and then waits for its abort signal,
 * ending exactly as Pi providers do: an `aborted` error terminal.
 */
export function hangingStream() {
  let aborted = false;
  const stream: MuleStreamFunction = (model, _context, options) => {
    const events = createAssistantMessageEventStream();
    const partial = partialAssistant(model, "started");
    events.push({ type: "start", partial });
    events.push({ type: "text_start", contentIndex: 0, partial });
    events.push({
      type: "text_delta",
      contentIndex: 0,
      delta: "started",
      partial,
    });
    options?.signal?.addEventListener(
      "abort",
      () => {
        aborted = true;
        const error: AssistantMessage = {
          ...partial,
          stopReason: "aborted",
          errorMessage: "Request was aborted",
        };
        events.push({ type: "error", reason: "aborted", error });
        events.end(error);
      },
      { once: true },
    );
    return events;
  };
  return { stream, aborted: () => aborted };
}

export const eventTypes = (events: readonly MuleEvent[]): string[] =>
  events.map((event) => event.type);

export const runEnds = (
  events: readonly MuleEvent[],
): Extract<MuleEvent, { type: "run_end" }>[] =>
  events.filter(
    (event): event is Extract<MuleEvent, { type: "run_end" }> =>
      event.type === "run_end",
  );
