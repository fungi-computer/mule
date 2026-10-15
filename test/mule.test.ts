import { fauxModel } from "../src/testing.js";
import {
  Type,
  type Context,
  type Message,
  type ToolResultMessage,
} from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";

import { mule, type MuleEvent } from "../src/index.js";
import { run, type MuleTool } from "../src/effect.js";

const userMessage = (content: string): Message => ({
  role: "user",
  content,
  timestamp: 1,
});

type Deferred = {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
};

function deferred(): Deferred {
  let release = (): void => undefined;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, resolve: release };
}

describe("Mule's Pi loop", () => {
  it("passes Pi values through unchanged and awaits the listener", async () => {
    const faux = fauxModel([], { tokenSize: { min: 1, max: 1 } });
    faux.setResponses([fauxAssistantMessage("hello")]);
    const events: MuleEvent[] = [];
    const listenerEntered = deferred();
    const listenerRelease = deferred();
    let settled = false;

    const execution = mule.run(
      {
        model: faux.model,
        stream: faux.stream,
        systemPrompt: "Answer concisely.",
        messages: [userMessage("Hi")],
      },
      async (event) => {
        events.push(event);
        if (event.type === "run_end") {
          listenerEntered.resolve();
          await listenerRelease.promise;
        }
      },
    );
    void execution.then(() => {
      settled = true;
    });

    await listenerEntered.promise;
    expect(settled).toBe(false);
    listenerRelease.resolve();
    const outcome = await execution;

    expect(outcome.status).toBe("completed");
    expect(outcome.message.content).toEqual([{ type: "text", text: "hello" }]);
    expect(events.slice(0, 3).map((event) => event.type)).toEqual([
      "run_start",
      "turn_start",
      "message_start",
    ]);
    expect(events.slice(-3).map((event) => event.type)).toEqual([
      "message_end",
      "turn_end",
      "run_end",
    ]);
    const updates = events.flatMap((event) =>
      event.type === "message_update" ? [event.event] : [],
    );
    expect(updates[0]?.type).toBe("text_start");
    expect(updates.at(-1)?.type).toBe("text_end");
    expect(
      updates
        .flatMap((event) => (event.type === "text_delta" ? [event.delta] : []))
        .join(""),
    ).toBe("hello");
  });

  it("feeds Pi tool results into the next Pi turn without a transcript adapter", async () => {
    const faux = fauxModel([]);
    let secondContext: Context | undefined;
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("echo", { text: "hello" }), {
        stopReason: "toolUse",
      }),
      (context) => {
        secondContext = structuredClone(context);
        return fauxAssistantMessage("done");
      },
    ]);

    const Input = Schema.Struct({ text: Schema.String });
    const tool: MuleTool = {
      descriptor: {
        name: "echo",
        description: "Echo text",
        parameters: Type.Object({ text: Type.String() }),
      },
      prepare: (call) =>
        Schema.decodeUnknownEffect(Input)(call.arguments).pipe(
          Effect.map((input) => ({
            execute: () =>
              Stream.succeed({
                type: "result" as const,
                result: {
                  content: [{ type: "text" as const, text: input.text }],
                  details: { echoed: input.text },
                  isError: false,
                },
              }),
          })),
          Effect.mapError(() => ({ message: "Invalid echo arguments" })),
        ),
    };

    const outcome = await Effect.runPromise(
      run({
        model: faux.model,
        stream: faux.stream,
        systemPrompt: "Use the tool.",
        messages: [userMessage("Echo hello")],
        tools: [tool],
      }),
    );

    expect(faux.state.callCount).toBe(2);
    expect(outcome.status).toBe("completed");
    expect(outcome.messages.map((message) => message.role)).toEqual([
      "assistant",
      "toolResult",
      "assistant",
    ]);
    const toolResult = secondContext?.messages.find(
      (message): message is ToolResultMessage => message.role === "toolResult",
    );
    expect(toolResult).toMatchObject({
      toolName: "echo",
      content: [{ type: "text", text: "hello" }],
      details: { echoed: "hello" },
      isError: false,
    });
  });

  it("refuses every tool call from a length-truncated Pi message", async () => {
    const faux = fauxModel([]);
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("unsafe", { path: "/partial" }), {
        stopReason: "length",
      }),
      fauxAssistantMessage("recovered"),
    ]);
    let prepared = false;
    const tool: MuleTool = {
      descriptor: {
        name: "unsafe",
        description: "Must not run from truncated arguments",
        parameters: Type.Object({ path: Type.String() }),
      },
      prepare: () => {
        prepared = true;
        return Effect.fail({ message: "must not be reached" });
      },
    };

    const outcome = await Effect.runPromise(
      run({
        model: faux.model,
        stream: faux.stream,
        systemPrompt: "Recover safely.",
        messages: [userMessage("Do it")],
        tools: [tool],
      }),
    );

    expect(prepared).toBe(false);
    expect(faux.state.callCount).toBe(2);
    expect(outcome.messages[1]).toMatchObject({
      role: "toolResult",
      isError: true,
    });
  });

  const sideEffectTool = (effects: string[]): MuleTool => ({
    descriptor: {
      name: "edit",
      description: "Writes a file",
      parameters: Type.Object({ path: Type.String() }),
    },
    prepare: (call) =>
      Effect.sync(() => {
        effects.push(`prepare ${JSON.stringify(call.arguments)}`);
        return {
          execute: () =>
            Stream.sync(() => {
              effects.push(`execute ${JSON.stringify(call.arguments)}`);
              return {
                type: "result" as const,
                result: {
                  content: [{ type: "text" as const, text: "written" }],
                  isError: false,
                },
              };
            }),
        };
      }),
  });

  it("runs no tool call from a message aborted mid-tool-call and ends cancelled", async () => {
    // Real providers keep the partial tool-call block when a stream is stopped
    // (pi-ai openai-responses), so the aborted message carries it deterministically.
    const faux = fauxModel([]);
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("edit", { path: "/etc/partial" }), {
        stopReason: "aborted",
      }),
      fauxAssistantMessage("must not be requested"),
    ]);
    const effects: string[] = [];
    const events: MuleEvent[] = [];

    const outcome = await Effect.runPromise(
      run(
        {
          model: faux.model,
          stream: faux.stream,
          systemPrompt: "Edit the file.",
          messages: [userMessage("Edit it")],
          tools: [sideEffectTool(effects)],
        },
        (event) => Effect.sync(() => events.push(event)),
      ),
    );

    expect(outcome.message.stopReason).toBe("aborted");
    expect(outcome.message.content).toMatchObject([
      { type: "toolCall", name: "edit" },
    ]);
    expect(effects).toEqual([]);
    expect(faux.state.callCount).toBe(1);
    expect(outcome.status).toBe("cancelled");
    expect(outcome.messages).toEqual([outcome.message]);
    expect(
      events.some((event) => event.type.startsWith("tool_execution")),
    ).toBe(false);
    expect(events.at(-2)).toMatchObject({ type: "turn_end", toolResults: [] });
    expect(events.at(-1)).toMatchObject({
      type: "run_end",
      outcome: "cancelled",
    });
  });

  it("runs no tool call from a message that errored with partial tool arguments", async () => {
    const faux = fauxModel([]);
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("edit", { path: "/etc/par" }), {
        stopReason: "error",
        errorMessage: "network error",
      }),
      fauxAssistantMessage("must not be requested"),
    ]);
    const effects: string[] = [];
    const events: MuleEvent[] = [];

    const outcome = await Effect.runPromise(
      run(
        {
          model: faux.model,
          stream: faux.stream,
          systemPrompt: "Edit the file.",
          messages: [userMessage("Edit it")],
          tools: [sideEffectTool(effects)],
        },
        (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
      ),
    );

    expect(outcome.message.content).toMatchObject([
      { type: "toolCall", name: "edit" },
    ]);
    expect(effects).toEqual([]);
    expect(faux.state.callCount).toBe(1);
    expect(outcome.status).toBe("failed");
    expect(outcome.messages).toEqual([outcome.message]);
    expect(
      events.some((event) => event.type.startsWith("tool_execution")),
    ).toBe(false);
    expect(events.at(-1)).toMatchObject({ type: "run_end", outcome: "failed" });
  });

  it("still runs a tool call from a normally completed message", async () => {
    const faux = fauxModel([]);
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("edit", { path: "/tmp/whole" }), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("done"),
    ]);
    const effects: string[] = [];

    const outcome = await Effect.runPromise(
      run({
        model: faux.model,
        stream: faux.stream,
        systemPrompt: "Edit the file.",
        messages: [userMessage("Edit it")],
        tools: [sideEffectTool(effects)],
      }),
    );

    expect(outcome.status).toBe("completed");
    expect(effects).toEqual([
      'prepare {"path":"/tmp/whole"}',
      'execute {"path":"/tmp/whole"}',
    ]);
    expect(outcome.messages.map((message) => message.role)).toEqual([
      "assistant",
      "toolResult",
      "assistant",
    ]);
  });

  it("polls a follow-up only after the current Pi turn would stop", async () => {
    const faux = fauxModel([]);
    let secondContext: Context | undefined;
    faux.setResponses([
      fauxAssistantMessage("first"),
      (context) => {
        secondContext = structuredClone(context);
        return fauxAssistantMessage("second");
      },
    ]);
    let poll = 0;

    const outcome = await Effect.runPromise(
      run({
        model: faux.model,
        stream: faux.stream,
        systemPrompt: "Continue when asked.",
        messages: [userMessage("Start")],
        getFollowUpMessages: () =>
          Effect.sync(() => {
            poll += 1;
            return poll === 1 ? [userMessage("Continue")] : [];
          }),
      }),
    );

    expect(faux.state.callCount).toBe(2);
    expect(outcome.messages.map((message) => message.role)).toEqual([
      "assistant",
      "user",
      "assistant",
    ]);
    expect(secondContext?.messages.map((message) => message.role)).toEqual([
      "system",
      "user",
      "assistant",
      "user",
    ]);
    expect(secondContext?.messages[0]).toMatchObject({
      role: "system",
      content: "Continue when asked.",
    });
  });
});
