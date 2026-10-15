import { Type, type ToolCall } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";

import type {
  MuleError,
  MuleEvent,
  MuleToolExecutionPart,
  MuleToolResult,
} from "../src/contract.js";
import { run, type MuleTool } from "../src/effect.js";
import { deferred, eventTypes, fauxRun, runEnds } from "./support.js";

const text = (value: string, isError = false): MuleToolResult => ({
  content: [{ type: "text", text: value }],
  isError,
});

const result = (value: MuleToolResult): MuleToolExecutionPart => ({
  type: "result",
  result: value,
});

function tool(
  name: string,
  execute: (call: ToolCall) => Stream.Stream<MuleToolExecutionPart, MuleError>,
  options: { readonly executionMode?: "parallel" | "sequential" } = {},
): MuleTool {
  return {
    descriptor: {
      name,
      description: name,
      parameters: Type.Object({}),
    },
    executionMode: options.executionMode,
    prepare: (call) => Effect.succeed({ execute: () => execute(call) }),
  };
}

const collect = (events: MuleEvent[]) => (event: MuleEvent) =>
  Effect.sync(() => {
    events.push(event);
  });

describe("Mule tool batches", () => {
  it("keeps one tool-call identity from start to end and one result identity across its message", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("echo", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage("done"),
    ]);
    const events: MuleEvent[] = [];

    await Effect.runPromise(
      run(
        {
          ...settings,
          tools: [
            tool("echo", () =>
              Stream.make(
                { type: "update" as const, result: text("working") },
                result(text("ok")),
              ),
            ),
          ],
        },
        collect(events),
      ),
    );

    const toolEvents = events.filter(
      (event) =>
        event.type === "tool_execution_start" ||
        event.type === "tool_execution_update" ||
        event.type === "tool_execution_end",
    );
    expect(toolEvents.map((event) => event.type)).toEqual([
      "tool_execution_start",
      "tool_execution_update",
      "tool_execution_end",
    ]);
    const toolCall = toolEvents[0]?.toolCall;
    expect(toolCall?.kind).toBe("tool_call");
    expect(toolEvents.every((event) => event.toolCall === toolCall)).toBe(true);

    const resultMessages = events.filter(
      (
        event,
      ): event is Extract<
        MuleEvent,
        { type: "message_start" | "message_end" }
      > =>
        (event.type === "message_start" || event.type === "message_end") &&
        event.value.role === "toolResult",
    );
    expect(resultMessages.map((event) => event.type)).toEqual([
      "message_start",
      "message_end",
    ]);
    expect(resultMessages[0]?.message).toBe(resultMessages[1]?.message);
    expect(resultMessages[0]?.message).toMatchObject({
      kind: "tool_result",
      parent: toolCall,
    });
  });

  it("reports an unknown tool and a prepare rejection as error results without executing", async () => {
    const { faux, settings } = fauxRun([
      fauxAssistantMessage(
        [fauxToolCall("missing", {}), fauxToolCall("guarded", {})],
        { stopReason: "toolUse" },
      ),
      fauxAssistantMessage("done"),
    ]);
    let executed = false;
    const guarded: MuleTool = {
      descriptor: {
        name: "guarded",
        description: "Rejects",
        parameters: Type.Object({}),
      },
      prepare: () => Effect.fail({ message: "Not authorized" }),
    };
    const events: MuleEvent[] = [];

    const outcome = await Effect.runPromise(
      run(
        {
          ...settings,
          tools: [
            guarded,
            tool("unused", () => {
              executed = true;
              return Stream.empty;
            }),
          ],
        },
        collect(events),
      ),
    );

    expect(executed).toBe(false);
    expect(faux.state.callCount).toBe(2);
    expect(outcome.messages.slice(1, 3)).toMatchObject([
      {
        role: "toolResult",
        toolName: "missing",
        isError: true,
        content: [{ type: "text", text: 'Tool "missing" not found' }],
      },
      {
        role: "toolResult",
        toolName: "guarded",
        isError: true,
        content: [{ type: "text", text: "Not authorized" }],
      },
    ]);
    expect(
      eventTypes(events).filter((type) => type.startsWith("tool_execution")),
    ).toEqual([
      "tool_execution_start",
      "tool_execution_end",
      "tool_execution_start",
      "tool_execution_end",
    ]);
  });

  it("reports parallel terminals by completion and returns results in source order", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage(
        [fauxToolCall("slow", {}), fauxToolCall("fast", {})],
        { stopReason: "toolUse" },
      ),
      fauxAssistantMessage("done"),
    ]);
    const fastDone = deferred();
    const events: MuleEvent[] = [];

    const outcome = await Effect.runPromise(
      run(
        {
          ...settings,
          tools: [
            tool("slow", () =>
              Stream.fromEffect(
                Effect.promise(() => fastDone.promise).pipe(
                  Effect.as(result(text("slow"))),
                ),
              ),
            ),
            tool("fast", () => Stream.make(result(text("fast")))),
          ],
        },
        (event) =>
          Effect.sync(() => {
            events.push(event);
            if (
              event.type === "tool_execution_end" &&
              event.call.name === "fast"
            )
              fastDone.resolve();
          }),
      ),
    );

    expect(
      events.flatMap((event) =>
        event.type === "tool_execution_end" ? [event.call.name] : [],
      ),
    ).toEqual(["fast", "slow"]);
    expect(
      outcome.messages.flatMap((message) =>
        message.role === "toolResult" ? [message.toolName] : [],
      ),
    ).toEqual(["slow", "fast"]);
  });

  it("runs a batch sequentially when any selected tool is sequential", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage(
        [fauxToolCall("first", {}), fauxToolCall("second", {})],
        { stopReason: "toolUse" },
      ),
      fauxAssistantMessage("done"),
    ]);
    const order: string[] = [];
    const step = (name: string) =>
      Stream.fromEffect(
        Effect.gen(function* () {
          order.push(`${name}:start`);
          yield* Effect.yieldNow;
          order.push(`${name}:end`);
          return result(text(name));
        }),
      );

    await Effect.runPromise(
      run({
        ...settings,
        tools: [
          tool("first", () => step("first"), { executionMode: "sequential" }),
          tool("second", () => step("second")),
        ],
      }),
    );

    expect(order).toEqual([
      "first:start",
      "first:end",
      "second:start",
      "second:end",
    ]);
  });

  it("serializes listener settlement while parallel tools report progress", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage([fauxToolCall("a", {}), fauxToolCall("b", {})], {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("done"),
    ]);
    let active = 0;
    let maxActive = 0;
    const progress = () =>
      Stream.make(
        { type: "update" as const, result: text("1") },
        { type: "update" as const, result: text("2") },
        result(text("done")),
      );

    await Effect.runPromise(
      run(
        { ...settings, tools: [tool("a", progress), tool("b", progress)] },
        () =>
          Effect.gen(function* () {
            active += 1;
            maxActive = Math.max(maxActive, active);
            yield* Effect.sleep("1 millis");
            active -= 1;
          }),
      ),
    );

    expect(maxActive).toBe(1);
  });

  it("ignores progress after a tool's terminal result", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("late", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage("done"),
    ]);
    const events: MuleEvent[] = [];

    const outcome = await Effect.runPromise(
      run(
        {
          ...settings,
          tools: [
            tool("late", () =>
              Stream.make(result(text("first")), {
                type: "update" as const,
                result: text("late"),
              }),
            ),
          ],
        },
        collect(events),
      ),
    );

    expect(eventTypes(events)).not.toContain("tool_execution_update");
    expect(outcome.messages[1]).toMatchObject({
      content: [{ type: "text", text: "first" }],
    });
  });

  it("turns a tool stream's own failure into a model-visible error result", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("flaky", {}), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("done"),
    ]);

    const outcome = await Effect.runPromise(
      run({
        ...settings,
        tools: [
          tool("flaky", () =>
            Stream.fail<MuleError>({
              _tag: "MuleError",
              phase: "tool",
              message: "disk full",
            }),
          ),
        ],
      }),
    );

    expect(outcome.status).toBe("completed");
    expect(outcome.messages[1]).toMatchObject({
      role: "toolResult",
      isError: true,
      content: [{ type: "text", text: "disk full" }],
    });
  });

  it("fails the run when the listener rejects tool progress, and never feeds that failure to the model", async () => {
    const { faux, settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage("must not be requested"),
    ]);
    const listenerError: MuleError = {
      _tag: "MuleError",
      phase: "listener",
      message: "recorder unavailable",
    };
    const events: MuleEvent[] = [];

    const exit = await Effect.runPromise(
      Effect.result(
        run(
          {
            ...settings,
            tools: [
              tool("work", () =>
                Stream.make(
                  { type: "update" as const, result: text("progress") },
                  result(text("ok")),
                ),
              ),
            ],
          },
          (event) =>
            event.type === "tool_execution_update"
              ? Effect.fail(listenerError)
              : Effect.sync(() => {
                  events.push(event);
                }),
        ),
      ),
    );

    expect(Result.isFailure(exit) && exit.failure).toBe(listenerError);
    expect(faux.state.callCount).toBe(1);
    expect(eventTypes(events)).not.toContain("tool_execution_end");
    expect(runEnds(events)).toEqual([]);
  });

  it("stops after a batch in which every tool asks to terminate", async () => {
    const { faux, settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("finish", {}), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("must not be requested"),
    ]);

    const outcome = await Effect.runPromise(
      run({
        ...settings,
        tools: [
          tool("finish", () =>
            Stream.make(result({ ...text("final"), terminate: true })),
          ),
        ],
      }),
    );

    expect(faux.state.callCount).toBe(1);
    expect(outcome.status).toBe("completed");
    expect(outcome.messages.map((message) => message.role)).toEqual([
      "assistant",
      "toolResult",
    ]);
  });
});
