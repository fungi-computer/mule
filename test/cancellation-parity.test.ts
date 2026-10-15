import { Type } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";

import type { MuleEvent } from "../src/contract.js";
import { run, type MuleTool as MuleEffectTool } from "../src/effect.js";
import { mule, type MuleTool } from "../src/index.js";
import {
  deferred,
  eventTypes,
  fauxRun,
  hangingStream,
  runEnds,
  userMessage,
} from "./support.js";

const descriptor = {
  name: "wait",
  description: "Waits until cancelled",
  parameters: Type.Object({}),
};

describe("Mule cancellation and Promise/Effect parity", () => {
  it("keeps a model-produced aborted stop as a cancelled terminal outcome", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage("partial", { stopReason: "aborted" }),
    ]);
    const events: MuleEvent[] = [];

    const outcome = await mule.run(settings, (event) => {
      events.push(event);
    });

    expect(outcome.status).toBe("cancelled");
    expect(eventTypes(events).at(-1)).toBe("run_end");
    expect(runEnds(events)).toEqual([
      expect.objectContaining({
        outcome: "cancelled",
        terminal: outcome.terminal,
      }),
    ]);
  });

  it("invokes no port or listener for an already-aborted Promise signal", async () => {
    const controller = new AbortController();
    const reason = new Error("stopped before start");
    controller.abort(reason);
    const calls: string[] = [];

    await expect(
      mule.run(
        {
          model: fauxRun([]).settings.model,
          stream: () => {
            calls.push("model");
            throw new Error("unreachable");
          },
          systemPrompt: "",
          messages: [userMessage("Go")],
          getSteeringMessages: () => {
            calls.push("steering");
            return Promise.resolve([]);
          },
        },
        () => {
          calls.push("listener");
        },
        controller.signal,
      ),
    ).rejects.toBe(reason);
    expect(calls).toEqual([]);
  });

  it("ends an interrupted Effect run mid-stream with exactly one cancelled run_end", async () => {
    const model = hangingStream();
    const events: MuleEvent[] = [];
    const streaming = deferred();

    const fiber = Effect.runFork(
      run(
        {
          model: fauxRun([]).settings.model,
          stream: model.stream,
          systemPrompt: "",
          messages: [userMessage("Go")],
        },
        (event) =>
          Effect.suspend(() => {
            events.push(event);
            if (
              event.type !== "message_update" ||
              event.event.type !== "text_delta"
            )
              return Effect.void;
            // Hold the listener so the observed prefix is exact.
            streaming.resolve();
            return Effect.never;
          }),
      ),
    );
    await streaming.promise;
    await Effect.runPromise(Fiber.interrupt(fiber));
    const exit = await Effect.runPromise(Fiber.await(fiber));

    expect(Exit.hasInterrupts(exit)).toBe(true);
    expect(model.aborted()).toBe(true);
    expect(eventTypes(events)).toEqual([
      "run_start",
      "turn_start",
      "message_start",
      "message_update",
      "message_update",
      "run_end",
    ]);
    const [end, ...extra] = runEnds(events);
    expect(extra).toEqual([]);
    expect(end?.outcome).toBe("cancelled");
    expect(end?.run).toBe(events[0]?.run);
    expect(end?.terminal).toMatchObject({ kind: "terminal", parent: end?.run });
  });

  it("interrupts a running Effect tool and ends with one cancelled run_end", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("wait", {}), { stopReason: "toolUse" }),
    ]);
    let toolClosed = 0;
    const tool: MuleEffectTool = {
      descriptor,
      prepare: () =>
        Effect.succeed({
          execute: () =>
            Stream.concat(
              Stream.succeed({
                type: "update" as const,
                result: { content: [], isError: false },
              }),
              Stream.never,
            ).pipe(
              Stream.ensuring(
                Effect.sync(() => {
                  toolClosed += 1;
                }),
              ),
            ),
        }),
    };
    const events: MuleEvent[] = [];
    const running = deferred();

    const fiber = Effect.runFork(
      run({ ...settings, tools: [tool] }, (event) =>
        Effect.sync(() => {
          events.push(event);
          if (event.type === "tool_execution_update") running.resolve();
        }),
      ),
    );
    await running.promise;
    await Effect.runPromise(Fiber.interrupt(fiber));
    const exit = await Effect.runPromise(Fiber.await(fiber));

    expect(Exit.hasInterrupts(exit)).toBe(true);
    expect(toolClosed).toBe(1);
    expect(eventTypes(events)).not.toContain("tool_execution_end");
    expect(eventTypes(events).at(-1)).toBe("run_end");
    expect(runEnds(events).map((event) => event.outcome)).toEqual([
      "cancelled",
    ]);
  });

  it("aborts a Promise tool through its signal and rejects with the caller's reason", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("wait", {}), { stopReason: "toolUse" }),
    ]);
    let toolClosed = 0;
    const tool: MuleTool = {
      descriptor,
      prepare: () =>
        Promise.resolve({
          status: "prepared",
          execute: async function* (signal) {
            try {
              yield {
                type: "update" as const,
                result: { content: [], isError: false },
              };
              await new Promise<void>((resolve) => {
                signal.addEventListener("abort", () => {
                  resolve();
                });
              });
            } finally {
              toolClosed += 1;
            }
          },
        }),
    };
    const controller = new AbortController();
    const reason = new Error("user pressed stop");
    const events: MuleEvent[] = [];

    const pending = mule.run(
      { ...settings, tools: [tool] },
      (event) => {
        events.push(event);
        if (event.type === "tool_execution_update") controller.abort(reason);
      },
      controller.signal,
    );

    await expect(pending).rejects.toBe(reason);
    expect(toolClosed).toBe(1);
    expect(eventTypes(events)).not.toContain("tool_execution_end");
    expect(runEnds(events).map((event) => event.outcome)).toEqual([
      "cancelled",
    ]);
  });

  it("keeps the interruption prefix equal across the Promise and Effect roots", async () => {
    const promiseModel = hangingStream();
    const controller = new AbortController();
    const stop = new Error("stop");
    const promiseEvents: MuleEvent[] = [];
    await expect(
      mule.run(
        {
          model: fauxRun([]).settings.model,
          stream: promiseModel.stream,
          systemPrompt: "",
          messages: [userMessage("Go")],
        },
        (event) => {
          promiseEvents.push(event);
          if (event.type !== "message_update") return;
          controller.abort(stop);
          return new Promise<void>(() => undefined);
        },
        controller.signal,
      ),
    ).rejects.toBe(stop);

    const effectModel = hangingStream();
    const effectEvents: MuleEvent[] = [];
    const firstUpdate = deferred();
    const fiber = Effect.runFork(
      run(
        {
          model: fauxRun([]).settings.model,
          stream: effectModel.stream,
          systemPrompt: "",
          messages: [userMessage("Go")],
        },
        (event) =>
          Effect.suspend(() => {
            effectEvents.push(event);
            if (event.type !== "message_update") return Effect.void;
            firstUpdate.resolve();
            return Effect.never;
          }),
      ),
    );
    await firstUpdate.promise;
    await Effect.runPromise(Fiber.interrupt(fiber));

    expect(promiseModel.aborted()).toBe(true);
    expect(effectModel.aborted()).toBe(true);
    expect(eventTypes(promiseEvents)).toEqual([
      "run_start",
      "turn_start",
      "message_start",
      "message_update",
      "run_end",
    ]);
    expect(eventTypes(effectEvents)).toEqual(eventTypes(promiseEvents));
  });

  it("aborts a cooperative Promise listener and then records the cancelled terminal", async () => {
    const { settings } = fauxRun([fauxAssistantMessage("unused")]);
    const controller = new AbortController();
    const stop = new Error("stop");
    const events: MuleEvent[] = [];
    let listenerAborted = 0;
    const started = deferred();

    const pending = mule.run(
      settings,
      (event, signal) => {
        events.push(event);
        if (event.type !== "turn_start") return;
        started.resolve();
        return new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => {
            listenerAborted += 1;
            resolve();
          });
        });
      },
      controller.signal,
    );
    await started.promise;
    controller.abort(stop);

    await expect(pending).rejects.toBe(stop);
    expect(listenerAborted).toBe(1);
    expect(eventTypes(events)).toEqual(["run_start", "turn_start", "run_end"]);
  });
});
