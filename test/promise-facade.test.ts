import { Type } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";

import type { MuleEvent } from "../src/contract.js";
import { muleEffect } from "../src/effect.js";
import { mule, type MuleTool } from "../src/index.js";
import { eventTypes, fauxRun, runEnds, userMessage } from "./support.js";

const descriptor = (name: string) => ({
  name,
  description: name,
  parameters: Type.Object({}),
});

describe("Mule Promise facade", () => {
  it("runs the same lifecycle as the Effect root", async () => {
    const responses = () => [
      fauxAssistantMessage(fauxToolCall("missing", {}), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("done"),
    ];
    const promiseEvents: MuleEvent[] = [];
    const effectEvents: MuleEvent[] = [];

    const promiseOutcome = await mule.run(
      fauxRun(responses()).settings,
      (event) => {
        promiseEvents.push(event);
      },
    );
    const effectOutcome = await Effect.runPromise(
      muleEffect.run(fauxRun(responses()).settings, (event) =>
        Effect.sync(() => {
          effectEvents.push(event);
        }),
      ),
    );

    expect(promiseOutcome.status).toBe("completed");
    expect(effectOutcome.status).toBe("completed");
    expect(eventTypes(promiseEvents)).toEqual(eventTypes(effectEvents));
  });

  it("lifts Promise tools and hooks once into the canonical loop", async () => {
    const { settings } = fauxRun([
      fauxAssistantMessage(
        [
          fauxToolCall("echo", {}),
          fauxToolCall("guarded", {}),
          fauxToolCall("broken", {}),
          fauxToolCall("throws", {}),
        ],
        { stopReason: "toolUse" },
      ),
      fauxAssistantMessage("done"),
    ]);
    const signals: AbortSignal[] = [];
    const tools: MuleTool[] = [
      {
        descriptor: descriptor("echo"),
        prepare: (_call, signal) => {
          signals.push(signal);
          return Promise.resolve({
            status: "prepared",
            execute: async function* (executeSignal) {
              signals.push(executeSignal);
              await Promise.resolve();
              yield {
                type: "result" as const,
                result: {
                  content: [{ type: "text" as const, text: "echoed" }],
                  isError: false,
                },
              };
            },
          });
        },
      },
      {
        descriptor: descriptor("guarded"),
        prepare: () =>
          Promise.resolve({ status: "rejected", message: "Not authorized" }),
      },
      {
        descriptor: descriptor("broken"),
        prepare: () => Promise.reject(new Error("prepare bug")),
      },
      {
        descriptor: descriptor("throws"),
        prepare: () =>
          Promise.resolve({
            status: "prepared",
            execute: async function* () {
              yield await Promise.reject(new Error("tool crashed"));
            },
          }),
      },
    ];
    const polled: string[] = [];

    const outcome = await mule.run({
      ...settings,
      tools,
      resolveApiKey: (_provider, signal) => {
        signals.push(signal);
        return Promise.resolve("key");
      },
      getSteeringMessages: (signal) => {
        signals.push(signal);
        polled.push("steer");
        return Promise.resolve([]);
      },
      getFollowUpMessages: () => {
        polled.push("followUp");
        return Promise.resolve([]);
      },
      shouldStopAfterTurn: () => {
        polled.push("stop");
        return Promise.resolve(false);
      },
    });

    expect(outcome.status).toBe("completed");
    expect(
      outcome.messages.flatMap((message) =>
        message.role === "toolResult"
          ? [[message.toolName, message.isError, message.content]]
          : [],
      ),
    ).toEqual([
      ["echo", false, [{ type: "text", text: "echoed" }]],
      ["guarded", true, [{ type: "text", text: "Not authorized" }]],
      [
        "broken",
        true,
        [{ type: "text", text: 'Tool "broken" could not be prepared' }],
      ],
      ["throws", true, [{ type: "text", text: 'Tool "throws" failed' }]],
    ]);
    expect(signals.every((signal) => signal instanceof AbortSignal)).toBe(true);
    expect(polled).toEqual([
      "steer",
      "stop",
      "steer",
      "stop",
      "steer",
      "followUp",
    ]);
  });

  it("rejects with the listener's MuleError, carrying its cause, and emits nothing after it", async () => {
    const { settings } = fauxRun([fauxAssistantMessage("hello")]);
    const cause = new Error("disk full");
    const events: MuleEvent[] = [];

    await expect(
      mule.run(settings, (event) => {
        events.push(event);
        if (event.type === "message_start") throw cause;
      }),
    ).rejects.toEqual({
      _tag: "MuleError",
      phase: "listener",
      message: "Mule listener failed",
      cause,
    });
    expect(eventTypes(events).at(-1)).toBe("message_start");
    expect(runEnds(events)).toEqual([]);
  });

  it("rejects a failed model start with MuleError rather than an Effect fiber failure", async () => {
    const cause = new Error("bad credentials");

    await expect(
      mule.run({
        ...fauxRun([]).settings,
        messages: [userMessage("Go")],
        stream: () => Promise.reject(cause),
      }),
    ).rejects.toEqual({
      _tag: "MuleError",
      phase: "model",
      message: "Pi model stream could not start",
      cause,
    });
  });

  it("rejects a failed Promise hook as a control MuleError", async () => {
    const cause = new Error("queue offline");

    await expect(
      mule.run({
        ...fauxRun([fauxAssistantMessage("hello")]).settings,
        getFollowUpMessages: () => Promise.reject(cause),
      }),
    ).rejects.toMatchObject({ _tag: "MuleError", phase: "control", cause });
  });
});
