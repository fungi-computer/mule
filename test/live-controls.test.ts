import { Type, type Context } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";

import type {
  MuleError,
  MuleEvent,
  MuleToolExecutionPart,
} from "../src/contract.js";
import { run, type MuleTool } from "../src/effect.js";
import { fauxRun, hangingStream, runEnds, userMessage } from "./support.js";

const echo: MuleTool = {
  descriptor: {
    name: "echo",
    description: "Echo",
    parameters: Type.Object({}),
  },
  prepare: () =>
    Effect.succeed({
      execute: () =>
        Stream.succeed<MuleToolExecutionPart>({
          type: "result" as const,
          result: {
            content: [{ type: "text" as const, text: "ok" }],
            isError: false,
          },
        }),
    }),
};

describe("Mule live controls", () => {
  it("settles when a provider ignores abort and never closes its iterator", async () => {
    const events: MuleEvent[] = [];
    const fiber = Effect.runFork(
      run(
        {
          ...fauxRun([]).settings,
          stream: () => createAssistantMessageEventStream(),
          modelStreamIdleTimeoutMs: 25,
        },
        (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
      ),
    );
    try {
      const exit = await Promise.race([
        Effect.runPromise(Fiber.await(fiber)),
        new Promise<never>((_resolve, reject) => {
          setTimeout(() => {
            reject(new Error("Uncooperative stream blocked cleanup"));
          }, 2_000);
        }),
      ]);
      expect(Exit.isSuccess(exit)).toBe(true);
      if (Exit.isSuccess(exit)) {
        expect(exit.value.status).toBe("failed");
        expect(exit.value.message.errorMessage).toMatch(/timed out/u);
      }
      expect(runEnds(events)).toEqual([
        expect.objectContaining({ outcome: "failed" }),
      ]);
    } finally {
      await Effect.runPromise(Fiber.interrupt(fiber));
    }
  });
  it("ends a model stream that emits no more events and aborts its provider", async () => {
    const model = hangingStream();
    const events: MuleEvent[] = [];
    const fiber = Effect.runFork(
      run(
        {
          ...fauxRun([]).settings,
          stream: model.stream,
          modelStreamIdleTimeoutMs: 25,
        },
        (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
      ),
    );
    try {
      const exit = await Promise.race([
        Effect.runPromise(Fiber.await(fiber)),
        new Promise<never>((_resolve, reject) => {
          setTimeout(() => {
            reject(new Error("Model stream remained thinking"));
          }, 2_000);
        }),
      ]);
      expect(Exit.isSuccess(exit)).toBe(true);
      if (Exit.isSuccess(exit)) {
        expect(exit.value.status).toBe("failed");
        expect(exit.value.message.content).toEqual([
          { type: "text", text: "started" },
        ]);
        expect(exit.value.message.errorMessage).toMatch(/silence/u);
      }
      expect(model.aborted()).toBe(true);
      expect(runEnds(events)).toEqual([
        expect.objectContaining({ outcome: "failed" }),
      ]);
    } finally {
      await Effect.runPromise(Fiber.interrupt(fiber));
    }
  });
  it("delivers steering after a tool batch, before the next model request", async () => {
    let secondContext: Context | undefined;
    const { settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("echo", {}), { stopReason: "toolUse" }),
      (context) => {
        secondContext = structuredClone(context);
        return fauxAssistantMessage("done");
      },
    ]);
    let polls = 0;

    const outcome = await Effect.runPromise(
      run({
        ...settings,
        tools: [echo],
        getSteeringMessages: () =>
          Effect.sync(() => {
            polls += 1;
            return polls === 2 ? [userMessage("Also check the tests")] : [];
          }),
      }),
    );

    expect(secondContext?.messages.map((message) => message.role)).toEqual([
      "system",
      "user",
      "assistant",
      "toolResult",
      "user",
    ]);
    expect(secondContext?.messages[0]).toMatchObject({
      role: "system",
      content: "Test.",
      toolsAdded: [{ name: "echo" }],
    });
    expect(outcome.messages.map((message) => message.role)).toEqual([
      "assistant",
      "toolResult",
      "user",
      "assistant",
    ]);
  });

  it("gives steering precedence over follow-up when a turn would stop", async () => {
    const { faux, settings } = fauxRun([
      fauxAssistantMessage("first"),
      fauxAssistantMessage("second"),
    ]);
    let steeringPolls = 0;
    let followUpPolls = 0;

    const outcome = await Effect.runPromise(
      run({
        ...settings,
        getSteeringMessages: () =>
          Effect.sync(() => {
            steeringPolls += 1;
            return steeringPolls === 2 ? [userMessage("steer")] : [];
          }),
        getFollowUpMessages: () =>
          Effect.sync(() => {
            followUpPolls += 1;
            return [];
          }),
      }),
    );

    expect(faux.state.callCount).toBe(2);
    expect(followUpPolls).toBe(1);
    expect(outcome.messages[1]).toMatchObject({
      role: "user",
      content: "steer",
    });
  });

  it("does not revive a cancelled terminal turn with follow-up", async () => {
    const { faux, settings } = fauxRun([
      fauxAssistantMessage("partial", { stopReason: "aborted" }),
    ]);
    let followUpPolls = 0;

    const outcome = await Effect.runPromise(
      run({
        ...settings,
        getFollowUpMessages: () =>
          Effect.sync(() => {
            followUpPolls += 1;
            return [userMessage("more")];
          }),
      }),
    );

    expect(outcome.status).toBe("cancelled");
    expect(followUpPolls).toBe(0);
    expect(faux.state.callCount).toBe(1);
  });

  it("stops gracefully after a settled turn when shouldStopAfterTurn asks", async () => {
    const { faux, settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("echo", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage("must not be requested"),
    ]);
    const events: MuleEvent[] = [];

    const outcome = await Effect.runPromise(
      run(
        {
          ...settings,
          tools: [echo],
          shouldStopAfterTurn: (turn) =>
            Effect.succeed(turn.toolResults.length === 1),
        },
        (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
      ),
    );

    expect(faux.state.callCount).toBe(1);
    expect(outcome.status).toBe("completed");
    expect(runEnds(events).map((event) => event.outcome)).toEqual([
      "completed",
    ]);
  });

  it("fails the run with the hook's error and still records a failed terminal", async () => {
    const { settings } = fauxRun([fauxAssistantMessage("first")]);
    const hookError: MuleError = {
      _tag: "MuleError",
      phase: "control",
      message: "control store unavailable",
    };
    const events: MuleEvent[] = [];

    const exit = await Effect.runPromise(
      Effect.result(
        run(
          { ...settings, getFollowUpMessages: () => Effect.fail(hookError) },
          (event) =>
            Effect.sync(() => {
              events.push(event);
            }),
        ),
      ),
    );

    expect(Result.isFailure(exit) && exit.failure).toBe(hookError);
    expect(runEnds(events)).toEqual([
      expect.objectContaining({ outcome: "failed" }),
    ]);
  });
});

describe("Mule between-turn context (Pi's prepareNextTurn point)", () => {
  it("replaces the context after tool results and before the next request", async () => {
    const seen: Context[] = [];
    const { faux, settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("echo", {}), { stopReason: "toolUse" }),
      fauxAssistantMessage("done"),
    ]);
    const turns: number[] = [];
    const outcome = await Effect.runPromise(
      run({
        ...settings,
        stream: (model, context, options) => {
          seen.push(structuredClone(context));
          return faux.stream(model, context, options);
        },
        tools: [echo],
        prepareNextTurn: (turn) =>
          Effect.sync(() => {
            turns.push(turn.toolResults.length);
            return [userMessage("compacted context")];
          }),
      }),
    );
    expect(outcome.status).toBe("completed");
    expect(turns).toEqual([1]);
    expect(seen).toHaveLength(2);
    // The second request starts from the replacement, not the old context.
    expect(seen[1]?.messages.filter(({ role }) => role !== "system")).toEqual([
      expect.objectContaining({ role: "user", content: "compacted context" }),
    ]);
  });

  it("keeps the context when the hook returns undefined, and skips text-only turns", async () => {
    const { settings } = fauxRun([fauxAssistantMessage("just text")]);
    let calls = 0;
    const outcome = await Effect.runPromise(
      run({
        ...settings,
        tools: [echo],
        prepareNextTurn: () =>
          Effect.sync(() => {
            calls += 1;
            return undefined;
          }),
      }),
    );
    expect(outcome.status).toBe("completed");
    expect(calls).toBe(0);
  });
});
