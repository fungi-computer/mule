import { type ToolCall } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";

import type {
  MuleError,
  MuleEvent,
  MuleToolCheckpoint,
  MuleToolResult,
} from "../src/contract.js";
import { run, type MuleTool } from "../src/effect.js";
import { fauxRun } from "./support.js";

const defaultText =
  "Interrupted by a restart; it may or may not have completed; check before retrying.";

const reran: MuleToolResult = {
  content: [{ type: "text", text: "reran" }],
  isError: false,
};

const tool = (name: string, replay: "safe" | "unsafe"): MuleTool => ({
  descriptor: { name, description: name, parameters: Type.Object({}) },
  replay,
  prepare: () =>
    Effect.succeed({
      execute: () =>
        Stream.make({
          type: "result" as const,
          result: reran,
        }),
    }),
});

/** Resume an assistant turn holding one call in the given checkpoint state. */
async function resumed(
  checkpoint: (call: ToolCall) => MuleToolCheckpoint,
  recover: (call: ToolCall) => Effect.Effect<string | undefined, MuleError>,
  replay: "safe" | "unsafe" = "unsafe",
) {
  const { settings } = fauxRun([fauxAssistantMessage("done")]);
  const call = fauxToolCall("execute", {}, { id: "call-1" });
  const message = fauxAssistantMessage(call, { stopReason: "toolUse" });
  const calls: string[] = [];
  const events: MuleEvent[] = [];
  const outcome = await Effect.runPromise(
    run(
      {
        ...settings,
        tools: [tool("execute", replay)],
        resume: { kind: "assistant", message, tools: [checkpoint(call)] },
        recoverInterruptedTool: (interrupted) => {
          calls.push(interrupted.id);
          return recover(interrupted);
        },
      },
      (event) =>
        Effect.sync(() => {
          events.push(event);
        }),
    ),
  );
  const texts = events.flatMap((event) =>
    event.type === "tool_execution_end"
      ? JSON.stringify(event.result.content)
      : [],
  );
  return { outcome, calls, texts };
}

it("uses the host's account of a started unsafe call instead of the fixed text", async () => {
  const { calls, texts } = await resumed(
    (call) => ({ call, state: "started", replay: "unsafe" }),
    () => Effect.succeed("It finished while the Hub restarted."),
  );
  expect(calls).toEqual(["call-1"]);
  expect(texts.join()).toContain("It finished while the Hub restarted.");
  expect(texts.join()).not.toContain(defaultText);
});

it("keeps the fixed text when the host has nothing to add", async () => {
  const { calls, texts } = await resumed(
    (call) => ({ call, state: "started", replay: "unsafe" }),
    () => Effect.succeed(undefined),
  );
  expect(calls).toEqual(["call-1"]);
  expect(texts.join()).toContain(defaultText);
});

it("keeps the fixed text when the host's recovery fails", async () => {
  const { texts } = await resumed(
    (call) => ({ call, state: "started", replay: "unsafe" }),
    () =>
      Effect.fail({
        _tag: "MuleError" as const,
        phase: "control" as const,
        message: "recovery failed",
      }),
  );
  expect(texts.join()).toContain(defaultText);
});

it.each([
  [
    "pending",
    (call: ToolCall): MuleToolCheckpoint => ({ call, state: "pending" }),
    "unsafe" as const,
  ],
  [
    "started and safe",
    (call: ToolCall): MuleToolCheckpoint => ({
      call,
      state: "started",
      replay: "safe",
    }),
    "safe" as const,
  ],
  [
    "completed",
    (call: ToolCall): MuleToolCheckpoint => ({
      call,
      state: "completed",
      result: { content: [{ type: "text", text: "stored" }], isError: false },
    }),
    "unsafe" as const,
  ],
])("never asks the host about a %s call", async (_name, checkpoint, replay) => {
  const { calls } = await resumed(
    checkpoint,
    () => Effect.succeed("must not be used"),
    replay,
  );
  expect(calls).toEqual([]);
});
