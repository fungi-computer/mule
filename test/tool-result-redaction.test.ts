import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import { Type } from "typebox";
import { describe, expect, it } from "vitest";

import type {
  MuleError,
  MuleEvent,
  MuleToolExecutionPart,
  MuleToolResult,
} from "../src/contract.js";
import { run, type MuleTool } from "../src/effect.js";
import { fauxRun } from "./support.js";

const canary = "sk_live_mule_canary_7f3a9c2e";

const text = (value: string): MuleToolResult => ({
  content: [{ type: "text", text: value }],
  data: { echoed: value },
  isError: false,
});

const catTool: MuleTool = {
  descriptor: { name: "cat", description: "cat", parameters: Type.Object({}) },
  prepare: () =>
    Effect.succeed({
      execute: () =>
        Stream.fromIterable<MuleToolExecutionPart>([
          { type: "update", result: text(`STRIPE_KEY=${canary.slice(0, 9)}`) },
          { type: "result", result: text(`STRIPE_KEY=${canary}\n`) },
        ]),
    }),
};

/** The host's matcher stands in for Veil: Mule only owns where it runs. */
const scrub = (result: MuleToolResult): MuleToolResult => ({
  ...result,
  content: result.content.map((part) =>
    part.type === "text"
      ? { ...part, text: part.text.replaceAll(canary, "[REDACTED]") }
      : part,
  ),
  data: { echoed: "[REDACTED]" },
});

function collect(events: MuleEvent[]) {
  return (event: MuleEvent) =>
    Effect.sync(() => {
      events.push(event);
    });
}

describe("Mule tool-result redaction", () => {
  it("redacts a tool result before listeners and the next model request see it", async () => {
    const contexts: unknown[] = [];
    const { settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("cat", {}), { stopReason: "toolUse" }),
      (context) => {
        contexts.push(structuredClone(context));
        return fauxAssistantMessage("done");
      },
    ]);
    const events: MuleEvent[] = [];
    const phases: string[] = [];

    await Effect.runPromise(
      run(
        {
          ...settings,
          tools: [catTool],
          redactToolResult: (result, phase) =>
            Effect.sync(() => {
              phases.push(phase);
              // Update tails hold back a possible secret prefix.
              return phase === "update"
                ? { content: [], isError: false }
                : scrub(result);
            }),
        },
        collect(events),
      ),
    );

    expect(phases).toEqual(["update", "result"]);
    expect(JSON.stringify(contexts)).toContain("[REDACTED]");
    expect(JSON.stringify(contexts)).not.toContain(canary);
    expect(JSON.stringify(events)).not.toContain(canary);
    expect(JSON.stringify(events)).not.toContain(canary.slice(0, 9));
  });

  it("withholds the output and fails the run when redaction is unavailable", async () => {
    let modelCalls = 0;
    const { settings } = fauxRun([
      fauxAssistantMessage(fauxToolCall("cat", {}), { stopReason: "toolUse" }),
      () => {
        modelCalls += 1;
        return fauxAssistantMessage("never");
      },
    ]);
    const events: MuleEvent[] = [];
    const down: MuleError = {
      _tag: "MuleError",
      phase: "tool",
      message: "port down",
    };

    const exit = await Effect.runPromiseExit(
      run(
        {
          ...settings,
          tools: [catTool],
          redactToolResult: () => Effect.fail(down),
        },
        collect(events),
      ),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit))
      expect(JSON.stringify(exit.cause)).toContain('"phase":"redaction"');
    expect(modelCalls).toBe(0);
    expect(JSON.stringify(events)).not.toContain(canary);
    expect(JSON.stringify(events)).not.toContain(canary.slice(0, 9));
    const end = events.find((event) => event.type === "tool_execution_end");
    expect(end?.type === "tool_execution_end" && end.result).toMatchObject({
      content: [
        { type: "text", text: "[output withheld: redaction unavailable]" },
      ],
      isError: true,
    });
    expect(events.at(-1)).toMatchObject({ type: "run_end", outcome: "failed" });
  });
});
