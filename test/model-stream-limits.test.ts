import { createAssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";
import type { MuleEvent, MuleStreamFunction } from "../src/contract.js";
import { run } from "../src/effect.js";
import { fauxRun, userMessage } from "./support.js";

type Mode = "never" | "silent" | "trickle" | "output";

it.each<Mode>(["never", "silent", "trickle", "output"])(
  "bounds a %s provider, preserves partials, retries three times and permits the next prompt",
  async (mode) => {
    const { settings } = fauxRun([]);
    const emitted: MuleEvent[] = [];
    let calls = 0;
    const stream: MuleStreamFunction = (_model, _context, options) => {
      calls++;
      const events = createAssistantMessageEventStream();
      const partial = fauxAssistantMessage([{ type: "text", text: "partial" }]);
      const stop = () => {
        clearInterval(interval);
        events.end(partial);
      };
      options?.signal?.addEventListener("abort", stop, { once: true });
      const interval = setInterval(() => {
        if (mode === "trickle") {
          events.push({
            type: "text_delta",
            contentIndex: 0,
            delta: "x",
            partial,
          });
        }
      }, 3);
      if (mode !== "never") {
        events.push({ type: "start", partial });
        events.push({
          type: "text_delta",
          contentIndex: 0,
          delta: "partial",
          partial,
        });
        if (mode === "output")
          events.push({
            type: "text_delta",
            contentIndex: 0,
            delta: "x".repeat(5 * 1024 * 1024),
            partial,
          });
      }
      return events;
    };
    const started = Date.now();
    const failed = await Effect.runPromise(
      run(
        {
          ...settings,
          stream,
          retryable: () => false,
          retry: {
            enabled: true,
            maxRetries: 3,
            baseDelayMs: 1,
            maxAgentDelayMs: 1,
          },
          modelStreamLimits: {
            firstEventMs: 20,
            attemptMs: 45,
            outputBytes: 1024,
          },
          modelStreamIdleTimeoutMs: 20,
        },
        (event) =>
          Effect.sync(() => {
            emitted.push(event);
          }),
      ),
    );
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(calls).toBe(4);
    expect(failed.status).toBe("failed");
    expect(failed.message.content).toEqual(
      mode === "never" ? [] : partialContent,
    );
    expect(failed.message.errorMessage).toMatch(
      /failed 4 times on this conversation/u,
    );
    const diagnostic = failed.message.errorMessage ?? "";
    const kind = {
      never: "first event timed out",
      silent: "stream timed out \\(silence\\)",
      trickle: "attempt timed out",
      output: "output limit exceeded",
    }[mode];
    expect(diagnostic).toMatch(new RegExp(`Model ${kind} after \\d+ ms`, "u"));
    const metrics = /max provider gap (\d+) ms, streamed (\d+) bytes/u.exec(
      diagnostic,
    );
    expect(metrics).not.toBeNull();
    expect(Number(metrics?.[1])).toBeGreaterThanOrEqual(0);
    const bytes = Number(metrics?.[2]);
    if (mode === "never") expect(bytes).toBe(0);
    if (mode === "silent") expect(bytes).toBe(7);
    if (mode === "trickle") expect(bytes).toBeGreaterThan(7);
    if (mode === "output") expect(bytes).toBe(5 * 1024 * 1024 + 7);
    expect(
      emitted.filter((event) => event.type === "message_end"),
    ).toHaveLength(4);
    const next = await Effect.runPromise(
      run({
        ...settings,
        messages: [userMessage("next")],
        stream: fauxRun([fauxAssistantMessage("recovered")]).settings.stream,
      }),
    );
    expect(next.status).toBe("completed");
    expect(next.message.content).toEqual([{ type: "text", text: "recovered" }]);
  },
);

const partialContent = [{ type: "text" as const, text: "partial" }];
