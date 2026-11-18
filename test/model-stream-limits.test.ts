import { createAssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import { expect, it, vi } from "vitest";
import type { MuleEvent, MuleStreamFunction } from "../src/contract.js";
import { run } from "../src/effect.js";
import { fauxRun, userMessage } from "./support.js";

type Mode = "never" | "silent" | "trickle" | "output";

it.each<Mode>(["never", "silent", "trickle", "output"])(
  "bounds a %s provider, preserves partials and permits the next prompt",
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
    const expectedCalls = mode === "trickle" ? 2 : 4;
    expect(calls).toBe(expectedCalls);
    expect(failed.status).toBe("failed");
    expect(failed.message.content).toEqual(
      mode === "never" ? [] : partialContent,
    );
    if (mode === "trickle") {
      expect(failed.modelLimitReason).toContain(
        "Write big content with file tools",
      );
      const terminal = emitted
        .filter((event) => event.type === "message_end")
        .at(-1);
      expect(terminal).toMatchObject({
        retry: { type: "retry_finished", success: false, attempt: 1 },
      });
    } else {
      expect(failed.message.errorMessage).toMatch(
        /failed 4 times on this conversation/u,
      );
    }
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
    ).toHaveLength(expectedCalls);
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

it.each([
  { name: "resume seeds", resumeAttempt: 3, transientFailures: 0 },
  { name: "non-timeout failures", resumeAttempt: 0, transientFailures: 1 },
])(
  "does not count $name toward the repeated whole-attempt timeout limit",
  async ({ resumeAttempt, transientFailures }) => {
    const { settings } = fauxRun([
      fauxAssistantMessage("", {
        stopReason: "error",
        errorMessage: "Temporary provider failure",
      }),
    ]);
    let calls = 0;
    const stream: MuleStreamFunction = (model, context, options) => {
      calls++;
      if (calls <= transientFailures)
        return settings.stream(model, context, options);
      const events = createAssistantMessageEventStream();
      const partial = fauxAssistantMessage("partial");
      events.push({ type: "start", partial });
      const interval = setInterval(() => {
        events.push({
          type: "text_delta",
          contentIndex: 0,
          delta: "x",
          partial,
        });
      }, 3);
      options?.signal?.addEventListener(
        "abort",
        () => {
          clearInterval(interval);
          events.end(partial);
        },
        { once: true },
      );
      return events;
    };
    const outcome = await Effect.runPromise(
      run({
        ...settings,
        stream,
        retryable: () => true,
        retry: {
          enabled: true,
          maxRetries: 10,
          baseDelayMs: 1,
          maxAgentDelayMs: 1,
        },
        modelStreamLimits: { attemptMs: 30 },
        resume:
          resumeAttempt === 0
            ? undefined
            : {
                kind: "retry",
                phase: "waiting",
                attempt: resumeAttempt,
                maxAttempts: 10,
                dueAt: Date.now(),
              },
      }),
    );
    expect(calls).toBe(transientFailures + 2);
    expect(outcome.status).toBe("failed");
    expect(outcome.modelLimitReason).toContain(
      "Write big content with file tools",
    );
  },
);

it("does not time out a model attempt when the wall clock jumps mid-attempt", async () => {
  // The limits measure how long the attempt has run, not what the wall clock
  // says: a clock step (NTP, a suspended host, a test's fake time) must not
  // read as an hour of silence.
  const { settings } = fauxRun([]);
  const final = fauxAssistantMessage("steady");
  const stream: MuleStreamFunction = () => {
    const events = createAssistantMessageEventStream();
    events.push({ type: "start", partial: final });
    setTimeout(() => {
      const real = Date.now();
      vi.spyOn(Date, "now").mockReturnValue(real + 3_600_000);
      events.push({
        type: "text_delta",
        contentIndex: 0,
        delta: "y",
        partial: final,
      });
      setTimeout(() => {
        events.push({ type: "done", reason: "stop", message: final });
      }, 20);
    }, 20);
    return events;
  };
  try {
    const outcome = await Effect.runPromise(
      run({
        ...settings,
        stream,
        modelStreamLimits: {
          firstEventMs: 5_000,
          attemptMs: 10_000,
          outputBytes: 1024,
        },
        modelStreamIdleTimeoutMs: 5_000,
      }),
    );
    expect(outcome.status).toBe("completed");
    expect(outcome.message.content).toEqual([{ type: "text", text: "steady" }]);
  } finally {
    vi.restoreAllMocks();
  }
});
