import type { RetryPolicy } from "@earendil-works/pi-ai";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { MuleEvent, MuleRetryObservation } from "../src/contract.js";
import { run, piCodingAgentRetryDefaults } from "../src/effect.js";
import { fauxRun } from "./support.js";

// Advance the backing JavaScript timers for both active-stream cancellation
// and Effect's retry backoff.

const policy: RetryPolicy = {
  enabled: true,
  maxRetries: 3,
  baseDelayMs: 2_000,
  maxAgentDelayMs: 60_000,
};

const invalidPolicies: RetryPolicy[] = [
  { enabled: true, maxRetries: Infinity, baseDelayMs: 0 },
  { enabled: true, maxRetries: -1, baseDelayMs: 0 },
  { enabled: true, maxRetries: 1, baseDelayMs: Infinity },
];

function providerFailure(errorMessage: string) {
  return fauxAssistantMessage("", { stopReason: "error", errorMessage });
}

function collectRetry() {
  const events: MuleRetryObservation[] = [];
  const onRetry = (event: MuleRetryObservation) =>
    Effect.sync(() => {
      events.push(event);
    });
  return { events, onRetry };
}

describe("Mule's typed retry policy", () => {
  it("retains Pi coding-agent's bounded retry defaults", () => {
    expect(piCodingAgentRetryDefaults).toEqual({
      enabled: true,
      maxRetries: 3,
      baseDelayMs: 2_000,
      maxAgentDelayMs: 60_000,
    });
  });

  it("calls produce once and returns the response unchanged when retry is undefined", async () => {
    const { settings, faux } = fauxRun([
      providerFailure("503 Service Unavailable"),
    ]);

    const outcome = await Effect.runPromise(run(settings));

    expect(outcome.status).toBe("failed");
    expect(faux.state.callCount).toBe(1);
  });

  it("disables provider-native retries so only the typed host policy can retry", async () => {
    const { settings } = fauxRun([providerFailure("503 retry")]);
    const stream = vi.fn(settings.stream);
    const outcome = await Effect.runPromise(run({ ...settings, stream }));
    expect(outcome.status).toBe("failed");
    expect(stream).toHaveBeenCalledTimes(1);
    expect(stream).toHaveBeenCalledWith(
      settings.model,
      expect.anything(),
      expect.objectContaining({ maxRetries: 0 }),
    );
  });

  it.each(invalidPolicies)(
    "rejects a non-finite or negative retry policy before supplier dispatch: %j",
    async (retry) => {
      const { settings, faux } = fauxRun([fauxAssistantMessage("unused")]);
      const exit = await Effect.runPromiseExit(run({ ...settings, retry }));
      expect(exit._tag).toBe("Failure");
      if (exit._tag !== "Failure") return;
      expect(
        Option.getOrUndefined(Cause.findErrorOption(exit.cause)),
      ).toMatchObject({
        _tag: "MuleError",
        phase: "model",
        message:
          "Retry policy requires non-negative safe-integer counts and delays",
      });
      expect(faux.state.callCount).toBe(0);
    },
  );

  it("captures the retry budget for the turn instead of following later caller mutations", async () => {
    const retry: RetryPolicy = {
      enabled: true,
      maxRetries: 1,
      baseDelayMs: 0,
    };
    const { settings, faux } = fauxRun([
      providerFailure("first"),
      providerFailure("last admitted attempt"),
      providerFailure("must not dispatch"),
      fauxAssistantMessage("must not recover"),
    ]);
    const outcome = await Effect.runPromise(
      run({
        ...settings,
        retry,
        retryable: () => true,
        onRetry: (event) =>
          Effect.sync(() => {
            if (event.type === "retry_scheduled") retry.maxRetries = 100;
          }),
      }),
    );
    expect(outcome.status).toBe("failed");
    expect(faux.state.callCount).toBe(2);
  });

  describe("with real timers advanced to Pi's backoff schedule", () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });
    afterEach(() => {
      vi.useRealTimers();
    });

    it("retries 3 transient failures then succeeds: 4 calls, delays 2000/4000/8000 ms", async () => {
      const { settings, faux } = fauxRun([
        providerFailure("503 Service Unavailable"),
        providerFailure("502 Bad Gateway"),
        providerFailure("rate limit exceeded"),
        fauxAssistantMessage("recovered"),
      ]);
      const { events, onRetry } = collectRetry();
      const muleEvents: MuleEvent[] = [];

      const fiber = Effect.runFork(
        run(
          { ...settings, retry: policy, retryable: () => true, onRetry },
          (event) =>
            Effect.sync(() => {
              muleEvents.push(event);
            }),
        ),
      );
      // `advanceTimersByTimeAsync` flushes pending microtasks between timer
      // ticks, so the first (failing) call settles and schedules Pi's
      // backoff sleep before the advance below needs to satisfy it.
      await vi.advanceTimersByTimeAsync(2_000);
      await vi.advanceTimersByTimeAsync(4_000);
      await vi.advanceTimersByTimeAsync(8_000);

      const outcome = await Effect.runPromise(Fiber.join(fiber));

      expect(outcome.status).toBe("completed");
      expect(faux.state.callCount).toBe(4);
      expect(
        events
          .filter((event) => event.type === "retry_scheduled")
          .map((event) => event.delayMs),
      ).toEqual([2_000, 4_000, 8_000]);
      expect(
        events
          .filter((event) => event.type === "retry_attempt_started")
          .map((event) => event.attempt),
      ).toEqual([1, 2, 3]);
      expect(
        events.every((event) => event.turn.value === events[0]?.turn.value),
      ).toBe(true);
      expect(events.at(-1)).toMatchObject({
        type: "retry_finished",
        success: true,
        attempt: 3,
      });
      // A failed attempt is recorded (`message_start`/`message_end` fire for
      // every attempt) but absent from the run's own outcome messages and
      // context: it is never replayed into the next call.
      const recordedMessages = muleEvents.filter(
        (event) => event.type === "message_end" && event.value.role !== "user",
      );
      expect(recordedMessages).toHaveLength(4);
      expect(outcome.messages.filter((m) => m.role === "assistant")).toEqual([
        outcome.message,
      ]);
      expect(outcome.message.content).toEqual([
        { type: "text", text: "recovered" },
      ]);
    }, 20_000);

    it("exhausts the policy after the fourth error", async () => {
      const terminalError = "503 final failure";
      const { settings, faux } = fauxRun(
        Array.from({ length: 4 }, () => providerFailure(terminalError)),
      );
      const { events, onRetry } = collectRetry();

      const fiber = Effect.runFork(
        run({ ...settings, retry: policy, retryable: () => true, onRetry }),
      );
      await vi.advanceTimersByTimeAsync(2_000);
      await vi.advanceTimersByTimeAsync(4_000);
      await vi.advanceTimersByTimeAsync(8_000);

      const outcome = await Effect.runPromise(Fiber.join(fiber));

      expect(outcome.status).toBe("failed");
      expect(faux.state.callCount).toBe(4);
      expect(outcome.message.errorMessage).toBe(terminalError);
      expect(events.at(-1)).toMatchObject({
        type: "retry_finished",
        success: false,
        attempt: 3,
        finalError: terminalError,
      });
    }, 20_000);

    it("respects a capped gateway Retry-After within Pi's single retry schedule", async () => {
      const { settings } = fauxRun([]);
      let calls = 0;
      let refused: ReturnType<typeof providerFailure> | undefined;
      const stream: typeof settings.stream = () => {
        calls++;
        const result =
          calls === 1
            ? providerFailure("rate_limited: Supplier returned 429")
            : fauxAssistantMessage("recovered");
        if (calls === 1) refused = result;
        const out = createAssistantMessageEventStream();
        if (result.stopReason === "error")
          out.push({ type: "error", reason: "error", error: result });
        else out.push({ type: "done", reason: "stop", message: result });
        out.end(result);
        return out;
      };
      const { events, onRetry } = collectRetry();
      const fiber = Effect.runFork(
        run({
          ...settings,
          stream,
          retry: policy,
          onRetry,
          retryAfterMs: (message) => (message === refused ? 5_000 : undefined),
          retryable: (message) => message === refused,
        }),
      );
      await vi.advanceTimersByTimeAsync(4_999);
      expect(calls).toBe(1);
      await vi.advanceTimersByTimeAsync(2_001);
      const outcome = await Effect.runPromise(Fiber.join(fiber));
      expect(outcome.status).toBe("completed");
      expect(calls).toBe(2);
      expect(events[0]).toMatchObject({
        type: "retry_scheduled",
        delayMs: 5_000,
      });
    });

    it("ends an abort during backoff cleanly, without a further call", async () => {
      const { settings, faux } = fauxRun([
        providerFailure("503 Service Unavailable"),
        fauxAssistantMessage("must not run"),
      ]);
      const { events, onRetry } = collectRetry();

      const fiber = Effect.runFork(
        run({ ...settings, retry: policy, retryable: () => true, onRetry }),
      );
      // Flush the first (failing) call and the sleep it schedules, then
      // interrupt while Pi's 2 s backoff is still pending.
      await vi.advanceTimersByTimeAsync(500);
      await Effect.runPromise(Fiber.interrupt(fiber));
      const exit = await Effect.runPromise(Fiber.await(fiber));

      expect(exit._tag).toBe("Failure");
      expect(faux.state.callCount).toBe(1);
      expect(events).toHaveLength(2);
      expect(events[0]).toMatchObject({ type: "retry_scheduled", attempt: 1 });
      expect(events[1]).toMatchObject({
        type: "retry_finished",
        success: false,
        attempt: 1,
      });
    });
  });

  it("does not retry a quota/billing error", async () => {
    const { settings, faux } = fauxRun([
      providerFailure("insufficient_quota: your account is out of budget"),
    ]);
    const { events, onRetry } = collectRetry();

    const outcome = await Effect.runPromise(
      run({ ...settings, retry: policy, onRetry }),
    );

    expect(outcome.status).toBe("failed");
    expect(faux.state.callCount).toBe(1);
    expect(events).toEqual([]);
  });

  it("retains an empty provider diagnostic verbatim in typed retry observations", async () => {
    const { settings, faux } = fauxRun([
      providerFailure(""),
      fauxAssistantMessage("recovered"),
    ]);
    const { events, onRetry } = collectRetry();
    const outcome = await Effect.runPromise(
      run({
        ...settings,
        retry: { enabled: true, maxRetries: 1, baseDelayMs: 0 },
        retryable: () => true,
        onRetry,
      }),
    );
    expect(outcome.status).toBe("completed");
    expect(faux.state.callCount).toBe(2);
    expect(events[0]).toMatchObject({
      type: "retry_scheduled",
      errorMessage: "",
    });
  });

  it("reports a transport exception as its own MuleError, not wrapped as a retry-loop failure", async () => {
    const { settings } = fauxRun([]);
    const cause = new Error("bad credentials");

    const exit = await Effect.runPromiseExit(
      run({
        ...settings,
        retry: policy,
        stream: () => Promise.reject(cause),
      }),
    );

    expect(exit._tag).toBe("Failure");
    if (exit._tag !== "Failure") return;
    const failure = Cause.findErrorOption(exit.cause);
    expect(Option.isSome(failure)).toBe(true);
    expect(Option.getOrUndefined(failure)).toMatchObject({
      _tag: "MuleError",
      phase: "model",
      message: "Pi model stream could not start",
      cause,
    });
  });

  it("retains a retry-delay observer exception as a typed model failure", async () => {
    const { settings, faux } = fauxRun([providerFailure("typed transient")]);
    const cause = new Error("delay observer failed");
    const exit = await Effect.runPromiseExit(
      run({
        ...settings,
        retry: policy,
        retryable: () => true,
        retryAfterMs: () => {
          throw cause;
        },
      }),
    );
    expect(exit._tag).toBe("Failure");
    if (exit._tag !== "Failure") return;
    expect(
      Option.getOrUndefined(Cause.findErrorOption(exit.cause)),
    ).toMatchObject({
      _tag: "MuleError",
      phase: "model",
      cause,
    });
    expect(faux.state.callCount).toBe(1);
  });
});
