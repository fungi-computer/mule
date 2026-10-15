import type { SimpleStreamOptions } from "@earendil-works/pi-ai";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { describe, expect, it } from "vitest";

import type { MuleEvent } from "../src/contract.js";
import { run } from "../src/effect.js";
import { eventTypes, fauxRun, runEnds } from "./support.js";

const collect = (events: MuleEvent[]) => (event: MuleEvent) =>
  Effect.sync(() => {
    events.push(event);
  });

describe("Mule model lifecycle", () => {
  it("keeps one message identity across Pi's start, updates, and end", async () => {
    const { settings } = fauxRun([fauxAssistantMessage("hello world")]);
    const events: MuleEvent[] = [];

    await Effect.runPromise(run(settings, collect(events)));

    const assistant = events.filter(
      (event) =>
        event.type === "message_start" ||
        event.type === "message_update" ||
        event.type === "message_end",
    );
    expect(assistant.length).toBeGreaterThan(2);
    const identity = assistant[0]?.message;
    expect(identity).toMatchObject({ kind: "message" });
    expect(assistant.every((event) => event.message === identity)).toBe(true);
    const turn = events.find((event) => event.type === "turn_start");
    expect(identity?.parent).toBe(turn?.turn);
  });

  it("retains Pi's provider failure message through message_end and a failed outcome", async () => {
    const errorMessage = "429 Too Many Requests";
    const { settings } = fauxRun([
      fauxAssistantMessage([], { stopReason: "error", errorMessage }),
    ]);
    const events: MuleEvent[] = [];

    const outcome = await Effect.runPromise(run(settings, collect(events)));

    const ended = events.find((event) => event.type === "message_end");
    expect(ended?.value).toMatchObject({ stopReason: "error", errorMessage });
    expect(outcome).toMatchObject({ status: "failed", message: ended?.value });
    expect(runEnds(events)).toEqual([
      expect.objectContaining({
        outcome: "failed",
        terminal: outcome.terminal,
      }),
    ]);
  });

  it("resolves a fresh API key before each Pi call and prefers it over streamOptions", async () => {
    const seen: (string | undefined)[] = [];
    const { settings } = fauxRun([
      (_context, options) => {
        seen.push(options?.apiKey);
        return fauxAssistantMessage("first");
      },
      (_context, options) => {
        seen.push(options?.apiKey);
        return fauxAssistantMessage("second");
      },
    ]);
    let issued = 0;
    let followUps = 0;

    await Effect.runPromise(
      run({
        ...settings,
        streamOptions: { apiKey: "static" },
        resolveApiKey: (provider) =>
          Effect.sync(() => {
            expect(provider).toBe(settings.model.provider);
            issued += 1;
            return `fresh-${String(issued)}`;
          }),
        getFollowUpMessages: () =>
          Effect.sync(() =>
            followUps++ === 0
              ? [{ role: "user" as const, content: "again", timestamp: 2 }]
              : [],
          ),
      }),
    );

    expect(seen).toEqual(["fresh-1", "fresh-2"]);
  });

  it("falls back to streamOptions.apiKey when the resolver has none", async () => {
    let seen: SimpleStreamOptions | undefined;
    const { settings } = fauxRun([
      (_context, options) => {
        seen = options;
        return fauxAssistantMessage("ok");
      },
    ]);

    await Effect.runPromise(
      run({
        ...settings,
        streamOptions: { apiKey: "static" },
        resolveApiKey: () => Effect.succeed(undefined),
      }),
    );

    expect(seen?.apiKey).toBe("static");
  });

  it("fails with the provider's thrown cause and records a failed terminal", async () => {
    const { settings } = fauxRun([]);
    const thrown = new Error("invalid base URL");
    const events: MuleEvent[] = [];

    const exit = await Effect.runPromise(
      Effect.result(
        run(
          {
            ...settings,
            stream: () => {
              throw thrown;
            },
          },
          collect(events),
        ),
      ),
    );

    expect(Result.isFailure(exit) && exit.failure).toMatchObject({
      _tag: "MuleError",
      phase: "model",
      cause: thrown,
    });
    expect(eventTypes(events)).toEqual(["run_start", "turn_start", "run_end"]);
    expect(runEnds(events)[0]?.outcome).toBe("failed");
  });
});
