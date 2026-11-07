import { createAssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { expect, it } from "vitest";
import { run } from "../src/effect.js";
import { deferred, fauxRun, userMessage } from "./support.js";

it.each(["startup", "iterator"])(
  "reports detached provider %s cleanup to its owner",
  async (boundary) => {
    const started = deferred();
    let undrained = false;
    const settings = fauxRun([]).settings;
    const fiber = Effect.runFork(
      run(
        {
          ...settings,
          messages: [userMessage("Wait")],
          onUndrained: Effect.sync(() => {
            undrained = true;
          }),
          stream: () => {
            if (boundary === "startup") {
              started.resolve();
              return new Promise<never>(() => undefined);
            }
            const events = createAssistantMessageEventStream();
            events.push({
              type: "start",
              partial: fauxAssistantMessage("waiting"),
            });
            return events;
          },
        },
        (event) =>
          Effect.sync(() => {
            if (event.type === "message_start") started.resolve();
          }),
      ),
    );
    await started.promise;
    await Effect.runPromise(Fiber.interrupt(fiber));
    expect(undrained).toBe(true);
  },
);
