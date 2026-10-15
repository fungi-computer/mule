import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";
import { mule } from "../src/index.js";
import { fauxModel } from "../src/testing.js";

it("runs and replaces deterministic replies without a provider account", async () => {
  const faux = fauxModel([fauxAssistantMessage("first")]);
  const run = () =>
    mule.run({
      ...faux,
      systemPrompt: "Reply once.",
      messages: [{ role: "user", content: "ping", timestamp: 1 }],
    });
  const first = await run();
  expect(first.status).toBe("completed");
  expect(first.message.content).toEqual([{ type: "text", text: "first" }]);
  faux.setResponses([fauxAssistantMessage("second")]);
  expect((await run()).message.content).toEqual([
    { type: "text", text: "second" },
  ]);
  expect(faux.state.callCount).toBe(2);
});
