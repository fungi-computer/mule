import { mule } from "@fungi.computer/mule";
import { muleEffect } from "@fungi.computer/mule/effect";
import * as Predicate from "effect/Predicate";

if (!Predicate.isFunction(mule.run)) {
  throw new Error("Mule stranger import did not expose the promise run facade");
}

if (!Predicate.isFunction(muleEffect.run)) {
  throw new Error("Mule stranger import did not expose the Effect run facade");
}

const { fauxModel } = await import("@fungi.computer/mule/testing");
const { fauxAssistantMessage } =
  await import("@earendil-works/pi-ai/providers/faux");
const faux = fauxModel([fauxAssistantMessage("pong")]);
const result = await mule.run({
  ...faux,
  systemPrompt: "Reply once.",
  messages: [{ role: "user", content: "ping", timestamp: 1 }],
});
if (result.status !== "completed" || result.message.content[0]?.text !== "pong")
  throw new Error("Mule testing helper failed");
