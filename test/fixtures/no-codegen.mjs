import { fauxModel } from "../../dist/testing.js";
// Runs the built Promise root through Pi's faux provider and a tool round trip.
// The parent test launches it with --disallow-code-generation-from-strings,
// the restriction workerd applies to every Mule host.
import process from "node:process";

import { Type } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";

import { mule } from "../../dist/index.js";

const faux = fauxModel([]);
faux.setResponses([
  fauxAssistantMessage(fauxToolCall("lookup", { key: "a" }), {
    stopReason: "toolUse",
  }),
  fauxAssistantMessage("ready"),
]);
const outcome = await mule.run({
  model: faux.model,
  stream: faux.stream,
  systemPrompt: "Use the available tool when appropriate.",
  messages: [{ role: "user", content: "ready?", timestamp: 1 }],
  tools: [
    {
      descriptor: {
        name: "lookup",
        description: "Look up a value.",
        parameters: Type.Object({ key: Type.String() }),
      },
      prepare: () =>
        Promise.resolve({
          status: "prepared",
          execute: async function* () {
            await Promise.resolve();
            yield {
              type: "result",
              result: {
                content: [{ type: "text", text: "a" }],
                isError: false,
              },
            };
          },
        }),
    },
  ],
});
const text = outcome.message.content.find((part) => part.type === "text");
if (outcome.status !== "completed" || text?.text !== "ready") {
  process.stderr.write(`unexpected outcome: ${JSON.stringify(outcome)}\n`);
  process.exitCode = 1;
}
