import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { mule } from "@fungi.computer/mule";
import { fauxModel } from "@fungi.computer/mule/testing";

const faux = fauxModel([fauxAssistantMessage("pong")]);
const outcome = await mule.run(
  {
    model: faux.model,
    stream: faux.stream,
    systemPrompt: "Reply once.",
    messages: [{ role: "user", content: "ping", timestamp: 1 }],
  },
  () => {},
);
console.log(outcome.status); // "completed"
