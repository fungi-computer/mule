import { mule } from "@fungi.computer/mule";
import type {
  MuleError,
  MuleEvent,
  MuleRunInput,
  MuleRunOutcome,
  MuleTool,
} from "@fungi.computer/mule";
import { muleEffect } from "@fungi.computer/mule/effect";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";

declare const input: MuleRunInput;
declare const event: MuleEvent;
declare const outcome: MuleRunOutcome;
declare const failure: MuleError;
declare const descriptor: MuleTool["descriptor"];

// A stranger implements tools with Promises and async iterables only.
const tool: MuleTool = {
  descriptor,
  prepare: () =>
    Promise.resolve({
      status: "prepared",
      execute: async function* (signal: AbortSignal) {
        await Promise.resolve(signal.aborted);
        yield {
          type: "result" as const,
          result: { content: [], isError: false },
        };
      },
    }),
};

const run = (): Promise<MuleRunOutcome> =>
  mule.run({ ...input, tools: [tool] });

void run;
void muleEffect.run;
void event;
void outcome;
void failure;

const faux = fauxModel([fauxAssistantMessage("pong")]);
void mule.run({ ...input, model: faux.model, stream: faux.stream });
