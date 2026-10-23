import type { Model } from "@earendil-works/pi-ai";
import { pathToFileURL } from "node:url";

import { mule } from "../src/index.js";

const defaultBaseUrl = "https://stream.camelai.com/v1";
const defaultModel = "auto";
const instructions = "Answer concisely.";
type PromptOutcomeStatus = "completed" | "cancelled" | "failed";

function promptExitCode(
  status: PromptOutcomeStatus,
  receivedSigint: boolean,
): number {
  if (receivedSigint) return 130;
  return status === "completed" ? 0 : 1;
}

function usage(): void {
  process.stderr.write('Usage: pnpm mule:prompt -- "<prompt>"\n');
}

function configuredValue(name: string, fallback?: string): string | undefined {
  const value = process.env[name];
  return value === undefined || value.length === 0 ? fallback : value;
}

function promptFrom(args: readonly string[]): string | undefined {
  if (args.length !== 1) return undefined;
  const prompt = args[0];
  return prompt === undefined || prompt.length === 0 ? undefined : prompt;
}

function camelModel(baseUrl: string, id: string): Model<"openai-responses"> {
  return {
    id,
    name: "camelStream Auto",
    api: "openai-responses",
    provider: "camel-stream",
    baseUrl,
    reasoning: true,
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: 262_144,
    maxTokens: 32_768,
  };
}

type PromptOutcome =
  | Readonly<{ kind: "finished"; exitCode: number }>
  | Readonly<{ kind: "failed"; exitCode: number; failure: Error }>;

async function main(): Promise<PromptOutcome> {
  const rawArgs = process.argv.slice(2);
  const args = rawArgs[0] === "--" ? rawArgs.slice(1) : rawArgs;
  const prompt = promptFrom(args);
  if (prompt === undefined) {
    usage();
    return { kind: "finished", exitCode: 2 };
  }

  const apiKey = configuredValue("CAMEL_API_KEY");
  if (apiKey === undefined) {
    process.stderr.write("CAMEL_API_KEY is required\n");
    return { kind: "finished", exitCode: 2 };
  }
  const baseUrl = configuredValue("CAMEL_API_URL", defaultBaseUrl);
  const modelId = configuredValue("CAMEL_MODEL", defaultModel);
  if (baseUrl === undefined || modelId === undefined) {
    process.stderr.write("CAMEL_API_URL and CAMEL_MODEL must be nonempty\n");
    return { kind: "finished", exitCode: 2 };
  }

  const controller = new AbortController();
  const onInterrupt = (): void => {
    controller.abort(new DOMException("Interrupted", "AbortError"));
  };
  process.once("SIGINT", onInterrupt);

  try {
    const outcome = await mule.run(
      {
        model: camelModel(baseUrl, modelId),
        systemPrompt: instructions,
        messages: [{ role: "user", content: prompt, timestamp: Date.now() }],
        streamOptions: { apiKey },
        // This example makes one call without a retry policy.
        retryable: () => false,
      },
      (event) => {
        if (
          event.type === "message_update" &&
          event.event.type === "text_delta"
        )
          process.stdout.write(event.event.delta);
      },
      controller.signal,
    );
    if (outcome.status === "completed") process.stdout.write("\n");
    else process.stderr.write(`Mule prompt ${outcome.status}\n`);
    return {
      kind: "finished",
      exitCode: promptExitCode(outcome.status, controller.signal.aborted),
    };
  } catch (cause) {
    const failure = new Error(
      controller.signal.aborted
        ? "Mule prompt cancelled"
        : "Mule prompt failed",
      { cause },
    );
    process.stderr.write(
      controller.signal.aborted
        ? "Mule prompt cancelled\n"
        : "Mule prompt failed\n",
    );
    return {
      kind: "failed",
      exitCode: controller.signal.aborted ? 130 : 1,
      failure,
    };
  } finally {
    process.off("SIGINT", onInterrupt);
  }
}

const invokedAsScript =
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href;
if (invokedAsScript) process.exitCode = (await main()).exitCode;
