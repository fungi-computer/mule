import type { Message, Tool, ToolCall } from "@earendil-works/pi-ai";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import type {
  MuleCompletedTurn,
  MuleError,
  MuleEvent,
  MuleRunOutcome,
  MuleRunSettings,
  MuleToolExecutionPart,
  MuleToolRejection,
} from "./contract.js";
import {
  run as runEffect,
  type MuleRunInput as MuleEffectRunInput,
  type MuleTool as MuleEffectTool,
} from "./effect.js";

export type {
  MuleCompletedTurn,
  MuleResume,
  MuleToolCheckpoint,
  MuleError,
  MuleEvent,
  MuleIdentity,
  MuleIdentityKind,
  MuleRunOutcome,
  MuleRunSettings,
  MuleStreamFunction,
  MuleToolExecutionPart,
  MuleToolRejection,
  MuleToolResult,
} from "./contract.js";

/** Promise listener adapted once into Mule's canonical Effect program. */
export type MulePromiseListener = (
  event: MuleEvent,
  signal: AbortSignal,
) => Promise<void> | void;

/** Outcome of preparing one exact Pi tool call. */
export type MuleToolPreparation =
  | {
      readonly status: "prepared";
      /** Run the already-decoded and authorized call; abort ends it early. */
      readonly execute: (
        signal: AbortSignal,
      ) => AsyncIterable<MuleToolExecutionPart>;
    }
  | ({ readonly status: "rejected" } & MuleToolRejection);

/** Pi tool declaration paired with its Promise preparation door. */
export type MuleTool = {
  readonly descriptor: Tool;
  /** Only read-only or explicitly idempotent tools may opt into crash replay. */
  readonly replay?: "safe" | "unsafe" | undefined;
  readonly executionMode?: "parallel" | "sequential" | undefined;
  /** Decode and authorize one exact Pi tool call before it can execute. */
  readonly prepare: (
    call: ToolCall,
    signal: AbortSignal,
  ) => Promise<MuleToolPreparation>;
};

/** Inputs for one complete Pi model/tool loop through the Promise facade. */
export type MuleRunInput = MuleRunSettings & {
  readonly tools?: readonly MuleTool[] | undefined;
  /** Resolve a fresh provider credential immediately before each Pi call. */
  readonly resolveApiKey?:
    | ((provider: string, signal: AbortSignal) => Promise<string | undefined>)
    | undefined;
  /** Pull messages that should steer the active run at Pi's safe point. */
  readonly getSteeringMessages?:
    ((signal: AbortSignal) => Promise<readonly Message[]>) | undefined;
  /** Pull messages queued until the run would otherwise finish. */
  readonly getFollowUpMessages?:
    ((signal: AbortSignal) => Promise<readonly Message[]>) | undefined;
  /** Request a graceful stop after the current Pi turn has fully settled. */
  readonly shouldStopAfterTurn?:
    | ((turn: MuleCompletedTurn, signal: AbortSignal) => Promise<boolean>)
    | undefined;
};

/** Promise-facing Mule interface. */
export type Mule = {
  /**
   * Run one complete Pi model/tool loop. Rejects with the `MuleError` that
   * failed the run, or with the abort reason (an `AbortError` when the reason
   * is not an `Error`) after the caller aborts `signal`.
   */
  readonly run: (
    input: MuleRunInput,
    listener?: MulePromiseListener,
    signal?: AbortSignal,
  ) => Promise<MuleRunOutcome>;
};

function fromPromise<A>(
  phase: MuleError["phase"],
  message: string,
  evaluate: (signal: AbortSignal) => PromiseLike<A> | A,
): Effect.Effect<A, MuleError> {
  return Effect.tryPromise({
    try: (signal) => Promise.resolve(evaluate(signal)),
    catch: (cause): MuleError => ({ _tag: "MuleError", phase, message, cause }),
  });
}

function executionStream(
  call: ToolCall,
  execute: (signal: AbortSignal) => AsyncIterable<MuleToolExecutionPart>,
): Stream.Stream<MuleToolExecutionPart, MuleError> {
  // Effect closes the iterator with `return()`, which an async generator only
  // honours at its next yield. Abort first so a tool awaiting its signal can
  // reach that point instead of holding interruption open.
  const execution: AsyncIterable<MuleToolExecutionPart> = {
    [Symbol.asyncIterator]() {
      const controller = new AbortController();
      const iterator = execute(controller.signal)[Symbol.asyncIterator]();
      return {
        next: () => iterator.next(),
        return: async () => {
          controller.abort();
          return (
            (await iterator.return?.()) ?? { done: true, value: undefined }
          );
        },
      };
    },
  };
  return Stream.fromAsyncIterable(execution, (cause): MuleError => ({
    _tag: "MuleError",
    phase: "tool",
    message: `Tool "${call.name}" failed`,
    cause,
  }));
}

function liftTool(tool: MuleTool): MuleEffectTool {
  return {
    descriptor: tool.descriptor,
    executionMode: tool.executionMode,
    replay: tool.replay,
    prepare: (call) =>
      Effect.tryPromise({
        try: (signal) => tool.prepare(call, signal),
        catch: (): MuleToolRejection => ({
          message: `Tool "${call.name}" could not be prepared`,
        }),
      }).pipe(
        Effect.flatMap((preparation) =>
          preparation.status === "rejected"
            ? Effect.fail({
                message: preparation.message,
                terminate: preparation.terminate,
              })
            : Effect.succeed({
                execute: () => executionStream(call, preparation.execute),
              }),
        ),
      ),
  };
}

/** Adapt the complete Promise port set once into the canonical Effect ports. */
function liftInput(input: MuleRunInput): MuleEffectRunInput {
  const {
    tools,
    resolveApiKey,
    getSteeringMessages,
    getFollowUpMessages,
    shouldStopAfterTurn,
    ...settings
  } = input;
  return {
    ...settings,
    tools: tools?.map(liftTool),
    resolveApiKey:
      resolveApiKey &&
      ((provider) =>
        fromPromise("model", "Mule could not resolve the API key", (signal) =>
          resolveApiKey(provider, signal),
        )),
    getSteeringMessages:
      getSteeringMessages &&
      (() =>
        fromPromise(
          "control",
          "Mule could not read steering messages",
          getSteeringMessages,
        )),
    getFollowUpMessages:
      getFollowUpMessages &&
      (() =>
        fromPromise(
          "control",
          "Mule could not read follow-up messages",
          getFollowUpMessages,
        )),
    shouldStopAfterTurn:
      shouldStopAfterTurn &&
      ((turn) =>
        fromPromise("control", "Mule stop hook failed", (signal) =>
          shouldStopAfterTurn(turn, signal),
        )),
  };
}

const noListener: MulePromiseListener = () => undefined;

async function run(
  input: MuleRunInput,
  listener: MulePromiseListener = noListener,
  signal?: AbortSignal,
): Promise<MuleRunOutcome> {
  if (signal?.aborted === true) throw abortReason(signal);
  const program = runEffect(liftInput(input), (event) =>
    fromPromise("listener", "Mule listener failed", (listenerSignal) =>
      listener(event, listenerSignal),
    ),
  );
  const exit = await Effect.runPromiseExit(
    program,
    signal === undefined ? undefined : { signal },
  );
  if (Exit.isSuccess(exit)) return exit.value;
  const error = Cause.findErrorOption(exit.cause);
  // Preserve the facade's documented typed MuleError rejection.
  if (Option.isSome(error)) return Effect.runPromise(Effect.fail(error.value));
  if (signal !== undefined && Cause.hasInterruptsOnly(exit.cause))
    throw abortReason(signal);
  throw Cause.squash(exit.cause);
}

/** The caller's abort reason, as `fetch` rejects; never a non-Error value. */
function abortReason(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return reason instanceof Error
    ? reason
    : new DOMException("The operation was aborted", "AbortError");
}

/** Default Promise-facing Mule interface. */
export const mule: Mule = { run };
