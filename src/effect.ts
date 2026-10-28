import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
export { decodeProviderData } from "./provider-data.js";
export { streamPi as streamModel } from "./provider.js";
import { nanoid } from "@fungi.computer/cairn";
export {
  isRetryableProviderRejection,
  observeProviderStatus,
  type ProviderRejection,
  type ProviderStatusRecorder,
} from "./provider-status.js";
// Pi's root entry also evaluates its TypeBox validator compiler and model
// stores; Mule needs only these leaf helpers, so it imports them directly and
// keeps that work out of every host's startup.
import { isContextOverflow as piIsContextOverflow } from "@earendil-works/pi-ai/utils/overflow";
import {
  DEFAULT_MAX_AGENT_RETRY_DELAY_MS,
  retryDelayMs,
} from "@earendil-works/pi-ai/utils/retry";
import { streamPi } from "./provider.js";
export { normalizeModel } from "./provider-model.js";
import type {
  AssistantMessage,
  AssistantMessageEvent,
  Context,
  Message,
  RetryPolicy,
  SimpleStreamOptions,
  Tool,
  ToolCall,
  ToolResultMessage,
} from "./types.js";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Result from "effect/Result";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import type {
  MuleCompletedTurn,
  MuleResume,
  MuleError,
  MuleEvent,
  MuleIdentity,
  MuleIdentityKind,
  MuleRetryObservation,
  MuleRunOutcome,
  MuleRunSettings,
  MuleToolExecutionPart,
  MuleToolRejection,
  MuleToolResult,
} from "./contract.js";

export type * from "./types.js";
export { Type } from "typebox";
export type { MuleRetryObservation } from "./contract.js";

/**
 * Pi coding-agent's retry defaults (`settings-manager.js` `getRetrySettings()`,
 * `@earendil-works/pi-ai` 0.99.0, the version pinned in the lockfile). Pi-ai's
 * own `RetryPolicy` has no default — an undefined policy means no retry — so
 * a host that wants Pi coding-agent's numbers passes this explicitly.
 */
export const piCodingAgentRetryDefaults: RetryPolicy = {
  enabled: true,
  maxRetries: 3,
  baseDelayMs: 2000,
  maxAgentDelayMs: 60_000,
};

/** Starting safety limits for one provider attempt; hosts may tighten them. Not public: the values are starting points, not a contract. */
const modelStreamLimits = {
  firstEventMs: 60_000,
  idleMs: 120_000,
  attemptMs: 600_000,
  outputBytes: 2 * 1024 * 1024,
} as const;

/**
 * Grace period a release waits for a provider iterator that ignores abort,
 * so a timed-out run's scope close never hangs. This is a Mule transport
 * detail with no Pi equivalent, so it stays a constant, not host config.
 */
const providerIteratorReleaseGraceMs = 1_000;
const modelLimitCause = Symbol("Mule model limit");

/** Apply Pi's maintained context-overflow semantics to an exact Pi message. */
export function isContextOverflow(
  message: AssistantMessage,
  contextWindow?: number,
): boolean {
  return piIsContextOverflow(message, contextWindow);
}

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

/** A decoded, authorized invocation ready to execute. */
export type MulePreparedTool = {
  /** Run the already-decoded and authorized tool call. */
  readonly execute: () => Stream.Stream<MuleToolExecutionPart, MuleError>;
};

/** Pi tool declaration paired with its Effect-native preparation door. */
export type MuleTool = {
  readonly descriptor: Tool;
  /** Only read-only or explicitly idempotent tools may opt into crash replay. */
  readonly replay?: "safe" | "unsafe" | undefined;
  readonly executionMode?: "parallel" | "sequential" | undefined;
  /** Decode and authorize one exact Pi tool call before it can execute. */
  readonly prepare: (
    call: ToolCall,
  ) => Effect.Effect<MulePreparedTool, MuleToolRejection>;
};

/** Effect-native listener. Completion is part of run settlement. */
export type MuleListener = (event: MuleEvent) => Effect.Effect<void, MuleError>;

/**
 * Inputs for one complete Pi model/tool loop. Cancel a run by interrupting its
 * fiber; the run then ends with a `cancelled` `run_end`.
 */
export type MuleRunInput = MuleRunSettings & {
  /** Rebuild the model system prompt immediately before each provider request. */
  readonly prepareSystemPrompt?:
    (() => Effect.Effect<string, MuleError>) | undefined;
  readonly tools?: readonly MuleTool[] | undefined;
  /** Resolve a fresh provider credential immediately before each Pi call. */
  readonly resolveApiKey?:
    | ((provider: string) => Effect.Effect<string | undefined, MuleError>)
    | undefined;
  /** Pull messages that should steer the active run at Pi's safe point. */
  readonly getSteeringMessages?:
    (() => Effect.Effect<readonly Message[], MuleError>) | undefined;
  /** Pull messages queued until the run would otherwise finish. */
  readonly getFollowUpMessages?:
    (() => Effect.Effect<readonly Message[], MuleError>) | undefined;
  /**
   * Pi's between-turn point: after a turn's tool results and before the next
   * assistant request of the same run. Return a replacement context, such as
   * one rebuilt after compaction, or `undefined` to keep the current one.
   */
  readonly prepareNextTurn?:
    | ((
        turn: MuleCompletedTurn,
      ) => Effect.Effect<readonly Message[] | undefined, MuleError>)
    | undefined;
  /** Request a graceful stop after the current Pi turn has fully settled. */
  readonly shouldStopAfterTurn?:
    | ((turn: MuleCompletedTurn) => Effect.Effect<boolean, MuleError>)
    | undefined;
  /** Effect-native observation sink for Pi's retry policy (`retry`), one call per scheduled or finished retry. */
  readonly onRetry?:
    | ((event: MuleRetryObservation) => Effect.Effect<void, MuleError>)
    | undefined;
};

const failure = (
  phase: MuleError["phase"],
  message: string,
  cause?: unknown,
): MuleError => ({ _tag: "MuleError", phase, message, cause });

const identity = (
  kind: MuleIdentityKind,
  parent?: MuleIdentity,
): MuleIdentity => ({ kind, value: `mule_${kind}_${nanoid()}`, parent });

const noListener: MuleListener = () => Effect.void;

function resultMessage(
  call: ToolCall,
  result: MuleToolResult,
): ToolResultMessage {
  const message: ToolResultMessage = {
    role: "toolResult",
    toolCallId: call.id,
    toolName: call.name,
    content: result.content,
    isError: result.isError,
    timestamp: Date.now(),
  };
  if (result.usage !== undefined) message.usage = result.usage;
  return message;
}

const errorResult = (message: string, terminate?: boolean): MuleToolResult => ({
  content: [{ type: "text", text: message }],
  isError: true,
  terminate,
});

type FinalizedTool = {
  readonly call: ToolCall;
  readonly identity: MuleIdentity;
  readonly result: MuleToolResult;
};

type PreparedTool = {
  readonly kind: "prepared";
  readonly call: ToolCall;
  readonly identity: MuleIdentity;
  readonly tool: MulePreparedTool;
};

type PreparedToolEntry =
  { readonly kind: "finalized"; readonly value: FinalizedTool } | PreparedTool;

function selectedTool(
  tools: readonly MuleTool[],
  call: ToolCall,
): MuleTool | undefined {
  return tools.find((tool) => tool.descriptor.name === call.name);
}

function emitToolEnd(
  finalized: FinalizedTool,
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<void, MuleError> {
  return emit({
    type: "tool_execution_end",
    run,
    turn,
    toolCall: finalized.identity,
    call: finalized.call,
    result: finalized.result,
  });
}

function prepareTool(
  tool: MuleTool | undefined,
  call: ToolCall,
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<PreparedToolEntry, MuleError> {
  const toolCall = identity("tool_call", turn);
  return Effect.gen(function* () {
    yield* emit({
      type: "tool_execution_start",
      replay: tool?.replay ?? "unsafe",
      run,
      turn,
      toolCall,
      call,
    });

    if (tool === undefined) {
      const result = errorResult(`Tool "${call.name}" not found`);
      const finalized = { call, identity: toolCall, result };
      yield* emitToolEnd(finalized, run, turn, emit);
      return { kind: "finalized", value: finalized };
    }

    const preparation = yield* Effect.result(tool.prepare(call));
    if (Result.isFailure(preparation)) {
      const result = errorResult(
        preparation.failure.message,
        preparation.failure.terminate,
      );
      const finalized = { call, identity: toolCall, result };
      yield* emitToolEnd(finalized, run, turn, emit);
      return { kind: "finalized", value: finalized };
    }

    return {
      kind: "prepared",
      call,
      identity: toolCall,
      tool: preparation.success,
    };
  });
}

function executePreparedTool(
  prepared: PreparedTool,
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<FinalizedTool, MuleError> {
  return Effect.gen(function* () {
    let terminal: MuleToolResult | undefined;
    let toolFailure: MuleError | undefined;
    // Only the tool stream's own failure becomes a model-visible error result.
    // A listener failure while reporting progress fails the run.
    yield* Stream.runForEach(Stream.result(prepared.tool.execute()), (part) => {
      if (Result.isFailure(part)) {
        toolFailure = part.failure;
        return Effect.void;
      }
      if (terminal !== undefined) return Effect.void;
      if (part.success.type === "result") {
        terminal = part.success.result;
        return Effect.void;
      }
      return emit({
        type: "tool_execution_update",
        run,
        turn,
        toolCall: prepared.identity,
        call: prepared.call,
        result: part.success.result,
      });
    });
    const result =
      toolFailure !== undefined
        ? errorResult(toolFailure.message)
        : (terminal ??
          errorResult(`Tool "${prepared.call.name}" returned no result`));
    const finalized = {
      call: prepared.call,
      identity: prepared.identity,
      result,
    };
    yield* emitToolEnd(finalized, run, turn, emit);
    return finalized;
  });
}

function emitToolResult(
  executed: FinalizedTool,
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<ToolResultMessage, MuleError> {
  const message = resultMessage(executed.call, executed.result);
  const messageIdentity = identity("tool_result", executed.identity);
  return Effect.gen(function* () {
    yield* emit({
      type: "message_start",
      run,
      turn,
      message: messageIdentity,
      value: message,
    });
    yield* emit({
      type: "message_end",
      run,
      turn,
      message: messageIdentity,
      value: message,
      data: executed.result.data,
    });
    return message;
  });
}

function executeToolBatch(
  calls: readonly ToolCall[],
  input: MuleRunInput,
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<
  {
    readonly messages: readonly ToolResultMessage[];
    readonly terminate: boolean;
  },
  MuleError
> {
  const tools = input.tools ?? [];
  const sequential =
    input.toolExecution === "sequential" ||
    calls.some(
      (call) => selectedTool(tools, call)?.executionMode === "sequential",
    );
  return Effect.gen(function* () {
    const executeEntry = (entry: PreparedToolEntry) =>
      entry.kind === "finalized"
        ? Effect.succeed(entry.value)
        : executePreparedTool(entry, run, turn, emit);
    const finalized = sequential
      ? yield* Effect.forEach(calls, (call) =>
          Effect.flatMap(
            prepareTool(selectedTool(tools, call), call, run, turn, emit),
            executeEntry,
          ),
        )
      : yield* Effect.gen(function* () {
          const entries = yield* Effect.forEach(calls, (call) =>
            prepareTool(selectedTool(tools, call), call, run, turn, emit),
          );
          return yield* Effect.all(entries.map(executeEntry), {
            concurrency: "unbounded",
          });
        });
    const messages = yield* Effect.forEach(finalized, (result) =>
      emitToolResult(result, run, turn, emit),
    );
    return {
      messages,
      terminate:
        finalized.length > 0 &&
        finalized.every((result) => result.result.terminate === true),
    };
  });
}

const truncatedToolResult = (call: ToolCall) =>
  errorResult(
    `Tool call "${call.name}" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.`,
  );

function failTruncatedToolCalls(
  calls: readonly ToolCall[],
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<readonly ToolResultMessage[], MuleError> {
  return Effect.gen(function* () {
    const finalized = yield* Effect.forEach(calls, (call) =>
      Effect.gen(function* () {
        const toolCall = identity("tool_call", turn);
        const result = truncatedToolResult(call);
        const value = { call, identity: toolCall, result };
        yield* emit({
          type: "tool_execution_start",
          replay: "unsafe",
          run,
          turn,
          toolCall,
          call,
        });
        yield* emitToolEnd(value, run, turn, emit);
        return value;
      }),
    );
    return yield* Effect.forEach(finalized, (result) =>
      emitToolResult(result, run, turn, emit),
    );
  });
}

type StreamedTurn = {
  readonly identity: MuleIdentity;
  readonly message: AssistantMessage;
  readonly limitReached?: boolean;
};

type StreamState = {
  started: boolean;
  terminal: AssistantMessage | undefined;
};

function pullPiEvent(
  iterator: AsyncIterator<AssistantMessageEvent>,
  controller: AbortController,
): Effect.Effect<IteratorResult<AssistantMessageEvent>, MuleError> {
  return Effect.tryPromise({
    try: (signal) => {
      const abort = () => {
        controller.abort(signal.reason);
      };
      if (signal.aborted) abort();
      else signal.addEventListener("abort", abort, { once: true });
      return Promise.resolve(iterator.next()).finally(() => {
        signal.removeEventListener("abort", abort);
      });
    },
    catch: (cause) => failure("model", "Pi model stream failed", cause),
  });
}

function withModelStreamDeadline<A>(
  effect: Effect.Effect<A, MuleError>,
  timeoutMs: number,
  reason: () => string,
): Effect.Effect<A, MuleError> {
  return effect.pipe(
    Effect.timeoutOrElse({
      duration: timeoutMs,
      orElse: () => Effect.fail(failure("model", reason(), modelLimitCause)),
    }),
  );
}

function streamTurn(
  input: MuleRunInput,
  context: Message[],
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
  attempt: number,
): Effect.Effect<StreamedTurn, MuleError> {
  const messageIdentity = identity("message", turn);
  let partial: AssistantMessage | undefined;
  const streamState: StreamState = { started: false, terminal: undefined };
  return Effect.scoped(
    Effect.gen(function* () {
      const limits = input.modelStreamLimits;
      const firstEventMs = Math.min(
        limits?.firstEventMs ?? modelStreamLimits.firstEventMs,
        modelStreamLimits.firstEventMs,
      );
      const attemptMs = Math.min(
        limits?.attemptMs ?? modelStreamLimits.attemptMs,
        modelStreamLimits.attemptMs,
      );
      const outputLimit = Math.min(
        limits?.outputBytes ?? modelStreamLimits.outputBytes,
        modelStreamLimits.outputBytes,
      );
      if (
        [
          firstEventMs,
          attemptMs,
          outputLimit,
          input.modelStreamIdleTimeoutMs ?? 1,
        ].some((value) => !Number.isSafeInteger(value) || value <= 0)
      )
        return yield* Effect.fail(
          failure(
            "model",
            "Model stream limits must be positive safe integers",
          ),
        );
      const startedAt = Date.now();
      let lastEventAt = startedAt;
      let maxGapMs = 0;
      let outputBytes = 0;
      const encoder = new TextEncoder();
      let hasEvent = false;
      const limitError = (kind: string) =>
        failure(
          "model",
          `Model ${kind} after ${String(Date.now() - startedAt)} ms (max provider gap ${String(Math.max(maxGapMs, Date.now() - lastEventAt))} ms, streamed ${String(outputBytes)} bytes)`,
          modelLimitCause,
        );
      const deadline = <A>(effect: Effect.Effect<A, MuleError>) => {
        const elapsed = Date.now() - startedAt;
        const wholeRemaining = attemptMs - elapsed;
        const gapLimit = Math.min(
          hasEvent ? modelStreamLimits.idleMs : firstEventMs,
          input.modelStreamIdleTimeoutMs ?? modelStreamLimits.idleMs,
        );
        const gapRemaining = gapLimit - (Date.now() - lastEventAt);
        const kind =
          wholeRemaining <= gapRemaining
            ? "attempt timed out"
            : hasEvent
              ? "stream timed out (silence)"
              : "first event timed out";
        return withModelStreamDeadline(
          effect,
          Math.max(0, Math.min(wholeRemaining, gapRemaining)),
          () => limitError(kind).message,
        );
      };
      const controller = new AbortController();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          controller.abort("Mule model scope closed");
        }),
      );

      const resolvedApiKey =
        (input.resolveApiKey === undefined
          ? undefined
          : yield* input.resolveApiKey(input.model.provider)) ??
        input.streamOptions?.apiKey;
      const options: SimpleStreamOptions = {
        ...input.streamOptions,
        maxRetries: 0,
        signal: controller.signal,
      };
      if (resolvedApiKey !== undefined) options.apiKey = resolvedApiKey;

      const systemPrompt =
        input.prepareSystemPrompt === undefined
          ? input.systemPrompt
          : yield* input.prepareSystemPrompt();
      if (input.prepareSystemPrompt !== undefined)
        yield* emit({ type: "model_prompt", run, turn, attempt, systemPrompt });
      const piContext: Context = {
        systemPrompt,
        messages: context,
      };
      const descriptors = (input.tools ?? []).map((tool) => tool.descriptor);
      if (descriptors.length > 0) piContext.tools = descriptors;

      const response = yield* deadline(
        Effect.tryPromise({
          try: async () => {
            const stream = input.stream ?? streamPi;
            return stream(input.model, normalizeContext(piContext), options);
          },
          catch: (cause) =>
            failure("model", "Pi model stream could not start", cause),
        }),
      );

      const iterator = yield* Effect.acquireRelease(
        Effect.sync(() => response[Symbol.asyncIterator]()),
        (active) =>
          Effect.tryPromise({
            try: () => {
              controller.abort("Mule model stream closed");
              // Provider iterators can ignore abort while a read is pending.
              // Releasing a timed-out run must not await that iterator forever.
              return new Promise<void>((resolve) => {
                const timer = setTimeout(
                  resolve,
                  providerIteratorReleaseGraceMs,
                );
                Promise.resolve(active.return?.()).then(
                  () => {
                    clearTimeout(timer);
                    resolve();
                  },
                  () => {
                    clearTimeout(timer);
                    resolve();
                  },
                );
              });
            },
            catch: (cause) => failure("model", "Pi model stream failed", cause),
          }).pipe(Effect.ignore),
      );
      const observeEvent = (
        event: AssistantMessageEvent,
      ): Effect.Effect<void, MuleError> => {
        const now = Date.now();
        maxGapMs = Math.max(maxGapMs, now - lastEventAt);
        lastEventAt = now;
        hasEvent = true;
        if (
          event.type === "text_delta" ||
          event.type === "thinking_delta" ||
          event.type === "toolcall_delta"
        ) {
          outputBytes += encoder.encode(event.delta).byteLength;
          if (outputBytes > outputLimit)
            return Effect.fail(limitError("output limit exceeded"));
        }
        if ("partial" in event) partial = event.partial;
        switch (event.type) {
          case "start": {
            context.push(event.partial);
            streamState.started = true;
            return emit({
              type: "message_start",
              run,
              turn,
              message: messageIdentity,
              value: event.partial,
            });
          }
          case "text_start":
          case "text_delta":
          case "text_end":
          case "thinking_start":
          case "thinking_delta":
          case "thinking_end":
          case "toolcall_start":
          case "toolcall_delta":
          case "toolcall_end": {
            if (streamState.started)
              context[context.length - 1] = event.partial;
            return emit({
              type: "message_update",
              run,
              turn,
              message: messageIdentity,
              value: event.partial,
              event,
            });
          }
          case "done": {
            streamState.terminal = event.message;
            return Effect.void;
          }
          case "error": {
            streamState.terminal = event.error;
            return Effect.void;
          }
        }
      };
      const pull = () => deadline(pullPiEvent(iterator, controller));
      let next = yield* pull();
      while (!next.done) {
        yield* observeEvent(next.value);
        next = yield* pull();
      }

      const completed =
        streamState.terminal ??
        (yield* deadline(
          Effect.tryPromise({
            try: () => response.result(),
            catch: (cause) =>
              failure("model", "Pi model stream did not settle", cause),
          }),
        ));
      if (streamState.started) context[context.length - 1] = completed;
      else {
        context.push(completed);
        yield* emit({
          type: "message_start",
          run,
          turn,
          message: messageIdentity,
          value: completed,
        });
      }
      yield* emit({
        type: "message_end",
        run,
        turn,
        message: messageIdentity,
        value: completed,
      });
      return { identity: messageIdentity, message: completed };
    }).pipe(
      Effect.catch((error) => {
        if (error.cause !== modelLimitCause) return Effect.fail(error);
        const value: AssistantMessage = {
          role: "assistant",
          content: partial?.content ?? [],
          api: input.model.api,
          provider: input.model.provider,
          model: input.model.id,
          usage: partial?.usage ?? {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 0,
            cost: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              total: 0,
            },
          },
          stopReason: "error",
          timestamp: Date.now(),
          errorMessage:
            input.retry?.enabled && attempt > input.retry.maxRetries
              ? `The model failed ${String(attempt)} times on this conversation: ${error.message}`
              : error.message,
        };
        return Effect.gen(function* () {
          if (!streamState.started) {
            context.push(value);
            yield* emit({
              type: "message_start",
              run,
              turn,
              message: messageIdentity,
              value,
            });
          } else context[context.length - 1] = value;
          yield* emit({
            type: "message_end",
            run,
            turn,
            message: messageIdentity,
            value,
          });
          return {
            identity: messageIdentity,
            message: value,
            limitReached: true,
          };
        });
      }),
    ),
  );
}

/**
 * Retry only the host's typed transient decision, using Pi's maintained delay.
 * Effect owns interruption of both the active stream and the backoff sleep.
 *
 * Each attempt streams and is recorded exactly like any other turn (so a
 * failed attempt stays in the retained event log for inspection), but a
 * failed attempt's message is removed from `context` before the next
 * attempt runs, so it is never replayed into the model's next call.
 */
function streamTurnWithRetry(
  input: MuleRunInput,
  context: Message[],
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<StreamedTurn, MuleError> {
  return Effect.gen(function* () {
    // One turn owns one finite budget, even if its caller updates future policy.
    const policy = input.retry === undefined ? undefined : { ...input.retry };
    if (
      policy?.enabled &&
      [
        policy.maxRetries,
        policy.baseDelayMs,
        policy.maxAgentDelayMs ?? DEFAULT_MAX_AGENT_RETRY_DELAY_MS,
      ].some((value) => !Number.isSafeInteger(value) || value < 0)
    )
      return yield* Effect.fail(
        failure(
          "model",
          "Retry policy requires non-negative safe-integer counts and delays",
        ),
      );
    let latest: StreamedTurn | undefined;
    let calls = 0;
    let retries = 0;
    let finished = false;
    let lastError: string | undefined;

    const reportRetry = (
      event: MuleRetryObservation,
    ): Effect.Effect<void, MuleError> =>
      input.onRetry === undefined ? Effect.void : input.onRetry(event);
    const finish = (success: boolean, finalError?: string) =>
      Effect.gen(function* () {
        finished = true;
        yield* reportRetry(
          finalError === undefined
            ? { type: "retry_finished", run, turn, attempt: retries, success }
            : {
                type: "retry_finished",
                run,
                turn,
                attempt: retries,
                success,
                finalError,
              },
        );
      });

    return yield* Effect.gen(function* () {
      for (;;) {
        if (latest !== undefined) {
          const index = context.indexOf(latest.message);
          if (index !== -1) context.splice(index, 1);
        }
        latest = yield* streamTurn(input, context, run, turn, emit, ++calls);
        const message = latest.message;
        if (
          message.stopReason !== "error" ||
          policy === undefined ||
          !policy.enabled ||
          retries >= policy.maxRetries
        ) {
          if (retries > 0) {
            yield* finish(
              message.stopReason !== "error" &&
                message.stopReason !== "aborted",
              message.stopReason === "error" ? message.errorMessage : undefined,
            );
          }
          return latest;
        }
        const retryable = yield* Effect.try({
          try: () => latest?.limitReached === true || input.retryable(message),
          catch: (cause) =>
            failure("model", "Typed model retry decision failed", cause),
        });
        if (!retryable) {
          if (retries > 0) yield* finish(false, message.errorMessage);
          return latest;
        }
        retries++;
        lastError = message.errorMessage ?? "Unknown error";
        const after = yield* Effect.try({
          try: () => input.retryAfterMs?.(message),
          catch: (cause) =>
            failure("model", "Model retry delay observation failed", cause),
        });
        const minimumDelayMs = Math.max(
          retryDelayMs(policy, retries),
          after !== undefined &&
            Number.isSafeInteger(after) &&
            after >= 0 &&
            after <= 60_000
            ? after
            : 0,
        );
        // Add rather than subtract jitter: a provider's Retry-After is a floor.
        // Jitter stops at one minute; a longer explicit host policy stays intact.
        const jitterMs = Math.min(
          1_000,
          minimumDelayMs / 4,
          Math.max(0, 60_000 - minimumDelayMs),
        );
        const delayMs = minimumDelayMs + Math.floor(Math.random() * jitterMs);
        yield* reportRetry({
          type: "retry_scheduled",
          run,
          turn,
          attempt: retries,
          maxAttempts: policy.maxRetries,
          delayMs,
          errorMessage: lastError,
        });
        yield* Effect.sleep(delayMs);
        yield* reportRetry({
          type: "retry_attempt_started",
          run,
          turn,
          attempt: retries,
        });
      }
    }).pipe(
      Effect.onInterrupt(() =>
        retries === 0 || finished ? Effect.void : finish(false, lastError),
      ),
    );
  });
}

const statusFor = (message: AssistantMessage): MuleRunOutcome["status"] =>
  message.stopReason === "aborted"
    ? "cancelled"
    : message.stopReason === "error"
      ? "failed"
      : "completed";

function emitPendingMessages(
  messages: readonly Message[],
  context: Message[],
  newMessages: Message[],
  run: MuleIdentity,
  turn: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<void, MuleError> {
  return Effect.forEach(messages, (message) =>
    Effect.gen(function* () {
      const messageIdentity = identity("message", turn);
      context.push(message);
      newMessages.push(message);
      yield* emit({
        type: "message_start",
        run,
        turn,
        message: messageIdentity,
        value: message,
      });
      yield* emit({
        type: "message_end",
        run,
        turn,
        message: messageIdentity,
        value: message,
      });
    }),
  ).pipe(Effect.asVoid);
}

type CompletedLoopTurn = {
  readonly message: AssistantMessage;
  readonly toolResults: readonly ToolResultMessage[];
  readonly terminate: boolean;
  readonly modelLimitReason?: string;
};

function completeLoopTurn(
  input: MuleRunInput,
  context: Message[],
  newMessages: Message[],
  run: MuleIdentity,
  turn: MuleIdentity,
  pending: readonly Message[],
  emit: MuleListener,
): Effect.Effect<CompletedLoopTurn, MuleError> {
  return Effect.gen(function* () {
    yield* emit({ type: "turn_start", run, turn });
    yield* emitPendingMessages(pending, context, newMessages, run, turn, emit);
    const streamed = yield* streamTurnWithRetry(
      input,
      context,
      run,
      turn,
      emit,
    );
    // Pi keeps partially streamed tool calls on an aborted or errored message.
    // Like Pi's loop, the turn ends there: nothing executes and no tool result
    // pretends a call ran. The run then settles with the honest terminal.
    const interrupted = statusFor(streamed.message) !== "completed";
    const calls = streamed.message.content.filter(
      (content): content is ToolCall => content.type === "toolCall",
    );
    const batch =
      calls.length === 0 || interrupted
        ? { messages: [], terminate: false }
        : streamed.message.stopReason === "length"
          ? {
              messages: yield* failTruncatedToolCalls(calls, run, turn, emit),
              terminate: false,
            }
          : yield* executeToolBatch(calls, input, run, turn, emit);
    context.push(...batch.messages);
    newMessages.push(streamed.message, ...batch.messages);
    yield* emit({
      type: "turn_end",
      run,
      turn,
      message: streamed.message,
      toolResults: batch.messages,
    });
    const completed: CompletedLoopTurn = {
      message: streamed.message,
      toolResults: batch.messages,
      terminate: batch.terminate,
    };
    if (streamed.limitReached && streamed.message.errorMessage !== undefined)
      return { ...completed, modelLimitReason: streamed.message.errorMessage };
    return completed;
  });
}

/** Resolve only missing results. A stored and current safe policy are both required. */
function resumeTurn(
  input: MuleRunInput,
  resume: MuleResume,
  context: Message[],
  newMessages: Message[],
  run: MuleIdentity,
  emit: MuleListener,
): Effect.Effect<CompletedLoopTurn, MuleError> {
  const turn = identity("turn", run);
  return Effect.gen(function* () {
    // Do not emit turn_start: the assistant response is already durable and
    // no new model request is being dispatched for this recovered boundary.
    if (statusFor(resume.message) !== "completed")
      return {
        message: resume.message,
        toolResults: [],
        terminate: true,
      };
    const recovered = yield* Effect.forEach(resume.tools, (checkpoint) =>
      Effect.gen(function* () {
        if (
          checkpoint.state === "completed" &&
          checkpoint.message !== undefined
        )
          return {
            message: checkpoint.message,
            terminate: checkpoint.result.terminate === true,
            recorded: true,
          };
        const tool = selectedTool(input.tools ?? [], checkpoint.call);
        let finalized: FinalizedTool;
        if (
          resume.message.stopReason !== "length" &&
          (checkpoint.state === "pending" ||
            (checkpoint.state === "started" &&
              checkpoint.replay === "safe" &&
              tool?.replay === "safe"))
        ) {
          const prepared = yield* prepareTool(
            tool,
            checkpoint.call,
            run,
            turn,
            emit,
          );
          finalized =
            prepared.kind === "finalized"
              ? prepared.value
              : yield* executePreparedTool(prepared, run, turn, emit);
        } else {
          finalized = {
            call: checkpoint.call,
            identity: identity("tool_call", turn),
            result:
              checkpoint.state === "completed"
                ? checkpoint.result
                : checkpoint.state === "pending" &&
                    resume.message.stopReason === "length"
                  ? truncatedToolResult(checkpoint.call)
                  : errorResult(
                      "Interrupted by a restart; it may or may not have completed; check before retrying.",
                    ),
          };
          yield* emit({
            type: "tool_execution_start",
            replay: "unsafe",
            run,
            turn,
            toolCall: finalized.identity,
            call: finalized.call,
          });
          yield* emitToolEnd(finalized, run, turn, emit);
        }
        return {
          message: yield* emitToolResult(finalized, run, turn, emit),
          terminate: finalized.result.terminate === true,
          recorded: false,
        };
      }),
    );
    const messages = recovered
      .filter(({ recorded }) => !recorded)
      .map(({ message }) => message);
    context.push(...messages);
    newMessages.push(...messages);
    return {
      message: resume.message,
      toolResults: recovered.map(({ message }) => message),
      terminate:
        recovered.length > 0 && recovered.every(({ terminate }) => terminate),
    };
  });
}

function selectNextTurn(
  input: MuleRunInput,
  turn: CompletedLoopTurn,
  context: readonly Message[],
  newMessages: readonly Message[],
): Effect.Effect<
  { readonly continue: boolean; readonly pending: readonly Message[] },
  MuleError
> {
  return Effect.gen(function* () {
    if (
      input.shouldStopAfterTurn !== undefined &&
      (yield* input.shouldStopAfterTurn({
        message: turn.message,
        toolResults: turn.toolResults,
        context,
        newMessages,
      }))
    )
      return { continue: false, pending: [] };

    const steering =
      input.getSteeringMessages === undefined
        ? []
        : [...(yield* input.getSteeringMessages())];
    const hasToolCalls = turn.message.content.some(
      (content) => content.type === "toolCall",
    );
    if ((hasToolCalls && !turn.terminate) || steering.length > 0)
      return { continue: true, pending: steering };

    const followUps =
      input.getFollowUpMessages === undefined
        ? []
        : [...(yield* input.getFollowUpMessages())];
    return { continue: followUps.length > 0, pending: followUps };
  });
}

function finishRun(
  run: MuleIdentity,
  terminal: MuleIdentity,
  message: AssistantMessage,
  messages: readonly Message[],
  status: MuleRunOutcome["status"],
  emit: MuleListener,
  modelLimitReason?: string,
): Effect.Effect<MuleRunOutcome, MuleError> {
  return Effect.gen(function* () {
    yield* emit({ type: "run_end", run, terminal, outcome: status });
    const outcome: MuleRunOutcome = {
      status,
      run,
      terminal,
      message,
      messages,
    };
    if (modelLimitReason !== undefined) return { ...outcome, modelLimitReason };
    return outcome;
  });
}

/** Run Pi's model/tool loop inside Effect without changing Pi's values. */
export function run(
  input: MuleRunInput,
  listener: MuleListener = noListener,
): Effect.Effect<MuleRunOutcome, MuleError> {
  return Effect.gen(function* () {
    const semaphore = yield* Semaphore.make(1);
    const lifecycle = { started: false, ended: false };
    const emit: MuleListener = (event) =>
      Effect.suspend(() => {
        if (event.type === "run_end") lifecycle.ended = true;
        return semaphore.withPermits(1)(listener(event));
      }).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            if (event.type === "run_start") lifecycle.started = true;
          }),
        ),
      );
    const runIdentity = identity("run");
    const terminalIdentity = identity("terminal", runIdentity);
    return yield* runLoop(input, emit, runIdentity, terminalIdentity).pipe(
      Effect.onExit((exit) => {
        const outcome = abnormalOutcome(exit);
        if (!lifecycle.started || lifecycle.ended || outcome === undefined)
          return Effect.void;
        // The exit is already decided; a listener failure here cannot change it.
        return emit({
          type: "run_end",
          run: runIdentity,
          terminal: terminalIdentity,
          outcome,
        }).pipe(Effect.ignore);
      }),
    );
  });
}

/** Terminal outcome owed to the listener for an exit that produced no outcome. */
function abnormalOutcome(
  exit: Exit.Exit<MuleRunOutcome, MuleError>,
): "cancelled" | "failed" | undefined {
  if (Exit.isSuccess(exit)) return undefined;
  if (Cause.hasInterruptsOnly(exit.cause)) return "cancelled";
  const error = Cause.findErrorOption(exit.cause);
  // A failed listener cannot be trusted to record its own terminal.
  if (Option.isSome(error) && error.value.phase === "listener")
    return undefined;
  return "failed";
}

function runLoop(
  input: MuleRunInput,
  emit: MuleListener,
  runIdentity: MuleIdentity,
  terminalIdentity: MuleIdentity,
): Effect.Effect<MuleRunOutcome, MuleError> {
  return Effect.gen(function* () {
    const context: Message[] = [...input.messages];
    const newMessages: Message[] = [];
    let pending =
      input.getSteeringMessages === undefined
        ? []
        : [...(yield* input.getSteeringMessages())];
    let finalMessage: AssistantMessage;
    let continueLoop: boolean;

    yield* emit({ type: "run_start", run: runIdentity });
    if (input.resume !== undefined) {
      const resumed = yield* resumeTurn(
        input,
        input.resume,
        context,
        newMessages,
        runIdentity,
        emit,
      );
      const status = statusFor(resumed.message);
      const next =
        status === "completed"
          ? yield* selectNextTurn(input, resumed, context, newMessages)
          : { continue: false, pending: [] };
      if (!next.continue && pending.length === 0)
        return yield* finishRun(
          runIdentity,
          terminalIdentity,
          resumed.message,
          newMessages,
          status,
          emit,
        );
      pending.push(...next.pending);
    }

    do {
      const completed = yield* completeLoopTurn(
        input,
        context,
        newMessages,
        runIdentity,
        identity("turn", runIdentity),
        pending,
        emit,
      );
      finalMessage = completed.message;
      const status = statusFor(completed.message);
      if (status !== "completed") {
        return yield* finishRun(
          runIdentity,
          terminalIdentity,
          completed.message,
          newMessages,
          status,
          emit,
          completed.modelLimitReason,
        );
      }
      // Tool results always continue the run: this is where Pi checks its
      // context before the next assistant response (Pi 0.84.4+).
      if (
        input.prepareNextTurn !== undefined &&
        !completed.terminate &&
        completed.message.content.some((part) => part.type === "toolCall")
      ) {
        const replaced = yield* input.prepareNextTurn({
          message: completed.message,
          toolResults: completed.toolResults,
          context,
          newMessages,
        });
        if (replaced !== undefined)
          context.splice(0, context.length, ...replaced);
      }
      const next = yield* selectNextTurn(
        input,
        completed,
        context,
        newMessages,
      );
      pending = [...next.pending];
      continueLoop = next.continue;
    } while (continueLoop);
    return yield* finishRun(
      runIdentity,
      terminalIdentity,
      finalMessage,
      newMessages,
      "completed",
      emit,
    );
  });
}

/** Canonical Effect-native Mule interface. */
export type MuleEffect = {
  /** Run one complete Pi model/tool loop. */
  readonly run: (
    input: MuleRunInput,
    listener?: MuleListener,
  ) => Effect.Effect<MuleRunOutcome, MuleError>;
};

/** Canonical Effect-native Mule value. */
export const muleEffect: MuleEffect = { run };
