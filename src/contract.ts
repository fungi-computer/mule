import type {
  Api,
  AssistantMessage,
  AssistantMessageEvent,
  AssistantMessageEventStream,
  Context,
  ImageContent,
  JsonValue,
  Message,
  Model,
  RetryPolicy,
  SimpleStreamOptions,
  TextContent,
  ToolCall,
  ToolResultMessage,
  Usage,
} from "./types.js";

// Data shared by the Promise root and the `./effect` root. This module must not
// import Effect: both declaration graphs reach it.

/** Pi's model stream function, supplied explicitly by the caller. */
export type MuleStreamFunction = (
  model: Model<Api>,
  context: Context,
  options?: SimpleStreamOptions,
) => AssistantMessageEventStream | Promise<AssistantMessageEventStream>;

/** Causal roles added around Pi's model and tool loop. */
export type MuleIdentityKind =
  "run" | "turn" | "message" | "tool_call" | "tool_result" | "terminal";

/** One run-local causal identity. */
export type MuleIdentity = {
  readonly kind: MuleIdentityKind;
  readonly value: string;
  readonly parent?: MuleIdentity | undefined;
};

/** Expected failure from Mule's model, tool, control, or listener work. */
export type MuleError = {
  readonly _tag: "MuleError";
  readonly phase: "model" | "tool" | "control" | "listener";
  readonly message: string;
  /** The underlying rejection or thrown value, when one exists. */
  readonly cause?: unknown;
};

/** Result produced by a tool before Mule forms Pi's correlated result message. */
export type MuleToolResult = {
  readonly content: (TextContent | ImageContent)[];
  /** Application payload for listeners, never part of a model message. */
  readonly data?: JsonValue | undefined;
  readonly usage?: Usage | undefined;
  readonly isError: boolean;
  readonly terminate?: boolean | undefined;
};

/** Progress or terminal output from one tool execution. */
export type MuleToolExecutionPart =
  | { readonly type: "update"; readonly result: MuleToolResult }
  | { readonly type: "result"; readonly result: MuleToolResult };

/** A real parser or authorization rejection produced before tool execution. */
export type MuleToolRejection = {
  readonly message: string;
  readonly terminate?: boolean | undefined;
};

/** Facts from a completed Pi turn supplied to the retained stop hook. */
export type MuleCompletedTurn = {
  readonly message: AssistantMessage;
  readonly toolResults: readonly ToolResultMessage[];
  readonly context: readonly Message[];
  readonly newMessages: readonly Message[];
};

/** Lifecycle emitted around Pi values. Pi messages and events are not rewritten. */
export type MuleEvent =
  | { readonly type: "run_start"; readonly run: MuleIdentity }
  | {
      /** Exact prompt sent on this model attempt, including after a retry. */
      readonly type: "model_prompt";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly attempt: number;
      readonly systemPrompt: string;
    }
  | {
      readonly type: "turn_start";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
    }
  | {
      readonly type: "message_start";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly message: MuleIdentity;
      readonly value: Message;
    }
  | {
      readonly type: "message_update";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly message: MuleIdentity;
      readonly value: AssistantMessage;
      readonly event: AssistantMessageEvent;
    }
  | {
      readonly type: "message_end";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly message: MuleIdentity;
      readonly value: Message;
      /** Application payload independent of the completed model message. */
      readonly data?: JsonValue | undefined;
    }
  | {
      readonly type: "tool_execution_start";
      readonly replay: "safe" | "unsafe";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly toolCall: MuleIdentity;
      readonly call: ToolCall;
    }
  | {
      readonly type: "tool_execution_update";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly toolCall: MuleIdentity;
      readonly call: ToolCall;
      readonly result: MuleToolResult;
    }
  | {
      readonly type: "tool_execution_end";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly toolCall: MuleIdentity;
      readonly call: ToolCall;
      readonly result: MuleToolResult;
    }
  | {
      readonly type: "turn_end";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly message: AssistantMessage;
      readonly toolResults: readonly ToolResultMessage[];
    }
  | {
      /**
       * The run's one terminal. A run interrupted after `run_start` ends
       * `cancelled`; a run that fails with a non-listener `MuleError` ends
       * `failed`. A listener failure ends the run without this event.
       */
      readonly type: "run_end";
      readonly run: MuleIdentity;
      readonly terminal: MuleIdentity;
      readonly outcome: "completed" | "cancelled" | "failed";
    };

/** Durable evidence for a call in an interrupted assistant turn. */
export type MuleToolCheckpoint =
  | { readonly call: ToolCall; readonly state: "pending" }
  | {
      readonly call: ToolCall;
      readonly state: "started";
      readonly replay: "safe" | "unsafe";
    }
  | {
      readonly call: ToolCall;
      readonly state: "completed";
      readonly result: MuleToolResult;
      /** Present when the transcript message was committed too. */
      readonly message?: ToolResultMessage | undefined;
    };

/** Completed assistant turn recovered by the transcript owner, before its next model call. */
export type MuleResume = {
  readonly message: AssistantMessage;
  readonly tools: readonly MuleToolCheckpoint[];
};

/** Run inputs that are identical on the Promise and Effect roots. */
export type MuleRunSettings = {
  readonly model: Model<Api>;
  /** Override Pi's default `streamSimple`, primarily for explicit providers and tests. */
  readonly stream?: MuleStreamFunction | undefined;
  readonly systemPrompt: string;
  readonly messages: readonly Message[];
  /** Continue a durable, completed assistant turn before requesting another response. */
  readonly resume?: MuleResume | undefined;
  /** Pi stream options. Cancellation is the run's interruption, never a signal here. */
  /** Provider-native retries are disabled; the typed host policy owns retry. */
  readonly streamOptions?:
    Omit<SimpleStreamOptions, "signal" | "maxRetries"> | undefined;
  /** Maximum silence between provider stream events, in milliseconds. */
  readonly modelStreamIdleTimeoutMs?: number | undefined;
  readonly toolExecution?: "parallel" | "sequential" | undefined;
  /**
   * Bounded retry around each assistant call, using Pi's exponential delay.
   * Undefined means no retry, matching pi-ai's own default. Hosts that want
   * Pi coding-agent's numbers pass `piCodingAgentRetryDefaults`.
   * Enabled policy counts and delays must be non-negative safe integers.
   * Each assistant turn captures its policy before requesting the model.
   */
  readonly retry?: RetryPolicy | undefined;
  /**
   * Host-owned typed decision for this exact assistant message. Only true
   * permits retry; unknown messages return false, never a decision from prose.
   */
  readonly retryable: (message: AssistantMessage) => boolean;
  /** Provider's bounded Retry-After evidence for the last assistant error. */
  readonly retryAfterMs?:
    ((message: AssistantMessage) => number | undefined) | undefined;
};

/** One observed typed-retry event around a single assistant call. */
export type MuleRetryObservation =
  | {
      readonly type: "retry_scheduled";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly attempt: number;
      readonly maxAttempts: number;
      readonly delayMs: number;
      readonly errorMessage: string;
    }
  | {
      readonly type: "retry_attempt_started";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly attempt: number;
    }
  | {
      readonly type: "retry_finished";
      readonly run: MuleIdentity;
      readonly turn: MuleIdentity;
      readonly attempt: number;
      readonly success: boolean;
      readonly finalError?: string;
    };

/** Honest terminal result of one Mule invocation. */
export type MuleRunOutcome = {
  readonly status: "completed" | "cancelled" | "failed";
  readonly run: MuleIdentity;
  readonly terminal: MuleIdentity;
  readonly message: AssistantMessage;
  readonly messages: readonly Message[];
};
