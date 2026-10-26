// Mule's provider-neutral public data contract, matching pi-ai 1.3.0.
// Copyright (c) 2025 Mario Zechner, MIT; see THIRD_PARTY_NOTICES.
// Provider implementations and their SDK declarations stay private. The two
// schema leaves below contain only TypeBox data, with no provider imports.
import type { TSchema } from "typebox";
import type {
  AnthropicMessagesCompat,
  BedrockCompat,
  MistralConversationsCompat,
  OpenAICompletionsCompat,
  OpenAIResponsesCompat,
} from "@earendil-works/pi-ai/providers/compat-schema";
import type {
  CacheRetention,
  ModelCost,
  ModelInputLimits,
  ModelInputModality,
  ModelPromptCache,
  ThinkingLevelMap,
} from "@earendil-works/pi-ai/providers/model-schema";

/** Provider-neutral reasoning effort requested by a caller. */
export type ThinkingLevel =
  "minimal" | "low" | "medium" | "high" | "xhigh" | "max";
/** Effective reasoning effort, including disabled thinking. */
export type ModelThinkingLevel = "off" | ThinkingLevel;

/** Built-in chat APIs recognized by the pinned Pi adapter. */
export type KnownApi =
  | "openai-completions"
  | "mistral-conversations"
  | "openai-responses"
  | "azure-openai-responses"
  | "openai-codex-responses"
  | "anthropic-messages"
  | "bedrock-converse-stream"
  | "google-generative-ai"
  | "google-vertex"
  | "pi-messages";
/** Built-in or caller-defined chat API identifier. */
export type Api = KnownApi | (string & {});
/** Provider identifier carried with a model or assistant message. */
export type ProviderId = string;
/** Provider-neutral choice to allow or disable tools. */
export type ToolChoice = "auto" | "none";
/** Provider sampling fields passed through unchanged. */
export type SamplingParams = JsonObject;
/** Sampling overrides selected by reasoning effort. */
export type SamplingParamsByThinkingLevel = Partial<
  Record<ModelThinkingLevel, SamplingParams>
>;
/** Token budgets for providers that meter reasoning separately. */
export type ThinkingBudgets = {
  minimal?: number;
  low?: number;
  medium?: number;
  high?: number;
};
/** Preferred provider stream transport. */
export type Transport = "sse" | "websocket" | "websocket-cached" | "auto";
/** Request-scoped provider environment overrides. */
export type ProviderEnv = Record<string, string>;
/** Provider headers; null removes a default header. */
export type ProviderHeaders = Record<string, string | null>;
/** Fetch implementation supplied to the provider adapter. */
export type FetchFunction = typeof globalThis.fetch;
/** HTTP status and headers observed before stream consumption. */
export type ProviderResponse = {
  status: number;
  headers: Record<string, string>;
};
/** Authentication, transport and lifecycle hooks for one provider request. */
export type ProviderRequestOptions<TModel = Model<Api>> = {
  signal?: AbortSignal;
  telemetryContext?: TelemetryContext;
  apiKey?: string;
  /** Override HTTP transport for this request. */
  fetch?: FetchFunction;
  env?: ProviderEnv;
  /** Observe or replace the provider payload before dispatch. */
  onPayload?: (
    payload: ProviderData,
    model: TModel,
  ) => ProviderData | undefined | Promise<ProviderData | undefined>;
  /** Observe status and headers before provider stream consumption. */
  onResponse?: (
    response: ProviderResponse,
    model: TModel,
  ) => void | Promise<void>;
  headers?: ProviderHeaders;
  timeoutMs?: number;
  maxRetries?: number;
  maxRetryDelayMs?: number;
};
/** Common provider stream controls, independent of any SDK. */
export type StreamOptions = {
  /** Observe an adapter-parsed event before Pi normalizes it. */
  onProviderStreamEvent?: (
    data: ProviderData,
    model: Model<Api>,
  ) => void | Promise<void>;
  temperature?: number;
  samplingParams?: SamplingParams;
  maxTokens?: number;
  transport?: Transport;
  cacheRetention?: CacheRetention;
  sessionId?: string;
  websocketConnectTimeoutMs?: number;
  metadata?: JsonObject;
} & ProviderRequestOptions;
/** Provider-neutral model request options used by Mule. */
export type SimpleStreamOptions = {
  toolChoice?: ToolChoice;
  reasoning?: ThinkingLevel;
  deferred?:
    | boolean
    | {
        window?: "15m" | "1h" | "24h";
      };
  thinkingBudgets?: ThinkingBudgets;
} & StreamOptions;
/** Text block with an optional provider continuity signature. */
export type TextContent = {
  type: "text";
  text: string;
  textSignature?: string;
};
/** Reasoning block, including opaque redacted provider thinking. */
export type ThinkingContent = {
  type: "thinking";
  thinking: string;
  thinkingSignature?: string;
  redacted?: boolean;
};
/** Base64 image bytes with their media type. */
export type ImageContent = {
  type: "image";
  data: string;
  mimeType: string;
};
/** Model-requested tool invocation with JSON arguments. */
export type ToolCall = {
  type: "toolCall";
  id: string;
  name: string;
  arguments: JsonObject;
  thoughtSignature?: string;
  namespace?: string;
  argumentsError?: string;
};
/** Token counts and provider costs for one response or tool result. */
export type Usage = {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  cacheWrite1h?: number;
  reasoning?: number;
  totalTokens: number;
  cost: {
    input: number;
    output: number;
    cacheRead: number;
    cacheWrite: number;
    total: number;
  };
};
/** Provider outcome for an assistant response. */
export type StopReason =
  "pending" | "stop" | "length" | "toolUse" | "error" | "aborted" | "deferred";
/** JSON data carried by messages and tool results. */
export type JsonValue =
  null | boolean | number | string | readonly JsonValue[] | JsonObject;
/** Provider request/event values, including optional fields, binary bytes and cancellation. */
export type ProviderValue =
  | undefined
  | null
  | boolean
  | number
  | string
  | Uint8Array
  | AbortSignal
  | readonly ProviderValue[]
  | ProviderData;
/** Parsed provider request or event fields; Pi owns the provider-specific grammar. */
export type ProviderData = { [key: string]: ProviderValue };
/** Named JSON properties in tool arguments or message metadata. */
export type JsonObject = {
  [key: string]: JsonValue;
};
/** Provider receipt used to recover a deferred assistant response. */
export type DeferredHandle = {
  provider: string;
  modelId: string;
  api: string;
  id: string;
  expiresAt?: number;
  pollAfterMs?: number;
  data?: JsonValue;
};
/** Prompt and tool-set update at a point in the transcript. */
export type SystemMessage = {
  role: "system";
  content: string | TextContent[];
  sections?: Record<string, string | null>;
  toolsAdded?: Tool[];
  toolsRemoved?: ToolReference[];
  timestamp: number;
};
/** User text and image input with its timestamp. */
export type UserMessage = {
  role: "user";
  content: string | (TextContent | ImageContent)[];
  timestamp: number;
};
/** Provider response, usage and terminal reason, preserved by Mule. */
export type AssistantMessage = {
  role: "assistant";
  content: (TextContent | ThinkingContent | ToolCall)[];
  api: Api;
  provider: ProviderId;
  model: string;
  responseModel?: string;
  responseId?: string;
  providerThinkingLevel?: string;
  thinkingLevel?: ModelThinkingLevel;
  diagnostics?: AssistantMessageDiagnostic[];
  usage: Usage;
  stopReason: StopReason;
  deferred?: DeferredHandle;
  errorMessage?: string;
  /** Provider prose for the failed conversation turn, never a diagnostic. */
  providerError?: string;
  rawStopReason?: string;
  endTurn?: boolean;
  timestamp: number;
  durationMs?: number;
};
/** Bounded evidence of a tool invocation made inside another tool. */
export type NestedToolCallRecord = {
  id: string;
  name: string;
  arguments?: JsonObject;
  argumentsBytes?: number;
  status: "ok" | "error" | "unfinished";
  durationMs?: number;
  error?: string;
};
/** Recorded nested invocations and whether the record is complete. */
export type NestedToolCalls = {
  calls: NestedToolCallRecord[];
  complete: boolean;
};
/** Correlated tool output with optional JSON details and usage. */
export type ToolResultMessage = {
  role: "toolResult";
  toolCallId: string;
  toolName: string;
  content: (TextContent | ImageContent)[];
  details?: JsonValue;
  usage?: Usage;
  nestedCalls?: NestedToolCalls;
  isError: boolean;
  timestamp: number;
  durationMs?: number;
};
/** One system, user, assistant or tool-result transcript entry. */
export type Message =
  SystemMessage | UserMessage | AssistantMessage | ToolResultMessage;
/** Provider grammar formats available for constrained sampling. */
export type GrammarFormat = "openai_lark" | "openai_regex";
/** Grammar text supplied for each supported provider format. */
export type GrammarVariants = Partial<Record<GrammarFormat, string>>;
/** Provider constraint for a tool declaration. */
export type ConstrainedSamplingConfig =
  | {
      type: "json_schema";
      strict: "prefer" | "require";
    }
  | {
      type: "grammar";
      variants: GrammarVariants;
    };
/** Model-visible tool name, description and TypeBox argument schema. */
export type Tool<TParameters extends TSchema = TSchema> = {
  name: string;
  description: string;
  parameters: TParameters;
  constrainedSampling?: false | ConstrainedSamplingConfig;
};
/** Named tool removed by a transcript system update. */
export type ToolReference = {
  name: string;
};
/** Model request context; Pi adapters normalize it before provider dispatch. */
export type Context = {
  systemPrompt?: string;
  messages: Message[];
  tools?: Tool[];
};
/** Partial updates and the terminal event for an assistant stream. */
export type AssistantMessageEvent =
  | {
      type: "start";
      partial: AssistantMessage;
    }
  | {
      type: "text_start";
      contentIndex: number;
      partial: AssistantMessage;
    }
  | {
      type: "text_delta";
      contentIndex: number;
      delta: string;
      partial: AssistantMessage;
    }
  | {
      type: "text_end";
      contentIndex: number;
      content: string;
      partial: AssistantMessage;
    }
  | {
      type: "thinking_start";
      contentIndex: number;
      partial: AssistantMessage;
    }
  | {
      type: "thinking_delta";
      contentIndex: number;
      delta: string;
      partial: AssistantMessage;
    }
  | {
      type: "thinking_end";
      contentIndex: number;
      content: string;
      partial: AssistantMessage;
    }
  | {
      type: "toolcall_start";
      contentIndex: number;
      partial: AssistantMessage;
    }
  | {
      type: "toolcall_delta";
      contentIndex: number;
      delta: string;
      partial: AssistantMessage;
    }
  | {
      type: "toolcall_end";
      contentIndex: number;
      toolCall: ToolCall;
      partial: AssistantMessage;
    }
  | {
      type: "done";
      reason: Extract<StopReason, "stop" | "length" | "toolUse" | "deferred">;
      message: AssistantMessage;
    }
  | {
      type: "error";
      reason: Extract<StopReason, "aborted" | "error">;
      error: AssistantMessage;
    };
/** Catalog fields shared by chat model definitions. */
export type BaseModel<TApi extends string> = {
  id: string;
  name: string;
  api: TApi;
  provider: ProviderId;
  baseUrl: string;
  input: ModelInputModality[];
  inputLimits?: ModelInputLimits;
  cost: ModelCost;
  headers?: Record<string, string>;
};
/** Chat model configuration accepted by Mule and its Pi adapter. */
export type Model<TApi extends Api> = {
  type?: "chat";
  reasoning: boolean;
  thinkingLevelMap?: ThinkingLevelMap;
  promptCache?: ModelPromptCache;
  contextWindow: number;
  maxTokens: number;
  samplingParams?: SamplingParams;
  samplingParamsByThinkingLevel?: SamplingParamsByThinkingLevel;
  compat?: TApi extends "openai-completions"
    ? OpenAICompletionsCompat
    : TApi extends
          | "openai-responses"
          | "azure-openai-responses"
          | "openai-codex-responses"
      ? OpenAIResponsesCompat
      : TApi extends "anthropic-messages"
        ? AnthropicMessagesCompat
        : TApi extends "bedrock-converse-stream"
          ? BedrockCompat
          : TApi extends "mistral-conversations"
            ? MistralConversationsCompat
            : never;
} & BaseModel<TApi>;
/** Serializable diagnostic information retained with an assistant message. */
export type DiagnosticErrorInfo = {
  name?: string;
  message: string;
  stack?: string;
  code?: string | number;
};
/** Timestamped provider diagnostic with optional JSON details. */
export type AssistantMessageDiagnostic = {
  type: string;
  timestamp: number;
  error?: DiagnosticErrorInfo;
  details?: JsonObject;
};

/** Only the stream operations consumed by Mule. Pi's concrete stream satisfies this port. */
export type AssistantMessageEventStream = {
  /** Await the authoritative final assistant message. */
  result(): Promise<AssistantMessage>;
} & AsyncIterable<AssistantMessageEvent>;
/** Bounded assistant-call retries with capped exponential delays. */
export type RetryPolicy = {
  enabled: boolean;
  maxRetries: number;
  baseDelayMs: number;
  maxAgentDelayMs?: number;
};
/** Scalar or homogeneous-array telemetry attribute. */
export type AttributeValue =
  | string
  | number
  | boolean
  | readonly string[]
  | readonly number[]
  | readonly boolean[];
/** Named telemetry attributes, with undefined values omitted by the owner. */
export type SpanAttributes = Record<string, AttributeValue | undefined>;
/** Name and initial attributes of a request telemetry span. */
export type SpanOptions = {
  name: string;
  attributes?: SpanAttributes;
};
/** Successful or failed telemetry completion. */
export type SpanStatus =
  | {
      status: "ok";
    }
  | {
      status: "error";
      error?: {
        name: string;
        message: string;
      };
    };
/** Parent context through which a provider opens telemetry spans. */
export type TelemetryContext = {
  /** Run provider work under a child span. */
  startSpan<T>(
    options: SpanOptions,
    callback: (span: TelemetrySpan) => T | Promise<T>,
  ): Promise<T>;
};
/** Provider telemetry span with events, attributes and completion status. */
export type TelemetrySpan = {
  /** Record a timestamped event on this span. */
  addEvent(name: string, attributes?: SpanAttributes): void;
  /** Add or replace attributes on this span. */
  setAttributes(attributes: SpanAttributes): void;
  /** Record the completion status of this span. */
  setStatus(status: SpanStatus): void;
} & TelemetryContext;
