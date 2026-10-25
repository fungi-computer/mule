import { isFunction } from "effect/Predicate";
import { muleOptions, piOptions } from "./pi-options.js";
import { normalizeModel } from "./provider-model.js";
import { fauxProvider } from "@earendil-works/pi-ai/providers/faux";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import type {
  Api,
  AssistantMessage,
  AssistantMessageEventStream,
  Context,
  Model,
} from "./types.js";
import type { MuleStreamFunction } from "./contract.js";

/** Credential-free model definition for the scripted provider. */
export type FauxModelDefinition = {
  id: string;
  name?: string;
  reasoning?: boolean;
  input?: ("text" | "image")[];
  inputLimits?: Model<Api>["inputLimits"];
  cost?: {
    input: number;
    output: number;
    cacheRead: number;
    cacheWrite: number;
  };
  contextWindow?: number;
  maxTokens?: number;
};

/** Scripted provider configuration, independent of provider SDK declarations. */
export type RegisterFauxProviderOptions = {
  api?: string;
  provider?: string;
  models?: FauxModelDefinition[];
  deferred?: { pendingFetches?: number; pollAfterMs?: number };
  tokensPerSecond?: number;
  tokenSize?: { min?: number; max?: number };
};

/** Counters retained by the scripted provider. */
export type FauxProviderState = {
  callCount: number;
  deferredFetchCount: number;
  cancelledDeferred: NonNullable<AssistantMessage["deferred"]>[];
};

/** Function that observes a model request and supplies a scripted reply. */
export type FauxResponseFactory = (
  context: Context,
  options: Parameters<MuleStreamFunction>[2],
  state: FauxProviderState,
  model: Model<Api>,
) => AssistantMessage | Promise<AssistantMessage>;

/** A scripted reply or a factory that observes the normalized request. */
export type FauxResponseStep = AssistantMessage | FauxResponseFactory;

/** Deterministic model and stream with Pi's observable response state. */
export type FauxModel = Readonly<{
  /** Model supplied to Mule's run input. */
  model: Model<Api>;
  /** Bound Pi faux stream; never calls a remote provider. */
  stream: (
    ...args: Parameters<MuleStreamFunction>
  ) => AssistantMessageEventStream;
  /** Scripted replies have no typed provider failure; opt into retry explicitly. */
  retryable: (message: AssistantMessage) => boolean;
  /** Replace the responses consumed by subsequent assistant calls. */
  setResponses: (replies: readonly FauxResponseStep[]) => void;
  /** Pi's call history and counters for deterministic assertions. */
  state: FauxProviderState;
  /** Scripted replies not yet consumed, supplied by Pi's maintained queue. */
  getPendingResponseCount: () => number;
}>;

/** Create a credential-free model for tests and runnable examples. */
export function fauxModel(
  replies: readonly FauxResponseStep[],
  options: RegisterFauxProviderOptions = {},
): FauxModel {
  const faux = fauxProvider(options);
  const setResponses = (responses: readonly FauxResponseStep[]) => {
    faux.setResponses(
      responses.map((step) =>
        isFunction(step)
          ? (context, options, state, model) =>
              step(context, muleOptions(options), state, normalizeModel(model))
          : step,
      ),
    );
  };
  setResponses(replies);
  return {
    model: normalizeModel(faux.getModel()),
    stream: (model, context, streamOptions) =>
      faux.provider.streamSimple(
        model,
        normalizeContext(context),
        piOptions(streamOptions),
      ),
    retryable: () => false,
    setResponses,
    state: faux.state,
    getPendingResponseCount: () => faux.getPendingResponseCount(),
  };
}
