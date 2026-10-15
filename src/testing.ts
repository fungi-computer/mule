import {
  fauxProvider,
  type FauxProviderHandle,
  type FauxResponseStep,
  type RegisterFauxProviderOptions,
} from "@earendil-works/pi-ai/providers/faux";
import type { Api, Model } from "@earendil-works/pi-ai";

/** Deterministic model and stream with Pi's observable response state. */
export type FauxModel = Readonly<{
  /** Model supplied to Mule's run input. */
  model: Model<Api>;
  /** Bound Pi faux stream; never calls a remote provider. */
  stream: FauxProviderHandle["provider"]["streamSimple"];
  /** Replace the responses consumed by subsequent assistant calls. */
  setResponses: (replies: readonly FauxResponseStep[]) => void;
  /** Pi's call history and counters for deterministic assertions. */
  state: FauxProviderHandle["state"];
}>;

/** Create a credential-free model for tests and runnable examples. */
export function fauxModel(
  replies: readonly FauxResponseStep[],
  options: RegisterFauxProviderOptions = {},
): FauxModel {
  const faux = fauxProvider(options);
  faux.setResponses([...replies]);
  return {
    model: faux.getModel(),
    stream: faux.provider.streamSimple.bind(faux.provider),
    setResponses: (responses) => {
      faux.setResponses([...responses]);
    },
    state: faux.state,
  };
}
