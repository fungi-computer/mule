import * as Schema from "effect/Schema";
import type { ModelFailure, ProviderRejection } from "./model-failure.js";

const providerRejection: Schema.Codec<ProviderRejection> = Schema.Union([
  Schema.Struct({
    code: Schema.Literal("usage-limit-reached"),
    cause: Schema.optionalKey(Schema.Literal("quota")),
    resetAt: Schema.optionalKey(Schema.Number),
    limit: Schema.optionalKey(
      Schema.Literals([
        "five_hour",
        "seven_day",
        "seven_day_opus",
        "seven_day_sonnet",
      ]),
    ),
  }),
  Schema.Struct({
    code: Schema.Literal("rate-limit"),
    retryAfterMs: Schema.optionalKey(Schema.Number),
  }),
  Schema.Struct({ code: Schema.Literal("overloaded") }),
  Schema.Struct({
    code: Schema.Literal("provider_unavailable"),
    status: Schema.optionalKey(Schema.Number),
  }),
  Schema.Struct({
    code: Schema.Literal("provider_rejected"),
    status: Schema.Number,
  }),
]);

/** Storage codec for plain model-failure data; exposed only by Mule's Effect door. */
export const ModelFailureSchema: Schema.Codec<ModelFailure> = Schema.Struct({
  provider: Schema.String,
  cause: Schema.Union([
    providerRejection,
    Schema.Struct({
      code: Schema.Literals([
        "setup_required",
        "tools_unsupported",
        "request_failed",
      ]),
    }),
  ]),
});
