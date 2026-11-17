/** Bounded provider evidence, independent of SDKs, Effect and provider prose. */
export type ProviderRejection =
  | Readonly<{
      code: "usage-limit-reached";
      /** Absent for a subscription limit; "quota" for exhausted account quota or credit. */
      cause?: "quota";
      /** Provider-declared epoch milliseconds, absent when unknown. */
      resetAt?: number;
      limit?: "five_hour" | "seven_day" | "seven_day_opus" | "seven_day_sonnet";
    }>
  | Readonly<{ code: "rate-limit"; retryAfterMs?: number }>
  | Readonly<{ code: "overloaded" }>
  | Readonly<{ code: "provider_unavailable"; status?: number }>
  | Readonly<{ code: "provider_rejected"; status: number }>;

/** A provider identity and typed cause, independent of diagnostic/provider prose. */
export type ModelFailure = Readonly<{
  provider: string;
  cause:
    | ProviderRejection
    | Readonly<{
        code: "setup_required" | "tools_unsupported" | "request_failed";
      }>;
}>;
