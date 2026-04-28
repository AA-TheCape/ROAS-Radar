type ResolverOutcomeInput = {
  touchpoints: unknown[];
  winner: {
    isDirect?: boolean;
    ingestionSource?: string | null;
    sessionId?: string | null;
  } | null;
  tier?: string | null;
  attributionReason?: string | null;
  confidenceScore?: number | null;
  pipeline?: string | null;
  orderOccurredAtUtc?: Date | string | null;
  shopifyOrderId?: string | null;
  normalizationFailures?: Array<{
    scope?: string | null;
    reason?: string | null;
    sourceKey?: string | null;
  }>;
};

export function summarizeResolverOutcome(input: ResolverOutcomeInput): SerializableFields {
  const normalizationFailures = Array.isArray(input.normalizationFailures) ? input.normalizationFailures : [];
  const normalizedTier = normalizeString(input.tier) ?? 'unattributed';
  const resolverFallthroughDepth =
    normalizedTier === 'deterministic_first_party'
      ? 0
      : normalizedTier === 'deterministic_shopify_hint'
        ? 1
        : normalizedTier === 'ga4_fallback'
          ? 2
          : 3;
  const fallthroughStage =
    normalizedTier === 'deterministic_first_party'
      ? 'resolved_in_first_party'
      : normalizedTier === 'deterministic_shopify_hint'
        ? 'fell_through_to_shopify_hint'
        : normalizedTier === 'ga4_fallback'
          ? 'fell_through_to_ga4_fallback'
          : 'fell_through_to_unattributed';
  const baseFields = {
    attributionTier: normalizedTier,
    attributionReason: normalizeString(input.attributionReason) ?? null,
    confidenceScore: typeof input.confidenceScore === 'number' ? input.confidenceScore : null,
    pipeline: normalizeString(input.pipeline) ?? 'unknown',
    shopifyOrderId: normalizeString(input.shopifyOrderId) ?? null,
    orderOccurredAtUtc:
      input.orderOccurredAtUtc instanceof Date
        ? input.orderOccurredAtUtc.toISOString()
        : normalizeString(input.orderOccurredAtUtc) ?? null,
    resolverFallthroughDepth,
    fallthroughStage,
    normalizationFailureCount: normalizationFailures.length,
    hasNormalizationFailures: normalizationFailures.length > 0,
    firstNormalizationFailureScope: normalizeString(normalizationFailures[0]?.scope) ?? null,
    firstNormalizationFailureReason: normalizeString(normalizationFailures[0]?.reason) ?? null,
    firstNormalizationFailureSourceKey: normalizeString(normalizationFailures[0]?.sourceKey) ?? null
  };

  if (!input.winner) {
    return {
      ...baseFields,
      resolverOutcome: 'unattributed',
      touchpointCount: input.touchpoints.length
    };
  }

  return {
    ...baseFields,
    resolverOutcome: input.winner.isDirect ? 'direct_winner' : 'non_direct_winner',
    touchpointCount: input.touchpoints.length,
    winningIngestionSource: input.winner.ingestionSource ?? null,
    winningSessionId: input.winner.sessionId ?? null,
    hasWinningSessionId: Boolean(input.winner.sessionId)
  };
}

export function emitAttributionResolverOutcomeLog(input: ResolverOutcomeInput): void {
  logInfo('attribution_resolver_outcome', {
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    ...summarizeResolverOutcome(input)
  });
}

export const __observabilityTestUtils = {
  buildAttributionBacklogLog,
  emitAttributionResolverOutcomeLog,
  emitOrderAttributionBackfillJobLifecycleLog,
  parseCloudTraceContext,
  summarizeOrderAttributionBackfillReport,
  summarizeAttributionObservation,
  summarizeDualWriteConsistency,
  summarizeResolverOutcome
};
