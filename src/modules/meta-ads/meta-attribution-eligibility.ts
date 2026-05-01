export type MetaEligibilityOutcome = 'eligible_canonical' | 'eligible_parallel_only' | 'ineligible';

export type MetaEligibilityReasonCode =
  | 'meta_canonical_selected'
  | 'meta_parallel_only_below_confidence_threshold'
  | 'meta_ineligible_missing_required_fields'
  | 'meta_ineligible_failed_hard_guard'
  | 'meta_ineligible_below_parallel_threshold';

export type MetaAttributionThresholdConfig = {
  canonicalThreshold: number;
  parallelThreshold: number;
};

export type MetaAttributionEligibilityInput = {
  orderOccurredAtUtc: Date | null;
  metaTouchpointOccurredAtUtc: Date | null;
  attributionWindowDays: number;
  sourceKind: 'order_scoped' | 'order_joinable' | 'aggregate_only' | 'unknown';
  matchBasis: string | null;
  confidenceScore: number | null;
  rawPayloadReference: string | null;
  rawRecordId: number | null;
  ingestionRunId: number | null;
  normalizationFailures: string[];
};

export type MetaAttributionEligibilityEvaluation = {
  eligibilityOutcome: MetaEligibilityOutcome;
  reasonCode: MetaEligibilityReasonCode;
  eligibilityReasons: string[];
  disqualificationReasons: string[];
  parallelOnlyReasons: string[];
  eligibilitySignals: Record<string, unknown>;
  thresholds: MetaAttributionThresholdConfig;
};

export const DEFAULT_META_ATTRIBUTION_THRESHOLDS: MetaAttributionThresholdConfig = Object.freeze({
  canonicalThreshold: 0.5,
  parallelThreshold: 0.35
});

const ORDER_JOINABLE_SOURCE_KINDS = new Set(['order_scoped', 'order_joinable']);

function dedupeStrings(values: string[]): string[] {
  const deduped = new Set<string>();

  for (const value of values) {
    const normalized = value.trim();
    if (normalized) {
      deduped.add(normalized);
    }
  }

  return Array.from(deduped);
}

export function resolveMetaAttributionThresholds(
  overrides: Partial<MetaAttributionThresholdConfig> = {}
): MetaAttributionThresholdConfig {
  const thresholds: MetaAttributionThresholdConfig = {
    canonicalThreshold: overrides.canonicalThreshold ?? DEFAULT_META_ATTRIBUTION_THRESHOLDS.canonicalThreshold,
    parallelThreshold: overrides.parallelThreshold ?? DEFAULT_META_ATTRIBUTION_THRESHOLDS.parallelThreshold
  };

  if (
    !Number.isFinite(thresholds.parallelThreshold) ||
    !Number.isFinite(thresholds.canonicalThreshold) ||
    thresholds.parallelThreshold < 0 ||
    thresholds.canonicalThreshold < 0 ||
    thresholds.parallelThreshold > 1 ||
    thresholds.canonicalThreshold > 1 ||
    thresholds.parallelThreshold > thresholds.canonicalThreshold
  ) {
    throw new Error(
      `Invalid Meta attribution thresholds: parallelThreshold=${thresholds.parallelThreshold}, canonicalThreshold=${thresholds.canonicalThreshold}`
    );
  }

  return thresholds;
}

export function evaluateMetaAttributionEligibility(
  input: MetaAttributionEligibilityInput,
  overrides: Partial<MetaAttributionThresholdConfig> = {}
): MetaAttributionEligibilityEvaluation {
  const thresholds = resolveMetaAttributionThresholds(overrides);
  const hasOrderTimestamp = input.orderOccurredAtUtc !== null;
  const hasMetaTouchpoint = input.metaTouchpointOccurredAtUtc !== null;
  const hasApprovedMatchBasis = input.matchBasis !== null;
  const hasRawPayloadTraceability = Boolean(input.rawPayloadReference || input.rawRecordId !== null);
  const hasIngestionRunReference = input.ingestionRunId !== null;
  const isOrderJoinable = ORDER_JOINABLE_SOURCE_KINDS.has(input.sourceKind);
  const hasConfidenceScore = input.confidenceScore !== null;
  const touchpointBeforeOrder =
    hasOrderTimestamp &&
    hasMetaTouchpoint &&
    input.metaTouchpointOccurredAtUtc!.getTime() <= input.orderOccurredAtUtc!.getTime();
  const withinAttributionWindow =
    hasOrderTimestamp &&
    hasMetaTouchpoint &&
    touchpointBeforeOrder &&
    input.metaTouchpointOccurredAtUtc!.getTime() >=
      input.orderOccurredAtUtc!.getTime() - input.attributionWindowDays * 24 * 60 * 60 * 1000;
  const confidenceAtLeastCanonical = (input.confidenceScore ?? -1) >= thresholds.canonicalThreshold;
  const confidenceWithinParallelBand =
    input.confidenceScore !== null &&
    input.confidenceScore >= thresholds.parallelThreshold &&
    input.confidenceScore < thresholds.canonicalThreshold;
  const confidenceBelowParallelFloor =
    input.confidenceScore !== null && input.confidenceScore < thresholds.parallelThreshold;

  const eligibilitySignals = {
    hasOrderTimestamp,
    hasMetaTouchpoint,
    hasApprovedMatchBasis,
    hasRawPayloadTraceability,
    hasIngestionRunReference,
    isOrderJoinable,
    touchpointBeforeOrder,
    withinAttributionWindow,
    hasConfidenceScore,
    confidenceAtLeastCanonical,
    confidenceWithinParallelBand,
    confidenceBelowParallelFloor,
    thresholds
  };

  const missingRequiredReasons = dedupeStrings([
    ...input.normalizationFailures,
    ...(hasOrderTimestamp ? [] : ['missing_order_timestamp']),
    ...(hasMetaTouchpoint ? [] : ['missing_meta_touchpoint_timestamp']),
    ...(hasApprovedMatchBasis ? [] : ['missing_approved_match_basis']),
    ...(hasRawPayloadTraceability ? [] : ['missing_raw_payload_traceability']),
    ...(hasIngestionRunReference ? [] : ['missing_ingestion_run_reference']),
    ...(isOrderJoinable ? [] : ['aggregate_only_or_non_joinable_source']),
    ...(hasConfidenceScore ? [] : ['missing_confidence_score'])
  ]);

  if (missingRequiredReasons.length > 0) {
    return {
      eligibilityOutcome: 'ineligible',
      reasonCode: 'meta_ineligible_missing_required_fields',
      eligibilityReasons: ['meta_ineligible_missing_required_fields'],
      disqualificationReasons: missingRequiredReasons,
      parallelOnlyReasons: [],
      eligibilitySignals,
      thresholds
    };
  }

  const hardGuardReasons = dedupeStrings([
    ...(touchpointBeforeOrder ? [] : ['meta_touchpoint_after_order']),
    ...(withinAttributionWindow ? [] : ['outside_attribution_window'])
  ]);

  if (hardGuardReasons.length > 0) {
    return {
      eligibilityOutcome: 'ineligible',
      reasonCode: 'meta_ineligible_failed_hard_guard',
      eligibilityReasons: ['meta_ineligible_failed_hard_guard'],
      disqualificationReasons: hardGuardReasons,
      parallelOnlyReasons: [],
      eligibilitySignals,
      thresholds
    };
  }

  if (confidenceAtLeastCanonical) {
    return {
      eligibilityOutcome: 'eligible_canonical',
      reasonCode: 'meta_canonical_selected',
      eligibilityReasons: ['meta_canonical_selected'],
      disqualificationReasons: [],
      parallelOnlyReasons: [],
      eligibilitySignals,
      thresholds
    };
  }

  if (confidenceWithinParallelBand) {
    return {
      eligibilityOutcome: 'eligible_parallel_only',
      reasonCode: 'meta_parallel_only_below_confidence_threshold',
      eligibilityReasons: ['meta_parallel_only_below_confidence_threshold'],
      disqualificationReasons: [],
      parallelOnlyReasons: ['confidence_below_canonical_threshold'],
      eligibilitySignals,
      thresholds
    };
  }

  return {
    eligibilityOutcome: 'ineligible',
    reasonCode: 'meta_ineligible_below_parallel_threshold',
    eligibilityReasons: ['meta_ineligible_below_parallel_threshold'],
    disqualificationReasons: ['confidence_below_parallel_floor'],
    parallelOnlyReasons: [],
    eligibilitySignals,
    thresholds
  };
}
