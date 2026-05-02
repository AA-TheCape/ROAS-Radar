import type { TieredAttributionCandidate, TieredAttributionResolverInput, ResolvedJourney } from './resolver.js';

export type MetaAttributionSummary = {
  metaPresent: boolean;
  metaAffectedCanonical: boolean;
  metaEvaluationOutcome: 'eligible_canonical' | 'eligible_parallel_only' | 'ineligible' | 'not_evaluated';
  metaAttributionEvidenceId: string | null;
  confidenceScore: number | null;
};

function compareMetaPriority(left: TieredAttributionCandidate, right: TieredAttributionCandidate): number {
  if (right.occurredAtUtc.getTime() !== left.occurredAtUtc.getTime()) {
    return right.occurredAtUtc.getTime() - left.occurredAtUtc.getTime();
  }

  if (Boolean(right.isClickThrough) !== Boolean(left.isClickThrough)) {
    return Number(Boolean(right.isClickThrough)) - Number(Boolean(left.isClickThrough));
  }

  if (right.confidenceScore !== left.confidenceScore) {
    return right.confidenceScore - left.confidenceScore;
  }

  return (left.metaSignalId ?? left.sourceKey).localeCompare(right.metaSignalId ?? right.sourceKey);
}

function pickPrimaryMetaCandidate(candidates: TieredAttributionCandidate[]): TieredAttributionCandidate | null {
  return candidates.slice().sort(compareMetaPriority)[0] ?? null;
}

export function summarizeMetaAttribution(
  resolverInput: TieredAttributionResolverInput,
  journey: Pick<ResolvedJourney, 'tier' | 'confidenceScore'>
): MetaAttributionSummary {
  const metaCandidates = resolverInput.platformReportedMeta ?? [];

  if (metaCandidates.length === 0) {
    return {
      metaPresent: false,
      metaAffectedCanonical: false,
      metaEvaluationOutcome: 'not_evaluated',
      metaAttributionEvidenceId: null,
      confidenceScore: null
    };
  }

  const primaryCandidate = pickPrimaryMetaCandidate(metaCandidates);
  const confidenceScore = primaryCandidate?.confidenceScore ?? null;

  if (journey.tier === 'deterministic_first_party' || journey.tier === 'deterministic_shopify_hint') {
    return {
      metaPresent: true,
      metaAffectedCanonical: false,
      metaEvaluationOutcome: 'not_evaluated',
      metaAttributionEvidenceId: primaryCandidate?.metaAttributionEvidenceId ?? null,
      confidenceScore
    };
  }

  if (journey.tier === 'platform_reported_meta') {
    return {
      metaPresent: true,
      metaAffectedCanonical: true,
      metaEvaluationOutcome: 'eligible_canonical',
      metaAttributionEvidenceId: primaryCandidate?.metaAttributionEvidenceId ?? null,
      confidenceScore: journey.confidenceScore
    };
  }

  if (metaCandidates.some((candidate) => candidate.metaEligibilityOutcome === 'eligible_parallel_only')) {
    return {
      metaPresent: true,
      metaAffectedCanonical: false,
      metaEvaluationOutcome: 'eligible_parallel_only',
      metaAttributionEvidenceId: primaryCandidate?.metaAttributionEvidenceId ?? null,
      confidenceScore
    };
  }

  return {
    metaPresent: true,
    metaAffectedCanonical: false,
    metaEvaluationOutcome: 'ineligible',
    metaAttributionEvidenceId: primaryCandidate?.metaAttributionEvidenceId ?? null,
    confidenceScore
  };
}
