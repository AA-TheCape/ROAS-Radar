import type { ResolvedAttributionTouchpoint, ResolvedIngestionSource, ResolvedJourney } from './resolver.js';

const CONFIDENCE_DECIMAL_PLACES = 4;

const DEFAULT_SOURCE_CONFIDENCE: Record<ResolvedIngestionSource, number> = {
  landing_session_id: 1,
  checkout_token: 1,
  cart_token: 0.9,
  customer_identity: 0.6,
  shopify_marketing_hint: 0.55,
  ga4_fallback: 0.35
};

export type AttributionConfidenceMetadata = {
  confidenceScore: number;
  attributionSourceCode: string;
  matchingMethodCode: string;
  lastAttributionRunAt: Date;
};

export function boundConfidenceScore(value: number | null | undefined, fallback = 0): number {
  const candidate = typeof value === 'number' && Number.isFinite(value) ? value : fallback;
  const bounded = Math.min(Math.max(candidate, 0), 1);

  return Number(bounded.toFixed(CONFIDENCE_DECIMAL_PLACES));
}

export function confidenceScoreForWinner(
  winner: Pick<ResolvedAttributionTouchpoint, 'ingestionSource'> | null,
  explicitScore?: number | null
): number {
  if (explicitScore !== undefined && explicitScore !== null) {
    return boundConfidenceScore(explicitScore);
  }

  if (!winner) {
    return 0;
  }

  return boundConfidenceScore(DEFAULT_SOURCE_CONFIDENCE[winner.ingestionSource]);
}

export function buildAttributionConfidenceMetadata(input: {
  journey: Pick<ResolvedJourney, 'confidenceScore' | 'attributionReason'>;
  attributionSourceCode: string;
  lastAttributionRunAt: Date;
}): AttributionConfidenceMetadata {
  return {
    confidenceScore: boundConfidenceScore(input.journey.confidenceScore),
    attributionSourceCode: input.attributionSourceCode,
    matchingMethodCode: input.journey.attributionReason || 'unknown',
    lastAttributionRunAt: input.lastAttributionRunAt
  };
}
