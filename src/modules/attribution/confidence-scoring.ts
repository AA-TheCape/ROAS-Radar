import type { ResolvedAttributionTouchpoint, ResolvedIngestionSource, ResolvedJourney } from './resolver.js';

export const ATTRIBUTION_CONFIDENCE_CONTRACT_VERSION = 'v1';

const CONFIDENCE_DECIMAL_PLACES = 2;

const DEFAULT_SOURCE_CONFIDENCE: Record<ResolvedIngestionSource, number> = {
  landing_session_id: 1,
  checkout_token: 1,
  cart_token: 0.9,
  customer_identity: 0.6,
  shopify_marketing_hint: 0.55,
  meta_platform_reported: 0.5,
  ga4_fallback: 0.35
};

export type AttributionConfidenceMetadata = {
  confidenceScore: number;
  attributionSourceCode: string;
  matchingMethodCode: string;
  confidenceContractVersion: typeof ATTRIBUTION_CONFIDENCE_CONTRACT_VERSION;
  lastAttributionRunAt: Date;
};

export type AttributionConfidenceFingerprint = {
  sessionId: string | null;
  attributedSource: string | null;
  attributedMedium: string | null;
  attributedCampaign: string | null;
  attributedContent: string | null;
  attributedTerm: string | null;
  attributedClickIdType: string | null;
  attributedClickIdValue: string | null;
  confidenceScore: number;
  attributionReason: string;
  modelVersion: number;
  matchSource: string;
  attributionSourceCode: string;
  matchingMethodCode: string;
  confidenceContractVersion: string;
};

export type PersistedAttributionConfidenceState = AttributionConfidenceFingerprint & {
  lastAttributionRunAt: Date | null;
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
  const matchingMethodCode =
    input.attributionSourceCode === 'customer_identity' &&
    input.journey.attributionReason === 'matched_by_identity_journey'
      ? 'matched_by_customer_identity'
      : input.journey.attributionReason || 'unknown';

  return {
    confidenceScore: boundConfidenceScore(input.journey.confidenceScore),
    attributionSourceCode: input.attributionSourceCode,
    matchingMethodCode,
    confidenceContractVersion: ATTRIBUTION_CONFIDENCE_CONTRACT_VERSION,
    lastAttributionRunAt: input.lastAttributionRunAt
  };
}

export function attributionConfidenceFingerprintChanged(
  previous: AttributionConfidenceFingerprint | null | undefined,
  next: AttributionConfidenceFingerprint
): boolean {
  if (!previous) {
    return true;
  }

  const keys = Object.keys(next) as Array<keyof AttributionConfidenceFingerprint>;

  return keys.some((key) => previous[key] !== next[key]);
}
