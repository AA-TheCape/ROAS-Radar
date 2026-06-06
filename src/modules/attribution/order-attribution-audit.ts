import type { DeterministicIngestionSource, ResolvedIngestionSource, ResolvedJourney } from './resolver.js';

export const ORDER_ATTRIBUTION_TIERS = [
  'deterministic_first_party',
  'deterministic_shopify_hint',
  'platform_reported_meta',
  'ga4_fallback',
  'unattributed'
] as const;

export type OrderAttributionTier = (typeof ORDER_ATTRIBUTION_TIERS)[number];

export type OrderAttributionAuditRecord = {
  tier: OrderAttributionTier;
  source: string;
  matchedAt: Date;
  reason: string | null;
};

export type AttributionConfidenceLabel = 'high' | 'medium' | 'low' | 'none';

function mapDeterministicSource(source: DeterministicIngestionSource): string {
  switch (source) {
    case 'landing_session_id':
      return 'landing_session_id';
    case 'checkout_token':
      return 'checkout_token';
    case 'cart_token':
      return 'cart_token';
    case 'customer_identity':
      return 'customer_identity';
  }
}

function mapAttributionSource(source: ResolvedIngestionSource): string {
  switch (source) {
    case 'shopify_marketing_hint':
      return 'shopify_marketing_hint';
    case 'meta_platform_reported':
      return 'meta_platform_reported';
    case 'ga4_fallback':
      return 'ga4_fallback';
    default:
      return mapDeterministicSource(source);
  }
}

export function buildOrderAttributionAuditRecord(
  journey: Pick<ResolvedJourney, 'tier' | 'winner' | 'attributionReason'>,
  matchedAt: Date
): OrderAttributionAuditRecord {
  if (journey.tier === 'unattributed' || !journey.winner) {
    return {
      tier: 'unattributed',
      source: 'unattributed',
      matchedAt,
      reason: journey.attributionReason
    };
  }

  if (journey.tier === 'deterministic_shopify_hint') {
    return {
      tier: 'deterministic_shopify_hint',
      source: 'shopify_hint_fallback',
      matchedAt,
      reason: journey.attributionReason
    };
  }

  if (journey.tier === 'ga4_fallback') {
    return {
      tier: 'ga4_fallback',
      source: 'ga4_fallback',
      matchedAt,
      reason: journey.attributionReason
    };
  }

  if (journey.tier === 'platform_reported_meta') {
    return {
      tier: 'platform_reported_meta',
      source: 'meta_platform_reported',
      matchedAt,
      reason: journey.attributionReason
    };
  }

  if (!journey.winner.ingestionSource) {
    throw new Error('Deterministic attribution winner is missing an ingestion source');
  }

  return {
    tier: 'deterministic_first_party',
    source: mapAttributionSource(journey.winner.ingestionSource),
    matchedAt,
    reason: journey.attributionReason
  };
}

export function buildAttributionMatchSource(
  journey: Pick<ResolvedJourney, 'tier' | 'winner'>
): string {
  if (journey.tier === 'unattributed' || !journey.winner) {
    return 'unattributed';
  }

  if (journey.tier === 'deterministic_shopify_hint') {
    return 'shopify_hint_fallback';
  }

  if (journey.tier === 'ga4_fallback') {
    return 'ga4_fallback';
  }

  return mapAttributionSource(journey.winner.ingestionSource);
}

export function buildAttributionConfidenceLabel(
  confidenceScore: number
): AttributionConfidenceLabel {
  switch (confidenceScore.toFixed(2)) {
    case '1.00':
    case '0.90':
      return 'high';
    case '0.60':
      return 'medium';
    case '0.55':
    case '0.40':
    case '0.35':
    case '0.25':
      return 'low';
    case '0.00':
      return 'none';
    default:
      throw new Error(`Unsupported attribution confidence score for v1 contract: ${confidenceScore.toFixed(2)}`);
  }
}
