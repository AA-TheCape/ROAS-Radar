import type { DeterministicIngestionSource, ResolvedIngestionSource, ResolvedJourney } from './resolver.js';

export const ORDER_ATTRIBUTION_TIERS = [
  'deterministic_first_party',
  'deterministic_shopify_hint',
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

function mapDeterministicSource(source: DeterministicIngestionSource): string {
  switch (source) {
    case 'landing_session_id':
      return 'landing_session_id';
    case 'checkout_token':
      return 'checkout_token';
    case 'cart_token':
      return 'cart_token';
    case 'customer_identity':
      return 'stitched_identity_journey';
  }
}

function mapAttributionSource(source: ResolvedIngestionSource): string {
  switch (source) {
    case 'shopify_marketing_hint':
      return 'shopify_marketing_hint';
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
      source: 'shopify_marketing_hint',
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
