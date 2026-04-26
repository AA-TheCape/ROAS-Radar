import type { DeterministicIngestionSource, ResolvedAttributionTouchpoint } from './resolver.js';

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

export function buildOrderAttributionAuditRecord(
  winner: Pick<ResolvedAttributionTouchpoint, 'attributionReason' | 'ingestionSource'> | null,
  matchedAt: Date
): OrderAttributionAuditRecord {
  if (!winner || winner.attributionReason === 'unattributed') {
    return {
      tier: 'unattributed',
      source: 'unattributed',
      matchedAt,
      reason: 'unattributed'
    };
  }

  if (winner.attributionReason === 'shopify_hint_derived') {
    return {
      tier: 'deterministic_shopify_hint',
      source: 'shopify_marketing_hint',
      matchedAt,
      reason: winner.attributionReason
    };
  }

  if (!winner.ingestionSource) {
    throw new Error('Deterministic attribution winner is missing an ingestion source');
  }

  return {
    tier: 'deterministic_first_party',
    source: mapDeterministicSource(winner.ingestionSource),
    matchedAt,
    reason: winner.attributionReason
  };
}
