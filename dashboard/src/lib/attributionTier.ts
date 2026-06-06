import type { AttributionTier } from './api';

export const ATTRIBUTION_TIER_VALUES = [
  'deterministic_first_party',
  'deterministic_shopify_hint',
  'platform_reported_meta',
  'ga4_fallback',
  'unattributed'
] as const satisfies ReadonlyArray<AttributionTier>;

export const ATTRIBUTION_TIER_PRECEDENCE_TOOLTIP =
  'Canonical winner precedence is deterministic first-party, Shopify hint, Meta platform-reported, GA4 fallback, then unattributed. Meta evidence can also appear as parallel-only influence when it did not change the canonical winner.';

const ATTRIBUTION_TIER_META: Record<
  AttributionTier,
  {
    label: string;
    description: string;
    badgeTone: 'brand' | 'teal' | 'warning' | 'danger';
  }
> = {
  deterministic_first_party: {
    label: 'Deterministic first-party',
    description:
      'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
    badgeTone: 'brand'
  },
  deterministic_shopify_hint: {
    label: 'Deterministic Shopify hint',
    description: 'Recovered synthetically from Shopify marketing hints after first-party resolution failed.',
    badgeTone: 'teal'
  },
  platform_reported_meta: {
    label: 'Meta platform-reported',
    description:
      'Canonical winner recovered from Meta platform-reported attribution after first-party and Shopify-hint matches were unavailable. Parallel-only Meta evidence is shown separately when Meta was present but did not affect the canonical winner.',
    badgeTone: 'warning'
  },
  ga4_fallback: {
    label: 'GA4 fallback',
    description:
      'Recovered from the GA4 fallback contract only after first-party, Shopify-hint, and Meta platform-reported matches were unavailable.',
    badgeTone: 'warning'
  },
  unattributed: {
    label: 'Unattributed',
    description:
      'No eligible first-party, Shopify hint, Meta platform-reported, or GA4 fallback match qualified, or the required timing data could not be normalized.',
    badgeTone: 'danger'
  }
};

export function isAttributionTier(value: string | null | undefined): value is AttributionTier {
  return ATTRIBUTION_TIER_VALUES.includes(value as AttributionTier);
}

export function formatAttributionTierLabel(tier: AttributionTier): string {
  return ATTRIBUTION_TIER_META[tier].label;
}

export function getAttributionTierDescription(tier: AttributionTier): string {
  return ATTRIBUTION_TIER_META[tier].description;
}

export function getAttributionTierBadgeTone(tier: AttributionTier): 'brand' | 'teal' | 'warning' | 'danger' {
  return ATTRIBUTION_TIER_META[tier].badgeTone;
}
