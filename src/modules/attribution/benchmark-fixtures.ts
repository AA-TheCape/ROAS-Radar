import type {
  AttributionAllocationStatus,
  AttributionLookbackRule,
  AttributionModel,
  AttributionTouchpoint
} from './engine.js';

export const NORTHBEAM_BENCHMARK_FIXTURE_SET_VERSION = '2026-04-30-v1';

export const NORTHBEAM_BENCHMARK_COHORTS = [
  'direct_only',
  'mixed_click_view',
  'click_id_only_missing_utms',
  'identity_journey_fallback',
  'shopify_hint_fallback',
  'same_timestamp_tie',
  'no_eligible_touches'
] as const;

export type NorthbeamBenchmarkCohort = (typeof NORTHBEAM_BENCHMARK_COHORTS)[number];

export type NorthbeamBenchmarkReferenceCredit = {
  touchpointId: string;
  revenueCredit: string;
  creditWeight: number;
  isPrimary: boolean;
  source?: string | null;
  medium?: string | null;
  isDirect?: boolean;
};

export type NorthbeamBenchmarkReferenceModel = {
  allocationStatus: AttributionAllocationStatus;
  winnerTouchpointId: string | null;
  totalRevenueCredited: string;
  touchpointCountConsidered: number;
  eligibleClickCount: number;
  eligibleViewCount: number;
  lookbackRuleApplied: AttributionLookbackRule;
  directSuppressionApplied?: boolean;
  deterministicBlockApplied?: boolean;
  credits: NorthbeamBenchmarkReferenceCredit[];
};

export type NorthbeamBenchmarkFixture = {
  fixtureId: string;
  name: string;
  cohort: NorthbeamBenchmarkCohort;
  orderId: string;
  orderOccurredAt: string;
  orderRevenue: string;
  touchpoints: AttributionTouchpoint[];
  northbeamReferenceByModel: Record<AttributionModel, NorthbeamBenchmarkReferenceModel>;
};

function buildTouchpoint(
  touchpointId: string,
  occurredAt: string,
  overrides: Partial<AttributionTouchpoint> = {}
): AttributionTouchpoint {
  return {
    touchpointId,
    sessionId: `session:${touchpointId}`,
    occurredAt: new Date(occurredAt),
    source: 'google',
    medium: 'cpc',
    campaign: `campaign:${touchpointId}`,
    content: null,
    term: null,
    clickIdType: 'gclid',
    clickIdValue: `gclid:${touchpointId}`,
    attributionReason: 'matched_by_customer_identity',
    evidenceSource: 'customer_identity',
    engagementType: 'click',
    isDirect: false,
    isForced: false,
    isSynthetic: false,
    ...overrides
  };
}

function weightedCredit(
  touchpointId: string,
  revenueCredit: string,
  creditWeight: number,
  isPrimary: boolean,
  overrides: Omit<NorthbeamBenchmarkReferenceCredit, 'touchpointId' | 'revenueCredit' | 'creditWeight' | 'isPrimary'> = {}
): NorthbeamBenchmarkReferenceCredit {
  return {
    touchpointId,
    revenueCredit,
    creditWeight,
    isPrimary,
    ...overrides
  };
}

function winnerTakeAll(
  winnerTouchpointId: string,
  totalRevenueCredited: string,
  touchpointCountConsidered: number,
  eligibleClickCount: number,
  eligibleViewCount: number,
  lookbackRuleApplied: AttributionLookbackRule,
  options: {
    allocationStatus?: AttributionAllocationStatus;
    directSuppressionApplied?: boolean;
    deterministicBlockApplied?: boolean;
    source?: string | null;
    medium?: string | null;
    isDirect?: boolean;
  } = {}
): NorthbeamBenchmarkReferenceModel {
  return {
    allocationStatus: options.allocationStatus ?? 'attributed',
    winnerTouchpointId,
    totalRevenueCredited,
    touchpointCountConsidered,
    eligibleClickCount,
    eligibleViewCount,
    lookbackRuleApplied,
    directSuppressionApplied: options.directSuppressionApplied ?? false,
    deterministicBlockApplied: options.deterministicBlockApplied ?? false,
    credits: [
      weightedCredit(winnerTouchpointId, totalRevenueCredited, 1, true, {
        source: options.source,
        medium: options.medium,
        isDirect: options.isDirect
      })
    ]
  };
}

function linearSplit(
  credits: NorthbeamBenchmarkReferenceCredit[],
  touchpointCountConsidered: number,
  eligibleClickCount: number,
  eligibleViewCount: number,
  lookbackRuleApplied: AttributionLookbackRule
): NorthbeamBenchmarkReferenceModel {
  return {
    allocationStatus: credits.length > 0 ? 'attributed' : 'no_eligible_touches',
    winnerTouchpointId: credits.find((credit) => credit.isPrimary)?.touchpointId ?? null,
    totalRevenueCredited: credits.reduce((sum, credit) => sum + Number.parseFloat(credit.revenueCredit), 0).toFixed(2),
    touchpointCountConsidered,
    eligibleClickCount,
    eligibleViewCount,
    lookbackRuleApplied,
    directSuppressionApplied: false,
    deterministicBlockApplied: false,
    credits
  };
}

function noCredit(
  allocationStatus: AttributionAllocationStatus,
  touchpointCountConsidered: number,
  eligibleClickCount: number,
  eligibleViewCount: number,
  lookbackRuleApplied: AttributionLookbackRule,
  options: {
    deterministicBlockApplied?: boolean;
  } = {}
): NorthbeamBenchmarkReferenceModel {
  return {
    allocationStatus,
    winnerTouchpointId: null,
    totalRevenueCredited: '0.00',
    touchpointCountConsidered,
    eligibleClickCount,
    eligibleViewCount,
    lookbackRuleApplied,
    directSuppressionApplied: false,
    deterministicBlockApplied: options.deterministicBlockApplied ?? false,
    credits: []
  };
}

export const NORTHBEAM_BENCHMARK_FIXTURES: NorthbeamBenchmarkFixture[] = [
  {
    fixtureId: 'direct-only-journey',
    name: 'Direct-only journey remains direct across deterministic models',
    cohort: 'direct_only',
    orderId: 'order-direct-only',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '120.00',
    touchpoints: [
      buildTouchpoint('direct-early', '2026-04-20T00:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        clickIdType: null,
        clickIdValue: null,
        isDirect: true
      }),
      buildTouchpoint('direct-late', '2026-04-28T00:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        clickIdType: null,
        clickIdValue: null,
        isDirect: true
      })
    ],
    northbeamReferenceByModel: {
      first_touch: winnerTakeAll('direct-early', '120.00', 2, 2, 0, '28d_click', { isDirect: true }),
      last_touch: winnerTakeAll('direct-late', '120.00', 2, 2, 0, '28d_click', { isDirect: true }),
      last_non_direct: winnerTakeAll('direct-late', '120.00', 2, 2, 0, '28d_click', { isDirect: true }),
      linear: linearSplit(
        [
          weightedCredit('direct-early', '60.00', 0.5, true, { isDirect: true }),
          weightedCredit('direct-late', '60.00', 0.5, false, { isDirect: true })
        ],
        2,
        2,
        0,
        '28d_click'
      ),
      clicks_only: winnerTakeAll('direct-late', '120.00', 2, 2, 0, '28d_click', { isDirect: true }),
      hinted_fallback_only: noCredit('blocked_by_deterministic', 2, 2, 0, '28d_click', {
        deterministicBlockApplied: true
      })
    }
  },
  {
    fixtureId: 'mixed-click-view-journey',
    name: 'Mixed click and view journey preserves click and view windows',
    cohort: 'mixed_click_view',
    orderId: 'order-mixed-click-view',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '100.00',
    touchpoints: [
      buildTouchpoint('click-earliest', '2026-04-10T00:00:00.000Z'),
      buildTouchpoint('view-middle', '2026-04-28T00:00:00.000Z', {
        clickIdType: null,
        clickIdValue: null,
        engagementType: 'view'
      }),
      buildTouchpoint('direct-latest', '2026-04-29T00:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        clickIdType: null,
        clickIdValue: null,
        isDirect: true
      })
    ],
    northbeamReferenceByModel: {
      first_touch: winnerTakeAll('click-earliest', '100.00', 3, 2, 1, 'mixed'),
      last_touch: winnerTakeAll('direct-latest', '100.00', 3, 2, 1, 'mixed', { isDirect: true }),
      last_non_direct: winnerTakeAll('view-middle', '100.00', 3, 2, 1, 'mixed', {
        directSuppressionApplied: true
      }),
      linear: linearSplit(
        [
          weightedCredit('click-earliest', '33.34', 1 / 3, true),
          weightedCredit('view-middle', '33.33', 1 / 3, false),
          weightedCredit('direct-latest', '33.33', 1 / 3, false, { isDirect: true })
        ],
        3,
        2,
        1,
        'mixed'
      ),
      clicks_only: winnerTakeAll('click-earliest', '100.00', 3, 2, 1, '28d_click', {
        directSuppressionApplied: true
      }),
      hinted_fallback_only: noCredit('blocked_by_deterministic', 3, 2, 1, 'mixed', {
        deterministicBlockApplied: true
      })
    }
  },
  {
    fixtureId: 'click-id-only-journey',
    name: 'Click-id-only journeys expose channel normalization variance',
    cohort: 'click_id_only_missing_utms',
    orderId: 'order-click-id-only',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '90.00',
    touchpoints: [
      buildTouchpoint('search-click', '2026-04-23T00:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        content: null,
        term: null,
        clickIdType: 'gclid',
        clickIdValue: 'gclid:search-click',
        isDirect: false
      }),
      buildTouchpoint('direct-return', '2026-04-29T00:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        clickIdType: null,
        clickIdValue: null,
        isDirect: true
      })
    ],
    northbeamReferenceByModel: {
      first_touch: winnerTakeAll('search-click', '90.00', 2, 2, 0, '28d_click'),
      last_touch: winnerTakeAll('direct-return', '90.00', 2, 2, 0, '28d_click', { isDirect: true }),
      last_non_direct: winnerTakeAll('search-click', '90.00', 2, 2, 0, '28d_click', {
        directSuppressionApplied: true
      }),
      linear: linearSplit(
        [
          weightedCredit('search-click', '45.00', 0.5, true),
          weightedCredit('direct-return', '45.00', 0.5, false, { isDirect: true })
        ],
        2,
        2,
        0,
        '28d_click'
      ),
      clicks_only: winnerTakeAll('search-click', '90.00', 2, 2, 0, '28d_click', {
        directSuppressionApplied: true
      }),
      hinted_fallback_only: noCredit('blocked_by_deterministic', 2, 2, 0, '28d_click', {
        deterministicBlockApplied: true
      })
    }
  },
  {
    fixtureId: 'identity-journey-fallback',
    name: 'Identity-journey fallback differs from Northbeam modeled recovery',
    cohort: 'identity_journey_fallback',
    orderId: 'order-identity-fallback',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '150.00',
    touchpoints: [
      buildTouchpoint('affiliate-early', '2026-04-15T00:00:00.000Z', {
        source: 'impact',
        medium: 'affiliate',
        campaign: 'publisher-a'
      }),
      buildTouchpoint('meta-late', '2026-04-28T00:00:00.000Z', {
        source: 'meta',
        medium: 'paid_social',
        campaign: 'retargeting'
      })
    ],
    northbeamReferenceByModel: {
      first_touch: winnerTakeAll('affiliate-early', '150.00', 2, 2, 0, '28d_click', {
        source: 'impact',
        medium: 'affiliate'
      }),
      last_touch: winnerTakeAll('meta-late', '150.00', 2, 2, 0, '28d_click', {
        source: 'meta',
        medium: 'paid_social'
      }),
      last_non_direct: winnerTakeAll('meta-late', '150.00', 2, 2, 0, '28d_click', {
        directSuppressionApplied: true,
        source: 'meta',
        medium: 'paid_social'
      }),
      linear: linearSplit(
        [
          weightedCredit('affiliate-early', '75.00', 0.5, true, {
            source: 'impact',
            medium: 'affiliate'
          }),
          weightedCredit('meta-late', '75.00', 0.5, false, {
            source: 'meta',
            medium: 'paid_social'
          })
        ],
        2,
        2,
        0,
        '28d_click'
      ),
      clicks_only: winnerTakeAll('meta-late', '150.00', 2, 2, 0, '28d_click', {
        directSuppressionApplied: true,
        source: 'meta',
        medium: 'paid_social'
      }),
      hinted_fallback_only: noCredit('blocked_by_deterministic', 2, 2, 0, '28d_click', {
        deterministicBlockApplied: true
      })
    }
  },
  {
    fixtureId: 'shopify-hint-fallback',
    name: 'Shopify hint fallback remains analyst-visible and non-parity',
    cohort: 'shopify_hint_fallback',
    orderId: 'order-shopify-hint',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '80.00',
    touchpoints: [
      buildTouchpoint('hint-meta', '2026-04-27T00:00:00.000Z', {
        sessionId: null,
        source: 'meta',
        medium: 'paid_social',
        campaign: 'retargeting',
        clickIdType: null,
        clickIdValue: null,
        evidenceSource: 'shopify_marketing_hint',
        attributionReason: 'synthetic_shopify_marketing_hint',
        isSynthetic: true,
        isForced: true
      })
    ],
    northbeamReferenceByModel: {
      first_touch: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      last_touch: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      last_non_direct: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      linear: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      clicks_only: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      hinted_fallback_only: winnerTakeAll('hint-meta', '80.00', 1, 1, 0, '28d_click', {
        source: 'meta',
        medium: 'paid_social'
      })
    }
  },
  {
    fixtureId: 'same-timestamp-tie',
    name: 'Same-timestamp ties emphasize evidence precedence differences',
    cohort: 'same_timestamp_tie',
    orderId: 'order-same-timestamp',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '110.00',
    touchpoints: [
      buildTouchpoint('google-checkout', '2026-04-26T00:00:00.000Z', {
        source: 'google',
        medium: 'cpc',
        campaign: 'brand-search',
        evidenceSource: 'checkout_token'
      }),
      buildTouchpoint('meta-landing', '2026-04-26T00:00:00.000Z', {
        source: 'meta',
        medium: 'paid_social',
        campaign: 'prospecting',
        evidenceSource: 'landing_session_id'
      })
    ],
    northbeamReferenceByModel: {
      first_touch: winnerTakeAll('meta-landing', '110.00', 2, 2, 0, '28d_click', {
        source: 'meta',
        medium: 'paid_social'
      }),
      last_touch: winnerTakeAll('meta-landing', '110.00', 2, 2, 0, '28d_click', {
        source: 'meta',
        medium: 'paid_social'
      }),
      last_non_direct: winnerTakeAll('meta-landing', '110.00', 2, 2, 0, '28d_click', {
        directSuppressionApplied: true,
        source: 'meta',
        medium: 'paid_social'
      }),
      linear: linearSplit(
        [
          weightedCredit('meta-landing', '55.00', 0.5, true, {
            source: 'meta',
            medium: 'paid_social'
          }),
          weightedCredit('google-checkout', '55.00', 0.5, false, {
            source: 'google',
            medium: 'cpc'
          })
        ],
        2,
        2,
        0,
        '28d_click'
      ),
      clicks_only: winnerTakeAll('meta-landing', '110.00', 2, 2, 0, '28d_click', {
        directSuppressionApplied: true,
        source: 'meta',
        medium: 'paid_social'
      }),
      hinted_fallback_only: noCredit('blocked_by_deterministic', 2, 2, 0, '28d_click', {
        deterministicBlockApplied: true
      })
    }
  },
  {
    fixtureId: 'no-eligible-touches',
    name: 'Orders outside the lookback remain unattributed',
    cohort: 'no_eligible_touches',
    orderId: 'order-no-eligible',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '130.00',
    touchpoints: [
      buildTouchpoint('expired-click', '2026-03-10T00:00:00.000Z'),
      buildTouchpoint('expired-view', '2026-04-20T00:00:00.000Z', {
        clickIdType: null,
        clickIdValue: null,
        engagementType: 'view'
      })
    ],
    northbeamReferenceByModel: {
      first_touch: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      last_touch: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      last_non_direct: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      linear: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      clicks_only: noCredit('no_eligible_touches', 0, 0, 0, '28d_click'),
      hinted_fallback_only: noCredit('unattributed', 0, 0, 0, '28d_click')
    }
  }
];
