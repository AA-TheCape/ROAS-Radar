import type {
  AttributionAllocationStatus,
  AttributionLookbackRule,
  AttributionModel,
  AttributionTouchpoint
} from '../../src/modules/attribution/engine.js';

type GoldenModelExpectation = {
  allocationStatus: AttributionAllocationStatus;
  winnerTouchpointId: string | null;
  revenueCredits?: Record<string, string>;
  totalRevenueCredited?: string;
  touchpointCountConsidered?: number;
  eligibleClickCount?: number;
  eligibleViewCount?: number;
  lookbackRuleApplied?: AttributionLookbackRule;
  directSuppressionApplied?: boolean;
  deterministicBlockApplied?: boolean;
};

export type AttributionEngineGoldenFixture = {
  name: string;
  orderOccurredAt: string;
  orderRevenue: string;
  touchpoints: AttributionTouchpoint[];
  expectedByModel: Partial<Record<AttributionModel, GoldenModelExpectation>>;
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

export const ATTRIBUTION_ENGINE_GOLDEN_FIXTURES: AttributionEngineGoldenFixture[] = [
  {
    name: 'all-direct deterministic journey keeps direct winners and equal linear split',
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
    expectedByModel: {
      first_touch: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'direct-early',
        revenueCredits: {
          'direct-early': '120.00',
          'direct-late': '0.00'
        },
        totalRevenueCredited: '120.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: false
      },
      last_touch: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'direct-late',
        revenueCredits: {
          'direct-early': '0.00',
          'direct-late': '120.00'
        },
        totalRevenueCredited: '120.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: false
      },
      last_non_direct: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'direct-late',
        revenueCredits: {
          'direct-early': '0.00',
          'direct-late': '120.00'
        },
        totalRevenueCredited: '120.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: false
      },
      linear: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'direct-early',
        revenueCredits: {
          'direct-early': '60.00',
          'direct-late': '60.00'
        },
        totalRevenueCredited: '120.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: false
      },
      clicks_only: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'direct-late',
        revenueCredits: {
          'direct-early': '0.00',
          'direct-late': '120.00'
        },
        totalRevenueCredited: '120.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: false
      },
      hinted_fallback_only: {
        allocationStatus: 'blocked_by_deterministic',
        winnerTouchpointId: null,
        revenueCredits: {
          'direct-early': '0.00',
          'direct-late': '0.00'
        },
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        deterministicBlockApplied: true
      }
    }
  },
  {
    name: 'mixed click view journey keeps 28d click, 7d view, and direct suppression semantics',
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
    expectedByModel: {
      first_touch: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'click-earliest',
        revenueCredits: {
          'click-earliest': '100.00',
          'view-middle': '0.00',
          'direct-latest': '0.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 2,
        eligibleViewCount: 1,
        lookbackRuleApplied: 'mixed',
        directSuppressionApplied: false
      },
      last_touch: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'direct-latest',
        revenueCredits: {
          'click-earliest': '0.00',
          'view-middle': '0.00',
          'direct-latest': '100.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 2,
        eligibleViewCount: 1,
        lookbackRuleApplied: 'mixed',
        directSuppressionApplied: false
      },
      last_non_direct: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'view-middle',
        revenueCredits: {
          'click-earliest': '0.00',
          'view-middle': '100.00',
          'direct-latest': '0.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 2,
        eligibleViewCount: 1,
        lookbackRuleApplied: 'mixed',
        directSuppressionApplied: true
      },
      linear: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'click-earliest',
        revenueCredits: {
          'click-earliest': '33.34',
          'view-middle': '33.33',
          'direct-latest': '33.33'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 2,
        eligibleViewCount: 1,
        lookbackRuleApplied: 'mixed',
        directSuppressionApplied: false
      },
      clicks_only: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'click-earliest',
        revenueCredits: {
          'click-earliest': '100.00',
          'view-middle': '0.00',
          'direct-latest': '0.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 2,
        eligibleViewCount: 1,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: true
      },
      hinted_fallback_only: {
        allocationStatus: 'blocked_by_deterministic',
        winnerTouchpointId: null,
        revenueCredits: {
          'click-earliest': '0.00',
          'view-middle': '0.00',
          'direct-latest': '0.00'
        },
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 2,
        eligibleViewCount: 1,
        lookbackRuleApplied: 'mixed',
        deterministicBlockApplied: true
      }
    }
  },
  {
    name: 'no eligible touches stays unattributed across core models',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '77.77',
    touchpoints: [
      buildTouchpoint('old-click', '2026-03-15T00:00:00.000Z'),
      buildTouchpoint('old-view', '2026-04-20T00:00:00.000Z', {
        clickIdType: null,
        clickIdValue: null,
        engagementType: 'view'
      }),
      buildTouchpoint('future-click', '2026-05-01T00:00:00.000Z')
    ],
    expectedByModel: {
      first_touch: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      last_touch: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      last_non_direct: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      linear: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      clicks_only: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      hinted_fallback_only: {
        allocationStatus: 'unattributed',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        deterministicBlockApplied: false
      }
    }
  },
  {
    name: 'equal-priority touches use click-id and lexical tie-breakers deterministically',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '100.00',
    touchpoints: [
      buildTouchpoint('priority-z', '2026-04-25T12:00:00.000Z', {
        clickIdType: null,
        clickIdValue: null
      }),
      buildTouchpoint('priority-b', '2026-04-25T12:00:00.000Z'),
      buildTouchpoint('priority-a', '2026-04-25T12:00:00.000Z')
    ],
    expectedByModel: {
      first_touch: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'priority-a',
        revenueCredits: {
          'priority-a': '100.00',
          'priority-b': '0.00',
          'priority-z': '0.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 3,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      last_touch: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'priority-a',
        revenueCredits: {
          'priority-a': '100.00',
          'priority-b': '0.00',
          'priority-z': '0.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 3,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      last_non_direct: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'priority-a',
        revenueCredits: {
          'priority-a': '100.00',
          'priority-b': '0.00',
          'priority-z': '0.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 3,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: true
      },
      linear: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'priority-a',
        revenueCredits: {
          'priority-a': '33.34',
          'priority-b': '33.33',
          'priority-z': '33.33'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 3,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      clicks_only: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'priority-a',
        revenueCredits: {
          'priority-a': '100.00',
          'priority-b': '0.00',
          'priority-z': '0.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 3,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: true
      },
      hinted_fallback_only: {
        allocationStatus: 'blocked_by_deterministic',
        winnerTouchpointId: null,
        revenueCredits: {
          'priority-a': '0.00',
          'priority-b': '0.00',
          'priority-z': '0.00'
        },
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 3,
        eligibleClickCount: 3,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        deterministicBlockApplied: true
      }
    }
  },
  {
    name: 'evidence precedence wins same-timestamp tie before weaker linkage',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '95.00',
    touchpoints: [
      buildTouchpoint('customer-identity-touch', '2026-04-24T12:00:00.000Z', {
        evidenceSource: 'customer_identity'
      }),
      buildTouchpoint('landing-session-touch', '2026-04-24T12:00:00.000Z', {
        evidenceSource: 'landing_session_id',
        attributionReason: 'matched_by_landing_session'
      })
    ],
    expectedByModel: {
      first_touch: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'landing-session-touch',
        revenueCredits: {
          'landing-session-touch': '95.00',
          'customer-identity-touch': '0.00'
        },
        totalRevenueCredited: '95.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      last_touch: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'landing-session-touch',
        revenueCredits: {
          'landing-session-touch': '95.00',
          'customer-identity-touch': '0.00'
        },
        totalRevenueCredited: '95.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      last_non_direct: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'landing-session-touch',
        revenueCredits: {
          'landing-session-touch': '95.00',
          'customer-identity-touch': '0.00'
        },
        totalRevenueCredited: '95.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: true
      },
      linear: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'landing-session-touch',
        revenueCredits: {
          'landing-session-touch': '47.50',
          'customer-identity-touch': '47.50'
        },
        totalRevenueCredited: '95.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      clicks_only: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'landing-session-touch',
        revenueCredits: {
          'landing-session-touch': '95.00',
          'customer-identity-touch': '0.00'
        },
        totalRevenueCredited: '95.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        directSuppressionApplied: true
      },
      hinted_fallback_only: {
        allocationStatus: 'blocked_by_deterministic',
        winnerTouchpointId: null,
        revenueCredits: {
          'landing-session-touch': '0.00',
          'customer-identity-touch': '0.00'
        },
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 2,
        eligibleClickCount: 2,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        deterministicBlockApplied: true
      }
    }
  },
  {
    name: 'hinted fallback isolates qualifying synthetic hints and ignores ga4 fallback',
    orderOccurredAt: '2026-04-30T00:00:00.000Z',
    orderRevenue: '100.00',
    touchpoints: [
      buildTouchpoint('hint-ambiguous', '2026-04-27T12:00:00.000Z', {
        sessionId: null,
        source: 'meta',
        medium: null,
        campaign: null,
        clickIdType: null,
        clickIdValue: null,
        evidenceSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_marketing_hint_only',
        isSynthetic: true,
        isForced: true
      }),
      buildTouchpoint('hint-qualified', '2026-04-28T12:00:00.000Z', {
        sessionId: null,
        source: 'meta',
        medium: 'paid_social',
        campaign: 'retargeting',
        clickIdType: null,
        clickIdValue: null,
        evidenceSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_marketing_hint_only',
        isSynthetic: true,
        isForced: true
      }),
      buildTouchpoint('ga4-fallback', '2026-04-29T12:00:00.000Z', {
        sessionId: null,
        source: 'google',
        medium: 'cpc',
        campaign: 'ga4-fallback',
        clickIdType: 'gclid',
        clickIdValue: 'ga4-click',
        evidenceSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        isSynthetic: true
      })
    ],
    expectedByModel: {
      first_touch: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      last_touch: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      last_non_direct: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      linear: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      clicks_only: {
        allocationStatus: 'no_eligible_touches',
        winnerTouchpointId: null,
        totalRevenueCredited: '0.00',
        touchpointCountConsidered: 0,
        eligibleClickCount: 0,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click'
      },
      hinted_fallback_only: {
        allocationStatus: 'attributed',
        winnerTouchpointId: 'hint-qualified',
        revenueCredits: {
          'hint-qualified': '100.00'
        },
        totalRevenueCredited: '100.00',
        touchpointCountConsidered: 1,
        eligibleClickCount: 1,
        eligibleViewCount: 0,
        lookbackRuleApplied: '28d_click',
        deterministicBlockApplied: false
      }
    }
  }
];
