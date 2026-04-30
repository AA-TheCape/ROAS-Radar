import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ATTRIBUTION_MODELS,
  executeAttributionModels,
  type AttributionTouchpoint
} from '../src/modules/attribution/engine.js';

function buildTouchpoint(
  sessionId: string,
  occurredAt: string,
  overrides: Partial<AttributionTouchpoint> = {}
): AttributionTouchpoint {
  return {
    touchpointId: `tp:${sessionId}:${occurredAt}`,
    sessionId,
    occurredAt: new Date(occurredAt),
    source: 'google',
    medium: 'cpc',
    campaign: sessionId,
    content: null,
    term: null,
    clickIdType: 'gclid',
    clickIdValue: `gclid:${sessionId}`,
    attributionReason: 'matched_by_customer_identity',
    evidenceSource: 'customer_identity',
    engagementType: 'click',
    isDirect: false,
    isForced: false,
    isSynthetic: false,
    ...overrides
  };
}

function execute(
  touchpoints: AttributionTouchpoint[],
  attributionModels: Array<(typeof ATTRIBUTION_MODELS)[number]> = ATTRIBUTION_MODELS
) {
  return executeAttributionModels(touchpoints, {
    orderRevenue: '100.00',
    orderOccurredAt: new Date('2026-04-30T00:00:00.000Z'),
    attributionModels
  });
}

test('first_touch and last_touch keep explicit deterministic winner selection', () => {
  const result = execute([
    buildTouchpoint('session-a', '2026-04-10T00:00:00.000Z'),
    buildTouchpoint('session-b', '2026-04-12T00:00:00.000Z'),
    buildTouchpoint('session-c', '2026-04-15T00:00:00.000Z')
  ], ['first_touch', 'last_touch']);

  assert.deepEqual(
    result.creditsByModel.first_touch.map((credit) => credit.revenueCredit),
    ['100.00', '0.00', '0.00']
  );
  assert.deepEqual(
    result.creditsByModel.last_touch.map((credit) => credit.revenueCredit),
    ['0.00', '0.00', '100.00']
  );
  assert.equal(result.summariesByModel.first_touch.winnerSelectionRule, 'first_touch');
  assert.equal(result.summariesByModel.last_touch.winnerSelectionRule, 'last_touch');
});

test('last_non_direct suppresses direct revisits while last_touch does not', () => {
  const result = execute([
    buildTouchpoint('session-a', '2026-04-10T00:00:00.000Z'),
    buildTouchpoint('session-b', '2026-04-20T00:00:00.000Z', {
      source: null,
      medium: null,
      campaign: null,
      clickIdType: null,
      clickIdValue: null,
      isDirect: true
    })
  ], ['last_touch', 'last_non_direct']);

  assert.deepEqual(
    result.creditsByModel.last_touch.map((credit) => credit.revenueCredit),
    ['0.00', '100.00']
  );
  assert.deepEqual(
    result.creditsByModel.last_non_direct.map((credit) => credit.revenueCredit),
    ['100.00', '0.00']
  );
  assert.equal(result.summariesByModel.last_non_direct.directSuppressionApplied, true);
});

test('linear splits revenue evenly and preserves cents deterministically', () => {
  const result = execute([
    buildTouchpoint('session-a', '2026-04-10T00:00:00.000Z'),
    buildTouchpoint('session-b', '2026-04-12T00:00:00.000Z'),
    buildTouchpoint('session-c', '2026-04-15T00:00:00.000Z')
  ], ['linear']);

  assert.deepEqual(
    result.creditsByModel.linear.map((credit) => credit.revenueCredit),
    ['33.34', '33.33', '33.33']
  );
  assert.equal(result.summariesByModel.linear.totalCreditWeight, 1);
});

test('clicks_only excludes view-only candidates even when they are newer', () => {
  const result = execute([
    buildTouchpoint('session-click', '2026-04-05T00:00:00.000Z'),
    buildTouchpoint('session-view', '2026-04-29T00:00:00.000Z', {
      clickIdType: null,
      clickIdValue: null,
      engagementType: 'view',
      evidenceSource: 'customer_identity'
    })
  ], ['clicks_only']);

  assert.deepEqual(
    result.creditsByModel.clicks_only.map((credit) => credit.revenueCredit),
    ['100.00', '0.00']
  );
  assert.equal(result.summariesByModel.clicks_only.lookbackRuleApplied, '28d_click');
});

test('hinted_fallback_only is blocked when deterministic evidence is present', () => {
  const result = execute([
    buildTouchpoint('session-deterministic', '2026-04-10T00:00:00.000Z', {
      evidenceSource: 'landing_session_id'
    }),
    buildTouchpoint('session-hint', '2026-04-12T00:00:00.000Z', {
      touchpointId: 'hint-1',
      sessionId: null,
      source: 'meta',
      medium: 'paid_social',
      campaign: 'retargeting',
      clickIdType: null,
      clickIdValue: null,
      evidenceSource: 'shopify_marketing_hint',
      isSynthetic: true,
      isForced: true
    })
  ], ['hinted_fallback_only']);

  assert.deepEqual(
    result.creditsByModel.hinted_fallback_only.map((credit) => credit.revenueCredit),
    ['0.00', '0.00']
  );
  assert.equal(result.summariesByModel.hinted_fallback_only.allocationStatus, 'blocked_by_deterministic');
  assert.equal(result.summariesByModel.hinted_fallback_only.deterministicBlockApplied, true);
});

test('hinted_fallback_only attributes only qualifying synthetic hint rows when deterministic evidence is absent', () => {
  const result = execute([
    buildTouchpoint('session-hint-a', '2026-04-11T00:00:00.000Z', {
      touchpointId: 'hint-a',
      sessionId: null,
      source: 'meta',
      medium: 'paid_social',
      campaign: 'prospecting',
      clickIdType: null,
      clickIdValue: null,
      evidenceSource: 'shopify_marketing_hint',
      isSynthetic: true,
      isForced: true
    }),
    buildTouchpoint('session-hint-b', '2026-04-12T00:00:00.000Z', {
      touchpointId: 'hint-b',
      sessionId: null,
      source: 'meta',
      medium: 'paid_social',
      campaign: 'retargeting',
      clickIdType: null,
      clickIdValue: null,
      evidenceSource: 'shopify_marketing_hint',
      isSynthetic: true,
      isForced: true
    })
  ], ['hinted_fallback_only']);

  assert.deepEqual(
    result.creditsByModel.hinted_fallback_only.map((credit) => credit.revenueCredit),
    ['0.00', '100.00']
  );
  assert.equal(result.summariesByModel.hinted_fallback_only.allocationStatus, 'attributed');
  assert.equal(result.summariesByModel.hinted_fallback_only.winnerTouchpointId, 'hint-b');
});

test('hinted_fallback_only ignores ambiguous synthetic hints and ga4 fallback rows when no qualifying hint exists', () => {
  const result = execute([
    buildTouchpoint('session-hint-ambiguous', '2026-04-12T00:00:00.000Z', {
      touchpointId: 'hint-ambiguous',
      sessionId: null,
      source: 'meta',
      medium: null,
      campaign: null,
      clickIdType: null,
      clickIdValue: null,
      evidenceSource: 'shopify_marketing_hint',
      isSynthetic: true,
      isForced: true
    }),
    buildTouchpoint('session-hint-direct', '2026-04-13T00:00:00.000Z', {
      touchpointId: 'hint-direct',
      sessionId: null,
      source: null,
      medium: null,
      campaign: null,
      content: null,
      term: null,
      clickIdType: null,
      clickIdValue: null,
      evidenceSource: 'shopify_marketing_hint',
      isDirect: true,
      isSynthetic: true,
      isForced: true
    }),
    buildTouchpoint('session-ga4', '2026-04-14T00:00:00.000Z', {
      touchpointId: 'ga4-1',
      sessionId: null,
      source: 'google',
      medium: 'cpc',
      campaign: 'ga4-retargeting',
      clickIdType: 'gclid',
      clickIdValue: 'ga4-click',
      evidenceSource: 'ga4_fallback',
      attributionReason: 'ga4_fallback_match',
      isSynthetic: true,
      isForced: false
    })
  ], ['hinted_fallback_only']);

  assert.deepEqual(
    result.creditsByModel.hinted_fallback_only.map((credit) => credit.revenueCredit),
    []
  );
  assert.equal(result.summariesByModel.hinted_fallback_only.allocationStatus, 'unattributed');
  assert.equal(result.summariesByModel.hinted_fallback_only.touchpointCountConsidered, 0);
});

test('core v1 models do not spill fallback evidence into deterministic attribution pools', () => {
  const result = execute([
    buildTouchpoint('session-hint', '2026-04-11T00:00:00.000Z', {
      touchpointId: 'hint-qualifying',
      sessionId: null,
      source: 'meta',
      medium: 'paid_social',
      campaign: 'prospecting',
      clickIdType: null,
      clickIdValue: null,
      evidenceSource: 'shopify_marketing_hint',
      isSynthetic: true,
      isForced: true
    }),
    buildTouchpoint('session-ga4', '2026-04-12T00:00:00.000Z', {
      touchpointId: 'ga4-qualifying',
      sessionId: null,
      source: 'google',
      medium: 'cpc',
      campaign: 'brand',
      clickIdType: 'gclid',
      clickIdValue: 'ga4-click',
      evidenceSource: 'ga4_fallback',
      attributionReason: 'ga4_fallback_match',
      isSynthetic: true,
      isForced: false
    })
  ], ['first_touch', 'last_touch', 'linear', 'clicks_only', 'hinted_fallback_only']);

  assert.equal(result.summariesByModel.first_touch.allocationStatus, 'no_eligible_touches');
  assert.equal(result.summariesByModel.last_touch.allocationStatus, 'no_eligible_touches');
  assert.equal(result.summariesByModel.linear.allocationStatus, 'no_eligible_touches');
  assert.equal(result.summariesByModel.clicks_only.allocationStatus, 'no_eligible_touches');
  assert.equal(result.summariesByModel.hinted_fallback_only.allocationStatus, 'attributed');
  assert.equal(result.summariesByModel.hinted_fallback_only.winnerTouchpointId, 'hint-qualifying');
});

test('model execution can target a single model or multiple models in one batch', () => {
  const touchpoints = [
    buildTouchpoint('session-a', '2026-04-10T00:00:00.000Z'),
    buildTouchpoint('session-b', '2026-04-12T00:00:00.000Z')
  ];

  const singleModelRun = execute(touchpoints, ['linear']);
  const multiModelRun = execute(touchpoints, ['first_touch', 'linear', 'last_touch']);

  assert.deepEqual(singleModelRun.models, ['linear']);
  assert.deepEqual(multiModelRun.models, ['first_touch', 'linear', 'last_touch']);
  assert.equal(singleModelRun.summariesByModel.linear.allocationStatus, 'attributed');
  assert.equal(multiModelRun.summariesByModel.first_touch.allocationStatus, 'attributed');
  assert.equal(multiModelRun.summariesByModel.last_touch.allocationStatus, 'attributed');
});

test('summaries report no_eligible_touches when nothing qualifies under the shared validation layer', () => {
  const result = executeAttributionModels([
    buildTouchpoint('session-old-click', '2026-03-01T00:00:00.000Z'),
    buildTouchpoint('session-old-view', '2026-04-01T00:00:00.000Z', {
      clickIdType: null,
      clickIdValue: null,
      engagementType: 'view'
    })
  ], {
    orderRevenue: '123.45',
    orderOccurredAt: new Date('2026-04-30T00:00:00.000Z'),
    attributionModels: ['first_touch', 'clicks_only', 'hinted_fallback_only']
  });

  assert.equal(result.summariesByModel.first_touch.allocationStatus, 'no_eligible_touches');
  assert.equal(result.summariesByModel.clicks_only.allocationStatus, 'no_eligible_touches');
  assert.equal(result.summariesByModel.hinted_fallback_only.allocationStatus, 'unattributed');
  assert.deepEqual(
    result.creditsByModel.first_touch.map((credit) => credit.revenueCredit),
    []
  );
});
