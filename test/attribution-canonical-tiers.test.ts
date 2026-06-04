import assert from 'node:assert/strict';
import test from 'node:test';

import {
  computeCanonicalAttributionTiers,
  type CanonicalAttributionTierOutputs
} from '../src/modules/attribution/canonical-tiers.js';
import type { AttributionTouchpoint } from '../src/modules/attribution/engine.js';

function buildTouchpoint(
  touchpointId: string,
  occurredAt: string,
  overrides: Partial<AttributionTouchpoint> = {}
): AttributionTouchpoint {
  return {
    touchpointId,
    sessionId: `session-${touchpointId}`,
    occurredAt: new Date(occurredAt),
    source: 'google',
    medium: 'cpc',
    campaign: `campaign-${touchpointId}`,
    content: null,
    term: null,
    clickIdType: 'gclid',
    clickIdValue: `gclid-${touchpointId}`,
    attributionReason: 'matched_by_customer_identity',
    evidenceSource: 'customer_identity',
    engagementType: 'click',
    isDirect: false,
    isForced: false,
    isSynthetic: false,
    ...overrides
  };
}

function simplify(outputs: CanonicalAttributionTierOutputs) {
  return Object.fromEntries(
    Object.entries(outputs).map(([tier, output]) => [
      tier,
      {
        allocationStatus: output.allocationStatus,
        winnerTouchpointId: output.winnerTouchpointId,
        touchpointIds: output.touchpoints.map((touchpoint) => touchpoint.touchpointId),
        weights: output.touchpoints.map((touchpoint) => touchpoint.creditWeight),
        lookbackWindows: output.lookbackWindows
      }
    ])
  );
}

test('canonical attribution tiers are reproducible from the same inputs', () => {
  const input = {
    orderOccurredAt: new Date('2026-04-30T12:00:00.000Z'),
    touchpoints: [
      buildTouchpoint('strict-older', '2026-04-10T12:00:00.000Z', {
        evidenceSource: 'checkout_token',
        attributionReason: 'matched_by_checkout_token'
      }),
      buildTouchpoint('hint-newer', '2026-04-29T12:00:00.000Z', {
        sessionId: null,
        evidenceSource: 'shopify_marketing_hint',
        clickIdType: null,
        clickIdValue: null,
        isForced: true,
        isSynthetic: true,
        source: 'meta',
        medium: 'paid_social',
        campaign: 'retargeting'
      })
    ]
  };

  assert.deepEqual(simplify(computeCanonicalAttributionTiers(input)), simplify(computeCanonicalAttributionTiers(input)));
});

test('canonical attribution tiers enforce 30-day click and 7-day view defaults', () => {
  const outputs = computeCanonicalAttributionTiers({
    orderOccurredAt: new Date('2026-04-30T12:00:00.000Z'),
    touchpoints: [
      buildTouchpoint('click-30d', '2026-03-31T12:00:00.000Z'),
      buildTouchpoint('click-31d', '2026-03-30T11:59:59.000Z'),
      buildTouchpoint('view-7d', '2026-04-23T12:00:00.000Z', {
        clickIdType: null,
        clickIdValue: null,
        engagementType: 'view'
      }),
      buildTouchpoint('view-8d', '2026-04-22T11:59:59.000Z', {
        clickIdType: null,
        clickIdValue: null,
        engagementType: 'view'
      })
    ]
  });

  assert.deepEqual(outputs.strict.touchpoints.map((touchpoint) => touchpoint.touchpointId), ['click-30d', 'view-7d']);
  assert.deepEqual(outputs.strict.lookbackWindows, {
    clickWindowDays: 30,
    viewWindowDays: 7
  });
});

test('canonical attribution tiers allow explicit window overrides', () => {
  const outputs = computeCanonicalAttributionTiers({
    orderOccurredAt: new Date('2026-04-30T12:00:00.000Z'),
    lookbackWindows: {
      clickWindowDays: 10,
      viewWindowDays: 2
    },
    touchpoints: [
      buildTouchpoint('click-11d', '2026-04-19T12:00:00.000Z'),
      buildTouchpoint('click-9d', '2026-04-21T12:00:00.000Z'),
      buildTouchpoint('view-3d', '2026-04-27T12:00:00.000Z', {
        clickIdType: null,
        clickIdValue: null,
        engagementType: 'view'
      })
    ]
  });

  assert.deepEqual(outputs.strict.touchpoints.map((touchpoint) => touchpoint.touchpointId), ['click-9d']);
  assert.deepEqual(outputs.strict.lookbackWindows, {
    clickWindowDays: 10,
    viewWindowDays: 2
  });
});

test('canonical tier tie-breaking prefers strict evidence before assisted recency', () => {
  const outputs = computeCanonicalAttributionTiers({
    orderOccurredAt: new Date('2026-04-30T12:00:00.000Z'),
    touchpoints: [
      buildTouchpoint('hint-newer', '2026-04-29T12:00:00.000Z', {
        sessionId: null,
        evidenceSource: 'shopify_marketing_hint',
        clickIdType: null,
        clickIdValue: null,
        isForced: true,
        isSynthetic: true,
        source: 'meta',
        medium: 'paid_social',
        campaign: 'retargeting'
      }),
      buildTouchpoint('strict-older', '2026-04-01T12:00:00.000Z', {
        evidenceSource: 'landing_session_id',
        attributionReason: 'matched_by_landing_session'
      })
    ]
  });

  assert.equal(outputs.probabilistic_assisted_deterministic.winnerTouchpointId, 'strict-older');
  assert.equal(outputs.blended_deterministic_reporting.winnerTouchpointId, 'strict-older');
});

test('canonical tie-breaking is stable across same timestamp, evidence, and click presence', () => {
  const outputs = computeCanonicalAttributionTiers({
    orderOccurredAt: new Date('2026-04-30T12:00:00.000Z'),
    touchpoints: [
      buildTouchpoint('b-touch', '2026-04-29T12:00:00.000Z', {
        evidenceSource: 'cart_token'
      }),
      buildTouchpoint('a-touch', '2026-04-29T12:00:00.000Z', {
        evidenceSource: 'cart_token'
      })
    ]
  });

  assert.equal(outputs.strict.winnerTouchpointId, 'a-touch');
  assert.deepEqual(outputs.strict.touchpoints.map((touchpoint) => touchpoint.touchpointId), ['a-touch', 'b-touch']);
});
