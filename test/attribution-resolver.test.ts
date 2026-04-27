import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

async function getTestUtils() {
  const attributionModule = await import('../src/modules/attribution/index.js');
  return attributionModule.__attributionTestUtils;
}

type TestUtils = Awaited<ReturnType<typeof getTestUtils>>;
type TestTouchpoint = Parameters<TestUtils['dedupeDeterministicCandidates']>[0][number];
type TierResolverInput = Parameters<TestUtils['resolveAttributionTier']>[0];
type TierResolverCandidate = TierResolverInput['deterministicFirstParty'][number];

function buildTouchpoint(
  sessionId: string,
  occurredAt: string,
  overrides: Partial<TestTouchpoint> = {}
): TestTouchpoint {
  return {
    sessionId,
    sourceTouchEventId: `${sessionId}-event`,
    occurredAt: new Date(occurredAt),
    source: 'google',
    medium: 'cpc',
    campaign: `campaign-${sessionId}`,
    content: null,
    term: null,
    clickIdType: null,
    clickIdValue: null,
    attributionReason: 'matched_by_customer_identity',
    ingestionSource: 'customer_identity',
    isDirect: false,
    isForced: false,
    ...overrides
  };
}

function buildTierCandidate(
  sourceKey: string,
  occurredAtUtc: string,
  overrides: Partial<TierResolverCandidate> = {}
): TierResolverCandidate {
  return {
    sourceKey,
    sessionId: sourceKey,
    sourceTouchEventId: `${sourceKey}-event`,
    ingestionSource: 'customer_identity',
    occurredAtUtc: new Date(occurredAtUtc),
    source: 'google',
    medium: 'cpc',
    campaign: `campaign-${sourceKey}`,
    content: null,
    term: null,
    clickIdType: null,
    clickIdValue: null,
    attributionReason: 'matched_by_customer_identity',
    confidenceScore: 0.6,
    isDirect: false,
    isSynthetic: false,
    ...overrides
  };
}

test('last non-direct winner ignores a later direct revisit', async () => {
  const testUtils = await getTestUtils();
  const touchpoints = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-paid', '2026-04-01T10:00:00.000Z', {
      clickIdType: 'gclid',
      clickIdValue: 'gclid-123'
    }),
    buildTouchpoint('session-direct', '2026-04-03T09:00:00.000Z', {
      source: null,
      medium: null,
      campaign: null,
      isDirect: true
    })
  ]);

  const winner = testUtils.selectLastNonDirectWinner(touchpoints);

  assert.equal(winner?.sessionId, 'session-paid');
});

test('direct-only timelines resolve to the latest direct touch', async () => {
  const testUtils = await getTestUtils();
  const touchpoints = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z', {
      source: null,
      medium: null,
      campaign: null,
      isDirect: true
    }),
    buildTouchpoint('session-b', '2026-04-02T11:00:00.000Z', {
      source: null,
      medium: null,
      campaign: null,
      isDirect: true
    })
  ]);

  const winner = testUtils.selectLastNonDirectWinner(touchpoints);

  assert.equal(winner?.sessionId, 'session-b');
});

test('winner selection returns null when there are no deterministic candidates', async () => {
  const testUtils = await getTestUtils();

  assert.equal(testUtils.selectLastNonDirectWinner([]), null);
});

test('dedupe keeps the strongest evidence when the same session is visible multiple ways', async () => {
  const testUtils = await getTestUtils();
  const deduped = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z', {
      ingestionSource: 'customer_identity',
      attributionReason: 'matched_by_customer_identity'
    }),
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z', {
      ingestionSource: 'landing_session_id',
      attributionReason: 'matched_by_landing_session'
    })
  ]);

  assert.equal(deduped.length, 1);
  assert.equal(deduped[0].ingestionSource, 'landing_session_id');
  assert.equal(deduped[0].attributionReason, 'matched_by_landing_session');
});

test('dedupe breaks same-session ties by later occurredAt, then click id presence', async () => {
  const testUtils = await getTestUtils();
  const winnerFromLaterOccurredAt = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z'),
    buildTouchpoint('session-a', '2026-04-01T11:00:00.000Z')
  ]);

  assert.equal(winnerFromLaterOccurredAt.length, 1);
  assert.equal(winnerFromLaterOccurredAt[0].occurredAt.toISOString(), '2026-04-01T11:00:00.000Z');

  const winnerFromClickId = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z'),
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z', {
      clickIdType: 'gclid',
      clickIdValue: 'gclid-123'
    })
  ]);

  assert.equal(winnerFromClickId.length, 1);
  assert.equal(winnerFromClickId[0].clickIdType, 'gclid');
  assert.equal(winnerFromClickId[0].clickIdValue, 'gclid-123');
});

test('same-timestamp winner selection prefers stronger source, then click id, then lexical session id', async () => {
  const testUtils = await getTestUtils();
  const winnerFromSourcePrecedence = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-cart', '2026-04-05T15:00:00.000Z', {
        ingestionSource: 'cart_token',
        attributionReason: 'matched_by_cart_token'
      }),
      buildTouchpoint('session-checkout', '2026-04-05T15:00:00.000Z', {
        ingestionSource: 'checkout_token',
        attributionReason: 'matched_by_checkout_token'
      })
    ])
  );

  assert.equal(winnerFromSourcePrecedence?.sessionId, 'session-checkout');

  const winnerFromClickId = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-a', '2026-04-05T15:00:00.000Z'),
      buildTouchpoint('session-b', '2026-04-05T15:00:00.000Z', {
        clickIdType: 'fbclid',
        clickIdValue: 'fbclid-123'
      })
    ])
  );

  assert.equal(winnerFromClickId?.sessionId, 'session-b');

  const winnerFromLexicalSession = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-b', '2026-04-05T15:00:00.000Z'),
      buildTouchpoint('session-a', '2026-04-05T15:00:00.000Z')
    ])
  );

  assert.equal(winnerFromLexicalSession?.sessionId, 'session-a');
});

test('click-id-only touches remain non-direct and beat later direct revisits', async () => {
  const testUtils = await getTestUtils();
  const winner = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-paid', '2026-04-02T14:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        clickIdType: 'fbclid',
        clickIdValue: 'fbclid-abc',
        isDirect: false
      }),
      buildTouchpoint('session-direct', '2026-04-03T11:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        clickIdType: null,
        clickIdValue: null,
        isDirect: true
      })
    ])
  );

  assert.equal(winner?.sessionId, 'session-paid');
});

test('latest non-direct wins even when the newer touch has UTMs and no click id', async () => {
  const testUtils = await getTestUtils();
  const winner = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-older-click-id', '2026-04-01T10:00:00.000Z', {
        clickIdType: 'gclid',
        clickIdValue: 'gclid-123'
      }),
      buildTouchpoint('session-newer-utm', '2026-04-04T08:00:00.000Z', {
        clickIdType: null,
        clickIdValue: null,
        source: 'google',
        medium: 'cpc',
        campaign: 'spring-search',
        isDirect: false
      })
    ])
  );

  assert.equal(winner?.sessionId, 'session-newer-utm');
});

test('resolveAttributionTier prefers deterministic first-party over eligible Shopify hint and GA4 candidates', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [
      buildTierCandidate('session-first-party', '2026-04-07T10:00:00.000Z', {
        ingestionSource: 'checkout_token',
        attributionReason: 'matched_by_checkout_token',
        confidenceScore: 1
      })
    ],
    shopifyHint: [
      buildTierCandidate('shopify-hint', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      })
    ],
    ga4Fallback: [
      buildTierCandidate('ga4-candidate', '2026-04-08T09:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(resolved.tier, 'deterministic_first_party');
  assert.equal(resolved.winner?.sessionId, 'session-first-party');
  assert.equal(resolved.winner?.ingestionSource, 'checkout_token');
});

test('resolveAttributionTier only considers Shopify hints inside the 7-day lookback and before GA4', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [
      buildTierCandidate('shopify-too-old', '2026-03-31T11:59:59.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      }),
      buildTierCandidate('shopify-eligible', '2026-04-07T12:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        clickIdType: 'fbclid',
        clickIdValue: 'fbclid-1',
        isSynthetic: true
      })
    ],
    ga4Fallback: [
      buildTierCandidate('ga4-eligible', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(resolved.tier, 'deterministic_shopify_hint');
  assert.equal(resolved.winner?.ingestionSource, 'shopify_marketing_hint');
  assert.equal(resolved.winner?.clickIdType, 'fbclid');
  assert.equal(resolved.touchpoints.length, 1);
});

test('resolveAttributionTier falls back to GA4 only when higher tiers are missing or ineligible', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [
      buildTierCandidate('shopify-too-old', '2026-03-31T12:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      })
    ],
    ga4Fallback: [
      buildTierCandidate('ga4-too-old', '2026-03-31T11:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      }),
      buildTierCandidate('ga4-eligible', '2026-04-08T10:30:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        clickIdType: 'gclid',
        clickIdValue: 'gclid-1',
        isSynthetic: true
      })
    ]
  });

  assert.equal(resolved.tier, 'ga4_fallback');
  assert.equal(resolved.winner?.ingestionSource, 'ga4_fallback');
  assert.equal(resolved.winner?.clickIdType, 'gclid');
  assert.equal(resolved.touchpoints.length, 1);
});

test('resolveAttributionTier is deterministic across repeated runs and returns unattributed when nothing qualifies', async () => {
  const testUtils = await getTestUtils();

  const input: TierResolverInput = {
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [
      buildTierCandidate('shopify-future', '2026-04-08T12:00:01.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      })
    ],
    ga4Fallback: [
      buildTierCandidate('ga4-too-old', '2026-03-31T11:59:59.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  };

  const first = testUtils.resolveAttributionTier(input);
  const second = testUtils.resolveAttributionTier(input);

  assert.deepEqual(
    {
      tier: first.tier,
      winner: first.winner?.sourceTouchEventId ?? null,
      confidenceScore: first.confidenceScore
    },
    {
      tier: second.tier,
      winner: second.winner?.sourceTouchEventId ?? null,
      confidenceScore: second.confidenceScore
    }
  );
  assert.equal(first.tier, 'unattributed');
  assert.equal(first.winner, null);
});
