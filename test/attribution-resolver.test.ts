import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

async function getTestUtils() {
  const attributionModule = await import('../src/modules/attribution/index.js');
  return attributionModule.__attributionTestUtils;
}

async function getResolverModule() {
  return import('../src/modules/attribution/resolver.js');
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
    platformReportedMeta: [
      buildTierCandidate('meta-eligible', '2026-04-08T11:30:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.8,
        isSynthetic: true,
        metaSignalId: 'meta-eligible',
        metaMatchBasis: 'fbclid',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
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
  assert.equal(resolved.attributionReason, 'shopify_hint_derived');
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
    platformReportedMeta: [
      buildTierCandidate('meta-parallel-only', '2026-04-08T11:30:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.4,
        isSynthetic: true,
        metaSignalId: 'meta-parallel-only',
        metaMatchBasis: 'fbclid',
        metaEligibilityOutcome: 'eligible_parallel_only',
        isClickThrough: true,
        isViewThrough: false
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
  assert.equal(resolved.attributionReason, 'ga4_fallback_match');
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
  assert.equal(first.attributionReason, 'unattributed');
});

test('isDirectTouchpoint only treats fully empty marketing metadata as direct', async () => {
  const resolverModule = await getResolverModule();

  assert.equal(
    resolverModule.isDirectTouchpoint({
      source: null,
      medium: null,
      campaign: null,
      content: null,
      term: null,
      clickIdValue: null
    }),
    true
  );

  assert.equal(
    resolverModule.isDirectTouchpoint({
      source: null,
      medium: null,
      campaign: null,
      content: null,
      term: null,
      clickIdValue: 'fbclid-1'
    }),
    false
  );
});

test('confidenceScoreForWinner covers every tier source and null winner', async () => {
  const testUtils = await getTestUtils();

  assert.equal(testUtils.confidenceScoreForWinner(null), 0);
  assert.equal(testUtils.confidenceScoreForWinner({ ingestionSource: 'landing_session_id' }), 1);
  assert.equal(testUtils.confidenceScoreForWinner({ ingestionSource: 'checkout_token' }), 1);
  assert.equal(testUtils.confidenceScoreForWinner({ ingestionSource: 'cart_token' }), 0.9);
  assert.equal(testUtils.confidenceScoreForWinner({ ingestionSource: 'customer_identity' }), 0.6);
  assert.equal(testUtils.confidenceScoreForWinner({ ingestionSource: 'shopify_marketing_hint' }), 0.55);
  assert.equal(testUtils.confidenceScoreForWinner({ ingestionSource: 'meta_platform_reported' }), 0.5);
  assert.equal(testUtils.confidenceScoreForWinner({ ingestionSource: 'ga4_fallback' }), 0.35);
});

test('resolveAttributionTier promotes eligible Meta only after deterministic first-party and Shopify hint fail', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    platformReportedMeta: [
      buildTierCandidate('meta-eligible', '2026-04-08T11:30:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.72,
        source: 'meta',
        medium: 'paid_social',
        campaign: 'meta-retargeting',
        clickIdType: 'fbclid',
        clickIdValue: 'fbclid-1',
        isSynthetic: true,
        metaSignalId: 'meta-eligible',
        metaMatchBasis: 'fbclid',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      })
    ],
    ga4Fallback: [
      buildTierCandidate('ga4-eligible', '2026-04-08T10:30:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(resolved.tier, 'platform_reported_meta');
  assert.equal(resolved.winner?.ingestionSource, 'meta_platform_reported');
  assert.equal(resolved.winner?.clickIdType, 'fbclid');
  assert.equal(resolved.touchpoints.length, 1);
  assert.equal(resolved.attributionReason, 'meta_platform_reported_match');
});

test('resolveAttributionTier excludes Meta candidates that are outside the canonical eligibility guardrails', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    platformReportedMeta: [
      buildTierCandidate('meta-future', '2026-04-08T12:00:01.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.8,
        isSynthetic: true,
        metaSignalId: 'meta-future',
        metaMatchBasis: 'fbclid',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      }),
      buildTierCandidate('meta-parallel-only', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.4,
        isSynthetic: true,
        metaSignalId: 'meta-parallel-only',
        metaMatchBasis: 'fbclid',
        metaEligibilityOutcome: 'eligible_parallel_only',
        isClickThrough: true,
        isViewThrough: false
      })
    ],
    ga4Fallback: [
      buildTierCandidate('ga4-eligible', '2026-04-08T10:30:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(resolved.tier, 'ga4_fallback');
  assert.equal(resolved.winner?.ingestionSource, 'ga4_fallback');
});

test('dedupe ignores deterministic candidates without a session id and keeps timeline ordering stable', async () => {
  const testUtils = await getTestUtils();

  const deduped = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-b', '2026-04-03T10:00:00.000Z'),
    buildTouchpoint('session-missing', '2026-04-02T10:00:00.000Z', {
      sessionId: null,
      sourceTouchEventId: 'missing-event'
    }),
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z')
  ]);

  assert.deepEqual(
    deduped.map((touchpoint) => touchpoint.sessionId),
    ['session-a', 'session-b']
  );
});

test('resolveAttributionTier returns unattributed when the order timestamp is missing', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: null,
    deterministicFirstParty: [
      buildTierCandidate('session-first-party', '2026-04-07T10:00:00.000Z')
    ],
    shopifyHint: [
      buildTierCandidate('shopify-hint', '2026-04-07T11:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      })
    ],
    ga4Fallback: [
      buildTierCandidate('ga4-hint', '2026-04-07T11:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(resolved.tier, 'unattributed');
  assert.equal(resolved.winner, null);
  assert.deepEqual(resolved.touchpoints, []);
  assert.equal(resolved.confidenceScore, 0);
  assert.equal(resolved.attributionReason, 'missing_order_timestamp');
});

test('resolveAttributionTier dedupes Shopify and GA4 candidates by stable source key before selecting a winner', async () => {
  const testUtils = await getTestUtils();

  const shopifyResolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [
      buildTierCandidate('shopify-dup', '2026-04-07T09:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'shopify_marketing_hint',
        clickIdType: null,
        clickIdValue: null,
        confidenceScore: 0.4,
        isSynthetic: true
      }),
      buildTierCandidate('shopify-dup', '2026-04-07T10:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'shopify_marketing_hint',
        clickIdType: 'fbclid',
        clickIdValue: 'fbclid-1',
        confidenceScore: 0.55,
        isSynthetic: true
      })
    ],
    ga4Fallback: []
  });

  assert.equal(shopifyResolved.touchpoints.length, 1);
  assert.equal(shopifyResolved.winner?.clickIdValue, 'fbclid-1');

  const ga4Resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    ga4Fallback: [
      buildTierCandidate('ga4-dup', '2026-04-08T09:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        clickIdType: null,
        clickIdValue: null,
        confidenceScore: 0.25,
        isSynthetic: true
      }),
      buildTierCandidate('ga4-dup', '2026-04-08T10:00:00.000Z', {
        sessionId: null,
        sourceTouchEventId: null,
        ingestionSource: 'ga4_fallback',
        clickIdType: 'gclid',
        clickIdValue: 'gclid-1',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(ga4Resolved.touchpoints.length, 1);
  assert.equal(ga4Resolved.winner?.clickIdValue, 'gclid-1');
});

test('resolveAttributionTier carries normalization failure reasons into unattributed outcomes when order time exists', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    ga4Fallback: [],
    normalizationFailures: [
      {
        scope: 'ga4_fallback',
        reason: 'invalid_candidate_timestamp',
        sourceKey: 'ga4-bad'
      }
    ]
  });

  assert.equal(resolved.tier, 'unattributed');
  assert.equal(resolved.attributionReason, 'invalid_candidate_timestamp');
});

test('resolveAttributionTier ignores ineligible higher-tier timestamps and still evaluates lower tiers', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [
      buildTierCandidate('deterministic-future', '2026-04-08T12:00:01.000Z', {
        ingestionSource: 'landing_session_id',
        attributionReason: 'matched_by_landing_session',
        confidenceScore: 1
      }),
      buildTierCandidate('deterministic-invalid', '2026-04-07T12:00:00.000Z', {
        occurredAtUtc: new Date('invalid'),
        ingestionSource: 'checkout_token',
        attributionReason: 'matched_by_checkout_token',
        confidenceScore: 1
      })
    ],
    shopifyHint: [
      buildTierCandidate('shopify-eligible', '2026-04-07T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      })
    ],
    ga4Fallback: [
      buildTierCandidate('ga4-eligible', '2026-04-07T10:00:00.000Z', {
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
  assert.equal(resolved.winner?.sourceTouchEventId, 'shopify-eligible-event');
});

test('resolveAttributionTier treats the 7-day Shopify hint lookback as inclusive and excludes older or future hints', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [
      buildTierCandidate('shopify-too-old', '2026-04-01T11:59:59.999Z', {
        sessionId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      }),
      buildTierCandidate('shopify-boundary', '2026-04-01T12:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      }),
      buildTierCandidate('shopify-future', '2026-04-08T12:00:00.001Z', {
        sessionId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      })
    ],
    ga4Fallback: []
  });

  assert.equal(resolved.tier, 'deterministic_shopify_hint');
  assert.deepEqual(
    resolved.touchpoints.map((touchpoint) => touchpoint.sourceTouchEventId),
    ['shopify-boundary-event']
  );
});

test('resolveAttributionTier breaks Shopify hint ties by click id, then recency, then source key', async () => {
  const testUtils = await getTestUtils();

  const winnerFromClickId = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [
      buildTierCandidate('shopify-newer-utm', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      }),
      buildTierCandidate('shopify-older-click-id', '2026-04-08T10:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        clickIdType: 'fbclid',
        clickIdValue: 'fbclid-1',
        isSynthetic: true
      })
    ],
    ga4Fallback: []
  });

  assert.equal(winnerFromClickId.winner?.sourceTouchEventId, 'shopify-older-click-id-event');

  const winnerFromLexicalKey = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [
      buildTierCandidate('shopify-b', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      }),
      buildTierCandidate('shopify-a', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'shopify_marketing_hint',
        attributionReason: 'shopify_hint_derived',
        confidenceScore: 0.55,
        isSynthetic: true
      })
    ],
    ga4Fallback: []
  });

  assert.equal(winnerFromLexicalKey.winner?.sourceTouchEventId, 'shopify-a-event');
});

test('resolveAttributionTier treats the 7-day GA4 lookback as inclusive and excludes older or future fallback candidates', async () => {
  const testUtils = await getTestUtils();

  const resolved = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    ga4Fallback: [
      buildTierCandidate('ga4-too-old', '2026-04-01T11:59:59.999Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      }),
      buildTierCandidate('ga4-boundary', '2026-04-01T12:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      }),
      buildTierCandidate('ga4-future', '2026-04-08T12:00:00.001Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(resolved.tier, 'ga4_fallback');
  assert.deepEqual(
    resolved.touchpoints.map((touchpoint) => touchpoint.sourceTouchEventId),
    ['ga4-boundary-event']
  );
});

test('resolveAttributionTier breaks GA4 ties by recency, then click id, then source key', async () => {
  const testUtils = await getTestUtils();

  const winnerFromClickId = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    ga4Fallback: [
      buildTierCandidate('ga4-a', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      }),
      buildTierCandidate('ga4-b', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        clickIdType: 'gclid',
        clickIdValue: 'gclid-1',
        isSynthetic: true
      })
    ]
  });

  assert.equal(winnerFromClickId.winner?.sourceTouchEventId, 'ga4-b-event');

  const winnerFromRecency = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    ga4Fallback: [
      buildTierCandidate('ga4-older-click-id', '2026-04-08T10:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        clickIdType: 'gclid',
        clickIdValue: 'gclid-1',
        isSynthetic: true
      }),
      buildTierCandidate('ga4-newer-no-click', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(winnerFromRecency.winner?.sourceTouchEventId, 'ga4-newer-no-click-event');

  const winnerFromLexicalKey = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    ga4Fallback: [
      buildTierCandidate('ga4-b', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      }),
      buildTierCandidate('ga4-a', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'ga4_fallback',
        attributionReason: 'ga4_fallback_match',
        confidenceScore: 0.35,
        isSynthetic: true
      })
    ]
  });

  assert.equal(winnerFromLexicalKey.winner?.sourceTouchEventId, 'ga4-a-event');
});

test('resolveAttributionTier breaks Meta ties by recency, match basis, click-through, confidence, then signal id', async () => {
  const testUtils = await getTestUtils();

  const winnerFromRecency = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    platformReportedMeta: [
      buildTierCandidate('meta-older', '2026-04-08T10:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.8,
        isSynthetic: true,
        metaSignalId: 'meta-older',
        metaMatchBasis: 'fbclid',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      }),
      buildTierCandidate('meta-newer', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.6,
        isSynthetic: true,
        metaSignalId: 'meta-newer',
        metaMatchBasis: 'conversion_api_event_id',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: false,
        isViewThrough: true
      })
    ],
    ga4Fallback: []
  });

  assert.equal(winnerFromRecency.winner?.sourceTouchEventId, 'meta-newer-event');

  const winnerFromMatchBasis = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    platformReportedMeta: [
      buildTierCandidate('meta-fbp', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.8,
        isSynthetic: true,
        metaSignalId: 'meta-fbp',
        metaMatchBasis: 'fbp',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      }),
      buildTierCandidate('meta-fbclid', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.6,
        isSynthetic: true,
        metaSignalId: 'meta-fbclid',
        metaMatchBasis: 'fbclid',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: false,
        isViewThrough: true
      })
    ],
    ga4Fallback: []
  });

  assert.equal(winnerFromMatchBasis.winner?.sourceTouchEventId, 'meta-fbclid-event');

  const winnerFromClickThrough = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    platformReportedMeta: [
      buildTierCandidate('meta-view-through', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.8,
        isSynthetic: true,
        metaSignalId: 'meta-view-through',
        metaMatchBasis: 'external_id',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: false,
        isViewThrough: true
      }),
      buildTierCandidate('meta-click-through', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.6,
        isSynthetic: true,
        metaSignalId: 'meta-click-through',
        metaMatchBasis: 'external_id',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      })
    ],
    ga4Fallback: []
  });

  assert.equal(winnerFromClickThrough.winner?.sourceTouchEventId, 'meta-click-through-event');

  const winnerFromConfidence = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    platformReportedMeta: [
      buildTierCandidate('meta-lower-confidence', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.6,
        isSynthetic: true,
        metaSignalId: 'meta-lower-confidence',
        metaMatchBasis: 'external_id',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      }),
      buildTierCandidate('meta-higher-confidence', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.8,
        isSynthetic: true,
        metaSignalId: 'meta-higher-confidence',
        metaMatchBasis: 'external_id',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      })
    ],
    ga4Fallback: []
  });

  assert.equal(winnerFromConfidence.winner?.sourceTouchEventId, 'meta-higher-confidence-event');

  const winnerFromSignalId = testUtils.resolveAttributionTier({
    orderOccurredAtUtc: new Date('2026-04-08T12:00:00.000Z'),
    deterministicFirstParty: [],
    shopifyHint: [],
    platformReportedMeta: [
      buildTierCandidate('meta-b', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.8,
        isSynthetic: true,
        metaSignalId: 'meta-b',
        metaMatchBasis: 'external_id',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      }),
      buildTierCandidate('meta-a', '2026-04-08T11:00:00.000Z', {
        sessionId: null,
        ingestionSource: 'meta_platform_reported',
        attributionReason: 'meta_platform_reported_match',
        confidenceScore: 0.8,
        isSynthetic: true,
        metaSignalId: 'meta-a',
        metaMatchBasis: 'external_id',
        metaEligibilityOutcome: 'eligible_canonical',
        isClickThrough: true,
        isViewThrough: false
      })
    ],
    ga4Fallback: []
  });

  assert.equal(winnerFromSignalId.winner?.sourceTouchEventId, 'meta-a-event');
});
