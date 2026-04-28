test('summarizeResolverOutcome reports unattributed and non-direct winners deterministically', () => {
  const unattributed = __observabilityTestUtils.summarizeResolverOutcome({
    touchpoints: [],
    winner: null,
    tier: 'unattributed',
    attributionReason: 'missing_order_timestamp',
    confidenceScore: 0,
    pipeline: 'realtime_queue',
    shopifyOrderId: 'order-1',
    normalizationFailures: [
      {
        scope: 'order',
        reason: 'missing_order_timestamp',
        sourceKey: null
      }
    ]
  });

  assert.equal(unattributed.resolverOutcome, 'unattributed');
  assert.equal(unattributed.attributionTier, 'unattributed');
  assert.equal(unattributed.resolverFallthroughDepth, 3);
  assert.equal(unattributed.fallthroughStage, 'fell_through_to_unattributed');
  assert.equal(unattributed.firstNormalizationFailureReason, 'missing_order_timestamp');

  const resolved = __observabilityTestUtils.summarizeResolverOutcome({
    touchpoints: [{ occurredAt: '2026-04-01T10:00:00.000Z' }],
    winner: {
      isDirect: false,
      ingestionSource: 'checkout_token',
      sessionId: 'session-123'
    },
    tier: 'deterministic_first_party',
    attributionReason: 'matched_by_checkout_token',
    confidenceScore: 1,
    pipeline: 'order_backfill',
    shopifyOrderId: 'order-2',
    orderOccurredAtUtc: '2026-04-02T10:00:00.000Z',
    normalizationFailures: []
  });

  assert.equal(resolved.resolverOutcome, 'non_direct_winner');
  assert.equal(resolved.attributionTier, 'deterministic_first_party');
  assert.equal(resolved.resolverFallthroughDepth, 0);
  assert.equal(resolved.hasWinningSessionId, true);
});

test('emitAttributionResolverOutcomeLog emits tier and fallthrough metrics for dashboards and alerts', () => {
  const { entries } = captureStructuredLogs(() => {
    __observabilityTestUtils.emitAttributionResolverOutcomeLog({
      shopifyOrderId: 'shopify-order-123',
      orderOccurredAtUtc: new Date('2026-04-25T10:00:00.000Z'),
      tier: 'deterministic_shopify_hint',
      attributionReason: 'matched_by_shopify_landing_page_gclid',
      confidenceScore: 0.55,
      pipeline: 'order_backfill',
      touchpoints: [{ id: 'synthetic-touchpoint' }],
      winner: {
        isDirect: false,
        ingestionSource: 'shopify_marketing_hint',
        sessionId: null
      },
      normalizationFailures: [
        {
          scope: 'shopify_hint',
          reason: 'missing_first_party_candidate_timestamp',
          sourceKey: 'utm:gclid'
        }
      ]
    });
  });

  assert.equal(entries.length, 1);
  assert.equal(entries[0].event, 'attribution_resolver_outcome');
  assert.equal(entries[0].attributionTier, 'deterministic_shopify_hint');
  assert.equal(entries[0].resolverFallthroughDepth, 1);
  assert.equal(entries[0].fallthroughStage, 'fell_through_to_shopify_hint');
  assert.equal(entries[0].pipeline, 'order_backfill');
  assert.equal(entries[0].winningIngestionSource, 'shopify_marketing_hint');
  assert.equal(entries[0].hasWinningSessionId, false);
  assert.equal(entries[0].normalizationFailureCount, 1);
  assert.equal(entries[0].firstNormalizationFailureScope, 'shopify_hint');
});
