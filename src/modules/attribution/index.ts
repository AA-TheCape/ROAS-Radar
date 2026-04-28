import { emitAttributionResolverOutcomeLog, logError } from '../../observability/index.js';

async function persistAttribution(client: PoolClient, order: OrderRow, journey: ResolvedJourney): Promise<void> {
  // existing persistence work unchanged above

  await client.query(
    `
      UPDATE shopify_orders
      SET
        attribution_tier = $2,
        attribution_source = $3,
        attribution_matched_at = $4,
        attribution_reason = $5,
        attribution_snapshot = $6::jsonb,
        attribution_snapshot_updated_at = $4
      WHERE shopify_order_id = $1
    `,
    [
      order.shopify_order_id,
      orderAttributionAudit.tier,
      orderAttributionAudit.source,
      orderAttributionAudit.matchedAt,
      orderAttributionAudit.reason,
      JSON.stringify({
        tier: journey.tier,
        attributionReason: journey.attributionReason,
        orderOccurredAtUtc: journey.orderOccurredAtUtc?.toISOString() ?? null,
        normalizationFailures: journey.normalizationFailures,
        confidenceScore: journey.confidenceScore,
        winner: journey.winner ? serializeResolvedTouchpoint(journey.winner) : null,
        timeline: journey.touchpoints.map(serializeResolvedTouchpoint)
      })
    ]
  );

  emitAttributionResolverOutcomeLog({
    shopifyOrderId: order.shopify_order_id,
    orderOccurredAtUtc: journey.orderOccurredAtUtc,
    tier: journey.tier,
    attributionReason: journey.attributionReason,
    confidenceScore: journey.confidenceScore,
    pipeline: 'realtime_queue',
    touchpoints: journey.touchpoints,
    winner: journey.winner,
    normalizationFailures: journey.normalizationFailures
  });
}
