import { emitAttributionResolverOutcomeLog, logError, logInfo } from '../../observability/index.js';

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
        winner: journey.winner
          ? {
              sessionId: journey.winner.sessionId,
              sourceTouchEventId: journey.winner.sourceTouchEventId,
              occurredAt: journey.winner.occurredAt.toISOString(),
              source: journey.winner.source,
              medium: journey.winner.medium,
              campaign: journey.winner.campaign,
              content: journey.winner.content,
              term: journey.winner.term,
              clickIdType: journey.winner.clickIdType,
              clickIdValue: journey.winner.clickIdValue,
              attributionReason: journey.winner.attributionReason,
              ingestionSource: journey.winner.ingestionSource,
              isDirect: journey.winner.isDirect
            }
          : null,
        timeline: journey.touchpoints.map((touchpoint) => ({
          sessionId: touchpoint.sessionId,
          sourceTouchEventId: touchpoint.sourceTouchEventId,
          occurredAt: touchpoint.occurredAt.toISOString(),
          source: touchpoint.source,
          medium: touchpoint.medium,
          campaign: touchpoint.campaign,
          content: touchpoint.content,
          term: touchpoint.term,
          clickIdType: touchpoint.clickIdType,
          clickIdValue: touchpoint.clickIdValue,
          attributionReason: touchpoint.attributionReason,
          ingestionSource: touchpoint.ingestionSource,
          isDirect: touchpoint.isDirect
        }))
      })
    ]
  );

  emitAttributionResolverOutcomeLog({
    shopifyOrderId: order.shopify_order_id,
    orderOccurredAtUtc: journey.orderOccurredAtUtc,
    tier: journey.tier,
    attributionReason: journey.attributionReason,
    confidenceScore: journey.confidenceScore,
    pipeline: 'order_backfill',
    touchpoints: journey.touchpoints,
    winner: journey.winner,
    normalizationFailures: journey.normalizationFailures
  });
}
