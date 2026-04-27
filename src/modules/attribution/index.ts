import { logError, logInfo, summarizeResolverOutcome } from '../../observability/index.js';

// ...existing code...

function buildAttributionCorrelationId(shopifyOrderId: string): string {
  return `attribution:${shopifyOrderId}`;
}

function emitAttributionResolverOutcomeLog(order: OrderRow, journey: ResolvedJourney): void {
  logInfo('attribution_resolver_outcome', {
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    correlationId: buildAttributionCorrelationId(order.shopify_order_id),
    shopifyOrderId: order.shopify_order_id,
    sourceName: normalizeNullableString(order.source_name),
    confidenceScore: journey.confidenceScore,
    confidenceLabel: journey.confidenceLabel,
    attributionReason: primaryCreditReason(journey),
    ...summarizeResolverOutcome({
      touchpoints: journey.touchpoints,
      winner: journey.winner,
      deterministicWinnerExists: Boolean(journey.winner?.ingestionSource),
      shopifyHintMatchExists: journey.winner?.matchSource === 'shopify_hint_fallback'
    })
  });
}

async function processClaimedJob(client: PoolClient, job: AttributionJob, workerId: string): Promise<void> {
  // ...existing fetch and persist logic...
  const journey = await resolveAttributionJourney(client, order);
  await persistAttribution(client, order, journey);
  emitAttributionResolverOutcomeLog(order, journey);

  process.stdout.write(
    `${JSON.stringify({
      severity: 'INFO',
      event: 'attribution_job_processed',
      message: 'attribution_job_processed',
      timestamp: new Date().toISOString(),
      workerId,
      correlationId: buildAttributionCorrelationId(job.shopify_order_id),
      shopifyOrderId: job.shopify_order_id,
      confidenceScore: journey.confidenceScore,
      touchpointCount: journey.touchpoints.length,
      attributionReason: primaryCreditReason(journey)
    })}\n`
  );
}
