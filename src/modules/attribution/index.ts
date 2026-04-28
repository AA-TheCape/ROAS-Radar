// Existing file retained; changed regions shown below.

import {
  buildJourney,
  getGa4FallbackRolloutMode,
  persistGa4FallbackShadowComparison,
  type Ga4FallbackRolloutMode
} from './ga4-rollout.js';

async function resolveAttributionJourneys(
  client: PoolClient,
  order: OrderRow
): Promise<{ appliedJourney: ResolvedJourney; shadowJourney: ResolvedJourney; rolloutMode: Ga4FallbackRolloutMode }> {
  const orderOccurredAt = resolveOrderOccurredAt(order);
  const touchpoints = dedupeDeterministicCandidates(await collectDeterministicCandidates(client, order));
  const deterministicWinner = selectLastNonDirectWinner(touchpoints);
  const shopifyHintWinner = deterministicWinner ? null : resolveShopifyHintFallback(order);
  const preGa4Winner = deterministicWinner ?? shopifyHintWinner;
  const ga4Winner = preGa4Winner ? null : await resolveGa4Fallback(client, order, orderOccurredAt);
  const rolloutMode = getGa4FallbackRolloutMode();
  const shadowWinner = preGa4Winner ?? ga4Winner;
  const appliedWinner = rolloutMode === 'on' ? shadowWinner : preGa4Winner;
  const appliedConfidenceScore = confidenceScoreForWinner(appliedWinner);
  const shadowConfidenceScore = confidenceScoreForWinner(shadowWinner);

  return {
    appliedJourney: buildJourney(
      appliedWinner && touchpoints.length === 0 ? [appliedWinner] : touchpoints,
      appliedWinner,
      appliedConfidenceScore,
      confidenceLabelForScore(appliedConfidenceScore)
    ),
    shadowJourney: buildJourney(
      shadowWinner && touchpoints.length === 0 ? [shadowWinner] : touchpoints,
      shadowWinner,
      shadowConfidenceScore,
      confidenceLabelForScore(shadowConfidenceScore)
    ),
    rolloutMode
  };
}

async function processClaimedJob(client: PoolClient, job: AttributionJob, workerId: string): Promise<void> {
  const order = await fetchOrder(client, job.shopify_order_id);
  // ... unchanged not-found handling ...

  const { appliedJourney, shadowJourney, rolloutMode } = await resolveAttributionJourneys(client, order);
  await persistAttribution(client, order, appliedJourney);
  await persistGa4FallbackShadowComparison(client, {
    shopifyOrderId: order.shopify_order_id,
    orderOccurredAt: resolveOrderOccurredAt(order),
    orderRevenue: order.total_price,
    rolloutMode,
    currentJourney: appliedJourney,
    shadowJourney
  });
  emitAttributionResolverOutcomeLog(order, appliedJourney);

  const metricDate = formatDateInTimezone(resolveOrderOccurredAt(order), await getReportingTimezone(client));
  await refreshDailyReportingMetrics(client, [metricDate]);

  // ... unchanged job completion update ...

  process.stdout.write(
    `${JSON.stringify({
      severity: 'INFO',
      event: 'attribution_job_processed',
      message: 'attribution_job_processed',
      timestamp: new Date().toISOString(),
      workerId,
      correlationId: buildAttributionCorrelationId(job.shopify_order_id),
      shopifyOrderId: job.shopify_order_id,
      confidenceScore: appliedJourney.confidenceScore,
      touchpointCount: appliedJourney.touchpoints.length,
      attributionReason: primaryCreditReason(appliedJourney),
      ga4FallbackRolloutMode: rolloutMode,
      shadowMatchSource: shadowJourney.winner?.matchSource ?? 'unattributed'
    })}\n`
  );
}
