import { logError, logInfo, summarizeResolverOutcome } from '../../observability/index.js';

const journey = await resolveAttributionJourney(client, order);
logInfo('attribution_resolver_outcome', {
  shopifyOrderId: job.shopify_order_id,
  ...summarizeResolverOutcome(journey)
});
await persistAttribution(client, order, journey);
