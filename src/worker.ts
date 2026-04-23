import { processShopifyOrderWritebackQueue, reconcileRecentShopifyOrderAttributes } from './modules/shopify/writeback.js';

async function run(): Promise<void> {
  const workerId = `attribution-worker-${randomUUID()}`;
  let shouldStop = false;
  let nextShopifyReconciliationAt = 0;

  do {
    const shouldRunReconciliation =
      env.SHOPIFY_RECONCILIATION_ENABLED && Date.now() >= nextShopifyReconciliationAt;
    const reconciliationResult = shouldRunReconciliation
      ? await reconcileRecentShopifyOrderAttributes({
          workerId,
          limit: env.SHOPIFY_RECONCILIATION_BATCH_SIZE,
          lookbackDays: env.SHOPIFY_RECONCILIATION_LOOKBACK_DAYS
        })
      : null;

    if (shouldRunReconciliation) {
      nextShopifyReconciliationAt = Date.now() + env.SHOPIFY_RECONCILIATION_INTERVAL_MS;
    }

    const attributionResult = await processAttributionQueue({ ... });
    const writebackResult = await processShopifyOrderWritebackQueue({ ... });

    if (
      (!reconciliationResult || reconciliationResult.requeuedOrders === 0) &&
      attributionResult.claimedJobs === 0 &&
      attributionResult.staleJobsEnqueued === 0 &&
      writebackResult.claimedJobs === 0 &&
      writebackResult.staleJobsEnqueued === 0
    ) {
      await delay(env.ATTRIBUTION_WORKER_POLL_INTERVAL_MS);
    }
  } while (!shouldStop);
}
