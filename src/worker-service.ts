import { processShopifyOrderWritebackQueue, reconcileRecentShopifyOrderAttributes } from './modules/shopify/writeback.js';

async function run(): Promise<void> {
  const workerId = `attribution-worker-${randomUUID()}`;
  let shuttingDown = false;
  let running = false;
  let lastRunAt: string | null = null;
  let lastError: string | null = null;
  let nextShopifyReconciliationAt = 0;

  while (!shuttingDown) {
    running = true;

    try {
      if (env.SHOPIFY_RECONCILIATION_ENABLED && Date.now() >= nextShopifyReconciliationAt) {
        await reconcileRecentShopifyOrderAttributes({
          workerId,
          limit: env.SHOPIFY_RECONCILIATION_BATCH_SIZE,
          lookbackDays: env.SHOPIFY_RECONCILIATION_LOOKBACK_DAYS
        });
        nextShopifyReconciliationAt = Date.now() + env.SHOPIFY_RECONCILIATION_INTERVAL_MS;
      }

      await processAttributionQueue({ ... });
      await processShopifyOrderWritebackQueue({ ... });
      await emitAttributionBacklogSnapshot(workerId);
      lastRunAt = new Date().toISOString();
      lastError = null;
    } catch (error) {
      ...
    } finally {
      running = false;
    }

    ...
  }
}
