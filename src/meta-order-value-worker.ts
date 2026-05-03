import { env } from './config/env.js';
import { pool } from './db/pool.js';
import { runMetaAdsOrderValueSync } from './modules/meta-ads/order-value.js';
import { resolveMetaAdsRuntimeDescriptor } from './modules/meta-ads/runtime.js';
import { logError, logInfo } from './observability/index.js';

async function run(): Promise<void> {
  const runtime = resolveMetaAdsRuntimeDescriptor('order_value');
  const service = runtime.service;

  if (!env.META_ADS_ORDER_VALUE_SYNC_ENABLED) {
    logInfo('meta_ads_order_value_worker_skipped', {
      service,
      reason: 'disabled'
    });
    await pool.end();
    return;
  }

  logInfo('meta_ads_order_value_worker_started', {
    service,
    triggerSource: runtime.triggerSource,
    pipeline: runtime.pipeline,
    runtimeMode: runtime.runtimeMode,
    windowDays: env.META_ADS_ORDER_VALUE_WINDOW_DAYS
  });

  const result = await runMetaAdsOrderValueSync({
    triggerSource: runtime.triggerSource
  });

  logInfo('meta_ads_order_value_worker_completed', {
    service,
    pipeline: runtime.pipeline,
    runtimeMode: runtime.runtimeMode,
    ...result
  });

  await pool.end();
}

run().catch(async (error) => {
  logError('meta_ads_order_value_worker_failed', error, {
    service: process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-meta-order-value-sync'
  });
  await pool.end().catch(() => undefined);
  process.exit(1);
});
