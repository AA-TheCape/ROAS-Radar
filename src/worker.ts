import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

import { env } from './config/env.js';
import { pool } from './db/pool.js';
import { processAttributionQueue } from './modules/attribution/index.js';
import { processOrderAttributionBackfillRuns } from './modules/attribution/backfill-jobs.js';
import { logError, logInfo } from './observability/index.js';

async function run(): Promise<void> {
  const workerId = `attribution-worker-${randomUUID()}`;
  let shouldStop = false;

  const requestStop = () => {
    shouldStop = true;
  };

  process.on('SIGINT', requestStop);
  process.on('SIGTERM', requestStop);

  logInfo('attribution_worker_started', {
    workerId,
    mode: env.ATTRIBUTION_WORKER_LOOP ? 'daemon' : 'oneshot'
  });

  do {
    const attributionResult = await processAttributionQueue({
      workerId,
      limit: env.ATTRIBUTION_JOB_BATCH_SIZE,
      staleScanLimit: env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE,
      emitMetrics: true
    });
    const backfillResult = await processOrderAttributionBackfillRuns({
      workerId
    });

    if (!env.ATTRIBUTION_WORKER_LOOP) {
      break;
    }

    if (shouldStop) {
      break;
    }

    if (
      attributionResult.claimedJobs === 0 &&
      attributionResult.staleJobsEnqueued === 0 &&
      backfillResult.claimedRuns === 0
    ) {
      await delay(env.ATTRIBUTION_WORKER_POLL_INTERVAL_MS);
    }
  } while (!shouldStop);

  await pool.end();
}

run().catch(async (error) => {
  logError('attribution_worker_crashed', error, {
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker'
  });
  await pool.end().catch(() => undefined);
  process.exit(1);
});
