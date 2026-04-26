import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

import { env } from './config/env.js';
import { pool } from './db/pool.js';
import { runDailyDataQualityChecks } from './modules/data-quality/index.js';
import { logError, logInfo } from './observability/index.js';

async function run(): Promise<void> {
  const workerId = `data-quality-worker-${randomUUID()}`;
  let shouldStop = false;

  const requestStop = () => {
    shouldStop = true;
  };

  process.on('SIGINT', requestStop);
  process.on('SIGTERM', requestStop);

  logInfo('data_quality_worker_started', {
    workerId,
    service: process.env.K_SERVICE ?? 'roas-radar-data-quality',
    mode: env.DATA_QUALITY_CHECK_LOOP ? 'daemon' : 'oneshot'
  });

  do {
    const result = await runDailyDataQualityChecks();
    logInfo('data_quality_worker_iteration_completed', {
      workerId,
      service: process.env.K_SERVICE ?? 'roas-radar-data-quality',
      runDate: result.runDate,
      totals: result.totals
    });

    if (!env.DATA_QUALITY_CHECK_LOOP) {
      break;
    }

    if (shouldStop) {
      break;
    }

    await delay(env.DATA_QUALITY_CHECK_INTERVAL_MS);
  } while (!shouldStop);

  await pool.end();
}

run().catch(async (error) => {
  logError('data_quality_worker_crashed', error, {
    service: process.env.K_SERVICE ?? 'roas-radar-data-quality'
  });
  await pool.end().catch(() => undefined);
  process.exit(1);
});
