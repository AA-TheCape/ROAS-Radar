import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { env } from './config/env.js';
import { pool } from './db/pool.js';
import { processAttributionQueue } from './modules/attribution/index.js';
async function run() {
    const workerId = `attribution-worker-${randomUUID()}`;
    let shouldStop = false;
    const requestStop = () => {
        shouldStop = true;
    };
    process.on('SIGINT', requestStop);
    process.on('SIGTERM', requestStop);
    do {
        const result = await processAttributionQueue({
            workerId,
            limit: env.ATTRIBUTION_JOB_BATCH_SIZE,
            staleScanLimit: env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE,
            emitMetrics: true
        });
        if (!env.ATTRIBUTION_WORKER_LOOP) {
            break;
        }
        if (shouldStop) {
            break;
        }
        if (result.claimedJobs === 0 && result.staleJobsEnqueued === 0) {
            await delay(env.ATTRIBUTION_WORKER_POLL_INTERVAL_MS);
        }
    } while (!shouldStop);
    await pool.end();
}
run().catch(async (error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    await pool.end().catch(() => undefined);
    process.exit(1);
});
