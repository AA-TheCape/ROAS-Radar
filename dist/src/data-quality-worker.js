import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { env } from './config/env.js';
import { pool } from './db/pool.js';
import { runDailyDataQualityChecks } from './modules/data-quality/index.js';
async function run() {
    const workerId = `data-quality-worker-${randomUUID()}`;
    let shouldStop = false;
    const requestStop = () => {
        shouldStop = true;
    };
    process.on('SIGINT', requestStop);
    process.on('SIGTERM', requestStop);
    do {
        const result = await runDailyDataQualityChecks();
        process.stdout.write(`${JSON.stringify({
            event: 'data_quality_run',
            workerId,
            runDate: result.runDate,
            totals: result.totals
        })}\n`);
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
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    await pool.end().catch(() => undefined);
    process.exit(1);
});
