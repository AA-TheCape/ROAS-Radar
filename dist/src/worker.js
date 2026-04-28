"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_crypto_1 = require("node:crypto");
const promises_1 = require("node:timers/promises");
const env_js_1 = require("./config/env.js");
const pool_js_1 = require("./db/pool.js");
const index_js_1 = require("./modules/attribution/index.js");
const backfill_jobs_js_1 = require("./modules/attribution/backfill-jobs.js");
const ga4_bigquery_config_js_1 = require("./modules/attribution/ga4-bigquery-config.js");
const index_js_2 = require("./observability/index.js");
async function run() {
    (0, ga4_bigquery_config_js_1.assertGa4BigQueryIngestionConfig)();
    const workerId = `attribution-worker-${(0, node_crypto_1.randomUUID)()}`;
    let shouldStop = false;
    const requestStop = () => {
        shouldStop = true;
    };
    process.on('SIGINT', requestStop);
    process.on('SIGTERM', requestStop);
    (0, index_js_2.logInfo)('attribution_worker_started', {
        workerId,
        mode: env_js_1.env.ATTRIBUTION_WORKER_LOOP ? 'daemon' : 'oneshot'
    });
    do {
        const attributionResult = await (0, index_js_1.processAttributionQueue)({
            workerId,
            limit: env_js_1.env.ATTRIBUTION_JOB_BATCH_SIZE,
            staleScanLimit: env_js_1.env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE,
            emitMetrics: true
        });
        const backfillResult = await (0, backfill_jobs_js_1.processOrderAttributionBackfillRuns)({
            workerId
        });
        if (!env_js_1.env.ATTRIBUTION_WORKER_LOOP) {
            break;
        }
        if (shouldStop) {
            break;
        }
        if (attributionResult.claimedJobs === 0 &&
            attributionResult.staleJobsEnqueued === 0 &&
            backfillResult.claimedRuns === 0) {
            await (0, promises_1.setTimeout)(env_js_1.env.ATTRIBUTION_WORKER_POLL_INTERVAL_MS);
        }
    } while (!shouldStop);
    await pool_js_1.pool.end();
}
run().catch(async (error) => {
    (0, index_js_2.logError)('attribution_worker_crashed', error, {
        service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker'
    });
    await pool_js_1.pool.end().catch(() => undefined);
    process.exit(1);
});
