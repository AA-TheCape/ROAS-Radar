"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_crypto_1 = require("node:crypto");
const promises_1 = require("node:timers/promises");
const env_js_1 = require("./config/env.js");
const pool_js_1 = require("./db/pool.js");
const index_js_1 = require("./modules/google-ads/index.js");
async function run() {
    const workerId = `google-ads-worker-${(0, node_crypto_1.randomUUID)()}`;
    let shouldStop = false;
    const requestStop = () => {
        shouldStop = true;
    };
    process.on('SIGINT', requestStop);
    process.on('SIGTERM', requestStop);
    do {
        const result = await (0, index_js_1.processGoogleAdsSyncQueue)({
            workerId,
            limit: env_js_1.env.GOOGLE_ADS_SYNC_BATCH_SIZE,
            emitMetrics: true
        });
        if (!env_js_1.env.GOOGLE_ADS_WORKER_LOOP) {
            break;
        }
        if (shouldStop) {
            break;
        }
        if (result.claimedJobs === 0 && result.enqueuedJobs === 0) {
            await (0, promises_1.setTimeout)(env_js_1.env.GOOGLE_ADS_WORKER_POLL_INTERVAL_MS);
        }
    } while (!shouldStop);
    await pool_js_1.pool.end();
}
run().catch(async (error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    await pool_js_1.pool.end().catch(() => undefined);
    process.exit(1);
});
