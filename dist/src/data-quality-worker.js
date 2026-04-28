"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_crypto_1 = require("node:crypto");
const promises_1 = require("node:timers/promises");
const env_js_1 = require("./config/env.js");
const pool_js_1 = require("./db/pool.js");
const index_js_1 = require("./modules/data-quality/index.js");
const index_js_2 = require("./observability/index.js");
async function run() {
    const workerId = `data-quality-worker-${(0, node_crypto_1.randomUUID)()}`;
    let shouldStop = false;
    const requestStop = () => {
        shouldStop = true;
    };
    process.on('SIGINT', requestStop);
    process.on('SIGTERM', requestStop);
    (0, index_js_2.logInfo)('data_quality_worker_started', {
        workerId,
        service: process.env.K_SERVICE ?? 'roas-radar-data-quality',
        mode: env_js_1.env.DATA_QUALITY_CHECK_LOOP ? 'daemon' : 'oneshot'
    });
    do {
        const result = await (0, index_js_1.runDailyDataQualityChecks)();
        (0, index_js_2.logInfo)('data_quality_worker_iteration_completed', {
            workerId,
            service: process.env.K_SERVICE ?? 'roas-radar-data-quality',
            runDate: result.runDate,
            totals: result.totals
        });
        if (!env_js_1.env.DATA_QUALITY_CHECK_LOOP) {
            break;
        }
        if (shouldStop) {
            break;
        }
        await (0, promises_1.setTimeout)(env_js_1.env.DATA_QUALITY_CHECK_INTERVAL_MS);
    } while (!shouldStop);
    await pool_js_1.pool.end();
}
run().catch(async (error) => {
    (0, index_js_2.logError)('data_quality_worker_crashed', error, {
        service: process.env.K_SERVICE ?? 'roas-radar-data-quality'
    });
    await pool_js_1.pool.end().catch(() => undefined);
    process.exit(1);
});
