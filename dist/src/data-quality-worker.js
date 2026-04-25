"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_crypto_1 = require("node:crypto");
const promises_1 = require("node:timers/promises");
const env_js_1 = require("./config/env.js");
const pool_js_1 = require("./db/pool.js");
const index_js_1 = require("./modules/data-quality/index.js");
async function run() {
    const workerId = `data-quality-worker-${(0, node_crypto_1.randomUUID)()}`;
    let shouldStop = false;
    const requestStop = () => {
        shouldStop = true;
    };
    process.on('SIGINT', requestStop);
    process.on('SIGTERM', requestStop);
    do {
        const result = await (0, index_js_1.runDailyDataQualityChecks)();
        process.stdout.write(`${JSON.stringify({
            event: 'data_quality_run',
            workerId,
            runDate: result.runDate,
            totals: result.totals
        })}\n`);
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
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    await pool_js_1.pool.end().catch(() => undefined);
    process.exit(1);
});
