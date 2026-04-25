"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_crypto_1 = require("node:crypto");
const pool_js_1 = require("./db/pool.js");
const retention_js_1 = require("./modules/tracking/retention.js");
async function run() {
    const workerId = `session-attribution-retention-${(0, node_crypto_1.randomUUID)()}`;
    const result = await (0, retention_js_1.runSessionAttributionRetentionJob)();
    process.stdout.write(`${JSON.stringify({
        event: 'session_attribution_retention_run',
        workerId,
        ...result
    })}\n`);
    await pool_js_1.pool.end();
}
run().catch(async (error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    await pool_js_1.pool.end().catch(() => undefined);
    process.exit(1);
});
