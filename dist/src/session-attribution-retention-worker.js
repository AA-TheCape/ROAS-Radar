import { randomUUID } from "node:crypto";
import { pool } from "./db/pool.js";
import { runSessionAttributionRetentionJob } from "./modules/tracking/retention.js";
async function run() {
    const workerId = `session-attribution-retention-${randomUUID()}`;
    const result = await runSessionAttributionRetentionJob();
    process.stdout.write(`${JSON.stringify({
        event: "session_attribution_retention_run",
        workerId,
        ...result,
    })}\n`);
    await pool.end();
}
run().catch(async (error) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    await pool.end().catch(() => undefined);
    process.exit(1);
});
