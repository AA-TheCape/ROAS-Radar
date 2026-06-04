import { randomUUID } from "node:crypto";

import { pool } from "./db/pool.js";
import { runAttributionQaRetentionJob } from "./modules/attribution/qa-retention.js";

async function run(): Promise<void> {
	const workerId = `attribution-qa-retention-${randomUUID()}`;
	const result = await runAttributionQaRetentionJob();

	process.stdout.write(
		`${JSON.stringify({
			event: "attribution_qa_retention_run",
			workerId,
			...result,
		})}\n`,
	);

	await pool.end();
}

run().catch(async (error) => {
	process.stderr.write(
		`${error instanceof Error ? error.stack : String(error)}\n`,
	);
	await pool.end().catch(() => undefined);
	process.exit(1);
});
