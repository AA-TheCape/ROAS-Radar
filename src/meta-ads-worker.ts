import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import { env } from "./config/env.js";
import { pool } from "./db/pool.js";
import { processMetaAdsSyncQueue } from "./modules/meta-ads/index.js";

async function run(): Promise<void> {
	const workerId = `meta-ads-worker-${randomUUID()}`;
	let shouldStop = false;

	const requestStop = () => {
		shouldStop = true;
	};

	process.on("SIGINT", requestStop);
	process.on("SIGTERM", requestStop);

	do {
		const result = await processMetaAdsSyncQueue({
			workerId,
			limit: env.META_ADS_SYNC_BATCH_SIZE,
			emitMetrics: true,
		});

		if (!env.META_ADS_WORKER_LOOP) {
			break;
		}

		if (shouldStop) {
			break;
		}

		if (result.claimedJobs === 0 && result.enqueuedJobs === 0) {
			await delay(env.META_ADS_WORKER_POLL_INTERVAL_MS);
		}
	} while (!shouldStop);

	await pool.end();
}

run().catch(async (error) => {
	process.stderr.write(
		`${error instanceof Error ? error.stack : String(error)}\n`,
	);
	await pool.end().catch(() => undefined);
	process.exit(1);
});
