import { env } from "./config/env.js";
import { pool } from "./db/pool.js";
import { runMetaDeterministicSync } from "./modules/meta-ads/deterministic-events.js";
import { logError, logInfo } from "./observability/index.js";

async function run(): Promise<void> {
	const service =
		process.env.K_SERVICE ??
		process.env.K_JOB ??
		"roas-radar-meta-deterministic-sync";
	const triggerSource =
		process.env.K_JOB || process.env.K_JOB_EXECUTION
			? "cloud_run_job"
			: "manual";

	if (!env.META_ADS_DETERMINISTIC_SYNC_ENABLED) {
		logInfo("meta_ads_deterministic_worker_skipped", {
			service,
			reason: "disabled",
		});
		await pool.end();
		return;
	}

	logInfo("meta_ads_deterministic_worker_started", {
		service,
		triggerSource,
	});

	const result = await runMetaDeterministicSync({
		triggerSource,
	});

	logInfo("meta_ads_deterministic_worker_completed", {
		service,
		triggerSource,
		...result,
	});

	await pool.end();
}

run().catch(async (error) => {
	logError("meta_ads_deterministic_worker_failed", error, {
		service:
			process.env.K_SERVICE ??
			process.env.K_JOB ??
			"roas-radar-meta-deterministic-sync",
	});
	await pool.end().catch(() => undefined);
	process.exit(1);
});
