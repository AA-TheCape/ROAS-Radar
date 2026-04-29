import { pathToFileURL } from "node:url";
import { pool } from "./db/pool.js";
import { assertGa4BigQueryIngestionConfig } from "./modules/attribution/ga4-bigquery-config.js";
import { createGa4BigQueryExecutor } from "./modules/attribution/ga4-bigquery-executor.js";
import { resolveGa4IngestionExecution } from "./modules/attribution/ga4-ingestion-execution.js";
import { processGa4SessionAttributionHourlyJobs } from "./modules/attribution/ga4-ingestion-jobs.js";
import { logError, logInfo } from "./observability/index.js";
async function run() {
    const config = assertGa4BigQueryIngestionConfig();
    if (!config.enabled) {
        throw new Error("GA4 BigQuery ingestion is disabled");
    }
    const execution = resolveGa4IngestionExecution(new Date());
    logInfo("ga4_session_attribution_worker_started", {
        workerId: execution.workerId,
        requestedBy: execution.requestedBy,
        explicitHours: execution.explicitHourStarts ?? [],
        service: process.env.K_SERVICE ??
            process.env.K_JOB ??
            "roas-radar-ga4-session-attribution",
    });
    const result = await processGa4SessionAttributionHourlyJobs({
        requestedBy: execution.requestedBy,
        workerId: execution.workerId,
        config,
        executor: createGa4BigQueryExecutor(config.ga4.location),
        batchSize: execution.batchSize,
        maxRetries: execution.maxRetries,
        initialBackoffSeconds: execution.initialBackoffSeconds,
        maxBackoffSeconds: execution.maxBackoffSeconds,
        staleLockMinutes: execution.staleLockMinutes,
        explicitHourStarts: execution.explicitHourStarts,
    });
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    await pool.end();
}
if (process.argv[1] &&
    import.meta.url === pathToFileURL(process.argv[1]).href) {
    run().catch(async (error) => {
        logError("ga4_session_attribution_worker_failed", error, {
            service: process.env.K_SERVICE ??
                process.env.K_JOB ??
                "roas-radar-ga4-session-attribution",
        });
        await pool.end().catch(() => undefined);
        process.exit(1);
    });
}
