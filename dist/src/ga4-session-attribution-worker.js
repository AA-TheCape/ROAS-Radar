"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.resolveGa4IngestionExecution = resolveGa4IngestionExecution;
const node_crypto_1 = require("node:crypto");
const node_url_1 = require("node:url");
const pool_js_1 = require("./db/pool.js");
const ga4_bigquery_executor_js_1 = require("./modules/attribution/ga4-bigquery-executor.js");
const ga4_bigquery_config_js_1 = require("./modules/attribution/ga4-bigquery-config.js");
const ga4_ingestion_jobs_js_1 = require("./modules/attribution/ga4-ingestion-jobs.js");
const index_js_1 = require("./observability/index.js");
function parseOptionalInteger(name) {
    const value = process.env[name]?.trim();
    if (!value) {
        return undefined;
    }
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid ${name} value: ${value}`);
    }
    return parsed;
}
function parseOptionalHour(name) {
    const value = process.env[name]?.trim();
    if (!value) {
        return undefined;
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        throw new Error(`Invalid ${name} value: ${value}`);
    }
    return new Date(Date.UTC(parsed.getUTCFullYear(), parsed.getUTCMonth(), parsed.getUTCDate(), parsed.getUTCHours())).toISOString();
}
function resolveGa4IngestionExecution(now) {
    const requestedBy = process.env.GA4_INGESTION_REQUESTED_BY?.trim() ||
        process.env.K_SERVICE?.trim() ||
        process.env.K_JOB?.trim() ||
        'cloud-run-scheduler';
    const workerId = process.env.GA4_INGESTION_WORKER_ID?.trim() ||
        process.env.K_JOB_EXECUTION?.trim() ||
        `ga4-session-attribution-${(0, node_crypto_1.randomUUID)()}`;
    const explicitStartHour = parseOptionalHour('GA4_INGESTION_START_HOUR');
    const explicitEndHour = parseOptionalHour('GA4_INGESTION_END_HOUR') ?? explicitStartHour;
    const explicitHourStarts = explicitStartHour && explicitEndHour
        ? (0, ga4_ingestion_jobs_js_1.listHourlyRange)(explicitStartHour, explicitEndHour)
        : undefined;
    return {
        requestedBy,
        workerId,
        explicitHourStarts,
        batchSize: parseOptionalInteger('GA4_INGESTION_BATCH_SIZE') ?? 24,
        maxRetries: parseOptionalInteger('GA4_INGESTION_MAX_RETRIES') ?? 5,
        initialBackoffSeconds: parseOptionalInteger('GA4_INGESTION_INITIAL_BACKOFF_SECONDS') ?? 30,
        maxBackoffSeconds: parseOptionalInteger('GA4_INGESTION_MAX_BACKOFF_SECONDS') ?? 1_800,
        staleLockMinutes: parseOptionalInteger('GA4_INGESTION_STALE_LOCK_MINUTES') ?? 30
    };
}
async function run() {
    const config = (0, ga4_bigquery_config_js_1.assertGa4BigQueryIngestionConfig)();
    if (!config.enabled) {
        throw new Error('GA4 BigQuery ingestion is disabled');
    }
    const execution = resolveGa4IngestionExecution(new Date());
    (0, index_js_1.logInfo)('ga4_session_attribution_worker_started', {
        workerId: execution.workerId,
        requestedBy: execution.requestedBy,
        explicitHours: execution.explicitHourStarts ?? [],
        service: process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-ga4-session-attribution'
    });
    const result = await (0, ga4_ingestion_jobs_js_1.processGa4SessionAttributionHourlyJobs)({
        requestedBy: execution.requestedBy,
        workerId: execution.workerId,
        config,
        executor: (0, ga4_bigquery_executor_js_1.createGa4BigQueryExecutor)(config.ga4.location),
        batchSize: execution.batchSize,
        maxRetries: execution.maxRetries,
        initialBackoffSeconds: execution.initialBackoffSeconds,
        maxBackoffSeconds: execution.maxBackoffSeconds,
        staleLockMinutes: execution.staleLockMinutes,
        explicitHourStarts: execution.explicitHourStarts
    });
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    await pool_js_1.pool.end();
}
if (process.argv[1] && import.meta.url === (0, node_url_1.pathToFileURL)(process.argv[1]).href) {
    run().catch(async (error) => {
        (0, index_js_1.logError)('ga4_session_attribution_worker_failed', error, {
            service: process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-ga4-session-attribution'
        });
        await pool_js_1.pool.end().catch(() => undefined);
        process.exit(1);
    });
}
