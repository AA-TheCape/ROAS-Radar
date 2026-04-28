import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { pool } from './db/pool.js';
import { createGa4BigQueryExecutor } from './modules/attribution/ga4-bigquery-executor.js';
import { assertGa4BigQueryIngestionConfig } from './modules/attribution/ga4-bigquery-config.js';
import { listHourlyRange, processGa4SessionAttributionHourlyJobs } from './modules/attribution/ga4-ingestion-jobs.js';
import { logError, logInfo } from './observability/index.js';
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
export function resolveGa4IngestionExecution(now) {
    const requestedBy = process.env.GA4_INGESTION_REQUESTED_BY?.trim() ||
        process.env.K_SERVICE?.trim() ||
        process.env.K_JOB?.trim() ||
        'cloud-run-scheduler';
    const workerId = process.env.GA4_INGESTION_WORKER_ID?.trim() ||
        process.env.K_JOB_EXECUTION?.trim() ||
        `ga4-session-attribution-${randomUUID()}`;
    const explicitStartHour = parseOptionalHour('GA4_INGESTION_START_HOUR');
    const explicitEndHour = parseOptionalHour('GA4_INGESTION_END_HOUR') ?? explicitStartHour;
    const explicitHourStarts = explicitStartHour && explicitEndHour
        ? listHourlyRange(explicitStartHour, explicitEndHour)
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
    const config = assertGa4BigQueryIngestionConfig();
    if (!config.enabled) {
        throw new Error('GA4 BigQuery ingestion is disabled');
    }
    const execution = resolveGa4IngestionExecution(new Date());
    logInfo('ga4_session_attribution_worker_started', {
        workerId: execution.workerId,
        requestedBy: execution.requestedBy,
        explicitHours: execution.explicitHourStarts ?? [],
        service: process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-ga4-session-attribution'
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
        explicitHourStarts: execution.explicitHourStarts
    });
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    await pool.end();
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    run().catch(async (error) => {
        logError('ga4_session_attribution_worker_failed', error, {
            service: process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-ga4-session-attribution'
        });
        await pool.end().catch(() => undefined);
        process.exit(1);
    });
}
