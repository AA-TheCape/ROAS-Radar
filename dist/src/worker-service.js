"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_crypto_1 = require("node:crypto");
const node_http_1 = require("node:http");
const promises_1 = require("node:timers/promises");
const express_1 = __importDefault(require("express"));
const env_js_1 = require("./config/env.js");
const pool_js_1 = require("./db/pool.js");
const index_js_1 = require("./modules/attribution/index.js");
const backfill_jobs_js_1 = require("./modules/attribution/backfill-jobs.js");
const ga4_bigquery_config_js_1 = require("./modules/attribution/ga4-bigquery-config.js");
const index_js_2 = require("./observability/index.js");
async function emitAttributionBacklogSnapshot(workerId) {
    const result = await pool_js_1.pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE status IN ('pending', 'retry'))::text AS pending_jobs,
        COALESCE(
          MAX(
            EXTRACT(
              EPOCH FROM now() - available_at
            )
          ) FILTER (WHERE status IN ('pending', 'retry') AND available_at <= now()),
          0
        )::text AS oldest_job_age_seconds,
        COUNT(*) FILTER (
          WHERE status = 'processing'
            AND locked_at < now() - interval '15 minutes'
        )::text AS stale_processing_jobs
      FROM attribution_jobs
    `);
    const row = result.rows[0];
    if (!row) {
        return;
    }
    process.stdout.write(`${(0, index_js_2.buildAttributionBacklogLog)({
        workerId,
        pendingJobs: Number(row.pending_jobs),
        oldestJobAgeSeconds: Math.trunc(Number(row.oldest_job_age_seconds)),
        staleProcessingJobs: Number(row.stale_processing_jobs)
    })}\n`);
}
async function run() {
    (0, ga4_bigquery_config_js_1.assertGa4BigQueryIngestionConfig)();
    const workerId = `attribution-worker-${(0, node_crypto_1.randomUUID)()}`;
    let shuttingDown = false;
    let running = false;
    let lastRunAt = null;
    let lastError = null;
    const app = (0, express_1.default)();
    app.get('/healthz', (_req, res) => {
        res.status(200).json({ ok: true, running, lastRunAt, lastError });
    });
    app.get('/readyz', async (_req, res) => {
        try {
            const db = await (0, pool_js_1.checkDatabaseHealth)();
            res.status(200).json({ ok: true, running, lastRunAt, db });
        }
        catch (error) {
            res.status(503).json({
                ok: false,
                running,
                lastRunAt,
                lastError: error instanceof Error ? error.message : String(error)
            });
        }
    });
    const server = (0, node_http_1.createServer)(app);
    server.listen(env_js_1.env.PORT);
    const requestStop = () => {
        shuttingDown = true;
    };
    process.on('SIGINT', requestStop);
    process.on('SIGTERM', requestStop);
    (0, index_js_2.logInfo)('attribution_worker_service_started', {
        workerId,
        service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker'
    });
    while (!shuttingDown) {
        running = true;
        try {
            await (0, index_js_1.processAttributionQueue)({
                workerId,
                limit: env_js_1.env.ATTRIBUTION_JOB_BATCH_SIZE,
                staleScanLimit: env_js_1.env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE,
                emitMetrics: true
            });
            await (0, backfill_jobs_js_1.processOrderAttributionBackfillRuns)({
                workerId
            });
            await emitAttributionBacklogSnapshot(workerId);
            lastRunAt = new Date().toISOString();
            lastError = null;
        }
        catch (error) {
            lastRunAt = new Date().toISOString();
            lastError = error instanceof Error ? error.message : String(error);
            (0, index_js_2.logError)('attribution_worker_loop_failed', error, {
                workerId,
                service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker'
            });
        }
        finally {
            running = false;
        }
        if (!env_js_1.env.ATTRIBUTION_WORKER_LOOP || shuttingDown) {
            break;
        }
        await (0, promises_1.setTimeout)(env_js_1.env.ATTRIBUTION_WORKER_POLL_INTERVAL_MS);
    }
    await new Promise((resolve) => server.close(() => resolve()));
    await pool_js_1.pool.end();
}
run().catch(async (error) => {
    (0, index_js_2.logError)('attribution_worker_service_crashed', error, {
        service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker'
    });
    await pool_js_1.pool.end().catch(() => undefined);
    process.exit(1);
});
