import { randomUUID } from 'node:crypto';
import { createServer as createHttpServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';

import express from 'express';

import { env } from './config/env.js';
import { checkDatabaseHealth, pool } from './db/pool.js';
import { processAttributionQueue } from './modules/attribution/index.js';
import { processOrderAttributionBackfillRuns } from './modules/attribution/backfill-jobs.js';
import { buildAttributionBacklogLog, logError, logInfo } from './observability/index.js';

type AttributionBacklogRow = {
  pending_jobs: string;
  oldest_job_age_seconds: string;
  stale_processing_jobs: string;
};

async function emitAttributionBacklogSnapshot(workerId: string): Promise<void> {
  const result = await pool.query<AttributionBacklogRow>(
    `
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
    `
  );
  const row = result.rows[0];

  if (!row) {
    return;
  }

  process.stdout.write(
    `${buildAttributionBacklogLog({
      workerId,
      pendingJobs: Number(row.pending_jobs),
      oldestJobAgeSeconds: Math.trunc(Number(row.oldest_job_age_seconds)),
      staleProcessingJobs: Number(row.stale_processing_jobs)
    })}\n`
  );
}

async function run(): Promise<void> {
  const workerId = `attribution-worker-${randomUUID()}`;
  let shuttingDown = false;
  let running = false;
  let lastRunAt: string | null = null;
  let lastError: string | null = null;

  const app = express();

  app.get('/healthz', (_req, res) => {
    res.status(200).json({ ok: true, running, lastRunAt, lastError });
  });

  app.get('/readyz', async (_req, res) => {
    try {
      const db = await checkDatabaseHealth();
      res.status(200).json({ ok: true, running, lastRunAt, db });
    } catch (error) {
      res.status(503).json({
        ok: false,
        running,
        lastRunAt,
        lastError: error instanceof Error ? error.message : String(error)
      });
    }
  });

  const server = createHttpServer(app);
  server.listen(env.PORT);

  const requestStop = () => {
    shuttingDown = true;
  };

  process.on('SIGINT', requestStop);
  process.on('SIGTERM', requestStop);

  logInfo('attribution_worker_service_started', {
    workerId,
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker'
  });

  while (!shuttingDown) {
    running = true;

    try {
      await processAttributionQueue({
        workerId,
        limit: env.ATTRIBUTION_JOB_BATCH_SIZE,
        staleScanLimit: env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE,
        emitMetrics: true
      });
      await processOrderAttributionBackfillRuns({
        workerId
      });
      await emitAttributionBacklogSnapshot(workerId);
      lastRunAt = new Date().toISOString();
      lastError = null;
    } catch (error) {
      lastRunAt = new Date().toISOString();
      lastError = error instanceof Error ? error.message : String(error);
      logError('attribution_worker_loop_failed', error, {
        workerId,
        service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker'
      });
    } finally {
      running = false;
    }

    if (!env.ATTRIBUTION_WORKER_LOOP || shuttingDown) {
      break;
    }

    await delay(env.ATTRIBUTION_WORKER_POLL_INTERVAL_MS);
  }

  await new Promise<void>((resolve) => server.close(() => resolve()));
  await pool.end();
}

run().catch(async (error) => {
  logError('attribution_worker_service_crashed', error, {
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker'
  });
  await pool.end().catch(() => undefined);
  process.exit(1);
});
