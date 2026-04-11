import { randomUUID } from 'node:crypto';
import { createServer as createHttpServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';

import express from 'express';

import { env } from './config/env.js';
import { checkDatabaseHealth, pool } from './db/pool.js';
import { processAttributionQueue } from './modules/attribution/index.js';

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

  while (!shuttingDown) {
    running = true;

    try {
      await processAttributionQueue({
        workerId,
        limit: env.ATTRIBUTION_JOB_BATCH_SIZE,
        staleScanLimit: env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE,
        emitMetrics: true
      });
      lastRunAt = new Date().toISOString();
      lastError = null;
    } catch (error) {
      lastRunAt = new Date().toISOString();
      lastError = error instanceof Error ? error.message : String(error);
      process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
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
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  await pool.end().catch(() => undefined);
  process.exit(1);
});
