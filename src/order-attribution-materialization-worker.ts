import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';

import { pool } from './db/pool.js';
import { backfillRecentOrdersWithRecoveredAttribution } from './modules/attribution/backfill.js';
import { logError, logInfo } from './observability/index.js';

function parseOptionalInteger(name: string): number | undefined {
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

function parseBoolean(name: string, defaultValue: boolean): boolean {
  const value = process.env[name]?.trim().toLowerCase();

  if (!value) {
    return defaultValue;
  }

  return ['1', 'true', 'yes', 'on'].includes(value);
}

function startOfUtcDay(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0));
}

function endOfUtcDay(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 23, 59, 59, 999));
}

export function resolveOrderAttributionMaterializationExecution(now: Date): {
  requestedBy: string;
  workerId: string;
  windowStart: Date;
  windowEnd: Date;
  limit?: number;
  dryRun: boolean;
  onlyWebOrders: boolean;
  writeToShopifyWhenAvailable: boolean;
} {
  const requestedBy =
    process.env.ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY?.trim() ||
    'cloud-run-scheduler';
  const workerId =
    process.env.ORDER_ATTRIBUTION_MATERIALIZATION_WORKER_ID?.trim() ||
    process.env.K_JOB_EXECUTION?.trim() ||
    `order-attribution-materialization-${randomUUID()}`;
  const lookbackDays = parseOptionalInteger('ORDER_ATTRIBUTION_MATERIALIZATION_LOOKBACK_DAYS') ?? 2;
  const lagDays = parseOptionalInteger('ORDER_ATTRIBUTION_MATERIALIZATION_LAG_DAYS') ?? 1;
  const anchorDate = new Date(now.getTime() - lagDays * 24 * 60 * 60 * 1000);
  const windowEnd = endOfUtcDay(anchorDate);
  const windowStart = startOfUtcDay(
    new Date(windowEnd.getTime() - (lookbackDays - 1) * 24 * 60 * 60 * 1000)
  );
  const dryRun = parseBoolean('ORDER_ATTRIBUTION_MATERIALIZATION_DRY_RUN', false);
  const onlyWebOrders = parseBoolean('ORDER_ATTRIBUTION_MATERIALIZATION_ONLY_WEB_ORDERS', true);
  const writeToShopifyWhenAvailable = !parseBoolean(
    'ORDER_ATTRIBUTION_MATERIALIZATION_SKIP_SHOPIFY_WRITEBACK',
    false
  );

  return {
    requestedBy,
    workerId,
    windowStart,
    windowEnd,
    limit: parseOptionalInteger('ORDER_ATTRIBUTION_MATERIALIZATION_LIMIT'),
    dryRun,
    onlyWebOrders,
    writeToShopifyWhenAvailable
  };
}

async function run(): Promise<void> {
  const execution = resolveOrderAttributionMaterializationExecution(new Date());

  logInfo('order_attribution_materialization_worker_started', {
    workerId: execution.workerId,
    requestedBy: execution.requestedBy,
    windowStart: execution.windowStart.toISOString(),
    windowEnd: execution.windowEnd.toISOString(),
    dryRun: execution.dryRun,
    onlyWebOrders: execution.onlyWebOrders,
    writeToShopifyWhenAvailable: execution.writeToShopifyWhenAvailable,
    service:
      process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-order-attribution-materialization'
  });

  const report = await backfillRecentOrdersWithRecoveredAttribution(execution);

  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  await pool.end();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  run().catch(async (error) => {
    logError('order_attribution_materialization_worker_failed', error, {
      service:
        process.env.K_SERVICE ?? process.env.K_JOB ?? 'roas-radar-order-attribution-materialization'
    });
    await pool.end().catch(() => undefined);
    process.exit(1);
  });
}
