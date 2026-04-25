import {
  orderAttributionBackfillReportSchema,
  orderAttributionBackfillSubmittedOptionsSchema,
  type OrderAttributionBackfillReport,
  type OrderAttributionBackfillSubmittedOptions
} from '../../../packages/attribution-schema/index.js';
import { query, withTransaction } from '../../db/pool.js';
import { logError, logInfo } from '../../observability/index.js';
import {
  backfillRecentOrdersWithRecoveredAttribution,
  toOrderAttributionBackfillJobReport,
  type OrderAttributionBackfillOptions as BackfillExecutionOptions
} from './backfill.js';

const DEFAULT_ORDER_ATTRIBUTION_BACKFILL_RUN_BATCH_SIZE = 1;
type ExecuteBackfillRun = typeof backfillRecentOrdersWithRecoveredAttribution;

type OrderAttributionBackfillRunRow = {
  id: string;
  submitted_by: string;
  options: unknown;
};

type ProcessOrderAttributionBackfillRunsOptions = {
  workerId: string;
  limit?: number;
  now?: Date;
  executeBackfillRun?: ExecuteBackfillRun;
  claimRuns?: (workerId: string, now: Date, limit: number) => Promise<OrderAttributionBackfillRunRow[]>;
  markRunCompleted?: (runId: string, report: OrderAttributionBackfillReport, now: Date) => Promise<void>;
  markRunFailed?: (runId: string, error: unknown, now: Date) => Promise<void>;
};

type ProcessOrderAttributionBackfillRunsResult = {
  claimedRuns: number;
  completedRuns: number;
  failedRuns: number;
};

function parseJobOptions(input: unknown): OrderAttributionBackfillSubmittedOptions {
  return orderAttributionBackfillSubmittedOptionsSchema.parse(input);
}

function toUtcWindowStart(dateOnly: string): Date {
  return new Date(`${dateOnly}T00:00:00.000Z`);
}

function toUtcWindowEnd(dateOnly: string): Date {
  return new Date(`${dateOnly}T23:59:59.999Z`);
}

function normalizeErrorCode(error: unknown): string {
  if (typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' && error.code.trim()) {
    return error.code.trim();
  }

  if (error instanceof Error && error.name.trim()) {
    return error.name.trim();
  }

  return 'order_attribution_backfill_run_failed';
}

function normalizeErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }

  if (typeof error === 'string' && error.trim()) {
    return error.trim();
  }

  return 'Order attribution backfill job failed';
}

export function buildBackfillExecutionOptions(
  run: Pick<OrderAttributionBackfillRunRow, 'submitted_by' | 'options'>,
  workerId: string
): BackfillExecutionOptions {
  const options = parseJobOptions(run.options);

  return {
    windowStart: toUtcWindowStart(options.startDate),
    windowEnd: toUtcWindowEnd(options.endDate),
    requestedBy: run.submitted_by,
    workerId,
    limit: options.limit,
    dryRun: options.dryRun,
    onlyWebOrders: options.webOrdersOnly,
    writeToShopifyWhenAvailable: !options.skipShopifyWriteback
  };
}

async function claimOrderAttributionBackfillRuns(workerId: string, now: Date, limit: number): Promise<OrderAttributionBackfillRunRow[]> {
  void workerId;

  return withTransaction(async (client) => {
    const result = await client.query<OrderAttributionBackfillRunRow>(
      `
        WITH candidates AS (
          SELECT id
          FROM order_attribution_backfill_runs
          WHERE status = 'queued'
          ORDER BY submitted_at ASC, id ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE order_attribution_backfill_runs runs
        SET
          status = 'processing',
          started_at = COALESCE(runs.started_at, $1),
          updated_at = $1,
          error_code = NULL,
          error_message = NULL
        FROM candidates
        WHERE runs.id = candidates.id
        RETURNING runs.id, runs.submitted_by, runs.options
      `,
      [now, limit]
    );

    return result.rows;
  });
}

async function markOrderAttributionBackfillRunCompleted(
  runId: string,
  report: OrderAttributionBackfillReport,
  now: Date
): Promise<void> {
  await query(
    `
      UPDATE order_attribution_backfill_runs
      SET
        status = 'completed',
        completed_at = $2,
        report = $3::jsonb,
        error_code = NULL,
        error_message = NULL,
        updated_at = $2
      WHERE id = $1
    `,
    [runId, now, JSON.stringify(report)]
  );
}

async function markOrderAttributionBackfillRunFailed(runId: string, error: unknown, now: Date): Promise<void> {
  await query(
    `
      UPDATE order_attribution_backfill_runs
      SET
        status = 'failed',
        completed_at = $2,
        error_code = $3,
        error_message = $4,
        updated_at = $2
      WHERE id = $1
    `,
    [runId, now, normalizeErrorCode(error), normalizeErrorMessage(error)]
  );
}

export async function processOrderAttributionBackfillRuns(
  options: ProcessOrderAttributionBackfillRunsOptions
): Promise<ProcessOrderAttributionBackfillRunsResult> {
  const now = options.now ?? new Date();
  const limit = Math.max(1, options.limit ?? DEFAULT_ORDER_ATTRIBUTION_BACKFILL_RUN_BATCH_SIZE);
  const executeBackfillRun = options.executeBackfillRun ?? backfillRecentOrdersWithRecoveredAttribution;
  const claimRuns = options.claimRuns ?? claimOrderAttributionBackfillRuns;
  const markRunCompleted = options.markRunCompleted ?? markOrderAttributionBackfillRunCompleted;
  const markRunFailed = options.markRunFailed ?? markOrderAttributionBackfillRunFailed;
  const runs = await claimRuns(options.workerId, now, limit);

  let completedRuns = 0;
  let failedRuns = 0;

  for (const run of runs) {
    try {
      const executionOptions = buildBackfillExecutionOptions(run, options.workerId);
      const detailedReport = await executeBackfillRun(executionOptions);
      const finalReport = orderAttributionBackfillReportSchema.parse(toOrderAttributionBackfillJobReport(detailedReport));

      await markRunCompleted(run.id, finalReport, new Date());
      completedRuns += 1;
    } catch (error) {
      failedRuns += 1;
      logError('order_attribution_backfill_run_failed', error, {
        workerId: options.workerId,
        jobId: run.id
      });
      await markRunFailed(run.id, error, new Date());
    }
  }

  if (runs.length > 0) {
    logInfo('order_attribution_backfill_run_batch_processed', {
      workerId: options.workerId,
      claimedRuns: runs.length,
      completedRuns,
      failedRuns
    });
  }

  return {
    claimedRuns: runs.length,
    completedRuns,
    failedRuns
  };
}
