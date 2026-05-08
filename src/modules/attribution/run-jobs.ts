import { logInfo } from '../../observability/index.js';
import { executeAttributionRun } from './run-executor.js';
import {
  claimAttributionRuns,
  markAttributionRunCompleted,
  markAttributionRunFailed,
  updateAttributionRunProgress,
  type ClaimedAttributionRun
} from './run-store.js';

type ProcessAttributionRunsOptions = {
  workerId: string;
  limit?: number;
  now?: Date;
  claimRuns?: (workerId: string, now: Date, limit: number) => Promise<ClaimedAttributionRun[]>;
  executeRun?: typeof executeAttributionRun;
  markRunCompleted?: (runId: string, report: Record<string, unknown>, now: Date) => Promise<void>;
  markRunFailed?: (runId: string, error: unknown, report: Record<string, unknown> | null, now: Date) => Promise<void>;
};

type ProcessAttributionRunsResult = {
  claimedRuns: number;
  completedRuns: number;
  failedRuns: number;
};

export async function processAttributionRuns(options: ProcessAttributionRunsOptions): Promise<ProcessAttributionRunsResult> {
  const now = options.now ?? new Date();
  const limit = Math.max(1, options.limit ?? 1);
  const claimRuns = options.claimRuns ?? claimAttributionRuns;
  const executeRun = options.executeRun ?? executeAttributionRun;
  const markRunCompleted = options.markRunCompleted ?? markAttributionRunCompleted;
  const markRunFailed = options.markRunFailed ?? markAttributionRunFailed;
  const runs = await claimRuns(options.workerId, now, limit);
  let completedRuns = 0;
  let failedRuns = 0;

  for (const run of runs) {
    try {
      const report = await executeRun({
        run,
        now,
        onProgress: async (progress) => updateAttributionRunProgress(run.id, progress, new Date())
      });

      if (report.retryOrderIdsOutstanding.length > 0) {
        throw Object.assign(new Error('Attribution run has retryable orders outstanding'), {
          code: 'attribution_run_retryable_orders_outstanding',
          report
        });
      }

      await markRunCompleted(run.id, report, new Date());
      completedRuns += 1;
    } catch (error) {
      failedRuns += 1;
      const report =
        typeof error === 'object' && error !== null && 'report' in error && error.report && typeof error.report === 'object'
          ? (error.report as Record<string, unknown>)
          : null;
      await markRunFailed(run.id, error, report, new Date());
    }
  }

  if (runs.length > 0) {
    logInfo('attribution_run_batch_processed', {
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
