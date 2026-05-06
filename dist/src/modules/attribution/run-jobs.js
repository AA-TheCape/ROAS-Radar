import { logInfo } from '../../observability/index.js';
import { executeAttributionRun } from './run-executor.js';
import { claimAttributionRuns, markAttributionRunCompleted, markAttributionRunFailed, updateAttributionRunProgress } from './run-store.js';
export async function processAttributionRuns(options) {
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
        }
        catch (error) {
            failedRuns += 1;
            const report = typeof error === 'object' && error !== null && 'report' in error && error.report && typeof error.report === 'object'
                ? error.report
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
