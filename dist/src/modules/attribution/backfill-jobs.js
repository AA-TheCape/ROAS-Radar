import { orderAttributionBackfillReportSchema, } from '../../../packages/attribution-schema/index.js';
import { emitOrderAttributionBackfillJobLifecycleLog, logInfo } from '../../observability/index.js';
import { backfillRecentOrdersWithRecoveredAttribution, OrderAttributionBackfillRunError, toOrderAttributionBackfillJobReport } from './backfill.js';
import { claimOrderAttributionBackfillRuns, markOrderAttributionBackfillRunCompleted, markOrderAttributionBackfillRunFailed, updateOrderAttributionBackfillRunProgress } from './backfill-run-store.js';
import { buildEmptyOrderAttributionBackfillProgress } from './backfill-progress.js';
const DEFAULT_ORDER_ATTRIBUTION_BACKFILL_RUN_BATCH_SIZE = 1;
function toUtcWindowStart(dateOnly) {
    return new Date(`${dateOnly}T00:00:00.000Z`);
}
function toUtcWindowEnd(dateOnly) {
    return new Date(`${dateOnly}T23:59:59.999Z`);
}
function extractFailedRunReport(error) {
    if (!(error instanceof OrderAttributionBackfillRunError)) {
        return null;
    }
    return orderAttributionBackfillReportSchema.parse(error.report);
}
export function buildBackfillExecutionOptions(run, workerId) {
    const requestedBy = 'submittedBy' in run ? run.submittedBy : run.submitted_by;
    return {
        windowStart: toUtcWindowStart(run.options.startDate),
        windowEnd: toUtcWindowEnd(run.options.endDate),
        requestedBy,
        workerId,
        limit: run.options.limit,
        dryRun: run.options.dryRun,
        onlyWebOrders: run.options.webOrdersOnly,
        writeToShopifyWhenAvailable: !run.options.skipShopifyWriteback
    };
}
export async function processOrderAttributionBackfillRuns(options) {
    const now = options.now ?? new Date();
    const limit = Math.max(1, options.limit ?? DEFAULT_ORDER_ATTRIBUTION_BACKFILL_RUN_BATCH_SIZE);
    const executeBackfillRun = options.executeBackfillRun ?? backfillRecentOrdersWithRecoveredAttribution;
    const claimRuns = options.claimRuns ?? claimOrderAttributionBackfillRuns;
    const markRunCompleted = options.markRunCompleted ?? markOrderAttributionBackfillRunCompleted;
    const markRunFailed = options.markRunFailed ?? markOrderAttributionBackfillRunFailed;
    const runs = (await claimRuns(options.workerId, now, limit)).map((run) => ({
        id: run.id,
        submittedBy: 'submittedBy' in run ? run.submittedBy : run.submitted_by,
        options: run.options,
        submittedAt: run.submittedAt ?? null,
        startedAt: run.startedAt ?? null,
        progress: 'progress' in run ? run.progress : buildEmptyOrderAttributionBackfillProgress()
    }));
    let completedRuns = 0;
    let failedRuns = 0;
    for (const run of runs) {
        const submittedOptions = run.options;
        const startedAt = run.startedAt ?? now.toISOString();
        emitOrderAttributionBackfillJobLifecycleLog({
            stage: 'started',
            jobId: run.id,
            workerId: options.workerId,
            startedAt,
            options: submittedOptions
        });
        try {
            const executionOptions = buildBackfillExecutionOptions(run, options.workerId);
            const detailedReport = await executeBackfillRun({
                ...executionOptions,
                runId: run.id,
                progress: run.progress,
                onProgress: async (progress) => updateOrderAttributionBackfillRunProgress(run.id, progress, new Date())
            });
            const finalReport = orderAttributionBackfillReportSchema.parse(toOrderAttributionBackfillJobReport(detailedReport));
            const completedAt = new Date();
            await markRunCompleted(run.id, finalReport, completedAt);
            emitOrderAttributionBackfillJobLifecycleLog({
                stage: 'completed',
                jobId: run.id,
                workerId: options.workerId,
                startedAt,
                completedAt: completedAt.toISOString(),
                options: submittedOptions,
                report: finalReport
            });
            completedRuns += 1;
        }
        catch (error) {
            failedRuns += 1;
            const failedReport = extractFailedRunReport(error);
            const completedAt = new Date();
            emitOrderAttributionBackfillJobLifecycleLog({
                stage: 'failed',
                jobId: run.id,
                workerId: options.workerId,
                startedAt,
                completedAt: completedAt.toISOString(),
                options: submittedOptions,
                report: failedReport,
                error
            });
            await markRunFailed(run.id, error, failedReport, completedAt);
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
