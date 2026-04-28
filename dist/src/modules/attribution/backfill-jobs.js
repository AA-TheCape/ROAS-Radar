"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildBackfillExecutionOptions = buildBackfillExecutionOptions;
exports.processOrderAttributionBackfillRuns = processOrderAttributionBackfillRuns;
const index_js_1 = require("../../../packages/attribution-schema/index.js");
const index_js_2 = require("../../observability/index.js");
const backfill_js_1 = require("./backfill.js");
const backfill_run_store_js_1 = require("./backfill-run-store.js");
const DEFAULT_ORDER_ATTRIBUTION_BACKFILL_RUN_BATCH_SIZE = 1;
function toUtcWindowStart(dateOnly) {
    return new Date(`${dateOnly}T00:00:00.000Z`);
}
function toUtcWindowEnd(dateOnly) {
    return new Date(`${dateOnly}T23:59:59.999Z`);
}
function extractFailedRunReport(error) {
    if (!(error instanceof backfill_js_1.OrderAttributionBackfillRunError)) {
        return null;
    }
    return index_js_1.orderAttributionBackfillReportSchema.parse(error.report);
}
function buildBackfillExecutionOptions(run, workerId) {
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
async function processOrderAttributionBackfillRuns(options) {
    const now = options.now ?? new Date();
    const limit = Math.max(1, options.limit ?? DEFAULT_ORDER_ATTRIBUTION_BACKFILL_RUN_BATCH_SIZE);
    const executeBackfillRun = options.executeBackfillRun ?? backfill_js_1.backfillRecentOrdersWithRecoveredAttribution;
    const claimRuns = options.claimRuns ?? backfill_run_store_js_1.claimOrderAttributionBackfillRuns;
    const markRunCompleted = options.markRunCompleted ?? backfill_run_store_js_1.markOrderAttributionBackfillRunCompleted;
    const markRunFailed = options.markRunFailed ?? backfill_run_store_js_1.markOrderAttributionBackfillRunFailed;
    const runs = (await claimRuns(options.workerId, now, limit)).map((run) => ({
        id: run.id,
        submittedBy: 'submittedBy' in run ? run.submittedBy : run.submitted_by,
        options: run.options,
        submittedAt: run.submittedAt ?? null,
        startedAt: run.startedAt ?? null
    }));
    let completedRuns = 0;
    let failedRuns = 0;
    for (const run of runs) {
        const submittedOptions = run.options;
        const startedAt = run.startedAt ?? now.toISOString();
        (0, index_js_2.emitOrderAttributionBackfillJobLifecycleLog)({
            stage: 'started',
            jobId: run.id,
            workerId: options.workerId,
            startedAt,
            options: submittedOptions
        });
        try {
            const executionOptions = buildBackfillExecutionOptions(run, options.workerId);
            const detailedReport = await executeBackfillRun(executionOptions);
            const finalReport = index_js_1.orderAttributionBackfillReportSchema.parse((0, backfill_js_1.toOrderAttributionBackfillJobReport)(detailedReport));
            const completedAt = new Date();
            await markRunCompleted(run.id, finalReport, completedAt);
            (0, index_js_2.emitOrderAttributionBackfillJobLifecycleLog)({
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
            (0, index_js_2.emitOrderAttributionBackfillJobLifecycleLog)({
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
        (0, index_js_2.logInfo)('order_attribution_backfill_run_batch_processed', {
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
