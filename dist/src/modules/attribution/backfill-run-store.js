"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.enqueueOrderAttributionBackfillRun = enqueueOrderAttributionBackfillRun;
exports.getOrderAttributionBackfillRun = getOrderAttributionBackfillRun;
exports.claimOrderAttributionBackfillRuns = claimOrderAttributionBackfillRuns;
exports.markOrderAttributionBackfillRunCompleted = markOrderAttributionBackfillRunCompleted;
exports.markOrderAttributionBackfillRunFailed = markOrderAttributionBackfillRunFailed;
const node_crypto_1 = require("node:crypto");
const index_js_1 = require("../../../packages/attribution-schema/index.js");
const pool_js_1 = require("../../db/pool.js");
function normalizeErrorCode(error) {
    if (typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' && error.code.trim()) {
        return error.code.trim();
    }
    if (error instanceof Error && error.name.trim()) {
        return error.name.trim();
    }
    return 'order_attribution_backfill_run_failed';
}
function normalizeErrorMessage(error) {
    if (error instanceof Error && error.message.trim()) {
        return error.message.trim();
    }
    if (typeof error === 'string' && error.trim()) {
        return error.trim();
    }
    return 'Order attribution backfill job failed';
}
function mapBackfillRunRow(row) {
    return index_js_1.orderAttributionBackfillJobResponseSchema.parse({
        ok: true,
        jobId: row.id,
        status: row.status,
        submittedAt: row.submitted_at.toISOString(),
        submittedBy: row.submitted_by,
        startedAt: row.started_at?.toISOString() ?? null,
        completedAt: row.completed_at?.toISOString() ?? null,
        options: index_js_1.orderAttributionBackfillSubmittedOptionsSchema.parse(row.options),
        report: row.report == null ? null : index_js_1.orderAttributionBackfillReportSchema.parse(row.report),
        error: row.error_code && row.error_message
            ? {
                code: row.error_code,
                message: row.error_message
            }
            : null
    });
}
async function enqueueOrderAttributionBackfillRun(options, submittedBy, now = new Date()) {
    const jobId = (0, node_crypto_1.randomUUID)();
    const submittedAt = now.toISOString();
    await (0, pool_js_1.query)(`
      INSERT INTO order_attribution_backfill_runs (
        id,
        status,
        submitted_at,
        submitted_by,
        options
      )
      VALUES (
        $1,
        'queued',
        $2::timestamptz,
        $3,
        $4::jsonb
      )
    `, [jobId, submittedAt, submittedBy, JSON.stringify(options)]);
    return index_js_1.orderAttributionBackfillEnqueueResponseSchema.parse({
        ok: true,
        jobId,
        status: 'queued',
        submittedAt,
        submittedBy,
        options
    });
}
async function getOrderAttributionBackfillRun(jobId) {
    const result = await (0, pool_js_1.query)(`
      SELECT
        id,
        status,
        submitted_at,
        submitted_by,
        started_at,
        completed_at,
        options,
        report,
        error_code,
        error_message
      FROM order_attribution_backfill_runs
      WHERE id = $1
      LIMIT 1
    `, [jobId]);
    const row = result.rows[0];
    return row ? mapBackfillRunRow(row) : null;
}
async function claimOrderAttributionBackfillRuns(workerId, now, limit) {
    void workerId;
    const result = await (0, pool_js_1.withTransaction)(async (client) => client.query(`
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
          completed_at = NULL,
          report = NULL,
          updated_at = $1,
          error_code = NULL,
          error_message = NULL
        FROM candidates
        WHERE runs.id = candidates.id
        RETURNING
          runs.id,
          runs.status,
          runs.submitted_at,
          runs.submitted_by,
          runs.started_at,
          runs.completed_at,
          runs.options,
          runs.report,
          runs.error_code,
          runs.error_message
      `, [now, limit]));
    return result.rows.map((row) => {
        const options = index_js_1.orderAttributionBackfillSubmittedOptionsSchema.parse(row.options);
        return {
            id: row.id,
            submittedBy: row.submitted_by,
            submittedAt: row.submitted_at.toISOString(),
            startedAt: row.started_at?.toISOString() ?? null,
            options
        };
    });
}
async function markOrderAttributionBackfillRunCompleted(runId, report, now) {
    const normalizedReport = index_js_1.orderAttributionBackfillReportSchema.parse(report);
    await (0, pool_js_1.query)(`
      UPDATE order_attribution_backfill_runs
      SET
        status = 'completed',
        completed_at = $2,
        report = $3::jsonb,
        error_code = NULL,
        error_message = NULL,
        updated_at = $2
      WHERE id = $1
    `, [runId, now, JSON.stringify(normalizedReport)]);
}
async function markOrderAttributionBackfillRunFailed(runId, error, report, now) {
    const normalizedReport = report === null ? null : index_js_1.orderAttributionBackfillReportSchema.parse(report);
    await (0, pool_js_1.query)(`
      UPDATE order_attribution_backfill_runs
      SET
        status = 'failed',
        completed_at = $2,
        report = $3::jsonb,
        error_code = $4,
        error_message = $5,
        updated_at = $2
      WHERE id = $1
    `, [runId, now, normalizedReport === null ? null : JSON.stringify(normalizedReport), normalizeErrorCode(error), normalizeErrorMessage(error)]);
}
