import { randomUUID } from 'node:crypto';
import { orderAttributionBackfillEnqueueResponseSchema, orderAttributionBackfillJobResponseSchema, orderAttributionBackfillReportSchema, orderAttributionBackfillSubmittedOptionsSchema } from '../../../packages/attribution-schema/index.js';
import { query, withTransaction } from '../../db/pool.js';
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
    return orderAttributionBackfillJobResponseSchema.parse({
        ok: true,
        jobId: row.id,
        status: row.status,
        submittedAt: row.submitted_at.toISOString(),
        submittedBy: row.submitted_by,
        startedAt: row.started_at?.toISOString() ?? null,
        completedAt: row.completed_at?.toISOString() ?? null,
        options: orderAttributionBackfillSubmittedOptionsSchema.parse(row.options),
        report: row.report == null ? null : orderAttributionBackfillReportSchema.parse(row.report),
        error: row.error_code && row.error_message
            ? {
                code: row.error_code,
                message: row.error_message
            }
            : null
    });
}
export async function enqueueOrderAttributionBackfillRun(options, submittedBy, now = new Date()) {
    const jobId = randomUUID();
    const submittedAt = now.toISOString();
    await query(`
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
    return orderAttributionBackfillEnqueueResponseSchema.parse({
        ok: true,
        jobId,
        status: 'queued',
        submittedAt,
        submittedBy,
        options
    });
}
export async function getOrderAttributionBackfillRun(jobId) {
    const result = await query(`
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
export async function claimOrderAttributionBackfillRuns(workerId, now, limit) {
    void workerId;
    const result = await withTransaction(async (client) => client.query(`
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
        const options = orderAttributionBackfillSubmittedOptionsSchema.parse(row.options);
        return {
            id: row.id,
            submittedBy: row.submitted_by,
            submittedAt: row.submitted_at.toISOString(),
            startedAt: row.started_at?.toISOString() ?? null,
            options
        };
    });
}
export async function markOrderAttributionBackfillRunCompleted(runId, report, now) {
    const normalizedReport = orderAttributionBackfillReportSchema.parse(report);
    await query(`
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
export async function markOrderAttributionBackfillRunFailed(runId, error, report, now) {
    const normalizedReport = report === null ? null : orderAttributionBackfillReportSchema.parse(report);
    await query(`
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
