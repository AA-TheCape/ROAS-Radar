import { randomUUID } from 'node:crypto';
import { createHash } from 'node:crypto';
import { orderAttributionBackfillEnqueueResponseSchema, orderAttributionBackfillJobResponseSchema, orderAttributionBackfillReportSchema, orderAttributionBackfillSubmittedOptionsSchema } from '../../../packages/attribution-schema/index.js';
import { query, withTransaction } from '../../db/pool.js';
import { buildEmptyOrderAttributionBackfillProgress, parseOrderAttributionBackfillProgress } from './backfill-progress.js';
const ORDER_ATTRIBUTION_BACKFILL_STALE_AFTER_MINUTES = 15;
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
function stableSerialize(value) {
    if (value === null || typeof value !== 'object') {
        return JSON.stringify(value);
    }
    if (Array.isArray(value)) {
        return `[${value.map((entry) => stableSerialize(entry)).join(',')}]`;
    }
    const record = value;
    const keys = Object.keys(record).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableSerialize(record[key])}`).join(',')}}`;
}
function buildIdempotencyKey(options) {
    const explicitKey = 'idempotencyKey' in options && typeof options.idempotencyKey === 'string'
        ? options.idempotencyKey.trim()
        : '';
    if (explicitKey) {
        return `manual:${explicitKey}`;
    }
    return createHash('sha256')
        .update(stableSerialize({
        startDate: options.startDate,
        endDate: options.endDate,
        dryRun: options.dryRun,
        limit: options.limit,
        reclassificationTarget: 'reclassificationTarget' in options ? options.reclassificationTarget : 'full_rebuild',
        organizationIds: 'organizationIds' in options ? options.organizationIds : [],
        webOrdersOnly: options.webOrdersOnly,
        skipShopifyWriteback: options.skipShopifyWriteback
    }))
        .digest('hex');
}
export async function enqueueOrderAttributionBackfillRun(options, submittedBy, now = new Date()) {
    const jobId = randomUUID();
    const submittedAt = now.toISOString();
    const idempotencyKey = buildIdempotencyKey(options);
    const result = await query(`
      INSERT INTO order_attribution_backfill_runs (
        id,
        status,
        submitted_at,
        submitted_by,
        options,
        progress,
        last_heartbeat_at,
        idempotency_key
      )
      VALUES (
        $1,
        'queued',
        $2::timestamptz,
        $3,
        $4::jsonb,
        $5::jsonb,
        $2::timestamptz,
        $6
      )
      ON CONFLICT (idempotency_key)
      DO UPDATE SET idempotency_key = order_attribution_backfill_runs.idempotency_key
      RETURNING
        id,
        status,
        submitted_at,
        submitted_by,
        started_at,
        completed_at,
        options,
        progress,
        report,
        error_code,
        error_message,
        last_heartbeat_at,
        idempotency_key
    `, [
        jobId,
        submittedAt,
        submittedBy,
        JSON.stringify(options),
        JSON.stringify(buildEmptyOrderAttributionBackfillProgress()),
        idempotencyKey
    ]);
    const row = result.rows[0];
    if (!row) {
        return orderAttributionBackfillEnqueueResponseSchema.parse({
            ok: true,
            jobId,
            status: 'queued',
            submittedAt,
            submittedBy,
            options
        });
    }
    const normalizedOptions = orderAttributionBackfillSubmittedOptionsSchema.parse(row.options);
    return orderAttributionBackfillEnqueueResponseSchema.parse({
        ok: true,
        jobId: row.id,
        status: row.status,
        submittedAt: row.submitted_at.toISOString(),
        submittedBy: row.submitted_by,
        options: normalizedOptions
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
        progress,
        report,
        error_code,
        error_message,
        last_heartbeat_at,
        idempotency_key
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
             OR (
               status = 'processing'
               AND COALESCE(last_heartbeat_at, started_at, submitted_at) <= $1 - interval '${ORDER_ATTRIBUTION_BACKFILL_STALE_AFTER_MINUTES} minutes'
             )
          ORDER BY submitted_at ASC, id ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE order_attribution_backfill_runs runs
        SET
          status = 'processing',
          started_at = COALESCE(runs.started_at, $1),
          completed_at = NULL,
          last_heartbeat_at = $1,
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
          runs.progress,
          runs.report,
          runs.error_code,
          runs.error_message,
          runs.last_heartbeat_at,
          runs.idempotency_key
      `, [now, limit]));
    return result.rows.map((row) => {
        const options = orderAttributionBackfillSubmittedOptionsSchema.parse(row.options);
        return {
            id: row.id,
            submittedBy: row.submitted_by,
            submittedAt: row.submitted_at.toISOString(),
            startedAt: row.started_at?.toISOString() ?? null,
            options,
            progress: parseOrderAttributionBackfillProgress(row.progress)
        };
    });
}
export async function updateOrderAttributionBackfillRunProgress(runId, progress, now) {
    const normalizedProgress = parseOrderAttributionBackfillProgress(progress);
    await query(`
      UPDATE order_attribution_backfill_runs
      SET
        status = 'processing',
        progress = $3::jsonb,
        last_heartbeat_at = $2,
        updated_at = $2
      WHERE id = $1
    `, [runId, now, JSON.stringify(normalizedProgress)]);
}
export async function markOrderAttributionBackfillRunCompleted(runId, report, now) {
    const normalizedReport = orderAttributionBackfillReportSchema.parse(report);
    await query(`
      UPDATE order_attribution_backfill_runs
      SET
        status = 'completed',
        completed_at = $2,
        report = $3::jsonb,
        last_heartbeat_at = $2,
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
        last_heartbeat_at = $2,
        error_code = $4,
        error_message = $5,
        updated_at = $2
      WHERE id = $1
    `, [runId, now, normalizedReport === null ? null : JSON.stringify(normalizedReport), normalizeErrorCode(error), normalizeErrorMessage(error)]);
}
