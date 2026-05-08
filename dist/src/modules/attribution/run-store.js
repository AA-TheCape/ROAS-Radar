import { createHash } from 'node:crypto';
import { query, withTransaction } from '../../db/pool.js';
import { buildEmptyAttributionRunProgress, parseAttributionRunProgress } from './run-progress.js';
const ATTRIBUTION_RUN_STALE_AFTER_MINUTES = 15;
const DEFAULT_ATTRIBUTION_RUN_BATCH_SIZE = 100;
const MAX_ATTRIBUTION_RUN_BATCH_SIZE = 5_000;
export class AttributionRunConcurrencyError extends Error {
    code = 'attribution_run_concurrency_conflict';
    constructor(message) {
        super(message);
        this.name = 'AttributionRunConcurrencyError';
    }
}
function normalizeTrimmedString(value, fallback) {
    const normalized = value?.trim();
    if (normalized) {
        return normalized;
    }
    if (fallback !== undefined) {
        return fallback;
    }
    throw new Error('Expected non-empty string');
}
function normalizeBatchSize(value) {
    const numeric = Number(value ?? DEFAULT_ATTRIBUTION_RUN_BATCH_SIZE);
    if (!Number.isFinite(numeric)) {
        return DEFAULT_ATTRIBUTION_RUN_BATCH_SIZE;
    }
    return Math.min(MAX_ATTRIBUTION_RUN_BATCH_SIZE, Math.max(1, Math.trunc(numeric)));
}
function stableStringify(value) {
    if (Array.isArray(value)) {
        return `[${value.map((entry) => stableStringify(entry)).join(',')}]`;
    }
    if (value && typeof value === 'object') {
        const entries = Object.entries(value).sort(([left], [right]) => left.localeCompare(right));
        return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`).join(',')}}`;
    }
    return JSON.stringify(value);
}
function hashString(value) {
    return createHash('sha256').update(value).digest('hex');
}
function normalizeSnapshot(value) {
    const orderIds = value && typeof value === 'object' && Array.isArray(value.orderIds)
        ? value.orderIds.filter((entry) => typeof entry === 'string' && entry.trim().length > 0)
        : [];
    return {
        orderIds: Array.from(new Set(orderIds)).sort()
    };
}
function normalizeRecord(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}
function mapRunRow(row) {
    return {
        id: row.id,
        attributionSpecVersion: row.attribution_spec_version,
        status: row.run_status,
        triggerSource: row.trigger_source,
        submittedBy: row.submitted_by,
        scopeKey: row.scope_key,
        concurrencyKey: row.concurrency_key,
        idempotencyKey: row.idempotency_key,
        startedAtUtc: row.started_at_utc?.toISOString() ?? null,
        completedAtUtc: row.completed_at_utc?.toISOString() ?? null,
        failedAtUtc: row.failed_at_utc?.toISOString() ?? null,
        createdAtUtc: row.created_at_utc.toISOString(),
        updatedAtUtc: row.updated_at_utc.toISOString(),
        windowStartUtc: row.window_start_utc?.toISOString() ?? null,
        windowEndUtc: row.window_end_utc?.toISOString() ?? null,
        batchSize: row.batch_size,
        inputSnapshot: normalizeSnapshot(row.input_snapshot),
        inputSnapshotHash: row.input_snapshot_hash,
        runConfigHash: row.run_config_hash,
        runMetadata: normalizeRecord(row.run_metadata),
        progress: parseAttributionRunProgress(row.progress),
        report: row.report == null ? null : normalizeRecord(row.report),
        error: row.error_code && row.error_message
            ? {
                code: row.error_code,
                message: row.error_message
            }
            : null,
        claimedBy: row.claimed_by,
        lastHeartbeatAtUtc: row.last_heartbeat_at?.toISOString() ?? null,
        resumedFromRunId: row.resumed_from_run_id
    };
}
async function fetchScopeOrderIds(windowStartUtc, windowEndUtc) {
    const result = await query(`
      SELECT shopify_order_id
      FROM shopify_orders
      WHERE COALESCE(processed_at, created_at_shopify, ingested_at) >= $1::timestamptz
        AND COALESCE(processed_at, created_at_shopify, ingested_at) <= $2::timestamptz
      ORDER BY COALESCE(processed_at, created_at_shopify, ingested_at) ASC, shopify_order_id ASC
    `, [windowStartUtc, windowEndUtc]);
    return result.rows.map((row) => row.shopify_order_id);
}
export function buildAttributionRunConfigHash(request) {
    return hashString(stableStringify({
        attributionSpecVersion: 'v1',
        batchSize: normalizeBatchSize(request.batchSize),
        concurrencyKey: normalizeTrimmedString(request.concurrencyKey, request.scopeKey?.trim() || 'global'),
        scopeKey: normalizeTrimmedString(request.scopeKey, 'global'),
        triggerSource: normalizeTrimmedString(request.triggerSource, 'manual'),
        windowEndUtc: normalizeTrimmedString(request.windowEndUtc),
        windowStartUtc: normalizeTrimmedString(request.windowStartUtc)
    }));
}
export async function enqueueAttributionRun(request) {
    const submittedBy = normalizeTrimmedString(request.submittedBy);
    const triggerSource = normalizeTrimmedString(request.triggerSource, 'manual');
    const scopeKey = normalizeTrimmedString(request.scopeKey, 'global');
    const concurrencyKey = normalizeTrimmedString(request.concurrencyKey, scopeKey);
    const batchSize = normalizeBatchSize(request.batchSize);
    const windowStartUtc = new Date(normalizeTrimmedString(request.windowStartUtc)).toISOString();
    const windowEndUtc = new Date(normalizeTrimmedString(request.windowEndUtc)).toISOString();
    const orderIds = await fetchScopeOrderIds(windowStartUtc, windowEndUtc);
    const inputSnapshot = { orderIds };
    const inputSnapshotHash = hashString(stableStringify(inputSnapshot));
    const runConfigHash = buildAttributionRunConfigHash({
        ...request,
        triggerSource,
        scopeKey,
        concurrencyKey,
        batchSize,
        windowStartUtc,
        windowEndUtc
    });
    const idempotencyKey = normalizeTrimmedString(request.idempotencyKey, hashString(stableStringify({ inputSnapshotHash, runConfigHash })));
    const metadata = {
        ...(request.runMetadata ?? {}),
        submittedBy,
        requestedAtUtc: new Date().toISOString()
    };
    try {
        const result = await withTransaction(async (client) => {
            const existing = await client.query(`
          SELECT *
          FROM attribution_runs
          WHERE idempotency_key = $1
          LIMIT 1
        `, [idempotencyKey]);
            if (existing.rows[0]) {
                return existing.rows[0];
            }
            const inserted = await client.query(`
          INSERT INTO attribution_runs (
            attribution_spec_version,
            run_status,
            trigger_source,
            submitted_by,
            scope_key,
            concurrency_key,
            idempotency_key,
            window_start_utc,
            window_end_utc,
            batch_size,
            input_snapshot,
            input_snapshot_hash,
            run_config_hash,
            run_metadata,
            progress,
            created_at_utc,
            updated_at_utc
          )
          VALUES (
            'v1',
            'pending',
            $1,
            $2,
            $3,
            $4,
            $5,
            $6::timestamptz,
            $7::timestamptz,
            $8,
            $9::jsonb,
            $10,
            $11,
            $12::jsonb,
            $13::jsonb,
            now(),
            now()
          )
          RETURNING *
        `, [
                triggerSource,
                submittedBy,
                scopeKey,
                concurrencyKey,
                idempotencyKey,
                windowStartUtc,
                windowEndUtc,
                batchSize,
                JSON.stringify(inputSnapshot),
                inputSnapshotHash,
                runConfigHash,
                JSON.stringify(metadata),
                JSON.stringify(buildEmptyAttributionRunProgress())
            ]);
            return inserted.rows[0];
        });
        return mapRunRow(result);
    }
    catch (error) {
        if (typeof error === 'object' &&
            error !== null &&
            'code' in error &&
            error.code === '23505' &&
            'constraint' in error &&
            error.constraint === 'attribution_runs_active_concurrency_idx') {
            throw new AttributionRunConcurrencyError(`Another attribution run is already active for concurrency key ${concurrencyKey}`);
        }
        throw error;
    }
}
export async function getAttributionRun(runId) {
    const result = await query(`
      SELECT *
      FROM attribution_runs
      WHERE id = $1::uuid
      LIMIT 1
    `, [runId]);
    return result.rows[0] ? mapRunRow(result.rows[0]) : null;
}
export async function claimAttributionRuns(workerId, now, limit) {
    const result = await withTransaction(async (client) => client.query(`
        WITH candidates AS (
          SELECT id
          FROM attribution_runs
          WHERE run_status = 'pending'
             OR (
               run_status = 'running'
               AND COALESCE(last_heartbeat_at, started_at_utc, created_at_utc) <= $1::timestamptz - interval '${ATTRIBUTION_RUN_STALE_AFTER_MINUTES} minutes'
             )
          ORDER BY created_at_utc ASC, id ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE attribution_runs runs
        SET
          run_status = 'running',
          started_at_utc = COALESCE(runs.started_at_utc, $1::timestamptz),
          completed_at_utc = NULL,
          failed_at_utc = NULL,
          claimed_by = $3,
          last_heartbeat_at = $1::timestamptz,
          error_code = NULL,
          error_message = NULL,
          updated_at_utc = $1::timestamptz
        FROM candidates
        WHERE runs.id = candidates.id
        RETURNING runs.*
      `, [now.toISOString(), Math.max(1, Math.trunc(limit)), workerId]));
    return result.rows.map(mapRunRow);
}
export async function updateAttributionRunProgress(runId, progress, now) {
    const normalized = parseAttributionRunProgress(progress);
    await query(`
      UPDATE attribution_runs
      SET
        run_status = 'running',
        progress = $2::jsonb,
        last_heartbeat_at = $3::timestamptz,
        updated_at_utc = $3::timestamptz
      WHERE id = $1::uuid
    `, [runId, JSON.stringify(normalized), now.toISOString()]);
}
export async function markAttributionRunCompleted(runId, report, now) {
    await query(`
      UPDATE attribution_runs
      SET
        run_status = 'completed',
        report = $2::jsonb,
        completed_at_utc = $3::timestamptz,
        failed_at_utc = NULL,
        last_heartbeat_at = $3::timestamptz,
        updated_at_utc = $3::timestamptz
      WHERE id = $1::uuid
    `, [runId, JSON.stringify(report), now.toISOString()]);
}
function normalizeErrorCode(error) {
    if (typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' && error.code.trim()) {
        return error.code.trim();
    }
    if (error instanceof Error && error.name.trim()) {
        return error.name.trim();
    }
    return 'attribution_run_failed';
}
function normalizeErrorMessage(error) {
    if (error instanceof Error && error.message.trim()) {
        return error.message.trim();
    }
    if (typeof error === 'string' && error.trim()) {
        return error.trim();
    }
    return 'Attribution run failed';
}
export async function markAttributionRunFailed(runId, error, report, now) {
    await query(`
      UPDATE attribution_runs
      SET
        run_status = 'failed',
        report = COALESCE($2::jsonb, report),
        failed_at_utc = $3::timestamptz,
        last_heartbeat_at = $3::timestamptz,
        error_code = $4,
        error_message = $5,
        updated_at_utc = $3::timestamptz
      WHERE id = $1::uuid
    `, [runId, report ? JSON.stringify(report) : null, now.toISOString(), normalizeErrorCode(error), normalizeErrorMessage(error)]);
}
export async function resumeAttributionRun(runId, submittedBy, now = new Date()) {
    const result = await query(`
      UPDATE attribution_runs
      SET
        run_status = CASE
          WHEN run_status IN ('failed', 'cancelled') THEN 'pending'
          ELSE run_status
        END,
        submitted_by = $2,
        claimed_by = NULL,
        completed_at_utc = NULL,
        failed_at_utc = NULL,
        error_code = NULL,
        error_message = NULL,
        last_heartbeat_at = NULL,
        updated_at_utc = $3::timestamptz
      WHERE id = $1::uuid
      RETURNING *
    `, [runId, normalizeTrimmedString(submittedBy), now.toISOString()]);
    return result.rows[0] ? mapRunRow(result.rows[0]) : null;
}
