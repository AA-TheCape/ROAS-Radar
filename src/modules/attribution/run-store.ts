import { createHash } from 'node:crypto';

import { query, withTransaction } from '../../db/pool.js';
import { buildEmptyAttributionRunProgress, parseAttributionRunProgress, type AttributionRunProgress } from './run-progress.js';

const ATTRIBUTION_RUN_STALE_AFTER_MINUTES = 15;
const DEFAULT_ATTRIBUTION_RUN_BATCH_SIZE = 100;
const MAX_ATTRIBUTION_RUN_BATCH_SIZE = 5_000;

export type AttributionRunStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export type AttributionRunRequest = {
  windowStartUtc: string;
  windowEndUtc: string;
  submittedBy: string;
  triggerSource?: string;
  scopeKey?: string;
  concurrencyKey?: string;
  batchSize?: number;
  idempotencyKey?: string;
  runMetadata?: Record<string, unknown> | null;
};

type AttributionRunRow = {
  id: string;
  attribution_spec_version: 'v1';
  run_status: AttributionRunStatus;
  trigger_source: string;
  submitted_by: string;
  scope_key: string;
  concurrency_key: string;
  idempotency_key: string;
  started_at_utc: Date | null;
  completed_at_utc: Date | null;
  failed_at_utc: Date | null;
  created_at_utc: Date;
  updated_at_utc: Date;
  window_start_utc: Date | null;
  window_end_utc: Date | null;
  batch_size: number;
  input_snapshot: unknown;
  input_snapshot_hash: string;
  run_config_hash: string;
  run_metadata: unknown;
  progress: unknown;
  report: unknown;
  error_code: string | null;
  error_message: string | null;
  claimed_by: string | null;
  last_heartbeat_at: Date | null;
  resumed_from_run_id: string | null;
};

export type AttributionRunInputSnapshot = {
  orderIds: string[];
};

export type AttributionRunRecord = {
  id: string;
  attributionSpecVersion: 'v1';
  status: AttributionRunStatus;
  triggerSource: string;
  submittedBy: string;
  scopeKey: string;
  concurrencyKey: string;
  idempotencyKey: string;
  startedAtUtc: string | null;
  completedAtUtc: string | null;
  failedAtUtc: string | null;
  createdAtUtc: string;
  updatedAtUtc: string;
  windowStartUtc: string | null;
  windowEndUtc: string | null;
  batchSize: number;
  inputSnapshot: AttributionRunInputSnapshot;
  inputSnapshotHash: string;
  runConfigHash: string;
  runMetadata: Record<string, unknown>;
  progress: AttributionRunProgress;
  report: Record<string, unknown> | null;
  error: { code: string; message: string } | null;
  claimedBy: string | null;
  lastHeartbeatAtUtc: string | null;
  resumedFromRunId: string | null;
};

export type ClaimedAttributionRun = AttributionRunRecord;

export class AttributionRunConcurrencyError extends Error {
  code = 'attribution_run_concurrency_conflict';

  constructor(message: string) {
    super(message);
    this.name = 'AttributionRunConcurrencyError';
  }
}

function normalizeTrimmedString(value: string | null | undefined, fallback?: string): string {
  const normalized = value?.trim();
  if (normalized) {
    return normalized;
  }

  if (fallback !== undefined) {
    return fallback;
  }

  throw new Error('Expected non-empty string');
}

function normalizeBatchSize(value: number | undefined): number {
  const numeric = Number(value ?? DEFAULT_ATTRIBUTION_RUN_BATCH_SIZE);
  if (!Number.isFinite(numeric)) {
    return DEFAULT_ATTRIBUTION_RUN_BATCH_SIZE;
  }

  return Math.min(MAX_ATTRIBUTION_RUN_BATCH_SIZE, Math.max(1, Math.trunc(numeric)));
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(',')}]`;
  }

  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) => left.localeCompare(right));
    return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`).join(',')}}`;
  }

  return JSON.stringify(value);
}

function hashString(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function normalizeSnapshot(value: unknown): AttributionRunInputSnapshot {
  const orderIds =
    value && typeof value === 'object' && Array.isArray((value as { orderIds?: unknown[] }).orderIds)
      ? (value as { orderIds: unknown[] }).orderIds.filter(
          (entry): entry is string => typeof entry === 'string' && entry.trim().length > 0
        )
      : [];

  return {
    orderIds: Array.from(new Set(orderIds)).sort()
  };
}

function normalizeRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function mapRunRow(row: AttributionRunRow): AttributionRunRecord {
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
    error:
      row.error_code && row.error_message
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

async function fetchScopeOrderIds(windowStartUtc: string, windowEndUtc: string): Promise<string[]> {
  const result = await query<{ shopify_order_id: string }>(
    `
      SELECT shopify_order_id
      FROM shopify_orders
      WHERE COALESCE(processed_at, created_at_shopify, ingested_at) >= $1::timestamptz
        AND COALESCE(processed_at, created_at_shopify, ingested_at) <= $2::timestamptz
      ORDER BY COALESCE(processed_at, created_at_shopify, ingested_at) ASC, shopify_order_id ASC
    `,
    [windowStartUtc, windowEndUtc]
  );

  return result.rows.map((row) => row.shopify_order_id);
}

export function buildAttributionRunConfigHash(request: AttributionRunRequest): string {
  return hashString(
    stableStringify({
      attributionSpecVersion: 'v1',
      batchSize: normalizeBatchSize(request.batchSize),
      concurrencyKey: normalizeTrimmedString(request.concurrencyKey, request.scopeKey?.trim() || 'global'),
      scopeKey: normalizeTrimmedString(request.scopeKey, 'global'),
      triggerSource: normalizeTrimmedString(request.triggerSource, 'manual'),
      windowEndUtc: normalizeTrimmedString(request.windowEndUtc),
      windowStartUtc: normalizeTrimmedString(request.windowStartUtc)
    })
  );
}

export async function enqueueAttributionRun(request: AttributionRunRequest): Promise<AttributionRunRecord> {
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
  const idempotencyKey = normalizeTrimmedString(
    request.idempotencyKey,
    hashString(stableStringify({ inputSnapshotHash, runConfigHash }))
  );
  const metadata = {
    ...(request.runMetadata ?? {}),
    submittedBy,
    requestedAtUtc: new Date().toISOString()
  };

  try {
    const result = await withTransaction(async (client) => {
      const existing = await client.query<AttributionRunRow>(
        `
          SELECT *
          FROM attribution_runs
          WHERE idempotency_key = $1
          LIMIT 1
        `,
        [idempotencyKey]
      );

      if (existing.rows[0]) {
        return existing.rows[0];
      }

      const inserted = await client.query<AttributionRunRow>(
        `
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
        `,
        [
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
        ]
      );

      return inserted.rows[0];
    });

    return mapRunRow(result);
  } catch (error) {
    if (
      typeof error === 'object' &&
      error !== null &&
      'code' in error &&
      error.code === '23505' &&
      'constraint' in error &&
      error.constraint === 'attribution_runs_active_concurrency_idx'
    ) {
      throw new AttributionRunConcurrencyError(`Another attribution run is already active for concurrency key ${concurrencyKey}`);
    }

    throw error;
  }
}

export async function getAttributionRun(runId: string): Promise<AttributionRunRecord | null> {
  const result = await query<AttributionRunRow>(
    `
      SELECT *
      FROM attribution_runs
      WHERE id = $1::uuid
      LIMIT 1
    `,
    [runId]
  );

  return result.rows[0] ? mapRunRow(result.rows[0]) : null;
}

export async function claimAttributionRuns(workerId: string, now: Date, limit: number): Promise<ClaimedAttributionRun[]> {
  const result = await withTransaction(async (client) =>
    client.query<AttributionRunRow>(
      `
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
      `,
      [now.toISOString(), Math.max(1, Math.trunc(limit)), workerId]
    )
  );

  return result.rows.map(mapRunRow);
}

export async function updateAttributionRunProgress(
  runId: string,
  progress: AttributionRunProgress,
  now: Date
): Promise<void> {
  const normalized = parseAttributionRunProgress(progress);

  await query(
    `
      UPDATE attribution_runs
      SET
        run_status = 'running',
        progress = $2::jsonb,
        last_heartbeat_at = $3::timestamptz,
        updated_at_utc = $3::timestamptz
      WHERE id = $1::uuid
    `,
    [runId, JSON.stringify(normalized), now.toISOString()]
  );
}

export async function markAttributionRunCompleted(
  runId: string,
  report: Record<string, unknown>,
  now: Date
): Promise<void> {
  await query(
    `
      UPDATE attribution_runs
      SET
        run_status = 'completed',
        report = $2::jsonb,
        completed_at_utc = $3::timestamptz,
        failed_at_utc = NULL,
        last_heartbeat_at = $3::timestamptz,
        updated_at_utc = $3::timestamptz
      WHERE id = $1::uuid
    `,
    [runId, JSON.stringify(report), now.toISOString()]
  );
}

function normalizeErrorCode(error: unknown): string {
  if (typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' && error.code.trim()) {
    return error.code.trim();
  }

  if (error instanceof Error && error.name.trim()) {
    return error.name.trim();
  }

  return 'attribution_run_failed';
}

function normalizeErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }

  if (typeof error === 'string' && error.trim()) {
    return error.trim();
  }

  return 'Attribution run failed';
}

export async function markAttributionRunFailed(
  runId: string,
  error: unknown,
  report: Record<string, unknown> | null,
  now: Date
): Promise<void> {
  await query(
    `
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
    `,
    [runId, report ? JSON.stringify(report) : null, now.toISOString(), normalizeErrorCode(error), normalizeErrorMessage(error)]
  );
}

export async function resumeAttributionRun(runId: string, submittedBy: string, now = new Date()): Promise<AttributionRunRecord | null> {
  const result = await query<AttributionRunRow>(
    `
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
    `,
    [runId, normalizeTrimmedString(submittedBy), now.toISOString()]
  );

  return result.rows[0] ? mapRunRow(result.rows[0]) : null;
}
