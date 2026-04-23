import type { PoolClient } from 'pg';

import { env } from '../../config/env.js';
import { withTransaction } from '../../db/pool.js';
import { logError, logInfo } from '../../observability/index.js';

type SessionAttributionRetentionOptions = {
  batchSize?: number;
  maxBatches?: number;
  asOf?: Date;
  client?: PoolClient;
  emitLogs?: boolean;
};

export type SessionAttributionRetentionResult = {
  cutoffAt: string;
  batchSize: number;
  maxBatches: number;
  batchesRun: number;
  deletedTouchEvents: number;
  deletedSessions: number;
  protectedSessionsSkipped: number;
  protectedTouchEventsSkipped: number;
};

type ProtectedCountRow = {
  protected_sessions: string;
  protected_touch_events: string;
};

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value)) {
    return fallback;
  }

  return Math.max(Math.trunc(value ?? fallback), 1);
}

function resolveCutoffAt(asOf: Date | undefined): Date {
  const referenceTime = asOf ? new Date(asOf) : new Date();
  referenceTime.setUTCDate(referenceTime.getUTCDate() - env.SESSION_ATTRIBUTION_RETENTION_DAYS);
  return referenceTime;
}

async function execute<T>(client: PoolClient | undefined, callback: (db: PoolClient) => Promise<T>): Promise<T> {
  if (client) {
    return callback(client);
  }

  return withTransaction(callback);
}

async function countProtectedRows(client: PoolClient, cutoffAt: Date): Promise<ProtectedCountRow> {
  const result = await client.query<ProtectedCountRow>(`...`, [cutoffAt]);
  return result.rows[0] ?? { protected_sessions: '0', protected_touch_events: '0' };
}

async function deleteExpiredTouchEvents(client: PoolClient, cutoffAt: Date, batchSize: number): Promise<number> {
  const result = await client.query(`...DELETE expired unlinked touch events in batches...`, [cutoffAt, batchSize]);
  return result.rowCount ?? 0;
}

async function deleteExpiredSessions(client: PoolClient, cutoffAt: Date, batchSize: number): Promise<number> {
  const result = await client.query(`...DELETE expired unlinked session identities in batches...`, [cutoffAt, batchSize]);
  return result.rowCount ?? 0;
}

export async function runSessionAttributionRetention(
  options: SessionAttributionRetentionOptions = {}
): Promise<SessionAttributionRetentionResult> {
  // computes cutoff, loops in batches, logs each batch, returns totals
}

export async function runSessionAttributionRetentionJob(
  options: SessionAttributionRetentionOptions = {}
): Promise<SessionAttributionRetentionResult> {
  // wraps the job with structured error logging
}
