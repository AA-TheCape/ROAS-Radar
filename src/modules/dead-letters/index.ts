import type { PoolClient } from 'pg';
import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { logError, logInfo } from '../../observability/index.js';

export async function recordDeadLetter(client: PoolClient, input: DeadLetterInput): Promise<void> {
  // Upserts durable DLQ rows with payload + serialized error context.
}

export async function replayDeadLetters(filters: ReplayFilters): Promise<{
  replayRunId: number;
  candidateCount: number;
  replayedCount: number;
  skippedCount: number;
  failedCount: number;
}> {
  // Creates an audited replay run, filters by event type/source/time window,
  // requeues source jobs safely, writes run-item outcomes, and marks DLQ rows replayed.
}
