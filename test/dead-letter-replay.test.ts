import assert from 'node:assert/strict';
import test from 'node:test';
import type { Pool, PoolClient } from 'pg';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

async function getModules() {
  const poolModule = await import('../src/db/pool.js');
  const deadLetterModule = await import('../src/modules/dead-letters/index.js');

  return {
    pool: poolModule.pool,
    withTransaction: poolModule.withTransaction,
    recordDeadLetter: deadLetterModule.recordDeadLetter,
    replayDeadLetters: deadLetterModule.replayDeadLetters,
    countPendingDeadLetters: deadLetterModule.countPendingDeadLetters
  };
}

async function ensureDeadLetterTables(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS event_dead_letters (
      id bigserial PRIMARY KEY,
      event_type text NOT NULL,
      source_table text NOT NULL,
      source_record_id text NOT NULL,
      source_queue_key text,
      status text NOT NULL DEFAULT 'pending_replay',
      first_failed_at timestamptz NOT NULL DEFAULT now(),
      last_failed_at timestamptz NOT NULL DEFAULT now(),
      last_error_message text,
      payload jsonb NOT NULL DEFAULT '{}'::jsonb,
      error_context jsonb NOT NULL DEFAULT '{}'::jsonb,
      failure_count integer NOT NULL DEFAULT 1,
      replayed_at timestamptz,
      last_replay_run_id bigint,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS event_dead_letters_source_uidx
      ON event_dead_letters (event_type, source_table, source_record_id)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS event_replay_runs (
      id bigserial PRIMARY KEY,
      replay_scope text NOT NULL DEFAULT 'filtered',
      event_type text,
      source_table text,
      window_start timestamptz,
      window_end timestamptz,
      requested_by text,
      dry_run boolean NOT NULL DEFAULT false,
      requested_at timestamptz NOT NULL DEFAULT now(),
      filters jsonb NOT NULL DEFAULT '{}'::jsonb,
      candidate_count integer NOT NULL DEFAULT 0,
      replayed_count integer NOT NULL DEFAULT 0,
      skipped_count integer NOT NULL DEFAULT 0,
      failed_count integer NOT NULL DEFAULT 0,
      dry_run_count integer NOT NULL DEFAULT 0,
      results jsonb NOT NULL DEFAULT '{}'::jsonb,
      started_at timestamptz NOT NULL DEFAULT now(),
      completed_at timestamptz
    )
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS replay_scope text NOT NULL DEFAULT 'filtered'
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS event_type text
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS source_table text
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS window_start timestamptz
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS window_end timestamptz
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS requested_by text
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS filters jsonb NOT NULL DEFAULT '{}'::jsonb
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS dry_run boolean NOT NULL DEFAULT false
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS requested_at timestamptz NOT NULL DEFAULT now()
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS candidate_count integer NOT NULL DEFAULT 0
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS replayed_count integer NOT NULL DEFAULT 0
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS skipped_count integer NOT NULL DEFAULT 0
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS failed_count integer NOT NULL DEFAULT 0
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS dry_run_count integer NOT NULL DEFAULT 0
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS results jsonb NOT NULL DEFAULT '{}'::jsonb
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS started_at timestamptz NOT NULL DEFAULT now()
  `);

  await pool.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS completed_at timestamptz
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS event_replay_run_items (
      id bigserial PRIMARY KEY,
      replay_run_id bigint NOT NULL REFERENCES event_replay_runs(id) ON DELETE CASCADE,
      dead_letter_id bigint NOT NULL REFERENCES event_dead_letters(id) ON DELETE CASCADE,
      source_table text NOT NULL,
      source_record_id text NOT NULL,
      outcome text NOT NULL,
      message text,
      created_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS source_table text
  `);

  await pool.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS source_record_id text
  `);

  await pool.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS outcome text
  `);

  await pool.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS message text
  `);

  await pool.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now()
  `);
}

async function resetDatabase(): Promise<void> {
  const { pool } = await getModules();
  await ensureDeadLetterTables(pool);

  await pool.query(`
    TRUNCATE TABLE
      event_replay_run_items,
      event_replay_runs,
      event_dead_letters,
      shopify_order_writeback_jobs,
      attribution_jobs,
      shopify_orders,
      tracking_events,
      tracking_sessions
    RESTART IDENTITY CASCADE
  `);
}

async function insertTrackingSession(pool: Pool, sessionId: string): Promise<void> {
  await pool.query(
    `
      INSERT INTO tracking_sessions (
        id,
        first_seen_at,
        last_seen_at,
        landing_page
      )
      VALUES (
        $1::uuid,
        '2026-04-20T10:00:00.000Z',
        '2026-04-20T10:05:00.000Z',
        'https://store.example/landing'
      )
    `,
    [sessionId]
  );
}

async function insertShopifyOrder(pool: Pool, orderId: string, sessionId: string): Promise<void> {
  await pool.query(
    `
      INSERT INTO shopify_orders (
        shopify_order_id,
        currency_code,
        subtotal_price,
        total_price,
        landing_session_id,
        processed_at,
        raw_payload,
        ingested_at
      )
      VALUES (
        $1,
        'USD',
        '100.00',
        '100.00',
        $2::uuid,
        '2026-04-22T12:00:00.000Z',
        '{}'::jsonb,
        now()
      )
    `,
    [orderId, sessionId]
  );
}

async function insertShopifyWritebackJob(pool: Pool, orderId: string) {
  const result = await pool.query<{ id: string }>(
    `
      INSERT INTO shopify_order_writeback_jobs (
        queue_key,
        shopify_order_id,
        requested_reason,
        status,
        attempts,
        available_at,
        dead_lettered_at,
        last_error,
        updated_at
      )
      VALUES (
        $1,
        $2,
        'shopify_order_upserted',
        'failed',
        1,
        now() - interval '10 minutes',
        now(),
        'shopify rejected note attributes',
        now()
      )
      RETURNING id::text AS id
    `,
    [`shopify_order:${orderId}`, orderId]
  );

  return result.rows[0].id;
}

async function insertAttributionJob(pool: Pool, orderId: string) {
  const result = await pool.query<{ id: string }>(
    `
      INSERT INTO attribution_jobs (
        queue_key,
        job_type,
        shopify_order_id,
        requested_reason,
        requested_model_version,
        status,
        attempts,
        available_at,
        dead_lettered_at,
        last_error,
        updated_at
      )
      VALUES (
        $1,
        'order',
        $2,
        'order_updated',
        1,
        'failed',
        2,
        now() - interval '15 minutes',
        now(),
        'resolver failed',
        now()
      )
      RETURNING id::text AS id
    `,
    [`order:${orderId}`, orderId]
  );

  return result.rows[0].id;
}

async function seedOrder(pool: Pool, orderId: string, sessionId: string): Promise<void> {
  await insertTrackingSession(pool, sessionId);
  await insertShopifyOrder(pool, orderId, sessionId);
}

async function recordDeadLetterInTransaction(
  callback: (client: PoolClient, modules: Awaited<ReturnType<typeof getModules>>) => Promise<void>
): Promise<void> {
  const modules = await getModules();
  await modules.withTransaction(async (client) => {
    await callback(client, modules);
  });
}

test.beforeEach(async () => {
  await resetDatabase();
});

test.after(async () => {
  const { pool } = await getModules();
  await pool.end();
});

test('failed Shopify writeback jobs retain payload and error context, and replay is auditable', async () => {
  const modules = await getModules();
  const sessionId = '123e4567-e89b-42d3-a456-426614174101';
  await seedOrder(modules.pool, '4101', sessionId);
  const jobId = await insertShopifyWritebackJob(modules.pool, '4101');

  await recordDeadLetterInTransaction(async (client, currentModules) => {
    const error = Object.assign(new Error('shopify rejected note attributes'), {
      retryable: false,
      statusCode: 422,
      responseBody: { errors: ['invalid note attribute'] }
    });

    await currentModules.recordDeadLetter(client, {
      eventType: 'shopify_writeback_failed',
      sourceTable: 'shopify_order_writeback_jobs',
      sourceRecordId: jobId,
      sourceQueueKey: 'shopify_order:4101',
      payload: {
        shopifyOrderId: '4101',
        requestedReason: 'shopify_order_upserted',
        attributes: [{ key: 'roas_radar_session_id', value: sessionId }]
      },
      error
    });
  });

  const pendingBeforeReplay = await modules.countPendingDeadLetters();
  assert.equal(pendingBeforeReplay, 1);

  const storedDeadLetter = await modules.pool.query<{
    source_queue_key: string | null;
    last_error_message: string | null;
    failure_count: number;
    shopify_order_id: string | null;
    status_code: string | null;
    retryable: string | null;
    response_body: string | null;
  }>(
    `
      SELECT
        source_queue_key,
        last_error_message,
        failure_count,
        payload->>'shopifyOrderId' AS shopify_order_id,
        error_context->>'statusCode' AS status_code,
        error_context->>'retryable' AS retryable,
        error_context->'responseBody'->'errors'->>0 AS response_body
      FROM event_dead_letters
      WHERE source_record_id = $1
    `,
    [jobId]
  );

  assert.equal(storedDeadLetter.rows[0].source_queue_key, 'shopify_order:4101');
  assert.equal(storedDeadLetter.rows[0].last_error_message, 'shopify rejected note attributes');
  assert.equal(storedDeadLetter.rows[0].failure_count, 1);
  assert.equal(storedDeadLetter.rows[0].shopify_order_id, '4101');
  assert.equal(storedDeadLetter.rows[0].status_code, '422');
  assert.equal(storedDeadLetter.rows[0].retryable, 'false');
  assert.equal(storedDeadLetter.rows[0].response_body, 'invalid note attribute');

  const replayResult = await modules.replayDeadLetters({
    requestedBy: 'ops+test@example.com',
    eventType: 'shopify_writeback_failed',
    sourceTable: 'shopify_order_writeback_jobs',
    limit: 10
  });

  assert.equal(replayResult.candidateCount, 1);
  assert.equal(replayResult.replayedCount, 1);
  assert.equal(replayResult.skippedCount, 0);
  assert.equal(replayResult.failedCount, 0);
  assert.equal(replayResult.dryRunCount, 0);

  const queueState = await modules.pool.query<{
    status: string;
    dead_lettered_at: Date | null;
    last_error: string | null;
    locked_by: string | null;
  }>(
    `
      SELECT status, dead_lettered_at, last_error, locked_by
      FROM shopify_order_writeback_jobs
      WHERE id = $1::bigint
    `,
    [jobId]
  );

  assert.equal(queueState.rows[0].status, 'pending');
  assert.equal(queueState.rows[0].dead_lettered_at, null);
  assert.equal(queueState.rows[0].last_error, null);
  assert.equal(queueState.rows[0].locked_by, null);

  const deadLetterAfterReplay = await modules.pool.query<{
    status: string;
    replayed_at: Date | null;
    last_replay_run_id: string | null;
  }>(
    `
      SELECT status, replayed_at, last_replay_run_id::text AS last_replay_run_id
      FROM event_dead_letters
      WHERE source_record_id = $1
    `,
    [jobId]
  );

  assert.equal(deadLetterAfterReplay.rows[0].status, 'replayed');
  assert.ok(deadLetterAfterReplay.rows[0].replayed_at);
  assert.equal(deadLetterAfterReplay.rows[0].last_replay_run_id, String(replayResult.replayRunId));

  const replayRun = await modules.pool.query<{
    requested_by: string | null;
    dry_run: boolean;
    candidate_count: number;
    replayed_count: number;
    dry_run_count: number;
    requested_by_filter: string | null;
  }>(
    `
      SELECT
        requested_by,
        dry_run,
        candidate_count,
        replayed_count,
        dry_run_count,
        filters->>'requestedBy' AS requested_by_filter
      FROM event_replay_runs
      WHERE id = $1
    `,
    [replayResult.replayRunId]
  );

  assert.equal(replayRun.rows[0].requested_by, 'ops+test@example.com');
  assert.equal(replayRun.rows[0].requested_by_filter, 'ops+test@example.com');
  assert.equal(replayRun.rows[0].dry_run, false);
  assert.equal(replayRun.rows[0].candidate_count, 1);
  assert.equal(replayRun.rows[0].replayed_count, 1);
  assert.equal(replayRun.rows[0].dry_run_count, 0);

  const replayRunItem = await modules.pool.query<{ outcome: string; message: string | null }>(
    `
      SELECT outcome, message
      FROM event_replay_run_items
      WHERE replay_run_id = $1
    `,
    [replayResult.replayRunId]
  );

  assert.equal(replayRunItem.rows[0].outcome, 'replayed');
  assert.equal(replayRunItem.rows[0].message, 'source record requeued');
});

test('replay can scope to a time window and requeue failed attribution jobs safely', async () => {
  const modules = await getModules();

  await seedOrder(modules.pool, '4201', '123e4567-e89b-42d3-a456-426614174201');
  await seedOrder(modules.pool, '4202', '123e4567-e89b-42d3-a456-426614174202');

  const insideWindowJobId = await insertAttributionJob(modules.pool, '4201');
  const outsideWindowJobId = await insertAttributionJob(modules.pool, '4202');

  await recordDeadLetterInTransaction(async (client, currentModules) => {
    await currentModules.recordDeadLetter(client, {
      eventType: 'attribution_job_failed',
      sourceTable: 'attribution_jobs',
      sourceRecordId: insideWindowJobId,
      sourceQueueKey: 'order:4201',
      payload: { shopifyOrderId: '4201' },
      error: { message: 'inside window failure', code: 'EINSIDE' }
    });

    await currentModules.recordDeadLetter(client, {
      eventType: 'attribution_job_failed',
      sourceTable: 'attribution_jobs',
      sourceRecordId: outsideWindowJobId,
      sourceQueueKey: 'order:4202',
      payload: { shopifyOrderId: '4202' },
      error: { message: 'outside window failure', code: 'EOUTSIDE' }
    });
  });

  await modules.pool.query(
    `
      UPDATE event_dead_letters
      SET last_failed_at = CASE source_record_id
        WHEN $1 THEN '2026-04-22T12:30:00.000Z'::timestamptz
        WHEN $2 THEN '2026-04-19T09:00:00.000Z'::timestamptz
        ELSE last_failed_at
      END
      WHERE source_record_id IN ($1, $2)
    `,
    [insideWindowJobId, outsideWindowJobId]
  );

  const dryRunResult = await modules.replayDeadLetters({
    requestedBy: 'ops-window-test',
    sourceTable: 'attribution_jobs',
    eventType: 'attribution_job_failed',
    fromTime: new Date('2026-04-22T00:00:00.000Z'),
    toTime: new Date('2026-04-22T23:59:59.999Z'),
    dryRun: true
  });

  assert.equal(dryRunResult.candidateCount, 1);
  assert.equal(dryRunResult.replayedCount, 0);
  assert.equal(dryRunResult.skippedCount, 0);
  assert.equal(dryRunResult.failedCount, 0);
  assert.equal(dryRunResult.dryRunCount, 1);

  const insideAfterDryRun = await modules.pool.query<{ status: string; dead_lettered_at: Date | null }>(
    `
      SELECT status, dead_lettered_at
      FROM attribution_jobs
      WHERE id = $1::bigint
    `,
    [insideWindowJobId]
  );

  assert.equal(insideAfterDryRun.rows[0].status, 'failed');
  assert.ok(insideAfterDryRun.rows[0].dead_lettered_at);

  const deadLetterAfterDryRun = await modules.pool.query<{ status: string }>(
    `
      SELECT status
      FROM event_dead_letters
      WHERE source_record_id = $1
    `,
    [insideWindowJobId]
  );

  assert.equal(deadLetterAfterDryRun.rows[0].status, 'pending_replay');

  const dryRunItem = await modules.pool.query<{ outcome: string; message: string | null }>(
    `
      SELECT outcome, message
      FROM event_replay_run_items
      WHERE replay_run_id = $1
    `,
    [dryRunResult.replayRunId]
  );

  assert.equal(dryRunItem.rows[0].outcome, 'dry_run');
  assert.equal(dryRunItem.rows[0].message, 'dry run only; source record was not requeued');

  const replayResult = await modules.replayDeadLetters({
    requestedBy: 'ops-window-test',
    sourceTable: 'attribution_jobs',
    eventType: 'attribution_job_failed',
    windowStart: new Date('2026-04-22T00:00:00.000Z'),
    windowEnd: new Date('2026-04-22T23:59:59.999Z')
  });

  assert.equal(replayResult.candidateCount, 1);
  assert.equal(replayResult.replayedCount, 1);
  assert.equal(replayResult.skippedCount, 0);
  assert.equal(replayResult.failedCount, 0);
  assert.equal(replayResult.dryRunCount, 0);

  const insideAfterReplay = await modules.pool.query<{ status: string; dead_lettered_at: Date | null }>(
    `
      SELECT status, dead_lettered_at
      FROM attribution_jobs
      WHERE id = $1::bigint
    `,
    [insideWindowJobId]
  );

  const outsideAfterReplay = await modules.pool.query<{ status: string; dead_lettered_at: Date | null }>(
    `
      SELECT status, dead_lettered_at
      FROM attribution_jobs
      WHERE id = $1::bigint
    `,
    [outsideWindowJobId]
  );

  assert.equal(insideAfterReplay.rows[0].status, 'pending');
  assert.equal(insideAfterReplay.rows[0].dead_lettered_at, null);
  assert.equal(outsideAfterReplay.rows[0].status, 'failed');
  assert.ok(outsideAfterReplay.rows[0].dead_lettered_at);

  const outsideDeadLetter = await modules.pool.query<{ status: string; last_replay_run_id: string | null }>(
    `
      SELECT status, last_replay_run_id::text AS last_replay_run_id
      FROM event_dead_letters
      WHERE source_record_id = $1
    `,
    [outsideWindowJobId]
  );

  assert.equal(outsideDeadLetter.rows[0].status, 'pending_replay');
  assert.equal(outsideDeadLetter.rows[0].last_replay_run_id, null);
});
