import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';

const { pool } = await import('../src/db/pool.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');
const { buildRawPayloadFixture } = await import('./integration-test-helpers.js');

const EXPECTED_COLUMNS = [
  'id',
  'connection_id',
  'sync_date',
  'status',
  'attempts',
  'available_at',
  'locked_at',
  'locked_by',
  'last_error',
  'completed_at',
  'created_at',
  'updated_at'
] as const;

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('meta order value sync job migration matches spend queue conventions', async () => {
  const columnResult = await pool.query<{
    column_name: string;
    data_type: string;
    is_nullable: 'YES' | 'NO';
    column_default: string | null;
  }>(
    `
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'meta_ads_order_value_sync_jobs'
      ORDER BY ordinal_position ASC
    `
  );

  assert.deepEqual(
    columnResult.rows.map((row) => row.column_name),
    EXPECTED_COLUMNS
  );

  const constraintResult = await pool.query<{ constraint_name: string }>(
    `
      SELECT constraint_name
      FROM information_schema.table_constraints
      WHERE table_schema = 'public'
        AND table_name = 'meta_ads_order_value_sync_jobs'
        AND constraint_type IN ('UNIQUE', 'CHECK')
      ORDER BY constraint_name ASC
    `
  );

  assert.equal(
    constraintResult.rows.some((row) => row.constraint_name === 'meta_ads_order_value_sync_jobs_connection_id_sync_date_key'),
    true
  );
  assert.equal(
    constraintResult.rows.some((row) => row.constraint_name === 'meta_ads_order_value_sync_jobs_status_check'),
    true
  );

  const indexResult = await pool.query<{ indexname: string }>(
    `
      SELECT indexname
      FROM pg_indexes
      WHERE schemaname = 'public'
        AND tablename = 'meta_ads_order_value_sync_jobs'
      ORDER BY indexname ASC
    `
  );

  assert.equal(
    indexResult.rows.some((row) => row.indexname === 'meta_ads_order_value_sync_jobs_status_available_idx'),
    true
  );

  const rawAccountFixture = buildRawPayloadFixture({
    id: '123456789',
    name: 'Meta Account',
    currency: 'USD'
  }, '123456789');
  const connectionInsert = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_connections (
        ad_account_id,
        access_token_encrypted,
        account_currency,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_payload_size_bytes,
        raw_account_payload_hash,
        raw_account_external_id
      )
      VALUES (
        $1,
        '\\x01'::bytea,
        'USD',
        $2::jsonb,
        'meta_ads_account',
        '2026-04-29T15:55:00.000Z',
        $3,
        $4,
        $5
      )
      RETURNING id
    `,
    [
      '123456789',
      rawAccountFixture.rawPayloadJson,
      rawAccountFixture.payloadSizeBytes,
      rawAccountFixture.payloadHash,
      rawAccountFixture.payloadExternalId
    ]
  );
  const connectionId = connectionInsert.rows[0]?.id;

  assert.ok(connectionId);

  await pool.query(
    `
      INSERT INTO meta_ads_order_value_sync_jobs (
        connection_id,
        sync_date,
        status,
        attempts,
        last_error
      )
      VALUES ($1, '2026-04-29', 'retry', 1, 'temporary failure')
    `,
    [connectionId]
  );

  await assert.rejects(
    pool.query(
      `
        INSERT INTO meta_ads_order_value_sync_jobs (connection_id, sync_date)
        VALUES ($1, '2026-04-29')
      `,
      [connectionId]
    ),
    (error: unknown) => {
      assert.ok(error && typeof error === 'object');
      const databaseError = error as { constraint?: string };
      assert.equal(
        databaseError.constraint,
        'meta_ads_order_value_sync_jobs_connection_id_sync_date_key'
      );
      return true;
    }
  );
});
