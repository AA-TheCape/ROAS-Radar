import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';

const { pool } = await import('../src/db/pool.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');

const EXPECTED_COLUMNS = [
  'id',
  'ad_account_id',
  'object_type',
  'object_id',
  'object_name',
  'status',
  'last_fetched_at',
  'lookup_failed_at',
  'created_at',
  'updated_at'
] as const;

const EXPECTED_INDEXES = [
  'meta_ads_metadata_cache_account_type_idx',
  'meta_ads_metadata_cache_freshness_idx',
  'meta_ads_metadata_cache_lookup_failure_idx',
  'meta_ads_metadata_cache_pkey',
  'meta_ads_metadata_cache_scope_key'
] as const;

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('meta metadata cache migration stores successful and failed campaign/adset lookups uniquely', async () => {
  const columnResult = await pool.query<{ column_name: string }>(
    `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'meta_ads_metadata_cache'
      ORDER BY ordinal_position ASC
    `
  );

  assert.deepEqual(
    columnResult.rows.map((row) => row.column_name),
    [...EXPECTED_COLUMNS]
  );

  const constraintResult = await pool.query<{ constraint_name: string }>(
    `
      SELECT constraint_name
      FROM information_schema.table_constraints
      WHERE table_schema = 'public'
        AND table_name = 'meta_ads_metadata_cache'
        AND constraint_type IN ('UNIQUE', 'CHECK')
      ORDER BY constraint_name ASC
    `
  );
  const constraints = new Set(constraintResult.rows.map((row) => row.constraint_name));

  assert.equal(constraints.has('meta_ads_metadata_cache_scope_key'), true);
  assert.equal(constraints.has('meta_ads_metadata_cache_object_type_chk'), true);
  assert.equal(constraints.has('meta_ads_metadata_cache_lookup_timestamp_chk'), true);

  const indexResult = await pool.query<{ indexname: string }>(
    `
      SELECT indexname
      FROM pg_indexes
      WHERE schemaname = 'public'
        AND tablename = 'meta_ads_metadata_cache'
      ORDER BY indexname ASC
    `
  );

  assert.deepEqual(
    indexResult.rows.map((row) => row.indexname),
    [...EXPECTED_INDEXES].sort()
  );

  await pool.query(
    `
      INSERT INTO meta_ads_metadata_cache (
        ad_account_id,
        object_type,
        object_id,
        object_name,
        status,
        last_fetched_at
      )
      VALUES (
        'act_123',
        'campaign',
        'cmp_123',
        'Prospecting Campaign',
        'ACTIVE',
        '2026-06-02T10:00:00.000Z'
      )
    `
  );

  await pool.query(
    `
      INSERT INTO meta_ads_metadata_cache (
        ad_account_id,
        object_type,
        object_id,
        lookup_failed_at
      )
      VALUES (
        'act_123',
        'adset',
        'adset_missing',
        '2026-06-02T10:05:00.000Z'
      )
    `
  );

  await assert.rejects(
    pool.query(
      `
        INSERT INTO meta_ads_metadata_cache (
          ad_account_id,
          object_type,
          object_id,
          object_name,
          last_fetched_at
        )
        VALUES (
          'act_123',
          'campaign',
          'cmp_123',
          'Duplicate Campaign',
          '2026-06-02T11:00:00.000Z'
        )
      `
    ),
    (error: unknown) => {
      assert.ok(error && typeof error === 'object');
      assert.equal((error as { constraint?: string }).constraint, 'meta_ads_metadata_cache_scope_key');
      return true;
    }
  );

  await assert.rejects(
    pool.query(
      `
        INSERT INTO meta_ads_metadata_cache (
          ad_account_id,
          object_type,
          object_id
        )
        VALUES ('act_123', 'adset', 'adset_without_timestamp')
      `
    ),
    (error: unknown) => {
      assert.ok(error && typeof error === 'object');
      assert.equal((error as { constraint?: string }).constraint, 'meta_ads_metadata_cache_lookup_timestamp_chk');
      return true;
    }
  );
});
