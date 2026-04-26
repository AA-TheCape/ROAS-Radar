import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';

const { pool } = await import('../src/db/pool.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');

const EXPECTED_INDEXES = [
  'shopify_webhook_receipts_payload_lookup_idx',
  'shopify_orders_payload_lookup_idx',
  'meta_ads_connections_raw_account_lookup_idx',
  'meta_ads_raw_spend_records_payload_lookup_idx',
  'google_ads_connections_raw_customer_lookup_idx',
  'google_ads_raw_spend_records_payload_lookup_idx'
];

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('raw payload lookup indexes are present after migrations', async () => {
  const indexResult = await pool.query<{ indexname: string }>(
    `
      SELECT indexname
      FROM pg_indexes
      WHERE schemaname = 'public'
        AND indexname = ANY($1::text[])
      ORDER BY indexname ASC
    `,
    [EXPECTED_INDEXES]
  );

  assert.deepEqual(
    indexResult.rows.map((row) => row.indexname),
    [...EXPECTED_INDEXES].sort()
  );
});
