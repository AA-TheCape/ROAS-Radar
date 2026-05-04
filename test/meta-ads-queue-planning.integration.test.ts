import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';
process.env.META_ADS_ENCRYPTION_KEY ??= 'meta-encryption-key';
process.env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS = '30';
process.env.META_ADS_SYNC_LOOKBACK_DAYS = '7';

const { pool } = await import('../src/db/pool.js');
const { processMetaAdsSyncQueue } = await import('../src/modules/meta-ads/index.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');

test.beforeEach(async () => {
  await resetE2EDatabase();
});
test.afterEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await pool.end();
});

test('processMetaAdsSyncQueue plans spend sync jobs into meta_ads_sync_jobs without writing to the order-value queue', { concurrency: false }, async () => {
  const connectionResult = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_connections (
        ad_account_id,
        access_token_encrypted,
        token_type,
        granted_scopes,
        status,
        account_name,
        account_currency,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_external_id,
        raw_account_payload_size_bytes,
        raw_account_payload_hash
      )
      VALUES (
        '123456789',
        pgp_sym_encrypt($1, $2, 'cipher-algo=aes256, compress-algo=0'),
        'Bearer',
        ARRAY['ads_read']::text[],
        'active',
        'Meta Account',
        'USD',
        '{"id":"123456789","name":"Meta Account","currency":"USD"}'::jsonb,
        'meta_ads_account',
        now(),
        '123456789',
        55,
        repeat('a', 64)
      )
      RETURNING id
    `,
    ['meta-access-token', process.env.META_ADS_ENCRYPTION_KEY]
  );
  const connectionId = connectionResult.rows[0]?.id;

  assert.ok(connectionId);

  await pool.query(
    `
      INSERT INTO meta_ads_sync_jobs (
        connection_id,
        sync_date,
        status,
        available_at,
        updated_at
      )
      VALUES ($1, '2026-04-10'::date, 'completed', now(), now())
    `,
    [connectionId]
  );

  const result = await processMetaAdsSyncQueue({
    now: new Date('2026-04-11T12:00:00.000Z')
  });

  assert.equal(result.enqueuedJobs, 30);
  assert.equal(result.claimedJobs, 0);
  assert.equal(result.succeededJobs, 0);
  assert.equal(result.failedJobs, 0);

  const [spendJobs, plannedFor, orderValueJobs] = await Promise.all([
    pool.query<{ sync_date: string }>(
      `
        SELECT sync_date::text
        FROM meta_ads_sync_jobs
        WHERE connection_id = $1
        ORDER BY sync_date ASC
      `,
      [connectionId]
    ),
    pool.query<{ last_sync_planned_for: string | null }>(
      `
        SELECT last_sync_planned_for::text
        FROM meta_ads_connections
        WHERE id = $1
      `,
      [connectionId]
    ),
    pool.query<{ count: string }>('SELECT count(*)::text AS count FROM meta_ads_order_value_sync_jobs')
  ]);

  assert.equal(spendJobs.rows.length, 30);
  assert.equal(spendJobs.rows[0]?.sync_date, '2026-03-13');
  assert.equal(spendJobs.rows.at(-1)?.sync_date, '2026-04-11');
  assert.equal(plannedFor.rows[0]?.last_sync_planned_for, '2026-04-11');
  assert.equal(orderValueJobs.rows[0]?.count, '0');
});
