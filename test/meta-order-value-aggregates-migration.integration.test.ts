import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';

const { pool } = await import('../src/db/pool.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');
const { buildRawPayloadFixture } = await import('./integration-test-helpers.js');

const EXPECTED_INDEXES = [
  'meta_ads_order_value_aggregates_campaign_report_date_idx',
  'meta_ads_order_value_aggregates_connection_report_date_idx',
  'meta_ads_order_value_aggregates_dedupe_key',
  'meta_ads_order_value_aggregates_org_account_report_date_idx',
  'meta_ads_order_value_aggregates_org_report_date_campaign_idx',
  'meta_ads_order_value_aggregates_sync_job_idx'
] as const;

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('meta order value aggregate migration adds the expected indexes and uniqueness constraint', async () => {
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

  const orderValueSyncJobInsert = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_order_value_sync_jobs (connection_id, sync_date)
      VALUES ($1, '2026-04-29')
      RETURNING id
    `,
    [connectionId]
  );
  const orderValueSyncJobId = orderValueSyncJobInsert.rows[0]?.id;

  assert.ok(orderValueSyncJobId);

  const spendSyncJobInsert = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_sync_jobs (connection_id, sync_date)
      VALUES ($1, '2026-04-29')
      RETURNING id
    `,
    [connectionId]
  );
  const spendSyncJobId = spendSyncJobInsert.rows[0]?.id;

  assert.ok(spendSyncJobId);

  const rawSpendFixture = buildRawPayloadFixture({
    campaign_id: 'cmp_123',
    date_start: '2026-04-29',
    date_stop: '2026-04-29'
  }, 'cmp_123');
  const rawRecordInsert = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_raw_spend_records (
        connection_id,
        sync_job_id,
        report_date,
        level,
        entity_id,
        currency,
        spend,
        raw_payload,
        payload_source,
        payload_received_at,
        payload_size_bytes,
        payload_hash,
        payload_external_id
      )
      VALUES (
        $1,
        $2,
        '2026-04-29',
        'campaign',
        'cmp_123',
        'USD',
        12.34,
        $3::jsonb,
        'meta_ads_insights',
        '2026-04-29T15:59:00.000Z',
        $4,
        $5,
        $6
      )
      RETURNING id
    `,
    [
      connectionId,
      spendSyncJobId,
      rawSpendFixture.rawPayloadJson,
      rawSpendFixture.payloadSizeBytes,
      rawSpendFixture.payloadHash,
      rawSpendFixture.payloadExternalId
    ]
  );
  const rawRecordId = rawRecordInsert.rows[0]?.id;

  assert.ok(rawRecordId);

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

  await pool.query(
    `
      INSERT INTO meta_ads_order_value_aggregates (
        organization_id,
        meta_connection_id,
        sync_job_id,
        raw_record_id,
        ad_account_id,
        report_date,
        raw_date_start,
        raw_date_stop,
        campaign_id,
        campaign_name,
        attributed_revenue,
        purchase_count,
        spend,
        purchase_roas,
        currency,
        canonical_action_type,
        canonical_selection_mode,
        raw_action_values,
        raw_actions,
        source_synced_at,
        action_report_time,
        use_account_attribution_setting
      )
      VALUES (
        77,
        $1,
        $2,
        $3,
        '123456789',
        '2026-04-29',
        '2026-04-29',
        '2026-04-29',
        'cmp_123',
        'Campaign One',
        45.67,
        3,
        12.34,
        3.701135,
        'USD',
        'purchase',
        'priority',
        '[{"action_type":"purchase","value":"45.67"}]'::jsonb,
        '[{"action_type":"purchase","value":"3"}]'::jsonb,
        '2026-04-29T16:00:00.000Z',
        'conversion',
        true
      )
    `,
    [connectionId, orderValueSyncJobId, rawRecordId]
  );

  await assert.rejects(
    pool.query(
      `
        INSERT INTO meta_ads_order_value_aggregates (
          organization_id,
          meta_connection_id,
          sync_job_id,
          raw_record_id,
          ad_account_id,
          report_date,
          raw_date_start,
          raw_date_stop,
          campaign_id,
          campaign_name,
          attributed_revenue,
          purchase_count,
          spend,
          purchase_roas,
          currency,
          canonical_action_type,
          canonical_selection_mode,
          raw_action_values,
          raw_actions,
          source_synced_at,
          action_report_time,
          use_account_attribution_setting
        )
        VALUES (
          77,
          $1,
          $2,
          $3,
          '123456789',
          '2026-04-29',
          '2026-04-29',
          '2026-04-29',
          'cmp_123',
          'Campaign One Duplicate',
          40.00,
          2,
          10.00,
          4.000000,
          'USD',
          'purchase',
          'priority',
          '[]'::jsonb,
          '[]'::jsonb,
          '2026-04-29T17:00:00.000Z',
          'conversion',
          true
        )
      `,
      [connectionId, orderValueSyncJobId, rawRecordId]
    ),
    (error: unknown) => {
      assert.equal(typeof error, 'object');
      assert.ok(error);

      const databaseError = error as { code?: string; constraint?: string };
      assert.equal(databaseError.code, '23505');
      assert.equal(databaseError.constraint, 'meta_ads_order_value_aggregates_dedupe_key');

      return true;
    }
  );
});
