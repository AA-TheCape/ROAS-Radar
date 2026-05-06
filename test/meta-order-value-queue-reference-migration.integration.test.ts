import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';

const { pool } = await import('../src/db/pool.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');
const { buildRawPayloadFixture } = await import('./integration-test-helpers.js');

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('meta order value tables reference order value sync jobs instead of spend sync jobs', async () => {
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

  const spendJobInsert = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_sync_jobs (connection_id, sync_date)
      VALUES ($1, '2026-04-29')
      RETURNING id
    `,
    [connectionId]
  );
  const spendJobId = spendJobInsert.rows[0]?.id;

  assert.ok(spendJobId);

  const syncRunInsert = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_order_value_sync_runs (
        connection_id,
        trigger_source,
        status,
        window_start_date,
        window_end_date
      )
      VALUES ($1, 'test', 'running', '2026-04-29', '2026-04-29')
      RETURNING id
    `,
    [connectionId]
  );
  const syncRunId = syncRunInsert.rows[0]?.id;

  assert.ok(syncRunId);

  await assert.rejects(
    pool.query(
      `
        INSERT INTO meta_ads_order_value_raw_records (
          connection_id,
          sync_run_id,
          sync_job_id,
          report_date,
          campaign_id,
          campaign_name,
          action_type,
          raw_payload
        )
        VALUES ($1, $2, $3, '2026-04-29', 'cmp_spend', 'Spend Job', 'purchase', '{}'::jsonb)
      `,
      [connectionId, syncRunId, spendJobId]
    ),
    (error: unknown) => {
      assert.ok(error && typeof error === 'object');
      const databaseError = error as { constraint?: string };
      assert.equal(databaseError.constraint, 'meta_ads_order_value_raw_records_sync_job_id_fkey');
      return true;
    }
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
          raw_revenue_record_ids,
          source_synced_at,
          action_report_time,
          use_account_attribution_setting
        )
        VALUES (
          77,
          $1,
          $2,
          NULL,
          '123456789',
          '2026-04-29',
          '2026-04-29',
          '2026-04-29',
          'cmp_spend',
          'Spend Job',
          42.10,
          3,
          12.34,
          3.414773,
          'USD',
          'purchase',
          'priority',
          '[]'::jsonb,
          '[]'::jsonb,
          '[]'::jsonb,
          '2026-04-29T16:00:00.000Z',
          'conversion',
          true
        )
      `,
      [connectionId, spendJobId]
    ),
    (error: unknown) => {
      assert.ok(error && typeof error === 'object');
      const databaseError = error as { constraint?: string };
      assert.equal(databaseError.constraint, 'meta_ads_order_value_aggregates_sync_job_id_fkey');
      return true;
    }
  );

  const orderValueJobInsert = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_order_value_sync_jobs (connection_id, sync_date)
      VALUES ($1, '2026-04-29')
      RETURNING id
    `,
    [connectionId]
  );
  const orderValueJobId = orderValueJobInsert.rows[0]?.id;

  assert.ok(orderValueJobId);

  await pool.query(
    `
      INSERT INTO meta_ads_order_value_raw_records (
        connection_id,
        sync_run_id,
        sync_job_id,
        report_date,
        campaign_id,
        campaign_name,
        action_type,
        raw_payload
      )
      VALUES ($1, $2, $3, '2026-04-29', 'cmp_order_value', 'Order Value Job', 'purchase', '{}'::jsonb)
    `,
    [connectionId, syncRunId, orderValueJobId]
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
        raw_revenue_record_ids,
        source_synced_at,
        action_report_time,
        use_account_attribution_setting
      )
      VALUES (
        77,
        $1,
        $2,
        NULL,
        '123456789',
        '2026-04-29',
        '2026-04-29',
        '2026-04-29',
        'cmp_order_value',
        'Order Value Job',
        42.10,
        3,
        12.34,
        3.414773,
        'USD',
        'purchase',
        'priority',
        '[]'::jsonb,
        '[]'::jsonb,
        '[]'::jsonb,
        '2026-04-29T16:00:00.000Z',
        'conversion',
        true
      )
    `,
    [connectionId, orderValueJobId]
  );
});
