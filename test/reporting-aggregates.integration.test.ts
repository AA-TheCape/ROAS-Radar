import assert from 'node:assert/strict';
import test from 'node:test';

import { buildRawPayloadFixture, resetIntegrationTables } from './integration-test-helpers.js';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const poolModule = await import('../src/db/pool.js');
const reportingModule = await import('../src/modules/reporting/aggregates.js');

const { pool } = poolModule;
const { refreshDailyReportingMetrics } = reportingModule;

async function seedGoogleConnection() {
  const rawCustomerFixture = buildRawPayloadFixture({ customerId: 'test-customer' }, 'test-customer');
  const connectionResult = await pool.query<{ id: number }>(
    `
      INSERT INTO google_ads_connections (
        customer_id,
        developer_token_encrypted,
        client_id,
        client_secret_encrypted,
        refresh_token_encrypted,
        status,
        raw_customer_payload_size_bytes,
        raw_customer_payload_hash,
        raw_customer_data
      )
      VALUES ('test-customer', '\\x00'::bytea, 'test-client', '\\x00'::bytea, '\\x00'::bytea, 'active', $1, $2, $3::jsonb)
      RETURNING id
    `,
    [rawCustomerFixture.payloadSizeBytes, rawCustomerFixture.payloadHash, rawCustomerFixture.rawPayloadJson]
  );

  return connectionResult.rows[0].id;
}

async function seedGoogleSyncJob(connectionId: number, syncDate: string) {
  const jobResult = await pool.query<{ id: number }>(
    `
      INSERT INTO google_ads_sync_jobs (connection_id, sync_date, status)
      VALUES ($1, $2::date, 'completed')
      RETURNING id
    `,
    [connectionId, syncDate]
  );

  return jobResult.rows[0].id;
}

test('refreshDailyReportingMetrics includes campaign-only Google spend when no creative rows exist', async () => {
  const syncDate = '2026-04-24';

  await resetIntegrationTables(pool, [
    'daily_reporting_metrics',
    'google_ads_daily_spend',
    'google_ads_raw_spend_records',
    'google_ads_sync_jobs',
    'google_ads_connections'
  ]);

  const connectionId = await seedGoogleConnection();
  const syncJobId = await seedGoogleSyncJob(connectionId, syncDate);

  await pool.query(
    `
      INSERT INTO google_ads_daily_spend (
        connection_id,
        sync_job_id,
        report_date,
        granularity,
        entity_key,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        creative_id,
        creative_name,
        canonical_source,
        canonical_medium,
        canonical_campaign,
        canonical_content,
        canonical_term,
        currency,
        spend,
        impressions,
        clicks,
        raw_payload
      )
      VALUES
        ($1, $2, $3::date, 'campaign', 'campaign-pmax', 'acct-1', 'Account', 'campaign-pmax', 'Marketplace with individual Asset Groups', NULL, NULL, NULL, NULL, NULL, NULL, 'google', 'cpc', 'marketplace with individual asset groups', 'unknown', 'unknown', 'USD', 35.20, 1000, 50, '{}'::jsonb),
        ($1, $2, $3::date, 'campaign', 'campaign-search', 'acct-1', 'Account', 'campaign-search', 'Search Campaign', NULL, NULL, NULL, NULL, NULL, NULL, 'google', 'cpc', 'search campaign', 'unknown', 'unknown', 'USD', 10.00, 500, 20, '{}'::jsonb),
        ($1, $2, $3::date, 'creative', 'creative-search', 'acct-1', 'Account', 'campaign-search', 'Search Campaign', 'adgroup-1', 'Ad group 1', 'ad-1', 'Search Ad', 'creative-search', 'Search Ad', 'google', 'cpc', 'search campaign', 'search ad', 'unknown', 'USD', 10.00, 500, 20, '{}'::jsonb)
    `,
    [connectionId, syncJobId, syncDate]
  );

  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await refreshDailyReportingMetrics(client, [syncDate]);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }

  const result = await pool.query(
    `
      SELECT campaign, spend
      FROM daily_reporting_metrics
      WHERE metric_date = $1::date
        AND attribution_model = 'last_touch'
        AND source = 'google'
      ORDER BY campaign ASC, spend DESC
    `,
    [syncDate]
  );

  assert.deepEqual(result.rows, [
    {
      campaign: 'marketplace with individual asset groups',
      spend: '35.20'
    },
    {
      campaign: 'search campaign',
      spend: '10.00'
    }
  ]);

  await resetIntegrationTables(pool, [
    'daily_reporting_metrics',
    'google_ads_daily_spend',
    'google_ads_raw_spend_records',
    'google_ads_sync_jobs',
    'google_ads_connections'
  ]);
});

test('refreshDailyReportingMetrics excludes non-online-store Shopify orders from attributed order metrics', async () => {
  const metricDate = '2026-04-29';

  await resetIntegrationTables(pool, [
    'daily_reporting_metrics',
    'attribution_order_credits',
    'shopify_orders'
  ]);

  await pool.query(
    `
      INSERT INTO shopify_orders (
        shopify_order_id,
        shopify_order_number,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        source_name,
        raw_payload
      ) VALUES
        ('web-order-1', '18391', 'USD', 90.00, 100.00, $1::timestamptz, 'web', '{}'::jsonb),
        ('pos-order-1', '18392', 'USD', 45.00, 50.00, $1::timestamptz, 'pos', '{}'::jsonb)
    `,
    [`${metricDate}T17:00:00.000Z`]
  );

  await pool.query(
    `
      INSERT INTO attribution_order_credits (
        shopify_order_id,
        attribution_model,
        touchpoint_position,
        session_id,
        touchpoint_occurred_at,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        credit_weight,
        revenue_credit,
        is_primary,
        attribution_reason,
        model_version
      ) VALUES
        ('web-order-1', 'last_touch', 1, NULL, $1::timestamptz, 'facebook', 'paid_social', 'prospecting-us', 1.0, 100.00, true, 'matched_by_checkout_token', 1),
        ('pos-order-1', 'last_touch', 1, NULL, $1::timestamptz, 'pos', 'offline', 'retail', 1.0, 50.00, true, 'matched_by_checkout_token', 1)
    `,
    [`${metricDate}T16:55:00.000Z`]
  );

  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await refreshDailyReportingMetrics(client, [metricDate]);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }

  const result = await pool.query(
    `
      SELECT source, medium, campaign, attributed_orders, attributed_revenue
      FROM daily_reporting_metrics
      WHERE metric_date = $1::date
        AND attribution_model = 'last_touch'
        AND attributed_orders > 0
      ORDER BY source ASC, medium ASC, campaign ASC
    `,
    [metricDate]
  );

  assert.deepEqual(result.rows, [
    {
      source: 'facebook',
      medium: 'paid_social',
      campaign: 'prospecting-us',
      attributed_orders: '1.00000000',
      attributed_revenue: '100.00'
    }
  ]);

  await resetIntegrationTables(pool, [
    'daily_reporting_metrics',
    'attribution_order_credits',
    'shopify_orders'
  ]);
});

test.after(async () => {
  await pool.end();
});
