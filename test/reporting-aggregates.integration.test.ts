import assert from "node:assert/strict";
import test from "node:test";

import { buildRawPayloadFixture, resetIntegrationTables } from './integration-test-helpers.js';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const poolModule = await import("../src/db/pool.js");
const reportingModule = await import("../src/modules/reporting/aggregates.js");

const { pool } = poolModule;
const { refreshDailyMmmInputMart, refreshDailyReportingMetrics } = reportingModule;

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
		[connectionId, syncDate],
	);

	return jobResult.rows[0].id;
}

async function seedMetaConnection() {
  const accountFixture = buildRawPayloadFixture({ accountId: 'acct-meta' }, 'acct-meta');
  const connectionResult = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_connections (
        ad_account_id,
        access_token_encrypted,
        status,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_external_id,
        raw_account_payload_size_bytes,
        raw_account_payload_hash
      )
      VALUES ('acct-meta', '\\x00'::bytea, 'active', $1::jsonb, 'meta_ads_account', now(), $2, $3, $4)
      RETURNING id
    `,
    [
      accountFixture.rawPayloadJson,
      accountFixture.payloadExternalId,
      accountFixture.payloadSizeBytes,
      accountFixture.payloadHash
    ]
  );

  return connectionResult.rows[0].id;
}

async function seedMetaSyncJob(connectionId: number, syncDate: string) {
  const jobResult = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_sync_jobs (connection_id, sync_date, status, completed_at)
      VALUES ($1, $2::date, 'completed', $2::date + time '23:00')
      RETURNING id
    `,
    [connectionId, syncDate]
  );

  return jobResult.rows[0].id;
}

test("refreshDailyReportingMetrics includes campaign-only Google spend when no creative rows exist", async () => {
	const syncDate = "2026-04-24";

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
		[connectionId, syncJobId, syncDate],
	);

	const client = await pool.connect();

	try {
		await client.query("BEGIN");
		await refreshDailyReportingMetrics(client, [syncDate]);
		await client.query("COMMIT");
	} catch (error) {
		await client.query("ROLLBACK");
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
		[syncDate],
	);

	assert.deepEqual(result.rows, [
		{
			campaign: "marketplace with individual asset groups",
			spend: "35.20",
		},
		{
			campaign: "search campaign",
			spend: "10.00",
		},
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

  const webOrderPayload = buildRawPayloadFixture({ id: 'web-order-1' }, 'web-order-1');
  const posOrderPayload = buildRawPayloadFixture({ id: 'pos-order-1' }, 'pos-order-1');

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
        raw_payload,
        payload_external_id,
        payload_size_bytes,
        payload_hash
      ) VALUES
        ('web-order-1', '18391', 'USD', 90.00, 100.00, $1::timestamptz, 'web', $2::jsonb, $3, $4, $5),
        ('pos-order-1', '18392', 'USD', 45.00, 50.00, $1::timestamptz, 'pos', $6::jsonb, $7, $8, $9)
    `,
    [
      `${metricDate}T17:00:00.000Z`,
      webOrderPayload.rawPayloadJson,
      webOrderPayload.payloadExternalId,
      webOrderPayload.payloadSizeBytes,
      webOrderPayload.payloadHash,
      posOrderPayload.rawPayloadJson,
      posOrderPayload.payloadExternalId,
      posOrderPayload.payloadSizeBytes,
      posOrderPayload.payloadHash
    ]
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
        match_source,
        confidence_label,
        model_version
      ) VALUES
        ('web-order-1', 'last_touch', 1, NULL, $1::timestamptz, 'facebook', 'paid_social', 'prospecting-us', 1.0, 100.00, true, 'matched_by_checkout_token', 'matched_by_checkout_token', 'high', 1),
        ('pos-order-1', 'last_touch', 1, NULL, $1::timestamptz, 'pos', 'offline', 'retail', 1.0, 50.00, true, 'matched_by_checkout_token', 'matched_by_checkout_token', 'high', 1)
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

test('refreshDailyMmmInputMart builds versioned paid and attribution rows with coverage fields', async () => {
  const metricDate = '2026-05-02';

  await resetIntegrationTables(pool, [
    'mmm_daily_input_mart_v1',
    'meta_ads_daily_spend',
    'meta_ads_raw_spend_records',
    'meta_ads_sync_jobs',
    'meta_ads_connections',
    'attribution_order_credits',
    'shopify_orders'
  ]);

  const connectionId = await seedMetaConnection();
  const syncJobId = await seedMetaSyncJob(connectionId, metricDate);
  const orderPayload = buildRawPayloadFixture({ id: 'mmm-order-1' }, 'mmm-order-1');

  await pool.query(
    `
      INSERT INTO meta_ads_daily_spend (
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
        updated_at,
        raw_payload
      )
      VALUES (
        $1,
        $2,
        $3::date,
        'creative',
        'creative-1',
        'acct-meta',
        'Meta Account',
        'campaign-1',
        'Prospecting US',
        'adset-1',
        'Broad',
        'ad-1',
        'Static One',
        'creative-1',
        'Static One',
        'meta',
        'paid_social',
        'prospecting-us',
        'static-one',
        'unknown',
        'USD',
        125.50,
        10000,
        325,
        $3::date + time '23:30',
        '{}'::jsonb
      )
    `,
    [connectionId, syncJobId, metricDate]
  );

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
        raw_payload,
        payload_external_id,
        payload_size_bytes,
        payload_hash,
        ingested_at
      )
      VALUES (
        'mmm-order-1',
        '19001',
        'USD',
        90.00,
        100.00,
        $1::timestamptz,
        'web',
        $2::jsonb,
        $3,
        $4,
        $5,
        $6::timestamptz
      )
    `,
    [
      `${metricDate}T18:00:00.000Z`,
      orderPayload.rawPayloadJson,
      orderPayload.payloadExternalId,
      orderPayload.payloadSizeBytes,
      orderPayload.payloadHash,
      `${metricDate}T18:05:00.000Z`
    ]
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
        attributed_content,
        attributed_term,
        credit_weight,
        revenue_credit,
        is_primary,
        attribution_reason,
        match_source,
        confidence_label,
        model_version,
        created_at
      )
      VALUES (
        'mmm-order-1',
        'last_touch',
        1,
        NULL,
        $1::timestamptz,
        'meta',
        'paid_social',
        'prospecting-us',
        'static-one',
        'unknown',
        1.0,
        100.00,
        true,
        'matched_by_checkout_token',
        'checkout_token',
        'high',
        1,
        $2::timestamptz
      )
    `,
    [`${metricDate}T17:55:00.000Z`, `${metricDate}T18:10:00.000Z`]
  );

  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await refreshDailyMmmInputMart(client, [metricDate]);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }

  const result = await pool.query(
    `
      SELECT
        mart_version,
        mart_row_type,
        attribution_model,
        platform,
        platform_connection_id::text,
        granularity,
        entity_key,
        account_id,
        campaign_id,
        adset_id,
        ad_id,
        creative_id,
        source,
        medium,
        campaign,
        content,
        term,
        currency,
        spend,
        impressions::text,
        clicks::text,
        shopify_orders::text,
        shopify_revenue,
        attribution_credit_orders,
        attribution_credit_revenue,
        match_source_coverage,
        confidence_label_coverage,
        spend_last_synced_at IS NOT NULL AS has_spend_freshness,
        shopify_last_ingested_at IS NOT NULL AS has_shopify_freshness,
        attribution_last_computed_at IS NOT NULL AS has_attribution_freshness
      FROM mmm_daily_input_mart_v1
      WHERE metric_date = $1::date
      ORDER BY mart_row_type DESC, platform ASC
    `,
    [metricDate]
  );

  assert.deepEqual(result.rows, [
    {
      mart_version: 'v1',
      mart_row_type: 'paid_media',
      attribution_model: 'none',
      platform: 'meta',
      platform_connection_id: String(connectionId),
      granularity: 'creative',
      entity_key: 'creative-1',
      account_id: 'acct-meta',
      campaign_id: 'campaign-1',
      adset_id: 'adset-1',
      ad_id: 'ad-1',
      creative_id: 'creative-1',
      source: 'meta',
      medium: 'paid_social',
      campaign: 'prospecting-us',
      content: 'static-one',
      term: 'unknown',
      currency: 'USD',
      spend: '125.50',
      impressions: '10000',
      clicks: '325',
      shopify_orders: '0',
      shopify_revenue: '0.00',
      attribution_credit_orders: '0.00000000',
      attribution_credit_revenue: '0.00',
      match_source_coverage: {},
      confidence_label_coverage: {},
      has_spend_freshness: true,
      has_shopify_freshness: false,
      has_attribution_freshness: false
    },
    {
      mart_version: 'v1',
      mart_row_type: 'attribution',
      attribution_model: 'last_touch',
      platform: 'taxonomy',
      platform_connection_id: null,
      granularity: 'taxonomy',
      entity_key: 'meta|paid_social|prospecting-us|static-one|unknown',
      account_id: null,
      campaign_id: null,
      adset_id: null,
      ad_id: null,
      creative_id: null,
      source: 'meta',
      medium: 'paid_social',
      campaign: 'prospecting-us',
      content: 'static-one',
      term: 'unknown',
      currency: null,
      spend: '0.00',
      impressions: '0',
      clicks: '0',
      shopify_orders: '1',
      shopify_revenue: '100.00',
      attribution_credit_orders: '1.00000000',
      attribution_credit_revenue: '100.00',
      match_source_coverage: { checkout_token: 1 },
      confidence_label_coverage: { high: 1 },
      has_spend_freshness: false,
      has_shopify_freshness: true,
      has_attribution_freshness: true
    }
  ]);

  await resetIntegrationTables(pool, [
    'mmm_daily_input_mart_v1',
    'meta_ads_daily_spend',
    'meta_ads_raw_spend_records',
    'meta_ads_sync_jobs',
    'meta_ads_connections',
    'attribution_order_credits',
    'shopify_orders'
  ]);
});

test.after(async () => {
	await pool.end();
});
