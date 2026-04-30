import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';
process.env.SHOPIFY_APP_API_SECRET ??= 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET ??= 'test-webhook-secret';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');
const harnessModule = await import('./e2e-harness.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const { resetE2EDatabase } = harnessModule;

function buildHeaders(): Record<string, string> {
  return {
    authorization: 'Bearer test-reporting-token'
  };
}

async function requestJson(server: ReturnType<typeof createServer>, path: string, headers = buildHeaders()) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    headers
  });
  const body = await response.json();

  return { response, body };
}

test('reporting summary reads persisted daily aggregates from PostgreSQL', async () => {
  await resetE2EDatabase();
  await pool.query(
    `INSERT INTO daily_reporting_metrics (
      metric_date,
      attribution_model,
      source,
      medium,
      campaign,
      content,
      term,
      visits,
      attributed_orders,
      attributed_revenue,
      spend,
      impressions,
      clicks,
      new_customer_orders,
      returning_customer_orders,
      new_customer_revenue,
      returning_customer_revenue,
      last_computed_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now())`,
    ['2026-04-10', 'last_touch', 'google', 'cpc', 'spring-sale', 'hero-ad-1', 'widget', 42, 3, '390.00', '0.00', 0, 0, 1, 2, '120.00', '270.00']
  );

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-10&endDate=2026-04-10&source=google&campaign=spring-sale'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      range: {
        startDate: '2026-04-10',
        endDate: '2026-04-10'
      },
      totals: {
        visits: 42,
        orders: 3,
        revenue: 390,
        spend: 0,
        conversionRate: 3 / 42,
        roas: null
      }
    });
  } finally {
    await closeServer(server);
    await resetE2EDatabase();
  }
});

test('reporting campaign-oriented responses enrich display names from metadata lookup rows with deterministic fallback order', async () => {
  await resetE2EDatabase();

  await pool.query(
    `
      INSERT INTO google_ads_connections (
        id,
        customer_id,
        developer_token_encrypted,
        client_id,
        client_secret_encrypted,
        refresh_token_encrypted,
        status
      )
      VALUES (1, 'acct-google', '\\x00'::bytea, 'client', '\\x00'::bytea, '\\x00'::bytea, 'active')
    `
  );

  await pool.query(
    `
      INSERT INTO google_ads_sync_jobs (id, connection_id, sync_date, status)
      VALUES (1, 1, '2026-04-10'::date, 'completed')
    `
  );

  await pool.query(
    `
      INSERT INTO meta_ads_connections (
        id,
        ad_account_id,
        access_token_encrypted,
        status
      )
      VALUES (1, 'acct-meta', '\\x00'::bytea, 'active')
    `
  );

  await pool.query(
    `
      INSERT INTO meta_ads_sync_jobs (id, connection_id, sync_date, status)
      VALUES (1, 1, '2026-04-10'::date, 'completed')
    `
  );

  await pool.query(
    `
      INSERT INTO daily_reporting_metrics (
        metric_date,
        attribution_model,
        source,
        medium,
        campaign,
        content,
        term,
        visits,
        attributed_orders,
        attributed_revenue,
        spend,
        impressions,
        clicks,
        new_customer_orders,
        returning_customer_orders,
        new_customer_revenue,
        returning_customer_revenue,
        last_computed_at
      ) VALUES
        ('2026-04-10'::date, 'last_touch', 'google', 'cpc', 'brand-search', 'unknown', 'unknown', 120, 6, '900.00', '500.00', 0, 0, 0, 0, 0, 0, now()),
        ('2026-04-10'::date, 'last_touch', 'meta', 'paid_social', 'prospecting-us', 'unknown', 'unknown', 80, 3, '420.00', '200.00', 0, 0, 0, 0, 0, 0, now()),
        ('2026-04-10'::date, 'last_touch', 'google', 'cpc', 'clearance', 'unknown', 'unknown', 30, 1, '100.00', '50.00', 0, 0, 0, 0, 0, 0, now())
    `
  );

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
      ) VALUES
        (1, 1, '2026-04-10'::date, 'campaign', 'brand-search', 'acct-google', 'Google Account', 'cmp_google_1', 'Google Raw Brand Search', 'google', 'cpc', 'brand-search', 'unknown', 'unknown', 'USD', '500.00', 0, 0, '{}'::jsonb),
        (1, 1, '2026-04-10'::date, 'campaign', 'clearance', 'acct-google', 'Google Account', 'cmp_google_2', NULL, 'google', 'cpc', 'clearance', 'unknown', 'unknown', 'USD', '50.00', 0, 0, '{}'::jsonb)
    `
  );

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
      ) VALUES
        (1, 1, '2026-04-10'::date, 'campaign', 'prospecting-us', 'acct-meta', 'Meta Account', 'cmp_meta_1', 'Meta Prospecting Raw', 'meta', 'paid_social', 'prospecting-us', 'unknown', 'unknown', 'USD', '200.00', 0, 0, '{}'::jsonb)
    `
  );

  await pool.query(
    `
      INSERT INTO ad_platform_entity_metadata (
        platform,
        account_id,
        entity_type,
        entity_id,
        latest_name,
        last_seen_at,
        updated_at
      ) VALUES
        ('google_ads', 'acct-google', 'campaign', 'cmp_google_1', 'Google Brand Search Latest', '2026-04-10T08:00:00.000Z', '2026-04-10T08:05:00.000Z')
    `
  );

  const server = createServer();

  try {
    const campaigns = await requestJson(
      server,
      '/api/reporting/campaigns?startDate=2026-04-10&endDate=2026-04-10&limit=10'
    );
    const spendDetails = await requestJson(
      server,
      '/api/reporting/spend-details?startDate=2026-04-10&endDate=2026-04-10'
    );
    const timeseries = await requestJson(
      server,
      '/api/reporting/timeseries?startDate=2026-04-10&endDate=2026-04-10&groupBy=campaign'
    );

    assert.equal(campaigns.response.status, 200);
    assert.deepEqual(campaigns.body, {
      rows: [
        {
          source: 'google',
          medium: 'cpc',
          campaign: 'brand-search',
          content: 'unknown',
          visits: 120,
          orders: 6,
          revenue: 900,
          conversionRate: 6 / 120,
          campaignDisplayName: 'Google Brand Search Latest',
          campaignEntityId: 'cmp_google_1',
          campaignPlatform: 'google_ads',
          campaignNameResolutionStatus: 'resolved'
        },
        {
          source: 'meta',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          content: 'unknown',
          visits: 80,
          orders: 3,
          revenue: 420,
          conversionRate: 3 / 80,
          campaignDisplayName: 'Meta Prospecting Raw',
          campaignEntityId: 'cmp_meta_1',
          campaignPlatform: 'meta_ads',
          campaignNameResolutionStatus: 'fallback_name'
        },
        {
          source: 'google',
          medium: 'cpc',
          campaign: 'clearance',
          content: 'unknown',
          visits: 30,
          orders: 1,
          revenue: 100,
          conversionRate: 1 / 30,
          campaignDisplayName: 'cmp_google_2',
          campaignEntityId: 'cmp_google_2',
          campaignPlatform: 'google_ads',
          campaignNameResolutionStatus: 'unresolved'
        }
      ],
      nextCursor: null
    });

    assert.equal(spendDetails.response.status, 200);
    assert.deepEqual(spendDetails.body.groups, [
      {
        source: 'google',
        medium: 'cpc',
        channel: 'google / cpc',
        subtotal: 550,
        campaigns: [
          {
            campaign: 'brand-search',
            spend: 500,
            campaignDisplayName: 'Google Brand Search Latest',
            campaignEntityId: 'cmp_google_1',
            campaignPlatform: 'google_ads',
            campaignNameResolutionStatus: 'resolved'
          },
          {
            campaign: 'clearance',
            spend: 50,
            campaignDisplayName: 'cmp_google_2',
            campaignEntityId: 'cmp_google_2',
            campaignPlatform: 'google_ads',
            campaignNameResolutionStatus: 'unresolved'
          }
        ]
      },
      {
        source: 'meta',
        medium: 'paid_social',
        channel: 'meta / paid_social',
        subtotal: 200,
        campaigns: [
          {
            campaign: 'prospecting-us',
            spend: 200,
            campaignDisplayName: 'Meta Prospecting Raw',
            campaignEntityId: 'cmp_meta_1',
            campaignPlatform: 'meta_ads',
            campaignNameResolutionStatus: 'fallback_name'
          }
        ]
      }
    ]);

    assert.equal(timeseries.response.status, 200);
    assert.deepEqual(timeseries.body, {
      points: [
        {
          date: 'brand-search',
          visits: 120,
          orders: 6,
          revenue: 900,
          campaignDisplayName: 'Google Brand Search Latest',
          campaignEntityId: 'cmp_google_1',
          campaignPlatform: 'google_ads',
          campaignNameResolutionStatus: 'resolved'
        },
        {
          date: 'clearance',
          visits: 30,
          orders: 1,
          revenue: 100,
          campaignDisplayName: 'cmp_google_2',
          campaignEntityId: 'cmp_google_2',
          campaignPlatform: 'google_ads',
          campaignNameResolutionStatus: 'unresolved'
        },
        {
          date: 'prospecting-us',
          visits: 80,
          orders: 3,
          revenue: 420,
          campaignDisplayName: 'Meta Prospecting Raw',
          campaignEntityId: 'cmp_meta_1',
          campaignPlatform: 'meta_ads',
          campaignNameResolutionStatus: 'fallback_name'
        }
      ],
      lowestBuckets: [
        {
          bucket: 'clearance',
          visits: 30,
          orders: 1,
          revenue: 100,
          spend: 50,
          conversionRate: 1 / 30,
          roas: 2,
          campaignDisplayName: 'cmp_google_2',
          campaignEntityId: 'cmp_google_2',
          campaignPlatform: 'google_ads',
          campaignNameResolutionStatus: 'unresolved'
        },
        {
          bucket: 'prospecting-us',
          visits: 80,
          orders: 3,
          revenue: 420,
          spend: 200,
          conversionRate: 3 / 80,
          roas: 2.1,
          campaignDisplayName: 'Meta Prospecting Raw',
          campaignEntityId: 'cmp_meta_1',
          campaignPlatform: 'meta_ads',
          campaignNameResolutionStatus: 'fallback_name'
        },
        {
          bucket: 'brand-search',
          visits: 120,
          orders: 6,
          revenue: 900,
          spend: 500,
          conversionRate: 6 / 120,
          roas: 1.8,
          campaignDisplayName: 'Google Brand Search Latest',
          campaignEntityId: 'cmp_google_1',
          campaignPlatform: 'google_ads',
          campaignNameResolutionStatus: 'resolved'
        }
      ]
    });
  } finally {
    await closeServer(server);
    await resetE2EDatabase();
  }
});

test('reporting spend details and lowest buckets are scoped to the requested date range', async () => {
  await resetE2EDatabase();
  await pool.query(
    `INSERT INTO daily_reporting_metrics (
      metric_date,
      attribution_model,
      source,
      medium,
      campaign,
      content,
      term,
      visits,
      attributed_orders,
      attributed_revenue,
      spend,
      impressions,
      clicks,
      new_customer_orders,
      returning_customer_orders,
      new_customer_revenue,
      returning_customer_revenue,
      last_computed_at
    ) VALUES
      ($1, $2, $3, $4, $5, 'unknown', 'unknown', $6, $7, $8, $9, 0, 0, 0, 0, 0, 0, now()),
      ($10, $11, $12, $13, $14, 'unknown', 'unknown', $15, $16, $17, $18, 0, 0, 0, 0, 0, 0, now()),
      ($19, $20, $21, $22, $23, 'unknown', 'unknown', $24, $25, $26, $27, 0, 0, 0, 0, 0, 0, now())`,
    [
      '2026-04-08', 'last_touch', 'google', 'cpc', 'brand-search', 120, 4, '540.00', '210.00',
      '2026-04-09', 'last_touch', 'google', 'cpc', 'spring-search', 300, 10, '1800.00', '700.00',
      '2026-04-10', 'last_touch', 'meta', 'paid_social', 'prospecting-us', 180, 6, '620.00', '450.00'
    ]
  );

  const server = createServer();

  try {
    const spendDetails = await requestJson(
      server,
      '/api/reporting/spend-details?startDate=2026-04-09&endDate=2026-04-10'
    );
    const timeseries = await requestJson(
      server,
      '/api/reporting/timeseries?startDate=2026-04-09&endDate=2026-04-10&groupBy=campaign'
    );

    assert.equal(spendDetails.response.status, 200);
    assert.deepEqual(spendDetails.body, {
      summary: {
        totalSpend: 1150,
        activeChannels: 2,
        activeCampaigns: 2,
        averageDailySpend: 575,
        topChannel: {
          source: 'google',
          medium: 'cpc',
          channel: 'google / cpc',
          spend: 700
        }
      },
      groups: [
        {
          source: 'google',
          medium: 'cpc',
          channel: 'google / cpc',
          subtotal: 700,
          campaigns: [
            {
              campaign: 'spring-search',
              spend: 700
            }
          ]
        },
        {
          source: 'meta',
          medium: 'paid_social',
          channel: 'meta / paid_social',
          subtotal: 450,
          campaigns: [
            {
              campaign: 'prospecting-us',
              spend: 450
            }
          ]
        }
      ],
      totalSpend: 1150
    });

    assert.equal(timeseries.response.status, 200);
    assert.deepEqual(timeseries.body, {
      points: [
        {
          date: 'prospecting-us',
          visits: 180,
          orders: 6,
          revenue: 620
        },
        {
          date: 'spring-search',
          visits: 300,
          orders: 10,
          revenue: 1800
        }
      ],
      lowestBuckets: [
        {
          bucket: 'prospecting-us',
          visits: 180,
          orders: 6,
          revenue: 620,
          spend: 450,
          conversionRate: 6 / 180,
          roas: 620 / 450
        },
        {
          bucket: 'spring-search',
          visits: 300,
          orders: 10,
          revenue: 1800,
          spend: 700,
          conversionRate: 10 / 300,
          roas: 1800 / 700
        }
      ]
    });
  } finally {
    await closeServer(server);
    await resetE2EDatabase();
  }
});

test('reporting orders only returns online store Shopify orders', async () => {
  await resetE2EDatabase();
  await pool.query(
    `INSERT INTO shopify_orders (
      shopify_order_id,
      shopify_order_number,
      currency_code,
      subtotal_price,
      total_price,
      processed_at,
      source_name,
      raw_payload,
      payload_source,
      payload_external_id,
      payload_size_bytes,
      payload_hash
    ) VALUES
      ($1, $2, 'USD', '75.00', '80.00', $3, 'web', '{}'::jsonb, 'shopify_order', $1, 2, '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),
      ($4, $5, 'USD', '45.00', '50.00', $6, 'pos', '{}'::jsonb, 'shopify_order', $4, 2, '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a')`,
    [
      'web-order-1',
      '18387',
      '2026-04-10T13:00:00.000Z',
      'pos-order-1',
      '18388',
      '2026-04-10T14:00:00.000Z'
    ]
  );

  await pool.query(
    `INSERT INTO attribution_order_credits (
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
      ($1, 'last_touch', 1, NULL, $2, 'facebook', 'paid_social', 'prospecting-us', '1.0', '80.00', true, 'matched_by_checkout_token', 1),
      ($3, 'last_touch', 1, NULL, $4, 'pos', 'offline', 'retail', '1.0', '50.00', true, 'matched_by_checkout_token', 1)`,
    [
      'web-order-1',
      '2026-04-10T12:55:00.000Z',
      'pos-order-1',
      '2026-04-10T13:55:00.000Z'
    ]
  );

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/orders?startDate=2026-04-10&endDate=2026-04-10&limit=10'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      rows: [
        {
          shopifyOrderId: 'web-order-1',
          processedAt: '2026-04-10T13:00:00.000Z',
          orderOccurredAtUtc: '2026-04-10T13:00:00.000Z',
          totalPrice: 80,
          source: 'facebook',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          attributionReason: 'unattributed',
          primaryCreditAttributionReason: 'matched_by_checkout_token',
          attributionTier: 'unattributed',
          attributionTierLabel: 'Unattributed',
          attributionTierDescription:
            'No eligible first-party, Shopify hint, or GA4 fallback match qualified, or the required timing data could not be normalized.',
          attributionSource: null,
          attributionMatchedAt: null,
          confidenceScore: null,
          sessionId: null
        }
      ]
    });
  } finally {
    await closeServer(server);
    await resetE2EDatabase();
  }
});

test.after(async () => {
  await pool.end();
});
