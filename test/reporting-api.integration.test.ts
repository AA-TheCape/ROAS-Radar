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

test.after(async () => {
  await pool.end();
});
