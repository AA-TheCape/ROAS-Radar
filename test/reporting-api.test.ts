import assert from 'node:assert/strict';
import { type AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const originalPoolQuery = pool.query.bind(pool);

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

test('reporting routes require the configured bearer token', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10',
      {}
    );

    assert.equal(response.status, 401);
    assert.equal(body.error, 'unauthorized');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting summary returns headline metrics from daily campaign aggregates', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /FROM daily_reporting_metrics/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch', 'google', 'spring-sale']);

    return {
      rows: [
        {
          visits: '1240',
          orders: '48',
          revenue: '5210.50',
          spend: '0.00'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&source=google&campaign=spring-sale'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      range: {
        startDate: '2026-04-01',
        endDate: '2026-04-10'
      },
      totals: {
        visits: 1240,
        orders: 48,
        revenue: 5210.5,
        conversionRate: 48 / 1240,
        roas: null
      }
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting campaigns returns campaign rows sorted for dashboard tables', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /GROUP BY source, medium, campaign, content/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch', 2]);

    return {
      rows: [
        {
          source: 'google',
          medium: 'cpc',
          campaign: 'spring-sale',
          content: 'hero-ad-1',
          visits: '420',
          orders: '19',
          revenue: '2110.00'
        },
        {
          source: 'meta',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          content: '',
          visits: '310',
          orders: '9',
          revenue: '880.25'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/campaigns?startDate=2026-04-01&endDate=2026-04-10&limit=2'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      rows: [
        {
          source: 'google',
          medium: 'cpc',
          campaign: 'spring-sale',
          content: 'hero-ad-1',
          visits: 420,
          orders: 19,
          revenue: 2110,
          conversionRate: 19 / 420
        },
        {
          source: 'meta',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          content: null,
          visits: 310,
          orders: 9,
          revenue: 880.25,
          conversionRate: 9 / 310
        }
      ],
      nextCursor: null
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting timeseries returns grouped points for the requested dimension', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /SELECT\s+source AS bucket/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch']);

    return {
      rows: [
        {
          bucket: 'google',
          visits: '900',
          orders: '33',
          revenue: '3000.00'
        },
        {
          bucket: 'meta',
          visits: '340',
          orders: '15',
          revenue: '2210.50'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/timeseries?startDate=2026-04-01&endDate=2026-04-10&groupBy=source'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      points: [
        {
          date: 'google',
          visits: 900,
          orders: 33,
          revenue: 3000
        },
        {
          date: 'meta',
          visits: 340,
          orders: 15,
          revenue: 2210.5
        }
      ]
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting orders returns order-level attribution details for debugging', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /LEFT JOIN LATERAL/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch', 'facebook', 1]);

    return {
      rows: [
        {
          shopify_order_id: '1234567890',
          processed_at: new Date('2026-04-10T13:00:00.000Z'),
          total_price: '120.00',
          attributed_source: 'facebook',
          attributed_medium: 'paid_social',
          attributed_campaign: 'prospecting-us',
          attribution_reason: 'matched_by_checkout_token'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/orders?startDate=2026-04-01&endDate=2026-04-10&source=facebook&limit=1'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      rows: [
        {
          shopifyOrderId: '1234567890',
          processedAt: '2026-04-10T13:00:00.000Z',
          totalPrice: 120,
          source: 'facebook',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          attributionReason: 'matched_by_checkout_token'
        }
      ]
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting reconciliation returns persisted data quality checks', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('FROM data_quality_check_runs')) {
      return {
        rows: [
          {
            run_date: '2026-04-10',
            check_key: 'shopify_webhook_gaps',
            status: 'failed',
            severity: 'critical',
            discrepancy_count: 3,
            summary: '3 orders are missing webhook receipts.',
            details: {
              sampleMissingOrderIds: ['1001', '1002', '1003']
            },
            checked_at: new Date('2026-04-11T00:15:00.000Z'),
            alert_emitted_at: new Date('2026-04-11T00:15:00.000Z')
          }
        ]
      };
    }

    throw new Error(`Unexpected SQL in reconciliation test: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/reconciliation?runDate=2026-04-10'
    );

    assert.equal(response.status, 200);
    assert.equal(body.version, '2026-04-11');
    assert.equal(body.tenantId, 'roas-radar');
    assert.equal(body.data.runDate, '2026-04-10');
    assert.equal(body.data.totals.failedChecks, 1);
    assert.equal(body.data.totals.totalDiscrepancies, 3);
    assert.equal(body.data.checks[0].checkKey, 'shopify_webhook_gaps');
    assert.deepEqual(body.data.checks[0].details.sampleMissingOrderIds, ['1001', '1002', '1003']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});
