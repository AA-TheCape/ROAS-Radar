import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
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
        spend: 0,
        conversionRate: 48 / 1240,
        roas: null
      }
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting routes reject invalid date ranges before querying aggregates', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-10&endDate=2026-04-01'
    );

    assert.equal(response.status, 400);
    assert.equal(body.error, 'invalid_request');
    assert.equal(queryCalls, 0);
    assert.deepEqual(body.details.fieldErrors.startDate, ['startDate must be on or before endDate']);
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

test('reporting spend details return channel groups with campaign subtotals in descending order', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /GROUP BY source, medium, campaign/);
    assert.match(text, /AND spend > 0/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch']);

    return {
      rows: [
        {
          source: 'google',
          medium: 'cpc',
          campaign: 'spring-search',
          spend: '1200.00'
        },
        {
          source: 'google',
          medium: 'cpc',
          campaign: 'brand-search',
          spend: '300.00'
        },
        {
          source: 'meta',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          spend: '900.50'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/spend-details?startDate=2026-04-01&endDate=2026-04-10'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      summary: {
        totalSpend: 2400.5,
        activeChannels: 2,
        activeCampaigns: 3,
        averageDailySpend: 240.05,
        topChannel: {
          source: 'google',
          medium: 'cpc',
          channel: 'google / cpc',
          spend: 1500
        }
      },
      groups: [
        {
          source: 'google',
          medium: 'cpc',
          channel: 'google / cpc',
          subtotal: 1500,
          campaigns: [
            {
              campaign: 'spring-search',
              spend: 1200
            },
            {
              campaign: 'brand-search',
              spend: 300
            }
          ]
        },
        {
          source: 'meta',
          medium: 'paid_social',
          channel: 'meta / paid_social',
          subtotal: 900.5,
          campaigns: [
            {
              campaign: 'prospecting-us',
              spend: 900.5
            }
          ]
        }
      ],
      totalSpend: 2400.5
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
          revenue: '3000.00',
          spend: '1200.00'
        },
        {
          bucket: 'meta',
          visits: '340',
          orders: '15',
          revenue: '2210.50',
          spend: '900.50'
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
      ],
      lowestBuckets: [
        {
          bucket: 'meta',
          visits: 340,
          orders: 15,
          revenue: 2210.5,
          spend: 900.5,
          conversionRate: 15 / 340,
          roas: 2210.5 / 900.5
        },
        {
          bucket: 'google',
          visits: 900,
          orders: 33,
          revenue: 3000,
          spend: 1200,
          conversionRate: 33 / 900,
          roas: 2.5
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
    if (text.includes('INSERT INTO app_settings')) {
      assert.deepEqual(params, ['America/Los_Angeles']);
      return { rows: [], rowCount: 0 };
    }

    if (text.includes('SELECT reporting_timezone')) {
      return {
        rows: [
          {
            reporting_timezone: 'America/Los_Angeles',
            updated_at: new Date('2026-04-01T00:00:00.000Z')
          }
        ],
        rowCount: 1
      };
    }

    assert.match(text, /LEFT JOIN LATERAL/);
    assert.match(text, /COALESCE\(o\.source_name, ''\) = 'web'/);
    assert.deepEqual(
      params,
      ['2026-04-01', '2026-04-10', 'last_touch', 'facebook', 'deterministic_first_party', 'America/Los_Angeles', 1]
    );

    return {
      rows: [
        {
          shopify_order_id: '1234567890',
          processed_at: new Date('2026-04-10T13:00:00.000Z'),
          total_price: '120.00',
          attribution_tier: 'deterministic_first_party',
          attribution_source: 'checkout_token',
          order_attribution_reason: 'matched_by_checkout_token',
          attribution_matched_at: new Date('2026-04-10T13:01:00.000Z'),
          attribution_snapshot: {
            confidenceScore: 1,
            winner: {
              sessionId: '11111111-1111-4111-8111-111111111111',
              source: 'facebook',
              medium: 'paid_social',
              campaign: 'prospecting-us'
            }
          },
          attributed_source: 'facebook',
          attributed_medium: 'paid_social',
          attributed_campaign: 'prospecting-us',
          primary_credit_attribution_reason: 'matched_by_checkout_token'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/orders?startDate=2026-04-01&endDate=2026-04-10&source=facebook&attributionTier=deterministic_first_party&limit=1'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      rows: [
        {
          shopifyOrderId: '1234567890',
          processedAt: '2026-04-10T13:00:00.000Z',
          orderOccurredAtUtc: '2026-04-10T13:00:00.000Z',
          totalPrice: 120,
          source: 'facebook',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          attributionReason: 'matched_by_checkout_token',
          primaryCreditAttributionReason: 'matched_by_checkout_token',
          attributionTier: 'deterministic_first_party',
          attributionTierLabel: 'Deterministic first-party',
          attributionTierDescription:
            'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
          attributionSource: 'checkout_token',
          attributionMatchedAt: '2026-04-10T13:01:00.000Z',
          confidenceScore: 1,
          sessionId: '11111111-1111-4111-8111-111111111111'
        }
      ]
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting order details expose attribution tier metadata additively', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM shopify_orders o')) {
      assert.deepEqual(params, ['1234567890']);

      return {
        rows: [
          {
            shopify_order_id: '1234567890',
            shopify_order_number: 'RR-1001',
            shopify_customer_id: 'gid://shopify/Customer/42',
            customer_identity_id: '22222222-2222-4222-8222-222222222222',
            email_hash: 'hash_abc123',
            currency_code: 'USD',
            subtotal_price: '100.00',
            total_price: '120.00',
            financial_status: 'paid',
            fulfillment_status: 'fulfilled',
            processed_at: new Date('2026-04-10T13:00:00.000Z'),
            created_at_shopify: new Date('2026-04-10T12:58:00.000Z'),
            updated_at_shopify: new Date('2026-04-10T13:05:00.000Z'),
            landing_session_id: '33333333-3333-4333-8333-333333333333',
            checkout_token: 'checkout-123',
            cart_token: 'cart-123',
            source_name: 'web',
            attribution_tier: 'deterministic_first_party',
            attribution_source: 'landing_session_id',
            attribution_matched_at: new Date('2026-04-10T13:01:00.000Z'),
            attribution_reason: 'matched_by_landing_session',
            attribution_snapshot: {
              confidenceScore: 1,
              winner: {
                sessionId: '33333333-3333-4333-8333-333333333333',
                source: 'google',
                medium: 'cpc',
                campaign: 'brand-search',
                content: 'hero',
                term: 'widget',
                clickIdType: 'gclid',
                clickIdValue: 'gclid-123'
              }
            },
            attribution_snapshot_updated_at: new Date('2026-04-10T13:01:30.000Z'),
            ingested_at: new Date('2026-04-10T13:02:00.000Z'),
            raw_payload: { id: '1234567890' }
          }
        ],
        rowCount: 1
      };
    }

    if (text.includes('FROM shopify_order_line_items li')) {
      return {
        rows: [],
        rowCount: 0
      };
    }

    if (text.includes('FROM attribution_order_credits c')) {
      return {
        rows: [],
        rowCount: 0
      };
    }

    throw new Error(`Unexpected SQL in order details test: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/reporting/orders/1234567890');

    assert.equal(response.status, 200);
    assert.equal(body.order.shopifyOrderId, '1234567890');
    assert.equal(body.order.orderOccurredAtUtc, '2026-04-10T13:00:00.000Z');
    assert.equal(body.order.attributionTier, 'deterministic_first_party');
    assert.equal(body.order.attributionTierLabel, 'Deterministic first-party');
    assert.match(body.order.attributionTierDescription, /durable ROAS Radar first-party evidence/);
    assert.equal(body.order.attributionSource, 'landing_session_id');
    assert.equal(body.order.attributionMatchedAt, '2026-04-10T13:01:00.000Z');
    assert.equal(body.order.attributionReason, 'matched_by_landing_session');
    assert.equal(body.order.confidenceScore, 1);
    assert.equal(body.order.sessionId, '33333333-3333-4333-8333-333333333333');
    assert.equal(body.order.attributedSource, 'google');
    assert.equal(body.order.attributedMedium, 'cpc');
    assert.equal(body.order.attributedCampaign, 'brand-search');
    assert.equal(body.order.attributedContent, 'hero');
    assert.equal(body.order.attributedTerm, 'widget');
    assert.equal(body.order.attributedClickIdType, 'gclid');
    assert.equal(body.order.attributedClickIdValue, 'gclid-123');
    assert.equal(body.order.attributionSnapshotUpdatedAt, '2026-04-10T13:01:30.000Z');
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
