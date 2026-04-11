import assert from 'node:assert/strict';
import test, { afterEach } from 'node:test';
import type { AddressInfo } from 'node:net';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'reporting-token';
process.env.REPORTING_TENANT_ID = 'roas-radar';
process.env.REPORTING_API_SCOPES = 'reporting:read';

const { createApp } = await import('../src/app.js');
const { pool } = await import('../src/db/pool.js');

const originalQuery = pool.query.bind(pool);

afterEach(() => {
  pool.query = originalQuery;
});

function createServer() {
  const app = createApp();
  const server = app.listen(0);

  return server;
}

async function closeServer(server: ReturnType<typeof createServer>) {
  server.closeAllConnections?.();
  await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
}

function buildHeaders(overrides: Record<string, string> = {}): HeadersInit {
  return {
    authorization: 'Bearer reporting-token',
    'x-roas-radar-tenant-id': 'roas-radar',
    ...overrides
  };
}

test('reporting API rejects requests without tenant context', async () => {
  const server = createServer();

  try {
    const address = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${address.port}/api/reporting/models`, {
      headers: {
        authorization: 'Bearer reporting-token'
      }
    });
    const body = await response.json();

    assert.equal(response.status, 401);
    assert.equal(body.error.code, 'reporting_tenant_required');
  } finally {
    await closeServer(server);
  }
});

test('reporting overview returns versioned KPI payloads', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('SUM(new_customer_revenue)')) {
      return {
        rows: [
          {
            visits: '100',
            orders: '5',
            revenue: '525.00',
            spend: '175.00',
            clicks: '80',
            impressions: '1000',
            new_customer_orders: '3',
            returning_customer_orders: '2',
            new_customer_revenue: '300.00',
            returning_customer_revenue: '225.00'
          }
        ]
      };
    }

    throw new Error(`Unexpected SQL in overview test: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const address = server.address() as AddressInfo;
    const response = await fetch(
      `http://127.0.0.1:${address.port}/api/reporting/overview?startDate=2026-04-01&endDate=2026-04-10&attributionModel=linear`,
      {
        headers: buildHeaders()
      }
    );
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.equal(body.version, '2026-04-11');
    assert.equal(body.tenantId, 'roas-radar');
    assert.equal(body.attributionModel, 'linear');
    assert.equal(body.data.totals.visits, 100);
    assert.equal(body.data.totals.orders, 5);
    assert.equal(body.data.totals.roas, 3);
    assert.equal(body.data.totals.averageOrderValue, 105);
  } finally {
    await closeServer(server);
  }
});

test('reporting channels, campaigns, and creatives return paginated rows under a shared attribution model', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('GROUP BY source, medium') && text.includes('LIMIT $12')) {
      return {
        rows: [
          {
            source: 'google',
            medium: 'cpc',
            visits: '50',
            orders: '4',
            revenue: '400.00',
            spend: '100.00',
            clicks: '40',
            impressions: '500'
          },
          {
            source: 'facebook',
            medium: 'paid_social',
            visits: '30',
            orders: '2',
            revenue: '150.00',
            spend: '75.00',
            clicks: '20',
            impressions: '300'
          }
        ]
      };
    }

    if (text.includes('GROUP BY source, medium, campaign') && text.includes('LIMIT $13')) {
      return {
        rows: [
          {
            source: 'google',
            medium: 'cpc',
            campaign: 'spring-sale',
            visits: '35',
            orders: '3',
            revenue: '320.00',
            spend: '105.00',
            clicks: '30',
            impressions: '350'
          }
        ]
      };
    }

    if (text.includes('WITH attributed_groups AS') && text.includes('LIMIT $16')) {
      return {
        rows: [
          {
            source: 'google',
            medium: 'cpc',
            campaign: 'spring-sale',
            campaign_id: 'cmp_1',
            campaign_name: 'Spring Sale',
            ad_id: 'ad_1',
            ad_name: 'Hero Ad 1',
            creative_id: 'creative_1',
            creative_name: 'Hero Creative 1',
            content: 'hero-1',
            visits: '20',
            orders: '2',
            revenue: '220.00',
            spend: '70.00',
            clicks: '18',
            impressions: '200'
          },
          {
            source: 'google',
            medium: 'cpc',
            campaign: 'spring-sale',
            campaign_id: 'cmp_1',
            campaign_name: 'Spring Sale',
            ad_id: 'ad_2',
            ad_name: 'Hero Ad 2',
            creative_id: 'creative_2',
            creative_name: 'Hero Creative 2',
            content: 'hero-2',
            visits: '15',
            orders: '1',
            revenue: '100.00',
            spend: '35.00',
            clicks: '12',
            impressions: '150'
          },
          {
            source: 'facebook',
            medium: 'paid_social',
            campaign: 'retargeting',
            campaign_id: 'cmp_2',
            campaign_name: 'Retargeting',
            ad_id: 'ad_3',
            ad_name: 'Carousel Ad',
            creative_id: 'creative_3',
            creative_name: 'Carousel Creative',
            content: 'carousel-1',
            visits: '10',
            orders: '1',
            revenue: '80.00',
            spend: '20.00',
            clicks: '8',
            impressions: '120'
          }
        ]
      };
    }

    throw new Error(`Unexpected SQL in pagination test: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const address = server.address() as AddressInfo;
    const baseUrl = `http://127.0.0.1:${address.port}/api/reporting`;

    const channelsResponse = await fetch(`${baseUrl}/channels?startDate=2026-04-01&endDate=2026-04-10&limit=1`, {
      headers: buildHeaders()
    });
    const channelsBody = await channelsResponse.json();

    assert.equal(channelsResponse.status, 200);
    assert.equal(channelsBody.attributionModel, 'last_touch');
    assert.equal(channelsBody.data.rows.length, 1);
    assert.equal(channelsBody.data.rows[0].source, 'google');
    assert.ok(channelsBody.data.pagination.nextCursor);

    const campaignsResponse = await fetch(
      `${baseUrl}/campaigns?startDate=2026-04-01&endDate=2026-04-10&attributionModel=position_based&limit=1`,
      {
        headers: buildHeaders()
      }
    );
    const campaignsBody = await campaignsResponse.json();

    assert.equal(campaignsResponse.status, 200);
    assert.equal(campaignsBody.attributionModel, 'position_based');
    assert.equal(campaignsBody.data.rows.length, 1);
    assert.equal(campaignsBody.data.rows[0].campaign, 'spring-sale');
    assert.equal(campaignsBody.data.rows[0].roas, 320 / 105);
    assert.ok(campaignsBody.data.pagination.nextCursor);

    const creativesResponse = await fetch(
      `${baseUrl}/creatives?startDate=2026-04-01&endDate=2026-04-10&attributionModel=position_based&limit=2`,
      {
        headers: buildHeaders()
      }
    );
    const creativesBody = await creativesResponse.json();

    assert.equal(creativesResponse.status, 200);
    assert.equal(creativesBody.attributionModel, 'position_based');
    assert.equal(creativesBody.data.rows.length, 2);
    assert.equal(creativesBody.data.rows[0].campaign, 'spring-sale');
    assert.equal(creativesBody.data.rows[0].creativeId, 'creative_1');
    assert.equal(creativesBody.data.rows[0].creativeName, 'Hero Creative 1');
    assert.ok(creativesBody.data.pagination.nextCursor);
  } finally {
    await closeServer(server);
  }
});
