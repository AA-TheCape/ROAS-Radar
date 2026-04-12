import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;

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
  await pool.query('TRUNCATE daily_campaign_metrics');
  await pool.query(
    `INSERT INTO daily_campaign_metrics (
      metric_date,
      source,
      medium,
      campaign,
      content,
      visits,
      orders,
      revenue,
      last_computed_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())`,
    ['2026-04-10', 'google', 'cpc', 'spring-sale', 'hero-ad-1', 42, 3, '390.00']
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
        conversionRate: 3 / 42,
        roas: null
      }
    });
  } finally {
    await closeServer(server);
    await pool.query('TRUNCATE daily_campaign_metrics');
  }
});

test.after(async () => {
  await pool.end();
});
