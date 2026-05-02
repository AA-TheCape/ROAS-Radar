import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

import { buildRawPayloadFixture } from './integration-test-helpers.js';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';
process.env.SHOPIFY_APP_API_SECRET ??= 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET ??= 'test-webhook-secret';

let cachedModules:
  | {
      pool: typeof import('../src/db/pool.js').pool;
      createServer: typeof import('../src/server.js').createServer;
      closeServer: typeof import('../src/server.js').closeServer;
      enqueueAttributionForOrder: typeof import('../src/modules/attribution/index.js').enqueueAttributionForOrder;
      processAttributionQueue: typeof import('../src/modules/attribution/index.js').processAttributionQueue;
      enqueueShopifyOrderWriteback: typeof import('../src/modules/shopify/writeback.js').enqueueShopifyOrderWriteback;
      processShopifyOrderWritebackQueue: typeof import('../src/modules/shopify/writeback.js').processShopifyOrderWritebackQueue;
      testUtils: typeof import('../src/modules/shopify/writeback.js').__shopifyWritebackTestUtils;
      resetE2EDatabase: typeof import('./e2e-harness.js').resetE2EDatabase;
    }
  | null = null;

async function getModules() {
  if (cachedModules) {
    return cachedModules;
  }

  const [poolModule, serverModule, attributionModule, writebackModule, harnessModule] = await Promise.all([
    import('../src/db/pool.js'),
    import('../src/server.js'),
    import('../src/modules/attribution/index.js'),
    import('../src/modules/shopify/writeback.js'),
    import('./e2e-harness.js')
  ]);

  cachedModules = {
    pool: poolModule.pool,
    createServer: serverModule.createServer,
    closeServer: serverModule.closeServer,
    enqueueAttributionForOrder: attributionModule.enqueueAttributionForOrder,
    processAttributionQueue: attributionModule.processAttributionQueue,
    enqueueShopifyOrderWriteback: writebackModule.enqueueShopifyOrderWriteback,
    processShopifyOrderWritebackQueue: writebackModule.processShopifyOrderWritebackQueue,
    testUtils: writebackModule.__shopifyWritebackTestUtils,
    resetE2EDatabase: harnessModule.resetE2EDatabase
  };

  return cachedModules;
}

function buildReportingHeaders(): Record<string, string> {
  return {
    authorization: 'Bearer test-reporting-token',
    accept: 'application/json'
  };
}

async function bootstrapSession(server: { address(): AddressInfo | null }) {
  const address = server.address() as AddressInfo;
  const response = await fetch(
    `http://127.0.0.1:${address.port}/track/session?pageUrl=${encodeURIComponent(
      'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123'
    )}&landingUrl=${encodeURIComponent(
      'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123'
    )}&referrerUrl=${encodeURIComponent('https://www.google.com/search?q=widget')}`,
    {
      headers: {
        accept: 'application/json',
        referer: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale'
      }
    }
  );

  assert.equal(response.status, 200);
  return (await response.json()) as { sessionId: string; isNewSession: boolean };
}

async function requestJson(server: { address(): AddressInfo | null }, path: string) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    headers: buildReportingHeaders()
  });

  return {
    response,
    body: (await response.json()) as Record<string, unknown>
  };
}

test.beforeEach(async () => {
  const { resetE2EDatabase, testUtils } = await getModules();
  testUtils.reset();
  await resetE2EDatabase();
});

test.after(async () => {
  const { pool, resetE2EDatabase } = await getModules();
  await resetE2EDatabase();
  await pool.end();
});

test('paid capture survives attribution, Shopify writeback, and reporting end to end', async () => {
  const {
    pool,
    createServer,
    closeServer,
    enqueueAttributionForOrder,
    processAttributionQueue,
    enqueueShopifyOrderWriteback,
    processShopifyOrderWritebackQueue,
    testUtils
  } = await getModules();

  testUtils.setWritebackProcessor(async () => undefined);
  const server = createServer();

  try {
    const bootstrap = await bootstrapSession(server);
    assert.equal(bootstrap.isNewSession, true);
    const reportingDate = new Date().toISOString().slice(0, 10);
    const orderProcessedAt = new Date(`${reportingDate}T12:15:00.000Z`);
    const orderFixture = buildRawPayloadFixture(
      {
        id: 'e2e-order-1',
        landing_session_id: bootstrap.sessionId
      },
      'e2e-order-1'
    );

    await pool.query(
      `
        INSERT INTO shopify_orders (
          shopify_order_id,
          currency_code,
          subtotal_price,
          total_price,
          processed_at,
          landing_session_id,
          source_name,
          payload_external_id,
          payload_size_bytes,
          payload_hash,
          raw_payload,
          ingested_at
        )
        VALUES (
          'e2e-order-1',
          'USD',
          '120.00',
          '120.00',
          $6,
          $1::uuid,
          'web',
          $2,
          $3,
          $4,
          $5::jsonb,
          now()
        )
      `,
      [
        bootstrap.sessionId,
        orderFixture.payloadExternalId,
        orderFixture.payloadSizeBytes,
        orderFixture.payloadHash,
        orderFixture.rawPayloadJson,
        orderProcessedAt.toISOString()
      ]
    );

    await enqueueAttributionForOrder('e2e-order-1', 'test_e2e');
    const attributionReport = await processAttributionQueue({
      workerId: 'test-e2e-attribution',
      limit: 10,
      staleScanLimit: 0,
      emitMetrics: false
    });

    assert.equal(attributionReport.succeededJobs, 1);
    assert.equal(attributionReport.failedJobs, 0);

    const attributionResult = await pool.query<{
      attributed_source: string | null;
      attributed_medium: string | null;
      attributed_campaign: string | null;
      attributed_click_id_type: string | null;
      attributed_click_id_value: string | null;
      attribution_reason: string;
    }>(
      `
        SELECT
          attributed_source,
          attributed_medium,
          attributed_campaign,
          attributed_click_id_type,
          attributed_click_id_value,
          attribution_reason
        FROM attribution_results
        WHERE shopify_order_id = 'e2e-order-1'
      `
    );

    assert.equal(attributionResult.rowCount, 1);
    assert.deepEqual(attributionResult.rows[0], {
      attributed_source: 'google',
      attributed_medium: 'cpc',
      attributed_campaign: 'spring-sale',
      attributed_click_id_type: 'gbraid',
      attributed_click_id_value: 'GBRAID-123',
      attribution_reason: 'matched_by_landing_session'
    });

    await enqueueShopifyOrderWriteback('e2e-order-1', 'test_e2e');
    const writebackReport = await processShopifyOrderWritebackQueue({
      workerId: 'test-e2e-writeback',
      limit: 10,
      now: new Date('2100-04-23T00:00:00.000Z')
    });

    assert.equal(writebackReport.completedJobs, 1);
    assert.equal(writebackReport.deadLetteredJobs, 0);

    const appliedWritebacks = testUtils.getAppliedWritebacks();
    assert.equal(appliedWritebacks.length, 1);
    assert.equal(appliedWritebacks[0].shopifyOrderId, 'e2e-order-1');
    assert.deepEqual(
      appliedWritebacks[0].attributes.filter((attribute) =>
        ['schema_version', 'roas_radar_session_id', 'utm_source', 'utm_medium', 'utm_campaign', 'gbraid'].includes(
          attribute.key
        )
      ),
      [
        { key: 'schema_version', value: '1' },
        { key: 'roas_radar_session_id', value: bootstrap.sessionId },
        { key: 'utm_source', value: 'google' },
        { key: 'utm_medium', value: 'cpc' },
        { key: 'utm_campaign', value: 'spring-sale' },
        { key: 'gbraid', value: 'GBRAID-123' }
      ]
    );

    const reportingSummary = await requestJson(
      server,
      `/api/reporting/summary?startDate=${reportingDate}&endDate=${reportingDate}&source=google&campaign=spring-sale`
    );

    assert.equal(reportingSummary.response.status, 200);
    assert.deepEqual(reportingSummary.body, {
      range: {
        startDate: reportingDate,
        endDate: reportingDate
      },
      totals: {
        visits: 0,
        orders: 1,
        revenue: 120,
        spend: 0,
        conversionRate: 0,
        roas: null
      }
    });
  } finally {
    await closeServer(server);
  }
});
