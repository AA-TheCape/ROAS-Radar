import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';
process.env.SHOPIFY_APP_API_SECRET ??= 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET ??= 'test-webhook-secret';

async function getModules() {
  const poolModule = await import('../src/db/pool.js');
  const serverModule = await import('../src/server.js');
  const attributionModule = await import('../src/modules/attribution/index.js');

  return {
    pool: poolModule.pool,
    createServer: serverModule.createServer,
    closeServer: serverModule.closeServer,
    enqueueAttributionForOrder: attributionModule.enqueueAttributionForOrder,
    processAttributionQueue: attributionModule.processAttributionQueue
  };
}

async function resetIntegrationDatabase() {
  const { pool } = await getModules();

  await pool.query(`
    TRUNCATE TABLE
      attribution_jobs,
      shopify_order_writeback_jobs,
      attribution_order_credits,
      attribution_results,
      daily_reporting_metrics,
      order_attribution_links,
      session_attribution_touch_events,
      session_attribution_identities,
      shopify_order_line_items,
      shopify_orders,
      shopify_webhook_receipts,
      tracking_events,
      tracking_sessions,
      shopify_customers,
      customer_identities
    RESTART IDENTITY CASCADE
  `);
}

test('request-context bootstrap fallback preserves attributable revenue when the browser page beacon is missing', async () => {
  await resetIntegrationDatabase();
  const { pool, createServer, closeServer, enqueueAttributionForOrder, processAttributionQueue } = await getModules();
  const server = createServer();

  try {
    const address = server.address() as AddressInfo;
    const bootstrapResponse = await fetch(
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

    assert.equal(bootstrapResponse.status, 200);
    const bootstrapBody = (await bootstrapResponse.json()) as {
      sessionId: string;
      isNewSession: boolean;
    };

    assert.equal(bootstrapBody.isNewSession, true);

    const persistedTrackingEvent = await pool.query<{
      event_type: string;
      page_url: string | null;
      referrer_url: string | null;
      utm_source: string | null;
      utm_medium: string | null;
      utm_campaign: string | null;
      gbraid: string | null;
      ingestion_source: string;
      consent_state: string;
    }>(
      `
        SELECT
          event_type,
          page_url,
          referrer_url,
          utm_source,
          utm_medium,
          utm_campaign,
          gbraid,
          ingestion_source,
          consent_state
        FROM tracking_events
        WHERE session_id = $1::uuid
      `,
      [bootstrapBody.sessionId]
    );

    assert.equal(persistedTrackingEvent.rowCount, 1);
    assert.deepEqual(persistedTrackingEvent.rows[0], {
      event_type: 'page_view',
      page_url: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123',
      referrer_url: 'https://www.google.com/search?q=widget',
      utm_source: 'google',
      utm_medium: 'cpc',
      utm_campaign: 'spring-sale',
      gbraid: 'GBRAID-123',
      ingestion_source: 'request_query',
      consent_state: 'unknown'
    });

    const persistedTouchEvent = await pool.query<{ ingestion_source: string; consent_state: string }>(
      `
        SELECT ingestion_source, consent_state
        FROM session_attribution_touch_events
        WHERE roas_radar_session_id = $1::uuid
      `,
      [bootstrapBody.sessionId]
    );

    assert.equal(persistedTouchEvent.rowCount, 1);
    assert.equal(persistedTouchEvent.rows[0].ingestion_source, 'request_query');
    assert.equal(persistedTouchEvent.rows[0].consent_state, 'unknown');

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
          raw_payload,
          ingested_at
        )
        VALUES (
          $1,
          'USD',
          '120.00',
          '120.00',
          $2::timestamptz,
          $3::uuid,
          'web',
          $4::jsonb,
          now()
        )
      `,
      [
        'fallback-order-1',
        '2026-04-23T12:15:00.000Z',
        bootstrapBody.sessionId,
        JSON.stringify({
          id: 'fallback-order-1',
          landing_session_id: bootstrapBody.sessionId
        })
      ]
    );

    await enqueueAttributionForOrder('fallback-order-1', 'test_request_context_fallback');
    const queueResult = await processAttributionQueue({
      workerId: 'test-request-context-fallback',
      limit: 10,
      staleScanLimit: 0,
      emitMetrics: false
    });

    assert.equal(queueResult.succeededJobs, 1);
    assert.equal(queueResult.failedJobs, 0);

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
        WHERE shopify_order_id = $1
      `,
      ['fallback-order-1']
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

    const reportingRow = await pool.query<{ attributed_orders: number; attributed_revenue: number }>(
      `
        SELECT
          attributed_orders::float8 AS attributed_orders,
          attributed_revenue::float8 AS attributed_revenue
        FROM daily_reporting_metrics
        WHERE metric_date = $1::date
          AND attribution_model = 'last_touch'
          AND source = 'google'
          AND medium = 'cpc'
          AND campaign = 'spring-sale'
      `,
      ['2026-04-23']
    );

    assert.equal(reportingRow.rowCount, 1);
    assert.equal(reportingRow.rows[0].attributed_orders, 1);
    assert.equal(reportingRow.rows[0].attributed_revenue, 120);
  } finally {
    await closeServer(server);
    await resetIntegrationDatabase();
  }
});

test.after(async () => {
  const { pool } = await getModules();
  await pool.end();
});
