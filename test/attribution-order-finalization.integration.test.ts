import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';
process.env.SHOPIFY_APP_API_SECRET ??= 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET ??= 'test-webhook-secret';

async function getModules() {
  const poolModule = await import('../src/db/pool.js');
  const attributionModule = await import('../src/modules/attribution/index.js');

  return {
    pool: poolModule.pool,
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

test('order finalization persists a deterministic last non-direct winner snapshot with source touch event auditability', async () => {
  await resetIntegrationDatabase();
  const { pool, enqueueAttributionForOrder, processAttributionQueue } = await getModules();

  try {
    const paidSessionResult = await pool.query<{ id: string }>(
      `
        INSERT INTO tracking_sessions (
          first_seen_at,
          last_seen_at,
          landing_page,
          referrer_url,
          initial_utm_source,
          initial_utm_medium,
          initial_utm_campaign,
          initial_gclid
        )
        VALUES (
          '2026-04-01T10:00:00.000Z',
          '2026-04-01T10:10:00.000Z',
          'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search',
          'https://www.google.com/search?q=widget',
          'google',
          'cpc',
          'brand-search',
          'gclid-123'
        )
        RETURNING id::text
      `
    );
    const paidSessionId = paidSessionResult.rows[0].id;

    const paidEventResult = await pool.query<{ id: string }>(
      `
        INSERT INTO tracking_events (
          session_id,
          event_type,
          occurred_at,
          page_url,
          referrer_url,
          utm_source,
          utm_medium,
          utm_campaign,
          gclid,
          raw_payload
        )
        VALUES (
          $1::uuid,
          'page_view',
          '2026-04-01T10:00:00.000Z',
          'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search',
          'https://www.google.com/search?q=widget',
          'google',
          'cpc',
          'brand-search',
          'gclid-123',
          '{}'::jsonb
        )
        RETURNING id::text
      `,
      [paidSessionId]
    );
    const paidEventId = paidEventResult.rows[0].id;

    const directSessionResult = await pool.query<{ id: string }>(
      `
        INSERT INTO tracking_sessions (
          first_seen_at,
          last_seen_at,
          landing_page
        )
        VALUES (
          '2026-04-03T09:00:00.000Z',
          '2026-04-03T09:05:00.000Z',
          'https://store.example/cart'
        )
        RETURNING id::text
      `
    );
    const directSessionId = directSessionResult.rows[0].id;

    await pool.query(
      `
        INSERT INTO tracking_events (
          session_id,
          event_type,
          occurred_at,
          page_url,
          shopify_checkout_token,
          raw_payload
        )
        VALUES (
          $1::uuid,
          'checkout_started',
          '2026-04-03T09:00:00.000Z',
          'https://store.example/checkout',
          'checkout-direct-1',
          '{}'::jsonb
        )
      `,
      [directSessionId]
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
          checkout_token,
          source_name,
          raw_payload,
          ingested_at
        )
        VALUES (
          'order-finalization-1',
          'USD',
          '120.00',
          '120.00',
          '2026-04-03T09:05:00.000Z',
          $1::uuid,
          'checkout-direct-1',
          'web',
          '{"id":"order-finalization-1"}'::jsonb,
          now()
        )
      `,
      [paidSessionId]
    );

    await enqueueAttributionForOrder('order-finalization-1', 'test_order_finalization_snapshot');
    const queueResult = await processAttributionQueue({
      workerId: 'test-order-finalization',
      limit: 10,
      staleScanLimit: 0,
      emitMetrics: false
    });

    assert.equal(queueResult.succeededJobs, 1);
    assert.equal(queueResult.failedJobs, 0);

    const attributionResult = await pool.query<{
      session_id: string | null;
      attributed_source: string | null;
      attributed_medium: string | null;
      attributed_campaign: string | null;
      attribution_reason: string;
    }>(
      `
        SELECT
          session_id::text AS session_id,
          attributed_source,
          attributed_medium,
          attributed_campaign,
          attribution_reason
        FROM attribution_results
        WHERE shopify_order_id = 'order-finalization-1'
      `
    );

    assert.equal(attributionResult.rowCount, 1);
    assert.deepEqual(attributionResult.rows[0], {
      session_id: paidSessionId,
      attributed_source: 'google',
      attributed_medium: 'cpc',
      attributed_campaign: 'brand-search',
      attribution_reason: 'matched_by_landing_session'
    });

    const orderSnapshotResult = await pool.query<{ attribution_snapshot: Record<string, unknown> | null }>(
      `
        SELECT attribution_snapshot
        FROM shopify_orders
        WHERE shopify_order_id = 'order-finalization-1'
      `
    );

    const snapshot = orderSnapshotResult.rows[0].attribution_snapshot;
    assert.ok(snapshot);
    assert.deepEqual(snapshot?.winner, {
      sessionId: paidSessionId,
      sourceTouchEventId: paidEventId,
      occurredAt: '2026-04-01T10:00:00.000Z',
      source: 'google',
      medium: 'cpc',
      campaign: 'brand-search',
      content: null,
      term: null,
      clickIdType: 'gclid',
      clickIdValue: 'gclid-123',
      attributionReason: 'matched_by_landing_session',
      ingestionSource: 'landing_session_id',
      isDirect: false
    });
    assert.equal(Array.isArray(snapshot?.timeline), true);
    assert.equal((snapshot?.timeline as unknown[]).length, 2);
  } finally {
    await resetIntegrationDatabase();
  }
});

test.after(async () => {
  const { pool } = await getModules();
  await pool.end();
});
