import assert from 'node:assert/strict';
import test from 'node:test';
import type { Pool } from 'pg';

import { buildRawPayloadFixture, resetIntegrationTables } from './integration-test-helpers.js';

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

type TrackingSessionInput = {
  firstSeenAt: string;
  lastSeenAt?: string;
  landingPage?: string | null;
  referrerUrl?: string | null;
  customerIdentityId?: string | null;
  utmSource?: string | null;
  utmMedium?: string | null;
  utmCampaign?: string | null;
  utmContent?: string | null;
  utmTerm?: string | null;
  gclid?: string | null;
  gbraid?: string | null;
  wbraid?: string | null;
  fbclid?: string | null;
  ttclid?: string | null;
  msclkid?: string | null;
};

type TrackingEventInput = {
  sessionId: string;
  eventType: string;
  occurredAt: string;
  pageUrl?: string | null;
  referrerUrl?: string | null;
  utmSource?: string | null;
  utmMedium?: string | null;
  utmCampaign?: string | null;
  utmContent?: string | null;
  utmTerm?: string | null;
  gclid?: string | null;
  gbraid?: string | null;
  wbraid?: string | null;
  fbclid?: string | null;
  ttclid?: string | null;
  msclkid?: string | null;
  shopifyCheckoutToken?: string | null;
  shopifyCartToken?: string | null;
};

type ShopifyOrderInput = {
  shopifyOrderId: string;
  processedAt: string;
  totalPrice?: string;
  subtotalPrice?: string;
  landingSessionId?: string | null;
  checkoutToken?: string | null;
  cartToken?: string | null;
  customerIdentityId?: string | null;
  sourceName?: string;
  rawPayload?: string;
};

async function insertCustomerIdentity(pool: Pool, identityId: string): Promise<void> {
  await pool.query(
    `
      INSERT INTO customer_identities (
        id,
        hashed_email,
        created_at,
        updated_at,
        last_stitched_at
      )
      VALUES (
        $1::uuid,
        $2,
        now(),
        now(),
        now()
      )
      ON CONFLICT (id) DO NOTHING
    `,
    [identityId, `${identityId}@example.com`]
  );
}

async function insertTrackingSession(pool: Pool, input: TrackingSessionInput): Promise<string> {
  const result = await pool.query<{ id: string }>(
    `
      INSERT INTO tracking_sessions (
        first_seen_at,
        last_seen_at,
        landing_page,
        referrer_url,
        customer_identity_id,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_utm_content,
        initial_utm_term,
        initial_gclid,
        initial_gbraid,
        initial_wbraid,
        initial_fbclid,
        initial_ttclid,
        initial_msclkid
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5::uuid,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16
      )
      RETURNING id::text
    `,
    [
      input.firstSeenAt,
      input.lastSeenAt ?? input.firstSeenAt,
      input.landingPage ?? null,
      input.referrerUrl ?? null,
      input.customerIdentityId ?? null,
      input.utmSource ?? null,
      input.utmMedium ?? null,
      input.utmCampaign ?? null,
      input.utmContent ?? null,
      input.utmTerm ?? null,
      input.gclid ?? null,
      input.gbraid ?? null,
      input.wbraid ?? null,
      input.fbclid ?? null,
      input.ttclid ?? null,
      input.msclkid ?? null
    ]
  );

  return result.rows[0].id;
}

async function insertTrackingEvent(pool: Pool, input: TrackingEventInput): Promise<string> {
  const rawPayloadFixture = buildRawPayloadFixture({});
  const result = await pool.query<{ id: string }>(
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
        utm_content,
        utm_term,
        gclid,
        gbraid,
        wbraid,
        fbclid,
        ttclid,
        msclkid,
        shopify_checkout_token,
        shopify_cart_token,
        payload_size_bytes,
        payload_hash,
        raw_payload
      )
      VALUES (
        $1::uuid,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16,
        $17,
        $18,
        $19,
        $20,
        $21::jsonb
      )
      RETURNING id::text
    `,
    [
      input.sessionId,
      input.eventType,
      input.occurredAt,
      input.pageUrl ?? null,
      input.referrerUrl ?? null,
      input.utmSource ?? null,
      input.utmMedium ?? null,
      input.utmCampaign ?? null,
      input.utmContent ?? null,
      input.utmTerm ?? null,
      input.gclid ?? null,
      input.gbraid ?? null,
      input.wbraid ?? null,
      input.fbclid ?? null,
      input.ttclid ?? null,
      input.msclkid ?? null,
      input.shopifyCheckoutToken ?? null,
      input.shopifyCartToken ?? null,
      rawPayloadFixture.payloadSizeBytes,
      rawPayloadFixture.payloadHash,
      rawPayloadFixture.rawPayloadJson
    ]
  );

  return result.rows[0].id;
}

async function insertShopifyOrder(pool: Pool, input: ShopifyOrderInput): Promise<void> {
  const rawPayloadJson = input.rawPayload ?? JSON.stringify({ id: input.shopifyOrderId });
  const orderFixture = buildRawPayloadFixture(JSON.parse(rawPayloadJson) as Record<string, unknown>, input.shopifyOrderId);

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
        cart_token,
        customer_identity_id,
        source_name,
        payload_external_id,
        payload_size_bytes,
        payload_hash,
        raw_payload,
        ingested_at
      )
      VALUES (
        $1,
        'USD',
        $2,
        $3,
        $4,
        $5::uuid,
        $6,
        $7,
        $8::uuid,
        $9,
        $10,
        $11,
        $12,
        $13::jsonb,
        now()
      )
    `,
    [
      input.shopifyOrderId,
      input.subtotalPrice ?? input.totalPrice ?? '120.00',
      input.totalPrice ?? '120.00',
      input.processedAt,
      input.landingSessionId ?? null,
      input.checkoutToken ?? null,
      input.cartToken ?? null,
      input.customerIdentityId ?? null,
      input.sourceName ?? 'web',
      orderFixture.payloadExternalId,
      orderFixture.payloadSizeBytes,
      orderFixture.payloadHash,
      orderFixture.rawPayloadJson
    ]
  );
}

async function processOrder(shopifyOrderId: string) {
  const { enqueueAttributionForOrder, processAttributionQueue } = await getModules();
  await enqueueAttributionForOrder(shopifyOrderId, 'integration_test');

  const queueResult = await processAttributionQueue({
    workerId: `test-${shopifyOrderId}`,
    limit: 10,
    staleScanLimit: 0,
    emitMetrics: false
  });

  assert.equal(queueResult.succeededJobs, 1);
  assert.equal(queueResult.failedJobs, 0);
}

async function fetchAttributionResult(shopifyOrderId: string) {
  const { pool } = await getModules();

  const result = await pool.query<{
    session_id: string | null;
    attributed_source: string | null;
    attributed_medium: string | null;
    attributed_campaign: string | null;
    attributed_click_id_type: string | null;
    attributed_click_id_value: string | null;
    confidence_score: string;
    attribution_reason: string;
  }>(
    `
      SELECT
        session_id::text AS session_id,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        attributed_click_id_type,
        attributed_click_id_value,
        confidence_score::text,
        attribution_reason
      FROM attribution_results
      WHERE shopify_order_id = $1
    `,
    [shopifyOrderId]
  );

  assert.equal(result.rowCount, 1);
  return result.rows[0];
}

async function fetchOrderSnapshot(shopifyOrderId: string) {
  const { pool } = await getModules();
  const result = await pool.query<{ attribution_snapshot: Record<string, unknown> | null }>(
    `
      SELECT attribution_snapshot
      FROM shopify_orders
      WHERE shopify_order_id = $1
    `,
    [shopifyOrderId]
  );

  return result.rows[0].attribution_snapshot;
}

async function fetchOrderAttributionAudit(shopifyOrderId: string) {
  const { pool } = await getModules();
  const result = await pool.query<{
    attribution_tier: string | null;
    attribution_source: string | null;
    attribution_matched_at: Date | null;
    attribution_reason: string | null;
  }>(
    `
      SELECT
        attribution_tier,
        attribution_source,
        attribution_matched_at,
        attribution_reason
      FROM shopify_orders
      WHERE shopify_order_id = $1
    `,
    [shopifyOrderId]
  );

  assert.equal(result.rowCount, 1);
  return result.rows[0];
}

async function resetIntegrationDatabase() {
  const { pool } = await getModules();

  await resetIntegrationTables(pool, [
    'attribution_jobs',
    'shopify_order_writeback_jobs',
    'attribution_order_credits',
    'attribution_results',
    'daily_reporting_metrics',
    'order_attribution_links',
    'session_attribution_touch_events',
    'session_attribution_identities',
    'shopify_order_line_items',
    'shopify_orders',
    'shopify_webhook_receipts',
    'tracking_events',
    'tracking_sessions',
    'shopify_customers',
    'customer_identities'
  ]);
}

test('order finalization persists a deterministic last non-direct winner snapshot with source touch event auditability', async () => {
  await resetIntegrationDatabase();
  const { pool, enqueueAttributionForOrder, processAttributionQueue } = await getModules();

  try {
    const emptyRawPayloadFixture = buildRawPayloadFixture({});
    const orderRawPayloadFixture = buildRawPayloadFixture({ id: 'order-finalization-1' }, 'order-finalization-1');
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
          payload_size_bytes,
          payload_hash,
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
          $2,
          $3,
          $4::jsonb
        )
        RETURNING id::text
      `,
      [
        paidSessionId,
        emptyRawPayloadFixture.payloadSizeBytes,
        emptyRawPayloadFixture.payloadHash,
        emptyRawPayloadFixture.rawPayloadJson
      ]
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
          payload_size_bytes,
          payload_hash,
          raw_payload
        )
        VALUES (
          $1::uuid,
          'checkout_started',
          '2026-04-03T09:00:00.000Z',
          'https://store.example/checkout',
          'checkout-direct-1',
          $2,
          $3,
          $4::jsonb
        )
      `,
      [
        directSessionId,
        emptyRawPayloadFixture.payloadSizeBytes,
        emptyRawPayloadFixture.payloadHash,
        emptyRawPayloadFixture.rawPayloadJson
      ]
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
          payload_external_id,
          payload_size_bytes,
          payload_hash,
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
          $2,
          $3,
          $4,
          $5::jsonb,
          now()
        )
      `,
      [
        paidSessionId,
        orderRawPayloadFixture.payloadExternalId,
        orderRawPayloadFixture.payloadSizeBytes,
        orderRawPayloadFixture.payloadHash,
        orderRawPayloadFixture.rawPayloadJson
      ]
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

    const orderAudit = await fetchOrderAttributionAudit('order-finalization-1');
    assert.equal(orderAudit.attribution_tier, 'deterministic_first_party');
    assert.equal(orderAudit.attribution_source, 'landing_session_id');
    assert.equal(orderAudit.attribution_reason, 'matched_by_landing_session');
    assert.ok(orderAudit.attribution_matched_at instanceof Date);

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

test('latest non-direct winner survives a multi-touch timeline with a later direct revisit', async () => {
  await resetIntegrationDatabase();
  const { pool } = await getModules();
  const customerIdentityId = '11111111-1111-4111-8111-111111111111';

  try {
    await insertCustomerIdentity(pool, customerIdentityId);

    const firstPaidSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-01T10:00:00.000Z',
      landingPage: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search',
      customerIdentityId,
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'brand-search',
      gclid: 'gclid-123'
    });
    await insertTrackingEvent(pool, {
      sessionId: firstPaidSessionId,
      eventType: 'page_view',
      occurredAt: '2026-04-01T10:00:00.000Z',
      pageUrl: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'brand-search',
      gclid: 'gclid-123'
    });

    const directSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-02T16:00:00.000Z',
      landingPage: 'https://store.example/cart',
      customerIdentityId
    });
    await insertTrackingEvent(pool, {
      sessionId: directSessionId,
      eventType: 'page_view',
      occurredAt: '2026-04-02T16:00:00.000Z',
      pageUrl: 'https://store.example/cart'
    });

    const latestPaidSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-04T08:00:00.000Z',
      landingPage: 'https://store.example/products/widget?utm_source=meta&utm_medium=paid_social&utm_campaign=retargeting',
      customerIdentityId,
      utmSource: 'meta',
      utmMedium: 'paid_social',
      utmCampaign: 'retargeting'
    });
    await insertTrackingEvent(pool, {
      sessionId: latestPaidSessionId,
      eventType: 'page_view',
      occurredAt: '2026-04-04T08:00:00.000Z',
      pageUrl: 'https://store.example/products/widget?utm_source=meta&utm_medium=paid_social&utm_campaign=retargeting',
      utmSource: 'meta',
      utmMedium: 'paid_social',
      utmCampaign: 'retargeting'
    });

    await insertShopifyOrder(pool, {
      shopifyOrderId: 'order-multi-touch-1',
      processedAt: '2026-04-04T08:15:00.000Z',
      customerIdentityId
    });

    await processOrder('order-multi-touch-1');

    const attributionResult = await fetchAttributionResult('order-multi-touch-1');
    assert.deepEqual(attributionResult, {
      session_id: latestPaidSessionId,
      attributed_source: 'meta',
      attributed_medium: 'paid_social',
      attributed_campaign: 'retargeting',
      attributed_click_id_type: null,
      attributed_click_id_value: null,
      confidence_score: '0.60',
      attribution_reason: 'matched_by_customer_identity'
    });

    const snapshot = await fetchOrderSnapshot('order-multi-touch-1');
    assert.ok(snapshot);
    assert.equal(Array.isArray(snapshot?.timeline), true);
    assert.equal((snapshot?.timeline as unknown[]).length, 3);
  } finally {
    await resetIntegrationDatabase();
  }
});

test('click-id-only identity touches stay non-direct and beat later direct revisits during attribution processing', async () => {
  await resetIntegrationDatabase();
  const { pool } = await getModules();
  const customerIdentityId = '22222222-2222-4222-8222-222222222222';

  try {
    await insertCustomerIdentity(pool, customerIdentityId);

    const clickOnlySessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-02T14:00:00.000Z',
      landingPage: 'https://store.example/products/widget?fbclid=fbclid-abc',
      customerIdentityId,
      fbclid: 'fbclid-abc'
    });
    await insertTrackingEvent(pool, {
      sessionId: clickOnlySessionId,
      eventType: 'page_view',
      occurredAt: '2026-04-02T14:00:00.000Z',
      pageUrl: 'https://store.example/products/widget?fbclid=fbclid-abc',
      fbclid: 'fbclid-abc'
    });

    const directSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-03T11:00:00.000Z',
      landingPage: 'https://store.example/cart',
      customerIdentityId
    });
    await insertTrackingEvent(pool, {
      sessionId: directSessionId,
      eventType: 'page_view',
      occurredAt: '2026-04-03T11:00:00.000Z',
      pageUrl: 'https://store.example/cart'
    });

    await insertShopifyOrder(pool, {
      shopifyOrderId: 'order-click-id-only-1',
      processedAt: '2026-04-03T12:00:00.000Z',
      customerIdentityId
    });

    await processOrder('order-click-id-only-1');

    const attributionResult = await fetchAttributionResult('order-click-id-only-1');
    assert.deepEqual(attributionResult, {
      session_id: clickOnlySessionId,
      attributed_source: null,
      attributed_medium: null,
      attributed_campaign: null,
      attributed_click_id_type: 'fbclid',
      attributed_click_id_value: 'fbclid-abc',
      confidence_score: '0.60',
      attribution_reason: 'matched_by_customer_identity'
    });
  } finally {
    await resetIntegrationDatabase();
  }
});

test('same-timestamp deterministic collisions prefer checkout token evidence over cart token evidence', async () => {
  await resetIntegrationDatabase();
  const { pool } = await getModules();

  try {
    const cartSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-05T15:00:00.000Z',
      landingPage: 'https://store.example/cart?utm_source=google&utm_medium=cpc&utm_campaign=cart-touch',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'cart-touch'
    });
    await insertTrackingEvent(pool, {
      sessionId: cartSessionId,
      eventType: 'page_view',
      occurredAt: '2026-04-05T15:00:00.000Z',
      pageUrl: 'https://store.example/cart?utm_source=google&utm_medium=cpc&utm_campaign=cart-touch',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'cart-touch',
      shopifyCartToken: 'cart-collision-1'
    });

    const checkoutSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-05T15:00:00.000Z',
      landingPage: 'https://store.example/checkout?utm_source=google&utm_medium=cpc&utm_campaign=checkout-touch',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'checkout-touch'
    });
    await insertTrackingEvent(pool, {
      sessionId: checkoutSessionId,
      eventType: 'checkout_started',
      occurredAt: '2026-04-05T15:00:00.000Z',
      pageUrl: 'https://store.example/checkout?utm_source=google&utm_medium=cpc&utm_campaign=checkout-touch',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'checkout-touch',
      shopifyCheckoutToken: 'checkout-collision-1'
    });

    await insertShopifyOrder(pool, {
      shopifyOrderId: 'order-source-precedence-1',
      processedAt: '2026-04-05T15:10:00.000Z',
      checkoutToken: 'checkout-collision-1',
      cartToken: 'cart-collision-1'
    });

    await processOrder('order-source-precedence-1');

    const attributionResult = await fetchAttributionResult('order-source-precedence-1');
    assert.equal(attributionResult.session_id, checkoutSessionId);
    assert.equal(attributionResult.attributed_campaign, 'checkout-touch');
    assert.equal(attributionResult.attribution_reason, 'matched_by_checkout_token');
  } finally {
    await resetIntegrationDatabase();
  }
});

test('same-session evidence is deduped before winner selection and keeps the strongest source', async () => {
  await resetIntegrationDatabase();
  const { pool } = await getModules();

  try {
    const sessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-06T10:00:00.000Z',
      landingPage: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'brand-search'
    });
    const firstEventId = await insertTrackingEvent(pool, {
      sessionId,
      eventType: 'page_view',
      occurredAt: '2026-04-06T10:00:00.000Z',
      pageUrl: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'brand-search'
    });
    await insertTrackingEvent(pool, {
      sessionId,
      eventType: 'checkout_started',
      occurredAt: '2026-04-06T10:10:00.000Z',
      pageUrl: 'https://store.example/checkout',
      shopifyCheckoutToken: 'checkout-dedupe-1'
    });

    await insertShopifyOrder(pool, {
      shopifyOrderId: 'order-dedupe-1',
      processedAt: '2026-04-06T10:15:00.000Z',
      landingSessionId: sessionId,
      checkoutToken: 'checkout-dedupe-1'
    });

    await processOrder('order-dedupe-1');

    const snapshot = await fetchOrderSnapshot('order-dedupe-1');
    assert.ok(snapshot);
    assert.equal(Array.isArray(snapshot?.timeline), true);
    assert.equal((snapshot?.timeline as unknown[]).length, 1);
    assert.deepEqual(snapshot?.winner, {
      sessionId,
      sourceTouchEventId: firstEventId,
      occurredAt: '2026-04-06T10:00:00.000Z',
      source: 'google',
      medium: 'cpc',
      campaign: 'brand-search',
      content: null,
      term: null,
      clickIdType: null,
      clickIdValue: null,
      attributionReason: 'matched_by_landing_session',
      ingestionSource: 'landing_session_id',
      isDirect: false
    });
  } finally {
    await resetIntegrationDatabase();
  }
});

test('out-of-window and future-dated candidates are excluded so in-window direct evidence can still win', async () => {
  await resetIntegrationDatabase();
  const { pool } = await getModules();
  const customerIdentityId = '33333333-3333-4333-8333-333333333333';

  try {
    await insertCustomerIdentity(pool, customerIdentityId);

    const oldPaidSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-03-05T09:00:00.000Z',
      landingPage: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=old-paid',
      customerIdentityId,
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'old-paid'
    });
    await insertTrackingEvent(pool, {
      sessionId: oldPaidSessionId,
      eventType: 'page_view',
      occurredAt: '2026-03-05T09:00:00.000Z',
      pageUrl: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=old-paid',
      utmSource: 'google',
      utmMedium: 'cpc',
      utmCampaign: 'old-paid'
    });

    const directSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-03T09:00:00.000Z',
      landingPage: 'https://store.example/cart',
      customerIdentityId
    });
    await insertTrackingEvent(pool, {
      sessionId: directSessionId,
      eventType: 'page_view',
      occurredAt: '2026-04-03T09:00:00.000Z',
      pageUrl: 'https://store.example/cart'
    });

    const futurePaidSessionId = await insertTrackingSession(pool, {
      firstSeenAt: '2026-04-03T09:06:00.000Z',
      landingPage: 'https://store.example/checkout?utm_source=meta&utm_medium=paid_social&utm_campaign=future-paid',
      utmSource: 'meta',
      utmMedium: 'paid_social',
      utmCampaign: 'future-paid'
    });
    await insertTrackingEvent(pool, {
      sessionId: futurePaidSessionId,
      eventType: 'checkout_started',
      occurredAt: '2026-04-03T09:06:00.000Z',
      pageUrl: 'https://store.example/checkout?utm_source=meta&utm_medium=paid_social&utm_campaign=future-paid',
      utmSource: 'meta',
      utmMedium: 'paid_social',
      utmCampaign: 'future-paid',
      shopifyCheckoutToken: 'checkout-future-1'
    });

    await insertShopifyOrder(pool, {
      shopifyOrderId: 'order-window-1',
      processedAt: '2026-04-03T09:05:00.000Z',
      checkoutToken: 'checkout-future-1',
      customerIdentityId
    });

    await processOrder('order-window-1');

    const attributionResult = await fetchAttributionResult('order-window-1');
    assert.deepEqual(attributionResult, {
      session_id: directSessionId,
      attributed_source: null,
      attributed_medium: null,
      attributed_campaign: null,
      attributed_click_id_type: null,
      attributed_click_id_value: null,
      confidence_score: '0.60',
      attribution_reason: 'matched_by_customer_identity'
    });
  } finally {
    await resetIntegrationDatabase();
  }
});

test('orders with no deterministic candidates persist an unattributed fallback snapshot and result row', async () => {
  await resetIntegrationDatabase();
  const { pool } = await getModules();

  try {
    await insertShopifyOrder(pool, {
      shopifyOrderId: 'order-unattributed-1',
      processedAt: '2026-04-07T09:05:00.000Z'
    });

    await processOrder('order-unattributed-1');

    const attributionResult = await fetchAttributionResult('order-unattributed-1');
    assert.deepEqual(attributionResult, {
      session_id: null,
      attributed_source: null,
      attributed_medium: null,
      attributed_campaign: null,
      attributed_click_id_type: null,
      attributed_click_id_value: null,
      confidence_score: '0.00',
      attribution_reason: 'unattributed'
    });

    const orderAudit = await fetchOrderAttributionAudit('order-unattributed-1');
    assert.equal(orderAudit.attribution_tier, 'unattributed');
    assert.equal(orderAudit.attribution_source, 'unattributed');
    assert.equal(orderAudit.attribution_reason, 'unattributed');
    assert.ok(orderAudit.attribution_matched_at instanceof Date);

    const snapshot = await fetchOrderSnapshot('order-unattributed-1');
    assert.ok(snapshot);
    assert.equal(snapshot?.confidenceScore, 0);
    assert.equal(snapshot?.winner, null);
    assert.deepEqual(snapshot?.timeline, []);
  } finally {
    await resetIntegrationDatabase();
  }
});

test('shopify_orders attribution tier constraint rejects unsupported values', async () => {
  await resetIntegrationDatabase();
  const { pool } = await getModules();

  try {
    await insertShopifyOrder(pool, {
      shopifyOrderId: 'order-invalid-tier-1',
      processedAt: '2026-04-11T10:00:00.000Z'
    });

    await assert.rejects(
      () =>
        pool.query(
          `
            UPDATE shopify_orders
            SET attribution_tier = 'invalid_tier'
            WHERE shopify_order_id = 'order-invalid-tier-1'
          `
        ),
      /shopify_orders_attribution_tier_chk/
    );
  } finally {
    await resetIntegrationDatabase();
  }
});

test.after(async () => {
  const { pool } = await getModules();
  await pool.end();
});
