import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

import { resetE2EDatabase } from './e2e-harness.ts';

const EMPTY_JSON_PAYLOAD = '{}';
const EMPTY_JSON_PAYLOAD_HASH = createHash('sha256').update(EMPTY_JSON_PAYLOAD).digest('hex');
const EMPTY_JSON_PAYLOAD_SIZE = Buffer.byteLength(EMPTY_JSON_PAYLOAD, 'utf8');

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  const { pool } = await import('../src/db/pool.js');
  await pool.end();
});

test('identity graph ingestion dual-writes canonical ids onto rollout surfaces', async () => {
  const sessionId = '123e4567-e89b-42d3-a456-426614174201';
  const checkoutToken = 'co-dual-write-1';
  const cartToken = 'ca-dual-write-1';
  const shopifyCustomerId = 'sc-dual-write-1';
  const shopifyOrderId = 'so-dual-write-1';
  const email = 'buyer@example.com';

  const [{ pool, withTransaction }, { ingestIdentityEdges, hashIdentityEmail }] = await Promise.all([
    import('../src/db/pool.js'),
    import('../src/modules/identity/index.js')
  ]);
  const emailHash = hashIdentityEmail(email);

  await pool.query(
    `
      INSERT INTO tracking_sessions (
        id,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      )
      VALUES ($1::uuid, $2::timestamptz, $2::timestamptz, $2::timestamptz, $2::timestamptz)
    `,
    [sessionId, '2026-04-24T12:00:00.000Z']
  );

  await pool.query(
    `
      INSERT INTO tracking_events (
        id,
        session_id,
        event_type,
        occurred_at,
        shopify_cart_token,
        shopify_checkout_token,
        raw_payload,
        ingested_at,
        payload_source,
        payload_received_at,
        payload_size_bytes,
        payload_hash
      )
      VALUES (
        '223e4567-e89b-42d3-a456-426614174201'::uuid,
        $1::uuid,
        'checkout_started',
        $2::timestamptz,
        $3,
        $4,
        $5::jsonb,
        $2::timestamptz,
        'browser',
        $2::timestamptz,
        $6,
        $7
      )
    `,
    [
      sessionId,
      '2026-04-24T12:05:00.000Z',
      cartToken,
      checkoutToken,
      EMPTY_JSON_PAYLOAD,
      EMPTY_JSON_PAYLOAD_SIZE,
      EMPTY_JSON_PAYLOAD_HASH
    ]
  );

  await pool.query(
    `
      INSERT INTO session_attribution_identities (
        roas_radar_session_id,
        first_captured_at,
        last_captured_at,
        retained_until,
        created_at,
        updated_at
      )
      VALUES (
        $1::uuid,
        $2::timestamptz,
        $2::timestamptz,
        $3::timestamptz,
        $2::timestamptz,
        $2::timestamptz
      )
    `,
    [sessionId, '2026-04-24T12:00:00.000Z', '2026-05-24T12:00:00.000Z']
  );

  await pool.query(
    `
      INSERT INTO shopify_customers (
        shopify_customer_id,
        email,
        email_hash,
        created_at,
        updated_at
      )
      VALUES ($1, null, $2, $3::timestamptz, $3::timestamptz)
    `,
    [shopifyCustomerId, emailHash, '2026-04-24T12:10:00.000Z']
  );

  await pool.query(
    `
      INSERT INTO shopify_orders (
        shopify_order_id,
        shopify_customer_id,
        email,
        email_hash,
        currency_code,
        total_price,
        processed_at,
        created_at_shopify,
        updated_at_shopify,
        landing_session_id,
        checkout_token,
        cart_token,
        raw_payload,
        ingested_at,
        payload_source,
        payload_received_at,
        payload_size_bytes,
        payload_hash
      )
      VALUES (
        $1,
        $2,
        null,
        $3,
        'USD',
        100.00,
        $4::timestamptz,
        $4::timestamptz,
        $4::timestamptz,
        $5::uuid,
        $6,
        $7,
        $8::jsonb,
        $4::timestamptz,
        'shopify_order',
        $4::timestamptz,
        $9,
        $10
      )
    `,
    [
      shopifyOrderId,
      shopifyCustomerId,
      emailHash,
      '2026-04-24T12:15:00.000Z',
      sessionId,
      checkoutToken,
      cartToken,
      EMPTY_JSON_PAYLOAD,
      EMPTY_JSON_PAYLOAD_SIZE,
      EMPTY_JSON_PAYLOAD_HASH
    ]
  );

  const result = await withTransaction((client) =>
    ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-24T12:20:00.000Z',
      evidenceSource: 'shopify_order_webhook',
      sourceTable: 'shopify_orders',
      sourceRecordId: shopifyOrderId,
      idempotencyKey: 'identity-dual-write-1',
      sessionId,
      checkoutToken,
      cartToken,
      shopifyCustomerId,
      email
    })
  );

  assert.equal(result.outcome, 'linked');
  assert.equal(result.linkedSessionIds.includes(sessionId), true);
  const state = await pool.query<{
    tracking_session_journey_id: string | null;
    tracking_session_customer_identity_id: string | null;
    tracking_event_journey_id: string | null;
    tracking_event_customer_identity_id: string | null;
    session_identity_journey_id: string | null;
    session_identity_customer_identity_id: string | null;
    order_journey_id: string | null;
    order_customer_identity_id: string | null;
    customer_journey_id: string | null;
    customer_customer_identity_id: string | null;
    compatibility_identity_id: string | null;
    compatibility_email_hash: string | null;
    compatibility_shopify_customer_id: string | null;
  }>(
    `
      SELECT
        sessions.identity_journey_id::text AS tracking_session_journey_id,
        sessions.customer_identity_id::text AS tracking_session_customer_identity_id,
        events.identity_journey_id::text AS tracking_event_journey_id,
        events.customer_identity_id::text AS tracking_event_customer_identity_id,
        captured.identity_journey_id::text AS session_identity_journey_id,
        captured.customer_identity_id::text AS session_identity_customer_identity_id,
        orders.identity_journey_id::text AS order_journey_id,
        orders.customer_identity_id::text AS order_customer_identity_id,
        customers.identity_journey_id::text AS customer_journey_id,
        customers.customer_identity_id::text AS customer_customer_identity_id,
        identities.id::text AS compatibility_identity_id,
        identities.hashed_email AS compatibility_email_hash,
        identities.shopify_customer_id AS compatibility_shopify_customer_id
      FROM tracking_sessions sessions
      INNER JOIN tracking_events events
        ON events.session_id = sessions.id
      INNER JOIN session_attribution_identities captured
        ON captured.roas_radar_session_id = sessions.id
      INNER JOIN shopify_orders orders
        ON orders.shopify_order_id = $2
      INNER JOIN shopify_customers customers
        ON customers.shopify_customer_id = $3
      INNER JOIN customer_identities identities
        ON identities.id = sessions.customer_identity_id
      WHERE sessions.id = $1::uuid
    `,
    [sessionId, shopifyOrderId, shopifyCustomerId]
  );

  assert.equal(state.rowCount, 1);
  assert.deepEqual(state.rows[0], {
    tracking_session_journey_id: result.journeyId,
    tracking_session_customer_identity_id: result.journeyId,
    tracking_event_journey_id: result.journeyId,
    tracking_event_customer_identity_id: result.journeyId,
    session_identity_journey_id: result.journeyId,
    session_identity_customer_identity_id: result.journeyId,
    order_journey_id: result.journeyId,
    order_customer_identity_id: result.journeyId,
    customer_journey_id: result.journeyId,
    customer_customer_identity_id: result.journeyId,
    compatibility_identity_id: result.journeyId,
    compatibility_email_hash: emailHash,
    compatibility_shopify_customer_id: shopifyCustomerId
  });
});

test('identity graph dual-write keeps attribution fallback compatible during rollout', async () => {
  const sessionId = '123e4567-e89b-42d3-a456-426614174202';
  const shopifyOrderId = 'so-dual-write-fallback-1';
  const shopifyCustomerId = 'sc-dual-write-fallback-1';
  const email = 'fallback@example.com';

  const [{ pool, withTransaction }, { ingestIdentityEdges, hashIdentityEmail }, attributionModule] = await Promise.all([
    import('../src/db/pool.js'),
    import('../src/modules/identity/index.js'),
    import('../src/modules/attribution/index.js')
  ]);
  const emailHash = hashIdentityEmail(email);

  await pool.query(
    `
      INSERT INTO tracking_sessions (
        id,
        first_seen_at,
        last_seen_at,
        landing_page,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        created_at,
        updated_at
      )
      VALUES (
        $1::uuid,
        '2026-04-24T12:00:00.000Z',
        '2026-04-24T12:05:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=identity-fallback',
        'google',
        'cpc',
        'identity-fallback',
        '2026-04-24T12:00:00.000Z',
        '2026-04-24T12:00:00.000Z'
      )
    `,
    [sessionId]
  );

  await pool.query(
    `
      INSERT INTO tracking_events (
        id,
        session_id,
        event_type,
        occurred_at,
        page_url,
        utm_source,
        utm_medium,
        utm_campaign,
        raw_payload,
        ingested_at,
        payload_source,
        payload_received_at,
        payload_size_bytes,
        payload_hash
      )
      VALUES (
        '223e4567-e89b-42d3-a456-426614174202'::uuid,
        $1::uuid,
        'page_view',
        '2026-04-24T12:00:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=identity-fallback',
        'google',
        'cpc',
        'identity-fallback',
        $2::jsonb,
        '2026-04-24T12:00:00.000Z',
        'browser',
        '2026-04-24T12:00:00.000Z',
        $3,
        $4
      )
    `,
    [sessionId, EMPTY_JSON_PAYLOAD, EMPTY_JSON_PAYLOAD_SIZE, EMPTY_JSON_PAYLOAD_HASH]
  );

  await pool.query(
    `
      INSERT INTO shopify_orders (
        shopify_order_id,
        shopify_customer_id,
        email,
        email_hash,
        currency_code,
        total_price,
        processed_at,
        created_at_shopify,
        updated_at_shopify,
        raw_payload,
        ingested_at,
        payload_source,
        payload_received_at,
        payload_size_bytes,
        payload_hash
      )
      VALUES (
        $1,
        $2,
        null,
        $3,
        'USD',
        100.00,
        '2026-04-24T12:15:00.000Z',
        '2026-04-24T12:15:00.000Z',
        '2026-04-24T12:15:00.000Z',
        $4::jsonb,
        '2026-04-24T12:15:00.000Z',
        'shopify_order',
        '2026-04-24T12:15:00.000Z',
        $5,
        $6
      )
    `,
    [shopifyOrderId, shopifyCustomerId, emailHash, EMPTY_JSON_PAYLOAD, EMPTY_JSON_PAYLOAD_SIZE, EMPTY_JSON_PAYLOAD_HASH]
  );

  const stitched = await withTransaction((client) =>
    ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-24T12:20:00.000Z',
      evidenceSource: 'shopify_order_webhook',
      sourceTable: 'shopify_orders',
      sourceRecordId: shopifyOrderId,
      idempotencyKey: 'identity-dual-write-fallback',
      sessionId,
      shopifyCustomerId,
      email
    })
  );

  await attributionModule.enqueueAttributionForOrder(shopifyOrderId, 'integration_test');
  const queueResult = await attributionModule.processAttributionQueue({
    workerId: 'test-identity-fallback',
    limit: 10,
    staleScanLimit: 0,
    emitMetrics: false
  });

  assert.equal(stitched.outcome, 'linked');
  assert.equal(queueResult.succeededJobs, 1);
  assert.equal(queueResult.failedJobs, 0);

  const [attributionResult, orderState] = await Promise.all([
    pool.query<{
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
        WHERE shopify_order_id = $1
      `,
      [shopifyOrderId]
    ),
    pool.query<{
      identity_journey_id: string | null;
      customer_identity_id: string | null;
    }>(
      `
        SELECT
          identity_journey_id::text AS identity_journey_id,
          customer_identity_id::text AS customer_identity_id
        FROM shopify_orders
        WHERE shopify_order_id = $1
      `,
      [shopifyOrderId]
    )
  ]);

  assert.deepEqual(attributionResult.rows[0], {
    session_id: sessionId,
    attributed_source: 'google',
    attributed_medium: 'cpc',
    attributed_campaign: 'identity-fallback',
    attribution_reason: 'matched_by_customer_identity'
  });
  assert.deepEqual(orderState.rows[0], {
    identity_journey_id: stitched.journeyId,
    customer_identity_id: stitched.journeyId
  });
});
