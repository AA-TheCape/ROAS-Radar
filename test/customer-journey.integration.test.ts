import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const poolModule = await import('../src/db/pool.js');
const identityModule = await import('../src/modules/identity/index.js');

const { pool } = poolModule;
const { ingestIdentityEdges } = identityModule;

async function truncateJourneyFixtures() {
  await pool.query(`
    TRUNCATE TABLE
      customer_journey,
      identity_edge_ingestion_runs,
      identity_edges,
      identity_nodes,
      identity_journeys,
      session_attribution_touch_events,
      session_attribution_identities,
      shopify_order_line_items,
      shopify_orders,
      tracking_events,
      tracking_sessions,
      shopify_customers,
      customer_identities
    RESTART IDENTITY CASCADE
  `);
}

test('customer_journey refreshes incrementally and preserves reproducible canonical journey ordering', async () => {
  await truncateJourneyFixtures();

  const client = await pool.connect();
  const emailHash = 'a'.repeat(64);
  let transactionOpen = false;

  try {
    const sessionOneId = '11111111-1111-4111-8111-111111111111';
    const sessionTwoId = '22222222-2222-4222-8222-222222222222';

    await client.query('BEGIN');
    transactionOpen = true;
    await client.query(
      `
        INSERT INTO tracking_sessions (
          id,
          first_seen_at,
          last_seen_at,
          landing_page,
          referrer_url,
          initial_utm_source,
          initial_utm_medium,
          initial_utm_campaign
        )
        VALUES (
          $1::uuid,
          '2026-04-25T10:00:00.000Z',
          '2026-04-25T10:01:00.000Z',
          'https://store.example.com/?utm_source=google&utm_medium=cpc&utm_campaign=spring',
          'https://www.google.com/',
          'google',
          'cpc',
          'spring'
        )
      `,
      [sessionOneId]
    );
    await client.query(
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
          shopify_checkout_token,
          payload_size_bytes,
          raw_payload
        )
        VALUES (
          $1::uuid,
          'checkout_started',
          '2026-04-25T10:00:30.000Z',
          'https://store.example.com/checkout',
          'https://www.google.com/',
          'google',
          'cpc',
          'spring',
          'co-1',
          2,
          '{}'::jsonb
        )
      `,
      [sessionOneId]
    );
    await ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-25T10:00:30.000Z',
      evidenceSource: 'tracking_event',
      sourceTable: 'tracking_events',
      sourceRecordId: 'evt-session-1',
      idempotencyKey: 'customer-journey-session-1',
      sessionId: sessionOneId,
      checkoutToken: 'co-1'
    });
    await client.query('COMMIT');
    transactionOpen = false;

    const firstJourneyResult = await pool.query<{ identity_journey_id: string }>(
      `
        SELECT identity_journey_id::text AS identity_journey_id
        FROM tracking_sessions
        WHERE id = $1::uuid
      `,
      [sessionOneId]
    );
    const firstJourneyId = firstJourneyResult.rows[0]?.identity_journey_id ?? null;
    assert.ok(firstJourneyId);

    const initialJourneyRow = await pool.query(
      `
        SELECT
          identity_journey_id::text AS identity_journey_id,
          journey_session_number,
          journey_session_count,
          session_event_count,
          session_order_count,
          checkout_started_count,
          is_converting_session,
          utm_source
        FROM customer_journey
        WHERE session_id = $1::uuid
      `,
      [sessionOneId]
    );
    assert.deepEqual(initialJourneyRow.rows[0], {
      identity_journey_id: firstJourneyId,
      journey_session_number: 1,
      journey_session_count: 1,
      session_event_count: 1,
      session_order_count: 0,
      checkout_started_count: 1,
      is_converting_session: false,
      utm_source: 'google'
    });

    await client.query('BEGIN');
    transactionOpen = true;
    await client.query(
      `
        INSERT INTO shopify_orders (
          shopify_order_id,
          shopify_order_number,
          shopify_customer_id,
          email_hash,
          currency_code,
          subtotal_price,
          total_price,
          processed_at,
          created_at_shopify,
          updated_at_shopify,
          landing_session_id,
          checkout_token,
          payload_size_bytes,
          source_name
        )
        VALUES (
          'order-1',
          '1001',
          'sc-1',
          $1,
          'USD',
          50.00,
          50.00,
          '2026-04-25T11:00:00.000Z',
          '2026-04-25T11:00:00.000Z',
          '2026-04-25T11:00:00.000Z',
          $2::uuid,
          'co-1',
          2,
          'web'
        )
      `,
      [emailHash, sessionOneId]
    );
    await ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-25T11:00:00.000Z',
      evidenceSource: 'shopify_order_webhook',
      sourceTable: 'shopify_orders',
      sourceRecordId: 'order-1',
      idempotencyKey: 'customer-journey-order-1',
      sessionId: sessionOneId,
      checkoutToken: 'co-1',
      shopifyCustomerId: 'sc-1',
      hashedEmail: emailHash
    });
    await client.query('COMMIT');
    transactionOpen = false;

    const firstOrderRow = await pool.query(
      `
        SELECT
          authoritative_shopify_customer_id,
          session_order_count,
          journey_order_count,
          session_order_revenue::text AS session_order_revenue,
          journey_order_revenue::text AS journey_order_revenue,
          is_converting_session
        FROM customer_journey
        WHERE session_id = $1::uuid
      `,
      [sessionOneId]
    );
    assert.deepEqual(firstOrderRow.rows[0], {
      authoritative_shopify_customer_id: 'sc-1',
      session_order_count: 1,
      journey_order_count: 1,
      session_order_revenue: '50.00',
      journey_order_revenue: '50.00',
      is_converting_session: true
    });

    await client.query('BEGIN');
    transactionOpen = true;
    await client.query(
      `
        INSERT INTO tracking_sessions (
          id,
          first_seen_at,
          last_seen_at,
          landing_page,
          referrer_url,
          initial_utm_source,
          initial_utm_medium,
          initial_utm_campaign
        )
        VALUES (
          $1::uuid,
          '2026-04-25T12:00:00.000Z',
          '2026-04-25T12:02:00.000Z',
          'https://store.example.com/?utm_source=email&utm_medium=lifecycle&utm_campaign=vip',
          'https://mail.example.com/',
          'email',
          'lifecycle',
          'vip'
        )
      `,
      [sessionTwoId]
    );
    await client.query(
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
          shopify_checkout_token,
          payload_size_bytes,
          raw_payload
        )
        VALUES (
          $1::uuid,
          'checkout_started',
          '2026-04-25T12:00:45.000Z',
          'https://store.example.com/checkout',
          'https://mail.example.com/',
          'email',
          'lifecycle',
          'vip',
          'co-2',
          2,
          '{}'::jsonb
        )
      `,
      [sessionTwoId]
    );
    await ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-25T12:00:45.000Z',
      evidenceSource: 'tracking_event',
      sourceTable: 'tracking_events',
      sourceRecordId: 'evt-session-2',
      idempotencyKey: 'customer-journey-session-2',
      sessionId: sessionTwoId,
      checkoutToken: 'co-2'
    });
    await client.query('COMMIT');
    transactionOpen = false;

    const secondJourneyResult = await pool.query<{ identity_journey_id: string }>(
      `
        SELECT identity_journey_id::text AS identity_journey_id
        FROM tracking_sessions
        WHERE id = $1::uuid
      `,
      [sessionTwoId]
    );
    const secondJourneyId = secondJourneyResult.rows[0]?.identity_journey_id ?? null;
    assert.ok(secondJourneyId);
    assert.notEqual(secondJourneyId, firstJourneyId);

    await client.query('BEGIN');
    transactionOpen = true;
    await client.query(
      `
        INSERT INTO shopify_orders (
          shopify_order_id,
          shopify_order_number,
          shopify_customer_id,
          email_hash,
          currency_code,
          subtotal_price,
          total_price,
          processed_at,
          created_at_shopify,
          updated_at_shopify,
          landing_session_id,
          checkout_token,
          payload_size_bytes,
          source_name
        )
        VALUES (
          'order-2',
          '1002',
          'sc-1',
          $1,
          'USD',
          90.00,
          90.00,
          '2026-04-25T13:00:00.000Z',
          '2026-04-25T13:00:00.000Z',
          '2026-04-25T13:00:00.000Z',
          $2::uuid,
          'co-2',
          2,
          'web'
        )
      `,
      [emailHash, sessionTwoId]
    );
    await ingestIdentityEdges(client, {
      sourceTimestamp: '2026-04-25T13:00:00.000Z',
      evidenceSource: 'shopify_order_webhook',
      sourceTable: 'shopify_orders',
      sourceRecordId: 'order-2',
      idempotencyKey: 'customer-journey-order-2',
      sessionId: sessionTwoId,
      checkoutToken: 'co-2',
      shopifyCustomerId: 'sc-1',
      hashedEmail: emailHash
    });
    await client.query('COMMIT');
    transactionOpen = false;

    const finalRows = await pool.query(
      `
        SELECT
          session_id::text AS session_id,
          identity_journey_id::text AS identity_journey_id,
          journey_status,
          journey_session_number,
          reverse_journey_session_number,
          journey_session_count,
          journey_event_start_number,
          journey_event_end_number,
          journey_event_count,
          journey_order_count,
          session_order_count,
          journey_order_revenue::text AS journey_order_revenue,
          is_converting_session
        FROM customer_journey
        ORDER BY journey_session_number ASC
      `
    );

    assert.deepEqual(finalRows.rows, [
      {
        session_id: sessionOneId,
        identity_journey_id: firstJourneyId,
        journey_status: 'active',
        journey_session_number: 1,
        reverse_journey_session_number: 2,
        journey_session_count: 2,
        journey_event_start_number: 1,
        journey_event_end_number: 1,
        journey_event_count: 2,
        journey_order_count: 2,
        session_order_count: 1,
        journey_order_revenue: '140.00',
        is_converting_session: true
      },
      {
        session_id: sessionTwoId,
        identity_journey_id: firstJourneyId,
        journey_status: 'active',
        journey_session_number: 2,
        reverse_journey_session_number: 1,
        journey_session_count: 2,
        journey_event_start_number: 2,
        journey_event_end_number: 2,
        journey_event_count: 2,
        journey_order_count: 2,
        session_order_count: 1,
        journey_order_revenue: '140.00',
        is_converting_session: true
      }
    ]);

    const mergedJourneyResult = await pool.query(
      `
        SELECT status
        FROM identity_journeys
        WHERE id = $1::uuid
      `,
      [secondJourneyId]
    );
    assert.equal(mergedJourneyResult.rows[0]?.status, 'merged');

    const oldJourneyRows = await pool.query<{ count: string }>(
      `
        SELECT COUNT(*)::text AS count
        FROM customer_journey
        WHERE identity_journey_id = $1::uuid
      `,
      [secondJourneyId]
    );
    assert.equal(oldJourneyRows.rows[0]?.count, '0');
  } finally {
    if (transactionOpen) {
      await client.query('ROLLBACK').catch(() => undefined);
    }
    client.release();
    await truncateJourneyFixtures();
  }
});

test('customer_journey materialization only includes sessions and orders inside the journey lookback window', async () => {
  await truncateJourneyFixtures();

  const journeyId = '33333333-3333-4333-8333-333333333333';
  const boundarySessionId = '44444444-4444-4444-8444-444444444444';
  const outsideSessionId = '55555555-5555-4555-8555-555555555555';
  const { refreshCustomerJourneyForJourneys } = await import('../src/modules/identity/customer-journey.js');

  try {
    await pool.query(
      `
        INSERT INTO identity_journeys (
          id,
          status,
          merge_version,
          lookback_window_started_at,
          lookback_window_expires_at,
          last_touch_eligible_at,
          created_at,
          updated_at,
          last_resolved_at
        )
        VALUES (
          $1::uuid,
          'active',
          1,
          '2026-03-26T12:00:00.000Z',
          '2026-04-25T12:00:00.000Z',
          '2026-04-25T12:00:00.000Z',
          now(),
          now(),
          now()
        )
      `,
      [journeyId]
    );

    await pool.query(
      `
        INSERT INTO tracking_sessions (
          id,
          identity_journey_id,
          first_seen_at,
          last_seen_at
        )
        VALUES
          ($1::uuid, $3::uuid, '2026-03-26T12:00:00.000Z', '2026-03-26T12:05:00.000Z'),
          ($2::uuid, $3::uuid, '2026-03-26T11:59:59.000Z', '2026-03-26T12:04:59.000Z')
      `,
      [boundarySessionId, outsideSessionId, journeyId]
    );

    await pool.query(
      `
        INSERT INTO tracking_events (
          session_id,
          event_type,
          occurred_at,
          payload_size_bytes,
          raw_payload
        )
        VALUES
          ($1::uuid, 'page_view', '2026-03-26T12:01:00.000Z', 2, '{}'::jsonb),
          ($2::uuid, 'page_view', '2026-03-26T12:01:00.000Z', 2, '{}'::jsonb)
      `,
      [boundarySessionId, outsideSessionId]
    );

    await pool.query(
      `
        INSERT INTO shopify_orders (
          shopify_order_id,
          shopify_order_number,
          currency_code,
          subtotal_price,
          total_price,
          processed_at,
          landing_session_id,
          identity_journey_id,
          payload_size_bytes,
          source_name
        )
        VALUES
          ('journey-window-order-in', '2001', 'USD', 10.00, 10.00, '2026-04-25T12:00:00.000Z', $1::uuid, $3::uuid, 2, 'web'),
          ('journey-window-order-out', '2002', 'USD', 20.00, 20.00, '2026-03-26T11:59:59.000Z', $2::uuid, $3::uuid, 2, 'web')
      `,
      [boundarySessionId, outsideSessionId, journeyId]
    );

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await refreshCustomerJourneyForJourneys(client, [journeyId]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }

    const rows = await pool.query<{
      session_id: string;
      journey_session_count: number;
      journey_order_count: number;
      journey_order_revenue: string;
    }>(
      `
        SELECT
          session_id::text AS session_id,
          journey_session_count,
          journey_order_count,
          journey_order_revenue::text AS journey_order_revenue
        FROM customer_journey
        ORDER BY session_id ASC
      `
    );

    assert.deepEqual(rows.rows, [
      {
        session_id: boundarySessionId,
        journey_session_count: 1,
        journey_order_count: 1,
        journey_order_revenue: '10.00'
      }
    ]);
  } finally {
    await truncateJourneyFixtures();
  }
});

test.after(async () => {
  await pool.end();
});
