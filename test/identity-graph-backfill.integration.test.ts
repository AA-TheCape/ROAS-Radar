import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

async function resetIdentityBackfillFixtures(): Promise<void> {
  const { pool } = await import('../src/db/pool.js');
  await pool.query(`
    TRUNCATE TABLE
      identity_graph_backfill_runs,
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

test.beforeEach(async () => {
  await resetIdentityBackfillFixtures();
});

test.after(async () => {
  await resetIdentityBackfillFixtures();
  const { pool } = await import('../src/db/pool.js');
  await pool.end();
});

test('identity graph backfill reconciles processed source rows and materializes canonical references', async () => {
  const [{ pool }, { backfillHistoricalIdentityGraph }] = await Promise.all([
    import('../src/db/pool.js'),
    import('../src/modules/identity/backfill.js')
  ]);

  const sessionId = '123e4567-e89b-42d3-a456-426614174111';
  const orderEmailHash = 'a'.repeat(64);
  const customerEmailHash = 'b'.repeat(64);
  const customerPhoneHash = 'c'.repeat(64);

  await pool.query(
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
        '2026-04-20T10:00:00.000Z',
        '2026-04-20T10:05:00.000Z',
        'https://store.example.com/?utm_source=google&utm_medium=cpc&utm_campaign=launch',
        'https://www.google.com/',
        'google',
        'cpc',
        'launch'
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
        referrer_url,
        utm_source,
        utm_medium,
        utm_campaign,
        shopify_checkout_token,
        payload_size_bytes,
        raw_payload
      )
      VALUES (
        '123e4567-e89b-42d3-a456-426614174112'::uuid,
        $1::uuid,
        'checkout_started',
        '2026-04-20T10:02:00.000Z',
        'https://store.example.com/checkout',
        'https://www.google.com/',
        'google',
        'cpc',
        'launch',
        'co-backfill-1',
        2,
        '{}'::jsonb
      )
    `,
    [sessionId]
  );
  await pool.query(
    `
      INSERT INTO shopify_customers (
        shopify_customer_id,
        email_hash,
        phone_hash,
        created_at,
        updated_at
      )
      VALUES (
        'shopify-customer-1',
        $1,
        $2,
        '2026-04-20T10:10:00.000Z',
        '2026-04-20T10:10:00.000Z'
      )
    `,
    [customerEmailHash, customerPhoneHash]
  );
  await pool.query(
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
        source_name,
        payload_size_bytes
      )
      VALUES (
        'order-backfill-1',
        '1001',
        'shopify-customer-1',
        $1,
        'USD',
        75.00,
        75.00,
        '2026-04-20T10:15:00.000Z',
        '2026-04-20T10:15:00.000Z',
        '2026-04-20T10:15:00.000Z',
        $2::uuid,
        'co-backfill-1',
        'web',
        2
      )
    `,
    [orderEmailHash, sessionId]
  );

  const report = await backfillHistoricalIdentityGraph({
    requestedBy: 'integration-test',
    workerId: 'identity-graph-backfill-test',
    batchSize: 2
  });

  assert.equal(report.status, 'completed');
  assert.equal(report.reconciliation.matches, true);
  assert.deepEqual(report.metrics.expectedCounts, {
    tracking_sessions: 1,
    tracking_events: 1,
    shopify_customers: 1,
    shopify_orders: 1
  });
  assert.deepEqual(report.metrics.processedCounts, report.metrics.expectedCounts);

  const sessionState = await pool.query<{
    identity_journey_id: string | null;
  }>(
    `
      SELECT identity_journey_id::text AS identity_journey_id
      FROM tracking_sessions
      WHERE id = $1::uuid
    `,
    [sessionId]
  );
  const journeyId = sessionState.rows[0]?.identity_journey_id ?? null;
  assert.ok(journeyId);

  const orderState = await pool.query<{
    identity_journey_id: string | null;
  }>(
    `
      SELECT identity_journey_id::text AS identity_journey_id
      FROM shopify_orders
      WHERE shopify_order_id = 'order-backfill-1'
    `
  );
  assert.equal(orderState.rows[0]?.identity_journey_id, journeyId);

  const customerJourneyState = await pool.query<{
    authoritative_shopify_customer_id: string | null;
    journey_order_count: number;
    session_order_count: number;
  }>(
    `
      SELECT
        authoritative_shopify_customer_id,
        journey_order_count,
        session_order_count
      FROM customer_journey
      WHERE session_id = $1::uuid
    `,
    [sessionId]
  );
  assert.deepEqual(customerJourneyState.rows[0], {
    authoritative_shopify_customer_id: 'shopify-customer-1',
    journey_order_count: 1,
    session_order_count: 1
  });
});

test('identity graph backfill resumes from persisted checkpoints without losing progress', async () => {
  const [{ pool }, { backfillHistoricalIdentityGraph, getIdentityGraphBackfillRun }] = await Promise.all([
    import('../src/db/pool.js'),
    import('../src/modules/identity/backfill.js')
  ]);

  await pool.query(
    `
      INSERT INTO tracking_sessions (
        id,
        first_seen_at,
        last_seen_at
      )
      VALUES
        ('123e4567-e89b-42d3-a456-426614174121'::uuid, '2026-04-20T09:00:00.000Z', '2026-04-20T09:01:00.000Z'),
        ('123e4567-e89b-42d3-a456-426614174122'::uuid, '2026-04-20T09:05:00.000Z', '2026-04-20T09:06:00.000Z')
    `
  );

  const partialReport = await backfillHistoricalIdentityGraph({
    requestedBy: 'integration-test',
    workerId: 'identity-graph-backfill-test',
    batchSize: 1,
    sources: ['tracking_sessions'],
    maxBatches: 1
  });

  assert.equal(partialReport.status, 'processing');
  assert.equal(partialReport.metrics.processedCounts.tracking_sessions, 1);
  assert.equal(partialReport.checkpoints.tracking_sessions.completed, false);
  assert.ok(partialReport.checkpoints.tracking_sessions.lastCursor);

  const resumedReport = await backfillHistoricalIdentityGraph({
    requestedBy: 'integration-test',
    workerId: 'identity-graph-backfill-test-resume',
    runId: partialReport.runId
  });

  assert.equal(resumedReport.status, 'completed');
  assert.equal(resumedReport.reconciliation.matches, true);
  assert.equal(resumedReport.metrics.processedCounts.tracking_sessions, 2);
  assert.equal(resumedReport.checkpoints.tracking_sessions.completed, true);

  const persistedRun = await getIdentityGraphBackfillRun(partialReport.runId);
  assert.ok(persistedRun);
  assert.equal(persistedRun?.status, 'completed');
  assert.equal(persistedRun?.metrics.processedCounts.tracking_sessions, 2);
});
