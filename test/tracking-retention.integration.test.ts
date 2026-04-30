import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';

import { buildRawPayloadFixture, resetIntegrationTables } from './integration-test-helpers.js';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

let pool: typeof import('../src/db/pool.js').pool;
let runSessionAttributionRetention: typeof import('../src/modules/tracking/retention.js').runSessionAttributionRetention;

const RETENTION_AS_OF = new Date('2026-04-25T12:00:00.000Z');
const RETENTION_CUTOFF = '2026-03-26T12:00:00.000Z';

async function resetIntegrationDatabase(): Promise<void> {
  await resetIntegrationTables(pool, [
    'order_attribution_links',
    'session_attribution_touch_events',
    'session_attribution_identities',
    'shopify_orders',
    'tracking_sessions'
  ]);
}

async function insertTrackingSession(sessionId: string): Promise<void> {
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
        '2026-03-01T10:00:00.000Z',
        '2026-03-01T10:05:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale'
      )
    `,
    [sessionId]
  );
}

async function insertSessionAttributionIdentity(input: {
  sessionId: string;
  retainedUntil: string;
}): Promise<void> {
  await pool.query(
    `
      INSERT INTO session_attribution_identities (
        roas_radar_session_id,
        first_captured_at,
        last_captured_at,
        retained_until,
        landing_url,
        referrer_url,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign
      )
      VALUES (
        $1::uuid,
        '2026-03-01T10:00:00.000Z',
        '2026-03-01T10:05:00.000Z',
        $2::timestamptz,
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale'
      )
    `,
    [input.sessionId, input.retainedUntil]
  );
}

async function insertSessionAttributionTouchEvent(input: {
  sessionId: string;
  retainedUntil: string;
}): Promise<void> {
  const rawPayloadFixture = buildRawPayloadFixture({});
  await pool.query(
    `
      INSERT INTO session_attribution_touch_events (
        roas_radar_session_id,
        event_type,
        occurred_at,
        captured_at,
        retained_until,
        page_url,
        referrer_url,
        utm_source,
        utm_medium,
        utm_campaign,
        payload_size_bytes,
        payload_hash,
        raw_payload
      )
      VALUES (
        $1::uuid,
        'page_view',
        '2026-03-01T10:04:00.000Z',
        '2026-03-01T10:04:05.000Z',
        $2::timestamptz,
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        $3,
        $4,
        $5::jsonb
      )
    `,
    [
      input.sessionId,
      input.retainedUntil,
      rawPayloadFixture.payloadSizeBytes,
      rawPayloadFixture.payloadHash,
      rawPayloadFixture.rawPayloadJson
    ]
  );
}

async function insertProtectedOrderLink(sessionId: string, shopifyOrderId: string): Promise<void> {
  await insertTrackingSession(sessionId);
  const rawPayloadFixture = buildRawPayloadFixture({ id: shopifyOrderId, landing_session_id: sessionId }, shopifyOrderId);

  await pool.query(
    `
      INSERT INTO shopify_orders (
        shopify_order_id,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        landing_session_id,
        payload_external_id,
        payload_size_bytes,
        payload_hash,
        raw_payload,
        ingested_at
      )
      VALUES (
        $1,
        'USD',
        '100.00',
        '100.00',
        '2026-03-02T12:00:00.000Z',
        $2::uuid,
        $3,
        $4,
        $5,
        $6::jsonb,
        now()
      )
    `,
    [
      shopifyOrderId,
      sessionId,
      rawPayloadFixture.payloadExternalId,
      rawPayloadFixture.payloadSizeBytes,
      rawPayloadFixture.payloadHash,
      rawPayloadFixture.rawPayloadJson
    ]
  );

  await pool.query(
    `
      INSERT INTO order_attribution_links (
        shopify_order_id,
        roas_radar_session_id,
        attribution_model,
        link_type,
        attribution_reason,
        retained_until,
        is_primary
      )
      VALUES (
        $1,
        $2::uuid,
        'last_touch',
        'deterministic',
        'matched_by_landing_session',
        '2026-06-01T00:00:00.000Z',
        true
      )
    `,
    [shopifyOrderId, sessionId]
  );
}

async function listSessionIds(): Promise<string[]> {
  const result = await pool.query<{ roas_radar_session_id: string }>(
    `
      SELECT roas_radar_session_id::text
      FROM session_attribution_identities
      ORDER BY roas_radar_session_id::text ASC
    `
  );

  return result.rows.map((row) => row.roas_radar_session_id);
}

async function listTouchEventSessionIds(): Promise<string[]> {
  const result = await pool.query<{ roas_radar_session_id: string }>(
    `
      SELECT roas_radar_session_id::text
      FROM session_attribution_touch_events
      ORDER BY roas_radar_session_id::text ASC, id ASC
    `
  );

  return result.rows.map((row) => row.roas_radar_session_id);
}

test.beforeEach(async () => {
  if (!pool || !runSessionAttributionRetention) {
    const [poolModule, retentionModule] = await Promise.all([
      import('../src/db/pool.js'),
      import('../src/modules/tracking/retention.js')
    ]);

    pool = poolModule.pool;
    runSessionAttributionRetention = retentionModule.runSessionAttributionRetention;
  }

  await resetIntegrationDatabase();
});

test.after(async () => {
  if (pool) {
    await resetIntegrationDatabase();
    await pool.end();
  }
});

test('runSessionAttributionRetention deletes expired unprotected session capture rows in batches and preserves protected rows', async () => {
  const expiredSessionA = randomUUID();
  const expiredSessionB = randomUUID();
  const expiredSessionC = randomUUID();
  const protectedSession = randomUUID();
  const freshSession = randomUUID();

  await insertSessionAttributionIdentity({
    sessionId: expiredSessionA,
    retainedUntil: '2026-03-20T00:00:00.000Z'
  });
  await insertSessionAttributionTouchEvent({
    sessionId: expiredSessionA,
    retainedUntil: '2026-03-20T00:00:00.000Z'
  });

  await insertSessionAttributionIdentity({
    sessionId: expiredSessionB,
    retainedUntil: '2026-03-21T00:00:00.000Z'
  });
  await insertSessionAttributionTouchEvent({
    sessionId: expiredSessionB,
    retainedUntil: '2026-03-21T00:00:00.000Z'
  });

  await insertSessionAttributionIdentity({
    sessionId: expiredSessionC,
    retainedUntil: '2026-03-22T00:00:00.000Z'
  });
  await insertSessionAttributionTouchEvent({
    sessionId: expiredSessionC,
    retainedUntil: '2026-03-22T00:00:00.000Z'
  });

  await insertSessionAttributionIdentity({
    sessionId: protectedSession,
    retainedUntil: '2026-03-19T00:00:00.000Z'
  });
  await insertSessionAttributionTouchEvent({
    sessionId: protectedSession,
    retainedUntil: '2026-03-19T00:00:00.000Z'
  });
  await insertProtectedOrderLink(protectedSession, 'protected-order-1');

  await insertSessionAttributionIdentity({
    sessionId: freshSession,
    retainedUntil: '2026-04-10T00:00:00.000Z'
  });
  await insertSessionAttributionTouchEvent({
    sessionId: freshSession,
    retainedUntil: '2026-04-10T00:00:00.000Z'
  });

  const result = await runSessionAttributionRetention({
    asOf: RETENTION_AS_OF,
    batchSize: 1,
    maxBatches: 2,
    emitLogs: false
  });

  assert.deepEqual(result, {
    cutoffAt: RETENTION_CUTOFF,
    batchSize: 1,
    maxBatches: 2,
    batchesRun: 2,
    deletedTouchEvents: 2,
    deletedSessions: 2,
    protectedSessionsSkipped: 1,
    protectedTouchEventsSkipped: 1
  });

  assert.deepEqual(await listSessionIds(), [expiredSessionC, freshSession, protectedSession].sort());
  assert.deepEqual(await listTouchEventSessionIds(), [expiredSessionC, freshSession, protectedSession].sort());
});

test('runSessionAttributionRetention does not delete rows exactly on the retention cutoff', async () => {
  const expiredSession = randomUUID();
  const cutoffSession = randomUUID();

  await insertSessionAttributionIdentity({
    sessionId: expiredSession,
    retainedUntil: '2026-03-26T11:59:59.000Z'
  });
  await insertSessionAttributionTouchEvent({
    sessionId: expiredSession,
    retainedUntil: '2026-03-26T11:59:59.000Z'
  });

  await insertSessionAttributionIdentity({
    sessionId: cutoffSession,
    retainedUntil: RETENTION_CUTOFF
  });
  await insertSessionAttributionTouchEvent({
    sessionId: cutoffSession,
    retainedUntil: RETENTION_CUTOFF
  });

  const result = await runSessionAttributionRetention({
    asOf: RETENTION_AS_OF,
    batchSize: 10,
    maxBatches: 5,
    emitLogs: false
  });

  assert.equal(result.cutoffAt, RETENTION_CUTOFF);
  assert.equal(result.deletedTouchEvents, 1);
  assert.equal(result.deletedSessions, 1);
  assert.equal(result.protectedSessionsSkipped, 0);
  assert.equal(result.protectedTouchEventsSkipped, 0);

  assert.deepEqual(await listSessionIds(), [cutoffSession]);
  assert.deepEqual(await listTouchEventSessionIds(), [cutoffSession]);
});
