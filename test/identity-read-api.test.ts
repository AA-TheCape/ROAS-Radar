import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const originalPoolQuery = pool.query.bind(pool);

function buildInternalHeaders(): Record<string, string> {
  return {
    authorization: 'Bearer test-reporting-token'
  };
}

async function requestJson(server: ReturnType<typeof createServer>, path: string, headers?: Record<string, string>) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    headers
  });
  const body = await response.json();

  return { response, body };
}

test('internal identity routes require authentication', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/internal/identity/journeys/11111111-1111-4111-8111-111111111111');

    assert.equal(response.status, 401);
    assert.equal(body.error, 'unauthorized');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('internal identity routes reject authenticated user sessions and require the internal service token', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /FROM app_sessions s/);
    assert.equal(Array.isArray(params), true);
    assert.equal(typeof params?.[0], 'string');

    return {
      rows: [
        {
          session_id: 42,
          user_id: 7,
          email: 'analyst@example.com',
          display_name: 'Analyst',
          is_admin: true,
          status: 'active',
          last_login_at: new Date('2026-04-20T00:00:00.000Z'),
          created_at: new Date('2026-04-01T00:00:00.000Z'),
          expires_at: new Date('2026-05-01T00:00:00.000Z')
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/internal/identity/journeys/11111111-1111-4111-8111-111111111111',
      {
        authorization: 'Bearer rrs_fixture_session_token'
      }
    );

    assert.equal(response.status, 403);
    assert.equal(body.error, 'forbidden');
    assert.equal(body.message, 'Internal service token required');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('internal identity lookup rejects unhashed contact identifiers before querying storage', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/internal/identity/lookup?nodeType=hashed_email&nodeKey=buyer@example.com',
      buildInternalHeaders()
    );

    assert.equal(response.status, 400);
    assert.equal(body.error, 'invalid_request');
    assert.equal(queryCalls, 0);
    assert.deepEqual(body.details.fieldErrors.nodeKey, ['hashed_email lookups require a sha256 hex digest']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('internal identity journey lookup returns canonical identity, linked identifiers, and timeline without plaintext contact fields', async () => {
  const journeyId = '11111111-1111-4111-8111-111111111111';
  const hashedEmail = 'a'.repeat(64);
  const phoneHash = 'b'.repeat(64);

  pool.query = (async (text: string, params?: unknown[]) => {
    if (/FROM identity_nodes node/.test(text)) {
      assert.deepEqual(params, ['hashed_email', hashedEmail]);
      return {
        rows: [
          {
            id: journeyId,
            status: 'active',
            authoritative_shopify_customer_id: 'sc-1',
            primary_email_hash: hashedEmail,
            primary_phone_hash: phoneHash,
            merge_version: 3,
            merged_into_journey_id: null,
            lookback_window_started_at: new Date('2026-04-01T00:00:00.000Z'),
            lookback_window_expires_at: new Date('2026-05-01T00:00:00.000Z'),
            last_touch_eligible_at: new Date('2026-04-30T00:00:00.000Z'),
            created_at: new Date('2026-04-01T00:00:00.000Z'),
            updated_at: new Date('2026-04-25T12:00:00.000Z'),
            last_resolved_at: new Date('2026-04-25T12:00:00.000Z')
          }
        ]
      };
    }

    if (/FROM identity_journeys[\s\S]*WHERE id = \$1::uuid/.test(text)) {
      assert.deepEqual(params, [journeyId]);
      return {
        rows: [
          {
            id: journeyId,
            status: 'active',
            authoritative_shopify_customer_id: 'sc-1',
            primary_email_hash: hashedEmail,
            primary_phone_hash: phoneHash,
            merge_version: 3,
            merged_into_journey_id: null,
            lookback_window_started_at: new Date('2026-04-01T00:00:00.000Z'),
            lookback_window_expires_at: new Date('2026-05-01T00:00:00.000Z'),
            last_touch_eligible_at: new Date('2026-04-30T00:00:00.000Z'),
            created_at: new Date('2026-04-01T00:00:00.000Z'),
            updated_at: new Date('2026-04-25T12:00:00.000Z'),
            last_resolved_at: new Date('2026-04-25T12:00:00.000Z')
          }
        ]
      };
    }

    if (/FROM identity_edges edge/.test(text)) {
      assert.deepEqual(params, [journeyId]);
      return {
        rows: [
          {
            edge_id: 'edge-1',
            node_id: 'node-1',
            node_type: 'shopify_customer_id',
            node_key: 'sc-1',
            is_authoritative: true,
            is_ambiguous: false,
            edge_type: 'authoritative',
            precedence_rank: 100,
            evidence_source: 'shopify_order_webhook',
            source_table: 'shopify_orders',
            source_record_id: 'order-1',
            is_active: true,
            conflict_code: null,
            first_observed_at: new Date('2026-04-02T00:00:00.000Z'),
            last_observed_at: new Date('2026-04-25T12:00:00.000Z'),
            edge_created_at: new Date('2026-04-02T00:00:00.000Z'),
            edge_updated_at: new Date('2026-04-25T12:00:00.000Z')
          },
          {
            edge_id: 'edge-2',
            node_id: 'node-2',
            node_type: 'hashed_email',
            node_key: hashedEmail,
            is_authoritative: false,
            is_ambiguous: false,
            edge_type: 'promoted',
            precedence_rank: 70,
            evidence_source: 'shopify_order_webhook',
            source_table: 'shopify_orders',
            source_record_id: 'order-1',
            is_active: true,
            conflict_code: null,
            first_observed_at: new Date('2026-04-02T00:00:00.000Z'),
            last_observed_at: new Date('2026-04-25T12:00:00.000Z'),
            edge_created_at: new Date('2026-04-02T00:00:00.000Z'),
            edge_updated_at: new Date('2026-04-25T12:00:00.000Z')
          }
        ]
      };
    }

    if (/FROM customer_journey/.test(text)) {
      assert.deepEqual(params, [journeyId]);
      return {
        rows: [
          {
            session_id: '22222222-2222-4222-8222-222222222222',
            session_started_at: new Date('2026-04-10T10:00:00.000Z'),
            session_ended_at: new Date('2026-04-10T10:15:00.000Z'),
            journey_session_number: 1,
            reverse_journey_session_number: 1,
            session_event_count: 4,
            page_view_count: 2,
            product_view_count: 1,
            add_to_cart_count: 1,
            checkout_started_count: 1,
            session_order_count: 1,
            session_order_revenue: '88.50',
            is_first_session: true,
            is_last_session: true,
            is_converting_session: true,
            anonymous_user_id: 'anon-1',
            landing_page: 'https://store.example.com/products/widget',
            referrer_url: 'https://www.google.com/',
            utm_source: 'google',
            utm_medium: 'cpc',
            utm_campaign: 'spring',
            utm_content: 'hero',
            utm_term: 'widget',
            gclid: 'gclid-1',
            gbraid: null,
            wbraid: null,
            fbclid: null,
            ttclid: null,
            msclkid: null
          }
        ]
      };
    }

    if (/FROM shopify_orders/.test(text)) {
      assert.deepEqual(params, [journeyId]);
      return {
        rows: [
          {
            shopify_order_id: 'order-1',
            shopify_order_number: '1001',
            shopify_customer_id: 'sc-1',
            email_hash: hashedEmail,
            currency_code: 'USD',
            total_price: '88.50',
            financial_status: 'paid',
            fulfillment_status: 'fulfilled',
            processed_at: new Date('2026-04-10T10:20:00.000Z'),
            created_at_shopify: new Date('2026-04-10T10:19:00.000Z'),
            updated_at_shopify: new Date('2026-04-10T10:21:00.000Z'),
            landing_session_id: '22222222-2222-4222-8222-222222222222',
            checkout_token: 'co-1',
            cart_token: 'ca-1',
            source_name: 'web',
            ingested_at: new Date('2026-04-10T10:21:30.000Z')
          }
        ]
      };
    }

    throw new Error(`Unexpected query: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      `/api/internal/identity/lookup?nodeType=hashed_email&nodeKey=${hashedEmail}`,
      buildInternalHeaders()
    );

    assert.equal(response.status, 200);
    assert.equal(body.lookup.nodeType, 'hashed_email');
    assert.equal(body.lookup.nodeKey, hashedEmail);
    assert.equal(body.journey.journeyId, journeyId);
    assert.equal(body.journey.authoritativeShopifyCustomerId, 'sc-1');
    assert.equal(body.journey.primaryIdentifiers.hashedEmail, hashedEmail);
    assert.equal(body.journey.primaryIdentifiers.phoneHash, phoneHash);
    assert.equal(body.identifiers.activeCount, 2);
    assert.equal(body.timeline.sessions.length, 1);
    assert.equal(body.timeline.orders.length, 1);
    assert.equal(body.timeline.orders[0].emailHash, hashedEmail);
    assert.equal(body.timeline.orders[0].checkoutToken, 'co-1');
    assert.equal(body.timeline.orders[0].cartToken, 'ca-1');
    assert.equal(body.timeline.orders[0].landingSessionId, '22222222-2222-4222-8222-222222222222');
    assert.equal(body.timeline.sessions[0].acquisition.utmSource, 'google');
    assert.equal(body.timeline.sessions[0].metrics.orderRevenue, 88.5);

    const serialized = JSON.stringify(body);
    assert.doesNotMatch(serialized, /buyer@example\.com/i);
    assert.doesNotMatch(serialized, /"email":/i);
    assert.doesNotMatch(serialized, /"phone":/i);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});
