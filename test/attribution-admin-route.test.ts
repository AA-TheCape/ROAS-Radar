import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');
const schemaModule = await import('../packages/attribution-schema/index.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const { attributionQaPayloadV1SuccessFixture } = schemaModule;
const originalPoolQuery = pool.query.bind(pool);
const emptyTierCounts = {
  deterministic_first_party: 0,
  deterministic_shopify_hint: 0,
  platform_reported_meta: 0,
  ga4_fallback: 0,
  unattributed: 0
};

async function requestJson(
  server: ReturnType<typeof createServer>,
  path: string,
  input: {
    method?: string;
    headers?: Record<string, string>;
    body?: unknown;
  } = {}
) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    method: input.method ?? 'GET',
    headers: {
      'content-type': 'application/json',
      ...(input.headers ?? {})
    },
    body: input.body === undefined ? undefined : JSON.stringify(input.body)
  });
  const body = await response.json();

  return { response, body };
}

test('order attribution backfill admin route rejects unauthorized requests with the standard admin response', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05'
      }
    });

    assert.equal(response.status, 401);
    assert.deepEqual(body, {
      error: 'unauthorized',
      message: 'Authentication required'
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route rejects authenticated non-admin users', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return {
      rows: [
        {
          session_id: 7,
          user_id: 42,
          email: 'analyst@example.com',
          display_name: 'Analyst',
          is_admin: false,
          status: 'active',
          last_login_at: new Date('2026-04-25T10:00:00.000Z'),
          created_at: new Date('2026-04-01T00:00:00.000Z'),
          expires_at: new Date('2026-05-01T00:00:00.000Z')
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer user-session-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05'
      }
    });

    assert.equal(response.status, 403);
    assert.deepEqual(body, {
      error: 'forbidden',
      message: 'Admin access required'
    });
    assert.equal(queryCalls, 1);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution QA debug route rejects internal service tokens because raw evidence requires an admin user', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/order-qa/qa-debug', {
      headers: {
        authorization: 'Bearer test-reporting-token'
      }
    });

    assert.equal(response.status, 403);
    assert.deepEqual(body, {
      error: 'forbidden',
      message: 'Internal admin user access required'
    });
    assert.equal(queryCalls, 0);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution QA debug route rejects unauthenticated requests before querying QA data', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/order-qa/qa-debug');

    assert.equal(response.status, 401);
    assert.deepEqual(body, {
      error: 'unauthorized',
      message: 'Authentication required'
    });
    assert.equal(queryCalls, 0);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution QA debug route rejects authenticated non-admin users before loading raw evidence', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return {
      rows: [
        {
          session_id: 7,
          user_id: 42,
          email: 'analyst@example.com',
          display_name: 'Analyst',
          is_admin: false,
          status: 'active',
          last_login_at: new Date('2026-04-25T10:00:00.000Z'),
          created_at: new Date('2026-04-01T00:00:00.000Z'),
          expires_at: new Date('2026-05-01T00:00:00.000Z')
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/order-qa/qa-debug', {
      headers: {
        authorization: 'Bearer user-session-token'
      }
    });

    assert.equal(response.status, 403);
    assert.deepEqual(body, {
      error: 'forbidden',
      message: 'Admin access required'
    });
    assert.equal(queryCalls, 1);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution QA debug route returns redacted schema payload and grouped raw evidence for admin users', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];
  const fullPayload = {
    ...attributionQaPayloadV1SuccessFixture,
    order: {
      ...attributionQaPayloadV1SuccessFixture.order,
      order_id: 'order-qa-debug',
      identifiers: {
        ...attributionQaPayloadV1SuccessFixture.order.identifiers,
        checkout_token: 'checkout-token-secret',
        cart_token: 'cart-token-secret',
        email_hash: '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
      }
    },
    candidates: {
      ...attributionQaPayloadV1SuccessFixture.candidates,
      ga4_fallback: [
        {
          candidate_group: 'ga4_fallback',
          source_key: 'ga4-candidate-key',
          touchpoint_id: 'ga4-candidate-key',
          session_id: null,
          source_touch_event_id: null,
          occurred_at_utc: '2026-04-10T11:00:00.000Z',
          source: 'google',
          medium: 'cpc',
          campaign: 'https://store.example/campaign?gclid=QA-PAYLOAD-GA4-CAMPAIGN-GCLID',
          content: 'content?ga_client_id=QA-PAYLOAD-GA4-CONTENT-CLIENT',
          term: 'term&utm_id=QA-PAYLOAD-GA4-UTM-ID',
          click_id_type: 'gclid',
          click_id_value: 'GA4-GCLID-SECRET',
          match_source: 'ga4_fallback',
          attribution_reason: 'ga4_fallback_match',
          confidence_score: 0.5,
          confidence_label: 'low',
          is_direct: false,
          is_synthetic: true,
          selected: false
        }
      ]
    }
  };

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedQueries.push({ text, params });

    if (text.includes('FROM app_sessions')) {
      return {
        rows: [
          {
            session_id: 7,
            user_id: 42,
            email: 'admin@example.com',
            display_name: 'Admin',
            is_admin: true,
            status: 'active',
            last_login_at: new Date('2026-04-25T10:00:00.000Z'),
            created_at: new Date('2026-04-01T00:00:00.000Z'),
            expires_at: new Date('2026-05-01T00:00:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM shopify_orders')) {
      assert.equal(params?.[0], 'order-qa-debug');
      return {
        rows: [
          {
            shopify_order_id: 'order-qa-debug',
            name: '#QA-DEBUG',
            currency_code: 'USD',
            subtotal_price: '90.00',
            total_price: '100.00',
            processed_at: new Date('2026-04-10T12:00:00.000Z'),
            created_at_shopify: null,
            ingested_at: new Date('2026-04-10T12:01:00.000Z'),
            landing_session_id: '123e4567-e89b-42d3-a456-426614174000',
            checkout_token: 'checkout-token-secret',
            cart_token: 'cart-token-secret',
            shopify_customer_id: null,
            email_hash: '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
            customer_identity_id: null,
            identity_journey_id: null,
            source_name: 'web',
            raw_payload: { name: '#QA-DEBUG' },
            attribution_snapshot: {
              qaSnapshot: fullPayload
            }
          }
        ]
      };
    }

    if (text.includes('FROM attribution_order_inputs')) {
      return {
        rows: [
          {
            run_id: '11111111-1111-4111-8111-111111111111',
            normalized_at_utc: new Date('2026-04-10T12:04:00.000Z'),
            retained_until: new Date('2027-04-10T12:04:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM attribution_raw_evidence')) {
      return {
        rows: [
          {
            id: '1',
            run_id: '11111111-1111-4111-8111-111111111111',
            order_id: 'order-qa-debug',
            evidence_type: 'shopify_hint',
            source_table: 'shopify_orders',
            source_record_id: 'order-qa-debug',
            touchpoint_id: 'shopify:order-qa-debug',
            session_id: null,
            ingestion_source: 'shopify_marketing_hint',
            event_type: null,
            occurred_at_utc: new Date('2026-04-10T12:00:00.000Z'),
            captured_at_utc: null,
            evidence_status: 'valid',
            error_code: null,
            error_message: null,
            normalized_metadata: { hint: 'landing_site' },
            raw_payload: {
              landing_site: 'https://store.example/?gclid=RAW-GCLID',
              checkout_token: 'raw-checkout-token-secret',
              email_hash: 'fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210',
              customer_id: 'raw-shopify-customer-id',
              nested: {
                cart_token: 'nested-cart-token-secret',
                links: [
                  'gclid=NESTED-SHOPIFY-QUERY-GCLID&checkout_token=NESTED-SHOPIFY-CHECKOUT',
                  {
                    email: 'nested-shopify-email@example.com',
                    shopify_customer_id: 'nested-shopify-customer'
                  }
                ]
              }
            },
            payload_size_bytes: 64,
            payload_hash: 'a'.repeat(64),
            created_at_utc: new Date('2026-04-10T12:05:00.000Z'),
            retained_until: new Date('2026-10-10T12:05:00.000Z')
          },
          {
            id: '2',
            run_id: '11111111-1111-4111-8111-111111111111',
            order_id: 'order-qa-debug',
            evidence_type: 'tracking_touchpoint',
            source_table: 'session_attribution_touch_events',
            source_record_id: 'touch-1',
            touchpoint_id: 'touch-1',
            session_id: '123e4567-e89b-42d3-a456-426614174000',
            ingestion_source: 'browser',
            event_type: 'page_view',
            occurred_at_utc: new Date('2026-04-09T11:00:00.000Z'),
            captured_at_utc: new Date('2026-04-09T11:00:01.000Z'),
            evidence_status: 'valid',
            error_code: null,
            error_message: null,
            normalized_metadata: {
              ga4_client_id: 'raw-ga4-client-id',
              referrer: 'https://store.example/?email_hash=raw-email-hash',
              nested: {
                ga_session_id: 'raw-nested-ga-session-id',
                urls: [
                  'https://example.com/path?_ga=RAW-NESTED-GA&client_id=RAW-NESTED-CLIENT-ID',
                  {
                    user_id: 'raw-nested-user-id',
                    clickid: 'raw-nested-clickid'
                  }
                ]
              }
            },
            raw_payload: {
              gclid: 'RAW-TOUCH-GCLID',
              url: 'https://store.example/products/widget?fbclid=RAW-FBCLID&discount=SAVE10',
              gaClientId: 'RAW-TOUCH-GA-CLIENT-CAMEL',
              payloads: [
                {
                  page_location: 'https://store.example/?wbraid=RAW-ARRAY-WBRAID&promo=keep',
                  ga_user_id: 'RAW-ARRAY-GA-USER'
                }
              ]
            },
            payload_size_bytes: 32,
            payload_hash: 'b'.repeat(64),
            created_at_utc: new Date('2026-04-10T12:05:00.000Z'),
            retained_until: new Date('2026-10-10T12:05:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM ga4_fallback_candidates')) {
      assert.deepEqual(params?.[0], ['ga4-candidate-key']);
      assert.equal(params?.[1], 'order-qa-debug');
      assert.equal(params?.[2], '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef');
      return {
        rows: [
          {
            candidate_key: 'ga4-candidate-key',
            occurred_at: new Date('2026-04-10T11:00:00.000Z'),
            ga4_user_key: 'ga4-user-secret',
            ga4_client_id: 'ga4-client-secret',
            ga4_session_id: 'ga4-session-secret',
            transaction_id: 'order-qa-debug',
            email_hash: '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
            customer_identity_id: null,
            source: 'google',
            medium: 'cpc',
            campaign: 'https://store.example/ga4?gclid=GA4-CAMPAIGN-GCLID',
            content: 'https://store.example/content?client_id=GA4-CONTENT-CLIENT-ID',
            term: 'query?utm_id=GA4-TERM-UTM-ID',
            click_id_type: 'gclid',
            click_id_value: 'GA4-GCLID-SECRET',
            session_has_required_fields: true,
            source_export_hour: new Date('2026-04-10T11:00:00.000Z'),
            source_dataset: 'analytics_123',
            source_table_type: 'events',
            retained_until: new Date('2026-07-10T11:00:00.000Z'),
            created_at: new Date('2026-04-10T12:00:00.000Z'),
            updated_at: new Date('2026-04-10T12:00:00.000Z'),
            matched_on: 'qa_candidate_key'
          }
        ]
      };
    }

    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/order-qa-debug/qa-debug', {
      headers: {
        authorization: 'Bearer admin-session-token'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(body.orderId, 'order-qa-debug');
    assert.equal(body.source, 'persisted_snapshot');
    assert.equal(body.selectedRunId, '11111111-1111-4111-8111-111111111111');
    assert.equal(body.evidenceState.rawEvidence, 'available');
    assert.equal(body.evidenceState.rawShopifyHints, 'available');
    assert.equal(body.evidenceState.rawTouchpoints, 'available');
    assert.equal(body.evidenceState.ga4FallbackCandidate, 'available');
    assert.equal(body.payload.order.identifiers.checkout_token, null);
    assert.equal(body.payload.order.identifiers.cart_token, null);
    assert.equal(body.payload.order.identifiers.email_hash, null);
    assert.equal(body.payload.candidates.ga4_fallback[0].source_key, 'ga4_fallback_candidate_1');
    assert.equal(body.payload.candidates.ga4_fallback[0].click_id_value, null);
    assert.match(body.payload.candidates.ga4_fallback[0].campaign, /redacted/i);
    assert.match(body.payload.candidates.ga4_fallback[0].content, /redacted/i);
    assert.match(body.payload.candidates.ga4_fallback[0].term, /redacted/i);
    assert.equal(body.rawShopifyHints[0].rawPayload.landing_site, 'https://store.example/?gclid=%5BREDACTED%5D');
    assert.match(body.rawShopifyHints[0].rawPayload.checkout_token, /\[REDACTED\]/);
    assert.match(body.rawShopifyHints[0].rawPayload.email_hash, /\[REDACTED\]/);
    assert.match(body.rawShopifyHints[0].rawPayload.customer_id, /\[REDACTED\]/);
    assert.match(body.rawShopifyHints[0].rawPayload.nested.cart_token, /\[REDACTED\]/);
    assert.equal(
      body.rawShopifyHints[0].rawPayload.nested.links[0],
      'gclid=[REDACTED]&checkout_token=[REDACTED]'
    );
    assert.match(body.rawShopifyHints[0].rawPayload.nested.links[1].email, /\[REDACTED\]/);
    assert.match(body.rawShopifyHints[0].rawPayload.nested.links[1].shopify_customer_id, /\[REDACTED\]/);
    assert.match(body.rawTouchpoints[0].sessionId, /\[REDACTED\]/);
    assert.match(body.rawTouchpoints[0].sourceRecordId, /\[REDACTED\]/);
    assert.match(body.rawTouchpoints[0].rawPayload.gclid, /\[REDACTED\]/);
    assert.equal(
      body.rawTouchpoints[0].rawPayload.url,
      'https://store.example/products/widget?fbclid=%5BREDACTED%5D&discount=SAVE10'
    );
    assert.match(body.rawTouchpoints[0].rawPayload.gaClientId, /\[REDACTED\]/);
    assert.equal(
      body.rawTouchpoints[0].rawPayload.payloads[0].page_location,
      'https://store.example/?wbraid=%5BREDACTED%5D&promo=keep'
    );
    assert.match(body.rawTouchpoints[0].rawPayload.payloads[0].ga_user_id, /\[REDACTED\]/);
    assert.match(body.rawTouchpoints[0].normalizedMetadata.ga4_client_id, /\[REDACTED\]/);
    assert.equal(body.rawTouchpoints[0].normalizedMetadata.referrer, 'https://store.example/?email_hash=%5BREDACTED%5D');
    assert.match(body.rawTouchpoints[0].normalizedMetadata.nested.ga_session_id, /\[REDACTED\]/);
    assert.equal(
      body.rawTouchpoints[0].normalizedMetadata.nested.urls[0],
      'https://example.com/path?_ga=%5BREDACTED%5D&client_id=%5BREDACTED%5D'
    );
    assert.match(body.rawTouchpoints[0].normalizedMetadata.nested.urls[1].user_id, /\[REDACTED\]/);
    assert.match(body.rawTouchpoints[0].normalizedMetadata.nested.urls[1].clickid, /\[REDACTED\]/);
    assert.match(body.ga4FallbackCandidate.candidateKey, /\[REDACTED\]/);
    assert.match(body.ga4FallbackCandidate.ga4UserKey, /\[REDACTED\]/);
    assert.match(body.ga4FallbackCandidate.ga4ClientId, /\[REDACTED\]/);
    assert.match(body.ga4FallbackCandidate.ga4SessionId, /\[REDACTED\]/);
    assert.match(body.ga4FallbackCandidate.emailHash, /\[REDACTED\]/);
    assert.match(body.ga4FallbackCandidate.clickIdValue, /\[REDACTED\]/);
    assert.equal(body.ga4FallbackCandidate.campaign, 'https://store.example/ga4?gclid=%5BREDACTED%5D');
    assert.equal(body.ga4FallbackCandidate.content, 'https://store.example/content?client_id=%5BREDACTED%5D');
    assert.equal(body.ga4FallbackCandidate.term, 'query?utm_id=[REDACTED]');
    const serialized = JSON.stringify(body);
    assert.doesNotMatch(serialized, /checkout-token-secret/);
    assert.doesNotMatch(serialized, /cart-token-secret/);
    assert.doesNotMatch(serialized, /0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/);
    assert.doesNotMatch(serialized, /raw-checkout-token-secret/);
    assert.doesNotMatch(serialized, /fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210/);
    assert.doesNotMatch(serialized, /RAW-GCLID/);
    assert.doesNotMatch(serialized, /RAW-TOUCH-GCLID/);
    assert.doesNotMatch(serialized, /RAW-FBCLID/);
    assert.doesNotMatch(serialized, /raw-ga4-client-id/);
    assert.doesNotMatch(serialized, /raw-email-hash/);
    assert.doesNotMatch(serialized, /ga4-user-secret/);
    assert.doesNotMatch(serialized, /ga4-client-secret/);
    assert.doesNotMatch(serialized, /ga4-session-secret/);
    assert.doesNotMatch(serialized, /GA4-GCLID-SECRET/);
    assert.doesNotMatch(serialized, /QA-PAYLOAD-GA4-CAMPAIGN-GCLID/);
    assert.doesNotMatch(serialized, /QA-PAYLOAD-GA4-CONTENT-CLIENT/);
    assert.doesNotMatch(serialized, /QA-PAYLOAD-GA4-UTM-ID/);
    assert.doesNotMatch(serialized, /raw-shopify-customer-id/);
    assert.doesNotMatch(serialized, /nested-cart-token-secret/);
    assert.doesNotMatch(serialized, /NESTED-SHOPIFY-QUERY-GCLID/);
    assert.doesNotMatch(serialized, /NESTED-SHOPIFY-CHECKOUT/);
    assert.doesNotMatch(serialized, /nested-shopify-email@example\.com/);
    assert.doesNotMatch(serialized, /nested-shopify-customer/);
    assert.doesNotMatch(serialized, /raw-nested-ga-session-id/);
    assert.doesNotMatch(serialized, /RAW-NESTED-GA/);
    assert.doesNotMatch(serialized, /RAW-NESTED-CLIENT-ID/);
    assert.doesNotMatch(serialized, /raw-nested-user-id/);
    assert.doesNotMatch(serialized, /raw-nested-clickid/);
    assert.doesNotMatch(serialized, /RAW-TOUCH-GA-CLIENT-CAMEL/);
    assert.doesNotMatch(serialized, /RAW-ARRAY-WBRAID/);
    assert.doesNotMatch(serialized, /RAW-ARRAY-GA-USER/);
    assert.doesNotMatch(serialized, /GA4-CAMPAIGN-GCLID/);
    assert.doesNotMatch(serialized, /GA4-CONTENT-CLIENT-ID/);
    assert.doesNotMatch(serialized, /GA4-TERM-UTM-ID/);
    assert.equal(capturedQueries.some((entry) => entry.text.includes('FROM attribution_raw_evidence')), true);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution QA debug route reports expired raw evidence when only retained order inputs remain', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('FROM app_sessions')) {
      return {
        rows: [
          {
            session_id: 7,
            user_id: 42,
            email: 'admin@example.com',
            display_name: 'Admin',
            is_admin: true,
            status: 'active',
            last_login_at: new Date('2026-04-25T10:00:00.000Z'),
            created_at: new Date('2026-04-01T00:00:00.000Z'),
            expires_at: new Date('2026-05-01T00:00:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM shopify_orders')) {
      return {
        rows: [
          {
            shopify_order_id: 'order-qa-expired',
            name: '#QA-EXPIRED',
            currency_code: 'USD',
            subtotal_price: '90.00',
            total_price: '100.00',
            processed_at: new Date('2026-04-10T12:00:00.000Z'),
            created_at_shopify: null,
            ingested_at: new Date('2026-04-10T12:01:00.000Z'),
            landing_session_id: '123e4567-e89b-42d3-a456-426614174000',
            checkout_token: null,
            cart_token: null,
            shopify_customer_id: null,
            email_hash: null,
            customer_identity_id: null,
            identity_journey_id: null,
            source_name: 'web',
            raw_payload: { name: '#QA-EXPIRED' },
            attribution_snapshot: {
              qaSnapshot: {
                ...attributionQaPayloadV1SuccessFixture,
                order: {
                  ...attributionQaPayloadV1SuccessFixture.order,
                  order_id: 'order-qa-expired'
                }
              }
            }
          }
        ]
      };
    }

    if (text.includes('FROM attribution_order_inputs')) {
      return {
        rows: [
          {
            run_id: '11111111-1111-4111-8111-111111111111',
            normalized_at_utc: new Date('2026-04-10T12:04:00.000Z'),
            retained_until: new Date('2027-04-10T12:04:00.000Z')
          }
        ]
      };
    }

    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/order-qa-expired/qa-debug', {
      headers: {
        authorization: 'Bearer admin-session-token'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(body.evidenceState.attributionRun, 'available');
    assert.equal(body.evidenceState.rawEvidence, 'expired_or_pruned');
    assert.deepEqual(body.rawShopifyHints, []);
    assert.deepEqual(body.rawTouchpoints, []);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution QA debug route returns a clean not-found response for missing Shopify orders', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('FROM app_sessions')) {
      return {
        rows: [
          {
            session_id: 7,
            user_id: 42,
            email: 'admin@example.com',
            display_name: 'Admin',
            is_admin: true,
            status: 'active',
            last_login_at: new Date('2026-04-25T10:00:00.000Z'),
            created_at: new Date('2026-04-01T00:00:00.000Z'),
            expires_at: new Date('2026-05-01T00:00:00.000Z')
          }
        ]
      };
    }

    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/missing-order/qa-debug', {
      headers: {
        authorization: 'Bearer admin-session-token'
      }
    });

    assert.equal(response.status, 404);
    assert.equal(body.error, 'shopify_order_not_found');
    assert.equal(body.message, 'No Shopify order was found for missing-order');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route validates the shared request contract before enqueueing', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        startDate: '2026-04-10',
        endDate: '2026-04-01',
        limit: 6000
      }
    });

    assert.equal(response.status, 400);
    assert.equal(body.error, 'invalid_request');
    assert.equal(body.message, 'Invalid order attribution backfill request');
    assert.equal(queryCalls, 0);
    assert.deepEqual(body.details.fieldErrors.endDate, ['Start date must be on or before end date.']);
    assert.deepEqual(body.details.fieldErrors.limit, ['Limit must be 5000 or less.']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route rejects limit values below the shared minimum', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        limit: 0
      }
    });

    assert.equal(response.status, 400);
    assert.equal(body.error, 'invalid_request');
    assert.equal(queryCalls, 0);
    assert.deepEqual(body.details.fieldErrors.limit, ['Limit must be greater than 0.']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route enqueues normalized jobs and returns 202 metadata immediately', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedQueries.push({ text, params });
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05'
      }
    });

    assert.equal(response.status, 202);
    assert.equal(body.ok, true);
    assert.equal(body.status, 'queued');
    assert.equal(body.submittedBy, 'internal');
    assert.equal(body.options.startDate, '2026-04-01');
    assert.equal(body.options.endDate, '2026-04-05');
    assert.equal(body.options.dryRun, true);
    assert.equal(body.options.limit, 500);
    assert.equal(body.options.reclassificationTarget, 'full_rebuild');
    assert.deepEqual(body.options.organizationIds, []);
    assert.equal(body.options.webOrdersOnly, true);
    assert.equal(body.options.skipShopifyWriteback, false);
    assert.match(body.jobId, /^[0-9a-f-]{36}$/i);
    assert.match(body.submittedAt, /^\d{4}-\d{2}-\d{2}T/);

    assert.equal(capturedQueries.length, 1);
    assert.match(capturedQueries[0].text, /INSERT INTO order_attribution_backfill_runs/);
    assert.equal(capturedQueries[0].params?.[0], body.jobId);
    assert.equal(capturedQueries[0].params?.[2], 'internal');
    assert.deepEqual(JSON.parse(String(capturedQueries[0].params?.[3])), {
      startDate: '2026-04-01',
      endDate: '2026-04-05',
      dryRun: true,
      limit: 500,
      reclassificationTarget: 'full_rebuild',
      organizationIds: [],
      webOrdersOnly: true,
      skipShopifyWriteback: false
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route preserves explicit options, including max limit and writeback flags', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedQueries.push({ text, params });
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        dryRun: false,
        limit: 5000,
        webOrdersOnly: false,
        skipShopifyWriteback: true
      }
    });

    assert.equal(response.status, 202);
    assert.deepEqual(body.options, {
      startDate: '2026-04-01',
      endDate: '2026-04-05',
      dryRun: false,
      limit: 5000,
      reclassificationTarget: 'full_rebuild',
      organizationIds: [],
      webOrdersOnly: false,
      skipShopifyWriteback: true
    });
    assert.equal(capturedQueries.length, 1);
    assert.deepEqual(JSON.parse(String(capturedQueries[0].params?.[3])), {
      startDate: '2026-04-01',
      endDate: '2026-04-05',
      dryRun: false,
      limit: 5000,
      reclassificationTarget: 'full_rebuild',
      organizationIds: [],
      webOrdersOnly: false,
      skipShopifyWriteback: true
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route derives submittedBy from authenticated admin users', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    if (/FROM app_sessions s/.test(text)) {
      return {
        rows: [
          {
            session_id: 9,
            user_id: 73,
            email: 'admin@example.com',
            display_name: 'Admin User',
            is_admin: true,
            status: 'active',
            last_login_at: new Date('2026-04-25T10:00:00.000Z'),
            created_at: new Date('2026-04-01T00:00:00.000Z'),
            expires_at: new Date('2026-05-01T00:00:00.000Z')
          }
        ]
      };
    }

    capturedQueries.push({ text, params });
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill', {
      method: 'POST',
      headers: {
        authorization: 'Bearer admin-session-token'
      },
      body: {
        startDate: '2026-04-01',
        endDate: '2026-04-05'
      }
    });

    assert.equal(response.status, 202);
    assert.equal(body.submittedBy, 'admin@example.com');
    assert.equal(capturedQueries.length, 1);
    assert.equal(capturedQueries[0].params?.[2], 'admin@example.com');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route returns persisted partial reports for failed jobs', async () => {
  pool.query = (async () => ({
    rows: [
      {
        id: 'job-failed',
        status: 'failed',
        submitted_at: new Date('2026-04-25T10:00:00.000Z'),
        submitted_by: 'internal',
        started_at: new Date('2026-04-25T10:00:05.000Z'),
        completed_at: new Date('2026-04-25T10:01:00.000Z'),
        options: {
          startDate: '2026-04-01',
          endDate: '2026-04-05',
          dryRun: false,
          limit: 500,
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          webOrdersOnly: true,
          skipShopifyWriteback: false
        },
        report: {
          scanned: 12,
          recovered: 4,
          unrecoverable: 3,
          writebackCompleted: 2,
          dryRun: false,
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          beforeCounts: emptyTierCounts,
          afterCounts: emptyTierCounts,
          failures: [
            {
              orderId: 'order-9',
              code: 'order_processing_failed',
              message: 'Timed out while refreshing daily reporting metrics'
            }
          ]
        },
        error_code: 'DatabaseTimeout',
        error_message: 'database timeout while scanning orders'
      }
    ]
  })) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/orders/backfill/job-failed', {
      headers: {
        authorization: 'Bearer test-reporting-token'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(body.ok, true);
    assert.equal(body.jobId, 'job-failed');
    assert.equal(body.status, 'failed');
    assert.deepEqual(body.report, {
      scanned: 12,
      recovered: 4,
      unrecoverable: 3,
      writebackCompleted: 2,
      dryRun: false,
      reclassificationTarget: 'full_rebuild',
      organizationIds: [],
      beforeCounts: emptyTierCounts,
      afterCounts: emptyTierCounts,
      failures: [
        {
          orderId: 'order-9',
          code: 'order_processing_failed',
          message: 'Timed out while refreshing daily reporting metrics'
        }
      ]
    });
    assert.deepEqual(body.error, {
      code: 'DatabaseTimeout',
      message: 'database timeout while scanning orders'
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('admin debug campaign resolver route is admin guarded and writes an audit entry', async () => {
  const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

  pool.query = (async (text: string, params?: unknown[]) => {
    capturedQueries.push({ text, params });

    if (/FROM campaign_metadata_resolver_rules/.test(text)) {
      return {
        rows: [
          {
            id: 'rule-1',
            resolver_version: 'campaign_metadata_resolver_v1',
            rule_kind: 'override',
            priority: 1,
            match_platform: 'google_ads',
            match_source: null,
            match_medium: null,
            match_campaign: null,
            match_content: null,
            match_term: null,
            match_account_id: 'acct-1',
            match_campaign_id: 'cmp-1',
            match_adset_id: null,
            match_ad_id: null,
            match_expression: {},
            canonical_campaign_id: 'cmp-1',
            canonical_campaign_name: 'Brand Search',
            canonical_source: 'google',
            canonical_medium: 'cpc',
            canonical_channel: 'paid_search',
            canonical_channel_group: 'paid_media',
            hierarchy_metadata: {
              accountId: 'acct-1'
            },
            confidence: '0.9900',
            source_label: 'qa_override'
          }
        ]
      };
    }

    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/admin/attribution/debug/campaign-resolver', {
      method: 'POST',
      headers: {
        authorization: 'Bearer test-reporting-token'
      },
      body: {
        platform: 'google_ads',
        accountId: 'acct-1',
        campaignId: 'cmp-1',
        enqueueUnmapped: false
      }
    });

    assert.equal(response.status, 200);
    assert.equal(body.resolution.status, 'resolved');
    assert.equal(body.resolution.ruleId, 'rule-1');
    assert.equal(body.resolution.canonical.campaignName, 'Brand Search');

    const auditQuery = capturedQueries.find((call) => /INSERT INTO admin_debug_audit_log/.test(call.text));
    assert.ok(auditQuery);
    assert.equal(auditQuery.params?.[0], 'internal');
    assert.equal(auditQuery.params?.[2], 'internal@system');
    assert.equal(auditQuery.params?.[3], 'campaign_resolver_debug');
    assert.equal(auditQuery.params?.[4], 'campaign_metadata');
    assert.equal(auditQuery.params?.[5], 'cmp-1');
    assert.deepEqual(JSON.parse(String(auditQuery.params?.[7])), {
      status: 'resolved',
      source: 'override',
      ruleId: 'rule-1',
      qaQueueId: null
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('order attribution backfill admin route returns queued and completed polling payloads with the shared status contract', async () => {
  const rowsByJobId = new Map([
    [
      'job-queued',
      {
        id: 'job-queued',
        status: 'queued',
        submitted_at: new Date('2026-04-25T10:00:00.000Z'),
        submitted_by: 'internal',
        started_at: null,
        completed_at: null,
        options: {
          startDate: '2026-04-01',
          endDate: '2026-04-05',
          dryRun: true,
          limit: 500,
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          webOrdersOnly: true,
          skipShopifyWriteback: false
        },
        report: null,
        error_code: null,
        error_message: null
      }
    ],
    [
      'job-completed',
      {
        id: 'job-completed',
        status: 'completed',
        submitted_at: new Date('2026-04-25T11:00:00.000Z'),
        submitted_by: 'admin@example.com',
        started_at: new Date('2026-04-25T11:00:05.000Z'),
        completed_at: new Date('2026-04-25T11:01:00.000Z'),
        options: {
          startDate: '2026-04-06',
          endDate: '2026-04-08',
          dryRun: false,
          limit: 5000,
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          webOrdersOnly: false,
          skipShopifyWriteback: true
        },
        report: {
          scanned: 100,
          recovered: 9,
          unrecoverable: 9,
          writebackCompleted: 0,
          dryRun: false,
          reclassificationTarget: 'full_rebuild',
          organizationIds: [],
          beforeCounts: emptyTierCounts,
          afterCounts: emptyTierCounts,
          failures: [
            {
              orderId: 'order-22',
              code: 'shopify_timeout',
              message: 'Shopify API timed out while checking writeback state'
            }
          ]
        },
        error_code: null,
        error_message: null
      }
    ]
  ]);

  pool.query = (async (_text: string, params?: unknown[]) => ({
    rows: params?.[0] ? [rowsByJobId.get(String(params[0]))].filter(Boolean) : []
  })) as typeof pool.query;

  const server = createServer();

  try {
    const queuedResult = await requestJson(server, '/api/admin/attribution/orders/backfill/job-queued', {
      headers: {
        authorization: 'Bearer test-reporting-token'
      }
    });
    const completedResult = await requestJson(server, '/api/admin/attribution/orders/backfill/job-completed', {
      headers: {
        authorization: 'Bearer test-reporting-token'
      }
    });

    assert.equal(queuedResult.response.status, 200);
    assert.deepEqual(queuedResult.body, {
      ok: true,
      jobId: 'job-queued',
      status: 'queued',
      submittedAt: '2026-04-25T10:00:00.000Z',
      submittedBy: 'internal',
      startedAt: null,
      completedAt: null,
      options: {
        startDate: '2026-04-01',
        endDate: '2026-04-05',
        dryRun: true,
        limit: 500,
        reclassificationTarget: 'full_rebuild',
        organizationIds: [],
        webOrdersOnly: true,
        skipShopifyWriteback: false
      },
      report: null,
      error: null
    });

    assert.equal(completedResult.response.status, 200);
    assert.deepEqual(completedResult.body, {
      ok: true,
      jobId: 'job-completed',
      status: 'completed',
      submittedAt: '2026-04-25T11:00:00.000Z',
      submittedBy: 'admin@example.com',
      startedAt: '2026-04-25T11:00:05.000Z',
      completedAt: '2026-04-25T11:01:00.000Z',
      options: {
        startDate: '2026-04-06',
        endDate: '2026-04-08',
        dryRun: false,
        limit: 5000,
        reclassificationTarget: 'full_rebuild',
        organizationIds: [],
        webOrdersOnly: false,
        skipShopifyWriteback: true
      },
      report: {
        scanned: 100,
        recovered: 9,
        unrecoverable: 9,
        writebackCompleted: 0,
        dryRun: false,
        reclassificationTarget: 'full_rebuild',
        organizationIds: [],
        beforeCounts: emptyTierCounts,
        afterCounts: emptyTierCounts,
        failures: [
          {
            orderId: 'order-22',
            code: 'shopify_timeout',
            message: 'Shopify API timed out while checking writeback state'
          }
        ]
      },
      error: null
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});
