import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import type { Pool } from 'pg';

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
      resetE2EDatabase: typeof import('./e2e-harness.js').resetE2EDatabase;
    }
  | null = null;

async function getModules() {
  if (cachedModules) {
    return cachedModules;
  }

  const [poolModule, serverModule, attributionModule, harnessModule] = await Promise.all([
    import('../src/db/pool.js'),
    import('../src/server.js'),
    import('../src/modules/attribution/index.js'),
    import('./e2e-harness.js')
  ]);

  cachedModules = {
    pool: poolModule.pool,
    createServer: serverModule.createServer,
    closeServer: serverModule.closeServer,
    enqueueAttributionForOrder: attributionModule.enqueueAttributionForOrder,
    processAttributionQueue: attributionModule.processAttributionQueue,
    resetE2EDatabase: harnessModule.resetE2EDatabase
  };

  return cachedModules;
}

type TrackingSessionInput = {
  firstSeenAt: string;
  lastSeenAt?: string;
  landingPage?: string | null;
  referrerUrl?: string | null;
  utmSource?: string | null;
  utmMedium?: string | null;
  utmCampaign?: string | null;
  gclid?: string | null;
  fbclid?: string | null;
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
  gclid?: string | null;
  fbclid?: string | null;
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
  sourceName?: string;
  rawPayload?: Record<string, unknown>;
};

function buildReportingHeaders(): Record<string, string> {
  return {
    authorization: 'Bearer test-reporting-token',
    accept: 'application/json'
  };
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

async function insertTrackingSession(pool: Pool, input: TrackingSessionInput): Promise<string> {
  const result = await pool.query<{ id: string }>(
    `
      INSERT INTO tracking_sessions (
        first_seen_at,
        last_seen_at,
        landing_page,
        referrer_url,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_gclid,
        initial_fbclid
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9
      )
      RETURNING id::text
    `,
    [
      input.firstSeenAt,
      input.lastSeenAt ?? input.firstSeenAt,
      input.landingPage ?? null,
      input.referrerUrl ?? null,
      input.utmSource ?? null,
      input.utmMedium ?? null,
      input.utmCampaign ?? null,
      input.gclid ?? null,
      input.fbclid ?? null
    ]
  );

  return result.rows[0].id;
}

async function insertTrackingEvent(pool: Pool, input: TrackingEventInput): Promise<void> {
  const rawPayloadFixture = buildRawPayloadFixture({});

  await pool.query(
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
        fbclid,
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
        $15::jsonb
      )
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
      input.gclid ?? null,
      input.fbclid ?? null,
      input.shopifyCheckoutToken ?? null,
      input.shopifyCartToken ?? null,
      rawPayloadFixture.payloadSizeBytes,
      rawPayloadFixture.payloadHash,
      rawPayloadFixture.rawPayloadJson
    ]
  );
}

async function insertShopifyOrder(pool: Pool, input: ShopifyOrderInput): Promise<void> {
  const rawPayload = {
    id: input.shopifyOrderId,
    source_name: input.sourceName ?? 'web',
    processed_at: input.processedAt,
    ...(input.rawPayload ?? {})
  };
  const rawPayloadFixture = buildRawPayloadFixture(rawPayload, input.shopifyOrderId);

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
        $8,
        $9,
        $10,
        $11,
        $12::jsonb,
        now()
      )
    `,
    [
      input.shopifyOrderId,
      input.subtotalPrice ?? input.totalPrice ?? '100.00',
      input.totalPrice ?? '100.00',
      input.processedAt,
      input.landingSessionId ?? null,
      input.checkoutToken ?? null,
      input.cartToken ?? null,
      input.sourceName ?? 'web',
      rawPayloadFixture.payloadExternalId,
      rawPayloadFixture.payloadSizeBytes,
      rawPayloadFixture.payloadHash,
      rawPayloadFixture.rawPayloadJson
    ]
  );
}

async function processOrders(shopifyOrderIds: string[]): Promise<void> {
  const { enqueueAttributionForOrder, processAttributionQueue } = await getModules();

  for (const shopifyOrderId of shopifyOrderIds) {
    await enqueueAttributionForOrder(shopifyOrderId, 'test_attribution_tier_data_flow');
  }

  const report = await processAttributionQueue({
    workerId: 'test-attribution-tier-data-flow',
    limit: shopifyOrderIds.length + 2,
    staleScanLimit: 0,
    emitMetrics: false
  });

  assert.equal(report.succeededJobs, shopifyOrderIds.length);
  assert.equal(report.failedJobs, 0);
}

async function fetchPersistedTiers(pool: Pool, shopifyOrderIds: string[]) {
  const result = await pool.query<{
    shopify_order_id: string;
    attribution_tier: string | null;
    attribution_source: string | null;
    attribution_reason: string | null;
  }>(
    `
      SELECT
        shopify_order_id,
        attribution_tier,
        attribution_source,
        attribution_reason
      FROM shopify_orders
      WHERE shopify_order_id = ANY($1::text[])
      ORDER BY processed_at DESC, shopify_order_id DESC
    `,
    [shopifyOrderIds]
  );

  return result.rows;
}

async function fetchAttributionResultCounts(pool: Pool, shopifyOrderIds: string[]) {
  const result = await pool.query<{ shopify_order_id: string; count: string }>(
    `
      SELECT shopify_order_id, COUNT(*)::text AS count
      FROM attribution_results
      WHERE shopify_order_id = ANY($1::text[])
      GROUP BY shopify_order_id
      ORDER BY shopify_order_id ASC
    `,
    [shopifyOrderIds]
  );

  return new Map(result.rows.map((row) => [row.shopify_order_id, Number(row.count)]));
}

function stripMatchedAt(row: Record<string, unknown>) {
  const { attributionMatchedAt, ...rest } = row;
  return {
    ...rest,
    attributionMatchedAt: typeof attributionMatchedAt === 'string' ? '<dynamic>' : attributionMatchedAt
  };
}

test.beforeEach(async () => {
  const { resetE2EDatabase } = await getModules();
  await resetE2EDatabase();
});

test.after(async () => {
  const { pool, resetE2EDatabase } = await getModules();
  await resetE2EDatabase();
  await pool.end();
});

test('attribution tier precedence persists once per order and is exposed consistently through reporting APIs', async () => {
  const { pool, createServer, closeServer } = await getModules();
  const processedDate = '2026-04-10';
  const firstPartyOrderId = 'order-tier-first-party-1';
  const shopifyHintOrderId = 'order-tier-shopify-hint-1';
  const unattributedOrderId = 'order-tier-unattributed-1';

  const firstPartySessionId = await insertTrackingSession(pool, {
    firstSeenAt: '2026-04-10T12:00:00.000Z',
    landingPage:
      'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search',
    referrerUrl: 'https://www.google.com/search?q=widget',
    utmSource: 'google',
    utmMedium: 'cpc',
    utmCampaign: 'brand-search',
    gclid: 'gclid-tier-1'
  });

  await insertTrackingEvent(pool, {
    sessionId: firstPartySessionId,
    eventType: 'page_view',
    occurredAt: '2026-04-10T12:00:00.000Z',
    pageUrl: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search',
    referrerUrl: 'https://www.google.com/search?q=widget',
    utmSource: 'google',
    utmMedium: 'cpc',
    utmCampaign: 'brand-search',
    gclid: 'gclid-tier-1'
  });

  await insertShopifyOrder(pool, {
    shopifyOrderId: firstPartyOrderId,
    processedAt: '2026-04-10T12:20:00.000Z',
    totalPrice: '125.00',
    landingSessionId: firstPartySessionId,
    rawPayload: {
      landing_site:
        'https://store.example/products/widget?utm_source=klaviyo&utm_medium=email&utm_campaign=should-not-win',
      note_attributes: [{ name: 'fbclid', value: 'FB-CLICK-SHOULD-NOT-WIN' }]
    }
  });

  await insertShopifyOrder(pool, {
    shopifyOrderId: shopifyHintOrderId,
    processedAt: '2026-04-10T12:10:00.000Z',
    totalPrice: '95.00',
    rawPayload: {
      landing_site: 'https://store.example/products/widget?fbclid=FB-HINT-123',
      note_attributes: [{ name: 'utm_campaign', value: 'fallback-social' }]
    }
  });

  await insertShopifyOrder(pool, {
    shopifyOrderId: unattributedOrderId,
    processedAt: '2026-04-10T12:05:00.000Z',
    totalPrice: '80.00',
    rawPayload: {
      landing_site: 'https://store.example/products/widget'
    }
  });

  await processOrders([firstPartyOrderId, shopifyHintOrderId, unattributedOrderId]);

  const persistedTiers = await fetchPersistedTiers(pool, [firstPartyOrderId, shopifyHintOrderId, unattributedOrderId]);
  assert.deepEqual(persistedTiers, [
    {
      shopify_order_id: firstPartyOrderId,
      attribution_tier: 'deterministic_first_party',
      attribution_source: 'landing_session_id',
      attribution_reason: 'matched_by_landing_session'
    },
    {
      shopify_order_id: shopifyHintOrderId,
      attribution_tier: 'deterministic_shopify_hint',
      attribution_source: 'shopify_marketing_hint',
      attribution_reason: 'shopify_hint_derived'
    },
    {
      shopify_order_id: unattributedOrderId,
      attribution_tier: 'unattributed',
      attribution_source: 'unattributed',
      attribution_reason: 'unattributed'
    }
  ]);

  const attributionResultCounts = await fetchAttributionResultCounts(pool, [
    firstPartyOrderId,
    shopifyHintOrderId,
    unattributedOrderId
  ]);
  assert.equal(attributionResultCounts.get(firstPartyOrderId), 1);
  assert.equal(attributionResultCounts.get(shopifyHintOrderId), 1);
  assert.equal(attributionResultCounts.get(unattributedOrderId), 1);

  const server = createServer();

  try {
    const ordersResponse = await requestJson(
      server,
      `/api/reporting/orders?startDate=${processedDate}&endDate=${processedDate}&limit=10`
    );

    assert.equal(ordersResponse.response.status, 200);
    const orderRows = ((ordersResponse.body.rows ?? []) as Array<Record<string, unknown>>).map(stripMatchedAt);
    assert.deepEqual(orderRows, [
      {
        shopifyOrderId: firstPartyOrderId,
        processedAt: '2026-04-10T12:20:00.000Z',
        orderOccurredAtUtc: '2026-04-10T12:20:00.000Z',
        totalPrice: 125,
        source: 'google',
        medium: 'cpc',
        campaign: 'brand-search',
        attributionReason: 'matched_by_landing_session',
        primaryCreditAttributionReason: 'matched_by_landing_session',
        attributionTier: 'deterministic_first_party',
        attributionTierLabel: 'Deterministic first-party',
        attributionTierDescription:
          'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
        attributionSource: 'landing_session_id',
        attributionMatchedAt: '<dynamic>',
        confidenceScore: 1,
        sessionId: firstPartySessionId
      },
      {
        shopifyOrderId: shopifyHintOrderId,
        processedAt: '2026-04-10T12:10:00.000Z',
        orderOccurredAtUtc: '2026-04-10T12:10:00.000Z',
        totalPrice: 95,
        source: 'meta',
        medium: 'paid_social',
        campaign: 'fallback-social',
        attributionReason: 'shopify_hint_derived',
        primaryCreditAttributionReason: 'shopify_hint_derived',
        attributionTier: 'deterministic_shopify_hint',
        attributionTierLabel: 'Deterministic Shopify hint',
        attributionTierDescription:
          'Recovered synthetically from Shopify marketing hints after first-party resolution failed.',
        attributionSource: 'shopify_marketing_hint',
        attributionMatchedAt: '<dynamic>',
        confidenceScore: 0.55,
        sessionId: null
      },
      {
        shopifyOrderId: unattributedOrderId,
        processedAt: '2026-04-10T12:05:00.000Z',
        orderOccurredAtUtc: '2026-04-10T12:05:00.000Z',
        totalPrice: 80,
        source: null,
        medium: null,
        campaign: null,
        attributionReason: 'unattributed',
        primaryCreditAttributionReason: 'unattributed',
        attributionTier: 'unattributed',
        attributionTierLabel: 'Unattributed',
        attributionTierDescription:
          'No eligible first-party, Shopify hint, or GA4 fallback match qualified, or the required timing data could not be normalized.',
        attributionSource: 'unattributed',
        attributionMatchedAt: '<dynamic>',
        confidenceScore: 0,
        sessionId: null
      }
    ]);
    assert.deepEqual(ordersResponse.body, {
      rows: [
        {
          shopifyOrderId: firstPartyOrderId,
          processedAt: '2026-04-10T12:20:00.000Z',
          orderOccurredAtUtc: '2026-04-10T12:20:00.000Z',
          totalPrice: 125,
          source: 'google',
          medium: 'cpc',
          campaign: 'brand-search',
          attributionReason: 'matched_by_landing_session',
          primaryCreditAttributionReason: 'matched_by_landing_session',
          attributionTier: 'deterministic_first_party',
          attributionTierLabel: 'Deterministic first-party',
          attributionTierDescription:
            'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
          attributionSource: 'landing_session_id',
          attributionMatchedAt: (ordersResponse.body.rows as Array<Record<string, unknown>>)[0].attributionMatchedAt,
          confidenceScore: 1,
          sessionId: firstPartySessionId
        },
        {
          shopifyOrderId: shopifyHintOrderId,
          processedAt: '2026-04-10T12:10:00.000Z',
          orderOccurredAtUtc: '2026-04-10T12:10:00.000Z',
          totalPrice: 95,
          source: 'meta',
          medium: 'paid_social',
          campaign: 'fallback-social',
          attributionReason: 'shopify_hint_derived',
          primaryCreditAttributionReason: 'shopify_hint_derived',
          attributionTier: 'deterministic_shopify_hint',
          attributionTierLabel: 'Deterministic Shopify hint',
          attributionTierDescription:
            'Recovered synthetically from Shopify marketing hints after first-party resolution failed.',
          attributionSource: 'shopify_marketing_hint',
          attributionMatchedAt: (ordersResponse.body.rows as Array<Record<string, unknown>>)[1].attributionMatchedAt,
          confidenceScore: 0.55,
          sessionId: null
        },
        {
          shopifyOrderId: unattributedOrderId,
          processedAt: '2026-04-10T12:05:00.000Z',
          orderOccurredAtUtc: '2026-04-10T12:05:00.000Z',
          totalPrice: 80,
          source: null,
          medium: null,
          campaign: null,
          attributionReason: 'unattributed',
          primaryCreditAttributionReason: 'unattributed',
          attributionTier: 'unattributed',
          attributionTierLabel: 'Unattributed',
          attributionTierDescription:
            'No eligible first-party, Shopify hint, or GA4 fallback match qualified, or the required timing data could not be normalized.',
          attributionSource: 'unattributed',
          attributionMatchedAt: (ordersResponse.body.rows as Array<Record<string, unknown>>)[2].attributionMatchedAt,
          confidenceScore: 0,
          sessionId: null
        }
      ]
    });

    for (const row of (ordersResponse.body.rows ?? []) as Array<Record<string, unknown>>) {
      assert.match(String(row.attributionMatchedAt), /^\d{4}-\d{2}-\d{2}T/);
    }

    const filteredOrdersResponse = await requestJson(
      server,
      `/api/reporting/orders?startDate=${processedDate}&endDate=${processedDate}&attributionTier=deterministic_shopify_hint&limit=10`
    );

    assert.equal(filteredOrdersResponse.response.status, 200);
    assert.deepEqual(filteredOrdersResponse.body, {
      rows: [
        {
          shopifyOrderId: shopifyHintOrderId,
          processedAt: '2026-04-10T12:10:00.000Z',
          orderOccurredAtUtc: '2026-04-10T12:10:00.000Z',
          totalPrice: 95,
          source: 'meta',
          medium: 'paid_social',
          campaign: 'fallback-social',
          attributionReason: 'shopify_hint_derived',
          primaryCreditAttributionReason: 'shopify_hint_derived',
          attributionTier: 'deterministic_shopify_hint',
          attributionTierLabel: 'Deterministic Shopify hint',
          attributionTierDescription:
            'Recovered synthetically from Shopify marketing hints after first-party resolution failed.',
          attributionSource: 'shopify_marketing_hint',
          attributionMatchedAt: (filteredOrdersResponse.body.rows as Array<Record<string, unknown>>)[0].attributionMatchedAt,
          confidenceScore: 0.55,
          sessionId: null
        }
      ]
    });

    const firstPartyDetails = await requestJson(server, `/api/reporting/orders/${firstPartyOrderId}`);
    assert.equal(firstPartyDetails.response.status, 200);
    assert.equal(firstPartyDetails.body.order?.attributionTier, 'deterministic_first_party');
    assert.equal(firstPartyDetails.body.order?.attributionSource, 'landing_session_id');
    assert.equal(firstPartyDetails.body.order?.attributionReason, 'matched_by_landing_session');
    assert.equal(firstPartyDetails.body.order?.sessionId, firstPartySessionId);
    assert.equal(firstPartyDetails.body.order?.attributedSource, 'google');
    assert.equal(firstPartyDetails.body.order?.attributedMedium, 'cpc');
    assert.equal(firstPartyDetails.body.order?.attributedCampaign, 'brand-search');

    const shopifyHintDetails = await requestJson(server, `/api/reporting/orders/${shopifyHintOrderId}`);
    assert.equal(shopifyHintDetails.response.status, 200);
    assert.equal(shopifyHintDetails.body.order?.attributionTier, 'deterministic_shopify_hint');
    assert.equal(shopifyHintDetails.body.order?.attributionSource, 'shopify_marketing_hint');
    assert.equal(shopifyHintDetails.body.order?.attributionReason, 'shopify_hint_derived');
    assert.equal(shopifyHintDetails.body.order?.sessionId, null);
    assert.equal(shopifyHintDetails.body.order?.attributedSource, 'meta');
    assert.equal(shopifyHintDetails.body.order?.attributedMedium, 'paid_social');
    assert.equal(shopifyHintDetails.body.order?.attributedCampaign, 'fallback-social');
    assert.equal(shopifyHintDetails.body.order?.attributedClickIdType, 'fbclid');
    assert.equal(shopifyHintDetails.body.order?.attributedClickIdValue, 'FB-HINT-123');
  } finally {
    await closeServer(server);
  }
});
