import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import type { AddressInfo } from 'node:net';

import type { Server } from 'node:http';

import { processAttributionQueue } from '../src/modules/attribution/index.js';
import {
  ATTRIBUTION_MODELS,
  computeAttributionOutputs,
  type AttributionCredit,
  type AttributionModel,
  type AttributionTouchpoint
} from '../src/modules/attribution/engine.js';
import { pool } from '../src/db/pool.js';

export const E2E_REPORTING_TOKEN = 'test-reporting-token';

const SHOP_DOMAIN = 'roas-radar-test.myshopify.com';
const WEBHOOK_SECRET = process.env.SHOPIFY_WEBHOOK_SECRET ?? 'test-webhook-secret';

type TrackingEventInput = {
  eventType: 'page_view' | 'checkout_started';
  occurredAt: string;
  sessionId: string;
  pageUrl: string;
  referrerUrl: string | null;
  shopifyCartToken: string | null;
  shopifyCheckoutToken: string | null;
  clientEventId: string;
  context: {
    userAgent: string;
    screen: string;
    language: string;
  };
};

type ReportingSummaryResponse = {
  range: {
    startDate: string;
    endDate: string;
  };
  totals: {
    visits: number;
    orders: number;
    revenue: number;
    spend: number;
    conversionRate: number;
    roas: number | null;
  };
};

type ReportingCampaignsResponse = {
  rows: Array<{
    source: string;
    medium: string;
    campaign: string;
    content: string | null;
    visits: number;
    orders: number;
    revenue: number;
    conversionRate: number;
  }>;
  nextCursor: string | null;
};

type ReportingTimeseriesResponse = {
  points: Array<{
    date: string;
    visits: number;
    orders: number;
    revenue: number;
  }>;
};

type ReportingOrdersResponse = {
  rows: Array<{
    shopifyOrderId: string;
    processedAt: string | null;
    totalPrice: number;
    source: string | null;
    medium: string | null;
    campaign: string | null;
    attributionReason: string;
  }>;
};

export type SeededSyntheticJourney = {
  startDate: string;
  endDate: string;
  multiTouchOrderId: string;
  checkoutOrderId: string;
  multiTouchTouchpoints: AttributionTouchpoint[];
  expectedOutputs: Record<AttributionModel, AttributionCredit[]>;
  processedJobs: number;
};

function isoAtUtcDaysAgo(daysAgo: number, hour: number): string {
  const date = new Date();
  date.setUTCHours(hour, 0, 0, 0);
  date.setUTCDate(date.getUTCDate() - daysAgo);
  return date.toISOString();
}

function dateOnly(isoTimestamp: string): string {
  return isoTimestamp.slice(0, 10);
}

function buildTrackingEvent(input: TrackingEventInput): TrackingEventInput {
  return input;
}

function buildTouchpoint(
  sessionId: string,
  occurredAt: string,
  source: string | null,
  medium: string | null,
  campaign: string | null,
  clickIdType: string | null,
  clickIdValue: string | null,
  attributionReason = 'matched_by_customer_identity',
  isForced = false
): AttributionTouchpoint {
  return {
    sessionId,
    occurredAt: new Date(occurredAt),
    source,
    medium,
    campaign,
    content: null,
    term: null,
    clickIdType,
    clickIdValue,
    attributionReason,
    isDirect: !source && !medium && !campaign && !clickIdValue,
    isForced
  };
}

function buildWebhookHeaders(payload: string, webhookId: string): Record<string, string> {
  const digest = createHmac('sha256', WEBHOOK_SECRET).update(payload).digest('base64');

  return {
    'content-type': 'application/json',
    'x-shopify-hmac-sha256': digest,
    'x-shopify-shop-domain': SHOP_DOMAIN,
    'x-shopify-topic': 'orders/create',
    'x-shopify-webhook-id': webhookId
  };
}

function serverBaseUrl(server: Server): string {
  const address = server.address() as AddressInfo;
  return `http://127.0.0.1:${address.port}`;
}

async function requestJson<T>(
  server: Server,
  path: string,
  init: RequestInit = {},
  expectedStatus = 200
): Promise<T> {
  const response = await fetch(`${serverBaseUrl(server)}${path}`, init);
  const text = await response.text();
  const body = text ? (JSON.parse(text) as T) : (undefined as T);

  assert.equal(response.status, expectedStatus, `${path} returned ${response.status}: ${text}`);

  return body;
}

export async function resetE2EDatabase(): Promise<void> {
  await pool.query(`
    TRUNCATE TABLE
      attribution_jobs,
      attribution_order_credits,
      attribution_results,
      shopify_order_line_items,
      shopify_webhook_receipts,
      shopify_orders,
      shopify_customers,
      tracking_events,
      tracking_sessions,
      customer_identities,
      daily_reporting_metrics,
      daily_campaign_metrics
    RESTART IDENTITY CASCADE
  `);
}

export async function trackSyntheticEvent(server: Server, input: TrackingEventInput): Promise<void> {
  await requestJson(
    server,
    '/track',
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        origin: 'https://store.example.com'
      },
      body: JSON.stringify(buildTrackingEvent(input))
    }
  );
}

export async function sendSyntheticOrderWebhook(
  server: Server,
  payload: Record<string, unknown>,
  webhookId: string
): Promise<void> {
  const rawPayload = JSON.stringify(payload);

  await requestJson(
    server,
    '/webhooks/shopify/orders-create',
    {
      method: 'POST',
      headers: buildWebhookHeaders(rawPayload, webhookId),
      body: rawPayload
    }
  );
}

async function linkSessionsToOrderIdentity(shopifyOrderId: string, sessionIds: string[]): Promise<void> {
  const identityResult = await pool.query<{ customer_identity_id: string | null }>(
    `
      SELECT customer_identity_id::text
      FROM shopify_orders
      WHERE shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );

  const identityId = identityResult.rows[0]?.customer_identity_id;
  assert.ok(identityId, `expected order ${shopifyOrderId} to have a stitched identity`);

  await pool.query(
    `
      UPDATE tracking_sessions
      SET
        customer_identity_id = $1::uuid,
        updated_at = now()
      WHERE id = ANY($2::uuid[])
    `,
    [identityId, sessionIds]
  );

  await pool.query(
    `
      UPDATE tracking_events
      SET customer_identity_id = $1::uuid
      WHERE session_id = ANY($2::uuid[])
    `,
    [identityId, sessionIds]
  );
}

export async function seedSyntheticJourney(server: Server): Promise<SeededSyntheticJourney> {
  const sessionGoogle = '11111111-1111-4111-8111-111111111111';
  const sessionDirect = '22222222-2222-4222-8222-222222222222';
  const sessionMeta = '33333333-3333-4333-8333-333333333333';
  const sessionCheckout = '44444444-4444-4444-8444-444444444444';
  const multiTouchOrderId = 'synthetic-order-multi-touch';
  const checkoutOrderId = 'synthetic-order-checkout';
  const multiTouchAmount = '120.00';
  const checkoutAmount = '80.00';
  const multiTouchOccurredAt = isoAtUtcDaysAgo(2, 14);
  const checkoutOccurredAt = isoAtUtcDaysAgo(1, 15);

  const googleOccurredAt = isoAtUtcDaysAgo(5, 9);
  const directOccurredAt = isoAtUtcDaysAgo(4, 11);
  const metaOccurredAt = isoAtUtcDaysAgo(3, 13);
  const checkoutTouchOccurredAt = isoAtUtcDaysAgo(2, 9);

  await trackSyntheticEvent(server, {
    eventType: 'page_view',
    occurredAt: googleOccurredAt,
    sessionId: sessionGoogle,
    pageUrl: 'https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-search&gclid=gclid-123',
    referrerUrl: 'https://www.google.com/',
    shopifyCartToken: null,
    shopifyCheckoutToken: null,
    clientEventId: 'evt-google-touch',
    context: {
      userAgent: 'Synthetic Test Browser',
      screen: '1440x900',
      language: 'en-US'
    }
  });

  await trackSyntheticEvent(server, {
    eventType: 'page_view',
    occurredAt: directOccurredAt,
    sessionId: sessionDirect,
    pageUrl: 'https://store.example.com/products/widget',
    referrerUrl: null,
    shopifyCartToken: null,
    shopifyCheckoutToken: null,
    clientEventId: 'evt-direct-touch',
    context: {
      userAgent: 'Synthetic Test Browser',
      screen: '1440x900',
      language: 'en-US'
    }
  });

  await trackSyntheticEvent(server, {
    eventType: 'page_view',
    occurredAt: metaOccurredAt,
    sessionId: sessionMeta,
    pageUrl: 'https://store.example.com/products/widget?utm_source=meta&utm_medium=paid_social&utm_campaign=retargeting&fbclid=fbclid-123',
    referrerUrl: 'https://www.instagram.com/',
    shopifyCartToken: null,
    shopifyCheckoutToken: null,
    clientEventId: 'evt-meta-touch',
    context: {
      userAgent: 'Synthetic Test Browser',
      screen: '1440x900',
      language: 'en-US'
    }
  });

  await trackSyntheticEvent(server, {
    eventType: 'checkout_started',
    occurredAt: checkoutTouchOccurredAt,
    sessionId: sessionCheckout,
    pageUrl: 'https://store.example.com/checkout?utm_source=google&utm_medium=cpc&utm_campaign=brand-defense&gclid=gclid-brand-1',
    referrerUrl: 'https://www.google.com/',
    shopifyCartToken: 'cart-checkout-order',
    shopifyCheckoutToken: 'checkout-token-1',
    clientEventId: 'evt-checkout-touch',
    context: {
      userAgent: 'Synthetic Test Browser',
      screen: '1440x900',
      language: 'en-US'
    }
  });

  await sendSyntheticOrderWebhook(
    server,
    {
      id: multiTouchOrderId,
      order_number: 1001,
      customer: {
        id: 'customer-1',
        email: 'customer@example.com'
      },
      email: 'customer@example.com',
      currency: 'USD',
      subtotal_price: multiTouchAmount,
      total_price: multiTouchAmount,
      financial_status: 'paid',
      fulfillment_status: null,
      processed_at: multiTouchOccurredAt,
      created_at: multiTouchOccurredAt,
      updated_at: multiTouchOccurredAt,
      checkout_token: null,
      cart_token: null,
      source_name: 'web',
      note_attributes: [],
      attributes: []
    },
    'webhook-multi-touch'
  );

  await sendSyntheticOrderWebhook(
    server,
    {
      id: checkoutOrderId,
      order_number: 1002,
      customer: {
        id: 'customer-2',
        email: 'checkout@example.com'
      },
      email: 'checkout@example.com',
      currency: 'USD',
      subtotal_price: checkoutAmount,
      total_price: checkoutAmount,
      financial_status: 'paid',
      fulfillment_status: null,
      processed_at: checkoutOccurredAt,
      created_at: checkoutOccurredAt,
      updated_at: checkoutOccurredAt,
      checkout_token: 'checkout-token-1',
      cart_token: 'cart-checkout-order',
      source_name: 'web',
      note_attributes: [],
      attributes: []
    },
    'webhook-checkout-order'
  );

  await linkSessionsToOrderIdentity(multiTouchOrderId, [sessionGoogle, sessionDirect, sessionMeta]);

  const queueResult = await processAttributionQueue({
    workerId: 'e2e-harness',
    limit: 10
  });

  const multiTouchTouchpoints = [
    buildTouchpoint(sessionGoogle, googleOccurredAt, 'google', 'cpc', 'spring-search', 'gclid', 'gclid-123'),
    buildTouchpoint(sessionDirect, directOccurredAt, null, null, null, null, null),
    buildTouchpoint(sessionMeta, metaOccurredAt, 'meta', 'paid_social', 'retargeting', 'fbclid', 'fbclid-123')
  ];

  return {
    startDate: dateOnly(googleOccurredAt),
    endDate: dateOnly(checkoutOccurredAt),
    multiTouchOrderId,
    checkoutOrderId,
    multiTouchTouchpoints,
    expectedOutputs: computeAttributionOutputs(multiTouchTouchpoints, {
      orderRevenue: multiTouchAmount,
      orderOccurredAt: new Date(multiTouchOccurredAt)
    }),
    processedJobs: queueResult.succeededJobs
  };
}

export async function fetchPersistedCredits(
  shopifyOrderId: string,
  attributionModel: AttributionModel
): Promise<Array<{
  source: string | null;
  medium: string | null;
  campaign: string | null;
  revenueCredit: string;
  isPrimary: boolean;
  attributionReason: string;
}>> {
  const result = await pool.query<{
    attributed_source: string | null;
    attributed_medium: string | null;
    attributed_campaign: string | null;
    revenue_credit: string;
    is_primary: boolean;
    attribution_reason: string;
  }>(
    `
      SELECT
        attributed_source,
        attributed_medium,
        attributed_campaign,
        revenue_credit::text,
        is_primary,
        attribution_reason
      FROM attribution_order_credits
      WHERE shopify_order_id = $1
        AND attribution_model = $2
      ORDER BY touchpoint_position ASC
    `,
    [shopifyOrderId, attributionModel]
  );

  return result.rows.map((row) => ({
    source: row.attributed_source,
    medium: row.attributed_medium,
    campaign: row.attributed_campaign,
    revenueCredit: row.revenue_credit,
    isPrimary: row.is_primary,
    attributionReason: row.attribution_reason
  }));
}

export async function fetchReportingSummary(
  server: Server,
  query: URLSearchParams
): Promise<ReportingSummaryResponse> {
  return requestJson<ReportingSummaryResponse>(server, `/api/reporting/summary?${query.toString()}`, {
    headers: {
      authorization: `Bearer ${E2E_REPORTING_TOKEN}`
    }
  });
}

export async function fetchReportingCampaigns(
  server: Server,
  query: URLSearchParams
): Promise<ReportingCampaignsResponse> {
  return requestJson<ReportingCampaignsResponse>(server, `/api/reporting/campaigns?${query.toString()}`, {
    headers: {
      authorization: `Bearer ${E2E_REPORTING_TOKEN}`
    }
  });
}

export async function fetchReportingTimeseries(
  server: Server,
  query: URLSearchParams
): Promise<ReportingTimeseriesResponse> {
  return requestJson<ReportingTimeseriesResponse>(server, `/api/reporting/timeseries?${query.toString()}`, {
    headers: {
      authorization: `Bearer ${E2E_REPORTING_TOKEN}`
    }
  });
}

export async function fetchReportingOrders(
  server: Server,
  query: URLSearchParams
): Promise<ReportingOrdersResponse> {
  return requestJson<ReportingOrdersResponse>(server, `/api/reporting/orders?${query.toString()}`, {
    headers: {
      authorization: `Bearer ${E2E_REPORTING_TOKEN}`
    }
  });
}

export function buildReportingQuery(
  seeded: SeededSyntheticJourney,
  attributionModel: AttributionModel,
  overrides: Record<string, string> = {}
): URLSearchParams {
  const query = new URLSearchParams({
    startDate: seeded.startDate,
    endDate: seeded.endDate,
    attributionModel
  });

  for (const [key, value] of Object.entries(overrides)) {
    query.set(key, value);
  }

  return query;
}

export { ATTRIBUTION_MODELS };
