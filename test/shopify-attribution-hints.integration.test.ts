import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import type { Pool } from "pg";

import { buildRawPayloadFixture, resetIntegrationTables } from './integration-test-helpers.js';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';
process.env.SHOPIFY_APP_API_SECRET ??= 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET ??= 'test-webhook-secret';

async function getModules() {
	const poolModule = await import("../src/db/pool.js");
	const attributionModule = await import("../src/modules/attribution/index.js");
	const shopifyModule = await import("../src/modules/shopify/index.js");

	return {
		pool: poolModule.pool,
		enqueueAttributionForOrder: attributionModule.enqueueAttributionForOrder,
		processAttributionQueue: attributionModule.processAttributionQueue,
		shopifyTestUtils: shopifyModule.__shopifyTestUtils,
	};
}

type TrackingSessionInput = {
	firstSeenAt: string;
	lastSeenAt?: string;
	landingPage?: string | null;
	utmSource?: string | null;
	utmMedium?: string | null;
	utmCampaign?: string | null;
	gclid?: string | null;
};

type TrackingEventInput = {
	sessionId: string;
	eventType: string;
	occurredAt: string;
	pageUrl?: string | null;
	utmSource?: string | null;
	utmMedium?: string | null;
	utmCampaign?: string | null;
	gclid?: string | null;
	shopifyCheckoutToken?: string | null;
	shopifyCartToken?: string | null;
};

type ShopifyOrderInput = {
	shopifyOrderId: string;
	processedAt: string;
	landingSessionId?: string | null;
	checkoutToken?: string | null;
	cartToken?: string | null;
	sourceName?: string;
	rawPayload?: string;
};

async function insertTrackingSession(
	pool: Pool,
	input: TrackingSessionInput,
): Promise<string> {
	const result = await pool.query<{ id: string }>(
		`
      INSERT INTO tracking_sessions (
        first_seen_at,
        last_seen_at,
        landing_page,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_gclid
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id::text
    `,
		[
			input.firstSeenAt,
			input.lastSeenAt ?? input.firstSeenAt,
			input.landingPage ?? null,
			input.utmSource ?? null,
			input.utmMedium ?? null,
			input.utmCampaign ?? null,
			input.gclid ?? null,
		],
	);

	return result.rows[0].id;
}

async function insertTrackingEvent(pool: Pool, input: TrackingEventInput): Promise<void> {
  const payload = buildRawPayloadFixture({});

  await pool.query(
    `
      INSERT INTO tracking_events (
        session_id,
        event_type,
        occurred_at,
        page_url,
        utm_source,
        utm_medium,
        utm_campaign,
        gclid,
        shopify_checkout_token,
        shopify_cart_token,
        raw_payload,
        payload_size_bytes,
        payload_hash
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
    `,
    [
      input.sessionId,
      input.eventType,
      input.occurredAt,
      input.pageUrl ?? null,
      input.utmSource ?? null,
      input.utmMedium ?? null,
      input.utmCampaign ?? null,
      input.gclid ?? null,
      input.shopifyCheckoutToken ?? null,
      input.shopifyCartToken ?? null,
      payload.rawPayloadJson,
      payload.payloadSizeBytes,
      payload.payloadHash
    ]
  );
}

async function insertShopifyOrder(pool: Pool, input: ShopifyOrderInput): Promise<void> {
  const payload = buildRawPayloadFixture(
    JSON.parse(input.rawPayload ?? JSON.stringify({ id: input.shopifyOrderId })),
    input.shopifyOrderId
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
        cart_token,
        source_name,
        payload_external_id,
        payload_size_bytes,
        payload_hash,
        raw_payload,
        payload_size_bytes,
        payload_hash,
        ingested_at
      )
      VALUES (
        $1,
        'USD',
        '120.00',
        '120.00',
        $2,
        $3::uuid,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10::jsonb,
        now()
      )
    `,
    [
      input.shopifyOrderId,
      input.processedAt,
      input.landingSessionId ?? null,
      input.checkoutToken ?? null,
      input.cartToken ?? null,
      input.sourceName ?? 'web',
      payload.payloadExternalId,
      payload.payloadSizeBytes,
      payload.payloadHash,
      payload.rawPayloadJson
    ]
  );
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

async function fetchAttributionResult(shopifyOrderId: string) {
	const { pool } = await getModules();

	const result = await pool.query<{
		session_id: string | null;
		attributed_source: string | null;
		attributed_medium: string | null;
		attributed_campaign: string | null;
		attributed_click_id_type: string | null;
		attributed_click_id_value: string | null;
		match_source: string;
		confidence_score: string;
		confidence_label: string;
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
        match_source,
        confidence_score::text,
        confidence_label,
        attribution_reason
      FROM attribution_results
      WHERE shopify_order_id = $1
    `,
		[shopifyOrderId],
	);

	return result.rows[0] ?? null;
}

async function fetchOrderSnapshot(shopifyOrderId: string) {
	const { pool } = await getModules();
	const result = await pool.query<{
		attribution_snapshot: Record<string, unknown> | null;
	}>(
		`
      SELECT attribution_snapshot
      FROM shopify_orders
      WHERE shopify_order_id = $1
    `,
		[shopifyOrderId],
	);

	return result.rows[0]?.attribution_snapshot ?? null;
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

  return result.rows[0] ?? null;
}

async function fetchPendingAttributionJobs(shopifyOrderId: string) {
	const { pool } = await getModules();
	const result = await pool.query<{ count: string }>(
		`
      SELECT COUNT(*)::text AS count
      FROM attribution_jobs
      WHERE shopify_order_id = $1
        AND status IN ('pending', 'retry')
    `,
		[shopifyOrderId],
	);

	return Number(result.rows[0]?.count ?? "0");
}

test("recoverShopifyAttributionHints applies click-id-backed synthetic attribution only for eligible web orders without deterministic matches", async () => {
	await resetIntegrationDatabase();
	const { pool, shopifyTestUtils } = await getModules();

	try {
		await insertShopifyOrder(pool, {
			shopifyOrderId: "order-shopify-hint-click-id-1",
			processedAt: "2026-04-08T09:05:00.000Z",
			sourceName: "web",
			rawPayload: JSON.stringify({
				id: "order-shopify-hint-click-id-1",
				customer: null,
				email: null,
				source_name: "web",
				processed_at: "2026-04-08T09:05:00.000Z",
				landing_site:
					"https://store.example/products/widget?fbclid=FB-CLICK-123",
				line_items: [],
			}),
		});

		const recovery = await shopifyTestUtils.recoverShopifyAttributionHints(
			"UTC",
			"2026-04-08",
			"2026-04-08",
		);
		assert.deepEqual(recovery, {
			rescannedOrders: 1,
			relinkedOrders: 0,
			requeuedOrders: 0,
			shopifyHintAttributedOrders: 1,
		});

		const attributionResult = await fetchAttributionResult(
			"order-shopify-hint-click-id-1",
		);
		assert.deepEqual(attributionResult, {
			session_id: null,
			attributed_source: "meta",
			attributed_medium: "paid_social",
			attributed_campaign: null,
			attributed_click_id_type: "fbclid",
			attributed_click_id_value: "FB-CLICK-123",
			match_source: "shopify_hint_fallback",
			confidence_score: "0.55",
			confidence_label: "low",
			attribution_reason: "shopify_hint_derived",
		});

    const snapshot = await fetchOrderSnapshot('order-shopify-hint-click-id-1');
    assert.ok(snapshot);
    assert.equal(snapshot?.confidenceScore, 0.55);
    assert.deepEqual(snapshot?.winner, {
      sessionId: null,
      sourceTouchEventId: null,
      occurredAt: '2026-04-08T09:05:00.000Z',
      source: 'meta',
      medium: 'paid_social',
      campaign: null,
      content: null,
      term: null,
      clickIdType: 'fbclid',
      clickIdValue: 'FB-CLICK-123',
      attributionReason: 'shopify_hint_derived',
      ingestionSource: 'customer_identity',
      isDirect: false
    });
    assert.deepEqual(snapshot?.timeline, [snapshot?.winner]);

    const orderAudit = await fetchOrderAttributionAudit('order-shopify-hint-click-id-1');
    assert.deepEqual(
      {
        attribution_tier: orderAudit?.attribution_tier,
        attribution_source: orderAudit?.attribution_source,
        attribution_reason: orderAudit?.attribution_reason
      },
      {
        attribution_tier: 'deterministic_first_party',
        attribution_source: 'stitched_identity_journey',
        attribution_reason: 'shopify_hint_derived'
      }
    );
    assert.ok(orderAudit?.attribution_matched_at instanceof Date);

    assert.equal(await fetchPendingAttributionJobs('order-shopify-hint-click-id-1'), 0);
  } finally {
    await resetIntegrationDatabase();
  }
});

test("recoverShopifyAttributionHints ignores non-web orders even when Shopify hints are present", async () => {
	await resetIntegrationDatabase();
	const { pool, shopifyTestUtils } = await getModules();

	try {
		await insertShopifyOrder(pool, {
			shopifyOrderId: "order-shopify-hint-pos-1",
			processedAt: "2026-04-08T11:15:00.000Z",
			sourceName: "pos",
			rawPayload: JSON.stringify({
				id: "order-shopify-hint-pos-1",
				customer: null,
				email: null,
				source_name: "pos",
				processed_at: "2026-04-08T11:15:00.000Z",
				landing_site:
					"https://store.example/products/widget?utm_source=klaviyo&utm_medium=email&utm_campaign=spring-sale",
				line_items: [],
			}),
		});

		const recovery = await shopifyTestUtils.recoverShopifyAttributionHints(
			"UTC",
			"2026-04-08",
			"2026-04-08",
		);
		assert.deepEqual(recovery, {
			rescannedOrders: 0,
			relinkedOrders: 0,
			requeuedOrders: 0,
			shopifyHintAttributedOrders: 0,
		});

		assert.equal(
			await fetchAttributionResult("order-shopify-hint-pos-1"),
			null,
		);
		assert.equal(await fetchOrderSnapshot("order-shopify-hint-pos-1"), null);
		assert.equal(
			await fetchPendingAttributionJobs("order-shopify-hint-pos-1"),
			0,
		);
	} finally {
		await resetIntegrationDatabase();
	}
});

test("recoverShopifyAttributionHints suppresses Shopify fallback when checkout or cart evidence resolves a deterministic session", async () => {
	await resetIntegrationDatabase();
	const { pool, processAttributionQueue, shopifyTestUtils } =
		await getModules();

	try {
		const checkoutSessionId = await insertTrackingSession(pool, {
			firstSeenAt: "2026-04-09T10:00:00.000Z",
			landingPage:
				"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search",
			utmSource: "google",
			utmMedium: "cpc",
			utmCampaign: "brand-search",
			gclid: "gclid-123",
		});
		await insertTrackingEvent(pool, {
			sessionId: checkoutSessionId,
			eventType: "checkout_started",
			occurredAt: "2026-04-09T10:00:00.000Z",
			pageUrl: "https://store.example/checkout",
			utmSource: "google",
			utmMedium: "cpc",
			utmCampaign: "brand-search",
			gclid: "gclid-123",
			shopifyCheckoutToken: "checkout-suppress-1",
		});

		await insertShopifyOrder(pool, {
			shopifyOrderId: "order-shopify-hint-suppressed-1",
			processedAt: "2026-04-09T10:05:00.000Z",
			checkoutToken: "checkout-suppress-1",
			rawPayload: JSON.stringify({
				id: "order-shopify-hint-suppressed-1",
				customer: null,
				email: null,
				source_name: "web",
				processed_at: "2026-04-09T10:05:00.000Z",
				checkout_token: "checkout-suppress-1",
				landing_site:
					"https://store.example/products/widget?utm_source=klaviyo&utm_medium=email",
				note_attributes: [{ name: "fbclid", value: "FB-CLICK-SHOULD-NOT-WIN" }],
				line_items: [],
			}),
		});

		const recovery = await shopifyTestUtils.recoverShopifyAttributionHints(
			"UTC",
			"2026-04-09",
			"2026-04-09",
		);
		assert.deepEqual(recovery, {
			rescannedOrders: 1,
			relinkedOrders: 1,
			requeuedOrders: 1,
			shopifyHintAttributedOrders: 0,
		});

		assert.equal(
			await fetchAttributionResult("order-shopify-hint-suppressed-1"),
			null,
		);
		assert.equal(
			await fetchPendingAttributionJobs("order-shopify-hint-suppressed-1"),
			1,
		);

		const queueResult = await processAttributionQueue({
			workerId: "shopify-hint-suppressed-worker",
			limit: 10,
			staleScanLimit: 0,
			emitMetrics: false,
		});

		assert.equal(queueResult.failedJobs, 0);
		assert.equal(queueResult.succeededJobs, 1);

		const attributionResult = await fetchAttributionResult(
			"order-shopify-hint-suppressed-1",
		);
		assert.deepEqual(attributionResult, {
			session_id: checkoutSessionId,
			attributed_source: "google",
			attributed_medium: "cpc",
			attributed_campaign: "brand-search",
			attributed_click_id_type: "gclid",
			attributed_click_id_value: "gclid-123",
			match_source: "landing_session_id",
			confidence_score: "1.00",
			confidence_label: "high",
			attribution_reason: "matched_by_landing_session",
		});

		const snapshot = await fetchOrderSnapshot(
			"order-shopify-hint-suppressed-1",
		);
		assert.ok(snapshot);
		assert.equal(snapshot?.confidenceScore, 1);
		assert.equal(snapshot?.confidenceLabel, "high");
		assert.deepEqual(snapshot?.winner, {
			sessionId: checkoutSessionId,
			sourceTouchEventId:
				snapshot?.winner && typeof snapshot.winner === "object"
					? (snapshot.winner as Record<string, unknown>).sourceTouchEventId
					: null,
			occurredAt: "2026-04-09T10:00:00.000Z",
			source: "google",
			medium: "cpc",
			campaign: "brand-search",
			content: null,
			term: null,
			clickIdType: "gclid",
			clickIdValue: "gclid-123",
			attributionReason: "matched_by_landing_session",
			matchSource: "landing_session_id",
			confidenceLabel: "high",
			ingestionSource: "landing_session_id",
			ga4ClientId: null,
			ga4SessionId: null,
			isDirect: false,
		});
	} finally {
		await resetIntegrationDatabase();
	}
});

test.after(async () => {
	const { pool } = await getModules();
	await pool.end();
});
