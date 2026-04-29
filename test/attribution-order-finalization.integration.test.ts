import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import type { Pool } from "pg";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";

async function getModules() {
	const [poolModule, attributionModule, ga4FallbackModule] = await Promise.all([
		import("../src/db/pool.js"),
		import("../src/modules/attribution/index.js"),
		import("../src/modules/attribution/ga4-fallback-candidates.js"),
	]);

	return {
		pool: poolModule.pool,
		enqueueAttributionForOrder: attributionModule.enqueueAttributionForOrder,
		processAttributionQueue: attributionModule.processAttributionQueue,
		upsertGa4FallbackCandidates: ga4FallbackModule.upsertGa4FallbackCandidates,
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
	emailHash?: string | null;
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

async function insertTrackingEvent(
	pool: Pool,
	input: TrackingEventInput,
): Promise<void> {
	const rawPayload = "{}";
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
			rawPayload,
			Buffer.byteLength(rawPayload, "utf8"),
			createHash("sha256").update(rawPayload).digest("hex"),
		],
	);
}

async function insertShopifyOrder(
	pool: Pool,
	input: ShopifyOrderInput,
): Promise<void> {
	const rawPayload =
		input.rawPayload ??
		JSON.stringify({
			id: input.shopifyOrderId,
			source_name: input.sourceName ?? "web",
		});
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
        email_hash,
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
        $8::jsonb,
        $9,
        $10,
        now()
      )
    `,
		[
			input.shopifyOrderId,
			input.processedAt,
			input.landingSessionId ?? null,
			input.checkoutToken ?? null,
			input.cartToken ?? null,
			input.sourceName ?? "web",
			input.emailHash ?? null,
			rawPayload,
			Buffer.byteLength(rawPayload, "utf8"),
			createHash("sha256").update(rawPayload).digest("hex"),
		],
	);
}

async function insertGa4FallbackCandidate(input: {
	shopifyOrderId: string;
	occurredAt: string;
	source?: string | null;
	medium?: string | null;
	campaign?: string | null;
	content?: string | null;
	term?: string | null;
	clickIdType?: string | null;
	clickIdValue?: string | null;
	ga4ClientId: string | null;
	ga4SessionId: string | null;
	emailHash?: string | null;
}): Promise<void> {
	const { upsertGa4FallbackCandidates } = await getModules();
	await upsertGa4FallbackCandidates([
		{
			occurredAt: input.occurredAt,
			ga4UserKey:
				input.ga4ClientId ?? input.ga4SessionId ?? input.shopifyOrderId,
			ga4ClientId: input.ga4ClientId,
			ga4SessionId: input.ga4SessionId,
			transactionId: input.shopifyOrderId,
			emailHash: input.emailHash ?? null,
			customerIdentityId: null,
			source: input.source ?? null,
			medium: input.medium ?? null,
			campaign: input.campaign ?? null,
			content: input.content ?? null,
			term: input.term ?? null,
			clickIdType: input.clickIdType ?? null,
			clickIdValue: input.clickIdValue ?? null,
			sessionHasRequiredFields: true,
			sourceExportHour: input.occurredAt,
			sourceDataset: "ga4_export",
			sourceTableType: "events",
		},
	]);
}

async function processOrder(shopifyOrderId: string) {
	const { enqueueAttributionForOrder, processAttributionQueue } =
		await getModules();

	await enqueueAttributionForOrder(shopifyOrderId, "integration_test");
	const result = await processAttributionQueue({
		workerId: `integration-${shopifyOrderId}`,
		limit: 10,
		staleScanLimit: 0,
		emitMetrics: false,
	});

	assert.equal(result.failedJobs, 0);
	assert.equal(result.succeededJobs, 1);
}

async function resetIntegrationDatabase(): Promise<void> {
	const { pool } = await getModules();

	await pool.query(`
    TRUNCATE TABLE
      event_replay_run_items,
      event_replay_runs,
      event_dead_letters,
      attribution_jobs,
      shopify_order_writeback_jobs,
      ga4_fallback_shadow_comparisons,
      ga4_fallback_candidates,
      attribution_order_credits,
      attribution_results,
      daily_reporting_metrics,
      order_attribution_links,
      session_attribution_touch_events,
      session_attribution_identities,
      shopify_order_line_items,
      shopify_orders,
      shopify_webhook_receipts,
      tracking_events,
      tracking_sessions,
      shopify_customers,
      customer_identities
    RESTART IDENTITY CASCADE
  `);
}

async function fetchAttributionResult(shopifyOrderId: string) {
	const { pool } = await getModules();
	const result = await pool.query<{
		session_id: string | null;
		attributed_source: string | null;
		attributed_medium: string | null;
		attributed_campaign: string | null;
		attributed_content: string | null;
		attributed_term: string | null;
		attributed_click_id_type: string | null;
		attributed_click_id_value: string | null;
		match_source: string;
		confidence_score: string;
		confidence_label: string;
		attribution_reason: string;
		ga4_client_id: string | null;
		ga4_session_id: string | null;
	}>(
		`
      SELECT
        session_id::text AS session_id,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        attributed_content,
        attributed_term,
        attributed_click_id_type,
        attributed_click_id_value,
        match_source,
        confidence_score::text,
        confidence_label,
        attribution_reason,
        ga4_client_id,
        ga4_session_id
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

async function fetchPrimaryCredits(shopifyOrderId: string) {
	const { pool } = await getModules();
	const result = await pool.query<{
		attribution_model: string;
		session_id: string | null;
		attributed_source: string | null;
		attributed_medium: string | null;
		attributed_campaign: string | null;
		attributed_click_id_type: string | null;
		attributed_click_id_value: string | null;
		attribution_reason: string;
		match_source: string;
		confidence_label: string;
		is_primary: boolean;
	}>(
		`
      SELECT
        attribution_model,
        session_id::text AS session_id,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        attributed_click_id_type,
        attributed_click_id_value,
        attribution_reason,
        match_source,
        confidence_label,
        is_primary
      FROM attribution_order_credits
      WHERE shopify_order_id = $1
        AND is_primary = true
      ORDER BY attribution_model ASC
    `,
		[shopifyOrderId],
	);

	return result.rows;
}

async function fetchGa4ShadowComparison(shopifyOrderId: string) {
	const { pool } = await getModules();
	const result = await pool.query<{
		rollout_mode: string;
		current_match_source: string;
		shadow_match_source: string;
		shadow_would_change_winner: boolean;
		shadow_ga4_client_id: string | null;
		shadow_ga4_session_id: string | null;
	}>(
		`
      SELECT
        rollout_mode,
        current_match_source,
        shadow_match_source,
        shadow_would_change_winner,
        shadow_ga4_client_id,
        shadow_ga4_session_id
      FROM ga4_fallback_shadow_comparisons
      WHERE shopify_order_id = $1
    `,
		[shopifyOrderId],
	);

	return result.rows[0] ?? null;
}

test.beforeEach(async () => {
	process.env.GA4_FALLBACK_ROLLOUT_MODE = undefined;
	await resetIntegrationDatabase();
});

test.after(async () => {
	process.env.GA4_FALLBACK_ROLLOUT_MODE = undefined;
	await resetIntegrationDatabase();
	const { pool } = await getModules();
	await pool.end();
});

test("deterministic landing-session attribution suppresses an otherwise eligible GA4 fallback candidate", async () => {
	const { pool } = await getModules();

	const sessionId = await insertTrackingSession(pool, {
		firstSeenAt: "2026-04-10T08:55:00.000Z",
		landingPage:
			"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search",
		utmSource: "google",
		utmMedium: "cpc",
		utmCampaign: "brand-search",
		gclid: "gclid-deterministic",
	});
	await insertTrackingEvent(pool, {
		sessionId,
		eventType: "page_view",
		occurredAt: "2026-04-10T08:55:00.000Z",
		pageUrl:
			"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=brand-search",
		utmSource: "google",
		utmMedium: "cpc",
		utmCampaign: "brand-search",
		gclid: "gclid-deterministic",
	});

	await insertGa4FallbackCandidate({
		shopifyOrderId: "order-ga4-precedence-1",
		occurredAt: "2026-04-10T08:54:00.000Z",
		source: "meta",
		medium: "paid_social",
		campaign: "should-not-win",
		clickIdType: "fbclid",
		clickIdValue: "fbclid-ignored",
		ga4ClientId: "ga4-ignored-client",
		ga4SessionId: "ga4-ignored-session",
	});

	await insertShopifyOrder(pool, {
		shopifyOrderId: "order-ga4-precedence-1",
		processedAt: "2026-04-10T09:05:00.000Z",
		landingSessionId: sessionId,
		rawPayload: JSON.stringify({
			id: "order-ga4-precedence-1",
			source_name: "web",
			landing_session_id: sessionId,
		}),
	});

	await processOrder("order-ga4-precedence-1");

	assert.deepEqual(await fetchAttributionResult("order-ga4-precedence-1"), {
		session_id: sessionId,
		attributed_source: "google",
		attributed_medium: "cpc",
		attributed_campaign: "brand-search",
		attributed_content: null,
		attributed_term: null,
		attributed_click_id_type: "gclid",
		attributed_click_id_value: "gclid-deterministic",
		match_source: "landing_session_id",
		confidence_score: "1.00",
		confidence_label: "high",
		attribution_reason: "matched_by_landing_session",
		ga4_client_id: null,
		ga4_session_id: null,
	});
});

test("Shopify hint fallback suppresses GA4 fallback when deterministic matching fails", async () => {
	const { pool } = await getModules();

	await insertGa4FallbackCandidate({
		shopifyOrderId: "order-ga4-precedence-2",
		occurredAt: "2026-04-11T10:54:00.000Z",
		source: "google",
		medium: "cpc",
		campaign: "ga4-should-not-win",
		clickIdType: "gclid",
		clickIdValue: "ga4-click-id",
		ga4ClientId: "ga4-client-2",
		ga4SessionId: "ga4-session-2",
	});

	await insertShopifyOrder(pool, {
		shopifyOrderId: "order-ga4-precedence-2",
		processedAt: "2026-04-11T11:05:00.000Z",
		rawPayload: JSON.stringify({
			id: "order-ga4-precedence-2",
			source_name: "web",
			landing_site: "https://store.example/products/widget?fbclid=FB-HINT-123",
		}),
	});

	await processOrder("order-ga4-precedence-2");

	assert.deepEqual(await fetchAttributionResult("order-ga4-precedence-2"), {
		session_id: null,
		attributed_source: "meta",
		attributed_medium: "paid_social",
		attributed_campaign: null,
		attributed_content: null,
		attributed_term: null,
		attributed_click_id_type: "fbclid",
		attributed_click_id_value: "FB-HINT-123",
		match_source: "shopify_hint_fallback",
		confidence_score: "0.55",
		confidence_label: "low",
		attribution_reason: "shopify_hint_derived",
		ga4_client_id: null,
		ga4_session_id: null,
	});
});

test("shadow rollout records the GA4 comparison without applying the fallback result", async () => {
	process.env.GA4_FALLBACK_ROLLOUT_MODE = "shadow";
	const { pool } = await getModules();

	await insertGa4FallbackCandidate({
		shopifyOrderId: "order-ga4-shadow-1",
		occurredAt: "2026-04-07T08:55:00.000Z",
		source: "google",
		medium: "cpc",
		campaign: "shadow-campaign",
		clickIdType: "gclid",
		clickIdValue: "gclid-shadow",
		ga4ClientId: "ga4-shadow-client",
		ga4SessionId: "ga4-shadow-session",
	});

	await insertShopifyOrder(pool, {
		shopifyOrderId: "order-ga4-shadow-1",
		processedAt: "2026-04-07T09:05:00.000Z",
		rawPayload: JSON.stringify({
			id: "order-ga4-shadow-1",
			source_name: "web",
			landing_site: "https://store.example/products/widget",
		}),
	});

	await processOrder("order-ga4-shadow-1");

	const attributionResult = await fetchAttributionResult("order-ga4-shadow-1");
	assert.equal(attributionResult?.match_source, "unattributed");
	assert.equal(attributionResult?.confidence_score, "0.00");

	assert.deepEqual(await fetchGa4ShadowComparison("order-ga4-shadow-1"), {
		rollout_mode: "shadow",
		current_match_source: "unattributed",
		shadow_match_source: "ga4_fallback",
		shadow_would_change_winner: true,
		shadow_ga4_client_id: "ga4-shadow-client",
		shadow_ga4_session_id: "ga4-shadow-session",
	});
});

test("live GA4 rollout persists fallback attribution, snapshot provenance, and primary order credits end to end", async () => {
	process.env.GA4_FALLBACK_ROLLOUT_MODE = "on";
	const { pool } = await getModules();

	await insertGa4FallbackCandidate({
		shopifyOrderId: "order-ga4-live-1",
		occurredAt: "2026-04-12T12:55:00.000Z",
		source: "google",
		medium: "cpc",
		campaign: "ga4-live-campaign",
		content: "Hero",
		term: "sandals",
		clickIdType: "gclid",
		clickIdValue: "ga4-live-click",
		ga4ClientId: "ga4-live-client",
		ga4SessionId: "ga4-live-session",
	});

	await insertShopifyOrder(pool, {
		shopifyOrderId: "order-ga4-live-1",
		processedAt: "2026-04-12T13:05:00.000Z",
		rawPayload: JSON.stringify({
			id: "order-ga4-live-1",
			source_name: "web",
			landing_site: "https://store.example/products/widget",
		}),
	});

	await processOrder("order-ga4-live-1");

	assert.deepEqual(await fetchAttributionResult("order-ga4-live-1"), {
		session_id: null,
		attributed_source: "google",
		attributed_medium: "cpc",
		attributed_campaign: "ga4-live-campaign",
		attributed_content: "Hero",
		attributed_term: "sandals",
		attributed_click_id_type: "gclid",
		attributed_click_id_value: "ga4-live-click",
		match_source: "ga4_fallback",
		confidence_score: "0.35",
		confidence_label: "low",
		attribution_reason: "ga4_fallback_derived",
		ga4_client_id: "ga4-live-client",
		ga4_session_id: "ga4-live-session",
	});

	const snapshot = await fetchOrderSnapshot("order-ga4-live-1");
	assert.ok(snapshot);
	assert.equal(snapshot?.confidenceScore, 0.35);
	assert.equal(snapshot?.confidenceLabel, "low");
	assert.deepEqual(snapshot?.winner, {
		sessionId: null,
		sourceTouchEventId: null,
		occurredAt: "2026-04-12T12:55:00.000Z",
		source: "google",
		medium: "cpc",
		campaign: "ga4-live-campaign",
		content: "Hero",
		term: "sandals",
		clickIdType: "gclid",
		clickIdValue: "ga4-live-click",
		attributionReason: "ga4_fallback_derived",
		matchSource: "ga4_fallback",
		confidenceLabel: "low",
		ingestionSource: null,
		ga4ClientId: "ga4-live-client",
		ga4SessionId: "ga4-live-session",
		isDirect: false,
	});
	assert.deepEqual(snapshot?.timeline, [snapshot?.winner]);

	const primaryCredits = await fetchPrimaryCredits("order-ga4-live-1");
	assert.ok(primaryCredits.length >= 2);
	assert.ok(
		primaryCredits.some((credit) => credit.attribution_model === "last_touch"),
	);
	assert.ok(
		primaryCredits.some((credit) => credit.attribution_model === "first_touch"),
	);
	for (const credit of primaryCredits) {
		assert.deepEqual(credit, {
			attribution_model: credit.attribution_model,
			session_id: null,
			attributed_source: "google",
			attributed_medium: "cpc",
			attributed_campaign: "ga4-live-campaign",
			attributed_click_id_type: "gclid",
			attributed_click_id_value: "ga4-live-click",
			attribution_reason: "ga4_fallback_derived",
			match_source: "ga4_fallback",
			confidence_label: "low",
			is_primary: true,
		});
	}

	assert.equal(await fetchGa4ShadowComparison("order-ga4-live-1"), null);
});
