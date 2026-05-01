import assert from "node:assert/strict";
import test from "node:test";
import type { Pool } from "pg";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";

async function getModules() {
	const poolModule = await import("../src/db/pool.js");
	const backfillModule = await import("../src/modules/attribution/backfill.js");
	const writebackModule = await import("../src/modules/shopify/writeback.js");

	return {
		pool: poolModule.pool,
		backfillRecentOrdersWithRecoveredAttribution:
			backfillModule.backfillRecentOrdersWithRecoveredAttribution,
		previewShopifyOrderWritebackAttributes:
			writebackModule.previewShopifyOrderWritebackAttributes,
		shopifyWritebackTestUtils: writebackModule.__shopifyWritebackTestUtils,
	};
}

async function resetIntegrationDatabase() {
	const { pool, shopifyWritebackTestUtils } = await getModules();

	shopifyWritebackTestUtils.reset();

	await pool.query(`
    TRUNCATE TABLE
      attribution_jobs,
      shopify_order_writeback_jobs,
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

async function insertRecoverableOrder(
	pool: Pool,
	input: {
		shopifyOrderId: string;
		checkoutToken: string;
		processedAt: string;
	},
): Promise<string> {
	const sessionResult = await pool.query<{ id: string }>(
		`
      INSERT INTO tracking_sessions (
        first_seen_at,
        last_seen_at,
        landing_page,
        referrer_url,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_utm_content,
        initial_utm_term,
        initial_gclid,
        initial_gbraid,
        initial_wbraid
      )
      VALUES (
        '2026-04-12T10:00:00.000Z',
        '2026-04-12T10:05:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'GCLID-123',
        'GBRAID-123',
        'WBRAID-123'
      )
      RETURNING id::text
    `,
	);
	const sessionId = sessionResult.rows[0].id;

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
        utm_content,
        utm_term,
        gclid,
        gbraid,
        wbraid,
        shopify_checkout_token,
        payload_size_bytes,
        raw_payload
      )
      VALUES (
        $1::uuid,
        'checkout_started',
        '2026-04-12T10:04:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'GCLID-123',
        'GBRAID-123',
        'WBRAID-123',
        $2,
        octet_length(convert_to('{}', 'utf8')),
        '{}'::jsonb
      )
    `,
		[sessionId, input.checkoutToken],
	);

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
        initial_utm_campaign,
        initial_utm_content,
        initial_utm_term,
        initial_gclid,
        initial_gbraid,
        initial_wbraid
      )
      VALUES (
        $1::uuid,
        '2026-04-12T10:00:00.000Z',
        '2026-04-12T10:05:00.000Z',
        '2026-05-12T10:05:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'GCLID-123',
        'GBRAID-123',
        'WBRAID-123'
      )
    `,
		[sessionId],
	);

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
        utm_content,
        utm_term,
        gclid,
        gbraid,
        wbraid,
        payload_size_bytes,
        raw_payload
      )
      VALUES (
        $1::uuid,
        'checkout_started',
        '2026-04-12T10:04:00.000Z',
        '2026-04-12T10:04:05.000Z',
        '2026-05-12T10:04:05.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'GCLID-123',
        'GBRAID-123',
        'WBRAID-123',
        octet_length(convert_to('{}', 'utf8')),
        '{}'::jsonb
      )
    `,
		[sessionId],
	);

	await pool.query(
		`
      INSERT INTO shopify_orders (
        shopify_order_id,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        checkout_token,
        source_name,
        raw_payload,
        payload_size_bytes,
        ingested_at
      )
      VALUES (
        $1,
        'USD',
        '120.00',
        '120.00',
        $2,
        $3,
        'web',
        $4::jsonb,
        octet_length(convert_to($4::text, 'utf8')),
        now()
      )
    `,
		[
			input.shopifyOrderId,
			input.processedAt,
			input.checkoutToken,
			JSON.stringify({
				id: input.shopifyOrderId,
				source_name: "web",
				note_attributes: [],
			}),
		],
	);

	return sessionId;
}

test("dry-run reports recoverable recent orders without mutating attribution records", async () => {
	await resetIntegrationDatabase();
	const { pool, backfillRecentOrdersWithRecoveredAttribution } =
		await getModules();

	try {
		await insertRecoverableOrder(pool, {
			shopifyOrderId: "order-backfill-dry-run-1",
			checkoutToken: "checkout-backfill-dry-run-1",
			processedAt: "2026-04-12T10:05:00.000Z",
		});

		const report = await backfillRecentOrdersWithRecoveredAttribution({
			requestedBy: "dry-run-review",
			workerId: "test-backfill-dry-run",
			windowStart: new Date("2026-04-12T00:00:00.000Z"),
			windowEnd: new Date("2026-04-12T23:59:59.999Z"),
			dryRun: true,
		});

		assert.equal(report.dryRun, true);
		assert.equal(report.beforeMetrics.ordersMissingAttribution, 1);
		assert.equal(report.afterMetrics.ordersMissingAttribution, 1);
		assert.equal(report.scannedOrders, 1);
		assert.equal(report.recoverableOrders, 1);
		assert.equal(report.recoveredOrders, 0);
		assert.equal(report.preview.length, 1);
		assert.equal(report.preview[0].shopifyOrderId, "order-backfill-dry-run-1");
		assert.equal(report.preview[0].recoverable, true);

		const attributionResult = await pool.query(
			`
        SELECT 1
        FROM attribution_results
        WHERE shopify_order_id = 'order-backfill-dry-run-1'
      `,
		);

		assert.equal(attributionResult.rowCount, 0);
	} finally {
		await resetIntegrationDatabase();
	}
});

test("production backfill recovers internal attribution and writes canonical Shopify attributes for recent recoverable orders", async () => {
	await resetIntegrationDatabase();
	const {
		pool,
		backfillRecentOrdersWithRecoveredAttribution,
		previewShopifyOrderWritebackAttributes,
	} = await getModules();

	try {
		const sessionId = await insertRecoverableOrder(pool, {
			shopifyOrderId: "order-backfill-run-1",
			checkoutToken: "checkout-backfill-run-1",
			processedAt: "2026-04-12T10:05:00.000Z",
		});
		const capturedWritebacks: Array<{
			shopifyOrderId: string;
			attributes: Array<{ key: string; value: string }>;
		}> = [];

		const report = await backfillRecentOrdersWithRecoveredAttribution({
			requestedBy: "production-run",
			workerId: "test-backfill-run",
			windowStart: new Date("2026-04-12T00:00:00.000Z"),
			windowEnd: new Date("2026-04-12T23:59:59.999Z"),
			applyWriteback: async (input) => {
				const attributes = await previewShopifyOrderWritebackAttributes(
					input.shopifyOrderId,
				);

				capturedWritebacks.push({
					shopifyOrderId: input.shopifyOrderId,
					attributes: attributes ?? [],
				});

				return {
					status: attributes && attributes.length > 0 ? "completed" : "skipped",
					attributesCount: attributes?.length ?? 0,
				};
			},
		});

		assert.equal(report.dryRun, false);
		assert.equal(report.beforeMetrics.ordersMissingAttribution, 1);
		assert.equal(report.afterMetrics.ordersMissingAttribution, 0);
		assert.equal(report.afterMetrics.ordersWithAttribution, 1);
		assert.equal(report.recoverableOrders, 1);
		assert.equal(report.recoveredOrders, 1);
		assert.equal(report.shopifyWritebackCompleted, 1);
		assert.equal(report.shopifyWritebackFailed, 0);

		const attributionResult = await pool.query<{
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
        WHERE shopify_order_id = 'order-backfill-run-1'
      `,
		);

		assert.equal(attributionResult.rowCount, 1);
		assert.deepEqual(attributionResult.rows[0], {
			session_id: sessionId,
			attributed_source: "google",
			attributed_medium: "cpc",
			attributed_campaign: "spring-sale",
			attribution_reason: "matched_by_checkout_token",
		});

		const orderSnapshot = await pool.query<{
			attribution_snapshot: Record<string, unknown> | null;
		}>(
			`
        SELECT attribution_snapshot
        FROM shopify_orders
        WHERE shopify_order_id = 'order-backfill-run-1'
      `,
		);

		assert.ok(orderSnapshot.rows[0].attribution_snapshot);

		assert.equal(capturedWritebacks.length, 1);
		assert.equal(capturedWritebacks[0].shopifyOrderId, "order-backfill-run-1");

		const attributeMap = new Map(
			capturedWritebacks[0].attributes.map((attribute) => [
				attribute.key,
				attribute.value,
			]),
		);
		assert.equal(attributeMap.get("roas_radar_session_id"), sessionId);
		assert.equal(attributeMap.get("utm_source"), "google");
		assert.equal(attributeMap.get("utm_medium"), "cpc");
		assert.equal(attributeMap.get("utm_campaign"), "spring-sale");
		assert.equal(attributeMap.get("gbraid"), "GBRAID-123");
		assert.equal(
			attributeMap.get("page_url"),
			"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123",
		);
	} finally {
		await resetIntegrationDatabase();
	}
});
