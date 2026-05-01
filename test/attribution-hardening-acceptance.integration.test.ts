import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import test from "node:test";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";
process.env.REPORTING_API_TOKEN = "test-reporting-token";
process.env.SHOPIFY_APP_API_SECRET ??= "test-app-secret";
process.env.SHOPIFY_WEBHOOK_SECRET ??= "test-webhook-secret";

type PoolModule = typeof import("../src/db/pool.js");
type ServerModule = typeof import("../src/server.js");
type AttributionModule = typeof import("../src/modules/attribution/index.js");
type ShopifyWritebackModule = typeof import("../src/modules/shopify/writeback.js");
type E2EHarnessModule = typeof import("./e2e-harness.js");

let cachedModules: {
	pool: PoolModule["pool"];
	createServer: ServerModule["createServer"];
	closeServer: ServerModule["closeServer"];
	enqueueAttributionForOrder: AttributionModule["enqueueAttributionForOrder"];
	processAttributionQueue: AttributionModule["processAttributionQueue"];
	reconcileRecentShopifyOrderAttributes:
		ShopifyWritebackModule["reconcileRecentShopifyOrderAttributes"];
	processShopifyOrderWritebackQueue:
		ShopifyWritebackModule["processShopifyOrderWritebackQueue"];
	testUtils: ShopifyWritebackModule["__shopifyWritebackTestUtils"];
	resetE2EDatabase: E2EHarnessModule["resetE2EDatabase"];
} | null = null;

async function getModules() {
	if (cachedModules) {
		return cachedModules;
	}

	const [
		poolModule,
		serverModule,
		attributionModule,
		writebackModule,
		harnessModule,
	] = await Promise.all([
		import("../src/db/pool.js"),
		import("../src/server.js"),
		import("../src/modules/attribution/index.js"),
		import("../src/modules/shopify/writeback.js"),
		import("./e2e-harness.js"),
	]);

	cachedModules = {
		pool: poolModule.pool,
		createServer: serverModule.createServer,
		closeServer: serverModule.closeServer,
		enqueueAttributionForOrder: attributionModule.enqueueAttributionForOrder,
		processAttributionQueue: attributionModule.processAttributionQueue,
		reconcileRecentShopifyOrderAttributes:
			writebackModule.reconcileRecentShopifyOrderAttributes,
		processShopifyOrderWritebackQueue:
			writebackModule.processShopifyOrderWritebackQueue,
		testUtils: writebackModule.__shopifyWritebackTestUtils,
		resetE2EDatabase: harnessModule.resetE2EDatabase,
	};

	return cachedModules;
}

async function bootstrapSession(server: { address(): AddressInfo | null }) {
	const address = server.address() as AddressInfo;
	const landingUrl =
		"https://store.example/products/widget?utm_source=Google&utm_medium=CPC&utm_campaign=Spring-Launch&utm_content=Hero&utm_term=Widget&gclid=GCLID-123&gbraid=GBRAID-123&wbraid=WBRAID-123&fbclid=FBCLID-123&ttclid=TTCLID-123&msclkid=MSCLKID-123";
	const referrerUrl = "https://www.google.com/search?q=widget";
	const response = await fetch(
		`http://127.0.0.1:${address.port}/track/session?pageUrl=${encodeURIComponent(landingUrl)}&landingUrl=${encodeURIComponent(
			landingUrl,
		)}&referrerUrl=${encodeURIComponent(referrerUrl)}`,
		{
			headers: {
				accept: "application/json",
				referer: landingUrl,
			},
		},
	);

	assert.equal(response.status, 200);
	return {
		landingUrl,
		referrerUrl,
		body: (await response.json()) as {
			sessionId: string;
			isNewSession: boolean;
		},
	};
}

test.beforeEach(async () => {
	const { resetE2EDatabase, testUtils } = await getModules();
	testUtils.reset();
	await resetE2EDatabase();
});

test.after(async () => {
	const { pool, resetE2EDatabase } = await getModules();
	await resetE2EDatabase();
	await pool.end();
});

test("hard-to-lose acceptance preserves canonical capture, session id, and Shopify recovery when the browser event never arrives", async () => {
	const {
		pool,
		createServer,
		closeServer,
		enqueueAttributionForOrder,
		processAttributionQueue,
		reconcileRecentShopifyOrderAttributes,
		processShopifyOrderWritebackQueue,
		testUtils,
	} = await getModules();

	testUtils.setWritebackProcessor(async () => undefined);
	const server = createServer();

	try {
		const bootstrap = await bootstrapSession(server);
		assert.equal(bootstrap.body.isNewSession, true);

		const trackingSession = await pool.query<{
			id: string;
			landing_page: string | null;
			referrer_url: string | null;
			initial_utm_source: string | null;
			initial_utm_medium: string | null;
			initial_utm_campaign: string | null;
			initial_utm_content: string | null;
			initial_utm_term: string | null;
			initial_gclid: string | null;
			initial_gbraid: string | null;
			initial_wbraid: string | null;
			initial_fbclid: string | null;
			initial_ttclid: string | null;
			initial_msclkid: string | null;
		}>(
			`
        SELECT
          id::text AS id,
          landing_page,
          referrer_url,
          initial_utm_source,
          initial_utm_medium,
          initial_utm_campaign,
          initial_utm_content,
          initial_utm_term,
          initial_gclid,
          initial_gbraid,
          initial_wbraid,
          initial_fbclid,
          initial_ttclid,
          initial_msclkid
        FROM tracking_sessions
        WHERE id = $1::uuid
      `,
			[bootstrap.body.sessionId],
		);

		assert.equal(trackingSession.rowCount, 1);
		assert.deepEqual(trackingSession.rows[0], {
			id: bootstrap.body.sessionId,
			landing_page: bootstrap.landingUrl,
			referrer_url: bootstrap.referrerUrl,
			initial_utm_source: "google",
			initial_utm_medium: "cpc",
			initial_utm_campaign: "spring-launch",
			initial_utm_content: "hero",
			initial_utm_term: "widget",
			initial_gclid: "GCLID-123",
			initial_gbraid: "GBRAID-123",
			initial_wbraid: "WBRAID-123",
			initial_fbclid: "FBCLID-123",
			initial_ttclid: "TTCLID-123",
			initial_msclkid: "MSCLKID-123",
		});

		const identityCapture = await pool.query<{
			roas_radar_session_id: string;
			landing_url: string | null;
			referrer_url: string | null;
			initial_utm_source: string | null;
			initial_utm_medium: string | null;
			initial_utm_campaign: string | null;
			initial_utm_content: string | null;
			initial_utm_term: string | null;
			initial_gclid: string | null;
			initial_gbraid: string | null;
			initial_wbraid: string | null;
			initial_fbclid: string | null;
			initial_ttclid: string | null;
			initial_msclkid: string | null;
		}>(
			`
        SELECT
          roas_radar_session_id::text AS roas_radar_session_id,
          landing_url,
          referrer_url,
          initial_utm_source,
          initial_utm_medium,
          initial_utm_campaign,
          initial_utm_content,
          initial_utm_term,
          initial_gclid,
          initial_gbraid,
          initial_wbraid,
          initial_fbclid,
          initial_ttclid,
          initial_msclkid
        FROM session_attribution_identities
        WHERE roas_radar_session_id = $1::uuid
      `,
			[bootstrap.body.sessionId],
		);

		assert.equal(identityCapture.rowCount, 1);
		assert.deepEqual(identityCapture.rows[0], {
			roas_radar_session_id: bootstrap.body.sessionId,
			landing_url: bootstrap.landingUrl,
			referrer_url: bootstrap.referrerUrl,
			initial_utm_source: "google",
			initial_utm_medium: "cpc",
			initial_utm_campaign: "spring-launch",
			initial_utm_content: "hero",
			initial_utm_term: "widget",
			initial_gclid: "GCLID-123",
			initial_gbraid: "GBRAID-123",
			initial_wbraid: "WBRAID-123",
			initial_fbclid: "FBCLID-123",
			initial_ttclid: "TTCLID-123",
			initial_msclkid: "MSCLKID-123",
		});

		const touchEvent = await pool.query<{
			roas_radar_session_id: string;
			page_url: string | null;
			referrer_url: string | null;
			utm_source: string | null;
			utm_medium: string | null;
			utm_campaign: string | null;
			utm_content: string | null;
			utm_term: string | null;
			gclid: string | null;
			gbraid: string | null;
			wbraid: string | null;
			fbclid: string | null;
			ttclid: string | null;
			msclkid: string | null;
			ingestion_source: string;
		}>(
			`
        SELECT
          roas_radar_session_id::text AS roas_radar_session_id,
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
          fbclid,
          ttclid,
          msclkid,
          ingestion_source
        FROM session_attribution_touch_events
        WHERE roas_radar_session_id = $1::uuid
      `,
			[bootstrap.body.sessionId],
		);

		assert.equal(touchEvent.rowCount, 1);
		assert.deepEqual(touchEvent.rows[0], {
			roas_radar_session_id: bootstrap.body.sessionId,
			page_url: bootstrap.landingUrl,
			referrer_url: bootstrap.referrerUrl,
			utm_source: "google",
			utm_medium: "cpc",
			utm_campaign: "spring-launch",
			utm_content: "hero",
			utm_term: "widget",
			gclid: "GCLID-123",
			gbraid: "GBRAID-123",
			wbraid: "WBRAID-123",
			fbclid: "FBCLID-123",
			ttclid: "TTCLID-123",
			msclkid: "MSCLKID-123",
			ingestion_source: "request_query",
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
          source_name,
          raw_payload,
          payload_size_bytes,
          ingested_at
        )
        VALUES (
          'hardening-order-1',
          'USD',
          '120.00',
          '120.00',
          '2026-04-23T12:15:00.000Z',
          $1::uuid,
          'web',
          $2::jsonb,
          octet_length(convert_to($2::text, 'utf8')),
          now()
        )
      `,
			[
				bootstrap.body.sessionId,
				JSON.stringify({
					id: "hardening-order-1",
					landing_session_id: bootstrap.body.sessionId,
					note_attributes: [],
				}),
			],
		);

		await enqueueAttributionForOrder(
			"hardening-order-1",
			"acceptance_missing_browser_event",
		);
		const attributionReport = await processAttributionQueue({
			workerId: "test-hardening-attribution",
			limit: 10,
			staleScanLimit: 0,
			emitMetrics: false,
		});

		assert.equal(attributionReport.succeededJobs, 1);
		assert.equal(attributionReport.failedJobs, 0);

		const attributionResult = await pool.query<{
			session_id: string | null;
			attributed_source: string | null;
			attributed_medium: string | null;
			attributed_campaign: string | null;
			attributed_content: string | null;
			attributed_term: string | null;
			attributed_click_id_type: string | null;
			attributed_click_id_value: string | null;
			attribution_reason: string;
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
          attribution_reason
        FROM attribution_results
        WHERE shopify_order_id = 'hardening-order-1'
      `,
		);

		assert.equal(attributionResult.rowCount, 1);
		assert.deepEqual(attributionResult.rows[0], {
			session_id: bootstrap.body.sessionId,
			attributed_source: "google",
			attributed_medium: "cpc",
			attributed_campaign: "spring-launch",
			attributed_content: "hero",
			attributed_term: "widget",
			attributed_click_id_type: "gclid",
			attributed_click_id_value: "GCLID-123",
			attribution_reason: "matched_by_landing_session",
		});

		const reconciliationReport = await reconcileRecentShopifyOrderAttributes({
			workerId: "test-hardening-reconciliation",
			limit: 10,
			lookbackDays: 30,
			now: new Date("2026-04-24T00:00:00.000Z"),
		});

		assert.equal(reconciliationReport.scannedOrders, 1);
		assert.equal(reconciliationReport.ordersNeedingWriteback, 1);
		assert.equal(reconciliationReport.requeuedOrders, 1);
		assert.equal(reconciliationReport.failedOrders, 0);

		const writebackReport = await processShopifyOrderWritebackQueue({
			workerId: "test-hardening-writeback",
			limit: 10,
			now: new Date("2100-04-23T00:00:00.000Z"),
		});

		assert.equal(writebackReport.claimedJobs, 1);
		assert.equal(writebackReport.completedJobs, 1);
		assert.equal(writebackReport.deadLetteredJobs, 0);

		const appliedWritebacks = testUtils.getAppliedWritebacks();
		assert.equal(appliedWritebacks.length, 1);
		assert.equal(appliedWritebacks[0].shopifyOrderId, "hardening-order-1");

		const attributeMap = new Map(
			appliedWritebacks[0].attributes.map((attribute) => [
				attribute.key,
				attribute.value,
			]),
		);
		assert.deepEqual(Object.fromEntries(attributeMap), {
			schema_version: "1",
			roas_radar_session_id: bootstrap.body.sessionId,
			landing_url: bootstrap.landingUrl,
			referrer_url: bootstrap.referrerUrl,
			page_url: bootstrap.landingUrl,
			utm_source: "google",
			utm_medium: "cpc",
			utm_campaign: "spring-launch",
			utm_content: "hero",
			utm_term: "widget",
			gclid: "GCLID-123",
			gbraid: "GBRAID-123",
			wbraid: "WBRAID-123",
			fbclid: "FBCLID-123",
			ttclid: "TTCLID-123",
			msclkid: "MSCLKID-123",
		});
	} finally {
		await closeServer(server);
	}
});
