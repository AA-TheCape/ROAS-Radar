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
type Ga4FallbackCandidatesModule = typeof import(
	"../src/modules/attribution/ga4-fallback-candidates.js"
);
type ShopifyWritebackModule = typeof import("../src/modules/shopify/writeback.js");
type E2EHarnessModule = typeof import("./e2e-harness.js");

let cachedModules: {
	pool: PoolModule["pool"];
	createServer: ServerModule["createServer"];
	closeServer: ServerModule["closeServer"];
	enqueueAttributionForOrder: AttributionModule["enqueueAttributionForOrder"];
	processAttributionQueue: AttributionModule["processAttributionQueue"];
	upsertGa4FallbackCandidates:
		Ga4FallbackCandidatesModule["upsertGa4FallbackCandidates"];
	enqueueShopifyOrderWriteback:
		ShopifyWritebackModule["enqueueShopifyOrderWriteback"];
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
		ga4FallbackModule,
		writebackModule,
		harnessModule,
	] = await Promise.all([
		import("../src/db/pool.js"),
		import("../src/server.js"),
		import("../src/modules/attribution/index.js"),
		import("../src/modules/attribution/ga4-fallback-candidates.js"),
		import("../src/modules/shopify/writeback.js"),
		import("./e2e-harness.js"),
	]);

	cachedModules = {
		pool: poolModule.pool,
		createServer: serverModule.createServer,
		closeServer: serverModule.closeServer,
		enqueueAttributionForOrder: attributionModule.enqueueAttributionForOrder,
		processAttributionQueue: attributionModule.processAttributionQueue,
		upsertGa4FallbackCandidates: ga4FallbackModule.upsertGa4FallbackCandidates,
		enqueueShopifyOrderWriteback: writebackModule.enqueueShopifyOrderWriteback,
		processShopifyOrderWritebackQueue:
			writebackModule.processShopifyOrderWritebackQueue,
		testUtils: writebackModule.__shopifyWritebackTestUtils,
		resetE2EDatabase: harnessModule.resetE2EDatabase,
	};

	return cachedModules;
}

function buildReportingHeaders(): Record<string, string> {
	return {
		authorization: "Bearer test-reporting-token",
		accept: "application/json",
	};
}

async function bootstrapSession(server: { address(): AddressInfo | null }) {
	const address = server.address() as AddressInfo;
	const response = await fetch(
		`http://127.0.0.1:${address.port}/track/session?pageUrl=${encodeURIComponent(
			"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123",
		)}&landingUrl=${encodeURIComponent(
			"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123",
		)}&referrerUrl=${encodeURIComponent("https://www.google.com/search?q=widget")}`,
		{
			headers: {
				accept: "application/json",
				referer:
					"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale",
			},
		},
	);

	assert.equal(response.status, 200);
	return (await response.json()) as {
		sessionId: string;
		isNewSession: boolean;
	};
}

async function requestJson(
	server: { address(): AddressInfo | null },
	path: string,
) {
	const address = server.address() as AddressInfo;
	const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
		headers: buildReportingHeaders(),
	});

	return {
		response,
		body: (await response.json()) as Record<string, unknown>,
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

test("paid capture survives attribution, Shopify writeback, and reporting end to end", async () => {
	const {
		pool,
		createServer,
		closeServer,
		enqueueAttributionForOrder,
		processAttributionQueue,
		enqueueShopifyOrderWriteback,
		processShopifyOrderWritebackQueue,
		testUtils,
	} = await getModules();

	testUtils.setWritebackProcessor(async () => undefined);
	const server = createServer();

	try {
		const bootstrap = await bootstrapSession(server);
		assert.equal(bootstrap.isNewSession, true);

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
          'e2e-order-1',
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
				bootstrap.sessionId,
				JSON.stringify({
					id: "e2e-order-1",
					landing_session_id: bootstrap.sessionId,
				}),
			],
		);

		await enqueueAttributionForOrder("e2e-order-1", "test_e2e");
		const attributionReport = await processAttributionQueue({
			workerId: "test-e2e-attribution",
			limit: 10,
			staleScanLimit: 0,
			emitMetrics: false,
		});

		assert.equal(attributionReport.succeededJobs, 1);
		assert.equal(attributionReport.failedJobs, 0);

		const attributionResult = await pool.query<{
			attributed_source: string | null;
			attributed_medium: string | null;
			attributed_campaign: string | null;
			attributed_click_id_type: string | null;
			attributed_click_id_value: string | null;
			attribution_reason: string;
		}>(
			`
        SELECT
          attributed_source,
          attributed_medium,
          attributed_campaign,
          attributed_click_id_type,
          attributed_click_id_value,
          attribution_reason
        FROM attribution_results
        WHERE shopify_order_id = 'e2e-order-1'
      `,
		);

		assert.equal(attributionResult.rowCount, 1);
		assert.deepEqual(attributionResult.rows[0], {
			attributed_source: "google",
			attributed_medium: "cpc",
			attributed_campaign: "spring-sale",
			attributed_click_id_type: "gbraid",
			attributed_click_id_value: "GBRAID-123",
			attribution_reason: "matched_by_landing_session",
		});

		await enqueueShopifyOrderWriteback("e2e-order-1", "test_e2e");
		const writebackReport = await processShopifyOrderWritebackQueue({
			workerId: "test-e2e-writeback",
			limit: 10,
			now: new Date("2100-04-23T00:00:00.000Z"),
		});

		assert.equal(writebackReport.completedJobs, 1);
		assert.equal(writebackReport.deadLetteredJobs, 0);

		const appliedWritebacks = testUtils.getAppliedWritebacks();
		assert.equal(appliedWritebacks.length, 1);
		assert.equal(appliedWritebacks[0].shopifyOrderId, "e2e-order-1");
		assert.deepEqual(
			appliedWritebacks[0].attributes.filter((attribute) =>
				[
					"schema_version",
					"roas_radar_session_id",
					"utm_source",
					"utm_medium",
					"utm_campaign",
					"gbraid",
				].includes(attribute.key),
			),
			[
				{ key: "schema_version", value: "1" },
				{ key: "roas_radar_session_id", value: bootstrap.sessionId },
				{ key: "utm_source", value: "google" },
				{ key: "utm_medium", value: "cpc" },
				{ key: "utm_campaign", value: "spring-sale" },
				{ key: "gbraid", value: "GBRAID-123" },
			],
		);

		const reportingSummary = await requestJson(
			server,
			"/api/reporting/summary?startDate=2026-04-23&endDate=2026-04-23&source=google&campaign=spring-sale",
		);

		assert.equal(reportingSummary.response.status, 200);
		assert.deepEqual(reportingSummary.body, {
			range: {
				startDate: "2026-04-23",
				endDate: "2026-04-23",
			},
			totals: {
				visits: 0,
				orders: 1,
				revenue: 120,
				spend: 0,
				conversionRate: 0,
				roas: null,
			},
		});
	} finally {
		await closeServer(server);
	}
});

test("GA4-only fallback flows through attribution and reporting with the fallback label and campaign intact", async () => {
	const {
		pool,
		createServer,
		closeServer,
		enqueueAttributionForOrder,
		processAttributionQueue,
		upsertGa4FallbackCandidates,
	} = await getModules();

	const server = createServer();

	try {
		await upsertGa4FallbackCandidates([
			{
				occurredAt: "2026-04-24T12:00:00.000Z",
				ga4UserKey: "ga4-user-e2e",
				ga4ClientId: "ga4-client-e2e",
				ga4SessionId: "ga4-session-e2e",
				transactionId: "e2e-order-ga4-1",
				emailHash: null,
				customerIdentityId: null,
				source: "google",
				medium: "cpc",
				campaign: "ga4-e2e-campaign",
				content: "hero",
				term: "boots",
				clickIdType: null,
				clickIdValue: null,
				sessionHasRequiredFields: true,
				sourceExportHour: "2026-04-24T12:00:00.000Z",
				sourceDataset: "ga4_export",
				sourceTableType: "events",
			},
		]);

		await pool.query(
			`
        INSERT INTO shopify_orders (
          shopify_order_id,
          currency_code,
          subtotal_price,
          total_price,
          processed_at,
          source_name,
          raw_payload,
          payload_size_bytes,
          ingested_at
        )
        VALUES (
          'e2e-order-ga4-1',
          'USD',
          '95.00',
          '95.00',
          '2026-04-24T12:15:00.000Z',
          'web',
          $1::jsonb,
          octet_length(convert_to($1::text, 'utf8')),
          now()
        )
      `,
			[
				JSON.stringify({
					id: "e2e-order-ga4-1",
					source_name: "web",
					landing_site: "https://store.example/products/widget",
				}),
			],
		);

		await enqueueAttributionForOrder("e2e-order-ga4-1", "test_e2e_ga4_only");
		const attributionReport = await processAttributionQueue({
			workerId: "test-e2e-ga4-only",
			limit: 10,
			staleScanLimit: 0,
			emitMetrics: false,
		});

		assert.equal(attributionReport.succeededJobs, 1);
		assert.equal(attributionReport.failedJobs, 0);

		const attributionResult = await pool.query<{
			match_source: string;
			confidence_label: string;
			attributed_source: string | null;
			attributed_medium: string | null;
			attributed_campaign: string | null;
			attribution_reason: string;
		}>(
			`
        SELECT
          match_source,
          confidence_label,
          attributed_source,
          attributed_medium,
          attributed_campaign,
          attribution_reason
        FROM attribution_results
        WHERE shopify_order_id = 'e2e-order-ga4-1'
      `,
		);

		assert.deepEqual(attributionResult.rows[0], {
			match_source: "ga4_fallback",
			confidence_label: "low",
			attributed_source: "google",
			attributed_medium: "cpc",
			attributed_campaign: "ga4-e2e-campaign",
			attribution_reason: "ga4_fallback_derived",
		});

		const reportingOrders = await requestJson(
			server,
			"/api/reporting/orders?startDate=2026-04-24&endDate=2026-04-24&source=google&campaign=ga4-e2e-campaign&limit=5",
		);

		assert.equal(reportingOrders.response.status, 200);
		assert.deepEqual(reportingOrders.body, {
			rows: [
				{
					shopifyOrderId: "e2e-order-ga4-1",
					processedAt: "2026-04-24T12:15:00.000Z",
					totalPrice: 95,
					source: "google",
					medium: "cpc",
					campaign: "ga4-e2e-campaign",
					attributionReason: "ga4_fallback_derived",
				},
			],
		});
	} finally {
		await closeServer(server);
	}
});
