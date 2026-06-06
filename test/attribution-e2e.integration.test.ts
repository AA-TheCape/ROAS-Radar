import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import test from "node:test";

import {
	DEFAULT_REPORTING_TIMEZONE,
	formatDateInTimezone,
} from "../src/modules/settings/index.js";
import { buildRawPayloadFixture } from "./integration-test-helpers.js";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";
process.env.REPORTING_API_TOKEN = "test-reporting-token";
process.env.SHOPIFY_APP_API_SECRET ??= "test-app-secret";
process.env.SHOPIFY_WEBHOOK_SECRET ??= "test-webhook-secret";

let cachedModules: {
	pool: typeof import("../src/db/pool.js").pool;
	createServer: typeof import("../src/server.js").createServer;
	closeServer: typeof import("../src/server.js").closeServer;
	enqueueAttributionForOrder: typeof import("../src/modules/attribution/index.js").enqueueAttributionForOrder;
	processAttributionQueue: typeof import("../src/modules/attribution/index.js").processAttributionQueue;
	upsertGa4FallbackCandidates: typeof import("../src/modules/attribution/ga4-fallback-candidates.js").upsertGa4FallbackCandidates;
	enqueueShopifyOrderWriteback: typeof import("../src/modules/shopify/writeback.js").enqueueShopifyOrderWriteback;
	processShopifyOrderWritebackQueue: typeof import("../src/modules/shopify/writeback.js").processShopifyOrderWritebackQueue;
	testUtils: typeof import("../src/modules/shopify/writeback.js").__shopifyWritebackTestUtils;
	resetE2EDatabase: typeof import("./e2e-harness.js").resetE2EDatabase;
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

test("GA4 Google CPC fallback preserves campaign and account metadata without campaign name", async () => {
	const {
		pool,
		enqueueAttributionForOrder,
		processAttributionQueue,
		upsertGa4FallbackCandidates,
	} = await getModules();

	const customerIdentityId = "11111111-1111-4111-8111-111111111111";
	const orderProcessedAt = new Date("2026-04-27T12:00:00.000Z");
	const orderFixture = buildRawPayloadFixture(
		{
			id: "e2e-ga4-google-campaign-metadata",
		},
		"e2e-ga4-google-campaign-metadata",
	);

	await pool.query(
		`
        INSERT INTO customer_identities (
          id,
          hashed_email,
          created_at,
          updated_at,
          last_stitched_at
        )
        VALUES (
          $1::uuid,
          $2,
          now(),
          now(),
          now()
        )
      `,
		[
			customerIdentityId,
			"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		],
	);

	await pool.query(
		`
        INSERT INTO shopify_orders (
          shopify_order_id,
          currency_code,
          subtotal_price,
          total_price,
          processed_at,
          customer_identity_id,
          source_name,
          payload_external_id,
          payload_size_bytes,
          payload_hash,
          raw_payload,
          ingested_at
        )
        VALUES (
          'e2e-ga4-google-campaign-metadata',
          'USD',
          '88.00',
          '88.00',
          $1,
          $2::uuid,
          'web',
          $3,
          $4,
          $5,
          $6::jsonb,
          now()
        )
      `,
		[
			orderProcessedAt.toISOString(),
			customerIdentityId,
			orderFixture.payloadExternalId,
			orderFixture.payloadSizeBytes,
			orderFixture.payloadHash,
			orderFixture.rawPayloadJson,
		],
	);

	await upsertGa4FallbackCandidates([
		{
			occurredAt: "2026-04-26T11:00:00.000Z",
			ga4UserKey: "ga4-user-google-campaign-metadata",
			ga4ClientId: "client-google-campaign-metadata",
			ga4SessionId: "session-google-campaign-metadata",
			transactionId: null,
			emailHash: null,
			customerIdentityId,
			source: "google",
			medium: "cpc",
			campaignId: "987654321",
			campaign: null,
			content: null,
			term: null,
			clickIdType: null,
			clickIdValue: null,
			accountId: "1234567890",
			accountName: "Cape Google Ads",
			channelType: "SEARCH",
			channelSubtype: "SEARCH_STANDARD",
			campaignMetadataSource: "google_ads_transfer",
			accountMetadataSource: "google_ads_transfer",
			channelMetadataSource: "google_ads_transfer",
			sessionHasRequiredFields: true,
			sourceExportHour: "2026-04-26T12:00:00.000Z",
			sourceDataset: "ga4_export",
			sourceTableType: "events",
		},
	]);

	await enqueueAttributionForOrder(
		"e2e-ga4-google-campaign-metadata",
		"test_ga4_google_campaign_metadata",
	);
	const attributionReport = await processAttributionQueue({
		workerId: "test-e2e-ga4-google-campaign-metadata",
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
		attributed_campaign_id: string | null;
		attributed_account_id: string | null;
		attributed_account_name: string | null;
		attributed_channel_type: string | null;
		attributed_channel_subtype: string | null;
		attributed_campaign_metadata_source: string | null;
		attributed_account_metadata_source: string | null;
		attributed_channel_metadata_source: string | null;
		attribution_reason: string;
		match_source: string;
	}>(
		`
        SELECT
          attributed_source,
          attributed_medium,
          attributed_campaign,
          attributed_campaign_id,
          attributed_account_id,
          attributed_account_name,
          attributed_channel_type,
          attributed_channel_subtype,
          attributed_campaign_metadata_source,
          attributed_account_metadata_source,
          attributed_channel_metadata_source,
          attribution_reason,
          match_source
        FROM attribution_results
        WHERE shopify_order_id = 'e2e-ga4-google-campaign-metadata'
      `,
	);

	assert.equal(attributionResult.rowCount, 1);
	assert.deepEqual(attributionResult.rows[0], {
		attributed_source: "google",
		attributed_medium: "cpc",
		attributed_campaign: "987654321",
		attributed_campaign_id: "987654321",
		attributed_account_id: "1234567890",
		attributed_account_name: "Cape Google Ads",
		attributed_channel_type: "SEARCH",
		attributed_channel_subtype: "SEARCH_STANDARD",
		attributed_campaign_metadata_source: "google_ads_transfer",
		attributed_account_metadata_source: "google_ads_transfer",
		attributed_channel_metadata_source: "google_ads_transfer",
		attribution_reason: "ga4_fallback_derived",
		match_source: "ga4_fallback",
	});

	const creditResult = await pool.query<{
		attributed_campaign_id: string | null;
		attributed_account_id: string | null;
		attributed_account_name: string | null;
	}>(
		`
        SELECT
          attributed_campaign_id,
          attributed_account_id,
          attributed_account_name
        FROM attribution_order_credits
        WHERE shopify_order_id = 'e2e-ga4-google-campaign-metadata'
          AND attribution_model = 'hinted_fallback_only'
          AND is_primary = true
      `,
	);

	assert.equal(creditResult.rowCount, 1);
	assert.deepEqual(creditResult.rows[0], {
		attributed_campaign_id: "987654321",
		attributed_account_id: "1234567890",
		attributed_account_name: "Cape Google Ads",
	});
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
		const orderProcessedAt = new Date(Date.now() - 1_000);
		const touchOccurredAt = new Date(
			orderProcessedAt.getTime() - 86_400_000,
		).toISOString();
		const reportingDate = formatDateInTimezone(
			orderProcessedAt,
			DEFAULT_REPORTING_TIMEZONE,
		);
		const orderFixture = buildRawPayloadFixture(
			{
				id: "e2e-order-1",
				landing_session_id: bootstrap.sessionId,
			},
			"e2e-order-1",
		);

		await pool.query(
			`
        UPDATE tracking_sessions
        SET first_seen_at = $2::timestamptz,
            last_seen_at = $2::timestamptz
        WHERE id = $1::uuid
      `,
			[bootstrap.sessionId, touchOccurredAt],
		);
		await pool.query(
			`
        UPDATE tracking_events
        SET occurred_at = $2::timestamptz
        WHERE session_id = $1::uuid
      `,
			[bootstrap.sessionId, touchOccurredAt],
		);
		await pool.query(
			`
        UPDATE session_attribution_touch_events
        SET occurred_at = $2::timestamptz
        WHERE roas_radar_session_id = $1::uuid
      `,
			[bootstrap.sessionId, touchOccurredAt],
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
          source_name,
          payload_external_id,
          payload_size_bytes,
          payload_hash,
          raw_payload,
          ingested_at
        )
        VALUES (
          'e2e-order-1',
          'USD',
          '120.00',
          '120.00',
          $6,
          $1::uuid,
          'web',
          $2,
          $3,
          $4,
          $5::jsonb,
          now()
        )
      `,
			[
				bootstrap.sessionId,
				orderFixture.payloadExternalId,
				orderFixture.payloadSizeBytes,
				orderFixture.payloadHash,
				orderFixture.rawPayloadJson,
				orderProcessedAt.toISOString(),
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
			`/api/reporting/summary?startDate=${reportingDate}&endDate=${reportingDate}&source=google&campaign=spring-sale`,
		);

		assert.equal(reportingSummary.response.status, 200);
		assert.equal(reportingSummary.body.reportingMode, "clicks");
		assert.equal(reportingSummary.body.totalsCanonical, true);
		assert.deepEqual(reportingSummary.body.range, {
			startDate: reportingDate,
			endDate: reportingDate,
		});
		assert.deepEqual(reportingSummary.body.totals, {
			visits: 0,
			orders: 1,
			revenue: 120,
			spend: 0,
			conversionRate: 0,
			roas: null,
		});
		assert.deepEqual(
			(reportingSummary.body.layers as Record<string, { totals: unknown }>)
				.clicks.totals,
			reportingSummary.body.totals,
		);
		assert.deepEqual(
			(reportingSummary.body.layers as Record<string, { totals: unknown }>)
				.deterministicViews.totals,
			{
				visits: 0,
				orders: 0,
				revenue: 0,
				spend: 0,
				conversionRate: 0,
				roas: null,
			},
		);
		assert.deepEqual(
			(
				reportingSummary.body.comparisonTotals as Record<
					string,
					{ totals: unknown }
				>
			).combined.totals,
			{
				visits: 0,
				orders: 1,
				revenue: 120,
				spend: 0,
				conversionRate: 0,
				roas: null,
			},
		);
	} finally {
		await closeServer(server);
	}
});
