import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import test from "node:test";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@localhost:5432/roas_radar_test";
process.env.REPORTING_API_TOKEN = "test-reporting-token";

const poolModule = await import("../src/db/pool.js");
const serverModule = await import("../src/server.js");

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const originalPoolQuery = pool.query.bind(pool);

async function requestJson(
	server: ReturnType<typeof createServer>,
	path: string,
	headers?: Record<string, string>,
) {
	const address = server.address() as AddressInfo;
	const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
		headers,
	});
	const body = await response.json();

	return { response, body };
}

test("GA4 fallback shadow report route validates the requested date range", async () => {
	let queryCalls = 0;
	pool.query = (async () => {
		queryCalls += 1;
		return { rows: [] };
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/attribution/ga4-fallback/shadow-report?startDate=2026-04-10&endDate=2026-04-01",
			{
				authorization: "Bearer test-reporting-token",
			},
		);

		assert.equal(response.status, 400);
		assert.equal(body.error, "invalid_request");
		assert.equal(body.message, "Invalid GA4 fallback shadow report request");
		assert.equal(queryCalls, 0);
		assert.deepEqual(body.details.fieldErrors.startDate, [
			"startDate must be on or before endDate",
		]);
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});

test("GA4 fallback shadow report route returns fallback volume, key deltas, and explicit approval gating", async () => {
	process.env.GA4_FALLBACK_ROLLOUT_MODE = "shadow";
	process.env.GA4_FALLBACK_SHADOW_MIN_EVALUATED_ORDERS = "100";
	process.env.GA4_FALLBACK_SHADOW_MAX_ATTRIBUTED_ORDER_DELTA_RATE = "0.05";
	process.env.GA4_FALLBACK_SHADOW_MAX_ATTRIBUTED_REVENUE_DELTA_RATE = "0.05";

	pool.query = (async (text: string, params?: unknown[]) => {
		assert.match(text, /FROM ga4_fallback_shadow_comparisons/);
		assert.deepEqual(params, ["2026-04-01", "2026-04-10"]);

		return {
			rows: [
				{
					evaluated_orders: "125",
					shadow_ga4_fallback_orders: "9",
					changed_orders: "9",
					current_attributed_orders: "100",
					shadow_attributed_orders: "104",
					current_attributed_revenue: "10000.00",
					shadow_attributed_revenue: "10300.00",
				},
			],
		};
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/attribution/ga4-fallback/shadow-report?startDate=2026-04-01&endDate=2026-04-10",
			{
				authorization: "Bearer test-reporting-token",
			},
		);

		assert.equal(response.status, 200);
		assert.deepEqual(body, {
			range: {
				startDate: "2026-04-01",
				endDate: "2026-04-10",
			},
			rolloutMode: "shadow",
			summary: {
				evaluatedOrders: 125,
				shadowGa4FallbackOrders: 9,
				changedOrders: 9,
				currentAttributedOrders: 100,
				shadowAttributedOrders: 104,
				attributedOrderDelta: 4,
				currentAttributedRevenue: 10000,
				shadowAttributedRevenue: 10300,
				attributedRevenueDelta: 300,
			},
			productionEnablement: {
				requiresExplicitApproval: true,
				meetsAcceptanceThresholds: true,
				approvalStatus: "pending_explicit_approval",
				thresholds: {
					minEvaluatedOrders: 100,
					maxAttributedOrderDeltaRate: 0.05,
					maxAttributedRevenueDeltaRate: 0.05,
				},
			},
		});
	} finally {
		process.env.GA4_FALLBACK_ROLLOUT_MODE = undefined;
		process.env.GA4_FALLBACK_SHADOW_MIN_EVALUATED_ORDERS = undefined;
		process.env.GA4_FALLBACK_SHADOW_MAX_ATTRIBUTED_ORDER_DELTA_RATE = undefined;
		process.env.GA4_FALLBACK_SHADOW_MAX_ATTRIBUTED_REVENUE_DELTA_RATE =
			undefined;
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});
