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

test("identity admin health route rejects unauthorized requests", async () => {
	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/identity/health?startDate=2026-04-01&endDate=2026-04-05",
		);

		assert.equal(response.status, 401);
		assert.deepEqual(body, {
			error: "unauthorized",
			message: "Authentication required",
		});
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});

test("identity admin health route returns merge KPIs, daily series, and latest backfill status", async () => {
	const capturedQueries: Array<{ text: string; params?: unknown[] }> = [];

	pool.query = (async (text: string, params?: unknown[]) => {
		capturedQueries.push({ text, params });

		if (/COUNT\(\*\)::bigint AS total_ingestions/.test(text)) {
			assert.deepEqual(params, [
				"2026-04-01",
				"2026-04-05",
				"shopify_order_webhook",
			]);
			return {
				rows: [
					{
						total_ingestions: 14,
						linked_ingestions: 10,
						skipped_ingestions: 2,
						conflict_ingestions: 2,
						merge_runs: 3,
						rehomed_nodes: 7,
						quarantined_nodes: 2,
						unresolved_conflicts: 1,
					},
				],
			};
		}

		if (
			/to_char\(date_trunc\('day', runs.source_timestamp AT TIME ZONE 'UTC'\), 'YYYY-MM-DD'\)/.test(
				text,
			)
		) {
			return {
				rows: [
					{
						bucket_date: "2026-04-01",
						linked_count: 4,
						skipped_count: 1,
						conflict_count: 0,
						merge_runs: 1,
						rehomed_nodes: 2,
						quarantined_nodes: 0,
					},
					{
						bucket_date: "2026-04-02",
						linked_count: 6,
						skipped_count: 1,
						conflict_count: 2,
						merge_runs: 2,
						rehomed_nodes: 5,
						quarantined_nodes: 2,
					},
				],
			};
		}

		if (
			/COUNT\(\*\) FILTER \(WHERE sessions.identity_journey_id IS NULL\)::bigint AS unlinked_sessions/.test(
				text,
			)
		) {
			return {
				rows: [
					{
						unlinked_sessions: 5,
						linked_sessions: 18,
					},
				],
			};
		}

		if (
			/COUNT\(\*\) FILTER \(WHERE runs.status = 'processing'\)::bigint AS active_runs/.test(
				text,
			)
		) {
			return {
				rows: [
					{
						active_runs: 1,
						failed_runs: 0,
						completed_runs: 4,
					},
				],
			};
		}

		if (
			/FROM identity_graph_backfill_runs runs/.test(text) &&
			/LIMIT 1/.test(text)
		) {
			return {
				rows: [
					{
						id: "11111111-1111-4111-8111-111111111111",
						status: "processing",
						requested_by: "ops@roasradar.dev",
						worker_id: "identity-worker-1",
						options: {
							sources: ["shopify_orders", "tracking_events"],
						},
						error_code: null,
						error_message: null,
						started_at: new Date("2026-04-05T10:00:00.000Z"),
						completed_at: null,
						updated_at: new Date("2026-04-05T10:05:00.000Z"),
					},
				],
			};
		}

		throw new Error(`Unexpected query: ${text}`);
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/identity/health?startDate=2026-04-01&endDate=2026-04-05&source=shopify_order_webhook",
			{
				authorization: "Bearer test-reporting-token",
			},
		);

		assert.equal(response.status, 200);
		assert.equal(body.summary.mergeRuns, 3);
		assert.equal(body.summary.rehomedNodes, 7);
		assert.equal(body.summary.conflictIngestions, 2);
		assert.equal(body.summary.unlinkedSessions, 5);
		assert.deepEqual(body.series, [
			{
				date: "2026-04-01",
				linked: 4,
				skipped: 1,
				conflicts: 0,
				mergeRuns: 1,
				rehomedNodes: 2,
				quarantinedNodes: 0,
			},
			{
				date: "2026-04-02",
				linked: 6,
				skipped: 1,
				conflicts: 2,
				mergeRuns: 2,
				rehomedNodes: 5,
				quarantinedNodes: 2,
			},
		]);
		assert.deepEqual(body.backfill, {
			activeRuns: 1,
			failedRuns: 0,
			completedRuns: 4,
			latestRun: {
				runId: "11111111-1111-4111-8111-111111111111",
				status: "processing",
				requestedBy: "ops@roasradar.dev",
				workerId: "identity-worker-1",
				sources: ["shopify_orders", "tracking_events"],
				startedAt: "2026-04-05T10:00:00.000Z",
				completedAt: null,
				updatedAt: "2026-04-05T10:05:00.000Z",
				errorCode: null,
				errorMessage: null,
			},
		});
		assert.equal(capturedQueries.length, 5);
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});

test("identity admin conflict route returns recent conflict drill-down rows", async () => {
	pool.query = (async (text: string, params?: unknown[]) => {
		assert.match(text, /FROM identity_edges edge/);
		assert.deepEqual(params, ["2026-04-01", "2026-04-05", 2]);

		return {
			rows: [
				{
					edge_id: "edge-1",
					journey_id: "journey-1",
					journey_status: "quarantined",
					authoritative_shopify_customer_id: "sc-1",
					node_type: "phone_hash",
					node_key: "c".repeat(64),
					evidence_source: "shopify_order_webhook",
					source_table: "shopify_orders",
					source_record_id: "order-1001",
					conflict_code: "phone_hash_conflicts_across_authoritative_customers",
					first_observed_at: new Date("2026-04-02T00:00:00.000Z"),
					last_observed_at: new Date("2026-04-04T00:00:00.000Z"),
					updated_at: new Date("2026-04-04T01:00:00.000Z"),
				},
			],
		};
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/identity/health/conflicts?startDate=2026-04-01&endDate=2026-04-05&limit=2",
			{
				authorization: "Bearer test-reporting-token",
			},
		);

		assert.equal(response.status, 200);
		assert.deepEqual(body.conflicts, [
			{
				edgeId: "edge-1",
				journeyId: "journey-1",
				journeyStatus: "quarantined",
				authoritativeShopifyCustomerId: "sc-1",
				nodeType: "phone_hash",
				nodeKey: "c".repeat(64),
				evidenceSource: "shopify_order_webhook",
				sourceTable: "shopify_orders",
				sourceRecordId: "order-1001",
				conflictCode: "phone_hash_conflicts_across_authoritative_customers",
				firstObservedAt: "2026-04-02T00:00:00.000Z",
				lastObservedAt: "2026-04-04T00:00:00.000Z",
				updatedAt: "2026-04-04T01:00:00.000Z",
			},
		]);
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});
