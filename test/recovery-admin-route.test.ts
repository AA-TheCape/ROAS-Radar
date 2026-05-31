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
const originalPoolConnect = pool.connect.bind(pool);

async function requestJson(
	server: ReturnType<typeof createServer>,
	path: string,
	input: {
		method?: string;
		headers?: Record<string, string>;
		body?: unknown;
	} = {},
) {
	const address = server.address() as AddressInfo;
	const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
		method: input.method ?? "GET",
		headers: {
			"content-type": "application/json",
			...(input.headers ?? {}),
		},
		body: input.body === undefined ? undefined : JSON.stringify(input.body),
	});
	const body = await response.json();

	return { response, body };
}

function buildRecoveryRunRow(
	overrides: Record<string, unknown> = {},
): Record<string, unknown> {
	return {
		id: "22222222-2222-4222-8222-222222222222",
		job_type: "shopify_attribution_hint_recovery",
		status: "queued",
		mode: "manual",
		initiated_by: "internal",
		dry_run: true,
		time_range_start: new Date("2026-04-01T00:00:00.000Z"),
		time_range_end: new Date("2026-04-05T23:59:59.999Z"),
		idempotency_key: "recovery:shopify_attribution_hint_recovery:test",
		concurrency_key: "range:test",
		scope_key: "shopify-attribution-hints",
		resume_from_run_id: null,
		rerun_of_run_id: null,
		input_parameters: { chunkSize: 125, pageSize: 125 },
		checkpoint: {},
		records_discovered: 0,
		records_claimed: 0,
		records_processed: 0,
		records_succeeded: 0,
		records_failed: 0,
		records_skipped: 0,
		records_retried: 0,
		side_effects_attempted: 0,
		side_effects_succeeded: 0,
		side_effects_suppressed: 0,
		priority: 100,
		available_at: new Date("2026-04-01T00:00:00.000Z"),
		attempt_count: 0,
		max_attempts: 3,
		heartbeat_timeout_seconds: 300,
		claimed_by: null,
		queued_at: new Date("2026-04-01T00:00:00.000Z"),
		started_at: null,
		completed_at: null,
		last_heartbeat_at: new Date("2026-04-01T00:00:00.000Z"),
		lock_expires_at: null,
		dead_lettered_at: null,
		error_code: null,
		error_message: null,
		last_error_details: {},
		completion_report: {},
		...overrides,
	};
}

test("manual recovery admin routes reject unauthorized operators", async () => {
	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/recovery/runs",
			{
				method: "POST",
				body: {
					jobType: "shopify_attribution_hint_recovery",
					startDate: "2026-04-01",
					endDate: "2026-04-05",
				},
			},
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

test("manual recovery admin routes reject authenticated non-admin users", async () => {
	let queryCalls = 0;
	pool.query = (async () => {
		queryCalls += 1;
		return {
			rows: [
				{
					session_id: 7,
					user_id: 42,
					email: "analyst@example.com",
					display_name: "Analyst",
					is_admin: false,
					status: "active",
					last_login_at: new Date("2026-04-25T10:00:00.000Z"),
					created_at: new Date("2026-04-01T00:00:00.000Z"),
					expires_at: new Date("2026-05-01T00:00:00.000Z"),
				},
			],
		};
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/recovery/runs",
			{
				method: "POST",
				headers: {
					authorization: "Bearer user-session-token",
				},
				body: {
					jobType: "shopify_attribution_hint_recovery",
					startDate: "2026-04-01",
					endDate: "2026-04-05",
				},
			},
		);

		assert.equal(response.status, 403);
		assert.deepEqual(body, {
			error: "forbidden",
			message: "Admin access required",
		});
		assert.equal(queryCalls, 1);
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});

test("manual recovery admin route rejects invalid date ranges before enqueueing", async () => {
	let queryCalls = 0;
	pool.query = (async () => {
		queryCalls += 1;
		return { rows: [] };
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/recovery/runs",
			{
				method: "POST",
				headers: {
					authorization: "Bearer test-reporting-token",
				},
				body: {
					jobType: "ga4_fallback_unattributed_recovery",
					startDate: "2026-04-10",
					endDate: "2026-04-01",
					chunkSize: 100,
				},
			},
		);

		assert.equal(response.status, 400);
		assert.equal(body.error, "invalid_date_range");
		assert.equal(queryCalls, 0);
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});

test("manual recovery admin route rejects invalid chunk sizes before enqueueing", async () => {
	let queryCalls = 0;
	pool.query = (async () => {
		queryCalls += 1;
		return { rows: [] };
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/recovery/runs",
			{
				method: "POST",
				headers: {
					authorization: "Bearer test-reporting-token",
				},
				body: {
					jobType: "ga4_fallback_unattributed_recovery",
					startDate: "2026-04-01",
					endDate: "2026-04-10",
					chunkSize: 0,
				},
			},
		);

		assert.equal(response.status, 400);
		assert.equal(body.error, "invalid_request");
		assert.deepEqual(body.details.fieldErrors.chunkSize, [
			"Number must be greater than or equal to 1",
		]);
		assert.equal(queryCalls, 0);
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});

test("manual recovery admin route durably enqueues work without starting execution", async () => {
	const row = buildRecoveryRunRow();
	const topLevelQueries: string[] = [];
	const clientQueries: string[] = [];
	let insertValues: unknown[] = [];

	pool.query = (async (sql: unknown) => {
		const text = String(sql);
		topLevelQueries.push(text);
		if (text.includes("tstzrange")) {
			return { rows: [] };
		}
		if (text.includes("WHERE id = $1")) {
			return { rows: [row] };
		}
		return { rows: [] };
	}) as typeof pool.query;

	pool.connect = (async () => {
		return {
			query: async (sql: unknown, values?: unknown[]) => {
				const text = String(sql);
				clientQueries.push(text);
				if (text.includes("INSERT INTO recovery_job_runs")) {
					insertValues = values ?? [];
					return { rows: [row] };
				}
				return { rows: [] };
			},
			release: () => undefined,
		};
	}) as typeof pool.connect;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/recovery/runs",
			{
				method: "POST",
				headers: {
					authorization: "Bearer test-reporting-token",
				},
				body: {
					jobType: "shopify_attribution_hint_recovery",
					startDate: "2026-04-01",
					endDate: "2026-04-05",
					dryRun: true,
					chunkSize: 125,
				},
			},
		);

		const allSql = [...topLevelQueries, ...clientQueries].join("\n");
		const payload = JSON.parse(String(insertValues[11])) as Record<
			string,
			unknown
		>;

		assert.equal(response.status, 201);
		assert.equal(body.created, true);
		assert.equal(body.status, "queued");
		assert.equal(body.run.status, "queued");
		assert.match(allSql, /INSERT INTO recovery_job_runs/);
		assert.match(allSql, /VALUES \(\$1, 'queued'/);
		assert.doesNotMatch(allSql, new RegExp("set" + "Immediate", "i"));
		assert.doesNotMatch(
			allSql,
			/UPDATE recovery_job_runs[\s\S]*status = 'running'/i,
		);
		assert.equal(insertValues[0], "shopify_attribution_hint_recovery");
		assert.equal(insertValues[1], "manual");
		assert.equal(insertValues[2], "internal");
		assert.deepEqual(payload, {
			chunkSize: 125,
			pageSize: 125,
			startDate: "2026-04-01",
			endDate: "2026-04-05",
		});
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		pool.connect = originalPoolConnect as typeof pool.connect;
		await closeServer(server);
	}
});

test("manual recovery admin route lists registered recovery job kinds", async () => {
	let queryCalls = 0;
	pool.query = (async () => {
		queryCalls += 1;
		return { rows: [] };
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/recovery/job-types",
			{
				headers: {
					authorization: "Bearer test-reporting-token",
				},
			},
		);

		assert.equal(response.status, 200);
		assert.equal(queryCalls, 0);
		assert.deepEqual(
			body.jobTypes.map((job: { jobType: string }) => job.jobType),
			[
				"shopify_attribution_hint_recovery",
				"ga4_fallback_unattributed_recovery",
				"campaign_metadata_api_refresh",
				"campaign_metadata_history_backfill",
				"ga4_session_enrichment_backfill",
				"order_attribution_backfill",
			],
		);
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});

test("manual recovery admin route lists run history with filters", async () => {
	let capturedSql = "";
	let capturedValues: unknown[] = [];
	pool.query = (async (sql: unknown, values?: unknown[]) => {
		capturedSql = String(sql);
		capturedValues = values ?? [];
		return {
			rows: [
				{
					id: "11111111-1111-4111-8111-111111111111",
					job_type: "ga4_fallback_unattributed_recovery",
					status: "dead_lettered",
					mode: "manual",
					initiated_by: "operator@example.com",
					dry_run: true,
					time_range_start: new Date("2026-04-01T00:00:00.000Z"),
					time_range_end: new Date("2026-04-30T23:59:59.999Z"),
					idempotency_key: "idem-key",
					concurrency_key: "concurrency-key",
					scope_key: "ga4-fallback-unattributed",
					resume_from_run_id: null,
					rerun_of_run_id: null,
					input_parameters: { chunkSize: 250 },
					checkpoint: { page: 2 },
					records_discovered: 12,
					records_claimed: 10,
					records_processed: 9,
					records_succeeded: 7,
					records_failed: 2,
					records_skipped: 1,
					records_retried: 3,
					side_effects_attempted: 0,
					side_effects_succeeded: 0,
					side_effects_suppressed: 7,
					claimed_by: "manual-ga4-fallback-recovery",
					queued_at: new Date("2026-05-01T10:00:00.000Z"),
					started_at: new Date("2026-05-01T10:01:00.000Z"),
					completed_at: new Date("2026-05-01T10:05:00.000Z"),
					last_heartbeat_at: new Date("2026-05-01T10:04:00.000Z"),
					error_code: "ga4_recovery_failed",
					error_message: "One record failed",
				},
			],
		};
	}) as typeof pool.query;

	const server = createServer();

	try {
		const { response, body } = await requestJson(
			server,
			"/api/admin/recovery/runs?jobType=ga4_fallback_unattributed_recovery&status=dead_lettered&limit=5",
			{
				headers: {
					authorization: "Bearer test-reporting-token",
				},
			},
		);

		assert.equal(response.status, 200);
		assert.match(capturedSql, /FROM recovery_job_runs/);
		assert.match(capturedSql, /job_type = \$1/);
		assert.match(capturedSql, /status = \$2/);
		assert.match(capturedSql, /LIMIT \$3/);
		assert.deepEqual(capturedValues, [
			"ga4_fallback_unattributed_recovery",
			"dead_lettered",
			5,
		]);
		assert.equal(body.runs.length, 1);
		assert.equal(body.runs[0].id, "11111111-1111-4111-8111-111111111111");
		assert.equal(body.runs[0].jobType, "ga4_fallback_unattributed_recovery");
		assert.equal(body.runs[0].recordsProcessed, 9);
		assert.equal(body.runs[0].sideEffectsSuppressed, 7);
		assert.equal(body.runs[0].errorMessage, "One record failed");
	} finally {
		pool.query = originalPoolQuery as typeof pool.query;
		await closeServer(server);
	}
});
