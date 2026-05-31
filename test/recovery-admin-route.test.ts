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
