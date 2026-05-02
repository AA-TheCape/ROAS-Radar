import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import test from "node:test";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@localhost:5432/roas_radar_test";

let cachedModules: {
	pool: typeof import("../src/db/pool.js").pool;
	closeServer: typeof import("../src/server.js").closeServer;
	createServer: typeof import("../src/server.js").createServer;
	originalPoolQuery: typeof import("../src/db/pool.js").pool.query;
	originalPoolConnect: typeof import("../src/db/pool.js").pool.connect;
} | null = null;

const validPayload = {
	schema_version: 1,
	roas_radar_session_id: "123e4567-e89b-42d3-a456-426614174000",
	occurred_at: "2026-04-23T12:00:00.000Z",
	captured_at: "2026-04-23T12:00:05.000Z",
	landing_url:
		"https://example.com/?utm_source=Google&utm_medium=CPC&utm_campaign=Spring",
	referrer_url: "https://google.com/search?q=roas",
	page_url: "https://example.com/products/widget?gclid=ABC123#details",
	utm_source: "Google",
	utm_medium: "CPC",
	utm_campaign: "Spring",
	utm_content: "Hero",
	utm_term: "Widget",
	gclid: "ABC123",
	gbraid: "GB-123",
	wbraid: "WB-456",
	consent_state: "denied",
	fbclid: null,
	ttclid: null,
	msclkid: null,
};

const rawPayloadWithExtraFields = {
	...validPayload,
	landing_url:
		"  https://example.com/?utm_source=Google&utm_medium=CPC&utm_campaign=Spring  ",
	referrer_url: "  https://google.com/search?q=roas  ",
	page_url: "  https://example.com/products/widget?gclid=ABC123#details  ",
	utm_source: "  Google  ",
	utm_medium: "  CPC  ",
	utm_campaign: "  Spring  ",
	utm_content: "  Hero  ",
	utm_term: "  Widget  ",
	gclid: "  ABC123  ",
	gbraid: "  GB-123  ",
	wbraid: "  WB-456  ",
	debug_context: {
		nested: {
			untouched: true,
		},
	},
};

async function postAttribution(
	server: { address(): AddressInfo | null },
	payload: unknown,
): Promise<{ response: Response; body: Record<string, unknown> }> {
	const address = server.address() as AddressInfo;
	const response = await fetch(
		`http://127.0.0.1:${address.port}/track/attribution`,
		{
			method: "POST",
			headers: {
				"content-type": "application/json",
				accept: "application/json",
			},
			body: JSON.stringify(payload),
		},
	);

	return {
		response,
		body: (await response.json()) as Record<string, unknown>,
	};
}

test.afterEach(async () => {
	const { pool, originalPoolConnect, originalPoolQuery } = await getModules();
	pool.query = originalPoolQuery as typeof pool.query;
	pool.connect = originalPoolConnect as typeof pool.connect;
});

async function getModules() {
	if (cachedModules) {
		return cachedModules;
	}

	const poolModule = await import("../src/db/pool.js");
	const serverModule = await import("../src/server.js");

	cachedModules = {
		pool: poolModule.pool,
		closeServer: serverModule.closeServer,
		createServer: serverModule.createServer,
		originalPoolQuery: poolModule.pool.query.bind(poolModule.pool),
		originalPoolConnect: poolModule.pool.connect.bind(poolModule.pool),
	};

	return cachedModules;
}

test("tracking attribution endpoint rejects invalid canonical payloads with structured field errors", async () => {
	const { createServer, closeServer } = await getModules();
	const server = createServer();

	try {
		const { response, body } = await postAttribution(server, {
			...validPayload,
			roas_radar_session_id: "not-a-uuid",
			page_url: "ftp://example.com/invalid",
		});

		assert.equal(response.status, 400);
		assert.equal(body.error, "invalid_request");
		assert.equal(body.message, "Invalid attribution capture payload");
		assert.deepEqual(body.details, {
			formErrors: [],
			fieldErrors: {
				roas_radar_session_id: ["Invalid uuid"],
				page_url: ["URL must use http or https"],
			},
		});
	} finally {
		await closeServer(server);
	}
});

test("tracking attribution endpoint persists canonical touch events and mirrors them into tracking tables", async () => {
	const { pool, createServer, closeServer } = await getModules();
	const queries: Array<{ text: string; params?: unknown[] }> = [];
	let connectCalls = 0;

	pool.query = (async (text: string, params?: unknown[]) => {
		queries.push({ text, params });

		if (text.includes("FROM session_attribution_touch_events")) {
			return {
				rows: [],
			};
		}

		throw new Error(`Unexpected pool.query call: ${text}`);
	}) as typeof pool.query;

	pool.connect = (async () => {
		connectCalls += 1;

		return {
			query: async (text: string, params?: unknown[]) => {
				queries.push({ text, params });

				if (text === "BEGIN" || text === "COMMIT" || text === "ROLLBACK") {
					return { rows: [] };
				}

				if (text.includes("INSERT INTO session_attribution_identities")) {
					return { rows: [] };
				}

				if (text.includes("INSERT INTO tracking_sessions")) {
					return { rows: [] };
				}

				if (text.includes("INSERT INTO session_attribution_touch_events")) {
					return {
						rows: [
							{
								id: 42,
								captured_at: new Date("2026-04-23T12:00:05.000Z"),
								roas_radar_session_id: validPayload.roas_radar_session_id,
							},
						],
					};
				}

				if (text.includes("INSERT INTO tracking_events")) {
					return { rows: [] };
				}

				throw new Error(`Unexpected client.query call: ${text}`);
			},
			release: () => undefined,
		};
	}) as typeof pool.connect;

	const server = createServer();

	try {
		const { response, body } = await postAttribution(server, validPayload);

		assert.equal(response.status, 200);
		assert.equal(body.ok, true);
		assert.equal(body.sessionId, validPayload.roas_radar_session_id);
		assert.equal(body.touchEventId, 42);
		assert.equal(body.deduplicated, false);
		assert.equal(connectCalls, 1);

		const touchInsert = queries.find((entry) =>
			entry.text.includes("INSERT INTO session_attribution_touch_events"),
		);
		assert.ok(touchInsert);
		assert.equal(
			touchInsert.params?.[4],
			"https://example.com/products/widget?gclid=ABC123",
		);
		assert.equal(touchInsert.params?.[6], "google");
		assert.equal(touchInsert.params?.[7], "cpc");
		assert.equal(touchInsert.params?.[12], "GB-123");
		assert.equal(touchInsert.params?.[13], "WB-456");
		assert.equal(touchInsert.params?.[17], "denied");
		assert.equal(touchInsert.params?.[18], "server");
		assert.deepEqual(
			JSON.parse(String(touchInsert.params?.[20])),
			validPayload,
		);

		const trackingInsert = queries.find((entry) =>
			entry.text.includes("INSERT INTO tracking_events"),
		);
		assert.ok(trackingInsert);
		assert.equal(trackingInsert.params?.[6], "google");
		assert.equal(trackingInsert.params?.[7], "cpc");
		assert.equal(trackingInsert.params?.[12], "GB-123");
		assert.equal(trackingInsert.params?.[13], "WB-456");
		assert.equal(trackingInsert.params?.[17], "denied");
		assert.equal(trackingInsert.params?.[19], "server");
		assert.deepEqual(
			JSON.parse(String(trackingInsert.params?.[20])),
			validPayload,
		);
	} finally {
		await closeServer(server);
	}
});

test("tracking attribution endpoint preserves the full raw payload before canonical normalization", async () => {
	const { pool, createServer, closeServer } = await getModules();
	const queries: Array<{ text: string; params?: unknown[] }> = [];

	pool.query = (async (text: string, params?: unknown[]) => {
		queries.push({ text, params });

		if (text.includes("FROM session_attribution_touch_events")) {
			return {
				rows: [],
			};
		}

		throw new Error(`Unexpected pool.query call: ${text}`);
	}) as typeof pool.query;

	pool.connect = (async () => ({
		query: async (text: string, params?: unknown[]) => {
			queries.push({ text, params });

			if (text === "BEGIN" || text === "COMMIT" || text === "ROLLBACK") {
				return { rows: [] };
			}

			if (text.includes("INSERT INTO session_attribution_identities")) {
				return { rows: [] };
			}

			if (text.includes("INSERT INTO tracking_sessions")) {
				return { rows: [] };
			}

			if (text.includes("INSERT INTO session_attribution_touch_events")) {
				return {
					rows: [
						{
							id: 42,
							captured_at: new Date("2026-04-23T12:00:05.000Z"),
							roas_radar_session_id: validPayload.roas_radar_session_id,
						},
					],
				};
			}

			if (text.includes("INSERT INTO tracking_events")) {
				return { rows: [] };
			}

			throw new Error(`Unexpected client.query call: ${text}`);
		},
		release: () => undefined,
	})) as typeof pool.connect;

	const server = createServer();

	try {
		const { response, body } = await postAttribution(
			server,
			rawPayloadWithExtraFields,
		);

		assert.equal(response.status, 200);
		assert.equal(body.ok, true);

		const touchInsert = queries.find((entry) =>
			entry.text.includes("INSERT INTO session_attribution_touch_events"),
		);
		assert.ok(touchInsert);
		assert.equal(
			touchInsert.params?.[4],
			"https://example.com/products/widget?gclid=ABC123",
		);
		assert.equal(touchInsert.params?.[6], "google");
		assert.equal(touchInsert.params?.[7], "cpc");
		assert.equal(touchInsert.params?.[12], "GB-123");
		assert.equal(touchInsert.params?.[13], "WB-456");
		assert.deepEqual(
			JSON.parse(String(touchInsert.params?.[20])),
			rawPayloadWithExtraFields,
		);

		const trackingInsert = queries.find((entry) =>
			entry.text.includes("INSERT INTO tracking_events"),
		);
		assert.ok(trackingInsert);
		assert.equal(
			trackingInsert.params?.[4],
			"https://example.com/products/widget?gclid=ABC123",
		);
		assert.equal(trackingInsert.params?.[6], "google");
		assert.equal(trackingInsert.params?.[7], "cpc");
		assert.equal(trackingInsert.params?.[12], "GB-123");
		assert.equal(trackingInsert.params?.[13], "WB-456");
		assert.deepEqual(
			JSON.parse(String(trackingInsert.params?.[20])),
			rawPayloadWithExtraFields,
		);
	} finally {
		await closeServer(server);
	}
});

test("tracking attribution endpoint short-circuits duplicate payloads without opening a write transaction", async () => {
	const { pool, createServer, closeServer } = await getModules();
	let connectCalls = 0;

	pool.query = (async (text: string) => {
		if (text.includes("FROM session_attribution_touch_events")) {
			return {
				rows: [
					{
						id: 77,
						captured_at: new Date("2026-04-23T12:00:05.000Z"),
						roas_radar_session_id: validPayload.roas_radar_session_id,
					},
				],
			};
		}

		throw new Error(`Unexpected pool.query call: ${text}`);
	}) as typeof pool.query;

	pool.connect = (async () => {
		connectCalls += 1;
		throw new Error("duplicate requests should not open a transaction");
	}) as typeof pool.connect;

	const server = createServer();

	try {
		const { response, body } = await postAttribution(server, validPayload);

		assert.equal(response.status, 200);
		assert.equal(body.touchEventId, 77);
		assert.equal(body.deduplicated, true);
		assert.equal(connectCalls, 0);
	} finally {
		await closeServer(server);
	}
});
