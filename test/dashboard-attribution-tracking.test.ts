import assert from "node:assert/strict";
import test from "node:test";

import {
	createDom,
	loadDashboardModule,
	tick,
} from "./dashboard-ui-test-helpers";

function jsonResponse(body: unknown, status = 200) {
	return new Response(JSON.stringify(body), {
		status,
		headers: {
			"content-type": "application/json",
		},
	});
}

test("React attribution tracker bootstraps the first landing and posts updated page views on SPA navigation", async () => {
	const dom = createDom({
		url: "http://localhost/?utm_source=google&utm_medium=cpc&gbraid=GBRAID-123&wbraid=WBRAID-456",
	});
	const previousFetch = globalThis.fetch;
	const sessionCalls: URL[] = [];
	const trackPayloads: Array<Record<string, unknown>> = [];

	try {
		const trackingModule = await loadDashboardModule<
			typeof import("../dashboard/src/lib/attributionTracking")
		>("dashboard/src/lib/attributionTracking.tsx");

		trackingModule.__attributionTrackingTestUtils.reset();

		globalThis.fetch = (async (
			input: string | URL | Request,
			init?: RequestInit,
		) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof Request
						? input.url
						: input.toString(),
				dom.window.location.origin,
			);

			if (url.pathname === "/track/session") {
				sessionCalls.push(url);
				return jsonResponse({
					ok: true,
					sessionId: "123e4567-e89b-42d3-a456-426614174000",
					createdAt: "2026-04-23T12:00:00.000Z",
					isNewSession: true,
				});
			}

			if (url.pathname === "/track") {
				trackPayloads.push(
					JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>,
				);
				return jsonResponse({
					ok: true,
					sessionId: "123e4567-e89b-42d3-a456-426614174000",
				});
			}

			throw new Error(`Unexpected fetch: ${url.pathname}`);
		}) as typeof globalThis.fetch;

		trackingModule.startAttributionTracking();
		await tick(25);

		assert.equal(sessionCalls.length, 1);
		assert.equal(
			sessionCalls[0].searchParams.get("landingUrl"),
			"http://localhost/?utm_source=google&utm_medium=cpc&gbraid=GBRAID-123&wbraid=WBRAID-456",
		);
		assert.equal(trackPayloads.length, 1);
		assert.equal(
			trackPayloads[0].pageUrl,
			"http://localhost/?utm_source=google&utm_medium=cpc&gbraid=GBRAID-123&wbraid=WBRAID-456",
		);
		assert.equal(trackPayloads[0].consentState, "unknown");

		dom.window.history.pushState({}, "", "/collections/sale");
		await tick(25);

		assert.equal(trackPayloads.length, 2);
		assert.equal(trackPayloads[1].pageUrl, "http://localhost/collections/sale");
		assert.equal(trackPayloads[1].consentState, "unknown");
		assert.equal(
			trackPayloads[1].referrerUrl,
			"http://localhost/?utm_source=google&utm_medium=cpc&gbraid=GBRAID-123&wbraid=WBRAID-456",
		);
	} finally {
		globalThis.fetch = previousFetch;
		dom.window.close();
	}
});

test("React attribution tracker retries transient ingestion failures from durable storage", async () => {
	const dom = createDom({
		url: "http://localhost/?utm_source=google&utm_medium=cpc",
	});
	const previousFetch = globalThis.fetch;
	let postAttempts = 0;

	dom.window.__ROAS_RADAR_ATTRIBUTION_CONFIG__ = {
		retryBaseDelayMs: 5,
		maxRetryDelayMs: 5,
	};

	try {
		const trackingModule = await loadDashboardModule<
			typeof import("../dashboard/src/lib/attributionTracking")
		>("dashboard/src/lib/attributionTracking.tsx");

		trackingModule.__attributionTrackingTestUtils.reset();

		globalThis.fetch = (async (
			input: string | URL | Request,
			init?: RequestInit,
		) => {
			const url = new URL(
				typeof input === "string"
					? input
					: input instanceof Request
						? input.url
						: input.toString(),
				dom.window.location.origin,
			);

			if (url.pathname === "/track/session") {
				return jsonResponse({
					ok: true,
					sessionId: "123e4567-e89b-42d3-a456-426614174001",
					createdAt: "2026-04-23T12:00:00.000Z",
					isNewSession: true,
				});
			}

			if (url.pathname === "/track") {
				postAttempts += 1;

				if (postAttempts === 1) {
					return jsonResponse({ ok: false }, 503);
				}

				assert.ok(init?.body);
				return jsonResponse({
					ok: true,
					sessionId: "123e4567-e89b-42d3-a456-426614174001",
				});
			}

			throw new Error(`Unexpected fetch: ${url.pathname}`);
		}) as typeof globalThis.fetch;

		trackingModule.startAttributionTracking();
		await tick(50);

		assert.equal(postAttempts, 2);
		assert.equal(
			dom.window.localStorage.getItem("roas_radar_pending_track_events"),
			"[]",
		);
	} finally {
		globalThis.fetch = previousFetch;
		dom.window.close();
	}
});
