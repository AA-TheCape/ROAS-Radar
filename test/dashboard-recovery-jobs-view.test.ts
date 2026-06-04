import assert from "node:assert/strict";
import test from "node:test";

import {
	React,
	click,
	createDom,
	loadDashboardModule,
	mountUi,
	tick,
} from "./dashboard-ui-test-helpers";

type FetchCall = {
	path: string;
	method: string;
	body: unknown;
};

function jsonResponse(body: unknown, status = 200) {
	return new Response(JSON.stringify(body), {
		status,
		headers: {
			"content-type": "application/json",
		},
	});
}

function parseBody(init?: RequestInit): unknown {
	if (!init?.body || typeof init.body !== "string") {
		return null;
	}

	return JSON.parse(init.body);
}

function buildRun(jobType: string, id: string, overrides: Record<string, unknown> = {}) {
	return {
		id,
		jobType,
		status: "queued",
		mode: "manual",
		initiatedBy: "operator@example.com",
		dryRun: true,
		timeRangeStart: "2026-04-01T00:00:00.000Z",
		timeRangeEnd: "2026-04-30T23:59:59.999Z",
		scopeKey: jobType,
		inputParameters: {},
		checkpoint: {},
		recordsDiscovered: 1,
		recordsClaimed: 1,
		recordsProcessed: 1,
		recordsSucceeded: 1,
		recordsFailed: 0,
		recordsSkipped: 0,
		recordsRetried: 0,
		sideEffectsAttempted: 0,
		sideEffectsSucceeded: 0,
		sideEffectsSuppressed: 1,
		claimedBy: null,
		queuedAt: "2026-05-01T10:00:00.000Z",
		startedAt: null,
		completedAt: null,
		lastHeartbeatAt: null,
		errorCode: null,
		errorMessage: null,
		...overrides,
	};
}

function createFetchStub(calls: FetchCall[]) {
	let nextRunId = 1;

	return async (input: string | URL | Request, init?: RequestInit) => {
		const rawUrl =
			typeof input === "string"
				? input
				: input instanceof URL
					? input.toString()
					: input.url;
		const url = new URL(rawUrl, "http://localhost");
		const method = init?.method ?? "GET";
		const body = parseBody(init);

		calls.push({ path: url.pathname, method, body });

		if (url.pathname === "/api/admin/recovery/runs" && method === "GET") {
			return jsonResponse({
				runs: [
					buildRun("shopify_order_reimport", "11111111-1111-4111-8111-111111111111"),
					buildRun("ga4_session_enrichment_backfill", "22222222-2222-4222-8222-222222222222"),
					buildRun("campaign_metadata_api_refresh", "33333333-3333-4333-8333-333333333333"),
					buildRun("order_attribution_backfill", "44444444-4444-4444-8444-444444444444", {
						status: "dead_lettered",
						errorCode: "recovery_job_attempts_exhausted",
						errorMessage: "Recovery job exhausted retry attempts",
					}),
				],
			});
		}

		if (url.pathname === "/api/admin/recovery/runs" && method === "POST") {
			const jobType = (body as { jobType: string }).jobType;
			const id = `${String(nextRunId).padStart(8, "0")}-aaaa-4aaa-8aaa-aaaaaaaaaaaa`;
			nextRunId += 1;
			return jsonResponse(
				{
					created: true,
					runId: id,
					status: "queued",
					jobType,
					dryRun: (body as { dryRun?: boolean }).dryRun ?? true,
					progressLink: `/api/admin/recovery/runs/${id}`,
					startLink: `/api/admin/recovery/runs/${id}/start`,
					cancelLink: `/api/admin/recovery/runs/${id}/cancel`,
					run: buildRun(jobType, id),
				},
				201,
			);
		}

		if (/^\/api\/admin\/recovery\/runs\/[^/]+\/start$/.test(url.pathname)) {
			return jsonResponse({ queued: true, started: false });
		}

		throw new Error(`Unexpected fetch: ${method} ${url.pathname}`);
	};
}

async function launchCard(container: ParentNode, title: string) {
	const card = Array.from(container.querySelectorAll("article"))
		.filter((candidate) => candidate.textContent?.includes(title))
		.sort(
			(a, b) => (a.textContent?.length ?? 0) - (b.textContent?.length ?? 0),
		)[0];
	assert.ok(card, `expected card ${title}`);
	const button = Array.from(card.querySelectorAll("button")).find((candidate) =>
		candidate.textContent?.includes("Launch"),
	);
	assert.ok(button, `expected launch button for ${title}`);
	click(button);
	await tick(20);
}

test("recovery jobs view launches all exposed jobs through recovery APIs and shows shared history", async () => {
	const calls: FetchCall[] = [];
	const previousFetch = globalThis.fetch;
	globalThis.fetch = createFetchStub(calls) as typeof globalThis.fetch;

	try {
		createDom();
		const { default: RecoveryJobsView } = await loadDashboardModule<
			typeof import("../dashboard/src/components/RecoveryJobsView")
		>("dashboard/src/components/RecoveryJobsView.tsx");
		const mounted = await mountUi(
			React.createElement(RecoveryJobsView, { reportingTimezone: "UTC" }),
		);

		try {
			await tick(20);
			const text = mounted.container.textContent ?? "";
			assert.match(text, /Shopify order import/);
			assert.match(text, /GA4 session enrichment/);
			assert.match(text, /Campaign metadata refresh/);
			assert.match(text, /Campaign metadata history/);
			assert.match(text, /Order attribution backfill/);
			assert.match(text, /dead lettered/);
			assert.match(text, /recovery_job_attempts_exhausted/);

			await launchCard(mounted.container, "Shopify order import");
			await launchCard(mounted.container, "GA4 session enrichment");
			await launchCard(mounted.container, "Campaign metadata refresh");
			await launchCard(mounted.container, "Campaign metadata history");
			await launchCard(mounted.container, "Order attribution backfill");

			const createCalls = calls.filter(
				(call) =>
					call.path === "/api/admin/recovery/runs" && call.method === "POST",
			);
			assert.deepEqual(
				createCalls.map((call) => (call.body as { jobType: string }).jobType),
				[
					"shopify_order_reimport",
					"ga4_session_enrichment_backfill",
					"campaign_metadata_api_refresh",
					"campaign_metadata_history_backfill",
					"order_attribution_backfill",
				],
			);
			assert.equal(
				calls.some((call) => call.path === "/api/admin/shopify/orders/backfill"),
				false,
			);
			assert.equal(
				calls.some(
					(call) => call.path === "/api/admin/attribution/orders/backfill",
				),
				false,
			);
			assert.deepEqual(createCalls.at(-1)?.body, {
				jobType: "order_attribution_backfill",
				startDate: (createCalls.at(-1)?.body as { startDate: string }).startDate,
				endDate: (createCalls.at(-1)?.body as { endDate: string }).endDate,
				dryRun: true,
				chunkSize: 250,
				limit: 500,
				onlyWebOrders: true,
				skipShopifyWriteback: false,
			});
		} finally {
			mounted.cleanup();
		}
	} finally {
		globalThis.fetch = previousFetch;
	}
});
