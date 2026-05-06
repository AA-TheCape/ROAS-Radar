import assert from "node:assert/strict";
import test from "node:test";

import {
	changeInputValue,
	click,
	h,
	loadDashboardModule,
	mountUi,
	tick,
} from "./dashboard-ui-test-helpers";

test("identity graph health view exposes filters, KPIs, and conflict drill-down selection", async () => {
	const { default: IdentityGraphHealthView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/IdentityGraphHealthView")
	>("dashboard/src/components/IdentityGraphHealthView.tsx");

	const filterUpdates: Array<{
		startDate: string;
		endDate: string;
		source?: string;
	}> = [];
	let refreshCount = 0;

	const mounted = await mountUi(
		h(IdentityGraphHealthView, {
			filters: {
				startDate: "2026-04-01",
				endDate: "2026-04-05",
				source: "",
			},
			onFiltersChange: (next: {
				startDate: string;
				endDate: string;
				source?: string;
			}) => {
				filterUpdates.push(next);
			},
			onRefresh: () => {
				refreshCount += 1;
			},
			reportingTimezone: "UTC",
			overviewSection: {
				data: {
					range: {
						startDate: "2026-04-01",
						endDate: "2026-04-05",
					},
					source: null,
					summary: {
						totalIngestions: 14,
						linkedIngestions: 10,
						skippedIngestions: 2,
						conflictIngestions: 2,
						mergeRuns: 3,
						rehomedNodes: 7,
						quarantinedNodes: 2,
						unresolvedConflicts: 1,
						unlinkedSessions: 5,
						linkedSessions: 18,
					},
					series: [
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
					],
					backfill: {
						activeRuns: 1,
						failedRuns: 0,
						completedRuns: 4,
						latestRun: {
							runId: "run-1",
							status: "processing",
							requestedBy: "ops@roasradar.dev",
							workerId: "identity-worker-1",
							sources: ["shopify_orders"],
							startedAt: "2026-04-05T10:00:00.000Z",
							completedAt: null,
							updatedAt: "2026-04-05T10:05:00.000Z",
							errorCode: null,
							errorMessage: null,
						},
					},
				},
				loading: false,
				error: null,
			},
			conflictsSection: {
				data: {
					range: {
						startDate: "2026-04-01",
						endDate: "2026-04-05",
					},
					source: null,
					conflicts: [
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
							conflictCode:
								"phone_hash_conflicts_across_authoritative_customers",
							firstObservedAt: "2026-04-02T00:00:00.000Z",
							lastObservedAt: "2026-04-04T00:00:00.000Z",
							updatedAt: "2026-04-04T01:00:00.000Z",
						},
						{
							edgeId: "edge-2",
							journeyId: "journey-2",
							journeyStatus: "conflicted",
							authoritativeShopifyCustomerId: "sc-2",
							nodeType: "hashed_email",
							nodeKey: "a".repeat(64),
							evidenceSource: "backfill",
							sourceTable: "shopify_customers",
							sourceRecordId: "customer-42",
							conflictCode:
								"hashed_email_conflicts_across_authoritative_customers",
							firstObservedAt: "2026-04-03T00:00:00.000Z",
							lastObservedAt: "2026-04-05T00:00:00.000Z",
							updatedAt: "2026-04-05T01:00:00.000Z",
						},
					],
				},
				loading: false,
				error: null,
			},
		}),
	);

	try {
		const text = mounted.container.textContent ?? "";
		assert.match(text, /Identity graph health/);
		assert.match(text, /Merge runs/);
		assert.match(text, /Unlinked sessions/);
		assert.match(text, /Latest identity graph run/);
		assert.match(text, /Phone Hash Conflicts Across Authoritative Customers/);

		const refreshButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) => /Refresh health metrics/.test(button.textContent ?? ""));
		assert.ok(refreshButton);
		click(refreshButton);
		await tick();
		assert.equal(refreshCount, 1);

		const [startDateInput] = Array.from(
			mounted.container.querySelectorAll<HTMLInputElement>(
				'input[type="date"]',
			),
		);
		assert.ok(startDateInput);

		const sourceSelect = mounted.container.querySelector(
			"select",
		) as HTMLSelectElement;
		assert.ok(sourceSelect);
		changeInputValue(sourceSelect, "backfill");
		await tick();
		assert.deepEqual(filterUpdates[0], {
			startDate: "2026-04-01",
			endDate: "2026-04-05",
			source: "backfill",
		});

		const secondConflictButton = mounted.container.querySelector(
			'button[aria-label="Open conflict hashed_email_conflicts_across_authoritative_customers"]',
		) as HTMLButtonElement;
		assert.ok(secondConflictButton);
		click(secondConflictButton);
		await tick();

		assert.match(
			mounted.container.textContent ?? "",
			/Hashed Email Conflicts Across Authoritative Customers/,
		);
		assert.match(mounted.container.textContent ?? "", /journey-2/);
		assert.match(mounted.container.textContent ?? "", /customer-42/);
	} finally {
		mounted.cleanup();
	}
});
