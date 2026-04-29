import assert from "node:assert/strict";
import test from "node:test";

import { formatCurrency, formatNumber } from "../dashboard/src/lib/format";
import {
	click,
	createOrderDetailsProps,
	createReportingDashboardProps,
	createSettingsAdminProps,
	createShellProps,
	h,
	loadDashboardModule,
	mountUi,
	tick,
} from "./dashboard-ui-test-helpers";

test("authenticated shell mobile navigation opens and closes on route change", async () => {
	const { default: AuthenticatedAppShell } = await loadDashboardModule<
		typeof import("../dashboard/src/components/AuthenticatedAppShell")
	>("dashboard/src/components/AuthenticatedAppShell.tsx");

	let activeNavKey = "dashboard";
	const mounted = await mountUi(
		h(AuthenticatedAppShell, createShellProps({ activeNavKey })),
		{ width: 768, height: 900 },
	);

	try {
		const toggle = mounted.container.querySelector(
			'button[aria-controls="app-shell-mobile-nav"]',
		) as HTMLButtonElement;
		assert.ok(toggle);

		click(toggle);
		await tick();
		assert.ok(
			mounted.dom.window.document.getElementById("app-shell-mobile-nav"),
		);

		activeNavKey = "settings";
		mounted.root.render(
			h(AuthenticatedAppShell, createShellProps({ activeNavKey })),
		);
		await tick();

		assert.equal(
			mounted.dom.window.document.getElementById("app-shell-mobile-nav"),
			null,
		);
	} finally {
		mounted.cleanup();
	}
});

test("authenticated shell removes deprecated workspace and header cards without leaving layout gaps", async () => {
	const { default: AuthenticatedAppShell } = await loadDashboardModule<
		typeof import("../dashboard/src/components/AuthenticatedAppShell")
	>("dashboard/src/components/AuthenticatedAppShell.tsx");

	const mounted = await mountUi(h(AuthenticatedAppShell, createShellProps()), {
		width: 1440,
		height: 900,
	});

	try {
		assert.doesNotMatch(mounted.container.textContent ?? "", /Workspace/);
		assert.doesNotMatch(mounted.container.textContent ?? "", /Active window/);
		assert.match(mounted.container.textContent ?? "", /Current time/);
		assert.match(mounted.container.textContent ?? "", /UTC Apr 20, 7:15 PM/);
		assert.equal(
			mounted.container.querySelector('aside[aria-label="Section navigation"]'),
			null,
		);
		assert.equal(
			mounted.container.querySelector(
				'[aria-label="Current workspace status"]',
			),
			null,
		);
		assert.ok(
			mounted.container.querySelector('[aria-label="Current timestamp"]'),
		);
		assert.ok(mounted.container.querySelector("#app-shell-main"));
	} finally {
		mounted.cleanup();
	}
});

test("title bar timestamp updates on the next minute boundary and clears timers on unmount", async () => {
	const { default: TitleBarTimestamp } = await loadDashboardModule<
		typeof import("../dashboard/src/components/TitleBarTimestamp")
	>("dashboard/src/components/TitleBarTimestamp.tsx");

	let now = new Date("2026-04-20T19:14:15.250Z");
	let timeoutCallback: (() => void) | null = null;
	let intervalCallback: (() => void) | null = null;
	let timeoutDelay = -1;
	let intervalDelay = -1;
	const clearedTimeouts: number[] = [];
	const clearedIntervals: number[] = [];

	const mounted = await mountUi(
		h(TitleBarTimestamp, {
			getNow: () => now,
			scheduleTimeout: ((callback: () => void, delay?: number) => {
				timeoutCallback = callback;
				timeoutDelay = delay ?? 0;
				return 11 as ReturnType<typeof setTimeout>;
			}) as typeof setTimeout,
			clearScheduledTimeout: ((timer: number) => {
				clearedTimeouts.push(timer);
			}) as typeof clearTimeout,
			scheduleInterval: ((callback: () => void, delay?: number) => {
				intervalCallback = callback;
				intervalDelay = delay ?? 0;
				return 29 as ReturnType<typeof setInterval>;
			}) as typeof setInterval,
			clearScheduledInterval: ((timer: number) => {
				clearedIntervals.push(timer);
			}) as typeof clearInterval,
		}),
	);

	try {
		assert.match(mounted.container.textContent ?? "", /UTC Apr 20, 7:14 PM/);
		assert.equal(timeoutDelay, 44_750);

		now = new Date("2026-04-20T19:15:00.000Z");
		timeoutCallback?.();
		await tick();

		assert.equal(intervalDelay, 60_000);
		assert.match(mounted.container.textContent ?? "", /UTC Apr 20, 7:15 PM/);

		now = new Date("2026-04-20T19:16:00.000Z");
		intervalCallback?.();
		await tick();

		assert.match(mounted.container.textContent ?? "", /UTC Apr 20, 7:16 PM/);
	} finally {
		mounted.cleanup();
	}

	assert.deepEqual(clearedTimeouts, [11]);
	assert.deepEqual(clearedIntervals, [29]);
});

test("reporting dashboard search and order drill-in stay wired for high-traffic workflows", async () => {
	const { default: ReportingDashboard } = await loadDashboardModule<
		typeof import("../dashboard/src/components/ReportingDashboard")
	>("dashboard/src/components/ReportingDashboard.tsx");

	let openedOrderId: string | null = null;
	const mounted = await mountUi(
		h(
			ReportingDashboard,
			createReportingDashboardProps({
				onOpenOrderDetails: (shopifyOrderId: string) => {
					openedOrderId = shopifyOrderId;
				},
			}),
		),
	);

	try {
		assert.match(mounted.container.textContent ?? "", /Campaign performance/);
		assert.match(mounted.container.textContent ?? "", /Attributed orders/);

		const orderButton = mounted.container.querySelector(
			'button[aria-label="Open order details for Shopify order 1105"]',
		);
		assert.ok(orderButton);

		click(orderButton);
		await tick();
		assert.equal(openedOrderId, "1105");
	} finally {
		mounted.cleanup();
	}
});

test("reporting dashboard summary cards keep spend visible alongside the overview KPIs", async () => {
	const { default: ReportingDashboard } = await loadDashboardModule<
		typeof import("../dashboard/src/components/ReportingDashboard")
	>("dashboard/src/components/ReportingDashboard.tsx");

	const mounted = await mountUi(
		h(ReportingDashboard, createReportingDashboardProps()),
	);

	try {
		const text = mounted.container.textContent ?? "";
		assert.match(text, /Overview command center/);
		assert.match(text, /Revenue captured/);
		assert.match(text, /Spend/);
		assert.match(text, /\$11,376\.00/);
		assert.match(text, /Media/);
	} finally {
		mounted.cleanup();
	}
});

test("reporting dashboard preserves key sections across qa target breakpoints", async () => {
	const { default: ReportingDashboard } = await loadDashboardModule<
		typeof import("../dashboard/src/components/ReportingDashboard")
	>("dashboard/src/components/ReportingDashboard.tsx");

	for (const width of [375, 768, 1024, 1440]) {
		const mounted = await mountUi(
			h(ReportingDashboard, createReportingDashboardProps()),
			{ width, height: 900 },
		);

		try {
			const text = mounted.container.textContent ?? "";
			assert.match(text, /Top control card/);
			assert.match(text, /Overview command center/);
			assert.match(text, /Top buckets/);
			assert.match(text, /Lowest-performing buckets/);
			assert.match(text, /Marketing spend detail/);
			assert.match(text, /Grouped spend total/);
		} finally {
			mounted.cleanup();
		}
	}
});

test("reporting dashboard keeps overview, charts, and report tables internally consistent for qa totals", async () => {
	const { default: ReportingDashboard } = await loadDashboardModule<
		typeof import("../dashboard/src/components/ReportingDashboard")
	>("dashboard/src/components/ReportingDashboard.tsx");

	const props = createReportingDashboardProps({
		summaryCards: [
			{ label: "Visits", value: "1,500", detail: "Apr 1 to Apr 3" },
			{ label: "Orders", value: "12", detail: "0.8% conversion" },
			{ label: "Revenue", value: "$1,200.00", detail: "2 ROAS" },
			{ label: "Spend", value: "$600.00", detail: "Apr 1 to Apr 3" },
			{ label: "AOV", value: "$100.00", detail: "12 attributed orders" },
		],
		summarySection: {
			data: {
				visits: 1500,
				orders: 12,
				revenue: 1200,
				spend: 600,
				conversionRate: 0.008,
				roas: 2,
			},
			loading: false,
			error: null,
		},
		campaignsSection: {
			data: [
				{
					source: "google",
					medium: "cpc",
					campaign: "Brand Search",
					content: "hero",
					visits: 900,
					orders: 7,
					revenue: 700,
					conversionRate: 0.0078,
				},
				{
					source: "meta",
					medium: "paid_social",
					campaign: "Prospecting Video",
					content: "video",
					visits: 600,
					orders: 5,
					revenue: 500,
					conversionRate: 0.0083,
				},
			],
			loading: false,
			error: null,
		},
		timeseriesSection: {
			data: [
				{ date: "2026-04-01", visits: 500, orders: 4, revenue: 400 },
				{ date: "2026-04-02", visits: 450, orders: 3, revenue: 300 },
				{ date: "2026-04-03", visits: 550, orders: 5, revenue: 500 },
			],
			loading: false,
			error: null,
		},
		ordersSection: {
			data: [
				{
					shopifyOrderId: "1201",
					processedAt: "2026-04-03T18:00:00.000Z",
					source: "google",
					medium: "cpc",
					campaign: "Brand Search",
					totalPrice: 700,
					matchSource: "checkout_token",
					confidenceLabel: "high",
					attributionReason: "last-touch",
				},
				{
					shopifyOrderId: "1200",
					processedAt: "2026-04-02T18:00:00.000Z",
					source: "meta",
					medium: "paid_social",
					campaign: "Prospecting Video",
					totalPrice: 500,
					matchSource: "ga4_fallback",
					confidenceLabel: "low",
					attributionReason: "linear",
				},
			],
			loading: false,
			error: null,
		},
		spendDetailsSection: {
			data: [
				{
					source: "google",
					medium: "cpc",
					channel: "google / cpc",
					subtotal: 350,
					campaigns: [
						{ campaign: "Brand Search", spend: 200 },
						{ campaign: "Non-Brand Search", spend: 150 },
					],
				},
				{
					source: "meta",
					medium: "paid_social",
					channel: "meta / paid_social",
					subtotal: 250,
					campaigns: [{ campaign: "Prospecting Video", spend: 250 }],
				},
			],
			loading: false,
			error: null,
		},
	});

	const summary = props.summarySection.data;
	const campaigns = props.campaignsSection.data ?? [];
	const points = props.timeseriesSection.data ?? [];
	const orders = props.ordersSection.data ?? [];
	const spendGroups = props.spendDetailsSection.data ?? [];

	assert.ok(summary);
	assert.equal(
		campaigns.reduce((sum, row) => sum + row.visits, 0),
		summary.visits,
	);
	assert.equal(
		campaigns.reduce((sum, row) => sum + row.orders, 0),
		summary.orders,
	);
	assert.equal(
		campaigns.reduce((sum, row) => sum + row.revenue, 0),
		summary.revenue,
	);
	assert.equal(
		points.reduce((sum, point) => sum + point.visits, 0),
		summary.visits,
	);
	assert.equal(
		points.reduce((sum, point) => sum + point.orders, 0),
		summary.orders,
	);
	assert.equal(
		points.reduce((sum, point) => sum + point.revenue, 0),
		summary.revenue,
	);
	assert.equal(
		orders.reduce((sum, row) => sum + row.totalPrice, 0),
		summary.revenue,
	);
	assert.equal(
		spendGroups.reduce((sum, group) => sum + group.subtotal, 0),
		summary.spend,
	);
	assert.equal(
		spendGroups.reduce(
			(sum, group) =>
				sum +
				group.campaigns.reduce(
					(groupSum, campaign) => groupSum + campaign.spend,
					0,
				),
			0,
		),
		summary.spend,
	);

	const mounted = await mountUi(h(ReportingDashboard, props), {
		width: 1440,
		height: 900,
	});

	try {
		const text = mounted.container.textContent ?? "";
		assert.match(text, new RegExp(formatNumber(summary.visits)));
		assert.match(text, new RegExp(formatNumber(summary.orders)));
		assert.match(text, new RegExp(`\\${formatCurrency(summary.revenue)}`));
		assert.match(text, new RegExp(`\\${formatCurrency(summary.spend)}`));
		assert.match(text, /Brand Search/);
		assert.match(text, /Prospecting Video/);
		assert.match(text, /Channel subtotal/);
	} finally {
		mounted.cleanup();
	}
});

test("reporting dashboard renders the bottom spend report grouped by channel then campaign", async () => {
	const { default: ReportingDashboard } = await loadDashboardModule<
		typeof import("../dashboard/src/components/ReportingDashboard")
	>("dashboard/src/components/ReportingDashboard.tsx");

	const mounted = await mountUi(
		h(ReportingDashboard, createReportingDashboardProps()),
	);

	try {
		const text = mounted.container.textContent ?? "";
		assert.match(text, /Marketing spend detail/);
		assert.match(text, /Google \/ Cpc/);
		assert.match(text, /Spring Search/);
		assert.match(text, /Channel subtotal/);
	} finally {
		mounted.cleanup();
	}
});

test("reporting dashboard shows top and lowest-performing bucket cards side by side with correct ranking labels", async () => {
	const { default: ReportingDashboard } = await loadDashboardModule<
		typeof import("../dashboard/src/components/ReportingDashboard")
	>("dashboard/src/components/ReportingDashboard.tsx");

	const mounted = await mountUi(
		h(
			ReportingDashboard,
			createReportingDashboardProps({
				timeseriesSection: {
					data: [
						{ date: "2026-04-16", visits: 410, orders: 14, revenue: 1800 },
						{ date: "2026-04-17", visits: 390, orders: 11, revenue: 920 },
						{ date: "2026-04-18", visits: 540, orders: 21, revenue: 3210 },
						{ date: "2026-04-19", visits: 610, orders: 24, revenue: 4020 },
						{ date: "2026-04-20", visits: 575, orders: 23, revenue: 3680 },
					],
					loading: false,
					error: null,
				},
			}),
		),
		{ width: 1440, height: 900 },
	);

	try {
		const text = mounted.container.textContent ?? "";
		assert.match(text, /Top buckets/);
		assert.match(text, /Lowest-performing buckets/);
		assert.match(text, /Highest revenue buckets in this view\./);
		assert.match(text, /Lowest revenue buckets in this view\./);
		assert.match(text, /Apr 19/);
		assert.match(text, /Apr 17/);

		const compactCards = Array.from(
			mounted.container.querySelectorAll<HTMLElement>("article"),
		).filter((card) => /\bp-3\b/.test(card.className));
		const topCard = compactCards.find(
			(card) =>
				/Top buckets/.test(card.textContent ?? "") &&
				/Highest revenue buckets in this view\./.test(card.textContent ?? ""),
		);
		const lowestCard = compactCards.find(
			(card) =>
				/Lowest-performing buckets/.test(card.textContent ?? "") &&
				/Lowest revenue buckets in this view\./.test(card.textContent ?? ""),
		);

		assert.ok(topCard);
		assert.ok(lowestCard);

		const topText = topCard?.textContent ?? "";
		const lowestText = lowestCard?.textContent ?? "";
		assert.ok(topText.indexOf("Apr 19") < topText.indexOf("Apr 20"));
		assert.ok(lowestText.indexOf("Apr 17") < lowestText.indexOf("Apr 16"));
	} finally {
		mounted.cleanup();
	}
});

test("order details empty state stays explicit when no drill-in selection is active", async () => {
	const { default: OrderDetailsView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/OrderDetailsView")
	>("dashboard/src/components/OrderDetailsView.tsx");

	const mounted = await mountUi(
		h(
			OrderDetailsView,
			createOrderDetailsProps({
				selectedOrderId: null,
				orderDetailsSection: {
					loading: false,
					error: null,
					data: null,
				},
			}),
		),
	);

	try {
		assert.match(mounted.container.textContent ?? "", /No order selected\./);
	} finally {
		mounted.cleanup();
	}
});

test("settings admin view keeps user management gated for non-admin access", async () => {
	const { default: SettingsAdminView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/SettingsAdminView")
	>("dashboard/src/components/SettingsAdminView.tsx");

	const mounted = await mountUi(
		h(SettingsAdminView, createSettingsAdminProps({ isAdmin: false })),
	);

	try {
		assert.match(mounted.container.textContent ?? "", /Settings operations/);
		assert.match(mounted.container.textContent ?? "", /Shopify connection/);
		assert.doesNotMatch(mounted.container.textContent ?? "", /User access/);
		assert.doesNotMatch(
			mounted.container.textContent ?? "",
			/Create app access/,
		);
	} finally {
		mounted.cleanup();
	}
});

test("settings admin view explains recovery actions in recommended operator order", async () => {
	const { default: SettingsAdminView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/SettingsAdminView")
	>("dashboard/src/components/SettingsAdminView.tsx");

	const mounted = await mountUi(
		h(SettingsAdminView, createSettingsAdminProps()),
	);

	try {
		const text = mounted.container.textContent ?? "";
		assert.match(text, /Recovery tools/);
		assert.match(
			text,
			/Use these in order for the selected date window: import Shopify orders first, recover attribution hints second, and queue the broader attribution backfill last/,
		);
		assert.match(text, /Import Shopify orders/);
		assert.match(
			text,
			/Run this first to import historical Shopify orders for the window/,
		);
		assert.match(text, /Recover attribution hints/);
		assert.match(
			text,
			/Run this second when imported Shopify web orders are still unattributed; it retries deterministic relinking/,
		);
		assert.match(text, /Select attribution backfill/);
		assert.match(
			text,
			/Run this last to queue the broader asynchronous attribution backfill for the same window; always do a dry run first/,
		);
	} finally {
		mounted.cleanup();
	}
});

test("settings admin view reveals attribution options with safe defaults only after selection", async () => {
	const { default: SettingsAdminView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/SettingsAdminView")
	>("dashboard/src/components/SettingsAdminView.tsx");

	const mounted = await mountUi(
		h(SettingsAdminView, createSettingsAdminProps()),
	);

	try {
		assert.doesNotMatch(
			mounted.container.textContent ?? "",
			/Attribution backfill options/,
		);

		const selectButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) =>
			button.textContent?.includes("Select attribution backfill"),
		);
		assert.ok(selectButton);
		click(selectButton);
		await tick();

		const text = mounted.container.textContent ?? "";
		assert.match(text, /Attribution backfill options/);
		assert.match(
			text,
			/Review these before queueing the asynchronous attribution backfill/,
		);
		assert.match(text, /Run a dry run first for this exact window/);
		assert.match(
			text,
			/Defaults to enabled\. Keep this on for the first run so the job analyzes the window without writing attribution changes/,
		);
		assert.match(
			text,
			/Defaults to enabled\. Keep the backfill focused on Shopify web orders/,
		);
		assert.match(
			text,
			/Defaults to off\. Turn this on only when you want local attribution updates without Shopify writeback/,
		);

		const limitInput = mounted.container.querySelector(
			"#shopify-order-attribution-limit",
		) as HTMLInputElement | null;
		const dryRunInput = mounted.container.querySelector(
			"#shopify-order-attribution-dry-run",
		) as HTMLInputElement | null;
		const webOnlyInput = mounted.container.querySelector(
			"#shopify-order-attribution-web-only",
		) as HTMLInputElement | null;
		const skipWritebackInput = mounted.container.querySelector(
			"#shopify-order-attribution-skip-writeback",
		) as HTMLInputElement | null;

		assert.ok(limitInput);
		assert.equal(limitInput.value, "500");
		assert.ok(dryRunInput);
		assert.equal(dryRunInput.checked, true);
		assert.ok(webOnlyInput);
		assert.equal(webOnlyInput.checked, true);
		assert.ok(skipWritebackInput);
		assert.equal(skipWritebackInput.checked, false);
	} finally {
		mounted.cleanup();
	}
});

test("settings admin view bypasses confirmation for dry-run attribution backfills", async () => {
	const { default: SettingsAdminView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/SettingsAdminView")
	>("dashboard/src/components/SettingsAdminView.tsx");

	let queuedCount = 0;
	const mounted = await mountUi(
		h(
			SettingsAdminView,
			createSettingsAdminProps({
				onShopifyOrderAttributionBackfill() {
					queuedCount += 1;
				},
			}),
		),
	);

	try {
		const selectButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) =>
			button.textContent?.includes("Select attribution backfill"),
		);
		assert.ok(selectButton);
		click(selectButton);
		await tick();

		const queueButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) =>
			button.textContent?.includes("Queue order attribution backfill"),
		);
		assert.ok(queueButton);
		click(queueButton);
		await tick();

		assert.equal(queuedCount, 1);
		assert.equal(mounted.dom.window.document.querySelector("dialog"), null);
	} finally {
		mounted.cleanup();
	}
});

test("settings admin view requires confirmation before non-dry-run attribution backfills", async () => {
	const { default: SettingsAdminView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/SettingsAdminView")
	>("dashboard/src/components/SettingsAdminView.tsx");

	let queuedCount = 0;
	const mounted = await mountUi(
		h(
			SettingsAdminView,
			createSettingsAdminProps({
				shopifyOrderAttributionBackfillOptions: {
					dryRun: false,
					limit: "500",
					webOrdersOnly: true,
					skipShopifyWriteback: false,
				},
				onShopifyOrderAttributionBackfill() {
					queuedCount += 1;
				},
			}),
		),
	);

	try {
		const selectButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) =>
			button.textContent?.includes("Select attribution backfill"),
		);
		assert.ok(selectButton);
		click(selectButton);
		await tick();

		const queueButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) =>
			button.textContent?.includes("Queue order attribution backfill"),
		);
		assert.ok(queueButton);
		click(queueButton);
		await tick();

		assert.equal(queuedCount, 0);
		assert.ok(mounted.dom.window.document.querySelector("dialog"));
		assert.match(
			mounted.container.textContent ?? "",
			/may also write updates back to Shopify/,
		);

		const confirmButton = Array.from(
			mounted.dom.window.document.querySelectorAll("button"),
		).find((button) => button.textContent?.includes("Yes, queue backfill"));
		assert.ok(confirmButton);
		click(confirmButton);
		await tick();

		assert.equal(queuedCount, 1);
		assert.equal(mounted.dom.window.document.querySelector("dialog"), null);
	} finally {
		mounted.cleanup();
	}
});

test("settings admin view renders queued, processing, completed, and failed backfill report states", async () => {
	const { default: SettingsAdminView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/SettingsAdminView")
	>("dashboard/src/components/SettingsAdminView.tsx");

	const mounted = await mountUi(
		h(
			SettingsAdminView,
			createSettingsAdminProps({
				orderAttributionBackfillJob: {
					data: {
						ok: true,
						jobId: "job-queued",
						status: "queued",
						submittedAt: "2026-04-20T19:10:00.000Z",
						submittedBy: "taylor@roasradar.dev",
						startedAt: null,
						completedAt: null,
						options: {
							startDate: "2026-04-01",
							endDate: "2026-04-20",
							dryRun: true,
							limit: 500,
							webOrdersOnly: true,
							skipShopifyWriteback: false,
						},
						report: null,
						error: null,
					},
					loading: false,
					error: null,
				},
			}),
		),
	);

	try {
		assert.match(
			mounted.container.textContent ?? "",
			/Latest attribution backfill run/,
		);
		assert.match(mounted.container.textContent ?? "", /Queued/);
		assert.match(
			mounted.container.textContent ?? "",
			/This backfill is queued and will update here once a worker starts it\./,
		);

		mounted.root.render(
			h(
				SettingsAdminView,
				createSettingsAdminProps({
					orderAttributionBackfillJob: {
						data: {
							ok: true,
							jobId: "job-processing",
							status: "processing",
							submittedAt: "2026-04-20T19:10:00.000Z",
							submittedBy: "taylor@roasradar.dev",
							startedAt: "2026-04-20T19:11:00.000Z",
							completedAt: null,
							options: {
								startDate: "2026-04-01",
								endDate: "2026-04-20",
								dryRun: true,
								limit: 500,
								webOrdersOnly: true,
								skipShopifyWriteback: false,
							},
							report: null,
							error: null,
						},
						loading: false,
						error: null,
					},
				}),
			),
		);
		await tick();

		assert.match(mounted.container.textContent ?? "", /Running/);
		assert.match(
			mounted.container.textContent ?? "",
			/This backfill is currently running\. The report will populate automatically when processing finishes\./,
		);

		mounted.root.render(
			h(
				SettingsAdminView,
				createSettingsAdminProps({
					orderAttributionBackfillJob: {
						data: {
							ok: true,
							jobId: "job-completed",
							status: "completed",
							submittedAt: "2026-04-20T19:10:00.000Z",
							submittedBy: "taylor@roasradar.dev",
							startedAt: "2026-04-20T19:11:00.000Z",
							completedAt: "2026-04-20T19:14:00.000Z",
							options: {
								startDate: "2026-04-01",
								endDate: "2026-04-20",
								dryRun: false,
								limit: 150,
								webOrdersOnly: false,
								skipShopifyWriteback: true,
							},
							report: {
								scanned: 150,
								recovered: 42,
								unrecoverable: 7,
								writebackCompleted: 0,
								failures: [
									{
										orderId: "1001",
										code: "missing-session",
										message: "Missing session evidence",
									},
									{
										orderId: "1002",
										code: "no-click-id",
										message: "No deterministic click id found",
									},
									{
										orderId: "1003",
										code: "writeback-skipped",
										message: "Writeback disabled",
									},
									{
										orderId: "1004",
										code: "late-order",
										message: "Order processed after range close",
									},
								],
							},
							error: null,
						},
						loading: false,
						error: null,
					},
				}),
			),
		);
		await tick();

		const completedText = mounted.container.textContent ?? "";
		assert.match(completedText, /Completed/);
		assert.match(completedText, /Web orders only/);
		assert.match(completedText, /No/);
		assert.match(completedText, /Shopify writeback/);
		assert.match(completedText, /Skipped/);
		assert.match(completedText, /Orders scanned/);
		assert.match(completedText, /150/);
		assert.match(completedText, /Recovered/);
		assert.match(completedText, /42/);
		assert.match(completedText, /Recent failures/);
		assert.match(
			completedText,
			/1001: missing-session \(Missing session evidence\)/,
		);
		assert.match(
			completedText,
			/1003: writeback-skipped \(Writeback disabled\)/,
		);
		assert.doesNotMatch(
			completedText,
			/1004: late-order \(Order processed after range close\)/,
		);

		mounted.root.render(
			h(
				SettingsAdminView,
				createSettingsAdminProps({
					orderAttributionBackfillJob: {
						data: {
							ok: true,
							jobId: "job-failed",
							status: "failed",
							submittedAt: "2026-04-20T19:10:00.000Z",
							submittedBy: "taylor@roasradar.dev",
							startedAt: "2026-04-20T19:11:00.000Z",
							completedAt: "2026-04-20T19:12:00.000Z",
							options: {
								startDate: "2026-04-01",
								endDate: "2026-04-20",
								dryRun: false,
								limit: 75,
								webOrdersOnly: true,
								skipShopifyWriteback: false,
							},
							report: {
								scanned: 23,
								recovered: 5,
								unrecoverable: 2,
								writebackCompleted: 1,
								failures: [
									{
										orderId: "2001",
										code: "worker-timeout",
										message: "Worker timed out",
									},
								],
							},
							error: {
								code: "job-failed",
								message: "Worker exited before finishing the backfill",
							},
						},
						loading: false,
						error: null,
					},
				}),
			),
		);
		await tick();

		const failedText = mounted.container.textContent ?? "";
		assert.match(failedText, /Failed/);
		assert.match(failedText, /Web orders only/);
		assert.match(failedText, /Yes/);
		assert.match(failedText, /Shopify writeback/);
		assert.match(failedText, /Allowed/);
		assert.match(
			failedText,
			/job-failed: Worker exited before finishing the backfill/,
		);
		assert.match(failedText, /Orders scanned/);
		assert.match(failedText, /23/);
	} finally {
		mounted.cleanup();
	}
});

test("settings admin view lets operators cancel non-dry-run attribution confirmation", async () => {
	const { default: SettingsAdminView } = await loadDashboardModule<
		typeof import("../dashboard/src/components/SettingsAdminView")
	>("dashboard/src/components/SettingsAdminView.tsx");

	let queuedCount = 0;
	const mounted = await mountUi(
		h(
			SettingsAdminView,
			createSettingsAdminProps({
				shopifyOrderAttributionBackfillOptions: {
					dryRun: false,
					limit: "500",
					webOrdersOnly: true,
					skipShopifyWriteback: true,
				},
				onShopifyOrderAttributionBackfill() {
					queuedCount += 1;
				},
			}),
		),
	);

	try {
		const selectButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) =>
			button.textContent?.includes("Select attribution backfill"),
		);
		assert.ok(selectButton);
		click(selectButton);
		await tick();

		const queueButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) =>
			button.textContent?.includes("Queue order attribution backfill"),
		);
		assert.ok(queueButton);
		click(queueButton);
		await tick();

		assert.ok(mounted.dom.window.document.querySelector("dialog"));
		assert.match(
			mounted.container.textContent ?? "",
			/Shopify writeback is disabled for this run/,
		);

		const cancelButton = Array.from(
			mounted.dom.window.document.querySelectorAll("button"),
		).find((button) => button.textContent?.includes("No, go back"));
		assert.ok(cancelButton);
		click(cancelButton);
		await tick();

		assert.equal(queuedCount, 0);
		assert.equal(mounted.dom.window.document.querySelector("dialog"), null);

		const restoredQueueButton = Array.from(
			mounted.container.querySelectorAll("button"),
		).find((button) =>
			button.textContent?.includes("Queue order attribution backfill"),
		);
		assert.ok(restoredQueueButton);
	} finally {
		mounted.cleanup();
	}
});
