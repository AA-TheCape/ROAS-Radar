import assert from "node:assert/strict";
import test from "node:test";

import {
	buildBaselineCalibrationFreeze,
	buildBaselineMmmArtifact,
} from "../src/modules/mmm/baseline.js";

test("MMM baseline uses attribution metrics as calibration diagnostics instead of segment labels", () => {
	const rows = [
		{
			metric_date: "2026-04-01",
			mart_row_type: "paid_media" as const,
			attribution_model: "none",
			platform: "meta",
			source: "meta",
			medium: "paid_social",
			campaign: "prospecting",
			spend: "100",
			impressions: "1000",
			clicks: "50",
			shopify_revenue: "0",
			attribution_credit_revenue: "0",
			attribution_credit_orders: "0",
			match_source_coverage: {},
			confidence_label_coverage: {},
		},
		{
			metric_date: "2026-04-02",
			mart_row_type: "paid_media" as const,
			attribution_model: "none",
			platform: "meta",
			source: "meta",
			medium: "paid_social",
			campaign: "prospecting",
			spend: "150",
			impressions: "1200",
			clicks: "60",
			shopify_revenue: "0",
			attribution_credit_revenue: "0",
			attribution_credit_orders: "0",
			match_source_coverage: {},
			confidence_label_coverage: {},
		},
		{
			metric_date: "2026-04-03",
			mart_row_type: "paid_media" as const,
			attribution_model: "none",
			platform: "google",
			source: "google",
			medium: "cpc",
			campaign: "brand",
			spend: "80",
			impressions: "900",
			clicks: "70",
			shopify_revenue: "0",
			attribution_credit_revenue: "0",
			attribution_credit_orders: "0",
			match_source_coverage: {},
			confidence_label_coverage: {},
		},
		{
			metric_date: "2026-04-01",
			mart_row_type: "attribution" as const,
			attribution_model: "last_touch",
			platform: "taxonomy",
			source: "meta",
			medium: "paid_social",
			campaign: "prospecting",
			spend: "0",
			impressions: "0",
			clicks: "0",
			shopify_revenue: "300",
			attribution_credit_revenue: "300",
			attribution_credit_orders: "3",
			match_source_coverage: { first_party: 3 },
			confidence_label_coverage: { high: 3 },
		},
		{
			metric_date: "2026-04-02",
			mart_row_type: "attribution" as const,
			attribution_model: "last_touch",
			platform: "taxonomy",
			source: "meta",
			medium: "paid_social",
			campaign: "prospecting",
			spend: "0",
			impressions: "0",
			clicks: "0",
			shopify_revenue: "450",
			attribution_credit_revenue: "450",
			attribution_credit_orders: "4.5",
			match_source_coverage: { first_party: 4.5 },
			confidence_label_coverage: { high: 4.5 },
		},
		{
			metric_date: "2026-04-03",
			mart_row_type: "attribution" as const,
			attribution_model: "last_touch",
			platform: "taxonomy",
			source: "google",
			medium: "cpc",
			campaign: "brand",
			spend: "0",
			impressions: "0",
			clicks: "0",
			shopify_revenue: "240",
			attribution_credit_revenue: "240",
			attribution_credit_orders: "2.4",
			match_source_coverage: { first_party: 2.4 },
			confidence_label_coverage: { high: 2.4 },
		},
	];

	const run = buildBaselineMmmArtifact(rows, {
		startDate: "2026-04-01",
		endDate: "2026-04-03",
		attributionModel: "last_touch",
		adstockDecay: 0,
		posteriorChains: 2,
		posteriorDraws: 200,
		holdoutRatio: 0,
	});

	assert.equal(run.modelVersion, "baseline_linear_mmm_v1");
	assert.equal(run.approvedFreezeId, null);
	assert.equal(
		run.runConfig.bayesianEngine,
		"closed_form_linear_gaussian_posterior_v1",
	);
	assert.equal(
		run.runConfig.responseVariable,
		"daily_total_shopify_revenue_from_mart_outcomes",
	);
	assert.equal(
		run.calibrationReport.deterministicAttributionUsage,
		"calibration_and_validation_segments_only",
	);
	assert.equal(
		run.calibrationReport.reportVersion,
		"mmm_calibration_report_v1",
	);
	assert.deepEqual(run.calibrationReport.deterministicBaseline, {
		version: "mmm_deterministic_baseline_30d_click_7d_view_v1",
		clickLookbackWindowDays: 30,
		viewLookbackWindowDays: 7,
		lookbackRules: ["30d_click", "7d_view"],
		productionAlignment: "enforced",
	});
	assert.ok(run.calibrationReport.governance);
	assert.equal(run.validationReport.train.observationCount, 3);
	assert.equal(run.validationReport.posteriorSanityChecks.status, "pass");
	assert.equal(run.validationReport.posteriorDiagnostics.chains, 2);
	assert.equal(run.validationReport.posteriorDiagnostics.totalDraws, 200);
	assert.deepEqual(run.inputSummary.selectedSegments, [
		"meta|paid_social|prospecting",
		"google|cpc|brand",
	]);
	assert.ok(run.modelArtifact.posteriorCoefficients);
	assert.ok(run.modelArtifact.contributionOutputs);

	const calibrationSegments = run.calibrationReport.segments as Array<{
		key: string;
		attributedRevenue: number;
		deterministicContributionShare: number | null;
		posteriorContributionShare: number | null;
		productionContributionShare: number | null;
		trustWeights: {
			deterministicBaseline: number;
			posteriorCalibration: number;
			production: number;
		};
	}>;
	assert.deepEqual(
		calibrationSegments.map((segment) => [
			segment.key,
			segment.attributedRevenue,
		]),
		[
			["meta|paid_social|prospecting", 750],
			["google|cpc|brand", 240],
		],
	);
	assert.equal(
		calibrationSegments[0]?.productionContributionShare,
		calibrationSegments[0]?.deterministicContributionShare,
	);
	assert.equal(calibrationSegments[0]?.trustWeights.deterministicBaseline, 1);
	assert.equal(calibrationSegments[0]?.trustWeights.production, 1);
	assert.equal(
		typeof calibrationSegments[0]?.trustWeights.posteriorCalibration,
		"number",
	);

	const contributionOutputs = run.modelArtifact.contributionOutputs as {
		channels: Array<{
			key: string;
			contribution: { credibleInterval95: { lower: number; upper: number } };
			contributionShare: { mean: number };
		}>;
	};
	assert.deepEqual(
		contributionOutputs.channels.map((channel) => channel.key),
		["meta|paid_social|prospecting", "google|cpc|brand", "__other_paid__"],
	);
	assert.ok(
		contributionOutputs.channels[0]?.contribution.credibleInterval95.upper,
	);
	assert.ok(contributionOutputs.channels[0]?.contributionShare.mean);

	const governance = run.calibrationReport.governance as {
		status: string;
		thresholds: { warnDivergenceRate: number; alertDivergenceRate: number };
		reconciliationLogic: string;
		rowCount: number;
		channelWeekReconciliation: Array<{
			weekStartDate: string;
			key: string;
			governanceTier: string;
		}>;
	};
	assert.equal(governance.thresholds.warnDivergenceRate, 0.25);
	assert.equal(governance.thresholds.alertDivergenceRate, 0.5);
	assert.equal(governance.rowCount, 9);
	assert.ok(
		governance.reconciliationLogic.includes(
			"deterministic attribution_credit_revenue",
		),
	);
	assert.deepEqual(
		governance.channelWeekReconciliation
			.slice(0, 3)
			.map((row) => [row.weekStartDate, row.key]),
		[
			["2026-04-01", "meta|paid_social|prospecting"],
			["2026-04-01", "google|cpc|brand"],
			["2026-04-01", "__other_paid__"],
		],
	);

	const strictRun = buildBaselineMmmArtifact(rows, {
		startDate: "2026-04-01",
		endDate: "2026-04-03",
		attributionModel: "last_touch",
		adstockDecay: 0,
		posteriorChains: 2,
		posteriorDraws: 200,
		holdoutRatio: 0,
		calibrationWarnDivergenceRate: 0,
		calibrationAlertDivergenceRate: 0,
	});
	const strictGovernance = strictRun.calibrationReport.governance as {
		status: string;
		alertCount: number;
	};
	assert.equal(strictGovernance.status, "alert");
	assert.ok(strictGovernance.alertCount > 0);
});

test("MMM baseline freeze persists deterministic calibration evidence and stable hash", () => {
	const rows = [
		{
			week_start_date: "2026-04-06",
			week_end_date: "2026-04-12",
			mart_version: "mmm_weekly_channel_input_mart_v1",
			source_mart_version: "mmm_daily_input_mart_v1",
			attribution_model: "last_touch",
			channel_key: "meta|paid_social|prospecting|paid_social|paid",
			source: "meta",
			medium: "paid_social",
			campaign: "prospecting",
			channel: "paid_social",
			channel_group: "paid",
			spend: "100",
			impressions: "1000",
			clicks: "50",
			shopify_orders: "4",
			shopify_revenue: "400",
			attribution_credit_orders: "3.5",
			attribution_credit_revenue: "350",
			new_customer_credit_orders: "2",
			returning_customer_credit_orders: "1.5",
			new_customer_credit_revenue: "225",
			returning_customer_credit_revenue: "125",
			match_source_coverage: { first_party: 3.5 },
			confidence_label_coverage: { high: 3.5 },
			controls: {},
			deterministic_anchors: {},
			missingness_report: { missingDimensions: [] },
			leakage_report: { hasFutureDatedSourceRows: false },
			dq_status: "pass",
			source_row_count: "7",
			generated_at: "2026-04-13T00:00:00.000Z",
		},
		{
			week_start_date: "2026-04-13",
			week_end_date: "2026-04-19",
			mart_version: "mmm_weekly_channel_input_mart_v1",
			source_mart_version: "mmm_daily_input_mart_v1",
			attribution_model: "last_touch",
			channel_key: "unknown|cpc|brand|unknown|unknown",
			source: "unknown",
			medium: "cpc",
			campaign: "brand",
			channel: "unknown",
			channel_group: "unknown",
			spend: "80",
			impressions: "0",
			clicks: "0",
			shopify_orders: "1",
			shopify_revenue: "90",
			attribution_credit_orders: "0",
			attribution_credit_revenue: "0",
			new_customer_credit_orders: "0",
			returning_customer_credit_orders: "0",
			new_customer_credit_revenue: "0",
			returning_customer_credit_revenue: "0",
			match_source_coverage: {},
			confidence_label_coverage: {},
			controls: {},
			deterministic_anchors: {},
			missingness_report: { missingDimensions: ["source", "channel"] },
			leakage_report: { hasFutureDatedSourceRows: false },
			dq_status: "warn",
			source_row_count: "3",
			generated_at: "2026-04-20T00:00:00.000Z",
		},
	];
	const input = {
		startDate: "2026-04-06",
		endDate: "2026-04-19",
		attributionModel: "last_touch",
		qualitySummary: {
			rowCount: 2,
			failCount: 0,
			warnCount: 1,
			unknownDimensionRowCount: 1,
			futureDatedSourceRowCount: 0,
		},
		generationTimestamp: "2026-04-20T01:00:00.000Z",
		freezeStatus: "approved" as const,
		createdBy: "ops",
		approvedBy: "lead",
		approvedAt: "2026-04-20T01:00:00.000Z",
	};

	const freeze = buildBaselineCalibrationFreeze(rows, input);
	const secondFreeze = buildBaselineCalibrationFreeze(rows, input);

	assert.equal(
		freeze.freezeSchemaVersion,
		"mmm_baseline_calibration_freeze_v1",
	);
	assert.equal(freeze.snapshotVersion, "mmm_weekly_channel_snapshot_v1");
	assert.equal(freeze.rowCounts.rowCount, 2);
	assert.equal(freeze.rowCounts.warnCount, 1);
	assert.equal(freeze.deterministicAttributionCoverage.attributedRowCount, 1);
	assert.equal(freeze.campaignMetadataCoverage.unknownDimensionRowCount, 1);
	assert.equal(freeze.exposureCoverage.rowsWithSpend, 2);
	assert.equal(freeze.dataQualityChecks.status, "warn");
	assert.equal(freeze.aggregateMetricTotals.spend, 180);
	assert.equal(freeze.aggregateMetricTotals.attributionCreditRevenue, 350);
	assert.equal(freeze.evidenceHash, secondFreeze.evidenceHash);
	assert.match(freeze.evidenceHash, /^[0-9a-f]{64}$/);
});

test("MMM baseline rejects insufficient mart observations", () => {
	assert.throws(
		() =>
			buildBaselineMmmArtifact(
				[
					{
						metric_date: "2026-04-01",
						mart_row_type: "attribution" as const,
						attribution_model: "last_touch",
						platform: "taxonomy",
						source: "meta",
						medium: "paid_social",
						campaign: "prospecting",
						spend: "0",
						impressions: "0",
						clicks: "0",
						shopify_revenue: "300",
						attribution_credit_revenue: "300",
						attribution_credit_orders: "3",
						match_source_coverage: {},
						confidence_label_coverage: {},
					},
				],
				{
					startDate: "2026-04-01",
					endDate: "2026-04-01",
				},
			),
		/requires at least 3 daily observations/,
	);
});
