import assert from "node:assert/strict";
import test from "node:test";

import {
	BAYESIAN_HIERARCHICAL_MMM_MODEL_VERSION,
	buildBayesianHierarchicalMmmArtifact,
} from "../src/modules/mmm/bayesian-hierarchical.js";
import type { BayesianHierarchicalMmmV1FeatureRow } from "../src/modules/mmm/weekly-mart.js";

function weeklyRow(input: {
	weekStartDate: string;
	weekEndDate: string;
	source: string;
	medium: string;
	campaign: string;
	channel: string;
	channelGroup: string;
	spend: number;
	revenue: number;
	attributionRevenue: number;
	dqStatus?: string;
}): BayesianHierarchicalMmmV1FeatureRow {
	return {
		input_contract_version: "bayesian_hierarchical_mmm_v1",
		week_start_date: input.weekStartDate,
		week_end_date: input.weekEndDate,
		mart_version: "mmm_weekly_channel_input_mart_v1",
		source_mart_version: "mmm_daily_input_mart_v1",
		attribution_model: "last_touch",
		channel_key: `${input.source}|${input.medium}|${input.campaign}|${input.channel}|${input.channelGroup}`,
		source: input.source,
		medium: input.medium,
		campaign: input.campaign,
		channel: input.channel,
		channel_group: input.channelGroup,
		spend: input.spend,
		impressions: input.spend * 100,
		clicks: input.spend * 3,
		shopify_orders: input.revenue / 100,
		shopify_revenue: input.revenue,
		attribution_credit_orders: input.attributionRevenue / 100,
		attribution_credit_revenue: input.attributionRevenue,
		new_customer_credit_orders: input.attributionRevenue / 200,
		returning_customer_credit_orders: input.attributionRevenue / 200,
		new_customer_credit_revenue: input.attributionRevenue / 2,
		returning_customer_credit_revenue: input.attributionRevenue / 2,
		match_source_coverage: { first_party: input.attributionRevenue / 100 },
		confidence_label_coverage: { high: input.attributionRevenue / 100 },
		controls: {},
		deterministic_anchors: {},
		missingness_report: { missingDimensions: [] },
		leakage_report: { hasFutureDatedSourceRows: false },
		dq_status: input.dqStatus ?? "pass",
		source_row_count: 7,
		generated_at: `${input.weekEndDate}T00:00:00.000Z`,
	};
}

function fixtureRows(): BayesianHierarchicalMmmV1FeatureRow[] {
	const weeks = [
		["2026-04-06", "2026-04-12"],
		["2026-04-13", "2026-04-19"],
		["2026-04-20", "2026-04-26"],
		["2026-04-27", "2026-05-03"],
		["2026-05-04", "2026-05-10"],
		["2026-05-11", "2026-05-17"],
	] as const;

	return weeks.flatMap(([weekStartDate, weekEndDate], index) => [
		weeklyRow({
			weekStartDate,
			weekEndDate,
			source: "meta",
			medium: "paid_social",
			campaign: "prospecting",
			channel: "paid_social",
			channelGroup: "paid",
			spend: 100 + index * 20,
			revenue: 650 + index * 85,
			attributionRevenue: 220 + index * 30,
		}),
		weeklyRow({
			weekStartDate,
			weekEndDate,
			source: "google",
			medium: "cpc",
			campaign: "brand",
			channel: "paid_search",
			channelGroup: "paid",
			spend: 70 + index * 10,
			revenue: 650 + index * 85,
			attributionRevenue: 180 + index * 20,
		}),
	]);
}

test("Bayesian hierarchical MMM builds v1 posterior artifact with priors and transformed media", () => {
	const run = buildBayesianHierarchicalMmmArtifact(fixtureRows(), {
		startDate: "2026-04-06",
		endDate: "2026-05-17",
		attributionModel: "last_touch",
		adstockDecay: 0.4,
		saturationHalfSaturation: 250,
		saturationSlope: 1.2,
		posteriorChains: 2,
		posteriorDraws: 200,
		holdoutRatio: 0,
	});

	assert.equal(run.modelVersion, BAYESIAN_HIERARCHICAL_MMM_MODEL_VERSION);
	assert.equal(run.modelType, "bayesian_hierarchical_mmm");
	assert.equal(run.martVersion, "mmm_weekly_channel_input_mart_v1");
	assert.equal(
		run.runConfig.inputContractVersion,
		"bayesian_hierarchical_mmm_v1",
	);
	assert.deepEqual(run.runConfig.featureTransform, {
		adstock: { type: "geometric", decay: 0.4 },
		saturation: { type: "hill", halfSaturation: 250, slope: 1.2 },
	});
	assert.equal(
		(run.runConfig.priors as { mediaEffect: { distribution: string } })
			.mediaEffect.distribution,
		"normal",
	);
	assert.equal(run.inputSummary.observationCount, 6);
	assert.deepEqual(run.inputSummary.selectedChannels, [
		"meta|paid_social|prospecting",
		"google|cpc|brand",
	]);
	assert.equal(run.validationReport.posteriorSanityChecks.status, "pass");
	assert.equal(run.validationReport.posteriorDiagnostics.totalDraws, 200);
	assert.ok(run.modelArtifact.posteriorCoefficients);
	assert.ok(run.modelArtifact.contributionOutputs);

	const contributionOutputs = run.modelArtifact.contributionOutputs as {
		channels: Array<{
			key: string;
			contribution: { credibleInterval95: { lower: number; upper: number } };
		}>;
	};
	assert.deepEqual(
		contributionOutputs.channels.map((channel) => channel.key),
		["meta|paid_social|prospecting", "google|cpc|brand", "__other_paid__"],
	);
	assert.ok(
		Number.isFinite(
			contributionOutputs.channels[0]?.contribution.credibleInterval95.upper,
		),
	);

	const coefficients = run.modelArtifact.coefficients as Record<string, number>;
	assert.ok("trend" in coefficients);
	assert.ok("seasonalitySin52" in coefficients);
	assert.ok("seasonalityCos52" in coefficients);
	assert.equal(
		run.calibrationReport.deterministicAttributionUsage,
		"hierarchical_priors_and_calibration_diagnostics",
	);
});

test("Bayesian hierarchical MMM rejects failed weekly mart rows", () => {
	const rows = fixtureRows();
	rows[0] = { ...rows[0], dq_status: "fail" };

	assert.throws(
		() =>
			buildBayesianHierarchicalMmmArtifact(rows, {
				startDate: "2026-04-06",
				endDate: "2026-05-17",
			}),
		/input contains failed quality rows: 1/,
	);
});
