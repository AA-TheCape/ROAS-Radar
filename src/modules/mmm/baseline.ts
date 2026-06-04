import { createHash } from "node:crypto";

import type { PoolClient } from "pg";

import { withTransaction } from "../../db/pool.js";
import {
	MMM_WEEKLY_CHANNEL_MART_VERSION,
	fetchWeeklyMmmSnapshotRowsWithClient,
	refreshWeeklyMmmChannelInputMartWithClient,
	snapshotWeeklyMmmInputRowsWithClient,
} from "./weekly-mart.js";

export const MMM_BASELINE_MODEL_VERSION = "baseline_linear_mmm_v1";
export const MMM_BASELINE_MODEL_TYPE = "baseline_linear_mmm";
export const MMM_BASELINE_MART_VERSION = MMM_WEEKLY_CHANNEL_MART_VERSION;

const DEFAULT_MAX_SEGMENTS = 8;
const DEFAULT_ADSTOCK_DECAY = 0.5;
const DEFAULT_RIDGE_LAMBDA = 1;
const DEFAULT_POSTERIOR_DRAWS = 1_000;
const DEFAULT_POSTERIOR_CHAINS = 4;
const MIN_OBSERVATIONS = 3;
const MIN_EFFECTIVE_SAMPLE_SIZE = 100;
const MAX_RHAT = 1.1;
const DEFAULT_CALIBRATION_WARN_DIVERGENCE_RATE = 0.25;
const DEFAULT_CALIBRATION_ALERT_DIVERGENCE_RATE = 0.5;

export type MmmBaselineTrainingInput = {
	startDate: string;
	endDate: string;
	approvedFreezeId?: string;
	attributionModel?: string;
	maxSegments?: number;
	adstockDecay?: number;
	ridgeLambda?: number;
	posteriorDraws?: number;
	posteriorChains?: number;
	holdoutRatio?: number;
	calibrationWarnDivergenceRate?: number;
	calibrationAlertDivergenceRate?: number;
	submittedBy?: string;
};

export type MmmBaselineFreezeInput = {
	startDate: string;
	endDate: string;
	attributionModel?: string;
	freezeStatus?: "pending" | "approved" | "rejected";
	submittedBy?: string;
	approvedBy?: string;
};

type MmmBaselineMartRow = {
	metric_date: string;
	mart_row_type: "paid_media" | "attribution" | "weekly_channel";
	attribution_model: string;
	platform: string;
	source: string;
	medium: string;
	campaign: string;
	spend: string | number;
	impressions: string | number;
	clicks: string | number;
	shopify_revenue: string | number;
	attribution_credit_revenue: string | number;
	attribution_credit_orders: string | number;
	match_source_coverage: unknown;
	confidence_label_coverage: unknown;
};

type SegmentStats = {
	key: string;
	source: string;
	medium: string;
	campaign: string;
	spend: number;
	impressions: number;
	clicks: number;
	attributedRevenue: number;
	attributedOrders: number;
};

type DailyObservation = {
	date: string;
	revenue: number;
	features: Record<string, number>;
};

type CalibrationGovernanceThresholds = {
	warnDivergenceRate: number;
	alertDivergenceRate: number;
};

type FittedRegression = {
	intercept: number;
	coefficients: Record<string, number>;
	covariance: number[][];
	residualSigma: number;
	parameterNames: string[];
};

export type MmmBaselineModelRun = {
	id: string | null;
	approvedFreezeId: string | null;
	modelType: typeof MMM_BASELINE_MODEL_TYPE;
	modelVersion: typeof MMM_BASELINE_MODEL_VERSION;
	martVersion: typeof MMM_BASELINE_MART_VERSION;
	attributionModel: string;
	trainingStartDate: string;
	trainingEndDate: string;
	holdoutStartDate: string | null;
	holdoutEndDate: string | null;
	runConfig: Record<string, unknown>;
	inputSummary: Record<string, unknown>;
	modelArtifact: Record<string, unknown>;
	calibrationReport: Record<string, unknown>;
	validationReport: Record<string, unknown>;
};

export type MmmBaselineCalibrationFreeze = {
	id: string | null;
	freezeSchemaVersion: "mmm_baseline_calibration_freeze_v1";
	martVersion: typeof MMM_BASELINE_MART_VERSION;
	snapshotVersion: "mmm_weekly_channel_snapshot_v1";
	freezeStatus: "pending" | "approved" | "rejected";
	generationTimestamp: string;
	calibrationStartDate: string;
	calibrationEndDate: string;
	attributionModel: string;
	rowCounts: Record<string, unknown>;
	deterministicAttributionCoverage: Record<string, unknown>;
	freshnessMetrics: Record<string, unknown>;
	campaignMetadataCoverage: Record<string, unknown>;
	exposureCoverage: Record<string, unknown>;
	dataQualityChecks: Record<string, unknown>;
	aggregateMetricTotals: Record<string, unknown>;
	evidenceHash: string;
	snapshotRows: WeeklyFreezeRow[];
	createdBy: string;
	approvedBy: string | null;
	approvedAt: string | null;
};

type WeeklyFreezeRow = Awaited<
	ReturnType<typeof fetchWeeklyMmmSnapshotRowsWithClient>
>[number];

function toNumber(value: string | number | null | undefined): number {
	if (value === null || value === undefined) {
		return 0;
	}

	const numeric = Number(value);
	return Number.isFinite(numeric) ? numeric : 0;
}

function normalizeDate(value: string, fieldName: string): string {
	const trimmed = value.trim();
	if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
		throw new Error(`${fieldName} must use YYYY-MM-DD format`);
	}

	return trimmed;
}

function clampInteger(
	value: number | undefined,
	fallback: number,
	min: number,
	max: number,
): number {
	const numeric = Number(value ?? fallback);
	if (!Number.isFinite(numeric)) {
		return fallback;
	}

	return Math.min(max, Math.max(min, Math.trunc(numeric)));
}

function clampNumber(
	value: number | undefined,
	fallback: number,
	min: number,
	max: number,
): number {
	const numeric = Number(value ?? fallback);
	if (!Number.isFinite(numeric)) {
		return fallback;
	}

	return Math.min(max, Math.max(min, numeric));
}

function stableStringify(value: unknown): string {
	if (Array.isArray(value)) {
		return `[${value.map((entry) => stableStringify(entry)).join(",")}]`;
	}

	if (value && typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>).sort(
			([left], [right]) => left.localeCompare(right),
		);
		return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`).join(",")}}`;
	}

	return JSON.stringify(value);
}

function hashJson(value: unknown): string {
	return createHash("sha256").update(stableStringify(value)).digest("hex");
}

function normalizeFreezeId(value: string | undefined | null): string | null {
	const trimmed = value?.trim();
	if (!trimmed) {
		return null;
	}

	if (
		!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
			trimmed,
		)
	) {
		throw new Error("approvedFreezeId must be a valid UUID");
	}

	return trimmed;
}

function countNonEmptyObjectCoverage(
	rows: WeeklyFreezeRow[],
	field: "match_source_coverage" | "confidence_label_coverage",
) {
	const nonEmptyRowCount = rows.filter((row) => {
		const value = row[field];
		return (
			value &&
			typeof value === "object" &&
			!Array.isArray(value) &&
			Object.keys(value as Record<string, unknown>).length > 0
		);
	}).length;

	return {
		nonEmptyRowCount,
		coverageRate: rows.length > 0 ? nonEmptyRowCount / rows.length : 0,
	};
}

function buildMmmBaselineFreezeSnapshot(input: {
	startDate: string;
	endDate: string;
	attributionModel: string;
	rows: WeeklyFreezeRow[];
	qualitySummary: Record<string, unknown>;
	generationTimestamp: string;
	freezeStatus: MmmBaselineCalibrationFreeze["freezeStatus"];
	createdBy: string;
	approvedBy: string | null;
	approvedAt: string | null;
}): MmmBaselineCalibrationFreeze {
	const rows = input.rows;
	const rowCounts = {
		rowCount: rows.length,
		paidMediaRowCount: rows.filter((row) => toNumber(row.spend) > 0).length,
		attributionRowCount: rows.filter(
			(row) =>
				toNumber(row.shopify_revenue) > 0 ||
				toNumber(row.attribution_credit_revenue) > 0,
		).length,
		passCount: rows.filter((row) => row.dq_status === "pass").length,
		warnCount: rows.filter((row) => row.dq_status === "warn").length,
		failCount: rows.filter((row) => row.dq_status === "fail").length,
	};
	const sourceRowCount = rows.reduce(
		(sum, row) => sum + toNumber(row.source_row_count),
		0,
	);
	const attributedRows = rows.filter(
		(row) =>
			toNumber(row.attribution_credit_revenue) > 0 ||
			toNumber(row.attribution_credit_orders) > 0,
	);
	const generatedTimes = rows
		.map((row) => new Date(row.generated_at).getTime())
		.filter((value) => Number.isFinite(value))
		.sort((left, right) => left - right);
	const generatedAtLatest = generatedTimes.at(-1);
	const generationTime = new Date(input.generationTimestamp).getTime();
	const unknownDimensionRows = rows.filter(
		(row) =>
			row.source === "unknown" ||
			row.medium === "unknown" ||
			row.campaign === "unknown" ||
			row.channel === "unknown",
	).length;
	const rowsWithDelivery = rows.filter(
		(row) => toNumber(row.impressions) > 0 || toNumber(row.clicks) > 0,
	).length;
	const rowsWithSpend = rows.filter((row) => toNumber(row.spend) > 0).length;
	const totals = {
		spend: rows.reduce((sum, row) => sum + toNumber(row.spend), 0),
		impressions: rows.reduce((sum, row) => sum + toNumber(row.impressions), 0),
		clicks: rows.reduce((sum, row) => sum + toNumber(row.clicks), 0),
		shopifyOrders: rows.reduce(
			(sum, row) => sum + toNumber(row.shopify_orders),
			0,
		),
		shopifyRevenue: rows.reduce(
			(sum, row) => sum + toNumber(row.shopify_revenue),
			0,
		),
		attributionCreditOrders: rows.reduce(
			(sum, row) => sum + toNumber(row.attribution_credit_orders),
			0,
		),
		attributionCreditRevenue: rows.reduce(
			(sum, row) => sum + toNumber(row.attribution_credit_revenue),
			0,
		),
		newCustomerCreditRevenue: rows.reduce(
			(sum, row) => sum + toNumber(row.new_customer_credit_revenue),
			0,
		),
		returningCustomerCreditRevenue: rows.reduce(
			(sum, row) => sum + toNumber(row.returning_customer_credit_revenue),
			0,
		),
	};
	const deterministicAttributionCoverage = {
		attributedRowCount: attributedRows.length,
		attributedRowCoverageRate:
			rows.length > 0 ? attributedRows.length / rows.length : 0,
		matchSourceCoverage: countNonEmptyObjectCoverage(
			rows,
			"match_source_coverage",
		),
		confidenceLabelCoverage: countNonEmptyObjectCoverage(
			rows,
			"confidence_label_coverage",
		),
		attributionCreditRevenueCoverageRate:
			totals.shopifyRevenue > 0
				? totals.attributionCreditRevenue / totals.shopifyRevenue
				: null,
	};
	const freshnessMetrics = {
		generationTimestamp: input.generationTimestamp,
		minSourceGeneratedAt: generatedTimes[0]
			? new Date(generatedTimes[0]).toISOString()
			: null,
		maxSourceGeneratedAt: generatedAtLatest
			? new Date(generatedAtLatest).toISOString()
			: null,
		sourceGeneratedAtLagHours:
			generatedAtLatest && Number.isFinite(generationTime)
				? (generationTime - generatedAtLatest) / (60 * 60 * 1000)
				: null,
		maxWeekEndDate:
			rows
				.map((row) => row.week_end_date)
				.sort()
				.at(-1) ?? null,
	};
	const campaignMetadataCoverage = {
		rowCount: rows.length,
		sourceCoverageRate:
			rows.length > 0
				? rows.filter((row) => row.source !== "unknown").length / rows.length
				: 0,
		mediumCoverageRate:
			rows.length > 0
				? rows.filter((row) => row.medium !== "unknown").length / rows.length
				: 0,
		campaignCoverageRate:
			rows.length > 0
				? rows.filter((row) => row.campaign !== "unknown").length / rows.length
				: 0,
		channelCoverageRate:
			rows.length > 0
				? rows.filter((row) => row.channel !== "unknown").length / rows.length
				: 0,
		unknownDimensionRowCount: unknownDimensionRows,
	};
	const exposureCoverage = {
		rowCount: rows.length,
		rowsWithSpend,
		rowsWithDelivery,
		spendWithDeliveryCoverageRate:
			rowsWithSpend > 0
				? rows.filter(
						(row) =>
							toNumber(row.spend) > 0 &&
							(toNumber(row.impressions) > 0 || toNumber(row.clicks) > 0),
					).length / rowsWithSpend
				: null,
		impressionCoverageRate:
			rows.length > 0
				? rows.filter((row) => toNumber(row.impressions) > 0).length /
					rows.length
				: 0,
		clickCoverageRate:
			rows.length > 0
				? rows.filter((row) => toNumber(row.clicks) > 0).length / rows.length
				: 0,
	};
	const dataQualityChecks = {
		...input.qualitySummary,
		rowCounts,
		status:
			rowCounts.failCount > 0
				? "fail"
				: rowCounts.warnCount > 0
					? "warn"
					: "pass",
	};
	const aggregateMetricTotals = {
		...totals,
		sourceRowCount,
	};
	const evidence = {
		freezeSchemaVersion: "mmm_baseline_calibration_freeze_v1",
		martVersion: MMM_BASELINE_MART_VERSION,
		snapshotVersion: "mmm_weekly_channel_snapshot_v1",
		calibrationStartDate: input.startDate,
		calibrationEndDate: input.endDate,
		attributionModel: input.attributionModel,
		rowCounts,
		deterministicAttributionCoverage,
		freshnessMetrics,
		campaignMetadataCoverage,
		exposureCoverage,
		dataQualityChecks,
		aggregateMetricTotals,
		snapshotRows: rows,
	};

	return {
		id: null,
		freezeSchemaVersion: "mmm_baseline_calibration_freeze_v1",
		martVersion: MMM_BASELINE_MART_VERSION,
		snapshotVersion: "mmm_weekly_channel_snapshot_v1",
		freezeStatus: input.freezeStatus,
		generationTimestamp: input.generationTimestamp,
		calibrationStartDate: input.startDate,
		calibrationEndDate: input.endDate,
		attributionModel: input.attributionModel,
		rowCounts,
		deterministicAttributionCoverage,
		freshnessMetrics,
		campaignMetadataCoverage,
		exposureCoverage,
		dataQualityChecks,
		aggregateMetricTotals,
		evidenceHash: hashJson(evidence),
		snapshotRows: rows,
		createdBy: input.createdBy,
		approvedBy: input.approvedBy,
		approvedAt: input.approvedAt,
	};
}

function segmentKey(
	row: Pick<MmmBaselineMartRow, "source" | "medium" | "campaign">,
): string {
	return [
		row.source || "unknown",
		row.medium || "unknown",
		row.campaign || "unknown",
	].join("|");
}

function addSegmentMetric(
	target: Map<string, SegmentStats>,
	key: string,
	row: MmmBaselineMartRow,
): SegmentStats {
	const existing = target.get(key);
	if (existing) {
		return existing;
	}

	const created = {
		key,
		source: row.source || "unknown",
		medium: row.medium || "unknown",
		campaign: row.campaign || "unknown",
		spend: 0,
		impressions: 0,
		clicks: 0,
		attributedRevenue: 0,
		attributedOrders: 0,
	};
	target.set(key, created);
	return created;
}

function applyAdstock(values: number[], decay: number): number[] {
	const transformed: number[] = [];
	let carry = 0;

	for (const value of values) {
		carry = value + carry * decay;
		transformed.push(Math.log1p(carry));
	}

	return transformed;
}

function solveLinearSystem(matrix: number[][], vector: number[]): number[] {
	const size = vector.length;
	const augmented = matrix.map((row, index) => [...row, vector[index] ?? 0]);

	for (let column = 0; column < size; column += 1) {
		let pivotRow = column;
		for (let row = column + 1; row < size; row += 1) {
			if (
				Math.abs(augmented[row][column] ?? 0) >
				Math.abs(augmented[pivotRow][column] ?? 0)
			) {
				pivotRow = row;
			}
		}

		[augmented[column], augmented[pivotRow]] = [
			augmented[pivotRow],
			augmented[column],
		];

		const pivot = augmented[column][column] ?? 0;
		if (Math.abs(pivot) < 1e-12) {
			continue;
		}

		for (let entry = column; entry <= size; entry += 1) {
			augmented[column][entry] = (augmented[column][entry] ?? 0) / pivot;
		}

		for (let row = 0; row < size; row += 1) {
			if (row === column) {
				continue;
			}

			const factor = augmented[row][column] ?? 0;
			for (let entry = column; entry <= size; entry += 1) {
				augmented[row][entry] =
					(augmented[row][entry] ?? 0) -
					factor * (augmented[column][entry] ?? 0);
			}
		}
	}

	return augmented.map((row) => row[size] ?? 0);
}

function invertMatrix(matrix: number[][]): number[][] {
	return matrix
		.map((_, column) => {
			const basis = Array.from({ length: matrix.length }, (_entry, index) =>
				index === column ? 1 : 0,
			);
			return solveLinearSystem(matrix, basis);
		})
		.reduce<number[][]>((inverse, columnValues, column) => {
			columnValues.forEach((value, row) => {
				inverse[row] ??= [];
				inverse[row][column] = value;
			});
			return inverse;
		}, []);
}

function fitRidgeRegression(
	observations: DailyObservation[],
	segmentKeys: string[],
	lambda: number,
): FittedRegression {
	const featureCount = segmentKeys.length + 1;
	const matrix = Array.from({ length: featureCount }, () =>
		Array.from({ length: featureCount }, () => 0),
	);
	const vector = Array.from({ length: featureCount }, () => 0);

	for (const observation of observations) {
		const row = [
			1,
			...segmentKeys.map((key) => observation.features[key] ?? 0),
		];
		for (let left = 0; left < featureCount; left += 1) {
			vector[left] += row[left] * observation.revenue;
			for (let right = 0; right < featureCount; right += 1) {
				matrix[left][right] += row[left] * row[right];
			}
		}
	}

	for (let index = 1; index < featureCount; index += 1) {
		matrix[index][index] += lambda;
	}

	const coefficients = solveLinearSystem(matrix, vector);
	const intercept = coefficients[0] ?? 0;
	const coefficientMap = Object.fromEntries(
		segmentKeys.map((key, index) => [key, coefficients[index + 1] ?? 0]),
	);
	const residualSse = observations.reduce((sum, observation) => {
		const error =
			observation.revenue - predict(observation, intercept, coefficientMap);
		return sum + error ** 2;
	}, 0);
	const degreesOfFreedom = Math.max(1, observations.length - featureCount);
	const residualSigma = Math.max(
		1e-6,
		Math.sqrt(residualSse / degreesOfFreedom),
	);
	const inverse = invertMatrix(matrix);

	return {
		intercept,
		coefficients: coefficientMap,
		covariance: inverse.map((row) =>
			row.map((value) => value * residualSigma ** 2),
		),
		residualSigma,
		parameterNames: ["intercept", ...segmentKeys],
	};
}

function predict(
	observation: DailyObservation,
	intercept: number,
	coefficients: Record<string, number>,
): number {
	return Object.entries(coefficients).reduce(
		(sum, [key, coefficient]) =>
			sum + (observation.features[key] ?? 0) * coefficient,
		intercept,
	);
}

function validationMetrics(
	observations: DailyObservation[],
	intercept: number,
	coefficients: Record<string, number>,
) {
	if (observations.length === 0) {
		return {
			observationCount: 0,
			mape: null,
			rmse: null,
			meanActualRevenue: null,
			meanPredictedRevenue: null,
		};
	}

	const scored = observations.map((observation) => ({
		actual: observation.revenue,
		predicted: Math.max(0, predict(observation, intercept, coefficients)),
	}));
	const squaredError = scored.reduce(
		(sum, row) => sum + (row.actual - row.predicted) ** 2,
		0,
	);
	const absolutePercentageError = scored.reduce(
		(sum, row) =>
			sum +
			(row.actual > 0 ? Math.abs(row.actual - row.predicted) / row.actual : 0),
		0,
	);

	return {
		observationCount: observations.length,
		mape: absolutePercentageError / observations.length,
		rmse: Math.sqrt(squaredError / observations.length),
		meanActualRevenue:
			scored.reduce((sum, row) => sum + row.actual, 0) / observations.length,
		meanPredictedRevenue:
			scored.reduce((sum, row) => sum + row.predicted, 0) / observations.length,
	};
}

function resolveDivergenceTier(
	deterministicAnchorRevenue: number,
	modeledRevenue: number,
	thresholds: CalibrationGovernanceThresholds,
) {
	const absoluteDivergence = modeledRevenue - deterministicAnchorRevenue;
	const divergenceRate =
		Math.abs(absoluteDivergence) /
		Math.max(Math.abs(deterministicAnchorRevenue), 1);
	const tier =
		divergenceRate >= thresholds.alertDivergenceRate
			? "alert"
			: divergenceRate >= thresholds.warnDivergenceRate
				? "watch"
				: "aligned";

	return {
		absoluteDivergence,
		divergenceRate,
		tier,
	};
}

function buildChannelWeekReconciliation(input: {
	attributionRows: MmmBaselineMartRow[];
	observations: DailyObservation[];
	modelSegments: string[];
	selectedSegmentSet: Set<string>;
	segmentStats: Map<string, SegmentStats>;
	coefficients: Record<string, number>;
	thresholds: CalibrationGovernanceThresholds;
}) {
	const deterministicRevenueByWeekSegment = new Map<string, number>();

	for (const row of input.attributionRows) {
		const rawKey = segmentKey(row);
		const modelKey = input.selectedSegmentSet.has(rawKey)
			? rawKey
			: "__other_paid__";
		const key = `${row.metric_date}::${modelKey}`;
		deterministicRevenueByWeekSegment.set(
			key,
			(deterministicRevenueByWeekSegment.get(key) ?? 0) +
				toNumber(row.attribution_credit_revenue),
		);
	}

	const reconciliationRows = input.observations.flatMap((observation) =>
		input.modelSegments.map((modelKey) => {
			const stats = input.segmentStats.get(modelKey);
			const modeledRevenue = Math.max(
				0,
				(input.coefficients[modelKey] ?? 0) *
					(observation.features[modelKey] ?? 0),
			);
			const deterministicAnchorRevenue =
				deterministicRevenueByWeekSegment.get(
					`${observation.date}::${modelKey}`,
				) ?? 0;
			const divergence = resolveDivergenceTier(
				deterministicAnchorRevenue,
				modeledRevenue,
				input.thresholds,
			);

			return {
				weekStartDate: observation.date,
				key: modelKey,
				source:
					stats?.source ??
					(modelKey === "__other_paid__" ? "__other_paid__" : "unknown"),
				medium: stats?.medium ?? "unknown",
				campaign: stats?.campaign ?? "unknown",
				deterministicAnchorRevenue,
				modeledRevenue,
				absoluteDivergence: divergence.absoluteDivergence,
				divergenceRate: divergence.divergenceRate,
				governanceTier: divergence.tier,
			};
		}),
	);
	const divergenceAlerts = reconciliationRows.filter(
		(row) => row.governanceTier === "alert",
	);

	return {
		status:
			divergenceAlerts.length > 0
				? "alert"
				: reconciliationRows.some((row) => row.governanceTier === "watch")
					? "watch"
					: "aligned",
		thresholds: input.thresholds,
		reconciliationLogic:
			"For each week_start_date and modeled channel segment, modeled revenue is max(0, fitted coefficient * transformed spend feature) and is reconciled to deterministic attribution_credit_revenue from the same weekly MMM mart snapshot. Non-selected paid segments are reconciled under __other_paid__. Divergence rate is abs(modeled - deterministic_anchor) / max(abs(deterministic_anchor), 1).",
		rowCount: reconciliationRows.length,
		alertCount: divergenceAlerts.length,
		watchCount: reconciliationRows.filter(
			(row) => row.governanceTier === "watch",
		).length,
		channelWeekReconciliation: reconciliationRows,
		divergenceAlerts,
	};
}

function createSeededRandom(seedText: string): () => number {
	let seed = createHash("sha256").update(seedText).digest().readUInt32LE(0);

	return () => {
		seed += 0x6d2b79f5;
		let value = seed;
		value = Math.imul(value ^ (value >>> 15), value | 1);
		value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
		return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
	};
}

function normalDraw(random: () => number): number {
	const left = Math.max(Number.MIN_VALUE, random());
	const right = random();
	return Math.sqrt(-2 * Math.log(left)) * Math.cos(2 * Math.PI * right);
}

function cholesky(matrix: number[][]): number[][] {
	const size = matrix.length;
	const lower = Array.from({ length: size }, () =>
		Array.from({ length: size }, () => 0),
	);

	for (let row = 0; row < size; row += 1) {
		for (let column = 0; column <= row; column += 1) {
			const sum = Array.from({ length: column }).reduce<number>(
				(subtotal, _entry, index) =>
					subtotal + (lower[row][index] ?? 0) * (lower[column][index] ?? 0),
				0,
			);

			if (row === column) {
				lower[row][column] = Math.sqrt(
					Math.max((matrix[row][row] ?? 0) - sum, 1e-10),
				);
			} else {
				lower[row][column] =
					((matrix[row][column] ?? 0) - sum) /
					Math.max(lower[column][column] ?? 0, 1e-10);
			}
		}
	}

	return lower;
}

function quantile(values: number[], probability: number): number {
	if (values.length === 0) {
		return 0;
	}

	const sorted = [...values].sort((left, right) => left - right);
	const position = (sorted.length - 1) * probability;
	const lower = Math.floor(position);
	const upper = Math.ceil(position);
	if (lower === upper) {
		return sorted[lower] ?? 0;
	}

	const weight = position - lower;
	return (sorted[lower] ?? 0) * (1 - weight) + (sorted[upper] ?? 0) * weight;
}

function summarizePosterior(values: number[]) {
	const mean =
		values.reduce((sum, value) => sum + value, 0) / Math.max(values.length, 1);

	return {
		mean,
		median: quantile(values, 0.5),
		credibleInterval80: {
			lower: quantile(values, 0.1),
			upper: quantile(values, 0.9),
		},
		credibleInterval95: {
			lower: quantile(values, 0.025),
			upper: quantile(values, 0.975),
		},
	};
}

function variance(values: number[]): number {
	if (values.length < 2) {
		return 0;
	}

	const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
	return (
		values.reduce((sum, value) => sum + (value - mean) ** 2, 0) /
		(values.length - 1)
	);
}

function autocorrelation(values: number[], lag: number): number {
	if (values.length <= lag) {
		return 0;
	}

	const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
	const denominator = values.reduce(
		(sum, value) => sum + (value - mean) ** 2,
		0,
	);
	if (denominator === 0) {
		return 0;
	}

	let numerator = 0;
	for (let index = 0; index < values.length - lag; index += 1) {
		numerator +=
			((values[index] ?? 0) - mean) * ((values[index + lag] ?? 0) - mean);
	}

	return numerator / denominator;
}

function posteriorDiagnostics(chains: number[][][]) {
	const parameterCount = chains[0]?.[0]?.length ?? 0;
	const chainCount = chains.length;
	const drawsPerChain = chains[0]?.length ?? 0;
	const byParameter = Array.from(
		{ length: parameterCount },
		(_entry, parameterIndex) => {
			const parameterChains = chains.map((chain) =>
				chain.map((draw) => draw[parameterIndex] ?? 0),
			);
			const chainMeans = parameterChains.map(
				(chain) =>
					chain.reduce((sum, value) => sum + value, 0) /
					Math.max(chain.length, 1),
			);
			const chainVariances = parameterChains.map(variance);
			const meanOfMeans =
				chainMeans.reduce((sum, value) => sum + value, 0) /
				Math.max(chainMeans.length, 1);
			const between =
				(drawsPerChain *
					chainMeans.reduce(
						(sum, value) => sum + (value - meanOfMeans) ** 2,
						0,
					)) /
				Math.max(chainCount - 1, 1);
			const within =
				chainVariances.reduce((sum, value) => sum + value, 0) /
				Math.max(chainVariances.length, 1);
			const posteriorVariance =
				((drawsPerChain - 1) / Math.max(drawsPerChain, 1)) * within +
				between / Math.max(drawsPerChain, 1);
			const rhat =
				within > 0 ? Math.sqrt(Math.max(posteriorVariance / within, 0)) : 1;
			const merged = parameterChains.flat();
			let autocorrelationSum = 0;
			for (let lag = 1; lag < Math.min(50, merged.length - 1); lag += 1) {
				const rho = autocorrelation(merged, lag);
				if (rho <= 0) {
					break;
				}
				autocorrelationSum += rho;
			}
			const effectiveSampleSize =
				merged.length / Math.max(1 + 2 * autocorrelationSum, 1);

			return {
				rhat,
				effectiveSampleSize,
			};
		},
	);

	return {
		chains: chainCount,
		drawsPerChain,
		totalDraws: chainCount * drawsPerChain,
		maxRhat: Math.max(...byParameter.map((entry) => entry.rhat), 1),
		minEffectiveSampleSize: Math.min(
			...byParameter.map((entry) => entry.effectiveSampleSize),
			chainCount * drawsPerChain,
		),
		byParameter,
	};
}

function samplePosterior(
	fitted: FittedRegression,
	segmentKeys: string[],
	inputHash: string,
	posteriorChains: number,
	posteriorDraws: number,
) {
	const meanVector = [
		fitted.intercept,
		...segmentKeys.map((key) => fitted.coefficients[key] ?? 0),
	];
	const lower = cholesky(fitted.covariance);
	const drawsPerChain = Math.max(
		1,
		Math.floor(posteriorDraws / posteriorChains),
	);

	return Array.from({ length: posteriorChains }, (_entry, chainIndex) => {
		const random = createSeededRandom(`${inputHash}:${chainIndex}`);
		return Array.from({ length: drawsPerChain }, () => {
			const standardNormal = meanVector.map(() => normalDraw(random));
			return meanVector.map((mean, row) => {
				const offset = standardNormal.reduce(
					(sum, value, column) => sum + (lower[row][column] ?? 0) * value,
					0,
				);
				return mean + offset;
			});
		});
	});
}

function summarizePosteriorCoefficients(
	chains: number[][][],
	parameterNames: string[],
) {
	const draws = chains.flat();
	return Object.fromEntries(
		parameterNames.map((parameterName, parameterIndex) => [
			parameterName,
			summarizePosterior(draws.map((draw) => draw[parameterIndex] ?? 0)),
		]),
	);
}

function buildContributionOutputs(
	chains: number[][][],
	segmentKeys: string[],
	observations: DailyObservation[],
	segmentStats: Map<string, SegmentStats>,
) {
	const draws = chains.flat();
	const totalActualRevenue = observations.reduce(
		(sum, observation) => sum + observation.revenue,
		0,
	);
	const totalFeatureBySegment = Object.fromEntries(
		segmentKeys.map((key) => [
			key,
			observations.reduce(
				(sum, observation) => sum + (observation.features[key] ?? 0),
				0,
			),
		]),
	);
	const contributionDraws = Object.fromEntries(
		segmentKeys.map((key, keyIndex) => [
			key,
			draws.map((draw) =>
				Math.max(
					0,
					(draw[keyIndex + 1] ?? 0) * (totalFeatureBySegment[key] ?? 0),
				),
			),
		]),
	);
	const totalMediaDraws = draws.map((_draw, drawIndex) =>
		segmentKeys.reduce(
			(sum, key) => sum + ((contributionDraws[key] ?? [])[drawIndex] ?? 0),
			0,
		),
	);

	return {
		totalActualRevenue,
		totalMediaContribution: summarizePosterior(totalMediaDraws),
		channels: segmentKeys.map((key) => {
			const stats = segmentStats.get(key);
			const values = contributionDraws[key] ?? [];
			const shares = values.map((value, index) => {
				const total = totalMediaDraws[index] ?? 0;
				return total > 0 ? value / total : 0;
			});

			return {
				key,
				source: stats?.source ?? "unknown",
				medium: stats?.medium ?? "unknown",
				campaign: stats?.campaign ?? "unknown",
				spend: stats?.spend ?? 0,
				impressions: stats?.impressions ?? 0,
				clicks: stats?.clicks ?? 0,
				attributedRevenue: stats?.attributedRevenue ?? 0,
				contribution: summarizePosterior(values),
				contributionShare: summarizePosterior(shares),
				posteriorProbabilityPositive:
					values.filter((value) => value > 0).length /
					Math.max(values.length, 1),
			};
		}),
	};
}

export function buildBaselineMmmArtifact(
	rows: MmmBaselineMartRow[],
	input: MmmBaselineTrainingInput,
): MmmBaselineModelRun {
	const startDate = normalizeDate(input.startDate, "startDate");
	const endDate = normalizeDate(input.endDate, "endDate");
	if (startDate > endDate) {
		throw new Error("startDate must be on or before endDate");
	}

	const attributionModel = input.attributionModel?.trim() || "last_touch";
	const maxSegments = clampInteger(
		input.maxSegments,
		DEFAULT_MAX_SEGMENTS,
		1,
		25,
	);
	const adstockDecay = clampNumber(
		input.adstockDecay,
		DEFAULT_ADSTOCK_DECAY,
		0,
		0.95,
	);
	const ridgeLambda = clampNumber(
		input.ridgeLambda,
		DEFAULT_RIDGE_LAMBDA,
		0,
		10_000,
	);
	const posteriorChains = clampInteger(
		input.posteriorChains,
		DEFAULT_POSTERIOR_CHAINS,
		2,
		8,
	);
	const posteriorDraws = clampInteger(
		input.posteriorDraws,
		DEFAULT_POSTERIOR_DRAWS,
		posteriorChains * 50,
		posteriorChains * 5_000,
	);
	const holdoutRatio = clampNumber(input.holdoutRatio, 0.2, 0, 0.5);
	const calibrationThresholds = {
		warnDivergenceRate: clampNumber(
			input.calibrationWarnDivergenceRate,
			DEFAULT_CALIBRATION_WARN_DIVERGENCE_RATE,
			0,
			10,
		),
		alertDivergenceRate: clampNumber(
			input.calibrationAlertDivergenceRate,
			DEFAULT_CALIBRATION_ALERT_DIVERGENCE_RATE,
			0,
			10,
		),
	};
	if (
		calibrationThresholds.warnDivergenceRate >
		calibrationThresholds.alertDivergenceRate
	) {
		throw new Error(
			"calibrationWarnDivergenceRate must be less than or equal to calibrationAlertDivergenceRate",
		);
	}

	const dates = Array.from(new Set(rows.map((row) => row.metric_date))).sort();
	const paidRows = rows.filter(
		(row) =>
			row.mart_row_type === "paid_media" ||
			row.mart_row_type === "weekly_channel",
	);
	const attributionRows = rows.filter(
		(row) =>
			(row.mart_row_type === "attribution" ||
				row.mart_row_type === "weekly_channel") &&
			row.attribution_model === attributionModel,
	);

	const segmentStats = new Map<string, SegmentStats>();
	const dailySpendBySegment = new Map<string, Map<string, number>>();
	for (const row of paidRows) {
		const key = segmentKey(row);
		const stats = addSegmentMetric(segmentStats, key, row);
		stats.spend += toNumber(row.spend);
		stats.impressions += toNumber(row.impressions);
		stats.clicks += toNumber(row.clicks);

		const daily =
			dailySpendBySegment.get(row.metric_date) ?? new Map<string, number>();
		daily.set(key, (daily.get(key) ?? 0) + toNumber(row.spend));
		dailySpendBySegment.set(row.metric_date, daily);
	}

	const dailyRevenue = new Map<string, number>();
	const attributionSegments = new Map<string, SegmentStats>();
	for (const row of attributionRows) {
		const key = segmentKey(row);
		const stats = addSegmentMetric(attributionSegments, key, row);
		stats.attributedRevenue += toNumber(row.attribution_credit_revenue);
		stats.attributedOrders += toNumber(row.attribution_credit_orders);
		dailyRevenue.set(
			row.metric_date,
			(dailyRevenue.get(row.metric_date) ?? 0) + toNumber(row.shopify_revenue),
		);
	}

	for (const [key, attributionStats] of attributionSegments) {
		const stats = segmentStats.get(key) ?? attributionStats;
		stats.attributedRevenue = attributionStats.attributedRevenue;
		stats.attributedOrders = attributionStats.attributedOrders;
		segmentStats.set(key, stats);
	}

	const selectedSegments = [...segmentStats.values()]
		.filter((segment) => segment.spend > 0)
		.sort(
			(left, right) =>
				right.spend - left.spend || left.key.localeCompare(right.key),
		)
		.slice(0, maxSegments)
		.map((segment) => segment.key);
	const selectedSegmentSet = new Set(selectedSegments);
	const modelSegments = [...selectedSegments, "__other_paid__"];
	const spendSeries = Object.fromEntries(
		modelSegments.map((key) => [key, dates.map(() => 0)]),
	);

	dates.forEach((date, dateIndex) => {
		const daily = dailySpendBySegment.get(date);
		if (!daily) {
			return;
		}

		for (const [key, spend] of daily) {
			const featureKey = selectedSegmentSet.has(key) ? key : "__other_paid__";
			spendSeries[featureKey][dateIndex] += spend;
		}
	});

	const transformedSeries = Object.fromEntries(
		Object.entries(spendSeries).map(([key, values]) => [
			key,
			applyAdstock(values, adstockDecay),
		]),
	);
	const observations = dates
		.filter((date) => dailyRevenue.has(date))
		.map((date) => {
			const dateIndex = dates.indexOf(date);
			return {
				date,
				revenue: dailyRevenue.get(date) ?? 0,
				features: Object.fromEntries(
					modelSegments.map((key) => [
						key,
						transformedSeries[key][dateIndex] ?? 0,
					]),
				),
			};
		});

	if (observations.length < MIN_OBSERVATIONS) {
		throw new Error(
			`MMM baseline requires at least ${MIN_OBSERVATIONS} daily observations from the approved mart`,
		);
	}

	const holdoutCount =
		observations.length >= 8
			? Math.max(1, Math.floor(observations.length * holdoutRatio))
			: 0;
	const trainingObservations =
		holdoutCount > 0 ? observations.slice(0, -holdoutCount) : observations;
	const holdoutObservations =
		holdoutCount > 0 ? observations.slice(-holdoutCount) : [];
	const fitted = fitRidgeRegression(
		trainingObservations,
		modelSegments,
		ridgeLambda,
	);
	const martInputHash = hashJson(rows);
	const posteriorChainsByDraw = samplePosterior(
		fitted,
		modelSegments,
		martInputHash,
		posteriorChains,
		posteriorDraws,
	);
	const diagnostics = posteriorDiagnostics(posteriorChainsByDraw);
	const posteriorCoefficients = summarizePosteriorCoefficients(
		posteriorChainsByDraw,
		fitted.parameterNames,
	);
	const contributionOutputs = buildContributionOutputs(
		posteriorChainsByDraw,
		modelSegments,
		observations,
		segmentStats,
	);
	const posteriorSanityChecks = {
		status:
			Number.isFinite(diagnostics.maxRhat) &&
			diagnostics.maxRhat <= MAX_RHAT &&
			diagnostics.minEffectiveSampleSize >= MIN_EFFECTIVE_SAMPLE_SIZE &&
			contributionOutputs.channels.every(
				(channel) =>
					channel.contribution.mean >= 0 &&
					Number.isFinite(channel.contribution.mean),
			)
				? "pass"
				: "fail",
		maxRhat: diagnostics.maxRhat,
		maxAllowedRhat: MAX_RHAT,
		minEffectiveSampleSize: diagnostics.minEffectiveSampleSize,
		minRequiredEffectiveSampleSize: MIN_EFFECTIVE_SAMPLE_SIZE,
		finiteContributionIntervals: contributionOutputs.channels.every(
			(channel) =>
				Number.isFinite(channel.contribution.credibleInterval95.lower) &&
				Number.isFinite(channel.contribution.credibleInterval95.upper),
		),
	};
	if (posteriorSanityChecks.status !== "pass") {
		throw new Error(
			`MMM posterior sanity checks failed: maxRhat=${diagnostics.maxRhat.toFixed(3)}, minESS=${diagnostics.minEffectiveSampleSize.toFixed(0)}`,
		);
	}
	const trainMetrics = validationMetrics(
		trainingObservations,
		fitted.intercept,
		fitted.coefficients,
	);
	const holdoutMetrics = validationMetrics(
		holdoutObservations,
		fitted.intercept,
		fitted.coefficients,
	);
	const otherSegmentStats = modelSegments.includes("__other_paid__")
		? {
				key: "__other_paid__",
				source: "__other_paid__",
				medium: "mixed",
				campaign: "mixed",
				spend: [...segmentStats.values()]
					.filter((segment) => !selectedSegmentSet.has(segment.key))
					.reduce((sum, segment) => sum + segment.spend, 0),
				impressions: [...segmentStats.values()]
					.filter((segment) => !selectedSegmentSet.has(segment.key))
					.reduce((sum, segment) => sum + segment.impressions, 0),
				clicks: [...segmentStats.values()]
					.filter((segment) => !selectedSegmentSet.has(segment.key))
					.reduce((sum, segment) => sum + segment.clicks, 0),
				attributedRevenue: [...segmentStats.values()]
					.filter((segment) => !selectedSegmentSet.has(segment.key))
					.reduce((sum, segment) => sum + segment.attributedRevenue, 0),
				attributedOrders: [...segmentStats.values()]
					.filter((segment) => !selectedSegmentSet.has(segment.key))
					.reduce((sum, segment) => sum + segment.attributedOrders, 0),
			}
		: null;
	const reconciliationSegmentStats = new Map(segmentStats);
	if (otherSegmentStats) {
		reconciliationSegmentStats.set("__other_paid__", otherSegmentStats);
	}
	const channelWeekGovernance = buildChannelWeekReconciliation({
		attributionRows,
		observations,
		modelSegments,
		selectedSegmentSet,
		segmentStats: reconciliationSegmentStats,
		coefficients: fitted.coefficients,
		thresholds: calibrationThresholds,
	});
	const totalAttributedRevenue = [...segmentStats.values()].reduce(
		(sum, segment) => sum + segment.attributedRevenue,
		0,
	);
	const coefficientRevenue = Object.fromEntries(
		modelSegments.map((key) => {
			const totalFeature = observations.reduce(
				(sum, observation) => sum + (observation.features[key] ?? 0),
				0,
			);
			return [key, Math.max(0, (fitted.coefficients[key] ?? 0) * totalFeature)];
		}),
	);
	const totalCoefficientRevenue = Object.values(coefficientRevenue).reduce(
		(sum, value) => sum + value,
		0,
	);
	const calibrationSegments = [...segmentStats.values()]
		.filter((segment) => selectedSegmentSet.has(segment.key))
		.map((segment) => {
			const modeledRevenue = coefficientRevenue[segment.key] ?? 0;
			return {
				key: segment.key,
				source: segment.source,
				medium: segment.medium,
				campaign: segment.campaign,
				spend: segment.spend,
				attributedRevenue: segment.attributedRevenue,
				attributedRevenueShare:
					totalAttributedRevenue > 0
						? segment.attributedRevenue / totalAttributedRevenue
						: null,
				modeledRevenue,
				modeledRevenueShare:
					totalCoefficientRevenue > 0
						? modeledRevenue / totalCoefficientRevenue
						: null,
				calibrationRatio:
					segment.attributedRevenue > 0 && modeledRevenue > 0
						? modeledRevenue / segment.attributedRevenue
						: null,
			};
		});
	const config = {
		attributionModel,
		maxSegments,
		adstockDecay,
		ridgeLambda,
		posteriorChains,
		posteriorDraws: diagnostics.totalDraws,
		calibrationGovernanceThresholds: calibrationThresholds,
		bayesianEngine: "closed_form_linear_gaussian_posterior_v1",
		hierarchy: {
			level: "channel_segment",
			grouping: "source|medium|campaign",
			prior: "ridge_precision_partial_pooling_to_global_media_effect",
		},
		holdoutRatio,
		responseVariable: rows.some((row) => row.mart_row_type === "weekly_channel")
			? "weekly_total_shopify_revenue_from_channel_mart_outcomes"
			: "daily_total_shopify_revenue_from_mart_outcomes",
		calibrationUse:
			"segment attribution credit metrics are validation/calibration diagnostics, not per-segment training labels",
	};
	const inputSummary = {
		rowCount: rows.length,
		paidMediaRowCount: paidRows.length,
		attributionRowCount: attributionRows.length,
		observationCount: observations.length,
		trainingObservationCount: trainingObservations.length,
		holdoutObservationCount: holdoutObservations.length,
		selectedSegments,
		martInputHash,
	};

	return {
		id: null,
		approvedFreezeId: input.approvedFreezeId ?? null,
		modelType: MMM_BASELINE_MODEL_TYPE,
		modelVersion: MMM_BASELINE_MODEL_VERSION,
		martVersion: MMM_BASELINE_MART_VERSION,
		attributionModel,
		trainingStartDate: startDate,
		trainingEndDate: endDate,
		holdoutStartDate: holdoutObservations[0]?.date ?? null,
		holdoutEndDate: holdoutObservations.at(-1)?.date ?? null,
		runConfig: config,
		inputSummary,
		modelArtifact: {
			intercept: fitted.intercept,
			coefficients: fitted.coefficients,
			posteriorCoefficients,
			residualSigma: fitted.residualSigma,
			featureTransform: {
				spend: "log1p(adstock(spend))",
				adstockDecay,
			},
			contributionOutputs,
			segments: calibrationSegments.map(
				({ key, source, medium, campaign, spend }) => ({
					key,
					source,
					medium,
					campaign,
					spend,
				}),
			),
		},
		calibrationReport: {
			attributionModel,
			deterministicAttributionUsage: "calibration_and_validation_segments_only",
			governanceStatus: channelWeekGovernance.status,
			totalAttributedRevenue,
			totalModeledMediaRevenue: totalCoefficientRevenue,
			segments: calibrationSegments,
			governance: channelWeekGovernance,
			divergenceAlerts: channelWeekGovernance.divergenceAlerts,
		},
		validationReport: {
			train: trainMetrics,
			holdout: holdoutMetrics,
			posteriorDiagnostics: {
				...diagnostics,
				byParameter: Object.fromEntries(
					fitted.parameterNames.map((parameterName, index) => [
						parameterName,
						diagnostics.byParameter[index],
					]),
				),
			},
			posteriorSanityChecks,
		},
	};
}

export function buildBaselineCalibrationFreeze(
	rows: WeeklyFreezeRow[],
	input: Omit<Parameters<typeof buildMmmBaselineFreezeSnapshot>[0], "rows">,
): MmmBaselineCalibrationFreeze {
	return buildMmmBaselineFreezeSnapshot({ ...input, rows });
}

export async function createBaselineCalibrationFreezeWithClient(
	client: PoolClient,
	input: MmmBaselineFreezeInput,
): Promise<MmmBaselineCalibrationFreeze> {
	const startDate = normalizeDate(input.startDate, "startDate");
	const endDate = normalizeDate(input.endDate, "endDate");
	const attributionModel = input.attributionModel?.trim() || "last_touch";
	const freezeStatus = input.freezeStatus ?? "pending";
	const createdBy = input.submittedBy?.trim() || "admin-cli";
	const approvedBy =
		freezeStatus === "approved" ? input.approvedBy?.trim() || createdBy : null;
	const approvedAt =
		freezeStatus === "approved" ? new Date().toISOString() : null;
	if (freezeStatus !== "approved" && input.approvedBy?.trim()) {
		throw new Error(
			"approvedBy can only be set for approved MMM baseline freezes",
		);
	}

	const qualitySummary = await refreshWeeklyMmmChannelInputMartWithClient(
		client,
		{
			startDate,
			endDate,
			attributionModels: [attributionModel],
		},
	);
	const weeklyRows = await fetchWeeklyMmmSnapshotRowsWithClient(client, {
		startDate,
		endDate,
		attributionModels: [attributionModel],
	});
	const generationTimestamp = new Date().toISOString();
	const freeze = buildMmmBaselineFreezeSnapshot({
		startDate,
		endDate,
		attributionModel,
		rows: weeklyRows,
		qualitySummary,
		generationTimestamp,
		freezeStatus,
		createdBy,
		approvedBy,
		approvedAt,
	});
	const insertResult = await client.query<{ id: string }>(
		`
      INSERT INTO mmm_baseline_calibration_freezes (
        freeze_schema_version,
        mart_version,
        snapshot_version,
        freeze_status,
        generation_timestamp,
        calibration_start_date,
        calibration_end_date,
        attribution_model,
        row_counts,
        deterministic_attribution_coverage,
        freshness_metrics,
        campaign_metadata_coverage,
        exposure_coverage,
        data_quality_checks,
        aggregate_metric_totals,
        evidence_hash,
        snapshot_rows,
        created_by,
        approved_by,
        approved_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5::timestamptz,
        $6::date,
        $7::date,
        $8,
        $9::jsonb,
        $10::jsonb,
        $11::jsonb,
        $12::jsonb,
        $13::jsonb,
        $14::jsonb,
        $15::jsonb,
        $16,
        $17::jsonb,
        $18,
        $19,
        $20::timestamptz
      )
      RETURNING id
    `,
		[
			freeze.freezeSchemaVersion,
			freeze.martVersion,
			freeze.snapshotVersion,
			freeze.freezeStatus,
			freeze.generationTimestamp,
			freeze.calibrationStartDate,
			freeze.calibrationEndDate,
			freeze.attributionModel,
			JSON.stringify(freeze.rowCounts),
			JSON.stringify(freeze.deterministicAttributionCoverage),
			JSON.stringify(freeze.freshnessMetrics),
			JSON.stringify(freeze.campaignMetadataCoverage),
			JSON.stringify(freeze.exposureCoverage),
			JSON.stringify(freeze.dataQualityChecks),
			JSON.stringify(freeze.aggregateMetricTotals),
			freeze.evidenceHash,
			JSON.stringify(freeze.snapshotRows),
			freeze.createdBy,
			freeze.approvedBy,
			freeze.approvedAt,
		],
	);
	const freezeId = insertResult.rows[0]?.id;
	if (!freezeId) {
		throw new Error(
			"MMM baseline calibration freeze insert did not return an id",
		);
	}

	return {
		...freeze,
		id: freezeId,
	};
}

export async function createBaselineCalibrationFreeze(
	input: MmmBaselineFreezeInput,
): Promise<MmmBaselineCalibrationFreeze> {
	return withTransaction((client) =>
		createBaselineCalibrationFreezeWithClient(client, input),
	);
}

async function fetchApprovedBaselineFreezeWithClient(
	client: PoolClient,
	input: {
		freezeId: string;
		startDate: string;
		endDate: string;
		attributionModel: string;
	},
): Promise<MmmBaselineCalibrationFreeze> {
	const result = await client.query<{
		id: string;
		freeze_schema_version: MmmBaselineCalibrationFreeze["freezeSchemaVersion"];
		mart_version: typeof MMM_BASELINE_MART_VERSION;
		snapshot_version: MmmBaselineCalibrationFreeze["snapshotVersion"];
		freeze_status: MmmBaselineCalibrationFreeze["freezeStatus"];
		generation_timestamp: Date | string;
		calibration_start_date: string;
		calibration_end_date: string;
		attribution_model: string;
		row_counts: Record<string, unknown>;
		deterministic_attribution_coverage: Record<string, unknown>;
		freshness_metrics: Record<string, unknown>;
		campaign_metadata_coverage: Record<string, unknown>;
		exposure_coverage: Record<string, unknown>;
		data_quality_checks: Record<string, unknown>;
		aggregate_metric_totals: Record<string, unknown>;
		evidence_hash: string;
		snapshot_rows: WeeklyFreezeRow[];
		created_by: string;
		approved_by: string | null;
		approved_at: Date | string | null;
	}>(
		`
      SELECT
        id,
        freeze_schema_version,
        mart_version,
        snapshot_version,
        freeze_status,
        generation_timestamp,
        calibration_start_date::text,
        calibration_end_date::text,
        attribution_model,
        row_counts,
        deterministic_attribution_coverage,
        freshness_metrics,
        campaign_metadata_coverage,
        exposure_coverage,
        data_quality_checks,
        aggregate_metric_totals,
        evidence_hash,
        snapshot_rows,
        created_by,
        approved_by,
        approved_at
      FROM mmm_baseline_calibration_freezes
      WHERE id = $1::uuid
        AND freeze_status = 'approved'
      FOR SHARE
    `,
		[input.freezeId],
	);
	const row = result.rows[0];
	if (!row) {
		throw new Error(
			`MMM baseline training requires an approved freeze id; no approved freeze found for ${input.freezeId}`,
		);
	}
	if (
		row.calibration_start_date !== input.startDate ||
		row.calibration_end_date !== input.endDate ||
		row.attribution_model !== input.attributionModel
	) {
		throw new Error(
			"approvedFreezeId does not match the requested MMM baseline training window and attribution model",
		);
	}
	if (
		row.freeze_schema_version !== "mmm_baseline_calibration_freeze_v1" ||
		row.mart_version !== MMM_BASELINE_MART_VERSION
	) {
		throw new Error(
			"approvedFreezeId uses an unsupported MMM baseline freeze schema or mart version",
		);
	}
	if (!Array.isArray(row.snapshot_rows) || row.snapshot_rows.length === 0) {
		throw new Error("approvedFreezeId has no frozen snapshot rows");
	}

	return {
		id: row.id,
		freezeSchemaVersion: row.freeze_schema_version,
		martVersion: row.mart_version,
		snapshotVersion: row.snapshot_version,
		freezeStatus: row.freeze_status,
		generationTimestamp: new Date(row.generation_timestamp).toISOString(),
		calibrationStartDate: row.calibration_start_date,
		calibrationEndDate: row.calibration_end_date,
		attributionModel: row.attribution_model,
		rowCounts: row.row_counts,
		deterministicAttributionCoverage: row.deterministic_attribution_coverage,
		freshnessMetrics: row.freshness_metrics,
		campaignMetadataCoverage: row.campaign_metadata_coverage,
		exposureCoverage: row.exposure_coverage,
		dataQualityChecks: row.data_quality_checks,
		aggregateMetricTotals: row.aggregate_metric_totals,
		evidenceHash: row.evidence_hash,
		snapshotRows: row.snapshot_rows,
		createdBy: row.created_by,
		approvedBy: row.approved_by,
		approvedAt: row.approved_at
			? new Date(row.approved_at).toISOString()
			: null,
	};
}

export async function trainBaselineMmmModelWithClient(
	client: PoolClient,
	input: MmmBaselineTrainingInput,
): Promise<MmmBaselineModelRun> {
	const startDate = normalizeDate(input.startDate, "startDate");
	const endDate = normalizeDate(input.endDate, "endDate");
	const attributionModel = input.attributionModel?.trim() || "last_touch";
	const approvedFreezeId = normalizeFreezeId(input.approvedFreezeId);
	if (!approvedFreezeId) {
		throw new Error(
			"baseline_linear_mmm_v1 training requires approvedFreezeId",
		);
	}
	const freeze = await fetchApprovedBaselineFreezeWithClient(client, {
		freezeId: approvedFreezeId,
		startDate,
		endDate,
		attributionModel,
	});
	const freezeFailCount = Number(
		(freeze.rowCounts as { failCount?: unknown }).failCount ?? 0,
	);
	if (freezeFailCount > 0) {
		throw new Error(
			`MMM approved freeze contains failed quality rows: ${freezeFailCount}`,
		);
	}

	const trainingRows = freeze.snapshotRows.map<MmmBaselineMartRow>((row) => ({
		metric_date: row.week_start_date,
		mart_row_type: "weekly_channel",
		attribution_model: row.attribution_model,
		platform: "taxonomy",
		source: row.source,
		medium: row.medium,
		campaign: row.campaign,
		spend: row.spend,
		impressions: row.impressions,
		clicks: row.clicks,
		shopify_revenue: row.shopify_revenue,
		attribution_credit_revenue: row.attribution_credit_revenue,
		attribution_credit_orders: row.attribution_credit_orders,
		match_source_coverage: row.match_source_coverage,
		confidence_label_coverage: row.confidence_label_coverage,
	}));
	const run = buildBaselineMmmArtifact(trainingRows, {
		...input,
		startDate,
		endDate,
		attributionModel,
	});
	const insertResult = await client.query<{ id: string }>(
		`
      INSERT INTO mmm_model_runs (
        model_type,
        model_version,
        mart_version,
        attribution_model,
        run_status,
        training_start_date,
        training_end_date,
        holdout_start_date,
        holdout_end_date,
        run_config,
        input_summary,
        model_artifact,
        calibration_report,
        validation_report,
        approved_freeze_id,
        completed_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        'completed',
        $5::date,
        $6::date,
        $7::date,
        $8::date,
        $9::jsonb,
        $10::jsonb,
        $11::jsonb,
        $12::jsonb,
        $13::jsonb,
        $14::uuid,
        now()
      )
      RETURNING id
    `,
		[
			run.modelType,
			run.modelVersion,
			run.martVersion,
			run.attributionModel,
			run.trainingStartDate,
			run.trainingEndDate,
			run.holdoutStartDate,
			run.holdoutEndDate,
			JSON.stringify(run.runConfig),
			JSON.stringify(run.inputSummary),
			JSON.stringify(run.modelArtifact),
			JSON.stringify(run.calibrationReport),
			JSON.stringify(run.validationReport),
			approvedFreezeId,
		],
	);
	const modelRunId = insertResult.rows[0]?.id;
	if (!modelRunId) {
		throw new Error("MMM baseline model run insert did not return an id");
	}

	const snapshot = await snapshotWeeklyMmmInputRowsWithClient(
		client,
		modelRunId,
		freeze.snapshotRows,
	);
	const inputSummary = {
		...run.inputSummary,
		approvedFreezeId,
		freezeSchemaVersion: freeze.freezeSchemaVersion,
		freezeGenerationTimestamp: freeze.generationTimestamp,
		freezeEvidenceHash: freeze.evidenceHash,
		freezeRowCounts: freeze.rowCounts,
		deterministicAttributionCoverage: freeze.deterministicAttributionCoverage,
		freshnessMetrics: freeze.freshnessMetrics,
		campaignMetadataCoverage: freeze.campaignMetadataCoverage,
		exposureCoverage: freeze.exposureCoverage,
		dataQualityChecks: freeze.dataQualityChecks,
		aggregateMetricTotals: freeze.aggregateMetricTotals,
		snapshotVersion: "mmm_weekly_channel_snapshot_v1",
		snapshotRowCount: snapshot.snapshotRowCount,
		snapshotHash: snapshot.snapshotHash,
	};
	await client.query(
		`
      UPDATE mmm_model_runs
      SET input_summary = $2::jsonb
      WHERE id = $1::uuid
    `,
		[modelRunId, JSON.stringify(inputSummary)],
	);

	return {
		...run,
		id: modelRunId,
		approvedFreezeId,
		inputSummary,
	};
}

export async function trainBaselineMmmModel(
	input: MmmBaselineTrainingInput,
): Promise<MmmBaselineModelRun> {
	return withTransaction((client) =>
		trainBaselineMmmModelWithClient(client, input),
	);
}
