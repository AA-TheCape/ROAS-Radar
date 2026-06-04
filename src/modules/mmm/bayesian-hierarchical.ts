import { createHash } from "node:crypto";

import type { PoolClient } from "pg";

import { withTransaction } from "../../db/pool.js";
import {
	buildMmmCalibrationReportV1,
	MMM_BASELINE_CLICK_LOOKBACK_DAYS,
	MMM_BASELINE_VIEW_LOOKBACK_DAYS,
	MMM_DETERMINISTIC_BASELINE_VERSION,
} from "./calibration-report.js";
import {
	BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION,
	type BayesianHierarchicalMmmV1FeatureRow,
	MMM_WEEKLY_CHANNEL_MART_VERSION,
	fetchBayesianHierarchicalMmmV1FeatureRowsWithClient,
	refreshWeeklyMmmChannelInputMartWithClient,
	snapshotWeeklyMmmInputRowsWithClient,
} from "./weekly-mart.js";

export const BAYESIAN_HIERARCHICAL_MMM_MODEL_TYPE = "bayesian_hierarchical_mmm";
export const BAYESIAN_HIERARCHICAL_MMM_MODEL_VERSION =
	"bayesian_hierarchical_mmm_v1";
export const BAYESIAN_HIERARCHICAL_MMM_MART_VERSION =
	MMM_WEEKLY_CHANNEL_MART_VERSION;

const DEFAULT_MAX_CHANNELS = 12;
const DEFAULT_ADSTOCK_DECAY = 0.55;
const DEFAULT_SATURATION_HALF_SATURATION = 1_000;
const DEFAULT_SATURATION_SLOPE = 1.4;
const DEFAULT_MEDIA_PRIOR_SD = 0.35;
const DEFAULT_GROUP_PRIOR_SD = 0.5;
const DEFAULT_CONTROL_PRIOR_SD = 5;
const DEFAULT_POSTERIOR_CHAINS = 4;
const DEFAULT_POSTERIOR_DRAWS = 1_000;
const DEFAULT_HOLDOUT_RATIO = 0.2;
const MIN_WEEKLY_OBSERVATIONS = 4;
const MIN_EFFECTIVE_SAMPLE_SIZE = 100;
const MAX_RHAT = 1.1;

export type BayesianHierarchicalMmmTrainingInput = {
	startDate: string;
	endDate: string;
	attributionModel?: string;
	refreshMart?: boolean;
	maxChannels?: number;
	adstockDecay?: number;
	saturationHalfSaturation?: number;
	saturationSlope?: number;
	mediaPriorSd?: number;
	groupPriorSd?: number;
	controlPriorSd?: number;
	posteriorChains?: number;
	posteriorDraws?: number;
	holdoutRatio?: number;
	submittedBy?: string;
};

type WeeklyObservation = {
	weekStartDate: string;
	revenue: number;
	features: Record<string, number>;
	controls: Record<string, number>;
};

type ChannelStats = {
	key: string;
	source: string;
	medium: string;
	campaign: string;
	channel: string;
	channelGroup: string;
	spend: number;
	impressions: number;
	clicks: number;
	attributedRevenue: number;
	attributedOrders: number;
};

type FittedModel = {
	coefficients: Record<string, number>;
	covariance: number[][];
	residualSigma: number;
	parameterNames: string[];
};

export type BayesianHierarchicalMmmModelRun = {
	id: string | null;
	modelType: typeof BAYESIAN_HIERARCHICAL_MMM_MODEL_TYPE;
	modelVersion: typeof BAYESIAN_HIERARCHICAL_MMM_MODEL_VERSION;
	martVersion: typeof BAYESIAN_HIERARCHICAL_MMM_MART_VERSION;
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

function channelKey(
	row: Pick<
		BayesianHierarchicalMmmV1FeatureRow,
		"source" | "medium" | "campaign"
	>,
): string {
	return [
		row.source || "unknown",
		row.medium || "unknown",
		row.campaign || "unknown",
	].join("|");
}

function addChannelStats(
	target: Map<string, ChannelStats>,
	key: string,
	row: BayesianHierarchicalMmmV1FeatureRow,
): ChannelStats {
	const existing = target.get(key);
	if (existing) {
		return existing;
	}

	const created = {
		key,
		source: row.source || "unknown",
		medium: row.medium || "unknown",
		campaign: row.campaign || "unknown",
		channel: row.channel || "unknown",
		channelGroup: row.channel_group || "unknown",
		spend: 0,
		impressions: 0,
		clicks: 0,
		attributedRevenue: 0,
		attributedOrders: 0,
	};
	target.set(key, created);
	return created;
}

function adstock(values: number[], decay: number): number[] {
	const transformed: number[] = [];
	let carry = 0;
	for (const value of values) {
		carry = value + carry * decay;
		transformed.push(carry);
	}
	return transformed;
}

function hillSaturation(
	values: number[],
	halfSaturation: number,
	slope: number,
) {
	return values.map((value) => {
		const numerator = Math.max(value, 0) ** slope;
		const denominator = numerator + Math.max(halfSaturation, 1e-6) ** slope;
		return denominator > 0 ? numerator / denominator : 0;
	});
}

function weekOfYear(dateString: string): number {
	const date = new Date(`${dateString}T00:00:00.000Z`);
	const start = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
	return Math.floor(
		(date.getTime() - start.getTime()) / (7 * 24 * 60 * 60 * 1000),
	);
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
			augmented[column][column] = 1e-8;
		}

		const denominator = augmented[column][column] ?? 1e-8;
		for (let entry = column; entry <= size; entry += 1) {
			augmented[column][entry] = (augmented[column][entry] ?? 0) / denominator;
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

function predict(
	observation: WeeklyObservation,
	coefficients: Record<string, number>,
) {
	return Object.entries(observation.features).reduce(
		(sum, [key, value]) => sum + value * (coefficients[key] ?? 0),
		Object.entries(observation.controls).reduce(
			(controlSum, [key, value]) =>
				controlSum + value * (coefficients[key] ?? 0),
			coefficients.intercept ?? 0,
		),
	);
}

function fitHierarchicalRegression(input: {
	observations: WeeklyObservation[];
	mediaFeatureKeys: string[];
	controlFeatureKeys: string[];
	channelStats: Map<string, ChannelStats>;
	mediaPriorSd: number;
	groupPriorSd: number;
	controlPriorSd: number;
}): FittedModel {
	const parameterNames = [
		"intercept",
		...input.mediaFeatureKeys,
		...input.controlFeatureKeys,
	];
	const size = parameterNames.length;
	const matrix = Array.from({ length: size }, () =>
		Array.from({ length: size }, () => 0),
	);
	const vector = Array.from({ length: size }, () => 0);
	const totalRevenue = input.observations.reduce(
		(sum, row) => sum + row.revenue,
		0,
	);
	const totalMediaFeature = input.mediaFeatureKeys.reduce(
		(sum, key) =>
			sum +
			input.observations.reduce(
				(subtotal, row) => subtotal + (row.features[key] ?? 0),
				0,
			),
		0,
	);
	const globalMediaPriorMean =
		totalMediaFeature > 0
			? totalRevenue / Math.max(totalMediaFeature, 1) / 2
			: 0;
	const groupPriorMeans = new Map<string, number>();
	for (const featureKey of input.mediaFeatureKeys) {
		const stats = input.channelStats.get(featureKey);
		const group = stats?.channelGroup ?? "unknown";
		const peers = input.mediaFeatureKeys.filter(
			(key) =>
				(input.channelStats.get(key)?.channelGroup ?? "unknown") === group,
		);
		const peerRevenue = peers.reduce(
			(sum, key) => sum + (input.channelStats.get(key)?.attributedRevenue ?? 0),
			0,
		);
		const peerFeature = peers.reduce(
			(sum, key) =>
				sum +
				input.observations.reduce(
					(subtotal, row) => subtotal + (row.features[key] ?? 0),
					0,
				),
			0,
		);
		groupPriorMeans.set(
			featureKey,
			peerFeature > 0
				? peerRevenue / Math.max(peerFeature, 1)
				: globalMediaPriorMean,
		);
	}

	for (const observation of input.observations) {
		const row = parameterNames.map((name) => {
			if (name === "intercept") {
				return 1;
			}
			return observation.features[name] ?? observation.controls[name] ?? 0;
		});
		for (let left = 0; left < size; left += 1) {
			vector[left] += row[left] * observation.revenue;
			for (let right = 0; right < size; right += 1) {
				matrix[left][right] += row[left] * row[right];
			}
		}
	}

	for (const [index, name] of parameterNames.entries()) {
		if (name === "intercept") {
			matrix[index][index] += 1e-6;
			continue;
		}

		if (input.mediaFeatureKeys.includes(name)) {
			const mediaPrecision = 1 / input.mediaPriorSd ** 2;
			const groupPrecision = 1 / input.groupPriorSd ** 2;
			matrix[index][index] += mediaPrecision + groupPrecision;
			vector[index] +=
				mediaPrecision * globalMediaPriorMean +
				groupPrecision * (groupPriorMeans.get(name) ?? globalMediaPriorMean);
			continue;
		}

		const controlPrecision = 1 / input.controlPriorSd ** 2;
		matrix[index][index] += controlPrecision;
	}

	const solved = solveLinearSystem(matrix, vector);
	const coefficients = Object.fromEntries(
		parameterNames.map((name, index) => [name, solved[index] ?? 0]),
	);
	const residualSse = input.observations.reduce((sum, observation) => {
		const error = observation.revenue - predict(observation, coefficients);
		return sum + error ** 2;
	}, 0);
	const residualSigma = Math.max(
		1e-6,
		Math.sqrt(residualSse / Math.max(1, input.observations.length - size)),
	);
	const covariance = invertMatrix(matrix).map((row) =>
		row.map((value) => value * residualSigma ** 2),
	);

	return {
		coefficients,
		covariance,
		residualSigma,
		parameterNames,
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
			let sum = 0;
			for (let index = 0; index < column; index += 1) {
				sum += (lower[row][index] ?? 0) * (lower[column][index] ?? 0);
			}

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

function samplePosterior(input: {
	fitted: FittedModel;
	inputHash: string;
	posteriorChains: number;
	posteriorDraws: number;
}) {
	const meanVector = input.fitted.parameterNames.map(
		(name) => input.fitted.coefficients[name] ?? 0,
	);
	const lower = cholesky(input.fitted.covariance);
	const drawsPerChain = Math.max(
		1,
		Math.floor(input.posteriorDraws / input.posteriorChains),
	);

	return Array.from({ length: input.posteriorChains }, (_entry, chainIndex) => {
		const random = createSeededRandom(`${input.inputHash}:${chainIndex}`);
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
			const effectiveSampleSize = chainCount * drawsPerChain;

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

function validationMetrics(
	observations: WeeklyObservation[],
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
		predicted: Math.max(0, predict(observation, coefficients)),
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

function buildContributionOutputs(input: {
	chains: number[][][];
	parameterNames: string[];
	mediaFeatureKeys: string[];
	observations: WeeklyObservation[];
	channelStats: Map<string, ChannelStats>;
}) {
	const draws = input.chains.flat();
	const parameterIndex = new Map(
		input.parameterNames.map((name, index) => [name, index]),
	);
	const totalFeatureByChannel = Object.fromEntries(
		input.mediaFeatureKeys.map((key) => [
			key,
			input.observations.reduce(
				(sum, observation) => sum + (observation.features[key] ?? 0),
				0,
			),
		]),
	);
	const contributionDraws = Object.fromEntries(
		input.mediaFeatureKeys.map((key) => {
			const index = parameterIndex.get(key) ?? 0;
			return [
				key,
				draws.map((draw) =>
					Math.max(0, (draw[index] ?? 0) * (totalFeatureByChannel[key] ?? 0)),
				),
			];
		}),
	);
	const totalMediaDraws = draws.map((_draw, drawIndex) =>
		input.mediaFeatureKeys.reduce(
			(sum, key) => sum + ((contributionDraws[key] ?? [])[drawIndex] ?? 0),
			0,
		),
	);

	return {
		totalActualRevenue: input.observations.reduce(
			(sum, observation) => sum + observation.revenue,
			0,
		),
		totalMediaContribution: summarizePosterior(totalMediaDraws),
		channels: input.mediaFeatureKeys.map((key) => {
			const stats = input.channelStats.get(key);
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
				channel: stats?.channel ?? "unknown",
				channelGroup: stats?.channelGroup ?? "unknown",
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

function buildWeeklyObservations(input: {
	rows: BayesianHierarchicalMmmV1FeatureRow[];
	selectedChannels: string[];
	adstockDecay: number;
	saturationHalfSaturation: number;
	saturationSlope: number;
}) {
	const weeks = Array.from(
		new Set(input.rows.map((row) => row.week_start_date)),
	).sort();
	const selectedChannelSet = new Set(input.selectedChannels);
	const modelChannels = [...input.selectedChannels, "__other_paid__"];
	const spendSeries = Object.fromEntries(
		modelChannels.map((key) => [key, weeks.map(() => 0)]),
	);
	const revenueByWeek = new Map<string, number>();

	for (const row of input.rows) {
		const weekIndex = weeks.indexOf(row.week_start_date);
		const rawKey = channelKey(row);
		const featureKey = selectedChannelSet.has(rawKey)
			? rawKey
			: "__other_paid__";
		spendSeries[featureKey][weekIndex] += toNumber(row.spend);
		revenueByWeek.set(
			row.week_start_date,
			(revenueByWeek.get(row.week_start_date) ?? 0) +
				toNumber(row.shopify_revenue),
		);
	}

	const transformedSeries = Object.fromEntries(
		Object.entries(spendSeries).map(([key, values]) => [
			key,
			hillSaturation(
				adstock(values, input.adstockDecay),
				input.saturationHalfSaturation,
				input.saturationSlope,
			),
		]),
	);

	return weeks
		.filter((week) => revenueByWeek.has(week))
		.map((week, weekIndex) => {
			const trend = weekIndex;
			const seasonalWeek = weekOfYear(week);
			return {
				weekStartDate: week,
				revenue: revenueByWeek.get(week) ?? 0,
				features: Object.fromEntries(
					modelChannels.map((key) => [
						key,
						transformedSeries[key][weekIndex] ?? 0,
					]),
				),
				controls: {
					trend,
					seasonalitySin52: Math.sin((2 * Math.PI * seasonalWeek) / 52),
					seasonalityCos52: Math.cos((2 * Math.PI * seasonalWeek) / 52),
				},
			};
		});
}

export function buildBayesianHierarchicalMmmArtifact(
	rows: BayesianHierarchicalMmmV1FeatureRow[],
	input: BayesianHierarchicalMmmTrainingInput,
): BayesianHierarchicalMmmModelRun {
	const startDate = normalizeDate(input.startDate, "startDate");
	const endDate = normalizeDate(input.endDate, "endDate");
	if (startDate > endDate) {
		throw new Error("startDate must be on or before endDate");
	}

	const attributionModel = input.attributionModel?.trim() || "last_touch";
	const filteredRows = rows.filter(
		(row) => row.attribution_model === attributionModel,
	);
	const failedRows = filteredRows.filter((row) => row.dq_status === "fail");
	if (failedRows.length > 0) {
		throw new Error(
			`Bayesian hierarchical MMM input contains failed quality rows: ${failedRows.length}`,
		);
	}

	const maxChannels = clampInteger(
		input.maxChannels,
		DEFAULT_MAX_CHANNELS,
		1,
		50,
	);
	const adstockDecay = clampNumber(
		input.adstockDecay,
		DEFAULT_ADSTOCK_DECAY,
		0,
		0.98,
	);
	const saturationHalfSaturation = clampNumber(
		input.saturationHalfSaturation,
		DEFAULT_SATURATION_HALF_SATURATION,
		1,
		1_000_000,
	);
	const saturationSlope = clampNumber(
		input.saturationSlope,
		DEFAULT_SATURATION_SLOPE,
		0.25,
		5,
	);
	const mediaPriorSd = clampNumber(
		input.mediaPriorSd,
		DEFAULT_MEDIA_PRIOR_SD,
		0.01,
		100,
	);
	const groupPriorSd = clampNumber(
		input.groupPriorSd,
		DEFAULT_GROUP_PRIOR_SD,
		0.01,
		100,
	);
	const controlPriorSd = clampNumber(
		input.controlPriorSd,
		DEFAULT_CONTROL_PRIOR_SD,
		0.01,
		1_000,
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
	const holdoutRatio = clampNumber(
		input.holdoutRatio,
		DEFAULT_HOLDOUT_RATIO,
		0,
		0.5,
	);

	const channelStats = new Map<string, ChannelStats>();
	for (const row of filteredRows) {
		const key = channelKey(row);
		const stats = addChannelStats(channelStats, key, row);
		stats.spend += toNumber(row.spend);
		stats.impressions += toNumber(row.impressions);
		stats.clicks += toNumber(row.clicks);
		stats.attributedRevenue += toNumber(row.attribution_credit_revenue);
		stats.attributedOrders += toNumber(row.attribution_credit_orders);
	}

	const selectedChannels = [...channelStats.values()]
		.filter((channel) => channel.spend > 0)
		.sort(
			(left, right) =>
				right.spend - left.spend || left.key.localeCompare(right.key),
		)
		.slice(0, maxChannels)
		.map((channel) => channel.key);
	const mediaFeatureKeys = [...selectedChannels, "__other_paid__"];
	const observations = buildWeeklyObservations({
		rows: filteredRows,
		selectedChannels,
		adstockDecay,
		saturationHalfSaturation,
		saturationSlope,
	});

	if (observations.length < MIN_WEEKLY_OBSERVATIONS) {
		throw new Error(
			`Bayesian hierarchical MMM requires at least ${MIN_WEEKLY_OBSERVATIONS} weekly observations`,
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
	const controlFeatureKeys = ["trend", "seasonalitySin52", "seasonalityCos52"];
	const fitted = fitHierarchicalRegression({
		observations: trainingObservations,
		mediaFeatureKeys,
		controlFeatureKeys,
		channelStats,
		mediaPriorSd,
		groupPriorSd,
		controlPriorSd,
	});
	const martInputHash = hashJson(filteredRows);
	const chains = samplePosterior({
		fitted,
		inputHash: martInputHash,
		posteriorChains,
		posteriorDraws,
	});
	const diagnostics = posteriorDiagnostics(chains);
	const contributionOutputs = buildContributionOutputs({
		chains,
		parameterNames: fitted.parameterNames,
		mediaFeatureKeys,
		observations,
		channelStats,
	});
	const posteriorCoefficients = Object.fromEntries(
		fitted.parameterNames.map((parameterName, parameterIndex) => [
			parameterName,
			summarizePosterior(
				chains.flat().map((draw) => draw[parameterIndex] ?? 0),
			),
		]),
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
	};
	if (posteriorSanityChecks.status !== "pass") {
		throw new Error(
			`Bayesian hierarchical MMM posterior sanity checks failed: maxRhat=${diagnostics.maxRhat.toFixed(3)}, minESS=${diagnostics.minEffectiveSampleSize.toFixed(0)}`,
		);
	}

	const totalAttributedRevenue = [...channelStats.values()].reduce(
		(sum, channel) => sum + channel.attributedRevenue,
		0,
	);
	const totalModeledMediaRevenue =
		contributionOutputs.totalMediaContribution.mean;
	const calibrationSegments = contributionOutputs.channels.map((channel) => ({
		key: channel.key,
		source: channel.source,
		medium: channel.medium,
		campaign: channel.campaign,
		channel: channel.channel,
		channelGroup: channel.channelGroup,
		spend: channel.spend,
		attributedRevenue: channel.attributedRevenue,
		modeledRevenue: channel.contribution.mean,
		calibrationRatio:
			channel.attributedRevenue > 0 && channel.contribution.mean > 0
				? channel.contribution.mean / channel.attributedRevenue
				: null,
	}));
	const calibrationReport = buildMmmCalibrationReportV1({
		attributionModel,
		deterministicAttributionUsage:
			"hierarchical_priors_and_calibration_diagnostics",
		totalDeterministicRevenue: totalAttributedRevenue,
		totalPosteriorMediaContribution: totalModeledMediaRevenue,
		segments: contributionOutputs.channels.map((channel) => ({
			key: channel.key,
			source: channel.source,
			medium: channel.medium,
			campaign: channel.campaign,
			channel: channel.channel,
			channelGroup: channel.channelGroup,
			spend: channel.spend,
			impressions: channel.impressions,
			clicks: channel.clicks,
			deterministicRevenue: channel.attributedRevenue,
			posteriorContributionMean: channel.contribution.mean,
			posteriorContributionShareMean: channel.contributionShare.mean,
			posteriorProbabilityPositive: channel.posteriorProbabilityPositive,
		})),
		governanceStatus: "passed",
	});
	const config = {
		attributionModel,
		inputContractVersion: BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION,
		maxChannels,
		priors: {
			mediaEffect: {
				distribution: "normal",
				mean: "global_media_effect_from_total_revenue_over_transformed_spend",
				sd: mediaPriorSd,
			},
			channelGroupEffect: {
				distribution: "normal",
				mean: "channel_group_deterministic_anchor_over_transformed_spend",
				sd: groupPriorSd,
			},
			trendAndSeasonality: {
				distribution: "normal",
				mean: 0,
				sd: controlPriorSd,
			},
		},
		featureTransform: {
			adstock: {
				type: "geometric",
				decay: adstockDecay,
			},
			saturation: {
				type: "hill",
				halfSaturation: saturationHalfSaturation,
				slope: saturationSlope,
			},
		},
		controls: {
			trend: "linear_week_index",
			seasonality: "annual_fourier_order_1_weekly",
		},
		posteriorEngine: "closed_form_hierarchical_gaussian_approximation_v1",
		holdoutRatio,
		responseVariable: "weekly_total_shopify_revenue_from_channel_mart_outcomes",
		calibrationUse:
			"deterministic attribution credit metrics inform hierarchical priors and calibration diagnostics, not direct channel labels",
		productionCalibrationBaseline: {
			version: MMM_DETERMINISTIC_BASELINE_VERSION,
			clickLookbackWindowDays: MMM_BASELINE_CLICK_LOOKBACK_DAYS,
			viewLookbackWindowDays: MMM_BASELINE_VIEW_LOOKBACK_DAYS,
			lookbackRules: ["30d_click", "7d_view"],
			productionAlignment: "enforced",
		},
	};
	const inputSummary = {
		rowCount: filteredRows.length,
		paidMediaRowCount: filteredRows.filter((row) => toNumber(row.spend) > 0)
			.length,
		attributionRowCount: filteredRows.filter(
			(row) => toNumber(row.attribution_credit_revenue) > 0,
		).length,
		warnCount: filteredRows.filter((row) => row.dq_status === "warn").length,
		failCount: failedRows.length,
		observationCount: observations.length,
		trainingObservationCount: trainingObservations.length,
		holdoutObservationCount: holdoutObservations.length,
		selectedChannels,
		otherPaidChannelCount: Math.max(
			0,
			channelStats.size - selectedChannels.length,
		),
		martInputHash,
	};

	return {
		id: null,
		modelType: BAYESIAN_HIERARCHICAL_MMM_MODEL_TYPE,
		modelVersion: BAYESIAN_HIERARCHICAL_MMM_MODEL_VERSION,
		martVersion: BAYESIAN_HIERARCHICAL_MMM_MART_VERSION,
		attributionModel,
		trainingStartDate: startDate,
		trainingEndDate: endDate,
		holdoutStartDate: holdoutObservations[0]?.weekStartDate ?? null,
		holdoutEndDate: holdoutObservations.at(-1)?.weekStartDate ?? null,
		runConfig: config,
		inputSummary,
		modelArtifact: {
			coefficients: fitted.coefficients,
			posteriorCoefficients,
			residualSigma: fitted.residualSigma,
			contributionOutputs,
			channels: calibrationSegments.map(
				({ key, source, medium, campaign, channel, channelGroup, spend }) => ({
					key,
					source,
					medium,
					campaign,
					channel,
					channelGroup,
					spend,
				}),
			),
		},
		calibrationReport,
		validationReport: {
			train: validationMetrics(trainingObservations, fitted.coefficients),
			holdout: validationMetrics(holdoutObservations, fitted.coefficients),
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

export async function trainBayesianHierarchicalMmmModelWithClient(
	client: PoolClient,
	input: BayesianHierarchicalMmmTrainingInput,
): Promise<BayesianHierarchicalMmmModelRun> {
	const startDate = normalizeDate(input.startDate, "startDate");
	const endDate = normalizeDate(input.endDate, "endDate");
	const attributionModel = input.attributionModel?.trim() || "last_touch";
	if (input.refreshMart ?? true) {
		await refreshWeeklyMmmChannelInputMartWithClient(client, {
			startDate,
			endDate,
			attributionModels: [attributionModel],
		});
	}

	const rows = await fetchBayesianHierarchicalMmmV1FeatureRowsWithClient(
		client,
		{
			startDate,
			endDate,
			attributionModels: [attributionModel],
		},
	);
	const run = buildBayesianHierarchicalMmmArtifact(rows, {
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
		],
	);
	const modelRunId = insertResult.rows[0]?.id;
	if (!modelRunId) {
		throw new Error(
			"Bayesian hierarchical MMM model run insert did not return an id",
		);
	}

	const snapshotRows = rows.map(
		({ input_contract_version: _version, ...row }) => row,
	);
	const snapshot = await snapshotWeeklyMmmInputRowsWithClient(
		client,
		modelRunId,
		snapshotRows,
	);
	const inputSummary = {
		...run.inputSummary,
		inputContractVersion: BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION,
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
		inputSummary,
	};
}

export async function trainBayesianHierarchicalMmmModel(
	input: BayesianHierarchicalMmmTrainingInput,
): Promise<BayesianHierarchicalMmmModelRun> {
	return withTransaction((client) =>
		trainBayesianHierarchicalMmmModelWithClient(client, input),
	);
}
