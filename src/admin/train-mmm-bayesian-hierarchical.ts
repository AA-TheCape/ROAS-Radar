import { randomUUID } from "node:crypto";

import { pool } from "../db/pool.js";
import { trainBayesianHierarchicalMmmModel } from "../modules/mmm/bayesian-hierarchical.js";
import { emitMmmBaselineJobLifecycleLog } from "../observability/index.js";

function readFlag(name: string): string | null {
	const prefixed = `--${name}`;
	const index = process.argv.indexOf(prefixed);
	if (index === -1) {
		return null;
	}

	return process.argv[index + 1] ?? null;
}

function readBooleanFlag(name: string): boolean | undefined {
	const value = readFlag(name)?.trim().toLowerCase();
	if (!value) {
		return undefined;
	}

	if (["1", "true", "yes"].includes(value)) {
		return true;
	}

	if (["0", "false", "no"].includes(value)) {
		return false;
	}

	throw new Error(`--${name} must be boolean`);
}

function readBooleanEnv(name: string): boolean | undefined {
	const value = process.env[name]?.trim().toLowerCase();
	if (!value) {
		return undefined;
	}

	if (["1", "true", "yes"].includes(value)) {
		return true;
	}

	if (["0", "false", "no"].includes(value)) {
		return false;
	}

	throw new Error(`${name} must be boolean`);
}

function readNumberFlag(name: string): number | undefined {
	const value = readFlag(name)?.trim();
	if (!value) {
		return undefined;
	}

	const numeric = Number(value);
	if (!Number.isFinite(numeric)) {
		throw new Error(`--${name} must be numeric`);
	}

	return numeric;
}

function readNumberEnv(name: string): number | undefined {
	const value = process.env[name]?.trim();
	if (!value) {
		return undefined;
	}

	const numeric = Number(value);
	if (!Number.isFinite(numeric)) {
		throw new Error(`${name} must be numeric`);
	}

	return numeric;
}

function toDateString(date: Date): string {
	return date.toISOString().slice(0, 10);
}

function addUtcDays(date: Date, days: number): Date {
	const next = new Date(date);
	next.setUTCDate(next.getUTCDate() + days);
	return next;
}

function resolveTrainingWindow() {
	const explicitStartDate =
		readFlag("start-date")?.trim() ||
		process.env.MMM_BAYESIAN_START_DATE?.trim();
	const explicitEndDate =
		readFlag("end-date")?.trim() || process.env.MMM_BAYESIAN_END_DATE?.trim();

	if (explicitStartDate && explicitEndDate) {
		return {
			startDate: explicitStartDate,
			endDate: explicitEndDate,
		};
	}

	const lookbackDays = readNumberEnv("MMM_BAYESIAN_LOOKBACK_DAYS");
	const lagDays = readNumberEnv("MMM_BAYESIAN_LAG_DAYS") ?? 1;
	if (lookbackDays && lookbackDays > 0) {
		const today = new Date();
		const endDate = addUtcDays(today, -lagDays);
		const startDate = addUtcDays(endDate, -(Math.trunc(lookbackDays) - 1));
		return {
			startDate: toDateString(startDate),
			endDate: toDateString(endDate),
		};
	}

	throw new Error(
		"Usage: npm run mmm:train-bayesian -- --start-date YYYY-MM-DD --end-date YYYY-MM-DD, or set MMM_BAYESIAN_LOOKBACK_DAYS",
	);
}

async function main() {
	const { startDate, endDate } = resolveTrainingWindow();
	const startedAt = new Date();
	const workerId = `mmm-bayesian-${randomUUID()}`;
	const requestedBy =
		readFlag("submitted-by")?.trim() ||
		process.env.MMM_BAYESIAN_SUBMITTED_BY?.trim() ||
		"admin-cli";
	const attributionModel =
		readFlag("attribution-model")?.trim() ||
		process.env.MMM_BAYESIAN_ATTRIBUTION_MODEL?.trim() ||
		undefined;

	emitMmmBaselineJobLifecycleLog({
		stage: "started",
		workerId,
		requestedBy,
		startedAt: startedAt.toISOString(),
		trainingStartDate: startDate,
		trainingEndDate: endDate,
		attributionModel: attributionModel ?? null,
	});

	try {
		const run = await trainBayesianHierarchicalMmmModel({
			startDate,
			endDate,
			attributionModel,
			refreshMart:
				readBooleanFlag("refresh-mart") ??
				readBooleanEnv("MMM_BAYESIAN_REFRESH_MART"),
			maxChannels:
				readNumberFlag("max-channels") ??
				readNumberEnv("MMM_BAYESIAN_MAX_CHANNELS"),
			adstockDecay:
				readNumberFlag("adstock-decay") ??
				readNumberEnv("MMM_BAYESIAN_ADSTOCK_DECAY"),
			saturationHalfSaturation:
				readNumberFlag("saturation-half-saturation") ??
				readNumberEnv("MMM_BAYESIAN_SATURATION_HALF_SATURATION"),
			saturationSlope:
				readNumberFlag("saturation-slope") ??
				readNumberEnv("MMM_BAYESIAN_SATURATION_SLOPE"),
			mediaPriorSd:
				readNumberFlag("media-prior-sd") ??
				readNumberEnv("MMM_BAYESIAN_MEDIA_PRIOR_SD"),
			groupPriorSd:
				readNumberFlag("group-prior-sd") ??
				readNumberEnv("MMM_BAYESIAN_GROUP_PRIOR_SD"),
			controlPriorSd:
				readNumberFlag("control-prior-sd") ??
				readNumberEnv("MMM_BAYESIAN_CONTROL_PRIOR_SD"),
			posteriorDraws:
				readNumberFlag("posterior-draws") ??
				readNumberEnv("MMM_BAYESIAN_POSTERIOR_DRAWS"),
			posteriorChains:
				readNumberFlag("posterior-chains") ??
				readNumberEnv("MMM_BAYESIAN_POSTERIOR_CHAINS"),
			posteriorWarmupDraws:
				readNumberFlag("posterior-warmup-draws") ??
				readNumberEnv("MMM_BAYESIAN_POSTERIOR_WARMUP_DRAWS"),
			holdoutRatio:
				readNumberFlag("holdout-ratio") ??
				readNumberEnv("MMM_BAYESIAN_HOLDOUT_RATIO"),
			randomSeed:
				readFlag("random-seed")?.trim() ||
				process.env.MMM_BAYESIAN_RANDOM_SEED?.trim() ||
				undefined,
			submittedBy: requestedBy,
		});
		const completedAt = new Date();
		const holdout = run.validationReport.holdout as
			| { mape?: unknown; rmse?: unknown }
			| undefined;

		emitMmmBaselineJobLifecycleLog({
			stage: "completed",
			workerId,
			requestedBy,
			startedAt: startedAt.toISOString(),
			completedAt: completedAt.toISOString(),
			durationMs: completedAt.getTime() - startedAt.getTime(),
			trainingStartDate: run.trainingStartDate,
			trainingEndDate: run.trainingEndDate,
			attributionModel: run.attributionModel,
			modelRunId: run.id,
			modelVersion: run.modelVersion,
			martVersion: run.martVersion,
			inputRowCount: Number(run.inputSummary.rowCount ?? 0),
			paidMediaRowCount: Number(run.inputSummary.paidMediaRowCount ?? 0),
			observationCount: Number(run.inputSummary.observationCount ?? 0),
			holdoutMape: typeof holdout?.mape === "number" ? holdout.mape : null,
			holdoutRmse: typeof holdout?.rmse === "number" ? holdout.rmse : null,
			governanceStatus:
				typeof run.calibrationReport.governanceStatus === "string"
					? run.calibrationReport.governanceStatus
					: null,
			divergenceAlertCount: 0,
			maxDivergenceRate: 0,
		});

		process.stdout.write(
			`${JSON.stringify(
				{
					id: run.id,
					modelVersion: run.modelVersion,
					attributionModel: run.attributionModel,
					trainingStartDate: run.trainingStartDate,
					trainingEndDate: run.trainingEndDate,
					inputSummary: run.inputSummary,
					calibrationReport: run.calibrationReport,
					validationReport: run.validationReport,
				},
				null,
				2,
			)}\n`,
		);
	} catch (error) {
		const completedAt = new Date();
		emitMmmBaselineJobLifecycleLog({
			stage: "failed",
			workerId,
			requestedBy,
			startedAt: startedAt.toISOString(),
			completedAt: completedAt.toISOString(),
			durationMs: completedAt.getTime() - startedAt.getTime(),
			trainingStartDate: startDate,
			trainingEndDate: endDate,
			attributionModel: attributionModel ?? null,
			error,
		});
		throw error;
	}
}

main()
	.catch((error) => {
		process.stderr.write(
			`${error instanceof Error ? error.stack : String(error)}\n`,
		);
		process.exitCode = 1;
	})
	.finally(async () => {
		await pool.end();
	});
