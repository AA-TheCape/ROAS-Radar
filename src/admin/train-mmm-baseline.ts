import { randomUUID } from "node:crypto";

import { pool } from "../db/pool.js";
import {
	MMM_BASELINE_MODEL_VERSION,
	trainBaselineMmmModel,
} from "../modules/mmm/baseline.js";
import { emitMmmBaselineJobLifecycleLog } from "../observability/index.js";

function readFlag(name: string): string | null {
	const prefixed = `--${name}`;
	const index = process.argv.indexOf(prefixed);
	if (index === -1) {
		return null;
	}

	return process.argv[index + 1] ?? null;
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

function hasFlag(name: string): boolean {
	return process.argv.includes(`--${name}`);
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
		process.env.MMM_BASELINE_START_DATE?.trim();
	const explicitEndDate =
		readFlag("end-date")?.trim() || process.env.MMM_BASELINE_END_DATE?.trim();

	if (explicitStartDate && explicitEndDate) {
		return {
			startDate: explicitStartDate,
			endDate: explicitEndDate,
		};
	}

	const lookbackDays = readNumberEnv("MMM_BASELINE_LOOKBACK_DAYS");
	const lagDays = readNumberEnv("MMM_BASELINE_LAG_DAYS") ?? 1;
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
		"Usage: npm run mmm:train-baseline -- --start-date YYYY-MM-DD --end-date YYYY-MM-DD, or set MMM_BASELINE_LOOKBACK_DAYS",
	);
}

function requireRuntimeConfig(input: {
	approvedFreezeId?: string;
}) {
	if (!process.env.DATABASE_URL?.trim()) {
		throw new Error("baseline_linear_mmm_v1 training requires DATABASE_URL");
	}

	if (!input.approvedFreezeId?.trim()) {
		throw new Error(
			"baseline_linear_mmm_v1 training requires --freeze-id or MMM_BASELINE_FREEZE_ID",
		);
	}
}

async function main() {
	const { startDate, endDate } = resolveTrainingWindow();
	const startedAt = new Date();
	const workerId = `mmm-baseline-${randomUUID()}`;
	const requestedBy =
		readFlag("submitted-by")?.trim() ||
		process.env.MMM_BASELINE_SUBMITTED_BY?.trim() ||
		"admin-cli";
	const attributionModel =
		readFlag("attribution-model")?.trim() ||
		process.env.MMM_BASELINE_ATTRIBUTION_MODEL?.trim() ||
		undefined;
	const approvedFreezeId =
		readFlag("freeze-id")?.trim() || process.env.MMM_BASELINE_FREEZE_ID?.trim();

	requireRuntimeConfig({
		approvedFreezeId,
	});

	if (hasFlag("validate-config")) {
		process.stdout.write(
			`${JSON.stringify({
				ok: true,
				command: "mmm:train-baseline:start",
				modelVersion: MMM_BASELINE_MODEL_VERSION,
				startDate,
				endDate,
				attributionModel: attributionModel ?? null,
				approvedFreezeId,
			})}\n`,
		);
		return;
	}

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
		const run = await trainBaselineMmmModel({
			startDate,
			endDate,
			attributionModel,
			maxSegments:
				readNumberFlag("max-segments") ??
				readNumberEnv("MMM_BASELINE_MAX_SEGMENTS"),
			adstockDecay:
				readNumberFlag("adstock-decay") ??
				readNumberEnv("MMM_BASELINE_ADSTOCK_DECAY"),
			ridgeLambda:
				readNumberFlag("ridge-lambda") ??
				readNumberEnv("MMM_BASELINE_RIDGE_LAMBDA"),
			posteriorDraws:
				readNumberFlag("posterior-draws") ??
				readNumberEnv("MMM_BASELINE_POSTERIOR_DRAWS"),
			posteriorChains:
				readNumberFlag("posterior-chains") ??
				readNumberEnv("MMM_BASELINE_POSTERIOR_CHAINS"),
			holdoutRatio:
				readNumberFlag("holdout-ratio") ??
				readNumberEnv("MMM_BASELINE_HOLDOUT_RATIO"),
			calibrationWarnDivergenceRate:
				readNumberFlag("calibration-warn-divergence-rate") ??
				readNumberEnv("MMM_BASELINE_CALIBRATION_WARN_DIVERGENCE_RATE"),
			calibrationAlertDivergenceRate:
				readNumberFlag("calibration-alert-divergence-rate") ??
				readNumberEnv("MMM_BASELINE_CALIBRATION_ALERT_DIVERGENCE_RATE"),
			approvedFreezeId,
			submittedBy: requestedBy,
		});
		const completedAt = new Date();
		const divergenceAlerts = Array.isArray(
			run.calibrationReport.divergenceAlerts,
		)
			? run.calibrationReport.divergenceAlerts
			: [];
		const maxDivergenceRate = divergenceAlerts.reduce((max, alert) => {
			if (!alert || typeof alert !== "object" || !("divergenceRate" in alert)) {
				return max;
			}

			const rate = Number(
				(alert as { divergenceRate?: unknown }).divergenceRate,
			);
			return Number.isFinite(rate) ? Math.max(max, rate) : max;
		}, 0);
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
			divergenceAlertCount: divergenceAlerts.length,
			maxDivergenceRate,
		});

		process.stdout.write(
			`${JSON.stringify(
				{
					id: run.id,
					approvedFreezeId: run.approvedFreezeId,
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
