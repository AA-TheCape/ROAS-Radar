import { randomUUID } from "node:crypto";

import { pool } from "../db/pool.js";
import { createBaselineCalibrationFreeze } from "../modules/mmm/baseline.js";

function readFlag(name: string): string | null {
	const prefixed = `--${name}`;
	const index = process.argv.indexOf(prefixed);
	if (index === -1) {
		return null;
	}

	return process.argv[index + 1] ?? null;
}

function resolveTrainingWindow() {
	const startDate =
		readFlag("start-date")?.trim() ||
		process.env.MMM_BASELINE_START_DATE?.trim();
	const endDate =
		readFlag("end-date")?.trim() || process.env.MMM_BASELINE_END_DATE?.trim();
	if (!startDate || !endDate) {
		throw new Error(
			"Usage: npm run mmm:freeze-baseline -- --start-date YYYY-MM-DD --end-date YYYY-MM-DD",
		);
	}

	return { startDate, endDate };
}

async function main() {
	const { startDate, endDate } = resolveTrainingWindow();
	const requestedBy =
		readFlag("submitted-by")?.trim() ||
		process.env.MMM_BASELINE_SUBMITTED_BY?.trim() ||
		`admin-cli-${randomUUID()}`;
	const attributionModel =
		readFlag("attribution-model")?.trim() ||
		process.env.MMM_BASELINE_ATTRIBUTION_MODEL?.trim() ||
		undefined;
	const freezeStatus =
		readFlag("status")?.trim() ||
		process.env.MMM_BASELINE_FREEZE_STATUS?.trim() ||
		"pending";
	if (!["pending", "approved", "rejected"].includes(freezeStatus)) {
		throw new Error("--status must be pending, approved, or rejected");
	}

	const freeze = await createBaselineCalibrationFreeze({
		startDate,
		endDate,
		attributionModel,
		freezeStatus: freezeStatus as "pending" | "approved" | "rejected",
		submittedBy: requestedBy,
		approvedBy:
			readFlag("approved-by")?.trim() ||
			process.env.MMM_BASELINE_FREEZE_APPROVED_BY?.trim(),
	});

	process.stdout.write(
		`${JSON.stringify(
			{
				id: freeze.id,
				freezeStatus: freeze.freezeStatus,
				freezeSchemaVersion: freeze.freezeSchemaVersion,
				martVersion: freeze.martVersion,
				snapshotVersion: freeze.snapshotVersion,
				generationTimestamp: freeze.generationTimestamp,
				calibrationStartDate: freeze.calibrationStartDate,
				calibrationEndDate: freeze.calibrationEndDate,
				attributionModel: freeze.attributionModel,
				rowCounts: freeze.rowCounts,
				deterministicAttributionCoverage:
					freeze.deterministicAttributionCoverage,
				freshnessMetrics: freeze.freshnessMetrics,
				campaignMetadataCoverage: freeze.campaignMetadataCoverage,
				exposureCoverage: freeze.exposureCoverage,
				dataQualityChecks: freeze.dataQualityChecks,
				aggregateMetricTotals: freeze.aggregateMetricTotals,
				evidenceHash: freeze.evidenceHash,
				approvedBy: freeze.approvedBy,
				approvedAt: freeze.approvedAt,
			},
			null,
			2,
		)}\n`,
	);
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
