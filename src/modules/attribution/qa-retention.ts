import type { PoolClient } from "pg";

import { env } from "../../config/env.js";
import { withTransaction } from "../../db/pool.js";
import { logError, logInfo } from "../../observability/index.js";

type AttributionQaRetentionOptions = {
	retentionDays?: number;
	batchSize?: number;
	maxBatches?: number;
	asOf?: Date;
	client?: PoolClient;
	emitLogs?: boolean;
};

export type AttributionQaRetentionResult = {
	cutoffAt: string;
	retentionDays: number;
	batchSize: number;
	maxBatches: number;
	batchesRun: number;
	deletedRawEvidence: number;
	prunedSnapshots: number;
};

const DEFAULT_RETENTION_DAYS = 30;

function normalizePositiveInteger(
	value: number | undefined,
	fallback: number,
): number {
	if (!Number.isFinite(value)) {
		return fallback;
	}

	return Math.max(Math.trunc(value ?? fallback), 1);
}

function resolveCutoffAt(asOf: Date | undefined, retentionDays: number): Date {
	const referenceTime = asOf ? new Date(asOf) : new Date();
	referenceTime.setUTCDate(referenceTime.getUTCDate() - retentionDays);
	return referenceTime;
}

async function deleteExpiredRawEvidence(
	client: PoolClient,
	cutoffAt: Date,
	batchSize: number,
): Promise<number> {
	const result = await client.query(
		`
			WITH expired_evidence AS (
				SELECT id
				FROM attribution_raw_evidence
				WHERE retained_until < $1::timestamptz
				ORDER BY retained_until ASC, id ASC
				LIMIT $2
				FOR UPDATE SKIP LOCKED
			)
			DELETE FROM attribution_raw_evidence evidence
			USING expired_evidence
			WHERE evidence.id = expired_evidence.id
		`,
		[cutoffAt, batchSize],
	);

	return result.rowCount ?? 0;
}

async function pruneExpiredQaSnapshots(
	client: PoolClient,
	cutoffAt: Date,
	batchSize: number,
): Promise<number> {
	const result = await client.query(
		`
			WITH expired_snapshots AS (
				SELECT id
				FROM shopify_orders
				WHERE attribution_snapshot_updated_at < $1::timestamptz
					AND attribution_snapshot ? 'qaSnapshot'
				ORDER BY attribution_snapshot_updated_at ASC, id ASC
				LIMIT $2
				FOR UPDATE SKIP LOCKED
			)
			UPDATE shopify_orders orders
			SET
				attribution_snapshot = orders.attribution_snapshot - 'qaSnapshot',
				attribution_snapshot_updated_at = orders.attribution_snapshot_updated_at
			FROM expired_snapshots
			WHERE orders.id = expired_snapshots.id
		`,
		[cutoffAt, batchSize],
	);

	return result.rowCount ?? 0;
}

export async function runAttributionQaRetention(
	options: AttributionQaRetentionOptions = {},
): Promise<AttributionQaRetentionResult> {
	const retentionDays = normalizePositiveInteger(
		options.retentionDays,
		env.ATTRIBUTION_QA_RETENTION_DAYS || DEFAULT_RETENTION_DAYS,
	);
	const batchSize = normalizePositiveInteger(
		options.batchSize,
		env.ATTRIBUTION_QA_RETENTION_BATCH_SIZE,
	);
	const maxBatches = normalizePositiveInteger(
		options.maxBatches,
		env.ATTRIBUTION_QA_RETENTION_MAX_BATCHES,
	);
	const cutoffAt = resolveCutoffAt(options.asOf, retentionDays);
	const emitLogs = options.emitLogs ?? true;

	let batchesRun = 0;
	let deletedRawEvidence = 0;
	let prunedSnapshots = 0;

	const runBatch = async (client: PoolClient) => {
		const deletedRawEvidenceInBatch = await deleteExpiredRawEvidence(
			client,
			cutoffAt,
			batchSize,
		);
		const prunedSnapshotsInBatch = await pruneExpiredQaSnapshots(
			client,
			cutoffAt,
			batchSize,
		);

		return { deletedRawEvidenceInBatch, prunedSnapshotsInBatch };
	};

	for (let batchNumber = 1; batchNumber <= maxBatches; batchNumber += 1) {
		const batchResult = options.client
			? await runBatch(options.client)
			: await withTransaction(runBatch);

		if (
			batchResult.deletedRawEvidenceInBatch === 0 &&
			batchResult.prunedSnapshotsInBatch === 0
		) {
			break;
		}

		batchesRun += 1;
		deletedRawEvidence += batchResult.deletedRawEvidenceInBatch;
		prunedSnapshots += batchResult.prunedSnapshotsInBatch;

		if (emitLogs) {
			logInfo("attribution_qa_retention_batch_completed", {
				service: process.env.K_SERVICE ?? "roas-radar-attribution-qa-retention",
				batchNumber,
				cutoffAt: cutoffAt.toISOString(),
				retentionDays,
				batchSize,
				deletedRawEvidenceInBatch: batchResult.deletedRawEvidenceInBatch,
				prunedSnapshotsInBatch: batchResult.prunedSnapshotsInBatch,
				cleanupDeletionCount:
					batchResult.deletedRawEvidenceInBatch +
					batchResult.prunedSnapshotsInBatch,
			});
		}
	}

	const result: AttributionQaRetentionResult = {
		cutoffAt: cutoffAt.toISOString(),
		retentionDays,
		batchSize,
		maxBatches,
		batchesRun,
		deletedRawEvidence,
		prunedSnapshots,
	};

	if (emitLogs) {
		logInfo("attribution_qa_retention_completed", {
			service: process.env.K_SERVICE ?? "roas-radar-attribution-qa-retention",
			...result,
			cleanupDeletionCount: deletedRawEvidence + prunedSnapshots,
		});
	}

	return result;
}

export async function runAttributionQaRetentionJob(
	options: AttributionQaRetentionOptions = {},
): Promise<AttributionQaRetentionResult> {
	try {
		return await runAttributionQaRetention(options);
	} catch (error) {
		logError("attribution_qa_retention_failed", error, {
			retentionDays:
				options.retentionDays ??
				env.ATTRIBUTION_QA_RETENTION_DAYS ??
				DEFAULT_RETENTION_DAYS,
			batchSize: options.batchSize ?? env.ATTRIBUTION_QA_RETENTION_BATCH_SIZE,
			maxBatches:
				options.maxBatches ?? env.ATTRIBUTION_QA_RETENTION_MAX_BATCHES,
			hasCustomAsOf: Boolean(options.asOf),
		});
		throw error;
	}
}
