import type { PoolClient } from "pg";

import { env } from "../../config/env.js";
import { withTransaction } from "../../db/pool.js";
import { logError, logInfo } from "../../observability/index.js";

type SessionAttributionRetentionOptions = {
	batchSize?: number;
	maxBatches?: number;
	asOf?: Date;
	client?: PoolClient;
	emitLogs?: boolean;
};

export type SessionAttributionRetentionResult = {
	cutoffAt: string;
	ga4FallbackCutoffAt: string;
	batchSize: number;
	maxBatches: number;
	batchesRun: number;
	deletedGa4FallbackCandidates: number;
	deletedTouchEvents: number;
	deletedSessions: number;
	protectedSessionsSkipped: number;
	protectedTouchEventsSkipped: number;
};

type ProtectedCountRow = {
	protected_sessions: string;
	protected_touch_events: string;
};

const DEFAULT_RETENTION_BATCH_SIZE = 100;
const DEFAULT_RETENTION_MAX_BATCHES = 50;

function normalizePositiveInteger(
	value: number | undefined,
	fallback: number,
): number {
	if (!Number.isFinite(value)) {
		return fallback;
	}

	return Math.max(Math.trunc(value ?? fallback), 1);
}

function resolveCutoffAt(asOf: Date | undefined): Date {
	const referenceTime = asOf ? new Date(asOf) : new Date();
	referenceTime.setUTCDate(
		referenceTime.getUTCDate() - env.SESSION_ATTRIBUTION_RETENTION_DAYS,
	);
	return referenceTime;
}

function resolveGa4FallbackCutoffAt(asOf: Date | undefined): Date {
	const referenceTime = asOf ? new Date(asOf) : new Date();
	referenceTime.setUTCDate(
		referenceTime.getUTCDate() - env.GA4_FALLBACK_RETENTION_DAYS,
	);
	return referenceTime;
}

async function countProtectedRows(
	client: PoolClient,
	cutoffAt: Date,
): Promise<ProtectedCountRow> {
	const result = await client.query<ProtectedCountRow>(
		`
      SELECT
        (
          SELECT COUNT(*)::text
          FROM session_attribution_identities identities
          WHERE identities.retained_until < $1::timestamptz
            AND EXISTS (
              SELECT 1
              FROM order_attribution_links links
              WHERE links.roas_radar_session_id = identities.roas_radar_session_id
            )
        ) AS protected_sessions,
        (
          SELECT COUNT(*)::text
          FROM session_attribution_touch_events touch_events
          WHERE touch_events.retained_until < $1::timestamptz
            AND EXISTS (
              SELECT 1
              FROM order_attribution_links links
              WHERE links.roas_radar_session_id = touch_events.roas_radar_session_id
            )
        ) AS protected_touch_events
    `,
		[cutoffAt],
	);

	return (
		result.rows[0] ?? { protected_sessions: "0", protected_touch_events: "0" }
	);
}

async function deleteExpiredTouchEvents(
	client: PoolClient,
	cutoffAt: Date,
	batchSize: number,
): Promise<number> {
	const result = await client.query(
		`
      WITH expired_touch_events AS (
        SELECT touch_events.id
        FROM session_attribution_touch_events touch_events
        WHERE touch_events.retained_until < $1::timestamptz
          AND NOT EXISTS (
            SELECT 1
            FROM order_attribution_links links
            WHERE links.roas_radar_session_id = touch_events.roas_radar_session_id
          )
        ORDER BY touch_events.retained_until ASC, touch_events.id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      DELETE FROM session_attribution_touch_events touch_events
      USING expired_touch_events
      WHERE touch_events.id = expired_touch_events.id
    `,
		[cutoffAt, batchSize],
	);

	return result.rowCount ?? 0;
}

async function deleteExpiredSessions(
	client: PoolClient,
	cutoffAt: Date,
	batchSize: number,
): Promise<number> {
	const result = await client.query(
		`
      WITH expired_sessions AS (
        SELECT identities.roas_radar_session_id
        FROM session_attribution_identities identities
        WHERE identities.retained_until < $1::timestamptz
          AND NOT EXISTS (
            SELECT 1
            FROM order_attribution_links links
            WHERE links.roas_radar_session_id = identities.roas_radar_session_id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM session_attribution_touch_events touch_events
            WHERE touch_events.roas_radar_session_id = identities.roas_radar_session_id
          )
        ORDER BY identities.retained_until ASC, identities.roas_radar_session_id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      DELETE FROM session_attribution_identities identities
      USING expired_sessions
      WHERE identities.roas_radar_session_id = expired_sessions.roas_radar_session_id
    `,
		[cutoffAt, batchSize],
	);

	return result.rowCount ?? 0;
}

async function deleteExpiredGa4FallbackCandidates(
	client: PoolClient,
	cutoffAt: Date,
	batchSize: number,
): Promise<number> {
	const result = await client.query(
		`
      WITH expired_candidates AS (
        SELECT candidate_key, occurred_at
        FROM ga4_fallback_candidates
        WHERE retained_until < $1::timestamptz
        ORDER BY retained_until ASC, occurred_at ASC, candidate_key ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      DELETE FROM ga4_fallback_candidates candidates
      USING expired_candidates
      WHERE candidates.candidate_key = expired_candidates.candidate_key
        AND candidates.occurred_at = expired_candidates.occurred_at
    `,
		[cutoffAt, batchSize],
	);

	return result.rowCount ?? 0;
}

export async function runSessionAttributionRetention(
	options: SessionAttributionRetentionOptions = {},
): Promise<SessionAttributionRetentionResult> {
	const batchSize = normalizePositiveInteger(
		options.batchSize,
		DEFAULT_RETENTION_BATCH_SIZE,
	);
	const maxBatches = normalizePositiveInteger(
		options.maxBatches,
		DEFAULT_RETENTION_MAX_BATCHES,
	);
	const cutoffAt = resolveCutoffAt(options.asOf);
	const ga4FallbackCutoffAt = resolveGa4FallbackCutoffAt(options.asOf);
	const emitLogs = options.emitLogs ?? true;

	const runWithClient = async (client: PoolClient) => {
		const protectedCounts = await countProtectedRows(client, cutoffAt);

		let batchesRun = 0;
		let deletedGa4FallbackCandidates = 0;
		let deletedTouchEvents = 0;
		let deletedSessions = 0;

		for (let batchNumber = 1; batchNumber <= maxBatches; batchNumber += 1) {
			const deletedGa4FallbackCandidatesInBatch =
				await deleteExpiredGa4FallbackCandidates(
					client,
					ga4FallbackCutoffAt,
					batchSize,
				);
			const deletedTouchEventsInBatch = await deleteExpiredTouchEvents(
				client,
				cutoffAt,
				batchSize,
			);
			const deletedSessionsInBatch = await deleteExpiredSessions(
				client,
				cutoffAt,
				batchSize,
			);

			if (
				deletedGa4FallbackCandidatesInBatch === 0 &&
				deletedTouchEventsInBatch === 0 &&
				deletedSessionsInBatch === 0
			) {
				break;
			}

			batchesRun += 1;
			deletedGa4FallbackCandidates += deletedGa4FallbackCandidatesInBatch;
			deletedTouchEvents += deletedTouchEventsInBatch;
			deletedSessions += deletedSessionsInBatch;

			if (emitLogs) {
				logInfo("session_attribution_retention_batch_completed", {
					batchNumber,
					cutoffAt: cutoffAt.toISOString(),
					ga4FallbackCutoffAt: ga4FallbackCutoffAt.toISOString(),
					batchSize,
					deletedGa4FallbackCandidatesInBatch,
					deletedTouchEventsInBatch,
					deletedSessionsInBatch,
				});
			}
		}

		const result: SessionAttributionRetentionResult = {
			cutoffAt: cutoffAt.toISOString(),
			ga4FallbackCutoffAt: ga4FallbackCutoffAt.toISOString(),
			batchSize,
			maxBatches,
			batchesRun,
			deletedGa4FallbackCandidates,
			deletedTouchEvents,
			deletedSessions,
			protectedSessionsSkipped: Number(
				protectedCounts.protected_sessions ?? "0",
			),
			protectedTouchEventsSkipped: Number(
				protectedCounts.protected_touch_events ?? "0",
			),
		};

		if (emitLogs) {
			logInfo("session_attribution_retention_completed", result);
		}

		return result;
	};

	if (options.client) {
		return runWithClient(options.client);
	}

	const protectedCounts = await withTransaction(async (client) =>
		countProtectedRows(client, cutoffAt),
	);

	let batchesRun = 0;
	let deletedGa4FallbackCandidates = 0;
	let deletedTouchEvents = 0;
	let deletedSessions = 0;

	for (let batchNumber = 1; batchNumber <= maxBatches; batchNumber += 1) {
		const batchResult = await withTransaction(async (client) => {
			const deletedGa4FallbackCandidatesInBatch =
				await deleteExpiredGa4FallbackCandidates(
					client,
					ga4FallbackCutoffAt,
					batchSize,
				);
			const deletedTouchEventsInBatch = await deleteExpiredTouchEvents(
				client,
				cutoffAt,
				batchSize,
			);
			const deletedSessionsInBatch = await deleteExpiredSessions(
				client,
				cutoffAt,
				batchSize,
			);

			return {
				deletedGa4FallbackCandidatesInBatch,
				deletedTouchEventsInBatch,
				deletedSessionsInBatch,
			};
		});

		if (
			batchResult.deletedGa4FallbackCandidatesInBatch === 0 &&
			batchResult.deletedTouchEventsInBatch === 0 &&
			batchResult.deletedSessionsInBatch === 0
		) {
			break;
		}

		batchesRun += 1;
		deletedGa4FallbackCandidates +=
			batchResult.deletedGa4FallbackCandidatesInBatch;
		deletedTouchEvents += batchResult.deletedTouchEventsInBatch;
		deletedSessions += batchResult.deletedSessionsInBatch;

		if (emitLogs) {
			logInfo("session_attribution_retention_batch_completed", {
				batchNumber,
				cutoffAt: cutoffAt.toISOString(),
				ga4FallbackCutoffAt: ga4FallbackCutoffAt.toISOString(),
				batchSize,
				deletedGa4FallbackCandidatesInBatch:
					batchResult.deletedGa4FallbackCandidatesInBatch,
				deletedTouchEventsInBatch: batchResult.deletedTouchEventsInBatch,
				deletedSessionsInBatch: batchResult.deletedSessionsInBatch,
			});
		}
	}

	const result: SessionAttributionRetentionResult = {
		cutoffAt: cutoffAt.toISOString(),
		ga4FallbackCutoffAt: ga4FallbackCutoffAt.toISOString(),
		batchSize,
		maxBatches,
		batchesRun,
		deletedGa4FallbackCandidates,
		deletedTouchEvents,
		deletedSessions,
		protectedSessionsSkipped: Number(protectedCounts.protected_sessions ?? "0"),
		protectedTouchEventsSkipped: Number(
			protectedCounts.protected_touch_events ?? "0",
		),
	};

	if (emitLogs) {
		logInfo("session_attribution_retention_completed", result);
	}

	return result;
}

export async function runSessionAttributionRetentionJob(
	options: SessionAttributionRetentionOptions = {},
): Promise<SessionAttributionRetentionResult> {
	try {
		return await runSessionAttributionRetention(options);
	} catch (error) {
		logError("session_attribution_retention_failed", error, {
			batchSize: options.batchSize ?? DEFAULT_RETENTION_BATCH_SIZE,
			maxBatches: options.maxBatches ?? DEFAULT_RETENTION_MAX_BATCHES,
			hasCustomAsOf: Boolean(options.asOf),
		});
		throw error;
	}
}
