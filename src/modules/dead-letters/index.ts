import type { PoolClient, QueryResultRow } from "pg";

import { env } from "../../config/env.js";
import { query, withTransaction } from "../../db/pool.js";
import { logError, logInfo } from "../../observability/index.js";

export type DeadLetterStatus = "pending_replay" | "replayed";

const DEFAULT_DEAD_LETTER_REPLAY_MAX_BATCH_SIZE = 100;

type Queryable = Pick<PoolClient, "query">;

type SerializedError = {
	message: string;
	context: Record<string, unknown>;
};

type DeadLetterSourceTable =
	| "shopify_order_writeback_jobs"
	| "attribution_jobs"
	| "ga4_bigquery_hourly_jobs";

type DeadLetterRow = {
	id: number;
	event_type: string;
	source_table: string;
	source_record_id: string;
	source_queue_key: string | null;
};

type ReplayFiltersInput = {
	requestedBy?: string | null;
	eventType?: string | null;
	sourceTable?: string | null;
	status?: DeadLetterStatus;
	fromTime?: Date | null;
	toTime?: Date | null;
	windowStart?: Date | null;
	windowEnd?: Date | null;
	limit?: number;
	dryRun?: boolean;
};

type NormalizedReplayFilters = {
	requestedBy: string | null;
	eventType: string | null;
	sourceTable: string | null;
	status: DeadLetterStatus;
	fromTime: Date | null;
	toTime: Date | null;
	limit: number;
	dryRun: boolean;
};

type InsertReplayRunItemInput = {
	replayRunId: number;
	deadLetter: DeadLetterRow;
	outcome: "replayed" | "skipped" | "failed" | "dry_run";
	message: string;
};

type RecordDeadLetterInput = {
	eventType: string;
	sourceTable: string;
	sourceRecordId: string;
	sourceQueueKey?: string | null;
	payload?: Record<string, unknown>;
	error: unknown;
};

type ReplayRunIdRow = QueryResultRow & { id: number };
type CountRow = QueryResultRow & { total: string };

function serializeError(error: unknown): SerializedError {
	if (error instanceof Error) {
		const { name, message, stack, ...extraContext } = error as Error &
			Record<string, unknown>;
		return {
			message,
			context: {
				name,
				message,
				stack,
				...extraContext,
			},
		};
	}

	if (typeof error === "object" && error !== null) {
		const context = error as Record<string, unknown>;
		return {
			message: String(context.message ?? "Unknown error"),
			context,
		};
	}

	return {
		message: String(error),
		context: { value: error },
	};
}

async function ensureDeadLetterTables(client: Queryable): Promise<void> {
	await client.query(`
    CREATE TABLE IF NOT EXISTS event_dead_letters (
      id bigserial PRIMARY KEY,
      event_type text NOT NULL,
      source_table text NOT NULL,
      source_record_id text NOT NULL,
      source_queue_key text,
      status text NOT NULL DEFAULT 'pending_replay',
      first_failed_at timestamptz NOT NULL DEFAULT now(),
      last_failed_at timestamptz NOT NULL DEFAULT now(),
      last_error_message text,
      payload jsonb NOT NULL DEFAULT '{}'::jsonb,
      error_context jsonb NOT NULL DEFAULT '{}'::jsonb,
      failure_count integer NOT NULL DEFAULT 1,
      replayed_at timestamptz,
      last_replay_run_id bigint,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
	await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS event_dead_letters_source_uidx
      ON event_dead_letters (event_type, source_table, source_record_id)
  `);
	await client.query(`
    CREATE TABLE IF NOT EXISTS event_replay_runs (
      id bigserial PRIMARY KEY,
      replay_scope text NOT NULL DEFAULT 'filtered',
      event_type text,
      source_table text,
      window_start timestamptz,
      window_end timestamptz,
      requested_by text,
      dry_run boolean NOT NULL DEFAULT false,
      requested_at timestamptz NOT NULL DEFAULT now(),
      filters jsonb NOT NULL DEFAULT '{}'::jsonb,
      candidate_count integer NOT NULL DEFAULT 0,
      replayed_count integer NOT NULL DEFAULT 0,
      skipped_count integer NOT NULL DEFAULT 0,
      failed_count integer NOT NULL DEFAULT 0,
      dry_run_count integer NOT NULL DEFAULT 0,
      results jsonb NOT NULL DEFAULT '{}'::jsonb,
      started_at timestamptz NOT NULL DEFAULT now(),
      completed_at timestamptz
    )
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS replay_scope text NOT NULL DEFAULT 'filtered'
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS event_type text
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS source_table text
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS window_start timestamptz
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS window_end timestamptz
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS requested_by text
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS filters jsonb NOT NULL DEFAULT '{}'::jsonb
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS dry_run boolean NOT NULL DEFAULT false
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS requested_at timestamptz NOT NULL DEFAULT now()
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS candidate_count integer NOT NULL DEFAULT 0
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS replayed_count integer NOT NULL DEFAULT 0
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS skipped_count integer NOT NULL DEFAULT 0
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS failed_count integer NOT NULL DEFAULT 0
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS dry_run_count integer NOT NULL DEFAULT 0
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS results jsonb NOT NULL DEFAULT '{}'::jsonb
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS started_at timestamptz NOT NULL DEFAULT now()
  `);
	await client.query(`
    ALTER TABLE event_replay_runs
      ADD COLUMN IF NOT EXISTS completed_at timestamptz
  `);
	await client.query(`
    CREATE TABLE IF NOT EXISTS event_replay_run_items (
      id bigserial PRIMARY KEY,
      replay_run_id bigint NOT NULL REFERENCES event_replay_runs(id) ON DELETE CASCADE,
      dead_letter_id bigint NOT NULL REFERENCES event_dead_letters(id) ON DELETE CASCADE,
      source_table text NOT NULL,
      source_record_id text NOT NULL,
      outcome text NOT NULL,
      message text,
      created_at timestamptz NOT NULL DEFAULT now()
    )
  `);
	await client.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS source_table text
  `);
	await client.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS source_record_id text
  `);
	await client.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS outcome text
  `);
	await client.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS message text
  `);
	await client.query(`
    ALTER TABLE event_replay_run_items
      ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now()
  `);
}

async function requeueSourceRecord(
	client: Queryable,
	deadLetter: DeadLetterRow,
): Promise<"replayed" | "skipped"> {
	if (
		(deadLetter.source_table as DeadLetterSourceTable) ===
		"shopify_order_writeback_jobs"
	) {
		const result = await client.query(
			`
        UPDATE shopify_order_writeback_jobs
        SET
          status = 'pending',
          available_at = now(),
          locked_at = NULL,
          locked_by = NULL,
          last_error = NULL,
          dead_lettered_at = NULL,
          updated_at = now()
        WHERE id = $1::bigint
      `,
			[deadLetter.source_record_id],
		);
		return result.rowCount ? "replayed" : "skipped";
	}

	if (
		(deadLetter.source_table as DeadLetterSourceTable) === "attribution_jobs"
	) {
		const result = await client.query(
			`
        UPDATE attribution_jobs
        SET
          status = 'pending',
          available_at = now(),
          locked_at = NULL,
          locked_by = NULL,
          last_error = NULL,
          dead_lettered_at = NULL,
          updated_at = now()
        WHERE id = $1::bigint
      `,
			[deadLetter.source_record_id],
		);
		return result.rowCount ? "replayed" : "skipped";
	}

	if (
		(deadLetter.source_table as DeadLetterSourceTable) ===
		"ga4_bigquery_hourly_jobs"
	) {
		const result = await client.query(
			`
        UPDATE ga4_bigquery_hourly_jobs
        SET
          status = 'pending',
          available_at = now(),
          locked_at = NULL,
          locked_by = NULL,
          last_error = NULL,
          dead_lettered_at = NULL,
          updated_at = now()
        WHERE pipeline_name = $1
          AND hour_start = $2::timestamptz
      `,
			[deadLetter.source_queue_key, deadLetter.source_record_id],
		);
		return result.rowCount ? "replayed" : "skipped";
	}

	return "skipped";
}

function normalizeReplayFilters(
	filters: ReplayFiltersInput,
): NormalizedReplayFilters {
	const requestedBy = filters.requestedBy?.trim() || null;
	const fromTime = filters.fromTime ?? filters.windowStart ?? null;
	const toTime = filters.toTime ?? filters.windowEnd ?? null;
	const limit = Math.max(
		1,
		Math.min(filters.limit ?? DEFAULT_DEAD_LETTER_REPLAY_MAX_BATCH_SIZE, 500),
	);
	const status = filters.status ?? "pending_replay";
	const dryRun = filters.dryRun ?? false;

	return {
		requestedBy,
		eventType: filters.eventType ?? null,
		sourceTable: filters.sourceTable ?? null,
		status,
		fromTime,
		toTime,
		limit,
		dryRun,
	};
}

async function insertReplayRunItem(
	client: Queryable,
	input: InsertReplayRunItemInput,
): Promise<void> {
	await client.query(
		`
      INSERT INTO event_replay_run_items (
        replay_run_id,
        dead_letter_id,
        source_table,
        source_record_id,
        outcome,
        message
      )
      VALUES ($1, $2, $3, $4, $5, $6)
    `,
		[
			input.replayRunId,
			input.deadLetter.id,
			input.deadLetter.source_table,
			input.deadLetter.source_record_id,
			input.outcome,
			input.message,
		],
	);
}

export async function recordDeadLetter(
	client: Queryable,
	input: RecordDeadLetterInput,
): Promise<void> {
	await ensureDeadLetterTables(client);
	const serializedError = serializeError(input.error);

	await client.query(
		`
      INSERT INTO event_dead_letters (
        event_type,
        source_table,
        source_record_id,
        source_queue_key,
        status,
        first_failed_at,
        last_failed_at,
        last_error_message,
        payload,
        error_context,
        failure_count,
        replayed_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        'pending_replay',
        now(),
        now(),
        $5,
        $6::jsonb,
        $7::jsonb,
        1,
        NULL,
        now()
      )
      ON CONFLICT (event_type, source_table, source_record_id)
      DO UPDATE SET
        source_queue_key = COALESCE(EXCLUDED.source_queue_key, event_dead_letters.source_queue_key),
        status = 'pending_replay',
        last_failed_at = now(),
        last_error_message = EXCLUDED.last_error_message,
        payload = EXCLUDED.payload,
        error_context = EXCLUDED.error_context,
        failure_count = event_dead_letters.failure_count + 1,
        replayed_at = NULL,
        updated_at = now()
    `,
		[
			input.eventType,
			input.sourceTable,
			input.sourceRecordId,
			input.sourceQueueKey ?? null,
			serializedError.message,
			JSON.stringify(input.payload ?? {}),
			JSON.stringify(serializedError.context),
		],
	);
}

export async function replayDeadLetters(filters: ReplayFiltersInput) {
	return withTransaction(async (client) => {
		await ensureDeadLetterTables(client);
		const normalized = normalizeReplayFilters(filters);
		const replayRunResult = await client.query<ReplayRunIdRow>(
			`
        INSERT INTO event_replay_runs (
          replay_scope,
          event_type,
          source_table,
          window_start,
          window_end,
          requested_by,
          dry_run,
          requested_at,
          filters,
          started_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8::jsonb, now())
        RETURNING id
      `,
			[
				normalized.eventType ||
				normalized.sourceTable ||
				normalized.fromTime ||
				normalized.toTime
					? "filtered"
					: "all_pending",
				normalized.eventType,
				normalized.sourceTable,
				normalized.fromTime,
				normalized.toTime,
				normalized.requestedBy,
				normalized.dryRun,
				JSON.stringify({
					requestedBy: normalized.requestedBy,
					eventType: normalized.eventType,
					sourceTable: normalized.sourceTable,
					status: normalized.status,
					fromTime: normalized.fromTime?.toISOString() ?? null,
					toTime: normalized.toTime?.toISOString() ?? null,
					limit: normalized.limit,
					dryRun: normalized.dryRun,
				}),
			],
		);
		const replayRunId = replayRunResult.rows[0].id;

		const candidates = await client.query<DeadLetterRow>(
			`
        SELECT
          id,
          event_type,
          source_table,
          source_record_id,
          source_queue_key
        FROM event_dead_letters
        WHERE status = $1
          AND ($2::text IS NULL OR event_type = $2)
          AND ($3::text IS NULL OR source_table = $3)
          AND ($4::timestamptz IS NULL OR last_failed_at >= $4)
          AND ($5::timestamptz IS NULL OR last_failed_at <= $5)
        ORDER BY last_failed_at ASC, id ASC
        LIMIT $6
        FOR UPDATE SKIP LOCKED
      `,
			[
				normalized.status,
				normalized.eventType,
				normalized.sourceTable,
				normalized.fromTime,
				normalized.toTime,
				normalized.limit,
			],
		);

		let replayedCount = 0;
		let skippedCount = 0;
		let failedCount = 0;
		let dryRunCount = 0;

		for (const deadLetter of candidates.rows) {
			if (normalized.dryRun) {
				dryRunCount += 1;
				await insertReplayRunItem(client, {
					replayRunId,
					deadLetter,
					outcome: "dry_run",
					message: "dry run only; source record was not requeued",
				});
				continue;
			}

			try {
				const outcome = await requeueSourceRecord(client, deadLetter);
				if (outcome === "replayed") {
					replayedCount += 1;
					await client.query(
						`
              UPDATE event_dead_letters
              SET
                status = 'replayed',
                replayed_at = now(),
                last_replay_run_id = $2,
                updated_at = now()
              WHERE id = $1
            `,
						[deadLetter.id, replayRunId],
					);
				} else {
					skippedCount += 1;
				}

				await insertReplayRunItem(client, {
					replayRunId,
					deadLetter,
					outcome,
					message:
						outcome === "replayed"
							? "source record requeued"
							: "source record was not found or unsupported",
				});
			} catch (error) {
				failedCount += 1;
				const serializedError = serializeError(error);
				await insertReplayRunItem(client, {
					replayRunId,
					deadLetter,
					outcome: "failed",
					message: serializedError.message,
				});
				logError("dead_letter_replay_failed", error, {
					replayRunId,
					deadLetterId: deadLetter.id,
					eventType: deadLetter.event_type,
					sourceTable: deadLetter.source_table,
					sourceRecordId: deadLetter.source_record_id,
				});
			}
		}

		await client.query(
			`
        UPDATE event_replay_runs
        SET
          candidate_count = $2,
          replayed_count = $3,
          skipped_count = $4,
          failed_count = $5,
          dry_run_count = $6,
          results = $7::jsonb,
          completed_at = now()
        WHERE id = $1
      `,
			[
				replayRunId,
				candidates.rowCount,
				replayedCount,
				skippedCount,
				failedCount,
				dryRunCount,
				JSON.stringify({
					candidateCount: candidates.rows.length,
					replayedCount,
					skippedCount,
					failedCount,
					dryRunCount,
				}),
			],
		);

		logInfo("dead_letter_replay_completed", {
			replayRunId,
			requestedBy: normalized.requestedBy,
			dryRun: normalized.dryRun,
			candidateCount: candidates.rows.length,
			replayedCount,
			skippedCount,
			failedCount,
			dryRunCount,
		});

		return {
			replayRunId,
			candidateCount: candidates.rows.length,
			replayedCount,
			skippedCount,
			failedCount,
			dryRunCount,
		};
	});
}

export async function countPendingDeadLetters() {
	try {
		const result = await query<CountRow>(`
      SELECT COUNT(*)::text AS total
      FROM event_dead_letters
      WHERE status = 'pending_replay'
    `);
		return Number(result.rows[0]?.total ?? "0");
	} catch (error) {
		logError("dead_letter_count_failed", error, {});
		return 0;
	}
}
