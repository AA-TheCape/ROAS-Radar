import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type { PoolClient } from "pg";

import { query, withTransaction } from "../../db/pool.js";
import {
	emitRecoveryRecordFailureLog,
	emitRecoveryRunChunkLog,
	emitRecoveryRunLifecycleLog,
} from "../../observability/index.js";

export type RecoveryJobMode = "manual" | "scheduled" | "automatic";
export type RecoveryRunStatus =
	| "queued"
	| "running"
	| "succeeded"
	| "partial_failure"
	| "failed"
	| "cancelled"
	| "dead_lettered";
export type RecoveryRecordStatus =
	| "queued"
	| "processing"
	| "succeeded"
	| "failed"
	| "skipped"
	| "retry_pending";

export type RecoveryJsonValue =
	| string
	| number
	| boolean
	| null
	| RecoveryJsonValue[]
	| { [key: string]: RecoveryJsonValue };

export type RecoveryCheckpoint = Record<string, RecoveryJsonValue>;

export type RecoveryRunCounters = {
	recordsDiscovered: number;
	recordsClaimed: number;
	recordsProcessed: number;
	recordsSucceeded: number;
	recordsFailed: number;
	recordsSkipped: number;
	recordsRetried: number;
	sideEffectsAttempted: number;
	sideEffectsSucceeded: number;
	sideEffectsSuppressed: number;
};

export type RecoveryRun = RecoveryRunCounters & {
	id: string;
	jobType: string;
	status: RecoveryRunStatus;
	mode: RecoveryJobMode;
	initiatedBy: string;
	dryRun: boolean;
	timeRangeStart: string;
	timeRangeEnd: string;
	idempotencyKey: string;
	concurrencyKey: string;
	scopeKey: string;
	resumeFromRunId: string | null;
	rerunOfRunId: string | null;
	inputParameters: RecoveryCheckpoint;
	checkpoint: RecoveryCheckpoint;
	claimedBy: string | null;
	queuedAt: string;
	startedAt: string | null;
	completedAt: string | null;
	lastHeartbeatAt: string | null;
	errorCode: string | null;
	errorMessage: string | null;
};

export type RecoveryRecord = {
	id: string;
	runId: string;
	jobType: string;
	recordType: string;
	recordKey: string;
	sourceFingerprint: string | null;
	sideEffectKey: string | null;
	processingStatus: RecoveryRecordStatus;
	attemptCount: number;
};

export type RecoveryRunRequest = {
	jobType: string;
	mode?: RecoveryJobMode;
	initiatedBy: string;
	dryRun?: boolean;
	timeRangeStart: Date | string;
	timeRangeEnd: Date | string;
	scopeKey?: string;
	inputParameters?: RecoveryCheckpoint;
	resumeFromRunId?: string | null;
	rerunOfRunId?: string | null;
	now?: Date;
};

export type RecoveryPage<TRecord> = {
	records: TRecord[];
	checkpoint: RecoveryCheckpoint;
	done: boolean;
};

export type RecoveryRecordIdentity<TRecord> = {
	recordType: string;
	recordKey: string;
	sourceFingerprint?: string | null;
	sideEffectKey?: string | null;
	payload?: RecoveryCheckpoint;
};

export type RecoveryRecordResult = {
	status?: "succeeded" | "skipped";
	result?: RecoveryCheckpoint;
	sideEffectAttempted?: boolean;
	sideEffectSucceeded?: boolean;
};

export type RecoveryJobDefinition<TRecord> = {
	jobType: string;
	pageSize?: number;
	maxAttempts?: number;
	backoffBaseMs?: number;
	backoffMaxMs?: number;
	validate?: (request: RecoveryRunRequest) => void | Promise<void>;
	fetchPage: (context: RecoveryExecutionContext) => Promise<RecoveryPage<TRecord>>;
	identifyRecord: (
		record: TRecord,
		context: RecoveryExecutionContext,
	) => RecoveryRecordIdentity<TRecord>;
	processRecord: (
		record: TRecord,
		context: RecoveryExecutionContext,
	) => Promise<RecoveryRecordResult | undefined>;
};

export type RecoveryExecutionContext = {
	run: RecoveryRun;
	workerId: string;
	dryRun: boolean;
	pageSize: number;
	checkpoint: RecoveryCheckpoint;
	now: Date;
	buildRecordIdempotencyKey: (
		recordType: string,
		recordKey: string,
	) => string;
};

export type RecoveryStartResult =
	| { started: true; run: RecoveryRun; reusedExistingRun: boolean }
	| { started: false; conflict: RecoveryRun };

export type RecoveryExecutionResult = {
	run: RecoveryRun;
	pagesProcessed: number;
	recordsProcessed: number;
};

export type RecoveryJobStore = {
	createOrGetRun(input: CreateRecoveryRunInput): Promise<{ run: RecoveryRun; created: boolean }>;
	findActiveConflict(input: RecoveryConflictLookupInput): Promise<RecoveryRun | null>;
	claimRun(runId: string, workerId: string, now: Date): Promise<RecoveryRun>;
	getRun(runId: string): Promise<RecoveryRun | null>;
	upsertRecord(input: UpsertRecoveryRecordInput): Promise<RecoveryRecord>;
	markRecordProcessing(
		recordId: string,
		workerId: string,
		now: Date,
	): Promise<void>;
	markRecordSucceeded(
		recordId: string,
		result: RecoveryCheckpoint,
		sideEffectAttempted: boolean,
		sideEffectSucceeded: boolean,
		now: Date,
	): Promise<void>;
	markRecordSkipped(
		recordId: string,
		result: RecoveryCheckpoint,
		sideEffectSuppressed: boolean,
		now: Date,
	): Promise<void>;
	markRecordRetryPending(
		recordId: string,
		error: NormalizedRecoveryError,
		nextAttemptAt: Date,
		now: Date,
	): Promise<void>;
	markRecordFailed(
		recordId: string,
		error: NormalizedRecoveryError,
		now: Date,
	): Promise<void>;
	updateCheckpoint(
		runId: string,
		checkpointName: string,
		checkpoint: RecoveryCheckpoint,
		recordsProcessed: number,
		now: Date,
	): Promise<RecoveryRun>;
	incrementRunCounters(
		runId: string,
		counters: Partial<RecoveryRunCounters>,
		now: Date,
	): Promise<RecoveryRun>;
	finalizeRun(
		runId: string,
		status: Extract<RecoveryRunStatus, "succeeded" | "partial_failure" | "failed">,
		error: NormalizedRecoveryError | null,
		now: Date,
	): Promise<RecoveryRun>;
	recordError(input: RecordRecoveryErrorInput): Promise<void>;
};

type RecoveryRunRow = {
	id: string;
	job_type: string;
	status: RecoveryRunStatus;
	mode: RecoveryJobMode;
	initiated_by: string;
	dry_run: boolean;
	time_range_start: Date;
	time_range_end: Date;
	idempotency_key: string;
	concurrency_key: string;
	scope_key: string;
	resume_from_run_id: string | null;
	rerun_of_run_id: string | null;
	input_parameters: unknown;
	checkpoint: unknown;
	records_discovered: number;
	records_claimed: number;
	records_processed: number;
	records_succeeded: number;
	records_failed: number;
	records_skipped: number;
	records_retried: number;
	side_effects_attempted: number;
	side_effects_succeeded: number;
	side_effects_suppressed: number;
	claimed_by: string | null;
	queued_at: Date;
	started_at: Date | null;
	completed_at: Date | null;
	last_heartbeat_at: Date | null;
	error_code: string | null;
	error_message: string | null;
};

type RecoveryRecordRow = {
	id: string;
	run_id: string;
	job_type: string;
	record_type: string;
	record_key: string;
	source_fingerprint: string | null;
	side_effect_key: string | null;
	processing_status: RecoveryRecordStatus;
	attempt_count: number;
};

export type CreateRecoveryRunInput = {
	jobType: string;
	mode: RecoveryJobMode;
	initiatedBy: string;
	dryRun: boolean;
	timeRangeStart: string;
	timeRangeEnd: string;
	idempotencyKey: string;
	concurrencyKey: string;
	scopeKey: string;
	resumeFromRunId: string | null;
	rerunOfRunId: string | null;
	inputParameters: RecoveryCheckpoint;
	checkpoint: RecoveryCheckpoint;
	now: Date;
};

export type RecoveryConflictLookupInput = {
	jobType: string;
	scopeKey: string;
	timeRangeStart: string;
	timeRangeEnd: string;
	idempotencyKey: string;
};

export type UpsertRecoveryRecordInput = {
	runId: string;
	jobType: string;
	recordType: string;
	recordKey: string;
	sourceFingerprint: string | null;
	sideEffectKey: string | null;
	now: Date;
};

export type RecordRecoveryErrorInput = {
	runId: string;
	recordId: string | null;
	jobType: string;
	recordType: string | null;
	recordKey: string | null;
	severity: "warning" | "error" | "fatal";
	error: NormalizedRecoveryError;
	retryable: boolean;
	now: Date;
};

export type NormalizedRecoveryError = {
	code: string;
	message: string;
	details: RecoveryCheckpoint;
};

const DEFAULT_PAGE_SIZE = 250;
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_BACKOFF_BASE_MS = 1_000;
const DEFAULT_BACKOFF_MAX_MS = 60_000;

function normalizeDate(value: Date | string, fieldName: string): string {
	const date = value instanceof Date ? value : new Date(value);

	if (Number.isNaN(date.getTime())) {
		throw new Error(`Invalid recovery ${fieldName}`);
	}

	return date.toISOString();
}

function normalizeCheckpoint(value: unknown): RecoveryCheckpoint {
	if (value && typeof value === "object" && !Array.isArray(value)) {
		return value as RecoveryCheckpoint;
	}

	return {};
}

function stableStringify(value: unknown): string {
	if (Array.isArray(value)) {
		return `[${value.map((item) => stableStringify(item)).join(",")}]`;
	}

	if (value && typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
			a.localeCompare(b),
		);
		return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${stableStringify(item)}`).join(",")}}`;
	}

	return JSON.stringify(value);
}

export function buildRecoveryIdempotencyKey(input: {
	jobType: string;
	scopeKey: string;
	timeRangeStart: string;
	timeRangeEnd: string;
	dryRun: boolean;
	inputParameters?: RecoveryCheckpoint;
}): string {
	const digest = createHash("sha256")
		.update(
			stableStringify({
				dryRun: input.dryRun,
				inputParameters: input.inputParameters ?? {},
				jobType: input.jobType,
				scopeKey: input.scopeKey,
				timeRangeEnd: input.timeRangeEnd,
				timeRangeStart: input.timeRangeStart,
			}),
		)
		.digest("hex");

	return `recovery:${input.jobType}:${digest}`;
}

export function buildRecoveryConcurrencyKey(input: {
	scopeKey: string;
	timeRangeStart: string;
	timeRangeEnd: string;
}): string {
	const digest = createHash("sha256")
		.update(
			stableStringify({
				scopeKey: input.scopeKey,
				timeRangeEnd: input.timeRangeEnd,
				timeRangeStart: input.timeRangeStart,
			}),
		)
		.digest("hex");

	return `range:${digest}`;
}

export function buildRecoveryRecordIdempotencyKey(input: {
	runIdempotencyKey: string;
	recordType: string;
	recordKey: string;
}): string {
	const digest = createHash("sha256")
		.update(
			stableStringify({
				recordKey: input.recordKey,
				recordType: input.recordType,
				run: input.runIdempotencyKey,
			}),
		)
		.digest("hex");

	return `recovery-record:${digest}`;
}

export function calculateRecoveryBackoffMs(
	attemptCount: number,
	baseMs = DEFAULT_BACKOFF_BASE_MS,
	maxMs = DEFAULT_BACKOFF_MAX_MS,
): number {
	const exponent = Math.max(0, attemptCount - 1);
	return Math.min(maxMs, baseMs * 2 ** exponent);
}

function normalizeRecoveryError(error: unknown): NormalizedRecoveryError {
	const code =
		typeof error === "object" &&
		error !== null &&
		"code" in error &&
		typeof error.code === "string" &&
		error.code.trim()
			? error.code.trim()
			: error instanceof Error && error.name.trim()
				? error.name.trim()
				: "recovery_job_error";

	const message =
		error instanceof Error && error.message.trim()
			? error.message.trim()
			: typeof error === "string" && error.trim()
				? error.trim()
				: "Recovery job failed";

	return {
		code: code.slice(0, 128),
		message: message.slice(0, 2048),
		details: {},
	};
}

function mapRunRow(row: RecoveryRunRow): RecoveryRun {
	return {
		id: row.id,
		jobType: row.job_type,
		status: row.status,
		mode: row.mode,
		initiatedBy: row.initiated_by,
		dryRun: row.dry_run,
		timeRangeStart: row.time_range_start.toISOString(),
		timeRangeEnd: row.time_range_end.toISOString(),
		idempotencyKey: row.idempotency_key,
		concurrencyKey: row.concurrency_key,
		scopeKey: row.scope_key,
		resumeFromRunId: row.resume_from_run_id,
		rerunOfRunId: row.rerun_of_run_id,
		inputParameters: normalizeCheckpoint(row.input_parameters),
		checkpoint: normalizeCheckpoint(row.checkpoint),
		recordsDiscovered: row.records_discovered,
		recordsClaimed: row.records_claimed,
		recordsProcessed: row.records_processed,
		recordsSucceeded: row.records_succeeded,
		recordsFailed: row.records_failed,
		recordsSkipped: row.records_skipped,
		recordsRetried: row.records_retried,
		sideEffectsAttempted: row.side_effects_attempted,
		sideEffectsSucceeded: row.side_effects_succeeded,
		sideEffectsSuppressed: row.side_effects_suppressed,
		claimedBy: row.claimed_by,
		queuedAt: row.queued_at.toISOString(),
		startedAt: row.started_at?.toISOString() ?? null,
		completedAt: row.completed_at?.toISOString() ?? null,
		lastHeartbeatAt: row.last_heartbeat_at?.toISOString() ?? null,
		errorCode: row.error_code,
		errorMessage: row.error_message,
	};
}

function mapRecordRow(row: RecoveryRecordRow): RecoveryRecord {
	return {
		id: row.id,
		runId: row.run_id,
		jobType: row.job_type,
		recordType: row.record_type,
		recordKey: row.record_key,
		sourceFingerprint: row.source_fingerprint,
		sideEffectKey: row.side_effect_key,
		processingStatus: row.processing_status,
		attemptCount: row.attempt_count,
	};
}

const runColumns = `
	id,
	job_type,
	status,
	mode,
	initiated_by,
	dry_run,
	time_range_start,
	time_range_end,
	idempotency_key,
	concurrency_key,
	scope_key,
	resume_from_run_id,
	rerun_of_run_id,
	input_parameters,
	checkpoint,
	records_discovered,
	records_claimed,
	records_processed,
	records_succeeded,
	records_failed,
	records_skipped,
	records_retried,
	side_effects_attempted,
	side_effects_succeeded,
	side_effects_suppressed,
	claimed_by,
	queued_at,
	started_at,
	completed_at,
	last_heartbeat_at,
	error_code,
	error_message
`;

export class PostgresRecoveryJobStore implements RecoveryJobStore {
	async createOrGetRun(
		input: CreateRecoveryRunInput,
	): Promise<{ run: RecoveryRun; created: boolean }> {
		return withTransaction(async (client) => {
			const insertResult = await client.query<RecoveryRunRow>(
				`
					INSERT INTO recovery_job_runs (
						job_type,
						status,
						mode,
						initiated_by,
						dry_run,
						time_range_start,
						time_range_end,
						idempotency_key,
						concurrency_key,
						scope_key,
						resume_from_run_id,
						rerun_of_run_id,
						input_parameters,
						checkpoint,
						last_heartbeat_at,
						created_at,
						updated_at
					)
					VALUES (
						$1,
						'queued',
						$2,
						$3,
						$4,
						$5::timestamptz,
						$6::timestamptz,
						$7,
						$8,
						$9,
						$10,
						$11,
						$12::jsonb,
						$13::jsonb,
						$14,
						$14,
						$14
					)
					ON CONFLICT (idempotency_key) DO NOTHING
					RETURNING ${runColumns}
				`,
				[
					input.jobType,
					input.mode,
					input.initiatedBy,
					input.dryRun,
					input.timeRangeStart,
					input.timeRangeEnd,
					input.idempotencyKey,
					input.concurrencyKey,
					input.scopeKey,
					input.resumeFromRunId,
					input.rerunOfRunId,
					JSON.stringify(input.inputParameters),
					JSON.stringify(input.checkpoint),
					input.now,
				],
			);

			const inserted = insertResult.rows[0];

			if (inserted) {
				await this.insertStatusEvent(client, inserted.id, null, "queued", input.initiatedBy, "created", input.inputParameters);
				return { run: mapRunRow(inserted), created: true };
			}

			const existingResult = await client.query<RecoveryRunRow>(
				`
					SELECT ${runColumns}
					FROM recovery_job_runs
					WHERE idempotency_key = $1
					LIMIT 1
				`,
				[input.idempotencyKey],
			);

			const existing = existingResult.rows[0];
			if (!existing) {
				throw new Error("Failed to create or find recovery run");
			}

			return { run: mapRunRow(existing), created: false };
		});
	}

	async findActiveConflict(input: RecoveryConflictLookupInput): Promise<RecoveryRun | null> {
		const result = await query<RecoveryRunRow>(
			`
				SELECT ${runColumns}
				FROM recovery_job_runs
				WHERE job_type = $1
					AND scope_key = $2
					AND idempotency_key <> $5
					AND status IN ('queued', 'running')
					AND tstzrange(time_range_start, time_range_end, '[]')
						&& tstzrange($3::timestamptz, $4::timestamptz, '[]')
				ORDER BY queued_at ASC, id ASC
				LIMIT 1
			`,
			[
				input.jobType,
				input.scopeKey,
				input.timeRangeStart,
				input.timeRangeEnd,
				input.idempotencyKey,
			],
		);

		const row = result.rows[0];
		return row ? mapRunRow(row) : null;
	}

	async claimRun(runId: string, workerId: string, now: Date): Promise<RecoveryRun> {
		return withTransaction(async (client) => {
			const result = await client.query<RecoveryRunRow>(
				`
					UPDATE recovery_job_runs
					SET
						status = 'running',
						started_at = COALESCE(started_at, $2),
						completed_at = NULL,
						claimed_by = $3,
						last_heartbeat_at = $2,
						error_code = NULL,
						error_message = NULL,
						updated_at = $2
					WHERE id = $1
						AND status IN ('queued', 'running')
					RETURNING ${runColumns}
				`,
				[runId, now, workerId],
			);

			const row = result.rows[0];
			if (!row) {
				throw new Error(`Recovery run ${runId} is not claimable`);
			}

			if (row.status === "running") {
				await this.insertStatusEvent(client, runId, "queued", "running", workerId, "claimed", {});
			}

			return mapRunRow(row);
		});
	}

	async getRun(runId: string): Promise<RecoveryRun | null> {
		const result = await query<RecoveryRunRow>(
			`
				SELECT ${runColumns}
				FROM recovery_job_runs
				WHERE id = $1
				LIMIT 1
			`,
			[runId],
		);

		const row = result.rows[0];
		return row ? mapRunRow(row) : null;
	}

	async upsertRecord(input: UpsertRecoveryRecordInput): Promise<RecoveryRecord> {
		const result = await query<RecoveryRecordRow>(
			`
				INSERT INTO recovery_job_records (
					run_id,
					job_type,
					record_type,
					record_key,
					source_fingerprint,
					side_effect_key,
					created_at,
					updated_at
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
				ON CONFLICT (run_id, record_type, record_key) DO UPDATE
				SET
					source_fingerprint = EXCLUDED.source_fingerprint,
					side_effect_key = COALESCE(recovery_job_records.side_effect_key, EXCLUDED.side_effect_key),
					updated_at = EXCLUDED.updated_at
				RETURNING
					id::text,
					run_id::text,
					job_type,
					record_type,
					record_key,
					source_fingerprint,
					side_effect_key,
					processing_status,
					attempt_count
			`,
			[
				input.runId,
				input.jobType,
				input.recordType,
				input.recordKey,
				input.sourceFingerprint,
				input.sideEffectKey,
				input.now,
			],
		);

		return mapRecordRow(result.rows[0]);
	}

	async markRecordProcessing(
		recordId: string,
		workerId: string,
		now: Date,
	): Promise<void> {
		await query(
			`
				UPDATE recovery_job_records
				SET
					processing_status = 'processing',
					attempt_count = attempt_count + 1,
					last_attempt_at = $2,
					locked_by = $3,
					locked_at = $2,
					started_at = COALESCE(started_at, $2),
					status_updated_at = $2,
					updated_at = $2
				WHERE id = $1::bigint
			`,
			[recordId, now, workerId],
		);
	}

	async markRecordSucceeded(
		recordId: string,
		result: RecoveryCheckpoint,
		sideEffectAttempted: boolean,
		sideEffectSucceeded: boolean,
		now: Date,
	): Promise<void> {
		await query(
			`
				UPDATE recovery_job_records
				SET
					processing_status = 'succeeded',
					completed_at = $2,
					status_updated_at = $2,
					result = $3::jsonb,
					last_error_code = NULL,
					last_error_message = NULL,
					updated_at = $2
				WHERE id = $1::bigint
			`,
			[
				recordId,
				now,
				JSON.stringify({
					...result,
					sideEffectAttempted,
					sideEffectSucceeded,
				}),
			],
		);
	}

	async markRecordSkipped(
		recordId: string,
		result: RecoveryCheckpoint,
		sideEffectSuppressed: boolean,
		now: Date,
	): Promise<void> {
		await query(
			`
				UPDATE recovery_job_records
				SET
					processing_status = 'skipped',
					completed_at = $2,
					status_updated_at = $2,
					result = $3::jsonb,
					last_error_code = NULL,
					last_error_message = NULL,
					updated_at = $2
				WHERE id = $1::bigint
			`,
			[
				recordId,
				now,
				JSON.stringify({
					...result,
					sideEffectSuppressed,
				}),
			],
		);
	}

	async markRecordRetryPending(
		recordId: string,
		error: NormalizedRecoveryError,
		nextAttemptAt: Date,
		now: Date,
	): Promise<void> {
		await query(
			`
				UPDATE recovery_job_records
				SET
					processing_status = 'retry_pending',
					next_attempt_at = $3,
					status_updated_at = $2,
					last_error_code = $4,
					last_error_message = $5,
					updated_at = $2
				WHERE id = $1::bigint
			`,
			[recordId, now, nextAttemptAt, error.code, error.message],
		);
	}

	async markRecordFailed(
		recordId: string,
		error: NormalizedRecoveryError,
		now: Date,
	): Promise<void> {
		await query(
			`
				UPDATE recovery_job_records
				SET
					processing_status = 'failed',
					completed_at = $2,
					status_updated_at = $2,
					last_error_code = $3,
					last_error_message = $4,
					updated_at = $2
				WHERE id = $1::bigint
			`,
			[recordId, now, error.code, error.message],
		);
	}

	async updateCheckpoint(
		runId: string,
		checkpointName: string,
		checkpoint: RecoveryCheckpoint,
		recordsProcessed: number,
		now: Date,
	): Promise<RecoveryRun> {
		return withTransaction(async (client) => {
			await client.query(
				`
					INSERT INTO recovery_job_checkpoints (
						run_id,
						checkpoint_name,
						sequence_number,
						cursor_value,
						records_processed,
						created_at,
						updated_at
					)
					VALUES ($1, $2, 1, $3::jsonb, $4, $5, $5)
					ON CONFLICT (run_id, checkpoint_name) DO UPDATE
					SET
						sequence_number = recovery_job_checkpoints.sequence_number + 1,
						cursor_value = EXCLUDED.cursor_value,
						records_processed = EXCLUDED.records_processed,
						updated_at = EXCLUDED.updated_at
				`,
				[runId, checkpointName, JSON.stringify(checkpoint), recordsProcessed, now],
			);

			const result = await client.query<RecoveryRunRow>(
				`
					UPDATE recovery_job_runs
					SET
						checkpoint = $2::jsonb,
						records_processed = records_processed + $3,
						last_heartbeat_at = $4,
						updated_at = $4
					WHERE id = $1
					RETURNING ${runColumns}
				`,
				[runId, JSON.stringify(checkpoint), recordsProcessed, now],
			);

			return mapRunRow(result.rows[0]);
		});
	}

	async incrementRunCounters(
		runId: string,
		counters: Partial<RecoveryRunCounters>,
		now: Date,
	): Promise<RecoveryRun> {
		const result = await query<RecoveryRunRow>(
			`
				UPDATE recovery_job_runs
				SET
					records_discovered = records_discovered + $2,
					records_claimed = records_claimed + $3,
					records_succeeded = records_succeeded + $4,
					records_failed = records_failed + $5,
					records_skipped = records_skipped + $6,
					records_retried = records_retried + $7,
					side_effects_attempted = side_effects_attempted + $8,
					side_effects_succeeded = side_effects_succeeded + $9,
					side_effects_suppressed = side_effects_suppressed + $10,
					last_heartbeat_at = $11,
					updated_at = $11
				WHERE id = $1
				RETURNING ${runColumns}
			`,
			[
				runId,
				counters.recordsDiscovered ?? 0,
				counters.recordsClaimed ?? 0,
				counters.recordsSucceeded ?? 0,
				counters.recordsFailed ?? 0,
				counters.recordsSkipped ?? 0,
				counters.recordsRetried ?? 0,
				counters.sideEffectsAttempted ?? 0,
				counters.sideEffectsSucceeded ?? 0,
				counters.sideEffectsSuppressed ?? 0,
				now,
			],
		);

		return mapRunRow(result.rows[0]);
	}

	async finalizeRun(
		runId: string,
		status: Extract<RecoveryRunStatus, "succeeded" | "partial_failure" | "failed">,
		error: NormalizedRecoveryError | null,
		now: Date,
	): Promise<RecoveryRun> {
		return withTransaction(async (client) => {
			const previous = await client.query<Pick<RecoveryRunRow, "status">>(
				"SELECT status FROM recovery_job_runs WHERE id = $1 LIMIT 1",
				[runId],
			);
			const previousStatus = previous.rows[0]?.status ?? null;
			const result = await client.query<RecoveryRunRow>(
				`
					UPDATE recovery_job_runs
					SET
						status = $2,
						completed_at = $3,
						last_heartbeat_at = $3,
						error_code = $4,
						error_message = $5,
						updated_at = $3
					WHERE id = $1
					RETURNING ${runColumns}
				`,
				[runId, status, now, error?.code ?? null, error?.message ?? null],
			);

			await this.insertStatusEvent(client, runId, previousStatus, status, "recovery-orchestrator", error?.message ?? "finalized", error?.details ?? {});

			return mapRunRow(result.rows[0]);
		});
	}

	async recordError(input: RecordRecoveryErrorInput): Promise<void> {
		await query(
			`
				INSERT INTO recovery_job_errors (
					run_id,
					record_status_id,
					job_type,
					record_type,
					record_key,
					severity,
					error_code,
					error_message,
					error_details,
					retryable,
					occurred_at
				)
				VALUES ($1, $2::bigint, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
			`,
			[
				input.runId,
				input.recordId,
				input.jobType,
				input.recordType,
				input.recordKey,
				input.severity,
				input.error.code,
				input.error.message,
				JSON.stringify(input.error.details),
				input.retryable,
				input.now,
			],
		);
	}

	private async insertStatusEvent(
		client: PoolClient,
		runId: string,
		fromStatus: RecoveryRunStatus | null,
		toStatus: RecoveryRunStatus,
		changedBy: string,
		reason: string,
		metadata: RecoveryCheckpoint,
	): Promise<void> {
		await client.query(
			`
				INSERT INTO recovery_job_status_events (
					run_id,
					from_status,
					to_status,
					changed_by,
					reason,
					metadata
				)
				VALUES ($1, $2, $3, $4, $5, $6::jsonb)
			`,
			[runId, fromStatus, toStatus, changedBy, reason, JSON.stringify(metadata)],
		);
	}
}

export class RecoveryJobOrchestrator<TRecord> {
	private readonly store: RecoveryJobStore;
	private readonly definition: RecoveryJobDefinition<TRecord>;

	constructor(
		definition: RecoveryJobDefinition<TRecord>,
		store: RecoveryJobStore = new PostgresRecoveryJobStore(),
	) {
		this.definition = definition;
		this.store = store;
	}

	async start(request: RecoveryRunRequest): Promise<RecoveryStartResult> {
		if (request.jobType !== this.definition.jobType) {
			throw new Error(
				`Recovery definition ${this.definition.jobType} cannot run ${request.jobType}`,
			);
		}

		await this.definition.validate?.(request);

		const timeRangeStart = normalizeDate(request.timeRangeStart, "timeRangeStart");
		const timeRangeEnd = normalizeDate(request.timeRangeEnd, "timeRangeEnd");
		if (new Date(timeRangeEnd).getTime() < new Date(timeRangeStart).getTime()) {
			throw new Error("Recovery timeRangeEnd must be greater than or equal to timeRangeStart");
		}

		const scopeKey = request.scopeKey?.trim() || "global";
		const inputParameters = request.inputParameters ?? {};
		const dryRun = request.dryRun ?? true;
		const idempotencyKey = buildRecoveryIdempotencyKey({
			jobType: request.jobType,
			scopeKey,
			timeRangeStart,
			timeRangeEnd,
			dryRun,
			inputParameters,
		});
		const concurrencyKey = buildRecoveryConcurrencyKey({
			scopeKey,
			timeRangeStart,
			timeRangeEnd,
		});

		const conflict = await this.store.findActiveConflict({
			jobType: request.jobType,
			scopeKey,
			timeRangeStart,
			timeRangeEnd,
			idempotencyKey,
		});

		if (conflict) {
			return { started: false, conflict };
		}

		const { run, created } = await this.store.createOrGetRun({
			jobType: request.jobType,
			mode: request.mode ?? "manual",
			initiatedBy: request.initiatedBy,
			dryRun,
			timeRangeStart,
			timeRangeEnd,
			idempotencyKey,
			concurrencyKey,
			scopeKey,
			resumeFromRunId: request.resumeFromRunId ?? null,
			rerunOfRunId: request.rerunOfRunId ?? null,
			inputParameters,
			checkpoint: {},
			now: request.now ?? new Date(),
		});

		return { started: true, run, reusedExistingRun: !created };
	}

	async execute(
		runId: string,
		workerId: string,
		now = new Date(),
		options: { managesCompletion?: boolean } = {},
	): Promise<RecoveryExecutionResult> {
		const managesCompletion = options.managesCompletion ?? true;
		let run = await this.store.claimRun(runId, workerId, now);
		let pagesProcessed = 0;
		let recordsProcessed = 0;
		let done = false;
		const runStartedAt = process.hrtime.bigint();

		emitRecoveryRunLifecycleLog({
			stage: "started",
			run,
			workerId,
			pagesProcessed,
			durationMs: 0,
		});

		try {
			while (!done) {
				const latestRun = await this.store.getRun(run.id);
				if (latestRun?.status === "cancelled") {
					emitRecoveryRunLifecycleLog({
						stage: "cancelled",
						run: latestRun,
						workerId,
						pagesProcessed,
						durationMs: Number(process.hrtime.bigint() - runStartedAt) / 1_000_000,
					});

					return {
						run: latestRun,
						pagesProcessed,
						recordsProcessed,
					};
				}

				const context = this.buildContext(run, workerId, now);
				const pageStartedAt = process.hrtime.bigint();
				const page = await this.definition.fetchPage(context);
				if (page.records.length > 0) {
					run = await this.store.incrementRunCounters(
						run.id,
						{
							recordsDiscovered: page.records.length,
							recordsClaimed: page.records.length,
						},
						now,
					);
				}

				let pageRecordsProcessed = 0;
				for (const record of page.records) {
					const processed = await this.processRecord(record, context, now);
					pageRecordsProcessed += processed ? 1 : 0;
					recordsProcessed += processed ? 1 : 0;
				}

				pagesProcessed += 1;
				run = await this.store.updateCheckpoint(
					run.id,
					"default",
					page.checkpoint,
					page.records.length,
					now,
				);
				emitRecoveryRunChunkLog({
					run,
					workerId,
					pageNumber: pagesProcessed,
					recordsDiscovered: page.records.length,
					recordsProcessed: pageRecordsProcessed,
					done: page.done,
					durationMs: Number(process.hrtime.bigint() - pageStartedAt) / 1_000_000,
					checkpoint: page.checkpoint,
				});
				done = page.done;
			}

			const latestRun = await this.store.getRun(run.id);
			if (latestRun?.status === "cancelled") {
				emitRecoveryRunLifecycleLog({
					stage: "cancelled",
					run: latestRun,
					workerId,
					pagesProcessed,
					durationMs: Number(process.hrtime.bigint() - runStartedAt) / 1_000_000,
				});

				return {
					run: latestRun,
					pagesProcessed,
					recordsProcessed,
				};
			}

			const terminalStatus =
				run.recordsFailed > 0 || run.recordsRetried > 0
					? "partial_failure"
					: "succeeded";
			if (!managesCompletion) {
				const completedRun: RecoveryRun = {
					...run,
					status: terminalStatus,
				};
				emitRecoveryRunLifecycleLog({
					stage: "completed",
					run: completedRun,
					workerId,
					pagesProcessed,
					durationMs: Number(process.hrtime.bigint() - runStartedAt) / 1_000_000,
				});

				return {
					run: completedRun,
					pagesProcessed,
					recordsProcessed,
				};
			}
			const finalized = await this.store.finalizeRun(run.id, terminalStatus, null, now);

			emitRecoveryRunLifecycleLog({
				stage: "completed",
				run: finalized,
				workerId,
				pagesProcessed,
				durationMs: Number(process.hrtime.bigint() - runStartedAt) / 1_000_000,
			});

			return {
				run: finalized,
				pagesProcessed,
				recordsProcessed,
			};
		} catch (error) {
			const normalizedError = normalizeRecoveryError(error);
			const failedRun = managesCompletion
				? await this.store.finalizeRun(
						run.id,
						"failed",
						normalizedError,
						new Date(),
					)
				: {
						...run,
						status: "failed" as const,
						errorCode: normalizedError.code,
						errorMessage: normalizedError.message,
					};
			emitRecoveryRunLifecycleLog({
				stage: "failed",
				run: failedRun,
				workerId,
				pagesProcessed,
				durationMs: Number(process.hrtime.bigint() - runStartedAt) / 1_000_000,
				error,
			});
			throw error;
		}
	}

	async startAndExecute(
		request: RecoveryRunRequest,
		workerId: string,
	): Promise<RecoveryStartResult | RecoveryExecutionResult> {
		const startResult = await this.start(request);

		if (!startResult.started) {
			return startResult;
		}

		if (
			startResult.reusedExistingRun &&
			!["queued", "running"].includes(startResult.run.status)
		) {
			return {
				run: startResult.run,
				pagesProcessed: 0,
				recordsProcessed: 0,
			};
		}

		return this.execute(startResult.run.id, workerId, request.now ?? new Date());
	}

	private buildContext(
		run: RecoveryRun,
		workerId: string,
		now: Date,
	): RecoveryExecutionContext {
		return {
			run,
			workerId,
			dryRun: run.dryRun,
			pageSize: this.definition.pageSize ?? DEFAULT_PAGE_SIZE,
			checkpoint: run.checkpoint,
			now,
			buildRecordIdempotencyKey: (recordType, recordKey) =>
				buildRecoveryRecordIdempotencyKey({
					runIdempotencyKey: run.idempotencyKey,
					recordType,
					recordKey,
				}),
		};
	}

	private async processRecord(
		record: TRecord,
		context: RecoveryExecutionContext,
		now: Date,
	): Promise<boolean> {
		const identity = this.definition.identifyRecord(record, context);
		const sideEffectKey =
			identity.sideEffectKey ??
			context.buildRecordIdempotencyKey(identity.recordType, identity.recordKey);
		const storedRecord = await this.store.upsertRecord({
			runId: context.run.id,
			jobType: context.run.jobType,
			recordType: identity.recordType,
			recordKey: identity.recordKey,
			sourceFingerprint: identity.sourceFingerprint ?? null,
			sideEffectKey,
			now,
		});

		if (storedRecord.processingStatus === "succeeded" || storedRecord.processingStatus === "skipped") {
			return false;
		}

		let attemptNumber = storedRecord.attemptCount + 1;
		await this.store.markRecordProcessing(storedRecord.id, context.workerId, now);

		if (context.dryRun) {
			await this.store.markRecordSkipped(
				storedRecord.id,
				{ dryRun: true },
				true,
				now,
			);
			await this.store.incrementRunCounters(
				context.run.id,
				{
					recordsSkipped: 1,
					sideEffectsSuppressed: 1,
				},
				now,
			);
			return true;
		}

		while (true) {
			try {
				const result = await this.definition.processRecord(record, context);
				const status = result?.status ?? "succeeded";
				const sideEffectAttempted = result?.sideEffectAttempted ?? true;
				const sideEffectSucceeded = result?.sideEffectSucceeded ?? sideEffectAttempted;

				if (status === "skipped") {
					await this.store.markRecordSkipped(
						storedRecord.id,
						result?.result ?? {},
						false,
						now,
					);
					await this.store.incrementRunCounters(
						context.run.id,
						{
							recordsSkipped: 1,
						},
						now,
					);
				} else {
					await this.store.markRecordSucceeded(
						storedRecord.id,
						result?.result ?? {},
						sideEffectAttempted,
						sideEffectSucceeded,
						now,
					);
					await this.store.incrementRunCounters(
						context.run.id,
						{
							recordsSucceeded: 1,
							sideEffectsAttempted: sideEffectAttempted ? 1 : 0,
							sideEffectsSucceeded: sideEffectSucceeded ? 1 : 0,
						},
						now,
					);
				}

				return true;
			} catch (error) {
				const normalizedError = normalizeRecoveryError(error);
				const maxAttempts = this.definition.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
				const retryable = attemptNumber < maxAttempts;

				await this.store.recordError({
					runId: context.run.id,
					recordId: storedRecord.id,
					jobType: context.run.jobType,
					recordType: identity.recordType,
					recordKey: identity.recordKey,
					severity: retryable ? "warning" : "error",
					error: normalizedError,
					retryable,
					now,
				});
				emitRecoveryRecordFailureLog({
					run: context.run,
					workerId: context.workerId,
					recordId: storedRecord.id,
					recordType: identity.recordType,
					recordKey: identity.recordKey,
					attemptNumber,
					retryable,
					nextAttemptAt: retryable
						? new Date(
								now.getTime() +
									calculateRecoveryBackoffMs(
										attemptNumber,
										this.definition.backoffBaseMs,
										this.definition.backoffMaxMs,
									),
							)
						: null,
					error: normalizedError,
				});

				if (!retryable) {
					await this.store.markRecordFailed(storedRecord.id, normalizedError, now);
					await this.store.incrementRunCounters(
						context.run.id,
						{
							recordsFailed: 1,
						},
						now,
					);
					return true;
				}

				const backoffMs = calculateRecoveryBackoffMs(
					attemptNumber,
					this.definition.backoffBaseMs,
					this.definition.backoffMaxMs,
				);
				await this.store.markRecordRetryPending(
					storedRecord.id,
					normalizedError,
					new Date(now.getTime() + backoffMs),
					now,
				);
				await this.store.incrementRunCounters(
					context.run.id,
					{
						recordsRetried: 1,
					},
					now,
				);
				if (backoffMs > 0) {
					await delay(backoffMs);
				}
				attemptNumber += 1;
				await this.store.markRecordProcessing(storedRecord.id, context.workerId, new Date());
			}
		}
	}
}

export function createRecoveryJobOrchestrator<TRecord>(
	definition: RecoveryJobDefinition<TRecord>,
	store?: RecoveryJobStore,
): RecoveryJobOrchestrator<TRecord> {
	return new RecoveryJobOrchestrator(definition, store);
}
