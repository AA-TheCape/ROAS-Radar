import { createHash } from "node:crypto";
import type { PoolClient } from "pg";

import { query, withTransaction } from "../../db/pool.js";
import { recordDeadLetter } from "../dead-letters/index.js";

export type RecoveryJobStatus =
	| "queued"
	| "running"
	| "succeeded"
	| "partial_failure"
	| "failed"
	| "cancelled"
	| "dead_lettered";

export type RecoveryJobTerminalStatus = Extract<
	RecoveryJobStatus,
	"succeeded" | "partial_failure" | "failed" | "cancelled" | "dead_lettered"
>;

export type RecoveryJobMode = "manual" | "scheduled" | "automatic";

export type RecoveryJobJson =
	| string
	| number
	| boolean
	| null
	| RecoveryJobJson[]
	| { [key: string]: RecoveryJobJson };

export type RecoveryJobPayload = Record<string, RecoveryJobJson>;

export type RecoveryJobCounters = {
	recordsDiscovered: number;
	recordsProcessed: number;
	recordsSucceeded: number;
	recordsFailed: number;
	recordsSkipped: number;
	recordsRetried: number;
	sideEffectsAttempted: number;
	sideEffectsSucceeded: number;
	sideEffectsSuppressed: number;
};

export type RecoveryJobRun<
	TPayload extends RecoveryJobPayload = RecoveryJobPayload,
> = RecoveryJobCounters & {
	id: string;
	jobType: string;
	status: RecoveryJobStatus;
	mode: RecoveryJobMode;
	initiatedBy: string;
	dryRun: boolean;
	timeRangeStart: string;
	timeRangeEnd: string;
	idempotencyKey: string;
	concurrencyKey: string;
	scopeKey: string;
	payload: TPayload;
	checkpoint: RecoveryJobPayload;
	priority: number;
	availableAt: string;
	attemptCount: number;
	maxAttempts: number;
	heartbeatTimeoutSeconds: number;
	claimedBy: string | null;
	queuedAt: string;
	startedAt: string | null;
	completedAt: string | null;
	lastHeartbeatAt: string | null;
	lockExpiresAt: string | null;
	deadLetteredAt: string | null;
	errorCode: string | null;
	errorMessage: string | null;
	lastErrorDetails: RecoveryJobPayload;
	completionReport: RecoveryJobPayload;
};

export type EnqueueRecoveryJobInput<
	TJobType extends string = string,
	TPayload extends RecoveryJobPayload = RecoveryJobPayload,
> = {
	jobType: TJobType;
	mode?: RecoveryJobMode;
	initiatedBy: string;
	dryRun?: boolean;
	timeRangeStart: Date | string;
	timeRangeEnd: Date | string;
	scopeKey?: string;
	payload?: TPayload;
	idempotencyKey?: string;
	concurrencyKey?: string;
	priority?: number;
	availableAt?: Date | string;
	maxAttempts?: number;
	heartbeatTimeoutSeconds?: number;
	resumeFromRunId?: string | null;
	rerunOfRunId?: string | null;
	now?: Date;
};

export type RecoveryJobProgressUpdate = {
	checkpoint?: RecoveryJobPayload;
	counters?: Partial<RecoveryJobCounters>;
};

export type RecoveryJobError = {
	code: string;
	message: string;
	details: RecoveryJobPayload;
	retryable: boolean;
};

export type RecoveryJobCompletion<
	TReport extends RecoveryJobPayload = RecoveryJobPayload,
> = {
	status?: Extract<RecoveryJobTerminalStatus, "succeeded" | "partial_failure">;
	report?: TReport;
	counters?: Partial<RecoveryJobCounters>;
};

export type RecoveryJobExecutionContext<
	TPayload extends RecoveryJobPayload = RecoveryJobPayload,
> = {
	run: RecoveryJobRun<TPayload>;
	workerId: string;
	dryRun: boolean;
	heartbeat: () => Promise<RecoveryJobRun<TPayload>>;
	updateProgress: (
		update: RecoveryJobProgressUpdate,
	) => Promise<RecoveryJobRun<TPayload>>;
};

export type RecoveryJobDefinition<
	TJobType extends string = string,
	TPayload extends RecoveryJobPayload = RecoveryJobPayload,
	TReport extends RecoveryJobPayload = RecoveryJobPayload,
> = {
	jobType: TJobType;
	parsePayload?: (payload: RecoveryJobPayload) => TPayload;
	maxAttempts?: number;
	heartbeatTimeoutSeconds?: number;
	run: (
		context: RecoveryJobExecutionContext<TPayload>,
	) => Promise<RecoveryJobCompletion<TReport> | TReport | undefined>;
};

export type RecoveryJobStore = {
	enqueue<TJobType extends string, TPayload extends RecoveryJobPayload>(
		input: EnqueueRecoveryJobInput<TJobType, TPayload>,
	): Promise<{ run: RecoveryJobRun<TPayload>; created: boolean }>;
	claimNext(input: {
		workerId: string;
		jobTypes?: string[];
		now?: Date;
	}): Promise<RecoveryJobRun | null>;
	heartbeat(
		runId: string,
		workerId: string,
		now?: Date,
	): Promise<RecoveryJobRun>;
	updateProgress(
		runId: string,
		workerId: string,
		update: RecoveryJobProgressUpdate,
		now?: Date,
	): Promise<RecoveryJobRun>;
	complete(
		runId: string,
		workerId: string,
		status: Extract<
			RecoveryJobTerminalStatus,
			"succeeded" | "partial_failure" | "failed"
		>,
		report: RecoveryJobPayload,
		counters?: Partial<RecoveryJobCounters>,
		now?: Date,
	): Promise<RecoveryJobRun>;
	fail(
		runId: string,
		workerId: string,
		error: RecoveryJobError,
		now?: Date,
	): Promise<RecoveryJobRun>;
	recoverStale(now?: Date): Promise<RecoveryJobRun[]>;
	getRun(runId: string): Promise<RecoveryJobRun | null>;
};

type RecoveryJobRunRow = {
	id: string;
	job_type: string;
	status: RecoveryJobStatus;
	mode: RecoveryJobMode;
	initiated_by: string;
	dry_run: boolean;
	time_range_start: Date | string;
	time_range_end: Date | string;
	idempotency_key: string;
	concurrency_key: string;
	scope_key: string;
	input_parameters: unknown;
	checkpoint: unknown;
	records_discovered: number;
	records_processed: number;
	records_succeeded: number;
	records_failed: number;
	records_skipped: number;
	records_retried: number;
	side_effects_attempted: number;
	side_effects_succeeded: number;
	side_effects_suppressed: number;
	priority: number;
	available_at: Date | string;
	attempt_count: number;
	max_attempts: number;
	heartbeat_timeout_seconds: number;
	claimed_by: string | null;
	queued_at: Date | string;
	started_at: Date | string | null;
	completed_at: Date | string | null;
	last_heartbeat_at: Date | string | null;
	lock_expires_at: Date | string | null;
	dead_lettered_at: Date | string | null;
	error_code: string | null;
	error_message: string | null;
	last_error_details: unknown;
	completion_report: unknown;
};

const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 300;
const DEFAULT_BACKOFF_BASE_MS = 30_000;
const DEFAULT_BACKOFF_MAX_MS = 30 * 60_000;

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
	input_parameters,
	checkpoint,
	records_discovered,
	records_processed,
	records_succeeded,
	records_failed,
	records_skipped,
	records_retried,
	side_effects_attempted,
	side_effects_succeeded,
	side_effects_suppressed,
	priority,
	available_at,
	attempt_count,
	max_attempts,
	heartbeat_timeout_seconds,
	claimed_by,
	queued_at,
	started_at,
	completed_at,
	last_heartbeat_at,
	lock_expires_at,
	dead_lettered_at,
	error_code,
	error_message,
	last_error_details,
	completion_report
`;

const aliasedRunColumns = `
	run.id,
	run.job_type,
	run.status,
	run.mode,
	run.initiated_by,
	run.dry_run,
	run.time_range_start,
	run.time_range_end,
	run.idempotency_key,
	run.concurrency_key,
	run.scope_key,
	run.input_parameters,
	run.checkpoint,
	run.records_discovered,
	run.records_processed,
	run.records_succeeded,
	run.records_failed,
	run.records_skipped,
	run.records_retried,
	run.side_effects_attempted,
	run.side_effects_succeeded,
	run.side_effects_suppressed,
	run.priority,
	run.available_at,
	run.attempt_count,
	run.max_attempts,
	run.heartbeat_timeout_seconds,
	run.claimed_by,
	run.queued_at,
	run.started_at,
	run.completed_at,
	run.last_heartbeat_at,
	run.lock_expires_at,
	run.dead_lettered_at,
	run.error_code,
	run.error_message,
	run.last_error_details,
	run.completion_report
`;

function stableStringify(value: unknown): string {
	if (Array.isArray(value)) {
		return `[${value.map((item) => stableStringify(item)).join(",")}]`;
	}

	if (value && typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>).sort(
			([a], [b]) => a.localeCompare(b),
		);
		return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${stableStringify(item)}`).join(",")}}`;
	}

	return JSON.stringify(value);
}

function normalizeDate(value: Date | string, fieldName: string): string {
	const date = value instanceof Date ? value : new Date(value);
	if (Number.isNaN(date.getTime())) {
		throw new Error(`Invalid recovery job ${fieldName}`);
	}
	return date.toISOString();
}

function toIsoString(value: Date | string | null): string | null {
	if (value === null) {
		return null;
	}
	return value instanceof Date
		? value.toISOString()
		: new Date(value).toISOString();
}

function normalizePayload(value: unknown): RecoveryJobPayload {
	return value && typeof value === "object" && !Array.isArray(value)
		? (value as RecoveryJobPayload)
		: {};
}

function normalizeError(error: unknown): RecoveryJobError {
	if (
		error &&
		typeof error === "object" &&
		"code" in error &&
		"message" in error
	) {
		const candidate = error as Partial<RecoveryJobError>;
		return {
			code: String(candidate.code ?? "recovery_job_error").slice(0, 128),
			message: String(candidate.message ?? "Recovery job failed").slice(
				0,
				2048,
			),
			details: normalizePayload(candidate.details),
			retryable: candidate.retryable ?? true,
		};
	}

	return {
		code:
			error instanceof Error && error.name
				? error.name.slice(0, 128)
				: "recovery_job_error",
		message:
			error instanceof Error
				? error.message.slice(0, 2048)
				: String(error).slice(0, 2048),
		details: {},
		retryable: true,
	};
}

function mapRunRow<TPayload extends RecoveryJobPayload = RecoveryJobPayload>(
	row: RecoveryJobRunRow,
): RecoveryJobRun<TPayload> {
	return {
		id: row.id,
		jobType: row.job_type,
		status: row.status,
		mode: row.mode,
		initiatedBy: row.initiated_by,
		dryRun: row.dry_run,
		timeRangeStart: toIsoString(row.time_range_start) ?? "",
		timeRangeEnd: toIsoString(row.time_range_end) ?? "",
		idempotencyKey: row.idempotency_key,
		concurrencyKey: row.concurrency_key,
		scopeKey: row.scope_key,
		payload: normalizePayload(row.input_parameters) as TPayload,
		checkpoint: normalizePayload(row.checkpoint),
		recordsDiscovered: Number(row.records_discovered),
		recordsProcessed: Number(row.records_processed),
		recordsSucceeded: Number(row.records_succeeded),
		recordsFailed: Number(row.records_failed),
		recordsSkipped: Number(row.records_skipped),
		recordsRetried: Number(row.records_retried),
		sideEffectsAttempted: Number(row.side_effects_attempted),
		sideEffectsSucceeded: Number(row.side_effects_succeeded),
		sideEffectsSuppressed: Number(row.side_effects_suppressed),
		priority: Number(row.priority),
		availableAt: toIsoString(row.available_at) ?? "",
		attemptCount: Number(row.attempt_count),
		maxAttempts: Number(row.max_attempts),
		heartbeatTimeoutSeconds: Number(row.heartbeat_timeout_seconds),
		claimedBy: row.claimed_by,
		queuedAt: toIsoString(row.queued_at) ?? "",
		startedAt: toIsoString(row.started_at),
		completedAt: toIsoString(row.completed_at),
		lastHeartbeatAt: toIsoString(row.last_heartbeat_at),
		lockExpiresAt: toIsoString(row.lock_expires_at),
		deadLetteredAt: toIsoString(row.dead_lettered_at),
		errorCode: row.error_code,
		errorMessage: row.error_message,
		lastErrorDetails: normalizePayload(row.last_error_details),
		completionReport: normalizePayload(row.completion_report),
	};
}

export function calculateRecoveryJobBackoffMs(
	attemptCount: number,
	baseMs = DEFAULT_BACKOFF_BASE_MS,
	maxMs = DEFAULT_BACKOFF_MAX_MS,
): number {
	return Math.min(maxMs, baseMs * 2 ** Math.max(0, attemptCount - 1));
}

export function buildRecoveryJobIdempotencyKey(input: {
	jobType: string;
	scopeKey: string;
	timeRangeStart: string;
	timeRangeEnd: string;
	dryRun: boolean;
	payload?: RecoveryJobPayload;
}): string {
	const digest = createHash("sha256")
		.update(stableStringify(input))
		.digest("hex");
	return `recovery-job:${input.jobType}:${digest}`;
}

export function buildRecoveryJobConcurrencyKey(input: {
	jobType: string;
	scopeKey: string;
	timeRangeStart: string;
	timeRangeEnd: string;
}): string {
	const digest = createHash("sha256")
		.update(stableStringify(input))
		.digest("hex");
	return `recovery-job-lock:${digest}`;
}

export class PostgresRecoveryJobQueue implements RecoveryJobStore {
	async enqueue<TJobType extends string, TPayload extends RecoveryJobPayload>(
		input: EnqueueRecoveryJobInput<TJobType, TPayload>,
	): Promise<{ run: RecoveryJobRun<TPayload>; created: boolean }> {
		const now = input.now ?? new Date();
		const timeRangeStart = normalizeDate(
			input.timeRangeStart,
			"timeRangeStart",
		);
		const timeRangeEnd = normalizeDate(input.timeRangeEnd, "timeRangeEnd");
		if (new Date(timeRangeEnd).getTime() < new Date(timeRangeStart).getTime()) {
			throw new Error(
				"Recovery job timeRangeEnd must be greater than or equal to timeRangeStart",
			);
		}

		const payload = input.payload ?? ({} as TPayload);
		const scopeKey = input.scopeKey?.trim() || "global";
		const dryRun = input.dryRun ?? true;
		const idempotencyKey =
			input.idempotencyKey ??
			buildRecoveryJobIdempotencyKey({
				jobType: input.jobType,
				scopeKey,
				timeRangeStart,
				timeRangeEnd,
				dryRun,
				payload,
			});
		const concurrencyKey =
			input.concurrencyKey ??
			buildRecoveryJobConcurrencyKey({
				jobType: input.jobType,
				scopeKey,
				timeRangeStart,
				timeRangeEnd,
			});

		return withTransaction(async (client) => {
			const result = await client.query<RecoveryJobRunRow>(
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
						priority,
						available_at,
						max_attempts,
						heartbeat_timeout_seconds,
						last_heartbeat_at,
						created_at,
						updated_at
					)
					VALUES ($1, 'queued', $2, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8, $9, $10, $11, $12::jsonb, '{}'::jsonb, $13, $14::timestamptz, $15, $16, $17, $17, $17)
					ON CONFLICT (idempotency_key) DO NOTHING
					RETURNING ${runColumns}
				`,
				[
					input.jobType,
					input.mode ?? "automatic",
					input.initiatedBy,
					dryRun,
					timeRangeStart,
					timeRangeEnd,
					idempotencyKey,
					concurrencyKey,
					scopeKey,
					input.resumeFromRunId ?? null,
					input.rerunOfRunId ?? null,
					JSON.stringify(payload),
					input.priority ?? 100,
					input.availableAt
						? normalizeDate(input.availableAt, "availableAt")
						: now.toISOString(),
					input.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
					input.heartbeatTimeoutSeconds ?? DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
					now,
				],
			);

			const inserted = result.rows[0];
			if (inserted) {
				await insertStatusEvent(
					client,
					inserted.id,
					null,
					"queued",
					input.initiatedBy,
					"enqueued",
					payload,
				);
				return { run: mapRunRow<TPayload>(inserted), created: true };
			}

			const existing = await client.query<RecoveryJobRunRow>(
				`
					SELECT ${runColumns}
					FROM recovery_job_runs
					WHERE idempotency_key = $1
					LIMIT 1
				`,
				[idempotencyKey],
			);
			const row = existing.rows[0];
			if (!row) {
				throw new Error("Failed to enqueue or find recovery job");
			}
			return { run: mapRunRow<TPayload>(row), created: false };
		});
	}

	async claimNext(input: {
		workerId: string;
		jobTypes?: string[];
		now?: Date;
	}): Promise<RecoveryJobRun | null> {
		const now = input.now ?? new Date();
		const jobTypes = input.jobTypes?.length ? input.jobTypes : null;

		return withTransaction(async (client) => {
			const result = await client.query<RecoveryJobRunRow>(
				`
					WITH candidate AS (
						SELECT id
						FROM recovery_job_runs
						WHERE status = 'queued'
							AND available_at <= $2
							AND ($3::text[] IS NULL OR job_type = ANY($3::text[]))
						ORDER BY priority ASC, available_at ASC, queued_at ASC, id ASC
						FOR UPDATE SKIP LOCKED
						LIMIT 1
					)
					UPDATE recovery_job_runs run
					SET
						status = 'running',
						started_at = COALESCE(run.started_at, $2),
						completed_at = NULL,
						claimed_by = $1,
						attempt_count = run.attempt_count + 1,
						last_heartbeat_at = $2,
						lock_expires_at = $2 + (run.heartbeat_timeout_seconds || ' seconds')::interval,
						error_code = NULL,
						error_message = NULL,
						updated_at = $2
					FROM candidate
					WHERE run.id = candidate.id
					RETURNING ${aliasedRunColumns}
				`,
				[input.workerId, now, jobTypes],
			);

			const row = result.rows[0];
			if (!row) {
				return null;
			}

			await insertStatusEvent(
				client,
				row.id,
				"queued",
				"running",
				input.workerId,
				"claimed",
				{},
			);
			return mapRunRow(row);
		});
	}

	async heartbeat(
		runId: string,
		workerId: string,
		now = new Date(),
	): Promise<RecoveryJobRun> {
		const result = await query<RecoveryJobRunRow>(
			`
				UPDATE recovery_job_runs
				SET
					last_heartbeat_at = $3,
					lock_expires_at = $3 + (heartbeat_timeout_seconds || ' seconds')::interval,
					updated_at = $3
				WHERE id = $1
					AND status = 'running'
					AND claimed_by = $2
				RETURNING ${runColumns}
			`,
			[runId, workerId, now],
		);

		const row = result.rows[0];
		if (!row) {
			throw new Error(`Recovery job ${runId} is not held by ${workerId}`);
		}
		return mapRunRow(row);
	}

	async updateProgress(
		runId: string,
		workerId: string,
		update: RecoveryJobProgressUpdate,
		now = new Date(),
	): Promise<RecoveryJobRun> {
		const result = await query<RecoveryJobRunRow>(
			`
				UPDATE recovery_job_runs
				SET
					checkpoint = COALESCE($3::jsonb, checkpoint),
					records_discovered = records_discovered + $4,
					records_processed = records_processed + $5,
					records_succeeded = records_succeeded + $6,
					records_failed = records_failed + $7,
					records_skipped = records_skipped + $8,
					records_retried = records_retried + $9,
					side_effects_attempted = side_effects_attempted + $10,
					side_effects_succeeded = side_effects_succeeded + $11,
					side_effects_suppressed = side_effects_suppressed + $12,
					last_heartbeat_at = $13,
					lock_expires_at = $13 + (heartbeat_timeout_seconds || ' seconds')::interval,
					updated_at = $13
				WHERE id = $1
					AND status = 'running'
					AND claimed_by = $2
				RETURNING ${runColumns}
			`,
			[
				runId,
				workerId,
				update.checkpoint ? JSON.stringify(update.checkpoint) : null,
				update.counters?.recordsDiscovered ?? 0,
				update.counters?.recordsProcessed ?? 0,
				update.counters?.recordsSucceeded ?? 0,
				update.counters?.recordsFailed ?? 0,
				update.counters?.recordsSkipped ?? 0,
				update.counters?.recordsRetried ?? 0,
				update.counters?.sideEffectsAttempted ?? 0,
				update.counters?.sideEffectsSucceeded ?? 0,
				update.counters?.sideEffectsSuppressed ?? 0,
				now,
			],
		);

		const row = result.rows[0];
		if (!row) {
			throw new Error(`Recovery job ${runId} progress update was not accepted`);
		}
		return mapRunRow(row);
	}

	async complete(
		runId: string,
		workerId: string,
		status: Extract<
			RecoveryJobTerminalStatus,
			"succeeded" | "partial_failure" | "failed"
		>,
		report: RecoveryJobPayload,
		counters: Partial<RecoveryJobCounters> = {},
		now = new Date(),
	): Promise<RecoveryJobRun> {
		return withTransaction(async (client) => {
			const previous = await client.query<Pick<RecoveryJobRunRow, "status">>(
				"SELECT status FROM recovery_job_runs WHERE id = $1 LIMIT 1",
				[runId],
			);
			const previousStatus = previous.rows[0]?.status ?? null;
			const result = await client.query<RecoveryJobRunRow>(
				`
					UPDATE recovery_job_runs
					SET
						status = $3,
						completed_at = $4,
						last_heartbeat_at = $4,
						lock_expires_at = NULL,
						completion_report = $5::jsonb,
						records_discovered = records_discovered + $6,
						records_processed = records_processed + $7,
						records_succeeded = records_succeeded + $8,
						records_failed = records_failed + $9,
						records_skipped = records_skipped + $10,
						records_retried = records_retried + $11,
						side_effects_attempted = side_effects_attempted + $12,
						side_effects_succeeded = side_effects_succeeded + $13,
						side_effects_suppressed = side_effects_suppressed + $14,
						updated_at = $4
					WHERE id = $1
						AND status = 'running'
						AND claimed_by = $2
					RETURNING ${runColumns}
				`,
				[
					runId,
					workerId,
					status,
					now,
					JSON.stringify(report),
					counters.recordsDiscovered ?? 0,
					counters.recordsProcessed ?? 0,
					counters.recordsSucceeded ?? 0,
					counters.recordsFailed ?? 0,
					counters.recordsSkipped ?? 0,
					counters.recordsRetried ?? 0,
					counters.sideEffectsAttempted ?? 0,
					counters.sideEffectsSucceeded ?? 0,
					counters.sideEffectsSuppressed ?? 0,
				],
			);

			const row = result.rows[0];
			if (!row) {
				throw new Error(`Recovery job ${runId} completion was not accepted`);
			}

			await insertCompletionReport(client, row, report);
			await insertStatusEvent(
				client,
				row.id,
				previousStatus,
				row.status,
				workerId,
				"completed",
				report,
			);
			return mapRunRow(row);
		});
	}

	async fail(
		runId: string,
		workerId: string,
		error: RecoveryJobError,
		now = new Date(),
	): Promise<RecoveryJobRun> {
		return withTransaction(async (client) => {
			const locked = await client.query<RecoveryJobRunRow>(
				`
					SELECT ${runColumns}
					FROM recovery_job_runs
					WHERE id = $1
						AND status = 'running'
						AND claimed_by = $2
					FOR UPDATE
					LIMIT 1
				`,
				[runId, workerId],
			);
			const current = locked.rows[0];
			if (!current) {
				throw new Error(`Recovery job ${runId} failure was not accepted`);
			}

			const shouldDeadLetter =
				!error.retryable || current.attempt_count >= current.max_attempts;
			const nextStatus: RecoveryJobStatus = shouldDeadLetter
				? "dead_lettered"
				: "queued";
			const backoffMs = calculateRecoveryJobBackoffMs(current.attempt_count);
			const availableAt = new Date(now.getTime() + backoffMs);
			const report = {
				errorCode: error.code,
				errorMessage: error.message,
				errorDetails: error.details,
				attemptCount: current.attempt_count,
				maxAttempts: current.max_attempts,
			};

			const result = await client.query<RecoveryJobRunRow>(
				`
					UPDATE recovery_job_runs
					SET
						status = $3,
						available_at = CASE WHEN $3 = 'queued' THEN $4 ELSE available_at END,
						completed_at = CASE WHEN $3 = 'dead_lettered' THEN $5 ELSE NULL END,
						dead_lettered_at = CASE WHEN $3 = 'dead_lettered' THEN $5 ELSE NULL END,
						claimed_by = NULL,
						lock_expires_at = NULL,
						last_heartbeat_at = $5,
						error_code = $6,
						error_message = $7,
						last_error_details = $8::jsonb,
						completion_report = CASE WHEN $3 = 'dead_lettered' THEN $9::jsonb ELSE completion_report END,
						updated_at = $5
					WHERE id = $1
						AND status = 'running'
						AND claimed_by = $2
					RETURNING ${runColumns}
				`,
				[
					runId,
					workerId,
					nextStatus,
					availableAt,
					now,
					error.code,
					error.message,
					JSON.stringify(error.details),
					JSON.stringify(report),
				],
			);

			const row = result.rows[0];
			if (!row) {
				throw new Error(`Recovery job ${runId} failure was not accepted`);
			}

			await insertStatusEvent(
				client,
				row.id,
				"running",
				row.status,
				workerId,
				shouldDeadLetter ? "dead_lettered" : "retry_scheduled",
				report,
			);

			if (shouldDeadLetter) {
				await insertCompletionReport(client, row, report);
				await recordDeadLetter(client, {
					eventType: row.job_type,
					sourceTable: "recovery_job_runs",
					sourceRecordId: row.id,
					sourceQueueKey: row.idempotency_key,
					payload: normalizePayload(row.input_parameters),
					error,
				});
			}

			return mapRunRow(row);
		});
	}

	async recoverStale(now = new Date()): Promise<RecoveryJobRun[]> {
		return withTransaction(async (client) => {
			const stale = await client.query<RecoveryJobRunRow>(
				`
					SELECT ${runColumns}
					FROM recovery_job_runs
					WHERE status = 'running'
						AND lock_expires_at IS NOT NULL
						AND lock_expires_at < $1
					FOR UPDATE SKIP LOCKED
				`,
				[now],
			);

			const recovered: RecoveryJobRun[] = [];
			for (const row of stale.rows) {
				const error: RecoveryJobError = {
					code: "recovery_job_heartbeat_expired",
					message: `Recovery job heartbeat expired for worker ${row.claimed_by ?? "unknown"}`,
					details: {
						claimedBy: row.claimed_by,
						lockExpiresAt: toIsoString(row.lock_expires_at),
					},
					retryable: row.attempt_count < row.max_attempts,
				};
				const shouldDeadLetter = row.attempt_count >= row.max_attempts;
				const nextStatus: RecoveryJobStatus = shouldDeadLetter
					? "dead_lettered"
					: "queued";
				const availableAt = new Date(
					now.getTime() + calculateRecoveryJobBackoffMs(row.attempt_count),
				);
				const report = {
					errorCode: error.code,
					errorMessage: error.message,
					errorDetails: error.details,
					attemptCount: row.attempt_count,
					maxAttempts: row.max_attempts,
				};

				const updated = await client.query<RecoveryJobRunRow>(
					`
						UPDATE recovery_job_runs
						SET
							status = $2,
							available_at = CASE WHEN $2 = 'queued' THEN $3 ELSE available_at END,
							completed_at = CASE WHEN $2 = 'dead_lettered' THEN $4 ELSE NULL END,
							dead_lettered_at = CASE WHEN $2 = 'dead_lettered' THEN $4 ELSE NULL END,
							claimed_by = NULL,
							lock_expires_at = NULL,
							last_heartbeat_at = $4,
							error_code = $5,
							error_message = $6,
							last_error_details = $7::jsonb,
							completion_report = CASE WHEN $2 = 'dead_lettered' THEN $8::jsonb ELSE completion_report END,
							updated_at = $4
						WHERE id = $1
						RETURNING ${runColumns}
					`,
					[
						row.id,
						nextStatus,
						availableAt,
						now,
						error.code,
						error.message,
						JSON.stringify(error.details),
						JSON.stringify(report),
					],
				);

				const next = updated.rows[0];
				await insertStatusEvent(
					client,
					next.id,
					"running",
					next.status,
					"recovery-job-queue",
					shouldDeadLetter ? "heartbeat_dead_lettered" : "heartbeat_recovered",
					report,
				);

				if (shouldDeadLetter) {
					await insertCompletionReport(client, next, report);
					await recordDeadLetter(client, {
						eventType: next.job_type,
						sourceTable: "recovery_job_runs",
						sourceRecordId: next.id,
						sourceQueueKey: next.idempotency_key,
						payload: normalizePayload(next.input_parameters),
						error,
					});
				}

				recovered.push(mapRunRow(next));
			}

			return recovered;
		});
	}

	async getRun(runId: string): Promise<RecoveryJobRun | null> {
		const result = await query<RecoveryJobRunRow>(
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
}

export class RecoveryJobExecutor {
	private readonly definitions: Map<string, RecoveryJobDefinition>;
	private readonly store: RecoveryJobStore;

	constructor(
		definitions: RecoveryJobDefinition[],
		store: RecoveryJobStore = new PostgresRecoveryJobQueue(),
	) {
		this.definitions = new Map(
			definitions.map((definition) => [definition.jobType, definition]),
		);
		this.store = store;
	}

	async enqueue<TJobType extends string, TPayload extends RecoveryJobPayload>(
		input: EnqueueRecoveryJobInput<TJobType, TPayload>,
	): Promise<{ run: RecoveryJobRun<TPayload>; created: boolean }> {
		const definition = this.definitions.get(input.jobType);
		return this.store.enqueue({
			...input,
			maxAttempts: input.maxAttempts ?? definition?.maxAttempts,
			heartbeatTimeoutSeconds:
				input.heartbeatTimeoutSeconds ?? definition?.heartbeatTimeoutSeconds,
		});
	}

	async executeNext(input: {
		workerId: string;
		jobTypes?: string[];
		now?: Date;
	}): Promise<{ claimed: false } | { claimed: true; run: RecoveryJobRun }> {
		const run = await this.store.claimNext(input);
		if (!run) {
			return { claimed: false };
		}

		const definition = this.definitions.get(run.jobType);
		if (!definition) {
			const failed = await this.store.fail(
				run.id,
				input.workerId,
				{
					code: "unknown_recovery_job_type",
					message: `No recovery job handler registered for ${run.jobType}`,
					details: { jobType: run.jobType },
					retryable: false,
				},
				input.now,
			);
			return { claimed: true, run: failed };
		}

		try {
			const payload = definition.parsePayload
				? definition.parsePayload(run.payload)
				: run.payload;
			const typedRun = { ...run, payload };
			const context: RecoveryJobExecutionContext = {
				run: typedRun,
				workerId: input.workerId,
				dryRun: run.dryRun,
				heartbeat: () =>
					this.store.heartbeat(run.id, input.workerId, new Date()),
				updateProgress: (update) =>
					this.store.updateProgress(run.id, input.workerId, update, new Date()),
			};
			const result = await definition.run(context);
			const completion = normalizeCompletion(result);
			const completed = await this.store.complete(
				run.id,
				input.workerId,
				completion.status,
				completion.report,
				completion.counters,
				new Date(),
			);
			return { claimed: true, run: completed };
		} catch (error) {
			const failed = await this.store.fail(
				run.id,
				input.workerId,
				normalizeError(error),
				new Date(),
			);
			return { claimed: true, run: failed };
		}
	}

	async recoverStale(now?: Date): Promise<RecoveryJobRun[]> {
		return this.store.recoverStale(now);
	}
}

function normalizeCompletion(
	result: RecoveryJobCompletion | RecoveryJobPayload | undefined,
): Required<RecoveryJobCompletion> {
	if (!result) {
		return { status: "succeeded", report: {}, counters: {} };
	}

	if ("status" in result || "report" in result || "counters" in result) {
		const completion = result as RecoveryJobCompletion;
		return {
			status: completion.status ?? "succeeded",
			report: completion.report ?? {},
			counters: completion.counters ?? {},
		};
	}

	return { status: "succeeded", report: result, counters: {} };
}

async function insertStatusEvent(
	client: PoolClient,
	runId: string,
	fromStatus: RecoveryJobStatus | null,
	toStatus: RecoveryJobStatus,
	changedBy: string,
	reason: string,
	metadata: RecoveryJobPayload,
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

async function insertCompletionReport(
	client: PoolClient,
	row: RecoveryJobRunRow,
	report: RecoveryJobPayload,
): Promise<void> {
	const counters = {
		recordsDiscovered: Number(row.records_discovered),
		recordsProcessed: Number(row.records_processed),
		recordsSucceeded: Number(row.records_succeeded),
		recordsFailed: Number(row.records_failed),
		recordsSkipped: Number(row.records_skipped),
		recordsRetried: Number(row.records_retried),
		sideEffectsAttempted: Number(row.side_effects_attempted),
		sideEffectsSucceeded: Number(row.side_effects_succeeded),
		sideEffectsSuppressed: Number(row.side_effects_suppressed),
	};

	await client.query(
		`
			INSERT INTO recovery_job_completion_reports (
				run_id,
				job_type,
				status,
				report,
				counters
			)
			VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
			ON CONFLICT (run_id) DO UPDATE
			SET
				status = EXCLUDED.status,
				report = EXCLUDED.report,
				counters = EXCLUDED.counters,
				created_at = now()
		`,
		[
			row.id,
			row.job_type,
			row.status,
			JSON.stringify(report),
			JSON.stringify(counters),
		],
	);
}

export function createRecoveryJobExecutor(
	definitions: RecoveryJobDefinition[],
	store?: RecoveryJobStore,
): RecoveryJobExecutor {
	return new RecoveryJobExecutor(definitions, store);
}

export const __recoveryJobTestUtils = {
	buildRecoveryJobConcurrencyKey,
	buildRecoveryJobIdempotencyKey,
	calculateRecoveryJobBackoffMs,
};
