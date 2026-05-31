import { type Router, Router as createRouter } from "express";
import { z } from "zod";

import {
	GA4_FALLBACK_RECOVERY_JOB_TYPE,
	executeGa4FallbackRecoveryRun,
} from "../attribution/ga4-fallback-recovery.js";
import {
	SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE,
	executeShopifyAttributionRecoveryRun,
} from "../attribution/shopify-hint-recovery.js";
import { attachAuthContext, requireAdmin, type AuthContext } from "../auth/index.js";
import { query, withTransaction } from "../../db/pool.js";
import { logError } from "../../observability/index.js";
import {
	PostgresRecoveryJobStore,
	buildRecoveryConcurrencyKey,
	buildRecoveryIdempotencyKey,
	type RecoveryCheckpoint,
	type RecoveryRun,
	type RecoveryRunStatus,
} from "./index.js";

class RecoveryAdminHttpError extends Error {
	statusCode: number;
	code: string;
	details?: unknown;

	constructor(
		statusCode: number,
		code: string,
		message: string,
		details?: unknown,
	) {
		super(message);
		this.name = "RecoveryAdminHttpError";
		this.statusCode = statusCode;
		this.code = code;
		this.details = details;
	}
}

type RecoveryRunRow = {
	id: string;
	job_type: string;
	status: RecoveryRunStatus;
	mode: "manual" | "scheduled" | "automatic";
	initiated_by: string;
	dry_run: boolean;
	time_range_start: Date | string;
	time_range_end: Date | string;
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
	queued_at: Date | string;
	started_at: Date | string | null;
	completed_at: Date | string | null;
	last_heartbeat_at: Date | string | null;
	error_code: string | null;
	error_message: string | null;
};

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

const jobTypeSchema = z.enum([
	SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE,
	GA4_FALLBACK_RECOVERY_JOB_TYPE,
]);

const createRunSchema = z
	.object({
		jobType: jobTypeSchema,
		timeRangeStart: z.string().datetime({ offset: true }).optional(),
		timeRangeEnd: z.string().datetime({ offset: true }).optional(),
		startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
		endDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
		dryRun: z.boolean().optional().default(true),
		chunkSize: z.number().int().min(1).max(5000).optional().default(250),
		scopeKey: z.string().trim().min(1).max(255).optional(),
		lookbackDays: z.number().int().min(1).max(90).optional(),
	})
	.superRefine((value, context) => {
		const hasInstantRange = Boolean(value.timeRangeStart || value.timeRangeEnd);
		const hasDateRange = Boolean(value.startDate || value.endDate);

		if (hasInstantRange && hasDateRange) {
			context.addIssue({
				code: z.ZodIssueCode.custom,
				message: "Use either timeRangeStart/timeRangeEnd or startDate/endDate.",
				path: ["timeRangeStart"],
			});
		}

		if (hasInstantRange && (!value.timeRangeStart || !value.timeRangeEnd)) {
			context.addIssue({
				code: z.ZodIssueCode.custom,
				message: "timeRangeStart and timeRangeEnd are both required.",
				path: ["timeRangeEnd"],
			});
		}

		if (!hasInstantRange && (!value.startDate || !value.endDate)) {
			context.addIssue({
				code: z.ZodIssueCode.custom,
				message: "startDate and endDate are both required.",
				path: ["endDate"],
			});
		}
	});

const runIdSchema = z.object({
	runId: z.string().uuid(),
});

const startRunSchema = z.object({
	workerId: z.string().trim().min(1).max(255).optional(),
});

const listRunsSchema = z.object({
	jobType: jobTypeSchema.optional(),
	status: z
		.enum(["queued", "running", "succeeded", "partial_failure", "failed", "cancelled"])
		.optional(),
	limit: z.coerce.number().int().min(1).max(100).optional().default(25),
});

function parseInput<TSchema extends z.ZodTypeAny>(
	schema: TSchema,
	input: unknown,
	message: string,
): z.infer<TSchema> {
	try {
		return schema.parse(input);
	} catch (error) {
		if (error instanceof z.ZodError) {
			throw new RecoveryAdminHttpError(
				400,
				"invalid_request",
				message,
				error.flatten(),
			);
		}

		throw error;
	}
}

function toIsoString(value: Date | string | null): string | null {
	if (value === null) {
		return null;
	}

	return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}

function normalizeCheckpoint(value: unknown): RecoveryCheckpoint {
	if (value && typeof value === "object" && !Array.isArray(value)) {
		return value as RecoveryCheckpoint;
	}

	return {};
}

function mapRun(row: RecoveryRunRow): RecoveryRun {
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
		resumeFromRunId: row.resume_from_run_id,
		rerunOfRunId: row.rerun_of_run_id,
		inputParameters: normalizeCheckpoint(row.input_parameters),
		checkpoint: normalizeCheckpoint(row.checkpoint),
		recordsDiscovered: Number(row.records_discovered),
		recordsClaimed: Number(row.records_claimed),
		recordsProcessed: Number(row.records_processed),
		recordsSucceeded: Number(row.records_succeeded),
		recordsFailed: Number(row.records_failed),
		recordsSkipped: Number(row.records_skipped),
		recordsRetried: Number(row.records_retried),
		sideEffectsAttempted: Number(row.side_effects_attempted),
		sideEffectsSucceeded: Number(row.side_effects_succeeded),
		sideEffectsSuppressed: Number(row.side_effects_suppressed),
		claimedBy: row.claimed_by,
		queuedAt: toIsoString(row.queued_at) ?? "",
		startedAt: toIsoString(row.started_at),
		completedAt: toIsoString(row.completed_at),
		lastHeartbeatAt: toIsoString(row.last_heartbeat_at),
		errorCode: row.error_code,
		errorMessage: row.error_message,
	};
}

function getInitiatedBy(auth: AuthContext | null | undefined): string {
	if (!auth) {
		throw new RecoveryAdminHttpError(401, "unauthorized", "Authentication required");
	}

	return auth.kind === "internal" ? "internal" : auth.user.email;
}

function normalizeRange(input: z.infer<typeof createRunSchema>): {
	timeRangeStart: string;
	timeRangeEnd: string;
} {
	const timeRangeStart = input.timeRangeStart
		? new Date(input.timeRangeStart)
		: new Date(`${input.startDate}T00:00:00.000Z`);
	const timeRangeEnd = input.timeRangeEnd
		? new Date(input.timeRangeEnd)
		: new Date(`${input.endDate}T23:59:59.999Z`);

	if (
		Number.isNaN(timeRangeStart.getTime()) ||
		Number.isNaN(timeRangeEnd.getTime()) ||
		timeRangeEnd.getTime() < timeRangeStart.getTime()
	) {
		throw new RecoveryAdminHttpError(
			400,
			"invalid_date_range",
			"Recovery timeRangeEnd must be greater than or equal to timeRangeStart",
		);
	}

	return {
		timeRangeStart: timeRangeStart.toISOString(),
		timeRangeEnd: timeRangeEnd.toISOString(),
	};
}

function buildProgressLink(runId: string): string {
	return `/api/admin/recovery/runs/${runId}`;
}

function buildRunResponse(run: RecoveryRun) {
	return {
		runId: run.id,
		status: run.status,
		jobType: run.jobType,
		dryRun: run.dryRun,
		progressLink: buildProgressLink(run.id),
		startLink: `/api/admin/recovery/runs/${run.id}/start`,
		cancelLink: `/api/admin/recovery/runs/${run.id}/cancel`,
		run,
	};
}

async function getRun(runId: string): Promise<RecoveryRun | null> {
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
	return row ? mapRun(row) : null;
}

async function cancelRun(
	run: RecoveryRun,
	cancelledBy: string,
	reason: string,
): Promise<RecoveryRun> {
	return withTransaction(async (client) => {
		const now = new Date();
		const result = await client.query<RecoveryRunRow>(
			`
				UPDATE recovery_job_runs
				SET
					status = 'cancelled',
					started_at = COALESCE(started_at, $2),
					completed_at = $2,
					last_heartbeat_at = $2,
					error_code = NULL,
					error_message = NULL,
					updated_at = $2
				WHERE id = $1
					AND status IN ('queued', 'running')
				RETURNING ${runColumns}
			`,
			[run.id, now],
		);

		const updated = result.rows[0];
		if (!updated) {
			throw new RecoveryAdminHttpError(
				409,
				"recovery_run_not_cancellable",
				`Recovery run ${run.id} is already terminal`,
			);
		}

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
				VALUES ($1, $2, 'cancelled', $3, $4, '{}'::jsonb)
			`,
			[run.id, run.status, cancelledBy, reason],
		);

		return mapRun(updated);
	});
}

function executeRunInBackground(run: RecoveryRun, workerId: string): void {
	const execute =
		run.jobType === SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE
			? executeShopifyAttributionRecoveryRun
			: run.jobType === GA4_FALLBACK_RECOVERY_JOB_TYPE
				? executeGa4FallbackRecoveryRun
				: null;

	if (!execute) {
		throw new RecoveryAdminHttpError(
			400,
			"unsupported_recovery_job_type",
			`Unsupported recovery job type: ${run.jobType}`,
		);
	}

	setImmediate(() => {
		void execute(run.id, workerId).catch((error) => {
			logError("manual_recovery_run_failed", error, {
				runId: run.id,
				jobType: run.jobType,
				workerId,
			});
		});
	});
}

export function createRecoveryAdminRouter(): Router {
	const router = createRouter();

	router.use(attachAuthContext);
	router.use(requireAdmin);

	router.get("/job-types", (_req, res) => {
		res.status(200).json({
			jobTypes: [
				{
					jobType: SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE,
					defaultScopeKey: "shopify-attribution-hints",
				},
				{
					jobType: GA4_FALLBACK_RECOVERY_JOB_TYPE,
					defaultScopeKey: "ga4-fallback-unattributed",
					optionalParameters: ["lookbackDays"],
				},
			],
		});
	});

	router.get("/runs", async (req, res, next) => {
		try {
			const input = parseInput(
				listRunsSchema,
				req.query ?? {},
				"Invalid recovery run history request",
			);
			const whereClauses: string[] = [];
			const values: unknown[] = [];

			if (input.jobType) {
				values.push(input.jobType);
				whereClauses.push(`job_type = $${values.length}`);
			}

			if (input.status) {
				values.push(input.status);
				whereClauses.push(`status = $${values.length}`);
			}

			values.push(input.limit);

			const result = await query<RecoveryRunRow>(
				`
					SELECT ${runColumns}
					FROM recovery_job_runs
					${whereClauses.length ? `WHERE ${whereClauses.join(" AND ")}` : ""}
					ORDER BY queued_at DESC
					LIMIT $${values.length}
				`,
				values,
			);

			res.status(200).json({
				runs: result.rows.map(mapRun),
			});
		} catch (error) {
			next(error);
		}
	});

	router.post("/runs", async (req, res, next) => {
		try {
			const auth = res.locals.auth as AuthContext | null | undefined;
			const input = parseInput(
				createRunSchema,
				req.body ?? {},
				"Invalid manual recovery run request",
			);
			const initiatedBy = getInitiatedBy(auth);
			const { timeRangeStart, timeRangeEnd } = normalizeRange(input);
			const scopeKey =
				input.scopeKey ??
				(input.jobType === SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE
					? "shopify-attribution-hints"
					: "ga4-fallback-unattributed");
			const inputParameters: RecoveryCheckpoint = {
				chunkSize: input.chunkSize,
				pageSize: input.chunkSize,
				...(input.lookbackDays === undefined
					? {}
					: { lookbackDays: input.lookbackDays }),
			};
			const idempotencyKey = buildRecoveryIdempotencyKey({
				jobType: input.jobType,
				scopeKey,
				timeRangeStart,
				timeRangeEnd,
				dryRun: input.dryRun,
				inputParameters,
			});
			const concurrencyKey = buildRecoveryConcurrencyKey({
				scopeKey,
				timeRangeStart,
				timeRangeEnd,
			});
			const store = new PostgresRecoveryJobStore();
			const conflict = await store.findActiveConflict({
				jobType: input.jobType,
				scopeKey,
				timeRangeStart,
				timeRangeEnd,
				idempotencyKey,
			});

			if (conflict) {
				res.status(409).json({
					error: "recovery_run_conflict",
					message: "An overlapping recovery run is already queued or running",
					conflictRunId: conflict.id,
					progressLink: buildProgressLink(conflict.id),
				});
				return;
			}

			const { run, created } = await store.createOrGetRun({
				jobType: input.jobType,
				mode: "manual",
				initiatedBy,
				dryRun: input.dryRun,
				timeRangeStart,
				timeRangeEnd,
				idempotencyKey,
				concurrencyKey,
				scopeKey,
				resumeFromRunId: null,
				rerunOfRunId: null,
				inputParameters,
				checkpoint: {},
				now: new Date(),
			});

			res.status(created ? 201 : 200).json({
				created,
				...buildRunResponse(run),
			});
		} catch (error) {
			next(error);
		}
	});

	router.get("/runs/:runId", async (req, res, next) => {
		try {
			const params = parseInput(
				runIdSchema,
				req.params,
				"Invalid recovery run identifier",
			);
			const run = await getRun(params.runId);

			if (!run) {
				throw new RecoveryAdminHttpError(
					404,
					"recovery_run_not_found",
					"Recovery run was not found",
				);
			}

			res.status(200).json(buildRunResponse(run));
		} catch (error) {
			next(error);
		}
	});

	router.post("/runs/:runId/start", async (req, res, next) => {
		try {
			const params = parseInput(
				runIdSchema,
				req.params,
				"Invalid recovery run identifier",
			);
			const input = parseInput(
				startRunSchema,
				req.body ?? {},
				"Invalid recovery run start request",
			);
			const run = await getRun(params.runId);

			if (!run) {
				throw new RecoveryAdminHttpError(
					404,
					"recovery_run_not_found",
					"Recovery run was not found",
				);
			}

			if (run.mode !== "manual") {
				throw new RecoveryAdminHttpError(
					409,
					"recovery_run_not_manual",
					"Only manual recovery runs can be started from this endpoint",
				);
			}

			if (run.status !== "queued") {
				throw new RecoveryAdminHttpError(
					409,
					"recovery_run_not_startable",
					`Recovery run ${run.id} is ${run.status}`,
				);
			}

			const workerId =
				input.workerId ??
				(run.jobType === SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE
					? "manual-shopify-attribution-recovery"
					: "manual-ga4-fallback-recovery");

			executeRunInBackground(run, workerId);

			res.status(202).json({
				started: true,
				...buildRunResponse(run),
			});
		} catch (error) {
			next(error);
		}
	});

	router.post("/runs/:runId/cancel", async (req, res, next) => {
		try {
			const auth = res.locals.auth as AuthContext | null | undefined;
			const params = parseInput(
				runIdSchema,
				req.params,
				"Invalid recovery run identifier",
			);
			const run = await getRun(params.runId);

			if (!run) {
				throw new RecoveryAdminHttpError(
					404,
					"recovery_run_not_found",
					"Recovery run was not found",
				);
			}

			const cancelled = await cancelRun(
				run,
				getInitiatedBy(auth),
				"operator_cancelled",
			);

			res.status(200).json({
				cancelled: true,
				...buildRunResponse(cancelled),
			});
		} catch (error) {
			next(error);
		}
	});

	return router;
}
