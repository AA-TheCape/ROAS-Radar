import assert from "node:assert/strict";
import test from "node:test";
import type {
	EnqueueRecoveryJobInput,
	RecoveryJobError,
	RecoveryJobPayload,
	RecoveryJobProgressUpdate,
	RecoveryJobRun,
	RecoveryJobStore,
} from "../src/modules/recovery-jobs/index.js";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";

async function getRecoveryJobsModule() {
	return import("../src/modules/recovery-jobs/index.js");
}

class MemoryRecoveryJobStore implements RecoveryJobStore {
	runs = new Map<string, RecoveryJobRun>();
	private nextRunId = 1;

	async enqueue<TJobType extends string, TPayload extends RecoveryJobPayload>(
		input: EnqueueRecoveryJobInput<TJobType, TPayload>,
	) {
		const { buildRecoveryJobConcurrencyKey, buildRecoveryJobIdempotencyKey } =
			await getRecoveryJobsModule();
		const now = input.now ?? new Date();
		const timeRangeStart = new Date(input.timeRangeStart).toISOString();
		const timeRangeEnd = new Date(input.timeRangeEnd).toISOString();
		const scopeKey = input.scopeKey ?? "global";
		const payload = input.payload ?? ({} as TPayload);
		const idempotencyKey =
			input.idempotencyKey ??
			buildRecoveryJobIdempotencyKey({
				jobType: input.jobType,
				scopeKey,
				timeRangeStart,
				timeRangeEnd,
				dryRun: input.dryRun ?? true,
				payload,
			});

		const existing = [...this.runs.values()].find(
			(run) => run.idempotencyKey === idempotencyKey,
		);
		if (existing) {
			return { run: existing as RecoveryJobRun<TPayload>, created: false };
		}

		const run: RecoveryJobRun<TPayload> = {
			id: `run-${this.nextRunId++}`,
			jobType: input.jobType,
			status: "queued",
			mode: input.mode ?? "automatic",
			initiatedBy: input.initiatedBy,
			dryRun: input.dryRun ?? true,
			timeRangeStart,
			timeRangeEnd,
			idempotencyKey,
			concurrencyKey:
				input.concurrencyKey ??
				buildRecoveryJobConcurrencyKey({
					jobType: input.jobType,
					scopeKey,
					timeRangeStart,
					timeRangeEnd,
				}),
			scopeKey,
			payload,
			checkpoint: {},
			recordsDiscovered: 0,
			recordsProcessed: 0,
			recordsSucceeded: 0,
			recordsFailed: 0,
			recordsSkipped: 0,
			recordsRetried: 0,
			sideEffectsAttempted: 0,
			sideEffectsSucceeded: 0,
			sideEffectsSuppressed: 0,
			priority: input.priority ?? 100,
			availableAt: now.toISOString(),
			attemptCount: 0,
			maxAttempts: input.maxAttempts ?? 3,
			heartbeatTimeoutSeconds: input.heartbeatTimeoutSeconds ?? 300,
			claimedBy: null,
			queuedAt: now.toISOString(),
			startedAt: null,
			completedAt: null,
			lastHeartbeatAt: now.toISOString(),
			lockExpiresAt: null,
			deadLetteredAt: null,
			errorCode: null,
			errorMessage: null,
			lastErrorDetails: {},
			completionReport: {},
		};
		this.runs.set(run.id, run);
		return { run, created: true };
	}

	async claimNext(input: {
		workerId: string;
		jobTypes?: string[];
		now?: Date;
	}) {
		const now = input.now ?? new Date();
		const run = [...this.runs.values()]
			.filter(
				(candidate) =>
					candidate.status === "queued" &&
					(!input.jobTypes?.length ||
						input.jobTypes.includes(candidate.jobType)),
			)
			.sort((a, b) => a.priority - b.priority)[0];
		if (!run) {
			return null;
		}

		const updated: RecoveryJobRun = {
			...run,
			status: "running",
			claimedBy: input.workerId,
			attemptCount: run.attemptCount + 1,
			startedAt: run.startedAt ?? now.toISOString(),
			lastHeartbeatAt: now.toISOString(),
			lockExpiresAt: new Date(
				now.getTime() + run.heartbeatTimeoutSeconds * 1000,
			).toISOString(),
		};
		this.runs.set(run.id, updated);
		return updated;
	}

	async heartbeat(runId: string, workerId: string, now = new Date()) {
		const run = this.mustRun(runId, workerId);
		const updated = { ...run, lastHeartbeatAt: now.toISOString() };
		this.runs.set(runId, updated);
		return updated;
	}

	async updateProgress(
		runId: string,
		workerId: string,
		update: RecoveryJobProgressUpdate,
		now = new Date(),
	) {
		const run = this.mustRun(runId, workerId);
		const updated = {
			...run,
			checkpoint: update.checkpoint ?? run.checkpoint,
			recordsDiscovered:
				run.recordsDiscovered + (update.counters?.recordsDiscovered ?? 0),
			recordsProcessed:
				run.recordsProcessed + (update.counters?.recordsProcessed ?? 0),
			recordsSucceeded:
				run.recordsSucceeded + (update.counters?.recordsSucceeded ?? 0),
			lastHeartbeatAt: now.toISOString(),
		};
		this.runs.set(runId, updated);
		return updated;
	}

	async complete(
		runId: string,
		workerId: string,
		status: "succeeded" | "partial_failure" | "failed",
		report: RecoveryJobPayload,
	) {
		const run = this.mustRun(runId, workerId);
		const updated: RecoveryJobRun = {
			...run,
			status,
			completedAt: new Date().toISOString(),
			completionReport: report,
		};
		this.runs.set(runId, updated);
		return updated;
	}

	async fail(
		runId: string,
		workerId: string,
		error: RecoveryJobError,
		now = new Date(),
	) {
		const run = this.mustRun(runId, workerId);
		const deadLetter = !error.retryable || run.attemptCount >= run.maxAttempts;
		const updated: RecoveryJobRun = {
			...run,
			status: deadLetter ? "dead_lettered" : "queued",
			claimedBy: null,
			errorCode: error.code,
			errorMessage: error.message,
			lastErrorDetails: error.details,
			deadLetteredAt: deadLetter ? now.toISOString() : null,
		};
		this.runs.set(runId, updated);
		return updated;
	}

	async recoverStale() {
		return [];
	}

	async getRun(runId: string) {
		return this.runs.get(runId) ?? null;
	}

	private mustRun(runId: string, workerId: string) {
		const run = this.runs.get(runId);
		assert.ok(run, `expected run ${runId}`);
		assert.equal(run.claimedBy, workerId);
		return run;
	}
}

test("recovery job idempotency keys are stable for object key ordering", async () => {
	const { __recoveryJobTestUtils } = await getRecoveryJobsModule();
	const first = __recoveryJobTestUtils.buildRecoveryJobIdempotencyKey({
		jobType: "demo",
		scopeKey: "global",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		dryRun: false,
		payload: { b: 2, a: 1 },
	});
	const second = __recoveryJobTestUtils.buildRecoveryJobIdempotencyKey({
		jobType: "demo",
		scopeKey: "global",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		dryRun: false,
		payload: { a: 1, b: 2 },
	});

	assert.equal(first, second);
	assert.equal(__recoveryJobTestUtils.calculateRecoveryJobBackoffMs(1), 30_000);
	assert.equal(
		__recoveryJobTestUtils.calculateRecoveryJobBackoffMs(3),
		120_000,
	);
});

test("recovery job executor claims, reports progress, and completes a job", async () => {
	const { createRecoveryJobExecutor } = await getRecoveryJobsModule();
	const store = new MemoryRecoveryJobStore();
	const executor = createRecoveryJobExecutor(
		[
			{
				jobType: "demo",
				parsePayload: (payload) => ({
					orderId: String(payload.orderId),
				}),
				run: async (context) => {
					assert.equal(context.run.payload.orderId, "order-1");
					await context.updateProgress({
						checkpoint: { cursor: "order-1" },
						counters: {
							recordsDiscovered: 1,
							recordsProcessed: 1,
							recordsSucceeded: 1,
						},
					});
					return { report: { processedOrderId: context.run.payload.orderId } };
				},
			},
		],
		store,
	);

	const enqueued = await executor.enqueue({
		jobType: "demo",
		initiatedBy: "test",
		dryRun: false,
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		payload: { orderId: "order-1" },
	});
	const duplicate = await executor.enqueue({
		jobType: "demo",
		initiatedBy: "test",
		dryRun: false,
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		payload: { orderId: "order-1" },
	});

	assert.equal(enqueued.created, true);
	assert.equal(duplicate.created, false);
	assert.equal(duplicate.run.id, enqueued.run.id);

	const result = await executor.executeNext({ workerId: "worker-1" });
	assert.equal(result.claimed, true);
	if (result.claimed) {
		assert.equal(result.run.status, "succeeded");
		assert.equal(result.run.recordsProcessed, 1);
		assert.deepEqual(result.run.checkpoint, { cursor: "order-1" });
		assert.deepEqual(result.run.completionReport, {
			processedOrderId: "order-1",
		});
	}
});

test("recovery job executor dead-letters unknown job types", async () => {
	const { createRecoveryJobExecutor } = await getRecoveryJobsModule();
	const store = new MemoryRecoveryJobStore();
	const executor = createRecoveryJobExecutor([], store);

	await executor.enqueue({
		jobType: "missing",
		initiatedBy: "test",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
	});

	const result = await executor.executeNext({ workerId: "worker-1" });
	assert.equal(result.claimed, true);
	if (result.claimed) {
		assert.equal(result.run.status, "dead_lettered");
		assert.equal(result.run.errorCode, "unknown_recovery_job_type");
	}
});
