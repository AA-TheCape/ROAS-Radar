import assert from "node:assert/strict";
import test from "node:test";
import type {
	EnqueueRecoveryJobInput,
	RecoveryJobCounters,
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
			recordsFailed: run.recordsFailed + (update.counters?.recordsFailed ?? 0),
			recordsSkipped:
				run.recordsSkipped + (update.counters?.recordsSkipped ?? 0),
			recordsRetried:
				run.recordsRetried + (update.counters?.recordsRetried ?? 0),
			sideEffectsAttempted:
				run.sideEffectsAttempted +
				(update.counters?.sideEffectsAttempted ?? 0),
			sideEffectsSucceeded:
				run.sideEffectsSucceeded +
				(update.counters?.sideEffectsSucceeded ?? 0),
			sideEffectsSuppressed:
				run.sideEffectsSuppressed +
				(update.counters?.sideEffectsSuppressed ?? 0),
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
		counters: Partial<RecoveryJobCounters> = {},
	) {
		const run = this.mustRun(runId, workerId);
		const updated: RecoveryJobRun = {
			...run,
			status,
			completedAt: new Date().toISOString(),
			completionReport: report,
			recordsDiscovered:
				run.recordsDiscovered + (counters.recordsDiscovered ?? 0),
			recordsProcessed: run.recordsProcessed + (counters.recordsProcessed ?? 0),
			recordsSucceeded: run.recordsSucceeded + (counters.recordsSucceeded ?? 0),
			recordsFailed: run.recordsFailed + (counters.recordsFailed ?? 0),
			recordsSkipped: run.recordsSkipped + (counters.recordsSkipped ?? 0),
			recordsRetried: run.recordsRetried + (counters.recordsRetried ?? 0),
			sideEffectsAttempted:
				run.sideEffectsAttempted + (counters.sideEffectsAttempted ?? 0),
			sideEffectsSucceeded:
				run.sideEffectsSucceeded + (counters.sideEffectsSucceeded ?? 0),
			sideEffectsSuppressed:
				run.sideEffectsSuppressed + (counters.sideEffectsSuppressed ?? 0),
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

	async recoverStale(now = new Date()) {
		const recovered: RecoveryJobRun[] = [];
		for (const run of this.runs.values()) {
			if (
				run.status !== "running" ||
				!run.lockExpiresAt ||
				new Date(run.lockExpiresAt).getTime() >= now.getTime()
			) {
				continue;
			}
			const deadLetter = run.attemptCount >= run.maxAttempts;
			const updated: RecoveryJobRun = {
				...run,
				status: deadLetter ? "dead_lettered" : "queued",
				claimedBy: null,
				lockExpiresAt: null,
				errorCode: "recovery_job_heartbeat_expired",
				errorMessage: `Recovery job heartbeat expired for worker ${run.claimedBy}`,
				deadLetteredAt: deadLetter ? now.toISOString() : null,
			};
			this.runs.set(run.id, updated);
			recovered.push(updated);
		}
		return recovered;
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

test("recovery job executor propagates dry-run and suppresses side effects", async () => {
	const { createRecoveryJobExecutor } = await getRecoveryJobsModule();
	const store = new MemoryRecoveryJobStore();
	const sideEffects: string[] = [];
	const executor = createRecoveryJobExecutor(
		[
			{
				jobType: "dry-run-demo",
				run: async (context) => {
					if (!context.dryRun) {
						sideEffects.push(String(context.run.payload.orderId));
					}
					return {
						report: { dryRun: context.dryRun },
						counters: {
							recordsProcessed: 1,
							sideEffectsAttempted: 1,
							sideEffectsSucceeded: context.dryRun ? 0 : 1,
							sideEffectsSuppressed: context.dryRun ? 1 : 0,
						},
					};
				},
			},
		],
		store,
	);

	await executor.enqueue({
		jobType: "dry-run-demo",
		initiatedBy: "test",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		payload: { orderId: "order-1" },
	});

	const result = await executor.executeNext({ workerId: "worker-1" });

	assert.equal(result.claimed, true);
	assert.deepEqual(sideEffects, []);
	if (result.claimed) {
		assert.deepEqual(result.run.completionReport, { dryRun: true });
		assert.equal(result.run.sideEffectsAttempted, 1);
		assert.equal(result.run.sideEffectsSucceeded, 0);
		assert.equal(result.run.sideEffectsSuppressed, 1);
	}
});

test("recovery job executor classifies retryable and permanent failures", async () => {
	const { createRecoveryJobExecutor } = await getRecoveryJobsModule();
	const store = new MemoryRecoveryJobStore();
	const executor = createRecoveryJobExecutor(
		[
			{
				jobType: "retryable-demo",
				run: async () => {
					throw {
						code: "upstream_timeout",
						message: "Temporary upstream outage",
						details: { upstream: "ga4" },
						retryable: true,
					};
				},
			},
			{
				jobType: "permanent-demo",
				run: async () => {
					throw {
						code: "invalid_schema",
						message: "Payload cannot be recovered",
						details: { field: "shopifyOrderId" },
						retryable: false,
					};
				},
			},
		],
		store,
	);

	await executor.enqueue({
		jobType: "retryable-demo",
		initiatedBy: "test",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		maxAttempts: 2,
	});

	const retryable = await executor.executeNext({
		workerId: "worker-1",
		jobTypes: ["retryable-demo"],
	});
	assert.equal(retryable.claimed, true);
	if (retryable.claimed) {
		assert.equal(retryable.run.status, "queued");
		assert.equal(retryable.run.errorCode, "upstream_timeout");
		assert.deepEqual(retryable.run.lastErrorDetails, { upstream: "ga4" });
	}

	await executor.enqueue({
		jobType: "permanent-demo",
		initiatedBy: "test",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
	});

	const permanent = await executor.executeNext({
		workerId: "worker-1",
		jobTypes: ["permanent-demo"],
	});
	assert.equal(permanent.claimed, true);
	if (permanent.claimed) {
		assert.equal(permanent.run.status, "dead_lettered");
		assert.equal(permanent.run.errorCode, "invalid_schema");
		assert.ok(permanent.run.deadLetteredAt);
	}
});

test("recovery job executor recovers stale runs for retry or dead-letter replay", async () => {
	const { createRecoveryJobExecutor } = await getRecoveryJobsModule();
	const store = new MemoryRecoveryJobStore();
	const executor = createRecoveryJobExecutor(
		[
			{
				jobType: "stale-demo",
				run: async () => ({ processed: true }),
			},
		],
		store,
	);

	await executor.enqueue({
		jobType: "stale-demo",
		initiatedBy: "test",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		heartbeatTimeoutSeconds: 1,
		maxAttempts: 2,
	});
	const claimed = await store.claimNext({ workerId: "worker-1" });
	assert.ok(claimed);
	store.runs.set(claimed.id, {
		...claimed,
		lockExpiresAt: "2026-05-01T00:00:01.000Z",
	});

	const recovered = await executor.recoverStale(
		new Date("2026-05-01T00:00:02.000Z"),
	);

	assert.equal(recovered.length, 1);
	assert.equal(recovered[0].status, "queued");
	assert.equal(recovered[0].claimedBy, null);
	assert.equal(recovered[0].errorCode, "recovery_job_heartbeat_expired");
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

test("registered recovery worker queue polls, claims, and completes queued work", async () => {
	const { createRecoveryJobExecutor } = await getRecoveryJobsModule();
	const { processRegisteredRecoveryJobs } = await import(
		"../src/modules/recovery-jobs/registered.js"
	);
	const store = new MemoryRecoveryJobStore();
	const processedRunIds: string[] = [];
	const executor = createRecoveryJobExecutor(
		[
			{
				jobType: "shopify_attribution_hint_recovery",
				run: async (context) => {
					processedRunIds.push(context.run.id);
					await context.heartbeat();
					return {
						report: { migratedJob: context.run.jobType },
						counters: {
							recordsDiscovered: 1,
							recordsProcessed: 1,
							recordsSucceeded: 1,
						},
					};
				},
			},
		],
		store,
	);

	const enqueued = await executor.enqueue({
		jobType: "shopify_attribution_hint_recovery",
		mode: "manual",
		initiatedBy: "operator@example.com",
		dryRun: true,
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		payload: { pageSize: 50 },
	});

	const result = await processRegisteredRecoveryJobs({
		workerId: "worker-1",
		limit: 1,
		executor,
	});

	assert.equal(result.claimed, 1);
	assert.equal(result.completed, 1);
	assert.equal(result.lastRun?.status, "succeeded");
	assert.deepEqual(processedRunIds, [enqueued.run.id]);
	assert.equal(store.runs.get(enqueued.run.id)?.claimedBy, "worker-1");
});

test("registered recovery worker routes adapter failures through shared retry and dead-letter lifecycle", async () => {
	const { createRecoveryJobExecutor } = await getRecoveryJobsModule();
	const { createRegisteredRecoveryJobDefinitions, processRegisteredRecoveryJobs } =
		await import("../src/modules/recovery-jobs/registered.js");
	const store = new MemoryRecoveryJobStore();
	const observedManagesCompletion: Array<boolean | undefined> = [];
	const executor = createRecoveryJobExecutor(
		createRegisteredRecoveryJobDefinitions({
			executeRegisteredRun: async (_run, _workerId, _now, options) => {
				observedManagesCompletion.push(options?.managesCompletion);
				throw {
					code: "registered_upstream_timeout",
					message: "Registered dependency timed out",
					details: { dependency: "shopify" },
					retryable: true,
				};
			},
		}),
		store,
	);

	const enqueued = await executor.enqueue({
		jobType: "shopify_attribution_hint_recovery",
		mode: "automatic",
		initiatedBy: "scheduler",
		dryRun: false,
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		maxAttempts: 2,
	});

	const retry = await processRegisteredRecoveryJobs({
		workerId: "registered-worker-1",
		limit: 1,
		executor,
	});
	const retryRun = store.runs.get(enqueued.run.id);
	assert.equal(retry.claimed, 1);
	assert.equal(retry.completed, 0);
	assert.equal(retry.deadLettered, 0);
	assert.equal(retryRun?.status, "queued");
	assert.equal(retryRun?.attemptCount, 1);
	assert.equal(retryRun?.errorCode, "registered_upstream_timeout");
	assert.deepEqual(retryRun?.lastErrorDetails, { dependency: "shopify" });

	const deadLetter = await processRegisteredRecoveryJobs({
		workerId: "registered-worker-2",
		limit: 1,
		executor,
	});
	const deadLetterRun = store.runs.get(enqueued.run.id);
	assert.equal(deadLetter.claimed, 1);
	assert.equal(deadLetter.completed, 0);
	assert.equal(deadLetter.deadLettered, 1);
	assert.equal(deadLetterRun?.status, "dead_lettered");
	assert.equal(deadLetterRun?.attemptCount, 2);
	assert.equal(deadLetterRun?.errorCode, "registered_upstream_timeout");
	assert.ok(deadLetterRun?.deadLetteredAt);
	assert.deepEqual(observedManagesCompletion, [false, false]);
});

test("registered recovery worker recovers expired heartbeats before claiming work", async () => {
	const { createRecoveryJobExecutor } = await getRecoveryJobsModule();
	const { processRegisteredRecoveryJobs } = await import(
		"../src/modules/recovery-jobs/registered.js"
	);
	const store = new MemoryRecoveryJobStore();
	const executor = createRecoveryJobExecutor(
		[
			{
				jobType: "shopify_attribution_hint_recovery",
				run: async () => ({ recovered: true }),
			},
		],
		store,
	);

	await executor.enqueue({
		jobType: "shopify_attribution_hint_recovery",
		mode: "manual",
		initiatedBy: "operator@example.com",
		dryRun: true,
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-01T23:59:59.999Z",
		heartbeatTimeoutSeconds: 1,
		maxAttempts: 2,
	});
	const claimed = await store.claimNext({ workerId: "worker-1" });
	assert.ok(claimed);
	store.runs.set(claimed.id, {
		...claimed,
		lockExpiresAt: "2026-05-01T00:00:01.000Z",
	});

	const result = await processRegisteredRecoveryJobs({
		workerId: "worker-2",
		limit: 0,
		executor,
	});

	const recovered = store.runs.get(claimed.id);
	assert.equal(result.recoveredStale, 1);
	assert.equal(result.claimed, 0);
	assert.equal(recovered?.status, "queued");
	assert.equal(recovered?.claimedBy, null);
	assert.equal(recovered?.errorCode, "recovery_job_heartbeat_expired");
});
