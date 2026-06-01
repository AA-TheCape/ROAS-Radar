import assert from "node:assert/strict";
import test from "node:test";
import type {
	RecoveryJobStore,
	RecoveryRecordStatus,
	RecoveryRun,
} from "../src/modules/recovery/index.js";

process.env.DATABASE_URL ??= "postgres://postgres:postgres@127.0.0.1:5432/roas_radar";

async function getRecoveryModule() {
	return import("../src/modules/recovery/index.js");
}

type Store = RecoveryJobStore;
type Run = RecoveryRun;
type RecordStatus = RecoveryRecordStatus;

type StoredRecord = {
	id: string;
	runId: string;
	jobType: string;
	recordType: string;
	recordKey: string;
	sourceFingerprint: string | null;
	sideEffectKey: string | null;
	processingStatus: RecordStatus;
	attemptCount: number;
};

const MEMORY_RECOVERY_HEARTBEAT_TIMEOUT_MS = 300_000;

class MemoryRecoveryStore implements Store {
	runs = new Map<string, Run>();
	records = new Map<string, StoredRecord>();
	errors: unknown[] = [];
	claimCalls: string[] = [];
	private nextRunId = 1;
	private nextRecordId = 1;

	async createOrGetRun(input: Parameters<Store["createOrGetRun"]>[0]) {
		const existing = [...this.runs.values()].find(
			(run) => run.idempotencyKey === input.idempotencyKey,
		);

		if (existing) {
			return { run: existing, created: false };
		}

		const run: Run = {
			id: `run-${this.nextRunId++}`,
			jobType: input.jobType,
			status: "queued",
			mode: input.mode,
			initiatedBy: input.initiatedBy,
			dryRun: input.dryRun,
			timeRangeStart: input.timeRangeStart,
			timeRangeEnd: input.timeRangeEnd,
			idempotencyKey: input.idempotencyKey,
			concurrencyKey: input.concurrencyKey,
			scopeKey: input.scopeKey,
			resumeFromRunId: input.resumeFromRunId,
			rerunOfRunId: input.rerunOfRunId,
			inputParameters: input.inputParameters,
			checkpoint: input.checkpoint,
			recordsDiscovered: 0,
			recordsClaimed: 0,
			recordsProcessed: 0,
			recordsSucceeded: 0,
			recordsFailed: 0,
			recordsSkipped: 0,
			recordsRetried: 0,
			sideEffectsAttempted: 0,
			sideEffectsSucceeded: 0,
			sideEffectsSuppressed: 0,
			claimedBy: null,
			queuedAt: input.now.toISOString(),
			startedAt: null,
			completedAt: null,
			lastHeartbeatAt: input.now.toISOString(),
			errorCode: null,
			errorMessage: null,
		};

		this.runs.set(run.id, run);
		return { run, created: true };
	}

	async findActiveConflict(input: Parameters<Store["findActiveConflict"]>[0]) {
		const start = new Date(input.timeRangeStart).getTime();
		const end = new Date(input.timeRangeEnd).getTime();

		return (
			[...this.runs.values()].find((run) => {
				const runStart = new Date(run.timeRangeStart).getTime();
				const runEnd = new Date(run.timeRangeEnd).getTime();

				return (
					run.jobType === input.jobType &&
					run.scopeKey === input.scopeKey &&
					run.idempotencyKey !== input.idempotencyKey &&
					(run.status === "queued" || run.status === "running") &&
					runStart <= end &&
					start <= runEnd
				);
			}) ?? null
		);
	}

	async claimRun(runId: string, workerId: string, now: Date) {
		this.claimCalls.push(runId);
		const run = this.mustRun(runId);
		const lastHeartbeatAt = run.lastHeartbeatAt
			? new Date(run.lastHeartbeatAt).getTime()
			: null;
		const heartbeatExpired =
			lastHeartbeatAt !== null &&
			lastHeartbeatAt <= now.getTime() - MEMORY_RECOVERY_HEARTBEAT_TIMEOUT_MS;
		const claimable =
			run.status === "queued" ||
			(run.status === "running" &&
				(run.claimedBy === workerId || run.claimedBy === null || heartbeatExpired));
		if (!claimable) {
			throw new Error(`Recovery run ${runId} is not claimable`);
		}
		const updated: Run = {
			...run,
			status: "running",
			claimedBy: workerId,
			startedAt: run.startedAt ?? now.toISOString(),
			completedAt: null,
			lastHeartbeatAt: now.toISOString(),
		};
		this.runs.set(runId, updated);
		return updated;
	}

	async getRun(runId: string) {
		return this.runs.get(runId) ?? null;
	}

	async upsertRecord(input: Parameters<Store["upsertRecord"]>[0]) {
		const existing = [...this.records.values()].find(
			(record) =>
				record.runId === input.runId &&
				record.recordType === input.recordType &&
				record.recordKey === input.recordKey,
		);

		if (existing) {
			return existing;
		}

		const record: StoredRecord = {
			id: String(this.nextRecordId++),
			runId: input.runId,
			jobType: input.jobType,
			recordType: input.recordType,
			recordKey: input.recordKey,
			sourceFingerprint: input.sourceFingerprint,
			sideEffectKey: input.sideEffectKey,
			processingStatus: "queued",
			attemptCount: 0,
		};
		this.records.set(record.id, record);
		return record;
	}

	async markRecordProcessing(recordId: string) {
		const record = this.mustRecord(recordId);
		record.processingStatus = "processing";
		record.attemptCount += 1;
	}

	async markRecordSucceeded(recordId: string) {
		this.mustRecord(recordId).processingStatus = "succeeded";
	}

	async markRecordSkipped(recordId: string) {
		this.mustRecord(recordId).processingStatus = "skipped";
	}

	async markRecordRetryPending(recordId: string) {
		this.mustRecord(recordId).processingStatus = "retry_pending";
	}

	async markRecordFailed(recordId: string) {
		this.mustRecord(recordId).processingStatus = "failed";
	}

	async updateCheckpoint(
		runId: string,
		workerId: string,
		_checkpointName: string,
		checkpoint: Run["checkpoint"],
		recordsProcessed: number,
		now: Date,
	) {
		const run = this.mustRun(runId);
		this.assertHeld(run, workerId, "checkpoint update");
		const updated: Run = {
			...run,
			checkpoint,
			recordsProcessed: run.recordsProcessed + recordsProcessed,
			lastHeartbeatAt: now.toISOString(),
		};
		this.runs.set(runId, updated);
		return updated;
	}

	async incrementRunCounters(
		runId: string,
		workerId: string,
		counters: Parameters<Store["incrementRunCounters"]>[1],
		now: Date,
	) {
		const run = this.mustRun(runId);
		this.assertHeld(run, workerId, "counter update");
		const updated: Run = {
			...run,
			recordsDiscovered:
				run.recordsDiscovered + (counters.recordsDiscovered ?? 0),
			recordsClaimed: run.recordsClaimed + (counters.recordsClaimed ?? 0),
			recordsSucceeded:
				run.recordsSucceeded + (counters.recordsSucceeded ?? 0),
			recordsFailed: run.recordsFailed + (counters.recordsFailed ?? 0),
			recordsSkipped: run.recordsSkipped + (counters.recordsSkipped ?? 0),
			recordsRetried: run.recordsRetried + (counters.recordsRetried ?? 0),
			sideEffectsAttempted:
				run.sideEffectsAttempted + (counters.sideEffectsAttempted ?? 0),
			sideEffectsSucceeded:
				run.sideEffectsSucceeded + (counters.sideEffectsSucceeded ?? 0),
			sideEffectsSuppressed:
				run.sideEffectsSuppressed + (counters.sideEffectsSuppressed ?? 0),
			lastHeartbeatAt: now.toISOString(),
		};
		this.runs.set(runId, updated);
		return updated;
	}

	async finalizeRun(
		runId: string,
		workerId: string,
		status: Extract<Run["status"], "succeeded" | "partial_failure" | "failed">,
		error: Parameters<Store["finalizeRun"]>[2],
		now: Date,
	) {
		const run = this.mustRun(runId);
		this.assertHeld(run, workerId, "completion");
		const updated: Run = {
			...run,
			status,
			completedAt: now.toISOString(),
			lastHeartbeatAt: now.toISOString(),
			errorCode: error?.code ?? null,
			errorMessage: error?.message ?? null,
		};
		this.runs.set(runId, updated);
		return updated;
	}

	async recordError(input: Parameters<Store["recordError"]>[0]) {
		this.errors.push(input);
	}

	private mustRun(runId: string): Run {
		const run = this.runs.get(runId);
		assert.ok(run, `expected run ${runId}`);
		return run;
	}

	private mustRecord(recordId: string): StoredRecord {
		const record = this.records.get(recordId);
		assert.ok(record, `expected record ${recordId}`);
		return record;
	}

	private assertHeld(run: Run, workerId: string, operation: string): void {
		if (run.status !== "running" || run.claimedBy !== workerId) {
			throw new Error(`Recovery run ${run.id} ${operation} was not accepted`);
		}
	}
}

test("recovery orchestrator blocks overlapping active runs", async () => {
	const { createRecoveryJobOrchestrator } = await getRecoveryModule();
	const store = new MemoryRecoveryStore();
	const orchestrator = createRecoveryJobOrchestrator<number>(
		{
			jobType: "demo",
			fetchPage: async () => ({ records: [], checkpoint: {}, done: true }),
			identifyRecord: (record) => ({
				recordType: "number",
				recordKey: String(record),
			}),
			processRecord: async () => undefined,
		},
		store,
	);

	const first = await orchestrator.start({
		jobType: "demo",
		initiatedBy: "operator",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-10T00:00:00.000Z",
		dryRun: true,
	});
	assert.equal(first.started, true);

	const second = await orchestrator.start({
		jobType: "demo",
		initiatedBy: "operator",
		timeRangeStart: "2026-05-09T00:00:00.000Z",
		timeRangeEnd: "2026-05-12T00:00:00.000Z",
		dryRun: false,
	});

	assert.equal(second.started, false);
	if (!second.started) {
		assert.equal(second.conflict.id, first.run.id);
	}
});

test("legacy recovery store rejects duplicate active claims and stale finalizers", async () => {
	const store = new MemoryRecoveryStore();
	const created = await store.createOrGetRun({
		jobType: "demo",
		mode: "manual",
		initiatedBy: "operator",
		dryRun: false,
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-10T00:00:00.000Z",
		idempotencyKey: "duplicate-claim",
		concurrencyKey: "range:duplicate-claim",
		scopeKey: "global",
		resumeFromRunId: null,
		rerunOfRunId: null,
		inputParameters: {},
		checkpoint: {},
		now: new Date("2026-05-01T00:00:00.000Z"),
	});

	const run = await store.claimRun(
		created.run.id,
		"worker-active",
		new Date("2026-05-01T00:01:00.000Z"),
	);

	await assert.rejects(
		store.claimRun(
			run.id,
			"worker-duplicate",
			new Date("2026-05-01T00:02:00.000Z"),
		),
		/not claimable/,
	);
	await assert.rejects(
		store.finalizeRun(
			run.id,
			"worker-duplicate",
			"succeeded",
			null,
			new Date("2026-05-01T00:03:00.000Z"),
		),
		/completion was not accepted/,
	);

	assert.equal(store.runs.get(run.id)?.claimedBy, "worker-active");
	assert.equal(store.runs.get(run.id)?.status, "running");
});

test("legacy recovery store allows explicit stale claim recovery", async () => {
	const store = new MemoryRecoveryStore();
	const created = await store.createOrGetRun({
		jobType: "demo",
		mode: "manual",
		initiatedBy: "operator",
		dryRun: false,
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-10T00:00:00.000Z",
		idempotencyKey: "stale-claim",
		concurrencyKey: "range:stale-claim",
		scopeKey: "global",
		resumeFromRunId: null,
		rerunOfRunId: null,
		inputParameters: {},
		checkpoint: {},
		now: new Date("2026-05-01T00:00:00.000Z"),
	});

	const firstClaim = await store.claimRun(
		created.run.id,
		"worker-stale",
		new Date("2026-05-01T00:01:00.000Z"),
	);
	const recovered = await store.claimRun(
		firstClaim.id,
		"worker-recovery",
		new Date("2026-05-01T00:07:00.001Z"),
	);
	await assert.rejects(
		store.finalizeRun(
			firstClaim.id,
			"worker-stale",
			"succeeded",
			null,
			new Date("2026-05-01T00:08:00.000Z"),
		),
		/completion was not accepted/,
	);
	const finalized = await store.finalizeRun(
		recovered.id,
		"worker-recovery",
		"succeeded",
		null,
		new Date("2026-05-01T00:09:00.000Z"),
	);

	assert.equal(recovered.claimedBy, "worker-recovery");
	assert.equal(finalized.status, "succeeded");
});

test("recovery orchestrator paginates, checkpoints, and suppresses side effects in dry run", async () => {
	const { createRecoveryJobOrchestrator } = await getRecoveryModule();
	const store = new MemoryRecoveryStore();
	const processed: number[] = [];
	const records = [1, 2, 3];
	const orchestrator = createRecoveryJobOrchestrator<number>(
		{
			jobType: "demo",
			pageSize: 2,
			fetchPage: async (context) => {
				const offset =
					typeof context.checkpoint.offset === "number"
						? context.checkpoint.offset
						: 0;
				const page = records.slice(offset, offset + context.pageSize);
				const nextOffset = offset + page.length;
				return {
					records: page,
					checkpoint: { offset: nextOffset },
					done: nextOffset >= records.length,
				};
			},
			identifyRecord: (record) => ({
				recordType: "number",
				recordKey: String(record),
			}),
			processRecord: async (record) => {
				processed.push(record);
			},
		},
		store,
	);

	const result = await orchestrator.startAndExecute(
		{
			jobType: "demo",
			initiatedBy: "operator",
			timeRangeStart: "2026-05-01T00:00:00.000Z",
			timeRangeEnd: "2026-05-10T00:00:00.000Z",
			dryRun: true,
		},
		"worker-1",
	);

	assert.equal("run" in result, true);
	if ("run" in result) {
		assert.equal(result.pagesProcessed, 2);
		assert.equal(result.run.status, "succeeded");
		assert.deepEqual(result.run.checkpoint, { offset: 3 });
		assert.equal(result.run.recordsSkipped, 3);
		assert.equal(result.run.sideEffectsSuppressed, 3);
	}
	assert.deepEqual(processed, []);
});

test("recovery orchestrator retries with exponential backoff before partial failure", async () => {
	const { calculateRecoveryBackoffMs, createRecoveryJobOrchestrator } =
		await getRecoveryModule();
	const store = new MemoryRecoveryStore();
	const orchestrator = createRecoveryJobOrchestrator<number>(
		{
			jobType: "demo",
			maxAttempts: 2,
			backoffBaseMs: 0,
			fetchPage: async () => ({
				records: [1],
				checkpoint: { complete: true },
				done: true,
			}),
			identifyRecord: (record) => ({
				recordType: "number",
				recordKey: String(record),
			}),
			processRecord: async () => {
				throw new Error("temporary outage");
			},
		},
		store,
	);

	const result = await orchestrator.startAndExecute(
		{
			jobType: "demo",
			initiatedBy: "operator",
			timeRangeStart: "2026-05-01T00:00:00.000Z",
			timeRangeEnd: "2026-05-10T00:00:00.000Z",
			dryRun: false,
		},
		"worker-1",
	);

	assert.equal(calculateRecoveryBackoffMs(1, 500, 10_000), 500);
	assert.equal(calculateRecoveryBackoffMs(3, 500, 10_000), 2_000);
	assert.equal("run" in result, true);
	if ("run" in result) {
		assert.equal(result.run.status, "partial_failure");
		assert.equal(result.run.recordsRetried, 1);
		assert.equal(result.run.recordsFailed, 1);
		assert.equal(store.errors.length, 2);
	}
});

test("recovery orchestrator resumes from the persisted checkpoint on an existing run", async () => {
	const { createRecoveryJobOrchestrator } = await getRecoveryModule();
	const store = new MemoryRecoveryStore();
	const fetchedOffsets: number[] = [];
	const processed: number[] = [];
	const records = [1, 2, 3];
	const orchestrator = createRecoveryJobOrchestrator<number>(
		{
			jobType: "demo",
			pageSize: 2,
			fetchPage: async (context) => {
				const offset =
					typeof context.checkpoint.offset === "number"
						? context.checkpoint.offset
						: 0;
				fetchedOffsets.push(offset);
				const page = records.slice(offset, offset + context.pageSize);
				const nextOffset = offset + page.length;

				return {
					records: page,
					checkpoint: { offset: nextOffset },
					done: nextOffset >= records.length,
				};
			},
			identifyRecord: (record) => ({
				recordType: "number",
				recordKey: String(record),
			}),
			processRecord: async (record) => {
				processed.push(record);
				return { status: "succeeded" };
			},
		},
		store,
	);

	const started = await orchestrator.start({
		jobType: "demo",
		initiatedBy: "operator",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-10T00:00:00.000Z",
		dryRun: false,
		inputParameters: { pageSize: 2 },
		resumeFromRunId: "run-interrupted",
	});
	assert.equal(started.started, true);
	if (!started.started) {
		return;
	}

	store.runs.set(started.run.id, {
		...started.run,
		checkpoint: { offset: 1 },
	});

	const result = await orchestrator.execute(started.run.id, "worker-resume");

	assert.equal(result.run.status, "succeeded");
	assert.deepEqual(fetchedOffsets, [1]);
	assert.deepEqual(processed, [2, 3]);
	assert.deepEqual(result.run.checkpoint, { offset: 3 });
	assert.equal(result.recordsProcessed, 2);
});

test("recovery orchestrator reuses terminal idempotency keys without replaying side effects", async () => {
	const { createRecoveryJobOrchestrator } = await getRecoveryModule();
	const store = new MemoryRecoveryStore();
	const processed: number[] = [];
	const request = {
		jobType: "demo",
		initiatedBy: "operator",
		timeRangeStart: "2026-05-01T00:00:00.000Z",
		timeRangeEnd: "2026-05-10T00:00:00.000Z",
		dryRun: false,
	};
	const orchestrator = createRecoveryJobOrchestrator<number>(
		{
			jobType: "demo",
			fetchPage: async () => ({
				records: [1],
				checkpoint: { done: true },
				done: true,
			}),
			identifyRecord: (record) => ({
				recordType: "number",
				recordKey: String(record),
			}),
			processRecord: async (record) => {
				processed.push(record);
				return {
					status: "succeeded",
					sideEffectAttempted: true,
					sideEffectSucceeded: true,
				};
			},
		},
		store,
	);

	const first = await orchestrator.startAndExecute(request, "worker-1");
	const second = await orchestrator.startAndExecute(request, "worker-2");

	assert.equal("run" in first, true);
	assert.equal("run" in second, true);
	if ("run" in first && "run" in second) {
		assert.equal(first.run.status, "succeeded");
		assert.equal(second.run.id, first.run.id);
		assert.equal(second.run.status, "succeeded");
		assert.equal(second.pagesProcessed, 0);
		assert.equal(second.recordsProcessed, 0);
	}
	assert.deepEqual(processed, [1]);
	assert.deepEqual(store.claimCalls, ["run-1"]);
});
