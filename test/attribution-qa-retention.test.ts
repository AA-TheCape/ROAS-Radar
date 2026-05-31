import assert from "node:assert/strict";
import test from "node:test";

import type { PoolClient } from "pg";

import { runAttributionQaRetention } from "../src/modules/attribution/qa-retention.js";

type EvidenceRow = {
	id: number;
	retainedUntil: Date;
};

type OrderRow = {
	id: number;
	attributionSnapshotUpdatedAt: Date | null;
	attributionSnapshot: Record<string, unknown> | null;
};

function buildFakeClient(input: {
	evidence: EvidenceRow[];
	orders: OrderRow[];
	rawEvidenceCutoffTimes?: string[];
	snapshotCutoffTimes?: string[];
}): PoolClient {
	return {
		query: async (text: string, params?: unknown[]) => {
			const cutoffAt = params?.[0] as Date;
			const batchSize = params?.[1] as number;

			if (text.includes("DELETE FROM attribution_raw_evidence")) {
				input.rawEvidenceCutoffTimes?.push(cutoffAt.toISOString());
				const expiredIds = input.evidence
					.filter((row) => row.retainedUntil.getTime() < cutoffAt.getTime())
					.sort(
						(left, right) =>
							left.retainedUntil.getTime() - right.retainedUntil.getTime() ||
							left.id - right.id,
					)
					.slice(0, batchSize)
					.map((row) => row.id);

				input.evidence.splice(
					0,
					input.evidence.length,
					...input.evidence.filter((row) => !expiredIds.includes(row.id)),
				);

				return { rows: [], rowCount: expiredIds.length };
			}

			if (text.includes("UPDATE shopify_orders")) {
				input.snapshotCutoffTimes?.push(cutoffAt.toISOString());
				const expiredOrders = input.orders
					.filter(
						(row) =>
							row.attributionSnapshotUpdatedAt &&
							row.attributionSnapshotUpdatedAt.getTime() < cutoffAt.getTime() &&
							row.attributionSnapshot &&
							Object.hasOwn(row.attributionSnapshot, "qaSnapshot"),
					)
					.sort((left, right) => {
						const leftUpdatedAt =
							left.attributionSnapshotUpdatedAt?.getTime() ?? 0;
						const rightUpdatedAt =
							right.attributionSnapshotUpdatedAt?.getTime() ?? 0;
						return leftUpdatedAt - rightUpdatedAt || left.id - right.id;
					})
					.slice(0, batchSize);

				for (const order of expiredOrders) {
					const { qaSnapshot: _qaSnapshot, ...operationalSnapshot } =
						order.attributionSnapshot ?? {};
					order.attributionSnapshot = operationalSnapshot;
				}

				return { rows: [], rowCount: expiredOrders.length };
			}

			throw new Error(`Unexpected query: ${text}`);
		},
	} as PoolClient;
}

test("runAttributionQaRetention deletes only raw evidence and QA snapshots expired before the cleanup boundary", async () => {
	const snapshotWrittenAt = new Date("2026-03-26T12:00:00.000Z");
	const expiryAt = new Date(
		snapshotWrittenAt.getTime() + 30 * 24 * 60 * 60 * 1000,
	);
	const asOf = expiryAt;
	const rawEvidenceCutoffTimes: string[] = [];
	const snapshotCutoffTimes: string[] = [];
	const evidence: EvidenceRow[] = [
		{ id: 1, retainedUntil: new Date(expiryAt.getTime() - 1) },
		{ id: 2, retainedUntil: expiryAt },
		{ id: 3, retainedUntil: asOf },
		{ id: 4, retainedUntil: new Date(asOf.getTime() + 1) },
	];
	const orders: OrderRow[] = [
		{
			id: 1,
			attributionSnapshotUpdatedAt: new Date(snapshotWrittenAt.getTime() - 1),
			attributionSnapshot: {
				tier: "deterministic_first_party",
				qaSnapshot: {},
			},
		},
		{
			id: 2,
			attributionSnapshotUpdatedAt: snapshotWrittenAt,
			attributionSnapshot: {
				tier: "deterministic_first_party",
				qaSnapshot: {},
			},
		},
		{
			id: 3,
			attributionSnapshotUpdatedAt: new Date(snapshotWrittenAt.getTime() + 1),
			attributionSnapshot: {
				tier: "deterministic_first_party",
				qaSnapshot: {},
			},
		},
		{
			id: 4,
			attributionSnapshotUpdatedAt: new Date(asOf.getTime() + 1),
			attributionSnapshot: {
				tier: "deterministic_first_party",
				qaSnapshot: {},
			},
		},
	];
	const client = buildFakeClient({
		evidence,
		orders,
		rawEvidenceCutoffTimes,
		snapshotCutoffTimes,
	});

	const result = await runAttributionQaRetention({
		client,
		asOf,
		retentionDays: 30,
		batchSize: 10,
		maxBatches: 5,
		emitLogs: false,
	});

	assert.deepEqual(result, {
		cutoffAt: "2026-04-25T12:00:00.000Z",
		snapshotCutoffAt: "2026-03-26T12:00:00.000Z",
		retentionDays: 30,
		batchSize: 10,
		maxBatches: 5,
		batchesRun: 1,
		deletedRawEvidence: 1,
		prunedSnapshots: 1,
	});
	assert.deepEqual([...new Set(rawEvidenceCutoffTimes)], [asOf.toISOString()]);
	assert.deepEqual([...new Set(snapshotCutoffTimes)], [
		"2026-03-26T12:00:00.000Z",
	]);
	assert.deepEqual(
		evidence.map((row) => row.id),
		[2, 3, 4],
	);
	assert.deepEqual(orders[0].attributionSnapshot, {
		tier: "deterministic_first_party",
	});
	assert.deepEqual(orders[1].attributionSnapshot, {
		tier: "deterministic_first_party",
		qaSnapshot: {},
	});
	assert.deepEqual(orders[2].attributionSnapshot, {
		tier: "deterministic_first_party",
		qaSnapshot: {},
	});
	assert.deepEqual(orders[3].attributionSnapshot, {
		tier: "deterministic_first_party",
		qaSnapshot: {},
	});

	const secondResult = await runAttributionQaRetention({
		client,
		asOf,
		retentionDays: 30,
		batchSize: 10,
		maxBatches: 5,
		emitLogs: false,
	});

	assert.equal(secondResult.deletedRawEvidence, 0);
	assert.equal(secondResult.prunedSnapshots, 0);
	assert.equal(secondResult.batchesRun, 0);
});

test("runAttributionQaRetention logs cleanup deletion counts for completed batches and runs", async () => {
	const evidence: EvidenceRow[] = [
		{ id: 1, retainedUntil: new Date("2026-04-25T11:59:59.999Z") },
	];
	const orders: OrderRow[] = [
		{
			id: 1,
			attributionSnapshotUpdatedAt: new Date("2026-03-26T11:59:59.999Z"),
			attributionSnapshot: {
				tier: "deterministic_first_party",
				qaSnapshot: {},
			},
		},
	];
	const client = buildFakeClient({ evidence, orders });
	const writes: string[] = [];
	const originalWrite = process.stdout.write;

	process.stdout.write = ((chunk: string | Uint8Array) => {
		writes.push(String(chunk));
		return true;
	}) as typeof process.stdout.write;

	try {
		await runAttributionQaRetention({
			client,
			asOf: new Date("2026-04-25T12:00:00.000Z"),
			retentionDays: 30,
			batchSize: 10,
			maxBatches: 5,
		});
	} finally {
		process.stdout.write = originalWrite;
	}

	const logs = writes
		.join("")
		.trim()
		.split("\n")
		.filter(Boolean)
		.map((line) => JSON.parse(line) as Record<string, unknown>);

	assert.deepEqual(
		logs.map((log) => log.event),
		[
			"attribution_qa_retention_batch_completed",
			"attribution_qa_retention_completed",
		],
	);
	assert.equal(logs[0]?.cleanupDeletionCount, 2);
	assert.equal(logs[0]?.deletedRawEvidenceInBatch, 1);
	assert.equal(logs[0]?.prunedSnapshotsInBatch, 1);
	assert.equal(logs[1]?.cleanupDeletionCount, 2);
	assert.equal(logs[1]?.deletedRawEvidence, 1);
	assert.equal(logs[1]?.prunedSnapshots, 1);
});
