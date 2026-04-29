import assert from "node:assert/strict";
import test from "node:test";

import { __observabilityTestUtils } from "../src/observability/index.js";

test("summarizeResolverOutcome reports unattributed and non-direct winners deterministically", () => {
	const unattributed = __observabilityTestUtils.summarizeResolverOutcome({
		touchpoints: [],
		winner: null,
	});

	assert.equal(unattributed.resolverOutcome, "unattributed");
	assert.equal(unattributed.winnerMatchSource, "unattributed");
	assert.equal(unattributed.ga4SkippedDueToPrecedence, false);

	const ga4FallbackWinner = __observabilityTestUtils.summarizeResolverOutcome({
		touchpoints: [{}],
		winner: {
			isDirect: false,
			ingestionSource: null,
			sessionId: null,
			matchSource: "ga4_fallback",
			source: "google",
			medium: "cpc",
			campaign: "brand",
			clickIdValue: "GCLID-123",
		},
	});

	assert.equal(ga4FallbackWinner.winnerMatchSource, "ga4_fallback");
	assert.equal(ga4FallbackWinner.fallbackUsed, true);
	assert.equal(ga4FallbackWinner.hasClickId, true);
	assert.equal(ga4FallbackWinner.ga4SkippedDueToPrecedence, false);
});

test("summarizeGa4IngestionResult reports lag and fill rates for hourly ingestion health", () => {
	const summary = __observabilityTestUtils.summarizeGa4IngestionResult({
		watermarkBefore: "2026-04-27T08:00:00.000Z",
		watermarkAfter: "2026-04-27T09:00:00.000Z",
		processedHours: ["2026-04-27T09:00:00.000Z"],
		extractedRows: 2,
		upsertedRows: 2,
		now: new Date("2026-04-27T12:35:00.000Z"),
		lagAlertThresholdHours: 2,
		rows: [
			{
				source: "google",
				medium: "cpc",
				campaign: "spring",
				clickIdValue: "GCLID-123",
			},
			{ source: null, medium: "email", campaign: null, clickIdValue: null },
		],
	});

	assert.equal(summary.lagHours, 2);
	assert.equal(summary.lagStatus, "lagging");
	assert.equal(summary.sourcePresentRows, 1);
	assert.equal(summary.mediumPresentRows, 2);
	assert.equal(summary.campaignPresentRows, 1);
	assert.equal(summary.clickIdPresentRows, 1);
	assert.equal(summary.sourceFillRate, 0.5);
	assert.equal(summary.mediumFillRate, 1);
	assert.equal(summary.campaignFillRate, 0.5);
	assert.equal(summary.clickIdFillRate, 0.5);
});

test("emitOrderAttributionBackfillJobLifecycleLog emits structured lifecycle logs with job ids and failure metadata", () => {
	const entries: Array<Record<string, unknown>> = [];
	const originalWrite = process.stdout.write.bind(process.stdout);

	process.stdout.write = ((chunk: string | Uint8Array) => {
		const text =
			typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
		entries.push(JSON.parse(text.trim()) as Record<string, unknown>);
		return true;
	}) as typeof process.stdout.write;

	try {
		__observabilityTestUtils.emitOrderAttributionBackfillJobLifecycleLog({
			stage: "started",
			jobId: "job-1",
			workerId: "worker-1",
			options: {
				startDate: "2026-04-10",
				endDate: "2026-04-12",
				dryRun: true,
				limit: 42,
			},
		});
		__observabilityTestUtils.emitOrderAttributionBackfillJobLifecycleLog({
			stage: "failed",
			jobId: "job-1",
			workerId: "worker-1",
			report: {
				recoveredOrders: 1,
				failedOrders: 2,
				recoverableOrders: 3,
				scannedOrders: 4,
				dryRun: true,
			},
			error: new Error("worker failed"),
		});
	} finally {
		process.stdout.write = originalWrite;
	}

	assert.equal(entries.length, 2);
	assert.equal(entries[0].correlationId, "job-1");
	assert.equal(entries[0].status, "processing");
	assert.equal(entries[1].correlationId, "job-1");
	assert.equal(entries[1].status, "failed");
	assert.equal(entries[1].errorMessage, "worker failed");
	assert.equal(entries[1].recoveredOrders, 1);
	assert.equal(entries[1].failedOrders, 2);
});
