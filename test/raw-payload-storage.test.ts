import assert from "node:assert/strict";
import test from "node:test";

const { __rawPayloadStorageTestUtils } = await import(
	"../src/shared/raw-payload-storage.js"
);

test("summarizeRawPayloadIntegrity reports matched payload metadata when stored JSONB stays exact", () => {
	const metadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata({
		id: "payload-1",
		nested: {
			untouched: true,
		},
	});

	assert.deepEqual(
		__rawPayloadStorageTestUtils.summarizeRawPayloadIntegrity(metadata, {
			storedPayloadSizeBytes: metadata.payloadSizeBytes,
			storedPayloadHash: metadata.payloadHash,
			persistedRawPayload: {
				id: "payload-1",
				nested: {
					untouched: true,
				},
			},
		}).integrityStatus,
		"matched",
	);
});

test("buildRawPayloadStorageMetadata preserves nested unknown fields, arrays, nulls, and large payload sizes", () => {
	const rawPayload = {
		id: "payload-3",
		nested: {
			unknown: {
				keep: ["first", null, { deep: ["value", 42, false] }],
			},
		},
		arrayField: [{ name: "alpha" }, { name: "beta", values: [1, 2, 3] }],
		nullableField: null,
		largeText: "raw-segment-".repeat(1024),
	};

	const metadata =
		__rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(rawPayload);

	assert.deepEqual(metadata.rawPayload, rawPayload);
	assert.ok(metadata.rawPayloadJson.includes('"nullableField":null'));
	assert.ok(metadata.rawPayloadJson.includes('"largeText"'));
	assert.ok(metadata.payloadSizeBytes > 12_000);
	assert.match(metadata.payloadHash, /^[a-f0-9]{64}$/);
	assert.equal(
		__rawPayloadStorageTestUtils.summarizeRawPayloadIntegrity(metadata, {
			storedPayloadSizeBytes: metadata.payloadSizeBytes,
			storedPayloadHash: metadata.payloadHash,
			persistedRawPayload: rawPayload,
		}).integrityStatus,
		"matched",
	);
});

test("logRawPayloadIntegrityMismatch emits a monitoring warning for mismatched stored payloads", (t) => {
	const writes: string[] = [];

	t.mock.method(process.stdout, "write", (chunk: string | Uint8Array) => {
		writes.push(
			typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"),
		);
		return true;
	});

	const metadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata({
		id: "payload-2",
		source: "shopify",
	});

	__rawPayloadStorageTestUtils.logRawPayloadIntegrityMismatch(
		metadata,
		{
			storedPayloadSizeBytes: metadata.payloadSizeBytes - 1,
			storedPayloadHash: metadata.payloadHash,
			persistedRawPayload: {
				id: "payload-2",
				source: "shopify",
			},
		},
		{
			surface: "shopify_orders",
			operation: "upsert",
			recordId: "payload-2",
		},
	);

	assert.equal(writes.length, 1);

	const payload = JSON.parse(writes[0]);
	assert.equal(payload.severity, "WARNING");
	assert.equal(payload.event, "raw_payload_integrity_mismatch");
	assert.equal(payload.surface, "shopify_orders");
	assert.equal(payload.operation, "upsert");
	assert.equal(payload.recordId, "payload-2");
	assert.equal(payload.integrityStatus, "mismatched");
	assert.equal(payload.storedSizeMatches, false);
});
