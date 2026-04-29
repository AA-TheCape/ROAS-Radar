import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { logWarning } from "../observability/index.js";
export function buildRawPayloadStorageMetadata(rawPayload) {
    const rawPayloadJson = stableJsonStringify(rawPayload);
    return {
        rawPayload,
        rawPayloadJson,
        payloadSizeBytes: Buffer.byteLength(rawPayloadJson, "utf8"),
        payloadHash: createHash("sha256").update(rawPayloadJson).digest("hex"),
    };
}
function stableJsonValue(value) {
    if (Array.isArray(value)) {
        return value.map((entry) => stableJsonValue(entry));
    }
    if (value && typeof value === "object") {
        const entries = Object.entries(value).sort(([left], [right]) => left.localeCompare(right));
        return Object.fromEntries(entries.map(([key, entry]) => [key, stableJsonValue(entry)]));
    }
    return value;
}
function stableJsonStringify(value) {
    return JSON.stringify(stableJsonValue(value));
}
export function summarizeRawPayloadIntegrity(expected, actual) {
    const persistedMetadata = buildRawPayloadStorageMetadata(actual.persistedRawPayload);
    const payloadMatches = isDeepStrictEqual(actual.persistedRawPayload, expected.rawPayload);
    const storedSizeMatches = actual.storedPayloadSizeBytes === expected.payloadSizeBytes;
    const storedHashMatches = actual.storedPayloadHash === expected.payloadHash;
    const persistedSizeMatches = persistedMetadata.payloadSizeBytes === expected.payloadSizeBytes;
    const persistedHashMatches = persistedMetadata.payloadHash === expected.payloadHash;
    const metadataPresent = actual.storedPayloadSizeBytes !== null && actual.storedPayloadHash !== null;
    return {
        integrityStatus: metadataPresent &&
            payloadMatches &&
            storedSizeMatches &&
            storedHashMatches &&
            persistedSizeMatches &&
            persistedHashMatches
            ? "matched"
            : "mismatched",
        metadataPresent,
        payloadMatches,
        storedSizeMatches,
        storedHashMatches,
        persistedSizeMatches,
        persistedHashMatches,
        expectedPayloadSizeBytes: expected.payloadSizeBytes,
        expectedPayloadHash: expected.payloadHash,
        storedPayloadSizeBytes: actual.storedPayloadSizeBytes,
        storedPayloadHash: actual.storedPayloadHash,
        persistedPayloadSizeBytes: persistedMetadata.payloadSizeBytes,
        persistedPayloadHash: persistedMetadata.payloadHash,
    };
}
export function logRawPayloadIntegrityMismatch(expected, actual, context) {
    const summary = summarizeRawPayloadIntegrity(expected, actual);
    if (summary.integrityStatus === "matched") {
        return;
    }
    logWarning("raw_payload_integrity_mismatch", {
        surface: context.surface,
        operation: context.operation,
        recordId: context.recordId ?? null,
        ...summary,
        ...(context.fields ?? {}),
    });
}
export const __rawPayloadStorageTestUtils = {
    buildRawPayloadStorageMetadata,
    summarizeRawPayloadIntegrity,
    logRawPayloadIntegrityMismatch,
};
