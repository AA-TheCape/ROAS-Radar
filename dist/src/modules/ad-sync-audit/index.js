import { query } from "../../db/pool.js";
function toJsonbLiteral(payload) {
    return JSON.stringify(payload === undefined ? null : payload);
}
function pushQueryParamValue(target, key, value) {
    const existing = target[key];
    if (existing === undefined) {
        target[key] = value;
        return;
    }
    if (Array.isArray(existing)) {
        existing.push(value);
        return;
    }
    target[key] = [existing, value];
}
export function buildSearchParamsAuditPayload(searchParams, redactedKeys = []) {
    const payload = {};
    const redacted = new Set(redactedKeys);
    for (const [key, value] of searchParams.entries()) {
        pushQueryParamValue(payload, key, redacted.has(key) ? "[redacted]" : value);
    }
    return Object.keys(payload).length > 0 ? payload : null;
}
export function parseJsonResponsePayload(text) {
    if (!text) {
        return null;
    }
    try {
        return JSON.parse(text);
    }
    catch {
        return text;
    }
}
export async function recordAdSyncApiTransaction(input) {
    await query(`
      INSERT INTO ad_sync_api_transactions (
        platform,
        connection_id,
        sync_job_id,
        transaction_source,
        source_metadata,
        request_method,
        request_url,
        request_payload,
        request_started_at,
        response_status,
        response_payload,
        response_received_at,
        error_message,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5::jsonb,
        $6,
        $7,
        $8::jsonb,
        $9,
        $10,
        $11::jsonb,
        $12,
        $13,
        now()
      )
    `, [
        input.platform,
        input.connectionId,
        input.syncJobId,
        input.transactionSource,
        toJsonbLiteral(input.sourceMetadata ?? {}),
        input.requestMethod,
        input.requestUrl,
        toJsonbLiteral(input.requestPayload ?? null),
        input.requestStartedAt,
        input.responseStatus ?? null,
        toJsonbLiteral(input.responsePayload ?? null),
        input.responseReceivedAt ?? null,
        input.errorMessage ?? null,
    ]);
}
