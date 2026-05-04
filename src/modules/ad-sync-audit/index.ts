import { query } from "../../db/pool.js";

type AdSyncPlatform = "meta_ads" | "google_ads";

type AdSyncApiTransactionInput = {
	platform: AdSyncPlatform;
	connectionId: number;
	syncJobId: number;
	transactionSource: string;
	sourceMetadata?: Record<string, unknown>;
	requestMethod: string;
	requestUrl: string;
	requestPayload?: unknown;
	requestStartedAt: Date;
	responseStatus?: number | null;
	responsePayload?: unknown;
	responseReceivedAt?: Date | null;
	errorMessage?: string | null;
};

function toJsonbLiteral(payload: unknown): string {
	return JSON.stringify(payload === undefined ? null : payload);
}

function pushQueryParamValue(
	target: Record<string, string | string[]>,
	key: string,
	value: string,
): void {
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

export function buildSearchParamsAuditPayload(
	searchParams: URLSearchParams,
	redactedKeys: string[] = [],
): Record<string, string | string[]> | null {
	const payload: Record<string, string | string[]> = {};
	const redacted = new Set(redactedKeys);

	for (const [key, value] of searchParams.entries()) {
		pushQueryParamValue(payload, key, redacted.has(key) ? "[redacted]" : value);
	}

	return Object.keys(payload).length > 0 ? payload : null;
}

export function parseJsonResponsePayload(text: string): unknown {
	if (!text) {
		return null;
	}

	try {
		return JSON.parse(text) as unknown;
	} catch {
		return text;
	}
}

export async function recordAdSyncApiTransaction(
	input: AdSyncApiTransactionInput,
): Promise<void> {
	await query(
		`
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
    `,
		[
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
		],
	);
}
