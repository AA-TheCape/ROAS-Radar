import {
	ATTRIBUTION_CLICK_ID_FIELDS,
	ATTRIBUTION_SCHEMA_VERSION,
	ATTRIBUTION_UTM_FIELDS,
	normalizeAttributionClickId,
	normalizeAttributionUrl,
	normalizeAttributionUtm,
} from "../../../packages/attribution-schema/index.js";
import { env } from "../../config/env.js";
import { query, withTransaction } from "../../db/pool.js";
import { recordDeadLetter } from "../dead-letters/index.js";

type Queryable = {
	query: typeof query;
};

type ShopifyOrderWritebackJob = {
	id: number;
	queue_key: string;
	shopify_order_id: string;
	requested_reason: string;
	status: "pending" | "processing" | "retry" | "completed" | "failed";
	attempts: number;
};

type CanonicalAttributeSourceRow = {
	shopify_order_id: string;
	resolved_session_id: string | null;
	raw_payload: unknown;
	landing_url: string | null;
	identity_referrer_url: string | null;
	initial_utm_source: string | null;
	initial_utm_medium: string | null;
	initial_utm_campaign: string | null;
	initial_utm_content: string | null;
	initial_utm_term: string | null;
	initial_gclid: string | null;
	initial_gbraid: string | null;
	initial_wbraid: string | null;
	initial_fbclid: string | null;
	initial_ttclid: string | null;
	initial_msclkid: string | null;
	page_url: string | null;
	event_referrer_url: string | null;
	utm_source: string | null;
	utm_medium: string | null;
	utm_campaign: string | null;
	utm_content: string | null;
	utm_term: string | null;
	gclid: string | null;
	gbraid: string | null;
	wbraid: string | null;
	fbclid: string | null;
	ttclid: string | null;
	msclkid: string | null;
};

type CanonicalAttributeRow = {
	key: string;
	value: string;
};

type ShopifyWritebackPayload = {
	workerId: string;
	shopifyOrderId: string;
	requestedReason: string;
	attributes: CanonicalAttributeRow[];
};

type ShopifyWritebackProcessor = (
	payload: ShopifyWritebackPayload,
) => Promise<void>;

type ProcessShopifyOrderWritebackOptions = {
	workerId: string;
	limit?: number;
	now?: Date;
};

type ReconcileRecentShopifyOrderAttributesOptions = {
	workerId: string;
	limit?: number;
	lookbackDays?: number;
	now?: Date;
};

type ReconciliationOrderRow = {
	shopify_order_id: string;
	landing_session_id: string | null;
	raw_payload: unknown;
};

class ShopifyWritebackError extends Error {
	retryable: boolean;
	statusCode?: number;

	constructor(
		message: string,
		options: { retryable?: boolean; statusCode?: number } = {},
	) {
		super(message);
		this.name = "ShopifyWritebackError";
		this.retryable = options.retryable ?? true;
		this.statusCode = options.statusCode;
	}
}

function buildQueueKey(shopifyOrderId: string): string {
	return `shopify_order:${shopifyOrderId}`;
}

function coalesce(...values: Array<string | null | undefined>): string | null {
	for (const value of values) {
		if (value != null) {
			return value;
		}
	}

	return null;
}

function normalizeOptionalUrl(value: string | null | undefined): string | null {
	try {
		return normalizeAttributionUrl(value);
	} catch {
		return null;
	}
}

function normalizeOptionalUtm(value: string | null | undefined): string | null {
	return normalizeAttributionUtm(value);
}

function normalizeOptionalClickId(
	value: string | null | undefined,
): string | null {
	return normalizeAttributionClickId(value);
}

function stringifyAttributeValue(value: unknown): string | null {
	if (value == null) {
		return null;
	}

	if (typeof value === "string") {
		const normalized = value.trim();
		return normalized ? normalized : null;
	}

	if (typeof value === "number" || typeof value === "boolean") {
		return String(value);
	}

	return null;
}

function extractRawPayloadAttributeMap(
	rawPayload: unknown,
): Map<string, string> {
	const attributes = new Map<string, string>();

	if (!rawPayload || typeof rawPayload !== "object") {
		return attributes;
	}

	const record = rawPayload as Record<string, unknown>;

	for (const key of ["note_attributes", "attributes"]) {
		const value = record[key];

		if (Array.isArray(value)) {
			for (const entry of value) {
				if (!entry || typeof entry !== "object") {
					continue;
				}

				const normalizedEntry = entry as Record<string, unknown>;
				const attributeKey = stringifyAttributeValue(
					normalizedEntry.name ?? normalizedEntry.key,
				);
				const attributeValue = stringifyAttributeValue(normalizedEntry.value);

				if (attributeKey && attributeValue) {
					attributes.set(attributeKey, attributeValue);
				}
			}
			continue;
		}

		if (!value || typeof value !== "object") {
			continue;
		}

		for (const [attributeKey, attributeValue] of Object.entries(
			value as Record<string, unknown>,
		)) {
			const normalizedValue = stringifyAttributeValue(attributeValue);

			if (normalizedValue) {
				attributes.set(attributeKey, normalizedValue);
			}
		}
	}

	return attributes;
}

function buildCanonicalAttributeRows(
	row: CanonicalAttributeSourceRow,
): CanonicalAttributeRow[] {
	if (!row.resolved_session_id) {
		throw new ShopifyWritebackError(
			"shopify order is missing a resolved roas_radar_session_id",
			{ retryable: false },
		);
	}

	if (row.landing_url == null && row.page_url == null) {
		throw new ShopifyWritebackError(
			"session attribution capture was not found for landing session",
			{ retryable: false },
		);
	}

	const attributes: CanonicalAttributeRow[] = [
		{ key: "schema_version", value: String(ATTRIBUTION_SCHEMA_VERSION) },
		{ key: "roas_radar_session_id", value: row.resolved_session_id },
	];

	const urlPairs: Array<[string, string | null]> = [
		["landing_url", normalizeOptionalUrl(row.landing_url)],
		[
			"referrer_url",
			normalizeOptionalUrl(
				coalesce(row.event_referrer_url, row.identity_referrer_url),
			),
		],
		["page_url", normalizeOptionalUrl(row.page_url)],
	];

	for (const [key, value] of urlPairs) {
		if (value) {
			attributes.push({ key, value });
		}
	}

	for (const field of ATTRIBUTION_UTM_FIELDS) {
		const value = normalizeOptionalUtm(
			coalesce(
				row[field],
				row[`initial_${field}` as keyof CanonicalAttributeSourceRow] as
					| string
					| null,
			),
		);

		if (value) {
			attributes.push({ key: field, value });
		}
	}

	for (const field of ATTRIBUTION_CLICK_ID_FIELDS) {
		const value = normalizeOptionalClickId(
			coalesce(
				row[field],
				row[`initial_${field}` as keyof CanonicalAttributeSourceRow] as
					| string
					| null,
			),
		);

		if (value) {
			attributes.push({ key: field, value });
		}
	}

	return attributes;
}

async function fetchCanonicalAttributeSource(
	db: Queryable,
	shopifyOrderId: string,
): Promise<CanonicalAttributeSourceRow | null> {
	const result = await db.query<CanonicalAttributeSourceRow>(
		`
      SELECT
        o.shopify_order_id,
        COALESCE(o.landing_session_id, attribution.session_id)::text AS resolved_session_id,
        o.raw_payload,
        identity_capture.landing_url,
        identity_capture.referrer_url AS identity_referrer_url,
        identity_capture.initial_utm_source,
        identity_capture.initial_utm_medium,
        identity_capture.initial_utm_campaign,
        identity_capture.initial_utm_content,
        identity_capture.initial_utm_term,
        identity_capture.initial_gclid,
        identity_capture.initial_gbraid,
        identity_capture.initial_wbraid,
        identity_capture.initial_fbclid,
        identity_capture.initial_ttclid,
        identity_capture.initial_msclkid,
        last_touch.page_url,
        last_touch.referrer_url AS event_referrer_url,
        last_touch.utm_source,
        last_touch.utm_medium,
        last_touch.utm_campaign,
        last_touch.utm_content,
        last_touch.utm_term,
        last_touch.gclid,
        last_touch.gbraid,
        last_touch.wbraid,
        last_touch.fbclid,
        last_touch.ttclid,
        last_touch.msclkid
      FROM shopify_orders o
      LEFT JOIN attribution_results attribution
        ON attribution.shopify_order_id = o.shopify_order_id
      LEFT JOIN session_attribution_identities identity_capture
        ON identity_capture.roas_radar_session_id = COALESCE(o.landing_session_id, attribution.session_id)
      LEFT JOIN LATERAL (
        SELECT
          page_url,
          referrer_url,
          utm_source,
          utm_medium,
          utm_campaign,
          utm_content,
          utm_term,
          gclid,
          gbraid,
          wbraid,
          fbclid,
          ttclid,
          msclkid
        FROM session_attribution_touch_events
        WHERE roas_radar_session_id = COALESCE(o.landing_session_id, attribution.session_id)
        ORDER BY occurred_at DESC, captured_at DESC, id DESC
        LIMIT 1
      ) AS last_touch ON TRUE
      WHERE o.shopify_order_id = $1
    `,
		[shopifyOrderId],
	);

	return result.rows[0] ?? null;
}

async function fetchCanonicalAttributeRows(
	db: Queryable,
	shopifyOrderId: string,
): Promise<CanonicalAttributeRow[] | null> {
	const row = await fetchCanonicalAttributeSource(db, shopifyOrderId);

	if (!row) {
		return null;
	}

	return buildCanonicalAttributeRows(row);
}

export async function previewShopifyOrderWritebackAttributes(
	shopifyOrderId: string,
): Promise<CanonicalAttributeRow[] | null> {
	return fetchCanonicalAttributeRows({ query }, shopifyOrderId);
}

async function fetchShopifyWritebackCredentials(): Promise<{
	shopDomain: string;
	accessToken: string;
}> {
	if (!env.SHOPIFY_APP_ENCRYPTION_KEY) {
		throw new ShopifyWritebackError("shopify writeback is not configured", {
			retryable: false,
		});
	}

	const result = await query<{ shop_domain: string; access_token: string }>(
		`
      SELECT
        shop_domain,
        pgp_sym_decrypt(access_token_encrypted, $1) AS access_token
      FROM shopify_app_installations
      WHERE status = 'active'
      ORDER BY updated_at DESC
      LIMIT 1
    `,
		[env.SHOPIFY_APP_ENCRYPTION_KEY],
	);

	if (!result.rowCount) {
		throw new ShopifyWritebackError(
			"no active Shopify installation is available for writeback",
			{ retryable: false },
		);
	}

	return {
		shopDomain: result.rows[0].shop_domain,
		accessToken: result.rows[0].access_token,
	};
}

async function defaultShopifyWritebackProcessor(
	payload: ShopifyWritebackPayload,
): Promise<void> {
	const credentials = await fetchShopifyWritebackCredentials();

	const response = await fetch(
		`https://${credentials.shopDomain}/admin/api/${env.SHOPIFY_APP_API_VERSION}/orders/${encodeURIComponent(payload.shopifyOrderId)}.json`,
		{
			method: "PUT",
			headers: {
				"content-type": "application/json",
				accept: "application/json",
				"x-shopify-access-token": credentials.accessToken,
			},
			body: JSON.stringify({
				order: {
					id: payload.shopifyOrderId,
					note_attributes: payload.attributes.map((attribute) => ({
						name: attribute.key,
						value: attribute.value,
					})),
				},
			}),
		},
	);

	if (response.ok) {
		return;
	}

	let details: unknown = null;

	try {
		details = await response.json();
	} catch {
		details = await response.text().catch(() => null);
	}

	throw new ShopifyWritebackError(
		`Shopify order writeback failed with status ${response.status}`,
		{
			retryable: response.status === 429 || response.status >= 500,
			statusCode: response.status,
		},
	);
}

let shopifyWritebackProcessor: ShopifyWritebackProcessor =
	defaultShopifyWritebackProcessor;
let enqueueHook:
	| ((input: {
			shopifyOrderId: string;
			requestedReason: string;
	  }) => Promise<void>)
	| null = null;
const appliedWritebacks: ShopifyWritebackPayload[] = [];

function isRetryableError(error: unknown): boolean {
	if (error instanceof ShopifyWritebackError) {
		return error.retryable;
	}

	if (typeof error === "object" && error !== null && "retryable" in error) {
		return Boolean((error as { retryable?: unknown }).retryable);
	}

	const statusCode =
		typeof error === "object" && error !== null
			? Number(
					(error as { statusCode?: unknown; status?: unknown }).statusCode ??
						(error as { status?: unknown }).status,
				)
			: Number.NaN;

	if (Number.isFinite(statusCode)) {
		return statusCode === 429 || statusCode >= 500;
	}

	return true;
}

function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function calculateRetryDelayMs(attemptNumber: number): number {
	return Math.min(60_000, 1_000 * 2 ** Math.max(0, attemptNumber - 1));
}

async function claimShopifyOrderWritebackJobs(
	workerId: string,
	now: Date,
	limit: number,
): Promise<ShopifyOrderWritebackJob[]> {
	return withTransaction(async (client) => {
		const result = await client.query<ShopifyOrderWritebackJob>(
			`
        WITH candidates AS (
          SELECT id
          FROM shopify_order_writeback_jobs
          WHERE status IN ('pending', 'retry')
            AND available_at <= $2
          ORDER BY available_at ASC, id ASC
          LIMIT $3
          FOR UPDATE SKIP LOCKED
        )
        UPDATE shopify_order_writeback_jobs jobs
        SET
          status = 'processing',
          locked_at = $2,
          locked_by = $1,
          updated_at = $2
        FROM candidates
        WHERE jobs.id = candidates.id
        RETURNING jobs.id, jobs.queue_key, jobs.shopify_order_id, jobs.requested_reason, jobs.status, jobs.attempts
      `,
			[workerId, now, limit],
		);

		return result.rows;
	});
}

export async function enqueueShopifyOrderWriteback(
	shopifyOrderId: string,
	requestedReason: string,
	client: Queryable = { query },
): Promise<{ jobId: number; queueKey: string; status: string }> {
	if (enqueueHook) {
		await enqueueHook({ shopifyOrderId, requestedReason });
	}

	const queueKey = buildQueueKey(shopifyOrderId);
	const result = await client.query<{
		id: number;
		queue_key: string;
		status: string;
	}>(
		`
      INSERT INTO shopify_order_writeback_jobs (
        queue_key,
        shopify_order_id,
        requested_reason,
        status,
        attempts,
        available_at,
        locked_at,
        locked_by,
        last_error,
        completed_at,
        dead_lettered_at,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, 'pending', 0, now(), NULL, NULL, NULL, NULL, NULL, now(), now())
      ON CONFLICT (queue_key)
      DO UPDATE SET
        requested_reason = EXCLUDED.requested_reason,
        status = CASE
          WHEN shopify_order_writeback_jobs.status IN ('completed', 'failed') THEN 'pending'
          ELSE shopify_order_writeback_jobs.status
        END,
        attempts = CASE
          WHEN shopify_order_writeback_jobs.status IN ('completed', 'failed') THEN 0
          ELSE shopify_order_writeback_jobs.attempts
        END,
        available_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        completed_at = NULL,
        dead_lettered_at = NULL,
        updated_at = now()
      RETURNING id, queue_key, status
    `,
		[queueKey, shopifyOrderId, requestedReason],
	);

	return {
		jobId: result.rows[0].id,
		queueKey: result.rows[0].queue_key,
		status: result.rows[0].status,
	};
}

async function markJobCompleted(jobId: number): Promise<void> {
	await query(
		`
      UPDATE shopify_order_writeback_jobs
      SET
        status = 'completed',
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        updated_at = now()
      WHERE id = $1
    `,
		[jobId],
	);
}

async function markJobSkipped(jobId: number, message: string): Promise<void> {
	await query(
		`
      UPDATE shopify_order_writeback_jobs
      SET
        status = 'completed',
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = $2,
        updated_at = now()
      WHERE id = $1
    `,
		[jobId, message],
	);
}

async function markJobForRetry(
	job: ShopifyOrderWritebackJob,
	workerId: string,
	error: unknown,
): Promise<"retry" | "dead_lettered"> {
	const nextAttempts = job.attempts + 1;
	const shouldDeadLetter =
		!isRetryableError(error) ||
		nextAttempts >= env.SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES;

	if (shouldDeadLetter) {
		await withTransaction(async (client) => {
			await recordDeadLetter(client, {
				eventType: "shopify_writeback_failed",
				sourceTable: "shopify_order_writeback_jobs",
				sourceRecordId: String(job.id),
				sourceQueueKey: job.queue_key,
				payload: {
					jobId: String(job.id),
					shopifyOrderId: job.shopify_order_id,
					requestedReason: job.requested_reason,
					attempts: nextAttempts,
					workerId,
				},
				error,
			});

			await client.query(
				`
          UPDATE shopify_order_writeback_jobs
          SET
            status = 'failed',
            attempts = $2,
            dead_lettered_at = now(),
            locked_at = NULL,
            locked_by = NULL,
            last_error = $3,
            updated_at = now()
          WHERE id = $1
        `,
				[job.id, nextAttempts, errorMessage(error)],
			);
		});

		return "dead_lettered";
	}

	await query(
		`
      UPDATE shopify_order_writeback_jobs
      SET
        status = 'retry',
        attempts = $2,
        available_at = now() + ($3 || ' milliseconds')::interval,
        locked_at = NULL,
        locked_by = NULL,
        last_error = $4,
        updated_at = now()
      WHERE id = $1
    `,
		[
			job.id,
			nextAttempts,
			String(calculateRetryDelayMs(nextAttempts)),
			errorMessage(error),
		],
	);

	return "retry";
}

export async function applyShopifyOrderWriteback(input: {
	workerId: string;
	shopifyOrderId: string;
	requestedReason: string;
}): Promise<{ status: "completed" | "skipped"; attributesCount: number }> {
	const attributes = await fetchCanonicalAttributeRows(
		{ query },
		input.shopifyOrderId,
	);

	if (!attributes || attributes.length === 0) {
		return {
			status: "skipped",
			attributesCount: 0,
		};
	}

	const payload = {
		workerId: input.workerId,
		shopifyOrderId: input.shopifyOrderId,
		requestedReason: input.requestedReason,
		attributes,
	};

	appliedWritebacks.push(payload);
	await shopifyWritebackProcessor(payload);

	return {
		status: "completed",
		attributesCount: attributes.length,
	};
}

export async function processShopifyOrderWritebackQueue(
	options: ProcessShopifyOrderWritebackOptions,
): Promise<{
	claimedJobs: number;
	completedJobs: number;
	retriedJobs: number;
	deadLetteredJobs: number;
	skippedJobs: number;
}> {
	const workerId = options.workerId;
	const now = options.now ?? new Date();
	const limit = Math.max(
		1,
		options.limit ?? env.SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE,
	);
	const jobs = await claimShopifyOrderWritebackJobs(workerId, now, limit);

	let completedJobs = 0;
	let retriedJobs = 0;
	let deadLetteredJobs = 0;
	let skippedJobs = 0;

	for (const job of jobs) {
		try {
			const outcome = await applyShopifyOrderWriteback({
				workerId,
				shopifyOrderId: job.shopify_order_id,
				requestedReason: job.requested_reason,
			});

			if (outcome.status === "skipped") {
				await markJobSkipped(job.id, "canonical_attributes_not_available");
				skippedJobs += 1;
				continue;
			}
			await markJobCompleted(job.id);
			completedJobs += 1;
		} catch (error) {
			const outcome = await markJobForRetry(job, workerId, error);

			if (outcome === "retry") {
				retriedJobs += 1;
			} else {
				deadLetteredJobs += 1;
			}
		}
	}

	return {
		claimedJobs: jobs.length,
		completedJobs,
		retriedJobs,
		deadLetteredJobs,
		skippedJobs,
	};
}

export async function reconcileRecentShopifyOrderAttributes(
	options: ReconcileRecentShopifyOrderAttributesOptions,
): Promise<{
	scannedOrders: number;
	ordersNeedingWriteback: number;
	requeuedOrders: number;
	upToDateOrders: number;
	skippedOrders: number;
	failedOrders: number;
}> {
	const limit = Math.max(
		1,
		options.limit ?? env.SHOPIFY_RECONCILIATION_BATCH_SIZE,
	);
	const lookbackDays = Math.max(
		1,
		options.lookbackDays ?? env.SHOPIFY_RECONCILIATION_LOOKBACK_DAYS,
	);
	const now = options.now ?? new Date();

	const recentOrders = await query<ReconciliationOrderRow>(
		`
      SELECT
        shopify_order_id,
        landing_session_id::text AS landing_session_id,
        raw_payload
      FROM shopify_orders
      WHERE COALESCE(processed_at, created_at_shopify, ingested_at) >= $1::timestamptz - ($2 || ' days')::interval
      ORDER BY COALESCE(processed_at, created_at_shopify, ingested_at) DESC, id DESC
      LIMIT $3
    `,
		[now, String(lookbackDays), limit],
	);

	let ordersNeedingWriteback = 0;
	let requeuedOrders = 0;
	let upToDateOrders = 0;
	let skippedOrders = 0;
	let failedOrders = 0;

	for (const order of recentOrders.rows) {
		if (!order.landing_session_id) {
			skippedOrders += 1;
			continue;
		}

		try {
			const expectedAttributes = await fetchCanonicalAttributeRows(
				{ query },
				order.shopify_order_id,
			);

			if (!expectedAttributes || expectedAttributes.length === 0) {
				failedOrders += 1;
				continue;
			}

			const currentAttributeMap = extractRawPayloadAttributeMap(
				order.raw_payload,
			);
			const isUpToDate = expectedAttributes.every(
				(attribute) =>
					currentAttributeMap.get(attribute.key) === attribute.value,
			);

			if (isUpToDate) {
				upToDateOrders += 1;
				continue;
			}

			ordersNeedingWriteback += 1;
			await enqueueShopifyOrderWriteback(
				order.shopify_order_id,
				"reconciliation_missing_canonical_attributes",
			);
			requeuedOrders += 1;
		} catch {
			failedOrders += 1;
		}
	}

	return {
		scannedOrders: recentOrders.rows.length,
		ordersNeedingWriteback,
		requeuedOrders,
		upToDateOrders,
		skippedOrders,
		failedOrders,
	};
}

export const __shopifyWritebackTestUtils = {
	getAppliedWritebacks(): ShopifyWritebackPayload[] {
		return [...appliedWritebacks];
	},
	reset(): void {
		appliedWritebacks.length = 0;
		shopifyWritebackProcessor = defaultShopifyWritebackProcessor;
		enqueueHook = null;
	},
	setWritebackProcessor(processor: ShopifyWritebackProcessor | null): void {
		shopifyWritebackProcessor = processor ?? defaultShopifyWritebackProcessor;
	},
	setEnqueueHook(
		hook:
			| ((input: {
					shopifyOrderId: string;
					requestedReason: string;
			  }) => Promise<void>)
			| null,
	): void {
		enqueueHook = hook;
	},
};
