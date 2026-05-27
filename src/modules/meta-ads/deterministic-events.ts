import { createHash, randomBytes } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

import type { PoolClient, QueryResult, QueryResultRow } from "pg";

import { env } from "../../config/env.js";
import { query, withTransaction } from "../../db/pool.js";
import { logError, logInfo } from "../../observability/index.js";
import {
	buildSearchParamsAuditPayload,
	parseJsonResponsePayload,
	recordAdSyncApiTransaction,
} from "../ad-sync-audit/index.js";

const META_GRAPH_API_BASE_URL = "https://graph.facebook.com";
const META_DETERMINISTIC_REQUEST_FIELDS = [
	"account_id",
	"account_name",
	"campaign_id",
	"campaign_name",
	"adset_id",
	"adset_name",
	"ad_id",
	"ad_name",
	"date_start",
	"date_stop",
	"impressions",
	"video_play_actions",
	"video_thruplay_watched_actions",
] as const;
const META_DETERMINISTIC_RETRYABLE_STATUS_CODES = new Set([
	429, 500, 502, 503, 504,
]);

type MetaDeterministicInsightsRow = {
	account_id?: string | null;
	account_name?: string | null;
	campaign_id?: string | null;
	campaign_name?: string | null;
	adset_id?: string | null;
	adset_name?: string | null;
	ad_id?: string | null;
	ad_name?: string | null;
	date_start?: string | null;
	date_stop?: string | null;
	impressions?: string | number | null;
	video_play_actions?: MetaActionMetricEntry[] | null;
	video_thruplay_watched_actions?: MetaActionMetricEntry[] | null;
	[key: string]: unknown;
};

type MetaActionMetricEntry = {
	action_type?: string | null;
	value?: string | number | null;
};

type MetaInsightsApiResponse = {
	data?: MetaDeterministicInsightsRow[];
	paging?: {
		cursors?: {
			after?: string;
		};
		next?: string;
	};
	[key: string]: unknown;
};

type MetaDeterministicSyncJobRow = {
	id: number;
	connection_id: number;
	ad_account_id: string;
	sync_date: string;
	cursor: string | null;
	attempts: number;
};

type MetaDeterministicConnectionSecretRow = {
	id: number;
	ad_account_id: string;
	access_token: string;
};

type MetaDeterministicEventType = "impression" | "view";

type NormalizedDeterministicEventRow = {
	eventType: MetaDeterministicEventType;
	eventDate: string;
	eventCount: number;
	accountId: string;
	campaignId: string | null;
	campaignName: string | null;
	adsetId: string | null;
	adsetName: string | null;
	adId: string | null;
	adName: string | null;
	rawPayload: MetaDeterministicInsightsRow;
	dedupeKey: string;
};

type MetaAdsQueryable = {
	query<TResult extends QueryResultRow = QueryResultRow>(
		text: string,
		params?: unknown[],
	): Promise<QueryResult<TResult>>;
};

type MetaDeterministicApiMetrics = {
	requestCount: number;
	errorCount: number;
	retryCount: number;
	latencyMsTotal: number;
	latencyMsMax: number;
};

export type MetaDeterministicSyncResult = {
	succeededJobs: number;
	failedJobs: number;
	recordsReceived: number;
	rawRowsFetched: number;
	rawRowsUpserted: number;
	factRowsUpserted: number;
	apiRequestCount: number;
};

export type MetaDeterministicQueueProcessOptions = {
	workerId?: string;
	limit?: number;
	now?: Date;
	triggerSource?: string;
	planJobs?: boolean;
	emitMetrics?: boolean;
};

export type MetaDeterministicQueueProcessResult = {
	workerId: string;
	enqueuedJobs: number;
	claimedJobs: number;
	succeededJobs: number;
	failedJobs: number;
	durationMs: number;
};

class MetaDeterministicApiError extends Error {
	statusCode: number;
	details: unknown;

	constructor(statusCode: number, message: string, details: unknown) {
		super(message);
		this.name = "MetaDeterministicApiError";
		this.statusCode = statusCode;
		this.details = details;
	}
}

function formatDateOnly(date: Date): string {
	return date.toISOString().slice(0, 10);
}

function addUtcDays(date: Date, days: number): Date {
	const copy = new Date(
		Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
	);
	copy.setUTCDate(copy.getUTCDate() + days);
	return copy;
}

function parseDateOnly(value: string): Date {
	const [year, month, day] = value
		.split("-")
		.map((part) => Number.parseInt(part, 10));
	return new Date(Date.UTC(year, month - 1, day));
}

function normalizeAdAccountId(value: string): string {
	return value.startsWith("act_") ? value.slice(4) : value;
}

function parseNonNegativeInteger(
	value: string | number | null | undefined,
): number {
	if (typeof value === "number") {
		return Number.isFinite(value) && value > 0 ? Math.trunc(value) : 0;
	}

	if (typeof value !== "string" || value.trim() === "") {
		return 0;
	}

	const parsed = Number.parseInt(value, 10);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function sumActionValues(
	actions: MetaActionMetricEntry[] | null | undefined,
): number {
	if (!Array.isArray(actions)) {
		return 0;
	}

	return actions.reduce(
		(total, action) => total + parseNonNegativeInteger(action.value),
		0,
	);
}

function buildDedupeKey(
	row: Omit<NormalizedDeterministicEventRow, "dedupeKey" | "rawPayload">,
): string {
	return createHash("sha256")
		.update(
			[
				"meta_ads",
				row.accountId,
				row.eventType,
				row.eventDate,
				row.campaignId ?? "",
				row.adsetId ?? "",
				row.adId ?? "",
			].join("|"),
		)
		.digest("hex");
}

function normalizeInsightsRows(
	rows: MetaDeterministicInsightsRow[],
	fallbackAccountId: string,
): NormalizedDeterministicEventRow[] {
	const normalized: NormalizedDeterministicEventRow[] = [];

	for (const row of rows) {
		const eventDate =
			typeof row.date_start === "string" ? row.date_start : null;
		const accountId =
			typeof row.account_id === "string" && row.account_id.trim()
				? normalizeAdAccountId(row.account_id)
				: fallbackAccountId;
		const campaignId =
			typeof row.campaign_id === "string" && row.campaign_id.trim()
				? row.campaign_id
				: null;
		const adId =
			typeof row.ad_id === "string" && row.ad_id.trim() ? row.ad_id : null;

		if (!eventDate || (!campaignId && !adId)) {
			continue;
		}

		const base = {
			eventDate,
			accountId,
			campaignId,
			campaignName:
				typeof row.campaign_name === "string" ? row.campaign_name : null,
			adsetId: typeof row.adset_id === "string" ? row.adset_id : null,
			adsetName: typeof row.adset_name === "string" ? row.adset_name : null,
			adId,
			adName: typeof row.ad_name === "string" ? row.ad_name : null,
		};
		const impressionCount = parseNonNegativeInteger(row.impressions);
		const viewCount = Math.max(
			sumActionValues(row.video_play_actions),
			sumActionValues(row.video_thruplay_watched_actions),
		);

		for (const event of [
			{ eventType: "impression" as const, eventCount: impressionCount },
			{ eventType: "view" as const, eventCount: viewCount },
		]) {
			if (event.eventCount <= 0) {
				continue;
			}

			const eventRow = {
				...base,
				eventType: event.eventType,
				eventCount: event.eventCount,
			};

			normalized.push({
				...eventRow,
				rawPayload: row,
				dedupeKey: buildDedupeKey(eventRow),
			});
		}
	}

	return normalized;
}

function buildMetaInsightsUrl(
	adAccountId: string,
	syncDate: string,
	cursor: string | null,
): URL {
	const accountId = adAccountId.startsWith("act_")
		? adAccountId
		: `act_${adAccountId}`;
	const url = new URL(
		`${META_GRAPH_API_BASE_URL}/${env.META_ADS_API_VERSION}/${accountId}/insights`,
	);
	url.searchParams.set("level", "ad");
	url.searchParams.set("time_increment", "1");
	url.searchParams.set("fields", META_DETERMINISTIC_REQUEST_FIELDS.join(","));
	url.searchParams.set(
		"time_range",
		JSON.stringify({ since: syncDate, until: syncDate }),
	);
	url.searchParams.set("limit", "500");

	if (cursor) {
		url.searchParams.set("after", cursor);
	}

	return url;
}

function buildMetrics(): MetaDeterministicApiMetrics {
	return {
		requestCount: 0,
		errorCount: 0,
		retryCount: 0,
		latencyMsTotal: 0,
		latencyMsMax: 0,
	};
}

async function enqueueSyncJobsForWindow(now: Date): Promise<number> {
	const today = new Date(
		Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()),
	);
	const connections = await query<{
		id: number;
		last_completed_date: string | null;
	}>(
		`
      SELECT
        c.id,
        checkpoint.last_completed_date::text
      FROM meta_ads_connections c
      LEFT JOIN meta_ads_deterministic_sync_checkpoints checkpoint
        ON checkpoint.connection_id = c.id
      WHERE c.status = 'active'
        AND c.deterministic_view_impression_sync_enabled = true
      ORDER BY c.id ASC
    `,
	);
	let enqueuedJobs = 0;

	for (const connection of connections.rows) {
		const lookbackDays = connection.last_completed_date
			? env.META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS
			: env.META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS;
		const checkpointStart = connection.last_completed_date
			? addUtcDays(parseDateOnly(connection.last_completed_date), 1)
			: null;
		const lookbackStart = addUtcDays(today, -Math.max(1, lookbackDays));
		const start =
			checkpointStart && checkpointStart > lookbackStart
				? checkpointStart
				: lookbackStart;
		const end = addUtcDays(today, -1);

		for (
			let cursorDate = start;
			cursorDate <= end;
			cursorDate = addUtcDays(cursorDate, 1)
		) {
			const syncDate = formatDateOnly(cursorDate);
			await query(
				`
          INSERT INTO meta_ads_deterministic_sync_jobs (
            connection_id,
            sync_date,
            status,
            attempts,
            available_at,
            locked_at,
            locked_by,
            last_error,
            completed_at,
            updated_at
          )
          VALUES ($1, $2::date, 'pending', 0, now(), NULL, NULL, NULL, NULL, now())
          ON CONFLICT (connection_id, sync_date)
          DO UPDATE SET
            status = CASE
              WHEN meta_ads_deterministic_sync_jobs.status = 'processing' THEN meta_ads_deterministic_sync_jobs.status
              ELSE 'pending'
            END,
            available_at = CASE
              WHEN meta_ads_deterministic_sync_jobs.status = 'processing' THEN meta_ads_deterministic_sync_jobs.available_at
              ELSE now()
            END,
            locked_at = CASE
              WHEN meta_ads_deterministic_sync_jobs.status = 'processing' THEN meta_ads_deterministic_sync_jobs.locked_at
              ELSE NULL
            END,
            locked_by = CASE
              WHEN meta_ads_deterministic_sync_jobs.status = 'processing' THEN meta_ads_deterministic_sync_jobs.locked_by
              ELSE NULL
            END,
            last_error = CASE
              WHEN meta_ads_deterministic_sync_jobs.status = 'processing' THEN meta_ads_deterministic_sync_jobs.last_error
              ELSE NULL
            END,
            completed_at = CASE
              WHEN meta_ads_deterministic_sync_jobs.status = 'processing' THEN meta_ads_deterministic_sync_jobs.completed_at
              ELSE NULL
            END,
            updated_at = now()
        `,
				[connection.id, syncDate],
			);
			enqueuedJobs += 1;
		}

		await query(
			`
        UPDATE meta_ads_connections
        SET deterministic_view_impression_last_planned_for = $2::date,
            updated_at = now()
        WHERE id = $1
      `,
			[connection.id, formatDateOnly(end)],
		);
	}

	return enqueuedJobs;
}

async function claimSyncJobs(
	workerId: string,
	limit: number,
): Promise<MetaDeterministicSyncJobRow[]> {
	const result = await query<MetaDeterministicSyncJobRow>(
		`
      WITH claimable AS (
        SELECT j.id, j.connection_id
        FROM meta_ads_deterministic_sync_jobs j
        JOIN meta_ads_connections c ON c.id = j.connection_id
        WHERE j.status IN ('pending', 'retry')
          AND j.available_at <= now()
          AND c.status = 'active'
          AND c.deterministic_view_impression_sync_enabled = true
        ORDER BY j.sync_date ASC, j.id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE meta_ads_deterministic_sync_jobs j
      SET
        status = 'processing',
        locked_at = now(),
        locked_by = $1,
        attempts = j.attempts + 1,
        updated_at = now()
      FROM claimable
      JOIN meta_ads_connections c ON c.id = claimable.connection_id
      WHERE j.id = claimable.id
      RETURNING
        j.id,
        j.connection_id,
        c.ad_account_id,
        j.sync_date::text,
        j.cursor,
        j.attempts
    `,
		[workerId, limit],
	);

	return result.rows;
}

async function getConnectionSecret(
	connectionId: number,
): Promise<MetaDeterministicConnectionSecretRow> {
	const result = await query<MetaDeterministicConnectionSecretRow>(
		`
      SELECT
        id,
        ad_account_id,
        pgp_sym_decrypt(access_token_encrypted, $2)::text AS access_token
      FROM meta_ads_connections
      WHERE id = $1
        AND deterministic_view_impression_sync_enabled = true
    `,
		[connectionId, env.META_ADS_ENCRYPTION_KEY],
	);

	const row = result.rows[0];

	if (!row?.access_token) {
		throw new Error(
			`Meta Ads connection ${connectionId} is missing a decryptable access token`,
		);
	}

	return row;
}

async function performMetaApiRequest(params: {
	job: MetaDeterministicSyncJobRow;
	connection: MetaDeterministicConnectionSecretRow;
	url: URL;
	apiMetrics: MetaDeterministicApiMetrics;
	triggerSource: string;
}): Promise<MetaInsightsApiResponse> {
	const requestStartedAt = new Date();
	const response = await fetch(params.url, {
		method: "GET",
		headers: {
			authorization: `Bearer ${params.connection.access_token}`,
		},
	});
	const responseReceivedAt = new Date();
	const bodyText = await response.text();
	const payload = parseJsonResponsePayload(bodyText);
	const latencyMs = responseReceivedAt.getTime() - requestStartedAt.getTime();

	params.apiMetrics.requestCount += 1;
	params.apiMetrics.latencyMsTotal += latencyMs;
	params.apiMetrics.latencyMsMax = Math.max(
		params.apiMetrics.latencyMsMax,
		latencyMs,
	);

	await recordAdSyncApiTransaction({
		platform: "meta_ads",
		connectionId: params.job.connection_id,
		syncJobId: params.job.id,
		transactionSource: "meta_ads_deterministic_insights",
		sourceMetadata: {
			triggerSource: params.triggerSource,
			syncDate: params.job.sync_date,
			cursor: params.job.cursor,
		},
		requestMethod: "GET",
		requestUrl: params.url.toString(),
		requestPayload: buildSearchParamsAuditPayload(params.url.searchParams),
		requestStartedAt,
		responseStatus: response.status,
		responsePayload: payload,
		responseReceivedAt,
	});

	if (!response.ok) {
		params.apiMetrics.errorCount += 1;
		throw new MetaDeterministicApiError(
			response.status,
			`Meta deterministic Insights request failed with status ${response.status}`,
			payload,
		);
	}

	return payload as MetaInsightsApiResponse;
}

async function fetchAllDeterministicRows(params: {
	job: MetaDeterministicSyncJobRow;
	connection: MetaDeterministicConnectionSecretRow;
	apiMetrics: MetaDeterministicApiMetrics;
	triggerSource: string;
}): Promise<{
	rows: MetaDeterministicInsightsRow[];
	finalCursor: string | null;
}> {
	const rows: MetaDeterministicInsightsRow[] = [];
	let nextUrl: URL | null = buildMetaInsightsUrl(
		params.connection.ad_account_id,
		params.job.sync_date,
		params.job.cursor,
	);
	let finalCursor: string | null = params.job.cursor;

	while (nextUrl) {
		const requestUrl = nextUrl;
		let payload: MetaInsightsApiResponse;

		try {
			payload = await performMetaApiRequest({
				...params,
				url: requestUrl,
			});
		} catch (error) {
			if (
				!(error instanceof MetaDeterministicApiError) ||
				!META_DETERMINISTIC_RETRYABLE_STATUS_CODES.has(error.statusCode)
			) {
				throw error;
			}

			params.apiMetrics.retryCount += 1;
			await delay(Math.min(2000, 250 * params.apiMetrics.retryCount));
			payload = await performMetaApiRequest({
				...params,
				url: requestUrl,
			});
		}

		rows.push(...(Array.isArray(payload.data) ? payload.data : []));
		finalCursor = payload.paging?.cursors?.after ?? finalCursor;
		nextUrl =
			typeof payload.paging?.next === "string"
				? new URL(payload.paging.next)
				: null;
	}

	return { rows, finalCursor };
}

async function upsertSource(
	client: PoolClient,
	params: {
		job: MetaDeterministicSyncJobRow;
		apiRequestCount: number;
		finalCursor: string | null;
	},
): Promise<number> {
	const sourceKey = createHash("sha256")
		.update(
			[
				"meta_ads",
				params.job.ad_account_id,
				params.job.sync_date,
				"ads_insights",
			].join("|"),
		)
		.digest("hex");
	const result = await client.query<{ id: number }>(
		`
      INSERT INTO deterministic_event_sources (
        source_key,
        platform,
        account_id,
        evidence_origin,
        source_type,
        sync_job_id,
        external_request_id,
        api_version,
        requested_range_start,
        requested_range_end,
        source_metadata
      )
      VALUES (
        $1,
        'meta_ads',
        $2,
        'api',
        'ads_insights',
        $3,
        $1,
        $4,
        $5::date,
        $5::date,
        $6::jsonb
      )
      ON CONFLICT (source_key)
      DO UPDATE SET
        sync_job_id = EXCLUDED.sync_job_id,
        received_at_utc = now(),
        source_metadata = EXCLUDED.source_metadata
      RETURNING id
    `,
		[
			sourceKey,
			normalizeAdAccountId(params.job.ad_account_id),
			params.job.id,
			env.META_ADS_API_VERSION,
			params.job.sync_date,
			JSON.stringify({
				endpoint: "insights",
				level: "ad",
				apiRequestCount: params.apiRequestCount,
				finalCursor: params.finalCursor,
			}),
		],
	);

	return result.rows[0].id;
}

async function upsertDeterministicRows(
	client: PoolClient,
	params: {
		sourceId: number;
		rows: NormalizedDeterministicEventRow[];
	},
): Promise<{ rawRowsUpserted: number; factRowsUpserted: number }> {
	let rawRowsUpserted = 0;
	let factRowsUpserted = 0;

	for (const row of params.rows) {
		const rawResult = await client.query<{ id: number }>(
			`
        INSERT INTO raw_deterministic_events (
          source_id,
          platform,
          account_id,
          campaign_id,
          adset_id,
          ad_id,
          event_type,
          event_date,
          event_count,
          evidence_origin,
          platform_verified,
          dedupe_key,
          raw_payload
        )
        VALUES (
          $1,
          'meta_ads',
          $2,
          $3,
          $4,
          $5,
          $6,
          $7::date,
          $8,
          'api',
          true,
          $9,
          $10::jsonb
        )
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
          source_id = EXCLUDED.source_id,
          event_count = EXCLUDED.event_count,
          raw_payload = EXCLUDED.raw_payload,
          ingested_at_utc = now(),
          retained_until = now() + interval '400 days'
        RETURNING id
      `,
			[
				params.sourceId,
				row.accountId,
				row.campaignId,
				row.adsetId,
				row.adId,
				row.eventType,
				row.eventDate,
				row.eventCount,
				row.dedupeKey,
				JSON.stringify(row.rawPayload),
			],
		);
		rawRowsUpserted += 1;
		const rawEventId = rawResult.rows[0].id;

		const factResult = await client.query<{ id: number }>(
			`
        INSERT INTO deterministic_event_facts (
          source_id,
          raw_event_id,
          platform,
          account_id,
          campaign_id,
          campaign_name,
          adset_id,
          adset_name,
          ad_id,
          ad_name,
          event_type,
          fact_date,
          event_count,
          evidence_origin,
          platform_verified,
          normalization_status,
          normalization_notes
        )
        VALUES (
          $1,
          $2,
          'meta_ads',
          $3,
          $4,
          $5,
          $6,
          $7,
          $8,
          $9,
          $10,
          $11::date,
          $12,
          'api',
          true,
          'normalized',
          '{}'::jsonb
        )
        ON CONFLICT (
          platform,
          account_id,
          event_type,
          fact_date,
          COALESCE(campaign_id, ''),
          COALESCE(adset_id, ''),
          COALESCE(ad_id, ''),
          COALESCE(creative_id, ''),
          evidence_origin
        )
        DO UPDATE SET
          source_id = EXCLUDED.source_id,
          raw_event_id = EXCLUDED.raw_event_id,
          campaign_name = EXCLUDED.campaign_name,
          adset_name = EXCLUDED.adset_name,
          ad_name = EXCLUDED.ad_name,
          event_count = EXCLUDED.event_count,
          platform_verified = true,
          normalization_status = 'normalized',
          normalized_at_utc = now(),
          retained_until = now() + interval '400 days'
        RETURNING id
      `,
			[
				params.sourceId,
				rawEventId,
				row.accountId,
				row.campaignId,
				row.campaignName,
				row.adsetId,
				row.adsetName,
				row.adId,
				row.adName,
				row.eventType,
				row.eventDate,
				row.eventCount,
			],
		);
		factRowsUpserted += 1;

		await client.query(
			`
        INSERT INTO deterministic_event_verification_statuses (
          fact_id,
          verification_status,
          evidence_origin,
          platform_verified,
          verified_by_source_id,
          verified_at_utc,
          verification_metadata
        )
        VALUES ($1, 'verified', 'api', true, $2, now(), $3::jsonb)
        ON CONFLICT (fact_id)
        DO UPDATE SET
          verification_status = 'verified',
          evidence_origin = 'api',
          platform_verified = true,
          verified_by_source_id = EXCLUDED.verified_by_source_id,
          verified_at_utc = now(),
          failure_reason = NULL,
          verification_metadata = EXCLUDED.verification_metadata
      `,
			[
				factResult.rows[0].id,
				params.sourceId,
				JSON.stringify({
					platform: "meta_ads",
					source: "ads_insights",
				}),
			],
		);
	}

	return { rawRowsUpserted, factRowsUpserted };
}

async function markSyncJobSucceeded(
	job: MetaDeterministicSyncJobRow,
	finalCursor: string | null,
	client: MetaAdsQueryable,
): Promise<void> {
	await client.query(
		`
      UPDATE meta_ads_deterministic_sync_jobs
      SET
        status = 'completed',
        cursor = $2,
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        updated_at = now()
      WHERE id = $1
    `,
		[job.id, finalCursor],
	);

	await client.query(
		`
      INSERT INTO meta_ads_deterministic_sync_checkpoints (
        connection_id,
        last_completed_date,
        last_cursor,
        last_synced_at,
        updated_at
      )
      VALUES ($1, $2::date, $3, now(), now())
      ON CONFLICT (connection_id)
      DO UPDATE SET
        last_completed_date = GREATEST(
          COALESCE(meta_ads_deterministic_sync_checkpoints.last_completed_date, EXCLUDED.last_completed_date),
          EXCLUDED.last_completed_date
        ),
        last_cursor = EXCLUDED.last_cursor,
        last_synced_at = now(),
        updated_at = now()
    `,
		[job.connection_id, job.sync_date, finalCursor],
	);
}

async function markSyncJobFailed(
	job: MetaDeterministicSyncJobRow,
	error: unknown,
	client?: MetaAdsQueryable,
): Promise<void> {
	const executor = client ?? poolQueryExecutor;
	const message = error instanceof Error ? error.message : String(error);
	const retryDelaySeconds = Math.min(300, Math.max(15, job.attempts * 30));
	const shouldRetry =
		error instanceof MetaDeterministicApiError &&
		META_DETERMINISTIC_RETRYABLE_STATUS_CODES.has(error.statusCode) &&
		job.attempts < env.META_ADS_SYNC_MAX_RETRIES;
	const nextStatus = shouldRetry ? "retry" : "failed";

	await executor.query(
		`
      UPDATE meta_ads_deterministic_sync_jobs
      SET
        status = $2,
        available_at = CASE
          WHEN $2 = 'retry' THEN now() + ($3::int * interval '1 second')
          ELSE available_at
        END,
        locked_at = NULL,
        locked_by = NULL,
        last_error = $4,
        completed_at = CASE WHEN $2 = 'failed' THEN now() ELSE completed_at END,
        updated_at = now()
      WHERE id = $1
    `,
		[job.id, nextStatus, retryDelaySeconds, message],
	);
}

async function processSyncJob(
	job: MetaDeterministicSyncJobRow,
	triggerSource: string,
): Promise<{
	outcome: "succeeded" | "failed";
	recordsReceived: number;
	rawRowsFetched: number;
	rawRowsUpserted: number;
	factRowsUpserted: number;
	apiRequestCount: number;
}> {
	const connection = await getConnectionSecret(job.connection_id);
	const apiMetrics = buildMetrics();

	try {
		const { rows: rawRows, finalCursor } = await fetchAllDeterministicRows({
			job,
			connection,
			apiMetrics,
			triggerSource,
		});
		const normalizedRows = normalizeInsightsRows(
			rawRows,
			normalizeAdAccountId(connection.ad_account_id),
		);

		const persistence = await withTransaction(async (client) => {
			const sourceId = await upsertSource(client, {
				job,
				apiRequestCount: apiMetrics.requestCount,
				finalCursor,
			});
			const upsertResult = await upsertDeterministicRows(client, {
				sourceId,
				rows: normalizedRows,
			});
			await markSyncJobSucceeded(job, finalCursor, client);
			return upsertResult;
		});

		logInfo("meta_ads_deterministic_sync_job_completed", {
			service: process.env.K_SERVICE ?? "roas-radar",
			jobId: job.id,
			connectionId: job.connection_id,
			adAccountId: connection.ad_account_id,
			syncDate: job.sync_date,
			triggerSource,
			rawRowsFetched: rawRows.length,
			recordsReceived: normalizedRows.length,
			rawRowsUpserted: persistence.rawRowsUpserted,
			factRowsUpserted: persistence.factRowsUpserted,
			apiRequestCount: apiMetrics.requestCount,
			apiRequestRetryCount: apiMetrics.retryCount,
			apiLatencyMsMax: apiMetrics.latencyMsMax,
		});

		return {
			outcome: "succeeded",
			recordsReceived: normalizedRows.length,
			rawRowsFetched: rawRows.length,
			rawRowsUpserted: persistence.rawRowsUpserted,
			factRowsUpserted: persistence.factRowsUpserted,
			apiRequestCount: apiMetrics.requestCount,
		};
	} catch (error) {
		await markSyncJobFailed(job, error);
		logError("meta_ads_deterministic_sync_job_failed", error, {
			service: process.env.K_SERVICE ?? "roas-radar",
			jobId: job.id,
			connectionId: job.connection_id,
			adAccountId: connection.ad_account_id,
			syncDate: job.sync_date,
			triggerSource,
			attempts: job.attempts,
			apiRequestCount: apiMetrics.requestCount,
		});

		return {
			outcome: "failed",
			recordsReceived: 0,
			rawRowsFetched: 0,
			rawRowsUpserted: 0,
			factRowsUpserted: 0,
			apiRequestCount: apiMetrics.requestCount,
		};
	}
}

const poolQueryExecutor: MetaAdsQueryable = {
	query<TResult extends QueryResultRow = QueryResultRow>(
		text: string,
		params?: unknown[],
	): Promise<QueryResult<TResult>> {
		return query<TResult>(text, params);
	},
};

export async function processMetaDeterministicSyncQueue(
	options: MetaDeterministicQueueProcessOptions = {},
): Promise<MetaDeterministicQueueProcessResult> {
	const startedAt = Date.now();
	const workerId =
		options.workerId ?? `meta-deterministic-${randomBytes(6).toString("hex")}`;
	const enqueuedJobs = options.planJobs
		? await enqueueSyncJobsForWindow(options.now ?? new Date())
		: 0;
	const jobs = await claimSyncJobs(
		workerId,
		options.limit ?? env.META_ADS_SYNC_BATCH_SIZE,
	);
	let succeededJobs = 0;
	let failedJobs = 0;

	for (const job of jobs) {
		const result = await processSyncJob(job, options.triggerSource ?? "worker");

		if (result.outcome === "succeeded") {
			succeededJobs += 1;
		} else {
			failedJobs += 1;
		}
	}

	const result = {
		workerId,
		enqueuedJobs,
		claimedJobs: jobs.length,
		succeededJobs,
		failedJobs,
		durationMs: Date.now() - startedAt,
	};

	if (options.emitMetrics) {
		logInfo("meta_ads_deterministic_sync_run", {
			service: process.env.K_SERVICE ?? "roas-radar",
			...result,
		});
	}

	return result;
}

export async function runMetaDeterministicSync(
	options: {
		now?: Date;
		triggerSource?: string;
	} = {},
): Promise<MetaDeterministicSyncResult> {
	const workerId = `meta-deterministic-${randomBytes(6).toString("hex")}`;
	const triggerSource = options.triggerSource ?? "scheduler";
	let jobsPlanned = false;
	const summary: MetaDeterministicSyncResult = {
		succeededJobs: 0,
		failedJobs: 0,
		recordsReceived: 0,
		rawRowsFetched: 0,
		rawRowsUpserted: 0,
		factRowsUpserted: 0,
		apiRequestCount: 0,
	};

	while (true) {
		if (!jobsPlanned) {
			await enqueueSyncJobsForWindow(options.now ?? new Date());
			jobsPlanned = true;
		}

		const jobs = await claimSyncJobs(workerId, env.META_ADS_SYNC_BATCH_SIZE);

		if (jobs.length === 0) {
			break;
		}

		for (const job of jobs) {
			const result = await processSyncJob(job, triggerSource);

			if (result.outcome === "succeeded") {
				summary.succeededJobs += 1;
			} else {
				summary.failedJobs += 1;
			}

			summary.recordsReceived += result.recordsReceived;
			summary.rawRowsFetched += result.rawRowsFetched;
			summary.rawRowsUpserted += result.rawRowsUpserted;
			summary.factRowsUpserted += result.factRowsUpserted;
			summary.apiRequestCount += result.apiRequestCount;
		}
	}

	logInfo("meta_ads_deterministic_sync_completed", {
		service: process.env.K_SERVICE ?? "roas-radar",
		triggerSource,
		...summary,
	});

	return summary;
}

export const __metaDeterministicTestUtils = {
	normalizeInsightsRows,
	buildDedupeKey,
};
