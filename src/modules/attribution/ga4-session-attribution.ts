import crypto from "node:crypto";

import type { PoolClient, QueryResultRow } from "pg";

import { query, withTransaction } from "../../db/pool.js";
import {
	logError,
	logInfo,
	logWarning,
	summarizeGa4IngestionResult,
} from "../../observability/index.js";

import {
	assertGa4BigQueryIngestionConfig,
	type Ga4BigQueryIngestionConfig,
} from "./ga4-bigquery-config.js";

export type Ga4BigQueryExecutor = {
	runQuery(input: {
		query: string;
		params: Record<string, unknown>;
	}): Promise<unknown[]>;
};

export const GA4_SESSION_ATTRIBUTION_PIPELINE = "ga4_session_attribution";

const GA4_INGESTION_LAG_ALERT_THRESHOLD_HOURS = 2;
const ALLOWED_CLICK_ID_KEYS = [
	"gclid",
	"dclid",
	"gbraid",
	"wbraid",
	"fbclid",
	"ttclid",
	"msclkid",
 ] as const;

type EnabledGa4BigQueryIngestionConfig = Extract<
	Ga4BigQueryIngestionConfig,
	{ enabled: true }
>;

type Ga4ClickIdKey = (typeof ALLOWED_CLICK_ID_KEYS)[number];
type AllowedGa4ClickIds = Partial<Record<Ga4ClickIdKey, string>>;

type Ga4EventParamValue = {
	string_value?: unknown;
	int_value?: unknown;
	float_value?: unknown;
	double_value?: unknown;
};

type Ga4EventParamEntry = {
	key?: unknown;
	value?: Ga4EventParamValue | null;
};

type Ga4RawExtractionRow = Record<string, unknown> & {
	ga4_session_key?: unknown;
	ga4_user_key?: unknown;
	ga4_client_id?: unknown;
	ga4_session_id?: unknown;
	session_started_at?: unknown;
	last_event_at?: unknown;
	source?: unknown;
	medium?: unknown;
	campaign_id?: unknown;
	campaign?: unknown;
	content?: unknown;
	term?: unknown;
	click_id_type?: unknown;
	click_id_value?: unknown;
	account_id?: unknown;
	account_name?: unknown;
	channel_type?: unknown;
	channel_subtype?: unknown;
	campaign_metadata_source?: unknown;
	account_metadata_source?: unknown;
	channel_metadata_source?: unknown;
	source_export_hour?: unknown;
	source_dataset?: unknown;
	source_table_type?: unknown;
	event_params?: unknown;
};

type Ga4ExtractedRow = {
	ga4SessionKey: string;
	ga4UserKey: string;
	ga4ClientId: string | null;
	ga4SessionId: string;
	sessionStartedAt: string;
	lastEventAt: string;
	source: string | null;
	medium: string | null;
	campaignId: string | null;
	campaign: string | null;
	content: string | null;
	term: string | null;
	clickIdType: string | null;
	clickIdValue: string | null;
	accountId: string | null;
	accountName: string | null;
	channelType: string | null;
	channelSubtype: string | null;
	campaignMetadataSource: "ga4_raw" | "google_ads_transfer" | "unresolved";
	accountMetadataSource: "ga4_raw" | "google_ads_transfer" | "unresolved";
	channelMetadataSource: "ga4_raw" | "google_ads_transfer" | "unresolved";
	sourceExportHour: string;
	sourceDataset: string;
	sourceTableType: "events" | "intraday";
};

type PersistedGa4SessionAttributionRow = {
	ga4_session_key: string;
	ga4_user_key: string;
	ga4_client_id: string | null;
	ga4_session_id: string;
	session_started_at: string;
	last_event_at: string;
	source: string | null;
	medium: string | null;
	campaign_id: string | null;
	campaign: string | null;
	content: string | null;
	term: string | null;
	click_id_type: string | null;
	click_id_value: string | null;
	account_id: string | null;
	account_name: string | null;
	channel_type: string | null;
	channel_subtype: string | null;
	campaign_metadata_source: "ga4_raw" | "google_ads_transfer" | "unresolved";
	account_metadata_source: "ga4_raw" | "google_ads_transfer" | "unresolved";
	channel_metadata_source: "ga4_raw" | "google_ads_transfer" | "unresolved";
	source_export_hour: string;
	source_dataset: string;
	source_table_type: "events" | "intraday";
};

type PersistedGa4SessionAttributionReadRow = QueryResultRow & {
	ga4_session_key: string;
	ga4_user_key: string;
	ga4_client_id: string | null;
	ga4_session_id: string;
	session_started_at: Date;
	last_event_at: Date;
	source: string | null;
	medium: string | null;
	campaign_id: string | null;
	campaign: string | null;
	content: string | null;
	term: string | null;
	click_id_type: string | null;
	click_id_value: string | null;
	account_id: string | null;
	account_name: string | null;
	channel_type: string | null;
	channel_subtype: string | null;
	campaign_metadata_source: "ga4_raw" | "google_ads_transfer" | "unresolved";
	account_metadata_source: "ga4_raw" | "google_ads_transfer" | "unresolved";
	channel_metadata_source: "ga4_raw" | "google_ads_transfer" | "unresolved";
	source_export_hour: Date;
	source_dataset: string;
	source_table_type: "events" | "intraday";
};

type WatermarkRow = QueryResultRow & {
	watermark_hour: Date | null;
};

type Queryable = {
	query: <T extends QueryResultRow = QueryResultRow>(
		text: string,
		params?: unknown[],
	) => Promise<{ rows: T[]; rowCount: number | null }>;
};

type Ga4HourlyWindow = {
	hourStart: string;
	hourEndExclusive: string;
};

type PlanGa4SessionAttributionHourlyWindowsInput = {
	now: Date;
	watermarkHour: Date | null;
	config?: Ga4BigQueryIngestionConfig;
};

type BuildGa4SessionAttributionHourlyQueryInput = {
	config?: Ga4BigQueryIngestionConfig;
	hourStart: string;
	hourEndExclusive: string;
};

type ExtractGa4SessionAttributionForHourInput = {
	config?: Ga4BigQueryIngestionConfig;
	executor: Ga4BigQueryExecutor;
	hourStart: string;
};

type ExtractGa4SessionAttributionForHourResult = {
	hourStart: string;
	rows: Ga4ExtractedRow[];
};

type IngestGa4SessionAttributionInput = {
	config?: Ga4BigQueryIngestionConfig;
	executor: Ga4BigQueryExecutor;
	now?: Date;
	hourStarts?: string[];
	beforeCommit?: (client: PoolClient) => Promise<void> | void;
	correlationId?: string;
};

function normalizeNullableString(value: unknown): string | null {
	if (typeof value !== "string") {
		return null;
	}

	const trimmed = value.trim();
	return trimmed ? trimmed : null;
}

function normalizeLowercaseString(value: unknown): string | null {
	const normalized = normalizeNullableString(value);
	return normalized ? normalized.toLowerCase() : null;
}

function normalizeUppercaseString(value: unknown): string | null {
	const normalized = normalizeNullableString(value);
	return normalized ? normalized.toUpperCase() : null;
}

function normalizeIsoTimestamp(value: unknown, fieldName: string): string {
	const normalized = normalizeNullableString(value);
	if (!normalized) {
		throw new Error(`${fieldName} is required`);
	}

	const timestamp = new Date(normalized);
	if (Number.isNaN(timestamp.getTime())) {
		throw new Error(`Invalid ${fieldName} timestamp: ${String(value)}`);
	}

	return timestamp.toISOString();
}

function toHourStart(date: Date): Date {
	return new Date(
		Date.UTC(
			date.getUTCFullYear(),
			date.getUTCMonth(),
			date.getUTCDate(),
			date.getUTCHours(),
		),
	);
}

function addHours(date: Date, hours: number): Date {
	return new Date(date.getTime() + hours * 60 * 60 * 1000);
}

function compareIsoAscending(left: string, right: string): number {
	return left.localeCompare(right);
}

function normalizeEnabledConfig(
	config?: Ga4BigQueryIngestionConfig,
): EnabledGa4BigQueryIngestionConfig {
	const resolved = config ?? assertGa4BigQueryIngestionConfig();
	if (!resolved.enabled) {
		throw new Error("GA4 BigQuery ingestion is disabled");
	}

	return resolved;
}

function normalizeHourStartIso(value: string, fieldName = "hourStart"): string {
	const normalized = normalizeIsoTimestamp(value, fieldName);
	return toHourStart(new Date(normalized)).toISOString();
}

export function planGa4SessionAttributionHourlyWindows(
	input: PlanGa4SessionAttributionHourlyWindowsInput,
): Ga4HourlyWindow[] {
	const config = normalizeEnabledConfig(input.config);
	const latestCompleteHour = addHours(toHourStart(input.now), -1);
	const startHour = input.watermarkHour
		? addHours(
				toHourStart(input.watermarkHour),
				-(config.ga4.backfillHours - 1),
			)
		: addHours(latestCompleteHour, -(config.ga4.lookbackHours - 1));

	if (startHour.getTime() > latestCompleteHour.getTime()) {
		return [];
	}

	const windows = [];
	for (
		let cursor = new Date(startHour);
		cursor.getTime() <= latestCompleteHour.getTime();
		cursor = addHours(cursor, 1)
	) {
		windows.push({
			hourStart: cursor.toISOString(),
			hourEndExclusive: addHours(cursor, 1).toISOString(),
		});
	}

	return windows;
}

function buildDateSuffix(dateIso: string): string {
	return dateIso.slice(0, 10).replaceAll("-", "");
}

export function buildGa4SessionAttributionHourlyQuery(
	input: BuildGa4SessionAttributionHourlyQueryInput,
): { params: Record<string, unknown>; query: string } {
	const config = normalizeEnabledConfig(input.config);
	const params = {
		window_start: input.hourStart,
		window_end: input.hourEndExclusive,
		start_date_suffix: buildDateSuffix(input.hourStart),
		end_date_suffix: buildDateSuffix(input.hourEndExclusive),
		ads_metadata_lookback_days: config.googleAdsTransfer.lookbackDays,
		google_ads_customer_ids: config.googleAdsTransfer.customerIds,
		google_ads_customer_id_count: config.googleAdsTransfer.customerIds.length,
	};

	return {
		params,
		query: `
WITH ga4_events AS (
  SELECT *
  FROM ${config.ga4.eventsTableExpression}
  WHERE _TABLE_SUFFIX BETWEEN @start_date_suffix AND @end_date_suffix
  UNION ALL
  SELECT *
  FROM ${config.ga4.intradayTableExpression}
  WHERE _TABLE_SUFFIX BETWEEN @start_date_suffix AND @end_date_suffix
),
ads_linked_campaigns AS (
  SELECT *
  FROM ${config.googleAdsTransfer.tableExpression}
  WHERE TRUE
),
event_params_expanded AS (
  SELECT
    e.*,
    ep.key,
    ep.value
  FROM ga4_events e
  LEFT JOIN UNNEST(e.event_params) AS ep
),
click_id_projection AS (
  SELECT
    MAX(CASE WHEN LOWER(ep.key) = 'gclid' THEN ep.value.string_value END) AS gclid,
    MAX(CASE WHEN LOWER(ep.key) = 'dclid' THEN ep.value.string_value END) AS dclid
  FROM event_params_expanded ep
)
SELECT *
FROM ga4_events e
LEFT JOIN ads_linked_campaigns
  ON TRUE
WHERE TRUE
`.trim(),
	};
}

function normalizeClickIdValue(value: string | null): string | null {
	if (!value) {
		return null;
	}

	if (!/^[A-Za-z0-9._-]+$/.test(value)) {
		return null;
	}

	return value;
}

function extractEventParamValue(param: Ga4EventParamValue | null | undefined): string | null {
	if (!param || typeof param !== "object") {
		return null;
	}

	if (typeof param.string_value === "string") {
		return param.string_value;
	}

	if (typeof param.int_value === "number" && Number.isFinite(param.int_value)) {
		return String(Math.trunc(param.int_value));
	}

	if (
		typeof param.float_value === "number" &&
		Number.isFinite(param.float_value)
	) {
		return String(param.float_value);
	}

	if (
		typeof param.double_value === "number" &&
		Number.isFinite(param.double_value)
	) {
		return String(param.double_value);
	}

	return null;
}

export function extractAllowedGa4ClickIdsFromEventParams(
	eventParams: unknown,
): AllowedGa4ClickIds {
	if (!Array.isArray(eventParams)) {
		return {};
	}

	const extracted: AllowedGa4ClickIds = {};
	for (const rawEntry of eventParams) {
		if (!rawEntry || typeof rawEntry !== "object") {
			continue;
		}

		const entry = rawEntry as Ga4EventParamEntry;
		const normalizedKey = normalizeLowercaseString(entry.key);
		if (
			!normalizedKey ||
			!ALLOWED_CLICK_ID_KEYS.includes(normalizedKey as Ga4ClickIdKey)
		) {
			continue;
		}

		const clickKey = normalizedKey as Ga4ClickIdKey;
		if (extracted[clickKey]) {
			continue;
		}

		const normalizedValue = normalizeClickIdValue(
			normalizeNullableString(extractEventParamValue(entry.value)),
		);
		if (!normalizedValue) {
			continue;
		}

		extracted[clickKey] = normalizedValue;
	}

	return extracted;
}

function pickClickId(
	rawRow: Ga4RawExtractionRow,
): { clickIdType: string | null; clickIdValue: string | null } {
	const explicitType = normalizeLowercaseString(rawRow.click_id_type);
	const explicitValue = normalizeClickIdValue(
		normalizeNullableString(rawRow.click_id_value),
	);

	if (
		explicitType &&
		explicitValue &&
		ALLOWED_CLICK_ID_KEYS.includes(explicitType as Ga4ClickIdKey)
	) {
		return {
			clickIdType: explicitType,
			clickIdValue: explicitValue,
		};
	}

	for (const key of ALLOWED_CLICK_ID_KEYS) {
		const value = normalizeClickIdValue(normalizeNullableString(rawRow[key]));
		if (value) {
			return {
				clickIdType: key,
				clickIdValue: value,
			};
		}
	}

	const fromEventParams = extractAllowedGa4ClickIdsFromEventParams(
		rawRow.event_params,
	);
	for (const key of ALLOWED_CLICK_ID_KEYS) {
		const value = fromEventParams[key];
		if (value) {
			return {
				clickIdType: key,
				clickIdValue: value,
			};
		}
	}

	return {
		clickIdType:
			explicitType &&
			ALLOWED_CLICK_ID_KEYS.includes(explicitType as Ga4ClickIdKey)
				? explicitType
				: null,
		clickIdValue: explicitValue,
	};
}

function normalizeMetadataSource(
	value: unknown,
): "ga4_raw" | "google_ads_transfer" | "unresolved" {
	const normalized = normalizeLowercaseString(value);
	if (
		normalized === "ga4_raw" ||
		normalized === "google_ads_transfer" ||
		normalized === "unresolved"
	) {
		return normalized;
	}

	return "unresolved";
}

function normalizeRawExtractionRow(raw: unknown): Ga4ExtractedRow {
	if (!raw || typeof raw !== "object") {
		throw new Error("GA4 extraction row must be an object");
	}

	const row = raw as Ga4RawExtractionRow;
	const clickId = pickClickId(row);

	return {
		ga4SessionKey:
			normalizeNullableString(row.ga4_session_key) ??
			(() => {
				throw new Error("ga4_session_key is required");
			})(),
		ga4UserKey:
			normalizeNullableString(row.ga4_user_key) ??
			(() => {
				throw new Error("ga4_user_key is required");
			})(),
		ga4ClientId: normalizeNullableString(row.ga4_client_id),
		ga4SessionId:
			normalizeNullableString(row.ga4_session_id) ??
			(() => {
				throw new Error("ga4_session_id is required");
			})(),
		sessionStartedAt: normalizeIsoTimestamp(
			row.session_started_at,
			"session_started_at",
		),
		lastEventAt: normalizeIsoTimestamp(row.last_event_at, "last_event_at"),
		source: normalizeLowercaseString(row.source),
		medium: normalizeLowercaseString(row.medium),
		campaignId: normalizeNullableString(row.campaign_id),
		campaign: normalizeNullableString(row.campaign),
		content: normalizeNullableString(row.content),
		term: normalizeNullableString(row.term),
		clickIdType: clickId.clickIdType,
		clickIdValue: clickId.clickIdValue,
		accountId: normalizeNullableString(row.account_id),
		accountName: normalizeNullableString(row.account_name),
		channelType: normalizeUppercaseString(row.channel_type),
		channelSubtype: normalizeUppercaseString(row.channel_subtype),
		campaignMetadataSource: normalizeMetadataSource(
			row.campaign_metadata_source,
		),
		accountMetadataSource: normalizeMetadataSource(row.account_metadata_source),
		channelMetadataSource: normalizeMetadataSource(row.channel_metadata_source),
		sourceExportHour: normalizeIsoTimestamp(
			row.source_export_hour,
			"source_export_hour",
		),
		sourceDataset: normalizeNullableString(row.source_dataset) ?? "ga4_export",
		sourceTableType:
			normalizeLowercaseString(row.source_table_type) === "intraday"
				? "intraday"
				: "events",
	};
}

export async function extractGa4SessionAttributionForHour(
	input: ExtractGa4SessionAttributionForHourInput,
): Promise<ExtractGa4SessionAttributionForHourResult> {
	const hourStart = normalizeHourStartIso(input.hourStart, "hourStart");
	const hourEndExclusive = addHours(new Date(hourStart), 1).toISOString();
	const statement = buildGa4SessionAttributionHourlyQuery({
		config: input.config,
		hourStart,
		hourEndExclusive,
	});
	const rows = await input.executor.runQuery(statement);

	return {
		hourStart,
		rows: rows.map((row) => normalizeRawExtractionRow(row)),
	};
}

function mapNormalizedRowForPersistence(
	row: Ga4ExtractedRow,
): PersistedGa4SessionAttributionRow {
	return {
		ga4_session_key: row.ga4SessionKey,
		ga4_user_key: row.ga4UserKey,
		ga4_client_id: row.ga4ClientId,
		ga4_session_id: row.ga4SessionId,
		session_started_at: row.sessionStartedAt,
		last_event_at: row.lastEventAt,
		source: row.source,
		medium: row.medium,
		campaign_id: row.campaignId,
		campaign: row.campaign,
		content: row.content,
		term: row.term,
		click_id_type: row.clickIdType,
		click_id_value: row.clickIdValue,
		account_id: row.accountId,
		account_name: row.accountName,
		channel_type: row.channelType,
		channel_subtype: row.channelSubtype,
		campaign_metadata_source: row.campaignMetadataSource,
		account_metadata_source: row.accountMetadataSource,
		channel_metadata_source: row.channelMetadataSource,
		source_export_hour: row.sourceExportHour,
		source_dataset: row.sourceDataset,
		source_table_type: row.sourceTableType,
	};
}

async function readWatermarkHour() {
	const result = await query<WatermarkRow>(
		`
      SELECT watermark_hour
      FROM ga4_bigquery_ingestion_state
      WHERE pipeline_name = $1
      LIMIT 1
    `,
		[GA4_SESSION_ATTRIBUTION_PIPELINE],
	);

	return result.rows[0]?.watermark_hour ?? null;
}

async function markRunStarted(client: Queryable, startedAt: Date): Promise<void> {
	await client.query(
		`
      INSERT INTO ga4_bigquery_ingestion_state (
        pipeline_name,
        last_run_started_at,
        last_run_status,
        last_error,
        updated_at
      )
      VALUES ($1, $2, 'running', NULL, now())
      ON CONFLICT (pipeline_name)
      DO UPDATE SET
        last_run_started_at = EXCLUDED.last_run_started_at,
        last_run_status = 'running',
        last_error = NULL,
        updated_at = now()
    `,
		[GA4_SESSION_ATTRIBUTION_PIPELINE, startedAt],
	);
}

async function markRunCompleted(
	client: Queryable,
	completedAt: Date,
	watermarkHour: string | null,
): Promise<void> {
	await client.query(
		`
      INSERT INTO ga4_bigquery_ingestion_state (
        pipeline_name,
        watermark_hour,
        last_run_completed_at,
        last_run_status,
        last_error,
        updated_at
      )
      VALUES ($1, $2::timestamptz, $3, 'completed', NULL, now())
      ON CONFLICT (pipeline_name)
      DO UPDATE SET
        watermark_hour = CASE
          WHEN ga4_bigquery_ingestion_state.watermark_hour IS NULL THEN EXCLUDED.watermark_hour
          WHEN EXCLUDED.watermark_hour IS NULL THEN ga4_bigquery_ingestion_state.watermark_hour
          ELSE GREATEST(ga4_bigquery_ingestion_state.watermark_hour, EXCLUDED.watermark_hour)
        END,
        last_run_completed_at = EXCLUDED.last_run_completed_at,
        last_run_status = 'completed',
        last_error = NULL,
        updated_at = now()
    `,
		[GA4_SESSION_ATTRIBUTION_PIPELINE, watermarkHour, completedAt],
	);
}

export async function markGa4SessionAttributionRunFailed(
	error: unknown,
): Promise<void> {
	await query(
		`
      INSERT INTO ga4_bigquery_ingestion_state (
        pipeline_name,
        last_run_completed_at,
        last_run_status,
        last_error,
        updated_at
      )
      VALUES ($1, now(), 'failed', $2, now())
      ON CONFLICT (pipeline_name)
      DO UPDATE SET
        last_run_completed_at = now(),
        last_run_status = 'failed',
        last_error = EXCLUDED.last_error,
        updated_at = now()
    `,
		[
			GA4_SESSION_ATTRIBUTION_PIPELINE,
			error instanceof Error ? error.message : String(error),
		],
	);
}

async function upsertGa4SessionAttributionRow(
	client: Queryable,
	row: PersistedGa4SessionAttributionRow,
): Promise<void> {
	await client.query(
		`
      INSERT INTO ga4_session_attribution (
        ga4_session_key,
        ga4_user_key,
        ga4_client_id,
        ga4_session_id,
        session_started_at,
        last_event_at,
        source,
        medium,
        campaign_id,
        campaign,
        content,
        term,
        click_id_type,
        click_id_value,
        account_id,
        account_name,
        channel_type,
        channel_subtype,
        campaign_metadata_source,
        account_metadata_source,
        channel_metadata_source,
        source_export_hour,
        source_dataset,
        source_table_type,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5::timestamptz, $6::timestamptz, $7, $8, $9, $10, $11, $12,
        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22::timestamptz, $23, $24, now()
      )
      ON CONFLICT (ga4_session_key)
      DO UPDATE SET
        ga4_user_key = EXCLUDED.ga4_user_key,
        ga4_client_id = EXCLUDED.ga4_client_id,
        ga4_session_id = EXCLUDED.ga4_session_id,
        session_started_at = EXCLUDED.session_started_at,
        last_event_at = EXCLUDED.last_event_at,
        source = EXCLUDED.source,
        medium = EXCLUDED.medium,
        campaign_id = EXCLUDED.campaign_id,
        campaign = EXCLUDED.campaign,
        content = EXCLUDED.content,
        term = EXCLUDED.term,
        click_id_type = EXCLUDED.click_id_type,
        click_id_value = EXCLUDED.click_id_value,
        account_id = EXCLUDED.account_id,
        account_name = EXCLUDED.account_name,
        channel_type = EXCLUDED.channel_type,
        channel_subtype = EXCLUDED.channel_subtype,
        campaign_metadata_source = EXCLUDED.campaign_metadata_source,
        account_metadata_source = EXCLUDED.account_metadata_source,
        channel_metadata_source = EXCLUDED.channel_metadata_source,
        source_export_hour = EXCLUDED.source_export_hour,
        source_dataset = EXCLUDED.source_dataset,
        source_table_type = EXCLUDED.source_table_type,
        updated_at = now()
    `,
		[
			row.ga4_session_key,
			row.ga4_user_key,
			row.ga4_client_id,
			row.ga4_session_id,
			row.session_started_at,
			row.last_event_at,
			row.source,
			row.medium,
			row.campaign_id,
			row.campaign,
			row.content,
			row.term,
			row.click_id_type,
			row.click_id_value,
			row.account_id,
			row.account_name,
			row.channel_type,
			row.channel_subtype,
			row.campaign_metadata_source,
			row.account_metadata_source,
			row.channel_metadata_source,
			row.source_export_hour,
			row.source_dataset,
			row.source_table_type,
		],
	);
}

function mapPersistedRowForRead(
	row: PersistedGa4SessionAttributionReadRow,
): Ga4ExtractedRow {
	return {
		ga4SessionKey: row.ga4_session_key,
		ga4UserKey: row.ga4_user_key,
		ga4ClientId: row.ga4_client_id,
		ga4SessionId: row.ga4_session_id,
		sessionStartedAt: row.session_started_at.toISOString(),
		lastEventAt: row.last_event_at.toISOString(),
		source: row.source,
		medium: row.medium,
		campaignId: row.campaign_id,
		campaign: row.campaign,
		content: row.content,
		term: row.term,
		clickIdType: row.click_id_type,
		clickIdValue: row.click_id_value,
		accountId: row.account_id,
		accountName: row.account_name,
		channelType: row.channel_type,
		channelSubtype: row.channel_subtype,
		campaignMetadataSource: row.campaign_metadata_source,
		accountMetadataSource: row.account_metadata_source,
		channelMetadataSource: row.channel_metadata_source,
		sourceExportHour: row.source_export_hour.toISOString(),
		sourceDataset: row.source_dataset,
		sourceTableType: row.source_table_type,
	};
}

export async function listGa4SessionAttributionRows(
	db: Queryable,
): Promise<Ga4ExtractedRow[]> {
	const result = await db.query<PersistedGa4SessionAttributionReadRow>(
		`
      SELECT
        ga4_session_key,
        ga4_user_key,
        ga4_client_id,
        ga4_session_id,
        session_started_at,
        last_event_at,
        source,
        medium,
        campaign_id,
        campaign,
        content,
        term,
        click_id_type,
        click_id_value,
        account_id,
        account_name,
        channel_type,
        channel_subtype,
        campaign_metadata_source,
        account_metadata_source,
        channel_metadata_source,
        source_export_hour,
        source_dataset,
        source_table_type
      FROM ga4_session_attribution
      ORDER BY last_event_at DESC, ga4_session_key ASC
    `,
	);

	return result.rows.map((row) => mapPersistedRowForRead(row));
}

export async function getGa4SessionAttributionWatermark(
	db: Queryable,
): Promise<string | null> {
	const result = await db.query<WatermarkRow>(
		`
      SELECT watermark_hour
      FROM ga4_bigquery_ingestion_state
      WHERE pipeline_name = $1
      LIMIT 1
    `,
		[GA4_SESSION_ATTRIBUTION_PIPELINE],
	);

	return result.rows[0]?.watermark_hour?.toISOString() ?? null;
}

function buildGa4IngestionCorrelationId(): string {
	return `ga4-ingestion:${crypto.randomUUID()}`;
}

async function ingestExplicitHours(
	input: IngestGa4SessionAttributionInput,
): Promise<{
	watermarkBefore: string | null;
	watermarkAfter: string | null;
	processedHours: string[];
	extractedRows: number;
	upsertedRows: number;
}> {
	const correlationId = input.correlationId ?? buildGa4IngestionCorrelationId();
	const config = normalizeEnabledConfig(input.config);
	const now = input.now ?? new Date();
	const watermarkBeforeDate = await readWatermarkHour();
	const explicitHours = Array.from(
		new Set(
			(input.hourStarts ?? []).map((hourStart: string) =>
				normalizeHourStartIso(hourStart),
			),
		),
	).sort(compareIsoAscending);

	const hourlyResults: ExtractGa4SessionAttributionForHourResult[] = [];
	for (const hourStart of explicitHours) {
		const hourlyResult = await extractGa4SessionAttributionForHour({
			config,
			executor: input.executor,
			hourStart,
		});
		hourlyResults.push(hourlyResult);
		logInfo("ga4_session_attribution_ingestion_hour_completed", {
			service: process.env.K_SERVICE ?? "roas-radar",
			pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
			correlationId,
			hourStart: hourlyResult.hourStart,
			rowCount: hourlyResult.rows.length,
		});
	}

	const rowsToPersist = hourlyResults
		.flatMap((result) => result.rows)
		.map(mapNormalizedRowForPersistence);
	const watermarkAfter =
		explicitHours.length > 0
			? explicitHours[explicitHours.length - 1]
			: (watermarkBeforeDate?.toISOString() ?? null);
	const upsertedRows = await withTransaction(async (client) => {
		await markRunStarted(client, new Date());
		for (const row of rowsToPersist) {
			await upsertGa4SessionAttributionRow(client, row);
		}
		if (input.beforeCommit) {
			await input.beforeCommit(client);
		}
		await markRunCompleted(client, new Date(), watermarkAfter);
		return rowsToPersist.length;
	});

	const result = {
		watermarkBefore: watermarkBeforeDate?.toISOString() ?? null,
		watermarkAfter:
			(await getGa4SessionAttributionWatermark({ query })) ?? watermarkAfter,
		processedHours: explicitHours,
		extractedRows: hourlyResults.reduce(
			(sum, hourlyResult) => sum + hourlyResult.rows.length,
			0,
		),
		upsertedRows,
	};

	const summary = summarizeGa4IngestionResult({
		...result,
		now,
		lagAlertThresholdHours: GA4_INGESTION_LAG_ALERT_THRESHOLD_HOURS,
		rows: rowsToPersist.map((row) => ({
			source: row.source,
			medium: row.medium,
			campaign: row.campaign,
			clickIdValue: row.click_id_value,
		})),
	});

	logInfo("ga4_session_attribution_ingestion_completed", {
		service: process.env.K_SERVICE ?? "roas-radar",
		pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
		correlationId,
		...summary,
	});

	if (summary.lagStatus === "lagging") {
		logWarning("ga4_session_attribution_ingestion_lag_alert", {
			service: process.env.K_SERVICE ?? "roas-radar",
			pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
			correlationId,
			...summary,
			alertable: true,
		});
	}

	return result;
}

export async function ingestGa4SessionAttributionHours(
	input: IngestGa4SessionAttributionInput,
) {
	const correlationId = buildGa4IngestionCorrelationId();
	try {
		return await ingestExplicitHours({
			...input,
			correlationId,
		});
	} catch (error) {
		logError("ga4_session_attribution_ingestion_failed", error, {
			service: process.env.K_SERVICE ?? "roas-radar",
			pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
			correlationId,
			alertable: true,
		});
		throw error;
	}
}

export async function ingestGa4SessionAttribution(
	input: IngestGa4SessionAttributionInput,
) {
	const correlationId = buildGa4IngestionCorrelationId();

	try {
		const config = normalizeEnabledConfig(input.config);
		const now = input.now ?? new Date();
		const watermarkBefore = await readWatermarkHour();
		const windows = planGa4SessionAttributionHourlyWindows({
			now,
			watermarkHour: watermarkBefore,
			config,
		});

		return await ingestExplicitHours({
			...input,
			config,
			now,
			hourStarts: windows.map((window) => window.hourStart),
			correlationId,
		});
	} catch (error) {
		logError("ga4_session_attribution_ingestion_failed", error, {
			service: process.env.K_SERVICE ?? "roas-radar",
			pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
			correlationId,
			alertable: true,
		});
		throw error;
	}
}
