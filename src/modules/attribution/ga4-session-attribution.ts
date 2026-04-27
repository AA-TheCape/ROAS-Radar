import type { PoolClient, QueryResult, QueryResultRow } from 'pg';

import { query, withTransaction } from '../../db/pool.js';
import {
  resolveGa4BigQueryIngestionConfig,
  type Ga4BigQueryIngestionConfig
} from './ga4-bigquery-config.js';

const GA4_SESSION_ATTRIBUTION_PIPELINE = 'ga4_session_attribution';
const HOUR_IN_MILLISECONDS = 60 * 60 * 1000;
const GA4_CLICK_ID_VALUE_MAX_LENGTH = 255;
const GA4_ALLOWED_CLICK_ID_KEYS = [
  'gclid',
  'dclid',
  'gbraid',
  'wbraid',
  'fbclid',
  'ttclid',
  'msclkid'
] as const;

type Ga4AllowedClickIdKey = (typeof GA4_ALLOWED_CLICK_ID_KEYS)[number];

type EnabledGa4BigQueryIngestionConfig = Extract<Ga4BigQueryIngestionConfig, { enabled: true }>;

type QueryExecutor = typeof query | Pick<PoolClient, 'query'>;

type IngestionStateRow = {
  watermark_hour: Date | null;
};

type PersistedGa4SessionAttributionRow = {
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
  campaign_metadata_source: MetadataLineage;
  account_metadata_source: MetadataLineage;
  channel_metadata_source: MetadataLineage;
  source_export_hour: Date;
  source_dataset: string;
  source_table_type: string;
};

type BigQueryQueryRow = Record<string, unknown>;

type Ga4EventParamValue = {
  string_value?: unknown;
  int_value?: unknown;
  double_value?: unknown;
  float_value?: unknown;
};

type Ga4EventParam = {
  key?: unknown;
  value?: Ga4EventParamValue | null;
};

type MetadataLineage = 'ga4_raw' | 'google_ads_transfer' | 'unresolved';

export type Ga4SessionAttributionRow = {
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
  campaignMetadataSource: MetadataLineage;
  accountMetadataSource: MetadataLineage;
  channelMetadataSource: MetadataLineage;
  sourceExportHour: string;
  sourceDataset: string;
  sourceTableType: 'events' | 'intraday';
};

export type Ga4HourlyExtractionWindow = {
  hourStart: string;
  hourEndExclusive: string;
};

export type Ga4HourlyExtractionResult = {
  hourStart: string;
  rows: Ga4SessionAttributionRow[];
};

export type Ga4SessionAttributionIngestionResult = {
  watermarkBefore: string | null;
  watermarkAfter: string | null;
  processedHours: string[];
  extractedRows: number;
  upsertedRows: number;
};

export interface Ga4BigQueryExecutor {
  runQuery<T extends BigQueryQueryRow = BigQueryQueryRow>(input: {
    query: string;
    location: string;
    params: Record<string, unknown>;
  }): Promise<T[]>;
}

export async function createDefaultGa4BigQueryExecutor(
  config: EnabledGa4BigQueryIngestionConfig
): Promise<Ga4BigQueryExecutor> {
  const importer = new Function('specifier', 'return import(specifier)') as (specifier: string) => Promise<{
    BigQuery: new (options: { projectId: string; location: string }) => {
      query: (options: {
        query: string;
        params: Record<string, unknown>;
        location: string;
        useLegacySql: false;
      }) => Promise<[BigQueryQueryRow[]]>;
    };
  }>;

  const { BigQuery } = await importer('@google-cloud/bigquery').catch((error: unknown) => {
    throw new Error(
      `GA4 BigQuery ingestion requires the optional @google-cloud/bigquery dependency: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
  });

  const client = new BigQuery({
    projectId: config.ga4.projectId,
    location: config.ga4.location
  });

  return {
    async runQuery<T extends BigQueryQueryRow = BigQueryQueryRow>(input: {
      query: string;
      location: string;
      params: Record<string, unknown>;
    }): Promise<T[]> {
      const [rows] = await client.query({
        query: input.query,
        params: input.params,
        location: input.location,
        useLegacySql: false
      });

      return rows as T[];
    }
  };
}

function normalizeNullableString(value: unknown, { lowerCase = false }: { lowerCase?: boolean } = {}): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  return lowerCase ? trimmed.toLowerCase() : trimmed;
}

function normalizeGa4ClickIdValue(value: unknown): string | null {
  if (typeof value !== 'string' && typeof value !== 'number') {
    return null;
  }

  const normalized = String(value).trim();

  if (!normalized) {
    return null;
  }

  if (normalized.length > GA4_CLICK_ID_VALUE_MAX_LENGTH) {
    return null;
  }

  if (/\s/.test(normalized) || /[\u0000-\u001f\u007f]/.test(normalized)) {
    return null;
  }

  return normalized;
}

function normalizeGa4ClickIdKey(value: unknown): Ga4AllowedClickIdKey | null {
  const normalized = normalizeNullableString(value, { lowerCase: true });

  if (!normalized) {
    return null;
  }

  return GA4_ALLOWED_CLICK_ID_KEYS.find((candidate) => candidate === normalized) ?? null;
}

function readGa4EventParamValue(value: Ga4EventParamValue | null | undefined): string | null {
  if (!value || typeof value !== 'object') {
    return null;
  }

  return (
    normalizeGa4ClickIdValue(value.string_value) ??
    normalizeGa4ClickIdValue(value.int_value) ??
    normalizeGa4ClickIdValue(value.double_value) ??
    normalizeGa4ClickIdValue(value.float_value)
  );
}

export function extractAllowedGa4ClickIdsFromEventParams(
  eventParams: unknown
): Partial<Record<Ga4AllowedClickIdKey, string>> {
  if (!Array.isArray(eventParams)) {
    return {};
  }

  const extracted: Partial<Record<Ga4AllowedClickIdKey, string>> = {};

  for (const candidate of eventParams as Ga4EventParam[]) {
    const key = normalizeGa4ClickIdKey(candidate?.key);
    if (!key || extracted[key]) {
      continue;
    }

    const value = readGa4EventParamValue(candidate?.value);
    if (!value) {
      continue;
    }

    extracted[key] = value;
  }

  return extracted;
}

function normalizeRequiredString(value: unknown, fieldName: string): string {
  const normalized = normalizeNullableString(value);

  if (!normalized) {
    throw new Error(`GA4 row is missing required ${fieldName}`);
  }

  return normalized;
}

function normalizeDate(value: unknown, fieldName: string): string {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new Error(`GA4 row has invalid ${fieldName}`);
    }

    return value.toISOString();
  }

  if (typeof value === 'string' || typeof value === 'number') {
    const normalized = new Date(value);

    if (Number.isNaN(normalized.getTime())) {
      throw new Error(`GA4 row has invalid ${fieldName}`);
    }

    return normalized.toISOString();
  }

  throw new Error(`GA4 row is missing required ${fieldName}`);
}

function normalizeMetadataLineage(value: unknown, fieldName: string): MetadataLineage {
  const normalized = normalizeNullableString(value);

  if (normalized === 'ga4_raw' || normalized === 'google_ads_transfer' || normalized === 'unresolved') {
    return normalized;
  }

  throw new Error(`GA4 row has invalid ${fieldName}`);
}

function floorToHour(date: Date): Date {
  return new Date(Math.floor(date.getTime() / HOUR_IN_MILLISECONDS) * HOUR_IN_MILLISECONDS);
}

function addHours(date: Date, hours: number): Date {
  return new Date(date.getTime() + hours * HOUR_IN_MILLISECONDS);
}

function compareIsoAscending(left: string, right: string): number {
  return left.localeCompare(right);
}

function compareIsoDescending(left: string, right: string): number {
  return right.localeCompare(left);
}

function selectPreferredClickId(row: Record<string, string | null>): { type: string | null; value: string | null } {
  const clickIdPreference: Array<{ type: string; value: string | null }> = [
    { type: 'gclid', value: row.gclid ?? null },
    { type: 'dclid', value: row.dclid ?? null },
    { type: 'gbraid', value: row.gbraid ?? null },
    { type: 'wbraid', value: row.wbraid ?? null },
    { type: 'fbclid', value: row.fbclid ?? null },
    { type: 'ttclid', value: row.ttclid ?? null },
    { type: 'msclkid', value: row.msclkid ?? null }
  ];

  for (const candidate of clickIdPreference) {
    if (candidate.value) {
      return candidate;
    }
  }

  return { type: null, value: null };
}

function mapBigQueryRowToNormalizedRow(row: BigQueryQueryRow): Ga4SessionAttributionRow {
  const sourceTableType = normalizeRequiredString(row.source_table_type, 'source_table_type');

  if (sourceTableType !== 'events' && sourceTableType !== 'intraday') {
    throw new Error(`GA4 row has unsupported source_table_type: ${sourceTableType}`);
  }

  return {
    ga4SessionKey: normalizeRequiredString(row.ga4_session_key, 'ga4_session_key'),
    ga4UserKey: normalizeRequiredString(row.ga4_user_key, 'ga4_user_key'),
    ga4ClientId: normalizeNullableString(row.ga4_client_id),
    ga4SessionId: normalizeRequiredString(row.ga4_session_id, 'ga4_session_id'),
    sessionStartedAt: normalizeDate(row.session_started_at, 'session_started_at'),
    lastEventAt: normalizeDate(row.last_event_at, 'last_event_at'),
    source: normalizeNullableString(row.source, { lowerCase: true }),
    medium: normalizeNullableString(row.medium, { lowerCase: true }),
    campaignId: normalizeNullableString(row.campaign_id),
    campaign: normalizeNullableString(row.campaign),
    content: normalizeNullableString(row.content),
    term: normalizeNullableString(row.term),
    clickIdType: normalizeNullableString(row.click_id_type, { lowerCase: true }),
    clickIdValue: normalizeNullableString(row.click_id_value),
    accountId: normalizeNullableString(row.account_id),
    accountName: normalizeNullableString(row.account_name),
    channelType: normalizeNullableString(row.channel_type),
    channelSubtype: normalizeNullableString(row.channel_subtype),
    campaignMetadataSource: normalizeMetadataLineage(row.campaign_metadata_source, 'campaign_metadata_source'),
    accountMetadataSource: normalizeMetadataLineage(row.account_metadata_source, 'account_metadata_source'),
    channelMetadataSource: normalizeMetadataLineage(row.channel_metadata_source, 'channel_metadata_source'),
    sourceExportHour: normalizeDate(row.source_export_hour, 'source_export_hour'),
    sourceDataset: normalizeRequiredString(row.source_dataset, 'source_dataset'),
    sourceTableType
  };
}

function mapNormalizedRowForPersistence(row: Ga4SessionAttributionRow): PersistedGa4SessionAttributionRow {
  return {
    ga4_session_key: row.ga4SessionKey,
    ga4_user_key: row.ga4UserKey,
    ga4_client_id: row.ga4ClientId,
    ga4_session_id: row.ga4SessionId,
    session_started_at: new Date(row.sessionStartedAt),
    last_event_at: new Date(row.lastEventAt),
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
    source_export_hour: new Date(row.sourceExportHour),
    source_dataset: row.sourceDataset,
    source_table_type: row.sourceTableType
  };
}

function executeQuery<TResult extends QueryResultRow = QueryResultRow>(
  executor: QueryExecutor,
  sql: string,
  params?: unknown[]
): Promise<QueryResult<TResult>> {
  if (typeof executor === 'function') {
    return executor<TResult>(sql, params);
  }

  return executor.query<TResult>(sql, params);
}

async function ensureIngestionStateRow(executor: QueryExecutor): Promise<void> {
  await executeQuery(
    executor,
    `
      INSERT INTO ga4_bigquery_ingestion_state (
        pipeline_name,
        last_run_status
      )
      VALUES ($1, 'idle')
      ON CONFLICT (pipeline_name) DO NOTHING
    `,
    [GA4_SESSION_ATTRIBUTION_PIPELINE]
  );
}

async function readWatermarkHour(executor: QueryExecutor = query): Promise<Date | null> {
  await ensureIngestionStateRow(executor);

  const result = await executeQuery<IngestionStateRow>(
    executor,
    `
      SELECT watermark_hour
      FROM ga4_bigquery_ingestion_state
      WHERE pipeline_name = $1
      LIMIT 1
    `,
    [GA4_SESSION_ATTRIBUTION_PIPELINE]
  );

  return result.rows[0]?.watermark_hour ?? null;
}

function normalizeEnabledConfig(
  config: Ga4BigQueryIngestionConfig | undefined
): EnabledGa4BigQueryIngestionConfig {
  const resolved = config ?? resolveGa4BigQueryIngestionConfig();

  if (!resolved.enabled) {
    throw new Error('GA4 BigQuery ingestion is disabled');
  }

  return resolved;
}

export function planGa4SessionAttributionHourlyWindows(input: {
  now?: Date;
  watermarkHour?: Date | null;
  config: Ga4BigQueryIngestionConfig;
}): Ga4HourlyExtractionWindow[] {
  const enabledConfig = normalizeEnabledConfig(input.config);
  const now = input.now ?? new Date();
  const latestCompleteHour = addHours(floorToHour(now), -1);

  if (latestCompleteHour.getTime() < 0) {
    return [];
  }

  const startHour = input.watermarkHour
    ? addHours(floorToHour(input.watermarkHour), -(enabledConfig.ga4.backfillHours - 1))
    : addHours(latestCompleteHour, -(enabledConfig.ga4.lookbackHours - 1));

  const hours: Ga4HourlyExtractionWindow[] = [];

  for (
    let cursor = floorToHour(startHour);
    cursor.getTime() <= latestCompleteHour.getTime();
    cursor = addHours(cursor, 1)
  ) {
    hours.push({
      hourStart: cursor.toISOString(),
      hourEndExclusive: addHours(cursor, 1).toISOString()
    });
  }

  return hours;
}

export function buildGa4SessionAttributionHourlyQuery(input: {
  config: Ga4BigQueryIngestionConfig;
  hourStart: string;
  hourEndExclusive: string;
}): {
  query: string;
  location: string;
  params: Record<string, unknown>;
} {
  const config = normalizeEnabledConfig(input.config);
  const hourStart = normalizeDate(input.hourStart, 'hourStart');
  const hourEndExclusive = normalizeDate(input.hourEndExclusive, 'hourEndExclusive');
  const startDateSuffix = hourStart.slice(0, 10).replaceAll('-', '');
  const endDateSuffix = hourEndExclusive.slice(0, 10).replaceAll('-', '');

  const buildClickIdSelect = (key: Ga4AllowedClickIdKey): string => `
          (
            SELECT ARRAY_AGG(click_id_value IGNORE NULLS ORDER BY param_offset ASC LIMIT 1)[SAFE_OFFSET(0)]
            FROM (
              SELECT
                TRIM(
                  COALESCE(
                    ep.value.string_value,
                    CAST(ep.value.int_value AS STRING),
                    CAST(ep.value.double_value AS STRING),
                    CAST(ep.value.float_value AS STRING)
                  )
                ) AS click_id_value,
                param_offset
              FROM UNNEST(event_params) ep WITH OFFSET param_offset
              WHERE LOWER(ep.key) = '${key}'
            )
            WHERE click_id_value IS NOT NULL
              AND click_id_value != ''
              AND LENGTH(click_id_value) <= ${GA4_CLICK_ID_VALUE_MAX_LENGTH}
              AND NOT REGEXP_CONTAINS(click_id_value, r'[[:space:][:cntrl:]]')
          ) AS ${key}`;

  return {
    location: config.ga4.location,
    params: {
      window_start: hourStart,
      window_end: hourEndExclusive,
      start_date_suffix: startDateSuffix,
      end_date_suffix: endDateSuffix,
      source_dataset: config.ga4.dataset,
      source_export_hour: hourStart,
      ads_metadata_lookback_days: config.googleAdsTransfer.lookbackDays,
      google_ads_customer_ids: config.googleAdsTransfer.customerIds,
      google_ads_customer_id_count: config.googleAdsTransfer.customerIds.length
    },
    query: `
      WITH exported_events AS (
        SELECT
          'events' AS source_table_type,
          TIMESTAMP_MICROS(event_timestamp) AS occurred_at,
          user_pseudo_id,
          NULLIF(user_id, '') AS user_id,
          event_name,
          (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id' LIMIT 1) AS ga_session_id,
          NULLIF(collected_traffic_source.manual_source, '') AS source,
          NULLIF(collected_traffic_source.manual_medium, '') AS medium,
          NULLIF(collected_traffic_source.manual_campaign_name, '') AS campaign,
          NULLIF(collected_traffic_source.manual_content, '') AS content,
          NULLIF(collected_traffic_source.manual_term, '') AS term,
          NULLIF(session_traffic_source_last_click.manual_campaign.campaign_id, '') AS manual_campaign_id,
          NULLIF(session_traffic_source_last_click.google_ads_campaign.customer_id, '') AS ga4_google_ads_customer_id,
          NULLIF(session_traffic_source_last_click.google_ads_campaign.account_name, '') AS ga4_google_ads_account_name,
          NULLIF(session_traffic_source_last_click.google_ads_campaign.campaign_id, '') AS ga4_google_ads_campaign_id,
          NULLIF(session_traffic_source_last_click.google_ads_campaign.campaign_name, '') AS ga4_google_ads_campaign_name,
          ${GA4_ALLOWED_CLICK_ID_KEYS.map((key) => buildClickIdSelect(key)).join(',\n')}
        FROM ${config.ga4.eventsTableExpression}
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND _TABLE_SUFFIX BETWEEN @start_date_suffix AND @end_date_suffix
          AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP(@window_start)
          AND TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP(@window_end)

        UNION ALL

        SELECT
          'intraday' AS source_table_type,
          TIMESTAMP_MICROS(event_timestamp) AS occurred_at,
          user_pseudo_id,
          NULLIF(user_id, '') AS user_id,
          event_name,
          (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id' LIMIT 1) AS ga_session_id,
          NULLIF(collected_traffic_source.manual_source, '') AS source,
          NULLIF(collected_traffic_source.manual_medium, '') AS medium,
          NULLIF(collected_traffic_source.manual_campaign_name, '') AS campaign,
          NULLIF(collected_traffic_source.manual_content, '') AS content,
          NULLIF(collected_traffic_source.manual_term, '') AS term,
          NULLIF(session_traffic_source_last_click.manual_campaign.campaign_id, '') AS manual_campaign_id,
          NULLIF(session_traffic_source_last_click.google_ads_campaign.customer_id, '') AS ga4_google_ads_customer_id,
          NULLIF(session_traffic_source_last_click.google_ads_campaign.account_name, '') AS ga4_google_ads_account_name,
          NULLIF(session_traffic_source_last_click.google_ads_campaign.campaign_id, '') AS ga4_google_ads_campaign_id,
          NULLIF(session_traffic_source_last_click.google_ads_campaign.campaign_name, '') AS ga4_google_ads_campaign_name,
          ${GA4_ALLOWED_CLICK_ID_KEYS.map((key) => buildClickIdSelect(key)).join(',\n')}
        FROM ${config.ga4.intradayTableExpression}
        WHERE _TABLE_SUFFIX BETWEEN @start_date_suffix AND @end_date_suffix
          AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP(@window_start)
          AND TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP(@window_end)
      ),
      deduplicated_events AS (
        SELECT *
        FROM exported_events
        WHERE user_pseudo_id IS NOT NULL
          AND ga_session_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY user_pseudo_id, ga_session_id, occurred_at, event_name
          ORDER BY CASE source_table_type WHEN 'events' THEN 0 ELSE 1 END
        ) = 1
      ),
      session_rollup AS (
        SELECT
          CONCAT(user_pseudo_id, ':', CAST(ga_session_id AS STRING)) AS ga4_session_key,
          COALESCE(user_id, user_pseudo_id) AS ga4_user_key,
          user_pseudo_id AS ga4_client_id,
          CAST(ga_session_id AS STRING) AS ga4_session_id,
          MIN(occurred_at) AS session_started_at,
          MAX(occurred_at) AS last_event_at,
          ARRAY_AGG(source IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS source,
          ARRAY_AGG(medium IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS medium,
          ARRAY_AGG(campaign IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS campaign,
          ARRAY_AGG(content IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS content,
          ARRAY_AGG(term IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS term,
          ARRAY_AGG(manual_campaign_id IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS manual_campaign_id,
          ARRAY_AGG(ga4_google_ads_customer_id IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS ga4_google_ads_customer_id,
          ARRAY_AGG(ga4_google_ads_account_name IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS ga4_google_ads_account_name,
          ARRAY_AGG(ga4_google_ads_campaign_id IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS ga4_google_ads_campaign_id,
          ARRAY_AGG(ga4_google_ads_campaign_name IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS ga4_google_ads_campaign_name,
          ARRAY_AGG(gclid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS gclid,
          ARRAY_AGG(dclid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS dclid,
          ARRAY_AGG(gbraid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS gbraid,
          ARRAY_AGG(wbraid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS wbraid,
          ARRAY_AGG(fbclid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS fbclid,
          ARRAY_AGG(ttclid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS ttclid,
          ARRAY_AGG(msclkid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS msclkid,
          ARRAY_AGG(source_table_type ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS source_table_type
        FROM deduplicated_events
        GROUP BY 1, 2, 3, 4
      ),
      ads_linked_campaigns AS (
        SELECT
          CAST(customer_id AS STRING) AS google_ads_customer_id,
          CAST(campaign_id AS STRING) AS google_ads_campaign_id,
          NULLIF(customer_descriptive_name, '') AS google_ads_account_name,
          NULLIF(campaign_name, '') AS google_ads_campaign_name,
          NULLIF(campaign_advertising_channel_type, '') AS google_ads_channel_type,
          NULLIF(campaign_advertising_channel_sub_type, '') AS google_ads_channel_subtype
        FROM ${config.googleAdsTransfer.tableExpression}
        WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^(Campaign_|\\d+$)')
          AND campaign_id IS NOT NULL
          AND _DATA_DATE >= DATE_SUB(DATE(@window_end), INTERVAL @ads_metadata_lookback_days DAY)
          AND (_LATEST_DATE IS NULL OR _DATA_DATE = _LATEST_DATE)
          AND (
            @google_ads_customer_id_count = 0
            OR CAST(customer_id AS STRING) IN UNNEST(@google_ads_customer_ids)
          )
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY CAST(customer_id AS STRING), CAST(campaign_id AS STRING)
          ORDER BY _DATA_DATE DESC
        ) = 1
      )
      SELECT
        session_rollup.ga4_session_key,
        session_rollup.ga4_user_key,
        session_rollup.ga4_client_id,
        session_rollup.ga4_session_id,
        session_rollup.session_started_at,
        session_rollup.last_event_at,
        session_rollup.source,
        session_rollup.medium,
        COALESCE(
          ads_linked_campaigns.google_ads_campaign_id,
          session_rollup.ga4_google_ads_campaign_id,
          session_rollup.manual_campaign_id
        ) AS campaign_id,
        COALESCE(
          ads_linked_campaigns.google_ads_campaign_name,
          session_rollup.ga4_google_ads_campaign_name,
          session_rollup.campaign
        ) AS campaign,
        session_rollup.content,
        session_rollup.term,
        CASE
          WHEN session_rollup.gclid IS NOT NULL THEN 'gclid'
          WHEN session_rollup.dclid IS NOT NULL THEN 'dclid'
          WHEN session_rollup.gbraid IS NOT NULL THEN 'gbraid'
          WHEN session_rollup.wbraid IS NOT NULL THEN 'wbraid'
          WHEN session_rollup.fbclid IS NOT NULL THEN 'fbclid'
          WHEN session_rollup.ttclid IS NOT NULL THEN 'ttclid'
          WHEN session_rollup.msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          session_rollup.gclid,
          session_rollup.dclid,
          session_rollup.gbraid,
          session_rollup.wbraid,
          session_rollup.fbclid,
          session_rollup.ttclid,
          session_rollup.msclkid
        ) AS click_id_value,
        COALESCE(
          ads_linked_campaigns.google_ads_customer_id,
          session_rollup.ga4_google_ads_customer_id
        ) AS account_id,
        COALESCE(
          ads_linked_campaigns.google_ads_account_name,
          session_rollup.ga4_google_ads_account_name
        ) AS account_name,
        ads_linked_campaigns.google_ads_channel_type AS channel_type,
        ads_linked_campaigns.google_ads_channel_subtype AS channel_subtype,
        CASE
          WHEN ads_linked_campaigns.google_ads_campaign_id IS NOT NULL
            OR ads_linked_campaigns.google_ads_campaign_name IS NOT NULL THEN 'google_ads_transfer'
          WHEN session_rollup.ga4_google_ads_campaign_id IS NOT NULL
            OR session_rollup.ga4_google_ads_campaign_name IS NOT NULL
            OR session_rollup.manual_campaign_id IS NOT NULL
            OR session_rollup.campaign IS NOT NULL THEN 'ga4_raw'
          ELSE 'unresolved'
        END AS campaign_metadata_source,
        CASE
          WHEN ads_linked_campaigns.google_ads_customer_id IS NOT NULL
            OR ads_linked_campaigns.google_ads_account_name IS NOT NULL THEN 'google_ads_transfer'
          WHEN session_rollup.ga4_google_ads_customer_id IS NOT NULL
            OR session_rollup.ga4_google_ads_account_name IS NOT NULL THEN 'ga4_raw'
          ELSE 'unresolved'
        END AS account_metadata_source,
        CASE
          WHEN ads_linked_campaigns.google_ads_channel_type IS NOT NULL
            OR ads_linked_campaigns.google_ads_channel_subtype IS NOT NULL THEN 'google_ads_transfer'
          ELSE 'unresolved'
        END AS channel_metadata_source,
        TIMESTAMP(@source_export_hour) AS source_export_hour,
        @source_dataset AS source_dataset,
        session_rollup.source_table_type
      FROM session_rollup
      LEFT JOIN ads_linked_campaigns
        ON ads_linked_campaigns.google_ads_campaign_id = COALESCE(
          session_rollup.ga4_google_ads_campaign_id,
          session_rollup.manual_campaign_id
        )
       AND (
         session_rollup.ga4_google_ads_customer_id IS NULL
         OR ads_linked_campaigns.google_ads_customer_id = session_rollup.ga4_google_ads_customer_id
       )
      ORDER BY session_rollup.last_event_at DESC, session_rollup.ga4_session_key ASC
    `
  };
}

export async function extractGa4SessionAttributionForHour(input: {
  config?: Ga4BigQueryIngestionConfig;
  executor: Ga4BigQueryExecutor;
  hourStart: Date | string;
}): Promise<Ga4HourlyExtractionResult> {
  const config = normalizeEnabledConfig(input.config);
  const hourStartIso = normalizeDate(input.hourStart, 'hourStart');
  const hourEndIso = addHours(new Date(hourStartIso), 1).toISOString();
  const queryInput = buildGa4SessionAttributionHourlyQuery({
    config,
    hourStart: hourStartIso,
    hourEndExclusive: hourEndIso
  });

  const rows = await input.executor.runQuery(queryInput);

  return {
    hourStart: hourStartIso,
    rows: rows.map((row) => {
      const normalizedRow = mapBigQueryRowToNormalizedRow(row);
      const extractedClickIds = extractAllowedGa4ClickIdsFromEventParams((row as Record<string, unknown>).event_params);
      const clickId = selectPreferredClickId({
        gclid: normalizeGa4ClickIdValue((row as Record<string, unknown>).gclid) ?? extractedClickIds.gclid ?? null,
        dclid: normalizeGa4ClickIdValue((row as Record<string, unknown>).dclid) ?? extractedClickIds.dclid ?? null,
        gbraid: normalizeGa4ClickIdValue((row as Record<string, unknown>).gbraid) ?? extractedClickIds.gbraid ?? null,
        wbraid: normalizeGa4ClickIdValue((row as Record<string, unknown>).wbraid) ?? extractedClickIds.wbraid ?? null,
        fbclid: normalizeGa4ClickIdValue((row as Record<string, unknown>).fbclid) ?? extractedClickIds.fbclid ?? null,
        ttclid: normalizeGa4ClickIdValue((row as Record<string, unknown>).ttclid) ?? extractedClickIds.ttclid ?? null,
        msclkid: normalizeGa4ClickIdValue((row as Record<string, unknown>).msclkid) ?? extractedClickIds.msclkid ?? null
      });

      return {
        ...normalizedRow,
        clickIdType: normalizedRow.clickIdType ?? clickId.type,
        clickIdValue: normalizedRow.clickIdValue ?? clickId.value
      };
    })
  };
}

async function upsertGa4SessionAttributionRow(client: PoolClient, row: PersistedGa4SessionAttributionRow): Promise<void> {
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
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, now()
      )
      ON CONFLICT (ga4_session_key)
      DO UPDATE SET
        ga4_user_key = EXCLUDED.ga4_user_key,
        ga4_client_id = COALESCE(EXCLUDED.ga4_client_id, ga4_session_attribution.ga4_client_id),
        ga4_session_id = EXCLUDED.ga4_session_id,
        session_started_at = LEAST(ga4_session_attribution.session_started_at, EXCLUDED.session_started_at),
        last_event_at = GREATEST(ga4_session_attribution.last_event_at, EXCLUDED.last_event_at),
        source = COALESCE(EXCLUDED.source, ga4_session_attribution.source),
        medium = COALESCE(EXCLUDED.medium, ga4_session_attribution.medium),
        campaign_id = COALESCE(EXCLUDED.campaign_id, ga4_session_attribution.campaign_id),
        campaign = COALESCE(EXCLUDED.campaign, ga4_session_attribution.campaign),
        content = COALESCE(EXCLUDED.content, ga4_session_attribution.content),
        term = COALESCE(EXCLUDED.term, ga4_session_attribution.term),
        click_id_type = COALESCE(EXCLUDED.click_id_type, ga4_session_attribution.click_id_type),
        click_id_value = COALESCE(EXCLUDED.click_id_value, ga4_session_attribution.click_id_value),
        account_id = COALESCE(EXCLUDED.account_id, ga4_session_attribution.account_id),
        account_name = COALESCE(EXCLUDED.account_name, ga4_session_attribution.account_name),
        channel_type = COALESCE(EXCLUDED.channel_type, ga4_session_attribution.channel_type),
        channel_subtype = COALESCE(EXCLUDED.channel_subtype, ga4_session_attribution.channel_subtype),
        campaign_metadata_source = CASE
          WHEN EXCLUDED.campaign_id IS NOT NULL OR EXCLUDED.campaign IS NOT NULL
            THEN EXCLUDED.campaign_metadata_source
          ELSE ga4_session_attribution.campaign_metadata_source
        END,
        account_metadata_source = CASE
          WHEN EXCLUDED.account_id IS NOT NULL OR EXCLUDED.account_name IS NOT NULL
            THEN EXCLUDED.account_metadata_source
          ELSE ga4_session_attribution.account_metadata_source
        END,
        channel_metadata_source = CASE
          WHEN EXCLUDED.channel_type IS NOT NULL OR EXCLUDED.channel_subtype IS NOT NULL
            THEN EXCLUDED.channel_metadata_source
          ELSE ga4_session_attribution.channel_metadata_source
        END,
        source_export_hour = GREATEST(ga4_session_attribution.source_export_hour, EXCLUDED.source_export_hour),
        source_dataset = EXCLUDED.source_dataset,
        source_table_type = CASE
          WHEN EXCLUDED.source_export_hour >= ga4_session_attribution.source_export_hour
            THEN EXCLUDED.source_table_type
          ELSE ga4_session_attribution.source_table_type
        END,
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
      row.source_table_type
    ]
  );
}

async function markRunStarted(client: PoolClient, startedAt: Date): Promise<void> {
  await ensureIngestionStateRow(client);
  await client.query(
    `
      UPDATE ga4_bigquery_ingestion_state
      SET
        last_run_started_at = $2,
        last_run_status = 'running',
        last_error = NULL,
        updated_at = $2
      WHERE pipeline_name = $1
    `,
    [GA4_SESSION_ATTRIBUTION_PIPELINE, startedAt]
  );
}

async function markRunCompleted(client: PoolClient, completedAt: Date, watermarkAfter: string | null): Promise<void> {
  await client.query(
    `
      UPDATE ga4_bigquery_ingestion_state
      SET
        watermark_hour = $2::timestamptz,
        last_run_completed_at = $3,
        last_run_status = 'completed',
        last_error = NULL,
        updated_at = $3
      WHERE pipeline_name = $1
    `,
    [GA4_SESSION_ATTRIBUTION_PIPELINE, watermarkAfter, completedAt]
  );
}

export async function markGa4SessionAttributionRunFailed(error: unknown, completedAt = new Date()): Promise<void> {
  await ensureIngestionStateRow(query);
  await query(
    `
      UPDATE ga4_bigquery_ingestion_state
      SET
        last_run_completed_at = $2,
        last_run_status = 'failed',
        last_error = $3,
        updated_at = $2
      WHERE pipeline_name = $1
    `,
    [
      GA4_SESSION_ATTRIBUTION_PIPELINE,
      completedAt,
      error instanceof Error ? error.message : String(error)
    ]
  );
}

export async function ingestGa4SessionAttribution(input: {
  config?: Ga4BigQueryIngestionConfig;
  executor: Ga4BigQueryExecutor;
  now?: Date;
  beforeCommit?: (client: PoolClient) => Promise<void>;
}): Promise<Ga4SessionAttributionIngestionResult> {
  const config = normalizeEnabledConfig(input.config);
  const now = input.now ?? new Date();
  const watermarkBefore = await readWatermarkHour();
  const windows = planGa4SessionAttributionHourlyWindows({
    now,
    watermarkHour: watermarkBefore,
    config
  });

  const hourlyResults: Ga4HourlyExtractionResult[] = [];

  for (const window of windows) {
    hourlyResults.push(
      await extractGa4SessionAttributionForHour({
        config,
        executor: input.executor,
        hourStart: window.hourStart
      })
    );
  }

  const rowsToPersist = hourlyResults.flatMap((result) => result.rows).map(mapNormalizedRowForPersistence);
  const watermarkAfter = windows.length > 0 ? windows[windows.length - 1]?.hourStart ?? null : watermarkBefore?.toISOString() ?? null;

  const upsertedRows = await withTransaction(async (client) => {
    const startedAt = new Date();
    await markRunStarted(client, startedAt);

    for (const row of rowsToPersist) {
      await upsertGa4SessionAttributionRow(client, row);
    }

    if (input.beforeCommit) {
      await input.beforeCommit(client);
    }

    await markRunCompleted(client, new Date(), watermarkAfter);
    return rowsToPersist.length;
  });

  return {
    watermarkBefore: watermarkBefore?.toISOString() ?? null,
    watermarkAfter,
    processedHours: windows.map((window) => window.hourStart).sort(compareIsoAscending),
    extractedRows: hourlyResults.reduce((sum, result) => sum + result.rows.length, 0),
    upsertedRows
  };
}

export async function listGa4SessionAttributionRows(executor: QueryExecutor = query): Promise<Ga4SessionAttributionRow[]> {
  const result = await executeQuery<PersistedGa4SessionAttributionRow>(
    executor,
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
    `
  );

  return result.rows
    .map((row) => ({
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
      sourceTableType: row.source_table_type === 'intraday' ? ('intraday' as const) : ('events' as const)
    }))
    .sort((left, right) => {
      const eventComparison = compareIsoDescending(left.lastEventAt, right.lastEventAt);
      if (eventComparison !== 0) {
        return eventComparison;
      }

      return left.ga4SessionKey.localeCompare(right.ga4SessionKey);
    });
}

export async function getGa4SessionAttributionWatermark(executor: QueryExecutor = query): Promise<string | null> {
  const watermark = await readWatermarkHour(executor);
  return watermark?.toISOString() ?? null;
}
