import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

import { Router } from 'express';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { logError, logInfo, logWarning } from '../../observability/index.js';
import { buildSearchParamsAuditPayload, parseJsonResponsePayload, recordAdSyncApiTransaction } from '../ad-sync-audit/index.js';

const META_GRAPH_API_BASE_URL = 'https://graph.facebook.com/v99.0';
const META_ORDER_VALUE_ACTION_REPORT_TIME = 'conversion';
const META_ORDER_VALUE_USE_ACCOUNT_ATTRIBUTION_SETTING = true;
const META_ORDER_VALUE_REQUEST_FIELDS = [
  'campaign_id',
  'campaign_name',
  'date_start',
  'date_stop',
  'spend',
  'actions',
  'action_values',
  'purchase_roas'
] as const;
const META_ORDER_VALUE_PRIMARY_ACTION_TYPES = [
  'purchase',
  'omni_purchase',
  'offsite_conversion.fb_pixel_purchase'
] as const;
const META_ADS_SYNC_MAX_RETRIES = 3;
const META_ADS_RETRYABLE_STATUS_CODES = new Set([429, 500, 502, 503, 504]);

export type CanonicalSelectionMode = 'priority' | 'fallback' | 'none';

export type MetaAdsCampaignDailyRevenueRecord = {
  attributedRevenue: number | null;
  purchaseCount: number | null;
  actionTypeUsed: string | null;
  canonicalSelectionMode: CanonicalSelectionMode;
};

type MetaAdsActionMetricEntry = {
  action_type?: string;
  value?: string | number | null;
};

type MetaAdsOrderValueApiRow = {
  campaign_id?: string;
  campaign_name?: string | null;
  date_start?: string;
  date_stop?: string | null;
  spend?: string | number | null;
  action_type?: string | null;
  actions?: MetaAdsActionMetricEntry[] | null;
  action_values?: MetaAdsActionMetricEntry[] | null;
  purchase_roas?: MetaAdsActionMetricEntry[] | null;
  [key: string]: unknown;
};

type MetaAdsInsightsApiResponse = {
  data?: MetaAdsOrderValueApiRow[];
  paging?: {
    next?: string;
  };
  [key: string]: unknown;
};

type MetaAdsApiRequestMetricsAccumulator = {
  requestCount: number;
  errorCount: number;
  retryCount: number;
  latencyMsTotal: number;
  latencyMsMax: number;
};

type MetaAdsOrderValueBaselineStats = {
  totalRows: number;
  nullAttributedRevenueCount: number;
  nullPurchaseCountCount: number;
  nullActionTypeCount: number;
};

type MetaAdsOrderValueRecordSummary = MetaAdsOrderValueBaselineStats & {
  fallbackSelectionCount: number;
  prioritySelectionCount: number;
  noSelectionCount: number;
};

type MetaAdsOrderValueSyncAnomaly = {
  type:
    | 'zero_rows_pulled'
    | 'null_attributed_revenue_spike'
    | 'null_purchase_count_spike'
    | 'null_action_type_spike';
  severity: 'warning';
  summary: string;
  details: Record<string, unknown>;
};

type MetaAdsConnectionSyncJobRow = {
  id: number;
  connection_id: number;
  ad_account_id: string;
  account_currency: string | null;
  sync_date: string;
  attempts: number;
};

type MetaAdsConnectionSecretRow = {
  id: number;
  ad_account_id: string;
  access_token: string;
  account_currency: string | null;
};

type MetaAdsQueryable = {
  query<TResult extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[]
  ): Promise<QueryResult<TResult>>;
};

type PersistedMetaAdsRawOrderValueRow = {
  id: number;
  payload: MetaAdsOrderValueApiRow;
};

type NormalizedMetaAdsOrderValueRow = MetaAdsCampaignDailyRevenueRecord & {
  reportDate: string;
  rawDateStart: string;
  rawDateStop: string | null;
  campaignId: string;
  campaignName: string | null;
  currency: string | null;
  spend: number;
  purchaseRoas: number | null;
  rawRecordId: number | null;
  rawRevenueRecordIds: number[];
  rawActionValues: MetaAdsActionMetricEntry[];
  rawActions: MetaAdsActionMetricEntry[];
};

type MetaAdsSyncJobOutcome = 'succeeded' | 'failed';

export type MetaAdsOrderValueSyncResult = {
  succeededConnections: number;
  failedConnections: number;
  recordsReceived: number;
  rawRowsFetched: number;
  rawRowsPersisted: number;
  aggregateRowsUpserted: number;
  apiRequestCount: number;
  anomalyCount: number;
};

export type MetaAdsQueueProcessOptions = {
  workerId?: string;
  limit?: number;
  emitMetrics?: boolean;
  now?: Date;
  triggerSource?: string;
  planJobs?: boolean;
};

export type MetaAdsQueueProcessResult = {
  workerId: string;
  enqueuedJobs: number;
  claimedJobs: number;
  succeededJobs: number;
  failedJobs: number;
  durationMs: number;
};

class MetaAdsApiError extends Error {
  statusCode: number;
  details: unknown;

  constructor(statusCode: number, message: string, details: unknown) {
    super(message);
    this.name = 'MetaAdsApiError';
    this.statusCode = statusCode;
    this.details = details;
  }
}

function safeRatio(numerator: number, denominator: number): number | null {
  if (denominator <= 0) {
    return null;
  }

  return Number((numerator / denominator).toFixed(4));
}

function buildMetaAdsApiRequestMetricsAccumulator(): MetaAdsApiRequestMetricsAccumulator {
  return {
    requestCount: 0,
    errorCount: 0,
    retryCount: 0,
    latencyMsTotal: 0,
    latencyMsMax: 0
  };
}

function summarizeOrderValueRecords(records: MetaAdsCampaignDailyRevenueRecord[]): MetaAdsOrderValueRecordSummary {
  return records.reduce<MetaAdsOrderValueRecordSummary>(
    (summary, record) => {
      summary.totalRows += 1;

      if (record.attributedRevenue === null) {
        summary.nullAttributedRevenueCount += 1;
      }

      if (record.purchaseCount === null) {
        summary.nullPurchaseCountCount += 1;
      }

      if (record.actionTypeUsed === null) {
        summary.nullActionTypeCount += 1;
      }

      if (record.canonicalSelectionMode === 'fallback') {
        summary.fallbackSelectionCount += 1;
      } else if (record.canonicalSelectionMode === 'priority') {
        summary.prioritySelectionCount += 1;
      } else {
        summary.noSelectionCount += 1;
      }

      return summary;
    },
    {
      totalRows: 0,
      nullAttributedRevenueCount: 0,
      nullPurchaseCountCount: 0,
      nullActionTypeCount: 0,
      fallbackSelectionCount: 0,
      prioritySelectionCount: 0,
      noSelectionCount: 0
    }
  );
}

function buildOrderValueSyncAnomalies(input: {
  rawRowsFetched: number;
  records: MetaAdsCampaignDailyRevenueRecord[];
  summary: MetaAdsOrderValueRecordSummary;
  baseline: MetaAdsOrderValueBaselineStats;
}): MetaAdsOrderValueSyncAnomaly[] {
  const anomalies: MetaAdsOrderValueSyncAnomaly[] = [];

  if (input.rawRowsFetched === 0 || input.summary.totalRows === 0) {
    anomalies.push({
      type: 'zero_rows_pulled',
      severity: 'warning',
      summary: 'Meta order value sync completed with zero upstream rows or zero normalized records.',
      details: {
        rawRowsFetched: input.rawRowsFetched,
        normalizedRecords: input.summary.totalRows
      }
    });
  }

  if (input.summary.totalRows < env.META_ADS_ORDER_VALUE_ANOMALY_MIN_ROWS) {
    return anomalies;
  }

  const currentChecks = [
    {
      type: 'null_attributed_revenue_spike' as const,
      currentCount: input.summary.nullAttributedRevenueCount,
      baselineCount: input.baseline.nullAttributedRevenueCount,
      detailKey: 'nullAttributedRevenueRate',
      summary: 'Meta order value sync detected a spike in null attributed revenue rows.'
    },
    {
      type: 'null_purchase_count_spike' as const,
      currentCount: input.summary.nullPurchaseCountCount,
      baselineCount: input.baseline.nullPurchaseCountCount,
      detailKey: 'nullPurchaseCountRate',
      summary: 'Meta order value sync detected a spike in null purchase count rows.'
    },
    {
      type: 'null_action_type_spike' as const,
      currentCount: input.summary.nullActionTypeCount,
      baselineCount: input.baseline.nullActionTypeCount,
      detailKey: 'nullActionTypeRate',
      summary: 'Meta order value sync detected a spike in rows without a canonical action type.'
    }
  ];

  for (const check of currentChecks) {
    const currentRate = safeRatio(check.currentCount, input.summary.totalRows) ?? 0;
    const baselineRate = safeRatio(check.baselineCount, input.baseline.totalRows) ?? 0;

    if (
      currentRate >= env.META_ADS_ORDER_VALUE_NULL_SPIKE_MIN_RATIO &&
      currentRate - baselineRate >= env.META_ADS_ORDER_VALUE_NULL_SPIKE_RATIO_DELTA
    ) {
      anomalies.push({
        type: check.type,
        severity: 'warning',
        summary: check.summary,
        details: {
          rawRowsFetched: input.rawRowsFetched,
          totalRows: input.summary.totalRows,
          baselineTotalRows: input.baseline.totalRows,
          [check.detailKey]: currentRate,
          baselineRate,
          ratioDelta: Number((currentRate - baselineRate).toFixed(4))
        }
      });
    }
  }

  return anomalies;
}

function buildSyncWindowDates(now: Date): string[] {
  const dates: string[] = [];
  const totalDays = Math.max(1, env.META_ADS_ORDER_VALUE_WINDOW_DAYS);
  const end = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());

  for (let offset = totalDays - 1; offset >= 0; offset -= 1) {
    dates.push(new Date(end - offset * 24 * 60 * 60 * 1000).toISOString().slice(0, 10));
  }

  return dates;
}

function normalizeAdAccountId(adAccountId: string): string {
  return adAccountId.replace(/^act_/, '');
}

function buildMetaInsightsUrl(adAccountId: string, syncDate: string): URL {
  const url = new URL(`${META_GRAPH_API_BASE_URL}/act_${normalizeAdAccountId(adAccountId)}/insights`);
  url.searchParams.set('level', 'campaign');
  url.searchParams.set('time_increment', '1');
  url.searchParams.set('fields', META_ORDER_VALUE_REQUEST_FIELDS.join(','));
  url.searchParams.set('action_breakdowns', 'action_type');
  url.searchParams.set('action_report_time', META_ORDER_VALUE_ACTION_REPORT_TIME);
  url.searchParams.set(
    'use_account_attribution_setting',
    META_ORDER_VALUE_USE_ACCOUNT_ATTRIBUTION_SETTING ? 'true' : 'false'
  );
  url.searchParams.set('time_range', JSON.stringify({ since: syncDate, until: syncDate }));
  return url;
}

function parseMetricNumber(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();

  if (!trimmed) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseMetricInteger(value: unknown): number | null {
  const parsed = parseMetricNumber(value);

  if (parsed === null) {
    return null;
  }

  return Math.trunc(parsed);
}

function isAllowedPurchaseLikeActionType(actionType: string | null | undefined): actionType is string {
  if (typeof actionType !== 'string') {
    return false;
  }

  const normalized = actionType.trim();
  return normalized.length > 0 && normalized.toLowerCase().includes('purchase');
}

function collectEncounteredPurchaseActionTypes(rows: MetaAdsOrderValueApiRow[]): string[] {
  const encountered: string[] = [];
  const seen = new Set<string>();

  const visit = (actionType: string | null | undefined) => {
    if (!isAllowedPurchaseLikeActionType(actionType) || seen.has(actionType)) {
      return;
    }

    seen.add(actionType);
    encountered.push(actionType);
  };

  for (const row of rows) {
    visit(row.action_type);

    for (const entry of row.actions ?? []) {
      visit(entry?.action_type);
    }

    for (const entry of row.action_values ?? []) {
      visit(entry?.action_type);
    }

    for (const entry of row.purchase_roas ?? []) {
      visit(entry?.action_type);
    }
  }

  return encountered;
}

function selectCanonicalActionType(rows: MetaAdsOrderValueApiRow[]): {
  actionTypeUsed: string | null;
  canonicalSelectionMode: CanonicalSelectionMode;
} {
  const encountered = collectEncounteredPurchaseActionTypes(rows);
  const encounteredSet = new Set(encountered);

  for (const actionType of META_ORDER_VALUE_PRIMARY_ACTION_TYPES) {
    if (encounteredSet.has(actionType)) {
      return {
        actionTypeUsed: actionType,
        canonicalSelectionMode: 'priority'
      };
    }
  }

  if (encountered.length > 0) {
    return {
      actionTypeUsed: encountered[0] ?? null,
      canonicalSelectionMode: 'fallback'
    };
  }

  return {
    actionTypeUsed: null,
    canonicalSelectionMode: 'none'
  };
}

function sumMetricEntries(rows: MetaAdsOrderValueApiRow[], field: 'action_values' | 'actions', actionType: string): number | null {
  let found = false;
  let total = 0;

  for (const row of rows) {
    for (const entry of row[field] ?? []) {
      if (entry?.action_type !== actionType) {
        continue;
      }

      const parsed = field === 'actions' ? parseMetricInteger(entry.value) : parseMetricNumber(entry.value);

      if (parsed === null) {
        continue;
      }

      found = true;
      total += parsed;
    }
  }

  if (!found) {
    return null;
  }

  if (field === 'actions') {
    return Math.trunc(total);
  }

  return Number(total.toFixed(2));
}

function firstMetricEntryValue(
  rows: MetaAdsOrderValueApiRow[],
  field: 'purchase_roas',
  actionType: string
): number | null {
  for (const row of rows) {
    for (const entry of row[field] ?? []) {
      if (entry?.action_type !== actionType) {
        continue;
      }

      const parsed = parseMetricNumber(entry.value);

      if (parsed !== null) {
        return parsed;
      }
    }
  }

  return null;
}

function hasSelectedActionType(row: MetaAdsOrderValueApiRow, actionType: string): boolean {
  if (row.action_type === actionType) {
    return true;
  }

  return [row.action_values ?? [], row.actions ?? [], row.purchase_roas ?? []].some((entries) =>
    entries.some((entry) => entry?.action_type === actionType)
  );
}

function groupRowsByCampaignDate(rows: PersistedMetaAdsRawOrderValueRow[]): Map<string, PersistedMetaAdsRawOrderValueRow[]> {
  const grouped = new Map<string, PersistedMetaAdsRawOrderValueRow[]>();

  for (const row of rows) {
    const reportDate = typeof row.payload.date_start === 'string' ? row.payload.date_start : '';
    const campaignId = typeof row.payload.campaign_id === 'string' ? row.payload.campaign_id : '';

    if (!reportDate || !campaignId) {
      continue;
    }

    const key = `${reportDate}::${campaignId}`;
    const existing = grouped.get(key);

    if (existing) {
      existing.push(row);
    } else {
      grouped.set(key, [row]);
    }
  }

  return grouped;
}

function normalizeOrderValueRows(params: {
  persistedRows: PersistedMetaAdsRawOrderValueRow[];
  currency: string | null;
}): NormalizedMetaAdsOrderValueRow[] {
  const normalizedRows: NormalizedMetaAdsOrderValueRow[] = [];
  const grouped = groupRowsByCampaignDate(params.persistedRows);

  for (const rows of grouped.values()) {
    const payloads = rows.map((row) => row.payload);
    const first = payloads[0];
    const campaignId = typeof first?.campaign_id === 'string' ? first.campaign_id : null;
    const reportDate = typeof first?.date_start === 'string' ? first.date_start : null;

    if (!campaignId || !reportDate) {
      continue;
    }

    const selection = selectCanonicalActionType(payloads);
    const selectedRow = selection.actionTypeUsed
      ? rows.find((row) => hasSelectedActionType(row.payload, selection.actionTypeUsed!)) ?? rows[0]
      : rows[0];
    const attributedRevenue =
      selection.actionTypeUsed === null ? null : sumMetricEntries(payloads, 'action_values', selection.actionTypeUsed);
    const purchaseCount =
      selection.actionTypeUsed === null ? null : sumMetricEntries(payloads, 'actions', selection.actionTypeUsed);
    const purchaseRoas =
      selection.actionTypeUsed === null ? null : firstMetricEntryValue(payloads, 'purchase_roas', selection.actionTypeUsed);

    normalizedRows.push({
      reportDate,
      rawDateStart: reportDate,
      rawDateStop: typeof first.date_stop === 'string' ? first.date_stop : null,
      campaignId,
      campaignName: typeof first.campaign_name === 'string' ? first.campaign_name : null,
      currency: params.currency,
      spend: parseMetricNumber(first.spend) ?? 0,
      attributedRevenue,
      purchaseCount,
      purchaseRoas,
      actionTypeUsed: selection.actionTypeUsed,
      canonicalSelectionMode: selection.canonicalSelectionMode,
      rawRecordId: selectedRow?.id ?? null,
      rawRevenueRecordIds: rows.map((row) => row.id),
      rawActionValues: payloads.flatMap((row) => row.action_values ?? []),
      rawActions: payloads.flatMap((row) => row.actions ?? [])
    });
  }

  return normalizedRows;
}

async function loadOrderValueBaseline(connectionId: number, beforeDate: string): Promise<MetaAdsOrderValueBaselineStats> {
  const result = await query<{
    total_rows: string;
    null_attributed_revenue_count: string;
    null_purchase_count_count: string;
    null_action_type_count: string;
  }>(
    `
      SELECT
        COUNT(*)::text AS total_rows,
        COUNT(*) FILTER (WHERE attributed_revenue IS NULL)::text AS null_attributed_revenue_count,
        COUNT(*) FILTER (WHERE purchase_count IS NULL)::text AS null_purchase_count_count,
        COUNT(*) FILTER (WHERE canonical_action_type IS NULL)::text AS null_action_type_count
      FROM meta_ads_order_value_aggregates
      WHERE meta_connection_id = $1
        AND report_date < $2::date
        AND action_report_time = $3
        AND use_account_attribution_setting = $4
    `,
    [connectionId, beforeDate, META_ORDER_VALUE_ACTION_REPORT_TIME, META_ORDER_VALUE_USE_ACCOUNT_ATTRIBUTION_SETTING]
  );

  return {
    totalRows: Number(result.rows[0]?.total_rows ?? '0'),
    nullAttributedRevenueCount: Number(result.rows[0]?.null_attributed_revenue_count ?? '0'),
    nullPurchaseCountCount: Number(result.rows[0]?.null_purchase_count_count ?? '0'),
    nullActionTypeCount: Number(result.rows[0]?.null_action_type_count ?? '0')
  };
}

async function createSyncRun(params: {
  connectionId: number;
  triggerSource: string;
  syncDate: string;
  client?: MetaAdsQueryable;
}): Promise<number> {
  const executor = params.client ?? poolQueryExecutor;
  const result = await executor.query<{ id: number }>(
    `
      INSERT INTO meta_ads_order_value_sync_runs (
        connection_id,
        trigger_source,
        status,
        window_start_date,
        window_end_date,
        started_at,
        updated_at
      )
      VALUES ($1, $2, 'running', $3::date, $3::date, now(), now())
      RETURNING id
    `,
    [params.connectionId, params.triggerSource, params.syncDate]
  );

  return result.rows[0].id;
}

function serializeErrorDetails(error: unknown): Record<string, unknown> {
  if (error instanceof MetaAdsApiError) {
    return {
      message: error.message,
      statusCode: error.statusCode,
      details: error.details
    };
  }

  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name
    };
  }

  return {
    message: String(error)
  };
}

async function markSyncRunCompleted(params: {
  runId: number;
  recordsReceived: number;
  rawRowsPersisted: number;
  aggregateRowsUpserted: number;
  client?: MetaAdsQueryable;
}): Promise<void> {
  const executor = params.client ?? poolQueryExecutor;
  await executor.query(
    `
      UPDATE meta_ads_order_value_sync_runs
      SET
        status = 'completed',
        completed_at = now(),
        records_received = $2,
        raw_rows_persisted = $3,
        aggregate_rows_upserted = $4,
        error_count = 0,
        error_details = '[]'::jsonb,
        updated_at = now()
      WHERE id = $1
    `,
    [params.runId, params.recordsReceived, params.rawRowsPersisted, params.aggregateRowsUpserted]
  );
}

async function markSyncRunFailed(runId: number, error: unknown): Promise<void> {
  await markSyncRunFailedWithClient(runId, error, poolQueryExecutor);
}

async function enqueueSyncJobsForWindow(now: Date): Promise<number> {
  const connections = await query<{ id: number }>(
    `
      SELECT id
      FROM meta_ads_connections
      WHERE status = 'active'
      ORDER BY id ASC
    `
  );
  const syncDates = buildSyncWindowDates(now);
  let enqueuedJobs = 0;

  for (const connection of connections.rows) {
    for (const syncDate of syncDates) {
      await query(
        `
          INSERT INTO meta_ads_sync_jobs (
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
              WHEN meta_ads_sync_jobs.status = 'processing' THEN meta_ads_sync_jobs.status
              ELSE 'pending'
            END,
            available_at = CASE
              WHEN meta_ads_sync_jobs.status = 'processing' THEN meta_ads_sync_jobs.available_at
              ELSE now()
            END,
            locked_at = CASE
              WHEN meta_ads_sync_jobs.status = 'processing' THEN meta_ads_sync_jobs.locked_at
              ELSE NULL
            END,
            locked_by = CASE
              WHEN meta_ads_sync_jobs.status = 'processing' THEN meta_ads_sync_jobs.locked_by
              ELSE NULL
            END,
            last_error = CASE
              WHEN meta_ads_sync_jobs.status = 'processing' THEN meta_ads_sync_jobs.last_error
              ELSE NULL
            END,
            completed_at = CASE
              WHEN meta_ads_sync_jobs.status = 'processing' THEN meta_ads_sync_jobs.completed_at
              ELSE NULL
            END,
            updated_at = now()
        `,
        [connection.id, syncDate]
      );
      enqueuedJobs += 1;
    }
  }

  return enqueuedJobs;
}

async function claimSyncJobs(workerId: string, limit: number): Promise<MetaAdsConnectionSyncJobRow[]> {
  const result = await query<MetaAdsConnectionSyncJobRow>(
    `
      WITH claimable AS (
        SELECT j.id, j.connection_id
        FROM meta_ads_sync_jobs j
        JOIN meta_ads_connections c ON c.id = j.connection_id
        WHERE j.status IN ('pending', 'retry')
          AND j.available_at <= now()
          AND c.status = 'active'
        ORDER BY j.sync_date ASC, j.id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE meta_ads_sync_jobs j
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
        c.account_currency,
        j.sync_date::text,
        j.attempts
    `,
    [workerId, limit]
  );

  return result.rows;
}

async function getConnectionSecret(connectionId: number): Promise<MetaAdsConnectionSecretRow> {
  const result = await query<MetaAdsConnectionSecretRow>(
    `
      SELECT
        id,
        ad_account_id,
        pgp_sym_decrypt(access_token_encrypted, $2)::text AS access_token,
        account_currency
      FROM meta_ads_connections
      WHERE id = $1
    `,
    [connectionId, env.META_ADS_ENCRYPTION_KEY]
  );

  const row = result.rows[0];

  if (!row?.access_token) {
    throw new Error(`Meta Ads connection ${connectionId} is missing a decryptable access token`);
  }

  return row;
}

async function updateConnectionSyncStarted(connectionId: number): Promise<void> {
  await poolQueryExecutor.query(
    `
      UPDATE meta_ads_connections
      SET
        last_sync_started_at = now(),
        last_sync_status = 'running',
        last_sync_error = NULL,
        updated_at = now()
      WHERE id = $1
    `,
    [connectionId]
  );
}

async function markSyncJobSucceeded(jobId: number, connectionId: number, client?: MetaAdsQueryable): Promise<void> {
  const executor = client ?? poolQueryExecutor;

  await executor.query(
    `
      UPDATE meta_ads_sync_jobs
      SET
        status = 'completed',
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        updated_at = now()
      WHERE id = $1
    `,
    [jobId]
  );

  await executor.query(
    `
      UPDATE meta_ads_connections
      SET
        last_sync_completed_at = now(),
        last_sync_status = 'succeeded',
        last_sync_error = NULL,
        updated_at = now()
      WHERE id = $1
    `,
    [connectionId]
  );
}

async function markSyncJobFailed(
  job: MetaAdsConnectionSyncJobRow,
  error: unknown,
  client?: MetaAdsQueryable
): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  const retryDelaySeconds = Math.min(300, Math.max(15, job.attempts * 30));
  const shouldRetry =
    (error instanceof MetaAdsApiError && META_ADS_RETRYABLE_STATUS_CODES.has(error.statusCode)) ||
    job.attempts < META_ADS_SYNC_MAX_RETRIES;
  const nextStatus = shouldRetry ? 'retry' : 'failed';

  const executor = client ?? poolQueryExecutor;

  await executor.query(
    `
      UPDATE meta_ads_sync_jobs
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
    [job.id, nextStatus, retryDelaySeconds, message]
  );

  await executor.query(
    `
      UPDATE meta_ads_connections
      SET
        last_sync_status = $2,
        last_sync_error = $3,
        updated_at = now()
      WHERE id = $1
    `,
    [job.connection_id, shouldRetry ? 'retry' : 'failed', message]
  );
}

async function performMetaApiRequest(params: {
  job: MetaAdsConnectionSyncJobRow;
  connection: MetaAdsConnectionSecretRow;
  url: URL;
  apiMetrics: MetaAdsApiRequestMetricsAccumulator;
  triggerSource: string;
}): Promise<MetaAdsInsightsApiResponse> {
  const requestStartedAt = new Date();
  const response = await fetch(params.url, {
    method: 'GET',
    headers: {
      authorization: `Bearer ${params.connection.access_token}`
    }
  });
  const responseReceivedAt = new Date();
  const bodyText = await response.text();
  const payload = parseJsonResponsePayload(bodyText);
  const latencyMs = responseReceivedAt.getTime() - requestStartedAt.getTime();

  params.apiMetrics.requestCount += 1;
  params.apiMetrics.latencyMsTotal += latencyMs;
  params.apiMetrics.latencyMsMax = Math.max(params.apiMetrics.latencyMsMax, latencyMs);

  await recordAdSyncApiTransaction({
    platform: 'meta_ads',
    connectionId: params.job.connection_id,
    syncJobId: params.job.id,
    transactionSource: 'meta_ads_order_value_insights',
    sourceMetadata: {
      triggerSource: params.triggerSource,
      syncDate: params.job.sync_date,
      actionReportTime: META_ORDER_VALUE_ACTION_REPORT_TIME
    },
    requestMethod: 'GET',
    requestUrl: params.url.toString(),
    requestPayload: buildSearchParamsAuditPayload(params.url.searchParams),
    requestStartedAt,
    responseStatus: response.status,
    responsePayload: payload,
    responseReceivedAt
  });

  if (!response.ok) {
    params.apiMetrics.errorCount += 1;
    logError('meta_ads_api_request_failed', new MetaAdsApiError(response.status, 'Meta Ads API request failed', payload), {
      service: process.env.K_SERVICE ?? 'roas-radar',
      connectionId: params.job.connection_id,
      syncJobId: params.job.id,
      adAccountId: params.connection.ad_account_id,
      syncDate: params.job.sync_date,
      triggerSource: params.triggerSource,
      requestUrl: params.url.toString(),
      responseStatus: response.status
    });
    throw new MetaAdsApiError(response.status, `Meta Ads API request failed with status ${response.status}`, payload);
  }

  const parsed = payload as MetaAdsInsightsApiResponse;

  logInfo('meta_ads_api_request_completed', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    connectionId: params.job.connection_id,
    syncJobId: params.job.id,
    adAccountId: params.connection.ad_account_id,
    syncDate: params.job.sync_date,
    triggerSource: params.triggerSource,
    requestUrl: params.url.toString(),
    requestNumber: params.apiMetrics.requestCount,
    rowCount: Array.isArray(parsed.data) ? parsed.data.length : 0,
    latencyMs,
    hasNextPage: typeof parsed.paging?.next === 'string'
  });

  return parsed;
}

async function fetchAllOrderValueRows(params: {
  job: MetaAdsConnectionSyncJobRow;
  connection: MetaAdsConnectionSecretRow;
  apiMetrics: MetaAdsApiRequestMetricsAccumulator;
  triggerSource: string;
}): Promise<MetaAdsOrderValueApiRow[]> {
  const rows: MetaAdsOrderValueApiRow[] = [];
  let nextUrl: URL | null = buildMetaInsightsUrl(params.connection.ad_account_id, params.job.sync_date);

  while (nextUrl) {
    const requestUrl = nextUrl;

    try {
      const payload = await performMetaApiRequest({
        job: params.job,
        connection: params.connection,
        url: requestUrl,
        apiMetrics: params.apiMetrics,
        triggerSource: params.triggerSource
      });

      rows.push(...(Array.isArray(payload.data) ? payload.data : []));
      nextUrl = typeof payload.paging?.next === 'string' ? new URL(payload.paging.next) : null;
    } catch (error) {
      if (!(error instanceof MetaAdsApiError) || !META_ADS_RETRYABLE_STATUS_CODES.has(error.statusCode)) {
        throw error;
      }

      params.apiMetrics.retryCount += 1;
      await delay(500);

      const payload = await performMetaApiRequest({
        job: params.job,
        connection: params.connection,
        url: requestUrl,
        apiMetrics: params.apiMetrics,
        triggerSource: params.triggerSource
      });

      rows.push(...(Array.isArray(payload.data) ? payload.data : []));
      nextUrl = typeof payload.paging?.next === 'string' ? new URL(payload.paging.next) : null;
    }
  }

  return rows;
}

async function persistRawOrderValueRows(
  client: PoolClient,
  params: {
    connectionId: number;
    syncRunId: number;
    syncJobId: number;
    rows: MetaAdsOrderValueApiRow[];
  }
): Promise<PersistedMetaAdsRawOrderValueRow[]> {
  const persistedRows: PersistedMetaAdsRawOrderValueRow[] = [];

  for (const row of params.rows) {
    const campaignId = typeof row.campaign_id === 'string' ? row.campaign_id : null;
    const reportDate = typeof row.date_start === 'string' ? row.date_start : null;

    if (!campaignId || !reportDate) {
      continue;
    }

    const insert = await client.query<{ id: number }>(
      `
        INSERT INTO meta_ads_order_value_raw_records (
          connection_id,
          sync_run_id,
          sync_job_id,
          report_date,
          campaign_id,
          campaign_name,
          action_type,
          raw_payload
        )
        VALUES ($1, $2, $3, $4::date, $5, $6, $7, $8::jsonb)
        RETURNING id
      `,
      [
        params.connectionId,
        params.syncRunId,
        params.syncJobId,
        reportDate,
        campaignId,
        typeof row.campaign_name === 'string' ? row.campaign_name : null,
        typeof row.action_type === 'string' ? row.action_type : null,
        JSON.stringify(row)
      ]
    );

    persistedRows.push({
      id: insert.rows[0].id,
      payload: row
    });
  }

  return persistedRows;
}

async function replaceAggregateRows(
  client: PoolClient,
  params: {
    connectionId: number;
    adAccountId: string;
    syncJobId: number;
    syncDate: string;
    normalizedRows: NormalizedMetaAdsOrderValueRow[];
    sourceSyncedAt: Date;
  }
): Promise<void> {
  await client.query(
    `
      DELETE FROM meta_ads_order_value_aggregates
      WHERE organization_id = $1
        AND meta_connection_id = $2
        AND report_date = $3::date
        AND action_report_time = $4
        AND use_account_attribution_setting = $5
    `,
    [
      env.DEFAULT_ORGANIZATION_ID,
      params.connectionId,
      params.syncDate,
      META_ORDER_VALUE_ACTION_REPORT_TIME,
      META_ORDER_VALUE_USE_ACCOUNT_ATTRIBUTION_SETTING
    ]
  );

  for (const row of params.normalizedRows) {
    await client.query(
      `
        INSERT INTO meta_ads_order_value_aggregates (
          organization_id,
          meta_connection_id,
          sync_job_id,
          raw_record_id,
          ad_account_id,
          report_date,
          raw_date_start,
          raw_date_stop,
          campaign_id,
          campaign_name,
          attributed_revenue,
          purchase_count,
          spend,
          purchase_roas,
          currency,
          canonical_action_type,
          canonical_selection_mode,
          raw_action_values,
          raw_actions,
          raw_revenue_record_ids,
          source_synced_at,
          action_report_time,
          use_account_attribution_setting,
          updated_at
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          $5,
          $6::date,
          $7::date,
          $8::date,
          $9,
          $10,
          $11,
          $12,
          $13,
          $14,
          $15,
          $16,
          $17,
          $18::jsonb,
          $19::jsonb,
          $20::jsonb,
          $21,
          $22,
          $23,
          now()
        )
      `,
      [
        env.DEFAULT_ORGANIZATION_ID,
        params.connectionId,
        params.syncJobId,
        null,
        normalizeAdAccountId(params.adAccountId),
        row.reportDate,
        row.rawDateStart,
        row.rawDateStop,
        row.campaignId,
        row.campaignName,
        row.attributedRevenue,
        row.purchaseCount,
        row.spend,
        row.purchaseRoas,
        row.currency,
        row.actionTypeUsed,
        row.canonicalSelectionMode,
        JSON.stringify(row.rawActionValues),
        JSON.stringify(row.rawActions),
        JSON.stringify(row.rawRevenueRecordIds),
        params.sourceSyncedAt,
        META_ORDER_VALUE_ACTION_REPORT_TIME,
        META_ORDER_VALUE_USE_ACCOUNT_ATTRIBUTION_SETTING
      ]
    );
  }
}

function emitOrderValueSyncAnomalies(params: {
  runId: number;
  connectionId: number;
  adAccountId: string;
  triggerSource: string;
  windowStartDate: string;
  windowEndDate: string;
  anomalies: MetaAdsOrderValueSyncAnomaly[];
}): void {
  for (const anomaly of params.anomalies) {
    logWarning('meta_ads_order_value_sync_anomaly', {
      service: process.env.K_SERVICE ?? 'roas-radar',
      runId: params.runId,
      connectionId: params.connectionId,
      adAccountId: params.adAccountId,
      triggerSource: params.triggerSource,
      windowStartDate: params.windowStartDate,
      windowEndDate: params.windowEndDate,
      anomalyType: anomaly.type,
      severity: anomaly.severity,
      summary: anomaly.summary,
      details: anomaly.details,
      alertable: true
    });
  }
}

async function processSyncJob(job: MetaAdsConnectionSyncJobRow, triggerSource: string): Promise<{
  outcome: MetaAdsSyncJobOutcome;
  recordsReceived: number;
  rawRowsFetched: number;
  rawRowsPersisted: number;
  aggregateRowsUpserted: number;
  apiRequestCount: number;
  anomalyCount: number;
}> {
  await updateConnectionSyncStarted(job.connection_id);
  const connection = await getConnectionSecret(job.connection_id);
  const apiMetrics = buildMetaAdsApiRequestMetricsAccumulator();
  const sourceSyncedAt = new Date();
  let runId: number | null = null;

  try {
    const rawRows = await fetchAllOrderValueRows({
      job,
      connection,
      apiMetrics,
      triggerSource
    });
    const baseline = await loadOrderValueBaseline(job.connection_id, job.sync_date);

    const { persistedRows, normalizedRows, persistedRunId } = await withTransaction(async (client) => {
      const createdRunId = await createSyncRun({
        connectionId: job.connection_id,
        triggerSource,
        syncDate: job.sync_date,
        client
      });

      const persisted = await persistRawOrderValueRows(client, {
        connectionId: job.connection_id,
        syncRunId: createdRunId,
        syncJobId: job.id,
        rows: rawRows
      });
      const normalized = normalizeOrderValueRows({
        persistedRows: persisted,
        currency: connection.account_currency
      });

      await replaceAggregateRows(client, {
        connectionId: job.connection_id,
        adAccountId: connection.ad_account_id,
        syncJobId: job.id,
        syncDate: job.sync_date,
        normalizedRows: normalized,
        sourceSyncedAt
      });

      await markSyncRunCompleted({
        runId: createdRunId,
        recordsReceived: normalized.length,
        rawRowsPersisted: persisted.length,
        aggregateRowsUpserted: normalized.length,
        client
      });
      await markSyncJobSucceeded(job.id, job.connection_id, client);

      return {
        persistedRows: persisted,
        normalizedRows: normalized,
        persistedRunId: createdRunId
      };
    });
    runId = persistedRunId;

    const summary = summarizeOrderValueRecords(normalizedRows);
    const anomalies = buildOrderValueSyncAnomalies({
      rawRowsFetched: rawRows.length,
      records: normalizedRows,
      summary,
      baseline
    });

    logInfo('meta_ads_order_value_sync_connection_completed', {
      service: process.env.K_SERVICE ?? 'roas-radar',
      runId,
      jobId: job.id,
      connectionId: job.connection_id,
      adAccountId: connection.ad_account_id,
      triggerSource,
      windowStartDate: job.sync_date,
      windowEndDate: job.sync_date,
      rawRowsFetched: rawRows.length,
      normalizedRecordsReceived: summary.totalRows,
      rawRowsPersisted: persistedRows.length,
      aggregateRowsUpserted: normalizedRows.length,
      apiRequestCount: apiMetrics.requestCount,
      apiRequestErrorCount: apiMetrics.errorCount,
      apiRequestRetryCount: apiMetrics.retryCount,
      apiLatencyMsTotal: apiMetrics.latencyMsTotal,
      apiLatencyMsMax: apiMetrics.latencyMsMax,
      apiLatencyMsAvg:
        apiMetrics.requestCount > 0 ? Number((apiMetrics.latencyMsTotal / apiMetrics.requestCount).toFixed(2)) : 0,
      nullAttributedRevenueCount: summary.nullAttributedRevenueCount,
      nullAttributedRevenueRate: safeRatio(summary.nullAttributedRevenueCount, summary.totalRows),
      nullPurchaseCountCount: summary.nullPurchaseCountCount,
      nullPurchaseCountRate: safeRatio(summary.nullPurchaseCountCount, summary.totalRows),
      nullActionTypeCount: summary.nullActionTypeCount,
      nullActionTypeRate: safeRatio(summary.nullActionTypeCount, summary.totalRows),
      anomalyCount: anomalies.length,
      anomalyTypes: anomalies.map((anomaly) => anomaly.type),
      zeroRowsPulled: rawRows.length === 0 || summary.totalRows === 0
    });

    emitOrderValueSyncAnomalies({
      runId,
      connectionId: job.connection_id,
      adAccountId: connection.ad_account_id,
      triggerSource,
      windowStartDate: job.sync_date,
      windowEndDate: job.sync_date,
      anomalies
    });

    return {
      outcome: 'succeeded',
      recordsReceived: summary.totalRows,
      rawRowsFetched: rawRows.length,
      rawRowsPersisted: persistedRows.length,
      aggregateRowsUpserted: normalizedRows.length,
      apiRequestCount: apiMetrics.requestCount,
      anomalyCount: anomalies.length
    };
  } catch (error) {
    runId = await withTransaction(async (client) => {
      const failedRunId =
        runId ??
        (await createSyncRun({
          connectionId: job.connection_id,
          triggerSource,
          syncDate: job.sync_date,
          client
        }));

      await markSyncRunFailedWithClient(failedRunId, error, client);
      await markSyncJobFailed(job, error, client);

      return failedRunId;
    });

    logError('meta_ads_order_value_sync_connection_failed', error, {
      service: process.env.K_SERVICE ?? 'roas-radar',
      runId,
      jobId: job.id,
      connectionId: job.connection_id,
      adAccountId: connection.ad_account_id,
      triggerSource,
      windowStartDate: job.sync_date,
      windowEndDate: job.sync_date,
      attempts: job.attempts
    });

    return {
      outcome: 'failed',
      recordsReceived: 0,
      rawRowsFetched: 0,
      rawRowsPersisted: 0,
      aggregateRowsUpserted: 0,
      apiRequestCount: apiMetrics.requestCount,
      anomalyCount: 0
    };
  }
}

const poolQueryExecutor: MetaAdsQueryable = {
  query<TResult extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[]
  ): Promise<QueryResult<TResult>> {
    return query<TResult>(text, params);
  }
};

async function markSyncRunFailedWithClient(runId: number, error: unknown, client: MetaAdsQueryable): Promise<void> {
  await client.query(
    `
      UPDATE meta_ads_order_value_sync_runs
      SET
        status = 'failed',
        completed_at = now(),
        error_count = 1,
        error_details = $2::jsonb,
        updated_at = now()
      WHERE id = $1
    `,
    [runId, JSON.stringify([serializeErrorDetails(error)])]
  );
}

function buildQueueMetricsLog(result: MetaAdsQueueProcessResult): void {
  logInfo('meta_ads_sync_run', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    workerId: result.workerId,
    enqueuedJobs: result.enqueuedJobs,
    claimedJobs: result.claimedJobs,
    succeededJobs: result.succeededJobs,
    failedJobs: result.failedJobs,
    durationMs: result.durationMs
  });
}

export async function processMetaAdsSyncQueue(options: MetaAdsQueueProcessOptions = {}): Promise<MetaAdsQueueProcessResult> {
  const startedAt = Date.now();
  const workerId = options.workerId ?? `meta-ads-sync-${randomBytes(6).toString('hex')}`;
  const now = options.now ?? new Date();
  const limit = options.limit ?? env.META_ADS_SYNC_BATCH_SIZE;
  const triggerSource = options.triggerSource ?? 'worker';
  const enqueuedJobs = options.planJobs ? await enqueueSyncJobsForWindow(now) : 0;
  const jobs = await claimSyncJobs(workerId, limit);
  let succeededJobs = 0;
  let failedJobs = 0;

  for (const job of jobs) {
    const result = await processSyncJob(job, triggerSource);

    if (result.outcome === 'succeeded') {
      succeededJobs += 1;
    } else {
      failedJobs += 1;
    }
  }

  const result: MetaAdsQueueProcessResult = {
    workerId,
    enqueuedJobs,
    claimedJobs: jobs.length,
    succeededJobs,
    failedJobs,
    durationMs: Date.now() - startedAt
  };

  if (options.emitMetrics) {
    buildQueueMetricsLog(result);
  }

  return result;
}

export async function runMetaAdsOrderValueSync(options: {
  now?: Date;
  triggerSource?: string;
} = {}): Promise<MetaAdsOrderValueSyncResult> {
  const now = options.now ?? new Date();
  const triggerSource = options.triggerSource ?? 'scheduler';
  const workerId = `meta-ads-order-value-${randomBytes(6).toString('hex')}`;
  let succeededConnections = 0;
  let failedConnections = 0;
  let recordsReceived = 0;
  let rawRowsFetched = 0;
  let rawRowsPersisted = 0;
  let aggregateRowsUpserted = 0;
  let apiRequestCount = 0;
  let anomalyCount = 0;
  let jobsPlanned = false;

  while (true) {
    if (!jobsPlanned) {
      await enqueueSyncJobsForWindow(now);
      jobsPlanned = true;
    }

    const jobs = await claimSyncJobs(workerId, env.META_ADS_SYNC_BATCH_SIZE);

    if (jobs.length === 0) {
      break;
    }

    for (const job of jobs) {
      const result = await processSyncJob(job, triggerSource);

      if (result.outcome === 'succeeded') {
        succeededConnections += 1;
      } else {
        failedConnections += 1;
      }

      recordsReceived += result.recordsReceived;
      rawRowsFetched += result.rawRowsFetched;
      rawRowsPersisted += result.rawRowsPersisted;
      aggregateRowsUpserted += result.aggregateRowsUpserted;
      apiRequestCount += result.apiRequestCount;
      anomalyCount += result.anomalyCount;
    }
  }

  const result: MetaAdsOrderValueSyncResult = {
    succeededConnections,
    failedConnections,
    recordsReceived,
    rawRowsFetched,
    rawRowsPersisted,
    aggregateRowsUpserted,
    apiRequestCount,
    anomalyCount
  };

  logInfo('meta_ads_order_value_sync_completed', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    triggerSource,
    succeededConnections: result.succeededConnections,
    failedConnections: result.failedConnections,
    recordsReceived: result.recordsReceived,
    rawRowsFetched: result.rawRowsFetched,
    aggregateRowsUpserted: result.aggregateRowsUpserted,
    apiRequestCount: result.apiRequestCount,
    anomalyCount: result.anomalyCount
  });

  return result;
}

export function startMetaAdsOrderValueScheduler(): () => void {
  if (!env.META_ADS_ORDER_VALUE_SYNC_ENABLED) {
    return () => undefined;
  }

  const timer = setInterval(() => {
    void runMetaAdsOrderValueSync({ triggerSource: 'scheduler' }).catch(() => undefined);
  }, env.META_ADS_ORDER_VALUE_SYNC_INTERVAL_MS);

  return () => {
    clearInterval(timer);
  };
}

export function createMetaAdsPublicRouter(): Router {
  const router = Router();

  router.get('/healthz', (_req, res) => {
    res.status(200).json({ ok: true });
  });

  return router;
}

export function createMetaAdsAdminRouter(): Router {
  const router = Router();

  router.get('/healthz', (_req, res) => {
    res.status(200).json({ ok: true });
  });

  return router;
}

export const __metaAdsTestUtils = {
  buildMetaAdsApiRequestMetricsAccumulator,
  summarizeOrderValueRecords,
  buildOrderValueSyncAnomalies,
  selectCanonicalActionType,
  normalizeOrderValueRows
};
