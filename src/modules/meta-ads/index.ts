import { createHash, randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

import { Router } from 'express';
import { type PoolClient } from 'pg';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { buildCanonicalSpendDimensions } from '../marketing-dimensions/index.js';

const META_OAUTH_STATE_TTL_MINUTES = 10;
const META_GRAPH_BASE_URL = 'https://graph.facebook.com';
const META_SYNC_JOB_STATUSES = ['pending', 'processing', 'retry', 'completed', 'failed'] as const;
const META_SPEND_LEVELS = ['account', 'campaign', 'adset', 'ad'] as const;
const META_SPEND_GRANULARITIES = ['account', 'campaign', 'adset', 'ad', 'creative'] as const;

const oauthStartQuerySchema = z.object({
  redirectPath: z.string().optional()
});

const manualSyncSchema = z.object({
  startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  endDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/)
});

const oauthCallbackSchema = z.object({
  code: z.string().min(1).optional(),
  state: z.string().min(1).optional(),
  error: z.string().optional(),
  error_description: z.string().optional()
});

type MetaSpendLevel = (typeof META_SPEND_LEVELS)[number];
type MetaSpendGranularity = (typeof META_SPEND_GRANULARITIES)[number];

type MetaAdsConnectionRow = {
  id: number;
  ad_account_id: string;
  access_token: string;
  token_type: string;
  granted_scopes: string[];
  token_expires_at: Date | null;
  last_refreshed_at: Date | null;
  last_sync_planned_for: string | null;
  status: string;
  account_name: string | null;
  account_currency: string | null;
};

type MetaAdsConnectionSummaryRow = {
  id: number;
  ad_account_id: string;
  granted_scopes: string[];
  token_expires_at: Date | null;
  last_refreshed_at: Date | null;
  last_sync_started_at: Date | null;
  last_sync_completed_at: Date | null;
  last_sync_status: string;
  last_sync_error: string | null;
  status: string;
  account_name: string | null;
  account_currency: string | null;
};

type MetaAdsSyncJobRow = {
  id: number;
  connection_id: number;
  ad_account_id: string;
  sync_date: string;
  attempts: number;
};

type MetaAdsInsightRow = {
  date_start?: string;
  date_stop?: string;
  account_id?: string;
  account_name?: string;
  campaign_id?: string;
  campaign_name?: string;
  adset_id?: string;
  adset_name?: string;
  ad_id?: string;
  ad_name?: string;
  spend?: string;
  impressions?: string;
  clicks?: string;
  objective?: string;
};

type MetaAdsTokenResponse = {
  access_token: string;
  token_type?: string;
  expires_in?: number;
};

type MetaAdsAccountResponse = {
  id: string;
  name?: string;
  account_currency?: string;
};

type MetaAdsApiErrorBody = {
  error?: {
    message?: string;
    type?: string;
    code?: number;
    error_subcode?: number;
    fbtrace_id?: string;
  };
};

type MetaAdsCreativeMap = Record<string, { creativeId: string | null; creativeName: string | null }>;

type MetaAdsNormalizedSpendRow = {
  granularity: MetaSpendGranularity;
  entityKey: string;
  accountId: string | null;
  accountName: string | null;
  campaignId: string | null;
  campaignName: string | null;
  adsetId: string | null;
  adsetName: string | null;
  adId: string | null;
  adName: string | null;
  creativeId: string | null;
  creativeName: string | null;
  canonicalSource: string;
  canonicalMedium: string;
  canonicalCampaign: string;
  canonicalContent: string;
  canonicalTerm: string;
  currency: string | null;
  spend: string;
  impressions: number;
  clicks: number;
  rawPayload: Record<string, unknown>;
};

export type MetaAdsQueueProcessOptions = {
  limit?: number;
  workerId?: string;
  emitMetrics?: boolean;
  now?: Date;
};

export type MetaAdsQueueProcessResult = {
  workerId: string;
  enqueuedJobs: number;
  claimedJobs: number;
  succeededJobs: number;
  failedJobs: number;
  durationMs: number;
};

class MetaAdsHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'MetaAdsHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

class MetaAdsApiError extends Error {
  statusCode: number;
  details: MetaAdsApiErrorBody | null;

  constructor(statusCode: number, message: string, details: MetaAdsApiErrorBody | null = null) {
    super(message);
    this.name = 'MetaAdsApiError';
    this.statusCode = statusCode;
    this.details = details;
  }
}

function requireInternalAuth(authHeader: string | undefined): boolean {
  return authHeader === `Bearer ${env.REPORTING_API_TOKEN}`;
}

function assertMetaAdsConfig(): void {
  const missing = [
    ['META_ADS_APP_ID', env.META_ADS_APP_ID],
    ['META_ADS_APP_SECRET', env.META_ADS_APP_SECRET],
    ['META_ADS_APP_BASE_URL', env.META_ADS_APP_BASE_URL],
    ['META_ADS_ENCRYPTION_KEY', env.META_ADS_ENCRYPTION_KEY],
    ['META_ADS_AD_ACCOUNT_ID', env.META_ADS_AD_ACCOUNT_ID]
  ]
    .filter(([, value]) => !value)
    .map(([key]) => key);

  if (missing.length > 0) {
    throw new MetaAdsHttpError(500, 'meta_ads_config_missing', `Missing Meta Ads configuration: ${missing.join(', ')}`);
  }
}

function normalizeMetaAdAccountId(value: string): string {
  const normalized = value.trim();
  const accountId = normalized.startsWith('act_') ? normalized.slice(4) : normalized;

  if (!/^\d+$/.test(accountId)) {
    throw new MetaAdsHttpError(400, 'invalid_meta_ad_account_id', 'META_ADS_AD_ACCOUNT_ID must be numeric or act_<id>');
  }

  return accountId;
}

function normalizeRedirectPath(rawValue: string | undefined): string | null {
  if (!rawValue) {
    return null;
  }

  const trimmed = rawValue.trim();

  if (!trimmed) {
    return null;
  }

  if (!trimmed.startsWith('/')) {
    throw new MetaAdsHttpError(400, 'invalid_redirect_path', 'redirectPath must be a root-relative path');
  }

  return trimmed;
}

function getMetaAdsAppBaseUrl(): string {
  return new URL(env.META_ADS_APP_BASE_URL).toString().replace(/\/$/, '');
}

function buildMetaAdsRedirectUri(): string {
  return `${getMetaAdsAppBaseUrl()}/meta-ads/oauth/callback`;
}

function createOAuthStateDigest(state: string): string {
  return createHash('sha256').update(state).digest('hex');
}

function buildMetaAdsAuthorizationUrl(state: string): string {
  assertMetaAdsConfig();

  const url = new URL('https://www.facebook.com/dialog/oauth');
  url.searchParams.set('client_id', env.META_ADS_APP_ID);
  url.searchParams.set('redirect_uri', buildMetaAdsRedirectUri());
  url.searchParams.set('state', state);
  url.searchParams.set('scope', env.META_ADS_APP_SCOPES.join(','));

  return url.toString();
}

function calculateTokenExpiresAt(expiresInSeconds: number | undefined, now = new Date()): Date | null {
  if (!expiresInSeconds || expiresInSeconds <= 0) {
    return null;
  }

  return new Date(now.getTime() + expiresInSeconds * 1000);
}

function computeRetryDelaySeconds(attempts: number): number {
  const safeAttempts = Math.max(1, attempts);
  return Math.min(60 * 2 ** (safeAttempts - 1), 60 * 60);
}

function shouldRefreshToken(tokenExpiresAt: Date | null, now = new Date()): boolean {
  if (!tokenExpiresAt) {
    return false;
  }

  return tokenExpiresAt.getTime() - now.getTime() <= env.META_ADS_TOKEN_REFRESH_LEEWAY_HOURS * 60 * 60 * 1000;
}

function formatDateOnly(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function parseDateOnly(value: string): Date {
  return new Date(`${value}T00:00:00.000Z`);
}

function listDateRangeInclusive(startDate: string, endDate: string): string[] {
  const start = parseDateOnly(startDate);
  const end = parseDateOnly(endDate);

  if (start.getTime() > end.getTime()) {
    throw new MetaAdsHttpError(400, 'invalid_date_range', 'startDate must be on or before endDate');
  }

  const dates: string[] = [];

  for (let cursor = start; cursor.getTime() <= end.getTime(); cursor = new Date(cursor.getTime() + 24 * 60 * 60 * 1000)) {
    dates.push(formatDateOnly(cursor));
  }

  return dates;
}

function buildPlanningDates(now = new Date(), lastSyncCompletedAt: Date | null = null): string[] {
  const yesterday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
  const lookbackDays = lastSyncCompletedAt ? env.META_ADS_SYNC_LOOKBACK_DAYS : env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS;
  const firstDate = new Date(yesterday.getTime() - (lookbackDays - 1) * 24 * 60 * 60 * 1000);

  if (yesterday.getTime() < firstDate.getTime()) {
    return [];
  }

  return listDateRangeInclusive(formatDateOnly(firstDate), formatDateOnly(yesterday));
}

function buildInsightsEntityId(level: MetaSpendLevel, row: MetaAdsInsightRow): string {
  switch (level) {
    case 'account':
      return row.account_id ?? '';
    case 'campaign':
      return row.campaign_id ?? '';
    case 'adset':
      return row.adset_id ?? '';
    case 'ad':
      return row.ad_id ?? '';
  }
}

function parseMetricInteger(value: string | undefined): number {
  const parsed = Number.parseInt(value ?? '0', 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseMetricDecimal(value: string | undefined): string {
  const parsed = Number.parseFloat(value ?? '0');
  return Number.isFinite(parsed) ? parsed.toFixed(2) : '0.00';
}

function normalizeInsightRows(
  row: MetaAdsInsightRow,
  creativeMap: MetaAdsCreativeMap,
  currency: string | null
): MetaAdsNormalizedSpendRow[] {
  const normalized: MetaAdsNormalizedSpendRow[] = [];
  const baseDimensions = buildCanonicalSpendDimensions({
    source: 'meta',
    medium: 'paid_social',
    campaign: row.campaign_name ?? null,
    content: null,
    term: null
  });
  const baseRow = {
    accountId: row.account_id ?? null,
    accountName: row.account_name ?? null,
    campaignId: row.campaign_id ?? null,
    campaignName: row.campaign_name ?? null,
    adsetId: row.adset_id ?? null,
    adsetName: row.adset_name ?? null,
    adId: row.ad_id ?? null,
    adName: row.ad_name ?? null,
    creativeId: null,
    creativeName: null,
    canonicalSource: baseDimensions.source,
    canonicalMedium: baseDimensions.medium,
    canonicalCampaign: baseDimensions.campaign,
    canonicalContent: baseDimensions.content,
    canonicalTerm: baseDimensions.term,
    currency,
    spend: parseMetricDecimal(row.spend),
    impressions: parseMetricInteger(row.impressions),
    clicks: parseMetricInteger(row.clicks),
    rawPayload: row as Record<string, unknown>
  };

  if (row.account_id) {
    normalized.push({
      ...baseRow,
      granularity: 'account',
      entityKey: row.account_id
    });
  }

  if (row.campaign_id) {
    normalized.push({
      ...baseRow,
      granularity: 'campaign',
      entityKey: row.campaign_id
    });
  }

  if (row.adset_id) {
    normalized.push({
      ...baseRow,
      granularity: 'adset',
      entityKey: row.adset_id
    });
  }

  if (row.ad_id) {
    const adDimensions = buildCanonicalSpendDimensions({
      source: 'meta',
      medium: 'paid_social',
      campaign: row.campaign_name ?? null,
      content: row.ad_name ?? null,
      term: null
    });

    normalized.push({
      ...baseRow,
      granularity: 'ad',
      entityKey: row.ad_id,
      canonicalContent: adDimensions.content
    });

    const creative = creativeMap[row.ad_id];

    if (creative?.creativeId) {
      const creativeDimensions = buildCanonicalSpendDimensions({
        source: 'meta',
        medium: 'paid_social',
        campaign: row.campaign_name ?? null,
        content: creative.creativeName ?? row.ad_name ?? null,
        term: null
      });

      normalized.push({
        ...baseRow,
        granularity: 'creative',
        entityKey: creative.creativeId,
        creativeId: creative.creativeId,
        creativeName: creative.creativeName,
        canonicalContent: creativeDimensions.content
      });
    }
  }

  return normalized;
}

function buildMetaLog(event: string, payload: Record<string, unknown>): string {
  return JSON.stringify({
    event,
    ...payload
  });
}

async function metaFetchJson<T>(url: URL, retryCount = 2): Promise<T> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= retryCount + 1; attempt += 1) {
    try {
      const response = await fetch(url);
      const text = await response.text();
      const json = text ? (JSON.parse(text) as T | MetaAdsApiErrorBody) : ({} as T);

      if (!response.ok) {
        const errorBody = (json as MetaAdsApiErrorBody) ?? null;
        const errorMessage =
          errorBody?.error?.message ?? `Meta Ads API request failed with status ${response.status}`;
        throw new MetaAdsApiError(response.status, errorMessage, errorBody);
      }

      return json as T;
    } catch (error) {
      lastError = error;

      if (
        attempt > retryCount ||
        !(error instanceof MetaAdsApiError) ||
        ![429, 500, 502, 503, 504].includes(error.statusCode)
      ) {
        break;
      }

      await delay(attempt * 500);
    }
  }

  throw lastError;
}

async function exchangeCodeForAccessToken(code: string): Promise<MetaAdsTokenResponse> {
  const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/oauth/access_token`);
  url.searchParams.set('client_id', env.META_ADS_APP_ID);
  url.searchParams.set('client_secret', env.META_ADS_APP_SECRET);
  url.searchParams.set('redirect_uri', buildMetaAdsRedirectUri());
  url.searchParams.set('code', code);

  return metaFetchJson<MetaAdsTokenResponse>(url);
}

async function exchangeLongLivedAccessToken(accessToken: string): Promise<MetaAdsTokenResponse> {
  const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/oauth/access_token`);
  url.searchParams.set('grant_type', 'fb_exchange_token');
  url.searchParams.set('client_id', env.META_ADS_APP_ID);
  url.searchParams.set('client_secret', env.META_ADS_APP_SECRET);
  url.searchParams.set('fb_exchange_token', accessToken);

  return metaFetchJson<MetaAdsTokenResponse>(url);
}

async function fetchMetaAdsAccount(accessToken: string, adAccountId: string): Promise<MetaAdsAccountResponse> {
  const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/act_${adAccountId}`);
  url.searchParams.set('access_token', accessToken);
  url.searchParams.set('fields', 'id,name,account_currency');

  return metaFetchJson<MetaAdsAccountResponse>(url);
}

async function insertOAuthState(redirectPath: string | null): Promise<string> {
  const state = randomBytes(24).toString('hex');
  const stateDigest = createOAuthStateDigest(state);

  await query(
    `
      INSERT INTO meta_ads_oauth_states (state_digest, redirect_path, expires_at)
      VALUES ($1, $2, now() + ($3::int * interval '1 minute'))
    `,
    [stateDigest, redirectPath, META_OAUTH_STATE_TTL_MINUTES]
  );

  return state;
}

async function consumeOAuthState(state: string): Promise<string | null> {
  const result = await query<{ redirect_path: string | null }>(
    `
      UPDATE meta_ads_oauth_states
      SET consumed_at = now()
      WHERE state_digest = $1
        AND consumed_at IS NULL
        AND expires_at >= now()
      RETURNING redirect_path
    `,
    [createOAuthStateDigest(state)]
  );

  if (!result.rowCount) {
    throw new MetaAdsHttpError(400, 'invalid_meta_oauth_state', 'The Meta Ads OAuth state is invalid or expired');
  }

  return result.rows[0].redirect_path;
}

async function upsertMetaAdsConnection(params: {
  adAccountId: string;
  accessToken: string;
  tokenType: string;
  grantedScopes: string[];
  tokenExpiresAt: Date | null;
  account: MetaAdsAccountResponse;
}): Promise<void> {
  await query(
    `
      INSERT INTO meta_ads_connections (
        ad_account_id,
        access_token_encrypted,
        token_type,
        granted_scopes,
        token_expires_at,
        last_refreshed_at,
        status,
        account_name,
        account_currency,
        raw_account_data,
        updated_at
      )
      VALUES (
        $1,
        pgp_sym_encrypt($2, $3, 'cipher-algo=aes256, compress-algo=0'),
        $4,
        $5::text[],
        $6,
        now(),
        'active',
        $7,
        $8,
        $9::jsonb,
        now()
      )
      ON CONFLICT (ad_account_id)
      DO UPDATE SET
        access_token_encrypted = pgp_sym_encrypt($2, $3, 'cipher-algo=aes256, compress-algo=0'),
        token_type = $4,
        granted_scopes = $5::text[],
        token_expires_at = $6,
        last_refreshed_at = now(),
        status = 'active',
        account_name = $7,
        account_currency = $8,
        raw_account_data = $9::jsonb,
        updated_at = now()
    `,
    [
      params.adAccountId,
      params.accessToken,
      env.META_ADS_ENCRYPTION_KEY,
      params.tokenType,
      params.grantedScopes,
      params.tokenExpiresAt,
      params.account.name ?? null,
      params.account.account_currency ?? null,
      JSON.stringify(params.account)
    ]
  );
}

async function getActiveMetaAdsConnection(): Promise<MetaAdsConnectionRow | null> {
  const result = await query<MetaAdsConnectionRow>(
    `
      SELECT
        id,
        ad_account_id,
        pgp_sym_decrypt(access_token_encrypted, $1) AS access_token,
        token_type,
        granted_scopes,
        token_expires_at,
        last_refreshed_at,
        last_sync_planned_for::text,
        status,
        account_name,
        account_currency
      FROM meta_ads_connections
      WHERE status = 'active'
      ORDER BY updated_at DESC
      LIMIT 1
    `,
    [env.META_ADS_ENCRYPTION_KEY]
  );

  return result.rows[0] ?? null;
}

async function refreshMetaAdsConnectionToken(connection: MetaAdsConnectionRow): Promise<MetaAdsConnectionRow> {
  const refreshed = await exchangeLongLivedAccessToken(connection.access_token);
  const expiresAt = calculateTokenExpiresAt(refreshed.expires_in);
  const accessToken = refreshed.access_token;

  await query(
    `
      UPDATE meta_ads_connections
      SET
        access_token_encrypted = pgp_sym_encrypt($2, $3, 'cipher-algo=aes256, compress-algo=0'),
        token_type = $4,
        token_expires_at = $5,
        last_refreshed_at = now(),
        updated_at = now()
      WHERE id = $1
    `,
    [connection.id, accessToken, env.META_ADS_ENCRYPTION_KEY, refreshed.token_type ?? connection.token_type, expiresAt]
  );

  return {
    ...connection,
    access_token: accessToken,
    token_type: refreshed.token_type ?? connection.token_type,
    token_expires_at: expiresAt,
    last_refreshed_at: new Date()
  };
}

async function getUsableMetaAdsConnection(forceRefresh = false): Promise<MetaAdsConnectionRow> {
  const connection = await getActiveMetaAdsConnection();

  if (!connection) {
    throw new MetaAdsHttpError(404, 'meta_ads_connection_not_found', 'No active Meta Ads connection was found');
  }

  if (forceRefresh || shouldRefreshToken(connection.token_expires_at)) {
    return refreshMetaAdsConnectionToken(connection);
  }

  return connection;
}

async function enqueueSyncDates(connectionId: number, dates: string[]): Promise<number> {
  let enqueuedJobs = 0;

  for (const date of dates) {
    await query(
      `
        INSERT INTO meta_ads_sync_jobs (connection_id, sync_date, status, available_at, updated_at)
        VALUES ($1, $2::date, 'pending', now(), now())
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
          last_error = NULL,
          completed_at = CASE
            WHEN meta_ads_sync_jobs.status = 'processing' THEN meta_ads_sync_jobs.completed_at
            ELSE NULL
          END,
          updated_at = now()
      `,
      [connectionId, date]
    );

    enqueuedJobs += 1;
  }

  return enqueuedJobs;
}

async function planIncrementalSyncs(now = new Date()): Promise<number> {
  const result = await query<{
    id: number;
    last_sync_completed_at: Date | null;
    last_sync_planned_for: string | null;
  }>(
    `
      SELECT
        id,
        last_sync_completed_at,
        last_sync_planned_for::text
      FROM meta_ads_connections
      WHERE status = 'active'
    `
  );

  let plannedJobs = 0;
  const today = formatDateOnly(now);

  for (const row of result.rows) {
    if (row.last_sync_planned_for === today) {
      continue;
    }

    const dates = buildPlanningDates(now, row.last_sync_completed_at);

    if (dates.length === 0) {
      continue;
    }

    plannedJobs += await enqueueSyncDates(row.id, dates);
    await query('UPDATE meta_ads_connections SET last_sync_planned_for = $2::date, updated_at = now() WHERE id = $1', [
      row.id,
      today
    ]);
  }

  return plannedJobs;
}

async function claimSyncJobs(workerId: string, limit: number): Promise<MetaAdsSyncJobRow[]> {
  const result = await query<MetaAdsSyncJobRow>(
    `
      WITH claimable AS (
        SELECT j.id, j.connection_id
        FROM meta_ads_sync_jobs j
        WHERE j.status IN ('pending', 'retry')
          AND j.available_at <= now()
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
      RETURNING j.id, j.connection_id, c.ad_account_id, j.sync_date::text, j.attempts
    `,
    [workerId, limit]
  );

  return result.rows;
}

async function fetchInsightsForLevel(
  accessToken: string,
  adAccountId: string,
  syncDate: string,
  level: MetaSpendLevel
): Promise<MetaAdsInsightRow[]> {
  const rows: MetaAdsInsightRow[] = [];
  let nextUrl: URL | null = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/act_${adAccountId}/insights`);

  nextUrl.searchParams.set(
    'fields',
    'account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,objective,date_start,date_stop'
  );
  nextUrl.searchParams.set('access_token', accessToken);
  nextUrl.searchParams.set('level', level);
  nextUrl.searchParams.set('time_increment', '1');
  nextUrl.searchParams.set('limit', '500');
  nextUrl.searchParams.set('time_range', JSON.stringify({ since: syncDate, until: syncDate }));

  while (nextUrl) {
    const page: {
      data?: MetaAdsInsightRow[];
      paging?: { next?: string };
    } = await metaFetchJson(nextUrl);

    rows.push(...(page.data ?? []));
    nextUrl = page.paging?.next ? new URL(page.paging.next) : null;
  }

  return rows.filter((row) => buildInsightsEntityId(level, row));
}

async function fetchCreativeMap(accessToken: string, adIds: string[]): Promise<MetaAdsCreativeMap> {
  const creativeMap: MetaAdsCreativeMap = {};

  for (let index = 0; index < adIds.length; index += 50) {
    const chunk = adIds.slice(index, index + 50);

    if (chunk.length === 0) {
      continue;
    }

    const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/`);
    url.searchParams.set('access_token', accessToken);
    url.searchParams.set('ids', chunk.join(','));
    url.searchParams.set('fields', 'creative{id,name}');

    const response = await metaFetchJson<Record<string, { creative?: { id?: string; name?: string } }>>(url);

    for (const adId of chunk) {
      const creative = response[adId]?.creative;
      creativeMap[adId] = {
        creativeId: creative?.id ?? null,
        creativeName: creative?.name ?? null
      };
    }
  }

  return creativeMap;
}

async function persistDailySpendSnapshot(
  client: PoolClient,
  params: {
    connectionId: number;
    syncJobId: number;
    syncDate: string;
    currency: string | null;
    rowsByLevel: Record<MetaSpendLevel, MetaAdsInsightRow[]>;
    creativeMap: MetaAdsCreativeMap;
  }
): Promise<void> {
  await client.query(
    'DELETE FROM meta_ads_daily_spend WHERE connection_id = $1 AND report_date = $2::date',
    [params.connectionId, params.syncDate]
  );
  await client.query(
    'DELETE FROM meta_ads_raw_spend_records WHERE connection_id = $1 AND report_date = $2::date',
    [params.connectionId, params.syncDate]
  );

  for (const level of META_SPEND_LEVELS) {
    for (const row of params.rowsByLevel[level]) {
      const entityId = buildInsightsEntityId(level, row);

      if (!entityId) {
        continue;
      }

      const rawInsert = await client.query<{ id: number }>(
        `
          INSERT INTO meta_ads_raw_spend_records (
            connection_id,
            sync_job_id,
            report_date,
            level,
            entity_id,
            currency,
            spend,
            impressions,
            clicks,
            raw_payload,
            updated_at
          )
          VALUES ($1, $2, $3::date, $4, $5, $6, $7::numeric, $8, $9, $10::jsonb, now())
          RETURNING id
        `,
        [
          params.connectionId,
          params.syncJobId,
          params.syncDate,
          level,
          entityId,
          params.currency,
          parseMetricDecimal(row.spend),
          parseMetricInteger(row.impressions),
          parseMetricInteger(row.clicks),
          JSON.stringify(row)
        ]
      );

      const rawRecordId = rawInsert.rows[0].id;
      const normalizedRows = normalizeInsightRows(row, params.creativeMap, params.currency);

      for (const normalizedRow of normalizedRows) {
        await client.query(
          `
            INSERT INTO meta_ads_daily_spend (
              connection_id,
              raw_record_id,
              sync_job_id,
              report_date,
              granularity,
              entity_key,
              account_id,
              account_name,
              campaign_id,
              campaign_name,
              adset_id,
              adset_name,
              ad_id,
              ad_name,
              creative_id,
              creative_name,
              canonical_source,
              canonical_medium,
              canonical_campaign,
              canonical_content,
              canonical_term,
              currency,
              spend,
              impressions,
              clicks,
              raw_payload,
              updated_at
            )
            VALUES (
              $1, $2, $3, $4::date, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
              $18, $19, $20, $21, $22,
              $23::numeric, $24, $25, $26::jsonb, now()
            )
          `,
          [
            params.connectionId,
            rawRecordId,
            params.syncJobId,
            params.syncDate,
            normalizedRow.granularity,
            normalizedRow.entityKey,
            normalizedRow.accountId,
            normalizedRow.accountName,
            normalizedRow.campaignId,
            normalizedRow.campaignName,
            normalizedRow.adsetId,
            normalizedRow.adsetName,
            normalizedRow.adId,
            normalizedRow.adName,
            normalizedRow.creativeId,
            normalizedRow.creativeName,
            normalizedRow.canonicalSource,
            normalizedRow.canonicalMedium,
            normalizedRow.canonicalCampaign,
            normalizedRow.canonicalContent,
            normalizedRow.canonicalTerm,
            normalizedRow.currency,
            normalizedRow.spend,
            normalizedRow.impressions,
            normalizedRow.clicks,
            JSON.stringify(normalizedRow.rawPayload)
          ]
        );
      }
    }
  }
}

async function markSyncJobSucceeded(jobId: number, connectionId: number): Promise<void> {
  await query(
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

  await query(
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

async function markSyncJobFailed(job: MetaAdsSyncJobRow, error: unknown): Promise<void> {
  const lastError = error instanceof Error ? error.message : String(error);
  const shouldRetry = job.attempts < env.META_ADS_SYNC_MAX_RETRIES;
  const nextStatus = shouldRetry ? 'retry' : 'failed';
  const retryDelaySeconds = computeRetryDelaySeconds(job.attempts);

  await query(
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
    [job.id, nextStatus, retryDelaySeconds, lastError]
  );

  await query(
    `
      UPDATE meta_ads_connections
      SET
        last_sync_status = $2,
        last_sync_error = $3,
        updated_at = now()
      WHERE id = $1
    `,
    [job.connection_id, shouldRetry ? 'retry' : 'failed', lastError]
  );

  process.stderr.write(
    `${buildMetaLog('meta_ads_sync_job_failed', {
      severity: shouldRetry ? 'WARNING' : 'ERROR',
      alertable: !shouldRetry,
      jobId: job.id,
      connectionId: job.connection_id,
      adAccountId: job.ad_account_id,
      syncDate: job.sync_date,
      attempts: job.attempts,
      willRetry: shouldRetry,
      retryDelaySeconds: shouldRetry ? retryDelaySeconds : 0,
      error: lastError
    })}\n`
  );
}

async function processSyncJob(job: MetaAdsSyncJobRow): Promise<void> {
  await query(
    `
      UPDATE meta_ads_connections
      SET
        last_sync_started_at = now(),
        last_sync_status = 'running',
        last_sync_error = NULL,
        updated_at = now()
      WHERE id = $1
    `,
    [job.connection_id]
  );

  let connection = await getUsableMetaAdsConnection();

  try {
    const rowsByLevel = {
      account: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'account'),
      campaign: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'campaign'),
      adset: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'adset'),
      ad: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'ad')
    } satisfies Record<MetaSpendLevel, MetaAdsInsightRow[]>;

    const adIds = [...new Set(rowsByLevel.ad.map((row) => row.ad_id).filter((value): value is string => Boolean(value)))];
    const creativeMap = await fetchCreativeMap(connection.access_token, adIds);
    await withTransaction(async (client) => {
      await persistDailySpendSnapshot(client, {
        connectionId: job.connection_id,
        syncJobId: job.id,
        syncDate: job.sync_date,
        currency: connection.account_currency,
        rowsByLevel,
        creativeMap
      });
    });

    await markSyncJobSucceeded(job.id, job.connection_id);
  } catch (error) {
    if (error instanceof MetaAdsApiError && [400, 401, 403].includes(error.statusCode)) {
      connection = await getUsableMetaAdsConnection(true);

      try {
        const rowsByLevel = {
          account: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'account'),
          campaign: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'campaign'),
          adset: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'adset'),
          ad: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'ad')
        } satisfies Record<MetaSpendLevel, MetaAdsInsightRow[]>;

        const adIds = [
          ...new Set(rowsByLevel.ad.map((row) => row.ad_id).filter((value): value is string => Boolean(value)))
        ];
        const creativeMap = await fetchCreativeMap(connection.access_token, adIds);

        await withTransaction(async (client) => {
          await persistDailySpendSnapshot(client, {
            connectionId: job.connection_id,
            syncJobId: job.id,
            syncDate: job.sync_date,
            currency: connection.account_currency,
            rowsByLevel,
            creativeMap
          });
        });

        await markSyncJobSucceeded(job.id, job.connection_id);
        return;
      } catch (retryError) {
        await markSyncJobFailed(job, retryError);
        return;
      }
    }

    await markSyncJobFailed(job, error);
  }
}

function buildMetricsLog(result: MetaAdsQueueProcessResult): string {
  return buildMetaLog('meta_ads_sync_run', {
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
  const limit = options.limit ?? env.META_ADS_SYNC_BATCH_SIZE;
  const now = options.now ?? new Date();
  const enqueuedJobs = await planIncrementalSyncs(now);
  const jobs = await claimSyncJobs(workerId, limit);
  let succeededJobs = 0;
  let failedJobs = 0;

  for (const job of jobs) {
    try {
      await processSyncJob(job);
      succeededJobs += 1;
    } catch (error) {
      failedJobs += 1;
      await markSyncJobFailed(job, error);
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
    process.stdout.write(`${buildMetricsLog(result)}\n`);
  }

  return result;
}

export function createMetaAdsPublicRouter(): Router {
  const router = Router();

  router.get('/oauth/callback', async (req, res, next) => {
    try {
      assertMetaAdsConfig();
      const payload = oauthCallbackSchema.parse(req.query);

      if (payload.error) {
        throw new MetaAdsHttpError(
          400,
          'meta_ads_oauth_denied',
          payload.error_description ?? `Meta Ads OAuth failed with error: ${payload.error}`
        );
      }

      if (!payload.code || !payload.state) {
        throw new MetaAdsHttpError(400, 'meta_ads_oauth_invalid_callback', 'Missing OAuth callback parameters');
      }

      const redirectPath = await consumeOAuthState(payload.state);
      const shortLivedToken = await exchangeCodeForAccessToken(payload.code);
      const longLivedToken = await exchangeLongLivedAccessToken(shortLivedToken.access_token);
      const adAccountId = normalizeMetaAdAccountId(env.META_ADS_AD_ACCOUNT_ID);
      const account = await fetchMetaAdsAccount(longLivedToken.access_token, adAccountId);

      await upsertMetaAdsConnection({
        adAccountId,
        accessToken: longLivedToken.access_token,
        tokenType: longLivedToken.token_type ?? shortLivedToken.token_type ?? 'Bearer',
        grantedScopes: env.META_ADS_APP_SCOPES,
        tokenExpiresAt: calculateTokenExpiresAt(longLivedToken.expires_in),
        account
      });

      const initialDates = buildPlanningDates(new Date(), null);
      const connection = await getUsableMetaAdsConnection();
      await enqueueSyncDates(connection.id, initialDates);

      if (redirectPath) {
        res.redirect(302, redirectPath);
        return;
      }

      res.status(200).json({
        ok: true,
        adAccountId,
        plannedDates: initialDates
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}

export function createMetaAdsAdminRouter(): Router {
  const router = Router();

  router.use((req, res, next) => {
    if (!requireInternalAuth(req.header('authorization') ?? undefined)) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }

    next();
  });

  router.get('/oauth/start', async (req, res, next) => {
    try {
      assertMetaAdsConfig();
      const payload = oauthStartQuerySchema.parse(req.query);
      const redirectPath = normalizeRedirectPath(payload.redirectPath);
      const state = await insertOAuthState(redirectPath);

      res.status(200).json({
        authorizationUrl: buildMetaAdsAuthorizationUrl(state),
        redirectUri: buildMetaAdsRedirectUri(),
        state
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/status', async (_req, res, next) => {
    try {
      const result = await query<MetaAdsConnectionSummaryRow>(
        `
          SELECT
            id,
            ad_account_id,
            granted_scopes,
            token_expires_at,
            last_refreshed_at,
            last_sync_started_at,
            last_sync_completed_at,
            last_sync_status,
            last_sync_error,
            status,
            account_name,
            account_currency
          FROM meta_ads_connections
          ORDER BY updated_at DESC
          LIMIT 1
        `
      );

      res.status(200).json({
        connection: result.rows[0] ?? null
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/sync', async (req, res, next) => {
    try {
      const payload = manualSyncSchema.parse(req.body);
      const connection = await getUsableMetaAdsConnection();
      const dates = listDateRangeInclusive(payload.startDate, payload.endDate);
      const enqueuedJobs = await enqueueSyncDates(connection.id, dates);

      res.status(202).json({
        ok: true,
        enqueuedJobs,
        dates
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/refresh-token', async (_req, res, next) => {
    try {
      const connection = await getUsableMetaAdsConnection(true);

      res.status(200).json({
        ok: true,
        tokenExpiresAt: connection.token_expires_at,
        lastRefreshedAt: connection.last_refreshed_at
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}

export const __metaAdsTestUtils = {
  normalizeMetaAdAccountId,
  buildMetaAdsAuthorizationUrl,
  buildMetaAdsRedirectUri,
  calculateTokenExpiresAt,
  computeRetryDelaySeconds,
  shouldRefreshToken,
  buildPlanningDates,
  listDateRangeInclusive,
  normalizeInsightRows
};
