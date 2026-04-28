import { createHash, randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

import { Router } from 'express';
import type { PoolClient } from 'pg';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import {
  buildRawPayloadStorageMetadata,
  logRawPayloadIntegrityMismatch,
  type RawPayloadIntegrityRow
} from '../../shared/raw-payload-storage.js';
import { attachAuthContext, requireAdmin } from '../auth/index.js';
import {
  buildSearchParamsAuditPayload,
  parseJsonResponsePayload,
  recordAdSyncApiTransaction
} from '../ad-sync-audit/index.js';
import { buildCanonicalSpendDimensions } from '../marketing-dimensions/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';

const META_OAUTH_STATE_TTL_MINUTES = 10;
const META_GRAPH_BASE_URL = 'https://graph.facebook.com';
const META_SYNC_JOB_STATUSES = ['pending', 'processing', 'retry', 'completed', 'failed'] as const;
const META_SPEND_LEVELS = ['account', 'campaign', 'adset', 'ad'] as const;
const META_SPEND_GRANULARITIES = ['account', 'campaign', 'adset', 'ad', 'creative'] as const;
const META_ADS_SYNC_TIME_ZONE = 'America/Los_Angeles';

const oauthStartQuerySchema = z.object({
  redirectPath: z.string().optional()
});

const metaAdsConfigUpdateSchema = z.object({
  appId: z.string().min(1),
  appSecret: z.string().optional(),
  appBaseUrl: z.string().url(),
  appScopes: z.union([z.string(), z.array(z.string())]).optional(),
  adAccountId: z.string().min(1)
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

type MetaAdsSettingsRow = {
  id: number;
  app_id: string;
  app_secret: string | null;
  app_base_url: string;
  app_scopes: string[];
  ad_account_id: string;
  updated_at: Date;
};

type MetaAdsResolvedConfig = {
  appId: string;
  appSecret: string;
  appBaseUrl: string;
  appScopes: string[];
  adAccountId: string;
  encryptionKey: string;
  source: 'database' | 'environment';
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
  currency?: string;
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

type MetaAdsPersistableSpendRow = {
  rawRecordId: number | null;
  normalizedRow: MetaAdsNormalizedSpendRow;
};

type MetaAdsSyncAuditContext = {
  connectionId: number;
  syncJobId: number;
  transactionSource: string;
  sourceMetadata?: Record<string, unknown>;
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

function normalizeMetaAdsScopes(rawValue: string | string[] | undefined): string[] {
  if (Array.isArray(rawValue)) {
    return rawValue.map((entry) => entry.trim()).filter(Boolean);
  }

  if (typeof rawValue !== 'string') {
    return [];
  }

  return rawValue
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
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

async function getStoredMetaAdsSettings(): Promise<MetaAdsSettingsRow | null> {
  const result = await query<MetaAdsSettingsRow>(
    `
      SELECT
        id,
        app_id,
        pgp_sym_decrypt(app_secret_encrypted, $1) AS app_secret,
        app_base_url,
        app_scopes,
        ad_account_id,
        updated_at
      FROM meta_ads_settings
      ORDER BY updated_at DESC
      LIMIT 1
    `,
    [env.META_ADS_ENCRYPTION_KEY]
  );

  return result.rows[0] ?? null;
}

async function getResolvedMetaAdsConfig(): Promise<MetaAdsResolvedConfig> {
  if (!env.META_ADS_ENCRYPTION_KEY) {
    throw new MetaAdsHttpError(500, 'meta_ads_config_missing', 'Missing Meta Ads configuration: META_ADS_ENCRYPTION_KEY');
  }

  const stored = await getStoredMetaAdsSettings();
  const appId = stored?.app_id?.trim() || env.META_ADS_APP_ID.trim();
  const appSecret = stored?.app_secret?.trim() || env.META_ADS_APP_SECRET.trim();
  const appBaseUrl = (stored?.app_base_url?.trim() || env.META_ADS_APP_BASE_URL.trim()).replace(/\/$/, '');
  const appScopes = (stored?.app_scopes?.length ? stored.app_scopes : env.META_ADS_APP_SCOPES).map((entry) => entry.trim()).filter(Boolean);
  const adAccountId = stored?.ad_account_id?.trim() || env.META_ADS_AD_ACCOUNT_ID.trim();
  const missing = [
    ['META_ADS_APP_ID', appId],
    ['META_ADS_APP_SECRET', appSecret],
    ['META_ADS_APP_BASE_URL', appBaseUrl],
    ['META_ADS_AD_ACCOUNT_ID', adAccountId]
  ]
    .filter(([, value]) => !value)
    .map(([key]) => key);

  if (missing.length > 0) {
    throw new MetaAdsHttpError(500, 'meta_ads_config_missing', `Missing Meta Ads configuration: ${missing.join(', ')}`);
  }

  return {
    appId,
    appSecret,
    appBaseUrl,
    appScopes,
    adAccountId,
    encryptionKey: env.META_ADS_ENCRYPTION_KEY,
    source: stored ? 'database' : 'environment'
  };
}

async function getMetaAdsConfigurationSummary(): Promise<{
  source: 'database' | 'environment';
  appId: string;
  appBaseUrl: string;
  appScopes: string[];
  adAccountId: string;
  appSecretConfigured: boolean;
  missingFields: string[];
}> {
  const stored = env.META_ADS_ENCRYPTION_KEY ? await getStoredMetaAdsSettings() : null;
  const appId = stored?.app_id?.trim() || env.META_ADS_APP_ID.trim();
  const appSecretConfigured = Boolean(stored?.app_secret?.trim() || env.META_ADS_APP_SECRET.trim());
  const appBaseUrl = (stored?.app_base_url?.trim() || env.META_ADS_APP_BASE_URL.trim()).replace(/\/$/, '');
  const appScopes = (stored?.app_scopes?.length ? stored.app_scopes : env.META_ADS_APP_SCOPES).map((entry) => entry.trim()).filter(Boolean);
  const adAccountId = stored?.ad_account_id?.trim() || env.META_ADS_AD_ACCOUNT_ID.trim();
  const missingFields = [
    ['appId', appId],
    ['appSecret', appSecretConfigured ? 'configured' : ''],
    ['appBaseUrl', appBaseUrl],
    ['adAccountId', adAccountId],
    ['encryptionKey', env.META_ADS_ENCRYPTION_KEY]
  ]
    .filter(([, value]) => !value)
    .map(([key]) => key);

  return {
    source: stored ? 'database' : 'environment',
    appId,
    appBaseUrl,
    appScopes,
    adAccountId,
    appSecretConfigured,
    missingFields
  };
}

async function upsertMetaAdsSettings(payload: z.infer<typeof metaAdsConfigUpdateSchema>): Promise<void> {
  if (!env.META_ADS_ENCRYPTION_KEY) {
    throw new MetaAdsHttpError(500, 'meta_ads_config_missing', 'Missing Meta Ads configuration: META_ADS_ENCRYPTION_KEY');
  }

  const secretProvided = typeof payload.appSecret === 'string' && payload.appSecret.trim().length > 0;
  const normalizedScopes = normalizeMetaAdsScopes(payload.appScopes);
  const existing = await getStoredMetaAdsSettings();
  const nextSecret = secretProvided ? (payload.appSecret ?? '').trim() : existing?.app_secret ?? '';

  await query(
    `
      DELETE FROM meta_ads_settings
    `
  );

  await query(
    `
      INSERT INTO meta_ads_settings (
        id,
        app_id,
        app_secret_encrypted,
        app_base_url,
        app_scopes,
        ad_account_id,
        updated_at
      )
      VALUES (
        1,
        $1,
        CASE
          WHEN $2::text = '' THEN NULL
          ELSE pgp_sym_encrypt($2, $6, 'cipher-algo=aes256, compress-algo=0')
        END,
        $3,
        $4::text[],
        $5,
        now()
      )
    `,
    [
      payload.appId.trim(),
      nextSecret,
      new URL(payload.appBaseUrl).toString().replace(/\/$/, ''),
      normalizedScopes,
      payload.adAccountId.trim(),
      env.META_ADS_ENCRYPTION_KEY
    ]
  );
}

function getMetaAdsAppBaseUrl(config: MetaAdsResolvedConfig): string {
  return new URL(config.appBaseUrl).toString().replace(/\/$/, '');
}

function buildMetaAdsRedirectUri(config: MetaAdsResolvedConfig): string {
  return `${getMetaAdsAppBaseUrl(config)}/meta-ads/oauth/callback`;
}

function createOAuthStateDigest(state: string): string {
  return createHash('sha256').update(state).digest('hex');
}

function buildMetaAdsAuthorizationUrl(config: MetaAdsResolvedConfig, state: string): string {
  const url = new URL('https://www.facebook.com/dialog/oauth');
  url.searchParams.set('client_id', config.appId);
  url.searchParams.set('redirect_uri', buildMetaAdsRedirectUri(config));
  url.searchParams.set('state', state);
  url.searchParams.set('scope', config.appScopes.join(','));

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
  const currentBusinessDate = parseDateOnly(formatDateInTimeZone(now, META_ADS_SYNC_TIME_ZONE));
  const firstDate = lastSyncCompletedAt
    ? parseDateOnly(formatDateInTimeZone(lastSyncCompletedAt, META_ADS_SYNC_TIME_ZONE))
    : new Date(currentBusinessDate.getTime() - (env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS - 1) * 24 * 60 * 60 * 1000);

  if (currentBusinessDate.getTime() < firstDate.getTime()) {
    return [];
  }

  return listDateRangeInclusive(formatDateOnly(firstDate), formatDateOnly(currentBusinessDate));
}

function buildIncrementalPlanningDates(
  now = new Date(),
  lastSyncCompletedAt: Date | null = null,
  lastSyncPlannedFor: string | null = null
): string[] {
  const today = formatDateInTimeZone(now, META_ADS_SYNC_TIME_ZONE);

  if (lastSyncPlannedFor === today) {
    return [today];
  }

  return buildPlanningDates(now, lastSyncCompletedAt);
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

function formatDateInTimeZone(value: Date, timeZone: string): string {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });

  return formatter.format(value);
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

function rollupNormalizedSpendRows(rows: MetaAdsNormalizedSpendRow[]): MetaAdsNormalizedSpendRow[] {
  const rollup = new Map<string, MetaAdsNormalizedSpendRow>();

  for (const row of rows) {
    const key = `${row.granularity}:${row.entityKey}`;
    const existing = rollup.get(key);

    if (!existing) {
      rollup.set(key, { ...row });
      continue;
    }

    existing.spend = (Number(existing.spend) + Number(row.spend)).toFixed(2);
    existing.impressions += row.impressions;
    existing.clicks += row.clicks;
  }

  return [...rollup.values()];
}

function rollupPersistableSpendRows(rows: MetaAdsPersistableSpendRow[]): MetaAdsPersistableSpendRow[] {
  const rollup = new Map<string, MetaAdsPersistableSpendRow>();

  for (const row of rows) {
    const key = `${row.normalizedRow.granularity}:${row.normalizedRow.entityKey}`;
    const existing = rollup.get(key);

    if (!existing) {
      rollup.set(key, {
        rawRecordId: row.rawRecordId,
        normalizedRow: { ...row.normalizedRow }
      });
      continue;
    }

    existing.normalizedRow.spend = (
      Number(existing.normalizedRow.spend) + Number(row.normalizedRow.spend)
    ).toFixed(2);
    existing.normalizedRow.impressions += row.normalizedRow.impressions;
    existing.normalizedRow.clicks += row.normalizedRow.clicks;

    if (existing.rawRecordId === null) {
      existing.rawRecordId = row.rawRecordId;
    }
  }

  return [...rollup.values()];
}

function buildMetaLog(event: string, payload: Record<string, unknown>): string {
  return JSON.stringify({
    event,
    ...payload
  });
}

async function metaFetchJson<T>(url: URL, retryCount = 2, audit?: MetaAdsSyncAuditContext): Promise<T> {
  let lastError: unknown;
  const requestUrl = `${url.origin}${url.pathname}`;
  const requestPayload = buildSearchParamsAuditPayload(url.searchParams, ['access_token']);

  for (let attempt = 1; attempt <= retryCount + 1; attempt += 1) {
    const requestStartedAt = new Date();

    try {
      const response = await fetch(url);
      const text = await response.text();
      const json = parseJsonResponsePayload(text) as T | MetaAdsApiErrorBody;
      const responseReceivedAt = new Date();

      if (audit) {
        await recordAdSyncApiTransaction({
          platform: 'meta_ads',
          connectionId: audit.connectionId,
          syncJobId: audit.syncJobId,
          transactionSource: audit.transactionSource,
          sourceMetadata: {
            ...(audit.sourceMetadata ?? {}),
            attempt
          },
          requestMethod: 'GET',
          requestUrl,
          requestPayload,
          requestStartedAt,
          responseStatus: response.status,
          responsePayload: json,
          responseReceivedAt,
          errorMessage:
            response.ok ? null : (json as MetaAdsApiErrorBody | null)?.error?.message ?? `HTTP ${response.status}`
        });
      }

      if (!response.ok) {
        const errorBody = (json as MetaAdsApiErrorBody) ?? null;
        const errorMessage =
          errorBody?.error?.message ?? `Meta Ads API request failed with status ${response.status}`;
        throw new MetaAdsApiError(response.status, errorMessage, errorBody);
      }

      return json as T;
    } catch (error) {
      lastError = error;

      if (audit && !(error instanceof MetaAdsApiError)) {
        await recordAdSyncApiTransaction({
          platform: 'meta_ads',
          connectionId: audit.connectionId,
          syncJobId: audit.syncJobId,
          transactionSource: audit.transactionSource,
          sourceMetadata: {
            ...(audit.sourceMetadata ?? {}),
            attempt
          },
          requestMethod: 'GET',
          requestUrl,
          requestPayload,
          requestStartedAt,
          responseStatus: null,
          responsePayload: null,
          responseReceivedAt: null,
          errorMessage: error instanceof Error ? error.message : String(error)
        });
      }

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

async function exchangeCodeForAccessToken(config: MetaAdsResolvedConfig, code: string): Promise<MetaAdsTokenResponse> {
  const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/oauth/access_token`);
  url.searchParams.set('client_id', config.appId);
  url.searchParams.set('client_secret', config.appSecret);
  url.searchParams.set('redirect_uri', buildMetaAdsRedirectUri(config));
  url.searchParams.set('code', code);

  return metaFetchJson<MetaAdsTokenResponse>(url);
}

async function exchangeLongLivedAccessToken(
  config: MetaAdsResolvedConfig,
  accessToken: string
): Promise<MetaAdsTokenResponse> {
  const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/oauth/access_token`);
  url.searchParams.set('grant_type', 'fb_exchange_token');
  url.searchParams.set('client_id', config.appId);
  url.searchParams.set('client_secret', config.appSecret);
  url.searchParams.set('fb_exchange_token', accessToken);

  return metaFetchJson<MetaAdsTokenResponse>(url);
}

async function fetchMetaAdsAccount(accessToken: string, adAccountId: string): Promise<MetaAdsAccountResponse> {
  const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/act_${adAccountId}`);
  url.searchParams.set('access_token', accessToken);
  url.searchParams.set('fields', 'id,name,currency');

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
  encryptionKey: string;
}): Promise<void> {
  const rawPayloadMetadata = buildRawPayloadStorageMetadata(params.account);
  const { rawPayloadJson, payloadSizeBytes, payloadHash } = rawPayloadMetadata;

  const upsertResult = await query<RawPayloadIntegrityRow>(
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
        raw_account_external_id,
        raw_account_payload_size_bytes,
        raw_account_payload_hash,
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
        $10,
        $11,
        $12,
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
        raw_account_external_id = $10,
        raw_account_payload_size_bytes = $11,
        raw_account_payload_hash = $12,
        updated_at = now()
      RETURNING
        raw_account_payload_size_bytes AS "storedPayloadSizeBytes",
        raw_account_payload_hash AS "storedPayloadHash",
        raw_account_data AS "persistedRawPayload"
    `,
    [
      params.adAccountId,
      params.accessToken,
      params.encryptionKey,
      params.tokenType,
      params.grantedScopes,
      params.tokenExpiresAt,
      params.account.name ?? null,
      params.account.currency ?? params.account.account_currency ?? null,
      rawPayloadJson,
      params.adAccountId,
      payloadSizeBytes,
      payloadHash
    ]
  );

  logRawPayloadIntegrityMismatch(rawPayloadMetadata, upsertResult.rows[0], {
    surface: 'meta_ads_connections.raw_account_data',
    operation: 'upsert',
    recordId: params.adAccountId
  });
}

async function getActiveMetaAdsConnection(): Promise<MetaAdsConnectionRow | null> {
  const config = await getResolvedMetaAdsConfig();
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
    [config.encryptionKey]
  );

  return result.rows[0] ?? null;
}

async function refreshMetaAdsConnectionToken(connection: MetaAdsConnectionRow): Promise<MetaAdsConnectionRow> {
  const config = await getResolvedMetaAdsConfig();
  const refreshed = await exchangeLongLivedAccessToken(config, connection.access_token);
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
    [connection.id, accessToken, config.encryptionKey, refreshed.token_type ?? connection.token_type, expiresAt]
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
            WHEN meta_ads_sync_jobs.status IN ('pending', 'retry', 'processing') THEN meta_ads_sync_jobs.status
            ELSE 'pending'
          END,
          available_at = CASE
            WHEN meta_ads_sync_jobs.status IN ('pending', 'retry', 'processing') THEN meta_ads_sync_jobs.available_at
            ELSE now()
          END,
          last_error = CASE
            WHEN meta_ads_sync_jobs.status IN ('pending', 'retry', 'processing') THEN meta_ads_sync_jobs.last_error
            ELSE NULL
          END,
          completed_at = CASE
            WHEN meta_ads_sync_jobs.status IN ('pending', 'retry', 'processing') THEN meta_ads_sync_jobs.completed_at
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
  const today = formatDateInTimeZone(now, META_ADS_SYNC_TIME_ZONE);

  for (const row of result.rows) {
    const dates = buildIncrementalPlanningDates(now, row.last_sync_completed_at, row.last_sync_planned_for);

    if (dates.length === 0) {
      continue;
    }

    plannedJobs += await enqueueSyncDates(row.id, dates);

    if (row.last_sync_planned_for !== today) {
      await query('UPDATE meta_ads_connections SET last_sync_planned_for = $2::date, updated_at = now() WHERE id = $1', [
        row.id,
        today
      ]);
    }
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
  audit: {
    connectionId: number;
    syncJobId: number;
  },
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
    } = await metaFetchJson(nextUrl, 2, {
      connectionId: audit.connectionId,
      syncJobId: audit.syncJobId,
      transactionSource: 'meta_ads_insights',
      sourceMetadata: {
        adAccountId,
        syncDate,
        level
      }
    });

    rows.push(...(page.data ?? []));
    nextUrl = page.paging?.next ? new URL(page.paging.next) : null;
  }

  return rows;
}

async function fetchCreativeMap(
  audit: {
    connectionId: number;
    syncJobId: number;
    adAccountId: string;
    syncDate: string;
  },
  accessToken: string,
  adIds: string[]
): Promise<MetaAdsCreativeMap> {
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

    const response = await metaFetchJson<Record<string, { creative?: { id?: string; name?: string } }>>(url, 2, {
      connectionId: audit.connectionId,
      syncJobId: audit.syncJobId,
      transactionSource: 'meta_ads_creatives',
      sourceMetadata: {
        adAccountId: audit.adAccountId,
        syncDate: audit.syncDate,
        adIds: chunk
      }
    });

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
  const normalizedRowsToInsert: MetaAdsPersistableSpendRow[] = [];

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
      const entityId = buildInsightsEntityId(level, row) || null;

      const rawPayloadMetadata = buildRawPayloadStorageMetadata(row);
      const { rawPayloadJson, payloadSizeBytes, payloadHash } = rawPayloadMetadata;

      // docs/raw-payload-persistence-contract.md governs this table: persist the
      // decoded Meta insight row exactly before any normalization or rollup logic.
      const rawInsert = await client.query<{ id: number } & RawPayloadIntegrityRow>(
        `
          INSERT INTO meta_ads_raw_spend_records (
            connection_id,
            sync_job_id,
            report_date,
            level,
            entity_id,
            payload_external_id,
            currency,
            spend,
            impressions,
            clicks,
            raw_payload,
            payload_size_bytes,
            payload_hash,
            updated_at
          )
          VALUES ($1, $2, $3::date, $4, $5, $6, $7, $8::numeric, $9, $10, $11::jsonb, $12, $13, now())
          RETURNING
            id,
            payload_size_bytes AS "storedPayloadSizeBytes",
            payload_hash AS "storedPayloadHash",
            raw_payload AS "persistedRawPayload"
        `,
        [
          params.connectionId,
          params.syncJobId,
          params.syncDate,
          level,
          entityId,
          entityId,
          params.currency,
          parseMetricDecimal(row.spend),
          parseMetricInteger(row.impressions),
          parseMetricInteger(row.clicks),
          rawPayloadJson,
          payloadSizeBytes,
          payloadHash
        ]
      );

      logRawPayloadIntegrityMismatch(
        rawPayloadMetadata,
        rawInsert.rows[0],
        {
          surface: 'meta_ads_raw_spend_records',
          operation: 'insert',
          recordId: rawInsert.rows[0].id,
          fields: {
            level,
            entityId,
            syncJobId: params.syncJobId
          }
        }
      );

      // Raw spend records are the canonical source-payload surface. Projection rows are
      // derived later and are allowed to skip malformed rows that cannot produce entity keys.
      const rawRecordId = rawInsert.rows[0].id;

      if (!entityId) {
        continue;
      }

      const normalizedRows = normalizeInsightRows(row, params.creativeMap, params.currency);

      for (const normalizedRow of normalizedRows) {
        normalizedRowsToInsert.push({
          rawRecordId,
          normalizedRow
        });
      }
    }
  }

  for (const row of rollupPersistableSpendRows(normalizedRowsToInsert)) {
    const normalizedRow = row.normalizedRow;

    // meta_ads_daily_spend is a derived reporting projection, not the canonical raw-source
    // retention surface. It intentionally stores normalized rollups linked back to raw rows.
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
        row.rawRecordId,
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

  await refreshDailyReportingMetrics(client, [params.syncDate]);
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
      // Keep the decoded API responses in audit storage before row-level normalization.
      account: await fetchInsightsForLevel(
        { connectionId: job.connection_id, syncJobId: job.id },
        connection.access_token,
        job.ad_account_id,
        job.sync_date,
        'account'
      ),
      campaign: await fetchInsightsForLevel(
        { connectionId: job.connection_id, syncJobId: job.id },
        connection.access_token,
        job.ad_account_id,
        job.sync_date,
        'campaign'
      ),
      adset: await fetchInsightsForLevel(
        { connectionId: job.connection_id, syncJobId: job.id },
        connection.access_token,
        job.ad_account_id,
        job.sync_date,
        'adset'
      ),
      ad: await fetchInsightsForLevel(
        { connectionId: job.connection_id, syncJobId: job.id },
        connection.access_token,
        job.ad_account_id,
        job.sync_date,
        'ad'
      )
    } satisfies Record<MetaSpendLevel, MetaAdsInsightRow[]>;

    const adIds = [...new Set(rowsByLevel.ad.map((row) => row.ad_id).filter((value): value is string => Boolean(value)))];
    const creativeMap = await fetchCreativeMap(
      {
        connectionId: job.connection_id,
        syncJobId: job.id,
        adAccountId: job.ad_account_id,
        syncDate: job.sync_date
      },
      connection.access_token,
      adIds
    );
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
          account: await fetchInsightsForLevel(
            { connectionId: job.connection_id, syncJobId: job.id },
            connection.access_token,
            job.ad_account_id,
            job.sync_date,
            'account'
          ),
          campaign: await fetchInsightsForLevel(
            { connectionId: job.connection_id, syncJobId: job.id },
            connection.access_token,
            job.ad_account_id,
            job.sync_date,
            'campaign'
          ),
          adset: await fetchInsightsForLevel(
            { connectionId: job.connection_id, syncJobId: job.id },
            connection.access_token,
            job.ad_account_id,
            job.sync_date,
            'adset'
          ),
          ad: await fetchInsightsForLevel(
            { connectionId: job.connection_id, syncJobId: job.id },
            connection.access_token,
            job.ad_account_id,
            job.sync_date,
            'ad'
          )
        } satisfies Record<MetaSpendLevel, MetaAdsInsightRow[]>;

        const adIds = [
          ...new Set(rowsByLevel.ad.map((row) => row.ad_id).filter((value): value is string => Boolean(value)))
        ];
        const creativeMap = await fetchCreativeMap(
          {
            connectionId: job.connection_id,
            syncJobId: job.id,
            adAccountId: job.ad_account_id,
            syncDate: job.sync_date
          },
          connection.access_token,
          adIds
        );

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
      const config = await getResolvedMetaAdsConfig();
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
      const shortLivedToken = await exchangeCodeForAccessToken(config, payload.code);
      const longLivedToken = await exchangeLongLivedAccessToken(config, shortLivedToken.access_token);
      const adAccountId = normalizeMetaAdAccountId(config.adAccountId);
      const account = await fetchMetaAdsAccount(longLivedToken.access_token, adAccountId);

      await upsertMetaAdsConnection({
        adAccountId,
        accessToken: longLivedToken.access_token,
        tokenType: longLivedToken.token_type ?? shortLivedToken.token_type ?? 'Bearer',
        grantedScopes: config.appScopes,
        tokenExpiresAt: calculateTokenExpiresAt(longLivedToken.expires_in),
        account,
        encryptionKey: config.encryptionKey
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

  router.use(attachAuthContext);
  router.use(requireAdmin);

  router.get('/oauth/start', async (req, res, next) => {
    try {
      const config = await getResolvedMetaAdsConfig();
      const payload = oauthStartQuerySchema.parse(req.query);
      const redirectPath = normalizeRedirectPath(payload.redirectPath);
      const state = await insertOAuthState(redirectPath);

      res.status(200).json({
        authorizationUrl: buildMetaAdsAuthorizationUrl(config, state),
        redirectUri: buildMetaAdsRedirectUri(config),
        state
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/status', async (_req, res, next) => {
    try {
      const config = await getMetaAdsConfigurationSummary();
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
        config,
        connection: result.rows[0] ?? null
      });
    } catch (error) {
      next(error);
    }
  });

  router.put('/config', async (req, res, next) => {
    try {
      const payload = metaAdsConfigUpdateSchema.parse(req.body);
      await upsertMetaAdsSettings(payload);

      res.status(200).json({
        ok: true,
        config: await getMetaAdsConfigurationSummary()
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
  buildIncrementalPlanningDates,
  listDateRangeInclusive,
  normalizeInsightRows,
  rollupNormalizedSpendRows,
  rollupPersistableSpendRows
};
