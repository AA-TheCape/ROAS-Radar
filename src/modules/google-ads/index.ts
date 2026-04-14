import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

import { Router } from 'express';
import { type PoolClient } from 'pg';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { buildCanonicalSpendDimensions } from '../marketing-dimensions/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';

const GOOGLE_OAUTH_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_ADS_API_BASE_URL = 'https://googleads.googleapis.com';
const GOOGLE_ADS_SYNC_JOB_STATUSES = ['pending', 'processing', 'retry', 'completed', 'failed'] as const;
const GOOGLE_ADS_SPEND_GRANULARITIES = ['account', 'campaign', 'adset', 'ad', 'creative'] as const;

const connectionUpsertSchema = z.object({
  customerId: z.string().min(1),
  loginCustomerId: z.string().optional().nullable(),
  developerToken: z.string().min(1),
  clientId: z.string().min(1),
  clientSecret: z.string().min(1),
  refreshToken: z.string().min(1)
});

const manualSyncSchema = z.object({
  startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  endDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/)
});

type GoogleAdsSpendGranularity = (typeof GOOGLE_ADS_SPEND_GRANULARITIES)[number];

type GoogleAdsConnectionSecretRow = {
  id: number;
  customer_id: string;
  login_customer_id: string | null;
  developer_token: string;
  client_id: string;
  client_secret: string;
  refresh_token: string;
  token_scopes: string[];
  last_refreshed_at: Date | null;
  status: string;
  customer_descriptive_name: string | null;
  currency_code: string | null;
};

type GoogleAdsConnectionSummaryRow = {
  id: number;
  customer_id: string;
  login_customer_id: string | null;
  token_scopes: string[];
  last_refreshed_at: Date | null;
  last_sync_started_at: Date | null;
  last_sync_completed_at: Date | null;
  last_sync_status: string;
  last_sync_error: string | null;
  status: string;
  customer_descriptive_name: string | null;
  currency_code: string | null;
};

type GoogleAdsSyncJobRow = {
  id: number;
  connection_id: number;
  customer_id: string;
  sync_date: string;
  attempts: number;
};

type GoogleAdsReconciliationRow = {
  checked_range_start: string;
  checked_range_end: string;
  missing_dates: string[];
  enqueued_jobs: number;
  status: string;
  checked_at: Date;
};

type GoogleAdsAccessTokenResponse = {
  access_token?: string;
  expires_in?: number;
  scope?: string;
  token_type?: string;
  error?: string;
  error_description?: string;
};

type GoogleAdsCustomerMetadata = {
  customerId: string;
  descriptiveName: string | null;
  currencyCode: string | null;
  rawPayload: Record<string, unknown>;
};

type GoogleAdsCampaignApiRow = {
  customer?: {
    id?: string;
    descriptiveName?: string;
    currencyCode?: string;
  };
  campaign?: {
    id?: string;
    name?: string;
  };
  metrics?: {
    costMicros?: string;
    impressions?: string;
    clicks?: string;
  };
  segments?: {
    date?: string;
  };
};

type GoogleAdsAdApiRow = {
  customer?: {
    id?: string;
    descriptiveName?: string;
    currencyCode?: string;
  };
  campaign?: {
    id?: string;
    name?: string;
  };
  adGroup?: {
    id?: string;
    name?: string;
  };
  adGroupAd?: {
    ad?: {
      id?: string;
      name?: string;
      resourceName?: string;
    };
  };
  metrics?: {
    costMicros?: string;
    impressions?: string;
    clicks?: string;
  };
  segments?: {
    date?: string;
  };
};

type GoogleAdsNormalizedSpendRow = {
  granularity: GoogleAdsSpendGranularity;
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

export type GoogleAdsQueueProcessOptions = {
  limit?: number;
  workerId?: string;
  emitMetrics?: boolean;
  now?: Date;
};

export type GoogleAdsQueueProcessResult = {
  workerId: string;
  enqueuedJobs: number;
  claimedJobs: number;
  succeededJobs: number;
  failedJobs: number;
  durationMs: number;
};

class GoogleAdsHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'GoogleAdsHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

class GoogleAdsApiError extends Error {
  statusCode: number;
  details: unknown;

  constructor(statusCode: number, message: string, details: unknown) {
    super(message);
    this.name = 'GoogleAdsApiError';
    this.statusCode = statusCode;
    this.details = details;
  }
}

function requireInternalAuth(authHeader: string | undefined): boolean {
  return authHeader === `Bearer ${env.REPORTING_API_TOKEN}`;
}

function assertGoogleAdsConfig(): void {
  if (!env.GOOGLE_ADS_ENCRYPTION_KEY) {
    throw new GoogleAdsHttpError(500, 'google_ads_config_missing', 'Missing Google Ads configuration: GOOGLE_ADS_ENCRYPTION_KEY');
  }
}

function normalizeGoogleAdsCustomerId(value: string): string {
  const normalized = value.replace(/-/g, '').trim();

  if (!/^\d+$/.test(normalized)) {
    throw new GoogleAdsHttpError(400, 'invalid_google_ads_customer_id', 'Google Ads customer ids must contain digits only');
  }

  return normalized;
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
    throw new GoogleAdsHttpError(400, 'invalid_date_range', 'startDate must be on or before endDate');
  }

  const dates: string[] = [];

  for (let cursor = start; cursor.getTime() <= end.getTime(); cursor = new Date(cursor.getTime() + 24 * 60 * 60 * 1000)) {
    dates.push(formatDateOnly(cursor));
  }

  return dates;
}

function buildPlanningDates(now = new Date(), lastSyncCompletedAt: Date | null = null): string[] {
  const yesterday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
  const lookbackDays = lastSyncCompletedAt
    ? env.GOOGLE_ADS_SYNC_LOOKBACK_DAYS
    : env.GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS;
  const firstDate = new Date(yesterday.getTime() - (lookbackDays - 1) * 24 * 60 * 60 * 1000);

  if (yesterday.getTime() < firstDate.getTime()) {
    return [];
  }

  return listDateRangeInclusive(formatDateOnly(firstDate), formatDateOnly(yesterday));
}

function buildReconciliationWindow(now = new Date(), lastSyncCompletedAt: Date | null = null): {
  startDate: string;
  endDate: string;
  dates: string[];
} | null {
  const dates = buildPlanningDates(now, lastSyncCompletedAt);

  if (dates.length === 0) {
    return null;
  }

  return {
    startDate: dates[0],
    endDate: dates[dates.length - 1],
    dates
  };
}

function computeRetryDelaySeconds(attempts: number): number {
  const safeAttempts = Math.max(1, attempts);
  return Math.min(60 * 2 ** (safeAttempts - 1), 60 * 60);
}

function parseMetricInteger(value: string | undefined): number {
  const parsed = Number.parseInt(value ?? '0', 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseMicrosToDecimal(value: string | undefined): string {
  const parsed = Number.parseFloat(value ?? '0');

  if (!Number.isFinite(parsed)) {
    return '0.00';
  }

  return (parsed / 1_000_000).toFixed(2);
}

function buildGoogleAdsLog(event: string, payload: Record<string, unknown>): string {
  return JSON.stringify({
    event,
    ...payload
  });
}

function buildAccessTokenRequestBody(connection: GoogleAdsConnectionSecretRow): URLSearchParams {
  const body = new URLSearchParams();
  body.set('client_id', connection.client_id);
  body.set('client_secret', connection.client_secret);
  body.set('refresh_token', connection.refresh_token);
  body.set('grant_type', 'refresh_token');
  return body;
}

async function exchangeRefreshToken(connection: GoogleAdsConnectionSecretRow): Promise<string> {
  const response = await fetch(GOOGLE_OAUTH_TOKEN_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: buildAccessTokenRequestBody(connection)
  });
  const text = await response.text();
  const payload = text ? (JSON.parse(text) as GoogleAdsAccessTokenResponse) : {};

  if (!response.ok || !payload.access_token) {
    throw new GoogleAdsApiError(
      response.status,
      payload.error_description ?? payload.error ?? 'Google OAuth token exchange failed',
      payload
    );
  }

  await query('UPDATE google_ads_connections SET last_refreshed_at = now(), updated_at = now() WHERE id = $1', [connection.id]);
  return payload.access_token;
}

async function googleAdsSearch<T>(params: {
  connection: GoogleAdsConnectionSecretRow;
  accessToken: string;
  gaql: string;
}): Promise<T[]> {
  const url = new URL(
    `${GOOGLE_ADS_API_BASE_URL}/${env.GOOGLE_ADS_API_VERSION}/customers/${params.connection.customer_id}/googleAds:searchStream`
  );
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${params.accessToken}`,
      'content-type': 'application/json',
      'developer-token': params.connection.developer_token,
      ...(params.connection.login_customer_id ? { 'login-customer-id': params.connection.login_customer_id } : {})
    },
    body: JSON.stringify({
      query: params.gaql
    })
  });
  const text = await response.text();
  const payload = text ? (JSON.parse(text) as Array<{ results?: T[] }> | Record<string, unknown>) : [];

  if (!response.ok) {
    const details = payload;
    throw new GoogleAdsApiError(response.status, 'Google Ads API request failed', details);
  }

  if (!Array.isArray(payload)) {
    return [];
  }

  return payload.flatMap((batch) => batch.results ?? []);
}

async function fetchCustomerMetadata(
  connection: GoogleAdsConnectionSecretRow,
  accessToken: string
): Promise<GoogleAdsCustomerMetadata> {
  const rows = await googleAdsSearch<{
    customer?: {
      id?: string;
      descriptiveName?: string;
      currencyCode?: string;
    };
  }>({
    connection,
    accessToken,
    gaql: 'SELECT customer.id, customer.descriptive_name, customer.currency_code FROM customer LIMIT 1'
  });

  const row = rows[0]?.customer;

  if (!row?.id) {
    throw new GoogleAdsHttpError(400, 'google_ads_customer_not_found', 'Unable to resolve the Google Ads customer');
  }

  return {
    customerId: normalizeGoogleAdsCustomerId(row.id),
    descriptiveName: row.descriptiveName ?? null,
    currencyCode: row.currencyCode ?? null,
    rawPayload: (rows[0] as Record<string, unknown>) ?? {}
  };
}

function buildCampaignSpendQuery(syncDate: string): string {
  return `
    SELECT
      customer.id,
      customer.descriptive_name,
      customer.currency_code,
      campaign.id,
      campaign.name,
      metrics.cost_micros,
      metrics.impressions,
      metrics.clicks,
      segments.date
    FROM campaign
    WHERE segments.date = '${syncDate}'
  `;
}

function buildAdSpendQuery(syncDate: string): string {
  return `
    SELECT
      customer.id,
      customer.descriptive_name,
      customer.currency_code,
      campaign.id,
      campaign.name,
      ad_group.id,
      ad_group.name,
      ad_group_ad.ad.id,
      ad_group_ad.ad.name,
      ad_group_ad.ad.resource_name,
      metrics.cost_micros,
      metrics.impressions,
      metrics.clicks,
      segments.date
    FROM ad_group_ad
    WHERE segments.date = '${syncDate}'
  `;
}

async function fetchCampaignSpendRows(
  connection: GoogleAdsConnectionSecretRow,
  accessToken: string,
  syncDate: string
): Promise<GoogleAdsCampaignApiRow[]> {
  return googleAdsSearch<GoogleAdsCampaignApiRow>({
    connection,
    accessToken,
    gaql: buildCampaignSpendQuery(syncDate)
  });
}

async function fetchAdSpendRows(
  connection: GoogleAdsConnectionSecretRow,
  accessToken: string,
  syncDate: string
): Promise<GoogleAdsAdApiRow[]> {
  return googleAdsSearch<GoogleAdsAdApiRow>({
    connection,
    accessToken,
    gaql: buildAdSpendQuery(syncDate)
  });
}

function normalizeSpendSnapshot(params: {
  customer: GoogleAdsCustomerMetadata;
  campaignRows: GoogleAdsCampaignApiRow[];
  adRows: GoogleAdsAdApiRow[];
}): GoogleAdsNormalizedSpendRow[] {
  const rollup = new Map<string, GoogleAdsNormalizedSpendRow>();

  const upsertRow = (row: GoogleAdsNormalizedSpendRow) => {
    const mapKey = `${row.granularity}:${row.entityKey}`;
    const existing = rollup.get(mapKey);

    if (!existing) {
      rollup.set(mapKey, { ...row });
      return;
    }

    existing.spend = (Number(existing.spend) + Number(row.spend)).toFixed(2);
    existing.impressions += row.impressions;
    existing.clicks += row.clicks;
  };

  const campaignSourceRows = params.campaignRows.length > 0 ? params.campaignRows : params.adRows;
  const accountSpend = campaignSourceRows.reduce((total, row) => total + Number(parseMicrosToDecimal(row.metrics?.costMicros)), 0);
  const accountImpressions = campaignSourceRows.reduce((total, row) => total + parseMetricInteger(row.metrics?.impressions), 0);
  const accountClicks = campaignSourceRows.reduce((total, row) => total + parseMetricInteger(row.metrics?.clicks), 0);

  upsertRow({
    granularity: 'account',
    entityKey: params.customer.customerId,
    accountId: params.customer.customerId,
    accountName: params.customer.descriptiveName,
    campaignId: null,
    campaignName: null,
    adsetId: null,
    adsetName: null,
    adId: null,
    adName: null,
    creativeId: null,
    creativeName: null,
    canonicalSource: 'google',
    canonicalMedium: 'cpc',
    canonicalCampaign: 'unknown',
    canonicalContent: 'unknown',
    canonicalTerm: 'unknown',
    currency: params.customer.currencyCode,
    spend: accountSpend.toFixed(2),
    impressions: accountImpressions,
    clicks: accountClicks,
    rawPayload: {
      source: 'google_ads_campaign_rollup',
      rowCount: campaignSourceRows.length
    }
  });

  for (const row of params.campaignRows) {
    if (!row.campaign?.id) {
      continue;
    }

    const campaignDimensions = buildCanonicalSpendDimensions({
      source: 'google',
      medium: 'cpc',
      campaign: row.campaign.name ?? null,
      content: null,
      term: null
    });

    upsertRow({
      granularity: 'campaign',
      entityKey: row.campaign.id,
      accountId: params.customer.customerId,
      accountName: row.customer?.descriptiveName ?? params.customer.descriptiveName,
      campaignId: row.campaign.id,
      campaignName: row.campaign.name ?? null,
      adsetId: null,
      adsetName: null,
      adId: null,
      adName: null,
      creativeId: null,
      creativeName: null,
      canonicalSource: campaignDimensions.source,
      canonicalMedium: campaignDimensions.medium,
      canonicalCampaign: campaignDimensions.campaign,
      canonicalContent: campaignDimensions.content,
      canonicalTerm: campaignDimensions.term,
      currency: row.customer?.currencyCode ?? params.customer.currencyCode,
      spend: parseMicrosToDecimal(row.metrics?.costMicros),
      impressions: parseMetricInteger(row.metrics?.impressions),
      clicks: parseMetricInteger(row.metrics?.clicks),
      rawPayload: row as Record<string, unknown>
    });
  }

  for (const row of params.adRows) {
    const adGroupId = row.adGroup?.id ?? null;
    const adId = row.adGroupAd?.ad?.id ?? null;
    const adName = row.adGroupAd?.ad?.name ?? row.adGroupAd?.ad?.resourceName ?? null;
    const spend = parseMicrosToDecimal(row.metrics?.costMicros);
    const impressions = parseMetricInteger(row.metrics?.impressions);
    const clicks = parseMetricInteger(row.metrics?.clicks);
    const currency = row.customer?.currencyCode ?? params.customer.currencyCode;
    const accountName = row.customer?.descriptiveName ?? params.customer.descriptiveName;
    const adsetDimensions = buildCanonicalSpendDimensions({
      source: 'google',
      medium: 'cpc',
      campaign: row.campaign?.name ?? null,
      content: null,
      term: null
    });
    const adDimensions = buildCanonicalSpendDimensions({
      source: 'google',
      medium: 'cpc',
      campaign: row.campaign?.name ?? null,
      content: adName,
      term: null
    });

    if (adGroupId) {
      upsertRow({
        granularity: 'adset',
        entityKey: adGroupId,
        accountId: params.customer.customerId,
        accountName,
        campaignId: row.campaign?.id ?? null,
        campaignName: row.campaign?.name ?? null,
        adsetId: adGroupId,
        adsetName: row.adGroup?.name ?? null,
        adId: null,
        adName: null,
        creativeId: null,
        creativeName: null,
        canonicalSource: adsetDimensions.source,
        canonicalMedium: adsetDimensions.medium,
        canonicalCampaign: adsetDimensions.campaign,
        canonicalContent: adsetDimensions.content,
        canonicalTerm: adsetDimensions.term,
        currency,
        spend,
        impressions,
        clicks,
        rawPayload: row as Record<string, unknown>
      });
    }

    if (adId) {
      upsertRow({
        granularity: 'ad',
        entityKey: adId,
        accountId: params.customer.customerId,
        accountName,
        campaignId: row.campaign?.id ?? null,
        campaignName: row.campaign?.name ?? null,
        adsetId: adGroupId,
        adsetName: row.adGroup?.name ?? null,
        adId,
        adName,
        creativeId: null,
        creativeName: null,
        canonicalSource: adDimensions.source,
        canonicalMedium: adDimensions.medium,
        canonicalCampaign: adDimensions.campaign,
        canonicalContent: adDimensions.content,
        canonicalTerm: adDimensions.term,
        currency,
        spend,
        impressions,
        clicks,
        rawPayload: row as Record<string, unknown>
      });

      upsertRow({
        granularity: 'creative',
        entityKey: adId,
        accountId: params.customer.customerId,
        accountName,
        campaignId: row.campaign?.id ?? null,
        campaignName: row.campaign?.name ?? null,
        adsetId: adGroupId,
        adsetName: row.adGroup?.name ?? null,
        adId,
        adName,
        creativeId: adId,
        creativeName: adName,
        canonicalSource: adDimensions.source,
        canonicalMedium: adDimensions.medium,
        canonicalCampaign: adDimensions.campaign,
        canonicalContent: adDimensions.content,
        canonicalTerm: adDimensions.term,
        currency,
        spend,
        impressions,
        clicks,
        rawPayload: row as Record<string, unknown>
      });
    }
  }

  return [...rollup.values()];
}

async function upsertGoogleAdsConnection(params: {
  customerId: string;
  loginCustomerId: string | null;
  developerToken: string;
  clientId: string;
  clientSecret: string;
  refreshToken: string;
  customer: GoogleAdsCustomerMetadata;
}): Promise<void> {
  await query(
    `
      INSERT INTO google_ads_connections (
        customer_id,
        login_customer_id,
        developer_token_encrypted,
        client_id,
        client_secret_encrypted,
        refresh_token_encrypted,
        token_scopes,
        last_refreshed_at,
        status,
        customer_descriptive_name,
        currency_code,
        raw_customer_data,
        updated_at
      )
      VALUES (
        $1,
        $2,
        pgp_sym_encrypt($3, $7, 'cipher-algo=aes256, compress-algo=0'),
        $4,
        pgp_sym_encrypt($5, $7, 'cipher-algo=aes256, compress-algo=0'),
        pgp_sym_encrypt($6, $7, 'cipher-algo=aes256, compress-algo=0'),
        ARRAY['https://www.googleapis.com/auth/adwords']::text[],
        now(),
        'active',
        $8,
        $9,
        $10::jsonb,
        now()
      )
      ON CONFLICT (customer_id)
      DO UPDATE SET
        login_customer_id = $2,
        developer_token_encrypted = pgp_sym_encrypt($3, $7, 'cipher-algo=aes256, compress-algo=0'),
        client_id = $4,
        client_secret_encrypted = pgp_sym_encrypt($5, $7, 'cipher-algo=aes256, compress-algo=0'),
        refresh_token_encrypted = pgp_sym_encrypt($6, $7, 'cipher-algo=aes256, compress-algo=0'),
        token_scopes = ARRAY['https://www.googleapis.com/auth/adwords']::text[],
        last_refreshed_at = now(),
        status = 'active',
        customer_descriptive_name = $8,
        currency_code = $9,
        raw_customer_data = $10::jsonb,
        updated_at = now()
    `,
    [
      params.customerId,
      params.loginCustomerId,
      params.developerToken,
      params.clientId,
      params.clientSecret,
      params.refreshToken,
      env.GOOGLE_ADS_ENCRYPTION_KEY,
      params.customer.descriptiveName,
      params.customer.currencyCode,
      JSON.stringify(params.customer.rawPayload)
    ]
  );
}

async function getGoogleAdsConnectionById(connectionId: number): Promise<GoogleAdsConnectionSecretRow> {
  const result = await query<GoogleAdsConnectionSecretRow>(
    `
      SELECT
        id,
        customer_id,
        login_customer_id,
        pgp_sym_decrypt(developer_token_encrypted, $1) AS developer_token,
        client_id,
        pgp_sym_decrypt(client_secret_encrypted, $1) AS client_secret,
        pgp_sym_decrypt(refresh_token_encrypted, $1) AS refresh_token,
        token_scopes,
        last_refreshed_at,
        status,
        customer_descriptive_name,
        currency_code
      FROM google_ads_connections
      WHERE id = $2
        AND status = 'active'
      LIMIT 1
    `,
    [env.GOOGLE_ADS_ENCRYPTION_KEY, connectionId]
  );

  const connection = result.rows[0];

  if (!connection) {
    throw new GoogleAdsHttpError(404, 'google_ads_connection_not_found', 'No active Google Ads connection was found');
  }

  return connection;
}

async function getLatestGoogleAdsConnection(): Promise<GoogleAdsConnectionSecretRow> {
  const result = await query<GoogleAdsConnectionSecretRow>(
    `
      SELECT
        id,
        customer_id,
        login_customer_id,
        pgp_sym_decrypt(developer_token_encrypted, $1) AS developer_token,
        client_id,
        pgp_sym_decrypt(client_secret_encrypted, $1) AS client_secret,
        pgp_sym_decrypt(refresh_token_encrypted, $1) AS refresh_token,
        token_scopes,
        last_refreshed_at,
        status,
        customer_descriptive_name,
        currency_code
      FROM google_ads_connections
      WHERE status = 'active'
      ORDER BY updated_at DESC
      LIMIT 1
    `,
    [env.GOOGLE_ADS_ENCRYPTION_KEY]
  );

  const connection = result.rows[0];

  if (!connection) {
    throw new GoogleAdsHttpError(404, 'google_ads_connection_not_found', 'No active Google Ads connection was found');
  }

  return connection;
}

async function enqueueSyncDates(connectionId: number, dates: string[]): Promise<number> {
  let enqueuedJobs = 0;

  for (const date of dates) {
    await query(
      `
        INSERT INTO google_ads_sync_jobs (connection_id, sync_date, status, available_at, updated_at)
        VALUES ($1, $2::date, 'pending', now(), now())
        ON CONFLICT (connection_id, sync_date)
        DO UPDATE SET
          status = CASE
            WHEN google_ads_sync_jobs.status = 'processing' THEN google_ads_sync_jobs.status
            ELSE 'pending'
          END,
          available_at = CASE
            WHEN google_ads_sync_jobs.status = 'processing' THEN google_ads_sync_jobs.available_at
            ELSE now()
          END,
          last_error = NULL,
          completed_at = CASE
            WHEN google_ads_sync_jobs.status = 'processing' THEN google_ads_sync_jobs.completed_at
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

async function findMissingSyncDates(connectionId: number, dates: string[]): Promise<string[]> {
  if (dates.length === 0) {
    return [];
  }

  const result = await query<{ sync_date: string }>(
    `
      SELECT sync_date::text
      FROM google_ads_sync_jobs
      WHERE connection_id = $1
        AND sync_date = ANY($2::date[])
        AND status = 'completed'
    `,
    [connectionId, dates]
  );

  const completed = new Set(result.rows.map((row) => row.sync_date));
  return dates.filter((date) => !completed.has(date));
}

async function runReconciliation(params: {
  connectionId: number;
  now?: Date;
  lastSyncCompletedAt?: Date | null;
}): Promise<number> {
  const window = buildReconciliationWindow(params.now ?? new Date(), params.lastSyncCompletedAt ?? null);

  if (!window) {
    return 0;
  }

  const missingDates = await findMissingSyncDates(params.connectionId, window.dates);
  const enqueuedJobs = missingDates.length > 0 ? await enqueueSyncDates(params.connectionId, missingDates) : 0;

  await query(
    `
      INSERT INTO google_ads_reconciliation_runs (
        connection_id,
        checked_range_start,
        checked_range_end,
        missing_dates,
        enqueued_jobs,
        status
      )
      VALUES ($1, $2::date, $3::date, $4::jsonb, $5, $6)
    `,
    [
      params.connectionId,
      window.startDate,
      window.endDate,
      JSON.stringify(missingDates),
      enqueuedJobs,
      missingDates.length > 0 ? 'missing_dates' : 'healthy'
    ]
  );

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
      FROM google_ads_connections
      WHERE status = 'active'
    `
  );

  let plannedJobs = 0;
  const today = formatDateOnly(now);

  for (const row of result.rows) {
    if (row.last_sync_planned_for !== today) {
      const dates = buildPlanningDates(now, row.last_sync_completed_at);

      if (dates.length > 0) {
        plannedJobs += await enqueueSyncDates(row.id, dates);
      }

      await query('UPDATE google_ads_connections SET last_sync_planned_for = $2::date, updated_at = now() WHERE id = $1', [
        row.id,
        today
      ]);
    }

    plannedJobs += await runReconciliation({
      connectionId: row.id,
      now,
      lastSyncCompletedAt: row.last_sync_completed_at
    });
  }

  return plannedJobs;
}

async function claimSyncJobs(workerId: string, limit: number): Promise<GoogleAdsSyncJobRow[]> {
  const result = await query<GoogleAdsSyncJobRow>(
    `
      WITH claimable AS (
        SELECT j.id, j.connection_id
        FROM google_ads_sync_jobs j
        WHERE j.status IN ('pending', 'retry')
          AND j.available_at <= now()
        ORDER BY j.sync_date ASC, j.id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE google_ads_sync_jobs j
      SET
        status = 'processing',
        locked_at = now(),
        locked_by = $1,
        attempts = j.attempts + 1,
        updated_at = now()
      FROM claimable
      JOIN google_ads_connections c ON c.id = claimable.connection_id
      WHERE j.id = claimable.id
      RETURNING j.id, j.connection_id, c.customer_id, j.sync_date::text, j.attempts
    `,
    [workerId, limit]
  );

  return result.rows;
}

async function persistDailySpendSnapshot(
  client: PoolClient,
  params: {
    connectionId: number;
    syncJobId: number;
    syncDate: string;
    campaignRows: GoogleAdsCampaignApiRow[];
    adRows: GoogleAdsAdApiRow[];
    normalizedRows: GoogleAdsNormalizedSpendRow[];
  }
): Promise<void> {
  await client.query('DELETE FROM google_ads_daily_spend WHERE connection_id = $1 AND report_date = $2::date', [
    params.connectionId,
    params.syncDate
  ]);
  await client.query('DELETE FROM google_ads_raw_spend_records WHERE connection_id = $1 AND report_date = $2::date', [
    params.connectionId,
    params.syncDate
  ]);

  for (const row of params.campaignRows) {
    const entityId = row.campaign?.id ?? null;

    if (!entityId) {
      continue;
    }

    await client.query(
      `
        INSERT INTO google_ads_raw_spend_records (
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
        VALUES ($1, $2, $3::date, 'campaign', $4, $5, $6::numeric, $7, $8, $9::jsonb, now())
      `,
      [
        params.connectionId,
        params.syncJobId,
        params.syncDate,
        entityId,
        row.customer?.currencyCode ?? null,
        parseMicrosToDecimal(row.metrics?.costMicros),
        parseMetricInteger(row.metrics?.impressions),
        parseMetricInteger(row.metrics?.clicks),
        JSON.stringify(row)
      ]
    );
  }

  const rawRecordIdsByAdId = new Map<string, number>();

  for (const row of params.adRows) {
    const entityId = row.adGroupAd?.ad?.id ?? null;

    if (!entityId) {
      continue;
    }

    const insertResult = await client.query<{ id: number }>(
      `
        INSERT INTO google_ads_raw_spend_records (
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
        VALUES ($1, $2, $3::date, 'ad', $4, $5, $6::numeric, $7, $8, $9::jsonb, now())
        RETURNING id
      `,
      [
        params.connectionId,
        params.syncJobId,
        params.syncDate,
        entityId,
        row.customer?.currencyCode ?? null,
        parseMicrosToDecimal(row.metrics?.costMicros),
        parseMetricInteger(row.metrics?.impressions),
        parseMetricInteger(row.metrics?.clicks),
        JSON.stringify(row)
      ]
    );

    rawRecordIdsByAdId.set(entityId, insertResult.rows[0].id);
  }

  for (const normalizedRow of params.normalizedRows) {
    await client.query(
      `
        INSERT INTO google_ads_daily_spend (
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
        normalizedRow.adId ? rawRecordIdsByAdId.get(normalizedRow.adId) ?? null : null,
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
      UPDATE google_ads_sync_jobs
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
      UPDATE google_ads_connections
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

async function markSyncJobFailed(job: GoogleAdsSyncJobRow, error: unknown): Promise<void> {
  const lastError = error instanceof Error ? error.message : String(error);
  const shouldRetry = job.attempts < env.GOOGLE_ADS_SYNC_MAX_RETRIES;
  const nextStatus = shouldRetry ? 'retry' : 'failed';
  const retryDelaySeconds = computeRetryDelaySeconds(job.attempts);

  await query(
    `
      UPDATE google_ads_sync_jobs
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
      UPDATE google_ads_connections
      SET
        last_sync_status = $2,
        last_sync_error = $3,
        updated_at = now()
      WHERE id = $1
    `,
    [job.connection_id, shouldRetry ? 'retry' : 'failed', lastError]
  );

  process.stderr.write(
    `${buildGoogleAdsLog('google_ads_sync_job_failed', {
      severity: shouldRetry ? 'WARNING' : 'ERROR',
      alertable: !shouldRetry,
      jobId: job.id,
      connectionId: job.connection_id,
      customerId: job.customer_id,
      syncDate: job.sync_date,
      attempts: job.attempts,
      willRetry: shouldRetry,
      retryDelaySeconds: shouldRetry ? retryDelaySeconds : 0,
      error: lastError
    })}\n`
  );
}

async function processSyncJob(job: GoogleAdsSyncJobRow): Promise<void> {
  await query(
    `
      UPDATE google_ads_connections
      SET
        last_sync_started_at = now(),
        last_sync_status = 'running',
        last_sync_error = NULL,
        updated_at = now()
      WHERE id = $1
    `,
    [job.connection_id]
  );

  const attemptJob = async (): Promise<void> => {
    const connection = await getGoogleAdsConnectionById(job.connection_id);
    const accessToken = await exchangeRefreshToken(connection);
    const customer = await fetchCustomerMetadata(connection, accessToken);
    const campaignRows = await fetchCampaignSpendRows(connection, accessToken, job.sync_date);
    const adRows = await fetchAdSpendRows(connection, accessToken, job.sync_date);
    const normalizedRows = normalizeSpendSnapshot({
      customer,
      campaignRows,
      adRows
    });

    await withTransaction(async (client) => {
      await persistDailySpendSnapshot(client, {
        connectionId: job.connection_id,
        syncJobId: job.id,
        syncDate: job.sync_date,
        campaignRows,
        adRows,
        normalizedRows
      });
    });

    await query(
      `
        UPDATE google_ads_connections
        SET
          customer_descriptive_name = $2,
          currency_code = $3,
          raw_customer_data = $4::jsonb,
          updated_at = now()
        WHERE id = $1
      `,
      [job.connection_id, customer.descriptiveName, customer.currencyCode, JSON.stringify(customer.rawPayload)]
    );
  };

  try {
    await attemptJob();
    await markSyncJobSucceeded(job.id, job.connection_id);
  } catch (error) {
    if (error instanceof GoogleAdsApiError && [401, 403, 429, 500, 502, 503, 504].includes(error.statusCode)) {
      try {
        await delay(500);
        await attemptJob();
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

function buildMetricsLog(result: GoogleAdsQueueProcessResult): string {
  return buildGoogleAdsLog('google_ads_sync_run', {
    workerId: result.workerId,
    enqueuedJobs: result.enqueuedJobs,
    claimedJobs: result.claimedJobs,
    succeededJobs: result.succeededJobs,
    failedJobs: result.failedJobs,
    durationMs: result.durationMs
  });
}

export async function processGoogleAdsSyncQueue(
  options: GoogleAdsQueueProcessOptions = {}
): Promise<GoogleAdsQueueProcessResult> {
  const startedAt = Date.now();
  const workerId = options.workerId ?? `google-ads-sync-${randomBytes(6).toString('hex')}`;
  const limit = options.limit ?? env.GOOGLE_ADS_SYNC_BATCH_SIZE;
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

  const result: GoogleAdsQueueProcessResult = {
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

export function createGoogleAdsAdminRouter(): Router {
  const router = Router();

  router.use((req, res, next) => {
    if (!requireInternalAuth(req.header('authorization') ?? undefined)) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }

    next();
  });

  router.post('/connections', async (req, res, next) => {
    try {
      assertGoogleAdsConfig();
      const payload = connectionUpsertSchema.parse(req.body);
      const normalizedCustomerId = normalizeGoogleAdsCustomerId(payload.customerId);
      const loginCustomerId = payload.loginCustomerId ? normalizeGoogleAdsCustomerId(payload.loginCustomerId) : null;
      const connectionForValidation: GoogleAdsConnectionSecretRow = {
        id: 0,
        customer_id: normalizedCustomerId,
        login_customer_id: loginCustomerId,
        developer_token: payload.developerToken.trim(),
        client_id: payload.clientId.trim(),
        client_secret: payload.clientSecret.trim(),
        refresh_token: payload.refreshToken.trim(),
        token_scopes: ['https://www.googleapis.com/auth/adwords'],
        last_refreshed_at: null,
        status: 'active',
        customer_descriptive_name: null,
        currency_code: null
      };
      const accessToken = await exchangeRefreshToken(connectionForValidation);
      const customer = await fetchCustomerMetadata(connectionForValidation, accessToken);

      if (customer.customerId !== normalizedCustomerId) {
        throw new GoogleAdsHttpError(
          400,
          'google_ads_customer_mismatch',
          `Provided customerId ${normalizedCustomerId} does not match resolved customer ${customer.customerId}`
        );
      }

      await upsertGoogleAdsConnection({
        customerId: normalizedCustomerId,
        loginCustomerId,
        developerToken: payload.developerToken.trim(),
        clientId: payload.clientId.trim(),
        clientSecret: payload.clientSecret.trim(),
        refreshToken: payload.refreshToken.trim(),
        customer
      });

      const connection = await getLatestGoogleAdsConnection();
      const initialDates = buildPlanningDates(new Date(), null);
      await enqueueSyncDates(connection.id, initialDates);

      res.status(201).json({
        ok: true,
        customerId: normalizedCustomerId,
        customerName: customer.descriptiveName,
        currencyCode: customer.currencyCode,
        plannedDates: initialDates
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/status', async (_req, res, next) => {
    try {
      const connectionResult = await query<GoogleAdsConnectionSummaryRow>(
        `
          SELECT
            id,
            customer_id,
            login_customer_id,
            token_scopes,
            last_refreshed_at,
            last_sync_started_at,
            last_sync_completed_at,
            last_sync_status,
            last_sync_error,
            status,
            customer_descriptive_name,
            currency_code
          FROM google_ads_connections
          ORDER BY updated_at DESC
          LIMIT 1
        `
      );
      const reconciliationResult = await query<GoogleAdsReconciliationRow>(
        `
          SELECT
            checked_range_start::text,
            checked_range_end::text,
            ARRAY(
              SELECT jsonb_array_elements_text(missing_dates)
            ) AS missing_dates,
            enqueued_jobs,
            status,
            checked_at
          FROM google_ads_reconciliation_runs
          ORDER BY checked_at DESC
          LIMIT 1
        `
      );

      res.status(200).json({
        connection: connectionResult.rows[0] ?? null,
        reconciliation: reconciliationResult.rows[0] ?? null
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/sync', async (req, res, next) => {
    try {
      const payload = manualSyncSchema.parse(req.body);
      const connection = await getLatestGoogleAdsConnection();
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

  router.post('/reconcile', async (_req, res, next) => {
    try {
      const connection = await getLatestGoogleAdsConnection();
      const enqueuedJobs = await runReconciliation({
        connectionId: connection.id
      });

      res.status(200).json({
        ok: true,
        enqueuedJobs
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}

export const __googleAdsTestUtils = {
  normalizeGoogleAdsCustomerId,
  listDateRangeInclusive,
  buildPlanningDates,
  buildReconciliationWindow,
  computeRetryDelaySeconds,
  normalizeSpendSnapshot
};
