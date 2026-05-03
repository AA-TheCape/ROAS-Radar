import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { emitCampaignMetadataSyncJobLifecycleLog } from '../../observability/index.js';

type GoogleMetadataEntityType = 'campaign' | 'adset' | 'ad';

type GoogleMetadataRecord = {
  platform: 'google_ads';
  accountId: string;
  entityType: GoogleMetadataEntityType;
  entityId: string;
  latestName: string | null;
  lastSeenAt: Date;
};

const GOOGLE_METADATA_ENTITY_ORDER: Record<GoogleMetadataEntityType, number> = {
  campaign: 0,
  adset: 1,
  ad: 2
};

type GoogleAdsConnection = {
  id: number;
  customer_id: string;
  login_customer_id: string | null;
  developer_token: string;
  client_id: string;
  client_secret: string;
  refresh_token: string;
};

type GoogleAdsApiErrorShape = {
  error?: {
    code?: number;
    message?: string;
    status?: string;
    details?: Array<{
      errors?: Array<{
        details?: {
          quotaErrorDetails?: {
            retryDelay?: string;
          };
        };
      }>;
    }>;
  };
};

const legacyGoogleAdsModule = (await import(
  pathToFileURL(resolve(process.cwd(), 'dist/modules/google-ads/index.js')).href
)) as {
  createGoogleAdsRouter?: () => unknown;
  processGoogleAdsSyncQueue: (options?: Record<string, unknown>) => Promise<Record<string, unknown>>;
  __googleAdsTestUtils?: Record<string, unknown>;
};

export const createGoogleAdsRouter =
  legacyGoogleAdsModule.createGoogleAdsRouter ??
  (() => {
    throw new Error('Legacy Google Ads router is unavailable');
  });

function normalizeString(value: string | null | undefined): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function collapseWhitespace(value: string | null | undefined): string | null {
  const normalized = normalizeString(value);
  return normalized ? normalized.replace(/\s+/g, ' ') : null;
}

export function normalizeGoogleAdsCustomerId(value: string): string {
  return value.replace(/-/g, '').trim();
}

export function computeRetryDelaySeconds(attempts: number): number {
  const safeAttempts = Math.max(1, attempts);
  return Math.min(60 * 2 ** (safeAttempts - 1), 60 * 60);
}

function formatDateOnly(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function addDays(date: Date, days: number): Date {
  return new Date(date.getTime() + days * 86_400_000);
}

function listDateRangeInclusive(startDate: string, endDate: string): string[] {
  const start = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  const dates: string[] = [];

  for (let cursor = start; cursor.getTime() <= end.getTime(); cursor = addDays(cursor, 1)) {
    dates.push(formatDateOnly(cursor));
  }

  return dates;
}

export function buildPlanningDates(now = new Date(), _lastSyncCompletedAt: Date | null = null): string[] {
  return [formatDateOnly(now)];
}

export function buildReconciliationWindow(
  now = new Date(),
  lastSyncCompletedAt: Date | null = null
): {
  startDate: string;
  endDate: string;
  dates: string[];
} {
  const dates = buildPlanningDates(now, lastSyncCompletedAt);
  return {
    startDate: dates[0],
    endDate: dates[dates.length - 1],
    dates
  };
}

export function buildGoogleAdsMetadataRecords(input: {
  accountId: string;
  observedAt: Date;
  campaignRows: Array<{ campaign?: { id?: string; name?: string } }>;
  adsetRows: Array<{ adGroup?: { id?: string; name?: string } }>;
  adRows: Array<{ adGroupAd?: { ad?: { id?: string; name?: string; resourceName?: string } } }>;
}): GoogleMetadataRecord[] {
  const accountId = normalizeString(input.accountId);

  if (!accountId) {
    return [];
  }

  const records = new Map<string, GoogleMetadataRecord>();
  const upsert = (entityType: GoogleMetadataEntityType, entityId: string | undefined, latestName: string | null) => {
    const normalizedEntityId = normalizeString(entityId);

    if (!normalizedEntityId) {
      return;
    }

    const key = `${entityType}\u0000${normalizedEntityId}`;
    const existing = records.get(key);

    if (!existing) {
      records.set(key, {
        platform: 'google_ads',
        accountId,
        entityType,
        entityId: normalizedEntityId,
        latestName,
        lastSeenAt: input.observedAt
      });
      return;
    }

    if (latestName) {
      existing.latestName = latestName;
    }

    existing.lastSeenAt = input.observedAt;
  };

  for (const row of input.campaignRows) {
    upsert('campaign', row.campaign?.id, collapseWhitespace(row.campaign?.name));
  }

  for (const row of input.adsetRows) {
    upsert('adset', row.adGroup?.id, collapseWhitespace(row.adGroup?.name));
  }

  for (const row of input.adRows) {
    upsert('ad', row.adGroupAd?.ad?.id, collapseWhitespace(row.adGroupAd?.ad?.name));
  }

  return [...records.values()].sort(
    (left, right) =>
      GOOGLE_METADATA_ENTITY_ORDER[left.entityType] - GOOGLE_METADATA_ENTITY_ORDER[right.entityType] ||
      left.entityId.localeCompare(right.entityId)
  );
}

async function loadActiveGoogleAdsConnections(): Promise<GoogleAdsConnection[]> {
  const result = await query<GoogleAdsConnection>(
    `
      SELECT
        id,
        customer_id,
        login_customer_id,
        pgp_sym_decrypt(developer_token_encrypted, $1) AS developer_token,
        client_id,
        pgp_sym_decrypt(client_secret_encrypted, $1) AS client_secret,
        pgp_sym_decrypt(refresh_token_encrypted, $1) AS refresh_token
      FROM google_ads_connections
      WHERE status = 'active'
      ORDER BY id ASC
    `,
    [env.GOOGLE_ADS_ENCRYPTION_KEY]
  );

  return result.rows;
}

async function acquireMetadataRefreshLock(platform: string, accountId: string): Promise<boolean> {
  const result = await query<{ acquired: boolean }>(
    `SELECT pg_try_advisory_lock(hashtext($1), hashtext($2)) AS acquired`,
    [platform, accountId]
  );

  return Boolean(result.rows[0]?.acquired);
}

async function releaseMetadataRefreshLock(platform: string, accountId: string): Promise<void> {
  await query(`SELECT pg_advisory_unlock(hashtext($1), hashtext($2))`, [platform, accountId]);
}

async function fetchGoogleAccessToken(connection: GoogleAdsConnection): Promise<string> {
  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams({
      client_id: connection.client_id,
      client_secret: connection.client_secret,
      refresh_token: connection.refresh_token,
      grant_type: 'refresh_token'
    })
  });

  if (!response.ok) {
    throw new Error(`Google OAuth token refresh failed with status ${response.status}`);
  }

  const payload = (await response.json()) as { access_token?: string };

  if (!payload.access_token) {
    throw new Error('Google OAuth token refresh response did not include access_token');
  }

  return payload.access_token;
}

async function runGoogleAdsSearchStream(
  connection: GoogleAdsConnection,
  accessToken: string,
  queryText: string
): Promise<Array<Record<string, unknown>>> {
  const response = await fetch(
    `https://googleads.googleapis.com/v22/customers/${connection.customer_id}/googleAds:searchStream`,
    {
      method: 'POST',
      headers: {
        authorization: `Bearer ${accessToken}`,
        'content-type': 'application/json',
        'developer-token': connection.developer_token,
        ...(connection.login_customer_id ? { 'login-customer-id': connection.login_customer_id } : {})
      },
      body: JSON.stringify({ query: queryText })
    }
  );

  if (!response.ok) {
    throw createGoogleAdsApiErrorForTest(response.status, 'Google Ads API request failed', await response.json());
  }

  const payload = (await response.json()) as Array<{ results?: Array<Record<string, unknown>> }>;
  return payload.flatMap((chunk) => chunk.results ?? []);
}

async function upsertGoogleMetadataRecords(records: GoogleMetadataRecord[]): Promise<void> {
  for (const record of records) {
    if (!record.latestName) {
      continue;
    }

    await query(
      `
        INSERT INTO ad_platform_entity_metadata (
          platform,
          account_id,
          entity_type,
          entity_id,
          latest_name,
          last_seen_at,
          updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, now())
        ON CONFLICT (
          platform,
          account_id,
          entity_type,
          entity_id,
          COALESCE(tenant_id, ''),
          COALESCE(workspace_id, '')
        )
        DO UPDATE
        SET
          latest_name = CASE
            WHEN EXCLUDED.latest_name <> '' THEN EXCLUDED.latest_name
            ELSE ad_platform_entity_metadata.latest_name
          END,
          last_seen_at = GREATEST(ad_platform_entity_metadata.last_seen_at, EXCLUDED.last_seen_at),
          updated_at = now()
      `,
      [record.platform, record.accountId, record.entityType, record.entityId, record.latestName, record.lastSeenAt]
    );
  }
}

export async function refreshGoogleAdsMetadataForConnection(
  connection: GoogleAdsConnection,
  now = new Date(),
  workerId = 'google-ads-metadata-refresh',
  requestedBy?: string
): Promise<{ skipped: boolean; recordCount: number }> {
  const acquired = await acquireMetadataRefreshLock('google_ads', connection.customer_id);

  if (!acquired) {
    return { skipped: true, recordCount: 0 };
  }

  const startedAt = new Date();
  emitCampaignMetadataSyncJobLifecycleLog({
    stage: 'started',
    platform: 'google_ads',
    workerId,
    jobId: String(connection.id),
    requestedBy,
    startedAt: startedAt.toISOString()
  });

  try {
    const accessToken = await fetchGoogleAccessToken(connection);
    const [campaignRows, adsetRows, adRows] = await Promise.all([
      runGoogleAdsSearchStream(
        connection,
        accessToken,
        'SELECT campaign.id, campaign.name FROM campaign WHERE campaign.id IS NOT NULL'
      ),
      runGoogleAdsSearchStream(
        connection,
        accessToken,
        'SELECT ad_group.id, ad_group.name FROM ad_group WHERE ad_group.id IS NOT NULL'
      ),
      runGoogleAdsSearchStream(
        connection,
        accessToken,
        'SELECT ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.resource_name FROM ad_group_ad WHERE ad_group_ad.ad.id IS NOT NULL'
      )
    ]);

    const records = buildGoogleAdsMetadataRecords({
      accountId: connection.customer_id,
      observedAt: now,
      campaignRows: campaignRows as Array<{ campaign?: { id?: string; name?: string } }>,
      adsetRows: adsetRows as Array<{ adGroup?: { id?: string; name?: string } }>,
      adRows: adRows as Array<{ adGroupAd?: { ad?: { id?: string; name?: string; resourceName?: string } } }>
    });

    await upsertGoogleMetadataRecords(records);

    emitCampaignMetadataSyncJobLifecycleLog({
      stage: 'completed',
      platform: 'google_ads',
      workerId,
      jobId: String(connection.id),
      requestedBy,
      startedAt: startedAt.toISOString(),
      completedAt: new Date().toISOString()
    });

    return { skipped: false, recordCount: records.length };
  } catch (error) {
    emitCampaignMetadataSyncJobLifecycleLog({
      stage: 'failed',
      platform: 'google_ads',
      workerId,
      jobId: String(connection.id),
      requestedBy,
      startedAt: startedAt.toISOString(),
      completedAt: new Date().toISOString(),
      error
    });
    throw error;
  } finally {
    await releaseMetadataRefreshLock('google_ads', connection.customer_id);
  }
}

export async function refreshActiveGoogleAdsMetadataConnections(options?: {
  now?: Date;
  workerId?: string;
  requestedBy?: string;
}): Promise<{ attempted: number; refreshed: number; skipped: number }> {
  const connections = await loadActiveGoogleAdsConnections();
  let refreshed = 0;
  let skipped = 0;

  for (const connection of connections) {
    const result = await refreshGoogleAdsMetadataForConnection(
      connection,
      options?.now ?? new Date(),
      options?.workerId ?? 'google-ads-metadata-refresh',
      options?.requestedBy
    );

    if (result.skipped) {
      skipped += 1;
    } else {
      refreshed += 1;
    }
  }

  return {
    attempted: connections.length,
    refreshed,
    skipped
  };
}

export async function processGoogleAdsSyncQueue(
  options?: Record<string, unknown>
): Promise<Record<string, unknown> & { metadataRefresh?: { attempted: number; refreshed: number; skipped: number } }> {
  const result = (await legacyGoogleAdsModule.processGoogleAdsSyncQueue(options)) as Record<string, unknown>;
  const metadataRefresh = await refreshActiveGoogleAdsMetadataConnections({
    now: options?.now instanceof Date ? options.now : new Date(),
    workerId: typeof options?.workerId === 'string' ? options.workerId : 'google-ads-worker'
  });

  return {
    ...result,
    metadataRefresh
  };
}

export function createGoogleAdsApiErrorForTest(
  statusCode: number,
  message: string,
  details: GoogleAdsApiErrorShape
): Error & { statusCode: number; details: GoogleAdsApiErrorShape } {
  const error = new Error(message) as Error & {
    statusCode: number;
    details: GoogleAdsApiErrorShape;
  };
  error.name = 'GoogleAdsApiError';
  error.statusCode = statusCode;
  error.details = details;
  return error;
}

export function formatGoogleAdsError(error: unknown): string {
  if (!(error instanceof Error) || !('statusCode' in error)) {
    return error instanceof Error ? error.message : String(error);
  }

  const statusCode = typeof error.statusCode === 'number' ? error.statusCode : null;
  const details = 'details' in error ? JSON.stringify(error.details) : null;

  return `${error.message}${statusCode ? ` (status ${statusCode}` : ''}${details ? `; details=${details}` : ''}${
    statusCode ? ')' : ''
  }`;
}

export function extractGoogleAdsProviderRetryDelaySeconds(errors: GoogleAdsApiErrorShape[]): number | null {
  for (const error of errors) {
    for (const detail of error.error?.details ?? []) {
      for (const entry of detail.errors ?? []) {
        const retryDelay = entry.details?.quotaErrorDetails?.retryDelay;

        if (retryDelay && /^\d+s$/.test(retryDelay)) {
          return Number.parseInt(retryDelay.slice(0, -1), 10);
        }
      }
    }
  }

  return null;
}

export const __googleAdsTestUtils = {
  ...(legacyGoogleAdsModule.__googleAdsTestUtils ?? {}),
  normalizeGoogleAdsCustomerId,
  computeRetryDelaySeconds,
  buildPlanningDates,
  buildReconciliationWindow,
  buildGoogleAdsMetadataRecords,
  createGoogleAdsApiErrorForTest,
  formatGoogleAdsError,
  extractGoogleAdsProviderRetryDelaySeconds
};
