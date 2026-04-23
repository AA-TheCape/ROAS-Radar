import { createHash, randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Router } from 'express';
import { z } from 'zod';
import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { attachAuthContext, requireAdmin } from '../auth/index.js';
import { buildCanonicalSpendDimensions } from '../marketing-dimensions/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
const META_OAUTH_STATE_TTL_MINUTES = 10;
const META_GRAPH_BASE_URL = 'https://graph.facebook.com';
const META_SYNC_JOB_STATUSES = ['pending', 'processing', 'retry', 'completed', 'failed'];
const META_SPEND_LEVELS = ['account', 'campaign', 'adset', 'ad'];
const META_SPEND_GRANULARITIES = ['account', 'campaign', 'adset', 'ad', 'creative'];
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
class MetaAdsHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'MetaAdsHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
class MetaAdsApiError extends Error {
    statusCode;
    details;
    constructor(statusCode, message, details = null) {
        super(message);
        this.name = 'MetaAdsApiError';
        this.statusCode = statusCode;
        this.details = details;
    }
}
function normalizeMetaAdsScopes(rawValue) {
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
function normalizeMetaAdAccountId(value) {
    const normalized = value.trim();
    const accountId = normalized.startsWith('act_') ? normalized.slice(4) : normalized;
    if (!/^\d+$/.test(accountId)) {
        throw new MetaAdsHttpError(400, 'invalid_meta_ad_account_id', 'META_ADS_AD_ACCOUNT_ID must be numeric or act_<id>');
    }
    return accountId;
}
function normalizeRedirectPath(rawValue) {
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
async function getStoredMetaAdsSettings() {
    const result = await query(`
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
    `, [env.META_ADS_ENCRYPTION_KEY]);
    return result.rows[0] ?? null;
}
async function getResolvedMetaAdsConfig() {
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
async function getMetaAdsConfigurationSummary() {
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
async function upsertMetaAdsSettings(payload) {
    if (!env.META_ADS_ENCRYPTION_KEY) {
        throw new MetaAdsHttpError(500, 'meta_ads_config_missing', 'Missing Meta Ads configuration: META_ADS_ENCRYPTION_KEY');
    }
    const secretProvided = typeof payload.appSecret === 'string' && payload.appSecret.trim().length > 0;
    const normalizedScopes = normalizeMetaAdsScopes(payload.appScopes);
    const existing = await getStoredMetaAdsSettings();
    const nextSecret = secretProvided ? (payload.appSecret ?? '').trim() : existing?.app_secret ?? '';
    await query(`
      DELETE FROM meta_ads_settings
    `);
    await query(`
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
    `, [
        payload.appId.trim(),
        nextSecret,
        new URL(payload.appBaseUrl).toString().replace(/\/$/, ''),
        normalizedScopes,
        payload.adAccountId.trim(),
        env.META_ADS_ENCRYPTION_KEY
    ]);
}
function getMetaAdsAppBaseUrl(config) {
    return new URL(config.appBaseUrl).toString().replace(/\/$/, '');
}
function buildMetaAdsRedirectUri(config) {
    return `${getMetaAdsAppBaseUrl(config)}/meta-ads/oauth/callback`;
}
function createOAuthStateDigest(state) {
    return createHash('sha256').update(state).digest('hex');
}
function buildMetaAdsAuthorizationUrl(config, state) {
    const url = new URL('https://www.facebook.com/dialog/oauth');
    url.searchParams.set('client_id', config.appId);
    url.searchParams.set('redirect_uri', buildMetaAdsRedirectUri(config));
    url.searchParams.set('state', state);
    url.searchParams.set('scope', config.appScopes.join(','));
    return url.toString();
}
function calculateTokenExpiresAt(expiresInSeconds, now = new Date()) {
    if (!expiresInSeconds || expiresInSeconds <= 0) {
        return null;
    }
    return new Date(now.getTime() + expiresInSeconds * 1000);
}
function computeRetryDelaySeconds(attempts) {
    const safeAttempts = Math.max(1, attempts);
    return Math.min(60 * 2 ** (safeAttempts - 1), 60 * 60);
}
function shouldRefreshToken(tokenExpiresAt, now = new Date()) {
    if (!tokenExpiresAt) {
        return false;
    }
    return tokenExpiresAt.getTime() - now.getTime() <= env.META_ADS_TOKEN_REFRESH_LEEWAY_HOURS * 60 * 60 * 1000;
}
function formatDateOnly(value) {
    return value.toISOString().slice(0, 10);
}
function parseDateOnly(value) {
    return new Date(`${value}T00:00:00.000Z`);
}
function listDateRangeInclusive(startDate, endDate) {
    const start = parseDateOnly(startDate);
    const end = parseDateOnly(endDate);
    if (start.getTime() > end.getTime()) {
        throw new MetaAdsHttpError(400, 'invalid_date_range', 'startDate must be on or before endDate');
    }
    const dates = [];
    for (let cursor = start; cursor.getTime() <= end.getTime(); cursor = new Date(cursor.getTime() + 24 * 60 * 60 * 1000)) {
        dates.push(formatDateOnly(cursor));
    }
    return dates;
}
function buildPlanningDates(now = new Date(), lastSyncCompletedAt = null) {
    const yesterday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
    const lookbackDays = lastSyncCompletedAt ? env.META_ADS_SYNC_LOOKBACK_DAYS : env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS;
    const firstDate = new Date(yesterday.getTime() - (lookbackDays - 1) * 24 * 60 * 60 * 1000);
    if (yesterday.getTime() < firstDate.getTime()) {
        return [];
    }
    return listDateRangeInclusive(formatDateOnly(firstDate), formatDateOnly(yesterday));
}
function buildInsightsEntityId(level, row) {
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
function parseMetricInteger(value) {
    const parsed = Number.parseInt(value ?? '0', 10);
    return Number.isFinite(parsed) ? parsed : 0;
}
function parseMetricDecimal(value) {
    const parsed = Number.parseFloat(value ?? '0');
    return Number.isFinite(parsed) ? parsed.toFixed(2) : '0.00';
}
function normalizeInsightRows(row, creativeMap, currency) {
    const normalized = [];
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
        rawPayload: row
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
function buildMetaLog(event, payload) {
    return JSON.stringify({
        event,
        ...payload
    });
}
async function metaFetchJson(url, retryCount = 2) {
    let lastError;
    for (let attempt = 1; attempt <= retryCount + 1; attempt += 1) {
        try {
            const response = await fetch(url);
            const text = await response.text();
            const json = text ? JSON.parse(text) : {};
            if (!response.ok) {
                const errorBody = json ?? null;
                const errorMessage = errorBody?.error?.message ?? `Meta Ads API request failed with status ${response.status}`;
                throw new MetaAdsApiError(response.status, errorMessage, errorBody);
            }
            return json;
        }
        catch (error) {
            lastError = error;
            if (attempt > retryCount ||
                !(error instanceof MetaAdsApiError) ||
                ![429, 500, 502, 503, 504].includes(error.statusCode)) {
                break;
            }
            await delay(attempt * 500);
        }
    }
    throw lastError;
}
async function exchangeCodeForAccessToken(config, code) {
    const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/oauth/access_token`);
    url.searchParams.set('client_id', config.appId);
    url.searchParams.set('client_secret', config.appSecret);
    url.searchParams.set('redirect_uri', buildMetaAdsRedirectUri(config));
    url.searchParams.set('code', code);
    return metaFetchJson(url);
}
async function exchangeLongLivedAccessToken(config, accessToken) {
    const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/oauth/access_token`);
    url.searchParams.set('grant_type', 'fb_exchange_token');
    url.searchParams.set('client_id', config.appId);
    url.searchParams.set('client_secret', config.appSecret);
    url.searchParams.set('fb_exchange_token', accessToken);
    return metaFetchJson(url);
}
async function fetchMetaAdsAccount(accessToken, adAccountId) {
    const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/act_${adAccountId}`);
    url.searchParams.set('access_token', accessToken);
    url.searchParams.set('fields', 'id,name,currency');
    return metaFetchJson(url);
}
async function insertOAuthState(redirectPath) {
    const state = randomBytes(24).toString('hex');
    const stateDigest = createOAuthStateDigest(state);
    await query(`
      INSERT INTO meta_ads_oauth_states (state_digest, redirect_path, expires_at)
      VALUES ($1, $2, now() + ($3::int * interval '1 minute'))
    `, [stateDigest, redirectPath, META_OAUTH_STATE_TTL_MINUTES]);
    return state;
}
async function consumeOAuthState(state) {
    const result = await query(`
      UPDATE meta_ads_oauth_states
      SET consumed_at = now()
      WHERE state_digest = $1
        AND consumed_at IS NULL
        AND expires_at >= now()
      RETURNING redirect_path
    `, [createOAuthStateDigest(state)]);
    if (!result.rowCount) {
        throw new MetaAdsHttpError(400, 'invalid_meta_oauth_state', 'The Meta Ads OAuth state is invalid or expired');
    }
    return result.rows[0].redirect_path;
}
async function upsertMetaAdsConnection(params) {
    await query(`
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
    `, [
        params.adAccountId,
        params.accessToken,
        params.encryptionKey,
        params.tokenType,
        params.grantedScopes,
        params.tokenExpiresAt,
        params.account.name ?? null,
        params.account.currency ?? params.account.account_currency ?? null,
        JSON.stringify(params.account)
    ]);
}
async function getActiveMetaAdsConnection() {
    const config = await getResolvedMetaAdsConfig();
    const result = await query(`
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
    `, [config.encryptionKey]);
    return result.rows[0] ?? null;
}
async function refreshMetaAdsConnectionToken(connection) {
    const config = await getResolvedMetaAdsConfig();
    const refreshed = await exchangeLongLivedAccessToken(config, connection.access_token);
    const expiresAt = calculateTokenExpiresAt(refreshed.expires_in);
    const accessToken = refreshed.access_token;
    await query(`
      UPDATE meta_ads_connections
      SET
        access_token_encrypted = pgp_sym_encrypt($2, $3, 'cipher-algo=aes256, compress-algo=0'),
        token_type = $4,
        token_expires_at = $5,
        last_refreshed_at = now(),
        updated_at = now()
      WHERE id = $1
    `, [connection.id, accessToken, config.encryptionKey, refreshed.token_type ?? connection.token_type, expiresAt]);
    return {
        ...connection,
        access_token: accessToken,
        token_type: refreshed.token_type ?? connection.token_type,
        token_expires_at: expiresAt,
        last_refreshed_at: new Date()
    };
}
async function getUsableMetaAdsConnection(forceRefresh = false) {
    const connection = await getActiveMetaAdsConnection();
    if (!connection) {
        throw new MetaAdsHttpError(404, 'meta_ads_connection_not_found', 'No active Meta Ads connection was found');
    }
    if (forceRefresh || shouldRefreshToken(connection.token_expires_at)) {
        return refreshMetaAdsConnectionToken(connection);
    }
    return connection;
}
async function enqueueSyncDates(connectionId, dates) {
    let enqueuedJobs = 0;
    for (const date of dates) {
        await query(`
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
      `, [connectionId, date]);
        enqueuedJobs += 1;
    }
    return enqueuedJobs;
}
async function planIncrementalSyncs(now = new Date()) {
    const result = await query(`
      SELECT
        id,
        last_sync_completed_at,
        last_sync_planned_for::text
      FROM meta_ads_connections
      WHERE status = 'active'
    `);
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
async function claimSyncJobs(workerId, limit) {
    const result = await query(`
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
    `, [workerId, limit]);
    return result.rows;
}
async function fetchInsightsForLevel(accessToken, adAccountId, syncDate, level) {
    const rows = [];
    let nextUrl = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/act_${adAccountId}/insights`);
    nextUrl.searchParams.set('fields', 'account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,objective,date_start,date_stop');
    nextUrl.searchParams.set('access_token', accessToken);
    nextUrl.searchParams.set('level', level);
    nextUrl.searchParams.set('time_increment', '1');
    nextUrl.searchParams.set('limit', '500');
    nextUrl.searchParams.set('time_range', JSON.stringify({ since: syncDate, until: syncDate }));
    while (nextUrl) {
        const page = await metaFetchJson(nextUrl);
        rows.push(...(page.data ?? []));
        nextUrl = page.paging?.next ? new URL(page.paging.next) : null;
    }
    return rows.filter((row) => buildInsightsEntityId(level, row));
}
async function fetchCreativeMap(accessToken, adIds) {
    const creativeMap = {};
    for (let index = 0; index < adIds.length; index += 50) {
        const chunk = adIds.slice(index, index + 50);
        if (chunk.length === 0) {
            continue;
        }
        const url = new URL(`${META_GRAPH_BASE_URL}/${env.META_ADS_API_VERSION}/`);
        url.searchParams.set('access_token', accessToken);
        url.searchParams.set('ids', chunk.join(','));
        url.searchParams.set('fields', 'creative{id,name}');
        const response = await metaFetchJson(url);
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
async function persistDailySpendSnapshot(client, params) {
    await client.query('DELETE FROM meta_ads_daily_spend WHERE connection_id = $1 AND report_date = $2::date', [params.connectionId, params.syncDate]);
    await client.query('DELETE FROM meta_ads_raw_spend_records WHERE connection_id = $1 AND report_date = $2::date', [params.connectionId, params.syncDate]);
    for (const level of META_SPEND_LEVELS) {
        for (const row of params.rowsByLevel[level]) {
            const entityId = buildInsightsEntityId(level, row);
            if (!entityId) {
                continue;
            }
            const rawInsert = await client.query(`
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
        `, [
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
            ]);
            const rawRecordId = rawInsert.rows[0].id;
            const normalizedRows = normalizeInsightRows(row, params.creativeMap, params.currency);
            for (const normalizedRow of normalizedRows) {
                await client.query(`
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
          `, [
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
                ]);
            }
        }
    }
    await refreshDailyReportingMetrics(client, [params.syncDate]);
}
async function markSyncJobSucceeded(jobId, connectionId) {
    await query(`
      UPDATE meta_ads_sync_jobs
      SET
        status = 'completed',
        completed_at = now(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        updated_at = now()
      WHERE id = $1
    `, [jobId]);
    await query(`
      UPDATE meta_ads_connections
      SET
        last_sync_completed_at = now(),
        last_sync_status = 'succeeded',
        last_sync_error = NULL,
        updated_at = now()
      WHERE id = $1
    `, [connectionId]);
}
async function markSyncJobFailed(job, error) {
    const lastError = error instanceof Error ? error.message : String(error);
    const shouldRetry = job.attempts < env.META_ADS_SYNC_MAX_RETRIES;
    const nextStatus = shouldRetry ? 'retry' : 'failed';
    const retryDelaySeconds = computeRetryDelaySeconds(job.attempts);
    await query(`
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
    `, [job.id, nextStatus, retryDelaySeconds, lastError]);
    await query(`
      UPDATE meta_ads_connections
      SET
        last_sync_status = $2,
        last_sync_error = $3,
        updated_at = now()
      WHERE id = $1
    `, [job.connection_id, shouldRetry ? 'retry' : 'failed', lastError]);
    process.stderr.write(`${buildMetaLog('meta_ads_sync_job_failed', {
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
    })}\n`);
}
async function processSyncJob(job) {
    await query(`
      UPDATE meta_ads_connections
      SET
        last_sync_started_at = now(),
        last_sync_status = 'running',
        last_sync_error = NULL,
        updated_at = now()
      WHERE id = $1
    `, [job.connection_id]);
    let connection = await getUsableMetaAdsConnection();
    try {
        const rowsByLevel = {
            account: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'account'),
            campaign: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'campaign'),
            adset: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'adset'),
            ad: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'ad')
        };
        const adIds = [...new Set(rowsByLevel.ad.map((row) => row.ad_id).filter((value) => Boolean(value)))];
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
    }
    catch (error) {
        if (error instanceof MetaAdsApiError && [400, 401, 403].includes(error.statusCode)) {
            connection = await getUsableMetaAdsConnection(true);
            try {
                const rowsByLevel = {
                    account: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'account'),
                    campaign: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'campaign'),
                    adset: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'adset'),
                    ad: await fetchInsightsForLevel(connection.access_token, job.ad_account_id, job.sync_date, 'ad')
                };
                const adIds = [
                    ...new Set(rowsByLevel.ad.map((row) => row.ad_id).filter((value) => Boolean(value)))
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
            }
            catch (retryError) {
                await markSyncJobFailed(job, retryError);
                return;
            }
        }
        await markSyncJobFailed(job, error);
    }
}
function buildMetricsLog(result) {
    return buildMetaLog('meta_ads_sync_run', {
        workerId: result.workerId,
        enqueuedJobs: result.enqueuedJobs,
        claimedJobs: result.claimedJobs,
        succeededJobs: result.succeededJobs,
        failedJobs: result.failedJobs,
        durationMs: result.durationMs
    });
}
export async function processMetaAdsSyncQueue(options = {}) {
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
        }
        catch (error) {
            failedJobs += 1;
            await markSyncJobFailed(job, error);
        }
    }
    const result = {
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
export function createMetaAdsPublicRouter() {
    const router = Router();
    router.get('/oauth/callback', async (req, res, next) => {
        try {
            const config = await getResolvedMetaAdsConfig();
            const payload = oauthCallbackSchema.parse(req.query);
            if (payload.error) {
                throw new MetaAdsHttpError(400, 'meta_ads_oauth_denied', payload.error_description ?? `Meta Ads OAuth failed with error: ${payload.error}`);
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
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
export function createMetaAdsAdminRouter() {
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
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/status', async (_req, res, next) => {
        try {
            const config = await getMetaAdsConfigurationSummary();
            const result = await query(`
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
        `);
            res.status(200).json({
                config,
                connection: result.rows[0] ?? null
            });
        }
        catch (error) {
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
        }
        catch (error) {
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
        }
        catch (error) {
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
        }
        catch (error) {
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
