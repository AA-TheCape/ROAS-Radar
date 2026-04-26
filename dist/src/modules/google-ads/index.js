import { createHash, randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Router } from 'express';
import { z } from 'zod';
import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { buildRawPayloadStorageMetadata, logRawPayloadIntegrityMismatch } from '../../shared/raw-payload-storage.js';
import { parseJsonResponsePayload, recordAdSyncApiTransaction } from '../ad-sync-audit/index.js';
import { attachAuthContext, requireAdmin } from '../auth/index.js';
import { buildCanonicalSpendDimensions } from '../marketing-dimensions/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
const GOOGLE_OAUTH_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_OAUTH_AUTHORIZATION_URL = 'https://accounts.google.com/o/oauth2/v2/auth';
const GOOGLE_ADS_API_BASE_URL = 'https://googleads.googleapis.com';
const GOOGLE_ADS_OAUTH_STATE_TTL_MINUTES = 10;
const GOOGLE_ADS_SYNC_JOB_STATUSES = ['pending', 'processing', 'retry', 'completed', 'failed'];
const GOOGLE_ADS_SPEND_GRANULARITIES = ['account', 'campaign', 'adset', 'ad', 'creative'];
const GOOGLE_ADS_SYNC_TIME_ZONE = 'America/Los_Angeles';
const oauthStartQuerySchema = z.object({
    customerId: z.string().min(1),
    loginCustomerId: z.string().optional(),
    redirectPath: z.string().optional()
});
const googleAdsConfigUpdateSchema = z.object({
    clientId: z.string().min(1),
    clientSecret: z.string().optional(),
    developerToken: z.string().optional(),
    appBaseUrl: z.string().url(),
    appScopes: z.union([z.string(), z.array(z.string())]).optional()
});
const oauthCallbackSchema = z.object({
    code: z.string().min(1).optional(),
    state: z.string().min(1).optional(),
    error: z.string().optional(),
    error_description: z.string().optional()
});
const manualSyncSchema = z.object({
    startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    endDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/)
});
class GoogleAdsHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'GoogleAdsHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
class GoogleAdsApiError extends Error {
    statusCode;
    details;
    constructor(statusCode, message, details) {
        super(message);
        this.name = 'GoogleAdsApiError';
        this.statusCode = statusCode;
        this.details = details;
    }
}
function assertGoogleAdsConfig() {
    if (!env.GOOGLE_ADS_ENCRYPTION_KEY) {
        throw new GoogleAdsHttpError(500, 'google_ads_config_missing', 'Missing Google Ads configuration: GOOGLE_ADS_ENCRYPTION_KEY');
    }
}
function normalizeGoogleAdsScopes(rawValue) {
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
function normalizeGoogleAdsCustomerId(value) {
    const normalized = value.replace(/-/g, '').trim();
    if (!/^\d+$/.test(normalized)) {
        throw new GoogleAdsHttpError(400, 'invalid_google_ads_customer_id', 'Google Ads customer ids must contain digits only');
    }
    return normalized;
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
        throw new GoogleAdsHttpError(400, 'invalid_redirect_path', 'redirectPath must be a root-relative path');
    }
    return trimmed;
}
async function getStoredGoogleAdsSettings() {
    const result = await query(`
      SELECT
        id,
        client_id,
        pgp_sym_decrypt(client_secret_encrypted, $1) AS client_secret,
        pgp_sym_decrypt(developer_token_encrypted, $1) AS developer_token,
        app_base_url,
        app_scopes,
        updated_at
      FROM google_ads_settings
      ORDER BY updated_at DESC
      LIMIT 1
    `, [env.GOOGLE_ADS_ENCRYPTION_KEY]);
    return result.rows[0] ?? null;
}
async function getResolvedGoogleAdsConfig() {
    assertGoogleAdsConfig();
    const stored = await getStoredGoogleAdsSettings();
    const clientId = stored?.client_id?.trim() || env.GOOGLE_ADS_CLIENT_ID.trim();
    const clientSecret = stored?.client_secret?.trim() || env.GOOGLE_ADS_CLIENT_SECRET.trim();
    const developerToken = stored?.developer_token?.trim() || env.GOOGLE_ADS_DEVELOPER_TOKEN.trim();
    const appBaseUrl = (stored?.app_base_url?.trim() || env.GOOGLE_ADS_APP_BASE_URL.trim()).replace(/\/$/, '');
    const appScopes = (stored?.app_scopes?.length ? stored.app_scopes : env.GOOGLE_ADS_APP_SCOPES)
        .map((entry) => entry.trim())
        .filter(Boolean);
    const missing = [
        ['GOOGLE_ADS_CLIENT_ID', clientId],
        ['GOOGLE_ADS_CLIENT_SECRET', clientSecret],
        ['GOOGLE_ADS_DEVELOPER_TOKEN', developerToken],
        ['GOOGLE_ADS_APP_BASE_URL', appBaseUrl]
    ]
        .filter(([, value]) => !value)
        .map(([key]) => key);
    if (missing.length > 0) {
        throw new GoogleAdsHttpError(500, 'google_ads_config_missing', `Missing Google Ads configuration: ${missing.join(', ')}`);
    }
    return {
        clientId,
        clientSecret,
        developerToken,
        appBaseUrl,
        appScopes,
        encryptionKey: env.GOOGLE_ADS_ENCRYPTION_KEY,
        source: stored ? 'database' : 'environment'
    };
}
async function getGoogleAdsConfigurationSummary() {
    const stored = env.GOOGLE_ADS_ENCRYPTION_KEY ? await getStoredGoogleAdsSettings() : null;
    const clientId = stored?.client_id?.trim() || env.GOOGLE_ADS_CLIENT_ID.trim();
    const clientSecretConfigured = Boolean(stored?.client_secret?.trim() || env.GOOGLE_ADS_CLIENT_SECRET.trim());
    const developerTokenConfigured = Boolean(stored?.developer_token?.trim() || env.GOOGLE_ADS_DEVELOPER_TOKEN.trim());
    const appBaseUrl = (stored?.app_base_url?.trim() || env.GOOGLE_ADS_APP_BASE_URL.trim()).replace(/\/$/, '');
    const appScopes = (stored?.app_scopes?.length ? stored.app_scopes : env.GOOGLE_ADS_APP_SCOPES)
        .map((entry) => entry.trim())
        .filter(Boolean);
    const missingFields = [
        ['clientId', clientId],
        ['clientSecret', clientSecretConfigured ? 'configured' : ''],
        ['developerToken', developerTokenConfigured ? 'configured' : ''],
        ['appBaseUrl', appBaseUrl],
        ['encryptionKey', env.GOOGLE_ADS_ENCRYPTION_KEY]
    ]
        .filter(([, value]) => !value)
        .map(([key]) => key);
    return {
        source: stored ? 'database' : 'environment',
        clientId,
        appBaseUrl,
        appScopes,
        clientSecretConfigured,
        developerTokenConfigured,
        missingFields
    };
}
async function upsertGoogleAdsSettings(payload) {
    assertGoogleAdsConfig();
    const clientSecretProvided = typeof payload.clientSecret === 'string' && payload.clientSecret.trim().length > 0;
    const developerTokenProvided = typeof payload.developerToken === 'string' && payload.developerToken.trim().length > 0;
    const normalizedScopes = normalizeGoogleAdsScopes(payload.appScopes);
    const existing = await getStoredGoogleAdsSettings();
    const nextClientSecret = clientSecretProvided ? (payload.clientSecret ?? '').trim() : existing?.client_secret ?? '';
    const nextDeveloperToken = developerTokenProvided ? (payload.developerToken ?? '').trim() : existing?.developer_token ?? '';
    await query(`
      DELETE FROM google_ads_settings
    `);
    await query(`
      INSERT INTO google_ads_settings (
        id,
        client_id,
        client_secret_encrypted,
        developer_token_encrypted,
        app_base_url,
        app_scopes,
        updated_at
      )
      VALUES (
        1,
        $1,
        CASE
          WHEN $2::text = '' THEN NULL
          ELSE pgp_sym_encrypt($2, $5, 'cipher-algo=aes256, compress-algo=0')
        END,
        CASE
          WHEN $3::text = '' THEN NULL
          ELSE pgp_sym_encrypt($3, $5, 'cipher-algo=aes256, compress-algo=0')
        END,
        $4,
        $6::text[],
        now()
      )
    `, [
        payload.clientId.trim(),
        nextClientSecret,
        nextDeveloperToken,
        new URL(payload.appBaseUrl).toString().replace(/\/$/, ''),
        env.GOOGLE_ADS_ENCRYPTION_KEY,
        normalizedScopes.length > 0 ? normalizedScopes : ['https://www.googleapis.com/auth/adwords']
    ]);
}
function getGoogleAdsAppBaseUrl(config) {
    return new URL(config.appBaseUrl).toString().replace(/\/$/, '');
}
function buildGoogleAdsRedirectUri(config) {
    return `${getGoogleAdsAppBaseUrl(config)}/google-ads/oauth/callback`;
}
function createOAuthStateDigest(state) {
    return createHash('sha256').update(state).digest('hex');
}
function buildGoogleAdsAuthorizationUrl(config, state) {
    const url = new URL(GOOGLE_OAUTH_AUTHORIZATION_URL);
    url.searchParams.set('client_id', config.clientId);
    url.searchParams.set('redirect_uri', buildGoogleAdsRedirectUri(config));
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('scope', config.appScopes.join(' '));
    url.searchParams.set('access_type', 'offline');
    url.searchParams.set('prompt', 'consent');
    url.searchParams.set('include_granted_scopes', 'true');
    url.searchParams.set('state', state);
    return url.toString();
}
async function insertOAuthState(params) {
    const state = randomBytes(24).toString('hex');
    const stateDigest = createOAuthStateDigest(state);
    await query(`
      INSERT INTO google_ads_oauth_states (state_digest, redirect_path, customer_id, login_customer_id, expires_at)
      VALUES ($1, $2, $3, $4, now() + ($5::int * interval '1 minute'))
    `, [stateDigest, params.redirectPath, params.customerId, params.loginCustomerId, GOOGLE_ADS_OAUTH_STATE_TTL_MINUTES]);
    return state;
}
async function consumeOAuthState(state) {
    const result = await query(`
      UPDATE google_ads_oauth_states
      SET consumed_at = now()
      WHERE state_digest = $1
        AND consumed_at IS NULL
        AND expires_at >= now()
      RETURNING redirect_path, customer_id, login_customer_id
    `, [createOAuthStateDigest(state)]);
    if (!result.rowCount) {
        throw new GoogleAdsHttpError(400, 'invalid_google_ads_oauth_state', 'The Google Ads OAuth state is invalid or expired');
    }
    return result.rows[0];
}
function formatDateOnly(value) {
    return value.toISOString().slice(0, 10);
}
function parseDateOnly(value) {
    return new Date(`${value}T00:00:00.000Z`);
}
function formatDateInTimeZone(value, timeZone) {
    const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    });
    return formatter.format(value);
}
function listDateRangeInclusive(startDate, endDate) {
    const start = parseDateOnly(startDate);
    const end = parseDateOnly(endDate);
    if (start.getTime() > end.getTime()) {
        throw new GoogleAdsHttpError(400, 'invalid_date_range', 'startDate must be on or before endDate');
    }
    const dates = [];
    for (let cursor = start; cursor.getTime() <= end.getTime(); cursor = new Date(cursor.getTime() + 24 * 60 * 60 * 1000)) {
        dates.push(formatDateOnly(cursor));
    }
    return dates;
}
function buildPlanningDates(now = new Date(), lastSyncCompletedAt = null) {
    const currentBusinessDate = parseDateOnly(formatDateInTimeZone(now, GOOGLE_ADS_SYNC_TIME_ZONE));
    const lookbackDays = lastSyncCompletedAt
        ? env.GOOGLE_ADS_SYNC_LOOKBACK_DAYS
        : env.GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS;
    const firstDate = new Date(currentBusinessDate.getTime() - (lookbackDays - 1) * 24 * 60 * 60 * 1000);
    if (currentBusinessDate.getTime() < firstDate.getTime()) {
        return [];
    }
    return listDateRangeInclusive(formatDateOnly(firstDate), formatDateOnly(currentBusinessDate));
}
function buildReconciliationWindow(now = new Date(), lastSyncCompletedAt = null) {
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
function computeRetryDelaySeconds(attempts) {
    const safeAttempts = Math.max(1, attempts);
    return Math.min(60 * 2 ** (safeAttempts - 1), 60 * 60);
}
function parseMetricInteger(value) {
    const parsed = Number.parseInt(value ?? '0', 10);
    return Number.isFinite(parsed) ? parsed : 0;
}
function parseMicrosToDecimal(value) {
    const parsed = Number.parseFloat(value ?? '0');
    if (!Number.isFinite(parsed)) {
        return '0.00';
    }
    return (parsed / 1_000_000).toFixed(2);
}
function buildGoogleAdsLog(event, payload) {
    return JSON.stringify({
        event,
        ...payload
    });
}
function buildAccessTokenRequestBody(connection) {
    const body = new URLSearchParams();
    body.set('client_id', connection.client_id);
    body.set('client_secret', connection.client_secret);
    body.set('refresh_token', connection.refresh_token);
    body.set('grant_type', 'refresh_token');
    return body;
}
function buildAuthorizationCodeRequestBody(config, code) {
    const body = new URLSearchParams();
    body.set('client_id', config.clientId);
    body.set('client_secret', config.clientSecret);
    body.set('code', code);
    body.set('grant_type', 'authorization_code');
    body.set('redirect_uri', buildGoogleAdsRedirectUri(config));
    return body;
}
async function exchangeAuthorizationCode(config, code) {
    const response = await fetch(GOOGLE_OAUTH_TOKEN_URL, {
        method: 'POST',
        headers: {
            'content-type': 'application/x-www-form-urlencoded'
        },
        body: buildAuthorizationCodeRequestBody(config, code)
    });
    const text = await response.text();
    const payload = parseJsonResponsePayload(text) ?? {};
    if (!response.ok || !payload.access_token || !payload.refresh_token) {
        throw new GoogleAdsApiError(response.status, payload.error_description ?? payload.error ?? 'Google OAuth code exchange failed', payload);
    }
    return payload;
}
async function exchangeRefreshToken(connection) {
    const response = await fetch(GOOGLE_OAUTH_TOKEN_URL, {
        method: 'POST',
        headers: {
            'content-type': 'application/x-www-form-urlencoded'
        },
        body: buildAccessTokenRequestBody(connection)
    });
    const text = await response.text();
    const payload = parseJsonResponsePayload(text) ?? {};
    if (!response.ok || !payload.access_token) {
        throw new GoogleAdsApiError(response.status, payload.error_description ?? payload.error ?? 'Google OAuth token exchange failed', payload);
    }
    await query('UPDATE google_ads_connections SET last_refreshed_at = now(), updated_at = now() WHERE id = $1', [connection.id]);
    return payload.access_token;
}
async function googleAdsSearch(params) {
    const url = new URL(`${GOOGLE_ADS_API_BASE_URL}/${env.GOOGLE_ADS_API_VERSION}/customers/${params.connection.customer_id}/googleAds:searchStream`);
    const requestStartedAt = new Date();
    const requestPayload = {
        query: params.gaql
    };
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
    const payload = parseJsonResponsePayload(text);
    const responseReceivedAt = new Date();
    if (params.audit) {
        await recordAdSyncApiTransaction({
            platform: 'google_ads',
            connectionId: params.audit.connectionId,
            syncJobId: params.audit.syncJobId,
            transactionSource: params.audit.transactionSource,
            sourceMetadata: params.audit.sourceMetadata,
            requestMethod: 'POST',
            requestUrl: url.toString(),
            requestPayload,
            requestStartedAt,
            responseStatus: response.status,
            responsePayload: payload,
            responseReceivedAt,
            errorMessage: response.ok ? null : `HTTP ${response.status}`
        });
    }
    if (!response.ok) {
        const details = payload;
        throw new GoogleAdsApiError(response.status, 'Google Ads API request failed', details);
    }
    if (!Array.isArray(payload)) {
        return [];
    }
    return payload.flatMap((batch) => batch.results ?? []);
}
async function fetchCustomerMetadata(connection, accessToken, audit) {
    const rows = await googleAdsSearch({
        connection,
        accessToken,
        gaql: 'SELECT customer.id, customer.descriptive_name, customer.currency_code FROM customer LIMIT 1',
        audit: audit
            ? {
                connectionId: audit.connectionId,
                syncJobId: audit.syncJobId,
                transactionSource: 'google_ads_customer_search',
                sourceMetadata: {
                    customerId: connection.customer_id
                }
            }
            : undefined
    });
    const row = rows[0]?.customer;
    if (!row?.id) {
        throw new GoogleAdsHttpError(400, 'google_ads_customer_not_found', 'Unable to resolve the Google Ads customer');
    }
    return {
        customerId: normalizeGoogleAdsCustomerId(row.id),
        descriptiveName: row.descriptiveName ?? null,
        currencyCode: row.currencyCode ?? null,
        rawPayload: rows[0] ?? {}
    };
}
function buildCampaignSpendQuery(syncDate) {
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
function buildAdSpendQuery(syncDate) {
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
async function fetchCampaignSpendRows(connection, accessToken, syncDate, audit) {
    return googleAdsSearch({
        connection,
        accessToken,
        gaql: buildCampaignSpendQuery(syncDate),
        audit: {
            connectionId: audit.connectionId,
            syncJobId: audit.syncJobId,
            transactionSource: 'google_ads_campaign_search',
            sourceMetadata: {
                customerId: connection.customer_id,
                syncDate
            }
        }
    });
}
async function fetchAdSpendRows(connection, accessToken, syncDate, audit) {
    return googleAdsSearch({
        connection,
        accessToken,
        gaql: buildAdSpendQuery(syncDate),
        audit: {
            connectionId: audit.connectionId,
            syncJobId: audit.syncJobId,
            transactionSource: 'google_ads_ad_search',
            sourceMetadata: {
                customerId: connection.customer_id,
                syncDate
            }
        }
    });
}
function normalizeSpendSnapshot(params) {
    const rollup = new Map();
    const upsertRow = (row) => {
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
            rawPayload: row
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
                rawPayload: row
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
                rawPayload: row
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
                rawPayload: row
            });
        }
    }
    return [...rollup.values()];
}
async function upsertGoogleAdsConnection(params) {
    const rawPayloadMetadata = buildRawPayloadStorageMetadata(params.customer.rawPayload);
    const { rawPayloadJson, payloadSizeBytes, payloadHash } = rawPayloadMetadata;
    const upsertResult = await query(`
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
        raw_customer_external_id,
        raw_customer_payload_size_bytes,
        raw_customer_payload_hash,
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
        $11,
        $12,
        $13,
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
        raw_customer_external_id = $11,
        raw_customer_payload_size_bytes = $12,
        raw_customer_payload_hash = $13,
        updated_at = now()
      RETURNING
        raw_customer_payload_size_bytes AS "storedPayloadSizeBytes",
        raw_customer_payload_hash AS "storedPayloadHash",
        raw_customer_data AS "persistedRawPayload"
    `, [
        params.customerId,
        params.loginCustomerId,
        params.developerToken,
        params.clientId,
        params.clientSecret,
        params.refreshToken,
        env.GOOGLE_ADS_ENCRYPTION_KEY,
        params.customer.descriptiveName,
        params.customer.currencyCode,
        rawPayloadJson,
        params.customerId,
        payloadSizeBytes,
        payloadHash
    ]);
    logRawPayloadIntegrityMismatch(rawPayloadMetadata, upsertResult.rows[0], {
        surface: 'google_ads_connections.raw_customer_data',
        operation: 'upsert',
        recordId: params.customerId
    });
}
async function getGoogleAdsConnectionById(connectionId) {
    const result = await query(`
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
    `, [env.GOOGLE_ADS_ENCRYPTION_KEY, connectionId]);
    const connection = result.rows[0];
    if (!connection) {
        throw new GoogleAdsHttpError(404, 'google_ads_connection_not_found', 'No active Google Ads connection was found');
    }
    return connection;
}
async function getLatestGoogleAdsConnection() {
    const result = await query(`
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
    `, [env.GOOGLE_ADS_ENCRYPTION_KEY]);
    const connection = result.rows[0];
    if (!connection) {
        throw new GoogleAdsHttpError(404, 'google_ads_connection_not_found', 'No active Google Ads connection was found');
    }
    return connection;
}
async function enqueueSyncDates(connectionId, dates) {
    let enqueuedJobs = 0;
    for (const date of dates) {
        await query(`
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
      `, [connectionId, date]);
        enqueuedJobs += 1;
    }
    return enqueuedJobs;
}
async function findMissingSyncDates(connectionId, dates) {
    if (dates.length === 0) {
        return [];
    }
    const result = await query(`
      SELECT sync_date::text
      FROM google_ads_sync_jobs
      WHERE connection_id = $1
        AND sync_date = ANY($2::date[])
        AND status = 'completed'
    `, [connectionId, dates]);
    const completed = new Set(result.rows.map((row) => row.sync_date));
    return dates.filter((date) => !completed.has(date));
}
async function runReconciliation(params) {
    const window = buildReconciliationWindow(params.now ?? new Date(), params.lastSyncCompletedAt ?? null);
    if (!window) {
        return 0;
    }
    const missingDates = await findMissingSyncDates(params.connectionId, window.dates);
    const enqueuedJobs = missingDates.length > 0 ? await enqueueSyncDates(params.connectionId, missingDates) : 0;
    await query(`
      INSERT INTO google_ads_reconciliation_runs (
        connection_id,
        checked_range_start,
        checked_range_end,
        missing_dates,
        enqueued_jobs,
        status
      )
      VALUES ($1, $2::date, $3::date, $4::jsonb, $5, $6)
    `, [
        params.connectionId,
        window.startDate,
        window.endDate,
        JSON.stringify(missingDates),
        enqueuedJobs,
        missingDates.length > 0 ? 'missing_dates' : 'healthy'
    ]);
    return enqueuedJobs;
}
async function planIncrementalSyncs(now = new Date()) {
    const result = await query(`
      SELECT
        id,
        last_sync_completed_at,
        last_sync_planned_for::text
      FROM google_ads_connections
      WHERE status = 'active'
    `);
    let plannedJobs = 0;
    const today = formatDateInTimeZone(now, GOOGLE_ADS_SYNC_TIME_ZONE);
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
async function claimSyncJobs(workerId, limit) {
    const result = await query(`
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
    `, [workerId, limit]);
    return result.rows;
}
async function persistDailySpendSnapshot(client, params) {
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
        const rawPayloadMetadata = buildRawPayloadStorageMetadata(row);
        const { rawPayloadJson, payloadSizeBytes, payloadHash } = rawPayloadMetadata;
        const insertResult = await client.query(`
        INSERT INTO google_ads_raw_spend_records (
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
        VALUES ($1, $2, $3::date, 'campaign', $4, $5, $6, $7::numeric, $8, $9, $10::jsonb, $11, $12, now())
        RETURNING
          id,
          payload_size_bytes AS "storedPayloadSizeBytes",
          payload_hash AS "storedPayloadHash",
          raw_payload AS "persistedRawPayload"
      `, [
            params.connectionId,
            params.syncJobId,
            params.syncDate,
            entityId,
            entityId,
            row.customer?.currencyCode ?? null,
            parseMicrosToDecimal(row.metrics?.costMicros),
            parseMetricInteger(row.metrics?.impressions),
            parseMetricInteger(row.metrics?.clicks),
            rawPayloadJson,
            payloadSizeBytes,
            payloadHash
        ]);
        logRawPayloadIntegrityMismatch(rawPayloadMetadata, insertResult.rows[0], {
            surface: 'google_ads_raw_spend_records',
            operation: 'insert',
            recordId: insertResult.rows[0].id,
            fields: {
                level: 'campaign',
                entityId,
                syncJobId: params.syncJobId
            }
        });
    }
    const rawRecordIdsByAdId = new Map();
    for (const row of params.adRows) {
        const entityId = row.adGroupAd?.ad?.id ?? null;
        const rawPayloadMetadata = buildRawPayloadStorageMetadata(row);
        const { rawPayloadJson, payloadSizeBytes, payloadHash } = rawPayloadMetadata;
        const insertResult = await client.query(`
        INSERT INTO google_ads_raw_spend_records (
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
        VALUES ($1, $2, $3::date, 'ad', $4, $5, $6, $7::numeric, $8, $9, $10::jsonb, $11, $12, now())
        RETURNING
          id,
          payload_size_bytes AS "storedPayloadSizeBytes",
          payload_hash AS "storedPayloadHash",
          raw_payload AS "persistedRawPayload"
      `, [
            params.connectionId,
            params.syncJobId,
            params.syncDate,
            entityId,
            entityId,
            row.customer?.currencyCode ?? null,
            parseMicrosToDecimal(row.metrics?.costMicros),
            parseMetricInteger(row.metrics?.impressions),
            parseMetricInteger(row.metrics?.clicks),
            rawPayloadJson,
            payloadSizeBytes,
            payloadHash
        ]);
        logRawPayloadIntegrityMismatch(rawPayloadMetadata, insertResult.rows[0], {
            surface: 'google_ads_raw_spend_records',
            operation: 'insert',
            recordId: insertResult.rows[0].id,
            fields: {
                level: 'ad',
                entityId,
                syncJobId: params.syncJobId
            }
        });
        if (entityId) {
            rawRecordIdsByAdId.set(entityId, insertResult.rows[0].id);
        }
    }
    for (const normalizedRow of params.normalizedRows) {
        // google_ads_daily_spend is a derived reporting projection. The raw API rows above remain
        // the canonical source-payload storage even when a row cannot be projected cleanly.
        await client.query(`
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
      `, [
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
        ]);
    }
    await refreshDailyReportingMetrics(client, [params.syncDate]);
}
async function markSyncJobSucceeded(jobId, connectionId) {
    await query(`
      UPDATE google_ads_sync_jobs
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
      UPDATE google_ads_connections
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
    const shouldRetry = job.attempts < env.GOOGLE_ADS_SYNC_MAX_RETRIES;
    const nextStatus = shouldRetry ? 'retry' : 'failed';
    const retryDelaySeconds = computeRetryDelaySeconds(job.attempts);
    await query(`
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
    `, [job.id, nextStatus, retryDelaySeconds, lastError]);
    await query(`
      UPDATE google_ads_connections
      SET
        last_sync_status = $2,
        last_sync_error = $3,
        updated_at = now()
      WHERE id = $1
    `, [job.connection_id, shouldRetry ? 'retry' : 'failed', lastError]);
    process.stderr.write(`${buildGoogleAdsLog('google_ads_sync_job_failed', {
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
    })}\n`);
}
async function processSyncJob(job) {
    await query(`
      UPDATE google_ads_connections
      SET
        last_sync_started_at = now(),
        last_sync_status = 'running',
        last_sync_error = NULL,
        updated_at = now()
      WHERE id = $1
    `, [job.connection_id]);
    const attemptJob = async () => {
        const connection = await getGoogleAdsConnectionById(job.connection_id);
        const accessToken = await exchangeRefreshToken(connection);
        // Preserve decoded Google Ads API responses before any projection into spend rows.
        const customer = await fetchCustomerMetadata(connection, accessToken, {
            connectionId: job.connection_id,
            syncJobId: job.id
        });
        const campaignRows = await fetchCampaignSpendRows(connection, accessToken, job.sync_date, {
            connectionId: job.connection_id,
            syncJobId: job.id
        });
        const adRows = await fetchAdSpendRows(connection, accessToken, job.sync_date, {
            connectionId: job.connection_id,
            syncJobId: job.id
        });
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
        const rawPayloadMetadata = buildRawPayloadStorageMetadata(customer.rawPayload);
        const { rawPayloadJson, payloadSizeBytes, payloadHash } = rawPayloadMetadata;
        const updateResult = await query(`
        UPDATE google_ads_connections
        SET
          customer_descriptive_name = $2,
          currency_code = $3,
          raw_customer_data = $4::jsonb,
          raw_customer_external_id = $5,
          raw_customer_payload_size_bytes = $6,
          raw_customer_payload_hash = $7,
          updated_at = now()
        WHERE id = $1
        RETURNING
          raw_customer_payload_size_bytes AS "storedPayloadSizeBytes",
          raw_customer_payload_hash AS "storedPayloadHash",
          raw_customer_data AS "persistedRawPayload"
      `, [
            job.connection_id,
            customer.descriptiveName,
            customer.currencyCode,
            rawPayloadJson,
            customer.customerId,
            payloadSizeBytes,
            payloadHash
        ]);
        logRawPayloadIntegrityMismatch(rawPayloadMetadata, updateResult.rows[0], {
            surface: 'google_ads_connections.raw_customer_data',
            operation: 'update',
            recordId: job.connection_id,
            fields: {
                syncJobId: job.id
            }
        });
    };
    try {
        await attemptJob();
        await markSyncJobSucceeded(job.id, job.connection_id);
    }
    catch (error) {
        if (error instanceof GoogleAdsApiError && [401, 403, 429, 500, 502, 503, 504].includes(error.statusCode)) {
            try {
                await delay(500);
                await attemptJob();
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
    return buildGoogleAdsLog('google_ads_sync_run', {
        workerId: result.workerId,
        enqueuedJobs: result.enqueuedJobs,
        claimedJobs: result.claimedJobs,
        succeededJobs: result.succeededJobs,
        failedJobs: result.failedJobs,
        durationMs: result.durationMs
    });
}
export async function processGoogleAdsSyncQueue(options = {}) {
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
export function createGoogleAdsPublicRouter() {
    const router = Router();
    router.get('/oauth/callback', async (req, res, next) => {
        try {
            const config = await getResolvedGoogleAdsConfig();
            const payload = oauthCallbackSchema.parse(req.query);
            if (payload.error) {
                throw new GoogleAdsHttpError(400, 'google_ads_oauth_denied', payload.error_description ?? `Google Ads OAuth failed with error: ${payload.error}`);
            }
            if (!payload.code || !payload.state) {
                throw new GoogleAdsHttpError(400, 'google_ads_oauth_invalid_callback', 'Missing OAuth callback parameters');
            }
            const oauthState = await consumeOAuthState(payload.state);
            const tokenPayload = await exchangeAuthorizationCode(config, payload.code);
            const refreshToken = tokenPayload.refresh_token;
            const accessToken = tokenPayload.access_token;
            if (!refreshToken || !accessToken) {
                throw new GoogleAdsHttpError(502, 'google_ads_oauth_token_missing', 'Google Ads OAuth response did not include the required access and refresh tokens');
            }
            const connectionForValidation = {
                id: 0,
                customer_id: oauthState.customer_id,
                login_customer_id: oauthState.login_customer_id,
                developer_token: config.developerToken,
                client_id: config.clientId,
                client_secret: config.clientSecret,
                refresh_token: refreshToken,
                token_scopes: config.appScopes,
                last_refreshed_at: null,
                status: 'active',
                customer_descriptive_name: null,
                currency_code: null
            };
            const customer = await fetchCustomerMetadata(connectionForValidation, accessToken);
            if (customer.customerId !== oauthState.customer_id) {
                throw new GoogleAdsHttpError(400, 'google_ads_customer_mismatch', `Provided customerId ${oauthState.customer_id} does not match resolved customer ${customer.customerId}`);
            }
            await upsertGoogleAdsConnection({
                customerId: oauthState.customer_id,
                loginCustomerId: oauthState.login_customer_id,
                developerToken: config.developerToken,
                clientId: config.clientId,
                clientSecret: config.clientSecret,
                refreshToken,
                customer
            });
            const connection = await getLatestGoogleAdsConnection();
            const initialDates = buildPlanningDates(new Date(), null);
            await enqueueSyncDates(connection.id, initialDates);
            if (oauthState.redirect_path) {
                res.redirect(302, oauthState.redirect_path);
                return;
            }
            res.status(200).json({
                ok: true,
                customerId: oauthState.customer_id,
                customerName: customer.descriptiveName,
                currencyCode: customer.currencyCode,
                plannedDates: initialDates
            });
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
export function createGoogleAdsAdminRouter() {
    const router = Router();
    router.use(attachAuthContext);
    router.use(requireAdmin);
    router.put('/config', async (req, res, next) => {
        try {
            const payload = googleAdsConfigUpdateSchema.parse(req.body);
            await upsertGoogleAdsSettings(payload);
            res.status(200).json({
                ok: true,
                config: await getGoogleAdsConfigurationSummary()
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/oauth/start', async (req, res, next) => {
        try {
            const config = await getResolvedGoogleAdsConfig();
            const payload = oauthStartQuerySchema.parse(req.query);
            const normalizedCustomerId = normalizeGoogleAdsCustomerId(payload.customerId);
            const loginCustomerId = payload.loginCustomerId ? normalizeGoogleAdsCustomerId(payload.loginCustomerId) : null;
            const redirectPath = normalizeRedirectPath(payload.redirectPath);
            const state = await insertOAuthState({
                redirectPath,
                customerId: normalizedCustomerId,
                loginCustomerId
            });
            res.status(200).json({
                authorizationUrl: buildGoogleAdsAuthorizationUrl(config, state),
                redirectUri: buildGoogleAdsRedirectUri(config),
                state
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/status', async (_req, res, next) => {
        try {
            const config = await getGoogleAdsConfigurationSummary();
            const connectionResult = await query(`
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
        `);
            const reconciliationResult = await query(`
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
        `);
            res.status(200).json({
                config,
                connection: connectionResult.rows[0] ?? null,
                reconciliation: reconciliationResult.rows[0] ?? null
            });
        }
        catch (error) {
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
        }
        catch (error) {
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
        }
        catch (error) {
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
