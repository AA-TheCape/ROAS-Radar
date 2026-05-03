import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { emitCampaignMetadataSyncJobLifecycleLog } from '../../observability/index.js';
const META_METADATA_ENTITY_ORDER = {
    campaign: 0,
    adset: 1,
    ad: 2
};
const legacyMetaAdsModule = (await import(pathToFileURL(resolve(process.cwd(), 'dist/modules/meta-ads/index.js')).href));
export const createMetaAdsRouter = legacyMetaAdsModule.createMetaAdsRouter ??
    (() => {
        throw new Error('Legacy Meta Ads router is unavailable');
    });
function normalizeString(value) {
    if (typeof value !== 'string') {
        return null;
    }
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
}
function collapseWhitespace(value) {
    const normalized = normalizeString(value);
    return normalized ? normalized.replace(/\s+/g, ' ') : null;
}
function formatDateOnly(value) {
    return value.toISOString().slice(0, 10);
}
function addDays(date, days) {
    return new Date(date.getTime() + days * 86_400_000);
}
function listDateRangeInclusive(startDate, endDate) {
    const start = new Date(`${startDate}T00:00:00.000Z`);
    const end = new Date(`${endDate}T00:00:00.000Z`);
    const dates = [];
    for (let cursor = start; cursor.getTime() <= end.getTime(); cursor = addDays(cursor, 1)) {
        dates.push(formatDateOnly(cursor));
    }
    return dates;
}
export function buildPlanningDates(now = new Date(), lastSyncCompletedAt = null) {
    const today = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    const lookbackDays = lastSyncCompletedAt ? env.META_ADS_SYNC_LOOKBACK_DAYS : env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS;
    const start = addDays(today, -(lookbackDays - 1));
    if (lastSyncCompletedAt) {
        const lastSyncDate = formatDateOnly(lastSyncCompletedAt);
        const todayDate = formatDateOnly(today);
        if (lastSyncDate === todayDate) {
            return [todayDate];
        }
    }
    return listDateRangeInclusive(formatDateOnly(start), formatDateOnly(today));
}
export function buildIncrementalPlanningDates(now = new Date(), lastSyncCompletedAt, plannedForDate) {
    const today = formatDateOnly(now);
    if (plannedForDate === today) {
        return [today];
    }
    return buildPlanningDates(now, lastSyncCompletedAt);
}
export function rollupPersistableSpendRows(rows) {
    const rolled = new Map();
    for (const row of rows) {
        if (row.normalizedRow.granularity !== 'campaign') {
            rolled.set(`${row.rawRecordId}`, row);
            continue;
        }
        const key = `${row.normalizedRow.granularity}\u0000${row.normalizedRow.entityKey}`;
        const existing = rolled.get(key);
        if (!existing) {
            rolled.set(key, {
                ...row,
                normalizedRow: {
                    ...row.normalizedRow
                }
            });
            continue;
        }
        existing.normalizedRow.spend = (Number.parseFloat(existing.normalizedRow.spend) + Number.parseFloat(row.normalizedRow.spend)).toFixed(2);
        existing.normalizedRow.impressions += row.normalizedRow.impressions;
        existing.normalizedRow.clicks += row.normalizedRow.clicks;
    }
    return [...rolled.values()];
}
export function buildMetaAdsMetadataRecords(input) {
    const accountId = normalizeString(input.accountId);
    if (!accountId) {
        return [];
    }
    const records = new Map();
    const upsert = (entityType, entityId, latestName) => {
        const normalizedEntityId = normalizeString(entityId);
        if (!normalizedEntityId) {
            return;
        }
        const key = `${entityType}\u0000${normalizedEntityId}`;
        const existing = records.get(key);
        if (!existing) {
            records.set(key, {
                platform: 'meta_ads',
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
        upsert('campaign', row.id, collapseWhitespace(row.name));
    }
    for (const row of input.adsetRows) {
        upsert('adset', row.id, collapseWhitespace(row.name));
    }
    for (const row of input.adRows) {
        upsert('ad', row.id, collapseWhitespace(row.name));
    }
    return [...records.values()].sort((left, right) => META_METADATA_ENTITY_ORDER[left.entityType] - META_METADATA_ENTITY_ORDER[right.entityType] ||
        left.entityId.localeCompare(right.entityId));
}
async function loadActiveMetaAdsConnections() {
    const result = await query(`
      SELECT
        id,
        ad_account_id,
        pgp_sym_decrypt(access_token_encrypted, $1) AS access_token
      FROM meta_ads_connections
      WHERE status = 'active'
      ORDER BY id ASC
    `, [env.META_ADS_ENCRYPTION_KEY]);
    return result.rows;
}
async function acquireMetadataRefreshLock(platform, accountId) {
    const result = await query(`SELECT pg_try_advisory_lock(hashtext($1), hashtext($2)) AS acquired`, [platform, accountId]);
    return Boolean(result.rows[0]?.acquired);
}
async function releaseMetadataRefreshLock(platform, accountId) {
    await query(`SELECT pg_advisory_unlock(hashtext($1), hashtext($2))`, [platform, accountId]);
}
async function fetchMetaCollection(connection, path) {
    const url = new URL(`https://graph.facebook.com/v23.0/act_${connection.ad_account_id}/${path}`);
    url.searchParams.set('fields', 'id,name');
    url.searchParams.set('limit', '1000');
    url.searchParams.set('access_token', connection.access_token);
    const response = await fetch(url);
    if (!response.ok) {
        throw createMetaAdsApiErrorForTest(response.status, 'Meta Ads API request failed', await response.json());
    }
    const payload = (await response.json());
    return payload.data ?? [];
}
async function upsertMetaMetadataRecords(records) {
    for (const record of records) {
        if (!record.latestName) {
            continue;
        }
        await query(`
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
      `, [record.platform, record.accountId, record.entityType, record.entityId, record.latestName, record.lastSeenAt]);
    }
}
export async function refreshMetaAdsMetadataForConnection(connection, now = new Date(), workerId = 'meta-ads-metadata-refresh') {
    const acquired = await acquireMetadataRefreshLock('meta_ads', connection.ad_account_id);
    if (!acquired) {
        return { skipped: true, recordCount: 0 };
    }
    const startedAt = new Date();
    emitCampaignMetadataSyncJobLifecycleLog({
        stage: 'started',
        platform: 'meta_ads',
        workerId,
        jobId: String(connection.id),
        startedAt: startedAt.toISOString()
    });
    try {
        const [campaignRows, adsetRows, adRows] = await Promise.all([
            fetchMetaCollection(connection, 'campaigns'),
            fetchMetaCollection(connection, 'adsets'),
            fetchMetaCollection(connection, 'ads')
        ]);
        const records = buildMetaAdsMetadataRecords({
            accountId: connection.ad_account_id,
            observedAt: now,
            campaignRows,
            adsetRows,
            adRows
        });
        await upsertMetaMetadataRecords(records);
        emitCampaignMetadataSyncJobLifecycleLog({
            stage: 'completed',
            platform: 'meta_ads',
            workerId,
            jobId: String(connection.id),
            startedAt: startedAt.toISOString(),
            completedAt: new Date().toISOString()
        });
        return { skipped: false, recordCount: records.length };
    }
    catch (error) {
        emitCampaignMetadataSyncJobLifecycleLog({
            stage: 'failed',
            platform: 'meta_ads',
            workerId,
            jobId: String(connection.id),
            startedAt: startedAt.toISOString(),
            completedAt: new Date().toISOString(),
            error
        });
        throw error;
    }
    finally {
        await releaseMetadataRefreshLock('meta_ads', connection.ad_account_id);
    }
}
export async function refreshActiveMetaAdsMetadataConnections(options) {
    const connections = await loadActiveMetaAdsConnections();
    let refreshed = 0;
    let skipped = 0;
    for (const connection of connections) {
        const result = await refreshMetaAdsMetadataForConnection(connection, options?.now ?? new Date(), options?.workerId ?? 'meta-ads-metadata-refresh');
        if (result.skipped) {
            skipped += 1;
        }
        else {
            refreshed += 1;
        }
    }
    return {
        attempted: connections.length,
        refreshed,
        skipped
    };
}
export async function processMetaAdsSyncQueue(options) {
    const result = (await legacyMetaAdsModule.processMetaAdsSyncQueue(options));
    const metadataRefresh = await refreshActiveMetaAdsMetadataConnections({
        now: options?.now instanceof Date ? options.now : new Date(),
        workerId: typeof options?.workerId === 'string' ? options.workerId : 'meta-ads-worker'
    });
    return {
        ...result,
        metadataRefresh
    };
}
export function createMetaAdsApiErrorForTest(statusCode, message, details) {
    const error = new Error(message);
    error.name = 'MetaAdsApiError';
    error.statusCode = statusCode;
    error.details = details;
    return error;
}
export function isRetryableMetaAdsApiError(error) {
    if (!(error instanceof Error) || !('details' in error)) {
        return false;
    }
    const details = error.details;
    const code = details.error?.code;
    return code === 4 || code === 17 || code === 32 || code === 613;
}
export function formatMetaAdsError(error) {
    if (!(error instanceof Error) || !('statusCode' in error)) {
        return error instanceof Error ? error.message : String(error);
    }
    const statusCode = typeof error.statusCode === 'number' ? error.statusCode : null;
    const details = ('details' in error ? error.details : null);
    const code = details?.error?.code;
    const subcode = details?.error?.error_subcode;
    if (statusCode && typeof code === 'number') {
        return `${error.message} (status=${statusCode}, code=${code}, subcode=${subcode ?? 'unknown'})`;
    }
    return statusCode ? `${error.message} (status=${statusCode})` : error.message;
}
export const __metaAdsTestUtils = {
    ...(legacyMetaAdsModule.__metaAdsTestUtils ?? {}),
    buildPlanningDates,
    buildIncrementalPlanningDates,
    rollupPersistableSpendRows,
    buildMetaAdsMetadataRecords,
    createMetaAdsApiErrorForTest,
    isRetryableMetaAdsApiError,
    formatMetaAdsError
};
