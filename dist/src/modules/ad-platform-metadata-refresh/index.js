import { randomUUID } from 'node:crypto';
import { query } from '../../db/pool.js';
import { emitCampaignMetadataFreshnessSnapshotLog, emitCampaignMetadataSyncJobLifecycleLog, logInfo } from '../../observability/index.js';
import { resolveCampaignDisplayMetadata } from '../reporting/metadata-resolution.js';
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
function buildKey(record) {
    return `${record.platform}\u0000${record.accountId}\u0000${record.entityType}\u0000${record.entityId}`;
}
function endOfReportDate(reportDate) {
    return new Date(`${reportDate}T23:59:59.000Z`);
}
async function loadHistoricalMetadataRows(startDate, endDate) {
    const result = await query(`
      SELECT 'google_ads'::text AS platform, account_id, 'campaign'::text AS entity_type, campaign_id AS entity_id, campaign_name AS entity_name, report_date::text
      FROM google_ads_daily_spend
      WHERE report_date BETWEEN $1::date AND $2::date
        AND campaign_id IS NOT NULL
      UNION ALL
      SELECT 'google_ads'::text AS platform, account_id, 'adset'::text AS entity_type, adset_id AS entity_id, adset_name AS entity_name, report_date::text
      FROM google_ads_daily_spend
      WHERE report_date BETWEEN $1::date AND $2::date
        AND adset_id IS NOT NULL
      UNION ALL
      SELECT 'google_ads'::text AS platform, account_id, 'ad'::text AS entity_type, ad_id AS entity_id, ad_name AS entity_name, report_date::text
      FROM google_ads_daily_spend
      WHERE report_date BETWEEN $1::date AND $2::date
        AND ad_id IS NOT NULL
      UNION ALL
      SELECT 'meta_ads'::text AS platform, account_id, 'campaign'::text AS entity_type, campaign_id AS entity_id, campaign_name AS entity_name, report_date::text
      FROM meta_ads_daily_spend
      WHERE report_date BETWEEN $1::date AND $2::date
        AND campaign_id IS NOT NULL
      UNION ALL
      SELECT 'meta_ads'::text AS platform, account_id, 'adset'::text AS entity_type, adset_id AS entity_id, adset_name AS entity_name, report_date::text
      FROM meta_ads_daily_spend
      WHERE report_date BETWEEN $1::date AND $2::date
        AND adset_id IS NOT NULL
      UNION ALL
      SELECT 'meta_ads'::text AS platform, account_id, 'ad'::text AS entity_type, ad_id AS entity_id, ad_name AS entity_name, report_date::text
      FROM meta_ads_daily_spend
      WHERE report_date BETWEEN $1::date AND $2::date
        AND ad_id IS NOT NULL
      ORDER BY report_date ASC
    `, [startDate, endDate]);
    return result.rows;
}
function collapseHistoricalRows(rows) {
    const records = new Map();
    for (const row of rows) {
        const accountId = normalizeString(row.account_id);
        const entityId = normalizeString(row.entity_id);
        if (!accountId || !entityId) {
            continue;
        }
        const key = buildKey({
            platform: row.platform,
            accountId,
            entityType: row.entity_type,
            entityId
        });
        const latestName = collapseWhitespace(row.entity_name);
        const lastSeenAt = endOfReportDate(row.report_date);
        const existing = records.get(key);
        if (!existing) {
            records.set(key, {
                platform: row.platform,
                accountId,
                entityType: row.entity_type,
                entityId,
                latestName,
                lastSeenAt
            });
            continue;
        }
        if (lastSeenAt.getTime() >= existing.lastSeenAt.getTime()) {
            existing.lastSeenAt = lastSeenAt;
        }
        if (latestName) {
            existing.latestName = latestName;
        }
    }
    return [...records.values()].sort((left, right) => left.platform.localeCompare(right.platform) ||
        left.entityType.localeCompare(right.entityType) ||
        left.entityId.localeCompare(right.entityId));
}
async function loadExistingMetadata(records) {
    if (records.length === 0) {
        return new Map();
    }
    const result = await query(`
      SELECT platform, account_id, entity_type, entity_id, latest_name, last_seen_at
      FROM ad_platform_entity_metadata
    `);
    const map = new Map();
    for (const row of result.rows) {
        map.set(buildKey({
            platform: row.platform,
            accountId: row.account_id,
            entityType: row.entity_type,
            entityId: row.entity_id
        }), row);
    }
    return map;
}
async function upsertMetadata(records) {
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
async function loadRequestedCampaigns(startDate, endDate) {
    const result = await query(`
      SELECT DISTINCT campaign
      FROM daily_reporting_metrics
      WHERE metric_date BETWEEN $1::date AND $2::date
      ORDER BY campaign ASC
    `, [startDate, endDate]);
    return result.rows.map((row) => row.campaign);
}
function summarizeCoverage(result) {
    const values = [...result.byCampaign.values()];
    return {
        totalEntities: values.length,
        resolvedEntities: values.filter((entry) => entry.campaignNameResolutionStatus === 'resolved').length
    };
}
async function emitFreshnessSnapshots(records, freshnessThresholdHours) {
    const now = Date.now();
    const staleThresholdMs = freshnessThresholdHours * 60 * 60 * 1000;
    const buckets = new Map();
    for (const record of records) {
        const key = `${record.platform}\u0000${record.entityType}`;
        const bucket = buckets.get(key) ?? [];
        bucket.push(record);
        buckets.set(key, bucket);
    }
    let totalStale = 0;
    for (const [key, bucket] of buckets) {
        const [platform, entityType] = key.split('\u0000');
        const staleEntityCount = bucket.filter((record) => now - record.lastSeenAt.getTime() > staleThresholdMs).length;
        totalStale += staleEntityCount;
        const sorted = [...bucket].sort((left, right) => left.lastSeenAt.getTime() - right.lastSeenAt.getTime());
        emitCampaignMetadataFreshnessSnapshotLog({
            platform,
            entityType,
            freshEntityCount: bucket.length - staleEntityCount,
            staleEntityCount,
            freshnessThresholdHours,
            oldestLastSeenAt: sorted[0]?.lastSeenAt.toISOString() ?? null,
            newestLastSeenAt: sorted.at(-1)?.lastSeenAt.toISOString() ?? null
        });
    }
    return totalStale;
}
export async function backfillCampaignMetadataHistory(input) {
    const runId = input.runId?.trim() || randomUUID();
    const startedAt = new Date();
    const dryRun = Boolean(input.dryRun);
    const unresolvedSampleLimit = input.unresolvedSampleLimit ?? 25;
    await query(`
      INSERT INTO campaign_metadata_backfill_runs (
        id,
        status,
        requested_by,
        worker_id,
        started_at,
        window_start,
        window_end,
        dry_run
      )
      VALUES ($1::uuid, 'processing', $2, $3, $4, $5::date, $6::date, $7)
    `, [runId, input.requestedBy, input.workerId, startedAt, input.startDate, input.endDate, dryRun]);
    emitCampaignMetadataSyncJobLifecycleLog({
        stage: 'started',
        platform: 'all',
        workerId: input.workerId,
        jobId: runId,
        requestedBy: input.requestedBy,
        startedAt: startedAt.toISOString()
    });
    try {
        const campaigns = await loadRequestedCampaigns(input.startDate, input.endDate);
        const beforeResolution = await resolveCampaignDisplayMetadata(input.startDate, input.endDate, campaigns);
        const historicalRows = await loadHistoricalMetadataRows(input.startDate, input.endDate);
        const records = collapseHistoricalRows(historicalRows);
        const existingMap = await loadExistingMetadata(records);
        let plannedInserts = 0;
        let plannedUpdates = 0;
        for (const record of records) {
            if (!record.latestName) {
                continue;
            }
            const existing = existingMap.get(buildKey(record));
            if (!existing) {
                plannedInserts += 1;
                continue;
            }
            if (collapseWhitespace(existing.latest_name) !== record.latestName ||
                existing.last_seen_at.getTime() !== record.lastSeenAt.getTime()) {
                plannedUpdates += 1;
            }
        }
        if (!dryRun) {
            await upsertMetadata(records);
        }
        const afterResolution = await resolveCampaignDisplayMetadata(input.startDate, input.endDate, campaigns);
        const unresolvedRecords = records.filter((record) => !record.latestName);
        const unresolvedSamples = unresolvedRecords.slice(0, unresolvedSampleLimit).map((record) => ({
            platform: record.platform,
            accountId: record.accountId,
            entityType: record.entityType,
            entityId: record.entityId,
            lastSeenAt: record.lastSeenAt.toISOString(),
            hadNameInHistory: false
        }));
        const staleEntityCount = await emitFreshnessSnapshots(records, 48);
        const completedAt = new Date();
        const report = {
            runId,
            status: 'completed',
            startedAt: startedAt.toISOString(),
            completedAt: completedAt.toISOString(),
            requestedBy: input.requestedBy,
            workerId: input.workerId,
            startDate: input.startDate,
            endDate: input.endDate,
            dryRun,
            plannedInserts,
            plannedUpdates,
            campaignCoverageBefore: summarizeCoverage(beforeResolution),
            campaignCoverageAfter: summarizeCoverage(afterResolution),
            unresolvedRate: {
                totalEntities: records.length,
                unresolvedEntities: unresolvedRecords.length
            },
            unresolvedSamples
        };
        await query(`
        UPDATE campaign_metadata_backfill_runs
        SET
          status = 'completed',
          completed_at = $2,
          report = $3::jsonb,
          updated_at = now()
        WHERE id = $1::uuid
      `, [runId, completedAt, JSON.stringify(report)]);
        emitCampaignMetadataSyncJobLifecycleLog({
            stage: 'completed',
            platform: 'all',
            workerId: input.workerId,
            jobId: runId,
            requestedBy: input.requestedBy,
            startedAt: startedAt.toISOString(),
            completedAt: completedAt.toISOString(),
            durationMs: completedAt.getTime() - startedAt.getTime(),
            plannedInserts,
            plannedUpdates,
            campaignResolvedRate: report.campaignCoverageAfter.totalEntities > 0
                ? report.campaignCoverageAfter.resolvedEntities / report.campaignCoverageAfter.totalEntities
                : 0,
            overallUnresolvedRate: report.unresolvedRate.totalEntities > 0
                ? report.unresolvedRate.unresolvedEntities / report.unresolvedRate.totalEntities
                : 0,
            staleEntityCount
        });
        logInfo('campaign_metadata_backfill_completed', {
            runId,
            plannedInserts,
            plannedUpdates,
            unresolvedEntities: report.unresolvedRate.unresolvedEntities
        });
        return report;
    }
    catch (error) {
        const completedAt = new Date();
        const errorMessage = error instanceof Error ? error.message : String(error);
        await query(`
        UPDATE campaign_metadata_backfill_runs
        SET
          status = 'failed',
          completed_at = $2,
          error_code = $3,
          error_message = $4,
          updated_at = now()
        WHERE id = $1::uuid
      `, [runId, completedAt, error instanceof Error ? error.name : 'Error', errorMessage]);
        emitCampaignMetadataSyncJobLifecycleLog({
            stage: 'failed',
            platform: 'all',
            workerId: input.workerId,
            jobId: runId,
            requestedBy: input.requestedBy,
            startedAt: startedAt.toISOString(),
            completedAt: completedAt.toISOString(),
            error
        });
        throw error;
    }
}
