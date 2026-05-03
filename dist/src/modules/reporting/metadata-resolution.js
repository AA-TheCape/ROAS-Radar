import { query } from '../../db/pool.js';
import { emitCampaignMetadataResolutionCoverageLog } from '../../observability/index.js';
export function buildCampaignResolutionGroupKey(source, medium, campaign) {
    return `${source}\u0000${medium}\u0000${campaign}`;
}
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
function chooseBetterResolution(current, candidate) {
    if (!current) {
        return candidate;
    }
    const rank = (value) => {
        switch (value) {
            case 'resolved':
                return 3;
            case 'fallback_name':
                return 2;
            case 'unresolved':
                return 1;
        }
    };
    const currentRank = rank(current.campaignNameResolutionStatus);
    const candidateRank = rank(candidate.campaignNameResolutionStatus);
    if (candidateRank !== currentRank) {
        return candidateRank > currentRank ? candidate : current;
    }
    const currentTimestamp = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
    const candidateTimestamp = candidate.lastSeenAt ? Date.parse(candidate.lastSeenAt) : 0;
    if (candidateTimestamp !== currentTimestamp) {
        return candidateTimestamp > currentTimestamp ? candidate : current;
    }
    return current;
}
function buildResolution(row) {
    const resolvedName = collapseWhitespace(row.latest_name);
    const fallbackName = collapseWhitespace(row.fallback_name);
    const entityId = normalizeString(row.campaign_id);
    if (resolvedName) {
        return {
            campaign: row.campaign,
            source: row.source,
            medium: row.medium,
            campaignDisplayName: resolvedName,
            campaignEntityId: entityId,
            campaignPlatform: row.platform,
            campaignNameResolutionStatus: 'resolved',
            lastSeenAt: row.last_seen_at?.toISOString() ?? null,
            updatedAt: row.updated_at?.toISOString() ?? null
        };
    }
    if (fallbackName) {
        return {
            campaign: row.campaign,
            source: row.source,
            medium: row.medium,
            campaignDisplayName: fallbackName,
            campaignEntityId: entityId,
            campaignPlatform: row.platform,
            campaignNameResolutionStatus: 'fallback_name',
            lastSeenAt: null,
            updatedAt: null
        };
    }
    return {
        campaign: row.campaign,
        source: row.source,
        medium: row.medium,
        campaignDisplayName: entityId ?? 'unknown',
        campaignEntityId: entityId,
        campaignPlatform: row.platform,
        campaignNameResolutionStatus: 'unresolved',
        lastSeenAt: null,
        updatedAt: null
    };
}
export async function resolveCampaignDisplayMetadata(startDate, endDate, campaigns, source) {
    const normalizedCampaigns = [...new Set(campaigns.map((value) => value.trim()).filter(Boolean))];
    if (normalizedCampaigns.length === 0) {
        return {
            byCampaign: new Map(),
            byGroup: new Map()
        };
    }
    const result = await query(`
      WITH google_candidates AS (
        SELECT DISTINCT ON (g.canonical_campaign, g.canonical_source, g.canonical_medium, g.account_id, g.campaign_id)
          g.canonical_campaign AS campaign,
          g.canonical_source AS source,
          g.canonical_medium AS medium,
          'google_ads'::text AS platform,
          g.account_id,
          g.campaign_id,
          NULLIF(regexp_replace(COALESCE(g.campaign_name, ''), '\\s+', ' ', 'g'), '') AS fallback_name,
          m.latest_name,
          m.last_seen_at,
          m.updated_at,
          g.report_date,
          g.id
        FROM google_ads_daily_spend g
        LEFT JOIN ad_platform_entity_metadata m
          ON m.platform = 'google_ads'
         AND m.account_id = g.account_id
         AND m.entity_type = 'campaign'
         AND m.entity_id = g.campaign_id
        WHERE g.report_date BETWEEN $1::date AND $2::date
          AND g.campaign_id IS NOT NULL
          AND g.canonical_campaign = ANY($3::text[])
          AND ($4::text IS NULL OR g.canonical_source = $4::text)
        ORDER BY
          g.canonical_campaign,
          g.canonical_source,
          g.canonical_medium,
          g.account_id,
          g.campaign_id,
          g.report_date DESC,
          g.id DESC
      ),
      meta_candidates AS (
        SELECT DISTINCT ON (mads.canonical_campaign, mads.canonical_source, mads.canonical_medium, mads.account_id, mads.campaign_id)
          mads.canonical_campaign AS campaign,
          mads.canonical_source AS source,
          mads.canonical_medium AS medium,
          'meta_ads'::text AS platform,
          mads.account_id,
          mads.campaign_id,
          NULLIF(regexp_replace(COALESCE(mads.campaign_name, ''), '\\s+', ' ', 'g'), '') AS fallback_name,
          m.latest_name,
          m.last_seen_at,
          m.updated_at,
          mads.report_date,
          mads.id
        FROM meta_ads_daily_spend mads
        LEFT JOIN ad_platform_entity_metadata m
          ON m.platform = 'meta_ads'
         AND m.account_id = mads.account_id
         AND m.entity_type = 'campaign'
         AND m.entity_id = mads.campaign_id
        WHERE mads.report_date BETWEEN $1::date AND $2::date
          AND mads.campaign_id IS NOT NULL
          AND mads.canonical_campaign = ANY($3::text[])
          AND ($4::text IS NULL OR mads.canonical_source = $4::text)
        ORDER BY
          mads.canonical_campaign,
          mads.canonical_source,
          mads.canonical_medium,
          mads.account_id,
          mads.campaign_id,
          mads.report_date DESC,
          mads.id DESC
      )
      SELECT campaign, source, medium, platform, account_id, campaign_id, fallback_name, latest_name, last_seen_at, updated_at
      FROM google_candidates
      UNION ALL
      SELECT campaign, source, medium, platform, account_id, campaign_id, fallback_name, latest_name, last_seen_at, updated_at
      FROM meta_candidates
    `, [startDate, endDate, normalizedCampaigns, source ?? null]);
    const byCampaign = new Map();
    const byGroup = new Map();
    const rowsByPlatform = new Map();
    for (const row of result.rows) {
        const resolution = buildResolution(row);
        const existingByCampaign = byCampaign.get(row.campaign);
        const existingByGroup = byGroup.get(buildCampaignResolutionGroupKey(row.source, row.medium, row.campaign));
        byCampaign.set(row.campaign, chooseBetterResolution(existingByCampaign, resolution));
        byGroup.set(buildCampaignResolutionGroupKey(row.source, row.medium, row.campaign), chooseBetterResolution(existingByGroup, resolution));
        const platformEntries = rowsByPlatform.get(row.platform) ?? [];
        platformEntries.push(resolution);
        rowsByPlatform.set(row.platform, platformEntries);
    }
    for (const [platform, resolutions] of rowsByPlatform) {
        const requestedCount = resolutions.length;
        const resolvedCount = resolutions.filter((entry) => entry.campaignNameResolutionStatus === 'resolved').length;
        const fallbackCount = resolutions.filter((entry) => entry.campaignNameResolutionStatus === 'fallback_name').length;
        const unresolved = resolutions.filter((entry) => entry.campaignNameResolutionStatus === 'unresolved');
        emitCampaignMetadataResolutionCoverageLog({
            resolutionScope: 'campaign_group',
            platform,
            entityType: 'campaign',
            requestedCount,
            matchedCount: requestedCount,
            resolvedCount,
            fallbackCount,
            unresolvedCount: unresolved.length,
            unresolvedEntityIds: unresolved.map((entry) => entry.campaignEntityId ?? 'unknown'),
            startDate,
            endDate,
            source: source ?? null
        });
    }
    const campaignResolutions = [...byCampaign.values()];
    emitCampaignMetadataResolutionCoverageLog({
        resolutionScope: 'campaign',
        platform: source === 'google' ? 'google_ads' : source === 'meta' ? 'meta_ads' : 'mixed',
        entityType: 'campaign',
        requestedCount: normalizedCampaigns.length,
        matchedCount: campaignResolutions.length,
        resolvedCount: campaignResolutions.filter((entry) => entry.campaignNameResolutionStatus === 'resolved').length,
        fallbackCount: campaignResolutions.filter((entry) => entry.campaignNameResolutionStatus === 'fallback_name').length,
        unresolvedCount: campaignResolutions.filter((entry) => entry.campaignNameResolutionStatus === 'unresolved').length,
        unresolvedEntityIds: campaignResolutions
            .filter((entry) => entry.campaignNameResolutionStatus === 'unresolved')
            .map((entry) => entry.campaignEntityId ?? 'unknown'),
        startDate,
        endDate,
        source: source ?? null
    });
    return {
        byCampaign,
        byGroup
    };
}
