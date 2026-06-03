import { query } from '../../db/pool.js';
import {
  emitCampaignMetadataResolutionCoverageLog,
  emitMetaMetadataRawIdFallbackLog
} from '../../observability/index.js';
import { env } from '../../config/env.js';
import { resolveMetaMetadata, type MetaMetadataObjectType } from '../meta-ads/metadata-resolver.js';

export type CampaignNameResolutionStatus = 'resolved' | 'fallback_name' | 'unresolved';

export type CampaignDisplayResolution = {
  campaign: string;
  source: string;
  medium: string;
  campaignDisplayName: string;
  campaignEntityId: string | null;
  campaignEntityType?: 'campaign' | 'adset';
  parentCampaignEntityId?: string | null;
  parentCampaignDisplayName?: string | null;
  campaignPlatform: 'google_ads' | 'meta_ads' | null;
  campaignNameResolutionStatus: CampaignNameResolutionStatus;
  lastSeenAt: string | null;
  updatedAt: string | null;
};

type CampaignResolutionRow = {
  campaign: string;
  source: string;
  medium: string;
  platform: 'google_ads' | 'meta_ads';
  account_id: string | null;
  campaign_id: string | null;
  entity_id?: string | null;
  fallback_name: string | null;
  latest_name: string | null;
  last_seen_at: Date | null;
  updated_at: Date | null;
};

type MetaAttributedIdAccountRow = {
  ad_account_id: string;
  object_type: MetaMetadataObjectType;
  object_id: string;
  object_name: string | null;
  parent_campaign_id: string | null;
  parent_campaign_name: string | null;
  last_seen_at: Date | null;
  metadata_source: 'ad_platform_entity_metadata' | 'spend' | 'cache' | 'active_account';
};

type MetaAttributedIdCandidate = {
  adAccountId: string;
  objectType: MetaMetadataObjectType;
  objectId: string;
  objectName: string | null;
  parentCampaignId: string | null;
  parentCampaignName: string | null;
  lastSeenAt: string | null;
  metadataSource: MetaAttributedIdAccountRow['metadata_source'];
};

export function buildCampaignResolutionGroupKey(source: string, medium: string, campaign: string): string {
  return `${source}\u0000${medium}\u0000${campaign}`;
}

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

function chooseBetterResolution(
  current: CampaignDisplayResolution | undefined,
  candidate: CampaignDisplayResolution
): CampaignDisplayResolution {
  if (!current) {
    return candidate;
  }

  const rank = (value: CampaignNameResolutionStatus): number => {
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

function buildResolutionFingerprint(resolution: CampaignDisplayResolution): string {
  return [
    resolution.campaignDisplayName,
    resolution.campaignEntityId ?? '',
    resolution.campaignEntityType ?? '',
    resolution.parentCampaignEntityId ?? '',
    resolution.parentCampaignDisplayName ?? '',
    resolution.campaignPlatform ?? '',
    resolution.campaignNameResolutionStatus
  ].join('\u0000');
}

function collapseScopedResolutions(
  resolutions: CampaignDisplayResolution[]
): CampaignDisplayResolution | undefined {
  if (resolutions.length === 0) {
    return undefined;
  }

  const resolvedResolutions = resolutions.filter(
    (resolution) => resolution.campaignNameResolutionStatus === 'resolved'
  );
  const conflictScope = resolvedResolutions.length > 0 ? resolvedResolutions : resolutions;
  const fingerprints = new Set(conflictScope.map(buildResolutionFingerprint));

  if (fingerprints.size > 1) {
    return undefined;
  }

  let winner: CampaignDisplayResolution | undefined;

  for (const resolution of conflictScope) {
    winner = chooseBetterResolution(winner, resolution);
  }

  return winner;
}

function buildResolution(row: CampaignResolutionRow): CampaignDisplayResolution {
  const resolvedName = collapseWhitespace(row.latest_name);
  const fallbackName = collapseWhitespace(row.fallback_name);
  const entityId = normalizeString(row.campaign_id ?? row.entity_id);

  if (resolvedName) {
    return {
      campaign: row.campaign,
      source: row.source,
      medium: row.medium,
      campaignDisplayName: resolvedName,
      campaignEntityId: entityId,
      campaignEntityType: 'campaign',
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
      campaignEntityType: 'campaign',
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
    campaignEntityType: 'campaign',
    campaignPlatform: row.platform,
    campaignNameResolutionStatus: 'unresolved',
    lastSeenAt: null,
    updatedAt: null
  };
}

function buildMetaAttributedIdResolution(input: {
  campaign: string;
  objectId: string;
  objectType: MetaMetadataObjectType;
  objectName: string;
  parentCampaignId?: string | null;
  parentCampaignName?: string | null;
  lastFetchedAt: string | null;
}): CampaignDisplayResolution {
  return {
    campaign: input.campaign,
    source: 'meta',
    medium: 'paid_social',
    campaignDisplayName: input.objectName,
    campaignEntityId: input.objectId,
    campaignEntityType: input.objectType,
    parentCampaignEntityId: input.objectType === 'adset' ? normalizeString(input.parentCampaignId) : null,
    parentCampaignDisplayName: input.objectType === 'adset' ? collapseWhitespace(input.parentCampaignName) : null,
    campaignPlatform: 'meta_ads',
    campaignNameResolutionStatus: 'resolved',
    lastSeenAt: input.lastFetchedAt,
    updatedAt: input.lastFetchedAt
  };
}

function isNumericMetaObjectId(value: string): boolean {
  return /^\d+$/.test(value.trim());
}

async function resolveAttributedMetaIdMetadata(
  startDate: string,
  endDate: string,
  campaigns: string[],
  source?: string | null
): Promise<Map<string, CampaignDisplayResolution>> {
  const candidateIds = [...new Set(campaigns.map((value) => value.trim()).filter(isNumericMetaObjectId))];

  if (candidateIds.length === 0) {
    return new Map();
  }

  const candidateMap = new Map<string, MetaAttributedIdCandidate[]>();

  const accountRows = await query<MetaAttributedIdAccountRow>(
    `
      WITH requested_ids AS (
        SELECT unnest($1::text[]) AS object_id
      ),
      spend_matches AS (
        SELECT DISTINCT
          mads.account_id AS ad_account_id,
          'campaign'::text AS object_type,
          mads.campaign_id AS object_id,
          mads.campaign_name AS object_name,
          NULL::text AS parent_campaign_id,
          NULL::text AS parent_campaign_name,
          MAX(mads.report_date)::timestamptz AS last_seen_at,
          'spend'::text AS metadata_source
        FROM meta_ads_daily_spend mads
        JOIN requested_ids requested
          ON requested.object_id = mads.campaign_id
        WHERE mads.report_date BETWEEN $2::date AND $3::date
          AND mads.account_id IS NOT NULL
          AND mads.campaign_id IS NOT NULL
        GROUP BY mads.account_id, mads.campaign_id, mads.campaign_name

        UNION

        SELECT DISTINCT
          mads.account_id AS ad_account_id,
          'adset'::text AS object_type,
          mads.adset_id AS object_id,
          mads.adset_name AS object_name,
          mads.campaign_id AS parent_campaign_id,
          mads.campaign_name AS parent_campaign_name,
          MAX(mads.report_date)::timestamptz AS last_seen_at,
          'spend'::text AS metadata_source
        FROM meta_ads_daily_spend mads
        JOIN requested_ids requested
          ON requested.object_id = mads.adset_id
        WHERE mads.report_date BETWEEN $2::date AND $3::date
          AND mads.account_id IS NOT NULL
          AND mads.adset_id IS NOT NULL
        GROUP BY mads.account_id, mads.adset_id, mads.adset_name, mads.campaign_id, mads.campaign_name
      ),
      platform_metadata_matches AS (
        SELECT DISTINCT
          metadata.account_id AS ad_account_id,
          metadata.entity_type AS object_type,
          metadata.entity_id AS object_id,
          metadata.latest_name AS object_name,
          NULL::text AS parent_campaign_id,
          NULL::text AS parent_campaign_name,
          metadata.last_seen_at,
          'ad_platform_entity_metadata'::text AS metadata_source
        FROM ad_platform_entity_metadata metadata
        JOIN requested_ids requested
          ON requested.object_id = metadata.entity_id
        WHERE metadata.platform = 'meta_ads'
          AND metadata.entity_type IN ('campaign', 'adset')
      ),
      cache_matches AS (
        SELECT DISTINCT
          cache.ad_account_id,
          cache.object_type,
          cache.object_id,
          cache.object_name,
          NULL::text AS parent_campaign_id,
          NULL::text AS parent_campaign_name,
          cache.last_fetched_at AS last_seen_at,
          'cache'::text AS metadata_source
        FROM meta_ads_metadata_cache cache
        JOIN requested_ids requested
          ON requested.object_id = cache.object_id
        WHERE cache.object_type IN ('campaign', 'adset')
          AND cache.last_fetched_at IS NOT NULL
          AND cache.object_name IS NOT NULL
      ),
      active_accounts AS (
        SELECT ad_account_id
        FROM meta_ads_connections
        WHERE status = 'active'
          AND $5::boolean

        UNION

        SELECT NULLIF(regexp_replace($4::text, '^act_', ''), '')
        WHERE $6::boolean
      )
      SELECT ad_account_id, object_type, object_id
           , object_name, parent_campaign_id, parent_campaign_name, last_seen_at, metadata_source
      FROM spend_matches
      UNION
      SELECT ad_account_id, object_type, object_id
           , object_name, parent_campaign_id, parent_campaign_name, last_seen_at, metadata_source
      FROM platform_metadata_matches
      UNION
      SELECT ad_account_id, object_type, object_id
           , object_name, parent_campaign_id, parent_campaign_name, last_seen_at, metadata_source
      FROM cache_matches
      UNION
      SELECT
        active_accounts.ad_account_id,
        object_types.object_type,
        requested_ids.object_id,
        NULL::text AS object_name,
        NULL::text AS parent_campaign_id,
        NULL::text AS parent_campaign_name,
        NULL::timestamptz AS last_seen_at,
        'active_account'::text AS metadata_source
      FROM active_accounts
      CROSS JOIN requested_ids
      CROSS JOIN (VALUES ('campaign'::text), ('adset'::text)) AS object_types(object_type)
      WHERE active_accounts.ad_account_id IS NOT NULL
    `,
    [
      candidateIds,
      startDate,
      endDate,
      env.META_ADS_AD_ACCOUNT_ID || null,
      Boolean(env.META_ADS_ENCRYPTION_KEY),
      Boolean(env.META_ADS_AD_ACCOUNT_ID && env.META_ADS_METADATA_ACCESS_TOKEN)
    ]
  );

  const requestMap = new Map<
    string,
    {
      adAccountId: string;
      objectType: MetaMetadataObjectType;
      objectIds: string[];
    }
  >();

  for (const row of accountRows.rows) {
    const adAccountId = row.ad_account_id?.trim();
    const objectId = row.object_id?.trim();

    if (!adAccountId || !objectId || !['campaign', 'adset'].includes(row.object_type)) {
      continue;
    }

    const objectType = row.object_type as MetaMetadataObjectType;
    const candidateKey = `${adAccountId}\u0000${objectType}\u0000${objectId}`;
    const candidates = candidateMap.get(candidateKey) ?? [];

    candidates.push({
      adAccountId,
      objectType,
      objectId,
      objectName: collapseWhitespace(row.object_name),
      parentCampaignId: normalizeString(row.parent_campaign_id),
      parentCampaignName: collapseWhitespace(row.parent_campaign_name),
      lastSeenAt: row.last_seen_at?.toISOString() ?? null,
      metadataSource: row.metadata_source
    });
    candidateMap.set(candidateKey, candidates);

    const key = `${adAccountId}\u0000${objectType}`;
    const request = requestMap.get(key) ?? {
      adAccountId,
      objectType,
      objectIds: []
    };

    request.objectIds.push(objectId);
    requestMap.set(key, request);
  }

  if (requestMap.size === 0) {
    return new Map();
  }

  const metaResult = await resolveMetaMetadata(
    [...requestMap.values()].map((request) => ({
      ...request,
      objectIds: [...new Set(request.objectIds)]
    }))
  );

  if (metaResult.unresolved.length > 0) {
    emitMetaMetadataRawIdFallbackLog({
      resolutionScope: 'campaign_adset_metadata',
      startDate,
      endDate,
      source,
      requestedCount: candidateIds.length,
      unresolvedCount: metaResult.unresolved.length,
      unresolvedEntityIds: metaResult.unresolved.map((entry) => entry.objectId),
      unresolvedReasons: metaResult.unresolved.reduce<Record<string, number>>((summary, entry) => {
        summary[entry.reason] = (summary[entry.reason] ?? 0) + 1;
        return summary;
      }, {})
    });
  }

  const resolutionsByCampaign = new Map<string, CampaignDisplayResolution[]>();

  for (const candidates of candidateMap.values()) {
    const authoritativeMetadataCandidates = candidates.filter(
      (candidate) =>
        (candidate.metadataSource === 'ad_platform_entity_metadata' || candidate.metadataSource === 'cache') &&
        candidate.objectName
    );

    if (authoritativeMetadataCandidates.length === 0) {
      continue;
    }

    const bestCandidate = authoritativeMetadataCandidates.reduce<MetaAttributedIdCandidate | undefined>(
      (current, candidate) => {
        if (!current) {
          return candidate;
        }

        const currentTimestamp = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
        const candidateTimestamp = candidate.lastSeenAt ? Date.parse(candidate.lastSeenAt) : 0;

        return candidateTimestamp > currentTimestamp ? candidate : current;
      },
      undefined
    );
    const parentCandidate = candidates.reduce<MetaAttributedIdCandidate | undefined>((current, candidate) => {
      if (!current) {
        return candidate;
      }

      const currentHasParent = Boolean(current.parentCampaignId || current.parentCampaignName);
      const candidateHasParent = Boolean(candidate.parentCampaignId || candidate.parentCampaignName);

      if (candidateHasParent !== currentHasParent) {
        return candidateHasParent ? candidate : current;
      }

      const currentTimestamp = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
      const candidateTimestamp = candidate.lastSeenAt ? Date.parse(candidate.lastSeenAt) : 0;

      return candidateTimestamp > currentTimestamp ? candidate : current;
    }, undefined);

    if (!bestCandidate?.objectName) {
      continue;
    }

    const resolutions = resolutionsByCampaign.get(bestCandidate.objectId) ?? [];

    resolutions.push(
      buildMetaAttributedIdResolution({
        campaign: bestCandidate.objectId,
        objectId: bestCandidate.objectId,
        objectType: bestCandidate.objectType,
        objectName: bestCandidate.objectName,
        parentCampaignId: parentCandidate?.parentCampaignId ?? null,
        parentCampaignName: parentCandidate?.parentCampaignName ?? null,
        lastFetchedAt: bestCandidate.lastSeenAt
      })
    );
    resolutionsByCampaign.set(bestCandidate.objectId, resolutions);
  }

  for (const resolved of metaResult.resolved) {
    const candidateKey = `${resolved.adAccountId}\u0000${resolved.objectType}\u0000${resolved.objectId}`;
    const candidateMetadata = candidateMap.get(candidateKey) ?? [];
    const authoritativeCandidates = candidateMetadata.filter(
      (candidate) =>
        (candidate.metadataSource === 'ad_platform_entity_metadata' || candidate.metadataSource === 'cache') &&
        candidate.objectName
    );
    const bestCandidate = candidateMetadata.reduce<MetaAttributedIdCandidate | undefined>((current, candidate) => {
      if (!current) {
        return candidate;
      }

      const currentHasName = Boolean(current.objectName);
      const candidateHasName = Boolean(candidate.objectName);

      if (candidateHasName !== currentHasName) {
        return candidateHasName ? candidate : current;
      }

      const currentTimestamp = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
      const candidateTimestamp = candidate.lastSeenAt ? Date.parse(candidate.lastSeenAt) : 0;

      return candidateTimestamp > currentTimestamp ? candidate : current;
    }, undefined);
    const parentCandidate = candidateMetadata.reduce<MetaAttributedIdCandidate | undefined>((current, candidate) => {
      if (!current) {
        return candidate;
      }

      const currentHasParent = Boolean(current.parentCampaignId || current.parentCampaignName);
      const candidateHasParent = Boolean(candidate.parentCampaignId || candidate.parentCampaignName);

      if (candidateHasParent !== currentHasParent) {
        return candidateHasParent ? candidate : current;
      }

      const currentTimestamp = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
      const candidateTimestamp = candidate.lastSeenAt ? Date.parse(candidate.lastSeenAt) : 0;

      return candidateTimestamp > currentTimestamp ? candidate : current;
    }, undefined);
    const authoritativeCandidate = authoritativeCandidates.reduce<MetaAttributedIdCandidate | undefined>(
      (current, candidate) => {
        if (!current) {
          return candidate;
        }

        const currentTimestamp = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
        const candidateTimestamp = candidate.lastSeenAt ? Date.parse(candidate.lastSeenAt) : 0;

        return candidateTimestamp > currentTimestamp ? candidate : current;
      },
      undefined
    );
    const candidate = buildMetaAttributedIdResolution({
      campaign: resolved.objectId,
      objectId: resolved.objectId,
      objectType: resolved.objectType,
      objectName: authoritativeCandidate?.objectName ?? resolved.objectName,
      parentCampaignId: parentCandidate?.parentCampaignId ?? null,
      parentCampaignName: parentCandidate?.parentCampaignName ?? null,
      lastFetchedAt: authoritativeCandidate?.lastSeenAt ?? bestCandidate?.lastSeenAt ?? resolved.lastFetchedAt
    });
    const resolutions = resolutionsByCampaign.get(resolved.objectId) ?? [];

    resolutions.push(candidate);
    resolutionsByCampaign.set(resolved.objectId, resolutions);
  }

  for (const unresolved of metaResult.unresolved) {
    const existingResolutions = resolutionsByCampaign.get(unresolved.objectId) ?? [];

    if (
      existingResolutions.some(
        (resolution) =>
          resolution.campaignEntityId === unresolved.objectId &&
          resolution.campaignEntityType === unresolved.objectType &&
          resolution.campaignNameResolutionStatus === 'resolved'
      )
    ) {
      continue;
    }

    const candidateKey = `${unresolved.adAccountId}\u0000${unresolved.objectType}\u0000${unresolved.objectId}`;
    const candidateMetadata = candidateMap.get(candidateKey) ?? [];
    const bestCandidate = candidateMetadata.reduce<MetaAttributedIdCandidate | undefined>((current, candidate) => {
      if (!current) {
        return candidate;
      }

      const currentTimestamp = current.lastSeenAt ? Date.parse(current.lastSeenAt) : 0;
      const candidateTimestamp = candidate.lastSeenAt ? Date.parse(candidate.lastSeenAt) : 0;

      return candidateTimestamp > currentTimestamp ? candidate : current;
    }, undefined);
    const resolutions = resolutionsByCampaign.get(unresolved.objectId) ?? [];

    resolutions.push({
      campaign: unresolved.objectId,
      source: source ?? 'unknown',
      medium: 'unknown',
      campaignDisplayName: unresolved.objectId,
      campaignEntityId: unresolved.objectId,
      campaignEntityType: unresolved.objectType,
      parentCampaignEntityId: unresolved.objectType === 'adset' ? normalizeString(bestCandidate?.parentCampaignId) : null,
      parentCampaignDisplayName: unresolved.objectType === 'adset' ? collapseWhitespace(bestCandidate?.parentCampaignName) : null,
      campaignPlatform: null,
      campaignNameResolutionStatus: 'unresolved',
      lastSeenAt: bestCandidate?.lastSeenAt ?? null,
      updatedAt: null
    });
    resolutionsByCampaign.set(unresolved.objectId, resolutions);
  }

  const byCampaign = new Map<string, CampaignDisplayResolution>();

  for (const [campaign, resolutions] of resolutionsByCampaign) {
    const collapsed = collapseScopedResolutions(resolutions);

    if (collapsed) {
      byCampaign.set(campaign, collapsed);
    }
  }

  return byCampaign;
}

export async function resolveCampaignDisplayMetadata(
  startDate: string,
  endDate: string,
  campaigns: string[],
  source?: string
): Promise<{
  byCampaign: Map<string, CampaignDisplayResolution>;
  byGroup: Map<string, CampaignDisplayResolution>;
}> {
  const normalizedCampaigns = [...new Set(campaigns.map((value) => value.trim()).filter(Boolean))];

  if (normalizedCampaigns.length === 0) {
    return {
      byCampaign: new Map(),
      byGroup: new Map()
    };
  }

  const result = await query<CampaignResolutionRow>(
    `
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
    `,
    [startDate, endDate, normalizedCampaigns, source ?? null]
  );

  const byCampaign = new Map<string, CampaignDisplayResolution>();
  const byGroup = new Map<string, CampaignDisplayResolution>();
  const attributedMetaIdMetadata =
    source === undefined || source === 'meta' || source === 'facebook' || source === 'instagram'
      ? await resolveAttributedMetaIdMetadata(startDate, endDate, normalizedCampaigns, source ?? null)
      : new Map<string, CampaignDisplayResolution>();
  const rowsByPlatform = new Map<'google_ads' | 'meta_ads', CampaignDisplayResolution[]>();
  const scopedCampaignCandidates = new Map<string, CampaignDisplayResolution[]>();
  const scopedGroupCandidates = new Map<string, CampaignDisplayResolution[]>();

  for (const row of result.rows) {
    const resolution = buildResolution(row);
    const groupKey = buildCampaignResolutionGroupKey(row.source, row.medium, row.campaign);
    const campaignCandidates = scopedCampaignCandidates.get(row.campaign) ?? [];
    const groupCandidates = scopedGroupCandidates.get(groupKey) ?? [];

    campaignCandidates.push(resolution);
    groupCandidates.push(resolution);

    scopedCampaignCandidates.set(row.campaign, campaignCandidates);
    scopedGroupCandidates.set(groupKey, groupCandidates);

    const platformEntries = rowsByPlatform.get(row.platform) ?? [];
    platformEntries.push(resolution);
    rowsByPlatform.set(row.platform, platformEntries);
  }

  for (const [campaign, resolutions] of scopedCampaignCandidates) {
    const collapsed = collapseScopedResolutions(resolutions);

    if (collapsed) {
      byCampaign.set(campaign, collapsed);
    }
  }

  for (const [groupKey, resolutions] of scopedGroupCandidates) {
    const collapsed = collapseScopedResolutions(resolutions);

    if (collapsed) {
      byGroup.set(groupKey, collapsed);
    }
  }

  for (const [campaign, resolution] of attributedMetaIdMetadata) {
    byCampaign.set(campaign, chooseBetterResolution(byCampaign.get(campaign), resolution));
    byGroup.set(
      buildCampaignResolutionGroupKey(resolution.source, resolution.medium, campaign),
      chooseBetterResolution(
        byGroup.get(buildCampaignResolutionGroupKey(resolution.source, resolution.medium, campaign)),
        resolution
      )
    );
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
