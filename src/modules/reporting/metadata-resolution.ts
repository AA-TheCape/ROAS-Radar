import { query } from '../../db/pool.js';

export type MetadataPlatform = 'google_ads' | 'meta_ads';
export type MetadataResolutionStatus = 'resolved' | 'fallback_name' | 'unresolved';

type CampaignMetadataResolutionRow = {
  source: string;
  medium: string;
  campaign: string;
  platform: MetadataPlatform;
  account_id: string | null;
  entity_id: string | null;
  fallback_name: string | null;
  latest_name: string | null;
  last_seen_at: Date | null;
  updated_at: Date | null;
  rank_by_group: string | number;
  rank_by_campaign: string | number;
};

export type CampaignDisplayResolution = {
  campaign: string;
  source: string | null;
  medium: string | null;
  campaignDisplayName: string | null;
  campaignEntityId: string | null;
  campaignPlatform: MetadataPlatform | null;
  campaignNameResolutionStatus: MetadataResolutionStatus;
  lastSeenAt: string | null;
  updatedAt: string | null;
};

export type CampaignDisplayResolutionResult = {
  byCampaign: Map<string, CampaignDisplayResolution>;
  byGroup: Map<string, CampaignDisplayResolution>;
};

function normalizeName(value: string | null): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function buildResolution(row: Pick<
  CampaignMetadataResolutionRow,
  'campaign' | 'source' | 'medium' | 'platform' | 'entity_id' | 'fallback_name' | 'latest_name' | 'last_seen_at' | 'updated_at'
>): CampaignDisplayResolution {
  const latestName = normalizeName(row.latest_name);
  const fallbackName = normalizeName(row.fallback_name);
  const entityId = normalizeName(row.entity_id);

  if (latestName) {
    return {
      campaign: row.campaign,
      source: row.source,
      medium: row.medium,
      campaignDisplayName: latestName,
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
      lastSeenAt: row.last_seen_at?.toISOString() ?? null,
      updatedAt: row.updated_at?.toISOString() ?? null
    };
  }

  return {
    campaign: row.campaign,
    source: row.source,
    medium: row.medium,
    campaignDisplayName: entityId,
    campaignEntityId: entityId,
    campaignPlatform: row.platform,
    campaignNameResolutionStatus: 'unresolved',
    lastSeenAt: row.last_seen_at?.toISOString() ?? null,
    updatedAt: row.updated_at?.toISOString() ?? null
  };
}

export function buildCampaignResolutionGroupKey(source: string, medium: string, campaign: string): string {
  return `${source}\u0000${medium}\u0000${campaign}`;
}

export async function resolveCampaignDisplayMetadata(
  startDate: string,
  endDate: string,
  campaignKeys: string[],
  source?: string
): Promise<CampaignDisplayResolutionResult> {
  const normalizedCampaignKeys = [...new Set(campaignKeys.map((value) => value.trim()).filter((value) => value.length > 0))];

  if (normalizedCampaignKeys.length === 0) {
    return {
      byCampaign: new Map(),
      byGroup: new Map()
    };
  }

  const result = await query<CampaignMetadataResolutionRow>(
    `
      WITH spend_candidates AS (
        SELECT
          canonical_source AS source,
          canonical_medium AS medium,
          canonical_campaign AS campaign,
          'meta_ads'::text AS platform,
          NULLIF(btrim(account_id), '') AS account_id,
          NULLIF(btrim(campaign_id), '') AS entity_id,
          NULLIF(regexp_replace(btrim(campaign_name), '\\s+', ' ', 'g'), '') AS fallback_name,
          COALESCE(SUM(spend), 0)::numeric(12, 2) AS spend,
          MAX(report_date) AS last_report_date
        FROM meta_ads_daily_spend
        WHERE report_date BETWEEN $1::date AND $2::date
          AND canonical_campaign = ANY($3::text[])
          AND ($4::text IS NULL OR canonical_source = $4::text)
          AND granularity IN ('campaign', 'adset', 'ad', 'creative')
        GROUP BY 1, 2, 3, 4, 5, 6, 7

        UNION ALL

        SELECT
          canonical_source AS source,
          canonical_medium AS medium,
          canonical_campaign AS campaign,
          'google_ads'::text AS platform,
          NULLIF(btrim(account_id), '') AS account_id,
          NULLIF(btrim(campaign_id), '') AS entity_id,
          NULLIF(regexp_replace(btrim(campaign_name), '\\s+', ' ', 'g'), '') AS fallback_name,
          COALESCE(SUM(spend), 0)::numeric(12, 2) AS spend,
          MAX(report_date) AS last_report_date
        FROM google_ads_daily_spend
        WHERE report_date BETWEEN $1::date AND $2::date
          AND canonical_campaign = ANY($3::text[])
          AND ($4::text IS NULL OR canonical_source = $4::text)
          AND granularity IN ('campaign', 'adset', 'ad', 'creative')
        GROUP BY 1, 2, 3, 4, 5, 6, 7
      ),
      ranked_candidates AS (
        SELECT
          c.source,
          c.medium,
          c.campaign,
          c.platform,
          c.account_id,
          c.entity_id,
          c.fallback_name,
          m.latest_name,
          m.last_seen_at,
          m.updated_at,
          ROW_NUMBER() OVER (
            PARTITION BY c.source, c.medium, c.campaign
            ORDER BY c.spend DESC, c.last_report_date DESC, c.platform ASC, c.account_id ASC NULLS LAST, c.entity_id ASC NULLS LAST
          ) AS rank_by_group,
          ROW_NUMBER() OVER (
            PARTITION BY c.campaign
            ORDER BY c.spend DESC, c.last_report_date DESC, c.platform ASC, c.account_id ASC NULLS LAST, c.entity_id ASC NULLS LAST
          ) AS rank_by_campaign
        FROM spend_candidates c
        LEFT JOIN ad_platform_entity_metadata m
          ON m.platform = c.platform
          AND m.account_id = c.account_id
          AND m.entity_type = 'campaign'
          AND m.entity_id = c.entity_id
          AND m.tenant_id IS NULL
          AND m.workspace_id IS NULL
      )
      SELECT
        source,
        medium,
        campaign,
        platform,
        account_id,
        entity_id,
        fallback_name,
        latest_name,
        last_seen_at,
        updated_at,
        rank_by_group,
        rank_by_campaign
      FROM ranked_candidates
      WHERE rank_by_group = 1 OR rank_by_campaign = 1
    `,
    [startDate, endDate, normalizedCampaignKeys, source ?? null]
  );

  const byCampaign = new Map<string, CampaignDisplayResolution>();
  const byGroup = new Map<string, CampaignDisplayResolution>();

  for (const row of result.rows) {
    const resolution = buildResolution(row);
    const rankByCampaign = Number(row.rank_by_campaign);
    const rankByGroup = Number(row.rank_by_group);

    if (rankByCampaign === 1) {
      byCampaign.set(row.campaign, resolution);
    }

    if (rankByGroup === 1) {
      byGroup.set(buildCampaignResolutionGroupKey(row.source, row.medium, row.campaign), resolution);
    }
  }

  return {
    byCampaign,
    byGroup
  };
}
