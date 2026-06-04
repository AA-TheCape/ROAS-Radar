import type { PoolClient, QueryResult, QueryResultRow } from 'pg';
import { z } from 'zod';

import { query } from '../../db/pool.js';

export const DEFAULT_CAMPAIGN_RESOLVER_VERSION = 'campaign_metadata_resolver_v1';

type Queryable = {
  query<T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]): Promise<QueryResult<T>>;
};

export type CampaignResolverInput = {
  resolverVersion?: string | null;
  platform?: string | null;
  source?: string | null;
  medium?: string | null;
  campaign?: string | null;
  content?: string | null;
  term?: string | null;
  accountId?: string | null;
  campaignId?: string | null;
  adsetId?: string | null;
  adId?: string | null;
  occurredAt?: Date | string | null;
  samplePayload?: Record<string, unknown> | null;
  enqueueUnmapped?: boolean;
};

export type CampaignResolverStatus = 'resolved' | 'fallback' | 'unmapped';

export type CampaignResolverOutput = {
  status: CampaignResolverStatus;
  resolverVersion: string;
  source: 'override' | 'rule' | 'heuristic' | 'unmapped';
  confidence: number;
  ruleId: string | null;
  canonical: {
    campaignId: string | null;
    campaignName: string | null;
    source: string | null;
    medium: string | null;
    channel: string | null;
    channelGroup: string | null;
    hierarchy: Record<string, unknown>;
  };
  qaQueueId: string | null;
};

type ResolverRuleRow = {
  id: string;
  resolver_version: string;
  rule_kind: 'rule' | 'override';
  priority: number;
  match_platform: string | null;
  match_source: string | null;
  match_medium: string | null;
  match_campaign: string | null;
  match_content: string | null;
  match_term: string | null;
  match_account_id: string | null;
  match_campaign_id: string | null;
  match_adset_id: string | null;
  match_ad_id: string | null;
  match_expression: Record<string, unknown>;
  canonical_campaign_id: string | null;
  canonical_campaign_name: string;
  canonical_source: string;
  canonical_medium: string;
  canonical_channel: string;
  canonical_channel_group: string;
  hierarchy_metadata: Record<string, unknown>;
  confidence: string | number;
  source_label: string;
};

type QaQueueRow = {
  id: string;
};

type MmmBackfillRow = {
  metric_date: string;
  mart_version: string;
  mart_row_type: string;
  attribution_model: string;
  platform: string;
  granularity: string;
  entity_key: string;
  source: string;
  medium: string;
  campaign: string;
  content: string;
  term: string;
  account_id: string | null;
  campaign_id: string | null;
  adset_id: string | null;
  ad_id: string | null;
};

type NormalizedCampaignResolverInput = {
  resolverVersion: string;
  platform: string | null;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  accountId: string | null;
  campaignId: string | null;
  adsetId: string | null;
  adId: string | null;
  occurredAt: Date | string | null;
  enqueueUnmapped: boolean;
  samplePayload: Record<string, unknown>;
};

export type CampaignMetadataBackfillResult = {
  resolverVersion: string;
  processedRows: number;
  resolvedRows: number;
  fallbackRows: number;
  unmappedRows: number;
  qaQueuedRows: number;
};

export const campaignResolverRequestSchema = z.object({
  resolverVersion: z.string().trim().min(1).max(200).optional(),
  platform: z.string().trim().min(1).max(100).nullable().optional(),
  source: z.string().trim().min(1).max(200).nullable().optional(),
  medium: z.string().trim().min(1).max(200).nullable().optional(),
  campaign: z.string().trim().min(1).max(500).nullable().optional(),
  content: z.string().trim().min(1).max(500).nullable().optional(),
  term: z.string().trim().min(1).max(500).nullable().optional(),
  accountId: z.string().trim().min(1).max(200).nullable().optional(),
  campaignId: z.string().trim().min(1).max(200).nullable().optional(),
  adsetId: z.string().trim().min(1).max(200).nullable().optional(),
  adId: z.string().trim().min(1).max(200).nullable().optional(),
  occurredAt: z.string().datetime().nullable().optional(),
  samplePayload: z.record(z.unknown()).nullable().optional(),
  enqueueUnmapped: z.boolean().optional()
});

function db(client?: Queryable | PoolClient): Queryable {
  return client ?? { query };
}

function normalizeString(value: string | null | undefined): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed ? trimmed.replace(/\s+/g, ' ') : null;
}

function normalizeComparable(value: string | null | undefined): string | null {
  const normalized = normalizeString(value);
  return normalized ? normalized.toLowerCase() : null;
}

function normalizeInput(input: CampaignResolverInput): NormalizedCampaignResolverInput {
  return {
    resolverVersion: normalizeString(input.resolverVersion) ?? DEFAULT_CAMPAIGN_RESOLVER_VERSION,
    platform: normalizeComparable(input.platform),
    source: normalizeComparable(input.source),
    medium: normalizeComparable(input.medium),
    campaign: normalizeComparable(input.campaign),
    content: normalizeComparable(input.content),
    term: normalizeComparable(input.term),
    accountId: normalizeString(input.accountId),
    campaignId: normalizeString(input.campaignId),
    adsetId: normalizeString(input.adsetId),
    adId: normalizeString(input.adId),
    occurredAt: input.occurredAt ?? null,
    enqueueUnmapped: input.enqueueUnmapped ?? true,
    samplePayload: input.samplePayload ?? {}
  };
}

function platformToSource(platform: string | null): string | null {
  switch (platform) {
    case 'google':
    case 'google_ads':
      return 'google';
    case 'meta':
    case 'meta_ads':
    case 'facebook':
    case 'instagram':
      return 'meta';
    default:
      return null;
  }
}

function inferChannel(source: string | null, medium: string | null, platform: string | null): {
  channel: string | null;
  channelGroup: string | null;
} {
  const normalizedSource = source ?? platformToSource(platform);
  const normalizedMedium = medium;

  if (normalizedSource === 'google' && ['cpc', 'paid_search', 'ppc'].includes(normalizedMedium ?? '')) {
    return { channel: 'paid_search', channelGroup: 'paid_media' };
  }

  if (
    normalizedSource === 'meta' ||
    ['facebook', 'instagram'].includes(normalizedSource ?? '') ||
    ['paid_social', 'social_paid'].includes(normalizedMedium ?? '')
  ) {
    return { channel: 'paid_social', channelGroup: 'paid_media' };
  }

  if (['email', 'newsletter'].includes(normalizedMedium ?? '') || normalizedSource === 'klaviyo') {
    return { channel: 'email', channelGroup: 'owned' };
  }

  if (normalizedMedium === 'organic') {
    return { channel: 'organic', channelGroup: 'earned' };
  }

  return { channel: null, channelGroup: null };
}

function matchString(ruleValue: string | null, inputValue: string | null): boolean {
  if (!ruleValue) {
    return true;
  }

  return normalizeComparable(ruleValue) === normalizeComparable(inputValue);
}

function matchPattern(pattern: unknown, inputValue: string | null): boolean {
  if (typeof pattern !== 'string' || !pattern.trim()) {
    return true;
  }

  if (!inputValue) {
    return false;
  }

  try {
    return new RegExp(pattern, 'i').test(inputValue);
  } catch {
    return false;
  }
}

function ruleMatches(rule: ResolverRuleRow, input: ReturnType<typeof normalizeInput>): boolean {
  const exactMatches =
    matchString(rule.match_platform, input.platform) &&
    matchString(rule.match_source, input.source) &&
    matchString(rule.match_medium, input.medium) &&
    matchString(rule.match_campaign, input.campaign) &&
    matchString(rule.match_content, input.content) &&
    matchString(rule.match_term, input.term) &&
    matchString(rule.match_account_id, input.accountId) &&
    matchString(rule.match_campaign_id, input.campaignId) &&
    matchString(rule.match_adset_id, input.adsetId) &&
    matchString(rule.match_ad_id, input.adId);

  if (!exactMatches) {
    return false;
  }

  const expression = rule.match_expression ?? {};
  return (
    matchPattern(expression.platformPattern, input.platform) &&
    matchPattern(expression.sourcePattern, input.source) &&
    matchPattern(expression.mediumPattern, input.medium) &&
    matchPattern(expression.campaignPattern, input.campaign) &&
    matchPattern(expression.contentPattern, input.content) &&
    matchPattern(expression.termPattern, input.term) &&
    matchPattern(expression.accountIdPattern, input.accountId) &&
    matchPattern(expression.campaignIdPattern, input.campaignId) &&
    matchPattern(expression.adsetIdPattern, input.adsetId) &&
    matchPattern(expression.adIdPattern, input.adId)
  );
}

async function loadCandidateRules(
  client: Queryable,
  input: ReturnType<typeof normalizeInput>
): Promise<ResolverRuleRow[]> {
  const occurredAt = input.occurredAt ? new Date(input.occurredAt) : new Date();
  const result = await client.query<ResolverRuleRow>(
    `
      SELECT
        id,
        resolver_version,
        rule_kind,
        priority,
        match_platform,
        match_source,
        match_medium,
        match_campaign,
        match_content,
        match_term,
        match_account_id,
        match_campaign_id,
        match_adset_id,
        match_ad_id,
        match_expression,
        canonical_campaign_id,
        canonical_campaign_name,
        canonical_source,
        canonical_medium,
        canonical_channel,
        canonical_channel_group,
        hierarchy_metadata,
        confidence,
        source_label
      FROM campaign_metadata_resolver_rules
      WHERE active = true
        AND resolver_version = $1
        AND effective_from <= $2::timestamptz
        AND effective_to > $2::timestamptz
        AND (match_platform IS NULL OR lower(match_platform) = $3)
        AND (match_source IS NULL OR lower(match_source) = $4)
        AND (match_medium IS NULL OR lower(match_medium) = $5)
        AND (match_campaign IS NULL OR lower(match_campaign) = $6)
        AND (match_content IS NULL OR lower(match_content) = $7)
        AND (match_term IS NULL OR lower(match_term) = $8)
        AND (match_account_id IS NULL OR match_account_id = $9)
        AND (match_campaign_id IS NULL OR match_campaign_id = $10)
        AND (match_adset_id IS NULL OR match_adset_id = $11)
        AND (match_ad_id IS NULL OR match_ad_id = $12)
      ORDER BY
        CASE rule_kind WHEN 'override' THEN 0 ELSE 1 END ASC,
        priority ASC,
        updated_at DESC
    `,
    [
      input.resolverVersion,
      occurredAt,
      input.platform,
      input.source,
      input.medium,
      input.campaign,
      input.content,
      input.term,
      input.accountId,
      input.campaignId,
      input.adsetId,
      input.adId
    ]
  );

  return result.rows;
}

function outputFromRule(rule: ResolverRuleRow): CampaignResolverOutput {
  return {
    status: 'resolved',
    resolverVersion: rule.resolver_version,
    source: rule.rule_kind === 'override' ? 'override' : 'rule',
    confidence: Number(rule.confidence),
    ruleId: rule.id,
    canonical: {
      campaignId: rule.canonical_campaign_id,
      campaignName: rule.canonical_campaign_name,
      source: rule.canonical_source,
      medium: rule.canonical_medium,
      channel: rule.canonical_channel,
      channelGroup: rule.canonical_channel_group,
      hierarchy: rule.hierarchy_metadata ?? {}
    },
    qaQueueId: null
  };
}

function heuristicResolution(input: ReturnType<typeof normalizeInput>): CampaignResolverOutput | null {
  const source = input.source ?? platformToSource(input.platform);
  const medium = input.medium ?? (source === 'meta' ? 'paid_social' : source === 'google' ? 'cpc' : null);
  const campaignName = input.campaign ?? input.campaignId;
  const { channel, channelGroup } = inferChannel(source, medium, input.platform);

  if (!source || !medium || !campaignName || !channel || !channelGroup) {
    return null;
  }

  return {
    status: 'fallback',
    resolverVersion: input.resolverVersion,
    source: 'heuristic',
    confidence: 0.55,
    ruleId: null,
    canonical: {
      campaignId: input.campaignId,
      campaignName,
      source,
      medium,
      channel,
      channelGroup,
      hierarchy: {
        platform: input.platform,
        accountId: input.accountId,
        campaignId: input.campaignId,
        adsetId: input.adsetId,
        adId: input.adId
      }
    },
    qaQueueId: null
  };
}

async function enqueueQaRecord(
  client: Queryable,
  input: ReturnType<typeof normalizeInput>,
  reason: string
): Promise<string> {
  const result = await client.query<QaQueueRow>(
    `
      INSERT INTO campaign_metadata_qa_queue (
        resolver_version,
        reason,
        platform,
        source,
        medium,
        campaign,
        content,
        term,
        account_id,
        campaign_id,
        adset_id,
        ad_id,
        sample_payload,
        last_seen_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, now())
      ON CONFLICT (
        COALESCE(platform, ''),
        COALESCE(source, ''),
        COALESCE(medium, ''),
        COALESCE(campaign, ''),
        COALESCE(content, ''),
        COALESCE(term, ''),
        COALESCE(account_id, ''),
        COALESCE(campaign_id, ''),
        COALESCE(adset_id, ''),
        COALESCE(ad_id, ''),
        COALESCE(resolver_version, ''),
        reason
      )
      WHERE status = 'open'
      DO UPDATE
      SET
        occurrence_count = campaign_metadata_qa_queue.occurrence_count + 1,
        last_seen_at = now(),
        sample_payload = EXCLUDED.sample_payload,
        updated_at = now()
      RETURNING id
    `,
    [
      input.resolverVersion,
      reason,
      input.platform,
      input.source,
      input.medium,
      input.campaign,
      input.content,
      input.term,
      input.accountId,
      input.campaignId,
      input.adsetId,
      input.adId,
      JSON.stringify(input.samplePayload)
    ]
  );

  return result.rows[0]?.id ?? '';
}

export async function resolveCampaignMetadata(
  rawInput: CampaignResolverInput,
  client?: Queryable | PoolClient
): Promise<CampaignResolverOutput> {
  const database = db(client);
  const input = normalizeInput(rawInput);
  const rules = await loadCandidateRules(database, input);
  const matchingRule = rules.find((rule) => ruleMatches(rule, input));

  if (matchingRule) {
    return outputFromRule(matchingRule);
  }

  const heuristic = heuristicResolution(input);
  if (heuristic) {
    return heuristic;
  }

  const qaQueueId = input.enqueueUnmapped
    ? await enqueueQaRecord(database, input, 'no_matching_campaign_metadata_rule')
    : null;

  return {
    status: 'unmapped',
    resolverVersion: input.resolverVersion,
    source: 'unmapped',
    confidence: 0,
    ruleId: null,
    canonical: {
      campaignId: null,
      campaignName: null,
      source: null,
      medium: null,
      channel: null,
      channelGroup: null,
      hierarchy: {}
    },
    qaQueueId
  };
}

async function updateMmmRow(
  client: Queryable,
  row: MmmBackfillRow,
  resolution: CampaignResolverOutput
): Promise<void> {
  await client.query(
    `
      UPDATE mmm_daily_input_mart_v1
      SET
        resolver_version = $13,
        resolver_source = $14,
        resolver_confidence = $15,
        resolved_canonical_campaign_id = $16,
        resolved_canonical_campaign_name = $17,
        resolved_canonical_source = $18,
        resolved_canonical_medium = $19,
        resolved_canonical_channel = $20,
        resolved_canonical_channel_group = $21,
        resolved_hierarchy_metadata = $22::jsonb,
        needs_metadata_qa = $23,
        last_computed_at = now()
      WHERE metric_date = $1::date
        AND mart_version = $2
        AND mart_row_type = $3
        AND attribution_model = $4
        AND platform = $5
        AND granularity = $6
        AND entity_key = $7
        AND source = $8
        AND medium = $9
        AND campaign = $10
        AND content = $11
        AND term = $12
    `,
    [
      row.metric_date,
      row.mart_version,
      row.mart_row_type,
      row.attribution_model,
      row.platform,
      row.granularity,
      row.entity_key,
      row.source,
      row.medium,
      row.campaign,
      row.content,
      row.term,
      resolution.resolverVersion,
      resolution.source,
      resolution.confidence,
      resolution.canonical.campaignId,
      resolution.canonical.campaignName,
      resolution.canonical.source,
      resolution.canonical.medium,
      resolution.canonical.channel,
      resolution.canonical.channelGroup,
      JSON.stringify(resolution.canonical.hierarchy),
      resolution.status === 'unmapped'
    ]
  );
}

export async function backfillMmmCampaignMetadata(
  input: {
    startDate: string;
    endDate: string;
    resolverVersion?: string | null;
    limit?: number;
  },
  client?: Queryable | PoolClient
): Promise<CampaignMetadataBackfillResult> {
  const database = db(client);
  const resolverVersion = normalizeString(input.resolverVersion) ?? DEFAULT_CAMPAIGN_RESOLVER_VERSION;
  const limit = input.limit ?? 5000;
  const result = await database.query<MmmBackfillRow>(
    `
      SELECT
        metric_date::text,
        mart_version,
        mart_row_type,
        attribution_model,
        platform,
        granularity,
        entity_key,
        source,
        medium,
        campaign,
        content,
        term,
        account_id,
        campaign_id,
        adset_id,
        ad_id
      FROM mmm_daily_input_mart_v1
      WHERE metric_date BETWEEN $1::date AND $2::date
      ORDER BY metric_date ASC, mart_row_type ASC, platform ASC, entity_key ASC
      LIMIT $3
    `,
    [input.startDate, input.endDate, limit]
  );

  const report: CampaignMetadataBackfillResult = {
    resolverVersion,
    processedRows: 0,
    resolvedRows: 0,
    fallbackRows: 0,
    unmappedRows: 0,
    qaQueuedRows: 0
  };

  for (const row of result.rows) {
    const resolution = await resolveCampaignMetadata(
      {
        resolverVersion,
        platform: row.platform,
        source: row.source,
        medium: row.medium,
        campaign: row.campaign,
        content: row.content,
        term: row.term,
        accountId: row.account_id,
        campaignId: row.campaign_id,
        adsetId: row.adset_id,
        adId: row.ad_id,
        samplePayload: {
          metricDate: row.metric_date,
          martRowType: row.mart_row_type,
          attributionModel: row.attribution_model,
          entityKey: row.entity_key
        }
      },
      database
    );

    await updateMmmRow(database, row, resolution);

    report.processedRows += 1;
    report.resolvedRows += resolution.status === 'resolved' ? 1 : 0;
    report.fallbackRows += resolution.status === 'fallback' ? 1 : 0;
    report.unmappedRows += resolution.status === 'unmapped' ? 1 : 0;
    report.qaQueuedRows += resolution.qaQueueId ? 1 : 0;
  }

  return report;
}
