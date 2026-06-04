import { Router } from 'express';
import { z } from 'zod';

import { query } from '../../db/pool.js';
import { attachAuthContext, requireAuthenticated } from '../auth/index.js';
import { ATTRIBUTION_MODELS } from '../attribution/engine.js';
import {
  backfillMmmCampaignMetadata,
  campaignResolverRequestSchema,
  resolveCampaignMetadata
} from '../campaign-resolver/index.js';

class MmmHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'MmmHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

const MMM_SCHEMA_VERSION = 'mmm_daily_input_mart_v1';

const dateStringSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);

const mmmQuerySchema = z
  .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    martRowType: z.enum(['paid_media', 'attribution']).optional(),
    attributionModel: z.enum(ATTRIBUTION_MODELS).optional(),
    platform: z.enum(['meta', 'google', 'taxonomy']).optional(),
    source: z.string().trim().min(1).max(200).optional(),
    campaign: z.string().trim().min(1).max(500).optional(),
    format: z.enum(['json', 'csv']).optional().default('json'),
    limit: z.coerce.number().int().positive().max(10000).optional().default(1000),
    offset: z.coerce.number().int().min(0).optional().default(0)
  })
  .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

const taxonomyDriftQuerySchema = z
  .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    martRowType: z.enum(['paid_media', 'attribution']).optional(),
    attributionModel: z.enum(ATTRIBUTION_MODELS).optional(),
    platform: z.enum(['meta', 'google', 'taxonomy']).optional(),
    source: z.string().trim().min(1).max(200).optional(),
    campaign: z.string().trim().min(1).max(500).optional(),
    staleAfterDays: z.coerce.number().int().positive().max(365).optional().default(14),
    sampleLimit: z.coerce.number().int().positive().max(50).optional().default(10)
  })
  .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

const campaignResolverBackfillSchema = z
  .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    resolverVersion: z.string().trim().min(1).max(200).optional(),
    limit: z.coerce.number().int().positive().max(50000).optional()
  })
  .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

const modelRunsQuerySchema = z
  .object({
    startDate: dateStringSchema.optional(),
    endDate: dateStringSchema.optional(),
    attributionModel: z.enum(ATTRIBUTION_MODELS).optional(),
    limit: z.coerce.number().int().positive().max(100).optional().default(10)
  })
  .superRefine((value, ctx) => {
    if (value.startDate && value.endDate && value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });

type MmmQueryInput = z.infer<typeof mmmQuerySchema>;
type TaxonomyDriftQueryInput = z.infer<typeof taxonomyDriftQuerySchema>;

type MmmReadinessStatus = 'ready' | 'partial' | 'not_ready';

type MmmReadinessRow = {
  metric_date: string;
  matching_row_count: string | number;
  mart_row_count: string | number;
  generation_timestamp: Date | null;
};

type MmmExportRow = {
  metric_date: string;
  mart_version: string;
  mart_row_type: string;
  attribution_model: string;
  platform: string;
  platform_connection_id: string | number | null;
  granularity: string;
  entity_key: string;
  account_id: string | null;
  account_name: string | null;
  campaign_id: string | null;
  campaign_name: string | null;
  adset_id: string | null;
  adset_name: string | null;
  ad_id: string | null;
  ad_name: string | null;
  creative_id: string | null;
  creative_name: string | null;
  source: string;
  medium: string;
  campaign: string;
  content: string;
  term: string;
  currency: string | null;
  spend: string | number;
  impressions: string | number;
  clicks: string | number;
  shopify_orders: string | number;
  shopify_revenue: string | number;
  attribution_credit_orders: string | number;
  attribution_credit_revenue: string | number;
  new_customer_credit_orders: string | number;
  returning_customer_credit_orders: string | number;
  new_customer_credit_revenue: string | number;
  returning_customer_credit_revenue: string | number;
  match_source_coverage: unknown;
  confidence_label_coverage: unknown;
  spend_last_synced_at: Date | null;
  shopify_last_ingested_at: Date | null;
  attribution_last_computed_at: Date | null;
  last_computed_at: Date;
  resolver_version: string | null;
  resolver_source: string | null;
  resolver_confidence: string | number | null;
  resolved_canonical_campaign_id: string | null;
  resolved_canonical_campaign_name: string | null;
  resolved_canonical_source: string | null;
  resolved_canonical_medium: string | null;
  resolved_canonical_channel: string | null;
  resolved_canonical_channel_group: string | null;
  resolved_hierarchy_metadata: unknown;
  needs_metadata_qa: boolean;
};

type MmmModelRunRow = {
  id: string;
  model_type: string;
  model_version: string;
  mart_version: string;
  attribution_model: string;
  run_status: string;
  training_start_date: string;
  training_end_date: string;
  holdout_start_date: string | null;
  holdout_end_date: string | null;
  run_config: unknown;
  input_summary: unknown;
  model_artifact: unknown;
  calibration_report: unknown;
  validation_report: unknown;
  error_code: string | null;
  error_message: string | null;
  created_at: Date | string;
  started_at: Date | string;
  completed_at: Date | string | null;
};

type ExposureCoverageRow = {
  metric_date: string;
  source_platform: string;
  exposure_type: string;
  total_exposures: string | number;
  valid_exposures: string | number;
  invalid_exposures: string | number;
  identity_resolved_exposures: string | number;
  identity_unresolved_exposures: string | number;
  campaign_joinable_exposures: string | number;
  campaign_metadata_resolved_exposures: string | number;
  latest_exposure_at: Date | null;
};

type TaxonomyDriftSummaryRow = {
  metric_date: string | null;
  total_rows: string | number;
  unknown_source_rows: string | number;
  unmapped_source_rows: string | number;
  unknown_or_unmapped_source_rows: string | number;
  unknown_medium_rows: string | number;
  unmapped_medium_rows: string | number;
  unknown_or_unmapped_medium_rows: string | number;
  unresolved_campaign_metadata_rows: string | number;
  stale_campaign_metadata_rows: string | number;
  native_id_eligible_rows: string | number;
  account_id_rows: string | number;
  campaign_id_rows: string | number;
  adset_id_rows: string | number;
  ad_id_rows: string | number;
  creative_id_rows: string | number;
  platform_native_id_rows: string | number;
};

type TaxonomyDriftSampleRow = {
  sample_type: string;
  row_count: string | number;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  platform: string | null;
  mart_row_type: string | null;
  attribution_model: string | null;
  account_id: string | null;
  campaign_id: string | null;
  metadata_last_seen_at: Date | string | null;
  metadata_updated_at: Date | string | null;
};

function parseInput<TSchema extends z.ZodTypeAny>(schema: TSchema, input: unknown): z.infer<TSchema> {
  try {
    return schema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new MmmHttpError(400, 'invalid_request', 'Invalid MMM export query parameters', error.flatten());
    }

    throw error;
  }
}

function buildMmmFilters(input: MmmQueryInput): { sql: string; params: unknown[] } {
  const params: unknown[] = [input.startDate, input.endDate];
  const filters = ['metric_date BETWEEN $1::date AND $2::date'];

  if (input.martRowType) {
    params.push(input.martRowType);
    filters.push(`mart_row_type = $${params.length}`);
  }

  if (input.attributionModel) {
    params.push(input.attributionModel);
    filters.push(`attribution_model = $${params.length}`);
  }

  if (input.platform) {
    params.push(input.platform);
    filters.push(`platform = $${params.length}`);
  }

  if (input.source) {
    params.push(input.source);
    filters.push(`source = $${params.length}`);
  }

  if (input.campaign) {
    params.push(input.campaign);
    filters.push(`campaign = $${params.length}`);
  }

  return {
    sql: filters.join('\n        AND '),
    params
  };
}

function buildTaxonomyDriftFilters(input: TaxonomyDriftQueryInput): { sql: string; params: unknown[] } {
  const params: unknown[] = [input.startDate, input.endDate];
  const filters = ['mart.metric_date BETWEEN $1::date AND $2::date'];

  if (input.martRowType) {
    params.push(input.martRowType);
    filters.push(`mart.mart_row_type = $${params.length}`);
  }

  if (input.attributionModel) {
    params.push(input.attributionModel);
    filters.push(`mart.attribution_model = $${params.length}`);
  }

  if (input.platform) {
    params.push(input.platform);
    filters.push(`mart.platform = $${params.length}`);
  }

  if (input.source) {
    params.push(input.source);
    filters.push(`mart.source = $${params.length}`);
  }

  if (input.campaign) {
    params.push(input.campaign);
    filters.push(`mart.campaign = $${params.length}`);
  }

  return {
    sql: filters.join('\n            AND '),
    params
  };
}

function toIsoString(value: Date | string | null | undefined): string | null {
  if (value instanceof Date) {
    return value.toISOString();
  }

  return typeof value === 'string' && value.length > 0 ? new Date(value).toISOString() : null;
}

function toNumber(value: string | number | null | undefined): number {
  if (value === null || value === undefined) {
    return 0;
  }

  return Number(value);
}

function toNullableRate(numerator: number, denominator: number): number | null {
  return denominator > 0 ? numerator / denominator : null;
}

function mapMmmRow(row: MmmExportRow) {
  const mapped = {
    date: row.metric_date,
    martVersion: row.mart_version,
    martRowType: row.mart_row_type,
    attributionModel: row.attribution_model,
    platform: row.platform,
    platformConnectionId: row.platform_connection_id === null ? null : Number(row.platform_connection_id),
    granularity: row.granularity,
    entityKey: row.entity_key,
    accountId: row.account_id,
    accountName: row.account_name,
    campaignId: row.campaign_id,
    campaignName: row.campaign_name,
    adsetId: row.adset_id,
    adsetName: row.adset_name,
    adId: row.ad_id,
    adName: row.ad_name,
    creativeId: row.creative_id,
    creativeName: row.creative_name,
    source: row.source,
    medium: row.medium,
    campaign: row.campaign,
    content: row.content,
    term: row.term,
    currency: row.currency,
    spend: toNumber(row.spend),
    impressions: toNumber(row.impressions),
    clicks: toNumber(row.clicks),
    shopifyOrders: toNumber(row.shopify_orders),
    shopifyRevenue: toNumber(row.shopify_revenue),
    attributionCreditOrders: toNumber(row.attribution_credit_orders),
    attributionCreditRevenue: toNumber(row.attribution_credit_revenue),
    newCustomerCreditOrders: toNumber(row.new_customer_credit_orders),
    returningCustomerCreditOrders: toNumber(row.returning_customer_credit_orders),
    newCustomerCreditRevenue: toNumber(row.new_customer_credit_revenue),
    returningCustomerCreditRevenue: toNumber(row.returning_customer_credit_revenue),
    matchSourceCoverage: row.match_source_coverage,
    confidenceLabelCoverage: row.confidence_label_coverage,
    spendLastSyncedAt: toIsoString(row.spend_last_synced_at),
    shopifyLastIngestedAt: toIsoString(row.shopify_last_ingested_at),
    attributionLastComputedAt: toIsoString(row.attribution_last_computed_at),
    lastComputedAt: toIsoString(row.last_computed_at)
  };

  if (!Object.hasOwn(row, 'resolver_version')) {
    return mapped;
  }

  return {
    ...mapped,
    resolverVersion: row.resolver_version,
    resolverSource: row.resolver_source,
    resolverConfidence: row.resolver_confidence === null ? null : Number(row.resolver_confidence),
    resolvedCanonicalCampaignId: row.resolved_canonical_campaign_id,
    resolvedCanonicalCampaignName: row.resolved_canonical_campaign_name,
    resolvedCanonicalSource: row.resolved_canonical_source,
    resolvedCanonicalMedium: row.resolved_canonical_medium,
    resolvedCanonicalChannel: row.resolved_canonical_channel,
    resolvedCanonicalChannelGroup: row.resolved_canonical_channel_group,
    resolvedHierarchyMetadata: row.resolved_hierarchy_metadata,
    needsMetadataQa: row.needs_metadata_qa
  };
}

function escapeCsvValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }

  const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function renderCsv(rows: ReturnType<typeof mapMmmRow>[], generationTimestamp: string | null, readinessStatus: MmmReadinessStatus): string {
  const headers = [
    'schemaVersion',
    'generationTimestamp',
    'readinessStatus',
    'date',
    'martVersion',
    'martRowType',
    'attributionModel',
    'platform',
    'platformConnectionId',
    'granularity',
    'entityKey',
    'accountId',
    'accountName',
    'campaignId',
    'campaignName',
    'adsetId',
    'adsetName',
    'adId',
    'adName',
    'creativeId',
    'creativeName',
    'source',
    'medium',
    'campaign',
    'content',
    'term',
    'currency',
    'spend',
    'impressions',
    'clicks',
    'shopifyOrders',
    'shopifyRevenue',
    'attributionCreditOrders',
    'attributionCreditRevenue',
    'newCustomerCreditOrders',
    'returningCustomerCreditOrders',
    'newCustomerCreditRevenue',
    'returningCustomerCreditRevenue',
    'matchSourceCoverage',
    'confidenceLabelCoverage',
    'spendLastSyncedAt',
    'shopifyLastIngestedAt',
    'attributionLastComputedAt',
    'lastComputedAt',
    'resolverVersion',
    'resolverSource',
    'resolverConfidence',
    'resolvedCanonicalCampaignId',
    'resolvedCanonicalCampaignName',
    'resolvedCanonicalSource',
    'resolvedCanonicalMedium',
    'resolvedCanonicalChannel',
    'resolvedCanonicalChannelGroup',
    'resolvedHierarchyMetadata',
    'needsMetadataQa'
  ];

  const lines = [headers.join(',')];
  for (const row of rows) {
    const record = {
      schemaVersion: MMM_SCHEMA_VERSION,
      generationTimestamp,
      readinessStatus,
      ...row
    };
    lines.push(headers.map((header) => escapeCsvValue(record[header as keyof typeof record])).join(','));
  }

  return `${lines.join('\n')}\n`;
}

function mapExposureCoverageRow(row: ExposureCoverageRow) {
  const totalExposures = toNumber(row.total_exposures);
  const validExposures = toNumber(row.valid_exposures);
  const identityResolvedExposures = toNumber(row.identity_resolved_exposures);
  const campaignJoinableExposures = toNumber(row.campaign_joinable_exposures);
  const campaignMetadataResolvedExposures = toNumber(row.campaign_metadata_resolved_exposures);

  return {
    date: row.metric_date,
    sourcePlatform: row.source_platform,
    exposureType: row.exposure_type,
    totalExposures,
    validExposures,
    invalidExposures: toNumber(row.invalid_exposures),
    identityResolvedExposures,
    identityUnresolvedExposures: toNumber(row.identity_unresolved_exposures),
    identityResolutionRate: totalExposures > 0 ? identityResolvedExposures / totalExposures : null,
    campaignJoinableExposures,
    campaignMetadataResolvedExposures,
    campaignMetadataResolutionRate: campaignJoinableExposures > 0 ? campaignMetadataResolvedExposures / campaignJoinableExposures : null,
    latestExposureAt: toIsoString(row.latest_exposure_at)
  };
}

function mapTaxonomyDriftSummaryRow(row: TaxonomyDriftSummaryRow) {
  const totalRows = toNumber(row.total_rows);
  const nativeIdEligibleRows = toNumber(row.native_id_eligible_rows);
  const unknownSourceRows = toNumber(row.unknown_source_rows);
  const unmappedSourceRows = toNumber(row.unmapped_source_rows);
  const unknownOrUnmappedSourceRows = toNumber(row.unknown_or_unmapped_source_rows);
  const unknownMediumRows = toNumber(row.unknown_medium_rows);
  const unmappedMediumRows = toNumber(row.unmapped_medium_rows);
  const unknownOrUnmappedMediumRows = toNumber(row.unknown_or_unmapped_medium_rows);
  const unresolvedCampaignMetadataRows = toNumber(row.unresolved_campaign_metadata_rows);
  const staleCampaignMetadataRows = toNumber(row.stale_campaign_metadata_rows);
  const accountIdRows = toNumber(row.account_id_rows);
  const campaignIdRows = toNumber(row.campaign_id_rows);
  const adsetIdRows = toNumber(row.adset_id_rows);
  const adIdRows = toNumber(row.ad_id_rows);
  const creativeIdRows = toNumber(row.creative_id_rows);
  const platformNativeIdRows = toNumber(row.platform_native_id_rows);

  return {
    date: row.metric_date,
    totalRows,
    unknownSourceRows,
    unknownSourceRate: toNullableRate(unknownSourceRows, totalRows),
    unmappedSourceRows,
    unmappedSourceRate: toNullableRate(unmappedSourceRows, totalRows),
    unknownOrUnmappedSourceRows,
    unknownOrUnmappedSourceRate: toNullableRate(unknownOrUnmappedSourceRows, totalRows),
    unknownMediumRows,
    unknownMediumRate: toNullableRate(unknownMediumRows, totalRows),
    unmappedMediumRows,
    unmappedMediumRate: toNullableRate(unmappedMediumRows, totalRows),
    unknownOrUnmappedMediumRows,
    unknownOrUnmappedMediumRate: toNullableRate(unknownOrUnmappedMediumRows, totalRows),
    unresolvedCampaignMetadataRows,
    unresolvedCampaignMetadataRate: toNullableRate(unresolvedCampaignMetadataRows, totalRows),
    staleCampaignMetadataRows,
    staleCampaignMetadataRate: toNullableRate(staleCampaignMetadataRows, totalRows),
    nativeIdEligibleRows,
    nativeIdCoverage: {
      accountIdRows,
      accountIdRate: toNullableRate(accountIdRows, nativeIdEligibleRows),
      campaignIdRows,
      campaignIdRate: toNullableRate(campaignIdRows, nativeIdEligibleRows),
      adsetIdRows,
      adsetIdRate: toNullableRate(adsetIdRows, nativeIdEligibleRows),
      adIdRows,
      adIdRate: toNullableRate(adIdRows, nativeIdEligibleRows),
      creativeIdRows,
      creativeIdRate: toNullableRate(creativeIdRows, nativeIdEligibleRows),
      platformNativeIdRows,
      platformNativeIdRate: toNullableRate(platformNativeIdRows, nativeIdEligibleRows)
    }
  };
}

function mapTaxonomyDriftSampleRow(row: TaxonomyDriftSampleRow) {
  return {
    sampleType: row.sample_type,
    rowCount: toNumber(row.row_count),
    source: row.source,
    medium: row.medium,
    campaign: row.campaign,
    platform: row.platform,
    martRowType: row.mart_row_type,
    attributionModel: row.attribution_model,
    accountId: row.account_id,
    campaignId: row.campaign_id,
    metadataLastSeenAt: toIsoString(row.metadata_last_seen_at),
    metadataUpdatedAt: toIsoString(row.metadata_updated_at)
  };
}

function deriveReadiness(rows: MmmReadinessRow[]) {
  const excludedDateWindows = rows
    .filter((row) => Number(row.matching_row_count) === 0)
    .map((row) => ({
      startDate: row.metric_date,
      endDate: row.metric_date,
      reason: Number(row.mart_row_count) === 0 ? 'no_mmm_mart_rows' : 'no_rows_matching_filters'
    }));
  const includedDateCount = rows.length - excludedDateWindows.length;
  const status: MmmReadinessStatus =
    excludedDateWindows.length === 0 ? 'ready' : includedDateCount > 0 ? 'partial' : 'not_ready';
  const generationTimestamp = rows.reduce<string | null>((latest, row) => {
    const candidate = toIsoString(row.generation_timestamp);
    if (!candidate) {
      return latest;
    }

    return latest === null || candidate > latest ? candidate : latest;
  }, null);

  return {
    status,
    generationTimestamp,
    includedDateCount,
    excludedDateWindows
  };
}

function mapMmmModelRun(row: MmmModelRunRow) {
  return {
    id: row.id,
    modelType: row.model_type,
    modelVersion: row.model_version,
    martVersion: row.mart_version,
    attributionModel: row.attribution_model,
    runStatus: row.run_status,
    trainingStartDate: row.training_start_date,
    trainingEndDate: row.training_end_date,
    holdoutStartDate: row.holdout_start_date,
    holdoutEndDate: row.holdout_end_date,
    runConfig: row.run_config ?? {},
    inputSummary: row.input_summary ?? {},
    modelArtifact: row.model_artifact ?? {},
    calibrationReport: row.calibration_report ?? {},
    validationReport: row.validation_report ?? {},
    errorCode: row.error_code,
    errorMessage: row.error_message,
    createdAt: toIsoString(row.created_at),
    startedAt: toIsoString(row.started_at),
    completedAt: toIsoString(row.completed_at)
  };
}

export function createMmmRouter(): Router {
  const router = Router();

  router.use(attachAuthContext);
  router.use(requireAuthenticated);
  router.use((_req, res, next) => {
    res.setHeader('X-ROAS-Radar-MMM-Schema', MMM_SCHEMA_VERSION);
    next();
  });

  router.get('/model-runs', async (req, res, next) => {
    try {
      const input = parseInput(modelRunsQuerySchema, req.query);
      const params: unknown[] = [];
      const filters: string[] = [];

      if (input.startDate) {
        params.push(input.startDate);
        filters.push(`training_end_date >= $${params.length}::date`);
      }

      if (input.endDate) {
        params.push(input.endDate);
        filters.push(`training_start_date <= $${params.length}::date`);
      }

      if (input.attributionModel) {
        params.push(input.attributionModel);
        filters.push(`attribution_model = $${params.length}`);
      }

      params.push(input.limit);
      const result = await query<MmmModelRunRow>(
        `
          SELECT
            id::text,
            model_type,
            model_version,
            mart_version,
            attribution_model,
            run_status,
            training_start_date::text,
            training_end_date::text,
            holdout_start_date::text,
            holdout_end_date::text,
            run_config,
            input_summary,
            model_artifact,
            calibration_report,
            validation_report,
            error_code,
            error_message,
            created_at,
            started_at,
            completed_at
          FROM mmm_model_runs
          ${filters.length > 0 ? `WHERE ${filters.join('\n            AND ')}` : ''}
          ORDER BY created_at DESC
          LIMIT $${params.length}
        `,
        params
      );

      res.status(200).json({
        schemaVersion: 'mmm_model_runs_v1',
        rows: result.rows.map(mapMmmModelRun)
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/campaign-resolver/resolve', async (req, res, next) => {
    try {
      const input = parseInput(campaignResolverRequestSchema, req.body);
      const resolution = await resolveCampaignMetadata(input);
      res.status(200).json({
        schemaVersion: 'campaign_metadata_resolver_v1',
        resolution
      });
    } catch (error) {
      next(error);
    }
  });

  router.post('/campaign-resolver/backfill', async (req, res, next) => {
    try {
      const input = parseInput(campaignResolverBackfillSchema, req.body);
      const report = await backfillMmmCampaignMetadata(input);
      res.status(202).json({
        schemaVersion: 'campaign_metadata_resolver_v1',
        report
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/taxonomy-drift', async (req, res, next) => {
    try {
      const input = parseInput(taxonomyDriftQuerySchema, req.query);
      const filters = buildTaxonomyDriftFilters(input);
      const staleAfterDaysParam = filters.params.length + 1;
      const sampleLimitParam = filters.params.length + 2;
      const driftCte = `
        WITH filtered_mart AS (
          SELECT
            mart.metric_date,
            mart.mart_row_type,
            mart.attribution_model,
            mart.platform,
            mart.source,
            mart.medium,
            mart.campaign,
            mart.account_id,
            mart.campaign_id,
            mart.adset_id,
            mart.ad_id,
            mart.creative_id,
            mart.resolved_canonical_source,
            mart.resolved_canonical_medium,
            mart.resolved_canonical_campaign_name,
            mart.needs_metadata_qa,
            campaign_meta.last_seen_at AS metadata_last_seen_at,
            campaign_meta.updated_at AS metadata_updated_at,
            lower(btrim(COALESCE(mart.source, ''))) AS normalized_source,
            lower(btrim(COALESCE(mart.medium, ''))) AS normalized_medium,
            lower(btrim(COALESCE(mart.resolved_canonical_source, mart.source, ''))) AS normalized_effective_source,
            lower(btrim(COALESCE(mart.resolved_canonical_medium, mart.medium, ''))) AS normalized_effective_medium,
            lower(btrim(COALESCE(mart.resolved_canonical_campaign_name, mart.campaign, ''))) AS normalized_effective_campaign,
            mart.platform IN ('meta', 'google') AS native_id_eligible,
            campaign_meta.id IS NOT NULL
              AND campaign_meta.last_seen_at < ($2::date - ($${staleAfterDaysParam}::int * interval '1 day')) AS stale_campaign_metadata
          FROM mmm_daily_input_mart_v1 mart
          LEFT JOIN ad_platform_entity_metadata campaign_meta
            ON campaign_meta.platform = CASE
                WHEN mart.platform = 'meta' THEN 'meta_ads'
                WHEN mart.platform = 'google' THEN 'google_ads'
                ELSE NULL
              END
           AND campaign_meta.entity_type = 'campaign'
           AND campaign_meta.account_id = mart.account_id
           AND campaign_meta.entity_id = mart.campaign_id
          WHERE ${filters.sql}
        ),
        classified_mart AS (
          SELECT
            *,
            normalized_source IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned')
              OR normalized_effective_source IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unknown_source,
            resolved_canonical_source IS NULL AS has_unmapped_source,
            normalized_medium IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned')
              OR normalized_effective_medium IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unknown_medium,
            resolved_canonical_medium IS NULL AS has_unmapped_medium,
            needs_metadata_qa
              OR resolved_canonical_campaign_name IS NULL
              OR normalized_effective_campaign IN ('', 'unknown', '(not set)', 'not set', 'null', 'none', 'unassigned') AS has_unresolved_campaign_metadata,
            account_id IS NOT NULL AND campaign_id IS NOT NULL AS has_platform_native_campaign_key
          FROM filtered_mart
        )
      `;

      const summaryResult = await query<TaxonomyDriftSummaryRow>(
        `
          ${driftCte},
          daily_summary AS (
            SELECT
              metric_date::text,
              COUNT(*)::bigint AS total_rows,
              COUNT(*) FILTER (WHERE has_unknown_source)::bigint AS unknown_source_rows,
              COUNT(*) FILTER (WHERE has_unmapped_source)::bigint AS unmapped_source_rows,
              COUNT(*) FILTER (WHERE has_unknown_source OR has_unmapped_source)::bigint AS unknown_or_unmapped_source_rows,
              COUNT(*) FILTER (WHERE has_unknown_medium)::bigint AS unknown_medium_rows,
              COUNT(*) FILTER (WHERE has_unmapped_medium)::bigint AS unmapped_medium_rows,
              COUNT(*) FILTER (WHERE has_unknown_medium OR has_unmapped_medium)::bigint AS unknown_or_unmapped_medium_rows,
              COUNT(*) FILTER (WHERE has_unresolved_campaign_metadata)::bigint AS unresolved_campaign_metadata_rows,
              COUNT(*) FILTER (WHERE stale_campaign_metadata)::bigint AS stale_campaign_metadata_rows,
              COUNT(*) FILTER (WHERE native_id_eligible)::bigint AS native_id_eligible_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND account_id IS NOT NULL)::bigint AS account_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND campaign_id IS NOT NULL)::bigint AS campaign_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND adset_id IS NOT NULL)::bigint AS adset_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND ad_id IS NOT NULL)::bigint AS ad_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND creative_id IS NOT NULL)::bigint AS creative_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND has_platform_native_campaign_key)::bigint AS platform_native_id_rows
            FROM classified_mart
            GROUP BY metric_date
          ),
          overall_summary AS (
            SELECT
              NULL::text AS metric_date,
              COUNT(*)::bigint AS total_rows,
              COUNT(*) FILTER (WHERE has_unknown_source)::bigint AS unknown_source_rows,
              COUNT(*) FILTER (WHERE has_unmapped_source)::bigint AS unmapped_source_rows,
              COUNT(*) FILTER (WHERE has_unknown_source OR has_unmapped_source)::bigint AS unknown_or_unmapped_source_rows,
              COUNT(*) FILTER (WHERE has_unknown_medium)::bigint AS unknown_medium_rows,
              COUNT(*) FILTER (WHERE has_unmapped_medium)::bigint AS unmapped_medium_rows,
              COUNT(*) FILTER (WHERE has_unknown_medium OR has_unmapped_medium)::bigint AS unknown_or_unmapped_medium_rows,
              COUNT(*) FILTER (WHERE has_unresolved_campaign_metadata)::bigint AS unresolved_campaign_metadata_rows,
              COUNT(*) FILTER (WHERE stale_campaign_metadata)::bigint AS stale_campaign_metadata_rows,
              COUNT(*) FILTER (WHERE native_id_eligible)::bigint AS native_id_eligible_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND account_id IS NOT NULL)::bigint AS account_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND campaign_id IS NOT NULL)::bigint AS campaign_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND adset_id IS NOT NULL)::bigint AS adset_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND ad_id IS NOT NULL)::bigint AS ad_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND creative_id IS NOT NULL)::bigint AS creative_id_rows,
              COUNT(*) FILTER (WHERE native_id_eligible AND has_platform_native_campaign_key)::bigint AS platform_native_id_rows
            FROM classified_mart
          )
          SELECT * FROM overall_summary
          UNION ALL
          SELECT * FROM daily_summary
          ORDER BY metric_date NULLS FIRST
        `,
        [...filters.params, input.staleAfterDays]
      );

      const samplesResult = await query<TaxonomyDriftSampleRow>(
        `
          ${driftCte},
          sample_candidates AS (
            SELECT
              'unknown_or_unmapped_source'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE has_unknown_source OR has_unmapped_source
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id

            UNION ALL

            SELECT
              'unknown_or_unmapped_medium'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE has_unknown_medium OR has_unmapped_medium
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id

            UNION ALL

            SELECT
              'unresolved_campaign_metadata'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE has_unresolved_campaign_metadata
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id

            UNION ALL

            SELECT
              'stale_campaign_metadata'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE stale_campaign_metadata
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id

            UNION ALL

            SELECT
              'missing_platform_native_campaign_key'::text AS sample_type,
              COUNT(*)::bigint AS row_count,
              source,
              medium,
              campaign,
              platform,
              mart_row_type,
              attribution_model,
              account_id,
              campaign_id,
              MAX(metadata_last_seen_at) AS metadata_last_seen_at,
              MAX(metadata_updated_at) AS metadata_updated_at
            FROM classified_mart
            WHERE native_id_eligible AND NOT has_platform_native_campaign_key
            GROUP BY source, medium, campaign, platform, mart_row_type, attribution_model, account_id, campaign_id
          ),
          ranked_samples AS (
            SELECT
              *,
              row_number() OVER (
                PARTITION BY sample_type
                ORDER BY row_count DESC, source ASC, medium ASC, campaign ASC, platform ASC
              ) AS sample_rank
            FROM sample_candidates
          )
          SELECT
            sample_type,
            row_count,
            source,
            medium,
            campaign,
            platform,
            mart_row_type,
            attribution_model,
            account_id,
            campaign_id,
            metadata_last_seen_at,
            metadata_updated_at
          FROM ranked_samples
          WHERE sample_rank <= $${sampleLimitParam}
          ORDER BY sample_type ASC, row_count DESC, source ASC, medium ASC, campaign ASC
        `,
        [...filters.params, input.staleAfterDays, input.sampleLimit]
      );

      const [overallRow, ...dailyRows] = summaryResult.rows.map(mapTaxonomyDriftSummaryRow);

      res.status(200).json({
        schemaVersion: 'mmm_taxonomy_drift_report_v1',
        range: {
          startDate: input.startDate,
          endDate: input.endDate
        },
        filters: {
          martRowType: input.martRowType ?? null,
          attributionModel: input.attributionModel ?? null,
          platform: input.platform ?? null,
          source: input.source ?? null,
          campaign: input.campaign ?? null,
          staleAfterDays: input.staleAfterDays,
          sampleLimit: input.sampleLimit
        },
        overall: overallRow,
        daily: dailyRows,
        samples: samplesResult.rows.map(mapTaxonomyDriftSampleRow)
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/exposure-coverage', async (req, res, next) => {
    try {
      const input = parseInput(
        z
          .object({
            startDate: dateStringSchema,
            endDate: dateStringSchema,
            sourcePlatform: z
              .enum(['meta_ads', 'google_ads', 'tiktok_ads', 'pinterest_ads', 'snapchat_ads', 'unknown'])
              .optional(),
            exposureType: z.enum(['impression', 'view']).optional()
          })
          .superRefine((value, ctx) => {
            if (value.startDate > value.endDate) {
              ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'startDate must be on or before endDate',
                path: ['startDate']
              });
            }
          }),
        req.query
      );
      const params: unknown[] = [input.startDate, input.endDate];
      const filters = ['e.occurred_at >= $1::date', "e.occurred_at < ($2::date + interval '1 day')"];

      if (input.sourcePlatform) {
        params.push(input.sourcePlatform);
        filters.push(`e.source_platform = $${params.length}`);
      }

      if (input.exposureType) {
        params.push(input.exposureType);
        filters.push(`e.exposure_type = $${params.length}`);
      }

      const result = await query<ExposureCoverageRow>(
        `
          SELECT
            e.occurred_at::date::text AS metric_date,
            e.source_platform,
            e.exposure_type,
            COUNT(*)::bigint AS total_exposures,
            COUNT(*) FILTER (WHERE e.validity_status = 'valid')::bigint AS valid_exposures,
            COUNT(*) FILTER (WHERE e.validity_status = 'invalid')::bigint AS invalid_exposures,
            COUNT(*) FILTER (WHERE e.identity_journey_id IS NOT NULL)::bigint AS identity_resolved_exposures,
            COUNT(*) FILTER (
              WHERE e.validity_status = 'valid'
                AND e.identity_journey_id IS NULL
            )::bigint AS identity_unresolved_exposures,
            COUNT(*) FILTER (
              WHERE e.validity_status = 'valid'
                AND e.account_id IS NOT NULL
                AND e.campaign_id IS NOT NULL
            )::bigint AS campaign_joinable_exposures,
            COUNT(*) FILTER (WHERE campaign_meta.id IS NOT NULL)::bigint AS campaign_metadata_resolved_exposures,
            MAX(e.occurred_at) AS latest_exposure_at
          FROM ad_exposure_events e
          LEFT JOIN ad_platform_entity_metadata campaign_meta
            ON campaign_meta.platform = e.source_platform
           AND campaign_meta.entity_type = 'campaign'
           AND campaign_meta.account_id = e.account_id
           AND campaign_meta.entity_id = e.campaign_id
           AND COALESCE(campaign_meta.tenant_id, '') = COALESCE(e.tenant_id, '')
           AND COALESCE(campaign_meta.workspace_id, '') = COALESCE(e.workspace_id, '')
          WHERE ${filters.join('\n            AND ')}
          GROUP BY e.occurred_at::date, e.source_platform, e.exposure_type
          ORDER BY e.occurred_at::date ASC, e.source_platform ASC, e.exposure_type ASC
        `,
        params
      );
      const rows = result.rows.map(mapExposureCoverageRow);
      const totals = rows.reduce(
        (current, row) => ({
          totalExposures: current.totalExposures + row.totalExposures,
          validExposures: current.validExposures + row.validExposures,
          invalidExposures: current.invalidExposures + row.invalidExposures,
          identityResolvedExposures: current.identityResolvedExposures + row.identityResolvedExposures,
          identityUnresolvedExposures: current.identityUnresolvedExposures + row.identityUnresolvedExposures,
          campaignJoinableExposures: current.campaignJoinableExposures + row.campaignJoinableExposures,
          campaignMetadataResolvedExposures:
            current.campaignMetadataResolvedExposures + row.campaignMetadataResolvedExposures
        }),
        {
          totalExposures: 0,
          validExposures: 0,
          invalidExposures: 0,
          identityResolvedExposures: 0,
          identityUnresolvedExposures: 0,
          campaignJoinableExposures: 0,
          campaignMetadataResolvedExposures: 0
        }
      );

      res.status(200).json({
        schemaVersion: 'ad_exposure_coverage_v1',
        range: {
          startDate: input.startDate,
          endDate: input.endDate
        },
        filters: {
          sourcePlatform: input.sourcePlatform ?? null,
          exposureType: input.exposureType ?? null
        },
        totals: {
          ...totals,
          identityResolutionRate:
            totals.totalExposures > 0 ? totals.identityResolvedExposures / totals.totalExposures : null,
          campaignMetadataResolutionRate:
            totals.campaignJoinableExposures > 0
              ? totals.campaignMetadataResolvedExposures / totals.campaignJoinableExposures
              : null
        },
        rows
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/', async (req, res, next) => {
    try {
      const input = parseInput(mmmQuerySchema, req.query);
      const filters = buildMmmFilters(input);
      const readinessResult = await query<MmmReadinessRow>(
        `
          WITH requested_dates AS (
            SELECT generate_series($1::date, $2::date, interval '1 day')::date AS metric_date
          ),
          filtered_rows AS (
            SELECT
              metric_date,
              COUNT(*) AS row_count,
              MAX(last_computed_at) AS generation_timestamp
            FROM mmm_daily_input_mart_v1
            WHERE ${filters.sql}
            GROUP BY metric_date
          ),
          mart_rows AS (
            SELECT
              metric_date,
              COUNT(*) AS row_count
            FROM mmm_daily_input_mart_v1
            WHERE metric_date BETWEEN $1::date AND $2::date
            GROUP BY metric_date
          )
          SELECT
            requested_dates.metric_date::text,
            COALESCE(filtered_rows.row_count, 0) AS matching_row_count,
            COALESCE(mart_rows.row_count, 0) AS mart_row_count,
            filtered_rows.generation_timestamp
          FROM requested_dates
          LEFT JOIN filtered_rows ON filtered_rows.metric_date = requested_dates.metric_date
          LEFT JOIN mart_rows ON mart_rows.metric_date = requested_dates.metric_date
          ORDER BY requested_dates.metric_date ASC
        `,
        filters.params
      );
      const readiness = deriveReadiness(readinessResult.rows);
      const totalRows = readinessResult.rows.reduce((sum, row) => sum + Number(row.matching_row_count), 0);

      const rowsResult = await query<MmmExportRow>(
        `
          SELECT
            metric_date::text,
            mart_version,
            mart_row_type,
            attribution_model,
            platform,
            platform_connection_id,
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
            source,
            medium,
            campaign,
            content,
            term,
            currency,
            spend,
            impressions,
            clicks,
            shopify_orders,
            shopify_revenue,
            attribution_credit_orders,
            attribution_credit_revenue,
            new_customer_credit_orders,
            returning_customer_credit_orders,
            new_customer_credit_revenue,
            returning_customer_credit_revenue,
            match_source_coverage,
            confidence_label_coverage,
            spend_last_synced_at,
            shopify_last_ingested_at,
            attribution_last_computed_at,
            last_computed_at,
            resolver_version,
            resolver_source,
            resolver_confidence,
            resolved_canonical_campaign_id,
            resolved_canonical_campaign_name,
            resolved_canonical_source,
            resolved_canonical_medium,
            resolved_canonical_channel,
            resolved_canonical_channel_group,
            resolved_hierarchy_metadata,
            needs_metadata_qa
          FROM mmm_daily_input_mart_v1
          WHERE ${filters.sql}
          ORDER BY metric_date ASC, mart_row_type ASC, attribution_model ASC, platform ASC, entity_key ASC
          LIMIT $${filters.params.length + 1}
          OFFSET $${filters.params.length + 2}
        `,
        [...filters.params, input.limit, input.offset]
      );
      const rows = rowsResult.rows.map(mapMmmRow);

      if (input.format === 'csv') {
        res.status(200);
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="mmm-${input.startDate}-${input.endDate}.csv"`);
        res.send(renderCsv(rows, readiness.generationTimestamp, readiness.status));
        return;
      }

      res.status(200).json({
        schemaVersion: MMM_SCHEMA_VERSION,
        range: {
          startDate: input.startDate,
          endDate: input.endDate
        },
        filters: {
          martRowType: input.martRowType ?? null,
          attributionModel: input.attributionModel ?? null,
          platform: input.platform ?? null,
          source: input.source ?? null,
          campaign: input.campaign ?? null
        },
        readiness: {
          status: readiness.status,
          generationTimestamp: readiness.generationTimestamp,
          includedDateCount: readiness.includedDateCount,
          excludedDateWindows: readiness.excludedDateWindows
        },
        pagination: {
          limit: input.limit,
          offset: input.offset,
          returned: rows.length,
          totalRows,
          hasMore: input.offset + rows.length < totalRows
        },
        rows
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
