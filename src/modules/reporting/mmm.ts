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
