import { Router } from 'express';
import { z } from 'zod';

import { query } from '../../db/pool.js';
import { attachAuthContext, requireAuthenticated } from '../auth/index.js';
import { ATTRIBUTION_MODELS } from '../attribution/engine.js';

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
  return {
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
    'lastComputedAt'
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

export function createMmmRouter(): Router {
  const router = Router();

  router.use(attachAuthContext);
  router.use(requireAuthenticated);
  router.use((_req, res, next) => {
    res.setHeader('X-ROAS-Radar-MMM-Schema', MMM_SCHEMA_VERSION);
    next();
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
            last_computed_at
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
