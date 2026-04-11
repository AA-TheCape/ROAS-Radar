import { Router } from 'express';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { calculatePerformanceMetrics } from '../../shared/metrics.js';
import { fetchDataQualityReport, resolveRunDate } from '../data-quality/index.js';

class ReportingHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'ReportingHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

const dateStringSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);

const baseFiltersObjectSchema = z.object({
  startDate: dateStringSchema,
  endDate: dateStringSchema,
  source: z.string().trim().min(1).optional(),
  campaign: z.string().trim().min(1).optional()
});

function withValidDateRange<T extends z.ZodRawShape>(schema: z.ZodObject<T>) {
  return schema.superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate must be on or before endDate',
        path: ['startDate']
      });
    }
  });
}

const baseFiltersSchema = withValidDateRange(baseFiltersObjectSchema);

const campaignsQuerySchema = withValidDateRange(baseFiltersObjectSchema.extend({
  limit: z.coerce.number().int().positive().max(200).optional().default(50)
}));

const timeseriesQuerySchema = withValidDateRange(baseFiltersObjectSchema.extend({
  groupBy: z.enum(['day', 'source', 'campaign']).optional().default('day')
}));

const ordersQuerySchema = withValidDateRange(baseFiltersObjectSchema.extend({
  limit: z.coerce.number().int().positive().max(200).optional().default(50)
}));

const reconciliationQuerySchema = z.object({
  runDate: dateStringSchema.optional()
});

type SummaryRow = {
  visits: string | number;
  orders: string | number;
  revenue: string | number;
};

type CampaignRow = {
  source: string;
  medium: string;
  campaign: string;
  content: string | null;
  visits: string | number;
  orders: string | number;
  revenue: string | number;
};

type TimeseriesRow = {
  bucket: string;
  visits: string | number;
  orders: string | number;
  revenue: string | number;
};

type OrderAttributionRow = {
  shopify_order_id: string;
  processed_at: Date | null;
  total_price: string | number;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  attribution_reason: string | null;
};

function requireInternalAuth(authHeader: string | undefined): boolean {
  return authHeader === `Bearer ${env.REPORTING_API_TOKEN}`;
}

function parseInput<TSchema extends z.ZodTypeAny>(schema: TSchema, input: unknown): z.infer<TSchema> {
  try {
    return schema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new ReportingHttpError(400, 'invalid_request', 'Invalid reporting query parameters', error.flatten());
    }

    throw error;
  }
}

function buildMetricDimensionFilters(
  source: string | undefined,
  campaign: string | undefined,
  alias = ''
): { sql: string; params: string[] } {
  const params: string[] = [];
  const qualifiedAlias = alias ? `${alias}.` : '';
  const filters: string[] = [];

  if (source) {
    params.push(source);
    filters.push(`${qualifiedAlias}source = $${params.length + 2}`);
  }

  if (campaign) {
    params.push(campaign);
    filters.push(`${qualifiedAlias}campaign = $${params.length + 2}`);
  }

  return {
    sql: filters.length > 0 ? ` AND ${filters.join(' AND ')}` : '',
    params
  };
}

function buildOrderAttributionFilters(
  source: string | undefined,
  campaign: string | undefined
): { sql: string; params: string[] } {
  const params: string[] = [];
  const filters: string[] = [];

  if (source) {
    params.push(source);
    filters.push(`a.attributed_source = $${params.length + 3}`);
  }

  if (campaign) {
    params.push(campaign);
    filters.push(`a.attributed_campaign = $${params.length + 3}`);
  }

  return {
    sql: filters.length > 0 ? ` AND ${filters.join(' AND ')}` : '',
    params
  };
}

function normalizeContent(value: string | null): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function createReportingRouter(): Router {
  const router = Router();

  router.use((req, res, next) => {
    if (!requireInternalAuth(req.header('authorization') ?? undefined)) {
      res.status(401).json({
        error: 'unauthorized',
        message: 'Unauthorized'
      });
      return;
    }

    next();
  });

  router.get('/summary', async (req, res, next) => {
    try {
      const input = parseInput(baseFiltersSchema, req.query);
      const filters = buildMetricDimensionFilters(input.source, input.campaign);
      const result = await query<SummaryRow>(
        `
          SELECT
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(orders), 0) AS orders,
            COALESCE(SUM(revenue), 0) AS revenue
          FROM daily_campaign_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
        `,
        [input.startDate, input.endDate, ...filters.params]
      );

      const row = result.rows[0];
      const metrics = calculatePerformanceMetrics({
        visits: row?.visits ?? 0,
        orders: row?.orders ?? 0,
        attributedRevenue: row?.revenue ?? 0
      });

      res.json({
        range: {
          startDate: input.startDate,
          endDate: input.endDate
        },
        totals: {
          visits: metrics.visits,
          orders: metrics.orders,
          revenue: metrics.attributedRevenue,
          conversionRate: metrics.conversionRate,
          roas: metrics.roas
        }
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/campaigns', async (req, res, next) => {
    try {
      const input = parseInput(campaignsQuerySchema, req.query);
      const filters = buildMetricDimensionFilters(input.source, input.campaign);
      const result = await query<CampaignRow>(
        `
          SELECT
            source,
            medium,
            campaign,
            NULLIF(content, '') AS content,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(orders), 0) AS orders,
            COALESCE(SUM(revenue), 0) AS revenue
          FROM daily_campaign_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
          GROUP BY source, medium, campaign, content
          ORDER BY revenue DESC, orders DESC, visits DESC, source ASC, campaign ASC
          LIMIT $${filters.params.length + 3}
        `,
        [input.startDate, input.endDate, ...filters.params, input.limit]
      );

      res.json({
        rows: result.rows.map((row) => {
          const visits = Number(row.visits);
          const orders = Number(row.orders);
          const revenue = Number(row.revenue);

          return {
            source: row.source,
            medium: row.medium,
            campaign: row.campaign,
            content: normalizeContent(row.content),
            visits,
            orders,
            revenue,
            conversionRate: visits > 0 ? orders / visits : 0
          };
        }),
        nextCursor: null
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/timeseries', async (req, res, next) => {
    try {
      const input = parseInput(timeseriesQuerySchema, req.query);
      const filters = buildMetricDimensionFilters(input.source, input.campaign);
      const groupExpr =
        input.groupBy === 'source' ? 'source' : input.groupBy === 'campaign' ? 'campaign' : 'metric_date::text';
      const result = await query<TimeseriesRow>(
        `
          SELECT
            ${groupExpr} AS bucket,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(orders), 0) AS orders,
            COALESCE(SUM(revenue), 0) AS revenue
          FROM daily_campaign_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
          GROUP BY bucket
          ORDER BY bucket ASC
        `,
        [input.startDate, input.endDate, ...filters.params]
      );

      res.json({
        points: result.rows.map((row) => ({
          date: row.bucket,
          visits: Number(row.visits),
          orders: Number(row.orders),
          revenue: Number(row.revenue)
        }))
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/orders', async (req, res, next) => {
    try {
      const input = parseInput(ordersQuerySchema, req.query);
      const filters = buildOrderAttributionFilters(input.source, input.campaign);
      const result = await query<OrderAttributionRow>(
        `
          SELECT
            o.shopify_order_id,
            COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS processed_at,
            o.total_price,
            a.attributed_source,
            a.attributed_medium,
            a.attributed_campaign,
            a.attribution_reason
          FROM shopify_orders o
          LEFT JOIN attribution_results a
            ON a.shopify_order_id = o.shopify_order_id
          WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= $1::date
            AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) < ($2::date + interval '1 day')
            ${filters.sql}
          ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) DESC, o.shopify_order_id DESC
          LIMIT $${filters.params.length + 3}
        `,
        [input.startDate, input.endDate, ...filters.params, input.limit]
      );

      res.json({
        rows: result.rows.map((row) => ({
          shopifyOrderId: row.shopify_order_id,
          processedAt: row.processed_at?.toISOString() ?? null,
          totalPrice: Number(row.total_price),
          source: row.attributed_source,
          medium: row.attributed_medium,
          campaign: row.attributed_campaign,
          attributionReason: row.attribution_reason ?? 'unattributed'
        }))
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/reconciliation', async (req, res, next) => {
    try {
      const input = parseInput(reconciliationQuerySchema, req.query);
      const runDate = input.runDate ?? resolveRunDate();
      const report = await fetchDataQualityReport(runDate);

      res.json({
        version: '2026-04-11',
        tenantId: 'roas-radar',
        data: report
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
