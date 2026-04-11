import { Router } from 'express';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { calculatePerformanceMetrics } from '../../shared/metrics.js';
import { fetchDataQualityReport, resolveRunDate } from '../data-quality/index.js';

const dateRangeSchema = z.object({
  startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  endDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  source: z.string().optional(),
  campaign: z.string().optional()
});

const campaignsQuerySchema = dateRangeSchema.extend({
  limit: z.coerce.number().int().positive().max(200).optional().default(50)
});

const timeseriesQuerySchema = dateRangeSchema.extend({
  groupBy: z.enum(['day', 'source', 'campaign']).optional().default('day')
});

const ordersQuerySchema = dateRangeSchema.extend({
  limit: z.coerce.number().int().positive().max(200).optional().default(50)
});

const reconciliationQuerySchema = z.object({
  runDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional()
});

function requireInternalAuth(authHeader: string | undefined): boolean {
  return authHeader === `Bearer ${env.REPORTING_API_TOKEN}`;
}

type SummaryRow = {
  visits: string | number;
  attributed_orders: string | number;
  attributed_revenue: string | number;
  spend: string | number;
  clicks: string | number;
  impressions: string | number;
  new_customer_orders: string | number;
  returning_customer_orders: string | number;
  new_customer_revenue: string | number;
  returning_customer_revenue: string | number;
};

function buildDimensionFilters(
  source: string | undefined,
  campaign: string | undefined
): { sql: string; params: string[] } {
  const filters: string[] = [];
  const params: string[] = [];

  if (source) {
    params.push(source);
    filters.push(`source = $${params.length + 2}`);
  }

  if (campaign) {
    params.push(campaign);
    filters.push(`campaign = $${params.length + 2}`);
  }

  return {
    sql: filters.length > 0 ? ` AND ${filters.join(' AND ')}` : '',
    params
  };
}

export function createReportingRouter(): Router {
  const router = Router();

  router.use((req, res, next) => {
    if (!requireInternalAuth(req.header('authorization') ?? undefined)) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }

    next();
  });

  router.get('/summary', async (req, res, next) => {
    try {
      const input = dateRangeSchema.parse(req.query);
      const filters = buildDimensionFilters(input.source, input.campaign);
      const result = await query<SummaryRow>(
        `
          SELECT
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS attributed_orders,
            COALESCE(SUM(attributed_revenue), 0) AS attributed_revenue,
            COALESCE(SUM(spend), 0) AS spend,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(new_customer_orders), 0) AS new_customer_orders,
            COALESCE(SUM(returning_customer_orders), 0) AS returning_customer_orders,
            COALESCE(SUM(new_customer_revenue), 0) AS new_customer_revenue,
            COALESCE(SUM(returning_customer_revenue), 0) AS returning_customer_revenue
          FROM daily_reporting_metrics
          WHERE attribution_model = 'last_touch'
            AND metric_date BETWEEN $1::date AND $2::date
            ${filters.sql}
        `,
        [input.startDate, input.endDate, ...filters.params]
      );
      const row = result.rows[0];
      const metrics = calculatePerformanceMetrics({
        visits: row?.visits ?? 0,
        orders: row?.attributed_orders ?? 0,
        attributedRevenue: row?.attributed_revenue ?? 0,
        spend: row?.spend ?? 0,
        clicks: row?.clicks ?? 0,
        impressions: row?.impressions ?? 0,
        newCustomerOrders: row?.new_customer_orders ?? 0,
        returningCustomerOrders: row?.returning_customer_orders ?? 0,
        newCustomerRevenue: row?.new_customer_revenue ?? 0,
        returningCustomerRevenue: row?.returning_customer_revenue ?? 0
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
      const input = campaignsQuerySchema.parse(req.query);
      const filters = buildDimensionFilters(input.source, input.campaign);
      const result = await query<{
        source: string;
        medium: string;
        campaign: string;
        content: string;
        visits: string | number;
        orders: string | number;
        revenue: string | number;
      }>(
        `
          SELECT
            source,
            medium,
            campaign,
            content,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue
          FROM daily_reporting_metrics
          WHERE attribution_model = 'last_touch'
            AND metric_date BETWEEN $1::date AND $2::date
            ${filters.sql}
          GROUP BY source, medium, campaign, content
          ORDER BY revenue DESC, orders DESC, visits DESC
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
            content: row.content,
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
      const input = timeseriesQuerySchema.parse(req.query);
      const filters = buildDimensionFilters(input.source, input.campaign);
      const groupExpr =
        input.groupBy === 'source' ? 'source' : input.groupBy === 'campaign' ? 'campaign' : `metric_date::text`;
      const result = await query<{
        bucket: string;
        visits: string | number;
        orders: string | number;
        revenue: string | number;
      }>(
        `
          SELECT
            ${groupExpr} AS bucket,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue
          FROM daily_reporting_metrics
          WHERE attribution_model = 'last_touch'
            AND metric_date BETWEEN $1::date AND $2::date
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
      const input = ordersQuerySchema.parse(req.query);
      const result = await query<{
        shopify_order_id: string;
        processed_at: Date | null;
        total_price: string | number;
        attributed_source: string | null;
        attributed_medium: string | null;
        attributed_campaign: string | null;
        attribution_reason: string;
      }>(
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
          WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) BETWEEN $1::date AND ($2::date + interval '1 day')
          ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) DESC
          LIMIT $3
        `,
        [input.startDate, input.endDate, input.limit]
      );

      res.json({
        rows: result.rows.map((row) => ({
          shopifyOrderId: row.shopify_order_id,
          processedAt: row.processed_at?.toISOString() ?? null,
          totalPrice: Number(row.total_price),
          source: row.attributed_source,
          medium: row.attributed_medium,
          campaign: row.attributed_campaign,
          attributionReason: row.attribution_reason
        }))
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/reconciliation', async (req, res, next) => {
    try {
      const input = reconciliationQuerySchema.parse(req.query);
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
