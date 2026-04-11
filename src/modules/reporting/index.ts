import { Router } from 'express';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { ATTRIBUTION_MODELS } from '../attribution/engine.js';

const attributionModelSchema = z.enum(ATTRIBUTION_MODELS).default('last_touch');

const dateRangeSchema = z.object({
  startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  endDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  source: z.string().optional(),
  campaign: z.string().optional(),
  attributionModel: attributionModelSchema.optional()
});

function requireReportingAuth(authHeader: string | undefined): boolean {
  return authHeader === `Bearer ${env.REPORTING_API_TOKEN}`;
}

export function createReportingRouter(): Router {
  const router = Router();

  router.use((req, res, next) => {
    if (!requireReportingAuth(req.header('authorization') ?? undefined)) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }

    next();
  });

  router.get('/summary', async (req, res, next) => {
    try {
      const filters = dateRangeSchema.parse(req.query);
      const attributionModel = filters.attributionModel ?? 'last_touch';
      const result = await query<{
        visits: string;
        orders: string;
        revenue: string;
        spend: string;
        new_customer_orders: string;
        returning_customer_orders: string;
        new_customer_revenue: string;
        returning_customer_revenue: string;
      }>(
        `
          SELECT
            COALESCE(SUM(visits), 0)::text AS visits,
            COALESCE(SUM(attributed_orders), 0)::text AS orders,
            COALESCE(SUM(attributed_revenue), 0)::text AS revenue,
            COALESCE(SUM(spend), 0)::text AS spend,
            COALESCE(SUM(new_customer_orders), 0)::text AS new_customer_orders,
            COALESCE(SUM(returning_customer_orders), 0)::text AS returning_customer_orders,
            COALESCE(SUM(new_customer_revenue), 0)::text AS new_customer_revenue,
            COALESCE(SUM(returning_customer_revenue), 0)::text AS returning_customer_revenue
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
            AND attribution_model = $3
            AND ($4::text IS NULL OR source = $4)
            AND ($5::text IS NULL OR campaign = $5)
        `,
        [filters.startDate, filters.endDate, attributionModel, filters.source ?? null, filters.campaign ?? null]
      );

      const row = result.rows[0];
      const visits = Number(row.visits);
      const orders = Number(row.orders);

      res.json({
        range: {
          startDate: filters.startDate,
          endDate: filters.endDate
        },
        attributionModel,
        totals: {
          visits,
          orders,
          revenue: Number(row.revenue),
          conversionRate: visits === 0 ? 0 : orders / visits,
          spend: Number(row.spend),
          roas: Number(row.spend) === 0 ? null : Number(row.revenue) / Number(row.spend),
          cac: orders === 0 ? null : Number(row.spend) / orders,
          newCustomerOrders: Number(row.new_customer_orders),
          returningCustomerOrders: Number(row.returning_customer_orders),
          newCustomerRevenue: Number(row.new_customer_revenue),
          returningCustomerRevenue: Number(row.returning_customer_revenue)
        }
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/campaigns', async (req, res, next) => {
    try {
      const filters = dateRangeSchema
        .extend({
          limit: z.coerce.number().int().positive().max(200).default(50),
          cursor: z.string().optional()
        })
        .parse(req.query);
      const attributionModel = filters.attributionModel ?? 'last_touch';

      const result = await query<{
        source: string;
        medium: string;
        campaign: string;
        content: string;
        visits: string;
        orders: string;
        revenue: string;
        spend: string;
        new_customer_orders: string;
        returning_customer_orders: string;
      }>(
        `
          SELECT
            source,
            medium,
            campaign,
            content,
            SUM(visits)::text AS visits,
            SUM(attributed_orders)::text AS orders,
            SUM(attributed_revenue)::text AS revenue,
            SUM(spend)::text AS spend,
            SUM(new_customer_orders)::text AS new_customer_orders,
            SUM(returning_customer_orders)::text AS returning_customer_orders
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
            AND attribution_model = $3
            AND ($4::text IS NULL OR source = $4)
            AND ($5::text IS NULL OR campaign = $5)
          GROUP BY source, medium, campaign, content
          ORDER BY SUM(revenue) DESC, campaign ASC
          LIMIT $6
        `,
        [
          filters.startDate,
          filters.endDate,
          attributionModel,
          filters.source ?? null,
          filters.campaign ?? null,
          filters.limit
        ]
      );

      res.json({
        attributionModel,
        rows: result.rows.map((row: (typeof result.rows)[number]) => {
          const visits = Number(row.visits);
          const orders = Number(row.orders);

          return {
            source: row.source,
            medium: row.medium,
            campaign: row.campaign,
            content: row.content || null,
            visits,
            orders,
            revenue: Number(row.revenue),
            spend: Number(row.spend),
            conversionRate: visits === 0 ? 0 : orders / visits,
            roas: Number(row.spend) === 0 ? null : Number(row.revenue) / Number(row.spend),
            cac: orders === 0 ? null : Number(row.spend) / orders,
            newCustomerOrders: Number(row.new_customer_orders),
            returningCustomerOrders: Number(row.returning_customer_orders)
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
      const filters = dateRangeSchema
        .extend({
          groupBy: z.enum(['day', 'source', 'campaign']).default('day')
        })
        .parse(req.query);
      const attributionModel = filters.attributionModel ?? 'last_touch';

      if (filters.groupBy === 'day') {
        const result = await query<{
          metric_date: string;
          visits: string;
          orders: string;
          revenue: string;
          spend: string;
          new_customer_orders: string;
          returning_customer_orders: string;
        }>(
          `
            SELECT
              metric_date::text,
              SUM(visits)::text AS visits,
              SUM(attributed_orders)::text AS orders,
              SUM(attributed_revenue)::text AS revenue,
              SUM(spend)::text AS spend,
              SUM(new_customer_orders)::text AS new_customer_orders,
              SUM(returning_customer_orders)::text AS returning_customer_orders
            FROM daily_reporting_metrics
            WHERE metric_date BETWEEN $1::date AND $2::date
              AND attribution_model = $3
              AND ($4::text IS NULL OR source = $4)
              AND ($5::text IS NULL OR campaign = $5)
            GROUP BY metric_date
            ORDER BY metric_date ASC
          `,
          [filters.startDate, filters.endDate, attributionModel, filters.source ?? null, filters.campaign ?? null]
        );

        res.json({
          attributionModel,
          points: result.rows.map((row: (typeof result.rows)[number]) => {
            const orders = Number(row.orders);
            const spend = Number(row.spend);

            return {
              date: row.metric_date,
              visits: Number(row.visits),
              orders,
              revenue: Number(row.revenue),
              spend,
              roas: spend === 0 ? null : Number(row.revenue) / spend,
              cac: orders === 0 ? null : spend / orders,
              newCustomerOrders: Number(row.new_customer_orders),
              returningCustomerOrders: Number(row.returning_customer_orders)
            };
          })
        });
        return;
      }

      const dimension = filters.groupBy;
      const result = await query<{
        dimension: string;
        visits: string;
        orders: string;
        revenue: string;
        spend: string;
        new_customer_orders: string;
        returning_customer_orders: string;
      }>(
        `
          SELECT
            ${dimension} AS dimension,
            SUM(visits)::text AS visits,
            SUM(attributed_orders)::text AS orders,
            SUM(attributed_revenue)::text AS revenue,
            SUM(spend)::text AS spend,
            SUM(new_customer_orders)::text AS new_customer_orders,
            SUM(returning_customer_orders)::text AS returning_customer_orders
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
            AND attribution_model = $3
            AND ($4::text IS NULL OR source = $4)
            AND ($5::text IS NULL OR campaign = $5)
          GROUP BY ${dimension}
          ORDER BY SUM(revenue) DESC, ${dimension} ASC
        `,
        [filters.startDate, filters.endDate, attributionModel, filters.source ?? null, filters.campaign ?? null]
      );

      res.json({
        attributionModel,
        points: result.rows.map((row: (typeof result.rows)[number]) => {
          const orders = Number(row.orders);
          const spend = Number(row.spend);

          return {
            [dimension]: row.dimension,
            visits: Number(row.visits),
            orders,
            revenue: Number(row.revenue),
            spend,
            roas: spend === 0 ? null : Number(row.revenue) / spend,
            cac: orders === 0 ? null : spend / orders,
            newCustomerOrders: Number(row.new_customer_orders),
            returningCustomerOrders: Number(row.returning_customer_orders)
          };
        })
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/orders', async (req, res, next) => {
    try {
      const filters = dateRangeSchema
        .extend({
          limit: z.coerce.number().int().positive().max(200).default(50)
        })
        .parse(req.query);
      const attributionModel = filters.attributionModel ?? 'last_touch';

      const result = await query<{
        shopify_order_id: string;
        processed_at: Date | null;
        total_price: string;
        attributed_source: string | null;
        attributed_medium: string | null;
        attributed_campaign: string | null;
        attribution_reason: string;
        revenue_credit: string;
      }>(
        `
          WITH ranked_credits AS (
            SELECT
              c.shopify_order_id,
              c.attributed_source,
              c.attributed_medium,
              c.attributed_campaign,
              c.attribution_reason,
              c.revenue_credit,
              ROW_NUMBER() OVER (
                PARTITION BY c.shopify_order_id, c.attribution_model
                ORDER BY c.revenue_credit DESC, c.touchpoint_position ASC
              ) AS credit_rank
            FROM attribution_order_credits c
            WHERE c.attribution_model = $3
          )
          SELECT
            o.shopify_order_id,
            o.processed_at,
            o.total_price::text,
            rc.attributed_source,
            rc.attributed_medium,
            rc.attributed_campaign,
            COALESCE(rc.attribution_reason, 'unattributed') AS attribution_reason,
            COALESCE(rc.revenue_credit, '0.00') AS revenue_credit
          FROM shopify_orders o
          LEFT JOIN ranked_credits rc
            ON rc.shopify_order_id = o.shopify_order_id
           AND rc.credit_rank = 1
          WHERE DATE(COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) BETWEEN $1::date AND $2::date
            AND ($4::text IS NULL OR rc.attributed_source = $4)
            AND ($5::text IS NULL OR rc.attributed_campaign = $5)
          ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) DESC
          LIMIT $6
        `,
        [
          filters.startDate,
          filters.endDate,
          attributionModel,
          filters.source ?? null,
          filters.campaign ?? null,
          filters.limit
        ]
      );

      res.json({
        attributionModel,
        rows: result.rows.map((row: (typeof result.rows)[number]) => ({
          shopifyOrderId: row.shopify_order_id,
          processedAt: row.processed_at?.toISOString() ?? null,
          totalPrice: Number(row.total_price),
          source: row.attributed_source,
          medium: row.attributed_medium,
          campaign: row.attributed_campaign,
          attributionReason: row.attribution_reason,
          revenueCredit: Number(row.revenue_credit)
        }))
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
