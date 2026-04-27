import { Router } from 'express';
import { z } from 'zod';

import { query } from '../../db/pool.js';
import { calculatePerformanceMetrics } from '../../shared/metrics.js';
import { ATTRIBUTION_MODELS } from '../attribution/engine.js';
import { attachAuthContext, requireAuthenticated } from '../auth/index.js';
import { fetchDataQualityReport, resolveRunDate } from '../data-quality/index.js';
import { getReportingTimezone } from '../settings/index.js';

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
  attributionModel: z.enum(ATTRIBUTION_MODELS).optional().default('last_touch'),
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

const orderDetailsParamsSchema = z.object({
  shopifyOrderId: z.string().trim().min(1)
});

const reconciliationQuerySchema = z.object({
  runDate: dateStringSchema.optional()
});

type SummaryRow = {
  visits: string | number;
  orders: string | number;
  revenue: string | number;
  spend: string | number;
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

type SpendDetailRow = {
  source: string;
  medium: string;
  campaign: string;
  spend: string | number;
};

type TimeseriesRow = {
  bucket: string;
  visits: string | number;
  orders: string | number;
  revenue: string | number;
  spend: string | number;
};

type OrderAttributionRow = {
  shopify_order_id: string;
  processed_at: Date | null;
  total_price: string | number;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  match_source: string | null;
  confidence_label: string | null;
  attribution_reason: string | null;
};

type OrderDetailsRow = {
  shopify_order_id: string;
  shopify_order_number: string | null;
  shopify_customer_id: string | null;
  customer_identity_id: string | null;
  email_hash: string | null;
  currency_code: string;
  subtotal_price: string | number;
  total_price: string | number;
  financial_status: string | null;
  fulfillment_status: string | null;
  processed_at: Date | null;
  created_at_shopify: Date | null;
  updated_at_shopify: Date | null;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
  source_name: string | null;
  ingested_at: Date;
  attribution_snapshot: unknown;
  raw_payload: unknown;
};

type OrderLineItemRow = {
  shopify_line_item_id: string;
  shopify_product_id: string | null;
  shopify_variant_id: string | null;
  sku: string | null;
  title: string | null;
  variant_title: string | null;
  vendor: string | null;
  quantity: number;
  price: string | number;
  total_discount: string | number;
  fulfillment_status: string | null;
  requires_shipping: boolean | null;
  taxable: boolean | null;
  ingested_at: Date;
  raw_payload: unknown;
};

type AttributionCreditRow = {
  attribution_model: string;
  touchpoint_position: number;
  session_id: string | null;
  touchpoint_occurred_at: Date | null;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  attributed_content: string | null;
  attributed_term: string | null;
  attributed_click_id_type: string | null;
  attributed_click_id_value: string | null;
  credit_weight: string | number;
  revenue_credit: string | number;
  is_primary: boolean;
  attribution_reason: string;
  match_source: string;
  confidence_label: string;
  created_at: Date;
  model_version: number;
};

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
  attributionModel: string,
  source: string | undefined,
  campaign: string | undefined,
  alias = ''
): { sql: string; params: string[] } {
  const params: string[] = [attributionModel];
  const qualifiedAlias = alias ? `${alias}.` : '';
  const filters: string[] = [`${qualifiedAlias}attribution_model = $3`];

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
  attributionModel: string,
  source: string | undefined,
  campaign: string | undefined
): { sql: string; params: string[] } {
  const params: string[] = [attributionModel];
  const filters: string[] = [];

  if (source) {
    params.push(source);
    filters.push(`c.attributed_source = $${params.length + 2}`);
  }

  if (campaign) {
    params.push(campaign);
    filters.push(`c.attributed_campaign = $${params.length + 2}`);
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

function countDaysInRange(startDate: string, endDate: string): number {
  const start = Date.parse(`${startDate}T00:00:00.000Z`);
  const end = Date.parse(`${endDate}T00:00:00.000Z`);

  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
    return 0;
  }

  return Math.floor((end - start) / 86_400_000) + 1;
}

export function createReportingRouter(): Router {
  const router = Router();

  router.use(attachAuthContext);
  router.use(requireAuthenticated);

  router.get('/summary', async (req, res, next) => {
    try {
      const input = parseInput(baseFiltersSchema, req.query);
      const filters = buildMetricDimensionFilters(input.attributionModel, input.source, input.campaign);
      const result = await query<SummaryRow>(
        `
          SELECT
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue,
            COALESCE(SUM(spend), 0) AS spend
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
        `,
        [input.startDate, input.endDate, ...filters.params]
      );

      const row = result.rows[0];
      const metrics = calculatePerformanceMetrics({
        visits: row?.visits ?? 0,
        orders: row?.orders ?? 0,
        attributedRevenue: row?.revenue ?? 0,
        spend: row?.spend ?? 0
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
          spend: metrics.spend,
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
      const filters = buildMetricDimensionFilters(input.attributionModel, input.source, input.campaign);
      const result = await query<CampaignRow>(
        `
          SELECT
            source,
            medium,
            campaign,
            NULLIF(content, '') AS content,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue
          FROM daily_reporting_metrics
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

  router.get('/spend-details', async (req, res, next) => {
    try {
      const input = parseInput(baseFiltersSchema, req.query);
      const filters = buildMetricDimensionFilters(input.attributionModel, input.source, input.campaign);
      const result = await query<SpendDetailRow>(
        `
          SELECT
            source,
            medium,
            campaign,
            COALESCE(SUM(spend), 0) AS spend
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
            AND spend > 0
            ${filters.sql}
          GROUP BY source, medium, campaign
          ORDER BY spend DESC, source ASC, medium ASC, campaign ASC
        `,
        [input.startDate, input.endDate, ...filters.params]
      );

      const groupMap = new Map<
        string,
        {
          source: string;
          medium: string;
          channel: string;
          subtotal: number;
          campaigns: Array<{
            campaign: string;
            spend: number;
          }>;
        }
      >();

      for (const row of result.rows) {
        const spend = Number(row.spend);
        const source = row.source;
        const medium = row.medium;
        const channel = `${source} / ${medium}`;
        const groupKey = `${source}\u0000${medium}`;
        const existingGroup = groupMap.get(groupKey);

        if (existingGroup) {
          existingGroup.subtotal += spend;
          existingGroup.campaigns.push({
            campaign: row.campaign,
            spend
          });
          continue;
        }

        groupMap.set(groupKey, {
          source,
          medium,
          channel,
          subtotal: spend,
          campaigns: [
            {
              campaign: row.campaign,
              spend
            }
          ]
        });
      }

      const groups = [...groupMap.values()].sort((left, right) => right.subtotal - left.subtotal || left.channel.localeCompare(right.channel));
      const totalSpend = groups.reduce((sum, group) => sum + group.subtotal, 0);
      const rangeDays = countDaysInRange(input.startDate, input.endDate);
      const topChannel = groups[0]
        ? {
            source: groups[0].source,
            medium: groups[0].medium,
            channel: groups[0].channel,
            spend: groups[0].subtotal
          }
        : null;

      res.json({
        summary: {
          totalSpend,
          activeChannels: groups.length,
          activeCampaigns: result.rows.length,
          averageDailySpend: rangeDays > 0 ? totalSpend / rangeDays : 0,
          topChannel
        },
        groups,
        totalSpend
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/timeseries', async (req, res, next) => {
    try {
      const input = parseInput(timeseriesQuerySchema, req.query);
      const filters = buildMetricDimensionFilters(input.attributionModel, input.source, input.campaign);
      const groupExpr =
        input.groupBy === 'source' ? 'source' : input.groupBy === 'campaign' ? 'campaign' : 'metric_date::text';
      const result = await query<TimeseriesRow>(
        `
          SELECT
            ${groupExpr} AS bucket,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue,
            COALESCE(SUM(spend), 0) AS spend
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
          GROUP BY bucket
          ORDER BY bucket ASC
        `,
        [input.startDate, input.endDate, ...filters.params]
      );

      const bucketMetrics = result.rows.map((row) => {
        const metrics = calculatePerformanceMetrics({
          visits: row.visits,
          orders: row.orders,
          attributedRevenue: row.revenue,
          spend: row.spend
        });

        return {
          bucket: row.bucket,
          visits: metrics.visits,
          orders: metrics.orders,
          revenue: metrics.attributedRevenue,
          spend: metrics.spend,
          conversionRate: metrics.conversionRate,
          roas: metrics.roas
        };
      });

      res.json({
        points: bucketMetrics.map((row) => ({
          date: row.bucket,
          visits: row.visits,
          orders: row.orders,
          revenue: row.revenue
        })),
        lowestBuckets: [...bucketMetrics]
          .sort(
            (left, right) =>
              left.revenue - right.revenue ||
              left.orders - right.orders ||
              left.visits - right.visits ||
              left.bucket.localeCompare(right.bucket)
          )
          .slice(0, 3)
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/orders', async (req, res, next) => {
    try {
      const input = parseInput(ordersQuerySchema, req.query);
      const filters = buildOrderAttributionFilters(input.attributionModel, input.source, input.campaign);
      const reportingTimezone = await getReportingTimezone();
      const result = await query<OrderAttributionRow>(
        `
          SELECT
            o.shopify_order_id,
            COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS processed_at,
            o.total_price,
            c.attributed_source,
            c.attributed_medium,
            c.attributed_campaign,
            c.match_source,
            c.confidence_label,
            c.attribution_reason
          FROM shopify_orders o
          LEFT JOIN LATERAL (
            SELECT
              attributed_source,
              attributed_medium,
              attributed_campaign,
              match_source,
              confidence_label,
              attribution_reason
            FROM attribution_order_credits
            WHERE shopify_order_id = o.shopify_order_id
              AND attribution_model = $3
            ORDER BY is_primary DESC, touchpoint_position ASC
            LIMIT 1
          ) c
            ON TRUE
          WHERE timezone($${filters.params.length + 3}::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) >= $1::date
            AND timezone($${filters.params.length + 3}::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) < ($2::date + interval '1 day')
            ${filters.sql}
          ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) DESC, o.shopify_order_id DESC
          LIMIT $${filters.params.length + 4}
        `,
        [input.startDate, input.endDate, ...filters.params, reportingTimezone, input.limit]
      );

      res.json({
        rows: result.rows.map((row) => ({
          shopifyOrderId: row.shopify_order_id,
          processedAt: row.processed_at?.toISOString() ?? null,
          totalPrice: Number(row.total_price),
          source: row.attributed_source,
          medium: row.attributed_medium,
          campaign: row.attributed_campaign,
          matchSource: row.match_source ?? 'unattributed',
          confidenceLabel: row.confidence_label ?? 'none',
          attributionReason: row.attribution_reason ?? 'unattributed'
        }))
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/orders/:shopifyOrderId', async (req, res, next) => {
    try {
      const { shopifyOrderId } = parseInput(orderDetailsParamsSchema, req.params);

      const orderResult = await query<OrderDetailsRow>(
        `
          SELECT
            o.shopify_order_id,
            o.shopify_order_number,
            o.shopify_customer_id,
            o.customer_identity_id::text AS customer_identity_id,
            o.email_hash,
            o.currency_code,
            o.subtotal_price,
            o.total_price,
            o.financial_status,
            o.fulfillment_status,
            o.processed_at,
            o.created_at_shopify,
            o.updated_at_shopify,
            o.landing_session_id::text AS landing_session_id,
            o.checkout_token,
            o.cart_token,
            o.source_name,
            o.ingested_at,
            o.attribution_snapshot,
            o.raw_payload
          FROM shopify_orders o
          WHERE o.shopify_order_id = $1
          LIMIT 1
        `,
        [shopifyOrderId]
      );

      if (!orderResult.rowCount) {
        throw new ReportingHttpError(404, 'order_not_found', `Shopify order ${shopifyOrderId} was not found`);
      }

      const lineItemsResult = await query<OrderLineItemRow>(
        `
          SELECT
            li.shopify_line_item_id,
            li.shopify_product_id,
            li.shopify_variant_id,
            li.sku,
            li.title,
            li.variant_title,
            li.vendor,
            li.quantity,
            li.price,
            li.total_discount,
            li.fulfillment_status,
            li.requires_shipping,
            li.taxable,
            li.ingested_at,
            li.raw_payload
          FROM shopify_order_line_items li
          WHERE li.shopify_order_id = $1
          ORDER BY li.id ASC
        `,
        [shopifyOrderId]
      );

      const creditsResult = await query<AttributionCreditRow>(
        `
          SELECT
            c.attribution_model,
            c.touchpoint_position,
            c.session_id::text AS session_id,
            c.touchpoint_occurred_at,
            c.attributed_source,
            c.attributed_medium,
            c.attributed_campaign,
            c.attributed_content,
            c.attributed_term,
            c.attributed_click_id_type,
            c.attributed_click_id_value,
            c.credit_weight,
            c.revenue_credit,
            c.is_primary,
            c.attribution_reason,
            c.match_source,
            c.confidence_label,
            c.created_at,
            c.model_version
          FROM attribution_order_credits c
          WHERE c.shopify_order_id = $1
          ORDER BY c.attribution_model ASC, c.touchpoint_position ASC
        `,
        [shopifyOrderId]
      );

      const order = orderResult.rows[0];

      res.json({
        order: {
          shopifyOrderId: order.shopify_order_id,
          shopifyOrderNumber: order.shopify_order_number,
          shopifyCustomerId: order.shopify_customer_id,
          customerIdentityId: order.customer_identity_id,
          emailHash: order.email_hash,
          currencyCode: order.currency_code,
          subtotalPrice: Number(order.subtotal_price),
          totalPrice: Number(order.total_price),
          financialStatus: order.financial_status,
          fulfillmentStatus: order.fulfillment_status,
          processedAt: order.processed_at?.toISOString() ?? null,
          createdAtShopify: order.created_at_shopify?.toISOString() ?? null,
          updatedAtShopify: order.updated_at_shopify?.toISOString() ?? null,
          landingSessionId: order.landing_session_id,
          checkoutToken: order.checkout_token,
          cartToken: order.cart_token,
          sourceName: order.source_name,
          ingestedAt: order.ingested_at.toISOString(),
          attributionSnapshot: order.attribution_snapshot,
          rawPayload: order.raw_payload
        },
        lineItems: lineItemsResult.rows.map((row) => ({
          shopifyLineItemId: row.shopify_line_item_id,
          shopifyProductId: row.shopify_product_id,
          shopifyVariantId: row.shopify_variant_id,
          sku: row.sku,
          title: row.title,
          variantTitle: row.variant_title,
          vendor: row.vendor,
          quantity: row.quantity,
          price: Number(row.price),
          totalDiscount: Number(row.total_discount),
          fulfillmentStatus: row.fulfillment_status,
          requiresShipping: row.requires_shipping,
          taxable: row.taxable,
          ingestedAt: row.ingested_at.toISOString(),
          rawPayload: row.raw_payload
        })),
        attributionCredits: creditsResult.rows.map((row) => ({
          attributionModel: row.attribution_model,
          touchpointPosition: row.touchpoint_position,
          sessionId: row.session_id,
          touchpointOccurredAt: row.touchpoint_occurred_at?.toISOString() ?? null,
          source: row.attributed_source,
          medium: row.attributed_medium,
          campaign: row.attributed_campaign,
          content: row.attributed_content,
          term: row.attributed_term,
          clickIdType: row.attributed_click_id_type,
          clickIdValue: row.attributed_click_id_value,
          creditWeight: Number(row.credit_weight),
          revenueCredit: Number(row.revenue_credit),
          isPrimary: row.is_primary,
          attributionReason: row.attribution_reason,
          matchSource: row.match_source,
          confidenceLabel: row.confidence_label,
          createdAt: row.created_at.toISOString(),
          modelVersion: row.model_version
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
