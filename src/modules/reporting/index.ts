import { Buffer } from 'node:buffer';

import { Router, type Request } from 'express';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { ATTRIBUTION_MODELS, type AttributionModel } from '../attribution/engine.js';

const REPORTING_API_VERSION = '2026-04-11';
const REPORTING_TENANT_HEADER = 'x-roas-radar-tenant-id';
const REQUIRED_REPORTING_SCOPE = 'reporting:read';

const attributionModelSchema = z.enum(ATTRIBUTION_MODELS).default('last_touch');

const baseFilterSchema = z
  .object({
    startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    endDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    attributionModel: attributionModelSchema.optional(),
    source: z.string().trim().min(1).optional(),
    medium: z.string().trim().min(1).optional(),
    campaign: z.string().trim().min(1).optional(),
    content: z.string().trim().min(1).optional(),
    search: z.string().trim().min(1).optional()
  })
  .refine((value) => value.startDate <= value.endDate, {
    message: 'startDate must be before or equal to endDate',
    path: ['startDate']
  });

const paginationSchema = z.object({
  limit: z.coerce.number().int().positive().max(100).default(25),
  cursor: z.string().optional()
});

const rowValueSchema = z.object({
  revenue: z.string(),
  source: z.string(),
  medium: z.string(),
  campaign: z.string().optional(),
  content: z.string().optional()
});

type ReportingPrincipal = {
  scopes: string[];
  tenantId: string;
};

type ReportingFilters = z.infer<typeof baseFilterSchema> & {
  attributionModel: AttributionModel;
};

type CursorValue = {
  revenue: string;
  source: string;
  medium: string;
  campaign?: string;
  content?: string;
};

type OverviewRow = {
  visits: string;
  orders: string;
  revenue: string;
  spend: string;
  clicks: string;
  impressions: string;
  new_customer_orders: string;
  returning_customer_orders: string;
  new_customer_revenue: string;
  returning_customer_revenue: string;
};

type TimeseriesRow = {
  metric_date: string;
  visits: string;
  orders: string;
  revenue: string;
  spend: string;
  clicks: string;
  impressions: string;
};

type ChannelRow = {
  source: string;
  medium: string;
  visits: string;
  orders: string;
  revenue: string;
  spend: string;
  clicks: string;
  impressions: string;
};

type CreativeRow = {
  source: string;
  medium: string;
  campaign: string;
  content: string;
  visits: string;
  orders: string;
  revenue: string;
  spend: string;
  clicks: string;
  impressions: string;
};

type OrderRow = {
  shopify_order_id: string;
  processed_at: Date | null;
  total_price: string;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  attributed_content: string | null;
  attribution_reason: string;
  revenue_credit: string;
};

type ResponseEnvelope<T> = {
  version: string;
  generatedAt: string;
  tenantId: string;
  attributionModel: AttributionModel;
  filters: {
    startDate: string;
    endDate: string;
    source?: string;
    medium?: string;
    campaign?: string;
    content?: string;
    search?: string;
  };
  data: T;
};

function toNumber(value: string | number | null | undefined): number {
  if (typeof value === 'number') {
    return value;
  }

  if (typeof value !== 'string') {
    return 0;
  }

  return Number(value);
}

function safeDivide(numerator: number, denominator: number): number | null {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) {
    return null;
  }

  return numerator / denominator;
}

function parseReportingAuth(authHeader: string | undefined): ReportingPrincipal | null {
  if (!authHeader?.startsWith('Bearer ')) {
    return null;
  }

  const token = authHeader.slice('Bearer '.length).trim();

  if (token !== env.REPORTING_API_TOKEN) {
    return null;
  }

  return {
    tenantId: env.REPORTING_TENANT_ID,
    scopes: env.REPORTING_API_SCOPES
  };
}

function assertReportingAccess(req: Request): ReportingPrincipal {
  const principal = parseReportingAuth(req.header('authorization') ?? undefined);

  if (!principal) {
    throw {
      statusCode: 401,
      code: 'reporting_unauthorized',
      message: 'A valid reporting bearer token is required.'
    };
  }

  if (!principal.scopes.includes(REQUIRED_REPORTING_SCOPE)) {
    throw {
      statusCode: 403,
      code: 'reporting_scope_forbidden',
      message: `Missing required scope ${REQUIRED_REPORTING_SCOPE}.`
    };
  }

  const tenantId = req.header(REPORTING_TENANT_HEADER);

  if (!tenantId) {
    throw {
      statusCode: 401,
      code: 'reporting_tenant_required',
      message: `${REPORTING_TENANT_HEADER} header is required.`
    };
  }

  if (tenantId !== principal.tenantId) {
    throw {
      statusCode: 403,
      code: 'reporting_tenant_forbidden',
      message: `Tenant ${tenantId} is not authorized for this reporting token.`
    };
  }

  return principal;
}

function normalizeFilters(input: z.infer<typeof baseFilterSchema>): ReportingFilters {
  return {
    ...input,
    attributionModel: input.attributionModel ?? 'last_touch'
  };
}

function encodeCursor(value: CursorValue): string {
  return Buffer.from(JSON.stringify(value), 'utf8').toString('base64url');
}

function decodeCursor<T extends CursorValue>(
  cursor: string | undefined,
  schema: z.ZodType<T>
): T | null {
  if (!cursor) {
    return null;
  }

  const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  return schema.parse(parsed);
}

function createEnvelope<T>(
  principal: ReportingPrincipal,
  filters: ReportingFilters,
  data: T
): ResponseEnvelope<T> {
  return {
    version: REPORTING_API_VERSION,
    generatedAt: new Date().toISOString(),
    tenantId: principal.tenantId,
    attributionModel: filters.attributionModel,
    filters: {
      startDate: filters.startDate,
      endDate: filters.endDate,
      source: filters.source,
      medium: filters.medium,
      campaign: filters.campaign,
      content: filters.content,
      search: filters.search
    },
    data
  };
}

function baseMetricParams(filters: ReportingFilters): unknown[] {
  return [
    filters.startDate,
    filters.endDate,
    filters.attributionModel,
    filters.source ?? null,
    filters.medium ?? null,
    filters.campaign ?? null,
    filters.content ?? null,
    filters.search ? `%${filters.search}%` : null
  ];
}

function baseMetricWhereClause(): string {
  return `
    metric_date BETWEEN $1::date AND $2::date
    AND attribution_model = $3
    AND ($4::text IS NULL OR source = $4)
    AND ($5::text IS NULL OR medium = $5)
    AND ($6::text IS NULL OR campaign = $6)
    AND ($7::text IS NULL OR content = $7)
    AND (
      $8::text IS NULL
      OR source ILIKE $8
      OR medium ILIKE $8
      OR campaign ILIKE $8
      OR content ILIKE $8
    )
  `;
}

async function fetchOverview(filters: ReportingFilters): Promise<OverviewRow> {
  const result = await query<OverviewRow>(
    `
      SELECT
        COALESCE(SUM(visits), 0)::text AS visits,
        COALESCE(SUM(attributed_orders), 0)::text AS orders,
        COALESCE(SUM(attributed_revenue), 0)::text AS revenue,
        COALESCE(SUM(spend), 0)::text AS spend,
        COALESCE(SUM(clicks), 0)::text AS clicks,
        COALESCE(SUM(impressions), 0)::text AS impressions,
        COALESCE(SUM(new_customer_orders), 0)::text AS new_customer_orders,
        COALESCE(SUM(returning_customer_orders), 0)::text AS returning_customer_orders,
        COALESCE(SUM(new_customer_revenue), 0)::text AS new_customer_revenue,
        COALESCE(SUM(returning_customer_revenue), 0)::text AS returning_customer_revenue
      FROM daily_reporting_metrics
      WHERE ${baseMetricWhereClause()}
    `,
    baseMetricParams(filters)
  );

  return result.rows[0];
}

function formatOverviewResponse(principal: ReportingPrincipal, filters: ReportingFilters, row: OverviewRow) {
  const visits = toNumber(row.visits);
  const orders = toNumber(row.orders);
  const revenue = toNumber(row.revenue);
  const spend = toNumber(row.spend);
  const clicks = toNumber(row.clicks);
  const impressions = toNumber(row.impressions);

  return createEnvelope(principal, filters, {
    totals: {
      visits,
      orders,
      revenue,
      spend,
      clicks,
      impressions,
      conversionRate: safeDivide(orders, visits) ?? 0,
      roas: safeDivide(revenue, spend),
      cac: safeDivide(spend, orders),
      averageOrderValue: safeDivide(revenue, orders),
      clickThroughRate: safeDivide(clicks, impressions),
      newCustomerOrders: toNumber(row.new_customer_orders),
      returningCustomerOrders: toNumber(row.returning_customer_orders),
      newCustomerRevenue: toNumber(row.new_customer_revenue),
      returningCustomerRevenue: toNumber(row.returning_customer_revenue)
    }
  });
}

async function fetchTimeseries(filters: ReportingFilters): Promise<TimeseriesRow[]> {
  const result = await query<TimeseriesRow>(
    `
      SELECT
        metric_date::text,
        SUM(visits)::text AS visits,
        SUM(attributed_orders)::text AS orders,
        SUM(attributed_revenue)::text AS revenue,
        SUM(spend)::text AS spend,
        SUM(clicks)::text AS clicks,
        SUM(impressions)::text AS impressions
      FROM daily_reporting_metrics
      WHERE ${baseMetricWhereClause()}
      GROUP BY metric_date
      ORDER BY metric_date ASC
    `,
    baseMetricParams(filters)
  );

  return result.rows;
}

function formatTimeseriesResponse(principal: ReportingPrincipal, filters: ReportingFilters, rows: TimeseriesRow[]) {
  return createEnvelope(principal, filters, {
    points: rows.map((row) => {
      const visits = toNumber(row.visits);
      const orders = toNumber(row.orders);
      const revenue = toNumber(row.revenue);
      const spend = toNumber(row.spend);
      const clicks = toNumber(row.clicks);
      const impressions = toNumber(row.impressions);

      return {
        date: row.metric_date,
        visits,
        orders,
        revenue,
        spend,
        clicks,
        impressions,
        conversionRate: safeDivide(orders, visits) ?? 0,
        roas: safeDivide(revenue, spend),
        clickThroughRate: safeDivide(clicks, impressions)
      };
    })
  });
}

async function fetchChannels(
  filters: ReportingFilters,
  limit: number,
  cursor: CursorValue | null
): Promise<{ rows: ChannelRow[]; nextCursor: string | null }> {
  const params = [
    ...baseMetricParams(filters),
    cursor?.revenue ?? null,
    cursor?.source ?? null,
    cursor?.medium ?? null,
    limit + 1
  ];

  const result = await query<ChannelRow>(
    `
      WITH grouped AS (
        SELECT
          source,
          medium,
          SUM(visits)::bigint AS visits,
          SUM(attributed_orders)::numeric(12, 8) AS orders,
          SUM(attributed_revenue)::numeric(12, 2) AS revenue,
          SUM(spend)::numeric(12, 2) AS spend,
          SUM(clicks)::bigint AS clicks,
          SUM(impressions)::bigint AS impressions
        FROM daily_reporting_metrics
        WHERE ${baseMetricWhereClause()}
        GROUP BY source, medium
      )
      SELECT
        source,
        medium,
        visits::text AS visits,
        orders::text AS orders,
        revenue::text AS revenue,
        spend::text AS spend,
        clicks::text AS clicks,
        impressions::text AS impressions
      FROM grouped
      WHERE (
        $9::numeric IS NULL
        OR revenue < $9::numeric
        OR (revenue = $9::numeric AND (source, medium) > ($10::text, $11::text))
      )
      ORDER BY revenue DESC, source ASC, medium ASC
      LIMIT $12
    `,
    params
  );

  const pageRows = result.rows.slice(0, limit);
  const nextRow = result.rows[limit];
  const nextCursor = nextRow
    ? encodeCursor({
        revenue: nextRow.revenue,
        source: nextRow.source,
        medium: nextRow.medium
      })
    : null;

  return {
    rows: pageRows,
    nextCursor
  };
}

function formatChannelRows(rows: ChannelRow[]) {
  const totalRevenue = rows.reduce((sum, row) => sum + toNumber(row.revenue), 0);

  return rows.map((row) => {
    const visits = toNumber(row.visits);
    const orders = toNumber(row.orders);
    const revenue = toNumber(row.revenue);
    const spend = toNumber(row.spend);

    return {
      source: row.source,
      medium: row.medium,
      visits,
      orders,
      revenue,
      spend,
      clicks: toNumber(row.clicks),
      impressions: toNumber(row.impressions),
      conversionRate: safeDivide(orders, visits) ?? 0,
      roas: safeDivide(revenue, spend),
      shareOfRevenue: safeDivide(revenue, totalRevenue) ?? 0
    };
  });
}

async function fetchCreatives(
  filters: ReportingFilters,
  limit: number,
  cursor: CursorValue | null
): Promise<{ rows: CreativeRow[]; nextCursor: string | null }> {
  const params = [
    ...baseMetricParams(filters),
    cursor?.revenue ?? null,
    cursor?.source ?? null,
    cursor?.medium ?? null,
    cursor?.campaign ?? null,
    cursor?.content ?? null,
    limit + 1
  ];

  const result = await query<CreativeRow>(
    `
      WITH grouped AS (
        SELECT
          source,
          medium,
          campaign,
          content,
          SUM(visits)::bigint AS visits,
          SUM(attributed_orders)::numeric(12, 8) AS orders,
          SUM(attributed_revenue)::numeric(12, 2) AS revenue,
          SUM(spend)::numeric(12, 2) AS spend,
          SUM(clicks)::bigint AS clicks,
          SUM(impressions)::bigint AS impressions
        FROM daily_reporting_metrics
        WHERE ${baseMetricWhereClause()}
        GROUP BY source, medium, campaign, content
      )
      SELECT
        source,
        medium,
        campaign,
        content,
        visits::text AS visits,
        orders::text AS orders,
        revenue::text AS revenue,
        spend::text AS spend,
        clicks::text AS clicks,
        impressions::text AS impressions
      FROM grouped
      WHERE (
        $9::numeric IS NULL
        OR revenue < $9::numeric
        OR (
          revenue = $9::numeric
          AND (source, medium, campaign, content) > ($10::text, $11::text, $12::text, $13::text)
        )
      )
      ORDER BY revenue DESC, source ASC, medium ASC, campaign ASC, content ASC
      LIMIT $14
    `,
    params
  );

  const pageRows = result.rows.slice(0, limit);
  const nextRow = result.rows[limit];
  const nextCursor = nextRow
    ? encodeCursor({
        revenue: nextRow.revenue,
        source: nextRow.source,
        medium: nextRow.medium,
        campaign: nextRow.campaign,
        content: nextRow.content
      })
    : null;

  return {
    rows: pageRows,
    nextCursor
  };
}

function formatCreativeRows(rows: CreativeRow[]) {
  return rows.map((row) => {
    const visits = toNumber(row.visits);
    const orders = toNumber(row.orders);
    const revenue = toNumber(row.revenue);
    const spend = toNumber(row.spend);

    return {
      source: row.source,
      medium: row.medium,
      campaign: row.campaign,
      content: row.content,
      visits,
      orders,
      revenue,
      spend,
      clicks: toNumber(row.clicks),
      impressions: toNumber(row.impressions),
      conversionRate: safeDivide(orders, visits) ?? 0,
      roas: safeDivide(revenue, spend)
    };
  });
}

async function fetchOrders(filters: ReportingFilters, limit: number): Promise<OrderRow[]> {
  const result = await query<OrderRow>(
    `
      WITH ranked_credits AS (
        SELECT
          c.shopify_order_id,
          c.attributed_source,
          c.attributed_medium,
          c.attributed_campaign,
          c.attributed_content,
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
        COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS processed_at,
        o.total_price::text,
        rc.attributed_source,
        rc.attributed_medium,
        rc.attributed_campaign,
        rc.attributed_content,
        COALESCE(rc.attribution_reason, 'unattributed') AS attribution_reason,
        COALESCE(rc.revenue_credit, '0.00') AS revenue_credit
      FROM shopify_orders o
      LEFT JOIN ranked_credits rc
        ON rc.shopify_order_id = o.shopify_order_id
       AND rc.credit_rank = 1
      WHERE DATE(COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) BETWEEN $1::date AND $2::date
        AND ($4::text IS NULL OR rc.attributed_source = $4)
        AND ($5::text IS NULL OR rc.attributed_medium = $5)
        AND ($6::text IS NULL OR rc.attributed_campaign = $6)
        AND ($7::text IS NULL OR rc.attributed_content = $7)
      ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) DESC, o.shopify_order_id DESC
      LIMIT $8
    `,
    [
      filters.startDate,
      filters.endDate,
      filters.attributionModel,
      filters.source ?? null,
      filters.medium ?? null,
      filters.campaign ?? null,
      filters.content ?? null,
      limit
    ]
  );

  return result.rows;
}

const overviewResponseSchema = z.object({
  version: z.string(),
  generatedAt: z.string(),
  tenantId: z.string(),
  attributionModel: attributionModelSchema,
  filters: z.object({
    startDate: z.string(),
    endDate: z.string(),
    source: z.string().optional(),
    medium: z.string().optional(),
    campaign: z.string().optional(),
    content: z.string().optional(),
    search: z.string().optional()
  }),
  data: z.object({
    totals: z.object({
      visits: z.number(),
      orders: z.number(),
      revenue: z.number(),
      spend: z.number(),
      clicks: z.number(),
      impressions: z.number(),
      conversionRate: z.number(),
      roas: z.number().nullable(),
      cac: z.number().nullable(),
      averageOrderValue: z.number().nullable(),
      clickThroughRate: z.number().nullable(),
      newCustomerOrders: z.number(),
      returningCustomerOrders: z.number(),
      newCustomerRevenue: z.number(),
      returningCustomerRevenue: z.number()
    })
  })
});

const timeseriesResponseSchema = z.object({
  version: z.string(),
  generatedAt: z.string(),
  tenantId: z.string(),
  attributionModel: attributionModelSchema,
  filters: overviewResponseSchema.shape.filters,
  data: z.object({
    points: z.array(
      z.object({
        date: z.string(),
        visits: z.number(),
        orders: z.number(),
        revenue: z.number(),
        spend: z.number(),
        clicks: z.number(),
        impressions: z.number(),
        conversionRate: z.number(),
        roas: z.number().nullable(),
        clickThroughRate: z.number().nullable()
      })
    )
  })
});

const paginatedResponseSchema = z.object({
  version: z.string(),
  generatedAt: z.string(),
  tenantId: z.string(),
  attributionModel: attributionModelSchema,
  filters: overviewResponseSchema.shape.filters,
  data: z.object({
    rows: z.array(z.record(z.union([z.string(), z.number(), z.null()]))),
    pagination: z.object({
      limit: z.number(),
      nextCursor: z.string().nullable()
    })
  })
});

const modelsResponseSchema = z.object({
  version: z.string(),
  generatedAt: z.string(),
  tenantId: z.string(),
  data: z.object({
    defaultModel: attributionModelSchema,
    supportedModels: z.array(attributionModelSchema),
    requiredScope: z.string()
  })
});

export function createReportingRouter(): Router {
  const router = Router();

  router.use((req, res, next) => {
    try {
      const principal = assertReportingAccess(req);
      res.locals.reportingPrincipal = principal;
      next();
    } catch (error) {
      next(error);
    }
  });

  router.get('/models', (req, res, next) => {
    try {
      const principal = res.locals.reportingPrincipal as ReportingPrincipal;
      const response = modelsResponseSchema.parse({
        version: REPORTING_API_VERSION,
        generatedAt: new Date().toISOString(),
        tenantId: principal.tenantId,
        data: {
          defaultModel: 'last_touch',
          supportedModels: ATTRIBUTION_MODELS,
          requiredScope: REQUIRED_REPORTING_SCOPE
        }
      });

      res.json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/overview', async (req, res, next) => {
    try {
      const principal = res.locals.reportingPrincipal as ReportingPrincipal;
      const filters = normalizeFilters(baseFilterSchema.parse(req.query));
      const overview = await fetchOverview(filters);
      const response = overviewResponseSchema.parse(formatOverviewResponse(principal, filters, overview));
      res.json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/summary', async (req, res, next) => {
    try {
      const principal = res.locals.reportingPrincipal as ReportingPrincipal;
      const filters = normalizeFilters(baseFilterSchema.parse(req.query));
      const overview = await fetchOverview(filters);
      const response = overviewResponseSchema.parse(formatOverviewResponse(principal, filters, overview));
      res.json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/timeseries', async (req, res, next) => {
    try {
      const principal = res.locals.reportingPrincipal as ReportingPrincipal;
      const filters = normalizeFilters(baseFilterSchema.parse(req.query));
      const rows = await fetchTimeseries(filters);
      const response = timeseriesResponseSchema.parse(formatTimeseriesResponse(principal, filters, rows));
      res.json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/channels', async (req, res, next) => {
    try {
      const principal = res.locals.reportingPrincipal as ReportingPrincipal;
      const filters = normalizeFilters(baseFilterSchema.parse(req.query));
      const pagination = paginationSchema.parse(req.query);
      const cursor = decodeCursor(
        pagination.cursor,
        rowValueSchema.pick({ revenue: true, source: true, medium: true })
      );
      const page = await fetchChannels(filters, pagination.limit, cursor);
      const response = paginatedResponseSchema.parse(
        createEnvelope(principal, filters, {
          rows: formatChannelRows(page.rows),
          pagination: {
            limit: pagination.limit,
            nextCursor: page.nextCursor
          }
        })
      );
      res.json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/creatives', async (req, res, next) => {
    try {
      const principal = res.locals.reportingPrincipal as ReportingPrincipal;
      const filters = normalizeFilters(baseFilterSchema.parse(req.query));
      const pagination = paginationSchema.parse(req.query);
      const cursor = decodeCursor(pagination.cursor, rowValueSchema);
      const page = await fetchCreatives(filters, pagination.limit, cursor);
      const response = paginatedResponseSchema.parse(
        createEnvelope(principal, filters, {
          rows: formatCreativeRows(page.rows),
          pagination: {
            limit: pagination.limit,
            nextCursor: page.nextCursor
          }
        })
      );
      res.json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/campaigns', async (req, res, next) => {
    try {
      const principal = res.locals.reportingPrincipal as ReportingPrincipal;
      const filters = normalizeFilters(baseFilterSchema.parse(req.query));
      const pagination = paginationSchema.parse(req.query);
      const cursor = decodeCursor(pagination.cursor, rowValueSchema);
      const page = await fetchCreatives(filters, pagination.limit, cursor);
      const response = paginatedResponseSchema.parse(
        createEnvelope(principal, filters, {
          rows: formatCreativeRows(page.rows),
          pagination: {
            limit: pagination.limit,
            nextCursor: page.nextCursor
          }
        })
      );
      res.json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/orders', async (req, res, next) => {
    try {
      const principal = res.locals.reportingPrincipal as ReportingPrincipal;
      const filters = normalizeFilters(baseFilterSchema.parse(req.query));
      const pagination = z.object({ limit: z.coerce.number().int().positive().max(100).default(50) }).parse(req.query);
      const rows = await fetchOrders(filters, pagination.limit);
      const response = paginatedResponseSchema.parse(
        createEnvelope(principal, filters, {
          rows: rows.map((row) => ({
            shopifyOrderId: row.shopify_order_id,
            processedAt: row.processed_at?.toISOString() ?? null,
            totalPrice: toNumber(row.total_price),
            source: row.attributed_source,
            medium: row.attributed_medium,
            campaign: row.attributed_campaign,
            content: row.attributed_content,
            attributionReason: row.attribution_reason,
            revenueCredit: toNumber(row.revenue_credit)
          })),
          pagination: {
            limit: pagination.limit,
            nextCursor: null
          }
        })
      );
      res.json(response);
    } catch (error) {
      next(error);
    }
  });

  return router;
}
