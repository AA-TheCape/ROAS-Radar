import { Buffer } from 'node:buffer';

import { Router, type Request } from 'express';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { calculatePerformanceMetrics, safeDivide, toNumber } from '../../shared/metrics.js';
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
  content: z.string().optional(),
  creativeId: z.string().optional(),
  creativeName: z.string().optional()
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
  creativeId?: string;
  creativeName?: string;
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
  new_customer_orders: string;
  returning_customer_orders: string;
  new_customer_revenue: string;
  returning_customer_revenue: string;
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
  new_customer_orders: string;
  returning_customer_orders: string;
  new_customer_revenue: string;
  returning_customer_revenue: string;
};

type CampaignRow = {
  source: string;
  medium: string;
  campaign: string;
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

type CreativeRow = {
  source: string;
  medium: string;
  campaign: string;
  campaign_id: string | null;
  campaign_name: string | null;
  ad_id: string | null;
  ad_name: string | null;
  creative_id: string | null;
  creative_name: string | null;
  content: string;
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
  const metrics = calculatePerformanceMetrics({
    visits: row.visits,
    orders: row.orders,
    attributedRevenue: row.revenue,
    spend: row.spend,
    clicks: row.clicks,
    impressions: row.impressions,
    newCustomerOrders: row.new_customer_orders,
    returningCustomerOrders: row.returning_customer_orders,
    newCustomerRevenue: row.new_customer_revenue,
    returningCustomerRevenue: row.returning_customer_revenue
  });

  return createEnvelope(principal, filters, {
    totals: metrics
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
        SUM(impressions)::text AS impressions,
        SUM(new_customer_orders)::text AS new_customer_orders,
        SUM(returning_customer_orders)::text AS returning_customer_orders,
        SUM(new_customer_revenue)::text AS new_customer_revenue,
        SUM(returning_customer_revenue)::text AS returning_customer_revenue
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
      const metrics = calculatePerformanceMetrics({
        visits: row.visits,
        orders: row.orders,
        attributedRevenue: row.revenue,
        spend: row.spend,
        clicks: row.clicks,
        impressions: row.impressions,
        newCustomerOrders: row.new_customer_orders,
        returningCustomerOrders: row.returning_customer_orders,
        newCustomerRevenue: row.new_customer_revenue,
        returningCustomerRevenue: row.returning_customer_revenue
      });

      return {
        date: row.metric_date,
        ...metrics
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
          SUM(impressions)::bigint AS impressions,
          SUM(new_customer_orders)::numeric(12, 8) AS new_customer_orders,
          SUM(returning_customer_orders)::numeric(12, 8) AS returning_customer_orders,
          SUM(new_customer_revenue)::numeric(12, 2) AS new_customer_revenue,
          SUM(returning_customer_revenue)::numeric(12, 2) AS returning_customer_revenue
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
        impressions::text AS impressions,
        new_customer_orders::text AS new_customer_orders,
        returning_customer_orders::text AS returning_customer_orders,
        new_customer_revenue::text AS new_customer_revenue,
        returning_customer_revenue::text AS returning_customer_revenue
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
    const metrics = calculatePerformanceMetrics({
      visits: row.visits,
      orders: row.orders,
      attributedRevenue: row.revenue,
      spend: row.spend,
      clicks: row.clicks,
      impressions: row.impressions,
      newCustomerOrders: row.new_customer_orders,
      returningCustomerOrders: row.returning_customer_orders,
      newCustomerRevenue: row.new_customer_revenue,
      returningCustomerRevenue: row.returning_customer_revenue
    });

    return {
      source: row.source,
      medium: row.medium,
      ...metrics,
      shareOfRevenue: safeDivide(metrics.revenue, totalRevenue) ?? 0
    };
  });
}

async function fetchCampaigns(
  filters: ReportingFilters,
  limit: number,
  cursor: CursorValue | null
): Promise<{ rows: CampaignRow[]; nextCursor: string | null }> {
  const params = [
    ...baseMetricParams(filters),
    cursor?.revenue ?? null,
    cursor?.source ?? null,
    cursor?.medium ?? null,
    cursor?.campaign ?? null,
    limit + 1
  ];

  const result = await query<CampaignRow>(
    `
      WITH grouped AS (
        SELECT
          source,
          medium,
          campaign,
          SUM(visits)::bigint AS visits,
          SUM(attributed_orders)::numeric(12, 8) AS orders,
          SUM(attributed_revenue)::numeric(12, 2) AS revenue,
          SUM(spend)::numeric(12, 2) AS spend,
          SUM(clicks)::bigint AS clicks,
          SUM(impressions)::bigint AS impressions,
          SUM(new_customer_orders)::numeric(12, 8) AS new_customer_orders,
          SUM(returning_customer_orders)::numeric(12, 8) AS returning_customer_orders,
          SUM(new_customer_revenue)::numeric(12, 2) AS new_customer_revenue,
          SUM(returning_customer_revenue)::numeric(12, 2) AS returning_customer_revenue
        FROM daily_reporting_metrics
        WHERE ${baseMetricWhereClause()}
        GROUP BY source, medium, campaign
      )
      SELECT
        source,
        medium,
        campaign,
        visits::text AS visits,
        orders::text AS orders,
        revenue::text AS revenue,
        spend::text AS spend,
        clicks::text AS clicks,
        impressions::text AS impressions,
        new_customer_orders::text AS new_customer_orders,
        returning_customer_orders::text AS returning_customer_orders,
        new_customer_revenue::text AS new_customer_revenue,
        returning_customer_revenue::text AS returning_customer_revenue
      FROM grouped
      WHERE (
        $9::numeric IS NULL
        OR revenue < $9::numeric
        OR (
          revenue = $9::numeric
          AND (source, medium, campaign) > ($10::text, $11::text, $12::text)
        )
      )
      ORDER BY revenue DESC, source ASC, medium ASC, campaign ASC
      LIMIT $13
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
        campaign: nextRow.campaign
      })
    : null;

  return {
    rows: pageRows,
    nextCursor
  };
}

function formatCampaignRows(rows: CampaignRow[]) {
  return rows.map((row) => {
    const metrics = calculatePerformanceMetrics({
      visits: row.visits,
      orders: row.orders,
      attributedRevenue: row.revenue,
      spend: row.spend,
      clicks: row.clicks,
      impressions: row.impressions,
      newCustomerOrders: row.new_customer_orders,
      returningCustomerOrders: row.returning_customer_orders,
      newCustomerRevenue: row.new_customer_revenue,
      returningCustomerRevenue: row.returning_customer_revenue
    });

    return {
      source: row.source,
      medium: row.medium,
      campaign: row.campaign,
      ...metrics
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
    cursor?.creativeId ?? null,
    cursor?.creativeName ?? null,
    limit + 1
  ];

  const result = await query<CreativeRow>(
    `
      WITH attributed_groups AS (
        SELECT
          metric_date,
          source,
          medium,
          campaign,
          content,
          SUM(visits)::numeric(12, 4) AS visits,
          SUM(attributed_orders)::numeric(12, 8) AS orders,
          SUM(attributed_revenue)::numeric(12, 2) AS revenue
        FROM daily_reporting_metrics
        WHERE ${baseMetricWhereClause()}
        GROUP BY metric_date, source, medium, campaign, content
      ),
      spend_creatives AS (
        SELECT
          report_date AS metric_date,
          canonical_source AS source,
          canonical_medium AS medium,
          canonical_campaign AS campaign,
          canonical_content AS content,
          campaign_id,
          campaign_name,
          ad_id,
          ad_name,
          creative_id,
          creative_name,
          spend::numeric(12, 2) AS spend,
          clicks::numeric(12, 4) AS clicks,
          impressions::numeric(12, 4) AS impressions
        FROM meta_ads_daily_spend
        WHERE report_date BETWEEN $1::date AND $2::date
          AND granularity = 'creative'
          AND ($4::text IS NULL OR canonical_source = $4)
          AND ($5::text IS NULL OR canonical_medium = $5)
          AND ($6::text IS NULL OR canonical_campaign = $6)
          AND ($7::text IS NULL OR canonical_content = $7)
          AND (
            $8::text IS NULL
            OR canonical_source ILIKE $8
            OR canonical_medium ILIKE $8
            OR canonical_campaign ILIKE $8
            OR canonical_content ILIKE $8
            OR campaign_name ILIKE $8
            OR ad_name ILIKE $8
            OR creative_name ILIKE $8
            OR creative_id ILIKE $8
          )

        UNION ALL

        SELECT
          report_date AS metric_date,
          canonical_source AS source,
          canonical_medium AS medium,
          canonical_campaign AS campaign,
          canonical_content AS content,
          campaign_id,
          campaign_name,
          ad_id,
          ad_name,
          creative_id,
          creative_name,
          spend::numeric(12, 2) AS spend,
          clicks::numeric(12, 4) AS clicks,
          impressions::numeric(12, 4) AS impressions
        FROM google_ads_daily_spend
        WHERE report_date BETWEEN $1::date AND $2::date
          AND granularity = 'creative'
          AND ($4::text IS NULL OR canonical_source = $4)
          AND ($5::text IS NULL OR canonical_medium = $5)
          AND ($6::text IS NULL OR canonical_campaign = $6)
          AND ($7::text IS NULL OR canonical_content = $7)
          AND (
            $8::text IS NULL
            OR canonical_source ILIKE $8
            OR canonical_medium ILIKE $8
            OR canonical_campaign ILIKE $8
            OR canonical_content ILIKE $8
            OR campaign_name ILIKE $8
            OR ad_name ILIKE $8
            OR creative_name ILIKE $8
            OR creative_id ILIKE $8
          )
      ),
      spend_creative_totals AS (
        SELECT
          metric_date,
          source,
          medium,
          campaign,
          content,
          COUNT(*)::numeric(12, 4) AS creative_count,
          COALESCE(SUM(spend), 0)::numeric(12, 4) AS spend_total,
          COALESCE(SUM(clicks), 0)::numeric(12, 4) AS clicks_total,
          COALESCE(SUM(impressions), 0)::numeric(12, 4) AS impressions_total
        FROM spend_creatives
        GROUP BY metric_date, source, medium, campaign, content
      ),
      spend_backed_rows AS (
        SELECT
          s.source,
          s.medium,
          s.campaign,
          s.campaign_id,
          s.campaign_name,
          s.ad_id,
          s.ad_name,
          s.creative_id,
          s.creative_name,
          s.content,
          SUM(
            COALESCE(a.visits, 0)
            * CASE
                WHEN t.spend_total > 0 THEN s.spend / t.spend_total
                WHEN t.clicks_total > 0 THEN s.clicks / t.clicks_total
                WHEN t.impressions_total > 0 THEN s.impressions / t.impressions_total
                WHEN t.creative_count > 0 THEN 1 / t.creative_count
                ELSE 0
              END
          )::numeric(12, 4) AS visits,
          SUM(
            COALESCE(a.orders, 0)
            * CASE
                WHEN t.spend_total > 0 THEN s.spend / t.spend_total
                WHEN t.clicks_total > 0 THEN s.clicks / t.clicks_total
                WHEN t.impressions_total > 0 THEN s.impressions / t.impressions_total
                WHEN t.creative_count > 0 THEN 1 / t.creative_count
                ELSE 0
              END
          )::numeric(12, 8) AS orders,
          SUM(
            COALESCE(a.revenue, 0)
            * CASE
                WHEN t.spend_total > 0 THEN s.spend / t.spend_total
                WHEN t.clicks_total > 0 THEN s.clicks / t.clicks_total
                WHEN t.impressions_total > 0 THEN s.impressions / t.impressions_total
                WHEN t.creative_count > 0 THEN 1 / t.creative_count
                ELSE 0
              END
          )::numeric(12, 2) AS revenue,
          COALESCE(SUM(s.spend), 0)::numeric(12, 2) AS spend,
          COALESCE(SUM(s.clicks), 0)::numeric(12, 4) AS clicks,
          COALESCE(SUM(s.impressions), 0)::numeric(12, 4) AS impressions
        FROM spend_creatives s
        LEFT JOIN attributed_groups a
          ON a.metric_date = s.metric_date
         AND a.source = s.source
         AND a.medium = s.medium
         AND a.campaign = s.campaign
         AND a.content = s.content
        INNER JOIN spend_creative_totals t
          ON t.metric_date = s.metric_date
         AND t.source = s.source
         AND t.medium = s.medium
         AND t.campaign = s.campaign
         AND t.content = s.content
        GROUP BY
          s.source,
          s.medium,
          s.campaign,
          s.campaign_id,
          s.campaign_name,
          s.ad_id,
          s.ad_name,
          s.creative_id,
          s.creative_name,
          s.content
      ),
      unattributed_dimension_rows AS (
        SELECT
          a.source,
          a.medium,
          a.campaign,
          NULL::text AS campaign_id,
          a.campaign AS campaign_name,
          NULL::text AS ad_id,
          NULL::text AS ad_name,
          NULL::text AS creative_id,
          NULLIF(a.content, 'unknown') AS creative_name,
          a.content,
          SUM(a.visits)::numeric(12, 4) AS visits,
          SUM(a.orders)::numeric(12, 8) AS orders,
          SUM(a.revenue)::numeric(12, 2) AS revenue,
          0::numeric(12, 2) AS spend,
          0::numeric(12, 4) AS clicks,
          0::numeric(12, 4) AS impressions
        FROM attributed_groups a
        LEFT JOIN spend_creative_totals t
          ON t.metric_date = a.metric_date
         AND t.source = a.source
         AND t.medium = a.medium
         AND t.campaign = a.campaign
         AND t.content = a.content
        WHERE t.metric_date IS NULL
        GROUP BY a.source, a.medium, a.campaign, a.content
      ),
      grouped AS (
        SELECT
          source,
          medium,
          campaign,
          campaign_id,
          campaign_name,
          ad_id,
          ad_name,
          creative_id,
          creative_name,
          content,
          SUM(visits)::numeric(12, 4) AS visits,
          SUM(orders)::numeric(12, 8) AS orders,
          SUM(revenue)::numeric(12, 2) AS revenue,
          SUM(spend)::numeric(12, 2) AS spend,
          SUM(clicks)::numeric(12, 4) AS clicks,
          SUM(impressions)::numeric(12, 4) AS impressions,
          SUM(orders)::numeric(12, 8) AS new_customer_orders,
          0::numeric(12, 8) AS returning_customer_orders,
          SUM(revenue)::numeric(12, 2) AS new_customer_revenue,
          0::numeric(12, 2) AS returning_customer_revenue
        FROM (
          SELECT * FROM spend_backed_rows
          UNION ALL
          SELECT * FROM unattributed_dimension_rows
        ) combined
        GROUP BY source, medium, campaign, campaign_id, campaign_name, ad_id, ad_name, creative_id, creative_name, content
      )
      SELECT
        source,
        medium,
        campaign,
        campaign_id,
        campaign_name,
        ad_id,
        ad_name,
        creative_id,
        creative_name,
        content,
        visits::text AS visits,
        orders::text AS orders,
        revenue::text AS revenue,
        spend::text AS spend,
        clicks::text AS clicks,
        impressions::text AS impressions,
        new_customer_orders::text AS new_customer_orders,
        returning_customer_orders::text AS returning_customer_orders,
        new_customer_revenue::text AS new_customer_revenue,
        returning_customer_revenue::text AS returning_customer_revenue
      FROM grouped
      WHERE (
        $9::numeric IS NULL
        OR revenue < $9::numeric
        OR (
          revenue = $9::numeric
          AND (source, medium, campaign, content, COALESCE(creative_id, ''), COALESCE(creative_name, ''))
            > ($10::text, $11::text, $12::text, $13::text, $14::text, $15::text)
        )
      )
      ORDER BY revenue DESC, source ASC, medium ASC, campaign ASC, content ASC, creative_id ASC NULLS FIRST, creative_name ASC NULLS FIRST
      LIMIT $16
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
        content: nextRow.content,
        creativeId: nextRow.creative_id ?? '',
        creativeName: nextRow.creative_name ?? ''
      })
    : null;

  return {
    rows: pageRows,
    nextCursor
  };
}

function formatCreativeRows(rows: CreativeRow[]) {
  return rows.map((row) => {
    const metrics = calculatePerformanceMetrics({
      visits: row.visits,
      orders: row.orders,
      attributedRevenue: row.revenue,
      spend: row.spend,
      clicks: row.clicks,
      impressions: row.impressions,
      newCustomerOrders: row.new_customer_orders,
      returningCustomerOrders: row.returning_customer_orders,
      newCustomerRevenue: row.new_customer_revenue,
      returningCustomerRevenue: row.returning_customer_revenue
    });

    return {
      source: row.source,
      medium: row.medium,
      campaign: row.campaign,
      campaignId: row.campaign_id,
      campaignName: row.campaign_name,
      adId: row.ad_id,
      adName: row.ad_name,
      creativeId: row.creative_id,
      creativeName: row.creative_name ?? row.content,
      content: row.content,
      ...metrics,
      costPerClick: safeDivide(metrics.spend, metrics.clicks)
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
      attributedRevenue: z.number(),
      revenue: z.number(),
      spend: z.number(),
      clicks: z.number(),
      impressions: z.number(),
      conversionRate: z.number(),
      roas: z.number().nullable(),
      cac: z.number().nullable(),
      blendedCac: z.number().nullable(),
      averageOrderValue: z.number().nullable(),
      clickThroughRate: z.number().nullable(),
      newCustomerOrders: z.number(),
      returningCustomerOrders: z.number(),
      newCustomerRevenue: z.number(),
      returningCustomerRevenue: z.number(),
      newCustomerRate: z.number(),
      returningCustomerRate: z.number()
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
        attributedRevenue: z.number(),
        revenue: z.number(),
        spend: z.number(),
        clicks: z.number(),
        impressions: z.number(),
        conversionRate: z.number(),
        roas: z.number().nullable(),
        cac: z.number().nullable(),
        blendedCac: z.number().nullable(),
        averageOrderValue: z.number().nullable(),
        clickThroughRate: z.number().nullable(),
        newCustomerOrders: z.number(),
        returningCustomerOrders: z.number(),
        newCustomerRevenue: z.number(),
        returningCustomerRevenue: z.number(),
        newCustomerRate: z.number(),
        returningCustomerRate: z.number()
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
      const cursor = decodeCursor(
        pagination.cursor,
        rowValueSchema.pick({ revenue: true, source: true, medium: true, campaign: true })
      );
      const page = await fetchCampaigns(filters, pagination.limit, cursor);
      const response = paginatedResponseSchema.parse(
        createEnvelope(principal, filters, {
          rows: formatCampaignRows(page.rows),
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
