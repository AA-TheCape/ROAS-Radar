import type { PoolClient } from 'pg';

import { ATTRIBUTION_MODELS } from '../attribution/engine.js';
import { getReportingTimezone } from '../settings/index.js';

function normalizeMetricDates(metricDates: string[]): string[] {
  return [...new Set(metricDates.map((value) => value.trim()).filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value)))].sort();
}

export async function refreshDailyReportingMetrics(client: PoolClient, metricDates: string[]): Promise<void> {
  const normalizedMetricDates = normalizeMetricDates(metricDates);

  if (normalizedMetricDates.length === 0) {
    return;
  }

  const reportingTimezone = await getReportingTimezone(client);

  await client.query('SELECT pg_advisory_xact_lock($1)', [82134721]);
  await client.query('DELETE FROM daily_reporting_metrics WHERE metric_date = ANY($1::date[])', [normalizedMetricDates]);

  await client.query(
    `
      WITH attribution_models AS (
        SELECT unnest($3::text[]) AS attribution_model
      ),
      visit_rows AS (
        SELECT
          DATE(timezone($2::text, s.first_seen_at)) AS metric_date,
          m.attribution_model,
          COALESCE(s.initial_utm_source, 'unknown') AS source,
          COALESCE(s.initial_utm_medium, 'unknown') AS medium,
          COALESCE(s.initial_utm_campaign, 'unknown') AS campaign,
          COALESCE(s.initial_utm_content, 'unknown') AS content,
          COALESCE(s.initial_utm_term, 'unknown') AS term,
          COUNT(*)::int AS visits,
          0::numeric(12, 8) AS attributed_orders,
          0::numeric(12, 2) AS attributed_revenue,
          0::numeric(12, 2) AS spend,
          0::bigint AS impressions,
          0::bigint AS clicks,
          0::numeric(12, 8) AS new_customer_orders,
          0::numeric(12, 8) AS returning_customer_orders,
          0::numeric(12, 2) AS new_customer_revenue,
          0::numeric(12, 2) AS returning_customer_revenue
        FROM tracking_sessions s
        CROSS JOIN attribution_models m
        WHERE DATE(timezone($2::text, s.first_seen_at)) = ANY($1::date[])
        GROUP BY 1, 2, 3, 4, 5, 6, 7
      ),
      order_customer_rankings AS (
        SELECT
          o.shopify_order_id,
          ROW_NUMBER() OVER (
            PARTITION BY COALESCE(
              NULLIF(o.customer_identity_id::text, ''),
              NULLIF(lower(trim(o.shopify_customer_id)), ''),
              NULLIF(trim(o.email_hash), ''),
              'guest:' || o.shopify_order_id
            )
            ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) ASC, o.shopify_order_id ASC
          ) AS customer_order_rank
        FROM shopify_orders o
      ),
      attributed_order_rows AS (
        SELECT
          DATE(timezone($2::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at))) AS metric_date,
          c.attribution_model,
          COALESCE(c.attributed_source, 'unknown') AS source,
          COALESCE(c.attributed_medium, 'unknown') AS medium,
          COALESCE(c.attributed_campaign, 'unknown') AS campaign,
          COALESCE(c.attributed_content, 'unknown') AS content,
          COALESCE(c.attributed_term, 'unknown') AS term,
          0::int AS visits,
          COALESCE(SUM(c.credit_weight), 0)::numeric(12, 8) AS attributed_orders,
          COALESCE(SUM(c.revenue_credit), 0)::numeric(12, 2) AS attributed_revenue,
          0::numeric(12, 2) AS spend,
          0::bigint AS impressions,
          0::bigint AS clicks,
          COALESCE(SUM(CASE WHEN r.customer_order_rank = 1 THEN c.credit_weight ELSE 0 END), 0)::numeric(12, 8) AS new_customer_orders,
          COALESCE(SUM(CASE WHEN r.customer_order_rank > 1 THEN c.credit_weight ELSE 0 END), 0)::numeric(12, 8) AS returning_customer_orders,
          COALESCE(SUM(CASE WHEN r.customer_order_rank = 1 THEN c.revenue_credit ELSE 0 END), 0)::numeric(12, 2) AS new_customer_revenue,
          COALESCE(SUM(CASE WHEN r.customer_order_rank > 1 THEN c.revenue_credit ELSE 0 END), 0)::numeric(12, 2) AS returning_customer_revenue
        FROM attribution_order_credits c
        INNER JOIN shopify_orders o
          ON o.shopify_order_id = c.shopify_order_id
        INNER JOIN order_customer_rankings r
          ON r.shopify_order_id = o.shopify_order_id
        WHERE DATE(timezone($2::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at))) = ANY($1::date[])
        GROUP BY 1, 2, 3, 4, 5, 6, 7
      ),
      spend_source_rows AS (
        SELECT
          report_date AS metric_date,
          canonical_source AS source,
          canonical_medium AS medium,
          canonical_campaign AS campaign,
          canonical_content AS content,
          canonical_term AS term,
          spend,
          impressions,
          clicks
        FROM meta_ads_daily_spend
        WHERE report_date = ANY($1::date[])
          AND granularity = 'creative'

        UNION ALL

        SELECT
          report_date AS metric_date,
          canonical_source AS source,
          canonical_medium AS medium,
          canonical_campaign AS campaign,
          canonical_content AS content,
          canonical_term AS term,
          spend,
          impressions,
          clicks
        FROM google_ads_daily_spend
        WHERE report_date = ANY($1::date[])
          AND granularity = 'creative'

        UNION ALL

        SELECT
          campaign_row.report_date AS metric_date,
          campaign_row.canonical_source AS source,
          campaign_row.canonical_medium AS medium,
          campaign_row.canonical_campaign AS campaign,
          campaign_row.canonical_content AS content,
          campaign_row.canonical_term AS term,
          campaign_row.spend,
          campaign_row.impressions,
          campaign_row.clicks
        FROM google_ads_daily_spend campaign_row
        WHERE campaign_row.report_date = ANY($1::date[])
          AND campaign_row.granularity = 'campaign'
          AND NOT EXISTS (
            SELECT 1
            FROM google_ads_daily_spend creative_row
            WHERE creative_row.report_date = campaign_row.report_date
              AND creative_row.granularity = 'creative'
              AND creative_row.connection_id = campaign_row.connection_id
              AND creative_row.campaign_id IS NOT DISTINCT FROM campaign_row.campaign_id
          )
      ),
      spend_rows AS (
        SELECT
          s.metric_date,
          m.attribution_model,
          COALESCE(s.source, 'unknown') AS source,
          COALESCE(s.medium, 'unknown') AS medium,
          COALESCE(s.campaign, 'unknown') AS campaign,
          COALESCE(s.content, 'unknown') AS content,
          COALESCE(s.term, 'unknown') AS term,
          0::int AS visits,
          0::numeric(12, 8) AS attributed_orders,
          0::numeric(12, 2) AS attributed_revenue,
          COALESCE(SUM(s.spend), 0)::numeric(12, 2) AS spend,
          COALESCE(SUM(s.impressions), 0)::bigint AS impressions,
          COALESCE(SUM(s.clicks), 0)::bigint AS clicks,
          0::numeric(12, 8) AS new_customer_orders,
          0::numeric(12, 8) AS returning_customer_orders,
          0::numeric(12, 2) AS new_customer_revenue,
          0::numeric(12, 2) AS returning_customer_revenue
        FROM spend_source_rows s
        CROSS JOIN attribution_models m
        GROUP BY 1, 2, 3, 4, 5, 6, 7
      )
      INSERT INTO daily_reporting_metrics (
        metric_date,
        attribution_model,
        source,
        medium,
        campaign,
        content,
        term,
        visits,
        attributed_orders,
        attributed_revenue,
        spend,
        impressions,
        clicks,
        new_customer_orders,
        returning_customer_orders,
        new_customer_revenue,
        returning_customer_revenue,
        last_computed_at
      )
      SELECT
        metric_date,
        attribution_model,
        source,
        medium,
        campaign,
        content,
        term,
        SUM(visits)::int AS visits,
        SUM(attributed_orders)::numeric(12, 8) AS attributed_orders,
        SUM(attributed_revenue)::numeric(12, 2) AS attributed_revenue,
        SUM(spend)::numeric(12, 2) AS spend,
        SUM(impressions)::bigint AS impressions,
        SUM(clicks)::bigint AS clicks,
        SUM(new_customer_orders)::numeric(12, 8) AS new_customer_orders,
        SUM(returning_customer_orders)::numeric(12, 8) AS returning_customer_orders,
        SUM(new_customer_revenue)::numeric(12, 2) AS new_customer_revenue,
        SUM(returning_customer_revenue)::numeric(12, 2) AS returning_customer_revenue,
        now()
      FROM (
        SELECT * FROM visit_rows
        UNION ALL
        SELECT * FROM attributed_order_rows
        UNION ALL
        SELECT * FROM spend_rows
      ) combined
      GROUP BY 1, 2, 3, 4, 5, 6, 7
    `,
    [normalizedMetricDates, reportingTimezone, ATTRIBUTION_MODELS]
  );
}

export async function refreshAllDailyReportingMetrics(client: PoolClient): Promise<void> {
  const reportingTimezone = await getReportingTimezone(client);
  const result = await client.query<{ metric_date: string }>(
    `
      SELECT DISTINCT metric_date::text
      FROM (
        SELECT DATE(timezone($1::text, first_seen_at)) AS metric_date
        FROM tracking_sessions

        UNION

        SELECT DATE(timezone($1::text, COALESCE(processed_at, created_at_shopify, ingested_at))) AS metric_date
        FROM shopify_orders

        UNION

        SELECT report_date AS metric_date
        FROM meta_ads_daily_spend

        UNION

        SELECT report_date AS metric_date
        FROM google_ads_daily_spend
      ) metric_dates
      WHERE metric_date IS NOT NULL
      ORDER BY metric_date ASC
    `,
    [reportingTimezone]
  );

  await refreshDailyReportingMetrics(
    client,
    result.rows.map((row) => row.metric_date)
  );
}
