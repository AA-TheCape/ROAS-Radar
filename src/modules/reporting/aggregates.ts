import type { PoolClient } from "pg";

import { ATTRIBUTION_MODELS } from "../attribution/engine.js";
import { backfillMmmCampaignMetadata } from "../campaign-resolver/index.js";
import { getReportingTimezone } from "../settings/index.js";

function normalizeMetricDates(metricDates: string[]): string[] {
	return [
		...new Set(
			metricDates
				.map((value) => value.trim())
				.filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value)),
		),
	].sort();
}

export async function refreshDailyReportingMetrics(
	client: PoolClient,
	metricDates: string[],
): Promise<void> {
	const normalizedMetricDates = normalizeMetricDates(metricDates);

	if (normalizedMetricDates.length === 0) {
		return;
	}

	const reportingTimezone = await getReportingTimezone(client);

	await client.query("SELECT pg_advisory_xact_lock($1)", [82134721]);
	await client.query(
		"DELETE FROM daily_reporting_metrics WHERE metric_date = ANY($1::date[])",
		[normalizedMetricDates],
	);

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
        WHERE COALESCE(o.source_name, '') = 'web'
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
        WHERE COALESCE(o.source_name, '') = 'web'
          AND DATE(timezone($2::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at))) = ANY($1::date[])
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
		[normalizedMetricDates, reportingTimezone, ATTRIBUTION_MODELS],
	);

	await refreshReportingModelComparisons(client, normalizedMetricDates);
}

export async function refreshReportingModelComparisons(
	client: PoolClient,
	metricDates: string[],
): Promise<void> {
	const normalizedMetricDates = normalizeMetricDates(metricDates);

	if (normalizedMetricDates.length === 0) {
		return;
	}

	const reportingTimezone = await getReportingTimezone(client);

	await client.query("SELECT pg_advisory_xact_lock($1)", [82134723]);
	await client.query(
		"DELETE FROM reporting_model_comparison_daily WHERE metric_date = ANY($1::date[])",
		[normalizedMetricDates],
	);

	await client.query(
		`
      WITH reporting_views AS (
        SELECT *
        FROM (
          VALUES
            ('strict_deterministic'::text, ARRAY['deterministic_first_party']::text[]),
            ('fallback_included'::text, ARRAY[
              'deterministic_first_party',
              'deterministic_shopify_hint',
              'ga4_fallback'
            ]::text[]),
            ('blended_deterministic'::text, ARRAY[
              'deterministic_first_party',
              'deterministic_shopify_hint',
              'ga4_fallback'
            ]::text[])
        ) AS view_definitions(reporting_view, included_tiers)
      ),
      metric_context AS (
        SELECT
          metric_date,
          attribution_model,
          source,
          medium,
          campaign,
          COALESCE(SUM(visits), 0)::int AS visits,
          COALESCE(SUM(spend), 0)::numeric(12, 2) AS spend
        FROM daily_reporting_metrics
        WHERE metric_date = ANY($1::date[])
        GROUP BY 1, 2, 3, 4, 5
      ),
      attributed_credit_rows AS (
        SELECT
          DATE(timezone($2::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at))) AS metric_date,
          c.attribution_model,
          COALESCE(c.attributed_source, 'unknown') AS source,
          COALESCE(c.attributed_medium, 'unknown') AS medium,
          COALESCE(c.attributed_campaign, 'unknown') AS campaign,
          COALESCE(o.attribution_tier, 'unattributed') AS attribution_tier,
          COALESCE(SUM(c.credit_weight), 0)::numeric(12, 8) AS attributed_orders,
          COALESCE(SUM(c.revenue_credit), 0)::numeric(12, 2) AS attributed_revenue
        FROM attribution_order_credits c
        INNER JOIN shopify_orders o
          ON o.shopify_order_id = c.shopify_order_id
        WHERE COALESCE(o.source_name, '') = 'web'
          AND DATE(timezone($2::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at))) = ANY($1::date[])
        GROUP BY 1, 2, 3, 4, 5, 6
      ),
      rollup_rows AS (
        SELECT
          metric_date,
          attribution_model,
          source,
          medium,
          campaign,
          COALESCE(SUM(CASE WHEN attribution_tier = 'deterministic_first_party' THEN attributed_orders ELSE 0 END), 0)::numeric(12, 8) AS strict_deterministic_orders,
          COALESCE(SUM(CASE WHEN attribution_tier IN ('deterministic_first_party', 'deterministic_shopify_hint', 'ga4_fallback') THEN attributed_orders ELSE 0 END), 0)::numeric(12, 8) AS fallback_included_orders,
          COALESCE(SUM(CASE WHEN attribution_tier IN ('deterministic_first_party', 'deterministic_shopify_hint', 'ga4_fallback') THEN attributed_orders ELSE 0 END), 0)::numeric(12, 8) AS blended_deterministic_orders
        FROM attributed_credit_rows
        GROUP BY 1, 2, 3, 4, 5
      ),
      comparison_rows AS (
        SELECT
          credits.metric_date,
          credits.attribution_model,
          view_definitions.reporting_view,
          credits.source,
          credits.medium,
          credits.campaign,
          COALESCE(SUM(credits.attributed_orders), 0)::numeric(12, 8) AS attributed_orders,
          COALESCE(SUM(credits.attributed_revenue), 0)::numeric(12, 2) AS attributed_revenue
        FROM attributed_credit_rows credits
        INNER JOIN reporting_views view_definitions
          ON credits.attribution_tier = ANY(view_definitions.included_tiers)
        GROUP BY 1, 2, 3, 4, 5, 6
      ),
      all_dimensions AS (
        SELECT metric_date, attribution_model, source, medium, campaign
        FROM metric_context

        UNION

        SELECT metric_date, attribution_model, source, medium, campaign
        FROM comparison_rows
      )
      INSERT INTO reporting_model_comparison_daily (
        metric_date,
        attribution_model,
        reporting_view,
        source,
        medium,
        campaign,
        visits,
        attributed_orders,
        attributed_revenue,
        spend,
        strict_deterministic_orders,
        fallback_included_orders,
        blended_deterministic_orders,
        last_computed_at
      )
      SELECT
        dimensions.metric_date,
        dimensions.attribution_model,
        view_definitions.reporting_view,
        dimensions.source,
        dimensions.medium,
        dimensions.campaign,
        COALESCE(context.visits, 0)::int AS visits,
        COALESCE(comparison.attributed_orders, 0)::numeric(12, 8) AS attributed_orders,
        COALESCE(comparison.attributed_revenue, 0)::numeric(12, 2) AS attributed_revenue,
        COALESCE(context.spend, 0)::numeric(12, 2) AS spend,
        COALESCE(rollups.strict_deterministic_orders, 0)::numeric(12, 8) AS strict_deterministic_orders,
        COALESCE(rollups.fallback_included_orders, 0)::numeric(12, 8) AS fallback_included_orders,
        COALESCE(rollups.blended_deterministic_orders, 0)::numeric(12, 8) AS blended_deterministic_orders,
        now()
      FROM all_dimensions dimensions
      CROSS JOIN reporting_views view_definitions
      LEFT JOIN metric_context context
        ON context.metric_date = dimensions.metric_date
        AND context.attribution_model = dimensions.attribution_model
        AND context.source = dimensions.source
        AND context.medium = dimensions.medium
        AND context.campaign = dimensions.campaign
      LEFT JOIN comparison_rows comparison
        ON comparison.metric_date = dimensions.metric_date
        AND comparison.attribution_model = dimensions.attribution_model
        AND comparison.reporting_view = view_definitions.reporting_view
        AND comparison.source = dimensions.source
        AND comparison.medium = dimensions.medium
        AND comparison.campaign = dimensions.campaign
      LEFT JOIN rollup_rows rollups
        ON rollups.metric_date = dimensions.metric_date
        AND rollups.attribution_model = dimensions.attribution_model
        AND rollups.source = dimensions.source
        AND rollups.medium = dimensions.medium
        AND rollups.campaign = dimensions.campaign
    `,
		[normalizedMetricDates, reportingTimezone],
	);
}

export async function refreshAllDailyReportingMetrics(
	client: PoolClient,
): Promise<void> {
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
		[reportingTimezone],
	);

	await refreshDailyReportingMetrics(
		client,
		result.rows.map((row) => row.metric_date),
	);
}

export async function refreshDailyMmmInputMart(
	client: PoolClient,
	metricDates: string[],
): Promise<void> {
	const normalizedMetricDates = normalizeMetricDates(metricDates);

	if (normalizedMetricDates.length === 0) {
		return;
	}

	const reportingTimezone = await getReportingTimezone(client);

	await client.query("SELECT pg_advisory_xact_lock($1)", [82134722]);
	await client.query(
		"DELETE FROM mmm_daily_input_mart_v1 WHERE metric_date = ANY($1::date[])",
		[normalizedMetricDates],
	);

	await client.query(
		`
      WITH spend_source_rows AS (
        SELECT
          'meta'::text AS platform,
          connection_id AS platform_connection_id,
          report_date AS metric_date,
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
          canonical_source AS source,
          canonical_medium AS medium,
          canonical_campaign AS campaign,
          canonical_content AS content,
          canonical_term AS term,
          currency,
          spend,
          impressions,
          clicks,
          updated_at AS row_updated_at
        FROM meta_ads_daily_spend
        WHERE report_date = ANY($1::date[])
          AND granularity = 'creative'

        UNION ALL

        SELECT
          'google'::text AS platform,
          connection_id AS platform_connection_id,
          report_date AS metric_date,
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
          canonical_source AS source,
          canonical_medium AS medium,
          canonical_campaign AS campaign,
          canonical_content AS content,
          canonical_term AS term,
          currency,
          spend,
          impressions,
          clicks,
          updated_at AS row_updated_at
        FROM google_ads_daily_spend
        WHERE report_date = ANY($1::date[])
          AND granularity = 'creative'

        UNION ALL

        SELECT
          'google'::text AS platform,
          campaign_row.connection_id AS platform_connection_id,
          campaign_row.report_date AS metric_date,
          campaign_row.granularity,
          campaign_row.entity_key,
          campaign_row.account_id,
          campaign_row.account_name,
          campaign_row.campaign_id,
          campaign_row.campaign_name,
          campaign_row.adset_id,
          campaign_row.adset_name,
          campaign_row.ad_id,
          campaign_row.ad_name,
          campaign_row.creative_id,
          campaign_row.creative_name,
          campaign_row.canonical_source AS source,
          campaign_row.canonical_medium AS medium,
          campaign_row.canonical_campaign AS campaign,
          campaign_row.canonical_content AS content,
          campaign_row.canonical_term AS term,
          campaign_row.currency,
          campaign_row.spend,
          campaign_row.impressions,
          campaign_row.clicks,
          campaign_row.updated_at AS row_updated_at
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
      paid_media_rows AS (
        SELECT
          metric_date,
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
          COALESCE(source, 'unknown') AS source,
          COALESCE(medium, 'unknown') AS medium,
          COALESCE(campaign, 'unknown') AS campaign,
          COALESCE(content, 'unknown') AS content,
          COALESCE(term, 'unknown') AS term,
          currency,
          COALESCE(SUM(spend), 0)::numeric(12, 2) AS spend,
          COALESCE(SUM(impressions), 0)::bigint AS impressions,
          COALESCE(SUM(clicks), 0)::bigint AS clicks,
          MAX(row_updated_at) AS spend_last_synced_at
        FROM spend_source_rows
        GROUP BY
          metric_date,
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
          currency
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
        WHERE COALESCE(o.source_name, '') = 'web'
      ),
      attribution_base AS (
        SELECT
          DATE(timezone($2::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at))) AS metric_date,
          c.attribution_model,
          COALESCE(c.attributed_source, 'unknown') AS source,
          COALESCE(c.attributed_medium, 'unknown') AS medium,
          COALESCE(c.attributed_campaign, 'unknown') AS campaign,
          COALESCE(c.attributed_content, 'unknown') AS content,
          COALESCE(c.attributed_term, 'unknown') AS term,
          NULLIF(c.attributed_account_id, '') AS account_id,
          NULLIF(c.attributed_account_name, '') AS account_name,
          NULLIF(c.attributed_campaign_id, '') AS campaign_id,
          NULLIF(c.attributed_campaign, '') AS campaign_name,
          o.shopify_order_id,
          o.total_price,
          o.ingested_at,
          c.credit_weight,
          c.revenue_credit,
          c.match_source,
          c.confidence_label,
          c.created_at,
          r.customer_order_rank
        FROM attribution_order_credits c
        INNER JOIN shopify_orders o
          ON o.shopify_order_id = c.shopify_order_id
        INNER JOIN order_customer_rankings r
          ON r.shopify_order_id = o.shopify_order_id
        WHERE COALESCE(o.source_name, '') = 'web'
          AND DATE(timezone($2::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at))) = ANY($1::date[])
      ),
      attribution_order_metrics AS (
        SELECT
          metric_date,
          attribution_model,
          source,
          medium,
          campaign,
          content,
          term,
          account_id,
          account_name,
          campaign_id,
          campaign_name,
          COUNT(DISTINCT shopify_order_id)::bigint AS shopify_orders,
          COALESCE(SUM(total_price), 0)::numeric(12, 2) AS shopify_revenue
        FROM (
          SELECT DISTINCT
            metric_date,
            attribution_model,
            source,
            medium,
            campaign,
            content,
            term,
            account_id,
            account_name,
            campaign_id,
            campaign_name,
            shopify_order_id,
            total_price
          FROM attribution_base
          WHERE credit_weight > 0
        ) distinct_attributed_orders
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
      ),
      attribution_credit_metrics AS (
        SELECT
          metric_date,
          attribution_model,
          source,
          medium,
          campaign,
          content,
          term,
          account_id,
          account_name,
          campaign_id,
          campaign_name,
          COALESCE(SUM(credit_weight), 0)::numeric(12, 8) AS attribution_credit_orders,
          COALESCE(SUM(revenue_credit), 0)::numeric(12, 2) AS attribution_credit_revenue,
          COALESCE(SUM(CASE WHEN customer_order_rank = 1 THEN credit_weight ELSE 0 END), 0)::numeric(12, 8) AS new_customer_credit_orders,
          COALESCE(SUM(CASE WHEN customer_order_rank > 1 THEN credit_weight ELSE 0 END), 0)::numeric(12, 8) AS returning_customer_credit_orders,
          COALESCE(SUM(CASE WHEN customer_order_rank = 1 THEN revenue_credit ELSE 0 END), 0)::numeric(12, 2) AS new_customer_credit_revenue,
          COALESCE(SUM(CASE WHEN customer_order_rank > 1 THEN revenue_credit ELSE 0 END), 0)::numeric(12, 2) AS returning_customer_credit_revenue,
          MAX(ingested_at) AS shopify_last_ingested_at,
          MAX(created_at) AS attribution_last_computed_at
        FROM attribution_base
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
      ),
      attribution_metrics AS (
        SELECT
          credits.metric_date,
          credits.attribution_model,
          credits.source,
          credits.medium,
          credits.campaign,
          credits.content,
          credits.term,
          credits.account_id,
          credits.account_name,
          credits.campaign_id,
          credits.campaign_name,
          COALESCE(orders.shopify_orders, 0)::bigint AS shopify_orders,
          COALESCE(orders.shopify_revenue, 0)::numeric(12, 2) AS shopify_revenue,
          credits.attribution_credit_orders,
          credits.attribution_credit_revenue,
          credits.new_customer_credit_orders,
          credits.returning_customer_credit_orders,
          credits.new_customer_credit_revenue,
          credits.returning_customer_credit_revenue,
          credits.shopify_last_ingested_at,
          credits.attribution_last_computed_at
        FROM attribution_credit_metrics credits
        LEFT JOIN attribution_order_metrics orders
          ON orders.metric_date = credits.metric_date
         AND orders.attribution_model = credits.attribution_model
         AND orders.source = credits.source
         AND orders.medium = credits.medium
         AND orders.campaign = credits.campaign
         AND orders.content = credits.content
         AND orders.term = credits.term
         AND orders.account_id IS NOT DISTINCT FROM credits.account_id
         AND orders.account_name IS NOT DISTINCT FROM credits.account_name
         AND orders.campaign_id IS NOT DISTINCT FROM credits.campaign_id
         AND orders.campaign_name IS NOT DISTINCT FROM credits.campaign_name
      ),
      match_source_coverage AS (
        SELECT
          metric_date,
          attribution_model,
          source,
          medium,
          campaign,
          content,
          term,
          account_id,
          account_name,
          campaign_id,
          campaign_name,
          jsonb_object_agg(match_source, credited_orders ORDER BY match_source) AS coverage
        FROM (
          SELECT
            metric_date,
            attribution_model,
            source,
            medium,
            campaign,
            content,
            term,
            account_id,
            account_name,
            campaign_id,
            campaign_name,
            COALESCE(match_source, 'unknown') AS match_source,
            SUM(credit_weight)::numeric(12, 8) AS credited_orders
          FROM attribution_base
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        ) grouped_match_sources
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
      ),
      confidence_label_coverage AS (
        SELECT
          metric_date,
          attribution_model,
          source,
          medium,
          campaign,
          content,
          term,
          account_id,
          account_name,
          campaign_id,
          campaign_name,
          jsonb_object_agg(confidence_label, credited_orders ORDER BY confidence_label) AS coverage
        FROM (
          SELECT
            metric_date,
            attribution_model,
            source,
            medium,
            campaign,
            content,
            term,
            account_id,
            account_name,
            campaign_id,
            campaign_name,
            COALESCE(confidence_label, 'none') AS confidence_label,
            SUM(credit_weight)::numeric(12, 8) AS credited_orders
          FROM attribution_base
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        ) grouped_confidence_labels
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
      )
      INSERT INTO mmm_daily_input_mart_v1 (
        metric_date,
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
      )
      SELECT
        metric_date,
        'v1',
        'paid_media',
        'none',
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
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        '{}'::jsonb,
        '{}'::jsonb,
        spend_last_synced_at,
        NULL,
        NULL,
        now()
      FROM paid_media_rows

      UNION ALL

      SELECT
        metrics.metric_date,
        'v1',
        'attribution',
        metrics.attribution_model,
        CASE WHEN metrics.campaign_id IS NOT NULL THEN 'google_ads' ELSE 'taxonomy' END,
        NULL,
        CASE WHEN metrics.campaign_id IS NOT NULL THEN 'campaign' ELSE 'taxonomy' END,
        CASE
          WHEN metrics.campaign_id IS NOT NULL THEN concat_ws('|', 'google_ads', metrics.account_id, metrics.campaign_id)
          ELSE concat_ws('|', metrics.source, metrics.medium, metrics.campaign, metrics.content, metrics.term)
        END,
        metrics.account_id,
        metrics.account_name,
        metrics.campaign_id,
        metrics.campaign_name,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        metrics.source,
        metrics.medium,
        metrics.campaign,
        metrics.content,
        metrics.term,
        NULL,
        0,
        0,
        0,
        metrics.shopify_orders,
        metrics.shopify_revenue,
        metrics.attribution_credit_orders,
        metrics.attribution_credit_revenue,
        metrics.new_customer_credit_orders,
        metrics.returning_customer_credit_orders,
        metrics.new_customer_credit_revenue,
        metrics.returning_customer_credit_revenue,
        COALESCE(match_sources.coverage, '{}'::jsonb),
        COALESCE(confidence_labels.coverage, '{}'::jsonb),
        NULL,
        metrics.shopify_last_ingested_at,
        metrics.attribution_last_computed_at,
        now()
      FROM attribution_metrics metrics
      LEFT JOIN match_source_coverage match_sources
        ON match_sources.metric_date = metrics.metric_date
       AND match_sources.attribution_model = metrics.attribution_model
       AND match_sources.source = metrics.source
       AND match_sources.medium = metrics.medium
       AND match_sources.campaign = metrics.campaign
       AND match_sources.content = metrics.content
       AND match_sources.term = metrics.term
       AND match_sources.account_id IS NOT DISTINCT FROM metrics.account_id
       AND match_sources.account_name IS NOT DISTINCT FROM metrics.account_name
       AND match_sources.campaign_id IS NOT DISTINCT FROM metrics.campaign_id
       AND match_sources.campaign_name IS NOT DISTINCT FROM metrics.campaign_name
      LEFT JOIN confidence_label_coverage confidence_labels
        ON confidence_labels.metric_date = metrics.metric_date
       AND confidence_labels.attribution_model = metrics.attribution_model
       AND confidence_labels.source = metrics.source
       AND confidence_labels.medium = metrics.medium
       AND confidence_labels.campaign = metrics.campaign
       AND confidence_labels.content = metrics.content
       AND confidence_labels.term = metrics.term
       AND confidence_labels.account_id IS NOT DISTINCT FROM metrics.account_id
       AND confidence_labels.account_name IS NOT DISTINCT FROM metrics.account_name
       AND confidence_labels.campaign_id IS NOT DISTINCT FROM metrics.campaign_id
       AND confidence_labels.campaign_name IS NOT DISTINCT FROM metrics.campaign_name
	`,
		[normalizedMetricDates, reportingTimezone],
	);

	await backfillMmmCampaignMetadata(
		{
			startDate: normalizedMetricDates[0],
			endDate: normalizedMetricDates[normalizedMetricDates.length - 1],
		},
		client,
	);
}

export async function refreshAllDailyMmmInputMart(
	client: PoolClient,
): Promise<void> {
	const reportingTimezone = await getReportingTimezone(client);
	const result = await client.query<{ metric_date: string }>(
		`
      SELECT DISTINCT metric_date::text
      FROM (
        SELECT DATE(timezone($1::text, COALESCE(processed_at, created_at_shopify, ingested_at))) AS metric_date
        FROM shopify_orders
        WHERE COALESCE(source_name, '') = 'web'

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
		[reportingTimezone],
	);

	await refreshDailyMmmInputMart(
		client,
		result.rows.map((row) => row.metric_date),
	);
}
