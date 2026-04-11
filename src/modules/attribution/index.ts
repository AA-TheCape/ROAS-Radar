import { PoolClient } from 'pg';

import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';

type Candidate = {
  session_id: string | null;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  attributed_content: string | null;
  attributed_term: string | null;
  attributed_click_id_type: string | null;
  attributed_click_id_value: string | null;
  attribution_reason: string;
  confidence_score: string;
};

type PendingOrder = {
  shopify_order_id: string;
  created_at_shopify: Date | null;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
};

async function findCandidate(client: PoolClient, order: PendingOrder): Promise<Candidate | null> {
  if (order.landing_session_id) {
    const result = await client.query<Candidate>(
      `
        SELECT
          s.id AS session_id,
          s.initial_utm_source AS attributed_source,
          s.initial_utm_medium AS attributed_medium,
          s.initial_utm_campaign AS attributed_campaign,
          s.initial_utm_content AS attributed_content,
          s.initial_utm_term AS attributed_term,
          CASE
            WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
            WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
            WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
            WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
            ELSE NULL
          END AS attributed_click_id_type,
          COALESCE(s.initial_gclid, s.initial_fbclid, s.initial_ttclid, s.initial_msclkid) AS attributed_click_id_value,
          'matched_by_landing_session_id' AS attribution_reason,
          '1.00' AS confidence_score
        FROM tracking_sessions s
        WHERE s.id = $1::uuid
        LIMIT 1
      `,
      [order.landing_session_id]
    );

    if (result.rowCount) {
      return result.rows[0];
    }
  }

  if (order.checkout_token) {
    const result = await client.query<Candidate>(
      `
        SELECT
          e.session_id,
          s.initial_utm_source AS attributed_source,
          s.initial_utm_medium AS attributed_medium,
          s.initial_utm_campaign AS attributed_campaign,
          s.initial_utm_content AS attributed_content,
          s.initial_utm_term AS attributed_term,
          CASE
            WHEN e.gclid IS NOT NULL THEN 'gclid'
            WHEN e.fbclid IS NOT NULL THEN 'fbclid'
            WHEN e.ttclid IS NOT NULL THEN 'ttclid'
            WHEN e.msclkid IS NOT NULL THEN 'msclkid'
            ELSE NULL
          END AS attributed_click_id_type,
          COALESCE(e.gclid, e.fbclid, e.ttclid, e.msclkid) AS attributed_click_id_value,
          'matched_by_checkout_token' AS attribution_reason,
          '1.00' AS confidence_score
        FROM tracking_events e
        INNER JOIN tracking_sessions s ON s.id = e.session_id
        WHERE e.shopify_checkout_token = $1
          AND ($2::timestamptz IS NULL OR e.occurred_at >= $2::timestamptz - ($3::int * interval '1 day'))
          AND ($2::timestamptz IS NULL OR e.occurred_at <= $2::timestamptz)
        ORDER BY e.occurred_at DESC
        LIMIT 1
      `,
      [order.checkout_token, order.created_at_shopify, env.ATTRIBUTION_WINDOW_DAYS]
    );

    if (result.rowCount) {
      return result.rows[0];
    }
  }

  if (order.cart_token) {
    const result = await client.query<Candidate>(
      `
        SELECT
          e.session_id,
          s.initial_utm_source AS attributed_source,
          s.initial_utm_medium AS attributed_medium,
          s.initial_utm_campaign AS attributed_campaign,
          s.initial_utm_content AS attributed_content,
          s.initial_utm_term AS attributed_term,
          CASE
            WHEN e.gclid IS NOT NULL THEN 'gclid'
            WHEN e.fbclid IS NOT NULL THEN 'fbclid'
            WHEN e.ttclid IS NOT NULL THEN 'ttclid'
            WHEN e.msclkid IS NOT NULL THEN 'msclkid'
            ELSE NULL
          END AS attributed_click_id_type,
          COALESCE(e.gclid, e.fbclid, e.ttclid, e.msclkid) AS attributed_click_id_value,
          'matched_by_cart_token' AS attribution_reason,
          '0.90' AS confidence_score
        FROM tracking_events e
        INNER JOIN tracking_sessions s ON s.id = e.session_id
        WHERE e.shopify_cart_token = $1
          AND ($2::timestamptz IS NULL OR e.occurred_at >= $2::timestamptz - ($3::int * interval '1 day'))
          AND ($2::timestamptz IS NULL OR e.occurred_at <= $2::timestamptz)
        ORDER BY e.occurred_at DESC
        LIMIT 1
      `,
      [order.cart_token, order.created_at_shopify, env.ATTRIBUTION_WINDOW_DAYS]
    );

    if (result.rowCount) {
      return result.rows[0];
    }
  }

  return null;
}

async function refreshDailyCampaignMetrics(client: PoolClient, metricDates: string[]): Promise<void> {
  if (metricDates.length === 0) {
    return;
  }

  await client.query(
    'DELETE FROM daily_campaign_metrics WHERE metric_date = ANY($1::date[])',
    [metricDates]
  );

  await client.query(
    `
      WITH visit_rows AS (
        SELECT
          DATE(first_seen_at) AS metric_date,
          COALESCE(initial_utm_source, 'unknown') AS source,
          COALESCE(initial_utm_medium, 'unknown') AS medium,
          COALESCE(initial_utm_campaign, 'unknown') AS campaign,
          COALESCE(initial_utm_content, '') AS content,
          COUNT(*)::int AS visits,
          0::int AS orders,
          0::numeric(12, 2) AS revenue
        FROM tracking_sessions
        WHERE DATE(first_seen_at) = ANY($1::date[])
        GROUP BY 1, 2, 3, 4, 5
      ),
      order_rows AS (
        SELECT
          DATE(COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) AS metric_date,
          COALESCE(a.attributed_source, 'unknown') AS source,
          COALESCE(a.attributed_medium, 'unknown') AS medium,
          COALESCE(a.attributed_campaign, 'unknown') AS campaign,
          COALESCE(a.attributed_content, '') AS content,
          0::int AS visits,
          COUNT(*)::int AS orders,
          COALESCE(SUM(o.total_price), 0)::numeric(12, 2) AS revenue
        FROM attribution_results a
        INNER JOIN shopify_orders o ON o.shopify_order_id = a.shopify_order_id
        WHERE DATE(COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) = ANY($1::date[])
        GROUP BY 1, 2, 3, 4, 5
      )
      INSERT INTO daily_campaign_metrics (
        metric_date,
        source,
        medium,
        campaign,
        content,
        visits,
        orders,
        revenue,
        last_computed_at
      )
      SELECT
        metric_date,
        source,
        medium,
        campaign,
        content,
        SUM(visits)::int AS visits,
        SUM(orders)::int AS orders,
        SUM(revenue)::numeric(12, 2) AS revenue,
        now()
      FROM (
        SELECT * FROM visit_rows
        UNION ALL
        SELECT * FROM order_rows
      ) combined
      GROUP BY 1, 2, 3, 4, 5
    `,
    [metricDates]
  );
}

export async function processPendingAttribution(limit = 100): Promise<number> {
  const pendingOrders = await query<PendingOrder>(
    `
      SELECT
        o.shopify_order_id,
        o.created_at_shopify,
        o.landing_session_id,
        o.checkout_token,
        o.cart_token
      FROM shopify_orders o
      LEFT JOIN attribution_results a ON a.shopify_order_id = o.shopify_order_id
      WHERE a.shopify_order_id IS NULL
      ORDER BY COALESCE(o.created_at_shopify, o.ingested_at) ASC
      LIMIT $1
    `,
    [limit]
  );

  if (!pendingOrders.rowCount) {
    return 0;
  }

  await withTransaction(async (client) => {
    const metricDates = new Set<string>();

    for (const order of pendingOrders.rows) {
      const candidate = await findCandidate(client, order);

      await client.query(
        `
          INSERT INTO attribution_results (
            shopify_order_id,
            session_id,
            attribution_model,
            attributed_source,
            attributed_medium,
            attributed_campaign,
            attributed_content,
            attributed_term,
            attributed_click_id_type,
            attributed_click_id_value,
            confidence_score,
            attribution_reason,
            attributed_at,
            reprocess_version
          )
          VALUES (
            $1,
            $2::uuid,
            'last_non_direct_click',
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            now(),
            1
          )
          ON CONFLICT (shopify_order_id)
          DO UPDATE SET
            session_id = EXCLUDED.session_id,
            attribution_model = EXCLUDED.attribution_model,
            attributed_source = EXCLUDED.attributed_source,
            attributed_medium = EXCLUDED.attributed_medium,
            attributed_campaign = EXCLUDED.attributed_campaign,
            attributed_content = EXCLUDED.attributed_content,
            attributed_term = EXCLUDED.attributed_term,
            attributed_click_id_type = EXCLUDED.attributed_click_id_type,
            attributed_click_id_value = EXCLUDED.attributed_click_id_value,
            confidence_score = EXCLUDED.confidence_score,
            attribution_reason = EXCLUDED.attribution_reason,
            attributed_at = now(),
            reprocess_version = attribution_results.reprocess_version + 1
        `,
        [
          order.shopify_order_id,
          candidate?.session_id ?? null,
          candidate?.attributed_source ?? null,
          candidate?.attributed_medium ?? null,
          candidate?.attributed_campaign ?? null,
          candidate?.attributed_content ?? null,
          candidate?.attributed_term ?? null,
          candidate?.attributed_click_id_type ?? null,
          candidate?.attributed_click_id_value ?? null,
          candidate?.confidence_score ?? '0.00',
          candidate?.attribution_reason ?? 'unattributed'
        ]
      );

      const metricDate = (order.created_at_shopify ?? new Date()).toISOString().slice(0, 10);
      metricDates.add(metricDate);
    }

    await refreshDailyCampaignMetrics(client, [...metricDates]);
  });

  return pendingOrders.rowCount;
}

